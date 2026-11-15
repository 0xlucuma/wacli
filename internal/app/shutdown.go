package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
)

// A supervisor that stops the process kills it a fixed time after asking
// (pm2 gives the sync owner 15 seconds). Everything a shutdown waits for
// therefore shares one deadline, set when its first step begins: the end of
// Sync, the removal of a command's persistence handler, or Close.
var (
	appShutdownBudget = 8 * time.Second
	// The end of the budget is kept for what follows the app-state drain:
	// recording what it left behind, removing handlers, the persistence
	// queue and the stores.
	appShutdownReserve = 2 * time.Second
	// What a step still gets once the deadline has passed, so the quick ones
	// (closing a store) are not skipped by a timer that already fired.
	appShutdownGrace = 100 * time.Millisecond
)

func (a *App) shutdownDeadline() time.Time {
	a.shutdownMu.Lock()
	defer a.shutdownMu.Unlock()
	if a.shutdownAt.IsZero() {
		a.shutdownAt = time.Now().Add(appShutdownBudget)
	}
	return a.shutdownAt
}

// shutdownContext ends at the shutdown deadline, or after the grace when
// the deadline has passed.
func (a *App) shutdownContext() (context.Context, context.CancelFunc) {
	deadline := a.shutdownDeadline()
	if grace := time.Now().Add(appShutdownGrace); deadline.Before(grace) {
		deadline = grace
	}
	return context.WithDeadline(context.Background(), deadline)
}

// shutdownStep runs a step that cannot be cancelled and stops waiting for it
// at the shutdown deadline. The step goes on in the background: the process
// is about to exit, and a handler that never returns must not keep it.
func (a *App) shutdownStep(what string, step func()) bool {
	ctx, cancel := a.shutdownContext()
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		step()
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		a.emitWarning("shutdown_step_abandoned",
			fmt.Sprintf("warning: shutdown budget of %s is used up; not waiting any longer to %s", appShutdownBudget, what),
			map[string]any{"step": what, "budget_ms": appShutdownBudget.Milliseconds()})
		return false
	}
}

// appStateFence is implemented by the real client: see
// whatsmeow.Client.PauseAppState.
type appStateFence interface {
	PauseAppState(ctx context.Context) (map[appstate.WAPatchName]whatsmeow.AppStateUnfinished, error)
	ResumeAppState()
}

// appStateEventsWaiter is a client that can wait for the app-state events it
// committed, but can neither fence new work nor say which collections are
// left. What it leaves is recorded for every collection.
type appStateEventsWaiter interface {
	WaitForAppStateEvents(ctx context.Context) (undelivered int, err error)
}

// resumeAppState is called once an event handler that persists app state is
// registered: app state that an earlier owner paused flows again, and a later
// shutdown gets a budget of its own.
func (a *App) resumeAppState() {
	a.shutdownMu.Lock()
	a.shutdownAt = time.Time{}
	a.shutdownUnfinished, a.shutdownRecorded, a.shutdownRecordErr, a.shutdownReported = nil, nil, nil, nil
	a.shutdownMu.Unlock()
	if fence, ok := a.wa.(appStateFence); ok {
		fence.ResumeAppState()
	}
}

// detachAppStateOwner removes an event handler that persists app state. The
// SDK stores app state before it hands the events to the handlers, so the
// handler first stays until the SDK has delivered everything it committed.
// Not from an event handler: it would wait for itself.
func (a *App) detachAppStateOwner(waClient WAClient, handlerID uint32) {
	a.quiesceAppState(waClient)
	a.shutdownStep("remove the app state event handler", func() { waClient.RemoveEventHandler(handlerID) })
}

// quiesceAppState stops the SDK from taking on app-state work and waits,
// within the shutdown budget, until the handlers have been given every event
// batch it committed. What is left when the budget ends would otherwise be
// lost without a trace: the SDK's cursor is already past it. Those
// collections get a durable replay request, which the next start acts on
// like on any other (syncAppStateDeltas).
func (a *App) quiesceAppState(waClient WAClient) {
	if waClient == nil {
		return
	}
	ctx, cancel := context.WithDeadline(context.Background(), a.shutdownDeadline().Add(-appShutdownReserve))
	defer cancel()
	var unfinished map[appstate.WAPatchName]whatsmeow.AppStateUnfinished
	switch client := waClient.(type) {
	case appStateFence:
		unfinished, _ = client.PauseAppState(ctx)
	case appStateEventsWaiter:
		if undelivered, err := client.WaitForAppStateEvents(ctx); err != nil {
			unfinished = make(map[appstate.WAPatchName]whatsmeow.AppStateUnfinished)
			for _, name := range appstate.AllPatchNames {
				unfinished[name] = whatsmeow.AppStateUnfinished{Undelivered: max(undelivered, 1)}
			}
		}
	}
	a.recordUnfinishedAppState(unfinished)
}

// recordUnfinishedAppState takes what a drain left unfinished, records the
// replay requests it calls for, and tells the operator.
//
// Three things are kept apart per collection: what is unfinished (the worst
// seen in this shutdown), whether its replay request is durably written, and
// what the operator was last told. A write that failed or ran out of budget
// is not remembered as done: every later step of the shutdown tries it again
// (retryUnfinishedAppStateRecord). The operator is told again whenever what
// there is to say about a collection changed.
func (a *App) recordUnfinishedAppState(unfinished map[appstate.WAPatchName]whatsmeow.AppStateUnfinished) {
	a.shutdownMu.Lock()
	if a.shutdownUnfinished == nil {
		a.shutdownUnfinished = make(map[string]whatsmeow.AppStateUnfinished)
		a.shutdownRecorded = make(map[string]bool)
		a.shutdownRecordErr = make(map[string]error)
		a.shutdownReported = make(map[string]string)
	}
	for name, state := range unfinished {
		worst := a.shutdownUnfinished[string(name)]
		worst.Undelivered = max(worst.Undelivered, state.Undelivered)
		worst.Owed = worst.Owed || state.Owed
		worst.RecoveryDropped = worst.RecoveryDropped || state.RecoveryDropped
		a.shutdownUnfinished[string(name)] = worst
	}
	a.shutdownMu.Unlock()
	a.retryUnfinishedAppStateRecord()
}

// retryUnfinishedAppStateRecord writes the replay requests of this shutdown
// that are not durable yet, and reports what changed.
//
// Only event batches the SDK stored and nobody applied need a replay. An
// owed fetch or a dropped recovery snapshot stored nothing: the SDK's cursor
// is not ahead of wacli.db, the start catches up every collection that has a
// cursor (syncAppStateDeltas), and the recovery request that asked for a
// snapshot is still recorded.
func (a *App) retryUnfinishedAppStateRecord() {
	a.shutdownMu.Lock()
	var missing []string
	for name, state := range a.shutdownUnfinished {
		if state.Undelivered > 0 && !a.shutdownRecorded[name] {
			missing = append(missing, name)
		}
	}
	a.shutdownMu.Unlock()
	slices.Sort(missing)
	if len(missing) > 0 && a.db != nil {
		// Bounded like every other step: the handler that would not return
		// may be holding the database. An attempt that is abandoned here may
		// still complete, and then marks its collections itself. Writing a
		// request twice is harmless: the replay clears every request of its
		// collection that is older than its own.
		a.shutdownStep("record the app state replay request", func() {
			for _, name := range missing {
				_, err := a.db.MarkAppStateRecoveryGenerations([]string{name})
				if err == nil && name == string(appstate.WAPatchRegular) {
					// Completeness must not be claimed for a projection
					// that missed events.
					err = a.db.InvalidateRegularFullSync()
				}
				a.shutdownMu.Lock()
				// Nil once a new owner started over: this attempt was
				// abandoned long ago, and its request simply stands.
				if a.shutdownRecorded != nil && err == nil {
					a.shutdownRecorded[name] = true
					delete(a.shutdownRecordErr, name)
				} else if a.shutdownRecordErr != nil && err != nil {
					a.shutdownRecordErr[name] = err
				}
				a.shutdownMu.Unlock()
			}
		})
	}

	// A collection is reported when what would be said about it differs
	// from what was last said: its state got worse in any way, or the
	// outcome of its replay record changed.
	a.shutdownMu.Lock()
	var names, recorded, failed []string
	var recordErr error
	describe := make(map[string]string)
	states := make(map[string]whatsmeow.AppStateUnfinished)
	for name, state := range a.shutdownUnfinished {
		var parts []string
		if state.Undelivered > 0 {
			parts = append(parts, fmt.Sprintf("%d event batches not applied", state.Undelivered))
		}
		if state.Owed {
			parts = append(parts, "a fetch is owed")
		}
		if state.RecoveryDropped {
			parts = append(parts, "a recovery snapshot was dropped")
		}
		detail := fmt.Sprintf("%s (%s)", name, strings.Join(parts, ", "))
		outcome := ""
		switch {
		case state.Undelivered == 0:
		case a.shutdownRecorded[name]:
			outcome = "replay recorded"
		default:
			outcome = "replay not recorded"
		}
		if a.shutdownReported[name] == detail+"; "+outcome {
			continue
		}
		a.shutdownReported[name] = detail + "; " + outcome
		names = append(names, name)
		describe[name], states[name] = detail, state
		switch outcome {
		case "replay recorded":
			recorded = append(recorded, name)
		case "replay not recorded":
			failed = append(failed, name)
			if err := a.shutdownRecordErr[name]; err != nil {
				recordErr = errors.Join(recordErr, fmt.Errorf("%s: %w", name, err))
			}
		}
	}
	a.shutdownMu.Unlock()
	if len(names) == 0 {
		return
	}
	slices.Sort(names)
	slices.Sort(recorded)
	slices.Sort(failed)
	details := make([]string, len(names))
	collections := make(map[string]any, len(names))
	for i, name := range names {
		state := states[name]
		details[i] = describe[name]
		collections[name] = map[string]any{"undelivered": state.Undelivered, "owed": state.Owed, "recovery_dropped": state.RecoveryDropped}
	}
	message := "warning: stopping with WhatsApp app state unfinished: " + strings.Join(details, "; ")
	data := map[string]any{"collections": collections, "replay": append([]string{}, recorded...), "replay_failed": append([]string{}, failed...)}
	if len(recorded) > 0 {
		message += fmt.Sprintf("; a replay of %s is recorded for the next start", strings.Join(recorded, ", "))
	}
	if len(failed) > 0 {
		if recordErr == nil {
			recordErr = errors.New("the database did not answer within the shutdown budget")
		}
		message += fmt.Sprintf("; could not record the replay of %s for the next start, trying again until the process stops: %v", strings.Join(failed, ", "), recordErr)
		data["error"] = recordErr.Error()
	}
	a.emitWarning("app_state_shutdown_unfinished", message, data)
}
