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
	a.shutdownRecorded = nil
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

func (a *App) recordUnfinishedAppState(unfinished map[appstate.WAPatchName]whatsmeow.AppStateUnfinished) {
	a.shutdownMu.Lock()
	if a.shutdownRecorded == nil {
		a.shutdownRecorded = make(map[string]bool)
	}
	var names []string
	replay := []string{}
	for name, state := range unfinished {
		// Once per shutdown and collection, or again when it got worse.
		if recorded, seen := a.shutdownRecorded[string(name)]; seen && (recorded || state.Undelivered == 0) {
			continue
		}
		a.shutdownRecorded[string(name)] = state.Undelivered > 0
		names = append(names, string(name))
		if state.Undelivered > 0 {
			replay = append(replay, string(name))
		}
	}
	a.shutdownMu.Unlock()
	if len(names) == 0 {
		return
	}
	slices.Sort(names)
	slices.Sort(replay)
	// Durable first. An owed fetch or a dropped recovery snapshot needs no
	// replay: nothing was stored, so the SDK's cursor is not ahead of
	// wacli.db, and the recovery request that asked for a snapshot is still
	// recorded.
	var recordErr error
	if len(replay) > 0 && a.db != nil {
		// Bounded like every other step: the handler that would not return
		// may be holding the database.
		var stepErr error
		if a.shutdownStep("record the app state replay request", func() {
			_, stepErr = a.db.MarkAppStateRecoveryGenerations(replay)
			if slices.Contains(replay, string(appstate.WAPatchRegular)) {
				// Completeness must not be claimed for a projection that
				// missed events.
				stepErr = errors.Join(stepErr, a.db.InvalidateRegularFullSync())
			}
		}) {
			recordErr = stepErr
		} else {
			recordErr = errors.New("the database did not answer within the shutdown budget")
		}
	}
	details := make([]string, len(names))
	collections := make(map[string]any, len(names))
	for i, name := range names {
		state := unfinished[appstate.WAPatchName(name)]
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
		details[i] = fmt.Sprintf("%s (%s)", name, strings.Join(parts, ", "))
		collections[name] = map[string]any{"undelivered": state.Undelivered, "owed": state.Owed, "recovery_dropped": state.RecoveryDropped}
	}
	message := "warning: stopping with WhatsApp app state unfinished: " + strings.Join(details, "; ")
	data := map[string]any{"collections": collections, "replay": replay}
	switch {
	case recordErr != nil:
		message += fmt.Sprintf("; could not record the replay of %s for the next start: %v", strings.Join(replay, ", "), recordErr)
		data["error"] = recordErr.Error()
	case len(replay) > 0:
		message += fmt.Sprintf("; a replay of %s is recorded for the next start", strings.Join(replay, ", "))
	}
	a.emitWarning("app_state_shutdown_unfinished", message, data)
}
