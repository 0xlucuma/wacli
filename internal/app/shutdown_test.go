package app

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/out"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// fencedSDKOwner is the pre-review probe's owner (a real SDK client behind a
// synthetic connection) with the fence of the real wa.Client.
type fencedSDKOwner struct{ *prereviewSDKOwner }

func (w *fencedSDKOwner) PauseAppState(ctx context.Context) (map[appstate.WAPatchName]whatsmeow.AppStateUnfinished, error) {
	return w.cli.PauseAppState(ctx)
}
func (w *fencedSDKOwner) ResumeAppState() { w.cli.ResumeAppState() }

func fencedOwner(t *testing.T) (*App, *fencedSDKOwner, *lockedBuffer) {
	t.Helper()
	a, w := prereviewOwner(t)
	fenced := &fencedSDKOwner{w}
	a.wa = fenced
	log := &lockedBuffer{}
	a.opts.Events = out.NewEventWriter(log, true)
	return a, fenced, log
}

func followInBackground(t *testing.T, a *App, ctx context.Context) <-chan struct{} {
	t.Helper()
	ready, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.Sync(ctx, SyncOptions{Mode: SyncModeFollow, AfterConnect: func(context.Context) error { close(ready); return nil }})
	}()
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("Sync did not connect")
	}
	return done
}

func waitForStack(t *testing.T, function string) {
	t.Helper()
	stack := make([]byte, 1<<20)
	for deadline := time.Now().Add(5 * time.Second); ; runtime.Gosched() {
		if strings.Contains(string(stack[:runtime.Stack(stack, true)]), function) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing reached %s", function)
		}
	}
}

// A handler that does not return cannot keep the process past its budget.
// The process says what it left behind and records, durably, which
// collection the SDK stored further than wacli.db applied; the next start
// replays that collection through the usual path.
func TestShutdownWithAStuckHandlerLeavesAReplayRequest(t *testing.T) {
	for _, collection := range []string{"regular_low", "regular"} {
		t.Run(collection, func(t *testing.T) {
			const budget = 800 * time.Millisecond
			shortShutdown(t, budget, 300*time.Millisecond, 30*time.Millisecond)
			a, w, log := fencedOwner(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			syncDone := followInBackground(t, a, ctx)
			if err := a.db.MarkRegularFullSyncComplete(); err != nil {
				t.Fatal(err)
			}
			deliver := func(version uint64) {
				if collection == "regular" {
					prereviewRecover(t, w.prereviewSDKOwner, version, map[uint64]string{1: "first", 2: "second"}[version])
				} else {
					prereviewRecoverRead(t, w.prereviewSDKOwner, version, version == 1)
				}
			}
			firstDone := make(chan struct{})
			go func() { defer close(firstDone); deliver(1) }()
			<-w.entered
			deliver(2)
			// The handler never returns while the process is shutting down.
			defer func() { close(w.release); <-firstDone }()

			started := time.Now()
			cancel()
			select {
			case <-syncDone:
			case <-time.After(budget + 3*time.Second):
				t.Fatal("Sync did not end with a handler stuck")
			}
			a.Close()
			if elapsed := time.Since(started); elapsed > budget+time.Second {
				t.Fatalf("shutdown took %s with a budget of %s", elapsed, budget)
			}
			warnings := lifecycleWarnings(t, log)
			unfinished := warnings["app_state_shutdown_unfinished"]
			state, _ := unfinished["collections"].(map[string]any)[collection].(map[string]any)
			if state["undelivered"] != float64(2) || !slices.Equal(unfinished["replay"].([]any), []any{collection}) || unfinished["error"] != nil {
				t.Fatalf("app_state_shutdown_unfinished: %v", unfinished)
			}
			if want := "warning: stopping with WhatsApp app state unfinished: " + collection + " (2 event batches not applied); a replay of " + collection + " is recorded for the next start"; unfinished["message"] != want {
				t.Fatalf("message %q, want %q", unfinished["message"], want)
			}
			if abandoned := warnings["shutdown_step_abandoned"]; abandoned == nil || abandoned["budget_ms"] != float64(budget.Milliseconds()) {
				t.Fatalf("shutdown_step_abandoned: %v", abandoned)
			}

			// The marker survives the process: reopen the store.
			next, err := New(Options{StoreDir: a.opts.StoreDir})
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			if pending, err := next.db.AppStateRecoveryRequired(collection); err != nil || !pending {
				t.Fatalf("replay request after reopening: %v, %v", pending, err)
			}
			if complete, err := next.db.RegularFullSyncComplete(); err != nil || complete != (collection != "regular") {
				t.Fatalf("regular completeness after reopening: %v, %v", complete, err)
			}
			// The next start acts on it: a guarded full replay of that collection.
			f := newFakeWA()
			next.wa = f
			if _, err := next.Sync(t.Context(), SyncOptions{Mode: SyncModeOnce, IdleExit: 50 * time.Millisecond}); err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			replayed := slices.ContainsFunc(f.appStateFetches, func(fetch fakeAppStateFetch) bool { return fetch.name == collection && fetch.fullSync })
			f.mu.Unlock()
			if pending, err := next.db.AppStateRecoveryRequired(collection); err != nil || pending || !replayed {
				t.Fatalf("next start: replayed=%v still pending=%v err=%v", replayed, pending, err)
			}
		})
	}
}

// App state that arrives while a shutdown drains is refused before the SDK
// stores it: nothing is lost (the server keeps it and the SDK's cursor does
// not move), nothing is replayed, and it is not reported as a backlog.
func TestShutdownRefusesAppStateThatArrivesDuringTheDrain(t *testing.T) {
	a, w, log := fencedOwner(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	syncDone := followInBackground(t, a, ctx)
	firstDone := make(chan struct{})
	go func() { defer close(firstDone); prereviewRecover(t, w.prereviewSDKOwner, 1, "first") }()
	<-w.entered
	cancel()
	waitForStack(t, "go.mau.fi/whatsmeow.(*Client).PauseAppState(")

	w.cli.DangerousInternals().HandleAppStateNotification(t.Context(), &waBinary.Node{
		Tag:     "notification",
		Content: []waBinary.Node{{Tag: "collection", Attrs: waBinary.Attrs{"name": "regular_low", "version": "9"}}},
	})
	prereviewRecoverAction(t, w.prereviewSDKOwner, "regular_high", 5, &waSyncAction.SyncActionData{
		Index: []byte(`["label_edit","8"]`),
		Value: &waSyncAction.SyncActionValue{LabelEditAction: &waSyncAction.LabelEditAction{Name: proto.String("refused")}},
	})
	if err := w.cli.FetchAppState(t.Context(), appstate.WAPatchRegular, false, false); !errors.Is(err, whatsmeow.ErrAppStatePaused) {
		t.Fatalf("fetch during the drain: %v", err)
	}
	select {
	case <-syncDone:
		t.Fatal("Sync ended while its persistence handler was still busy")
	default:
	}
	close(w.release)
	<-firstDone
	<-syncDone

	// The batch the handler was busy with is applied by the time Sync ends.
	if err := a.appStatePersist.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if lists, err := a.db.Lists(); err != nil || len(lists.Lists) != 1 || lists.Lists[0].Name != "first" {
		t.Fatalf("projection after the drain: %+v, %v", lists.Lists, err)
	}
	for _, name := range []string{"regular_low", "regular_high"} {
		version, _, err := w.cli.Store.AppState.GetAppStateVersion(t.Context(), name)
		pending, pendingErr := a.db.AppStateRecoveryRequired(name)
		if err != nil || version != 0 || pendingErr != nil || pending {
			t.Fatalf("%s: SDK version=%d (%v), replay requested=%v (%v); want nothing stored and nothing to replay", name, version, err, pending, pendingErr)
		}
	}
	warnings := lifecycleWarnings(t, log)
	if warnings["app_state_backlog"] != nil {
		t.Fatalf("a refusal during the drain raised the backlog alarm: %v", warnings["app_state_backlog"])
	}
	unfinished := warnings["app_state_shutdown_unfinished"]
	want := "warning: stopping with WhatsApp app state unfinished: regular (a fetch is owed); regular_high (a recovery snapshot was dropped); regular_low (a fetch is owed)"
	if unfinished["message"] != want || len(unfinished["replay"].([]any)) != 0 {
		t.Fatalf("app_state_shutdown_unfinished: %v", unfinished)
	}
}

// Sync pauses app state when it ends. A later Sync on the same App, as after
// an authentication bootstrap, resumes it: its events are applied again.
func TestSyncAfterSyncResumesAppState(t *testing.T) {
	a, w, _ := fencedOwner(t)
	if _, err := a.Sync(t.Context(), SyncOptions{Mode: SyncModeBootstrap, IdleExit: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := w.cli.FetchAppState(t.Context(), appstate.WAPatchRegular, false, false); !errors.Is(err, whatsmeow.ErrAppStatePaused) {
		t.Fatalf("fetch between the two syncs: %v, want app state paused", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	syncDone := followInBackground(t, a, ctx)
	prereviewRecover(t, w.prereviewSDKOwner, 1, "after the bootstrap")
	cancel()
	<-syncDone
	if err := a.appStatePersist.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if lists, err := a.db.Lists(); err != nil || len(lists.Lists) != 1 || lists.Lists[0].Name != "after the bootstrap" {
		t.Fatalf("projection of the second sync: %+v, %v", lists.Lists, err)
	}
}

// One-shot sync modes close media intake before they end. App state that the
// SDK delivers after that is still applied.
func TestSyncHandlerAppliesAppStateAfterMediaIntakeClosed(t *testing.T) {
	a, f := newLabelWriteApp(t)
	queue := newMediaQueue(1)
	if !queue.waitIdle(t.Context()) {
		t.Fatal("media queue did not close its intake")
	}
	var stored, last atomic.Int64
	id, _ := a.addSyncEventHandler(t.Context(), SyncOptions{Mode: SyncModeOnce}, &stored, &last, nil, nil, nil, func(string, string) {}, nil, nil, &syncPresence{}, queue)
	defer f.RemoveEventHandler(id)
	f.emit(&events.LabelEdit{LabelID: "7", Action: &waSyncAction.LabelEditAction{Name: proto.String("kept")}, Timestamp: time.Unix(1, 0)})
	if err := a.appStatePersist.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
	if lists, err := a.db.Lists(); err != nil || len(lists.Lists) != 1 || lists.Lists[0].Name != "kept" {
		t.Fatalf("projection: %+v, %v", lists.Lists, err)
	}
}

// What a shutdown records depends on what the SDK left: only event batches
// it stored and nobody applied need a replay, and for `regular` they also
// end the claim that its projection is complete. Each collection is
// recorded once per shutdown, and again only when it got worse.
func TestUnfinishedAppStateRecord(t *testing.T) {
	a := newTestApp(t)
	log := &lockedBuffer{}
	a.opts.Events = out.NewEventWriter(log, true)
	if err := a.db.MarkRegularFullSyncComplete(); err != nil {
		t.Fatal(err)
	}
	state := func() (pending []string, complete bool) {
		t.Helper()
		pending, err := a.db.AppStateRecoveryCollections()
		if err != nil {
			t.Fatal(err)
		}
		complete, err = a.db.RegularFullSyncComplete()
		if err != nil {
			t.Fatal(err)
		}
		return pending, complete
	}
	owed := map[appstate.WAPatchName]whatsmeow.AppStateUnfinished{
		appstate.WAPatchRegular:    {Owed: true},
		appstate.WAPatchRegularLow: {RecoveryDropped: true},
	}
	a.recordUnfinishedAppState(owed)
	if pending, complete := state(); len(pending) != 0 || !complete {
		t.Fatalf("nothing was stored, yet: replay requests=%v complete=%v", pending, complete)
	}
	first := lifecycleWarnings(t, log)["app_state_shutdown_unfinished"]["message"]

	// Asked again with the same answer: not reported twice.
	a.recordUnfinishedAppState(owed)
	// Worse: now a replay is needed.
	a.recordUnfinishedAppState(map[appstate.WAPatchName]whatsmeow.AppStateUnfinished{appstate.WAPatchRegular: {Undelivered: 1, Owed: true}})
	if pending, complete := state(); !slices.Equal(pending, []string{"regular"}) || complete {
		t.Fatalf("after undelivered regular batches: replay requests=%v complete=%v", pending, complete)
	}
	a.recordUnfinishedAppState(map[appstate.WAPatchName]whatsmeow.AppStateUnfinished{appstate.WAPatchRegular: {Undelivered: 3}})
	lines := strings.Count(string(log.Bytes()), `"app_state_shutdown_unfinished"`)
	second := lifecycleWarnings(t, log)["app_state_shutdown_unfinished"]["message"]
	if lines != 2 || first != "warning: stopping with WhatsApp app state unfinished: regular (a fetch is owed); regular_low (a recovery snapshot was dropped)" ||
		second != "warning: stopping with WhatsApp app state unfinished: regular (1 event batches not applied, a fetch is owed); a replay of regular is recorded for the next start" {
		t.Fatalf("%d warnings: %q then %q", lines, first, second)
	}
	// A new owner starts a new shutdown.
	a.resumeAppState()
	a.recordUnfinishedAppState(owed)
	if lines := strings.Count(string(log.Bytes()), `"app_state_shutdown_unfinished"`); lines != 3 {
		t.Fatalf("%d warnings after a resume", lines)
	}
}
