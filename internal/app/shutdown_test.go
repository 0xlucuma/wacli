package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
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
	"go.mau.fi/whatsmeow/types"
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
			// The steps after the deadline get a grace that a loaded machine
			// does not turn into an abandoned store close.
			const budget = 800 * time.Millisecond
			shortShutdown(t, budget, 300*time.Millisecond, 250*time.Millisecond)
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
			if elapsed := time.Since(started); elapsed > budget+2*time.Second {
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

func replayRequests(t *testing.T, a *App, collection string) int {
	t.Helper()
	raw, err := sql.Open("sqlite3", filepath.Join(a.opts.StoreDir, "wacli.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var rows int
	if err := raw.QueryRow(`SELECT count(*) FROM app_state_recovery_intents WHERE collection=?`, collection).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

// A replay request that could not be written is not remembered as written.
// The operator is told, every later step tries again, and is told again
// when it worked. A request that ends up written twice costs one replay.
func TestReplayRecordIsRetriedUntilItIsDurable(t *testing.T) {
	a := newTestApp(t)
	log := &lockedBuffer{}
	a.opts.Events = out.NewEventWriter(log, true)
	raw, err := sql.Open("sqlite3", filepath.Join(a.opts.StoreDir, "wacli.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err = raw.Exec(`CREATE TRIGGER refuse_replay BEFORE INSERT ON app_state_recovery_intents BEGIN SELECT RAISE(FAIL, 'synthetic write failure'); END`); err != nil {
		t.Fatal(err)
	}
	left := map[appstate.WAPatchName]whatsmeow.AppStateUnfinished{appstate.WAPatchRegularLow: {Undelivered: 2}}
	a.recordUnfinishedAppState(left)
	a.recordUnfinishedAppState(left)
	first := lifecycleWarnings(t, log)["app_state_shutdown_unfinished"]
	if lines := strings.Count(string(log.Bytes()), `"app_state_shutdown_unfinished"`); lines != 1 || replayRequests(t, a, "regular_low") != 0 ||
		!strings.Contains(first["message"].(string), "could not record the replay of regular_low for the next start, trying again until the process stops: regular_low: ") ||
		!slices.Equal(first["replay_failed"].([]any), []any{"regular_low"}) || len(first["replay"].([]any)) != 0 || first["error"] == nil {
		t.Fatalf("%d warnings, %d requests, last: %v", lines, replayRequests(t, a, "regular_low"), first)
	}
	if _, err = raw.Exec(`DROP TRIGGER refuse_replay`); err != nil {
		t.Fatal(err)
	}
	// A later step asks nothing new of the SDK: it only tries again.
	a.retryUnfinishedAppStateRecord()
	a.retryUnfinishedAppStateRecord()
	second := lifecycleWarnings(t, log)["app_state_shutdown_unfinished"]
	if lines := strings.Count(string(log.Bytes()), `"app_state_shutdown_unfinished"`); lines != 2 || replayRequests(t, a, "regular_low") != 1 ||
		second["message"] != "warning: stopping with WhatsApp app state unfinished: regular_low (2 event batches not applied); a replay of regular_low is recorded for the next start" ||
		second["error"] != nil {
		t.Fatalf("%d warnings, %d requests, last: %v", lines, replayRequests(t, a, "regular_low"), second)
	}

	// As if an attempt that was abandoned had completed as well.
	if _, err := a.db.MarkAppStateRecoveryGenerations([]string{"regular_low"}); err != nil {
		t.Fatal(err)
	}
	dir := a.opts.StoreDir
	a.Close()
	next, err := New(Options{StoreDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
	f := newFakeWA()
	next.wa = f
	if _, err := next.Sync(t.Context(), SyncOptions{Mode: SyncModeOnce, IdleExit: 50 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	full := 0
	f.mu.Lock()
	for _, fetch := range f.appStateFetches {
		if fetch.name == "regular_low" && fetch.fullSync {
			full++
		}
	}
	f.mu.Unlock()
	if full != 1 || replayRequests(t, next, "regular_low") != 0 {
		t.Fatalf("two requests for one collection: %d replays, %d requests left", full, replayRequests(t, next, "regular_low"))
	}
}

// A write that the budget cut short is reported as not recorded. If it
// completes after all, it counts: nothing is written a second time for it.
func TestAbandonedReplayRecordStillCounts(t *testing.T) {
	a := newTestApp(t)
	// The app is closed with a budget of its own, after this one is restored.
	defer a.resumeAppState()
	shortShutdown(t, 300*time.Millisecond, 100*time.Millisecond, 30*time.Millisecond)
	log := &lockedBuffer{}
	a.opts.Events = out.NewEventWriter(log, true)
	raw, err := sql.Open("sqlite3", filepath.Join(a.opts.StoreDir, "wacli.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	// Something else holds the database's write lock past the budget.
	writer, err := raw.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err = writer.ExecContext(t.Context(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	a.recordUnfinishedAppState(map[appstate.WAPatchName]whatsmeow.AppStateUnfinished{appstate.WAPatchRegularLow: {Undelivered: 1}})
	warnings := lifecycleWarnings(t, log)
	if elapsed := time.Since(started); elapsed > 2*time.Second || warnings["shutdown_step_abandoned"]["step"] != "record the app state replay request" ||
		!strings.Contains(warnings["app_state_shutdown_unfinished"]["message"].(string), "could not record the replay of regular_low") {
		t.Fatalf("after %s: %v", elapsed, warnings)
	}
	if _, err = writer.ExecContext(t.Context(), `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(8 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		a.shutdownMu.Lock()
		recorded := a.shutdownRecorded["regular_low"]
		a.shutdownMu.Unlock()
		if recorded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the abandoned write completed and was not counted")
		}
	}
	a.retryUnfinishedAppStateRecord()
	last := lifecycleWarnings(t, log)["app_state_shutdown_unfinished"]
	if replayRequests(t, a, "regular_low") != 1 || !strings.HasSuffix(last["message"].(string), "a replay of regular_low is recorded for the next start") {
		t.Fatalf("%d requests, last warning: %v", replayRequests(t, a, "regular_low"), last)
	}
}

// cursorWA is a client whose SDK has a cursor for some collections only.
// firstSync stands for the server's answer to the first sync of the others.
type cursorWA struct {
	*fakeWA
	cursors   map[string]bool
	failing   error
	firstSync func(name string) error
}

func (w *cursorWA) HasLocalAppState(_ context.Context, name string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cursors[name], w.failing
}

func (w *cursorWA) FetchAppState(ctx context.Context, name string, fullSync, onlyIfNotSynced bool) error {
	if err := w.fakeWA.FetchAppState(ctx, name, fullSync, onlyIfNotSynced); err != nil || !onlyIfNotSynced {
		return err
	}
	w.mu.Lock()
	synced := w.cursors[name]
	w.mu.Unlock()
	if synced || w.firstSync == nil {
		return nil
	}
	return w.firstSync(name)
}

// The start catches up the two critical collections as well, because a
// notification the SDK consumed and refused before the restart is otherwise
// forgotten. With a cursor: incrementally, never with a full replay. Without
// one: the SDK's own first sync, unless the full sync is known broken. A
// failure is reported and the start goes on.
func TestStartCatchesUpCriticalCollections(t *testing.T) {
	critical := []string{"critical_block", "critical_unblock_low"}
	start := func(t *testing.T, cursors map[string]bool, prepare func(*App, *cursorWA)) (*App, *cursorWA, map[string]map[string]any) {
		t.Helper()
		a := newTestApp(t)
		log := &lockedBuffer{}
		a.opts.Events = out.NewEventWriter(log, true)
		if cursors == nil {
			cursors = map[string]bool{}
		}
		w := &cursorWA{fakeWA: newFakeWA(), cursors: cursors}
		a.wa = w
		if prepare != nil {
			prepare(a, w)
		}
		if _, err := a.Sync(t.Context(), SyncOptions{Mode: SyncModeOnce, IdleExit: 50 * time.Millisecond}); err != nil {
			t.Fatalf("the start was aborted: %v", err)
		}
		a.appStateRecoveryWorkers.Wait()
		if err := a.appStatePersist.waitIdle(t.Context()); err != nil {
			t.Fatal(err)
		}
		return a, w, lifecycleWarnings(t, log)
	}
	// fetches counts what the start asked for a collection: incremental
	// fetches, first syncs (onlyIfNotSynced) and full replays.
	fetches := func(w *cursorWA, name string) (incremental, first, full int) {
		w.mu.Lock()
		defer w.mu.Unlock()
		for _, fetch := range w.appStateFetches {
			switch {
			case fetch.name != name:
			case fetch.fullSync:
				full++
			case fetch.onlyIfNotSynced:
				first++
			default:
				incremental++
			}
		}
		return incremental, first, full
	}
	both := func() map[string]bool { return map[string]bool{"critical_block": true, "critical_unblock_low": true} }

	t.Run("with a cursor", func(t *testing.T) {
		_, w, warnings := start(t, both(), nil)
		for _, name := range critical {
			if incremental, first, full := fetches(w, name); incremental != 1 || first+full != 0 {
				t.Fatalf("%s: %d incremental fetches, %d first syncs, %d full replays", name, incremental, first, full)
			}
		}
		if warnings["app_state_sync_failed"] != nil {
			t.Fatalf("warning: %v", warnings["app_state_sync_failed"])
		}
	})
	t.Run("replay already requested", func(t *testing.T) {
		// The replay request owns the collection: no second fetch for it.
		_, w, _ := start(t, both(), func(a *App, _ *cursorWA) {
			if err := a.db.MarkAppStateRecoveryRequired("critical_block"); err != nil {
				t.Fatal(err)
			}
		})
		if incremental, first, full := fetches(w, "critical_block"); incremental+first != 0 || full != 1 {
			t.Fatalf("critical_block with a replay request: %d incremental fetches, %d first syncs, %d full replays", incremental, first, full)
		}
	})
	for _, broken := range []bool{false, true} {
		t.Run(fmt.Sprintf("with a cursor, fetch fails, full sync known broken=%v", broken), func(t *testing.T) {
			_, w, warnings := start(t, both(), func(a *App, w *cursorWA) {
				if broken {
					for _, name := range critical {
						if err := a.db.SetAppStateFullSyncBroken(name, true); err != nil {
							t.Fatal(err)
						}
					}
				}
				// The start fetches regular_high, regular_low and regular
				// first; the two critical fetches fail to verify.
				w.appStateFetchErrs = []error{nil, nil, nil, appstate.ErrMismatchingLTHash, appstate.ErrMismatchingLTHash}
			})
			for _, name := range critical {
				if incremental, first, full := fetches(w, name); incremental != 1 || first+full != 0 {
					t.Fatalf("%s: %d incremental fetches, %d first syncs, %d full replays after a failed catch-up", name, incremental, first, full)
				}
			}
			w.mu.Lock()
			phone := len(w.appStateRecoveries)
			w.mu.Unlock()
			failed := warnings["app_state_sync_failed"]
			if phone != 0 || failed["name"] != "critical_unblock_low" || !strings.Contains(failed["message"].(string), "warning: failed to sync WhatsApp app state critical_unblock_low: ") {
				t.Fatalf("phone recovery requests=%d, warning=%v", phone, failed)
			}
		})
	}
	t.Run("cursor cannot be read", func(t *testing.T) {
		_, w, warnings := start(t, nil, func(_ *App, w *cursorWA) { w.failing = errors.New("session store unavailable") })
		if incremental, first, full := fetches(w, "critical_block"); incremental+first+full != 0 || warnings["app_state_sync_failed"]["error"] != "session store unavailable" {
			t.Fatalf("fetches=%d warning=%v", incremental+first+full, warnings["app_state_sync_failed"])
		}
	})

	contact := types.NewJID("15550000009", types.DefaultUserServer)
	t.Run("never synced, the server has the collection", func(t *testing.T) {
		// The first sync stores the collection and hands its events to the
		// handlers, as the SDK does: the names go to its contact store first.
		a, w, warnings := start(t, nil, func(_ *App, w *cursorWA) {
			w.firstSync = func(name string) error {
				w.mu.Lock()
				w.cursors[name] = true
				w.mu.Unlock()
				if name == "critical_unblock_low" {
					w.applyContactBatch(&events.Contact{JID: contact, Timestamp: time.Unix(1, 0), Action: &waSyncAction.ContactAction{FullName: proto.String("First Sync")}})()
				}
				return nil
			}
		})
		for _, name := range critical {
			if incremental, first, full := fetches(w, name); first != 1 || incremental+full != 0 || !w.cursors[name] {
				t.Fatalf("%s: %d incremental fetches, %d first syncs, %d full replays, cursor=%v", name, incremental, first, full, w.cursors[name])
			}
		}
		if stored, err := a.db.GetContact(contact.String()); err != nil || stored.Name != "First Sync" {
			t.Fatalf("contact of the first sync: %+v, %v", stored, err)
		}
		if warnings["app_state_sync_failed"] != nil || warnings["app_state_first_sync_skipped"] != nil {
			t.Fatalf("warnings: %v", warnings)
		}
	})
	t.Run("never synced, full sync known broken", func(t *testing.T) {
		_, w, warnings := start(t, map[string]bool{"critical_unblock_low": true}, func(a *App, w *cursorWA) {
			if err := a.db.SetAppStateFullSyncBroken("critical_block", true); err != nil {
				t.Fatal(err)
			}
			w.firstSync = func(string) error { t.Error("a first sync was attempted"); return nil }
		})
		if incremental, first, full := fetches(w, "critical_block"); incremental+first+full != 0 {
			t.Fatalf("critical_block: %d incremental fetches, %d first syncs, %d full replays", incremental, first, full)
		}
		skipped := warnings["app_state_first_sync_skipped"]
		if skipped["name"] != "critical_block" || skipped["message"] != "warning: WhatsApp app state critical_block has never synced and its full sync is known broken; not syncing it at start" {
			t.Fatalf("app_state_first_sync_skipped: %v", skipped)
		}
		// The other one is caught up as before.
		if incremental, first, full := fetches(w, "critical_unblock_low"); incremental != 1 || first+full != 0 {
			t.Fatalf("critical_unblock_low: %d incremental fetches, %d first syncs, %d full replays", incremental, first, full)
		}
	})
	t.Run("never synced, the first sync fails", func(t *testing.T) {
		// The SDK reports a failed full sync to the handlers and returns the
		// error. The start reports it and goes on; the sync handler takes
		// the usual way, one recovery replay, and nothing asks again.
		_, w, warnings := start(t, map[string]bool{"critical_unblock_low": true}, func(_ *App, w *cursorWA) {
			w.firstSync = func(name string) error {
				err := fmt.Errorf("failed to decode app state %s patches: %w", name, appstate.ErrMismatchingLTHash)
				w.emit(&events.AppStateSyncError{Name: appstate.WAPatchName(name), FullSync: true, Error: err})
				return err
			}
		})
		incremental, first, full := fetches(w, "critical_block")
		failed := warnings["app_state_sync_failed"]
		if first != 1 || full != 1 || incremental != 0 || failed["name"] != "critical_block" || !strings.Contains(failed["error"].(string), "mismatching LTHash") {
			t.Fatalf("critical_block: %d incremental fetches, %d first syncs, %d full replays, warning=%v", incremental, first, full, failed)
		}
		if warnings["app_state_lthash_mismatch"]["name"] != "critical_block" {
			t.Fatalf("the failed first sync did not take the usual recovery path: %v", warnings)
		}
	})
}
