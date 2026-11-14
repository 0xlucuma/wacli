// Copy: wacli/internal/app; package: app. Synthetic SQLite fault, real
// shutdown recording and restart bookkeeping, no production stores.
package app

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
)

type prereviewR7ReplayFence struct {
	*fakeWA
	left map[appstate.WAPatchName]whatsmeow.AppStateUnfinished
}

func (f *prereviewR7ReplayFence) PauseAppState(context.Context) (map[appstate.WAPatchName]whatsmeow.AppStateUnfinished, error) {
	return f.left, nil
}
func (f *prereviewR7ReplayFence) ResumeAppState() {}

func TestPrereviewR7FailedReplayRecordCanBeRetriedByClose(t *testing.T) {
	a := newTestApp(t)
	raw, err := sql.Open("sqlite3", filepath.Join(a.opts.StoreDir, "wacli.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err = raw.Exec(`CREATE TRIGGER r7_refuse_replay BEFORE INSERT ON app_state_recovery_intents BEGIN SELECT RAISE(FAIL, 'r7 transient write failure'); END`); err != nil {
		t.Fatal(err)
	}
	left := map[appstate.WAPatchName]whatsmeow.AppStateUnfinished{appstate.WAPatchRegularLow: {Undelivered: 1}}
	f := &prereviewR7ReplayFence{fakeWA: newFakeWA(), left: left}
	a.wa = f
	a.quiesceAppState(f)
	if _, err = raw.Exec(`DROP TRIGGER r7_refuse_replay`); err != nil {
		t.Fatal(err)
	}
	// Close asks again about the same still-undelivered batch. SQLite has
	// recovered and the shared shutdown budget has not elapsed.
	remaining := time.Now().Before(a.shutdownDeadline())
	a.Close()
	var pending bool
	err = raw.QueryRow(`SELECT EXISTS(SELECT 1 FROM app_state_recovery_intents WHERE collection='regular_low')`).Scan(&pending)
	if err != nil {
		t.Fatal(err)
	}
	a.shutdownMu.Lock()
	remembered := a.shutdownRecorded["regular_low"]
	a.shutdownMu.Unlock()
	t.Logf("transient_fault_removed=true budget_remaining=%t recorded_in_memory=%t durable_replay=%t", remaining, remembered, pending)
	if remembered && !pending {
		t.Fatal("a failed durable replay write is memoized as recorded and never retried")
	}
}

func TestPrereviewR7KnownBrokenShutdownReplayDoesNotForceFullSync(t *testing.T) {
	a := newTestApp(t)
	a.recordUnfinishedAppState(map[appstate.WAPatchName]whatsmeow.AppStateUnfinished{appstate.WAPatchRegular: {Undelivered: 1}})
	if err := a.db.SetAppStateFullSyncBroken("regular", true); err != nil {
		t.Fatal(err)
	}
	dir := a.opts.StoreDir
	a.Close()
	for attempt := range 2 {
		next, err := New(Options{StoreDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		f := newFakeWA()
		f.appStateRecoveryErr = sql.ErrConnDone
		next.wa = f
		next.syncAppStateDeltas(t.Context(), new(sync.Map))
		next.appStateRecoveryWorkers.Wait()
		full := 0
		f.mu.Lock()
		for _, request := range f.appStateFetches {
			if request.name == "regular" && request.fullSync {
				full++
			}
		}
		f.mu.Unlock()
		pending, err := next.db.AppStateRecoveryRequired("regular")
		if err != nil {
			t.Fatal(err)
		}
		next.Close()
		t.Logf("restart=%d known_broken=true full_regular_fetches=%d replay_pending=%t", attempt+1, full, pending)
		if full != 0 || !pending {
			t.Fatal("known-broken replay bypassed its guard or falsely cleared debt")
		}
	}
}
