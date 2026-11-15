// Copy: wacli/internal/app; package: app. Synthetic SQLite only; probe the
// reporting and budget edges of the revised durable replay bookkeeping.
package app

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/out"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
)

func TestPrereviewR8WorsenedUnfinishedStateIsReported(t *testing.T) {
	a := newTestApp(t)
	log := &lockedBuffer{}
	a.opts.Events = out.NewEventWriter(log, true)
	left := map[appstate.WAPatchName]whatsmeow.AppStateUnfinished{appstate.WAPatchRegularLow: {Owed: true}}
	a.recordUnfinishedAppState(left)
	left[appstate.WAPatchRegularLow] = whatsmeow.AppStateUnfinished{Owed: true, RecoveryDropped: true}
	a.recordUnfinishedAppState(left)
	warning := lifecycleWarnings(t, log)["app_state_shutdown_unfinished"]
	state := warning["collections"].(map[string]any)["regular_low"].(map[string]any)
	lines := strings.Count(string(log.Bytes()), `"app_state_shutdown_unfinished"`)
	t.Logf("recovery_dropped_after_first_warning=true updated_warning=%v warning_count=%d", state["recovery_dropped"], lines)
	if state["recovery_dropped"] != true {
		t.Fatal("a worsened unfinished outcome is suppressed because its durability category did not change")
	}
}

func TestPrereviewR8AbandonedDuplicateRecordsStillCostOneReplay(t *testing.T) {
	a := newTestApp(t)
	defer a.resumeAppState()
	shortShutdown(t, 120*time.Millisecond, 50*time.Millisecond, 20*time.Millisecond)
	raw, err := sql.Open("sqlite3", filepath.Join(a.opts.StoreDir, "wacli.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	lock, err := raw.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err = lock.ExecContext(t.Context(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	left := map[appstate.WAPatchName]whatsmeow.AppStateUnfinished{appstate.WAPatchRegularLow: {Undelivered: 1}}
	started := time.Now()
	a.recordUnfinishedAppState(left)
	a.retryUnfinishedAppStateRecord()
	a.retryUnfinishedAppStateRecord()
	elapsed := time.Since(started)
	if _, err = lock.ExecContext(t.Context(), `ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(3 * time.Second); replayRequests(t, a, "regular_low") < 3; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("abandoned writers did not finish")
		}
	}
	rows := replayRequests(t, a, "regular_low")
	f := newFakeWA()
	a.wa = f
	a.recoverAppStateCollection(context.Background(), "regular_low", new(sync.Map), time.Second)
	remaining := replayRequests(t, a, "regular_low")
	full := 0
	f.mu.Lock()
	for _, fetch := range f.appStateFetches {
		if fetch.name == "regular_low" && fetch.fullSync {
			full++
		}
	}
	f.mu.Unlock()
	t.Logf("record_attempts=3 elapsed=%s duplicate_rows=%d full_replays=%d rows_after_replay=%d", elapsed.Round(time.Millisecond), rows, full, remaining)
	if elapsed > time.Second || full != 1 || remaining != 0 {
		t.Fatal("duplicate or abandoned records escaped the shutdown/replay bound")
	}
}
