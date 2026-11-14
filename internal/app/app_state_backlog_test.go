package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/out"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types/events"
)

// The SDK refuses a fetch while the collection's events are still backed up
// in handlers. Nothing was stored, so the refusal must not cost coverage,
// create replay debt or mark the collection broken, and a command is told to
// retry. Making up for it and reporting it is the SDK's part: see
// TestAppStateBacklogEventsBecomeWarnings and the real-client test in
// internal/wa.
func TestAppStateBacklogIsTransient(t *testing.T) {
	a, f := newLabelWriteApp(t)
	if err := a.db.MarkRegularFullSyncComplete(); err != nil {
		t.Fatal(err)
	}
	f.appStateFetchErr = fmt.Errorf("fetch: %w", &whatsmeow.AppStateBacklogError{
		Name:            appstate.WAPatchRegular,
		AppStateBacklog: whatsmeow.AppStateBacklog{Depth: 64, OldestAge: 90 * time.Second},
	})

	fetchErr, persistErr := a.fetchAndPersistAppState(t.Context(), appstate.WAPatchRegular, true, &appStatePersistenceTracker{})
	if !errors.Is(fetchErr, whatsmeow.ErrAppStateBacklog) || persistErr != nil {
		t.Fatalf("fetch=%v persist=%v", fetchErr, persistErr)
	}
	if complete, err := a.db.RegularFullSyncComplete(); err != nil || !complete {
		t.Fatalf("refused fetch invalidated verified coverage: complete=%v err=%v", complete, err)
	}
	assertNoStaleFetchDebt(t, a)

	err := a.syncChatStateBeforeWrite(t.Context(), appstate.WAPatchRegular)
	if !errors.Is(err, whatsmeow.ErrAppStateBacklog) || !strings.Contains(err.Error(), "retry the command") {
		t.Fatalf("caller must retry rather than write behind a backlog: %v", err)
	}
	assertNoStaleFetchDebt(t, a)
	noPatch(t, f)

	f.appStateFetchErr = nil
	if err := a.syncChatStateBeforeWrite(t.Context(), appstate.WAPatchRegular); err != nil {
		t.Fatalf("fetch after the backlog drained: %v", err)
	}
	// The first fetch above was the explicit full replay.
	for _, fetch := range f.appStateFetches[1:] {
		if fetch.fullSync {
			t.Fatal("refused incremental fetch escalated to a full replay")
		}
	}
}

func lifecycleWarnings(t *testing.T, log *bytes.Buffer) map[string]map[string]any {
	t.Helper()
	warnings := map[string]map[string]any{}
	for line := range bytes.SplitSeq(bytes.TrimSpace(log.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var event struct {
			Event string         `json:"event"`
			Data  map[string]any `json:"data"`
		}
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("invalid lifecycle JSON: %s: %v", line, err)
		}
		if code, ok := event.Data["code"].(string); ok && event.Event == "warning" {
			warnings[code] = event.Data
		}
	}
	return warnings
}

// The always-on handler turns each SDK backlog event into its warning.
func TestAppStateBacklogEventsBecomeWarnings(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	a.wa = f
	var eventLog bytes.Buffer
	a.opts.Events = out.NewEventWriter(&eventLog, true)
	if err := a.OpenWA(); err != nil {
		t.Fatal(err)
	}
	f.emit(&events.AppStateDeferred{Name: appstate.WAPatchRegular, Source: "notification", Version: 205, Depth: 64, OldestAge: 90 * time.Second})
	f.emit(&events.AppStateCatchUpFailed{Name: appstate.WAPatchRegular, Version: 205, Error: errors.New("timed out"), RetryIn: 5 * time.Second})
	f.emit(&events.AppStateCaughtUp{Name: appstate.WAPatchRegular, Version: 205})
	want := map[string]map[string]any{
		"app_state_backlog":         {"name": "regular", "source": "notification", "depth": float64(64), "oldest_age_ms": float64(90000), "version": float64(205)},
		"app_state_catch_up_failed": {"name": "regular", "version": float64(205), "retry_in_ms": float64(5000), "error": "timed out"},
		"app_state_caught_up":       {"name": "regular", "version": float64(205)},
	}
	got := lifecycleWarnings(t, &eventLog)
	for code, fields := range want {
		for field, value := range fields {
			if got[code][field] != value {
				t.Fatalf("%s.%s = %v, want %v (warning: %v)", code, field, got[code][field], value, got[code])
			}
		}
		if message, _ := got[code]["message"].(string); !strings.Contains(message, "regular") {
			t.Fatalf("%s message: %q", code, message)
		}
	}
}

// waitingWA is a client that still has app-state events to deliver.
type waitingWA struct {
	*fakeWA
	app      *App
	stuck    bool
	sawStore error
	waited   bool
}

func (w *waitingWA) WaitForAppStateEvents(ctx context.Context) (int, error) {
	w.waited = true
	// The handlers still need the store while events are being delivered.
	_, w.sawStore = w.app.db.Lists()
	if !w.stuck {
		return 0, nil
	}
	<-ctx.Done()
	return 3, ctx.Err()
}

// Close waits for the SDK's committed app-state events after disconnecting
// and before the stores go away. A handler that never finishes costs a
// bounded wait and a warning, not the shutdown.
func TestCloseWaitsForAppStateEvents(t *testing.T) {
	for _, stuck := range []bool{false, true} {
		t.Run(fmt.Sprintf("stuck=%t", stuck), func(t *testing.T) {
			previous := appStateEventsCloseWait
			appStateEventsCloseWait = 50 * time.Millisecond
			t.Cleanup(func() { appStateEventsCloseWait = previous })
			a, err := New(Options{StoreDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			f := newFakeWA()
			f.connected = true
			w := &waitingWA{fakeWA: f, app: a, stuck: stuck}
			a.wa = w
			var eventLog bytes.Buffer
			a.opts.Events = out.NewEventWriter(&eventLog, true)
			if err := a.OpenWA(); err != nil {
				t.Fatal(err)
			}
			a.Close()
			if !w.waited || w.sawStore != nil || f.connected {
				t.Fatalf("waited=%v store during the wait=%v still connected=%v", w.waited, w.sawStore, f.connected)
			}
			if _, err := a.db.Lists(); err == nil {
				t.Fatal("store still open after Close")
			}
			warning, warned := lifecycleWarnings(t, &eventLog)["app_state_events_undelivered"]
			if warned != stuck || (stuck && (warning["depth"] != float64(3) || warning["waited_ms"] != float64(50))) {
				t.Fatalf("stuck=%v warning=%v", stuck, warning)
			}
		})
	}
}
