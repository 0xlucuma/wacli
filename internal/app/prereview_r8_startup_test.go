// Copy: wacli/internal/app; package: app. Synthetic WA fetch/error seam,
// actual Sync startup sequencing, error ownership and SQLite bookkeeping.
package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/out"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types/events"
)

type prereviewR8CriticalWA struct{ *appStateContextWA }

func (w *prereviewR8CriticalWA) HasLocalAppState(context.Context, string) (bool, error) {
	return true, nil
}

func TestPrereviewR8CriticalStartupFailureDoesNotStartRecovery(t *testing.T) {
	for _, mode := range []SyncMode{SyncModeFollow, SyncModeOnce, SyncModeBootstrap} {
		for _, failure := range []error{appstate.ErrMismatchingLTHash, &whatsmeow.IQError{Code: 429}} {
			t.Run(fmt.Sprint(mode, "/", failure), func(t *testing.T) {
				a := newTestApp(t)
				log := &lockedBuffer{}
				a.opts.Events = out.NewEventWriter(log, true)
				f := newFakeWA()
				w := &prereviewR8CriticalWA{&appStateContextWA{fakeWA: f}}
				w.fetchAppState = f.FetchAppState
				var mu sync.Mutex
				var calls []fakeAppStateFetch
				w.fetchEvents = func(ctx context.Context, name string, full, only bool) ([]any, error) {
					mu.Lock()
					calls = append(calls, fakeAppStateFetch{name: name, fullSync: full, onlyIfNotSynced: only})
					mu.Unlock()
					if name == "critical_block" || name == "critical_unblock_low" {
						// Match the SDK's synchronous decode-error event, whose
						// ownership must suppress the automatic recovery path.
						f.emit(&events.AppStateSyncError{Name: appstate.WAPatchName(name), Error: failure})
						return nil, failure
					}
					return f.FetchAppStateEvents(ctx, name, full, only)
				}
				a.wa = w
				ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
				defer cancel()
				opts := SyncOptions{Mode: mode, IdleExit: 10 * time.Millisecond}
				if mode == SyncModeFollow {
					opts.AfterConnect = func(context.Context) error { cancel(); return nil }
				}
				_, err := a.Sync(ctx, opts)
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				a.appStateRecoveryWorkers.Wait()
				mu.Lock()
				var critical, full int
				for _, call := range calls {
					if call.name == "critical_block" || call.name == "critical_unblock_low" {
						critical++
						if call.fullSync {
							full++
						}
					}
				}
				mu.Unlock()
				f.mu.Lock()
				phone := len(f.appStateRecoveries)
				f.mu.Unlock()
				warning := lifecycleWarnings(t, log)["app_state_sync_failed"]
				t.Logf("mode=%s critical_attempts=%d full=%d phone_recoveries=%d failed_warning=%t", mode, critical, full, phone, warning != nil)
				if critical != 2 || full != 0 || phone != 0 || warning == nil {
					t.Fatal("critical catch-up introduced duplicate/full recovery work")
				}
			})
		}
	}
}
