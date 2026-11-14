// Copy: wacli/internal/app; package: app. Synthetic WA error seam, real
// recovery workers, replay bookkeeping and SQLite projection store.
package app

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func TestPrereviewR6RepeatedMismatchDoesNotRepeatFullReplay(t *testing.T) {
	a := newTestApp(t)
	f := newFakeWA()
	f.appStateFetchErr = appstate.ErrMismatchingLTHash
	var phoneAttempts atomic.Int32
	a.wa = &appStateContextWA{
		fakeWA: f, fetchAppState: f.FetchAppState, fetchEvents: f.FetchAppStateEvents,
		requestAppStateRecovery: func(context.Context, string) (types.MessageID, error) {
			phoneAttempts.Add(1)
			return "", errors.New("synthetic phone unavailable")
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var recoveries sync.Map
	for range 3 {
		a.handleAppStateSyncError(ctx, &events.AppStateSyncError{Name: appstate.WAPatchRegular, Error: appstate.ErrMismatchingLTHash}, &recoveries)
		a.appStateRecoveryWorkers.Wait()
	}
	f.mu.Lock()
	full := 0
	for _, attempt := range f.appStateFetches {
		if attempt.fullSync {
			full++
		}
	}
	f.mu.Unlock()
	phone := phoneAttempts.Load()
	broken, err := a.db.AppStateFullSyncBrokenCollections()
	if err != nil {
		t.Fatal(err)
	}
	pending, err := a.db.AppStateRecoveryRequired("regular")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("mismatch_events=3 full_replays=%d phone_requests=%d broken=%v replay_debt=%t", full, phone, broken, pending)
	if full != 1 || phone != 1 || !pending {
		t.Fatal("repeated errors escaped the owner's recovery deduplication")
	}
}
