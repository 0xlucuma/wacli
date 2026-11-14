// Copy: wacli/internal/app; package: app. Real SDK pause/debt and SQLite;
// synthetic startup transport seam counts and serves critical deltas.
package app

import (
	"context"
	"sync"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
)

func TestPrereviewR7RestartPaysCriticalDebt(t *testing.T) {
	for _, name := range []appstate.WAPatchName{appstate.WAPatchCriticalBlock, appstate.WAPatchCriticalUnblockLow} {
		t.Run(string(name), func(t *testing.T) {
			a, w, _ := fencedOwner(t)
			if err := a.OpenWA(); err != nil {
				t.Fatal(err)
			}
			state := w.cli.Store.AppState
			if err := state.PutAppStateVersion(t.Context(), string(name), 1, [128]byte{}); err != nil {
				t.Fatal(err)
			}
			if _, err := w.cli.PauseAppState(t.Context()); err != nil {
				t.Fatal(err)
			}
			w.cli.DangerousInternals().HandleAppStateNotification(t.Context(), &waBinary.Node{
				Tag: "notification", Content: []waBinary.Node{{Tag: "collection", Attrs: waBinary.Attrs{"name": string(name), "version": "2"}}},
			})
			left, err := w.cli.PauseAppState(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			owed := left[name].Owed
			a.Close()
			next, err := New(Options{StoreDir: a.opts.StoreDir})
			if err != nil {
				t.Fatal(err)
			}
			defer next.Close()
			durable, err := next.db.AppStateRecoveryRequired(string(name))
			if err != nil {
				t.Fatal(err)
			}
			f := newFakeWA()
			f.appStateFetchEvent = func(collection string, full, only bool) any {
				if collection == string(name) {
					if full {
						t.Error("catching up a refused change should be incremental")
					}
					if err := state.PutAppStateVersion(t.Context(), collection, 2, [128]byte{}); err != nil {
						t.Error(err)
					}
				}
				return nil
			}
			next.wa = f
			next.syncAppStateDeltas(t.Context(), &sync.Map{})
			var requests int
			f.mu.Lock()
			for _, fetch := range f.appStateFetches {
				if fetch.name == string(name) {
					requests++
				}
			}
			f.mu.Unlock()
			after, _, err := state.GetAppStateVersion(t.Context(), string(name))
			if err != nil {
				t.Fatal(err)
			}
			// Negative control: serving that incremental request would heal it.
			if err := f.FetchAppState(context.Background(), string(name), false, false); err != nil {
				t.Fatal(err)
			}
			manual, _, err := state.GetAppStateVersion(t.Context(), string(name))
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("owed_at_stop=%t durable_request=%t startup_fetches=%d cursor_after_start=%d explicit_fetch_cursor=%d",
				owed, durable, requests, after, manual)
			if owed && requests == 0 && after != manual {
				t.Fatal("a consumed critical notification is forgotten across restart, with no startup catch-up")
			}
		})
	}
}
