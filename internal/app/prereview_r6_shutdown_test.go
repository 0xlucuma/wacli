// Copy: wacli/internal/app; package: app. Synthetic bootstrap adapter; real
// App.Sync/App.Close, SDK recovery/ordered publication, and both SQLite stores.
package app

import (
	"context"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/sqliteutil"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	waStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

type prereviewSDKOwner struct {
	*fakeWA          // Synthetic authenticated connection/startup responses only.
	cli              *whatsmeow.Client
	entered, release chan struct{}
	once             sync.Once
	registrations    atomic.Int32
}

func (w *prereviewSDKOwner) AddEventHandler(fn func(any)) uint32 {
	owner := w.registrations.Add(1) == 2 // OpenWA observer first, Sync owner second.
	return w.cli.AddEventHandler(func(evt any) {
		slow := false
		switch v := evt.(type) {
		case *events.LabelEdit:
			slow = v.Action.GetName() == "first"
		case *events.MarkChatAsRead:
			slow = v.Action.GetRead()
		}
		if owner && slow {
			w.once.Do(func() { close(w.entered); <-w.release })
		}
		fn(evt)
	})
}
func (w *prereviewSDKOwner) RemoveEventHandler(id uint32) { w.cli.RemoveEventHandler(id) }
func (w *prereviewSDKOwner) Disconnect()                  { w.fakeWA.Disconnect(); w.cli.Disconnect() }
func (w *prereviewSDKOwner) WaitForAppStateEvents(ctx context.Context) (int, error) {
	err := w.cli.WaitForAppStateEvents(ctx)
	return w.cli.AppStateBacklog(appstate.WAPatchRegular).Depth, err
}

func prereviewOwner(t *testing.T) (*App, *prereviewSDKOwner) {
	t.Helper()
	a, err := New(Options{StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	container, err := sqlstore.New(t.Context(), "sqlite3", sqliteutil.FileURI(filepath.Join(t.TempDir(), "session.db"), "_foreign_keys=on"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Close() })
	device := container.NewDevice()
	device.ID = &types.JID{User: "15550000000", Device: 7, Server: types.DefaultUserServer}
	device.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte{1}, AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
	if err := device.Save(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := device.AppStateKeys.PutAppStateSyncKey(t.Context(), []byte{1}, waStore.AppStateSyncKey{Data: make([]byte, 32), Timestamp: 1, Fingerprint: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	w := &prereviewSDKOwner{fakeWA: newFakeWA(), cli: whatsmeow.NewClient(device, nil), entered: make(chan struct{}), release: make(chan struct{})}
	w.cli.EmitAppStateEventsOnFullSync = true
	a.wa = w
	t.Cleanup(a.Close)
	return a, w
}

func prereviewRecover(t *testing.T, w *prereviewSDKOwner, version uint64, label string) {
	prereviewRecoverAction(t, w, "regular", version, &waSyncAction.SyncActionData{Index: []byte(`["label_edit","7"]`), Value: &waSyncAction.SyncActionValue{LabelEditAction: &waSyncAction.LabelEditAction{Name: proto.String(label)}}})
}

func prereviewRecoverRead(t *testing.T, w *prereviewSDKOwner, version uint64, read bool) {
	prereviewRecoverAction(t, w, "regular_low", version, &waSyncAction.SyncActionData{Index: []byte(`["markChatAsRead","15550000001@s.whatsapp.net"]`), Value: &waSyncAction.SyncActionValue{MarkChatAsReadAction: &waSyncAction.MarkChatAsReadAction{Read: proto.Bool(read)}}})
}

func prereviewRecoverAction(t *testing.T, w *prereviewSDKOwner, collection string, version uint64, action *waSyncAction.SyncActionData) {
	t.Helper()
	mac := make([]byte, 32)
	mac[0] = byte(version)
	var hash [128]byte
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(hash[:], nil, [][]byte{mac})
	raw, err := proto.Marshal(&waSyncdSnapshotRecovery.SyncdSnapshotRecovery{
		CollectionName: proto.String(collection), CollectionLthash: hash[:],
		Version: &waSyncdSnapshotRecovery.SyncdVersion{Version: proto.Uint64(version)},
		MutationRecords: []*waSyncdSnapshotRecovery.SyncdPlainTextRecord{{
			KeyID: []byte{1}, Mac: mac,
			Value: action,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !w.cli.DangerousInternals().HandleAppStateRecovery(t.Context(), "synthetic", []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{{
		SyncdSnapshotFatalRecoveryResponse: &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{CollectionSnapshot: raw},
	}}) {
		t.Error("synthetic recovery failed")
	}
}

func TestPrereviewR6FollowShutdownPersistsCommittedQueue(t *testing.T) {
	t.Run("labels", func(t *testing.T) { prereviewShutdownQueue(t, false) })
	t.Run("manual_unread", func(t *testing.T) { prereviewShutdownQueue(t, true) })
}

func prereviewShutdownQueue(t *testing.T, unread bool) {
	a, w := prereviewOwner(t)
	if unread {
		if err := a.db.UpsertChat("15550000001@s.whatsapp.net", "dm", "synthetic", time.Unix(1, 0)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ready, syncDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(syncDone)
		_, _ = a.Sync(ctx, SyncOptions{Mode: SyncModeFollow, AfterConnect: func(context.Context) error { close(ready); return nil }})
	}()
	<-ready
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		if unread {
			prereviewRecoverRead(t, w, 1, true)
		} else {
			prereviewRecover(t, w, 1, "first")
		}
	}()
	<-w.entered
	// The second snapshot commits and queues behind the owner's slow first
	// callback. No fake publication or manual cursor advancement.
	if unread {
		prereviewRecoverRead(t, w, 2, false)
	} else {
		prereviewRecover(t, w, 2, "second")
	}
	cancel()
	// Establish that Sync's deferred removal is waiting for the SDK's read
	// lock. RWMutex's writer preference then removes the persistence owner
	// before publication acquires the next read lock.
	stack := make([]byte, 1<<20)
	deadline := time.Now().Add(3 * time.Second)
	for {
		n := runtime.Stack(stack, true)
		// Adapted from the pre-review probe, which waited for RemoveEventHandler here: Sync now drains before it removes, so the release waits for that drain instead.
		if strings.Contains(string(stack[:n]), "go.mau.fi/whatsmeow.(*Client).WaitForAppStateEvents(") {
			break
		}
		if time.Now().After(deadline) {
			close(w.release)
			t.Fatal("Sync did not reach deferred removal")
		}
		runtime.Gosched()
	}
	close(w.release)
	<-firstDone
	<-syncDone
	if err := w.cli.WaitForAppStateEvents(t.Context()); err != nil {
		t.Fatal(err)
	}
	collection := "regular"
	if unread {
		collection = "regular_low"
	}
	version, _, err := w.cli.Store.AppState.GetAppStateVersion(t.Context(), collection)
	if err != nil {
		t.Fatal(err)
	}
	// Close must really run its new barrier, then reopen wacli.db rather than
	// asserting against a speculative in-memory projection.
	a.Close()
	reopened, err := New(Options{StoreDir: a.opts.StoreDir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if unread {
		chat, err := reopened.db.GetChat("15550000001@s.whatsapp.net")
		if err != nil {
			t.Fatal(err)
		}
		pending, err := reopened.db.AppStateRecoveryRequired("regular_low")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("sync_cancelled=true collection=regular_low sdk_cursor=%d close_completed=true persisted_unread=%t replay_debt=%t want=true", version, chat.Unread, pending)
		if version != 2 || !chat.Unread {
			t.Fatal("shutdown consumed manual-unread events without persisting them")
		}
		return
	}
	lists, err := reopened.db.Lists()
	if err != nil {
		t.Fatal(err)
	}
	got := ""
	if len(lists.Lists) > 0 {
		got = lists.Lists[0].Name
	}
	t.Logf("sync_cancelled=true sdk_cursor=%d close_completed=true persisted_label=%q want=second", version, got)
	if version != 2 || got != "second" {
		t.Fatal("shutdown drained committed SDK events after removing their persistence owner")
	}
}

func TestPrereviewR6FollowShutdownFitsPM2KillTimeout(t *testing.T) {
	a, w := prereviewOwner(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ready, syncDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(syncDone)
		_, _ = a.Sync(ctx, SyncOptions{Mode: SyncModeFollow, AfterConnect: func(context.Context) error { close(ready); return nil }})
	}()
	<-ready
	firstDone := make(chan struct{})
	go func() { defer close(firstDone); prereviewRecover(t, w, 1, "first") }()
	<-w.entered
	cancel()
	start := time.Now()
	timedOut := false
	select {
	case <-syncDone:
	case <-time.After(15100 * time.Millisecond):
		timedOut = true
	}
	// Release the synthetic slow callback even on failure, and finish all
	// goroutines before the fixture closes either database.
	close(w.release)
	<-firstDone
	<-syncDone
	a.Close()
	t.Logf("pm2_budget=15s sync_cleanup_blocked_past_budget=%t elapsed=%s", timedOut, time.Since(start).Round(time.Millisecond))
	if timedOut {
		t.Fatal("Sync blocks removing its handler before Close can start its ten-second barrier")
	}
}
