package wa_test

import (
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/util/keys"
	"google.golang.org/protobuf/proto"
)

// Removing a persistence handler through the real client: the SDK has
// stored two batches and delivered neither when the command is done. The
// handler must get both before it is removed, and app state stays paused
// until a persistence handler is registered again.
func TestPersistenceHandlerRemovalDrainsTheRealClient(t *testing.T) {
	// This test must stay nonparallel: the SDK exports a process-wide trust root.
	root := keys.NewKeyPair()
	previousRoot := whatsmeow.WACertPubKey
	whatsmeow.WACertPubKey = *root.Pub
	t.Cleanup(func() { whatsmeow.WACertPubKey = previousRoot })

	e := newRegularTransport(t, root)
	entered, release := e.holdLabel("old")
	remove, err := e.app.AddChatStatePersistenceHandler(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	e.server.foreignPatch(e.ctx, labelEdit("old"))
	fetched := make(chan error, 1)
	go func() { fetched <- e.cli.FetchAppState(e.ctx, appstate.WAPatchRegular, false, false) }()
	e.wait(entered, "fetch never dispatched its label")
	e.adoptRecovery(&waSyncAction.SyncActionData{Index: []byte(`["label_edit","7"]`), Value: &waSyncAction.SyncActionValue{
		Timestamp: proto.Int64(time.Now().UnixMilli()), LabelEditAction: &waSyncAction.LabelEditAction{Name: proto.String("new")},
	}})

	removed := make(chan struct{})
	go func() { defer close(removed); remove() }()
	stack := make([]byte, 1<<20)
	for !strings.Contains(string(stack[:runtime.Stack(stack, true)]), "go.mau.fi/whatsmeow.(*Client).PauseAppState(") {
		if e.ctx.Err() != nil {
			t.Fatal("removing the handler did not pause app state")
		}
		runtime.Gosched()
	}
	select {
	case <-removed:
		t.Fatal("handler removed while the SDK still had events for it")
	default:
	}
	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	<-removed
	e.projection("new", nil)

	if err := e.cli.FetchAppState(e.ctx, appstate.WAPatchRegular, false, false); !errors.Is(err, whatsmeow.ErrAppStatePaused) {
		t.Fatalf("fetch without a persistence handler: %v, want app state paused", err)
	}
	e.persistOwner()
	if err := e.cli.FetchAppState(e.ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatalf("fetch with a persistence handler again: %v", err)
	}
}
