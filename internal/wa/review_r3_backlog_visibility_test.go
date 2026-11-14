package wa

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestReviewR3NotificationBacklogIsVisibleToOwner(t *testing.T) {
	c, err := New(Options{StorePath: newPairedSessionStore(t), KeyStateStore: newTestKeyStateStore(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cli := c.client
	cli.AppStateBacklogLimit = 1
	var diagnostic bytes.Buffer
	// Match session.go's production ERROR threshold, changing only the sink.
	cli.Log = newWhatsmeowLogger("Client", "ERROR", &diagnostic)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	defer resume()
	cli.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.AppStateSyncStart); ok {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	raw, err := proto.Marshal(&waSyncdSnapshotRecovery.SyncdSnapshotRecovery{
		CollectionName:   proto.String("regular"),
		CollectionLthash: make([]byte, 128),
		Version:          &waSyncdSnapshotRecovery.SyncdVersion{Version: proto.Uint64(1)},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(done)
		cli.DangerousInternals().HandleAppStateRecovery(ctx, "synthetic", []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{{
			SyncdSnapshotFatalRecoveryResponse: &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{CollectionSnapshot: raw},
		}})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("recovery did not reach its held handler")
	}
	cli.DangerousInternals().HandleAppStateNotification(ctx, &waBinary.Node{
		Tag:     "notification",
		Content: []waBinary.Node{{Tag: "collection", Attrs: waBinary.Attrs{"name": "regular", "version": "2"}}},
	})
	resume()
	<-done
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("owner_diagnostic_bytes=%d", diagnostic.Len())
	if diagnostic.Len() == 0 {
		t.Fatal("notification backlog warning was filtered by the owner's production log threshold")
	}
}
