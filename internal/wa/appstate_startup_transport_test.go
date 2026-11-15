package wa_test

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/app"
	"github.com/openclaw/wacli/internal/out"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	"google.golang.org/protobuf/proto"
)

type startupRequest struct {
	version  uint64
	snapshot bool
}

type lockedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// warnings returns the last lifecycle warning of each code.
func (l *lockedLog) warnings(t *testing.T) map[string]map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	warnings := map[string]map[string]any{}
	for line := range bytes.SplitSeq(bytes.TrimSpace(l.buf.Bytes()), []byte("\n")) {
		var event struct {
			Event string         `json:"event"`
			Data  map[string]any `json:"data"`
		}
		if len(line) == 0 || json.Unmarshal(line, &event) != nil || event.Event != "warning" {
			continue
		}
		if code, ok := event.Data["code"].(string); ok {
			warnings[code] = event.Data
		}
	}
	return warnings
}

// The start's catch-up of the two critical collections on the wire: the real
// App.Sync, the real client, a Noise peer and both SQLite stores. Neither
// collection has ever synced, so the SDK turns the start's fetch into its
// first sync. Checked is what the peer was asked and what the stores hold
// afterwards, not which flags the start passed.
func TestStartFirstSyncsCriticalCollectionsOnTheWire(t *testing.T) {
	// This test must stay nonparallel: the SDK exports a process-wide trust root.
	root := keys.NewKeyPair()
	previousRoot := whatsmeow.WACertPubKey
	whatsmeow.WACertPubKey = *root.Pub
	t.Cleanup(func() { whatsmeow.WACertPubKey = previousRoot })

	contact := types.NewJID("15550000009", types.DefaultUserServer)
	served := map[string]appstate.PatchInfo{
		"critical_block": appstate.BuildSettingPushName("first sync name"),
		"critical_unblock_low": {Type: appstate.WAPatchCriticalUnblockLow, Mutations: []appstate.MutationInfo{{
			Index: []string{appstate.IndexContact, contact.String()}, Version: 2,
			Value: &waSyncAction.SyncActionValue{ContactAction: &waSyncAction.ContactAction{FullName: proto.String("First Sync")}},
		}}},
	}
	type world struct {
		*regularTransport
		log      *lockedLog
		mu       sync.Mutex
		requests map[string][]startupRequest
	}
	// answer decides what the peer says to a request for a critical
	// collection; nil serves the collection's first patch.
	open := func(t *testing.T, answer func(w *world, name string, patch waBinary.Node) ([]waBinary.Node, string)) *world {
		t.Helper()
		log := &lockedLog{}
		w := &world{regularTransport: newRegularTransport(t, root, out.NewEventWriter(log, true)), log: log, requests: map[string][]startupRequest{}}
		patches := map[string]waBinary.Node{}
		for name, info := range served {
			raw, err := w.server.processor.EncodePatch(w.ctx, w.server.keyID, appstate.HashState{}, info)
			if err != nil {
				t.Fatal(err)
			}
			var patch waServerSync.SyncdPatch
			if err := proto.Unmarshal(raw, &patch); err != nil {
				t.Fatal(err)
			}
			patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(1)}
			if raw, err = proto.Marshal(&patch); err != nil {
				t.Fatal(err)
			}
			patches[name] = waBinary.Node{Tag: "patch", Content: raw}
		}
		w.server.others = func(name string, version uint64, snapshot bool) ([]waBinary.Node, string) {
			w.mu.Lock()
			w.requests[name] = append(w.requests[name], startupRequest{version, snapshot})
			w.mu.Unlock()
			patch, critical := patches[name]
			switch {
			case !critical || version >= 1:
				return nil, ""
			case answer != nil:
				return answer(w, name, patch)
			}
			return []waBinary.Node{patch}, ""
		}
		return w
	}
	start := func(t *testing.T, w *world) map[string]map[string]any {
		t.Helper()
		if _, err := w.app.Sync(w.ctx, app.SyncOptions{Mode: app.SyncModeOnce, IdleExit: 50 * time.Millisecond}); err != nil {
			t.Fatalf("the start was aborted: %v", err)
		}
		return w.log.warnings(t)
	}
	cursor := func(t *testing.T, w *world, name string) (uint64, [128]byte) {
		t.Helper()
		version, hash, err := w.cli.Store.AppState.GetAppStateVersion(w.ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		return version, hash
	}
	// once: the peer was asked for the collection exactly once, for a
	// snapshot from nothing. The start is bounded: no second attempt.
	once := func(t *testing.T, w *world, name string) {
		t.Helper()
		w.mu.Lock()
		defer w.mu.Unlock()
		if got := w.requests[name]; len(got) != 1 || got[0] != (startupRequest{snapshot: true}) {
			t.Fatalf("%s was requested as %+v, want one first sync", name, got)
		}
	}
	// nothingMore: a failure is reported and that is all. Nothing was sent
	// to the phone, no replay is requested, no collection is marked broken.
	nothingMore := func(t *testing.T, w *world, warnings map[string]map[string]any, failed string) {
		t.Helper()
		w.server.mu.Lock()
		nodes := slices.Clone(w.server.nodes)
		w.server.mu.Unlock()
		pending, err := w.app.DB().AppStateRecoveryCollections()
		if err != nil {
			t.Fatal(err)
		}
		broken, err := w.app.DB().AppStateFullSyncBrokenCollections()
		if err != nil {
			t.Fatal(err)
		}
		if slices.Contains(nodes, "message") || len(pending) != 0 || len(broken) != 0 {
			t.Fatalf("nodes sent besides the fetches: %v; replay requests: %v; known broken: %v", nodes, pending, broken)
		}
		warning := warnings["app_state_sync_failed"]
		if warning["name"] != failed || warnings["app_state_lthash_mismatch"] != nil || warnings["app_state_full_sync_failed"] != nil {
			t.Fatalf("warnings: %v", warnings)
		}
		if version, _ := cursor(t, w, failed); version != 0 {
			t.Fatalf("%s has a cursor at v%d after a failed first sync", failed, version)
		}
	}

	t.Run("the server has both collections", func(t *testing.T) {
		w := open(t, nil)
		warnings := start(t, w)
		for name := range served {
			once(t, w, name)
			if version, _ := cursor(t, w, name); version != 1 {
				t.Fatalf("%s: cursor at v%d after its first sync", name, version)
			}
		}
		if stored, err := w.app.DB().GetContact(contact.String()); err != nil || stored.Name != "First Sync" {
			t.Fatalf("contact of critical_unblock_low: %+v, %v", stored, err)
		}
		if w.cli.Store.PushName != "first sync name" {
			t.Fatalf("push name of critical_block: %q", w.cli.Store.PushName)
		}
		if warnings["app_state_sync_failed"] != nil || warnings["app_state_first_sync_skipped"] != nil {
			t.Fatalf("warnings: %v", warnings)
		}
		// A second start has cursors: it asks for what is new, once.
		w.mu.Lock()
		clear(w.requests)
		w.mu.Unlock()
		start(t, w)
		w.mu.Lock()
		defer w.mu.Unlock()
		for name := range served {
			if got := w.requests[name]; len(got) != 1 || got[0] != (startupRequest{version: 1}) {
				t.Fatalf("second start requested %s as %+v, want one incremental fetch from v1", name, got)
			}
		}
	})
	t.Run("the first sync does not verify", func(t *testing.T) {
		w := open(t, func(w *world, name string, patch waBinary.Node) ([]waBinary.Node, string) {
			if name != "critical_block" {
				return []waBinary.Node{patch}, ""
			}
			// The same patch, encoded on a base the client does not have.
			raw, err := w.server.processor.EncodePatch(w.ctx, w.server.keyID, appstate.HashState{Hash: [128]byte{1}}, served[name])
			if err != nil {
				t.Error(err)
			}
			var bad waServerSync.SyncdPatch
			if err := proto.Unmarshal(raw, &bad); err != nil {
				t.Error(err)
			}
			bad.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(1)}
			raw, _ = proto.Marshal(&bad)
			return []waBinary.Node{{Tag: "patch", Content: raw}}, ""
		})
		warnings := start(t, w)
		once(t, w, "critical_block")
		once(t, w, "critical_unblock_low")
		if !strings.Contains(warnings["app_state_sync_failed"]["error"].(string), "mismatching LTHash") {
			t.Fatalf("warning: %v", warnings["app_state_sync_failed"])
		}
		nothingMore(t, w, warnings, "critical_block")
		// The other collection is not held back by it.
		if version, _ := cursor(t, w, "critical_unblock_low"); version != 1 {
			t.Fatalf("critical_unblock_low: cursor at v%d", version)
		}
	})
	t.Run("the server refuses with 429", func(t *testing.T) {
		w := open(t, func(_ *world, name string, patch waBinary.Node) ([]waBinary.Node, string) {
			if name == "critical_unblock_low" {
				return nil, "429"
			}
			return []waBinary.Node{patch}, ""
		})
		warnings := start(t, w)
		once(t, w, "critical_block")
		once(t, w, "critical_unblock_low")
		if !strings.Contains(warnings["app_state_sync_failed"]["error"].(string), "429") {
			t.Fatalf("warning: %v", warnings["app_state_sync_failed"])
		}
		nothingMore(t, w, warnings, "critical_unblock_low")
	})
	t.Run("the full sync is known broken", func(t *testing.T) {
		w := open(t, nil)
		if err := w.app.DB().SetAppStateFullSyncBroken("critical_block", true); err != nil {
			t.Fatal(err)
		}
		warnings := start(t, w)
		w.mu.Lock()
		asked := len(w.requests["critical_block"])
		w.mu.Unlock()
		if version, _ := cursor(t, w, "critical_block"); asked != 0 || version != 0 || warnings["app_state_first_sync_skipped"]["name"] != "critical_block" {
			t.Fatalf("known-broken critical_block: %d requests, cursor v%d, warnings %v", asked, version, warnings)
		}
		once(t, w, "critical_unblock_low")
	})
	t.Run("a key share syncs the collection meanwhile", func(t *testing.T) {
		// While the start's request is on its way, the collection is synced
		// by someone else, to a newer state than the start's answer holds.
		acquired := [128]byte{}
		w := open(t, func(w *world, name string, patch waBinary.Node) ([]waBinary.Node, string) {
			if name == "critical_block" {
				if err := w.cli.Store.AppState.PutAppStateVersion(w.ctx, name, 3, acquired); err != nil {
					t.Error(err)
				}
			}
			return []waBinary.Node{patch}, ""
		})
		warnings := start(t, w)
		once(t, w, "critical_block")
		if version, hash := cursor(t, w, "critical_block"); version != 3 || hash != acquired {
			t.Fatalf("state acquired meanwhile was replaced: v%d", version)
		}
		if w.cli.Store.PushName == "first sync name" || warnings["app_state_sync_failed"] != nil {
			t.Fatalf("the stale answer was applied or reported: push name %q, warnings %v", w.cli.Store.PushName, warnings)
		}
	})
}
