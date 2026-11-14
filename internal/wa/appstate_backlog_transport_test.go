package wa_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/out"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/util/keys"
)

// Review finding R3-1 at the owner: the last server notification of a
// collection arrives while the persistence owner is still busy with an
// earlier batch and the backlog is full. The owner must see it in the
// diagnostics it really has, and must end up with the notified change
// without any further traffic.
//
// Nothing here replaces the owner's wiring: the app opens the session the
// way the CLI does, so the SDK logs through the logger session.go builds
// (ERROR and up, on stderr), and lifecycle warnings go where root.go sends
// them (stderr: plain lines by default, JSON lines with --events).
func TestAppStateBacklogReachesTheOperator(t *testing.T) {
	// This test must stay nonparallel: the SDK exports a process-wide trust
	// root, and the test redirects the process's stderr.
	root := keys.NewKeyPair()
	previousRoot := whatsmeow.WACertPubKey
	whatsmeow.WACertPubKey = *root.Pub
	t.Cleanup(func() { whatsmeow.WACertPubKey = previousRoot })

	for _, mode := range []struct {
		name   string
		events bool
	}{{"default_flags", false}, {"events_flag", true}} {
		t.Run(mode.name, func(t *testing.T) {
			stderr := redirectStderr(t)
			e := newRegularTransport(t, root, out.NewEventWriter(os.Stderr, mode.events))
			e.cli.AppStateBacklogLimit = 1
			entered, release := e.holdLabel("old")
			e.persistOwner()
			e.server.foreignPatch(e.ctx, labelEdit("old"))
			fetched := make(chan error, 1)
			go func() { fetched <- e.cli.FetchAppState(e.ctx, appstate.WAPatchRegular, false, false) }()
			e.wait(entered, "fetch never dispatched its label")

			// The last change, and the last notification about it.
			e.server.foreignPatch(e.ctx, labelEdit("new"))
			e.cli.DangerousInternals().HandleAppStateNotification(e.ctx, &waBinary.Node{
				Tag:     "notification",
				Content: []waBinary.Node{{Tag: "collection", Attrs: waBinary.Attrs{"name": "regular", "version": "2"}}},
			})

			// Visible while the handler is still held.
			sdkLine := "[Client ERROR] App state regular was not synced (notification): 1 event batches are undelivered"
			refusal := stderr.waitFor(t, e, "app_state_backlog", "warning: WhatsApp app state regular was not synced (notification): 1 event batches are still being applied")
			if !strings.Contains(stderr.read(t), sdkLine) || !strings.Contains(stderr.read(t), "notified version 2") {
				t.Fatalf("SDK refusal is not in the owner's log:\n%s", stderr.read(t))
			}
			if mode.events && (refusal["name"] != "regular" || refusal["source"] != "notification" || refusal["depth"] != float64(1) || refusal["version"] != float64(2) || refusal["oldest_age_ms"] == nil) {
				t.Fatalf("app_state_backlog warning: %v", refusal)
			}
			if strings.Contains(stderr.read(t), "caught up") {
				t.Fatal("caught up before the handler was released")
			}

			release()
			if err := <-fetched; err != nil {
				t.Fatal(err)
			}
			if err := e.cli.WaitForAppStateEvents(e.ctx); err != nil {
				t.Fatal(err)
			}
			// No other notification or fetch: the SDK caught up by itself, and
			// the owner persisted what it fetched.
			e.projection("new", nil)
			caughtUp := stderr.waitFor(t, e, "app_state_caught_up", "WhatsApp app state regular caught up after its event backlog")
			if mode.events && (caughtUp["name"] != "regular" || caughtUp["version"] != float64(2)) {
				t.Fatalf("app_state_caught_up warning: %v", caughtUp)
			}
			t.Logf("OWNER DIAGNOSTICS (%s):\n%s", mode.name, stderr.read(t))
		})
	}
}

type stderrFile struct{ path string }

// redirectStderr points the process's stderr at a file until the test and
// everything it registered for cleanup afterwards are done.
func redirectStderr(t *testing.T) stderrFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "stderr")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stderr
	os.Stderr = file
	t.Cleanup(func() {
		os.Stderr = previous
		_ = file.Close()
	})
	return stderrFile{path: path}
}

func (f stderrFile) read(t *testing.T) string {
	t.Helper()
	content, err := os.ReadFile(f.path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

// waitFor returns the lifecycle warning with that code: its JSON data with
// --events, or nil once the plain line is there.
func (f stderrFile) waitFor(t *testing.T, e *regularTransport, code, plain string) map[string]any {
	t.Helper()
	for {
		for line := range strings.SplitSeq(f.read(t), "\n") {
			if strings.HasPrefix(line, plain) {
				return nil
			}
			var event struct {
				Event string         `json:"event"`
				Data  map[string]any `json:"data"`
			}
			if json.Unmarshal([]byte(line), &event) == nil && event.Event == "warning" && event.Data["code"] == code {
				if message, _ := event.Data["message"].(string); !strings.HasPrefix(message, plain) {
					t.Fatalf("%s message: %q", code, message)
				}
				return event.Data
			}
		}
		select {
		case <-e.ctx.Done():
			t.Fatalf("owner diagnostics never showed %s:\n%s", code, f.read(t))
		case <-time.After(5 * time.Millisecond):
		}
	}
}
