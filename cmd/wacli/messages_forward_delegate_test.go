package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/openclaw/wacli/internal/app"
	"github.com/openclaw/wacli/internal/lock"
	"github.com/openclaw/wacli/internal/store"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/types"
)

const (
	forwardTestSourceChat = "111@s.whatsapp.net"
	forwardTestRecipient  = "222@s.whatsapp.net"
)

type fakeForwardWA struct {
	app.WAClient
	sends   []*waProto.Message
	sentTo  []types.JID
	sendErr error
}

func (f *fakeForwardWA) IsConnected() bool { return true }
func (f *fakeForwardWA) Disconnect()       {}
func (f *fakeForwardWA) IsOnWhatsApp(context.Context, []string) ([]types.IsOnWhatsAppResponse, error) {
	return nil, nil
}
func (f *fakeForwardWA) GetUserInfo(context.Context, []types.JID) (map[types.JID]types.UserInfo, error) {
	return nil, nil
}
func (f *fakeForwardWA) ResolveChatName(context.Context, types.JID, string) string {
	return "Recipient"
}
func (f *fakeForwardWA) SendProtoMessage(_ context.Context, to types.JID, msg *waProto.Message) (types.MessageID, error) {
	f.sends = append(f.sends, msg)
	f.sentTo = append(f.sentTo, to)
	if f.sendErr != nil {
		return "", f.sendErr
	}
	return "FORWARDED-1", nil
}

// forwardTestApp is a real store with a fake WhatsApp client: nothing reaches
// the network.
type forwardTestApp struct {
	*app.App
	client *fakeForwardWA
}

func (a *forwardTestApp) WA() app.WAClient { return a.client }
func (a *forwardTestApp) Connect(context.Context, bool, func(string)) error {
	return errors.New("test app must not reconnect")
}

func newForwardTestApp(t *testing.T) *forwardTestApp {
	t.Helper()
	base, err := app.New(app.Options{StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(base.Close)
	now := time.Now().UTC()
	if err := base.DB().UpsertChat(forwardTestSourceChat, "dm", "Source", now); err != nil {
		t.Fatal(err)
	}
	for _, msg := range []store.UpsertMessageParams{
		{ChatJID: forwardTestSourceChat, MsgID: "TEXT", SenderJID: forwardTestSourceChat, Timestamp: now, Text: "hola", ForwardingScore: 1},
		{ChatJID: forwardTestSourceChat, MsgID: "PHOTO", SenderJID: forwardTestSourceChat, Timestamp: now, MediaType: "image", MediaCaption: "foto", MimeType: "image/jpeg", DirectPath: "/v/photo", MediaKey: []byte{1}, FileSHA256: []byte{2}, FileEncSHA256: []byte{3}, FileLength: 10},
		{ChatJID: forwardTestSourceChat, MsgID: "GONE", SenderJID: forwardTestSourceChat, Timestamp: now, Text: "x", Revoked: true},
		{ChatJID: forwardTestSourceChat, MsgID: "REACTION", SenderJID: forwardTestSourceChat, Timestamp: now, Text: "x", ReactionToID: "TEXT"},
	} {
		if err := base.DB().UpsertMessage(msg); err != nil {
			t.Fatal(err)
		}
	}
	return &forwardTestApp{App: base, client: &fakeForwardWA{}}
}

func forwardRequest(id, to string) sendDelegateRequest {
	return sendDelegateRequest{Version: sendDelegateVersion, Kind: forwardKind, Chat: forwardTestSourceChat, ID: id, To: to}
}

func TestSendDelegateRequestPreservesForwardInJSON(t *testing.T) {
	raw, err := json.Marshal(sendDelegateRequest{Version: sendDelegateVersion, Kind: forwardKind, Chat: forwardTestSourceChat, ID: "TEXT", To: "Family", Pick: 2, PostSendWaitMS: 25})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"chat":"111@s.whatsapp.net"`) {
		t.Fatalf("encoded request missing source chat: %s", raw)
	}
	var got sendDelegateRequest
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.Kind != forwardKind || got.Chat != forwardTestSourceChat || got.ID != "TEXT" || got.To != "Family" || got.Pick != 2 || got.PostSendWaitMS != 25 {
		t.Fatalf("decoded request = %+v", got)
	}

	raw, err = json.Marshal(sendDelegateResponse{OK: true, Sent: true, To: forwardTestRecipient, ID: "FORWARDED-1", Source: "TEXT", StoreWarning: "disk full"})
	if err != nil {
		t.Fatal(err)
	}
	var resp sendDelegateResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Source != "TEXT" || resp.StoreWarning != "disk full" || resp.ID != "FORWARDED-1" {
		t.Fatalf("decoded response = %+v", resp)
	}
}

func TestExecuteDelegatedForwardSendsAndStoresForwardedCopy(t *testing.T) {
	a := newForwardTestApp(t)
	resp, err := executeDelegatedForward(t.Context(), a, forwardRequest("TEXT", forwardTestRecipient))
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK || !resp.Sent || resp.To != forwardTestRecipient || resp.ID != "FORWARDED-1" || resp.Source != "TEXT" || resp.StoreWarning != "" {
		t.Fatalf("response = %+v", resp)
	}
	if len(a.client.sends) != 1 || a.client.sentTo[0].String() != forwardTestRecipient {
		t.Fatalf("sends = %d to %v, want exactly one to %s", len(a.client.sends), a.client.sentTo, forwardTestRecipient)
	}
	text := a.client.sends[0].GetExtendedTextMessage()
	if text.GetText() != "hola" || !text.GetContextInfo().GetIsForwarded() || text.GetContextInfo().GetForwardingScore() != 2 {
		t.Fatalf("payload = %v", a.client.sends[0])
	}
	stored, err := a.DB().GetMessage(forwardTestRecipient, "FORWARDED-1")
	if err != nil || !stored.FromMe || !stored.IsForwarded || stored.Text != "hola" {
		t.Fatalf("outgoing copy = %+v, err=%v", stored, err)
	}
}

func TestExecuteDelegatedForwardReadsMediaDownloadInfo(t *testing.T) {
	a := newForwardTestApp(t)
	if _, err := executeDelegatedForward(t.Context(), a, forwardRequest("PHOTO", forwardTestRecipient)); err != nil {
		t.Fatal(err)
	}
	if len(a.client.sends) != 1 {
		t.Fatalf("sends = %d", len(a.client.sends))
	}
	image := a.client.sends[0].GetImageMessage()
	if image.GetDirectPath() != "/v/photo" || image.GetMimetype() != "image/jpeg" || image.GetCaption() != "foto" || !image.GetContextInfo().GetIsForwarded() {
		t.Fatalf("media payload = %v", a.client.sends[0])
	}
}

func TestExecuteDelegatedForwardValidatesBeforeSending(t *testing.T) {
	for name, req := range map[string]sendDelegateRequest{
		"missing chat":      {Version: sendDelegateVersion, Kind: forwardKind, ID: "TEXT", To: forwardTestRecipient},
		"missing id":        {Version: sendDelegateVersion, Kind: forwardKind, Chat: forwardTestSourceChat, To: forwardTestRecipient},
		"missing to":        forwardRequest("TEXT", ""),
		"unknown message":   forwardRequest("NOPE", forwardTestRecipient),
		"revoked":           forwardRequest("GONE", forwardTestRecipient),
		"reaction":          forwardRequest("REACTION", forwardTestRecipient),
		"media to channel":  forwardRequest("PHOTO", "123@newsletter"),
		"unknown recipient": forwardRequest("TEXT", "Nobody Matches This"),
	} {
		t.Run(name, func(t *testing.T) {
			a := newForwardTestApp(t)
			if _, err := executeDelegatedForward(t.Context(), a, req); err == nil || len(a.client.sends) != 0 {
				t.Fatalf("invalid forward reached WhatsApp: err=%v sends=%d", err, len(a.client.sends))
			}
		})
	}
}

func TestExecuteDelegatedSendRoutesForwardKind(t *testing.T) {
	a, err := app.New(app.Options{StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	_, err = executeDelegatedSend(t.Context(), a, sendDelegateRequest{Version: sendDelegateVersion, Kind: forwardKind, To: forwardTestRecipient, ID: "TEXT"})
	if err == nil || !strings.Contains(err.Error(), "--chat, --id, and --to are required") {
		t.Fatalf("forward kind not routed to its executor: %v", err)
	}
}

func TestMessagesForwardDelegatesThroughProductionServerWhenStoreLocked(t *testing.T) {
	skipPresenceDelegateSocketTestOnUnsupportedOS(t)
	storeDir := shortPresenceDelegateStoreDir(t)
	lk, err := lock.Acquire(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lk.Release()

	owner := newForwardTestApp(t)
	requests := make(chan sendDelegateRequest, 1)
	stop, err := startSendDelegateServerForStore(t.Context(), storeDir, sendSpacing{}, func(ctx context.Context, got sendDelegateRequest) (sendDelegateResponse, error) {
		requests <- got
		resp, err := executeDelegatedForward(ctx, owner, got)
		resp.StoreWarning = "message update: disk full"
		return resp, err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	stdout, stderr, err := runPresenceDelegateHelper(t, []string{
		"--store", storeDir, "--json", "--timeout", "750ms",
		"messages", "forward", "--chat", forwardTestSourceChat, "--id", "TEXT", "--to", forwardTestRecipient, "--pick", "2", "--post-send-wait", "25ms",
	})
	if err != nil {
		t.Fatalf("delegation failed: %v stdout=%s stderr=%s", err, stdout, stderr)
	}
	got := <-requests
	if got.Kind != forwardKind || got.Version != sendDelegateVersion || got.Chat != forwardTestSourceChat || got.ID != "TEXT" || got.To != forwardTestRecipient || got.Pick != 2 || got.PostSendWaitMS != 25 || got.TimeoutMS != 750 || got.DeadlineUnixMS == 0 {
		t.Fatalf("request = %+v", got)
	}
	if strings.Contains(stderr, "store is locked") || !strings.Contains(stderr, "message delivered (id FORWARDED-1) but local history update failed: message update: disk full") {
		t.Fatalf("stderr = %q", stderr)
	}
	// The standalone path renders through the same writer; compare raw JSON so
	// keys and the store warning cannot drift.
	direct := captureRootStdout(t, func() {
		_ = writeForwardResult(&rootFlags{asJSON: true}, forwardTestRecipient, "FORWARDED-1", "TEXT", "message update: disk full")
	})
	commandJSON, _, _ := strings.Cut(stdout, "\n")
	if commandJSON != strings.TrimSpace(direct) {
		t.Fatalf("delegated=%s standalone=%s", stdout, direct)
	}
	for _, want := range []string{`"forwarded":true`, `"source":"TEXT"`, `"id":"FORWARDED-1"`, `"store_warning":"message update: disk full"`} {
		if !strings.Contains(commandJSON, want) {
			t.Fatalf("stdout %s missing %s", commandJSON, want)
		}
	}
}

func TestMessagesForwardStandaloneReadOnlyAndMissingSocket(t *testing.T) {
	for _, mode := range []string{"local", "read-only", "locked-no-socket", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			storeDir := t.TempDir()
			args := []string{"--store", storeDir, "--json"}
			forward := []string{"messages", "forward", "--chat", forwardTestSourceChat, "--id", "TEXT", "--to", forwardTestRecipient}
			want := "not authenticated"
			switch mode {
			case "read-only":
				args = append(args, "--read-only")
				want = "read-only mode"
			case "locked-no-socket":
				lk, err := lock.Acquire(storeDir)
				if err != nil {
					t.Fatal(err)
				}
				defer lk.Release()
				want = "store is locked"
			case "invalid":
				forward = forward[:len(forward)-2]
				want = "--chat, --id, and --to are required"
			}
			var commandErr error
			captureRootStderr(t, func() { commandErr = execute(append(args, forward...)) })
			if commandErr == nil || !strings.Contains(commandErr.Error(), want) {
				t.Fatalf("error=%v, want %s", commandErr, want)
			}
		})
	}
}

func TestMessagesForwardOldOwnerRejectionIsNotRetriedLocally(t *testing.T) {
	skipPresenceDelegateSocketTestOnUnsupportedOS(t)
	storeDir := shortPresenceDelegateStoreDir(t)
	lk, err := lock.Acquire(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer lk.Release()
	server := startPresenceDelegateTestSocket(t, storeDir, func(req sendDelegateRequest) sendDelegateResponse {
		return sendDelegateResponse{OK: false, Error: `unsupported send kind "forward"`}
	})
	defer server.stop()
	stdout, stderr, err := runPresenceDelegateHelper(t, []string{
		"--store", storeDir, "--json", "--timeout", "750ms",
		"messages", "forward", "--chat", forwardTestSourceChat, "--id", "TEXT", "--to", forwardTestRecipient,
	})
	if err == nil || !strings.Contains(stderr, `unsupported send kind \"forward\"`) || strings.Contains(stderr, "store is locked") || strings.Contains(stdout, `"forwarded":true`) {
		t.Fatalf("old owner: err=%v stdout=%q stderr=%q", err, stdout, stderr)
	}
	if req := server.nextRequest(t); req.Kind != forwardKind {
		t.Fatalf("request = %+v", req)
	}
}

// Forward is a message send: it waits for the serialized send slot and the
// shared pacer exactly like `send text`, unlike group and chat-state kinds.
func TestDelegatedForwardTakesSendSlotAndPacer(t *testing.T) {
	t.Run("occupied slot", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer clientConn.Close()
		done := make(chan struct{})
		go func() {
			defer close(done)
			handleSendDelegateConn(t.Context(), serverConn, unexpectedExecute(t), make(chan struct{}, 1), newSendPacer(sendSpacing{}))
		}()
		resp := roundTripDelegate(t, clientConn, sendDelegateRequest{Version: sendDelegateVersion, Kind: forwardKind, TimeoutMS: 20})
		<-done
		if resp.OK || !strings.Contains(resp.Error, "request timed out in the send queue before dispatch; it was not sent") {
			t.Fatalf("response = %+v, want the send-queue refusal", resp)
		}
	})
	t.Run("paced like text", func(t *testing.T) {
		for _, kind := range []string{"text", forwardKind} {
			p, slept := fakePacer(sendSpacing{min: time.Second, max: time.Second}, func(int64) int64 { return 0 })
			sendSlot := make(chan struct{}, 1)
			sendSlot <- struct{}{}
			calls := 0
			for i := 0; i < 2; i++ {
				serverConn, clientConn := net.Pipe()
				done := make(chan struct{})
				go func() {
					defer close(done)
					handleSendDelegateConn(t.Context(), serverConn, func(context.Context, sendDelegateRequest) (sendDelegateResponse, error) {
						calls++
						return sendDelegateResponse{OK: true, Sent: true}, nil
					}, sendSlot, p)
				}()
				resp := roundTripDelegate(t, clientConn, sendDelegateRequest{Version: sendDelegateVersion, Kind: kind, TimeoutMS: 5000})
				<-done
				clientConn.Close()
				if !resp.OK {
					t.Fatalf("%s send %d: %+v", kind, i, resp)
				}
			}
			if calls != 2 || len(*slept) != 1 || (*slept)[0] != time.Second {
				t.Fatalf("%s: calls=%d slept=%v, want the second send spaced by the pacer", kind, calls, *slept)
			}
		}
	})
	t.Run("uncertain after dispatch", func(t *testing.T) {
		serverConn, clientConn := net.Pipe()
		defer clientConn.Close()
		sendSlot := make(chan struct{}, 1)
		sendSlot <- struct{}{}
		go handleSendDelegateConn(t.Context(), serverConn, func(context.Context, sendDelegateRequest) (sendDelegateResponse, error) {
			return sendDelegateResponse{}, context.DeadlineExceeded
		}, sendSlot, newSendPacer(sendSpacing{}))
		resp := roundTripDelegate(t, clientConn, sendDelegateRequest{Version: sendDelegateVersion, Kind: forwardKind, TimeoutMS: 750})
		if resp.OK || !strings.Contains(resp.Error, "delegated forward failed after dispatch and may still have gone through; check before retrying") {
			t.Fatalf("response = %+v", resp)
		}
	})
}
