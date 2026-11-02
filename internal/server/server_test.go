package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/sparshchadha/sparshur-service/internal/config"
	"github.com/sparshchadha/sparshur-service/internal/relay"
)

const testRoom = "test-room-0123456789abcdef"

func newTestServer(t *testing.T, opts ...func(*config.Config)) (*httptest.Server, *relay.Hub) {
	t.Helper()
	cfg := config.Config{
		MaxMessageBytes: 1 << 16,
		SendQueueSize:   16,
		PingInterval:    time.Second,
		WriteTimeout:    time.Second,
	}
	for _, o := range opts {
		o(&cfg)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := relay.NewHub(log)
	ts := httptest.NewServer(New(cfg, hub, log).Handler())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hub.Shutdown(ctx)
		ts.Close()
	})
	return ts, hub
}

func wsURL(ts *httptest.Server, room string, role relay.Role) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/rooms/" + room + "/ws?role=" + string(role)
}

func dial(t *testing.T, ts *httptest.Server, room string, role relay.Role) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, wsURL(ts, room, role), nil)
	if err != nil {
		t.Fatalf("dial %s: %v", role, err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return c
}

func read(t *testing.T, c *websocket.Conn) (websocket.MessageType, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return typ, data
}

func readControl(t *testing.T, c *websocket.Conn) relay.ControlMessage {
	t.Helper()
	typ, data := read(t, c)
	if typ != websocket.MessageText {
		t.Fatalf("expected text control frame, got %v", typ)
	}
	var m relay.ControlMessage
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("decode control: %v", err)
	}
	return m
}

func write(t *testing.T, c *websocket.Conn, typ websocket.MessageType, data []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, typ, data); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func expectClose(t *testing.T, c *websocket.Conn, want websocket.StatusCode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		_, _, err := c.Read(ctx)
		if err == nil {
			continue
		}
		if got := websocket.CloseStatus(err); got != want {
			t.Fatalf("close status = %v, want %v (err %v)", got, want, err)
		}
		return
	}
}

// pair connects mac then android and drains the presence handshake.
func pair(t *testing.T, ts *httptest.Server) (mac, android *websocket.Conn) {
	t.Helper()
	mac = dial(t, ts, testRoom, relay.RoleMac)
	if m := readControl(t, mac); m.Type != relay.MsgWelcome || *m.PeerPresent {
		t.Fatalf("mac welcome = %+v", m)
	}
	android = dial(t, ts, testRoom, relay.RoleAndroid)
	if m := readControl(t, android); m.Type != relay.MsgWelcome || !*m.PeerPresent {
		t.Fatalf("android welcome = %+v", m)
	}
	if m := readControl(t, mac); m.Type != relay.MsgPeerJoined || m.Role != relay.RoleAndroid {
		t.Fatalf("mac peer_joined = %+v", m)
	}
	return mac, android
}

func TestRelayForwardsBinaryBothWays(t *testing.T) {
	ts, _ := newTestServer(t)
	mac, android := pair(t, ts)

	toAndroid := []byte{0x00, 0x01, 0xfe, 0xff}
	write(t, mac, websocket.MessageBinary, toAndroid)
	if typ, got := read(t, android); typ != websocket.MessageBinary || !bytes.Equal(got, toAndroid) {
		t.Fatalf("android got %v %x", typ, got)
	}

	toMac := []byte("ciphertext")
	write(t, android, websocket.MessageBinary, toMac)
	if typ, got := read(t, mac); typ != websocket.MessageBinary || !bytes.Equal(got, toMac) {
		t.Fatalf("mac got %v %x", typ, got)
	}
}

func TestPeerLeftNotification(t *testing.T) {
	ts, _ := newTestServer(t)
	mac, android := pair(t, ts)

	android.Close(websocket.StatusNormalClosure, "")
	if m := readControl(t, mac); m.Type != relay.MsgPeerLeft || m.Role != relay.RoleAndroid {
		t.Fatalf("mac got %+v", m)
	}
}

func TestTextFramesRejected(t *testing.T) {
	ts, _ := newTestServer(t)
	mac, _ := pair(t, ts)

	write(t, mac, websocket.MessageText, []byte("hello"))
	expectClose(t, mac, websocket.StatusPolicyViolation)
}

func TestReconnectReplacesOldConnection(t *testing.T) {
	ts, _ := newTestServer(t)
	oldMac, android := pair(t, ts)

	newMac := dial(t, ts, testRoom, relay.RoleMac)
	if m := readControl(t, newMac); m.Type != relay.MsgWelcome || !*m.PeerPresent {
		t.Fatalf("new mac welcome = %+v", m)
	}
	expectClose(t, oldMac, relay.StatusReplaced)

	// The android side sees no presence churn and frames reach the new mac.
	write(t, android, websocket.MessageBinary, []byte("after reconnect"))
	if _, got := read(t, newMac); string(got) != "after reconnect" {
		t.Fatalf("new mac got %q", got)
	}
}

func TestRoomsAreIsolated(t *testing.T) {
	ts, _ := newTestServer(t)
	mac, _ := pair(t, ts)

	other := dial(t, ts, "another-room-0123456789abc", relay.RoleAndroid)
	if m := readControl(t, other); *m.PeerPresent {
		t.Fatalf("other room should be empty, got %+v", m)
	}
	write(t, other, websocket.MessageBinary, []byte("nope"))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, data, err := mac.Read(ctx); err == nil {
		t.Fatalf("mac received frame from another room: %q", data)
	}
}

func TestBadRequests(t *testing.T) {
	ts, _ := newTestServer(t)
	cases := map[string]string{
		"missing role": "/v1/rooms/" + testRoom + "/ws",
		"bad role":     "/v1/rooms/" + testRoom + "/ws?role=ios",
		"short room":   "/v1/rooms/abc/ws?role=mac",
	}
	for name, path := range cases {
		t.Run(name, func(t *testing.T) {
			resp, err := http.Get(ts.URL + path)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d", resp.StatusCode)
			}
		})
	}
}

func TestCreateRoom(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, err := http.Post(ts.URL+"/v1/rooms", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var body struct {
		RoomID string `json:"room_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if !relay.ValidRoomID(body.RoomID) {
		t.Fatalf("invalid room id %q", body.RoomID)
	}
}

func TestOversizedMessageClosesSender(t *testing.T) {
	ts, _ := newTestServer(t)
	mac, android := pair(t, ts)
	android.SetReadLimit(1 << 17) // the client library defaults to 32 KiB

	// The limit applies to whole WebSocket messages; exactly the limit passes.
	limit := make([]byte, 1<<16)
	write(t, mac, websocket.MessageBinary, limit)
	if _, got := read(t, android); len(got) != len(limit) {
		t.Fatalf("android got %d bytes", len(got))
	}

	write(t, mac, websocket.MessageBinary, make([]byte, 1<<16+1))
	expectClose(t, mac, websocket.StatusMessageTooBig)
	if m := readControl(t, android); m.Type != relay.MsgPeerLeft {
		t.Fatalf("android got %+v", m)
	}
}

func TestMessagesDroppedWhilePeerOffline(t *testing.T) {
	ts, _ := newTestServer(t)
	mac := dial(t, ts, testRoom, relay.RoleMac)
	readControl(t, mac)
	write(t, mac, websocket.MessageBinary, []byte("nobody home"))

	// Barrier: the relay's read loop handles mac's frames in order and
	// answers a ping only after it has dropped the message sent before it.
	// Ping needs a concurrent reader on mac to receive the pong; that reader
	// then picks up the peer_joined below.
	type result struct {
		typ  websocket.MessageType
		data []byte
		err  error
	}
	next := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		typ, data, err := mac.Read(ctx)
		next <- result{typ, data, err}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := mac.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	android := dial(t, ts, testRoom, relay.RoleAndroid)
	if m := readControl(t, android); m.Type != relay.MsgWelcome || !*m.PeerPresent {
		t.Fatalf("android welcome = %+v", m)
	}
	r := <-next
	if r.err != nil {
		t.Fatalf("mac read: %v", r.err)
	}
	var m relay.ControlMessage
	if r.typ != websocket.MessageText || json.Unmarshal(r.data, &m) != nil || m.Type != relay.MsgPeerJoined {
		t.Fatalf("mac got %v %q, want peer_joined", r.typ, r.data)
	}
	write(t, mac, websocket.MessageBinary, []byte("now"))
	if _, got := read(t, android); string(got) != "now" {
		t.Fatalf("android got %q; offline message was not dropped", got)
	}
}

func TestReconnectAfterLeaveNotifiesInOrder(t *testing.T) {
	ts, _ := newTestServer(t)
	mac, android := pair(t, ts)

	android.Close(websocket.StatusNormalClosure, "")
	if m := readControl(t, mac); m.Type != relay.MsgPeerLeft || m.Role != relay.RoleAndroid {
		t.Fatalf("mac got %+v", m)
	}
	android = dial(t, ts, testRoom, relay.RoleAndroid)
	if m := readControl(t, android); m.Type != relay.MsgWelcome || !*m.PeerPresent {
		t.Fatalf("android welcome = %+v", m)
	}
	if m := readControl(t, mac); m.Type != relay.MsgPeerJoined || m.Role != relay.RoleAndroid {
		t.Fatalf("mac got %+v", m)
	}
}

func TestOriginPolicy(t *testing.T) {
	ts, _ := newTestServer(t, func(c *config.Config) { c.AllowedOrigins = []string{"app.example"} })
	cases := map[string]struct {
		origin string
		ok     bool
	}{
		"native client": {"", true},
		"allowed":       {"https://app.example", true},
		"disallowed":    {"https://evil.example", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			h := http.Header{}
			if tc.origin != "" {
				h.Set("Origin", tc.origin)
			}
			c, resp, err := websocket.Dial(ctx, wsURL(ts, testRoom, relay.RoleMac), &websocket.DialOptions{HTTPHeader: h})
			if tc.ok {
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				c.Close(websocket.StatusNormalClosure, "")
				return
			}
			if err == nil {
				c.CloseNow()
				t.Fatal("dial succeeded")
			}
			if resp == nil || resp.StatusCode != http.StatusForbidden {
				t.Fatalf("resp = %v, err = %v", resp, err)
			}
		})
	}
}

func getJSON(t *testing.T, url string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, body
}

func TestHealthAndReadiness(t *testing.T) {
	ts, hub := newTestServer(t)
	if code, body := getJSON(t, ts.URL+"/healthz"); code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz = %d %v", code, body)
	}

	mac, android := pair(t, ts)
	code, body := getJSON(t, ts.URL+"/readyz")
	stats, _ := body["stats"].(map[string]any)
	if code != http.StatusOK || body["status"] != "ok" || stats["rooms"] != 1.0 || stats["peers"] != 2.0 {
		t.Fatalf("readyz = %d %v", code, body)
	}

	mac.CloseNow()
	android.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if code, body := getJSON(t, ts.URL+"/readyz"); code != http.StatusServiceUnavailable || body["status"] != "draining" {
		t.Fatalf("readyz while draining = %d %v", code, body)
	}
	if code, _ := getJSON(t, ts.URL+"/healthz"); code != http.StatusOK {
		t.Fatalf("healthz while draining = %d", code)
	}
	_, resp, err := websocket.Dial(ctx, wsURL(ts, testRoom, relay.RoleMac), nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("dial while draining: resp = %v, err = %v", resp, err)
	}
}
