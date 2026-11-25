package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/sparshchadha/sparshur-service/internal/bridge"
	"github.com/sparshchadha/sparshur-service/internal/config"
	"github.com/sparshchadha/sparshur-service/internal/relay"
	"github.com/sparshchadha/sparshur-service/internal/transfer"
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
	broker := transfer.New(log, transfer.Options{Wait: time.Second, IdleTimeout: time.Second, MaxActive: 16})
	ts := httptest.NewServer(New(cfg, hub, broker, log).Handler())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		broker.Close()
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

// The tests below are ported from the Sparshur-Service master branch
// (bb9f96c, PR #1) so both suites run against this implementation.

func TestSimultaneousConnect(t *testing.T) {
	ts, _ := newTestServer(t)
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/rooms/" + testRoom + "/ws?role=" + string(relay.RoleAndroid)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type result struct {
		c   *websocket.Conn
		err error
	}
	dialed := make(chan result, 1)
	go func() {
		c, _, err := websocket.Dial(ctx, url, nil)
		dialed <- result{c, err}
	}()
	mac := dial(t, ts, testRoom, relay.RoleMac)
	res := <-dialed
	if res.err != nil {
		t.Fatalf("dial android: %v", res.err)
	}
	android := res.c
	t.Cleanup(func() { android.CloseNow() })

	// Whatever the arrival order, each side gets its welcome first and ends
	// up seeing the other: exactly one was first and later gets peer_joined.
	macWelcome, androidWelcome := readControl(t, mac), readControl(t, android)
	if macWelcome.Type != relay.MsgWelcome || androidWelcome.Type != relay.MsgWelcome {
		t.Fatalf("welcomes = %+v, %+v", macWelcome, androidWelcome)
	}
	first, joined := mac, relay.RoleAndroid
	switch {
	case *macWelcome.PeerPresent && !*androidWelcome.PeerPresent:
		first, joined = android, relay.RoleMac
	case !*macWelcome.PeerPresent && *androidWelcome.PeerPresent:
	default:
		t.Fatalf("inconsistent presence: mac %+v android %+v", macWelcome, androidWelcome)
	}
	if m := readControl(t, first); m.Type != relay.MsgPeerJoined || m.Role != joined {
		t.Fatalf("first peer got %+v", m)
	}

	write(t, mac, websocket.MessageBinary, []byte("m"))
	write(t, android, websocket.MessageBinary, []byte("a"))
	if _, got := read(t, android); string(got) != "m" {
		t.Fatalf("android got %q", got)
	}
	if _, got := read(t, mac); string(got) != "a" {
		t.Fatalf("mac got %q", got)
	}
}

func TestRejoinAfterDisconnect(t *testing.T) {
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
	write(t, mac, websocket.MessageBinary, []byte("again"))
	if _, got := read(t, android); string(got) != "again" {
		t.Fatalf("android got %q", got)
	}
}

func TestReconnectReplacesOldAndroidConnection(t *testing.T) {
	ts, _ := newTestServer(t)
	mac, oldAndroid := pair(t, ts)

	newAndroid := dial(t, ts, testRoom, relay.RoleAndroid)
	if m := readControl(t, newAndroid); m.Type != relay.MsgWelcome || !*m.PeerPresent {
		t.Fatalf("new android welcome = %+v", m)
	}
	expectClose(t, oldAndroid, relay.StatusReplaced)

	write(t, mac, websocket.MessageBinary, []byte("after reconnect"))
	if _, got := read(t, newAndroid); string(got) != "after reconnect" {
		t.Fatalf("new android got %q", got)
	}
	// The mac saw no presence churn: its next frame is the android's data.
	write(t, newAndroid, websocket.MessageBinary, []byte("reply"))
	if typ, got := read(t, mac); typ != websocket.MessageBinary || string(got) != "reply" {
		t.Fatalf("mac got %v %q", typ, got)
	}
}

func TestTransferRouting(t *testing.T) {
	ts, hub := newTestServer(t)
	path := "/v1/rooms/" + testRoom + "/transfers/transfer-0123456789abcdef"
	put, err := http.NewRequest(http.MethodPut, ts.URL+path, strings.NewReader("opaque encrypted bytes"))
	if err != nil {
		t.Fatal(err)
	}
	upload := make(chan error, 1)
	go func() {
		resp, err := ts.Client().Do(put)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				err = fmt.Errorf("PUT status = %d", resp.StatusCode)
			}
		}
		upload <- err
	}()
	resp, err := ts.Client().Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || string(body) != "opaque encrypted bytes" {
		t.Fatalf("GET status = %d, body = %q: %v", resp.StatusCode, body, err)
	}
	if err := <-upload; err != nil {
		t.Fatal(err)
	}

	for _, method := range []string{http.MethodPut, http.MethodGet} {
		for _, invalid := range []string{
			"/v1/rooms/short/transfers/transfer-0123456789abcdef",
			"/v1/rooms/" + testRoom + "/transfers/short",
			"/v1/rooms/" + testRoom + "/transfers/invalid!0123456789abcdef",
			"/v1/rooms/" + testRoom + "/transfers/" + strings.Repeat("a", 129),
		} {
			req, _ := http.NewRequest(method, ts.URL+invalid, nil)
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("%s %s status = %d", method, invalid, resp.StatusCode)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := hub.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{http.MethodPut, http.MethodGet} {
		req, _ := http.NewRequest(method, ts.URL+path, nil)
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("draining %s status = %d", method, resp.StatusCode)
		}
	}
}

func TestBridgeAndStreamingShareServer(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := relay.NewHub(log)
	broker := transfer.New(log, transfer.Options{Wait: time.Second, IdleTimeout: time.Second, MaxActive: 16})
	defer broker.Close()
	enrollment := filepath.Join(t.TempDir(), "enrollment.json")
	if err := bridge.WriteEnrollment(enrollment, bridge.Enrollment{Devices: []bridge.Device{}, Pairings: []bridge.Pairing{}}); err != nil {
		t.Fatal(err)
	}
	authenticated, err := bridge.NewHub(bridge.Options{
		PublicURL: "wss://relay.example/v1/relay", RelayID: "test", EnrollmentFile: enrollment,
		QueueSize: 4, WriteTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	handler := New(config.Config{}, hub, broker, log).WithBridge(authenticated).Handler()

	const path = "/v1/rooms/" + testRoom + "/transfers/transfer-0123456789abcdef"
	waiting := make(chan struct{})
	received := httptest.NewRecorder()
	go func() {
		defer close(waiting)
		handler.ServeHTTP(received, httptest.NewRequest(http.MethodGet, path, nil))
	}()
	deadline := time.Now().Add(time.Second)
	for broker.Active() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("receiver did not enter broker")
		}
		time.Sleep(time.Millisecond)
	}
	checkReadiness := func(wantStatus, wantTransfers int) {
		t.Helper()
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		var body struct {
			Transfers             *int `json:"transfers"`
			AuthenticatedSessions *int `json:"authenticated_sessions"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != wantStatus || body.Transfers == nil || *body.Transfers != wantTransfers ||
			body.AuthenticatedSessions == nil || *body.AuthenticatedSessions != 0 {
			t.Fatalf("ready status = %d, body = %s", w.Code, w.Body.String())
		}
	}
	checkBridgeRoute := func(wantStatus int) {
		t.Helper()
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, bridge.Path, nil))
		if w.Code != wantStatus {
			t.Fatalf("bridge status = %d, want %d", w.Code, wantStatus)
		}
	}
	checkBridgeRoute(http.StatusBadRequest) // Missing subprotocol.
	checkReadiness(http.StatusOK, 1)

	uploaded := httptest.NewRecorder()
	const payload = "opaque encrypted bytes"
	handler.ServeHTTP(uploaded, httptest.NewRequest(http.MethodPut, path, strings.NewReader(payload)))
	<-waiting
	if uploaded.Code != http.StatusOK || received.Code != http.StatusOK || received.Body.String() != payload {
		t.Fatalf("streaming with bridge enabled: PUT %d, GET %d body %q", uploaded.Code, received.Code, received.Body)
	}
	checkReadiness(http.StatusOK, 0)

	if err := authenticated.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	checkBridgeRoute(http.StatusServiceUnavailable)
	checkReadiness(http.StatusServiceUnavailable, 0)
}

func TestReadyIncludesTransfers(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	hub := relay.NewHub(log)
	broker := transfer.New(log, transfer.Options{Wait: time.Second, IdleTimeout: time.Second, MaxActive: 16})
	defer broker.Close()
	s := New(config.Config{}, hub, broker, log)
	waiting := make(chan struct{})
	go func() {
		defer close(waiting)
		broker.Receive(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil), testRoom, "transfer-0123456789abcdef")
	}()
	deadline := time.Now().Add(time.Second)
	for broker.Active() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("receiver did not enter broker")
		}
		time.Sleep(time.Millisecond)
	}
	for _, draining := range []bool{false, true} {
		wantStatus, wantTransfers := http.StatusOK, float64(1)
		if draining {
			if err := hub.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
			broker.Close()
			<-waiting
			wantStatus, wantTransfers = http.StatusServiceUnavailable, 0
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		var body map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != wantStatus || body["transfers"] != wantTransfers {
			t.Fatalf("ready status = %d, body = %v", w.Code, body)
		}
	}
}
