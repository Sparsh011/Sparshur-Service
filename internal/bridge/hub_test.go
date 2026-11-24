package bridge

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const (
	macID       = "33333333-3333-4333-8333-333333333333"
	phoneID     = "22222222-2222-4222-8222-222222222222"
	unknownID   = "44444444-4444-4444-8444-444444444444"
	pairingID   = "55555555-5555-4555-8555-555555555555"
	messageID   = "018bcfe5-6800-7000-8000-000000000001"
	publicURL   = "wss://public.example:8443/v1/relay"
	testRelayID = "test-relay"
)

// httptest serves real HTTP, TLS and WebSocket traffic. In-memory connections
// also make writes deterministic and usable where listening ports are denied.
type pipeListener struct {
	pipes chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.pipes:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *pipeListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 443} }
func (l *pipeListener) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	select {
	case l.pipes <- pipeConn{server}:
		return pipeConn{client}, nil
	case <-ctx.Done():
		_ = client.Close()
		_ = server.Close()
		return nil, ctx.Err()
	case <-l.done:
		_ = client.Close()
		_ = server.Close()
		return nil, net.ErrClosed
	}
}

// TLS close_notify is best effort. An unbuffered pipe has nowhere to
// write it if both peers have left their WebSocket read loops.
type pipeConn struct{ net.Conn }

func (c pipeConn) SetWriteDeadline(deadline time.Time) error {
	if limit := time.Now().Add(250 * time.Millisecond); deadline.After(limit) {
		deadline = limit
	}
	return c.Conn.SetWriteDeadline(deadline)
}

type bridgeTest struct {
	t          *testing.T
	hub        *Hub
	server     *httptest.Server
	ctx        context.Context
	path       string
	enrollment Enrollment
	keys       map[string]ed25519.PrivateKey
}

func setupBridge(t *testing.T, configure func(*Hub)) *bridgeTest {
	t.Helper()
	x := &bridgeTest{
		t: t, path: filepath.Join(t.TempDir(), "enrollment.json"),
		enrollment: Enrollment{Pairings: []Pairing{{ID: pairingID, Devices: []string{macID, phoneID}}}},
		keys:       make(map[string]ed25519.PrivateKey),
	}
	for _, id := range []string{macID, phoneID} {
		key, private, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		x.keys[id] = private
		x.enrollment.Devices = append(x.enrollment.Devices, Device{ID: id, PublicKey: base64.StdEncoding.EncodeToString(key), Label: &id})
	}
	if err := WriteEnrollment(x.path, x.enrollment); err != nil {
		t.Fatal(err)
	}
	h, err := NewHub(Options{
		PublicURL: publicURL, RelayID: testRelayID, EnrollmentFile: x.path,
		QueueSize: 4, WriteTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	x.hub = h
	if configure != nil {
		configure(h)
	}
	listener := &pipeListener{pipes: make(chan net.Conn), done: make(chan struct{})}
	mux := http.NewServeMux()
	mux.Handle("GET "+Path, h)
	x.server = &httptest.Server{Listener: listener, Config: &http.Server{Handler: mux}}
	x.server.StartTLS()
	x.server.Client().Transport.(*http.Transport).DialContext = listener.dial
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	x.ctx = ctx
	t.Cleanup(func() {
		cancel()
		drain, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = h.Shutdown(drain)
		x.server.Close()
	})
	return x
}

func (x *bridgeTest) dialWith(path string, protocols ...string) (*websocket.Conn, *http.Response, error) {
	return websocket.Dial(x.ctx, "wss"+strings.TrimPrefix(x.server.URL, "https")+path,
		&websocket.DialOptions{HTTPClient: x.server.Client(), Subprotocols: protocols})
}
func (x *bridgeTest) dial() *websocket.Conn {
	x.t.Helper()
	c, _, err := x.dialWith(Path, "BRIDGE.RELAY.V1", Subprotocol)
	if err != nil {
		x.t.Fatal(err)
	}
	x.t.Cleanup(func() { _ = c.CloseNow() })
	if c.Subprotocol() != Subprotocol {
		x.t.Fatalf("selected subprotocol %s", c.Subprotocol())
	}
	return c
}
func (x *bridgeTest) send(c *websocket.Conn, f Frame) {
	x.t.Helper()
	if err := c.Write(x.ctx, websocket.MessageBinary, f.Encode()); err != nil {
		x.t.Fatal(err)
	}
}
func (x *bridgeTest) read(c *websocket.Conn, want Kind) Frame {
	x.t.Helper()
	typ, data, err := c.Read(x.ctx)
	if err != nil {
		x.t.Fatal(err)
	}
	f, err := Decode(data)
	if err != nil || typ != websocket.MessageBinary || f.Kind != want {
		x.t.Fatalf("wanted binary kind %d, received %d (%v)", want, f.Kind, err)
	}
	return f
}
func (x *bridgeTest) expectClose(c *websocket.Conn, code websocket.StatusCode) {
	x.t.Helper()
	if _, _, err := c.Read(x.ctx); websocket.CloseStatus(err) != code {
		x.t.Fatalf("want close %d, got %v", code, err)
	}
}

func (x *bridgeTest) issue(c *websocket.Conn, id string) Frame {
	x.t.Helper()
	nonce := bytes.Repeat([]byte{42}, 32)
	x.send(c, Frame{Kind: Hello, DeviceID: id, Nonce: nonce})
	challenge := x.read(c, Challenge)
	if challenge.RelayID != testRelayID || !bytes.Equal(challenge.Nonce, nonce) ||
		challenge.SessionID[14] != '4' || !strings.ContainsRune("89ab", rune(challenge.SessionID[19])) {
		x.t.Fatal("invalid challenge")
	}
	challenge.Kind, challenge.DeviceID = Authenticate, id
	challenge.Signature = ed25519.Sign(x.keys[id], challenge.SigningBytes(publicURL))
	return challenge
}

func (x *bridgeTest) ready(id string) *websocket.Conn {
	x.t.Helper()
	c := x.dial()
	auth := x.issue(c, id)
	x.send(c, auth)
	ready := x.read(c, Ready)
	if ready.SessionID != auth.SessionID || ready.DeviceID != id {
		x.t.Fatal("READY did not match authenticated session")
	}
	return c
}

func testPacket() Packet {
	return Packet{SenderID: phoneID, RecipientID: macID, PairingID: pairingID, MessageID: messageID, Body: []byte{1, 2, 3}, Kind: 1}
}

func TestBridgeTLSRouting(t *testing.T) {
	x := setupBridge(t, nil)
	phone, mac := x.ready(phoneID), x.ready(macID)
	packet := testPacket()
	encoded := packet.Encode()
	// Move the first packet field to the end and require verbatim forwarding.
	firstField := 2 + len(packet.SenderID)
	reordered := append(append([]byte{}, encoded[firstField:]...), encoded[:firstField]...)
	if err := phone.Write(x.ctx, websocket.MessageBinary, appendBytes(framePrefix(Submit), 10, reordered)); err != nil {
		t.Fatal(err)
	}
	typ, data, err := mac.Read(x.ctx)
	if err != nil || typ != websocket.MessageBinary || !bytes.Equal(data, appendBytes(framePrefix(Deliver), 10, reordered)) {
		t.Fatalf("expected verbatim DELIVER: %v", err)
	}
	digest := packet.Digest()
	ack := x.read(phone, Forwarded)
	if ack.MessageID != packet.MessageID || !bytes.Equal(ack.Digest, digest[:]) {
		t.Fatal("wrong forwarded acknowledgement")
	}

	if err := mac.Close(websocket.StatusNormalClosure, ""); err != nil {
		t.Fatal(err)
	}
	for count, _ := x.hub.Status(); count != 1; count, _ = x.hub.Status() {
		select {
		case <-x.ctx.Done():
			t.Fatal("closed recipient stayed online")
		case <-time.After(time.Millisecond):
		}
	}
	x.send(phone, Frame{Kind: Submit, Packet: packet})
	ack = x.read(phone, Unavailable)
	if ack.MessageID != packet.MessageID || !bytes.Equal(ack.Digest, digest[:]) {
		t.Fatal("wrong unavailable acknowledgement")
	}
}

func TestBridgeTLSRejectsInvalidAuthentication(t *testing.T) {
	t.Run("unknown", func(t *testing.T) {
		x := setupBridge(t, nil)
		c := x.dial()
		x.send(c, Frame{Kind: Hello, DeviceID: unknownID, Nonce: make([]byte, 32)})
		x.read(c, AuthRejected)
		x.expectClose(c, statusRejected)
	})
	t.Run("signature", func(t *testing.T) {
		x := setupBridge(t, nil)
		c := x.dial()
		auth := x.issue(c, phoneID)
		auth.Signature[0] ^= 1
		x.send(c, auth)
		x.read(c, AuthRejected)
		x.expectClose(c, statusRejected)
	})
	t.Run("expired", func(t *testing.T) {
		var seconds atomic.Int64
		seconds.Store(2000000000)
		x := setupBridge(t, func(h *Hub) { h.now = func() time.Time { return time.Unix(seconds.Load(), 0) } })
		c := x.dial()
		auth := x.issue(c, phoneID)
		seconds.Store(int64(auth.ExpiresAt))
		x.send(c, auth)
		x.read(c, AuthRejected)
		x.expectClose(c, statusRejected)
	})
	t.Run("cross-socket replay", func(t *testing.T) {
		x := setupBridge(t, nil)
		first, second := x.dial(), x.dial()
		auth := x.issue(first, phoneID)
		other := x.issue(second, phoneID)
		if auth.SessionID == other.SessionID || bytes.Equal(auth.Challenge, other.Challenge) {
			t.Fatal("challenge not unique per socket")
		}
		x.send(first, auth)
		x.read(first, Ready)
		x.send(second, auth)
		x.read(second, AuthRejected)
		x.expectClose(second, statusRejected)
	})
	t.Run("deadline", func(t *testing.T) {
		x := setupBridge(t, func(h *Hub) { h.authTimeout = 200 * time.Millisecond })
		c := x.dial()
		x.issue(c, phoneID)
		if _, _, err := c.Read(x.ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("relay did not enforce auth deadline: %v", err)
		}
	})
}

func TestBridgeTLSProtocolAndPolicyClosures(t *testing.T) {
	x := setupBridge(t, nil)
	for _, tt := range []struct {
		path      string
		protocols []string
	}{
		{Path, nil},
		{Path, []string{"BRIDGE.RELAY.V1"}},
		{Path + "?", []string{Subprotocol}},
		{Path + "?role=mac", []string{Subprotocol}},
	} {
		conn, resp, err := x.dialWith(tt.path, tt.protocols...)
		if err == nil {
			_ = conn.CloseNow()
		}
		if resp == nil || resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected upgrade refusal for %q: %v", tt.path, err)
		}
	}
	beforeReady := x.dial()
	x.send(beforeReady, Frame{Kind: Submit, Packet: testPacket()})
	x.expectClose(beforeReady, websocket.StatusProtocolError)
	afterReady := x.ready(phoneID)
	x.send(afterReady, Frame{Kind: Hello, DeviceID: phoneID, Nonce: make([]byte, 32)})
	x.expectClose(afterReady, websocket.StatusProtocolError)
	text := x.dial()
	if err := text.Write(x.ctx, websocket.MessageText, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	x.expectClose(text, websocket.StatusUnsupportedData)

	for _, mutate := range []func(*Packet){
		func(p *Packet) { p.PairingID = unknownID },
		func(p *Packet) { p.SenderID, p.RecipientID = p.RecipientID, p.SenderID },
		func(p *Packet) { p.RecipientID = p.SenderID },
	} {
		phone := x.ready(phoneID)
		p := testPacket()
		mutate(&p)
		x.send(phone, Frame{Kind: Submit, Packet: p})
		x.expectClose(phone, websocket.StatusPolicyViolation)
	}
}

func TestBridgeTLSReplacementRevocationAndDrain(t *testing.T) {
	x := setupBridge(t, nil)
	old := x.ready(phoneID)
	phone, mac := x.ready(phoneID), x.ready(macID)
	x.expectClose(old, statusReplaced)
	x.send(phone, Frame{Kind: Submit, Packet: testPacket()})
	x.read(mac, Deliver)
	x.read(phone, Forwarded)

	stopReload := WatchEnrollment(x.ctx, x.hub, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer stopReload()
	x.enrollment.Devices = x.enrollment.Devices[:1]
	x.enrollment.Pairings = []Pairing{}
	if err := WriteEnrollment(x.path, x.enrollment); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	x.expectClose(phone, statusRevoked)
	if count, _ := x.hub.Status(); count != 1 {
		t.Fatalf("want one authenticated session, have %d", count)
	}

	done := make(chan error, 1)
	go func() { done <- x.hub.Shutdown(x.ctx) }()
	x.expectClose(mac, websocket.StatusGoingAway)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	conn, resp, err := x.dialWith(Path, Subprotocol)
	if err == nil {
		_ = conn.CloseNow()
	}
	if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("draining did not reject upgrade: %v", err)
	}
}

func TestBridgeTLSFailedRecipientWriteGetsOneUnavailable(t *testing.T) {
	x := setupBridge(t, func(h *Hub) { h.opts.WriteTimeout = 300 * time.Millisecond })
	phone := x.ready(phoneID)
	// With an unbuffered transport, READY has been consumed and a DELIVER
	// cannot finish unless the recipient reads another WebSocket message.
	x.ready(macID)
	packet := testPacket()
	x.send(phone, Frame{Kind: Submit, Packet: packet})
	ack := x.read(phone, Unavailable)
	digest := packet.Digest()
	if ack.MessageID != packet.MessageID || !bytes.Equal(ack.Digest, digest[:]) {
		t.Fatal("write failure did not acknowledge the exact submission")
	}
	ctx, cancel := context.WithTimeout(x.ctx, 150*time.Millisecond)
	defer cancel()
	if _, _, err := phone.Read(ctx); err == nil || ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("expected no duplicate acknowledgement, got %v", err)
	}
}

func TestBridgeTLSForceCloseDuringAuth(t *testing.T) {
	x := setupBridge(t, nil)
	c := x.dial()
	ctx, cancel := context.WithTimeout(x.ctx, 80*time.Millisecond)
	defer cancel()
	// This client never reads the relay's close frame.
	if err := x.hub.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected forced shutdown, got %v", err)
	}
	if _, _, err := c.Read(x.ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("force-close did not release the transport: %v", err)
	}
}
