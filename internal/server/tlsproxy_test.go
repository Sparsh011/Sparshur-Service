package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/sparshchadha/sparshur-service/internal/relay"
)

// The supported deployment keeps the relay on plaintext HTTP and puts a TLS
// terminator (tailscale serve, Caddy, a load balancer) in front of it. This
// exercises that shape: clients speak https/wss to a reverse proxy whose
// certificate they verify, and the wire contract must be unchanged.
func TestWireContractBehindTLSTerminator(t *testing.T) {
	backend := newTestServer(t)
	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(target))
	t.Cleanup(front.Close)
	client := front.Client() // trusts only the terminator's certificate

	resp, err := client.Get(front.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz via terminator = %d", resp.StatusCode)
	}

	dialTLS := func(role relay.Role) *websocket.Conn {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		u := "wss" + strings.TrimPrefix(front.URL, "https") + "/v1/rooms/" + testRoom + "/ws?role=" + string(role)
		c, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPClient: client})
		if err != nil {
			t.Fatalf("dial %s via terminator: %v", role, err)
		}
		t.Cleanup(func() { c.CloseNow() })
		return c
	}

	mac := dialTLS(relay.RoleMac)
	if m := readControl(t, mac); m.Type != relay.MsgWelcome || *m.PeerPresent {
		t.Fatalf("mac welcome = %+v", m)
	}
	android := dialTLS(relay.RoleAndroid)
	if m := readControl(t, android); m.Type != relay.MsgWelcome || !*m.PeerPresent {
		t.Fatalf("android welcome = %+v", m)
	}
	if m := readControl(t, mac); m.Type != relay.MsgPeerJoined || m.Role != relay.RoleAndroid {
		t.Fatalf("mac peer_joined = %+v", m)
	}

	payload := []byte{0x00, 0xff, 0x10, 0x80}
	write(t, android, websocket.MessageBinary, payload)
	if typ, got := read(t, mac); typ != websocket.MessageBinary || string(got) != string(payload) {
		t.Fatalf("mac got %v %x", typ, got)
	}
	write(t, mac, websocket.MessageBinary, payload)
	if typ, got := read(t, android); typ != websocket.MessageBinary || string(got) != string(payload) {
		t.Fatalf("android got %v %x", typ, got)
	}

	// Application close codes must survive the proxy hop.
	write(t, android, websocket.MessageText, []byte("nope"))
	expectClose(t, android, websocket.StatusPolicyViolation)
	if m := readControl(t, mac); m.Type != relay.MsgPeerLeft || m.Role != relay.RoleAndroid {
		t.Fatalf("mac peer_left = %+v", m)
	}
}
