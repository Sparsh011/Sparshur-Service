package relay

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const (
	testRoom   = "test-room-0123456789abcdef"
	iterations = 200
)

func newTestHub() *Hub {
	return NewHub(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// newPeer builds a peer without read/write loops so tests can inspect its
// send queue directly. conn may be nil when the test never closes the peer.
func newPeer(h *Hub, room string, role Role, queue int, conn *websocket.Conn) *Peer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Peer{
		hub:    h,
		conn:   conn,
		roomID: room,
		role:   role,
		opts:   Options{SendQueueSize: queue, WriteTimeout: time.Second},
		log:    h.log,
		ctx:    ctx,
		cancel: cancel,
		send:   make(chan frame, queue),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
}

// wsPairs returns a function that yields a server-side WebSocket connection
// and a channel reporting the close status its client observes. The client
// keeps reading so server-initiated close handshakes complete.
func wsPairs(t *testing.T) func() (*websocket.Conn, <-chan websocket.StatusCode) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		accepted <- c
	}))
	t.Cleanup(ts.Close)
	url := "ws" + strings.TrimPrefix(ts.URL, "http")
	return func() (*websocket.Conn, <-chan websocket.StatusCode) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		status := make(chan websocket.StatusCode, 1)
		go func() {
			for {
				if _, _, err := c.Read(context.Background()); err != nil {
					status <- websocket.CloseStatus(err)
					return
				}
			}
		}()
		s := <-accepted
		t.Cleanup(func() {
			c.CloseNow()
			s.CloseNow()
		})
		return s, status
	}
}

func expectStatus(t *testing.T, status <-chan websocket.StatusCode, want websocket.StatusCode) {
	t.Helper()
	select {
	case got := <-status:
		if got != want {
			t.Fatalf("close status = %v, want %v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("client never saw close")
	}
}

// drain returns everything currently queued for p.
func drain(p *Peer) []frame {
	var out []frame
	for {
		select {
		case f := <-p.send:
			out = append(out, f)
		default:
			return out
		}
	}
}

func decode(t *testing.T, f frame) ControlMessage {
	t.Helper()
	if f.typ != websocket.MessageText {
		t.Fatalf("expected control frame, got %v %q", f.typ, f.data)
	}
	var m ControlMessage
	if err := json.Unmarshal(f.data, &m); err != nil {
		t.Fatalf("decode control: %v", err)
	}
	return m
}

// checkPresence replays the presence messages in frames the way a client
// would and fails if they are inconsistent: a welcome must come first when
// wantWelcome is set, peer_joined is only valid while the peer is absent and
// peer_left only while it is present. It returns the final presence.
func checkPresence(t *testing.T, frames []frame, present, wantWelcome bool) bool {
	t.Helper()
	for i, f := range frames {
		if f.typ != websocket.MessageText {
			if wantWelcome && i == 0 {
				t.Fatalf("data frame delivered before welcome")
			}
			continue
		}
		m := decode(t, f)
		switch {
		case i == 0 && wantWelcome:
			if m.Type != MsgWelcome {
				t.Fatalf("first message = %+v, want welcome", m)
			}
			present = *m.PeerPresent
		case m.Type == MsgPeerJoined && !present:
			present = true
		case m.Type == MsgPeerLeft && present:
			present = false
		default:
			t.Fatalf("message %d %+v inconsistent with presence %v (all: %s)", i, m, present, dump(frames))
		}
	}
	return present
}

func dump(frames []frame) string {
	var parts []string
	for _, f := range frames {
		parts = append(parts, string(f.data))
	}
	return strings.Join(parts, " ")
}

// together runs fns concurrently, releasing them at the same moment.
func together(fns ...func()) {
	var start, wg sync.WaitGroup
	start.Add(1)
	for _, fn := range fns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start.Wait()
			fn()
		}()
	}
	start.Done()
	wg.Wait()
}

func TestSimultaneousJoinsSeeConsistentPresence(t *testing.T) {
	for i := 0; i < iterations; i++ {
		h := newTestHub()
		mac := newPeer(h, testRoom, RoleMac, 8, nil)
		android := newPeer(h, testRoom, RoleAndroid, 8, nil)
		together(func() { h.join(mac) }, func() { h.join(android) })

		if !checkPresence(t, drain(mac), false, true) {
			t.Fatal("mac does not see android")
		}
		if !checkPresence(t, drain(android), false, true) {
			t.Fatal("android does not see mac")
		}
	}
}

func TestForwardDoesNotOvertakeWelcome(t *testing.T) {
	for i := 0; i < iterations; i++ {
		h := newTestHub()
		mac := newPeer(h, testRoom, RoleMac, 8, nil)
		h.join(mac)
		drain(mac)
		android := newPeer(h, testRoom, RoleAndroid, 8, nil)
		together(
			func() { h.join(android) },
			func() { h.forward(mac, websocket.MessageBinary, []byte("data")) },
		)
		checkPresence(t, drain(android), false, true)
	}
}

func TestLeaveRejoinKeepsOrder(t *testing.T) {
	pairs := wsPairs(t)
	for i := 0; i < iterations; i++ {
		h := newTestHub()
		mac := newPeer(h, testRoom, RoleMac, 8, nil)
		srv, _ := pairs()
		oldAndroid := newPeer(h, testRoom, RoleAndroid, 8, srv)
		h.join(mac)
		h.join(oldAndroid)
		drain(mac)

		// The old connection's cleanup races the new connection's join; the
		// mac must end up seeing android present either way.
		newAndroid := newPeer(h, testRoom, RoleAndroid, 8, nil)
		together(func() { h.leave(oldAndroid) }, func() { h.join(newAndroid) })

		if !checkPresence(t, drain(mac), true, false) {
			t.Fatal("mac does not see rejoined android")
		}
		if !checkPresence(t, drain(newAndroid), false, true) {
			t.Fatal("new android does not see mac")
		}
		if s := h.Stats(); s.Peers != 2 {
			t.Fatalf("stats = %+v", s)
		}
	}
}

func TestReplacementOfEitherRole(t *testing.T) {
	pairs := wsPairs(t)
	for _, role := range []Role{RoleMac, RoleAndroid} {
		t.Run(string(role), func(t *testing.T) {
			h := newTestHub()
			srv, status := pairs()
			old := newPeer(h, testRoom, role, 8, srv)
			other := newPeer(h, testRoom, role.Peer(), 8, nil)
			h.join(old)
			h.join(other)
			drain(other)

			repl := newPeer(h, testRoom, role, 8, nil)
			h.join(repl)
			if m := decode(t, drain(repl)[0]); m.Type != MsgWelcome || !*m.PeerPresent {
				t.Fatalf("replacement welcome = %+v", m)
			}
			expectStatus(t, status, StatusReplaced)

			// The stale connection's cleanup must not unregister its
			// replacement or tell the other side anything.
			h.leave(old)
			if f := drain(other); len(f) != 0 {
				t.Fatalf("other side saw %s", dump(f))
			}
			h.forward(other, websocket.MessageBinary, []byte("x"))
			if f := drain(repl); len(f) != 1 || string(f[0].data) != "x" {
				t.Fatalf("replacement got %s", dump(f))
			}
			if s := h.Stats(); s.Rooms != 1 || s.Peers != 2 {
				t.Fatalf("stats = %+v", s)
			}
		})
	}
}

func TestFramesDroppedWhilePeerOffline(t *testing.T) {
	h := newTestHub()
	mac := newPeer(h, testRoom, RoleMac, 8, nil)
	h.join(mac)
	h.forward(mac, websocket.MessageBinary, []byte("lost"))

	android := newPeer(h, testRoom, RoleAndroid, 8, nil)
	h.join(android)
	h.forward(mac, websocket.MessageBinary, []byte("kept"))

	f := drain(android)
	if len(f) != 2 || decode(t, f[0]).Type != MsgWelcome || string(f[1].data) != "kept" {
		t.Fatalf("android got %s", dump(f))
	}
}

func TestRoomsAreIsolated(t *testing.T) {
	h := newTestHub()
	mac := newPeer(h, testRoom, RoleMac, 8, nil)
	other := newPeer(h, "another-room-0123456789abc", RoleAndroid, 8, nil)
	h.join(mac)
	h.join(other)
	drain(mac)
	drain(other)

	h.forward(other, websocket.MessageBinary, []byte("nope"))
	if f := drain(mac); len(f) != 0 {
		t.Fatalf("mac got %s", dump(f))
	}
	if s := h.Stats(); s.Rooms != 2 || s.Peers != 2 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestQueueOverflowClosesSlowConsumer(t *testing.T) {
	srv, status := wsPairs(t)()
	h := newTestHub()
	mac := newPeer(h, testRoom, RoleMac, 8, nil)
	android := newPeer(h, testRoom, RoleAndroid, 1, srv)
	h.join(android) // fills android's one-slot queue with its welcome
	h.join(mac)     // peer_joined overflows it

	expectStatus(t, status, StatusSlowConsumer)

	// Later frames to the closed peer are discarded without blocking.
	h.forward(mac, websocket.MessageBinary, []byte("x"))
	h.leave(android)
	if f := drain(mac); len(f) != 2 || decode(t, f[1]).Type != MsgPeerLeft {
		t.Fatalf("mac got %s", dump(f))
	}
}
