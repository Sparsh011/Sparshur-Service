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
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const testRoom = "test-room-0123456789abcdef"

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

// pauseAt makes h stop the given operation on target right after it releases
// the hub lock, runs during while it is stopped, then lets it finish. This
// forces the interleaving where another operation completes inside what was
// the original implementation's window between unlocking and queuing.
func pauseAt(t *testing.T, h *Hub, op string, target *Peer, run func(), during func()) {
	t.Helper()
	paused, resume := make(chan struct{}), make(chan struct{})
	h.afterUnlock = func(gotOp string, p *Peer) {
		if gotOp == op && p == target {
			close(paused)
			<-resume
		}
	}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		run()
	}()
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never reached the pause point", op)
	}
	during()
	close(resume)
	<-finished
	h.afterUnlock = nil
}

func TestSimultaneousJoinsSeeConsistentPresence(t *testing.T) {
	h := newTestHub()
	mac := newPeer(h, testRoom, RoleMac, 8, nil)
	android := newPeer(h, testRoom, RoleAndroid, 8, nil)
	// The mac registers first but android's whole join runs before the mac's
	// join continues, so android's peer_joined must not precede mac's welcome.
	pauseAt(t, h, "join", mac, func() { h.join(mac) }, func() { h.join(android) })

	if !checkPresence(t, drain(mac), false, true) {
		t.Fatal("mac does not see android")
	}
	if !checkPresence(t, drain(android), false, true) {
		t.Fatal("android does not see mac")
	}
}

func TestForwardDoesNotOvertakeWelcome(t *testing.T) {
	h := newTestHub()
	mac := newPeer(h, testRoom, RoleMac, 8, nil)
	h.join(mac)
	drain(mac)
	android := newPeer(h, testRoom, RoleAndroid, 8, nil)
	pauseAt(t, h, "join", android, func() { h.join(android) }, func() {
		h.forward(mac, websocket.MessageBinary, []byte("data"))
	})

	f := drain(android)
	checkPresence(t, f, false, true)
	if len(f) != 2 || string(f[1].data) != "data" {
		t.Fatalf("android got %s", dump(f))
	}
}

func TestLeaveRejoinKeepsOrder(t *testing.T) {
	h := newTestHub()
	mac := newPeer(h, testRoom, RoleMac, 8, nil)
	oldAndroid := newPeer(h, testRoom, RoleAndroid, 8, nil)
	h.join(mac)
	h.join(oldAndroid)
	drain(mac)

	// The old connection's cleanup has unregistered it, and the new
	// connection joins before that cleanup finishes; the mac must still end
	// up seeing android present.
	newAndroid := newPeer(h, testRoom, RoleAndroid, 8, nil)
	pauseAt(t, h, "leave", oldAndroid, func() { h.leave(oldAndroid) }, func() { h.join(newAndroid) })

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

// blockingWriter stalls every write after it is armed until released, like a
// log sink whose pipe has filled up.
type blockingWriter struct {
	armed   atomic.Bool
	entered chan struct{}
	once    sync.Once
	release chan struct{}
}

func (w *blockingWriter) Write(b []byte) (int, error) {
	if w.armed.Load() {
		w.once.Do(func() { close(w.entered) })
		<-w.release
	}
	return len(b), nil
}

func TestBlockedLoggingDoesNotStallHub(t *testing.T) {
	w := &blockingWriter{entered: make(chan struct{}), release: make(chan struct{})}
	h := NewHub(slog.New(slog.NewTextHandler(w, nil)))
	srv, status := wsPairs(t)()
	android := newPeer(h, testRoom, RoleAndroid, 1, srv)
	mac := newPeer(h, testRoom, RoleMac, 8, nil)
	h.join(android) // fills android's one-slot queue with its welcome

	w.armed.Store(true)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		h.join(mac) // peer_joined overflows android's queue and gets logged
	}()
	t.Cleanup(func() {
		close(w.release)
		<-joined
	})
	select {
	case <-w.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("nothing was logged")
	}

	// With the log sink stuck, the hub must still route and report, and the
	// slow consumer must already be on its way out.
	stats := make(chan Stats, 1)
	go func() { stats <- h.Stats() }()
	select {
	case <-stats:
	case <-time.After(2 * time.Second):
		t.Fatal("hub lock held while logging")
	}
	expectStatus(t, status, StatusSlowConsumer)
}
