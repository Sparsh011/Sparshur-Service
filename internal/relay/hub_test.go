package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

const testRoom = "test-room-0123456789abcdef"

func testHub() *Hub {
	return NewHub(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// queuePeer returns a peer with no connection and no I/O goroutines, so the
// messages the hub queues for it can be inspected directly. It must never be
// closed; use connPeer for peers that may be.
func queuePeer(h *Hub, room string, role Role, queue int) *Peer {
	return newPeer(context.Background(), h, nil, nil, room, role, Options{SendQueueSize: queue, WriteTimeout: time.Second})
}

// inertPeer is a queuePeer that is already marked closed, so the hub may
// close it (e.g. on replacement) without touching a connection.
func inertPeer(h *Hub, room string, role Role) *Peer {
	p := queuePeer(h, room, role, 8)
	p.closeOnce.Do(func() { close(p.done) })
	return p
}

// connPeer is like queuePeer but backed by a real WebSocket connection, so
// it can be closed. It returns the client end of the connection.
func connPeer(t *testing.T, h *Hub, room string, role Role, queue int) (*Peer, *websocket.Conn) {
	t.Helper()
	conns := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			close(conns)
			return
		}
		conns <- c
	}))
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cli, _, err := websocket.Dial(ctx, wsURL(ts.URL), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	srv := <-conns
	if srv == nil {
		t.FailNow()
	}
	p := newPeer(context.Background(), h, srv, nil, room, role, Options{SendQueueSize: queue, WriteTimeout: time.Second})
	// Close the client first so a pending server close handshake ends.
	t.Cleanup(func() {
		cli.CloseNow()
		srv.CloseNow()
		p.cancel()
	})
	return p, cli
}

func wsURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}

// drain removes and returns everything queued for p.
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

func control(t *testing.T, f frame) ControlMessage {
	t.Helper()
	if f.typ != websocket.MessageText {
		t.Fatalf("expected control message, got %v %q", f.typ, f.data)
	}
	var m ControlMessage
	if err := json.Unmarshal(f.data, &m); err != nil {
		t.Fatalf("decode control: %v", err)
	}
	return m
}

func expectWelcome(t *testing.T, f frame, present bool) {
	t.Helper()
	m := control(t, f)
	if m.Type != MsgWelcome || m.PeerPresent == nil || *m.PeerPresent != present {
		t.Fatalf("got %+v, want welcome with peer_present=%v", m, present)
	}
}

func expectClosed(t *testing.T, p *Peer) {
	t.Helper()
	select {
	case <-p.done:
	default:
		t.Fatal("peer was not closed")
	}
}

func expectCloseStatus(t *testing.T, c *websocket.Conn, want websocket.StatusCode) {
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

func TestWelcomeIsFirstMessage(t *testing.T) {
	for i := 0; i < 50; i++ {
		h := testHub()
		mac := queuePeer(h, testRoom, RoleMac, 1024)
		h.join(mac)
		drain(mac)

		// Forward continuously from mac while android joins; nothing may
		// reach android ahead of its welcome.
		android := queuePeer(h, testRoom, RoleAndroid, 1024)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 200; j++ {
				h.forward(mac, websocket.MessageBinary, []byte{byte(j)})
			}
		}()
		close(start)
		h.join(android)
		wg.Wait()

		got := drain(android)
		expectWelcome(t, got[0], true)
		for _, f := range got[1:] {
			if f.typ != websocket.MessageBinary {
				t.Fatalf("unexpected message after welcome: %v %q", f.typ, f.data)
			}
		}
	}
}

// expectPresenceAgreement checks the queues of two peers that joined the
// same room concurrently: exactly one saw an empty room, and that one then
// got peer_joined for the other; the second got nothing after its welcome.
func expectPresenceAgreement(t *testing.T, mac, android *Peer) {
	t.Helper()
	macMsgs, androidMsgs := drain(mac), drain(android)
	macPresent := *control(t, macMsgs[0]).PeerPresent
	androidPresent := *control(t, androidMsgs[0]).PeerPresent
	if macPresent == androidPresent {
		t.Fatalf("both peers saw peer_present=%v", macPresent)
	}
	first, second := macMsgs, androidMsgs
	secondRole := RoleAndroid
	if macPresent {
		first, second, secondRole = androidMsgs, macMsgs, RoleMac
	}
	if len(first) != 2 {
		t.Fatalf("first joiner got %d messages, want welcome then peer_joined", len(first))
	}
	if m := control(t, first[1]); m.Type != MsgPeerJoined || m.Role != secondRole {
		t.Fatalf("first joiner got %+v, want peer_joined %s", m, secondRole)
	}
	if len(second) != 1 {
		t.Fatalf("second joiner got %d messages, want welcome only", len(second))
	}
}

// hookOnce makes fn run the first time join or leave releases the hub lock.
// The hook clears itself first, so fn may call join or leave.
func hookOnce(h *Hub, fn func()) {
	h.testHookUnlocked = func() {
		h.testHookUnlocked = nil
		fn()
	}
}

func TestWelcomePrecedesMessageForwardedRightAfterJoin(t *testing.T) {
	h := testHub()
	mac := queuePeer(h, testRoom, RoleMac, 8)
	h.join(mac)
	drain(mac)

	android := queuePeer(h, testRoom, RoleAndroid, 8)
	hookOnce(h, func() { h.forward(mac, websocket.MessageBinary, []byte("early")) })
	h.join(android)

	got := drain(android)
	if len(got) != 2 {
		t.Fatalf("android got %d messages, want welcome then the forwarded one", len(got))
	}
	expectWelcome(t, got[0], true)
	if string(got[1].data) != "early" {
		t.Fatalf("second message = %q", got[1].data)
	}
}

func TestPeerLeftPrecedesRejoinRightAfterLeave(t *testing.T) {
	h := testHub()
	mac := queuePeer(h, testRoom, RoleMac, 8)
	oldAndroid := queuePeer(h, testRoom, RoleAndroid, 8)
	h.join(mac)
	h.join(oldAndroid)
	drain(mac)

	hookOnce(h, func() { h.join(queuePeer(h, testRoom, RoleAndroid, 8)) })
	h.leave(oldAndroid)

	got := drain(mac)
	if len(got) != 2 || control(t, got[0]).Type != MsgPeerLeft || control(t, got[1]).Type != MsgPeerJoined {
		t.Fatalf("mac did not get peer_left then peer_joined: %d messages", len(got))
	}
}

func TestConcurrentJoinsAgreeOnPresence(t *testing.T) {
	const rooms = 50
	h := testHub()
	peers := make([][2]*Peer, rooms)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range peers {
		room := fmt.Sprintf("%s-%02d", testRoom, i)
		peers[i] = [2]*Peer{queuePeer(h, room, RoleMac, 8), queuePeer(h, room, RoleAndroid, 8)}
		for _, p := range peers[i] {
			wg.Add(1)
			go func(p *Peer) {
				defer wg.Done()
				<-start
				if !h.join(p) {
					t.Error("join rejected")
				}
			}(p)
		}
	}
	close(start)
	wg.Wait()

	if s := h.Stats(); s.Rooms != rooms || s.Peers != 2*rooms {
		t.Fatalf("stats = %+v", s)
	}
	for _, pr := range peers {
		expectPresenceAgreement(t, pr[0], pr[1])
	}
}

func TestLeaveThenRejoinNotifiesInOrder(t *testing.T) {
	h := testHub()
	mac := queuePeer(h, testRoom, RoleMac, 8)
	oldAndroid := queuePeer(h, testRoom, RoleAndroid, 8)
	h.join(mac)
	h.join(oldAndroid)
	drain(mac)

	h.leave(oldAndroid)
	newAndroid := queuePeer(h, testRoom, RoleAndroid, 8)
	h.join(newAndroid)

	got := drain(mac)
	if len(got) != 2 {
		t.Fatalf("mac got %d messages, want peer_left then peer_joined", len(got))
	}
	if m := control(t, got[0]); m.Type != MsgPeerLeft || m.Role != RoleAndroid {
		t.Fatalf("first = %+v", m)
	}
	if m := control(t, got[1]); m.Type != MsgPeerJoined || m.Role != RoleAndroid {
		t.Fatalf("second = %+v", m)
	}
	expectWelcome(t, drain(newAndroid)[0], true)
}

// TestReconnectRaceKeepsPresenceConsistent races an old connection's leave
// against its replacement's join. Either the replacement wins (no
// notifications) or the leave wins (peer_left then peer_joined); the mac must
// never end up believing android is gone while it is connected.
func TestReconnectRaceKeepsPresenceConsistent(t *testing.T) {
	sawReplace, sawRejoin := false, false
	for i := 0; i < 500; i++ {
		h := testHub()
		mac := queuePeer(h, testRoom, RoleMac, 8)
		oldAndroid := inertPeer(h, testRoom, RoleAndroid)
		h.join(mac)
		h.join(oldAndroid)
		drain(mac)

		newAndroid := queuePeer(h, testRoom, RoleAndroid, 8)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; h.leave(oldAndroid) }()
		go func() { defer wg.Done(); <-start; h.join(newAndroid) }()
		close(start)
		wg.Wait()

		got := drain(mac)
		switch len(got) {
		case 0:
			sawReplace = true
		case 2:
			sawRejoin = true
			if control(t, got[0]).Type != MsgPeerLeft || control(t, got[1]).Type != MsgPeerJoined {
				t.Fatalf("notifications out of order: %q then %q", got[0].data, got[1].data)
			}
		default:
			t.Fatalf("mac got %d presence messages", len(got))
		}
		if s := h.Stats(); s.Peers != 2 {
			t.Fatalf("stats = %+v", s)
		}
	}
	t.Logf("replacement won: %v, leave won: %v", sawReplace, sawRejoin)
}

func TestReplacementIsInvisibleAndDropsOldSender(t *testing.T) {
	h := testHub()
	mac := queuePeer(h, testRoom, RoleMac, 8)
	oldAndroid, oldClient := connPeer(t, h, testRoom, RoleAndroid, 8)
	h.join(mac)
	h.join(oldAndroid)
	drain(mac)

	newAndroid := queuePeer(h, testRoom, RoleAndroid, 8)
	h.join(newAndroid)
	expectClosed(t, oldAndroid)
	expectWelcome(t, drain(newAndroid)[0], true)
	if got := drain(mac); len(got) != 0 {
		t.Fatalf("mac saw presence churn: %q", got[0].data)
	}

	h.forward(oldAndroid, websocket.MessageBinary, []byte("stale"))
	h.forward(newAndroid, websocket.MessageBinary, []byte("fresh"))
	got := drain(mac)
	if len(got) != 1 || string(got[0].data) != "fresh" {
		t.Fatalf("mac got %d messages, want only the new connection's", len(got))
	}
	// The old leave is a no-op now that it no longer owns the role.
	h.leave(oldAndroid)
	if got := drain(mac); len(got) != 0 {
		t.Fatalf("stale leave notified mac: %q", got[0].data)
	}
	expectCloseStatus(t, oldClient, StatusReplaced)
}

func TestQueueOverflowDisconnectsSlowConsumer(t *testing.T) {
	h := testHub()
	// The welcome fills mac's single slot, so the next message overflows.
	mac, macClient := connPeer(t, h, testRoom, RoleMac, 1)
	h.join(mac)
	h.join(queuePeer(h, testRoom, RoleAndroid, 8))

	expectClosed(t, mac)
	expectCloseStatus(t, macClient, StatusSlowConsumer)
}

func TestForwardDropsWhilePeerOffline(t *testing.T) {
	h := testHub()
	mac := queuePeer(h, testRoom, RoleMac, 8)
	h.join(mac)
	h.forward(mac, websocket.MessageBinary, []byte("into the void"))
	if got := drain(mac); len(got) != 1 {
		t.Fatalf("mac queue has %d messages, want only welcome", len(got))
	}

	android := queuePeer(h, testRoom, RoleAndroid, 8)
	h.join(android)
	if got := drain(android); len(got) != 1 {
		t.Fatalf("android got %d messages, want only welcome (dropped message replayed?)", len(got))
	}
}

// testServer runs a hub behind a real HTTP server, the way the server
// package does.
type testServer struct {
	t   *testing.T
	url string
}

func serveHub(t *testing.T, h *Hub) *testServer {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := h.Accept(w, r, nil)
		if errors.Is(err, ErrDraining) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if err != nil {
			return
		}
		role, _ := ParseRole(r.URL.Query().Get("role"))
		h.Serve(r.Context(), c, testRoom, role, Options{
			MaxMessageBytes: 1 << 16,
			SendQueueSize:   8,
			WriteTimeout:    time.Second,
		})
	}))
	t.Cleanup(ts.Close)
	return &testServer{t: t, url: wsURL(ts.URL)}
}

func (s *testServer) wsURL(role Role) string {
	return s.url + "?role=" + string(role)
}

func (s *testServer) dial(role Role) *websocket.Conn {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, s.wsURL(role), nil)
	if err != nil {
		s.t.Fatalf("dial: %v", err)
	}
	s.t.Cleanup(func() { c.CloseNow() })
	return c
}

// waitFor polls cond until it holds, failing after 5s.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitSessionsEnded fails unless every session admitted by h ends within 2s,
// well under the WebSocket library's own 5s close-handshake timeout; only a
// forced socket close can end a stalled handshake that quickly.
func waitSessionsEnded(t *testing.T, h *Hub) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		h.active.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stalled session was not force-closed")
	}
}

func readOne(t *testing.T, c *websocket.Conn) ControlMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	typ, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return control(t, frame{typ: typ, data: data})
}

// readUntilClose reads c in the background (which also answers the close
// handshake) and reports the close status it ends with.
func readUntilClose(c *websocket.Conn) <-chan websocket.StatusCode {
	ch := make(chan websocket.StatusCode, 1)
	go func() {
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				ch <- websocket.CloseStatus(err)
				return
			}
		}
	}()
	return ch
}

func waitStatus(t *testing.T, ch <-chan websocket.StatusCode, want websocket.StatusCode) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("close status = %v, want %v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not close")
	}
}

func TestShutdownDrainsSessions(t *testing.T) {
	h := testHub()
	srv := serveHub(t, h)
	mac := srv.dial(RoleMac)
	readOne(t, mac)
	android := srv.dial(RoleAndroid)
	readOne(t, android)
	readOne(t, mac) // peer_joined
	macStatus, androidStatus := readUntilClose(mac), readUntilClose(android)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	waitStatus(t, macStatus, websocket.StatusGoingAway)
	waitStatus(t, androidStatus, websocket.StatusGoingAway)
	if !h.Draining() {
		t.Fatal("hub not draining after Shutdown")
	}
	if s := h.Stats(); s != (Stats{}) {
		t.Fatalf("stats after shutdown = %+v", s)
	}

	// Late arrivals are turned away before the upgrade.
	_, resp, err := websocket.Dial(ctx, srv.wsURL(RoleMac), nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("late dial: resp = %v, err = %v", resp, err)
	}
}

// TestShutdownForceClosesStuckSessions checks that Shutdown waits for a
// replaced connection that is still in its close handshake, and that the
// deadline force-closes it.
func TestShutdownForceClosesStuckSessions(t *testing.T) {
	h := testHub()
	srv := serveHub(t, h)
	oldMac := srv.dial(RoleMac)
	readOne(t, oldMac) // then never read again, so its close handshake stalls
	newMac := srv.dial(RoleMac)
	if m := readOne(t, newMac); m.Type != MsgWelcome {
		t.Fatalf("new mac got %+v", m)
	}
	newStatus := readUntilClose(newMac)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := h.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want deadline exceeded while replaced session is stuck", err)
	}
	waitStatus(t, newStatus, websocket.StatusGoingAway)
	waitSessionsEnded(t, h)
}

// TestShutdownForceClosesAfterReadLoopExits covers a handshake that nothing
// else is waiting on: the relay rejects a text message (1008), its read loop
// exits, and the library's Close waits for a reply the client never sends.
// Cancelling the session context cannot end that; closing the socket must.
func TestShutdownForceClosesAfterReadLoopExits(t *testing.T) {
	h := testHub()
	srv := serveHub(t, h)
	c := srv.dial(RoleMac)
	readOne(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte("not allowed")); err != nil {
		t.Fatal(err)
	}
	// leave runs only after the read loop has returned.
	waitFor(t, "read loop to exit", func() bool { return h.Stats().Peers == 0 })

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelShutdown()
	if err := h.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want deadline exceeded while the 1008 handshake is stuck", err)
	}
	waitSessionsEnded(t, h)
}

// TestShutdownAwaitsUpgradeInProgress pauses an upgrade right after the hub
// admitted it and starts shutdown before it reaches Serve. Shutdown must
// still count the connection, and must kill it once it arrives if the
// deadline has already passed. The client never reads, so only a forced
// close can end its session promptly.
func TestShutdownAwaitsUpgradeInProgress(t *testing.T) {
	for _, tc := range []struct {
		name          string
		afterDeadline bool
	}{
		{"drain begins mid-upgrade", false},
		{"deadline passes mid-upgrade", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := testHub()
			admitted, proceed := make(chan struct{}), make(chan struct{})
			h.testHookAdmitted = func() {
				close(admitted)
				<-proceed
			}
			srv := serveHub(t, h)

			dialed := make(chan *websocket.Conn, 1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				c, _, _ := websocket.Dial(ctx, srv.wsURL(RoleMac), nil)
				dialed <- c
			}()
			<-admitted

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			shutdownErr := make(chan error, 1)
			go func() { shutdownErr <- h.Shutdown(ctx) }()
			if tc.afterDeadline {
				if err := <-shutdownErr; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Shutdown = %v, want deadline exceeded with an upgrade pending", err)
				}
			} else {
				waitFor(t, "drain to begin", h.Draining)
			}

			close(proceed)
			if c := <-dialed; c != nil {
				t.Cleanup(func() { c.CloseNow() })
			}
			if !tc.afterDeadline {
				if err := <-shutdownErr; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Shutdown = %v, want deadline exceeded while the late session is stuck", err)
				}
			}
			waitSessionsEnded(t, h)
		})
	}
}

// TestShutdownClosesUpgradeStalledAfterHijack parks an upgrade after net/http
// has handed the socket over but before Accept returns, so no session exists
// yet. At the deadline the socket itself must be closed while the upgrade is
// still parked, not only once it eventually reaches Serve.
func TestShutdownClosesUpgradeStalledAfterHijack(t *testing.T) {
	h := testHub()
	hijacked, release := make(chan struct{}), make(chan struct{})
	h.testHookHijacked = func() {
		close(hijacked)
		<-release
	}
	srv := serveHub(t, h)

	dialed := make(chan *websocket.Conn, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, _, _ := websocket.Dial(ctx, srv.wsURL(RoleMac), nil)
		dialed <- c
	}()
	<-hijacked
	// net/http flushes the 101 inside Hijack, so the client is connected
	// even though the server side is still parked in the hook.
	c := <-dialed
	if c == nil {
		close(release)
		t.Fatal("dial failed")
	}
	t.Cleanup(func() { c.CloseNow() })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := h.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		close(release)
		t.Fatalf("Shutdown = %v, want deadline exceeded with an upgrade stalled", err)
	}

	readCtx, cancelRead := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelRead()
	_, _, err := c.Read(readCtx)
	stalledOpen := readCtx.Err() != nil
	close(release)
	if err == nil || stalledOpen {
		t.Fatalf("socket stayed open while the upgrade was stalled (err %v)", err)
	}
	waitSessionsEnded(t, h)
}
