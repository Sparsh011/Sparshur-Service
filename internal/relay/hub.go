package relay

import (
	"context"
	"log/slog"
	"net"
	"sync"

	"github.com/coder/websocket"
)

// Hub tracks rooms and routes messages between the two peers of a room.
type Hub struct {
	log *slog.Logger

	// mu serializes membership changes together with queueing the control
	// messages and frames that depend on them, so every peer observes
	// presence changes and forwarded messages in one consistent order. Only
	// nonblocking work (map updates, channel sends with a default case,
	// Peer.close) happens while it is held; network I/O and logging happen
	// after it is released or in each peer's own goroutines.
	mu    sync.Mutex
	rooms map[string]*room
	// sessions holds every connection whose Serve is running, including
	// connections that were replaced or refused and are still finishing
	// their close handshake.
	sessions map[*Peer]struct{}
	// upgrading holds connections hijacked by Accept that have not reached
	// Serve yet. track moves a connection from here to sessions under mu, so
	// Shutdown always finds it in exactly one of the two.
	upgrading map[net.Conn]struct{}
	draining  bool
	forcing   bool // Shutdown's deadline passed; late sessions are killed
	// active counts connections from Accept until Serve returns, so it also
	// covers upgrades that have not reached Serve yet.
	active sync.WaitGroup

	// testHookUnlocked, if set, runs right after join or leave releases mu.
	// Tests use it to interleave another operation at the point where the
	// messages queued by the first must already be in order.
	testHookUnlocked func()
	// testHookAdmitted, if set, runs in Accept right after a connection is
	// admitted and before the WebSocket upgrade.
	testHookAdmitted func()
	// testHookHijacked, if set, runs right after Accept's connection has
	// been hijacked and registered, before the upgrade completes.
	testHookHijacked func()
}

type room struct {
	peers map[Role]*Peer
}

// NewHub creates an empty hub.
func NewHub(log *slog.Logger) *Hub {
	return &Hub{
		log:       log,
		rooms:     make(map[string]*room),
		sessions:  make(map[*Peer]struct{}),
		upgrading: make(map[net.Conn]struct{}),
	}
}

// join registers p in its room, replacing any existing peer with the same
// role, and notifies both sides about presence. It returns false if the hub
// is draining.
func (h *Hub) join(p *Peer) bool {
	h.mu.Lock()
	if h.draining {
		h.mu.Unlock()
		return false
	}
	r, ok := h.rooms[p.roomID]
	if !ok {
		r = &room{peers: make(map[Role]*Peer, 2)}
		h.rooms[p.roomID] = r
	}
	old := r.peers[p.role]
	other := r.peers[p.role.Peer()]
	present := other != nil

	// Queue the welcome before p becomes visible to forward, so it is always
	// the first message p receives.
	var slow []*Peer
	if !p.sendControl(ControlMessage{Type: MsgWelcome, RoomID: p.roomID, Role: p.role, PeerPresent: &present}) {
		slow = append(slow, p)
	}
	r.peers[p.role] = p

	if old != nil {
		old.close(StatusReplaced, "replaced by newer connection")
	}
	// Only announce a join when the peer did not already see this role
	// connected; a replacement is invisible to the other side.
	if other != nil && old == nil {
		if !other.sendControl(ControlMessage{Type: MsgPeerJoined, Role: p.role}) {
			slow = append(slow, other)
		}
	}
	h.mu.Unlock()
	if h.testHookUnlocked != nil {
		h.testHookUnlocked()
	}

	logSlow(slow...)

	h.log.Info("peer joined", "room", logRoom(p.roomID), "role", p.role, "peer_present", present, "replaced", old != nil)
	return true
}

// leave removes p from its room if it is still the registered peer for its
// role, and notifies the other side.
func (h *Hub) leave(p *Peer) {
	h.mu.Lock()
	r, ok := h.rooms[p.roomID]
	if !ok || r.peers[p.role] != p {
		h.mu.Unlock()
		return
	}
	delete(r.peers, p.role)
	var slow *Peer
	if other := r.peers[p.role.Peer()]; other != nil && !other.sendControl(ControlMessage{Type: MsgPeerLeft, Role: p.role}) {
		slow = other
	}
	if len(r.peers) == 0 {
		delete(h.rooms, p.roomID)
	}
	h.mu.Unlock()
	if h.testHookUnlocked != nil {
		h.testHookUnlocked()
	}

	logSlow(slow)

	h.log.Info("peer left", "room", logRoom(p.roomID), "role", p.role)
}

// track makes p visible to Shutdown. A session that arrives after the
// shutdown deadline has passed is killed at once instead of being left to
// its close handshake.
func (h *Hub) track(p *Peer) {
	h.mu.Lock()
	if p.raw != nil {
		delete(h.upgrading, p.raw)
	}
	h.sessions[p] = struct{}{}
	forcing := h.forcing
	h.mu.Unlock()
	if forcing {
		p.close(websocket.StatusGoingAway, "relay shutting down")
		p.forceClose()
	}
}

// untrack forgets a session once its connection has fully closed.
func (h *Hub) untrack(p *Peer) {
	h.mu.Lock()
	delete(h.sessions, p)
	h.mu.Unlock()
}

// forward delivers a message from p to its counterpart. Messages sent while
// the counterpart is offline, or by a connection that has been replaced, are
// dropped; clients learn presence from control messages and are expected to
// resend.
func (h *Hub) forward(p *Peer, typ websocket.MessageType, data []byte) {
	h.mu.Lock()
	var other *Peer
	if r, ok := h.rooms[p.roomID]; ok && r.peers[p.role] == p {
		other = r.peers[p.role.Peer()]
	}
	delivered := other != nil && other.enqueue(frame{typ: typ, data: data})
	h.mu.Unlock()

	switch {
	case other == nil:
		h.log.Debug("dropping message, peer offline", "room", logRoom(p.roomID), "from", p.role, "bytes", len(data))
	case !delivered:
		logSlow(other)
	}
}

// logSlow records peers that enqueue disconnected for overflowing their send
// queue. It must be called without h.mu held, since logging may block.
func logSlow(peers ...*Peer) {
	for _, p := range peers {
		if p != nil {
			p.log.Warn("send queue full, disconnecting slow consumer")
		}
	}
}

// Stats is a snapshot of hub occupancy.
type Stats struct {
	Rooms int `json:"rooms"`
	Peers int `json:"peers"`
}

// Stats returns the current number of rooms and connected peers.
func (h *Hub) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := Stats{Rooms: len(h.rooms)}
	for _, r := range h.rooms {
		s.Peers += len(r.peers)
	}
	return s
}

// Draining reports whether Shutdown has been called.
func (h *Hub) Draining() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.draining
}

// Shutdown stops admitting new sessions, asks every active session to close
// with StatusGoingAway, and waits for them, including upgrades admitted but
// not yet serving, to finish. If ctx ends first, the remaining sessions'
// sockets are closed outright and ctx's error is returned without waiting
// further. It is needed because http.Server.Shutdown neither closes nor
// waits for hijacked connections.
func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	h.draining = true
	peers := h.sessionList()
	h.mu.Unlock()

	for _, p := range peers {
		p.close(websocket.StatusGoingAway, "relay shutting down")
	}

	done := make(chan struct{})
	go func() {
		h.active.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
	}

	h.mu.Lock()
	h.forcing = true
	peers = h.sessionList()
	upgrading := make([]net.Conn, 0, len(h.upgrading))
	for c := range h.upgrading {
		upgrading = append(upgrading, c)
	}
	h.mu.Unlock()
	h.log.Warn("shutdown deadline exceeded, force-closing connections", "remaining", len(peers), "upgrading", len(upgrading))
	for _, p := range peers {
		p.forceClose()
	}
	for _, c := range upgrading {
		_ = c.Close()
	}
	return ctx.Err()
}

func (h *Hub) sessionList() []*Peer {
	peers := make([]*Peer, 0, len(h.sessions))
	for p := range h.sessions {
		peers = append(peers, p)
	}
	return peers
}

// logRoom shortens a room ID for logs so full capabilities are not written
// to log storage.
func logRoom(id string) string {
	if len(id) > 8 {
		return id[:8] + "…"
	}
	return id
}
