package relay

import (
	"log/slog"
	"sync"

	"github.com/coder/websocket"
)

// Hub tracks rooms and routes frames between the two peers of a room.
type Hub struct {
	log *slog.Logger

	mu     sync.Mutex
	rooms  map[string]*room
	closed bool

	// afterUnlock, if set, runs right after join or leave releases mu. Tests
	// use it to pause an operation there and force a specific interleaving.
	afterUnlock func(op string, p *Peer)
}

type room struct {
	peers map[Role]*Peer
}

// NewHub creates an empty hub.
func NewHub(log *slog.Logger) *Hub {
	return &Hub{log: log, rooms: make(map[string]*room)}
}

// join registers p in its room, replacing any existing peer with the same
// role, and notifies both sides about presence. It returns false if the hub
// is shutting down.
//
// Control messages are queued while h.mu is held so that every membership
// change and the notifications it causes are observed in one global order:
// a peer always receives its welcome before any other frame, and presence
// events reach the other side in the order the connections changed. Queuing
// never blocks and close only starts a background handshake, so no network
// I/O or logging happens under the lock.
func (h *Hub) join(p *Peer) bool {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return false
	}
	r, ok := h.rooms[p.roomID]
	if !ok {
		r = &room{peers: make(map[Role]*Peer, 2)}
		h.rooms[p.roomID] = r
	}
	old := r.peers[p.role]
	r.peers[p.role] = p
	other := r.peers[p.role.Peer()]

	present := other != nil
	var slow []*Peer
	if !p.sendControl(ControlMessage{Type: MsgWelcome, RoomID: p.roomID, Role: p.role, PeerPresent: &present}) {
		slow = append(slow, p)
	}
	// Only announce a join when the peer did not already see this role
	// connected; a replacement is invisible to the other side.
	if other != nil && old == nil {
		if !other.sendControl(ControlMessage{Type: MsgPeerJoined, Role: p.role}) {
			slow = append(slow, other)
		}
	}
	hook := h.afterUnlock
	h.mu.Unlock()
	if hook != nil {
		hook("join", p)
	}

	logSlow(slow...)

	if old != nil {
		old.close(StatusReplaced, "replaced by newer connection")
	}
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
	other := r.peers[p.role.Peer()]
	if other != nil && other.sendControl(ControlMessage{Type: MsgPeerLeft, Role: p.role}) {
		other = nil // delivered; nothing to log
	}
	if len(r.peers) == 0 {
		delete(h.rooms, p.roomID)
	}
	hook := h.afterUnlock
	h.mu.Unlock()
	if hook != nil {
		hook("leave", p)
	}

	logSlow(other)

	h.log.Info("peer left", "room", logRoom(p.roomID), "role", p.role)
}

// forward delivers a frame from p to its counterpart. Frames sent while the
// counterpart is offline are dropped; clients learn presence from control
// messages and are expected to resend. The frame is queued under h.mu so it
// cannot overtake the presence messages that precede it.
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
		h.log.Debug("dropping frame, peer offline", "room", logRoom(p.roomID), "from", p.role, "bytes", len(data))
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

// Close disconnects every peer and refuses new ones. It is used on shutdown,
// because http.Server.Shutdown does not close hijacked connections.
func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	var peers []*Peer
	for _, r := range h.rooms {
		for _, p := range r.peers {
			peers = append(peers, p)
		}
	}
	h.mu.Unlock()

	for _, p := range peers {
		p.close(websocket.StatusGoingAway, "relay shutting down")
	}
}

// logRoom shortens a room ID for logs so full capabilities are not written
// to log storage.
func logRoom(id string) string {
	if len(id) > 8 {
		return id[:8] + "…"
	}
	return id
}
