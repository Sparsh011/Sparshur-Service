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
	h.mu.Unlock()

	if old != nil {
		old.close(StatusReplaced, "replaced by newer connection")
	}

	present := other != nil
	p.sendControl(ControlMessage{Type: MsgWelcome, RoomID: p.roomID, Role: p.role, PeerPresent: &present})
	// Only announce a join when the peer did not already see this role
	// connected; a replacement is invisible to the other side.
	if other != nil && old == nil {
		other.sendControl(ControlMessage{Type: MsgPeerJoined, Role: p.role})
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
	if len(r.peers) == 0 {
		delete(h.rooms, p.roomID)
	}
	h.mu.Unlock()

	if other != nil {
		other.sendControl(ControlMessage{Type: MsgPeerLeft, Role: p.role})
	}
	h.log.Info("peer left", "room", logRoom(p.roomID), "role", p.role)
}

// forward delivers a frame from p to its counterpart. Frames sent while the
// counterpart is offline are dropped; clients learn presence from control
// messages and are expected to resend.
func (h *Hub) forward(p *Peer, typ websocket.MessageType, data []byte) {
	h.mu.Lock()
	var other *Peer
	if r, ok := h.rooms[p.roomID]; ok && r.peers[p.role] == p {
		other = r.peers[p.role.Peer()]
	}
	h.mu.Unlock()

	if other == nil {
		h.log.Debug("dropping frame, peer offline", "room", logRoom(p.roomID), "from", p.role, "bytes", len(data))
		return
	}
	other.enqueue(frame{typ: typ, data: data})
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
