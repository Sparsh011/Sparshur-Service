// Package relay implements a blind WebSocket relay between two paired devices.
//
// Each room holds at most one "mac" peer and one "android" peer. Binary frames
// sent by one peer are forwarded verbatim to the other; the relay never
// inspects, decrypts, or stores them. All end-to-end encryption happens on the
// devices.
//
// Text frames are reserved for relay control messages (JSON, server to client
// only). A client that sends a text frame is disconnected.
package relay

import (
	"crypto/rand"
	"encoding/base64"
	"regexp"

	"github.com/coder/websocket"
)

// Role identifies which side of a pairing a connection belongs to.
type Role string

const (
	RoleMac     Role = "mac"
	RoleAndroid Role = "android"
)

// ParseRole validates a role string from a client.
func ParseRole(s string) (Role, bool) {
	switch Role(s) {
	case RoleMac, RoleAndroid:
		return Role(s), true
	}
	return "", false
}

// Peer returns the counterpart role.
func (r Role) Peer() Role {
	if r == RoleMac {
		return RoleAndroid
	}
	return RoleMac
}

// Application close codes (4000-4999 range is reserved for applications).
const (
	// StatusReplaced is sent when a newer connection with the same room and
	// role takes over, e.g. after a client reconnects on a new network.
	StatusReplaced websocket.StatusCode = 4000
	// StatusSlowConsumer is sent when a client cannot keep up with its
	// peer's send rate and its outbound queue overflows.
	StatusSlowConsumer websocket.StatusCode = 4001
)

// Control message types sent by the relay as JSON text frames.
const (
	MsgWelcome    = "welcome"
	MsgPeerJoined = "peer_joined"
	MsgPeerLeft   = "peer_left"
)

// ControlMessage is the JSON body of every relay-originated text frame.
type ControlMessage struct {
	Type        string `json:"type"`
	RoomID      string `json:"room_id,omitempty"`
	Role        Role   `json:"role,omitempty"`
	PeerPresent *bool  `json:"peer_present,omitempty"`
}

// Room IDs are opaque, unguessable, URL-safe strings. Knowing a room ID is
// what lets two devices meet, so it must carry enough entropy.
var roomIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{22,128}$`)

// ValidRoomID reports whether id is an acceptable room identifier.
func ValidRoomID(id string) bool {
	return roomIDPattern.MatchString(id)
}

// NewRoomID returns a random 256-bit room identifier, base64url encoded.
func NewRoomID() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}
