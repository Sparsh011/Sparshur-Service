// Package bridge implements the authenticated bridge.relay.v1 transport.
package bridge

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
)

const (
	Path        = "/v1/relay"
	Subprotocol = "bridge.relay.v1"
	MaxFrame    = 65536
	protocol    = "bridge-relay-e2ee-v1"
)

type Kind uint64

const (
	Hello Kind = iota + 1
	Challenge
	Authenticate
	Ready
	Submit
	Forwarded
	Deliver
	Unavailable
	AuthRejected
)

type Frame struct {
	Kind      Kind
	DeviceID  string
	Nonce     []byte
	RelayID   string
	SessionID string
	Challenge []byte
	ExpiresAt uint64
	Signature []byte
	Packet    Packet
	MessageID string
	Digest    []byte
}

type Packet struct {
	SenderID, RecipientID, PairingID, MessageID string
	Body                                        []byte
	Kind                                        uint64
}

func (p Packet) Encode() []byte {
	b := appendBytes(nil, 1, []byte(p.SenderID))
	b = appendBytes(b, 2, []byte(p.RecipientID))
	b = appendBytes(b, 3, []byte(p.PairingID))
	b = appendBytes(b, 4, []byte(p.MessageID))
	b = appendBytes(b, 5, p.Body)
	b = appendBytes(b, 6, []byte(protocol))
	return appendInteger(b, 7, p.Kind)
}

func (p Packet) Digest() [sha256.Size]byte { return sha256.Sum256(p.Encode()) }

func (f Frame) Encode() []byte {
	b := framePrefix(f.Kind)
	if f.DeviceID != "" {
		b = appendBytes(b, 3, []byte(f.DeviceID))
	}
	if len(f.Nonce) != 0 {
		b = appendBytes(b, 4, f.Nonce)
	}
	if f.RelayID != "" {
		b = appendBytes(b, 5, []byte(f.RelayID))
	}
	if f.SessionID != "" {
		b = appendBytes(b, 6, []byte(f.SessionID))
	}
	if len(f.Challenge) != 0 {
		b = appendBytes(b, 7, f.Challenge)
	}
	if f.ExpiresAt != 0 {
		b = appendInteger(b, 8, f.ExpiresAt)
	}
	if len(f.Signature) != 0 {
		b = appendBytes(b, 9, f.Signature)
	}
	if f.Kind == Submit || f.Kind == Deliver {
		b = appendBytes(b, 10, f.Packet.Encode())
	}
	if f.MessageID != "" {
		b = appendBytes(b, 11, []byte(f.MessageID))
	}
	if len(f.Digest) != 0 {
		b = appendBytes(b, 12, f.Digest)
	}
	return b
}

func framePrefix(kind Kind) []byte {
	return appendInteger(appendInteger(nil, 1, 1), 2, uint64(kind))
}

func appendInteger(b []byte, field, value uint64) []byte {
	return binary.AppendUvarint(binary.AppendUvarint(b, field<<3), value)
}

func appendBytes(b []byte, field uint64, value []byte) []byte {
	b = binary.AppendUvarint(b, field<<3|2)
	b = binary.AppendUvarint(b, uint64(len(value)))
	return append(b, value...)
}

var errWire = errors.New("invalid relay frame")

type field struct {
	n uint64
	b []byte
}

func parseFields(data []byte, out []field) (uint16, error) {
	if len(data) == 0 || len(data) > MaxFrame {
		return 0, errWire
	}
	read := func() uint64 {
		v, n := binary.Uvarint(data)
		if n <= 0 || v == 0 || data[n-1] == 0 {
			return 0
		}
		data = data[n:]
		return v
	}
	var seen uint16
	for len(data) > 0 {
		tag := read()
		id := tag >> 3
		if id == 0 || id >= uint64(len(out)) || seen&(1<<id) != 0 {
			return 0, errWire
		}
		seen |= 1 << id
		value := read()
		if value == 0 {
			return 0, errWire
		}
		switch tag & 7 {
		case 0:
			out[id].n = value
		case 2:
			if value > uint64(len(data)) {
				return 0, errWire
			}
			out[id].b, data = data[:value], data[value:]
		default:
			return 0, errWire
		}
	}
	return seen, nil
}

func ValidUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func ValidRelayID(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for i := range len(s) {
		if s[i] < 33 || s[i] > 126 || s[i] == '|' {
			return false
		}
	}
	return true
}

func decodePacket(data []byte) (Packet, error) {
	var p [8]field
	seen, err := parseFields(data, p[:])
	if err != nil || seen != 0xfe {
		return Packet{}, errWire
	}
	packet := Packet{
		SenderID: string(p[1].b), RecipientID: string(p[2].b), PairingID: string(p[3].b),
		MessageID: string(p[4].b), Body: p[5].b, Kind: p[7].n,
	}
	if !ValidUUID(packet.SenderID) || !ValidUUID(packet.RecipientID) || !ValidUUID(packet.PairingID) ||
		!ValidUUID(packet.MessageID) || len(packet.Body) < 1 || len(packet.Body) > 32768 ||
		string(p[6].b) != protocol || (packet.Kind != 1 && packet.Kind != 2) {
		return Packet{}, errWire
	}
	return packet, nil
}

func mask(ids ...uint) uint16 {
	result := uint16(1<<1 | 1<<2)
	for _, id := range ids {
		result |= 1 << id
	}
	return result
}

var frameFields = [...]uint16{
	Hello: mask(3, 4), Challenge: mask(4, 5, 6, 7, 8), Authenticate: mask(3, 4, 5, 6, 7, 8, 9),
	Ready: mask(3, 6), Submit: mask(10), Forwarded: mask(11, 12), Deliver: mask(10),
	Unavailable: mask(11, 12), AuthRejected: mask(),
}

func Decode(data []byte) (Frame, error) {
	f, _, err := decodeFrame(data)
	if err == nil && (f.Kind == Submit || f.Kind == Deliver) && f.Packet.SenderID == f.Packet.RecipientID {
		return Frame{}, errWire
	}
	return f, err
}

// The server checks self-sends as a routing policy violation after validating
// the wire shape. Keep the incoming nested bytes for verbatim forwarding.
func decodeFrame(data []byte) (Frame, []byte, error) {
	var p [13]field
	seen, err := parseFields(data, p[:])
	k := Kind(p[2].n)
	if err != nil || p[1].n != 1 || k < Hello || k > AuthRejected || seen != frameFields[k] {
		return Frame{}, nil, errWire
	}
	f := Frame{
		Kind: k, DeviceID: string(p[3].b), Nonce: p[4].b, RelayID: string(p[5].b),
		SessionID: string(p[6].b), Challenge: p[7].b, ExpiresAt: p[8].n, Signature: p[9].b,
		MessageID: string(p[11].b), Digest: p[12].b,
	}
	if (seen&(1<<3) != 0 && !ValidUUID(f.DeviceID)) ||
		(seen&(1<<4) != 0 && len(f.Nonce) != 32) ||
		(seen&(1<<5) != 0 && !ValidRelayID(f.RelayID)) ||
		(seen&(1<<6) != 0 && !ValidUUID(f.SessionID)) ||
		(seen&(1<<7) != 0 && len(f.Challenge) != 32) ||
		(seen&(1<<8) != 0 && f.ExpiresAt == 0) ||
		(seen&(1<<9) != 0 && len(f.Signature) != 64) ||
		(seen&(1<<11) != 0 && !ValidUUID(f.MessageID)) ||
		(seen&(1<<12) != 0 && len(f.Digest) != 32) {
		return Frame{}, nil, errWire
	}
	if seen&(1<<10) != 0 {
		f.Packet, err = decodePacket(p[10].b)
	}
	return f, p[10].b, err
}

func (f Frame) SigningBytes(publicURL string) []byte {
	return []byte(strings.Join([]string{
		"bridge-relay-v1/auth", publicURL, f.RelayID, f.DeviceID, f.SessionID,
		base64.StdEncoding.EncodeToString(f.Nonce), base64.StdEncoding.EncodeToString(f.Challenge),
		strconv.FormatUint(f.ExpiresAt, 10),
	}, "|"))
}
