package bridge

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
)

func fixture(t testing.TB, name string, out any) {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatal(err)
	}
}

func wireFixtures(t testing.TB) map[string][]byte {
	t.Helper()
	var vector struct {
		Hex map[string]string `json:"frames_hex"`
	}
	fixture(t, "relay-wire", &vector)
	frames := make(map[string][]byte)
	for name, encoded := range vector.Hex {
		data, err := hex.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		frames[name] = data
	}
	if len(frames) != 9 {
		t.Fatal("expected nine wire fixtures")
	}
	return frames
}

func TestSharedAuthVector(t *testing.T) {
	var vector struct {
		URL          string `json:"relay_url"`
		SigningBytes string `json:"signing_bytes"`
		Seed         []byte `json:"signing_seed"`
		PublicKey    []byte `json:"signing_public_key"`
		Signature    []byte `json:"signature"`
	}
	fixture(t, "relay-auth", &vector)
	f, err := Decode(wireFixtures(t)["authenticate"])
	if err != nil {
		t.Fatal(err)
	}
	signed := f.SigningBytes(vector.URL)
	key := ed25519.NewKeyFromSeed(vector.Seed)
	if string(signed) != vector.SigningBytes || !bytes.Equal(f.Signature, vector.Signature) ||
		!bytes.Equal(key.Public().(ed25519.PublicKey), vector.PublicKey) ||
		!ed25519.Verify(vector.PublicKey, signed, ed25519.Sign(key, signed)) ||
		!ed25519.Verify(vector.PublicKey, signed, f.Signature) {
		t.Fatal("authentication fixture differs from contract")
	}
}

func TestSharedWireVectors(t *testing.T) {
	wire := wireFixtures(t)
	for name, data := range wire {
		t.Run(name, func(t *testing.T) {
			f, err := Decode(data)
			if err != nil || !bytes.Equal(f.Encode(), data) {
				t.Fatalf("fixture did not round-trip: %v", err)
			}
		})
	}
	submit, _ := Decode(wire["submit"])
	forwarded, _ := Decode(wire["forwarded"])
	digest := submit.Packet.Digest()
	if !bytes.Equal(digest[:], forwarded.Digest) || submit.Packet.MessageID != forwarded.MessageID {
		t.Fatal("packet digest or message ID differs")
	}
	f, err := Decode([]byte{16, 9, 8, 1})
	if err != nil || !bytes.Equal(f.Encode(), wire["auth_rejected"]) {
		t.Fatal("out-of-order fields were not canonicalized")
	}
}

func TestStrictWire(t *testing.T) {
	wire := wireFixtures(t)
	submit, _ := Decode(wire["submit"])
	invalidPacket := func(data []byte) []byte { return appendBytes(framePrefix(Submit), 10, data) }
	invalid := map[string][]byte{
		"zero": {8, 0, 16, 9}, "duplicate": {8, 1, 16, 9, 8, 1},
		"unknown": {8, 1, 16, 9, 104, 1}, "tag zero": {0, 1},
		"wrong wire type": {10, 1, 1, 16, 9}, "unsupported wire type": {13, 1},
		"nonminimal": {8, 0x81, 0, 16, 9}, "empty": {8, 1, 16, 5, 82, 0},
		"overflow":         append([]byte{8}, bytes.Repeat([]byte{0xff}, 10)...),
		"unexpected field": (Frame{Kind: AuthRejected, DeviceID: submit.Packet.SenderID}).Encode(),
		"oversized":        make([]byte, MaxFrame+1), "truncated": wire["challenge"][:len(wire["challenge"])-1],
		"nested unknown":   invalidPacket(appendInteger(submit.Packet.Encode(), 8, 1)),
		"nested duplicate": invalidPacket(appendInteger(submit.Packet.Encode(), 7, 1)),
	}
	invalidHello := Frame{Kind: Hello, DeviceID: "33333333-3333-4333-8333-33333333333A", Nonce: make([]byte, 32)}
	invalid["uppercase UUID"] = invalidHello.Encode()
	invalidHello.DeviceID = "33333333-3333-4333-8333-33333333333\xff"
	invalid["invalid UTF-8"] = invalidHello.Encode()
	submit.Packet.Body = make([]byte, 32769)
	invalid["oversized body"] = submit.Encode()
	for name, data := range invalid {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(data); err == nil {
				t.Fatal("accepted invalid frame")
			}
		})
	}
}

func FuzzDecode(f *testing.F) {
	for _, data := range wireFixtures(f) {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		parsed, err := Decode(data)
		if err != nil {
			return
		}
		canonical := parsed.Encode()
		roundTrip, err := Decode(canonical)
		if err != nil || !bytes.Equal(roundTrip.Encode(), canonical) {
			t.Fatal("canonical encoding is not idempotent")
		}
	})
}
