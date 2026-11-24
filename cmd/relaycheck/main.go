// Command relaycheck smoke-tests a deployed relay from a client's point of
// view: health probes, room minting, and a full mac/android round trip over
// the WebSocket endpoint. It talks to the relay exactly as a device would, so
// it also proves the TLS terminator in front of the relay is trusted and
// passes WebSocket upgrades through unchanged.
//
//	relaycheck -url https://relay.example-tailnet.ts.net
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/sparshchadha/sparshur-service/internal/bridge"
	"github.com/sparshchadha/sparshur-service/internal/relay"
)

func main() {
	base := flag.String("url", "http://127.0.0.1:8080", "relay base URL (http, https, ws or wss)")
	requireTLS := flag.Bool("require-tls", false, "fail unless the URL is https/wss")
	bridgeMode := flag.Bool("bridge", false, "check the authenticated relay without enrolling a device")
	timeout := flag.Duration("timeout", 15*time.Second, "overall deadline")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if err := run(ctx, *base, *requireTLS, *bridgeMode); err != nil {
		fmt.Fprintln(os.Stderr, "FAIL:", err)
		os.Exit(1)
	}
	fmt.Println("PASS")
}

func run(ctx context.Context, base string, requireTLS, bridgeMode bool) error {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil {
		return err
	}
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	case "http", "https":
	default:
		return fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		(u.Path != "" && u.Path != "/" && (!bridgeMode || u.Path != bridge.Path)) {
		return errors.New("url must be a bare origin such as https://host[:port]")
	}
	u.Path = ""
	if u.Scheme != "https" {
		if requireTLS {
			return errors.New("-require-tls set but url is not https/wss")
		}
		fmt.Println("warn  plaintext URL; devices must use wss:// through a TLS terminator")
	}
	httpBase := u.String()
	wsBase := "ws" + strings.TrimPrefix(httpBase, "http")

	// Certificates are validated against the system trust store. There is
	// deliberately no flag to skip verification: clients do not have one.
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("unexpected redirect")
	}}

	if bridgeMode {
		return checkBridge(ctx, client, wsBase)
	}

	var health struct {
		Status string `json:"status"`
	}
	if err := getJSON(ctx, client, http.MethodGet, httpBase+"/healthz", http.StatusOK, &health); err != nil {
		return err
	}
	if health.Status != "ok" {
		return fmt.Errorf("/healthz status = %q", health.Status)
	}
	fmt.Println("ok    GET /healthz")

	var ready struct {
		Status string       `json:"status"`
		Stats  *relay.Stats `json:"stats"`
	}
	if err := getJSON(ctx, client, http.MethodGet, httpBase+"/readyz", http.StatusOK, &ready); err != nil {
		return err
	}
	if ready.Status != "ok" || ready.Stats == nil {
		return fmt.Errorf("/readyz body missing status or stats")
	}
	fmt.Printf("ok    GET /readyz (rooms=%d peers=%d)\n", ready.Stats.Rooms, ready.Stats.Peers)

	var minted struct {
		RoomID string `json:"room_id"`
	}
	if err := getJSON(ctx, client, http.MethodPost, httpBase+"/v1/rooms", http.StatusCreated, &minted); err != nil {
		return err
	}
	if !relay.ValidRoomID(minted.RoomID) {
		return errors.New("POST /v1/rooms returned an invalid room id")
	}
	fmt.Println("ok    POST /v1/rooms")

	dial := func(role relay.Role) (*websocket.Conn, error) {
		c, _, err := websocket.Dial(ctx, wsBase+"/v1/rooms/"+minted.RoomID+"/ws?role="+string(role),
			&websocket.DialOptions{HTTPClient: client})
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", role, err)
		}
		return c, nil
	}

	mac, err := dial(relay.RoleMac)
	if err != nil {
		return err
	}
	defer mac.CloseNow()
	if err := expectControl(ctx, mac, relay.MsgWelcome, relay.RoleMac, false); err != nil {
		return fmt.Errorf("mac welcome: %w", err)
	}

	android, err := dial(relay.RoleAndroid)
	if err != nil {
		return err
	}
	defer android.CloseNow()
	if err := expectControl(ctx, android, relay.MsgWelcome, relay.RoleAndroid, true); err != nil {
		return fmt.Errorf("android welcome: %w", err)
	}
	if err := expectControl(ctx, mac, relay.MsgPeerJoined, relay.RoleAndroid, false); err != nil {
		return fmt.Errorf("mac peer_joined: %w", err)
	}
	fmt.Println("ok    WebSocket upgrade, welcome and peer_joined for both roles")

	if err := roundTrip(ctx, android, mac, []byte("relaycheck android->mac \x00\xff")); err != nil {
		return fmt.Errorf("android->mac: %w", err)
	}
	if err := roundTrip(ctx, mac, android, []byte("relaycheck mac->android \x00\xff")); err != nil {
		return fmt.Errorf("mac->android: %w", err)
	}
	fmt.Println("ok    binary frames forwarded verbatim in both directions")

	if err := android.Close(websocket.StatusNormalClosure, ""); err != nil {
		return fmt.Errorf("close android: %w", err)
	}
	if err := expectControl(ctx, mac, relay.MsgPeerLeft, relay.RoleAndroid, false); err != nil {
		return fmt.Errorf("mac peer_left: %w", err)
	}
	fmt.Println("ok    peer_left on disconnect")
	return mac.Close(websocket.StatusNormalClosure, "")
}

func getJSON(ctx context.Context, client *http.Client, method, target string, want int, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, target, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return err
	}
	path := req.URL.Path
	if resp.StatusCode != want {
		return fmt.Errorf("%s %s: status %d, want %d", method, path, resp.StatusCode, want)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("%s %s: body is not relay JSON (is something else answering?): %w", method, path, err)
	}
	return nil
}

// expectControl reads the next frame and checks it is the given control
// message. peerPresent is only compared for welcome messages.
func expectControl(ctx context.Context, c *websocket.Conn, typ string, role relay.Role, peerPresent bool) error {
	mt, data, err := c.Read(ctx)
	if err != nil {
		return err
	}
	if mt != websocket.MessageText {
		return errors.New("expected a text control frame")
	}
	var msg relay.ControlMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}
	if msg.Type != typ || msg.Role != role {
		return fmt.Errorf("got %s/%s, want %s/%s", msg.Type, msg.Role, typ, role)
	}
	if typ == relay.MsgWelcome && (msg.PeerPresent == nil || *msg.PeerPresent != peerPresent) {
		return fmt.Errorf("peer_present mismatch, want %v", peerPresent)
	}
	return nil
}

func roundTrip(ctx context.Context, from, to *websocket.Conn, payload []byte) error {
	if err := from.Write(ctx, websocket.MessageBinary, payload); err != nil {
		return err
	}
	mt, data, err := to.Read(ctx)
	if err != nil {
		return err
	}
	if mt != websocket.MessageBinary || !bytes.Equal(data, payload) {
		return errors.New("frame was altered in transit")
	}
	return nil
}

func checkBridge(ctx context.Context, client *http.Client, base string) error {
	conn, _, err := websocket.Dial(ctx, base+bridge.Path, &websocket.DialOptions{
		HTTPClient: client, Subprotocols: []string{bridge.Subprotocol},
	})
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	if conn.Subprotocol() != bridge.Subprotocol {
		return errors.New("relay subprotocol was not negotiated")
	}
	conn.SetReadLimit(bridge.MaxFrame)
	var id [16]byte
	nonce := make([]byte, 32)
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	id[6], id[8] = id[6]&0x0f|0x40, id[8]&0x3f|0x80
	device := fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:])
	if err := conn.Write(ctx, websocket.MessageBinary, (bridge.Frame{Kind: bridge.Hello, DeviceID: device, Nonce: nonce}).Encode()); err != nil {
		return err
	}
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	f, err := bridge.Decode(data)
	if err != nil || typ != websocket.MessageBinary || f.Kind != bridge.AuthRejected {
		return errors.New("expected AUTH_REJECTED for an unenrolled device")
	}
	if _, _, err := conn.Read(ctx); websocket.CloseStatus(err) != 4401 {
		return errors.New("expected close code 4401 after AUTH_REJECTED")
	}
	fmt.Println("ok    /v1/relay negotiated bridge.relay.v1 and rejected an unenrolled device")
	return nil
}
