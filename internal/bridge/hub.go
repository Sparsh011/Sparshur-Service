package bridge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	maxAuthenticating = 64
	maxSockets        = 256
	statusRejected    = websocket.StatusCode(4401)
	statusRevoked     = websocket.StatusCode(4403)
	statusReplaced    = websocket.StatusCode(4409)
	statusSlow        = websocket.StatusCode(4001)
)

type Options struct {
	PublicURL, RelayID, EnrollmentFile string
	Origins                            []string
	QueueSize                          int
	WriteTimeout                       time.Duration
}

type Hub struct {
	opts        Options
	now         func() time.Time
	authTimeout time.Duration

	mu             sync.Mutex
	enrolled       registry
	sockets        map[*session]struct{}
	devices        map[string]*session
	authenticating int
	draining       bool
	forcing        bool
	active         sync.WaitGroup
}

func NewHub(opts Options) (*Hub, error) {
	h := &Hub{
		opts: opts, now: time.Now, authTimeout: 10 * time.Second,
		sockets: make(map[*session]struct{}), devices: make(map[string]*session),
	}
	if err := h.Reload(); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *Hub) Reload() error {
	e, err := ReadEnrollment(h.opts.EnrollmentFile)
	if err != nil {
		return err
	}
	next, err := e.validate()
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.enrolled = next
	for id, s := range h.devices {
		if !bytes.Equal(next.devices[id], s.key) {
			h.closeLocked(s, statusRevoked)
		}
	}
	return nil
}

func (h *Hub) Status() (count int, draining bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.devices), h.draining
}

type session struct {
	conn *websocket.Conn
	raw  net.Conn
	ctx  context.Context
	stop context.CancelFunc

	// Identity, admission and fencing are protected by Hub.mu.
	device   string
	key      ed25519.PublicKey
	closed   bool
	done     chan struct{}
	outbound chan outbound
}

type outbound struct {
	data    []byte
	sender  *session
	pairing string
	ack     func(bool)
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, draining := h.Status(); draining {
		http.Error(w, "relay shutting down", http.StatusServiceUnavailable)
		return
	}
	if r.URL.ForceQuery || r.URL.RawQuery != "" || !offersProtocol(r) {
		http.Error(w, "relay subprotocol required and queries are forbidden", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	if h.draining || h.authenticating >= maxAuthenticating || len(h.sockets) >= maxSockets {
		h.mu.Unlock()
		http.Error(w, "relay unavailable", http.StatusServiceUnavailable)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	s := &session{
		ctx: ctx, stop: cancel, done: make(chan struct{}),
		outbound: make(chan outbound, h.opts.QueueSize),
	}
	h.sockets[s] = struct{}{}
	h.authenticating++
	h.active.Add(1)
	h.mu.Unlock()

	defer h.active.Done()
	defer h.cleanup(s)
	// The WebSocket library matches offers case-insensitively. Negotiate only
	// the exact token already verified above, even if it follows an alias.
	r = r.Clone(r.Context())
	r.Header.Set("Sec-WebSocket-Protocol", Subprotocol)
	conn, err := websocket.Accept(&upgradeWriter{ResponseWriter: w, hub: h, session: s}, r, &websocket.AcceptOptions{
		Subprotocols: []string{Subprotocol}, OriginPatterns: h.opts.Origins,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		if s.raw != nil {
			_ = s.raw.Close()
		}
		return
	}
	conn.SetReadLimit(MaxFrame)
	h.mu.Lock()
	s.conn = conn
	if h.draining {
		h.closeLocked(s, websocket.StatusGoingAway)
		h.mu.Unlock()
		<-s.done
		return
	}
	h.mu.Unlock()

	defer conn.CloseNow()
	if h.authenticate(s) {
		var loops sync.WaitGroup
		loops.Add(2)
		go func() { defer loops.Done(); h.writeLoop(s) }()
		go func() { defer loops.Done(); h.pingLoop(s) }()
		h.readLoop(s)
		h.close(s, websocket.StatusNormalClosure)
		<-s.done
		loops.Wait()
		return
	}
	h.close(s, websocket.StatusNormalClosure)
	<-s.done
}

func offersProtocol(r *http.Request) bool {
	for _, header := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, token := range strings.Split(header, ",") {
			if strings.TrimSpace(token) == Subprotocol {
				return true
			}
		}
	}
	return false
}

type upgradeWriter struct {
	http.ResponseWriter
	hub     *Hub
	session *session
}

func (w *upgradeWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *upgradeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	raw, buffered, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	w.hub.mu.Lock()
	w.session.raw = raw
	forcing := w.hub.forcing
	w.hub.mu.Unlock()
	if forcing {
		_ = raw.Close()
	}
	return raw, buffered, nil
}

func (h *Hub) newChallenge(hello Frame) (Frame, error) {
	var id [16]byte
	challenge := make([]byte, 32)
	if _, err := rand.Read(id[:]); err != nil {
		return Frame{}, err
	}
	if _, err := rand.Read(challenge); err != nil {
		return Frame{}, err
	}
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	return Frame{
		Kind: Challenge, RelayID: h.opts.RelayID, Nonce: hello.Nonce, Challenge: challenge,
		SessionID: fmt.Sprintf("%x-%x-%x-%x-%x", id[:4], id[4:6], id[6:8], id[8:10], id[10:]),
		ExpiresAt: uint64(h.now().Add(20 * time.Second).Unix()),
	}, nil
}

func (h *Hub) receive(ctx context.Context, s *session, want Kind) (Frame, []byte, bool) {
	typ, data, err := s.conn.Read(ctx)
	if err != nil {
		return Frame{}, nil, false
	}
	if typ != websocket.MessageBinary {
		h.close(s, websocket.StatusUnsupportedData)
		return Frame{}, nil, false
	}
	f, packet, err := decodeFrame(data)
	if err != nil || f.Kind != want {
		h.close(s, websocket.StatusProtocolError)
		return Frame{}, nil, false
	}
	return f, packet, true
}

func (h *Hub) authenticate(s *session) bool {
	ctx, cancel := context.WithTimeout(s.ctx, h.authTimeout)
	defer cancel()
	reject := func() bool {
		_ = h.write(ctx, s, Frame{Kind: AuthRejected}.Encode())
		h.close(s, statusRejected)
		return false
	}
	hello, _, ok := h.receive(ctx, s, Hello)
	if !ok {
		return false
	}
	h.mu.Lock()
	key := h.enrolled.devices[hello.DeviceID]
	h.mu.Unlock()
	if key == nil {
		return reject()
	}
	challenge, err := h.newChallenge(hello)
	if err != nil {
		h.close(s, websocket.StatusInternalError)
		return false
	}
	if err := h.write(ctx, s, challenge.Encode()); err != nil {
		return false
	}
	auth, _, ok := h.receive(ctx, s, Authenticate)
	if !ok {
		return false
	}
	if auth.DeviceID != hello.DeviceID || auth.RelayID != challenge.RelayID ||
		auth.SessionID != challenge.SessionID || auth.ExpiresAt != challenge.ExpiresAt ||
		!bytes.Equal(auth.Nonce, challenge.Nonce) || !bytes.Equal(auth.Challenge, challenge.Challenge) ||
		!h.now().Before(time.Unix(int64(challenge.ExpiresAt), 0)) ||
		!ed25519.Verify(key, auth.SigningBytes(h.opts.PublicURL), auth.Signature) {
		return reject()
	}

	h.mu.Lock()
	if s.closed || h.draining || ctx.Err() != nil {
		h.mu.Unlock()
		return false
	}
	if !bytes.Equal(key, h.enrolled.devices[auth.DeviceID]) {
		h.mu.Unlock()
		return reject()
	}
	if old := h.devices[auth.DeviceID]; old != nil {
		h.closeLocked(old, statusReplaced)
	}
	s.device, s.key = auth.DeviceID, key
	h.devices[s.device] = s
	h.authenticating--
	h.mu.Unlock()

	return h.write(ctx, s, Frame{Kind: Ready, DeviceID: s.device, SessionID: auth.SessionID}.Encode()) == nil
}

func (h *Hub) readLoop(s *session) {
	for {
		f, raw, ok := h.receive(s.ctx, s, Submit)
		if !ok {
			return
		}
		h.route(s, f.Packet, raw)
	}
}

func (h *Hub) route(sender *session, p Packet, raw []byte) {
	digest := p.Digest()
	delivery := appendBytes(framePrefix(Deliver), 10, raw)
	ack := func(written bool) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.ackLocked(sender, p.MessageID, digest[:], written)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if sender.closed || h.devices[sender.device] != sender {
		return
	}
	if p.SenderID != sender.device || !h.authorizedLocked(p.SenderID, p.RecipientID, p.PairingID) {
		h.closeLocked(sender, websocket.StatusPolicyViolation)
		return
	}
	recipient := h.devices[p.RecipientID]
	if recipient == nil || !h.enqueueLocked(recipient, outbound{
		data: delivery, sender: sender, pairing: p.PairingID, ack: ack,
	}) {
		h.ackLocked(sender, p.MessageID, digest[:], false)
	}
}

func (h *Hub) authorizedLocked(sender, recipient, pairing string) bool {
	pair := h.enrolled.pairings[pairing]
	return sender != recipient && (pair == [2]string{sender, recipient} || pair == [2]string{recipient, sender})
}

func (h *Hub) ackLocked(sender *session, message string, digest []byte, written bool) {
	if sender.closed || h.devices[sender.device] != sender {
		return
	}
	kind := Unavailable
	if written {
		kind = Forwarded
	}
	h.enqueueLocked(sender, outbound{data: Frame{Kind: kind, MessageID: message, Digest: digest}.Encode()})
}

func (h *Hub) enqueueLocked(s *session, message outbound) bool {
	if s.closed {
		return false
	}
	select {
	case s.outbound <- message:
		return true
	default:
		h.closeLocked(s, statusSlow)
		return false
	}
}

func (h *Hub) write(ctx context.Context, s *session, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, h.opts.WriteTimeout)
	defer cancel()
	return s.conn.Write(ctx, websocket.MessageBinary, data)
}

func (h *Hub) writeLoop(s *session) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case message := <-s.outbound:
			h.mu.Lock()
			current := !s.closed && h.devices[s.device] == s
			authorized := message.sender == nil ||
				(h.devices[message.sender.device] == message.sender && h.authorizedLocked(message.sender.device, s.device, message.pairing))
			h.mu.Unlock()
			if current && !authorized {
				message.ack(false)
				continue
			}
			written := current && h.write(s.ctx, s, message.data) == nil
			if message.ack != nil {
				message.ack(written)
			}
			if !written {
				h.close(s, websocket.StatusInternalError)
				return
			}
		}
	}
}

func (h *Hub) pingLoop(s *session) {
	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(s.ctx, h.opts.WriteTimeout)
			err := s.conn.Ping(ctx)
			cancel()
			if err != nil {
				h.close(s, websocket.StatusGoingAway)
				return
			}
		}
	}
}

func (h *Hub) close(s *session, status websocket.StatusCode) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closeLocked(s, status)
}

func (h *Hub) closeLocked(s *session, status websocket.StatusCode) {
	if s.closed || s.conn == nil {
		return
	}
	s.closed = true
	if h.devices[s.device] == s {
		delete(h.devices, s.device)
	}
	go func() {
		_ = s.conn.Close(status, "")
		s.stop()
		close(s.done)
	}()
}

func (h *Hub) cleanup(s *session) {
	s.stop()
	h.mu.Lock()
	delete(h.sockets, s)
	if s.device == "" {
		h.authenticating--
	}
	h.mu.Unlock()
	// The writer has ended and the session is fenced, so no more messages can
	// be enqueued. Failed deliveries still need to notify their senders.
	for {
		select {
		case message := <-s.outbound:
			if message.ack != nil {
				message.ack(false)
			}
		default:
			return
		}
	}
}

func (h *Hub) Shutdown(ctx context.Context) error {
	h.mu.Lock()
	h.draining = true
	for s := range h.sockets {
		h.closeLocked(s, websocket.StatusGoingAway)
	}
	h.mu.Unlock()

	done := make(chan struct{})
	go func() { h.active.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		h.mu.Lock()
		h.forcing = true
		for s := range h.sockets {
			s.stop()
			if s.raw != nil {
				_ = s.raw.Close()
			}
		}
		h.mu.Unlock()
		return ctx.Err()
	}
}
