package relay

import (
	"bufio"
	"errors"
	"net"
	"net/http"

	"github.com/coder/websocket"
)

// ErrDraining is returned by Accept once Shutdown has begun. No response has
// been written, so the caller can reply with 503.
var ErrDraining = errors.New("relay is draining")

// Conn is a WebSocket connection accepted through Hub.Accept. It has been
// counted as an active session, so it must be passed to Serve exactly once.
type Conn struct {
	ws *websocket.Conn
	// raw is the hijacked transport. Closing it is the only way to abort a
	// close handshake that the WebSocket library is already running.
	raw net.Conn
}

// Accept upgrades r to a WebSocket. Admission is decided under the same lock
// Shutdown uses to start draining, so every accepted connection is either
// refused with ErrDraining or tracked until Serve returns; there is no window
// where shutdown can miss it.
func (h *Hub) Accept(w http.ResponseWriter, r *http.Request, opts *websocket.AcceptOptions) (*Conn, error) {
	h.mu.Lock()
	if h.draining {
		h.mu.Unlock()
		return nil, ErrDraining
	}
	h.active.Add(1)
	h.mu.Unlock()
	if h.testHookAdmitted != nil {
		h.testHookAdmitted()
	}

	hw := &hijackWriter{ResponseWriter: w, hub: h}
	ws, err := websocket.Accept(hw, r, opts)
	if err != nil {
		// Accept has already written an HTTP error response, or the
		// connection was hijacked and can no longer carry one.
		if hw.conn != nil {
			h.untrackUpgrade(hw.conn)
			_ = hw.conn.Close()
		}
		h.active.Done()
		return nil, err
	}
	return &Conn{ws: ws, raw: hw.conn}, nil
}

// hijackWriter hands the connection websocket.Accept hijacks to the hub the
// moment net/http releases it. Until Hijack returns, http.Server tracks the
// connection and srv.Close covers it; from then on only the hub can close it.
type hijackWriter struct {
	http.ResponseWriter
	hub  *Hub
	conn net.Conn
}

func (w *hijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("http.ResponseWriter does not implement http.Hijacker")
	}
	c, brw, err := hj.Hijack()
	if err != nil {
		return nil, nil, err
	}
	w.conn = c
	w.hub.trackUpgrade(c)
	if w.hub.testHookHijacked != nil {
		w.hub.testHookHijacked()
	}
	return c, brw, nil
}

// trackUpgrade makes a hijacked connection visible to Shutdown before it
// becomes a session. If the shutdown deadline has already passed, it is
// closed at once.
func (h *Hub) trackUpgrade(c net.Conn) {
	h.mu.Lock()
	h.upgrading[c] = struct{}{}
	forcing := h.forcing
	h.mu.Unlock()
	if forcing {
		_ = c.Close()
	}
}

func (h *Hub) untrackUpgrade(c net.Conn) {
	h.mu.Lock()
	delete(h.upgrading, c)
	h.mu.Unlock()
}
