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

	hw := &hijackWriter{ResponseWriter: w}
	ws, err := websocket.Accept(hw, r, opts)
	if err != nil {
		// Accept has already written an HTTP error response.
		h.active.Done()
		return nil, err
	}
	return &Conn{ws: ws, raw: hw.conn}, nil
}

// hijackWriter records the connection websocket.Accept hijacks.
type hijackWriter struct {
	http.ResponseWriter
	conn net.Conn
}

func (w *hijackWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("http.ResponseWriter does not implement http.Hijacker")
	}
	c, brw, err := hj.Hijack()
	w.conn = c
	return c, brw, err
}
