package relay

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// Options tunes per-connection behaviour.
type Options struct {
	MaxMessageBytes int64
	SendQueueSize   int
	PingInterval    time.Duration
	WriteTimeout    time.Duration
}

type frame struct {
	typ  websocket.MessageType
	data []byte
}

// Peer is one device's WebSocket connection inside a room.
type Peer struct {
	hub    *Hub
	conn   *websocket.Conn
	roomID string
	role   Role
	opts   Options
	log    *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	send      chan frame
	done      chan struct{} // closed when the peer starts shutting down
	closed    chan struct{} // closed once the WebSocket close handshake ends
	closeOnce sync.Once
}

// Serve runs a relay session for an accepted WebSocket connection and blocks
// until the connection ends.
func (h *Hub) Serve(ctx context.Context, conn *websocket.Conn, roomID string, role Role, opts Options) {
	ctx, cancel := context.WithCancel(ctx)
	p := &Peer{
		hub:    h,
		conn:   conn,
		roomID: roomID,
		role:   role,
		opts:   opts,
		log:    h.log.With("room", logRoom(roomID), "role", role),
		ctx:    ctx,
		cancel: cancel,
		send:   make(chan frame, opts.SendQueueSize),
		done:   make(chan struct{}),
		closed: make(chan struct{}),
	}
	conn.SetReadLimit(opts.MaxMessageBytes)

	go p.writeLoop()
	if opts.PingInterval > 0 {
		go p.pingLoop()
	}

	if !h.join(p) {
		p.close(websocket.StatusGoingAway, "relay shutting down")
		<-p.closed
		return
	}
	p.readLoop()
	h.leave(p)
	<-p.closed
}

func (p *Peer) readLoop() {
	for {
		typ, data, err := p.conn.Read(p.ctx)
		if err != nil {
			if status := websocket.CloseStatus(err); status != -1 {
				p.log.Debug("client closed connection", "status", status)
			} else if !errors.Is(err, context.Canceled) {
				p.log.Debug("read failed", "err", err)
			}
			p.close(websocket.StatusNormalClosure, "")
			return
		}
		if typ != websocket.MessageBinary {
			p.close(websocket.StatusPolicyViolation, "text frames are reserved for relay control messages")
			return
		}
		p.hub.forward(p, typ, data)
	}
}

func (p *Peer) writeLoop() {
	for {
		select {
		case <-p.done:
			return
		case f := <-p.send:
			ctx, cancel := context.WithTimeout(p.ctx, p.opts.WriteTimeout)
			err := p.conn.Write(ctx, f.typ, f.data)
			cancel()
			if err != nil {
				p.log.Debug("write failed", "err", err)
				p.close(websocket.StatusInternalError, "write failed")
				return
			}
		}
	}
}

func (p *Peer) pingLoop() {
	t := time.NewTicker(p.opts.PingInterval)
	defer t.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(p.ctx, p.opts.WriteTimeout)
			err := p.conn.Ping(ctx)
			cancel()
			if err != nil {
				p.log.Debug("ping failed", "err", err)
				p.close(websocket.StatusGoingAway, "ping timeout")
				return
			}
		}
	}
}

// enqueue queues a frame for delivery without blocking the sender. A peer
// whose queue is full is disconnected rather than allowed to stall its
// counterpart. The hub calls it while holding its lock to keep delivery
// ordered, so it must never block or perform network I/O itself.
func (p *Peer) enqueue(f frame) {
	select {
	case <-p.done:
	case p.send <- f:
	default:
		p.log.Warn("send queue full, disconnecting slow consumer")
		p.close(StatusSlowConsumer, "send queue overflow")
	}
}

func (p *Peer) sendControl(m ControlMessage) {
	b, err := json.Marshal(m)
	if err != nil {
		p.log.Error("marshal control message", "err", err)
		return
	}
	p.enqueue(frame{typ: websocket.MessageText, data: b})
}

// close starts shutting the peer down. It is safe to call many times and
// from any goroutine; only the first call's status is sent to the client.
func (p *Peer) close(code websocket.StatusCode, reason string) {
	p.closeOnce.Do(func() {
		close(p.done)
		go func() {
			// Close performs the close handshake, which needs the read loop
			// still running, so the context is cancelled only afterwards.
			_ = p.conn.Close(code, reason)
			p.cancel()
			close(p.closed)
		}()
	})
}
