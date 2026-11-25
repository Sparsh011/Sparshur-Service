// Package transfer pairs HTTP uploads and downloads without storing payloads.
package transfer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Options bounds rendezvous time, idle I/O, payload size and concurrent transfers.
// MaxBytes equal to zero allows unlimited payloads.
type Options struct {
	Wait, IdleTimeout time.Duration
	MaxBytes          int64
	MaxActive         int
}

// Broker is a blind, zero-storage rendezvous for one sender and one receiver.
type Broker struct {
	mu      sync.Mutex
	entries map[string]*entry
	closing chan struct{}
	closed  bool
	log     *slog.Logger
	opts    Options
	buffers sync.Pool
}

type entry struct {
	sender, receiver bool
	recvW            http.ResponseWriter
	recvContext      context.Context
	declaredLen      int64
	paired           chan struct{}
	headersReady     chan error
	done             chan error
}

var (
	errSizeExceeded = errors.New("transfer exceeds byte limit")
	errSenderRead   = errors.New("sender read failed or timed out")
	errReceiver     = errors.New("receiver unavailable")
)

// New creates a broker. Options must have positive Wait, IdleTimeout and MaxActive.
func New(log *slog.Logger, o Options) *Broker {
	return &Broker{
		entries: make(map[string]*entry),
		closing: make(chan struct{}),
		log:     log,
		opts:    o,
		buffers: sync.Pool{New: func() any { return new([256 << 10]byte) }},
	}
}

// Active reports the number of waiting and paired transfers.
func (b *Broker) Active() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.entries)
}

// Close refuses new transfers and wakes waiters. Paired copies finish under the
// HTTP server's graceful shutdown deadline.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.closed {
		b.closed = true
		close(b.closing)
	}
}

// Send waits for a receiver, then copies the request body directly to it.
func (b *Broker) Send(w http.ResponseWriter, r *http.Request, roomID, id string) {
	key := roomID + "/" + id
	e, status := b.join(w, r, key, true)
	if status != 0 {
		// Avoid draining an unpaired upload before sending its error response.
		w.Header().Set("Connection", "close")
		writeError(w, status, http.StatusText(status))
		return
	}
	if e == nil {
		return // The waiting client's context was cancelled.
	}

	started := time.Now()
	n, err := b.copy(w, r, e)
	// Receive may return as soon as done fires, so copy must have finished
	// using its writer and cleared its deadlines before publishing the result.
	e.done <- err
	b.mu.Lock()
	delete(b.entries, key)
	b.mu.Unlock()

	fields := []any{"room", shortID(roomID), "transfer", shortID(id),
		"bytes", n, "duration", time.Since(started)}
	if err == nil {
		b.log.Info("transfer completed", fields...)
		writeJSON(w, http.StatusOK, map[string]int64{"bytes": n})
		return
	}
	b.log.Debug("transfer failed", append(fields, "err", err)...)
	w.Header().Set("Connection", "close")
	switch {
	case errors.Is(err, errSizeExceeded):
		writeError(w, http.StatusRequestEntityTooLarge, errSizeExceeded.Error())
	case errors.Is(err, errReceiver):
		writeError(w, http.StatusBadGateway, errReceiver.Error())
	default:
		// Returning without writing would make net/http emit an implicit 200.
		// Abort the upload response to preserve failure for the sender as well.
		panic(http.ErrAbortHandler)
	}
}

// Receive writes and flushes the response headers, then keeps its writer alive
// until Send finishes. Failed copies abort the stream instead of ending cleanly.
func (b *Broker) Receive(w http.ResponseWriter, r *http.Request, roomID, id string) {
	e, status := b.join(w, r, roomID+"/"+id, false)
	if status != 0 {
		writeError(w, status, http.StatusText(status))
		return
	}
	if e == nil {
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if e.declaredLen >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(e.declaredLen, 10))
	}
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(b.opts.IdleTimeout))
	w.WriteHeader(http.StatusOK)
	// The sender waits for this flush before touching the receiver's writer.
	e.headersReady <- controller.Flush()
	if err := <-e.done; err != nil {
		panic(http.ErrAbortHandler)
	}
}

func (b *Broker) join(w http.ResponseWriter, r *http.Request, key string, sender bool) (*entry, int) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, http.StatusServiceUnavailable
	}
	if sender && b.opts.MaxBytes > 0 && r.ContentLength > b.opts.MaxBytes {
		b.mu.Unlock()
		return nil, http.StatusRequestEntityTooLarge
	}
	e := b.entries[key]
	if e == nil {
		if len(b.entries) >= b.opts.MaxActive {
			b.mu.Unlock()
			return nil, http.StatusServiceUnavailable
		}
		e = &entry{
			declaredLen:  -1,
			paired:       make(chan struct{}),
			headersReady: make(chan error, 1),
			done:         make(chan error, 1),
		}
		b.entries[key] = e
	}
	if (sender && e.sender) || (!sender && e.receiver) {
		b.mu.Unlock()
		return nil, http.StatusConflict
	}
	if sender {
		e.sender, e.declaredLen = true, r.ContentLength
	} else {
		e.receiver, e.recvW, e.recvContext = true, w, r.Context()
	}
	if e.sender && e.receiver {
		close(e.paired)
	}
	b.mu.Unlock()

	timer := time.NewTimer(b.opts.Wait)
	defer timer.Stop()
	status := http.StatusGatewayTimeout
	select {
	case <-e.paired:
		return e, 0
	case <-timer.C:
	case <-r.Context().Done():
		status = 0
	case <-b.closing:
		status = http.StatusServiceUnavailable
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// Pairing wins a simultaneous timeout, cancellation or shutdown. Once
	// paired, Send owns completion and Receive must keep its writer alive.
	if e.sender && e.receiver {
		return e, 0
	}
	if b.entries[key] == e {
		delete(b.entries, key)
	}
	return nil, status
}

func (b *Broker) copy(senderW http.ResponseWriter, r *http.Request, e *entry) (int64, error) {
	rc := http.NewResponseController(senderW)
	wc := http.NewResponseController(e.recvW)
	// A disconnected receiver must also interrupt an idle upload read.
	interrupted := make(chan struct{})
	stop := context.AfterFunc(e.recvContext, func() {
		_ = rc.SetReadDeadline(time.Now())
		close(interrupted)
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
		_ = rc.SetReadDeadline(time.Time{})
		_ = wc.SetWriteDeadline(time.Time{})
	}()
	if err := <-e.headersReady; err != nil {
		return 0, errors.Join(errReceiver, err)
	}

	buffer := b.buffers.Get().(*[256 << 10]byte)
	defer b.buffers.Put(buffer)
	var total int64
	for {
		_ = rc.SetReadDeadline(time.Now().Add(b.opts.IdleTimeout))
		if e.recvContext.Err() != nil {
			return total, errReceiver
		}
		chunk := buffer[:]
		// Read one extra byte to detect an oversized unknown-length body,
		// without forwarding bytes beyond the configured limit.
		if b.opts.MaxBytes > 0 && b.opts.MaxBytes-total < int64(len(chunk)) {
			chunk = chunk[:b.opts.MaxBytes-total+1]
		}
		n, readErr := r.Body.Read(chunk)
		if b.opts.MaxBytes > 0 && int64(n) > b.opts.MaxBytes-total {
			return total, errSizeExceeded
		}
		if n > 0 {
			_ = wc.SetWriteDeadline(time.Now().Add(b.opts.IdleTimeout))
			written, err := e.recvW.Write(chunk[:n])
			total += int64(written)
			if err != nil || written != n {
				return total, errors.Join(errReceiver, err, io.ErrShortWrite)
			}
			if err := wc.Flush(); err != nil {
				return total, errors.Join(errReceiver, err)
			}
		}
		if errors.Is(readErr, io.EOF) {
			if e.declaredLen >= 0 && total != e.declaredLen {
				return total, errors.Join(errSenderRead, io.ErrUnexpectedEOF)
			}
			return total, nil
		}
		if readErr != nil {
			if e.recvContext.Err() != nil {
				return total, errReceiver
			}
			return total, errors.Join(errSenderRead, readErr)
		}
	}
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
