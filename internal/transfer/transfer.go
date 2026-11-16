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
	closed  chan struct{}
	log     *slog.Logger
	opts    Options
	buffers sync.Pool
}

type entry struct {
	sender, receiver *http.Request
	receiverW        http.ResponseWriter
	ready, done      chan struct{}
	abort            bool // Written by Send before done closes, read by Receive afterwards.
}

// New creates a broker. Options must have positive Wait, IdleTimeout and MaxActive.
func New(log *slog.Logger, opts Options) *Broker {
	return &Broker{
		entries: make(map[string]*entry),
		closed:  make(chan struct{}),
		log:     log,
		opts:    opts,
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
	select {
	case <-b.closed:
		return
	default:
		close(b.closed)
	}
	for key, e := range b.entries {
		if e.sender == nil || e.receiver == nil {
			delete(b.entries, key)
		}
	}
}

// Send waits for a receiver, then copies the request body directly to it.
func (b *Broker) Send(w http.ResponseWriter, r *http.Request, roomID, transferID string) {
	if b.opts.MaxBytes > 0 && r.ContentLength > b.opts.MaxBytes {
		w.Header().Set("Connection", "close")
		writeError(w, http.StatusRequestEntityTooLarge, "transfer exceeds byte limit")
		return
	}
	e, status := b.join(w, r, roomID+"/"+transferID, true)
	if status != 0 {
		// Avoid draining an unpaired upload before sending its error response.
		w.Header().Set("Connection", "close")
		writeError(w, status, http.StatusText(status))
		return
	}

	started := time.Now()
	n, status, message := b.copy(w, r, e)
	e.abort = status != http.StatusOK
	b.mu.Lock()
	delete(b.entries, roomID+"/"+transferID)
	b.mu.Unlock()
	close(e.done)
	b.log.Info("transfer completed", "room", shortID(roomID), "transfer", shortID(transferID),
		"bytes", n, "duration", time.Since(started), "status", status)
	if status != http.StatusOK {
		w.Header().Set("Connection", "close")
		writeError(w, status, message)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"bytes": n})
}

// Receive waits until Send finishes using its ResponseWriter. A failed copy
// aborts the HTTP stream so an incomplete chunked body cannot appear complete.
func (b *Broker) Receive(w http.ResponseWriter, r *http.Request, roomID, transferID string) {
	e, status := b.join(w, r, roomID+"/"+transferID, false)
	if status != 0 {
		writeError(w, status, http.StatusText(status))
		return
	}
	<-e.done
	if e.abort {
		panic(http.ErrAbortHandler)
	}
}

func (b *Broker) join(w http.ResponseWriter, r *http.Request, key string, sender bool) (*entry, int) {
	b.mu.Lock()
	select {
	case <-b.closed:
		b.mu.Unlock()
		return nil, http.StatusServiceUnavailable
	default:
	}
	e := b.entries[key]
	if e == nil {
		if len(b.entries) >= b.opts.MaxActive {
			b.mu.Unlock()
			return nil, http.StatusServiceUnavailable
		}
		e = &entry{ready: make(chan struct{}), done: make(chan struct{})}
		b.entries[key] = e
	}
	if (sender && e.sender != nil) || (!sender && e.receiver != nil) {
		b.mu.Unlock()
		return nil, http.StatusConflict
	}
	if sender {
		e.sender = r
	} else {
		e.receiver, e.receiverW = r, w
	}
	if e.sender != nil && e.receiver != nil {
		close(e.ready)
	}
	b.mu.Unlock()

	timer := time.NewTimer(b.opts.Wait)
	defer timer.Stop()
	status := http.StatusGatewayTimeout
	select {
	case <-e.ready:
		return e, 0
	case <-timer.C:
	case <-r.Context().Done():
		status = http.StatusRequestTimeout
	case <-b.closed:
		status = http.StatusServiceUnavailable
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// Pairing wins a simultaneous timeout, cancellation or shutdown. Once
	// paired, Send owns completion and Receive must keep its writer alive.
	if e.sender != nil && e.receiver != nil {
		return e, 0
	}
	if b.entries[key] == e {
		delete(b.entries, key)
	}
	return nil, status
}

func (b *Broker) copy(senderW http.ResponseWriter, r *http.Request, e *entry) (int64, int, string) {
	sender := http.NewResponseController(senderW)
	receiver := http.NewResponseController(e.receiverW)
	// A disconnected receiver must also interrupt an idle upload read.
	interrupted := make(chan struct{})
	stop := context.AfterFunc(e.receiver.Context(), func() {
		_ = sender.SetReadDeadline(time.Now())
		close(interrupted)
	})
	defer func() {
		if !stop() {
			<-interrupted
		}
		_ = sender.SetReadDeadline(time.Time{})
		_ = receiver.SetWriteDeadline(time.Time{})
	}()

	w := e.receiverW
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.ContentLength >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(r.ContentLength, 10))
	}
	_ = receiver.SetWriteDeadline(time.Now().Add(b.opts.IdleTimeout))
	w.WriteHeader(http.StatusOK)
	if err := receiver.Flush(); err != nil {
		return 0, http.StatusBadGateway, "receiver unavailable"
	}

	buffer := b.buffers.Get().(*[256 << 10]byte)
	defer b.buffers.Put(buffer)
	var total int64
	for {
		_ = sender.SetReadDeadline(time.Now().Add(b.opts.IdleTimeout))
		if e.receiver.Context().Err() != nil {
			return total, http.StatusBadGateway, "receiver disconnected"
		}
		chunk := buffer[:]
		// Read one extra byte to detect an oversized unknown-length body,
		// without forwarding bytes beyond the configured limit.
		if b.opts.MaxBytes > 0 && b.opts.MaxBytes-total < int64(len(chunk)) {
			chunk = chunk[:b.opts.MaxBytes-total+1]
		}
		n, readErr := r.Body.Read(chunk)
		if b.opts.MaxBytes > 0 && int64(n) > b.opts.MaxBytes-total {
			return total, http.StatusRequestEntityTooLarge, "transfer exceeds byte limit"
		}
		if n > 0 {
			_ = receiver.SetWriteDeadline(time.Now().Add(b.opts.IdleTimeout))
			written, err := w.Write(chunk[:n])
			total += int64(written)
			if err != nil || written != n {
				return total, http.StatusBadGateway, "receiver write failed"
			}
			if err := receiver.Flush(); err != nil {
				return total, http.StatusBadGateway, "receiver write failed"
			}
		}
		if errors.Is(readErr, io.EOF) {
			return total, http.StatusOK, ""
		}
		if readErr != nil {
			if e.receiver.Context().Err() != nil {
				return total, http.StatusBadGateway, "receiver disconnected"
			}
			return total, http.StatusRequestTimeout, "sender read failed or timed out"
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
