package main

import (
	"io"
	"sync/atomic"
	"time"
)

// asyncWriter is an io.Writer that never blocks its caller. Each Write is
// copied onto a bounded queue that one goroutine drains into the underlying
// writer; when the queue is full the write is dropped and counted. A stuck
// log sink (a full stdout pipe, a hung collector) therefore cannot stall
// request handling, shutdown, or exit.
type asyncWriter struct {
	queue   chan logEntry
	dropped atomic.Uint64
}

// logEntry is either a log line or, with flushed set, a marker that is
// signalled once every earlier line has been written.
type logEntry struct {
	line    []byte
	flushed chan struct{}
}

func newAsyncWriter(w io.Writer, size int) *asyncWriter {
	a := &asyncWriter{queue: make(chan logEntry, size)}
	go func() {
		for e := range a.queue {
			if e.flushed != nil {
				close(e.flushed)
				continue
			}
			_, _ = w.Write(e.line)
		}
	}()
	return a
}

// Write queues a copy of p. It always reports success: there is nowhere
// better to report a logging failure.
func (a *asyncWriter) Write(p []byte) (int, error) {
	select {
	case a.queue <- logEntry{line: append([]byte(nil), p...)}:
	default:
		a.dropped.Add(1)
	}
	return len(p), nil
}

// Flush waits up to timeout for lines queued before the call to be written,
// and reports whether they were.
func (a *asyncWriter) Flush(timeout time.Duration) bool {
	t := time.NewTimer(timeout)
	defer t.Stop()
	flushed := make(chan struct{})
	select {
	case a.queue <- logEntry{flushed: flushed}:
	case <-t.C:
		return false
	}
	select {
	case <-flushed:
		return true
	case <-t.C:
		return false
	}
}
