package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// runMainEnv makes the test binary act as the relay executable, so the
// SIGTERM test exercises the real main without a separate build step.
const runMainEnv = "RELAY_TEST_RUN_MAIN"

func TestMain(m *testing.M) {
	if os.Getenv(runMainEnv) == "1" {
		main()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// stuckWriter blocks every Write until released.
type stuckWriter struct{ release chan struct{} }

func (w stuckWriter) Write(p []byte) (int, error) {
	<-w.release
	return len(p), nil
}

func TestAsyncWriterNeverBlocksOnStuckSink(t *testing.T) {
	sink := stuckWriter{release: make(chan struct{})}
	defer close(sink.release)
	w := newAsyncWriter(sink, 4)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			fmt.Fprintf(w, "line %d\n", i)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Write blocked on a stuck sink")
	}
	// At most one line is in the stuck Write and four are queued.
	if d := w.dropped.Load(); d < 95 {
		t.Fatalf("dropped = %d, want at least 95", d)
	}
	start := time.Now()
	if w.Flush(50 * time.Millisecond) {
		t.Fatal("Flush reported success with the sink stuck")
	}
	if time.Since(start) > time.Second {
		t.Fatal("Flush overran its timeout")
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestAsyncWriterFlushWritesInOrder(t *testing.T) {
	var buf syncBuffer
	w := newAsyncWriter(&buf, 64)
	var want strings.Builder
	for i := 0; i < 50; i++ {
		line := fmt.Sprintf("line %d\n", i)
		want.WriteString(line)
		// The caller may reuse its buffer; the writer must have copied it.
		b := []byte(line)
		w.Write(b)
		copy(b, "XXXX")
	}
	if !w.Flush(5 * time.Second) {
		t.Fatal("Flush timed out")
	}
	if got := buf.String(); got != want.String() {
		t.Fatalf("got %q", got)
	}
}

// fullPipe returns the write end of a pipe whose buffer is already full, so
// any process writing to it blocks.
func fullPipe(t *testing.T) *os.File {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		r.Close()
		w.Close()
	})
	chunk := make([]byte, 4096)
	for {
		_ = w.SetWriteDeadline(time.Now().Add(50 * time.Millisecond))
		if _, err := w.Write(chunk); err != nil {
			if errors.Is(err, os.ErrDeadlineExceeded) {
				return w
			}
			t.Fatal(err)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// TestSIGTERMWithBlockedOutput runs the relay with stdout and stderr stuck
// and a WebSocket client that never answers the close handshake. On SIGTERM
// the relay must still close the client's socket at the shutdown deadline
// and exit, instead of hanging on a log write.
func TestSIGTERMWithBlockedOutput(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	out := fullPipe(t)
	addr := freeAddr(t)

	cmd := exec.Command(exe, "-test.run=^$")
	cmd.Env = append(os.Environ(),
		runMainEnv+"=1",
		"RELAY_ADDR="+addr,
		"RELAY_SHUTDOWN_TIMEOUT=300ms",
	)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var waitErr error
	exited := make(chan struct{})
	go func() {
		waitErr = cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-exited
	})

	// The relay logs before it listens; with blocking logging it never
	// becomes ready.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay never became ready with stdout blocked: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	conn, br := rawUpgrade(t, addr, "/v1/rooms/test-room-0123456789abcdef/ws?role=mac")

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// Shutdown deadline (300ms) plus the bounded final log flush (1s).
	select {
	case <-exited:
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != 1 {
			t.Fatalf("relay exit = %v, want status 1 (stalled session force-closed)", waitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not exit after SIGTERM with stdout blocked")
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.Copy(io.Discard, br); errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("relay exited without closing the client socket")
	}
}

// rawUpgrade completes a WebSocket upgrade over plain TCP and then never
// speaks WebSocket, so it never answers the relay's close handshake.
func rawUpgrade(t *testing.T, addr, path string) (net.Conn, *bufio.Reader) {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	req := "GET " + path + " HTTP/1.1\r\nHost: " + addr +
		"\r\nUpgrade: websocket\r\nConnection: Upgrade" +
		"\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(c)
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("upgrade response: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d", resp.StatusCode)
	}
	return c, br
}
