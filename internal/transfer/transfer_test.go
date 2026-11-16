package transfer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func newBrokerServer(t *testing.T, change ...func(*Options)) (*Broker, *httptest.Server) {
	t.Helper()
	opts := Options{Wait: 2 * time.Second, IdleTimeout: 2 * time.Second, MaxActive: 16}
	for _, f := range change {
		f(&opts)
	}
	broker := New(slog.New(slog.NewTextHandler(io.Discard, nil)), opts)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			broker.Send(w, r, "room-0123456789abcdefghij", r.URL.Path)
		} else {
			broker.Receive(w, r, "room-0123456789abcdefghij", r.URL.Path)
		}
	}))
	ts.Client().Timeout = 15 * time.Second
	t.Cleanup(func() {
		broker.Close()
		ts.CloseClientConnections()
		ts.Close()
		if n := broker.Active(); n != 0 {
			t.Errorf("leaked %d transfers", n)
		}
	})
	return broker, ts
}

type response struct {
	resp *http.Response
	err  error
}

func requestAsync(client *http.Client, req *http.Request) <-chan response {
	ch := make(chan response, 1)
	go func() {
		r, err := client.Do(req)
		ch <- response{r, err}
	}()
	return ch
}

func request(t *testing.T, method, target string, body io.Reader) *http.Request {
	t.Helper()
	r, err := http.NewRequest(method, target, body)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func await(t *testing.T, ch <-chan response) *http.Response {
	t.Helper()
	select {
	case result := <-ch:
		if result.err != nil {
			t.Fatal(result.err)
		}
		t.Cleanup(func() { result.resp.Body.Close() })
		return result.resp
	case <-time.After(15 * time.Second):
		t.Fatal("request did not finish")
		return nil
	}
}

func waitActive(t *testing.T, broker *Broker, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for broker.Active() != want {
		if time.Now().After(deadline) {
			t.Fatalf("Active = %d, want %d", broker.Active(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func requireStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	defer resp.Body.Close()
	if resp.StatusCode != want {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d: %s", resp.StatusCode, want, body)
	}
	if want != http.StatusOK {
		var body map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil || body["error"] == "" {
			t.Fatalf("invalid error JSON: %v, %v", body, err)
		}
	}
}

func TestLargeStream(t *testing.T) {
	for _, senderFirst := range []bool{true, false} {
		t.Run(strconv.FormatBool(senderFirst), func(t *testing.T) {
			broker, ts := newBrokerServer(t)
			const size = 64 << 20
			sentHash := sha256.New()
			put := request(t, http.MethodPut, ts.URL+"/large", io.TeeReader(io.LimitReader(rand.Reader, size), sentHash))
			put.ContentLength = size
			get := request(t, http.MethodGet, ts.URL+"/large", nil)
			var upload, download <-chan response
			if senderFirst {
				upload = requestAsync(ts.Client(), put)
				waitActive(t, broker, 1)
				download = requestAsync(ts.Client(), get)
			} else {
				download = requestAsync(ts.Client(), get)
				waitActive(t, broker, 1)
				upload = requestAsync(ts.Client(), put)
			}
			received := await(t, download)
			gotHash := sha256.New()
			n, err := io.Copy(gotHash, received.Body)
			if err != nil || n != size {
				t.Fatalf("received %d bytes: %v", n, err)
			}
			result := await(t, upload)
			if result.StatusCode != http.StatusOK {
				t.Fatalf("upload status = %d", result.StatusCode)
			}
			var receipt struct{ Bytes int64 }
			if err := json.NewDecoder(result.Body).Decode(&receipt); err != nil || receipt.Bytes != size {
				t.Fatalf("receipt = %+v: %v", receipt, err)
			}
			if !bytes.Equal(sentHash.Sum(nil), gotHash.Sum(nil)) {
				t.Fatal("SHA-256 mismatch")
			}
			waitActive(t, broker, 0)
		})
	}
}

func TestResponseHeaders(t *testing.T) {
	for _, knownLength := range []bool{true, false} {
		t.Run(strconv.FormatBool(knownLength), func(t *testing.T) {
			broker, ts := newBrokerServer(t)
			payload := "encrypted payload"
			put := request(t, http.MethodPut, ts.URL+"/headers", strings.NewReader(payload))
			if !knownLength {
				put.ContentLength = -1
			}
			upload := requestAsync(ts.Client(), put)
			download := await(t, requestAsync(ts.Client(), request(t, http.MethodGet, ts.URL+"/headers", nil)))
			wantLength := ""
			if knownLength {
				wantLength = strconv.Itoa(len(payload))
			}
			if download.Header.Get("Content-Length") != wantLength ||
				download.Header.Get("Content-Type") != "application/octet-stream" ||
				download.Header.Get("Cache-Control") != "no-store" ||
				download.Header.Get("X-Content-Type-Options") != "nosniff" {
				t.Fatalf("headers = %v", download.Header)
			}
			got, err := io.ReadAll(download.Body)
			if err != nil || string(got) != payload {
				t.Fatalf("body = %q: %v", got, err)
			}
			requireStatus(t, await(t, upload), http.StatusOK)
			waitActive(t, broker, 0)
		})
	}
}

func TestBackpressureAndReceiverAbort(t *testing.T) {
	broker, ts := newBrokerServer(t)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	var accepted atomic.Int64
	producer := make(chan struct{})
	go func() {
		defer close(producer)
		chunk := make([]byte, 256<<10)
		for accepted.Load() < 256<<20 {
			n, err := writer.Write(chunk)
			accepted.Add(int64(n))
			if err != nil {
				return
			}
		}
		writer.Close()
	}()
	put := request(t, http.MethodPut, ts.URL+"/backpressure", reader)
	put.ContentLength = 256 << 20
	upload := requestAsync(ts.Client(), put)
	download := await(t, requestAsync(ts.Client(), request(t, http.MethodGet, ts.URL+"/backpressure", nil)))
	time.Sleep(300 * time.Millisecond)
	if got := accepted.Load(); got >= 64<<20 {
		t.Fatalf("accepted %d bytes while receiver was not reading", got)
	}
	download.Body.Close()
	result := <-upload
	if result.err == nil {
		requireStatus(t, result.resp, http.StatusBadGateway)
	}
	reader.Close()
	<-producer
	waitActive(t, broker, 0)
}

func TestSenderAbort(t *testing.T) {
	broker, ts := newBrokerServer(t)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	upload := requestAsync(ts.Client(), request(t, http.MethodPut, ts.URL+"/abort", reader))
	download := await(t, requestAsync(ts.Client(), request(t, http.MethodGet, ts.URL+"/abort", nil)))
	if _, err := writer.Write([]byte("partial encrypted body")); err != nil {
		t.Fatal(err)
	}
	writer.CloseWithError(errors.New("upload interrupted"))
	if _, err := io.ReadAll(download.Body); err == nil {
		t.Fatal("aborted sender produced a clean EOF")
	}
	result := <-upload
	if result.resp != nil {
		result.resp.Body.Close()
	}
	waitActive(t, broker, 0)
}

func TestIdleSender(t *testing.T) {
	broker, ts := newBrokerServer(t, func(o *Options) { o.IdleTimeout = 100 * time.Millisecond })
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	upload := requestAsync(ts.Client(), request(t, http.MethodPut, ts.URL+"/idle", reader))
	download := await(t, requestAsync(ts.Client(), request(t, http.MethodGet, ts.URL+"/idle", nil)))
	if _, err := writer.Write([]byte("prefix")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(download.Body); err == nil {
		t.Fatal("idle sender produced a clean EOF")
	}
	// Release the client's body reader even if its transport is still uploading.
	writer.Close()
	result := <-upload
	if result.err == nil {
		requireStatus(t, result.resp, http.StatusRequestTimeout)
	}
	waitActive(t, broker, 0)
}

func TestWaitTimeout(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			broker, ts := newBrokerServer(t, func(o *Options) { o.Wait = 30 * time.Millisecond })
			resp := await(t, requestAsync(ts.Client(), request(t, method, ts.URL+"/timeout", nil)))
			requireStatus(t, resp, http.StatusGatewayTimeout)
			waitActive(t, broker, 0)
		})
	}
}

func TestDuplicateAndCapacity(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			broker, ts := newBrokerServer(t, func(o *Options) { o.MaxActive = 1 })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := requestAsync(ts.Client(), request(t, method, ts.URL+"/duplicate", nil).WithContext(ctx))
			waitActive(t, broker, 1)
			requireStatus(t, await(t, requestAsync(ts.Client(), request(t, method, ts.URL+"/duplicate", nil))), http.StatusConflict)
			requireStatus(t, await(t, requestAsync(ts.Client(), request(t, method, ts.URL+"/other", nil))), http.StatusServiceUnavailable)
			cancel()
			result := <-first
			if result.resp != nil {
				result.resp.Body.Close()
			}
			waitActive(t, broker, 0)
		})
	}
}

func TestCloseWakesWaiters(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodGet} {
		t.Run(method, func(t *testing.T) {
			broker, ts := newBrokerServer(t)
			first := requestAsync(ts.Client(), request(t, method, ts.URL+"/close", nil))
			waitActive(t, broker, 1)
			broker.Close()
			broker.Close()
			requireStatus(t, await(t, first), http.StatusServiceUnavailable)
			for _, m := range []string{http.MethodPut, http.MethodGet} {
				requireStatus(t, await(t, requestAsync(ts.Client(), request(t, m, ts.URL+"/closed", nil))), http.StatusServiceUnavailable)
			}
			waitActive(t, broker, 0)
		})
	}
}

func TestByteLimit(t *testing.T) {
	t.Run("declared", func(t *testing.T) {
		broker, ts := newBrokerServer(t, func(o *Options) { o.MaxBytes = 3 })
		requireStatus(t, await(t, requestAsync(ts.Client(), request(t, http.MethodPut, ts.URL+"/limit", strings.NewReader("oversize")))), http.StatusRequestEntityTooLarge)
		waitActive(t, broker, 0)
	})
	t.Run("chunked", func(t *testing.T) {
		broker, ts := newBrokerServer(t, func(o *Options) { o.MaxBytes = 3 })
		put := request(t, http.MethodPut, ts.URL+"/limit", strings.NewReader("oversize"))
		put.ContentLength = -1
		upload := requestAsync(ts.Client(), put)
		download := await(t, requestAsync(ts.Client(), request(t, http.MethodGet, ts.URL+"/limit", nil)))
		if _, err := io.ReadAll(download.Body); err == nil {
			t.Fatal("oversized body produced a clean EOF")
		}
		requireStatus(t, await(t, upload), http.StatusRequestEntityTooLarge)
		waitActive(t, broker, 0)
	})
	t.Run("exact", func(t *testing.T) {
		broker, ts := newBrokerServer(t, func(o *Options) { o.MaxBytes = 3 })
		put := request(t, http.MethodPut, ts.URL+"/limit", strings.NewReader("123"))
		put.ContentLength = -1
		upload := requestAsync(ts.Client(), put)
		download := await(t, requestAsync(ts.Client(), request(t, http.MethodGet, ts.URL+"/limit", nil)))
		got, err := io.ReadAll(download.Body)
		if err != nil || string(got) != "123" {
			t.Fatalf("received %q: %v", got, err)
		}
		requireStatus(t, await(t, upload), http.StatusOK)
		waitActive(t, broker, 0)
	})
}

func TestExpectContinueWaitsForReceiver(t *testing.T) {
	broker, ts := newBrokerServer(t)
	var reads atomic.Int64
	body := &countingReader{Reader: strings.NewReader("secret"), reads: &reads}
	transport := &http.Transport{ExpectContinueTimeout: time.Second}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	put := request(t, http.MethodPut, ts.URL+"/continue", body)
	put.Header.Set("Expect", "100-continue")
	upload := requestAsync(client, put)
	waitActive(t, broker, 1)
	time.Sleep(50 * time.Millisecond)
	if reads.Load() != 0 {
		t.Fatal("sender body read before receiver paired")
	}
	download := await(t, requestAsync(client, request(t, http.MethodGet, ts.URL+"/continue", nil)))
	got, err := io.ReadAll(download.Body)
	if err != nil || string(got) != "secret" {
		t.Fatalf("received %q: %v", got, err)
	}
	requireStatus(t, await(t, upload), http.StatusOK)
	waitActive(t, broker, 0)
}

type countingReader struct {
	io.Reader
	reads *atomic.Int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	return r.Reader.Read(p)
}

func TestCloseAllowsPairedCopyToFinish(t *testing.T) {
	broker, ts := newBrokerServer(t)
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	upload := requestAsync(ts.Client(), request(t, http.MethodPut, ts.URL+"/drain", reader))
	download := await(t, requestAsync(ts.Client(), request(t, http.MethodGet, ts.URL+"/drain", nil)))
	broker.Close()
	if broker.Active() != 1 {
		t.Fatal("Close removed an in-flight transfer")
	}
	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(writer, "finish during shutdown")
		writer.Close()
		written <- err
	}()
	got, err := io.ReadAll(download.Body)
	if err != nil || string(got) != "finish during shutdown" {
		t.Fatalf("received %q: %v", got, err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	requireStatus(t, await(t, upload), http.StatusOK)
	waitActive(t, broker, 0)
}

func TestReceiverDisconnectInterruptsIdleSender(t *testing.T) {
	broker, ts := newBrokerServer(t, func(o *Options) { o.IdleTimeout = 10 * time.Second })
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	upload := requestAsync(ts.Client(), request(t, http.MethodPut, ts.URL+"/receiver-abort", reader))
	download := await(t, requestAsync(ts.Client(), request(t, http.MethodGet, ts.URL+"/receiver-abort", nil)))
	download.Body.Close()
	// Completion must happen before the upload's idle timeout.
	waitActive(t, broker, 0)
	writer.Close()
	result := <-upload
	if result.err == nil {
		requireStatus(t, result.resp, http.StatusBadGateway)
	}
}
