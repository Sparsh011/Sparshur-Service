// Command relay runs the Sparshur end-to-end encrypted WebSocket relay.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sparshchadha/sparshur-service/internal/config"
	"github.com/sparshchadha/sparshur-service/internal/relay"
	"github.com/sparshchadha/sparshur-service/internal/server"
)

// logQueueSize bounds buffered log lines; beyond it lines are dropped
// rather than blocking the relay.
const logQueueSize = 1024

func main() {
	// Every log write goes through a queue that never blocks, so a stuck
	// stdout cannot stop the relay from serving, shutting down, or exiting.
	out := newAsyncWriter(os.Stdout, logQueueSize)
	level := new(slog.LevelVar)
	log := slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	err := run(log, level)
	if err != nil {
		log.Error("relay exited", "err", err)
	}
	// Best effort only; exit regardless once the time is up.
	out.Flush(time.Second)
	if err != nil {
		os.Exit(1)
	}
}

func run(log *slog.Logger, level *slog.LevelVar) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return err
	}
	level.Set(l)

	hub := relay.NewHub(log)
	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           server.New(cfg, hub, log).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("relay listening", "addr", cfg.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
	}

	// Start the deadline before anything else, logging included.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	log.Info("shutting down")
	// srv.Shutdown closes the listener and drains plain HTTP requests, but
	// neither closes nor waits for hijacked WebSocket connections; the hub
	// drains those in parallel under the same deadline.
	srvErr := make(chan error, 1)
	go func() { srvErr <- srv.Shutdown(shutdownCtx) }()
	hubErr := hub.Shutdown(shutdownCtx)
	if hubErr != nil {
		hubErr = fmt.Errorf("drain websocket sessions: %w", hubErr)
	}
	err = <-srvErr
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		_ = srv.Close()
		err = fmt.Errorf("shut down http server: %w", err)
	} else {
		err = nil
	}
	return errors.Join(hubErr, err)
}
