package bridge

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

// WatchEnrollment reloads the registry on SIGHUP. A legacy-only service
// ignores that signal so the same service-manager unit can reload either mode.
func WatchEnrollment(ctx context.Context, hub *Hub, log *slog.Logger) func() {
	ctx, cancel := context.WithCancel(ctx)
	reloads := make(chan os.Signal, 1)
	signal.Notify(reloads, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-reloads:
				if hub == nil {
					continue
				}
				if err := hub.Reload(); err != nil {
					log.Warn("relay enrollment reload failed", "err", err)
				} else {
					log.Info("relay enrollment reloaded")
				}
			}
		}
	}()
	return func() {
		signal.Stop(reloads)
		cancel()
	}
}
