// Package config loads relay settings from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime settings for the relay.
type Config struct {
	// Addr is the TCP address the HTTP server listens on.
	Addr string

	// MaxMessageBytes caps the size of a single relayed frame.
	MaxMessageBytes int64

	// SendQueueSize is the number of frames buffered per connection before
	// the connection is treated as a slow consumer and dropped.
	SendQueueSize int

	// PingInterval is how often the relay pings each client to keep NATs and
	// proxies from timing out idle connections and to detect dead peers.
	PingInterval time.Duration

	// WriteTimeout bounds a single write (frame or ping) to a client.
	WriteTimeout time.Duration

	// ShutdownTimeout is how long in-flight connections get on SIGTERM.
	ShutdownTimeout time.Duration

	// AllowedOrigins lists Origin host patterns accepted on WebSocket upgrade.
	// Native clients (macOS app, Android app) usually send no Origin header,
	// which is always accepted.
	AllowedOrigins []string

	// LogLevel is one of debug, info, warn, error.
	LogLevel string
}

// Load reads configuration from the environment, applying defaults.
func Load() (Config, error) {
	cfg := Config{
		Addr:            getenv("RELAY_ADDR", ":8080"),
		MaxMessageBytes: 1 << 20,
		SendQueueSize:   64,
		PingInterval:    25 * time.Second,
		WriteTimeout:    10 * time.Second,
		ShutdownTimeout: 15 * time.Second,
		LogLevel:        getenv("RELAY_LOG_LEVEL", "info"),
	}

	var err error
	if cfg.MaxMessageBytes, err = intEnv("RELAY_MAX_MESSAGE_BYTES", cfg.MaxMessageBytes); err != nil {
		return Config{}, err
	}
	queue, err := intEnv("RELAY_SEND_QUEUE_SIZE", int64(cfg.SendQueueSize))
	if err != nil {
		return Config{}, err
	}
	cfg.SendQueueSize = int(queue)
	if cfg.PingInterval, err = durationEnv("RELAY_PING_INTERVAL", cfg.PingInterval); err != nil {
		return Config{}, err
	}
	if cfg.WriteTimeout, err = durationEnv("RELAY_WRITE_TIMEOUT", cfg.WriteTimeout); err != nil {
		return Config{}, err
	}
	if cfg.ShutdownTimeout, err = durationEnv("RELAY_SHUTDOWN_TIMEOUT", cfg.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if v := os.Getenv("RELAY_ALLOWED_ORIGINS"); v != "" {
		for _, o := range strings.Split(v, ",") {
			if o = strings.TrimSpace(o); o != "" {
				cfg.AllowedOrigins = append(cfg.AllowedOrigins, o)
			}
		}
	}

	if cfg.MaxMessageBytes <= 0 {
		return Config{}, fmt.Errorf("RELAY_MAX_MESSAGE_BYTES must be positive")
	}
	if cfg.SendQueueSize <= 0 {
		return Config{}, fmt.Errorf("RELAY_SEND_QUEUE_SIZE must be positive")
	}
	return cfg, nil
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnv(key string, def int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func durationEnv(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
