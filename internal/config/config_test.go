package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" || cfg.MaxMessageBytes != 1<<20 || cfg.SendQueueSize != 64 ||
		cfg.PingInterval != 25*time.Second || cfg.WriteTimeout != 10*time.Second ||
		cfg.ShutdownTimeout != 15*time.Second || cfg.TransferWait != 60*time.Second ||
		cfg.TransferIdleTimeout != 30*time.Second || cfg.MaxTransferBytes != 0 || cfg.MaxTransfers != 16 ||
		cfg.LogLevel != "info" || cfg.AllowedOrigins != nil {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("RELAY_TRANSFER_WAIT", "2m")
	t.Setenv("RELAY_TRANSFER_IDLE_TIMEOUT", "45s")
	t.Setenv("RELAY_MAX_TRANSFER_BYTES", "1073741824")
	t.Setenv("RELAY_MAX_TRANSFERS", "3")
	t.Setenv("RELAY_SEND_QUEUE_SIZE", "8")
	t.Setenv("RELAY_PING_INTERVAL", "0")
	t.Setenv("RELAY_ALLOWED_ORIGINS", " a.example , ,*.b.example")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TransferWait != 2*time.Minute || cfg.TransferIdleTimeout != 45*time.Second ||
		cfg.MaxTransferBytes != 1<<30 || cfg.MaxTransfers != 3 {
		t.Fatalf("unexpected transfer settings: %+v", cfg)
	}
	if cfg.SendQueueSize != 8 {
		t.Fatalf("SendQueueSize = %d", cfg.SendQueueSize)
	}
	if cfg.PingInterval != 0 {
		t.Fatalf("PingInterval = %v, want 0 (disabled)", cfg.PingInterval)
	}
	if len(cfg.AllowedOrigins) != 2 || cfg.AllowedOrigins[0] != "a.example" || cfg.AllowedOrigins[1] != "*.b.example" {
		t.Fatalf("AllowedOrigins = %q", cfg.AllowedOrigins)
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	cases := []struct{ key, value string }{
		{"RELAY_TRANSFER_WAIT", "0"},
		{"RELAY_TRANSFER_WAIT", "-1s"},
		{"RELAY_TRANSFER_WAIT", "soon"},
		{"RELAY_TRANSFER_IDLE_TIMEOUT", "0"},
		{"RELAY_TRANSFER_IDLE_TIMEOUT", "-1s"},
		{"RELAY_TRANSFER_IDLE_TIMEOUT", "later"},
		{"RELAY_MAX_TRANSFER_BYTES", "-1"},
		{"RELAY_MAX_TRANSFER_BYTES", "many"},
		{"RELAY_MAX_TRANSFER_BYTES", "9223372036854775808"},
		{"RELAY_MAX_TRANSFERS", "0"},
		{"RELAY_MAX_TRANSFERS", "-1"},
		{"RELAY_MAX_TRANSFERS", "many"},
		{"RELAY_MAX_TRANSFERS", "9223372036854775808"},
		{"RELAY_MAX_MESSAGE_BYTES", "0"},
		{"RELAY_MAX_MESSAGE_BYTES", "-1"},
		{"RELAY_MAX_MESSAGE_BYTES", "lots"},
		{"RELAY_SEND_QUEUE_SIZE", "0"},
		{"RELAY_SEND_QUEUE_SIZE", "-5"},
		{"RELAY_SEND_QUEUE_SIZE", "9223372036854775808"},
		{"RELAY_PING_INTERVAL", "-1s"},
		{"RELAY_PING_INTERVAL", "often"},
		{"RELAY_WRITE_TIMEOUT", "0"},
		{"RELAY_WRITE_TIMEOUT", "-1s"},
		{"RELAY_SHUTDOWN_TIMEOUT", "0s"},
		{"RELAY_SHUTDOWN_TIMEOUT", "-1s"},
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.value, func(t *testing.T) {
			t.Setenv(c.key, c.value)
			if cfg, err := Load(); err == nil {
				t.Fatalf("Load succeeded: %+v", cfg)
			}
		})
	}
}

// Ported from the Sparshur-Service master branch (bb9f96c, PR #1). Its
// TestLoadDefaults is covered by the stricter TestLoadDefaults above.
func TestLoadValidatesConnectionSettings(t *testing.T) {
	cases := []struct {
		key, value, wantErr string
	}{
		{"RELAY_PING_INTERVAL", "0", ""},
		{"RELAY_PING_INTERVAL", "-1s", "RELAY_PING_INTERVAL"},
		{"RELAY_WRITE_TIMEOUT", "0", "RELAY_WRITE_TIMEOUT"},
		{"RELAY_WRITE_TIMEOUT", "-1s", "RELAY_WRITE_TIMEOUT"},
		{"RELAY_SHUTDOWN_TIMEOUT", "0", "RELAY_SHUTDOWN_TIMEOUT"},
		{"RELAY_SHUTDOWN_TIMEOUT", "-1s", "RELAY_SHUTDOWN_TIMEOUT"},
		{"RELAY_SEND_QUEUE_SIZE", "0", "RELAY_SEND_QUEUE_SIZE"},
		{"RELAY_MAX_MESSAGE_BYTES", "0", "RELAY_MAX_MESSAGE_BYTES"},
	}
	for _, c := range cases {
		t.Run(c.key+"="+c.value, func(t *testing.T) {
			t.Setenv(c.key, c.value)
			_, err := Load()
			switch {
			case c.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
				t.Fatalf("err = %v, want mention of %s", err, c.wantErr)
			}
		})
	}
}
