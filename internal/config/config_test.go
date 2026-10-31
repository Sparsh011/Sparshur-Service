package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" || cfg.MaxMessageBytes != 1<<20 || cfg.SendQueueSize != 64 ||
		cfg.PingInterval != 25*time.Second || cfg.WriteTimeout != 10*time.Second ||
		cfg.ShutdownTimeout != 15*time.Second || cfg.LogLevel != "info" || cfg.AllowedOrigins != nil {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	t.Setenv("RELAY_SEND_QUEUE_SIZE", "8")
	t.Setenv("RELAY_PING_INTERVAL", "0")
	t.Setenv("RELAY_ALLOWED_ORIGINS", " a.example , ,*.b.example")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
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
