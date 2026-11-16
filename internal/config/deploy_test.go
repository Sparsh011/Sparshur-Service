package config

import (
	"testing"
	"time"
)

var relayEnv = []string{
	"RELAY_ADDR", "RELAY_MAX_MESSAGE_BYTES", "RELAY_SEND_QUEUE_SIZE",
	"RELAY_PING_INTERVAL", "RELAY_WRITE_TIMEOUT", "RELAY_SHUTDOWN_TIMEOUT",
	"RELAY_ALLOWED_ORIGINS", "RELAY_LOG_LEVEL",
	"RELAY_TRANSFER_WAIT", "RELAY_TRANSFER_IDLE_TIMEOUT", "RELAY_MAX_TRANSFER_BYTES", "RELAY_MAX_TRANSFERS",
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range relayEnv {
		t.Setenv(k, "")
	}
}

// Deployment docs and unit files rely on these defaults staying put.
func TestLoadDeploymentDefaults(t *testing.T) {
	clearEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != ":8080" {
		t.Errorf("Addr = %q, want :8080", cfg.Addr)
	}
	if cfg.MaxMessageBytes != 1<<20 || cfg.SendQueueSize != 64 {
		t.Errorf("limits = %d/%d, want 1048576/64", cfg.MaxMessageBytes, cfg.SendQueueSize)
	}
	if cfg.PingInterval != 25*time.Second || cfg.WriteTimeout != 10*time.Second || cfg.ShutdownTimeout != 15*time.Second {
		t.Errorf("timers = %v/%v/%v", cfg.PingInterval, cfg.WriteTimeout, cfg.ShutdownTimeout)
	}
	if len(cfg.AllowedOrigins) != 0 || cfg.LogLevel != "info" {
		t.Errorf("origins/log level = %v/%q", cfg.AllowedOrigins, cfg.LogLevel)
	}
}

// RELAY_ADDR is passed to the listener untouched, so a private deployment
// can pin the relay to loopback or a VPN interface address.
func TestLoadAddrOverride(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:8080", "100.101.102.103:8080", "[::1]:9000"} {
		clearEnv(t)
		t.Setenv("RELAY_ADDR", addr)
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Addr != addr {
			t.Errorf("Addr = %q, want %q", cfg.Addr, addr)
		}
	}
}
