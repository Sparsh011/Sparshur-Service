package config

import (
	"strings"
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

func TestBridgeConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name, url, id, enrollment string
		valid                     bool
	}{
		{"disabled", "", "", "", true},
		{"enabled", "wss://relay.example/v1/relay", "mini", "/etc/relay.json", true},
		{"explicit port", "wss://[::1]:443/v1/relay", "mini", "relay.json", true},
		{"partial", "wss://relay.example/v1/relay", "", "relay.json", false},
		{"uppercase host", "wss://Relay.example/v1/relay", "mini", "relay.json", false},
		{"plaintext", "ws://relay.example/v1/relay", "mini", "relay.json", false},
		{"userinfo", "wss://@relay.example/v1/relay", "mini", "relay.json", false},
		{"query", "wss://relay.example/v1/relay?", "mini", "relay.json", false},
		{"fragment", "wss://relay.example/v1/relay#", "mini", "relay.json", false},
		{"escaped path", "wss://relay.example/v1/%72elay", "mini", "relay.json", false},
		{"wrong path", "wss://relay.example/v1/relay/", "mini", "relay.json", false},
		{"URL delimiter", "wss://rel|ay.example/v1/relay", "mini", "relay.json", false},
		{"URL length", "wss://" + strings.Repeat("a", 2048) + "/v1/relay", "mini", "relay.json", false},
		{"bad port", "wss://relay.example:0/v1/relay", "mini", "relay.json", false},
		{"ID delimiter", "wss://relay.example/v1/relay", "mini|test", "relay.json", false},
		{"ID whitespace", "wss://relay.example/v1/relay", " mini", "relay.json", false},
		{"ID length", "wss://relay.example/v1/relay", strings.Repeat("a", 129), "relay.json", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv("RELAY_PUBLIC_URL", tc.url)
			t.Setenv("RELAY_ID", tc.id)
			t.Setenv("RELAY_ENROLLMENT_FILE", tc.enrollment)
			got, err := Load()
			if (err == nil) != tc.valid {
				t.Fatalf("Load returned %v, expected validity %v", err, tc.valid)
			}
			if tc.valid && (got.RelayPublicURL != tc.url || got.RelayID != tc.id || got.RelayEnrollmentFile != tc.enrollment) {
				t.Fatal("configuration did not preserve exact values")
			}
		})
	}
}
