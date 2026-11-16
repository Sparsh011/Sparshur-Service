package config

import (
	"strings"
	"testing"
)

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

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PingInterval <= 0 || cfg.WriteTimeout <= 0 || cfg.ShutdownTimeout <= 0 {
		t.Fatalf("bad defaults: %+v", cfg)
	}
}
