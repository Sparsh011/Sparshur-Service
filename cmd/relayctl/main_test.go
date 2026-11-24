package main

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sparshchadha/sparshur-service/internal/bridge"
)

func TestOfflineEnrollmentCommands(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enrollment.json")
	var output bytes.Buffer
	command := func(args ...string) error { return run(append([]string{"-file", path}, args...), &output) }
	runOK := func(args ...string) {
		t.Helper()
		if err := command(args...); err != nil {
			t.Fatal(err)
		}
	}
	const a = "33333333-3333-4333-8333-333333333333"
	const b = "22222222-2222-4222-8222-222222222222"
	const pair = "55555555-5555-4555-8555-555555555555"
	for _, id := range []string{a, b} {
		key, _, err := ed25519.GenerateKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		runOK("add-device", "-id", id, "-key", base64.StdEncoding.EncodeToString(key), "-label", "test")
	}
	if err := command("add-pairing", "-id", pair, "-a", a, "-b", a); err == nil {
		t.Fatal("accepted a self-pairing")
	}
	runOK("add-pairing", "-id", pair, "-a", a, "-b", b)
	runOK("list")
	if !strings.Contains(output.String(), "pairing "+pair+" "+a+" "+b) {
		t.Fatal("list omitted the pairing")
	}
	runOK("remove-pairing", "-id", pair)
	runOK("add-pairing", "-id", pair, "-a", a, "-b", b)
	runOK("remove-device", "-id", b)
	enrollment, err := bridge.ReadEnrollment(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(enrollment.Pairings) != 0 || len(enrollment.Devices) != 1 || enrollment.Devices[0].ID != a {
		t.Fatal("device removal did not cascade to its pairing")
	}
}
