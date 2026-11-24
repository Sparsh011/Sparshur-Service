package bridge

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestEnrollmentValidationAndAtomicWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "enrollment.json")
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	device := fmt.Sprintf(`{"device_id":%q,"label":"phone","public_key":%q}`, phoneID, key)
	mac := fmt.Sprintf(`{"device_id":%q,"label":"mac","public_key":%q}`, macID, key)
	valid := fmt.Sprintf(`{"devices":[%s,%s],"pairings":[{"pairing_id":%q,"devices":[%q,%q]}]}`, device, mac, pairingID, phoneID, macID)
	for _, bad := range []string{
		`{}`, `{"devices":null,"pairings":[]}`, `{"devices":[],"pairings":[],"extra":0}`,
		`{"devices":[],"devices":[],"pairings":[]}`, `{"Devices":[],"pairings":[]}`,
		fmt.Sprintf(`{"devices":[{"device_id":%q,"public_key":%q,"label":"one","label":"two"}],"pairings":[]}`, macID, key),
		valid + "{}", fmt.Sprintf(`{"devices":[%s,%s],"pairings":[]}`, device, device),
		fmt.Sprintf(`{"devices":[%s],"pairings":[{"pairing_id":%q,"devices":[%q,%q]}]}`, device, pairingID, phoneID, macID),
		fmt.Sprintf(`{"devices":[%s],"pairings":[{"pairing_id":%q,"devices":[%q,%q]}]}`, device, pairingID, phoneID, phoneID),
		fmt.Sprintf(`{"devices":[{"device_id":%q,"public_key":"AA==","label":"bad"}],"pairings":[]}`, macID),
	} {
		if err := os.WriteFile(path, []byte(bad), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadEnrollment(path); err == nil {
			t.Fatal("accepted invalid enrollment")
		}
	}
	if err := os.WriteFile(path, []byte(valid), 0644); err != nil {
		t.Fatal(err)
	}
	e, err := ReadEnrollment(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteEnrollment(path, e); err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(path)
	if err != nil || stat.Mode().Perm() != 0600 {
		t.Fatalf("expected mode 0600, got %v (%v)", stat, err)
	}
	e.Devices = nil
	if err := WriteEnrollment(path, e); err == nil {
		t.Fatal("replaced file with invalid enrollment")
	}
	if _, err := ReadEnrollment(path); err != nil {
		t.Fatalf("valid enrollment lost: %v", err)
	}
}
