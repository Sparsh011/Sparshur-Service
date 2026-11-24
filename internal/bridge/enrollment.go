package bridge

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"unicode/utf8"
)

type Device struct {
	ID        string  `json:"device_id"`
	PublicKey string  `json:"public_key"`
	Label     *string `json:"label"`
}

type Pairing struct {
	ID      string   `json:"pairing_id"`
	Devices []string `json:"devices"`
}

type Enrollment struct {
	Devices  []Device  `json:"devices"`
	Pairings []Pairing `json:"pairings"`
}

type registry struct {
	devices  map[string]ed25519.PublicKey
	pairings map[string][2]string
}

func (e Enrollment) validate() (registry, error) {
	r := registry{make(map[string]ed25519.PublicKey), make(map[string][2]string)}
	if e.Devices == nil || e.Pairings == nil {
		return r, errors.New("enrollment requires devices and pairings arrays")
	}
	for i, d := range e.Devices {
		key, err := base64.StdEncoding.Strict().DecodeString(d.PublicKey)
		if !ValidUUID(d.ID) || d.Label == nil || err != nil || len(key) != ed25519.PublicKeySize ||
			base64.StdEncoding.EncodeToString(key) != d.PublicKey || r.devices[d.ID] != nil {
			return r, fmt.Errorf("invalid or duplicate device at index %d", i)
		}
		r.devices[d.ID] = key
	}
	for i, p := range e.Pairings {
		_, exists := r.pairings[p.ID]
		if !ValidUUID(p.ID) || exists || len(p.Devices) != 2 || p.Devices[0] == p.Devices[1] ||
			r.devices[p.Devices[0]] == nil || r.devices[p.Devices[1]] == nil {
			return r, fmt.Errorf("invalid or duplicate pairing at index %d", i)
		}
		r.pairings[p.ID] = [2]string{p.Devices[0], p.Devices[1]}
	}
	return r, nil
}

var errEnrollmentJSON = errors.New("invalid enrollment JSON")

func ReadEnrollment(path string) (Enrollment, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Enrollment{}, err
	}
	if !utf8.Valid(data) {
		return Enrollment{}, errEnrollmentJSON
	}
	var e Enrollment
	err = decodeObject(data, map[string]any{"devices": &e.Devices, "pairings": &e.Pairings})
	if err != nil {
		return Enrollment{}, err
	}
	_, err = e.validate()
	return e, err
}

func (d *Device) UnmarshalJSON(data []byte) error {
	return decodeObject(data, map[string]any{"device_id": &d.ID, "public_key": &d.PublicKey, "label": &d.Label})
}

func (p *Pairing) UnmarshalJSON(data []byte) error {
	return decodeObject(data, map[string]any{"pairing_id": &p.ID, "devices": &p.Devices})
}

// The ordinary struct decoder accepts repeated and case-insensitive keys.
// Enrollment requires each field to appear once with its exact spelling.
func decodeObject(data []byte, fields map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if token, err := decoder.Token(); err != nil || token != json.Delim('{') {
		return errEnrollmentJSON
	}
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok || fields[key] == nil {
			return errEnrollmentJSON
		}
		if err := decoder.Decode(fields[key]); err != nil {
			return errEnrollmentJSON
		}
		delete(fields, key)
	}
	if token, err := decoder.Token(); err != nil || token != json.Delim('}') || len(fields) != 0 {
		return errEnrollmentJSON
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errEnrollmentJSON
	}
	return nil
}

func WriteEnrollment(path string, e Enrollment) error {
	if _, err := e.validate(); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".relay-enrollment-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(e); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Rename(file.Name(), path)
}
