// Command relayctl edits the authenticated relay's enrollment file locally.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"syscall"

	"github.com/sparshchadha/sparshur-service/internal/bridge"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	global := flag.NewFlagSet("relayctl", flag.ContinueOnError)
	path := global.String("file", os.Getenv("RELAY_ENROLLMENT_FILE"), "enrollment JSON path")
	if err := global.Parse(args); err != nil {
		return err
	}
	if *path == "" || global.NArg() == 0 {
		return errors.New("usage: relayctl -file PATH {add-device|remove-device|add-pairing|remove-pairing|list} [flags]")
	}
	command := global.Arg(0)
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	var id, key, label, a, b string
	switch command {
	case "add-device":
		flags.StringVar(&key, "key", "", "Base64 Ed25519 public key")
		flags.StringVar(&label, "label", "", "device label")
	case "add-pairing":
		flags.StringVar(&a, "a", "", "first enrolled device ID")
		flags.StringVar(&b, "b", "", "second enrolled device ID")
	case "remove-device", "remove-pairing", "list":
	default:
		return errors.New("unknown enrollment command")
	}
	if command != "list" {
		flags.StringVar(&id, "id", "", "canonical UUID")
	}
	if err := flags.Parse(global.Args()[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || (command != "list" && !bridge.ValidUUID(id)) {
		return errors.New("expected command flags and a canonical UUID for -id")
	}
	if command != "list" {
		// Serialize the entire read/edit/rename sequence across relayctl commands.
		lock, err := os.OpenFile(*path+".lock", os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		defer lock.Close()
		if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
			return err
		}
	}
	e, err := bridge.ReadEnrollment(*path)
	if command == "add-device" && errors.Is(err, os.ErrNotExist) {
		e = bridge.Enrollment{Devices: []bridge.Device{}, Pairings: []bridge.Pairing{}}
	} else if err != nil {
		return err
	}
	switch command {
	case "add-device":
		e.Devices = append(e.Devices, bridge.Device{ID: id, PublicKey: key, Label: &label})
	case "remove-device":
		n := len(e.Devices)
		e.Devices = slices.DeleteFunc(e.Devices, func(d bridge.Device) bool { return d.ID == id })
		if len(e.Devices) == n {
			return errors.New("device not found")
		}
		e.Pairings = slices.DeleteFunc(e.Pairings, func(p bridge.Pairing) bool { return slices.Contains(p.Devices, id) })
	case "add-pairing":
		e.Pairings = append(e.Pairings, bridge.Pairing{ID: id, Devices: []string{a, b}})
	case "remove-pairing":
		n := len(e.Pairings)
		e.Pairings = slices.DeleteFunc(e.Pairings, func(p bridge.Pairing) bool { return p.ID == id })
		if len(e.Pairings) == n {
			return errors.New("pairing not found")
		}
	case "list":
		for _, d := range e.Devices {
			fmt.Fprintln(out, "device", d.ID, strconv.Quote(*d.Label))
		}
		for _, p := range e.Pairings {
			fmt.Fprintln(out, "pairing", p.ID, p.Devices[0], p.Devices[1])
		}
		return nil
	}
	return bridge.WriteEnrollment(*path, e)
}
