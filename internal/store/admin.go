package store

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"

	"zigbeemqttlink/internal/config"
)

var CalibrationProperties = map[string]bool{"temperature": true, "device_temperature": true, "humidity": true, "pressure": true, "illuminance": true, "power": true, "energy": true, "voltage": true, "current": true}

func ValidateOptions(options map[string]float64) error {
	for key, v := range options {
		name := strings.TrimSuffix(key, "_calibration")
		if name == key || !CalibrationProperties[name] || math.IsNaN(v) || math.IsInf(v, 0) || v < -10000 || v > 10000 {
			return fmt.Errorf("unsupported calibration %q or invalid value", key)
		}
	}
	return nil
}

// Admin mutations are persisted before exposing the new in-memory snapshot.
// saveMu prevents an earlier periodic snapshot from overwriting the mutation.
func (s *Store) mutate(fn func(*Disk) error) error {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	next := clone(s.disk)
	if err := fn(&next); err != nil {
		return err
	}
	for id, d := range next.Devices {
		if err := validate(d); err != nil {
			return err
		}
		for otherID, other := range next.Devices {
			if id != otherID && (d.Name == other.Name || d.Network == other.Network || d.Name == other.IEEE) {
				return fmt.Errorf("duplicate friendly name or network address")
			}
		}
	}
	b, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		return err
	}
	if err = AtomicWrite(s.path, b); err != nil {
		return err
	}
	s.disk = next
	return nil
}

func (s *Store) Rename(id, name string) (Device, error) {
	if !config.Name(name) {
		return Device{}, fmt.Errorf("invalid friendly name")
	}
	var changed Device
	err := s.mutate(func(next *Disk) error {
		d, ok := next.Devices[id]
		if !ok {
			return fmt.Errorf("device no longer exists")
		}
		d.Name = name
		next.Devices[id] = d
		changed = d
		return nil
	})
	return clone(changed), err
}

func (s *Store) SetOptions(id string, options map[string]float64) error {
	if err := ValidateOptions(options); err != nil {
		return err
	}
	return s.mutate(func(next *Disk) error {
		d, ok := next.Devices[id]
		if !ok {
			return fmt.Errorf("device no longer exists")
		}
		d.Options = clone(options)
		next.Devices[id] = d
		return nil
	})
}

func (s *Store) Remove(id string) error {
	return s.mutate(func(next *Disk) error {
		if _, ok := next.Devices[id]; !ok {
			return fmt.Errorf("device no longer exists")
		}
		delete(next.Devices, id)
		delete(next.States, id)
		return nil
	})
}

func (s *Store) Replace(oldID, newID string) (Device, error) {
	var changed Device
	err := s.mutate(func(next *Disk) error {
		old, ok := next.Devices[oldID]
		if !ok {
			return fmt.Errorf("old device no longer exists")
		}
		new, ok := next.Devices[newID]
		if !ok || oldID == newID {
			return fmt.Errorf("select a different paired replacement")
		}
		if old.Model == "" || old.Model != new.Model {
			return fmt.Errorf("replacement requires the same known model_id")
		}
		if (old.Model == "TS0601" || old.Model == "TS011F") && (old.Manufacturer == "" || old.Manufacturer != new.Manufacturer) {
			return fmt.Errorf("Tuya replacement requires the same known manufacturer fingerprint as well as model_id")
		}
		new.Name = old.Name
		new.Options = clone(old.Options)
		delete(next.Devices, oldID)
		delete(next.States, oldID)
		next.Devices[newID] = new
		changed = new
		// New hardware retains only its own fresh state. Do not claim that the
		// old relay state or settings have physically transferred to it.
		return nil
	})
	return clone(changed), err
}

// Existing report/interview writers must not resurrect an administratively
// removed device or overwrite a concurrent rename/options edit.
func (s *Store) Refresh(d Device) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.disk.Devices[d.IEEE]
	if !ok {
		return fmt.Errorf("device was removed")
	}
	if current.Network != d.Network {
		return fmt.Errorf("device network address changed during update")
	}
	d.Name = current.Name
	d.Options = current.Options
	if err := validate(d); err != nil {
		return err
	}
	s.disk.Devices[d.IEEE] = clone(d)
	return nil
}

func ApplyOptions(d Device, state map[string]any) {
	for key, offset := range d.Options {
		name := strings.TrimSuffix(key, "_calibration")
		v, ok := state[name].(float64)
		if !ok {
			continue
		}
		if name == "temperature" || name == "device_temperature" || name == "humidity" || name == "pressure" {
			state[name] = v + offset
		} else {
			state[name] = v * (1 + offset/100)
		}
	}
}
