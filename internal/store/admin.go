package store

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

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

// Admin changes reach disk before they become visible in memory. Reports
// continue updating the current snapshot while the disk write is in progress.
// Only the admin delta is committed; concurrent radio updates are preserved
// and stay dirty for the next save.
func (s *Store) mutate(fn func(*Disk) error) error {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	s.mu.Lock()
	before := clone(s.disk)
	next := clone(before)
	if err := fn(&next); err != nil {
		s.mu.Unlock()
		return err
	}
	for id, d := range next.Devices {
		if err := validate(d); err != nil {
			s.mu.Unlock()
			return err
		}
		for otherID, other := range next.Devices {
			if id != otherID && (d.Name == other.Name || (uniqueNetwork(d.Network) && d.Network == other.Network) || d.Name == other.IEEE) {
				s.mu.Unlock()
				return fmt.Errorf("duplicate friendly name or network address")
			}
		}
	}
	b, err := json.MarshalIndent(next, "", "  ")
	if err != nil {
		s.mu.Unlock()
		return err
	}
	// Prevent a just-joining device from stealing a friendly name while the
	// pending rename/replacement is persisted.
	s.reservedNames = map[string]string{}
	for id, d := range next.Devices {
		s.reservedNames[d.Name] = id
	}
	wasDirty := s.dirty
	s.dirty = false
	s.mu.Unlock()
	err = s.persist(s.path, b)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reservedNames = nil
	if err != nil {
		s.dirty = s.dirty || wasDirty
		return err
	}
	for id, old := range before.Devices {
		changed, exists := next.Devices[id]
		if !exists {
			s.markAddress(id)
			delete(s.disk.Devices, id)
			delete(s.addressVersions, id)
			delete(s.disk.States, id)
			continue
		}
		current, exists := s.disk.Devices[id]
		if !exists {
			continue
		}
		if changed.Name != old.Name {
			current.Name = changed.Name
		}
		if !reflect.DeepEqual(changed.Options, old.Options) {
			current.Options = clone(changed.Options)
		}
		s.disk.Devices[id] = current
	}
	for id, state := range before.States {
		if _, exists := next.States[id]; !exists && reflect.DeepEqual(state, s.disk.States[id]) {
			delete(s.disk.States, id)
		}
	}
	s.disk.RetainedCleanup = next.RetainedCleanup
	s.lastSave = time.Now()
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
		if d.Name != name {
			addCleanup(next, d.Name)
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
		addCleanup(next, next.Devices[id].Name)
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
		addCleanup(next, old.Name, new.Name)
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
	s.disk.Devices[d.IEEE] = cloneDevice(d)
	s.dirty = true
	return nil
}

// Touch changes only liveness; protocol replies do not replace device metadata.
func (s *Store) Touch(id string, network uint16, seen time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.disk.Devices[id]
	if !ok || d.Network != network {
		return fmt.Errorf("device removed or address changed")
	}
	if seen.After(d.LastSeen) {
		d.LastSeen = seen
		s.disk.Devices[id] = d
		s.dirty = true
	}
	return nil
}

// Descriptor discovery commits only descriptors, preserving live metadata and
// measurement scales learned while the radio interview was in progress.
func (s *Store) SetEndpoints(id string, network uint16, endpoints []Endpoint) (Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.disk.Devices[id]
	if !ok || d.Network != network {
		return Device{}, fmt.Errorf("device removed or address changed")
	}
	next := cloneDevice(d)
	next.Endpoints = cloneDevice(Device{Endpoints: endpoints}).Endpoints
	for i := range next.Endpoints {
		for _, old := range d.Endpoints {
			if old.ID == next.Endpoints[i].ID {
				next.Endpoints[i].Scales = clone(old.Scales)
			}
		}
	}
	if err := validate(next); err != nil {
		return Device{}, err
	}
	s.disk.Devices[id] = next
	s.dirty = true
	return cloneDevice(next), nil
}

// Reports update identity metadata and scales, never stale descriptor lists.
func (s *Store) RefreshReport(report Device) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.disk.Devices[report.IEEE]
	if !ok || d.Network != report.Network {
		return fmt.Errorf("device removed or address changed")
	}
	d = cloneDevice(d)
	if report.LastSeen.After(d.LastSeen) {
		d.LastSeen = report.LastSeen
	}
	d.Model, d.Manufacturer = report.Model, report.Manufacturer
	for i := range d.Endpoints {
		for _, ep := range report.Endpoints {
			if ep.ID != d.Endpoints[i].ID {
				continue
			}
			if len(ep.Scales) > 0 && d.Endpoints[i].Scales == nil {
				d.Endpoints[i].Scales = map[string]float64{}
			}
			for key, value := range ep.Scales {
				d.Endpoints[i].Scales[key] = value
			}
		}
	}
	if err := validate(d); err != nil {
		return err
	}
	s.disk.Devices[d.IEEE] = d
	s.dirty = true
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

// Obsolete retained topics survive a process restart/MQTT outage. Cleanup is
// acknowledged only after broker PUBACKs, then persisted with the admin delta.
func addCleanup(d *Disk, names ...string) {
	for _, name := range names {
		found := false
		for _, old := range d.RetainedCleanup {
			if name == old {
				found = true
				break
			}
		}
		if !found {
			d.RetainedCleanup = append(d.RetainedCleanup, name)
		}
	}
}
func (s *Store) CleanupTopics() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]string(nil), s.disk.RetainedCleanup...)
}
func (s *Store) AcknowledgeCleanup(names []string) error {
	return s.mutate(func(next *Disk) error {
		cleared := map[string]bool{}
		for _, name := range names {
			cleared[name] = true
		}
		pending := []string{}
		for _, name := range next.RetainedCleanup {
			if !cleared[name] {
				pending = append(pending, name)
			}
		}
		next.RetainedCleanup = pending
		return nil
	})
}
