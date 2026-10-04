package store

import (
	"zigbeemqttlink/internal/config"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Endpoint struct {
	ID       uint8              `json:"ID"`
	Profile  uint16             `json:"profileID"`
	DeviceID uint16             `json:"deviceID"`
	In       []uint16           `json:"inputClusters"`
	Out      []uint16           `json:"outputClusters"`
	Scales   map[string]float64 `json:"measurement_scales,omitempty"`
}
type Device struct {
	IEEE         string             `json:"ieee_address"`
	Network      uint16             `json:"network_address"`
	Type         string             `json:"type"`
	Name         string             `json:"friendly_name"`
	Model        string             `json:"model_id,omitempty"`
	Manufacturer string             `json:"manufacturer,omitempty"`
	Endpoints    []Endpoint         `json:"endpoints"`
	LastSeen     time.Time          `json:"last_seen,omitempty"`
	Options      map[string]float64 `json:"options,omitempty"`
}
type Disk struct {
	CoordinatorIEEE string                    `json:"coordinator_ieee"`
	Version         int                       `json:"version"`
	Devices         map[string]Device         `json:"devices"`
	States          map[string]map[string]any `json:"states"`
}
type Store struct {
	mu     sync.RWMutex
	saveMu sync.Mutex
	path   string
	disk   Disk
}

func New(path string) (*Store, error) {
	s := &Store{path: path, disk: Disk{Version: 1, Devices: map[string]Device{}, States: map[string]map[string]any{}}}
	b, e := os.ReadFile(s.path)
	if os.IsNotExist(e) {
		return s, nil
	}
	if e != nil {
		return nil, e
	}
	if e = config.StrictJSON(b, &s.disk); e != nil {
		return nil, fmt.Errorf("state is corrupt (not overwritten): %w", e)
	}
	if s.disk.Version != 1 || s.disk.Devices == nil || s.disk.States == nil {
		return nil, fmt.Errorf("unsupported or incomplete state")
	}
	seenNames := map[string]bool{}
	seenNetworks := map[uint16]bool{}
	for id, d := range s.disk.Devices {
		if id != d.IEEE {
			return nil, fmt.Errorf("state IEEE key mismatch")
		}
		if e = validate(d); e != nil {
			return nil, e
		}
		if seenNames[d.Name] || seenNetworks[d.Network] {
			return nil, fmt.Errorf("duplicate name or network address in database")
		}
		seenNames[d.Name] = true
		seenNetworks[d.Network] = true
		if _, exists := s.disk.Devices[d.Name]; exists && d.Name != d.IEEE {
			return nil, fmt.Errorf("name conflicts with another IEEE address")
		}
	}
	return s, nil
}
func validate(d Device) error {
	if e := ValidateOptions(d.Options); e != nil {
		return e
	}
	if d.Type != "Router" && d.Type != "EndDevice" {
		return fmt.Errorf("unsupported device type %q", d.Type)
	}
	if !config.IEEE(d.IEEE) || !config.Name(d.Name) || d.Network == 0 || d.Network >= 0xfff8 {
		return fmt.Errorf("invalid device identity/address: %q", d.IEEE)
	}
	seenEP := map[uint8]bool{}
	for _, ep := range d.Endpoints {
		for key, v := range ep.Scales {
			if !MeasurementScaleKeys[key] || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v > 0xffffffff {
				return fmt.Errorf("invalid measurement scale %q", key)
			}
		}
		if ep.ID == 0 || ep.ID > 240 || seenEP[ep.ID] {
			return fmt.Errorf("invalid endpoint %d", ep.ID)
		}
		seenEP[ep.ID] = true
	}
	return nil
}
func (s *Store) Put(d Device) error {
	if e := validate(d); e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, old := range s.disk.Devices {
		if id != d.IEEE && (old.Name == d.Name || old.Network == d.Network || old.IEEE == d.Name || old.Name == d.IEEE) {
			return fmt.Errorf("duplicate friendly name or network address")
		}
	}
	s.disk.Devices[d.IEEE] = clone(d)
	return nil
}
func clone[T any](v T) T {
	b, _ := json.Marshal(v)
	var result T
	_ = json.Unmarshal(b, &result)
	return result
}
func (s *Store) Devices() []Device {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Device, 0, len(s.disk.Devices))
	for _, d := range s.disk.Devices {
		out = append(out, clone(d))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IEEE < out[j].IEEE })
	return out
}
func (s *Store) Find(id string) (Device, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.disk.Devices {
		if d.IEEE == id || d.Name == id {
			return clone(d), true
		}
	}
	return Device{}, false
}

// Radio identities must never be resolved through a friendly-name alias. A
// replacement may legitimately keep its predecessor's IEEE string as a name.
func (s *Store) ByIEEE(id string) (Device, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.disk.Devices[id]
	return clone(d), ok
}
func (s *Store) ByNetwork(n uint16) (Device, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.disk.Devices {
		if d.Network == n {
			return clone(d), true
		}
	}
	return Device{}, false
}
func (s *Store) Update(id string, update map[string]any) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.disk.Devices[id]; !ok {
		return nil
	}
	v := clone(s.disk.States[id])
	if v == nil {
		v = map[string]any{}
	}
	for k, x := range update {
		v[k] = clone(x)
	}
	cached := clone(v)
	delete(cached, "action")
	s.disk.States[id] = cached
	return v
}
func (s *Store) State(id string) map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return clone(s.disk.States[id])
}
func (s *Store) Save() error {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	s.mu.RLock()
	b, e := json.MarshalIndent(s.disk, "", "  ")
	s.mu.RUnlock()
	if e != nil {
		return e
	}
	return AtomicWrite(s.path, b)
}

// AtomicWrite keeps the previous complete file until the new bytes have been synced.
func AtomicWrite(path string, b []byte) error {
	dir := filepath.Dir(path)
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(dir, ".state-*")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, e = f.Write(b); e != nil {
		f.Close()
		return e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(tmp, path); e != nil {
		return e
	}
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}

func (s *Store) Coordinator() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.disk.CoordinatorIEEE
}
func (s *Store) SetCoordinator(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.disk.CoordinatorIEEE = id
}
