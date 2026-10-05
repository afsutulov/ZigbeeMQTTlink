package store

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
	"zigbeemqttlink/internal/config"
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
	RetainedCleanup []string                  `json:"retained_cleanup,omitempty"`
	Version         int                       `json:"version"`
	Devices         map[string]Device         `json:"devices"`
	States          map[string]map[string]any `json:"states"`
}
type Store struct {
	mu              sync.RWMutex
	saveMu          sync.Mutex
	path            string
	persist         func(string, []byte) error
	disk            Disk
	addressClock    uint64            // monotonic across address changes and delete/re-add
	addressVersions map[string]uint64 // guarded by mu; not persisted
	reservedNames   map[string]string // names reserved by a pending admin commit
	dirty           bool              // guarded by mu: in-memory data differs from the file
	lastSave        time.Time         // guarded by saveMu
}

// UnknownNetwork is the Zigbee "unknown short address" value. A device gets it
// when its old short address is announced by another device.
const UnknownNetwork uint16 = 0xfffe

func uniqueNetwork(n uint16) bool { return n != UnknownNetwork }

func New(path string) (*Store, error) {
	s := &Store{addressVersions: map[string]uint64{}, path: path, persist: AtomicWrite, disk: Disk{Version: 1, Devices: map[string]Device{}, States: map[string]map[string]any{}}}
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
	if s.disk.CoordinatorIEEE != "" && !config.IEEE(s.disk.CoordinatorIEEE) {
		return nil, fmt.Errorf("invalid coordinator IEEE in database")
	}
	for _, name := range s.disk.RetainedCleanup {
		if !config.Name(name) {
			return nil, fmt.Errorf("invalid retained cleanup topic")
		}
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
		if seenNames[d.Name] || (uniqueNetwork(d.Network) && seenNetworks[d.Network]) {
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
	if !config.IEEE(d.IEEE) || !config.Name(d.Name) || d.Network == 0 || (d.Network >= 0xfff8 && d.Network != UnknownNetwork) {
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
	if id, reserved := s.reservedNames[d.Name]; reserved && id != d.IEEE {
		return fmt.Errorf("name reserved by an administrative change")
	}
	for id, old := range s.disk.Devices {
		if id != d.IEEE && (old.Name == d.Name || (uniqueNetwork(d.Network) && old.Network == d.Network) || old.IEEE == d.Name || old.Name == d.IEEE) {
			return fmt.Errorf("duplicate friendly name or network address")
		}
	}
	s.disk.Devices[d.IEEE] = cloneDevice(d)
	s.markAddress(d.IEEE)
	s.dirty = true
	return nil
}

// Announce records a device announce. Zigbee short addresses can be reused or
// re-assigned after a conflict, so the announce is authoritative: another
// entry still holding the same address is marked UnknownNetwork instead of
// rejecting the (re)joining device.
func (s *Store) Announce(d Device) ([]string, error) {
	return s.recordAnnounce(d, true)
}

// AnnounceKnown prevents a concurrent administrative removal from being undone.
func (s *Store) AnnounceKnown(d Device) ([]string, error) {
	return s.recordAnnounce(d, false)
}

func (s *Store) recordAnnounce(d Device, allowNew bool) ([]string, error) {
	if d.Network == 0 || d.Network >= 0xfff8 {
		return nil, fmt.Errorf("invalid announced network address")
	}
	if e := validate(d); e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.disk.Devices[d.IEEE]; !exists && !allowNew {
		return nil, fmt.Errorf("device was removed before announce commit")
	}
	if current, exists := s.disk.Devices[d.IEEE]; exists {
		// The caller's copy may predate a concurrent rename, options edit,
		// interview or report. An announce changes only address and liveness.
		current = cloneDevice(current)
		current.Network = d.Network
		if d.LastSeen.After(current.LastSeen) {
			current.LastSeen = d.LastSeen
		}
		d = current
		if e := validate(d); e != nil {
			return nil, e
		}
	}
	if id, reserved := s.reservedNames[d.Name]; reserved && id != d.IEEE {
		return nil, fmt.Errorf("name reserved by an administrative change")
	}
	for id, old := range s.disk.Devices {
		if id != d.IEEE && (old.Name == d.Name || old.IEEE == d.Name || old.Name == d.IEEE) {
			return nil, fmt.Errorf("duplicate friendly name")
		}
	}
	var displaced []string
	for id, old := range s.disk.Devices {
		if id != d.IEEE && old.Network == d.Network {
			old.Network = UnknownNetwork
			s.disk.Devices[id] = old
			s.markAddress(id)
			displaced = append(displaced, old.Name)
		}
	}
	s.disk.Devices[d.IEEE] = cloneDevice(d)
	s.markAddress(d.IEEE)
	s.dirty = true
	return displaced, nil
}

// cloneDevice is a typed deep copy; it replaces a JSON round trip on the hot
// path (every incoming frame looks devices up).
func cloneDevice(d Device) Device {
	if d.Endpoints != nil {
		eps := make([]Endpoint, len(d.Endpoints))
		for i, e := range d.Endpoints {
			e.In = append([]uint16(nil), e.In...)
			e.Out = append([]uint16(nil), e.Out...)
			if e.Scales != nil {
				m := make(map[string]float64, len(e.Scales))
				for k, v := range e.Scales {
					m[k] = v
				}
				e.Scales = m
			}
			eps[i] = e
		}
		d.Endpoints = eps
	}
	if d.Options != nil {
		m := make(map[string]float64, len(d.Options))
		for k, v := range d.Options {
			m[k] = v
		}
		d.Options = m
	}
	return d
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
		out = append(out, cloneDevice(d))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].IEEE < out[j].IEEE })
	return out
}
func (s *Store) Find(id string) (Device, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.disk.Devices {
		if d.IEEE == id || d.Name == id {
			return cloneDevice(d), true
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
	return cloneDevice(d), ok
}
func (s *Store) ByNetwork(n uint16) (Device, bool) {
	if !uniqueNetwork(n) {
		return Device{}, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, d := range s.disk.Devices {
		if d.Network == n {
			return cloneDevice(d), true
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
	s.dirty = true
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
	s.mu.Lock()
	b, e := json.MarshalIndent(s.disk, "", "  ")
	wasDirty := s.dirty
	s.dirty = false
	s.mu.Unlock()
	if e == nil {
		e = s.persist(s.path, b)
	}
	if e != nil {
		if wasDirty {
			s.mu.Lock()
			s.dirty = true
			s.mu.Unlock()
		}
		return e
	}
	s.lastSave = time.Now()
	return nil
}

// SaveIfDirty writes the database only when something changed and at least
// minInterval passed since the last write. Rewriting and fsyncing the whole
// file every few seconds needlessly wears SD cards on single-board computers.
func (s *Store) SaveIfDirty(minInterval time.Duration) error {
	s.saveMu.Lock()
	recent := time.Since(s.lastSave) < minInterval
	s.saveMu.Unlock()
	s.mu.RLock()
	dirty := s.dirty
	s.mu.RUnlock()
	if !dirty || recent {
		return nil
	}
	return s.Save()
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
	if s.disk.CoordinatorIEEE != id {
		s.disk.CoordinatorIEEE = id
		s.dirty = true
	}
}
