package store

import (
	"fmt"
	"time"
)

// AddressEpoch snapshots the address generation before starting a slow lookup.
func (s *Store) AddressEpoch() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addressClock
}
func (s *Store) markAddress(id string) {
	s.addressClock++
	s.addressVersions[id] = s.addressClock
}

// RecoverAddress never creates records, and an earlier lookup cannot replace a
// later announce, displacement or delete/re-add. Check and commit are atomic.
func (s *Store) RecoverAddress(id string, network uint16, epoch uint64) (Device, []string, error) {
	return s.recoverAddress(id, network, epoch, false)
}

// RecoverAddressSnapshot is used when buffered frames have no known IEEE.
// Any intervening address/membership change makes their sender ambiguous.
func (s *Store) RecoverAddressSnapshot(id string, network uint16, epoch uint64) (Device, []string, error) {
	return s.recoverAddress(id, network, epoch, true)
}

func (s *Store) recoverAddress(id string, network uint16, epoch uint64, unchanged bool) (Device, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if network == 0 || network >= 0xfff8 {
		return Device{}, nil, fmt.Errorf("invalid recovered address")
	}
	if unchanged && s.addressClock != epoch {
		return Device{}, nil, fmt.Errorf("address/membership snapshot changed during lookup")
	}
	d, exists := s.disk.Devices[id]
	if !exists {
		return Device{}, nil, fmt.Errorf("device is not paired or was removed")
	}
	if s.addressVersions[id] > epoch {
		return Device{}, nil, fmt.Errorf("newer address information supersedes this lookup")
	}
	// Do not evict a device that announced/recovered this address after lookup.
	for otherID, other := range s.disk.Devices {
		if otherID != id && other.Network == network && s.addressVersions[otherID] > epoch {
			return Device{}, nil, fmt.Errorf("address now belongs to a newer announcement")
		}
	}
	var displaced []string
	for otherID, other := range s.disk.Devices {
		if otherID != id && other.Network == network {
			other.Network = UnknownNetwork
			s.disk.Devices[otherID] = other
			s.markAddress(otherID)
			displaced = append(displaced, other.Name)
		}
	}
	if d.Network != network {
		d.Network = network
		s.markAddress(id)
	}
	if now := time.Now().UTC(); now.After(d.LastSeen) {
		d.LastSeen = now
	}
	s.disk.Devices[id] = d
	s.dirty = true
	return cloneDevice(d), displaced, nil
}
