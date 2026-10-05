package znp

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"time"
)

// Subsystems and commands used for commissioning.
const (
	appCnf                    byte = 0x0f
	appCnfBdbStartCommission  byte = 0x05
	appCnfBdbSetChannel       byte = 0x08
	bdbModeFormation          byte = 0x04
	zdoStartupFromApp         byte = 0x40
	zdoExtNwkInfo             byte = 0x50
	zdoStateChangeInd         byte = 0xc0
	utilGetDeviceInfo         byte = 0x00
	devStateCoordinator       byte = 9
	startupOptionClearAll     byte = 0x03 // clear configuration and network state
	logicalTypeCoordinator    byte = 0x00
	frameCounterRestoreMargin      = 2500
)

// Timing, variable for tests with an emulated coordinator.
var (
	resetTimeout        = 30 * time.Second
	formationTimeout    = 100 * time.Second
	startupTimeout      = 60 * time.Second
	nibSettleInterval   = 3 * time.Second
	nibSettleAttempts   = 10
	restoreSettleDelay  = time.Second
	randomReader        = rand.Reader
	collisionRetryLimit = 3
)

// NetworkOptions are the parameters of a new network. ExtendedPanID is in
// big-endian display order.
type NetworkOptions struct {
	PanID         uint16
	ExtendedPanID []byte
	Channels      []int
	NetworkKey    []byte
}

// RandomNetwork generates a new network with a cryptographically random
// network key, PAN ID and extended PAN ID.
func RandomNetwork(channel int) (NetworkOptions, error) {
	if channel < 11 || channel > 26 {
		return NetworkOptions{}, fmt.Errorf("channel must be 11..26")
	}
	buf := make([]byte, 2+8+16)
	for {
		if _, err := io.ReadFull(randomReader, buf); err != nil {
			return NetworkOptions{}, fmt.Errorf("random generator: %w", err)
		}
		pan := binary.LittleEndian.Uint16(buf)
		epid, key := buf[2:10], buf[10:26]
		if pan == 0 || pan == 0xffff || allBytes(epid, 0) || allBytes(epid, 0xff) || allBytes(key, 0) {
			continue
		}
		return NetworkOptions{PanID: pan, ExtendedPanID: append([]byte{}, epid...), Channels: []int{channel}, NetworkKey: append([]byte{}, key...)}, nil
	}
}

func (o NetworkOptions) validate() error {
	if o.PanID == 0 || o.PanID == 0xffff {
		return fmt.Errorf("invalid PAN ID 0x%04x", o.PanID)
	}
	if len(o.ExtendedPanID) != 8 || allBytes(o.ExtendedPanID, 0) || allBytes(o.ExtendedPanID, 0xff) {
		return fmt.Errorf("invalid extended PAN ID")
	}
	if len(o.NetworkKey) != 16 {
		return fmt.Errorf("network key must be 16 bytes")
	}
	if len(o.Channels) == 0 {
		return fmt.Errorf("channel list is empty")
	}
	_, err := packChannels(o.Channels)
	return err
}

// CoordinatorStatus describes what is stored on the coordinator.
type CoordinatorStatus struct {
	Product    byte
	IEEE       string
	Configured bool
	// Ready: the ZigbeeMQTTlink/Zigbee2MQTT "configured" marker is set, i.e.
	// the last formation/restore completed. Configured && !Ready means an
	// interrupted operation or a network created by other software.
	Ready    bool
	PanID    uint16
	Channel  int
	ExtPanID []byte
}

// Inspect reads the coordinator state without changing it.
func Inspect(ctx context.Context, c *Client) (CoordinatorStatus, error) {
	var s CoordinatorStatus
	var err error
	if s.Product, err = version(ctx, c); err != nil {
		return s, err
	}
	if err = requireZStack3(s.Product); err != nil {
		return s, err
	}
	ieee, err := coordinatorIEEE(ctx, c)
	if err != nil {
		return s, err
	}
	s.IEEE = fmt.Sprintf("0x%x", ieee)
	nv, err := newNV(ctx, c)
	if err != nil {
		return s, err
	}
	if flag, err := nv.read(ctx, nvHasConfiguredZStack3); err != nil {
		return s, err
	} else {
		s.Ready = len(flag) >= 1 && flag[0] == 0x55
	}
	if s.Configured, err = configured(ctx, nv); err != nil || !s.Configured {
		return s, err
	}
	raw, err := nv.read(ctx, nvNIB)
	if err != nil {
		return s, err
	}
	nib, err := nibLayout.decode(raw)
	if err != nil {
		return s, err
	}
	s.PanID, s.Channel, s.ExtPanID = nib.u16("nwkPanId"), int(nib.u8("nwkLogicalChannel")), nib.reversed("extendedPANID")
	return s, nil
}

type commissioner struct {
	c       *Client
	nv      *nvMemory
	product byte
	log     *slog.Logger
}

func newCommissioner(ctx context.Context, c *Client, log *slog.Logger) (*commissioner, error) {
	product, err := version(ctx, c)
	if err != nil {
		return nil, err
	}
	if err = requireZStack3(product); err != nil {
		return nil, err
	}
	nv, err := newNV(ctx, c)
	if err != nil {
		return nil, err
	}
	return &commissioner{c: c, nv: nv, product: product, log: log}, nil
}

func (m *commissioner) reset(ctx context.Context) error {
	ch, stop := m.c.Watch(SYS, sysResetInd, nil)
	defer stop()
	if err := m.c.Notify(ctx, SYS, sysResetReq, []byte{0x01}); err != nil { // soft reset
		return err
	}
	wait, cancel := context.WithTimeout(ctx, resetTimeout)
	defer cancel()
	if _, err := m.c.Await(wait, ch); err != nil {
		return fmt.Errorf("coordinator did not report reset: %w", err)
	}
	return nil
}

func (m *commissioner) request(ctx context.Context, sub, cmd byte, data []byte, accept ...byte) (Frame, error) {
	f, err := m.c.Request(ctx, sub, cmd, data)
	if err != nil {
		return f, err
	}
	if len(f.Data) < 1 {
		return f, fmt.Errorf("empty reply to %02x/%02x", sub, cmd)
	}
	if f.Data[0] == 0 {
		return f, nil
	}
	for _, s := range accept {
		if f.Data[0] == s {
			return f, nil
		}
	}
	return f, fmt.Errorf("command %02x/%02x failed with status 0x%02x", sub, cmd, f.Data[0])
}

// updateCommissioningItems writes the parameters used when forming or
// resuming the network (herdsman updateCommissioningNvItems).
func (m *commissioner) updateCommissioningItems(ctx context.Context, o NetworkOptions) error {
	mask, err := packChannels(o.Channels)
	if err != nil {
		return err
	}
	maskBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(maskBytes, mask)
	epid := reverse(o.ExtendedPanID)
	items := []struct {
		id    uint16
		value []byte
	}{
		{nvStartupOption, []byte{0}},
		{nvLogicalType, []byte{logicalTypeCoordinator}},
		{nvZdoDirectCB, []byte{1}},
		{nvChanList, maskBytes},
		{nvPanID, U16(o.PanID)},
		{nvExtendedPanID, epid},
		{nvApsUseExtPanID, epid},
		{nvPreCfgKeysEnable, []byte{0}}, // the key is sent encrypted, not distributed in the clear
		{nvPreCfgKey, o.NetworkKey},
	}
	for _, it := range items {
		if err := m.nv.update(ctx, it.id, it.value); err != nil {
			return fmt.Errorf("NV item 0x%04x: %w", it.id, err)
		}
	}
	return nil
}

func (m *commissioner) extNwkInfo(ctx context.Context) (state byte, pan uint16, channel byte, err error) {
	f, err := m.c.Request(ctx, ZDO, zdoExtNwkInfo, nil)
	if err != nil {
		return 0, 0, 0, err
	}
	if len(f.Data) < 24 {
		return 0, 0, 0, fmt.Errorf("short ZDO_EXT_NWK_INFO reply")
	}
	return f.Data[2], le.Uint16(f.Data[3:5]), f.Data[23], nil
}

// commission forms a network with the given options (herdsman
// beginCommissioning). Z-Stack 3.x uses BDB network formation.
func (m *commissioner) commission(ctx context.Context, o NetworkOptions, failOnCollision, writeFlag bool) error {
	if err := o.validate(); err != nil {
		return err
	}
	mask, _ := packChannels(o.Channels)
	// The previous marker survives a Z-Stack clear. Never let a failed
	// operation expose a temporary/partial network as commissioned.
	if err := m.nv.write(ctx, nvHasConfiguredZStack3, []byte{0}); err != nil {
		return err
	}
	if err := m.nv.remove(ctx, nvNIB); err != nil {
		return err
	}
	m.log.Info("clearing coordinator network state")
	if err := m.nv.write(ctx, nvStartupOption, []byte{startupOptionClearAll}); err != nil {
		return err
	}
	if err := m.reset(ctx); err != nil {
		return err
	}
	if err := m.nv.write(ctx, nvStartupOption, []byte{0}); err != nil {
		return err
	}
	if err := m.updateCommissioningItems(ctx, o); err != nil {
		return err
	}
	maskBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(maskBytes, mask)
	if _, err := m.request(ctx, appCnf, appCnfBdbSetChannel, append([]byte{1}, maskBytes...)); err != nil {
		return err
	}
	if _, err := m.request(ctx, appCnf, appCnfBdbSetChannel, []byte{0, 0, 0, 0, 0}); err != nil {
		return err
	}
	m.log.Info("forming network", "pan_id", fmt.Sprintf("0x%04x", o.PanID), "channels", o.Channels)
	ch, stop := m.c.Watch(ZDO, zdoStateChangeInd, func(b []byte) bool { return len(b) >= 1 && b[0] == devStateCoordinator })
	defer stop()
	if _, err := m.request(ctx, appCnf, appCnfBdbStartCommission, []byte{bdbModeFormation}); err != nil {
		return err
	}
	wait, cancel := context.WithTimeout(ctx, formationTimeout)
	_, err := m.c.Await(wait, ch)
	cancel()
	if err != nil {
		return fmt.Errorf("network formation timed out; a network with the same PAN ID may exist nearby: %w", err)
	}
	var nib record
	for attempt := 0; ; attempt++ {
		if attempt >= nibSettleAttempts {
			return fmt.Errorf("network formation failed: network parameters did not settle")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(nibSettleInterval):
		}
		raw, err := m.nv.read(ctx, nvNIB)
		if err != nil {
			return err
		}
		if raw == nil {
			continue
		}
		if nib, err = nibLayout.decode(raw); err != nil {
			return err
		}
		if nib.u16("nwkPanId") != 0xffff && nib.u8("nwkLogicalChannel") != 0 {
			break
		}
	}
	_, pan, _, err := m.extNwkInfo(ctx)
	if err != nil {
		return err
	}
	if pan != o.PanID && failOnCollision {
		return errPANCollision{o.PanID, pan}
	}
	if writeFlag {
		actual, err := readBackup(ctx, m.c, "formation-verification", false)
		if err != nil {
			return err
		}
		actualMask, _ := packChannels(actual.ChannelMask)
		if actual.PanID != o.PanID || !bytes.Equal(actual.ExtendedPanID, o.ExtendedPanID) || !bytes.Equal(actual.NetworkKey, o.NetworkKey) || actualMask != mask || mask&(1<<uint(actual.Channel)) == 0 {
			return fmt.Errorf("formed network parameters differ from requested parameters")
		}
		return m.nv.write(ctx, nvHasConfiguredZStack3, []byte{0x55})
	}
	return nil
}

type errPANCollision struct{ want, got uint16 }

func (e errPANCollision) Error() string {
	return fmt.Sprintf("PAN ID collision: requested 0x%04x, coordinator chose 0x%04x", e.want, e.got)
}

// startCoordinator starts the coordinator from its stored state (herdsman
// beginStartup).
func (m *commissioner) startCoordinator(ctx context.Context) error {
	f, err := m.c.Request(ctx, UTIL, utilGetDeviceInfo, nil)
	if err != nil {
		return err
	}
	if len(f.Data) >= 14 && f.Data[12] == devStateCoordinator {
		return nil
	}
	ch, stop := m.c.Watch(ZDO, zdoStateChangeInd, func(b []byte) bool { return len(b) >= 1 && b[0] == devStateCoordinator })
	defer stop()
	if _, err := m.request(ctx, ZDO, zdoStartupFromApp, U16(100), 1); err != nil {
		return err
	}
	wait, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()
	if _, err := m.c.Await(wait, ch); err != nil {
		return fmt.Errorf("coordinator did not start: %w", err)
	}
	return nil
}

// FormNetwork creates a new network on a Z-Stack 3.x coordinator. All previous
// network state on the coordinator is erased. A PAN ID collision with a
// nearby network is retried with new random identifiers.
func FormNetwork(ctx context.Context, c *Client, o NetworkOptions, log *slog.Logger) (NetworkOptions, error) {
	m, err := newCommissioner(ctx, c, log)
	if err != nil {
		return o, err
	}
	for attempt := 1; ; attempt++ {
		err = m.commission(ctx, o, true, true)
		var collision errPANCollision
		if ok := asCollision(err, &collision); !ok || attempt >= collisionRetryLimit {
			return o, err
		}
		log.Warn("PAN ID already used nearby; retrying with new identifiers", "attempt", attempt, "error", err)
		fresh, rerr := RandomNetwork(o.Channels[0])
		if rerr != nil {
			return o, rerr
		}
		fresh.Channels = o.Channels
		fresh.NetworkKey = o.NetworkKey
		o = fresh
	}
}

func asCollision(err error, out *errPANCollision) bool {
	c, ok := err.(errPANCollision)
	if ok {
		*out = c
	}
	return ok
}

// RestoreNetwork writes a backup to a Z-Stack 3.x coordinator so that all
// previously paired devices keep working without re-pairing (herdsman
// beginRestore/restoreBackup). The coordinator IEEE address is cloned.
func RestoreNetwork(ctx context.Context, c *Client, b *Backup, log *slog.Logger) error {
	return RestoreNetworkWithOptions(ctx, c, b, log, RestoreOptions{})
}

// RestoreOptions explicitly authorizes losing the unreadable current snapshot.
// It never bypasses input validation or final network/counter verification.
type RestoreOptions struct{ AllowUnreadableCurrent bool }

func RestoreNetworkWithOptions(ctx context.Context, c *Client, b *Backup, log *slog.Logger, options RestoreOptions) error {
	if err := b.validateRestore(); err != nil {
		return err
	}
	m, err := newCommissioner(ctx, c, log)
	if err != nil {
		return err
	}
	if b.Product != nil && *b.Product == ProductZStack12 {
		return fmt.Errorf("backup was created on Z-Stack 1.2 and cannot be restored")
	}
	exists, err := configured(ctx, m.nv)
	if err != nil {
		return err
	}
	if exists {
		current, err := readBackup(ctx, c, "pre-restore-counters", true)
		if err != nil {
			if !allowUnreadable(ctx, c, options) {
				return err
			}
			log.Warn("current network snapshot is unreadable; restoring explicitly without its counters", "error", err)
		} else {
			b.RaiseCountersFrom(current)
		}
		if err = b.validateRestore(); err != nil {
			return err
		}
	}
	// Reject known table-capacity errors BEFORE erasing the existing network.
	current, err := readTables(ctx, m.nv, m.product)
	if err != nil {
		if !allowUnreadable(ctx, c, options) {
			return err
		}
		log.Warn("current NV table sizes cannot be decoded; capacity will be checked after temporary formation", "error", err)
		current = &tables{}
	}
	if len(current.addr) > 0 && len(current.sec) > 0 && len(current.keyData) > 0 && len(current.tclk) > 0 && len(current.secMat) > 0 {
		if err := checkRestoreCapacity(b, current); err != nil {
			return err
		}
	}
	// Commission a throw-away network first: it initialises every NV item and
	// table to the layout of this firmware; the backup then overwrites them.
	provisioning, err := RandomNetwork(11 + int(b.PanID%16))
	if err != nil {
		return err
	}
	log.Info("commissioning temporary network before restore")
	if err = m.commission(ctx, provisioning, false, false); err != nil {
		return fmt.Errorf("temporary network: %w", err)
	}
	if err = m.writeBackup(ctx, b); err != nil {
		return err
	}
	opts := NetworkOptions{PanID: b.PanID, ExtendedPanID: b.ExtendedPanID, Channels: b.ChannelMask, NetworkKey: b.NetworkKey}
	if err = m.updateCommissioningItems(ctx, opts); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(restoreSettleDelay):
	}
	if err = m.reset(ctx); err != nil {
		return err
	}
	if err = m.startCoordinator(ctx); err != nil {
		return err
	}
	actual, err := readBackup(ctx, c, "restore-verification", false)
	if err != nil {
		return err
	}
	if err = b.VerifyRestored(actual); err != nil {
		return err
	}
	for _, d := range b.DeviceDifferences(actual) {
		log.Warn("restored device table entry differs from backup; re-pair this device if it stops responding", "device", d)
	}
	if err = m.nv.write(ctx, nvHasConfiguredZStack3, []byte{0x55}); err != nil {
		return err
	}
	return nil
}

func nextFree(rows []record, used func(record) bool) int {
	for i, r := range rows {
		if !used(r) {
			return i
		}
	}
	return -1
}

func (m *commissioner) writeBackup(ctx context.Context, b *Backup) error {
	rawNIB, err := m.nv.read(ctx, nvNIB)
	if err != nil {
		return err
	}
	if rawNIB == nil {
		return fmt.Errorf("temporary network has no NIB")
	}
	nib, err := nibLayout.decode(rawNIB)
	if err != nil {
		return err
	}
	mask, err := packChannels(b.ChannelMask)
	if err != nil {
		return err
	}
	nib.setU16("nwkPanId", b.PanID)
	nib.setU32("channelList", mask)
	nib.setU8("nwkLogicalChannel", byte(b.Channel))
	nib.setReversed("extendedPANID", b.ExtendedPanID)
	nib.setU8("SecurityLevel", byte(b.SecurityLevel))
	nib.setU8("nwkUpdateId", byte(b.NetworkUpdateID))

	key := nwkKeyDescriptorLayout.empty()
	key.setU8("keySeqNum", b.KeySequence)
	key.setBytes("key", b.NetworkKey)

	current, err := readTables(ctx, m.nv, m.product)
	if err != nil {
		return err
	}
	if len(current.addr) == 0 || len(current.sec) == 0 || len(current.keyData) == 0 || len(current.tclk) == 0 || len(current.secMat) == 0 {
		return fmt.Errorf("coordinator NV tables are missing (addr=%d sec=%d keys=%d tclk=%d material=%d)",
			len(current.addr), len(current.sec), len(current.keyData), len(current.tclk), len(current.secMat))
	}
	if err := checkRestoreCapacity(b, current); err != nil {
		return err
	}
	addr := make([]record, len(current.addr))
	for i := range addr {
		addr[i] = addressManagerLayout.empty()
	}
	sec := make([]record, len(current.sec))
	for i := range sec {
		sec[i] = emptySecurityEntry()
	}
	keyData := make([]record, len(current.keyData))
	for i := range keyData {
		keyData[i] = apsLinkKeyDataLayout.empty()
	}
	tclk := make([]record, len(current.tclk))
	for i := range tclk {
		tclk[i] = apsTcLinkKeyLayout.empty()
	}
	secMat := make([]record, len(current.secMat))
	for i := range secMat {
		secMat[i] = nwkSecMaterialLayout.empty()
	}
	secMat[0].setU32("FrameCounter", b.FrameCounter+frameCounterRestoreMargin)
	secMat[0].setReversed("extendedPANID", b.ExtendedPanID)
	last := secMat[len(secMat)-1]
	last.setU32("FrameCounter", b.FrameCounter+frameCounterRestoreMargin)
	last.setReversed("extendedPANID", bytes.Repeat([]byte{0xff}, 8))

	usedKeyData := map[int]bool{}
	for _, d := range b.Devices {
		user := byte(0)
		if d.IsDirectChild {
			user |= addrMgrUserAssoc
		}
		if d.LinkKey != nil {
			user |= addrMgrUserSecurity
		}
		if user == 0 {
			// Neither a child nor a key holder: Z-Stack treats such an address
			// entry as free, so there is nothing to restore for it.
			continue
		}
		ami := nextFree(addr, addressEntryUsed)
		if ami < 0 {
			return fmt.Errorf("coordinator address table is too small for %d devices (capacity %d)", len(b.Devices), len(addr))
		}
		e := addr[ami]
		nwk := uint16(0xffff)
		if d.NetworkAddress != nil {
			nwk = *d.NetworkAddress
		}
		e.setU16("nwkAddr", nwk)
		e.setReversed("extAddr", d.IEEE)
		e.setU8("user", user)
		if d.LinkKey == nil {
			continue
		}
		if len(b.TCLKSeed) == 16 {
			if shift := seedShift(b.TCLKSeed, d.LinkKey.Key, d.IEEE); shift >= 0 {
				slot := nextFree(tclk, func(r record) bool { return !allBytes(r.bytes("extAddr"), 0) })
				if slot < 0 {
					return fmt.Errorf("coordinator TCLK table is too small (capacity %d)", len(tclk))
				}
				t := tclk[slot]
				t.setReversed("extAddr", d.IEEE)
				t.setU8("SeedShift_IcIndex", byte(shift))
				t.setU8("keyAttributes", 2) // ZG_VERIFIED_KEY
				t.setU8("keyType", 0)
				t.setU32("rxFrmCntr", d.LinkKey.RxCounter)
				t.setU32("txFrmCntr", d.LinkKey.TxCounter+frameCounterRestoreMargin)
				continue
			}
		}
		kd := -1
		for i := range keyData {
			if !usedKeyData[i] {
				kd = i
				break
			}
		}
		if kd < 0 {
			return fmt.Errorf("coordinator APS link key table is too small (capacity %d)", len(keyData))
		}
		usedKeyData[kd] = true
		keyData[kd].setBytes("key", d.LinkKey.Key)
		keyData[kd].setU32("rxFrmCntr", d.LinkKey.RxCounter)
		keyData[kd].setU32("txFrmCntr", d.LinkKey.TxCounter+frameCounterRestoreMargin)
		si := nextFree(sec, securityEntryUsed)
		if si < 0 {
			return fmt.Errorf("coordinator security manager table is too small (capacity %d)", len(sec))
		}
		sec[si].setU16("ami", uint16(ami))
		keyNvID := uint16(kd)
		if m.product == ProductZStack30x {
			keyNvID += nvApsLinkKeyDataStart
		}
		sec[si].setU16("keyNvId", keyNvID)
		sec[si].setU8("authenticationOption", 1) // ZDSecMgr_Authenticated_CBCK
	}

	log := m.log
	log.Info("writing network backup to coordinator", "devices", len(b.Devices))
	steps := []struct {
		what string
		fn   func() error
	}{
		{"coordinator IEEE", func() error { return m.nv.write(ctx, nvExtAddr, reverse(b.CoordinatorIEEE)) }},
		{"NIB", func() error { return m.nv.write(ctx, nvNIB, nib.encode(m.nv.aligned)) }},
		{"active key", func() error { return m.nv.update(ctx, nvActiveKeyInfo, key.encode(m.nv.aligned)) }},
		{"alternate key", func() error { return m.nv.update(ctx, nvAlternKeyInfo, key.encode(m.nv.aligned)) }},
		{"TCLK seed", func() error {
			if len(b.TCLKSeed) != 16 {
				return nil
			}
			return m.nv.write(ctx, nvTCLKSeed, b.TCLKSeed)
		}},
		{"frame counters", func() error {
			if m.product == ProductZStack3x0 {
				return m.nv.writeExTable(ctx, nvExNwkSecMaterialTable, secMat)
			}
			return m.nv.writeLegacyTable(ctx, nvLegacySecMaterial, secMat)
		}},
		{"address table", func() error {
			if m.product == ProductZStack3x0 {
				return m.nv.writeExTable(ctx, nvExAddrMgr, addr)
			}
			return m.nv.write(ctx, nvAddrMgr, encodeFixedTable(addr, m.nv.aligned))
		}},
		{"security table", func() error { return m.nv.write(ctx, nvApsLinkKeyTable, encodeSecurityTable(sec, m.nv.aligned)) }},
		{"link key table", func() error {
			if m.product == ProductZStack3x0 {
				return m.nv.writeExTable(ctx, nvExApsKeyDataTable, keyData)
			}
			return m.nv.writeLegacyTable(ctx, nvApsLinkKeyDataStart, keyData)
		}},
		{"TCLK table", func() error {
			if m.product == ProductZStack3x0 {
				return m.nv.writeExTable(ctx, nvExTCLKTable, tclk)
			}
			return m.nv.writeLegacyTable(ctx, nvLegacyTCLKTable, tclk)
		}},
	}
	for _, s := range steps {
		if err := s.fn(); err != nil {
			return fmt.Errorf("restore %s: %w", s.what, err)
		}
	}
	return nil
}

func checkRestoreCapacity(b *Backup, t *tables) error {
	addr, aps, tc := 0, 0, 0
	for _, d := range b.Devices {
		if !d.IsDirectChild && d.LinkKey == nil {
			continue
		}
		addr++
		if d.LinkKey == nil {
			continue
		}
		if len(b.TCLKSeed) == 16 && seedShift(b.TCLKSeed, d.LinkKey.Key, d.IEEE) >= 0 {
			tc++
		} else {
			aps++
		}
	}
	if addr > len(t.addr) || aps > len(t.sec) || aps > len(t.keyData) || tc > len(t.tclk) || len(t.secMat) < 2 {
		return fmt.Errorf("backup exceeds coordinator NV capacity: need addr=%d APS=%d TCLK=%d; capacity addr=%d security=%d APS=%d TCLK=%d material=%d", addr, aps, tc, len(t.addr), len(t.sec), len(t.keyData), len(t.tclk), len(t.secMat))
	}
	return nil
}

func allowUnreadable(ctx context.Context, c *Client, options RestoreOptions) bool {
	if !options.AllowUnreadableCurrent || ctx.Err() != nil {
		return false
	}
	select {
	case <-c.Done():
		return false
	default:
		return true
	}
}
