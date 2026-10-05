package znp

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"zigbeemqttlink/internal/config"
)

// Z-Stack product codes from SYS_VERSION.
const (
	ProductZStack12  byte = 0 // CC2530/CC2531 Z-Stack Home 1.2: not supported for formation/backup
	ProductZStack3x0 byte = 1 // SimpleLink CC2652/CC1352 (extended NV tables)
	ProductZStack30x byte = 2 // CC2530/CC2538 Z-Stack 3.0.x (legacy NV tables)
)

// LinkKey is a device's trust-center link key and its frame counters.
type LinkKey struct {
	Key       []byte
	RxCounter uint32
	TxCounter uint32
}

// BackupDevice is one entry of the coordinator address manager.
type BackupDevice struct {
	NetworkAddress *uint16
	IEEE           []byte // big-endian display order
	IsDirectChild  bool
	LinkKey        *LinkKey
}

// Backup is everything needed to recreate a network on another coordinator.
// Byte arrays use big-endian display order, as in the JSON file.
type Backup struct {
	PanID           uint16
	ExtendedPanID   []byte
	Channel         int
	ChannelMask     []int
	NetworkKey      []byte
	KeySequence     byte
	FrameCounter    uint32
	SecurityLevel   int
	NetworkUpdateID int
	CoordinatorIEEE []byte
	TCLKSeed        []byte
	Product         *byte
	Devices         []BackupDevice
	Created         time.Time
	Source          string
}

// Open coordinator backup format v1 (github.com/zigpy/open-coordinator-backup),
// also written by Zigbee2MQTT as coordinator_backup.json.
type ocbLinkKey struct {
	Key       string `json:"key"`
	RxCounter uint32 `json:"rx_counter"`
	TxCounter uint32 `json:"tx_counter"`
}
type ocbDevice struct {
	NwkAddress *string     `json:"nwk_address"`
	IEEE       string      `json:"ieee_address"`
	IsChild    *bool       `json:"is_child,omitempty"`
	LinkKey    *ocbLinkKey `json:"link_key,omitempty"`
}
type ocbFile struct {
	Metadata struct {
		Format   string         `json:"format"`
		Version  int            `json:"version"`
		Source   string         `json:"source"`
		Internal map[string]any `json:"internal"`
	} `json:"metadata"`
	StackSpecific   map[string]map[string]any `json:"stack_specific,omitempty"`
	CoordinatorIEEE string                    `json:"coordinator_ieee"`
	PanID           string                    `json:"pan_id"`
	ExtendedPanID   string                    `json:"extended_pan_id"`
	NwkUpdateID     int                       `json:"nwk_update_id"`
	SecurityLevel   int                       `json:"security_level"`
	Channel         int                       `json:"channel"`
	ChannelMask     []int                     `json:"channel_mask"`
	NetworkKey      struct {
		Key            string `json:"key"`
		SequenceNumber int    `json:"sequence_number"`
		FrameCounter   uint32 `json:"frame_counter"`
	} `json:"network_key"`
	Devices []ocbDevice `json:"devices"`
}

const backupFormat = "zigpy/open-coordinator-backup"

func hexBytes(s string, n int, what string) ([]byte, error) {
	s = strings.TrimPrefix(strings.ToLower(strings.ReplaceAll(s, ":", "")), "0x")
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != n {
		return nil, fmt.Errorf("invalid %s %q", what, s)
	}
	return b, nil
}

// MarshalJSON writes the open coordinator backup format.
func (b *Backup) MarshalJSON() ([]byte, error) {
	var f ocbFile
	f.Metadata.Format = backupFormat
	f.Metadata.Version = 1
	f.Metadata.Source = b.Source
	f.Metadata.Internal = map[string]any{"date": b.Created.UTC().Format(time.RFC3339)}
	if b.Product != nil {
		f.Metadata.Internal["znpVersion"] = *b.Product
	}
	f.StackSpecific = map[string]map[string]any{"zstack": {}}
	if len(b.TCLKSeed) == 16 {
		f.StackSpecific["zstack"]["tclk_seed"] = hex.EncodeToString(b.TCLKSeed)
	}
	f.CoordinatorIEEE = hex.EncodeToString(b.CoordinatorIEEE)
	f.PanID = fmt.Sprintf("%04x", b.PanID)
	f.ExtendedPanID = hex.EncodeToString(b.ExtendedPanID)
	f.NwkUpdateID = b.NetworkUpdateID
	f.SecurityLevel = b.SecurityLevel
	f.Channel = b.Channel
	f.ChannelMask = append([]int{}, b.ChannelMask...)
	f.NetworkKey.Key = hex.EncodeToString(b.NetworkKey)
	f.NetworkKey.SequenceNumber = int(b.KeySequence)
	f.NetworkKey.FrameCounter = b.FrameCounter
	f.Devices = []ocbDevice{}
	for _, d := range b.Devices {
		child := d.IsDirectChild
		od := ocbDevice{IEEE: hex.EncodeToString(d.IEEE), IsChild: &child}
		if d.NetworkAddress != nil {
			s := fmt.Sprintf("%04x", *d.NetworkAddress)
			od.NwkAddress = &s
		}
		if d.LinkKey != nil {
			od.LinkKey = &ocbLinkKey{hex.EncodeToString(d.LinkKey.Key), d.LinkKey.RxCounter, d.LinkKey.TxCounter}
		}
		f.Devices = append(f.Devices, od)
	}
	return json.MarshalIndent(f, "", "  ")
}

// ParseBackup reads an open coordinator backup (zigpy, Zigbee2MQTT,
// ZigbeeMQTTlink) and validates every value used for a restore.
func ParseBackup(raw []byte) (*Backup, error) {
	var tree map[string]json.RawMessage
	if err := config.StrictJSON(raw, &tree); err != nil {
		return nil, fmt.Errorf("invalid backup JSON: %w", err)
	}
	required := func(obj map[string]json.RawMessage, fields ...string) error {
		for _, field := range fields {
			v, ok := obj[field]
			if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
				return fmt.Errorf("backup is missing required field %s", field)
			}
		}
		return nil
	}
	if err := required(tree, "metadata", "coordinator_ieee", "pan_id", "extended_pan_id", "nwk_update_id", "security_level", "channel", "channel_mask", "network_key", "devices"); err != nil {
		return nil, err
	}
	var keyFields map[string]json.RawMessage
	if err := json.Unmarshal(tree["network_key"], &keyFields); err != nil {
		return nil, err
	}
	if err := required(keyFields, "key", "sequence_number", "frame_counter"); err != nil {
		return nil, err
	}
	var devices []map[string]json.RawMessage
	if err := json.Unmarshal(tree["devices"], &devices); err != nil {
		return nil, err
	}
	for _, d := range devices {
		if err := required(d, "ieee_address"); err != nil {
			return nil, err
		}
		if rawKey, ok := d["link_key"]; ok && string(rawKey) != "null" {
			var lk map[string]json.RawMessage
			if err := json.Unmarshal(rawKey, &lk); err != nil {
				return nil, err
			}
			if err := required(lk, "key", "rx_counter", "tx_counter"); err != nil {
				return nil, err
			}
		}
	}
	var f ocbFile
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("backup is not valid JSON: %w", err)
	}
	if f.Metadata.Format != backupFormat {
		if bytes.Contains(raw, []byte(`"adapterType"`)) {
			return nil, fmt.Errorf("legacy Zigbee2MQTT backup format is not supported; create a new backup with a current Zigbee2MQTT or with -backup")
		}
		return nil, fmt.Errorf("unknown backup format %q (expected %s)", f.Metadata.Format, backupFormat)
	}
	if f.Metadata.Version != 1 {
		return nil, fmt.Errorf("unsupported backup format version %d", f.Metadata.Version)
	}
	b := &Backup{Source: f.Metadata.Source, NetworkUpdateID: f.NwkUpdateID, SecurityLevel: f.SecurityLevel, Channel: f.Channel, FrameCounter: f.NetworkKey.FrameCounter}
	var err error
	if b.CoordinatorIEEE, err = hexBytes(f.CoordinatorIEEE, 8, "coordinator_ieee"); err != nil {
		return nil, err
	}
	pan, err := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(f.PanID), "0x"), 16, 16)
	if err != nil || pan == 0 || pan == 0xffff {
		return nil, fmt.Errorf("invalid pan_id %q", f.PanID)
	}
	b.PanID = uint16(pan)
	if b.ExtendedPanID, err = hexBytes(f.ExtendedPanID, 8, "extended_pan_id"); err != nil {
		return nil, err
	}
	if allBytes(b.ExtendedPanID, 0) || allBytes(b.ExtendedPanID, 0xff) {
		return nil, fmt.Errorf("invalid extended_pan_id")
	}
	if b.NetworkKey, err = hexBytes(f.NetworkKey.Key, 16, "network key"); err != nil {
		return nil, err
	}
	if f.NetworkKey.SequenceNumber < 0 || f.NetworkKey.SequenceNumber > 255 {
		return nil, fmt.Errorf("invalid network key sequence number")
	}
	b.KeySequence = byte(f.NetworkKey.SequenceNumber)
	if b.Channel < 11 || b.Channel > 26 {
		return nil, fmt.Errorf("invalid channel %d", b.Channel)
	}
	for _, c := range f.ChannelMask {
		if c < 11 || c > 26 {
			return nil, fmt.Errorf("invalid channel %d in channel_mask", c)
		}
	}
	b.ChannelMask = append([]int{}, f.ChannelMask...)
	if len(b.ChannelMask) == 0 {
		b.ChannelMask = []int{b.Channel}
	}
	if b.SecurityLevel < 0 || b.SecurityLevel > 7 || b.NetworkUpdateID < 0 || b.NetworkUpdateID > 255 {
		return nil, fmt.Errorf("invalid security_level/nwk_update_id")
	}
	if z := f.StackSpecific["zstack"]; z != nil {
		if s, ok := z["tclk_seed"].(string); ok && s != "" {
			if b.TCLKSeed, err = hexBytes(s, 16, "tclk_seed"); err != nil {
				return nil, err
			}
		}
	}
	if v, ok := f.Metadata.Internal["znpVersion"].(float64); ok && v >= 0 && v <= 2 && v == float64(int(v)) {
		p := byte(v)
		b.Product = &p
	}
	if s, ok := f.Metadata.Internal["date"].(string); ok {
		b.Created, _ = time.Parse(time.RFC3339, s)
	}
	seen := map[string]bool{}
	for i, d := range f.Devices {
		bd := BackupDevice{IsDirectChild: true}
		if bd.IEEE, err = hexBytes(d.IEEE, 8, fmt.Sprintf("devices[%d].ieee_address", i)); err != nil {
			return nil, err
		}
		if seen[string(bd.IEEE)] {
			return nil, fmt.Errorf("duplicate device %x in backup", bd.IEEE)
		}
		seen[string(bd.IEEE)] = true
		if d.NwkAddress != nil && *d.NwkAddress != "" {
			n, err := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(*d.NwkAddress), "0x"), 16, 16)
			if err != nil {
				return nil, fmt.Errorf("invalid devices[%d].nwk_address", i)
			}
			nn := uint16(n)
			bd.NetworkAddress = &nn
		}
		if d.IsChild != nil {
			bd.IsDirectChild = *d.IsChild
		}
		if d.LinkKey != nil {
			key, err := hexBytes(d.LinkKey.Key, 16, fmt.Sprintf("devices[%d].link_key", i))
			if err != nil {
				return nil, err
			}
			bd.LinkKey = &LinkKey{key, d.LinkKey.RxCounter, d.LinkKey.TxCounter}
		}
		b.Devices = append(b.Devices, bd)
	}
	return b, b.Validate()
}

// ReadBackupFile loads and validates a backup file.
func ReadBackupFile(path string) (*Backup, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseBackup(raw)
}

// SameNetwork reports whether two backups describe the same network.
func (b *Backup) SameNetwork(o *Backup) bool {
	return b != nil && o != nil && b.PanID == o.PanID && bytes.Equal(b.CoordinatorIEEE, o.CoordinatorIEEE) && bytes.Equal(b.ExtendedPanID, o.ExtendedPanID) && bytes.Equal(b.NetworkKey, o.NetworkKey)
}

// IEEEString returns the coordinator address in the 0x... form used by the
// device database.
func (b *Backup) IEEEString() string { return "0x" + hex.EncodeToString(b.CoordinatorIEEE) }

// version reads the Z-Stack product code.
func version(ctx context.Context, c *Client) (byte, error) {
	f, err := c.Request(ctx, SYS, sysVersion, nil)
	if err != nil {
		return 0, err
	}
	if len(f.Data) < 5 {
		return 0, fmt.Errorf("short SYS_VERSION reply")
	}
	return f.Data[1], nil
}

func requireZStack3(product byte) error {
	switch product {
	case ProductZStack3x0, ProductZStack30x:
		return nil
	case ProductZStack12:
		return fmt.Errorf("Z-Stack 1.2 (CC2531/CC2530 Home 1.2 firmware) is not supported for network formation, backup or restore; flash Z-Stack 3.x firmware")
	}
	return fmt.Errorf("unknown Z-Stack product %d", product)
}

func coordinatorIEEE(ctx context.Context, c *Client) ([]byte, error) {
	f, err := c.Request(ctx, SYS, sysGetExtAddr, nil)
	if err != nil {
		return nil, err
	}
	if len(f.Data) != 8 {
		return nil, fmt.Errorf("invalid SYS_GET_EXTADDR reply")
	}
	return reverse(f.Data), nil
}

// configured reports whether the coordinator holds a commissioned network.
func configured(ctx context.Context, nv *nvMemory) (bool, error) {
	// The application's private configured marker is not proof that NV is
	// empty: networks created by other software or interrupted operations can
	// hold a valid NIB without it. Such a network still needs -force and backup.
	nib, err := nv.read(ctx, nvNIB)
	if err != nil {
		return false, err
	}
	if nib == nil {
		return false, nil
	}
	r, err := nibLayout.decode(nib)
	if err != nil {
		return false, err
	}
	return r.u16("nwkPanId") != 0xffff && r.u8("nwkLogicalChannel") != 0, nil
}

type tables struct {
	addr    []record
	sec     []record
	keyData []record
	tclk    []record
	secMat  []record
}

func readTables(ctx context.Context, nv *nvMemory, product byte) (*tables, error) {
	t := &tables{}
	var err error
	var rows [][]byte
	if product == ProductZStack3x0 {
		if rows, err = nv.readExTable(ctx, nvExAddrMgr); err == nil {
			t.addr, err = decodeRows(addressManagerLayout, rows)
		}
	} else {
		var raw []byte
		if raw, err = nv.read(ctx, nvAddrMgr); err == nil && raw != nil {
			t.addr, err = decodeFixedTable(addressManagerLayout, raw, nv.aligned)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("address manager table: %w", err)
	}
	raw, err := nv.read(ctx, nvApsLinkKeyTable)
	if err == nil && raw != nil {
		t.sec, err = decodeSecurityTable(raw, nv.aligned)
	}
	if err != nil {
		return nil, fmt.Errorf("security manager table: %w", err)
	}
	if product == ProductZStack3x0 {
		rows, err = nv.readExTable(ctx, nvExApsKeyDataTable)
	} else {
		rows, err = nv.readLegacyTable(ctx, nvApsLinkKeyDataStart, maxLegacyTables)
	}
	if err == nil {
		t.keyData, err = decodeRows(apsLinkKeyDataLayout, rows)
	}
	if err != nil {
		return nil, fmt.Errorf("APS link key data table: %w", err)
	}
	if product == ProductZStack3x0 {
		rows, err = nv.readExTable(ctx, nvExTCLKTable)
	} else {
		rows, err = nv.readLegacyTable(ctx, nvLegacyTCLKTable, 239)
	}
	if err == nil {
		t.tclk, err = decodeRows(apsTcLinkKeyLayout, rows)
	}
	if err != nil {
		return nil, fmt.Errorf("TCLK table: %w", err)
	}
	if product == ProductZStack3x0 {
		rows, err = nv.readExTable(ctx, nvExNwkSecMaterialTable)
	} else {
		rows, err = nv.readLegacyTable(ctx, nvLegacySecMaterial, 12)
	}
	if err == nil {
		t.secMat, err = decodeRows(nwkSecMaterialLayout, rows)
	}
	if err != nil {
		return nil, fmt.Errorf("network security material table: %w", err)
	}
	return t, nil
}

// CreateBackup reads network parameters, keys and device tables from the
// coordinator (zigbee-herdsman AdapterBackup.createBackup).
func CreateBackup(ctx context.Context, c *Client, source string) (*Backup, error) {
	return readBackup(ctx, c, source, true)
}

// readBackup also supports verification before committing the configured flag.
func readBackup(ctx context.Context, c *Client, source string, requireConfigured bool) (*Backup, error) {
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
	ok, err := configured(ctx, nv)
	if err != nil {
		return nil, err
	}
	if requireConfigured && !ok {
		return nil, fmt.Errorf("coordinator has no commissioned network to back up")
	}
	ieee, err := coordinatorIEEE(ctx, c)
	if err != nil {
		return nil, err
	}
	rawNIB, err := nv.read(ctx, nvNIB)
	if err != nil {
		return nil, err
	}
	nib, err := nibLayout.decode(rawNIB)
	if err != nil {
		return nil, err
	}
	rawKey, err := nv.read(ctx, nvActiveKeyInfo)
	if err != nil {
		return nil, err
	}
	if rawKey == nil {
		return nil, fmt.Errorf("coordinator has no active network key")
	}
	key, err := nwkKeyDescriptorLayout.decode(rawKey)
	if err != nil {
		return nil, err
	}
	t, err := readTables(ctx, nv, product)
	if err != nil {
		return nil, err
	}
	var seed []byte
	if raw, err := nv.read(ctx, nvTCLKSeed); err != nil {
		return nil, err
	} else if len(raw) == 16 {
		seed = raw
	} else if len(raw) != 0 {
		return nil, fmt.Errorf("invalid coordinator TCLK seed length %d", len(raw))
	}
	epid := nib.reversed("extendedPANID")
	frameCounter := uint32(1250)
	foundCounter := false
	var generic *record
	for i, m := range t.secMat {
		e := m.reversed("extendedPANID")
		if bytes.Equal(e, epid) {
			frameCounter, generic = m.u32("FrameCounter"), nil
			foundCounter = true
			break
		}
		if generic == nil && allBytes(e, 0xff) {
			generic = &t.secMat[i]
		}
	}
	if generic != nil {
		frameCounter = generic.u32("FrameCounter")
		foundCounter = true
	}
	if !foundCounter {
		// A freshly formed network can have no material descriptor until its first
		// secured traffic. Preserve herdsman's initial reservation only while there
		// are no devices; an established network must provide its actual counter.
		for _, r := range t.addr {
			if addressEntryUsed(r) {
				return nil, fmt.Errorf("no network security frame counter found for an established network")
			}
		}
	}
	b := &Backup{
		PanID: nib.u16("nwkPanId"), ExtendedPanID: epid, Channel: int(nib.u8("nwkLogicalChannel")),
		ChannelMask: unpackChannels(nib.u32("channelList")), NetworkKey: key.bytes("key"), KeySequence: key.u8("keySeqNum"),
		FrameCounter: frameCounter, SecurityLevel: int(nib.u8("SecurityLevel")), NetworkUpdateID: int(nib.u8("nwkUpdateId")),
		CoordinatorIEEE: ieee, TCLKSeed: seed, Product: &product, Created: time.Now(), Source: source,
	}
	for ami, e := range t.addr {
		if !addressEntryUsed(e) || e.u8("user")&(addrMgrUserAssoc|addrMgrUserSecurity) == 0 {
			continue
		}
		n := e.u16("nwkAddr")
		d := BackupDevice{NetworkAddress: &n, IEEE: e.reversed("extAddr"), IsDirectChild: e.u8("user")&addrMgrUserAssoc != 0}
		var sme *record
		for i := range t.sec {
			if securityEntryUsed(t.sec[i]) && int(t.sec[i].u16("ami")) == ami {
				sme = &t.sec[i]
				break
			}
		}
		if sme != nil {
			index := int(sme.u16("keyNvId"))
			if product == ProductZStack30x {
				index -= int(nvApsLinkKeyDataStart)
			}
			if index >= 0 && index < len(t.keyData) {
				k := t.keyData[index]
				d.LinkKey = &LinkKey{k.bytes("key"), k.u32("rxFrmCntr"), k.u32("txFrmCntr")}
			} else {
				return nil, fmt.Errorf("device %x has invalid link-key index %d", d.IEEE, index)
			}
		} else if seed != nil {
			for _, tc := range t.tclk {
				if allBytes(tc.bytes("extAddr"), 0) || !bytes.Equal(tc.reversed("extAddr"), d.IEEE) {
					continue
				}
				if tc.u8("SeedShift_IcIndex") >= 16 {
					return nil, fmt.Errorf("device %x has invalid TCLK seed shift", d.IEEE)
				}
				d.LinkKey = &LinkKey{deriveTCLinkKey(seed, int(tc.u8("SeedShift_IcIndex")), d.IEEE), tc.u32("rxFrmCntr"), tc.u32("txFrmCntr")}
				break
			}
		}
		b.Devices = append(b.Devices, d)
	}
	return b, b.Validate()
}

// deriveTCLinkKey computes a unique trust-center link key from the TCLK seed:
// the seed rotated left by shift, XOR the little-endian IEEE repeated twice.
func deriveTCLinkKey(seed []byte, shift int, ieee []byte) []byte {
	shift %= 16
	rotated := append(append([]byte{}, seed[shift:]...), seed[:shift]...)
	ext := reverse(ieee)
	out := make([]byte, 16)
	for i := range out {
		out[i] = rotated[i] ^ ext[i%8]
	}
	return out
}

// seedShift finds the rotation that reproduces key from seed, or -1.
func seedShift(seed, key, ieee []byte) int {
	for s := 0; s < 16; s++ {
		if bytes.Equal(deriveTCLinkKey(seed, s, ieee), key) {
			return s
		}
	}
	return -1
}

// MergeMissingDevices keeps devices with link keys that the coordinator
// tables lost (a known Z-Stack issue) if they are still paired: losing their
// key would prevent them and their children from rejoining after a restore.
func (b *Backup) MergeMissingDevices(old *Backup, paired func(ieee string) bool) []string {
	if old == nil || !b.SameNetwork(old) {
		return nil
	}
	present := map[string]bool{}
	for _, d := range b.Devices {
		present[string(d.IEEE)] = true
	}
	var added []string
	for _, d := range old.Devices {
		id := "0x" + hex.EncodeToString(d.IEEE)
		if d.LinkKey == nil || present[string(d.IEEE)] || !paired(id) {
			continue
		}
		b.Devices = append(b.Devices, d)
		added = append(added, id)
	}
	return added
}

// Validate checks the in-memory representation too: callers need not come
// through JSON. Backups may record exhausted counters; restoration may not.
func (b *Backup) Validate() error {
	if b == nil {
		return fmt.Errorf("backup is nil")
	}
	if err := (NetworkOptions{b.PanID, b.ExtendedPanID, b.ChannelMask, b.NetworkKey}).validate(); err != nil {
		return err
	}
	if len(b.CoordinatorIEEE) != 8 || allBytes(b.CoordinatorIEEE, 0) || allBytes(b.CoordinatorIEEE, 0xff) {
		return fmt.Errorf("invalid coordinator IEEE")
	}
	if b.Channel < 11 || b.Channel > 26 {
		return fmt.Errorf("invalid channel")
	}
	mask, _ := packChannels(b.ChannelMask)
	if mask&(1<<uint(b.Channel)) == 0 {
		return fmt.Errorf("channel_mask does not include active channel %d", b.Channel)
	}
	if b.SecurityLevel < 0 || b.SecurityLevel > 7 || b.NetworkUpdateID < 0 || b.NetworkUpdateID > 255 {
		return fmt.Errorf("invalid security level/update ID")
	}
	if len(b.TCLKSeed) != 0 && len(b.TCLKSeed) != 16 {
		return fmt.Errorf("invalid TCLK seed length")
	}
	seen := map[string]bool{}
	for _, d := range b.Devices {
		if len(d.IEEE) != 8 || allBytes(d.IEEE, 0) || allBytes(d.IEEE, 0xff) || bytes.Equal(d.IEEE, b.CoordinatorIEEE) || seen[string(d.IEEE)] {
			return fmt.Errorf("invalid/duplicate device IEEE %x", d.IEEE)
		}
		seen[string(d.IEEE)] = true
		if d.LinkKey != nil && len(d.LinkKey.Key) != 16 {
			return fmt.Errorf("invalid link key for %x", d.IEEE)
		}
	}
	return nil
}

func (b *Backup) validateRestore() error {
	if err := b.Validate(); err != nil {
		return err
	}
	if b.FrameCounter > math.MaxUint32-frameCounterRestoreMargin {
		return fmt.Errorf("network frame counter cannot be safely increased by %d", frameCounterRestoreMargin)
	}
	for _, d := range b.Devices {
		if d.LinkKey != nil && d.LinkKey.TxCounter > math.MaxUint32-frameCounterRestoreMargin {
			return fmt.Errorf("link-key counter for %x cannot be safely increased", d.IEEE)
		}
	}
	return nil
}

// VerifyRestored checks the network itself: identity, radio parameters,
// network key, TCLK seed and the network frame counter. A mismatch here means
// devices cannot talk to the coordinator, so the restore is not completed.
// Counters can advance on air, so only lower bounds are checked.
func (b *Backup) VerifyRestored(got *Backup) error {
	if err := b.validateRestore(); err != nil {
		return err
	}
	if got == nil || !b.SameNetwork(got) || b.IEEEString() != got.IEEEString() || b.Channel != got.Channel || b.SecurityLevel != got.SecurityLevel || b.NetworkUpdateID != got.NetworkUpdateID || b.KeySequence != got.KeySequence {
		return fmt.Errorf("restored network parameters differ from backup")
	}
	a, _ := packChannels(b.ChannelMask)
	c, _ := packChannels(got.ChannelMask)
	if a != c || got.FrameCounter < b.FrameCounter+frameCounterRestoreMargin || len(b.TCLKSeed) > 0 && !bytes.Equal(b.TCLKSeed, got.TCLKSeed) {
		return fmt.Errorf("restored channel mask, seed or frame counter differs from backup")
	}
	// Missing/changed address entries may be repaired by re-pairing; a
	// surviving key with rolled-back counters is a security failure, not that
	// firmware quirk. Never mark such a network ready.
	by := map[string]BackupDevice{}
	for _, d := range got.Devices {
		by[string(d.IEEE)] = d
	}
	for _, d := range b.Devices {
		g, ok := by[string(d.IEEE)]
		if !ok || d.LinkKey == nil || g.LinkKey == nil || !bytes.Equal(d.LinkKey.Key, g.LinkKey.Key) {
			continue
		}
		if g.LinkKey.TxCounter < d.LinkKey.TxCounter+frameCounterRestoreMargin || g.LinkKey.RxCounter < d.LinkKey.RxCounter {
			return fmt.Errorf("unsafe restored link-key counters for 0x%x; restore did not complete", d.IEEE)
		}
	}
	return nil
}

// DeviceDifferences lists device-table entries that read back differently
// from the backup. Address/missing-entry differences are reported, not fatal.
// Existing-key counter rollback is rejected by VerifyRestored. The reason for
// allowing address-table differences is that Z-Stack is known to drop or
// rewrite address-table entries on its own (see herdsman AdapterBackup), and
// refusing to finish would leave the whole network unusable, while a single
// affected device can still be re-paired.
func (b *Backup) DeviceDifferences(got *Backup) []string {
	if got == nil {
		return []string{"coordinator device table could not be read"}
	}
	by := map[string]BackupDevice{}
	for _, d := range got.Devices {
		by[string(d.IEEE)] = d
	}
	var diff []string
	expected := map[string]bool{}
	for _, d := range b.Devices {
		if !d.IsDirectChild && d.LinkKey == nil {
			continue // no allocated NV entry
		}
		expected[string(d.IEEE)] = true
		g, ok := by[string(d.IEEE)]
		switch {
		case !ok:
			diff = append(diff, fmt.Sprintf("0x%x: missing from coordinator table", d.IEEE))
			continue
		case g.IsDirectChild != d.IsDirectChild:
			diff = append(diff, fmt.Sprintf("0x%x: child flag differs", d.IEEE))
		}
		if d.NetworkAddress != nil && (g.NetworkAddress == nil || *g.NetworkAddress != *d.NetworkAddress) {
			diff = append(diff, fmt.Sprintf("0x%x: network address differs", d.IEEE))
		}
		switch {
		case (g.LinkKey == nil) != (d.LinkKey == nil):
			diff = append(diff, fmt.Sprintf("0x%x: link key missing", d.IEEE))
		case d.LinkKey != nil && !bytes.Equal(d.LinkKey.Key, g.LinkKey.Key):
			diff = append(diff, fmt.Sprintf("0x%x: link key differs", d.IEEE))
		case d.LinkKey != nil && (g.LinkKey.RxCounter < d.LinkKey.RxCounter || g.LinkKey.TxCounter < d.LinkKey.TxCounter+frameCounterRestoreMargin):
			diff = append(diff, fmt.Sprintf("0x%x: link key counter lower than backup", d.IEEE))
		}
	}
	for _, g := range got.Devices {
		if !expected[string(g.IEEE)] {
			diff = append(diff, fmt.Sprintf("0x%x: present on coordinator but not in backup", g.IEEE))
		}
	}
	return diff
}

// RaiseCountersFrom prevents restoring an older snapshot over counters that
// are still readable on this coordinator (or in its newer local backup).
// No RX/TX counters cross device keys, networks or coordinator identities.
func (b *Backup) RaiseCountersFrom(current *Backup) {
	if !b.SameNetwork(current) || b.IEEEString() != current.IEEEString() {
		return
	}
	if current.FrameCounter > b.FrameCounter {
		b.FrameCounter = current.FrameCounter
	}
	by := map[string]BackupDevice{}
	for _, d := range current.Devices {
		by[string(d.IEEE)] = d
	}
	for _, d := range b.Devices {
		g, ok := by[string(d.IEEE)]
		if !ok || d.LinkKey == nil || g.LinkKey == nil || !bytes.Equal(d.LinkKey.Key, g.LinkKey.Key) {
			continue
		}
		if g.LinkKey.TxCounter > d.LinkKey.TxCounter {
			d.LinkKey.TxCounter = g.LinkKey.TxCounter
		}
		if g.LinkKey.RxCounter > d.LinkKey.RxCounter {
			d.LinkKey.RxCounter = g.LinkKey.RxCounter
		}
	}
}
