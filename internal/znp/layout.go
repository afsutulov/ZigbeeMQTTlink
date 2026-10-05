package znp

import (
	"fmt"
)

// Z-Stack stores C structs in NV memory. 8-bit MCUs (CC2530/CC2531) pack them
// ("unaligned"); 32-bit SimpleLink chips (CC2652/CC1352) align 16/32-bit
// members to 2 bytes and pad the struct to an even length ("aligned").
// The layouts below mirror zigbee-herdsman's z-stack structs.

type fieldKind byte

const (
	fU8 fieldKind = iota
	fU16
	fU32
	fBytes
)

type field struct {
	name string
	kind fieldKind
	n    int // byte length for fBytes
}

type layout struct {
	name    string
	fields  []field
	padding byte
	offsets map[string]int // canonical (unaligned) offsets
	size    int            // canonical (unaligned) size
}

func newLayout(name string, padding byte, fields ...field) *layout {
	l := &layout{name: name, fields: fields, padding: padding, offsets: map[string]int{}}
	for _, f := range fields {
		l.offsets[f.name] = l.size
		l.size += f.width()
	}
	return l
}

func (f field) width() int {
	switch f.kind {
	case fU8:
		return 1
	case fU16:
		return 2
	case fU32:
		return 4
	}
	return f.n
}

// alignedOffsets returns member offsets and total length in aligned mode.
func (l *layout) alignedOffsets() ([]int, int) {
	offsets := make([]int, len(l.fields))
	offset := 0
	for i, f := range l.fields {
		if f.kind == fU16 || f.kind == fU32 {
			offset += offset % 2
		}
		offsets[i] = offset
		offset += f.width()
	}
	return offsets, offset + offset%2
}

func (l *layout) length(aligned bool) int {
	if !aligned {
		return l.size
	}
	_, n := l.alignedOffsets()
	return n
}

// record holds a struct in canonical unaligned form.
type record struct {
	l   *layout
	buf []byte
}

func (l *layout) empty() record { return record{l, make([]byte, l.size)} }

// decode accepts either representation, detected by length.
func (l *layout) decode(b []byte) (record, error) {
	if len(b) == l.size {
		return record{l, append([]byte(nil), b...)}, nil
	}
	offsets, total := l.alignedOffsets()
	if len(b) != total {
		return record{}, fmt.Errorf("%s: length %d, expected %d (unaligned) or %d (aligned)", l.name, len(b), l.size, total)
	}
	r := l.empty()
	for i, f := range l.fields {
		copy(r.buf[l.offsets[f.name]:], b[offsets[i]:offsets[i]+f.width()])
	}
	return r, nil
}

func (r record) encode(aligned bool) []byte {
	if !aligned {
		return append([]byte(nil), r.buf...)
	}
	offsets, total := r.l.alignedOffsets()
	out := make([]byte, total)
	for i := range out {
		out[i] = r.l.padding
	}
	for i, f := range r.l.fields {
		o := r.l.offsets[f.name]
		copy(out[offsets[i]:], r.buf[o:o+f.width()])
	}
	return out
}

func (r record) at(name string) (int, field) {
	o, ok := r.l.offsets[name]
	if !ok {
		panic("znp: unknown struct member " + r.l.name + "." + name)
	}
	for _, f := range r.l.fields {
		if f.name == name {
			return o, f
		}
	}
	panic("unreachable")
}

func (r record) u8(name string) byte { o, _ := r.at(name); return r.buf[o] }
func (r record) u16(name string) uint16 {
	o, _ := r.at(name)
	return le.Uint16(r.buf[o:])
}
func (r record) u32(name string) uint32 {
	o, _ := r.at(name)
	return le.Uint32(r.buf[o:])
}
func (r record) bytes(name string) []byte {
	o, f := r.at(name)
	return append([]byte(nil), r.buf[o:o+f.width()]...)
}

// reversed returns a byte array member in big-endian display order (IEEE
// addresses and extended PAN IDs are stored little-endian).
func (r record) reversed(name string) []byte { return reverse(r.bytes(name)) }

func (r record) setU8(name string, v byte) { o, _ := r.at(name); r.buf[o] = v }
func (r record) setU16(name string, v uint16) {
	o, _ := r.at(name)
	le.PutUint16(r.buf[o:], v)
}
func (r record) setU32(name string, v uint32) {
	o, _ := r.at(name)
	le.PutUint32(r.buf[o:], v)
}
func (r record) setBytes(name string, v []byte) {
	o, f := r.at(name)
	if len(v) != f.width() {
		panic(fmt.Sprintf("znp: %s.%s needs %d bytes, got %d", r.l.name, name, f.width(), len(v)))
	}
	copy(r.buf[o:], v)
}
func (r record) setReversed(name string, v []byte) { r.setBytes(name, reverse(v)) }

func reverse(b []byte) []byte {
	out := make([]byte, len(b))
	for i := range b {
		out[len(b)-1-i] = b[i]
	}
	return out
}

func u8f(n string) field           { return field{n, fU8, 0} }
func u16f(n string) field          { return field{n, fU16, 0} }
func u32f(n string) field          { return field{n, fU32, 0} }
func bytesf(n string, w int) field { return field{n, fBytes, w} }

// NIB (nwkIB_t), Z-Stack 3.x.
var nibLayout = newLayout("NIB", 0,
	u8f("SequenceNum"), u8f("PassiveAckTimeout"), u8f("MaxBroadcastRetries"), u8f("MaxChildren"),
	u8f("MaxDepth"), u8f("MaxRouters"), u8f("dummyNeighborTable"), u8f("BroadcastDeliveryTime"),
	u8f("ReportConstantCost"), u8f("RouteDiscRetries"), u8f("dummyRoutingTable"), u8f("SecureAllFrames"),
	u8f("SecurityLevel"), u8f("SymLink"), u8f("CapabilityFlags"), u16f("TransactionPersistenceTime"),
	u8f("nwkProtocolVersion"), u8f("RouteDiscoveryTime"), u8f("RouteExpiryTime"), u16f("nwkDevAddress"),
	u8f("nwkLogicalChannel"), u16f("nwkCoordAddress"), bytesf("nwkCoordExtAddress", 8), u16f("nwkPanId"),
	u8f("nwkState"), u32f("channelList"), u8f("beaconOrder"), u8f("superFrameOrder"), u8f("scanDuration"),
	u8f("battLifeExt"), u32f("allocatedRouterAddresses"), u32f("allocatedEndDeviceAddresses"), u8f("nodeDepth"),
	bytesf("extendedPANID", 8), u8f("nwkKeyLoaded"),
	// spare1/spare2 are nwkKeyDesc (uint8 + 16 bytes): no 16-bit members, so
	// their layout is identical in both modes.
	bytesf("spare1", 17), bytesf("spare2", 17),
	u8f("spare3"), u8f("spare4"), u8f("nwkLinkStatusPeriod"), u8f("nwkRouterAgeLimit"), u8f("nwkUseMultiCast"),
	u8f("nwkIsConcentrator"), u8f("nwkConcentratorDiscoveryTime"), u8f("nwkConcentratorRadius"), u8f("nwkAllFresh"),
	u16f("nwkManagerAddr"), u16f("nwkTotalTransmissions"), u8f("nwkUpdateId"),
)

var nwkKeyDescriptorLayout = newLayout("nwkKeyDesc", 0, u8f("keySeqNum"), bytesf("key", 16))

var nwkSecMaterialLayout = newLayout("nwkSecMaterialDesc", 0, u32f("FrameCounter"), bytesf("extendedPANID", 8))

var addressManagerLayout = newLayout("AddrMgrEntry", 0xff, u8f("user"), u16f("nwkAddr"), bytesf("extAddr", 8))

var securityManagerLayout = newLayout("ZDSecMgrEntry", 0, u16f("ami"), u16f("keyNvId"), u8f("authenticationOption"))

var apsLinkKeyDataLayout = newLayout("APSME_LinkKeyData", 0, bytesf("key", 16), u32f("txFrmCntr"), u32f("rxFrmCntr"))

var apsTcLinkKeyLayout = newLayout("APSME_TCLinkKeyNVEntry", 0,
	u32f("txFrmCntr"), u32f("rxFrmCntr"), bytesf("extAddr", 8), u8f("keyAttributes"), u8f("keyType"), u8f("SeedShift_IcIndex"))

// Address manager user flags (ADDRMGR_USER_*).
const (
	addrMgrUserAssoc    = 0x01
	addrMgrUserSecurity = 0x02
)

func addressEntryUsed(r record) bool {
	ext := r.bytes("extAddr")
	return r.u8("user") != 0 && !allBytes(ext, 0) && !allBytes(ext, 0xff)
}

func securityEntryUsed(r record) bool {
	ami := r.u16("ami")
	return ami != 0xfffe && ami != 0xffff && !(ami == 0 && r.u8("authenticationOption") == 0)
}

func emptySecurityEntry() record {
	r := securityManagerLayout.empty()
	r.setU16("ami", 0xfffe)
	return r
}

func allBytes(b []byte, v byte) bool {
	for _, x := range b {
		if x != v {
			return false
		}
	}
	return true
}

// Security manager table is stored as one NV item: uint16 used-count header
// followed by the entries.
func decodeSecurityTable(b []byte, aligned bool) ([]record, error) {
	if len(b) < 2 {
		return nil, fmt.Errorf("security manager table too short")
	}
	size := securityManagerLayout.length(aligned)
	body := b[2:]
	if len(body)%size != 0 {
		return nil, fmt.Errorf("security manager table length %d not divisible by entry length %d", len(body), size)
	}
	out := make([]record, 0, len(body)/size)
	for i := 0; i < len(body); i += size {
		r, err := securityManagerLayout.decode(body[i : i+size])
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func encodeSecurityTable(entries []record, aligned bool) []byte {
	used := 0
	for _, e := range entries {
		if securityEntryUsed(e) {
			used++
		}
	}
	out := []byte{byte(used), byte(used >> 8)}
	for _, e := range entries {
		out = append(out, e.encode(aligned)...)
	}
	return out
}

// Address manager table for Z-Stack 3.0.x is one NV item without header.
func decodeFixedTable(l *layout, b []byte, aligned bool) ([]record, error) {
	size := l.length(aligned)
	if size == 0 || len(b)%size != 0 {
		return nil, fmt.Errorf("%s table length %d not divisible by entry length %d", l.name, len(b), size)
	}
	out := make([]record, 0, len(b)/size)
	for i := 0; i < len(b); i += size {
		r, err := l.decode(b[i : i+size])
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func encodeFixedTable(entries []record, aligned bool) []byte {
	var out []byte
	for _, e := range entries {
		out = append(out, e.encode(aligned)...)
	}
	return out
}

// Channel masks: bit n set means channel n (11..26).
func packChannels(channels []int) (uint32, error) {
	var mask uint32
	for _, c := range channels {
		if c < 11 || c > 26 {
			return 0, fmt.Errorf("unsupported channel %d", c)
		}
		mask |= 1 << uint(c)
	}
	return mask, nil
}

func unpackChannels(mask uint32) []int {
	var out []int
	for c := 11; c <= 26; c++ {
		if mask&(1<<uint(c)) != 0 {
			out = append(out, c)
		}
	}
	return out
}
