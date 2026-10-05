package znp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type exID struct {
	sys       byte
	item, sub uint16
}

// emulator is a minimal Z-Stack 3.x ZNP: NV memory (legacy and extended),
// soft reset, BDB formation, startup and the commands used by Adapter.Start.
type emulator struct {
	t         *testing.T
	product   byte
	aligned   bool
	mu        sync.Mutex
	nv        map[uint16][]byte
	ex        map[exID][]byte
	factory   []byte // factory IEEE, little-endian
	state     byte
	endpoints []byte
	conn      net.Conn
	writeMu   sync.Mutex
	// collisions makes the next N formations pick a different PAN ID.
	collisions          int
	writes              int
	failWriteID         uint16
	rollbackRestoredKey bool
}

func newEmulator(t *testing.T, product byte, factoryIEEE uint64) (*emulator, *Client) {
	t.Helper()
	host, dev := net.Pipe()
	e := &emulator{t: t, product: product, aligned: product == ProductZStack3x0, nv: map[uint16][]byte{}, ex: map[exID][]byte{}, conn: dev}
	e.factory = make([]byte, 8)
	binary.LittleEndian.PutUint64(e.factory, factoryIEEE)
	e.factoryDefaults()
	go e.serve()
	c := New(host)
	t.Cleanup(func() { c.Close(); dev.Close() })
	return e, c
}

func (e *emulator) rows(l *layout, n int, fill func() record) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		r := l.empty()
		if fill != nil {
			r = fill()
		}
		out[i] = r.encode(e.aligned)
	}
	return out
}

// factoryDefaults: an unconfigured coordinator with empty tables.
func (e *emulator) factoryDefaults() {
	if e.aligned {
		e.nv[nvNwkKey] = make([]byte, 24)
	} else {
		e.nv[nvNwkKey] = make([]byte, 21)
	}
	e.nv[nvHasConfiguredZStack3] = []byte{0}
	sec := make([]record, 16)
	for i := range sec {
		sec[i] = emptySecurityEntry()
	}
	e.nv[nvApsLinkKeyTable] = encodeSecurityTable(sec, e.aligned)
	e.nv[nvActiveKeyInfo] = nwkKeyDescriptorLayout.empty().encode(e.aligned)
	e.nv[nvAlternKeyInfo] = nwkKeyDescriptorLayout.empty().encode(e.aligned)
	if e.product == ProductZStack3x0 {
		for i, r := range e.rows(addressManagerLayout, 16, nil) {
			e.ex[exID{1, nvExAddrMgr, uint16(i)}] = r
		}
		for i, r := range e.rows(apsLinkKeyDataLayout, 16, nil) {
			e.ex[exID{1, nvExApsKeyDataTable, uint16(i)}] = r
		}
		for i, r := range e.rows(apsTcLinkKeyLayout, 16, nil) {
			e.ex[exID{1, nvExTCLKTable, uint16(i)}] = r
		}
		for i, r := range e.rows(nwkSecMaterialLayout, 4, nil) {
			e.ex[exID{1, nvExNwkSecMaterialTable, uint16(i)}] = r
		}
	} else {
		e.nv[nvAddrMgr] = bytes.Join(e.rows(addressManagerLayout, 16, nil), nil)
		for i, r := range e.rows(apsLinkKeyDataLayout, 16, nil) {
			e.nv[nvApsLinkKeyDataStart+uint16(i)] = r
		}
		for i, r := range e.rows(apsTcLinkKeyLayout, 16, nil) {
			e.nv[nvLegacyTCLKTable+uint16(i)] = r
		}
		for i, r := range e.rows(nwkSecMaterialLayout, 4, nil) {
			e.nv[nvLegacySecMaterial+uint16(i)] = r
		}
	}
}

func (e *emulator) send(cmd0, cmd1 byte, data []byte) {
	b, err := (Frame{cmd0, cmd1, data}).Bytes()
	if err != nil {
		e.t.Errorf("emulator frame: %v", err)
		return
	}
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	_, _ = e.conn.Write(b)
}

func (e *emulator) ieee() []byte {
	if v, ok := e.nv[nvExtAddr]; ok && len(v) == 8 {
		return append([]byte{}, v...)
	}
	return append([]byte{}, e.factory...)
}

func (e *emulator) nib() (record, bool) {
	raw, ok := e.nv[nvNIB]
	if !ok {
		return record{}, false
	}
	r, err := nibLayout.decode(raw)
	if err != nil {
		e.t.Errorf("emulator NIB: %v", err)
		return record{}, false
	}
	return r, true
}

func (e *emulator) serve() {
	for {
		f, err := ReadFrame(e.conn)
		if err != nil {
			if err != io.EOF && err != io.ErrClosedPipe {
				return
			}
			return
		}
		e.handle(f)
	}
}

func (e *emulator) handle(f Frame) {
	e.mu.Lock()
	defer e.mu.Unlock()
	sub, cmd, d := f.Cmd0&0x1f, f.Cmd1, f.Data
	srsp := func(data ...byte) { e.send(0x60|sub, cmd, data) }
	areq := func(sub, cmd byte, data []byte) {
		go func() { time.Sleep(time.Millisecond); e.send(0x40|sub, cmd, data) }()
	}
	switch {
	case f.Cmd0 == 0x41 && cmd == sysResetReq:
		if e.rollbackRestoredKey && len(e.nv[nvExtAddr]) == 8 {
			if e.product == ProductZStack3x0 {
				id := exID{1, nvExApsKeyDataTable, 0}
				r, _ := apsLinkKeyDataLayout.decode(e.ex[id])
				r.setU32("txFrmCntr", 0)
				e.ex[id] = r.encode(e.aligned)
			} else {
				r, _ := apsLinkKeyDataLayout.decode(e.nv[nvApsLinkKeyDataStart])
				r.setU32("txFrmCntr", 0)
				e.nv[nvApsLinkKeyDataStart] = r.encode(e.aligned)
			}
		}
		if v := e.nv[nvStartupOption]; len(v) == 1 && v[0]&startupOptionClearAll != 0 {
			// Z-Stack resets network state: NIB, keys and device tables
			// (rows keep their capacity), as on a real coordinator.
			delete(e.nv, nvNIB)
			delete(e.nv, nvExtAddr)
			keepFlag := e.nv[nvHasConfiguredZStack3]
			key := e.nv[nvNwkKey]
			for id := range e.nv {
				if id != nvStartupOption && id != nvHasConfiguredZStack3 {
					delete(e.nv, id)
				}
			}
			for id := range e.ex {
				delete(e.ex, id)
			}
			e.factoryDefaults()
			e.nv[nvHasConfiguredZStack3] = keepFlag
			e.nv[nvNwkKey] = key
		}
		e.state = 0
		areq(SYS, sysResetInd, []byte{2, 2, e.product, 2, 7, 1})
	case sub == SYS && cmd == sysVersion:
		srsp(2, e.product, 2, 7, 1, 0x30, 0x34, 0x21, 0x01)
	case sub == SYS && cmd == sysGetExtAddr:
		srsp(e.ieee()...)
	case sub == SYS && cmd == sysNvLength:
		v := e.nv[le.Uint16(d)]
		srsp(byte(len(v)), byte(len(v)>>8))
	case sub == SYS && cmd == 0x08: // osalNvRead
		v, ok := e.nv[le.Uint16(d)]
		if !ok {
			srsp(1, 0)
			return
		}
		v = v[d[2]:]
		srsp(append([]byte{0, byte(len(v))}, v...)...)
	case sub == SYS && cmd == sysNvReadExt:
		v, ok := e.nv[le.Uint16(d)]
		off := int(le.Uint16(d[2:]))
		if !ok || off >= len(v) {
			srsp(1, 0)
			return
		}
		v = v[off:]
		if len(v) > 240 {
			v = v[:240]
		}
		srsp(append([]byte{0, byte(len(v))}, v...)...)
	case sub == SYS && cmd == sysNvItemInit:
		id, n := le.Uint16(d), int(le.Uint16(d[2:]))
		if _, ok := e.nv[id]; ok {
			srsp(0)
			return
		}
		v := make([]byte, n)
		copy(v, d[5:5+int(d[4])])
		e.nv[id] = v
		srsp(nvItemCreated)
	case sub == SYS && cmd == sysNvWriteExt:
		id, off, n := le.Uint16(d), int(le.Uint16(d[2:])), int(le.Uint16(d[4:]))
		if id == e.failWriteID && e.failWriteID != 0 {
			srsp(0x0a)
			return
		}
		v, ok := e.nv[id]
		if !ok || off+n > len(v) {
			srsp(0x0a)
			return
		}
		copy(v[off:], d[6:6+n])
		e.writes++
		srsp(0)
	case sub == SYS && cmd == sysNvDelete:
		delete(e.nv, le.Uint16(d))
		srsp(0)
	case sub == SYS && cmd == sysExNvLength:
		v := e.ex[exID{d[0], le.Uint16(d[1:]), le.Uint16(d[3:])}]
		srsp(byte(len(v)), 0, 0, 0)
	case sub == SYS && cmd == sysExNvRead:
		v, ok := e.ex[exID{d[0], le.Uint16(d[1:]), le.Uint16(d[3:])}]
		off, n := int(le.Uint16(d[5:])), int(d[7])
		if !ok || off+n > len(v) {
			srsp(1, 0)
			return
		}
		srsp(append([]byte{0, byte(n)}, v[off:off+n]...)...)
	case sub == SYS && cmd == sysExNvCreate:
		id := exID{d[0], le.Uint16(d[1:]), le.Uint16(d[3:])}
		e.ex[id] = make([]byte, le.Uint32(d[5:]))
		srsp(0)
	case sub == SYS && cmd == sysExNvWrite:
		id := exID{d[0], le.Uint16(d[1:]), le.Uint16(d[3:])}
		off, n := int(le.Uint16(d[5:])), int(d[7])
		v, ok := e.ex[id]
		if !ok || off+n > len(v) {
			srsp(0x0a)
			return
		}
		copy(v[off:], d[8:8+n])
		e.writes++
		srsp(0)
	case sub == appCnf && cmd == appCnfBdbSetChannel:
		if d[0] == 1 {
			e.nv[nvChanList] = append([]byte{}, d[1:5]...)
		}
		srsp(0)
	case sub == appCnf && cmd == appCnfBdbStartCommission:
		srsp(0)
		e.form()
		areq(ZDO, zdoStateChangeInd, []byte{devStateCoordinator})
	case sub == ZDO && cmd == zdoStartupFromApp:
		srsp(0)
		e.state = devStateCoordinator
		areq(ZDO, zdoStateChangeInd, []byte{devStateCoordinator})
	case sub == ZDO && cmd == zdoExtNwkInfo:
		out := make([]byte, 24)
		le.PutUint16(out[3:], 0xffff)
		if n, ok := e.nib(); ok {
			out[2] = e.state
			le.PutUint16(out[3:], n.u16("nwkPanId"))
			copy(out[7:15], n.bytes("extendedPANID"))
			out[23] = n.u8("nwkLogicalChannel")
		}
		srsp(out...)
	case sub == UTIL && cmd == utilGetDeviceInfo:
		out := append([]byte{0}, e.ieee()...)
		out = append(out, 0, 0, 0x07, e.state, 0)
		srsp(out...)
	case sub == AF && cmd == 0x00: // AF_REGISTER
		e.endpoints = append(e.endpoints, d[0])
		srsp(0)
	case sub == ZDO && cmd == 0x36: // permit join
		srsp(0)
	case sub == ZDO && cmd == 0x05: // active endpoints
		srsp(0)
		areq(ZDO, 0x85, append([]byte{0, 0, 0, 0, 0, byte(len(e.endpoints))}, e.endpoints...))
	case sub == ZDO && cmd == 0x04: // simple descriptor
		srsp(0)
		areq(ZDO, 0x84, []byte{0, 0, 0, 0, 0, 8, d[4], 0x04, 0x01, 0x05, 0x00, 0, 0, 0})
	default:
		e.t.Errorf("emulator: unhandled command %02x/%02x", f.Cmd0, f.Cmd1)
		srsp(0xff)
	}
}

// form emulates BDB network formation from the commissioning NV items.
func (e *emulator) form() {
	r := nibLayout.empty()
	r.setU8("SecurityLevel", 5)
	pan := le.Uint16(e.nv[nvPanID])
	if e.collisions > 0 {
		e.collisions--
		pan ^= 0x5a5a
	}
	r.setU16("nwkPanId", pan)
	mask := le.Uint32(e.nv[nvChanList])
	r.setU32("channelList", mask)
	r.setU8("nwkLogicalChannel", byte(unpackChannels(mask)[0]))
	r.setBytes("extendedPANID", e.nv[nvExtendedPanID])
	e.nv[nvNIB] = r.encode(e.aligned)
	key := nwkKeyDescriptorLayout.empty()
	key.setBytes("key", e.nv[nvPreCfgKey])
	e.nv[nvActiveKeyInfo] = key.encode(e.aligned)
	e.nv[nvAlternKeyInfo] = key.encode(e.aligned)
	e.state = devStateCoordinator
}

// pair simulates devices joining: address entries, link keys and counters.
func (e *emulator) pair(t *testing.T, seed []byte) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.aligned {
		e.ex[exID{1, nvExNwkSecMaterialTable, 0}] = func() []byte {
			r := nwkSecMaterialLayout.empty()
			r.setU32("FrameCounter", 4321)
			n, _ := e.nib()
			r.setBytes("extendedPANID", n.bytes("extendedPANID"))
			return r.encode(true)
		}()
	} else {
		r := nwkSecMaterialLayout.empty()
		r.setU32("FrameCounter", 4321)
		n, _ := e.nib()
		r.setBytes("extendedPANID", n.bytes("extendedPANID"))
		e.nv[nvLegacySecMaterial] = r.encode(false)
	}
	e.nv[nvTCLKSeed] = append([]byte{}, seed...)
	addr := []record{}
	set := func(user byte, nwk uint16, ieee string) {
		r := addressManagerLayout.empty()
		r.setU8("user", user)
		r.setU16("nwkAddr", nwk)
		b, _ := hexBytes(ieee, 8, "ieee")
		r.setReversed("extAddr", b)
		addr = append(addr, r)
	}
	set(addrMgrUserAssoc, 0x1111, "00158d0001a2b3c4")                     // Xiaomi end device, no unique key
	set(addrMgrUserAssoc|addrMgrUserSecurity, 0x2222, "a4c1380000000001") // Tuya router, key in data table
	set(addrMgrUserSecurity, 0x3333, "0c4314fffe000002")                  // ZB 3.0 router, seed-derived key
	for len(addr) < 16 {
		addr = append(addr, addressManagerLayout.empty())
	}
	sec := make([]record, 16)
	for i := range sec {
		sec[i] = emptySecurityEntry()
	}
	sec[0].setU16("ami", 1)
	keyIndex := uint16(3)
	if e.product == ProductZStack30x {
		keyIndex += nvApsLinkKeyDataStart
	}
	sec[0].setU16("keyNvId", keyIndex)
	sec[0].setU8("authenticationOption", 1)
	e.nv[nvApsLinkKeyTable] = encodeSecurityTable(sec, e.aligned)
	kd := apsLinkKeyDataLayout.empty()
	kd.setBytes("key", []byte("0123456789abcdef"))
	kd.setU32("txFrmCntr", 77)
	kd.setU32("rxFrmCntr", 66)
	tc := apsTcLinkKeyLayout.empty()
	ieee3, _ := hexBytes("0c4314fffe000002", 8, "ieee")
	tc.setReversed("extAddr", ieee3)
	tc.setU8("SeedShift_IcIndex", 5)
	tc.setU8("keyAttributes", 2)
	tc.setU32("txFrmCntr", 12)
	tc.setU32("rxFrmCntr", 34)
	if e.product == ProductZStack3x0 {
		for i, r := range addr {
			e.ex[exID{1, nvExAddrMgr, uint16(i)}] = r.encode(true)
		}
		e.ex[exID{1, nvExApsKeyDataTable, 3}] = kd.encode(true)
		e.ex[exID{1, nvExTCLKTable, 0}] = tc.encode(true)
	} else {
		e.nv[nvAddrMgr] = encodeFixedTable(addr, false)
		e.nv[nvApsLinkKeyDataStart+3] = kd.encode(false)
		e.nv[nvLegacyTCLKTable] = tc.encode(false)
	}
}

func (e *emulator) String() string {
	return fmt.Sprintf("emulator(product=%d aligned=%v)", e.product, e.aligned)
}
