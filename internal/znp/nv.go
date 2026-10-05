package znp

import (
	"bytes"
	"context"
	"fmt"
)

// NV item identifiers (Z-Stack ZComDef.h / zigbee-herdsman NvItemsIds).
const (
	nvExtAddr               uint16 = 0x0001
	nvStartupOption         uint16 = 0x0003
	nvNIB                   uint16 = 0x0021
	nvAddrMgr               uint16 = 0x0023
	nvExtendedPanID         uint16 = 0x002d
	nvActiveKeyInfo         uint16 = 0x003a
	nvAlternKeyInfo         uint16 = 0x003b
	nvApsUseExtPanID        uint16 = 0x0047
	nvApsLinkKeyTable       uint16 = 0x004c
	nvPreCfgKey             uint16 = 0x0062
	nvPreCfgKeysEnable      uint16 = 0x0063
	nvLegacySecMaterial     uint16 = 0x0075 // Z-Stack <= 3.0.x
	nvNwkKey                uint16 = 0x0082
	nvPanID                 uint16 = 0x0083
	nvChanList              uint16 = 0x0084
	nvLogicalType           uint16 = 0x0087
	nvZdoDirectCB           uint16 = 0x008f
	nvTCLKSeed              uint16 = 0x0101
	nvLegacyTCLKTable       uint16 = 0x0111 // Z-Stack <= 3.0.x
	nvApsLinkKeyDataStart   uint16 = 0x0201 // Z-Stack <= 3.0.x
	nvHasConfiguredZStack3  uint16 = 0x0060
	nvSysZStack             byte   = 1
	nvExAddrMgr             uint16 = 0x0001 // Z-Stack 3.x.0 extended tables
	nvExTCLKTable           uint16 = 0x0004
	nvExApsKeyDataTable     uint16 = 0x0006
	nvExNwkSecMaterialTable uint16 = 0x0007
)

// MT SYS command identifiers used for NV access.
const (
	sysResetReq     byte = 0x00
	sysVersion      byte = 0x02
	sysGetExtAddr   byte = 0x04
	sysNvItemInit   byte = 0x07
	sysNvDelete     byte = 0x12
	sysNvLength     byte = 0x13
	sysNvReadExt    byte = 0x1c
	sysNvWriteExt   byte = 0x1d
	sysExNvCreate   byte = 0x30
	sysExNvLength   byte = 0x32
	sysExNvRead     byte = 0x33
	sysExNvWrite    byte = 0x34
	sysResetInd     byte = 0x80
	nvItemCreated   byte = 0x09 // NV_ITEM_UNINIT: item did not exist and was created
	maxNvChunk           = 200
	maxLegacyTables      = 255
	maxExtendedRows      = 1024
)

// nvMemory reads and writes coordinator NV memory. aligned selects the struct
// representation of the target platform (detected from the NWKKEY item).
type nvMemory struct {
	c       *Client
	aligned bool
}

func newNV(ctx context.Context, c *Client) (*nvMemory, error) {
	nv := &nvMemory{c: c}
	key, err := nv.read(ctx, nvNwkKey)
	if err != nil {
		return nil, err
	}
	switch len(key) {
	case 21:
		nv.aligned = false
	case 24:
		nv.aligned = true
	default:
		return nil, fmt.Errorf("cannot determine NV memory alignment: NWKKEY length %d", len(key))
	}
	return nv, nil
}

func (nv *nvMemory) request(ctx context.Context, cmd byte, data []byte) ([]byte, error) {
	f, err := nv.c.Request(ctx, SYS, cmd, data)
	if err != nil {
		return nil, err
	}
	return f.Data, nil
}

func (nv *nvMemory) length(ctx context.Context, id uint16) (int, error) {
	d, err := nv.request(ctx, sysNvLength, U16(id))
	if err != nil {
		return 0, err
	}
	if len(d) < 2 {
		return 0, fmt.Errorf("short NV length reply for item 0x%04x", id)
	}
	return int(le.Uint16(d)), nil
}

// read returns nil, nil when the item does not exist.
func (nv *nvMemory) read(ctx context.Context, id uint16) ([]byte, error) {
	n, err := nv.length(ctx, id)
	if err != nil || n == 0 {
		return nil, err
	}
	out := make([]byte, 0, n)
	for len(out) < n {
		d, err := nv.request(ctx, sysNvReadExt, append(U16(id), U16(uint16(len(out)))...))
		if err != nil {
			return nil, err
		}
		if len(d) < 2 || d[0] != 0 {
			return nil, fmt.Errorf("NV read of item 0x%04x at offset %d failed (reply %x)", id, len(out), d)
		}
		chunk := d[2:]
		if int(d[1]) != len(chunk) || len(chunk) == 0 {
			return nil, fmt.Errorf("NV read of item 0x%04x returned an invalid chunk", id)
		}
		if len(out)+len(chunk) > n {
			chunk = chunk[:n-len(out)]
		}
		out = append(out, chunk...)
	}
	return out, nil
}

func (nv *nvMemory) write(ctx context.Context, id uint16, value []byte) error {
	if len(value) == 0 {
		return fmt.Errorf("refusing empty NV write to item 0x%04x", id)
	}
	n, err := nv.length(ctx, id)
	if err != nil {
		return err
	}
	if n == 0 {
		initial := value
		if len(initial) > maxNvChunk {
			initial = initial[:maxNvChunk]
		}
		p := append(U16(id), U16(uint16(len(value)))...)
		p = append(p, byte(len(initial)))
		p = append(p, initial...)
		d, err := nv.request(ctx, sysNvItemInit, p)
		if err != nil {
			return err
		}
		if len(d) < 1 || (d[0] != 0 && d[0] != nvItemCreated) {
			return fmt.Errorf("NV item 0x%04x could not be created (status %x)", id, d)
		}
	}
	for offset := 0; offset < len(value); {
		end := offset + maxNvChunk
		if end > len(value) {
			end = len(value)
		}
		p := append(U16(id), U16(uint16(offset))...)
		p = append(p, U16(uint16(end-offset))...)
		p = append(p, value[offset:end]...)
		d, err := nv.request(ctx, sysNvWriteExt, p)
		if err != nil {
			return err
		}
		if len(d) < 1 || d[0] != 0 {
			return fmt.Errorf("NV write of item 0x%04x at offset %d failed (status %x)", id, offset, d)
		}
		offset = end
	}
	return nil
}

// update writes only if the stored value differs (saves flash wear).
func (nv *nvMemory) update(ctx context.Context, id uint16, value []byte) error {
	current, err := nv.read(ctx, id)
	if err != nil {
		return err
	}
	if bytes.Equal(current, value) {
		return nil
	}
	return nv.write(ctx, id, value)
}

func (nv *nvMemory) remove(ctx context.Context, id uint16) error {
	n, err := nv.length(ctx, id)
	if err != nil || n == 0 {
		return err
	}
	d, err := nv.request(ctx, sysNvDelete, append(U16(id), U16(uint16(n))...))
	if err != nil {
		return err
	}
	if len(d) < 1 || (d[0] != 0 && d[0] != nvItemCreated) {
		return fmt.Errorf("NV item 0x%04x could not be deleted (status %x)", id, d)
	}
	return nil
}

func exKey(sys byte, item, sub uint16) []byte {
	return append(append([]byte{sys}, U16(item)...), U16(sub)...)
}

func (nv *nvMemory) exLength(ctx context.Context, sys byte, item, sub uint16) (int, error) {
	d, err := nv.request(ctx, sysExNvLength, exKey(sys, item, sub))
	if err != nil {
		return 0, err
	}
	if len(d) == 0 || len(d) > 4 {
		return 0, fmt.Errorf("invalid extended NV length reply %x", d)
	}
	n := 0
	for i := len(d) - 1; i >= 0; i-- {
		n = n<<8 | int(d[i])
	}
	return n, nil
}

// exRead returns nil, nil when the table row does not exist.
func (nv *nvMemory) exRead(ctx context.Context, sys byte, item, sub uint16) ([]byte, error) {
	n, err := nv.exLength(ctx, sys, item, sub)
	if err != nil || n == 0 {
		return nil, err
	}
	out := make([]byte, 0, n)
	for len(out) < n {
		chunk := n - len(out)
		if chunk > maxNvChunk {
			chunk = maxNvChunk
		}
		p := append(exKey(sys, item, sub), U16(uint16(len(out)))...)
		p = append(p, byte(chunk))
		d, err := nv.request(ctx, sysExNvRead, p)
		if err != nil {
			return nil, err
		}
		if len(d) < 2 || d[0] != 0 || int(d[1]) != len(d)-2 || len(d) == 2 {
			return nil, fmt.Errorf("extended NV read %d/0x%04x/%d failed (reply %x)", sys, item, sub, d)
		}
		out = append(out, d[2:]...)
	}
	return out[:n], nil
}

func (nv *nvMemory) exWrite(ctx context.Context, sys byte, item, sub uint16, value []byte) error {
	if len(value) == 0 || len(value) > 240 {
		return fmt.Errorf("invalid extended NV row length %d", len(value))
	}
	n, err := nv.exLength(ctx, sys, item, sub)
	if err != nil {
		return err
	}
	if n == 0 {
		p := append(exKey(sys, item, sub), 0, 0, 0, 0)
		le.PutUint32(p[5:], uint32(len(value)))
		d, err := nv.request(ctx, sysExNvCreate, p)
		if err != nil {
			return err
		}
		if len(d) < 1 || (d[0] != 0 && d[0] != nvItemCreated) {
			return fmt.Errorf("extended NV row %d/0x%04x/%d could not be created (status %x)", sys, item, sub, d)
		}
	}
	p := append(exKey(sys, item, sub), 0, 0, byte(len(value)))
	p = append(p, value...)
	d, err := nv.request(ctx, sysExNvWrite, p)
	if err != nil {
		return err
	}
	if len(d) < 1 || d[0] != 0 {
		return fmt.Errorf("extended NV write %d/0x%04x/%d failed (status %x)", sys, item, sub, d)
	}
	return nil
}

// Tables are rows in consecutive legacy items or extended sub-ids. Reading
// stops at the first missing row.
func (nv *nvMemory) readLegacyTable(ctx context.Context, start uint16, max int) ([][]byte, error) {
	var rows [][]byte
	for i := 0; i < max; i++ {
		row, err := nv.read(ctx, start+uint16(i))
		if err != nil {
			return nil, err
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (nv *nvMemory) readExTable(ctx context.Context, item uint16) ([][]byte, error) {
	var rows [][]byte
	for i := 0; i < maxExtendedRows; i++ {
		row, err := nv.exRead(ctx, nvSysZStack, item, uint16(i))
		if err != nil {
			return nil, err
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func decodeRows(l *layout, rows [][]byte) ([]record, error) {
	out := make([]record, 0, len(rows))
	for i, row := range rows {
		r, err := l.decode(row)
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i, err)
		}
		out = append(out, r)
	}
	return out, nil
}

func (nv *nvMemory) writeLegacyTable(ctx context.Context, start uint16, rows []record) error {
	for i, r := range rows {
		if err := nv.write(ctx, start+uint16(i), r.encode(nv.aligned)); err != nil {
			return err
		}
	}
	return nil
}

func (nv *nvMemory) writeExTable(ctx context.Context, item uint16, rows []record) error {
	for i, r := range rows {
		if err := nv.exWrite(ctx, nvSysZStack, item, uint16(i), r.encode(nv.aligned)); err != nil {
			return err
		}
	}
	return nil
}
