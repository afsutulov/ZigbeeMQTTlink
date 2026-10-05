package znp

import (
	"context"
	"encoding/binary"
	"fmt"
	"go.bug.st/serial"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/store"
)

const SYS byte = 1
const AF byte = 4
const ZDO byte = 5
const UTIL byte = 7

var le = binary.LittleEndian

func U16(n uint16) []byte { b := make([]byte, 2); le.PutUint16(b, n); return b }
func Addr(b []byte) string {
	if len(b) != 8 {
		return ""
	}
	return fmt.Sprintf("0x%016x", le.Uint64(b))
}

// maxInFlight limits concurrent AF_DATA_REQUESTs awaiting AF_DATA_CONFIRM.
// Z-Stack buffers several requests; a slow sleeping device must not block
// commands to every other device, as a global mutex did.
const maxInFlight = 4

type Adapter struct {
	ExpectedIEEE string
	C            *Client
	transMu      sync.Mutex
	inFlight     chan struct{}
	trans        byte
	transactions [256]transactionSlot // guarded by transMu
	IEEE         string
	PAN          uint16
	Channel      byte
	Product      byte
}

type transactionSlot struct {
	active bool
	after  time.Time
}

// Reserve IDs while requests are active. Quarantine unconfirmed IDs for
// 30 seconds so a late AF_DATA_CONFIRM cannot acknowledge a new command.
func (a *Adapter) reserveTransaction(ctx context.Context) (byte, error) {
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		now := time.Now()
		a.transMu.Lock()
		for i := 0; i < 256; i++ {
			a.trans++
			id := a.trans
			if slot := &a.transactions[id]; !slot.active && !now.Before(slot.after) {
				slot.active = true
				a.transMu.Unlock()
				return id, nil
			}
		}
		a.transMu.Unlock()
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-a.C.Done():
			timer.Stop()
			return 0, a.C.Err()
		}
	}
}

func (a *Adapter) releaseTransaction(id byte, confirmed bool) {
	a.transMu.Lock()
	defer a.transMu.Unlock()
	a.transactions[id].active = false
	if !confirmed {
		a.transactions[id].after = time.Now().Add(30 * time.Second)
	}
}

func Open(ctx context.Context, s config.Serial) (*Adapter, error) {
	var rw io.ReadWriteCloser
	var e error
	if strings.HasPrefix(s.Port, "tcp://") {
		rw, e = (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", strings.TrimPrefix(s.Port, "tcp://"))
	} else {
		rw, e = serial.Open(s.Port, &serial.Mode{BaudRate: s.Baudrate, DataBits: 8, Parity: serial.NoParity, StopBits: serial.OneStopBit, InitialStatusBits: &serial.ModemOutputBits{DTR: false, RTS: false}})
	}
	if e != nil {
		return nil, e
	}
	return &Adapter{C: New(rw), inFlight: make(chan struct{}, maxInFlight)}, nil
}
func (a *Adapter) Start(ctx context.Context) error {
	f, e := a.C.Request(ctx, SYS, 2, nil)
	if e != nil {
		return e
	}
	if len(f.Data) < 5 {
		return fmt.Errorf("short SYS_VERSION")
	}
	a.Product = f.Data[1]
	if a.Product > 2 {
		return fmt.Errorf("unimplemented Z-Stack product %d", a.Product)
	}
	marker := uint16(0x60)
	if a.Product == 0 {
		marker = 0xf00
	}
	p := append(U16(marker), 0)
	f, e = a.C.Request(ctx, SYS, 8, p)
	if e != nil {
		return e
	}
	if len(f.Data) != 3 || f.Data[0] != 0 || f.Data[1] != 1 || f.Data[2] != 0x55 {
		return fmt.Errorf("coordinator has no ready network: run once with -form-network to create a new one or -restore <backup.json> to restore one; after an interrupted restore repeat -restore with the same backup; after interrupted formation inspect/back up the stored network, or deliberately replace it with -form-network -force")
	}
	f, e = a.C.Request(ctx, UTIL, 0, nil)
	if e != nil {
		return e
	}
	if e = Status(f); e != nil {
		return e
	}
	if len(f.Data) < 14 {
		return fmt.Errorf("short UTIL_GET_DEVICE_INFO")
	}
	a.IEEE = Addr(f.Data[1:9])
	if a.ExpectedIEEE != "" && a.ExpectedIEEE != a.IEEE {
		return fmt.Errorf("coordinator differs from the device database")
	}
	if f.Data[11]&1 == 0 {
		return fmt.Errorf("coordinator firmware required")
	}
	if f.Data[12] != 9 {
		// startupFromApp may return FAILURE while transitioning; poll state instead of assuming SRSP means ready.
		f, e = a.C.Request(ctx, ZDO, 0x40, U16(100))
		if e != nil {
			return e
		}
		if len(f.Data) < 1 || f.Data[0] > 1 {
			return fmt.Errorf("startup failed")
		}
		for {
			f, e = a.C.Request(ctx, UTIL, 0, nil)
			if e != nil {
				return e
			}
			if len(f.Data) >= 14 && f.Data[0] == 0 && f.Data[12] == 9 {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	f, e = a.C.Request(ctx, ZDO, 0x50, nil)
	if e != nil {
		return e
	}
	if len(f.Data) < 24 || f.Data[2] != 9 {
		return fmt.Errorf("coordinator network is not active")
	}
	a.PAN = le.Uint16(f.Data[3:5])
	a.Channel = f.Data[23]
	if a.Channel < 11 || a.Channel > 26 {
		return fmt.Errorf("invalid network channel")
	}
	eps, e := a.ActiveEndpoints(ctx, 0)
	if e != nil {
		return e
	}
	exists := false
	for _, ep := range eps {
		if ep == 1 {
			exists = true
		}
	}
	if exists {
		ep, e := a.Descriptor(ctx, 0, 1)
		if e != nil {
			return e
		}
		if ep.Profile != 0x104 || ep.DeviceID != 5 {
			return fmt.Errorf("existing coordinator endpoint 1 is incompatible")
		}
	} else {
		f, e = a.C.Request(ctx, AF, 0, []byte{1, 4, 1, 5, 0, 0, 0, 0, 0})
		if e != nil {
			return e
		}
		if e = Status(f); e != nil {
			return e
		}
	}
	return a.PermitJoin(ctx, 0)
}
func (a *Adapter) PermitJoin(ctx context.Context, seconds byte) error {
	if seconds > 254 {
		return fmt.Errorf("permit join duration must be 0..254")
	}
	f, e := a.C.Request(ctx, ZDO, 0x36, []byte{0x0f, 0xfc, 0xff, seconds, 1})
	if e != nil {
		return e
	}
	return Status(f)
}

func (a *Adapter) Leave(ctx context.Context, d store.Device) error {
	if d.Network >= 0xfff8 {
		return fmt.Errorf("network address unknown; use force=true to forget the device locally")
	}
	id, e := ieeeBytes(d.IEEE)
	if e != nil {
		return e
	}
	p := append(U16(d.Network), id...)
	p = append(p, 0)
	f, e := a.C.Request(ctx, ZDO, 0x34, p)
	if e != nil {
		return e
	}
	// SRSP confirms coordinator acceptance, not delivery to sleeping/offline hardware.
	return Status(f)
}
func (a *Adapter) ActiveEndpoints(ctx context.Context, n uint16) ([]byte, error) {
	p := append(U16(n), U16(n)...)
	f, e := a.C.Exchange(ctx, ZDO, 5, p, ZDO, 0x85, func(b []byte) bool { return len(b) >= 5 && le.Uint16(b[:2]) == n && le.Uint16(b[3:5]) == n })
	if e != nil {
		return nil, e
	}
	b := f.Data
	if len(b) < 6 || b[2] != 0 || len(b) != 6+int(b[5]) {
		return nil, fmt.Errorf("invalid active endpoint response")
	}
	return append([]byte(nil), b[6:]...), nil
}
func (a *Adapter) Descriptor(ctx context.Context, n uint16, ep byte) (store.Endpoint, error) {
	p := append(U16(n), U16(n)...)
	p = append(p, ep)
	f, e := a.C.Exchange(ctx, ZDO, 4, p, ZDO, 0x84, func(b []byte) bool {
		return len(b) >= 5 && le.Uint16(b[:2]) == n && le.Uint16(b[3:5]) == n && (b[2] != 0 || (len(b) > 6 && b[6] == ep))
	})
	if e != nil {
		return store.Endpoint{}, e
	}
	return ParseDescriptor(f.Data)
}
func ParseDescriptor(b []byte) (store.Endpoint, error) {
	ep := store.Endpoint{}
	if len(b) < 13 || b[2] != 0 || int(b[5])+6 != len(b) {
		return ep, fmt.Errorf("invalid simple descriptor response")
	}
	ep.ID = b[6]
	ep.Profile = le.Uint16(b[7:9])
	ep.DeviceID = le.Uint16(b[9:11])
	i := 13
	ni := int(b[12])
	if i+2*ni >= len(b) {
		return ep, fmt.Errorf("truncated input clusters")
	}
	for j := 0; j < ni; j++ {
		ep.In = append(ep.In, le.Uint16(b[i:i+2]))
		i += 2
	}
	no := int(b[i])
	i++
	if i+2*no != len(b) {
		return ep, fmt.Errorf("truncated output clusters")
	}
	for j := 0; j < no; j++ {
		ep.Out = append(ep.Out, le.Uint16(b[i:i+2]))
		i += 2
	}
	return ep, nil
}
func (a *Adapter) Send(ctx context.Context, n uint16, ep byte, cluster uint16, zcl []byte) error {
	if ep == 0 || ep > 240 || n == 0 || n >= 0xfff8 || len(zcl) > 240 {
		if n == UnknownNetwork {
			return fmt.Errorf("network address unknown; wait for the device to announce itself")
		}
		return fmt.Errorf("invalid AF destination/payload")
	}
	select {
	case a.inFlight <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-a.C.Done():
		return a.C.Err()
	}
	defer func() { <-a.inFlight }()
	id, e := a.reserveTransaction(ctx)
	if e != nil {
		return e
	}
	confirmed := false
	defer func() { a.releaseTransaction(id, confirmed) }()
	p := append(U16(n), ep, 1)
	p = append(p, U16(cluster)...)
	p = append(p, id, 0x10, 30, byte(len(zcl)))
	p = append(p, zcl...)
	f, e := a.C.Exchange(ctx, AF, 1, p, AF, 0x80, func(b []byte) bool { return len(b) >= 3 && b[1] == 1 && b[2] == id })
	if e != nil {
		return e
	}
	confirmed = true
	return Status(f)
}

type Incoming struct {
	Network   uint16
	Endpoint  byte
	Cluster   uint16
	LQI       byte
	ZCL       []byte
	MACSource *uint16
	Radius    *byte
}

func ParseIncoming(b []byte) (Incoming, error) {
	const headerSize = 17
	if len(b) < headerSize {
		return Incoming{}, fmt.Errorf("truncated AF_INCOMING_MSG header: received %d, need %d bytes", len(b), headerSize)
	}
	end := headerSize + int(b[16])
	if end > len(b) {
		return Incoming{}, fmt.Errorf("truncated AF_INCOMING_MSG data: declared %d, available %d bytes", b[16], len(b)-headerSize)
	}
	// TI MT_AfIncomingMsg appends macSrcAddr (2 bytes) and radius (1 byte).
	// Legacy firmware omits this metadata; it must never be passed to ZCL.
	trailerSize := len(b) - end
	if trailerSize != 0 && trailerSize != 3 {
		return Incoming{}, fmt.Errorf("unsupported AF_INCOMING_MSG trailer: %d bytes; declared data %d, frame size %d", trailerSize, b[16], len(b))
	}
	in := Incoming{Network: le.Uint16(b[4:6]), Endpoint: b[6], Cluster: le.Uint16(b[2:4]), LQI: b[9], ZCL: append([]byte(nil), b[headerSize:end]...)}
	if trailerSize == 3 {
		macSource, radius := le.Uint16(b[end:end+2]), b[end+2]
		in.MACSource, in.Radius = &macSource, &radius
	}
	return in, nil
}

// UnknownNetwork marks a device whose short address was taken over by another
// device; it becomes valid again on the next device announce.
const UnknownNetwork uint16 = 0xfffe

// Bind asks the device to bind cluster reports from ep to the coordinator endpoint 1
// (ZDO_BIND_REQ / ZDO_BIND_RSP).
func (a *Adapter) Bind(ctx context.Context, n uint16, deviceIEEE string, ep byte, cluster uint16) error {
	if n == 0 || n >= 0xfff8 || ep == 0 || ep > 240 {
		return fmt.Errorf("invalid bind source")
	}
	src, e := ieeeBytes(deviceIEEE)
	if e != nil {
		return e
	}
	dst, e := ieeeBytes(a.IEEE)
	if e != nil {
		return fmt.Errorf("coordinator IEEE unknown: %w", e)
	}
	p := append(U16(n), src...)
	p = append(p, ep)
	p = append(p, U16(cluster)...)
	p = append(p, 3) // Addr64Bit
	p = append(p, dst...)
	p = append(p, 1)
	f, e := a.C.Exchange(ctx, ZDO, 0x21, p, ZDO, 0xa1, func(b []byte) bool { return len(b) >= 3 && le.Uint16(b[:2]) == n })
	if e != nil {
		return e
	}
	if len(f.Data) < 3 || f.Data[2] != 0 {
		return fmt.Errorf("bind rejected by device")
	}
	return nil
}

// IEEEAddress asks the device at short address n for its IEEE address
// (ZDO_IEEE_ADDR_REQ / ZDO_IEEE_ADDR_RSP). It recovers the mapping of a
// device whose short address changed while its announce was missed.
func (a *Adapter) IEEEAddress(ctx context.Context, n uint16) (string, error) {
	if n == 0 || n >= 0xfff8 {
		return "", fmt.Errorf("invalid network address")
	}
	p := append(U16(n), 0, 0) // single device response, start index 0
	f, e := a.C.Exchange(ctx, ZDO, 1, p, ZDO, 0x81, func(b []byte) bool {
		return len(b) >= 11 && le.Uint16(b[9:11]) == n
	})
	if e != nil {
		return "", e
	}
	if f.Data[0] != 0 {
		return "", fmt.Errorf("IEEE address request status 0x%02x", f.Data[0])
	}
	id := Addr(f.Data[1:9])
	if !config.IEEE(id) {
		return "", fmt.Errorf("invalid IEEE address in response")
	}
	return id, nil
}

func ieeeBytes(s string) ([]byte, error) {
	if !config.IEEE(s) {
		return nil, fmt.Errorf("invalid IEEE address")
	}
	id, e := strconv.ParseUint(s[2:], 16, 64)
	if e != nil {
		return nil, e
	}
	b := make([]byte, 8)
	le.PutUint64(b, id)
	return b, nil
}

func (a *Adapter) Dropped() uint64       { return a.C.Dropped() }
func (a *Adapter) Events() <-chan Frame  { return a.C.Events() }
func (a *Adapter) Done() <-chan struct{} { return a.C.Done() }
func (a *Adapter) Err() error            { return a.C.Err() }
func (a *Adapter) Close() error          { return a.C.Close() }
