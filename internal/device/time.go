package device

import (
	"encoding/binary"
	"fmt"
	"time"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
)

func (r *Registry) TimeResponse(d store.Device, f zcl.Frame, now time.Time) ([]byte, error) {
	p := r.Match(d)
	if p == nil {
		return nil, nil
	}
	if p.Protocol == "builtin" {
		return zcl.TuyaTimeResponse(canonical(d, p), f, now)
	}
	if p.Protocol != "tuya" || p.TimeEpoch == "" || p.TimeEpoch == "off" || f.Control&7 != 1 || f.Command != 0x24 {
		return nil, nil
	}
	if len(f.Payload) != 2 {
		return nil, fmt.Errorf("invalid Tuya time request")
	}
	utc := now.Unix()
	if p.TimeEpoch == "2000" {
		utc -= 946684800
	}
	_, offset := now.Zone()
	local := utc + int64(offset)
	if utc < 0 || local < 0 || utc > 0xffffffff || local > 0xffffffff {
		return nil, fmt.Errorf("clock outside Tuya time range")
	}
	data := make([]byte, 10)
	binary.LittleEndian.PutUint16(data, 8)
	binary.BigEndian.PutUint32(data[2:6], uint32(utc))
	binary.BigEndian.PutUint32(data[6:10], uint32(local))
	return zcl.Header(0x11, f.Seq, 0x24, data), nil
}
