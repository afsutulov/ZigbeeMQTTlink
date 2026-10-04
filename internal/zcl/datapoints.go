package zcl

import (
	"zigbeemqttlink/internal/store"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"
	"unicode/utf8"
)

type Datapoint struct {
	ID    byte   `json:"id"`
	Type  byte   `json:"type"`
	Value any    `json:"value"`
	Raw   string `json:"raw"`
}

func datapoints(p []byte) ([]Datapoint, error) {
	if len(p) > 247 {
		return nil, fmt.Errorf("Tuya datapoints exceed unfragmented payload size")
	}
	if len(p) < 2 {
		return nil, fmt.Errorf("short Tuya sequence")
	}
	p = p[2:]
	values := []Datapoint{}
	for len(p) > 0 {
		if len(p) < 4 {
			return nil, fmt.Errorf("short Tuya datapoint header")
		}
		id, typ, n := p[0], p[1], int(binary.BigEndian.Uint16(p[2:4]))
		p = p[4:]
		if n > len(p) {
			return nil, fmt.Errorf("truncated Tuya datapoint %d", id)
		}
		data := p[:n]
		p = p[n:]
		var v any
		switch typ {
		case 0:
			v = hex.EncodeToString(data)
		case 1:
			if n != 1 || data[0] > 1 {
				return nil, fmt.Errorf("invalid Tuya boolean")
			}
			v = data[0] == 1
		case 2:
			if n != 4 {
				return nil, fmt.Errorf("Tuya value must be 4 bytes")
			}
			v = uint64(binary.BigEndian.Uint32(data))
		case 3:
			if !utf8.Valid(data) {
				return nil, fmt.Errorf("invalid Tuya text")
			}
			v = string(data)
		case 4:
			if n != 1 {
				return nil, fmt.Errorf("Tuya enum must be one byte")
			}
			v = uint64(data[0])
		case 5:
			if n < 1 || n > 4 {
				return nil, fmt.Errorf("invalid Tuya bitmap length")
			}
			x := uint64(0)
			for _, b := range data {
				x = x<<8 | uint64(b)
			}
			v = x
		default:
			v = hex.EncodeToString(data) // unknown type remains explicitly raw
		}
		values = append(values, Datapoint{id, typ, v, hex.EncodeToString(data)})
	}
	return values, nil
}

func tuyaDPState(d store.Device, f Frame) (map[string]any, error) {
	out := map[string]any{}
	if f.Control&3 != 1 || f.Control&4 != 0 || (f.Command != 1 && f.Command != 2) {
		return out, nil
	}
	values, err := datapoints(f.Payload)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return out, nil
	}
	out["tuya_datapoints"] = values
	mode := tuyaClimateManufacturers[d.Manufacturer]
	for _, dp := range values {
		v, ok := numeric(dp.Value)
		if !ok || mode == 0 {
			continue
		}
		switch dp.ID {
		case 1:
			if dp.Type != 2 {
				continue
			}
			x := int64(v)
			if x > 0x7fffffff {
				x -= 0x100000000
			} else if mode == 1 && x > 0x2000 {
				x -= 0x10000
			}
			if x >= -650 && x <= 1000 {
				out["temperature"] = float64(x) / 10
			}
		case 2:
			if dp.Type != 2 {
				continue
			}
			if mode == 4 || d.Manufacturer == "_TZE200_bjawzodf" || d.Manufacturer == "_TZE200_zl1kmjqx" {
				v /= 10
			}
			if v >= 0 && v <= 100 {
				out["humidity"] = v
			}
		case 3:
			if dp.Type != 4 {
				continue
			}
			if mode == 1 {
				if s := map[float64]string{0: "low", 1: "middle", 2: "high"}[v]; s != "" {
					out["battery_level"] = s
					out["battery_low"] = v == 0
				}
			}
			if mode == 2 {
				if s := map[float64]string{0: "low", 1: "medium", 2: "high"}[v]; s != "" {
					out["battery_state"] = s
				}
			}
		case 4:
			if (mode == 1 || mode == 3 || mode == 4) && dp.Type == 2 && v <= 100 {
				out["battery"] = v
			}
		case 9:
			if mode != 1 && dp.Type == 4 {
				if s := map[float64]string{0: "celsius", 1: "fahrenheit"}[v]; s != "" {
					out["temperature_unit"] = s
				}
			}
		}
	}
	return out, nil
}

func tuyaGet(d store.Device, p map[string]any) ([]Command, error) {
	if len(p) == 0 {
		return nil, fmt.Errorf("empty get")
	}
	for key := range p {
		if key == "tuya_datapoints" {
			continue
		}
		if tuyaClimateManufacturers[d.Manufacturer] == 0 || (key != "temperature" && key != "humidity" && key != "battery" && key != "battery_state" && key != "battery_level" && key != "battery_low" && key != "temperature_unit") {
			return nil, fmt.Errorf("Tuya property %q requires a supported manufacturer fingerprint", key)
		}
	}
	ep, err := endpoint(d, 0xef00)
	if err != nil {
		return nil, err
	}
	return []Command{{Endpoint: ep, Cluster: 0xef00, Control: 0x11, ID: 3}}, nil
}

func TuyaTimeResponse(d store.Device, f Frame, now time.Time) ([]byte, error) {
	mode := tuyaClimateManufacturers[d.Manufacturer]
	if d.Model != "TS0601" || (mode != 2 && mode != 3) || f.Control&7 != 1 || f.Command != 0x24 {
		return nil, nil
	}
	if len(f.Payload) != 2 {
		return nil, fmt.Errorf("invalid Tuya time request")
	}
	utc := now.Unix()
	_, offset := now.Zone()
	if utc < 0 || utc+int64(offset) < 0 || utc > 0xffffffff || utc+int64(offset) > 0xffffffff {
		return nil, fmt.Errorf("clock outside Tuya time range")
	}
	p := make([]byte, 10)
	le.PutUint16(p, 8)
	binary.BigEndian.PutUint32(p[2:6], uint32(utc))
	binary.BigEndian.PutUint32(p[6:10], uint32(utc+int64(offset)))
	return Header(0x11, f.Seq, 0x24, p), nil
}
