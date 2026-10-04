package zcl

import (
	"zigbeemqttlink/internal/store"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

var le = binary.LittleEndian

type Frame struct {
	Control      byte
	Manufacturer uint16
	Seq          byte
	Command      byte
	Payload      []byte
}

func Parse(b []byte) (Frame, error) {
	f := Frame{}
	if len(b) < 3 || len(b) > 250 {
		return f, fmt.Errorf("short ZCL header")
	}
	f.Control = b[0]
	i := 1
	if f.Control&4 != 0 {
		if len(b) < 5 {
			return f, fmt.Errorf("short manufacturer ZCL header")
		}
		f.Manufacturer = le.Uint16(b[1:3])
		i = 3
	}
	f.Seq = b[i]
	f.Command = b[i+1]
	f.Payload = b[i+2:]
	return f, nil
}
func Header(control, seq, cmd byte, p []byte) []byte { return append([]byte{control, seq, cmd}, p...) }
func u16(n uint16) []byte                            { b := make([]byte, 2); le.PutUint16(b, n); return b }

type Attribute struct {
	ID    uint16
	Type  byte
	Value any
}

func Attributes(f Frame) ([]Attribute, error) {
	return attributes(f, false)
}
func attributes(f Frame, lumi bool) ([]Attribute, error) {
	return attributesWithQuirks(f, lumi, false)
}

func attributesWithQuirks(f Frame, lumi, tuyaTiming bool) ([]Attribute, error) {
	if f.Control&3 != 0 || f.Control&4 != 0 || !(f.Command == 0x0a || f.Command == 1) {
		return nil, nil
	}
	p := f.Payload
	if len(p) > 250 {
		return nil, fmt.Errorf("ZCL payload exceeds supported unfragmented frame size")
	}
	out := []Attribute{}
	for len(p) > 0 {
		if len(p) < 3 {
			return nil, fmt.Errorf("short attribute record")
		}
		id := le.Uint16(p[:2])
		p = p[2:]
		if f.Command == 1 {
			status := p[0]
			p = p[1:]
			if status != 0 {
				continue
			}
			if len(p) == 0 {
				return nil, fmt.Errorf("missing read response type")
			}
		}
		typ := p[0]
		p = p[1:]
		var v any
		var n int
		var e error
		if lumi && id == 0xff01 && typ == 0x42 {
			v, n, e = miStruct(p)
		} else if tuyaTiming && (id == 0xd001 || id == 0xd002) && typ == 0x48 && len(p) > 0 && p[0] == 2 {
			// _TZ3000_ew3ldmgx/E000 reports a length-prefixed two-byte
			// private value with an ARRAY tag. Preserve raw data, not a
			// fabricated ZCL array or an assumed schedule interpretation.
			v, n, e = value(0x41, p)
		} else {
			v, n, e = value(typ, p)
		}
		if e != nil {
			return nil, fmt.Errorf("attribute 0x%04x type 0x%02x: %w", id, typ, e)
		}
		p = p[n:]
		out = append(out, Attribute{id, typ, v})
	}
	return out, nil
}
func value(t byte, p []byte) (any, int, error) {
	return valueDepth(t, p, 0)
}
func valueDepth(t byte, p []byte, depth int) (any, int, error) {
	if depth > 8 {
		return nil, 0, fmt.Errorf("ZCL collection nesting exceeds 8")
	}
	if t == 0x48 || t == 0x4c || t == 0x50 || t == 0x51 {
		return collection(t, p, depth)
	}
	if t == 0 {
		return nil, 0, nil
	}
	if t == 0x41 || t == 0x42 {
		if len(p) < 1 {
			return nil, 0, fmt.Errorf("short string")
		}
		n := int(p[0])
		if n == 255 {
			return nil, 1, nil
		}
		if len(p) < n+1 {
			return nil, 0, fmt.Errorf("truncated string")
		}
		if t == 0x41 {
			return fmt.Sprintf("%x", p[1:n+1]), n + 1, nil
		}
		if !utf8.Valid(p[1 : n+1]) {
			return nil, 0, fmt.Errorf("invalid UTF-8")
		}
		return string(p[1 : n+1]), n + 1, nil
	}
	n := 0
	unsigned := false
	signed := false
	switch {
	case t == 0x10:
		n = 1
	case t >= 0x08 && t <= 0x0f:
		n = int(t-0x08) + 1
	case t >= 0x18 && t <= 0x1f:
		n = int(t-0x18) + 1
	case t >= 0x20 && t <= 0x27:
		n = int(t-0x20) + 1
		unsigned = true
	case t >= 0x28 && t <= 0x2f:
		n = int(t-0x28) + 1
		signed = true
	case t == 0x30:
		n = 1
		unsigned = true
	case t == 0x31:
		n = 2
		unsigned = true
	case t == 0x39:
		n = 4
	case t == 0x3a:
		n = 8
	case t == 0xe0 || t == 0xe1 || t == 0xe2:
		n = 4
	case t == 0xf0:
		n = 8
	default:
		return nil, 0, fmt.Errorf("unsupported ZCL type 0x%02x", t)
	}
	if len(p) < n {
		return nil, 0, fmt.Errorf("truncated ZCL value")
	}
	if t == 0x10 {
		if p[0] == 0xff {
			return nil, n, nil
		}
		if p[0] > 1 {
			return nil, 0, fmt.Errorf("invalid boolean")
		}
		return p[0] == 1, n, nil
	}
	var v uint64
	for i := 0; i < n; i++ {
		v |= uint64(p[i]) << (8 * i)
	}
	if t == 0x39 {
		x := float64(math.Float32frombits(uint32(v)))
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, n, nil
		}
		return x, n, nil
	}
	if t == 0x3a {
		x := math.Float64frombits(v)
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, n, nil
		}
		return x, n, nil
	}
	if unsigned {
		max := uint64(math.MaxUint64)
		if n < 8 {
			max = (uint64(1) << (8 * n)) - 1
		}
		if v == max {
			return nil, n, nil
		}
	}
	if signed {
		if v == uint64(1)<<(8*n-1) {
			return nil, n, nil
		}
		if n < 8 && v&(uint64(1)<<(8*n-1)) != 0 {
			v |= math.MaxUint64 << (8 * n)
		}
		return int64(v), n, nil
	}
	return v, n, nil
}
func numeric(v any) (float64, bool) {
	switch x := v.(type) {
	case uint64:
		return float64(x), true
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
func State(cluster uint16, f Frame) (map[string]any, error) {
	attrs, e := Attributes(f)
	if e != nil {
		return nil, e
	}
	return stateAttributes(cluster, f, attrs)
}
func stateAttributes(cluster uint16, f Frame, attrs []Attribute) (map[string]any, error) {
	out := map[string]any{}
	for _, a := range attrs {
		if a.Value == nil {
			continue
		}
		v, number := numeric(a.Value)
		switch cluster {
		case 0:
			if a.ID == 4 {
				if s, ok := a.Value.(string); ok {
					out["manufacturer"] = s
				}
			}
			if a.ID == 5 {
				if s, ok := a.Value.(string); ok {
					out["model_id"] = s
				}
			}
		case 1:
			if number && a.ID == 0x21 && v <= 200 {
				out["battery"] = v / 2
			}
			if number && a.ID == 0x20 {
				out["voltage"] = v * 100
			}
		case 6:
			if a.ID == 0 {
				if b, ok := a.Value.(bool); ok {
					if b {
						out["state"] = "ON"
					} else {
						out["state"] = "OFF"
					}
				}
			}
		case 8:
			if number && a.ID == 0 && v <= 254 {
				out["brightness"] = v
			}
		case 0x300:
			if number && a.ID == 7 {
				out["color_temp"] = v
			}
		case 0x400:
			if number && a.ID == 0 {
				out["illuminance"] = v
				if v == 0 {
					out["illuminance_lux"] = float64(0)
				} else {
					out["illuminance_lux"] = math.Pow(10, (v-1)/10000)
				}
			}
		case 0x402:
			if number && a.ID == 0 {
				out["temperature"] = v / 100
			}
		case 0x403:
			if number && a.ID == 0 {
				out["pressure"] = v
			}
		case 0x405:
			if number && a.ID == 0 {
				out["humidity"] = v / 100
			}
		case 0x406:
			if number && a.ID == 0 {
				out["occupancy"] = uint64(v)&1 != 0
			}
		case 0x500:
			if number && a.ID == 2 {
				out["zone_status"] = v
				out["tamper"] = uint64(v)&4 != 0
				out["battery_low"] = uint64(v)&8 != 0
			}
		}
	}
	// A generic IAS alarm cannot safely be labelled 'contact': zone type differs by device.
	if cluster == 0x500 && f.Control&3 == 1 && f.Control&4 == 0 && f.Command == 0 {
		if len(f.Payload) < 6 {
			return nil, fmt.Errorf("short IAS notification")
		}
		v := le.Uint16(f.Payload[:2])
		out["zone_status"] = v
		out["tamper"] = v&4 != 0
		out["battery_low"] = v&8 != 0
	}
	return out, nil
}

type Command struct {
	Cluster      uint16
	Endpoint     byte
	Control      byte
	ID           byte
	Payload      []byte
	Manufacturer uint16
}

func endpoint(d store.Device, cluster uint16) (byte, error) {
	var eps []int
	for _, ep := range d.Endpoints {
		if ep.Profile != 0x104 {
			continue
		}
		for _, c := range ep.In {
			if c == cluster {
				eps = append(eps, int(ep.ID))
				break
			}
		}
	}
	if len(eps) != 1 {
		return 0, fmt.Errorf("cluster 0x%04x requires exactly one HA endpoint (found %d)", cluster, len(eps))
	}
	return byte(eps[0]), nil
}
func Set(d store.Device, p map[string]any) ([]Command, error) {
	if len(p) == 0 {
		return nil, fmt.Errorf("empty set")
	}
	for key := range p {
		if key != "state" && key != "brightness" && key != "color_temp" && key != "transition" {
			return nil, fmt.Errorf("unsupported property %q", key)
		}
	}
	transition := uint16(0)
	if x, ok := p["transition"]; ok {
		v, ok := x.(float64)
		if !ok || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 6553.4 {
			return nil, fmt.Errorf("transition must be 0..6553.4 seconds")
		}
		transition = uint16(math.Round(v * 10))
	}
	out := []Command{}
	for _, k := range []string{"brightness", "color_temp", "state"} {
		x, ok := p[k]
		if !ok {
			continue
		}
		cmd := Command{Control: 0x11}
		switch k {
		case "state":
			s, ok := x.(string)
			if !ok {
				return nil, fmt.Errorf("state must be ON/OFF/TOGGLE")
			}
			cmd.Cluster = 6
			switch strings.ToUpper(s) {
			case "OFF":
				cmd.ID = 0
			case "ON":
				cmd.ID = 1
			case "TOGGLE":
				cmd.ID = 2
			default:
				return nil, fmt.Errorf("state must be ON/OFF/TOGGLE")
			}
		case "brightness":
			v, ok := x.(float64)
			if !ok || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 254 || v != math.Trunc(v) {
				return nil, fmt.Errorf("brightness must be an integer 0..254")
			}
			cmd.Cluster = 8
			cmd.ID = 4
			cmd.Payload = append([]byte{byte(v)}, u16(transition)...)
		case "color_temp":
			v, ok := x.(float64)
			if !ok || math.IsNaN(v) || math.IsInf(v, 0) || v < 1 || v > 65534 || v != math.Trunc(v) {
				return nil, fmt.Errorf("color_temp must be integer 1..65534 mired")
			}
			cmd.Cluster = 0x300
			cmd.ID = 0x0a
			cmd.Payload = append(u16(uint16(v)), u16(transition)...)
		}
		ep, e := endpoint(d, cmd.Cluster)
		if e != nil {
			return nil, e
		}
		cmd.Endpoint = ep
		out = append(out, cmd)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("transition requires a property")
	}
	return out, nil
}
func Get(d store.Device, p map[string]any) ([]Command, error) {
	defs := map[string][2]uint16{"state": {6, 0}, "brightness": {8, 0}, "color_temp": {0x300, 7}, "battery": {1, 0x21}, "temperature": {0x402, 0}, "humidity": {0x405, 0}, "pressure": {0x403, 0}, "illuminance": {0x400, 0}, "occupancy": {0x406, 0}}
	if len(p) == 0 {
		return nil, fmt.Errorf("empty get")
	}
	keys := []string{}
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := []Command{}
	for _, k := range keys {
		def, ok := defs[k]
		if !ok {
			return nil, fmt.Errorf("unsupported property %q", k)
		}
		ep, e := endpoint(d, def[0])
		if e != nil {
			return nil, e
		}
		out = append(out, Command{Cluster: def[0], Endpoint: ep, Control: 0x10, ID: 0, Payload: u16(def[1])})
	}
	return out, nil
}
