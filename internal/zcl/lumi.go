package zcl

import (
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"zigbeemqttlink/internal/store"
)

const lumiManufacturer uint16 = 0x115f

func IsLumi(model string) bool {
	return len(Channels(model)) > 0 || model == "lumi.sensor_wleak.aq1" || model == "lumi.weather" || model == "lumi.remote.b186acn02"
}

// Only exact model IDs are used. IEEE OUIs cannot identify a converter.
func Channels(model string) map[string]byte {
	switch model {
	case "lumi.relay.c2acn01":
		return map[string]byte{"l1": 1, "l2": 2}
	case "lumi.switch.b2nc01":
		return map[string]byte{"left": 1, "right": 2}
	default:
		return nil
	}
}

func Wire(c Command, seq byte) []byte {
	if c.Manufacturer == 0 {
		return Header(c.Control, seq, c.ID, c.Payload)
	}
	p := []byte{c.Control | 4}
	p = append(p, u16(c.Manufacturer)...)
	return append(append(p, seq, c.ID), c.Payload...)
}

func StateDevice(d store.Device, ep byte, cluster uint16, f Frame) (map[string]any, error) {
	plain := f
	lumi := IsLumi(d.Model)
	if lumi && (cluster == 0 || cluster == 0xfcc0) && f.Manufacturer == lumiManufacturer {
		plain.Control &^= 4
	}
	tuyaTiming := d.Model == "TS011F" && d.Manufacturer == "_TZ3000_ew3ldmgx" && cluster == 0xe000
	attrs, err := attributesWithQuirks(plain, lumi && cluster == 0, tuyaTiming)
	if err != nil {
		return nil, err
	}
	out, err := stateAttributes(cluster, f, attrs)
	if err != nil {
		return nil, err
	}
	if d.Model == "TS011F" {
		tuyaState(cluster, attrs, out)
		if tuyaTiming {
			for _, a := range attrs {
				if raw, ok := a.Value.(string); ok && a.Type == 0x48 {
					if name := map[uint16]string{0xd001: "random_timing_raw", 0xd002: "cycle_timing_raw"}[a.ID]; name != "" {
						out[name] = raw
					}
				}
			}
		}
		meterState(d, ep, cluster, attrs, out)
	}
	if d.Model == "TS0601" && cluster == 0xef00 {
		return tuyaDPState(d, f)
	}
	channels := Channels(d.Model)
	if !lumi {
		return out, nil
	}
	if d.Model == "lumi.weather" {
		if v, ok := numeric(out["temperature"]); ok && (v <= -65 || v >= 65) {
			delete(out, "temperature")
		}
		if v, ok := numeric(out["humidity"]); ok && (v < 0 || v > 100) {
			delete(out, "humidity")
		}
	}
	label := ""
	for name, id := range channels {
		if ep == id {
			label = name
		}
	}
	if v, ok := out["state"]; ok {
		delete(out, "state")
		if label != "" {
			out["state_"+label] = v
		}
	}
	if d.Model == "lumi.remote.b186acn02" {
		for _, a := range attrs {
			if cluster == 6 && f.Command == 0x0a && a.ID == 0 {
				if _, ok := a.Value.(bool); ok {
					out["action"] = "single"
				}
			}
			if cluster == 0x12 && a.ID == 0x55 {
				if v, ok := numeric(a.Value); ok {
					if action := map[float64]string{0: "hold", 1: "single", 2: "double", 3: "triple", 255: "release"}[v]; action != "" {
						out["action"] = action
					}
				}
			}
		}
	}
	// The Aqara E1 sends rocker actions from endpoints 41/42/51, not 1/2.
	if d.Model == "lumi.switch.b2nc01" && cluster == 0x12 {
		button := map[byte]string{41: "left", 42: "right", 51: "both"}[ep]
		for _, a := range attrs {
			if a.ID != 0x55 || button == "" {
				continue
			}
			v, ok := numeric(a.Value)
			if !ok || v != math.Trunc(v) {
				continue
			}
			action := map[int]string{0: "hold", 1: "single", 2: "double", 3: "triple", 255: "release"}[int(v)]
			if action != "" {
				out["action"] = action + "_" + button
			}
		}
	}
	if d.Model == "lumi.relay.c2acn01" && cluster == 0x0c {
		for _, a := range attrs {
			if a.ID == 0x55 {
				if v, ok := numeric(a.Value); ok {
					out["power"] = v
				}
			}
		}
	}
	if (cluster == 0 || cluster == 0xfcc0) && (f.Control&4 == 0 || f.Manufacturer == lumiManufacturer) {
		for _, a := range attrs {
			if a.ID == 0xff01 || a.ID == 0xf7 {
				if elements, ok := a.Value.([]Attribute); ok {
					for _, item := range elements {
						lumiValue(out, d.Model, label, item.ID, item.Value)
					}
					continue
				}
				encoded, ok := a.Value.(string)
				if !ok || a.Type != 0x41 {
					continue
				}
				p, err := hex.DecodeString(encoded)
				if err != nil {
					return nil, err
				}
				for len(p) > 0 {
					if len(p) < 2 {
						return nil, fmt.Errorf("truncated Lumi TLV header")
					}
					id, typ := p[0], p[1]
					p = p[2:]
					v, n, err := value(typ, p)
					if err != nil {
						return nil, fmt.Errorf("Lumi TLV %d: %w", id, err)
					}
					p = p[n:]
					lumiValue(out, d.Model, label, uint16(id), v)
				}
			} else if cluster == 0xfcc0 {
				if d.Model == "lumi.switch.b2nc01" && a.ID == 0xdc && a.Type == 0x41 {
					// No state mapping in the pinned E1 converter. Keep the
					// diagnostic bytes without claiming a relay state change.
					out["aqara_raw_00dc"] = a.Value
				}
				lumiValue(out, d.Model, label, a.ID, a.Value)
			}
		}
	}
	if d.Model == "lumi.sensor_wleak.aq1" && cluster == 0x500 {
		if v, ok := numeric(out["zone_status"]); ok {
			out["water_leak"] = uint64(v)&1 != 0
		}
		if v, ok := out["zone_status"].(uint16); ok {
			out["water_leak"] = v&1 != 0
		}
	}
	return out, nil
}

func lumiValue(out map[string]any, model, label string, id uint16, value any) {
	v, ok := numeric(value)
	if b, isBool := value.(bool); isBool {
		ok = true
		if b {
			v = 1
		} else {
			v = 0
		}
	}
	if !ok {
		return
	}
	state := "OFF"
	if v == 1 {
		state = "ON"
	}
	channels := Channels(model)
	first, second := "left", "right"
	if model == "lumi.relay.c2acn01" {
		first, second = "l1", "l2"
	}
	switch id {
	case 1:
		if model == "lumi.sensor_wleak.aq1" || model == "lumi.weather" || model == "lumi.remote.b186acn02" {
			out["voltage"] = v
			out["battery"] = math.Max(0, math.Min(100, (v-2850)/150*100))
		}
	case 3:
		out["device_temperature"] = v
	case 5:
		if v >= 1 {
			out["power_outage_count"] = v - 1
		}
	case 6:
		if model == "lumi.sensor_wleak.aq1" && v >= 1 {
			out["trigger_count"] = float64(uint64(v)&0xffff) - 1
		}
	case 100:
		if model == "lumi.weather" && v/100 > -65 && v/100 < 65 {
			out["temperature"] = v / 100
		}
		if len(channels) > 0 && (v == 0 || v == 1) {
			out["state_"+first] = state
		}
	case 101:
		if model == "lumi.weather" && v >= 0 && v <= 10000 {
			out["humidity"] = v / 100
		}
		if len(channels) > 0 && (v == 0 || v == 1) {
			out["state_"+second] = state
		}
	case 102:
		if model == "lumi.weather" && v > 0 {
			out["pressure"] = v / 100
		}
	case 149:
		out["energy"] = v
		out["consumption"] = v
	case 150:
		out["voltage"] = v * 0.1
	case 151:
		if model == "lumi.relay.c2acn01" {
			out["current"] = v
		}
	case 152:
		out["power"] = v
	case 0x200:
		if label != "" && channels[label] != 0 && (v == 0 || v == 1) {
			mode := "decoupled"
			if v == 1 {
				mode = "control_relay"
			}
			out["operation_mode_"+label] = mode
		}
	case 0x201:
		out["power_outage_memory"] = v == 1
	case 0xf0:
		out["flip_indicator_light"] = state
	}
}

func endpointHas(d store.Device, ep byte, cluster uint16) bool {
	for _, e := range d.Endpoints {
		if e.ID == ep && e.Profile == 0x104 {
			for _, c := range e.In {
				if c == cluster {
					return true
				}
			}
		}
	}
	return false
}

// DeviceCommands validates the entire request before any command is sent.
func DeviceCommands(d store.Device, p map[string]any, get bool) ([]Command, error) {
	if d.Model == "TS0601" {
		if get {
			return tuyaGet(d, p)
		}
		return nil, fmt.Errorf("TS0601 writes require a model-specific datapoint converter")
	}
	if d.Model == "TS011F" {
		return tuyaCommands(d, p, get)
	}
	if len(Channels(d.Model)) == 0 {
		if get {
			return Get(d, p)
		}
		return Set(d, p)
	}
	if len(p) == 0 {
		return nil, fmt.Errorf("empty request")
	}
	keys := make([]string, 0, len(p))
	for key := range p {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := []Command{}
	for _, key := range keys {
		v := p[key]
		var c Command
		c.Endpoint = 1
		switch {
		case strings.HasPrefix(key, "state_"):
			ep := Channels(d.Model)[strings.TrimPrefix(key, "state_")]
			if ep == 0 || !endpointHas(d, ep, 6) {
				return nil, fmt.Errorf("unsupported channel %q or missing endpoint descriptor", key)
			}
			c.Endpoint, c.Cluster, c.Control = ep, 6, 0x11
			if get {
				c.Control = 0x10
				c.ID = 0
				c.Payload = u16(0)
			} else {
				s, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("%s must be ON/OFF/TOGGLE", key)
				}
				switch strings.ToUpper(s) {
				case "OFF":
					c.ID = 0
				case "ON":
					c.ID = 1
				case "TOGGLE":
					c.ID = 2
				default:
					return nil, fmt.Errorf("%s must be ON/OFF/TOGGLE", key)
				}
			}
		case strings.HasPrefix(key, "operation_mode_") && d.Model == "lumi.switch.b2nc01":
			c.Endpoint = Channels(d.Model)[strings.TrimPrefix(key, "operation_mode_")]
			if c.Endpoint == 0 {
				return nil, fmt.Errorf("unsupported channel %q", key)
			}
			mode, ok := v.(string)
			b := byte(0)
			if !get {
				if !ok || (mode != "control_relay" && mode != "decoupled") {
					return nil, fmt.Errorf("%s must be control_relay/decoupled", key)
				}
				if mode == "control_relay" {
					b = 1
				}
			}
			c = lumiAttribute(c.Endpoint, 0xfcc0, 0x200, 0x20, []byte{b}, get)
		case key == "power_outage_memory":
			b, ok := v.(bool)
			if !get && !ok {
				return nil, fmt.Errorf("power_outage_memory must be boolean")
			}
			x := byte(0)
			if b {
				x = 1
			}
			if d.Model == "lumi.switch.b2nc01" {
				c = lumiAttribute(1, 0xfcc0, 0x201, 0x10, []byte{x}, get)
			} else {
				if get {
					return nil, fmt.Errorf("relay power_outage_memory is write-only in this version")
				}
				first := []byte{0xaa, 0x80, 0x05, 0xd1, 0x47, 0x09, 0x01, 0x10, 0x00}
				second := []byte{0xaa, 0x80, 0x03, 0xd3, 0x07, 0x0a, 0x01}
				if b {
					first[5], first[8], second[5] = 7, 1, 8
				}
				result = append(result, lumiAttribute(1, 0, 0xfff0, 0x41, append([]byte{byte(len(first))}, first...), false), lumiAttribute(1, 0, 0xfff0, 0x41, append([]byte{byte(len(second))}, second...), false))
				continue
			}
		case key == "flip_indicator_light" && d.Model == "lumi.switch.b2nc01":
			s, ok := v.(string)
			x := byte(0)
			if !get {
				if !ok || (s != "ON" && s != "OFF") {
					return nil, fmt.Errorf("flip_indicator_light must be ON/OFF")
				}
				if s == "ON" {
					x = 1
				}
			}
			c = lumiAttribute(1, 0xfcc0, 0xf0, 0x20, []byte{x}, get)
		case key == "interlock" && d.Model == "lumi.relay.c2acn01":
			if get {
				return nil, fmt.Errorf("interlock is write-only in this version")
			}
			b, ok := v.(bool)
			if !ok {
				return nil, fmt.Errorf("interlock must be boolean")
			}
			x := byte(0)
			if b {
				x = 1
			}
			c = lumiAttribute(1, 0x10, 0xff06, 0x10, []byte{x}, false)
		case key == "power" && get && d.Model == "lumi.relay.c2acn01":
			ep, err := endpoint(d, 0x0c)
			if err != nil {
				return nil, err
			}
			c = Command{Cluster: 0x0c, Endpoint: ep, Control: 0x10, ID: 0, Payload: u16(0x55)}
		default:
			return nil, fmt.Errorf("property %q is not supported for %s; select an explicit channel for state", key, d.Model)
		}
		result = append(result, c)
	}
	return result, nil
}

func lumiAttribute(ep byte, cluster, id uint16, typ byte, v []byte, get bool) Command {
	c := Command{Endpoint: ep, Cluster: cluster, Manufacturer: lumiManufacturer, Control: 0x10, ID: 0, Payload: u16(id)}
	if !get {
		c.ID = 2
		c.Payload = append(append(c.Payload, typ), v...)
	}
	return c
}

func ConfigureCommands(d store.Device) []Command {
	if d.Model == "TS0601" && tuyaClimateManufacturers[d.Manufacturer] != 0 {
		commands, _ := tuyaGet(d, map[string]any{"tuya_datapoints": ""})
		return commands
	}
	if d.Model == "TS011F" {
		return meterReads(d)
	}
	if d.Model == "lumi.switch.b2nc01" {
		return []Command{lumiAttribute(1, 0xfcc0, 9, 0x20, []byte{1}, false)}
	}
	return nil
}

func PreventReset(d store.Device, cluster uint16, f Frame) (*Command, error) {
	if (d.Model != "lumi.switch.b2nc01" && d.Model != "lumi.remote.b186acn02") || cluster != 0 || f.Command != 0x0a || f.Control&3 != 0 || (f.Control&4 != 0 && f.Manufacturer != lumiManufacturer) {
		return nil, nil
	}
	plain := f
	plain.Control &^= 4
	attrs, err := attributes(plain, true)
	if err != nil {
		return nil, err
	}
	for _, a := range attrs {
		encoded, ok := a.Value.(string)
		if a.ID == 0xfff0 && a.Type == 0x41 && ok && strings.HasPrefix(encoded, "aa10054187") {
			payload := []byte{9, 0xaa, 0x10, 0x05, 0x41, 0x47, 0x01, 0x01, 0x10, 0x01}
			c := lumiAttribute(1, 0, 0xfff0, 0x41, payload, false)
			return &c, nil
		}
	}
	return nil, nil
}

// Time requests are protocol service requests, not device-state reports.
func TimeResponse(f Frame, now time.Time) ([]byte, error) {
	if f.Control&7 != 0 || f.Command != 0 || f.Control&8 != 0 || len(f.Payload) == 0 || len(f.Payload)%2 != 0 {
		return nil, fmt.Errorf("invalid Time read request")
	}
	utc := now.Unix() - 946684800
	_, offset := now.Zone()
	if utc < 0 || utc > 0xfffffffe {
		return nil, fmt.Errorf("system clock outside Zigbee time range")
	}
	out := []byte{}
	for p := f.Payload; len(p) > 0; p = p[2:] {
		id := le.Uint16(p[:2])
		out = append(out, p[:2]...)
		v := uint32(0)
		typ := byte(0xe2)
		switch id {
		case 0, 8:
			v = uint32(utc)
		case 1:
			out = append(out, 0, 0x18, 0x0d)
			continue
		case 2:
			v = uint32(int32(offset))
			typ = 0x2b
		case 3, 4:
			v = 0xffffffff
			typ = 0x23
		case 5:
			v = 0
			typ = 0x2b
		case 6, 7:
			v = uint32(utc + int64(offset))
			typ = 0x23
		case 9:
			v = uint32(utc + 86400)
		default:
			out = append(out, 0x86)
			continue
		}
		out = append(out, 0, typ, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
	}
	if len(out) > 237 {
		return nil, fmt.Errorf("Time response too long")
	}
	return Header(0x18, f.Seq, 1, out), nil
}
