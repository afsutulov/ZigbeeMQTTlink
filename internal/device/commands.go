package device

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
)

func number(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case uint64:
		return float64(x), true
	case int64:
		return float64(x), true
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}
func scale(p Property) float64 {
	if p.Scale == 0 {
		return 1
	}
	return p.Scale
}
func normalize(p Property, v any) (any, error) {
	switch p.Type {
	case "boolean":
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("must be true/false")
		}
		return b, nil
	case "string":
		s, ok := v.(string)
		if !ok || !utf8.ValidString(s) || len(s) > 240 {
			return nil, fmt.Errorf("must be a string of at most 240 bytes")
		}
		return s, nil
	case "enum":
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("must be an enum string")
		}
		if _, ok := p.CommandIDs[s]; ok {
			return s, nil
		}
		if _, ok := p.Values[s]; !ok {
			return nil, fmt.Errorf("unknown enum value %q", s)
		}
		return s, nil
	case "number", "integer":
		if p.NumericString {
			if s, ok := v.(string); ok {
				n, err := strconv.ParseFloat(s, 64)
				if err != nil {
					return nil, fmt.Errorf("invalid numeric string")
				}
				v = n
			}
		}
		// JSON booleans must not silently become numeric command values.
		if _, ok := v.(bool); ok {
			return nil, fmt.Errorf("must be a number")
		}
		n, ok := number(v)
		if !ok || !finite(n) || (p.Type == "integer" && n != math.Trunc(n)) {
			return nil, fmt.Errorf("must be a finite %s", p.Type)
		}
		if p.Min != nil && n < *p.Min || p.Max != nil && n > *p.Max {
			return nil, fmt.Errorf("value outside configured range")
		}
		return n, nil
	}
	return nil, fmt.Errorf("unknown property type")
}
func rawValue(p Property, v any) (any, error) {
	if p.Type == "enum" {
		s := v.(string)
		raw, ok := p.Values[s]
		if !ok {
			return nil, fmt.Errorf("enum has no wire value")
		}
		return raw, nil
	}
	if p.Type == "number" || p.Type == "integer" {
		n := v.(float64)
		raw := (n - p.Offset) / scale(p)
		if !finite(raw) {
			return nil, fmt.Errorf("wire value overflow")
		}
		return raw, nil
	}
	return v, nil
}
func dpBytes(p Property, v any) ([]byte, error) {
	switch p.WireType {
	case 1:
		n, ok := number(v)
		if !ok || n != 0 && n != 1 {
			return nil, fmt.Errorf("DP boolean must be 0/1")
		}
		return []byte{byte(n)}, nil
	case 2:
		n, ok := number(v)
		if !ok || !finite(n) || n != math.Trunc(n) {
			return nil, fmt.Errorf("DP value must be an integer")
		}
		if p.Signed {
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, fmt.Errorf("signed DP out of range")
			}
		} else if n < 0 || n > math.MaxUint32 {
			return nil, fmt.Errorf("DP out of range")
		}
		out := make([]byte, 4)
		u := uint32(n)
		if p.Signed {
			u = uint32(int32(n))
		}
		binary.BigEndian.PutUint32(out, u)
		return out, nil
	case 3:
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("DP string required")
		}
		return []byte(s), nil
	case 4:
		n, ok := number(v)
		if !ok || n < 0 || n > 255 || n != math.Trunc(n) {
			return nil, fmt.Errorf("DP enum out of range")
		}
		return []byte{byte(n)}, nil
	case 5:
		n, ok := number(v)
		if !ok || n < 0 || n > math.MaxUint32 || n != math.Trunc(n) {
			return nil, fmt.Errorf("DP bitmap out of range")
		}
		out := make([]byte, 4)
		binary.BigEndian.PutUint32(out, uint32(n))
		return out, nil
	}
	return nil, fmt.Errorf("unsupported DP type")
}
func (r *Registry) Commands(d store.Device, input map[string]any, get bool) ([]zcl.Command, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("empty request")
	}
	def := r.Match(d)
	if def == nil {
		if get {
			return zcl.Get(d, input)
		}
		return zcl.Set(d, input)
	}
	if def.Protocol == "builtin" {
		return zcl.DeviceCommands(canonical(d, def), input, get)
	}
	byKey := map[string]zcl.Command{}
	normalized := map[string]any{}
	for _, key := range sortedKeys(input) {
		p, ok := def.Properties[key]
		if !ok && get && def.Protocol == "tuya" && key == "tuya_datapoints" {
			continue
		}
		if !ok {
			return nil, fmt.Errorf("unsupported property %q for %s", key, def.ID)
		}
		access := "w"
		if get {
			access = "r"
		}
		if !strings.Contains(p.Access, access) {
			return nil, fmt.Errorf("property %q is not %s", key, access)
		}
		if get && def.Protocol == "tuya" {
			continue
		}
		ep := p.Endpoint
		if ep == 0 {
			ep = def.Endpoint
		}
		cluster := p.Cluster
		if def.Protocol == "tuya" {
			cluster = 0xef00
		}
		ep, err := endpoint(d, ep, cluster)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		c := zcl.Command{Endpoint: ep, Cluster: cluster, Manufacturer: p.Manufacturer, Control: 0x10}
		if get {
			c.Payload = []byte{byte(*p.Attribute), byte(*p.Attribute >> 8)}
			byKey[key] = c
			continue
		}
		v, err := normalize(p, input[key])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		normalized[key] = v
		commandKey := fmt.Sprint(v)
		if id, ok := p.CommandIDs[commandKey]; ok {
			c.Control = 0x11
			c.ID = id
			byKey[key] = c
			continue
		}
		raw, err := rawValue(p, v)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		if def.Protocol == "tuya" {
			data, e := dpBytes(p, raw)
			if e != nil {
				return nil, fmt.Errorf("%s: %w", key, e)
			}
			c.Control = 0x11
			c.Payload = []byte{0, 0, *p.Datapoint, p.WireType, byte(len(data) >> 8), byte(len(data))}
			c.Payload = append(c.Payload, data...)
		} else {
			data, e := zcl.EncodeScalar(p.WireType, raw)
			if e != nil {
				return nil, fmt.Errorf("%s: %w", key, e)
			}
			if p.WriteCommand != nil {
				prefix, _ := hex.DecodeString(p.PayloadPrefix)
				suffix, _ := hex.DecodeString(p.PayloadSuffix)
				c.Control = 0x11
				c.ID = *p.WriteCommand
				c.Payload = append(prefix, data...)
				c.Payload = append(c.Payload, suffix...)
			} else {
				c.ID = 2
				c.Payload = []byte{byte(*p.Attribute), byte(*p.Attribute >> 8), p.WireType}
				c.Payload = append(c.Payload, data...)
			}
		}
		if len(c.Payload) > 240 {
			return nil, fmt.Errorf("%s: payload too large", key)
		}
		byKey[key] = c
	}
	if get && def.Protocol == "tuya" {
		c, err := action(d, Action{Kind: "tuya_query"}, def.Endpoint, false)
		if err != nil {
			return nil, err
		}
		return []zcl.Command{c}, nil
	}
	keys := []string{}
	added := map[string]bool{}
	add := func(key string) {
		if _, ok := byKey[key]; ok && !added[key] {
			keys = append(keys, key)
			added[key] = true
		}
	}
	if !get && def.StopFirst != "" && normalized[def.StopFirst] == false {
		add(def.StopFirst)
	}
	if !get {
		for _, key := range def.CommandOrder {
			add(key)
		}
	}
	for _, key := range sortedKeys(input) {
		add(key)
	}
	cs := make([]zcl.Command, 0, len(keys))
	for _, key := range keys {
		cs = append(cs, byKey[key])
	}
	return cs, nil
}
