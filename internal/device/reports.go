package device

import (
	"fmt"
	"strings"

	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
)

func decode(p Property, v any) (any, bool) {
	if v == nil {
		return nil, false
	}
	switch p.Type {
	case "boolean":
		n, ok := number(v)
		if !ok || n != 0 && n != 1 {
			return nil, false
		}
		return n == 1, true
	case "enum":
		n, ok := number(v)
		if !ok {
			return nil, false
		}
		for key, value := range p.Values {
			if n == value {
				return key, true
			}
		}
		return nil, false
	case "string":
		s, ok := v.(string)
		return s, ok
	case "number", "integer":
		n, ok := number(v)
		if !ok {
			return nil, false
		}
		if p.Signed && n >= 2147483648 {
			n -= 4294967296
		}
		n = n*scale(p) + p.Offset
		if !finite(n) || p.Min != nil && n < *p.Min || p.Max != nil && n > *p.Max {
			return nil, false
		}
		return n, true
	}
	return nil, false
}
func (r *Registry) State(d store.Device, ep byte, cluster uint16, f zcl.Frame) (map[string]any, error) {
	def := r.Match(d)
	if def == nil {
		return zcl.State(cluster, f)
	}
	if def.Protocol == "builtin" {
		return zcl.StateDevice(canonical(d, def), ep, cluster, f)
	}
	out := map[string]any{}
	for _, event := range def.Events {
		if eventMatches(event, ep, cluster, f) {
			for key, value := range event.State {
				out[key] = value
			}
		}
	}
	if def.Protocol == "tuya" && cluster == 0xef00 {
		if f.Control&7 != 1 || f.Command != 1 && f.Command != 2 {
			return out, nil
		}
		values, err := zcl.DecodeDatapoints(f.Payload)
		if err != nil {
			return nil, err
		}
		if len(values) > 0 {
			out["tuya_datapoints"] = values
		}
		for key, p := range def.Properties {
			if !strings.Contains(p.Access, "r") {
				continue
			}
			target := p.Endpoint
			if target == 0 {
				target = def.Endpoint
			}
			if target != 0 && target != ep {
				continue
			}
			for _, dp := range values {
				if p.Datapoint != nil && dp.ID == *p.Datapoint && dp.Type == p.WireType {
					if v, ok := decode(p, dp.Value); ok {
						out[key] = v
					}
				}
			}
		}
		return out, nil
	}
	// Basic identification and standard IAS messages remain available for all
	// declarative definitions, to allow interview and pairing without scripts.
	basic, err := zcl.State(cluster, f)
	if err != nil && f.Control&4 == 0 {
		return nil, err
	}
	if cluster == 0 || cluster == 0x500 {
		for key, v := range basic {
			out[key] = v
		}
	}
	plain := f
	if f.Control&4 != 0 {
		plain.Control &^= 4
	}
	attrs, err := zcl.Attributes(plain)
	if err != nil {
		return nil, fmt.Errorf("attribute report: %w", err)
	}
	for key, p := range def.Properties {
		if !strings.Contains(p.Access, "r") || p.Attribute == nil || p.Cluster != cluster {
			continue
		}
		target := p.Endpoint
		if target == 0 {
			target = def.Endpoint
		}
		if target != 0 && target != ep {
			continue
		}
		if f.Control&4 != 0 && p.Manufacturer != f.Manufacturer || f.Control&4 == 0 && p.Manufacturer != 0 {
			continue
		}
		for _, a := range attrs {
			if a.ID == *p.Attribute && a.Type == p.WireType {
				if v, ok := decode(p, a.Value); ok {
					out[key] = v
				}
			}
		}
	}
	return out, nil
}
