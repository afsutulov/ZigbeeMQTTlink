package device

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
)

type Manifest struct {
	Version int          `json:"version"`
	Devices []Definition `json:"devices"`
}
type Definition struct {
	ID                  string              `json:"id"`
	Description         string              `json:"description,omitempty"` // shown in the web panel
	Models              []string            `json:"models"`
	Manufacturers       []string            `json:"manufacturers,omitempty"`
	Protocol            string              `json:"protocol"` // builtin, zcl, tuya
	Builtin             string              `json:"builtin,omitempty"`
	Endpoint            byte                `json:"endpoint,omitempty"` // 0 selects the unique HA input endpoint
	Channels            map[string]byte     `json:"channels,omitempty"`
	Properties          map[string]Property `json:"properties,omitempty"`
	Events              []Event             `json:"events,omitempty"`
	CommandOrder        []string            `json:"command_order,omitempty"`
	StopFirst           string              `json:"stop_first,omitempty"` // boolean property, false first
	InterCommandDelayMS int                 `json:"inter_command_delay_ms,omitempty"`
	Configure           []Action            `json:"configure,omitempty"`
	GatewayStatus       bool                `json:"gateway_status,omitempty"`
	TimeEpoch           string              `json:"time_epoch,omitempty"`
}
type Property struct {
	Type          string             `json:"type"`   // boolean, number, integer, enum, string
	Access        string             `json:"access"` // r, w, rw
	Endpoint      byte               `json:"endpoint,omitempty"`
	Cluster       uint16             `json:"cluster,omitempty"`
	Attribute     *uint16            `json:"attribute,omitempty"`
	Datapoint     *byte              `json:"datapoint,omitempty"`
	WireType      byte               `json:"wire_type"` // ZCL type or Tuya DP type, by protocol
	Scale         float64            `json:"scale,omitempty"`
	Offset        float64            `json:"offset,omitempty"`
	Min           *float64           `json:"min,omitempty"`
	Max           *float64           `json:"max,omitempty"`
	Signed        bool               `json:"signed,omitempty"` // Tuya VALUE interpreted as int32
	NumericString bool               `json:"numeric_string,omitempty"`
	Values        map[string]float64 `json:"values,omitempty"`
	CommandIDs    map[string]byte    `json:"command_ids,omitempty"`
	WriteCommand  *byte              `json:"write_command,omitempty"`
	PayloadPrefix string             `json:"payload_prefix_hex,omitempty"`
	PayloadSuffix string             `json:"payload_suffix_hex,omitempty"`
	Manufacturer  uint16             `json:"manufacturer_code,omitempty"`
}
type Event struct {
	Endpoint     byte           `json:"endpoint,omitempty"`
	Cluster      uint16         `json:"cluster"`
	Command      byte           `json:"command"`
	Manufacturer uint16         `json:"manufacturer_code,omitempty"`
	PayloadHex   *string        `json:"payload_hex,omitempty"`
	State        map[string]any `json:"state"`
}
type Action struct {
	Kind         string   `json:"kind"` // read, write, command, tuya_query, bind, report
	Endpoint     byte     `json:"endpoint,omitempty"`
	Cluster      uint16   `json:"cluster,omitempty"`
	Attributes   []uint16 `json:"attributes,omitempty"`
	Command      byte     `json:"command,omitempty"`
	PayloadHex   string   `json:"payload_hex,omitempty"`
	Attribute    *uint16  `json:"attribute,omitempty"`
	WireType     byte     `json:"wire_type,omitempty"`
	Value        any      `json:"value,omitempty"`
	Manufacturer uint16   `json:"manufacturer_code,omitempty"`
	// report: Configure Reporting intervals in seconds and reportable change
	// (raw wire units; required only for analog types, defaults to 1).
	MinInterval *uint16  `json:"min_interval,omitempty"`
	MaxInterval *uint16  `json:"max_interval,omitempty"`
	Change      *float64 `json:"change,omitempty"`
}
type Registry struct {
	mu       sync.RWMutex
	writeMu  sync.Mutex
	path     string
	manifest Manifest
}

var builtins = map[string]string{"aqara_e1": "lumi.switch.b2nc01", "aqara_relay": "lumi.relay.c2acn01", "aqara_weather": "lumi.weather", "aqara_leak": "lumi.sensor_wleak.aq1", "aqara_button": "lumi.remote.b186acn02", "tuya_plug": "TS011F", "tuya_climate": "TS0601"}

func Load(path string) (*Registry, error) {
	r := &Registry{path: path}
	if err := r.Reload(); err != nil {
		return nil, err
	}
	return r, nil
}
func Parse(b []byte) (Manifest, error) {
	var m Manifest
	if err := config.StrictJSON(b, &m); err != nil {
		return m, err
	}
	return m, validate(m)
}
func (r *Registry) Reload() error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	b, e := os.ReadFile(r.path)
	if e != nil {
		return e
	}
	m, e := Parse(b)
	if e != nil {
		return e
	}
	r.mu.Lock()
	r.manifest = m
	r.mu.Unlock()
	return nil
}
func (r *Registry) Snapshot() Manifest {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, _ := json.Marshal(r.manifest)
	var m Manifest
	_ = json.Unmarshal(b, &m)
	return m
}
func (r *Registry) Save(b []byte) error {
	m, err := Parse(b)
	if err != nil {
		return err
	}
	canonical, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	if err = store.AtomicWrite(r.path, append(canonical, '\n')); err != nil {
		return err
	}
	r.mu.Lock()
	r.manifest = m
	r.mu.Unlock()
	return nil
}
func contains(a []string, s string) bool {
	for _, x := range a {
		if x == s {
			return true
		}
	}
	return false
}
func (r *Registry) Match(d store.Device) *Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var chosen *Definition
	for i := range r.manifest.Devices {
		p := &r.manifest.Devices[i]
		if !contains(p.Models, d.Model) {
			continue
		}
		if len(p.Manufacturers) > 0 && !contains(p.Manufacturers, d.Manufacturer) {
			continue
		}
		if chosen == nil || len(p.Manufacturers) > 0 {
			chosen = p
		}
	}
	return chosen // immutable manifest snapshot; reload allocates a new one
}
func validate(m Manifest) error {
	if m.Version != 1 || m.Devices == nil {
		return fmt.Errorf("definitions.version=1 and devices array required")
	}
	if len(m.Devices) > 4096 {
		return fmt.Errorf("too many device definitions")
	}
	ids := map[string]bool{}
	for i, d := range m.Devices {
		if !config.Name(d.ID) || ids[d.ID] || len(d.Models) == 0 {
			return fmt.Errorf("definition %d: unique id and models required", i)
		}
		ids[d.ID] = true
		for _, model := range d.Models {
			if strings.TrimSpace(model) == "" {
				return fmt.Errorf("%s: empty model", d.ID)
			}
		}
		for _, v := range d.Manufacturers {
			if v == "" {
				return fmt.Errorf("%s: empty manufacturer", d.ID)
			}
		}
		if d.Endpoint > 240 || d.InterCommandDelayMS < 0 || d.InterCommandDelayMS > 1000 {
			return fmt.Errorf("%s: endpoint/delay out of range", d.ID)
		}
		switch d.Protocol {
		case "builtin":
			if len(d.Properties) > 0 || len(d.Events) > 0 || len(d.Configure) > 0 || len(d.CommandOrder) > 0 || d.StopFirst != "" || len(d.Channels) > 0 || d.GatewayStatus || d.TimeEpoch != "" || d.Endpoint != 0 {
				return fmt.Errorf("%s: builtin accepts only selectors, builtin and delay; use zcl/tuya for properties", d.ID)
			}
			if builtins[d.Builtin] == "" {
				return fmt.Errorf("%s: unknown builtin %q", d.ID, d.Builtin)
			}
		case "zcl", "tuya":
			if d.Builtin != "" || d.Protocol != "tuya" && (d.GatewayStatus || d.TimeEpoch != "") {
				return fmt.Errorf("%s: builtin/gateway_status/time_epoch does not apply to protocol", d.ID)
			}
			if len(d.Properties) == 0 && !(d.Protocol == "zcl" && len(d.Events) > 0) {
				return fmt.Errorf("%s: properties required", d.ID)
			}
		default:
			return fmt.Errorf("%s: protocol must be builtin/zcl/tuya", d.ID)
		}
		for name, ep := range d.Channels {
			if !config.Name(name) || strings.Contains(name, "/") || ep == 0 || ep > 240 {
				return fmt.Errorf("%s: invalid channel", d.ID)
			}
		}
		if d.TimeEpoch != "" && d.TimeEpoch != "off" && d.TimeEpoch != "1970" && d.TimeEpoch != "2000" {
			return fmt.Errorf("%s: time_epoch must be off/1970/2000", d.ID)
		}
		seenDP := map[byte]bool{}
		for name, p := range d.Properties {
			if !config.Name(name) || p.Endpoint > 240 {
				return fmt.Errorf("%s: invalid property/endpoint %q", d.ID, name)
			}
			switch p.Type {
			case "boolean", "number", "integer", "enum", "string":
			default:
				return fmt.Errorf("%s/%s: invalid type", d.ID, name)
			}
			if p.Access != "r" && p.Access != "w" && p.Access != "rw" {
				return fmt.Errorf("%s/%s: access must be r/w/rw", d.ID, name)
			}
			if p.Min != nil && p.Max != nil && *p.Min > *p.Max {
				return fmt.Errorf("%s/%s: min exceeds max", d.ID, name)
			}
			if !finite(p.Scale) || !finite(p.Offset) || p.Scale < 0 {
				return fmt.Errorf("%s/%s: scale must be positive", d.ID, name)
			}
			if p.Type == "enum" && len(p.Values) == 0 {
				return fmt.Errorf("%s/%s: enum values required", d.ID, name)
			}
			seenValues := map[float64]bool{}
			for key, v := range p.Values {
				if key == "" || !finite(v) || seenValues[v] {
					return fmt.Errorf("%s/%s: invalid/duplicate enum value", d.ID, name)
				}
				seenValues[v] = true
			}
			if d.Protocol == "tuya" {
				if p.Datapoint == nil || seenDP[*p.Datapoint] || p.Attribute != nil || len(p.CommandIDs) > 0 || p.Manufacturer != 0 || p.WriteCommand != nil || p.PayloadPrefix != "" || p.PayloadSuffix != "" {
					return fmt.Errorf("%s/%s: unique datapoint required", d.ID, name)
				}
				seenDP[*p.Datapoint] = true
				if p.WireType < 1 || p.WireType > 5 {
					return fmt.Errorf("%s/%s: DP type must be 1..5", d.ID, name)
				}
			} else if d.Protocol == "zcl" {
				if p.Datapoint != nil || (p.Attribute == nil && (strings.Contains(p.Access, "r") || p.WriteCommand == nil && len(p.CommandIDs) == 0)) {
					return fmt.Errorf("%s/%s: attribute required", d.ID, name)
				}
				if !zcl.ScalarType(p.WireType) {
					return fmt.Errorf("%s/%s: unsupported scalar ZCL type", d.ID, name)
				}
				if p.WriteCommand != nil && len(p.CommandIDs) > 0 {
					return fmt.Errorf("%s/%s: choose write_command or command_ids", d.ID, name)
				}
				if p.WriteCommand == nil && (p.PayloadPrefix != "" || p.PayloadSuffix != "") {
					return fmt.Errorf("%s/%s: payload prefix/suffix require write_command", d.ID, name)
				}
				for _, v := range []string{p.PayloadPrefix, p.PayloadSuffix} {
					raw, e := hex.DecodeString(v)
					if e != nil || len(raw) > 200 {
						return fmt.Errorf("%s/%s: invalid payload hex", d.ID, name)
					}
				}
				for key := range p.CommandIDs {
					if p.Type != "enum" && p.Type != "boolean" || key == "" {
						return fmt.Errorf("%s/%s: command_ids require enum or boolean", d.ID, name)
					}
				}
			}
		}
		if len(d.Properties) > 256 {
			return fmt.Errorf("%s: too many properties", d.ID)
		}
		if len(d.Events) > 256 || d.Protocol != "zcl" && len(d.Events) > 0 {
			return fmt.Errorf("%s: events require zcl, at most 256", d.ID)
		}
		seenEvents := map[string]bool{}
		for _, e := range d.Events {
			if e.Endpoint > 240 || len(e.State) == 0 {
				return fmt.Errorf("%s: invalid event", d.ID)
			}
			payload := "*"
			if e.PayloadHex != nil {
				raw, err := hex.DecodeString(*e.PayloadHex)
				if err != nil || len(raw) > 240 {
					return fmt.Errorf("%s: invalid event payload", d.ID)
				}
				payload = *e.PayloadHex
			}
			key := fmt.Sprintf("%d/%d/%d/%d/%s", e.Endpoint, e.Cluster, e.Command, e.Manufacturer, payload)
			if seenEvents[key] {
				return fmt.Errorf("%s: duplicate event", d.ID)
			}
			seenEvents[key] = true
			for k, v := range e.State {
				if !config.Name(k) || k == "model_id" || k == "manufacturer" {
					return fmt.Errorf("%s: invalid event state key", d.ID)
				}
				switch x := v.(type) {
				case bool, string:
				case float64:
					if !finite(x) {
						return fmt.Errorf("%s: invalid event number", d.ID)
					}
				default:
					return fmt.Errorf("%s: event state requires scalar values", d.ID)
				}
			}
		}
		seenOrder := map[string]bool{}
		for _, key := range d.CommandOrder {
			if _, ok := d.Properties[key]; !ok || seenOrder[key] {
				return fmt.Errorf("%s: unknown/duplicate command_order property", d.ID)
			}
			seenOrder[key] = true
		}
		if d.StopFirst != "" {
			p, ok := d.Properties[d.StopFirst]
			if !ok || p.Type != "boolean" || !strings.Contains(p.Access, "w") {
				return fmt.Errorf("%s: stop_first requires a writable boolean", d.ID)
			}
		}
		for _, a := range d.Configure {
			if _, err := action(store.Device{}, a, d.Endpoint, true); err != nil {
				return fmt.Errorf("%s configure: %w", d.ID, err)
			}
		}
		for j := 0; j < i; j++ {
			other := m.Devices[j]
			if (len(other.Manufacturers) == 0) != (len(d.Manufacturers) == 0) {
				continue
			}
			overlap := false
			for _, model := range d.Models {
				overlap = overlap || contains(other.Models, model)
			}
			if !overlap {
				continue
			}
			if len(d.Manufacturers) == 0 {
				return fmt.Errorf("ambiguous model definitions %s/%s", other.ID, d.ID)
			}
			for _, mf := range d.Manufacturers {
				if contains(other.Manufacturers, mf) {
					return fmt.Errorf("ambiguous manufacturer definitions %s/%s", other.ID, d.ID)
				}
			}
		}
	}
	return nil
}
func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
func (r *Registry) Channels(d store.Device) map[string]byte {
	p := r.Match(d)
	if p == nil {
		return nil
	}
	if p.Protocol == "builtin" {
		return zcl.Channels(builtins[p.Builtin])
	}
	return p.Channels
}
func (r *Registry) Delay(d store.Device) time.Duration {
	p := r.Match(d)
	if p == nil {
		return 0
	}
	return time.Duration(p.InterCommandDelayMS) * time.Millisecond
}
func (r *Registry) Description(d store.Device) map[string]any {
	p := r.Match(d)
	if p == nil {
		return map[string]any{"id": "standard_zcl", "protocol": "zcl"}
	}
	return map[string]any{"id": p.ID, "description": p.Description, "protocol": p.Protocol, "properties": p.Properties, "stop_first": p.StopFirst, "configurable": r.HasConfigure(d)}
}
func canonical(d store.Device, p *Definition) store.Device {
	if p.Protocol == "builtin" {
		d.Model = builtins[p.Builtin]
	}
	return d
}
func endpoint(d store.Device, explicit byte, cluster uint16) (byte, error) {
	if explicit != 0 {
		for _, e := range d.Endpoints {
			if e.ID == explicit && e.Profile == 0x104 {
				return explicit, nil
			}
		}
		return 0, fmt.Errorf("endpoint %d missing HA descriptor", explicit)
	}
	var ep byte
	for _, e := range d.Endpoints {
		if e.Profile != 0x104 {
			continue
		}
		for _, c := range e.In {
			if c == cluster {
				if ep != 0 && ep != e.ID {
					return 0, fmt.Errorf("cluster 0x%04x has multiple endpoints; specify endpoint", cluster)
				}
				ep = e.ID
			}
		}
	}
	if ep == 0 {
		return 0, fmt.Errorf("cluster 0x%04x endpoint missing", cluster)
	}
	return ep, nil
}
func action(d store.Device, a Action, defaultEP byte, validateOnly bool) (zcl.Command, error) {
	c := zcl.Command{Cluster: a.Cluster, Manufacturer: a.Manufacturer, Control: 0x10}
	if a.Endpoint > 240 {
		return c, fmt.Errorf("invalid endpoint")
	}
	switch a.Kind {
	case "read":
		if len(a.Attributes) == 0 || len(a.Attributes) > 100 {
			return c, fmt.Errorf("read requires 1..100 attributes")
		}
		for _, id := range a.Attributes {
			c.Payload = append(c.Payload, byte(id), byte(id>>8))
		}
	case "write":
		if a.Attribute == nil || !zcl.ScalarType(a.WireType) {
			return c, fmt.Errorf("write requires attribute and supported wire_type")
		}
		data, err := zcl.EncodeScalar(a.WireType, a.Value)
		if err != nil {
			return c, err
		}
		c.ID = 2
		c.Payload = []byte{byte(*a.Attribute), byte(*a.Attribute >> 8), a.WireType}
		c.Payload = append(c.Payload, data...)
	case "command":
		c.Control = 0x11
		c.ID = a.Command
		var e error
		c.Payload, e = hex.DecodeString(a.PayloadHex)
		if e != nil {
			return c, fmt.Errorf("invalid payload_hex")
		}
	case "tuya_query":
		c.Cluster = 0xef00
		c.Control = 0x11
		c.ID = 3
	case "bind":
		c.Bind = true
	case "report":
		if a.Attribute == nil || !zcl.ScalarType(a.WireType) || a.MinInterval == nil || a.MaxInterval == nil {
			return c, fmt.Errorf("report requires attribute, wire_type, min_interval and max_interval")
		}
		if *a.MaxInterval != 0xffff && *a.MaxInterval != 0 && *a.MinInterval > *a.MaxInterval {
			return c, fmt.Errorf("report min_interval exceeds max_interval")
		}
		c.ID = 6
		c.Payload = []byte{0, byte(*a.Attribute), byte(*a.Attribute >> 8), a.WireType, byte(*a.MinInterval), byte(*a.MinInterval >> 8), byte(*a.MaxInterval), byte(*a.MaxInterval >> 8)}
		if zcl.AnalogType(a.WireType) {
			change := 1.0
			if a.Change != nil {
				change = *a.Change
			}
			if !finite(change) || change < 0 {
				return c, fmt.Errorf("report change must be finite and non-negative")
			}
			data, err := zcl.EncodeScalar(a.WireType, change)
			if err != nil {
				return c, fmt.Errorf("report change: %w", err)
			}
			c.Payload = append(c.Payload, data...)
		} else if a.Change != nil {
			return c, fmt.Errorf("report change applies only to analog types")
		}
	default:
		return c, fmt.Errorf("unknown configure kind")
	}
	if !c.Bind && len(zcl.Wire(c, 0)) > 240 {
		return c, fmt.Errorf("configure payload too large")
	}
	if !validateOnly {
		ep := a.Endpoint
		if ep == 0 {
			ep = defaultEP
		}
		var err error
		c.Endpoint, err = endpoint(d, ep, c.Cluster)
		if err != nil {
			return c, err
		}
	}
	return c, nil
}
func (r *Registry) Configure(d store.Device) ([]zcl.Command, error) {
	p := r.Match(d)
	if p == nil {
		return nil, fmt.Errorf("no configure definition for %s", d.Model)
	}
	if p.Protocol == "builtin" {
		cs := zcl.ConfigureCommands(canonical(d, p))
		if len(cs) == 0 && len(r.Channels(d)) == 0 {
			return nil, fmt.Errorf("configure not defined for %s", p.ID)
		}
		return cs, nil
	}
	cs := []zcl.Command{}
	for _, a := range p.Configure {
		c, e := action(d, a, p.Endpoint, false)
		if e != nil {
			return nil, e
		}
		cs = append(cs, c)
	}
	if len(cs) == 0 && p.Protocol == "tuya" {
		c, e := action(d, Action{Kind: "tuya_query"}, p.Endpoint, false)
		if e != nil {
			return nil, e
		}
		cs = append(cs, c)
	}
	if len(cs) == 0 {
		return nil, fmt.Errorf("configure not defined for %s", p.ID)
	}
	return cs, nil
}
func (r *Registry) HasConfigure(d store.Device) bool {
	cs, err := r.Configure(d)
	return err == nil && (len(cs) > 0 || len(r.Channels(d)) > 0)
}
func (r *Registry) GatewayResponse(d store.Device, f zcl.Frame) ([]byte, error) {
	p := r.Match(d)
	if p == nil || p.Protocol != "tuya" || !p.GatewayStatus || f.Control&7 != 1 || f.Command != 0x25 {
		return nil, nil
	}
	if len(f.Payload) != 2 {
		return nil, fmt.Errorf("invalid gateway status request")
	}
	return zcl.Header(0x11, f.Seq, 0x25, []byte{1, 0, 1}), nil
}
func sortedKeys(p map[string]any) []string {
	keys := make([]string, 0, len(p))
	for k := range p {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
func (r *Registry) NativeDevice(d store.Device) store.Device {
	p := r.Match(d)
	if p == nil {
		return d
	}
	return canonical(d, p)
}

func (r *Registry) IsTuya(d store.Device) bool {
	p := r.Match(d)
	return p != nil && (p.Protocol == "tuya" || p.Builtin == "tuya_climate")
}

func eventMatches(e Event, ep byte, cluster uint16, f zcl.Frame) bool {
	if f.Control&3 != 1 || e.Endpoint != 0 && e.Endpoint != ep || e.Cluster != cluster || e.Command != f.Command {
		return false
	}
	if f.Control&4 != 0 && e.Manufacturer != f.Manufacturer || f.Control&4 == 0 && e.Manufacturer != 0 {
		return false
	}
	if e.PayloadHex != nil {
		raw, _ := hex.DecodeString(*e.PayloadHex)
		return bytes.Equal(raw, f.Payload)
	}
	return true
}
