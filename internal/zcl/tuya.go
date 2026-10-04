package zcl

import (
	"zigbeemqttlink/internal/store"
	"fmt"
	"strings"
)

// TS011F's legacy power_outage_memory and power_on_behavior use the
// manufacturer-independent genOnOff/0x8002 ENUM8 attribute (Tuya converter).
func tuyaCommands(d store.Device, p map[string]any, get bool) ([]Command, error) {
	standard := map[string]any{}
	meter := []Command{}
	var setting *Command
	for key, v := range p {
		if get && (key == "power" || key == "current" || key == "voltage" || key == "energy") {
			cluster, id := uint16(0xb04), map[string]uint16{"power": 0x50b, "current": 0x508, "voltage": 0x505}[key]
			if key == "energy" {
				cluster, id = 0x702, 0
			}
			ep, err := endpoint(d, cluster)
			if err != nil {
				return nil, err
			}
			meter = append(meter, Command{Endpoint: ep, Cluster: cluster, Control: 0x10, ID: 0, Payload: u16(id)})
			continue
		}
		if key != "power_outage_memory" && key != "power_on_behavior" {
			standard[key] = v
			continue
		}
		if setting != nil {
			return nil, fmt.Errorf("power_outage_memory and power_on_behavior refer to the same setting; use one")
		}
		ep, err := endpoint(d, 6)
		if err != nil {
			return nil, err
		}
		c := Command{Endpoint: ep, Cluster: 6, Control: 0x10, ID: 0, Payload: u16(0x8002)}
		if !get {
			s, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string: off/on/%s", key, map[string]string{"power_outage_memory": "restore", "power_on_behavior": "previous"}[key])
			}
			s = strings.ToLower(s)
			x := byte(0)
			switch s {
			case "off":
				x = 0
			case "on":
				x = 1
			case "restore":
				if key != "power_outage_memory" {
					return nil, fmt.Errorf("use previous for power_on_behavior")
				}
				x = 2
			case "previous":
				if key != "power_on_behavior" {
					return nil, fmt.Errorf("use restore for power_outage_memory")
				}
				x = 2
			default:
				return nil, fmt.Errorf("invalid %s value %q", key, s)
			}
			c.ID = 2
			c.Payload = append(c.Payload, 0x30, x)
		}
		setting = &c
	}
	var commands []Command
	if len(standard) > 0 {
		var err error
		if get {
			commands, err = Get(d, standard)
		} else {
			commands, err = Set(d, standard)
		}
		if err != nil {
			return nil, err
		}
	}
	// Apply state first so a slow settings write cannot delay an OFF command.
	if setting != nil {
		commands = append(commands, *setting)
	}
	commands = append(commands, meter...)
	if len(commands) == 0 {
		return nil, fmt.Errorf("empty request")
	}
	return commands, nil
}

func tuyaState(cluster uint16, attrs []Attribute, out map[string]any) {
	if cluster != 6 && cluster != 0xe001 {
		return
	}
	for _, a := range attrs {
		if (cluster == 6 && a.ID == 0x8002) || (cluster == 0xe001 && a.ID == 0xd010 && a.Type == 0x30) {
			v, ok := numeric(a.Value)
			if !ok {
				continue
			}
			if s, ok := map[float64]string{0: "off", 1: "on", 2: "restore"}[v]; ok {
				out["power_outage_memory"] = s
				out["power_on_behavior"] = map[float64]string{0: "off", 1: "on", 2: "previous"}[v]
			}
		}
	}
}
