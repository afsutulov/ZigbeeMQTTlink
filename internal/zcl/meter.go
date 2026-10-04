package zcl

import (
	"zigbeemqttlink/internal/store"
)

var electricalScales = map[uint16]string{0x600: "voltage_multiplier", 0x601: "voltage_divisor", 0x602: "current_multiplier", 0x603: "current_divisor", 0x604: "power_multiplier", 0x605: "power_divisor"}

func ApplyMeasurementScales(d *store.Device, ep byte, cluster uint16, f Frame) error {
	if d.Model != "TS011F" || (cluster != 0xb04 && cluster != 0x702) {
		return nil
	}
	attrs, err := Attributes(f)
	if err != nil {
		return err
	}
	for i := range d.Endpoints {
		if d.Endpoints[i].ID != ep {
			continue
		}
		legacy := legacyMeterManufacturers[d.Manufacturer] || d.Manufacturer == "Zbeacon"
		if legacy {
			if d.Endpoints[i].Scales == nil {
				d.Endpoints[i].Scales = map[string]float64{}
			}
			base := map[string]float64{"voltage_multiplier": 1, "voltage_divisor": 1, "current_multiplier": 1, "current_divisor": 1000, "power_multiplier": 1, "power_divisor": 1, "energy_multiplier": 1, "energy_divisor": 100}
			if d.Manufacturer == "_TZ3000_typdpbpg" {
				base["current_divisor"] = 2000
			}
			for key, v := range base {
				if d.Endpoints[i].Scales[key] <= 0 {
					d.Endpoints[i].Scales[key] = v
				}
			}
		}
		for _, a := range attrs {
			key := electricalScales[a.ID]
			if cluster == 0x702 {
				key = map[uint16]string{0x301: "energy_multiplier", 0x302: "energy_divisor"}[a.ID]
			}
			if legacy && cluster == 0xb04 && (key == "current_multiplier" || key == "current_divisor") {
				continue
			}
			if v, ok := numeric(a.Value); key != "" && ok && v > 0 && v <= 0xffffffff {
				if d.Endpoints[i].Scales == nil {
					d.Endpoints[i].Scales = map[string]float64{}
				}
				d.Endpoints[i].Scales[key] = v
			}
		}
	}
	return nil
}

func meterState(d store.Device, ep byte, cluster uint16, attrs []Attribute, out map[string]any) {
	if d.Model != "TS011F" || (cluster != 0xb04 && cluster != 0x702) {
		return
	}
	var scales map[string]float64
	for _, e := range d.Endpoints {
		if e.ID == ep {
			scales = e.Scales
		}
	}
	for _, a := range attrs {
		key := ""
		if cluster == 0xb04 {
			key = map[uint16]string{0x505: "voltage", 0x508: "current", 0x50b: "power"}[a.ID]
		}
		if cluster == 0x702 && a.ID == 0 {
			key = "energy"
		}
		v, ok := numeric(a.Value)
		if key == "" || !ok {
			continue
		}
		mult, div := scales[key+"_multiplier"], scales[key+"_divisor"]
		if mult > 0 && div > 0 {
			out[key] = v * mult / div
		} else {
			out[key+"_raw"] = v
		}
	}
}

func meterReads(d store.Device) []Command {
	commands := []Command{}
	for _, ep := range d.Endpoints {
		if ep.Profile != 0x104 {
			continue
		}
		for _, cluster := range ep.In {
			var ids []uint16
			if cluster == 0xb04 {
				ids = []uint16{0x600, 0x601, 0x602, 0x603, 0x604, 0x605, 0x505, 0x508, 0x50b}
			}
			if cluster == 0x702 {
				ids = []uint16{0x301, 0x302, 0}
			}
			for _, id := range ids {
				commands = append(commands, Command{Endpoint: ep.ID, Cluster: cluster, Control: 0x10, ID: 0, Payload: u16(id)})
			}
		}
	}
	return commands
}
