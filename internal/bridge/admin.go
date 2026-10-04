package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
)

func (b *Bridge) sendCommands(ctx context.Context, d store.Device, commands []zcl.Command) error {
	for i, c := range commands {
		if i > 0 && b.definitions.Delay(d) > 0 {
			// Individual MCU writes need separation; stopping with a single
			// alarm:false command is never delayed here.
			timer := time.NewTimer(b.definitions.Delay(d))
			select {
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("command %d/%d cancelled; preceding commands may have applied: %w", i+1, len(commands), ctx.Err())
			case <-timer.C:
			}
		}
		b.log.Debug("sending Zigbee command", "device", d.Name, "model", d.Model, "address", d.Network, "endpoint", c.Endpoint, "cluster", fmt.Sprintf("0x%04x", c.Cluster), "command", fmt.Sprintf("0x%02x", c.ID))
		if err := b.radio.Send(ctx, d.Network, c.Endpoint, c.Cluster, zcl.WireTransaction(c, b.seq.Add(1))); err != nil {
			return fmt.Errorf("command %d/%d failed; preceding commands may have applied: %w", i+1, len(commands), err)
		}
	}
	return nil
}

func (b *Bridge) sendDevice(ctx context.Context, d store.Device, p map[string]any, get bool) error {
	var err error
	d, p, err = b.resolveCommand(d.IEEE, p)
	if err != nil {
		return err
	}
	commands, err := b.definitions.Commands(d, p, get)
	if err != nil {
		return err
	}
	if err = b.sendCommands(ctx, d, commands); err != nil {
		return err
	}
	b.diagnostics.transportAccepted.Add(1)
	op := "set"
	if get {
		op = "get"
	}
	b.log.Info("command accepted by Zigbee transport; state awaits device report", "device", d.Name, "operation", op)
	return nil
}

func textField(p map[string]any, key string) string { s, _ := p[key].(string); return s }

func (b *Bridge) clearRetained(name string) error {
	if b.client == nil || !b.client.IsConnectionOpen() {
		return fmt.Errorf("MQTT disconnected")
	}
	return b.wait(b.client.Publish(b.cfg.MQTT.BaseTopic+"/"+name, 1, true, []byte{}))
}

func (b *Bridge) adminMetadata(data map[string]any, obsolete ...string) map[string]any {
	var err error
	for _, name := range obsolete {
		if e := b.clearRetained(name); e != nil {
			err = e
		}
	}
	if e := b.metadata(); e != nil {
		err = e
	}
	data["metadata_synced"] = err == nil
	if err != nil {
		data["warning"] = "local change saved; MQTT metadata update failed: " + err.Error()
	}
	return data
}

// MQTT and HTTP use the same administrative operations.
func (b *Bridge) adminRequest(ctx context.Context, request string, p map[string]any) (map[string]any, error) {
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	if request == "definitions/reload" {
		if err := b.definitions.Reload(); err != nil {
			return nil, err
		}
		b.log.Info("device definitions reloaded", "count", len(b.definitions.Snapshot().Devices))
		return map[string]any{"reloaded": true, "count": len(b.definitions.Snapshot().Devices)}, nil
	}
	if request == "definitions/save" {
		raw, ok := p["definitions"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("definitions must be a JSON object")
		}
		bytes, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		if err = b.definitions.Save(bytes); err != nil {
			return nil, err
		}
		b.log.Info("device definitions saved and activated", "count", len(b.definitions.Snapshot().Devices))
		return map[string]any{"reloaded": true}, nil
	}
	if request == "health_check" {
		return b.Health(), nil
	}
	if request == "permit_join" {
		v, ok := p["time"].(float64)
		if !ok || v < 0 || v > 254 || v != float64(int(v)) {
			return nil, fmt.Errorf("time must be integer 0..254")
		}
		if err := b.radio.PermitJoin(ctx, byte(v)); err != nil {
			return nil, err
		}
		until := int64(0)
		if v > 0 {
			until = time.Now().Add(time.Duration(v) * time.Second).Unix()
		}
		b.joinUntil.Store(until)
		b.log.Info("pairing mode changed", "seconds", v)
		return map[string]any{"time": v, "permit_join": v > 0}, nil
	}
	id := textField(p, "id")
	if id == "" {
		id = textField(p, "from")
	}
	d, ok := b.store.Find(id)
	if !ok {
		return nil, fmt.Errorf("unknown device %q", id)
	}
	switch request {
	case "device/rename":
		name := textField(p, "to")
		if name == "" {
			name = textField(p, "friendly_name")
		}
		updated, err := b.store.Rename(d.IEEE, name)
		if err != nil {
			return nil, err
		}
		return b.adminMetadata(map[string]any{"device": updated}, d.Name), nil
	case "device/options":
		raw, ok := p["options"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("options must be a JSON object")
		}
		options := map[string]float64{}
		for k, v := range raw {
			n, ok := v.(float64)
			if !ok {
				return nil, fmt.Errorf("calibration values must be numbers")
			}
			options[k] = n
		}
		if err := b.store.SetOptions(d.IEEE, options); err != nil {
			return nil, err
		}
		updated, _ := b.store.Find(d.IEEE)
		return b.adminMetadata(map[string]any{"device": updated}), nil
	case "device/replace":
		new, ok := b.store.Find(textField(p, "to"))
		if !ok {
			return nil, fmt.Errorf("replacement must first be paired")
		}
		updated, err := b.store.Replace(d.IEEE, new.IEEE)
		if err != nil {
			return nil, err
		}
		b.log.Info("device replaced; MQTT name preserved", "old_ieee", d.IEEE, "new_ieee", updated.IEEE, "friendly_name", updated.Name)
		return b.adminMetadata(map[string]any{"device": updated, "hardware_settings_copied": false}, d.Name, new.Name), nil
	case "device/remove":
		force := false
		if v, exists := p["force"]; exists {
			var valid bool
			force, valid = v.(bool)
			if !valid {
				return nil, fmt.Errorf("force must be boolean")
			}
		}
		leaveRequested := false
		if !force {
			if a, ok := b.radio.(interface {
				Leave(context.Context, store.Device) error
			}); ok {
				if err := a.Leave(ctx, d); err != nil {
					return nil, err
				}
				leaveRequested = true
			} else {
				return nil, fmt.Errorf("radio does not support removal; force=true only forgets the device locally")
			}
		}
		if err := b.store.Remove(d.IEEE); err != nil {
			return nil, err
		}
		return b.adminMetadata(map[string]any{"id": d.IEEE, "removed": true, "leave_requested": leaveRequested, "force": force}, d.Name), nil
	case "device/set", "device/get":
		payload, ok := p["payload"].(map[string]any)
		if !ok {
			return nil, fmt.Errorf("payload must be a JSON object")
		}
		if err := b.sendDevice(ctx, d, payload, request == "device/get"); err != nil {
			b.diagnostics.rejectedCommands.Add(1)
			return nil, err
		}
		return map[string]any{"transport_accepted": true, "state_confirmed": false}, nil
	case "device/configure":
		commands, err := b.definitions.Configure(d)
		if err != nil {
			return nil, err
		}
		if err := b.sendCommands(ctx, d, commands); err != nil {
			return nil, err
		}
		reads := map[string]any{}
		for name := range b.definitions.Channels(d) {
			reads["state_"+name] = ""
		}
		if len(reads) > 0 {
			if err := b.sendDevice(ctx, d, reads, true); err != nil {
				return nil, err
			}
		}
		return map[string]any{"transport_accepted": true, "state_confirmed": false}, nil
	default:
		return nil, fmt.Errorf("request %q is not implemented", request)
	}
}
