package bridge

import (
	"context"
	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/device"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
	"zigbeemqttlink/internal/znp"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Version = "0.1.1"

type Radio interface {
	Events() <-chan znp.Frame
	Done() <-chan struct{}
	Err() error
	Send(context.Context, uint16, byte, uint16, []byte) error
	PermitJoin(context.Context, byte) error
}
type acknowledgement struct {
	network  uint16
	endpoint byte
	cluster  uint16
	data     []byte
}

type job struct {
	ack       *acknowledgement
	topic     string
	payload   []byte
	announce  *store.Device
	configure *store.Device
}
type Bridge struct {
	cfg         config.Config
	store       *store.Store
	radio       Radio
	client      mqtt.Client
	queue       chan job
	seq         atomic.Uint32
	log         *slog.Logger
	ready       atomic.Bool
	errors      atomic.Uint64
	definitions *device.Registry
	joinUntil   atomic.Int64
	diagnostics diagnostics
	unconverted map[string]bool // Accessed only by the radio event loop; bounded below.
	adminMu     sync.Mutex
	lastActions map[string]actionStamp
}

type actionStamp struct {
	key      string
	received time.Time
}

func New(c config.Config, s *store.Store, r Radio, l *slog.Logger, definitions *device.Registry) *Bridge {
	return &Bridge{cfg: c, store: s, radio: r, queue: make(chan job, 128), log: l, definitions: definitions, unconverted: make(map[string]bool), lastActions: make(map[string]actionStamp)}
}
func (b *Bridge) wait(t mqtt.Token) error {
	if !t.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("MQTT operation timeout")
	}
	return t.Error()
}
func (b *Bridge) Publish(topic string, payload any, retain bool) error {
	if !config.Topic(topic) {
		return fmt.Errorf("invalid publish topic")
	}
	v, e := json.Marshal(payload)
	if e != nil {
		return e
	}
	if b.client == nil || !b.client.IsConnectionOpen() {
		return fmt.Errorf("MQTT disconnected")
	}
	return b.wait(b.client.Publish(b.cfg.MQTT.BaseTopic+"/"+topic, 1, retain && !b.cfg.MQTT.ForceDisableRetain, v))
}
func (b *Bridge) metadata() error {
	if e := b.Publish("bridge/info", map[string]any{"version": Version, "project": "ZigbeeMQTTlink", "device_definitions": len(b.definitions.Snapshot().Devices), "supported_requests": []string{"permit_join", "health_check", "device/rename", "device/remove", "device/replace", "device/options", "device/configure"}}, true); e != nil {
		return e
	}
	if e := b.Publish("bridge/devices", b.store.Devices(), true); e != nil {
		return e
	}
	for _, d := range b.store.Devices() {
		if s := b.store.State(d.IEEE); len(s) > 0 {
			if e := b.Publish(d.Name, s, true); e != nil {
				return e
			}
		}
	}
	return b.Publish("bridge/state", map[string]string{"state": "online"}, true)
}
func (b *Bridge) Run(ctx context.Context) (runErr error) {
	tlsCfg, e := b.cfg.MQTT.TLS()
	if e != nil {
		return e
	}
	o := mqtt.NewClientOptions().AddBroker(b.cfg.MQTT.BrokerURL()).SetClientID(b.cfg.MQTT.ClientID).SetUsername(b.cfg.MQTT.User).SetPassword(b.cfg.MQTT.Password).SetTLSConfig(tlsCfg).SetProtocolVersion(4).SetCleanSession(true).SetAutoReconnect(true).SetConnectTimeout(10 * time.Second).SetWriteTimeout(10 * time.Second).SetKeepAlive(30 * time.Second).SetPingTimeout(5 * time.Second).SetOrderMatters(false)
	o.SetWill(b.cfg.MQTT.BaseTopic+"/bridge/state", `{"state":"offline"}`, 1, !b.cfg.MQTT.ForceDisableRetain)
	o.SetConnectionLostHandler(func(_ mqtt.Client, e error) { b.ready.Store(false); b.log.Warn("MQTT connection lost", "error", e) })
	o.SetOnConnectHandler(func(c mqtt.Client) {
		go func() {
			b.ready.Store(false)
			e := b.wait(c.Subscribe(b.cfg.MQTT.BaseTopic+"/#", 1, func(_ mqtt.Client, m mqtt.Message) { b.enqueue(m) }))
			if e == nil {
				e = b.metadata()
			}
			if e != nil {
				b.log.Error("MQTT setup failed", "error", e)
				return
			}
			b.ready.Store(true)
			b.log.Info("MQTT subscriptions and metadata ready")
		}()
	})
	b.client = mqtt.NewClient(o)
	if e = b.wait(b.client.Connect()); e != nil {
		return e
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); b.worker(workerCtx) }()
	defer func() {
		b.ready.Store(false)
		cancel()
		<-workerDone
		if e := b.Publish("bridge/state", map[string]string{"state": "offline"}, true); e != nil {
			b.log.Warn("offline publish failed", "error", e)
		}
		b.client.Disconnect(1000)
		if e := b.store.Save(); e != nil {
			b.log.Error("final state save failed", "error", e)
			if runErr == nil {
				runErr = e
			}
		}
	}()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-b.radio.Done():
			return fmt.Errorf("coordinator disconnected: %w", b.radio.Err())
		case f := <-b.radio.Events():
			b.event(workerCtx, f)
		case <-ticker.C:
			b.log.Debug("diagnostic counters", "counters", b.diagnostics.snapshot())
			if e := b.store.Save(); e != nil {
				b.errors.Add(1)
				b.log.Error("state save failed", "error", e)
			}
		}
	}
}
func (b *Bridge) enqueue(m mqtt.Message) {
	prefix := b.cfg.MQTT.BaseTopic + "/"
	if !strings.HasPrefix(m.Topic(), prefix) {
		return
	}
	topic := strings.TrimPrefix(m.Topic(), prefix)
	if !(strings.HasPrefix(topic, "bridge/request/") || commandTopic.MatchString(topic)) {
		return
	}
	b.diagnostics.mqttCommands.Add(1)
	b.log.Info("MQTT command received", "topic", topic, "retained", m.Retained(), "bytes", len(m.Payload()))
	if m.Retained() {
		b.diagnostics.retainedIgnored.Add(1)
		b.log.Warn("retained command replay ignored", "topic", topic)
		return
	}
	if len(m.Payload()) > 64<<10 {
		b.diagnostics.rejectedCommands.Add(1)
		b.errors.Add(1)
		b.log.Warn("command too large", "topic", topic)
		return
	}
	j := job{topic: topic, payload: append([]byte(nil), m.Payload()...)}
	select {
	case b.queue <- j:
	default:
		b.diagnostics.rejectedCommands.Add(1)
		b.errors.Add(1)
		b.log.Error("command queue full", "topic", topic)
	}
}
func (b *Bridge) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j := <-b.queue:
			if ctx.Err() != nil {
				return
			}
			if j.ack != nil {
				ackCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				a := j.ack
				if e := b.radio.Send(ackCtx, a.network, a.endpoint, a.cluster, a.data); e != nil {
					b.log.Warn("default response failed", "error", e)
				}
				cancel()
				continue
			}
			if j.announce != nil {
				b.interview(ctx, *j.announce)
				continue
			}
			if j.configure != nil {
				op, cancel := context.WithTimeout(ctx, 10*time.Second)
				if _, e := b.adminRequest(op, "device/configure", map[string]any{"id": j.configure.IEEE}); e != nil {
					b.log.Warn("automatic device configuration failed", "device", j.configure.Name, "error", e)
				}
				cancel()
				continue
			}
			b.command(ctx, j)
		}
	}
}
func (b *Bridge) command(ctx context.Context, j job) {
	opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if strings.HasPrefix(j.topic, "bridge/request/") {
		var p map[string]any
		if e := json.Unmarshal(j.payload, &p); e != nil || p == nil {
			b.commandError(j, p, fmt.Errorf("payload must be a JSON object"))
			return
		}
		req := strings.TrimPrefix(j.topic, "bridge/request/")
		data, e := b.adminRequest(opCtx, req, p)
		if e != nil {
			b.commandError(j, p, e)
			return
		}
		response := map[string]any{"status": "ok", "data": data}
		if t, ok := p["transaction"]; ok {
			response["transaction"] = t
		}
		if e = b.Publish("bridge/response/"+req, response, false); e != nil {
			b.log.Error("response publish failed", "error", e)
		}
		return
	}
	target, op, p, e := parseCommand(j.topic, j.payload)
	if e != nil {
		b.commandError(j, p, e)
		return
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	d, p, e := b.resolveCommand(target, p)
	if e != nil {
		b.commandError(j, p, e)
		return
	}
	if e := b.sendDevice(opCtx, d, p, op == "get"); e != nil {
		b.commandError(j, p, e)
	}
}
func (b *Bridge) commandError(j job, p map[string]any, e error) {
	b.diagnostics.rejectedCommands.Add(1)
	b.errors.Add(1)
	b.log.Warn("command rejected", "topic", j.topic, "error", e)
	if strings.HasPrefix(j.topic, "bridge/request/") {
		response := map[string]any{"status": "error", "error": e.Error(), "data": map[string]any{}}
		if t, ok := p["transaction"]; ok {
			response["transaction"] = t
		}
		if pe := b.Publish(strings.Replace(j.topic, "bridge/request/", "bridge/response/", 1), response, false); pe != nil {
			b.log.Warn("error response publish failed", "error", pe)
		}
	}
}
func (b *Bridge) event(ctx context.Context, f znp.Frame) {
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	b.diagnostics.radioEvents.Add(1)
	b.diagnostics.lastRadioEvent.Store(time.Now().Unix())
	b.log.Debug("radio event received", "cmd0", fmt.Sprintf("0x%02x", f.Cmd0), "cmd1", fmt.Sprintf("0x%02x", f.Cmd1), "bytes", len(f.Data))
	if f.Cmd0 == 0x45 && f.Cmd1 == 0xc1 && len(f.Data) == 13 {
		n := binary.LittleEndian.Uint16(f.Data[2:4])
		id := znp.Addr(f.Data[4:12])
		d, ok := b.store.ByIEEE(id)
		if !ok {
			if time.Now().Unix() >= b.joinUntil.Load() {
				b.log.Warn("announce from unregistered device ignored while pairing is closed", "device", id)
				return
			}
			d = store.Device{IEEE: id, Name: id, Type: "EndDevice", Endpoints: []store.Endpoint{}}
			if f.Data[12]&2 != 0 {
				d.Type = "Router"
			}
		}
		d.Network = n
		d.LastSeen = time.Now().UTC()
		if e := b.store.Put(d); e != nil {
			b.log.Warn("announce rejected", "error", e)
			return
		}
		select {
		case b.queue <- job{announce: &d}:
		default:
			b.log.Warn("interview queue full")
		}
		return
	}
	if f.Cmd0 == 0x45 && f.Cmd1 == 0xc9 {
		b.log.Warn("device leave event received; automatic removal is not implemented")
		return
	}
	if f.Cmd0 != 0x44 || f.Cmd1 != 0x81 {
		if f.Cmd0 == 0x44 && f.Cmd1 == 0x82 {
			b.diagnostics.unsupportedExtended.Add(1)
			if b.diagnostics.unsupportedExtended.Load() == 1 {
				b.log.Warn("AF_INCOMING_MSG_EXT is not implemented; extended incoming frames cannot be converted")
			}
		}
		return
	}
	b.diagnostics.incomingFrames.Add(1)
	in, e := znp.ParseIncoming(f.Data)
	if e != nil {
		b.diagnostics.invalidFrames.Add(1)
		b.log.Warn("invalid incoming frame", "error", e)
		return
	}
	b.log.Debug("Zigbee packet received", "address", in.Network, "endpoint", in.Endpoint, "cluster", fmt.Sprintf("0x%04x", in.Cluster), "linkquality", in.LQI, "zcl_hex", hex.EncodeToString(in.ZCL))
	d, ok := b.store.ByNetwork(in.Network)
	if !ok {
		b.diagnostics.unknownAddress.Add(1)
		b.log.Warn("report from unknown address; database entry or device announce required", "address", in.Network)
		return
	}
	z, e := zcl.Parse(in.ZCL)
	if e != nil {
		b.diagnostics.invalidFrames.Add(1)
		b.log.Warn("invalid ZCL", "error", e)
		return
	}
	if in.Cluster == 0x0a && z.Control&7 == 0 && z.Command == 0 && z.Control&8 == 0 {
		d.LastSeen = time.Now().UTC()
		if err := b.store.Refresh(d); err != nil {
			return
		}
		response, err := zcl.TimeResponse(z, time.Now())
		if err != nil {
			b.log.Warn("invalid Time request", "device", d.Name, "error", err)
			return
		}
		select {
		case b.queue <- job{ack: &acknowledgement{in.Network, in.Endpoint, in.Cluster, response}}:
			b.log.Debug("Time response queued", "device", d.Name)
		default:
			b.errors.Add(1)
			b.log.Warn("Time response queue full")
		}
		return
	}
	if in.Cluster == 0xef00 {
		if response, err := b.definitions.GatewayResponse(d, z); err != nil {
			b.log.Warn("invalid Tuya gateway request", "device", d.Name, "error", err)
			return
		} else if response != nil {
			d.LastSeen = time.Now().UTC()
			if err = b.store.Refresh(d); err != nil {
				return
			}
			select {
			case b.queue <- job{ack: &acknowledgement{in.Network, in.Endpoint, in.Cluster, response}}:
			default:
				b.log.Warn("Tuya gateway response queue full")
			}
			return
		}
		if response, err := b.definitions.TimeResponse(d, z, time.Now()); err != nil {
			b.log.Warn("invalid Tuya time request", "device", d.Name, "error", err)
			return
		} else if response != nil {
			d.LastSeen = time.Now().UTC()
			if err = b.store.Refresh(d); err != nil {
				return
			}
			select {
			case b.queue <- job{ack: &acknowledgement{in.Network, in.Endpoint, in.Cluster, response}}:
			default:
				b.log.Warn("Tuya time response queue full")
			}
			return
		}
	}
	if c, err := zcl.PreventReset(b.definitions.NativeDevice(d), in.Cluster, z); err != nil {
		b.log.Warn("invalid reset-handshake frame", "device", d.Name, "error", err)
		return
	} else if c != nil {
		d.LastSeen = time.Now().UTC()
		if err := b.store.Refresh(d); err != nil {
			return
		}
		select {
		case b.queue <- job{ack: &acknowledgement{in.Network, c.Endpoint, c.Cluster, zcl.Wire(*c, byte(b.seq.Add(1)))}}:
			b.log.Debug("Aqara reset-handshake response queued", "device", d.Name)
		default:
			b.errors.Add(1)
			b.log.Warn("reset-handshake response queue full")
		}
		return
	}
	b.log.Debug("ZCL frame received", "device", d.Name, "model", d.Model, "endpoint", in.Endpoint, "cluster", fmt.Sprintf("0x%04x", in.Cluster), "control", fmt.Sprintf("0x%02x", z.Control), "manufacturer_code", z.Manufacturer, "command", fmt.Sprintf("0x%02x", z.Command), "payload_hex", hex.EncodeToString(z.Payload))
	if handled, err := zcl.CommandResponse(z); handled {
		d.LastSeen = time.Now().UTC()
		if e := b.store.Refresh(d); e != nil {
			b.log.Warn("device response update rejected", "error", e)
		}
		if err != nil {
			b.errors.Add(1)
			b.log.Warn("device rejected ZCL command", "device", d.Name, "cluster", fmt.Sprintf("0x%04x", in.Cluster), "sequence", z.Seq, "error", err)
		} else {
			b.log.Debug("ZCL command acknowledged", "device", d.Name, "sequence", z.Seq)
		}
		return
	}
	nativeDevice := b.definitions.NativeDevice(d)
	if err := zcl.ApplyMeasurementScales(&nativeDevice, in.Endpoint, in.Cluster, z); err != nil {
		b.diagnostics.conversionErrors.Add(1)
		b.log.Warn("invalid measurement scale report", "device", d.Name, "error", err)
		return
	}
	d.Endpoints = nativeDevice.Endpoints
	update, e := b.definitions.State(d, in.Endpoint, in.Cluster, z)
	if e != nil {
		b.diagnostics.conversionErrors.Add(1)
		b.log.Warn("unsupported/invalid ZCL report", "device", d.Name, "model", d.Model, "endpoint", in.Endpoint, "cluster", fmt.Sprintf("0x%04x", in.Cluster), "control", fmt.Sprintf("0x%02x", z.Control), "manufacturer", d.Manufacturer, "manufacturer_code", z.Manufacturer, "command", fmt.Sprintf("0x%02x", z.Command), "payload_hex", hex.EncodeToString(z.Payload), "error", e)
		return
	}
	if len(update) == 0 {
		b.diagnostics.noStateUpdate.Add(1)
		key := fmt.Sprintf("%s/%d/%d/%d/%d/%d", d.IEEE, in.Endpoint, in.Cluster, z.Control, z.Manufacturer, z.Command)
		routine := z.Control&3 == 0 && (z.Command == 1 || z.Command == 0x0a) && (in.Cluster == 0 || (d.Model == "TS011F" && (in.Cluster == 0xb04 || in.Cluster == 0x702)))
		if routine {
			b.log.Debug("metadata report contains no state update", "device", d.Name, "cluster", fmt.Sprintf("0x%04x", in.Cluster), "payload_hex", hex.EncodeToString(z.Payload))
		} else if !b.unconverted[key] && len(b.unconverted) < 256 {
			b.unconverted[key] = true
			b.log.Warn("Zigbee frame produced no supported state update; set logging.level to debug", "device", d.Name, "model", d.Model, "endpoint", in.Endpoint, "cluster", fmt.Sprintf("0x%04x", in.Cluster), "control", fmt.Sprintf("0x%02x", z.Control), "manufacturer", d.Manufacturer, "manufacturer_code", z.Manufacturer, "command", fmt.Sprintf("0x%02x", z.Command), "payload_hex", hex.EncodeToString(z.Payload))
		}
	}
	if d.Model == "TS011F" && (update["energy_raw"] != nil || update["power_raw"] != nil || update["current_raw"] != nil || update["voltage_raw"] != nil) {
		key := d.IEEE + "/measurement-scales-requested"
		if !b.unconverted[key] {
			select {
			case b.queue <- job{configure: &d}:
				b.unconverted[key] = true
			default:
				b.log.Warn("meter configuration queue full", "device", d.Name)
			}
		}
	}
	d.LastSeen = time.Now().UTC()
	previousModel := d.Model
	previousManufacturer := d.Manufacturer
	if s, ok := update["manufacturer"].(string); ok {
		d.Manufacturer = s
	}
	if s, ok := update["model_id"].(string); ok {
		d.Model = s
	}
	if e = b.store.Refresh(d); e != nil {
		b.log.Warn("device update rejected", "error", e)
		return
	}
	if (d.Model != previousModel || d.Manufacturer != previousManufacturer) && b.definitions.HasConfigure(d) {
		select {
		case b.queue <- job{configure: &d}:
		default:
			b.log.Warn("configuration queue full")
		}
	}
	// Standard default response to a server-to-client report requiring acknowledgement.
	convertedEvent := update["action"] != nil && z.Control&3 == 1
	if z.Control&0x10 == 0 && (z.Control&8 != 0 || convertedEvent) && (z.Control&4 == 0 || len(update) > 0 || (zcl.IsLumi(d.Model) && z.Manufacturer == 0x115f)) && ((z.Command == 0x0a && z.Control&3 == 0) || (b.definitions.IsTuya(d) && in.Cluster == 0xef00 && z.Control&3 == 1 && (z.Command == 1 || z.Command == 2)) || convertedEvent) {
		responseControl := byte(0x10) | (z.Control^0x08)&0x08
		ack := zcl.Wire(zcl.Command{Control: responseControl, Manufacturer: z.Manufacturer, ID: 0x0b, Payload: []byte{z.Command, 0}}, z.Seq)
		select {
		case b.queue <- job{ack: &acknowledgement{in.Network, in.Endpoint, in.Cluster, ack}}:
		default:
			b.errors.Add(1)
			b.log.Warn("ack queue full")
		}

	}
	if len(update) > 0 {
		if _, ok := update["action"]; ok {
			key := fmt.Sprintf("%d/%d/%x", in.Endpoint, in.Cluster, in.ZCL)
			previous := b.lastActions[d.IEEE]
			if previous.key == key && time.Since(previous.received) < 2*time.Second {
				b.log.Debug("duplicate action report ignored", "device", d.Name)
				return
			}
			if previous.key != "" || len(b.lastActions) < 256 {
				b.lastActions[d.IEEE] = actionStamp{key, time.Now()}
			}
		}
		store.ApplyOptions(d, update)
		b.diagnostics.convertedReports.Add(1)
		update["linkquality"] = in.LQI
		update["last_seen"] = d.LastSeen.Format(time.RFC3339Nano)
		state := b.store.Update(d.IEEE, update)
		_, isAction := update["action"]
		if e = b.Publish(d.Name, state, !isAction); e != nil {
			b.diagnostics.publishErrors.Add(1)
			b.log.Warn("state publish failed", "error", e)
		} else {
			b.diagnostics.publishedReports.Add(1)
			b.log.Debug("device state published", "device", d.Name, "topic", b.cfg.MQTT.BaseTopic+"/"+d.Name)
		}
	}

}
func (b *Bridge) interview(ctx context.Context, d store.Device) {
	a, ok := b.radio.(*znp.Adapter)
	if !ok {
		return
	}
	op, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	eps, e := a.ActiveEndpoints(op, d.Network)
	if e != nil {
		b.log.Warn("interview incomplete", "device", d.Name, "error", e)
		return
	}
	descriptors := []store.Endpoint{}
	for _, ep := range eps {
		if ep == 242 {
			continue
		}
		desc, e := a.Descriptor(op, d.Network, ep)
		if e != nil {
			b.log.Warn("descriptor failed", "error", e)
			return
		}
		descriptors = append(descriptors, desc)
	}
	b.adminMu.Lock()
	defer b.adminMu.Unlock()
	current, ok := b.store.ByIEEE(d.IEEE)
	if !ok || current.Network != d.Network {
		return
	}
	for i := range descriptors {
		for _, old := range current.Endpoints {
			if old.ID == descriptors[i].ID {
				descriptors[i].Scales = old.Scales
			}
		}
	}
	current.Endpoints = descriptors
	if e = b.store.Refresh(current); e != nil {
		b.log.Warn("interview save rejected", "error", e)
		return
	}
	if e = b.store.Save(); e != nil {
		b.log.Error("interview persistence failed", "error", e)
		return
	}
	if e = b.Publish("bridge/devices", b.store.Devices(), true); e != nil {
		b.log.Warn("device list publish failed", "error", e)
	}
	for _, ep := range descriptors {
		if ep.Profile != 0x104 {
			continue
		}
		for _, cl := range ep.In {
			if cl == 0 {
				seq := byte(b.seq.Add(1))
				if e = b.radio.Send(op, current.Network, ep.ID, 0, zcl.Header(0x10, seq, 0, []byte{4, 0, 5, 0})); e != nil {
					b.log.Warn("basic read failed", "error", e)
				}
				break
			}
		}
	}
}
func (b *Bridge) Health() map[string]any {
	alive := true
	select {
	case <-b.radio.Done():
		alive = false
	default:
	}
	remaining := b.joinUntil.Load() - time.Now().Unix()
	if remaining < 0 {
		remaining = 0
	}
	return map[string]any{"healthy": alive && b.ready.Load(), "mqtt_ready": b.ready.Load(), "coordinator_connected": alive, "queued_commands": len(b.queue), "errors": b.errors.Load(), "permit_join": remaining > 0, "permit_join_remaining": remaining, "diagnostics": b.diagnostics.snapshot()}
}
