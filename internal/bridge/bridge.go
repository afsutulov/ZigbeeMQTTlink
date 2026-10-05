package bridge

import (
	"context"
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
	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/device"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
	"zigbeemqttlink/internal/znp"
)

const Version = "0.3.3"

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
	ieee     string
}

type job struct {
	topic       string
	received    time.Time
	payload     []byte
	announce    *store.Device
	configure   *store.Device
	lookupEpoch uint64
	resolve     uint16 // non-zero: recover the IEEE behind this short address
}
type Bridge struct {
	cfg             config.Config
	store           *store.Store
	radio           Radio
	client          mqtt.Client
	clientMu        sync.RWMutex
	queue           chan job
	reports         chan publication
	publishMu       sync.Mutex
	connectionMu    sync.Mutex
	connectionEpoch uint64
	setupMu         sync.Mutex
	operationMu     sync.Mutex
	operations      map[string]*operationLock
	acks            chan acknowledgement
	pendingJobs     atomic.Int64
	seq             atomic.Uint32
	log             *slog.Logger
	ready           atomic.Bool
	errors          atomic.Uint64
	definitions     *device.Registry
	joinUntil       atomic.Int64
	diagnostics     diagnostics
	unconverted     map[string]bool // Accessed only by the radio event loop; bounded below.
	adminMu         sync.Mutex
	lastActions     map[string]actionStamp
	addressLookups  map[uint16]*addressLookup // Accessed only by the radio event loop.
	recoveryResults chan addressResult
	bufferedFrames  int
}

type actionStamp struct {
	key      string
	received time.Time
}

func New(c config.Config, s *store.Store, r Radio, l *slog.Logger, definitions *device.Registry) *Bridge {
	return &Bridge{cfg: c, store: s, radio: r, queue: make(chan job, 128), acks: make(chan acknowledgement, 128), reports: make(chan publication, 256), operations: make(map[string]*operationLock), log: l, definitions: definitions, unconverted: make(map[string]bool), lastActions: make(map[string]actionStamp), addressLookups: make(map[uint16]*addressLookup), recoveryResults: make(chan addressResult, 128)}
}
func (b *Bridge) mqttClient() mqtt.Client {
	b.clientMu.RLock()
	defer b.clientMu.RUnlock()
	return b.client
}
func (b *Bridge) wait(t mqtt.Token) error {
	if !t.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("MQTT operation timeout")
	}
	return t.Error()
}
func (b *Bridge) Publish(topic string, payload any, retain bool) error {
	b.publishMu.Lock()
	defer b.publishMu.Unlock()
	return b.publish(topic, payload, retain)
}

// publish runs with publishMu held.
func (b *Bridge) publish(topic string, payload any, retain bool) error {
	if !config.Topic(topic) {
		return fmt.Errorf("invalid publish topic")
	}
	v, e := json.Marshal(payload)
	if e != nil {
		return e
	}
	client := b.mqttClient()
	if client == nil || !client.IsConnectionOpen() {
		return fmt.Errorf("MQTT disconnected")
	}
	return b.wait(client.Publish(b.cfg.MQTT.BaseTopic+"/"+topic, 1, retain && !b.cfg.MQTT.ForceDisableRetain, v))
}
func (b *Bridge) metadata() error {
	b.publishMu.Lock()
	defer b.publishMu.Unlock()
	if err := b.cleanupRetained(); err != nil {
		return err
	}
	if e := b.publish("bridge/info", map[string]any{"version": Version, "project": "ZigbeeMQTTlink", "device_definitions": len(b.definitions.Snapshot().Devices), "supported_requests": []string{"permit_join", "health_check", "device/rename", "device/remove", "device/replace", "device/options", "device/configure", "device/interview", "device/set", "device/get", "definitions/reload", "definitions/save"}}, true); e != nil {
		return e
	}
	if e := b.publish("bridge/devices", b.store.Devices(), true); e != nil {
		return e
	}
	for _, d := range b.store.Devices() {
		if s := b.store.State(d.IEEE); len(s) > 0 {
			if e := b.publish(d.Name, s, true); e != nil {
				return e
			}
		}
	}
	return b.publish("bridge/state", map[string]string{"state": "online"}, true)
}
func (b *Bridge) Run(ctx context.Context) (runErr error) {
	tlsCfg, e := b.cfg.MQTT.TLS()
	if e != nil {
		return e
	}
	o := mqtt.NewClientOptions().AddBroker(b.cfg.MQTT.BrokerURL()).SetClientID(b.cfg.MQTT.ClientID).SetUsername(b.cfg.MQTT.User).SetPassword(b.cfg.MQTT.Password).SetTLSConfig(tlsCfg).SetProtocolVersion(4).SetCleanSession(true).SetAutoReconnect(true).SetConnectTimeout(10 * time.Second).SetWriteTimeout(10 * time.Second).SetKeepAlive(30 * time.Second).SetPingTimeout(5 * time.Second).SetOrderMatters(true)
	o.SetWill(b.cfg.MQTT.BaseTopic+"/bridge/state", `{"state":"offline"}`, 1, !b.cfg.MQTT.ForceDisableRetain)
	o.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		if c.IsConnectionOpen() {
			return
		} // Ignore a delayed callback from an older connection.
		b.connectionMu.Lock()
		b.connectionEpoch++
		b.ready.Store(false)
		b.connectionMu.Unlock()
		b.log.Warn("MQTT connection lost", "error", err)
	})
	o.SetOnConnectHandler(func(c mqtt.Client) {
		b.connectionMu.Lock()
		b.connectionEpoch++
		epoch := b.connectionEpoch
		b.ready.Store(false)
		b.connectionMu.Unlock()
		go b.setupMQTT(ctx, c, epoch)
	})
	client := mqtt.NewClient(o)
	b.clientMu.Lock()
	b.client = client
	b.clientMu.Unlock()
	if e = b.wait(client.Connect()); e != nil {
		if strings.Contains(strings.ToLower(e.Error()), "not authori") || strings.Contains(e.Error(), "bad user name or password") {
			return fmt.Errorf("MQTT broker rejected the credentials; check mqtt.user/mqtt.password and broker ACL: %w", e)
		}
		return fmt.Errorf("MQTT connect to %s: %w", b.cfg.MQTT.BrokerURL(), e)
	}
	workerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerDone := make(chan struct{})
	ackDone := make(chan struct{})
	publicationDone := make(chan struct{})
	saveDone := make(chan struct{})
	go func() { defer close(workerDone); b.worker(workerCtx) }()
	// Protocol replies (default responses, Time, Tuya/Aqara handshakes) have
	// their own worker: they must not wait behind interviews or user commands.
	go func() {
		defer close(ackDone)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); b.ackWorker(workerCtx) }()
		}
		wg.Wait()
	}()
	go func() { defer close(publicationDone); b.publicationWorker(workerCtx) }()
	go func() { defer close(saveDone); b.saveWorker(workerCtx) }()
	defer func() {
		b.connectionMu.Lock()
		b.connectionEpoch++
		b.ready.Store(false)
		b.connectionMu.Unlock()
		cancel()
		<-workerDone
		<-ackDone
		<-publicationDone
		<-saveDone
		if e := b.Publish("bridge/state", map[string]string{"state": "offline"}, true); e != nil {
			b.log.Warn("offline publish failed", "error", e)
		}
		client.Disconnect(1000)
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
		case result := <-b.recoveryResults:
			b.handleAddressResult(workerCtx, result)
		case f := <-b.radio.Events():
			b.safeEvent(workerCtx, f)
		case <-ticker.C:
			b.pruneRecoveryFrames(time.Now())
			b.log.Debug("diagnostic counters", "counters", b.diagnostics.snapshot())
			if d, ok := b.radio.(interface{ Dropped() uint64 }); ok {
				if n := d.Dropped(); n > b.diagnostics.droppedEvents.Load() {
					b.log.Warn("coordinator events dropped because processing was too slow", "total", n)
					b.diagnostics.droppedEvents.Store(n)
				}
			}
			if d, ok := b.radio.(interface{ FramingErrors() uint64 }); ok {
				if n := d.FramingErrors(); n > b.diagnostics.framingErrors.Load() {
					b.log.Warn("corrupted coordinator frames discarded; check cable/USB power/baud rate", "total", n)
					b.diagnostics.framingErrors.Store(n)
				}
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
	j := job{topic: topic, received: time.Now(), payload: append([]byte(nil), m.Payload()...)}
	select {
	case b.queue <- j:
	default:
		b.diagnostics.rejectedCommands.Add(1)
		b.errors.Add(1)
		b.log.Error("command queue full", "topic", topic)
	}
}
func (b *Bridge) ackWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case a := <-b.acks:
			if ctx.Err() != nil {
				return
			}
			current, ok := b.store.ByIEEE(a.ieee)
			if !ok || current.Network != a.network {
				continue
			}
			// Sleeping end devices are served by their parent's indirect queue,
			// whose confirmation can take ~8 s.
			ackCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if e := b.radio.Send(ackCtx, a.network, a.endpoint, a.cluster, a.data); e != nil {
				b.log.Debug("protocol response not confirmed", "address", a.network, "cluster", fmt.Sprintf("0x%04x", a.cluster), "error", e)
			}
			cancel()
		}
	}
}

// reply queues a protocol response without blocking the radio event loop.
func (b *Bridge) reply(a acknowledgement, what string) {
	select {
	case b.acks <- a:
	default:
		b.errors.Add(1)
		b.log.Warn("protocol response queue full", "response", what)
	}
}
func (b *Bridge) command(ctx context.Context, j job) {
	opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if strings.HasPrefix(j.topic, "bridge/request/") {
		var p map[string]any
		if e := config.StrictJSON(j.payload, &p); e != nil || p == nil {
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
	// No admin lock here: radio I/O may take seconds and must never stall
	// the radio event loop (which would overflow the coordinator queue).
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

// safeEvent isolates one malformed or unexpected frame: a conversion bug must
// cost one report, not the whole service (and every other sensor) via a crash.
func (b *Bridge) safeEvent(ctx context.Context, f znp.Frame) {
	b.safeEventForDevice(ctx, f, "")
}

func (b *Bridge) safeEventForDevice(ctx context.Context, f znp.Frame, expectedIEEE string) {
	defer func() {
		if r := recover(); r != nil {
			b.diagnostics.panics.Add(1)
			b.errors.Add(1)
			b.log.Error("radio frame processing panicked; frame skipped", "panic", fmt.Sprint(r), "cmd0", fmt.Sprintf("0x%02x", f.Cmd0), "cmd1", fmt.Sprintf("0x%02x", f.Cmd1), "data_hex", hex.EncodeToString(f.Data))
		}
	}()
	b.processEvent(ctx, f, expectedIEEE)
}

func (b *Bridge) event(ctx context.Context, f znp.Frame) {
	b.processEvent(ctx, f, "")
}

func (b *Bridge) processEvent(ctx context.Context, f znp.Frame, expectedIEEE string) {
	if expectedIEEE == "" {
		b.diagnostics.radioEvents.Add(1)
		b.diagnostics.lastRadioEvent.Store(time.Now().Unix())
	}
	b.log.Debug("radio event received", "cmd0", fmt.Sprintf("0x%02x", f.Cmd0), "cmd1", fmt.Sprintf("0x%02x", f.Cmd1), "bytes", len(f.Data))
	if f.Cmd0 == 0x45 && f.Cmd1 == 0xc1 && len(f.Data) == 13 {
		n := binary.LittleEndian.Uint16(f.Data[2:4])
		id := znp.Addr(f.Data[4:12])
		d, ok := b.store.ByIEEE(id)
		interviewed := ok && d.Model != "" && len(d.Endpoints) > 0
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
		var displaced []string
		var e error
		if ok {
			displaced, e = b.store.AnnounceKnown(d)
		} else {
			displaced, e = b.store.Announce(d)
		}
		if e != nil {
			b.log.Warn("announce rejected", "error", e)
			return
		}
		for _, name := range displaced {
			b.log.Info("network address taken over; old entry waits for its own announce", "device", name, "address", n)
		}
		if interviewed {
			// A known device rejoined (battery change, power cycle): only its
			// short address may have changed. Re-interviewing a sleeping
			// sensor right after its announce usually times out anyway.
			b.log.Info("known device announced", "device", d.Name, "address", n)
			return
		}
		select {
		case b.queue <- job{announce: &d}:
		default:
			b.log.Warn("interview queue full")
		}
		return
	}
	if f.Cmd0 == 0x45 && f.Cmd1 == 0xca && len(f.Data) == 12 {
		// ZDO_TC_DEV_IND: trust-center join/rejoin with short and IEEE address.
		b.recoverAddress(znp.Addr(f.Data[2:10]), binary.LittleEndian.Uint16(f.Data[0:2]), "trust center indication")
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
	if expectedIEEE == "" {
		b.diagnostics.incomingFrames.Add(1)
	}
	in, e := znp.ParseIncoming(f.Data)
	if e != nil {
		b.diagnostics.invalidFrames.Add(1)
		b.log.Warn("invalid incoming frame", "error", e)
		return
	}
	b.log.Debug("Zigbee packet received", "address", in.Network, "endpoint", in.Endpoint, "cluster", fmt.Sprintf("0x%04x", in.Cluster), "linkquality", in.LQI, "zcl_hex", hex.EncodeToString(in.ZCL))
	d, ok := b.store.ByNetwork(in.Network)
	if !ok {
		if expectedIEEE != "" {
			return
		} // buffered frame must never be reassigned
		b.diagnostics.unknownAddress.Add(1)
		b.log.Warn("report from unknown address; database entry or device announce required", "address", in.Network)
		b.requestAddressRecovery(in.Network, f)
		return
	}
	if expectedIEEE != "" && d.IEEE != expectedIEEE {
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
		if err := b.store.Touch(d.IEEE, d.Network, d.LastSeen); err != nil {
			return
		}
		response, err := zcl.TimeResponse(z, time.Now())
		if err != nil {
			b.log.Warn("invalid Time request", "device", d.Name, "error", err)
			return
		}
		b.reply(acknowledgement{in.Network, in.Endpoint, in.Cluster, response, d.IEEE}, "time")
		return
	}
	if in.Cluster == 0xef00 {
		if response, err := b.definitions.GatewayResponse(d, z); err != nil {
			b.log.Warn("invalid Tuya gateway request", "device", d.Name, "error", err)
			return
		} else if response != nil {
			d.LastSeen = time.Now().UTC()
			if err = b.store.Touch(d.IEEE, d.Network, d.LastSeen); err != nil {
				return
			}
			b.reply(acknowledgement{in.Network, in.Endpoint, in.Cluster, response, d.IEEE}, "tuya gateway status")
			return
		}
		if response, err := b.definitions.TimeResponse(d, z, time.Now()); err != nil {
			b.log.Warn("invalid Tuya time request", "device", d.Name, "error", err)
			return
		} else if response != nil {
			d.LastSeen = time.Now().UTC()
			if err = b.store.Touch(d.IEEE, d.Network, d.LastSeen); err != nil {
				return
			}
			b.reply(acknowledgement{in.Network, in.Endpoint, in.Cluster, response, d.IEEE}, "tuya time")
			return
		}
	}
	if c, err := zcl.PreventReset(b.definitions.NativeDevice(d), in.Cluster, z); err != nil {
		b.log.Warn("invalid reset-handshake frame", "device", d.Name, "error", err)
		return
	} else if c != nil {
		d.LastSeen = time.Now().UTC()
		if err := b.store.Touch(d.IEEE, d.Network, d.LastSeen); err != nil {
			return
		}
		b.reply(acknowledgement{in.Network, c.Endpoint, c.Cluster, zcl.Wire(*c, byte(b.seq.Add(1))), d.IEEE}, "aqara handshake")
		return
	}
	b.log.Debug("ZCL frame received", "device", d.Name, "model", d.Model, "endpoint", in.Endpoint, "cluster", fmt.Sprintf("0x%04x", in.Cluster), "control", fmt.Sprintf("0x%02x", z.Control), "manufacturer_code", z.Manufacturer, "command", fmt.Sprintf("0x%02x", z.Command), "payload_hex", hex.EncodeToString(z.Payload))
	if handled, err := zcl.CommandResponse(z); handled {
		d.LastSeen = time.Now().UTC()
		if e := b.store.Touch(d.IEEE, d.Network, d.LastSeen); e != nil {
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
	if e = b.store.RefreshReport(d); e != nil {
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
		b.reply(acknowledgement{in.Network, in.Endpoint, in.Cluster, ack, d.IEEE}, "default response")

	}
	if len(update) > 0 {
		current, exists := b.store.ByIEEE(d.IEEE)
		if !exists || current.Network != d.Network {
			return
		}
		d = current
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
		b.queueReport(d, state, !isAction)
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
	current, e := b.store.SetEndpoints(d.IEEE, d.Network, descriptors)
	if e != nil {
		b.log.Warn("interview save rejected", "error", e)
		return
	}
	e = b.store.Save()
	if e != nil {
		b.log.Error("interview persistence failed", "error", e)
		return
	}
	b.log.Info("device interviewed", "device", current.Name, "endpoints", len(descriptors))
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
	return map[string]any{"healthy": alive && b.ready.Load(), "mqtt_ready": b.ready.Load(), "coordinator_connected": alive, "queued_commands": len(b.queue) + int(b.pendingJobs.Load()), "queued_reports": len(b.reports), "pending_retained_cleanup": len(b.store.CleanupTopics()), "queued_protocol_replies": len(b.acks), "errors": b.errors.Load(), "permit_join": remaining > 0, "permit_join_remaining": remaining, "diagnostics": b.diagnostics.snapshot()}
}
