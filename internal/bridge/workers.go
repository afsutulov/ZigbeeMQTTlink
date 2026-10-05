package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"zigbeemqttlink/internal/store"
)

type publication struct {
	ieee, name string
	payload    json.RawMessage
	retain     bool
}

// Event processing never waits for an MQTT acknowledgement. The queue is
// bounded; overflow is explicit in logs/health rather than unbounded memory.
func (b *Bridge) queueReport(d store.Device, state map[string]any, retain bool) {
	payload, err := json.Marshal(state)
	if err == nil {
		select {
		case b.reports <- publication{d.IEEE, d.Name, payload, retain}:
			return
		default:
			err = fmt.Errorf("MQTT report queue full")
		}
	}
	b.diagnostics.publishErrors.Add(1)
	b.log.Error("state report could not be queued", "device", d.Name, "error", err)
}

func (b *Bridge) publicationWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-b.reports:
			if ctx.Err() != nil {
				return
			}
			// Serialize with retained-topic deletion. A queued report from a
			// removed/renamed device must not restore the obsolete topic.
			b.publishMu.Lock()
			d, exists := b.store.ByIEEE(p.ieee)
			if !exists || d.Name != p.name {
				b.publishMu.Unlock()
				continue
			}
			err := b.publish(p.name, p.payload, p.retain)
			b.publishMu.Unlock()
			if err != nil {
				b.diagnostics.publishErrors.Add(1)
				b.log.Warn("state publish failed", "device", p.name, "error", err)
			} else {
				b.diagnostics.publishedReports.Add(1)
			}
		}
	}
}

type operationLock struct {
	gate  chan struct{}
	users int
}

// Serialize compound operations on one physical device, including HTTP
// requests, without stopping reports or operations on other devices.
func (b *Bridge) lockDevice(ctx context.Context, ieee string) (func(), error) {
	b.operationMu.Lock()
	l := b.operations[ieee]
	if l == nil {
		l = &operationLock{gate: make(chan struct{}, 1)}
		b.operations[ieee] = l
	}
	l.users++
	b.operationMu.Unlock()
	releaseReference := func() {
		b.operationMu.Lock()
		l.users--
		if l.users == 0 {
			delete(b.operations, ieee)
		}
		b.operationMu.Unlock()
	}
	select {
	case l.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-l.gate
			releaseReference()
			return nil, err
		}
		return func() { <-l.gate; releaseReference() }, nil
	case <-ctx.Done():
		releaseReference()
		return nil, ctx.Err()
	}
}

func (b *Bridge) jobKey(j job) string {
	if j.announce != nil {
		return j.announce.IEEE
	}
	if j.configure != nil {
		return j.configure.IEEE
	}
	if j.resolve != 0 {
		return "address-recovery"
	}
	var id string
	if len(j.topic) >= len("bridge/request/") && j.topic[:len("bridge/request/")] == "bridge/request/" {
		var p map[string]any
		if json.Unmarshal(j.payload, &p) == nil {
			id = textField(p, "id")
			if id == "" {
				id = textField(p, "from")
			}
		}
		if d, ok := b.store.Find(id); ok {
			return d.IEEE
		}
		return "administration"
	}
	id, _, p, err := parseCommand(j.topic, j.payload)
	if err == nil {
		if d, _, err := b.resolveCommand(id, p); err == nil {
			return d.IEEE
		}
	}
	return "invalid-command"
}

// Three independent device jobs run at a time, leaving one of the four AF
// slots available for protocol replies. Jobs for the same IEEE remain FIFO.
// The bounded backlog lets other devices bypass a sleeping device's queue.
func (b *Bridge) worker(ctx context.Context) {
	type scheduled struct {
		key string
		job job
	}
	backlog := make([]scheduled, 0, 128)
	active := map[string]bool{}
	completed := make(chan string, 3)
	var wg sync.WaitGroup
	defer func() { wg.Wait(); b.pendingJobs.Store(0) }()
	for {
		if ctx.Err() != nil {
			return
		}
		for len(active) < 3 {
			next := -1
			for i, item := range backlog {
				if !active[item.key] {
					next = i
					break
				}
			}
			if next < 0 {
				break
			}
			item := backlog[next]
			backlog = append(backlog[:next], backlog[next+1:]...)
			active[item.key] = true
			wg.Add(1)
			go func() {
				defer wg.Done()
				b.processJob(ctx, item.job)
				completed <- item.key
			}()
		}
		b.pendingJobs.Store(int64(len(backlog) + len(active)))
		input := b.queue
		if len(backlog) == 128 {
			input = nil
		}
		select {
		case <-ctx.Done():
			return
		case key := <-completed:
			delete(active, key)
		case j := <-input:
			backlog = append(backlog, scheduled{b.jobKey(j), j})
		}
	}
}

func (b *Bridge) processJob(ctx context.Context, j job) {
	if j.announce != nil {
		unlock, err := b.lockDevice(ctx, j.announce.IEEE)
		if err != nil {
			return
		}
		defer unlock()
		b.interview(ctx, *j.announce)
		return
	}
	if j.resolve != 0 {
		result := addressResult{network: j.resolve, epoch: j.lookupEpoch, started: j.received}
		resolver, ok := b.radio.(interface {
			IEEEAddress(context.Context, uint16) (string, error)
		})
		if !ok {
			result.err = fmt.Errorf("coordinator cannot resolve IEEE addresses")
		} else {
			op, cancel := context.WithTimeout(ctx, 10*time.Second)
			if !j.received.IsZero() {
				var c context.CancelFunc
				op, c = context.WithDeadline(op, j.received.Add(30*time.Second))
				defer c()
			}
			result.ieee, result.err = resolver.IEEEAddress(op, j.resolve)
			cancel()
		}
		// Only the event loop changes recovery buffers and applies results.
		select {
		case b.recoveryResults <- result:
		case <-ctx.Done():
		}
		return
	}
	if j.configure != nil {
		op, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if _, err := b.adminRequest(op, "device/configure", map[string]any{"id": j.configure.IEEE}); err != nil {
			b.log.Warn("automatic device configuration failed", "device", j.configure.Name, "error", err)
		}
		return
	}
	if !j.received.IsZero() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, j.received.Add(30*time.Second))
		defer cancel()
		if err := ctx.Err(); err != nil {
			b.commandError(j, nil, fmt.Errorf("queued command expired: %w", err))
			return
		}
	}
	b.command(ctx, j)
}

// Paho completes a SUBACK token even when its return code is 0x80. Check
// granted QoS explicitly so a broker ACL denial cannot look like readiness.
func (b *Bridge) subscribe(c mqtt.Client) error {
	topic := b.cfg.MQTT.BaseTopic + "/#"
	token := c.Subscribe(topic, 1, func(_ mqtt.Client, m mqtt.Message) { b.enqueue(m) })
	if err := b.wait(token); err != nil {
		return err
	}
	if result, ok := token.(*mqtt.SubscribeToken); ok {
		qos, present := result.Result()[topic]
		if !present || qos > 2 {
			return fmt.Errorf("MQTT broker denied subscription to %s; check broker ACL", topic)
		}
	}
	return nil
}

func (b *Bridge) setupMQTT(ctx context.Context, c mqtt.Client, epoch uint64) {
	b.setupMu.Lock()
	defer b.setupMu.Unlock()
	current := func() bool {
		b.connectionMu.Lock()
		defer b.connectionMu.Unlock()
		return b.connectionEpoch == epoch && ctx.Err() == nil
	}
	for current() {
		err := b.subscribe(c)
		if !current() {
			return
		}
		if err == nil {
			err = b.metadata()
		}
		if err == nil {
			b.connectionMu.Lock()
			if b.connectionEpoch == epoch && ctx.Err() == nil && c.IsConnectionOpen() {
				b.ready.Store(true)
			}
			b.connectionMu.Unlock()
			if b.ready.Load() && current() {
				b.log.Info("MQTT subscriptions and metadata ready")
			}
			return
		}
		b.log.Error("MQTT setup failed; retrying", "error", err)
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Persistence can be slow on an SD card. It must not run on the radio event
// loop; Store.Save serializes writers and preserves changes made during I/O.
func (b *Bridge) saveWorker(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := b.store.SaveIfDirty(time.Minute); err != nil {
				b.errors.Add(1)
				b.log.Error("state save failed", "error", err)
			}
		}
	}
}
