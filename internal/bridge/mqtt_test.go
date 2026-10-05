package bridge

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/znp"
)

// A small wire peer exercises the real Paho client, including SUBACK ACL
// denials and reconnects. It is a test fixture, not a production broker.
type mqttPeer struct {
	listener    net.Listener
	mu          sync.Mutex
	connections []net.Conn
	writes      sync.Mutex
	wg          sync.WaitGroup
	connected   chan net.Conn
	subscribed  chan struct{}
	published   chan *packets.PublishPacket
	deny        bool
}

func startPeer(t *testing.T, deny bool) *mqttPeer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &mqttPeer{listener: l, connected: make(chan net.Conn, 8), subscribed: make(chan struct{}, 8), published: make(chan *packets.PublishPacket, 128), deny: deny}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.connections = append(p.connections, c)
			p.mu.Unlock()
			p.wg.Add(1)
			go p.serve(c)
		}
	}()
	return p
}
func (p *mqttPeer) close() {
	p.listener.Close()
	p.mu.Lock()
	for _, c := range p.connections {
		c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}
func (p *mqttPeer) write(c net.Conn, packet packets.ControlPacket) error {
	p.writes.Lock()
	defer p.writes.Unlock()
	return packet.Write(c)
}
func (p *mqttPeer) serve(c net.Conn) {
	defer p.wg.Done()
	defer c.Close()
	for {
		packet, err := packets.ReadPacket(c)
		if err != nil {
			return
		}
		var response packets.ControlPacket
		switch m := packet.(type) {
		case *packets.ConnectPacket:
			response = packets.NewControlPacket(packets.Connack)
			p.connected <- c
		case *packets.SubscribePacket:
			s := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
			s.MessageID = m.MessageID
			for range m.Topics {
				qos := byte(1)
				if p.deny {
					qos = 0x80
				}
				s.ReturnCodes = append(s.ReturnCodes, qos)
			}
			response = s
			p.subscribed <- struct{}{}
		case *packets.PublishPacket:
			p.published <- m
			if m.Qos == 1 {
				ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
				ack.MessageID = m.MessageID
				response = ack
			}
		case *packets.PingreqPacket:
			response = packets.NewControlPacket(packets.Pingresp)
		case *packets.DisconnectPacket:
			return
		}
		if response != nil {
			if err := p.write(c, response); err != nil {
				return
			}
		}
	}
}

type eventRadio struct {
	*delayedRadio
	events chan znp.Frame
}

func (r *eventRadio) Events() <-chan znp.Frame { return r.events }
func waitReady(t *testing.T, b *Bridge) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !b.ready.Load() {
		if time.Now().After(deadline) {
			t.Fatal("MQTT did not become ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
func startWireBridge(t *testing.T, p *mqttPeer) (*Bridge, *eventRadio, func()) {
	t.Helper()
	b := newTestBridge(t, config.Web{})
	putPlug(t, b, 1)
	b.cfg.MQTT.Server = "tcp://" + p.listener.Addr().String()
	b.cfg.MQTT.ClientID = "review-test"
	r := &eventRadio{delayedRadio: &delayedRadio{fakeRadio: fakeRadio{make(chan struct{})}, sent: make(chan sent, 8), release: make(chan struct{})}, events: make(chan znp.Frame, 8)}
	close(r.release)
	b.radio = r
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- b.Run(ctx) }()
	return b, r, func() {
		cancel()
		select {
		case err := <-finished:
			if err != nil {
				t.Errorf("shutdown: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("bridge shutdown stalled")
			p.close()
		}
	}
}

func TestMQTTWireCommandsReportsAndReconnect(t *testing.T) {
	p := startPeer(t, false)
	defer p.close()
	b, r, stop := startWireBridge(t, p)
	defer stop()
	c := await(t, p.connected)
	waitReady(t, b)
	command := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	command.TopicName = "z/p1/set"
	command.Payload = []byte("ON")
	command.Qos = 1
	command.MessageID = 42
	if err := p.write(c, command); err != nil {
		t.Fatal(err)
	}
	if s := await(t, r.sent); s.network != 1 || s.command != 1 {
		t.Fatalf("MQTT command not delivered: %v", s)
	}
	command.Retain = true
	command.Payload = []byte("OFF")
	command.MessageID = 43
	if err := p.write(c, command); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for b.diagnostics.retainedIgnored.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("retained guard not reached")
		}
		time.Sleep(time.Millisecond)
	}
	if len(r.sent) != 0 {
		t.Fatal("retained command executed")
	}
	r.events <- reportFrame(1, 0x66)
	deadline = time.Now().Add(time.Second)
	for {
		var msg *packets.PublishPacket
		select {
		case msg = <-p.published:
		case <-time.After(time.Until(deadline)):
			t.Fatal("report not published")
		}
		if msg.TopicName != "z/p1" {
			continue
		}
		var state map[string]any
		if err := json.Unmarshal(msg.Payload, &state); err != nil || state["temperature"] != 21.5 {
			t.Fatalf("bad MQTT state: %s", msg.Payload)
		}
		break
	}
	c.Close()
	select {
	case <-p.connected:
	case <-time.After(5 * time.Second):
		t.Fatal("MQTT did not reconnect")
	}
	waitReady(t, b)
	if !b.Health()["mqtt_ready"].(bool) {
		t.Fatal("MQTT readiness lost after reconnect")
	}
}

func TestMQTTSubscriptionDenialIsNotHealthy(t *testing.T) {
	p := startPeer(t, true)
	defer p.close()
	b, _, stop := startWireBridge(t, p)
	defer stop()
	await(t, p.subscribed)
	time.Sleep(100 * time.Millisecond)
	if b.ready.Load() || b.Health()["healthy"].(bool) {
		t.Fatal("SUBACK 0x80 reported as a ready service")
	}
}

func TestOfflineRenameClearsOldTopicOnConnect(t *testing.T) {
	p := startPeer(t, false)
	defer p.close()
	b := newTestBridge(t, config.Web{})
	d := putPlug(t, b, 1)
	if _, err := b.store.Rename(d.IEEE, "renamed"); err != nil {
		t.Fatal(err)
	}
	// The bridge was offline during the admin change.
	b.cfg.MQTT.Server = "tcp://" + p.listener.Addr().String()
	b.cfg.MQTT.ClientID = "cleanup-test"
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- b.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-finished:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("shutdown stalled")
		}
	}()
	msg := await(t, p.published)
	if msg.TopicName != "z/p1" || !msg.Retain || len(msg.Payload) != 0 {
		t.Fatalf("obsolete topic not cleared first: %+v", msg)
	}
	waitReady(t, b)
	if len(b.store.CleanupTopics()) != 0 {
		t.Fatal("cleanup remains after broker acknowledgement")
	}
}
