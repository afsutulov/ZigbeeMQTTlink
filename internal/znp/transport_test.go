package znp

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestBlockedTransportWriteIsBounded(t *testing.T) {
	host, dev := net.Pipe()
	defer dev.Close()
	c := New(host)
	defer c.Close()
	finished := make(chan error, 1)
	go func() { _, err := c.Request(context.Background(), SYS, 1, nil); finished <- err }()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("blocked write succeeded")
		}
	case <-time.After(SRSPTimeout + time.Second):
		t.Fatal("blocked transport write outlived its internal deadline")
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("broken stream not closed")
	}
}

func TestAFConfirmationDoesNotMatchReusedID(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "active", true: "timed-out"}[timeout], func(t *testing.T) {
			host, dev := net.Pipe()
			defer dev.Close()
			c := New(host)
			defer c.Close()
			a := &Adapter{C: c, inFlight: make(chan struct{}, maxInFlight)}
			requests := make(chan Frame, 4)
			go func() {
				for {
					f, err := ReadFrame(dev)
					if err != nil {
						return
					}
					reply, _ := (Frame{0x60 | AF, 1, []byte{0}}).Bytes()
					if _, err = dev.Write(reply); err != nil {
						return
					}
					requests <- f
				}
			}()
			sendConfirm := func(id, status byte) {
				t.Helper()
				p, _ := (Frame{0x40 | AF, 0x80, []byte{status, 1, id}}).Bytes()
				if _, err := dev.Write(p); err != nil {
					t.Fatal(err)
				}
			}
			receive := func() Frame {
				t.Helper()
				select {
				case f := <-requests:
					return f
				case <-time.After(time.Second):
					t.Fatal("request not sent")
					return Frame{}
				}
			}
			firstCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			if !timeout {
				firstCtx = context.Background()
			}
			first := make(chan error, 1)
			go func() { first <- a.Send(firstCtx, 1, 1, 6, []byte{0x11, 1, 1}) }()
			id := receive().Data[6]
			if timeout {
				if err := <-first; !errors.Is(err, ErrAREQTimeout) {
					t.Fatalf("want timeout, got %v", err)
				}
			}
			// Force the same situation as wrapping the uint8 counter while an
			// earlier request is still pending or has an unconfirmed late reply.
			a.transMu.Lock()
			a.trans = id - 1
			a.transMu.Unlock()
			secondCtx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			second := make(chan error, 1)
			go func() { second <- a.Send(secondCtx, 2, 1, 6, []byte{0x11, 2, 0}) }()
			newID := receive().Data[6]
			if newID == id {
				t.Fatal("active/quarantined AF ID reused")
			}
			sendConfirm(id, 0xcd)
			select {
			case err := <-second:
				t.Fatalf("unrelated confirmation completed second request: %v", err)
			case <-time.After(30 * time.Millisecond):
			}
			sendConfirm(newID, 0)
			if err := <-second; err != nil {
				t.Fatal(err)
			}
			if !timeout {
				if err := <-first; err == nil {
					t.Fatal("first request lost its own failure status")
				}
			}
		})
	}
}

func TestBindWireAndDeviceRejection(t *testing.T) {
	host, dev := net.Pipe()
	defer dev.Close()
	c := New(host)
	defer c.Close()
	a := &Adapter{C: c, IEEE: "0x0102030405060708"}
	finished := make(chan error, 1)
	go func() { finished <- a.Bind(context.Background(), 0x1234, "0x1112131415161718", 2, 6) }()
	f, err := ReadFrame(dev)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x34, 0x12, 0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, 2, 6, 0, 3, 8, 7, 6, 5, 4, 3, 2, 1, 1}
	if f.Cmd0 != 0x25 || f.Cmd1 != 0x21 || string(f.Data) != string(want) {
		t.Fatalf("wrong bind request %x", f.Data)
	}
	for _, reply := range []Frame{{0x65, 0x21, []byte{0}}, {0x45, 0xa1, []byte{0x34, 0x12, 0x84}}} {
		p, _ := reply.Bytes()
		if _, err := dev.Write(p); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-finished; err == nil {
		t.Fatal("device bind rejection ignored")
	}
}
