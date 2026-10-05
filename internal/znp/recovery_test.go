package znp

import (
	"context"
	"net"
	"testing"
	"time"
)

// Line noise with an impossible length byte must not close the coordinator link.
func TestBadLengthResynchronises(t *testing.T) {
	host, dev := net.Pipe()
	defer dev.Close()
	c := New(host)
	defer c.Close()
	go func() {
		good, _ := (Frame{0x45, 0xc1, make([]byte, 13)}).Bytes()
		_, _ = dev.Write(append([]byte{0xfe, 0xff, 0x00, 0x00}, good...))
	}()
	select {
	case f := <-c.Events():
		if f.Cmd0 != 0x45 || f.Cmd1 != 0xc1 {
			t.Fatalf("unexpected frame %+v", f)
		}
	case <-c.Done():
		t.Fatalf("link closed on noise: %v", c.Err())
	case <-time.After(time.Second):
		t.Fatal("frame after noise not delivered")
	}
	if c.FramingErrors() != 1 {
		t.Fatalf("framing errors = %d", c.FramingErrors())
	}
}

func TestIEEEAddressWire(t *testing.T) {
	host, dev := net.Pipe()
	defer dev.Close()
	c := New(host)
	defer c.Close()
	a := &Adapter{C: c, inFlight: make(chan struct{}, maxInFlight)}
	go func() {
		f, err := ReadFrame(dev)
		if err != nil || f.Cmd0 != 0x25 || f.Cmd1 != 0x01 || len(f.Data) != 4 || f.Data[0] != 0x34 || f.Data[1] != 0x12 {
			return
		}
		srsp, _ := (Frame{0x65, 0x01, []byte{0}}).Bytes()
		// A response for another address must be ignored.
		other, _ := (Frame{0x45, 0x81, []byte{0, 8, 7, 6, 5, 4, 3, 2, 1, 0x99, 0x99, 0, 0}}).Bytes()
		areq, _ := (Frame{0x45, 0x81, []byte{0, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11, 0x34, 0x12, 0, 0}}).Bytes()
		_, _ = dev.Write(append(append(srsp, other...), areq...))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	id, err := a.IEEEAddress(ctx, 0x1234)
	if err != nil || id != "0x1122334455667788" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}
