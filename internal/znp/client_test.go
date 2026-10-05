package znp

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// fakeCoordinator answers every SREQ with a success SRSP and never sends AREQs.
func fakeCoordinator(t *testing.T, conn net.Conn) {
	t.Helper()
	go func() {
		for {
			f, err := ReadFrame(conn)
			if err != nil {
				return
			}
			resp, _ := (Frame{0x60 | (f.Cmd0 & 0x1f), f.Cmd1, []byte{0}}).Bytes()
			if _, err := conn.Write(resp); err != nil {
				return
			}
		}
	}()
}

func TestAREQTimeoutKeepsLinkOpen(t *testing.T) {
	host, dev := net.Pipe()
	fakeCoordinator(t, dev)
	c := New(host)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := c.Exchange(ctx, AF, 1, []byte{1}, AF, 0x80, nil)
	if !errors.Is(err, ErrAREQTimeout) {
		t.Fatalf("want AREQ timeout, got %v", err)
	}
	select {
	case <-c.Done():
		t.Fatalf("link closed after a missing device response: %v", c.Err())
	default:
	}
	if _, err := c.Request(context.Background(), SYS, 1, nil); err != nil {
		t.Fatalf("link unusable after AREQ timeout: %v", err)
	}
}

func TestCancelledCallerDoesNotCloseLink(t *testing.T) {
	host, dev := net.Pipe()
	fakeCoordinator(t, dev)
	c := New(host)
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Request(ctx, SYS, 1, nil); err == nil {
		t.Fatal("cancelled request must fail")
	}
	select {
	case <-c.Done():
		t.Fatalf("cancelled caller closed the link: %v", c.Err())
	default:
	}
}

func TestEventOverflowDropsInsteadOfClosing(t *testing.T) {
	host, dev := net.Pipe()
	c := New(host)
	defer c.Close()
	ev, _ := (Frame{0x45, 0xc4, []byte{1, 2, 3}}).Bytes()
	go func() {
		for i := 0; i < cap(c.events)+50; i++ {
			if _, err := dev.Write(ev); err != nil {
				return
			}
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for c.Dropped() < 50 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if c.Dropped() < 50 {
		t.Fatalf("expected dropped events, got %d", c.Dropped())
	}
	select {
	case <-c.Done():
		t.Fatalf("overflow closed the link: %v", c.Err())
	default:
	}
}

func TestFrameRoundTrip(t *testing.T) {
	b, err := (Frame{0x24, 0x01, []byte{1, 2, 3}}).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	f, err := ReadFrame(&sliceReader{b: append([]byte{0x00, 0x11}, b...)})
	if err != nil || f.Cmd0 != 0x24 || f.Cmd1 != 1 || len(f.Data) != 3 {
		t.Fatalf("round trip failed: %+v %v", f, err)
	}
	b[len(b)-1] ^= 0xff
	if _, err := ReadFrame(&sliceReader{b: b}); !errors.Is(err, ErrFCS) {
		t.Fatalf("checksum error expected, got %v", err)
	}
}

func TestParseIncoming(t *testing.T) {
	data := []byte{0, 0, 0x06, 0x00, 0x34, 0x12, 1, 1, 0, 200, 0, 0, 0, 0, 0, 7, 3, 0x18, 9, 0x0a}
	in, err := ParseIncoming(data)
	if err != nil || in.Cluster != 6 || in.Network != 0x1234 || in.LQI != 200 || len(in.ZCL) != 3 {
		t.Fatalf("bad parse %+v %v", in, err)
	}
	in, err = ParseIncoming(append(data, 0x34, 0x12, 5))
	if err != nil || in.MACSource == nil || *in.Radius != 5 {
		t.Fatalf("trailer not parsed %+v %v", in, err)
	}
}

func TestParseDescriptor(t *testing.T) {
	b := []byte{0x34, 0x12, 0, 0x34, 0x12, 14, 1, 0x04, 0x01, 0x00, 0x01, 1, 2, 0x00, 0x00, 0x06, 0x00, 1, 0x19, 0x00}
	ep, err := ParseDescriptor(b)
	if err != nil || ep.ID != 1 || ep.Profile != 0x104 || len(ep.In) != 2 || ep.In[1] != 6 || len(ep.Out) != 1 {
		t.Fatalf("bad descriptor %+v %v", ep, err)
	}
}

type sliceReader struct{ b []byte }

func (r *sliceReader) Read(p []byte) (int, error) {
	if len(r.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.b)
	r.b = r.b[n:]
	return n, nil
}
