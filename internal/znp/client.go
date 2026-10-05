package znp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// SRSPTimeout bounds the synchronous reply to an SREQ. Z-Stack answers an
// SREQ within milliseconds; a missing SRSP means the serial stream is out of
// sync, so only this case closes the connection.
const SRSPTimeout = 6 * time.Second

// ErrAREQTimeout reports a missing asynchronous answer (for example from a
// sleeping or unreachable device). It never closes the coordinator link.
var ErrAREQTimeout = errors.New("no asynchronous response from the network")

type waiter struct {
	cmd0, cmd1 byte
	match      func([]byte) bool
	ch         chan Frame
}
type Client struct {
	rw       io.ReadWriteCloser
	gate     chan struct{}
	writeMu  sync.Mutex
	mu       sync.Mutex
	pending  *waiter
	watchers map[*waiter]bool
	events   chan Frame
	done     chan struct{}
	closed   chan struct{}
	once     sync.Once
	err      error
	dropped  atomic.Uint64

	framingErrors atomic.Uint64
}

func New(rw io.ReadWriteCloser) *Client {
	c := &Client{rw: rw, gate: make(chan struct{}, 1), watchers: map[*waiter]bool{}, events: make(chan Frame, 1024), done: make(chan struct{}), closed: make(chan struct{})}
	go c.readLoop()
	return c
}
func (c *Client) Events() <-chan Frame  { return c.events }
func (c *Client) Done() <-chan struct{} { return c.done }
func (c *Client) Err() error            { c.mu.Lock(); defer c.mu.Unlock(); return c.err }

// Dropped counts asynchronous events discarded because the consumer was too slow.
func (c *Client) Dropped() uint64 { return c.dropped.Load() }

// FramingErrors counts discarded frames with a bad checksum or length.
func (c *Client) FramingErrors() uint64 { return c.framingErrors.Load() }
func (c *Client) fail(e error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = e
		c.mu.Unlock()
		close(c.done)
		// A broken device driver may block even in Close. Mark the stream
		// failed immediately, without holding sync.Once or the serial gate.
		go func() { defer close(c.closed); _ = c.rw.Close() }()
	})
}
func (c *Client) Close() error {
	c.fail(io.EOF)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-c.closed:
		return nil
	case <-timer.C:
		return fmt.Errorf("transport close timeout")
	}
}
func (c *Client) readLoop() {
	for {
		f, e := ReadFrame(c.rw)
		if errors.Is(e, ErrFCS) || errors.Is(e, ErrLength) {
			// Noise must not restart the whole service. A lost SRSP is still
			// detected by SRSPTimeout; a lost AREQ only fails its request.
			c.framingErrors.Add(1)
			continue
		}
		if e != nil {
			c.fail(e)
			return
		}
		c.mu.Lock()
		delivered := false
		if f.Cmd0&0xe0 == 0x60 {
			w := c.pending
			if w != nil && w.cmd0 == f.Cmd0 && w.cmd1 == f.Cmd1 {
				w.ch <- f
				c.pending = nil
				delivered = true
			}
		}
		for w := range c.watchers {
			if w.cmd0 == f.Cmd0 && w.cmd1 == f.Cmd1 && (w.match == nil || w.match(f.Data)) {
				select {
				case w.ch <- f:
				default:
				}
				delivered = true
			}
		}
		c.mu.Unlock()
		if f.Cmd0&0xe0 == 0x60 && !delivered {
			c.fail(fmt.Errorf("unexpected SRSP %02x/%02x; connection closed to prevent miscorrelation", f.Cmd0, f.Cmd1))
			return
		}
		if !delivered {
			select {
			case c.events <- f:
			case <-c.done:
				return
			default:
				// A slow consumer must not take the whole coordinator link
				// down: dropping one report is far less harmful.
				c.dropped.Add(1)
			}
		}
	}
}

// Request sends one SREQ and waits for its SRSP. The caller's context only
// limits the wait for the serial gate: once bytes are on the wire the reply is
// always awaited (bounded by SRSPTimeout), so cancelling an HTTP or MQTT
// request can no longer desynchronise or close the coordinator link.
func (c *Client) Request(ctx context.Context, sub, cmd byte, data []byte) (Frame, error) {
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case <-c.done:
		return Frame{}, c.Err()
	}
	defer func() { <-c.gate }()
	select {
	case <-c.done:
		return Frame{}, c.Err()
	default:
	}
	if e := ctx.Err(); e != nil {
		return Frame{}, e
	}
	b, e := (Frame{0x20 | sub, cmd, data}).Bytes()
	if e != nil {
		return Frame{}, e
	}
	w := &waiter{cmd0: 0x60 | sub, cmd1: cmd, ch: make(chan Frame, 1)}
	c.mu.Lock()
	c.pending = w
	c.mu.Unlock()
	// Bound the caller even if the transport driver does not interrupt Write.
	// Caller cancellation still does not interrupt an SREQ already sent.
	deadline := time.AfterFunc(SRSPTimeout, func() {
		c.fail(fmt.Errorf("ZNP transport write/SRSP timeout; connection closed"))
	})
	defer deadline.Stop()
	written := make(chan error, 1)
	go func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		for len(b) > 0 {
			n, err := c.rw.Write(b)
			if err != nil {
				written <- err
				return
			}
			if n == 0 {
				written <- io.ErrShortWrite
				return
			}
			b = b[n:]
		}
		written <- nil
	}()
	select {
	case e = <-written:
	case <-c.done:
		return Frame{}, c.Err()
	}
	if e != nil {
		c.fail(e)
		return Frame{}, e
	}
	select {
	case f := <-w.ch:
		return f, nil
	case <-c.done:
		return Frame{}, c.Err()
	}
}

// Exchange arms the AREQ waiter before sending its SREQ, covering an early event.
// A missing AREQ is a per-request failure, not a link failure.
func (c *Client) Exchange(ctx context.Context, sub, cmd byte, data []byte, eventSub, eventCmd byte, match func([]byte) bool) (Frame, error) {
	w := &waiter{cmd0: 0x40 | eventSub, cmd1: eventCmd, match: match, ch: make(chan Frame, 1)}
	c.mu.Lock()
	c.watchers[w] = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.watchers, w); c.mu.Unlock() }()
	f, e := c.Request(ctx, sub, cmd, data)
	if e != nil {
		return Frame{}, e
	}
	if e = Status(f); e != nil {
		return Frame{}, e
	}
	select {
	case f = <-w.ch:
		return f, nil
	case <-ctx.Done():
		return Frame{}, fmt.Errorf("%w: %v", ErrAREQTimeout, ctx.Err())
	case <-c.done:
		return Frame{}, c.Err()
	}
}
func Status(f Frame) error {
	if len(f.Data) < 1 {
		return fmt.Errorf("empty ZNP status")
	}
	if f.Data[0] != 0 {
		return fmt.Errorf("ZNP status 0x%02x", f.Data[0])
	}
	return nil
}

// Watch registers an AREQ waiter before the caller sends the request that
// triggers it. The returned stop function must be called.
func (c *Client) Watch(sub, cmd byte, match func([]byte) bool) (<-chan Frame, func()) {
	w := &waiter{cmd0: 0x40 | sub, cmd1: cmd, match: match, ch: make(chan Frame, 1)}
	c.mu.Lock()
	c.watchers[w] = true
	c.mu.Unlock()
	return w.ch, func() { c.mu.Lock(); delete(c.watchers, w); c.mu.Unlock() }
}

// Notify sends an AREQ (no synchronous reply), serialised with SREQs.
func (c *Client) Notify(ctx context.Context, sub, cmd byte, data []byte) error {
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return c.Err()
	}
	defer func() { <-c.gate }()
	b, e := (Frame{0x40 | sub, cmd, data}).Bytes()
	if e != nil {
		return e
	}
	written := make(chan error, 1)
	go func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		for len(b) > 0 {
			n, err := c.rw.Write(b)
			if err != nil {
				written <- err
				return
			}
			if n == 0 {
				written <- io.ErrShortWrite
				return
			}
			b = b[n:]
		}
		written <- nil
	}()
	timer := time.NewTimer(SRSPTimeout)
	defer timer.Stop()
	select {
	case e = <-written:
		if e != nil {
			c.fail(e)
		}
		return e
	case <-timer.C:
		c.fail(fmt.Errorf("ZNP transport write timeout; connection closed"))
		return c.Err()
	case <-c.done:
		return c.Err()
	}
}

// Await waits for a frame from Watch.
func (c *Client) Await(ctx context.Context, ch <-chan Frame) (Frame, error) {
	select {
	case f := <-ch:
		return f, nil
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case <-c.done:
		return Frame{}, c.Err()
	}
}
