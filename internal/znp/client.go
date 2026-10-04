package znp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
)

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
	once     sync.Once
	err      error
}

func New(rw io.ReadWriteCloser) *Client {
	c := &Client{rw: rw, gate: make(chan struct{}, 1), watchers: map[*waiter]bool{}, events: make(chan Frame, 256), done: make(chan struct{})}
	go c.readLoop()
	return c
}
func (c *Client) Events() <-chan Frame  { return c.events }
func (c *Client) Done() <-chan struct{} { return c.done }
func (c *Client) Err() error            { c.mu.Lock(); defer c.mu.Unlock(); return c.err }
func (c *Client) fail(e error) {
	c.once.Do(func() { c.mu.Lock(); c.err = e; c.mu.Unlock(); close(c.done); c.rw.Close() })
}
func (c *Client) Close() error { c.fail(io.EOF); return nil }
func (c *Client) readLoop() {
	for {
		f, e := ReadFrame(c.rw)
		if errors.Is(e, ErrFCS) {
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
				c.fail(fmt.Errorf("ZNP event queue overflow"))
				return
			}
		}
	}
}
func (c *Client) Request(ctx context.Context, sub, cmd byte, data []byte) (Frame, error) {
	select {
	case c.gate <- struct{}{}:
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case <-c.done:
		return Frame{}, c.Err()
	}
	defer func() { <-c.gate }()
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
	stopWrite := context.AfterFunc(ctx, func() { c.fail(fmt.Errorf("request cancelled: %w", ctx.Err())) })
	defer stopWrite()
	c.writeMu.Lock()
	for len(b) > 0 {
		n, err := c.rw.Write(b)
		if err != nil {
			e = err
			break
		}
		if n == 0 {
			e = io.ErrShortWrite
			break
		}
		b = b[n:]
	}
	c.writeMu.Unlock()
	if e != nil {
		c.fail(e)
		return Frame{}, e
	}
	select {
	case f := <-w.ch:
		return f, nil
	case <-ctx.Done():
		c.fail(fmt.Errorf("SRSP timeout: %w", ctx.Err()))
		return Frame{}, ctx.Err()
	case <-c.done:
		return Frame{}, c.Err()
	}
}

// Exchange arms the AREQ waiter before sending its SREQ, covering an early event.
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
		c.fail(fmt.Errorf("AREQ timeout: %w", ctx.Err()))
		return Frame{}, ctx.Err()
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
