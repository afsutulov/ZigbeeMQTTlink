package bridge

import (
	"context"
	"time"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/znp"
)

const recoveryTTL = 30 * time.Second
const maxRecoveryFrames = 256
const maxFramesPerAddress = 8

// After a failed lookup (typically a sleeping end device that missed the
// request) the next frame from that address may start a new lookup soon: a
// device that just transmitted is awake. Buffered frames are kept for it.
const lookupRetryBackoff = 15 * time.Second

// After an applied, superseded or unpaired-IEEE result, do not query again.
const lookupRepeatInterval = 2 * time.Minute

type bufferedFrame struct {
	frame    znp.Frame
	received time.Time
}
type addressLookup struct {
	started    time.Time
	retryAfter time.Time // valid when !pending
	epoch      uint64
	pending    bool
	frames     []bufferedFrame
}
type addressResult struct {
	network uint16
	epoch   uint64
	started time.Time
	ieee    string
	err     error
}

// Bounded, short-lived buffering preserves a single leak/button report while
// resolving its sender. This is not a durable event journal.
func (b *Bridge) requestAddressRecovery(n uint16, f znp.Frame) {
	if n == 0 || n >= 0xfff8 {
		return
	}
	now := time.Now()
	b.pruneRecoveryFrames(now)
	lookup := b.addressLookups[n]
	epoch := b.store.AddressEpoch()
	if lookup != nil && lookup.epoch != epoch {
		// The buffered sender was never identified. After any address/membership
		// change the old frames cannot safely be assigned to a new lookup result.
		b.dropRecoveryFrames(lookup)
		delete(b.addressLookups, n)
		lookup = nil
	}
	if lookup != nil {
		if lookup.pending && now.Sub(lookup.started) <= recoveryTTL {
			b.bufferRecoveryFrame(lookup, f, now)
			return
		}
		if !lookup.pending && now.Before(lookup.retryAfter) {
			// Keep the report for the next lookup attempt; it expires after
			// recoveryTTL if no further frame arrives to trigger one.
			b.bufferRecoveryFrame(lookup, f, now)
			return
		}
	}
	if lookup == nil && len(b.addressLookups) >= 256 {
		for k, old := range b.addressLookups {
			if !old.pending && !now.Before(old.retryAfter) {
				b.dropRecoveryFrames(old)
				delete(b.addressLookups, k)
			}
		}
		if len(b.addressLookups) >= 256 {
			b.diagnostics.recoveryDropped.Add(1)
			return
		}
	}
	select {
	case b.queue <- job{resolve: n, lookupEpoch: epoch, received: now}:
		next := &addressLookup{started: now, epoch: epoch, pending: true}
		if lookup != nil {
			// Carry still-valid reports of an earlier failed/expired attempt.
			// The earlier attempt's result no longer matches and is ignored.
			next.frames = lookup.frames
			lookup.frames = nil
		}
		b.addressLookups[n] = next
		b.bufferRecoveryFrame(next, f, now)
	default:
		b.diagnostics.recoveryDropped.Add(1)
	}
}

// pruneRecoveryFrames releases buffered reports older than recoveryTTL; they
// would be rejected at replay anyway and must not hold the global budget.
func (b *Bridge) pruneRecoveryFrames(now time.Time) {
	if b.bufferedFrames == 0 {
		return
	}
	for _, l := range b.addressLookups {
		kept := l.frames[:0]
		for _, item := range l.frames {
			if now.Sub(item.received) > recoveryTTL {
				b.bufferedFrames--
				b.diagnostics.recoveryDropped.Add(1)
				continue
			}
			kept = append(kept, item)
		}
		for i := len(kept); i < len(l.frames); i++ {
			l.frames[i] = bufferedFrame{}
		}
		l.frames = kept
	}
}
func (b *Bridge) bufferRecoveryFrame(l *addressLookup, f znp.Frame, now time.Time) {
	if len(l.frames) >= maxFramesPerAddress || b.bufferedFrames >= maxRecoveryFrames {
		b.diagnostics.recoveryDropped.Add(1)
		return
	}
	f.Data = append([]byte(nil), f.Data...)
	l.frames = append(l.frames, bufferedFrame{f, now})
	b.bufferedFrames++
}
func (b *Bridge) dropRecoveryFrames(l *addressLookup) {
	b.bufferedFrames -= len(l.frames)
	b.diagnostics.recoveryDropped.Add(uint64(len(l.frames)))
	l.frames = nil
}
func (b *Bridge) handleAddressResult(ctx context.Context, result addressResult) {
	l := b.addressLookups[result.network]
	if l == nil || !l.pending || l.epoch != result.epoch || !l.started.Equal(result.started) {
		return
	}
	l.pending = false
	if result.err != nil || time.Since(l.started) > recoveryTTL {
		// Keep buffered reports: the next frame from this (awake) device
		// starts a new lookup after a short backoff and replays them.
		l.retryAfter = l.started.Add(lookupRetryBackoff)
		b.pruneRecoveryFrames(time.Now())
		b.log.Info("unknown address lookup failed or expired; will retry on the next frame", "address", result.network, "buffered_frames", len(l.frames), "error", result.err)
		return
	}
	l.retryAfter = l.started.Add(lookupRepeatInterval)
	if !b.commitRecoveredAddress(result.ieee, result.network, result.epoch, "IEEE address request", true) {
		b.dropRecoveryFrames(l)
		return
	}
	frames := l.frames
	l.frames = nil
	b.bufferedFrames -= len(frames)
	for _, item := range frames {
		if time.Since(item.received) > recoveryTTL {
			b.diagnostics.recoveryDropped.Add(1)
			continue
		}
		// All conversion maps are owned by this loop, never by a lookup worker.
		b.safeEventForDevice(ctx, item.frame, result.ieee)
		b.diagnostics.recoveryReplayed.Add(1)
	}
}
func (b *Bridge) commitRecoveredAddress(ieee string, n uint16, epoch uint64, source string, unchanged bool) bool {
	old, _ := b.store.ByIEEE(ieee)
	var d store.Device
	var displaced []string
	var err error
	if unchanged {
		d, displaced, err = b.store.RecoverAddressSnapshot(ieee, n, epoch)
	} else {
		d, displaced, err = b.store.RecoverAddress(ieee, n, epoch)
	}
	if err != nil {
		b.log.Warn("network address recovery ignored", "ieee", ieee, "address", n, "source", source, "error", err)
		return false
	}
	if old.Network != n {
		b.diagnostics.addressRecovered.Add(1)
		b.log.Info("network address of paired device updated", "device", d.Name, "old_address", old.Network, "address", n, "source", source)
	}
	for _, name := range displaced {
		b.log.Info("network address taken over; old entry waits for its own announce", "device", name, "address", n)
	}
	return true
}
func (b *Bridge) recoverAddress(ieee string, n uint16, source string) {
	// A trust-center indication is current authoritative information. Invalid or
	// unknown devices are ignored atomically, without a separate read/add race.
	if !b.commitRecoveredAddress(ieee, n, b.store.AddressEpoch(), source, false) {
		return
	}
}
