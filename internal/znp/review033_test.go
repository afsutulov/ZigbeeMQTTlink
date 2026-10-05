package znp

import (
	"strings"
	"testing"
)

func TestReviewDeviceCounterRollbackCannotCompleteRestore(t *testing.T) {
	for _, rx := range []bool{false, true} {
		b, _ := ParseBackup([]byte(herdsmanBackup))
		g, _ := ParseBackup([]byte(herdsmanBackup))
		g.FrameCounter += frameCounterRestoreMargin
		for _, d := range g.Devices {
			if d.LinkKey != nil {
				d.LinkKey.TxCounter += frameCounterRestoreMargin
			}
		}
		if rx {
			g.Devices[6].LinkKey.RxCounter = 0
		} else {
			g.Devices[6].LinkKey.TxCounter = 0
		}
		if err := b.VerifyRestored(g); err == nil {
			t.Fatalf("ready allowed with reused key and rolled-back counter (rx=%v)", rx)
		}
	}
}

func TestReviewStrictRestoreUnreadableCurrent(t *testing.T) {
	fastTimings(t)
	e, c := newEmulator(t, ProductZStack3x0, 1)
	ctx := testCtx(t)
	if _, err := FormNetwork(ctx, c, fixedNetwork(), quietLog()); err != nil {
		t.Fatal(err)
	}
	e.pair(t, testSeed)
	// Valid NIB, but an APS key entry refers beyond the table.
	e.mu.Lock()
	rows, _ := decodeSecurityTable(e.nv[nvApsLinkKeyTable], e.aligned)
	rows[0].setU16("keyNvId", 999)
	e.nv[nvApsLinkKeyTable] = encodeSecurityTable(rows, e.aligned)
	before := e.writes
	e.mu.Unlock()
	b, _ := ParseBackup([]byte(herdsmanBackup))
	err := RestoreNetwork(ctx, c, b, quietLog())
	if err == nil || !strings.Contains(err.Error(), "index") {
		t.Fatalf("expected corrupt current-backup rejection: %v", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.writes != before {
		t.Fatal("strict restore altered NV")
	}
}

func TestReviewForcedRestoreRepairsUnreadableCurrent(t *testing.T) {
	fastTimings(t)
	for _, product := range []byte{ProductZStack3x0, ProductZStack30x} {
		e, c := newEmulator(t, product, 1)
		ctx := testCtx(t)
		if _, err := FormNetwork(ctx, c, fixedNetwork(), quietLog()); err != nil {
			t.Fatal(err)
		}
		e.pair(t, testSeed)
		e.mu.Lock()
		rows, _ := decodeSecurityTable(e.nv[nvApsLinkKeyTable], e.aligned)
		rows[0].setU16("keyNvId", 999)
		e.nv[nvApsLinkKeyTable] = encodeSecurityTable(rows, e.aligned)
		e.mu.Unlock()
		b, _ := ParseBackup([]byte(herdsmanBackup))
		if err := RestoreNetworkWithOptions(ctx, c, b, quietLog(), RestoreOptions{AllowUnreadableCurrent: true}); err != nil {
			t.Fatal(err)
		}
		actual, err := CreateBackup(ctx, c, "test")
		if err != nil {
			t.Fatal(err)
		}
		if err = b.VerifyRestored(actual); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReviewUnsafeKeyCounterLeavesReadyCleared(t *testing.T) {
	fastTimings(t)
	for _, product := range []byte{ProductZStack3x0, ProductZStack30x} {
		e, c := newEmulator(t, product, 1)
		ctx := testCtx(t)
		e.mu.Lock()
		e.rollbackRestoredKey = true
		e.mu.Unlock()
		b, _ := ParseBackup([]byte(herdsmanBackup))
		if err := RestoreNetwork(ctx, c, b, quietLog()); err == nil || !strings.Contains(err.Error(), "unsafe") {
			t.Fatalf("unexpected result: %v", err)
		}
		st, err := Inspect(ctx, c)
		if err != nil || st.Ready {
			t.Fatalf("unsafe key counters marked ready: %+v %v", st, err)
		}
	}
}
