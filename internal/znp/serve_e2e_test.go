package znp

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

// Manual end-to-end harness: EMU_ADDR=127.0.0.1:7000 go test -run TestServeEmulator
func TestServeEmulator(t *testing.T) {
	addr := os.Getenv("EMU_ADDR")
	if addr == "" {
		t.Skip("manual harness")
	}
	nibSettleInterval, restoreSettleDelay = 10*time.Millisecond, 10*time.Millisecond
	product := ProductZStack3x0
	if os.Getenv("EMU_LEGACY") != "" {
		product = ProductZStack30x
	}
	e := &emulator{t: t, product: product, aligned: product == ProductZStack3x0, nv: map[uint16][]byte{}, ex: map[exID][]byte{}}
	e.factory = []byte{0x44, 0x33, 0x22, 0x11, 0x00, 0x4b, 0x12, 0x00}
	e.factoryDefaults()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	connections := 0
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		l.(*net.TCPListener).SetDeadline(deadline)
		conn, err := l.Accept()
		if err != nil {
			return
		}
		e.conn = conn
		e.endpoints = nil
		connections++
		if fmt.Sprint(connections) == os.Getenv("EMU_CLEAR_READY_ON") {
			e.nv[nvHasConfiguredZStack3] = []byte{0} // simulate an interrupted operation
		}
		if os.Getenv("EMU_PAIR") != "" && e.state == devStateCoordinator {
			e.pair(t, testSeed)
		}
		if fmt.Sprint(connections) == os.Getenv("EMU_CORRUPT_KEYS_ON") && e.state == devStateCoordinator {
			rows, _ := decodeSecurityTable(e.nv[nvApsLinkKeyTable], e.aligned)
			rows[0].setU16("keyNvId", 999)
			e.nv[nvApsLinkKeyTable] = encodeSecurityTable(rows, e.aligned)
		}
		e.serve()
		conn.Close()
	}
}
