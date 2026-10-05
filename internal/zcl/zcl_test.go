package zcl

import (
	"testing"
	"time"
)

func TestReportAttributes(t *testing.T) {
	// temperature 21.50 (int16 0x0866), humidity 45.00 (uint16)
	f, err := Parse([]byte{0x18, 1, 0x0a, 0, 0, 0x29, 0x66, 0x08})
	if err != nil {
		t.Fatal(err)
	}
	out, err := State(0x402, f)
	if err != nil || out["temperature"] != 21.5 {
		t.Fatalf("temperature %v %v", out, err)
	}
}

func TestTuyaDatapoints(t *testing.T) {
	dps, err := DecodeDatapoints([]byte{0, 1, 1, 2, 0, 4, 0, 0, 0, 0xeb, 2, 1, 0, 1, 1})
	if err != nil || len(dps) != 2 || dps[0].Value != uint64(235) || dps[1].Value != true {
		t.Fatalf("%+v %v", dps, err)
	}
}

func TestTimeResponse(t *testing.T) {
	f := Frame{Control: 0x00, Seq: 7, Command: 0, Payload: []byte{0, 0, 7, 0}}
	b, err := TimeResponse(f, time.Unix(946684800+100, 0).UTC())
	if err != nil || b[0] != 0x18 || b[1] != 7 || b[2] != 1 || b[6] != 0xe2 || b[7] != 100 {
		t.Fatalf("%x %v", b, err)
	}
}

func TestConfigureReportingResponse(t *testing.T) {
	ok, err := CommandResponse(Frame{Control: 0x18, Command: 7, Payload: []byte{0}})
	if !ok || err != nil {
		t.Fatal("success not recognised")
	}
	ok, err = CommandResponse(Frame{Control: 0x18, Command: 7, Payload: []byte{0x86, 0, 0, 0}})
	if !ok || err == nil {
		t.Fatal("failure not reported")
	}
}

func TestTuyaSequenceIncrements(t *testing.T) {
	c := Command{Cluster: 0xef00, Control: 0x11, ID: 0, Payload: []byte{0, 0, 1, 1, 0, 1, 1}}
	a, b := WireTransaction(c, 1), WireTransaction(c, 2)
	if a[3] == b[3] {
		t.Fatal("Tuya sequence not updated")
	}
}
