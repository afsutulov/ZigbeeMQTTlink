package znp

import (
	"bytes"
	"errors"
	"testing"
)

func TestInvalidLengthDoesNotConsumeNextSOF(t *testing.T) {
	good, _ := (Frame{0x45, 0xc1, make([]byte, 13)}).Bytes()
	r := bytes.NewReader(append([]byte{0xfe, 0xff}, good...))
	if _, err := ReadFrame(r); !errors.Is(err, ErrLength) {
		t.Fatal(err)
	}
	f, err := ReadFrame(r)
	if err != nil || f.Cmd0 != 0x45 || f.Cmd1 != 0xc1 {
		t.Fatalf("valid next frame lost: %+v %v", f, err)
	}
}

// A stray 0xFE directly before a real SOF must not consume that SOF.
func TestNoiseSOFBeforeRealSOFKeepsFrame(t *testing.T) {
	good, _ := (Frame{0x45, 0xc1, make([]byte, 13)}).Bytes()
	r := &sliceReader{b: append([]byte{0xfe}, good...)}
	f, err := ReadFrame(r)
	if err != nil || f.Cmd0 != 0x45 || f.Cmd1 != 0xc1 || len(f.Data) != 13 {
		t.Fatalf("frame=%+v err=%v", f, err)
	}
	r = &sliceReader{b: append([]byte{0xfe, 0xfe, 0xfe}, good...)}
	if f, err = ReadFrame(r); err != nil || f.Cmd1 != 0xc1 {
		t.Fatalf("repeated noise SOF: frame=%+v err=%v", f, err)
	}
}
