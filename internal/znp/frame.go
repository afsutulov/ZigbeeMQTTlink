package znp

import (
	"errors"
	"fmt"
	"io"
)

type Frame struct {
	Cmd0 byte
	Cmd1 byte
	Data []byte
}

func (f Frame) Bytes() ([]byte, error) {
	if len(f.Data) > 250 {
		return nil, fmt.Errorf("ZNP payload exceeds 250 bytes")
	}
	b := []byte{0xfe, byte(len(f.Data)), f.Cmd0, f.Cmd1}
	b = append(b, f.Data...)
	fcs := byte(0)
	for _, x := range b[1:] {
		fcs ^= x
	}
	return append(b, fcs), nil
}

var ErrFCS = errors.New("ZNP checksum mismatch")

// ErrLength reports an impossible MT length byte (line noise or a lost SOF).
// Like ErrFCS it is recoverable: the reader resynchronises on the next SOF.
var ErrLength = errors.New("invalid ZNP length")

func ReadFrame(r io.Reader) (Frame, error) {
	var one [1]byte
	for {
		if _, e := io.ReadFull(r, one[:]); e != nil {
			return Frame{}, e
		}
		if one[0] == 0xfe {
			break
		}
	}
	var header [3]byte
	// Validate length before consuming command bytes: they may be the next SOF.
	if _, e := io.ReadFull(r, header[:1]); e != nil {
		return Frame{}, e
	}
	// A length byte of 0xFE is never valid (max 250), but it is the SOF of the
	// next frame when a noise byte 0xFE preceded it: restart the frame there.
	for header[0] == 0xfe {
		if _, e := io.ReadFull(r, header[:1]); e != nil {
			return Frame{}, e
		}
	}
	n := int(header[0])
	if n > 250 {
		return Frame{}, fmt.Errorf("%w %d", ErrLength, n)
	}
	if _, e := io.ReadFull(r, header[1:]); e != nil {
		return Frame{}, e
	}
	data := make([]byte, n+1)
	if _, e := io.ReadFull(r, data); e != nil {
		return Frame{}, e
	}
	sum := header[0] ^ header[1] ^ header[2]
	for _, b := range data {
		sum ^= b
	}
	if sum != 0 {
		return Frame{}, ErrFCS
	}
	return Frame{header[1], header[2], data[:n]}, nil
}
