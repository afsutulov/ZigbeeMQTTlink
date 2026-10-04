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
	if _, e := io.ReadFull(r, header[:]); e != nil {
		return Frame{}, e
	}
	n := int(header[0])
	if n > 250 {
		return Frame{}, fmt.Errorf("invalid ZNP length %d", n)
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
