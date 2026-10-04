package zcl

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"unicode/utf8"
)

// ScalarType is the supported data-description subset. Collections remain
// handled by the bounded reader; arbitrary scripts are not evaluated.
func ScalarType(t byte) bool {
	return t == 0x10 || t >= 0x08 && t <= 0x0f || t >= 0x18 && t <= 0x31 || t == 0x39 || t == 0x3a || t == 0x41 || t == 0x42
}
func EncodeScalar(t byte, v any) ([]byte, error) {
	if t == 0x41 || t == 0x42 {
		s, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("string value required")
		}
		data := []byte(s)
		if t == 0x41 {
			var err error
			data, err = hex.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("OCTET value must be hex")
			}
		} else if !utf8.Valid(data) {
			return nil, fmt.Errorf("invalid UTF-8")
		}
		if len(data) > 240 {
			return nil, fmt.Errorf("string too long")
		}
		return append([]byte{byte(len(data))}, data...), nil
	}
	n, ok := numeric(v)
	if b, yes := v.(bool); yes {
		ok = true
		n = 0
		if b {
			n = 1
		}
	}
	if !ok || math.IsNaN(n) || math.IsInf(n, 0) {
		return nil, fmt.Errorf("finite scalar required")
	}
	if t == 0x10 {
		if n != 0 && n != 1 {
			return nil, fmt.Errorf("boolean must be 0/1")
		}
		return []byte{byte(n)}, nil
	}
	if t == 0x39 {
		v := float32(n)
		if math.IsInf(float64(v), 0) {
			return nil, fmt.Errorf("float32 overflow")
		}
		p := make([]byte, 4)
		binary.LittleEndian.PutUint32(p, math.Float32bits(v))
		return p, nil
	}
	if t == 0x3a {
		p := make([]byte, 8)
		binary.LittleEndian.PutUint64(p, math.Float64bits(n))
		return p, nil
	}
	width := 0
	signed := false
	switch {
	case t >= 8 && t <= 15:
		width = int(t-8) + 1
	case t >= 0x18 && t <= 0x1f:
		width = int(t-0x18) + 1
	case t >= 0x20 && t <= 0x27:
		width = int(t-0x20) + 1
	case t >= 0x28 && t <= 0x2f:
		width = int(t-0x28) + 1
		signed = true
	case t == 0x30:
		width = 1
	case t == 0x31:
		width = 2
	default:
		return nil, fmt.Errorf("unsupported scalar type")
	}
	if math.Trunc(n) != n || math.Abs(n) > 9007199254740991 {
		return nil, fmt.Errorf("integer required within exact JSON number range")
	}
	if signed {
		max := math.Pow(2, float64(width*8-1))
		if n < -max || n >= max {
			return nil, fmt.Errorf("signed scalar out of range")
		}
	} else if n < 0 || n >= math.Pow(2, float64(width*8)) {
		return nil, fmt.Errorf("unsigned scalar out of range")
	}
	v64 := uint64(n)
	if signed {
		v64 = uint64(int64(n))
	}
	p := make([]byte, width)
	for i := range p {
		p[i] = byte(v64 >> uint(i*8))
	}
	return p, nil
}
func DecodeScalar(t byte, p []byte) (any, int, error) { return value(t, p) }
func DecodeDatapoints(p []byte) ([]Datapoint, error)  { return datapoints(p) }
