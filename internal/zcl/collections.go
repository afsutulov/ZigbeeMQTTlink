package zcl

import "fmt"

// Collections must consume exactly their wire value so subsequent attributes
// remain readable, even when the collection itself has no state converter.
func collection(t byte, p []byte, depth int) (any, int, error) {
	n, typ := 0, byte(0)
	if t != 0x4c {
		if len(p) < 1 {
			return nil, 0, fmt.Errorf("short collection type")
		}
		typ, n = p[0], 1
	}
	if len(p) < n+2 {
		return nil, 0, fmt.Errorf("short collection count")
	}
	count := int(le.Uint16(p[n : n+2]))
	n += 2
	if count == 0xffff {
		return nil, n, nil
	}
	if count > 256 {
		return nil, 0, fmt.Errorf("ZCL collection count exceeds 256")
	}
	values := make([]any, 0, count)
	for i := 0; i < count; i++ {
		if t == 0x4c {
			if len(p) <= n {
				return nil, 0, fmt.Errorf("short structure element type")
			}
			typ = p[n]
			n++
		}
		v, size, err := valueDepth(typ, p[n:], depth+1)
		if err != nil {
			return nil, 0, err
		}
		n += size
		values = append(values, v)
	}
	return values, n, nil
}

// Xiaomi FF01/CHAR_STR is a binary Mi structure, not UTF-8. Its first byte
// is an element count; legacy firmware can overstate it, ending at frame EOF.
func miStruct(p []byte) (any, int, error) {
	if len(p) == 0 {
		return nil, 0, fmt.Errorf("short Xiaomi structure")
	}
	if p[0] == 0xff {
		return nil, 1, nil
	}
	count, n := int(p[0]), 1
	values := []Attribute{}
	for i := 0; i < count; i++ {
		if n == len(p) {
			if i == 0 {
				return nil, 0, fmt.Errorf("empty truncated Xiaomi structure")
			}
			break
		}
		if i > 0 && len(p)-n == 1 {
			n++
			break
		} // documented Xiaomi trailing-byte quirk
		if len(p)-n < 2 {
			return nil, 0, fmt.Errorf("short Xiaomi element")
		}
		id, typ := p[n], p[n+1]
		n += 2
		v, size, err := value(typ, p[n:])
		if err != nil {
			return nil, 0, fmt.Errorf("Xiaomi element %d: %w", id, err)
		}
		n += size
		values = append(values, Attribute{uint16(id), typ, v})
	}
	return values, n, nil
}
