package zcl

import "fmt"

// Write/default responses acknowledge a request; they are not state reports.
func CommandResponse(f Frame) (bool, error) {
	if f.Control&3 != 0 || f.Control&8 == 0 {
		return false, nil
	}
	if f.Command == 0x0b {
		if len(f.Payload) != 2 {
			return true, fmt.Errorf("malformed ZCL default response")
		}
		if f.Payload[1] != 0 {
			return true, fmt.Errorf("ZCL command 0x%02x failed, status 0x%02x", f.Payload[0], f.Payload[1])
		}
		return true, nil
	}
	if f.Command != 4 {
		return false, nil
	}
	if len(f.Payload) == 1 && f.Payload[0] == 0 {
		return true, nil
	}
	if len(f.Payload) == 0 || len(f.Payload)%3 != 0 {
		return true, fmt.Errorf("malformed ZCL write response")
	}
	for p := f.Payload; len(p) > 0; p = p[3:] {
		if p[0] == 0 {
			return true, fmt.Errorf("malformed ZCL write success record")
		}
		return true, fmt.Errorf("ZCL attribute 0x%04x write failed, status 0x%02x", le.Uint16(p[1:3]), p[0])
	}
	return true, nil
}
