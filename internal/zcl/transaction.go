package zcl

import "encoding/binary"

// WireTransaction uses the full 16-bit Tuya sequence. Reusing a constant
// transaction can make a Tuya MCU discard otherwise valid commands.
func WireTransaction(c Command, seq uint32) []byte {
	if c.Cluster == 0xef00 && c.Control&7 == 1 && c.ID == 0 && len(c.Payload) >= 6 {
		c.Payload = append([]byte(nil), c.Payload...)
		binary.LittleEndian.PutUint16(c.Payload, uint16(seq))
	}
	return Wire(c, byte(seq))
}
