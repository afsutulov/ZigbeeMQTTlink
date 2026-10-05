package znp

import (
	"encoding/hex"
	"testing"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Fixtures from zigbee-herdsman test/adapter/z-stack/structs.test.ts.
const (
	herdsmanNIBAligned   = "fb050279147900640000000105018f000700020d1e000000150000000000000000000000ffff0800000020000f0f0400010000000100000000779fd609004b1200010000000000000000000000000000000000000000000000000000000000000000000000003c0c0001780a0100000006020000"
	herdsmanNIBUnaligned = "fb050279147900640000000105018f0700020d1e00001500000000000000000000ffff08000020000f0f0400010000000100000000779fd609004b1200010000000000000000000000000000000000000000000000000000000000000000000000003c0c0001780a010000060200"
	// commissioned3x0AlignedRequestMock NIB (PAN 0x007b, channel 21).
	herdsmanNIBCommissioned = "fb050279147900640000000105018f000700020d1e0000001500000000000000000000007b000800000020000f0f0400010000000100000000779fd609004b1200010000000000000000000000000000000000000000000000000000000000000000000000003c0c0001780a0100000006020000"
)

func TestNIBLayoutMatchesHerdsman(t *testing.T) {
	if nibLayout.length(false) != 110 || nibLayout.length(true) != 116 {
		t.Fatalf("NIB lengths %d/%d, herdsman 110/116", nibLayout.length(false), nibLayout.length(true))
	}
	aligned := mustHex(t, herdsmanNIBAligned)
	r, err := nibLayout.decode(aligned)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(r.encode(false)); got != herdsmanNIBUnaligned {
		t.Fatalf("unaligned serialization differs:\n%s\n%s", got, herdsmanNIBUnaligned)
	}
	if got := hex.EncodeToString(r.encode(true)); got != herdsmanNIBAligned {
		t.Fatalf("aligned round trip differs")
	}
	u, err := nibLayout.decode(mustHex(t, herdsmanNIBUnaligned))
	if err != nil || hex.EncodeToString(u.encode(true)) != herdsmanNIBAligned {
		t.Fatalf("unaligned->aligned differs: %v", err)
	}
	c, _ := nibLayout.decode(mustHex(t, herdsmanNIBCommissioned))
	if c.u16("nwkPanId") != 0x007b || c.u8("nwkLogicalChannel") != 21 || hex.EncodeToString(c.reversed("extendedPANID")) != "00124b0009d69f77" {
		t.Fatalf("pan=%04x ch=%d epid=%x", c.u16("nwkPanId"), c.u8("nwkLogicalChannel"), c.reversed("extendedPANID"))
	}
	if c.u8("SecurityLevel") != 5 || c.u32("channelList") != 1<<21 {
		t.Fatalf("security=%d mask=%08x", c.u8("SecurityLevel"), c.u32("channelList"))
	}
}

func TestTableLayoutsMatchHerdsman(t *testing.T) {
	// Empty security manager table, 8 entries (structs.test.ts).
	entries := make([]record, 8)
	for i := range entries {
		entries[i] = emptySecurityEntry()
	}
	if got := hex.EncodeToString(encodeSecurityTable(entries, false)); got != "0000feff000000feff000000feff000000feff000000feff000000feff000000feff000000feff000000" {
		t.Fatalf("unaligned security table %s", got)
	}
	if got := hex.EncodeToString(encodeSecurityTable(entries, true)); got != "0000feff00000000feff00000000feff00000000feff00000000feff00000000feff00000000feff00000000feff00000000" {
		t.Fatalf("aligned security table %s", got)
	}
	// Address manager entry: aligned padding byte is 0xff (adapter.test.ts).
	am, err := addressManagerLayout.decode(mustHex(t, "01ff4f3a0800000000000000"))
	if err != nil || am.u8("user") != 1 || am.u16("nwkAddr") != 0x3a4f {
		t.Fatalf("address entry %v %+v", err, am)
	}
	if hex.EncodeToString(addressManagerLayout.empty().encode(true)) != "00ff00000000000000000000" {
		t.Fatal("aligned empty address entry padding")
	}
	if addressManagerLayout.length(false) != 11 || apsTcLinkKeyLayout.length(false) != 19 || apsTcLinkKeyLayout.length(true) != 20 ||
		apsLinkKeyDataLayout.length(true) != 24 || nwkSecMaterialLayout.length(true) != 12 || nwkKeyDescriptorLayout.length(true) != 18 {
		t.Fatal("entry sizes differ from herdsman mocks")
	}
	k, err := nwkKeyDescriptorLayout.decode(mustHex(t, "0001030507090b0d0f00020406080a0c0d00"))
	if err != nil || hex.EncodeToString(k.bytes("key")) != "01030507090b0d0f00020406080a0c0d" {
		t.Fatalf("aligned key descriptor %v", err)
	}
	if m, _ := packChannels([]int{11, 15, 20, 25}); m != 0x02108800 || len(unpackChannels(m)) != 4 {
		t.Fatalf("channel mask %08x", m)
	}
}
