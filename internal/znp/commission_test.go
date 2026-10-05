package znp

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func fastTimings(t *testing.T) {
	t.Helper()
	old := []time.Duration{nibSettleInterval, restoreSettleDelay}
	nibSettleInterval, restoreSettleDelay = time.Millisecond, time.Millisecond
	t.Cleanup(func() { nibSettleInterval, restoreSettleDelay = old[0], old[1] })
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

var testSeed = mustHexStatic("928a2c479e72a9a53e3b5133fc55021f")

func mustHexStatic(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func fixedNetwork() NetworkOptions {
	return NetworkOptions{PanID: 0x1a2b, ExtendedPanID: mustHexStatic("dd00112233445566"), Channels: []int{15}, NetworkKey: mustHexStatic("00112233445566778899aabbccddeeff")}
}

func TestFormNetworkBothPlatforms(t *testing.T) {
	fastTimings(t)
	for _, product := range []byte{ProductZStack3x0, ProductZStack30x} {
		e, c := newEmulator(t, product, 0x00124b0001020304)
		ctx := testCtx(t)
		st, err := Inspect(ctx, c)
		if err != nil || st.Configured {
			t.Fatalf("%v: fresh stick configured=%v err=%v", e, st.Configured, err)
		}
		o := fixedNetwork()
		if _, err = FormNetwork(ctx, c, o, quietLog()); err != nil {
			t.Fatalf("%v: %v", e, err)
		}
		st, err = Inspect(ctx, c)
		if err != nil || !st.Configured || st.PanID != o.PanID || st.Channel != 15 || !bytes.Equal(st.ExtPanID, o.ExtendedPanID) {
			t.Fatalf("%v: after formation %+v err=%v", e, st, err)
		}
		// Commissioning items were written in the NV representation Z-Stack uses.
		if !bytes.Equal(e.nv[nvExtendedPanID], reverse(o.ExtendedPanID)) || !bytes.Equal(e.nv[nvApsUseExtPanID], reverse(o.ExtendedPanID)) || !bytes.Equal(e.nv[nvPreCfgKey], o.NetworkKey) ||
			!bytes.Equal(e.nv[nvLogicalType], []byte{0}) || !bytes.Equal(e.nv[nvPreCfgKeysEnable], []byte{0}) || e.nv[nvStartupOption][0] != 0 {
			t.Fatalf("%v: commissioning items %x %x", e, e.nv[nvExtendedPanID], e.nv[nvPreCfgKey])
		}
		// The normal service start accepts the new network.
		a := &Adapter{C: c, inFlight: make(chan struct{}, maxInFlight)}
		if err = a.Start(ctx); err != nil || a.PAN != o.PanID || a.Channel != 15 || a.IEEE != "0x00124b0001020304" {
			t.Fatalf("%v: Start after formation: %v %+v", e, err, a)
		}
	}
}

func TestFormNetworkRetriesPANCollision(t *testing.T) {
	fastTimings(t)
	e, c := newEmulator(t, ProductZStack3x0, 1)
	e.collisions = 1
	got, err := FormNetwork(testCtx(t), c, fixedNetwork(), quietLog())
	if err != nil {
		t.Fatal(err)
	}
	if got.PanID == fixedNetwork().PanID || !bytes.Equal(got.NetworkKey, fixedNetwork().NetworkKey) || got.Channels[0] != 15 {
		t.Fatalf("collision retry kept the colliding PAN or changed key/channel: %+v", got)
	}
}

func TestRandomNetworkIsRandomAndValid(t *testing.T) {
	a, err := RandomNetwork(20)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := RandomNetwork(20)
	if a.validate() != nil || bytes.Equal(a.NetworkKey, b.NetworkKey) || bytes.Equal(a.ExtendedPanID, b.ExtendedPanID) {
		t.Fatalf("random networks not distinct/valid: %+v %+v", a, b)
	}
	if _, err = RandomNetwork(27); err == nil {
		t.Fatal("channel 27 accepted")
	}
}

func checkDevices(t *testing.T, where string, b *Backup, txMargin uint32) {
	t.Helper()
	if len(b.Devices) != 3 {
		t.Fatalf("%s: devices %+v", where, b.Devices)
	}
	byIEEE := map[string]BackupDevice{}
	for _, d := range b.Devices {
		byIEEE[hex.EncodeToString(d.IEEE)] = d
	}
	x := byIEEE["00158d0001a2b3c4"]
	if x.LinkKey != nil || !x.IsDirectChild || *x.NetworkAddress != 0x1111 {
		t.Fatalf("%s: Xiaomi child %+v", where, x)
	}
	k := byIEEE["a4c1380000000001"]
	if k.LinkKey == nil || string(k.LinkKey.Key) != "0123456789abcdef" || k.LinkKey.TxCounter != 77+txMargin || k.LinkKey.RxCounter != 66 || *k.NetworkAddress != 0x2222 {
		t.Fatalf("%s: APS key device %+v %+v", where, k, k.LinkKey)
	}
	s := byIEEE["0c4314fffe000002"]
	want := deriveTCLinkKey(testSeed, 5, s.IEEE)
	if s.LinkKey == nil || !bytes.Equal(s.LinkKey.Key, want) || s.IsDirectChild || s.LinkKey.TxCounter != 12+txMargin || s.LinkKey.RxCounter != 34 {
		t.Fatalf("%s: seed key device %+v", where, s)
	}
}

func TestBackupRestoreRoundTrip(t *testing.T) {
	fastTimings(t)
	for _, pair := range [][2]byte{{ProductZStack3x0, ProductZStack3x0}, {ProductZStack30x, ProductZStack30x}, {ProductZStack30x, ProductZStack3x0}, {ProductZStack3x0, ProductZStack30x}} {
		ctx := testCtx(t)
		src, c1 := newEmulator(t, pair[0], 0x00124b00aaaaaaaa)
		if _, err := FormNetwork(ctx, c1, fixedNetwork(), quietLog()); err != nil {
			t.Fatal(err)
		}
		src.pair(t, testSeed)
		b1, err := CreateBackup(ctx, c1, "test")
		if err != nil {
			t.Fatalf("%v backup: %v", src, err)
		}
		if b1.PanID != 0x1a2b || b1.Channel != 15 || b1.FrameCounter != 4321 || !bytes.Equal(b1.NetworkKey, fixedNetwork().NetworkKey) ||
			b1.IEEEString() != "0x00124b00aaaaaaaa" || !bytes.Equal(b1.TCLKSeed, testSeed) || b1.SecurityLevel != 5 {
			t.Fatalf("%v backup content %+v", src, b1)
		}
		checkDevices(t, "backup", b1, 0)
		raw, err := json.Marshal(b1)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := ParseBackup(raw)
		if err != nil || !parsed.SameNetwork(b1) || len(parsed.Devices) != 3 {
			t.Fatalf("JSON round trip: %v", err)
		}

		dst, c2 := newEmulator(t, pair[1], 0x00124b00bbbbbbbb)
		if err = RestoreNetwork(ctx, c2, parsed, quietLog()); err != nil {
			t.Fatalf("%v -> %v restore: %v", src, dst, err)
		}
		b2, err := CreateBackup(ctx, c2, "test")
		if err != nil {
			t.Fatalf("%v backup after restore: %v", dst, err)
		}
		if !b2.SameNetwork(b1) || b2.Channel != 15 || b2.IEEEString() != "0x00124b00aaaaaaaa" || b2.FrameCounter != 4321+2500 ||
			b2.SecurityLevel != 5 || !bytes.Equal(b2.TCLKSeed, testSeed) {
			t.Fatalf("%v -> %v: restored %+v", src, dst, b2)
		}
		checkDevices(t, "restored", b2, 2500)
		// The network-specific frame counter row is keyed by the extended PAN
		// ID in NV (little-endian) order, as Z-Stack looks it up.
		row := dst.nv[nvLegacySecMaterial]
		if pair[1] == ProductZStack3x0 {
			row = dst.ex[exID{1, nvExNwkSecMaterialTable, 0}]
		}
		m, err := nwkSecMaterialLayout.decode(row)
		if err != nil || !bytes.Equal(m.bytes("extendedPANID"), reverse(fixedNetwork().ExtendedPanID)) || m.u32("FrameCounter") != 4321+2500 {
			t.Fatalf("%v: frame counter row %x %v", dst, row, err)
		}
		a := &Adapter{C: c2, inFlight: make(chan struct{}, maxInFlight), ExpectedIEEE: "0x00124b00aaaaaaaa"}
		if err = a.Start(ctx); err != nil || a.PAN != 0x1a2b || a.Channel != 15 {
			t.Fatalf("%v: service start after restore: %v", dst, err)
		}
	}
}

// A backup written by zigbee-herdsman (Zigbee2MQTT) restores and reads back
// unchanged: fixture from herdsman test/adapter/z-stack/adapter.test.ts.
const herdsmanBackup = `{
 "metadata": {"format": "zigpy/open-coordinator-backup", "version": 1, "source": "zigbee-herdsman@0.13.65",
  "internal": {"date": "2021-03-03T19:15:40.524Z", "znpVersion": 2}},
 "stack_specific": {"zstack": {"tclk_seed": "928a2c479e72a9a53e3b5133fc55021f"}},
 "coordinator_ieee": "00124b0009d80ba7", "pan_id": "007b", "extended_pan_id": "00124b0009d69f77",
 "nwk_update_id": 0, "security_level": 5, "channel": 21, "channel_mask": [21],
 "network_key": {"key": "01030507090b0d0f00020406080a0c0d", "sequence_number": 0, "frame_counter": 16754},
 "devices": [
  {"nwk_address": "ddf6", "ieee_address": "00124b002226ef87"},
  {"nwk_address": "c2dc", "ieee_address": "04cf8cdf3c79455f", "link_key": {"key": "0e768569dd935d8e7302e74e7629f13f", "rx_counter": 0, "tx_counter": 275}},
  {"nwk_address": "740a", "ieee_address": "680ae2fffeae5647", "link_key": {"key": "7c079d02aae015facd7ae9608d4baf56", "rx_counter": 0, "tx_counter": 275}},
  {"nwk_address": "19fa", "ieee_address": "00158d00024fa79b", "link_key": {"key": "cea550908aa1529ee90eea3c3bdc26fc", "rx_counter": 0, "tx_counter": 44}},
  {"nwk_address": "6182", "ieee_address": "00158d00024f4518", "link_key": {"key": "267e1e31fcd8171f8acf63459effbca5", "rx_counter": 0, "tx_counter": 44}},
  {"nwk_address": "4285", "ieee_address": "00158d00024f810d", "is_child": false, "link_key": {"key": "55ba1e31fcd8171f9f0b63459effbca5", "rx_counter": 0, "tx_counter": 44}},
  {"ieee_address": "00158d00024f810e", "is_child": true, "link_key": {"key": "55ba1e31fcd8171fee0b63459effeea5", "rx_counter": 24, "tx_counter": 91}}
 ]
}`

func TestRestoreZigbee2MQTTBackup(t *testing.T) {
	fastTimings(t)
	in, err := ParseBackup([]byte(herdsmanBackup))
	if err != nil {
		t.Fatal(err)
	}
	if in.PanID != 0x7b || in.Devices[6].NetworkAddress != nil || in.Devices[5].IsDirectChild || !in.Devices[0].IsDirectChild {
		t.Fatalf("parse %+v", in)
	}
	for _, product := range []byte{ProductZStack3x0, ProductZStack30x} {
		ctx := testCtx(t)
		e, c := newEmulator(t, product, 0x00124b00cccccccc)
		if err = RestoreNetwork(ctx, c, in, quietLog()); err != nil {
			t.Fatalf("%v: %v", e, err)
		}
		out, err := CreateBackup(ctx, c, "test")
		if err != nil {
			t.Fatal(err)
		}
		if !out.SameNetwork(in) || out.IEEEString() != "0x00124b0009d80ba7" || out.Channel != 21 || out.FrameCounter != 16754+2500 {
			t.Fatalf("%v: %+v", e, out)
		}
		if len(out.Devices) != len(in.Devices) {
			t.Fatalf("%v: %d devices restored, want %d", e, len(out.Devices), len(in.Devices))
		}
		for i, d := range in.Devices {
			got := out.Devices[i]
			if !bytes.Equal(got.IEEE, d.IEEE) || got.IsDirectChild != d.IsDirectChild {
				t.Fatalf("%v: device %d %+v", e, i, got)
			}
			if (d.LinkKey == nil) != (got.LinkKey == nil) {
				t.Fatalf("%v: device %x link key presence", e, d.IEEE)
			}
			if d.LinkKey != nil && (!bytes.Equal(got.LinkKey.Key, d.LinkKey.Key) || got.LinkKey.TxCounter != d.LinkKey.TxCounter+2500 || got.LinkKey.RxCounter != d.LinkKey.RxCounter) {
				t.Fatalf("%v: device %x key %x/%d", e, d.IEEE, got.LinkKey.Key, got.LinkKey.TxCounter)
			}
		}
	}
}

func TestRestoreRejectsTooManyDevices(t *testing.T) {
	fastTimings(t)
	b, _ := ParseBackup([]byte(herdsmanBackup))
	for i := 0; i < 20; i++ {
		ieee := []byte{0x00, 0x15, 0x8d, 0, 0, 0, 0, byte(i)}
		b.Devices = append(b.Devices, BackupDevice{IEEE: ieee, IsDirectChild: true})
	}
	_, c := newEmulator(t, ProductZStack3x0, 1)
	if err := RestoreNetwork(testCtx(t), c, b, quietLog()); err == nil {
		t.Fatal("restore into a too small address table succeeded")
	}
}

func TestParseBackupValidation(t *testing.T) {
	for _, bad := range []string{
		`{"metadata":{"format":"zigpy/open-coordinator-backup","version":2}}`,
		`{"adapterType":"zStack","data":{}}`,
		`{"metadata":{"format":"x","version":1}}`,
		`not json`,
	} {
		if _, err := ParseBackup([]byte(bad)); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	for _, mutate := range []func(*ocbFile){
		func(f *ocbFile) { f.NetworkKey.Key = "0011" },
		func(f *ocbFile) { f.PanID = "ffff" },
		func(f *ocbFile) { f.Channel = 27 },
		func(f *ocbFile) { f.ExtendedPanID = "ffffffffffffffff" },
		func(f *ocbFile) { f.Devices = append(f.Devices, f.Devices[0]) },
		func(f *ocbFile) { f.ChannelMask = []int{10} },
	} {
		var f ocbFile
		_ = json.Unmarshal([]byte(herdsmanBackup), &f)
		mutate(&f)
		raw, _ := json.Marshal(f)
		if _, err := ParseBackup(raw); err == nil {
			t.Fatalf("invalid backup accepted: %s", raw)
		}
	}
}

func TestMergeMissingDevices(t *testing.T) {
	old, _ := ParseBackup([]byte(herdsmanBackup))
	cur, _ := ParseBackup([]byte(herdsmanBackup))
	cur.Devices = cur.Devices[:2]
	added := cur.MergeMissingDevices(old, func(id string) bool { return id != "0x680ae2fffeae5647" })
	if len(added) != 4 || len(cur.Devices) != 6 {
		t.Fatalf("added %v", added)
	}
	other, _ := ParseBackup([]byte(herdsmanBackup))
	other.NetworkKey = bytes.Repeat([]byte{1}, 16)
	if len(other.MergeMissingDevices(old, func(string) bool { return true })) != 0 {
		t.Fatal("merged devices from another network")
	}
}

func TestReadOnlyInspectionDoesNotWrite(t *testing.T) {
	fastTimings(t)
	e, c := newEmulator(t, ProductZStack3x0, 1)
	ctx := testCtx(t)
	if _, err := FormNetwork(ctx, c, fixedNetwork(), quietLog()); err != nil {
		t.Fatal(err)
	}
	before := e.writes
	if _, err := Inspect(ctx, c); err != nil {
		t.Fatal(err)
	}
	if _, err := CreateBackup(ctx, c, "x"); err != nil {
		t.Fatal(err)
	}
	if e.writes != before {
		t.Fatal("inspection or backup wrote NV memory")
	}
}

func TestZStack12Refused(t *testing.T) {
	_, c := newEmulator(t, ProductZStack12, 1)
	ctx := testCtx(t)
	if _, err := Inspect(ctx, c); err == nil {
		t.Fatal("Z-Stack 1.2 accepted")
	}
	if _, err := FormNetwork(ctx, c, fixedNetwork(), quietLog()); err == nil {
		t.Fatal("formation on Z-Stack 1.2 accepted")
	}
}

// Real keys from a Z-Stack TCLK table, exported by zigbee-herdsman: the seed
// derivation must reproduce them independently of our own restore code.
func TestTCLinkKeyDerivationMatchesRealBackup(t *testing.T) {
	b, err := ParseBackup([]byte(herdsmanBackup))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"04cf8cdf3c79455f": 10, "680ae2fffeae5647": 9, "00158d00024fa79b": 13, "00158d00024f4518": 8, "00158d00024f810d": -1, "00158d00024f810e": -1}
	for _, d := range b.Devices {
		if d.LinkKey == nil {
			continue
		}
		id := hex.EncodeToString(d.IEEE)
		if got := seedShift(b.TCLKSeed, d.LinkKey.Key, d.IEEE); got != want[id] {
			t.Fatalf("%s: seed shift %d, want %d", id, got, want[id])
		}
	}
}
