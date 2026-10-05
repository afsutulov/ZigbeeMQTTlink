package device

import (
	"testing"
	"time"

	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/zcl"
)

// FuzzRadioPipelineNoPanic feeds arbitrary ZCL frames through every converter
// used by the installed models. Run longer with:
//
//	go test ./internal/device -run X -fuzz FuzzRadioPipelineNoPanic -fuzztime 2m
func FuzzRadioPipelineNoPanic(f *testing.F) {
	for _, seed := range [][]byte{
		{0x19, 1, 0, 1, 0, 0, 0, 0, 0},
		{0x19, 1, 2, 1, 0, 0x0d, 1, 0, 1, 1},
		{0x18, 1, 0x0a, 0x55, 0, 0x21, 2, 0},
		{0x18, 1, 0x0a, 0x01, 0xff, 0x42, 4, 1, 0x21, 0xb8, 0x0b},
	} {
		f.Add(seed, byte(1), uint16(0), byte(0))
	}
	r, err := Load("../../device-definitions.json")
	if err != nil {
		f.Fatal(err)
	}
	models := []store.Device{
		{Model: "lumi.sensor_wleak.aq1", Manufacturer: "LUMI"}, {Model: "lumi.weather", Manufacturer: "LUMI"},
		{Model: "lumi.remote.b186acn02", Manufacturer: "LUMI"}, {Model: "lumi.relay.c2acn01", Manufacturer: "LUMI"},
		{Model: "lumi.switch.b2nc01", Manufacturer: "LUMI"}, {Model: "TS011F", Manufacturer: "_TZ3000_ew3ldmgx"},
		{Model: "TS0601", Manufacturer: "_TZE200_t1blo2bj"}, {Model: "TS0601", Manufacturer: "_TZE200_bq5c8xfe"}, {},
	}
	clusters := []uint16{0, 1, 2, 6, 0xa, 0x12, 0x402, 0x403, 0x405, 0x500, 0x702, 0xb04, 0xe000, 0xe001, 0xef00, 0xfcc0}
	f.Fuzz(func(t *testing.T, data []byte, ep byte, cl uint16, sel byte) {
		fr, err := zcl.Parse(data)
		if err != nil {
			return
		}
		d := models[int(sel)%len(models)]
		d.IEEE, d.Name, d.Network, d.Type = "0x0000000000000001", "x", 1, "EndDevice"
		d.Endpoints = []store.Endpoint{{ID: 1, Profile: 0x104, In: clusters}, {ID: 2, Profile: 0x104, In: clusters}}
		cluster := clusters[int(cl)%len(clusters)]
		_, _ = r.State(d, ep, cluster, fr)
		_, _ = r.GatewayResponse(d, fr)
		_, _ = r.TimeResponse(d, fr, time.Now())
		_, _ = zcl.PreventReset(r.NativeDevice(d), cluster, fr)
		_, _ = zcl.CommandResponse(fr)
		nd := r.NativeDevice(d)
		_ = zcl.ApplyMeasurementScales(&nd, ep, cluster, fr)
		_, _ = zcl.TimeResponse(fr, time.Now())
	})
}
