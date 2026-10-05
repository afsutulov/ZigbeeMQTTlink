package bridge

import "sync/atomic"

// Counters describe this process lifetime. Published reports exclude cached state
// replay at startup, so a successful subscription does not imply radio traffic.
type diagnostics struct {
	radioEvents, incomingFrames, invalidFrames                         atomic.Uint64
	unknownAddress, noStateUpdate, conversionErrors                    atomic.Uint64
	convertedReports, publishedReports, publishErrors                  atomic.Uint64
	unsupportedExtended                                                atomic.Uint64
	mqttCommands, retainedIgnored, rejectedCommands, transportAccepted atomic.Uint64
	lastRadioEvent                                                     atomic.Int64
	droppedEvents                                                      atomic.Uint64
	recoveryDropped, recoveryReplayed                                  atomic.Uint64
	framingErrors, panics, addressRecovered                            atomic.Uint64
}

func (d *diagnostics) snapshot() map[string]any {
	return map[string]any{
		"radio_events":                       d.radioEvents.Load(),
		"incoming_frames":                    d.incomingFrames.Load(),
		"invalid_frames":                     d.invalidFrames.Load(),
		"unknown_address":                    d.unknownAddress.Load(),
		"no_state_update":                    d.noStateUpdate.Load(),
		"conversion_errors":                  d.conversionErrors.Load(),
		"converted_reports":                  d.convertedReports.Load(),
		"published_reports":                  d.publishedReports.Load(),
		"publish_errors":                     d.publishErrors.Load(),
		"unsupported_extended":               d.unsupportedExtended.Load(),
		"mqtt_commands_received":             d.mqttCommands.Load(),
		"retained_commands_ignored":          d.retainedIgnored.Load(),
		"commands_rejected":                  d.rejectedCommands.Load(),
		"device_commands_transport_accepted": d.transportAccepted.Load(),
		"last_radio_event_unix":              d.lastRadioEvent.Load(),
		"dropped_radio_events":               d.droppedEvents.Load(),
		"znp_framing_errors":                 d.framingErrors.Load(),
		"radio_event_panics":                 d.panics.Load(),
		"address_recovery_frames_dropped":    d.recoveryDropped.Load(),
		"address_recovery_frames_replayed":   d.recoveryReplayed.Load(),
		"network_addresses_recovered":        d.addressRecovered.Load(),
	}
}
