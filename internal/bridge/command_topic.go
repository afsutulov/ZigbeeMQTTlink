package bridge

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"zigbeemqttlink/internal/store"
)

var commandTopic = regexp.MustCompile(`^(.+?)/(set|get)(?:/(.+))?$`)

func parseCommand(topic string, body []byte) (string, string, map[string]any, error) {
	m := commandTopic.FindStringSubmatch(topic)
	if m == nil {
		return "", "", nil, fmt.Errorf("unsupported command topic")
	}
	var p map[string]any
	if m[3] != "" {
		var value any
		if m[2] == "get" && len(body) == 0 {
			value = ""
		} else if json.Unmarshal(body, &value) != nil {
			value = string(body)
		}
		p = map[string]any{m[3]: value}
	} else {
		if json.Unmarshal(body, &p) != nil || p == nil {
			state := strings.ToUpper(strings.TrimSpace(string(body)))
			if state != "ON" && state != "OFF" && state != "TOGGLE" {
				return "", "", nil, fmt.Errorf("payload must be a JSON object or ON/OFF/TOGGLE")
			}
			p = map[string]any{"state": state}
		}
	}
	return m[1], m[2], p, nil
}

func (b *Bridge) resolveCommand(target string, p map[string]any) (store.Device, map[string]any, error) {
	d, ok := b.store.Find(target)
	channel := ""
	if !ok {
		pos := strings.LastIndex(target, "/")
		if pos > 0 {
			d, ok = b.store.Find(target[:pos])
			channel = target[pos+1:]
		}
		if !ok || b.definitions.Channels(d)[channel] == 0 {
			return d, nil, fmt.Errorf("unknown device or channel %q", target)
		}
	}
	if channel == "" {
		for name, ep := range b.definitions.Channels(d) {
			if ep == 1 {
				channel = name
			}
		}
	}
	out := map[string]any{}
	for key, value := range p {
		name := key
		if channel != "" && (key == "state" || key == "operation_mode") {
			name = key + "_" + channel
		}
		if _, exists := p[name]; name != key && exists {
			return d, nil, fmt.Errorf("duplicate property %q", name)
		}
		out[name] = value
	}
	return d, out, nil
}
