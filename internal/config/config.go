package config

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	Version           int     `json:"version"`
	Database          string  `json:"database"`
	DeviceDefinitions string  `json:"device_definitions"`
	MQTT              MQTT    `json:"mqtt"`
	Serial            Serial  `json:"serial"`
	Web               Web     `json:"web"`
	Logging           Logging `json:"logging"`
}
type MQTT struct {
	Server             string `json:"server"`
	BaseTopic          string `json:"base_topic"`
	User               string `json:"user,omitempty"`
	Password           string `json:"password,omitempty"`
	ClientID           string `json:"client_id"`
	CA                 string `json:"ca,omitempty"`
	Cert               string `json:"cert,omitempty"`
	Key                string `json:"key,omitempty"`
	ServerName         string `json:"server_name,omitempty"`
	ForceDisableRetain bool   `json:"force_disable_retain,omitempty"`
}
type Serial struct {
	Port     string `json:"port"`
	Baudrate int    `json:"baudrate"`
}
type Web struct {
	Enabled bool   `json:"enabled"`
	Listen  string `json:"listen"`
}
type Logging struct {
	Level     string `json:"level"`
	File      string `json:"file"`
	Console   bool   `json:"console"`
	MaxSizeMB int    `json:"max_size_mb"`
	Backups   int    `json:"backups"`
}

var ieeePattern = regexp.MustCompile(`^0x[0-9a-f]{16}$`)

func IEEE(s string) bool  { return ieeePattern.MatchString(s) }
func Topic(s string) bool { return s != "" && len(s) <= 65535 && !strings.ContainsAny(s, "+#\x00") }
func Name(s string) bool {
	return Topic(s) && !strings.HasSuffix(s, "/set") && !strings.HasSuffix(s, "/get") && s != "bridge" && !strings.HasPrefix(s, "bridge/") && !strings.HasSuffix(s, "/") && !strings.HasPrefix(s, "/")
}

// StrictJSON refuses unknown fields, duplicate keys, trailing JSON and null roots.
func StrictJSON(b []byte, out any) error {
	if len(b) > 4<<20 {
		return fmt.Errorf("JSON exceeds 4 MiB")
	}
	if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		return fmt.Errorf("JSON object required")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	if err := checkKeys(d); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("expected one JSON value")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	return d.Decode(out)
}
func checkKeys(d *json.Decoder) error {
	t, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		keys := map[string]bool{}
		for d.More() {
			t, e := d.Token()
			if e != nil {
				return e
			}
			key, ok := t.(string)
			if !ok {
				return fmt.Errorf("invalid JSON key")
			}
			if keys[key] {
				return fmt.Errorf("duplicate JSON key %q", key)
			}
			keys[key] = true
			if e = checkKeys(d); e != nil {
				return e
			}
		}
	case '[':
		for d.More() {
			if e := checkKeys(d); e != nil {
				return e
			}
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter")
	}
	_, err = d.Token()
	return err
}
func Load(path string) (Config, error) {
	c := Config{Version: 1, Database: "devices.json", DeviceDefinitions: "device-definitions.json", MQTT: MQTT{Server: "mqtt://localhost:1883", BaseTopic: "zigbeemqttlink", ClientID: "zigbeemqttlink"}, Serial: Serial{Baudrate: 115200}, Web: Web{Enabled: true, Listen: "0.0.0.0:8080"}, Logging: Logging{Level: "info", File: "logs/zigbeemqttlink.log", Console: true, MaxSizeMB: 10, Backups: 3}}
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err = StrictJSON(b, &c); err != nil {
		return c, fmt.Errorf("config: %w", err)
	}
	base, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return c, err
	}
	resolve := func(p string) string {
		if p == "" || filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(base, p)
	}
	c.Database = resolve(c.Database)
	c.DeviceDefinitions = resolve(c.DeviceDefinitions)
	c.Logging.File = resolve(c.Logging.File)
	c.MQTT.CA = resolve(c.MQTT.CA)
	c.MQTT.Cert = resolve(c.MQTT.Cert)
	c.MQTT.Key = resolve(c.MQTT.Key)
	configPath, err := filepath.Abs(path)
	if err != nil {
		return c, err
	}
	if configPath == c.Database || configPath == c.DeviceDefinitions || configPath == c.Logging.File {
		return c, fmt.Errorf("database, definitions and log paths must differ from the main config")
	}
	if c.Serial.Port != "" && !strings.HasPrefix(c.Serial.Port, "tcp://") {
		c.Serial.Port = resolve(c.Serial.Port)
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("config.version must be 1")
	}
	if c.Database == "" || c.DeviceDefinitions == "" {
		return fmt.Errorf("database and device_definitions are required")
	}
	if c.Database == c.DeviceDefinitions || c.Database == c.Logging.File || c.DeviceDefinitions == c.Logging.File {
		return fmt.Errorf("database, definitions and log paths must differ")
	}
	if c.Serial.Port == "" || c.Serial.Baudrate <= 0 {
		return fmt.Errorf("serial.port and positive baudrate are required (TI Z-Stack)")
	}
	if !Topic(c.MQTT.BaseTopic) || strings.HasSuffix(c.MQTT.BaseTopic, "/") || c.MQTT.ClientID == "" {
		return fmt.Errorf("invalid MQTT base_topic/client_id")
	}
	if _, err := c.MQTT.brokerURL(); err != nil {
		return err
	}
	if (c.MQTT.Cert == "") != (c.MQTT.Key == "") {
		return fmt.Errorf("mqtt.cert and mqtt.key must be specified together")
	}
	if c.Web.Enabled {
		_, port, err := net.SplitHostPort(c.Web.Listen)
		if err != nil {
			return fmt.Errorf("web.listen: %w", err)
		}
		n, e := strconv.Atoi(port)
		if e != nil || n < 1 || n > 65535 {
			return fmt.Errorf("web.listen port must be 1..65535")
		}
	}
	switch c.Logging.Level {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("logging.level must be debug/info/warn/error")
	}
	if c.Logging.File == "" && !c.Logging.Console {
		return fmt.Errorf("logging requires a file or console")
	}
	if c.Logging.MaxSizeMB < 1 || c.Logging.MaxSizeMB > 1024 || c.Logging.Backups < 0 || c.Logging.Backups > 100 {
		return fmt.Errorf("logging.max_size_mb must be 1..1024; backups 0..100")
	}
	return nil
}
func (c MQTT) BrokerURL() string {
	u, err := c.brokerURL()
	if err != nil {
		return ""
	}
	return u.String()
}

// Normalize before passing the URL to Paho: its TCP transport expects host:port.
func (c MQTT) brokerURL() (*url.URL, error) {
	u, err := url.Parse(c.Server)
	if err != nil || u.Hostname() == "" || u.User != nil {
		return nil, fmt.Errorf("MQTT server must contain a host and no embedded credentials")
	}
	defaultPort := "1883"
	switch u.Scheme {
	case "mqtt", "tcp":
		u.Scheme = "tcp"
	case "mqtts", "ssl":
		u.Scheme = "ssl"
		defaultPort = "8883"
	default:
		return nil, fmt.Errorf("only mqtt/mqtts/tcp/ssl URLs are supported")
	}
	port := u.Port()
	if port == "" {
		port = defaultPort
	} else {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("MQTT port must be an integer from 1 to 65535")
		}
	}
	u.Host = net.JoinHostPort(u.Hostname(), port)
	return u, nil
}
func (c MQTT) TLS() (*tls.Config, error) {
	t := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.ServerName}
	if c.CA != "" {
		b, e := os.ReadFile(c.CA)
		if e != nil {
			return nil, e
		}
		p, e := x509.SystemCertPool()
		if e != nil {
			p = x509.NewCertPool()
		}
		if !p.AppendCertsFromPEM(b) {
			return nil, fmt.Errorf("CA file contains no certificates")
		}
		t.RootCAs = p
	}
	if c.Cert != "" {
		cert, e := tls.LoadX509KeyPair(c.Cert, c.Key)
		if e != nil {
			return nil, e
		}
		t.Certificates = []tls.Certificate{cert}
	}
	return t, nil
}
