package bridge

import (
	"context"
	"embed"
	"encoding/json"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"time"
)

//go:embed web/index.html web/app.js
var webAssets embed.FS

func (b *Bridge) HTTP() (*http.Server, error) {
	if !b.cfg.Web.Enabled {
		return nil, nil
	}
	host, _, err := net.SplitHostPort(b.cfg.Web.Listen)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	local := host == "localhost" || (ip != nil && ip.IsLoopback())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, r *http.Request) {
		h := b.Health()
		if !h["healthy"].(bool) {
			w.WriteHeader(503)
		}
		json.NewEncoder(w).Encode(h)
	})
	mux.HandleFunc("GET /api/devices", func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(b.store.Devices()) })
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		d, ok := b.store.Find(r.URL.Query().Get("id"))
		if !ok {
			http.Error(w, "unknown device", 404)
			return
		}
		json.NewEncoder(w).Encode(b.store.State(d.IEEE))
	})
	mux.HandleFunc("GET /api/definitions", func(w http.ResponseWriter, r *http.Request) { json.NewEncoder(w).Encode(b.definitions.Snapshot()) })
	mux.HandleFunc("GET /api/admin", func(w http.ResponseWriter, r *http.Request) {
		b.adminMu.Lock()
		defer b.adminMu.Unlock()
		devices := b.store.Devices()
		states := map[string]any{}
		channels := map[string]any{}
		definitions := map[string]any{}
		for _, d := range devices {
			states[d.IEEE] = b.store.State(d.IEEE)
			channels[d.IEEE] = b.definitions.Channels(d)
			definitions[d.IEEE] = b.definitions.Description(d)
		}
		json.NewEncoder(w).Encode(map[string]any{"version": Version, "health": b.Health(), "devices": devices, "states": states, "channels": channels, "definitions": definitions, "base_topic": b.cfg.MQTT.BaseTopic})
	})
	mux.HandleFunc("POST /api/request", func(w http.ResponseWriter, r *http.Request) {
		contentType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if contentType != "application/json" {
			http.Error(w, "application/json required", 415)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, e := url.Parse(origin)
			if e != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
				http.Error(w, "cross-origin mutation refused", 403)
				return
			}
		}
		if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			http.Error(w, "cross-site mutation refused", 403)
			return
		}
		var body struct {
			Request string         `json:"request"`
			Data    map[string]any `json:"data"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil || body.Request == "" || body.Data == nil {
			http.Error(w, "invalid request object", 400)
			return
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			http.Error(w, "expected one JSON object", 400)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		data, err := b.adminRequest(ctx, body.Request, body.Data)
		if err != nil {
			b.log.Warn("HTTP request rejected", "request", body.Request, "error", err)
			w.WriteHeader(400)
			json.NewEncoder(w).Encode(map[string]any{"status": "error", "error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"status": "ok", "data": data})
	})
	mux.HandleFunc("GET /app.js", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		p, _ := webAssets.ReadFile("web/app.js")
		w.Write(p)
	})
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		p, _ := webAssets.ReadFile("web/index.html")
		w.Write(p)
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if local {
			requestedHost := r.Host
			if host, _, e := net.SplitHostPort(requestedHost); e == nil {
				requestedHost = host
			}
			requestedIP := net.ParseIP(requestedHost)
			if requestedHost != "localhost" && (requestedIP == nil || !requestedIP.IsLoopback()) {
				http.Error(w, "loopback HTTP requires a loopback Host header", 403)
				return
			}
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; connect-src 'self'; style-src 'unsafe-inline'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		mux.ServeHTTP(w, r)
	})
	return &http.Server{Addr: b.cfg.Web.Listen, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}, nil
}
