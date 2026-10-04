package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"zigbeemqttlink/internal/bridge"
	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/device"
	"zigbeemqttlink/internal/logging"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/znp"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ZigbeeMQTTlink:", err)
		os.Exit(1)
	}
}
func run() (runErr error) {
	path := flag.String("config", "config.json", "JSON configuration file")
	check := flag.Bool("check-config", false, "validate configuration, device database and definitions, then exit")
	version := flag.Bool("version", false, "print version")
	flag.Parse()
	if *version {
		fmt.Println("ZigbeeMQTTlink " + bridge.Version)
		return nil
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	definitions, err := device.Load(cfg.DeviceDefinitions)
	if err != nil {
		return fmt.Errorf("device definitions: %w", err)
	}
	s, err := store.New(cfg.Database)
	if err != nil {
		return err
	}
	if *check {
		fmt.Printf("ZigbeeMQTTlink: JSON valid; definitions=%d; devices=%d\n", len(definitions.Snapshot().Devices), len(s.Devices()))
		return nil
	}
	log, file, err := logging.Open(cfg.Logging)
	if err != nil {
		return err
	}
	slog.SetDefault(log)
	defer func() {
		if runErr != nil {
			log.Error("stopped", "error", runErr)
		}
		if file != nil {
			if e := file.Close(); runErr == nil {
				runErr = e
			}
		}
	}()
	lock, err := store.Lock(cfg.Database)
	if err != nil {
		return err
	}
	defer lock.Close()
	// Load after taking the lock as well: another writer may have saved a new
	// snapshot between initial validation and acquiring the lock.
	s, err = store.New(cfg.Database)
	if err != nil {
		return err
	}
	log.Info("ZigbeeMQTTlink starting", "version", bridge.Version, "database", cfg.Database, "definitions", len(definitions.Snapshot().Devices), "log_level", cfg.Logging.Level)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 60*time.Second)
	a, err := znp.Open(startup, cfg.Serial)
	if err != nil {
		cancel()
		return err
	}
	defer a.Close()
	a.ExpectedIEEE = s.Coordinator()
	if err = a.Start(startup); err != nil {
		cancel()
		return err
	}
	cancel()
	s.SetCoordinator(a.IEEE)
	log.Info("existing network ready", "coordinator", a.IEEE, "pan_id", a.PAN, "channel", a.Channel)
	if err = s.Save(); err != nil {
		return err
	}
	b := bridge.New(cfg, s, a, log, definitions)
	server, err := b.HTTP()
	if err != nil {
		return err
	}
	httpErr := make(chan error, 1)
	if server != nil {
		go func() {
			log.Info("HTTP listening", "address", server.Addr)
			if e := server.ListenAndServe(); e != nil && e != http.ErrServerClosed {
				httpErr <- e
				stop()
			}
		}()
	}
	err = b.Run(ctx)
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if server != nil {
		if e := server.Shutdown(shutdown); err == nil {
			err = e
		}
	}
	select {
	case e := <-httpErr:
		return e
	default:
	}
	log.Info("ZigbeeMQTTlink stopped")
	return err
}
