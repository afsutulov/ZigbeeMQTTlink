package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"zigbeemqttlink/internal/bridge"
	"zigbeemqttlink/internal/config"
	"zigbeemqttlink/internal/store"
	"zigbeemqttlink/internal/znp"
)

// maintenance runs one-shot coordinator operations. The device database lock
// is held by the caller, so the service cannot use the coordinator meanwhile.
type maintenance struct {
	cfg        config.Config
	store      *store.Store
	log        *slog.Logger
	force      bool
	configPath string
}

const maintenanceTimeout = 10 * time.Minute

func source() string { return "zigbeemqttlink@" + bridge.Version }

func (m maintenance) open(ctx context.Context) (*znp.Adapter, znp.CoordinatorStatus, error) {
	a, err := znp.Open(ctx, m.cfg.Serial)
	if err != nil {
		return nil, znp.CoordinatorStatus{}, fmt.Errorf("open coordinator %s: %w", m.cfg.Serial.Port, err)
	}
	st, err := znp.Inspect(ctx, a.C)
	if err != nil {
		a.Close()
		return nil, st, err
	}
	m.log.Info("coordinator", "ieee", st.IEEE, "zstack_product", st.Product, "network", st.Configured)
	return a, st, nil
}

func (m maintenance) paired(ieee string) bool {
	_, ok := m.store.ByIEEE(ieee)
	return ok
}

// safetyBackup saves the network that is about to be replaced.
func (m maintenance) safetyBackup(ctx context.Context, a *znp.Adapter, why string) (string, error) {
	b, err := znp.CreateBackup(ctx, a.C, source())
	if err != nil {
		return "", fmt.Errorf("could not back up the existing network before %s (nothing was changed): %w", why, err)
	}
	dir := filepath.Dir(m.cfg.CoordinatorBackup)
	path := filepath.Join(dir, fmt.Sprintf("coordinator_backup-before-%s-%s.json", why, time.Now().UTC().Format("20060102T150405.000000000")))
	if _, err = znp.SaveBackupFile(path, b, m.paired, false); err != nil {
		return "", err
	}
	return path, nil
}

// verifyAndRecord starts the coordinator like the service does, saves the
// current backup and records the coordinator IEEE in the device database.
func (m maintenance) verifyAndRecord(ctx context.Context, a *znp.Adapter, expectIEEE string, verify func(*znp.Backup) error) (*znp.Backup, znp.SaveResult, error) {
	a.ExpectedIEEE = expectIEEE
	if err := a.Start(ctx); err != nil {
		return nil, znp.SaveResult{}, fmt.Errorf("coordinator check after the change failed: %w", err)
	}
	b, err := znp.CreateBackup(ctx, a.C, source())
	if err != nil {
		return nil, znp.SaveResult{}, err
	}
	if verify != nil {
		if err := verify(b); err != nil {
			return b, znp.SaveResult{}, err
		}
	}
	res, err := znp.SaveBackupFile(m.cfg.CoordinatorBackup, b, m.paired, true)
	if err != nil {
		return b, res, fmt.Errorf("network is ready, but the backup could not be saved: %w", err)
	}
	m.store.SetCoordinator(a.IEEE)
	return b, res, m.store.Save()
}

func (m maintenance) formNetwork(parent context.Context, channel int) error {
	ctx, cancel := context.WithTimeout(parent, maintenanceTimeout)
	defer cancel()
	opts, err := znp.RandomNetwork(channel)
	if err != nil {
		return err
	}
	a, st, err := m.open(ctx)
	if err != nil {
		return err
	}
	defer a.Close()
	devices := len(m.store.Devices())
	if (st.Configured || devices > 0) && !m.force {
		msg := "refusing to create a new network:"
		if st.Configured {
			msg += fmt.Sprintf(" the coordinator already holds network PAN 0x%04x on channel %d;", st.PanID, st.Channel)
			if !st.Ready {
				msg += " it is not marked ready (an interrupted operation, or created by other software): restore a matching backup, or use -force to deliberately create a replacement network;"
			}
		}
		if devices > 0 {
			msg += fmt.Sprintf(" the device database lists %d paired devices;", devices)
		}
		return fmt.Errorf("%s a new network disconnects every device, each must then be paired again. Use -force only if that is intended (the current network is backed up automatically first)", msg)
	}
	if st.Configured {
		path, err := m.safetyBackup(ctx, a, "form")
		if err != nil {
			return err
		}
		fmt.Printf("Previous network backed up to %s\n", path)
	}
	opts, err = znp.FormNetwork(ctx, a.C, opts, m.log)
	if err != nil {
		return fmt.Errorf("network formation failed: %w", err)
	}
	b, res, err := m.verifyAndRecord(ctx, a, "", nil)
	if err != nil {
		return err
	}
	m.log.Info("new network created", "pan_id", fmt.Sprintf("0x%04x", b.PanID), "extended_pan_id", hex.EncodeToString(b.ExtendedPanID), "channel", b.Channel, "backup", res.Path)
	fmt.Printf("New Zigbee network created.\n  PAN ID:          0x%04x\n  Extended PAN ID: %s\n  Channel:         %d\n  Coordinator:     %s\n  Backup:          %s\n",
		b.PanID, hex.EncodeToString(b.ExtendedPanID), b.Channel, a.IEEE, res.Path)
	if res.Preserved != "" {
		fmt.Printf("  Backup of the previous network kept at %s\n", res.Preserved)
	}
	if devices > 0 {
		fmt.Printf("The device database still lists %d devices of the old network: they must be paired again (permit_join).\n", devices)
	}
	fmt.Println("The backup contains the network key: keep a copy in a safe place, outside this computer.")
	return nil
}

func (m maintenance) backup(parent context.Context, path string) error {
	if err := m.checkBackupOutput(path); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, maintenanceTimeout)
	defer cancel()
	a, st, err := m.open(ctx)
	if err != nil {
		return err
	}
	defer a.Close()
	if !st.Configured {
		return fmt.Errorf("the coordinator has no network to back up")
	}
	b, err := znp.CreateBackup(ctx, a.C, source())
	if err != nil {
		return err
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	if old, rerr := znp.ReadBackupFile(abs); rerr == nil && !old.SameNetwork(b) && !m.force {
		return fmt.Errorf("%s holds a backup of another network (PAN 0x%04x); choose another file or use -force", abs, old.PanID)
	}
	res, err := znp.SaveBackupFile(abs, b, m.paired, true)
	if err != nil {
		return err
	}
	keyed := 0
	for _, d := range b.Devices {
		if d.LinkKey != nil {
			keyed++
		}
	}
	fmt.Printf("Backup written to %s\n  PAN ID 0x%04x, channel %d, coordinator %s, %d devices (%d with link keys)\n",
		res.Path, b.PanID, b.Channel, b.IEEEString(), len(b.Devices), keyed)
	if len(res.Merged) > 0 {
		fmt.Printf("  %d paired devices missing from the coordinator tables were kept from the previous backup\n", len(res.Merged))
	}
	if res.Preserved != "" {
		fmt.Printf("  Previous file of another network kept at %s\n", res.Preserved)
	}
	fmt.Println("The backup contains the network key: keep it private.")
	return nil
}

func (m maintenance) restore(parent context.Context, path string) error {
	ctx, cancel := context.WithTimeout(parent, maintenanceTimeout)
	defer cancel()
	sourceBytes, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("backup %s: %w", path, err)
	}
	b, err := znp.ParseBackup(sourceBytes)
	if err != nil {
		return fmt.Errorf("backup %s: %w", path, err)
	}
	if db := m.store.Coordinator(); db != "" && db != b.IEEEString() && !m.force {
		return fmt.Errorf("the device database belongs to coordinator %s, the backup to %s; restoring would leave the database unusable. Use the matching backup, or -force", db, b.IEEEString())
	}
	if saved, err := znp.ReadBackupFile(m.cfg.CoordinatorBackup); err == nil {
		b.RaiseCountersFrom(saved)
	}
	a, st, err := m.open(ctx)
	if err != nil {
		return err
	}
	defer a.Close()
	if st.Configured {
		current, cerr := znp.CreateBackup(ctx, a.C, source())
		same := cerr == nil && current.SameNetwork(b) && strings.EqualFold(current.IEEEString(), b.IEEEString())
		switch {
		case same && st.Ready && !m.force:
			fmt.Println("The coordinator already runs the network from this backup; nothing to do (use -force to write it again).")
			return nil
		case same && !st.Ready:
			// The same network without the ready marker: a previous restore was
			// interrupted or failed after writing it. Finishing it replaces
			// nothing, so -force is not required and the file is the backup.
			fmt.Println("The coordinator holds this network from an interrupted restore; restoring it again.")
		case !m.force && cerr != nil:
			return fmt.Errorf("the coordinator holds a network that cannot be read (%v); restoring replaces it. Use -force if that is intended", cerr)
		case !m.force:
			return fmt.Errorf("the coordinator runs another network (PAN 0x%04x, channel %d); restoring replaces it. Use -force if that is intended (it is backed up automatically first)", st.PanID, st.Channel)
		case cerr != nil:
			fmt.Printf("Warning: the current network cannot be read and is NOT backed up (%v); replacing it as requested with -force.\n", cerr)
		case !same:
			saved, err := m.safetyBackup(ctx, a, "restore")
			if err != nil {
				return err
			}
			fmt.Printf("Previous network backed up to %s\n", saved)
		}
	}
	preserved, err := m.preserveRestoreSource(sourceBytes)
	if err != nil {
		return fmt.Errorf("could not preserve restore source; coordinator unchanged: %w", err)
	}
	fmt.Printf("Restore source preserved at %s\n", preserved)
	m.log.Info("restoring network", "file", path, "pan_id", fmt.Sprintf("0x%04x", b.PanID), "channel", b.Channel, "devices", len(b.Devices), "source", b.Source)
	if err = znp.RestoreNetworkWithOptions(ctx, a.C, b, m.log, znp.RestoreOptions{AllowUnreadableCurrent: m.force}); err != nil {
		return fmt.Errorf("restore failed; the coordinator may hold a partial network, run -restore again: %w", err)
	}
	var differences []string
	after, res, err := m.verifyAndRecord(ctx, a, b.IEEEString(), func(actual *znp.Backup) error {
		if err := b.VerifyRestored(actual); err != nil {
			return err
		}
		// SaveBackupFile merges missing keys and counters; capture the actual radio
		// differences BEFORE that merge can hide them from the CLI.
		differences = b.DeviceDifferences(actual)
		return nil
	})
	if err != nil {
		return err
	}
	if len(differences) > 0 {
		fmt.Printf("Warning: %d device table differences; check these devices and re-pair if necessary:\n", len(differences))
		for _, d := range differences {
			fmt.Printf("  %s\n", d)
		}
	}
	if !after.SameNetwork(b) || after.IEEEString() != b.IEEEString() {
		return fmt.Errorf("verification failed: the coordinator reports PAN 0x%04x/%s instead of the backup", after.PanID, after.IEEEString())
	}
	fmt.Printf("Network restored.\n  PAN ID:      0x%04x\n  Channel:     %d\n  Coordinator: %s (cloned from the backup)\n  Devices:     %d in the coordinator tables\n  Backup:      %s\n",
		after.PanID, after.Channel, after.IEEEString(), len(after.Devices), res.Path)
	if len(differences) > 0 {
		fmt.Println("Network restored with device warnings. Start the service, check every affected device; some may require re-pairing.")
	} else {
		fmt.Println("Start the service and verify device operation. Sleeping devices may need a button press to report again.")
	}
	fmt.Println("Never run the old coordinator again in range: two coordinators of the same network disrupt it.")
	return nil
}

// periodicBackup refreshes the coordinator backup shortly after start and
// then daily. It shares the coordinator link with the bridge; every NV read
// is one short request, so radio traffic keeps flowing.
func periodicBackup(ctx context.Context, a *znp.Adapter, cfg config.Config, s *store.Store, log *slog.Logger) {
	paired := func(ieee string) bool { _, ok := s.ByIEEE(ieee); return ok }
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.Done():
			return
		case <-timer.C:
		}
		op, cancel := context.WithTimeout(ctx, 5*time.Minute)
		b, err := znp.CreateBackup(op, a.C, source())
		cancel()
		if err == nil {
			var res znp.SaveResult
			res, err = znp.SaveBackupFile(cfg.CoordinatorBackup, b, paired, false)
			if err == nil {
				if res.Path != cfg.CoordinatorBackup {
					log.Warn("coordinator runs a different network than the existing backup file; new backup written separately", "kept", res.Preserved, "written", res.Path)
				} else {
					log.Info("coordinator backup updated", "file", res.Path, "devices", len(b.Devices), "merged_from_previous", len(res.Merged))
				}
			}
		}
		if err != nil {
			log.Warn("coordinator backup failed; retrying in 1 hour", "error", err)
			timer.Reset(time.Hour)
			continue
		}
		timer.Reset(24 * time.Hour)
	}
}

func (m maintenance) checkBackupOutput(path string) error {
	protected := []string{m.configPath, m.cfg.Database, m.cfg.DeviceDefinitions, m.cfg.Logging.File, m.cfg.MQTT.CA, m.cfg.MQTT.Cert, m.cfg.MQTT.Key}
	if !strings.HasPrefix(m.cfg.Serial.Port, "tcp://") {
		protected = append(protected, m.cfg.Serial.Port)
	}
	if exe, err := os.Executable(); err == nil {
		protected = append(protected, exe)
	}
	for _, p := range protected {
		if config.SamePath(path, p) {
			return fmt.Errorf("backup output %s refers to protected project file %s; choose another path", path, p)
		}
	}
	return nil
}

// Preserve the exact input before any coordinator writes. Final/automatic
// snapshots may contain fewer keys than the input, especially when firmware
// drops entries. A unique, private source copy remains independently of them.
func (m maintenance) preserveRestoreSource(raw []byte) (string, error) {
	if _, err := znp.ParseBackup(raw); err != nil {
		return "", err
	}
	dir := filepath.Dir(m.cfg.CoordinatorBackup)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, "coordinator_backup-restore-source-*.json")
	if err != nil {
		return "", err
	}
	keep := f.Name()
	success := false
	defer func() {
		f.Close()
		if !success {
			os.Remove(keep)
		}
	}()
	if _, err = f.Write(raw); err != nil {
		return "", err
	}
	if err = f.Sync(); err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	d, err := os.Open(dir)
	if err != nil {
		return "", err
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return "", err
	}
	success = true
	return keep, nil
}
