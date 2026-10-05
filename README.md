# ZigbeeMQTTlink

**English** · [Русский](README.ru.md)

A service connecting an existing Zigbee network to MQTT, with a built-in web interface, a JSON device database and device definitions that can be updated without recompiling.

ZigbeeMQTTlink communicates directly with a **TI Z-Stack coordinator** over UART or TCP. Node.js and Zigbee2MQTT are not required at runtime. Your scripts consume device reports and issue commands through MQTT; automation rules remain in those scripts.

## Contents

- [Features and scope](#features-and-scope)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Zigbee network: formation, backup, restore](#zigbee-network-formation-backup-restore)
- [Build from source](#build-from-source)
- [Project layout](#project-layout)
- [Configuration](#configuration)
- [Device database](#device-database)
- [Supported models](#supported-models)
- [MQTT](#mqtt)
- [Web interface and HTTP API](#web-interface-and-http-api)
- [Device definitions](#device-definitions)
- [Logging](#logging)
- [Running with systemd](#running-with-systemd)
- [Troubleshooting](#troubleshooting)
- [Development and validation](#development-and-validation)
- [License and provenance](#license-and-provenance)

## Features and scope

- Direct Zigbee ZNP/ZCL communication and MQTT 3.1.1 integration.
- Device reports, commands, model/manufacturer identification and endpoint discovery.
- Web device list, state inspection, commands, renaming, calibration, removal and replacement.
- Time-limited pairing mode, closed at startup.
- Separate JSON files for configuration, device database and model definitions.
- Runtime ZCL/Tuya definitions: attributes, scalar command payloads, events, datapoints, scaling, constraints and configuration actions.
- Validated definition saving/reloading through the web or MQTT without restarting.
- JSON Lines logs with configurable severity, file rotation and console output.
- Existing MQTT names preserved when replacing hardware with a compatible device.
- Retained MQTT commands ignored; transport acceptance is not reported as confirmed physical state.
- New network formation, automatic and manual network backups, restore onto a replacement coordinator without re-pairing (Z-Stack 3.x; backup format compatible with Zigbee2MQTT).

Supported coordinators are TI Z-Stack 3.x (CC2652/CC1352, CC2530/CC2538 with Z-Stack 3.0.x); Z-Stack 1.2 (CC2531 legacy firmware) can run an existing network but cannot form, back up or restore one. Other coordinator protocols, OTA, groups/scenes and a complete binding/IAS enrollment workflow are not implemented. Home Assistant discovery is not included. There is no YAML configuration, database.db importer or simulator. Binding and reporting configuration are declared in model definitions (`bind`, `report`). Unit tests run with `make test`.

JSON definitions extend the supported ZCL and Tuya datapoint formats. New proprietary handshakes, encrypted protocols, nested vendor payloads or coordinator protocols can require Go changes. An identical model ID does not guarantee identical behavior across manufacturers or firmware versions.

## Requirements

For running a release binary:

- Linux AMD64, ARM64 or ARMv7.
- A TI Z-Stack coordinator with an existing Zigbee network.
- UART access, such as `/dev/ttyUSB0`, or a TCP serial connection.
- A reachable MQTT broker.
- Write access to the directories containing the database, definitions and logs.

Language/module minimum: **Go 1.25.0**. Release binaries are built and checked with **Go 1.26.8**; `go.mod` recommends this toolchain and Go may download it automatically. Use a supported Go release with current security patches for production builds. Dependencies use Go modules and are not bundled in `vendor`. A first build needs access to the module download service unless dependencies are already cached. Release binaries do not download dependencies at runtime.

## Quick start

1. Extract the release archive or obtain the source and build it.
2. Edit `config.json`: broker, credentials, coordinator port and file paths. Replace both `CHANGE_ME` passwords in the supplied configuration; startup and `-check-config` refuse placeholder credentials.
3. For an existing installation, stop the previous process and preserve its working database as described below. The archive does not include your device database. If `database` points to a missing file, the service starts with an empty database and creates it on startup.
4. Validate the files, then start:

```bash
cd ZigbeeMQTTlink
chmod +x ./dist/zigbeemqttlink-linux-amd64
./dist/zigbeemqttlink-linux-amd64 -check-config -config ./config.json
./dist/zigbeemqttlink-linux-amd64 -config ./config.json
```

On ARM64 use `zigbeemqttlink-linux-arm64`; on ARMv7 use `zigbeemqttlink-linux-armv7`. With the supplied web settings, open `http://SERVER_IP:8080`.

`-check-config` does not open the radio or connect to MQTT. It prints the number of definitions and database devices. For migration, this count must match the working database. `devices=0` is expected only for a new empty database; it must not replace existing records.

### Migration

The version 1 JSON format of the previous Go bridge's `native-state.json` is accepted directly. Stop the previous service, back up the file, then copy it to `devices.json` or set `database` to its absolute path. No converter or re-pairing is required when using the same working network and coordinator.

This compatibility applies to the Go bridge's JSON file, **not** Zigbee2MQTT's `database.db`. The importer has been removed. Do not run two processes against the same coordinator, even with different database files.

The supplied configuration retains `mqtt.base_topic: "zigbee2mqtt"` for existing scripts. This is an MQTT namespace, not a runtime dependency. To use `zigbeemqttlink`, update both the configuration and scripts' subscriptions/publications. The MQTT client ID and default log filename use the new project name.

## Zigbee network: formation, backup, restore

Network parameters (channel, PAN ID, extended PAN ID, network key, device link keys) live in the coordinator NV memory, not in project files. Since 0.3.0 they can be created, saved to a file and moved to another coordinator. The commands run with the service **stopped** (the database lock prevents another instance using the same database; it does not lock other applications or configurations using another database) and exit when done.

| Flag | Action |
|---|---|
| `-form-network [-channel N]` | Creates a new network with a random network key, PAN ID and extended PAN ID. Channel 11–26, default 15 (15, 20, 25 overlap least with Wi-Fi). |
| `-backup <file>` | Saves the network to a file. Read-only for the coordinator. |
| `-restore <file>` | Writes a network backup to the coordinator; devices keep working without re-pairing. |
| `-force` | Allows replacing an existing network or a backup file of another network. |

New network: flash Z-Stack 3.x, configure, run `-form-network`, start the service and pair devices with `permit_join`. Replacement coordinator: run `-restore <file>` on the new stick; the coordinator IEEE, network key, PAN IDs, channel, device table and link keys are copied and frame counters are advanced by 2500 above the larger value from the supplied backup and readable matching coordinator/local backup. A very old backup on a new stick can still be below counters remembered by devices; use the freshest backup available. Never power the old coordinator again near the network. Zigbee2MQTT `coordinator_backup.json` files (`zigpy/open-coordinator-backup`) restore directly; the legacy `adapterType` format is not supported.

Before a restore writes to the coordinator, the exact input is kept in a private `coordinator_backup-restore-source-*.json` file. Device address/missing-entry differences are warnings, but counter rollback for a surviving unchanged key blocks completion. `-force` explicitly allows replacement when the current snapshot cannot be decoded; it cannot guarantee recovery of unknown latest counters.

The running service refreshes the backup one minute after start and daily into `coordinator_backup` (default `coordinator_backup.json` next to the config). Link-keyed devices missing from coordinator tables but still paired are carried over from the previous backup. Formation/restore refuse to replace an existing network without `-force`; with `-force` the current network is backed up first. Restore refuses a database of another coordinator unless `-force` is explicitly used. A backup of another network is never overwritten. Invalid existing backup files are left untouched by automatic backups; manual replacement preserves them under a unique name. Backup files are mode `0600` and **contain the network key**.

## Build from source

Run from the repository root, where `main.go` and `go.mod` are located:

```bash
go mod download
go build -buildvcs=false -trimpath -o dist/zigbeemqttlink .
./dist/zigbeemqttlink -check-config -config ./config.json
./dist/zigbeemqttlink -config ./config.json
```

For development: `go run -buildvcs=false . -config ./config.json`. The build option `-buildvcs=false` allows building extracted source archives without Git metadata.

| Command | Result |
|---|---|
| `make build` | Local static binary: `dist/zigbeemqttlink` |
| `make release` | Static Linux AMD64/ARM64/ARMv7 binaries and `dist/SHA256SUMS` |
| `make vet` | Go static analysis |

Cross-compilation without Make:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -buildvcs=false -trimpath -ldflags='-s -w' -o dist/zigbeemqttlink-linux-amd64 .
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -buildvcs=false -trimpath -ldflags='-s -w' -o dist/zigbeemqttlink-linux-arm64 .
```

`go build` creates the output directory. CLI flags: `-config`, `-check-config`, `-version`. Log severity is set in JSON; `-debug` and `-import-db` are absent.

## Project layout

| Path | Purpose |
|---|---|
| `main.go` | Root entry point: CLI, startup and shutdown |
| `maintenance.go` | `-form-network`, `-backup`, `-restore` commands and the daily network backup |
| `go.mod`, `go.sum` | Dependencies and checksums |
| `config.json` | Main service configuration |
| `devices.json` | Identities, endpoints, options and cached state |
| `device-definitions.json` | Model selection and device behavior |
| `internal/config` | Strict JSON configuration and MQTT/TLS settings |
| `internal/store` | Atomic database writes and administration |
| `internal/device` | Runtime device-definition registry |
| `internal/znp` | TI ZNP transport and adapter; NV memory, network formation, backup and restore |
| `internal/zcl` | ZCL parsing and specialized converters |
| `internal/bridge` | MQTT, HTTP and embedded web assets |
| `internal/logging` | JSON logging and rotation |
| `examples/*.definition.json` | Model-definition examples |
| `deploy/zigbeemqttlink.service` | systemd unit |
| `Makefile` | Build, release binaries, tests |
| `CHANGES.ru.md` | Previous review notes for 0.2.0 (Russian) |
| `REVIEW-0.3.3.ru.md` | Current audit: forced recovery, link-key counter safety, preserved restore sources (Russian) |
| `REVIEW-0.3.2.ru.md` | 0.3.2 audit: completing interrupted restores, device-table verification (Russian) |
| `REVIEW-0.3.1.ru.md` | 0.3.1 audit: restore safety, counter validation, file protection (Russian) |
| `REVIEW-0.3.0.ru.md` | Network formation, backup and restore: design, verification, acceptance (Russian) |
| `REVIEW-0.2.5.ru.md` | 0.2.5 assessment, retry provenance and timing fixes |
| `REVIEW-0.2.4.ru.md` | Previous assessment of 0.2.3, fixes and verification (Russian) |
| `REVIEW-0.2.3.ru.md` | 0.2.3 assessment, recovery fixes and verification (Russian) |
| `REVIEW-0.2.2.ru.md` | 0.2.2 audit of 0.2.1, fixes and verification (Russian) |
| `REVIEW.ru.md` | Independent 0.2.1 review, fixes and deployment acceptance (Russian) |
| `dist` | Release binaries and checksums |
| `README.md`, `README.ru.md` | English and Russian documentation |
| `LICENSE` | GPL-3.0 license text |

## Configuration

Example main configuration; adapt credentials and paths to your installation:

```json
{
  "version": 1,
  "database": "devices.json",
  "device_definitions": "device-definitions.json",
  "coordinator_backup": "coordinator_backup.json",
  "mqtt": {
    "server": "mqtt://localhost:1883",
    "base_topic": "zigbee2mqtt",
    "client_id": "zigbeemqttlink"
  },
  "serial": {"port": "/dev/ttyUSB0", "baudrate": 115200},
  "web": {"enabled": true, "listen": "0.0.0.0:8080", "user": "admin", "password": "CHANGE_ME"},
  "logging": {
    "level": "info",
    "file": "logs/zigbeemqttlink.log",
    "console": true,
    "max_size_mb": 10,
    "backups": 3
  }
}
```

| Setting | Meaning |
|---|---|
| `version` | Schema version: `1` |
| `database` | Exact path to the separate device JSON file |
| `device_definitions` | Runtime model-definition file |
| `coordinator_backup` | Optional. Automatic network backup file (default `coordinator_backup.json`); contains the network key |
| `mqtt.server` | `mqtt://HOST[:PORT]` or `mqtts://HOST[:PORT]`; `tcp`/`ssl` aliases accepted |
| `mqtt.base_topic` | Namespace used by scripts and the bridge |
| `mqtt.client_id` | Unique client ID |
| `mqtt.user`, `mqtt.password` | Optional broker credentials |
| `mqtt.ca` | Optional custom CA certificate file |
| `mqtt.cert`, `mqtt.key` | Optional client certificate/key; specify both |
| `mqtt.server_name` | Optional TLS server name override |
| `mqtt.force_disable_retain` | Disable retain on normal publications and Last Will |
| `serial.port` | UART path or `tcp://HOST:PORT` for a TI Z-Stack serial connection |
| `serial.baudrate` | UART baud rate, commonly `115200` |
| `web.enabled`, `web.listen` | HTTP enable switch and bind address/port |
| `web.user`, `web.password` | HTTP Basic authentication for the panel and API (set both); required for a network listener. `/api/health` stays open for monitoring |
| `logging.level` | `debug`, `info`, `warn`, `error` |
| `logging.file`, `logging.console` | File path and/or stderr output |
| `logging.max_size_mb`, `logging.backups` | Rotation threshold and backup count |

Relative file paths resolve against the directory of `config.json`, not the process working directory. Relative UART paths follow this rule; `tcp://` addresses remain addresses. Database, definitions, log and main configuration paths must differ.

Default MQTT ports: `1883` without TLS, `8883` with TLS. TLS verifies certificates and requires TLS 1.2 or newer. MQTT is fixed to 3.1.1. Set credentials in separate fields, not in the broker URL.

The code's default namespace is `zigbeemqttlink`; the distributed configuration explicitly selects `zigbee2mqtt` for compatibility. Unknown fields, duplicate JSON keys, unsupported versions, comments and trailing values are rejected. Legacy YAML/Home Assistant/queue/advanced settings are not silently ignored.

Main configuration changes require a restart. Definitions have a separate live-reload mechanism. PAN, channel and network key remain in the existing coordinator; this version does not form a new network from configuration values.

## Device database

| Field | Content |
|---|---|
| `version` | Database schema: `1` |
| `coordinator_ieee` | Coordinator identity for consistency checking |
| `devices` | Device records keyed by IEEE |
| `states` | Cached state keyed by IEEE |

Records contain `ieee_address`, `network_address`, `type` (`Router`/`EndDevice`), `friendly_name`, `model_id`, `manufacturer`, `endpoints`, `last_seen` and `options`. Endpoint fields: `ID`, `profileID`, `deviceID`, `inputClusters`, `outputClusters`, optional `measurement_scales`.

Empty database:

```json
{"version":1,"coordinator_ieee":"","devices":{},"states":{}}
```

NWK addresses and endpoints come from the real network or previous working database. IEEE addresses alone are insufficient; do not invent NWK addresses.

Writes use temporary files and atomic replacement. The process lock is `DATABASE_PATH.lock`. Malformed/inconsistent data is rejected without being overwritten. Edit the database manually only while stopped; use web/API administration while running.

### Calibration and replacement

Numeric `options` keys:

- `temperature_calibration`, `device_temperature_calibration`, `humidity_calibration`, `pressure_calibration`: additive correction in the reported unit.
- `illuminance_calibration`, `power_calibration`, `energy_calibration`, `voltage_calibration`, `current_calibration`: percentage correction, `value × (1 + correction / 100)`.

Pair a replacement first, select it in the old device's web panel and run replacement. The known model must match; `TS0601`/`TS011F` additionally require the same known manufacturer. The new device receives the old MQTT name and calibration options; the old record is removed. Hardware settings and old physical state are not copied. Configure and verify the replacement separately.

## Supported models

| Model/fingerprint | Handler | Main capabilities |
|---|---|---|
| `lumi.switch.b2nc01` | `aqara_e1` | Left/right relays and supported Aqara settings |
| `lumi.relay.c2acn01` | `aqara_relay` | Two channels, interlock and supported settings |
| `lumi.weather` | `aqara_weather` | Temperature, humidity, pressure, battery |
| `lumi.sensor_wleak.aq1` | `aqara_leak` | Water leak and battery |
| `lumi.remote.b186acn02` | `aqara_button` | Button `action` events |
| `TS011F` | `tuya_plug` | On/off, supported startup behavior and metering |
| `TS0601`, listed climate manufacturers | `tuya_climate` | Known manufacturer-specific climate DP |
| `TS0601` + `_TZE200_t1blo2bj`, `_TZE204_t1blo2bj`, `_TZE204_q76rtoa9` | JSON `neo_nas_ab02b2` | NEO alarm, melody, volume, duration, battery percentage |

Manufacturer lists and constraints are in `device-definitions.json`. Support is targeted, not a guarantee for every firmware sharing a model string. Unmatched devices use generic ZCL; unknown Tuya variants do not receive guessed DP writes.

## MQTT

`BASE` means `mqtt.base_topic`; `DEVICE` is a friendly name or IEEE address. Send control messages **without retain**.

| Topic | Direction | Payload |
|---|---|---|
| `BASE/DEVICE` | Service → scripts | JSON state/report |
| `BASE/DEVICE/set` | Scripts → service | JSON command or supported `ON`/`OFF`/`TOGGLE` |
| `BASE/DEVICE/get` | Scripts → service | JSON property list |
| `BASE/DEVICE/set/PROPERTY` | Scripts → service | Single JSON value or supported plain value |
| `BASE/DEVICE/get/PROPERTY` | Scripts → service | Empty payload can request the property |
| `BASE/DEVICE/CHANNEL/set` | Scripts → service | Command for a declared channel |
| `BASE/bridge/state` | Service → scripts | Online/offline state and Last Will |
| `BASE/bridge/info` | Service → scripts | Version, project and metadata |
| `BASE/bridge/devices` | Service → scripts | Device list |
| `BASE/bridge/request/REQUEST` | Scripts → service | Administrative data object |
| `BASE/bridge/response/REQUEST` | Service → scripts | `status`, `data`, optional `error`/`transaction` |

Typical commands:

```json
{"state":"OFF"}
```

```json
{"state_left":"ON","state_right":"OFF"}
```

```json
{"state_l1":"OFF","state_l2":"ON"}
```

TS011F compound command:

```json
{"state":"OFF","power_outage_memory":"restore"}
```

NEO siren:

```json
{"melody":"7","volume":"high","duration":10,"alarm":true}
```

Stop with `{"alarm":false}`. The manifest sends settings before activation and deactivation first. `/get` lists property names with placeholders: `{"state":"","power":""}`. Tuya `/get` queries all DP; responses depend on firmware and availability.

Mosquitto CLI example; supply credentials if needed:

```bash
mosquitto_sub -h BROKER_HOST -t 'zigbee2mqtt/#' -v
mosquitto_pub -h BROKER_HOST -t 'zigbee2mqtt/siren/set' -m '{"alarm":false}'
mosquitto_pub -h BROKER_HOST -t 'zigbee2mqtt/bridge/request/permit_join' -m '{"time":60}'
```

Device command errors are logged. Administrative requests receive a response topic. Transport acceptance does not confirm physical action; state changes require device reports. Action events are published without retain and excluded from persistent state. Normal state/metadata can be retained; `force_disable_retain` disables that behavior. Cleanup of obsolete retained names still uses empty retained publications.

## Web interface and HTTP API

The embedded web UI needs no separate assets or Node.js build. It offers device/state inspection, commands, calibration, rename/remove/replace, configuration, timed pairing and the definition JSON editor.

The panel uses **HTTP Basic authentication** configured by `web.user`/`web.password`. Authentication is mandatory for non-loopback listeners; unauthenticated access is allowed only on loopback. The literal example password `CHANGE_ME` is refused. Basic authentication over plain HTTP does not encrypt credentials; if the network is not trusted, put the panel behind an HTTPS reverse proxy. Main `config.json` is edited on disk. The web editor edits device definitions; device options are stored in the database. JSON/origin/host checks remain for HTTP mutations.

| Endpoint | Purpose |
|---|---|
| `GET /api/health` | MQTT/radio/pairing status and counters; 503 when unhealthy |
| `GET /api/devices` | Device records |
| `GET /api/state?id=DEVICE` | Cached state |
| `GET /api/admin` | Combined web snapshot |
| `GET /api/definitions` | Active manifest |
| `POST /api/request` | Device/administrative operation |

HTTP example:

```json
{"request":"device/set","data":{"id":"siren","payload":{"alarm":false}}}
```

```bash
curl -X POST http://SERVER_IP:8080/api/request \
  -H 'Content-Type: application/json' \
  -d '{"request":"permit_join","data":{"time":60}}'
```

| Request | Data fields |
|---|---|
| `permit_join` | `time`: integer `0..254` seconds; `0` closes pairing |
| `health_check` | Empty object |
| `device/rename` | `id` (or `from`), `to` (or `friendly_name`) |
| `device/options` | `id`, `options`: calibration object |
| `device/replace` | `id`: old device; `to`: paired replacement |
| `device/remove` | `id`, optional `force`; `true` only forgets locally |
| `device/configure` | `id` |
| `device/set`, `device/get` | `id`, `payload` |
| `definitions/reload` | Empty object |
| `definitions/save` | `definitions`: complete manifest |

MQTT sends the data object directly to `BASE/bridge/request/REQUEST`. HTTP wraps it in `{"request":"REQUEST","data":{...}}`. An optional MQTT `transaction` is echoed. HTTP bodies: up to 4 MiB; MQTT commands: up to 64 KiB. Definitions are validated before replacing the file/registry.

## Device definitions

Manifest example:

```json
{
  "version": 1,
  "devices": [
    {
      "id": "example_temperature",
      "models": ["YOUR_SENSOR_MODEL"],
      "protocol": "zcl",
      "endpoint": 1,
      "properties": {
        "temperature": {
          "type": "number", "access": "r",
          "cluster": 1026, "attribute": 0,
          "wire_type": 41, "scale": 0.01
        }
      }
    }
  ]
}
```

Each `examples/*.definition.json` is **one array element**, not a manifest. Append it to the existing `devices` array and replace placeholders with actual protocol information. Preserve current device definitions.

### Model selection

| Field | Meaning |
|---|---|
| `id`, `models` | Unique ID and required exact model strings |
| `manufacturers` | Optional exact manufacturer strings, particularly for Tuya |
| `protocol` | `builtin`, `zcl`, `tuya` |
| `builtin` | Specialized converter, for `builtin` only |
| `endpoint` | `1..240`; omitted selects the unique matching HA input endpoint |
| `channels` | Label → endpoint, e.g. `{"left":1,"right":2}` |
| `properties` | MQTT property → protocol mapping |
| `events` | ZCL cluster-specific commands → state/events |
| `command_order` | Compound write order; remaining names sorted |
| `stop_first` | Writable boolean; `false` sent first |
| `inter_command_delay_ms` | Inter-frame delay, `0..1000` |
| `configure` | Ordered configuration actions |
| `gateway_status` | Tuya gateway status response |
| `time_epoch` | Tuya requested time response: `off`, `1970`, `2000` |

Manufacturer-specific selectors outrank model-only selectors. Ambiguous selectors at the same priority are rejected. Builtin definitions accept selectors, a builtin converter and delay; protocol behavior remains in Go. Use `zcl`/`tuya` for declarative mappings.

For channels, declare corresponding properties, such as `state_left`/`state_right`, with their endpoints. Routing translates `state`/`operation_mode` to suffixed names; it does not create properties automatically.

### Properties and wire types

| Field | Meaning |
|---|---|
| `type` | `boolean`, `number`, `integer`, `enum`, `string` |
| `access` | `r`: reports/read; `w`: write; `rw`: both |
| `endpoint` | Property endpoint override |
| `cluster`, `attribute` | ZCL IDs; attribute required for reads |
| `datapoint` | Unique Tuya DP ID within this definition |
| `wire_type` | ZCL scalar/Tuya DP type |
| `scale`, `offset` | Read: `raw × scale + offset`; default scale `1` |
| `min`, `max` | Range in MQTT units |
| `signed` | Tuya VALUE interpreted as int32 |
| `numeric_string` | Accept numeric strings, e.g. `"7"` |
| `values` | Enum label → numeric value |
| `command_ids` | ZCL enum/boolean label → command without payload |
| `write_command` | ZCL command carrying the encoded scalar |
| `payload_prefix_hex`, `payload_suffix_hex` | Static bytes around that scalar |
| `manufacturer_code` | Numeric ZCL manufacturer code |

IDs/types are **decimal JSON numbers**. Hex payloads are strings without `0x` or spaces. Write conversion: `(MQTT − offset) / scale`. Integer wire types require exact in-range integers; no rounding is applied. The complete request is validated before sending. A later transport failure can still leave earlier frames delivered; writes are not hardware transactions.

| Protocol | Decimal type | Meaning |
|---|---:|---|
| ZCL | `16` | Boolean (`0x10`) |
| ZCL | `32`, `33`, `35` | uint8, uint16, uint32 |
| ZCL | `40`, `41`, `43` | int8, int16, int32 |
| ZCL | `48` | enum8 |
| ZCL | `66` | UTF-8 CHAR STRING |
| Tuya | `1` | BOOL: 1 byte |
| Tuya | `2` | VALUE: 4 bytes big-endian |
| Tuya | `3` | UTF-8 STRING |
| Tuya | `4` | ENUM: 1 byte |
| Tuya | `5` | BITMAP: 4 bytes big-endian |

ZCL OCTET strings use hex representation. Collections/structures are not scalar declarative properties. Tuya DP numbers/units must be known for the exact manufacturer; examples are not universal mappings.

### Examples, events and configuration

| Example | Behavior |
|---|---|
| [zcl-light.definition.json](examples/zcl-light.definition.json) | On/off/toggle and brightness with zero transition |
| [zcl-climate.definition.json](examples/zcl-climate.definition.json) | Standard temperature/humidity, scale `0.01` |
| [zcl-button.definition.json](examples/zcl-button.definition.json) | Cluster 6 commands → `action` |
| [tuya-sensor.definition.json](examples/tuya-sensor.definition.json) | Illustrative DP1/DP2, signed temperature and time response |

Event example:

```json
{"endpoint":1,"cluster":6,"command":2,"payload_hex":"","state":{"action":"toggle"}}
```

Omitted `payload_hex` matches any payload; `""` matches only empty payload. Events match cluster-specific commands, not attribute reports. Manufacturer code defaults to `0`; omitted event endpoint matches any endpoint. Multiple matching events apply in order; the last repeated field wins. State values must be flat scalars.

`configure` array example; these actions illustrate the format, not universal setup:

```json
[
  {"kind":"read","endpoint":1,"cluster":0,"attributes":[4,5]},
  {"kind":"write","endpoint":1,"cluster":6,"attribute":32768,"wire_type":48,"value":1},
  {"kind":"command","endpoint":1,"cluster":6,"command":1,"payload_hex":""},
  {"kind":"tuya_query","endpoint":1}
]
```

`read` requests attributes, `write` writes a scalar, `command` sends static cluster-specific bytes, `tuya_query` requests all DP. ZCL actions accept `manufacturer_code`; omitted endpoint uses the definition endpoint. Reading attributes does not configure binding or periodic reporting.

NEO maps DP13 → `alarm`, DP21 → `melody`, DP5 → `volume`, DP7 → `duration`, DP15 → `battpercentage`. Constraints and write order are editable in JSON.

### Adding a model without rebuilding

1. Identify actual model/manufacturer/endpoints and documented attributes/DP. Inspect debug logs if needed.
2. Append the definition and reload through the web or `BASE/bridge/request/definitions/reload` with `{}`.
3. For new hardware, temporarily open pairing and activate the device's pairing mode. A definition alone does not create a device or allocate NWK.
4. Wait for identification, wake a sleeping device if needed and configure it.
5. Verify real reports/commands, then close pairing.

Web save validates and atomically stores the file before activation. Failed save/reload preserves the active registry. Correct invalid manual edits before restarting: startup validates definitions too. Reload does not automatically reconfigure every existing device.

## Logging

Each line is JSON with timestamp, severity, message and operation fields.

| Level | Included detail |
|---|---|
| `debug` | Radio/ZCL hex, addresses, counters and higher levels |
| `info` | Startup/shutdown, MQTT commands, transport outcomes, warnings/errors |
| `warn` | Rejections, unsupported reports and warnings/errors |
| `error` | Errors only |

`console: true` writes to stderr; `file: ""` disables the file. At least one output must stay enabled. `max_size_mb`: `1..1024` MiB; `backups`: `0..100`.

Rotation occurs before the next full record exceeds the threshold; a single oversized record can exceed it. Backups: `zigbeemqttlink.log.1` through `.N`, `.1` newest. `backups: 0` discards the old file. Restart to change settings; prefer rotation over deleting an open log manually.

## Running with systemd

Adapt [deploy/zigbeemqttlink.service](deploy/zigbeemqttlink.service). It expects `/opt/zigbeemqttlink`, user `zigbeemqttlink` and UART access via `dialout`. Create the user and grant access to database/definitions/log directories first. Keep the binary and credentials under appropriate ownership.

Install the binary for your architecture as `/opt/zigbeemqttlink/zigbeemqttlink` (see the comment at the top of the unit). Update `ReadWritePaths` only for data paths outside `/opt/zigbeemqttlink`. Then:

```bash
sudo cp deploy/zigbeemqttlink.service /etc/systemd/system/zigbeemqttlink.service
sudo systemctl daemon-reload
sudo systemctl enable --now zigbeemqttlink.service
sudo systemctl status zigbeemqttlink.service
journalctl -u zigbeemqttlink.service -f
```

File and console/journal output can coexist. Main configuration changes require `systemctl restart zigbeemqttlink.service`; definition reload does not.

## Troubleshooting

| Symptom | Check |
|---|---|
| `devices=0` after migration | Old native JSON path/copy; supplied database is empty |
| Unknown network address | Missing record/address change; check database and wait for announce/interview |
| Startup command ignored | Retained replay; send a fresh command without retain |
| Transport accepted, device unchanged | Subsequent report, endpoint, mapping and sleeping-device availability |
| Unsupported TS0601 property | Exact manufacturer and correct DP definition |
| Definition rejected | JSON/selector/type/range error; prior registry stays active |
| Web unreachable remotely | Enable switch, bind address, service status and network port access |
| UART permission error | Device path, group membership and competing process |
| Missing info/debug logs | Current severity filter |
| Replacement unavailable | Pair compatible hardware first; model/manufacturer must match |

Health/counters distinguish received frames, converted/published reports, MQTT commands and rejections. MQTT readiness and cached state alone do not prove current radio traffic.

## Development and validation

Internal packages separate transport, parsing, persistence, model definitions, logging and MQTT/HTTP orchestration. `main.go` is the root entry point. Add routine mappings in JSON; extend Go for new protocol primitives or specialized converters.

There is no coordinator simulator; unit tests cover the ZNP transport, ZCL/Tuya parsing, the database, definitions, HTTP auth and announce/report handling (`make check`). Build, run static analysis and validate configuration:

```bash
go vet -buildvcs=false ./...
go build -buildvcs=false -trimpath -o dist/zigbeemqttlink .
./dist/zigbeemqttlink -check-config -config ./config.json
```

Protocol examples, reload/validation, persistence, MQTT/HTTP and logging were checked with separate development tools before this release. Release builds/CLI are checked after renaming. Those tools are not included. Physical radio hardware and a firmware matrix were unavailable; software checks do not imply universal hardware compatibility.

## License and provenance

Source: **GPL-3.0**, see [LICENSE](LICENSE). Third-party Go modules retain their licenses. Renaming/separating the service preserves attribution for protocol formats and device mappings.

Studied sources:

| Source | Pinned revision |
|---|---|
| [Koenkk/zigbee2mqtt](https://github.com/Koenkk/zigbee2mqtt/tree/344b6e03b43644c97154a72ca9eae17193c553a2) | `344b6e03b43644c97154a72ca9eae17193c553a2` |
| [Koenkk/zigbee-herdsman](https://github.com/Koenkk/zigbee-herdsman/tree/03495312e131c705a7cabc07fd3c403f2ddf7e42) | `03495312e131c705a7cabc07fd3c403f2ddf7e42` |
| [Koenkk/zigbee-herdsman-converters](https://github.com/Koenkk/zigbee-herdsman-converters/tree/5750b44559405203b202e0f5539dc0d6f46f1c8d) | `5750b44559405203b202e0f5539dc0d6f46f1c8d` |

This includes ZNP/AF/ZDO/ZCL formats, Lumi/Aqara binary attributes/channels, Tuya DP, NEO mappings and TS011F fingerprints/scaling. TypeScript converters are not executed at runtime.

Direct dependencies: [Eclipse Paho MQTT](https://github.com/eclipse/paho.mqtt.golang), [go.bug.st/serial](https://github.com/bugst/go-serial), [golang.org/x/sys](https://pkg.go.dev/golang.org/x/sys). Exact versions and indirect modules are recorded in `go.mod`/`go.sum`.

## Runtime behavior (0.2.1–0.2.5)

Incoming radio reports do not wait for MQTT acknowledgements or database writes. Logging remains synchronous. MQTT reports have a bounded 256-entry queue. Command intake and the scheduler backlog each hold 128 jobs; three independent device jobs execute concurrently. MQTT commands for the same IEEE execute in reception order; HTTP operations on that device cannot interleave the frames of a compound command. A queued MQTT job expires 30 seconds after reception; individual MQTT radio operations normally have a 10-second deadline. HTTP operations have a 30-second deadline. Timeouts are not rolled back and may mean that part of a compound command has already reached the device. After ambiguity, inspect a fresh report rather than retrying `TOGGLE` blindly.

The adapter permits four AF requests in flight. Protocol replies have two separate workers. At least one AF slot is available to replies when the three MQTT jobs occupy the other slots; concurrent direct HTTP requests can also consume slots. Queue saturation and publication failure are reported in logs and diagnostics. This is bounded backpressure, not guaranteed event delivery. When MQTT is disconnected, current device state continues updating locally; latest retained state is replayed on reconnect, but past button/leak events are not a durable event journal. Automation scripts must check report timestamps and provide their own delivery/timeout handling.

MQTT `SUBACK` rejection keeps readiness false and is retried. The Go module requires no YAML, Home Assistant discovery, vendor directory or database.db converter. The tests included in the revised input archive are retained and extended for regression coverage. Names may contain `/`, but path components `set`/`get` are reserved for commands; channel names cannot contain `/`.

Obsolete retained topics from rename/remove/replace are recorded in the optional `retained_cleanup` database field and cleared after reconnect before live states are replayed. Preserve the configured MQTT base topic while pending cleanup exists. The runtime performs periodic saves off the radio event loop.

Since 0.2.2: a frame from an unknown short address triggers a rate-limited `ZDO_IEEE_ADDR_REQ` (at most once per address per 2 minutes); a `ZDO_TC_DEV_IND` updates the address directly. Only already paired devices are updated this way; new devices still require `permit_join` and an announce. A device announce changes only the address and `last_seen` of an existing record, so it cannot revert a concurrent rename/options edit/interview. ZNP frames with an impossible length byte are discarded and the reader resynchronises instead of restarting the service (`znp_framing_errors`). A panic while converting one radio frame skips that frame (`radio_event_panics`) instead of stopping the service. Recovered addresses are counted in `network_addresses_recovered`.

Since 0.2.3, address recovery buffers up to 8 frames per address (256 total) and reprocesses them after confirming an already paired IEEE within 30 seconds. Newer announcements or a device removal invalidate an old lookup. Invalid length is checked before reading command bytes, preserving the next valid frame. The buffer is not durable; MQTT outages and process restarts can still lose events. See the latest audit for limits and deployment acceptance.

Since 0.2.4, a failed or late IEEE lookup keeps the buffered reports (still limited to 30 seconds each). The next frame from that address, typically sent while a sleeping device is awake, starts a new lookup after 15 seconds and both reports are replayed in order. After an applied, superseded or unpaired-IEEE result the address is not queried again for 2 minutes. A length byte of `0xFE` is treated as the start of the next frame, so a stray `0xFE` before a real frame no longer loses that frame.

Since 0.2.5, buffered frames cannot cross an address/membership generation change on retry. Lookup results require the entire address snapshot to remain unchanged; known-IEEE trust-center indications still update directly. The 15-second retry interval is measured from the previous attempt start. This conservative check may discard reports after an unrelated announce; it avoids assigning old reports to a different device. It does not guarantee event delivery across failures.
