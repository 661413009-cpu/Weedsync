# WeedSync

![Python](https://img.shields.io/badge/Python-3-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![InfluxDB](https://img.shields.io/badge/InfluxDB-2.7-22ADF6?logo=influxdb&logoColor=white)
![MQTT](https://img.shields.io/badge/MQTT-Mosquitto-3C5280?logo=eclipsemosquitto&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Nginx](https://img.shields.io/badge/Nginx-Alpine-009639?logo=nginx&logoColor=white)
![PHP](https://img.shields.io/badge/PHP-777BB4?logo=php&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white)

WeedSync is an IoT pipeline for monitoring an indoor growing/cultivation environment. ESP32-based sensor nodes publish environmental, soil, and mineral (NPK) readings over MQTT; a bridge service persists them to InfluxDB; and a FastAPI service exposes that data as a REST API, including a threshold-based alarm engine.

This repository currently contains the three core application files:

| File | Role |
|---|---|
| [main.py](main.py) | "Sensor Data API" — FastAPI service that queries InfluxDB, manages thresholds/alarms, and serves REST endpoints. |
| [mqtt_to_influx.py](mqtt_to_influx.py) | MQTT → InfluxDB bridge. Subscribes to sensor topics and writes readings into InfluxDB v2. |
| [docker-compose.yml](docker-compose.yml) | Compose stack wiring together Mosquitto, InfluxDB, the bridge, the API, and a second, unrelated "Boss Farm" stack (Nginx + PHP + MySQL). |

> **Note on repo completeness:** `docker-compose.yml` references a `Dockerfile` (for `mqtt_to_influx`), `Dockerfile_api` (for `sensor_api`), `Dockerfile_php`, plus `mosquitto/config`, `nginx.conf`, `web/`, `api/`, and `db/` directories. None of these are present in the repository at the time of writing, and there is no `requirements.txt`. See [Known Gaps](#known-gaps--limitations) before trying to `docker compose up`.

## Table of Contents

- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Data Model](#data-model)
- [Components](#components)
- [REST API Reference](#rest-api-reference)
- [Alarm Engine](#alarm-engine)
- [Caching](#caching)
- [Configuration](#configuration)
- [Running the Stack](#running-the-stack)
- [Project Structure](#project-structure)
- [Security Notes](#security-notes)
- [Known Gaps / Limitations](#known-gaps--limitations)

## Architecture

```
┌────────────────────────┐
│      ESP32 sensors     │
└────────────┬───────────┘
             │  MQTT publish (JSON)
             ▼
┌────────────────────────┐
│  Mosquitto MQTT broker │
│          :1883         │
└────────────┬───────────┘
             │  subscribe
             ▼
┌────────────────────────┐
│    mqtt_to_influx.py   │
│   (ingestion bridge)   │
└────────────┬───────────┘
             │  write (line protocol)
             ▼
┌────────────────────────┐
│       InfluxDB v2      │
│  bucket: esp32_sensors │
│          :8086         │
└────────────┬───────────┘
             │  Flux query
             ▼
┌────────────────────────┐
│   main.py — Sensor API │
│        (FastAPI)       │
│          :5000         │
└────────────┬───────────┘
             │  REST / JSON
             ▼
┌────────────────────────┐
│   Dashboards / clients │
└────────────────────────┘
```

`main.py` also runs a background alarm-checker loop that reads/writes `thresholds.json` and `alarms.json` on local disk — see [Alarm Engine](#alarm-engine).

A second, independent stack ("Boss Farm": Nginx + PHP + MySQL) is declared in the same `docker-compose.yml` but shares no code or data path with the sensor pipeline above — see [Components](#components).

## Data Flow

1. **Publish** — Each ESP32 node publishes a JSON payload to an MQTT topic of the form `sensors/{device_id}/{telemetry|soil|mineral|attributes}` on the Mosquitto broker.
2. **Ingest** — `mqtt_to_influx.py` is subscribed to all four topic patterns. On every message it:
   - extracts `device_id` from the topic path (falling back to a `device_id` field in the payload),
   - extracts/parses a timestamp (`timestamp` or `time` field, ISO-8601; falls back to "now" in UTC),
   - maps the topic to an InfluxDB measurement (`telemetry`, `soil`, `mineral`, or `attributes`),
   - builds an InfluxDB `Point` tagged with `device_id` and `topic`, with one field per sensor reading,
   - writes it synchronously to the `esp32_sensors` bucket.
3. **Store** — InfluxDB retains all readings as time series, partitioned by measurement and tagged by `device_id`/`topic`.
4. **Serve** — `main.py` (the Sensor Data API) translates incoming HTTP requests into Flux queries against InfluxDB, merges/aggregates results, converts timestamps to Thai local time (UTC+7) for display, and returns JSON.
5. **Alarm check (background loop)** — Independently of any HTTP request, an `asyncio` task inside the API (`alarm_checker_loop`) wakes up every `ALARM_CHECK_INTERVAL` seconds (default 60s), pulls the latest reading per device/measurement plus any device-reported thresholds (`attributes` measurement) and user-defined thresholds (`thresholds.json`), and upserts/clears entries in `alarms.json` accordingly.
6. **Consume** — Dashboards/clients poll the REST API for live readings, history, thresholds, and active alarms. Responses for read-heavy endpoints are served from a short-lived in-memory cache to absorb polling load (see [Caching](#caching)).

## Data Model

### MQTT topics → InfluxDB measurements

| MQTT topic | InfluxDB measurement | Typical fields |
|---|---|---|
| `sensors/{device_id}/telemetry` | `telemetry` | `temperature`, `humidity`, `tvoc`, `eco2` |
| `sensors/{device_id}/soil` | `soil` | `ec`, `rh` |
| `sensors/{device_id}/mineral` | `mineral` | `ec`, `n`, `p`, `k` |
| `sensors/{device_id}/attributes` | `attributes` | device-reported config, e.g. `highTempThreshold`, `highHumThreshold`, `highTvocThreshold`, `highEco2Threshold` |

Every point is tagged with `device_id` and `topic`.

### Accepted payload shapes

The bridge accepts either a nested or a flat payload. Keys `device_id`, `timestamp`, `time`, `mac`, `ip`, `reading` are never written as fields.

```json
// "new" nested format
{
  "device_id": "device-001",
  "timestamp": "2026-06-20T10:15:30Z",
  "reading": { "temperature": 26.4, "humidity": 58.2, "tvoc": 120, "eco2": 410 }
}
```

```json
// "old" flat format — still supported
{
  "device_id": "device-001",
  "temperature": 26.4,
  "humidity": 58.2
}
```

Values are written as bool/int/float as-is; strings are coerced to float when possible, otherwise stored as strings.

### `thresholds.json` (mounted at `/app/thresholds.json`)

Two override levels — **device-level always wins over measurement-level**:

```json
{
  "measurement": {
    "telemetry": { "temperature_max": 35, "temperature_min": 10, "humidity_max": 80 },
    "soil":      { "ec_max": 2.5, "ec_min": 0.5 },
    "mineral":   { "n_max": 150, "n_min": 50 }
  },
  "device": {
    "device-001": { "temperature_max": 32 }
  }
}
```

Recognized fields (`ALL_THRESHOLD_FIELDS` in [main.py](main.py)): `temperature_max/min`, `humidity_max/min`, `tvoc_max`, `eco2_max`, `ec_max/min`, `rh_max/min`, `n_max/min`, `p_max/min`, `k_max/min`.

### `alarms.json` (mounted at `/app/alarms.json`)

A flat list of alarm records, both active and acknowledged:

```json
[
  {
    "device_id": "device-001",
    "alert_type": "telemetry_temperature_high",
    "value": 36.2,
    "threshold": 35,
    "triggered_at": "2026-06-20T03:15:30+00:00",
    "acknowledged": false
  }
]
```

`alert_type` follows the pattern `{measurement}_{field}_{high|low}`, e.g. `soil_ec_low`, `mineral_n_high`.

## Components

### Mosquitto (MQTT broker)
Stock `eclipse-mosquitto:2` image, ports `1883` (MQTT) and `9001` (WebSocket). Config/data/log directories are bind-mounted from `./mosquitto/*` (not included in this repo).

### `mqtt_to_influx.py` — ingestion bridge
A long-running `paho-mqtt` client (`loop_forever`) that subscribes to `sensors/+/telemetry`, `sensors/+/attributes`, `sensors/+/soil`, `sensors/+/mineral` and writes every message to InfluxDB synchronously. Handles both `paho-mqtt` v1.x and v2.x callback APIs. Logs connect/disconnect/write events to stdout.

### InfluxDB v2
Stores all sensor time series in the `esp32_sensors` bucket under org `myorg`. Initialized via Compose env vars on first boot (`DOCKER_INFLUXDB_INIT_*`).

### `main.py` — Sensor Data API (FastAPI, v4.2.0)
- Queries InfluxDB via Flux for live/historical data.
- Maintains an in-memory device registry (`POST /api/devices/{id}/register`) — **not persisted**, reset on restart.
- Manages threshold configuration and alarm state via two local JSON files.
- Runs the background alarm-checker loop for the lifetime of the app (`lifespan` context manager).
- Wide-open CORS (`allow_origins=["*"]`) for easy frontend integration.

### "Boss Farm" stack (Nginx + PHP + MySQL)
Declared in the same `docker-compose.yml` (`nginx`, `php`, `mysql` services) but is a separate application — a PHP API (`farmapi`) backed by MySQL, served behind Nginx on port `8888`/`8080`. It does not share data or code with the sensor pipeline. Its sources (`./web`, `./api`, `./db`, `Dockerfile_php`, `nginx.conf`) are not present in this repository.

## REST API Reference

Base path assumptions: service listens on port `5000` (per Compose); all responses are JSON.

### Health

| Method | Path | Description |
|---|---|---|
| GET | `/` | Liveness check |
| GET | `/health` | Liveness check |

### Devices

| Method | Path | Query params | Description |
|---|---|---|---|
| GET | `/api/devices` | `measurement` (optional) | Distinct `device_id`s seen in the last 30s, optionally filtered by measurement. Cached 10s. |
| GET | `/api/devices/registry` | — | In-memory map of `device_id` → last-registered IP. |
| POST | `/api/devices/{device_id}/register` | `ip` (required) | Registers/updates a device's IP in memory. |
| GET | `/api/devices/all/latest` | — | Latest reading per device, merged across all measurements. Cached 10s. |
| GET | `/api/devices/all/history` | `days` (1–90, default 1), `every` | History merged across all devices/measurements. Cached 10s. |
| GET | `/api/devices/{device_id}/latest` | — | Latest reading for one device across all measurements; `n`/`p`/`k` ordered first. 404 if no data. Cached 10s. |
| GET | `/api/devices/{device_id}/thresholds_from_device` | — | Device-reported threshold fields read back from the `attributes` measurement. |
| GET | `/api/devices/{device_id}/history` | `hours` (default 1), `field`, `every` (default `1m`) | History for one device, optionally limited to specific fields. Cached 10s. |

### Thresholds

| Method | Path | Notes |
|---|---|---|
| GET | `/api/thresholds` | Full contents of `thresholds.json` (`measurement` + `device`). |
| GET | `/api/thresholds/measurement/{measurement}` | `measurement` ∈ `telemetry`, `soil`, `mineral`. |
| POST | `/api/thresholds/measurement/{measurement}` | Query params: any subset of the 16 threshold fields. Triggers an immediate alarm re-check. |
| GET | `/api/thresholds/device/{device_id}` | Device-level overrides only. |
| GET | `/api/thresholds/device/{device_id}/effective` | `measurement` (required). Resolves device override → measurement default. |
| POST | `/api/thresholds/device/{device_id}` | Sets device-level overrides. Triggers re-check. |
| DELETE | `/api/thresholds/device/{device_id}` | Clears device-level overrides (falls back to measurement defaults). Triggers re-check. |
| POST | `/api/thresholds/device/batch` | JSON body `{ "device_ids": [...], "thresholds": {...} }`. Applies the same thresholds to multiple devices. |
| GET / POST | `/api/thresholds/{measurement}` | Legacy/compat alias of the `measurement/{measurement}` endpoints (enum-validated). |

### Alarms

Alarms are read straight from `alarms.json` — these endpoints never query InfluxDB.

| Method | Path | Query params | Description |
|---|---|---|---|
| GET | `/api/alarms/active` | — | All unacknowledged alarms, with a human-readable `time_elapsed`. |
| GET | `/api/alarms/active/grouped` | — | Same, grouped by `device_id`. |
| GET | `/api/alarms/history` | `days`, `hours` (optional) | Full alarm log (acknowledged + active), optionally time-bounded, newest first. |
| POST | `/api/alarms/{device_id}/{alert_type}/acknowledge` | — | Acknowledges the matching active alarm(s). 404 if none found. |
| POST | `/api/alarms/acknowledge-all` | — | Acknowledges every active alarm. |

### Sensors

| Method | Path | Query params | Description |
|---|---|---|---|
| GET | `/api/sensors/latest` | `measurement` (optional) | Raw last-30s readings across all devices, unpivoted to one row per `time`+`device`. Cached 10s. |
| GET | `/api/sensors/history` | `days` (1–90, default 90), `field`, `every` (default `1h`), `device_id`, `measurement` | Aggregated history (mean for numeric fields, last-value for bool/string). Cached 10s. |
| GET | `/api/sensors/range` | `start` (required, Thai local time), `end`, `every` (default `1m`), `device_id`, `measurement` | Aggregated history for an explicit time window. Cached 60s. |
| GET | `/api/sensors/fields` | `measurement` (default `telemetry`) | Lists known field keys for a measurement via Influx schema introspection. |

`every` must match `^\d+[smhd]$` (e.g. `30s`, `5m`, `1h`, `1d`); invalid values return `400`.

## Alarm Engine

Runs in `check_and_record_alarms`, invoked both by the background loop and immediately after any threshold write.

**Monitored fields:**
- High-only: `telemetry.tvoc`, `telemetry.eco2`
- High + low: `telemetry.temperature`, `telemetry.humidity`, `soil.ec`, `soil.rh`, `mineral.ec`, `mineral.k`, `mineral.n`, `mineral.p`

**Threshold resolution order** (first match wins):
1. Device-level override in `thresholds.json`
2. Measurement-level default in `thresholds.json`
3. *(high thresholds for `temperature`/`humidity`/`tvoc`/`eco2` only)* the device's own last-reported value in the `attributes` measurement (`highTempThreshold`, `highHumThreshold`, `highTvocThreshold`, `highEco2Threshold`)
4. If nothing resolves, that field/device is skipped — no alarm, no error.

Each check cycle pulls the latest reading per device/measurement (last 1h window) and the latest `attributes` per device (last 24h window), compares against resolved thresholds, then **upserts** an alarm (refreshing `value`/`threshold`/`triggered_at`) if breached, or **clears/acknowledges** it if back within range. State is persisted to `alarms.json` after each cycle.

## Caching

A single process-wide `cachetools.TTLCache` (`maxsize=200`) backs most read endpoints, keyed by endpoint name + request params:

- 10s TTL: `/api/devices*`, `/api/sensors/latest`, `/api/sensors/history`
- 60s TTL: `/api/sensors/range`

This exists to collapse N concurrent frontend polls into a single set of InfluxDB queries per TTL window. Threshold mutation endpoints currently do **not** invalidate the cache directly (a `cache_invalidate_prefix` helper exists for this but isn't yet wired into the threshold endpoints), so stale reads can persist for up to the relevant TTL after a threshold change.

## Configuration

`main.py` reads these from the environment (with defaults baked in for local/dev use):

| Variable | Default | Purpose |
|---|---|---|
| `INFLUXDB_URL` | `http://influxdb:8086` | InfluxDB connection URL |
| `INFLUXDB_TOKEN` | *(hardcoded fallback — see [Security Notes](#security-notes))* | InfluxDB auth token |
| `INFLUXDB_ORG` | `myorg` | InfluxDB org |
| `INFLUXDB_BUCKET` | `esp32_sensors` | InfluxDB bucket |
| `ALARM_CHECK_INTERVAL` | `60` (seconds) | Background alarm-loop interval |

`mqtt_to_influx.py` does **not** currently read from the environment — `MQTT_BROKER`, `MQTT_PORT`, `MQTT_USER`/`MQTT_PASS`, and all `INFLUXDB_*` values are hardcoded constants at the top of the file and must be edited in source to change.

Local file paths (not configurable): `ALARMS_FILE = /app/alarms.json`, `THRESHOLDS_FILE = /app/thresholds.json`.

## Running the Stack

```bash
docker compose up -d --build
```

This brings up `mosquitto`, `influxdb`, `mqtt_to_influx`, `sensor_api`, plus the unrelated `nginx`/`php`/`mysql` services. **As shipped in this repo, the build will fail** until the missing pieces below are supplied — see [Known Gaps](#known-gaps--limitations).

For local development of the API alone, without Docker:

```bash
pip install fastapi uvicorn "influxdb-client[ciso]" cachetools pydantic
uvicorn main:app --host 0.0.0.0 --port 5000 --reload
```

(`main.py` was written to run inside `/app` in its container — outside Docker, set `ALARMS_FILE`/`THRESHOLDS_FILE`-equivalent paths by running from a directory where `alarms.json`/`thresholds.json` can be created, or pre-create empty ones at `/app/...` if your filesystem allows it.)

For the bridge:

```bash
pip install paho-mqtt "influxdb-client[ciso]"
python mqtt_to_influx.py
```

## Project Structure

```
.
├── docker-compose.yml   # full stack definition (sensor pipeline + unrelated Boss Farm stack)
├── main.py              # Sensor Data API (FastAPI) — queries InfluxDB, manages thresholds/alarms
└── mqtt_to_influx.py    # MQTT -> InfluxDB ingestion bridge
```

## Security Notes

- **Hardcoded secrets in source**: both `main.py` and `mqtt_to_influx.py` contain a literal InfluxDB admin token, and `docker-compose.yml` contains literal InfluxDB/MySQL admin credentials. Since these are committed to git history, treat them as already compromised — rotate them and move all secrets to environment variables / an untracked `.env` file / a secrets manager before any real deployment.
- **CORS is fully open** (`allow_origins=["*"]`, all methods/headers) on the Sensor Data API — fine for local development, but should be restricted before exposing the API publicly.
- **No authentication** on any REST endpoint, including state-mutating ones (threshold writes, alarm acknowledgement, device registration).
- **MQTT has no TLS/auth configured** by default (`mqtt_client.tls_set()` is present but commented out in `mqtt_to_influx.py`; `MQTT_USER`/`MQTT_PASS` are empty strings).

## Known Gaps / Limitations

- `docker-compose.yml` references `Dockerfile`, `Dockerfile_api`, `Dockerfile_php`, `mosquitto/config`, `nginx.conf`, `web/`, `api/`, and `db/`, none of which exist in this repository yet — the Compose build will fail as-is.
- No `requirements.txt` / dependency manifest is included for either Python service.
- The device registry (`/api/devices/registry`) is in-memory only and resets on every API restart.
- Cache invalidation on threshold changes is not wired up (`cache_invalidate_prefix` exists but is unused), so reads can lag a threshold change by up to the endpoint's TTL.
- `mqtt_to_influx.py` configuration is hardcoded rather than environment-driven, unlike `main.py`.
