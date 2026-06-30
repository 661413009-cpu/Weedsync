#!/usr/bin/env python3
"""
Sensor Data API
FastAPI service for querying sensor data from InfluxDB
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from influxdb_client import InfluxDBClient
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import asyncio
import os
import re
import json
import secrets
import threading
from datetime import datetime, timezone, timedelta
from cachetools import TTLCache

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

INFLUXDB_URL    = os.getenv("INFLUXDB_URL",    "http://influxdb:8086")
INFLUXDB_TOKEN  = os.getenv("INFLUXDB_TOKEN",  "VwUbP4LzvgmLFywvBtcb3AXcCzYV8GodaTTEjINHVGiygPAheul1zACig2vCNoLp8P79P9mPgkTOtEvJs6X8Pw==")
INFLUXDB_ORG    = os.getenv("INFLUXDB_ORG",    "myorg")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "esp32_sensors")

ALARMS_FILE     = "/app/alarms.json"
THRESHOLDS_FILE = "/app/thresholds.json"

ALARM_CHECK_INTERVAL = int(os.getenv("ALARM_CHECK_INTERVAL", "60"))  # วินาที

# ──────────────────────────────────────────────────────────────────────────────
# In-memory TTL Cache
# ──────────────────────────────────────────────────────────────────────────────

# maxsize=200 keys, ttl=วินาที
_cache: TTLCache = TTLCache(maxsize=200, ttl=10)
_cache_lock = threading.Lock()

def cache_get(key: str):
    with _cache_lock:
        return _cache.get(key)

def cache_set(key: str, value):
    with _cache_lock:
        _cache[key] = value

def cache_invalidate_prefix(prefix: str):
    """ลบ key ทั้งหมดที่ขึ้นต้นด้วย prefix (ใช้ตอน alarm/threshold เปลี่ยน)"""
    with _cache_lock:
        keys_to_delete = [k for k in list(_cache.keys()) if k.startswith(prefix)]
        for k in keys_to_delete:
            _cache.pop(k, None)

# ──────────────────────────────────────────────────────────────────────────────
# Thresholds / Alarms helpers (ไม่เปลี่ยน)
# ──────────────────────────────────────────────────────────────────────────────

def load_thresholds() -> dict:
    if not os.path.exists(THRESHOLDS_FILE):
        return {"measurement": {}, "device": {}}
    try:
        with open(THRESHOLDS_FILE, "r") as f:
            data = json.load(f)
        if "measurement" not in data and "device" not in data:
            return {"measurement": data, "device": {}}
        data.setdefault("measurement", {})
        data.setdefault("device", {})
        return data
    except Exception:
        return {"measurement": {}, "device": {}}

def save_thresholds(data: dict):
    with open(THRESHOLDS_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_effective_threshold(device_id: str, field: str, measurement: str, thresholds: dict) -> Optional[float]:
    val = thresholds.get("device", {}).get(device_id, {}).get(field)
    if val is not None:
        return val
    return thresholds.get("measurement", {}).get(measurement, {}).get(field)

ALL_MEASUREMENTS = ["telemetry", "soil", "mineral"]

# ──────────────────────────────────────────────────────────────────────────────
# Device Registry
# ──────────────────────────────────────────────────────────────────────────────

device_registry: Dict[str, str] = {}

# ──────────────────────────────────────────────────────────────────────────────
# Background alarm checker loop
# ──────────────────────────────────────────────────────────────────────────────

async def alarm_checker_loop():
    await asyncio.sleep(5)
    while True:
        try:
            query_api = get_query_api()
            check_and_record_alarms(query_api)
        except Exception as e:
            print(f"[alarm_checker] error: {e}")
        await asyncio.sleep(ALARM_CHECK_INTERVAL)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(alarm_checker_loop())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

# ──────────────────────────────────────────────────────────────────────────────
# App
# ──────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Sensor Data API",
    description="API สำหรับดึงข้อมูล sensor จาก InfluxDB",
    version="4.2.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────────────────────
# API key auth (required for all routes except health checks / docs)
# ──────────────────────────────────────────────────────────────────────────────

API_KEY = os.getenv("API_KEY", "changeme-set-API_KEY-env-var")
if not API_KEY or API_KEY == "changeme-set-API_KEY-env-var":
    raise RuntimeError("API_KEY env var is not set — refusing to start with no real auth key")
PUBLIC_PATHS = {"/", "/health", "/docs", "/openapi.json", "/redoc"}

@app.middleware("http")
async def require_api_key(request: Request, call_next):
    if request.url.path not in PUBLIC_PATHS:
        key = request.headers.get("x-api-key", "")
        if not secrets.compare_digest(key, API_KEY):
            return JSONResponse(status_code=401, content={"detail": "Missing or invalid X-API-Key header"})
    return await call_next(request)

def to_thai_time(t) -> str:
    if t is None:
        return None
    thai_tz = timezone(timedelta(hours=7))
    return t.astimezone(thai_tz).isoformat()

def thai_to_utc(t_str: str) -> str:
    try:
        thai_tz = timezone(timedelta(hours=7))
        dt = datetime.fromisoformat(t_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=thai_tz)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        raise HTTPException(
            status_code=400,
            detail=f"รูปแบบเวลาไม่ถูกต้อง: '{t_str}' — ต้องใช้ YYYY-MM-DDTHH:MM:SS หรือ YYYY-MM-DD"
        )

# ──────────────────────────────────────────────────────────────────────────────
# InfluxDB client
# ──────────────────────────────────────────────────────────────────────────────

_influx_client = None
_query_api = None

def get_query_api():
    global _influx_client, _query_api
    if _query_api is None:
        _influx_client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG,
        )
        _query_api = _influx_client.query_api()
    return _query_api

# ──────────────────────────────────────────────────────────────────────────────
# Alarms Helpers
# ──────────────────────────────────────────────────────────────────────────────

def load_alarms() -> list:
    if not os.path.exists(ALARMS_FILE):
        return []
    try:
        with open(ALARMS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


def save_alarms(data: list):
    with open(ALARMS_FILE, "w") as f:
        json.dump(data, f, indent=2)


def time_elapsed(triggered_at: str) -> str:
    try:
        t = datetime.fromisoformat(triggered_at.replace("Z", "+00:00"))
        diff = datetime.now(timezone.utc) - t
        days = diff.days
        hours = diff.seconds // 3600
        minutes = (diff.seconds % 3600) // 60
        if days > 0:
            return f"{days}d ago"
        elif hours > 0:
            return f"{hours}h ago"
        else:
            return f"{minutes}m ago"
    except Exception:
        return "unknown"


def check_and_record_alarms(query_api):
    alarms = load_alarms()
    custom_thresholds = load_thresholds()
    now = datetime.now(timezone.utc).isoformat()

    MAX_ONLY = {
        "telemetry": ["tvoc", "eco2"],
    }
    MIN_MAX = {
        "telemetry": ["temperature", "humidity"],
        "soil":      ["ec", "rh"],
        "mineral":   ["ec", "k", "n", "p"],
    }
    INFLUX_THRESH_MAP = {
        "temperature": "highTempThreshold",
        "humidity":    "highHumThreshold",
        "tvoc":        "highTvocThreshold",
        "eco2":        "highEco2Threshold",
    }

    alarm_index: Dict[str, Dict[str, int]] = {}
    for i, a in enumerate(alarms):
        if not a.get("acknowledged", False):
            did = a["device_id"]
            alert_type = a["alert_type"]
            if did not in alarm_index:
                alarm_index[did] = {}
            alarm_index[did][alert_type] = i

    def upsert_alarm(device_id, alert_type, value, threshold):
        idx = alarm_index.get(device_id, {}).get(alert_type)
        if idx is not None:
            alarms[idx]["value"] = value
            alarms[idx]["threshold"] = threshold
            alarms[idx]["triggered_at"] = now
        else:
            alarms.append({
                "device_id":    device_id,
                "alert_type":   alert_type,
                "value":        value,
                "threshold":    threshold,
                "triggered_at": now,
                "acknowledged": False,
            })
            if device_id not in alarm_index:
                alarm_index[device_id] = {}
            alarm_index[device_id][alert_type] = len(alarms) - 1

    def remove_alarm(device_id, alert_type):
        idx = alarm_index.get(device_id, {}).get(alert_type)
        if idx is not None and not alarms[idx].get("acknowledged", False):
            alarms[idx]["acknowledged"] = True
            del alarm_index[device_id][alert_type]

    try:
        influx_thresholds: Dict[str, Dict] = {}
        q_thresh = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: -24h)
          |> filter(fn: (r) => r._measurement == "attributes")
          |> last()
        '''
        try:
            tables = query_api.query(q_thresh, org=INFLUXDB_ORG)
            for table in tables:
                for record in table.records:
                    did = record.values.get("device_id", "unknown")
                    if did not in influx_thresholds:
                        influx_thresholds[did] = {}
                    influx_thresholds[did][record.get_field()] = record.get_value()
        except Exception:
            pass

        all_measurements = set(list(MAX_ONLY.keys()) + list(MIN_MAX.keys()))

        for measurement in all_measurements:
            q_latest = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -1h)
              |> filter(fn: (r) => r._measurement == "{measurement}")
              |> last()
            '''
            telemetry: Dict[str, Dict] = {}
            try:
                tables = query_api.query(q_latest, org=INFLUXDB_ORG)
                for table in tables:
                    for record in table.records:
                        did = record.values.get("device_id", "unknown")
                        if did not in telemetry:
                            telemetry[did] = {}
                        telemetry[did][record.get_field()] = record.get_value()
            except Exception:
                pass

            for device_id, data in telemetry.items():

                for field in MAX_ONLY.get(measurement, []):
                    value = data.get(field)
                    if value is None:
                        continue
                    threshold = get_effective_threshold(device_id, f"{field}_max", measurement, custom_thresholds)
                    if threshold is None and field in INFLUX_THRESH_MAP:
                        threshold = influx_thresholds.get(device_id, {}).get(INFLUX_THRESH_MAP[field])
                    alert_type = f"{measurement}_{field}_high"
                    if threshold is not None:
                        if value > threshold:
                            upsert_alarm(device_id, alert_type, value, threshold)
                        else:
                            remove_alarm(device_id, alert_type)

                for field in MIN_MAX.get(measurement, []):
                    value = data.get(field)
                    if value is None:
                        continue

                    threshold_max = get_effective_threshold(device_id, f"{field}_max", measurement, custom_thresholds)
                    if threshold_max is None and field in INFLUX_THRESH_MAP:
                        threshold_max = influx_thresholds.get(device_id, {}).get(INFLUX_THRESH_MAP[field])
                    alert_type_high = f"{measurement}_{field}_high"
                    if threshold_max is not None:
                        if value > threshold_max:
                            upsert_alarm(device_id, alert_type_high, value, threshold_max)
                        else:
                            remove_alarm(device_id, alert_type_high)

                    threshold_min = get_effective_threshold(device_id, f"{field}_min", measurement, custom_thresholds)
                    alert_type_low = f"{measurement}_{field}_low"
                    if threshold_min is not None:
                        if value < threshold_min:
                            upsert_alarm(device_id, alert_type_low, value, threshold_min)
                        else:
                            remove_alarm(device_id, alert_type_low)

        save_alarms(alarms)

    except Exception as e:
        print(f"Error checking alarms: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

VALID_EVERY = re.compile(r"^\d+[smhd]$")

def validate_every(every: str) -> str:
    if not VALID_EVERY.match(every):
        raise HTTPException(
            status_code=400,
            detail="every ต้องอยู่ในรูปแบบ เช่น 30s, 1m, 5m, 1h, 1d"
        )
    return every


def build_field_filter(field: Optional[str]):
    if not field:
        return "", None
    fields = list(dict.fromkeys(
        f.strip() for f in field.split(",") if f.strip()
    ))
    if not fields:
        return "", None
    if len(fields) == 1:
        flux = f'|> filter(fn: (r) => r._field == "{fields[0]}")'
    else:
        conditions = " or ".join([f'r._field == "{f}"' for f in fields])
        flux = f'|> filter(fn: (r) => {conditions})'
    return flux, fields


def parse_unpivot_records(tables, selected_fields: Optional[list] = None):
    rows: dict = {}
    for table in tables:
        for record in table.records:
            t         = to_thai_time(record.get_time()) if record.get_time() else None
            device_id = record.values.get("device_id") or record.values.get("device")
            f_name    = record.get_field()
            f_val     = record.get_value()

            if selected_fields and f_name not in selected_fields:
                continue

            key = f"{t}|{device_id}"
            if key not in rows:
                rows[key] = {
                    "time":        t,
                    "device_id":   device_id,
                    "topic":       record.values.get("topic"),
                    "measurement": record.get_measurement(),
                }
            rows[key][f_name] = f_val

    data = list(rows.values())
    if selected_fields:
        for row in data:
            for f in selected_fields:
                if f not in row:
                    row[f] = None
    data.sort(key=lambda r: r.get("time") or "")
    return data


def parse_to_friend_format(tables):
    rows: dict = {}
    for table in tables:
        for record in table.records:
            t         = to_thai_time(record.get_time()) if record.get_time() else None
            device_id = record.values.get("device_id") or record.values.get("device")
            f_name    = record.get_field()
            f_val     = record.get_value()

            key = f"{t}|{device_id}"
            if key not in rows:
                rows[key] = {
                    "device_id":   device_id,
                    "timestamp":   t,
                    "measurement": record.get_measurement(),
                    "reading":     {},
                }
            rows[key]["reading"][f_name] = f_val

    data = list(rows.values())
    data.sort(key=lambda r: r.get("timestamp") or "")
    return data


def build_measurement_filter(measurement: Optional[str]) -> str:
    if measurement and measurement in ALL_MEASUREMENTS:
        return f'|> filter(fn: (r) => r._measurement == "{measurement}")'
    conditions = " or ".join([f'r._measurement == "{m}"' for m in ALL_MEASUREMENTS])
    return f'|> filter(fn: (r) => {conditions})'


def query_history(query_api, days: int, field_filter: str, every: str,
                  device_id: Optional[str] = None, measurement: Optional[str] = None):
    device_filter      = f'|> filter(fn: (r) => r.device_id == "{device_id}")' if device_id else ""
    measurement_filter = build_measurement_filter(measurement)
    rows: dict = {}

    q_numeric = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -{days}d)
      {measurement_filter}
      {device_filter}
      {field_filter}
      |> filter(fn: (r) => r._value != "")
      |> keep(columns: ["_time", "_field", "_value", "_measurement", "device_id", "topic"])
      |> toFloat()
      |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
    '''
    q_bool = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -{days}d)
      {measurement_filter}
      {device_filter}
      {field_filter}
      |> filter(fn: (r) => r._value == true or r._value == false)
      |> keep(columns: ["_time", "_field", "_value", "_measurement", "device_id", "topic"])
      |> aggregateWindow(every: {every}, fn: last, createEmpty: false)
    '''
    q_string = f'''
    from(bucket: "{INFLUXDB_BUCKET}")
      |> range(start: -{days}d)
      {measurement_filter}
      {device_filter}
      {field_filter}
      |> filter(fn: (r) => exists r._value)
      |> keep(columns: ["_time", "_field", "_value", "_measurement", "device_id", "topic"])
      |> aggregateWindow(every: {every}, fn: last, createEmpty: false)
    '''

    def merge_tables(tables):
        now = datetime.now(timezone.utc)
        for table in tables:
            for record in table.records:
                if record.get_time() and record.get_time() > now:
                    continue
                t         = to_thai_time(record.get_time()) if record.get_time() else None
                device_id = record.values.get("device_id") or record.values.get("device")
                f_name    = record.get_field()
                f_val     = record.get_value()
                key       = f"{t}|{device_id}"
                if key not in rows:
                    rows[key] = {
                        "time":        t,
                        "device_id":   device_id,
                        "measurement": record.get_measurement(),
                        "topic":       record.values.get("topic"),
                    }
                if f_name not in rows[key]:
                    rows[key][f_name] = f_val

    try:
        merge_tables(query_api.query(q_numeric, org=INFLUXDB_ORG))
    except Exception:
        pass
    try:
        merge_tables(query_api.query(q_bool, org=INFLUXDB_ORG))
    except Exception:
        pass
    try:
        merge_tables(query_api.query(q_string, org=INFLUXDB_ORG))
    except Exception:
        pass

    data = list(rows.values())
    data.sort(key=lambda r: r.get("time") or "")
    return data

# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "message": "Sensor Data API is running"}

@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok", "message": "Sensor Data API is running"}

# ──────────────────────────────────────────────────────────────────────────────
# Devices
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/devices", tags=["Devices"])
def get_devices(
    measurement: Optional[str] = Query(default=None)
):
    cache_key = f"devices:{measurement or 'all'}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        query_api          = get_query_api()
        measurement_filter = build_measurement_filter(measurement)
        query = f'''
        import "influxdata/influxdb/schema"
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: -30s)
          {measurement_filter}
          |> keep(columns: ["device_id"])
          |> distinct(column: "device_id")
        '''
        tables  = query_api.query(query, org=INFLUXDB_ORG)
        devices = list(set([
            record.get_value()
            for table in tables
            for record in table.records
            if record.get_value()
        ]))
        devices.sort()
        cache_set(cache_key, devices)
        return devices
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/devices/registry", tags=["Devices"])
def get_registry():
    return device_registry


@app.post("/api/devices/{device_id}/register", tags=["Devices"])
def register_device(
    device_id: str,
    ip: str = Query(description="IP address ของ device")
):
    device_registry[device_id] = ip
    return {"status": "ok", "device_id": device_id, "ip": ip}


@app.get("/api/devices/all/latest", tags=["Devices"])
def get_all_devices_latest():
    # ──────────────────────────────────────────
    # CACHED: TTL 10 วินาที
    # ก่อน cache: 6 InfluxDB queries ต่อ request
    # หลัง cache: 6 queries ทุก 10 วินาที (ไม่ว่า frontend จะยิงกี่ครั้ง)
    # ──────────────────────────────────────────
    cache_key = "all_devices_latest"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        query_api = get_query_api()
        result = {}
        for m in ALL_MEASUREMENTS:
            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -1m)
              |> filter(fn: (r) => r._measurement == "{m}")
              |> last()
            '''
            try:
                tables = query_api.query(query, org=INFLUXDB_ORG)
                data   = parse_to_friend_format(tables)
                for item in data:
                    did = item["device_id"]
                    if did not in result:
                        result[did] = {"device_id": did, "timestamp": item.get("timestamp"), "reading": {}}
                    result[did]["reading"].update(item.get("reading", {}))
            except Exception:
                pass

        output = list(result.values())
        cache_set(cache_key, output)
        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/devices/all/history", tags=["Devices"])
def get_all_devices_history(
    days: int = Query(default=1, ge=1, le=90),
    every: str = Query(default="1h"),
):
    # ──────────────────────────────────────────
    # CACHED: TTL 10 วินาที (แยก key ตาม params)
    # ก่อน cache: 6 InfluxDB queries ต่อ request
    # หลัง cache: 6 queries ทุก 10 วินาที
    # ──────────────────────────────────────────
    try:
        every = validate_every(every)
    except HTTPException:
        raise

    cache_key = f"all_devices_history:{days}:{every}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        query_api = get_query_api()
        rows: dict = {}

        def merge(tables):
            now = datetime.now(timezone.utc)
            for table in tables:
                for record in table.records:
                    if record.get_time() and record.get_time() > now:
                        continue
                    t      = to_thai_time(record.get_time()) if record.get_time() else None
                    did    = record.values.get("device_id") or record.values.get("device")
                    f_name = record.get_field()
                    f_val  = record.get_value()
                    key    = f"{t}|{did}"
                    if key not in rows:
                        rows[key] = {"time": t, "device_id": did}
                    if f_name not in rows[key]:
                        rows[key][f_name] = f_val

        for m in ALL_MEASUREMENTS:
            q_numeric = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -{days}d)
              |> filter(fn: (r) => r._measurement == "{m}")
              |> toFloat()
              |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
            '''
            q_string = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -{days}d)
              |> filter(fn: (r) => r._measurement == "{m}")
              |> aggregateWindow(every: {every}, fn: last, createEmpty: false)
            '''
            try:
                merge(query_api.query(q_numeric, org=INFLUXDB_ORG))
            except Exception:
                pass
            try:
                merge(query_api.query(q_string, org=INFLUXDB_ORG))
            except Exception:
                pass

        data = sorted(rows.values(), key=lambda r: r.get("time") or "")
        output = {"days": days, "every": every, "count": len(data), "data": data}
        cache_set(cache_key, output)
        return output
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/devices/{device_id}/latest", tags=["Devices"])
def get_device_latest(device_id: str):
    # ──────────────────────────────────────────
    # CACHED: TTL 10 วินาที (แยก key ตาม device_id)
    # ──────────────────────────────────────────
    cache_key = f"device_latest:{device_id}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        query_api = get_query_api()
        result = {"device_id": device_id, "timestamp": None, "reading": {}}
        for m in ALL_MEASUREMENTS:
            query = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -24h)
              |> filter(fn: (r) => r._measurement == "{m}")
              |> filter(fn: (r) => r.device_id == "{device_id}")
              |> last()
            '''
            try:
                tables = query_api.query(query, org=INFLUXDB_ORG)
                data   = parse_to_friend_format(tables)
                if data:
                    latest = data[-1]
                    result["reading"].update(latest.get("reading", {}))
                    if not result["timestamp"]:
                        result["timestamp"] = latest.get("timestamp")
            except Exception:
                pass

        if not result["reading"]:
            raise HTTPException(status_code=404, detail=f"ไม่พบข้อมูลของ device: {device_id}")

        priority = ["n", "p", "k"]
        reading  = result["reading"]
        ordered  = {k: reading[k] for k in priority if k in reading}
        ordered.update({k: v for k, v in reading.items() if k not in priority})
        result["reading"] = ordered

        cache_set(cache_key, result)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/devices/{device_id}/thresholds_from_device", tags=["Devices"])
def get_thresholds_from_device(device_id: str):
    try:
        query_api = get_query_api()
        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: -24h)
          |> filter(fn: (r) => r._measurement == "attributes")
          |> filter(fn: (r) => r.device_id == "{device_id}")
          |> last()
        '''
        tables = query_api.query(query, org=INFLUXDB_ORG)
        data   = parse_to_friend_format(tables)
        if not data:
            raise HTTPException(status_code=404, detail=f"ไม่พบข้อมูลของ device: {device_id}")
        result     = data[-1]
        thresholds = {
            k: v for k, v in result.get("reading", {}).items()
            if "threshold" in k.lower() or "thresh" in k.lower()
        }
        return {"device_id": device_id, "thresholds": thresholds}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/devices/{device_id}/history", tags=["Devices"])
def get_device_history(
    device_id: str,
    hours: Optional[int] = Query(default=1),
    field: Optional[str] = Query(default=None),
    every: str = Query(default="1m"),
):
    # ──────────────────────────────────────────
    # CACHED: TTL 10 วินาที (แยก key ตาม params)
    # ──────────────────────────────────────────
    try:
        every = validate_every(every)
    except HTTPException:
        raise

    cache_key = f"device_history:{device_id}:{hours}:{field}:{every}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        query_api = get_query_api()
        field_filter, selected_fields = build_field_filter(field)
        rows: dict = {}

        def merge(tables):
            now = datetime.now(timezone.utc)
            for table in tables:
                for record in table.records:
                    if record.get_time() and record.get_time() > now:
                        continue
                    t      = to_thai_time(record.get_time()) if record.get_time() else None
                    did    = record.values.get("device_id") or record.values.get("device")
                    f_name = record.get_field()
                    f_val  = record.get_value()
                    key    = f"{t}|{did}"
                    if key not in rows:
                        rows[key] = {"time": t, "device_id": did}
                    if f_name not in rows[key]:
                        rows[key][f_name] = f_val

        for m in ALL_MEASUREMENTS:
            q_numeric = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -{hours}h)
              |> filter(fn: (r) => r._measurement == "{m}")
              |> filter(fn: (r) => r.device_id == "{device_id}")
              {field_filter}
              |> toFloat()
              |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
            '''
            q_string = f'''
            from(bucket: "{INFLUXDB_BUCKET}")
              |> range(start: -{hours}h)
              |> filter(fn: (r) => r._measurement == "{m}")
              |> filter(fn: (r) => r.device_id == "{device_id}")
              {field_filter}
              |> aggregateWindow(every: {every}, fn: last, createEmpty: false)
            '''
            try:
                merge(query_api.query(q_numeric, org=INFLUXDB_ORG))
            except Exception:
                pass
            try:
                merge(query_api.query(q_string, org=INFLUXDB_ORG))
            except Exception:
                pass

        data = sorted(rows.values(), key=lambda r: r.get("time") or "")
        output = {"device_id": device_id, "hours": hours, "every": every, "count": len(data), "data": data}
        cache_set(cache_key, output)
        return output
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ──────────────────────────────────────────────────────────────────────────────
# Thresholds
# ──────────────────────────────────────────────────────────────────────────────

from pydantic import BaseModel
from enum import Enum

class MeasurementEnum(str, Enum):
    telemetry = "telemetry"
    soil      = "soil"
    mineral   = "mineral"

ALL_THRESHOLD_FIELDS = [
    "temperature_max", "temperature_min",
    "humidity_max",    "humidity_min",
    "tvoc_max",        "eco2_max",
    "ec_max",          "ec_min",
    "rh_max",          "rh_min",
    "n_max",           "n_min",
    "p_max",           "p_min",
    "k_max",           "k_min",
]

def _run_alarm_check():
    try:
        check_and_record_alarms(get_query_api())
    except Exception:
        pass

# ── measurement-level ─────────────────────────────────────────────────────────

@app.get("/api/thresholds", tags=["Thresholds"])
def get_all_thresholds():
    return load_thresholds()

@app.get("/api/thresholds/measurement/{measurement}", tags=["Thresholds"])
def get_threshold_by_measurement(measurement: str):
    if measurement not in ALL_MEASUREMENTS:
        raise HTTPException(status_code=400, detail=f"measurement ต้องเป็น {ALL_MEASUREMENTS}")
    return load_thresholds().get("measurement", {}).get(measurement, {})

@app.post("/api/thresholds/measurement/{measurement}", tags=["Thresholds"])
def set_threshold_by_measurement(
    measurement: str,
    temperature_max: Optional[float] = Query(default=None),
    temperature_min: Optional[float] = Query(default=None),
    tvoc_max:        Optional[float] = Query(default=None),
    eco2_max:        Optional[float] = Query(default=None),
    humidity_max:    Optional[float] = Query(default=None),
    humidity_min:    Optional[float] = Query(default=None),
    ec_max:          Optional[float] = Query(default=None),
    ec_min:          Optional[float] = Query(default=None),
    rh_max:          Optional[float] = Query(default=None),
    rh_min:          Optional[float] = Query(default=None),
    n_max:           Optional[float] = Query(default=None),
    n_min:           Optional[float] = Query(default=None),
    p_max:           Optional[float] = Query(default=None),
    p_min:           Optional[float] = Query(default=None),
    k_max:           Optional[float] = Query(default=None),
    k_min:           Optional[float] = Query(default=None),
):
    if measurement not in ALL_MEASUREMENTS:
        raise HTTPException(status_code=400, detail=f"measurement ต้องเป็น {ALL_MEASUREMENTS}")

    thresholds = load_thresholds()
    thresholds["measurement"].setdefault(measurement, {})

    for field, value in {
        "temperature_max": temperature_max, "temperature_min": temperature_min,
        "tvoc_max": tvoc_max, "eco2_max": eco2_max,
        "humidity_max": humidity_max, "humidity_min": humidity_min,
        "ec_max": ec_max, "ec_min": ec_min,
        "rh_max": rh_max, "rh_min": rh_min,
        "n_max": n_max, "n_min": n_min,
        "p_max": p_max, "p_min": p_min,
        "k_max": k_max, "k_min": k_min,
    }.items():
        if value is not None:
            thresholds["measurement"][measurement][field] = value

    save_thresholds(thresholds)
    _run_alarm_check()
    return {"status": "ok", "measurement": measurement, "thresholds": thresholds["measurement"][measurement]}

# ── device-level ──────────────────────────────────────────────────────────────

@app.get("/api/thresholds/device/{device_id}", tags=["Thresholds"])
def get_threshold_by_device(device_id: str):
    return load_thresholds().get("device", {}).get(device_id, {})

@app.get("/api/thresholds/device/{device_id}/effective", tags=["Thresholds"])
def get_effective_thresholds(device_id: str, measurement: str = Query(description="telemetry, soil, mineral")):
    if measurement not in ALL_MEASUREMENTS:
        raise HTTPException(status_code=400, detail=f"measurement ต้องเป็น {ALL_MEASUREMENTS}")
    thresholds = load_thresholds()
    result = {}
    for field in ALL_THRESHOLD_FIELDS:
        val = get_effective_threshold(device_id, field, measurement, thresholds)
        if val is not None:
            result[field] = val
    return {"device_id": device_id, "measurement": measurement, "effective_thresholds": result}

@app.post("/api/thresholds/device/{device_id}", tags=["Thresholds"])
def set_threshold_by_device(
    device_id: str,
    temperature_max: Optional[float] = Query(default=None),
    temperature_min: Optional[float] = Query(default=None),
    tvoc_max:        Optional[float] = Query(default=None),
    eco2_max:        Optional[float] = Query(default=None),
    humidity_max:    Optional[float] = Query(default=None),
    humidity_min:    Optional[float] = Query(default=None),
    ec_max:          Optional[float] = Query(default=None),
    ec_min:          Optional[float] = Query(default=None),
    rh_max:          Optional[float] = Query(default=None),
    rh_min:          Optional[float] = Query(default=None),
    n_max:           Optional[float] = Query(default=None),
    n_min:           Optional[float] = Query(default=None),
    p_max:           Optional[float] = Query(default=None),
    p_min:           Optional[float] = Query(default=None),
    k_max:           Optional[float] = Query(default=None),
    k_min:           Optional[float] = Query(default=None),
):
    thresholds = load_thresholds()
    thresholds["device"].setdefault(device_id, {})

    for field, value in {
        "temperature_max": temperature_max, "temperature_min": temperature_min,
        "tvoc_max": tvoc_max, "eco2_max": eco2_max,
        "humidity_max": humidity_max, "humidity_min": humidity_min,
        "ec_max": ec_max, "ec_min": ec_min,
        "rh_max": rh_max, "rh_min": rh_min,
        "n_max": n_max, "n_min": n_min,
        "p_max": p_max, "p_min": p_min,
        "k_max": k_max, "k_min": k_min,
    }.items():
        if value is not None:
            thresholds["device"][device_id][field] = value

    save_thresholds(thresholds)
    _run_alarm_check()
    return {"status": "ok", "device_id": device_id, "thresholds": thresholds["device"][device_id]}

@app.delete("/api/thresholds/device/{device_id}", tags=["Thresholds"])
def delete_threshold_by_device(device_id: str):
    thresholds = load_thresholds()
    if device_id not in thresholds.get("device", {}):
        raise HTTPException(status_code=404, detail=f"ไม่พบ threshold ของ device: {device_id}")
    del thresholds["device"][device_id]
    save_thresholds(thresholds)
    _run_alarm_check()
    return {"status": "ok", "device_id": device_id, "message": "ลบ threshold แล้ว กลับไปใช้ measurement default"}

# ── batch ──────────────────────────────────────────────────────────────────────

class BatchThresholdRequest(BaseModel):
    device_ids: list[str]
    thresholds: dict[str, float]

    model_config = {
        "json_schema_extra": {
            "example": {
                "device_ids": ["device-001", "device-002"],
                "thresholds": {
                    "temperature_max": 35,
                    "temperature_min": 10,
                    "humidity_max": 80,
                    "humidity_min": 40
                }
            }
        }
    }

@app.post("/api/thresholds/device/batch", tags=["Thresholds"])
def set_threshold_batch(request: BatchThresholdRequest):
    unknown = [f for f in request.thresholds if f not in ALL_THRESHOLD_FIELDS]
    if unknown:
        raise HTTPException(status_code=400, detail=f"field ไม่รู้จัก: {unknown}")

    try:
        thresholds = load_thresholds()
        for device_id in request.device_ids:
            thresholds["device"].setdefault(device_id, {})
            for field, value in request.thresholds.items():
                thresholds["device"][device_id][field] = value
        save_thresholds(thresholds)
        _run_alarm_check()
        return {
            "status": "ok",
            "updated_devices": request.device_ids,
            "thresholds": request.thresholds,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/thresholds/{measurement}", tags=["Thresholds"])
def get_threshold_compat(measurement: MeasurementEnum):
    return load_thresholds().get("measurement", {}).get(measurement.value, {})

@app.post("/api/thresholds/{measurement}", tags=["Thresholds"])
def set_threshold_compat(
    measurement: MeasurementEnum,
    temperature_max: Optional[float] = Query(default=None),
    temperature_min: Optional[float] = Query(default=None),
    tvoc_max:        Optional[float] = Query(default=None),
    eco2_max:        Optional[float] = Query(default=None),
    humidity_max:    Optional[float] = Query(default=None),
    humidity_min:    Optional[float] = Query(default=None),
    ec_max:          Optional[float] = Query(default=None),
    ec_min:          Optional[float] = Query(default=None),
    rh_max:          Optional[float] = Query(default=None),
    rh_min:          Optional[float] = Query(default=None),
    n_max:           Optional[float] = Query(default=None),
    n_min:           Optional[float] = Query(default=None),
    p_max:           Optional[float] = Query(default=None),
    p_min:           Optional[float] = Query(default=None),
    k_max:           Optional[float] = Query(default=None),
    k_min:           Optional[float] = Query(default=None),
):
    return set_threshold_by_measurement(
        measurement.value,
        temperature_max=temperature_max, temperature_min=temperature_min,
        tvoc_max=tvoc_max, eco2_max=eco2_max,
        humidity_max=humidity_max, humidity_min=humidity_min,
        ec_max=ec_max, ec_min=ec_min,
        rh_max=rh_max, rh_min=rh_min,
        n_max=n_max, n_min=n_min,
        p_max=p_max, p_min=p_min,
        k_max=k_max, k_min=k_min,
    )

# ──────────────────────────────────────────────────────────────────────────────
# Alarms  — อ่านจาก JSON file โดยตรง (ไม่ query InfluxDB)
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/alarms/active", tags=["Alarms"])
def get_active_alarms():
    try:
        alarms = load_alarms()
        active = [
            {
                "device_id":    a["device_id"],
                "alert_type":   a["alert_type"],
                "value":        a.get("value"),
                "threshold":    a.get("threshold"),
                "triggered_at": a["triggered_at"],
                "time_elapsed": time_elapsed(a["triggered_at"]),
            }
            for a in alarms
            if not a.get("acknowledged", False)
        ]
        return active
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/alarms/active/grouped", tags=["Alarms"])
def get_active_alarms_grouped():
    try:
        alarms  = load_alarms()
        grouped: Dict[str, list] = {}
        for a in alarms:
            if a.get("acknowledged", False):
                continue
            did = a["device_id"]
            if did not in grouped:
                grouped[did] = []
            grouped[did].append({
                "alert_type":   a["alert_type"],
                "value":        a.get("value"),
                "threshold":    a.get("threshold"),
                "triggered_at": a["triggered_at"],
                "time_elapsed": time_elapsed(a["triggered_at"]),
            })
        return grouped
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/alarms/history", tags=["Alarms"])
def get_alarm_history(
    days:  Optional[int] = Query(default=None),
    hours: Optional[int] = Query(default=None),
):
    try:
        alarms = load_alarms()
        if days or hours:
            now    = datetime.now(timezone.utc)
            cutoff = now - timedelta(days=days or 0, hours=hours or 0)
            alarms = [
                a for a in alarms
                if datetime.fromisoformat(a["triggered_at"].replace("Z", "+00:00")) >= cutoff
            ]
        history = [
            {
                "device_id":    a["device_id"],
                "alert_type":   a["alert_type"],
                "triggered_at": a["triggered_at"],
                "value":        a.get("value"),
                "threshold":    a.get("threshold"),
                "acknowledged": a.get("acknowledged", False),
            }
            for a in alarms
        ]
        history.sort(key=lambda x: x["triggered_at"], reverse=True)
        return history
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/alarms/{device_id}/{alert_type}/acknowledge", tags=["Alarms"])
def acknowledge_alarm(device_id: str, alert_type: str):
    try:
        alarms = load_alarms()
        found  = False
        for a in alarms:
            if (a["device_id"] == device_id
                    and a["alert_type"] == alert_type
                    and not a.get("acknowledged", False)):
                a["acknowledged"] = True
                found = True
        if not found:
            raise HTTPException(status_code=404, detail="ไม่พบ alarm นี้")
        save_alarms(alarms)
        return {"status": "ok", "device_id": device_id, "alert_type": alert_type}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/alarms/acknowledge-all", tags=["Alarms"])
def acknowledge_all_alarms():
    try:
        alarms = load_alarms()
        count  = sum(1 for a in alarms if not a.get("acknowledged", False))
        for a in alarms:
            a["acknowledged"] = True
        save_alarms(alarms)
        return {"status": "ok", "acknowledged_count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ──────────────────────────────────────────────────────────────────────────────
# Sensors
# ──────────────────────────────────────────────────────────────────────────────

@app.get("/api/sensors/latest", tags=["Sensors"])
def get_latest(
    measurement: Optional[str] = Query(default=None)
):
    cache_key = f"sensors_latest:{measurement or 'all'}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        query_api          = get_query_api()
        measurement_filter = build_measurement_filter(measurement)
        query = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: -30s)
          {measurement_filter}
          |> last()
        '''
        tables = query_api.query(query, org=INFLUXDB_ORG)
        data   = parse_unpivot_records(tables)
        if not data:
            raise HTTPException(status_code=404, detail="ไม่พบข้อมูล")
        cache_set(cache_key, data)
        return data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sensors/history", tags=["Sensors"])
def get_history(
    days:        int           = Query(default=90, ge=1, le=90),
    field:       Optional[str] = Query(default=None),
    every:       str           = Query(default="1h"),
    device_id:   Optional[str] = Query(default=None),
    measurement: Optional[str] = Query(default=None),
):
    try:
        every = validate_every(every)
    except HTTPException:
        raise

    cache_key = f"sensors_history:{days}:{field}:{every}:{device_id}:{measurement}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        query_api = get_query_api()
        field_filter, selected_fields = build_field_filter(field)
        data = query_history(query_api, days, field_filter, every, device_id, measurement)
        if selected_fields:
            for row in data:
                for f in selected_fields:
                    if f not in row:
                        row[f] = None
        output = {
            "days": days, "every": every, "field": field or "all",
            "device_id": device_id or "all", "measurement": measurement or "all",
            "count": len(data), "data": data,
        }
        cache_set(cache_key, output)
        return output
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sensors/range", tags=["Sensors"])
def get_range(
    start:       str           = Query(description="เวลาเริ่มต้น (เวลาไทย)"),
    end:         Optional[str] = Query(default=None),
    every:       str           = Query(default="1m"),
    device_id:   Optional[str] = Query(default=None),
    measurement: Optional[str] = Query(default=None),
):
    # ──────────────────────────────────────────
    # CACHED: TTL 60 วินาที (ข้อมูล historical range เปลี่ยนน้อย)
    # ──────────────────────────────────────────
    try:
        import urllib.parse
        every    = validate_every(every)
        now_thai = datetime.now(timezone(timedelta(hours=7))).strftime("%Y-%m-%dT%H:%M:%S+07:00")
        end_time = end if end else now_thai
        start_utc = thai_to_utc(urllib.parse.unquote(start))
        end_utc   = thai_to_utc(urllib.parse.unquote(end_time))

        cache_key = f"sensors_range:{start_utc}:{end_utc}:{every}:{device_id}:{measurement}"
        cached = cache_get(cache_key)
        if cached is not None:
            return cached

        device_filter      = f'|> filter(fn: (r) => r.device_id == "{device_id}")' if device_id else ""
        measurement_filter = build_measurement_filter(measurement)
        query_api          = get_query_api()
        rows: dict         = {}

        def merge(tables):
            now = datetime.now(timezone.utc)
            for table in tables:
                for record in table.records:
                    if record.get_time() and record.get_time() > now:
                        continue
                    t      = to_thai_time(record.get_time()) if record.get_time() else None
                    did    = record.values.get("device_id") or record.values.get("device")
                    f_name = record.get_field()
                    f_val  = record.get_value()
                    key    = f"{t}|{did}"
                    if key not in rows:
                        rows[key] = {"time": t, "device_id": did}
                    if f_name not in rows[key]:
                        rows[key][f_name] = f_val

        q_numeric = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: {start_utc}, stop: {end_utc})
          {measurement_filter}
          {device_filter}
          |> toFloat()
          |> aggregateWindow(every: {every}, fn: mean, createEmpty: false)
        '''
        q_string = f'''
        from(bucket: "{INFLUXDB_BUCKET}")
          |> range(start: {start_utc}, stop: {end_utc})
          {measurement_filter}
          {device_filter}
          |> aggregateWindow(every: {every}, fn: last, createEmpty: false)
        '''
        try:
            merge(query_api.query(q_numeric, org=INFLUXDB_ORG))
        except Exception:
            pass
        try:
            merge(query_api.query(q_string, org=INFLUXDB_ORG))
        except Exception:
            pass

        data = sorted(rows.values(), key=lambda r: r.get("time") or "")
        output = {
            "start": start_utc, "end": end_utc, "every": every,
            "device_id": device_id or "all", "measurement": measurement or "all",
            "count": len(data), "data": data,
        }
        cache_set(cache_key, output)
        return output
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/sensors/fields", tags=["Sensors"])
def get_fields(
    measurement: Optional[str] = Query(default="telemetry")
):
    try:
        m         = measurement if measurement in ALL_MEASUREMENTS else "telemetry"
        query_api = get_query_api()
        query = f'''
        import "influxdata/influxdb/schema"
        schema.measurementFieldKeys(
          bucket: "{INFLUXDB_BUCKET}",
          measurement: "{m}",
        )
        '''
        tables = query_api.query(query, org=INFLUXDB_ORG)
        fields = [record.get_value() for table in tables for record in table.records]
        return {"measurement": m, "fields": fields}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))