#!/usr/bin/env python3
"""
MQTT to InfluxDB Bridge
Subscribes to MQTT topics and writes sensor data to InfluxDB v2
"""

import json
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import logging
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────

# MQTT Configuration
MQTT_BROKER = "weedsync.local"  # ใช้ชื่อ service ใน Docker Compose
MQTT_PORT   = 1883
MQTT_TOPICS = [
    "sensors/+/telemetry",
    "sensors/+/attributes",
    "sensors/+/soil",
    "sensors/+/mineral"
]
MQTT_USER = ""
MQTT_PASS = ""

# InfluxDB Configuration
INFLUXDB_URL    = "http://influxdb:8086"
INFLUXDB_TOKEN  = "VwUbP4LzvgmLFywvBtcb3AXcCzYV8GodaTTEjINHVGiygPAheul1zACig2vCNoLp8P79P9mPgkTOtEvJs6X8Pw=="
INFLUXDB_ORG    = "myorg"
INFLUXDB_BUCKET = "esp32_sensors"

# ──────────────────────────────────────────────────────────────────────────────
# Global objects
# ──────────────────────────────────────────────────────────────────────────────

mqtt_client   = None
influx_client = None
write_api     = None

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def extract_device_id(topic: str, data: dict) -> str:
    """ดึง device_id จาก topic format: sensors/{device_id}/telemetry"""
    parts = topic.split("/")
    if len(parts) >= 3:
        return parts[1]
    return data.get("device_id", "unknown")


def parse_timestamp(data: dict) -> datetime:
    """
    ดึง timestamp จาก payload
    รองรับทั้ง 'timestamp' และ 'time'
    ถ้าไม่มีหรือ parse ไม่ได้ใช้เวลาปัจจุบัน
    """
    ts = data.get("timestamp") or data.get("time")
    if ts:
        try:
            ts = str(ts).replace("Z", "+00:00")
            return datetime.fromisoformat(ts)
        except Exception:
            pass
    return datetime.now(timezone.utc)


def write_fields(point: Point, fields: dict):
    """
    เขียน fields ลง InfluxDB point
    รองรับ bool, int, float, string
    ข้าม keys ที่ไม่ใช่ sensor data
    """
    skip_keys = {"device_id", "timestamp", "time", "mac", "ip", "reading"}

    for key, value in fields.items():
        if key in skip_keys:
            continue
        if isinstance(value, bool):
            point.field(key, value)
        elif isinstance(value, (int, float)):
            point.field(key, value)
        elif isinstance(value, str):
            try:
                point.field(key, float(value))
            except (ValueError, TypeError):
                point.field(key, value)

# ──────────────────────────────────────────────────────────────────────────────
# MQTT Callbacks
# ──────────────────────────────────────────────────────────────────────────────

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("✅ Connected to MQTT broker")
        for topic in MQTT_TOPICS:
            client.subscribe(topic)
            logger.info(f"Subscribed to: {topic}")
    else:
        logger.error(f"❌ Failed to connect to MQTT broker (rc={rc})")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning(f"⚠️ Unexpected disconnection (rc={rc})")
    else:
        logger.info("Disconnected from MQTT broker")

def on_message(client, userdata, message):
    try:
        topic   = message.topic
        payload = message.payload.decode('utf-8')
        data    = json.loads(payload)
        logger.info(f"Received from {topic}: {data}")
        write_to_influxdb(topic, data)
    except json.JSONDecodeError as e:
        logger.error(f"❌ Failed to parse JSON: {e}")
    except Exception as e:
        logger.error(f"❌ Error processing message: {e}")

def on_subscribe(client, userdata, mid, granted_qos):
    logger.info(f"Subscribe acknowledged with QoS: {granted_qos}")

# ──────────────────────────────────────────────────────────────────────────────
# InfluxDB Functions
# ──────────────────────────────────────────────────────────────────────────────

def write_to_influxdb(topic, data):
    try:
        # Measurement จาก topic
        measurement = "sensor_data"
        if "telemetry" in topic:
            measurement = "telemetry"
        elif "attributes" in topic:
            measurement = "attributes"
        elif "soil" in topic:
            measurement = "soil"
        elif "mineral" in topic:
            measurement = "mineral"

        # ดึง device_id และ timestamp
        device_id = extract_device_id(topic, data)
        timestamp = parse_timestamp(data)

        # Create InfluxDB point
        point = Point(measurement)
        point.tag("topic",     topic)
        point.tag("device_id", device_id)
        point.time(timestamp)

        # รองรับทั้ง format แบบ nested reading และ flat
        if "reading" in data and isinstance(data["reading"], dict):
            # format ใหม่: { device_id, timestamp, reading: {...} }
            write_fields(point, data["reading"])
        else:
            # format เก่า: { device_id, temperature, humidity, ... }
            write_fields(point, data)

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        logger.info(f"✅ Written to InfluxDB: {measurement} | device_id: {device_id}")

    except Exception as e:
        logger.error(f"❌ Failed to write to InfluxDB: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# Main Functions
# ──────────────────────────────────────────────────────────────────────────────

def init_mqtt():
    global mqtt_client

    # 🚨 แก้ไขตรงนี้: ป้องกันสคริปต์พังจาก Paho-MQTT เวอร์ชันใหม่ (v2.x)
    try:
        import paho.mqtt as mqtt_v2
        # ถ้าเป็น v2.x บังคับให้ใช้ API Version 1 เพื่อให้เข้ากับ Callback เดิมของคุณ
        mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION1)
    except AttributeError:
        # ถ้าเป็น v1.x แบบเก่า ให้สร้างแบบเดิม
        mqtt_client = mqtt.Client()
    
    mqtt_client.on_connect    = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message    = on_message
    mqtt_client.on_subscribe  = on_subscribe

    if MQTT_USER and MQTT_PASS:
        mqtt_client.username_pw_set(MQTT_USER, MQTT_PASS)
        logger.info(f"MQTT authentication enabled for user: {MQTT_USER}")

    # 🚨 เปิดใช้งาน TLS สำหรับต่อ HiveMQ Cloud
    #mqtt_client.tls_set() 

    try:
        logger.info(f"Connecting to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}...")
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        logger.error(f"❌ Failed to connect to MQTT broker: {e}")
        raise
    
def init_influxdb():
    global influx_client, write_api

    try:
        logger.info(f"Connecting to InfluxDB at {INFLUXDB_URL}...")
        influx_client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG
        )
        write_api = influx_client.write_api(write_options=SYNCHRONOUS)

        health = influx_client.health()
        if health.status == "pass":
            logger.info("✅ Connected to InfluxDB")
        else:
            logger.warning(f"⚠️ InfluxDB health: {health.status}")

    except Exception as e:
        logger.error(f"❌ Failed to connect to InfluxDB: {e}")
        raise

def main():
    logger.info("=" * 80)
    logger.info("MQTT to InfluxDB Bridge")
    logger.info("=" * 80)

    try:
        init_influxdb()
        init_mqtt()
        logger.info("Starting MQTT loop...")
        mqtt_client.loop_forever()

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        if mqtt_client:
            mqtt_client.disconnect()
            mqtt_client.loop_stop()
        if influx_client:
            influx_client.close()
        logger.info("Shutdown complete")

if __name__ == "__main__":
    main()