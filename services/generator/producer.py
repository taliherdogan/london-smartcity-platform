"""Greater London Smart City Multi-Source IoT Stream Generator.

High-throughput, backpressure-safe producer:
- Polls 5 real zero-key official London/UK APIs every API_POLL_INTERVAL_SEC seconds.
- Uses orjson (C/Rust) serialization + pre-encoded keys + fast counter event IDs to sustain
  20 to 25,000+ events/sec cleanly without CPU saturation or BufferError crashes.
- Tracks delivery errors, backpressure retries, and actual sustained throughput in Redis
  (smartcity:infra:generator) for live infrastructure health observability.
- Supports graceful SIGTERM/SIGINT shutdown with clean Kafka queue flush.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import redis
from confluent_kafka import Producer

try:
    import orjson

    def _dumps_bytes(obj: dict[str, Any]) -> bytes:
        return orjson.dumps(obj)
except ImportError:

    def _dumps_bytes(obj: dict[str, Any]) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")


from api_poller import (
    LONDON_ENERGY_SHARE,
    default_baselines,
    london_diurnal_traffic_factor,
    poll_all_sources,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("generator")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
DEFAULT_RATE = int(os.getenv("DEFAULT_RATE", "10"))
MAX_EVENT_RATE = int(os.getenv("MAX_EVENT_RATE", "25000"))
POLL_INTERVAL = int(os.getenv("API_POLL_INTERVAL_SEC", "60"))
METADATA_PATH = os.getenv("METADATA_PATH", "/data/metadata/london_districts.json")

TOPICS = {
    "traffic": "smartcity.traffic",
    "weather": "smartcity.weather",
    "air_quality": "smartcity.air_quality",
    "energy": "smartcity.energy",
}

REDIS_SOURCES_KEY = "smartcity:api_sources"
REDIS_RATE_KEY = "smartcity:control:rate"
REDIS_ANOMALY_KEY = "smartcity:control:anomaly"
REDIS_INFRA_GEN_KEY = "smartcity:infra:generator"
REDIS_INTERNET_KEY = "smartcity:infra:internet"
INTERNET_CHECK_INTERVAL = float(os.getenv("INTERNET_CHECK_INTERVAL_SEC", "3.0"))
INTERNET_PROBE_HOSTS = (
    ("1.1.1.1", 443),
    ("8.8.8.8", 443),
    ("api.open-meteo.com", 443),
)

RUN_PREFIX = uuid.uuid4().hex[:4]
shared_lock = threading.Lock()
live_baselines: dict[str, dict[str, Any]] = default_baselines()
active_anomaly: dict[str, Any] | None = None
stop_event = threading.Event()
internet_online = threading.Event()
internet_online.set()
first_poll_done = threading.Event()
poll_now_event = threading.Event()


def _handle_signal(signum: int, _frame: Any) -> None:
    logger.info("Received signal %s; initiating graceful generator shutdown...", signum)
    stop_event.set()


def create_redis_client() -> redis.Redis:
    pool = redis.ConnectionPool(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        max_connections=8,
        socket_connect_timeout=3,
        socket_timeout=3,
        retry_on_timeout=True,
        health_check_interval=15,
    )
    return redis.Redis(connection_pool=pool)


def load_districts() -> list[dict[str, Any]]:
    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        districts = json.load(f)
    for d in districts:
        d["_key_bytes"] = d["district_id"].encode("utf-8")
        d["_short_id"] = str(d.get("short_code") or d["district_id"][:3]).upper()
    return districts


def _has_internet_access() -> bool:
    """Fast TCP connectivity probe to detect internet loss within seconds."""
    for host, port in INTERNET_PROBE_HOSTS:
        try:
            with socket.create_connection((host, port), timeout=1.5):
                return True
        except OSError:
            continue
    return False


def connectivity_loop(r: redis.Redis) -> None:
    """Monitor internet connectivity and pause/resume pipeline processing immediately."""
    was_online = True
    while not stop_event.is_set():
        is_online = _has_internet_access()
        if is_online:
            internet_online.set()
            if not was_online:
                logger.info("Internet connectivity RESTORED — resuming live API polling and data processing.")
                poll_now_event.set()
        else:
            internet_online.clear()
            if was_online:
                logger.warning("Internet connectivity LOST — pausing data generation and stream processing.")
        was_online = is_online

        try:
            r.set(
                REDIS_INTERNET_KEY,
                json.dumps(
                    {
                        "online": is_online,
                        "checked_at": datetime.now(timezone.utc).isoformat(),
                    }
                ),
                ex=20,
            )
        except Exception as exc:
            logger.debug("Redis set internet status warning: %s", exc)

        stop_event.wait(INTERNET_CHECK_INTERVAL)


def poller_loop(r: redis.Redis, districts: list[dict[str, Any]]) -> None:
    global live_baselines
    while not stop_event.is_set():
        poll_now_event.clear()
        if internet_online.is_set():
            try:
                updated = poll_all_sources(live_baselines, districts)
                with shared_lock:
                    live_baselines = updated
                first_poll_done.set()
                try:
                    r.set(REDIS_SOURCES_KEY, json.dumps(updated))
                except Exception as exc:
                    logger.warning("Redis set api_sources failed: %s", exc)
            except Exception as exc:
                logger.warning("API poller iteration failed: %s", exc)
        # Wait up to POLL_INTERVAL, waking early on shutdown or internet restore
        deadline = time.monotonic() + POLL_INTERVAL
        while not stop_event.is_set() and not poll_now_event.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            poll_now_event.wait(min(1.0, remaining))


def _district_tfl_multiplier(district: dict[str, Any], corridor_statuses: dict[str, Any]) -> tuple[float, int]:
    """Compute realistic speed multiplier & disruption count from real live TfL corridors assigned to a district."""
    assigned = district.get("tfl_corridors", [])
    if not assigned or not corridor_statuses:
        return 1.0, 0
    mults: list[float] = []
    disrupted = 0
    for name in assigned:
        info = corridor_statuses.get(name)
        if info:
            mults.append(float(info.get("speed_mult", 1.0)))
            if int(info.get("rank", 0)) >= 2:
                disrupted += 1
    if not mults:
        return 1.0, 0
    avg_m = sum(mults) / len(mults)
    min_m = min(mults)
    return round(0.6 * avg_m + 0.4 * min_m, 3), disrupted


def build_event(
    seq: int,
    domain: str,
    district: dict[str, Any],
    baselines: dict[str, dict[str, Any]],
    anomaly: dict[str, Any] | None,
    now_ms: int,
    now_iso: str,
    diurnal: float,
    tfl_cache: dict[str, tuple[float, int]],
) -> dict[str, Any]:
    base = baselines[domain]
    did = district["district_id"]
    sensor_idx = (seq & 3) + 1
    sensor_id = f"LON-{domain[:3].upper()}-{district['_short_id']}-{sensor_idx:02d}"

    is_injected = (
        anomaly is not None
        and anomaly.get("domain") == domain
        and (anomaly.get("district_id") in (None, "all", did))
    )

    if domain == "traffic":
        tfl_mult, dist_disrupted = tfl_cache.get(did, (1.0, 0))
        free_flow = float(district.get("free_flow_kmh", 34.0))
        base_speed = free_flow * diurnal * tfl_mult
        speed = base_speed + random.gauss(0, 1.1)

        base_vol = float(base["secondary"].get("vehicle_count", 64.0))
        vol = max(
            12.0,
            base_vol * (1.15 - (free_flow / 120.0) + dist_disrupted * 0.22) + random.gauss(0, 3.0),
        )
        if is_injected:
            speed = random.uniform(4.5, 9.5)
            vol = max(vol * 1.85, 130.0)

        speed = round(max(3.5, min(free_flow * 1.12, speed)), 2)
        congestion = round(max(6.0, min(96.0, (1.0 - speed / (free_flow * 1.05)) * 100.0)), 1)
        metric_name = "avg_speed_kmh"
        value = speed
        unit = "km/h"
        secondary = {
            "vehicle_count": round(vol, 1),
            "congestion_pct": congestion,
            "disrupted_corridors": float(base["secondary"].get("disrupted_corridors", 0)),
            "district_disruptions": float(dist_disrupted),
        }

    elif domain == "weather":
        dist_w = base.get("district_values", {}).get(did)
        if dist_w:
            base_temp = float(dist_w["value"])
            base_hum = float(dist_w["humidity_pct"])
            base_wind = float(dist_w["wind_speed_ms"])
            base_precip = float(dist_w["precipitation_mm"])
        else:
            base_temp = float(base["value"]) + float(district.get("temp_offset_c", 0.0))
            base_hum = float(base["secondary"].get("humidity_pct", 76.0))
            base_wind = float(base["secondary"].get("wind_speed_ms", 4.0))
            base_precip = float(base["secondary"].get("precipitation_mm", 0.0))

        temp = base_temp + random.gauss(0, 0.06)
        wind = max(0.2, base_wind + random.gauss(0, 0.22))
        humidity = max(20.0, min(100.0, base_hum + random.gauss(0, 0.5)))
        precip = max(0.0, base_precip + (max(0.0, random.gauss(0, 0.02)) if base_precip > 0 else 0.0))
        if is_injected:
            wind = random.uniform(19.5, 26.0)
            precip = round(random.uniform(6.5, 14.0), 2)

        metric_name = "temperature_c"
        value = round(temp, 2)
        unit = "°C"
        secondary = {
            "humidity_pct": round(humidity, 1),
            "wind_speed_ms": round(wind, 2),
            "precipitation_mm": round(precip, 2),
        }

    elif domain == "air_quality":
        dist_aq = base.get("district_values", {}).get(did)
        if dist_aq:
            base_aqi = float(dist_aq["value"])
            base_pm25 = float(dist_aq["pm2_5"])
            base_pm10 = float(dist_aq["pm10"])
            base_no2 = float(dist_aq["no2"])
        else:
            factor = float(district.get("aqi_factor", 1.0))
            base_aqi = float(base["value"]) * factor
            base_pm25 = float(base["secondary"].get("pm2_5", 6.0)) * factor
            base_pm10 = float(base["secondary"].get("pm10", 11.0)) * factor
            base_no2 = float(base["secondary"].get("no2", 16.0)) * factor

        aqi = base_aqi + random.gauss(0, 0.5)
        pm25 = max(0.8, base_pm25 + random.gauss(0, 0.2))
        pm10 = max(1.5, base_pm10 + random.gauss(0, 0.35))
        no2 = max(1.0, base_no2 + random.gauss(0, 0.5))
        if is_injected:
            aqi = random.uniform(92.0, 138.0)
            pm25 = random.uniform(38.0, 68.0)
            pm10 = random.uniform(68.0, 118.0)

        metric_name = "european_aqi"
        value = round(max(4.0, aqi), 1)
        unit = "AQI"
        secondary = {
            "pm2_5": round(pm25, 2),
            "pm10": round(pm10, 2),
            "no2": round(no2, 2),
        }

    else:  # energy (Greater London ~14% share of GB national live demand from Elexon BMRS)
        national_mw = float(base["value"])
        london_metro_mw = national_mw * LONDON_ENERGY_SHARE
        district_mw = london_metro_mw * float(district["energy_weight"])
        load = district_mw + random.gauss(0, district_mw * 0.008)
        ren_pct = max(2.0, min(98.0, float(base["secondary"].get("renewable_share_pct", 30.0)) + random.gauss(0, 0.3)))
        low_carbon_pct = max(
            ren_pct,
            min(99.0, float(base["secondary"].get("low_carbon_pct", ren_pct + 10.0)) + random.gauss(0, 0.3)),
        )
        carbon_gco2 = max(12.0, float(base["secondary"].get("carbon_intensity_gco2", 185.0)) + random.gauss(0, 1.5))
        if is_injected:
            load = district_mw * random.uniform(1.38, 1.58)

        metric_name = "district_load_mw"
        value = round(max(50.0, load), 2)
        unit = "MW"
        secondary = {
            "national_load_mw": round(national_mw, 1),
            "london_total_mw": round(london_metro_mw, 1),
            "renewable_share_pct": round(ren_pct, 1),
            "low_carbon_pct": round(low_carbon_pct, 1),
            "carbon_intensity_gco2": round(carbon_gco2, 1),
        }

    return {
        "event_id": f"{RUN_PREFIX}{seq:09x}",
        "sensor_id": sensor_id,
        "district_id": did,
        "domain": domain,
        "metric_name": metric_name,
        "value": value,
        "unit": unit,
        "secondary_metrics": secondary,
        "data_source": f"{base['provider']} ({base['status']})",
        "is_injected_anomaly": bool(is_injected),
        "event_time": now_iso,
        "produced_at_ms": now_ms,
    }


def main() -> None:
    global active_anomaly

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    districts = load_districts()
    num_districts = len(districts)
    r = create_redis_client()

    # Resilient startup sync with Redis
    for attempt in range(5):
        try:
            if not r.get(REDIS_RATE_KEY):
                r.set(REDIS_RATE_KEY, str(DEFAULT_RATE))
            break
        except Exception as exc:
            logger.warning("Waiting for Redis (attempt %d/5): %s", attempt + 1, exc)
            time.sleep(1.5)

    conn_thread = threading.Thread(target=connectivity_loop, args=(r,), daemon=True)
    conn_thread.start()

    poller_thread = threading.Thread(target=poller_loop, args=(r, districts), daemon=True)
    poller_thread.start()

    delivery_errors = 0
    last_err_log = 0.0

    def _on_delivery(err: Any, _msg: Any) -> None:
        nonlocal delivery_errors, last_err_log
        if err is not None:
            delivery_errors += 1
            now_m = time.monotonic()
            if now_m - last_err_log >= 10.0:
                logger.warning("Kafka delivery error (total=%d): %s", delivery_errors, err)
                last_err_log = now_m

    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "linger.ms": 15,
            "batch.size": 131072,
            "batch.num.messages": 10000,
            "compression.type": "lz4",
            "acks": 1,
            "retries": 5,
            "retry.backoff.ms": 100,
            "message.timeout.ms": 15000,
            "queue.buffering.max.messages": 500000,
            "queue.buffering.max.kbytes": 131072,
            "socket.keepalive.enable": True,
        }
    )

    domains = ("traffic", "weather", "air_quality", "energy")
    num_domains = len(domains)
    ticks_per_sec = 10
    tick_duration = 1.0 / ticks_per_sec
    current_rate = max(10, min(MAX_EVENT_RATE, DEFAULT_RATE))
    rate_carry = 0.0
    next_seq = 0
    last_control_check = 0.0
    total_sent = 0
    window_sent = 0
    dropped_events = 0
    backpressure_hits = 0
    last_log = time.monotonic()
    last_infra_publish = 0.0

    logger.info(
        "London Generator started | bootstrap=%s | initial_rate=%d ev/s | max_rate=%d ev/s",
        KAFKA_BOOTSTRAP,
        current_rate,
        MAX_EVENT_RATE,
    )

    try:
        while not stop_event.is_set():
            tick_start = time.monotonic()

            if tick_start - last_control_check >= 0.5:
                last_control_check = tick_start
                try:
                    raw_rate = r.get(REDIS_RATE_KEY)
                    if isinstance(raw_rate, str) and raw_rate:
                        current_rate = max(10, min(MAX_EVENT_RATE, int(raw_rate)))
                    raw_anom = r.get(REDIS_ANOMALY_KEY)
                    if isinstance(raw_anom, str) and raw_anom:
                        parsed = json.loads(raw_anom)
                        if parsed.get("expires_at", 0) > time.time():
                            active_anomaly = parsed
                        else:
                            active_anomaly = None
                            r.delete(REDIS_ANOMALY_KEY)
                    else:
                        active_anomaly = None
                except Exception as exc:
                    logger.debug("Control poll warning: %s", exc)

            rate_carry += current_rate / ticks_per_sec
            events_this_tick = int(rate_carry)
            rate_carry -= events_this_tick
            if events_this_tick <= 0:
                events_this_tick = 1

            with shared_lock:
                baselines_snapshot = dict(live_baselines)

            # Pause all event production (and downstream Spark processing) while internet is down
            all_apis_down = first_poll_done.is_set() and not any(
                b.get("status") == "live" for b in baselines_snapshot.values()
            )
            if not internet_online.is_set() or all_apis_down:
                producer.poll(0)
                if tick_start - last_infra_publish >= 2.0:
                    try:
                        r.set(
                            REDIS_INFRA_GEN_KEY,
                            json.dumps(
                                {
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                    "status": "paused_offline",
                                    "target_rate_eps": current_rate,
                                    "actual_rate_eps": 0.0,
                                    "max_rate_eps": MAX_EVENT_RATE,
                                    "total_sent": total_sent,
                                    "queue_len": len(producer),
                                    "backpressure_hits": backpressure_hits,
                                    "dropped_events": dropped_events,
                                    "delivery_errors": delivery_errors,
                                }
                            ),
                            ex=30,
                        )
                    except Exception:
                        pass
                    window_sent = 0
                    last_infra_publish = tick_start
                stop_event.wait(0.25)
                continue

            # Compute shared tick metadata once per 100ms tick (huge CPU savings at 5K-25K ev/s)
            now_ms = int(time.time() * 1000)
            now_iso = datetime.now(timezone.utc).isoformat()
            diurnal = london_diurnal_traffic_factor()
            corridor_statuses = baselines_snapshot["traffic"].get("corridor_statuses", {})
            tfl_cache = {
                d["district_id"]: _district_tfl_multiplier(d, corridor_statuses)
                for d in districts
            }

            sent_this_tick = 0
            for i in range(events_this_tick):
                if stop_event.is_set():
                    break
                seq = next_seq
                next_seq += 1
                domain = domains[seq % num_domains]
                district = districts[(seq // num_domains) % num_districts]
                evt = build_event(
                    seq=seq,
                    domain=domain,
                    district=district,
                    baselines=baselines_snapshot,
                    anomaly=active_anomaly,
                    now_ms=now_ms,
                    now_iso=now_iso,
                    diurnal=diurnal,
                    tfl_cache=tfl_cache,
                )
                payload = _dumps_bytes(evt)
                topic = TOPICS[domain]
                key_bytes = district["_key_bytes"]

                enqueued = False
                for retry in range(8):
                    try:
                        producer.produce(
                            topic=topic,
                            key=key_bytes,
                            value=payload,
                            on_delivery=_on_delivery,
                        )
                        enqueued = True
                        break
                    except BufferError:
                        backpressure_hits += 1
                        producer.poll(0.05)
                        if retry >= 4:
                            time.sleep(0.02)

                if enqueued:
                    sent_this_tick += 1
                else:
                    dropped_events += 1

                # Periodically drain delivery callbacks during large bursts to keep buffer lean
                if (i & 511) == 511:
                    producer.poll(0)

            producer.poll(0)
            total_sent += sent_this_tick
            window_sent += sent_this_tick

            # Publish generator infra telemetry every 2 seconds (non-fatal)
            if tick_start - last_infra_publish >= 2.0:
                dt_win = max(0.5, tick_start - last_infra_publish) if last_infra_publish > 0 else 2.0
                actual_rate = round(window_sent / dt_win, 1)
                queue_len = len(producer)
                gen_status = "backpressure" if queue_len > 250000 else ("elevated" if queue_len > 80000 else "healthy")
                try:
                    r.set(
                        REDIS_INFRA_GEN_KEY,
                        json.dumps(
                            {
                                "timestamp": now_iso,
                                "status": gen_status,
                                "target_rate_eps": current_rate,
                                "actual_rate_eps": actual_rate,
                                "max_rate_eps": MAX_EVENT_RATE,
                                "total_sent": total_sent,
                                "queue_len": queue_len,
                                "backpressure_hits": backpressure_hits,
                                "dropped_events": dropped_events,
                                "delivery_errors": delivery_errors,
                            }
                        ),
                        ex=30,
                    )
                except Exception:
                    pass
                window_sent = 0
                last_infra_publish = tick_start

            if tick_start - last_log >= 10.0:
                logger.info(
                    "London Streaming active | target=%d ev/s | total_sent=%d | queue=%d | bp_hits=%d | dropped=%d | anomaly=%s",
                    current_rate,
                    total_sent,
                    len(producer),
                    backpressure_hits,
                    dropped_events,
                    active_anomaly.get("domain") if active_anomaly else "none",
                )
                last_log = tick_start

            elapsed = time.monotonic() - tick_start
            sleep_time = tick_duration - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
    finally:
        logger.info("Flushing Kafka producer queue (%d pending)...", len(producer))
        producer.flush(5.0)
        logger.info("Generator stopped cleanly. Total events sent=%d", total_sent)


if __name__ == "__main__":
    main()
