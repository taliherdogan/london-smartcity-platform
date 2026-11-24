"""Greater London Smart City — Resilient Apache Spark Structured Streaming Processor.

Infrastructure Hardening:
1. Out-of-core Spark tuning (Kryo serializer, G1GC, bounded memory fractions, dedicated spill dir,
   bounded checkpoint retention, configurable shuffle partitions & maxOffsetsPerTrigger).
2. Eliminates redundant .count() Spark jobs per micro-batch and uses bounded percentile_approx
   accuracy for O(1) aggregation memory even at 5K-25K+ events/sec.
3. Disk-safe Cold Path Data Lake:
   - Partitioned Snappy Parquet (dt/domain) with maxRecordsPerFile — NO single-core coalesce(1) OOM.
   - Automatic disk-space & max-size retention guard (LAKE_MAX_SIZE_MB) pruning oldest partitions.
   - Non-fatal error isolation so cold-path I/O issues never interrupt the real-time hot path.
4. Strictly bounded driver Python state (whitelisted district/domain keys) + pooled Redis client
   with non-fatal reconnection + SIGTERM graceful shutdown + Docker liveness heartbeat.
"""

from __future__ import annotations

import json
import logging
import math
import os
import shutil
import signal
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psutil
import redis
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from mssql_sink import MSSQLSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("spark_processor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
METADATA_PATH = os.getenv("METADATA_PATH", "/data/metadata/london_districts.json")
LAKE_DIR = os.getenv("LAKE_DIR", "/data/lake")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-spill/checkpoints")
SPARK_LOCAL_DIR = os.getenv("SPARK_LOCAL_DIR", "/tmp/spark-spill/local")
TRIGGER_SEC = int(os.getenv("TRIGGER_INTERVAL_SEC", "2"))
SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000"))
ARCHIVE_EVERY_N_BATCHES = max(1, int(os.getenv("ARCHIVE_EVERY_N_BATCHES", "10")))
ARCHIVE_CSV_ENABLED = os.getenv("ARCHIVE_CSV_ENABLED", "false").lower() in ("1", "true", "yes")
LAKE_MAX_SIZE_MB = float(os.getenv("LAKE_MAX_SIZE_MB", "512"))
MSSQL_WRITE_EVERY_N_BATCHES = max(1, int(os.getenv("MSSQL_WRITE_EVERY_N_BATCHES", "1")))
HEALTH_FILE = Path("/tmp/spark_healthy")

REDIS_PUBSUB_CHANNEL = "smartcity:live_updates"
REDIS_LATEST_KEY = "smartcity:latest_state"
REDIS_TIMESERIES_KEY = "smartcity:timeseries"
REDIS_ALERTS_KEY = "smartcity:alerts"
REDIS_RATE_KEY = "smartcity:control:rate"
REDIS_TELEMETRY_PREFIX = "smartcity:rate_telemetry:"
REDIS_INFRA_SPARK_KEY = "smartcity:infra:spark"
REDIS_STATE_SNAPSHOT_KEY = "smartcity:spark:state"
RESET_STATE_ON_START = os.getenv("RESET_STATE_ON_START", "false").lower() in ("1", "true", "yes")

LONDON_ENERGY_SHARE = 0.14
SEVERITY_ORDER = {"normal": 0, "warning": 1, "critical": 2}
VALID_DOMAINS = {"traffic", "weather", "air_quality", "energy"}
MAX_STATE_KEYS = 256


def _touch_health() -> None:
    try:
        HEALTH_FILE.write_text(str(int(time.time())), encoding="utf-8")
    except Exception:
        pass


def _process_tree_rss_mb() -> float:
    """Measure combined container/JVM+Python Spark memory in MB (cgroup-aware with psutil fallback)."""
    for cg_path in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        try:
            raw = Path(cg_path).read_text(encoding="utf-8").strip()
            val = int(raw)
            if 0 < val < (1 << 50):
                return round(val / (1024 * 1024), 1)
        except Exception:
            pass
    try:
        proc = psutil.Process(os.getpid())
        seen = {proc.pid}
        total = proc.memory_info().rss
        parent = proc.parent()
        if parent and parent.pid not in seen and "java" in (parent.name() or "").lower():
            seen.add(parent.pid)
            total += parent.memory_info().rss
        for child in proc.children(recursive=True):
            if child.pid not in seen:
                seen.add(child.pid)
                try:
                    total += child.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        return round(total / (1024 * 1024), 1)
    except Exception:
        return 0.0


def _dir_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    total = 0
    try:
        for f in path.rglob("*"):
            if f.is_file():
                try:
                    total += f.stat().st_size
                except OSError:
                    pass
    except Exception:
        return 0.0
    return round(total / (1024 * 1024), 2)


def enforce_lake_retention(lake_dir: str, max_size_mb: float) -> tuple[bool, float]:
    """Ensure Data Lake never exhausts container/host disk space.

    Returns: (can_write, current_lake_size_mb)
    """
    root = Path(lake_dir)
    root.mkdir(parents=True, exist_ok=True)

    # 1. Check host/volume free space (keep at least 256 MB safety margin)
    try:
        free_mb = shutil.disk_usage(root).free / (1024 * 1024)
        if free_mb < 256.0:
            logger.warning("Low disk space on %s (%.1f MB free) — pruning lake archives", root, free_mb)
    except Exception:
        free_mb = 1024.0

    size_mb = _dir_size_mb(root)
    if size_mb <= max_size_mb and free_mb >= 256.0:
        return True, size_mb

    # 2. Prune oldest partition folders or files inside parquet/ and csv/
    candidates: list[tuple[float, Path]] = []
    for sub_name in ("parquet", "csv"):
        sub = root / sub_name
        if not sub.exists():
            continue
        for entry in sub.iterdir():
            try:
                candidates.append((entry.stat().st_mtime, entry))
            except OSError:
                pass

    candidates.sort(key=lambda item: item[0])
    target_mb = max_size_mb * 0.75
    for _, path in candidates:
        if size_mb <= target_mb and free_mb >= 256.0:
            break
        try:
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)
            size_mb = _dir_size_mb(root)
            free_mb = shutil.disk_usage(root).free / (1024 * 1024)
        except Exception:
            pass

    if free_mb < 128.0:
        logger.warning("Skipping lake archive write due to critical free disk space (%.1f MB)", free_mb)
        return False, size_mb

    return True, size_mb


def london_now() -> datetime:
    """Return current UK local time (GMT/BST) without requiring zoneinfo/tzdata."""
    now_utc = datetime.now(timezone.utc)
    year = now_utc.year
    mar31 = datetime(year, 3, 31, 1, 0, tzinfo=timezone.utc)
    bst_start = mar31 - timedelta(days=(mar31.weekday() + 1) % 7)
    oct31 = datetime(year, 10, 31, 1, 0, tzinfo=timezone.utc)
    bst_end = oct31 - timedelta(days=(oct31.weekday() + 1) % 7)
    offset = timedelta(hours=1) if bst_start <= now_utc < bst_end else timedelta(0)
    return now_utc.astimezone(timezone(offset))


def london_diurnal_traffic_factor(local_now: datetime | None = None) -> float:
    """Expected Greater London time-of-day speed factor (matches api_poller.py)."""
    now = local_now or london_now()
    hour = now.hour
    is_weekend = now.weekday() >= 5
    if hour >= 22 or hour < 6:
        return 0.98
    if hour == 6:
        return 0.92 if is_weekend else 0.88
    if 7 <= hour <= 9:
        return 0.86 if is_weekend else 0.72
    if 10 <= hour <= 15:
        return 0.84 if is_weekend else 0.80
    if 16 <= hour <= 18:
        return 0.80 if is_weekend else 0.68
    return 0.88 if is_weekend else 0.85


EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("district_id", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("metric_name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("secondary_metrics", MapType(StringType(), DoubleType()), True),
        StructField("data_source", StringType(), True),
        StructField("is_injected_anomaly", BooleanType(), True),
        StructField("event_time", StringType(), True),
        StructField("produced_at_ms", LongType(), True),
    ]
)

WINDOW_HISTORY: dict[tuple[str, str], deque[float]] = defaultdict(lambda: deque(maxlen=30))
DISTRICT_ENERGY_BASE: dict[str, float] = {}
LATEST_DISTRICTS_STATE: dict[str, dict[str, Any]] = {}
LAST_ALERT_AT: dict[tuple[str, str], float] = {}
ALERT_COOLDOWN_SEC = 90.0
CUMULATIVE_EVENTS = 0
CUMULATIVE_ALERTS = 0
LAST_BATCH_WALL = time.monotonic()
LAST_LAKE_SIZE_MB = 0.0


def evaluate_threshold(
    domain: str,
    value: float,
    secondary: dict[str, float],
    district_id: str,
    district_meta: dict[str, Any] | None = None,
) -> tuple[bool, str, str, str, float | None, str]:
    """Layer 1: Realistic London domain thresholds grounded in live TfL, Open-Meteo & Elexon data."""
    if domain == "traffic":
        v = round(value, 1)
        congestion = round(secondary.get("congestion_pct", 0.0))
        disruptions = int(secondary.get("district_disruptions", 0))
        free_flow = float((district_meta or {}).get("free_flow_kmh", 32.0))
        expected_normal = round(free_flow * london_diurnal_traffic_factor(), 1)
        ref_baseline = expected_normal if expected_normal > v + 1.0 else round(free_flow, 1)

        if v < 12.0 or congestion > 82:
            return (
                True,
                "critical",
                f"Yoğun trafik sıkışıklığı (Hız: {v:.1f} km/s, Yoğunluk: %{congestion})",
                "Hız < 12 km/s veya Yoğunluk > %82",
                ref_baseline,
                "low",
            )
        if disruptions >= 1 and v < 24.0:
            return (
                True,
                "warning",
                f"Bölgedeki {disruptions} ana yolda aksama nedeniyle trafik yavaşladı (Hız: {v:.1f} km/s)",
                "Aktif yol aksaması + Hız < 24 km/s",
                ref_baseline,
                "low",
            )
    elif domain == "air_quality":
        v = round(value, 1)
        pm25 = round(secondary.get("pm2_5", 0.0), 1)
        if v > 80.0 or pm25 > 35.0:
            return (
                True,
                "critical",
                f"Çok kötü hava kalitesi (Avrupa AQI: {v:.0f}, PM2.5: {pm25:.1f} µg/m³)",
                "EAQI > 80 veya PM2.5 > 35 µg/m³",
                30.0,
                "high",
            )
        if v > 60.0 or pm25 > 25.0:
            return (
                True,
                "warning",
                f"Kötü / hassas hava kalitesi (Avrupa AQI: {v:.0f}, PM2.5: {pm25:.1f} µg/m³)",
                "EAQI > 60 veya PM2.5 > 25 µg/m³",
                30.0,
                "high",
            )
    elif domain == "weather":
        wind = round(secondary.get("wind_speed_ms", 0.0), 1)
        v = round(value, 1)
        if wind > 18.0:
            return (
                True,
                "critical",
                f"Şiddetli fırtına / rüzgar uyarısı ({wind:.1f} m/s)",
                "Rüzgar > 18 m/s (~65 km/s)",
                6.0,
                "high",
            )
        if wind > 12.5:
            return (
                True,
                "warning",
                f"Kuvvetli rüzgar uyarısı ({wind:.1f} m/s)",
                "Rüzgar > 12.5 m/s (~45 km/s)",
                6.0,
                "high",
            )
        if v < 0.0 or v > 30.0:
            return (
                True,
                "warning",
                f"{'Don / buzlanma riski' if v < 0.0 else 'Yüksek sıcaklık uyarısı'} ({v:.1f} °C)",
                "Sıcaklık < 0°C veya > 30°C",
                12.0 if v < 0.0 else 20.0,
                "low" if v < 0.0 else "high",
            )
    elif domain == "energy":
        v = round(value)
        base_mw = round(DISTRICT_ENERGY_BASE.get(district_id, 150.0))
        crit_mw = round(base_mw * 1.25)
        warn_mw = round(base_mw * 1.15)
        if base_mw > 0 and v > crit_mw:
            return (
                True,
                "critical",
                f"Yüksek Londra şebeke yükü ({v} MW, üst sınır: {crit_mw} MW)",
                f"Şebeke Yükü > {crit_mw} MW (+%25)",
                float(base_mw),
                "high",
            )
        if base_mw > 0 and v > warn_mw:
            return (
                True,
                "warning",
                f"Şebeke yükü normalin üzerinde ({v} MW)",
                f"Şebeke Yükü > {warn_mw} MW (+%15)",
                float(base_mw),
                "high",
            )
    return False, "info", "", "", None, "high"


def evaluate_zscore(key: tuple[str, str], value: float) -> tuple[bool, float, float | None]:
    """Layer 2: Sliding-window statistical Z-Score anomaly detection (bounded memory)."""
    if key not in WINDOW_HISTORY and len(WINDOW_HISTORY) >= MAX_STATE_KEYS:
        return False, 0.0, None
    hist = WINDOW_HISTORY[key]
    if len(hist) < 10:
        hist.append(value)
        return False, 0.0, None

    mean = sum(hist) / len(hist)
    variance = sum((x - mean) ** 2 for x in hist) / len(hist)
    std = math.sqrt(variance)
    eps = max(abs(mean) * 0.04, 1.0)
    z = (value - mean) / max(std, eps)

    if abs(z) <= 3.2:
        hist.append(value)
        return False, round(z, 2), round(mean, 2)
    return True, round(z, 2), round(mean, 2)


def create_redis_client() -> redis.Redis:
    pool = redis.ConnectionPool(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        max_connections=12,
        socket_connect_timeout=3,
        socket_timeout=3,
        retry_on_timeout=True,
        health_check_interval=15,
    )
    return redis.Redis(connection_pool=pool)


def archive_batch_safely(batch_with_latency: DataFrame, batch_id: int) -> float:
    """Write cold-path Parquet archive without coalesce(1) bottlenecks or disk exhaustion."""
    can_write, size_mb = enforce_lake_retention(LAKE_DIR, LAKE_MAX_SIZE_MB)
    if not can_write:
        return size_mb

    hour_bucket = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H")
    archive_df = batch_with_latency.select(
        "event_id",
        "sensor_id",
        "district_id",
        "district_name",
        "domain",
        "metric_name",
        "value",
        "unit",
        "event_time",
        "latency_ms",
    ).withColumn("dt", F.lit(hour_bucket))

    # Parallel partitioned Parquet write: repartition("domain") avoids small-file explosion
    # (1 file per domain per flush instead of N shuffle partitions x 4 domains)
    (
        archive_df.repartition("domain")
        .write.mode("append")
        .partitionBy("dt", "domain")
        .option("compression", "snappy")
        .parquet(os.path.join(LAKE_DIR, "parquet"))
    )

    if ARCHIVE_CSV_ENABLED:
        (
            archive_df.write.mode("append")
            .partitionBy("dt")
            .option("header", "true")
            .csv(os.path.join(LAKE_DIR, "csv"))
        )

    return _dir_size_mb(Path(LAKE_DIR))


def main() -> None:
    Path(SPARK_LOCAL_DIR).mkdir(parents=True, exist_ok=True)
    Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)
    _touch_health()

    spark = (
        SparkSession.builder.appName("LondonSmartCityStreaming")
        .config("spark.sql.shuffle.partitions", SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.25")
        .config("spark.local.dir", SPARK_LOCAL_DIR)
        .config("spark.sql.streaming.minBatchesToRetain", "20")
        .config("spark.cleaner.periodicGC.interval", "5min")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.parquet.block.size", str(64 * 1024 * 1024))
        .config("spark.sql.files.maxRecordsPerFile", "500000")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        districts_meta = json.load(f)
    districts_by_id = {d["district_id"]: d for d in districts_meta}
    district_order = {d["district_id"]: idx for idx, d in enumerate(districts_meta)}

    r = create_redis_client()
    mssql_sink = MSSQLSink()
    mssql_ready = mssql_sink.ensure_schema()

    def _apply_saved_window_history(saved_hist: Any) -> None:
        if isinstance(saved_hist, dict):
            for composite_key, vals in saved_hist.items():
                if "|" in composite_key and isinstance(vals, list) and len(WINDOW_HISTORY) < MAX_STATE_KEYS:
                    did_k, dom_k = composite_key.split("|", 1)
                    if did_k not in districts_by_id:
                        continue
                    WINDOW_HISTORY[(did_k, dom_k)] = deque(
                        (float(v) for v in vals[-30:]), maxlen=30
                    )

    try:
        if RESET_STATE_ON_START:
            r.delete(REDIS_ALERTS_KEY, REDIS_STATE_SNAPSHOT_KEY)
        else:
            global CUMULATIVE_EVENTS, CUMULATIVE_ALERTS
            restored_from_redis = False
            raw_saved = r.get(REDIS_STATE_SNAPSHOT_KEY)
            if isinstance(raw_saved, str) and raw_saved:
                saved = json.loads(raw_saved)
                CUMULATIVE_EVENTS = max(0, int(saved.get("cumulative_events", 0)))
                CUMULATIVE_ALERTS = max(0, int(saved.get("cumulative_alerts", 0)))
                _apply_saved_window_history(saved.get("window_history", {}))
                restored_from_redis = True
                logger.info(
                    "Restored Spark state from Redis | events=%d | alerts=%d | windows=%d",
                    CUMULATIVE_EVENTS,
                    CUMULATIVE_ALERTS,
                    len(WINDOW_HISTORY),
                )

            if mssql_ready:
                db_state = mssql_sink.restore_state()
                if db_state:
                    db_events = int(db_state.get("cumulative_events", 0))
                    db_alerts = int(db_state.get("cumulative_alerts", 0))
                    if db_events > CUMULATIVE_EVENTS:
                        CUMULATIVE_EVENTS = db_events
                    if db_alerts > CUMULATIVE_ALERTS:
                        CUMULATIVE_ALERTS = db_alerts
                    if not restored_from_redis:
                        _apply_saved_window_history(db_state.get("window_history", {}))

                    # Repopulate Redis hot lists if Redis was freshly recreated
                    pipe_restore = r.pipeline()
                    if r.llen(REDIS_ALERTS_KEY) == 0 and db_state.get("recent_alerts"):
                        for al in reversed(db_state["recent_alerts"]):
                            pipe_restore.lpush(REDIS_ALERTS_KEY, json.dumps(al))
                        pipe_restore.ltrim(REDIS_ALERTS_KEY, 0, 39)
                    if r.llen(REDIS_TIMESERIES_KEY) == 0 and db_state.get("timeseries"):
                        for pt in db_state["timeseries"][-60:]:
                            pipe_restore.rpush(REDIS_TIMESERIES_KEY, json.dumps(pt))
                        pipe_restore.ltrim(REDIS_TIMESERIES_KEY, -60, -1)
                    pipe_restore.execute()
                    logger.info(
                        "Synced persistent Spark state from MSSQL | events=%d | alerts=%d | alerts_cached=%d | ts_cached=%d",
                        CUMULATIVE_EVENTS,
                        CUMULATIVE_ALERTS,
                        len(db_state.get("recent_alerts", [])),
                        len(db_state.get("timeseries", [])),
                    )
    except Exception as exc:
        logger.warning("Non-fatal state restore warning: %s", exc)

    for d in districts_meta:
        DISTRICT_ENERGY_BASE[d["district_id"]] = 34500.0 * LONDON_ENERGY_SHARE * float(d["energy_weight"])

    flat_districts = [
        {
            "district_id": d["district_id"],
            "district_name": d["district_name"],
            "zone_type": d["zone_type"],
            "population_density": int(d["population_density"]),
        }
        for d in districts_meta
    ]
    broadcast_districts = F.broadcast(spark.createDataFrame(flat_districts))

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", "smartcity\\..*")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("kafka.session.timeout.ms", "30000")
        .option("kafka.request.timeout.ms", "40000")
        .load()
    )

    parsed_stream = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .filter(
            F.col("domain").isNotNull()
            & F.col("value").isNotNull()
            & F.col("district_id").isNotNull()
        )
        .withColumn("event_ts", F.to_timestamp(F.col("event_time")))
    )

    enriched_stream = parsed_stream.join(broadcast_districts, on="district_id", how="left")

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        global CUMULATIVE_EVENTS, CUMULATIVE_ALERTS, LAST_BATCH_WALL, LAST_LAKE_SIZE_MB
        batch_start = time.monotonic()
        now_ms = int(time.time() * 1000)

        batch_with_latency = batch_df.withColumn(
            "latency_ms",
            F.greatest(F.lit(1.0), (F.lit(now_ms) - F.col("produced_at_ms")).cast("double")),
        )
        batch_with_latency.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            # 1. Domain-level real-time aggregation (also gives exact row_count without a separate .count() job!)
            domain_rows = (
                batch_with_latency.groupBy("domain")
                .agg(
                    F.count("*").alias("event_count"),
                    F.avg("value").alias("avg_value"),
                    F.min("value").alias("min_value"),
                    F.max("value").alias("max_value"),
                    F.stddev("value").alias("std_value"),
                    F.first("unit").alias("unit"),
                    F.first("metric_name").alias("metric_name"),
                    F.first("data_source").alias("data_source"),
                    F.avg("latency_ms").alias("avg_latency_ms"),
                    F.expr("percentile_approx(latency_ms, 0.5, 2000)").alias("p50_latency_ms"),
                    F.expr("percentile_approx(latency_ms, 0.95, 2000)").alias("p95_latency_ms"),
                    F.avg(F.col("secondary_metrics")["congestion_pct"]).alias("avg_congestion_pct"),
                    F.avg(F.col("secondary_metrics")["vehicle_count"]).alias("avg_vehicle_count"),
                    F.max(F.col("secondary_metrics")["disrupted_corridors"]).alias("disrupted_corridors"),
                    F.avg(F.col("secondary_metrics")["humidity_pct"]).alias("avg_humidity_pct"),
                    F.avg(F.col("secondary_metrics")["wind_speed_ms"]).alias("avg_wind_speed_ms"),
                    F.avg(F.col("secondary_metrics")["pm2_5"]).alias("avg_pm2_5"),
                    F.avg(F.col("secondary_metrics")["pm10"]).alias("avg_pm10"),
                    F.avg(F.col("secondary_metrics")["renewable_share_pct"]).alias("avg_renewable_pct"),
                    F.avg(F.col("secondary_metrics")["low_carbon_pct"]).alias("avg_low_carbon_pct"),
                    F.avg(F.col("secondary_metrics")["national_load_mw"]).alias("avg_national_load_mw"),
                    F.avg(F.col("secondary_metrics")["london_total_mw"]).alias("avg_london_total_mw"),
                    F.avg(F.col("secondary_metrics")["carbon_intensity_gco2"]).alias("avg_carbon_intensity"),
                )
                .collect()
            )

            row_count = sum(int(row["event_count"]) for row in domain_rows)
            if row_count == 0:
                _touch_health()
                return

            elapsed_since_last = max(0.5, batch_start - LAST_BATCH_WALL)
            LAST_BATCH_WALL = batch_start
            CUMULATIVE_EVENTS += row_count

            # 2. District x Domain enriched aggregation (post Broadcast Join)
            district_rows = (
                batch_with_latency.groupBy("district_id", "district_name", "zone_type", "domain")
                .agg(
                    F.count("*").alias("event_count"),
                    F.avg("value").alias("avg_value"),
                    F.max("value").alias("max_value"),
                    F.min("value").alias("min_value"),
                    F.avg(F.col("secondary_metrics")["congestion_pct"]).alias("avg_congestion_pct"),
                    F.max(F.col("secondary_metrics")["district_disruptions"]).alias("district_disruptions"),
                    F.avg(F.col("secondary_metrics")["pm2_5"]).alias("avg_pm2_5"),
                    F.avg(F.col("secondary_metrics")["wind_speed_ms"]).alias("avg_wind_speed_ms"),
                    F.max(F.col("secondary_metrics")["wind_speed_ms"]).alias("max_wind_speed_ms"),
                    F.max(F.col("secondary_metrics")["pm2_5"]).alias("max_pm2_5"),
                    F.max(F.col("secondary_metrics")["congestion_pct"]).alias("max_congestion_pct"),
                    F.max(F.col("is_injected_anomaly").cast("int")).alias("has_injected"),
                )
                .collect()
            )

            domains_summary: dict[str, dict[str, Any]] = {}
            all_p50: list[float] = []
            all_p95: list[float] = []
            all_avg_lat: list[float] = []

            for row in domain_rows:
                d = row["domain"]
                if d not in VALID_DOMAINS:
                    continue
                all_p50.append(float(row["p50_latency_ms"] or 0.0))
                all_p95.append(float(row["p95_latency_ms"] or 0.0))
                all_avg_lat.append(float(row["avg_latency_ms"] or 0.0))

                secondary: dict[str, float] = {}
                if d == "traffic":
                    secondary = {
                        "congestion_pct": round(float(row["avg_congestion_pct"] or 0.0), 1),
                        "vehicle_count": round(float(row["avg_vehicle_count"] or 0.0), 1),
                        "disrupted_corridors": int(row["disrupted_corridors"] or 0),
                    }
                elif d == "weather":
                    secondary = {
                        "humidity_pct": round(float(row["avg_humidity_pct"] or 0.0), 1),
                        "wind_speed_ms": round(float(row["avg_wind_speed_ms"] or 0.0), 2),
                    }
                elif d == "air_quality":
                    secondary = {
                        "pm2_5": round(float(row["avg_pm2_5"] or 0.0), 2),
                        "pm10": round(float(row["avg_pm10"] or 0.0), 2),
                    }
                elif d == "energy":
                    nat_mw = float(row["avg_national_load_mw"] or 34500.0)
                    lon_total_mw = float(row["avg_london_total_mw"] or (nat_mw * LONDON_ENERGY_SHARE))
                    for did, dmeta in districts_by_id.items():
                        DISTRICT_ENERGY_BASE[did] = nat_mw * LONDON_ENERGY_SHARE * float(dmeta["energy_weight"])
                    secondary = {
                        "renewable_share_pct": round(float(row["avg_renewable_pct"] or 0.0), 1),
                        "low_carbon_pct": round(float(row["avg_low_carbon_pct"] or 0.0), 1),
                        "national_load_mw": round(nat_mw, 1),
                        "london_total_mw": round(lon_total_mw, 1),
                        "carbon_intensity_gco2": round(float(row["avg_carbon_intensity"] or 185.0), 1),
                    }

                domains_summary[d] = {
                    "domain": d,
                    "metric_name": row["metric_name"],
                    "avg_value": round(float(row["avg_value"]), 2),
                    "min_value": round(float(row["min_value"]), 2),
                    "max_value": round(float(row["max_value"]), 2),
                    "unit": row["unit"],
                    "event_count": int(row["event_count"]),
                    "data_source": row["data_source"],
                    "secondary": secondary,
                }

            districts_map: dict[str, dict[str, Any]] = {}
            new_alerts: list[dict[str, Any]] = []
            ts_iso = datetime.now(timezone.utc).isoformat()

            for row in district_rows:
                did = row["district_id"]
                dom = row["domain"]
                if did not in districts_by_id or dom not in VALID_DOMAINS:
                    continue
                dname = row["district_name"] or did
                ztype = row["zone_type"] or "urban"
                avg_val = round(float(row["avg_value"]), 2)

                if did not in districts_map:
                    districts_map[did] = {
                        "district_id": did,
                        "district_name": dname,
                        "zone_type": ztype,
                        "tfl_corridors": districts_by_id.get(did, {}).get("tfl_corridors", []),
                        "tfl_disruptions": 0,
                        "metrics": {},
                        "status": "normal",
                    }
                if dom == "traffic" and row["district_disruptions"]:
                    districts_map[did]["tfl_disruptions"] = int(row["district_disruptions"])

                districts_map[did]["metrics"][dom] = {
                    "avg": avg_val,
                    "min": round(float(row["min_value"]), 2),
                    "max": round(float(row["max_value"]), 2),
                    "count": int(row["event_count"]),
                }

                avg_wind = float(row["avg_wind_speed_ms"] or 0.0)
                sec_check = {
                    "congestion_pct": float(row["avg_congestion_pct"] or 0.0),
                    "district_disruptions": float(row["district_disruptions"] or 0.0),
                    "pm2_5": float(row["avg_pm2_5"] or 0.0),
                    "wind_speed_ms": avg_wind,
                }

                wind_z_hit, wind_z_val, wind_baseline = (
                    evaluate_zscore((did, "weather_wind"), avg_wind)
                    if dom == "weather"
                    else (False, 0.0, None)
                )

                thresh_hit, severity, thresh_reason, thresh_label, ref_baseline, direction = evaluate_threshold(
                    dom, avg_val, sec_check, did, districts_by_id.get(did)
                )
                is_wind_event = dom == "weather" and avg_wind > 12.5
                if is_wind_event:
                    z_hit, z_val, baseline_mean = wind_z_hit, wind_z_val, wind_baseline
                else:
                    z_hit, z_val, baseline_mean = evaluate_zscore((did, dom), avg_val)

                if thresh_hit or z_hit:
                    if dom == "traffic":
                        metric_label, unit = "Ortalama Hız", "km/s"
                    elif dom == "air_quality":
                        metric_label, unit = "Hava Kalitesi", "AQI"
                    elif dom == "energy":
                        metric_label, unit = "Şebeke Yükü", "MW"
                    elif is_wind_event:
                        metric_label, unit = "Rüzgar Hızı", "m/s"
                    else:
                        metric_label, unit = "Sıcaklık", "°C"

                    raw_measured = avg_wind if is_wind_event else avg_val
                    measured_val = round(raw_measured) if dom == "energy" else round(raw_measured, 1)

                    if thresh_hit and z_hit:
                        method = "Hibrit (Eşik + Z-Score)"
                        chosen_baseline = ref_baseline if ref_baseline is not None else baseline_mean
                        baseline_label = "Beklenen Normal"
                    elif thresh_hit:
                        method = "Eşik Kuralı"
                        chosen_baseline = ref_baseline if ref_baseline is not None else baseline_mean
                        baseline_label = "Beklenen Normal"
                    else:
                        method = "Z-Score Sapması"
                        severity = "warning"
                        direction = "high" if z_val > 0 else "low"
                        dir_text = "üzerine çıktı" if z_val > 0 else "altına düştü"
                        chosen_baseline = round(baseline_mean, 1) if baseline_mean is not None else None
                        baseline_label = "Son 60 sn Ort."
                        base_str = f" (Normal ort: {chosen_baseline:.1f} {unit})" if chosen_baseline is not None else ""
                        thresh_reason = (
                            f"{metric_label} son 60 sn ortalamasının {dir_text}: "
                            f"{measured_val:.1f} {unit}{base_str}"
                        )
                        thresh_label = "İstatistiksel sınır: |z| > 3.2"

                    prev_sev = districts_map[did]["status"]
                    if SEVERITY_ORDER.get(severity, 0) > SEVERITY_ORDER.get(prev_sev, 0):
                        districts_map[did]["status"] = severity

                    cooldown_key = (did, dom)
                    now_wall = time.monotonic()
                    last_sent = LAST_ALERT_AT.get(cooldown_key, 0.0)
                    if bool(row["has_injected"]) or (now_wall - last_sent >= ALERT_COOLDOWN_SEC):
                        if len(LAST_ALERT_AT) < MAX_STATE_KEYS or cooldown_key in LAST_ALERT_AT:
                            LAST_ALERT_AT[cooldown_key] = now_wall
                        CUMULATIVE_ALERTS += 1
                        new_alerts.append(
                            {
                                "alert_id": uuid.uuid4().hex[:10],
                                "timestamp": ts_iso,
                                "district_id": did,
                                "district_name": dname,
                                "domain": dom,
                                "metric_label": metric_label,
                                "unit": unit,
                                "severity": severity,
                                "method": method,
                                "z_score": z_val,
                                "value": measured_val,
                                "baseline_value": chosen_baseline,
                                "baseline_label": baseline_label,
                                "direction": direction,
                                "threshold_label": thresh_label,
                                "message": thresh_reason,
                                "injected": bool(row["has_injected"]),
                            }
                        )

            batch_proc_ms = round((time.monotonic() - batch_start) * 1000.0, 1)
            throughput_eps = round(row_count / elapsed_since_last, 1)
            p50_ms = round(sum(all_p50) / len(all_p50), 1) if all_p50 else 0.0
            p95_ms = round(max(all_p95), 1) if all_p95 else 0.0
            avg_lat_ms = round(sum(all_avg_lat) / len(all_avg_lat), 1) if all_avg_lat else 0.0

            trigger_budget_ms = TRIGGER_SEC * 1000.0
            if batch_proc_ms > trigger_budget_ms * 1.15 or row_count >= int(MAX_OFFSETS_PER_TRIGGER * 0.95):
                bp_status = "backpressure"
            elif batch_proc_ms > trigger_budget_ms * 0.80:
                bp_status = "elevated"
            else:
                bp_status = "healthy"

            driver_rss_mb = _process_tree_rss_mb()

            # Cold Path: disk-safe partitioned Parquet archiving (isolated from hot path)
            if batch_id % ARCHIVE_EVERY_N_BATCHES == 0:
                try:
                    LAST_LAKE_SIZE_MB = archive_batch_safely(batch_with_latency, batch_id)
                except Exception as exc:
                    logger.warning("Non-fatal cold-path archive error on batch #%d: %s", batch_id, exc)

            # Hot Path: resilient Redis state & Pub/Sub broadcast
            try:
                raw_target = r.get(REDIS_RATE_KEY)
                target_rate = int(raw_target) if isinstance(raw_target, str) and raw_target else 100

                perf_snapshot = {
                    "batch_id": batch_id,
                    "timestamp": ts_iso,
                    "target_rate_eps": target_rate,
                    "throughput_eps": throughput_eps,
                    "batch_events": row_count,
                    "avg_latency_ms": avg_lat_ms,
                    "p50_latency_ms": p50_ms,
                    "p95_latency_ms": p95_ms,
                    "spark_batch_ms": batch_proc_ms,
                    "cumulative_events": CUMULATIVE_EVENTS,
                    "cumulative_alerts": CUMULATIVE_ALERTS,
                    "backpressure_status": bp_status,
                    "driver_rss_mb": driver_rss_mb,
                    "lake_size_mb": LAST_LAKE_SIZE_MB,
                    "max_offsets_per_trigger": MAX_OFFSETS_PER_TRIGGER,
                }

                ts_point = {
                    "time": london_now().strftime("%H:%M:%S"),
                    "timestamp": ts_iso,
                    "traffic": domains_summary.get("traffic", {}).get("avg_value"),
                    "weather": domains_summary.get("weather", {}).get("avg_value"),
                    "air_quality": domains_summary.get("air_quality", {}).get("avg_value"),
                    "energy": domains_summary.get("energy", {}).get("avg_value"),
                    "throughput_eps": throughput_eps,
                    "latency_ms": p50_ms,
                }

                pipe = r.pipeline()
                pipe.rpush(REDIS_TIMESERIES_KEY, json.dumps(ts_point))
                pipe.ltrim(REDIS_TIMESERIES_KEY, -60, -1)
                for al in new_alerts[:6]:
                    pipe.lpush(REDIS_ALERTS_KEY, json.dumps(al))
                pipe.ltrim(REDIS_ALERTS_KEY, 0, 39)

                rate_bucket_key = f"{REDIS_TELEMETRY_PREFIX}{target_rate}"
                pipe.rpush(
                    rate_bucket_key,
                    json.dumps(
                        {
                            "throughput_eps": throughput_eps,
                            "p50_latency_ms": p50_ms,
                            "p95_latency_ms": p95_ms,
                            "spark_batch_ms": batch_proc_ms,
                        }
                    ),
                )
                pipe.ltrim(rate_bucket_key, -30, -1)
                pipe.set(
                    REDIS_INFRA_SPARK_KEY,
                    json.dumps(
                        {
                            "timestamp": ts_iso,
                            "status": bp_status,
                            "batch_id": batch_id,
                            "batch_events": row_count,
                            "throughput_eps": throughput_eps,
                            "spark_batch_ms": batch_proc_ms,
                            "trigger_budget_ms": trigger_budget_ms,
                            "driver_rss_mb": driver_rss_mb,
                            "lake_size_mb": LAST_LAKE_SIZE_MB,
                            "lake_max_size_mb": LAKE_MAX_SIZE_MB,
                            "max_offsets_per_trigger": MAX_OFFSETS_PER_TRIGGER,
                        }
                    ),
                    ex=30,
                )
                pipe.set(
                    REDIS_STATE_SNAPSHOT_KEY,
                    json.dumps(
                        {
                            "cumulative_events": CUMULATIVE_EVENTS,
                            "cumulative_alerts": CUMULATIVE_ALERTS,
                            "window_history": {
                                f"{k[0]}|{k[1]}": list(v) for k, v in WINDOW_HISTORY.items()
                            },
                        },
                        separators=(",", ":"),
                    ),
                    ex=86400,
                )
                pipe.execute()

                raw_alerts = r.lrange(REDIS_ALERTS_KEY, 0, 24)
                raw_ts = r.lrange(REDIS_TIMESERIES_KEY, -40, -1)
                recent_alerts = [
                    al
                    for al in (json.loads(x) for x in (raw_alerts if isinstance(raw_alerts, list) else []))
                    if al.get("district_id") in districts_by_id
                ]
                timeseries = [json.loads(x) for x in (raw_ts if isinstance(raw_ts, list) else [])]
                # Merge batch updates into bounded latest per-district state so all 32 regions stay present
                for did_k, d_entry in districts_map.items():
                    prev_entry = LATEST_DISTRICTS_STATE.get(did_k)
                    if prev_entry is None:
                        LATEST_DISTRICTS_STATE[did_k] = d_entry
                    else:
                        merged_metrics = dict(prev_entry.get("metrics", {}))
                        merged_metrics.update(d_entry["metrics"])
                        has_traffic_update = "traffic" in d_entry["metrics"]
                        LATEST_DISTRICTS_STATE[did_k] = {
                            "district_id": d_entry["district_id"],
                            "district_name": d_entry["district_name"],
                            "zone_type": d_entry["zone_type"],
                            "tfl_corridors": d_entry["tfl_corridors"],
                            "tfl_disruptions": (
                                d_entry["tfl_disruptions"]
                                if has_traffic_update
                                else prev_entry.get("tfl_disruptions", 0)
                            ),
                            "metrics": merged_metrics,
                            "status": d_entry["status"],
                        }

                ordered_districts = sorted(
                    LATEST_DISTRICTS_STATE.values(),
                    key=lambda item: district_order.get(item["district_id"], 999),
                )

                live_payload = {
                    "timestamp": ts_iso,
                    "domains": domains_summary,
                    "districts": ordered_districts,
                    "performance": perf_snapshot,
                    "timeseries": timeseries,
                    "alerts": recent_alerts,
                    "new_alerts": new_alerts[:6],
                }

                encoded = json.dumps(live_payload, separators=(",", ":"))
                r.set(REDIS_LATEST_KEY, encoded)
                r.publish(REDIS_PUBSUB_CHANNEL, encoded)
            except Exception as exc:
                logger.warning("Non-fatal Redis sink error on batch #%d: %s", batch_id, exc)

            # Persistent Relational Store: MSSQL (isolated from hot path)
            if mssql_sink.is_available() and (batch_id % MSSQL_WRITE_EVERY_N_BATCHES == 0 or new_alerts):
                try:
                    perf_for_db = {
                        "target_rate_eps": target_rate if "target_rate" in locals() else 100,
                        "throughput_eps": throughput_eps,
                        "batch_events": row_count,
                        "avg_latency_ms": avg_lat_ms,
                        "p50_latency_ms": p50_ms,
                        "p95_latency_ms": p95_ms,
                        "spark_batch_ms": batch_proc_ms,
                        "cumulative_events": CUMULATIVE_EVENTS,
                        "cumulative_alerts": CUMULATIVE_ALERTS,
                        "backpressure_status": bp_status,
                        "driver_rss_mb": driver_rss_mb,
                        "lake_size_mb": LAST_LAKE_SIZE_MB,
                    }
                    mssql_sink.save_batch(
                        batch_id=batch_id,
                        ts_iso=ts_iso,
                        domains_summary=domains_summary,
                        districts_map=districts_map,
                        new_alerts=new_alerts,
                        perf_snapshot=perf_for_db,
                        window_history=WINDOW_HISTORY,
                    )
                except Exception as exc:
                    logger.warning("Non-fatal MSSQL sink error on batch #%d: %s", batch_id, exc)

            _touch_health()
            logger.info(
                "London Batch #%d | events=%d | tp=%.1f ev/s | p50=%.1fms | p95=%.1fms | spark=%.1fms | rss=%.1fMB | bp=%s | alerts=%d",
                batch_id,
                row_count,
                throughput_eps,
                p50_ms,
                p95_ms,
                batch_proc_ms,
                driver_rss_mb,
                bp_status,
                len(new_alerts),
            )
        finally:
            batch_with_latency.unpersist(blocking=False)

    def start_query():
        return (
            enriched_stream.writeStream.foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .trigger(processingTime=f"{TRIGGER_SEC} seconds")
            .start()
        )

    logger.info(
        "Starting resilient London Spark Structured Streaming | kafka=%s | trigger=%ds | maxOffsets=%d | shuffle=%s | lakeMax=%.0fMB",
        KAFKA_BOOTSTRAP,
        TRIGGER_SEC,
        MAX_OFFSETS_PER_TRIGGER,
        SPARK_SHUFFLE_PARTITIONS,
        LAKE_MAX_SIZE_MB,
    )

    try:
        query = start_query()
    except Exception as exc:
        logger.warning("Initial checkpoint incompatible (%s); resetting checkpoint dir and retrying...", exc)
        shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)
        Path(CHECKPOINT_DIR).mkdir(parents=True, exist_ok=True)
        query = start_query()

    def _shutdown(signum: int, _frame: Any) -> None:
        logger.info("Received signal %s; stopping Spark streaming query gracefully...", signum)
        try:
            query.stop()
        except Exception:
            pass

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        query.awaitTermination()
    finally:
        mssql_sink.close()
        spark.stop()
        logger.info("Spark Structured Streaming processor exited cleanly.")


if __name__ == "__main__":
    main()
