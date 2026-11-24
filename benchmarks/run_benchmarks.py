"""Greater London Smart City — High-Scale, OOM-Proof Big Data Benchmark Suite.

Hardened for large datasets (100K -> 1M -> 5M -> 10M -> 25M -> 50M+ records):
1. Dynamic partition scaling (~400K rows/partition, up to 512 partitions) + Adaptive Query Execution (AQE)
   so per-task heap remains strictly bounded regardless of dataset scale.
2. Out-of-core spill tuning (Kryo serializer, 64 MB Parquet row-group cap, maxRecordsPerFile,
   dedicated spill directory on mounted volume, inter-scale cache clear & JVM GC).
3. Sequential format execution + immediate disk cleanup + pre-flight free disk-space guard
   (prevents ENOSPC crashes when raw CSV size exceeds free volume space).
4. Full process-tree RSS tracking (Python + JVM Spark child processes) and atomic incremental
   checkpointing of benchmark_summary.json after each completed scale.
5. Configurable multi-rate live streaming benchmark (default: 100 / 500 / 2,000 / 5,000 ev/s).
"""

from __future__ import annotations

import json
import logging
import math
import os
import shutil
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import psutil
import redis
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("benchmarks")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RESULTS_DIR = Path(os.getenv("RESULTS_DIR", "/app/results"))
CHARTS_DIR = RESULTS_DIR / "charts"
TMP_LAKE = Path(os.getenv("BENCHMARK_TMP_DIR", "/tmp/benchmark_lake"))
SPILL_DIR = TMP_LAKE / "spark_spill"
METADATA_PATH = Path(os.getenv("METADATA_PATH", "/data/metadata/london_districts.json"))

SCALES = [int(x.strip()) for x in os.getenv("BENCHMARK_SCALES", "100000,1000000,5000000").split(",") if x.strip()]
STREAM_RATES = [int(x.strip()) for x in os.getenv("BENCHMARK_STREAM_RATES", "100,500,2000,5000").split(",") if x.strip()]
ROWS_PER_PARTITION = max(100_000, int(os.getenv("BENCHMARK_ROWS_PER_PARTITION", "400000")))
DISK_SAFETY_MARGIN_MB = 512.0

REDIS_RATE_KEY = "smartcity:control:rate"
REDIS_TELEMETRY_PREFIX = "smartcity:rate_telemetry:"


def dir_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    total = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    return round(total / (1024 * 1024), 2)


def free_disk_mb(path: Path) -> float:
    path.mkdir(parents=True, exist_ok=True)
    return round(shutil.disk_usage(path).free / (1024 * 1024), 1)


def process_tree_rss_mb() -> float:
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


def scale_label(n: int) -> str:
    if n >= 1_000_000_000:
        val = n / 1_000_000_000
        return f"{val:g}B Kayıt"
    if n >= 1_000_000:
        val = n / 1_000_000
        return f"{val:g}M Kayıt"
    if n >= 1_000:
        val = n / 1_000
        return f"{val:g}K Kayıt"
    return f"{n} Kayıt"


def compute_partitions(n_rows: int) -> int:
    """Scale partition count proportionally with row count to bound per-task JVM heap."""
    return max(8, min(512, math.ceil(n_rows / ROWS_PER_PARTITION)))


def write_summary_atomic(format_res: list[dict[str, Any]], stream_res: list[dict[str, Any]]) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "city": "London, United Kingdom",
        "format_experiments": format_res,
        "streaming_experiments": stream_res,
    }
    out_file = RESULTS_DIR / "benchmark_summary.json"
    tmp_file = RESULTS_DIR / ".benchmark_summary.json.tmp"
    tmp_file.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp_file.replace(out_file)
    return out_file


def generate_synthetic_df(spark: SparkSession, n_rows: int, district_ids: list[str]) -> tuple[DataFrame, int]:
    """Distributed PySpark generation with dynamic partitioning for constant O(1) task memory."""
    num_parts = compute_partitions(n_rows)
    num_districts = max(1, len(district_ids))
    domains_arr = F.array(
        F.lit("traffic"),
        F.lit("weather"),
        F.lit("air_quality"),
        F.lit("energy"),
    )
    districts_arr = F.array(*(F.lit(did) for did in district_ids))

    base = spark.range(0, n_rows, numPartitions=num_parts)
    df = (
        base.withColumn("domain", domains_arr[(F.col("id") % 4).cast("int")])
        .withColumn("district_id", districts_arr[(F.col("id") % num_districts).cast("int")])
        .withColumn(
            "sensor_id",
            F.concat(F.lit("LON-"), F.upper(F.substring("domain", 1, 3)), F.lit("-"), (F.col("id") % (num_districts * 4))),
        )
        .withColumn(
            "value",
            F.when(F.col("domain") == "traffic", F.round(F.lit(26.0) + F.randn(42) * 6.5, 2))
            .when(F.col("domain") == "weather", F.round(F.lit(9.5) + F.randn(43) * 3.0, 2))
            .when(F.col("domain") == "air_quality", F.round(F.abs(F.lit(28.0) + F.randn(44) * 10.0), 2))
            .otherwise(F.round(F.lit(150.0) + F.randn(45) * 30.0, 2)),
        )
        .withColumn("secondary_val", F.round(F.abs(F.lit(35.0) + F.randn(46) * 15.0), 2))
        .withColumn("event_ts", F.from_unixtime(F.lit(1763900000) + (F.col("id") % 86400)))
        .withColumn("latency_ms", F.round(F.abs(F.lit(140.0) + F.randn(47) * 35.0), 1))
        .select("id", "sensor_id", "district_id", "domain", "value", "secondary_val", "event_ts", "latency_ms")
    )
    return df, num_parts


def run_analytical_query(df: DataFrame, broadcast_districts: DataFrame) -> int:
    """Realistic Big Data workload: column pruning + predicate pushdown + Broadcast Join + groupBy."""
    result = (
        df.filter(F.col("domain").isin("traffic", "air_quality", "energy"))
        .filter(F.col("value") > 10.0)
        .join(broadcast_districts, on="district_id", how="inner")
        .groupBy("district_name", "zone_type", "domain")
        .agg(
            F.count("*").alias("cnt"),
            F.avg("value").alias("avg_val"),
            F.max("value").alias("max_val"),
            F.stddev("value").alias("std_val"),
        )
        .collect()
    )
    return len(result)


def _cleanup_data_dirs() -> None:
    if not TMP_LAKE.exists():
        return
    for item in TMP_LAKE.iterdir():
        if item.name == "spark_spill":
            continue
        if item.is_dir():
            shutil.rmtree(item, ignore_errors=True)
        else:
            item.unlink(missing_ok=True)


def warmup_spark(spark: SparkSession, broadcast_districts: DataFrame, district_ids: list[str]) -> None:
    """Run a lightweight 10K-row CSV+Parquet warm-up so JVM/Catalyst JIT doesn't skew the first scale."""
    warm_csv = TMP_LAKE / "_warmup_csv"
    warm_pq = TMP_LAKE / "_warmup_pq"
    logger.info("Running 10K-row JVM/Catalyst JIT warm-up before timed format benchmarks...")
    try:
        warm_df, _ = generate_synthetic_df(spark, 10_000, district_ids)
        warm_schema = warm_df.schema
        warm_df.write.mode("overwrite").option("header", "true").csv(str(warm_csv))
        run_analytical_query(
            spark.read.option("header", "true").schema(warm_schema).csv(str(warm_csv)),
            broadcast_districts,
        )
        warm_df.write.mode("overwrite").option("compression", "snappy").parquet(str(warm_pq))
        run_analytical_query(spark.read.parquet(str(warm_pq)), broadcast_districts)
        spark.catalog.clearCache()
    finally:
        shutil.rmtree(warm_csv, ignore_errors=True)
        shutil.rmtree(warm_pq, ignore_errors=True)


def run_format_benchmarks(
    spark: SparkSession, broadcast_districts: DataFrame, district_ids: list[str]
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    TMP_LAKE.mkdir(parents=True, exist_ok=True)
    _cleanup_data_dirs()
    warmup_spark(spark, broadcast_districts, district_ids)

    for n_rows in SCALES:
        label = scale_label(n_rows)
        df, num_parts = generate_synthetic_df(spark, n_rows, district_ids)
        csv_schema = df.schema
        csv_path = TMP_LAKE / f"csv_{n_rows}"
        pq_path = TMP_LAKE / f"parquet_{n_rows}"

        avail_mb = free_disk_mb(TMP_LAKE)
        est_csv_mb = round((n_rows * 90.0) / (1024 * 1024), 1)
        est_pq_mb = round((n_rows * 18.0) / (1024 * 1024), 1)

        logger.info(
            "=== Format Benchmark: %s | partitions=%d | free_disk=%.1f MB (est CSV=%.1f MB, Parquet=%.1f MB) ===",
            label,
            num_parts,
            avail_mb,
            est_csv_mb,
            est_pq_mb,
        )

        # Tune shuffle partitions dynamically for this scale
        spark.conf.set("spark.sql.shuffle.partitions", str(min(64, max(8, num_parts))))

        # 1. CSV phase (sequential + immediate cleanup so CSV and Parquet never occupy disk simultaneously)
        csv_entry: dict[str, Any]
        if avail_mb >= est_csv_mb + DISK_SAFETY_MARGIN_MB:
            try:
                t0 = time.perf_counter()
                df.write.mode("overwrite").option("header", "true").csv(str(csv_path))
                csv_write_sec = round(time.perf_counter() - t0, 3)
                csv_mb = dir_size_mb(csv_path)

                t0 = time.perf_counter()
                csv_df = spark.read.option("header", "true").schema(csv_schema).csv(str(csv_path))
                run_analytical_query(csv_df, broadcast_districts)
                csv_query_sec = round(time.perf_counter() - t0, 3)

                csv_entry = {
                    "write_time_sec": csv_write_sec,
                    "query_time_sec": csv_query_sec,
                    "disk_size_mb": csv_mb,
                    "skipped": False,
                }
            finally:
                shutil.rmtree(csv_path, ignore_errors=True)
        else:
            logger.warning(
                "Skipping CSV at %s: needs ~%.1f MB + %.0f MB safety margin, only %.1f MB free",
                label,
                est_csv_mb,
                DISK_SAFETY_MARGIN_MB,
                avail_mb,
            )
            csv_entry = {
                "write_time_sec": None,
                "query_time_sec": None,
                "disk_size_mb": None,
                "skipped": True,
                "skipped_reason": f"insufficient_disk ({avail_mb:.0f} MB free)",
            }

        # 2. Parquet (Snappy) phase + immediate cleanup
        avail_mb = free_disk_mb(TMP_LAKE)
        if avail_mb < est_pq_mb + 256.0:
            logger.error("Insufficient disk for Parquet at %s (%.1f MB free); stopping format suite cleanly.", label, avail_mb)
            break

        try:
            t0 = time.perf_counter()
            df.write.mode("overwrite").option("compression", "snappy").parquet(str(pq_path))
            pq_write_sec = round(time.perf_counter() - t0, 3)
            pq_mb = dir_size_mb(pq_path)

            t0 = time.perf_counter()
            pq_df = spark.read.parquet(str(pq_path))
            run_analytical_query(pq_df, broadcast_districts)
            pq_query_sec = round(time.perf_counter() - t0, 3)
        finally:
            shutil.rmtree(pq_path, ignore_errors=True)

        rss_mb = process_tree_rss_mb()

        if not csv_entry["skipped"] and csv_entry["disk_size_mb"] and csv_entry["query_time_sec"]:
            savings_pct = round((1.0 - (pq_mb / max(csv_entry["disk_size_mb"], 0.01))) * 100.0, 1)
            speedup_x = round(csv_entry["query_time_sec"] / max(pq_query_sec, 0.01), 2)
        else:
            savings_pct = None
            speedup_x = None

        entry = {
            "rows": n_rows,
            "scale_label": label,
            "partitions": num_parts,
            "csv": csv_entry,
            "parquet": {
                "write_time_sec": pq_write_sec,
                "query_time_sec": pq_query_sec,
                "disk_size_mb": pq_mb,
            },
            "disk_savings_pct": savings_pct,
            "query_speedup_x": speedup_x,
            "driver_rss_mb": rss_mb,
        }
        results.append(entry)

        logger.info(
            "Scale %s (parts=%d, rss=%.1fMB) | CSV: %s MB / %ss | Parquet: %.2f MB / %.3fs | Savings: %s%% | Speedup: %sx",
            label,
            num_parts,
            rss_mb,
            csv_entry["disk_size_mb"] if not csv_entry["skipped"] else "SKIP",
            csv_entry["query_time_sec"] if not csv_entry["skipped"] else "SKIP",
            pq_mb,
            pq_query_sec,
            savings_pct if savings_pct is not None else "N/A",
            speedup_x if speedup_x is not None else "N/A",
        )

        # Save incremental checkpoint so completed scales are preserved even if a gigantic scale is interrupted
        write_summary_atomic(results, [])

        # Reclaim Spark & JVM heap cleanly before moving to the next larger scale
        spark.catalog.clearCache()
        try:
            spark._jvm.java.lang.System.gc()  # type: ignore[union-attr]
        except Exception:
            pass

    return results


def run_streaming_rate_benchmarks() -> list[dict[str, Any]]:
    """Drives the live Kafka + Spark Streaming pipeline across configured target rates."""
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        r.ping()
    except Exception as exc:
        logger.warning("Redis unavailable (%s); skipping live streaming rate benchmark.", exc)
        return []

    raw_orig = r.get(REDIS_RATE_KEY)
    original_rate = raw_orig if isinstance(raw_orig, str) and raw_orig else "100"
    results: list[dict[str, Any]] = []

    try:
        for rate in STREAM_RATES:
            logger.info("=== Measuring Live Streaming Pipeline at %d ev/s ===", rate)
            key = f"{REDIS_TELEMETRY_PREFIX}{rate}"
            r.delete(key)
            r.set(REDIS_RATE_KEY, str(rate))

            # Warm-up 4s + measure 12s (~5-6 Spark micro-batches)
            time.sleep(16)
            raw_samples = r.lrange(key, 0, -1)
            samples = [json.loads(x) for x in (raw_samples if isinstance(raw_samples, list) else [])]

            if len(samples) > 2:
                samples = samples[1:]

            if samples:
                avg_tp = round(sum(s["throughput_eps"] for s in samples) / len(samples), 1)
                avg_p50 = round(sum(s["p50_latency_ms"] for s in samples) / len(samples), 1)
                avg_p95 = round(sum(s["p95_latency_ms"] for s in samples) / len(samples), 1)
                avg_spark = round(sum(s["spark_batch_ms"] for s in samples) / len(samples), 1)
                results.append(
                    {
                        "target_rate_eps": rate,
                        "batches_measured": len(samples),
                        "avg_throughput_eps": avg_tp,
                        "avg_p50_latency_ms": avg_p50,
                        "avg_p95_latency_ms": avg_p95,
                        "avg_spark_batch_ms": avg_spark,
                    }
                )
                logger.info(
                    "Rate %d ev/s | Measured=%.1f ev/s | p50=%.1fms | p95=%.1fms | spark_batch=%.1fms",
                    rate,
                    avg_tp,
                    avg_p50,
                    avg_p95,
                    avg_spark,
                )
            else:
                logger.warning("No streaming telemetry received at %d ev/s (is spark-processor running?)", rate)
    finally:
        try:
            r.set(REDIS_RATE_KEY, original_rate)
        except Exception:
            pass

    return results


def generate_charts(format_res: list[dict[str, Any]], stream_res: list[dict[str, Any]]) -> None:
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    plt.rcParams.update(
        {
            "figure.facecolor": "#FFFFFF",
            "axes.facecolor": "#FFFFFF",
            "axes.edgecolor": "#E2E8F0",
            "axes.grid": True,
            "grid.color": "#F1F5F9",
            "grid.linestyle": "--",
            "font.size": 10,
        }
    )

    if format_res:
        labels = [e["scale_label"] for e in format_res]
        x = list(range(len(labels)))
        width = 0.34

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8), dpi=160)

        ax1.bar([i - width / 2 for i in x], [e["csv"]["query_time_sec"] or 0.0 for e in format_res], width, label="CSV", color="#F59E0B")
        ax1.bar([i + width / 2 for i in x], [e["parquet"]["query_time_sec"] for e in format_res], width, label="Parquet (Snappy)", color="#2563EB")
        ax1.set_title("PySpark Sorgu + Aggregation Süresi (sn)", fontweight="bold", color="#0F172A")
        ax1.set_xticks(x)
        ax1.set_xticklabels(labels)
        ax1.set_ylabel("Süre (saniye)")
        ax1.legend()

        ax2.bar([i - width / 2 for i in x], [e["csv"]["disk_size_mb"] or 0.0 for e in format_res], width, label="CSV", color="#E11D48")
        ax2.bar([i + width / 2 for i in x], [e["parquet"]["disk_size_mb"] for e in format_res], width, label="Parquet (Snappy)", color="#059669")
        ax2.set_title("Disk Boyutu Karşılaştırması (MB)", fontweight="bold", color="#0F172A")
        ax2.set_xticks(x)
        ax2.set_xticklabels(labels)
        ax2.set_ylabel("Disk Boyutu (MB)")
        ax2.legend()

        plt.tight_layout()
        plt.savefig(CHARTS_DIR / "csv_vs_parquet_performance.png")
        plt.close(fig)

    if stream_res:
        labels = [f"{s['target_rate_eps']} ev/sn" for s in stream_res]
        x = list(range(len(labels)))
        width = 0.34

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8), dpi=160)

        ax1.bar(x, [s["avg_throughput_eps"] for s in stream_res], width=0.45, color="#0891B2")
        ax1.set_title("Ölçülen Sürdürülebilir Throughput (ev/sn)", fontweight="bold", color="#0F172A")
        ax1.set_xticks(x)
        ax1.set_xticklabels(labels)
        ax1.set_ylabel("Olay / Saniye")

        ax2.bar([i - width / 2 for i in x], [s["avg_p50_latency_ms"] for s in stream_res], width, label="p50 Gecikme (ms)", color="#2563EB")
        ax2.bar([i + width / 2 for i in x], [s["avg_p95_latency_ms"] for s in stream_res], width, label="p95 Gecikme (ms)", color="#E11D48")
        ax2.set_title("End-to-End Akış Gecikmesi (ms)", fontweight="bold", color="#0F172A")
        ax2.set_xticks(x)
        ax2.set_xticklabels(labels)
        ax2.set_ylabel("Gecikme (ms)")
        ax2.legend()

        plt.tight_layout()
        plt.savefig(CHARTS_DIR / "streaming_rate_scaling.png")
        plt.close(fig)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    SPILL_DIR.mkdir(parents=True, exist_ok=True)

    def _on_signal(signum: int, _frame: Any) -> None:
        logger.warning("Benchmark received signal %s; cleaning up temp files...", signum)
        _cleanup_data_dirs()
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    spark = (
        SparkSession.builder.appName("LondonSmartCityBenchmarks")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.2")
        .config("spark.local.dir", str(SPILL_DIR))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.parquet.block.size", str(64 * 1024 * 1024))
        .config("spark.sql.files.maxRecordsPerFile", "1000000")
        .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    format_res: list[dict[str, Any]] = []
    try:
        with open(METADATA_PATH, "r", encoding="utf-8") as f:
            districts_meta = json.load(f)
        flat_districts = [
            {"district_id": d["district_id"], "district_name": d["district_name"], "zone_type": d["zone_type"]}
            for d in districts_meta
        ]
        district_ids = [d["district_id"] for d in districts_meta]
        broadcast_districts = F.broadcast(spark.createDataFrame(flat_districts))

        format_res = run_format_benchmarks(spark, broadcast_districts, district_ids)
    finally:
        spark.stop()
        _cleanup_data_dirs()
        shutil.rmtree(SPILL_DIR, ignore_errors=True)

    stream_res = run_streaming_rate_benchmarks()
    generate_charts(format_res, stream_res)

    out_file = write_summary_atomic(format_res, stream_res)
    logger.info("Saved benchmark summary to %s", out_file)


if __name__ == "__main__":
    main()
