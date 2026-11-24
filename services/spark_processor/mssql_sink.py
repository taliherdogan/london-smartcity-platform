"""Persistent Microsoft SQL Server (MSSQL) Sink for Spark Structured Streaming.

Stores all aggregated city snapshots, per-borough measurements, anomaly alerts,
streaming telemetry, and platform state in MSSQL (`SmartCityDB`) so data survives
container recreations and can be queried directly from SSMS / Azure Data Studio
or via the backend API.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    import pymssql
except ImportError:  # pragma: no cover - optional in slim unit-test environments
    pymssql = None  # type: ignore[assignment]

logger = logging.getLogger("mssql_sink")

_SAFE_DB_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def _parse_iso_utc(ts_iso: str) -> datetime:
    cleaned = ts_iso.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _utc_to_london_naive(dt_utc_naive: datetime) -> datetime:
    dt_utc = dt_utc_naive.replace(tzinfo=timezone.utc)
    year = dt_utc.year
    mar31 = datetime(year, 3, 31, 1, 0, tzinfo=timezone.utc)
    bst_start = mar31 - timedelta(days=(mar31.weekday() + 1) % 7)
    oct31 = datetime(year, 10, 31, 1, 0, tzinfo=timezone.utc)
    bst_end = oct31 - timedelta(days=(oct31.weekday() + 1) % 7)
    offset = timedelta(hours=1) if bst_start <= dt_utc < bst_end else timedelta(0)
    return (dt_utc + offset).replace(tzinfo=None)


class MSSQLSink:
    """Resilient, non-fatal MSSQL persistence sink for Spark micro-batches."""

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        retention_days: int | None = None,
    ) -> None:
        raw_enabled = os.getenv("MSSQL_ENABLED", "true").lower() in ("1", "true", "yes")
        self.enabled = raw_enabled if enabled is None else enabled
        self.host = host or os.getenv("MSSQL_HOST", "mssql")
        self.port = port if port is not None else int(os.getenv("MSSQL_PORT", "1433"))
        self.database = database or os.getenv("MSSQL_DATABASE", "SmartCityDB")
        self.user = user or os.getenv("MSSQL_USER", "sa")
        self.password = password or os.getenv("MSSQL_PASSWORD", "SmartCity#2026!")
        self.retention_days = (
            retention_days
            if retention_days is not None
            else max(0, int(os.getenv("MSSQL_RETENTION_DAYS", "90")))
        )

        if not _SAFE_DB_RE.match(self.database):
            raise ValueError(f"Unsafe MSSQL database name: {self.database!r}")

        self._conn: Any = None
        self._schema_ready = False
        self._batches_written = 0

    def is_available(self) -> bool:
        return self.enabled and pymssql is not None

    def _connect(self, database: str | None = None, timeout: int = 10, autocommit: bool = False) -> Any:
        if pymssql is None:
            raise RuntimeError("pymssql is not installed")
        return pymssql.connect(
            server=self.host,
            port=str(self.port),
            user=self.user,
            password=self.password,
            database=database or self.database,
            login_timeout=timeout,
            timeout=timeout,
            autocommit=autocommit,
            charset="UTF-8",
        )

    def _get_conn(self) -> Any:
        if self._conn is None:
            if not self._schema_ready:
                self.ensure_schema(retries=1, delay_sec=1.0)
            self._conn = self._connect(self.database, timeout=10, autocommit=False)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def ensure_schema(self, retries: int = 15, delay_sec: float = 2.0) -> bool:
        """Create SmartCityDB, tables, indexes, and SSMS views idempotently."""
        if not self.is_available():
            logger.info("MSSQL sink disabled or pymssql unavailable; skipping schema init.")
            return False

        last_err: Exception | None = None
        for attempt in range(1, retries + 1):
            master_conn = None
            db_conn = None
            try:
                master_conn = self._connect("master", timeout=10, autocommit=True)
                with master_conn.cursor() as cur:
                    cur.execute(
                        f"IF DB_ID(N'{self.database}') IS NULL CREATE DATABASE [{self.database}];"
                    )
                master_conn.close()
                master_conn = None

                db_conn = self._connect(self.database, timeout=15, autocommit=True)
                with db_conn.cursor() as cur:
                    for stmt in _DDL_STATEMENTS:
                        cur.execute(stmt)
                db_conn.close()
                db_conn = None

                self._schema_ready = True
                logger.info(
                    "MSSQL schema ready | server=%s:%d | db=%s",
                    self.host,
                    self.port,
                    self.database,
                )
                return True
            except Exception as exc:
                last_err = exc
                for c in (master_conn, db_conn):
                    if c is not None:
                        try:
                            c.close()
                        except Exception:
                            pass
                if attempt < retries:
                    logger.info(
                        "Waiting for MSSQL (%s:%d) attempt %d/%d: %s",
                        self.host,
                        self.port,
                        attempt,
                        retries,
                        exc,
                    )
                    time.sleep(delay_sec)

        logger.warning("MSSQL schema init failed after %d attempts: %s", retries, last_err)
        return False

    def restore_state(self) -> dict[str, Any] | None:
        """Recover cumulative counters, Z-score windows, recent alerts & timeseries from MSSQL."""
        if not self.is_available():
            return None
        try:
            conn = self._get_conn()
            result: dict[str, Any] = {
                "cumulative_events": 0,
                "cumulative_alerts": 0,
                "window_history": {},
                "recent_alerts": [],
                "timeseries": [],
            }
            with conn.cursor(as_dict=True) as cur:
                cur.execute(
                    """
                    SELECT cumulative_events, cumulative_alerts, window_history_json
                    FROM dbo.platform_state
                    WHERE state_key = N'spark_streaming'
                    """
                )
                row = cur.fetchone()
                if row:
                    result["cumulative_events"] = max(0, int(row.get("cumulative_events") or 0))
                    result["cumulative_alerts"] = max(0, int(row.get("cumulative_alerts") or 0))
                    raw_win = row.get("window_history_json")
                    if isinstance(raw_win, str) and raw_win:
                        try:
                            parsed_win = json.loads(raw_win)
                            if isinstance(parsed_win, dict):
                                result["window_history"] = parsed_win
                        except Exception:
                            pass

                cur.execute(
                    """
                    SELECT TOP 40
                        alert_id, recorded_at, district_id, district_name, domain,
                        metric_label, unit, severity, method, z_score,
                        measured_value, baseline_value, threshold_label, message, injected
                    FROM dbo.anomaly_alerts
                    ORDER BY recorded_at DESC, id DESC
                    """
                )
                alerts_out: list[dict[str, Any]] = []
                for al in cur.fetchall() or []:
                    rec_at = al["recorded_at"]
                    ts_iso = (
                        rec_at.replace(tzinfo=timezone.utc).isoformat()
                        if isinstance(rec_at, datetime)
                        else str(rec_at)
                    )
                    alerts_out.append(
                        {
                            "alert_id": al["alert_id"],
                            "timestamp": ts_iso,
                            "district_id": al["district_id"],
                            "district_name": al["district_name"],
                            "domain": al["domain"],
                            "metric_label": al.get("metric_label") or "",
                            "unit": al.get("unit") or "",
                            "severity": al["severity"],
                            "method": al.get("method") or "",
                            "z_score": float(al["z_score"]) if al.get("z_score") is not None else 0.0,
                            "value": float(al["measured_value"]) if al.get("measured_value") is not None else 0.0,
                            "baseline_value": (
                                float(al["baseline_value"]) if al.get("baseline_value") is not None else None
                            ),
                            "baseline_label": "Beklenen Normal",
                            "direction": "high",
                            "threshold_label": al.get("threshold_label") or "",
                            "message": al.get("message") or "",
                            "injected": bool(al.get("injected")),
                        }
                    )
                result["recent_alerts"] = alerts_out

                cur.execute(
                    """
                    SELECT TOP 60
                        t.recorded_at,
                        t.throughput_eps,
                        t.p50_latency_ms,
                        MAX(CASE WHEN s.domain = N'traffic' THEN s.avg_value END) AS traffic,
                        MAX(CASE WHEN s.domain = N'weather' THEN s.avg_value END) AS weather,
                        MAX(CASE WHEN s.domain = N'air_quality' THEN s.avg_value END) AS air_quality,
                        MAX(CASE WHEN s.domain = N'energy' THEN s.avg_value END) AS energy
                    FROM dbo.batch_telemetry t
                    LEFT JOIN dbo.domain_snapshots s
                        ON s.recorded_at = t.recorded_at AND s.batch_id = t.batch_id
                    GROUP BY t.recorded_at, t.batch_id, t.throughput_eps, t.p50_latency_ms
                    ORDER BY t.recorded_at DESC
                    """
                )
                ts_rows = list(reversed(cur.fetchall() or []))
                ts_out: list[dict[str, Any]] = []
                for tr in ts_rows:
                    rec_at = tr["recorded_at"]
                    if isinstance(rec_at, datetime):
                        lon_dt = _utc_to_london_naive(rec_at)
                        ts_iso = rec_at.replace(tzinfo=timezone.utc).isoformat()
                        time_label = lon_dt.strftime("%H:%M:%S")
                    else:
                        ts_iso = str(rec_at)
                        time_label = ts_iso[11:19]
                    ts_out.append(
                        {
                            "time": time_label,
                            "timestamp": ts_iso,
                            "traffic": tr.get("traffic"),
                            "weather": tr.get("weather"),
                            "air_quality": tr.get("air_quality"),
                            "energy": tr.get("energy"),
                            "throughput_eps": tr.get("throughput_eps"),
                            "latency_ms": tr.get("p50_latency_ms"),
                        }
                    )
                result["timeseries"] = ts_out

            return result
        except Exception as exc:
            logger.warning("Non-fatal MSSQL state restore warning: %s", exc)
            self.close()
            return None

    def save_batch(
        self,
        *,
        batch_id: int,
        ts_iso: str,
        domains_summary: dict[str, dict[str, Any]],
        districts_map: dict[str, dict[str, Any]],
        new_alerts: list[dict[str, Any]],
        perf_snapshot: dict[str, Any],
        window_history: dict[tuple[str, str], Any],
    ) -> bool:
        """Persist one micro-batch atomically to MSSQL; reconnect once on transient error."""
        if not self.is_available():
            return False

        rec_utc = _parse_iso_utc(ts_iso)
        lon_naive = _utc_to_london_naive(rec_utc)

        domain_rows = [
            (
                rec_utc,
                lon_naive,
                int(batch_id),
                dom,
                info.get("metric_name"),
                float(info["avg_value"]),
                float(info["min_value"]) if info.get("min_value") is not None else None,
                float(info["max_value"]) if info.get("max_value") is not None else None,
                info.get("unit"),
                int(info.get("event_count", 0)),
                info.get("data_source"),
                json.dumps(info.get("secondary", {}), separators=(",", ":")),
            )
            for dom, info in domains_summary.items()
        ]

        district_rows = []
        for did, entry in districts_map.items():
            dname = entry.get("district_name") or did
            ztype = entry.get("zone_type") or "urban"
            status = entry.get("status") or "normal"
            disruptions = int(entry.get("tfl_disruptions") or 0)
            for dom, m in (entry.get("metrics") or {}).items():
                district_rows.append(
                    (
                        rec_utc,
                        lon_naive,
                        int(batch_id),
                        did,
                        dname,
                        ztype,
                        dom,
                        float(m["avg"]),
                        float(m["min"]) if m.get("min") is not None else None,
                        float(m["max"]) if m.get("max") is not None else None,
                        int(m.get("count", 0)),
                        status,
                        disruptions if dom == "traffic" else 0,
                    )
                )

        alert_rows = [
            (
                str(al["alert_id"]),
                rec_utc,
                lon_naive,
                str(al["district_id"]),
                str(al["district_name"]),
                str(al["domain"]),
                str(al.get("metric_label") or ""),
                str(al.get("unit") or ""),
                str(al["severity"]),
                str(al.get("method") or ""),
                float(al["z_score"]) if al.get("z_score") is not None else None,
                float(al["value"]) if al.get("value") is not None else None,
                float(al["baseline_value"]) if al.get("baseline_value") is not None else None,
                str(al.get("threshold_label") or ""),
                str(al.get("message") or ""),
                1 if al.get("injected") else 0,
            )
            for al in new_alerts
        ]

        telemetry_row = (
            rec_utc,
            int(batch_id),
            int(perf_snapshot.get("target_rate_eps") or 0),
            float(perf_snapshot.get("throughput_eps") or 0.0),
            int(perf_snapshot.get("batch_events") or 0),
            float(perf_snapshot.get("avg_latency_ms") or 0.0),
            float(perf_snapshot.get("p50_latency_ms") or 0.0),
            float(perf_snapshot.get("p95_latency_ms") or 0.0),
            float(perf_snapshot.get("spark_batch_ms") or 0.0),
            int(perf_snapshot.get("cumulative_events") or 0),
            int(perf_snapshot.get("cumulative_alerts") or 0),
            str(perf_snapshot.get("backpressure_status") or "healthy"),
            float(perf_snapshot.get("driver_rss_mb") or 0.0),
            float(perf_snapshot.get("lake_size_mb") or 0.0),
        )

        win_json = json.dumps(
            {f"{k[0]}|{k[1]}": list(v) for k, v in window_history.items()},
            separators=(",", ":"),
        )
        cum_events = int(perf_snapshot.get("cumulative_events") or 0)
        cum_alerts = int(perf_snapshot.get("cumulative_alerts") or 0)

        for attempt in range(2):
            try:
                conn = self._get_conn()
                with conn.cursor() as cur:
                    if domain_rows:
                        cur.executemany(
                            """
                            INSERT INTO dbo.domain_snapshots (
                                recorded_at, london_time, batch_id, domain, metric_name,
                                avg_value, min_value, max_value, unit, event_count,
                                data_source, secondary_json
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            domain_rows,
                        )
                    if district_rows:
                        cur.executemany(
                            """
                            INSERT INTO dbo.district_measurements (
                                recorded_at, london_time, batch_id, district_id, district_name,
                                zone_type, domain, avg_value, min_value, max_value,
                                event_count, status, tfl_disruptions
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            district_rows,
                        )
                    for al_row in alert_rows:
                        cur.execute(
                            """
                            IF NOT EXISTS (SELECT 1 FROM dbo.anomaly_alerts WHERE alert_id = %s)
                            INSERT INTO dbo.anomaly_alerts (
                                alert_id, recorded_at, london_time, district_id, district_name,
                                domain, metric_label, unit, severity, method, z_score,
                                measured_value, baseline_value, threshold_label, message, injected
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (al_row[0], *al_row),
                        )

                    cur.execute(
                        """
                        INSERT INTO dbo.batch_telemetry (
                            recorded_at, batch_id, target_rate_eps, throughput_eps, batch_events,
                            avg_latency_ms, p50_latency_ms, p95_latency_ms, spark_batch_ms,
                            cumulative_events, cumulative_alerts, backpressure_status,
                            driver_rss_mb, lake_size_mb
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        telemetry_row,
                    )

                    cur.execute(
                        """
                        UPDATE dbo.platform_state
                        SET cumulative_events = %s,
                            cumulative_alerts = %s,
                            last_batch_id = %s,
                            window_history_json = %s,
                            updated_at = SYSUTCDATETIME()
                        WHERE state_key = N'spark_streaming';
                        IF @@ROWCOUNT = 0
                        INSERT INTO dbo.platform_state (
                            state_key, cumulative_events, cumulative_alerts,
                            last_batch_id, window_history_json
                        ) VALUES (N'spark_streaming', %s, %s, %s, %s);
                        """,
                        (
                            cum_events,
                            cum_alerts,
                            int(batch_id),
                            win_json,
                            cum_events,
                            cum_alerts,
                            int(batch_id),
                            win_json,
                        ),
                    )

                    self._batches_written += 1
                    if self.retention_days > 0 and self._batches_written % 1000 == 0:
                        cutoff = rec_utc - timedelta(days=self.retention_days)
                        cur.execute("DELETE FROM dbo.district_measurements WHERE recorded_at < %s", (cutoff,))
                        cur.execute("DELETE FROM dbo.domain_snapshots WHERE recorded_at < %s", (cutoff,))
                        cur.execute("DELETE FROM dbo.batch_telemetry WHERE recorded_at < %s", (cutoff,))
                        cur.execute("DELETE FROM dbo.anomaly_alerts WHERE recorded_at < %s", (cutoff,))

                conn.commit()
                return True
            except Exception as exc:
                logger.warning("MSSQL write attempt %d failed on batch #%d: %s", attempt + 1, batch_id, exc)
                try:
                    if self._conn is not None:
                        self._conn.rollback()
                except Exception:
                    pass
                self.close()
        return False


_DDL_STATEMENTS: list[str] = [
    """
    IF OBJECT_ID(N'dbo.platform_state', N'U') IS NULL
    CREATE TABLE dbo.platform_state (
        state_key NVARCHAR(64) NOT NULL PRIMARY KEY,
        cumulative_events BIGINT NOT NULL DEFAULT 0,
        cumulative_alerts BIGINT NOT NULL DEFAULT 0,
        last_batch_id BIGINT NOT NULL DEFAULT 0,
        window_history_json NVARCHAR(MAX) NULL,
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """,
    """
    IF OBJECT_ID(N'dbo.domain_snapshots', N'U') IS NULL
    CREATE TABLE dbo.domain_snapshots (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        recorded_at DATETIME2 NOT NULL,
        london_time DATETIME2 NOT NULL,
        batch_id BIGINT NOT NULL,
        domain NVARCHAR(32) NOT NULL,
        metric_name NVARCHAR(64) NULL,
        avg_value FLOAT NOT NULL,
        min_value FLOAT NULL,
        max_value FLOAT NULL,
        unit NVARCHAR(32) NULL,
        event_count INT NOT NULL,
        data_source NVARCHAR(128) NULL,
        secondary_json NVARCHAR(MAX) NULL
    );
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_domain_snapshots_london_domain'
          AND object_id = OBJECT_ID(N'dbo.domain_snapshots')
    )
    CREATE INDEX IX_domain_snapshots_london_domain
        ON dbo.domain_snapshots (london_time DESC, domain)
        INCLUDE (avg_value, min_value, max_value, event_count);
    """,
    """
    IF OBJECT_ID(N'dbo.district_measurements', N'U') IS NULL
    CREATE TABLE dbo.district_measurements (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        recorded_at DATETIME2 NOT NULL,
        london_time DATETIME2 NOT NULL,
        batch_id BIGINT NOT NULL,
        district_id NVARCHAR(64) NOT NULL,
        district_name NVARCHAR(128) NOT NULL,
        zone_type NVARCHAR(32) NULL,
        domain NVARCHAR(32) NOT NULL,
        avg_value FLOAT NOT NULL,
        min_value FLOAT NULL,
        max_value FLOAT NULL,
        event_count INT NOT NULL,
        status NVARCHAR(32) NOT NULL,
        tfl_disruptions INT NOT NULL DEFAULT 0
    );
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_district_measurements_london_dist_dom'
          AND object_id = OBJECT_ID(N'dbo.district_measurements')
    )
    CREATE INDEX IX_district_measurements_london_dist_dom
        ON dbo.district_measurements (london_time DESC, district_id, domain)
        INCLUDE (avg_value, status, event_count);
    """,
    """
    IF OBJECT_ID(N'dbo.anomaly_alerts', N'U') IS NULL
    CREATE TABLE dbo.anomaly_alerts (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        alert_id NVARCHAR(64) NOT NULL UNIQUE,
        recorded_at DATETIME2 NOT NULL,
        london_time DATETIME2 NOT NULL,
        district_id NVARCHAR(64) NOT NULL,
        district_name NVARCHAR(128) NOT NULL,
        domain NVARCHAR(32) NOT NULL,
        metric_label NVARCHAR(64) NULL,
        unit NVARCHAR(32) NULL,
        severity NVARCHAR(32) NOT NULL,
        method NVARCHAR(64) NULL,
        z_score FLOAT NULL,
        measured_value FLOAT NULL,
        baseline_value FLOAT NULL,
        threshold_label NVARCHAR(128) NULL,
        message NVARCHAR(512) NULL,
        injected BIT NOT NULL DEFAULT 0
    );
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_anomaly_alerts_recorded'
          AND object_id = OBJECT_ID(N'dbo.anomaly_alerts')
    )
    CREATE INDEX IX_anomaly_alerts_recorded
        ON dbo.anomaly_alerts (recorded_at DESC)
        INCLUDE (district_id, domain, severity);
    """,
    """
    IF OBJECT_ID(N'dbo.batch_telemetry', N'U') IS NULL
    CREATE TABLE dbo.batch_telemetry (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        recorded_at DATETIME2 NOT NULL,
        batch_id BIGINT NOT NULL,
        target_rate_eps INT NULL,
        throughput_eps FLOAT NULL,
        batch_events INT NOT NULL,
        avg_latency_ms FLOAT NULL,
        p50_latency_ms FLOAT NULL,
        p95_latency_ms FLOAT NULL,
        spark_batch_ms FLOAT NULL,
        cumulative_events BIGINT NOT NULL,
        cumulative_alerts BIGINT NOT NULL,
        backpressure_status NVARCHAR(32) NULL,
        driver_rss_mb FLOAT NULL,
        lake_size_mb FLOAT NULL
    );
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_batch_telemetry_recorded'
          AND object_id = OBJECT_ID(N'dbo.batch_telemetry')
    )
    CREATE INDEX IX_batch_telemetry_recorded
        ON dbo.batch_telemetry (recorded_at DESC);
    """,
    """
    CREATE OR ALTER VIEW dbo.vw_hourly_city_summary AS
    SELECT
        DATEADD(HOUR, DATEDIFF(HOUR, 0, london_time), 0) AS saat_londra,
        ROUND(AVG(CASE WHEN domain = N'traffic' THEN avg_value END), 2) AS trafik_hiz_kms,
        ROUND(AVG(CASE WHEN domain = N'weather' THEN avg_value END), 2) AS sicaklik_c,
        ROUND(AVG(CASE WHEN domain = N'air_quality' THEN avg_value END), 2) AS hava_kalitesi_aqi,
        ROUND(AVG(CASE WHEN domain = N'energy' THEN avg_value END), 2) AS enerji_mw,
        SUM(event_count) AS toplam_kayit
    FROM dbo.domain_snapshots
    GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, london_time), 0);
    """,
    """
    CREATE OR ALTER VIEW dbo.vw_recent_alerts AS
    SELECT
        london_time AS zaman_londra,
        district_name AS bolge,
        domain AS alan,
        severity AS seviye,
        metric_label AS metrik,
        measured_value AS olculen_deger,
        unit AS birim,
        baseline_value AS normal_deger,
        method AS yontem,
        z_score,
        message AS aciklama
    FROM dbo.anomaly_alerts;
    """,
    """
    CREATE OR ALTER VIEW dbo.vw_latest_district_status AS
    WITH ranked AS (
        SELECT
            district_id,
            district_name,
            zone_type,
            domain,
            avg_value,
            status,
            tfl_disruptions,
            london_time,
            ROW_NUMBER() OVER (PARTITION BY district_id, domain ORDER BY recorded_at DESC, id DESC) AS rn
        FROM dbo.district_measurements
    )
    SELECT
        district_id,
        district_name,
        zone_type,
        MAX(london_time) AS son_guncelleme,
        ROUND(MAX(CASE WHEN domain = N'traffic' THEN avg_value END), 2) AS trafik_hiz_kms,
        ROUND(MAX(CASE WHEN domain = N'weather' THEN avg_value END), 2) AS sicaklik_c,
        ROUND(MAX(CASE WHEN domain = N'air_quality' THEN avg_value END), 2) AS hava_kalitesi_aqi,
        ROUND(MAX(CASE WHEN domain = N'energy' THEN avg_value END), 2) AS enerji_mw,
        MAX(tfl_disruptions) AS yol_aksamasi
    FROM ranked
    WHERE rn = 1
    GROUP BY district_id, district_name, zone_type;
    """,
]
