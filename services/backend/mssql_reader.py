"""Read-only Microsoft SQL Server (MSSQL) query layer for the FastAPI backend.

Allows the backend and Historical Analytics service to read permanently archived
London Smart City snapshots, borough measurements, anomaly alerts, and database
storage statistics from `SmartCityDB`.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

try:
    import pymssql
except ImportError:  # pragma: no cover - optional in slim unit-test environments
    pymssql = None  # type: ignore[assignment]

logger = logging.getLogger("mssql_reader")

VALID_DOMAINS = {"traffic", "weather", "air_quality", "energy"}


def _config() -> dict[str, Any]:
    return {
        "enabled": os.getenv("MSSQL_ENABLED", "false").lower() in ("1", "true", "yes"),
        "host": os.getenv("MSSQL_HOST", "mssql"),
        "port": int(os.getenv("MSSQL_PORT", "1433")),
        "database": os.getenv("MSSQL_DATABASE", "SmartCityDB"),
        "user": os.getenv("MSSQL_USER", "sa"),
        "password": os.getenv("MSSQL_PASSWORD", "SmartCity#2026!"),
    }


def is_mssql_enabled() -> bool:
    return _config()["enabled"] and pymssql is not None


def _connect(timeout: int = 5) -> Any:
    cfg = _config()
    if not cfg["enabled"] or pymssql is None:
        raise RuntimeError("MSSQL is disabled or pymssql is not installed")
    return pymssql.connect(
        server=cfg["host"],
        port=str(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        login_timeout=timeout,
        timeout=timeout,
        autocommit=True,
        charset="UTF-8",
    )


def read_mssql_hourly(
    start_date: date,
    end_date: date,
    district_id: str = "all",
) -> tuple[dict[str, dict[str, float]], dict[str, Any]]:
    """Aggregate permanently stored MSSQL measurements into London hourly buckets."""
    stats: dict[str, Any] = {
        "available": False,
        "records_read": 0,
        "domains_hours": {"traffic": 0, "weather": 0, "air_quality": 0, "energy": 0},
    }
    if not is_mssql_enabled():
        return {}, stats

    start_dt = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0)
    end_exclusive = datetime(end_date.year, end_date.month, end_date.day, 0, 0, 0) + timedelta(days=1)

    conn = None
    try:
        conn = _connect(timeout=5)
        stats["available"] = True
        hourly_by_key: dict[str, dict[str, float]] = defaultdict(dict)

        with conn.cursor(as_dict=True) as cur:
            if district_id == "all":
                cur.execute(
                    """
                    SELECT
                        DATEADD(HOUR, DATEDIFF(HOUR, 0, london_time), 0) AS hour_bucket,
                        domain,
                        AVG(avg_value) AS mean_val,
                        SUM(CAST(event_count AS BIGINT)) AS rec_count
                    FROM dbo.domain_snapshots
                    WHERE london_time >= %s AND london_time < %s
                    GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, london_time), 0), domain
                    """,
                    (start_dt, end_exclusive),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        DATEADD(HOUR, DATEDIFF(HOUR, 0, london_time), 0) AS hour_bucket,
                        domain,
                        AVG(avg_value) AS mean_val,
                        SUM(CAST(event_count AS BIGINT)) AS rec_count
                    FROM dbo.district_measurements
                    WHERE london_time >= %s AND london_time < %s AND district_id = %s
                    GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, london_time), 0), domain
                    """,
                    (start_dt, end_exclusive, district_id),
                )

            for row in cur.fetchall() or []:
                dom = str(row.get("domain") or "")
                hb = row.get("hour_bucket")
                val = row.get("mean_val")
                if dom not in VALID_DOMAINS or hb is None or val is None:
                    continue
                hour_key = hb.strftime("%Y-%m-%dT%H:00") if isinstance(hb, datetime) else str(hb)[:13] + ":00"
                hourly_by_key[hour_key][dom] = round(float(val), 2)
                stats["domains_hours"][dom] += 1
                stats["records_read"] += int(row.get("rec_count") or 0)

        return dict(hourly_by_key), stats
    except Exception as exc:
        logger.debug("Non-fatal MSSQL hourly query warning: %s", exc)
        return {}, stats
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def get_storage_summary() -> dict[str, Any]:
    """Return MSSQL connectivity, row counts, time coverage, and database size."""
    cfg = _config()
    base = {
        "enabled": cfg["enabled"],
        "connected": False,
        "database": cfg["database"],
        "host": cfg["host"],
        "port": cfg["port"],
        "tables": {},
        "total_rows": 0,
        "db_size_mb": 0.0,
        "earliest_london_time": None,
        "latest_london_time": None,
        "cumulative_events": 0,
        "cumulative_alerts": 0,
        "source_breakdown": {
            "live_snapshots": 0,
            "backfill_snapshots": 0,
        },
    }
    if not is_mssql_enabled():
        return base

    conn = None
    try:
        conn = _connect(timeout=4)
        base["connected"] = True
        with conn.cursor(as_dict=True) as cur:
            cur.execute(
                """
                SELECT
                    t.name AS table_name,
                    SUM(p.rows) AS row_cnt
                FROM sys.tables t
                INNER JOIN sys.partitions p
                    ON t.object_id = p.object_id AND p.index_id IN (0, 1)
                WHERE t.name IN (
                    N'domain_snapshots',
                    N'district_measurements',
                    N'anomaly_alerts',
                    N'batch_telemetry',
                    N'platform_state'
                )
                GROUP BY t.name
                """
            )
            tables: dict[str, int] = {}
            for r in cur.fetchall() or []:
                tables[str(r["table_name"])] = int(r.get("row_cnt") or 0)
            base["tables"] = tables
            base["total_rows"] = sum(tables.values())

            cur.execute(
                """
                SELECT CAST(SUM(size) * 8.0 / 1024.0 AS FLOAT) AS size_mb
                FROM sys.database_files
                """
            )
            sz_row = cur.fetchone()
            if sz_row and sz_row.get("size_mb") is not None:
                base["db_size_mb"] = round(float(sz_row["size_mb"]), 2)

            if tables.get("domain_snapshots", 0) > 0:
                cur.execute(
                    """
                    SELECT
                        MIN(london_time) AS earliest_ts,
                        MAX(london_time) AS latest_ts
                    FROM dbo.domain_snapshots
                    """
                )
                tr = cur.fetchone()
                if tr:
                    if isinstance(tr.get("earliest_ts"), datetime):
                        base["earliest_london_time"] = tr["earliest_ts"].isoformat()
                    if isinstance(tr.get("latest_ts"), datetime):
                        base["latest_london_time"] = tr["latest_ts"].isoformat()

                cur.execute(
                    """
                    SELECT
                        SUM(CASE WHEN batch_id >= 0 THEN 1 ELSE 0 END) AS live_snapshots,
                        SUM(CASE WHEN batch_id < 0 THEN 1 ELSE 0 END) AS backfill_snapshots
                    FROM dbo.domain_snapshots
                    """
                )
                sb = cur.fetchone()
                if sb:
                    base["source_breakdown"] = {
                        "live_snapshots": int(sb.get("live_snapshots") or 0),
                        "backfill_snapshots": int(sb.get("backfill_snapshots") or 0),
                    }

            if tables.get("platform_state", 0) > 0:
                cur.execute(
                    """
                    SELECT cumulative_events, cumulative_alerts
                    FROM dbo.platform_state
                    WHERE state_key = N'spark_streaming'
                    """
                )
                st = cur.fetchone()
                if st:
                    base["cumulative_events"] = int(st.get("cumulative_events") or 0)
                    base["cumulative_alerts"] = int(st.get("cumulative_alerts") or 0)

        return base
    except Exception as exc:
        logger.debug("MSSQL storage summary unavailable: %s", exc)
        base["error"] = str(exc)
        return base
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def query_saved_snapshots(
    *,
    domain: str | None = None,
    district_id: str = "all",
    start_date: date | None = None,
    end_date: date | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return latest stored domain or district measurements from MSSQL."""
    if not is_mssql_enabled():
        return []

    safe_limit = max(1, min(int(limit), 1000))
    clauses: list[str] = []
    params: list[Any] = []

    if domain:
        clauses.append("domain = %s")
        params.append(domain)
    if start_date:
        clauses.append("london_time >= %s")
        params.append(datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0))
    if end_date:
        clauses.append("london_time < %s")
        params.append(datetime(end_date.year, end_date.month, end_date.day, 0, 0, 0) + timedelta(days=1))

    conn = None
    try:
        conn = _connect(timeout=5)
        with conn.cursor(as_dict=True) as cur:
            if district_id == "all":
                where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
                cur.execute(
                    f"""
                    SELECT TOP ({safe_limit})
                        id, recorded_at, london_time, batch_id, domain, metric_name,
                        avg_value, min_value, max_value, unit, event_count, data_source
                    FROM dbo.domain_snapshots
                    {where_sql}
                    ORDER BY london_time DESC, id DESC
                    """,
                    tuple(params),
                )
            else:
                clauses.append("district_id = %s")
                params.append(district_id)
                where_sql = "WHERE " + " AND ".join(clauses)
                cur.execute(
                    f"""
                    SELECT TOP ({safe_limit})
                        id, recorded_at, london_time, batch_id, district_id, district_name,
                        zone_type, domain, avg_value, min_value, max_value,
                        event_count, status, tfl_disruptions
                    FROM dbo.district_measurements
                    {where_sql}
                    ORDER BY london_time DESC, id DESC
                    """,
                    tuple(params),
                )

            rows: list[dict[str, Any]] = []
            for r in cur.fetchall() or []:
                item = dict(r)
                for ts_col in ("recorded_at", "london_time"):
                    if isinstance(item.get(ts_col), datetime):
                        item[ts_col] = item[ts_col].isoformat()
                rows.append(item)
            return rows
    except Exception as exc:
        logger.warning("Failed querying MSSQL snapshots: %s", exc)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def query_saved_alerts(
    *,
    domain: str | None = None,
    district_id: str | None = None,
    severity: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return permanently archived anomaly alerts from MSSQL."""
    if not is_mssql_enabled():
        return []

    safe_limit = max(1, min(int(limit), 500))
    clauses: list[str] = []
    params: list[Any] = []
    if domain:
        clauses.append("domain = %s")
        params.append(domain)
    if district_id and district_id != "all":
        clauses.append("district_id = %s")
        params.append(district_id)
    if severity:
        clauses.append("severity = %s")
        params.append(severity)

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    conn = None
    try:
        conn = _connect(timeout=5)
        with conn.cursor(as_dict=True) as cur:
            cur.execute(
                f"""
                SELECT TOP ({safe_limit})
                    id, alert_id, recorded_at, london_time, district_id, district_name,
                    domain, metric_label, unit, severity, method, z_score,
                    measured_value, baseline_value, threshold_label, message, injected
                FROM dbo.anomaly_alerts
                {where_sql}
                ORDER BY recorded_at DESC, id DESC
                """,
                tuple(params),
            )
            rows: list[dict[str, Any]] = []
            for r in cur.fetchall() or []:
                item = dict(r)
                for ts_col in ("recorded_at", "london_time"):
                    if isinstance(item.get(ts_col), datetime):
                        item[ts_col] = item[ts_col].isoformat()
                item["injected"] = bool(item.get("injected"))
                rows.append(item)
            return rows
    except Exception as exc:
        logger.warning("Failed querying MSSQL alerts: %s", exc)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
