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
                        DATEADD(HOUR, DATEDIFF(HOUR, 0, londra_zamani), 0) AS hour_bucket,
                        alan AS domain,
                        COALESCE(
                            SUM(CASE WHEN batch_no >= 0 THEN ortalama_deger * CAST(olay_sayisi AS FLOAT) END)
                                / NULLIF(SUM(CASE WHEN batch_no >= 0 THEN CAST(olay_sayisi AS FLOAT) END), 0),
                            AVG(CASE WHEN batch_no >= 0 THEN ortalama_deger END),
                            SUM(ortalama_deger * CAST(olay_sayisi AS FLOAT))
                                / NULLIF(SUM(CAST(olay_sayisi AS FLOAT)), 0),
                            AVG(ortalama_deger)
                        ) AS mean_val,
                        SUM(CAST(olay_sayisi AS BIGINT)) AS rec_count
                    FROM dbo.alan_ozetleri
                    WHERE londra_zamani >= %s AND londra_zamani < %s
                    GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, londra_zamani), 0), alan
                    """,
                    (start_dt, end_exclusive),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        DATEADD(HOUR, DATEDIFF(HOUR, 0, londra_zamani), 0) AS hour_bucket,
                        alan AS domain,
                        COALESCE(
                            SUM(CASE WHEN batch_no >= 0 THEN ortalama_deger * CAST(olay_sayisi AS FLOAT) END)
                                / NULLIF(SUM(CASE WHEN batch_no >= 0 THEN CAST(olay_sayisi AS FLOAT) END), 0),
                            AVG(CASE WHEN batch_no >= 0 THEN ortalama_deger END),
                            SUM(ortalama_deger * CAST(olay_sayisi AS FLOAT))
                                / NULLIF(SUM(CAST(olay_sayisi AS FLOAT)), 0),
                            AVG(ortalama_deger)
                        ) AS mean_val,
                        SUM(CAST(olay_sayisi AS BIGINT)) AS rec_count
                    FROM dbo.bolge_olcumleri
                    WHERE londra_zamani >= %s AND londra_zamani < %s AND bolge_id = %s
                    GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, londra_zamani), 0), alan
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
                    N'alan_ozetleri',
                    N'bolge_olcumleri',
                    N'anomali_uyarilari',
                    N'batch_telemetri',
                    N'platform_durumu'
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

            if tables.get("alan_ozetleri", 0) > 0:
                cur.execute(
                    """
                    SELECT
                        MIN(londra_zamani) AS earliest_ts,
                        MAX(londra_zamani) AS latest_ts
                    FROM dbo.alan_ozetleri
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
                        SUM(CASE WHEN batch_no >= 0 THEN 1 ELSE 0 END) AS live_snapshots,
                        SUM(CASE WHEN batch_no < 0 THEN 1 ELSE 0 END) AS backfill_snapshots
                    FROM dbo.alan_ozetleri
                    """
                )
                sb = cur.fetchone()
                if sb:
                    base["source_breakdown"] = {
                        "live_snapshots": int(sb.get("live_snapshots") or 0),
                        "backfill_snapshots": int(sb.get("backfill_snapshots") or 0),
                    }

            if tables.get("platform_durumu", 0) > 0:
                cur.execute(
                    """
                    SELECT
                        toplam_olay_sayisi AS cumulative_events,
                        toplam_uyari_sayisi AS cumulative_alerts
                    FROM dbo.platform_durumu
                    WHERE durum_anahtari = N'spark_streaming'
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
        clauses.append("alan = %s")
        params.append(domain)
    if start_date:
        clauses.append("londra_zamani >= %s")
        params.append(datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0))
    if end_date:
        clauses.append("londra_zamani < %s")
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
                        id,
                        kayit_zamani_utc AS recorded_at,
                        londra_zamani AS london_time,
                        batch_no AS batch_id,
                        alan AS domain,
                        metrik_adi AS metric_name,
                        ortalama_deger AS avg_value,
                        en_dusuk_deger AS min_value,
                        en_yuksek_deger AS max_value,
                        birim AS unit,
                        olay_sayisi AS event_count,
                        veri_kaynagi AS data_source
                    FROM dbo.alan_ozetleri
                    {where_sql}
                    ORDER BY londra_zamani DESC, id DESC
                    """,
                    tuple(params),
                )
            else:
                clauses.append("bolge_id = %s")
                params.append(district_id)
                where_sql = "WHERE " + " AND ".join(clauses)
                cur.execute(
                    f"""
                    SELECT TOP ({safe_limit})
                        id,
                        kayit_zamani_utc AS recorded_at,
                        londra_zamani AS london_time,
                        batch_no AS batch_id,
                        bolge_id AS district_id,
                        bolge_adi AS district_name,
                        bolge_tipi AS zone_type,
                        alan AS domain,
                        ortalama_deger AS avg_value,
                        en_dusuk_deger AS min_value,
                        en_yuksek_deger AS max_value,
                        olay_sayisi AS event_count,
                        durum AS status,
                        tfl_yol_aksamasi AS tfl_disruptions
                    FROM dbo.bolge_olcumleri
                    {where_sql}
                    ORDER BY londra_zamani DESC, id DESC
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
        clauses.append("alan = %s")
        params.append(domain)
    if district_id and district_id != "all":
        clauses.append("bolge_id = %s")
        params.append(district_id)
    if severity:
        clauses.append("seviye = %s")
        params.append(severity)

    where_sql = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    conn = None
    try:
        conn = _connect(timeout=5)
        with conn.cursor(as_dict=True) as cur:
            cur.execute(
                f"""
                SELECT TOP ({safe_limit})
                    id,
                    uyari_id AS alert_id,
                    kayit_zamani_utc AS recorded_at,
                    londra_zamani AS london_time,
                    bolge_id AS district_id,
                    bolge_adi AS district_name,
                    alan AS domain,
                    metrik_etiketi AS metric_label,
                    birim AS unit,
                    seviye AS severity,
                    tespit_yontemi AS method,
                    z_skoru AS z_score,
                    olculen_deger AS measured_value,
                    referans_deger AS baseline_value,
                    esik_etiketi AS threshold_label,
                    aciklama AS message,
                    test_enjekte AS injected
                FROM dbo.anomali_uyarilari
                {where_sql}
                ORDER BY kayit_zamani_utc DESC, id DESC
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
