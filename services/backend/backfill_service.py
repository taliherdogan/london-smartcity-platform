"""24/7 Cloud Archive Sync & Historical API Backfill Service for MSSQL (SmartCityDB).

Fills missing past hours in MSSQL (e.g. overnight hours when the local PC was off)
using a strict, idempotent priority hierarchy:
  1. Live Spark Streaming records (`batch_id >= 0`) — NEVER overwritten.
  2. 24/7 Cloud Collector real live captures (`batch_id = -2`) from GitHub `data-archive`
     branch (or local archive dir) — 100% real live TfL Road traffic, Open-Meteo weather
     & air quality, and Elexon/NESO energy at 15-minute resolution. Automatically upgrades
     any prior API/model fallback rows (`batch_id = -1`) for the same hour.
  3. Official Historical Open APIs (`batch_id = -1`) for dates prior to Cloud Collector
     activation (Open-Meteo Weather, Open-Meteo AQI, Elexon BMRS, NESO Region 13).
     By default (`MSSQL_BACKFILL_ALLOW_TRAFFIC_MODEL=false`), synthetic/model traffic is
     NOT inserted into MSSQL so all stored traffic records remain 100% real.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

try:
    import pymssql
except ImportError:  # pragma: no cover - optional in slim unit-test environments
    pymssql = None  # type: ignore[assignment]

from history_service import (
    DEFAULT_LONDON_FREE_FLOW_KMH,
    LONDON_ENERGY_SHARE,
    LONDON_LAT,
    LONDON_LON,
    _fetch_elexon_demand,
    _fetch_neso_london_carbon,
    _fetch_open_meteo_air_quality,
    _fetch_open_meteo_weather,
    _model_historical_traffic,
    _uk_utc_offset,
    build_expected_hour_keys,
    load_districts_metadata,
    london_now,
    resolve_date_range,
)
from mssql_reader import _config as mssql_config

logger = logging.getLogger("backfill_service")

BATCH_ID_CLOUD = -2
BATCH_ID_API_BACKFILL = -1
VALID_DOMAINS = ("traffic", "weather", "air_quality", "energy")
INSERT_CHUNK_SIZE = 500

_SYNC_LOCK = asyncio.Lock()
_BACKFILL_STATUS: dict[str, Any] = {
    "running": False,
    "last_run_at": None,
    "last_trigger": None,
    "last_result": None,
    "last_error": None,
}


def backfill_settings() -> dict[str, Any]:
    return {
        "auto_enabled": os.getenv("MSSQL_AUTO_BACKFILL_ENABLED", "true").lower() in ("1", "true", "yes"),
        "auto_hours": max(1, min(168, int(os.getenv("MSSQL_AUTO_BACKFILL_HOURS", "72")))),
        "allow_traffic_model": os.getenv("MSSQL_BACKFILL_ALLOW_TRAFFIC_MODEL", "false").lower() in ("1", "true", "yes"),
        "cloud_repo": os.getenv("CLOUD_ARCHIVE_REPO", "").strip().strip("/"),
        "cloud_branch": os.getenv("CLOUD_ARCHIVE_BRANCH", "data-archive").strip() or "data-archive",
        "cloud_token": os.getenv("CLOUD_ARCHIVE_TOKEN", "").strip(),
        "cloud_local_dir": os.getenv("CLOUD_ARCHIVE_LOCAL_DIR", "/data/cloud_archive").strip(),
        "sync_interval_min": max(0, int(os.getenv("CLOUD_SYNC_INTERVAL_MIN", "30"))),
    }


def get_backfill_status() -> dict[str, Any]:
    cfg = backfill_settings()
    local_dir = Path(cfg["cloud_local_dir"]) if cfg["cloud_local_dir"] else None
    has_local_archive = bool(local_dir and (local_dir / "snapshots").exists())
    return {
        "running": _BACKFILL_STATUS["running"],
        "auto_enabled": cfg["auto_enabled"],
        "auto_hours": cfg["auto_hours"],
        "allow_traffic_model": cfg["allow_traffic_model"],
        "cloud_configured": bool(cfg["cloud_repo"]) or has_local_archive,
        "cloud_repo": cfg["cloud_repo"] or None,
        "cloud_branch": cfg["cloud_branch"],
        "cloud_local_archive": has_local_archive,
        "sync_interval_min": cfg["sync_interval_min"],
        "last_run_at": _BACKFILL_STATUS["last_run_at"],
        "last_trigger": _BACKFILL_STATUS["last_trigger"],
        "last_result": _BACKFILL_STATUS["last_result"],
        "last_error": _BACKFILL_STATUS["last_error"],
    }


def _london_naive_to_utc_naive(lon_naive: datetime) -> datetime:
    """Convert a Europe/London naive datetime into a UTC naive datetime (GMT/BST aware)."""
    assumed_utc = lon_naive.replace(tzinfo=timezone.utc)
    offset = _uk_utc_offset(assumed_utc)
    candidate_utc = assumed_utc - offset
    verified_offset = _uk_utc_offset(candidate_utc)
    return (assumed_utc - verified_offset).replace(tzinfo=None)


def _parse_iso_naive_utc(ts_str: str) -> datetime:
    cleaned = ts_str.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None, microsecond=0)
    return dt.replace(microsecond=0)


def _parse_iso_naive_local(ts_str: str) -> datetime:
    cleaned = ts_str.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned)
    return dt.replace(tzinfo=None, microsecond=0)


def _connect_rw(timeout: int = 15) -> Any:
    cfg = mssql_config()
    if not cfg["enabled"] or pymssql is None:
        raise RuntimeError("MSSQL is disabled or pymssql is unavailable")
    return pymssql.connect(
        server=cfg["host"],
        port=str(cfg["port"]),
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        login_timeout=timeout,
        timeout=timeout,
        autocommit=False,
        charset="UTF-8",
    )


def _check_schema_ready() -> bool:
    cfg = mssql_config()
    if not cfg["enabled"] or pymssql is None:
        return False
    conn = None
    try:
        conn = _connect_rw(timeout=5)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT CASE
                    WHEN OBJECT_ID(N'dbo.domain_snapshots', N'U') IS NOT NULL
                     AND OBJECT_ID(N'dbo.district_measurements', N'U') IS NOT NULL
                    THEN 1 ELSE 0
                END
                """
            )
            row = cur.fetchone()
            return bool(row and row[0] == 1)
    except Exception:
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


async def wait_for_mssql_schema(max_wait_sec: float = 60.0) -> bool:
    deadline = asyncio.get_running_loop().time() + max_wait_sec
    while True:
        if await asyncio.to_thread(_check_schema_ready):
            return True
        if asyncio.get_running_loop().time() >= deadline:
            return False
        await asyncio.sleep(2.5)


def _parse_jsonl_text(text: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and obj.get("recorded_at_utc") and isinstance(obj.get("domains"), dict):
                records.append(obj)
        except json.JSONDecodeError:
            continue
    return records


async def fetch_cloud_snapshots(
    start_date: date,
    end_date: date,
    *,
    window_start_lon: datetime,
    window_end_lon: datetime,
    http_client: httpx.AsyncClient | None = None,
    settings: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Load 24/7 Cloud Collector snapshots from local archive dir and/or GitHub raw branch."""
    cfg = settings or backfill_settings()
    today_utc = datetime.now(timezone.utc).date()
    # Expand UTC date scan by +/- 1 day to safely cover Europe/London BST/GMT boundaries
    utc_start = start_date - timedelta(days=1)
    utc_end = min(today_utc, end_date + timedelta(days=1))

    utc_dates: list[date] = []
    cur = utc_start
    while cur <= utc_end:
        utc_dates.append(cur)
        cur += timedelta(days=1)

    by_key: dict[str, dict[str, Any]] = {}
    stats: dict[str, Any] = {
        "local_files_read": 0,
        "remote_files_read": 0,
        "snapshots_loaded": 0,
    }

    # 1. Local directory (useful for VPS/cron or local testing)
    local_root = Path(cfg["cloud_local_dir"]) if cfg.get("cloud_local_dir") else None
    if local_root and local_root.exists():
        for d_utc in utc_dates:
            fpath = local_root / "snapshots" / f"{d_utc.year:04d}" / f"{d_utc.month:02d}" / f"{d_utc.isoformat()}.jsonl"
            if fpath.is_file():
                try:
                    recs = _parse_jsonl_text(fpath.read_text(encoding="utf-8"))
                    stats["local_files_read"] += 1
                    for rec in recs:
                        k = str(rec.get("snapshot_id") or rec.get("recorded_at_utc"))
                        by_key[k] = rec
                except OSError as exc:
                    logger.warning("Failed reading local cloud archive %s: %s", fpath, exc)

    # 2. GitHub raw archive branch (public or token-authenticated private repo)
    repo = cfg.get("cloud_repo") or ""
    branch = cfg.get("cloud_branch") or "data-archive"
    token = cfg.get("cloud_token") or ""

    if repo:
        headers = {
            "Accept": "text/plain",
            "Cache-Control": "no-cache",
            "User-Agent": "SmartCityLondonBackfill/1.0",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        async def _fetch_day(client: httpx.AsyncClient, d_utc: date) -> list[dict[str, Any]]:
            url = (
                f"https://raw.githubusercontent.com/{repo}/{branch}"
                f"/snapshots/{d_utc.year:04d}/{d_utc.month:02d}/{d_utc.isoformat()}.jsonl"
            )
            try:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 404:
                    return []
                resp.raise_for_status()
                stats["remote_files_read"] += 1
                return _parse_jsonl_text(resp.text)
            except Exception as exc:
                logger.warning("Cloud archive fetch warning (%s): %s", url, exc)
                return []

        if http_client is not None:
            day_results = await asyncio.gather(*(_fetch_day(http_client, d) for d in utc_dates))
        else:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
                day_results = await asyncio.gather(*(_fetch_day(client, d) for d in utc_dates))

        for recs in day_results:
            for rec in recs:
                k = str(rec.get("snapshot_id") or rec.get("recorded_at_utc"))
                by_key[k] = rec

    # Filter to requested London time window and sort chronologically
    filtered: list[tuple[datetime, dict[str, Any]]] = []
    for rec in by_key.values():
        try:
            lon_dt = _parse_iso_naive_local(str(rec["london_time"]))
        except Exception:
            continue
        if window_start_lon <= lon_dt <= window_end_lon:
            filtered.append((lon_dt, rec))

    filtered.sort(key=lambda item: item[0])
    out = [rec for _, rec in filtered]
    stats["snapshots_loaded"] = len(out)
    return out, stats


def _inspect_existing_mssql_coverage(
    window_start_lon: datetime,
    window_end_lon: datetime,
) -> dict[str, Any]:
    """Query existing live, cloud, and API backfill rows in MSSQL for idempotent sync."""
    live_hours: dict[str, set[str]] = defaultdict(set)
    cloud_hours: dict[str, set[str]] = defaultdict(set)
    api_hours: dict[str, set[str]] = defaultdict(set)
    cloud_exact_ts: set[tuple[datetime, str]] = set()

    conn = None
    try:
        conn = _connect_rw(timeout=10)
        with conn.cursor(as_dict=True) as cur:
            cur.execute(
                """
                SELECT
                    DATEADD(HOUR, DATEDIFF(HOUR, 0, london_time), 0) AS hour_bucket,
                    domain,
                    MAX(CASE WHEN batch_id >= 0 THEN 1 ELSE 0 END) AS has_live,
                    MAX(CASE WHEN batch_id = %s THEN 1 ELSE 0 END) AS has_cloud,
                    MAX(CASE WHEN batch_id = %s THEN 1 ELSE 0 END) AS has_api
                FROM dbo.domain_snapshots
                WHERE london_time >= %s AND london_time <= %s
                GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, london_time), 0), domain
                """,
                (BATCH_ID_CLOUD, BATCH_ID_API_BACKFILL, window_start_lon, window_end_lon),
            )
            for row in cur.fetchall() or []:
                hb = row.get("hour_bucket")
                dom = str(row.get("domain") or "")
                if not isinstance(hb, datetime) or dom not in VALID_DOMAINS:
                    continue
                h_key = hb.strftime("%Y-%m-%dT%H:00")
                if row.get("has_live"):
                    live_hours[h_key].add(dom)
                if row.get("has_cloud"):
                    cloud_hours[h_key].add(dom)
                if row.get("has_api"):
                    api_hours[h_key].add(dom)

            cur.execute(
                """
                SELECT recorded_at, domain
                FROM dbo.domain_snapshots
                WHERE london_time >= %s AND london_time <= %s AND batch_id = %s
                """,
                (window_start_lon, window_end_lon, BATCH_ID_CLOUD),
            )
            for row in cur.fetchall() or []:
                rec = row.get("recorded_at")
                dom = str(row.get("domain") or "")
                if isinstance(rec, datetime) and dom in VALID_DOMAINS:
                    cloud_exact_ts.add((rec.replace(microsecond=0), dom))

        return {
            "live_hours": live_hours,
            "cloud_hours": cloud_hours,
            "api_hours": api_hours,
            "cloud_exact_ts": cloud_exact_ts,
        }
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _write_backfill_rows(
    *,
    domain_rows: list[tuple[Any, ...]],
    district_rows: list[tuple[Any, ...]],
    upgrade_api_hours: list[tuple[datetime, datetime, str]],
) -> None:
    """Persist new cloud/API backfill rows atomically in chunked executemany batches."""
    if not domain_rows and not district_rows and not upgrade_api_hours:
        return

    conn = None
    try:
        conn = _connect_rw(timeout=30)
        with conn.cursor() as cur:
            # Upgrade prior API/model fallback rows (-1) where real Cloud Collector data (-2) arrived
            for h_start, h_end, dom in upgrade_api_hours:
                cur.execute(
                    """
                    DELETE FROM dbo.domain_snapshots
                    WHERE london_time >= %s AND london_time < %s AND domain = %s AND batch_id = %s
                    """,
                    (h_start, h_end, dom, BATCH_ID_API_BACKFILL),
                )
                cur.execute(
                    """
                    DELETE FROM dbo.district_measurements
                    WHERE london_time >= %s AND london_time < %s AND domain = %s AND batch_id = %s
                    """,
                    (h_start, h_end, dom, BATCH_ID_API_BACKFILL),
                )

            if domain_rows:
                sql_dom = """
                    INSERT INTO dbo.domain_snapshots (
                        recorded_at, london_time, batch_id, domain, metric_name,
                        avg_value, min_value, max_value, unit, event_count,
                        data_source, secondary_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                for i in range(0, len(domain_rows), INSERT_CHUNK_SIZE):
                    cur.executemany(sql_dom, domain_rows[i : i + INSERT_CHUNK_SIZE])

            if district_rows:
                sql_dist = """
                    INSERT INTO dbo.district_measurements (
                        recorded_at, london_time, batch_id, district_id, district_name,
                        zone_type, domain, avg_value, min_value, max_value,
                        event_count, status, tfl_disruptions
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                for i in range(0, len(district_rows), INSERT_CHUNK_SIZE):
                    cur.executemany(sql_dist, district_rows[i : i + INSERT_CHUNK_SIZE])

        conn.commit()
    except Exception:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _build_cloud_rows(
    *,
    snapshots: list[dict[str, Any]],
    districts_by_id: dict[str, dict[str, Any]],
    coverage: dict[str, Any],
    include_districts: bool,
    allow_traffic_model: bool,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[tuple[datetime, datetime, str]], dict[str, Any]]:
    live_hours: dict[str, set[str]] = coverage["live_hours"]
    cloud_hours: dict[str, set[str]] = coverage["cloud_hours"]
    api_hours: dict[str, set[str]] = coverage["api_hours"]
    cloud_exact_ts: set[tuple[datetime, str]] = coverage["cloud_exact_ts"]

    domain_rows: list[tuple[Any, ...]] = []
    district_rows: list[tuple[Any, ...]] = []
    upgrade_keys: set[tuple[str, str]] = set()
    synced_snapshot_ids: set[str] = set()
    last_cloud_ts: str | None = None

    for snap in snapshots:
        rec_utc = _parse_iso_naive_utc(str(snap["recorded_at_utc"]))
        lon_dt = _parse_iso_naive_local(str(snap["london_time"]))
        hour_key = lon_dt.strftime("%Y-%m-%dT%H:00")
        snap_domains = snap.get("domains") or {}
        snap_districts = snap.get("districts") or {}

        for dom in VALID_DOMAINS:
            d_info = snap_domains.get(dom)
            if not isinstance(d_info, dict) or d_info.get("avg_value") is None:
                continue

            status_flag = str(d_info.get("status") or "live").lower()
            if dom == "traffic" and status_flag != "live" and not allow_traffic_model:
                continue
            if dom in live_hours.get(hour_key, set()):
                continue
            if (rec_utc, dom) in cloud_exact_ts:
                continue

            if dom in api_hours.get(hour_key, set()):
                upgrade_keys.add((hour_key, dom))

            provider = str(d_info.get("provider") or "Cloud Collector")
            source_tag = "Cloud Collector Live" if status_flag == "live" else "Cloud Collector Fallback"
            data_source = f"{provider} ({source_tag})"
            avg_v = float(d_info["avg_value"])
            min_v = float(d_info["min_value"]) if d_info.get("min_value") is not None else avg_v
            max_v = float(d_info["max_value"]) if d_info.get("max_value") is not None else avg_v

            domain_rows.append(
                (
                    rec_utc,
                    lon_dt,
                    BATCH_ID_CLOUD,
                    dom,
                    d_info.get("metric_name"),
                    avg_v,
                    min_v,
                    max_v,
                    d_info.get("unit"),
                    1,
                    data_source,
                    json.dumps(d_info.get("secondary") or {}, separators=(",", ":")),
                )
            )

            if include_districts and isinstance(snap_districts, dict):
                for did, dist_entry in snap_districts.items():
                    meta = districts_by_id.get(did)
                    if not meta or not isinstance(dist_entry, dict):
                        continue
                    m = dist_entry.get(dom)
                    if not isinstance(m, dict) or m.get("avg") is None:
                        continue
                    val = float(m["avg"])
                    district_rows.append(
                        (
                            rec_utc,
                            lon_dt,
                            BATCH_ID_CLOUD,
                            did,
                            str(meta.get("district_name") or did),
                            str(meta.get("zone_type") or "urban"),
                            dom,
                            val,
                            val,
                            val,
                            1,
                            str(m.get("status") or "normal"),
                            int(m.get("tfl_disruptions") or 0) if dom == "traffic" else 0,
                        )
                    )

            cloud_exact_ts.add((rec_utc, dom))
            cloud_hours.setdefault(hour_key, set()).add(dom)
            synced_snapshot_ids.add(str(snap.get("snapshot_id") or rec_utc.isoformat()))
            last_cloud_ts = lon_dt.isoformat()

    upgrade_api_hours: list[tuple[datetime, datetime, str]] = []
    for h_key, dom in sorted(upgrade_keys):
        h_start = datetime.strptime(h_key, "%Y-%m-%dT%H:%M")
        upgrade_api_hours.append((h_start, h_start + timedelta(hours=1), dom))

    return (
        domain_rows,
        district_rows,
        upgrade_api_hours,
        {
            "cloud_snapshots_synced": len(synced_snapshot_ids),
            "upgraded_hour_domains": len(upgrade_api_hours),
            "last_cloud_snapshot_at": last_cloud_ts,
        },
    )


async def _build_api_backfill_rows(
    *,
    start_date: date,
    end_date: date,
    window_start_lon: datetime,
    window_end_lon: datetime,
    now_lon: datetime,
    districts: list[dict[str, Any]],
    coverage: dict[str, Any],
    include_districts: bool,
    allow_traffic_model: bool,
    http_client: httpx.AsyncClient | None = None,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], dict[str, Any]]:
    """Fill remaining missing completed hours using official historical Open APIs."""
    live_hours: dict[str, set[str]] = coverage["live_hours"]
    cloud_hours: dict[str, set[str]] = coverage["cloud_hours"]
    api_hours: dict[str, set[str]] = coverage["api_hours"]

    current_hour_floor = now_lon.replace(minute=0, second=0, microsecond=0, tzinfo=None)
    all_hour_keys = build_expected_hour_keys(start_date, end_date, now_lon)
    target_domains = VALID_DOMAINS if allow_traffic_model else ("weather", "air_quality", "energy")

    missing_by_hour: dict[str, set[str]] = {}
    for h_key in all_hour_keys:
        h_dt = datetime.strptime(h_key, "%Y-%m-%dT%H:%M")
        if h_dt < window_start_lon.replace(minute=0, second=0, microsecond=0):
            continue
        if h_dt > window_end_lon:
            continue
        # Only backfill fully completed past hours so we never collide with the active live hour
        if h_dt >= current_hour_floor:
            continue

        covered = live_hours.get(h_key, set()) | cloud_hours.get(h_key, set()) | api_hours.get(h_key, set())
        needed = {d for d in target_domains if d not in covered}
        if needed:
            missing_by_hour[h_key] = needed

    if not missing_by_hour:
        return [], [], {"api_hours_filled": 0, "api_sources_used": []}

    today_lon = now_lon.date()
    need_weather = any(("weather" in s or "traffic" in s) for s in missing_by_hour.values())
    need_aqi = any("air_quality" in s for s in missing_by_hour.values())
    need_energy = any("energy" in s for s in missing_by_hour.values())

    async def _run_fetches(client: httpx.AsyncClient):
        tasks: dict[str, Any] = {}
        if need_weather:
            tasks["weather"] = _fetch_open_meteo_weather(client, LONDON_LAT, LONDON_LON, start_date, end_date, today_lon)
        if need_aqi:
            tasks["aqi"] = _fetch_open_meteo_air_quality(client, LONDON_LAT, LONDON_LON, start_date, end_date)
        if need_energy:
            tasks["elexon"] = _fetch_elexon_demand(client, start_date, end_date)
            tasks["neso"] = _fetch_neso_london_carbon(client, start_date, end_date)
        keys = list(tasks.keys())
        results = await asyncio.gather(*(tasks[k] for k in keys))
        return dict(zip(keys, results))

    if http_client is not None:
        fetched = await _run_fetches(http_client)
    else:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            fetched = await _run_fetches(client)

    weather_api, weather_ok = fetched.get("weather", ({}, False))
    aqi_api, aqi_ok = fetched.get("aqi", ({}, False))
    elexon_api, elexon_ok = fetched.get("elexon", ({}, False))
    neso_api, neso_ok = fetched.get("neso", ({}, False))

    domain_rows: list[tuple[Any, ...]] = []
    district_rows: list[tuple[Any, ...]] = []
    filled_hours: set[str] = set()
    sources_used: set[str] = set()

    n_dist = max(len(districts), 1)

    for h_key in sorted(missing_by_hour.keys()):
        needed = missing_by_hour[h_key]
        h_dt = datetime.strptime(h_key, "%Y-%m-%dT%H:%M")
        rec_utc = _london_naive_to_utc_naive(h_dt)

        w_pt = weather_api.get(h_key)
        aq_pt = aqi_api.get(h_key)
        nat_mw = elexon_api.get(h_key)
        neso_pt = neso_api.get(h_key)

        # Weather (real Open-Meteo only)
        if "weather" in needed and w_pt is not None:
            base_temp = float(w_pt["weather"])
            hum = float(w_pt.get("humidity_pct", 76.0))
            wind = float(w_pt.get("wind_speed_ms", 4.0))
            precip = float(w_pt.get("precipitation_mm", 0.0))

            dist_temps = [round(base_temp + float(d.get("temp_offset_c", 0.0)), 2) for d in districts] or [base_temp]
            domain_rows.append(
                (
                    rec_utc,
                    h_dt,
                    BATCH_ID_API_BACKFILL,
                    "weather",
                    "temperature_c",
                    round(sum(dist_temps) / len(dist_temps), 2),
                    round(min(dist_temps), 2),
                    round(max(dist_temps), 2),
                    "°C",
                    1,
                    "Open-Meteo Historical Backfill",
                    json.dumps(
                        {
                            "humidity_pct": round(hum, 1),
                            "wind_speed_ms": round(wind, 2),
                            "precipitation_mm": round(precip, 2),
                        },
                        separators=(",", ":"),
                    ),
                )
            )
            if include_districts:
                w_status = "critical" if wind > 18.0 else ("warning" if (wind > 12.5 or base_temp < 0.0 or base_temp > 30.0) else "normal")
                for d, t_val in zip(districts, dist_temps):
                    district_rows.append(
                        (
                            rec_utc,
                            h_dt,
                            BATCH_ID_API_BACKFILL,
                            str(d["district_id"]),
                            str(d["district_name"]),
                            str(d.get("zone_type") or "urban"),
                            "weather",
                            t_val,
                            t_val,
                            t_val,
                            1,
                            w_status,
                            0,
                        )
                    )
            filled_hours.add(h_key)
            sources_used.add("Open-Meteo Weather")

        # Air Quality (real Open-Meteo AQI only)
        if "air_quality" in needed and aq_pt is not None:
            base_aqi = float(aq_pt["air_quality"])
            pm25 = float(aq_pt.get("pm2_5", 6.0))
            pm10 = float(aq_pt.get("pm10", 11.0))
            no2 = float(aq_pt.get("no2", 16.0))

            dist_aqis = [round(max(4.0, base_aqi * float(d.get("aqi_factor", 1.0))), 1) for d in districts] or [base_aqi]
            domain_rows.append(
                (
                    rec_utc,
                    h_dt,
                    BATCH_ID_API_BACKFILL,
                    "air_quality",
                    "european_aqi",
                    round(sum(dist_aqis) / len(dist_aqis), 2),
                    round(min(dist_aqis), 2),
                    round(max(dist_aqis), 2),
                    "AQI",
                    1,
                    "Open-Meteo AQI Historical Backfill",
                    json.dumps(
                        {"pm2_5": round(pm25, 2), "pm10": round(pm10, 2), "no2": round(no2, 2)},
                        separators=(",", ":"),
                    ),
                )
            )
            if include_districts:
                for d, a_val in zip(districts, dist_aqis):
                    aq_status = "critical" if (a_val > 80.0 or pm25 > 35.0) else ("warning" if (a_val > 60.0 or pm25 > 25.0) else "normal")
                    district_rows.append(
                        (
                            rec_utc,
                            h_dt,
                            BATCH_ID_API_BACKFILL,
                            str(d["district_id"]),
                            str(d["district_name"]),
                            str(d.get("zone_type") or "urban"),
                            "air_quality",
                            a_val,
                            a_val,
                            a_val,
                            1,
                            aq_status,
                            0,
                        )
                    )
            filled_hours.add(h_key)
            sources_used.add("Open-Meteo Air Quality")

        # Energy (real Elexon BMRS + NESO Region 13 only)
        if "energy" in needed and nat_mw is not None:
            london_total_mw = round(float(nat_mw) * LONDON_ENERGY_SHARE, 1)
            carbon = float((neso_pt or {}).get("carbon_intensity_gco2", 180.0))
            ren_pct = float((neso_pt or {}).get("renewable_share_pct", 34.0))

            dist_mws = [
                round(max(50.0, london_total_mw * float(d.get("energy_weight", 1.0 / max(n_dist, 32)))), 2)
                for d in districts
            ] or [round(london_total_mw / 32.0, 2)]

            en_source = "Elexon BMRS + NESO Historical Backfill" if neso_ok else "Elexon BMRS Historical Backfill"
            domain_rows.append(
                (
                    rec_utc,
                    h_dt,
                    BATCH_ID_API_BACKFILL,
                    "energy",
                    "district_load_mw",
                    round(sum(dist_mws) / len(dist_mws), 2),
                    round(min(dist_mws), 2),
                    round(max(dist_mws), 2),
                    "MW",
                    1,
                    en_source,
                    json.dumps(
                        {
                            "national_load_mw": round(float(nat_mw), 1),
                            "london_total_mw": london_total_mw,
                            "renewable_share_pct": round(ren_pct, 1),
                            "carbon_intensity_gco2": round(carbon, 1),
                        },
                        separators=(",", ":"),
                    ),
                )
            )
            if include_districts:
                for d, mw_val in zip(districts, dist_mws):
                    district_rows.append(
                        (
                            rec_utc,
                            h_dt,
                            BATCH_ID_API_BACKFILL,
                            str(d["district_id"]),
                            str(d["district_name"]),
                            str(d.get("zone_type") or "urban"),
                            "energy",
                            mw_val,
                            mw_val,
                            mw_val,
                            1,
                            "normal",
                            0,
                        )
                    )
            filled_hours.add(h_key)
            sources_used.add("Elexon BMRS + NESO" if neso_ok else "Elexon BMRS")

        # Traffic (optional model — disabled by default unless allow_traffic_model=True)
        if "traffic" in needed and allow_traffic_model:
            precip_mm = float((w_pt or {}).get("precipitation_mm", 0.0))
            wind_ms = float((w_pt or {}).get("wind_speed_ms", 4.0))
            dist_speeds: list[float] = []
            dist_congs: list[float] = []
            for d in districts:
                ff = float(d.get("free_flow_kmh", DEFAULT_LONDON_FREE_FLOW_KMH))
                sp, cg = _model_historical_traffic(h_dt, ff, str(d["district_id"]), precip_mm, wind_ms)
                dist_speeds.append(sp)
                dist_congs.append(cg)
            if not dist_speeds:
                sp, cg = _model_historical_traffic(h_dt, DEFAULT_LONDON_FREE_FLOW_KMH, "all", precip_mm, wind_ms)
                dist_speeds, dist_congs = [sp], [cg]

            domain_rows.append(
                (
                    rec_utc,
                    h_dt,
                    BATCH_ID_API_BACKFILL,
                    "traffic",
                    "avg_speed_kmh",
                    round(sum(dist_speeds) / len(dist_speeds), 2),
                    round(min(dist_speeds), 2),
                    round(max(dist_speeds), 2),
                    "km/h",
                    1,
                    "TfL Diurnal + Historical Weather (Backfill Model)",
                    json.dumps(
                        {"congestion_pct": round(sum(dist_congs) / len(dist_congs), 1)},
                        separators=(",", ":"),
                    ),
                )
            )
            if include_districts:
                for d, sp, cg in zip(districts, dist_speeds, dist_congs):
                    tr_st = "critical" if (sp < 12.0 or cg > 82.0) else "normal"
                    district_rows.append(
                        (
                            rec_utc,
                            h_dt,
                            BATCH_ID_API_BACKFILL,
                            str(d["district_id"]),
                            str(d["district_name"]),
                            str(d.get("zone_type") or "urban"),
                            "traffic",
                            sp,
                            sp,
                            sp,
                            1,
                            tr_st,
                            0,
                        )
                    )
            filled_hours.add(h_key)
            sources_used.add("TfL Diurnal Model")

    return (
        domain_rows,
        district_rows,
        {
            "api_hours_filled": len(filled_hours),
            "api_sources_used": sorted(sources_used),
            "weather_api_ok": weather_ok,
            "aqi_api_ok": aqi_ok,
            "elexon_api_ok": elexon_ok,
            "neso_api_ok": neso_ok,
        },
    )


async def sync_missing_to_mssql(
    *,
    metadata_path: str | Path,
    start_date: str | None = None,
    end_date: str | None = None,
    hours: int | None = None,
    source_mode: str = "auto",
    include_districts: bool = True,
    allow_traffic_model: bool | None = None,
    trigger: str = "manual",
    http_client: httpx.AsyncClient | None = None,
    now_london_override: datetime | None = None,
) -> dict[str, Any]:
    """Synchronize missing past hours from 24/7 Cloud Collector & official APIs into MSSQL."""
    cfg = backfill_settings()
    eff_allow_traffic_model = (
        cfg["allow_traffic_model"] if allow_traffic_model is None else bool(allow_traffic_model)
    )
    if source_mode not in ("auto", "cloud_only", "api_only"):
        raise ValueError("source_mode must be one of: auto, cloud_only, api_only")

    m_cfg = mssql_config()
    if not m_cfg["enabled"] or pymssql is None:
        return {
            "ok": False,
            "skipped": True,
            "reason": "MSSQL is disabled or pymssql unavailable",
        }

    now_lon = now_london_override or london_now()
    today_lon = now_lon.date()

    if start_date or end_date:
        s_date, e_date = resolve_date_range(start_date=start_date, end_date=end_date, today=today_lon)
        window_start_lon = datetime(s_date.year, s_date.month, s_date.day, 0, 0, 0)
        window_end_lon = (
            now_lon.replace(tzinfo=None, microsecond=0)
            if e_date >= today_lon
            else datetime(e_date.year, e_date.month, e_date.day, 23, 59, 59)
        )
    else:
        lookback_h = max(1, min(744, int(hours if hours is not None else cfg["auto_hours"])))
        window_end_lon = now_lon.replace(tzinfo=None, microsecond=0)
        window_start_lon = (window_end_lon - timedelta(hours=lookback_h)).replace(minute=0, second=0, microsecond=0)
        s_date = window_start_lon.date()
        e_date = window_end_lon.date()

    async with _SYNC_LOCK:
        _BACKFILL_STATUS["running"] = True
        _BACKFILL_STATUS["last_trigger"] = trigger
        _BACKFILL_STATUS["last_error"] = None
        try:
            schema_ok = await asyncio.to_thread(_check_schema_ready)
            if not schema_ok:
                result = {
                    "ok": False,
                    "skipped": True,
                    "reason": "MSSQL tables not initialized yet",
                }
                _BACKFILL_STATUS["last_result"] = result
                return result

            districts = load_districts_metadata(metadata_path)
            districts_by_id = {str(d["district_id"]): d for d in districts}

            coverage = await asyncio.to_thread(_inspect_existing_mssql_coverage, window_start_lon, window_end_lon)

            cloud_dom_rows: list[tuple[Any, ...]] = []
            cloud_dist_rows: list[tuple[Any, ...]] = []
            upgrade_api_hours: list[tuple[datetime, datetime, str]] = []
            cloud_fetch_stats: dict[str, Any] = {"local_files_read": 0, "remote_files_read": 0, "snapshots_loaded": 0}
            cloud_build_stats: dict[str, Any] = {
                "cloud_snapshots_synced": 0,
                "upgraded_hour_domains": 0,
                "last_cloud_snapshot_at": None,
            }

            if source_mode in ("auto", "cloud_only"):
                snapshots, cloud_fetch_stats = await fetch_cloud_snapshots(
                    s_date,
                    e_date,
                    window_start_lon=window_start_lon,
                    window_end_lon=window_end_lon,
                    http_client=http_client,
                    settings=cfg,
                )
                if snapshots:
                    cloud_dom_rows, cloud_dist_rows, upgrade_api_hours, cloud_build_stats = _build_cloud_rows(
                        snapshots=snapshots,
                        districts_by_id=districts_by_id,
                        coverage=coverage,
                        include_districts=include_districts,
                        allow_traffic_model=eff_allow_traffic_model,
                    )

            api_dom_rows: list[tuple[Any, ...]] = []
            api_dist_rows: list[tuple[Any, ...]] = []
            api_stats: dict[str, Any] = {"api_hours_filled": 0, "api_sources_used": []}

            if source_mode in ("auto", "api_only"):
                api_dom_rows, api_dist_rows, api_stats = await _build_api_backfill_rows(
                    start_date=s_date,
                    end_date=e_date,
                    window_start_lon=window_start_lon,
                    window_end_lon=window_end_lon,
                    now_lon=now_lon,
                    districts=districts,
                    coverage=coverage,
                    include_districts=include_districts,
                    allow_traffic_model=eff_allow_traffic_model,
                    http_client=http_client,
                )

            all_dom_rows = cloud_dom_rows + api_dom_rows
            all_dist_rows = cloud_dist_rows + api_dist_rows

            if all_dom_rows or all_dist_rows or upgrade_api_hours:
                await asyncio.to_thread(
                    _write_backfill_rows,
                    domain_rows=all_dom_rows,
                    district_rows=all_dist_rows,
                    upgrade_api_hours=upgrade_api_hours,
                )

            result = {
                "ok": True,
                "trigger": trigger,
                "source_mode": source_mode,
                "allow_traffic_model": eff_allow_traffic_model,
                "include_districts": include_districts,
                "range": {
                    "start_date": s_date.isoformat(),
                    "end_date": e_date.isoformat(),
                    "window_start_london": window_start_lon.isoformat(),
                    "window_end_london": window_end_lon.isoformat(),
                },
                "cloud_snapshots_loaded": cloud_fetch_stats["snapshots_loaded"],
                "cloud_snapshots_synced": cloud_build_stats["cloud_snapshots_synced"],
                "cloud_files_read": cloud_fetch_stats["local_files_read"] + cloud_fetch_stats["remote_files_read"],
                "upgraded_hour_domains": cloud_build_stats["upgraded_hour_domains"],
                "last_cloud_snapshot_at": cloud_build_stats["last_cloud_snapshot_at"],
                "api_hours_filled": api_stats["api_hours_filled"],
                "api_sources_used": api_stats["api_sources_used"],
                "domain_rows_inserted": len(all_dom_rows),
                "district_rows_inserted": len(all_dist_rows),
                "total_rows_inserted": len(all_dom_rows) + len(all_dist_rows),
            }
            _BACKFILL_STATUS["last_run_at"] = datetime.now(timezone.utc).isoformat()
            _BACKFILL_STATUS["last_result"] = result

            logger.info(
                "MSSQL sync [%s] done (%s..%s) | cloud_snaps=%d | api_hours=%d | upgraded=%d | inserted=%d rows (%d dom + %d dist)",
                trigger,
                s_date.isoformat(),
                e_date.isoformat(),
                result["cloud_snapshots_synced"],
                result["api_hours_filled"],
                result["upgraded_hour_domains"],
                result["total_rows_inserted"],
                result["domain_rows_inserted"],
                result["district_rows_inserted"],
            )
            return result
        except Exception as exc:
            _BACKFILL_STATUS["last_run_at"] = datetime.now(timezone.utc).isoformat()
            _BACKFILL_STATUS["last_error"] = str(exc)
            logger.warning("MSSQL backfill sync failed (%s): %s", trigger, exc)
            raise
        finally:
            _BACKFILL_STATUS["running"] = False
