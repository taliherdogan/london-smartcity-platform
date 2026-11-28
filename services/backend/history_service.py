"""Greater London Smart City — Historical Data & Cold-Path Lake Query Service.

Combines two data layers transparently:
1. Cold-Path Parquet Data Lake (`/data/lake/parquet/dt=YYYY-MM-DD-HH/domain=...`)
   written by Apache Spark Structured Streaming (checked first with partition pruning).
2. Official Zero-Key Historical Open APIs for dates/hours not archived locally:
   - Weather: Open-Meteo Forecast (recent <= 85d) & Open-Meteo Archive (older)
   - Air Quality: Open-Meteo Air Quality Historical API
   - Energy: Elexon BMRS Historical Demand Outturn + NESO UK Carbon Intensity (Region 13: London)
   - Traffic: Data Lake Parquet when available, otherwise deterministic TfL London
     weekday/weekend diurnal speed curve modulated by real historical weather (rain/wind).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx

try:
    import pyarrow.dataset as ds
    import pyarrow.parquet as pq
except ImportError:  # pragma: no cover - optional in slim local environments
    ds = None
    pq = None

from mssql_reader import read_mssql_hourly

logger = logging.getLogger("history_service")

LONDON_LAT = 51.5074
LONDON_LON = -0.1278
LONDON_ENERGY_SHARE = 0.14
DEFAULT_LONDON_FREE_FLOW_KMH = 36.3
MAX_RANGE_DAYS = 31
MAX_LOOKBACK_DAYS = 365

TR_MONTHS_SHORT = {
    1: "Oca",
    2: "Şub",
    3: "Mar",
    4: "Nis",
    5: "May",
    6: "Haz",
    7: "Tem",
    8: "Ağu",
    9: "Eyl",
    10: "Eki",
    11: "Kas",
    12: "Ara",
}

_DISTRICTS_CACHE: list[dict[str, Any]] | None = None

# Thread-safe in-memory cache for Parquet domain partition summaries.
# Key: str(dom_dir) -> (mtime_ns, {district_id: (sum, count)}, total_sum, total_count)
_LAKE_DOMAIN_CACHE: dict[str, tuple[int, dict[str, tuple[float, int]], float, int]] = {}
_LAKE_CACHE_LOCK = threading.Lock()
_LAKE_CACHE_MAX_ENTRIES = 5000

# In-memory TTL cache for external historical API responses (shared across districts for Elexon/NESO).
# Key: tuple -> (expires_at_monotonic, data, ok_flag)
_API_CACHE: dict[tuple[Any, ...], tuple[float, Any, bool]] = {}
_API_CACHE_MAX_ENTRIES = 256


def _api_cache_get(key: tuple[Any, ...]) -> tuple[Any, bool] | None:
    entry = _API_CACHE.get(key)
    if entry is None:
        return None
    expires_at, data, ok = entry
    if time.monotonic() >= expires_at:
        _API_CACHE.pop(key, None)
        return None
    return data, ok


def _api_cache_set(key: tuple[Any, ...], data: Any, ok: bool, ttl_sec: float) -> None:
    if not ok:
        return
    if len(_API_CACHE) >= _API_CACHE_MAX_ENTRIES:
        # Evict oldest ~25% entries
        now = time.monotonic()
        expired = [k for k, (exp, _, _) in _API_CACHE.items() if exp <= now]
        for k in expired:
            _API_CACHE.pop(k, None)
        if len(_API_CACHE) >= _API_CACHE_MAX_ENTRIES:
            for k in list(_API_CACHE.keys())[: _API_CACHE_MAX_ENTRIES // 4]:
                _API_CACHE.pop(k, None)
    _API_CACHE[key] = (time.monotonic() + ttl_sec, data, ok)


def _uk_utc_offset(dt_utc: datetime) -> timedelta:
    """Return GMT (+0) or BST (+1) offset for a UTC datetime without tzdata dependency."""
    year = dt_utc.year
    mar31 = datetime(year, 3, 31, 1, 0, tzinfo=timezone.utc)
    bst_start = mar31 - timedelta(days=(mar31.weekday() + 1) % 7)
    oct31 = datetime(year, 10, 31, 1, 0, tzinfo=timezone.utc)
    bst_end = oct31 - timedelta(days=(oct31.weekday() + 1) % 7)
    return timedelta(hours=1) if bst_start <= dt_utc < bst_end else timedelta(0)


def utc_to_london(dt_utc: datetime) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt_utc.astimezone(timezone.utc)
    return dt_utc.astimezone(timezone(_uk_utc_offset(dt_utc)))


def london_now() -> datetime:
    return utc_to_london(datetime.now(timezone.utc))


def london_diurnal_traffic_factor(hour: int, weekday: int) -> float:
    """Greater London time-of-day speed factor (matches api_poller.py & streaming_app.py)."""
    is_weekend = weekday >= 5
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


def load_districts_metadata(metadata_path: str | Path) -> list[dict[str, Any]]:
    global _DISTRICTS_CACHE
    if _DISTRICTS_CACHE is not None:
        return _DISTRICTS_CACHE

    candidates: list[Path] = [Path(metadata_path)]
    resolved = Path(__file__).resolve()
    if len(resolved.parents) > 2:
        candidates.append(resolved.parents[2] / "data" / "metadata" / "london_districts.json")
    for path in candidates:
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, list) and data:
                    _DISTRICTS_CACHE = data
                    return data
            except Exception as exc:
                logger.warning("Failed reading districts metadata from %s: %s", path, exc)
    return []


def _format_tr_date(d: date) -> str:
    return f"{d.day} {TR_MONTHS_SHORT[d.month]}"


def _format_tr_full_date(d: date) -> str:
    return f"{d.day} {TR_MONTHS_SHORT[d.month]} {d.year}"


def _stable_unit_noise(seed_str: str) -> float:
    """Deterministic value in [-1.0, 1.0] for reproducible traffic/fallback modeling."""
    digest = hashlib.sha256(seed_str.encode("utf-8")).digest()
    raw = int.from_bytes(digest[:4], "big") / 0xFFFFFFFF
    return (raw * 2.0) - 1.0


def _parse_iso_to_london_hour_key(ts_str: str) -> str | None:
    if not ts_str:
        return None
    try:
        cleaned = ts_str.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            # Open-Meteo returns local Europe/London timestamps without offset
            return dt.strftime("%Y-%m-%dT%H:00")
        return utc_to_london(dt).strftime("%Y-%m-%dT%H:00")
    except Exception:
        return None


def resolve_date_range(
    *,
    single_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    days_ago: int | None = None,
    today: date | None = None,
) -> tuple[date, date]:
    """Validate and normalize requested historical date window."""
    today_lon = today or london_now().date()
    earliest = today_lon - timedelta(days=MAX_LOOKBACK_DAYS)

    if single_date:
        parsed = date.fromisoformat(single_date)
        s_date = e_date = parsed
    elif start_date or end_date:
        s_date = date.fromisoformat(start_date) if start_date else date.fromisoformat(end_date)  # type: ignore[arg-type]
        e_date = date.fromisoformat(end_date) if end_date else s_date
    elif days_ago is not None:
        target = today_lon - timedelta(days=max(0, days_ago))
        s_date = e_date = target
    else:
        # Default: 5 days ago
        target = today_lon - timedelta(days=5)
        s_date = e_date = target

    if s_date > e_date:
        raise ValueError("start_date cannot be after end_date")
    if s_date > today_lon:
        raise ValueError("Future dates are not supported")
    if e_date > today_lon:
        e_date = today_lon
    if s_date < earliest:
        raise ValueError(f"Dates older than {MAX_LOOKBACK_DAYS} days are not supported")
    if (e_date - s_date).days + 1 > MAX_RANGE_DAYS:
        raise ValueError(f"Date range cannot exceed {MAX_RANGE_DAYS} days")

    return s_date, e_date


def build_expected_hour_keys(start_date: date, end_date: date, now_london: datetime | None = None) -> list[str]:
    """Generate ordered London-local hourly keys (`YYYY-MM-DDTHH:00`) for the window."""
    now_lon = now_london or london_now()
    today_lon = now_lon.date()
    keys: list[str] = []
    cur = start_date
    while cur <= end_date:
        max_hour = now_lon.hour if cur == today_lon else 23
        for h in range(max_hour + 1):
            keys.append(f"{cur.isoformat()}T{h:02d}:00")
        cur += timedelta(days=1)
    return keys


def _summarize_domain_dir(dom_dir: Path) -> tuple[dict[str, tuple[float, int]], float, int]:
    """Vectorized PyArrow aggregation of a single dt=.../domain=... directory with mtime caching."""
    cache_key = str(dom_dir)
    try:
        mtime_ns = dom_dir.stat().st_mtime_ns
    except OSError:
        return {}, 0.0, 0

    with _LAKE_CACHE_LOCK:
        cached = _LAKE_DOMAIN_CACHE.get(cache_key)
        if cached is not None and cached[0] == mtime_ns:
            return cached[1], cached[2], cached[3]

    per_district: dict[str, tuple[float, int]] = {}
    total_sum = 0.0
    total_cnt = 0

    # Fast path: C++ vectorized pyarrow.dataset + group_by aggregation
    used_fast_path = False
    if ds is not None:
        try:
            dataset = ds.dataset(dom_dir, format="parquet")
            table = dataset.to_table(columns=["district_id", "value"])
            if table.num_rows > 0:
                agg = table.group_by("district_id").aggregate([("value", "sum"), ("value", "count")])
                dids = agg.column("district_id").to_pylist()
                vsums = agg.column("value_sum").to_pylist()
                vcnts = agg.column("value_count").to_pylist()
                for did, s_val, c_val in zip(dids, vsums, vcnts):
                    if did is None or s_val is None or not c_val:
                        continue
                    fs = float(s_val)
                    ic = int(c_val)
                    if math.isnan(fs) or ic <= 0:
                        continue
                    per_district[str(did)] = (fs, ic)
                    total_sum += fs
                    total_cnt += ic
            used_fast_path = True
        except Exception as exc:
            logger.debug("Dataset fast-path fallback for %s: %s", dom_dir, exc)

    # Fallback path: per-file read (handles partial/corrupt files gracefully)
    if not used_fast_path and pq is not None:
        d_sums: dict[str, float] = defaultdict(float)
        d_cnts: dict[str, int] = defaultdict(int)
        for pfile in dom_dir.glob("*.parquet"):
            try:
                table = pq.read_table(pfile, columns=["district_id", "value"])
                if table.num_rows == 0:
                    continue
                agg = table.group_by("district_id").aggregate([("value", "sum"), ("value", "count")])
                for did, s_val, c_val in zip(
                    agg.column("district_id").to_pylist(),
                    agg.column("value_sum").to_pylist(),
                    agg.column("value_count").to_pylist(),
                ):
                    if did is None or s_val is None or not c_val:
                        continue
                    fs = float(s_val)
                    ic = int(c_val)
                    if math.isnan(fs) or ic <= 0:
                        continue
                    s_did = str(did)
                    d_sums[s_did] += fs
                    d_cnts[s_did] += ic
                    total_sum += fs
                    total_cnt += ic
            except Exception as exc:
                logger.debug("Skipping unreadable parquet file %s: %s", pfile, exc)
        per_district = {k: (d_sums[k], d_cnts[k]) for k in d_sums}

    with _LAKE_CACHE_LOCK:
        if len(_LAKE_DOMAIN_CACHE) >= _LAKE_CACHE_MAX_ENTRIES:
            for k in list(_LAKE_DOMAIN_CACHE.keys())[: _LAKE_CACHE_MAX_ENTRIES // 4]:
                _LAKE_DOMAIN_CACHE.pop(k, None)
        _LAKE_DOMAIN_CACHE[cache_key] = (mtime_ns, per_district, total_sum, total_cnt)

    return per_district, total_sum, total_cnt


def read_lake_hourly(
    lake_dir: str | Path,
    start_date: date,
    end_date: date,
    district_id: str = "all",
) -> tuple[dict[str, dict[str, float]], dict[str, Any]]:
    """Read & aggregate archived Spark Parquet partitions for the requested date window.

    Uses vectorized PyArrow C++ group-by + mtime-validated in-memory partition summary cache.
    """
    parquet_root = Path(lake_dir) / "parquet"
    stats: dict[str, Any] = {
        "available": False,
        "partitions_scanned": 0,
        "records_read": 0,
        "domains_hours": {"traffic": 0, "weather": 0, "air_quality": 0, "energy": 0},
    }
    if pq is None or not parquet_root.exists():
        return {}, stats

    stats["available"] = True
    min_utc = datetime(start_date.year, start_date.month, start_date.day, 0, 0, tzinfo=timezone.utc) - timedelta(hours=2)
    max_utc = datetime(end_date.year, end_date.month, end_date.day, 23, 59, tzinfo=timezone.utc) + timedelta(hours=2)

    sums: dict[tuple[str, str], float] = defaultdict(float)
    counts: dict[tuple[str, str], int] = defaultdict(int)
    valid_domains = {"traffic", "weather", "air_quality", "energy"}

    try:
        dt_dirs = sorted(parquet_root.glob("dt=*"))
    except OSError:
        return {}, stats

    for dt_dir in dt_dirs:
        if not dt_dir.is_dir():
            continue
        bucket = dt_dir.name.removeprefix("dt=")
        try:
            part_utc = datetime.strptime(bucket, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if part_utc < min_utc or part_utc > max_utc:
            continue

        lon_dt = utc_to_london(part_utc)
        if not (start_date <= lon_dt.date() <= end_date):
            continue
        hour_key = lon_dt.strftime("%Y-%m-%dT%H:00")
        stats["partitions_scanned"] += 1

        for dom_dir in dt_dir.glob("domain=*"):
            domain = dom_dir.name.removeprefix("domain=")
            if domain not in valid_domains or not dom_dir.is_dir():
                continue
            per_district, total_sum, total_cnt = _summarize_domain_dir(dom_dir)
            if district_id == "all":
                if total_cnt > 0:
                    sums[(hour_key, domain)] += total_sum
                    counts[(hour_key, domain)] += total_cnt
                    stats["records_read"] += total_cnt
            else:
                d_entry = per_district.get(district_id)
                if d_entry is not None and d_entry[1] > 0:
                    sums[(hour_key, domain)] += d_entry[0]
                    counts[(hour_key, domain)] += d_entry[1]
                    stats["records_read"] += d_entry[1]

    hourly_by_key: dict[str, dict[str, float]] = defaultdict(dict)
    for (hour_key, domain), total in sums.items():
        cnt = counts[(hour_key, domain)]
        if cnt > 0:
            hourly_by_key[hour_key][domain] = round(total / cnt, 2)
            stats["domains_hours"][domain] += 1

    return dict(hourly_by_key), stats


async def _fetch_open_meteo_weather(
    client: httpx.AsyncClient,
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
    today_lon: date,
) -> tuple[dict[str, dict[str, float]], bool]:
    """Fetch real hourly London weather from Open-Meteo Forecast (<=85d) or Archive (>85d)."""
    params = (
        f"?latitude={lat:.4f}&longitude={lon:.4f}"
        f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
        f"&start_date={start_date.isoformat()}&end_date={end_date.isoformat()}"
        f"&wind_speed_unit=ms&timezone=Europe%2FLondon"
    )
    use_archive_first = (today_lon - start_date).days > 85
    urls = (
        [
            f"https://archive-api.open-meteo.com/v1/archive{params}",
            f"https://api.open-meteo.com/v1/forecast{params}",
        ]
        if use_archive_first
        else [
            f"https://api.open-meteo.com/v1/forecast{params}",
            f"https://archive-api.open-meteo.com/v1/archive{params}",
        ]
    )

    for url in urls:
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            payload = resp.json()
            hourly = payload.get("hourly") or {}
            times = hourly.get("time") or []
            temps = hourly.get("temperature_2m") or []
            hums = hourly.get("relative_humidity_2m") or []
            winds = hourly.get("wind_speed_10m") or []
            precips = hourly.get("precipitation") or []

            out: dict[str, dict[str, float]] = {}
            for idx, ts in enumerate(times):
                t_val = temps[idx] if idx < len(temps) else None
                if t_val is None:
                    continue
                key = _parse_iso_to_london_hour_key(str(ts))
                if not key:
                    continue
                out[key] = {
                    "weather": round(float(t_val), 2),
                    "humidity_pct": round(float(hums[idx]), 1) if idx < len(hums) and hums[idx] is not None else 75.0,
                    "wind_speed_ms": round(float(winds[idx]), 2) if idx < len(winds) and winds[idx] is not None else 4.0,
                    "precipitation_mm": round(float(precips[idx]), 2) if idx < len(precips) and precips[idx] is not None else 0.0,
                }
            if out:
                return out, True
        except Exception as exc:
            logger.warning("Open-Meteo weather historical fetch failed (%s): %s", url.split("?")[0], exc)

    return {}, False


async def _fetch_open_meteo_air_quality(
    client: httpx.AsyncClient,
    lat: float,
    lon: float,
    start_date: date,
    end_date: date,
) -> tuple[dict[str, dict[str, float]], bool]:
    """Fetch real hourly London European AQI, PM2.5, PM10, NO2 from Open-Meteo Air Quality API."""
    url = (
        f"https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={lat:.4f}&longitude={lon:.4f}"
        f"&hourly=european_aqi,pm2_5,pm10,nitrogen_dioxide"
        f"&start_date={start_date.isoformat()}&end_date={end_date.isoformat()}"
        f"&timezone=Europe%2FLondon"
    )
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        payload = resp.json()
        hourly = payload.get("hourly") or {}
        times = hourly.get("time") or []
        aqis = hourly.get("european_aqi") or []
        pm25s = hourly.get("pm2_5") or []
        pm10s = hourly.get("pm10") or []
        no2s = hourly.get("nitrogen_dioxide") or []

        out: dict[str, dict[str, float]] = {}
        for idx, ts in enumerate(times):
            aqi_val = aqis[idx] if idx < len(aqis) else None
            if aqi_val is None:
                continue
            key = _parse_iso_to_london_hour_key(str(ts))
            if not key:
                continue
            out[key] = {
                "air_quality": round(float(aqi_val), 1),
                "pm2_5": round(float(pm25s[idx]), 2) if idx < len(pm25s) and pm25s[idx] is not None else 6.0,
                "pm10": round(float(pm10s[idx]), 2) if idx < len(pm10s) and pm10s[idx] is not None else 11.0,
                "no2": round(float(no2s[idx]), 2) if idx < len(no2s) and no2s[idx] is not None else 16.0,
            }
        return out, bool(out)
    except Exception as exc:
        logger.warning("Open-Meteo air quality historical fetch failed: %s", exc)
        return {}, False


async def _fetch_elexon_demand(
    client: httpx.AsyncClient,
    start_date: date,
    end_date: date,
) -> tuple[dict[str, float], bool]:
    """Fetch real GB 30-min electricity demand outturn from Elexon BMRS and average per London hour."""
    url = (
        f"https://data.elexon.co.uk/bmrs/api/v1/demand/outturn"
        f"?settlementDateFrom={start_date.isoformat()}&settlementDateTo={end_date.isoformat()}&format=json"
    )
    try:
        resp = await client.get(url, headers={"Accept": "application/json"})
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("data", payload) if isinstance(payload, dict) else payload
        if not isinstance(rows, list):
            return {}, False

        buckets: dict[str, list[float]] = defaultdict(list)
        for item in rows:
            if not isinstance(item, dict):
                continue
            raw_val = (
                item.get("initialDemandOutturn")
                or item.get("initialTransmissionSystemDemandOutturn")
                or item.get("demand")
            )
            start_ts = item.get("startTime") or item.get("publishTime")
            if raw_val is None or not start_ts:
                continue
            val = float(raw_val)
            if val <= 0:
                continue
            key = _parse_iso_to_london_hour_key(str(start_ts))
            if key:
                buckets[key].append(val)

        out = {k: round(sum(v) / len(v), 1) for k, v in buckets.items() if v}
        return out, bool(out)
    except Exception as exc:
        logger.warning("Elexon BMRS historical demand fetch failed: %s", exc)
        return {}, False


async def _fetch_neso_london_carbon(
    client: httpx.AsyncClient,
    start_date: date,
    end_date: date,
) -> tuple[dict[str, dict[str, float]], bool]:
    """Fetch real London Region 13 historical carbon intensity & renewable share from NESO."""
    # NESO supports up to 14 days per query; chunk cleanly into <= 12-day windows
    windows: list[tuple[date, date]] = []
    cur = start_date
    while cur <= end_date:
        win_end = min(end_date, cur + timedelta(days=11))
        windows.append((cur, win_end))
        cur = win_end + timedelta(days=1)

    async def _fetch_window(w_start: date, w_end: date) -> list[dict[str, Any]]:
        from_iso = f"{w_start.isoformat()}T00:00Z"
        to_dt = w_end + timedelta(days=1)
        to_iso = f"{to_dt.isoformat()}T00:00Z"
        url = f"https://api.carbonintensity.org.uk/regional/intensity/{from_iso}/{to_iso}/regionid/13"
        try:
            resp = await client.get(url, headers={"Accept": "application/json"})
            resp.raise_for_status()
            payload = resp.json()
            region_obj = payload.get("data")
            if isinstance(region_obj, dict) and isinstance(region_obj.get("data"), list):
                return region_obj["data"]
            if isinstance(region_obj, list) and region_obj and isinstance(region_obj[0], dict):
                return region_obj[0].get("data", [])
        except Exception as exc:
            logger.warning("NESO London historical fetch failed (%s..%s): %s", w_start, w_end, exc)
        return []

    chunks = await asyncio.gather(*(_fetch_window(ws, we) for ws, we in windows))
    carbon_buckets: dict[str, list[float]] = defaultdict(list)
    ren_buckets: dict[str, list[float]] = defaultdict(list)
    renewable_fuels = {"wind", "solar", "hydro", "biomass"}

    for slots in chunks:
        for slot in slots:
            ts = slot.get("from")
            key = _parse_iso_to_london_hour_key(str(ts)) if ts else None
            if not key:
                continue
            intensity = slot.get("intensity") or {}
            c_val = intensity.get("actual")
            if c_val is None:
                c_val = intensity.get("forecast")
            if c_val is not None:
                carbon_buckets[key].append(float(c_val))
            gen_mix = slot.get("generationmix") or []
            if gen_mix:
                ren_pct = sum(
                    float(g.get("perc", 0.0))
                    for g in gen_mix
                    if str(g.get("fuel", "")).lower() in renewable_fuels
                )
                ren_buckets[key].append(ren_pct)

    out: dict[str, dict[str, float]] = {}
    for key in set(carbon_buckets.keys()) | set(ren_buckets.keys()):
        c_list = carbon_buckets.get(key)
        r_list = ren_buckets.get(key)
        out[key] = {
            "carbon_intensity_gco2": round(sum(c_list) / len(c_list), 1) if c_list else 180.0,
            "renewable_share_pct": round(sum(r_list) / len(r_list), 1) if r_list else 34.0,
        }
    return out, bool(out)


def _fallback_weather_point(hour_dt: datetime, temp_offset: float) -> dict[str, float]:
    """Seasonal + diurnal London weather fallback if Open-Meteo is unreachable."""
    doy = hour_dt.timetuple().tm_yday
    seasonal_avg = 11.5 - 6.5 * math.cos(2.0 * math.pi * (doy - 20) / 365.0)
    diurnal = -2.4 * math.cos(2.0 * math.pi * (hour_dt.hour - 4) / 24.0)
    day_var = _stable_unit_noise(f"w|{hour_dt.date().isoformat()}") * 2.2
    temp = round(seasonal_avg + diurnal + day_var + temp_offset, 2)
    return {
        "weather": temp,
        "humidity_pct": round(max(45.0, min(95.0, 78.0 - diurnal * 2.5)), 1),
        "wind_speed_ms": round(max(1.2, 4.4 + _stable_unit_noise(f"wind|{hour_dt.isoformat()}") * 1.8), 2),
        "precipitation_mm": 0.0,
    }


def _fallback_aqi_point(hour_dt: datetime, aqi_factor: float) -> dict[str, float]:
    rush = 1.15 if hour_dt.hour in (7, 8, 9, 17, 18, 19) else 0.94
    noise = _stable_unit_noise(f"aqi|{hour_dt.isoformat()}") * 4.5
    aqi = round(max(10.0, (27.0 * rush + noise) * aqi_factor), 1)
    return {
        "air_quality": aqi,
        "pm2_5": round(max(2.0, aqi * 0.24), 2),
        "pm10": round(max(4.0, aqi * 0.43), 2),
        "no2": round(max(4.0, aqi * 0.62), 2),
    }


def _fallback_national_mw(hour_dt: datetime) -> float:
    """Realistic GB electricity diurnal demand profile (MW) when Elexon is unreachable."""
    h = hour_dt.hour
    is_weekend = hour_dt.weekday() >= 5
    base = 29500.0 if is_weekend else 32500.0
    if 0 <= h <= 5:
        factor = 0.76
    elif 6 <= h <= 8:
        factor = 0.95
    elif 9 <= h <= 15:
        factor = 1.04
    elif 16 <= h <= 19:
        factor = 1.16
    elif 20 <= h <= 21:
        factor = 1.05
    else:
        factor = 0.88
    noise = _stable_unit_noise(f"en|{hour_dt.isoformat()}") * 850.0
    return round(base * factor + noise, 1)


def _model_historical_traffic(
    hour_dt: datetime,
    free_flow_kmh: float,
    district_id: str,
    precip_mm: float,
    wind_ms: float,
) -> tuple[float, float]:
    """Deterministic TfL London hourly traffic speed model modulated by real historical weather."""
    diurnal = london_diurnal_traffic_factor(hour_dt.hour, hour_dt.weekday())
    rain_mult = max(0.84, 1.0 - min(max(precip_mm, 0.0), 8.0) * 0.022) if precip_mm > 0.1 else 1.0
    wind_mult = max(0.91, 1.0 - (wind_ms - 10.0) * 0.012) if wind_ms > 10.0 else 1.0
    corridor_var = 1.0 + _stable_unit_noise(f"tr|{district_id}|{hour_dt.strftime('%Y-%m-%dT%H')}") * 0.045
    speed = free_flow_kmh * diurnal * rain_mult * wind_mult * corridor_var
    speed = round(max(6.0, min(free_flow_kmh * 1.08, speed)), 2)
    congestion = round(max(6.0, min(96.0, (1.0 - speed / (free_flow_kmh * 1.05)) * 100.0)), 1)
    return speed, congestion


def _extract_live_domain_values(
    latest_state: dict[str, Any] | None,
    district_id: str,
) -> dict[str, float | None]:
    out: dict[str, float | None] = {"traffic": None, "weather": None, "air_quality": None, "energy": None}
    if not isinstance(latest_state, dict):
        return out
    if district_id == "all":
        domains = latest_state.get("domains") or {}
        for dom in out:
            val = (domains.get(dom) or {}).get("avg_value")
            if val is not None:
                out[dom] = float(val)
    else:
        for d in latest_state.get("districts") or []:
            if d.get("district_id") == district_id:
                metrics = d.get("metrics") or {}
                for dom in out:
                    val = (metrics.get(dom) or {}).get("avg")
                    if val is not None:
                        out[dom] = float(val)
                break
    return out


def _aggregate_daily(hourly_series: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in hourly_series:
        by_day[row["date"]].append(row)

    daily: list[dict[str, Any]] = []
    for d_str in sorted(by_day.keys()):
        rows = by_day[d_str]
        d_obj = date.fromisoformat(d_str)

        def _avg(field: str, ndigits: int = 2) -> float | None:
            vals = [float(r[field]) for r in rows if r.get(field) is not None]
            return round(sum(vals) / len(vals), ndigits) if vals else None

        def _sum(field: str, ndigits: int = 2) -> float:
            vals = [float(r[field]) for r in rows if r.get(field) is not None]
            return round(sum(vals), ndigits) if vals else 0.0

        temps = [float(r["weather"]) for r in rows if r.get("weather") is not None]
        speeds = [float(r["traffic"]) for r in rows if r.get("traffic") is not None]

        daily.append(
            {
                "timestamp": d_str,
                "date": d_str,
                "hour": None,
                "label": _format_tr_date(d_obj),
                "full_label": _format_tr_full_date(d_obj),
                "traffic": _avg("traffic", 2),
                "traffic_min": round(min(speeds), 1) if speeds else None,
                "traffic_max": round(max(speeds), 1) if speeds else None,
                "weather": _avg("weather", 2),
                "weather_min": round(min(temps), 1) if temps else None,
                "weather_max": round(max(temps), 1) if temps else None,
                "air_quality": _avg("air_quality", 1),
                "energy": _avg("energy", 1),
                "humidity_pct": _avg("humidity_pct", 1),
                "wind_speed_ms": _avg("wind_speed_ms", 2),
                "precipitation_mm": _sum("precipitation_mm", 2),
                "congestion_pct": _avg("congestion_pct", 1),
                "pm2_5": _avg("pm2_5", 2),
                "pm10": _avg("pm10", 2),
                "renewable_share_pct": _avg("renewable_share_pct", 1),
                "carbon_intensity_gco2": _avg("carbon_intensity_gco2", 1),
                "london_total_mw": _avg("london_total_mw", 1),
                "lake_backed": any(bool(r.get("lake_backed")) for r in rows),
                "hours_count": len(rows),
            }
        )
    return daily


def _summarize_domain(
    series: list[dict[str, Any]],
    key: str,
    unit: str,
    source: str,
    source_type: str,
    lake_hours: int,
    total_hours: int,
    live_value: float | None,
    secondary: dict[str, Any],
) -> dict[str, Any]:
    points = [(row["label"], float(row[key])) for row in series if row.get(key) is not None]
    if not points:
        return {
            "avg": None,
            "min": None,
            "max": None,
            "min_time": None,
            "max_time": None,
            "unit": unit,
            "source": source,
            "source_type": source_type,
            "lake_hours": lake_hours,
            "total_hours": total_hours,
            "live_value": live_value,
            "delta_vs_live": None,
            "secondary": secondary,
        }

    vals = [v for _, v in points]
    avg_val = round(sum(vals) / len(vals), 2)
    min_label, min_val = min(points, key=lambda x: x[1])
    max_label, max_val = max(points, key=lambda x: x[1])
    delta = round(avg_val - live_value, 2) if live_value is not None else None

    return {
        "avg": avg_val,
        "min": round(min_val, 2),
        "max": round(max_val, 2),
        "min_time": min_label,
        "max_time": max_label,
        "unit": unit,
        "source": source,
        "source_type": source_type,
        "lake_hours": lake_hours,
        "total_hours": total_hours,
        "live_value": round(live_value, 2) if live_value is not None else None,
        "delta_vs_live": delta,
        "secondary": secondary,
    }


async def query_historical_data(
    *,
    lake_dir: str | Path,
    metadata_path: str | Path,
    single_date: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    days_ago: int | None = None,
    district_id: str = "all",
    granularity: str = "auto",
    latest_state: dict[str, Any] | None = None,
    http_client: httpx.AsyncClient | None = None,
    bypass_api_cache: bool = False,
) -> dict[str, Any]:
    """Build complete historical analytics payload combining Data Lake + real historical APIs."""
    now_lon = london_now()
    today_lon = now_lon.date()
    s_date, e_date = resolve_date_range(
        single_date=single_date,
        start_date=start_date,
        end_date=end_date,
        days_ago=days_ago,
        today=today_lon,
    )
    days_count = (e_date - s_date).days + 1
    eff_granularity = (
        ("daily" if days_count > 7 else "hourly")
        if granularity == "auto"
        else granularity
    )

    districts = load_districts_metadata(metadata_path)
    districts_by_id = {d["district_id"]: d for d in districts}
    if district_id != "all" and district_id not in districts_by_id:
        raise ValueError(f"Unknown district_id: {district_id}")

    selected_district = districts_by_id.get(district_id)
    if selected_district:
        lat = float(selected_district["lat"])
        lon = float(selected_district["lon"])
        free_flow_kmh = float(selected_district.get("free_flow_kmh", DEFAULT_LONDON_FREE_FLOW_KMH))
        energy_weight = float(selected_district.get("energy_weight", 1.0 / 32.0))
        temp_offset = float(selected_district.get("temp_offset_c", 0.0))
        aqi_factor = float(selected_district.get("aqi_factor", 1.0))
        district_name = str(selected_district["district_name"])
    else:
        lat = LONDON_LAT
        lon = LONDON_LON
        free_flow_kmh = (
            round(sum(float(d.get("free_flow_kmh", 36.0)) for d in districts) / len(districts), 2)
            if districts
            else DEFAULT_LONDON_FREE_FLOW_KMH
        )
        energy_weight = 1.0 / max(len(districts), 32)
        temp_offset = 0.0
        aqi_factor = 1.0
        district_name = "Tüm Londra (32 Bölge Ort.)"

    hour_keys = build_expected_hour_keys(s_date, e_date, now_lon)
    total_hours = len(hour_keys)

    # 1. Read Spark Parquet Data Lake & MSSQL archive in background threads + fetch APIs concurrently
    lake_task = asyncio.to_thread(read_lake_hourly, lake_dir, s_date, e_date, district_id)
    mssql_task = asyncio.to_thread(read_mssql_hourly, s_date, e_date, district_id)

    api_ttl = 300.0 if e_date >= today_lon else 3600.0
    s_iso = s_date.isoformat()
    e_iso = e_date.isoformat()
    lat_r = round(lat, 3)
    lon_r = round(lon, 3)

    async def _cached_fetch(cache_key: tuple[Any, ...], coro_factory):
        if not bypass_api_cache:
            hit = _api_cache_get(cache_key)
            if hit is not None:
                return hit
        data, ok = await coro_factory()
        _api_cache_set(cache_key, data, ok, api_ttl)
        return data, ok

    async def _fetch_all_apis(client: httpx.AsyncClient):
        return await asyncio.gather(
            _cached_fetch(
                ("weather", lat_r, lon_r, s_iso, e_iso),
                lambda: _fetch_open_meteo_weather(client, lat, lon, s_date, e_date, today_lon),
            ),
            _cached_fetch(
                ("aqi", lat_r, lon_r, s_iso, e_iso),
                lambda: _fetch_open_meteo_air_quality(client, lat, lon, s_date, e_date),
            ),
            _cached_fetch(
                ("elexon", s_iso, e_iso),
                lambda: _fetch_elexon_demand(client, s_date, e_date),
            ),
            _cached_fetch(
                ("neso", s_iso, e_iso),
                lambda: _fetch_neso_london_carbon(client, s_date, e_date),
            ),
        )

    if http_client is not None:
        lake_res, mssql_res, api_res = await asyncio.gather(lake_task, mssql_task, _fetch_all_apis(http_client))
    else:
        async with httpx.AsyncClient(timeout=httpx.Timeout(8.0, connect=4.0), follow_redirects=True) as client:
            lake_res, mssql_res, api_res = await asyncio.gather(lake_task, mssql_task, _fetch_all_apis(client))

    lake_hourly, lake_stats = lake_res
    mssql_hourly, mssql_stats = mssql_res
    # Merge MSSQL persistent archive into local archive map (MSSQL takes precedence for overlapping hours)
    merged_archive: dict[str, dict[str, float]] = {k: dict(v) for k, v in lake_hourly.items()}
    for h_key, dom_map in mssql_hourly.items():
        merged_archive.setdefault(h_key, {}).update(dom_map)
    lake_hourly = merged_archive
    if mssql_stats.get("records_read", 0) > 0:
        lake_stats["available"] = True
        lake_stats["records_read"] = int(lake_stats.get("records_read", 0)) + int(mssql_stats["records_read"])
    lake_stats["mssql_available"] = bool(mssql_stats.get("available"))
    lake_stats["mssql_records_read"] = int(mssql_stats.get("records_read", 0))

    (weather_api, weather_ok), (aqi_api, aqi_ok), (elexon_api, elexon_ok), (neso_api, neso_ok) = api_res

    hourly_series: list[dict[str, Any]] = []
    lake_matched_counts = {"traffic": 0, "weather": 0, "air_quality": 0, "energy": 0}
    multi_day = days_count > 1

    for h_key in hour_keys:
        h_dt = datetime.strptime(h_key, "%Y-%m-%dT%H:%M")
        d_str = h_dt.strftime("%Y-%m-%d")
        lake_row = lake_hourly.get(h_key, {})

        # Weather
        w_api = weather_api.get(h_key) or _fallback_weather_point(h_dt, temp_offset)
        if "weather" in lake_row:
            temp_val = lake_row["weather"]
            lake_matched_counts["weather"] += 1
        else:
            temp_val = w_api["weather"]
        hum_val = w_api["humidity_pct"]
        wind_val = w_api["wind_speed_ms"]
        precip_val = w_api["precipitation_mm"]

        # Air quality
        aq_api = aqi_api.get(h_key) or _fallback_aqi_point(h_dt, aqi_factor)
        if "air_quality" in lake_row:
            aqi_val = lake_row["air_quality"]
            lake_matched_counts["air_quality"] += 1
        else:
            aqi_val = aq_api["air_quality"]

        # Energy
        nat_mw = elexon_api.get(h_key) or _fallback_national_mw(h_dt)
        london_total_mw = round(nat_mw * LONDON_ENERGY_SHARE, 1)
        if "energy" in lake_row:
            energy_val = lake_row["energy"]
            lake_matched_counts["energy"] += 1
        else:
            energy_val = round(london_total_mw * energy_weight, 2)
        neso_row = neso_api.get(h_key) or {"carbon_intensity_gco2": 180.0, "renewable_share_pct": 34.0}

        # Traffic
        model_speed, model_cong = _model_historical_traffic(
            h_dt, free_flow_kmh, district_id, precip_val, wind_val
        )
        if "traffic" in lake_row:
            traffic_val = lake_row["traffic"]
            cong_val = round(max(6.0, min(96.0, (1.0 - traffic_val / (free_flow_kmh * 1.05)) * 100.0)), 1)
            lake_matched_counts["traffic"] += 1
        else:
            traffic_val = model_speed
            cong_val = model_cong

        label = f"{_format_tr_date(h_dt.date())} {h_dt.strftime('%H:%M')}" if multi_day else h_dt.strftime("%H:%M")
        hourly_series.append(
            {
                "timestamp": h_key,
                "date": d_str,
                "hour": h_dt.hour,
                "label": label,
                "full_label": f"{_format_tr_full_date(h_dt.date())} {h_dt.strftime('%H:%M')}",
                "traffic": traffic_val,
                "weather": temp_val,
                "air_quality": aqi_val,
                "energy": energy_val,
                "humidity_pct": hum_val,
                "wind_speed_ms": wind_val,
                "precipitation_mm": precip_val,
                "congestion_pct": cong_val,
                "pm2_5": aq_api["pm2_5"],
                "pm10": aq_api["pm10"],
                "renewable_share_pct": neso_row["renewable_share_pct"],
                "carbon_intensity_gco2": neso_row["carbon_intensity_gco2"],
                "london_total_mw": london_total_mw,
                "lake_backed": bool(lake_row),
            }
        )

    final_series = _aggregate_daily(hourly_series) if eff_granularity == "daily" else hourly_series

    def _source_meta(dom: str, api_ok: bool, api_label: str, fallback_label: str) -> tuple[str, str]:
        lh = lake_matched_counts[dom]
        if lh > 0 and lh >= total_hours:
            return "Data Lake (Spark Parquet)", "lake"
        if lh > 0:
            return f"Hibrit (Data Lake + {api_label if api_ok else fallback_label})", "hybrid"
        if api_ok:
            return api_label, "api"
        return fallback_label, "model"

    tr_src, tr_type = _source_meta(
        "traffic",
        False,
        "TfL Tarihsel Model",
        "TfL Gün-Saat Profili + Gerçek Hava Etkisi" if weather_ok else "TfL Gün-Saat Profili (Model)",
    )
    w_src, w_type = _source_meta("weather", weather_ok, "Open-Meteo Tarihsel (Gerçek)", "Londra Mevsimsel Model")
    aq_src, aq_type = _source_meta("air_quality", aqi_ok, "Open-Meteo AQI Arşivi (Gerçek)", "Londra AQI Modeli")
    en_api_label = (
        "Elexon BMRS + NESO (Gerçek)"
        if (elexon_ok and neso_ok)
        else ("Elexon BMRS (Gerçek)" if elexon_ok else "NESO + Yük Profili")
    )
    en_src, en_type = _source_meta("energy", elexon_ok or neso_ok, en_api_label, "GB Şebeke Profili (Model)")

    live_vals = _extract_live_domain_values(latest_state, district_id)

    def _mean_col(field: str, ndigits: int = 1) -> float | None:
        vals = [float(r[field]) for r in hourly_series if r.get(field) is not None]
        return round(sum(vals) / len(vals), ndigits) if vals else None

    total_precip = round(sum(float(r.get("precipitation_mm") or 0.0) for r in hourly_series), 1)

    summary = {
        "traffic": _summarize_domain(
            final_series,
            "traffic",
            "km/s",
            tr_src,
            tr_type,
            lake_matched_counts["traffic"],
            total_hours,
            live_vals["traffic"],
            {
                "avg_congestion_pct": _mean_col("congestion_pct", 1),
                "free_flow_kmh": free_flow_kmh,
            },
        ),
        "weather": _summarize_domain(
            final_series,
            "weather",
            "°C",
            w_src,
            w_type,
            lake_matched_counts["weather"],
            total_hours,
            live_vals["weather"],
            {
                "avg_humidity_pct": _mean_col("humidity_pct", 1),
                "avg_wind_speed_ms": _mean_col("wind_speed_ms", 2),
                "total_precipitation_mm": total_precip,
            },
        ),
        "air_quality": _summarize_domain(
            final_series,
            "air_quality",
            "puan",
            aq_src,
            aq_type,
            lake_matched_counts["air_quality"],
            total_hours,
            live_vals["air_quality"],
            {
                "avg_pm2_5": _mean_col("pm2_5", 1),
                "avg_pm10": _mean_col("pm10", 1),
            },
        ),
        "energy": _summarize_domain(
            final_series,
            "energy",
            "MW",
            en_src,
            en_type,
            lake_matched_counts["energy"],
            total_hours,
            live_vals["energy"],
            {
                "avg_renewable_share_pct": _mean_col("renewable_share_pct", 1),
                "avg_carbon_intensity_gco2": _mean_col("carbon_intensity_gco2", 1),
                "avg_london_total_mw": _mean_col("london_total_mw", 0),
            },
        ),
    }

    any_lake_hours = sum(1 for r in hourly_series if r["lake_backed"])

    return {
        "query": {
            "start_date": s_date.isoformat(),
            "end_date": e_date.isoformat(),
            "display_range": (
                _format_tr_full_date(s_date)
                if s_date == e_date
                else f"{_format_tr_date(s_date)} – {_format_tr_full_date(e_date)}"
            ),
            "days_count": days_count,
            "days_ago_end": (today_lon - e_date).days,
            "district_id": district_id,
            "district_name": district_name,
            "granularity": eff_granularity,
            "points_count": len(final_series),
        },
        "summary": summary,
        "series": final_series,
        "lake_stats": {
            "available": lake_stats["available"],
            "partitions_scanned": lake_stats["partitions_scanned"],
            "records_read": lake_stats["records_read"],
            "mssql_available": lake_stats.get("mssql_available", False),
            "mssql_records_read": lake_stats.get("mssql_records_read", 0),
            "lake_hours_matched": any_lake_hours,
            "total_hours": total_hours,
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
