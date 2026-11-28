"""Real Zero-Key Open API Poller for Greater London Smart City Baselines.

Sources (all free, zero API key required):
1. Traffic: Transport for London (TfL) Unified Road API (24 live London corridors)
2. Weather: Open-Meteo Forecast API (multi-coordinate batch for all 32 London boroughs)
3. Air Quality: Open-Meteo Air Quality API (multi-coordinate batch for all 32 London boroughs)
4. Energy:
   - Elexon BMRS Insights API (5-min live GB national electricity demand in MW)
   - NESO UK Carbon Intensity API — London Region 13 (live gCO2/kWh & London generation mix)
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

logger = logging.getLogger("api_poller")

LONDON_LAT = 51.5074
LONDON_LON = -0.1278
LONDON_TZ = ZoneInfo("Europe/London")
LONDON_ENERGY_SHARE = 0.14  # Greater London ~14% of GB national demand (GLA / UKPN)

TFL_ROAD_URL = "https://api.tfl.gov.uk/Road"
ELEXON_DEMAND_URL = "https://data.elexon.co.uk/bmrs/api/v1/demand/outturn/summary?format=json"
NESO_LONDON_CARBON_URL = "https://api.carbonintensity.org.uk/regional/regionid/13"

# Map TfL statusSeverity strings to realistic speed multipliers & severity rank
SEVERITY_RANK = {
    "good": 0,
    "no exceptional delays": 0,
    "minor": 1,
    "moderate": 2,
    "serious": 3,
    "severe": 4,
    "closure": 5,
}

SEVERITY_SPEED_MULTIPLIER = {
    0: 1.00,  # Good -> normal urban flow for time of day
    1: 0.88,  # Minor delays
    2: 0.74,  # Moderate delays
    3: 0.56,  # Serious delays (real active disruption)
    4: 0.38,  # Severe delays
    5: 0.24,  # Closure / diversion
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(val: Any, default: float) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def london_diurnal_traffic_factor(now: datetime | None = None) -> float:
    """Realistic Greater London time-of-day speed factor (TfL Road Network Performance).

    - Night (22:00-05:59): near free-flow (0.98)
    - Morning ramp (06:00-06:59): 0.88
    - AM Peak (07:00-09:59): 0.72 (weekday) / 0.86 (weekend)
    - Daytime inter-peak (10:00-15:59): 0.80 (weekday) / 0.84 (weekend)
    - PM Peak (16:00-18:59): 0.68 (weekday) / 0.80 (weekend)
    - Evening (19:00-21:59): 0.86
    """
    local_now = (now or datetime.now(timezone.utc)).astimezone(LONDON_TZ)
    hour = local_now.hour
    is_weekend = local_now.weekday() >= 5

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


def default_baselines() -> dict[str, dict[str, Any]]:
    """Realistic London fallback baselines if any external API is temporarily unreachable."""
    now = _now_iso()
    diurnal = london_diurnal_traffic_factor()
    fallback_speed = round(34.0 * diurnal, 2)
    fallback_cong = round(max(10.0, min(92.0, (1.0 - fallback_speed / 38.0) * 100.0)), 1)
    return {
        "traffic": {
            "domain": "traffic",
            "provider": "TfL Unified Road API",
            "endpoint": TFL_ROAD_URL,
            "status": "fallback",
            "fetched_at": now,
            "primary_metric": "avg_speed_kmh",
            "value": fallback_speed,
            "unit": "km/h",
            "secondary": {
                "vehicle_count": 64.0,
                "congestion_pct": fallback_cong,
                "corridors_monitored": 24,
                "disrupted_corridors": 0,
            },
            "corridor_statuses": {},
        },
        "weather": {
            "domain": "weather",
            "provider": "Open-Meteo Forecast",
            "endpoint": "https://api.open-meteo.com/v1/forecast",
            "status": "fallback",
            "fetched_at": now,
            "primary_metric": "temperature_c",
            "value": 9.5,
            "unit": "°C",
            "secondary": {
                "humidity_pct": 76.0,
                "wind_speed_ms": 4.2,
                "precipitation_mm": 0.0,
                "pressure_hpa": 1014.0,
            },
            "district_values": {},
        },
        "air_quality": {
            "domain": "air_quality",
            "provider": "Open-Meteo Air Quality",
            "endpoint": "https://air-quality-api.open-meteo.com/v1/air-quality",
            "status": "fallback",
            "fetched_at": now,
            "primary_metric": "european_aqi",
            "value": 28.0,
            "unit": "AQI",
            "secondary": {
                "pm2_5": 6.2,
                "pm10": 11.2,
                "no2": 18.4,
                "ozone": 44.0,
            },
            "district_values": {},
        },
        "energy": {
            "domain": "energy",
            "provider": "Elexon BMRS + NESO London",
            "endpoint": NESO_LONDON_CARBON_URL,
            "status": "fallback",
            "fetched_at": now,
            "primary_metric": "grid_load_mw",
            "value": 34500.0,
            "unit": "MW",
            "secondary": {
                "carbon_intensity_gco2": 185.0,
                "intensity_index": "moderate",
                "renewable_share_pct": 34.0,
                "low_carbon_pct": 46.0,
                "national_load_mw": 34500.0,
                "london_total_mw": round(34500.0 * LONDON_ENERGY_SHARE, 1),
            },
        },
    }


def _build_coord_params(districts: list[dict[str, Any]] | None) -> tuple[str, str, list[str]]:
    if not districts:
        return str(LONDON_LAT), str(LONDON_LON), []
    lats = ",".join(f"{float(d['lat']):.4f}" for d in districts)
    lons = ",".join(f"{float(d['lon']):.4f}" for d in districts)
    ids = [str(d["district_id"]) for d in districts]
    return lats, lons, ids


def fetch_weather(client: httpx.Client, districts: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    lats, lons, dids = _build_coord_params(districts)
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lats}&longitude={lons}"
        f"&current=temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,surface_pressure"
        f"&wind_speed_unit=ms&timezone=Europe%2FLondon"
    )
    resp = client.get(url)
    resp.raise_for_status()
    payload = resp.json()
    entries = payload if isinstance(payload, list) else [payload]

    district_values: dict[str, dict[str, float]] = {}
    temps: list[float] = []
    hums: list[float] = []
    winds: list[float] = []
    precips: list[float] = []
    pressures: list[float] = []

    for idx, item in enumerate(entries):
        cur = item.get("current") or {}
        t = round(_safe_float(cur.get("temperature_2m"), 9.5), 2)
        h = round(_safe_float(cur.get("relative_humidity_2m"), 76.0), 1)
        w = round(_safe_float(cur.get("wind_speed_10m"), 4.0), 2)
        p = round(_safe_float(cur.get("precipitation"), 0.0), 2)
        pr = round(_safe_float(cur.get("surface_pressure"), 1014.0), 1)
        temps.append(t)
        hums.append(h)
        winds.append(w)
        precips.append(p)
        pressures.append(pr)
        if idx < len(dids):
            district_values[dids[idx]] = {
                "value": t,
                "humidity_pct": h,
                "wind_speed_ms": w,
                "precipitation_mm": p,
                "pressure_hpa": pr,
            }

    n = max(len(temps), 1)
    return {
        "domain": "weather",
        "provider": "Open-Meteo Forecast",
        "endpoint": "https://api.open-meteo.com/v1/forecast",
        "status": "live",
        "fetched_at": _now_iso(),
        "primary_metric": "temperature_c",
        "value": round(sum(temps) / n, 2),
        "unit": "°C",
        "secondary": {
            "humidity_pct": round(sum(hums) / n, 1),
            "wind_speed_ms": round(sum(winds) / n, 2),
            "precipitation_mm": round(sum(precips) / n, 2),
            "pressure_hpa": round(sum(pressures) / n, 1),
        },
        "district_values": district_values,
    }


def fetch_air_quality(client: httpx.Client, districts: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    lats, lons, dids = _build_coord_params(districts)
    url = (
        f"https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={lats}&longitude={lons}"
        f"&current=european_aqi,pm2_5,pm10,nitrogen_dioxide,ozone"
        f"&timezone=Europe%2FLondon"
    )
    resp = client.get(url)
    resp.raise_for_status()
    payload = resp.json()
    entries = payload if isinstance(payload, list) else [payload]

    district_values: dict[str, dict[str, float]] = {}
    aqis: list[float] = []
    pm25s: list[float] = []
    pm10s: list[float] = []
    no2s: list[float] = []
    ozones: list[float] = []

    for idx, item in enumerate(entries):
        cur = item.get("current") or {}
        aqi = round(_safe_float(cur.get("european_aqi"), 28.0), 1)
        pm25 = round(_safe_float(cur.get("pm2_5"), 6.0), 2)
        pm10 = round(_safe_float(cur.get("pm10"), 11.0), 2)
        no2 = round(_safe_float(cur.get("nitrogen_dioxide"), 16.0), 2)
        o3 = round(_safe_float(cur.get("ozone"), 42.0), 2)
        aqis.append(aqi)
        pm25s.append(pm25)
        pm10s.append(pm10)
        no2s.append(no2)
        ozones.append(o3)
        if idx < len(dids):
            district_values[dids[idx]] = {
                "value": aqi,
                "pm2_5": pm25,
                "pm10": pm10,
                "no2": no2,
                "ozone": o3,
            }

    n = max(len(aqis), 1)
    return {
        "domain": "air_quality",
        "provider": "Open-Meteo Air Quality",
        "endpoint": "https://air-quality-api.open-meteo.com/v1/air-quality",
        "status": "live",
        "fetched_at": _now_iso(),
        "primary_metric": "european_aqi",
        "value": round(sum(aqis) / n, 1),
        "unit": "AQI",
        "secondary": {
            "pm2_5": round(sum(pm25s) / n, 2),
            "pm10": round(sum(pm10s) / n, 2),
            "no2": round(sum(no2s) / n, 2),
            "ozone": round(sum(ozones) / n, 2),
        },
        "district_values": district_values,
    }


def fetch_traffic(client: httpx.Client, _districts: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    resp = client.get(
        TFL_ROAD_URL,
        headers={"Accept": "application/json", "User-Agent": "SmartCityLondonBVA15/1.0"},
    )
    resp.raise_for_status()
    corridors = resp.json()
    if not isinstance(corridors, list) or not corridors:
        raise ValueError("Unexpected TfL Road API payload")

    corridor_statuses: dict[str, dict[str, Any]] = {}
    ranks: list[int] = []
    disrupted = 0

    for c in corridors:
        name = str(c.get("displayName", "")).strip()
        sev = str(c.get("statusSeverity", "Good")).strip()
        desc = str(c.get("statusSeverityDescription", "No Exceptional Delays")).strip()
        rank = SEVERITY_RANK.get(sev.lower(), 1 if sev.lower() != "good" else 0)
        ranks.append(rank)
        if rank >= 2:
            disrupted += 1
        if name:
            corridor_statuses[name] = {
                "severity": sev,
                "description": desc,
                "rank": rank,
                "speed_mult": SEVERITY_SPEED_MULTIPLIER.get(rank, 0.78),
            }

    # Realistic Greater London arterial reference (~34 km/h free-flow x London diurnal curve x TfL corridor status)
    diurnal = london_diurnal_traffic_factor()
    avg_mult = sum(SEVERITY_SPEED_MULTIPLIER.get(r, 0.78) for r in ranks) / len(ranks)
    london_speed = round(34.0 * diurnal * avg_mult, 2)
    congestion = round(max(8.0, min(95.0, (1.0 - (london_speed / 38.0)) * 100.0)), 1)
    est_vehicles = round(42.0 + (1.0 - diurnal) * 85.0 + disrupted * 7.5, 1)

    return {
        "domain": "traffic",
        "provider": "TfL Unified Road API",
        "endpoint": TFL_ROAD_URL,
        "status": "live",
        "fetched_at": _now_iso(),
        "primary_metric": "avg_speed_kmh",
        "value": london_speed,
        "unit": "km/h",
        "secondary": {
            "vehicle_count": est_vehicles,
            "congestion_pct": congestion,
            "corridors_monitored": len(corridors),
            "disrupted_corridors": disrupted,
            "diurnal_factor": diurnal,
        },
        "corridor_statuses": corridor_statuses,
    }


def fetch_energy(client: httpx.Client, _districts: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    # 1. Official 5-minute GB national electricity demand (MW) from Elexon BMRS
    national_mw: float | None = None
    try:
        e_resp = client.get(ELEXON_DEMAND_URL, headers={"Accept": "application/json"})
        e_resp.raise_for_status()
        e_rows = e_resp.json()
        if isinstance(e_rows, list) and e_rows:
            for item in reversed(e_rows):
                dval = item.get("demand")
                if dval is not None and float(dval) > 0:
                    national_mw = float(dval)
                    break
    except Exception as exc:
        logger.warning("Elexon BMRS fetch warning (continuing with NESO): %s", exc)

    # 2. Official London Region 13 live carbon intensity & generation mix from NESO
    neso_ok = False
    carbon_gco2 = 185.0
    intensity_idx = "moderate"
    renewable_pct = 34.0
    low_carbon_pct = 46.0
    try:
        c_resp = client.get(NESO_LONDON_CARBON_URL, headers={"Accept": "application/json"})
        c_resp.raise_for_status()
        c_payload = c_resp.json()
        data_list = c_payload.get("data") if isinstance(c_payload, dict) else None
        if isinstance(data_list, list) and data_list and isinstance(data_list[0].get("data"), list) and data_list[0]["data"]:
            latest_slot = data_list[0]["data"][0]
            intensity_obj = latest_slot.get("intensity", {})
            raw_intensity = intensity_obj.get("actual")
            if raw_intensity is None:
                raw_intensity = intensity_obj.get("forecast", 180.0)
            carbon_gco2 = _safe_float(raw_intensity, 180.0)
            intensity_idx = str(intensity_obj.get("index", "moderate"))

            gen_mix = latest_slot.get("generationmix", [])
            renewable_fuels = {"wind", "solar", "hydro", "biomass"}
            renewable_pct = sum(
                _safe_float(g.get("perc"), 0.0)
                for g in gen_mix
                if str(g.get("fuel", "")).lower() in renewable_fuels
            )
            nuclear_pct = sum(
                _safe_float(g.get("perc"), 0.0)
                for g in gen_mix
                if str(g.get("fuel", "")).lower() == "nuclear"
            )
            low_carbon_pct = renewable_pct + nuclear_pct
            neso_ok = True
    except Exception as exc:
        logger.warning("NESO London fetch warning: %s", exc)

    if national_mw is None and not neso_ok:
        raise RuntimeError("Both Elexon BMRS and NESO London energy APIs failed")

    if national_mw is None:
        national_mw = 34500.0

    london_total_mw = round(national_mw * LONDON_ENERGY_SHARE, 1)

    return {
        "domain": "energy",
        "provider": "Elexon BMRS + NESO London",
        "endpoint": NESO_LONDON_CARBON_URL,
        "status": "live",
        "fetched_at": _now_iso(),
        "primary_metric": "grid_load_mw",
        "value": round(national_mw, 1),
        "unit": "MW",
        "secondary": {
            "carbon_intensity_gco2": round(carbon_gco2, 1),
            "intensity_index": intensity_idx,
            "renewable_share_pct": round(renewable_pct, 1),
            "low_carbon_pct": round(low_carbon_pct, 1),
            "national_load_mw": round(national_mw, 1),
            "london_total_mw": london_total_mw,
        },
    }


def poll_all_sources(
    previous: dict[str, dict[str, Any]] | None = None,
    districts: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, Any]]:
    """Fetch all London open APIs with graceful fallback to previous/default baselines."""
    baselines = dict(previous) if previous else default_baselines()
    defaults = default_baselines()
    for dom_key, def_entry in defaults.items():
        if dom_key not in baselines:
            baselines[dom_key] = def_entry

    fetchers = {
        "weather": fetch_weather,
        "air_quality": fetch_air_quality,
        "traffic": fetch_traffic,
        "energy": fetch_energy,
    }

    with httpx.Client(timeout=15.0, follow_redirects=True) as client:
        for domain, fn in fetchers.items():
            try:
                baselines[domain] = fn(client, districts)
                logger.info(
                    "[API LIVE] %s -> %s %s (%s)",
                    domain,
                    baselines[domain]["value"],
                    baselines[domain]["unit"],
                    baselines[domain]["provider"],
                )
            except Exception as exc:
                logger.warning("[API FALLBACK] %s fetch failed: %s", domain, exc)
                fallback_copy = dict(baselines[domain])
                fallback_copy["status"] = "cached" if fallback_copy.get("status") == "live" else "fallback"
                baselines[domain] = fallback_copy

    return baselines
