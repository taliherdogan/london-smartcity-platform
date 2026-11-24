"""24/7 Cloud Snapshot Collector for Greater London Smart City.

Designed to run on GitHub Actions cron (every 15 min) or any lightweight always-on host
while the local PC is turned off. Polls the exact same 4 zero-key official London/UK APIs
as the live Kafka producer (TfL Unified Road, Open-Meteo Weather, Open-Meteo Air Quality,
Elexon BMRS + NESO Region 13) and appends one compact, lossless JSONL snapshot per run to
`snapshots/YYYY/MM/YYYY-MM-DD.jsonl` (UTC date partitioned).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATOR_DIR = REPO_ROOT / "services" / "generator"
if str(GENERATOR_DIR) not in sys.path:
    sys.path.insert(0, str(GENERATOR_DIR))

from api_poller import (  # noqa: E402
    LONDON_ENERGY_SHARE,
    london_diurnal_traffic_factor,
    poll_all_sources,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("cloud_collector")

DEFAULT_METADATA_PATH = REPO_ROOT / "data" / "metadata" / "london_districts.json"


def utc_to_london_naive(dt_utc: datetime) -> datetime:
    """Convert UTC datetime to Europe/London naive local time (GMT/BST, no tzdata required)."""
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt_utc.astimezone(timezone.utc)
    year = dt_utc.year
    mar31 = datetime(year, 3, 31, 1, 0, tzinfo=timezone.utc)
    bst_start = mar31 - timedelta(days=(mar31.weekday() + 1) % 7)
    oct31 = datetime(year, 10, 31, 1, 0, tzinfo=timezone.utc)
    bst_end = oct31 - timedelta(days=(oct31.weekday() + 1) % 7)
    offset = timedelta(hours=1) if bst_start <= dt_utc < bst_end else timedelta(0)
    return (dt_utc + offset).replace(tzinfo=None)


def load_districts(metadata_path: Path) -> list[dict[str, Any]]:
    data = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(data, list) or not data:
        raise ValueError(f"Invalid or empty districts metadata: {metadata_path}")
    return data


def _district_tfl_multiplier(
    district: dict[str, Any],
    corridor_statuses: dict[str, Any],
) -> tuple[float, int]:
    """Compute speed multiplier & active disruption count from live TfL corridors."""
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


def _eval_district_status(
    *,
    traffic_speed: float,
    congestion: float,
    disruptions: int,
    temp_c: float,
    wind_ms: float,
    aqi: float,
    pm25: float,
    energy_mw: float,
    energy_base_mw: float,
) -> dict[str, str]:
    tr_st = "normal"
    if traffic_speed < 12.0 or congestion > 82.0:
        tr_st = "critical"
    elif disruptions >= 1 and traffic_speed < 24.0:
        tr_st = "warning"

    w_st = "normal"
    if wind_ms > 18.0:
        w_st = "critical"
    elif wind_ms > 12.5 or temp_c < 0.0 or temp_c > 30.0:
        w_st = "warning"

    aq_st = "normal"
    if aqi > 80.0 or pm25 > 35.0:
        aq_st = "critical"
    elif aqi > 60.0 or pm25 > 25.0:
        aq_st = "warning"

    en_st = "normal"
    if energy_base_mw > 0:
        if energy_mw > energy_base_mw * 1.25:
            en_st = "critical"
        elif energy_mw > energy_base_mw * 1.15:
            en_st = "warning"

    return {
        "traffic": tr_st,
        "weather": w_st,
        "air_quality": aq_st,
        "energy": en_st,
    }


def build_snapshot(
    baselines: dict[str, dict[str, Any]],
    districts: list[dict[str, Any]],
    *,
    now_utc: datetime | None = None,
    collector_name: str = "github-actions-cron",
) -> dict[str, Any]:
    """Build one compact, deterministic city + 32-borough snapshot from polled live APIs."""
    dt_utc = (now_utc or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(microsecond=0)
    lon_naive = utc_to_london_naive(dt_utc)
    diurnal = london_diurnal_traffic_factor(dt_utc)

    t_base = baselines["traffic"]
    w_base = baselines["weather"]
    aq_base = baselines["air_quality"]
    e_base = baselines["energy"]

    corridor_statuses = t_base.get("corridor_statuses") or {}
    base_vol = float((t_base.get("secondary") or {}).get("vehicle_count", 64.0))
    national_mw = float(e_base.get("value", 34500.0))
    london_metro_mw = national_mw * LONDON_ENERGY_SHARE
    e_sec = e_base.get("secondary") or {}

    speeds: list[float] = []
    congestions: list[float] = []
    vehicles: list[float] = []
    temps: list[float] = []
    hums: list[float] = []
    winds: list[float] = []
    precips: list[float] = []
    aqis: list[float] = []
    pm25s: list[float] = []
    pm10s: list[float] = []
    no2s: list[float] = []
    energies: list[float] = []

    districts_payload: dict[str, dict[str, Any]] = {}

    for d in districts:
        did = str(d["district_id"])
        free_flow = float(d.get("free_flow_kmh", 34.0))
        tfl_mult, dist_disrupted = _district_tfl_multiplier(d, corridor_statuses)
        speed = round(max(3.5, min(free_flow * 1.12, free_flow * diurnal * tfl_mult)), 2)
        congestion = round(max(6.0, min(96.0, (1.0 - speed / (free_flow * 1.05)) * 100.0)), 1)
        vol = round(max(12.0, base_vol * (1.15 - (free_flow / 120.0) + dist_disrupted * 0.22)), 1)

        dist_w = (w_base.get("district_values") or {}).get(did)
        if dist_w:
            temp = round(float(dist_w["value"]), 2)
            hum = round(float(dist_w.get("humidity_pct", 76.0)), 1)
            wind = round(float(dist_w.get("wind_speed_ms", 4.0)), 2)
            precip = round(float(dist_w.get("precipitation_mm", 0.0)), 2)
        else:
            temp = round(float(w_base.get("value", 9.5)) + float(d.get("temp_offset_c", 0.0)), 2)
            w_sec = w_base.get("secondary") or {}
            hum = round(float(w_sec.get("humidity_pct", 76.0)), 1)
            wind = round(float(w_sec.get("wind_speed_ms", 4.0)), 2)
            precip = round(float(w_sec.get("precipitation_mm", 0.0)), 2)

        dist_aq = (aq_base.get("district_values") or {}).get(did)
        if dist_aq:
            aqi = round(max(4.0, float(dist_aq["value"])), 1)
            pm25 = round(max(0.5, float(dist_aq.get("pm2_5", 6.0))), 2)
            pm10 = round(max(1.0, float(dist_aq.get("pm10", 11.0))), 2)
            no2 = round(max(1.0, float(dist_aq.get("no2", 16.0))), 2)
        else:
            factor = float(d.get("aqi_factor", 1.0))
            aq_sec = aq_base.get("secondary") or {}
            aqi = round(max(4.0, float(aq_base.get("value", 28.0)) * factor), 1)
            pm25 = round(max(0.5, float(aq_sec.get("pm2_5", 6.0)) * factor), 2)
            pm10 = round(max(1.0, float(aq_sec.get("pm10", 11.0)) * factor), 2)
            no2 = round(max(1.0, float(aq_sec.get("no2", 16.0)) * factor), 2)

        weight = float(d.get("energy_weight", 1.0 / max(len(districts), 32)))
        district_mw = round(max(50.0, london_metro_mw * weight), 2)
        energy_base_mw = 34500.0 * LONDON_ENERGY_SHARE * weight

        statuses = _eval_district_status(
            traffic_speed=speed,
            congestion=congestion,
            disruptions=dist_disrupted,
            temp_c=temp,
            wind_ms=wind,
            aqi=aqi,
            pm25=pm25,
            energy_mw=district_mw,
            energy_base_mw=energy_base_mw,
        )

        speeds.append(speed)
        congestions.append(congestion)
        vehicles.append(vol)
        temps.append(temp)
        hums.append(hum)
        winds.append(wind)
        precips.append(precip)
        aqis.append(aqi)
        pm25s.append(pm25)
        pm10s.append(pm10)
        no2s.append(no2)
        energies.append(district_mw)

        districts_payload[did] = {
            "traffic": {
                "avg": speed,
                "congestion_pct": congestion,
                "tfl_disruptions": dist_disrupted,
                "status": statuses["traffic"],
            },
            "weather": {
                "avg": temp,
                "humidity_pct": hum,
                "wind_speed_ms": wind,
                "precipitation_mm": precip,
                "status": statuses["weather"],
            },
            "air_quality": {
                "avg": aqi,
                "pm2_5": pm25,
                "pm10": pm10,
                "no2": no2,
                "status": statuses["air_quality"],
            },
            "energy": {
                "avg": district_mw,
                "status": statuses["energy"],
            },
        }

    n = max(len(districts), 1)
    domains_payload = {
        "traffic": {
            "metric_name": "avg_speed_kmh",
            "avg_value": round(sum(speeds) / n, 2),
            "min_value": round(min(speeds), 2),
            "max_value": round(max(speeds), 2),
            "unit": "km/h",
            "status": str(t_base.get("status", "fallback")),
            "provider": str(t_base.get("provider", "TfL Unified Road API")),
            "secondary": {
                "congestion_pct": round(sum(congestions) / n, 1),
                "vehicle_count": round(sum(vehicles) / n, 1),
                "disrupted_corridors": int((t_base.get("secondary") or {}).get("disrupted_corridors", 0)),
                "corridors_monitored": int((t_base.get("secondary") or {}).get("corridors_monitored", 24)),
                "diurnal_factor": round(diurnal, 3),
            },
        },
        "weather": {
            "metric_name": "temperature_c",
            "avg_value": round(sum(temps) / n, 2),
            "min_value": round(min(temps), 2),
            "max_value": round(max(temps), 2),
            "unit": "°C",
            "status": str(w_base.get("status", "fallback")),
            "provider": str(w_base.get("provider", "Open-Meteo Forecast")),
            "secondary": {
                "humidity_pct": round(sum(hums) / n, 1),
                "wind_speed_ms": round(sum(winds) / n, 2),
                "precipitation_mm": round(sum(precips) / n, 2),
                "pressure_hpa": round(float((w_base.get("secondary") or {}).get("pressure_hpa", 1014.0)), 1),
            },
        },
        "air_quality": {
            "metric_name": "european_aqi",
            "avg_value": round(sum(aqis) / n, 2),
            "min_value": round(min(aqis), 2),
            "max_value": round(max(aqis), 2),
            "unit": "AQI",
            "status": str(aq_base.get("status", "fallback")),
            "provider": str(aq_base.get("provider", "Open-Meteo Air Quality")),
            "secondary": {
                "pm2_5": round(sum(pm25s) / n, 2),
                "pm10": round(sum(pm10s) / n, 2),
                "no2": round(sum(no2s) / n, 2),
            },
        },
        "energy": {
            "metric_name": "district_load_mw",
            "avg_value": round(sum(energies) / n, 2),
            "min_value": round(min(energies), 2),
            "max_value": round(max(energies), 2),
            "unit": "MW",
            "status": str(e_base.get("status", "fallback")),
            "provider": str(e_base.get("provider", "Elexon BMRS + NESO London")),
            "secondary": {
                "national_load_mw": round(national_mw, 1),
                "london_total_mw": round(london_metro_mw, 1),
                "renewable_share_pct": round(float(e_sec.get("renewable_share_pct", 34.0)), 1),
                "low_carbon_pct": round(float(e_sec.get("low_carbon_pct", 46.0)), 1),
                "carbon_intensity_gco2": round(float(e_sec.get("carbon_intensity_gco2", 185.0)), 1),
            },
        },
    }

    return {
        "schema_version": 1,
        "snapshot_id": dt_utc.strftime("%Y-%m-%dT%H:%MZ"),
        "recorded_at_utc": dt_utc.isoformat(),
        "london_time": lon_naive.isoformat(),
        "collector": collector_name,
        "domains": domains_payload,
        "districts": districts_payload,
    }


def snapshot_file_for_utc_date(output_dir: Path, utc_date: date) -> Path:
    return output_dir / "snapshots" / f"{utc_date.year:04d}" / f"{utc_date.month:02d}" / f"{utc_date.isoformat()}.jsonl"


def append_snapshot(output_dir: Path, snapshot: dict[str, Any]) -> tuple[Path, bool]:
    """Append snapshot to UTC daily JSONL file idempotently (skips duplicate snapshot_id)."""
    rec_utc = datetime.fromisoformat(str(snapshot["recorded_at_utc"]).replace("Z", "+00:00")).astimezone(timezone.utc)
    target = snapshot_file_for_utc_date(output_dir, rec_utc.date())
    target.parent.mkdir(parents=True, exist_ok=True)

    snap_id = str(snapshot.get("snapshot_id") or "")
    if target.exists() and snap_id:
        try:
            with target.open("r", encoding="utf-8") as fh:
                for line in fh:
                    if f'"snapshot_id":"{snap_id}"' in line:
                        logger.info("Snapshot %s already exists in %s; skipping duplicate.", snap_id, target)
                        return target, False
        except OSError as exc:
            logger.warning("Could not read existing archive %s: %s", target, exc)

    line = json.dumps(snapshot, separators=(",", ":"), ensure_ascii=False)
    with target.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    return target, True


def prune_old_snapshots(output_dir: Path, retention_days: int, *, today_utc: date | None = None) -> int:
    """Remove daily JSONL files older than retention_days and clean empty directories."""
    if retention_days <= 0:
        return 0
    snap_root = output_dir / "snapshots"
    if not snap_root.exists():
        return 0

    cutoff = (today_utc or datetime.now(timezone.utc).date()) - timedelta(days=retention_days)
    removed = 0
    for path in list(snap_root.rglob("*.jsonl")):
        try:
            file_date = date.fromisoformat(path.stem)
        except ValueError:
            continue
        if file_date < cutoff:
            try:
                path.unlink()
                removed += 1
            except OSError:
                pass

    # Clean empty month/year directories
    for folder in sorted(snap_root.rglob("*"), key=lambda p: len(p.parts), reverse=True):
        if folder.is_dir():
            try:
                next(folder.iterdir())
            except StopIteration:
                try:
                    folder.rmdir()
                except OSError:
                    pass
    return removed


def collect_once(
    *,
    output_dir: Path,
    metadata_path: Path = DEFAULT_METADATA_PATH,
    retention_days: int = 90,
    collector_name: str = "github-actions-cron",
) -> dict[str, Any]:
    districts = load_districts(metadata_path)
    baselines = poll_all_sources(None, districts)
    snapshot = build_snapshot(baselines, districts, collector_name=collector_name)
    out_file, written = append_snapshot(output_dir, snapshot)
    pruned = prune_old_snapshots(output_dir, retention_days)

    live_domains = [d for d, info in snapshot["domains"].items() if info.get("status") == "live"]
    logger.info(
        "Cloud snapshot %s | written=%s | live=%d/4 (%s) | traffic=%.2f km/h | weather=%.2f°C | aqi=%.1f | energy=%.1f MW | file=%s | pruned=%d",
        snapshot["snapshot_id"],
        written,
        len(live_domains),
        ",".join(live_domains) or "none",
        snapshot["domains"]["traffic"]["avg_value"],
        snapshot["domains"]["weather"]["avg_value"],
        snapshot["domains"]["air_quality"]["avg_value"],
        snapshot["domains"]["energy"]["avg_value"],
        out_file,
        pruned,
    )
    return {
        "snapshot_id": snapshot["snapshot_id"],
        "written": written,
        "live_domains": live_domains,
        "file": str(out_file),
        "pruned_files": pruned,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect one 24/7 Greater London live API snapshot into JSONL archive.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "data" / "cloud_archive",
        help="Root directory where snapshots/YYYY/MM/YYYY-MM-DD.jsonl will be stored.",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=DEFAULT_METADATA_PATH,
        help="Path to london_districts.json metadata file.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=90,
        help="Delete snapshot files older than N days (0 disables pruning).",
    )
    parser.add_argument(
        "--collector-name",
        type=str,
        default="github-actions-cron",
        help="Identifier stored in each snapshot record.",
    )
    args = parser.parse_args()

    result = collect_once(
        output_dir=args.output_dir,
        metadata_path=args.metadata,
        retention_days=args.retention_days,
        collector_name=args.collector_name,
    )
    if not result["live_domains"]:
        logger.warning("No live APIs succeeded in this run; snapshot saved with fallback status.")


if __name__ == "__main__":
    main()
