from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "cloud_collector"))
sys.path.insert(0, str(ROOT / "services" / "generator"))

import api_poller  # noqa: E402
import collect_snapshot  # noqa: E402


def _sample_districts() -> list[dict]:
    return [
        {
            "district_id": "westminster",
            "district_name": "Westminster",
            "zone_type": "commercial_hub",
            "lat": 51.4975,
            "lon": -0.1357,
            "tfl_corridors": ["Inner Ring", "A40"],
            "free_flow_kmh": 26.0,
            "aqi_factor": 1.05,
            "energy_weight": 0.06,
            "temp_offset_c": 0.4,
        },
        {
            "district_id": "camden",
            "district_name": "Camden",
            "zone_type": "dense_residential",
            "lat": 51.5390,
            "lon": -0.1426,
            "tfl_corridors": ["A41"],
            "free_flow_kmh": 30.0,
            "aqi_factor": 1.02,
            "energy_weight": 0.05,
            "temp_offset_c": 0.2,
        },
    ]


def _sample_baselines() -> dict:
    base = api_poller.default_baselines()
    base["traffic"]["status"] = "live"
    base["traffic"]["corridor_statuses"] = {
        "Inner Ring": {"severity": "Serious", "description": "Works", "rank": 3, "speed_mult": 0.56},
        "A40": {"severity": "Good", "description": "No delays", "rank": 0, "speed_mult": 1.0},
        "A41": {"severity": "Good", "description": "No delays", "rank": 0, "speed_mult": 1.0},
    }
    base["traffic"]["secondary"]["disrupted_corridors"] = 1
    base["weather"]["status"] = "live"
    base["weather"]["district_values"] = {
        "westminster": {"value": 8.2, "humidity_pct": 81.0, "wind_speed_ms": 3.4, "precipitation_mm": 0.0},
        "camden": {"value": 7.9, "humidity_pct": 82.0, "wind_speed_ms": 3.1, "precipitation_mm": 0.0},
    }
    base["air_quality"]["status"] = "live"
    base["air_quality"]["district_values"] = {
        "westminster": {"value": 24.0, "pm2_5": 5.5, "pm10": 10.1, "no2": 18.0},
        "camden": {"value": 20.0, "pm2_5": 4.8, "pm10": 8.9, "no2": 14.5},
    }
    base["energy"]["status"] = "live"
    base["energy"]["value"] = 28000.0
    return base


def test_build_snapshot_computes_real_tfl_disruptions_and_all_domains() -> None:
    dt_utc = datetime(2026, 11, 24, 2, 15, 0, tzinfo=timezone.utc)
    snap = collect_snapshot.build_snapshot(
        _sample_baselines(),
        _sample_districts(),
        now_utc=dt_utc,
        collector_name="test-runner",
    )

    assert snap["snapshot_id"] == "2026-11-24T02:15Z"
    assert snap["london_time"] == "2026-11-24T02:15:00"
    assert set(snap["domains"].keys()) == {"traffic", "weather", "air_quality", "energy"}
    assert all(snap["domains"][d]["status"] == "live" for d in ("traffic", "weather", "air_quality", "energy"))

    # Westminster has 1 serious disruption on Inner Ring -> slower than free flow & warning status
    wes_tr = snap["districts"]["westminster"]["traffic"]
    cam_tr = snap["districts"]["camden"]["traffic"]
    assert wes_tr["tfl_disruptions"] == 1
    assert wes_tr["status"] == "warning"
    assert cam_tr["tfl_disruptions"] == 0
    assert cam_tr["status"] == "normal"
    assert wes_tr["avg"] < cam_tr["avg"]

    # Energy matches streaming_app per-district MW convention
    expected_wes_mw = round(28000.0 * api_poller.LONDON_ENERGY_SHARE * 0.06, 2)
    assert snap["districts"]["westminster"]["energy"]["avg"] == expected_wes_mw


def test_append_snapshot_is_idempotent_and_prunes_old_files(tmp_path: Path) -> None:
    dt_utc = datetime(2026, 11, 24, 3, 30, 0, tzinfo=timezone.utc)
    snap = collect_snapshot.build_snapshot(_sample_baselines(), _sample_districts(), now_utc=dt_utc)

    path1, written1 = collect_snapshot.append_snapshot(tmp_path, snap)
    path2, written2 = collect_snapshot.append_snapshot(tmp_path, snap)

    assert written1 is True
    assert written2 is False
    assert path1 == path2
    lines = path1.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["snapshot_id"] == "2026-11-24T03:30Z"

    # Create an old snapshot file beyond retention and verify pruning
    old_file = collect_snapshot.snapshot_file_for_utc_date(tmp_path, date(2026, 1, 1))
    old_file.parent.mkdir(parents=True, exist_ok=True)
    old_file.write_text('{"snapshot_id":"2026-01-01T00:00Z"}\n', encoding="utf-8")

    removed = collect_snapshot.prune_old_snapshots(tmp_path, retention_days=90, today_utc=date(2026, 11, 24))
    assert removed == 1
    assert not old_file.exists()
    assert path1.exists()
