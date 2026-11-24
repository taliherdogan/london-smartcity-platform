from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "backend"))
sys.path.insert(0, str(ROOT / "services" / "cloud_collector"))
sys.path.insert(0, str(ROOT / "services" / "generator"))

import api_poller  # noqa: E402
import backfill_service  # noqa: E402
import collect_snapshot  # noqa: E402
import main as backend_main  # noqa: E402


def _make_live_snapshot(dt_utc: datetime) -> dict[str, Any]:
    districts = json.loads((ROOT / "data" / "metadata" / "london_districts.json").read_text(encoding="utf-8"))[:2]
    base = api_poller.default_baselines()
    for dom in base:
        base[dom]["status"] = "live"
    base["traffic"]["corridor_statuses"] = {
        "Inner Ring": {"severity": "Good", "description": "No delays", "rank": 0, "speed_mult": 1.0}
    }
    return collect_snapshot.build_snapshot(base, districts, now_utc=dt_utc, collector_name="github-actions-cron")


def test_sync_prioritizes_live_and_upgrades_api_with_cloud(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Create 2 overnight cloud snapshots: 02:15 (will be synced & upgrade an old -1 API row) and 03:15 (live exists -> skipped)
    snap_0215 = _make_live_snapshot(datetime(2026, 11, 24, 2, 15, 0, tzinfo=timezone.utc))
    snap_0315 = _make_live_snapshot(datetime(2026, 11, 24, 3, 15, 0, tzinfo=timezone.utc))
    collect_snapshot.append_snapshot(tmp_path, snap_0215)
    collect_snapshot.append_snapshot(tmp_path, snap_0315)

    monkeypatch.setenv("MSSQL_ENABLED", "true")
    monkeypatch.setenv("MSSQL_BACKFILL_ALLOW_TRAFFIC_MODEL", "false")
    monkeypatch.setenv("CLOUD_ARCHIVE_REPO", "")
    monkeypatch.setenv("CLOUD_ARCHIVE_LOCAL_DIR", str(tmp_path))
    monkeypatch.setattr(backfill_service, "pymssql", object())
    monkeypatch.setattr(backfill_service, "_check_schema_ready", lambda: True)

    # Simulate MSSQL already having live Spark data at 03:00 and an old API backfill (-1) at 02:00 for weather
    monkeypatch.setattr(
        backfill_service,
        "_inspect_existing_mssql_coverage",
        lambda _s, _e: {
            "live_hours": {"2026-11-24T03:00": {"traffic", "weather", "air_quality", "energy"}},
            "cloud_hours": {},
            "api_hours": {"2026-11-24T02:00": {"weather"}},
            "cloud_exact_ts": set(),
        },
    )

    captured: dict[str, Any] = {}

    def _fake_write(*, domain_rows, district_rows, upgrade_api_hours):
        captured["domain_rows"] = domain_rows
        captured["district_rows"] = district_rows
        captured["upgrade_api_hours"] = upgrade_api_hours

    monkeypatch.setattr(backfill_service, "_write_backfill_rows", _fake_write)

    # Stub historical APIs to return real hourly values for 01:00 (missing from cloud & live)
    async def _fake_weather(_c, _lat, _lon, _sd, _ed, _td):
        return (
            {"2026-11-24T01:00": {"weather": 7.4, "humidity_pct": 80.0, "wind_speed_ms": 3.5, "precipitation_mm": 0.0}},
            True,
        )

    async def _fake_aqi(_c, _lat, _lon, _sd, _ed):
        return (
            {"2026-11-24T01:00": {"air_quality": 22.0, "pm2_5": 5.0, "pm10": 9.0, "no2": 14.0}},
            True,
        )

    async def _fake_elexon(_c, _sd, _ed):
        return ({"2026-11-24T01:00": 26000.0}, True)

    async def _fake_neso(_c, _sd, _ed):
        return ({"2026-11-24T01:00": {"carbon_intensity_gco2": 140.0, "renewable_share_pct": 45.0}}, True)

    monkeypatch.setattr(backfill_service, "_fetch_open_meteo_weather", _fake_weather)
    monkeypatch.setattr(backfill_service, "_fetch_open_meteo_air_quality", _fake_aqi)
    monkeypatch.setattr(backfill_service, "_fetch_elexon_demand", _fake_elexon)
    monkeypatch.setattr(backfill_service, "_fetch_neso_london_carbon", _fake_neso)

    res = asyncio.run(
        backfill_service.sync_missing_to_mssql(
            metadata_path=ROOT / "data" / "metadata" / "london_districts.json",
            start_date="2026-11-24",
            end_date="2026-11-24",
            source_mode="auto",
            include_districts=True,
            now_london_override=datetime(2026, 11, 24, 4, 10, 0, tzinfo=timezone.utc),
        )
    )

    assert res["ok"] is True
    assert res["allow_traffic_model"] is False
    # Only 02:15 cloud snapshot should be synced; 03:15 must be skipped because live Spark covers 03:00
    assert res["cloud_snapshots_synced"] == 1
    # 02:00 weather API row (-1) must be scheduled for upgrade to real cloud (-2)
    assert res["upgraded_hour_domains"] == 1
    assert captured["upgrade_api_hours"] == [
        (datetime(2026, 11, 24, 2, 0, 0), datetime(2026, 11, 24, 3, 0, 0), "weather")
    ]

    dom_rows = captured["domain_rows"]
    cloud_doms = [r for r in dom_rows if r[2] == backfill_service.BATCH_ID_CLOUD]
    api_doms = [r for r in dom_rows if r[2] == backfill_service.BATCH_ID_API_BACKFILL]

    # Cloud snapshot inserts all 4 real domains including real TfL traffic!
    assert {r[3] for r in cloud_doms} == {"traffic", "weather", "air_quality", "energy"}
    assert all("Cloud Collector Live" in str(r[10]) for r in cloud_doms)

    # API backfill (01:00) MUST NOT insert synthetic traffic because allow_traffic_model=False
    assert {r[3] for r in api_doms} == {"weather", "air_quality", "energy"}
    assert all(r[1] == datetime(2026, 11, 24, 1, 0, 0) for r in api_doms)


def test_fetch_cloud_snapshots_from_github_raw_mock() -> None:
    snap = _make_live_snapshot(datetime(2026, 11, 24, 1, 45, 0, tzinfo=timezone.utc))
    jsonl_body = json.dumps(snap, separators=(",", ":")) + "\n"

    def _handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/2026-11-24.jsonl"):
            return httpx.Response(200, text=jsonl_body)
        return httpx.Response(404, text="Not Found")

    async def _run():
        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport) as client:
            return await backfill_service.fetch_cloud_snapshots(
                datetime(2026, 11, 24).date(),
                datetime(2026, 11, 24).date(),
                window_start_lon=datetime(2026, 11, 24, 0, 0, 0),
                window_end_lon=datetime(2026, 11, 24, 23, 59, 59),
                http_client=client,
                settings={
                    "cloud_repo": "demo-user/london-smartcity",
                    "cloud_branch": "data-archive",
                    "cloud_token": "",
                    "cloud_local_dir": "",
                },
            )

    snaps, stats = asyncio.run(_run())
    assert len(snaps) == 1
    assert snaps[0]["snapshot_id"] == "2026-11-24T01:45Z"
    assert stats["remote_files_read"] == 1


def test_backend_backfill_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_sync(**kwargs: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "trigger": kwargs.get("trigger", "manual"),
            "cloud_snapshots_synced": 6,
            "api_hours_filled": 2,
            "total_rows_inserted": 336,
        }

    monkeypatch.setattr(backend_main, "sync_missing_to_mssql", _fake_sync)
    monkeypatch.setattr(
        backend_main,
        "get_backfill_status",
        lambda: {"running": False, "cloud_configured": True, "cloud_repo": "demo/repo"},
    )

    async def _run():
        transport = httpx.ASGITransport(app=backend_main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            r_status = await client.get("/api/storage/backfill/status")
            r_post = await client.post(
                "/api/storage/backfill",
                json={"start_date": "2026-11-23", "end_date": "2026-11-24", "source_mode": "auto"},
            )
            return r_status, r_post

    r_status, r_post = asyncio.run(_run())
    assert r_status.status_code == 200
    assert r_status.json()["cloud_configured"] is True
    assert r_post.status_code == 200
    assert r_post.json()["cloud_snapshots_synced"] == 6
    assert r_post.json()["total_rows_inserted"] == 336
