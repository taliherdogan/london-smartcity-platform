from __future__ import annotations

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "backend"))

import backfill_service  # noqa: E402
import main as backend_main  # noqa: E402


def test_sync_prioritizes_live_and_backfills_missing_api_hours(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MSSQL_ENABLED", "true")
    monkeypatch.setenv("MSSQL_BACKFILL_ALLOW_TRAFFIC_MODEL", "false")
    monkeypatch.setattr(backfill_service, "pymssql", object())
    monkeypatch.setattr(backfill_service, "_check_schema_ready", lambda: True)

    # Simulate MSSQL already having live Spark data at 03:00 and an existing API backfill (-1) at 02:00 for weather
    monkeypatch.setattr(
        backfill_service,
        "_inspect_existing_mssql_coverage",
        lambda _s, _e: {
            "live_hours": {"2026-11-24T03:00": {"traffic", "weather", "air_quality", "energy"}},
            "api_hours": {"2026-11-24T02:00": {"weather"}},
        },
    )

    captured: dict[str, Any] = {}

    def _fake_write(*, domain_rows, district_rows):
        captured["domain_rows"] = domain_rows
        captured["district_rows"] = district_rows

    monkeypatch.setattr(backfill_service, "_write_backfill_rows", _fake_write)

    # Stub historical APIs to return real hourly values for 01:00 and 02:00 (03:00 is covered by live Spark)
    async def _fake_weather(_c, _lat, _lon, _sd, _ed, _td):
        return (
            {
                "2026-11-24T01:00": {"weather": 7.4, "humidity_pct": 80.0, "wind_speed_ms": 3.5, "precipitation_mm": 0.0},
                "2026-11-24T02:00": {"weather": 7.1, "humidity_pct": 82.0, "wind_speed_ms": 3.2, "precipitation_mm": 0.0},
                "2026-11-24T03:00": {"weather": 6.9, "humidity_pct": 84.0, "wind_speed_ms": 3.0, "precipitation_mm": 0.0},
            },
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
    assert res["api_hours_filled"] == 1

    dom_rows = captured["domain_rows"]
    assert all(r[2] == backfill_service.BATCH_ID_API_BACKFILL for r in dom_rows)
    # 03:00 (live) and 02:00 weather (already backfilled) must NOT be inserted again;
    # synthetic traffic must NOT be inserted when allow_traffic_model=False
    assert {r[3] for r in dom_rows} == {"weather", "air_quality", "energy"}
    assert all(r[1] == datetime(2026, 11, 24, 1, 0, 0) for r in dom_rows)


def test_backend_backfill_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _fake_sync(**kwargs: Any) -> dict[str, Any]:
        return {
            "ok": True,
            "trigger": kwargs.get("trigger", "manual"),
            "api_hours_filled": 2,
            "total_rows_inserted": 198,
        }

    monkeypatch.setattr(backend_main, "sync_missing_to_mssql", _fake_sync)
    monkeypatch.setattr(
        backend_main,
        "get_backfill_status",
        lambda: {"running": False, "auto_enabled": True, "auto_hours": 72},
    )

    async def _run():
        transport = httpx.ASGITransport(app=backend_main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            r_status = await client.get("/api/storage/backfill/status")
            r_post = await client.post(
                "/api/storage/backfill",
                json={"start_date": "2026-11-23", "end_date": "2026-11-24", "source_mode": "auto"},
            )
            r_bad = await client.post(
                "/api/storage/backfill",
                json={"start_date": "2026-11-23", "end_date": "2026-11-24", "source_mode": "cloud_only"},
            )
            return r_status, r_post, r_bad

    r_status, r_post, r_bad = asyncio.run(_run())
    assert r_status.status_code == 200
    assert r_status.json()["auto_enabled"] is True
    assert r_post.status_code == 200
    assert r_post.json()["api_hours_filled"] == 2
    assert r_post.json()["total_rows_inserted"] == 198
    assert r_bad.status_code == 422
