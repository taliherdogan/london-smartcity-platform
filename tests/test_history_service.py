from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "backend"))

import history_service  # noqa: E402


def _mock_transport(routes: dict[str, Any]) -> httpx.MockTransport:
    def _handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        for prefix, payload in routes.items():
            if url.startswith(prefix):
                return httpx.Response(200, json=payload)
        return httpx.Response(404, json={"error": "not found"})

    return httpx.MockTransport(_handler)


def test_resolve_date_range_presets_and_validation() -> None:
    today = date(2026, 11, 23)
    s, e = history_service.resolve_date_range(days_ago=5, today=today)
    assert s == date(2026, 11, 18)
    assert e == date(2026, 11, 18)

    s2, e2 = history_service.resolve_date_range(
        start_date="2026-11-16", end_date="2026-11-22", today=today
    )
    assert (e2 - s2).days + 1 == 7

    with pytest.raises(ValueError, match="Future dates"):
        history_service.resolve_date_range(single_date="2026-11-25", today=today)

    with pytest.raises(ValueError, match="cannot exceed"):
        history_service.resolve_date_range(
            start_date="2026-09-01", end_date="2026-11-20", today=today
        )

    with pytest.raises(ValueError, match="older than"):
        history_service.resolve_date_range(single_date="2024-01-01", today=today)


def test_read_lake_hourly_prunes_and_filters_by_district(tmp_path: Path) -> None:
    # Create a real Parquet partition matching Spark Structured Streaming layout
    part_traffic = tmp_path / "parquet" / "dt=2026-11-18-09" / "domain=traffic"
    part_weather = tmp_path / "parquet" / "dt=2026-11-18-09" / "domain=weather"
    part_traffic.mkdir(parents=True)
    part_weather.mkdir(parents=True)

    t_table = pa.table(
        {
            "district_id": ["westminster", "westminster", "greenwich"],
            "value": [18.0, 22.0, 35.0],
        }
    )
    w_table = pa.table(
        {
            "district_id": ["westminster", "greenwich"],
            "value": [11.5, 10.5],
        }
    )
    pq.write_table(t_table, part_traffic / "part-00000.parquet")
    pq.write_table(w_table, part_weather / "part-00000.parquet")

    # All districts
    hourly_all, stats_all = history_service.read_lake_hourly(
        tmp_path, date(2026, 11, 18), date(2026, 11, 18), district_id="all"
    )
    assert stats_all["available"] is True
    assert stats_all["partitions_scanned"] == 1
    assert stats_all["records_read"] == 5
    assert hourly_all["2026-11-18T09:00"]["traffic"] == 25.0  # (18 + 22 + 35) / 3
    assert hourly_all["2026-11-18T09:00"]["weather"] == 11.0  # (11.5 + 10.5) / 2

    # Specific borough filter
    hourly_west, stats_west = history_service.read_lake_hourly(
        tmp_path, date(2026, 11, 18), date(2026, 11, 18), district_id="westminster"
    )
    assert stats_west["records_read"] == 3
    assert hourly_west["2026-11-18T09:00"]["traffic"] == 20.0
    assert hourly_west["2026-11-18T09:00"]["weather"] == 11.5


def test_query_historical_data_merges_lake_and_official_apis(tmp_path: Path) -> None:
    target_day = "2026-11-18"
    # Seed 1 hour in Spark Parquet Data Lake to verify hybrid precedence
    part_traffic = tmp_path / "parquet" / f"dt={target_day}-08" / "domain=traffic"
    part_traffic.mkdir(parents=True)
    pq.write_table(
        pa.table({"district_id": ["westminster"], "value": [16.5]}),
        part_traffic / "part-00000.parquet",
    )

    hours = [f"{target_day}T{h:02d}:00" for h in range(24)]
    routes = {
        "https://api.open-meteo.com/v1/forecast": {
            "hourly": {
                "time": hours,
                "temperature_2m": [8.0 + (h % 6) * 0.5 for h in range(24)],
                "relative_humidity_2m": [72.0] * 24,
                "wind_speed_10m": [4.5] * 24,
                "precipitation": [0.0] * 24,
            }
        },
        "https://air-quality-api.open-meteo.com/v1/air-quality": {
            "hourly": {
                "time": hours,
                "european_aqi": [26.0] * 24,
                "pm2_5": [6.5] * 24,
                "pm10": [12.0] * 24,
                "nitrogen_dioxide": [18.0] * 24,
            }
        },
        "https://data.elexon.co.uk/bmrs/api/v1/demand/outturn": {
            "data": [
                {"startTime": f"{target_day}T{h:02d}:00:00Z", "initialDemandOutturn": 34000.0}
                for h in range(24)
            ]
        },
        "https://api.carbonintensity.org.uk/regional/intensity/": {
            "data": {
                "data": [
                    {
                        "from": f"{target_day}T{h:02d}:00Z",
                        "intensity": {"actual": 145},
                        "generationmix": [
                            {"fuel": "wind", "perc": 38.0},
                            {"fuel": "solar", "perc": 6.0},
                            {"fuel": "gas", "perc": 56.0},
                        ],
                    }
                    for h in range(24)
                ]
            }
        },
    }

    latest_state = {
        "domains": {
            "traffic": {"avg_value": 28.0},
            "weather": {"avg_value": 12.0},
            "air_quality": {"avg_value": 30.0},
            "energy": {"avg_value": 150.0},
        },
        "districts": [
            {
                "district_id": "westminster",
                "metrics": {
                    "traffic": {"avg": 21.0},
                    "weather": {"avg": 11.5},
                    "air_quality": {"avg": 29.0},
                    "energy": {"avg": 280.0},
                },
            }
        ],
    }

    async def _run() -> dict[str, Any]:
        async with httpx.AsyncClient(transport=_mock_transport(routes)) as client:
            return await history_service.query_historical_data(
                lake_dir=tmp_path,
                metadata_path=ROOT / "data" / "metadata" / "london_districts.json",
                single_date=target_day,
                district_id="westminster",
                granularity="hourly",
                latest_state=latest_state,
                http_client=client,
            )

    res = asyncio.run(_run())
    assert res["query"]["start_date"] == target_day
    assert res["query"]["district_name"] == "Westminster"
    assert res["query"]["granularity"] == "hourly"
    assert len(res["series"]) == 24

    # 08:00 must come directly from the Spark Parquet Data Lake
    hour_08 = next(r for r in res["series"] if r["timestamp"] == f"{target_day}T08:00")
    assert hour_08["traffic"] == 16.5
    assert hour_08["lake_backed"] is True

    # Summary checks
    assert res["summary"]["traffic"]["source_type"] == "hybrid"
    assert res["summary"]["traffic"]["lake_hours"] == 1
    assert res["summary"]["weather"]["source_type"] == "api"
    assert res["summary"]["air_quality"]["avg"] == 26.0
    assert res["summary"]["energy"]["source_type"] == "api"
    assert res["summary"]["energy"]["secondary"]["avg_renewable_share_pct"] == 44.0
    assert res["summary"]["weather"]["delta_vs_live"] is not None
    assert res["lake_stats"]["lake_hours_matched"] == 1


def test_query_historical_data_daily_aggregation_and_offline_fallback(tmp_path: Path) -> None:
    async def _run() -> dict[str, Any]:
        # All routes 404 -> verifies resilient fallback never crashes
        async with httpx.AsyncClient(transport=_mock_transport({})) as client:
            return await history_service.query_historical_data(
                lake_dir=tmp_path,
                metadata_path=ROOT / "data" / "metadata" / "london_districts.json",
                start_date="2026-11-10",
                end_date="2026-11-19",
                district_id="all",
                granularity="auto",
                http_client=client,
            )

    res = asyncio.run(_run())
    assert res["query"]["days_count"] == 10
    assert res["query"]["granularity"] == "daily"
    assert len(res["series"]) == 10
    assert res["summary"]["weather"]["avg"] is not None
    assert res["summary"]["traffic"]["avg"] is not None
    assert res["summary"]["weather"]["source_type"] == "model"
