from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "generator"))

import api_poller  # noqa: E402


class _FakeResponse:
    def __init__(self, payload: Any, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> Any:
        return self._payload


class _FakeClient:
    def __init__(self, routes: dict[str, _FakeResponse]) -> None:
        self._routes = routes

    def get(self, url: str, **_kwargs: Any) -> _FakeResponse:
        for prefix, resp in self._routes.items():
            if url.startswith(prefix):
                return resp
        raise RuntimeError(f"Unexpected URL: {url}")


SAMPLE_DISTRICTS = [
    {"district_id": "westminster", "lat": 51.4975, "lon": -0.1357},
    {"district_id": "greenwich", "lat": 51.4826, "lon": 0.0077},
]


def test_default_baselines_has_all_domains() -> None:
    baselines = api_poller.default_baselines()
    assert set(baselines.keys()) == {"traffic", "weather", "air_quality", "energy"}
    for domain, entry in baselines.items():
        assert entry["domain"] == domain
        assert entry["status"] == "fallback"
        assert isinstance(entry["value"], float)


def test_diurnal_traffic_factor_weekday_peaks() -> None:
    # Wednesday 08:00 UTC (GMT in January) -> AM Peak = 0.72
    am_peak = datetime(2026, 1, 14, 8, 0, tzinfo=timezone.utc)
    assert api_poller.london_diurnal_traffic_factor(am_peak) == 0.72

    # Wednesday 17:00 UTC -> PM Peak = 0.68
    pm_peak = datetime(2026, 1, 14, 17, 0, tzinfo=timezone.utc)
    assert api_poller.london_diurnal_traffic_factor(pm_peak) == 0.68

    # Wednesday 03:00 UTC -> Night free-flow = 0.98
    night = datetime(2026, 1, 14, 3, 0, tzinfo=timezone.utc)
    assert api_poller.london_diurnal_traffic_factor(night) == 0.98


def test_fetch_weather_multi_district() -> None:
    fake_payload = [
        {
            "current": {
                "temperature_2m": 12.0,
                "relative_humidity_2m": 70.0,
                "wind_speed_10m": 4.5,
                "precipitation": 0.0,
                "surface_pressure": 1015.0,
            }
        },
        {
            "current": {
                "temperature_2m": 14.0,
                "relative_humidity_2m": 74.0,
                "wind_speed_10m": 5.5,
                "precipitation": 0.2,
                "surface_pressure": 1013.0,
            }
        },
    ]
    client = _FakeClient({"https://api.open-meteo.com/v1/forecast": _FakeResponse(fake_payload)})
    result = api_poller.fetch_weather(client, SAMPLE_DISTRICTS)  # type: ignore[arg-type]

    assert result["status"] == "live"
    assert result["value"] == 13.0
    assert result["secondary"]["wind_speed_ms"] == 5.0
    assert set(result["district_values"].keys()) == {"westminster", "greenwich"}


def test_fetch_air_quality_multi_district() -> None:
    fake_payload = [
        {"current": {"european_aqi": 24, "pm2_5": 5.0, "pm10": 10.0, "nitrogen_dioxide": 14.0, "ozone": 40.0}},
        {"current": {"european_aqi": 36, "pm2_5": 9.0, "pm10": 18.0, "nitrogen_dioxide": 22.0, "ozone": 44.0}},
    ]
    client = _FakeClient({"https://air-quality-api.open-meteo.com/v1/air-quality": _FakeResponse(fake_payload)})
    result = api_poller.fetch_air_quality(client, SAMPLE_DISTRICTS)  # type: ignore[arg-type]

    assert result["status"] == "live"
    assert result["value"] == 30.0
    assert result["secondary"]["pm2_5"] == 7.0
    assert result["district_values"]["greenwich"]["value"] == 36.0


def test_fetch_weather_and_aqi_handle_nulls_and_zeroes() -> None:
    weather_payload = [
        {
            "current": {
                "temperature_2m": 0.0,
                "relative_humidity_2m": None,
                "wind_speed_10m": 0.0,
                "precipitation": 0.0,
                "surface_pressure": None,
            }
        }
    ]
    aqi_payload = [
        {
            "current": {
                "european_aqi": None,
                "pm2_5": 0.0,
                "pm10": 0.0,
                "nitrogen_dioxide": None,
                "ozone": 35.0,
            }
        }
    ]
    client = _FakeClient(
        {
            "https://api.open-meteo.com/v1/forecast": _FakeResponse(weather_payload),
            "https://air-quality-api.open-meteo.com/v1/air-quality": _FakeResponse(aqi_payload),
        }
    )
    w = api_poller.fetch_weather(client, [SAMPLE_DISTRICTS[0]])  # type: ignore[arg-type]
    assert w["value"] == 0.0
    assert w["secondary"]["humidity_pct"] == 76.0
    assert w["secondary"]["wind_speed_ms"] == 0.0

    aq = api_poller.fetch_air_quality(client, [SAMPLE_DISTRICTS[0]])  # type: ignore[arg-type]
    assert aq["value"] == 28.0
    assert aq["secondary"]["pm2_5"] == 0.0
    assert aq["secondary"]["pm10"] == 0.0


def test_fetch_energy_survives_neso_failure_when_elexon_succeeds() -> None:
    client = _FakeClient(
        {
            api_poller.ELEXON_DEMAND_URL: _FakeResponse([{"demand": 36000.0}]),
            api_poller.NESO_LONDON_CARBON_URL: _FakeResponse({"error": "down"}, status_code=503),
        }
    )
    res = api_poller.fetch_energy(client)  # type: ignore[arg-type]
    assert res["status"] == "live"
    assert res["value"] == 36000.0
    assert res["secondary"]["london_total_mw"] == pytest.approx(36000.0 * api_poller.LONDON_ENERGY_SHARE, rel=1e-3)


def test_fetch_traffic_parses_tfl_corridors() -> None:
    fake_corridors = [
        {"displayName": "A40", "statusSeverity": "Good", "statusSeverityDescription": "No Exceptional Delays"},
        {"displayName": "A2", "statusSeverity": "Serious", "statusSeverityDescription": "Serious Delays"},
    ]
    client = _FakeClient({api_poller.TFL_ROAD_URL: _FakeResponse(fake_corridors)})
    result = api_poller.fetch_traffic(client)  # type: ignore[arg-type]

    assert result["status"] == "live"
    assert result["secondary"]["corridors_monitored"] == 2
    assert result["secondary"]["disrupted_corridors"] == 1
    assert result["corridor_statuses"]["A2"]["rank"] == 3
    assert result["corridor_statuses"]["A40"]["speed_mult"] == 1.0


def test_fetch_energy_combines_elexon_and_neso() -> None:
    elexon_payload = [{"demand": 30000.0}, {"demand": 35000.0}]
    neso_payload = {
        "data": [
            {
                "data": [
                    {
                        "intensity": {"actual": 142, "index": "moderate"},
                        "generationmix": [
                            {"fuel": "wind", "perc": 32.0},
                            {"fuel": "solar", "perc": 8.0},
                            {"fuel": "nuclear", "perc": 15.0},
                            {"fuel": "gas", "perc": 45.0},
                        ],
                    }
                ]
            }
        ]
    }
    client = _FakeClient(
        {
            api_poller.ELEXON_DEMAND_URL: _FakeResponse(elexon_payload),
            api_poller.NESO_LONDON_CARBON_URL: _FakeResponse(neso_payload),
        }
    )
    result = api_poller.fetch_energy(client)  # type: ignore[arg-type]

    assert result["status"] == "live"
    assert result["value"] == 35000.0
    assert result["secondary"]["london_total_mw"] == pytest.approx(35000.0 * api_poller.LONDON_ENERGY_SHARE, rel=1e-3)
    assert result["secondary"]["renewable_share_pct"] == 40.0
    assert result["secondary"]["low_carbon_pct"] == 55.0
    assert result["secondary"]["carbon_intensity_gco2"] == 142.0


def test_london_districts_metadata_is_singular_and_valid() -> None:
    meta_path = ROOT / "data" / "metadata" / "london_districts.json"
    districts = json.loads(meta_path.read_text(encoding="utf-8"))

    assert len(districts) == 32
    ids = [d["district_id"] for d in districts]
    short_ids = [str(d.get("short_code") or d["district_id"][:3]).upper() for d in districts]
    assert len(set(ids)) == 32
    assert len(set(short_ids)) == 32
    assert "city_of_london" not in ids

    total_weight = sum(float(d["energy_weight"]) for d in districts)
    assert total_weight == pytest.approx(1.0, abs=1e-6)

    for d in districts:
        assert "&" not in d["district_name"]
        assert 51.25 <= float(d["lat"]) <= 51.72
        assert -0.55 <= float(d["lon"]) <= 0.35
        assert len(d["tfl_corridors"]) >= 2
