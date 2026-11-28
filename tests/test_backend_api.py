from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "backend"))

import main as backend_main  # noqa: E402


class _FakeAsyncRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {
            backend_main.REDIS_RATE_KEY: "500",
            backend_main.REDIS_INFRA_SPARK_KEY: json.dumps(
                {"status": "healthy", "driver_rss_mb": 640.5, "lake_size_mb": 12.4, "batch_id": 42}
            ),
            backend_main.REDIS_INFRA_GEN_KEY: json.dumps(
                {
                    "status": "healthy",
                    "target_rate_eps": 500,
                    "actual_rate_eps": 498.2,
                    "queue_len": 0,
                    "dropped_events": 0,
                }
            ),
        }

    async def ping(self) -> bool:
        return True

    async def info(self, _section: str = "memory") -> dict[str, Any]:
        return {"used_memory": 24 * 1024 * 1024, "maxmemory": 256 * 1024 * 1024}

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.store[key] = value
        return True

    async def scan_iter(self, match: str = "*"):
        prefix = match.rstrip("*")
        for k in list(self.store.keys()):
            if k.startswith(prefix):
                yield k

    async def lrange(self, _key: str, _start: int, _end: int) -> list[str]:
        return []


def _request(method: str, path: str, **kwargs: Any) -> httpx.Response:
    async def _run() -> httpx.Response:
        transport = httpx.ASGITransport(app=backend_main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.request(method, path, **kwargs)

    return asyncio.run(_run())


@pytest.fixture()
def fake_redis(monkeypatch: pytest.MonkeyPatch) -> _FakeAsyncRedis:
    mock_redis = _FakeAsyncRedis()
    monkeypatch.setattr(backend_main, "redis_client", mock_redis)
    return mock_redis


def test_health_endpoint(fake_redis: _FakeAsyncRedis) -> None:
    resp = _request("GET", "/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["redis"] is True


def test_system_telemetry_endpoint(fake_redis: _FakeAsyncRedis) -> None:
    resp = _request("GET", "/api/system")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "healthy"
    assert body["redis"]["used_memory_mb"] == 24.0
    assert body["redis"]["max_memory_mb"] == 256.0
    assert body["spark"]["driver_rss_mb"] == 640.5
    assert body["generator"]["actual_rate_eps"] == 498.2


def test_control_rate_validation_and_update(fake_redis: _FakeAsyncRedis) -> None:
    bad = _request("POST", "/api/control/rate", json={"rate": 5})
    assert bad.status_code == 422

    ok = _request("POST", "/api/control/rate", json={"rate": 2000})
    assert ok.status_code == 200
    assert ok.json() == {"ok": True, "target_rate": 2000}
    assert fake_redis.store[backend_main.REDIS_RATE_KEY] == "2000"


def test_control_anomaly_validation(fake_redis: _FakeAsyncRedis) -> None:
    bad = _request("POST", "/api/control/anomaly", json={"domain": "invalid_domain", "duration_sec": 15})
    assert bad.status_code == 422

    bad_dist = _request(
        "POST",
        "/api/control/anomaly",
        json={"domain": "traffic", "district_id": "nonexistent_borough", "duration_sec": 10},
    )
    assert bad_dist.status_code == 400

    ok = _request(
        "POST",
        "/api/control/anomaly",
        json={"domain": "traffic", "district_id": "westminster", "duration_sec": 10},
    )
    assert ok.status_code == 200
    assert backend_main.REDIS_ANOMALY_KEY in fake_redis.store


def test_storage_snapshots_rejects_inverted_date_range(fake_redis: _FakeAsyncRedis) -> None:
    resp = _request("GET", "/api/storage/snapshots?start_date=2026-11-28&end_date=2026-11-20")
    assert resp.status_code == 400


def test_districts_endpoint(fake_redis: _FakeAsyncRedis) -> None:
    resp = _request("GET", "/api/districts")
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 32
    ids = {d["district_id"] for d in body["districts"]}
    assert "westminster" in ids
    assert "greenwich" in ids


def test_history_endpoint_validation_and_caching(
    fake_redis: _FakeAsyncRedis, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = 0

    async def _fake_query(**kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {
            "query": {
                "start_date": kwargs["start_date"],
                "end_date": kwargs["end_date"],
                "display_range": kwargs["start_date"],
                "days_count": 1,
                "district_id": kwargs["district_id"],
                "district_name": "Tüm Londra (32 Bölge Ort.)",
                "granularity": "hourly",
                "points_count": 2,
            },
            "summary": {
                "traffic": {"avg": 29.4, "unit": "km/s"},
                "weather": {"avg": 10.8, "unit": "°C"},
            },
            "series": [],
            "lake_stats": {"available": False, "lake_hours_matched": 0, "total_hours": 24},
        }

    monkeypatch.setattr(backend_main, "query_historical_data", _fake_query)

    # First call hits service and caches in Redis
    r1 = _request("GET", "/api/history?days_ago=5&district_id=all")
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["cached"] is False
    assert body1["summary"]["weather"]["avg"] == 10.8
    assert calls == 1

    # Second identical call serves from Redis cache
    r2 = _request("GET", "/api/history?days_ago=5&district_id=all")
    assert r2.status_code == 200
    assert r2.json()["cached"] is True
    assert calls == 1

    # Future date returns 400
    bad = _request("GET", "/api/history?date=2099-01-01")
    assert bad.status_code == 400
