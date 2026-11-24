from __future__ import annotations

import asyncio
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "spark_processor"))
sys.path.insert(0, str(ROOT / "services" / "backend"))

import main as backend_main  # noqa: E402
import mssql_reader  # noqa: E402
import mssql_sink  # noqa: E402


class _FakeCursor:
    def __init__(self, script: list[tuple[str, Any]] | None = None) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.many_executed: list[tuple[str, list[Any]]] = []
        self._script = list(script or [])
        self._last_result: Any = None

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))
        if self._script:
            self._last_result = self._script.pop(0)[1]
        else:
            self._last_result = None

    def executemany(self, sql: str, seq: list[Any]) -> None:
        self.many_executed.append((sql, list(seq)))

    def fetchone(self) -> Any:
        return self._last_result

    def fetchall(self) -> list[Any]:
        return list(self._last_result or [])


class _FakeConn:
    def __init__(self, script: list[tuple[str, Any]] | None = None) -> None:
        self.cursor_obj = _FakeCursor(script)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self, as_dict: bool = False) -> _FakeCursor:
        return self.cursor_obj

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def test_mssql_sink_rejects_unsafe_db_name() -> None:
    with pytest.raises(ValueError, match="Unsafe MSSQL database name"):
        mssql_sink.MSSQLSink(enabled=True, database="SmartCity; DROP TABLE x")


def test_mssql_sink_save_batch_and_restore_state(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = _FakeConn()
    sink = mssql_sink.MSSQLSink(enabled=True, database="SmartCityDB")
    monkeypatch.setattr(mssql_sink, "pymssql", object())
    sink._schema_ready = True
    sink._conn = fake_conn

    ok = sink.save_batch(
        batch_id=7,
        ts_iso="2026-11-23T18:00:00+00:00",
        domains_summary={
            "traffic": {
                "metric_name": "avg_speed_kmh",
                "avg_value": 24.5,
                "min_value": 18.0,
                "max_value": 31.0,
                "unit": "km/h",
                "event_count": 50,
                "data_source": "TfL",
                "secondary": {"congestion_pct": 42.0},
            }
        },
        districts_map={
            "westminster": {
                "district_name": "Westminster",
                "zone_type": "central",
                "status": "warning",
                "tfl_disruptions": 1,
                "metrics": {"traffic": {"avg": 20.1, "min": 16.0, "max": 24.0, "count": 10}},
            }
        },
        new_alerts=[
            {
                "alert_id": "abc123",
                "district_id": "westminster",
                "district_name": "Westminster",
                "domain": "traffic",
                "metric_label": "Ortalama Hız",
                "unit": "km/s",
                "severity": "warning",
                "method": "Eşik Kuralı",
                "z_score": -2.1,
                "value": 20.1,
                "baseline_value": 28.0,
                "threshold_label": "Hız < 24",
                "message": "Yol aksaması",
                "injected": False,
            }
        ],
        perf_snapshot={
            "target_rate_eps": 100,
            "throughput_eps": 98.5,
            "batch_events": 200,
            "avg_latency_ms": 120.0,
            "p50_latency_ms": 110.0,
            "p95_latency_ms": 210.0,
            "spark_batch_ms": 340.0,
            "cumulative_events": 1400,
            "cumulative_alerts": 3,
            "backpressure_status": "healthy",
            "driver_rss_mb": 650.0,
            "lake_size_mb": 2.4,
        },
        window_history={("westminster", "traffic"): [24.0, 25.0]},
    )
    assert ok is True
    assert fake_conn.committed is True
    assert len(fake_conn.cursor_obj.many_executed) == 2
    assert any("dbo.anomaly_alerts" in sql for sql, _ in fake_conn.cursor_obj.executed)
    assert any("dbo.platform_state" in sql for sql, _ in fake_conn.cursor_obj.executed)

    restore_conn = _FakeConn(
        script=[
            (
                "state",
                {
                    "cumulative_events": 1400,
                    "cumulative_alerts": 3,
                    "window_history_json": '{"westminster|traffic":[24.0,25.0]}',
                },
            ),
            (
                "alerts",
                [
                    {
                        "alert_id": "abc123",
                        "recorded_at": datetime(2026, 11, 23, 18, 0, 0),
                        "district_id": "westminster",
                        "district_name": "Westminster",
                        "domain": "traffic",
                        "metric_label": "Ortalama Hız",
                        "unit": "km/s",
                        "severity": "warning",
                        "method": "Eşik Kuralı",
                        "z_score": -2.1,
                        "measured_value": 20.1,
                        "baseline_value": 28.0,
                        "threshold_label": "Hız < 24",
                        "message": "Yol aksaması",
                        "injected": 0,
                    }
                ],
            ),
            (
                "timeseries",
                [
                    {
                        "recorded_at": datetime(2026, 11, 23, 18, 0, 0),
                        "throughput_eps": 98.5,
                        "p50_latency_ms": 110.0,
                        "traffic": 24.5,
                        "weather": 10.2,
                        "air_quality": 22.0,
                        "energy": 145.0,
                    }
                ],
            ),
        ]
    )
    sink._conn = restore_conn
    restored = sink.restore_state()
    assert restored is not None
    assert restored["cumulative_events"] == 1400
    assert restored["cumulative_alerts"] == 3
    assert len(restored["recent_alerts"]) == 1
    assert len(restored["timeseries"]) == 1
    assert restored["timeseries"][0]["traffic"] == 24.5


def test_mssql_reader_hourly_and_storage_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_conn = _FakeConn(
        script=[
            (
                "hourly",
                [
                    {
                        "hour_bucket": datetime(2026, 11, 23, 14, 0, 0),
                        "domain": "traffic",
                        "mean_val": 26.44,
                        "rec_count": 320,
                    },
                    {
                        "hour_bucket": datetime(2026, 11, 23, 14, 0, 0),
                        "domain": "weather",
                        "mean_val": 9.81,
                        "rec_count": 310,
                    },
                ],
            )
        ]
    )
    monkeypatch.setenv("MSSQL_ENABLED", "true")
    monkeypatch.setattr(mssql_reader, "pymssql", object())
    monkeypatch.setattr(mssql_reader, "_connect", lambda timeout=5: fake_conn)

    hourly, stats = mssql_reader.read_mssql_hourly(date(2026, 11, 23), date(2026, 11, 23), "all")
    assert stats["available"] is True
    assert stats["records_read"] == 630
    assert hourly["2026-11-23T14:00"]["traffic"] == 26.44
    assert hourly["2026-11-23T14:00"]["weather"] == 9.81

    # Verify backend /api/storage/* endpoints
    monkeypatch.setattr(
        backend_main,
        "get_storage_summary",
        lambda: {"enabled": True, "connected": True, "database": "SmartCityDB", "total_rows": 420},
    )
    monkeypatch.setattr(
        backend_main,
        "query_saved_snapshots",
        lambda **_kwargs: [{"id": 1, "domain": "traffic", "avg_value": 25.0}],
    )
    monkeypatch.setattr(
        backend_main,
        "query_saved_alerts",
        lambda **_kwargs: [{"alert_id": "abc123", "domain": "traffic", "severity": "warning"}],
    )

    async def _run_requests() -> tuple[httpx.Response, httpx.Response, httpx.Response]:
        transport = httpx.ASGITransport(app=backend_main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            r1 = await client.get("/api/storage/summary")
            r2 = await client.get("/api/storage/snapshots?domain=traffic&limit=10")
            r3 = await client.get("/api/storage/alerts?severity=warning&limit=10")
            return r1, r2, r3

    r1, r2, r3 = asyncio.run(_run_requests())
    assert r1.status_code == 200 and r1.json()["connected"] is True
    assert r2.status_code == 200 and r2.json()["count"] == 1
    assert r3.status_code == 200 and r3.json()["count"] == 1
