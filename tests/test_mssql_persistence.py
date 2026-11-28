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
    assert any(
        "dbo.alan_ozetleri" in sql and "kayit_zamani_utc, londra_zamani, batch_no, alan" in sql
        for sql, _ in fake_conn.cursor_obj.many_executed
    )
    assert any(
        "dbo.bolge_olcumleri" in sql and "bolge_id, bolge_adi" in sql
        for sql, _ in fake_conn.cursor_obj.many_executed
    )
    assert any("dbo.anomali_uyarilari" in sql and "uyari_id" in sql for sql, _ in fake_conn.cursor_obj.executed)
    assert any("dbo.platform_durumu" in sql and "durum_anahtari" in sql for sql, _ in fake_conn.cursor_obj.executed)

    restore_conn = _FakeConn(
        script=[
            (
                "state",
                {
                    "toplam_olay_sayisi": 1400,
                    "toplam_uyari_sayisi": 3,
                    "pencere_gecmisi_json": '{"westminster|traffic":[24.0,25.0]}',
                },
            ),
            (
                "alerts",
                [
                    {
                        "uyari_id": "abc123",
                        "kayit_zamani_utc": datetime(2026, 11, 23, 18, 0, 0),
                        "bolge_id": "westminster",
                        "bolge_adi": "Westminster",
                        "alan": "traffic",
                        "metrik_etiketi": "Ortalama Hız",
                        "birim": "km/s",
                        "seviye": "warning",
                        "tespit_yontemi": "Eşik Kuralı",
                        "z_skoru": -2.1,
                        "olculen_deger": 20.1,
                        "referans_deger": 28.0,
                        "esik_etiketi": "Hız < 24",
                        "aciklama": "Yol aksaması",
                        "test_enjekte": 0,
                    }
                ],
            ),
            (
                "timeseries",
                [
                    {
                        "kayit_zamani_utc": datetime(2026, 11, 23, 18, 0, 0),
                        "islenen_hiz_olay_sn": 98.5,
                        "p50_gecikme_ms": 110.0,
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
    assert restored["recent_alerts"][0]["alert_id"] == "abc123"
    assert restored["recent_alerts"][0]["value"] == 20.1
    assert len(restored["timeseries"]) == 1
    assert restored["timeseries"][0]["traffic"] == 24.5
    assert restored["timeseries"][0]["throughput_eps"] == 98.5
    assert restored["timeseries"][0]["latency_ms"] == 110.0


def test_mssql_schema_uses_turkish_columns_and_idempotent_migrations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Every table and index DDL must use Turkish table, index, and column names
    table_and_index_sql = "\n".join(mssql_sink._TABLE_DDL_STATEMENTS + mssql_sink._INDEX_DDL_STATEMENTS)
    for old_table, new_table in mssql_sink._TABLE_RENAMES:
        assert f"dbo.{old_table}" not in table_and_index_sql
        assert f"dbo.{new_table}" in table_and_index_sql
    for _, old_idx, new_idx in mssql_sink._INDEX_RENAMES:
        assert old_idx not in table_and_index_sql
        assert new_idx in table_and_index_sql
    for _, old_col, new_col in mssql_sink._COLUMN_RENAMES:
        assert f"\n        {old_col} " not in table_and_index_sql
        assert new_col in table_and_index_sql

    # Every renamed table has an idempotent OBJECT_ID + sp_rename statement before CREATE TABLE
    table_migrations = mssql_sink._build_table_migration_statements()
    assert len(table_migrations) == 5
    for stmt, (old_table, new_table) in zip(table_migrations, mssql_sink._TABLE_RENAMES):
        assert f"OBJECT_ID(N'dbo.{old_table}', N'U') IS NOT NULL" in stmt
        assert f"OBJECT_ID(N'dbo.{new_table}', N'U') IS NULL" in stmt
        assert f"EXEC sp_rename N'dbo.{old_table}', N'{new_table}'" in stmt

    # Every renamed column has an idempotent COL_LENGTH + sp_rename migration statement
    migrations = mssql_sink._build_column_migration_statements()
    assert len(migrations) == len(mssql_sink._COLUMN_RENAMES)
    assert len(migrations) == 61
    for stmt, (table, old_col, new_col) in zip(migrations, mssql_sink._COLUMN_RENAMES):
        assert f"COL_LENGTH(N'dbo.{table}', N'{old_col}') IS NOT NULL" in stmt
        assert f"COL_LENGTH(N'dbo.{table}', N'{new_col}') IS NULL" in stmt
        assert f"EXEC sp_rename N'dbo.{table}.{old_col}', N'{new_col}', N'COLUMN'" in stmt

    # Every renamed index has an idempotent sp_rename statement
    idx_migrations = mssql_sink._build_index_migration_statements()
    assert len(idx_migrations) == 4
    for stmt, (table, old_idx, new_idx) in zip(idx_migrations, mssql_sink._INDEX_RENAMES):
        assert f"EXEC sp_rename N'dbo.{table}.{old_idx}', N'{new_idx}', N'INDEX'" in stmt

    # Views should expose only Turkish names and Turkish output columns
    views_sql = "\n".join(mssql_sink._VIEW_DDL_STATEMENTS)
    for new_view in ("vw_saatlik_sehir_ozeti", "vw_son_uyarilar", "vw_son_bolge_durumu"):
        assert f"CREATE OR ALTER VIEW dbo.{new_view}" in views_sql
    for legacy_view in mssql_sink._LEGACY_VIEWS:
        assert f"DROP VIEW IF EXISTS dbo.{legacy_view}" in views_sql
    assert "z_skoru" in views_sql and "z_score" not in views_sql
    assert "bolge_id" in views_sql and "district_id" not in views_sql
    assert "bolge_adi" in views_sql and "district_name" not in views_sql
    assert "bolge_tipi" in views_sql and "zone_type" not in views_sql

    # Verify reader queries Turkish columns while keeping backwards-compatible English API aliases
    snap_conn = _FakeConn(
        script=[
            (
                "snaps",
                [
                    {
                        "id": 1,
                        "recorded_at": datetime(2026, 11, 23, 18, 0, 0),
                        "london_time": datetime(2026, 11, 23, 18, 0, 0),
                        "batch_id": 7,
                        "domain": "traffic",
                        "metric_name": "avg_speed_kmh",
                        "avg_value": 24.5,
                        "min_value": 18.0,
                        "max_value": 31.0,
                        "unit": "km/h",
                        "event_count": 50,
                        "data_source": "TfL",
                    }
                ],
            )
        ]
    )
    monkeypatch.setenv("MSSQL_ENABLED", "true")
    monkeypatch.setattr(mssql_reader, "pymssql", object())
    monkeypatch.setattr(mssql_reader, "_connect", lambda timeout=5: snap_conn)

    snaps = mssql_reader.query_saved_snapshots(domain="traffic", district_id="all", limit=5)
    assert len(snaps) == 1 and snaps[0]["avg_value"] == 24.5
    snap_sql = snap_conn.cursor_obj.executed[0][0]
    assert "alan = %s" in snap_sql
    assert "ortalama_deger AS avg_value" in snap_sql
    assert "ORDER BY londra_zamani DESC, id DESC" in snap_sql

    alert_conn = _FakeConn(
        script=[
            (
                "alerts",
                [
                    {
                        "id": 1,
                        "alert_id": "abc123",
                        "recorded_at": datetime(2026, 11, 23, 18, 0, 0),
                        "london_time": datetime(2026, 11, 23, 18, 0, 0),
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
            )
        ]
    )
    monkeypatch.setattr(mssql_reader, "_connect", lambda timeout=5: alert_conn)
    alerts = mssql_reader.query_saved_alerts(domain="traffic", district_id="westminster", severity="warning", limit=5)
    assert len(alerts) == 1 and alerts[0]["alert_id"] == "abc123" and alerts[0]["injected"] is False
    alert_sql = alert_conn.cursor_obj.executed[0][0]
    assert "alan = %s" in alert_sql and "bolge_id = %s" in alert_sql and "seviye = %s" in alert_sql
    assert "olculen_deger AS measured_value" in alert_sql
    assert "ORDER BY kayit_zamani_utc DESC, id DESC" in alert_sql


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
