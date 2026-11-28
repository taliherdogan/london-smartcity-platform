from __future__ import annotations

import sys
import types
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "services" / "spark_processor"))


def _install_pyspark_stubs_if_missing() -> None:
    """Allow unit testing pure Python anomaly & threshold functions without a local JVM/PySpark install."""
    try:
        import pyspark  # noqa: F401
        return
    except ImportError:
        pass

    pyspark_mod = types.ModuleType("pyspark")
    setattr(pyspark_mod, "StorageLevel", types.SimpleNamespace(MEMORY_AND_DISK="MEMORY_AND_DISK"))

    sql_mod = types.ModuleType("pyspark.sql")
    setattr(sql_mod, "DataFrame", Any)
    setattr(sql_mod, "SparkSession", Any)
    setattr(sql_mod, "functions", types.ModuleType("pyspark.sql.functions"))

    types_mod = types.ModuleType("pyspark.sql.types")
    for cls_name in (
        "BooleanType",
        "DoubleType",
        "LongType",
        "MapType",
        "StringType",
        "StructField",
        "StructType",
    ):
        setattr(types_mod, cls_name, lambda *args, **kwargs: None)

    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = sql_mod
    sys.modules["pyspark.sql.functions"] = getattr(sql_mod, "functions")
    sys.modules["pyspark.sql.types"] = types_mod


_install_pyspark_stubs_if_missing()
import streaming_app  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_streaming_state() -> None:
    streaming_app.WINDOW_HISTORY.clear()
    streaming_app.ZSCORE_OUTLIER_STREAK.clear()
    streaming_app.DISTRICT_ENERGY_BASE.clear()
    streaming_app.DISTRICT_ENERGY_BASE["westminster"] = 1000.0
    for dom in streaming_app.VALID_DOMAINS:
        streaming_app.LATEST_DOMAIN_DISTRICT_METRICS[dom] = {}


def test_threshold_traffic_critical_and_warning() -> None:
    hit, sev, _, _, _, direction = streaming_app.evaluate_threshold(
        "traffic", 9.5, {"congestion_pct": 85.0, "district_disruptions": 1}, "westminster", {"free_flow_kmh": 34.0}
    )
    assert hit is True
    assert sev == "critical"
    assert direction == "low"

    hit_w, sev_w, _, _, _, _ = streaming_app.evaluate_threshold(
        "traffic", 20.0, {"congestion_pct": 45.0, "district_disruptions": 1}, "westminster", {"free_flow_kmh": 34.0}
    )
    assert hit_w is True
    assert sev_w == "warning"

    hit_ok, _, _, _, _, _ = streaming_app.evaluate_threshold(
        "traffic", 30.0, {"congestion_pct": 20.0, "district_disruptions": 0}, "westminster", {"free_flow_kmh": 34.0}
    )
    assert hit_ok is False


def test_threshold_air_quality() -> None:
    hit_c, sev_c, _, _, _, _ = streaming_app.evaluate_threshold("air_quality", 95.0, {"pm2_5": 42.0}, "westminster")
    assert hit_c is True and sev_c == "critical"

    hit_w, sev_w, _, _, _, _ = streaming_app.evaluate_threshold("air_quality", 65.0, {"pm2_5": 18.0}, "westminster")
    assert hit_w is True and sev_w == "warning"

    hit_ok, _, _, _, _, _ = streaming_app.evaluate_threshold("air_quality", 25.0, {"pm2_5": 6.0}, "westminster")
    assert hit_ok is False


def test_threshold_weather_wind_and_temp() -> None:
    hit_storm, sev_storm, _, _, _, _ = streaming_app.evaluate_threshold("weather", 10.0, {"wind_speed_ms": 21.0}, "westminster")
    assert hit_storm is True and sev_storm == "critical"

    hit_freeze, sev_freeze, _, _, _, direction = streaming_app.evaluate_threshold("weather", -2.5, {"wind_speed_ms": 3.0}, "westminster")
    assert hit_freeze is True and sev_freeze == "warning" and direction == "low"


def test_threshold_energy_relative_to_district_base() -> None:
    hit_c, sev_c, _, _, base, _ = streaming_app.evaluate_threshold("energy", 1300.0, {}, "westminster")
    assert hit_c is True and sev_c == "critical" and base == 1000.0

    hit_w, sev_w, _, _, _, _ = streaming_app.evaluate_threshold("energy", 1180.0, {}, "westminster")
    assert hit_w is True and sev_w == "warning"

    hit_ok, _, _, _, _, _ = streaming_app.evaluate_threshold("energy", 1020.0, {}, "westminster")
    assert hit_ok is False


def test_zscore_warmup_and_spike_detection() -> None:
    key = ("westminster", "traffic")
    # First 10 samples warm up the sliding window without firing
    for val in [28.0, 28.2, 27.9, 28.1, 28.0, 27.8, 28.3, 28.0, 27.9, 28.1]:
        hit, _, _ = streaming_app.evaluate_zscore(key, val)
        assert hit is False

    # Normal value should not trigger
    hit_normal, z_normal, mean_val = streaming_app.evaluate_zscore(key, 28.2)
    assert hit_normal is False
    assert abs(z_normal) < 1.0
    assert mean_val is not None and 27.5 <= mean_val <= 28.5

    # Sharp drop should trigger |z| > 3.2 without polluting window history
    hit_spike, z_spike, _ = streaming_app.evaluate_zscore(key, 8.0)
    assert hit_spike is True
    assert z_spike < -3.2
    assert 8.0 not in streaming_app.WINDOW_HISTORY[key]


def test_zscore_adapts_on_sustained_regime_shift_but_not_injected() -> None:
    key = ("hammersmith", "air_quality")
    for _ in range(12):
        streaming_app.evaluate_zscore(key, 40.0)

    # Injected anomalies must never shift the baseline
    for _ in range(6):
        hit_inj, _, _ = streaming_app.evaluate_zscore(key, 110.0, is_injected=True)
        assert hit_inj is True
    assert all(v == 40.0 for v in streaming_app.WINDOW_HISTORY[key])

    # Real sustained regime shift (40 -> 30 AQI) should adapt and stop spamming
    hits = [streaming_app.evaluate_zscore(key, 30.0)[0] for _ in range(6)]
    assert hits[0] is True
    assert hits[-1] is False
    assert 30.0 in streaming_app.WINDOW_HISTORY[key]


def _make_row(domain: str, did: str, val: float, **extras: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "domain": domain,
        "district_id": did,
        "avg_value": val,
        "min_value": val - 0.1,
        "max_value": val + 0.1,
        "metric_name": f"{domain}_metric",
        "unit": "u",
        "data_source": "test (live)",
        "avg_congestion_pct": None,
        "avg_vehicle_count": None,
        "disrupted_corridors": 0,
        "avg_humidity_pct": None,
        "avg_wind_speed_ms": None,
        "avg_pm2_5": None,
        "avg_pm10": None,
        "avg_renewable_pct": None,
        "avg_low_carbon_pct": None,
        "avg_national_load_mw": None,
        "avg_london_total_mw": None,
        "avg_carbon_intensity": None,
    }
    row.update(extras)
    return row


def test_city_domain_summary_aggregates_all_32_districts_across_partial_batches() -> None:
    districts_by_id = {
        f"d{i:02d}": {"district_id": f"d{i:02d}", "energy_weight": 1.0 / 32.0}
        for i in range(32)
    }

    # Feed 4 partial batches of 8 districts each for all 4 domains
    for batch_idx in range(4):
        start = batch_idx * 8
        slice_ids = [f"d{i:02d}" for i in range(start, start + 8)]

        traffic_rows = [
            _make_row(
                "traffic",
                did,
                20.0 + idx,
                avg_congestion_pct=40.0 + idx,
                avg_vehicle_count=50.0 + idx,
                disrupted_corridors=2 if batch_idx < 3 else 1,
            )
            for idx, did in enumerate(slice_ids, start=start)
        ]
        weather_rows = [
            _make_row("weather", did, 5.0 + (idx % 4) * 0.4, avg_humidity_pct=80.0, avg_wind_speed_ms=4.5)
            for idx, did in enumerate(slice_ids, start=start)
        ]
        aq_rows = [
            _make_row("air_quality", did, 16.0 + idx, avg_pm2_5=4.0 + idx * 0.1, avg_pm10=8.0 + idx * 0.2)
            for idx, did in enumerate(slice_ids, start=start)
        ]
        energy_rows = [
            _make_row(
                "energy",
                did,
                100.0 + idx * 4.0,
                avg_renewable_pct=35.0,
                avg_low_carbon_pct=50.0,
                avg_national_load_mw=32000.0,
                avg_london_total_mw=4480.0,
                avg_carbon_intensity=140.0,
            )
            for idx, did in enumerate(slice_ids, start=start)
        ]

        t_sum = streaming_app.update_city_domain_summary("traffic", traffic_rows, 8, districts_by_id)
        w_sum = streaming_app.update_city_domain_summary("weather", weather_rows, 8, districts_by_id)
        a_sum = streaming_app.update_city_domain_summary("air_quality", aq_rows, 8, districts_by_id)
        e_sum = streaming_app.update_city_domain_summary("energy", energy_rows, 8, districts_by_id)

    assert t_sum is not None and w_sum is not None and a_sum is not None and e_sum is not None
    # After 4 batches, all 32 districts are covered for all 4 domains
    for s in (t_sum, w_sum, a_sum, e_sum):
        assert s["districts_covered"] == 32
        assert s["event_count"] == 8

    assert t_sum["avg_value"] == round(sum(20.0 + i for i in range(32)) / 32, 2)
    assert t_sum["secondary"]["disrupted_corridors"] == 1
    assert w_sum["avg_value"] == round(sum(5.0 + (i % 4) * 0.4 for i in range(32)) / 32, 2)
    assert w_sum["secondary"]["humidity_pct"] == 80.0
    assert a_sum["avg_value"] == round(sum(16.0 + i for i in range(32)) / 32, 2)
    assert e_sum["avg_value"] == round(sum(100.0 + i * 4.0 for i in range(32)) / 32, 2)

    # A subsequent batch updating only the first 8 districts keeps all 32 districts in the city average
    next_weather = [
        _make_row("weather", f"d{i:02d}", 5.0 + (i % 4) * 0.4, avg_humidity_pct=80.0, avg_wind_speed_ms=4.5)
        for i in range(8)
    ]
    w_next = streaming_app.update_city_domain_summary("weather", next_weather, 8, districts_by_id)
    assert w_next is not None
    assert w_next["districts_covered"] == 32
    assert w_next["avg_value"] == w_sum["avg_value"]
