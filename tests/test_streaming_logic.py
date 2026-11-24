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
    streaming_app.DISTRICT_ENERGY_BASE.clear()
    streaming_app.DISTRICT_ENERGY_BASE["westminster"] = 1000.0


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
