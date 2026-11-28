"""Persistent Microsoft SQL Server (MSSQL) Sink for Spark Structured Streaming.

Stores all aggregated city snapshots, per-borough measurements, anomaly alerts,
streaming telemetry, and platform state in MSSQL (`SmartCityDB`) so data survives
container recreations and can be queried directly from SSMS / Azure Data Studio
or via the backend API.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    import pymssql
except ImportError:  # pragma: no cover - optional in slim unit-test environments
    pymssql = None  # type: ignore[assignment]

logger = logging.getLogger("mssql_sink")

_SAFE_DB_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def _parse_iso_utc(ts_iso: str) -> datetime:
    cleaned = ts_iso.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _utc_to_london_naive(dt_utc_naive: datetime) -> datetime:
    dt_utc = dt_utc_naive.replace(tzinfo=timezone.utc)
    year = dt_utc.year
    mar31 = datetime(year, 3, 31, 1, 0, tzinfo=timezone.utc)
    bst_start = mar31 - timedelta(days=(mar31.weekday() + 1) % 7)
    oct31 = datetime(year, 10, 31, 1, 0, tzinfo=timezone.utc)
    bst_end = oct31 - timedelta(days=(oct31.weekday() + 1) % 7)
    offset = timedelta(hours=1) if bst_start <= dt_utc < bst_end else timedelta(0)
    return (dt_utc + offset).replace(tzinfo=None)


class MSSQLSink:
    """Resilient, non-fatal MSSQL persistence sink for Spark micro-batches."""

    def __init__(
        self,
        *,
        enabled: bool | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        user: str | None = None,
        password: str | None = None,
        retention_days: int | None = None,
    ) -> None:
        raw_enabled = os.getenv("MSSQL_ENABLED", "true").lower() in ("1", "true", "yes")
        self.enabled = raw_enabled if enabled is None else enabled
        self.host = host or os.getenv("MSSQL_HOST", "mssql")
        self.port = port if port is not None else int(os.getenv("MSSQL_PORT", "1433"))
        self.database = database or os.getenv("MSSQL_DATABASE", "SmartCityDB")
        self.user = user or os.getenv("MSSQL_USER", "sa")
        self.password = password or os.getenv("MSSQL_PASSWORD", "SmartCity#2026!")
        self.retention_days = (
            retention_days
            if retention_days is not None
            else max(0, int(os.getenv("MSSQL_RETENTION_DAYS", "90")))
        )

        if not _SAFE_DB_RE.match(self.database):
            raise ValueError(f"Unsafe MSSQL database name: {self.database!r}")

        self._conn: Any = None
        self._schema_ready = False
        self._batches_written = 0

    def is_available(self) -> bool:
        return self.enabled and pymssql is not None

    def _connect(self, database: str | None = None, timeout: int = 10, autocommit: bool = False) -> Any:
        if pymssql is None:
            raise RuntimeError("pymssql is not installed")
        return pymssql.connect(
            server=self.host,
            port=str(self.port),
            user=self.user,
            password=self.password,
            database=database or self.database,
            login_timeout=timeout,
            timeout=timeout,
            autocommit=autocommit,
            charset="UTF-8",
        )

    def _get_conn(self) -> Any:
        if self._conn is None:
            if not self._schema_ready:
                self.ensure_schema(retries=1, delay_sec=1.0)
            self._conn = self._connect(self.database, timeout=10, autocommit=False)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def ensure_schema(self, retries: int = 15, delay_sec: float = 2.0) -> bool:
        """Create SmartCityDB, tables, indexes, and SSMS views idempotently."""
        if not self.is_available():
            logger.info("MSSQL sink disabled or pymssql unavailable; skipping schema init.")
            return False

        last_err: Exception | None = None
        for attempt in range(1, retries + 1):
            master_conn = None
            db_conn = None
            try:
                master_conn = self._connect("master", timeout=10, autocommit=True)
                with master_conn.cursor() as cur:
                    cur.execute(
                        f"IF DB_ID(N'{self.database}') IS NULL CREATE DATABASE [{self.database}];"
                    )
                master_conn.close()
                master_conn = None

                db_conn = self._connect(self.database, timeout=15, autocommit=True)
                with db_conn.cursor() as cur:
                    for stmt in _DDL_STATEMENTS:
                        cur.execute(stmt)
                db_conn.close()
                db_conn = None

                self._schema_ready = True
                logger.info(
                    "MSSQL schema ready | server=%s:%d | db=%s",
                    self.host,
                    self.port,
                    self.database,
                )
                return True
            except Exception as exc:
                last_err = exc
                for c in (master_conn, db_conn):
                    if c is not None:
                        try:
                            c.close()
                        except Exception:
                            pass
                if attempt < retries:
                    logger.info(
                        "Waiting for MSSQL (%s:%d) attempt %d/%d: %s",
                        self.host,
                        self.port,
                        attempt,
                        retries,
                        exc,
                    )
                    time.sleep(delay_sec)

        logger.warning("MSSQL schema init failed after %d attempts: %s", retries, last_err)
        return False

    def restore_state(self) -> dict[str, Any] | None:
        """Recover cumulative counters, Z-score windows, recent alerts & timeseries from MSSQL."""
        if not self.is_available():
            return None
        try:
            conn = self._get_conn()
            result: dict[str, Any] = {
                "cumulative_events": 0,
                "cumulative_alerts": 0,
                "window_history": {},
                "recent_alerts": [],
                "timeseries": [],
            }
            with conn.cursor(as_dict=True) as cur:
                cur.execute(
                    """
                    SELECT toplam_olay_sayisi, toplam_uyari_sayisi, pencere_gecmisi_json
                    FROM dbo.platform_durumu
                    WHERE durum_anahtari = N'spark_streaming'
                    """
                )
                row = cur.fetchone()
                if row:
                    result["cumulative_events"] = max(0, int(row.get("toplam_olay_sayisi") or 0))
                    result["cumulative_alerts"] = max(0, int(row.get("toplam_uyari_sayisi") or 0))
                    raw_win = row.get("pencere_gecmisi_json")
                    if isinstance(raw_win, str) and raw_win:
                        try:
                            parsed_win = json.loads(raw_win)
                            if isinstance(parsed_win, dict):
                                result["window_history"] = parsed_win
                        except Exception:
                            pass

                cur.execute(
                    """
                    SELECT TOP 40
                        uyari_id, kayit_zamani_utc, bolge_id, bolge_adi, alan,
                        metrik_etiketi, birim, seviye, tespit_yontemi, z_skoru,
                        olculen_deger, referans_deger, esik_etiketi, aciklama, test_enjekte
                    FROM dbo.anomali_uyarilari
                    ORDER BY kayit_zamani_utc DESC, id DESC
                    """
                )
                alerts_out: list[dict[str, Any]] = []
                for al in cur.fetchall() or []:
                    rec_at = al["kayit_zamani_utc"]
                    ts_iso = (
                        rec_at.replace(tzinfo=timezone.utc).isoformat()
                        if isinstance(rec_at, datetime)
                        else str(rec_at)
                    )
                    method = al.get("tespit_yontemi") or ""
                    z_val = float(al["z_skoru"]) if al.get("z_skoru") is not None else 0.0
                    val = float(al["olculen_deger"]) if al.get("olculen_deger") is not None else 0.0
                    ref_val = float(al["referans_deger"]) if al.get("referans_deger") is not None else None
                    is_low = (ref_val is not None and val < ref_val) or (ref_val is None and z_val < 0)
                    alerts_out.append(
                        {
                            "alert_id": al["uyari_id"],
                            "timestamp": ts_iso,
                            "district_id": al["bolge_id"],
                            "district_name": al["bolge_adi"],
                            "domain": al["alan"],
                            "metric_label": al.get("metrik_etiketi") or "",
                            "unit": al.get("birim") or "",
                            "severity": al["seviye"],
                            "method": method,
                            "z_score": z_val,
                            "value": val,
                            "baseline_value": ref_val,
                            "baseline_label": (
                                "Son 1 Dakika Ortalaması" if method == "Z-Score Sapması" else "Normal Değer"
                            ),
                            "direction": "low" if is_low else "high",
                            "threshold_label": al.get("esik_etiketi") or "",
                            "message": al.get("aciklama") or "",
                            "injected": bool(al.get("test_enjekte")),
                        }
                    )
                result["recent_alerts"] = alerts_out

                cur.execute(
                    """
                    SELECT TOP 60
                        t.kayit_zamani_utc,
                        t.islenen_hiz_olay_sn,
                        t.p50_gecikme_ms,
                        MAX(CASE WHEN s.alan = N'traffic' THEN s.ortalama_deger END) AS traffic,
                        MAX(CASE WHEN s.alan = N'weather' THEN s.ortalama_deger END) AS weather,
                        MAX(CASE WHEN s.alan = N'air_quality' THEN s.ortalama_deger END) AS air_quality,
                        MAX(CASE WHEN s.alan = N'energy' THEN s.ortalama_deger END) AS energy
                    FROM dbo.batch_telemetri t
                    LEFT JOIN dbo.alan_ozetleri s
                        ON s.kayit_zamani_utc = t.kayit_zamani_utc AND s.batch_no = t.batch_no
                    GROUP BY t.kayit_zamani_utc, t.batch_no, t.islenen_hiz_olay_sn, t.p50_gecikme_ms
                    ORDER BY t.kayit_zamani_utc DESC
                    """
                )
                ts_rows = list(reversed(cur.fetchall() or []))
                ts_out: list[dict[str, Any]] = []
                for tr in ts_rows:
                    rec_at = tr["kayit_zamani_utc"]
                    if isinstance(rec_at, datetime):
                        lon_dt = _utc_to_london_naive(rec_at)
                        ts_iso = rec_at.replace(tzinfo=timezone.utc).isoformat()
                        time_label = lon_dt.strftime("%H:%M:%S")
                    else:
                        ts_iso = str(rec_at)
                        time_label = ts_iso[11:19]
                    ts_out.append(
                        {
                            "time": time_label,
                            "timestamp": ts_iso,
                            "traffic": tr.get("traffic"),
                            "weather": tr.get("weather"),
                            "air_quality": tr.get("air_quality"),
                            "energy": tr.get("energy"),
                            "throughput_eps": tr.get("islenen_hiz_olay_sn"),
                            "latency_ms": tr.get("p50_gecikme_ms"),
                        }
                    )
                result["timeseries"] = ts_out

            return result
        except Exception as exc:
            logger.warning("Non-fatal MSSQL state restore warning: %s", exc)
            self.close()
            return None

    def save_batch(
        self,
        *,
        batch_id: int,
        ts_iso: str,
        domains_summary: dict[str, dict[str, Any]],
        districts_map: dict[str, dict[str, Any]],
        new_alerts: list[dict[str, Any]],
        perf_snapshot: dict[str, Any],
        window_history: dict[tuple[str, str], Any],
    ) -> bool:
        """Persist one micro-batch atomically to MSSQL; reconnect once on transient error."""
        if not self.is_available():
            return False

        rec_utc = _parse_iso_utc(ts_iso)
        lon_naive = _utc_to_london_naive(rec_utc)

        domain_rows = [
            (
                rec_utc,
                lon_naive,
                int(batch_id),
                dom,
                info.get("metric_name"),
                float(info["avg_value"]),
                float(info["min_value"]) if info.get("min_value") is not None else None,
                float(info["max_value"]) if info.get("max_value") is not None else None,
                info.get("unit"),
                int(info.get("event_count", 0)),
                info.get("data_source"),
                json.dumps(info.get("secondary", {}), separators=(",", ":")),
            )
            for dom, info in domains_summary.items()
        ]

        district_rows = []
        for did, entry in districts_map.items():
            dname = entry.get("district_name") or did
            ztype = entry.get("zone_type") or "urban"
            status = entry.get("status") or "normal"
            disruptions = int(entry.get("tfl_disruptions") or 0)
            for dom, m in (entry.get("metrics") or {}).items():
                district_rows.append(
                    (
                        rec_utc,
                        lon_naive,
                        int(batch_id),
                        did,
                        dname,
                        ztype,
                        dom,
                        float(m["avg"]),
                        float(m["min"]) if m.get("min") is not None else None,
                        float(m["max"]) if m.get("max") is not None else None,
                        int(m.get("count", 0)),
                        status,
                        disruptions if dom == "traffic" else 0,
                    )
                )

        alert_rows = [
            (
                str(al["alert_id"]),
                rec_utc,
                lon_naive,
                str(al["district_id"]),
                str(al["district_name"]),
                str(al["domain"]),
                str(al.get("metric_label") or ""),
                str(al.get("unit") or ""),
                str(al["severity"]),
                str(al.get("method") or ""),
                float(al["z_score"]) if al.get("z_score") is not None else None,
                float(al["value"]) if al.get("value") is not None else None,
                float(al["baseline_value"]) if al.get("baseline_value") is not None else None,
                str(al.get("threshold_label") or ""),
                str(al.get("message") or ""),
                1 if al.get("injected") else 0,
            )
            for al in new_alerts
        ]

        telemetry_row = (
            rec_utc,
            int(batch_id),
            int(perf_snapshot.get("target_rate_eps") or 0),
            float(perf_snapshot.get("throughput_eps") or 0.0),
            int(perf_snapshot.get("batch_events") or 0),
            float(perf_snapshot.get("avg_latency_ms") or 0.0),
            float(perf_snapshot.get("p50_latency_ms") or 0.0),
            float(perf_snapshot.get("p95_latency_ms") or 0.0),
            float(perf_snapshot.get("spark_batch_ms") or 0.0),
            int(perf_snapshot.get("cumulative_events") or 0),
            int(perf_snapshot.get("cumulative_alerts") or 0),
            str(perf_snapshot.get("backpressure_status") or "healthy"),
            float(perf_snapshot.get("driver_rss_mb") or 0.0),
            float(perf_snapshot.get("lake_size_mb") or 0.0),
        )

        win_json = json.dumps(
            {f"{k[0]}|{k[1]}": list(v) for k, v in window_history.items()},
            separators=(",", ":"),
        )
        cum_events = int(perf_snapshot.get("cumulative_events") or 0)
        cum_alerts = int(perf_snapshot.get("cumulative_alerts") or 0)

        for attempt in range(2):
            try:
                conn = self._get_conn()
                with conn.cursor() as cur:
                    if domain_rows:
                        cur.executemany(
                            """
                            INSERT INTO dbo.alan_ozetleri (
                                kayit_zamani_utc, londra_zamani, batch_no, alan, metrik_adi,
                                ortalama_deger, en_dusuk_deger, en_yuksek_deger, birim, olay_sayisi,
                                veri_kaynagi, ikincil_metrikler_json
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            domain_rows,
                        )
                    if district_rows:
                        cur.executemany(
                            """
                            INSERT INTO dbo.bolge_olcumleri (
                                kayit_zamani_utc, londra_zamani, batch_no, bolge_id, bolge_adi,
                                bolge_tipi, alan, ortalama_deger, en_dusuk_deger, en_yuksek_deger,
                                olay_sayisi, durum, tfl_yol_aksamasi
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            district_rows,
                        )
                    for al_row in alert_rows:
                        cur.execute(
                            """
                            IF NOT EXISTS (SELECT 1 FROM dbo.anomali_uyarilari WHERE uyari_id = %s)
                            INSERT INTO dbo.anomali_uyarilari (
                                uyari_id, kayit_zamani_utc, londra_zamani, bolge_id, bolge_adi,
                                alan, metrik_etiketi, birim, seviye, tespit_yontemi, z_skoru,
                                olculen_deger, referans_deger, esik_etiketi, aciklama, test_enjekte
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (al_row[0], *al_row),
                        )

                    cur.execute(
                        """
                        INSERT INTO dbo.batch_telemetri (
                            kayit_zamani_utc, batch_no, hedef_hiz_olay_sn, islenen_hiz_olay_sn, batch_olay_sayisi,
                            ort_gecikme_ms, p50_gecikme_ms, p95_gecikme_ms, spark_islem_suresi_ms,
                            toplam_olay_sayisi, toplam_uyari_sayisi, geri_basinc_durumu,
                            surucu_bellek_mb, veri_golu_boyut_mb
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        telemetry_row,
                    )

                    cur.execute(
                        """
                        UPDATE dbo.platform_durumu
                        SET toplam_olay_sayisi = %s,
                            toplam_uyari_sayisi = %s,
                            son_batch_no = %s,
                            pencere_gecmisi_json = %s,
                            guncelleme_zamani = SYSUTCDATETIME()
                        WHERE durum_anahtari = N'spark_streaming';
                        IF @@ROWCOUNT = 0
                        INSERT INTO dbo.platform_durumu (
                            durum_anahtari, toplam_olay_sayisi, toplam_uyari_sayisi,
                            son_batch_no, pencere_gecmisi_json
                        ) VALUES (N'spark_streaming', %s, %s, %s, %s);
                        """,
                        (
                            cum_events,
                            cum_alerts,
                            int(batch_id),
                            win_json,
                            cum_events,
                            cum_alerts,
                            int(batch_id),
                            win_json,
                        ),
                    )

                    self._batches_written += 1
                    if self.retention_days > 0 and self._batches_written % 1000 == 0:
                        cutoff = rec_utc - timedelta(days=self.retention_days)
                        cur.execute("DELETE FROM dbo.bolge_olcumleri WHERE kayit_zamani_utc < %s", (cutoff,))
                        cur.execute("DELETE FROM dbo.alan_ozetleri WHERE kayit_zamani_utc < %s", (cutoff,))
                        cur.execute("DELETE FROM dbo.batch_telemetri WHERE kayit_zamani_utc < %s", (cutoff,))
                        cur.execute("DELETE FROM dbo.anomali_uyarilari WHERE kayit_zamani_utc < %s", (cutoff,))

                conn.commit()
                return True
            except Exception as exc:
                logger.warning("MSSQL write attempt %d failed on batch #%d: %s", attempt + 1, batch_id, exc)
                try:
                    if self._conn is not None:
                        self._conn.rollback()
                except Exception:
                    pass
                self.close()
        return False


_TABLE_RENAMES: list[tuple[str, str]] = [
    ("platform_state", "platform_durumu"),
    ("domain_snapshots", "alan_ozetleri"),
    ("district_measurements", "bolge_olcumleri"),
    ("anomaly_alerts", "anomali_uyarilari"),
    ("batch_telemetry", "batch_telemetri"),
]

_INDEX_RENAMES: list[tuple[str, str, str]] = [
    ("alan_ozetleri", "IX_domain_snapshots_london_domain", "IX_alan_ozetleri_londra_alan"),
    ("bolge_olcumleri", "IX_district_measurements_london_dist_dom", "IX_bolge_olcumleri_londra_bolge_alan"),
    ("anomali_uyarilari", "IX_anomaly_alerts_recorded", "IX_anomali_uyarilari_kayit_zamani"),
    ("batch_telemetri", "IX_batch_telemetry_recorded", "IX_batch_telemetri_kayit_zamani"),
]

_LEGACY_VIEWS: list[str] = [
    "vw_hourly_city_summary",
    "vw_recent_alerts",
    "vw_latest_district_status",
]

_COLUMN_RENAMES: list[tuple[str, str, str]] = [
    # dbo.platform_durumu
    ("platform_durumu", "state_key", "durum_anahtari"),
    ("platform_durumu", "cumulative_events", "toplam_olay_sayisi"),
    ("platform_durumu", "cumulative_alerts", "toplam_uyari_sayisi"),
    ("platform_durumu", "last_batch_id", "son_batch_no"),
    ("platform_durumu", "window_history_json", "pencere_gecmisi_json"),
    ("platform_durumu", "updated_at", "guncelleme_zamani"),
    # dbo.alan_ozetleri
    ("alan_ozetleri", "recorded_at", "kayit_zamani_utc"),
    ("alan_ozetleri", "london_time", "londra_zamani"),
    ("alan_ozetleri", "batch_id", "batch_no"),
    ("alan_ozetleri", "domain", "alan"),
    ("alan_ozetleri", "metric_name", "metrik_adi"),
    ("alan_ozetleri", "avg_value", "ortalama_deger"),
    ("alan_ozetleri", "min_value", "en_dusuk_deger"),
    ("alan_ozetleri", "max_value", "en_yuksek_deger"),
    ("alan_ozetleri", "unit", "birim"),
    ("alan_ozetleri", "event_count", "olay_sayisi"),
    ("alan_ozetleri", "data_source", "veri_kaynagi"),
    ("alan_ozetleri", "secondary_json", "ikincil_metrikler_json"),
    # dbo.bolge_olcumleri
    ("bolge_olcumleri", "recorded_at", "kayit_zamani_utc"),
    ("bolge_olcumleri", "london_time", "londra_zamani"),
    ("bolge_olcumleri", "batch_id", "batch_no"),
    ("bolge_olcumleri", "district_id", "bolge_id"),
    ("bolge_olcumleri", "district_name", "bolge_adi"),
    ("bolge_olcumleri", "zone_type", "bolge_tipi"),
    ("bolge_olcumleri", "domain", "alan"),
    ("bolge_olcumleri", "avg_value", "ortalama_deger"),
    ("bolge_olcumleri", "min_value", "en_dusuk_deger"),
    ("bolge_olcumleri", "max_value", "en_yuksek_deger"),
    ("bolge_olcumleri", "event_count", "olay_sayisi"),
    ("bolge_olcumleri", "status", "durum"),
    ("bolge_olcumleri", "tfl_disruptions", "tfl_yol_aksamasi"),
    # dbo.anomali_uyarilari
    ("anomali_uyarilari", "alert_id", "uyari_id"),
    ("anomali_uyarilari", "recorded_at", "kayit_zamani_utc"),
    ("anomali_uyarilari", "london_time", "londra_zamani"),
    ("anomali_uyarilari", "district_id", "bolge_id"),
    ("anomali_uyarilari", "district_name", "bolge_adi"),
    ("anomali_uyarilari", "domain", "alan"),
    ("anomali_uyarilari", "metric_label", "metrik_etiketi"),
    ("anomali_uyarilari", "unit", "birim"),
    ("anomali_uyarilari", "severity", "seviye"),
    ("anomali_uyarilari", "method", "tespit_yontemi"),
    ("anomali_uyarilari", "z_score", "z_skoru"),
    ("anomali_uyarilari", "measured_value", "olculen_deger"),
    ("anomali_uyarilari", "baseline_value", "referans_deger"),
    ("anomali_uyarilari", "threshold_label", "esik_etiketi"),
    ("anomali_uyarilari", "message", "aciklama"),
    ("anomali_uyarilari", "injected", "test_enjekte"),
    # dbo.batch_telemetri
    ("batch_telemetri", "recorded_at", "kayit_zamani_utc"),
    ("batch_telemetri", "batch_id", "batch_no"),
    ("batch_telemetri", "target_rate_eps", "hedef_hiz_olay_sn"),
    ("batch_telemetri", "throughput_eps", "islenen_hiz_olay_sn"),
    ("batch_telemetri", "batch_events", "batch_olay_sayisi"),
    ("batch_telemetri", "avg_latency_ms", "ort_gecikme_ms"),
    ("batch_telemetri", "p50_latency_ms", "p50_gecikme_ms"),
    ("batch_telemetri", "p95_latency_ms", "p95_gecikme_ms"),
    ("batch_telemetri", "spark_batch_ms", "spark_islem_suresi_ms"),
    ("batch_telemetri", "cumulative_events", "toplam_olay_sayisi"),
    ("batch_telemetri", "cumulative_alerts", "toplam_uyari_sayisi"),
    ("batch_telemetri", "backpressure_status", "geri_basinc_durumu"),
    ("batch_telemetri", "driver_rss_mb", "surucu_bellek_mb"),
    ("batch_telemetri", "lake_size_mb", "veri_golu_boyut_mb"),
]


def _build_table_migration_statements() -> list[str]:
    """Generate idempotent sp_rename statements for zero-data-loss Turkish table rename."""
    return [
        f"""
        IF OBJECT_ID(N'dbo.{old_table}', N'U') IS NOT NULL
           AND OBJECT_ID(N'dbo.{new_table}', N'U') IS NULL
        BEGIN
            EXEC sp_rename N'dbo.{old_table}', N'{new_table}';
        END;
        """
        for old_table, new_table in _TABLE_RENAMES
    ]


def _build_column_migration_statements() -> list[str]:
    """Generate idempotent sp_rename statements for zero-data-loss Turkish column migration."""
    return [
        f"""
        IF COL_LENGTH(N'dbo.{table}', N'{old_col}') IS NOT NULL
           AND COL_LENGTH(N'dbo.{table}', N'{new_col}') IS NULL
        BEGIN
            EXEC sp_rename N'dbo.{table}.{old_col}', N'{new_col}', N'COLUMN';
        END;
        """
        for table, old_col, new_col in _COLUMN_RENAMES
    ]


def _build_index_migration_statements() -> list[str]:
    """Generate idempotent sp_rename statements for Turkish index names."""
    return [
        f"""
        IF EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = N'{old_idx}' AND object_id = OBJECT_ID(N'dbo.{table}')
        ) AND NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = N'{new_idx}' AND object_id = OBJECT_ID(N'dbo.{table}')
        )
        BEGIN
            EXEC sp_rename N'dbo.{table}.{old_idx}', N'{new_idx}', N'INDEX';
        END;
        """
        for table, old_idx, new_idx in _INDEX_RENAMES
    ]


_TABLE_DDL_STATEMENTS: list[str] = [
    """
    IF OBJECT_ID(N'dbo.platform_durumu', N'U') IS NULL
    CREATE TABLE dbo.platform_durumu (
        durum_anahtari NVARCHAR(64) NOT NULL PRIMARY KEY,
        toplam_olay_sayisi BIGINT NOT NULL DEFAULT 0,
        toplam_uyari_sayisi BIGINT NOT NULL DEFAULT 0,
        son_batch_no BIGINT NOT NULL DEFAULT 0,
        pencere_gecmisi_json NVARCHAR(MAX) NULL,
        guncelleme_zamani DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    """,
    """
    IF OBJECT_ID(N'dbo.alan_ozetleri', N'U') IS NULL
    CREATE TABLE dbo.alan_ozetleri (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        kayit_zamani_utc DATETIME2 NOT NULL,
        londra_zamani DATETIME2 NOT NULL,
        batch_no BIGINT NOT NULL,
        alan NVARCHAR(32) NOT NULL,
        metrik_adi NVARCHAR(64) NULL,
        ortalama_deger FLOAT NOT NULL,
        en_dusuk_deger FLOAT NULL,
        en_yuksek_deger FLOAT NULL,
        birim NVARCHAR(32) NULL,
        olay_sayisi INT NOT NULL,
        veri_kaynagi NVARCHAR(128) NULL,
        ikincil_metrikler_json NVARCHAR(MAX) NULL
    );
    """,
    """
    IF OBJECT_ID(N'dbo.bolge_olcumleri', N'U') IS NULL
    CREATE TABLE dbo.bolge_olcumleri (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        kayit_zamani_utc DATETIME2 NOT NULL,
        londra_zamani DATETIME2 NOT NULL,
        batch_no BIGINT NOT NULL,
        bolge_id NVARCHAR(64) NOT NULL,
        bolge_adi NVARCHAR(128) NOT NULL,
        bolge_tipi NVARCHAR(32) NULL,
        alan NVARCHAR(32) NOT NULL,
        ortalama_deger FLOAT NOT NULL,
        en_dusuk_deger FLOAT NULL,
        en_yuksek_deger FLOAT NULL,
        olay_sayisi INT NOT NULL,
        durum NVARCHAR(32) NOT NULL,
        tfl_yol_aksamasi INT NOT NULL DEFAULT 0
    );
    """,
    """
    IF OBJECT_ID(N'dbo.anomali_uyarilari', N'U') IS NULL
    CREATE TABLE dbo.anomali_uyarilari (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        uyari_id NVARCHAR(64) NOT NULL UNIQUE,
        kayit_zamani_utc DATETIME2 NOT NULL,
        londra_zamani DATETIME2 NOT NULL,
        bolge_id NVARCHAR(64) NOT NULL,
        bolge_adi NVARCHAR(128) NOT NULL,
        alan NVARCHAR(32) NOT NULL,
        metrik_etiketi NVARCHAR(64) NULL,
        birim NVARCHAR(32) NULL,
        seviye NVARCHAR(32) NOT NULL,
        tespit_yontemi NVARCHAR(64) NULL,
        z_skoru FLOAT NULL,
        olculen_deger FLOAT NULL,
        referans_deger FLOAT NULL,
        esik_etiketi NVARCHAR(128) NULL,
        aciklama NVARCHAR(512) NULL,
        test_enjekte BIT NOT NULL DEFAULT 0
    );
    """,
    """
    IF OBJECT_ID(N'dbo.batch_telemetri', N'U') IS NULL
    CREATE TABLE dbo.batch_telemetri (
        id BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        kayit_zamani_utc DATETIME2 NOT NULL,
        batch_no BIGINT NOT NULL,
        hedef_hiz_olay_sn INT NULL,
        islenen_hiz_olay_sn FLOAT NULL,
        batch_olay_sayisi INT NOT NULL,
        ort_gecikme_ms FLOAT NULL,
        p50_gecikme_ms FLOAT NULL,
        p95_gecikme_ms FLOAT NULL,
        spark_islem_suresi_ms FLOAT NULL,
        toplam_olay_sayisi BIGINT NOT NULL,
        toplam_uyari_sayisi BIGINT NOT NULL,
        geri_basinc_durumu NVARCHAR(32) NULL,
        surucu_bellek_mb FLOAT NULL,
        veri_golu_boyut_mb FLOAT NULL
    );
    """,
]

_INDEX_DDL_STATEMENTS: list[str] = [
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_alan_ozetleri_londra_alan'
          AND object_id = OBJECT_ID(N'dbo.alan_ozetleri')
    )
    CREATE INDEX IX_alan_ozetleri_londra_alan
        ON dbo.alan_ozetleri (londra_zamani DESC, alan)
        INCLUDE (ortalama_deger, en_dusuk_deger, en_yuksek_deger, olay_sayisi);
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_bolge_olcumleri_londra_bolge_alan'
          AND object_id = OBJECT_ID(N'dbo.bolge_olcumleri')
    )
    CREATE INDEX IX_bolge_olcumleri_londra_bolge_alan
        ON dbo.bolge_olcumleri (londra_zamani DESC, bolge_id, alan)
        INCLUDE (ortalama_deger, durum, olay_sayisi);
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_anomali_uyarilari_kayit_zamani'
          AND object_id = OBJECT_ID(N'dbo.anomali_uyarilari')
    )
    CREATE INDEX IX_anomali_uyarilari_kayit_zamani
        ON dbo.anomali_uyarilari (kayit_zamani_utc DESC)
        INCLUDE (bolge_id, alan, seviye);
    """,
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_batch_telemetri_kayit_zamani'
          AND object_id = OBJECT_ID(N'dbo.batch_telemetri')
    )
    CREATE INDEX IX_batch_telemetri_kayit_zamani
        ON dbo.batch_telemetri (kayit_zamani_utc DESC);
    """,
]

_VIEW_DDL_STATEMENTS: list[str] = [
    *[f"DROP VIEW IF EXISTS dbo.{legacy_view};" for legacy_view in _LEGACY_VIEWS],
    """
    CREATE OR ALTER VIEW dbo.vw_saatlik_sehir_ozeti AS
    SELECT
        DATEADD(HOUR, DATEDIFF(HOUR, 0, londra_zamani), 0) AS saat_londra,
        ROUND(AVG(CASE WHEN alan = N'traffic' THEN ortalama_deger END), 2) AS trafik_hiz_kms,
        ROUND(AVG(CASE WHEN alan = N'weather' THEN ortalama_deger END), 2) AS sicaklik_c,
        ROUND(AVG(CASE WHEN alan = N'air_quality' THEN ortalama_deger END), 2) AS hava_kalitesi_aqi,
        ROUND(AVG(CASE WHEN alan = N'energy' THEN ortalama_deger END), 2) AS enerji_mw,
        SUM(olay_sayisi) AS toplam_kayit
    FROM dbo.alan_ozetleri
    GROUP BY DATEADD(HOUR, DATEDIFF(HOUR, 0, londra_zamani), 0);
    """,
    """
    CREATE OR ALTER VIEW dbo.vw_son_uyarilar AS
    SELECT
        londra_zamani AS zaman_londra,
        bolge_adi AS bolge,
        alan,
        seviye,
        metrik_etiketi AS metrik,
        olculen_deger,
        birim,
        referans_deger AS normal_deger,
        tespit_yontemi AS yontem,
        z_skoru,
        aciklama
    FROM dbo.anomali_uyarilari;
    """,
    """
    CREATE OR ALTER VIEW dbo.vw_son_bolge_durumu AS
    WITH ranked AS (
        SELECT
            bolge_id,
            bolge_adi,
            bolge_tipi,
            alan,
            ortalama_deger,
            durum,
            tfl_yol_aksamasi,
            londra_zamani,
            ROW_NUMBER() OVER (PARTITION BY bolge_id, alan ORDER BY kayit_zamani_utc DESC, id DESC) AS rn
        FROM dbo.bolge_olcumleri
    )
    SELECT
        bolge_id,
        bolge_adi,
        bolge_tipi,
        MAX(londra_zamani) AS son_guncelleme,
        ROUND(MAX(CASE WHEN alan = N'traffic' THEN ortalama_deger END), 2) AS trafik_hiz_kms,
        ROUND(MAX(CASE WHEN alan = N'weather' THEN ortalama_deger END), 2) AS sicaklik_c,
        ROUND(MAX(CASE WHEN alan = N'air_quality' THEN ortalama_deger END), 2) AS hava_kalitesi_aqi,
        ROUND(MAX(CASE WHEN alan = N'energy' THEN ortalama_deger END), 2) AS enerji_mw,
        MAX(tfl_yol_aksamasi) AS yol_aksamasi
    FROM ranked
    WHERE rn = 1
    GROUP BY bolge_id, bolge_adi, bolge_tipi;
    """,
]

_DDL_STATEMENTS: list[str] = (
    _build_table_migration_statements()
    + _TABLE_DDL_STATEMENTS
    + _build_column_migration_statements()
    + _build_index_migration_statements()
    + _INDEX_DDL_STATEMENTS
    + _VIEW_DDL_STATEMENTS
)
