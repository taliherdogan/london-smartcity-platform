"""Greater London Smart City — Resilient FastAPI Real-Time WebSocket & Control Backend.

Infrastructure Hardening:
- Pooled async Redis client with timeouts, retries, and graceful 503 degradation.
- Non-blocking concurrent WebSocket fan-out (asyncio.gather + per-client send timeout)
  so slow/stalled browsers are evicted immediately and never block Redis Pub/Sub.
- Latest-frame coalescing buffer so burst traffic never queues unbounded frames in RAM.
- Exposes /api/system for live infrastructure observability (Redis memory, Spark backpressure
  & RSS, Generator throughput/drops, Data Lake footprint, active WebSocket clients).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from datetime import date as date_type

from backfill_service import (
    backfill_settings,
    get_backfill_status,
    sync_missing_to_mssql,
    wait_for_mssql_schema,
)
from history_service import (
    load_districts_metadata,
    london_now,
    query_historical_data,
    resolve_date_range,
)
from mssql_reader import (
    get_storage_summary,
    query_saved_alerts,
    query_saved_snapshots,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backend")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
BENCHMARK_RESULTS_DIR = Path(os.getenv("BENCHMARK_RESULTS_DIR", "/app/benchmark_results"))
METADATA_PATH = os.getenv("METADATA_PATH", "/data/metadata/london_districts.json")
LAKE_DIR = os.getenv("LAKE_DIR", "/data/lake")
MAX_EVENT_RATE = int(os.getenv("MAX_EVENT_RATE", "25000"))
WS_MAX_CLIENTS = int(os.getenv("WS_MAX_CLIENTS", "200"))
WS_SEND_TIMEOUT_SEC = float(os.getenv("WS_SEND_TIMEOUT_SEC", "2.0"))
CORS_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000,http://localhost:5173,http://127.0.0.1:5173",
    ).split(",")
    if o.strip()
]

REDIS_PUBSUB_CHANNEL = "smartcity:live_updates"
REDIS_LATEST_KEY = "smartcity:latest_state"
REDIS_SOURCES_KEY = "smartcity:api_sources"
REDIS_RATE_KEY = "smartcity:control:rate"
REDIS_ANOMALY_KEY = "smartcity:control:anomaly"
REDIS_TELEMETRY_PREFIX = "smartcity:rate_telemetry:"
REDIS_INFRA_SPARK_KEY = "smartcity:infra:spark"
REDIS_INFRA_GEN_KEY = "smartcity:infra:generator"
REDIS_HISTORY_CACHE_PREFIX = "smartcity:history:v1:"


class ConnectionManager:
    def __init__(self, max_clients: int = WS_MAX_CLIENTS, send_timeout: float = WS_SEND_TIMEOUT_SEC) -> None:
        self.active: set[WebSocket] = set()
        self.max_clients = max_clients
        self.send_timeout = send_timeout
        self.dropped_slow_clients = 0
        self.coalesced_frames = 0
        self._latest_frame: str | None = None
        self._frame_event = asyncio.Event()

    async def connect(self, ws: WebSocket) -> bool:
        if len(self.active) >= self.max_clients:
            await ws.close(code=1013, reason="Server busy: max WebSocket clients reached")
            return False
        await ws.accept()
        self.active.add(ws)
        return True

    def disconnect(self, ws: WebSocket) -> None:
        self.active.discard(ws)

    def enqueue_frame(self, message: str) -> None:
        """Keep only the freshest snapshot if broadcasters are mid-flush (O(1) RAM)."""
        if self._latest_frame is not None:
            self.coalesced_frames += 1
        self._latest_frame = message
        self._frame_event.set()

    async def _send_one(self, ws: WebSocket, message: str) -> WebSocket | None:
        try:
            await asyncio.wait_for(ws.send_text(message), timeout=self.send_timeout)
            return None
        except Exception:
            return ws

    async def broadcast(self, message: str) -> None:
        targets = list(self.active)
        if not targets:
            return
        results = await asyncio.gather(*(self._send_one(ws, message) for ws in targets), return_exceptions=True)
        for item in results:
            if isinstance(item, WebSocket):
                self.active.discard(item)
                self.dropped_slow_clients += 1
                try:
                    await item.close(code=1001)
                except Exception:
                    pass

    async def broadcaster_loop(self) -> None:
        while True:
            try:
                await self._frame_event.wait()
                self._frame_event.clear()
                frame = self._latest_frame
                self._latest_frame = None
                if frame:
                    await self.broadcast(frame)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Broadcaster loop error: %s", exc)
                await asyncio.sleep(0.2)


manager = ConnectionManager()
redis_client: aioredis.Redis | None = None
pubsub_task: asyncio.Task[None] | None = None
broadcaster_task: asyncio.Task[None] | None = None
backfill_task: asyncio.Task[None] | None = None
_benchmark_cache: tuple[float, dict[str, Any]] | None = None


async def _clear_history_cache() -> int:
    if redis_client is None:
        return 0
    cleared = 0
    try:
        keys: list[str] = []
        async for key in redis_client.scan_iter(match=f"{REDIS_HISTORY_CACHE_PREFIX}*"):
            keys.append(key)
        if keys:
            cleared = int(await redis_client.delete(*keys))
    except Exception as exc:
        logger.debug("Non-fatal Redis history cache clear warning: %s", exc)
    return cleared


async def auto_backfill_loop() -> None:
    """On startup, backfill missing past hours from official historical APIs into MSSQL."""
    cfg = backfill_settings()
    if not cfg["auto_enabled"]:
        logger.info("Automatic MSSQL backfill disabled via MSSQL_AUTO_BACKFILL_ENABLED=false")
        return

    try:
        schema_ready = await wait_for_mssql_schema(max_wait_sec=60.0)
        if not schema_ready:
            logger.info("MSSQL schema not ready within 60s; skipping initial auto-backfill.")
            return

        res = await sync_missing_to_mssql(
            metadata_path=METADATA_PATH,
            hours=cfg["auto_hours"],
            source_mode="auto",
            include_districts=True,
            trigger="startup",
        )
        if res.get("total_rows_inserted", 0) > 0:
            await _clear_history_cache()
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logger.warning("Non-fatal auto-backfill startup warning: %s", exc)


async def redis_listener() -> None:
    assert redis_client is not None
    while True:
        pubsub = None
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(REDIS_PUBSUB_CHANNEL)
            logger.info("Subscribed to Redis channel %s", REDIS_PUBSUB_CHANNEL)
            async for msg in pubsub.listen():
                if msg.get("type") == "message" and msg.get("data"):
                    manager.enqueue_frame(msg["data"])
        except asyncio.CancelledError:
            if pubsub is not None:
                try:
                    await pubsub.unsubscribe(REDIS_PUBSUB_CHANNEL)
                    await pubsub.aclose()
                except Exception:
                    pass
            break
        except Exception as exc:
            logger.warning("Redis pubsub reconnecting after error: %s", exc)
            if pubsub is not None:
                try:
                    await pubsub.aclose()
                except Exception:
                    pass
            await asyncio.sleep(2.0)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global redis_client, pubsub_task, broadcaster_task, backfill_task
    pool = aioredis.ConnectionPool(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        max_connections=50,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
        health_check_interval=15,
    )
    redis_client = aioredis.Redis(connection_pool=pool)
    broadcaster_task = asyncio.create_task(manager.broadcaster_loop())
    pubsub_task = asyncio.create_task(redis_listener())
    backfill_task = asyncio.create_task(auto_backfill_loop())
    yield
    for task in (pubsub_task, broadcaster_task, backfill_task):
        if task:
            task.cancel()
    if redis_client:
        await redis_client.aclose()


app = FastAPI(
    title="London Smart City Real-Time Platform API",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


class RateRequest(BaseModel):
    rate: int = Field(..., ge=10, le=MAX_EVENT_RATE, description="Target events per second")


class AnomalyRequest(BaseModel):
    domain: str = Field(..., pattern="^(traffic|weather|air_quality|energy)$")
    district_id: str = Field(default="westminster")
    duration_sec: int = Field(default=15, ge=5, le=60)


class BackfillRequest(BaseModel):
    start_date: str | None = Field(default=None, description="Start date (YYYY-MM-DD)")
    end_date: str | None = Field(default=None, description="End date (YYYY-MM-DD)")
    hours: int | None = Field(default=None, ge=1, le=744, description="Shortcut: sync last N hours")
    source_mode: str = Field(default="auto", pattern="^(auto|api_only)$")
    include_districts: bool = Field(default=True, description="Also fill 32 boroughs in district_measurements")
    allow_traffic_model: bool | None = Field(
        default=None,
        description="Override MSSQL_BACKFILL_ALLOW_TRAFFIC_MODEL (default: false -> real traffic only)",
    )


def _require_redis() -> aioredis.Redis:
    if redis_client is None:
        raise HTTPException(status_code=503, detail="Redis client not initialized")
    return redis_client


@app.get("/api/health")
async def health() -> dict[str, Any]:
    redis_ok = False
    if redis_client:
        try:
            redis_ok = bool(await redis_client.ping())
        except Exception:
            redis_ok = False
    return {
        "status": "ok" if redis_ok else "degraded",
        "redis": redis_ok,
        "clients": len(manager.active),
        "max_clients": manager.max_clients,
    }


@app.get("/api/system")
async def system_status() -> dict[str, Any]:
    """Live infrastructure health & resource telemetry across Redis, Spark, Generator, and Backend."""
    r = _require_redis()
    try:
        mem_info, raw_spark, raw_gen, mssql_info = await asyncio.gather(
            r.info("memory"),
            r.get(REDIS_INFRA_SPARK_KEY),
            r.get(REDIS_INFRA_GEN_KEY),
            asyncio.to_thread(get_storage_summary),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {exc}") from exc

    used_mb = round(float(mem_info.get("used_memory", 0)) / (1024 * 1024), 2)
    max_mb = round(float(mem_info.get("maxmemory", 0)) / (1024 * 1024), 2)
    spark_info = json.loads(raw_spark) if raw_spark else None
    gen_info = json.loads(raw_gen) if raw_gen else None

    overall = "healthy"
    if spark_info and spark_info.get("status") == "backpressure":
        overall = "backpressure"
    elif (spark_info and spark_info.get("status") == "elevated") or (gen_info and gen_info.get("status") != "healthy"):
        overall = "elevated"

    return {
        "status": overall,
        "redis": {
            "used_memory_mb": used_mb,
            "max_memory_mb": max_mb,
            "usage_pct": round((used_mb / max_mb) * 100.0, 1) if max_mb > 0 else 0.0,
        },
        "spark": spark_info,
        "generator": gen_info,
        "mssql": mssql_info,
        "backfill": get_backfill_status(),
        "backend": {
            "active_ws_clients": len(manager.active),
            "max_ws_clients": manager.max_clients,
            "dropped_slow_clients": manager.dropped_slow_clients,
            "coalesced_frames": manager.coalesced_frames,
            "max_event_rate": MAX_EVENT_RATE,
        },
    }


@app.get("/api/sources")
async def get_sources() -> dict[str, Any]:
    r = _require_redis()
    try:
        raw, raw_rate, raw_anom = await asyncio.gather(
            r.get(REDIS_SOURCES_KEY),
            r.get(REDIS_RATE_KEY),
            r.get(REDIS_ANOMALY_KEY),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {exc}") from exc

    sources = json.loads(raw) if raw else {}
    rate = int(raw_rate or 10)
    active_anom = json.loads(raw_anom) if raw_anom else None
    if active_anom and active_anom.get("expires_at", 0) <= time.time():
        active_anom = None
    return {
        "city": "London, United Kingdom",
        "target_rate": rate,
        "max_rate": MAX_EVENT_RATE,
        "active_anomaly": active_anom,
        "sources": sources,
    }


@app.get("/api/state")
async def get_latest_state() -> dict[str, Any]:
    r = _require_redis()
    try:
        raw_state, raw_sources, raw_rate = await asyncio.gather(
            r.get(REDIS_LATEST_KEY),
            r.get(REDIS_SOURCES_KEY),
            r.get(REDIS_RATE_KEY),
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {exc}") from exc

    return {
        "state": json.loads(raw_state) if raw_state else None,
        "sources": json.loads(raw_sources) if raw_sources else {},
        "target_rate": int(raw_rate or 10),
    }


@app.post("/api/control/rate")
async def set_rate(req: RateRequest) -> dict[str, Any]:
    r = _require_redis()
    try:
        await r.set(REDIS_RATE_KEY, str(req.rate))
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {exc}") from exc
    logger.info("Updated target stream rate to %d ev/s", req.rate)
    return {"ok": True, "target_rate": req.rate}


@app.post("/api/control/anomaly")
async def trigger_anomaly(req: AnomalyRequest) -> dict[str, Any]:
    r = _require_redis()
    payload = {
        "domain": req.domain,
        "district_id": req.district_id,
        "expires_at": time.time() + req.duration_sec,
        "duration_sec": req.duration_sec,
    }
    try:
        await r.set(REDIS_ANOMALY_KEY, json.dumps(payload), ex=req.duration_sec + 5)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis unavailable: {exc}") from exc
    logger.info("Triggered %ds test anomaly: domain=%s district=%s", req.duration_sec, req.domain, req.district_id)
    return {"ok": True, "anomaly": payload}


@app.get("/api/benchmarks")
async def get_benchmarks() -> dict[str, Any]:
    global _benchmark_cache
    r = _require_redis()
    summary_file = BENCHMARK_RESULTS_DIR / "benchmark_summary.json"
    summary: dict[str, Any] = {}
    has_file = summary_file.exists()
    if has_file:
        try:
            mtime = summary_file.stat().st_mtime
            if _benchmark_cache is not None and _benchmark_cache[0] == mtime:
                summary = _benchmark_cache[1]
            else:
                summary = json.loads(summary_file.read_text(encoding="utf-8"))
                _benchmark_cache = (mtime, summary)
        except Exception as exc:
            logger.warning("Failed reading benchmark_summary.json: %s", exc)

    # Dynamically discover all recorded streaming rate buckets and fetch in parallel
    live_rates: list[dict[str, Any]] = []
    try:
        discovered_rates: set[int] = {100, 500, 2000, 5000}
        async for key in r.scan_iter(match=f"{REDIS_TELEMETRY_PREFIX}*"):
            suffix = key.removeprefix(REDIS_TELEMETRY_PREFIX)
            if suffix.isdigit():
                discovered_rates.add(int(suffix))

        sorted_rates = sorted(discovered_rates)
        all_entries = await asyncio.gather(
            *(r.lrange(f"{REDIS_TELEMETRY_PREFIX}{rate}", 0, -1) for rate in sorted_rates)
        )
        for rate, entries in zip(sorted_rates, all_entries):
            if entries:
                parsed = [json.loads(x) for x in entries]
                count = len(parsed)
                live_rates.append(
                    {
                        "target_rate_eps": rate,
                        "samples": count,
                        "avg_throughput_eps": round(sum(p["throughput_eps"] for p in parsed) / count, 1),
                        "avg_p50_latency_ms": round(sum(p["p50_latency_ms"] for p in parsed) / count, 1),
                        "avg_p95_latency_ms": round(sum(p["p95_latency_ms"] for p in parsed) / count, 1),
                        "avg_spark_batch_ms": round(sum(p["spark_batch_ms"] for p in parsed) / count, 1),
                    }
                )
    except Exception as exc:
        logger.warning("Failed reading live rate telemetry from Redis: %s", exc)

    return {
        "has_benchmark_file": has_file,
        "benchmark_summary": summary,
        "live_streaming_telemetry": live_rates,
    }


@app.get("/api/districts")
async def list_districts() -> dict[str, Any]:
    """Return the 32 Greater London boroughs for selectors and filters."""
    districts = load_districts_metadata(METADATA_PATH)
    return {
        "count": len(districts),
        "districts": [
            {
                "district_id": d["district_id"],
                "district_name": d["district_name"],
                "zone_type": d.get("zone_type", "urban"),
                "free_flow_kmh": d.get("free_flow_kmh"),
            }
            for d in districts
        ],
    }


@app.get("/api/history")
async def get_history(
    date: str | None = Query(default=None, description="Single date (YYYY-MM-DD), e.g. 5 days ago"),
    start_date: str | None = Query(default=None, description="Range start date (YYYY-MM-DD)"),
    end_date: str | None = Query(default=None, description="Range end date (YYYY-MM-DD)"),
    days_ago: int | None = Query(default=None, ge=0, le=365, description="Shortcut: exact day N days ago"),
    district_id: str = Query(default="all", description="'all' or a London borough district_id"),
    granularity: str = Query(default="auto", pattern="^(auto|hourly|daily)$"),
    refresh: bool = Query(default=False, description="Bypass Redis cache"),
) -> dict[str, Any]:
    """Query historical London traffic, weather, air quality & energy data.

    Prioritizes archived Spark Parquet Data Lake partitions and seamlessly backfills
    missing hours from official zero-key historical APIs (Open-Meteo, Elexon BMRS, NESO).
    """
    today_lon = london_now().date()
    try:
        s_date, e_date = resolve_date_range(
            single_date=date,
            start_date=start_date,
            end_date=end_date,
            days_ago=days_ago,
            today=today_lon,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    cache_key = f"{REDIS_HISTORY_CACHE_PREFIX}{s_date.isoformat()}:{e_date.isoformat()}:{district_id}:{granularity}"
    r = redis_client

    latest_state: dict[str, Any] | None = None
    if r is not None:
        try:
            if not refresh:
                cached = await r.get(cache_key)
                if cached:
                    payload = json.loads(cached)
                    payload["cached"] = True
                    return payload
            raw_latest = await r.get(REDIS_LATEST_KEY)
            if raw_latest:
                latest_state = json.loads(raw_latest)
        except Exception as exc:
            logger.debug("Non-fatal Redis history cache read warning: %s", exc)

    try:
        payload = await query_historical_data(
            lake_dir=LAKE_DIR,
            metadata_path=METADATA_PATH,
            start_date=s_date.isoformat(),
            end_date=e_date.isoformat(),
            district_id=district_id,
            granularity=granularity,
            latest_state=latest_state,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Historical query failed: %s", exc)
        raise HTTPException(status_code=500, detail="Historical data query failed") from exc

    payload["cached"] = False
    if r is not None:
        ttl_sec = 300 if e_date >= today_lon else 3600
        try:
            await r.set(cache_key, json.dumps(payload, separators=(",", ":")), ex=ttl_sec)
        except Exception as exc:
            logger.debug("Non-fatal Redis history cache write warning: %s", exc)

    return payload


@app.get("/api/storage/summary")
async def storage_summary() -> dict[str, Any]:
    """Return persistent MSSQL database health, table row counts, and time span."""
    return await asyncio.to_thread(get_storage_summary)


@app.get("/api/storage/snapshots")
async def storage_snapshots(
    domain: str | None = Query(default=None, pattern="^(traffic|weather|air_quality|energy)$"),
    district_id: str = Query(default="all"),
    start_date: str | None = Query(default=None, description="YYYY-MM-DD"),
    end_date: str | None = Query(default=None, description="YYYY-MM-DD"),
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict[str, Any]:
    """Query permanently stored city or district measurements from MSSQL."""
    try:
        s_date = date_type.fromisoformat(start_date) if start_date else None
        e_date = date_type.fromisoformat(end_date) if end_date else None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {exc}") from exc

    rows = await asyncio.to_thread(
        query_saved_snapshots,
        domain=domain,
        district_id=district_id,
        start_date=s_date,
        end_date=e_date,
        limit=limit,
    )
    return {"count": len(rows), "district_id": district_id, "domain": domain, "items": rows}


@app.get("/api/storage/alerts")
async def storage_alerts(
    domain: str | None = Query(default=None, pattern="^(traffic|weather|air_quality|energy)$"),
    district_id: str | None = Query(default=None),
    severity: str | None = Query(default=None, pattern="^(warning|critical)$"),
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    """Query permanently stored anomaly alerts from MSSQL."""
    rows = await asyncio.to_thread(
        query_saved_alerts,
        domain=domain,
        district_id=district_id,
        severity=severity,
        limit=limit,
    )
    return {"count": len(rows), "items": rows}


@app.get("/api/storage/backfill/status")
async def storage_backfill_status() -> dict[str, Any]:
    """Return MSSQL historical API backfill configuration and last run stats."""
    return get_backfill_status()


@app.post("/api/storage/backfill")
async def trigger_storage_backfill(req: BackfillRequest | None = None) -> dict[str, Any]:
    """Backfill missing past hours from official historical Open APIs into MSSQL."""
    body = req or BackfillRequest()
    try:
        result = await sync_missing_to_mssql(
            metadata_path=METADATA_PATH,
            start_date=body.start_date,
            end_date=body.end_date,
            hours=body.hours,
            source_mode=body.source_mode,
            include_districts=body.include_districts,
            allow_traffic_model=body.allow_traffic_model,
            trigger="manual",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Manual MSSQL backfill failed: %s", exc)
        raise HTTPException(status_code=500, detail=f"MSSQL backfill failed: {exc}") from exc

    if result.get("total_rows_inserted", 0) > 0:
        result["cache_keys_cleared"] = await _clear_history_cache()
    else:
        result["cache_keys_cleared"] = 0
    return result


@app.websocket("/ws/live")
async def websocket_live(ws: WebSocket) -> None:
    accepted = await manager.connect(ws)
    if not accepted:
        return
    try:
        if redis_client:
            try:
                latest = await redis_client.get(REDIS_LATEST_KEY)
                if latest:
                    await asyncio.wait_for(ws.send_text(latest), timeout=WS_SEND_TIMEOUT_SEC)
            except Exception:
                pass
        while True:
            try:
                await asyncio.wait_for(ws.receive_text(), timeout=30.0)
            except asyncio.TimeoutError:
                await asyncio.wait_for(
                    ws.send_text(json.dumps({"type": "heartbeat", "ts": time.time()})),
                    timeout=WS_SEND_TIMEOUT_SEC,
                )
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)
