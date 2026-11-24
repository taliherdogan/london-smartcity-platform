import React, { memo, useState } from 'react'
import {
  ChevronDown,
  ChevronUp,
  Cpu,
  Database,
  HardDrive,
  Radio,
  Server,
  Wifi,
} from 'lucide-react'
import { formatInt } from '../utils/format'

function statusBadge(status) {
  if (status === 'backpressure') {
    return {
      label: 'Backpressure',
      cls: 'bg-rose-50 text-rose-700 ring-1 ring-rose-200/80',
      dot: 'bg-rose-500',
    }
  }
  if (status === 'elevated') {
    return {
      label: 'Yoğun',
      cls: 'bg-amber-50 text-amber-800 ring-1 ring-amber-200/80',
      dot: 'bg-amber-500',
    }
  }
  return {
    label: 'Sağlıklı',
    cls: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/80',
    dot: 'bg-emerald-500',
  }
}

function SystemHealthPanel({ systemHealth }) {
  const [expanded, setExpanded] = useState(false)
  if (!systemHealth) return null

  const badge = statusBadge(systemHealth.status)
  const redis = systemHealth.redis || {}
  const spark = systemHealth.spark || null
  const gen = systemHealth.generator || null
  const mssql = systemHealth.mssql || null
  const backend = systemHealth.backend || {}

  return (
    <section className="perf-card overflow-hidden rounded-2xl border border-slate-200/90 bg-white shadow-sm">
      <button
        type="button"
        onClick={() => setExpanded((v) => !v)}
        className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left transition-colors hover:bg-slate-50/70"
      >
        <div className="flex flex-wrap items-center gap-2.5">
          <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-slate-100 text-slate-600 ring-1 ring-slate-200/80">
            <Server className="h-4 w-4" />
          </span>
          <div>
            <div className="flex items-center gap-2">
              <span className="text-xs font-extrabold text-slate-900">
                Altyapı &amp; Sistem Sağlığı
              </span>
              <span
                className={`inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[10px] font-bold ${badge.cls}`}
              >
                <span className={`h-1.5 w-1.5 rounded-full ${badge.dot}`} />
                {badge.label}
              </span>
            </div>
            <p className="text-[11px] font-medium text-slate-400">
              Redis {redis.used_memory_mb ?? '—'} MB
              {mssql?.connected ? ` • MSSQL ${formatInt(mssql.total_rows ?? 0)} kayıt` : ''}
              {spark ? ` • Spark RSS ${spark.driver_rss_mb ?? '—'} MB • Lake ${spark.lake_size_mb ?? 0} MB` : ''}
              {backend.active_ws_clients != null ? ` • ${backend.active_ws_clients} WS istemci` : ''}
            </p>
          </div>
        </div>

        <span className="inline-flex shrink-0 items-center gap-1 rounded-lg bg-slate-100 px-2.5 py-1 text-[11px] font-bold text-slate-600">
          {expanded ? 'Gizle' : 'Detaylar'}
          {expanded ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
        </span>
      </button>

      {expanded && (
        <div className="grid grid-cols-1 gap-3 border-t border-slate-100 bg-slate-50/50 p-4 sm:grid-cols-2 lg:grid-cols-5">
          {/* Redis */}
          <div className="rounded-xl border border-slate-200/80 bg-white p-3">
            <div className="flex items-center gap-2 text-xs font-bold text-slate-700">
              <Database className="h-3.5 w-3.5 text-rose-500" />
              Redis 7 Bellek
            </div>
            <div className="mt-2 font-mono text-sm font-extrabold text-slate-900 tabular-nums">
              {redis.used_memory_mb ?? '—'}{' '}
              <span className="text-[11px] font-medium text-slate-400">
                / {redis.max_memory_mb ?? '—'} MB
              </span>
            </div>
            <div className="mt-1.5 h-1.5 w-full overflow-hidden rounded-full bg-slate-100">
              <div
                className="h-full rounded-full bg-gradient-to-r from-emerald-500 to-cyan-500"
                style={{ width: `${Math.min(100, redis.usage_pct || 0)}%` }}
              />
            </div>
            <div className="mt-1 text-[10px] font-medium text-slate-400">
              Kullanım: %{redis.usage_pct ?? 0}
            </div>
          </div>

          {/* Spark */}
          <div className="rounded-xl border border-slate-200/80 bg-white p-3">
            <div className="flex items-center justify-between gap-2 text-xs font-bold text-slate-700">
              <span className="inline-flex items-center gap-1.5">
                <Cpu className="h-3.5 w-3.5 text-amber-500" />
                Spark Streaming
              </span>
              {spark?.status && (
                <span className={`rounded-full px-1.5 py-0.5 text-[9px] font-bold ${statusBadge(spark.status).cls}`}>
                  {statusBadge(spark.status).label}
                </span>
              )}
            </div>
            <div className="mt-2 font-mono text-sm font-extrabold text-slate-900 tabular-nums">
              {spark?.driver_rss_mb ?? '—'}{' '}
              <span className="text-[11px] font-medium text-slate-400">MB RSS</span>
            </div>
            <div className="mt-1.5 flex items-center justify-between text-[10px] font-medium text-slate-500">
              <span className="inline-flex items-center gap-1">
                <HardDrive className="h-3 w-3 text-slate-400" />
                Lake: {spark?.lake_size_mb ?? 0}/{spark?.lake_max_size_mb ?? 512} MB
              </span>
              <span className="font-mono">Batch #{spark?.batch_id ?? '—'}</span>
            </div>
          </div>

          {/* Generator */}
          <div className="rounded-xl border border-slate-200/80 bg-white p-3">
            <div className="flex items-center justify-between gap-2 text-xs font-bold text-slate-700">
              <span className="inline-flex items-center gap-1.5">
                <Radio className="h-3.5 w-3.5 text-blue-500" />
                Kafka Generator
              </span>
              {gen?.status && (
                <span className={`rounded-full px-1.5 py-0.5 text-[9px] font-bold ${statusBadge(gen.status).cls}`}>
                  {statusBadge(gen.status).label}
                </span>
              )}
            </div>
            <div className="mt-2 font-mono text-sm font-extrabold text-slate-900 tabular-nums">
              {gen?.actual_rate_eps ?? '—'}{' '}
              <span className="text-[11px] font-medium text-slate-400">
                / {gen?.target_rate_eps ?? '—'} ev/sn
              </span>
            </div>
            <div className="mt-1.5 flex items-center justify-between text-[10px] font-medium text-slate-500">
              <span>Kuyruk: {formatInt(gen?.queue_len ?? 0)}</span>
              <span>Drop: {formatInt(gen?.dropped_events ?? 0)}</span>
            </div>
          </div>

          {/* MSSQL */}
          <div className="rounded-xl border border-slate-200/80 bg-white p-3">
            <div className="flex items-center justify-between gap-2 text-xs font-bold text-slate-700">
              <span className="inline-flex items-center gap-1.5">
                <HardDrive className="h-3.5 w-3.5 text-indigo-500" />
                MSSQL Arşiv
              </span>
              <span
                className={`rounded-full px-1.5 py-0.5 text-[9px] font-bold ${
                  mssql?.connected
                    ? 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/80'
                    : 'bg-slate-100 text-slate-500'
                }`}
              >
                {mssql?.connected ? 'Kalıcı Aktif' : 'Kapalı'}
              </span>
            </div>
            <div className="mt-2 font-mono text-sm font-extrabold text-slate-900 tabular-nums">
              {mssql?.connected ? formatInt(mssql.total_rows ?? 0) : '—'}{' '}
              <span className="text-[11px] font-medium text-slate-400">satır</span>
            </div>
            <div className="mt-1.5 flex items-center justify-between text-[10px] font-medium text-slate-500">
              <span>{mssql?.database ?? 'SmartCityDB'}</span>
              <span className="font-mono">{mssql?.db_size_mb ?? 0} MB</span>
            </div>
          </div>

          {/* Backend WS */}
          <div className="rounded-xl border border-slate-200/80 bg-white p-3">
            <div className="flex items-center gap-2 text-xs font-bold text-slate-700">
              <Wifi className="h-3.5 w-3.5 text-emerald-500" />
              FastAPI WebSocket
            </div>
            <div className="mt-2 font-mono text-sm font-extrabold text-slate-900 tabular-nums">
              {backend.active_ws_clients ?? 0}{' '}
              <span className="text-[11px] font-medium text-slate-400">
                / {backend.max_ws_clients ?? 200} istemci
              </span>
            </div>
            <div className="mt-1.5 flex items-center justify-between text-[10px] font-medium text-slate-500">
              <span>Birleştirilen: {formatInt(backend.coalesced_frames ?? 0)}</span>
              <span>Yavaş düşen: {formatInt(backend.dropped_slow_clients ?? 0)}</span>
            </div>
          </div>
        </div>
      )}
    </section>
  )
}

export default memo(SystemHealthPanel)
