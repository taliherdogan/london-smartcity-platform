import React, { memo, useMemo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import { ArrowRightLeft, Database, Gauge, HardDrive, RefreshCw } from 'lucide-react'

const X_TICK = { fontSize: 12, fill: '#475569' }
const Y_TICK = { fontSize: 11, fill: '#94a3b8' }
const LEGEND_STYLE = { fontSize: 12 }
const BAR_RADIUS = [6, 6, 0, 0]

const ARCH_COMPARISON = [
  {
    dim: 'Karar Gecikmesi (Latency)',
    streaming: 'Saniye / milisaniye düzeyinde (anlık anomali alarmı)',
    batch: 'Dakika – saat düzeyinde (periyodik toplu işleme)',
  },
  {
    dim: 'Veri Akış Modeli',
    streaming: 'Sürekli sınırsız akış (Kafka Topics + Spark Micro-Batch)',
    batch: 'Sınırlı arşiv veri kümeleri (Parquet / CSV Data Lake)',
  },
  {
    dim: 'Durum Yönetimi (State)',
    streaming: 'Kayan pencere (Sliding Window) ve Z-Score belleği',
    batch: 'Durumsuz tam tarama veya bölümlenmiş (partitioned) sorgular',
  },
  {
    dim: 'Depolama & Format Etkisi',
    streaming: 'Hızlı yol için bellek içi kuyruk (Kafka + Redis)',
    batch: 'Sütunsal Parquet sıkıştırma ve column pruning ile yüksek hız',
  },
  {
    dim: 'Akıllı Şehir Kullanım Alanı',
    streaming: 'Anlık kaza/sıkışıklık, hava kirliliği ve şebeke piki uyarısı',
    batch: 'Aylık trafik planlaması, mevsimsel enerji tahmini, raporlama',
  },
]

function BenchmarkView({ benchmarks, onRefresh }) {
  const summary = benchmarks?.benchmark_summary
  const formatExp = summary?.format_experiments || []
  const streamExp =
    summary?.streaming_experiments?.length > 0
      ? summary.streaming_experiments
      : benchmarks?.live_streaming_telemetry || []

  const scaleChartData = useMemo(
    () =>
      formatExp.map((e) => ({
        label: e.scale_label,
        csv_sec: e.csv?.skipped ? null : (e.csv?.query_time_sec ?? null),
        parquet_sec: e.parquet?.query_time_sec ?? 0,
        csv_mb: e.csv?.skipped ? null : (e.csv?.disk_size_mb ?? null),
        parquet_mb: e.parquet?.disk_size_mb ?? 0,
      })),
    [formatExp]
  )

  const scaleSummaryText = useMemo(() => {
    if (formatExp.length === 0) return '100K / 1M / 5M+ kayıt'
    return `${formatExp.length} farklı veri büyüklüğünde (${formatExp.map((e) => e.scale_label.replace(' Kayıt', '')).join(' / ')} kayıt)`
  }, [formatExp])

  const rateSummaryText = useMemo(() => {
    if (streamExp.length === 0) return '100 / 500 / 2000 / 5000 ev/sn'
    return `${streamExp.length} farklı event hızında (${streamExp.map((s) => s.target_rate_eps).join(' / ')} ev/sn)`
  }, [streamExp])

  const streamChartData = useMemo(
    () =>
      streamExp.map((s) => ({
        label: `${s.target_rate_eps} ev/sn`,
        throughput: s.avg_throughput_eps,
        p50: s.avg_p50_latency_ms,
        p95: s.avg_p95_latency_ms,
      })),
    [streamExp]
  )

  return (
    <div className="space-y-6">
      {/* Top Info Banner */}
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-blue-200/80 bg-gradient-to-r from-blue-50 to-cyan-50 p-5">
        <div>
          <span className="text-xs font-bold uppercase tracking-wider text-blue-600">
            Araştırma Sorusu &amp; Nicel Deneyler
          </span>
          <h2 className="mt-1 text-lg font-extrabold text-slate-900">
            Gerçek Zamanlı Karar Verme vs Batch Analytics &amp; CSV vs Parquet
          </h2>
          <p className="mt-0.5 text-xs text-slate-600">
            {scaleSummaryText} format karşılaştırması ve {rateSummaryText} akış performansı.
          </p>
        </div>
        <button
          onClick={onRefresh}
          className="inline-flex items-center gap-1.5 rounded-xl border border-blue-200 bg-white px-3.5 py-2 text-xs font-bold text-blue-700 shadow-sm transition-colors hover:bg-blue-50"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Sonuçları Yenile
        </button>
      </div>

      {/* Experiment 1: CSV vs Parquet across 100K, 1M, 5M */}
      {scaleChartData.length === 0 ? (
        <div className="rounded-2xl border border-dashed border-slate-300 bg-white p-8 text-center">
          <Database className="mx-auto h-8 w-8 text-slate-400" />
          <p className="mt-2 text-sm font-bold text-slate-700">
            100K / 1M / 5M Batch Benchmark Sonucu Henüz Üretilmedi
          </p>
          <p className="mt-1 font-mono text-xs text-slate-500">
            docker compose --profile benchmark run --rm benchmark
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <div className="perf-card rounded-2xl border border-slate-200/90 bg-white p-5 shadow-sm">
            <div className="mb-3 flex items-center gap-2">
              <Database className="h-4 w-4 text-blue-600" />
              <h3 className="text-sm font-bold text-slate-800">
                PySpark Sorgu + Aggregation Süresi (CSV vs Parquet)
              </h3>
            </div>
            <div className="h-60 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={scaleChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                  <XAxis dataKey="label" tick={X_TICK} axisLine={false} tickLine={false} />
                  <YAxis tick={Y_TICK} axisLine={false} tickLine={false} unit="s" />
                  <Tooltip isAnimationActive={false} />
                  <Legend wrapperStyle={LEGEND_STYLE} />
                  <Bar dataKey="csv_sec" name="CSV Süresi (sn)" fill="#F59E0B" radius={BAR_RADIUS} isAnimationActive={false} />
                  <Bar dataKey="parquet_sec" name="Parquet Süresi (sn)" fill="#2563EB" radius={BAR_RADIUS} isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="perf-card rounded-2xl border border-slate-200/90 bg-white p-5 shadow-sm">
            <div className="mb-3 flex items-center gap-2">
              <HardDrive className="h-4 w-4 text-emerald-600" />
              <h3 className="text-sm font-bold text-slate-800">
                Disk Boyutu Karşılaştırması (MB — Snappy Parquet vs CSV)
              </h3>
            </div>
            <div className="h-60 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={scaleChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                  <XAxis dataKey="label" tick={X_TICK} axisLine={false} tickLine={false} />
                  <YAxis tick={Y_TICK} axisLine={false} tickLine={false} unit=" MB" />
                  <Tooltip isAnimationActive={false} />
                  <Legend wrapperStyle={LEGEND_STYLE} />
                  <Bar dataKey="csv_mb" name="CSV Disk (MB)" fill="#F43F5E" radius={BAR_RADIUS} isAnimationActive={false} />
                  <Bar dataKey="parquet_mb" name="Parquet Disk (MB)" fill="#059669" radius={BAR_RADIUS} isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      )}

      {/* Numeric Format Comparison Table */}
      {formatExp.length > 0 && (
        <div className="perf-card overflow-hidden rounded-2xl border border-slate-200/90 bg-white shadow-sm">
          <div className="border-b border-slate-100 px-5 py-3.5">
            <h3 className="text-sm font-bold text-slate-800">
              {formatExp.length} Ölçekli Batch Deney Tablosu (CSV vs Parquet)
            </h3>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-50 text-slate-500">
                <tr>
                  <th className="px-5 py-3 font-semibold">Veri Büyüklüğü</th>
                  <th className="px-5 py-3 font-semibold">CSV Disk</th>
                  <th className="px-5 py-3 font-semibold">Parquet Disk</th>
                  <th className="px-5 py-3 font-semibold">Tasarruf</th>
                  <th className="px-5 py-3 font-semibold">CSV Sorgu</th>
                  <th className="px-5 py-3 font-semibold">Parquet Sorgu</th>
                  <th className="px-5 py-3 font-semibold">Hızlanma</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 font-mono">
                {formatExp.map((row) => (
                  <tr key={row.rows} className="hover:bg-slate-50/70">
                    <td className="px-5 py-3 font-sans font-bold text-slate-900">
                      {row.scale_label}
                      {row.partitions ? (
                        <span className="ml-1.5 rounded bg-slate-100 px-1.5 py-0.5 font-mono text-[10px] font-medium text-slate-500">
                          {row.partitions}p
                        </span>
                      ) : null}
                    </td>
                    <td className="px-5 py-3 text-slate-600">
                      {row.csv?.disk_size_mb != null ? `${row.csv.disk_size_mb} MB` : 'Disk Koruması'}
                    </td>
                    <td className="px-5 py-3 font-semibold text-emerald-700">{row.parquet.disk_size_mb} MB</td>
                    <td className="px-5 py-3">
                      {row.disk_savings_pct != null ? (
                        <span className="rounded-full bg-emerald-50 px-2 py-0.5 font-bold text-emerald-700">
                          %{row.disk_savings_pct}
                        </span>
                      ) : (
                        <span className="text-slate-400">—</span>
                      )}
                    </td>
                    <td className="px-5 py-3 text-slate-600">
                      {row.csv?.query_time_sec != null ? `${row.csv.query_time_sec} sn` : 'Atlandı'}
                    </td>
                    <td className="px-5 py-3 font-semibold text-blue-700">{row.parquet.query_time_sec} sn</td>
                    <td className="px-5 py-3">
                      {row.query_speedup_x != null ? (
                        <span className="rounded-full bg-blue-50 px-2 py-0.5 font-bold text-blue-700">
                          {row.query_speedup_x}x
                        </span>
                      ) : (
                        <span className="text-slate-400">—</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Experiment 2: 3 Event Rates (100 / 500 / 2000 ev/s) */}
      {streamChartData.length > 0 && (
        <div className="perf-card rounded-2xl border border-slate-200/90 bg-white p-5 shadow-sm">
          <div className="mb-3 flex items-center gap-2">
            <Gauge className="h-4 w-4 text-cyan-600" />
            <h3 className="text-sm font-bold text-slate-800">
              Farklı Event Hızlarında Akış Performansı (Throughput &amp; End-to-End Latency)
            </h3>
          </div>
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
            <div className="h-56 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={streamChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                  <XAxis dataKey="label" tick={X_TICK} axisLine={false} tickLine={false} />
                  <YAxis tick={Y_TICK} axisLine={false} tickLine={false} />
                  <Tooltip isAnimationActive={false} />
                  <Legend wrapperStyle={LEGEND_STYLE} />
                  <Bar dataKey="throughput" name="Ölçülen Throughput (ev/sn)" fill="#0891B2" radius={BAR_RADIUS} isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            </div>
            <div className="h-56 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={streamChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                  <XAxis dataKey="label" tick={X_TICK} axisLine={false} tickLine={false} />
                  <YAxis tick={Y_TICK} axisLine={false} tickLine={false} unit=" ms" />
                  <Tooltip isAnimationActive={false} />
                  <Legend wrapperStyle={LEGEND_STYLE} />
                  <Bar dataKey="p50" name="p50 Gecikme (ms)" fill="#2563EB" radius={BAR_RADIUS} isAnimationActive={false} />
                  <Bar dataKey="p95" name="p95 Gecikme (ms)" fill="#E11D48" radius={BAR_RADIUS} isAnimationActive={false} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      )}

      {/* Architectural Comparison Table */}
      <div className="perf-card overflow-hidden rounded-2xl border border-slate-200/90 bg-white shadow-sm">
        <div className="flex items-center gap-2 border-b border-slate-100 px-5 py-3.5">
          <ArrowRightLeft className="h-4 w-4 text-blue-600" />
          <h3 className="text-sm font-bold text-slate-800">
            Mimari Karşılaştırma: Gerçek Zamanlı Streaming vs Batch Analytics
          </h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead className="bg-slate-50 text-slate-500">
              <tr>
                <th className="px-5 py-3 font-semibold">Mimari Boyut</th>
                <th className="px-5 py-3 font-semibold text-blue-700">Gerçek Zamanlı Streaming (Kafka + Spark Streaming)</th>
                <th className="px-5 py-3 font-semibold text-amber-700">Batch Analytics (Parquet / CSV Data Lake)</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {ARCH_COMPARISON.map((row) => (
                <tr key={row.dim} className="hover:bg-slate-50/60">
                  <td className="px-5 py-3 font-bold text-slate-800">{row.dim}</td>
                  <td className="px-5 py-3 text-slate-600">{row.streaming}</td>
                  <td className="px-5 py-3 text-slate-600">{row.batch}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}

export default memo(BenchmarkView)
