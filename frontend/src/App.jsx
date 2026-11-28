import React, { memo, useState } from 'react'
import { Activity, Clock, Cpu, Layers, WifiOff } from 'lucide-react'
import HeaderBar from './components/HeaderBar'
import KpiCards from './components/KpiCards'
import LiveCharts from './components/LiveCharts'
import AnomalyFeed from './components/AnomalyFeed'
import DistrictView from './components/DistrictView'
import HistoryView from './components/HistoryView'
import BenchmarkView from './components/BenchmarkView'
import { useLiveStream } from './hooks/useLiveStream'
import { formatInt } from './utils/format'

const TelemetryStrip = memo(function TelemetryStrip({ perf }) {
  return (
    <section className="perf-card grid grid-cols-2 gap-3 rounded-2xl border border-slate-200/90 bg-white p-4 shadow-sm sm:grid-cols-4">
      <div className="flex items-center gap-3">
        <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-blue-50 text-blue-600">
          <Activity className="h-4 w-4" />
        </span>
        <div>
          <div className="text-[11px] font-semibold text-slate-400">Anlık Veri Akışı</div>
          <div className="font-mono text-base font-extrabold text-slate-900 tabular-nums">
            {perf.throughput_eps ?? '—'} <span className="text-xs font-medium text-slate-400">kayıt/sn</span>
          </div>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-emerald-50 text-emerald-600">
          <Clock className="h-4 w-4" />
        </span>
        <div>
          <div className="text-[11px] font-semibold text-slate-400">Ortalama Yanıt Süresi</div>
          <div className="font-mono text-base font-extrabold text-slate-900 tabular-nums">
            {perf.p50_latency_ms ?? '—'}{' '}
            <span className="text-xs font-medium text-slate-400">ms</span>
          </div>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-amber-50 text-amber-600">
          <Cpu className="h-4 w-4" />
        </span>
        <div>
          <div className="flex items-center gap-1.5 text-[11px] font-semibold text-slate-400">
            <span>Veri İşleme Süresi</span>
            {perf.backpressure_status && (
              <span
                className={`rounded-full px-1.5 py-0.2 text-[9px] font-bold ${
                  perf.backpressure_status === 'healthy'
                    ? 'bg-emerald-50 text-emerald-700'
                    : perf.backpressure_status === 'elevated'
                      ? 'bg-amber-50 text-amber-700'
                      : 'bg-rose-50 text-rose-700'
                }`}
                title={
                  perf.driver_rss_mb
                    ? `Spark RSS: ${perf.driver_rss_mb} MB | Data Lake: ${perf.lake_size_mb ?? 0} MB`
                    : undefined
                }
              >
                {perf.backpressure_status === 'healthy'
                  ? 'Stabil'
                  : perf.backpressure_status === 'elevated'
                    ? 'Yoğun'
                    : 'Backpressure'}
              </span>
            )}
          </div>
          <div className="font-mono text-base font-extrabold text-slate-900 tabular-nums">
            {perf.spark_batch_ms ?? '—'} <span className="text-xs font-medium text-slate-400">ms</span>
          </div>
        </div>
      </div>

      <div className="flex items-center gap-3">
        <span className="flex h-9 w-9 items-center justify-center rounded-xl bg-cyan-50 text-cyan-600">
          <Layers className="h-4 w-4" />
        </span>
        <div>
          <div className="text-[11px] font-semibold text-slate-400">Toplam İşlenen Kayıt</div>
          <div className="font-mono text-base font-extrabold text-slate-900 tabular-nums">
            {perf.cumulative_events ? formatInt(perf.cumulative_events) : '—'}
          </div>
        </div>
      </div>
    </section>
  )
})

const EMPTY_PERF = {}

export default function App() {
  const [activeTab, setActiveTab] = useState('live')
  const {
    liveState,
    sources,
    targetRate,
    benchmarks,
    todaySummary,
    connected,
    isOffline,
    browserOffline,
    controlBusy,
    changeRate,
    refreshBenchmarks,
  } = useLiveStream(activeTab)

  const perf = liveState?.performance || EMPTY_PERF

  return (
    <div className="min-h-screen pb-10">
      <HeaderBar
        activeTab={activeTab}
        onTabChange={setActiveTab}
        connected={connected}
        isOffline={isOffline}
        targetRate={targetRate}
        onRateChange={changeRate}
        controlBusy={controlBusy}
      />

      <main className="mx-auto max-w-7xl space-y-5 px-4 pt-5 sm:px-6">
        {isOffline && (
          <div
            role="alert"
            className="flex items-start gap-3 rounded-2xl border border-rose-200 bg-gradient-to-r from-rose-50 via-amber-50/70 to-rose-50 px-4 py-3 text-rose-900 shadow-sm"
          >
            <span className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-xl bg-rose-100 text-rose-600 ring-1 ring-rose-200">
              <WifiOff className="h-4 w-4" />
            </span>
            <div className="min-w-0 text-xs leading-relaxed">
              <div className="font-extrabold text-rose-900">
                İnternet Bağlantısı Kesildi
              </div>
              <p className="mt-0.5 font-medium text-rose-800/90">
                {browserOffline
                  ? 'İnternet bağlantınız yok gibi görünüyor. Bağlantı geri geldiğinde canlı veriler otomatik olarak yenilenecektir.'
                  : 'İnternet bağlantısı olmadığı için anlık veriler güncellenemiyor. Şu an en son kaydedilen veriler gösteriliyor.'}
              </p>
            </div>
          </div>
        )}

        {activeTab === 'live' && (
          <>
            {/* 4 Clean Domain KPI Cards */}
            <KpiCards domains={liveState?.domains} sources={sources} isOffline={isOffline} />

            {/* Live Charts (Left) + Hybrid Anomaly Feed (Right) */}
            <section className="grid grid-cols-1 gap-4 lg:grid-cols-12">
              <div className="lg:col-span-8">
                <LiveCharts
                  timeseries={liveState?.timeseries}
                  dailySummary={todaySummary}
                  isLive={connected && !isOffline}
                />
              </div>
              <div className="lg:col-span-4">
                <AnomalyFeed
                  alerts={liveState?.alerts}
                  cumulativeAlerts={perf.cumulative_alerts}
                  isOffline={isOffline}
                />
              </div>
            </section>

            {/* Bottom Live Streaming Telemetry Strip */}
            <TelemetryStrip perf={perf} />
          </>
        )}

        {activeTab === 'history' && <HistoryView />}

        {activeTab === 'districts' && (
          <DistrictView districts={liveState?.districts} sources={sources} />
        )}

        {activeTab === 'benchmarks' && (
          <BenchmarkView benchmarks={benchmarks} onRefresh={refreshBenchmarks} />
        )}
      </main>
    </div>
  )
}
