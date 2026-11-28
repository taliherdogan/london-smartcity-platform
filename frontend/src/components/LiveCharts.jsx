import React, { memo, useMemo } from 'react'
import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  Car,
  Minus,
  Thermometer,
  TrendingDown,
  TrendingUp,
  Wind,
  Zap,
} from 'lucide-react'
import { computeNiceYAxis, formatAxisTick, formatInt } from '../utils/format'

const CHART_MARGIN = { top: 8, right: 10, left: 0, bottom: 0 }
const AXIS_TICK = { fontSize: 10, fill: '#94a3b8', fontWeight: 500 }

const CHARTS = [
  {
    key: 'traffic',
    title: 'Ortalama Trafik Hızı',
    subtitle: 'Yüksek hız = daha akıcı trafik',
    unit: 'km/s',
    icon: Car,
    color: '#2563EB',
    gradId: 'gradLiveTraffic',
    topBar: 'bg-gradient-to-r from-blue-600 via-sky-500 to-cyan-400',
    iconBox: 'bg-blue-50 text-blue-600 ring-1 ring-blue-100',
    cursor: { stroke: '#2563EB', strokeWidth: 1, strokeDasharray: '3 3', strokeOpacity: 0.45 },
    activeDot: { r: 5, fill: '#2563EB', stroke: '#ffffff', strokeWidth: 2 },
    higherIsBetter: true,
    axisOpts: { minSpan: 2, minStep: 0.5, allowNegative: false },
    axisSuffix: '',
    formatVal: (v) => Number(v).toFixed(1),
    getLevel: (v) => {
      if (v < 14) return { label: 'Çok Yoğun', badge: 'bg-rose-100 text-rose-700' }
      if (v < 22) return { label: 'Yoğun', badge: 'bg-amber-100 text-amber-800' }
      if (v < 32) return { label: 'Orta Akış', badge: 'bg-blue-100 text-blue-700' }
      return { label: 'Akıcı', badge: 'bg-emerald-100 text-emerald-700' }
    },
  },
  {
    key: 'air_quality',
    title: 'Hava Kalitesi',
    subtitle: 'Düşük puan = daha temiz hava',
    unit: 'puan',
    icon: Wind,
    color: '#059669',
    gradId: 'gradLiveAqi',
    topBar: 'bg-gradient-to-r from-emerald-500 via-teal-400 to-cyan-400',
    iconBox: 'bg-emerald-50 text-emerald-600 ring-1 ring-emerald-100',
    cursor: { stroke: '#059669', strokeWidth: 1, strokeDasharray: '3 3', strokeOpacity: 0.45 },
    activeDot: { r: 5, fill: '#059669', stroke: '#ffffff', strokeWidth: 2 },
    higherIsBetter: false,
    axisOpts: { minSpan: 4, minStep: 1, allowNegative: false },
    axisSuffix: '',
    formatVal: (v) => Number(v).toFixed(1),
    getLevel: (v) => {
      if (v <= 20) return { label: 'İyi', badge: 'bg-emerald-100 text-emerald-700' }
      if (v <= 40) return { label: 'Makul', badge: 'bg-emerald-100 text-emerald-700' }
      if (v <= 60) return { label: 'Orta', badge: 'bg-amber-100 text-amber-800' }
      if (v <= 80) return { label: 'Kötü', badge: 'bg-orange-100 text-orange-800' }
      return { label: 'Çok Kötü', badge: 'bg-rose-100 text-rose-700' }
    },
  },
  {
    key: 'energy',
    title: 'Bölgesel Enerji Yükü',
    subtitle: 'Anlık elektrik tüketim seyri',
    unit: 'MW',
    icon: Zap,
    color: '#D97706',
    gradId: 'gradLiveEnergy',
    topBar: 'bg-gradient-to-r from-amber-500 via-orange-500 to-yellow-400',
    iconBox: 'bg-amber-50 text-amber-600 ring-1 ring-amber-100',
    cursor: { stroke: '#D97706', strokeWidth: 1, strokeDasharray: '3 3', strokeOpacity: 0.45 },
    activeDot: { r: 5, fill: '#D97706', stroke: '#ffffff', strokeWidth: 2 },
    higherIsBetter: false,
    axisOpts: { minSpan: 15, minStep: 5, allowNegative: false },
    axisSuffix: '',
    formatVal: (v) => formatInt(v),
    getLevel: (v) => {
      if (v >= 195) return { label: 'Yüksek Yük', badge: 'bg-orange-100 text-orange-800' }
      if (v >= 130) return { label: 'Normal Yük', badge: 'bg-amber-100 text-amber-800' }
      return { label: 'Dengeli', badge: 'bg-emerald-100 text-emerald-700' }
    },
  },
  {
    key: 'weather',
    title: 'Sıcaklık Akışı',
    subtitle: 'Anlık Londra sıcaklık değişimi',
    unit: '°C',
    icon: Thermometer,
    color: '#0891B2',
    gradId: 'gradLiveWeather',
    topBar: 'bg-gradient-to-r from-cyan-500 via-sky-400 to-blue-500',
    iconBox: 'bg-cyan-50 text-cyan-600 ring-1 ring-cyan-100',
    cursor: { stroke: '#0891B2', strokeWidth: 1, strokeDasharray: '3 3', strokeOpacity: 0.45 },
    activeDot: { r: 5, fill: '#0891B2', stroke: '#ffffff', strokeWidth: 2 },
    higherIsBetter: null,
    axisOpts: { minSpan: 1.5, minStep: 0.5, allowNegative: true },
    axisSuffix: '°',
    formatVal: (v) => Number(v).toFixed(1),
    getLevel: (v) => {
      if (v <= 4) return { label: 'Soğuk', badge: 'bg-blue-100 text-blue-700' }
      if (v <= 13) return { label: 'Serin', badge: 'bg-cyan-100 text-cyan-800' }
      if (v <= 23) return { label: 'Ilık', badge: 'bg-emerald-100 text-emerald-700' }
      return { label: 'Sıcak', badge: 'bg-orange-100 text-orange-800' }
    },
  },
]

function ChartTooltip({ active, payload, label, cfg, avg }) {
  if (!active || !payload?.length) return null
  const val = Number(payload[0].value)
  if (!Number.isFinite(val)) return null
  const lvl = cfg.getLevel(val)
  const diff = avg != null ? val - avg : 0

  return (
    <div className="min-w-[170px] rounded-xl border border-slate-200/90 bg-white p-3 text-xs shadow-lg">
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-[11px] font-semibold text-slate-400">{label}</span>
        <span className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${lvl.badge}`}>
          {lvl.label}
        </span>
      </div>
      <div className="mt-1.5 flex items-baseline gap-1">
        <span className="font-mono text-lg font-extrabold text-slate-900 tabular-nums">
          {cfg.formatVal(val)}
        </span>
        <span className="text-[11px] font-semibold text-slate-400">{cfg.unit}</span>
      </div>
      {avg != null && Math.abs(diff) >= 0.05 && (
        <div className="mt-1 border-t border-slate-100 pt-1 text-[10px] font-medium text-slate-500">
          Ortalamadan{' '}
          <strong className="font-mono text-slate-700">
            {diff > 0 ? '+' : ''}
            {cfg.key === 'energy' ? Math.round(diff) : diff.toFixed(1)} {cfg.unit}
          </strong>
        </div>
      )}
    </div>
  )
}

const LiveChartCard = memo(function LiveChartCard({ cfg, timeseries, st, daySummary, isLive }) {
  const Icon = cfg.icon

  const hasDayMin = daySummary?.min != null && Number.isFinite(Number(daySummary.min))
  const hasDayMax = daySummary?.max != null && Number.isFinite(Number(daySummary.max))
  const hasDayAvg = daySummary?.avg != null && Number.isFinite(Number(daySummary.avg))
  const footerMin = st ? (hasDayMin ? Math.min(Number(daySummary.min), st.min) : st.min) : null
  const footerMax = st ? (hasDayMax ? Math.max(Number(daySummary.max), st.max) : st.max) : null
  const footerAvg = st ? (hasDayAvg ? Number(daySummary.avg) : st.avg) : null

  let trendTone = 'bg-slate-100 text-slate-500'
  let TrendIcon = Minus
  let trendLabel = 'Stabil'

  if (st) {
    const threshold = cfg.key === 'energy' ? 3 : cfg.key === 'weather' ? 0.05 : 0.2
    if (st.delta > threshold) {
      TrendIcon = TrendingUp
      trendLabel = `+${cfg.key === 'energy' ? Math.round(st.delta) : st.delta.toFixed(1)}`
      if (cfg.higherIsBetter === true) trendTone = 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/70'
      else if (cfg.higherIsBetter === false) trendTone = 'bg-amber-50 text-amber-800 ring-1 ring-amber-200/70'
      else trendTone = 'bg-orange-50 text-orange-700 ring-1 ring-orange-200/70'
    } else if (st.delta < -threshold) {
      TrendIcon = TrendingDown
      trendLabel = `${cfg.key === 'energy' ? Math.round(st.delta) : st.delta.toFixed(1)}`
      if (cfg.higherIsBetter === true) trendTone = 'bg-amber-50 text-amber-800 ring-1 ring-amber-200/70'
      else if (cfg.higherIsBetter === false) trendTone = 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/70'
      else trendTone = 'bg-cyan-50 text-cyan-700 ring-1 ring-cyan-200/70'
    }
  }

  return (
    <div className="perf-card relative flex flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white p-4 shadow-sm transition-shadow duration-200 hover:shadow-md">
      {/* Top Gradient Accent Bar */}
      <div className={`absolute inset-x-0 top-0 h-1 ${cfg.topBar}`} />

      {/* Header */}
      <div className="mb-3 flex items-start justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2.5">
          <span
            className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-xl ${cfg.iconBox}`}
          >
            <Icon className="h-4 w-4" />
          </span>
          <div className="min-w-0">
            <h3 className="truncate text-sm font-extrabold text-slate-900">{cfg.title}</h3>
            <p className="truncate text-[11px] font-medium text-slate-400">{cfg.subtitle}</p>
          </div>
        </div>

        {st && (
          <div className="shrink-0 text-right">
            <div className="flex items-baseline justify-end gap-1">
              <span className="font-mono text-base font-extrabold text-slate-900 tabular-nums">
                {cfg.formatVal(st.latest)}
              </span>
              <span className="text-[10px] font-bold text-slate-400">{cfg.unit}</span>
            </div>
            <span
              className={`mt-0.5 inline-flex items-center gap-0.5 rounded-full px-1.5 py-0.5 font-mono text-[10px] font-bold tabular-nums ${trendTone}`}
              title="Grafik başlangıcına göre değişim"
            >
              <TrendIcon className="h-2.5 w-2.5" />
              {trendLabel}
            </span>
          </div>
        )}
      </div>

      {/* Live Area Chart */}
      <div className="h-44 w-full">
        {timeseries.length < 2 || !st ? (
          <div className="flex h-full items-center justify-center rounded-xl bg-slate-50/70 text-xs font-medium text-slate-400">
            Canlı akış verisi bekleniyor...
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={timeseries} margin={CHART_MARGIN}>
              <defs>
                <linearGradient id={cfg.gradId} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={cfg.color} stopOpacity={0.3} />
                  <stop offset="85%" stopColor={cfg.color} stopOpacity={0.04} />
                  <stop offset="100%" stopColor={cfg.color} stopOpacity={0.0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
              <XAxis
                dataKey="time"
                tick={AXIS_TICK}
                tickLine={false}
                axisLine={false}
                minTickGap={30}
              />
              <YAxis
                width={40}
                tick={AXIS_TICK}
                tickLine={false}
                axisLine={false}
                ticks={st.ticks}
                tickFormatter={(v) => formatAxisTick(v, st.decimals, cfg.axisSuffix)}
                domain={st.domain}
              />
              <Tooltip
                isAnimationActive={false}
                content={<ChartTooltip cfg={cfg} avg={st.avg} />}
                cursor={cfg.cursor}
              />
              <ReferenceLine
                y={st.avg}
                stroke={cfg.color}
                strokeOpacity={0.35}
                strokeDasharray="4 4"
                strokeWidth={1.2}
              />
              <Area
                type="monotone"
                dataKey={cfg.key}
                stroke={cfg.color}
                strokeWidth={2.5}
                fill={`url(#${cfg.gradId})`}
                activeDot={cfg.activeDot}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Daily Min / Avg / Max Footer Strip */}
      {st && (
        <div className="mt-2.5 flex items-center justify-between gap-2 border-t border-slate-100 pt-2.5 text-[11px]">
          <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1 text-slate-500">
            <span
              className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-bold text-slate-500"
              title="Bugünün (Londra saati) en düşük, ortalama ve en yüksek değerleri"
            >
              Bugün
            </span>
            <span
              title={
                daySummary?.min_time
                  ? `Bugün en düşük değer (${daySummary.min_time})`
                  : 'Bugün en düşük değer'
              }
            >
              En Düşük:{' '}
              <strong className="font-mono font-bold text-slate-700 tabular-nums">
                {cfg.formatVal(footerMin)}
              </strong>
            </span>
            <span title="Bugün günlük ortalama">
              Ort:{' '}
              <strong className="font-mono font-bold text-slate-700 tabular-nums">
                {cfg.formatVal(footerAvg)}
              </strong>
            </span>
            <span
              title={
                daySummary?.max_time
                  ? `Bugün en yüksek değer (${daySummary.max_time})`
                  : 'Bugün en yüksek değer'
              }
            >
              En Yüksek:{' '}
              <strong className="font-mono font-bold text-slate-700 tabular-nums">
                {cfg.formatVal(footerMax)}
              </strong>
            </span>
          </div>

          {isLive && (
            <span className="inline-flex shrink-0 items-center gap-1.5 text-[10px] font-bold text-emerald-600">
              <span className="inline-flex h-2 w-2 rounded-full bg-emerald-500" />
              Canlı
            </span>
          )}
        </div>
      )}
    </div>
  )
})

function LiveCharts({ timeseries = [], dailySummary = null, isLive = true }) {
  const statsByKey = useMemo(() => {
    const out = {}
    for (const cfg of CHARTS) {
      let min = Infinity
      let max = -Infinity
      let sum = 0
      let count = 0
      let first = 0
      let latest = 0

      for (let i = 0; i < timeseries.length; i += 1) {
        const n = Number(timeseries[i]?.[cfg.key])
        if (!Number.isFinite(n)) continue
        if (count === 0) first = n
        latest = n
        if (n < min) min = n
        if (n > max) max = n
        sum += n
        count += 1
      }

      if (count === 0) {
        out[cfg.key] = null
        continue
      }

      const avg = sum / count
      const delta = latest - first
      const yAxis = computeNiceYAxis(min, max, cfg.axisOpts)

      out[cfg.key] = {
        min,
        max,
        avg,
        latest,
        delta,
        domain: yAxis.domain,
        ticks: yAxis.ticks,
        decimals: yAxis.decimals,
      }
    }
    return out
  }, [timeseries])

  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
      {CHARTS.map((cfg) => (
        <LiveChartCard
          key={cfg.key}
          cfg={cfg}
          timeseries={timeseries}
          st={statsByKey[cfg.key]}
          daySummary={dailySummary?.[cfg.key]}
          isLive={isLive}
        />
      ))}
    </div>
  )
}

export default memo(LiveCharts)
