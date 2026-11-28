import React, { memo, startTransition, useCallback, useEffect, useMemo, useRef, useState } from 'react'
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
  AlertCircle,
  Calendar,
  Car,
  ChevronDown,
  ChevronUp,
  CloudRain,
  CloudSun,
  Database,
  Droplets,
  Gauge,
  Globe,
  History,
  Leaf,
  MapPin,
  RefreshCw,
  Table2,
  Thermometer,
  Wind,
  Zap,
} from 'lucide-react'
import { computeNiceYAxis, formatAxisTick, formatInt } from '../utils/format'

const CHART_MARGIN = { top: 8, right: 10, left: 0, bottom: 0 }
const AXIS_TICK = { fontSize: 10, fill: '#94a3b8', fontWeight: 500 }
const EMPTY_OBJ = {}
const EMPTY_ARR = []
const MAX_CHART_POINTS = 168
const INITIAL_TABLE_ROWS = 72
const CLIENT_CACHE_TTL_MS = 5 * 60 * 1000
const CLIENT_CACHE_MAX = 40

// Module-level caches so revisiting presets/districts or switching tabs is instant
const historyClientCache = new Map()
let cachedDistrictOptions = null

function getCachedHistory(key) {
  const entry = historyClientCache.get(key)
  if (!entry) return null
  if (Date.now() > entry.expiresAt) {
    historyClientCache.delete(key)
    return null
  }
  // Refresh LRU recency
  historyClientCache.delete(key)
  historyClientCache.set(key, entry)
  return entry.data
}

function setCachedHistory(key, data) {
  if (historyClientCache.has(key)) historyClientCache.delete(key)
  historyClientCache.set(key, { data, expiresAt: Date.now() + CLIENT_CACHE_TTL_MS })
  if (historyClientCache.size > CLIENT_CACHE_MAX) {
    const oldestKey = historyClientCache.keys().next().value
    historyClientCache.delete(oldestKey)
  }
}

function downsampleSeries(series, maxPoints) {
  if (!series || series.length <= maxPoints) return series
  const step = (series.length - 1) / (maxPoints - 1)
  const out = new Array(maxPoints)
  for (let i = 0; i < maxPoints; i += 1) {
    out[i] = series[Math.round(i * step)]
  }
  return out
}

const LONDON_ISO_DATE_FMT = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Europe/London',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
})

function isoDateDaysAgo(daysAgo) {
  const londonToday = LONDON_ISO_DATE_FMT.format(new Date())
  const [y, m, d] = londonToday.split('-').map(Number)
  const dt = new Date(Date.UTC(y, m - 1, d - daysAgo))
  const yy = dt.getUTCFullYear()
  const mm = String(dt.getUTCMonth() + 1).padStart(2, '0')
  const dd = String(dt.getUTCDate()).padStart(2, '0')
  return `${yy}-${mm}-${dd}`
}

const PRESETS = [
  { id: '1d', label: 'Dün', mode: 'single', daysAgo: 1 },
  { id: '3d', label: '3 Gün Önce', mode: 'single', daysAgo: 3 },
  { id: '5d', label: '5 Gün Önce', mode: 'single', daysAgo: 5 },
  { id: '7d_day', label: '7 Gün Önce', mode: 'single', daysAgo: 7 },
  { id: 'last7', label: 'Son 7 Gün', mode: 'range', rangeDays: 7 },
  { id: 'last14', label: 'Son 14 Gün', mode: 'range', rangeDays: 14 },
  { id: 'last30', label: 'Son 30 Gün', mode: 'range', rangeDays: 30 },
]

const DOMAIN_CONFIGS = [
  {
    key: 'traffic',
    kpiTitle: 'Trafik Hızı',
    kpiSubtitle: 'Ortalama akış hızı',
    title: 'Ortalama Trafik Hızı',
    subtitle: 'Yüksek hız = daha akıcı trafik',
    unit: 'km/s',
    minLabel: 'En Yavaş',
    maxLabel: 'En Akıcı',
    icon: Car,
    color: '#2563EB',
    gradId: 'gradHistTraffic',
    topBar: 'bg-gradient-to-r from-blue-600 via-sky-500 to-cyan-400',
    cardGlow: 'bg-gradient-to-br from-blue-50/55 via-white to-white',
    iconBox: 'bg-gradient-to-br from-blue-600 to-sky-500 text-white shadow-sm shadow-blue-200',
    chartIconBox: 'bg-blue-50 text-blue-600 ring-1 ring-blue-100',
    cursor: { stroke: '#2563EB', strokeWidth: 1, strokeDasharray: '3 3', strokeOpacity: 0.45 },
    activeDot: { r: 5, fill: '#2563EB', stroke: '#ffffff', strokeWidth: 2 },
    higherIsBetter: true,
    axisOpts: { minSpan: 2, minStep: 0.5, allowNegative: false },
    axisSuffix: '',
    formatVal: (v) => Number(v).toFixed(1),
    getLevel: (v) => {
      if (v < 14) return { label: 'Çok Yoğun', badge: 'bg-rose-100 text-rose-700', bar: 'bg-rose-500' }
      if (v < 22) return { label: 'Yoğun', badge: 'bg-amber-100 text-amber-800', bar: 'bg-amber-500' }
      if (v < 32) return { label: 'Orta Akış', badge: 'bg-blue-100 text-blue-700', bar: 'bg-blue-500' }
      return { label: 'Akıcı', badge: 'bg-emerald-100 text-emerald-700', bar: 'bg-emerald-500' }
    },
    formatDelta: (delta) => {
      const abs = Math.abs(delta).toFixed(1)
      if (Math.abs(delta) < 0.3) return { text: 'Bugünle benzer hızda', tone: 'bg-slate-100 text-slate-600' }
      if (delta > 0) return { text: `Bugünden ${abs} km/s daha akıcı`, tone: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/80' }
      return { text: `Bugünden ${abs} km/s daha yavaş`, tone: 'bg-amber-50 text-amber-800 ring-1 ring-amber-200/80' }
    },
    extraTooltip: (pt) => {
      const items = []
      if (pt.congestion_pct != null) items.push(`Yoğunluk: %${Math.round(pt.congestion_pct)}`)
      if (pt.precipitation_mm > 0.1) items.push(`Yağış: ${pt.precipitation_mm} mm`)
      return items
    },
  },
  {
    key: 'weather',
    kpiTitle: 'Sıcaklık',
    kpiSubtitle: 'Ortalama hava sıcaklığı',
    title: 'Sıcaklık Değişimi',
    subtitle: 'Seçili dönem Londra sıcaklık seyri',
    unit: '°C',
    minLabel: 'En Düşük',
    maxLabel: 'En Yüksek',
    icon: Thermometer,
    kpiIcon: CloudSun,
    color: '#0891B2',
    gradId: 'gradHistWeather',
    topBar: 'bg-gradient-to-r from-cyan-500 via-sky-400 to-blue-500',
    cardGlow: 'bg-gradient-to-br from-cyan-50/55 via-white to-white',
    iconBox: 'bg-gradient-to-br from-cyan-500 to-sky-500 text-white shadow-sm shadow-cyan-200',
    chartIconBox: 'bg-cyan-50 text-cyan-600 ring-1 ring-cyan-100',
    cursor: { stroke: '#0891B2', strokeWidth: 1, strokeDasharray: '3 3', strokeOpacity: 0.45 },
    activeDot: { r: 5, fill: '#0891B2', stroke: '#ffffff', strokeWidth: 2 },
    higherIsBetter: null,
    axisOpts: { minSpan: 1.5, minStep: 0.5, allowNegative: true },
    axisSuffix: '°',
    formatVal: (v) => Number(v).toFixed(1),
    getLevel: (v) => {
      if (v <= 4) return { label: 'Soğuk', badge: 'bg-blue-100 text-blue-700', bar: 'bg-blue-500' }
      if (v <= 13) return { label: 'Serin', badge: 'bg-cyan-100 text-cyan-800', bar: 'bg-cyan-500' }
      if (v <= 23) return { label: 'Ilık', badge: 'bg-emerald-100 text-emerald-700', bar: 'bg-emerald-500' }
      return { label: 'Sıcak', badge: 'bg-orange-100 text-orange-800', bar: 'bg-orange-500' }
    },
    formatDelta: (delta) => {
      const abs = Math.abs(delta).toFixed(1)
      if (Math.abs(delta) < 0.3) return { text: 'Bugünle aynı sıcaklıkta', tone: 'bg-slate-100 text-slate-600' }
      if (delta > 0) return { text: `Bugünden ${abs}° daha sıcak`, tone: 'bg-orange-50 text-orange-700 ring-1 ring-orange-200/80' }
      return { text: `Bugünden ${abs}° daha serin`, tone: 'bg-sky-50 text-sky-700 ring-1 ring-sky-200/80' }
    },
    extraTooltip: (pt) => {
      const items = []
      if (pt.humidity_pct != null) items.push(`Nem: %${Math.round(pt.humidity_pct)}`)
      if (pt.wind_speed_ms != null) items.push(`Rüzgar: ${Math.round(pt.wind_speed_ms * 3.6)} km/s`)
      if (pt.precipitation_mm != null && pt.precipitation_mm > 0) {
        items.push(`Yağış: ${pt.precipitation_mm} mm`)
      }
      return items
    },
  },
  {
    key: 'air_quality',
    kpiTitle: 'Hava Kalitesi',
    kpiSubtitle: 'Düşük puan = daha temiz',
    title: 'Hava Kalitesi',
    subtitle: 'Düşük puan = daha temiz hava',
    unit: 'puan',
    minLabel: 'En Temiz',
    maxLabel: 'En Yoğun',
    icon: Wind,
    color: '#059669',
    gradId: 'gradHistAqi',
    topBar: 'bg-gradient-to-r from-emerald-500 via-teal-400 to-cyan-400',
    cardGlow: 'bg-gradient-to-br from-emerald-50/55 via-white to-white',
    iconBox: 'bg-gradient-to-br from-emerald-600 to-teal-500 text-white shadow-sm shadow-emerald-200',
    chartIconBox: 'bg-emerald-50 text-emerald-600 ring-1 ring-emerald-100',
    cursor: { stroke: '#059669', strokeWidth: 1, strokeDasharray: '3 3', strokeOpacity: 0.45 },
    activeDot: { r: 5, fill: '#059669', stroke: '#ffffff', strokeWidth: 2 },
    higherIsBetter: false,
    axisOpts: { minSpan: 4, minStep: 1, allowNegative: false },
    axisSuffix: '',
    formatVal: (v) => Number(v).toFixed(1),
    getLevel: (v) => {
      if (v <= 20) return { label: 'İyi', badge: 'bg-emerald-100 text-emerald-700', bar: 'bg-emerald-500' }
      if (v <= 40) return { label: 'Makul', badge: 'bg-emerald-100 text-emerald-700', bar: 'bg-teal-500' }
      if (v <= 60) return { label: 'Orta', badge: 'bg-amber-100 text-amber-800', bar: 'bg-amber-500' }
      if (v <= 80) return { label: 'Kötü', badge: 'bg-orange-100 text-orange-800', bar: 'bg-orange-500' }
      return { label: 'Çok Kötü', badge: 'bg-rose-100 text-rose-700', bar: 'bg-rose-500' }
    },
    formatDelta: (delta) => {
      const abs = Math.round(Math.abs(delta))
      if (Math.abs(delta) < 1.5) return { text: 'Bugünle benzer düzeyde', tone: 'bg-slate-100 text-slate-600' }
      if (delta < 0) return { text: `Bugünden daha temiz (-${abs} puan)`, tone: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/80' }
      return { text: `Bugünden daha yoğun (+${abs} puan)`, tone: 'bg-amber-50 text-amber-800 ring-1 ring-amber-200/80' }
    },
    extraTooltip: (pt) => {
      const items = []
      if (pt.pm2_5 != null) items.push(`İnce Toz: ${Number(pt.pm2_5).toFixed(1)}`)
      if (pt.pm10 != null) items.push(`Toz Oranı: ${Number(pt.pm10).toFixed(1)}`)
      return items
    },
  },
  {
    key: 'energy',
    kpiTitle: 'Enerji Yükü',
    kpiSubtitle: 'Ortalama bölge tüketimi',
    title: 'Bölgesel Enerji Yükü',
    subtitle: 'Seçili dönem elektrik tüketim seyri',
    unit: 'MW',
    minLabel: 'En Düşük',
    maxLabel: 'En Yüksek',
    icon: Zap,
    color: '#D97706',
    gradId: 'gradHistEnergy',
    topBar: 'bg-gradient-to-r from-amber-500 via-orange-500 to-yellow-400',
    cardGlow: 'bg-gradient-to-br from-amber-50/55 via-white to-white',
    iconBox: 'bg-gradient-to-br from-amber-500 to-orange-500 text-white shadow-sm shadow-amber-200',
    chartIconBox: 'bg-amber-50 text-amber-600 ring-1 ring-amber-100',
    cursor: { stroke: '#D97706', strokeWidth: 1, strokeDasharray: '3 3', strokeOpacity: 0.45 },
    activeDot: { r: 5, fill: '#D97706', stroke: '#ffffff', strokeWidth: 2 },
    higherIsBetter: false,
    axisOpts: { minSpan: 15, minStep: 5, allowNegative: false },
    axisSuffix: '',
    formatVal: (v) => formatInt(v),
    getLevel: (v) => {
      if (v >= 195) return { label: 'Yüksek Yük', badge: 'bg-orange-100 text-orange-800', bar: 'bg-orange-500' }
      if (v >= 130) return { label: 'Normal Yük', badge: 'bg-amber-100 text-amber-800', bar: 'bg-amber-500' }
      return { label: 'Dengeli', badge: 'bg-emerald-100 text-emerald-700', bar: 'bg-emerald-500' }
    },
    formatDelta: (delta) => {
      const abs = formatInt(Math.abs(delta))
      if (Math.abs(delta) < 3) return { text: 'Bugünle benzer tüketim', tone: 'bg-slate-100 text-slate-600' }
      if (delta < 0) return { text: `Bugünden ${abs} MW daha az`, tone: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/80' }
      return { text: `Bugünden ${abs} MW daha fazla`, tone: 'bg-amber-50 text-amber-800 ring-1 ring-amber-200/80' }
    },
    extraTooltip: (pt) => {
      const items = []
      if (pt.renewable_share_pct != null) items.push(`Yenilenebilir: %${Math.round(pt.renewable_share_pct)}`)
      if (pt.carbon_intensity_gco2 != null) items.push(`Karbon: ${Math.round(pt.carbon_intensity_gco2)} g`)
      return items
    },
  },
]

function HistoryTooltip({ active, payload, cfg, avg }) {
  if (!active || !payload?.length) return null
  const pt = payload[0]?.payload
  const val = Number(payload[0]?.value)
  if (!pt || !Number.isFinite(val)) return null

  const lvl = cfg.getLevel(val)
  const diff = avg != null ? val - avg : 0
  const extras = cfg.extraTooltip(pt)

  return (
    <div className="min-w-[185px] rounded-xl border border-slate-200/90 bg-white p-3 text-xs shadow-lg">
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-[11px] font-semibold text-slate-500">
          {pt.full_label || pt.label}
        </span>
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
      {extras.length > 0 && (
        <div className="mt-1.5 flex flex-wrap gap-1.5 text-[10px] font-semibold text-slate-500">
          {extras.map((txt, idx) => (
            <span key={idx} className="rounded-md bg-slate-100 px-1.5 py-0.5">
              {txt}
            </span>
          ))}
        </div>
      )}
      {avg != null && Math.abs(diff) >= 0.05 && (
        <div className="mt-1.5 border-t border-slate-100 pt-1 text-[10px] font-medium text-slate-500">
          Dönem ortalamasına göre{' '}
          <strong className="font-mono text-slate-700">
            {diff > 0 ? '+' : ''}
            {cfg.key === 'energy' ? Math.round(diff) : diff.toFixed(1)} {cfg.unit}
          </strong>
        </div>
      )}
      {pt.lake_backed && (
        <div className="mt-1 inline-flex items-center gap-1 text-[10px] font-bold text-indigo-600">
          <Database className="h-2.5 w-2.5" />
          Spark Parquet Data Lake kaydı
        </div>
      )}
    </div>
  )
}

const HistoryChartCard = memo(function HistoryChartCard({ cfg, series }) {
  const Icon = cfg.icon

  const chartStats = useMemo(() => {
    let min = Infinity
    let max = -Infinity
    let sum = 0
    let count = 0
    for (let i = 0; i < series.length; i += 1) {
      const n = Number(series[i]?.[cfg.key])
      if (!Number.isFinite(n)) continue
      if (n < min) min = n
      if (n > max) max = n
      sum += n
      count += 1
    }
    if (count === 0) return null
    const avg = sum / count
    const yAxis = computeNiceYAxis(min, max, cfg.axisOpts)
    return { min, max, avg, domain: yAxis.domain, ticks: yAxis.ticks, decimals: yAxis.decimals }
  }, [series, cfg.key, cfg.axisOpts])

  const chartSeries = useMemo(
    () => downsampleSeries(series, MAX_CHART_POINTS),
    [series]
  )

  const tooltipContent = useMemo(
    () => <HistoryTooltip cfg={cfg} avg={chartStats?.avg} />,
    [cfg, chartStats?.avg]
  )

  return (
    <div className="perf-card relative flex flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white p-4 shadow-sm transition-shadow duration-200 hover:shadow-md">
      <div className={`absolute inset-x-0 top-0 h-1 ${cfg.topBar}`} />

      <div className="mb-3 flex items-start justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2.5">
          <span
            className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-xl ${cfg.chartIconBox}`}
          >
            <Icon className="h-4 w-4" />
          </span>
          <div className="min-w-0">
            <h3 className="truncate text-sm font-extrabold text-slate-900">{cfg.title}</h3>
            <p className="truncate text-[11px] font-medium text-slate-400">{cfg.subtitle}</p>
          </div>
        </div>

        {chartStats && (
          <div className="shrink-0 text-right">
            <div className="flex items-baseline justify-end gap-1">
              <span className="text-[10px] font-semibold text-slate-400">Ort</span>
              <span className="font-mono text-base font-extrabold text-slate-900 tabular-nums">
                {cfg.formatVal(chartStats.avg)}
              </span>
              <span className="text-[10px] font-bold text-slate-400">{cfg.unit}</span>
            </div>
          </div>
        )}
      </div>

      <div className="h-48 w-full">
        {!series.length || !chartStats ? (
          <div className="flex h-full items-center justify-center rounded-xl bg-slate-50/70 text-xs font-medium text-slate-400">
            Seçili dönem için veri bulunamadı.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartSeries} margin={CHART_MARGIN}>
              <defs>
                <linearGradient id={cfg.gradId} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={cfg.color} stopOpacity={0.28} />
                  <stop offset="85%" stopColor={cfg.color} stopOpacity={0.04} />
                  <stop offset="100%" stopColor={cfg.color} stopOpacity={0.0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
              <XAxis
                dataKey="label"
                tick={AXIS_TICK}
                tickLine={false}
                axisLine={false}
                minTickGap={26}
              />
              <YAxis
                width={40}
                tick={AXIS_TICK}
                tickLine={false}
                axisLine={false}
                ticks={chartStats.ticks}
                tickFormatter={(v) => formatAxisTick(v, chartStats.decimals, cfg.axisSuffix)}
                domain={chartStats.domain}
              />
              <Tooltip
                isAnimationActive={false}
                content={tooltipContent}
                cursor={cfg.cursor}
              />
              <ReferenceLine
                y={chartStats.avg}
                stroke={cfg.color}
                strokeOpacity={0.4}
                strokeDasharray="4 4"
                strokeWidth={1.2}
              />
              <Area
                type="monotone"
                dataKey={cfg.key}
                stroke={cfg.color}
                strokeWidth={2.4}
                fill={`url(#${cfg.gradId})`}
                activeDot={cfg.activeDot}
                isAnimationActive={false}
              />
            </AreaChart>
          </ResponsiveContainer>
        )}
      </div>

      {chartStats && (
        <div className="mt-2.5 flex flex-wrap items-center justify-between gap-2 border-t border-slate-100 pt-2.5 text-[11px]">
          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-slate-500">
            <span>
              En Düşük:{' '}
              <strong className="font-mono font-bold text-slate-700 tabular-nums">
                {cfg.formatVal(chartStats.min)}
              </strong>
            </span>
            <span>
              En Yüksek:{' '}
              <strong className="font-mono font-bold text-slate-700 tabular-nums">
                {cfg.formatVal(chartStats.max)}
              </strong>
            </span>
          </div>
          <span className="font-mono text-[10px] font-semibold text-slate-400">
            {series.length} ölçüm
          </span>
        </div>
      )}
    </div>
  )
})

function HistoryView() {
  const todayStr = useMemo(() => isoDateDaysAgo(0), [])
  const minDateStr = useMemo(() => isoDateDaysAgo(365), [])

  const [mode, setMode] = useState('single') // 'single' | 'range'
  const [activePreset, setActivePreset] = useState('5d')
  const [singleDate, setSingleDate] = useState(() => isoDateDaysAgo(5))
  const [startDate, setStartDate] = useState(() => isoDateDaysAgo(7))
  const [endDate, setEndDate] = useState(() => isoDateDaysAgo(1))
  const [districtId, setDistrictId] = useState('all')
  const [granularity, setGranularity] = useState('hourly')

  const [districtOptions, setDistrictOptions] = useState(() => cachedDistrictOptions || EMPTY_ARR)
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [showTable, setShowTable] = useState(false)
  const [visibleTableRows, setVisibleTableRows] = useState(INITIAL_TABLE_ROWS)
  const abortRef = useRef(null)

  // Fetch 32 London boroughs once per session
  useEffect(() => {
    if (cachedDistrictOptions && cachedDistrictOptions.length > 0) return
    let cancelled = false
    fetch('/api/districts')
      .then((r) => (r.ok ? r.json() : null))
      .then((res) => {
        if (!cancelled && res?.districts?.length) {
          cachedDistrictOptions = res.districts
          setDistrictOptions(res.districts)
        }
      })
      .catch(() => {})
    return () => {
      cancelled = true
    }
  }, [])

  const loadHistory = useCallback(
    async ({ forceRefresh = false } = {}) => {
      const params = new URLSearchParams()
      const effGranularity = mode === 'single' ? 'hourly' : granularity
      if (mode === 'single') {
        params.set('date', singleDate)
      } else {
        params.set('start_date', startDate)
        params.set('end_date', endDate)
      }
      params.set('district_id', districtId)
      params.set('granularity', effGranularity)
      const cacheKey = params.toString()

      if (!forceRefresh) {
        const cached = getCachedHistory(cacheKey)
        if (cached) {
          if (abortRef.current) abortRef.current.abort()
          setError(null)
          setLoading(false)
          startTransition(() => {
            setData(cached)
            setVisibleTableRows(INITIAL_TABLE_ROWS)
          })
          return
        }
      }

      if (abortRef.current) abortRef.current.abort()
      const controller = new AbortController()
      abortRef.current = controller

      setLoading(true)
      setError(null)
      try {
        if (forceRefresh) params.set('refresh', 'true')

        const resp = await fetch(`/api/history?${params.toString()}`, {
          signal: controller.signal,
        })
        if (!resp.ok) {
          let detail = `Sunucu hatası (${resp.status})`
          try {
            const errJson = await resp.json()
            if (errJson?.detail) detail = errJson.detail
          } catch {
            // ignore
          }
          throw new Error(detail)
        }
        const json = await resp.json()
        setCachedHistory(cacheKey, json)
        startTransition(() => {
          setData(json)
          setVisibleTableRows(INITIAL_TABLE_ROWS)
        })
      } catch (err) {
        if (err.name === 'AbortError') return
        setError(err.message || 'Geçmiş veriler yüklenemedi.')
      } finally {
        if (!controller.signal.aborted) {
          setLoading(false)
        }
      }
    },
    [mode, singleDate, startDate, endDate, districtId, granularity]
  )

  useEffect(() => {
    loadHistory()
    return () => {
      if (abortRef.current) abortRef.current.abort()
    }
  }, [loadHistory])

  const applyPreset = useCallback((preset) => {
    setActivePreset(preset.id)
    if (preset.mode === 'single') {
      setMode('single')
      setSingleDate(isoDateDaysAgo(preset.daysAgo))
      setGranularity('hourly')
    } else {
      setMode('range')
      setStartDate(isoDateDaysAgo(preset.rangeDays))
      setEndDate(isoDateDaysAgo(1))
      setGranularity(preset.rangeDays > 7 ? 'daily' : 'auto')
    }
  }, [])

  const summary = data?.summary || EMPTY_OBJ
  const series = data?.series || EMPTY_ARR
  const queryInfo = data?.query || null
  const tableRows = useMemo(
    () => (showTable ? series.slice(0, visibleTableRows) : EMPTY_ARR),
    [showTable, series, visibleTableRows]
  )

  const showGranularityToggle = mode === 'range' && (queryInfo?.days_count ?? 2) > 1

  return (
    <div className="space-y-5">
      {/* Control & Time-Travel Filter Bar */}
      <section className="perf-card relative overflow-hidden rounded-2xl border border-slate-200/90 bg-white p-4 shadow-sm sm:p-5">
        <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-indigo-600 via-blue-500 to-cyan-400" />

        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-indigo-600 to-blue-500 text-white shadow-sm shadow-indigo-200">
              <History className="h-5 w-5" />
            </span>
            <div>
              <div className="flex flex-wrap items-center gap-2">
                <h2 className="text-base font-extrabold text-slate-900">
                  Geçmiş Dönem Özeti
                </h2>
                {queryInfo && (
                  <span className="inline-flex items-center gap-1.5 rounded-full bg-indigo-50 px-2.5 py-0.5 text-xs font-bold text-indigo-700 ring-1 ring-indigo-200/80">
                    <Calendar className="h-3.5 w-3.5" />
                    {queryInfo.display_range}
                    <span className="text-indigo-300">•</span>
                    {queryInfo.district_id === 'all' ? 'Tüm Londra' : queryInfo.district_name}
                  </span>
                )}
              </div>
              <p className="mt-0.5 text-xs font-medium text-slate-500">
                Geçmiş bir gün ya da tarih aralığı seçin; o günün sıcaklık, trafik, hava kalitesi ve enerji durumunu görün.
              </p>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => loadHistory({ forceRefresh: true })}
              disabled={loading}
              className="inline-flex items-center gap-1.5 rounded-xl border border-slate-200/90 bg-white px-3 py-1.5 text-xs font-bold text-slate-700 transition-colors hover:bg-slate-50 disabled:opacity-50"
            >
              <RefreshCw className={`h-3.5 w-3.5 ${loading ? 'animate-spin' : ''}`} />
              Yenile
            </button>
          </div>
        </div>

        {/* Quick Presets */}
        <div className="mt-4 flex flex-wrap items-center gap-1.5">
          <span className="mr-1 text-[11px] font-bold uppercase tracking-wide text-slate-400">
            Hızlı Seçim:
          </span>
          {PRESETS.map((p) => {
            const active = activePreset === p.id
            return (
              <button
                key={p.id}
                type="button"
                onClick={() => applyPreset(p)}
                className={`rounded-xl px-3 py-1.5 text-xs font-bold transition-colors ${
                  active
                    ? 'bg-gradient-to-r from-indigo-600 to-blue-600 text-white shadow-sm shadow-indigo-500/25'
                    : 'bg-slate-100/90 text-slate-600 hover:bg-slate-200/70 hover:text-slate-900'
                }`}
              >
                {p.label}
              </button>
            )
          })}
        </div>

        {/* Custom Date, District & Granularity Controls */}
        <div className="mt-3.5 flex flex-wrap items-center gap-3 border-t border-slate-100 pt-3.5">
          {/* Mode Toggle */}
          <div className="flex items-center gap-0.5 rounded-xl bg-slate-100/90 p-0.5 ring-1 ring-slate-200/70">
            <button
              type="button"
              onClick={() => {
                setMode('single')
                setGranularity('hourly')
                setActivePreset(null)
              }}
              className={`rounded-lg px-2.5 py-1 text-xs font-bold transition-colors ${
                mode === 'single'
                  ? 'bg-white text-slate-900 shadow-sm'
                  : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              Tek Gün
            </button>
            <button
              type="button"
              onClick={() => {
                setMode('range')
                setGranularity('auto')
                setActivePreset(null)
              }}
              className={`rounded-lg px-2.5 py-1 text-xs font-bold transition-colors ${
                mode === 'range'
                  ? 'bg-white text-slate-900 shadow-sm'
                  : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              Tarih Aralığı
            </button>
          </div>

          {/* Date Input(s) */}
          {mode === 'single' ? (
            <label className="flex items-center gap-2 text-xs font-semibold text-slate-600">
              <span>Tarih:</span>
              <input
                type="date"
                value={singleDate}
                min={minDateStr}
                max={todayStr}
                onChange={(e) => {
                  if (!e.target.value) return
                  setActivePreset(null)
                  setSingleDate(e.target.value)
                }}
                className="rounded-xl border border-slate-200/90 bg-white px-2.5 py-1.5 font-mono text-xs font-semibold text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-100"
              />
            </label>
          ) : (
            <div className="flex flex-wrap items-center gap-2 text-xs font-semibold text-slate-600">
              <label className="flex items-center gap-1.5">
                <span>Başlangıç:</span>
                <input
                  type="date"
                  value={startDate}
                  min={minDateStr}
                  max={endDate || todayStr}
                  onChange={(e) => {
                    if (!e.target.value) return
                    setActivePreset(null)
                    setStartDate(e.target.value)
                  }}
                  className="rounded-xl border border-slate-200/90 bg-white px-2.5 py-1.5 font-mono text-xs font-semibold text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-100"
                />
              </label>
              <span className="text-slate-400">–</span>
              <label className="flex items-center gap-1.5">
                <span>Bitiş:</span>
                <input
                  type="date"
                  value={endDate}
                  min={startDate || minDateStr}
                  max={todayStr}
                  onChange={(e) => {
                    if (!e.target.value) return
                    setActivePreset(null)
                    setEndDate(e.target.value)
                  }}
                  className="rounded-xl border border-slate-200/90 bg-white px-2.5 py-1.5 font-mono text-xs font-semibold text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-100"
                />
              </label>
            </div>
          )}

          {/* District Selector */}
          <label className="flex items-center gap-1.5 text-xs font-semibold text-slate-600">
            <MapPin className="h-3.5 w-3.5 text-slate-400" />
            <span>Bölge:</span>
            <select
              value={districtId}
              onChange={(e) => setDistrictId(e.target.value)}
              className="rounded-xl border border-slate-200/90 bg-white px-2.5 py-1.5 text-xs font-bold text-slate-800 focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-100"
            >
              <option value="all">Tüm Londra (Genel Ortalama)</option>
              {districtOptions.map((d) => (
                <option key={d.district_id} value={d.district_id}>
                  {d.district_name}
                </option>
              ))}
            </select>
          </label>

          {/* Granularity Toggle (only meaningful for multi-day ranges) */}
          {showGranularityToggle && (
            <div className="flex items-center gap-0.5 rounded-xl bg-slate-100/90 p-0.5 ring-1 ring-slate-200/70">
              <button
                type="button"
                onClick={() => setGranularity('hourly')}
                className={`rounded-lg px-2.5 py-1 text-xs font-bold transition-colors ${
                  (queryInfo?.granularity || granularity) === 'hourly'
                    ? 'bg-white text-slate-900 shadow-sm'
                    : 'text-slate-600 hover:text-slate-900'
                }`}
              >
                Saatlik
              </button>
              <button
                type="button"
                onClick={() => setGranularity('daily')}
                className={`rounded-lg px-2.5 py-1 text-xs font-bold transition-colors ${
                  (queryInfo?.granularity || granularity) === 'daily'
                    ? 'bg-white text-slate-900 shadow-sm'
                    : 'text-slate-600 hover:text-slate-900'
                }`}
              >
                Günlük Ort.
              </button>
            </div>
          )}
        </div>
      </section>

      {/* Error Banner */}
      {error && (
        <div className="flex items-center gap-2.5 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-xs font-semibold text-rose-700">
          <AlertCircle className="h-4 w-4 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      {/* Loading Skeleton */}
      {loading && !data && (
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
          {[1, 2, 3, 4].map((n) => (
            <div
              key={n}
              className="h-44 animate-pulse rounded-2xl border border-slate-200/80 bg-white p-5"
            >
              <div className="h-4 w-28 rounded bg-slate-200" />
              <div className="mt-5 h-8 w-24 rounded bg-slate-200" />
              <div className="mt-4 h-3 w-full rounded bg-slate-100" />
            </div>
          ))}
        </div>
      )}

      {/* 4 Human-Friendly Historical KPI Summary Cards */}
      {data && (
        <section
          className={`grid grid-cols-1 gap-4 transition-opacity sm:grid-cols-2 lg:grid-cols-4 ${
            loading ? 'opacity-60' : 'opacity-100'
          }`}
        >
          {DOMAIN_CONFIGS.map((cfg) => {
            const Icon = cfg.kpiIcon || cfg.icon
            const domSum = summary[cfg.key]
            const avgVal = domSum?.avg
            const status =
              avgVal != null
                ? cfg.getLevel(avgVal)
                : { label: '—', badge: 'bg-slate-100 text-slate-500' }
            const delta = domSum?.delta_vs_live
            const deltaInfo = delta != null ? cfg.formatDelta(delta) : null
            const sec = domSum?.secondary || {}

            return (
              <div
                key={cfg.key}
                className={`perf-card relative flex flex-col justify-between overflow-hidden rounded-2xl border border-slate-200/90 p-5 shadow-sm ${cfg.cardGlow}`}
              >
                <div className={`absolute inset-x-0 top-0 h-1.5 ${cfg.topBar}`} />

                <div>
                  {/* Card Header: Crisp non-truncated title + Human Status */}
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex items-center gap-2.5">
                      <span
                        className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl ${cfg.iconBox}`}
                      >
                        <Icon className="h-5 w-5" />
                      </span>
                      <div>
                        <div className="text-sm font-extrabold text-slate-900">
                          {cfg.kpiTitle}
                        </div>
                        <div className="text-[11px] font-medium text-slate-400">
                          {cfg.kpiSubtitle}
                        </div>
                      </div>
                    </div>
                    <span
                      className={`shrink-0 rounded-full px-2.5 py-0.5 text-[11px] font-bold ${status.badge}`}
                    >
                      {status.label}
                    </span>
                  </div>

                  {/* Main Average Value */}
                  <div className="mt-4 flex items-baseline gap-1.5">
                    <span className="font-mono text-3xl font-extrabold tracking-tight text-slate-900 tabular-nums">
                      {avgVal != null ? cfg.formatVal(avgVal) : '—'}
                    </span>
                    <span className="text-sm font-bold text-slate-400">{cfg.unit}</span>
                  </div>

                  {/* Clean En Düşük / En Yüksek Box with Exact Time */}
                  {domSum?.min != null && domSum?.max != null && (
                    <div className="mt-3 grid grid-cols-2 gap-2 rounded-xl bg-slate-50/90 p-2.5 ring-1 ring-slate-200/60">
                      <div>
                        <div className="text-[10px] font-semibold text-slate-400">
                          {cfg.minLabel}
                        </div>
                        <div className="mt-0.5 font-mono text-xs font-extrabold text-slate-800 tabular-nums">
                          {cfg.formatVal(domSum.min)}{' '}
                          <span className="font-sans text-[10px] font-semibold text-slate-400">
                            {cfg.unit}
                          </span>
                        </div>
                      </div>
                      <div className="border-l border-slate-200/70 pl-2.5">
                        <div className="text-[10px] font-semibold text-slate-400">
                          {cfg.maxLabel}
                        </div>
                        <div className="mt-0.5 font-mono text-xs font-extrabold text-slate-800 tabular-nums">
                          {cfg.formatVal(domSum.max)}{' '}
                          <span className="font-sans text-[10px] font-semibold text-slate-400">
                            {cfg.unit}
                          </span>
                        </div>
                      </div>
                    </div>
                  )}
                </div>

                {/* Readable Context Chips + Plain-Turkish Comparison vs Today */}
                <div className="mt-3 space-y-2">
                  <div className="flex flex-wrap items-center gap-1.5">
                    {cfg.key === 'traffic' && sec.avg_congestion_pct != null && (
                      <span className="inline-flex items-center gap-1 rounded-lg bg-blue-50/90 px-2 py-1 text-[11px] font-semibold text-blue-700 ring-1 ring-blue-100">
                        <Gauge className="h-3 w-3" />
                        Yoğunluk %{Math.round(sec.avg_congestion_pct)}
                      </span>
                    )}
                    {cfg.key === 'weather' && sec.avg_humidity_pct != null && (
                      <>
                        <span className="inline-flex items-center gap-1 rounded-lg bg-cyan-50/90 px-2 py-1 text-[11px] font-semibold text-cyan-800 ring-1 ring-cyan-100">
                          <Droplets className="h-3 w-3" />
                          Nem %{Math.round(sec.avg_humidity_pct)}
                        </span>
                        <span className="inline-flex items-center gap-1 rounded-lg bg-sky-50 px-2 py-1 text-[11px] font-semibold text-sky-700 ring-1 ring-sky-200/70">
                          <CloudRain className="h-3 w-3" />
                          {sec.total_precipitation_mm > 0
                            ? `Yağış ${sec.total_precipitation_mm} mm`
                            : 'Yağışsız'}
                        </span>
                      </>
                    )}
                    {cfg.key === 'air_quality' && sec.avg_pm2_5 != null && (
                      <>
                        <span className="inline-flex items-center rounded-lg bg-emerald-50/90 px-2 py-1 text-[11px] font-semibold text-emerald-800 ring-1 ring-emerald-100">
                          İnce Toz: {sec.avg_pm2_5}
                        </span>
                        <span className="inline-flex items-center rounded-lg bg-slate-100/90 px-2 py-1 text-[11px] font-semibold text-slate-700 ring-1 ring-slate-200/70">
                          Toz: {sec.avg_pm10 ?? '—'}
                        </span>
                      </>
                    )}
                    {cfg.key === 'energy' && sec.avg_renewable_share_pct != null && (
                      <span className="inline-flex items-center gap-1 rounded-lg bg-emerald-50 px-2 py-1 text-[11px] font-semibold text-emerald-700 ring-1 ring-emerald-200/80">
                        <Leaf className="h-3 w-3" />
                        Temiz Enerji %{Math.round(sec.avg_renewable_share_pct)}
                      </span>
                    )}
                  </div>

                  {deltaInfo && (
                    <div
                      className={`inline-flex w-full items-center justify-center rounded-lg px-2.5 py-1.5 text-[11px] font-bold ${deltaInfo.tone}`}
                      title={`Bugünkü canlı değer: ${cfg.formatVal(domSum.live_value)} ${cfg.unit}`}
                    >
                      {deltaInfo.text}
                    </div>
                  )}
                </div>
              </div>
            )
          })}
        </section>
      )}

      {/* 4 Historical Trend Charts (2x2 Grid) */}
      {data && (
        <section
          className={`grid grid-cols-1 gap-4 transition-opacity md:grid-cols-2 ${
            loading ? 'opacity-60' : 'opacity-100'
          }`}
        >
          {DOMAIN_CONFIGS.map((cfg) => (
            <HistoryChartCard
              key={cfg.key}
              cfg={cfg}
              series={series}
            />
          ))}
        </section>
      )}

      {/* Expandable Hourly / Daily Breakdown Table */}
      {data && series.length > 0 && (
        <section className="perf-card overflow-hidden rounded-2xl border border-slate-200/90 bg-white shadow-sm">
          <button
            type="button"
            onClick={() => startTransition(() => setShowTable((v) => !v))}
            className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left transition-colors hover:bg-slate-50/70"
          >
            <div className="flex items-center gap-2.5">
              <span className="flex h-8 w-8 items-center justify-center rounded-xl bg-slate-100 text-slate-600 ring-1 ring-slate-200/80">
                <Table2 className="h-4 w-4" />
              </span>
              <div>
                <div className="text-xs font-extrabold text-slate-900">
                  {queryInfo?.granularity === 'daily'
                    ? 'Günlük Ölçüm Tablosu'
                    : 'Saatlik Ölçüm Tablosu'}
                </div>
                <p className="text-[11px] font-medium text-slate-400">
                  {series.length} kayıt • Trafik hızı, sıcaklık, nem, yağış, hava kalitesi ve enerji detayları
                </p>
              </div>
            </div>
            <span className="inline-flex items-center gap-1 rounded-lg bg-slate-100 px-2.5 py-1 text-[11px] font-bold text-slate-600">
              {showTable ? 'Gizle' : 'Tabloyu Aç'}
              {showTable ? <ChevronUp className="h-3.5 w-3.5" /> : <ChevronDown className="h-3.5 w-3.5" />}
            </span>
          </button>

          {showTable && (
            <div className="max-h-96 overflow-auto border-t border-slate-100">
              <table className="w-full text-left text-xs">
                <thead className="sticky top-0 bg-slate-50 text-[11px] font-bold text-slate-500">
                  <tr>
                    <th className="px-4 py-2.5">Zaman</th>
                    <th className="px-3 py-2.5 text-right">Trafik (km/s)</th>
                    <th className="px-3 py-2.5 text-right">Yoğunluk</th>
                    <th className="px-3 py-2.5 text-right">Sıcaklık (°C)</th>
                    <th className="px-3 py-2.5 text-right">Nem (%)</th>
                    <th className="px-3 py-2.5 text-right">Yağış (mm)</th>
                    <th className="px-3 py-2.5 text-right">AQI</th>
                    <th className="px-3 py-2.5 text-right">Enerji (MW)</th>
                    <th className="px-4 py-2.5 text-right">Kaynak</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 font-mono text-[11px] tabular-nums">
                  {tableRows.map((row) => (
                    <tr key={row.timestamp} className="hover:bg-slate-50/80">
                      <td className="px-4 py-2 font-sans font-semibold text-slate-700">
                        {row.full_label || row.label}
                      </td>
                      <td className="px-3 py-2 text-right font-bold text-blue-700">
                        {row.traffic != null ? Number(row.traffic).toFixed(1) : '—'}
                      </td>
                      <td className="px-3 py-2 text-right text-slate-600">
                        {row.congestion_pct != null ? `%${Math.round(row.congestion_pct)}` : '—'}
                      </td>
                      <td className="px-3 py-2 text-right font-bold text-cyan-700">
                        {row.weather != null ? Number(row.weather).toFixed(1) : '—'}
                      </td>
                      <td className="px-3 py-2 text-right text-slate-600">
                        {row.humidity_pct != null ? `%${Math.round(row.humidity_pct)}` : '—'}
                      </td>
                      <td className="px-3 py-2 text-right text-slate-600">
                        {row.precipitation_mm != null ? Number(row.precipitation_mm).toFixed(1) : '0.0'}
                      </td>
                      <td className="px-3 py-2 text-right font-bold text-emerald-700">
                        {row.air_quality != null ? Number(row.air_quality).toFixed(1) : '—'}
                      </td>
                      <td className="px-3 py-2 text-right font-bold text-amber-700">
                        {row.energy != null ? formatInt(row.energy) : '—'}
                      </td>
                      <td className="px-4 py-2 text-right font-sans">
                        {row.lake_backed ? (
                          <span className="inline-flex items-center gap-1 rounded-full bg-indigo-50 px-2 py-0.5 text-[10px] font-bold text-indigo-700">
                            <Database className="h-2.5 w-2.5" />
                            Data Lake
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-semibold text-slate-600">
                            <Globe className="h-2.5 w-2.5" />
                            Arşiv API
                          </span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {series.length > visibleTableRows && (
                <div className="flex items-center justify-center gap-2 border-t border-slate-100 bg-slate-50/70 px-4 py-2.5">
                  <button
                    type="button"
                    onClick={() =>
                      startTransition(() => setVisibleTableRows((n) => n + 100))
                    }
                    className="rounded-lg border border-slate-200 bg-white px-3 py-1 text-xs font-bold text-slate-700 hover:bg-slate-50"
                  >
                    Daha Fazla Göster ({series.length - visibleTableRows} kaldı)
                  </button>
                  <button
                    type="button"
                    onClick={() =>
                      startTransition(() => setVisibleTableRows(series.length))
                    }
                    className="rounded-lg bg-slate-200/70 px-3 py-1 text-xs font-bold text-slate-700 hover:bg-slate-200"
                  >
                    Tümünü Göster
                  </button>
                </div>
              )}
            </div>
          )}
        </section>
      )}
    </div>
  )
}

export default memo(HistoryView)
