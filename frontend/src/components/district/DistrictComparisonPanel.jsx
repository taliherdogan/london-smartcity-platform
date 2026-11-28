import React, { memo, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  AlertTriangle,
  CheckCircle2,
  Flame,
  Gauge,
  Thermometer,
  TrendingDown,
  TrendingUp,
  Wind,
  Zap,
} from 'lucide-react'
import {
  ENERGY_SHARE_COLORS,
  getAqiBarStyle,
  getEnergyBarStyle,
  getTempBarStyle,
  getTrafficBarStyle,
} from './districtHelpers'

const LABEL_STYLE = {
  fill: '#334155',
  fontSize: 9,
  fontWeight: 700,
  fontFamily: 'ui-monospace, SFMono-Regular, monospace',
}
const X_TICK = { fontSize: 10, fontWeight: 600, fill: '#475569' }
const Y_TICK = { fontSize: 11, fill: '#94a3b8' }

function TrafficChartTooltip({ active, payload, avgTraffic }) {
  if (!active || !payload?.length) return null
  const item = payload[0].payload
  const speed = item.traffic
  if (speed == null) return null
  const style = getTrafficBarStyle(speed)
  const diff = speed - avgTraffic
  return (
    <div className="min-w-[190px] rounded-xl border border-slate-200/90 bg-white p-3 text-xs shadow-lg">
      <div className="flex items-center justify-between gap-2">
        <span className="font-bold text-slate-900">{item.fullName}</span>
        <span className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${style.badge}`}>{style.label}</span>
      </div>
      <div className="mt-2 flex items-baseline gap-1.5">
        <span className="font-mono text-xl font-extrabold text-slate-900 tabular-nums">{speed.toFixed(1)}</span>
        <span className="text-[11px] font-semibold text-slate-400">km/s ortalama hız</span>
      </div>
      <div className="mt-1.5 border-t border-slate-100 pt-1.5 text-[11px] font-medium">
        {diff < -0.5 ? (
          <span className="text-amber-700">Şehir ortalamasından <strong>{Math.abs(diff).toFixed(1)} km/s</strong> daha yavaş</span>
        ) : diff > 0.5 ? (
          <span className="text-emerald-700">Şehir ortalamasından <strong>+{diff.toFixed(1)} km/s</strong> daha akıcı</span>
        ) : (
          <span className="text-slate-500">Şehir ortalamasına yakın akış</span>
        )}
      </div>
    </div>
  )
}

function EnergyChartTooltip({ active, payload, avgEnergy }) {
  if (!active || !payload?.length) return null
  const item = payload[0].payload
  const energy = item.energy
  const style = getEnergyBarStyle(energy, avgEnergy)
  const diff = energy - avgEnergy
  return (
    <div className="min-w-[200px] rounded-xl border border-slate-200/90 bg-white p-3 text-xs shadow-lg">
      <div className="flex items-center justify-between gap-2">
        <span className="font-bold text-slate-900">{item.fullName}</span>
        <span className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${style.badge}`}>{style.label}</span>
      </div>
      <div className="mt-2 flex items-baseline justify-between gap-2">
        <div className="flex items-baseline gap-1">
          <span className="font-mono text-xl font-extrabold text-slate-900 tabular-nums">{Math.round(energy).toLocaleString('tr-TR')}</span>
          <span className="text-[11px] font-semibold text-slate-400">MW</span>
        </div>
        <span className="rounded-md bg-amber-50 px-1.5 py-0.5 font-mono text-[11px] font-bold text-amber-700">Londra %{item.energyShare}</span>
      </div>
      <div className="mt-1.5 border-t border-slate-100 pt-1.5 text-[11px] font-medium">
        {diff >= 10 ? (
          <span className="text-orange-700">Bölge ortalamasından <strong>+{Math.round(diff).toLocaleString('tr-TR')} MW</strong> yüksek</span>
        ) : diff <= -10 ? (
          <span className="text-emerald-700">Bölge ortalamasından <strong>{Math.round(Math.abs(diff)).toLocaleString('tr-TR')} MW</strong> düşük</span>
        ) : (
          <span className="text-slate-500">Bölge ortalamasına yakın tüketim</span>
        )}
      </div>
    </div>
  )
}

function AqiChartTooltip({ active, payload, avgAqi }) {
  if (!active || !payload?.length) return null
  const item = payload[0].payload
  const aqi = item.aqi
  if (aqi == null) return null
  const style = getAqiBarStyle(aqi)
  const diff = aqi - avgAqi
  return (
    <div className="min-w-[195px] rounded-xl border border-slate-200/90 bg-white p-3 text-xs shadow-lg">
      <div className="flex items-center justify-between gap-2">
        <span className="font-bold text-slate-900">{item.fullName}</span>
        <span className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${style.badge}`}>{style.label}</span>
      </div>
      <div className="mt-2 flex items-baseline gap-1.5">
        <span className="font-mono text-xl font-extrabold text-slate-900 tabular-nums">{aqi.toFixed(1)}</span>
        <span className="text-[11px] font-semibold text-slate-400">hava puanı</span>
      </div>
      <div className="mt-1.5 border-t border-slate-100 pt-1.5 text-[11px] font-medium">
        {diff > 0.5 ? (
          <span className="text-amber-700">Şehir ortalamasından <strong>+{diff.toFixed(1)} puan</strong> daha yüksek</span>
        ) : diff < -0.5 ? (
          <span className="text-emerald-700">Şehir ortalamasından <strong>{Math.abs(diff).toFixed(1)} puan</strong> daha temiz</span>
        ) : (
          <span className="text-slate-500">Şehir ortalamasına yakın hava kalitesi</span>
        )}
      </div>
    </div>
  )
}

function TempChartTooltip({ active, payload, avgTemp }) {
  if (!active || !payload?.length) return null
  const item = payload[0].payload
  const temp = item.temp
  if (temp == null) return null
  const style = getTempBarStyle(temp)
  const diff = temp - avgTemp
  return (
    <div className="min-w-[195px] rounded-xl border border-slate-200/90 bg-white p-3 text-xs shadow-lg">
      <div className="flex items-center justify-between gap-2">
        <span className="font-bold text-slate-900">{item.fullName}</span>
        <span className={`rounded-full px-2 py-0.5 text-[10px] font-bold ${style.badge}`}>{style.label}</span>
      </div>
      <div className="mt-2 flex items-baseline gap-1.5">
        <span className="font-mono text-xl font-extrabold text-slate-900 tabular-nums">{temp.toFixed(1)}</span>
        <span className="text-[11px] font-semibold text-slate-400">°C anlık sıcaklık</span>
      </div>
      <div className="mt-1.5 border-t border-slate-100 pt-1.5 text-[11px] font-medium">
        {diff > 0.2 ? (
          <span className="text-orange-700">Şehir ortalamasından <strong>+{diff.toFixed(1)} °C</strong> daha sıcak</span>
        ) : diff < -0.2 ? (
          <span className="text-cyan-700">Şehir ortalamasından <strong>{Math.abs(diff).toFixed(1)} °C</strong> daha serin</span>
        ) : (
          <span className="text-slate-500">Şehir ortalamasına yakın sıcaklık</span>
        )}
      </div>
    </div>
  )
}

function DistrictComparisonPanel({ chartData, chartInsights }) {
  const [activeChart, setActiveChart] = useState('traffic')

  const switcherItems = [
    {
      id: 'traffic',
      label: 'Trafik Hızı',
      icon: Gauge,
      activeCls: 'bg-white text-blue-700 shadow-sm ring-1 ring-blue-200/80',
      badgeCls: 'bg-blue-50 text-blue-700',
      summary: `${chartInsights.avgTraffic.toFixed(1)} km/s`,
    },
    {
      id: 'energy',
      label: 'Enerji Yükü',
      icon: Zap,
      activeCls: 'bg-white text-amber-700 shadow-sm ring-1 ring-amber-200/80',
      badgeCls: 'bg-amber-50 text-amber-700',
      summary: `${Math.round(chartInsights.totalEnergy).toLocaleString('tr-TR')} MW`,
    },
    {
      id: 'air_quality',
      label: 'Hava Kalitesi',
      icon: Wind,
      activeCls: 'bg-white text-emerald-700 shadow-sm ring-1 ring-emerald-200/80',
      badgeCls: 'bg-emerald-50 text-emerald-700',
      summary: `${Math.round(chartInsights.avgAqi)} puan`,
    },
    {
      id: 'weather',
      label: 'Sıcaklık',
      icon: Thermometer,
      activeCls: 'bg-white text-cyan-700 shadow-sm ring-1 ring-cyan-200/80',
      badgeCls: 'bg-cyan-50 text-cyan-700',
      summary: `${chartInsights.avgTemp.toFixed(1)} °C`,
    },
  ]

  return (
    <div className="space-y-3">
      {/* Chart Switcher Bar */}
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-slate-200/90 bg-white px-4 py-3 shadow-sm">
        <div className="flex items-center gap-2">
          <span className="text-xs font-extrabold text-slate-800">Bölgesel Karşılaştırma Grafiği</span>
          <span className="hidden text-[11px] font-medium text-slate-400 sm:inline">
            • İncelemek istediğiniz grafiği seçin
          </span>
        </div>

        <div className="flex flex-wrap items-center gap-1 rounded-xl bg-slate-100/90 p-1 ring-1 ring-slate-200/70">
          {switcherItems.map(({ id, label, icon: Icon, activeCls, badgeCls, summary }) => {
            const active = activeChart === id
            return (
              <button
                key={id}
                type="button"
                onClick={() => setActiveChart(id)}
                className={`inline-flex items-center gap-2 rounded-lg px-3 py-1.5 text-xs font-bold transition-colors ${
                  active ? activeCls : 'text-slate-600 hover:text-slate-900'
                }`}
              >
                <Icon className="h-3.5 w-3.5" />
                <span>{label}</span>
                <span
                  className={`rounded-md px-1.5 py-0.5 font-mono text-[10px] font-bold tabular-nums ${
                    active ? badgeCls : 'bg-slate-200/70 text-slate-500'
                  }`}
                >
                  {summary}
                </span>
              </button>
            )
          })}
        </div>
      </div>

      {/* Traffic */}
      {activeChart === 'traffic' && (
        <div className="perf-card relative flex flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white p-5 shadow-sm transition-shadow hover:shadow-md">
          <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-rose-500 via-blue-500 to-emerald-500" />
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="flex items-center gap-3">
              <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-blue-50 text-blue-600 ring-1 ring-blue-100">
                <Gauge className="h-5 w-5" />
              </span>
              <div>
                <h3 className="text-sm font-extrabold text-slate-900">Bölgelere Göre Trafik Hızı</h3>
                <p className="text-[11px] font-medium text-slate-500">Düşük hız = daha yoğun trafik • Renkler akış durumunu gösterir</p>
              </div>
            </div>
            <div className="rounded-xl bg-slate-50 px-3 py-1.5 text-right ring-1 ring-slate-200/70">
              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Londra Ort.</div>
              <div className="font-mono text-sm font-extrabold text-slate-900 tabular-nums">
                {chartInsights.avgTraffic.toFixed(1)} <span className="text-[11px] font-semibold text-slate-400">km/s</span>
              </div>
            </div>
          </div>

          <div className="mt-3.5 grid grid-cols-1 gap-2 sm:grid-cols-2">
            <div className="flex items-center justify-between gap-2 rounded-xl bg-amber-50/80 px-3 py-2 ring-1 ring-amber-200/70">
              <div className="flex min-w-0 items-center gap-2">
                <TrendingDown className="h-4 w-4 shrink-0 text-amber-600" />
                <div className="truncate">
                  <div className="text-[10px] font-semibold text-amber-700/80">En Yoğun Bölge</div>
                  <div className="truncate text-xs font-bold text-slate-900">{chartInsights.busiest.fullName}</div>
                </div>
              </div>
              <span className="shrink-0 rounded-lg bg-white/90 px-2 py-0.5 font-mono text-xs font-extrabold text-amber-700 tabular-nums shadow-sm">
                {chartInsights.busiest.traffic.toFixed(1)} km/s
              </span>
            </div>
            <div className="flex items-center justify-between gap-2 rounded-xl bg-emerald-50/80 px-3 py-2 ring-1 ring-emerald-200/70">
              <div className="flex min-w-0 items-center gap-2">
                <TrendingUp className="h-4 w-4 shrink-0 text-emerald-600" />
                <div className="truncate">
                  <div className="text-[10px] font-semibold text-emerald-700/80">En Akıcı Bölge</div>
                  <div className="truncate text-xs font-bold text-slate-900">{chartInsights.smoothest.fullName}</div>
                </div>
              </div>
              <span className="shrink-0 rounded-lg bg-white/90 px-2 py-0.5 font-mono text-xs font-extrabold text-emerald-700 tabular-nums shadow-sm">
                {chartInsights.smoothest.traffic.toFixed(1)} km/s
              </span>
            </div>
          </div>

          <div className="mt-4 overflow-x-auto">
            <div className="h-80 min-w-[980px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} margin={{ top: 24, right: 16, left: -12, bottom: 12 }} barCategoryGap="14%">
                  <defs>
                    <linearGradient id="gradTrafficCritical" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#f43f5e" stopOpacity={1} /><stop offset="100%" stopColor="#fb7185" stopOpacity={0.82} /></linearGradient>
                    <linearGradient id="gradTrafficHeavy" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#f59e0b" stopOpacity={1} /><stop offset="100%" stopColor="#fbbf24" stopOpacity={0.82} /></linearGradient>
                    <linearGradient id="gradTrafficModerate" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#2563eb" stopOpacity={1} /><stop offset="100%" stopColor="#60a5fa" stopOpacity={0.85} /></linearGradient>
                    <linearGradient id="gradTrafficSmooth" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#059669" stopOpacity={1} /><stop offset="100%" stopColor="#34d399" stopOpacity={0.85} /></linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                  <XAxis dataKey="name" tick={X_TICK} axisLine={false} tickLine={false} interval={0} angle={-45} textAnchor="end" height={76} dy={6} />
                  <YAxis tick={Y_TICK} axisLine={false} tickLine={false} allowDecimals={false} domain={[0, (max) => Math.ceil((max + 10) / 10) * 10]} />
                  <Tooltip isAnimationActive={false} cursor={{ fill: '#f8fafc', radius: 8 }} content={<TrafficChartTooltip avgTraffic={chartInsights.avgTraffic} />} />
                  <ReferenceLine y={chartInsights.avgTraffic} stroke="#64748b" strokeDasharray="4 4" strokeWidth={1.5} label={{ value: `Ort. ${chartInsights.avgTraffic.toFixed(1)} km/s`, position: 'insideTopRight', fill: '#64748b', fontSize: 11, fontWeight: 700 }} />
                  <Bar dataKey="traffic" name="Hız" radius={[6, 6, 2, 2]} maxBarSize={26} isAnimationActive={false}>
                    {chartData.map((entry) => <Cell key={entry.fullName} fill={`url(#${getTrafficBarStyle(entry.traffic).gradId})`} />)}
                    <LabelList dataKey="traffic" position="top" offset={5} formatter={(val) => (val != null ? Number(val).toFixed(1) : '')} style={LABEL_STYLE} />
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="mt-3 flex flex-wrap items-center justify-between gap-2 border-t border-slate-100 pt-3 text-[11px] font-medium text-slate-500">
            <div className="flex flex-wrap items-center gap-3">
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-rose-500" />Çok Yoğun (&lt;14)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-amber-500" />Yoğun (14–22)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-blue-600" />Orta (22–32)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-emerald-500" />Akıcı (32+)</span>
            </div>
            <span className="inline-flex items-center gap-1.5 text-slate-400"><span className="w-4 border-t-2 border-dashed border-slate-400" />Şehir ort.</span>
          </div>
        </div>
      )}

      {/* Energy */}
      {activeChart === 'energy' && (
        <div className="perf-card relative flex flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white p-5 shadow-sm transition-shadow hover:shadow-md">
          <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-amber-400 via-orange-500 to-rose-500" />
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="flex items-center gap-3">
              <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-amber-50 text-amber-600 ring-1 ring-amber-100">
                <Zap className="h-5 w-5" />
              </span>
              <div>
                <h3 className="text-sm font-extrabold text-slate-900">Bölgelere Göre Enerji Yükü</h3>
                <p className="text-[11px] font-medium text-slate-500">Anlık bölgesel elektrik tüketimi (MW) ve Londra toplam payı</p>
              </div>
            </div>
            <div className="rounded-xl bg-amber-50/70 px-3 py-1.5 text-right ring-1 ring-amber-200/70">
              <div className="text-[10px] font-semibold uppercase tracking-wide text-amber-700/80">Toplam Yük</div>
              <div className="font-mono text-sm font-extrabold text-slate-900 tabular-nums">
                {Math.round(chartInsights.totalEnergy).toLocaleString('tr-TR')} <span className="text-[11px] font-semibold text-amber-700/70">MW</span>
              </div>
            </div>
          </div>

          <div className="mt-3.5 grid grid-cols-1 gap-2 sm:grid-cols-2">
            <div className="flex items-center justify-between gap-2 rounded-xl bg-orange-50/80 px-3 py-2 ring-1 ring-orange-200/70">
              <div className="flex min-w-0 items-center gap-2">
                <Flame className="h-4 w-4 shrink-0 text-orange-600" />
                <div className="truncate">
                  <div className="text-[10px] font-semibold text-orange-700/80">En Yüksek Tüketim</div>
                  <div className="truncate text-xs font-bold text-slate-900">
                    {chartInsights.peakEnergy.fullName} <span className="font-mono text-[11px] font-semibold text-orange-700">(%{chartInsights.peakEnergy.energyShare})</span>
                  </div>
                </div>
              </div>
              <span className="shrink-0 rounded-lg bg-white/90 px-2 py-0.5 font-mono text-xs font-extrabold text-orange-700 tabular-nums shadow-sm">
                {Math.round(chartInsights.peakEnergy.energy).toLocaleString('tr-TR')} MW
              </span>
            </div>
            <div className="flex items-center justify-between gap-2 rounded-xl bg-slate-50 px-3 py-2 ring-1 ring-slate-200/80">
              <div className="flex min-w-0 items-center gap-2">
                <Zap className="h-4 w-4 shrink-0 text-amber-500" />
                <div className="truncate">
                  <div className="text-[10px] font-semibold text-slate-500">En Düşük Tüketim</div>
                  <div className="truncate text-xs font-bold text-slate-900">
                    {chartInsights.lowestEnergy.fullName} <span className="font-mono text-[11px] font-semibold text-slate-500">(%{chartInsights.lowestEnergy.energyShare})</span>
                  </div>
                </div>
              </div>
              <span className="shrink-0 rounded-lg bg-white px-2 py-0.5 font-mono text-xs font-extrabold text-slate-700 tabular-nums shadow-sm ring-1 ring-slate-200/60">
                {Math.round(chartInsights.lowestEnergy.energy).toLocaleString('tr-TR')} MW
              </span>
            </div>
          </div>

          <div className="mt-4 overflow-x-auto">
            <div className="h-80 min-w-[980px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} margin={{ top: 24, right: 16, left: -4, bottom: 12 }} barCategoryGap="14%">
                  <defs>
                    <linearGradient id="gradEnergyPeak" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#ea580c" stopOpacity={1} /><stop offset="100%" stopColor="#fb923c" stopOpacity={0.85} /></linearGradient>
                    <linearGradient id="gradEnergyMid" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#d97706" stopOpacity={1} /><stop offset="100%" stopColor="#fbbf24" stopOpacity={0.85} /></linearGradient>
                    <linearGradient id="gradEnergyLow" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#eab308" stopOpacity={0.95} /><stop offset="100%" stopColor="#fde047" stopOpacity={0.78} /></linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                  <XAxis dataKey="name" tick={X_TICK} axisLine={false} tickLine={false} interval={0} angle={-45} textAnchor="end" height={76} dy={6} />
                  <YAxis tick={Y_TICK} axisLine={false} tickLine={false} allowDecimals={false} domain={[0, (max) => Math.ceil((max + 60) / 50) * 50]} />
                  <Tooltip isAnimationActive={false} cursor={{ fill: '#fffbeb', radius: 8 }} content={<EnergyChartTooltip avgEnergy={chartInsights.avgEnergy} />} />
                  <ReferenceLine y={chartInsights.avgEnergy} stroke="#b45309" strokeDasharray="4 4" strokeWidth={1.5} label={{ value: `Ort. ${Math.round(chartInsights.avgEnergy)} MW`, position: 'insideTopRight', fill: '#b45309', fontSize: 11, fontWeight: 700 }} />
                  <Bar dataKey="energy" name="Yük" radius={[6, 6, 2, 2]} maxBarSize={26} isAnimationActive={false}>
                    {chartData.map((entry) => <Cell key={entry.fullName} fill={`url(#${getEnergyBarStyle(entry.energy, chartInsights.avgEnergy).gradId})`} />)}
                    <LabelList dataKey="energy" position="top" offset={5} formatter={(val) => `${Math.round(Number(val))}`} style={LABEL_STYLE} />
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="mt-3 border-t border-slate-100 pt-3">
            <div className="mb-1.5 flex items-center justify-between text-[11px]">
              <span className="font-semibold text-slate-600">Londra Enerji Dağılım Payı</span>
              <span className="font-mono text-[10px] font-semibold text-slate-400">
                Bölge Ort: {Math.round(chartInsights.avgEnergy).toLocaleString('tr-TR')} MW
              </span>
            </div>
            <div className="flex h-2.5 w-full gap-0.5 overflow-hidden rounded-full bg-slate-100">
              {chartInsights.energyRanked.map((item, idx) => {
                const palette = ENERGY_SHARE_COLORS[idx % ENERGY_SHARE_COLORS.length]
                return (
                  <div
                    key={item.fullName}
                    title={`${item.fullName}: ${Math.round(item.energy)} MW (%${item.energyShare})`}
                    className={`h-full transition-all ${palette.bg}`}
                    style={{ width: `${Math.max(item.energyShare, 2)}%` }}
                  />
                )
              })}
            </div>
            <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px] font-semibold text-slate-500">
              {chartInsights.energyRanked.map((item, idx) => {
                const palette = ENERGY_SHARE_COLORS[idx % ENERGY_SHARE_COLORS.length]
                return (
                  <span key={item.fullName} className="inline-flex items-center gap-1">
                    <span className={`h-2 w-2 rounded-full ${palette.dot}`} />
                    <span className="text-slate-700">{item.name}</span>
                    <span className="font-mono text-slate-400">%{item.energyShare}</span>
                  </span>
                )
              })}
            </div>
          </div>
        </div>
      )}

      {/* Air Quality */}
      {activeChart === 'air_quality' && (
        <div className="perf-card relative flex flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white p-5 shadow-sm transition-shadow hover:shadow-md">
          <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-emerald-500 via-amber-400 to-rose-500" />
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="flex items-center gap-3">
              <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-emerald-50 text-emerald-600 ring-1 ring-emerald-100">
                <Wind className="h-5 w-5" />
              </span>
              <div>
                <h3 className="text-sm font-extrabold text-slate-900">Bölgelere Göre Hava Kalitesi</h3>
                <p className="text-[11px] font-medium text-slate-500">Düşük puan = daha temiz hava • Renkler hava temizlik seviyesini gösterir</p>
              </div>
            </div>
            <div className="rounded-xl bg-emerald-50/70 px-3 py-1.5 text-right ring-1 ring-emerald-200/70">
              <div className="text-[10px] font-semibold uppercase tracking-wide text-emerald-700/80">Londra Ort.</div>
              <div className="font-mono text-sm font-extrabold text-slate-900 tabular-nums">
                {Math.round(chartInsights.avgAqi)} <span className="text-[11px] font-semibold text-emerald-700/70">puan</span>
              </div>
            </div>
          </div>

          <div className="mt-3.5 grid grid-cols-1 gap-2 sm:grid-cols-2">
            <div className="flex items-center justify-between gap-2 rounded-xl bg-emerald-50/80 px-3 py-2 ring-1 ring-emerald-200/70">
              <div className="flex min-w-0 items-center gap-2">
                <CheckCircle2 className="h-4 w-4 shrink-0 text-emerald-600" />
                <div className="truncate">
                  <div className="text-[10px] font-semibold text-emerald-700/80">En Temiz Hava</div>
                  <div className="truncate text-xs font-bold text-slate-900">{chartInsights.cleanestAqi.fullName}</div>
                </div>
              </div>
              <span className="shrink-0 rounded-lg bg-white/90 px-2 py-0.5 font-mono text-xs font-extrabold text-emerald-700 tabular-nums shadow-sm">
                {Math.round(chartInsights.cleanestAqi.aqi)} puan
              </span>
            </div>
            <div className="flex items-center justify-between gap-2 rounded-xl bg-amber-50/80 px-3 py-2 ring-1 ring-amber-200/70">
              <div className="flex min-w-0 items-center gap-2">
                <AlertTriangle className="h-4 w-4 shrink-0 text-amber-600" />
                <div className="truncate">
                  <div className="text-[10px] font-semibold text-amber-700/80">En Yoğun Hava Kirliliği</div>
                  <div className="truncate text-xs font-bold text-slate-900">{chartInsights.highestAqi.fullName}</div>
                </div>
              </div>
              <span className="shrink-0 rounded-lg bg-white/90 px-2 py-0.5 font-mono text-xs font-extrabold text-amber-700 tabular-nums shadow-sm">
                {Math.round(chartInsights.highestAqi.aqi)} puan
              </span>
            </div>
          </div>

          <div className="mt-4 overflow-x-auto">
            <div className="h-80 min-w-[980px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} margin={{ top: 24, right: 16, left: -12, bottom: 12 }} barCategoryGap="14%">
                  <defs>
                    <linearGradient id="gradAqiVeryGood" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#059669" stopOpacity={1} /><stop offset="100%" stopColor="#34d399" stopOpacity={0.85} /></linearGradient>
                    <linearGradient id="gradAqiGood" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#10b981" stopOpacity={0.95} /><stop offset="100%" stopColor="#6ee7b7" stopOpacity={0.8} /></linearGradient>
                    <linearGradient id="gradAqiModerate" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#f59e0b" stopOpacity={1} /><stop offset="100%" stopColor="#fcd34d" stopOpacity={0.82} /></linearGradient>
                    <linearGradient id="gradAqiSensitive" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#ea580c" stopOpacity={1} /><stop offset="100%" stopColor="#fb923c" stopOpacity={0.82} /></linearGradient>
                    <linearGradient id="gradAqiBad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#e11d48" stopOpacity={1} /><stop offset="100%" stopColor="#fb7185" stopOpacity={0.85} /></linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                  <XAxis dataKey="name" tick={X_TICK} axisLine={false} tickLine={false} interval={0} angle={-45} textAnchor="end" height={76} dy={6} />
                  <YAxis tick={Y_TICK} axisLine={false} tickLine={false} allowDecimals={false} domain={[0, (max) => Math.ceil((max + 15) / 10) * 10]} />
                  <Tooltip isAnimationActive={false} cursor={{ fill: '#ecfdf5', radius: 8 }} content={<AqiChartTooltip avgAqi={chartInsights.avgAqi} />} />
                  <ReferenceLine y={chartInsights.avgAqi} stroke="#047857" strokeDasharray="4 4" strokeWidth={1.5} label={{ value: `Ort. ${Math.round(chartInsights.avgAqi)} puan`, position: 'insideTopRight', fill: '#047857', fontSize: 11, fontWeight: 700 }} />
                  <Bar dataKey="aqi" name="Hava Puanı" radius={[6, 6, 2, 2]} maxBarSize={26} isAnimationActive={false}>
                    {chartData.map((entry) => <Cell key={entry.fullName} fill={`url(#${getAqiBarStyle(entry.aqi).gradId})`} />)}
                    <LabelList dataKey="aqi" position="top" offset={5} formatter={(val) => (val != null ? `${Math.round(Number(val))}` : '')} style={LABEL_STYLE} />
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="mt-3 flex flex-wrap items-center justify-between gap-2 border-t border-slate-100 pt-3 text-[11px] font-medium text-slate-500">
            <div className="flex flex-wrap items-center gap-3">
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-emerald-600" />İyi (0–20)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-emerald-400" />Makul (21–40)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-amber-400" />Orta (41–60)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-orange-500" />Kötü (61–80)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-rose-500" />Çok Kötü (80+)</span>
            </div>
            <span className="inline-flex items-center gap-1.5 text-slate-400"><span className="w-4 border-t-2 border-dashed border-emerald-600" />Şehir ort.</span>
          </div>
        </div>
      )}

      {/* Weather / Temperature */}
      {activeChart === 'weather' && (
        <div className="perf-card relative flex flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white p-5 shadow-sm transition-shadow hover:shadow-md">
          <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-blue-500 via-cyan-500 to-orange-400" />
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="flex items-center gap-3">
              <span className="flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-cyan-50 text-cyan-600 ring-1 ring-cyan-100">
                <Thermometer className="h-5 w-5" />
              </span>
              <div>
                <h3 className="text-sm font-extrabold text-slate-900">Bölgelere Göre Sıcaklık</h3>
                <p className="text-[11px] font-medium text-slate-500">Bölgeler arası anlık sıcaklık ölçümü (°C) ve karşılaştırması</p>
              </div>
            </div>
            <div className="rounded-xl bg-cyan-50/70 px-3 py-1.5 text-right ring-1 ring-cyan-200/70">
              <div className="text-[10px] font-semibold uppercase tracking-wide text-cyan-700/80">Londra Ort.</div>
              <div className="font-mono text-sm font-extrabold text-slate-900 tabular-nums">
                {chartInsights.avgTemp.toFixed(1)} <span className="text-[11px] font-semibold text-cyan-700/70">°C</span>
              </div>
            </div>
          </div>

          <div className="mt-3.5 grid grid-cols-1 gap-2 sm:grid-cols-2">
            <div className="flex items-center justify-between gap-2 rounded-xl bg-cyan-50/80 px-3 py-2 ring-1 ring-cyan-200/70">
              <div className="flex min-w-0 items-center gap-2">
                <TrendingDown className="h-4 w-4 shrink-0 text-cyan-600" />
                <div className="truncate">
                  <div className="text-[10px] font-semibold text-cyan-700/80">En Serin Bölge</div>
                  <div className="truncate text-xs font-bold text-slate-900">{chartInsights.coolestTemp.fullName}</div>
                </div>
              </div>
              <span className="shrink-0 rounded-lg bg-white/90 px-2 py-0.5 font-mono text-xs font-extrabold text-cyan-700 tabular-nums shadow-sm">
                {chartInsights.coolestTemp.temp.toFixed(1)} °C
              </span>
            </div>
            <div className="flex items-center justify-between gap-2 rounded-xl bg-orange-50/80 px-3 py-2 ring-1 ring-orange-200/70">
              <div className="flex min-w-0 items-center gap-2">
                <TrendingUp className="h-4 w-4 shrink-0 text-orange-600" />
                <div className="truncate">
                  <div className="text-[10px] font-semibold text-orange-700/80">En Sıcak Bölge</div>
                  <div className="truncate text-xs font-bold text-slate-900">{chartInsights.warmestTemp.fullName}</div>
                </div>
              </div>
              <span className="shrink-0 rounded-lg bg-white/90 px-2 py-0.5 font-mono text-xs font-extrabold text-orange-700 tabular-nums shadow-sm">
                {chartInsights.warmestTemp.temp.toFixed(1)} °C
              </span>
            </div>
          </div>

          <div className="mt-4 overflow-x-auto">
            <div className="h-80 min-w-[980px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={chartData} margin={{ top: 24, right: 16, left: -12, bottom: 12 }} barCategoryGap="14%">
                  <defs>
                    <linearGradient id="gradTempCold" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#2563eb" stopOpacity={1} /><stop offset="100%" stopColor="#60a5fa" stopOpacity={0.85} /></linearGradient>
                    <linearGradient id="gradTempCool" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#0891b2" stopOpacity={1} /><stop offset="100%" stopColor="#22d3ee" stopOpacity={0.82} /></linearGradient>
                    <linearGradient id="gradTempMild" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#059669" stopOpacity={1} /><stop offset="100%" stopColor="#34d399" stopOpacity={0.85} /></linearGradient>
                    <linearGradient id="gradTempWarm" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#ea580c" stopOpacity={1} /><stop offset="100%" stopColor="#fb923c" stopOpacity={0.85} /></linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" vertical={false} />
                  <XAxis dataKey="name" tick={X_TICK} axisLine={false} tickLine={false} interval={0} angle={-45} textAnchor="end" height={76} dy={6} />
                  <YAxis tick={Y_TICK} axisLine={false} tickLine={false} allowDecimals={false} tickFormatter={(v) => `${v}°`} domain={[(min) => Math.min(0, Math.floor(min - 2)), (max) => Math.ceil(max + 4)]} />
                  <Tooltip isAnimationActive={false} cursor={{ fill: '#ecfeff', radius: 8 }} content={<TempChartTooltip avgTemp={chartInsights.avgTemp} />} />
                  <ReferenceLine y={chartInsights.avgTemp} stroke="#0e7490" strokeDasharray="4 4" strokeWidth={1.5} label={{ value: `Ort. ${chartInsights.avgTemp.toFixed(1)} °C`, position: 'insideTopRight', fill: '#0e7490', fontSize: 11, fontWeight: 700 }} />
                  <Bar dataKey="temp" name="Sıcaklık" radius={[6, 6, 2, 2]} maxBarSize={26} isAnimationActive={false}>
                    {chartData.map((entry) => <Cell key={entry.fullName} fill={`url(#${getTempBarStyle(entry.temp).gradId})`} />)}
                    <LabelList dataKey="temp" position="top" offset={5} formatter={(val) => (val != null ? `${Number(val).toFixed(1)}°` : '')} style={LABEL_STYLE} />
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="mt-3 flex flex-wrap items-center justify-between gap-2 border-t border-slate-100 pt-3 text-[11px] font-medium text-slate-500">
            <div className="flex flex-wrap items-center gap-3">
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-blue-600" />Soğuk (≤4°C)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-cyan-500" />Serin (4–13°C)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-emerald-500" />Ilık (13–23°C)</span>
              <span className="inline-flex items-center gap-1.5"><span className="h-2.5 w-2.5 rounded-full bg-orange-500" />Sıcak (23°C+)</span>
            </div>
            <span className="inline-flex items-center gap-1.5 text-slate-400"><span className="w-4 border-t-2 border-dashed border-cyan-600" />Şehir ort.</span>
          </div>
        </div>
      )}
    </div>
  )
}

export default memo(DistrictComparisonPanel)
