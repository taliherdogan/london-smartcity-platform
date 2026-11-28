import React, { memo } from 'react'
import { MapPin, Thermometer, TrafficCone, Wind, Zap } from 'lucide-react'
import {
  ZONE_LABELS,
  getAqiLevel,
  getEnergyLevel,
  getTempLevel,
  getTrafficLevel,
} from './districtHelpers'

function DistrictCard({ district: d, stats }) {
  const hasTfLDisruption = (d.tfl_disruptions || 0) > 0
  const isCritical = d.status === 'critical'
  const isWarning = d.status === 'warning'
  const isAlert = isCritical || isWarning || hasTfLDisruption

  const cardTheme = isCritical
    ? 'border-rose-300/90 bg-gradient-to-br from-rose-50/60 via-white to-white ring-1 ring-rose-100 hover:border-rose-300'
    : isWarning || hasTfLDisruption
      ? 'border-amber-200/90 bg-gradient-to-br from-amber-50/55 via-white to-white ring-1 ring-amber-100/70 hover:border-amber-300'
      : 'border-slate-200/90 bg-gradient-to-br from-blue-50/35 via-white to-white hover:border-blue-200/90'

  const topAccent = isCritical
    ? 'bg-gradient-to-r from-rose-600 via-rose-500 to-orange-400'
    : isWarning || hasTfLDisruption
      ? 'bg-gradient-to-r from-amber-500 via-orange-400 to-yellow-400'
      : 'bg-gradient-to-r from-blue-600 via-sky-500 to-emerald-400'

  const iconBox = isCritical
    ? 'bg-gradient-to-br from-rose-600 to-rose-500 text-white shadow-sm shadow-rose-200'
    : isWarning || hasTfLDisruption
      ? 'bg-gradient-to-br from-amber-500 to-orange-500 text-white shadow-sm shadow-amber-200'
      : 'bg-gradient-to-br from-blue-600 to-sky-500 text-white shadow-sm shadow-blue-200'

  const badge = isCritical
    ? 'bg-rose-100 text-rose-700 ring-1 ring-rose-200'
    : isWarning || hasTfLDisruption
      ? 'bg-amber-100 text-amber-800 ring-1 ring-amber-200'
      : 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/80'

  const badgeDot = isCritical
    ? 'bg-rose-500'
    : isWarning || hasTfLDisruption
      ? 'bg-amber-500'
      : 'bg-emerald-500'

  const badgeText = isCritical
    ? 'Yüksek Uyarı'
    : isWarning
      ? 'Yoğunluk Uyarısı'
      : hasTfLDisruption
        ? `${d.tfl_disruptions} Yolda Gecikme`
        : 'Normal'

  const speed = d.metrics?.traffic?.avg
  const aqi = d.metrics?.air_quality?.avg
  const energy = d.metrics?.energy?.avg
  const temp = d.metrics?.weather?.avg

  const trafficLvl = getTrafficLevel(speed)
  const aqiLvl = getAqiLevel(aqi)
  const energyLvl = getEnergyLevel(energy, stats.avgEnergy)
  const tempLvl = getTempLevel(temp)
  const energySharePct =
    energy != null && stats.totalEnergy > 0 ? Math.round((energy / stats.totalEnergy) * 100) : 0
  const energyBarPct =
    energy != null ? Math.min(100, Math.round((energy / stats.maxEnergy) * 100)) : 0

  return (
    <div
      className={`perf-card group relative flex flex-col overflow-hidden rounded-2xl border p-5 shadow-sm transition-[transform,box-shadow] duration-200 hover:-translate-y-0.5 hover:shadow-lg ${cardTheme}`}
    >
      <div className={`absolute inset-x-0 top-0 h-1.5 ${topAccent}`} />

      <div className="flex items-start justify-between gap-2 pt-0.5">
        <div className="flex min-w-0 items-center gap-3">
          <span
            className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl transition-transform duration-200 group-hover:scale-105 ${iconBox}`}
          >
            <MapPin className="h-5 w-5" />
          </span>
          <div className="min-w-0">
            <h3 className="truncate text-sm font-extrabold tracking-tight text-slate-900">
              {d.district_name}
            </h3>
            <p className="truncate text-[11px] font-medium text-slate-500">
              {ZONE_LABELS[d.zone_type] || d.zone_type}
            </p>
          </div>
        </div>
        <span className={`inline-flex shrink-0 items-center gap-1.5 rounded-full px-2.5 py-1 text-[11px] font-bold ${badge}`}>
          <span className="relative flex h-2 w-2">
            {isAlert && (
              <span className={`live-ping absolute inline-flex h-full w-full rounded-full opacity-75 ${badgeDot}`} />
            )}
            <span className={`relative inline-flex h-2 w-2 rounded-full ${badgeDot}`} />
          </span>
          {badgeText}
        </span>
      </div>

      <div className="mt-3.5 grid grid-cols-2 gap-2.5">
        {/* Traffic */}
        <div className="rounded-xl bg-gradient-to-br from-blue-50/80 via-white to-white p-3 ring-1 ring-blue-100/90 transition-colors hover:ring-blue-200">
          <div className="flex items-center justify-between gap-1">
            <span className="inline-flex items-center gap-1.5 text-[11px] font-bold text-slate-600">
              <span className="flex h-5 w-5 items-center justify-center rounded-md bg-blue-100 text-blue-600">
                <TrafficCone className="h-3 w-3" />
              </span>
              Trafik
            </span>
            <span className={`rounded-full px-1.5 py-0.5 text-[10px] font-bold ${trafficLvl.badge}`}>
              {trafficLvl.label}
            </span>
          </div>
          <div className="mt-2 flex items-baseline gap-1 font-mono text-lg font-extrabold tracking-tight text-slate-900 tabular-nums">
            <span>{speed?.toFixed(1) ?? '—'}</span>
            <span className="text-[11px] font-semibold text-slate-400">km/s</span>
          </div>
          <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-blue-100/80">
            <div
              className={`h-full w-full origin-left rounded-full transition-transform duration-300 ${trafficLvl.bar}`}
              style={{ transform: `scaleX(${trafficLvl.pct / 100})` }}
            />
          </div>
        </div>

        {/* Air Quality */}
        <div className="rounded-xl bg-gradient-to-br from-emerald-50/80 via-white to-white p-3 ring-1 ring-emerald-100/90 transition-colors hover:ring-emerald-200">
          <div className="flex items-center justify-between gap-1">
            <span className="inline-flex items-center gap-1.5 text-[11px] font-bold text-slate-600">
              <span className="flex h-5 w-5 items-center justify-center rounded-md bg-emerald-100 text-emerald-600">
                <Wind className="h-3 w-3" />
              </span>
              Hava
            </span>
            <span className={`rounded-full px-1.5 py-0.5 text-[10px] font-bold ${aqiLvl.badge}`}>
              {aqiLvl.label}
            </span>
          </div>
          <div className="mt-2 flex items-baseline gap-1 font-mono text-lg font-extrabold tracking-tight text-slate-900 tabular-nums">
            <span>{aqi != null ? Math.round(aqi) : '—'}</span>
            <span className="text-[11px] font-semibold text-slate-400">puan</span>
          </div>
          <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-emerald-100/80">
            <div
              className={`h-full w-full origin-left rounded-full transition-transform duration-300 ${aqiLvl.bar}`}
              style={{ transform: `scaleX(${aqiLvl.pct / 100})` }}
            />
          </div>
        </div>

        {/* Energy */}
        <div className="rounded-xl bg-gradient-to-br from-amber-50/80 via-white to-white p-3 ring-1 ring-amber-100/90 transition-colors hover:ring-amber-200">
          <div className="flex items-center justify-between gap-1">
            <span className="inline-flex items-center gap-1.5 text-[11px] font-bold text-slate-600">
              <span className="flex h-5 w-5 items-center justify-center rounded-md bg-amber-100 text-amber-600">
                <Zap className="h-3 w-3" />
              </span>
              Enerji
            </span>
            <span className={`rounded-full px-1.5 py-0.5 text-[10px] font-bold ${energyLvl.badge}`}>
              {energyLvl.label}
            </span>
          </div>
          <div className="mt-2 flex items-baseline gap-1 font-mono text-lg font-extrabold tracking-tight text-slate-900 tabular-nums">
            <span>{energy != null ? `%${energySharePct}` : '—'}</span>
            <span className="text-[11px] font-semibold text-slate-400">şehir payı</span>
          </div>
          <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-amber-100/80">
            <div
              className={`h-full w-full origin-left rounded-full transition-transform duration-300 ${energyLvl.bar}`}
              style={{ transform: `scaleX(${energyBarPct / 100})` }}
            />
          </div>
        </div>

        {/* Temperature */}
        <div className="rounded-xl bg-gradient-to-br from-cyan-50/80 via-white to-white p-3 ring-1 ring-cyan-100/90 transition-colors hover:ring-cyan-200">
          <div className="flex items-center justify-between gap-1">
            <span className="inline-flex items-center gap-1.5 text-[11px] font-bold text-slate-600">
              <span className="flex h-5 w-5 items-center justify-center rounded-md bg-cyan-100 text-cyan-600">
                <Thermometer className="h-3 w-3" />
              </span>
              Sıcaklık
            </span>
            <span className={`rounded-full px-1.5 py-0.5 text-[10px] font-bold ${tempLvl.badge}`}>
              {tempLvl.label}
            </span>
          </div>
          <div className="mt-2 flex items-baseline gap-1 font-mono text-lg font-extrabold tracking-tight text-slate-900 tabular-nums">
            <span>{temp?.toFixed(1) ?? '—'}</span>
            <span className="text-[11px] font-semibold text-slate-400">°C</span>
          </div>
          <div className="mt-2 h-1.5 w-full overflow-hidden rounded-full bg-cyan-100/80">
            <div
              className={`h-full w-full origin-left rounded-full transition-transform duration-300 ${tempLvl.bar}`}
              style={{ transform: `scaleX(${tempLvl.pct / 100})` }}
            />
          </div>
        </div>
      </div>
    </div>
  )
}

export default memo(DistrictCard)
