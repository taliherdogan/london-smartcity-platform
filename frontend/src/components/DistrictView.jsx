import React, { memo, useEffect, useMemo, useState } from 'react'
import { AlertTriangle, CheckCircle2, ChevronLeft, ChevronRight, Search } from 'lucide-react'
import DistrictCard from './district/DistrictCard'
import DistrictComparisonPanel from './district/DistrictComparisonPanel'
import { ZONE_LABELS, formatDistrictShortName } from './district/districtHelpers'

const STATUS_FILTERS = [
  { id: 'all', label: 'Tümü' },
  { id: 'alert', label: 'Dikkat / Uyarı' },
  { id: 'normal', label: 'Normal' },
]

const PAGE_SIZE_OPTIONS = [6, 12]

function DistrictView({ districts = [] }) {
  const [query, setQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState('all')
  const [pageSize, setPageSize] = useState(6)
  const [page, setPage] = useState(1)

  const stats = useMemo(() => {
    let normal = 0
    let attention = 0
    let critical = 0
    let totalEnergy = 0
    let maxEnergy = 1

    for (const d of districts) {
      const hasDisruption = (d.tfl_disruptions || 0) > 0
      if (d.status === 'critical') critical += 1
      else if (d.status === 'warning' || hasDisruption) attention += 1
      else normal += 1

      const e = d.metrics?.energy?.avg ?? 0
      totalEnergy += e
      if (e > maxEnergy) maxEnergy = e
    }
    const avgEnergy = districts.length > 0 ? totalEnergy / districts.length : 0
    return { normal, attention, critical, totalEnergy, maxEnergy, avgEnergy }
  }, [districts])

  const filteredDistricts = useMemo(() => {
    const q = query.trim().toLocaleLowerCase('tr-TR')
    return districts.filter((d) => {
      const hasDisruption = (d.tfl_disruptions || 0) > 0
      const isAlert = d.status === 'critical' || d.status === 'warning' || hasDisruption
      if (statusFilter === 'alert' && !isAlert) return false
      if (statusFilter === 'normal' && isAlert) return false
      if (!q) return true
      const zoneLabel = (ZONE_LABELS[d.zone_type] || d.zone_type || '').toLocaleLowerCase('tr-TR')
      const corridors = (d.tfl_corridors || []).join(' ').toLocaleLowerCase('tr-TR')
      return (
        d.district_name.toLocaleLowerCase('tr-TR').includes(q) ||
        zoneLabel.includes(q) ||
        corridors.includes(q)
      )
    })
  }, [districts, query, statusFilter])

  const totalPages = Math.max(1, Math.ceil(filteredDistricts.length / pageSize))
  const safePage = Math.min(page, totalPages)

  useEffect(() => {
    setPage(1)
  }, [query, statusFilter, pageSize])

  const paginatedDistricts = useMemo(() => {
    const start = (safePage - 1) * pageSize
    return filteredDistricts.slice(start, start + pageSize)
  }, [filteredDistricts, safePage, pageSize])

  const rangeStart = filteredDistricts.length === 0 ? 0 : (safePage - 1) * pageSize + 1
  const rangeEnd = Math.min(safePage * pageSize, filteredDistricts.length)

  const chartData = useMemo(() => {
    const totalE = districts.reduce((acc, d) => acc + (d.metrics?.energy?.avg ?? 0), 0)
    return districts.map((d) => {
      const rawTraffic = d.metrics?.traffic?.avg
      const rawAqi = d.metrics?.air_quality?.avg
      const rawEnergy = d.metrics?.energy?.avg
      const rawTemp = d.metrics?.weather?.avg
      const energy = rawEnergy != null ? Number(rawEnergy.toFixed(1)) : 0
      return {
        name: formatDistrictShortName(d.district_name),
        fullName: d.district_name,
        traffic: rawTraffic != null ? Number(rawTraffic.toFixed(1)) : null,
        aqi: rawAqi != null ? Number(rawAqi.toFixed(1)) : null,
        energy,
        hasEnergy: rawEnergy != null,
        temp: rawTemp != null ? Number(rawTemp.toFixed(1)) : null,
        energyShare: totalE > 0 && rawEnergy != null ? Math.round((energy / totalE) * 100) : 0,
      }
    })
  }, [districts])

  const chartInsights = useMemo(() => {
    if (!chartData.length) return null
    const trafficItems = chartData.filter((item) => item.traffic != null)
    const energyItems = chartData.filter((item) => item.hasEnergy)
    const aqiItems = chartData.filter((item) => item.aqi != null)
    const tempItems = chartData.filter((item) => item.temp != null)

    const avgTraffic = trafficItems.length
      ? trafficItems.reduce((sum, item) => sum + item.traffic, 0) / trafficItems.length
      : 0
    const totalEnergy = energyItems.reduce((sum, item) => sum + item.energy, 0)
    const avgEnergy = energyItems.length ? totalEnergy / energyItems.length : 0
    const avgAqi = aqiItems.length
      ? aqiItems.reduce((sum, item) => sum + item.aqi, 0) / aqiItems.length
      : 0
    const avgTemp = tempItems.length
      ? tempItems.reduce((sum, item) => sum + item.temp, 0) / tempItems.length
      : 0

    let busiest = trafficItems[0] || { ...chartData[0], traffic: 0 }
    let smoothest = trafficItems[0] || { ...chartData[0], traffic: 0 }
    for (const item of trafficItems) {
      if (item.traffic < busiest.traffic) busiest = item
      if (item.traffic > smoothest.traffic) smoothest = item
    }

    let peakEnergy = energyItems[0] || chartData[0]
    let lowestEnergy = energyItems[0] || chartData[0]
    for (const item of energyItems) {
      if (item.energy > peakEnergy.energy) peakEnergy = item
      if (item.energy < lowestEnergy.energy) lowestEnergy = item
    }

    let cleanestAqi = aqiItems[0] || { ...chartData[0], aqi: 0 }
    let highestAqi = aqiItems[0] || { ...chartData[0], aqi: 0 }
    for (const item of aqiItems) {
      if (item.aqi < cleanestAqi.aqi) cleanestAqi = item
      if (item.aqi > highestAqi.aqi) highestAqi = item
    }

    let coolestTemp = tempItems[0] || { ...chartData[0], temp: 0 }
    let warmestTemp = tempItems[0] || { ...chartData[0], temp: 0 }
    for (const item of tempItems) {
      if (item.temp < coolestTemp.temp) coolestTemp = item
      if (item.temp > warmestTemp.temp) warmestTemp = item
    }

    const energyRanked = [...chartData].sort((a, b) => b.energy - a.energy)

    return {
      avgTraffic,
      totalEnergy,
      avgEnergy,
      avgAqi,
      avgTemp,
      busiest,
      smoothest,
      peakEnergy,
      lowestEnergy,
      cleanestAqi,
      highestAqi,
      coolestTemp,
      warmestTemp,
      energyRanked,
    }
  }, [chartData])

  return (
    <div className="space-y-5">
      {/* Top Overview Bar */}
      <div className="relative flex flex-wrap items-center justify-between gap-3 overflow-hidden rounded-2xl border border-slate-200/90 bg-gradient-to-r from-white via-slate-50/40 to-white px-4 py-3 shadow-sm">
        <div className="flex flex-wrap items-center gap-2.5">
          <span className="inline-flex items-center gap-2 text-xs font-extrabold text-slate-800">
            <span className="relative flex h-2.5 w-2.5">
              <span className="live-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75" />
              <span className="relative inline-flex h-2.5 w-2.5 rounded-full bg-emerald-500" />
            </span>
            Londra {districts.length} Bölge Özeti:
          </span>
          <span className="inline-flex items-center gap-1 rounded-full bg-emerald-50 px-2.5 py-0.5 text-xs font-semibold text-emerald-700 ring-1 ring-emerald-200/80">
            <CheckCircle2 className="h-3.5 w-3.5" />
            {stats.normal} Normal
          </span>
          {stats.attention > 0 && (
            <span className="inline-flex items-center gap-1 rounded-full bg-amber-50 px-2.5 py-0.5 text-xs font-semibold text-amber-800 ring-1 ring-amber-200/80">
              <AlertTriangle className="h-3.5 w-3.5" />
              {stats.attention} Dikkat / Yoğun
            </span>
          )}
          {stats.critical > 0 && (
            <span className="inline-flex items-center gap-1 rounded-full bg-rose-50 px-2.5 py-0.5 text-xs font-semibold text-rose-700 ring-1 ring-rose-200/80">
              <AlertTriangle className="h-3.5 w-3.5" />
              {stats.critical} Yüksek Uyarı
            </span>
          )}
        </div>

        {/* Quick Search & Status Filter */}
        <div className="flex flex-wrap items-center gap-2">
          <div className="relative min-w-[180px] sm:w-52">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-400" />
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Bölge veya yol ara..."
              className="w-full rounded-xl border border-slate-200/90 bg-white py-1.5 pl-8 pr-3 text-xs font-medium text-slate-700 placeholder:text-slate-400 focus:border-blue-400 focus:outline-none focus:ring-2 focus:ring-blue-100"
            />
          </div>
          <div className="flex items-center gap-0.5 rounded-xl bg-slate-100/90 p-0.5 ring-1 ring-slate-200/70">
            {STATUS_FILTERS.map((f) => {
              const active = statusFilter === f.id
              return (
                <button
                  key={f.id}
                  type="button"
                  onClick={() => setStatusFilter(f.id)}
                  className={`rounded-lg px-2.5 py-1 text-[11px] font-bold transition-colors ${
                    active
                      ? 'bg-white text-slate-900 shadow-sm'
                      : 'text-slate-600 hover:text-slate-900'
                  }`}
                >
                  {f.label}
                </button>
              )
            })}
          </div>
        </div>
      </div>

      {/* District Cards (Paginated to Keep Page Height Compact) */}
      {filteredDistricts.length === 0 ? (
        <div className="rounded-2xl border border-dashed border-slate-200 bg-white p-8 text-center text-xs font-medium text-slate-400">
          Filtreye uyan bölge bulunamadı.
        </div>
      ) : (
        <div className="space-y-4">
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
            {paginatedDistricts.map((d) => (
              <DistrictCard
                key={d.district_id}
                district={d}
                stats={stats}
              />
            ))}
          </div>

          {/* Pagination Bar */}
          {filteredDistricts.length > PAGE_SIZE_OPTIONS[0] && (
            <div className="flex flex-wrap items-center justify-between gap-3 rounded-2xl border border-slate-200/90 bg-white px-4 py-2.5 shadow-sm">
              <div className="flex flex-wrap items-center gap-3 text-xs font-medium text-slate-500">
                <span className="font-mono tabular-nums">
                  <strong className="font-bold text-slate-800">
                    {rangeStart}–{rangeEnd}
                  </strong>{' '}
                  / {filteredDistricts.length} bölge
                </span>
                <div className="flex items-center gap-1 rounded-lg bg-slate-100/90 p-0.5 ring-1 ring-slate-200/70">
                  {PAGE_SIZE_OPTIONS.map((size) => {
                    const active = pageSize === size
                    return (
                      <button
                        key={size}
                        type="button"
                        onClick={() => setPageSize(size)}
                        className={`rounded-md px-2 py-0.5 text-[11px] font-bold transition-colors ${
                          active
                            ? 'bg-white text-slate-900 shadow-sm'
                            : 'text-slate-500 hover:text-slate-800'
                        }`}
                      >
                        {size} kart
                      </button>
                    )
                  })}
                </div>
              </div>

              {totalPages > 1 && (
                <div className="flex items-center gap-1">
                  <button
                    type="button"
                    disabled={safePage <= 1}
                    onClick={() => setPage((p) => Math.max(1, p - 1))}
                    className="inline-flex items-center gap-1 rounded-lg border border-slate-200/90 bg-white px-2.5 py-1 text-xs font-bold text-slate-700 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
                  >
                    <ChevronLeft className="h-3.5 w-3.5" />
                    Önceki
                  </button>

                  <div className="flex items-center gap-1 px-1">
                    {Array.from({ length: totalPages }, (_, idx) => idx + 1).map((pNum) => {
                      const active = pNum === safePage
                      return (
                        <button
                          key={pNum}
                          type="button"
                          onClick={() => setPage(pNum)}
                          className={`flex h-7 w-7 items-center justify-center rounded-lg font-mono text-xs font-bold tabular-nums transition-colors ${
                            active
                              ? 'bg-gradient-to-r from-blue-600 to-indigo-600 text-white shadow-sm shadow-blue-500/25'
                              : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
                          }`}
                        >
                          {pNum}
                        </button>
                      )
                    })}
                  </div>

                  <button
                    type="button"
                    disabled={safePage >= totalPages}
                    onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                    className="inline-flex items-center gap-1 rounded-lg border border-slate-200/90 bg-white px-2.5 py-1 text-xs font-bold text-slate-700 transition-colors hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40"
                  >
                    Sonraki
                    <ChevronRight className="h-3.5 w-3.5" />
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* Comparison Charts with User Selector */}
      {chartData.length > 0 && chartInsights && (
        <DistrictComparisonPanel chartData={chartData} chartInsights={chartInsights} />
      )}
    </div>
  )
}

export default memo(DistrictView)
