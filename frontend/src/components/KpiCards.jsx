import React, { memo } from 'react'
import { Car, CloudSun, Droplets, Gauge, Leaf, Route, Wind, Zap } from 'lucide-react'
import { formatInt } from '../utils/format'

function getTrafficStatus(speed) {
  if (speed == null) return { label: 'Bekleniyor', badge: 'bg-slate-100 text-slate-500', bar: 'bg-slate-300', pct: 0 }
  if (speed < 14) return { label: 'Çok Yoğun', badge: 'bg-rose-100 text-rose-700 ring-1 ring-rose-200', bar: 'bg-gradient-to-r from-rose-500 to-rose-400', pct: 22 }
  if (speed < 22) return { label: 'Yoğun', badge: 'bg-amber-100 text-amber-800 ring-1 ring-amber-200', bar: 'bg-gradient-to-r from-amber-500 to-amber-400', pct: 48 }
  if (speed < 32) return { label: 'Orta Akış', badge: 'bg-blue-100 text-blue-700 ring-1 ring-blue-200', bar: 'bg-gradient-to-r from-blue-600 to-sky-400', pct: 74 }
  return { label: 'Akıcı', badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200', bar: 'bg-gradient-to-r from-emerald-500 to-teal-400', pct: 94 }
}

function getWeatherStatus(temp) {
  if (temp == null) return { label: 'Bekleniyor', badge: 'bg-slate-100 text-slate-500', bar: 'bg-slate-300', pct: 0 }
  const pct = Math.max(10, Math.min(100, Math.round(((temp + 5) / 40) * 100)))
  if (temp <= 4) return { label: 'Soğuk', badge: 'bg-blue-100 text-blue-700 ring-1 ring-blue-200', bar: 'bg-gradient-to-r from-blue-600 to-sky-400', pct }
  if (temp <= 13) return { label: 'Serin', badge: 'bg-cyan-100 text-cyan-800 ring-1 ring-cyan-200', bar: 'bg-gradient-to-r from-cyan-500 to-sky-400', pct }
  if (temp <= 23) return { label: 'Ilık', badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200', bar: 'bg-gradient-to-r from-emerald-500 to-teal-400', pct }
  return { label: 'Sıcak', badge: 'bg-orange-100 text-orange-800 ring-1 ring-orange-200', bar: 'bg-gradient-to-r from-orange-500 to-amber-400', pct }
}

function getAqiStatus(aqi) {
  if (aqi == null) return { label: 'Bekleniyor', badge: 'bg-slate-100 text-slate-500', bar: 'bg-slate-300', pct: 0 }
  if (aqi <= 20) return { label: 'İyi', badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200', bar: 'bg-gradient-to-r from-emerald-600 to-emerald-400', pct: 20 }
  if (aqi <= 40) return { label: 'Makul', badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200', bar: 'bg-gradient-to-r from-emerald-500 to-teal-400', pct: 40 }
  if (aqi <= 60) return { label: 'Orta', badge: 'bg-amber-100 text-amber-800 ring-1 ring-amber-200', bar: 'bg-gradient-to-r from-amber-500 to-yellow-400', pct: 64 }
  if (aqi <= 80) return { label: 'Kötü', badge: 'bg-orange-100 text-orange-800 ring-1 ring-orange-200', bar: 'bg-gradient-to-r from-orange-500 to-amber-400', pct: 84 }
  return { label: 'Çok Kötü', badge: 'bg-rose-100 text-rose-700 ring-1 ring-rose-200', bar: 'bg-gradient-to-r from-rose-600 to-rose-400', pct: 100 }
}

function getEnergyStatus(mw) {
  if (mw == null) return { label: 'Bekleniyor', badge: 'bg-slate-100 text-slate-500', bar: 'bg-slate-300', pct: 0 }
  const pct = Math.max(15, Math.min(100, Math.round((mw / 250) * 100)))
  if (mw >= 195) return { label: 'Yüksek Yük', badge: 'bg-orange-100 text-orange-800 ring-1 ring-orange-200', bar: 'bg-gradient-to-r from-orange-600 to-amber-400', pct }
  if (mw >= 130) return { label: 'Normal Yük', badge: 'bg-amber-100 text-amber-800 ring-1 ring-amber-200', bar: 'bg-gradient-to-r from-amber-500 to-yellow-400', pct }
  return { label: 'Dengeli', badge: 'bg-emerald-100 text-emerald-700 ring-1 ring-emerald-200', bar: 'bg-gradient-to-r from-emerald-500 to-amber-400', pct }
}

const CARD_META = [
  {
    key: 'traffic',
    title: 'Trafik Akışı',
    unit: 'km/s',
    icon: Car,
    topBar: 'bg-gradient-to-r from-blue-600 via-sky-500 to-cyan-400',
    cardGlow: 'bg-gradient-to-br from-blue-50/55 via-white to-white',
    iconBox: 'bg-gradient-to-br from-blue-600 to-sky-500 text-white shadow-sm shadow-blue-200',
    providerShort: 'Şehir İçi Yollar',
    getStatus: getTrafficStatus,
    getChips: (d) => {
      const sec = d?.secondary
      if (!sec) return [{ text: '24 Londra ana koridoru', tone: 'bg-slate-100 text-slate-600' }]
      const disrupted = Number(sec.disrupted_corridors ?? 0)
      return [
        {
          icon: Gauge,
          text: `Yoğunluk %${Math.round(sec.congestion_pct ?? 0)}`,
          tone: 'bg-blue-50/90 text-blue-700 ring-1 ring-blue-100',
        },
        {
          icon: Route,
          text: disrupted > 0 ? `${disrupted} yolda gecikme` : 'Yollar açık',
          tone:
            disrupted > 0
              ? 'bg-amber-50 text-amber-800 ring-1 ring-amber-200/80'
              : 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/80',
        },
      ]
    },
  },
  {
    key: 'weather',
    title: 'Hava Durumu',
    unit: '°C',
    icon: CloudSun,
    topBar: 'bg-gradient-to-r from-cyan-500 via-sky-400 to-blue-500',
    cardGlow: 'bg-gradient-to-br from-cyan-50/55 via-white to-white',
    iconBox: 'bg-gradient-to-br from-cyan-500 to-sky-500 text-white shadow-sm shadow-cyan-200',
    providerShort: 'Anlık Hava',
    getStatus: getWeatherStatus,
    getChips: (d) => {
      const sec = d?.secondary
      if (!sec) return [{ text: 'Londra canlı hava', tone: 'bg-slate-100 text-slate-600' }]
      const windKmh = sec.wind_speed_ms != null ? Math.round(sec.wind_speed_ms * 3.6) : null
      return [
        {
          icon: Droplets,
          text: `Nem %${Math.round(sec.humidity_pct ?? 0)}`,
          tone: 'bg-cyan-50/90 text-cyan-800 ring-1 ring-cyan-100',
        },
        {
          icon: Wind,
          text: windKmh != null ? `Rüzgar ${windKmh} km/s` : 'Rüzgar —',
          title: sec.wind_speed_ms != null ? `${sec.wind_speed_ms} m/s` : undefined,
          tone: 'bg-slate-100/90 text-slate-700 ring-1 ring-slate-200/60',
        },
      ]
    },
  },
  {
    key: 'air_quality',
    title: 'Hava Kalitesi',
    unit: 'puan',
    icon: Wind,
    topBar: 'bg-gradient-to-r from-emerald-500 via-teal-400 to-cyan-400',
    cardGlow: 'bg-gradient-to-br from-emerald-50/55 via-white to-white',
    iconBox: 'bg-gradient-to-br from-emerald-600 to-teal-500 text-white shadow-sm shadow-emerald-200',
    providerShort: 'Hava Ölçümü',
    getStatus: getAqiStatus,
    getChips: (d) => {
      const sec = d?.secondary
      if (!sec) return [{ text: 'Londra hava ölçümü', tone: 'bg-slate-100 text-slate-600' }]
      return [
        {
          text: `İnce Toz: ${sec.pm2_5 != null ? Number(sec.pm2_5).toFixed(1) : '—'}`,
          title: 'Havadaki ince partikül yoğunluğu',
          tone: 'bg-emerald-50/90 text-emerald-800 ring-1 ring-emerald-100',
        },
        {
          text: `Toz Oranı: ${sec.pm10 != null ? Number(sec.pm10).toFixed(1) : '—'}`,
          title: 'Havadaki genel toz yoğunluğu',
          tone: 'bg-slate-100/90 text-slate-700 ring-1 ring-slate-200/60',
        },
      ]
    },
  },
  {
    key: 'energy',
    title: 'Bölgesel Enerji',
    unit: 'MW',
    icon: Zap,
    topBar: 'bg-gradient-to-r from-amber-500 via-orange-500 to-yellow-400',
    cardGlow: 'bg-gradient-to-br from-amber-50/55 via-white to-white',
    iconBox: 'bg-gradient-to-br from-amber-500 to-orange-500 text-white shadow-sm shadow-amber-200',
    providerShort: 'Şebeke Verisi',
    getStatus: getEnergyStatus,
    getChips: (d) => {
      const sec = d?.secondary
      if (!sec) return [{ text: 'Londra şebeke yükü', tone: 'bg-slate-100 text-slate-600' }]
      const londonTotal = sec.london_total_mw != null ? formatInt(sec.london_total_mw) : null
      return [
        {
          icon: Leaf,
          text: `Yenilenebilir %${Math.round(sec.renewable_share_pct ?? 0)}`,
          title: `Yenilenebilir enerji oranı: %${sec.renewable_share_pct ?? '—'}${londonTotal ? ` • Londra Toplam: ${londonTotal} MW` : ''}`,
          tone: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/80',
        },
        {
          text: `Karbon: ${Math.round(sec.carbon_intensity_gco2 ?? 0)} g`,
          title: 'Elektrik üretimindeki karbon salımı yoğunluğu',
          tone: 'bg-amber-50/90 text-amber-800 ring-1 ring-amber-200/70',
        },
      ]
    },
  },
]

function KpiCards({ domains = {}, sources = {}, isOffline = false }) {
  return (
    <section className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
      {CARD_META.map((card) => {
        const Icon = card.icon
        const data = domains[card.key]
        const src = sources[card.key]
        const isLiveApi = !isOffline && src?.status === 'live'
        const val = data?.avg_value
        const status = card.getStatus(val)
        const chips = card.getChips(data)

        const isWarning =
          (card.key === 'air_quality' && val > 60) ||
          (card.key === 'traffic' && val !== undefined && val < 14)

        return (
          <div
            key={card.key}
            className={`perf-card group relative overflow-hidden rounded-2xl border p-5 shadow-sm transition-[transform,box-shadow] duration-200 hover:-translate-y-0.5 hover:shadow-md ${
              isWarning
                ? 'border-rose-300 bg-gradient-to-br from-rose-50/70 via-white to-white ring-1 ring-rose-100'
                : `border-slate-200/90 ${card.cardGlow}`
            }`}
          >
            {/* Vibrant Top Accent Bar */}
            <div
              className={`absolute inset-x-0 top-0 h-1.5 ${
                isWarning ? 'bg-gradient-to-r from-rose-600 to-rose-400' : card.topBar
              }`}
            />

            {/* Header: Icon + Title/Source + Human Status Badge */}
            <div className="flex items-start justify-between gap-2">
              <div className="flex min-w-0 items-center gap-2.5">
                <span
                  className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-xl transition-transform duration-200 group-hover:scale-105 ${
                    isWarning
                      ? 'bg-gradient-to-br from-rose-600 to-rose-500 text-white shadow-sm shadow-rose-200'
                      : card.iconBox
                  }`}
                >
                  <Icon className="h-5 w-5" />
                </span>
                <div className="min-w-0">
                  <div className="truncate text-sm font-extrabold text-slate-900">{card.title}</div>
                  <div
                    title={card.providerShort}
                    className="mt-0.5 inline-flex items-center gap-1 text-[10px] font-semibold text-slate-400"
                  >
                    <span
                      className={`h-1.5 w-1.5 rounded-full ${
                        isLiveApi ? 'bg-emerald-500' : 'bg-slate-400'
                      }`}
                    />
                    <span className="truncate">{card.providerShort}</span>
                  </div>
                </div>
              </div>

              <span
                className={`shrink-0 rounded-full px-2.5 py-0.5 text-[11px] font-bold ${status.badge}`}
              >
                {status.label}
              </span>
            </div>

            {/* Big Readable Value */}
            <div className="mt-4 flex items-baseline gap-1.5">
              <span className="font-mono text-3xl font-extrabold tracking-tight text-slate-900 tabular-nums">
                {val !== undefined && val !== null
                  ? card.key === 'energy'
                    ? formatInt(val)
                    : val.toFixed(1)
                  : '—'}
              </span>
              <span className="text-sm font-bold text-slate-400">{card.unit}</span>
            </div>

            {/* Visual Level Bar (GPU-only scaleX transform) */}
            <div className="mt-3 h-1.5 w-full overflow-hidden rounded-full bg-slate-200/75">
              <div
                className={`h-full w-full origin-left rounded-full transition-transform duration-300 ${status.bar}`}
                style={{ transform: `scaleX(${status.pct / 100})` }}
              />
            </div>

            {/* Clean Mini Metric Chips */}
            <div className="mt-3 flex flex-wrap items-center gap-1.5">
              {chips.map((chip, idx) => {
                const ChipIcon = chip.icon
                return (
                  <span
                    key={idx}
                    title={chip.title}
                    className={`inline-flex items-center gap-1 rounded-lg px-2 py-1 text-[11px] font-semibold ${chip.tone}`}
                  >
                    {ChipIcon && <ChipIcon className="h-3 w-3 shrink-0" />}
                    <span>{chip.text}</span>
                  </span>
                )
              })}
            </div>
          </div>
        )
      })}
    </section>
  )
}

export default memo(KpiCards)
