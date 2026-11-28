import React, { memo } from 'react'
import {
  Activity,
  BarChart3,
  Gauge,
  History,
  MapPin,
  Radio,
  WifiOff,
} from 'lucide-react'

const TABS = [
  {
    id: 'live',
    label: 'Canlı Akış',
    icon: Activity,
    activeIcon: 'text-blue-600',
    activeRing: 'ring-blue-200/80 text-blue-700',
  },
  {
    id: 'districts',
    label: 'Bölgeler',
    icon: MapPin,
    activeIcon: 'text-emerald-600',
    activeRing: 'ring-emerald-200/80 text-emerald-700',
  },
  {
    id: 'benchmarks',
    label: 'Deneyler & Mimari',
    icon: BarChart3,
    activeIcon: 'text-violet-600',
    activeRing: 'ring-violet-200/80 text-violet-700',
  },
  {
    id: 'history',
    label: 'Geçmiş Veriler',
    icon: History,
    activeIcon: 'text-indigo-600',
    activeRing: 'ring-indigo-200/80 text-indigo-700',
  },
]

const RATES = [
  { value: 10, label: '10/sn', tag: 'Standart' },
  { value: 100, label: '100/sn', tag: 'Yüksek' },
]

function HeaderBar({
  activeTab,
  onTabChange,
  connected,
  isOffline = false,
  targetRate,
  onRateChange,
  controlBusy,
}) {
  return (
    <header className="sticky top-0 z-30 border-b border-slate-200/90 bg-white/95 shadow-[0_2px_12px_-6px_rgba(15,23,42,0.08)]">
      {/* Top Vibrant Gradient Line */}
      <div className="h-[3px] w-full bg-gradient-to-r from-blue-600 via-cyan-500 to-emerald-500" />

      <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-3 px-4 py-2.5 sm:px-6">
        {/* Brand */}
        <div className="flex items-center gap-3">
          <div className="relative flex h-10 w-10 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-blue-600 via-indigo-600 to-cyan-500 text-white shadow-md shadow-blue-500/25">
            <Radio className="h-5 w-5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h1 className="bg-gradient-to-r from-slate-900 via-slate-800 to-blue-900 bg-clip-text text-base font-extrabold tracking-tight text-transparent sm:text-lg">
                Londra Akıllı Şehir
              </h1>
              {isOffline ? (
                <span className="inline-flex items-center gap-1 rounded-full bg-rose-50 px-2 py-0.5 text-[11px] font-bold text-rose-700 ring-1 ring-rose-200/90">
                  <WifiOff className="h-3 w-3" />
                  Çevrimdışı
                </span>
              ) : (
                <span
                  className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-[11px] font-bold ${
                    connected
                      ? 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/90'
                      : 'bg-amber-50 text-amber-700 ring-1 ring-amber-200/90'
                  }`}
                >
                  <span className="relative flex h-2 w-2">
                    {connected && (
                      <span className="live-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400" />
                    )}
                    <span
                      className={`relative inline-flex h-2 w-2 rounded-full ${
                        connected ? 'bg-emerald-500' : 'bg-amber-500'
                      }`}
                    />
                  </span>
                  {connected ? 'Canlı' : 'Bağlanıyor'}
                </span>
              )}
            </div>
            <p
              className="text-[11px] font-medium text-slate-500"
              title="Londra Canlı Trafik, Hava ve Enerji İzleme"
            >
              Canlı Trafik, Hava &amp; Enerji İzleme
            </p>
          </div>
        </div>

        {/* Navigation Tabs */}
        <nav className="flex items-center gap-1 rounded-2xl bg-slate-100/90 p-1 ring-1 ring-slate-200/70">
          {TABS.map(({ id, label, icon: Icon, activeIcon, activeRing }) => {
            const active = activeTab === id
            return (
              <button
                key={id}
                type="button"
                onClick={() => onTabChange(id)}
                className={`flex items-center gap-1.5 rounded-xl px-3.5 py-1.5 text-xs font-bold transition-colors sm:text-[13px] ${
                  active
                    ? `bg-white shadow-sm ring-1 ${activeRing}`
                    : 'text-slate-600 hover:bg-white/60 hover:text-slate-900'
                }`}
              >
                <Icon className={`h-4 w-4 ${active ? activeIcon : 'text-slate-400'}`} />
                <span>{label}</span>
              </button>
            )
          })}
        </nav>

        {/* Stream Rate */}
        <div className="flex items-center gap-2">
          <div
            className="flex items-center gap-0.5 rounded-xl bg-slate-100/90 p-1 ring-1 ring-slate-200/70"
            title="Veri güncelleme hızı"
          >
            <Gauge className="mx-1.5 h-3.5 w-3.5 text-slate-400" />
            {RATES.map((r) => {
              const selected = targetRate === r.value
              return (
                <button
                  key={r.value}
                  type="button"
                  disabled={controlBusy}
                  onClick={() => onRateChange(r.value)}
                  title={`${r.tag} Hız (${r.label})`}
                  className={`rounded-lg px-2 py-1 font-mono text-[11px] font-bold transition-colors ${
                    selected
                      ? 'bg-gradient-to-r from-blue-600 to-indigo-600 text-white shadow-sm shadow-blue-500/25'
                      : 'text-slate-600 hover:bg-white hover:text-slate-900'
                  }`}
                >
                  {r.label}
                </button>
              )
            })}
          </div>
        </div>
      </div>
    </header>
  )
}

export default memo(HeaderBar)
