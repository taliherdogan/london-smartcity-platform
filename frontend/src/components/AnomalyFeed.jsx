import React, { memo, useCallback, useEffect, useMemo, useState } from 'react'
import {
  AlertTriangle,
  ArrowDownRight,
  ArrowUpRight,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  CloudLightning,
  HelpCircle,
  Info,
  ShieldAlert,
  ShieldCheck,
  Sparkles,
  TrafficCone,
  Wind,
  Zap,
} from 'lucide-react'
import { formatInt, formatTime } from '../utils/format'

const DOMAIN_META = {
  traffic: {
    label: 'Trafik',
    unit: 'km/s',
    icon: TrafficCone,
    iconBox: 'bg-blue-50 text-blue-600 ring-1 ring-blue-200/80',
    pill: 'bg-blue-50 text-blue-700 ring-1 ring-blue-200/70',
    filterActive: 'bg-blue-600 text-white shadow-sm',
    filterIdle: 'bg-blue-50/80 text-blue-700 ring-1 ring-blue-200/60 hover:bg-blue-100/80',
  },
  weather: {
    label: 'Hava',
    unit: '°C',
    icon: CloudLightning,
    iconBox: 'bg-cyan-50 text-cyan-600 ring-1 ring-cyan-200/80',
    pill: 'bg-cyan-50 text-cyan-700 ring-1 ring-cyan-200/70',
    filterActive: 'bg-cyan-600 text-white shadow-sm',
    filterIdle: 'bg-cyan-50/80 text-cyan-700 ring-1 ring-cyan-200/60 hover:bg-cyan-100/80',
  },
  air_quality: {
    label: 'Hava Kalitesi',
    unit: 'AQI',
    icon: Wind,
    iconBox: 'bg-emerald-50 text-emerald-600 ring-1 ring-emerald-200/80',
    pill: 'bg-emerald-50 text-emerald-700 ring-1 ring-emerald-200/70',
    filterActive: 'bg-emerald-600 text-white shadow-sm',
    filterIdle: 'bg-emerald-50/80 text-emerald-700 ring-1 ring-emerald-200/60 hover:bg-emerald-100/80',
  },
  energy: {
    label: 'Enerji',
    unit: 'MW',
    icon: Zap,
    iconBox: 'bg-amber-50 text-amber-600 ring-1 ring-amber-200/80',
    pill: 'bg-amber-50 text-amber-800 ring-1 ring-amber-200/70',
    filterActive: 'bg-amber-600 text-white shadow-sm',
    filterIdle: 'bg-amber-50/80 text-amber-800 ring-1 ring-amber-200/60 hover:bg-amber-100/80',
  },
}

const DOMAIN_ORDER = ['traffic', 'weather', 'air_quality', 'energy']

function formatRelativeTime(timestamp, nowMs) {
  if (!timestamp) return ''
  const tsMs = Date.parse(timestamp)
  if (!Number.isFinite(tsMs)) return ''
  const diffSec = Math.max(0, Math.floor((nowMs - tsMs) / 1000))
  if (diffSec < 5) return 'az önce'
  if (diffSec < 60) return `${diffSec} sn önce`
  const mins = Math.floor(diffSec / 60)
  if (mins < 60) return `${mins} dk önce`
  return formatTime(tsMs)
}

function formatCleanValue(val, domain) {
  const num = Number(val)
  if (!Number.isFinite(num)) return val
  if (domain === 'energy') return formatInt(num)
  if (domain === 'air_quality') return Math.round(num).toString()
  return num.toFixed(1)
}

function getComparisonInfo(al) {
  const val = Number(al.value)
  const base = Number(al.baseline_value)
  if (!Number.isFinite(val) || !Number.isFinite(base) || base <= 0) {
    return {
      text: 'Sınır aşıldı',
      detailText: 'Güvenlik sınırı aşıldı',
      dir: al.direction === 'low' ? 'down' : 'up',
      fillPct: 78,
      basePct: 55,
    }
  }

  const ratio = val / base
  const pctDiff = Math.round(Math.abs(((val - base) / base) * 100))
  const isLower = val < base

  let text = 'Ani değişim'
  let detailText = 'Ani değişim tespit edildi'
  if (al.domain === 'traffic' && isLower && pctDiff >= 2) {
    text = `%${pctDiff} daha yavaş`
    detailText = `Normalden %${pctDiff} daha yavaş`
  } else if (!isLower && ratio >= 1.8) {
    text = `Normalin ${ratio.toFixed(1)}x`
    detailText = `Normalin ${ratio.toFixed(1)} katı`
  } else if (!isLower && pctDiff >= 2) {
    text = `%${pctDiff} yüksek`
    detailText = `Normalden %${pctDiff} yüksek`
  } else if (isLower && pctDiff >= 2) {
    text = `%${pctDiff} düşük`
    detailText = `Normalden %${pctDiff} düşük`
  } else {
    text = 'Eşik sınırı'
    detailText = 'Sınır eşiğine ulaştı'
  }

  const scaleMax = Math.max(val, base) * 1.15
  const fillPct = Math.max(10, Math.min(96, Math.round((val / scaleMax) * 100)))
  const basePct = Math.max(12, Math.min(92, Math.round((base / scaleMax) * 100)))

  return {
    text,
    detailText,
    dir: isLower ? 'down' : 'up',
    fillPct,
    basePct,
  }
}

const AnomalyCard = memo(function AnomalyCard({ al, nowMs, isExpanded, onToggle }) {
  const isCrit = al.severity === 'critical'
  const dMeta = DOMAIN_META[al.domain] || DOMAIN_META.traffic
  const DomainIcon = dMeta.icon
  const exactTime = formatTime(al.timestamp)
  const relTime = formatRelativeTime(al.timestamp, nowMs)
  const unit = al.unit || dMeta.unit
  const hasBaseline = al.baseline_value !== undefined && al.baseline_value !== null
  const baselineLabel = al.baseline_label || 'Beklenen Normal'
  const comp = getComparisonInfo(al)
  const DirIcon = comp.dir === 'down' ? ArrowDownRight : ArrowUpRight

  return (
    <div
      className={`perf-card group rounded-xl border border-l-[3.5px] p-3 transition-shadow duration-150 hover:shadow-md ${
        isCrit
          ? 'border-rose-200/90 border-l-rose-500 bg-gradient-to-r from-rose-50/45 via-white to-white hover:border-rose-300'
          : 'border-amber-200/90 border-l-amber-500 bg-gradient-to-r from-amber-50/40 via-white to-white hover:border-amber-300'
      }`}
    >
      {/* Top Row */}
      <div className="flex items-start justify-between gap-2">
        <div className="flex min-w-0 items-start gap-2.5">
          <span
            className={`mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-xl ${dMeta.iconBox}`}
          >
            <DomainIcon className="h-4 w-4" />
          </span>
          <div className="min-w-0">
            <div className="truncate text-[13px] font-extrabold leading-snug text-slate-900">
              {al.district_name}
            </div>
            <div className="mt-1 flex flex-wrap items-center gap-1.5">
              <span
                className={`inline-flex items-center gap-1 rounded-md px-1.5 py-0.5 text-[9px] font-extrabold tracking-wide ${
                  isCrit
                    ? 'bg-rose-600 text-white shadow-2xs'
                    : 'bg-amber-600 text-white shadow-2xs'
                }`}
              >
                <AlertTriangle className="h-2.5 w-2.5" />
                {isCrit ? 'KRİTİK' : 'UYARI'}
              </span>
              <span
                className={`inline-block rounded-md px-1.5 py-0.5 text-[10px] font-bold ${dMeta.pill}`}
              >
                {dMeta.label}
              </span>
              {al.injected && (
                <span className="inline-flex items-center gap-0.5 rounded-md bg-fuchsia-50 px-1.5 py-0.5 text-[10px] font-bold text-fuchsia-700 ring-1 ring-fuchsia-200/70">
                  <Sparkles className="h-2.5 w-2.5" />
                  Test
                </span>
              )}
            </div>
          </div>
        </div>

        <span
          className="shrink-0 whitespace-nowrap rounded-md bg-slate-100/90 px-1.5 py-0.5 text-[10px] font-semibold text-slate-500"
          title={exactTime ? `Kayıt Saati: ${exactTime}` : undefined}
        >
          {relTime}
        </span>
      </div>

      {/* Plain-Language Message */}
      <p className="mt-2 text-xs font-medium leading-relaxed text-slate-700">
        {al.message}
      </p>

      {/* Sleek Comparison Metric Box */}
      <div className="mt-2.5 rounded-xl bg-slate-50/90 p-2.5 ring-1 ring-slate-200/75">
        <div className="flex flex-wrap items-center justify-between gap-1.5">
          <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5 text-[11px]">
            <span className="text-slate-500">
              Şu an{' '}
              <strong
                className={`font-mono text-xs font-extrabold tabular-nums ${
                  isCrit ? 'text-rose-600' : 'text-amber-700'
                }`}
              >
                {formatCleanValue(al.value, al.domain)} {unit}
              </strong>
            </span>
            {hasBaseline && (
              <span className="text-[10px] text-slate-400">
                / Normal{' '}
                <strong className="font-mono font-bold text-slate-600 tabular-nums">
                  {formatCleanValue(al.baseline_value, al.domain)} {unit}
                </strong>
              </span>
            )}
          </div>

          <span
            className={`inline-flex items-center gap-0.5 rounded-md px-1.5 py-0.5 text-[10px] font-extrabold ${
              isCrit
                ? 'bg-rose-100 text-rose-700 ring-1 ring-rose-200/70'
                : 'bg-amber-100 text-amber-800 ring-1 ring-amber-200/70'
            }`}
          >
            <DirIcon className="h-3 w-3" />
            {comp.text}
          </span>
        </div>

        {/* Visual Deviation Progress Bar (GPU-only scaleX) */}
        {hasBaseline && (
          <div className="mt-2">
            <div className="relative h-1.5 w-full overflow-hidden rounded-full bg-slate-200/80">
              <div
                className={`h-full w-full origin-left rounded-full ${
                  isCrit
                    ? 'bg-gradient-to-r from-rose-500 to-pink-500'
                    : 'bg-gradient-to-r from-amber-500 to-orange-500'
                }`}
                style={{ transform: `scaleX(${comp.fillPct / 100})` }}
              />
              <div
                className="absolute top-0 h-full w-0.5 bg-slate-800/80"
                style={{ left: `${comp.basePct}%` }}
                title={`${baselineLabel}: ${formatCleanValue(al.baseline_value, al.domain)} ${unit}`}
              />
            </div>
            <div className="mt-1 flex items-center justify-between text-[9px] font-semibold text-slate-400">
              <span>Ölçülen</span>
              <span>│ {baselineLabel}</span>
            </div>
          </div>
        )}
      </div>

      {/* Expandable Details Toggle */}
      <div className="mt-2 flex items-center justify-between">
        <span className="text-[10px] font-semibold text-slate-400">
          Otomatik Tespit
        </span>
        <button
          type="button"
          onClick={() => onToggle(al.alert_id)}
          className="inline-flex items-center gap-0.5 text-[10px] font-bold text-slate-500 transition-colors hover:text-amber-700"
        >
          {isExpanded ? 'Detayı gizle' : 'Detaylar'}
          {isExpanded ? (
            <ChevronUp className="h-3 w-3" />
          ) : (
            <ChevronDown className="h-3 w-3" />
          )}
        </button>
      </div>

      {isExpanded && (
        <div className="mt-1.5 space-y-1 rounded-lg bg-slate-100/70 px-2.5 py-2 text-[10px] text-slate-600 ring-1 ring-slate-200/70">
          <div className="flex items-center justify-between gap-2">
            <span className="text-slate-400">Sapma Özeti:</span>
            <span className="font-bold text-slate-700">{comp.detailText}</span>
          </div>
          {hasBaseline && (
            <div className="flex items-center justify-between gap-2">
              <span className="text-slate-400">{baselineLabel}:</span>
              <span className="font-mono font-semibold text-slate-700">
                {formatCleanValue(al.baseline_value, al.domain)} {unit}
              </span>
            </div>
          )}
          {exactTime && (
            <div className="flex items-center justify-between gap-2">
              <span className="text-slate-400">Kayıt Saati:</span>
              <span className="font-mono font-semibold text-slate-700">{exactTime}</span>
            </div>
          )}
        </div>
      )}
    </div>
  )
})

function AnomalyFeed({ alerts = [], cumulativeAlerts = 0 }) {
  const [severityFilter, setSeverityFilter] = useState('all')
  const [domainFilter, setDomainFilter] = useState('all')
  const [showGuide, setShowGuide] = useState(false)
  const [expandedId, setExpandedId] = useState(null)
  const [nowMs, setNowMs] = useState(() => Date.now())

  // Update relative timestamps every 5s (only while alerts exist and tab is visible)
  useEffect(() => {
    if (alerts.length === 0) return undefined
    const timer = setInterval(() => {
      if (!document.hidden) setNowMs(Date.now())
    }, 5000)
    return () => clearInterval(timer)
  }, [alerts.length > 0])

  const handleToggleExpand = useCallback((id) => {
    setExpandedId((prev) => (prev === id ? null : id))
  }, [])

  const counts = useMemo(() => {
    let critical = 0
    let warning = 0
    const byDomain = { traffic: 0, weather: 0, air_quality: 0, energy: 0 }
    for (const al of alerts) {
      if (al.severity === 'critical') critical += 1
      else warning += 1
      if (byDomain[al.domain] !== undefined) byDomain[al.domain] += 1
    }
    return { critical, warning, total: alerts.length, byDomain }
  }, [alerts])

  const filteredAlerts = useMemo(() => {
    return alerts.filter((al) => {
      if (severityFilter === 'critical' && al.severity !== 'critical') return false
      if (severityFilter === 'warning' && al.severity === 'critical') return false
      if (domainFilter !== 'all' && al.domain !== domainFilter) return false
      return true
    })
  }, [alerts, severityFilter, domainFilter])

  const sessionTotal = Math.max(cumulativeAlerts, alerts.length)
  const hasCritical = counts.critical > 0

  const topBarClass = hasCritical
    ? 'bg-gradient-to-r from-rose-600 via-pink-500 to-fuchsia-500'
    : alerts.length > 0
      ? 'bg-gradient-to-r from-amber-500 via-orange-500 to-amber-400'
      : 'bg-gradient-to-r from-emerald-500 via-teal-400 to-cyan-400'

  const headerIconClass = hasCritical
    ? 'bg-gradient-to-br from-rose-600 to-pink-600 shadow-rose-500/25'
    : alerts.length > 0
      ? 'bg-gradient-to-br from-amber-500 to-orange-600 shadow-amber-500/25'
      : 'bg-gradient-to-br from-emerald-500 to-teal-600 shadow-emerald-500/25'

  const dotBgClass = hasCritical
    ? 'bg-rose-500'
    : alerts.length > 0
      ? 'bg-amber-500'
      : 'bg-emerald-500'

  return (
    <div className="perf-card relative flex h-full flex-col overflow-hidden rounded-2xl border border-slate-200/90 bg-white p-4 shadow-sm">
      {/* Top Accent Bar */}
      <div className={`absolute inset-x-0 top-0 h-1 ${topBarClass}`} />

      {/* Header */}
      <div className="mb-3 flex items-center justify-between gap-2 pt-0.5">
        <div className="flex min-w-0 items-center gap-2.5">
          <span
            className={`relative flex h-9 w-9 shrink-0 items-center justify-center rounded-xl text-white shadow-sm ${headerIconClass}`}
          >
            {alerts.length > 0 ? (
              <ShieldAlert className="h-4 w-4" />
            ) : (
              <ShieldCheck className="h-4 w-4" />
            )}
            <span className="absolute -right-0.5 -top-0.5 flex h-2.5 w-2.5">
              <span
                className={`relative inline-flex h-2.5 w-2.5 rounded-full ring-2 ring-white ${dotBgClass}`}
              />
            </span>
          </span>
          <div className="min-w-0">
            <h3 className="truncate text-sm font-extrabold leading-tight text-slate-900">
              Canlı Anomali Akışı
            </h3>
            <p className="truncate text-[11px] font-medium text-slate-400">
              Anlık sapma ve eşik takibi
            </p>
          </div>
        </div>

        <button
          type="button"
          onClick={() => setShowGuide((v) => !v)}
          className={`inline-flex shrink-0 items-center gap-1 whitespace-nowrap rounded-lg px-2.5 py-1.5 text-[11px] font-bold transition-colors ${
            showGuide
              ? 'bg-slate-800 text-white shadow-sm shadow-slate-500/20'
              : 'bg-slate-100/90 text-slate-700 ring-1 ring-slate-200/80 hover:bg-slate-200/80'
          }`}
          title="Anomalilerin nasıl tespit edildiğini öğrenin"
        >
          <HelpCircle className="h-3.5 w-3.5" />
          Nasıl Çalışır?
        </button>
      </div>

      {/* Collapsible Plain-Language Guide */}
      {showGuide && (
        <div className="mb-3 rounded-xl border border-slate-200/90 bg-gradient-to-br from-slate-50/80 via-white to-amber-50/30 p-3 text-[11px] leading-relaxed text-slate-600">
          <div className="mb-1.5 flex items-center gap-1.5 font-extrabold text-slate-800">
            <Info className="h-3.5 w-3.5 text-slate-600" />
            Anomali Sistemi Nasıl Çalışır?
          </div>
          <ul className="space-y-1.5">
            <li className="flex items-start gap-1.5">
              <span className="mt-1 h-2 w-2 shrink-0 rounded-full bg-rose-500" />
              <span>
                <strong className="text-rose-700">Kritik:</strong> Trafik 12 km/s altına indiğinde,
                hava kalitesi tehlikeli düzeye çıktığında veya şebeke yükü +%25 aştığında anında
                tetiklenir.
              </span>
            </li>
            <li className="flex items-start gap-1.5">
              <span className="mt-1 h-2 w-2 shrink-0 rounded-full bg-amber-500" />
              <span>
                <strong className="text-amber-700">Uyarı:</strong> Ana yollarda trafik aksaması veya
                son 1 dakikalık ortalamadan ani bir sapma görüldüğünde listelenir.
              </span>
            </li>
            <li className="flex items-start gap-1.5">
              <span className="mt-1 h-2 w-2 shrink-0 rounded-full bg-slate-400" />
              <span className="text-slate-500">
                Listede performans için <strong>en güncel 25 kayıt</strong> tutulur; sayaç oturum
                boyunca tespit edilen toplam sayıyı gösterir.
              </span>
            </li>
          </ul>
        </div>
      )}

      {/* Clean Full-Width Severity Segmented Control */}
      <div className="mb-2.5 grid grid-cols-3 gap-1 rounded-xl bg-slate-100/90 p-1 ring-1 ring-slate-200/70">
        <button
          type="button"
          onClick={() => setSeverityFilter('all')}
          className={`rounded-lg px-2 py-1.5 text-[11px] font-bold transition-colors ${
            severityFilter === 'all'
              ? 'bg-white text-slate-900 shadow-sm ring-1 ring-slate-200/80'
              : 'text-slate-500 hover:text-slate-800'
          }`}
        >
          Tümü ({counts.total})
        </button>
        <button
          type="button"
          onClick={() => setSeverityFilter('critical')}
          className={`rounded-lg px-2 py-1.5 text-[11px] font-bold transition-colors ${
            severityFilter === 'critical'
              ? 'bg-gradient-to-r from-rose-600 to-pink-600 text-white shadow-sm'
              : 'text-rose-600 hover:bg-rose-50/80'
          }`}
        >
          Kritik ({counts.critical})
        </button>
        <button
          type="button"
          onClick={() => setSeverityFilter('warning')}
          className={`rounded-lg px-2 py-1.5 text-[11px] font-bold transition-colors ${
            severityFilter === 'warning'
              ? 'bg-gradient-to-r from-amber-500 to-orange-600 text-white shadow-sm'
              : 'text-amber-700 hover:bg-amber-50/80'
          }`}
        >
          Uyarı ({counts.warning})
        </button>
      </div>

      {/* Domain Quick-Filter Pills + Session Counter */}
      {(alerts.length > 0 || sessionTotal > 0) && (
        <div className="mb-3 flex flex-wrap items-center justify-between gap-1.5">
          <div className="flex flex-wrap items-center gap-1.5">
            {alerts.length > 0 && (
              <>
                <button
                  type="button"
                  onClick={() => setDomainFilter('all')}
                  className={`rounded-full px-2.5 py-0.5 text-[10px] font-bold transition-colors ${
                    domainFilter === 'all'
                      ? 'bg-slate-800 text-white shadow-sm'
                      : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                  }`}
                >
                  Tüm Alanlar
                </button>
                {DOMAIN_ORDER.map((dom) => {
                  const count = counts.byDomain[dom] || 0
                  if (count === 0 && domainFilter !== dom) return null
                  const meta = DOMAIN_META[dom]
                  const Icon = meta.icon
                  const active = domainFilter === dom
                  return (
                    <button
                      key={dom}
                      type="button"
                      onClick={() => setDomainFilter(active ? 'all' : dom)}
                      className={`inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-[10px] font-bold transition-colors ${
                        active ? meta.filterActive : meta.filterIdle
                      }`}
                    >
                      <Icon className="h-3 w-3" />
                      {meta.label} ({count})
                    </button>
                  )
                })}
              </>
            )}
          </div>

          {sessionTotal > 0 && (
            <span
              className="shrink-0 whitespace-nowrap rounded-full bg-slate-100 px-2 py-0.5 text-[10px] font-bold text-slate-500 ring-1 ring-slate-200/70 tabular-nums"
              title={
                sessionTotal > alerts.length
                  ? `Listede en güncel ${alerts.length} kayıt gösteriliyor. Oturum başından beri toplam ${sessionTotal} anomali tespit edildi.`
                  : `Oturum boyunca toplam ${sessionTotal} anomali tespit edildi.`
              }
            >
              {sessionTotal > alerts.length
                ? `Son ${alerts.length} / Toplam ${sessionTotal}`
                : `Toplam ${sessionTotal}`}
            </span>
          )}
        </div>
      )}

      {/* Alert List or Empty State */}
      {filteredAlerts.length === 0 ? (
        <div className="flex flex-1 flex-col items-center justify-center rounded-xl border border-dashed border-emerald-200 bg-gradient-to-b from-emerald-50/50 to-teal-50/30 p-6 text-center">
          <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-gradient-to-br from-emerald-500 to-teal-500 text-white shadow-md shadow-emerald-500/20">
            <CheckCircle2 className="h-6 w-6" />
          </div>
          <p className="mt-3 text-sm font-extrabold text-emerald-950">
            {alerts.length === 0 ? 'Londra Genelinde Her Şey Normal' : 'Bu Filtrede Kayıt Yok'}
          </p>
          <p className="mt-1 max-w-[260px] text-xs leading-relaxed text-slate-500">
            {alerts.length === 0
              ? 'Tüm bölgeler canlı olarak izleniyor. Trafik, hava veya enerjide olağan dışı bir durum oluştuğunda anında burada görünür.'
              : 'Seçili filtrede kayıt bulunmuyor. Diğer anomalileri görmek için filtreyi temizleyebilirsiniz.'}
          </p>
        </div>
      ) : (
        <div className="thin-scrollbar max-h-[540px] space-y-2.5 overflow-y-auto pr-1">
          {filteredAlerts.map((al) => (
            <AnomalyCard
              key={al.alert_id}
              al={al}
              nowMs={nowMs}
              isExpanded={expandedId === al.alert_id}
              onToggle={handleToggleExpand}
            />
          ))}
        </div>
      )}
    </div>
  )
}

export default memo(AnomalyFeed)
