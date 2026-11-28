// Cached Intl formatters avoid recreating locale instances on every render/tick.
const INT_FORMATTER = new Intl.NumberFormat('tr-TR', {
  maximumFractionDigits: 0,
})

const TIME_FORMATTER = new Intl.DateTimeFormat('tr-TR', {
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
})

export function formatInt(value) {
  const num = Number(value)
  if (!Number.isFinite(num)) return '—'
  return INT_FORMATTER.format(Math.round(num))
}

export function formatTime(timestamp) {
  if (!timestamp) return ''
  const ms = typeof timestamp === 'number' ? timestamp : Date.parse(timestamp)
  if (!Number.isFinite(ms)) return ''
  return TIME_FORMATTER.format(ms)
}

const NICE_STEPS = [0.5, 1, 2, 2.5, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000]

/**
 * Compute clean, evenly-spaced Y-axis domain & explicit ticks so narrow ranges
 * never produce duplicate rounded labels (e.g., 28, 28, 28, 27).
 */
export function computeNiceYAxis(
  min,
  max,
  { minSpan = 2, minStep = 0.5, allowNegative = false, maxIntervals = 4 } = {}
) {
  const safeMin = Number.isFinite(min) ? min : 0
  const safeMax = Number.isFinite(max) ? max : minSpan
  const center = (safeMin + safeMax) / 2
  const dataSpan = Math.max(safeMax - safeMin, 0)
  const targetSpan = Math.max(dataSpan * 1.25, minSpan)

  let lo = dataSpan > 0 ? safeMin - targetSpan * 0.12 : center - targetSpan / 2
  let hi = dataSpan > 0 ? safeMax + targetSpan * 0.12 : center + targetSpan / 2
  if (!allowNegative && lo < 0) {
    lo = 0
    hi = Math.max(hi, minSpan)
  }

  const pickStep = (raw) => {
    for (let i = 0; i < NICE_STEPS.length; i += 1) {
      if (NICE_STEPS[i] >= minStep && NICE_STEPS[i] >= raw) return NICE_STEPS[i]
    }
    const mag = 10 ** Math.floor(Math.log10(Math.max(raw, 1)))
    return Math.ceil(raw / mag) * mag
  }

  let step = pickStep((hi - lo) / 3)
  let yMin = Math.floor(lo / step) * step
  if (!allowNegative && yMin < 0) yMin = 0
  let yMax = Math.ceil(hi / step) * step
  if (yMax <= yMin) yMax = yMin + step * 3

  let intervals = Math.round((yMax - yMin) / step)
  while (intervals > maxIntervals) {
    step = pickStep(step * 1.25)
    yMin = Math.floor(lo / step) * step
    if (!allowNegative && yMin < 0) yMin = 0
    yMax = Math.ceil(hi / step) * step
    intervals = Math.round((yMax - yMin) / step)
  }
  while (intervals < 3) {
    yMax = Number((yMax + step).toFixed(4))
    intervals += 1
  }

  const decimals = Number.isInteger(step) ? 0 : 1
  const ticks = []
  for (let i = 0; i <= intervals; i += 1) {
    ticks.push(Number((yMin + i * step).toFixed(decimals)))
  }

  return {
    domain: [ticks[0], ticks[ticks.length - 1]],
    ticks,
    decimals,
  }
}

export function formatAxisTick(value, decimals = 0, suffix = '') {
  const num = Number(value)
  if (!Number.isFinite(num)) return ''
  const text = decimals > 0 ? num.toFixed(decimals) : Math.round(num).toString()
  return suffix ? `${text}${suffix}` : text
}
