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
