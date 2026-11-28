import { startTransition, useCallback, useEffect, useRef, useState } from 'react'

const MIN_UI_COMMIT_MS = 600

export function useLiveStream(activeTab = 'live') {
  const [liveState, setLiveState] = useState(null)
  const [sources, setSources] = useState({})
  const [targetRate, setTargetRate] = useState(10)
  const [activeAnomaly, setActiveAnomaly] = useState(null)
  const [todaySummary, setTodaySummary] = useState(null)
  const [connected, setConnected] = useState(false)
  const [browserOffline, setBrowserOffline] = useState(
    () => typeof navigator !== 'undefined' && navigator.onLine === false
  )
  const [serverInternetOnline, setServerInternetOnline] = useState(true)
  const [controlBusy, setControlBusy] = useState(false)

  const wsRef = useRef(null)
  const reconnectTimer = useRef(null)
  const anomalyRefreshTimerRef = useRef(null)
  const targetRateRef = useRef(10)
  targetRateRef.current = targetRate
  const reconnectDelayRef = useRef(2000)
  const pendingRawFrameRef = useRef(null)
  const rafIdRef = useRef(null)
  const throttleTimerRef = useRef(null)
  const lastCommitMsRef = useRef(0)
  const lastSourcesJsonRef = useRef('')
  const activeTabRef = useRef(activeTab)
  activeTabRef.current = activeTab
  const flushFnRef = useRef(null)

  const sourceEntries = Object.values(sources || {})
  const apisOffline =
    sourceEntries.length > 0 && sourceEntries.every((s) => s?.status !== 'live')
  const isOffline = browserOffline || !serverInternetOnline || apisOffline
  const isOfflineRef = useRef(isOffline)
  isOfflineRef.current = isOffline

  const fetchSources = useCallback(async () => {
    if (document.hidden) return
    try {
      const srcRes = await fetch('/api/sources')
      if (!srcRes.ok) return
      const s = await srcRes.json()
      const nextSources = s.sources || {}
      const serialized = JSON.stringify(nextSources)
      if (serialized !== lastSourcesJsonRef.current) {
        lastSourcesJsonRef.current = serialized
        startTransition(() => setSources(nextSources))
      }
      if (s.target_rate) {
        setTargetRate((prev) => (prev === s.target_rate ? prev : s.target_rate))
      }
      if (typeof s.internet_online === 'boolean') {
        setServerInternetOnline(s.internet_online)
      }
      setActiveAnomaly(s.active_anomaly || null)
    } catch {
      // retry silently on next poll
    }
  }, [])

  const fetchTodaySummary = useCallback(async () => {
    if (document.hidden) return
    try {
      const res = await fetch('/api/history?days_ago=0&district_id=all&granularity=hourly')
      if (!res.ok) return
      const data = await res.json()
      if (data?.summary) {
        startTransition(() => setTodaySummary(data.summary))
      }
    } catch {
      // retry silently on next poll
    }
  }, [])

  // Flush buffered live frame when returning to live/districts
  useEffect(() => {
    if (
      !isOfflineRef.current &&
      (activeTab === 'live' || activeTab === 'districts') &&
      pendingRawFrameRef.current &&
      flushFnRef.current
    ) {
      flushFnRef.current()
    }
  }, [activeTab])

  useEffect(() => {
    let unmounted = false

    // Load initial snapshot + sources immediately
    fetch('/api/state')
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        if (unmounted || !data) return
        if (data.state) setLiveState(data.state)
        if (data.sources) {
          lastSourcesJsonRef.current = JSON.stringify(data.sources)
          setSources(data.sources)
        }
        if (data.target_rate) setTargetRate(data.target_rate)
        if (typeof data.internet_online === 'boolean') {
          setServerInternetOnline(data.internet_online)
        }
      })
      .catch(() => {})

    fetchSources()
    fetchTodaySummary()
    const metaTimer = setInterval(fetchSources, 5000)
    const todayTimer = setInterval(fetchTodaySummary, 300000)

    const commitPendingFrame = () => {
      rafIdRef.current = null
      throttleTimerRef.current = null
      if (unmounted || document.hidden || isOfflineRef.current) return
      // Avoid parsing & re-rendering live stream frames while viewing History
      if (activeTabRef.current === 'history') return

      const raw = pendingRawFrameRef.current
      pendingRawFrameRef.current = null
      if (!raw) return

      try {
        const msg = JSON.parse(raw)
        if (msg.type === 'heartbeat') return
        lastCommitMsRef.current = performance.now()

        // Non-urgent transition keeps clicks, tab switches, and scrolling instant
        startTransition(() => {
          setLiveState(msg)
          const nextRate = msg.performance?.target_rate_eps
          if (nextRate) {
            setTargetRate((prev) => (prev === nextRate ? prev : nextRate))
          }
        })
      } catch {
        // ignore malformed frames
      }
    }

    const scheduleFlush = () => {
      if (unmounted || document.hidden || isOfflineRef.current) return
      if (activeTabRef.current === 'history') return
      if (rafIdRef.current !== null || throttleTimerRef.current !== null) return

      const elapsed = performance.now() - lastCommitMsRef.current
      if (elapsed >= MIN_UI_COMMIT_MS) {
        rafIdRef.current = window.requestAnimationFrame(commitPendingFrame)
      } else {
        throttleTimerRef.current = window.setTimeout(() => {
          throttleTimerRef.current = null
          if (!unmounted && !document.hidden && rafIdRef.current === null) {
            rafIdRef.current = window.requestAnimationFrame(commitPendingFrame)
          }
        }, MIN_UI_COMMIT_MS - elapsed)
      }
    }
    flushFnRef.current = scheduleFlush

    const handleVisibilityChange = () => {
      if (!document.hidden && pendingRawFrameRef.current) {
        scheduleFlush()
      }
    }
    const handleOnline = () => {
      setBrowserOffline(false)
      fetchSources()
      fetchTodaySummary()
    }
    const handleOffline = () => {
      setBrowserOffline(true)
    }
    document.addEventListener('visibilitychange', handleVisibilityChange)
    window.addEventListener('online', handleOnline)
    window.addEventListener('offline', handleOffline)

    const connectWs = () => {
      if (unmounted) return
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
      const ws = new WebSocket(`${proto}//${window.location.host}/ws/live`)
      wsRef.current = ws

      ws.onopen = () => {
        reconnectDelayRef.current = 2000
        setConnected(true)
      }
      ws.onmessage = (evt) => {
        if (isOfflineRef.current) return
        // Skip heartbeats cheaply without full JSON parse
        if (typeof evt.data === 'string' && evt.data.startsWith('{"type":"heartbeat"')) return
        // Store raw string; only parse the latest frame when actually committing
        pendingRawFrameRef.current = evt.data
        scheduleFlush()
      }
      ws.onclose = () => {
        if (unmounted) return
        setConnected(false)
        const delay = reconnectDelayRef.current
        reconnectDelayRef.current = Math.min(15000, Math.round(delay * 1.6))
        reconnectTimer.current = setTimeout(connectWs, delay)
      }
      ws.onerror = () => ws.close()
    }

    connectWs()

    return () => {
      unmounted = true
      document.removeEventListener('visibilitychange', handleVisibilityChange)
      window.removeEventListener('online', handleOnline)
      window.removeEventListener('offline', handleOffline)
      clearInterval(metaTimer)
      clearInterval(todayTimer)
      clearTimeout(reconnectTimer.current)
      clearTimeout(anomalyRefreshTimerRef.current)
      if (throttleTimerRef.current !== null) clearTimeout(throttleTimerRef.current)
      if (rafIdRef.current !== null) window.cancelAnimationFrame(rafIdRef.current)
      if (wsRef.current) wsRef.current.close()
    }
  }, [fetchSources, fetchTodaySummary])

  const changeRate = useCallback(async (rate) => {
    const prevRate = targetRateRef.current
    setControlBusy(true)
    setTargetRate(rate)
    try {
      const res = await fetch('/api/control/rate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rate }),
      })
      if (!res.ok) {
        setTargetRate(prevRate)
      }
    } catch {
      setTargetRate(prevRate)
    } finally {
      setControlBusy(false)
    }
  }, [])

  const injectAnomaly = useCallback(
    async (domain, districtId = 'westminster') => {
      setControlBusy(true)
      try {
        const res = await fetch('/api/control/anomaly', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ domain, district_id: districtId, duration_sec: 15 }),
        })
        if (res.ok) {
          const data = await res.json()
          setActiveAnomaly(data.anomaly)
          clearTimeout(anomalyRefreshTimerRef.current)
          anomalyRefreshTimerRef.current = setTimeout(() => fetchSources(), 16000)
        }
      } catch {
        // non-fatal control error
      } finally {
        setControlBusy(false)
      }
    },
    [fetchSources]
  )

  return {
    liveState,
    sources,
    targetRate,
    activeAnomaly,
    todaySummary,
    connected,
    isOffline,
    browserOffline,
    apisOffline,
    controlBusy,
    changeRate,
    injectAnomaly,
  }
}
