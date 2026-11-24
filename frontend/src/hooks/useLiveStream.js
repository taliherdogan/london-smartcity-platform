import { startTransition, useCallback, useEffect, useRef, useState } from 'react'

const MIN_UI_COMMIT_MS = 600

export function useLiveStream(activeTab = 'live') {
  const [liveState, setLiveState] = useState(null)
  const [sources, setSources] = useState({})
  const [targetRate, setTargetRate] = useState(100)
  const [activeAnomaly, setActiveAnomaly] = useState(null)
  const [benchmarks, setBenchmarks] = useState(null)
  const [systemHealth, setSystemHealth] = useState(null)
  const [connected, setConnected] = useState(false)
  const [controlBusy, setControlBusy] = useState(false)

  const wsRef = useRef(null)
  const reconnectTimer = useRef(null)
  const reconnectDelayRef = useRef(2000)
  const pendingRawFrameRef = useRef(null)
  const rafIdRef = useRef(null)
  const throttleTimerRef = useRef(null)
  const lastCommitMsRef = useRef(0)
  const lastSourcesJsonRef = useRef('')
  const activeTabRef = useRef(activeTab)
  activeTabRef.current = activeTab

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
      setActiveAnomaly(s.active_anomaly || null)
    } catch {
      // retry silently on next poll
    }
  }, [])

  const fetchSystemHealth = useCallback(async () => {
    if (document.hidden) return
    try {
      const sysRes = await fetch('/api/system')
      if (!sysRes.ok) return
      const data = await sysRes.json()
      startTransition(() => setSystemHealth(data))
    } catch {
      // retry silently on next poll
    }
  }, [])

  const fetchBenchmarks = useCallback(async () => {
    try {
      const benchRes = await fetch('/api/benchmarks')
      if (!benchRes.ok) return
      const b = await benchRes.json()
      startTransition(() => setBenchmarks(b))
    } catch {
      // retry silently on next refresh
    }
  }, [])

  const refreshBenchmarks = useCallback(async () => {
    await Promise.all([fetchSources(), fetchBenchmarks()])
  }, [fetchSources, fetchBenchmarks])

  // Refresh benchmark data when entering the benchmarks tab
  useEffect(() => {
    if (activeTab === 'benchmarks') {
      fetchBenchmarks()
    }
  }, [activeTab, fetchBenchmarks])

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
      })
      .catch(() => {})

    fetchSources()
    fetchBenchmarks()
    fetchSystemHealth()
    const metaTimer = setInterval(fetchSources, 20000)
    const sysTimer = setInterval(fetchSystemHealth, 10000)

    const commitPendingFrame = () => {
      rafIdRef.current = null
      throttleTimerRef.current = null
      if (unmounted || document.hidden) return

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
      if (unmounted || document.hidden) return
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

    const handleVisibilityChange = () => {
      if (!document.hidden && pendingRawFrameRef.current) {
        scheduleFlush()
      }
    }
    document.addEventListener('visibilitychange', handleVisibilityChange)

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
        // Skip heartbeats cheaply without full JSON parse
        if (typeof evt.data === 'string' && evt.data.includes('"heartbeat"')) return
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
      clearInterval(metaTimer)
      clearInterval(sysTimer)
      clearTimeout(reconnectTimer.current)
      if (throttleTimerRef.current !== null) clearTimeout(throttleTimerRef.current)
      if (rafIdRef.current !== null) window.cancelAnimationFrame(rafIdRef.current)
      if (wsRef.current) wsRef.current.close()
    }
  }, [fetchSources, fetchBenchmarks, fetchSystemHealth])

  const changeRate = useCallback(async (rate) => {
    setControlBusy(true)
    setTargetRate(rate)
    try {
      await fetch('/api/control/rate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rate }),
      })
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
          setTimeout(() => fetchSources(), 16000)
        }
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
    benchmarks,
    systemHealth,
    connected,
    controlBusy,
    changeRate,
    injectAnomaly,
    refreshBenchmarks,
  }
}
