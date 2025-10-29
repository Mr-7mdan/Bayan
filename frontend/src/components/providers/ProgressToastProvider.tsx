"use client"

import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import { Api, type SyncTaskOut } from '@/lib/api'

interface ProgressToastState {
  visible: boolean
  title?: string
  message?: string
  percent?: number | null
  current?: number | null
  total?: number | null
  runningTasks?: number
  datasourceId?: string
}

interface ProgressToastContextValue {
  state: ProgressToastState
  show: (title: string, message?: string) => void
  hide: () => void
  update: (p: Partial<ProgressToastState>) => void
  // Starts polling sync status for a datasource until all tasks finished
  startMonitoring: (datasourceId: string, actorId?: string) => void
}

const ProgressToastContext = createContext<ProgressToastContextValue | null>(null)

export function useProgressToast() {
  const ctx = useContext(ProgressToastContext)
  if (!ctx) throw new Error('useProgressToast must be used within ProgressToastProvider')
  return ctx
}

export default function ProgressToastProvider({ children }: { children: React.ReactNode }) {
  const [state, setState] = useState<ProgressToastState>({ visible: false })
  const timerRef = useRef<NodeJS.Timer | null>(null)

  const show = useCallback((title: string, message?: string) => {
    setState((s) => ({ ...s, visible: true, title, message }))
  }, [])

  const hide = useCallback(() => {
    setState({ visible: false })
    if (timerRef.current) {
      clearInterval(timerRef.current as unknown as number)
      timerRef.current = null
    }
  }, [])

  const update = useCallback((p: Partial<ProgressToastState>) => {
    setState((s) => ({ ...s, ...p }))
  }, [])

  const computeProgress = useCallback((tasks: SyncTaskOut[]) => {
    const running = tasks.filter((t) => t.inProgress)
    if (running.length === 0) return { percent: 1, current: 0, total: 0, runningTasks: 0 }
    let curSum = 0
    let totSum = 0
    let unknown = 0
    for (const t of running) {
      const c = Number(t.progressCurrent ?? 0)
      const tot = t.progressTotal
      curSum += isFinite(c) ? c : 0
      if (typeof tot === 'number' && isFinite(tot)) totSum += tot
      else unknown += 1
    }
    if (totSum > 0) {
      const p = Math.max(0, Math.min(1, curSum / totSum))
      return { percent: p, current: curSum, total: totSum, runningTasks: running.length }
    }
    // Unknown totals -> indeterminate
    return { percent: null, current: curSum, total: null, runningTasks: running.length }
  }, [])

  const startMonitoring = useCallback((datasourceId: string, actorId?: string) => {
    // Immediately show toast
    setState({ visible: true, title: 'Sync in progress', message: 'Starting…', datasourceId })
    if (timerRef.current) {
      clearInterval(timerRef.current as unknown as number)
      timerRef.current = null
    }
    const poll = async () => {
      try {
        const tasks = await Api.getSyncStatus(datasourceId, actorId)
        const { percent, current, total, runningTasks } = computeProgress(tasks)
        setState((s) => ({ ...s, visible: true, title: 'Sync in progress', message: runningTasks ? `Running ${runningTasks} task(s)…` : 'Finishing…', percent, current, total, runningTasks }))
        if (!tasks.some((t) => t.inProgress)) {
          // Done
          setState((s) => ({ ...s, visible: true, title: 'Sync complete', message: 'All tasks finished', percent: 1 }))
          if (timerRef.current) {
            clearInterval(timerRef.current as unknown as number)
            timerRef.current = null
          }
          // Auto-hide after a short delay
          setTimeout(() => hide(), 2000)
        }
      } catch (e) {
        setState((s) => ({ ...s, visible: true, title: 'Sync', message: 'Updating status…' }))
      }
    }
    // Kick off immediately, then poll every 2s
    void poll()
    timerRef.current = setInterval(poll, 2000)
  }, [computeProgress, hide])

  useEffect(() => () => { if (timerRef.current) clearInterval(timerRef.current as unknown as number) }, [])

  const value = useMemo(() => ({ state, show, hide, update, startMonitoring }), [state, show, hide, update, startMonitoring])

  return (
    <ProgressToastContext.Provider value={value}>
      {children}
      {state.visible && (
        <div className="fixed bottom-4 right-4 z-[9999] max-w-sm w-[360px] rounded-md border bg-card shadow-card">
          <div className="px-3 py-2 border-b">
            <div className="text-sm font-semibold">{state.title || 'Sync'}</div>
            <div className="text-xs text-muted-foreground">{state.message || ''}</div>
          </div>
          <div className="px-3 py-3">
            {state.percent === null ? (
              <div className="h-2 w-full bg-muted rounded overflow-hidden">
                <div className="h-2 w-1/3 bg-primary animate-pulse" />
              </div>
            ) : (
              <div className="h-2 w-full bg-muted rounded overflow-hidden">
                <div className="h-2 bg-primary" style={{ width: `${Math.round((state.percent || 0) * 100)}%` }} />
              </div>
            )}
            <div className="mt-1 text-[11px] text-muted-foreground">
              {typeof state.current === 'number' ? state.current.toLocaleString() : '—'}
              {typeof state.total === 'number' ? ` / ${state.total.toLocaleString()} rows` : ' rows'}
            </div>
          </div>
          <div className="px-3 pb-2 flex justify-end">
            <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={hide}>Hide</button>
          </div>
        </div>
      )}
    </ProgressToastContext.Provider>
  )
}
