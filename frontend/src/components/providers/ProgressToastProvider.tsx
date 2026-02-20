"use client"

import React, { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Api, type SyncTaskOut } from '@/lib/api'

declare global {
  namespace JSX {
    interface IntrinsicElements {
      [elemName: string]: any
    }
  }
}

interface ProgressToastState {
  visible: boolean
  title?: string
  message?: string
  percent?: number | null
  current?: number | null
  total?: number | null
  runningTasks?: number
  datasourceId?: string
  minimized?: boolean
  monitoring?: boolean
  actorId?: string
  phase?: string | null
  startedAtMs?: number
  elapsedSec?: number
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
  const queryClient = useQueryClient()
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const tickRef = useRef<ReturnType<typeof setInterval> | null>(null)
  const LS_KEY = 'progress_toast_active'
  const persist = useCallback((p: any) => { try { localStorage.setItem(LS_KEY, JSON.stringify(p)) } catch {} }, [])
  const getPersist = useCallback(() => { try { const s = localStorage.getItem(LS_KEY); return s ? JSON.parse(s) : null } catch { return null } }, [])
  const clearPersist = useCallback(() => { try { localStorage.removeItem(LS_KEY) } catch {} }, [])

  const show = useCallback((title: string, message?: string) => {
    setState((s: ProgressToastState) => ({ ...s, visible: true, title, message }))
  }, [])

  const hide = useCallback(() => {
    setState({ visible: false })
    if (timerRef.current) {
      clearInterval(timerRef.current as unknown as number)
      timerRef.current = null
    }
    if (tickRef.current) {
      clearInterval(tickRef.current as unknown as number)
      tickRef.current = null
    }
    try { clearPersist() } catch {}
  }, [])

  const update = useCallback((p: Partial<ProgressToastState>) => {
    setState((s: ProgressToastState) => ({ ...s, ...p }))
  }, [])

  const computeProgress = useCallback((tasks: SyncTaskOut[]) => {
    const running = tasks.filter((t) => t.inProgress)
    if (running.length === 0) return { percent: 1, current: 0, total: 0, runningTasks: 0 }
    let curSum = 0
    let totSum = 0
    let unknown = 0
    let phase: string | null = null
    for (const t of running) {
      const c = Number(t.progressCurrent ?? 0)
      const tot = t.progressTotal
      curSum += isFinite(c) ? c : 0
      if (typeof tot === 'number' && isFinite(tot)) totSum += tot
      else unknown += 1
      if (!phase) phase = (t.progressPhase || null) as any
      else if (t.progressPhase === 'insert') phase = 'insert'
    }
    if (totSum > 0) {
      const p = Math.max(0, Math.min(1, curSum / totSum))
      return { percent: p, current: curSum, total: totSum, runningTasks: running.length, phase }
    }
    // Unknown totals -> indeterminate
    return { percent: null, current: curSum, total: null, runningTasks: running.length, phase }
  }, [])

  const startMonitoring = useCallback((datasourceId: string, actorId?: string) => {
    // Immediately show toast — start time will be overridden by server startedAt on first poll
    const now = Date.now()
    setState({ visible: true, title: 'Sync in progress', message: 'Starting…', datasourceId, monitoring: true, minimized: false, actorId, startedAtMs: now, elapsedSec: 0 })
    try { persist({ active: true, datasourceId, actorId, minimized: false, startedAtMs: now }) } catch {}
    if (timerRef.current) {
      clearInterval(timerRef.current as unknown as number)
      timerRef.current = null
    }
    if (tickRef.current) {
      clearInterval(tickRef.current as unknown as number)
      tickRef.current = null
    }
    const poll = async () => {
      try {
        const tasks = await Api.getSyncStatus(datasourceId, actorId)
        const { percent, current, total, runningTasks, phase } = computeProgress(tasks)
        // Derive startedAtMs from server's startedAt (earliest running task)
        let serverStartMs: number | undefined
        for (const t of tasks) {
          if (t.inProgress && t.startedAt) {
            const ms = new Date(t.startedAt).getTime()
            if (!isNaN(ms) && (serverStartMs === undefined || ms < serverStartMs)) serverStartMs = ms
          }
        }
        setState((s: ProgressToastState) => {
          const startedAtMs = serverStartMs ?? s.startedAtMs ?? Date.now()
          const next = { ...s, visible: true, title: 'Sync in progress', message: runningTasks ? `Running ${runningTasks} task(s)…` : 'Finishing…', percent, current, total, runningTasks, phase: (phase || s.phase || null), monitoring: runningTasks > 0, datasourceId: s.datasourceId || datasourceId, actorId: s.actorId || actorId, startedAtMs }
          try { persist({ active: runningTasks > 0, datasourceId: next.datasourceId, actorId: next.actorId, minimized: !!next.minimized, startedAtMs }) } catch {}
          return next
        })
        if (!tasks.some((t) => t.inProgress)) {
          // Done - invalidate query cache so UI updates
          void queryClient.invalidateQueries({ queryKey: ['sync-tasks', datasourceId] })
          setState((s: ProgressToastState) => ({ ...s, visible: true, title: 'Sync complete', message: 'All tasks finished', percent: 1, phase: null }))
          if (timerRef.current) {
            clearInterval(timerRef.current as unknown as number)
            timerRef.current = null
          }
          if (tickRef.current) {
            clearInterval(tickRef.current as unknown as number)
            tickRef.current = null
          }
          // Auto-hide after a short delay
          setTimeout(() => hide(), 2000)
          try { clearPersist() } catch {}
        }
      } catch (e) {
        setState((s: ProgressToastState) => ({ ...s, visible: true, title: 'Sync', message: 'Updating status…' }))
      }
    }
    // Kick off immediately, then poll every 2s
    void poll()
    timerRef.current = setInterval(poll, 2000)
    // Tick elapsed every 1s
    tickRef.current = setInterval(() => {
      setState((s: ProgressToastState) => {
        if (!s.monitoring) return s
        const start = s.startedAtMs || now
        const el = Math.max(0, Math.floor((Date.now() - start) / 1000))
        return { ...s, elapsedSec: el }
      })
    }, 1000)
  }, [computeProgress, hide, queryClient])

  useEffect(() => () => {
    if (timerRef.current) clearInterval(timerRef.current as unknown as number)
    if (tickRef.current) clearInterval(tickRef.current as unknown as number)
  }, [])

  const value = useMemo(() => ({ state, show, hide, update, startMonitoring }), [state, show, hide, update, startMonitoring])

  useEffect(() => {
    try {
      const p = getPersist()
      if (p && p.active && p.datasourceId) {
        startMonitoring(p.datasourceId as string, p.actorId as string | undefined)
        setState((s: ProgressToastState) => ({ ...s, visible: true, minimized: !!p.minimized }))
      }
    } catch {}
  }, [getPersist, startMonitoring])

  return (
    <ProgressToastContext.Provider value={value}>
      {children}
      {state.visible && (
        state.minimized ? (
          <div className="fixed bottom-4 right-4 z-[9999] w-[260px] rounded-md border bg-card shadow-card px-3 py-2 cursor-pointer" onClick={() => setState((s: ProgressToastState) => { const n = { ...s, minimized: false, visible: true }; try { const p = getPersist(); if (p) persist({ ...p, minimized: false }) } catch {}; return n })}>
            <div className="flex items-center justify-between">
              <div className="text-xs font-semibold">{state.title || 'Sync'}</div>
              <div className="text-[11px] text-muted-foreground">{state.runningTasks ? `${state.runningTasks} task(s)` : ''}</div>
            </div>
            <div className="mt-1">
              {state.percent === null ? (
                <div className="h-1.5 w-full bg-muted rounded overflow-hidden">
                  <div className="h-1.5 w-1/3 bg-primary animate-pulse" />
                </div>
              ) : (
                <div className="h-1.5 w-full bg-muted rounded overflow-hidden">
                  <div className="h-1.5 bg-primary" style={{ width: `${Math.round((state.percent || 0) * 100)}%` }} />
                </div>
              )}
              <div className="mt-1 text-[10px] text-muted-foreground">
                <span className="capitalize">{state.phase === 'insert' ? 'inserted' : state.phase === 'fetch' ? 'fetched' : 'rows'}</span>
                : {typeof state.current === 'number' ? state.current.toLocaleString() : '—'}{typeof state.total === 'number' ? ` / ${state.total.toLocaleString()} rows` : ' rows'}
                {typeof state.elapsedSec === 'number' ? ` • ${state.elapsedSec}s` : ''}
              </div>
            </div>
          </div>
        ) : (
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
                <span className="capitalize">{state.phase === 'insert' ? 'Inserted' : state.phase === 'fetch' ? 'Fetched' : 'Rows'}</span>
                : {typeof state.current === 'number' ? state.current.toLocaleString() : '—'}{typeof state.total === 'number' ? ` / ${state.total.toLocaleString()} rows` : ' rows'}
              </div>
              <div className="mt-1 text-[11px] text-muted-foreground flex items-center justify-between">
                <span>Phase: <span className="capitalize">{state.phase || '—'}</span></span>
                <span>Elapsed: {typeof state.elapsedSec === 'number' ? `${state.elapsedSec}s` : '—'}</span>
              </div>
            </div>
            <div className="px-3 pb-2 flex justify-end gap-2">
              <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => setState((s: ProgressToastState) => { const n = { ...s, minimized: true, visible: true }; try { const p = getPersist(); if (p) persist({ ...p, minimized: true }); else if (s.datasourceId) persist({ active: true, datasourceId: s.datasourceId, actorId: s.actorId, minimized: true }) } catch {}; return n })}>Minimize</button>
              <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={hide}>Hide</button>
            </div>
          </div>
        )
      )}
    </ProgressToastContext.Provider>
  )
}
