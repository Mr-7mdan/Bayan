"use client"

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { Api, type IssueReportIn } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'

function parseStackTop(stack?: string): { file?: string; line?: number; column?: number } {
  try {
    const s = (stack || '').split('\n').map((x)=>x.trim())
    for (const line of s) {
      // Chrome/Edge: at func (https://host/path/file.js:123:45)
      let m = line.match(/\((.*):(\d+):(\d+)\)$/)
      if (!m) m = line.match(/at\s+([^\s]+)\s+\((.*):(\d+):(\d+)\)/)
      if (!m) m = line.match(/(https?:\/\/.*):(\d+):(\d+)/)
      if (m) {
        const file = m[1]
        const ln = parseInt(m[2] || '0', 10)
        const col = parseInt(m[3] || '0', 10)
        return { file, line: isNaN(ln) ? undefined : ln, column: isNaN(col) ? undefined : col }
      }
    }
  } catch {}
  return {}
}

function getFrontendVersion(): string | undefined {
  try {
    if (typeof window !== 'undefined') {
      const raw = window.localStorage.getItem('last_seen_versions')
      if (raw) {
        try {
          const obj = JSON.parse(raw)
          const v = (obj?.frontend || '').trim()
          if (v) return v
        } catch {}
      }
    }
  } catch {}
  return undefined
}

function getBrowserInfo(): string | undefined {
  try { return typeof navigator !== 'undefined' ? navigator.userAgent : undefined } catch { return undefined }
}

export default function ErrorReporterProvider({ children }: { children: React.ReactNode }) {
  const { user } = useAuth()
  const { env } = useEnvironment()
  const sentRef = useRef<Map<string, number>>(new Map())
  const appVersion = useMemo(() => getFrontendVersion(), [])
  const userIdRef = useRef<string | undefined>(user?.id)
  const appVersionRef = useRef<string | undefined>(appVersion)
  useEffect(() => { userIdRef.current = user?.id }, [user?.id])
  useEffect(() => { appVersionRef.current = appVersion }, [appVersion])
  const [askOpen, setAskOpen] = useState(false)
  const [askPayload, setAskPayload] = useState<IssueReportIn | null>(null)
  const [askBusy, setAskBusy] = useState(false)
  const [toast, setToast] = useState<{ show: boolean; text: string; kind?: 'ok'|'err'|'info' }>({ show: false, text: '' })

  const report = async (payload: IssueReportIn) => {
    try {
      const key = `${payload.errorName || ''}|${payload.file || ''}|${payload.line || ''}|${(payload.message || '').slice(0, 120)}|${payload.appVersion || ''}`
      const now = Date.now()
      const last = sentRef.current.get(key) || 0
      if (now - last < 60_000) return
      sentRef.current.set(key, now)
      await Api.reportIssue(payload)
    } catch { /* ignore */ }
  }

  useEffect(() => {
    const onError = (ev: ErrorEvent) => {
      try {
        const e = ev.error as any
        const name = (e?.name || 'Error') as string
        const msg = String(e?.message || ev.message || '')
        const st = String(e?.stack || ev.error?.stack || '')
        const loc = parseStackTop(st)
        const payload: IssueReportIn = {
          kind: 'frontend',
          errorName: name,
          message: msg,
          stack: st,
          file: loc.file,
          line: loc.line,
          column: loc.column,
          url: typeof window !== 'undefined' ? window.location.href : undefined,
          appVersion: appVersionRef.current,
          environment: process.env.NODE_ENV,
          browser: getBrowserInfo(),
          userId: userIdRef.current,
          metadata: { type: 'window.error' },
          occurredAt: new Date().toISOString(),
        }
        const mode = env?.bugReportMode || 'auto'
        if (mode === 'off') return
        if (mode === 'ask') {
          if (!askOpen) { setAskPayload(payload); setAskOpen(true) }
          return
        }
        void report(payload)
      } catch { /* noop */ }
    }
    const onRejection = (ev: PromiseRejectionEvent) => {
      try {
        const reason = (ev.reason instanceof Error) ? ev.reason : new Error(typeof ev.reason === 'string' ? ev.reason : JSON.stringify(ev.reason))
        const st = String(reason?.stack || '')
        const loc = parseStackTop(st)
        const payload: IssueReportIn = {
          kind: 'frontend',
          errorName: reason?.name || 'UnhandledRejection',
          message: String(reason?.message || ''),
          stack: st,
          file: loc.file,
          line: loc.line,
          column: loc.column,
          url: typeof window !== 'undefined' ? window.location.href : undefined,
          appVersion: appVersionRef.current,
          environment: process.env.NODE_ENV,
          browser: getBrowserInfo(),
          userId: userIdRef.current,
          metadata: { type: 'window.unhandledrejection' },
          occurredAt: new Date().toISOString(),
        }
        const mode = env?.bugReportMode || 'auto'
        if (mode === 'off') return
        if (mode === 'ask') {
          if (!askOpen) { setAskPayload(payload); setAskOpen(true) }
          return
        }
        void report(payload)
      } catch { /* noop */ }
    }
    if (typeof window !== 'undefined') {
      window.addEventListener('error', onError)
      window.addEventListener('unhandledrejection', onRejection)
    }
    return () => {
      if (typeof window !== 'undefined') {
        window.removeEventListener('error', onError)
        window.removeEventListener('unhandledrejection', onRejection)
      }
    }
  }, [user?.id, appVersion, env?.bugReportMode, askOpen])

  class ErrorBoundary extends React.Component<{ children: React.ReactNode }, { hasError: boolean }> {
    constructor(props: any) {
      super(props)
      this.state = { hasError: false }
    }
    componentDidCatch(error: Error, info: any) {
      const st = String(error?.stack || '')
      const loc = parseStackTop(st)
      const payload: IssueReportIn = {
        kind: 'frontend',
        errorName: error?.name || 'Error',
        message: String(error?.message || ''),
        stack: st,
        componentStack: String(info?.componentStack || ''),
        file: loc.file,
        line: loc.line,
        column: loc.column,
        url: typeof window !== 'undefined' ? window.location.href : undefined,
        appVersion: appVersionRef.current,
        environment: process.env.NODE_ENV,
        browser: getBrowserInfo(),
        userId: userIdRef.current,
        metadata: { type: 'react.error' },
        occurredAt: new Date().toISOString(),
      }
      const mode = (env as any)?.bugReportMode || 'auto'
      if (mode === 'off') { this.setState({ hasError: false }); return }
      if (mode === 'ask') { if (!askOpen) { setAskPayload(payload); setAskOpen(true) } this.setState({ hasError: false }); return }
      void report(payload)
      this.setState({ hasError: false })
    }
    render() { return this.props.children as any }
  }

  const askTitle = (() => {
    if (!askPayload) return ''
    const loc = `${askPayload.file || ''}${askPayload.line ? `:${askPayload.line}` : ''}`
    return `Bug detected${loc ? ` at ${loc}` : ''}`
  })()

  return (
    <>
      <ErrorBoundary>{children}</ErrorBoundary>
      {askOpen && askPayload && (
        <div className="fixed inset-0 z-[2000] flex items-center justify-center bg-black/30">
          <div className="w-[440px] max-w-[95vw] rounded-md border bg-card p-3 shadow-lg">
            <div className="text-sm font-semibold mb-1">{askTitle}</div>
            <div className="text-xs text-muted-foreground mb-2">{askPayload.errorName || 'Error'}: {(askPayload.message || '').slice(0, 180)}</div>
            <div className="text-[11px] text-muted-foreground mb-3">Submit this bug to the maintainers?</div>
            <div className="flex items-center justify-end gap-2">
              <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" disabled={askBusy} onClick={() => { setAskOpen(false); setAskPayload(null); setToast({ show: true, text: 'Bug report canceled', kind: 'info' }); setTimeout(()=>setToast({ show: false, text: '' }), 1800) }}>Dismiss</button>
              <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" disabled={askBusy} onClick={async () => { try { setAskBusy(true); await report(askPayload); setAskOpen(false); setAskPayload(null); setToast({ show: true, text: 'Bug submitted', kind: 'ok' }) } catch { setToast({ show: true, text: 'Failed to submit bug', kind: 'err' }) } finally { setAskBusy(false); setTimeout(()=>setToast({ show: false, text: '' }), 2000) } }}>Submit</button>
            </div>
          </div>
        </div>
      )}
      {toast.show && (
        <div className="fixed bottom-4 right-4 z-[2000]">
          <div className={`px-3 py-2 rounded-md border text-xs shadow-md ${toast.kind==='ok' ? 'bg-emerald-600 text-white' : toast.kind==='err' ? 'bg-rose-600 text-white' : 'bg-card text-foreground'}`}>{toast.text}</div>
        </div>
      )}
    </>
  )
}
