"use client"

import { useEffect, useMemo, useRef, useState } from 'react'
import { useTranslations } from 'next-intl'
import { Card, Title, Text, Select, SelectItem } from '@tremor/react'
import Switch from '@/components/Switch'
import { Api, type AlertOut, type AlertCreate } from '@/lib/api'
import { swallow } from '@/lib/log'
import { RiPlayLine, RiDeleteBinLine, RiRefreshLine, RiSettings3Line, RiAddLine, RiEdit2Line, RiAlarmWarningLine, RiNotificationBadgeLine, RiMailLine, RiMessage2Line } from '@remixicon/react'
import EmailConfigDialog from '@/components/alerts/EmailConfigDialog'
import SmsConfigDialog from '@/components/alerts/SmsConfigDialog'
import AlertDialog from '@/components/alerts/AlertDialog'
import { useAuth } from '@/components/providers/AuthProvider'
import { useProgressToast } from '@/components/providers/ProgressToastProvider'
import { Button } from '@/components/ui'

function parseISOForJS(iso?: string | null): Date | null {
  try {
    if (!iso) return null
    let s = String(iso).trim()
    if (/^\d{4}-\d{2}-\d{2}\s/.test(s)) s = s.replace(' ', 'T')
    if (/\.[0-9]{4,}$/.test(s)) s = s.replace(/(\.[0-9]{3})[0-9]+$/, '$1')
    if (/^\d{4}-\d{2}-\d{2}T/.test(s) && !/[zZ]|[+-]\d{2}:\d{2}$/.test(s)) s = s + 'Z'
    const d = new Date(s)
    return isNaN(d.getTime()) ? null : d
  } catch { return null }
}

function fmt(iso?: string | null) {
  try {
    if (!iso) return '—'
    const d = parseISOForJS(iso)
    if (!d) return '—'
    if (isNaN(d.getTime())) return '—'
    const dd = String(d.getDate()).padStart(2, '0')
    const mm = String(d.getMonth() + 1).padStart(2, '0')
    const yyyy = d.getFullYear()
    let hh = d.getHours()
    const ampm = hh >= 12 ? 'PM' : 'AM'
    hh = hh % 12
    if (hh === 0) hh = 12
    const hhS = String(hh).padStart(2, '0')
    const min = String(d.getMinutes()).padStart(2, '0')
    return `${dd}/${mm}/${yyyy} ${hhS}:${min} ${ampm}`
  } catch { return '—' }
}

type TFn = (key: string, values?: Record<string, any>) => string

function fmtNextRun(iso: string | null | undefined, t: TFn): string {
  try {
    if (!iso) return '—'
    const now = new Date()
    const d = new Date(iso)
    if (isNaN(d.getTime())) return '—'
    const diffMs = d.getTime() - now.getTime()
    if (diffMs < 0) return '—'
    const s = Math.floor(diffMs / 1000)
    const m = Math.floor(s / 60)
    const h = Math.floor(m / 60)
    const dys = Math.floor(h / 24)
    if (dys >= 1) return t('alerts.relativeTime.inDays', { n: dys })
    if (h >= 1) return t('alerts.relativeTime.inHrs', { n: h })
    if (m >= 1) return t('alerts.relativeTime.inMinutes', { n: m })
    return t('alerts.relativeTime.inSeconds', { n: s })
  } catch { return '—' }
}

function fmtLastRun(iso: string | null | undefined, t: TFn): string {
  try {
    if (!iso) return '—'
    const now = new Date()
    const d = parseISOForJS(iso)
    if (!d) return '—'
    if (isNaN(d.getTime())) return '—'
    const diffMs = now.getTime() - d.getTime()
    if (diffMs < 0) return fmt(iso)
    const s = Math.floor(diffMs / 1000)
    const m = Math.floor(s / 60)
    const h = Math.floor(m / 60)
    const dys = Math.floor(h / 24)
    if (dys > 1) return fmt(iso)
    if (dys === 1) return t('alerts.relativeTime.dayAgo')
    if (h >= 1) return t('alerts.relativeTime.hrsAgo', { n: h })
    if (m >= 1) return t('alerts.relativeTime.minutesAgo', { n: m })
    return t('alerts.relativeTime.secondsAgo', { n: s })
  } catch { return '—' }
}


function nextRunFromCron(cron: string, now?: Date): string | null {
  try {
    const parts = (cron || '').trim().split(/\s+/)
    if (parts.length < 5) return null
    const [mmS, hhS, domS, monS, dowS] = parts
    const mm = parseInt(mmS, 10)
    const hh = parseInt(hhS, 10)
    if (isNaN(mm) || isNaN(hh) || mm < 0 || mm > 59 || hh < 0 || hh > 23) return null
    const parseList = (s: string): Set<number> | null => {
      const t = (s || '').trim()
      if (t === '*') return null
      const set = new Set<number>()
      for (const v of t.split(',').map(x => x.trim()).filter(Boolean)) {
        const n = parseInt(v, 10)
        if (!isNaN(n)) set.add(n)
      }
      return set
    }
    const domSet = parseList(domS)
    const monSet = parseList(monS)
    // Convert APScheduler DOW (0=Mon..6=Sun) in cron to JS DOW (0=Sun..6=Sat) for comparison
    const dowSetRaw = parseList(dowS)
    const dowSet = dowSetRaw ? new Set([...dowSetRaw].map(d => d === 6 ? 0 : d + 1)) : null
    const base = now ? new Date(now) : new Date()
    for (let i = 0; i < 370; i++) {
      const d = new Date(base.getFullYear(), base.getMonth(), base.getDate(), hh, mm, 0, 0)
      d.setDate(d.getDate() + i)
      if (monSet && !monSet.has(d.getMonth() + 1)) continue
      const dayOkDom = !domSet || domSet.has(d.getDate())
      const dow = d.getDay() // 0..6, Sun=0
      const dayOkDow = !dowSet || dowSet.has(dow)
      const dayOk = (domSet && dowSet) ? (dayOkDom || dayOkDow) : (dayOkDom && dayOkDow)
      if (!dayOk) continue
      if (i === 0 && d <= base) continue
      return d.toISOString()
    }
    return null
  } catch { return null }
}

function toCreatePayload(a: AlertOut): AlertCreate {
  return {
    name: a.name,
    kind: a.kind,
    widgetId: (a as any).widgetId || undefined,
    dashboardId: (a as any).dashboardId || undefined,
    enabled: a.enabled,
    config: a.config,
  }
}

function QuickAddDialog({ open, onOpenChange, onCreated }: { open: boolean; onOpenChange: (v: boolean) => void; onCreated: (a: AlertOut) => void }) {
  const t = useTranslations('comms')
  const [name, setName] = useState(() => t('alerts.quickAdd.defaultName'))
  const [emails, setEmails] = useState('')
  const [time, setTime] = useState('09:00')
  const [scheduleKind, setScheduleKind] = useState<'weekly'|'monthly'>('weekly')
  const [dows, setDows] = useState<number[]>([1,2,3,4,5])
  const [doms, setDoms] = useState<number[]>([])
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  useEffect(() => { if (open) { setName(t('alerts.quickAdd.defaultName')); setEmails(''); setTime('09:00'); setScheduleKind('weekly'); setDows([1,2,3,4,5]); setDoms([]); setError(null); setSaving(false) } }, [open])
  if (!open || typeof document === 'undefined') return null
  const buildCron = (t: string, kind: 'weekly'|'monthly', dowsArr: number[], domsArr: number[]) => {
    try {
      const [hh, mm] = (t || '09:00').split(':').map((x) => parseInt(x, 10))
      if (kind === 'monthly') {
        const domList = (domsArr||[]).join(',') || '*'
        return `${isNaN(mm)?0:mm} ${isNaN(hh)?9:hh} ${domList} * *`
      }
      const list = (dowsArr||[]).join(',') || '*'
      return `${isNaN(mm)?0:mm} ${isNaN(hh)?9:hh} * * ${list}`
    } catch { return '0 9 * * 1,2,3,4,5' }
  }
  const onSave = async () => {
    try {
      setSaving(true); setError(null)
      const cron = buildCron(time, scheduleKind, dows, doms)
      const to = emails.split(',').map((s)=>s.trim()).filter(Boolean)
      if (!to.length) { setError(t('alerts.quickAdd.noRecipient')); setSaving(false); return }
      const payload: AlertCreate = {
        name,
        kind: 'notification',
        enabled: true,
        config: {
          triggers: [{ type: 'time', cron }],
          actions: [{ type: 'email', to, subject: name }],
          render: { mode: 'kpi', label: name },
          template: 'Scheduled notification',
        } as any,
      }
      const res = await Api.createAlert(payload)
      onCreated(res)
      onOpenChange(false)
    } catch (e: any) { setError(e?.message || t('alerts.quickAdd.failedToCreate')) } finally { setSaving(false) }
  }
  return (
    (typeof document !== 'undefined') ? (
      <div className="fixed inset-0 z-[1200]">
        <div className="absolute inset-0 bg-black/40" onClick={() => !saving && onOpenChange(false)} />
        <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[620px] max-w-[95vw] rounded-lg border bg-card p-4">
          <div className="flex items-center justify-between mb-2"><div className="text-sm font-medium">{t('alerts.quickAdd.title')}</div><button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => onOpenChange(false)} disabled={saving}>✕</button></div>
          {error && <div className="mb-2 text-xs text-rose-600">{error}</div>}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <label className="text-sm">{t('alerts.quickAdd.name')}<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={name} onChange={(e)=>setName(e.target.value)} /></label>
            <label className="text-sm">{t('alerts.quickAdd.emailTo')}<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="user@org.com,another@org.com" value={emails} onChange={(e)=>setEmails(e.target.value)} /></label>
            <label className="text-sm">{t('alerts.quickAdd.time')}<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="09:00" value={time} onChange={(e)=>setTime(e.target.value)} /></label>
            <div className="text-sm">
              <div className="mb-1">{t('alerts.quickAdd.schedule')}</div>
              <div className="inline-flex rounded-md border overflow-hidden">
                <button type="button" className={`px-2 py-1 text-xs ${scheduleKind==='weekly'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setScheduleKind('weekly')}>{t('alerts.quickAdd.weekly')}</button>
                <button type="button" className={`px-2 py-1 text-xs border-l ${scheduleKind==='monthly'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setScheduleKind('monthly')}>{t('alerts.quickAdd.monthly')}</button>
              </div>
              {scheduleKind === 'weekly' ? (
                <div className="mt-2 flex flex-wrap gap-1 text-xs">
                  {[{v:0,l:t('alerts.quickAdd.days.sun')},{v:1,l:t('alerts.quickAdd.days.mon')},{v:2,l:t('alerts.quickAdd.days.tue')},{v:3,l:t('alerts.quickAdd.days.wed')},{v:4,l:t('alerts.quickAdd.days.thu')},{v:5,l:t('alerts.quickAdd.days.fri')},{v:6,l:t('alerts.quickAdd.days.sat')}].map(d => (
                    <button key={d.v} type="button" className={`px-2 py-1 rounded-md border ${dows.includes(d.v)?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setDows((prev)=> prev.includes(d.v) ? prev.filter(x=>x!==d.v) : [...prev, d.v].sort((a,b)=>a-b))}>{d.l}</button>
                  ))}
                </div>
              ) : (
                <div className="mt-2 grid grid-cols-7 gap-1 text-xs">
                  {Array.from({ length: 31 }).map((_,i)=> i+1).map((d)=> (
                    <button key={d} type="button" className={`px-2 py-1 rounded-md border ${doms.includes(d)?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setDoms((prev)=> prev.includes(d) ? prev.filter(x=>x!==d) : [...prev, d].sort((a,b)=>a-b))}>{d}</button>
                  ))}
                </div>
              )}
            </div>
          </div>
          <div className="mt-4 flex items-center gap-2"><button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" disabled={saving} onClick={onSave}>{saving?t('alerts.quickAdd.saving'):t('alerts.quickAdd.save')}</button><button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" disabled={saving} onClick={() => onOpenChange(false)}>{t('alerts.quickAdd.cancel')}</button></div>
        </div>
      </div>
    ) : null
  )
}

export default function AlertsPage() {
  const t = useTranslations('comms')
  const { user } = useAuth()
  const isAdmin = String(user?.role || '').toLowerCase() === 'admin'
  const [items, setItems] = useState<AlertOut[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const { notify } = useProgressToast()
  const setToast = (m: string) => { if (m) notify(m, /fail|error|invalid|فشل|تعذّر|خطأ/i.test(m) ? 'error' : 'success') }
  const [dlgAdd, setDlgAdd] = useState(false)
  const [dlgEmail, setDlgEmail] = useState(false)
  const [dlgSms, setDlgSms] = useState(false)
  const [editOpen, setEditOpen] = useState(false)
  const [editTarget, setEditTarget] = useState<AlertOut | null>(null)
  const [createOpen, setCreateOpen] = useState(false)
  const [query, setQuery] = useState('')
  const [pageSize, setPageSize] = useState(8)
  const [page, setPage] = useState(0)
  const [sortKey, setSortKey] = useState<'name'|'kind'|'enabled'|'lastRunAt'|'lastStatus'>('name')
  const [sortDir, setSortDir] = useState<'asc'|'desc'>('asc')
  const [running, setRunning] = useState<Record<string, boolean>>({})
  const [runProg, setRunProg] = useState<Record<string, { open: boolean; steps: Array<{ id: string; status: 'start'|'ok'|'error'; ts?: string; mode?: string; to?: number; error?: string }>; final?: string; doneAt?: number }>>({})
  const runTimers = useRef<Record<string, any>>({}) as any

  useEffect(() => {
    let cancelled = false
    async function run() {
      setLoading(true); setError(null)
      try { const res = await Api.listAlerts(); if (!cancelled) setItems(Array.isArray(res)?res:[]) } catch (e: any) { if (!cancelled) setError(e?.message || t('alerts.toasts.failedToLoad')) } finally { if (!cancelled) setLoading(false) }
    }
    void run(); return () => { cancelled = true }
  }, [])

  const onRun = async (id: string) => {
    const startPolling = () => {
      // Initialize progress UI immediately
      setRunProg((prev)=> ({ ...prev, [id]: { open: true, steps: [{ id: 'calc', status: 'start' }], final: undefined } }))
      // Clear existing timer if any
      try { if (runTimers.current[id]) { clearInterval(runTimers.current[id]); delete runTimers.current[id] } } catch {}
      // Poll latest run every 1s
      runTimers.current[id] = setInterval(async () => {
        try {
          const rows = await Api.listAlertRuns(id, 1)
          const row = Array.isArray(rows) && rows.length ? rows[0] : null
          if (!row) return
          let steps: any[] = []
          try { const m = row.message || ''; const o = typeof m === 'string' ? JSON.parse(m) : m; if (o && Array.isArray(o.steps)) steps = o.steps } catch {}
          setRunProg((prev)=> ({ ...prev, [id]: { open: true, steps: steps.map((s) => ({ id: String(s.id||'').toLowerCase(), status: (s.status==='ok'?'ok':(s.status==='error'?'error':'start')), ts: s.ts, mode: s.mode, to: s.to, error: s.error })), final: row.status || undefined, doneAt: row.finishedAt ? Date.now() : (prev[id]?.doneAt) } }))
          if (row.finishedAt || (row.status && (row.status==='ok' || row.status==='failed'))) {
            clearInterval(runTimers.current[id]); delete runTimers.current[id]
            // Refresh alert row so lastRunAt updates in the table
            try { const updated = await Api.getAlert(id); setItems((prev)=>prev.map((x)=>x.id===id?updated:x)) } catch (e) { swallow(e, 'alerts.refreshRow') }
          }
        } catch {}
      }, 1000)
    }
    try {
      setRunning((prev) => ({ ...prev, [id]: true }))
      startPolling()
      const r = await Api.runAlertNow(id)
      const msg = r?.message || t('alerts.toasts.triggered')
      setToast(msg); setTimeout(()=>setToast(''), 1500)
    } catch (e: any) {
      setToast(e?.message || t('alerts.toasts.failed')); setTimeout(()=>setToast(''), 1800)
    } finally {
      setRunning((prev) => ({ ...prev, [id]: false }))
    }
  }
  const onDelete = async (id: string) => {
    try {
      if (typeof window !== 'undefined') { const ok = window.confirm(t('alerts.confirm.delete')); if (!ok) return }
      await Api.deleteAlert(id)
      setItems((prev)=>prev.filter((x)=>x.id!==id))
      try { if (typeof window !== 'undefined') window.dispatchEvent(new Event('sidebar-counts-refresh')) } catch {}
      setToast(t('alerts.toasts.deleted')); setTimeout(()=>setToast(''), 1500)
    } catch (e: any) { setToast(e?.message || t('alerts.toasts.failed')); setTimeout(()=>setToast(''), 1800) }
  }
  const onToggleEnabled = async (a: AlertOut, next: boolean) => {
    try { const payload = { ...toCreatePayload(a), enabled: next } as AlertCreate; const res = await Api.updateAlert(a.id, payload); setItems((prev)=>prev.map((x)=>x.id===a.id?res:x)); setToast(next?t('alerts.toasts.enabled'):t('alerts.toasts.disabled')); setTimeout(()=>setToast(''), 1200) } catch (e: any) { setToast(e?.message || t('alerts.toasts.failed')); setTimeout(()=>setToast(''), 1800) }
  }
  const onCreated = (a: AlertOut) => { setItems((prev)=>[a, ...prev]); try { if (typeof window !== 'undefined') window.dispatchEvent(new Event('sidebar-counts-refresh')) } catch {}; setToast(t('alerts.toasts.created')); setTimeout(()=>setToast(''), 1500) }
  const onEdit = (a: AlertOut) => { setEditTarget(a); setEditOpen(true) }
  const onSaved = (a: AlertOut) => { setItems((prev)=>prev.map((x)=>x.id===a.id?a:x)); try { if (typeof window !== 'undefined') window.dispatchEvent(new Event('sidebar-counts-refresh')) } catch {}; setToast(t('alerts.toasts.saved')); setTimeout(()=>setToast(''), 1500) }
  const onRefreshJobs = async () => {
    try {
      const actorId = user?.id || ''
      const res = await Api.adminSchedulerRefresh(actorId)
      const msg = res ? t('alerts.toasts.refreshed', { added: res.added, updated: res.updated, removed: res.removed, total: res.total }) : t('alerts.toasts.refreshedScheduler')
      setToast(msg); setTimeout(()=>setToast(''), 2500)
    } catch (e: any) { setToast(e?.message || t('alerts.toasts.failedToRefresh')); setTimeout(()=>setToast(''), 2000) }
  }

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return items
    return items.filter((a) => a.name.toLowerCase().includes(q) || a.kind.toLowerCase().includes(q) || (a.lastStatus||'').toLowerCase().includes(q))
  }, [items, query])
  const sorted = useMemo(() => {
    const arr = [...filtered]
    arr.sort((a, b) => {
      const dir = (sortDir === 'asc') ? 1 : -1
      const ka = (sortKey === 'enabled') ? (a.enabled ? 1 : 0) : (sortKey === 'lastRunAt' ? (a.lastRunAt || '') : (sortKey === 'lastStatus' ? (a.lastStatus || '') : (a as any)[sortKey] || ''))
      const kb = (sortKey === 'enabled') ? (b.enabled ? 1 : 0) : (sortKey === 'lastRunAt' ? (b.lastRunAt || '') : (sortKey === 'lastStatus' ? (b.lastStatus || '') : (b as any)[sortKey] || ''))
      if (typeof ka === 'number' && typeof kb === 'number') return (ka - kb) * dir
      return String(ka).localeCompare(String(kb)) * dir
    })
    return arr
  }, [filtered, sortKey, sortDir])
  const totalPages = Math.max(1, Math.ceil(filtered.length / pageSize))
  const visible = useMemo(() => sorted.slice(page * pageSize, page * pageSize + pageSize), [sorted, page, pageSize])
  useEffect(() => { setPage(0) }, [query, pageSize])
  // When alerts list length changes, refresh sidebar counts to update the badge
  useEffect(() => {
    try { if (typeof window !== 'undefined') window.dispatchEvent(new Event('sidebar-counts-refresh')) } catch {}
  }, [items.length])

  return (
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--card))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">{t('alerts.header.title')}</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">{t('alerts.header.subtitle')}</Text>
          </div>
          <div className="flex items-center gap-2">
            <Button size="sm" variant="primary" icon={<RiAddLine className="w-4 h-4" />} onClick={() => setCreateOpen(true)}>{t('alerts.toolbar.new')}</Button>
            {isAdmin && (
              <>
                <Button size="sm" variant="outline" icon={<RiSettings3Line className="w-4 h-4" />} onClick={() => setDlgEmail(true)}>{t('alerts.toolbar.emailConfig')}</Button>
                <Button size="sm" variant="outline" icon={<RiSettings3Line className="w-4 h-4" />} onClick={() => setDlgSms(true)}>{t('alerts.toolbar.smsConfig')}</Button>
              </>
            )}
            {isAdmin && (
              <Button size="sm" variant="outline" icon={<RiRefreshLine className="w-4 h-4" />} onClick={onRefreshJobs}>{t('alerts.toolbar.refreshScheduler')}</Button>
            )}
          </div>
        </div>
        {error && <div className="px-4 py-2 text-sm text-red-600">{error}</div>}
        <div className="px-3 py-2">
          <div className="flex items-center py-2 gap-2">
            <div className="flex items-center gap-2">
              <label htmlFor="searchAlerts" className="text-sm mr-2 text-gray-600 dark:text-gray-300">{t('alerts.toolbar.searchLabel')}</label>
              <input id="searchAlerts" value={query} onChange={(e) => setQuery(e.target.value)} placeholder={t('alerts.toolbar.searchPlaceholder')} className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]" />
            </div>
            <div className="ml-auto flex items-center gap-2 text-sm shrink-0">
              <span className="whitespace-nowrap min-w-[84px]">{t('alerts.toolbar.perPage')}</span>
              <div className="min-w-[96px] rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                [&_*]:!border-0 [&_*]:!border-transparent [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!ring-transparent [&_*]:!outline-none [&_*]:!shadow-none
                [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                <Select
                  value={String(pageSize)}
                  onValueChange={(v) => setPageSize(parseInt(v || '8') || 8)}
                  className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                >
                  <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="6">6</SelectItem>
                  <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="8">8</SelectItem>
                  <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="12">12</SelectItem>
                  <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="24">24</SelectItem>
                </Select>
              </div>
            </div>
          </div>
          <div className="overflow-auto rounded-xl border-2 border-[hsl(var(--border))]">
            <table className="min-w-full text-sm">
              <thead className="bg-[hsl(var(--card))] border-b border-[hsl(var(--border))]">
                <tr>
                  <th className="text-start px-3 py-2 font-medium">{t('alerts.table.name')}</th>
                  <th className="text-start px-3 py-2 font-medium">{t('alerts.table.type')}</th>
                  <th className="text-start px-3 py-2 font-medium">{t('alerts.table.lastRun')}</th>
                  <th className="text-start px-3 py-2 font-medium">{t('alerts.table.nextRun')}</th>
                  <th className="text-start px-3 py-2 font-medium">{t('alerts.table.status')}</th>
                  <th className="text-start px-3 py-2 font-medium">{t('alerts.table.actions')}</th>
                </tr>
              </thead>
              <tbody className="bg-[hsl(var(--background))]">
                {loading ? (
                  <tr><td className="px-3 py-3" colSpan={6}>{t('alerts.table.loading')}</td></tr>
                ) : (filtered.length === 0 ? (
                  <tr><td className="px-3 py-3" colSpan={6}>{t('alerts.table.empty')}</td></tr>
                ) : visible.map((a) => (
                  <tr key={a.id} className="border-t border-[hsl(var(--border))]">
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-2">
                        <Switch checked={!!a.enabled} onChangeAction={(v)=> onToggleEnabled(a, v)} />
                        <span>{a.name}</span>
                      </div>
                    </td>
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-2">
                        {String(a.kind || '').toLowerCase() === 'alert' ? (
                          <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs border bg-yellow-50 text-yellow-800 border-yellow-200 dark:bg-yellow-500/20 dark:text-yellow-300 dark:border-yellow-400/30" title={t('alerts.type.alert')}>
                            <RiAlarmWarningLine className="w-4 h-4" />
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs border bg-gray-900 text-white border-gray-700 dark:bg-white/10 dark:text-white dark:border-white/20" title={t('alerts.type.notification')}>
                            <RiNotificationBadgeLine className="w-4 h-4" />
                          </span>
                        )}
                        {Array.isArray((a as any).config?.actions) && (a as any).config.actions.some((ac: any) => String(ac?.type || '').toLowerCase() === 'email') && (
                          <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-700 dark:text-gray-300" title={t('alerts.type.email')}>
                            <RiMailLine className="w-4 h-4" />
                          </span>
                        )}
                        {Array.isArray((a as any).config?.actions) && (a as any).config.actions.some((ac: any) => String(ac?.type || '').toLowerCase() === 'sms') && (
                          <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-700 dark:text-gray-300" title={t('alerts.type.sms')}>
                            <RiMessage2Line className="w-4 h-4" />
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-3 py-2">{fmtLastRun(a.lastRunAt, t)}</td>
                    <td className="px-3 py-2">{(() => { try { const cron = ((a as any).config?.triggers || []).find((tr: any) => String(tr?.type || '').toLowerCase() === 'time')?.cron; if (a.enabled && typeof cron === 'string' && cron.trim()) { const n = nextRunFromCron(cron.trim()); return fmtNextRun(n || undefined, t) } return '—' } catch { return '—' } })()}</td>
                    <td className="px-3 py-2 truncate max-w-[320px]" title={a.lastStatus || ''}>{a.lastStatus || '—'}</td>
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-2">
                        <Button
                          size="sm"
                          variant="outline"
                          icon={<RiEdit2Line className="w-4 h-4" />}
                          title={t('alerts.tooltips.edit')}
                          aria-label={t('alerts.tooltips.edit')}
                          onClick={() => onEdit(a)}
                        />
                        <Button
                          size="sm"
                          variant="outline"
                          icon={<RiPlayLine className="w-4 h-4" />}
                          loading={!!running[a.id]}
                          title={t('alerts.tooltips.runNow')}
                          aria-label={t('alerts.tooltips.runNow')}
                          onClick={() => onRun(a.id)}
                        />
                        <Button
                          size="sm"
                          variant="danger"
                          icon={<RiDeleteBinLine className="w-4 h-4" />}
                          title={t('alerts.tooltips.delete')}
                          aria-label={t('alerts.tooltips.delete')}
                          onClick={() => onDelete(a.id)}
                        />
                      </div>
                    </td>
                  </tr>
                )))}
              </tbody>
            </table>
          </div>
          {!loading && filtered.length > 0 && (
            <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
              <span>{t('alerts.pagination.showing', { from: page * pageSize + 1, to: Math.min((page + 1) * pageSize, filtered.length), total: filtered.length })}</span>
              <div className="flex items-center gap-2">
                <Button size="sm" variant="outline" disabled={page <= 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>{t('alerts.pagination.prev')}</Button>
                <span>{t('alerts.pagination.pageOf', { page: page + 1, total: totalPages })}</span>
                <Button size="sm" variant="outline" disabled={page >= totalPages - 1} onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}>{t('alerts.pagination.next')}</Button>
              </div>
            </div>
          )}
        </div>
      </Card>
      {/* Run progress panel(s) */}
      <div className="fixed bottom-6 right-6 z-[110] space-y-2">
        {Object.entries(runProg).filter(([,v])=>v && v.open).map(([id, prog]) => (
          <div key={id} className="min-w-[280px] max-w-[360px] rounded-lg border bg-[hsl(var(--card))] p-3 shadow-lg">
            <div className="flex items-center justify-between mb-1">
              <div className="text-xs font-medium">{prog.final ? (prog.final==='ok'?t('alerts.runProgress.completed'):t('alerts.runProgress.finishedErrors')) : t('alerts.runProgress.running')}</div>
              <button className="text-[10px] px-1.5 py-0.5 rounded border hover:bg-[hsl(var(--muted))]" onClick={()=> setRunProg((prev)=> ({ ...prev, [id]: { ...(prev[id]||prog), open: false } }))}>{t('alerts.runProgress.close')}</button>
            </div>
            <div className="space-y-1 text-xs">
              {(() => {
                const last = (id: string) => { for (let i = prog.steps.length - 1; i >= 0; i--) { const it = prog.steps[i]; if (it.id === id) return it } return undefined }
                const ids: string[] = ['calc']
                if (prog.steps.some((s)=>s.id==='snapshot')) ids.push('snapshot')
                if (prog.steps.some((s)=>s.id==='email')) ids.push('email')
                if (prog.steps.some((s)=>s.id==='sms')) ids.push('sms')
                const renderLine = (id: 'calc'|'snapshot'|'email'|'sms') => {
                  const ev = last(id) as any
                  const st: 'start'|'ok'|'error' = (ev?.status || 'start')
                  let text = ''
                  if (id === 'calc') {
                    text = (st==='ok') ? t('alerts.runProgress.finishedCalculated') : t('alerts.runProgress.calculating')
                  } else if (id === 'snapshot') {
                    const m = String((ev?.mode || 'chart'))
                    const prep = (m==='kpi' ? t('alerts.runProgress.preparingKpi') : (m==='table' ? t('alerts.runProgress.preparingTable') : t('alerts.runProgress.preparingSnapshot')))
                    if (st==='ok') text = (m==='kpi' ? t('alerts.runProgress.preparedKpi') : (m==='table' ? t('alerts.runProgress.preparedTable') : t('alerts.runProgress.preparedChart')))
                    else if (st==='error') text = t('alerts.runProgress.snapshotFailed')
                    else text = prep
                  } else if (id === 'email') {
                    if (st==='ok') text = typeof ev?.to==='number' ? t('alerts.runProgress.emailSentTo', { n: ev.to }) : t('alerts.runProgress.emailSent')
                    else if (st==='error') text = typeof ev?.to==='number' ? t('alerts.runProgress.emailFailedTo', { n: ev.to }) : t('alerts.runProgress.emailFailed')
                    else text = t('alerts.runProgress.sendingEmail')
                  } else if (id === 'sms') {
                    if (st==='ok') text = typeof ev?.to==='number' ? t('alerts.runProgress.smsSentTo', { n: ev.to }) : t('alerts.runProgress.smsSent')
                    else if (st==='error') text = typeof ev?.to==='number' ? t('alerts.runProgress.smsFailedTo', { n: ev.to }) : t('alerts.runProgress.smsFailed')
                    else text = t('alerts.runProgress.sendingSms')
                  }
                  return (
                    <div key={id}>
                      <div className="flex items-center gap-2">
                        {st === 'start' ? (
                          <span className="inline-block h-3 w-3 border border-[hsl(var(--border))] border-l-transparent rounded-full animate-spin" aria-hidden="true"></span>
                        ) : st === 'ok' ? (
                          <span className="inline-block text-emerald-600">✓</span>
                        ) : (
                          <span className="inline-block text-rose-600">✕</span>
                        )}
                        <span>{text}</span>
                      </div>
                      {id==='snapshot' && st==='error' && (
                        <div className="pl-5 opacity-70">
                          <span className="block">{ev?.error ? t('alerts.runProgress.reason', { error: ev.error }) : ''}</span>
                          {(ev?.wid || ev?.did || ev?.actor) && (
                            <span className="block">{`wid=${ev?.wid||'-'} did=${ev?.did||'-'} actor=${ev?.actor||'-'}`}</span>
                          )}
                        </div>
                      )}
                      {id==='email' && st==='error' && ev?.error && (
                        <div className="pl-5 opacity-70">{String(ev.error)}</div>
                      )}
                      {id==='sms' && st==='error' && ev?.error && (
                        <div className="pl-5 opacity-70">{String(ev.error)}</div>
                      )}
                    </div>
                  )
                }
                return (
                  <div>
                    {ids.map((k)=> renderLine(k as any))}
                  </div>
                )
              })()}
            </div>
          </div>
        ))}
      </div>
      {/* Unified create dialog (Advanced mode default) */}
      <AlertDialog open={createOpen} mode="create" onCloseAction={() => setCreateOpen(false)} onSavedAction={(a)=>{ onCreated(a); setCreateOpen(false) }} />
      <EmailConfigDialog open={dlgEmail} onCloseAction={() => setDlgEmail(false)} />
      <SmsConfigDialog open={dlgSms} onCloseAction={() => setDlgSms(false)} />
      <AlertDialog open={editOpen} mode="edit" alert={editTarget} onCloseAction={() => setEditOpen(false)} onSavedAction={onSaved} />
    </div>
  )
}
