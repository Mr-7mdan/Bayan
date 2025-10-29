"use client"

import { useEffect, useMemo, useState } from 'react'
import { Card, Title, Text, Select, SelectItem } from '@tremor/react'
import Switch from '@/components/Switch'
import { Api, type AlertOut, type AlertCreate } from '@/lib/api'
import { RiPlayLine, RiDeleteBinLine, RiRefreshLine, RiSettings3Line, RiAddLine, RiEdit2Line, RiAlarmWarningLine, RiNotificationBadgeLine, RiMailLine, RiMessage2Line } from '@remixicon/react'
import EmailConfigDialog from '@/components/alerts/EmailConfigDialog'
import SmsConfigDialog from '@/components/alerts/SmsConfigDialog'
import AlertDialog from '@/components/alerts/AlertDialog'
import { useAuth } from '@/components/providers/AuthProvider'

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

function fmtNextRun(iso?: string | null): string {
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
    if (dys >= 1) return `in ${dys}days`
    if (h >= 1) return `in ${h}hrs`
    if (m >= 1) return `in ${m}m`
    return `in ${s}s`
  } catch { return '—' }
}

function fmtLastRun(iso?: string | null): string {
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
    if (dys === 1) return '1d ago'
    if (h >= 1) return `${h}hrs ago`
    if (m >= 1) return `${m}m ago`
    return `${s}s ago`
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
    const dowSet = parseList(dowS)
    const base = now ? new Date(now) : new Date()
    for (let i = 0; i < 370; i++) {
      const d = new Date(base.getFullYear(), base.getMonth(), base.getDate(), hh, mm, 0, 0)
      d.setDate(d.getDate() + i)
      if (monSet && !monSet.has(d.getMonth() + 1)) continue
      const dayOkDom = !domSet || domSet.has(d.getDate())
      const dow = d.getDay() // 0..6, Sun=0
      const dayOkDow = !dowSet || dowSet.has(dow) || dowSet.has(dow === 0 ? 7 : -1)
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
  const [name, setName] = useState('Scheduled Email')
  const [emails, setEmails] = useState('')
  const [time, setTime] = useState('09:00')
  const [scheduleKind, setScheduleKind] = useState<'weekly'|'monthly'>('weekly')
  const [dows, setDows] = useState<number[]>([1,2,3,4,5])
  const [doms, setDoms] = useState<number[]>([])
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  useEffect(() => { if (open) { setName('Scheduled Email'); setEmails(''); setTime('09:00'); setScheduleKind('weekly'); setDows([1,2,3,4,5]); setDoms([]); setError(null); setSaving(false) } }, [open])
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
      if (!to.length) { setError('Enter at least one recipient'); setSaving(false); return }
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
    } catch (e: any) { setError(e?.message || 'Failed to create') } finally { setSaving(false) }
  }
  return (
    (typeof document !== 'undefined') ? (
      <div className="fixed inset-0 z-[1200]">
        <div className="absolute inset-0 bg-black/40" onClick={() => !saving && onOpenChange(false)} />
        <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[620px] max-w-[95vw] rounded-lg border bg-card p-4">
          <div className="flex items-center justify-between mb-2"><div className="text-sm font-medium">New Scheduled Notification</div><button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => onOpenChange(false)} disabled={saving}>✕</button></div>
          {error && <div className="mb-2 text-xs text-rose-600">{error}</div>}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <label className="text-sm">Name<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={name} onChange={(e)=>setName(e.target.value)} /></label>
            <label className="text-sm">Email to (comma-separated)<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="user@org.com,another@org.com" value={emails} onChange={(e)=>setEmails(e.target.value)} /></label>
            <label className="text-sm">Time (HH:mm)<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="09:00" value={time} onChange={(e)=>setTime(e.target.value)} /></label>
            <div className="text-sm">
              <div className="mb-1">Schedule</div>
              <div className="inline-flex rounded-md border overflow-hidden">
                <button type="button" className={`px-2 py-1 text-xs ${scheduleKind==='weekly'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setScheduleKind('weekly')}>Weekly</button>
                <button type="button" className={`px-2 py-1 text-xs border-l ${scheduleKind==='monthly'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setScheduleKind('monthly')}>Monthly</button>
              </div>
              {scheduleKind === 'weekly' ? (
                <div className="mt-2 flex flex-wrap gap-1 text-xs">
                  {[{v:0,l:'Sun'},{v:1,l:'Mon'},{v:2,l:'Tue'},{v:3,l:'Wed'},{v:4,l:'Thu'},{v:5,l:'Fri'},{v:6,l:'Sat'}].map(d => (
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
          <div className="mt-4 flex items-center gap-2"><button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" disabled={saving} onClick={onSave}>{saving?'Saving…':'Save'}</button><button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" disabled={saving} onClick={() => onOpenChange(false)}>Cancel</button></div>
        </div>
      </div>
    ) : null
  )
}

export default function AlertsPage() {
  const { user } = useAuth()
  const [items, setItems] = useState<AlertOut[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [toast, setToast] = useState('')
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

  useEffect(() => {
    let cancelled = false
    async function run() {
      setLoading(true); setError(null)
      try { const res = await Api.listAlerts(); if (!cancelled) setItems(Array.isArray(res)?res:[]) } catch (e: any) { if (!cancelled) setError(e?.message || 'Failed to load') } finally { if (!cancelled) setLoading(false) }
    }
    void run(); return () => { cancelled = true }
  }, [])

  const onRun = async (id: string) => {
    try {
      const r = await Api.runAlertNow(id)
      const msg = r?.message || 'Triggered'
      setToast(msg); setTimeout(()=>setToast(''), 1500)
      try { const updated = await Api.getAlert(id); setItems((prev)=>prev.map((x)=>x.id===id?updated:x)) } catch {}
    } catch (e: any) { setToast(e?.message || 'Failed'); setTimeout(()=>setToast(''), 1800) }
  }
  const onDelete = async (id: string) => {
    try {
      if (typeof window !== 'undefined') { const ok = window.confirm('Delete this alert?'); if (!ok) return }
      await Api.deleteAlert(id)
      setItems((prev)=>prev.filter((x)=>x.id!==id))
      try { if (typeof window !== 'undefined') window.dispatchEvent(new Event('sidebar-counts-refresh')) } catch {}
      setToast('Deleted'); setTimeout(()=>setToast(''), 1500)
    } catch (e: any) { setToast(e?.message || 'Failed'); setTimeout(()=>setToast(''), 1800) }
  }
  const onToggleEnabled = async (a: AlertOut, next: boolean) => {
    try { const payload = { ...toCreatePayload(a), enabled: next } as AlertCreate; const res = await Api.updateAlert(a.id, payload); setItems((prev)=>prev.map((x)=>x.id===a.id?res:x)); setToast(next?'Enabled':'Disabled'); setTimeout(()=>setToast(''), 1200) } catch (e: any) { setToast(e?.message || 'Failed'); setTimeout(()=>setToast(''), 1800) }
  }
  const onCreated = (a: AlertOut) => { setItems((prev)=>[a, ...prev]); try { if (typeof window !== 'undefined') window.dispatchEvent(new Event('sidebar-counts-refresh')) } catch {}; setToast('Created'); setTimeout(()=>setToast(''), 1500) }
  const onEdit = (a: AlertOut) => { setEditTarget(a); setEditOpen(true) }
  const onSaved = (a: AlertOut) => { setItems((prev)=>prev.map((x)=>x.id===a.id?a:x)); try { if (typeof window !== 'undefined') window.dispatchEvent(new Event('sidebar-counts-refresh')) } catch {}; setToast('Saved'); setTimeout(()=>setToast(''), 1500) }
  const onRefreshJobs = async () => {
    try {
      const actorId = user?.id || ''
      const res = await Api.adminSchedulerRefresh(actorId)
      const msg = res ? `Refreshed jobs: added ${res.added}, updated ${res.updated}, removed ${res.removed}, total ${res.total}` : 'Refreshed scheduler'
      setToast(msg); setTimeout(()=>setToast(''), 2500)
    } catch (e: any) { setToast(e?.message || 'Failed to refresh'); setTimeout(()=>setToast(''), 2000) }
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
            <Title className="text-gray-500 dark:text-white">Alerts & Notifications</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">Create and manage alerts, and configure email/SMS providers</Text>
          </div>
          <div className="flex items-center gap-2">
            <button className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]" onClick={() => setCreateOpen(true)}><RiAddLine className="w-4 h-4" />New</button>
            <button className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]" onClick={() => setDlgEmail(true)}><RiSettings3Line className="w-4 h-4" />Email Config</button>
            <button className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]" onClick={() => setDlgSms(true)}><RiSettings3Line className="w-4 h-4" />SMS Config</button>
            {String(user?.role || '').toLowerCase() === 'admin' && (
              <button className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]" onClick={onRefreshJobs}><RiRefreshLine className="w-4 h-4" />Refresh Scheduler</button>
            )}
          </div>
        </div>
        {error && <div className="px-4 py-2 text-sm text-red-600">{error}</div>}
        <div className="px-3 py-2">
          <div className="flex items-center py-2 gap-2">
            <div className="flex items-center gap-2">
              <label htmlFor="searchAlerts" className="text-sm mr-2 text-gray-600 dark:text-gray-300">Search</label>
              <input id="searchAlerts" value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search alerts..." className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]" />
            </div>
            <div className="ml-auto flex items-center gap-2 text-sm shrink-0">
              <span className="whitespace-nowrap min-w-[84px]">Per page</span>
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
                  <th className="text-left px-3 py-2 font-medium">Name</th>
                  <th className="text-left px-3 py-2 font-medium">Type</th>
                  <th className="text-left px-3 py-2 font-medium">Last run</th>
                  <th className="text-left px-3 py-2 font-medium">Next run</th>
                  <th className="text-left px-3 py-2 font-medium">Status</th>
                  <th className="text-left px-3 py-2 font-medium">Actions</th>
                </tr>
              </thead>
              <tbody className="bg-[hsl(var(--background))]">
                {loading ? (
                  <tr><td className="px-3 py-3" colSpan={6}>Loading…</td></tr>
                ) : (filtered.length === 0 ? (
                  <tr><td className="px-3 py-3" colSpan={6}>No alerts yet.</td></tr>
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
                          <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs border bg-yellow-50 text-yellow-800 border-yellow-200 dark:bg-yellow-500/20 dark:text-yellow-300 dark:border-yellow-400/30" title="Alert">
                            <RiAlarmWarningLine className="w-4 h-4" />
                          </span>
                        ) : (
                          <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs border bg-gray-900 text-white border-gray-700 dark:bg-white/10 dark:text-white dark:border-white/20" title="Notification">
                            <RiNotificationBadgeLine className="w-4 h-4" />
                          </span>
                        )}
                        {Array.isArray((a as any).config?.actions) && (a as any).config.actions.some((ac: any) => String(ac?.type || '').toLowerCase() === 'email') && (
                          <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-700 dark:text-gray-300" title="Email">
                            <RiMailLine className="w-4 h-4" />
                          </span>
                        )}
                        {Array.isArray((a as any).config?.actions) && (a as any).config.actions.some((ac: any) => String(ac?.type || '').toLowerCase() === 'sms') && (
                          <span className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-700 dark:text-gray-300" title="SMS">
                            <RiMessage2Line className="w-4 h-4" />
                          </span>
                        )}
                      </div>
                    </td>
                    <td className="px-3 py-2">{fmtLastRun(a.lastRunAt)}</td>
                    <td className="px-3 py-2">{(() => { try { const cron = ((a as any).config?.triggers || []).find((t: any) => String(t?.type || '').toLowerCase() === 'time')?.cron; if (a.enabled && typeof cron === 'string' && cron.trim()) { const n = nextRunFromCron(cron.trim()); return fmtNextRun(n || undefined) } return '—' } catch { return '—' } })()}</td>
                    <td className="px-3 py-2 truncate max-w-[320px]" title={a.lastStatus || ''}>{a.lastStatus || '—'}</td>
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-2">
                        <button
                          className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 hover:bg-[hsl(var(--muted))]"
                          title="Edit"
                          onClick={() => onEdit(a)}
                        >
                          <RiEdit2Line className="w-4 h-4" />
                        </button>
                        <button
                          className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 hover:bg-[hsl(var(--muted))]"
                          title="Run now"
                          onClick={() => onRun(a.id)}
                        >
                          <RiPlayLine className="w-4 h-4" />
                        </button>
                        <button
                          className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 hover:bg-[hsl(var(--muted))]"
                          title="Delete"
                          onClick={() => onDelete(a.id)}
                        >
                          <RiDeleteBinLine className="w-4 h-4" />
                        </button>
                      </div>
                    </td>
                  </tr>
                )))}
              </tbody>
            </table>
          </div>
          {!loading && filtered.length > 0 && (
            <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
              <span>Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, filtered.length)} of {filtered.length}</span>
              <div className="flex items-center gap-2">
                <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={page <= 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>Prev</button>
                <span>Page {page + 1} / {totalPages}</span>
                <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={page >= totalPages - 1} onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}>Next</button>
              </div>
            </div>
          )}
        </div>
      </Card>
      {!!toast && (
        <div className="fixed top-6 right-6 z-[100] flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-3 text-[14px] font-medium text-white">
          <RiRefreshLine className="w-5 h-5" /><span>{toast}</span>
        </div>
      )}
      {/* Unified create dialog (Advanced mode default) */}
      <AlertDialog open={createOpen} mode="create" onCloseAction={() => setCreateOpen(false)} onSavedAction={(a)=>{ onCreated(a); setCreateOpen(false) }} />
      <EmailConfigDialog open={dlgEmail} onCloseAction={() => setDlgEmail(false)} />
      <SmsConfigDialog open={dlgSms} onCloseAction={() => setDlgSms(false)} />
      <AlertDialog open={editOpen} mode="edit" alert={editTarget} onCloseAction={() => setEditOpen(false)} onSavedAction={onSaved} />
    </div>
  )
}
