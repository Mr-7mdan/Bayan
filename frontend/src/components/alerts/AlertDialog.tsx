"use client"

import React, { useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import { PivotBuilder, type PivotAssignments } from '@/components/builder/PivotBuilder'
import type { WidgetConfig } from '@/types/widgets'
import { Api, QueryApi, type AlertOut, type AlertCreate, type AlertConfig, type DatasourceOut, type AlertRunOut, type DashboardOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import { useTheme } from '@/components/providers/ThemeProvider'

// Helper: format date as YYYY-MM-DD using local timezone (not UTC)
function ymd(d: Date): string {
  const y = d.getFullYear()
  const m = String(d.getMonth() + 1).padStart(2, '0')
  const da = String(d.getDate()).padStart(2, '0')
  return `${y}-${m}-${da}`
}

function parseCron(cron?: string) {
  try {
    const s = String(cron || '')
    const parts = s.trim().split(/\s+/)
    if (parts.length < 5) return { hh: '09', mm: '00', dows: [1,2,3,4,5] as number[], doms: [] as number[], mode: 'weekly' as const, everyHours: 1 }
    const mm = parts[0]
    const hh = parts[1]
    const domPart = (parts[2] || '*').trim()
    const dowPart = (parts[4] || '*').trim()
    const doms = domPart === '*' ? [] : domPart.split(',').map((x)=>parseInt(x,10)).filter((n)=>!isNaN(n) && n>=1 && n<=31)
    const dows = dowPart === '*' ? [] : dowPart.split(',').map((x) => parseInt(x, 10)).filter((n) => !isNaN(n) && n>=0 && n<=6)
    // Detect hourly pattern like: mm */N * * *
    if (/^\*\/\d+$/.test(hh)) {
      const n = parseInt(hh.split('*/')[1] || '1', 10)
      return { hh: '00', mm: String(mm).padStart(2,'0'), dows: [], doms: [], mode: 'hourly' as const, everyHours: (isNaN(n) || n<=0 ? 1 : n) }
    }
    const mode = doms.length ? 'monthly' as const : 'weekly' as const
    const wk = dows.length ? dows : [1,2,3,4,5]
    return { hh: String(hh).padStart(2,'0'), mm: String(mm).padStart(2,'0'), dows: wk, doms, mode, everyHours: 1 }
  } catch { return { hh: '09', mm: '00', dows: [1,2,3,4,5] as number[], doms: [] as number[], mode: 'weekly' as const, everyHours: 1 } }
}

function buildCron(time: string, opts: { mode: 'hourly'|'weekly'|'monthly'; dows: number[]; doms: number[]; everyHours?: number }) {
  try {
    const [hh, mm] = time.split(':').map((t)=>parseInt(t,10));
    if (opts.mode === 'hourly') {
      const every = Math.max(1, Math.min(24, Number(opts.everyHours || 1)))
      return `0 */${isNaN(every)?1:every} * * *`
    }
    if (opts.mode === 'monthly') {
      const domList = (opts.doms||[]).join(',') || '*'
      return `${isNaN(mm)?0:mm} ${isNaN(hh)?0:hh} ${domList} * *`
    }
    const dowList = (opts.dows||[]).join(',') || '*'
    return `${isNaN(mm)?0:mm} ${isNaN(hh)?0:hh} * * ${dowList}`
  } catch { return '0 9 * * 1,2,3,4,5' }
}

function defaultAggFromSpec(spec: any): string {
  try {
    if (spec?.agg && spec.agg !== 'none') return String(spec.agg)
    if (spec?.measure || spec?.y) return 'sum'
    if (Array.isArray(spec?.series) && spec.series.length) return String(spec.series?.[0]?.agg || 'sum')
  } catch {}
  return 'count'
}

// Lightweight Details UIs adapted for Alerts dialog Advanced mode
function NumberFilterDetails({ field, where, onPatch }: { field: string; where?: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  type NumberOp = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'between'
  const gte = (where as any)?.[`${field}__gte`] as number | undefined
  const lte = (where as any)?.[`${field}__lte`] as number | undefined
  const gt = (where as any)?.[`${field}__gt`] as number | undefined
  const lt = (where as any)?.[`${field}__lt`] as number | undefined
  const eqArr = (where as any)?.[field] as number[] | undefined
  const singleEq = (Array.isArray(eqArr) && eqArr.length === 1) ? Number(eqArr[0]) : undefined
  const initial: { op: NumberOp; a?: number | ''; b?: number | '' } = (() => {
    if (typeof singleEq === 'number') return { op: 'eq', a: singleEq }
    if (typeof gt === 'number') return { op: 'gt', a: gt }
    if (typeof gte === 'number' && typeof lte === 'number') return { op: 'between', a: gte, b: lte }
    if (typeof gte === 'number') return { op: 'gte', a: gte }
    if (typeof lt === 'number') return { op: 'lt', a: lt }
    if (typeof lte === 'number') return { op: 'lte', a: lte }
    return { op: 'eq', a: '' }
  })()
  const [op, setOp] = React.useState<NumberOp>(initial.op)
  const [a, setA] = React.useState<number | ''>(initial.a ?? '')
  const [b, setB] = React.useState<number | ''>(initial.b ?? '')
  React.useEffect(() => {
    const patch: Record<string, any> = { [`${field}__gt`]: undefined, [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined, [field]: undefined }
    const hasNum = (x: any) => typeof x === 'number' && !isNaN(x)
    switch (op) {
      case 'eq': if (hasNum(a)) patch[field] = [a]; break
      case 'ne': if (hasNum(a)) patch[`${field}__ne`] = a; break
      case 'gt': if (hasNum(a)) patch[`${field}__gt`] = a; break
      case 'gte': if (hasNum(a)) patch[`${field}__gte`] = a; break
      case 'lt': if (hasNum(a)) patch[`${field}__lt`] = a; break
      case 'lte': if (hasNum(a)) patch[`${field}__lte`] = a; break
      case 'between': if (hasNum(a)) patch[`${field}__gte`] = a; if (hasNum(b)) patch[`${field}__lte`] = b; break
    }
    onPatch(patch)
  }, [op, a, b])
  return (
    <div className="rounded-md border bg-card p-2">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">Value filter: {field}</div>
        <div className="flex items-center gap-2">
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { setOp('eq'); setA(''); setB(''); onPatch({ [field]: undefined, [`${field}__gt`]: undefined, [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined }) }}>Clear</button>
        </div>
      </div>
      <div className="grid grid-cols-3 gap-2 mt-2 items-center">
        <select className="col-span-3 sm:col-span-1 px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={op} onChange={(e) => setOp(e.target.value as NumberOp)}>
          <option value="eq">Is equal to</option>
          <option value="ne">Is not equal to</option>
          <option value="gt">Is greater than</option>
          <option value="gte">Is greater or equal</option>
          <option value="lt">Is less than</option>
          <option value="lte">Is less than or equal</option>
          <option value="between">Is between</option>
        </select>
        {op !== 'between' ? (
          <input type="number" className="col-span-3 sm:col-span-2 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={a} onChange={(e) => setA(e.target.value === '' ? '' : Number(e.target.value))} />
        ) : (
          <>
            <input type="number" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Min" value={a} onChange={(e) => setA(e.target.value === '' ? '' : Number(e.target.value))} />
            <input type="number" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Max" value={b} onChange={(e) => setB(e.target.value === '' ? '' : Number(e.target.value))} />
          </>
        )}
      </div>
    </div>
  )
}

function DateRangeDetails({ field, where, onPatch }: { field: string; where?: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  const a0 = (where as any)?.[`${field}__gte`] as string | undefined
  const b0 = (where as any)?.[`${field}__lt`] as string | undefined
  const normalizeEnd = (b?: string) => {
    if (!b) return undefined
    const d = new Date(`${b}T00:00:00`); if (isNaN(d.getTime())) return undefined
    d.setDate(d.getDate() + 1)
    return ymd(d)
  }
  const [start, setStart] = React.useState<string>(a0 || '')
  const [end, setEnd] = React.useState<string>(b0 ? (() => { const d = new Date(b0 + 'T00:00:00'); d.setDate(d.getDate() - 1); return ymd(d) })() : '')
  React.useEffect(() => {
    const patch: Record<string, any> = {}
    patch[`${field}__gte`] = start || undefined
    patch[`${field}__lt`] = normalizeEnd(end)
    onPatch(patch)
  }, [start, end])
  return (
    <div className="rounded-md border bg-card p-2">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">Date range: {field}</div>
        <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { setStart(''); setEnd(''); onPatch({ [`${field}__gte`]: undefined, [`${field}__lt`]: undefined }) }}>Clear</button>
      </div>
      <div className="grid grid-cols-2 gap-2 mt-2 items-center">
        <div className="flex flex-col gap-1">
          <label className="text-[11px] text-muted-foreground">Start</label>
          <input type="date" className="h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={start} onChange={(e) => setStart(e.target.value)} />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-[11px] text-muted-foreground">End</label>
          <input type="date" className="h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={end} onChange={(e) => setEnd(e.target.value)} />
        </div>
      </div>
    </div>
  )
}

const DISTINCT_CACHE = new Map<string, { ts: number; values: string[]; total: number }>()
const DISTINCT_TTL_MS = 10 * 60 * 1000

function ValuesFilterPicker({ field, datasourceId, source, where, onApply }: { field: string; datasourceId?: string; source?: string; where?: Record<string, any>; onApply: (vals: any[]) => void }) {
  const [selected, setSelected] = React.useState<any[]>(Array.isArray((where as any)?.[field]) ? ((where as any)[field] as any[]) : [])
  const [filterQuery, setFilterQuery] = React.useState('')
  const [samples, setSamples] = React.useState<string[]>([])
  const [loading, setLoading] = React.useState<boolean>(false)
  const [total, setTotal] = React.useState<number | null>(null)
  React.useEffect(() => setSelected(Array.isArray((where as any)?.[field]) ? ((where as any)[field] as any[]) : []), [JSON.stringify(where), field])
  React.useEffect(() => {
    let abort = false
    async function run() {
      try {
        if (!source) return
        setLoading(true)
        const omitWhere: Record<string, any> = { ...((where || {}) as any) }
        Object.keys(omitWhere).forEach((k) => { if (k === field || k.startsWith(`${field}__`)) delete (omitWhere as any)[k] })
        const cacheKey = JSON.stringify({ ds: datasourceId || '', src: source || '', field, where: omitWhere })
        const cached = DISTINCT_CACHE.get(cacheKey)
        if (cached && (Date.now() - cached.ts) < DISTINCT_TTL_MS) {
          if (!abort) { setSamples(cached.values); setTotal(cached.total || cached.values.length) }
          if (!abort) setLoading(false)
          return
        }
        const pageSize = 5000
        const acc = new Set<string>()
        let grandTotal = 0

        const fetchDistinct = async () => {
          let offset = 0
          while (!abort) {
            const spec: any = { source, select: [field], agg: 'distinct', where: Object.keys(omitWhere).length ? omitWhere : undefined, limit: pageSize, offset }
            const res = await QueryApi.querySpec({ spec, datasourceId, limit: pageSize, offset, includeTotal: true })
            const cols = (res.columns || []) as string[]
            const idx = cols.length > 0 ? (cols.indexOf(field) >= 0 ? cols.indexOf(field) : 0) : 0
            const rows = Array.isArray(res.rows) ? res.rows : []
            rows.forEach((arr: any) => { const v = Array.isArray(arr) ? arr[idx] : (Array.isArray(arr) ? arr[0] : undefined); if (v !== null && v !== undefined) acc.add(String(v)) })
            const got = rows.length
            const tot = Number(res.totalRows || 0)
            grandTotal = Math.max(grandTotal, tot)
            offset += got
            if (got < pageSize || (tot > 0 && offset >= tot)) break
          }
        }

        const fetchScan = async () => {
          let offset = 0
          while (!abort) {
            const spec: any = { source, select: [field], where: Object.keys(omitWhere).length ? omitWhere : undefined, limit: pageSize, offset }
            const res = await QueryApi.querySpec({ spec, datasourceId, limit: pageSize, offset, includeTotal: true })
            const cols = (res.columns || []) as string[]
            const idx = cols.length > 0 ? (cols.indexOf(field) >= 0 ? cols.indexOf(field) : 0) : 0
            const rows = Array.isArray(res.rows) ? res.rows : []
            rows.forEach((arr: any) => { const v = Array.isArray(arr) ? arr[idx] : (Array.isArray(arr) ? arr[0] : undefined); if (v !== null && v !== undefined) acc.add(String(v)) })
            const got = rows.length
            const tot = Number(res.totalRows || 0)
            grandTotal = Math.max(grandTotal, tot)
            offset += got
            if (got < pageSize || (tot > 0 && offset >= tot)) break
          }
        }

        let usedFallback = false
        try { await fetchDistinct() } catch { usedFallback = true }
        if (!usedFallback && acc.size === 0) { usedFallback = true }
        if (usedFallback) { await fetchScan() }
        if (!abort) {
          const valuesArr = Array.from(acc.values()).sort()
          const totalNum = grandTotal || acc.size
          setSamples(valuesArr)
          setTotal(totalNum)
          try { DISTINCT_CACHE.set(cacheKey, { ts: Date.now(), values: valuesArr, total: totalNum }) } catch {}
        }
      } catch { if (!abort) { setSamples([]); setTotal(0) } }
      finally { if (!abort) setLoading(false) }
    }
    run(); return () => { abort = true }
  }, [datasourceId, source, field, JSON.stringify(where)])
  const toggle = (v: any) => { const exists = selected.some((x) => x === v); const next = exists ? selected.filter((x) => x !== v) : [...selected, v]; setSelected(next) }
  return (
    <div className="rounded-md border bg-card p-2">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">Filter values: {field}</div>
        <div className="flex items-center gap-2">
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { setSelected([]); onApply([]) }}>Clear</button>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => onApply(selected)}>Apply</button>
        </div>
      </div>
      <div className="mt-2 flex items-center gap-2">
        <input className="w-full px-2 py-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)] text-xs" placeholder="Search values" value={filterQuery} onChange={(e) => setFilterQuery(e.target.value)} />
        {loading && <span className="h-4 w-4 border border-[hsl(var(--border))] border-l-transparent rounded-full animate-spin" aria-hidden="true"></span>}
      </div>
      <div className="max-h-56 overflow-auto mt-2">
        <ul className="space-y-1">
          {samples.filter((s) => String(s).toLowerCase().includes(filterQuery.toLowerCase())).map((v, i) => (
            <li key={i} className="flex items-center gap-2 text-xs">
              <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={selected.some((x) => x === v)} onChange={() => toggle(v)} />
              <span className="truncate max-w-[240px]" title={String(v)}>{String(v)}</span>
            </li>
          ))}
          {!loading && samples.length === 0 && (<li className="text-xs text-muted-foreground">No values found</li>)}
        </ul>
      </div>
    </div>
  )
}

export default function AlertDialog({ open, mode, onCloseAction, onSavedAction, alert, widget, parentDashboardId, defaultKind, defaultTemplate }: { open: boolean; mode: 'create'|'edit'; onCloseAction: () => void; onSavedAction: (a: AlertOut) => void; alert?: AlertOut | null; widget?: WidgetConfig | null; parentDashboardId?: string | null; defaultKind?: 'alert'|'notification'; defaultTemplate?: string }) {
  const { user } = useAuth()
  const { resolved } = useTheme()
  const tabBase = resolved === 'dark' ? 'sidebar-item-dark' : 'sidebar-item-light'
  const tabActive = resolved === 'dark' ? 'sidebar-item-active-dark' : 'sidebar-item-active-light'
  const [name, setName] = useState('')
  const [kind, setKind] = useState<'alert'|'notification'>(defaultKind ?? 'alert')
  const [enabled, setEnabled] = useState(true)
  const [emailTo, setEmailTo] = useState('')
  const [smsTo, setSmsTo] = useState('')
  const [template, setTemplate] = useState(defaultTemplate ?? 'Current KPI value: {{kpi}}')
  const [templateSms, setTemplateSms] = useState('KPI: {{kpi}}')
  const [customPlaceholders, setCustomPlaceholders] = useState<Array<{ name: string; html: string }>>([])
  const phAreaRefs = React.useRef<Array<HTMLTextAreaElement | null>>([])
  const [fmtSize, setFmtSize] = useState<string>('14')
  const [fmtFontColor, setFmtFontColor] = useState<string>('#E5E7EB')
  const [fmtBgColor, setFmtBgColor] = useState<string>('#0F172A')
  const [fmtBorderColor, setFmtBorderColor] = useState<string>('#94A3B8')
  const [fmtTarget, setFmtTarget] = useState<string>('kpi')
  const [fmtMarginTop, setFmtMarginTop] = useState<number>(0)
  const [fmtMarginBottom, setFmtMarginBottom] = useState<number>(0)
  const rawAreaRef = React.useRef<HTMLTextAreaElement | null>(null)
  const [msgKind, setMsgKind] = useState<'email'|'sms'>('email')
  const [msgView, setMsgView] = useState<'preview'|'raw'>('preview')
  const [uiSection, setUiSection] = useState<'header'|'recipients'|'trigger'|'insert'|'preview'>('header')
  const [chanEmail, setChanEmail] = useState<boolean>(true)
  const [chanSms, setChanSms] = useState<boolean>(false)
  const [renderMode, setRenderMode] = useState<'kpi'|'table'|'chart'>('kpi')
  const [snapWidth, setSnapWidth] = useState<number>(1000)
  const [snapHeight, setSnapHeight] = useState<number>(280)
  type RecipientToken = { kind: 'contact'|'email'|'phone'|'tag'; label: string; value: string; email?: string; phone?: string; id?: string; name?: string; tag?: string }
  const [recipTokens, setRecipTokens] = useState<RecipientToken[]>([])
  const [recipInput, setRecipInput] = useState<string>('')
  const [recipSuggestions, setRecipSuggestions] = useState<Array<(
    { type: 'contact'; id: string; name: string; email: string; phone: string; label: string } |
    { type: 'tag'; tag: string; label: string }
  )>>([])
  const [recipSugOpen, setRecipSugOpen] = useState<boolean>(false)
  const [recipSel, setRecipSel] = useState<Set<string>>(new Set())
  const recipKey = (s: { type: 'contact'; id: string } | { type: 'tag'; tag: string }) => (s.type === 'contact' ? `c:${(s.id||'').toLowerCase()}` : `t:${(s.tag||'').toLowerCase()}`)
  function toggleRecipSel(s: { type: 'contact'; id: string } | { type: 'tag'; tag: string }) { const k=recipKey(s); setRecipSel(prev=>{ const n=new Set(prev); if(n.has(k)) n.delete(k); else n.add(k); return n }) }
  function isValidEmail(s: string): boolean { const v=(s||'').trim(); return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v) }
  function isValidPhone(s: string): boolean { const v=(s||'').trim(); return /^[+]?[-(). \d]{6,}$/.test(v) }
  function addRecipToken(t: RecipientToken) {
    const key = t.kind === 'contact' ? (`contact:${(t.id||'').toLowerCase()}`) : (`${t.kind}:${t.value.toLowerCase()}`)
    setRecipTokens(prev => prev.some(x => (x.kind === 'contact' ? (`contact:${(x.id||'').toLowerCase()}`) : (`${x.kind}:${x.value.toLowerCase()}`)) === key) ? prev : [...prev, t])
  }
  function addSelectedRecipients() {
    const selected = new Set(recipSel)
    recipSuggestions.forEach(s => {
      const k = recipKey(s as any)
      if (!selected.has(k)) return
      if (s.type === 'contact') {
        addRecipToken({ kind:'contact', id: s.id, name: s.name, label: s.name || s.label, value: s.name || s.label, email: (s.email||'').trim(), phone: (s.phone||'').trim() })
      } else if (s.type === 'tag') {
        const tg = (s.tag||'').trim()
        if (tg) addRecipToken({ kind:'tag', tag: tg, label: `#${tg}`, value: tg })
      }
    })
    setRecipSel(new Set()); setRecipSugOpen(false); setRecipInput('')
  }
  function tryCommitRecipInput() {
    const raw = (recipInput || '').trim().replace(/[;,]+$/,'')
    if (!raw) return
    if (raw.startsWith('#')) { const tg = raw.replace(/^#+/, ''); if (tg) addRecipToken({ kind:'tag', tag: tg, label: `#${tg}`, value: tg }) }
    else if (isValidEmail(raw)) addRecipToken({ kind:'email', label: raw, value: raw })
    else if (isValidPhone(raw)) addRecipToken({ kind:'phone', label: raw, value: raw })
    setRecipInput(''); setRecipSugOpen(false)
  }
  useEffect(() => {
    const q = (recipInput || '').trim()
    if (!q) { setRecipSuggestions([]); return }
    const h = setTimeout(async () => {
      try {
        const res: any = await Api.listContacts({ search: q, active: true, page: 1, pageSize: 100 })
        const contacts: Array<{ type:'contact'; id: string; name: string; email: string; phone: string; label: string }> = []
        const tagSet = new Set<string>()
        ;(res.items || []).forEach((c: any) => {
          const name = String(c.name||'').trim()
          const em = String(c.email||'').trim()
          const ph = String(c.phone||'').trim()
          const label = `${name}${em?` <${em}>`:''}${ph?` · ${ph}`:''}`
          contacts.push({ type:'contact', id: String(c.id||''), name, email: em, phone: ph, label })
          try { (Array.isArray(c.tags) ? c.tags : []).forEach((t: string) => { const tg = String(t||'').trim(); if (tg.toLowerCase().includes(q.toLowerCase())) tagSet.add(tg) }) } catch {}
        })
        const tagItems: Array<{ type:'tag'; tag: string; label: string }> = Array.from(tagSet).slice(0,50).map(tg => ({ type:'tag', tag: tg, label: `#${tg}` }))
        setRecipSuggestions([ ...tagItems, ...contacts ].slice(0, 100)); setRecipSugOpen(true)
      } catch { setRecipSuggestions([]) }
    }, 150)
    return () => clearTimeout(h)
  }, [recipInput])

  // Expand selected tag tokens to contacts for buildPayload
  const [tagExpansions, setTagExpansions] = useState<Record<string, { emails: string[]; phones: string[] }>>({})
  useEffect(() => {
    const tags = Array.from(new Set(recipTokens.filter(t => t.kind==='tag').map(t => (t.tag||t.value||'').trim()).filter(Boolean)))
    tags.forEach(async (tg) => {
      if (!tg) return
      // Skip if already expanded
      if (tagExpansions[tg]) return
      try {
        const res: any = await Api.listContacts({ search: tg, active: true, page: 1, pageSize: 500 })
        const emails: string[] = []
        const phones: string[] = []
        ;(res.items || []).forEach((c: any) => {
          try {
            if ((Array.isArray(c.tags) ? c.tags: []).some((x: string) => String(x||'').trim().toLowerCase() === tg.toLowerCase())) {
              const em = String(c.email||'').trim(); if (em) emails.push(em)
              const ph = String(c.phone||'').trim(); if (ph) phones.push(ph)
            }
          } catch {}
        })
        setTagExpansions(prev => ({ ...prev, [tg]: { emails: Array.from(new Set(emails)), phones: Array.from(new Set(phones)) } }))
      } catch {}
    })
  }, [JSON.stringify(recipTokens.filter(t => t.kind==='tag').map(t => t.tag||t.value))])
  
  // When enabled and a widget reference is selected, compute height from widget's layout aspect ratio
  const [useWidgetAspect, setUseWidgetAspect] = useState<boolean>(false)
  const [refLayout, setRefLayout] = useState<{ w: number; h: number } | null>(null)
  const [rendererUseCarried, setRendererUseCarried] = useState<boolean>(true)
  const [pickDashId, setPickDashId] = useState<string>('')
  const [dashList, setDashList] = useState<Array<{ id: string; name: string }>>([])
  const [pickWidgets, setPickWidgets] = useState<Array<{ id: string; title: string; cfg: WidgetConfig }>>([])
  const [pickWidgetId, setPickWidgetId] = useState<string>('')
  const [pickWidgetCfg, setPickWidgetCfg] = useState<WidgetConfig | null>(null)
  const [pickDashLayouts, setPickDashLayouts] = useState<Record<string, { w: number; h: number }>>({})

  const [triggerType, setTriggerType] = useState<'threshold'|'time'>('threshold')
  const [operator, setOperator] = useState('>')
  const [value, setValue] = useState('0')
  const [triggerTimeEnabled, setTriggerTimeEnabled] = useState<boolean>(false)
  const [triggerThresholdEnabled, setTriggerThresholdEnabled] = useState<boolean>(true)
  const [triggerLogic, setTriggerLogic] = useState<'AND'|'OR'>('AND')

  const [timeOfDay, setTimeOfDay] = useState('09:00')
  const [daysOfWeek, setDaysOfWeek] = useState<number[]>([1,2,3,4,5])
  const [daysOfMonth, setDaysOfMonth] = useState<number[]>([])
  const [everyHours, setEveryHours] = useState<number>(1)
  const [scheduleKind, setScheduleKind] = useState<'hourly'|'weekly'|'monthly'>('weekly')

  const [aggSel, setAggSel] = useState<string>('count')
  const [measureSel, setMeasureSel] = useState<string>('')
  const [xValueSel, setXValueSel] = useState<string>('')

  const [advOpen, setAdvOpen] = useState(false)
  const [advDatasourceId, setAdvDatasourceId] = useState('')
  const [advSource, setAdvSource] = useState('')
  const [advAgg, setAdvAgg] = useState<'count'|'sum'|'avg'|'min'|'max'|'distinct'>('count')
  const [advMeasure, setAdvMeasure] = useState('')
  const [advWhere, setAdvWhere] = useState('')
  const [advXField, setAdvXField] = useState('')
  const [advXValue, setAdvXValue] = useState('')
  const [advXPick, setAdvXPick] = useState<'custom'|'range'|'today'|'yesterday'|'this_month'|'last'|'min'|'max'>('custom')
  const [advXFrom, setAdvXFrom] = useState('')
  const [advXTo, setAdvXTo] = useState('')

  const [dsList, setDsList] = useState<DatasourceOut[]>([])
  const [tableList, setTableList] = useState<string[]>([])
  const [advTablesLoading, setAdvTablesLoading] = useState<boolean>(false)
  const [tablesMeta, setTablesMeta] = useState<any>(null)
  const [columns, setColumns] = useState<Array<{ name: string; type?: string | null }>>([])
  const numericFields = useMemo(() => {
    const isNumeric = (t?: string | null) => !!t && /int|float|double|numeric|decimal|real|money|number/i.test(String(t))
    return columns.filter(c => isNumeric(c.type)).map(c => c.name)
  }, [columns])
  const dateLikeFields = useMemo(() => {
    const isDate = (t?: string | null) => !!t && /date|time|timestamp/i.test(String(t))
    return columns.filter(c => isDate(c.type)).map(c => c.name)
  }, [columns])
  const [advPivot, setAdvPivot] = useState<PivotAssignments>({ values: [], filters: [] })
  const [detailKind, setDetailKind] = useState<'filter'|'x'|'value'|'legend'|null>(null)
  const [detailField, setDetailField] = useState<string | undefined>(undefined)
  const [advCalcMode, setAdvCalcMode] = useState<'query'|'pivot'>('query')

  const [testHtml, setTestHtml] = useState('')
  const [runs, setRuns] = useState<AlertRunOut[]>([])
  const [localKpi, setLocalKpi] = useState<number | null>(null)
  const [localKpiLoading, setLocalKpiLoading] = useState<boolean>(false)
  const [perCatStats, setPerCatStats] = useState<{ total: number; matches: number } | null>(null)
  const [perCatLoading, setPerCatLoading] = useState<boolean>(false)
  const [perCatMatches, setPerCatMatches] = useState<Array<{ category: any; value: number; x?: any }>>([])
  const [testEmailHtml, setTestEmailHtml] = useState('')
  const [testSmsText, setTestSmsText] = useState('')
  const [testContext, setTestContext] = useState<any | null>(null)
  const [testSummary, setTestSummary] = useState<string | null>(null)
  const [testActiveTab, setTestActiveTab] = useState<'email'|'sms'|'context'|'raw'>('email')
  const [testEvaluating, setTestEvaluating] = useState<boolean>(false)
  const [showPayload, setShowPayload] = useState(false)
  const [lastPayload, setLastPayload] = useState<AlertCreate | null>(null)

  // Ensure defaults apply in create mode each time the dialog opens
  useEffect(() => {
    if (!open) return
    if (mode === 'create') {
      if (defaultKind) setKind(defaultKind)
      if (defaultTemplate != null) setTemplate(defaultTemplate)
    }
  }, [open, mode, defaultKind, defaultTemplate])

  const spec: any = (widget as any)?.querySpec || {}
  const wrapSelAt = React.useCallback((idx: number, before: string, after: string) => {
    try {
      const el = phAreaRefs.current[idx]
      const html = (customPlaceholders[idx]?.html ?? '')
      if (!el) return
      const s = el.selectionStart ?? 0
      const e = el.selectionEnd ?? s
      const sel = html.slice(s, e)
      const next = html.slice(0, s) + before + (sel || '') + after + html.slice(e)
      setCustomPlaceholders((arr) => { const n=[...arr]; n[idx] = { ...n[idx], html: next }; return n })
      requestAnimationFrame(() => { try { el.focus(); const pos = s + before.length + (sel||'').length; el.selectionStart = el.selectionEnd = pos } catch {} })
    } catch {}
  }, [customPlaceholders])
  const spanStyleAt = React.useCallback((idx: number, styles: string) => wrapSelAt(idx, `<span style="${styles}">`, `</span>`), [wrapSelAt])
  const divStyleAt = React.useCallback((idx: number, styles: string) => wrapSelAt(idx, `<div style="${styles}">`, `</div>`), [wrapSelAt])
  const bulletsAt = React.useCallback((idx: number) => {
    try {
      const el = phAreaRefs.current[idx]; const html = (customPlaceholders[idx]?.html ?? ''); if (!el) return
      const s = el.selectionStart ?? 0; const e = el.selectionEnd ?? s
      const sel = html.slice(s, e) || html
      const items = sel.split('\n').map(x=>x.trim()).filter(Boolean)
      const ul = `<ul>${items.map(it=>`<li>${it}</li>`).join('')}</ul>`
      const next = html.slice(0, s) + ul + html.slice(e)
      setCustomPlaceholders((arr)=>{ const n=[...arr]; n[idx] = { ...n[idx], html: next }; return n })
      requestAnimationFrame(()=>{ try { el.focus() } catch {} })
    } catch {}
  }, [customPlaceholders])
  const presetNormalAt = React.useCallback((idx: number) => spanStyleAt(idx, 'display:inline-block; padding:4px;'), [spanStyleAt])
  const presetTableAt = React.useCallback((idx: number) => {
    try {
      const el = phAreaRefs.current[idx]; const html = (customPlaceholders[idx]?.html ?? ''); if (!el) return
      const s = el.selectionStart ?? 0; const e = el.selectionEnd ?? s
      const sel = html.slice(s, e) || '&nbsp;'
      const table = `<table style="width:100%; border-collapse:collapse;"><tbody><tr><td>${sel}</td></tr></tbody></table>`
      const next = html.slice(0, s) + table + html.slice(e)
      setCustomPlaceholders((arr)=>{ const n=[...arr]; n[idx] = { ...n[idx], html: next }; return n })
      requestAnimationFrame(()=>{ try { el.focus() } catch {} })
    } catch {}
  }, [customPlaceholders])
  const measureOptions = useMemo(() => {
    const opts: Array<{ label: string; value: string }> = []
    try {
      if (Array.isArray(spec?.series) && spec.series.length) {
        for (const it of spec.series) {
          const lab = String(it?.name || it?.y || '').trim()
          const val = String(it?.y || '').trim()
          if (val) opts.push({ label: lab || val, value: val })
        }
      }
      const single = String(spec?.measure || spec?.y || '').trim()
      if (single) opts.push({ label: single, value: single })
    } catch {}
    const seen = new Set<string>()
    return opts.filter(o => { if (seen.has(o.value)) return false; seen.add(o.value); return true })
  }, [JSON.stringify(spec)])

  useEffect(() => {
    if (!open) return
    setTestHtml('')
    if (mode === 'edit' && alert) {
      setName(alert.name)
      setKind(alert.kind)
      setEnabled(!!alert.enabled)
      const cfg = alert.config || ({} as any)
      const acts = Array.isArray(cfg.actions) ? cfg.actions : []
      const email = acts.find((a: any) => String(a?.type) === 'email') || {}
      const sms = acts.find((a: any) => String(a?.type) === 'sms') || {}
      setEmailTo(Array.isArray(email.to) ? (email.to as string[]).join(',') : '')
      setSmsTo(Array.isArray(sms.to) ? (sms.to as string[]).join(',') : '')
      setChanEmail(Array.isArray(email.to) ? email.to.length > 0 : (Object.prototype.hasOwnProperty.call(email, 'type')))
      // Persist toggle if an SMS action exists, even if it has empty 'to'
      const hasSmsAction = (acts || []).some((a: any) => String(a?.type) === 'sms')
      setChanSms(hasSmsAction ? true : (Array.isArray(sms.to) ? sms.to.length > 0 : false))
      try {
        const emailsArr = (Array.isArray(email.to)? (email.to as string[]): []).map((v)=> String(v||'').trim()).filter(Boolean)
        const phonesArr = (Array.isArray(sms.to)? (sms.to as string[]): []).map((v)=> String(v||'').trim()).filter(Boolean)
        const uniqE = Array.from(new Set(emailsArr))
        const uniqP = Array.from(new Set(phonesArr))
        setRecipTokens([
          ...uniqE.map((v) => ({ kind: 'email', label: v, value: v } as RecipientToken)),
          ...uniqP.map((v) => ({ kind: 'phone', label: v, value: v } as RecipientToken)),
        ])
      } catch {}
      setTemplate(String(cfg.template || 'Current KPI value: {{kpi}}'))
      const r = cfg.render || { mode: 'kpi' }
      setRenderMode((r.mode || 'kpi') as any)
      try { setSnapWidth(Number((r as any).width || (String((r as any).mode||'kpi')==='kpi'?1000:1000))) } catch {}
      try { setSnapHeight(Number((r as any).height || (String((r as any).mode||'kpi')==='kpi'?280:360))) } catch {}
      
      try {
        const wref = (r as any)?.widgetRef
        if (wref && wref.dashboardId && wref.widgetId) {
          const curWid = String((widget as any)?.id || '')
          const curDash = String(((widget as any)?.dashboardId || parentDashboardId || '') || '')
          const refWid = String(wref.widgetId || '')
          const refDash = String(wref.dashboardId || '')
          const same = (!!curWid && !!curDash && curWid === refWid && curDash === refDash)
          if (same) {
            setRendererUseCarried(true)
          } else {
            setRendererUseCarried(false)
            setPickDashId(String(wref.dashboardId))
            setPickWidgetId(String(wref.widgetId))
          }
        } else {
          setRendererUseCarried(true)
        }
      } catch {}
      const triggersArr: any[] = Array.isArray(cfg.triggers) ? cfg.triggers : []
      const tm = triggersArr.find((t:any) => String(t?.type).toLowerCase() === 'time') || null
      const th = triggersArr.find((t:any) => String(t?.type).toLowerCase() === 'threshold') || null
      if (tm) {
        setTriggerType('time')
        const parsed = parseCron((tm as any).cron)
        setScheduleKind(parsed.mode as any)
        if (parsed.mode === 'hourly') {
          setEveryHours(parsed.everyHours || 1)
        } else {
          setTimeOfDay(`${parsed.hh}:${parsed.mm}`)
          setDaysOfWeek(parsed.dows)
          setDaysOfMonth(parsed.doms || [])
        }
      }
      if (th) {
        setTriggerType('threshold')
        setOperator(String((th as any)?.operator || '>'))
        const v = (((th as any)?.value != null) && Array.isArray((th as any).value)) ? ((th as any).value as any[]).join(',') : String(((th as any)?.value) ?? '0')
        setValue(v)
      }
      try {
        const cfgAny: any = cfg
        setAdvDatasourceId(String(cfgAny.datasourceId || ''))
        try {
          const smsAction = (Array.isArray(cfgAny.actions) ? cfgAny.actions : []).find((a: any) => String(a?.type) === 'sms')
          setTemplateSms(String((smsAction && smsAction.message) || 'KPI: {{kpi}}'))
        } catch {}
        try {
          const cp = (cfgAny as any).customPlaceholders
          if (cp && typeof cp === 'object') {
            const arr = Object.entries(cp).map(([name, html]) => ({ name, html: String(html || '') }))
            setCustomPlaceholders(arr)
          } else {
            setCustomPlaceholders([])
          }
        } catch { setCustomPlaceholders([]) }
        if (th) {
          setAdvSource(String((th as any).source || ''))
          setAdvAgg(String((th as any).aggregator || 'count') as any)
          setAdvMeasure(String((th as any).measure || ''))
          setAdvWhere((((th as any).where && typeof (th as any).where === 'object') ? JSON.stringify((th as any).where) : ''))
          setAdvXField(String((th as any).xField || ''))
          setAdvXValue(String((th as any).xValue ?? ''))
          try {
            const xm = String((th as any).xMode || '').trim()
            if (xm === 'token') {
              const tok = String((th as any).xToken || '') as any
              setAdvXPick((tok === 'today' || tok === 'yesterday' || tok === 'this_month') ? tok : 'custom')
            } else if (xm === 'special') {
              const sp = String((th as any).xSpecial || '') as any
              setAdvXPick((sp === 'last' || sp === 'min' || sp === 'max') ? sp : 'custom')
            } else if (xm === 'range') {
              setAdvXPick('range')
              const xr = (th as any).xRange || {}
              setAdvXFrom(String(xr?.from || ''))
              setAdvXTo(String(xr?.to || ''))
            } else {
              setAdvXPick('custom')
            }
          } catch {}
          try { const cm = String((th as any)?.calcMode || 'query'); setAdvCalcMode(cm === 'pivot' ? 'pivot' : 'query') } catch {}
          // Pre-populate PivotBuilder assignments
          try {
            const whereObj = ((th as any).where && typeof (th as any).where === 'object') ? ((th as any).where as Record<string, any>) : {}
            const filterFields = Array.from(new Set(Object.keys(whereObj).map(k => k.split('__')[0])))
            const vals = [{ field: (String((th as any).aggregator || 'count') === 'count') ? undefined : (String((th as any).measure || '')), agg: (String((th as any).aggregator || 'count') as any) }]
            const lfArr = Array.isArray((th as any).legendFields)
              ? ((th as any).legendFields as string[])
              : ((String((th as any).legendField || '').trim()) ? [String((th as any).legendField)] : [])
            const rowsArr: string[] = []
            const xf0 = String((th as any).xField || '').trim(); if (xf0) rowsArr.push(xf0)
            if (Array.isArray((th as any).rowFields)) rowsArr.push(...((th as any).rowFields as string[]))
            setAdvPivot({
              x: (rowsArr.length <= 1 ? (rowsArr[0] || undefined) : rowsArr),
              values: vals as any,
              legend: (lfArr.length <= 1 ? (lfArr[0] || undefined) : lfArr) as any,
              filters: filterFields,
            })
          } catch {}
        } else { setAdvSource(''); setAdvAgg('count'); setAdvMeasure(''); setAdvWhere(''); setAdvXField(''); setAdvXValue('') }
      } catch {}
      try { (async () => { const rs = await Api.listAlertRuns(alert.id, 50); setRuns(Array.isArray(rs)?rs:[]) })() } catch {}
      // Hydrate trigger toggles from V2 or legacy triggers
      try {
        const tg = (cfg as any)?.triggersGroup
        if (tg && typeof tg === 'object') {
          setTriggerLogic(String(tg.logic || 'AND').toUpperCase() === 'OR' ? 'OR' : 'AND')
          const tcond = (tg.time || {})
          const thrc = (tg.threshold || {})
          setTriggerTimeEnabled(!!tcond.enabled)
          setTriggerThresholdEnabled(!!thrc.enabled)
          try { const cm = String((thrc as any)?.calcMode || 'query'); setAdvCalcMode(cm === 'pivot' ? 'pivot' : 'query') } catch {}
          if (tcond.schedule?.kind) setScheduleKind(String(tcond.schedule.kind) as any)
          if (String(tcond.schedule?.kind) !== 'hourly') {
            if (tcond.time) setTimeOfDay(String(tcond.time))
            if (Array.isArray(tcond.schedule?.dows)) setDaysOfWeek(tcond.schedule.dows as number[])
            if (Array.isArray(tcond.schedule?.doms)) setDaysOfMonth(tcond.schedule.doms as number[])
          } else {
            setEveryHours(Number((tcond.schedule as any)?.everyHours || 1))
          }
        } else {
          const arr = Array.isArray(cfg?.triggers) ? cfg.triggers : []
          setTriggerTimeEnabled(arr.some((x:any)=> String(x?.type).toLowerCase()==='time'))
          setTriggerThresholdEnabled(arr.some((x:any)=> String(x?.type).toLowerCase()==='threshold'))
          setTriggerLogic('AND')
        }
      } catch {}
      // Ensure Advanced is open if any threshold trigger exists or V2 threshold is enabled
      try {
        const tg = (cfg as any)?.triggersGroup
        const advEnabled = !!th || !!(tg && tg.threshold && tg.threshold.enabled)
        setAdvOpen(advEnabled)
      } catch {}
    } else {
      setName((widget as any)?.title || 'New Alert')
      setKind('alert')
      setEnabled(true)
      setEmailTo('')
      setSmsTo('')
      setRecipTokens([])
      setTemplate('Current KPI value: {{kpi}}')
      setTemplateSms('KPI: {{kpi}}')
      setCustomPlaceholders([])
      setRenderMode('kpi')
      setTriggerType('threshold')
      setOperator('>')
      setValue('0')
      if (widget) {
        const agg = defaultAggFromSpec(spec)
        setAggSel(agg)
        const m = (spec?.measure || spec?.y || measureOptions?.[0]?.value || '') as string
        setMeasureSel(m)
        setAdvDatasourceId(String((widget as any)?.datasourceId || ''))
        setAdvSource(String(spec?.source || ''))
        setAdvAgg((agg as any) || 'count')
        setAdvMeasure(String(m || ''))
        setAdvWhere(spec?.where ? JSON.stringify(spec.where) : '')
        setAdvXField(String(spec?.x || ''))
        setAdvXValue('')
        setAdvOpen(false)
        try { setAdvPivot({ x: (spec as any)?.x, values: [{ field: m || undefined, agg: (agg as any) }], legend: (spec as any)?.legend, filters: Object.keys(((spec as any)?.where || {})) }) } catch {}
      } else {
        setAdvOpen(true)
        setAdvDatasourceId('')
        setAdvSource('')
        setAdvAgg('count')
        setAdvMeasure('')
        setAdvWhere('')
        setAdvXField('')
        setAdvXValue('')
        setAdvPivot({ values: [], filters: [] })
      }
    }
  }, [open, mode, alert?.id, (widget as any)?.id])

  useEffect(() => {
    if (!advOpen) return
    ;(async () => {
      try { const ds = await Api.listDatasources(undefined, user?.id); setDsList(Array.isArray(ds)?ds:[]) } catch { setDsList([]) }
    })()
  }, [advOpen, user?.id])

  useEffect(() => {
    if (!open || kind !== 'notification') return
    ;(async () => {
      try {
        const items: any = await Api.listDashboards(user?.id)
        const arr = Array.isArray(items) ? items.map((d: any) => ({ id: String(d.id), name: String(d.name || 'Untitled') })) : []
        setDashList(arr)
      } catch { setDashList([]) }
    })()
  }, [open, kind, user?.id])

  useEffect(() => {
    if (!pickDashId) { setPickWidgets([]); setPickWidgetId(''); setPickWidgetCfg(null); return }
    ;(async () => {
      try {
        const dash = await Api.getDashboard(pickDashId, user?.id)
        const widgets = Object.values((dash as any)?.definition?.widgets || {}) as WidgetConfig[]
        const arr = widgets.map((w: any) => ({ id: String(w.id), title: String(w.title || w.id), cfg: w }))
        setPickWidgets(arr)
        if (!arr.some(x => x.id === pickWidgetId)) setPickWidgetId(arr[0]?.id || '')
        // Capture grid layout (w,h) by widget id for aspect ratio
        try {
          const lyArr: Array<{ i: string; w: number; h: number }> = Array.isArray((dash as any)?.definition?.layout) ? ((dash as any).definition.layout as any[]) : []
          const map: Record<string, { w: number; h: number }> = {}
          lyArr.forEach((it: any) => { const k = String(it?.i || ''); if (k) map[k] = { w: Number(it?.w || 0), h: Number(it?.h || 0) } })
          setPickDashLayouts(map)
          const wid = arr.some(x => x.id === pickWidgetId) ? pickWidgetId : (arr[0]?.id || '')
          setRefLayout(wid && map[wid] ? map[wid] : null)
        } catch { setPickDashLayouts({}); setRefLayout(null) }
      } catch { setPickWidgets([]); setPickWidgetId(''); setPickWidgetCfg(null) }
    })()
  }, [pickDashId, user?.id])

  useEffect(() => {
    const found = pickWidgets.find(w => w.id === pickWidgetId) || null
    setPickWidgetCfg(found ? found.cfg : null)
    // Update refLayout based on selected widget in picked dashboard
    try {
      if (found && pickDashLayouts && pickDashLayouts[found.id]) setRefLayout(pickDashLayouts[found.id])
    } catch {}
  }, [pickWidgetId, JSON.stringify(pickWidgets.map(w=>w.id))])

  // Auto-select render mode from widget type (carried or picked)
  useEffect(() => {
    if (!open || kind !== 'notification') return
    if (rendererUseCarried && widget) {
      try {
        const t = String((widget as any)?.type || '').toLowerCase()
        setRenderMode(t === 'table' ? 'table' : (t === 'chart' ? 'chart' : 'kpi'))
      } catch {}
      return
    }
    if (!rendererUseCarried && pickWidgetCfg) {
      try {
        const t = String((pickWidgetCfg as any)?.type || '').toLowerCase()
        setRenderMode(t === 'table' ? 'table' : (t === 'chart' ? 'chart' : 'kpi'))
      } catch {}
    }
  }, [open, kind, rendererUseCarried, (widget as any)?.type, (pickWidgetCfg as any)?.type])

  useEffect(() => {
    if (!advOpen || !advDatasourceId) { setTableList([]); setTablesMeta(null); setAdvTablesLoading(false); return }
    let cancelled = false
    setAdvTablesLoading(true)
    ;(async () => {
      try {
        // Fast tables-only endpoint - don't pass abort signal
        const tbls = await Api.tablesOnly(advDatasourceId)
        if (cancelled) return
        const arr: string[] = []
        const schemas = (tbls as any)?.schemas || []
        for (const s of schemas) {
          const sch = String(s?.name || '').trim()
          for (const tn of (s?.tables || [])) {
            const tname = String(tn || '').trim()
            if (tname) arr.push(sch ? `${sch}.${tname}` : tname)
          }
        }
        setTableList(arr)
        // Defer full introspect until a table is selected
        setTablesMeta(null)
      } catch {
        if (cancelled) return
        try {
          // Fallback to full introspect if tablesOnly fails
          const meta = await Api.introspect(advDatasourceId)
          if (cancelled) return
          const arr: string[] = []
          const schemas = (meta as any)?.schemas || []
          for (const s of schemas) {
            const sch = String(s?.name || '').trim()
            for (const t of (s?.tables || [])) {
              const tn = String(t?.name || '').trim()
              if (tn) arr.push(sch ? `${sch}.${tn}` : tn)
            }
          }
          setTableList(arr)
          setTablesMeta(meta)
        } catch { if (!cancelled) setTableList([]) }
      } finally { if (!cancelled) setAdvTablesLoading(false) }
    })()
    return () => { cancelled = true }
  }, [advOpen, advDatasourceId])

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        if (!advOpen || !advSource) { setColumns([]); return }
        // If we don't yet have schema meta, fetch it lazily now
        if (!tablesMeta && advDatasourceId) {
          // Don't pass signal - let schema requests complete
          const meta = await Api.introspect(advDatasourceId)
          if (cancelled) return
          setTablesMeta(meta)
          // will recompute columns on next pass
          return
        }
        const [maybeSchema, maybeTable] = String(advSource).includes('.') ? String(advSource).split('.') : [undefined, String(advSource)]
        const tables: any[] = []
        for (const s of (tablesMeta?.schemas || [])) {
          const sch = String(s?.name || '').trim()
          for (const t of (s?.tables || [])) tables.push({ schema: sch, table: String(t?.name || ''), columns: t?.columns || [] })
        }
        const match = tables.find((t) => (maybeSchema ? (t.schema === maybeSchema && t.table === maybeTable) : (t.table === maybeTable)))
        if (!cancelled) setColumns(Array.isArray(match?.columns) ? match.columns : [])
      } catch { if (!cancelled) setColumns([]) }
    })()
    return () => { cancelled = true }
  }, [advOpen, advSource, tablesMeta, advDatasourceId])

  // When using carried widget, load its dashboard to read layout for aspect ratio
  useEffect(() => {
    if (!open || kind !== 'notification') return
    if (!rendererUseCarried) return
    const wid = (widget as any)?.id ? String((widget as any).id) : ''
    const dashId = ((widget as any)?.dashboardId || parentDashboardId || '') as string
    if (!wid || !dashId) { setRefLayout(null); return }
    let cancelled = false
    ;(async () => {
      try {
        const dash = await Api.getDashboard(String(dashId), user?.id)
        const lyArr: Array<{ i: string; w: number; h: number }> = Array.isArray((dash as any)?.definition?.layout) ? ((dash as any).definition.layout as any[]) : []
        const it = lyArr.find((x: any) => String(x?.i || '') === wid)
        if (!cancelled) setRefLayout(it ? { w: Number(it.w || 0), h: Number(it.h || 0) } : null)
      } catch { if (!cancelled) setRefLayout(null) }
    })()
    return () => { cancelled = true }
  }, [open, kind, rendererUseCarried, (widget as any)?.id, (widget as any)?.dashboardId, parentDashboardId, user?.id])

  // Keep height in sync with widget aspect ratio when enabled
  useEffect(() => {
    if (!useWidgetAspect) return
    const ly = refLayout
    if (!ly || !ly.w || !ly.h) return
    try { setSnapHeight(Math.max(80, Math.round(snapWidth * (ly.h / ly.w)))) } catch {}
  }, [useWidgetAspect, snapWidth, refLayout ? refLayout.w : 0, refLayout ? refLayout.h : 0])

  // Load datasource-level transforms so we can expose customColumns, joins, and unpivot outputs in field picker
  const [dsTransforms, setDsTransforms] = useState<any | null>(null)
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        if (!advOpen || !advDatasourceId) { if (!cancelled) setDsTransforms(null); return }
        const cfg = await Api.getDatasourceTransforms(String(advDatasourceId))
        if (!cancelled) setDsTransforms(cfg || {})
      } catch { if (!cancelled) setDsTransforms(null) }
    })()
    return () => { cancelled = true }
  }, [advOpen, advDatasourceId])

  // Merge base columns with datasource transforms: custom columns, unpivot key/value, join columns/agg alias (scope-aware)
  const allFieldNames = useMemo(() => {
    try {
      const base = (columns || []).map(c => String(c?.name || '')).filter(Boolean)
      const set = new Set<string>(base)
      const model = (dsTransforms || {}) as any
      // Custom columns: include all names so users can pick them
      try { (model?.customColumns || []).forEach((cc: any) => { const nm = String(cc?.name || '').trim(); if (nm) set.add(nm) }) } catch {}
      const srcNow = String(advSource || '')
      const norm = (s: string) => String(s || '').trim().replace(/^\[|\]|^"|"$/g, '')
      const tblEq = (a: string, b: string) => {
        const na = norm(a).split('.').pop() || ''; const nb = norm(b).split('.').pop() || ''
        return na.toLowerCase() === nb.toLowerCase()
      }
      // Unpivot additions/hide
      const unpivotHide: Set<string> = new Set()
      try {
        (Array.isArray(model?.transforms) ? model.transforms : []).forEach((t: any) => {
          if (String(t?.type || '').toLowerCase() !== 'unpivot') return
          const sc = (t?.scope || {}) as any
          const lvl = String(sc?.level || 'datasource').toLowerCase()
          const match = (lvl === 'datasource') || (lvl === 'table' && sc?.table && srcNow && tblEq(String(sc.table), srcNow))
          if (!match) return
          const kc = String(t?.keyColumn || '').trim(); const vc = String(t?.valueColumn || '').trim()
          if (kc) set.add(kc); if (vc) set.add(vc)
          const srcCols = Array.isArray(t?.sourceColumns) ? (t.sourceColumns as any[]).map((x:any)=>String(x||'')) : []
          srcCols.forEach((s) => { if (s) unpivotHide.add(s) })
        })
      } catch {}
      // Joins additions
      try {
        (Array.isArray(model?.joins) ? model.joins : []).forEach((j: any) => {
          const sc = (j?.scope || {}) as any
          const lvl = String(sc?.level || 'datasource').toLowerCase()
          const match = (lvl === 'datasource') || (lvl === 'table' && sc?.table && srcNow && tblEq(String(sc.table), srcNow))
          if (!match) return
          const cols = Array.isArray(j?.columns) ? (j.columns as any[]) : []
          cols.forEach((c: any) => { const nm = String((c?.alias || c?.name || '')).trim(); if (nm) set.add(nm) })
          const agg = (j?.aggregate || {}) as any; const alias = String(agg?.alias || '').trim(); if (alias) set.add(alias)
        })
      } catch {}
      // Hide unpivoted source columns
      unpivotHide.forEach((n) => { if (set.has(n)) set.delete(n) })
      return Array.from(set.values())
    } catch { return (columns || []).map(c => String((c as any)?.name || '')).filter(Boolean) }
  }, [JSON.stringify(columns || []), JSON.stringify(dsTransforms || {}), advSource])

  // Compute a local KPI preview using Advanced selections
  useEffect(() => {
    let cancelled = false
    async function run() {
      try {
        setLocalKpi(null)
        if (!advOpen) return
        if (!advSource) return
        const agg = (advPivot.values?.[0]?.agg as any) || advAgg
        const field = advPivot.values?.[0]?.field || advMeasure || undefined
        const xFld = (advPivot.x as any) || (advXField || undefined)
        const catFld = (advPivot.legend as any) || undefined
        if (!agg) return
        if (agg !== 'count' && !field) return
        setLocalKpiLoading(true)
        if (advCalcMode === 'pivot') {
          const whereSel: Record<string, any> = (() => { try { return advWhere.trim() ? JSON.parse(advWhere) : {} } catch { return {} } })()
          if (xFld) {
            const today = new Date()
            if (advXPick === 'today') {
              // As-of cumulative: only upper bound < tomorrow
              const e = new Date(today); e.setDate(e.getDate()+1)
              delete (whereSel as any)[`${xFld}__gte`]
              delete (whereSel as any)[`${xFld}__gt`]
              whereSel[`${xFld}__lt`] = ymd(e)
            } else if (advXPick === 'yesterday') {
              const e = today
              const s = new Date(today); s.setDate(s.getDate()-1)
              whereSel[`${xFld}__gte`] = ymd(s)
              whereSel[`${xFld}__lt`] = ymd(e)
            } else if (advXPick === 'this_month') {
              const s = new Date(today.getFullYear(), today.getMonth(), 1)
              const e = new Date(today.getFullYear(), today.getMonth()+1, 1)
              whereSel[`${xFld}__gte`] = ymd(s)
              whereSel[`${xFld}__lt`] = ymd(e)
            } else if (advXPick === 'range') {
              if (advXFrom) whereSel[`${xFld}__gte`] = advXFrom
              if (advXTo) { const d = new Date(advXTo + 'T00:00:00'); d.setDate(d.getDate()+1); whereSel[`${xFld}__lt`] = ymd(d) }
            } else if (advXPick === 'custom' && advXValue) {
              whereSel[xFld] = [advXValue]
            }
          }
          const res = await Api.pivot({ source: advSource, rows: [], cols: [], aggregator: agg as any, valueField: (agg !== 'count' ? (field as any) : undefined), where: (Object.keys(whereSel).length ? whereSel : undefined), datasourceId: advDatasourceId, limit: 1 })
          let v = 0
          const rows: any[] = Array.isArray(res.rows) ? (res.rows as any[]) : []
          if (rows.length > 0) {
            const r0 = rows[0]
            if (Array.isArray(r0)) {
              const cell = r0[r0.length-1]
              if (typeof cell === 'number' && isFinite(cell)) v = cell
              else for (const c of r0) if (typeof c === 'number' && isFinite(c)) { v = c; break }
            }
          }
          if (!cancelled) setLocalKpi(v)
        } else {
          const whereObj = (() => { try { return advWhere.trim() ? JSON.parse(advWhere) : {} } catch { return {} } })()
          const spec: any = { source: advSource, agg, where: Object.keys(whereObj).length ? whereObj : undefined }
          if (xFld) spec.x = xFld
          if (catFld) spec.legend = catFld
          if (agg !== 'count' && field) { spec.y = field; spec.measure = field }
          const res = await QueryApi.querySpec({ spec, datasourceId: advDatasourceId, limit: 10000, offset: 0, includeTotal: false })
          let v = 0
          const rows: any[] = Array.isArray(res.rows) ? (res.rows as any[]) : []
          const cols: string[] = Array.isArray(res.columns) ? (res.columns as string[]) : []
          const colsL = cols.map(c => String(c).toLowerCase())
          const vi = colsL.indexOf('value')
          const xi = colsL.indexOf('x')
          const selKey = (() => {
            if (advXPick === 'today' || advXPick === 'yesterday') return ymd(new Date())
            if (advXPick === 'this_month') { const d = new Date(); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}` }
            if (advXPick === 'custom' && advXValue) return String(advXValue)
            return ''
          })()
          const pickTok = advXPick
          const xMatches = (xv: any) => {
            const s = String(xv ?? '')
            if (!selKey) return true
            if (pickTok === 'this_month') return s.startsWith(selKey)
            return s.startsWith(selKey)
          }
          if (rows.length > 0) {
            if (Array.isArray(rows[0])) {
              for (const r of rows) {
                if (xi >= 0 && !xMatches(r[xi])) continue
                const cell = (vi >= 0 ? r[vi] : undefined)
                if (typeof cell === 'number' && isFinite(cell)) v += cell
                else for (const c of r) if (typeof c === 'number' && isFinite(c)) { v += c; break }
              }
            } else if (typeof rows[0] === 'object' && rows[0] != null) {
              const keyV = (vi >= 0 ? cols[vi] : undefined)
              const keyX = (xi >= 0 ? cols[xi] : undefined)
              for (const r of rows) {
                if (keyX && !xMatches((r as any)[keyX])) continue
                const cell = (keyV ? (r as any)[keyV] : (r as any)['value'])
                if (typeof cell === 'number' && isFinite(cell)) v += cell
                else for (const k of Object.keys(r as any)) { const cv = (r as any)[k]; if (typeof cv === 'number' && isFinite(cv)) { v += cv; break } }
              }
            }
          }
          if (!cancelled) setLocalKpi(v)
        }
      } catch { if (!cancelled) setLocalKpi(null) }
      finally { if (!cancelled) setLocalKpiLoading(false) }
    }
    void run(); return () => { cancelled = true }
  }, [advOpen, advDatasourceId, advSource, advWhere, JSON.stringify(advPivot), advMeasure, advAgg, advXField, advXValue, advXPick, advXFrom, advXTo, advCalcMode])

  // Compute per-category threshold matches when grouped by Legend (category) and optional X dimension; skip when a specific X value is entered
  useEffect(() => {
    let cancelled = false
    async function run() {
      try {
        setPerCatStats(null)
        setPerCatMatches([])
        if (!advOpen) return
        if (!advSource) return
        const agg = (advPivot.values?.[0]?.agg as any) || advAgg
        const field = advPivot.values?.[0]?.field || advMeasure || undefined
        const catFld = (advPivot.legend as any) || undefined
        const xFld = (advPivot.x as any) || undefined
        if (!catFld) return
        if (!agg) return
        if (agg !== 'count' && !field) return
        const thrRaw = String(value || '0')
        const parts = thrRaw.split(',').map(s => Number(s.trim())).filter(n => Number.isFinite(n))
        const thr = parts.length ? parts[0] : Number(thrRaw)
        const lo = parts.length ? Math.min(...parts) : Number.NaN
        const hi = parts.length ? Math.max(...parts) : Number.NaN
        // Build WHERE with X window so totals reflect the selected period/value
        const whereSel: Record<string, any> = (() => { try { return advWhere.trim() ? JSON.parse(advWhere) : {} } catch { return {} } })()
        if (xFld) {
          const today = new Date()
          if (advXPick === 'today') {
            // As-of cumulative: only upper bound < tomorrow
            const e = new Date(today); e.setDate(e.getDate()+1)
            delete (whereSel as any)[`${xFld}__gte`]
            delete (whereSel as any)[`${xFld}__gt`]
            whereSel[`${xFld}__lt`] = ymd(e)
          } else if (advXPick === 'yesterday') {
            const e = today
            const s = new Date(today); s.setDate(s.getDate()-1)
            whereSel[`${xFld}__gte`] = ymd(s)
            whereSel[`${xFld}__lt`] = ymd(e)
          } else if (advXPick === 'this_month') {
            const s = new Date(today.getFullYear(), today.getMonth(), 1)
            const e = new Date(today.getFullYear(), today.getMonth()+1, 1)
            whereSel[`${xFld}__gte`] = ymd(s)
            whereSel[`${xFld}__lt`] = ymd(e)
          } else if (advXPick === 'range') {
            if (advXFrom) whereSel[`${xFld}__gte`] = advXFrom
            if (advXTo) { const d = new Date(advXTo + 'T00:00:00'); d.setDate(d.getDate()+1); whereSel[`${xFld}__lt`] = ymd(d) }
          } else if (advXPick === 'custom' && advXValue) {
            whereSel[xFld] = [advXValue]
          }
        }
        setPerCatLoading(true)
        {
          // Always use pivot for per-legend scan to support multi-legend dims
          const legendDims = Array.isArray((advPivot.legend as any)) ? ((advPivot.legend as any) as string[]) : ((advPivot.legend as any) ? [String((advPivot.legend as any))] : [])
          const includeX = !(['today','yesterday','this_month','range'].includes(String(advXPick))) && !!xFld
          const rowsDims: string[] = includeX ? [...legendDims, String(xFld as any)] : [...legendDims]
          const res = await Api.pivot({ source: advSource, rows: rowsDims, cols: [], aggregator: agg as any, valueField: (agg !== 'count' ? (field as any) : undefined), where: (Object.keys(whereSel).length ? whereSel : undefined), datasourceId: advDatasourceId, limit: 20000 })
          let rows: any[] = Array.isArray(res.rows) ? (res.rows as any[]) : []
          const catDims = legendDims.length
          const dims = rowsDims.length
          const xIdx = (includeX ? catDims : -1)
          const numIdx = dims
          const selKey = (() => { if (advXPick === 'today' || advXPick === 'yesterday') return ymd(new Date()); if (advXPick === 'this_month') { const d = new Date(); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}` }; if (advXPick === 'custom' && advXValue) return String(advXValue); return '' })()
          const applyXMatch = !(advXPick === 'today' || advXPick === 'yesterday' || advXPick === 'this_month' || advXPick === 'range')
          const xMatches = (xv: any) => { const s = String(xv ?? ''); if (!selKey) return true; if (advXPick === 'this_month') return s.startsWith(selKey); return s.startsWith(selKey) }
          const check = (v: number) => {
            if (!Number.isFinite(v)) return false
            switch (operator) {
              case '>': return v > thr
              case '>=': return v >= thr
              case '<': return v < thr
              case '<=': return v <= thr
              case '==': return v === thr
              case 'between': return Number.isFinite(lo) && Number.isFinite(hi) ? (v >= lo && v <= hi) : false
              default: return true
            }
          }
          let total = 0, matches = 0
          const matchesList: Array<{ category: any; value: number; x?: any }> = []
          for (const r of rows) {
            if (!Array.isArray(r)) continue
            const v = (numIdx >= 0 ? r[numIdx] : undefined)
            const xv = (xIdx >= 0 ? r[xIdx] : undefined)
            const catParts: string[] = []
            for (let i=0;i<catDims;i++) { const cell = r[i]; catParts.push(cell == null ? '' : String(cell)) }
            const cat = catParts.filter(Boolean).join(' • ')
            if (typeof v === 'number') {
              total++
              const catStr = (cat == null ? '' : String(cat))
              if (catFld && (!catStr || catStr.trim() === '')) continue
              const finalCat = (catStr && catStr.trim() !== '') ? catStr : 'All'
              if (check(v) && (!applyXMatch || xMatches(xv))) { matches++; matchesList.push({ category: finalCat, value: v, x: xv }) }
            }
          }
          // If nothing returned with [legend, x], retry with [legend] only
          if (rows.length === 0 && includeX) {
            try {
              const resL = await Api.pivot({ source: advSource, rows: legendDims, cols: [], aggregator: agg as any, valueField: (agg !== 'count' ? (field as any) : undefined), where: (Object.keys(whereSel).length ? whereSel : undefined), datasourceId: advDatasourceId, limit: 20000 })
              rows = Array.isArray(resL.rows) ? (resL.rows as any[]) : []
              const valIdx2 = legendDims.length
              for (const r of rows) {
                if (!Array.isArray(r)) continue
                const v = (valIdx2 >= 0 ? r[valIdx2] : undefined)
                const catParts: string[] = []
                for (let i=0;i<legendDims.length;i++) { const cell = r[i]; catParts.push(cell == null ? '' : String(cell)) }
                const cat = catParts.filter(Boolean).join(' • ')
                if (typeof v === 'number') {
                  total++
                  const catStr = (cat == null ? '' : String(cat))
                  if (catFld && (!catStr || catStr.trim() === '')) continue
                  const finalCat = (catStr && catStr.trim() !== '') ? catStr : 'All'
                  if (check(v)) { matches++; matchesList.push({ category: finalCat, value: v }) }
                }
              }
            } catch {}
          }
          if (!catFld && total === 0 && rows.length === 0) {
            try {
              const res2 = await Api.pivot({ source: advSource, rows: [], cols: [], aggregator: agg as any, valueField: (agg !== 'count' ? (field as any) : undefined), where: (Object.keys(whereSel).length ? whereSel : undefined), datasourceId: advDatasourceId, limit: 1 })
              const rows2: any[] = Array.isArray(res2.rows) ? (res2.rows as any[]) : []
              if (rows2.length > 0 && Array.isArray(rows2[0])) {
                const r0 = rows2[0]
                let val: any = r0[r0.length-1]
                if (!(typeof val === 'number' && isFinite(val))) { for (const c of r0) { if (typeof c === 'number' && isFinite(c)) { val = c; break } } }
                if (typeof val === 'number' && isFinite(val)) {
                  total = 1
                  matches = check(val) ? 1 : 0
                  matchesList.push({ category: 'All', value: val })
                }
              }
            } catch {}
          }
          if (!cancelled) { setPerCatStats({ total, matches }); setPerCatMatches(matchesList) }
        }
      } catch { if (!cancelled) setPerCatStats(null) }
      finally { if (!cancelled) setPerCatLoading(false) }
    }
    void run(); return () => { cancelled = true }
  }, [advOpen, advDatasourceId, advSource, advWhere, JSON.stringify(advPivot), advMeasure, advAgg, operator, value, advXValue, advXPick, advXFrom, advXTo, advCalcMode])

  const carriedSummary = useMemo(() => {
    try {
      if (mode === 'edit' && alert) {
        const cfg: any = alert.config || {}
        const t0 = Array.isArray(cfg.triggers) ? (cfg.triggers.find((t: any) => String(t?.type) === 'threshold') || null) : null
        if (!t0) return null
        const lf = t0.legendField
        const cat = (() => { try { const v = lf ? (t0.where || {})[lf] : undefined; if (Array.isArray(v)) return String(v[0] ?? '') || 'All'; if (v != null) return String(v); return (lf ? 'All' : '—') } catch { return (lf ? 'All' : '—') } })()
        return { datasourceId: cfg.datasourceId, source: t0.source, aggregator: t0.aggregator, measure: t0.measure, xField: t0.xField, xValue: t0.xValue, legendField: lf, category: cat, where: t0.where, widgetId: (alert as any)?.widgetId || (cfg?.render?.widgetRef?.widgetId) }
      } else if (widget) {
        const whereObj = (() => { try { return advWhere.trim() ? JSON.parse(advWhere) : (spec?.where || undefined) } catch { return spec?.where } })()
        const agg = advOpen ? ((advPivot.values?.[0]?.agg as any) || advAgg) : aggSel
        const meas = advOpen ? (advPivot.values?.[0]?.field || undefined) : (measureSel || undefined)
        const xFld = advOpen ? ((advPivot.x as any) || (advPivot.legend as any) || undefined) : (spec?.x || undefined)
        const xVal = advOpen ? (advXValue || undefined) : (xValueSel || undefined)
        const lf = advOpen ? ((advPivot.legend as any) || undefined) : (spec as any)?.legend
        const cat = (() => { try { const v = lf ? (whereObj as any)?.[lf] : undefined; if (Array.isArray(v)) return String(v[0] ?? '') || 'All'; if (v != null) return String(v); return (lf ? 'All' : '—') } catch { return (lf ? 'All' : '—') } })()
        return { datasourceId: (widget as any)?.datasourceId, source: spec?.source, aggregator: agg, measure: meas, xField: xFld, xValue: xVal, legendField: lf, category: cat, where: whereObj, widgetId: (widget as any)?.id }
      }
    } catch {}
    return null
  }, [mode, alert?.id, JSON.stringify((alert as any)?.config || {}), (widget as any)?.id, aggSel, measureSel, xValueSel, advOpen, JSON.stringify(advPivot), advWhere, advAgg, advXValue])

  const buildPayload = (): AlertCreate => {
    const triggers: any[] = []
    const cfgDsId = advOpen ? (advDatasourceId || undefined) : ((mode==='edit' && alert) ? ((alert as any).config?.datasourceId) : ((widget as any)?.datasourceId))

    if (triggerTimeEnabled) {
      triggers.push({ type: 'time', cron: buildCron(timeOfDay, { mode: scheduleKind, dows: daysOfWeek, doms: daysOfMonth, everyHours }) })
    }
    if (triggerThresholdEnabled) {
      const useAgg = (advOpen ? ((advPivot.values?.[0]?.agg as any) || advAgg) : (aggSel || defaultAggFromSpec(spec))) as any
      const useMeasure = (useAgg === 'count') ? undefined : (advOpen ? (advPivot.values?.[0]?.field || advMeasure || undefined) : (measureSel || spec?.measure || spec?.y || undefined))
      const source = advOpen ? advSource : (mode==='edit' && alert ? ((alert as any).config?.triggers?.find?.((t:any)=>String(t?.type)==='threshold')?.source || (alert as any).config?.source) : (spec?.source || ''))
      let whereObj: any = undefined
      if (advOpen) {
        try { whereObj = advWhere.trim() ? JSON.parse(advWhere) : undefined } catch { whereObj = undefined }
      } else {
        whereObj = (mode==='edit' && alert) ? ((alert as any).config?.triggers?.find?.((t:any)=>String(t?.type)==='threshold')?.where) : (spec?.where || undefined)
      }
      const xField = advOpen ? (((advPivot.x as any) || advXField || undefined)) : (spec?.x || (mode==='edit' && alert ? ((alert as any).config?.triggers?.find?.((t:any)=>String(t?.type)==='threshold')?.xField) : undefined))
      const xValue = advOpen ? undefined : ((xValueSel || '').trim() === '' ? undefined : (isNaN(Number(xValueSel)) ? xValueSel : Number(xValueSel)))
      const legendField = advOpen ? ((advPivot.legend as any) || undefined) : undefined
      const legendDims: string[] = Array.isArray(legendField) ? (legendField as string[]) : (legendField ? [String(legendField)] : [])
      const rowDimsAll: string[] = Array.isArray(advPivot.x) ? (advPivot.x as string[]) : (advPivot.x ? [String(advPivot.x)] : (xField ? [String(xField)] : []))
      const xFieldFirst = rowDimsAll.length ? rowDimsAll[0] : undefined
      const rowFields = rowDimsAll.length > 1 ? rowDimsAll.slice(1) : undefined
      const t: any = { type: 'threshold', source: String(source || ''), aggregator: useAgg, where: whereObj, xField: xFieldFirst, legendField: (legendDims.length === 1 ? legendDims[0] : undefined), legendFields: (legendDims.length > 1 ? legendDims : undefined), rowFields, xValue, operator, value: value.includes(',') ? value.split(',').map((s)=>Number(s)) : Number(value) }
      if (advOpen) {
        if (advXPick === 'today' || advXPick === 'yesterday' || advXPick === 'this_month') { t.xMode = 'token'; t.xToken = advXPick }
        if (advXPick === 'last' || advXPick === 'min' || advXPick === 'max') { t.xMode = 'special'; t.xSpecial = advXPick }
        if (advXPick === 'range') { t.xMode = 'range'; t.xRange = { from: advXFrom || undefined, to: advXTo || undefined } }
        if (advXPick === 'custom') { t.xMode = 'custom' }
      }
      if (useMeasure) { t.measure = useMeasure; t.y = useMeasure }
      triggers.push(t)
    }

    const actions: any[] = []
    // Build recipients from unified tokens and pending input; filter by selected channels
    const emailSet = new Set<string>()
    const phoneSet = new Set<string>()
    recipTokens.forEach((t) => {
      if (t.kind === 'email') { const v=(t.value||'').trim(); if (v) emailSet.add(v) }
      else if (t.kind === 'phone') { const v=(t.value||'').trim(); if (v) phoneSet.add(v) }
      else if (t.kind === 'contact') {
        const em = (t.email||'').trim(); if (em) emailSet.add(em)
        const ph = (t.phone||'').trim(); if (ph) phoneSet.add(ph)
      }
    })
    // Expand selected tags
    recipTokens.filter(t => t.kind==='tag').forEach(t => {
      const tg = (t.tag || t.value || '').trim()
      if (!tg) return
      const exp = tagExpansions[tg]
      if (exp) {
        (exp.emails || []).forEach((em: string) => { const v=String(em||'').trim(); if (v) emailSet.add(v) });
        (exp.phones || []).forEach((ph: string) => { const v=String(ph||'').trim(); if (v) phoneSet.add(v) });
      }
    });
    const pending = (recipInput || '').trim()
    if (pending) {
      if (isValidEmail(pending)) emailSet.add(pending)
      else if (isValidPhone(pending)) phoneSet.add(pending)
    }
    // Also include currently selected suggestions that haven't been added as tokens yet
    try {
      const selSet = new Set<string>(recipSel)
      for (const s of recipSuggestions) {
        const k = (s.type === 'contact' ? `c:${(s.id||'').toLowerCase()}` : `t:${(s.tag||'').toLowerCase()}`)
        if (!selSet.has(k)) continue
        if (s.type === 'contact') {
          const em = String(s.email||'').trim(); if (em) emailSet.add(em)
          const ph = String(s.phone||'').trim(); if (ph) phoneSet.add(ph)
        } else if (s.type === 'tag') {
          const tg = String(s.tag||'').trim()
          if (!tg) continue
          const exp = tagExpansions[tg]
          if (exp) {
            for (const em of (exp.emails ?? [])) { const v = String(em || '').trim(); if (v) emailSet.add(v) }
            for (const ph of (exp.phones ?? [])) { const v = String(ph || '').trim(); if (v) phoneSet.add(v) }
          }
        }
      }
    } catch {}
    // Also include direct inputs from Email To / SMS To fields
    try {
      const splitList = (s: string) => (s||'').split(/[;\,\s]+/).map(x=>x.trim()).filter(Boolean)
      for (const em of splitList(emailTo)) { if (isValidEmail(em)) emailSet.add(em) }
      for (const ph of splitList(smsTo)) { if (isValidPhone(ph)) phoneSet.add(ph) }
    } catch {}
    const emails = Array.from(emailSet)
    const phones = Array.from(phoneSet)
    // Persist Email action even when no emails yet; backend/UI can allow editing later
    if (chanEmail) actions.push({ type: 'email', to: emails, subject: name })
    // Persist SMS action even when no phones yet; backend/UI can allow editing later
    if (chanSms) actions.push({ type: 'sms', to: phones, message: (templateSms && templateSms.trim()) ? templateSms : (template || name) })

    // Provide a concrete KPI QuerySpec so backend can compute {{kpi}} reliably
    const kpiSource = (advOpen ? (advSource || spec?.source) : (spec?.source)) || ''
    const kpiAgg = (advOpen ? ((advPivot.values?.[0]?.agg as any) || advAgg) : (aggSel || defaultAggFromSpec(spec))) as any
    const kpiMeasure = (kpiAgg === 'count') ? undefined : (advOpen ? (advPivot.values?.[0]?.field || advMeasure || undefined) : (measureSel || spec?.measure || spec?.y || undefined))
    const kpiWhere = (() => {
      if (advOpen) { try { return advWhere.trim() ? JSON.parse(advWhere) : undefined } catch { return undefined } }
      return spec?.where || undefined
    })()
    const kpiSpec: any = { source: String(kpiSource || ''), agg: kpiAgg, where: kpiWhere }
    if (kpiMeasure) { kpiSpec.y = kpiMeasure; kpiSpec.measure = kpiMeasure }
    let render: any
    if (kind === 'notification') {
      let refSpec: any = null
      let refLabel = (widget as any)?.title || name
      if (rendererUseCarried && widget && (widget as any)?.querySpec) {
        refSpec = (widget as any).querySpec
        refLabel = (widget as any)?.title || name
      } else if (!rendererUseCarried && pickWidgetCfg && (pickWidgetCfg as any)?.querySpec) {
        refSpec = (pickWidgetCfg as any).querySpec
        refLabel = (pickWidgets.find(w => w.id === pickWidgetId)?.title || name)
      }
      if (renderMode === 'table') {
        render = { mode: 'table', querySpec: refSpec || spec, width: snapWidth, height: snapHeight }
      } else if (renderMode === 'chart') {
        render = { mode: 'chart', querySpec: refSpec || spec, width: snapWidth, height: snapHeight }
      } else {
        render = { mode: 'kpi', label: refLabel, querySpec: refSpec || kpiSpec, width: snapWidth, height: snapHeight }
      }
      if (rendererUseCarried && widget && (widget as any)?.id) {
        (render as any).widgetRef = { dashboardId: ((widget as any)?.dashboardId || parentDashboardId || ''), widgetId: (widget as any)?.id }
      } else if (!rendererUseCarried && pickDashId && pickWidgetId) {
        (render as any).widgetRef = { dashboardId: pickDashId, widgetId: pickWidgetId }
      }
    } else {
      render = { mode: 'kpi', label: (widget as any)?.title || name, querySpec: kpiSpec, width: snapWidth, height: snapHeight }
      if (rendererUseCarried && widget && (widget as any)?.id) {
        (render as any).widgetRef = { dashboardId: ((widget as any)?.dashboardId || parentDashboardId || ''), widgetId: (widget as any)?.id }
      } else if (!rendererUseCarried && pickDashId && pickWidgetId) {
        (render as any).widgetRef = { dashboardId: pickDashId, widgetId: pickWidgetId }
      }
    }
    // Back-compat: include top-level fields many backends expect
    const cfgAny: any = { datasourceId: cfgDsId, triggers, actions, render, template, source: kpiSource, where: kpiWhere, agg: kpiAgg }
    try {
      const cpObj = (customPlaceholders || []).reduce((acc, it) => {
        const key = String(it?.name || '').trim()
        if (/^\w+$/.test(key)) acc[key] = String(it?.html || '')
        return acc
      }, {} as Record<string, string>)
      if (Object.keys(cpObj).length) (cfgAny as any).customPlaceholders = cpObj
    } catch {}
    if (kpiMeasure) cfgAny.measure = kpiMeasure
    // V2 triggersGroup
    const group: any = {
      logic: triggerLogic,
      time: { enabled: triggerTimeEnabled, time: (scheduleKind==='hourly'? undefined : timeOfDay), schedule: (scheduleKind==='hourly' ? { kind: scheduleKind, everyHours } : { kind: scheduleKind, dows: daysOfWeek, doms: daysOfMonth }) },
      threshold: undefined as any,
    }
    if (triggerThresholdEnabled) {
      // Recompute legend/x arrays for group.threshold
      const legendFieldAny = (advOpen ? ((advPivot.legend as any) || undefined) : undefined)
      const legendDims2: string[] = Array.isArray(legendFieldAny) ? (legendFieldAny as string[]) : (legendFieldAny ? [String(legendFieldAny)] : [])
      const rowDimsAll2: string[] = Array.isArray(advPivot.x) ? (advPivot.x as string[]) : (advPivot.x ? [String(advPivot.x)] : (advXField ? [String(advXField)] : []))
      const xFieldFirst2 = rowDimsAll2.length ? rowDimsAll2[0] : undefined
      const rowFields2 = rowDimsAll2.length > 1 ? rowDimsAll2.slice(1) : undefined
      group.threshold = {
        enabled: true,
        source: kpiSource,
        aggregator: kpiAgg,
        measure: kpiMeasure,
        where: kpiWhere,
        xField: (xFieldFirst2 || undefined),
        legendField: (legendDims2.length === 1 ? legendDims2[0] : undefined),
        legendFields: (legendDims2.length > 1 ? legendDims2 : undefined),
        rowFields: rowFields2,
        xMode: advOpen ? advXPick : 'custom',
        xValue: advOpen ? (advXPick==='custom'?advXValue:undefined) : xValueSel || undefined,
        xToken: advOpen && (advXPick==='today'||advXPick==='yesterday'||advXPick==='this_month') ? advXPick : undefined,
        xSpecial: advOpen && (advXPick==='last'||advXPick==='min'||advXPick==='max') ? advXPick : undefined,
        xRange: advOpen && (advXPick==='range') ? ({ from: advXFrom || undefined, to: advXTo || undefined }) : undefined,
        operator,
        value: value.includes(',') ? value.split(',').map((s)=>Number(s)) : Number(value),
        calcMode: advCalcMode,
      }
    } else {
      group.threshold = { enabled: false }
    }
    cfgAny.triggersGroup = group
    const cfg = cfgAny as AlertConfig
    // Prefer explicit dashboard picked in Header when renderer uses 'Pick from dashboard'
    const payloadDashId = (() => {
      const carried = rendererUseCarried
      const wDid = (widget as any)?.dashboardId
      const aDid = (alert as any)?.dashboardId
      const pDid = parentDashboardId
      // Prefer explicit pick if available; otherwise try carried/widget/parent/alert in order
      if (!carried) return (pickDashId || wDid || aDid || pDid || undefined) as any
      return (wDid || pDid || pickDashId || aDid || undefined) as any
    })()
    const payload: AlertCreate = { name, kind, widgetId: (widget as any)?.id, dashboardId: payloadDashId, enabled, config: cfg as any }
    return payload
  }

  const onTestEvaluate = async () => {
    try {
      setTestEvaluating(true)
      setTestHtml('')
      setTestEmailHtml('')
      setTestSmsText('')
      setTestContext(null)
      setTestSummary(null)
      const payload = buildPayload()
      setLastPayload(payload)
      const res2 = await Api.evaluateAlertV2({ name: payload.name, dashboardId: payload.dashboardId, config: payload.config }, user?.id || undefined)
      setTestEmailHtml(res2?.emailHtml || '')
      // Keep a copy for the Raw tab; fall back to legacy field if present
      setTestHtml((res2 as any)?.emailHtml || (res2 as any)?.html || '')
      setTestSmsText(res2?.smsText || '')
      setTestContext(res2?.context || null)
      setTestSummary(res2?.humanSummary || null)
      setTestActiveTab('email')
      setTestEvaluating(false)
    } catch (e: any) {
      const msg = (e && (e.message || String(e))) || 'Failed to evaluate alert'
      setTestEmailHtml(`<div style="color:#ef4444">${msg}</div>`)
      setTestContext({ error: msg })
      setTestActiveTab('context')
      setTestEvaluating(false)
    }
  }

  const onSave = async () => {
    try {
      const payload = buildPayload()
      if (mode === 'edit' && alert?.id) {
        const res = await Api.updateAlert(alert.id, payload)
        onSavedAction(res)
      } else {
        const res = await Api.createAlert(payload)
        onSavedAction(res)
      }
      onCloseAction()
    } catch {}
  }

  if (!open || typeof document === 'undefined') return null
  return createPortal(
    <div className="fixed inset-0 z-[1200]">
      <div className="absolute inset-0 bg-black/40" onClick={() => onCloseAction()} />
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[1040px] max-w-[96vw] max-h-[90vh] overflow-hidden rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="text-sm font-medium">{mode==='edit' ? 'Edit Alert' : 'Create Alert/Notification'}</div>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCloseAction}>✕</button>
        </div>
        <div className="flex gap-4 items-stretch min-h-0">
          <div className="w-44 shrink-0">
            <div className="flex flex-col gap-1 text-sm">
              {(['header','recipients','trigger','insert','preview'] as const).map(s => (
                <button
                  key={s}
                  type="button"
                  className={`${tabBase} ${uiSection===s ? tabActive : ''} w-full text-left`}
                  onClick={()=> setUiSection(s)}
                >
                  {s === 'insert' ? 'Insert Template' : (s[0].toUpperCase()+s.slice(1))}
                </button>
              ))}
            </div>
          </div>
          <div className="flex-1 overflow-auto max-h-[70vh] pr-1">
            {uiSection === 'header' && (
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                <label className="text-sm">Name<input className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" value={name} onChange={(e)=>setName(e.target.value)} /></label>
                <label className="text-sm">Type<select className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" value={kind} onChange={(e)=>setKind(e.target.value as any)}><option value="alert">Alert</option><option value="notification">Notification</option></select></label>
                <label className="text-sm inline-flex items-center gap-2 mt-6"><input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={enabled} onChange={(e)=>setEnabled(e.target.checked)} /><span>Enabled</span></label>
                <div className="md:col-span-3">
                  <div className="text-sm mb-1">Channels</div>
                  <div className="flex items-center gap-4 text-xs">
                    <label className="inline-flex items-center gap-2"><input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={chanEmail} onChange={(e)=> setChanEmail(e.target.checked)} /> Email</label>
                    <label className="inline-flex items-center gap-2"><input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={chanSms} onChange={(e)=> setChanSms(e.target.checked)} /> SMS</label>
                  </div>
                </div>
                {kind==='notification' && (
                  <div className="md:col-span-3 mt-2">
                    <div className="text-sm mb-1">Renderer</div>
                    <div className="flex items-center gap-2 text-xs mb-2">
                      <button type="button" className={`px-2 py-1 rounded-md border border-[hsl(var(--border))] ${rendererUseCarried?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setRendererUseCarried(true)}>Use carried widget</button>
                      <button type="button" className={`px-2 py-1 rounded-md border border-[hsl(var(--border))] ${!rendererUseCarried?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setRendererUseCarried(false)}>Pick from dashboard</button>
                    </div>
                    {!rendererUseCarried && (
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                        <label className="text-sm">Dashboard<select className="mt-1 w-full h-8 px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)]" value={pickDashId} onChange={(e)=> setPickDashId(e.target.value)}>
                          <option value="">Select dashboard</option>
                          {dashList.map(d => (<option key={d.id} value={d.id}>{d.name}</option>))}
                        </select></label>
                        <label className="text-sm md:col-span-2">Widget<select className="mt-1 w-full h-8 px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)]" value={pickWidgetId} onChange={(e)=> setPickWidgetId(e.target.value)} disabled={!pickDashId}>
                          <option value="">Select widget</option>
                          {pickWidgets.map(w => (<option key={w.id} value={w.id}>{w.title}</option>))}
                        </select></label>
                      </div>
                    )}
                    {(!rendererUseCarried && !pickWidgetId) && (
                      <div className="mt-2 text-xs">View as
                        <span className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden ml-2 divide-x divide-[hsl(var(--border))]">
                          <button type="button" className={`px-2 py-1 ${renderMode==='kpi'?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setRenderMode('kpi')}>KPI</button>
                          <button type="button" className={`px-2 py-1 ${renderMode==='table'?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setRenderMode('table')}>Table</button>
                          <button type="button" className={`px-2 py-1 ${renderMode==='chart'?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setRenderMode('chart')}>Chart</button>
                        </span>
                      </div>
                    )}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-2">
                      <label className="text-sm md:col-span-3 inline-flex items-center gap-2">
                        <input
                          type="checkbox"
                          className="h-4 w-4 accent-[hsl(var(--primary))]"
                          checked={useWidgetAspect}
                          onChange={(e)=> setUseWidgetAspect(e.target.checked)}
                          disabled={(rendererUseCarried ? (!((widget as any)?.id && (((widget as any)?.dashboardId || parentDashboardId)))) : (!(pickDashId && pickWidgetId)))}
                        />
                        <span>Use widget dimensions/aspect ratio</span>
                      </label>
                      <label className="text-sm">Snapshot width
                        <input type="number" className="mt-1 w-full h-8 px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)]" value={snapWidth} onChange={(e)=> setSnapWidth(Math.max(100, Math.min(4000, Number(e.target.value)||0)))} />
                      </label>
                      <label className="text-sm">Snapshot height
                        <input
                          type="number"
                          className={`mt-1 w-full h-8 px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)] ${useWidgetAspect && refLayout ? 'opacity-60 cursor-not-allowed' : ''}`}
                          value={snapHeight}
                          onChange={(e)=> setSnapHeight(Math.max(80, Math.min(3000, Number(e.target.value)||0)))}
                          disabled={useWidgetAspect && !!refLayout}
                          title={useWidgetAspect && refLayout ? 'Height derived from widget aspect ratio' : undefined}
                        />
                      </label>
                      
                    </div>
                  </div>
                )}
              </div>
            )}

            {uiSection === 'recipients' && (
              <div className="space-y-2 text-sm">
                <div className="mb-1">Recipients</div>
                <div className="relative">
                  <div className="min-h-9 rounded-md border bg-[hsl(var(--secondary)/0.6)] p-1 flex flex-wrap gap-1 items-center">
                    {recipTokens.map((t, i) => {
                      const disp = (t.kind === 'contact') ? (t.name || t.label || t.value) : (t.kind==='tag' ? (`#${t.tag || t.value}`) : (t.value || ''))
                      const warnMissing = (t.kind==='contact') && (((chanEmail && !(t.email||'').trim())) || ((chanSms && !(t.phone||'').trim())))
                      const title = warnMissing ? ((chanEmail && !(t.email||'').trim()) && (chanSms && !(t.phone||'').trim()) ? 'Missing email and phone for enabled channels' : (chanEmail && !(t.email||'').trim()) ? 'Missing email for Email channel' : 'Missing phone for SMS channel') : undefined
                      return (
                        <span key={(t.kind==='contact'?`contact:${t.id}`:`${t.kind}:${t.value}`)} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-xs bg-[hsl(var(--background))]" title={title}>
                          <span>{disp}</span>
                          {warnMissing && (<span className="text-[10px] text-amber-600" aria-label="Missing contact info">⚠</span>)}
                          <button type="button" className="opacity-70 hover:opacity-100" onClick={()=> setRecipTokens(prev => prev.filter((_, idx)=> idx!==i))}>✕</button>
                        </span>
                      )
                    })}
                    <input
                      className="flex-1 min-w-[200px] h-7 bg-transparent outline-none text-xs px-2"
                      placeholder="Type name, email, or phone; press Enter"
                      value={recipInput}
                      onChange={(e)=> setRecipInput(e.target.value)}
                      onFocus={()=> setRecipSugOpen(true)}
                      onKeyDown={(e)=> { if (e.key==='Enter' || e.key==='Tab' || e.key===',' || e.key===';') { e.preventDefault(); tryCommitRecipInput() } if (e.key==='Backspace' && !recipInput) { setRecipTokens(prev => prev.slice(0,-1)) } }}
                      onBlur={()=> setTimeout(()=> setRecipSugOpen(false), 120)}
                    />
                  </div>
                  {recipSugOpen && recipSuggestions.length>0 && (
                    <div className="absolute z-[1001] mt-1 w-full max-h-56 overflow-auto rounded-md border bg-[hsl(var(--card))] shadow">
                      <div className="sticky top-0 z-10 flex items-center justify-between px-2 py-1 border-b bg-[hsl(var(--card))] text-[11px]">
                        <div>{Array.from(recipSel).length} selected</div>
                        <div className="flex items-center gap-2">
                          <button className="px-2 py-0.5 rounded border" onMouseDown={(e)=>e.preventDefault()} onClick={addSelectedRecipients}>Add selected</button>
                          <button className="px-2 py-0.5 rounded border" onMouseDown={(e)=>e.preventDefault()} onClick={()=> setRecipSel(new Set())}>Clear</button>
                        </div>
                      </div>
                      {recipSuggestions.map((s, idx) => {
                        const key = (s.type==='contact') ? `c:${(s.id||'').toLowerCase()}` : `t:${(s.tag||'').toLowerCase()}`
                        const checked = recipSel.has(recipKey(s))
                        return (
                          <button key={key || String(idx)} className="w-full text-left text-xs px-2 py-1 hover:bg-[hsl(var(--muted))] inline-flex items-center gap-2" onMouseDown={(e)=> e.preventDefault()} onClick={()=> toggleRecipSel(s)}>
                            <input type="checkbox" readOnly checked={checked} className="h-3 w-3" />
                            <span>{s.label}</span>
                          </button>
                        )
                      })}
                    </div>
                  )}
                </div>
                <div className="text-[11px] text-muted-foreground">Selected channels determine which contact fields are used (email for Email channel, phone for SMS).</div>
              </div>
            )}

            {uiSection === 'insert' && (
              <div>
                <div className="flex items-center justify-between mb-2">
                  <div className="text-sm font-medium">Message Templates</div>
                  <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden text-xs divide-x divide-[hsl(var(--border))]">
                    <button type="button" className={`px-2 py-1 ${msgKind==='email'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setMsgKind('email')}>Email</button>
                    <button type="button" className={`px-2 py-1 ${msgKind==='sms'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setMsgKind('sms')}>SMS</button>
                  </div>
                </div>
                <div className="rounded-md border border-[hsl(var(--border))] p-2 bg-[hsl(var(--background))]">
                  <div className="flex items-center justify-between mb-2">
                    <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden text-xs divide-x divide-[hsl(var(--border))]">
                      <button type="button" className={`px-2 py-1 ${msgView==='preview'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setMsgView('preview')}>Preview</button>
                      <button type="button" className={`px-2 py-1 ${msgView==='raw'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setMsgView('raw')}>Raw</button>
                    </div>
                    <div className="text-[11px] text-muted-foreground">Use chips to insert variables</div>
                  </div>
                  <div className="mb-2 flex flex-wrap gap-1">
                    {[
                      {k:'kpi', l:'KPI'},
                      {k:'kpi_fmt', l:'KPI (fmt)'},
                      {k:'operator', l:'Operator'},
                      {k:'threshold', l:'Threshold'},
                      {k:'threshold_low', l:'Lower'},
                      {k:'threshold_high', l:'Upper'},
                      {k:'agg', l:'Agg'},
                      {k:'measure', l:'Measure'},
                      {k:'xField', l:'X Field'},
                      {k:'xValue', l:'X Value'},
                      {k:'xValueResolved', l:'X Resolved'},
                      {k:'xValuePretty', l:'X Pretty'},
                      {k:'xPick', l:'X Pick'},
                      {k:'legendField', l:'Legend Field'},
                      {k:'legend', l:'Legend'},
                      {k:'filters', l:'Filters'},
                      {k:'filters_values', l:'Filter Values'},
                      {k:'filters_values_html', l:'Filter Chips'},
                      {k:'filters_json', l:'Filters JSON'},
                      {k:'KPI_IMG', l:'KPI IMG'},
                      {k:'CHART_IMG', l:'CHART IMG'},
                      {k:'TABLE_HTML', l:'TABLE HTML'},
                      {k:'source', l:'Source'},
                      {k:'datasourceId', l:'Datasource'}
                    ].map(c => (
                      <button key={c.k} type="button" className="inline-flex items-center gap-1 px-2 py-1 rounded border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] text-xs" onClick={()=> {
                        const tok = ` {{${c.k}}}`
                        if (msgKind==='email') setTemplate((t)=> (t||'') + tok)
                        else setTemplateSms((t)=> (t||'') + tok)
                      }}>{c.l}</button>
                    ))}
                  </div>
                  <div className="mt-3 text-xs">
                    <div className="text-xs mb-1">Formatting</div>
                    {[{k:'_br',l:'Line break'}, {k:'_nl',l:'New line (SMS)'}].map(c => (
                      <button key={c.k} type="button" className="inline-flex items-center gap-1 px-2 py-1 rounded border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] text-xs" onClick={()=> {
                        if (c.k === '_br') {
                          if (msgKind==='email') setTemplate((t)=> (t||'') + ' <br/>' )
                          else setTemplateSms((t)=> (t||'') + '\n')
                        } else if (c.k === '_nl') {
                          if (msgKind==='email') setTemplate((t)=> (t||'') + ' <br/>' )
                          else setTemplateSms((t)=> (t||'') + '\n')
                        }
                      }}>{c.l}</button>
                    ))}
                  </div>

                  <div className="mt-4">
                    <div className="text-sm font-medium mb-1">Custom placeholders</div>
                    <div className="space-y-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-2">
                      {customPlaceholders.map((ph, idx) => (
                        <div key={idx} className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-2 space-y-2">
                          <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                            <label className="text-xs">Name
                              <input className="mt-1 w-full h-8 px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)]" placeholder="PLACEHOLDER" value={ph.name} onChange={(e)=> setCustomPlaceholders((arr)=>{ const next=[...arr]; next[idx] = { ...next[idx], name: e.target.value }; return next })} />
                            </label>
                            <div className="md:col-span-2">
                              <div className="text-xs">HTML</div>
                              <div className="mt-1 flex flex-wrap items-center gap-2 text-xs">
                                <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden divide-x divide-[hsl(var(--border))]">
                                  <button type="button" className="px-2 py-1" onClick={()=> presetNormalAt(idx)}>Normal</button>
                                  <button type="button" className="px-2 py-1" onClick={()=> presetTableAt(idx)}>Table</button>
                                </div>
                                <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden divide-x divide-[hsl(var(--border))]">
                                  <button type="button" className="px-2 py-1" onClick={()=> spanStyleAt(idx,'font-weight: normal; font-style: normal; text-decoration: none;')}>N</button>
                                  <button type="button" className="px-2 py-1" onClick={()=> wrapSelAt(idx, '<b>','</b>')}>B</button>
                                  <button type="button" className="px-2 py-1" onClick={()=> wrapSelAt(idx, '<i>','</i>')}>I</button>
                                  <button type="button" className="px-2 py-1" onClick={()=> wrapSelAt(idx, '<u>','</u>')}>U</button>
                                </div>
                                <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center">
                                  <select className="h-7 px-2 bg-[hsl(var(--secondary)/0.6)]" value={fmtSize} onChange={(e)=> setFmtSize(e.target.value)}>
                                    <option value="12">12</option>
                                    <option value="14">14</option>
                                    <option value="16">16</option>
                                    <option value="18">18</option>
                                    <option value="20">20</option>
                                    <option value="24">24</option>
                                  </select>
                                  <button type="button" className="px-2 py-1 border-l border-[hsl(var(--border))]" onClick={()=> spanStyleAt(idx, `font-size: ${fmtSize}px;`)}>Size</button>
                                </div>
                                <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center">
                                  <input type="color" className="h-7 w-7 border-0 bg-transparent" value={fmtFontColor} onChange={(e)=> setFmtFontColor(e.target.value)} />
                                  <button type="button" className="px-2 py-1 border-l border-[hsl(var(--border))]" onClick={()=> spanStyleAt(idx, `color: ${fmtFontColor};`)}>Font</button>
                                </div>
                                <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center">
                                  <input type="color" className="h-7 w-7 border-0 bg-transparent" value={fmtBgColor} onChange={(e)=> setFmtBgColor(e.target.value)} />
                                  <button type="button" className="px-2 py-1 border-l border-[hsl(var(--border))]" onClick={()=> spanStyleAt(idx, `background-color: ${fmtBgColor};`)}>Bg</button>
                                </div>
                                <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center gap-1 px-1">
                                  <input type="color" className="h-7 w-7 border-0 bg-transparent" value={fmtBorderColor} onChange={(e)=> setFmtBorderColor(e.target.value)} />
                                  <span className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden divide-x divide-[hsl(var(--border))]">
                                    <button type="button" className="px-2 py-1" onClick={()=> spanStyleAt(idx, `display:inline-block; border: 1px solid ${fmtBorderColor}; padding: 4px; border-radius: 6px;`)}>Solid</button>
                                    <button type="button" className="px-2 py-1" onClick={()=> spanStyleAt(idx, `display:inline-block; border: 1px dashed ${fmtBorderColor}; padding: 4px; border-radius: 6px;`)}>Dashed</button>
                                    <button type="button" className="px-2 py-1" onClick={()=> spanStyleAt(idx, `display:inline-block; border: 1px dotted ${fmtBorderColor}; padding: 4px; border-radius: 6px;`)}>Dotted</button>
                                    <button type="button" className="px-2 py-1" onClick={()=> spanStyleAt(idx, `display:inline-block; border: none;`)}>None</button>
                                  </span>
                                </div>
                                <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center gap-1 px-2">
                                  <span className="text-[11px]">Spacing</span>
                                  <select className="h-7 px-1 bg-[hsl(var(--secondary)/0.6)]" value={String(fmtMarginTop)} onChange={(e)=> setFmtMarginTop(parseInt(e.target.value)||0)}>
                                    {[0,4,8,12,16,24].map(v=> (<option key={v} value={v}>{`Top ${v}`}</option>))}
                                  </select>
                                  <select className="h-7 px-1 bg-[hsl(var(--secondary)/0.6)]" value={String(fmtMarginBottom)} onChange={(e)=> setFmtMarginBottom(parseInt(e.target.value)||0)}>
                                    {[0,4,8,12,16,24].map(v=> (<option key={v} value={v}>{`Bottom ${v}`}</option>))}
                                  </select>
                                  <button type="button" className="px-2 py-1 rounded-md border border-[hsl(var(--border))]" onClick={()=> spanStyleAt(idx, `display:inline-block; margin-top: ${fmtMarginTop}px; margin-bottom: ${fmtMarginBottom}px;`)}>Apply</button>
                                </div>
                                <button type="button" className="px-2 py-1 rounded-md border border-[hsl(var(--border))]" onClick={()=> bulletsAt(idx)}>•</button>
                              </div>
                              <textarea ref={(el)=> { phAreaRefs.current[idx] = el }} className="mt-1 w-full h-20 px-2 py-1.5 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)] text-xs" value={ph.html} onChange={(e)=> setCustomPlaceholders((arr)=>{ const next=[...arr]; next[idx] = { ...next[idx], html: e.target.value }; return next })} />
                            </div>
                          </div>
                          <div className="flex items-center gap-2 text-xs">
                            <button type="button" className="px-2 py-1 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]" onClick={()=> { const k = (ph.name || '').trim(); if (!/^\w+$/.test(k)) return; const tok = ` {{${k}}}`; if (msgKind==='email') setTemplate((t)=> (t||'') + tok); else setTemplateSms((t)=> (t||'') + tok) }}>Insert tag</button>
                            <button type="button" className="px-2 py-1 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]" onClick={()=> setCustomPlaceholders((arr)=> arr.filter((_,i)=> i!==idx))}>Remove</button>
                          </div>
                        </div>
                      ))}
                      <button type="button" className="text-xs px-2 py-1 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]" onClick={()=> setCustomPlaceholders((arr)=> [...arr, { name: '', html: '' }])}>Add placeholder</button>
                    </div>
                  </div>

                  {(() => {
                    const agg = advOpen ? ((advPivot.values?.[0]?.agg as any) || advAgg) : (aggSel || '')
                    const measure = advOpen ? (advPivot.values?.[0]?.field || advMeasure || '') : (measureSel || '')
                    const xField = advOpen ? (((advPivot.x as any) || advXField || '')) : (spec?.x || '')
                    const xPick = advOpen ? advXPick : 'custom'
                    const xValue = advOpen ? '' : xValueSel
                    const filtersObj = (() => { try { return advOpen ? (advWhere.trim()? JSON.parse(advWhere): {}) : (spec?.where || {}) } catch { return {} } })()
                    const filtersHuman = Object.keys(filtersObj).map(k => `${k}=${Array.isArray(filtersObj[k])?filtersObj[k].join('|'):filtersObj[k]}`).join('; ')
                    const dsId = advOpen ? advDatasourceId : (widget as any)?.datasourceId
                    const src = advOpen ? advSource : (spec?.source || '')
                    const thrRaw = String(value || '0')
                    const parts = thrRaw.split(',').map(s=>Number(s.trim())).filter(n=>Number.isFinite(n))
                    const thr = parts.length?parts[0]:Number(thrRaw)
                    const legendField = advOpen ? ((advPivot.legend as any) || '') : ''
                    const catAdv = (() => {
                      if (!advOpen) return ''
                      if (!legendField) return ''
                      try {
                        const v = (filtersObj as any)[legendField]
                        if (Array.isArray(v) && v.length) return String(v[0])
                        if (v != null) return String(v)
                        return '(multiple)'
                      } catch { return '(multiple)' }
                    })()
                    const ctx: Record<string, any> = {
                      kpi: localKpi != null ? localKpi : '',
                      kpi_fmt: (localKpi != null && !isNaN(Number(localKpi))) ? Number(localKpi).toLocaleString() : '',
                      operator,
                      threshold: thr,
                      threshold_low: parts.length?Math.min(...parts):'',
                      threshold_high: parts.length?Math.max(...parts):'',
                      agg,
                      measure,
                      xField,
                      xValue,
                      xPick,
                      legendField,
                      legend: advOpen ? catAdv : '',
                      category: advOpen ? catAdv : (xValue || xPick),
                      filters: filtersHuman,
                      filters_json: JSON.stringify(filtersObj),
                      source: src,
                      datasourceId: dsId
                    }
                    const customMap = (() => {
                      try {
                        const entries = (customPlaceholders || []).map((it) => [String(it?.name || '').trim(), String(it?.html || '')] as const)
                        return entries.filter(([k]) => /^\w+$/.test(k)).reduce((acc, [k, v]) => { acc[k] = v; return acc }, {} as Record<string,string>)
                      } catch { return {} as Record<string,string> }
                    })()
                    const ctxFull: Record<string, any> = { ...ctx, ...customMap }
                    const fillOnce = (tpl: string) => tpl.replace(/\{\{\s*(\w+)\s*\}\}/g, (_m, k) => String(ctxFull[k] ?? ''))
                    const fill = (tpl: string) => {
                      let out = String(tpl || '')
                      for (let i=0;i<3;i++) {
                        const next = fillOnce(out)
                        if (next === out) break
                        out = next
                      }
                      return out
                    }
                    if (msgView==='raw') {
                      const tokenDefs = [
                        {k:'kpi', l:'KPI'},
                        {k:'kpi_fmt', l:'KPI (fmt)'},
                        {k:'operator', l:'Operator'},
                        {k:'threshold', l:'Threshold'},
                        {k:'threshold_low', l:'Lower'},
                        {k:'threshold_high', l:'Upper'},
                        {k:'agg', l:'Agg'},
                        {k:'measure', l:'Measure'},
                        {k:'xField', l:'X Field'},
                        {k:'xValue', l:'X Value'},
                        {k:'xValueResolved', l:'X Resolved'},
                        {k:'xValuePretty', l:'X Pretty'},
                        {k:'xPick', l:'X Pick'},
                        {k:'legendField', l:'Legend Field'},
                        {k:'legend', l:'Legend'},
                        {k:'filters', l:'Filters'},
                        {k:'filters_values', l:'Filter Values'},
                        {k:'filters_values_html', l:'Filter Chips'},
                        {k:'filters_json', l:'Filters JSON'},
                        {k:'KPI_IMG', l:'KPI IMG'},
                        {k:'CHART_IMG', l:'CHART IMG'},
                        {k:'TABLE_HTML', l:'TABLE HTML'},
                        {k:'source', l:'Source'},
                        {k:'datasourceId', l:'Datasource'}
                      ].concat((customPlaceholders||[]).map(it => ({ k: String(it?.name||'').trim(), l: `Custom: ${String(it?.name||'').trim()}` })).filter(it => /^\w+$/.test(it.k)))

                      const target = tokenDefs.some(t=>t.k===fmtTarget) ? fmtTarget : (tokenDefs[0]?.k || 'kpi')

                      const getTpl = () => (msgKind==='email' ? (template||'') : (templateSms||''))
                      const setTpl = (v: string) => { if (msgKind==='email') setTemplate(v); else setTemplateSms(v) }
                      const wrapRawSel = (before: string, after: string) => {
                        const el = rawAreaRef.current
                        const t = getTpl(); if (!el) { setTpl(before + t + after); return }
                        const s = el.selectionStart ?? 0; const e = el.selectionEnd ?? s
                        const sel = t.slice(s,e)
                        const next = t.slice(0,s) + before + (sel||'') + after + t.slice(e)
                        setTpl(next)
                        requestAnimationFrame(()=>{ try { el.focus(); const pos = s + before.length + (sel||'').length; el.selectionStart = el.selectionEnd = pos } catch {} })
                      }
                      const esc = (x: string) => x.replace(/[-/\\^$*+?.()|[\]{}]/g,'\\$&')
                      const wrapRawToken = (key: string, before: string, after: string) => {
                        const el = rawAreaRef.current
                        let t = getTpl(); const re = new RegExp(`\\{\\{\\s*${esc(String(key))}\\s*\\}\\}`)
                        if (el) {
                          const s = el.selectionStart ?? 0; const e = el.selectionEnd ?? s
                          const seg = t.slice(s,e)
                          if (seg && re.test(seg)) {
                            const repl = seg.replace(re, (m) => before + m + after)
                            setTpl(t.slice(0,s) + repl + t.slice(e)); return
                          }
                        }
                        const m = t.match(re); if (!m) return
                        const i = (m.index ?? 0); setTpl(t.slice(0,i) + before + m[0] + after + t.slice(i + m[0].length))
                      }
                      const bulletsRaw = () => {
                        const el = (rawAreaRef as any)?.current as HTMLTextAreaElement | null
                        const t = getTpl(); if (!el) { setTpl(`<ul><li>${t}</li></ul>`); return }
                        const s = el.selectionStart ?? 0; const e = el.selectionEnd ?? s
                        const seg = t.slice(s,e)
                        const content = seg || `{{${target}}}`
                        const items = content.split(/\n+/).map(x=>x.trim()).filter(Boolean)
                        const ul = `<ul>${items.map(it=>`<li>${it}</li>`).join('')}</ul>`
                        setTpl(t.slice(0,s) + ul + t.slice(e))
                        requestAnimationFrame(()=>{ try { el.focus() } catch {} })
                      }

                      return (
                        <div className="space-y-2">
                          <div className="flex flex-wrap items-center gap-2 text-xs">
                            <select className="h-7 px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)]" value={target} onChange={(e)=> setFmtTarget(e.target.value)}>
                              {tokenDefs.map(t => (<option key={t.k} value={t.k}>{t.l}</option>))}
                            </select>
                            <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden divide-x divide-[hsl(var(--border))]">
                              <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target, '<span style=\"display:inline-block; padding:4px;\">','</span>')}>Normal</button>
                              <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target, '<table style=\"width:100%; border-collapse:collapse;\"><tbody><tr><td>','</td></tr></tbody></table>')}>Table</button>
                            </div>
                            <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden divide-x divide-[hsl(var(--border))]">
                              <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target,'<span style=\"font-weight: normal; font-style: normal; text-decoration: none;\">','</span>')}>N</button>
                              <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target,'<b>','</b>')}>B</button>
                              <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target,'<i>','</i>')}>I</button>
                              <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target,'<u>','</u>')}>U</button>
                            </div>
                            <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center">
                              <select className="h-7 px-2 bg-[hsl(var(--secondary)/0.6)]" value={fmtSize} onChange={(e)=> setFmtSize(e.target.value)}>
                                <option value="12">12</option>
                                <option value="14">14</option>
                                <option value="16">16</option>
                                <option value="18">18</option>
                                <option value="20">20</option>
                                <option value="24">24</option>
                              </select>
                              <button type="button" className="px-2 py-1 border-l border-[hsl(var(--border))]" onClick={()=> wrapRawToken(target, `<span style=\"font-size: ${fmtSize}px;\">`, '</span>')}>Size</button>
                            </div>
                            <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center">
                              <input type="color" className="h-7 w-7 border-0 bg-transparent" value={fmtFontColor} onChange={(e)=> setFmtFontColor(e.target.value)} />
                              <button type="button" className="px-2 py-1 border-l border-[hsl(var(--border))]" onClick={()=> wrapRawToken(target, `<span style=\"color: ${fmtFontColor};\">`, '</span>')}>Font</button>
                            </div>
                            <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center">
                              <input type="color" className="h-7 w-7 border-0 bg-transparent" value={fmtBgColor} onChange={(e)=> setFmtBgColor(e.target.value)} />
                              <button type="button" className="px-2 py-1 border-l border-[hsl(var(--border))]" onClick={()=> wrapRawToken(target, `<span style=\"background-color: ${fmtBgColor};\">`, '</span>')}>Bg</button>
                            </div>
                            <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center gap-1 px-1">
                              <input type="color" className="h-7 w-7 border-0 bg-transparent" value={fmtBorderColor} onChange={(e)=> setFmtBorderColor(e.target.value)} />
                              <span className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden divide-x divide-[hsl(var(--border))]">
                                <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target, `<span style=\"display:inline-block; border: 1px solid ${fmtBorderColor}; padding: 4px; border-radius: 6px;\">`, '</span>')}>Solid</button>
                                <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target, `<span style=\"display:inline-block; border: 1px dashed ${fmtBorderColor}; padding: 4px; border-radius: 6px;\">`, '</span>')}>Dashed</button>
                                <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target, `<span style=\"display:inline-block; border: 1px dotted ${fmtBorderColor}; padding: 4px; border-radius: 6px;\">`, '</span>')}>Dotted</button>
                                <button type="button" className="px-2 py-1" onClick={()=> wrapRawToken(target, `<span style=\"display:inline-block; border: none;\">`, '</span>')}>None</button>
                              </span>
                            </div>
                            <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden items-center gap-1 px-2">
                              <span className="text-[11px]">Spacing</span>
                              <select className="h-7 px-1 bg-[hsl(var(--secondary)/0.6)]" value={String(fmtMarginTop)} onChange={(e)=> setFmtMarginTop(parseInt(e.target.value)||0)}>
                                {[0,4,8,12,16,24].map(v=> (<option key={v} value={v}>{`Top ${v}`}</option>))}
                              </select>
                              <select className="h-7 px-1 bg-[hsl(var(--secondary)/0.6)]" value={String(fmtMarginBottom)} onChange={(e)=> setFmtMarginBottom(parseInt(e.target.value)||0)}>
                                {[0,4,8,12,16,24].map(v=> (<option key={v} value={v}>{`Bottom ${v}`}</option>))}
                              </select>
                              <button type="button" className="px-2 py-1 rounded-md border border-[hsl(var(--border))]" onClick={()=> wrapRawToken(target, `<span style=\"display:inline-block; margin-top: ${fmtMarginTop}px; margin-bottom: ${fmtMarginBottom}px;\">`, '</span>')}>Apply</button>
                            </div>
                            <button type="button" className="px-2 py-1 rounded-md border border-[hsl(var(--border))]" onClick={bulletsRaw}>•</button>
                          </div>

                          {msgKind==='email' ? (
                            <textarea ref={rawAreaRef} className="w-full h-24 px-2 py-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)] text-sm" value={template} onChange={(e)=>setTemplate(e.target.value)} />
                          ) : (
                            <textarea ref={rawAreaRef} className="w-full h-24 px-2 py-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)] text-sm" value={templateSms} onChange={(e)=>setTemplateSms(e.target.value)} />
                          )}
                        </div>
                      )
                    }
                    const rendered = fill(msgKind==='email' ? (template || '') : (templateSms || ''))
                    return (
                      <div className="rounded-md border bg-[hsl(var(--card))] p-3 text-sm">
                        {msgKind==='email' ? (
                          <div dangerouslySetInnerHTML={{ __html: rendered.replace(/\n/g,'<br/>') }} />
                        ) : (
                          <pre className="whitespace-pre-wrap font-sans">{rendered}</pre>
                        )}
                      </div>
                    )
                  })()}
                </div>
              </div>
            )}

        {uiSection === 'trigger' && (
        <div className="mt-4 space-y-3">
          <div className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-2">
            <div className="flex items-center gap-2">
              <div className="text-sm font-medium">Trigger</div>
            </div>
            <div className="mt-2 flex flex-wrap items-center gap-2 text-xs">
              <label className="inline-flex items-center gap-2"><input type="checkbox" className="h-3 w-3 accent-[hsl(var(--primary))]" checked={triggerTimeEnabled} onChange={(e)=> setTriggerTimeEnabled(e.target.checked)} /> Time condition</label>
              <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden divide-x divide-[hsl(var(--border))]">
                <button type="button" className={`px-2 py-1 text-xs ${triggerLogic==='AND'?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setTriggerLogic('AND')}>AND</button>
                <button type="button" className={`px-2 py-1 text-xs ${triggerLogic==='OR'?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setTriggerLogic('OR')}>OR</button>
              </div>
              <label className="inline-flex items-center gap-2"><input type="checkbox" className="h-3 w-3 accent-[hsl(var(--primary))]" checked={triggerThresholdEnabled} onChange={(e)=> setTriggerThresholdEnabled(e.target.checked)} /> Threshold condition</label>
            </div>
          </div>

          {triggerThresholdEnabled && (
            <div className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-2">
              <div className="text-sm font-medium mb-2">Threshold condition</div>
              <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
                {!advOpen ? (
                  <>
                    <label className="text-sm">Aggregator<select className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" value={aggSel} onChange={(e)=> setAggSel(e.target.value)}>{['count','sum','avg','min','max','distinct'].map((a)=> (<option key={a} value={a}>{a}</option>))}</select></label>
                    <label className="text-sm">Measure / Value<select className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" value={measureSel} onChange={(e)=> setMeasureSel(e.target.value)} disabled={aggSel==='count'}>
                      {measureOptions.map(o => (<option key={o.value} value={o.value}>{o.label}</option>))}
                    </select></label>
                  </>
                ) : (
                  <div className="text-xs col-span-2 self-end text-muted-foreground">KPI is defined in Advanced builder</div>
                )}
                <label className="text-sm">Operator<select className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" value={operator} onChange={(e)=>setOperator(e.target.value)}><option value=">">&gt;</option><option value=">=">&gt;=</option><option value="<">&lt;</option><option value="<=">&lt;=</option><option value="==">==</option><option value="between">between (enter A,B)</option></select></label>
                <label className="text-sm">Value<input className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" placeholder={operator==='between'? 'A,B' : 'number'} value={value} onChange={(e)=>setValue(e.target.value)} /></label>
              </div>
              {(advOpen && (advPivot as any)?.legend) ? (
                <div className="mt-3">
                  <label className="text-sm block">Category filter
                    <input
                      className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]"
                      placeholder={`Enter ${String((advPivot as any)?.legend)} value (optional)`}
                      value={advXValue}
                      onChange={(e)=> {
                        const v = e.target.value
                        setAdvXValue(v)
                        try {
                          const legendField = String((advPivot as any)?.legend || '')
                          if (!legendField) return
                          const cur = advWhere.trim() ? JSON.parse(advWhere) : {}
                          const next = { ...cur }
                          if (String(v).trim() === '') delete next[legendField]
                          else next[legendField] = [v]
                          setAdvWhere(JSON.stringify(next))
                        } catch {}
                      }}
                    />
                  </label>
                </div>
              ) : null}
            </div>
          )}

          {triggerTimeEnabled && (
            <div className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-2">
              <div className="text-sm font-medium mb-2">Time condition</div>
              <div className="text-sm">
                <div className="mb-2">Schedule</div>
                <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden divide-x divide-[hsl(var(--border))]">
                  <button type="button" className={`px-2 py-1 text-xs ${scheduleKind==='hourly'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setScheduleKind('hourly')}>Hourly</button>
                  <button type="button" className={`px-2 py-1 text-xs ${scheduleKind==='weekly'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setScheduleKind('weekly')}>Weekly</button>
                  <button type="button" className={`px-2 py-1 text-xs ${scheduleKind==='monthly'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setScheduleKind('monthly')}>Monthly</button>
                </div>

                {scheduleKind === 'hourly' && (
                  <div className="mt-3 grid grid-cols-1 md:grid-cols-3 gap-3 items-end">
                    <label className="text-sm md:col-span-1">Every N hours
                      <input type="number" min={1} max={24} className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)] text-[12px]" value={everyHours} onChange={(e)=> setEveryHours(Math.max(1, Math.min(24, Number(e.target.value||1))))} />
                    </label>
                    <div className="text-xs text-muted-foreground md:col-span-2">Runs at the top of the hour, every N hours.</div>
                  </div>
                )}

                {scheduleKind === 'weekly' && (
                  <div className="mt-3 space-y-3">
                    <div>
                      <div className="text-xs mb-1">Time (HH:mm)</div>
                      <input className="w-full md:w-[200px] h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)] text-[12px]" placeholder="09:00" value={timeOfDay} onChange={(e)=>setTimeOfDay(e.target.value)} />
                    </div>
                    <div>
                      <div className="text-xs mb-1">Days of week</div>
                      <div className="inline-flex rounded-md border border-[hsl(var(--border))] overflow-hidden divide-x divide-[hsl(var(--border))]">
                        {[{v:0,l:'Sun'},{v:1,l:'Mon'},{v:2,l:'Tue'},{v:3,l:'Wed'},{v:4,l:'Thu'},{v:5,l:'Fri'},{v:6,l:'Sat'}].map(d => (
                          <button key={d.v} type="button" className={`px-2 py-1 text-xs ${daysOfWeek.includes(d.v)?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setDaysOfWeek((prev)=> prev.includes(d.v) ? prev.filter(x=>x!==d.v) : [...prev, d.v].sort((a,b)=>a-b))}>{d.l}</button>
                        ))}
                      </div>
                      <div className="mt-2 text-[11px] text-muted-foreground">Selected: {(() => { const labels=['Sun','Mon','Tue','Wed','Thu','Fri','Sat']; return daysOfWeek.length ? daysOfWeek.map(i=>labels[i]).join(', ') : 'None' })()}</div>
                    </div>
                  </div>
                )}

                {scheduleKind === 'monthly' && (
                  <div className="mt-3 space-y-3">
                    <div>
                      <div className="text-xs mb-1">Time (HH:mm)</div>
                      <input className="w-full md:w-[200px] h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)] text-[12px]" placeholder="09:00" value={timeOfDay} onChange={(e)=>setTimeOfDay(e.target.value)} />
                    </div>
                    <div>
                      <div className="text-xs mb-1">Days of month</div>
                      <div className="grid grid-cols-7 gap-1 text-xs">
                        {Array.from({ length: 31 }).map((_,i)=> i+1).map((d)=> (
                          <button key={d} type="button" className={`px-2 py-1 rounded-md border border-[hsl(var(--border))] ${daysOfMonth.includes(d)?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setDaysOfMonth((prev)=> prev.includes(d) ? prev.filter(x=>x!==d) : [...prev, d].sort((a,b)=>a-b))}>{d}</button>
                        ))}
                      </div>
                      <div className="mt-2 text-[11px] text-muted-foreground">Selected: {daysOfMonth.length ? daysOfMonth.join(', ') : 'None'}</div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          <div className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-2">
            <div className="flex items-center justify-between mb-2">
              <div className="text-sm font-medium">Advanced (define/override KPI)</div>
              <label className="text-xs inline-flex items-center gap-2"><input type="checkbox" className="h-3 w-3 accent-[hsl(var(--primary))]" checked={advOpen} onChange={(e)=>setAdvOpen(e.target.checked)} /> Enable</label>
            </div>
            {advOpen && (
              <div className="space-y-3">
                <div className="grid grid-cols-1 md:grid-cols-4 gap-3 text-sm">
                  <label>Datasource<select className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" value={advDatasourceId} onChange={(e)=>setAdvDatasourceId(e.target.value)}><option value="">Select datasource</option>{dsList.map(ds => (<option key={ds.id} value={ds.id}>{ds.name}</option>))}</select></label>
                  <label>Table / Source {advTablesLoading && (
                    <span className="inline-flex items-center gap-1 text-[11px] ml-2 align-middle">
                      <span className="h-3 w-3 border border-[hsl(var(--border))] border-l-transparent rounded-full animate-spin" aria-hidden="true"></span>
                      Loading…
                    </span>
                  )}
                    <select className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" value={advSource} onChange={(e)=>setAdvSource(e.target.value)} disabled={!advDatasourceId || advTablesLoading} aria-busy={advTablesLoading}>
                      <option value="">{advDatasourceId ? (advTablesLoading ? 'Loading tables…' : (tableList.length>0 ? 'Select table' : 'No tables found or access denied')) : 'Select datasource first'}</option>
                      {tableList.map(t => (<option key={t} value={t}>{t}</option>))}
                    </select>
                  </label>
                  <label>X pick
                    <select className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" value={advXPick} onChange={(e)=> setAdvXPick(e.target.value as any)}>
                      <option value="custom">Custom value</option>
                      <option value="range">Range (from/to)</option>
                      <option value="today">Today</option>
                      <option value="yesterday">Yesterday</option>
                      <option value="this_month">This Month</option>
                      <option value="last">Last</option>
                      <option value="min">Min</option>
                      <option value="max">Max</option>
                    </select>
                  </label>
                  <label>Calculation
                    <select className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" value={advCalcMode} onChange={(e)=> setAdvCalcMode((e.target.value==='pivot'?'pivot':'query'))}>
                      <option value="query">Query</option>
                      <option value="pivot">PivotQuery</option>
                    </select>
                  </label>
                </div>
                {advXPick === 'custom' && (
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
                    <label>X Value<input className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" placeholder="value to target (optional)" value={advXValue} onChange={(e)=>setAdvXValue(e.target.value)} /></label>
                  </div>
                )}
                {advXPick === 'range' && (
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
                    <label>From<input className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" placeholder="from" value={advXFrom} onChange={(e)=>setAdvXFrom(e.target.value)} /></label>
                    <label>To<input className="mt-1 w-full h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]" placeholder="to" value={advXTo} onChange={(e)=>setAdvXTo(e.target.value)} /></label>
                  </div>
                )}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <div className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-2">
                    <div className="text-xs font-medium mb-2">Builder</div>
                    <PivotBuilder
                      fields={allFieldNames}
                      assignments={advPivot}
                      update={(p: PivotAssignments) => {
                        const removed = (advPivot.filters || []).filter((f) => !(p.filters || []).includes(f))
                        setAdvPivot(p)
                        try {
                          const cur = advWhere.trim() ? JSON.parse(advWhere) : {}
                          removed.forEach((f) => { delete (cur as any)[f]; delete (cur as any)[`${f}__gt`]; delete (cur as any)[`${f}__gte`]; delete (cur as any)[`${f}__lt`]; delete (cur as any)[`${f}__lte`]; delete (cur as any)[`${f}__ne`] })
                          setAdvWhere(JSON.stringify(cur))
                        } catch {}
                      }}
                      selectFieldAction={(kind, field) => { setDetailKind(kind); setDetailField(field) }}
                      selected={detailKind && detailField ? ({ kind: detailKind, id: detailField } as any) : undefined}
                      disableRows={false}
                      disableValues={false}
                      allowMultiLegend
                      allowMultiRows
                      numericFields={numericFields}
                      dateLikeFields={dateLikeFields}
                      datasourceId={advDatasourceId}
                      source={advSource}
                      valueRequired
                    />
                    <div className="text-[11px] text-muted-foreground mt-2">Pick one Value (field+agg), optional X, and Filters.</div>
                  </div>
                  <div className="space-y-2">
                    <div className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-2">
                      <div className="text-xs font-medium mb-2">Details</div>
                      {(() => {
                        try {
                          const whereObj = advWhere.trim() ? JSON.parse(advWhere) : {}
                          const onPatch = (patch: Record<string, any>) => { const next = { ...whereObj, ...patch }; const cleaned: Record<string, any> = {}; Object.keys(next).forEach((k)=>{ if (next[k] !== undefined) cleaned[k] = next[k] }); setAdvWhere(JSON.stringify(cleaned)) }
                          if (detailKind === 'filter' && detailField) {
                            if (numericFields.includes(detailField)) return <NumberFilterDetails field={detailField} where={whereObj} onPatch={onPatch} />
                            if (dateLikeFields.includes(detailField)) return <DateRangeDetails field={detailField} where={whereObj} onPatch={onPatch} />
                            return <ValuesFilterPicker field={detailField} datasourceId={advDatasourceId} source={advSource} where={whereObj} onApply={(vals)=> onPatch({ [detailField]: Array.isArray(vals)&&vals.length ? vals : undefined })} />
                          }
                          return <div className="text-xs text-muted-foreground">Select a Filter chip or X axis to edit.</div>
                        } catch { return <div className="text-xs text-muted-foreground">Select a Filter chip or X axis to edit.</div> }
                      })()}
                    </div>
                    <label className="block">Filters JSON (where)
                      <textarea className="mt-1 w-full h-28 px-2 py-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)] font-mono text-[12px]" placeholder='{"status":"paid"}' value={advWhere} onChange={(e)=>setAdvWhere(e.target.value)} />
                    </label>
                  </div>
                </div>
                <div className="text-xs text-muted-foreground">Advanced overrides will be used for evaluation, save, and sending.</div>
              </div>
            )}
          </div>
        </div>
        )}

        

        {uiSection === 'preview' && (
        <div className="mt-4">
          <div className="flex items-center justify-between mb-2">
            <div className="text-sm font-medium">Inline Test</div>
            <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onTestEvaluate} disabled={testEvaluating}>
              {testEvaluating ? (
                <span className="inline-flex items-center gap-1">
                  <span className="h-3 w-3 border border-[hsl(var(--border))] border-l-transparent rounded-full animate-spin" aria-hidden="true"></span>
                  <span>Testing…</span>
                </span>
              ) : (
                'Test evaluate'
              )}
            </button>
          </div>
          {advOpen && (
            <div className="mb-2 text-xs text-muted-foreground">
              <span className="mr-2">Local KPI (client):</span>
              {localKpiLoading ? <span>Loading…</span> : <span className="font-mono">{localKpi == null ? '—' : String(localKpi)}</span>}
            </div>
          )}
          {advOpen && perCatStats && (
            <div className="mb-2 text-xs text-muted-foreground">
              <span className="mr-2">Per-Legend matches:</span>
              {perCatLoading ? 'Loading…' : `${perCatStats.matches} / ${perCatStats.total}`}
            </div>
          )}
          {advOpen && !perCatLoading && perCatMatches.length > 0 && (
            <div className="mb-3 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)] p-1 max-h-[64px] overflow-auto">
              <div className="flex flex-wrap gap-1.5">
              {perCatMatches.map((m, i) => {
                const resolveX = () => {
                  if (m.x != null && m.x !== '') return String(m.x)
                  switch (advXPick) {
                    case 'custom': return advXValue || '—'
                    case 'today': { const d = new Date(); return ymd(d) }
                    case 'yesterday': { const d = new Date(); d.setDate(d.getDate()-1); return ymd(d) }
                    case 'this_month': { const d = new Date(); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}` }
                    case 'range': return `${advXFrom || '—'}..${advXTo || '—'}`
                    case 'last':
                    case 'min':
                    case 'max':
                      return advXPick
                    default: return advXValue || '—'
                  }
                }
                const xDisp = resolveX()
                const catDisp = (m.category == null || m.category === '') ? '—' : String(m.category)
                const valDisp = (typeof m.value === 'number' && isFinite(m.value)) ? m.value.toLocaleString() : String(m.value ?? '—')
                const onChipClick = () => {
                  // Clicking a chip narrows to that Legend (category) as Value to Compare
                  const legendField = (advPivot.legend as any)
                  const catVal = (m.category == null ? '' : String(m.category))
                  if (!legendField || !catVal) return
                  try {
                    const cur = advWhere.trim() ? JSON.parse(advWhere) : {}
                    const next = { ...cur, [legendField]: [catVal] }
                    setAdvWhere(JSON.stringify(next))
                  } catch {}
                  setUiSection('trigger')
                }
                return (
                  <button key={i} type="button" onClick={onChipClick} title="Click to fill X Value (custom)"
                    className="inline-flex items-center gap-2 px-2 py-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[11px] hover:bg-[hsl(var(--muted))] cursor-pointer">
                    <span className="opacity-70">X:</span>
                    <span className="font-mono">{xDisp}</span>
                    <span className="opacity-30">•</span>
                    <span className="opacity-70">Legend:</span>
                    <span className="font-mono">{catDisp}</span>
                    <span className="opacity-30">•</span>
                    <span className="opacity-70">Value:</span>
                    <span className="font-mono">{valDisp}</span>
                  </button>
                )
              })}
              </div>
            </div>
          )}
          {advOpen && !perCatLoading && perCatMatches.length > 0 && Array.isArray((advPivot.legend as any)) && ((advPivot.legend as any).length > 1) && (
            <div className="mb-3">
              <div className="text-xs font-medium mb-1">Grouped Preview (client)</div>
              {(() => {
                try {
                  const legendDims = Array.isArray((advPivot.legend as any)) ? ((advPivot.legend as any) as string[]) : []
                  if (legendDims.length < 2) return null
                  const groups = new Map<string, { sum: number; children: Map<string, number> }>()
                  for (const m of perCatMatches) {
                    const cat = String(m.category ?? '')
                    const parts = cat.split(' • ')
                    const parent = parts[0] || '—'
                    const child = parts.slice(1).filter(Boolean).join(' • ')
                    const g = groups.get(parent) || { sum: 0, children: new Map<string, number>() }
                    const v = (typeof m.value === 'number' && isFinite(m.value)) ? m.value : Number(m.value || 0)
                    g.sum += (isFinite(v) ? v : 0)
                    if (child) g.children.set(child, (g.children.get(child) || 0) + (isFinite(v) ? v : 0))
                    groups.set(parent, g)
                  }
                  const ordered = Array.from(groups.entries()).sort((a,b) => b[1].sum - a[1].sum)
                  return (
                    <div className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)] p-2 max-h-[220px] overflow-auto">
                      <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                      {ordered.map(([parent, data]) => (
                        <div key={parent} className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-2 text-xs">
                          <div className="flex items-center justify-between">
                            <div className="font-medium truncate" title={parent}>{parent}</div>
                            <div className="font-mono">{Math.round(data.sum).toLocaleString()}</div>
                          </div>
                          {data.children.size > 0 && (
                            <ul className="mt-1 space-y-0.5">
                              {Array.from(data.children.entries()).sort((a,b)=>b[1]-a[1]).slice(0, 8).map(([child, val]) => (
                                <li key={child} className="flex items-center justify-between">
                                  <span className="truncate" title={child}>{child}</span>
                                  <span className="font-mono">{Math.round(val).toLocaleString()}</span>
                                </li>
                              ))}
                            </ul>
                          )}
                        </div>
                      ))}
                      </div>
                    </div>
                  )
                } catch { return null }
              })()}
            </div>
          )}
          {(testEmailHtml || testSmsText || testHtml || testContext) && (
            <div className="mb-2 inline-flex rounded-md border overflow-hidden text-xs">
              <button type="button" className={`px-2 py-1 ${testActiveTab==='email'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setTestActiveTab('email')}>Email</button>
              <button type="button" className={`px-2 py-1 border-l ${testActiveTab==='sms'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setTestActiveTab('sms')}>SMS</button>
              <button type="button" className={`px-2 py-1 border-l ${testActiveTab==='context'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setTestActiveTab('context')}>Context</button>
              <button type="button" className={`px-2 py-1 border-l ${testActiveTab==='raw'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setTestActiveTab('raw')}>Raw</button>
            </div>
          )}
          {testActiveTab==='email' && (
            <div className="rounded-md border border-[hsl(var(--border))] bg-white overflow-auto" style={{ minHeight: 120 }}>
              {testEmailHtml
                ? <iframe title="alert-inline-email" className="w-full h-[260px]" srcDoc={testEmailHtml} />
                : <div className="p-2 text-xs text-muted-foreground">No HTML returned for Email.</div>}
            </div>
          )}
          {testActiveTab==='sms' && (
            <div className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-2 text-sm whitespace-pre-wrap min-h-[120px]">{testSmsText || '—'}</div>
          )}
          {testActiveTab==='context' && (
            <pre className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-2 text-xs overflow-auto min-h-[120px]">{JSON.stringify(testContext || {}, null, 2)}</pre>
          )}
          {testActiveTab==='raw' && (
            <div className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-2" style={{ minHeight: 120 }}>
              {(testHtml || testEmailHtml) ? (
                <div>
                  <div className="mb-1 flex items-center justify-between">
                    <div className="text-[11px] opacity-70">Raw HTML</div>
                    <button
                      type="button"
                      className="text-[11px] px-2 py-0.5 rounded border hover:bg-[hsl(var(--secondary)/0.6)]"
                      onClick={() => { try { navigator.clipboard.writeText(String(testHtml || testEmailHtml)) } catch {} }}
                    >Copy</button>
                  </div>
                  <textarea
                    readOnly
                    value={String(testHtml || testEmailHtml)}
                    className="w-full h-[260px] font-mono text-[11px] leading-[1.35] bg-transparent outline-none resize-none"
                  />
                </div>
              ) : (
                <div className="p-2 text-xs text-muted-foreground">No HTML available (server returned empty emailHtml).</div>
              )}
            </div>
          )}
          <div className="mt-2 text-xs text-muted-foreground">
            <label className="inline-flex items-center gap-2"><input type="checkbox" className="h-3 w-3 accent-[hsl(var(--primary))]" checked={showPayload} onChange={(e)=> setShowPayload(e.target.checked)} /> Show payload</label>
          </div>
          {showPayload && (
            <pre className="rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-2 text-xs overflow-auto max-h-[260px]">{JSON.stringify((lastPayload || buildPayload())?.config, null, 2)}</pre>
          )}
        </div>
        )}

        {uiSection === 'preview' && (carriedSummary || (mode==='edit' && runs.length>0)) && (
          <div className="mt-4 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-3">
            {carriedSummary && (
              <div className="mb-4">
                <div className="text-sm font-medium mb-2">Current KPI</div>
                <div className="text-xs rounded-md border border-[hsl(var(--border))] p-2">
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                    <div>Datasource: <span className="font-mono">{String((carriedSummary as any).datasourceId || '—')}</span></div>
                    <div>Widget: <span className="font-mono">{String((carriedSummary as any).widgetId || '—')}</span></div>
                    <div>Source: <span className="font-mono">{String((carriedSummary as any).source || '—')}</span></div>
                    <div>Aggregator: <span className="font-mono">{String((carriedSummary as any).aggregator || '—')}</span></div>
                    <div>Measure: <span className="font-mono">{String((carriedSummary as any).measure || '—')}</span></div>
                    <div>X Field: <span className="font-mono">{String((carriedSummary as any).xField || '—')}</span></div>
                    <div>X Value: <span className="font-mono">{String((carriedSummary as any).xValue ?? '—')}</span></div>
                    <div>Legend Field: <span className="font-mono">{String((carriedSummary as any).legendField || '—')}</span></div>
                    <div>Category: <span className="font-mono">{String((carriedSummary as any).category || '—')}</span></div>
                    <div className="md:col-span-3">Where: <span className="font-mono break-all">{(carriedSummary as any).where ? JSON.stringify((carriedSummary as any).where) : '—'}</span></div>
                  </div>
                </div>
              </div>
            )}
            {(mode==='edit' && runs.length>0) && (
              <div>
                <div className="text-sm font-medium mb-2">Runs</div>
                <div className="overflow-auto rounded-md border border-[hsl(var(--border))]">
                  <table className="min-w-full text-sm">
                    <thead className="bg-[hsl(var(--muted))]">
                      <tr>
                        <th className="text-left px-2 py-1 font-medium">Started</th>
                        <th className="text-left px-2 py-1 font-medium">Finished</th>
                        <th className="text-left px-2 py-1 font-medium">Status</th>
                        <th className="text-left px-2 py-1 font-medium">Message</th>
                      </tr>
                    </thead>
                    <tbody>
                      {runs.map((r) => (
                        <tr key={r.id} className="border-t border-[hsl(var(--border))]">
                          <td className="px-2 py-1">{r.startedAt ? new Date(r.startedAt).toLocaleString() : '—'}</td>
                          <td className="px-2 py-1">{r.finishedAt ? new Date(r.finishedAt).toLocaleString() : '—'}</td>
                          <td className="px-2 py-1">{r.status || '—'}</td>
                          <td className="px-2 py-1 truncate max-w-[360px]" title={r.message || ''}>{r.message || '—'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        )}

        <div className="mt-4 flex items-center gap-2">
          <button className="text-xs px-3 py-2 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onSave}>Save</button>
          <button className="text-xs px-3 py-2 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCloseAction}>Cancel</button>
        </div>
      </div>
    </div>
      </div>
    </div>,
    document.body
  )
}
