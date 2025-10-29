"use client"

import { useEffect, useRef, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'
import { RiArrowDownSLine, RiCalendar2Line } from '@remixicon/react'

export type FilterbarControlProps = {
  active?: string
  options: readonly string[]
  labels: Record<string, string>
  onChange: (value: string) => void
  className?: string
  disabled?: boolean
}

export function FilterbarRuleControl(props: {
  label: string
  kind: 'string'|'number'|'date'
  field: string
  where?: Record<string, any>
  onPatchAction: (patch: Record<string, any>) => void
  className?: string
  disabled?: boolean
  distinctCache?: Record<string, string[]>
  loadDistinctAction?: (field: string) => void
}) {
  const { label, kind, field, where, onPatchAction, className, disabled, distinctCache, loadDistinctAction } = props
  return (
    <FilterbarShell label={label} className={className}>
      {kind === 'number' && (
        <NumberRuleInline field={field} where={where} onPatchAction={onPatchAction} />
      )}
      {kind === 'date' && (
        <DateRuleInline field={field} where={where} onPatchAction={onPatchAction} />
      )}
      {kind === 'string' && (
        <StringRuleInline field={field} where={where} onPatchAction={onPatchAction} distinctCache={distinctCache} loadDistinctAction={loadDistinctAction} />
      )}
    </FilterbarShell>
  )
}

function NumberRuleInline({ field, where, onPatchAction }: { field: string; where?: Record<string, any>; onPatchAction: (patch: Record<string, any>) => void }) {
  type NumberOp = 'eq'|'ne'|'gt'|'gte'|'lt'|'lte'|'between'
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
  const [op, setOp] = useState<NumberOp>(initial.op)
  const [a, setA] = useState<number|''>(initial.a ?? '')
  const [b, setB] = useState<number|''>(initial.b ?? '')
  useEffect(() => {
    const patch: Record<string, any> = { [`${field}__gt`]: undefined, [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined, [field]: undefined, [`${field}__ne`] : undefined }
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
    onPatchAction(patch)
  }, [op, a, b])
  return (
    <div className="grid grid-cols-3 gap-2">
      <select className="col-span-3 sm:col-span-1 px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={op} onChange={(e)=>setOp(e.target.value as NumberOp)}>
        <option value="eq">Is equal to</option>
        <option value="ne">Is not equal to</option>
        <option value="gt">Is greater than</option>
        <option value="gte">Is greater or equal</option>
        <option value="lt">Is less than</option>
        <option value="lte">Is less than or equal</option>
        <option value="between">Is between</option>
      </select>
      {op !== 'between' ? (
        <input type="number" className="col-span-3 sm:col-span-2 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={a} onChange={(e)=>setA(e.target.value === '' ? '' : Number(e.target.value))} />
      ) : (
        <>
          <input type="number" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Min" value={a} onChange={(e)=>setA(e.target.value === '' ? '' : Number(e.target.value))} />
          <input type="number" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Max" value={b} onChange={(e)=>setB(e.target.value === '' ? '' : Number(e.target.value))} />
        </>
      )}
    </div>
  )
}

function StringRuleInline({ field, where, onPatchAction, distinctCache, loadDistinctAction }: { field: string; where?: Record<string, any>; onPatchAction: (patch: Record<string, any>) => void; distinctCache?: Record<string, string[]>; loadDistinctAction?: (field: string) => void }) {
  type StrOp = 'contains'|'not_contains'|'eq'|'ne'|'starts_with'|'ends_with'
  type Mode = 'criteria' | 'manual'
  const initialArr = Array.isArray((where as any)?.[field]) ? ((where as any)?.[field] as any[]).map((v) => String(v)) : []
  const [mode, setMode] = useState<Mode>(initialArr.length ? 'manual' : 'criteria')
  const [op, setOp] = useState<StrOp>('contains')
  const [val, setVal] = useState<string>('')
  const [sel, setSel] = useState<string[]>(initialArr)
  const [q, setQ] = useState<string>('')
  // Ensure distinct values are available when needed
  useEffect(() => {
    try {
      if (mode !== 'manual') return
      const arr = distinctCache?.[field]
      if ((!arr || arr.length === 0) && typeof loadDistinctAction === 'function') {
        loadDistinctAction(field)
      }
    } catch {}
  }, [mode, field, distinctCache?.[field]])
  useEffect(() => {
    const arr = Array.isArray((where as any)?.[field]) ? ((where as any)?.[field] as any[]).map((v) => String(v)) : []
    if (arr.length && JSON.stringify(arr) !== JSON.stringify(sel)) setSel(arr)
  }, [field, JSON.stringify((where as any)?.[field] || [])])
  useEffect(() => {
    if (mode !== 'criteria') return
    const patch: Record<string, any> = { [field]: undefined, [`${field}__contains`]: undefined, [`${field}__notcontains`]: undefined, [`${field}__startswith`]: undefined, [`${field}__endswith`]: undefined, [`${field}__ne`]: undefined }
    const v = String(val || '').trim()
    if (!v) { onPatchAction(patch); return }
    switch (op) {
      case 'eq': patch[field] = [v]; break
      case 'ne': patch[`${field}__ne`] = v; break
      case 'contains': patch[`${field}__contains`] = v; break
      case 'not_contains': patch[`${field}__notcontains`] = v; break
      case 'starts_with': patch[`${field}__startswith`] = v; break
      case 'ends_with': patch[`${field}__endswith`] = v; break
    }
    onPatchAction(patch)
  }, [mode, op, val])
  useEffect(() => {
    if (mode !== 'manual') return
    const patch: Record<string, any> = { [field]: undefined, [`${field}__contains`]: undefined, [`${field}__notcontains`]: undefined, [`${field}__startswith`]: undefined, [`${field}__endswith`]: undefined, [`${field}__ne`]: undefined }
    patch[field] = sel.length ? sel : undefined
    onPatchAction(patch)
  }, [mode, JSON.stringify(sel)])
  const opts = ((distinctCache?.[field] || []) as string[]).map((s) => String(s))
  const filtered = opts.filter((s) => s.toLowerCase().includes(q.toLowerCase()))
  // Auto-select filtered results by default when searching (matches Details behavior)
  useEffect(() => {
    if (mode !== 'manual') return
    if (!q) return
    if (filtered.length === 0) return
    setSel((prev) => {
      const set = new Set<string>(prev)
      filtered.forEach((v) => set.add(v))
      return Array.from(set.values())
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, q])
  const toggle = (v: string) => {
    const exists = sel.includes(v)
    setSel(exists ? sel.filter((x) => x !== v) : [...sel, v])
  }
  const selectResults = () => {
    const set = new Set<string>(sel)
    filtered.forEach((v) => set.add(v))
    setSel(Array.from(set.values()))
  }
  const clearAll = () => setSel([])
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='criteria'} onChange={()=> setMode('criteria')} /> Search</label>
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='manual'} onChange={()=> setMode('manual')} /> Manual</label>
      </div>
      {mode === 'criteria' ? (
        <div className="grid grid-cols-3 gap-2">
          <select className="col-span-3 sm:col-span-1 px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={op} onChange={(e)=>setOp(e.target.value as StrOp)}>
            <option value="contains">Contains</option>
            <option value="not_contains">Does not contain</option>
            <option value="eq">Is equal to</option>
            <option value="ne">Is not equal to</option>
            <option value="starts_with">Starts with</option>
            <option value="ends_with">Ends with</option>
          </select>
          <input className="col-span-3 sm:col-span-2 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Value" value={val} onChange={(e)=>setVal(e.target.value)} />
        </div>
      ) : (
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <input className="flex-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Search values" value={q} onChange={(e)=>setQ(e.target.value)} onKeyDown={(e)=>{ if (e.key==='Enter'){ e.preventDefault(); selectResults() } }} />
            <button className="px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-xs" onClick={selectResults}>Select results</button>
            <button className="px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-xs" onClick={clearAll}>Clear</button>
          </div>
          <div className="max-h-56 overflow-auto border rounded-md">
            <ul className="p-2 space-y-1">
              {filtered.map((v) => (
                <li key={v} className="flex items-center gap-2 text-xs">
                  <input type="checkbox" className="size-3" checked={sel.includes(v)} onChange={()=>toggle(v)} />
                  <span className="truncate" title={v}>{v}</span>
                </li>
              ))}
              {filtered.length === 0 && (
                <li className="text-xs text-muted-foreground">No values</li>
              )}
            </ul>
          </div>
        </div>
      )}
    </div>
  )
}

function DateRuleInline({ field, where, onPatchAction }: { field: string; where?: Record<string, any>; onPatchAction: (patch: Record<string, any>) => void }) {
  type Mode = 'preset'|'custom'
  type Preset = 'today'|'yesterday'|'this_month'|'last_month'|'this_quarter'|'last_quarter'|'this_year'|'last_year'
  type CustomOp = 'after'|'before'|'between'
  const storageKey = `frc-date:${field}`
  const [mode, setMode] = useState<Mode>('preset')
  const [preset, setPreset] = useState<Preset>('today')
  const [op, setOp] = useState<CustomOp>('between')
  const [a, setA] = useState<string>('')
  const [b, setB] = useState<string>('')
  const interactedRef = useRef(false)
  const editingRef = useRef(false)
  const editTimerRef = useRef<number | null>(null)
  const markEditing = () => {
    editingRef.current = true
    if (typeof window !== 'undefined') {
      if (editTimerRef.current) window.clearTimeout(editTimerRef.current)
      editTimerRef.current = window.setTimeout(() => { editingRef.current = false }, 1200) as any
    }
  }
  // Hydrate UI state on field change (handles remounts or parent re-renders)
  useEffect(() => {
    try {
      // Prefer hydrating from incoming where if present
      const gte = (where as any)?.[`${field}__gte`] as string | undefined
      const lt = (where as any)?.[`${field}__lt`] as string | undefined
      if (gte || lt) {
        const presets: Preset[] = ['today','yesterday','this_month','last_month','this_quarter','last_quarter','this_year','last_year']
        const match = presets.find((p) => {
          const r = rangeForPreset(p)
          return (r.gte || undefined) === (gte || undefined) && (r.lt || undefined) === (lt || undefined)
        })
        if (match) { setMode('preset'); setPreset(match) }
        else {
          setMode('custom')
          if (gte) setA(gte)
          if (lt) { const d = new Date(`${lt}T00:00:00`); d.setDate(d.getDate()-1); setB(d.toISOString().slice(0,10)) }
        }
      } else {
        const raw = typeof window !== 'undefined' ? localStorage.getItem(storageKey) : null
        if (raw) {
          const st = JSON.parse(raw) as { mode?: Mode; preset?: Preset; op?: CustomOp; a?: string; b?: string }
          if (st.mode) setMode(st.mode)
          if (st.preset) setPreset(st.preset)
          if (st.op) setOp(st.op)
          if (typeof st.a === 'string') setA(st.a)
          if (typeof st.b === 'string') setB(st.b)
        }
      }
    } catch {}
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [field])
  // Persist UI state only after user has interacted (avoid persisting defaults that can override where)
  useEffect(() => {
    if (!interactedRef.current) return
    try { if (typeof window !== 'undefined') localStorage.setItem(storageKey, JSON.stringify({ mode, preset, op, a, b })) } catch {}
  }, [storageKey, mode, preset, op, a, b])
  function ymd(d: Date) { return d.toISOString().slice(0,10) }
  function rangeForPreset(p: Preset): { gte?: string; lt?: string } {
    const now = new Date()
    const startOfMonth = (d: Date) => new Date(d.getFullYear(), d.getMonth(), 1)
    const endOfMonth = (d: Date) => new Date(d.getFullYear(), d.getMonth()+1, 1)
    const quarter = Math.floor(now.getMonth()/3)
    const startOfQuarter = (y: number, q: number) => new Date(y, q*3, 1)
    const endOfQuarter = (y: number, q: number) => new Date(y, q*3 + 3, 1)
    const startOfYear = (d: Date) => new Date(d.getFullYear(), 0, 1)
    const endOfYear = (d: Date) => new Date(d.getFullYear()+1, 0, 1)
    switch (p) {
      case 'today': { const s = new Date(now.getFullYear(), now.getMonth(), now.getDate()); const e = new Date(s); e.setDate(e.getDate()+1); return { gte: ymd(s), lt: ymd(e) } }
      case 'yesterday': { const e = new Date(now.getFullYear(), now.getMonth(), now.getDate()); const s = new Date(e); s.setDate(s.getDate()-1); return { gte: ymd(s), lt: ymd(e) } }
      case 'this_month': return { gte: ymd(startOfMonth(now)), lt: ymd(endOfMonth(now)) }
      case 'last_month': { const s = startOfMonth(now); s.setMonth(s.getMonth()-1); const e = new Date(s.getFullYear(), s.getMonth()+1, 1); return { gte: ymd(s), lt: ymd(e) } }
      case 'this_quarter': return { gte: ymd(startOfQuarter(now.getFullYear(), quarter)), lt: ymd(endOfQuarter(now.getFullYear(), quarter)) }
      case 'last_quarter': { const q = (quarter+3-1)%4; const yr = quarter===0 ? now.getFullYear()-1 : now.getFullYear(); return { gte: ymd(startOfQuarter(yr, q)), lt: ymd(endOfQuarter(yr, q)) } }
      case 'this_year': return { gte: ymd(startOfYear(now)), lt: ymd(endOfYear(now)) }
      case 'last_year': { const s = new Date(now.getFullYear()-1, 0, 1); const e = new Date(now.getFullYear(), 0, 1); return { gte: ymd(s), lt: ymd(e) } }
    }
  }
  // Reflect external where->UI to prevent defaulting back to Today
  useEffect(() => {
    try {
      const gte = (where as any)?.[`${field}__gte`] as string | undefined
      const lt = (where as any)?.[`${field}__lt`] as string | undefined
      if (editingRef.current) return
      if (!gte && !lt) return
      const presets: Preset[] = ['today','yesterday','this_month','last_month','this_quarter','last_quarter','this_year','last_year']
      const match = presets.find((p) => {
        const r = rangeForPreset(p)
        return (r.gte || undefined) === (gte || undefined) && (r.lt || undefined) === (lt || undefined)
      })
      if (match) {
        if (mode !== 'preset') setMode('preset')
        if (preset !== match) setPreset(match)
        return
      }
      if (mode !== 'custom') setMode('custom')
      const hasG = !!gte
      const hasL = !!lt
      if (hasG && hasL) {
        if (op !== 'between') setOp('between')
        if (a !== (gte || '')) setA(gte || '')
        try { if (lt) { const d = new Date(`${lt}T00:00:00`); d.setDate(d.getDate()-1); const prev = d.toISOString().slice(0,10); if (b !== prev) setB(prev) } } catch {}
      } else if (hasG) {
        if (op !== 'after') setOp('after')
        if (a !== (gte || '')) setA(gte || '')
        if (b !== '') setB('')
      } else if (hasL) {
        if (op !== 'before') setOp('before')
        try { if (lt) { const d = new Date(`${lt}T00:00:00`); d.setDate(d.getDate()-1); const prev = d.toISOString().slice(0,10); if (b !== prev) setB(prev) } } catch {}
        if (a !== '') setA('')
      }
    } catch {}
  }, [field, (where as any)?.[`${field}__gte`], (where as any)?.[`${field}__lt`]])
  const lastSigRef = useRef<string>('')
  useEffect(() => {
    if (!interactedRef.current) return
    const patch: Record<string, any> = { [`${field}__gte`]: undefined, [`${field}__lt`]: undefined }
    if (mode === 'preset') {
      const r = rangeForPreset(preset)
      patch[`${field}__gte`] = r.gte
      patch[`${field}__lt`] = r.lt
      const sig = JSON.stringify(patch)
      if (sig !== lastSigRef.current) { lastSigRef.current = sig; onPatchAction(patch) }
      return
    }
    if (op === 'after') {
      patch[`${field}__gte`] = a || undefined
    } else if (op === 'before') {
      if (b) { const d = new Date(`${b}T00:00:00`); d.setDate(d.getDate()+1); patch[`${field}__lt`] = ymd(d) }
    } else if (op === 'between') {
      patch[`${field}__gte`] = a || undefined
      if (b) { const d = new Date(`${b}T00:00:00`); d.setDate(d.getDate()+1); patch[`${field}__lt`] = ymd(d) }
    }
    const sig = JSON.stringify(patch)
    if (sig !== lastSigRef.current) { lastSigRef.current = sig; onPatchAction(patch) }
  }, [mode, preset, op, a, b])
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='preset'} onChange={()=>{ interactedRef.current = true; markEditing(); setMode('preset') }} /> Preset</label>
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='custom'} onChange={()=>{ interactedRef.current = true; markEditing(); setMode('custom') }} /> Custom</label>
      </div>
      {mode==='preset' ? (
        <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={preset} onChange={(e)=>{ interactedRef.current = true; markEditing(); setPreset(e.target.value as Preset) }}>
          <option value="today">Today</option>
          <option value="yesterday">Yesterday</option>
          <option value="this_month">This Month</option>
          <option value="last_month">Last Month</option>
          <option value="this_quarter">This Quarter</option>
          <option value="last_quarter">Last Quarter</option>
          <option value="this_year">This Year</option>
          <option value="last_year">Last Year</option>
        </select>
      ) : (
        <div className="grid grid-cols-3 gap-2">
          <select className="col-span-3 sm:col-span-1 px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={op} onChange={(e)=>{ interactedRef.current = true; markEditing(); setOp(e.target.value as CustomOp) }}>
            <option value="after">After</option>
            <option value="before">Before</option>
            <option value="between">Between</option>
          </select>
          {op!=='between' ? (
            <input type="date" className="col-span-3 sm:col-span-2 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={op==='after'?a:b} onChange={(e)=> { interactedRef.current = true; markEditing(); (op==='after'? setA(e.target.value) : setB(e.target.value)) }} />
          ) : (
            <>
              <input type="date" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Start" value={a} onChange={(e)=>{ interactedRef.current = true; markEditing(); setA(e.target.value) }} />
              <input type="date" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="End" value={b} onChange={(e)=>{ interactedRef.current = true; markEditing(); setB(e.target.value) }} />
            </>
          )}
        </div>
      )}
    </div>
  )
}

export function FilterbarShell(props: { label: string; icon?: ReactNode; className?: string; children: ReactNode }) {
  const { label, icon, className, children } = props
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [menuWidth, setMenuWidth] = useState<number | undefined>(undefined)
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null)
  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (!open) return
      const el = containerRef.current
      const tgt = e.target as Node
      if (el && (tgt === el || el.contains(tgt))) return
      if (e.target instanceof Element && (e.target as Element).closest('.filterbar-popover')) return
      setOpen(false)
    }
    document.addEventListener('mousedown', onDocClick)
    return () => document.removeEventListener('mousedown', onDocClick)
  }, [open])
  useEffect(() => {
    if (open && containerRef.current) {
      const { width, height, top, left } = containerRef.current.getBoundingClientRect()
      setMenuWidth(width)
      setPos({ top: top + height + window.scrollY + 4, left: left + window.scrollX })
    }
  }, [open])
  return (
    <div ref={containerRef} className={`group relative z-10 inline-flex items-center rounded-tremor-small text-[12px] leading-none font-medium shadow-tremor-input ${className||''}`}>
      <span className="inline-flex items-center h-8 rounded-l-tremor-small border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-2 text-[hsl(var(--muted-foreground))] transition-colors group-hover:bg-[hsl(var(--muted))] focus:z-10 cursor-pointer">
        {icon || <RiCalendar2Line className="size-4 shrink-0 text-[hsl(var(--muted-foreground))]" aria-hidden />}
      </span>
      <button
        type="button"
        className="-ml-px inline-flex items-center gap-x-2 h-8 rounded-r-tremor-small border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-3 text-[hsl(var(--foreground))] transition-colors hover:bg-[hsl(var(--muted))] group-hover:bg-[hsl(var(--muted))] focus:z-10 focus:outline-none focus:ring-2 focus:ring-[hsl(var(--ring))] focus:border-[hsl(var(--ring))] cursor-pointer"
        aria-expanded={open}
        aria-haspopup="menu"
        onClick={() => setOpen(v => !v)}
      >
        {label}
        <RiArrowDownSLine className="-mr-1 size-4 shrink-0" aria-hidden />
      </button>
      {open && pos && typeof window !== 'undefined' && createPortal(
        <div
          className="filterbar-popover z-[80] rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--popover))] text-[12px] shadow-none p-2"
          style={{ position: 'absolute', top: pos.top, left: pos.left, width: menuWidth ? `${Math.max(menuWidth, 300)}px` : '300px', maxWidth: '480px' }}
          role="menu"
        >
          {children}
        </div>,
        document.body
      )}
    </div>
  )
}

export default function FilterbarControl(props: any) {
  const { active, options, labels, onChange, className, disabled } = props as FilterbarControlProps
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [menuWidth, setMenuWidth] = useState<number | undefined>(undefined)
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null)
  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (!open) return
      const el = containerRef.current
      const tgt = e.target as Node
      if (el && (tgt === el || el.contains(tgt))) return
      if (e.target instanceof Element && (e.target as Element).closest('.filterbar-popover')) return
      setOpen(false)
    }
    document.addEventListener('mousedown', onDocClick)
    return () => document.removeEventListener('mousedown', onDocClick)
  }, [open])
  useEffect(() => {
    if (open && containerRef.current) {
      const { width, height, top, left } = containerRef.current.getBoundingClientRect()
      setMenuWidth(width)
      setPos({ top: top + height + window.scrollY + 4, left: left + window.scrollX })
    }
  }, [open])
  const activeLabel = (active && labels[active]) ? labels[active] : Object.values(labels)[0] || 'Select'
  return (
    <div ref={containerRef} className={`group relative z-10 inline-flex items-center rounded-tremor-small text-[12px] leading-none font-medium shadow-tremor-input dark:shadow-dark-tremor-input ${className||''} ${disabled?'opacity-60 cursor-not-allowed':''}`}>
      <span className="inline-flex items-center h-8 rounded-l-tremor-small border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-2 text-[hsl(var(--muted-foreground))] transition-colors ${disabled?'':'group-hover:bg-[hsl(var(--muted))]'} focus:z-10 ${disabled?'cursor-not-allowed':'cursor-pointer'}">
        <RiCalendar2Line className="size-4 shrink-0 text-[hsl(var(--muted-foreground))]" aria-hidden />
      </span>
      <button
        type="button"
        className="-ml-px inline-flex items-center gap-x-2 h-8 rounded-r-tremor-small border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-3 text-[hsl(var(--foreground))] transition-colors ${disabled?'':'hover:bg-[hsl(var(--muted))] group-hover:bg-[hsl(var(--muted))]'} focus:z-10 focus:outline-none focus:ring-2 focus:ring-[hsl(var(--ring))] focus:border-[hsl(var(--ring))] ${disabled?'cursor-not-allowed':'cursor-pointer'}"
        aria-expanded={open}
        aria-haspopup="menu"
        onClick={() => { if (disabled) return; setOpen(v => !v) }}
        disabled={!!disabled}
        aria-disabled={!!disabled}
      >
        {activeLabel}
        <RiArrowDownSLine className="-mr-1 size-4 shrink-0" aria-hidden />
      </button>
      {open && !disabled && pos && typeof window !== 'undefined' && createPortal(
        <div
          className="filterbar-popover z-[80] rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--popover))] text-[12px] shadow-none p-1 whitespace-normal break-words"
          style={{ position: 'absolute', top: pos.top, left: pos.left, width: menuWidth ? `${Math.max(menuWidth, 220)}px` : '240px', maxWidth: '320px' }}
          role="menu"
        >
          {options.map((opt) => (
            <button
              key={opt}
              className={`w-full text-left px-3 py-1.5 rounded-md mx-0.5 my-0.5 text-[hsl(var(--foreground))] transition-colors hover:bg-[hsl(var(--muted))] ${active===opt?'font-medium':''}`}
              role="menuitem"
              onClick={() => { if (disabled) return; onChange(opt); setOpen(false) }}
            >
              {labels[opt] ?? opt}
            </button>
          ))}
        </div>,
        document.body
      )}
    </div>
  )
}
