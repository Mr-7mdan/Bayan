"use client"

import React from 'react'
import { useTranslations } from 'next-intl'
import { QueryApi } from '@/lib/api'
import { ymd } from './state'

// Lightweight Details UIs adapted for Alerts dialog Advanced mode.
// Moved verbatim out of AlertDialog.tsx; no behavior change.
export function NumberFilterDetails({ field, where, onPatch }: { field: string; where?: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
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
  const t = useTranslations('comms')
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
        <div className="text-xs font-medium">{t('alertDialog.filters.valueFilter', { field })}</div>
        <div className="flex items-center gap-2">
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { setOp('eq'); setA(''); setB(''); onPatch({ [field]: undefined, [`${field}__gt`]: undefined, [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined }) }}>{t('alertDialog.filters.clear')}</button>
        </div>
      </div>
      <div className="grid grid-cols-3 gap-2 mt-2 items-center">
        <select className="col-span-3 sm:col-span-1 px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={op} onChange={(e) => setOp(e.target.value as NumberOp)}>
          <option value="eq">{t('alertDialog.filters.opEq')}</option>
          <option value="ne">{t('alertDialog.filters.opNe')}</option>
          <option value="gt">{t('alertDialog.filters.opGt')}</option>
          <option value="gte">{t('alertDialog.filters.opGte')}</option>
          <option value="lt">{t('alertDialog.filters.opLt')}</option>
          <option value="lte">{t('alertDialog.filters.opLte')}</option>
          <option value="between">{t('alertDialog.filters.opBetween')}</option>
        </select>
        {op !== 'between' ? (
          <input type="number" className="col-span-3 sm:col-span-2 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={a} onChange={(e) => setA(e.target.value === '' ? '' : Number(e.target.value))} />
        ) : (
          <>
            <input type="number" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder={t('alertDialog.filters.min')} value={a} onChange={(e) => setA(e.target.value === '' ? '' : Number(e.target.value))} />
            <input type="number" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder={t('alertDialog.filters.max')} value={b} onChange={(e) => setB(e.target.value === '' ? '' : Number(e.target.value))} />
          </>
        )}
      </div>
    </div>
  )
}

export function DateRangeDetails({ field, where, onPatch }: { field: string; where?: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  const a0 = (where as any)?.[`${field}__gte`] as string | undefined
  const b0 = (where as any)?.[`${field}__lt`] as string | undefined
  const normalizeEnd = (b?: string) => {
    if (!b) return undefined
    const d = new Date(`${b}T00:00:00`); if (isNaN(d.getTime())) return undefined
    d.setDate(d.getDate() + 1)
    return ymd(d)
  }
  const t = useTranslations('comms')
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
        <div className="text-xs font-medium">{t('alertDialog.filters.dateRange', { field })}</div>
        <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { setStart(''); setEnd(''); onPatch({ [`${field}__gte`]: undefined, [`${field}__lt`]: undefined }) }}>{t('alertDialog.filters.clear')}</button>
      </div>
      <div className="grid grid-cols-2 gap-2 mt-2 items-center">
        <div className="flex flex-col gap-1">
          <label className="text-[11px] text-muted-foreground">{t('alertDialog.filters.start')}</label>
          <input type="date" className="h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={start} onChange={(e) => setStart(e.target.value)} />
        </div>
        <div className="flex flex-col gap-1">
          <label className="text-[11px] text-muted-foreground">{t('alertDialog.filters.end')}</label>
          <input type="date" className="h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={end} onChange={(e) => setEnd(e.target.value)} />
        </div>
      </div>
    </div>
  )
}

export const DISTINCT_CACHE = new Map<string, { ts: number; values: string[]; total: number }>()
export const DISTINCT_TTL_MS = 10 * 60 * 1000

export function ValuesFilterPicker({ field, datasourceId, source, where, onApply }: { field: string; datasourceId?: string; source?: string; where?: Record<string, any>; onApply: (vals: any[]) => void }) {
  const t = useTranslations('comms')
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
        <div className="text-xs font-medium">{t('alertDialog.filters.filterValues', { field })}</div>
        <div className="flex items-center gap-2">
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { setSelected([]); onApply([]) }}>{t('alertDialog.filters.clear')}</button>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => onApply(selected)}>{t('alertDialog.filters.apply')}</button>
        </div>
      </div>
      <div className="mt-2 flex items-center gap-2">
        <input className="w-full px-2 py-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.6)] text-xs" placeholder={t('alertDialog.filters.searchValues')} value={filterQuery} onChange={(e) => setFilterQuery(e.target.value)} />
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
          {!loading && samples.length === 0 && (<li className="text-xs text-muted-foreground">{t('alertDialog.filters.noValues')}</li>)}
        </ul>
      </div>
    </div>
  )
}
