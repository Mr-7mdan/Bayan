"use client"

import { useQuery } from '@tanstack/react-query'
import { useEffect, useMemo, useRef, useState } from 'react'
import { Api, QueryApi } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import { compileFormula, parseReferences } from '@/lib/formula'
import type { QueryResponse, QuerySpec } from '@/lib/api'
import { useFilters } from '@/components/providers/FiltersProvider'
import type { WidgetConfig } from '@/types/widgets'
import AgTable from '@/components/widgets/AgTable'
import ErrorBoundary from '@/components/dev/ErrorBoundary'
import dynamic from 'next/dynamic'
import { RiFileExcel2Fill } from '@remixicon/react'
import FilterbarControl, { FilterbarRuleControl } from '@/components/shared/FilterbarControl'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/Tabs'
import { normalizeCategoryLabel } from '@/lib/calcEngine'
const PivotMatrixView = dynamic<any>(() => import('@/components/widgets/PivotMatrixView'), { ssr: false })

function useDebounced<T>(val: T, delay = 350): T {
  const [v, setV] = useState<T>(val as T)
  useEffect(() => { const t = setTimeout(() => setV(val), delay); return () => { try { clearTimeout(t) } catch {} } }, [val, delay])
  return v
}

export default function TableCard({
  title,
  sql,
  datasourceId,
  pageSize = 10,
  queryMode = 'sql',
  querySpec,
  options,
  widgetId,
  customColumns,
  pivot,
  tabbedGuard,
  tabbedField,
}: {
  title: string
  sql: string
  datasourceId?: string
  pageSize?: number
  queryMode?: 'sql' | 'spec'
  querySpec?: QuerySpec
  options?: WidgetConfig['options']
  widgetId?: string
  customColumns?: WidgetConfig['customColumns']
  pivot?: WidgetConfig['pivot']
  tabbedGuard?: boolean
  tabbedField?: string
}) {
  const { filters } = useFilters()
  const { user } = useAuth()
  const [uiWhere, setUiWhere] = useState<Record<string, any>>({})
  const [breakSeq, setBreakSeq] = useState(0)
  const [page, setPage] = useState(0)
  const [size, setSize] = useState(pageSize)
  // Grid-level WHERE derived from AG Grid filters
  const [gridWhere, setGridWhere] = useState<Record<string, any>>({})

  // Viewport gating
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [visible, setVisible] = useState(false)
  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const obs = new IntersectionObserver((entries) => {
      const e = entries[0]
      setVisible(!!(e && (e.isIntersecting || e.intersectionRatio > 0)))
    }, { root: null, threshold: 0 })
    obs.observe(el)
    return () => { try { obs.disconnect() } catch {} }
  }, [])

  // Load time tracking (seconds) to match ChartCard spinner timer
  const [loadingSeconds, setLoadingSeconds] = useState<number>(0)
  const loadStartRef = useRef<number | null>(null)
  const loadingTimerRef = useRef<any>(null)

  const setUiWhereAndEmit = (patch: Record<string, any>) => {
    setUiWhere((prev) => {
      const next = { ...prev }
      Object.entries(patch).forEach(([k, v]) => {
        if (v === undefined) delete (next as any)[k]
        else (next as any)[k] = v
      })
      return next
    })
    if (typeof window !== 'undefined' && widgetId) {
      try { window.dispatchEvent(new CustomEvent('chart-where-change', { detail: { widgetId, patch } } as any)) } catch {}
    }
  }


  // Datasource defaults (sort/Top N) indicator for spec-mode
  const dsDefaultsQ = useQuery({
    queryKey: ['ds-transforms', datasourceId],
    queryFn: () => Api.getDatasourceTransforms(String(datasourceId)),
    enabled: !!datasourceId && queryMode === 'spec',
    staleTime: 5 * 60 * 1000,
  })
  const dsDefaultsText = useMemo(() => {
    try {
      const defs = (dsDefaultsQ.data as any)?.defaults
      if (!defs) return ''
      const parts: string[] = []
      const s = defs.sort
      if (s?.by) parts.push(`sort ${s.by} ${(String(s.direction||'desc')).toLowerCase()}`)
      const tn = defs.limitTopN
      if (tn?.n) parts.push(`top ${tn.n}${tn.by?` by ${tn.by} ${(String(tn.direction||'desc')).toLowerCase()}`:''}`)
      return parts.join(' · ')
    } catch { return '' }
  }, [dsDefaultsQ.data])

  // Load datasource transforms to resolve simple alias -> base mappings (e.g., alias "20" -> base "Category1")
  const dsTransformsQ = useQuery({
    queryKey: ['ds-transforms-alias', datasourceId],
    queryFn: () => Api.getDatasourceTransforms(String(datasourceId)),
    enabled: !!datasourceId,
    staleTime: 5 * 60 * 1000,
  })
  const aliasToBase = useMemo(() => {
    const m: Record<string, string> = {}
    try {
      const cols = ((dsTransformsQ.data as any)?.customColumns || []) as Array<{ name?: string; expr?: string }>
      cols.forEach((cc: any) => {
        const name = String(cc?.name || '').trim()
        const expr = String(cc?.expr || '').trim()
        if (!name || !expr) return
        // Match simple proxy expressions: [Base] | [s].[Base] | s.Base | "Base"
        const re = /^\s*(?:\[s\]\.\[([^\]]+)\]|\[([^\]]+)\]|s\.\"?([A-Za-z0-9_]+)\"?|\"([^\"]+)\")\s*$/
        const m1 = expr.match(re)
        const base = (m1?.[1] || m1?.[2] || m1?.[3] || m1?.[4] || '').trim()
        if (base && base !== name) m[name] = base
      })
    } catch {}
    return m
  }, [dsTransformsQ.data])

  // Respect global break-link per widget (disable applying global filters)
  const ignoreGlobal = useMemo(() => {
    try { return !!(widgetId && localStorage.getItem(`gf_break_${widgetId}`) === '1') } catch { return false }
  }, [widgetId])

  const shouldExpose = (f: string) => (
    (options?.filtersExpose && typeof options.filtersExpose[f] === 'boolean')
      ? !!options.filtersExpose[f]
      : ((options?.filtersUI === 'filterbars'))
  )
  const fieldsExposed = useMemo(() => {
    const candidate = new Set<string>()
    ;((pivot?.filters || []) as string[]).forEach((f) => { if (f) candidate.add(f) })
    const ex = (options?.filtersExpose || {})
    Object.keys(ex).forEach((k) => { if (ex[k]) candidate.add(k) })
    return Array.from(candidate).filter(shouldExpose)
  }, [options?.filtersUI, options?.filtersExpose, pivot])

  // Hydrate UI state from base where for exposed fields (one-time per field)
  useEffect(() => {
    const baseWhere = ((querySpec as any)?.where || {}) as Record<string, any>
    setUiWhere((prev) => {
      const next = { ...prev }
      fieldsExposed.forEach((f) => {
        if (next[f] === undefined && Array.isArray(baseWhere[f]) && (baseWhere[f] as any[]).length > 0) next[f] = baseWhere[f]
        ;['gte','lte','gt','lt'].forEach((op) => {
          const k = `${f}__${op}`
          if (next[k] === undefined && baseWhere[k] != null) (next as any)[k] = baseWhere[k]
        })
      })
      return next
    })
  }, [JSON.stringify((querySpec as any)?.where || {}), JSON.stringify(fieldsExposed)])

  // Compute effective WHERE: base where + optional delta date range, with UI as source of truth for exposed fields.
  // Important: Only apply gridWhere (AG Grid filters) for Data Table mode, not Pivot mode.
  const effectiveWhere: Record<string, any> | undefined = useMemo(() => {
    if (queryMode !== 'spec' || !querySpec) return undefined
    const base: Record<string, any> = { ...((querySpec as any)?.where || {}) }
    const df = (options as any)?.deltaDateField as string | undefined
    if (df && !ignoreGlobal) {
      if (filters.startDate) base[`${df}__gte`] = filters.startDate
      if (filters.endDate) {
        const d = new Date(`${filters.endDate}T00:00:00`)
        d.setDate(d.getDate() + 1)
        const y = d.getFullYear()
        const m = String(d.getMonth() + 1).padStart(2, '0')
        const da = String(d.getDate()).padStart(2, '0')
        base[`${df}__lt`] = `${y}-${m}-${da}`
      }
    }
    const eff: Record<string, any> = { ...base }
    const rmKeysFor = (f: string) => {
      // When rendering inside a tabbed context, preserve the tab filter so panes stay filtered
      if (tabbedGuard && tabbedField && String(f) === String(tabbedField)) return
      delete eff[f]
      delete eff[`${f}__gte`]
      delete eff[`${f}__lte`]
      delete eff[`${f}__gt`]
      delete eff[`${f}__lt`]
    }
    fieldsExposed.forEach((f) => rmKeysFor(f))
    fieldsExposed.forEach((f) => {
      const val = (uiWhere as any)[f]
      const gte = (uiWhere as any)[`${f}__gte`]
      const lte = (uiWhere as any)[`${f}__lte`]
      const gt = (uiWhere as any)[`${f}__gt`]
      const lt = (uiWhere as any)[`${f}__lt`]
      // Skip overriding the tabbed field while inside a tabbed pane
      if (tabbedGuard && tabbedField && String(f) === String(tabbedField)) return
      if (Array.isArray(val) && val.length > 0) eff[f] = val
      if (gte != null) eff[`${f}__gte`] = gte
      if (lte != null) eff[`${f}__lte`] = lte
      if (gt != null) eff[`${f}__gt`] = gt
      if (lt != null) eff[`${f}__lt`] = lt
    })
    // Merge in grid-level filters from AG Grid only for Data Table
    const isPivotMode = ((options?.table?.tableType || 'data') === 'pivot')
    if (!isPivotMode) Object.assign(eff, (gridWhere || {}))
    return eff
  }, [queryMode, querySpec, filters.startDate, filters.endDate, uiWhere, fieldsExposed, (options as any)?.deltaDateField, widgetId, (options?.table?.tableType || 'data'), JSON.stringify(gridWhere || {})])

  // Dev-only: trace where composition for tables
  if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
    try {
      // eslint-disable-next-line no-console
      console.debug('[TableCard] [FiltersDebug] where', { title, effectiveWhere })
    } catch {}
  }

  // Distinct loader for strings (omits self constraint)
  function useDistinctStrings(
    source?: string,
    datasourceId?: string,
    baseWhere?: Record<string, any>,
    customCols?: Array<{ name: string; formula: string }>,
  ) {
    const [cache, setCache] = useState<Record<string, string[]>>({})
    useEffect(() => { setCache({}) }, [source, datasourceId, JSON.stringify(baseWhere || {}), JSON.stringify(customCols || [])])
    const load = async (field: string) => {
      try {
        if (!source) return
        const isCustom = !!(customCols || []).find((c) => String(c?.name ?? '').trim().toLowerCase() === String(field ?? '').trim().toLowerCase())
        const isDev = (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production')
        const safeQuerySpec = async (payload: any) => {
          try { return await QueryApi.querySpec(payload) } catch { return undefined as any }
        }
        if (!isCustom) {
          const omit = { ...(baseWhere || {}) }
          Object.keys(omit).forEach((k) => { if (k === field || k.startsWith(`${field}__`)) delete (omit as any)[k] })
          try {
            if (typeof (Api as any).distinct === 'function') {
              const res = await Api.distinct({ source: String(source), field: String(field), where: Object.keys(omit).length ? omit : undefined, datasourceId })
              const arr = ((res?.values || []) as any[]).map((v) => (v != null ? String(v) : null)).filter((v) => v != null) as string[]
              const dedup = Array.from(new Set(arr).values()).sort()
              setCache((prev) => ({ ...prev, [field]: dedup }))
              return
            }
          } catch {}
          const pageSize = 5000
          let offset = 0
          const setVals = new Set<string>()
          while (true) {
            const spec: any = { source, select: [field], where: Object.keys(omit).length ? omit : undefined, limit: pageSize, offset }
            const res = await safeQuerySpec({ spec, datasourceId, limit: pageSize, offset, includeTotal: false })
            const cols = ((res?.columns || []) as string[])
            const idx = cols.indexOf(field)
            const rows = ((res?.rows || []) as any[])
            rows.forEach((row: any) => {
              let v: any
              if (idx >= 0) v = Array.isArray(row) ? row[idx] : (row?.[field])
              else if (Array.isArray(row)) v = row[0]
              else v = row?.[field]
              if (v !== null && v !== undefined) setVals.add(String(v))
            })
            if (rows.length === 0 || rows.length < pageSize) break
            offset += pageSize
            if (offset >= 500000) break
          }
          setCache((prev) => ({ ...prev, [field]: Array.from(setVals.values()).sort() }))
          return
        }
        const c = (customCols || []).find((cc) => cc.name === field)!
        const customNames = new Set<string>((customCols || []).map((cc) => cc.name))
        const DERIVED_RE = /^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/
        const serverWhere: Record<string, any> = {}
        Object.entries(baseWhere || {}).forEach(([k, v]) => { if (!DERIVED_RE.test(k) && !customNames.has(k)) (serverWhere as any)[k] = v })
        const customMap = new Map<string, { name: string; formula: string }>((customCols || []).map((cc) => [cc.name, cc]))
        const visited = new Set<string>()
        const order: string[] = []
        const baseRefs = new Set<string>()
        const visit = (name: string) => {
          if (visited.has(name)) return
          visited.add(name)
          const def = customMap.get(name)
          if (!def) return
          const refs = (parseReferences(def.formula).row || []) as string[]
          refs.forEach((r) => { if (customMap.has(r)) visit(r); else baseRefs.add(r) })
          order.push(name)
        }
        ;((parseReferences(c.formula).row || []) as string[]).forEach((r) => { if (customMap.has(r)) visit(r); else baseRefs.add(r) })
        const selectBase = Array.from(baseRefs.values())
        Object.keys(serverWhere).forEach((k) => { if (!selectBase.includes(k)) selectBase.push(k) })
        if (selectBase.length === 0) { setCache((prev) => ({ ...prev, [field]: [] })); return }
        const pageSize = 5000
        let offset = 0
        const cf = compileFormula(c.formula)
        const compiledDeps = new Map<string, ReturnType<typeof compileFormula>>()
        order.forEach((name) => { const def = customMap.get(name)!; compiledDeps.set(name, compileFormula(def.formula)) })
        const setVals = new Set<string>()
        try {
          const qIdent = (nm: string) => `[${String(nm).replace(/]/g, ']]')}]`
          const qSource = (src: string) => String(src).split('.').map(qIdent).join('.')
          const qLit = (v: any) => { if (v == null) return 'NULL'; if (typeof v === 'number' && Number.isFinite(v)) return String(v); if (v instanceof Date) return `'${v.toISOString().slice(0, 19).replace('T',' ')}'`; const s = String(v); return `'${s.replace(/'/g, "''")}'` }
          const buildWhere = (w?: Record<string, any>) => {
            if (!w || Object.keys(w).length === 0) return ''
            const parts: string[] = []
            Object.entries(w).forEach(([k, val]) => {
              let col = k; let op: string = '='
              if (k.endsWith('__gte')) { col = k.slice(0,-5); op = '>=' }
              else if (k.endsWith('__lte')) { col = k.slice(0,-5); op = '<=' }
              else if (k.endsWith('__gt')) { col = k.slice(0,-4); op = '>' }
              else if (k.endsWith('__lt')) { col = k.slice(0,-4); op = '<' }
              else if (k.endsWith('__in')) { col = k.slice(0,-4); op = 'IN' }
              if (op === 'IN' || Array.isArray(val)) {
                const arr = Array.isArray(val) ? val : [val]
                if (arr.length) parts.push(`${qIdent(col)} IN (${arr.map(qLit).join(', ')})`)
              } else {
                parts.push(`${qIdent(col)} ${op} ${qLit(val)}`)
              }
            })
            return parts.length ? ` WHERE ${parts.join(' AND ')}` : ''
          }
          const colsSql = selectBase.map(qIdent).join(', ')
          const sql = `SELECT DISTINCT ${colsSql} FROM ${qSource(String(source))}${buildWhere(Object.keys(serverWhere).length ? serverWhere : undefined)} ORDER BY 1`
          const r = await Api.query({ sql, datasourceId, limit: 500000, offset: 0 })
          const cols = (r.columns || []) as string[]
          const rows = (r.rows || []) as any[]
          rows.forEach((arr: any[]) => {
            const env: Record<string, any> = {}
            cols.forEach((nm, i) => { env[nm] = arr[i] })
            order.forEach((dep) => { const fn = compiledDeps.get(dep); try { env[dep] = fn ? fn.exec({ row: env }) : null } catch { env[dep] = null } })
            try { const v = cf.exec({ row: env }); if (v != null) setVals.add(String(v)) } catch {}
          })
        } catch {}
        if (setVals.size === 0) {
          while (true) {
            const spec: any = { source, select: selectBase, where: Object.keys(serverWhere).length ? serverWhere : undefined, limit: pageSize, offset }
            const res = await safeQuerySpec({ spec, datasourceId, limit: pageSize, offset, includeTotal: false })
            const cols = ((res?.columns || []) as string[])
            const rows = ((res?.rows || []) as any[])
            rows.forEach((arr: any[]) => {
              const env: Record<string, any> = {}
              cols.forEach((name, i) => { env[name] = arr[i] })
              order.forEach((dep) => { const fn = compiledDeps.get(dep); try { env[dep] = fn ? fn.exec({ row: env }) : null } catch { env[dep] = null } })
              try { const v = cf.exec({ row: env }); if (v !== null && v !== undefined) setVals.add(String(v)) } catch {}
            })
            if (rows.length === 0 || rows.length < pageSize) break
            offset += pageSize
            if (offset >= 500000) break
          }
        }
        const out = Array.from(setVals.values()).sort()
        setCache((prev) => ({ ...prev, [field]: out }))
      } catch {
        setCache((prev) => ({ ...prev, [field]: [] }))
      }
    }
    return { cache, load }
  }
  const { cache: distinctCache, load: loadDistinct } = useDistinctStrings((querySpec as any)?.source, datasourceId, effectiveWhere, (customColumns as any) || [])

  // Shared date parser
  function parseDateLoose(v: any): Date | null {
    if (v == null) return null
    if (v instanceof Date) return isNaN(v.getTime()) ? null : v
    const s = String(v).trim()
    if (!s) return null
    if (/^\d{10,13}$/.test(s)) { const n = Number(s); const ms = s.length === 10 ? n*1000 : n; const d = new Date(ms); return isNaN(d.getTime())?null:d }
    const norm = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
    let d = new Date(norm); if (!isNaN(d.getTime())) return d
    const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/); if (iso) { d = new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`); return isNaN(d.getTime())?null:d }
    const m = s.match(/^([0-1]?\d)\/([0-3]?\d)\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/)
    if (m) { const mm=Number(m[1])-1, dd=Number(m[2]), yyyy=Number(m[3]); const t=m[4]||'00:00:00'; d = new Date(`${yyyy}-${String(mm+1).padStart(2,'0')}-${String(dd).padStart(2,'0')}T${t.length===5?t+':00':t}`); return isNaN(d.getTime())?null:d }
    return null
  }

  // Fallback string values from current query result rows
  const fallbackStringsFor = (field: string, qdata?: QueryResponse): string[] => {
    try {
      const cols = (qdata?.columns as string[]) || []
      const rows = (qdata?.rows as any[]) || []
      if (cols.length === 0 || rows.length === 0) return []
      const idx = cols.indexOf(field)
      const set = new Set<string>()
      if (idx >= 0) {
        rows.forEach((r: any) => { const v = Array.isArray(r) ? r[idx] : r?.[field]; if (v != null) set.add(String(v)) })
        return Array.from(set.values()).sort()
      }
      if (typeof rows[0] === 'object' && !Array.isArray(rows[0]) && (field in (rows[0] || {}))) {
        rows.forEach((r: any) => { const v = r?.[field]; if (v != null) set.add(String(v)) })
        return Array.from(set.values()).sort()
      }
      return []
    } catch { return [] }
  }

  const offset = page * size
  const uiKey = useMemo(() => JSON.stringify(effectiveWhere || {}), [effectiveWhere])
  const fieldsKey = useMemo(() => JSON.stringify(fieldsExposed || []), [fieldsExposed])
  const debouncedUiKey = useDebounced(uiKey, 350)
  const debouncedGridKey = useDebounced(JSON.stringify(gridWhere || {}), 350)
  // Listen for global break-link toggles
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string }
      if (!widgetId || !d?.widgetId || d.widgetId !== widgetId) return
      setBreakSeq((v) => v + 1)
    }
    if (typeof window !== 'undefined') window.addEventListener('global-filters-break-change', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('global-filters-break-change', handler as EventListener) }
  }, [widgetId])

  const isPivot = (options?.table?.tableType || 'data') === 'pivot'
  const isSnap = useMemo(() => {
    try { const el = (typeof document !== 'undefined') ? document.getElementById('widget-root') : null; return !!el && el.getAttribute('data-snap') === '1' } catch { return false }
  }, [])
  const pivotConfKey = JSON.stringify({
    cfg: options?.table?.pivotConfig || {},
    vals: Array.isArray((options?.table?.pivotConfig as any)?.vals) ? (options?.table?.pivotConfig as any)?.vals : [],
    pvAggs: Array.isArray((pivot as any)?.values) ? (pivot as any).values.map((v: any) => `${v.field||v.measureId||''}:${v.agg||''}`) : [],
    fmt: options?.yAxisFormat || 'number',
    cur: (options as any)?.valueCurrency || undefined,
  })
  const queryKeyArr = useMemo(() => {
    const src = String((querySpec as any)?.source || '')
    if (isPivot) {
      return ['table', 'pivot', title, datasourceId, src, debouncedUiKey, fieldsKey, pivotConfKey, breakSeq]
    }
    return ['table', 'data', title, sql, datasourceId, queryMode, src, debouncedUiKey, fieldsKey, 'paged', breakSeq, page, size, debouncedGridKey]
  }, [isPivot, title, datasourceId, (querySpec as any)?.source, debouncedUiKey, fieldsKey, pivotConfKey, breakSeq, sql, queryMode, page, size, debouncedGridKey])
  const q = useQuery<QueryResponse>({
    queryKey: queryKeyArr,
    enabled: visible,
    staleTime: 30000,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    retry: 0,
    placeholderData: (prev) => prev as any,
    queryFn: async ({ signal }) => {
      // Compose effective where (same logic as above) for execution
      let effective: Record<string, any> | undefined = undefined
      if (queryMode === 'spec' && querySpec) {
        const baseWhere: Record<string, any> = { ...(querySpec.where || {}) }
        const df = (options as any)?.deltaDateField as string | undefined
        const ignoreGlobal = (() => { try { return !!(widgetId && localStorage.getItem(`gf_break_${widgetId}`) === '1') } catch { return false } })()
        if (df && !ignoreGlobal) {
          if (filters.startDate) baseWhere[`${df}__gte`] = filters.startDate
          if (filters.endDate) {
            const d = new Date(`${filters.endDate}T00:00:00`)
            d.setDate(d.getDate() + 1)
            const y = d.getFullYear()
            const m = String(d.getMonth() + 1).padStart(2, '0')
            const da = String(d.getDate()).padStart(2, '0')
            const nextDay = `${y}-${m}-${da}`
            baseWhere[`${df}__lt`] = nextDay
          }
        }
        // Remove base constraints for exposed fields, then apply UI constraints for those fields
        effective = { ...baseWhere }
        const rmKeysFor = (f: string) => {
          delete effective![f]
          delete effective![`${f}__gte`]
          delete effective![`${f}__lte`]
          delete effective![`${f}__gt`]
          delete effective![`${f}__lt`]
        }
        fieldsExposed.forEach((f) => rmKeysFor(f))
        fieldsExposed.forEach((f) => {
          const val = (uiWhere as any)[f]
          if (Array.isArray(val) && val.length > 0) (effective as any)[f] = val
          const gte = (uiWhere as any)[`${f}__gte`]
          const lte = (uiWhere as any)[`${f}__lte`]
          const gt = (uiWhere as any)[`${f}__gt`]
          const lt = (uiWhere as any)[`${f}__lt`]
          if (gte != null) (effective as any)[`${f}__gte`] = gte
          if (lte != null) (effective as any)[`${f}__lte`] = lte
          if (gt != null) (effective as any)[`${f}__gt`] = gt
          if (lt != null) (effective as any)[`${f}__lt`] = lt
        })
      }

      // Pivot: ALWAYS use server-side pivot and support multiple Values (chips)
      if (isPivot) {
        // Require QuerySpec with source
        if (!(queryMode === 'spec' && querySpec && querySpec.source)) {
          return { columns: [], rows: [], elapsedMs: 0, totalRows: 0 }
        }
        const cfg: any = options?.table?.pivotConfig || {}
        const rowsDims: string[] = Array.isArray(cfg.rows) ? cfg.rows : []
        const colsDims: string[] = Array.isArray(cfg.cols) ? cfg.cols : []
        const valsArr: string[] = Array.isArray(cfg.vals) ? cfg.vals : []
        const pvList: Array<{ field?: string; measureId?: string; agg?: string; label?: string }> = Array.isArray((pivot as any)?.values) ? (pivot as any).values : []
        const maxRowsCfg = (options?.table as any)?.pivotMaxRows
        const mapFor = (agg?: string): 'sum'|'avg'|'min'|'max'|'distinct'|'count' => {
          const s = String(agg||'').toLowerCase()
          if (s.includes('sum')) return 'sum'
          if (s.startsWith('avg')) return 'avg'
          if (s === 'min') return 'min'
          if (s === 'max') return 'max'
          if (s.includes('distinct')) return 'distinct'
          return 'count'
        }
        // Build tasks per selected value field
        const fieldsToFetch: string[] = valsArr.filter(Boolean)
        if (fieldsToFetch.length === 0) return { columns: [], rows: [], elapsedMs: 0, totalRows: 0 }
        const meta: Array<{ label: string; field: string; agg: 'sum'|'avg'|'min'|'max'|'distinct'|'count' }> = []
        const wid = String(widgetId || title || 'table')
        // Build call descriptors but DO NOT start requests yet (avoid self-abort)
        const calls = fieldsToFetch.map((f, idx) => {
          const chip = (pvList[idx] as any) || pvList.find((v) => (v.field === f || v.measureId === f))
          const agg = mapFor(chip?.agg)
          const label = String(chip?.label || f)
          const chosen = String(chip?.field || chip?.measureId || f || '')
          const valueFieldName = aliasToBase[chosen] ? String(aliasToBase[chosen]) : chosen
          meta.push({ label, field: valueFieldName, agg })
          const subKey = `${wid}::pv:${valueFieldName}:${agg}`
          return (sig?: AbortSignal) => Api.pivotForWidget(subKey, {
            source: String((querySpec as any).source),
            datasourceId,
            rows: rowsDims,
            cols: colsDims,
            valueField: valueFieldName || null,
            aggregator: agg,
            where: effective,
            widgetId: widgetId,
            groupBy: (querySpec as any)?.groupBy || undefined,
            weekStart: (options as any)?.xWeekStart || (querySpec as any)?.weekStart || undefined,
            ...(maxRowsCfg != null ? { limit: Number(maxRowsCfg) } : {}),
          }, undefined, sig).promise
        })
        // Emit runtime pivot payloads for SQL preview dialogs
        try {
          if (typeof window !== 'undefined' && widgetId) {
            const payloads = meta.map((m) => ({
              source: String((querySpec as any).source),
              datasourceId,
              rows: rowsDims,
              cols: colsDims,
              valueField: m.field || null,
              aggregator: m.agg,
              where: effective,
              widgetId: widgetId,
              groupBy: (querySpec as any)?.groupBy || undefined,
              weekStart: (options as any)?.xWeekStart || (querySpec as any)?.weekStart || undefined,
              __label: m.label,
            }))
            window.dispatchEvent(new CustomEvent('widget-pivot-payloads', { detail: { widgetId, payloads } } as any))
            // Attempt to fetch generated SQL strings for these payloads in background
            ;(async () => {
              try {
                const results = await Promise.all(payloads.map(async (p) => {
                  try { const r = await Api.pivotSql(p); return { label: p.__label as string, sql: String((r as any)?.sql || '') } } catch { return { label: p.__label as string, sql: '' } }
                }))
                window.dispatchEvent(new CustomEvent('widget-pivot-sql', { detail: { widgetId, sqls: results } } as any))
              } catch {}
            })()
          }
        } catch {}
        // Run in small batches to avoid overwhelming the DB/driver when many value fields are selected
        const results: QueryResponse[] = []
        for (let i = 0; i < calls.length; i++) {
          // Run strictly sequentially to avoid overlapping aborts
          const r = await calls[i](signal)
          results.push(r)
        }
        // Merge by appending a synthetic metric dimension
        const METRIC = '__metric__'
        let columns: string[] = []
        const rows: any[] = []
        results.forEach((res, i) => {
          const label = meta[i]?.label || ''
          if (columns.length === 0) {
            columns = Array.isArray(res.columns) ? res.columns.slice() : []
            if (columns.length > 0 && columns[columns.length - 1] !== METRIC) columns.push(METRIC)
          }
          ;(res.rows || []).forEach((r: any[]) => {
            const arr = Array.isArray(r) ? r.slice() : []
            arr.push(label)
            rows.push(arr)
          })
        })
        const totalElapsed = results.reduce((acc, r) => acc + (Number(r?.elapsedMs || 0)), 0)
        return { columns, rows, elapsedMs: totalElapsed, totalRows: rows.length }
      }

      // Data table: single-page fetch with pagination
      if (queryMode === 'spec' && querySpec) {
        const merged: QuerySpec = { ...querySpec, where: effective }
        const mergedSafe: any = { ...merged }
        if (Array.isArray((mergedSafe as any).x)) (mergedSafe as any).x = (mergedSafe as any).x[0]
        if ('limit' in mergedSafe) delete (mergedSafe as any).limit
        if ('offset' in mergedSafe) delete (mergedSafe as any).offset
        return QueryApi.querySpec({ spec: mergedSafe, datasourceId, limit: size, offset, includeTotal: true, preferLocalDuck: (options as any)?.preferLocalDuck })
      }
      // Include grid filters for SQL mode too (best-effort)
      const params: any = ignoreGlobal ? (gridWhere || {}) : { ...(filters || {}), ...(gridWhere || {}) }
      const wid = String(widgetId || title || 'table')
      const { promise: __p } = Api.queryForWidget(
        wid,
        {
          sql,
          datasourceId,
          limit: size,
          offset,
          includeTotal: true,
          params,
          preferLocalDuck: (options as any)?.preferLocalDuck,
          preferLocalTable: ((querySpec as any)?.source as string | undefined),
        },
        user?.id,
      )
      return __p
    },
  })

  // Load time tracking (seconds) to match ChartCard spinner timer and emit event
  useEffect(() => {
    if (q.isLoading) {
      loadStartRef.current = Date.now()
      setLoadingSeconds(0)
      if (loadingTimerRef.current) { try { clearInterval(loadingTimerRef.current) } catch {} }
      loadingTimerRef.current = setInterval(() => {
        if (loadStartRef.current != null) setLoadingSeconds(Math.max(0, Math.floor((Date.now() - loadStartRef.current) / 1000)))
      }, 1000)
    }
    if (!q.isLoading) {
      if (loadingTimerRef.current) { try { clearInterval(loadingTimerRef.current) } catch {} }
      loadingTimerRef.current = null
      if (loadStartRef.current != null && q.data && (options as any)?.showLoadTime) {
        const secs = Math.max(0, Math.round((Date.now() - loadStartRef.current) / 1000))
        if (typeof window !== 'undefined' && widgetId) {
          try { window.dispatchEvent(new CustomEvent('chart-load-time', { detail: { widgetId, seconds: secs } } as any)) } catch {}
        }
      }
      loadStartRef.current = null
    }
    return () => { if (loadingTimerRef.current) { try { clearInterval(loadingTimerRef.current) } catch {} } }
  }, [q.isLoading, !!q.data, widgetId, (options as any)?.showLoadTime])

  // Emit latest result columns so Configurator can present correct field options
  useEffect(() => {
    if (!widgetId) return
    const cols = (q.data?.columns as string[]) || []
    if (!cols.length) return
    try { window.dispatchEvent(new CustomEvent('table-columns-change', { detail: { widgetId, columns: cols } })) } catch {}
  }, [widgetId, q.data?.columns])

  type Row = Record<string, unknown>
  const rows: Row[] = ((q.data?.rows as Array<Array<unknown>>) || []).map((r: Array<unknown>) => {
    const obj: Record<string, unknown> = {}
    r.forEach((cell: unknown, idx: number) => { obj[`c${idx}`] = cell as unknown })
    return obj as Row
  })

  // Emit sample distinct values per column for filters UI (based on current page rows)
  useEffect(() => {
    if (!widgetId) return
    const cols = (q.data?.columns as string[]) || []
    const rawRows = (q.data?.rows as Array<Array<unknown>>) || []
    if (!cols.length || !rawRows.length) return
    const samples: Record<string, string[]> = {}
    cols.forEach((name, idx) => {
      const set = new Set<string>()
      for (const r of rawRows) {
        const v = r[idx]
        if (v === null || v === undefined) continue
        set.add(String(v))
        if (set.size >= 200) break
      }
      samples[name] = Array.from(set.values()).sort()
    })
    try { window.dispatchEvent(new CustomEvent('table-sample-values-change', { detail: { widgetId, samples } })) } catch {}
  }, [widgetId, q.data?.columns, q.data?.rows])

  // Emit sample rows with named columns for custom column preview/editor
  useEffect(() => {
    if (!widgetId) return
    const cols = (q.data?.columns as string[]) || []
    const rawRows = (q.data?.rows as Array<Array<unknown>>) || []
    if (!cols.length || !rawRows.length) return
    const namedRows = rawRows.slice(0, 50).map((arr) => {
      const o: Record<string, unknown> = {}
      cols.forEach((c, i) => { o[c] = arr[i] })
      return o
    })
    try { window.dispatchEvent(new CustomEvent('table-sample-rows-change', { detail: { widgetId, rows: namedRows, columns: cols } })) } catch {}
  }, [widgetId, q.data?.columns, q.data?.rows])

  // Respond to on-demand rows requests from Configurator (for live preview)
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string }
      if (!d?.widgetId || d.widgetId !== widgetId) return
      const cols = (q.data?.columns as string[]) || []
      const rawRows = (q.data?.rows as Array<Array<unknown>>) || []
      if (!cols.length || !rawRows.length) return
      const namedRows = rawRows.slice(0, 50).map((arr) => {
        const o: Record<string, unknown> = {}
        cols.forEach((c, i) => { o[c] = arr[i] })
        return o
      })
      try { window.dispatchEvent(new CustomEvent('table-sample-rows-change', { detail: { widgetId, rows: namedRows, columns: cols } })) } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('request-table-rows', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('request-table-rows', handler as EventListener) }
  }, [widgetId, q.data?.columns, q.data?.rows])

  // Respond to on-demand columns request from Configurator (refresh fields list)
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string }
      if (!d?.widgetId || d.widgetId !== widgetId) return
      const cols = (q.data?.columns as string[]) || []
      if (!cols.length) return
      try { window.dispatchEvent(new CustomEvent('table-columns-change', { detail: { widgetId, columns: cols } })) } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('request-table-columns', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('request-table-columns', handler as EventListener) }
  }, [widgetId, q.data?.columns])

  // Respond to on-demand sample requests from Configurator
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string }
      if (!d?.widgetId || d.widgetId !== widgetId) return
      const cols = (q.data?.columns as string[]) || []
      const rawRows = (q.data?.rows as Array<Array<unknown>>) || []
      if (!cols.length || !rawRows.length) return
      const samples: Record<string, string[]> = {}
      cols.forEach((name, idx) => {
        const set = new Set<string>()
        for (const r of rawRows) {
          const v = r[idx]
          if (v === null || v === undefined) continue
          set.add(String(v))
          if (set.size >= 200) break
        }
        samples[name] = Array.from(set.values()).sort()
      })
      try { window.dispatchEvent(new CustomEvent('table-sample-values-change', { detail: { widgetId, samples } })) } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('request-table-samples', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('request-table-samples', handler as EventListener) }
  }, [widgetId, q.data?.columns, q.data?.rows])

  const autoFit = options?.autoFitCardContent !== false
  const cardFill = options?.cardFill || 'default'
  const bgStyle = cardFill === 'transparent' ? { backgroundColor: 'transparent' } : cardFill === 'custom' ? { backgroundColor: options?.cardCustomColor || '#ffffff' } : undefined
  const isPivotCard = ((options?.table?.tableType || 'data') === 'pivot')
  const cardClass = `${(autoFit && !isPivotCard) ? '' : 'h-full'} ${isPivotCard ? 'flex flex-col min-h-0' : ''} !border-0 shadow-none rounded-lg ${cardFill === 'transparent' ? 'bg-transparent' : 'bg-card'}`
  const tableType = options?.table?.tableType || 'data'

  // Date range menu with Apply/Cancel
  const DateRangeMenu = ({ field, a, b, onApply, onClear }: { field: string; a?: string; b?: string; onApply: (params: { a?: string; b?: string }) => void; onClear: () => void }) => {
    const [start, setStart] = useState<string>(a || '')
    const [end, setEnd] = useState<string>(b || '')
    return (
      <div className="p-1 space-y-2 w-[260px]">
        <div className="grid grid-cols-2 items-center gap-2">
          <label className="text-[11px] text-muted-foreground">Start</label>
          <input type="date" className="h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--card))]" value={start} onChange={(e) => setStart(e.target.value)} />
          <label className="text-[11px] text-muted-foreground">End</label>
          <input type="date" className="h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--card))]" value={end} onChange={(e) => setEnd(e.target.value)} />
        </div>
        <div className="flex items-center justify-end gap-2">
          <button className="text-[11px] px-2 py-1 rounded-md border hover:bg-muted" onClick={onClear}>Clear</button>
          <button className="text-[11px] px-2 py-1 rounded-md border bg-[hsl(var(--btn3))] text-black" onClick={() => onApply({ a: start || undefined, b: end || undefined })}>Apply</button>
        </div>
      </div>
    )
  }

  // Tabs (same pattern as ChartCard): render wrapper when tabsField configured and not already inside a tab
  const tabsFieldOpt = (options as any)?.tabsField as string | undefined
  const tabsVariant = ((options as any)?.tabsVariant || 'line') as 'line' | 'solid'
  const tabsShowAll = !!((options as any)?.tabsShowAll)
  const tabsMaxItems = Math.max(1, Number((options as any)?.tabsMaxItems ?? 8))
  const tabsStretch = !!((options as any)?.tabsStretch)
  const tabsLabelCase = (((options as any)?.tabsLabelCase || 'legend') as 'legend'|'lowercase'|'capitalize'|'proper')
  const tabsSort = ((options as any)?.tabsSort || {}) as { by?: 'x'|'value'; direction?: 'asc'|'desc' }
  const { cache: tabsCache, load: loadTabsVals } = useDistinctStrings((querySpec as any)?.source, datasourceId, effectiveWhere as any, (customColumns || []) as any)
  const tabValues = useMemo(() => {
    if (!tabsFieldOpt) return [] as string[]
    return (tabsCache?.[tabsFieldOpt] || []) as string[]
  }, [tabsCache, tabsFieldOpt])
  useEffect(() => { if (tabsFieldOpt) void loadTabsVals(tabsFieldOpt) }, [tabsFieldOpt, (querySpec as any)?.source, datasourceId, JSON.stringify(effectiveWhere || {}), JSON.stringify(customColumns || [])])
  // Optional sort by row counts when by='value'
  const [tabCounts, setTabCounts] = useState<Record<string, number> | null>(null)
  useEffect(() => {
    const run = async () => {
      try {
        if (!tabsFieldOpt || (tabsSort?.by !== 'value') || queryMode !== 'spec' || !querySpec) { setTabCounts(null); return }
        const baseVals = tabValues.slice(0, tabsMaxItems)
        const counts: Record<string, number> = {}
        // Compute counts using includeTotal only (fast)
        for (const v of baseVals) {
          const where: Record<string, any> = { ...(((querySpec as any)?.where || {}) as any), ...((effectiveWhere || {}) as any) }
          ;(where as any)[tabsFieldOpt] = [v]
          const res = await QueryApi.querySpec({ spec: { ...(querySpec as any), where }, datasourceId, limit: 0, offset: 0, includeTotal: true })
          const total = Number((res as any)?.totalRows ?? ((res as any)?.rows?.length || 0))
          counts[v] = total
        }
        setTabCounts(counts)
      } catch { setTabCounts(null) }
    }
    void run()
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tabsFieldOpt, (tabsSort?.by || 'x'), (querySpec as any)?.source, datasourceId, JSON.stringify(effectiveWhere || {}), JSON.stringify(customColumns || []), tabsMaxItems])
  const sortedTabValues = useMemo(() => {
    const arr = tabValues.slice(0, tabsMaxItems)
    const by = (tabsSort?.by || 'x')
    const dir = (tabsSort?.direction || 'desc')
    const cmp = (a: number, b: number) => (dir === 'asc' ? (a - b) : (b - a))
    if (by === 'value' && tabCounts) {
      return arr.sort((a, b) => cmp(tabCounts[a] || 0, tabCounts[b] || 0))
    }
    // by 'x' (label)
    return arr.sort((a, b) => {
      const la = String(a).toLowerCase(); const lb = String(b).toLowerCase()
      if (la === lb) return 0
      const asc = la < lb ? -1 : 1
      return dir === 'asc' ? asc : -asc
    })
  }, [tabValues, tabsMaxItems, JSON.stringify(tabCounts || {}), tabsSort?.by, tabsSort?.direction])
  const wantTabs = !!tabsFieldOpt && !tabbedGuard && ((sortedTabValues.length > 0) || tabsShowAll)
  const [activeTab, setActiveTab] = useState<string | undefined>(undefined)
  const allPrefixedValues = useMemo(() => (tabsShowAll ? ['__ALL__', ...sortedTabValues] : sortedTabValues), [tabsShowAll, sortedTabValues])
  const defaultTabValue = useMemo(() => (allPrefixedValues.length > 0 ? String(allPrefixedValues[0]) : ''), [allPrefixedValues])

  // Helper for tab labels (match ChartCard casing rules roughly)
  const toProperCase = (s: string): string => String(s||'').replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
  const labelForTab = (raw: string): string => {
    const base = raw === '__ALL__' ? 'All' : normalizeCategoryLabel(String(raw))
    const mode = tabsLabelCase
    if (!mode || mode === 'legend') return toProperCase(base)
    switch (mode) {
      case 'lowercase': return base.toLowerCase()
      case 'capitalize': return base.toUpperCase()
      case 'proper': default: return toProperCase(base)
    }
  }

  if (wantTabs) {
    const defaultVal = defaultTabValue
    return (
      <ErrorBoundary name="TableCard">
      <div className={cardClass} style={bgStyle as any} ref={containerRef}>
        <div className="h-full flex flex-col">
          <Tabs value={activeTab || defaultVal} defaultValue={defaultVal} className="h-full flex flex-col" onValueChangeAction={(v) => { setActiveTab(v); if (typeof window !== 'undefined') { if ('requestAnimationFrame' in window) { window.requestAnimationFrame(() => window.dispatchEvent(new Event('resize'))) } else { setTimeout(() => window.dispatchEvent(new Event('resize')), 16) } } }}>
            <TabsList variant={tabsVariant} className={`${tabsStretch ? 'w-full justify-start overflow-x-auto' : ''}`}>
              {allPrefixedValues.map((v) => {
                const label = labelForTab(String(v))
                const isActive = String(activeTab || '') === String(v)
                return (
                  <TabsTrigger key={v} value={String(v)} className={isActive ? 'font-semibold max-w-none overflow-visible' : 'truncate'} style={isActive ? { maxWidth: 'none' } : undefined}>
                    <span className="relative group block" title={label}>
                      <span className={isActive ? 'font-semibold whitespace-nowrap' : 'truncate'}>{label}</span>
                      <span className="pointer-events-none absolute top-full left-1/2 -translate-x-1/2 mt-1 whitespace-nowrap px-2 py-1 rounded-md border bg-[hsl(var(--card))] text-[hsl(var(--foreground))] text-[11px] opacity-0 group-hover:opacity-100 shadow-sm z-50">
                        {label}
                      </span>
                    </span>
                  </TabsTrigger>
                )
              })}
            </TabsList>
            <div className="mt-3 flex-1 min-h-0">
              {tabsShowAll && (
                <TabsContent key="__ALL__" value="__ALL__" className="h-full" forceMount>
                  <div className="h-full">
                    <TableCard
                      key={`${widgetId || title || 'table'}::tab:__ALL__`}
                      title={title}
                      sql={sql}
                      datasourceId={datasourceId}
                      options={{ ...(options || {}), tabsField: undefined }}
                      queryMode={queryMode}
                      querySpec={querySpec}
                      customColumns={customColumns}
                      widgetId={widgetId ? `${widgetId}::tab:__ALL__` : undefined}
                      pivot={pivot}
                      tabbedGuard={true}
                      tabbedField={tabsFieldOpt as string}
                    />
                  </div>
                </TabsContent>
              )}
              {sortedTabValues.map((v) => (
                <TabsContent key={v} value={String(v)} className="h-full" forceMount>
                  <div className="h-full">
                    <TableCard
                      key={`${widgetId || title || 'table'}::tab:${String(v)}`}
                      title={title}
                      sql={sql}
                      datasourceId={datasourceId}
                      options={{ ...(options || {}), tabsField: undefined }}
                      queryMode={queryMode}
                      querySpec={querySpec ? { ...querySpec, where: { ...((querySpec as any)?.where || {}), [tabsFieldOpt as string]: [v] } } : undefined}
                      customColumns={customColumns}
                      widgetId={widgetId ? `${widgetId}::tab:${String(v)}` : undefined}
                      pivot={pivot}
                      tabbedGuard={true}
                      tabbedField={tabsFieldOpt as string}
                    />
                  </div>
                </TabsContent>
              ))}
            </div>
          </Tabs>
        </div>
      </div>
      </ErrorBoundary>
    )
  }

  return (
    <ErrorBoundary name="TableCard">
    <div className={cardClass} style={bgStyle as any} ref={containerRef}>
      <style>{`@keyframes square-bounce{0%,80%,100%{transform:scale(0.9);opacity:0.6}40%{transform:scale(1);opacity:1}}`}</style>
      {q.isLoading ? (
        <div className="space-y-2 animate-pulse">
          <div className="flex flex-col items-center justify-center h-[220px]">
            <div className="grid grid-cols-2 gap-2">
              {[0,1,2,3].map((i)=> (
                <span key={i} className="w-3.5 h-3.5 rounded-sm" style={{ backgroundColor: 'hsl(var(--primary))', animation: `square-bounce 1.2s ${i*0.12}s infinite ease-in-out` }} />
              ))}
            </div>
            {((options as any)?.showLoadTime) ? (
              <div className="mt-2 text-[11px] text-muted-foreground">{Math.max(0, loadingSeconds)}s</div>
            ) : null}
          </div>
        </div>
      ) : q.error ? (
        <div className="text-sm text-red-600">
          Failed to load table
          {typeof window !== 'undefined' && process.env.NODE_ENV !== 'production' && (
            <div className="mt-2 text-xs font-mono whitespace-pre-wrap">
              {String((q.error as any)?.message || q.error)}
            </div>
          )}
        </div>
      ) : rows.length === 0 ? (
        <div className="text-sm text-muted-foreground">No data</div>
      ) : (
        <>
          {(() => {
            const wantsHeader = (queryMode === 'spec' && !!dsDefaultsText) || (fieldsExposed.length > 0)
            if (!wantsHeader) return null
            return (
            <div className="flex items-center justify-between gap-2 mb-2">
              <div className="min-w-0" />
              <div className="flex flex-wrap items-center justify-end gap-2">
              {queryMode === 'spec' && dsDefaultsText && (
                <span className="text-[10px] px-2 py-0.5 rounded-md border bg-card text-muted-foreground" title="Datasource defaults">
                  Using defaults: {dsDefaultsText}
                </span>
              )}
              {/* Loaded-in badge is shown in the card header via event; not duplicated here */}
              <FilterbarControl
                active={undefined}
                options={[] as any}
                labels={{}}
                onChange={() => {}}
                className="hidden"
              />
              {fieldsExposed.map((field) => {
                if (!distinctCache[field]) { void loadDistinct(field) }
                const sample = (distinctCache[field] || []).slice(0, 12)
                let kind: 'string'|'number'|'date' = 'string'
                const numHits = sample.filter((s) => Number.isFinite(Number(s))).length
                const dateHits = sample.filter((s) => { const d = parseDateLoose(s); return !!d }).length
                // Prefer number if both match
                if (numHits >= Math.max(1, Math.ceil(sample.length/2))) kind = 'number'
                else if (dateHits >= Math.max(1, Math.ceil(sample.length/2))) kind = 'date'
                const baseWhere = ((querySpec as any)?.where || {}) as Record<string, any>
                const sel = uiWhere[field] as any
                const label = (() => {
                  if (kind === 'string') {
                    const arr: any[] = Array.isArray(sel) ? sel : []
                    return arr && arr.length ? `${field} (${arr.length})` : field
                  }
                  if (kind === 'date') {
                    const a = (uiWhere[`${field}__gte`] as string|undefined) || (baseWhere as any)[`${field}__gte`]
                    const b = (uiWhere[`${field}__lt`] as string|undefined) || (baseWhere as any)[`${field}__lt`]
                    return (a||b) ? `${field} (${a||''}–${b||''})` : field
                  }
                  const ops = ['gte','lte','gt','lt'] as const
                  const exp = ops.map(op => (uiWhere[`${field}__${op}`] ?? (baseWhere as any)[`${field}__${op}`])).some(v => v!=null)
                  return exp ? `${field} (filtered)` : field
                })()
                const mergedWhere: Record<string, any> = (uiWhere as any)
                return (
                  <FilterbarRuleControl
                    key={field}
                    label={label}
                    kind={kind}
                    field={field}
                    where={mergedWhere}
                    distinctCache={distinctCache as any}
                    loadDistinctAction={loadDistinct as any}
                    onPatchAction={(patch: Record<string, any>) => setUiWhereAndEmit(patch)}
                  />
                )
              })}
              </div>
            </div>)
          })()}
          {tableType === 'pivot' ? (
            (() => {
              // Build options for PivotMatrixView: force vals=['value'] and append synthetic metric dim for multi-values
              const rawCfg: any = options?.table?.pivotConfig || {}
              const cleanCfg: any = { ...rawCfg }
              delete cleanCfg.aggregatorName
              const cols: string[] = Array.isArray(cleanCfg.cols) ? cleanCfg.cols.slice() : []
              const hasMetric = Array.isArray((pivot as any)?.values) && ((pivot as any).values.length > 1)
              const METRIC = '__metric__'
              const colsWithMetric = hasMetric ? (cols.includes(METRIC) ? cols : [...cols, METRIC]) : cols
              const style = { ...((options?.table?.pivotStyle || {}) as any), valueFormat: (options?.yAxisFormat || (options?.table?.pivotStyle as any)?.valueFormat), valueCurrency: (options as any)?.valueCurrency }
              const pivotViewTableOptions = { ...options?.table, pivotConfig: { ...cleanCfg, cols: colsWithMetric, vals: ['value'] }, pivotStyle: style }
              const pivotForMatrix = { ...(pivot as any), values: (Array.isArray((pivot as any)?.values) ? (pivot as any).values.map((v: any) => ({ ...v, agg: 'sum' })) : []) }
              const onExport = () => { try { window.dispatchEvent(new CustomEvent('pivot-export-excel', { detail: { widgetId, filename: title } } as any)) } catch {} }
              const showControls = pivotViewTableOptions?.showControls ?? true // Default to true
              return (
                <div className="flex-1 min-h-0 overflow-auto">
                  {!isSnap && showControls && (
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <button
                        className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
                        onClick={() => { try { window.dispatchEvent(new CustomEvent('pivot-expand-all', { detail: { widgetId } } as any)) } catch {} }}
                        title="Expand all parent rows"
                      >
                        Expand All
                      </button>
                      <button
                        className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
                        onClick={() => { try { window.dispatchEvent(new CustomEvent('pivot-collapse-all', { detail: { widgetId } } as any)) } catch {} }}
                        title="Collapse all parent rows"
                      >
                        Collapse All
                      </button>
                    </div>
                    <div className="flex items-center gap-2">
                      <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] inline-flex items-center gap-1" onClick={onExport} title="Download as Excel (.xlsx)">
                        <RiFileExcel2Fill className="w-4 h-4 text-emerald-600" />
                        <span>Download Excel</span>
                      </button>
                    </div>
                  </div>
                  )}
                  <PivotMatrixView
                    rows={rows as any}
                    columns={(q.data?.columns as string[]) || []}
                    widgetId={widgetId}
                    tableOptions={pivotViewTableOptions}
                    pivot={pivotForMatrix as any}
                    where={effectiveWhere as any}
                  />
                </div>
              )
            })()
          ) : (
            <>
              <div className="max-h-[70vh] overflow-y-auto">
                <AgTable
                  rows={rows as any}
                  columns={(q.data?.columns as string[]) || []}
                  tableOptions={options?.table}
                  onFilterWhereChangeAction={(w) => { setPage(0); setGridWhere(w || {}); }}
                />
              </div>
              {/* Pager */}
              <div className="mt-2 flex flex-col gap-2">
                {/* Top controls: page size + page select */}
                <div className="flex flex-wrap items-center gap-2">
                  <label className="text-[11px] text-muted-foreground">Rows per page</label>
                  <select
                    className="h-8 px-2 rounded-md border bg-card text-xs"
                    value={size}
                    onChange={(e)=>{ const n = Number(e.target.value)||10; setPage(0); setSize(n) }}
                  >
                    {[10,25,50,100,200,500].map(n => (<option key={n} value={n}>{n}</option>))}
                  </select>
                  {(() => {
                    const totalRows = (q.data?.totalRows ?? (q.data?.rows?.length || 0)) as number
                    const totalPages = Math.max(1, Math.ceil((totalRows || 0) / Math.max(1,size)))
                    const curr = Math.min(page, totalPages-1)
                    return (
                      <>
                        <span className="text-[11px] text-muted-foreground ml-2">Page</span>
                        <select
                          className="h-8 px-2 rounded-md border bg-card text-xs"
                          value={curr+1}
                          onChange={(e)=>{ const p0 = Math.max(0, Math.min(totalPages-1, (Number(e.target.value)||1)-1)); setPage(p0) }}
                        >
                          {Array.from({length: Math.min(totalPages, 500)}, (_,i)=>i+1).map(n => (<option key={n} value={n}>{n} / {totalPages}</option>))}
                        </select>
                        <span className="text-[11px] text-muted-foreground">of {totalPages}</span>
                      </>
                    )
                  })()}
                  <div className="ml-auto text-[11px] text-muted-foreground">
                    {(() => {
                      const totalRows = (q.data?.totalRows ?? (q.data?.rows?.length || 0)) as number
                      const start = (page*size) + 1
                      const end = Math.min(totalRows, (page*size)+size)
                      return totalRows ? `Showing ${start}-${end} of ${totalRows}` : ''
                    })()}
                  </div>
                </div>  
                {/* Numbered navigation */}
                {null}
              </div>
            </>
          )}
        </>
      )}
    </div>
    </ErrorBoundary>
  )
}
