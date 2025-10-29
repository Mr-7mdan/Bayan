"use client"

import { Metric } from '@tremor/react'
import dynamic from 'next/dynamic'
import { Api, QueryApi, RequestGuard } from '@/lib/api'
import { useEffect, useMemo, useState, useRef } from 'react'
import type { QuerySpec } from '@/lib/api'
import { useFilters } from '@/components/providers/FiltersProvider'
import type { WidgetConfig } from '@/types/widgets'
import { FilterbarRuleControl } from '@/components/shared/FilterbarControl'
import { useKpiData } from '@/components/widgets/useKpiData'
import ErrorBoundary from '@/components/dev/ErrorBoundary'
import { getDefaultSeriesColors, getPresetPalette, tremorNameToHex } from '@/lib/chartUtils'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'

// Tremor chart blocks we need
const TremorDonutChart = dynamic(
  () => import('@tremor/react').then((m) => (m as any).DonutChart as any),
  { ssr: false }
) as any
const TremorProgressBar = dynamic(
  () => import('@tremor/react').then((m) => (m as any).ProgressBar as any),
  { ssr: false }
) as any
const TremorCategoryBar = dynamic(
  () => import('@tremor/react').then((m) => (m as any).CategoryBar as any),
  { ssr: false }
) as any
const LineChart: any = dynamic(() => import('@tremor/react').then(m => m.LineChart as any), { ssr: false })
const AreaChart: any = dynamic(() => import('@tremor/react').then(m => (m as any).AreaChart as any), { ssr: false })
const BarChart: any = dynamic(() => import('@tremor/react').then(m => (m as any).BarChart as any), { ssr: false })

export default function KpiCard({
  title,
  sql,
  datasourceId,
  suffix,
  queryMode = 'sql',
  querySpec,
  options,
  pivot,
  widgetId,
}: {
  title: string
  sql: string
  datasourceId?: string
  suffix?: string
  queryMode?: 'sql' | 'spec'
  querySpec?: QuerySpec
  options?: WidgetConfig['options']
  pivot?: WidgetConfig['pivot']
  widgetId?: string
}) {
  const { env } = useEnvironment()
  const { filters } = useFilters()
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
  // Respect global break-link per widget (disable applying global date filters)
  const ignoreGlobal = useMemo(() => {
    try { return !!(widgetId && typeof window !== 'undefined' && localStorage.getItem(`gf_break_${widgetId}`) === '1') } catch { return false }
  }, [widgetId])
  // React to break-link toggles so KPIs recompute
  const [breakSeq, setBreakSeq] = useState(0)
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string }
      if (!widgetId || !d?.widgetId || d.widgetId !== widgetId) return
      setBreakSeq((v) => v + 1)
    }
    if (typeof window !== 'undefined') window.addEventListener('global-filters-break-change', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('global-filters-break-change', handler as EventListener) }
  }, [widgetId])
  const [uiWhere, setUiWhere] = useState<Record<string, any>>({})
  const setUiWhereAndEmit = (patch: Record<string, any>) => {
    setUiWhere((prev) => {
      const next = { ...prev }
      Object.entries(patch).forEach(([k, v]) => { if (v === undefined) delete (next as any)[k]; else (next as any)[k] = v })
      return next
    })
    if (typeof window !== 'undefined' && widgetId) {
      try { window.dispatchEvent(new CustomEvent('chart-where-change', { detail: { widgetId, patch } } as any)) } catch {}
    }
  }

  // KPI labels parity with charts: drop series prefix when single-series and apply per-category casing
  const isSingleSeriesKpi = Array.isArray((querySpec as any)?.series) ? ((querySpec as any).series.length <= 1) : true
  const formatCategoryCaseKpi = (name: string): string => {
    const str = String(name ?? '')
    const map = (((options as any)?.categoryLabelCaseMap) || {}) as Record<string, string>
    const mode = (map[str] || (options as any)?.categoryLabelCase) as ('lowercase'|'capitalize'|'uppercase'|'capitalcase'|'proper'|undefined)
    if (!mode) return str
    switch (mode) {
      case 'lowercase': return str.toLowerCase()
      case 'uppercase':
      case 'capitalcase': return str.toUpperCase()
      case 'capitalize': { const lower = str.toLowerCase(); return lower.length ? (lower[0].toUpperCase()+lower.slice(1)) : lower }
      case 'proper': default:
        return str.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w?(w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
    }
  }
  const displayKpiLabel = (raw: string): string => {
    try {
      const s = String(raw ?? '')
      if (s.toLowerCase() === 'null') return 'None'
      const parts = s.includes(' • ')
        ? s.split(' • ')
        : (s.includes(' · ')
          ? s.split(' · ')
          : s.split(' • '))
      if (parts.length >= 2) {
        const base = parts[0]
        const cat = parts.slice(1).join(' • ')
        if (isSingleSeriesKpi) return formatCategoryCaseKpi(cat)
        return `${formatLabelCase(base)} • ${formatCategoryCaseKpi(cat)}`
      }
      return formatLabelCase(s)
    } catch { return String(raw ?? '') }
  }

  // Format X labels (used by spark tooltips). If x is a date-like value, format friendly; else stringify.
  const formatXLabel = (x: any): string => {
    try {
      const d = parseDateLoose(x)
      if (d) {
        const loc = options?.valueFormatLocale || undefined
        const showYear = !!((options as any)?.kpi?.sparkShowYear)
        const fmt = new Intl.DateTimeFormat(loc, { month: 'short', day: 'numeric', ...(showYear ? { year: 'numeric' as const } : {}) })
        return fmt.format(d)
      }
      return String(x ?? '')
    } catch { return String(x ?? '') }
  }

  // Helper: case-insensitive share lookup (handles None/null/empty)
  const normKey = (s: any) => String(s ?? '').trim().toLowerCase()
  const findSharePct = (share: Record<string, number> | undefined, key: string): number | undefined => {
    try {
      if (!share) return undefined
      const nk = normKey(key)
      const hit = Object.keys(share).find((kk) => normKey(kk) === nk)
      if (hit != null) return Number((share as any)[hit])
      // Fallbacks for empty/null semantics
      if (nk === '' || nk === 'none' || nk === 'null' || nk === 'undefined') {
        for (const alt of ['None', '', 'null', 'undefined']) {
          if ((share as any)[alt] != null) return Number((share as any)[alt])
        }
      }
      return undefined
    } catch { return undefined }
  }

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

  // Hydrate uiWhere from base where
  useEffect(() => {
    const baseWhere = ((querySpec as any)?.where || {}) as Record<string, any>
    setUiWhere((prev) => {
      const next = { ...prev }
      fieldsExposed.forEach((f) => {
        if (next[f] === undefined && Array.isArray(baseWhere[f]) && (baseWhere[f] as any[]).length > 0) next[f] = baseWhere[f]
        ;['gte','lte','gt','lt'].forEach((op) => { const k = `${f}__${op}`; if (next[k] === undefined && baseWhere[k] != null) (next as any)[k] = baseWhere[k] })
      })
      return next
    })
  }, [JSON.stringify((querySpec as any)?.where || {}), JSON.stringify(fieldsExposed)])

  function parseDateLoose(v: any): Date | null {
    if (v == null) return null
    if (v instanceof Date) return isNaN(v.getTime()) ? null : v
    const s = String(v).trim(); if (!s) return null
    if (/^\d{10,13}$/.test(s)) { const n = Number(s); const ms = s.length === 10 ? n*1000 : n; const d = new Date(ms); return isNaN(d.getTime())?null:d }
    const norm = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
    let d = new Date(norm); if (!isNaN(d.getTime())) return d
    const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/); if (iso) { d = new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`); return isNaN(d.getTime())?null:d }
    const m = s.match(/^([0-1]?\d)\/([0-3]?\d)\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/)
    if (m) { const mm=Number(m[1])-1, dd=Number(m[2]), yyyy=Number(m[3]); const t=m[4]||'00:00:00'; d = new Date(`${yyyy}-${String(mm+1).padStart(2,'0')}-${String(dd).padStart(2,'0')}T${t.length===5?t+':00':t}`); return isNaN(d.getTime())?null:d }
    return null
  }

  // Infer a date-like field from QuerySpec when options.deltaDateField is not set
  const [schemaDateField, setSchemaDateField] = useState<string | undefined>(undefined)
  const inferredDeltaField = useMemo((): string | undefined => {
    try {
      const pick = (name?: string) => {
        const s = String(name || '')
        return s && /(date|time|timestamp|created|updated|_at)$/i.test(s) ? s : undefined
      }
      const x = pick((querySpec as any)?.x)
      if (x) return x
      const y = pick((querySpec as any)?.y)
      if (y) return y
      const legend = pick((querySpec as any)?.legend)
      if (legend) return legend
      return schemaDateField
    } catch { return undefined }
  }, [querySpec, schemaDateField])

  // KPI-effective delta settings from configurator (kpi.* overrides top-level)
  type KPIDeltaMode = 'TD_YSTD' | 'TW_LW' | 'MONTH_LMONTH' | 'MTD_LMTD' | 'TY_LY' | 'YTD_LYTD' | 'TQ_LQ'
  const normalizeDeltaMode = (raw?: string): KPIDeltaMode | undefined => {
    const m = String(raw || '').toUpperCase()
    const allowed: Record<string, KPIDeltaMode> = {
      TD_YSTD: 'TD_YSTD',
      TW_LW: 'TW_LW',
      MONTH_LMONTH: 'MONTH_LMONTH',
      MTD_LMTD: 'MTD_LMTD',
      TY_LY: 'TY_LY',
      YTD_LYTD: 'YTD_LYTD',
      TQ_LQ: 'TQ_LQ',
    }
    return (allowed as any)[m] as KPIDeltaMode | undefined
  }
  const effectiveDeltaMode = useMemo(() => {
    const raw = ((options as any)?.kpi?.deltaMode ?? (options as any)?.deltaMode) as string | undefined
    return normalizeDeltaMode(raw)
  }, [options?.deltaMode, (options as any)?.kpi?.deltaMode])
  const effectiveDeltaUI = useMemo(() => ((options as any)?.kpi?.deltaUI ?? (options as any)?.deltaUI) as ('none'|'filterbar'|undefined), [ (options as any)?.kpi?.deltaUI, options?.deltaUI ])
  const effectiveDeltaDateField = useMemo(() => (((options as any)?.kpi?.deltaDateField) ?? (options as any)?.deltaDateField ?? inferredDeltaField) as string | undefined, [ (options as any)?.kpi?.deltaDateField, options?.deltaDateField, inferredDeltaField ])
  const effectiveDeltaWeekStart = useMemo(() => (((options as any)?.kpi?.deltaWeekStart) ?? (options as any)?.deltaWeekStart ?? env.weekStart) as ('sat'|'sun'|'mon'), [ (options as any)?.kpi?.deltaWeekStart, options?.deltaWeekStart, env.weekStart ])

  // Compute effective WHERE: always include global date window (even in SQL mode). For spec mode, also apply UI-as-truth for exposed fields.
  const effectiveWhere: Record<string, any> | undefined = useMemo(() => {
    const isSpec = (queryMode === 'spec' && !!querySpec)
    const base: Record<string, any> = isSpec ? { ...((querySpec as any)?.where || {}) } : {}
    // Use KPI-effective delta date field (from configurator) when available
    const df = (effectiveDeltaDateField as string | undefined)
    if (df && !ignoreGlobal) {
      if (filters.startDate) base[`${df}__gte`] = filters.startDate
      if (filters.endDate) {
        const d = new Date(`${filters.endDate}T00:00:00`)
        d.setDate(d.getDate() + 1)
        base[`${df}__lt`] = d.toISOString().slice(0, 10)
      }
    }
    if (!isSpec) return Object.keys(base).length ? base : undefined
    const eff: Record<string, any> = { ...base }
    const rmKeysFor = (f: string) => { delete eff[f]; delete eff[`${f}__gte`]; delete eff[`${f}__lte`]; delete eff[`${f}__gt`]; delete eff[`${f}__lt`] }
    fieldsExposed.forEach((f) => rmKeysFor(f))
    fieldsExposed.forEach((f) => {
      const val = (uiWhere as any)[f]
      const gte = (uiWhere as any)[`${f}__gte`]
      const lte = (uiWhere as any)[`${f}__lte`]
      const gt = (uiWhere as any)[`${f}__gt`]
      const lt = (uiWhere as any)[`${f}__lt`]
      if (Array.isArray(val) && val.length > 0) eff[f] = val
      if (gte != null) eff[`${f}__gte`] = gte
      if (lte != null) eff[`${f}__lte`] = lte
      if (gt != null) eff[`${f}__gt`] = gt
      if (lt != null) eff[`${f}__lt`] = lt
    })
    // Normalize end boundary for the mapped date field: support __lte and bare YYYY-MM-DD __lt as inclusive end date
    if (df) {
      const lteRaw = eff[`${df}__lte`]
      if (typeof lteRaw === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(lteRaw)) {
        const d = new Date(`${lteRaw}T00:00:00`)
        if (!isNaN(d.getTime())) { d.setDate(d.getDate() + 1); eff[`${df}__lt`] = d.toISOString().slice(0, 10) }
        delete eff[`${df}__lte`]
      }
      const ltRaw = eff[`${df}__lt`]
      if (typeof ltRaw === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(ltRaw)) {
        const d = new Date(`${ltRaw}T00:00:00`)
        if (!isNaN(d.getTime())) { d.setDate(d.getDate() + 1); eff[`${df}__lt`] = d.toISOString().slice(0, 10) }
      }
    }
    return eff
  }, [queryMode, querySpec, effectiveDeltaDateField, filters.startDate, filters.endDate, uiWhere, fieldsExposed, ignoreGlobal, breakSeq])

  // Debug: log current KPI configuration and effective filters
  useEffect(() => {
    try {
      console.log('[KPICardDebug] config', {
        widgetId, title, queryMode, datasourceId,
        source: (querySpec as any)?.source,
        options: { deltaMode: options?.deltaMode, deltaDateField: options?.deltaDateField, preset: (options as any)?.kpi?.preset },
        specFields: {
          x: (querySpec as any)?.x, y: (querySpec as any)?.y, legend: (querySpec as any)?.legend,
          measure: (querySpec as any)?.measure, agg: (querySpec as any)?.agg, groupBy: (querySpec as any)?.groupBy,
        },
        effectiveWhere,
      })
    } catch {}
  }, [widgetId, title, queryMode, datasourceId, querySpec, JSON.stringify(effectiveWhere), options?.deltaMode, options?.deltaDateField, (options as any)?.kpi?.preset])

  // Distincts helper for string
  function useDistinctStrings(source?: string, datasourceId?: string, baseWhere?: Record<string, any>) {
    const [cache, setCache] = useState<Record<string, string[]>>({})
    // Reset cache when constraints change
    useEffect(() => { setCache({}) }, [source, datasourceId, JSON.stringify(baseWhere || {})])
    const load = async (field: string) => {
      try {
        if (!source) return
        const omit = { ...(baseWhere || {}) }
        delete (omit as any)[field]
        const spec: any = { source, select: [field], where: Object.keys(omit).length ? omit : undefined, limit: 1000, offset: 0 }
        const res = await (await import('@/lib/api')).QueryApi.querySpec({ spec, datasourceId, limit: 1000, offset: 0, includeTotal: false })
        const cols = (res.columns || []) as string[]
        const idx = Math.max(0, cols.indexOf(field))
        const setVals = new Set<string>()
        ;(res.rows || []).forEach((row: any) => { const v = Array.isArray(row) ? row[idx] : (row?.[field]); if (v != null) setVals.add(String(v)) })
        setCache((prev) => ({ ...prev, [field]: Array.from(setVals.values()).sort() }))
      } catch { setCache((prev) => ({ ...prev, [field]: [] })) }
    }
    return { cache, load }
  }
  const { cache: distinctCache, load: loadDistinct } = useDistinctStrings((querySpec as any)?.source, datasourceId, effectiveWhere)

  const kpiOptions = useMemo(() => ({
    ...(options || {}),
    deltaMode: effectiveDeltaMode,
    deltaUI: effectiveDeltaUI,
    deltaDateField: effectiveDeltaDateField,
    deltaWeekStart: effectiveDeltaWeekStart,
  }), [options, effectiveDeltaMode, effectiveDeltaUI, effectiveDeltaDateField, effectiveDeltaWeekStart])
  // For KPI deltas, avoid forcing a fixed window via effectiveWhere date bounds; let the configured mode determine windows
  const effectiveWhereKPI: Record<string, any> | undefined = useMemo(() => {
    try {
      const src = effectiveWhere
      if (!src) return undefined
      if (!effectiveDeltaMode || !effectiveDeltaDateField) return src
      const df = effectiveDeltaDateField
      const out: Record<string, any> = { ...src }
      delete out[df]; delete out[`${df}__gte`]; delete out[`${df}__lte`]; delete out[`${df}__gt`]; delete out[`${df}__lt`]
      return Object.keys(out).length ? out : undefined
    } catch { return effectiveWhere }
  }, [effectiveWhere, effectiveDeltaMode, effectiveDeltaDateField])
  const kpi = useKpiData({ title, datasourceId, querySpec: querySpec as QuerySpec, options: kpiOptions, effectiveWhere: effectiveWhereKPI, visible })

  // Debug: track kpi state
  useEffect(() => {
    try {
      console.log('[KPICardDebug] kpi state', { isLoading: kpi.isLoading, error: kpi.error, hasData: !!kpi.data, data: kpi.data })
    } catch {}
  }, [kpi.isLoading, kpi.error, kpi.data])

  // (moved below baseValue)

  // Validate that the configured deltaDateField exists in the source; if not, disable delta behavior
  const [deltaFieldValid, setDeltaFieldValid] = useState<boolean>(false)
  useEffect(() => {
    let ignore = false
    async function run() {
      try {
        const src = (querySpec as any)?.source as string | undefined
        const df = effectiveDeltaDateField
        if (!src || !df) { if (!ignore) setDeltaFieldValid(false); return }
        const schema = datasourceId ? await (await import('@/lib/api')).Api.introspect(datasourceId) : await (await import('@/lib/api')).Api.introspectLocal()
        const parts = (src || '').split('.')
        const tbl = parts.pop() || ''
        const sch = parts.join('.')
        const schObj = (schema as any)?.schemas?.find((s: any) => s.name === sch)
        const tblObj = schObj?.tables?.find((t: any) => t.name === tbl)
        const cols: Array<{ name: string; type?: string | null }> = tblObj?.columns || []
        const names = new Set(cols.map(c => c.name))
        try { console.log('[KPICardDebug] deltaFieldValid check', { src, df, found: names.has(df as string), columns: cols.map(c => c.name) }) } catch {}
        if (!ignore) setDeltaFieldValid(names.has(df))
      } catch { if (!ignore) setDeltaFieldValid(false) }
    }
    void run()
    return () => { ignore = true }
  }, [datasourceId, (querySpec as any)?.source, effectiveDeltaDateField])

  // If no configured deltaDateField, try to infer from schema types / common names
  useEffect(() => {
    let ignore = false
    async function run() {
      try {
        if (effectiveDeltaDateField) { if (!ignore) setSchemaDateField(undefined); return }
        const src = (querySpec as any)?.source as string | undefined
        if (!src) { if (!ignore) setSchemaDateField(undefined); return }
        const schema = datasourceId ? await (await import('@/lib/api')).Api.introspect(datasourceId) : await (await import('@/lib/api')).Api.introspectLocal()
        const parts = (src || '').split('.')
        const tbl = parts.pop() || ''
        const sch = parts.join('.')
        const schObj = (schema as any)?.schemas?.find((s: any) => s.name === sch)
        const tblObj = schObj?.tables?.find((t: any) => t.name === tbl)
        const cols: Array<{ name: string; type?: string | null }> = tblObj?.columns || []
        // Prefer common names
        const byNamePrefer = ['created_at','updated_at','timestamp','event_time','date']
        const byName = cols.find(c => byNamePrefer.includes(c.name.toLowerCase()))
        const byType = cols.find(c => /date|time|timestamp/i.test(String(c.type || '')))
        const pick = byName?.name || byType?.name
        if (!ignore) setSchemaDateField(pick)
        try { console.log('[KPICardDebug] inferred schema date field', { pick, cols }) } catch {}
      } catch { if (!ignore) setSchemaDateField(undefined) }
    }
    void run()
    return () => { ignore = true }
  }, [datasourceId, (querySpec as any)?.source, options?.deltaDateField])

  // Fallback: base value when delta not configured or invalid date field
  const deltaModeOn = !!effectiveDeltaMode
  const deltaEnabled = deltaModeOn && !!(querySpec as any)?.source && deltaFieldValid
  const [baseValue, setBaseValue] = useState<string | number | undefined>(undefined)
  useEffect(() => {
    let ignore = false
    async function run() {
      if (deltaEnabled) { try { console.log('[KPICardDebug] base fallback skipped (deltaEnabled=true)') } catch {}; return }
      try {
        if (queryMode === 'spec' && querySpec) {
          const source = (querySpec as any)?.source as string | undefined
          if (!source) { if (!ignore) setBaseValue(undefined); return }
          const y = (querySpec as any)?.y as string | undefined
          const measure = (querySpec as any)?.measure as string | undefined
          const agg = (((querySpec as any)?.agg) || (y ? 'sum' : 'count')) as any
          const groupBy = (querySpec as any)?.groupBy
          const spec: any = { source, where: effectiveWhere, agg }
          if (y) spec.y = y
          if (measure) spec.measure = measure
          // Do not include groupBy here; we want a single total value
          try { console.log('[KPICardDebug] base fallback spec', spec) } catch {}
          const res = await QueryApi.querySpec({ spec, datasourceId, limit: 1000, offset: 0, includeTotal: false })
          const cols = (res?.columns as string[] | undefined) || []
          const rows = (res?.rows as any[]) || []
          let val: number | undefined = undefined
          if (Array.isArray(rows) && rows.length > 0) {
            const first = rows[0]
            if (Array.isArray(first)) {
              const len = first.length
              if (len === 1) {
                val = Number(first[0] ?? 0)
              } else {
                const valueIdx = cols.includes('value') ? Math.max(0, cols.indexOf('value')) : (len === 2 ? 1 : (len - 1))
                let sum = 0
                rows.forEach((r) => { const v = Number(r[valueIdx] ?? 0); if (!isNaN(v)) sum += v })
                val = sum
              }
            } else if (typeof first === 'number') {
              val = Number(first)
            }
          }
          try { console.log('[KPICardDebug] base fallback result', { cols, rowCount: rows.length, val }) } catch {}
          if (!ignore) setBaseValue(val)
        } else {
          try { console.log('[KPICardDebug] base fallback sql', { sql }) } catch {}
          const params: any = ignoreGlobal ? {} : { ...(filters || {}) }
          if (!ignoreGlobal && filters?.startDate) params.start = filters.startDate
          if (!ignoreGlobal && filters?.endDate) params.end = filters.endDate
          // Also pass effectiveWhere so SQL can bind specific constraints like :OrderDate__gte
          Object.assign(params, (effectiveWhere || {}))
          // And pass UI chips too (string/date/number filters exposed in the tile)
          Object.assign(params, (uiWhere || {}))
          const wid = String(widgetId || title || 'kpi')
          const { requestId: __rid, promise: __p } = Api.queryForWidget(
            wid,
            {
              sql,
              datasourceId,
              limit: 1,
              offset: 0,
              includeTotal: false,
              params,
              preferLocalDuck: (options as any)?.preferLocalDuck,
              preferLocalTable: ((querySpec as any)?.source as string | undefined),
            }
          )
          const res = await __p
          if (!RequestGuard.isLatest(wid, __rid)) return
          const cell = res?.rows?.[0]?.[0]
          try { console.log('[KPICardDebug] base fallback sql result', { cell }) } catch {}
          if (!ignore) setBaseValue(cell as any)
        }
      } catch (e) {
        try { console.log('[KPICardDebug] base fallback error', e) } catch {}
        if (!ignore) setBaseValue(undefined)
      }
    }
    void run()
    return () => { ignore = true }
  }, [deltaEnabled, queryMode, querySpec, JSON.stringify(effectiveWhere), datasourceId, sql, filters, ignoreGlobal, breakSeq])

  // Signal to embed snapshot that visible data is ready (after baseValue is in scope)
  const readyFiredRef = useRef(false)
  useEffect(() => {
    try {
      const hasData = (!!kpi.data) || (baseValue !== undefined)
      const done = !kpi.isLoading && hasData
      if (done && !readyFiredRef.current) {
        readyFiredRef.current = true
        if (typeof window !== 'undefined') {
          try { window.dispatchEvent(new CustomEvent('widget-data-ready')) } catch {}
        }
      }
    } catch {}
  }, [kpi.isLoading, !!kpi.data, baseValue])

  // Value-only per-legend totals when delta is OFF
  const [baseByLegend, setBaseByLegend] = useState<Record<string, number>>({})
  useEffect(() => {
    let ignore = false
    async function run() {
      try {
        if (deltaEnabled) { if (!ignore) setBaseByLegend({}); return }
        if (queryMode !== 'spec' || !querySpec) { if (!ignore) setBaseByLegend({}); return }
        const source = (querySpec as any)?.source as string | undefined
        const legendRaw = (querySpec as any)?.legend as (string | string[] | undefined)
        const y = (querySpec as any)?.y as string | undefined
        const measure = (querySpec as any)?.measure as string | undefined
        const agg = (((querySpec as any)?.agg) || (y ? 'sum' : 'count')) as any
        const legend = Array.isArray(legendRaw) ? (legendRaw[0] as string | undefined) : (legendRaw as string | undefined)
        if (!source || !legend) { if (!ignore) setBaseByLegend({}); return }
        // Build spec that groups by the legend as X (no time dimension)
        const spec: any = { source, where: effectiveWhere, x: legend, agg }
        if (y) spec.y = y
        if (measure) spec.measure = measure
        const r = await QueryApi.querySpec({ spec, datasourceId, limit: 1000, offset: 0, includeTotal: false })
        const cols = (r?.columns || []) as string[]
        const legIdx = cols.includes(legend) ? cols.indexOf(legend) : (cols.includes('x') ? cols.indexOf('x') : 0)
        const valIdx = cols.includes('value') ? cols.indexOf('value') : (cols.length > 1 ? cols.length - 1 : 1)
        const map = new Map<string, number>()
        ;(r?.rows || []).forEach((row: any) => {
          const arr = Array.isArray(row) ? row : []
          const raw = arr[legIdx]
          const name = (raw === null || raw === undefined || String(raw).trim() === '') ? 'None' : String(raw)
          const num = Number(arr[valIdx] ?? 0)
          const v = isNaN(num) ? 0 : num
          map.set(name, (map.get(name) || 0) + v)
        })
        if (!ignore) setBaseByLegend(Object.fromEntries(map.entries()))
      } catch {
        if (!ignore) setBaseByLegend({})
      }
    }
    void run()
    return () => { ignore = true }
  }, [deltaEnabled, queryMode, querySpec, JSON.stringify(effectiveWhere), datasourceId])

  // Value formatting (expanded)
  const formatNumber = (n: number | string | undefined) => {
    if (n == null) return '-'
    const num = Number(n) || 0
    const fmt = options?.yAxisFormat || 'none'
    switch (fmt) {
      // Back-compat aliases
      case 'short':
      case 'abbrev': return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 2 }).format(num)
        return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(num)
      case 'currency': {
        const cur = options?.valueCurrency || 'USD'
        const loc = options?.valueFormatLocale || undefined
        try { return new Intl.NumberFormat(loc, { style: 'currency', currency: cur, maximumFractionDigits: 2 }).format(num) } catch { return `$${num.toFixed(2)}` }
      }
      case 'percent': {
        const v = Math.abs(num) <= 1 ? num * 100 : num
        return `${v.toFixed(1)}%`
      }
      case 'bytes':
        return `${num.toLocaleString()}`

      // New numeric styles
      case 'wholeNumber':
        return Math.round(num).toLocaleString()
      case 'number':
        return num.toLocaleString(undefined, { maximumFractionDigits: 2 })
      case 'oneDecimal':
        return num.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })
      case 'twoDecimals':
        return num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
      case 'thousands':
        return `${(num / 1000).toLocaleString(undefined, { maximumFractionDigits: 2 })}K`
      case 'millions':
        return `${(num / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}M`
      case 'billions':
        return `${(num / 1_000_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}B`

      // New percent styles
      case 'percentWhole': {
        const v = Math.abs(num) <= 1 ? num * 100 : num
        return `${Math.round(v)}%`
      }
      case 'percentOneDecimal': {
        const v = Math.abs(num) <= 1 ? num * 100 : num
        return `${v.toFixed(1)}%`
      }

      // New time and distance styles
      case 'timeHours': {
        const abs = Math.abs(num); const withDec = abs % 1 !== 0
        return withDec ? `${num.toFixed(1)}h` : `${Math.round(num)}h`
      }
      case 'timeMinutes': {
        const abs = Math.abs(num); const withDec = abs % 1 !== 0
        return withDec ? `${num.toFixed(1)}m` : `${Math.round(num)}m`
      }
      case 'distance-km': {
        const abs = Math.abs(num); const digits = abs >= 100 ? 0 : 1
        return `${num.toFixed(digits)} km`
      }
      case 'distance-mi': {
        const abs = Math.abs(num); const digits = abs >= 100 ? 0 : 1
        return `${num.toFixed(digits)} mi`
      }

      case 'none': default:
        return num.toLocaleString()
    }
  }

  const downIsGood = !!options?.kpi?.downIsGood
  const sparkType = ((options?.kpi as any)?.sparkType || 'line') as 'line'|'area'|'bar'
  const deltaColor = (pct: number) => {
    const n = Number(pct)
    if (!isFinite(n) || Math.abs(n) < 1e-9) return 'text-muted-foreground'
    const pos = n > 0
    const good = downIsGood ? !pos : pos
    return good ? 'text-emerald-600' : 'text-rose-600'
  }
  // Spark-only: unconditional sign-based color (ignore downIsGood)
  const deltaColorSpark = (pct: number) => {
    const n = Number(pct)
    if (!isFinite(n) || Math.abs(n) < 1e-9) return 'text-gray-500'
    return n > 0 ? 'text-emerald-600' : 'text-rose-600'
  }
  const deltaBadge = (pct: number) => {
    const n = Number(pct)
    if (!isFinite(n) || Math.abs(n) < 1e-9) return 'bg-gray-100 text-gray-700 ring-1 ring-inset ring-gray-600/10'
    const pos = n > 0
    const good = downIsGood ? !pos : pos
    return good ? 'bg-emerald-50 text-emerald-700 ring-1 ring-inset ring-emerald-600/10' : 'bg-rose-50 text-rose-700 ring-1 ring-inset ring-rose-600/10'
  }

  // Spark tooltip (dark/light aware)
  const SparkTooltip = ({ payload, active, label }: any) => {
    if (!active || !payload || !Array.isArray(payload) || payload.length === 0) return null
    const p = payload[0]
    const v = Number(p?.value ?? 0)
    const name = String(label ?? (p?.payload?.xLabel ?? ''))
    return (
      <div className="pointer-events-none rounded-md border bg-popover text-popover-foreground shadow-sm px-2 py-1 text-[11px]">
        <div className="font-medium leading-tight">{name}</div>
        <div className="opacity-80">{formatNumber(v)}</div>
      </div>
    )
  }

  // Label casing for category names (lowercase/capitalize/proper). Default: proper
  type LabelCase = 'lowercase' | 'capitalize' | 'uppercase' | 'capitalcase' | 'proper'
  const labelCase: LabelCase = ((options as any)?.kpi?.labelCase || 'proper') as LabelCase
  const formatLabelCase = (s: string): string => {
    const str = String(s ?? '')
    switch (labelCase) {
      case 'lowercase':
        return str.toLowerCase()
      case 'uppercase':
      case 'capitalcase':
        return str.toUpperCase()
      case 'capitalize': {
        const lower = str.toLowerCase()
        return lower.length ? lower[0].toUpperCase() + lower.slice(1) : lower
      }
      case 'proper':
      default:
        return str
          .replace(/[_-]+/g, ' ')
          .split(/\s+/)
          .map(w => w ? (w[0].toUpperCase() + w.slice(1).toLowerCase()) : w)
          .join(' ')
    }
  }

  // Sparkline data for 'spark' preset (current period only)
  type SparkPoint = { x: any; xLabel: string; value: number }
  const [sparkData, setSparkData] = useState<Array<SparkPoint>>([])
  const [sparkByLegend, setSparkByLegend] = useState<Record<string, Array<SparkPoint>>>({})
  useEffect(() => {
    let ignore = false
    async function run() {
      try {
        if ((options?.kpi?.preset !== 'spark')) return
        const source = (querySpec as any)?.source as string | undefined
        const rawX = (querySpec as any)?.x as any
        const x = (Array.isArray(rawX) ? (rawX[0] as any) : rawX) as string | undefined
        const y = (querySpec as any)?.y as string | undefined
        const agg = (((querySpec as any)?.agg) || (y ? 'sum' : 'count')) as any
        if (!source || !x || !y) { if (!ignore) { setSparkData([]); setSparkByLegend({}); } return }
        // Resolve current period and constrain where
        const mode = (effectiveDeltaMode || 'TD_YSTD') as any
        const res = await Api.resolvePeriods({ mode, tzOffsetMinutes: (typeof window !== 'undefined') ? new Date().getTimezoneOffset() : 0, weekStart: (effectiveDeltaWeekStart || env.weekStart) as any })
        const df = (effectiveDeltaDateField || inferredDeltaField)
        const where: Record<string, any> = { ...(effectiveWhere || {}) }
        const hasWindow = df ? ((`${df}__gte` in where) || (`${df}__lt` in where)) : false
        if (df && !hasWindow) {
          where[`${df}__gte`] = res.curStart
          where[`${df}__lt`] = res.curEnd
        }
        const legendField = (querySpec as any)?.legend as string | undefined
        const gb = (querySpec as any)?.groupBy
        // If legend present, ask server for legend-split aggregation [x, legend, value]
        if (legendField) {
          const spec: any = { source, where, x, y, agg, legend: legendField, groupBy: gb }
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[KPICardDebug] spark legend spec', { x: spec.x, y: spec.y, agg: spec.agg, legend: spec.legend, groupBy: spec.groupBy }) } catch {}
          }
          const r = await QueryApi.querySpec({ spec, datasourceId, limit: 1000, offset: 0, includeTotal: false })
          const rows: any[] = (r?.rows as any[]) || []
          const by: Record<string, Array<SparkPoint>> = {}
          rows.forEach((row: any[]) => {
            const xv = row?.[0]
            const rawLeg = row?.[1]
            const lv = (rawLeg === null || rawLeg === undefined || String(rawLeg).trim() === '') ? 'None' : String(rawLeg)
            const vv = Number(row?.[2] ?? 0)
            if (!by[lv]) by[lv] = []
            by[lv].push({ x: xv, xLabel: formatXLabel(xv), value: Number.isFinite(vv) ? vv : 0 })
          })
          if (!ignore) { setSparkData([]); setSparkByLegend(by) }
        } else {
          // Single series spark [x, value]
          const spec: any = { source, where, x, y, agg, groupBy: gb }
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[KPICardDebug] spark single spec', { x: spec.x, y: spec.y, agg: spec.agg, groupBy: spec.groupBy }) } catch {}
          }
          const r = await QueryApi.querySpec({ spec, datasourceId, limit: 1000, offset: 0, includeTotal: false })
          const rows: any[] = (r?.rows as any[]) || []
          const data: SparkPoint[] = rows.map((row) => ({ x: row?.[0], xLabel: formatXLabel(row?.[0]), value: Number(row?.[1] ?? 0) }))
          if (!ignore) { setSparkByLegend({}); setSparkData(data) }
        }
      } catch {
        if (!ignore) { setSparkData([]); setSparkByLegend({}) }
      }
    }
    void run()
    return () => { ignore = true }
  }, [options?.kpi?.preset, querySpec, JSON.stringify(effectiveWhere), datasourceId, effectiveDeltaMode, effectiveDeltaDateField, inferredDeltaField, effectiveDeltaWeekStart, env.weekStart])

  // Choose displayed value/delta for presets Basic/Badge
  const displayed = useMemo(() => {
    const legend = (querySpec as any)?.legend as string | undefined
    const data = kpi.data
    // If KPI delta query didn't run (delta disabled) or hasn't produced data, use baseValue fallback
    if (!data) return { value: baseValue as any, pct: undefined as number | undefined }
    // Legend present: prefer category totals if present; otherwise, fall back to overall totals (no legend)
    if (legend) {
      const byHasKeys = !!data.byLegend && Object.keys(data.byLegend || {}).length > 0
      if (byHasKeys && data.totals) return { value: data.totals.current, pct: data.totals.percentChange }
      if (data.overall) return { value: data.overall.current, pct: data.overall.percentChange }
    }
    // Multi-series totals
    if (data.bySeries && data.totals) return { value: data.totals.current, pct: data.totals.percentChange }
    // Single-value
    if (data.single) return { value: data.single.current, pct: data.single.percentChange }
    // Fallback: if no delta data is available (e.g., no valid date field), use baseValue
    return { value: baseValue, pct: undefined }
  }, [kpi.data, querySpec, baseValue])

  // Debug: log final displayed numbers
  useEffect(() => {
    try { console.log('[KPICardDebug] displayed', { displayed, deltaEnabled, baseValue, hasKpiData: !!kpi.data }) } catch {}
  }, [displayed, deltaEnabled, baseValue, kpi.data])

  // Ensure KPI numeric value uses neutral foreground (black) and scale responsively via container queries
  const metricClass = 'text-foreground font-semibold [font-size:var(--kpi-fs)] leading-[1.1]'
  // Shared typography classes
  const labelClass = 'text-[var(--kpi-tile-label)] !text-gray-500 dark:!text-gray-400 font-normal'
  const pctTextClass = 'text-xs'
  const totalTextClass = 'text-xs text-muted-foreground'

  const autoFit = options?.autoFitCardContent !== false
  const cardFill = options?.cardFill || 'default'
  const bgStyle = cardFill === 'transparent' ? { backgroundColor: 'transparent' } : cardFill === 'custom' ? { backgroundColor: options?.cardCustomColor || '#ffffff' } : undefined
  const containerStyle: any = {
    ...(bgStyle as any),
    // Enable container query units (cqw) for responsive type
    containerType: 'inline-size' as any,
    // Expose CSS vars for sizes with reasonable fallbacks
    ['--kpi-fs' as any]: 'clamp(20px, 2.6cqw, 36px)',
    ['--kpi-tile-label' as any]: 'clamp(12px, 1.4cqw, 14px)',
    ['--kpi-pct' as any]: 'clamp(12px, 1.5cqw, 14px)',
  }
  const cardClass = `${autoFit ? '' : 'h-full'} !border-0 shadow-none rounded-lg ${cardFill === 'transparent' ? 'bg-transparent' : 'bg-card'}`

  // Observe container resize to trigger Tremor charts re-mount for proper sizing (spark, etc.)
  const [sizeKey, setSizeKey] = useState<number>(0)
  useEffect(() => {
    const ro = new (typeof ResizeObserver !== 'undefined' ? ResizeObserver : (class { observe(){} disconnect(){} } as any))((entries: any) => {
      try { setSizeKey((k) => k + 1) } catch {}
    })
    if (containerRef.current) ro.observe(containerRef.current)
    const onWinResize = () => { try { setSizeKey((k) => k + 1) } catch {} }
    if (typeof window !== 'undefined') window.addEventListener('resize', onWinResize)
    return () => { try { ro.disconnect() } catch {}; if (typeof window !== 'undefined') window.removeEventListener('resize', onWinResize) }
  }, [])

  return (
    <ErrorBoundary name="KpiCard">
    <div className={cardClass} style={containerStyle} ref={containerRef}>
      {(fieldsExposed.length > 0) && (
        <div className="flex flex-wrap items-center justify-end gap-2 mb-1">
          {fieldsExposed.map((field) => {
            if (!distinctCache[field]) { void loadDistinct(field) }
            const sample = (distinctCache[field] || []).slice(0, 12)
            let kind: 'string'|'number'|'date' = 'string'
            const numHits = sample.filter((s) => Number.isFinite(Number(s))).length
            const dateHits = sample.filter((s) => { const d = parseDateLoose(s); return !!d }).length
            if (dateHits >= Math.max(1, Math.ceil(sample.length/2))) kind = 'date'
            else if (numHits >= Math.max(1, Math.ceil(sample.length/2))) kind = 'number'
            const baseWhere = ((querySpec as any)?.where || {}) as Record<string, any>
            const sel = uiWhere[field] as any
            const label = (() => {
              if (kind === 'string') {
                const arr: any[] = Array.isArray(sel) ? sel : (Array.isArray(baseWhere[field]) ? baseWhere[field] : [])
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
      )}
      {kpi.isLoading ? (
        <div className="mt-2 space-y-2 animate-pulse">
          <div className="h-6 bg-muted rounded w-2/3" />
          <div className="h-4 bg-muted rounded w-1/3" />
        </div>
      ) : kpi.error ? (
        <Metric className="text-red-600">Error</Metric>
      ) : (
        <div className="space-y-1">
          {/* Basic-family presets; also supports multi-tile when Legend present */}
          {(() => {
            const preset = options?.kpi?.preset || 'basic'
            const vStr = formatNumber(displayed.value)
            const pct = displayed.pct
            const pctStr = pct == null ? undefined : `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`
            const legendField = (querySpec as any)?.legend as string | undefined
            const legendFilterVals = legendField ? (effectiveWhere as any)?.[legendField] : undefined
            const isSingleLegendSelected = Array.isArray(legendFilterVals) && legendFilterVals.length === 1
            // If a legend exists and no single legend value is selected, render multiple KPI tiles (Top N)
            if (
              legendField &&
              !isSingleLegendSelected &&
              (preset === 'basic' || preset === 'badge' || preset === 'withPrevious') &&
              (
                (deltaEnabled && kpi.data?.byLegend && Object.keys(kpi.data.byLegend || {}).length > 0) ||
                (!deltaEnabled && Object.keys(baseByLegend || {}).length > 0)
              )
            ) {
              const topN = (typeof options?.kpi?.topN === 'number' ? options.kpi.topN : 3) || 3
              const entriesBase = deltaEnabled
                ? Object.entries(kpi.data?.byLegend || {})
                : Object.entries(baseByLegend || {}).map(([k, v]) => [k, { current: Number(v||0), previous: 0, absoluteDelta: 0, percentChange: 0 }] as any)
              const entries = entriesBase
                .sort((a:any,b:any) => Number((b[1] as any)?.current||0) - Number((a[1] as any)?.current||0))
                .slice(0, topN)
              const wrapEvery = (typeof (options as any)?.kpi?.wrapEveryN === 'number' && (options as any).kpi.wrapEveryN > 0) ? (options as any).kpi.wrapEveryN : 3
              const cols = Math.max(1, Math.min(wrapEvery, entries.length || 1))
              const curTotalAll = deltaEnabled
                ? Number(((kpi.data as any)?.overall?.current ?? kpi.data?.totals?.current) || 0)
                : entries.reduce((acc: number, [,v]: any) => acc + Number((v?.current)||0), 0)
              return (
                <div className="grid gap-[clamp(8px,1.4vw,12px)]" style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}>
                  {entries.map(([name, v]: any) => {
                    const pctLocal = Number(v?.percentChange || 0)
                    const pctLocalStr = `${pctLocal >= 0 ? '+' : ''}${Number(pctLocal||0).toFixed(1)}%`
                    if (preset === 'badge') {
                      return (
                        <div key={name} className="rounded-lg border bg-card p-[clamp(10px,1.3vw,14px)]">
                          <div className={`${labelClass} mb-1 truncate`} title={name}>{displayKpiLabel(name)}</div>
                          <div className="flex items-start justify-between">
                            <div className="flex items-center gap-2">
                              <Metric className={metricClass}>{formatNumber(v.current)}{suffix ? <span className="ml-1 text-base align-super">{suffix}</span> : null}</Metric>
                            </div>
                            {deltaEnabled && (
                              <span className={`ml-2 ${pctTextClass} px-2 py-0.5 rounded-md ${deltaBadge(pctLocal || 0)}`}>{pctLocalStr}</span>
                            )}
                          </div>
                        </div>
                      )
                    }
                    if (preset === 'withPrevious') {
                      return (
                        <div key={name} className="rounded-lg border bg-card p-[clamp(10px,1.3vw,14px)]">
                          <div className={`${labelClass} mb-1 truncate`} title={name}>{displayKpiLabel(name)}</div>
                          <div className="leading-tight">
                            <Metric className={metricClass}>{formatNumber(v.current)}{suffix ? <span className="ml-1 text-sm align-super">{suffix}</span> : null}</Metric>
                            <div className={`${totalTextClass}`}>of {formatNumber(curTotalAll)}</div>
                            {deltaEnabled && (
                              <div className={`${pctTextClass} mt-0.5 ${deltaColor(pctLocal || 0)}`}>{pctLocalStr}</div>
                            )}
                          </div>
                        </div>
                      )
                    }
                    // basic
                    return (
                      <div key={name} className="rounded-lg border bg-card p-[clamp(10px,1.3vw,14px)]">
                        <div className={`${labelClass} mb-1 truncate`} title={name}>{formatLabelCase(name)}</div>
                        <div className="flex items-center gap-2">
                          <Metric className={metricClass}>{formatNumber(v.current)}{suffix ? <span className="ml-1 text-base align-super">{suffix}</span> : null}</Metric>
                          {deltaEnabled && (
                            <span className={`${pctTextClass} ${deltaColor(pctLocal || 0)}`}>{pctLocalStr}</span>
                          )}
                        </div>
                      </div>
                    )
                  })}
                </div>
              )
            }
            // If delta is OFF but a legend exists and not filtered to a single value, show value-only tiles per-category
            if (!deltaEnabled && legendField && !isSingleLegendSelected) {
              const entries = (Object.entries(baseByLegend || {}) as Array<[string, number]>)
                .sort((a,b) => Number(b[1]||0) - Number(a[1]||0))
              const topN = (typeof options?.kpi?.topN === 'number' ? options.kpi.topN : 3) || 3
              const picked = entries.slice(0, topN)
              if (picked.length > 0 && preset === 'basic') {
                const wrapEvery = (typeof (options as any)?.kpi?.wrapEveryN === 'number' && (options as any).kpi.wrapEveryN > 0) ? (options as any).kpi.wrapEveryN : 3
                const cols = Math.max(1, Math.min(wrapEvery, picked.length || 1))
                return (
                  <div className="grid gap-[clamp(8px,1.4vw,12px)]" style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}>
                    {picked.map(([name, value]) => (
                      <div key={name} className="rounded-lg border bg-card p-[clamp(10px,1.3vw,14px)]">
                        <div className={`${labelClass} mb-1 truncate`} title={name}>{formatLabelCase(name)}</div>
                        <Metric className={metricClass}>{formatNumber(value)}{suffix ? <span className="ml-1 text-base align-super">{suffix}</span> : null}</Metric>
                      </div>
                    ))}
                  </div>
                )
              }
            }
            // Donut: per-category tiles when legend present; else single value vs total
            if (preset === 'donut') {
              const legendField = (querySpec as any)?.legend as string | undefined
              const legendFilterVals = legendField ? (effectiveWhere as any)?.[legendField] : undefined
              const isSingleLegendSelected = Array.isArray(legendFilterVals) && legendFilterVals.length === 1
              const topN = (typeof options?.kpi?.topN === 'number' ? options.kpi.topN : 3) || 3
              // Multi-category donut tiles
              if (legendField && ((deltaEnabled && kpi.data?.byLegend) || (!deltaEnabled && Object.keys(baseByLegend || {}).length > 0)) && !isSingleLegendSelected) {
                const entriesBase = deltaEnabled
                  ? Object.entries(kpi.data?.byLegend || {})
                  : Object.entries(baseByLegend || {}).map(([k, v]) => [k, { current: Number(v||0)}] as any)
                const entries = entriesBase
                  .sort((a:any,b:any) => Number((b[1] as any)?.current||0) - Number((a[1] as any)?.current||0))
                  .slice(0, topN) as Array<[string, any]>
                const totalNum = deltaEnabled
                  ? Number(((kpi.data as any)?.overall?.current ?? kpi.data?.totals?.current) || entries.reduce((acc, [,v]) => acc + Number((v?.current)||0), 0))
                  : entries.reduce((acc, [,v]) => acc + Number((v?.current)||0), 0)
                return (
                  <div className="grid gap-[clamp(8px,1.4vw,12px)]" style={{ gridTemplateColumns: `repeat(${Math.max(1, Math.min(3, entries.length))}, minmax(0, 1fr))` }}>
                    {entries.map(([name, v]) => {
                      const curr = Number((v?.current) || 0)
                      const rest = Math.max(0, totalNum - curr)
                      const shareCur: Record<string, number> | undefined = (kpi.data as any)?.share?.cur
                      const pctRaw = (deltaEnabled && shareCur) ? (findSharePct(shareCur, name) ?? (totalNum>0?((curr/totalNum)*100):0)) : (totalNum>0?((curr/totalNum)*100):0)
                      const percent = Number.isFinite(pctRaw) ? Math.max(0, Math.min(100, pctRaw)) : 0
                      const donutData = [ { name: 'value', value: curr }, { name: 'rest', value: rest } ]
                      return (
                        <div key={name} className="rounded-lg border bg-card p-[clamp(10px,1.3vw,14px)]">
                          <div className={`${labelClass} mb-1 truncate`} title={name}>{formatLabelCase(name)}</div>
                          <div className="flex items-center gap-3">
                            <div className="relative w-[72px] h-[72px]">
                              <TremorDonutChart data={donutData} index="name" category="value" showLabel={false} colors={['emerald','slate']} className="h-[72px]" />
                              <div className="absolute inset-0 flex items-center justify-center text-[12px] text-muted-foreground">{`${Math.round(percent)}%`}</div>
                            </div>
                            <div className="leading-tight">
                              <Metric className={metricClass}>{formatNumber(curr)}</Metric>
                              <div className={`${totalTextClass}`}>of {formatNumber(totalNum)}</div>
                            </div>
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )
              }
              // Single donut (no legend)
              const valueNum = deltaEnabled ? Number(displayed.value || 0) : Number(baseValue || 0)
              const totalsCurrent = Number(kpi.data?.totals?.current ?? 0)
              const fallbackSum = (() => {
                const map = (kpi.data?.byLegend || kpi.data?.bySeries) as Record<string, any> | undefined
                return map ? Object.values(map).reduce((acc, v: any) => acc + Number(v?.current || 0), 0) : valueNum
              })()
              const totalNum = deltaEnabled ? Number((kpi.data as any)?.overall?.current ?? totalsCurrent ?? fallbackSum) : (totalsCurrent > 0 ? totalsCurrent : fallbackSum)
              const rest = Math.max(0, totalNum - valueNum)
              const shareCur: Record<string, number> | undefined = (kpi.data as any)?.share?.cur
              const singleSelName = (Array.isArray(legendFilterVals) && legendFilterVals.length === 1) ? String(legendFilterVals[0]) : undefined
              const percentRaw = (deltaEnabled && shareCur && singleSelName != null && (shareCur as any)[singleSelName] != null) ? Number((shareCur as any)[singleSelName]) : (totalNum > 0 ? (valueNum / totalNum) * 100 : 0)
              const percent = Number.isFinite(percentRaw) ? Math.max(0, Math.min(100, percentRaw)) : 0
              const donutData = [ { name: 'value', value: valueNum }, { name: 'rest', value: rest } ]
              return (
                <div className="flex items-center gap-3">
                  <div className="relative w-[72px] h-[72px]">
                    <TremorDonutChart data={donutData} index="name" category="value" showLabel={false} colors={[ 'emerald', 'slate' ]} className="h-[72px]" />
                    <div className="absolute inset-0 flex items-center justify-center text-[12px] text-muted-foreground">{`${Math.round(percent)}%`}</div>
                  </div>
                  <div className="leading-tight">
                    <Metric className={metricClass}>{formatNumber(valueNum)}</Metric>
                    <div className={`${totalTextClass}`}>of {formatNumber(totalNum)}</div>
                    {deltaEnabled && pctStr != null && (<div className={`${pctTextClass} mt-0.5 ${deltaColor(displayed.pct || 0)}`}>{pctStr}</div>)}
                  </div>
                </div>
              )
            }
            // Progress: per-category tiles when legend present; else single progress vs total
            if (preset === 'progress') {
              const legendField = (querySpec as any)?.legend as string | undefined
              const legendFilterVals = legendField ? (effectiveWhere as any)?.[legendField] : undefined
              const isSingleLegendSelected = Array.isArray(legendFilterVals) && legendFilterVals.length === 1
              const topN = (typeof options?.kpi?.topN === 'number' ? options.kpi.topN : 3) || 3
              if (legendField && ((deltaEnabled && kpi.data?.byLegend) || (!deltaEnabled && Object.keys(baseByLegend || {}).length > 0)) && !isSingleLegendSelected) {
                const entriesBase = deltaEnabled
                  ? Object.entries(kpi.data?.byLegend || {})
                  : Object.entries(baseByLegend || {}).map(([k, v]) => [k, { current: Number(v||0)}] as any)
                const entries = entriesBase
                  .sort((a:any,b:any) => Number((b[1] as any)?.current||0) - Number((a[1] as any)?.current||0))
                  .slice(0, topN) as Array<[string, any]>
                const totalNum = deltaEnabled
                  ? Number(((kpi.data as any)?.overall?.current ?? kpi.data?.totals?.current) || entries.reduce((acc, [,v]) => acc + Number((v?.current)||0), 0))
                  : entries.reduce((acc, [,v]) => acc + Number((v?.current)||0), 0)
                return (
                  <div className="grid gap-[clamp(8px,1.4vw,12px)]" style={{ gridTemplateColumns: `repeat(${Math.max(1, Math.min(3, entries.length))}, minmax(0, 1fr))` }}>
                    {entries.map(([name, v], i) => {
                      const curr = Number((v?.current) || 0)
                      const shareCur: Record<string, number> | undefined = (kpi.data as any)?.share?.cur
                      const pctRaw = (deltaEnabled && shareCur) ? (findSharePct(shareCur, name) ?? (totalNum>0?((curr/totalNum)*100):0)) : (totalNum>0?((curr/totalNum)*100):0)
                      const pct = Number.isFinite(pctRaw) ? Math.max(0, Math.min(100, pctRaw)) : 0
                      return (
                        <div key={name} className="rounded-lg border bg-card p-[clamp(10px,1.3vw,14px)]">
                          <div className={`${labelClass} mb-1 truncate`} title={name}>{formatLabelCase(name)}</div>
                          <div className="flex items-center justify-between mb-1">
                            <Metric className={metricClass}>{formatNumber(curr)}</Metric>
                            <div className="text-[12px] text-muted-foreground">{formatNumber(totalNum)}</div>
                          </div>
                          <TremorProgressBar value={Math.max(0, Math.min(100, pct))} color={(options?.color || 'emerald') as any} />
                        </div>
                      )
                    })}
                  </div>
                )
              }
              // Single progress (no legend)
              const valueNum = deltaEnabled ? Number(displayed.value || 0) : Number(baseValue || 0)
              const totalsCurrent = Number(kpi.data?.totals?.current ?? 0)
              const fallbackSum = (() => {
                const map = (kpi.data?.byLegend || kpi.data?.bySeries) as Record<string, any> | undefined
                return map ? Object.values(map).reduce((acc, v: any) => acc + Number(v?.current || 0), 0) : valueNum
              })()
              const totalNum = deltaEnabled ? Number((kpi.data as any)?.overall?.current ?? totalsCurrent ?? fallbackSum) : (totalsCurrent > 0 ? totalsCurrent : fallbackSum)
              const shareCur: Record<string, number> | undefined = (kpi.data as any)?.share?.cur
              const singleSelName = (Array.isArray(legendFilterVals) && legendFilterVals.length === 1) ? String(legendFilterVals[0]) : undefined
              const percentRaw = (deltaEnabled && shareCur && singleSelName != null && (shareCur as any)[singleSelName] != null) ? Number((shareCur as any)[singleSelName]) : (totalNum > 0 ? (valueNum / totalNum) * 100 : 0)
              const percent = Number.isFinite(percentRaw) ? Math.max(0, Math.min(100, percentRaw)) : 0
              return (
                <div>
                  <div className="flex items-center justify-between mb-1">
                    <Metric className={metricClass}>{formatNumber(valueNum)}</Metric>
                    <div className="text-[12px] text-muted-foreground">{formatNumber(totalNum)}</div>
                  </div>
                  <TremorProgressBar value={Math.max(0, Math.min(100, percent))} color={(options?.color || 'emerald') as any} />
                  {deltaEnabled && pctStr != null && (<div className={`text-xs mt-1 ${deltaColor(displayed.pct || 0)}`}>{pctStr}</div>)}
                </div>
              )
            }
            // Spark: small trend line with colored stroke
            if (preset === 'spark') {
              const p = Number(displayed.pct || 0)
              const color = (p > 0) ? 'emerald' : (p < 0) ? 'rose' : 'slate'
              // Prefer sparkByLegend keys; if empty but legend data exists, still render per-category tiles
              const legends = (() => {
                const keys = Object.keys(sparkByLegend || {})
                if (keys.length > 0) return keys
                const by = (kpi.data?.byLegend || {}) as Record<string, any>
                return Object.keys(by)
              })()
              if (legends.length > 0) {
                // Multi-tile grid like Basic preset (Top N by current period)
                const sourceMap: Record<string, { current: number; previous: number; absoluteDelta: number; percentChange: number }> | undefined = (kpi.data?.byLegend || kpi.data?.bySeries)
                const entries = Object.entries(sourceMap || {})
                const sorted = entries.sort((a,b) => Number(b[1]?.current||0) - Number(a[1]?.current||0))
                const topN = (typeof options?.kpi?.topN === 'number' ? options.kpi.topN : 3) || 3
                const picked = sorted.slice(0, topN)
                const wrapEvery = (typeof (options as any)?.kpi?.wrapEveryN === 'number' && (options as any).kpi.wrapEveryN > 0) ? (options as any).kpi.wrapEveryN : 3
                const cols = Math.max(1, Math.min(wrapEvery, picked.length || 1))
                const palette = getPresetPalette((options?.colorPreset || 'default') as any)
                const lineColors = getDefaultSeriesColors(picked.length, palette) as any
                return (
                  <div className="grid gap-[clamp(8px,1.4vw,12px)]" style={{ gridTemplateColumns: `repeat(${cols}, minmax(0, 1fr))` }}>
                    {picked.map(([lg, v], i) => {
                      const pctLocal = Number(v?.percentChange || 0)
                      const pctLocalStr = isFinite(pctLocal) ? `${pctLocal >= 0 ? '+' : ''}${pctLocal.toFixed(1)}%` : ''
                      const currLocal = Number(v?.current || 0)
                      const prevVal = Number(((v as any)?.previous ?? 0) as number)
                      const deltaAbs = Number((((v as any)?.absoluteDelta) ?? (currLocal - prevVal)) ?? 0)
                      const deltaStr = `${deltaAbs >= 0 ? '+' : ''}${formatNumber(Math.abs(deltaAbs))}`
                      const series = (sparkByLegend[lg] || [])
                      const base = (Array.isArray(series) && series.length >= 2)
                        ? series
                        : ([
                          { x: 0, value: currLocal * 0.96 },
                          { x: 1, value: currLocal * 1.02 },
                          { x: 2, value: currLocal * 1.00 },
                        ])
                      const data: SparkPoint[] = base.map((d: any) => ({ x: d.x, xLabel: (d.xLabel ?? formatXLabel(d.x)), value: d.value }))
                      return (
                        <div key={lg} className="rounded-lg border bg-card p-[clamp(10px,1.3vw,14px)]">
                          <div className={`${labelClass} truncate mb-1`} title={String(lg)}>{displayKpiLabel(String(lg))}</div>
                          <div className="flex items-start justify-between">
                            <Metric className={`${metricClass}`}>{formatNumber(currLocal)}</Metric>
                            {deltaEnabled && (
                              <span className={`text-xs ${deltaColorSpark(pctLocal)}`}>{deltaStr} ({pctLocalStr})</span>
                            )}
                          </div>
                          <div className="w-full h-[48px] mt-1">
                            {sparkType === 'bar' ? (
                              <BarChart key={`spark-${sizeKey}-${i}`} data={data} index="xLabel" categories={["value"]} colors={[lineColors[i % lineColors.length]]} showLegend={false} showGridLines={false} showXAxis={false} showYAxis={false} valueFormatter={(v: number) => formatNumber(v as any)} className="h-[48px] text-[10px]" customTooltip={SparkTooltip as any} />
                            ) : sparkType === 'area' ? (
                              <AreaChart key={`spark-${sizeKey}-${i}`} data={data} index="xLabel" categories={["value"]} colors={[lineColors[i % lineColors.length]]} showLegend={false} showGridLines={false} showXAxis={false} showYAxis={false} showGradient={false} valueFormatter={(v: number) => formatNumber(v as any)} className="h-[48px] text-[10px]" customTooltip={SparkTooltip as any} />
                            ) : (
                              <LineChart key={`spark-${sizeKey}-${i}`} data={data} index="xLabel" categories={["value"]} colors={[lineColors[i % lineColors.length]]} showLegend={false} showGridLines={false} showXAxis={false} showYAxis={false} startEndOnly={true} autoMinValue={true} showGradient={false} valueFormatter={(v: number) => formatNumber(v as any)} className="h-[48px] text-[10px]" customTooltip={SparkTooltip as any} />
                            )}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )
              }
              // Single-tile layout (no legend)
              return (
                <div className="w-full">
                  <div className="flex items-start justify-between">
                    <Metric className={`${metricClass}`}>{formatNumber(displayed.value)}</Metric>
                    {deltaEnabled && pctStr != null && (
                      <div className={`text-xs ${deltaColorSpark(displayed.pct || 0)}`}>{pctStr}</div>
                    )}
                  </div>
                  <div className="w-full h-[48px] mt-1">
                    {(() => {
                      const base = (sparkData && sparkData.length >= 2) ? sparkData : (() => {
                        const v = Number(displayed.value || 0)
                        return [ { x: 0, xLabel: '0', value: v * 0.96 }, { x: 1, xLabel: '1', value: v * 1.02 }, { x: 2, xLabel: '2', value: v * 1.00 } ]
                      })()
                      const data: SparkPoint[] = base.map((d: any) => ({ x: d.x, xLabel: (d.xLabel ?? formatXLabel(d.x)), value: d.value }))
                      return sparkType === 'bar' ? (
                        <BarChart key={`spark-${sizeKey}`} data={data} index="xLabel" categories={["value"]} colors={[color]} showLegend={false} showGridLines={false} showXAxis={false} showYAxis={false} valueFormatter={(v: number) => formatNumber(v as any)} className="h-[48px] text-[10px]" customTooltip={SparkTooltip as any} />
                      ) : sparkType === 'area' ? (
                        <AreaChart key={`spark-${sizeKey}`} data={data} index="xLabel" categories={["value"]} colors={[color]} showLegend={false} showGridLines={false} showXAxis={false} showYAxis={false} showGradient={false} valueFormatter={(v: number) => formatNumber(v as any)} className="h-[48px] text-[10px]" customTooltip={SparkTooltip as any} />
                      ) : (
                        <LineChart key={`spark-${sizeKey}`} data={data} index="xLabel" categories={["value"]} colors={[color]} showLegend={false} showGridLines={false} showXAxis={false} showYAxis={false} startEndOnly={true} autoMinValue={true} showGradient={false} valueFormatter={(v: number) => formatNumber(v as any)} className="h-[48px] text-[10px]" customTooltip={SparkTooltip as any} />
                      )
                    })()}
                  </div>
                </div>
              )
            }
            if (preset === 'categoryBar') {
              // Top N by current period; visualize absolute values on bar, compute display percents separately
              const topN = (typeof options?.kpi?.topN === 'number' ? options.kpi.topN : 3) || 3
              const legendField = (querySpec as any)?.legend as string | undefined
              const sourceMap: Record<string, { current: number; previous: number; absoluteDelta: number; percentChange: number }> | undefined = deltaEnabled
                ? (kpi.data?.byLegend || kpi.data?.bySeries)
                : (legendField ? Object.fromEntries(Object.entries(baseByLegend || {}).map(([k, v]) => [k, { current: Number(v||0), previous: 0, absoluteDelta: Number(v||0), percentChange: 0 }])) : undefined)
              const entries = Object.entries(sourceMap || {})
              const sorted = entries.sort((a,b) => Number(b[1]?.current||0) - Number(a[1]?.current||0))
              const picked = sorted.slice(0, topN)
              // Legend filter toggle support (declare before using in share logic)
              const currentLegendSel: string[] = (() => {
                if (!legendField) return []
                const baseWhere = ((querySpec as any)?.where || {}) as Record<string, any>
                const uiSel = (uiWhere as any)[legendField]
                if (Array.isArray(uiSel) && uiSel.length > 0) return uiSel.map(String)
                const baseSel = (baseWhere as any)[legendField]
                if (Array.isArray(baseSel) && baseSel.length > 0) return baseSel.map(String)
                return []
              })()
              const total = deltaEnabled
                ? Number(kpi.data?.totals?.current || picked.reduce((acc, [,v]) => acc + Number(v.current||0), 0)) || 0
                : picked.reduce((acc, [,v]) => acc + Number(v.current||0), 0)
              const shareCur: Record<string, number> | undefined = (kpi.data as any)?.share?.cur
              const usingShare = !!(deltaEnabled && shareCur && currentLegendSel.length === 0)
              const rawPercents = picked.map(([name, v]) => {
                if (usingShare) {
                  const pct = findSharePct(shareCur, name)
                  return Number.isFinite(Number(pct)) ? Number(pct) : 0
                }
                return total > 0 ? (Number(v.current||0) / total) * 100 : 0
              })
              const percents = rawPercents.map((n) => (Number.isFinite(n) ? Math.max(0, Math.min(100, Math.round(n))) : 0))
              const absValues = picked.map(([,v]) => Number(v.current||0))
              const toggleLegend = (name: string) => {
                if (!legendField) return
                const set = new Set<string>(currentLegendSel.map(String))
                if (set.has(name)) set.delete(name); else set.add(name)
                const next = Array.from(set.values())
                setUiWhereAndEmit({ [legendField]: next.length ? next : undefined })
              }
              if (percents.length === 0) {
                // Nothing to render; show the main metric only
                return (
                  <Metric className={metricClass}>{formatNumber(displayed.value)}{suffix ? <span className="ml-1 text-base align-super">{suffix}</span> : null}</Metric>
                )
              }
              // Use extended palette so colors length always >= values length
              const palette = getPresetPalette((options?.colorPreset || 'default') as any)
              const segColors = getDefaultSeriesColors(percents.length, palette) as any
              return (
                <div className="space-y-1">
                  <Metric className={metricClass}>{formatNumber(displayed.value)}{suffix ? <span className="ml-1 text-base align-super">{suffix}</span> : null}</Metric>
                  <TremorCategoryBar values={absValues} colors={segColors} markerValue={total} className="mt-1" />
                  <div className="mt-1 flex w-full items-start text-[11px] text-muted-foreground pr-6">
                    {picked.map(([name, v], i) => {
                      const isLast = i === (percents.length - 1)
                      return (
                        <div key={name} className="px-1 min-w-0" style={{ width: isLast ? `calc(${percents[i]}% + 24px)` : `${percents[i]}%` }}>
                          <button type="button" onClick={(e) => { e.stopPropagation(); toggleLegend(name) }} className="w-full text-left">
                            <div className={`flex items-start ${isLast ? 'flex-row-reverse justify-end' : 'justify-start'} gap-1`} title={`${name}: ${formatNumber(v.current)} (${Math.round(percents[i])}%)`}>
                              <span className="inline-block w-2 h-2 rounded-sm flex-shrink-0" style={{ backgroundColor: tremorNameToHex(segColors[i]) }} />
                              <div className="min-w-0">
                                <div className={`whitespace-normal break-words leading-tight ${currentLegendSel.includes(String(name)) ? 'text-foreground' : ''}`}>{displayKpiLabel(name)}</div>
                                <div className="opacity-70 whitespace-normal break-words">{formatNumber(v.current)} ({Math.round(percents[i])}%)</div>
                              </div>
                            </div>
                          </button>
                        </div>
                      )
                    })}
                  </div>
                </div>
              )
            }
            if (preset === 'multiProgress') {
              // Render Top N rows with ProgressBar for each (percent-of-total)
              const topN = (typeof options?.kpi?.topN === 'number' ? options.kpi.topN : 3) || 3
              const sourceMap: Record<string, { current: number; previous: number; absoluteDelta: number; percentChange: number }> | undefined = (kpi.data?.byLegend || kpi.data?.bySeries)
              const entries = Object.entries(sourceMap || {})
              const sorted = entries.sort((a,b) => Number(b[1]?.current||0) - Number(a[1]?.current||0))
              const picked = sorted.slice(0, topN)
              const total = Number(kpi.data?.totals?.current || picked.reduce((acc, [,v]) => acc + Number(v.current||0), 0)) || 0
              const palette = getPresetPalette((options?.colorPreset || 'default') as any)
              const rowColors = getDefaultSeriesColors(picked.length, palette) as any
              // Legend filter toggle support
              const legendField = (querySpec as any)?.legend as string | undefined
              const currentLegendSel: string[] = (() => {
                if (!legendField) return []
                const baseWhere = ((querySpec as any)?.where || {}) as Record<string, any>
                const uiSel = (uiWhere as any)[legendField]
                if (Array.isArray(uiSel) && uiSel.length > 0) return uiSel.map(String)
                const baseSel = (baseWhere as any)[legendField]
                if (Array.isArray(baseSel) && baseSel.length > 0) return baseSel.map(String)
                return []
              })()
              const toggleLegend = (name: string) => {
                if (!legendField) return
                const set = new Set<string>(currentLegendSel.map(String))
                if (set.has(name)) set.delete(name); else set.add(name)
                const next = Array.from(set.values())
                setUiWhereAndEmit({ [legendField]: next.length ? next : undefined })
              }
              return (
                <div className="space-y-2">
                  {picked.map(([name, v], i) => {
                    const curr = Number(v.current||0)
                    const shareCur: Record<string, number> | undefined = (kpi.data as any)?.share?.cur
                    const usingShare = !!(deltaEnabled && shareCur && currentLegendSel.length === 0)
                    const pctRaw = usingShare ? (findSharePct(shareCur, name) ?? 0) : (total > 0 ? ((curr / total) * 100) : 0)
                    const pct = Number.isFinite(pctRaw) ? Math.max(0, Math.min(100, pctRaw)) : 0
                    const right = `${formatNumber(curr)} (${Math.round(pct)}%)`
                    return (
                      <div key={name} className="space-y-0.5">
                        <div className="flex items-center justify-between text-[12px] text-muted-foreground">
                          <button type="button" className={`truncate max-w-[160px] text-left ${currentLegendSel.includes(String(name)) ? 'text-foreground' : ''}`} title={name} onClick={(e) => { e.stopPropagation(); toggleLegend(name) }}>
                            {displayKpiLabel(name)}
                          </button>
                          <span className="whitespace-nowrap">{right}</span>
                        </div>
                        <TremorProgressBar value={pct} color={rowColors[i % rowColors.length]} />
                      </div>
                    )
                  })}
                </div>
              )
            }
            if (preset === 'badge') {
              return (
                <div className="flex items-start justify-between">
                  <Metric className={metricClass}>{vStr}{suffix ? <span className="ml-1 text-base align-super">{suffix}</span> : null}</Metric>
                  {deltaEnabled && pctStr != null && (<span className={`ml-2 text-xs px-2 py-0.5 rounded-md bg-muted ${deltaColor(pct ?? 0)}`}>{pctStr}</span>)}
                </div>
              )
            }
            // basic
            return (
              <div className="flex items-center gap-2">
                <Metric className={metricClass}>{vStr}{suffix ? <span className="ml-1 text-base align-super">{suffix}</span> : null}</Metric>
                {deltaEnabled && pctStr != null && (<span className={`text-sm ${deltaColor(displayed.pct || 0)}`}>{pctStr}</span>)}
              </div>
            )
          })()}
        </div>
      )}
    </div>
    </ErrorBoundary>
  )
}
