"use client"

import React, { useMemo, useEffect, useState, useRef } from 'react'
import dynamic from 'next/dynamic'
import { useQuery } from '@tanstack/react-query'
import { useFilters } from '@/components/providers/FiltersProvider'
import type { WidgetConfig } from '@/types/widgets'
import type { QuerySpec } from '@/lib/api'
import { Api, QueryApi } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import ErrorBoundary from '@/components/dev/ErrorBoundary'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/Tabs'
// no preset helpers needed

// ECharts client-only wrapper
const ReactECharts: any = dynamic(() => import('echarts-for-react').then(m => (m as any).default), { ssr: false }) as any

function isDark(): boolean {
  if (typeof window === 'undefined') return false
  return document.documentElement.classList.contains('dark')
}

function useDebounced<T>(val: T, delay = 350): T {
  const [v, setV] = useState<T>(val as T)
  useEffect(() => { const t = setTimeout(() => setV(val), delay); return () => { try { clearTimeout(t) } catch {} } }, [val, delay])
  return v
}

// Lightweight date parse similar to ChartCard.parseDateLoose
function parseDateLoose(v: any): Date | null {
  if (v == null) return null
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v
  const s = String(v).trim()
  if (!s) return null
  if (/^\d{10,13}$/.test(s)) { const n = Number(s); const ms = s.length === 10 ? n*1000 : n; const d = new Date(ms); return isNaN(d.getTime())?null:d }
  const norm = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
  let d = new Date(norm); if (!isNaN(d.getTime())) return d
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/); if (iso) { d = new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`); return isNaN(d.getTime())?null:d }
  const ym = s.match(/^(\d{4})-(\d{2})$/); if (ym) { const yyyy = Number(ym[1]); const mm = Math.max(1, Math.min(12, Number(ym[2]))); d = new Date(`${yyyy}-${String(mm).padStart(2,'0')}-01T00:00:00`); return isNaN(d.getTime())?null:d }
  return null
}

function fmtXLabel(raw: any, options?: WidgetConfig['options'], querySpec?: QuerySpec): string {
  const s = String(raw ?? '')
  const d = parseDateLoose(s)
  const pad = (n: number) => String(n).padStart(2, '0')
  const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
  const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
  const dateFmt = (options as any)?.xDateFormat
    || ((((querySpec as any)?.groupBy || '').toString()) === 'year' ? 'YYYY'
      : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
      : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
      : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
      : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
      : undefined)
  if (d && dateFmt) {
    switch (dateFmt) {
      case 'YYYY': return String(d.getFullYear())
      case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
      case 'YYYY-[W]ww': return `${d.getFullYear()}-W${String(isoWeek(d)).padStart(2,'0')}`
      case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
      case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
      case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
      case 'h:mm a': { let h=d.getHours(); const m=pad(d.getMinutes()); const am=h<12; h=h%12||12; return `${h}:${m} ${am?'AM':'PM'}` }
      case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
      case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
      case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
      default: return s
    }
  }
  const mode = (options as any)?.xLabelCase as 'lowercase'|'capitalize'|'proper'|undefined
  if (!mode) return s
  switch (mode) {
    case 'lowercase': return s.toLowerCase()
    case 'capitalize': { const lower = s.toLowerCase(); return lower.length ? (lower[0].toUpperCase()+lower.slice(1)) : lower }
    case 'proper': default: return s.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
  }
}

function buildTooltipHtml(header: string, lines: Array<{ label: string; value: string; right?: string }>) {
  const dark = isDark()
  const bg = dark ? 'hsla(199, 98.5%, 8.1%, 0.9)' : 'rgba(255,255,255,0.95)'
  const fg = dark ? 'hsl(var(--foreground))' : '#0f172a'
  const border = dark ? '1px solid hsl(var(--border))' : '1px solid rgba(148,163,184,.35)'
  const rows = lines.map(l => (
    `<tr><td style="padding:2px 6px;opacity:.85;text-align:left;white-space:nowrap">${l.label}</td>`+
    `<td style="padding:2px 6px;text-align:right">${l.value}</td>`+
    (l.right?`<td style=\"padding:2px 6px;opacity:.75;text-align:right\">${l.right}</td>`:'')+`</tr>`
  )).join('')
  return `<div style="padding:6px 8px;border:${border};background:${bg};color:${fg};border-radius:6px;font-size:12px;line-height:1.1;font-variant-numeric:tabular-nums;">`+
         `<div style="font-weight:600;margin-bottom:6px;text-align:left">${header}</div>`+
         `<table style="border-collapse:separate;border-spacing:0 2px;min-width:240px"><tbody>${rows}</tbody></table>`+
         `</div>`
}

function resolveAgg(q?: QuerySpec, opts?: WidgetConfig['options'], ctx?: 'weekdayHour'|'calendar'): string {
  const anyQ: any = q || {}
  if (ctx === 'weekdayHour') {
    return (opts?.heatmap?.weekdayHour?.agg)
      || anyQ.agg
      || ((Array.isArray(anyQ.series) && anyQ.series.length === 1 && anyQ.series[0]?.agg) || '')
      || 'sum'
  }
  return anyQ.agg
    || ((Array.isArray(anyQ.series) && anyQ.series.length === 1 && anyQ.series[0]?.agg) || '')
    || 'sum'
}

function resolveYField(q?: QuerySpec, opts?: WidgetConfig['options'], ctx?: 'weekdayHour'|'calendar'): string {
  const anyQ: any = q || {}
  if (ctx === 'weekdayHour') {
    return (opts?.heatmap?.weekdayHour?.valueField)
      || anyQ.y || anyQ.measure
      || ((Array.isArray(anyQ.series) && anyQ.series.length === 1 && anyQ.series[0]?.y) || 'Value')
  }
  return anyQ.y || anyQ.measure
    || ((Array.isArray(anyQ.series) && anyQ.series.length === 1 && anyQ.series[0]?.y) || 'Value')
}

function formatHeat(v: number, fmt?: string, currency?: string) {
  const n = Number(v)
  if (!Number.isFinite(n)) return ''
  try {
    switch (fmt) {
      case 'short': return new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 }).format(n)
      case 'currency': return new Intl.NumberFormat(undefined, { style: 'currency', currency: currency || 'USD', maximumFractionDigits: 0 }).format(n)
      case 'percent': return new Intl.NumberFormat(undefined, { style: 'percent', maximumFractionDigits: 1 }).format(n)
      case 'bytes': {
        const units = ['B','KB','MB','GB','TB']; let x=n, i=0; while (x>=1024 && i<units.length-1) { x/=1024; i++ } return `${x.toFixed(1)} ${units[i]}`
      }
      case 'oneDecimal': return new Intl.NumberFormat(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }).format(n)
      case 'twoDecimals': return new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(n)
      case 'none': default: return new Intl.NumberFormat().format(n)
    }
  } catch { return String(n) }
}

function gradientFromOptions(opts?: WidgetConfig['options']): string[] {
  const base = (opts?.colorBaseKey || opts?.color || 'blue') as string
  const preset = (opts?.colorPreset || 'default') as string
  const dark = isDark()
  const map: Record<string, { light: string; mid: string; dark: string }> = {
    blue:   { light: '#bfdbfe', mid: '#60a5fa', dark: '#1d4ed8' },
    indigo: { light: '#c7d2fe', mid: '#818cf8', dark: '#4338ca' },
    violet: { light: '#e9d5ff', mid: '#c084fc', dark: '#7c3aed' },
    emerald:{ light: '#a7f3d0', mid: '#34d399', dark: '#047857' },
    teal:   { light: '#99f6e4', mid: '#2dd4bf', dark: '#0f766e' },
    cyan:   { light: '#a5f3fc', mid: '#22d3ee', dark: '#0891b2' },
    amber:  { light: '#fde68a', mid: '#f59e0b', dark: '#b45309' },
    rose:   { light: '#fecdd3', mid: '#fb7185', dark: '#be123c' },
    pink:   { light: '#fbcfe8', mid: '#f472b6', dark: '#be185d' },
    lime:   { light: '#d9f99d', mid: '#84cc16', dark: '#65a30d' },
    fuchsia:{ light: '#f5d0fe', mid: '#e879f9', dark: '#a21caf' },
    gray:   { light: '#e5e7eb', mid: '#9ca3af', dark: '#374151' },
  }
  const b = map[base] || map.blue
  // Respect colorPreset: choose different low-end color
  if (dark) {
    if (preset === 'muted') return ['rgba(255,255,255,0.04)', b.light, b.mid]
    if (preset === 'corporate') return ['rgba(148,163,184,0.15)', b.mid, b.dark]
    if (preset === 'vibrant') return ['rgba(255,255,255,0.06)', b.mid, b.dark]
    return ['rgba(255,255,255,0.06)', b.mid, b.dark]
  }
  if (preset === 'muted') return ['#f3f4f6', b.light, b.mid]
  if (preset === 'corporate') return ['#e5e7eb', '#94a3b8', b.dark]
  if (preset === 'vibrant') return [b.light, b.mid, b.dark]
  return ['#f9fafb', b.light, b.dark]
}

export default function HeatmapCard({
  title,
  sql,
  datasourceId,
  options,
  queryMode = 'sql',
  querySpec,
  widgetId,
  tabbedGuard,
  tabbedField,
}: {
  title: string
  sql: string
  datasourceId?: string
  options?: WidgetConfig['options']
  queryMode?: 'sql' | 'spec'
  querySpec?: QuerySpec
  widgetId?: string
  tabbedGuard?: boolean
  tabbedField?: string
}) {
  const { filters } = useFilters()
  const { user } = useAuth()
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
  // Respect global break-link per widget (use base id before ::tab suffix)
  const baseId = useMemo(() => String(widgetId || '').split('::')[0], [widgetId])
  const ignoreGlobal = useMemo(() => {
    try { return !!(baseId && typeof window !== 'undefined' && localStorage.getItem(`gf_break_${baseId}`) === '1') } catch { return false }
  }, [baseId])
  const [breakSeq, setBreakSeq] = useState(0)
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string }
      if (!baseId || !d?.widgetId || d.widgetId !== baseId) return
      setBreakSeq((v) => v + 1)
    }
    if (typeof window !== 'undefined') window.addEventListener('global-filters-break-change', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('global-filters-break-change', handler as EventListener) }
  }, [baseId])

  // Tabs: distinct values loader similar to ChartCard
  const tabsField = (options as any)?.tabsField as string | undefined
  const tabsVariant = ((options as any)?.tabsVariant || 'line') as 'line' | 'solid'
  const tabsMaxItems = Math.max(1, Number((options as any)?.tabsMaxItems ?? 8))
  const tabsStretch = !!(options as any)?.tabsStretch
  const tabsShowAll = !!(options as any)?.tabsShowAll
  const tabsLabelCase = (((options as any)?.tabsLabelCase || 'legend') as 'legend'|'lowercase'|'capitalize'|'proper')
  const tabsSortDir = (((options as any)?.tabsSort?.direction || 'asc') as 'asc'|'desc')

  // format tab label
  const applyTabsCase = (s: string) => {
    const mode = (tabsLabelCase === 'legend') ? ((options as any)?.legendLabelCase as any) : (tabsLabelCase as any)
    const str = String(s ?? '')
    if (!mode) return str
    switch (mode) {
      case 'lowercase': return str.toLowerCase()
      case 'capitalize': { const lower = str.toLowerCase(); return lower.length ? (lower[0].toUpperCase()+lower.slice(1)) : lower }
      case 'proper': default: return str.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
    }
  }

  // Load distinct values for tabsField
  const [tabVals, setTabVals] = useState<string[]>([])
  useEffect(() => {
    let ignore = false
    async function run() {
      try {
        if (!tabsField || queryMode !== 'spec' || !querySpec) { if (!ignore) setTabVals([]); return }
        const source = (querySpec as any)?.source as string | undefined
        if (!source) { if (!ignore) setTabVals([]); return }
        // Build where with deltaDateField constraints (similar to main query)
        const base: any = { ...(querySpec as any) }
        const df = (options as any)?.deltaDateField as string | undefined
        const where: Record<string, any> = { ...(base.where || {}) }
        if (df && !ignoreGlobal) {
          if (filters.startDate) where[`${df}__gte`] = filters.startDate
          if (filters.endDate) { const d = new Date(`${filters.endDate}T00:00:00`); d.setDate(d.getDate() + 1); where[`${df}__lt`] = d.toISOString().slice(0,10) }
        }
        const spec: any = { source, select: [tabsField], where: Object.keys(where).length ? where : undefined, limit: 1000, offset: 0 }
        const res = await QueryApi.querySpec({ spec, datasourceId, limit: 1000, offset: 0, includeTotal: false })
        const cols = (res?.columns || []) as string[]
        const idx = cols.indexOf(tabsField)
        const set = new Set<string>()
        ;(res?.rows || []).forEach((row:any) => { const v = Array.isArray(row) ? row[idx] : row?.[tabsField]; if (v != null) set.add(String(v)) })
        let vals = Array.from(set.values())
        vals.sort((a,b) => a.localeCompare(b))
        if (tabsSortDir === 'desc') vals.reverse()
        if (!ignore) setTabVals(vals)
      } catch { if (!ignore) setTabVals([]) }
    }
    void run(); return () => { ignore = true }
  }, [tabsField, queryMode, JSON.stringify(querySpec || {}), datasourceId, filters.startDate, filters.endDate, (options as any)?.deltaDateField, tabsSortDir, ignoreGlobal, breakSeq])

  // Tabs UI (guard to avoid recursion)
  const wantTabs = !!tabsField && !tabbedGuard && (tabVals.length > 0 || tabsShowAll)

  const specKey = useMemo(() => JSON.stringify(querySpec || {}), [querySpec])
  const filtersKey = useMemo(() => JSON.stringify(filters || {}), [filters])
  const debSpecKey = useDebounced(specKey, 350)
  const debFiltersKey = useDebounced(filtersKey, 350)
  const q = useQuery({
    queryKey: ['heatmap', title, sql, datasourceId, options, queryMode, debSpecKey, debFiltersKey, breakSeq],
    placeholderData: (prev) => prev as any,
    queryFn: async () => {
      if (queryMode === 'spec' && querySpec) {
        // Start from base spec and attach date bounds
        const base: any = { ...querySpec }
        const df = (options as any)?.deltaDateField as string | undefined
        const where: Record<string, any> = { ...(base.where || {}) }
        if (df && !ignoreGlobal) {
          if (filters.startDate) where[`${df}__gte`] = filters.startDate
          if (filters.endDate) {
            const d = new Date(`${filters.endDate}T00:00:00`); d.setDate(d.getDate() + 1)
            where[`${df}__lt`] = d.toISOString().slice(0, 10)
          }
        }
        // Calendar presets: constrain to month/year (client aggregates by day)
        const preset = (options?.heatmap?.preset || 'calendarMonthly') as any
        if (preset === 'calendarMonthly' || preset === 'calendarAnnual') {
          const seriesArr: any[] = Array.isArray(base.series) ? base.series as any[] : []
          const s0 = seriesArr.find((s)=>s?.x) || null
          const x = base.x || s0?.x
          const addRange = (start: string, end: string) => { if (x) { (where as any)[`${x}__gte`] = start; (where as any)[`${x}__lt`] = end } }
          if (preset === 'calendarMonthly' && options?.heatmap?.calendarMonthly?.month) {
            const m = String(options.heatmap.calendarMonthly.month)
            const start = `${m}-01`
            const d = new Date(`${start}T00:00:00`); d.setMonth(d.getMonth() + 1)
            const end = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-01`
            addRange(start, end)
          }
          if (preset === 'calendarAnnual' && options?.heatmap?.calendarAnnual?.year) {
            const yStr = String(options.heatmap.calendarAnnual.year)
            const start = `${yStr}-01-01`
            const end = `${String(Number(yStr)+1)}-01-01`
            addRange(start, end)
          }
        }
        // Weekday × Hour: try server pre-bucketing with SQL when source/x/y exist
        if (preset === 'weekdayHour' && base?.source) {
          try {
            const seriesArr: any[] = Array.isArray(base.series) ? base.series as any[] : []
            const s0 = seriesArr.find((s)=>s?.y || s?.measure || s?.x) || null
            let xField: any = options?.heatmap?.weekdayHour?.timeField || base.x || s0?.x
            // If x is a derived part like "<field> (Day)", extract the base field name
            try {
              const m = String(xField || '').match(/^(.*)\s\((Year|Quarter|Month(?: Name| Short)?|Week|Day(?: Name| Short)?)\)$/)
              if (m) xField = m[1]
            } catch {}
            // Prefer explicit widget valueField if provided
            const yField = options?.heatmap?.weekdayHour?.valueField || base.y || base.measure || s0?.y || s0?.measure
            const aggEff = resolveAgg(base, options, 'weekdayHour')
            if (!xField || (!yField && String(aggEff).toLowerCase() !== 'count')) throw new Error('missing x/y')
            const x = String(xField)
            const y = String(yField || '')
            // Only require simple identifier for column-based aggs; COUNT(*) needs no column
            const aggLower = String(aggEff || 'sum').toLowerCase()
            const agg = (aggLower === 'none') ? 'sum' : aggLower
            const needsColumn = (agg === 'sum' || agg === 'avg' || agg === 'min' || agg === 'max' || agg === 'distinct')
            if (needsColumn) {
              const simpleIdent = /^[A-Za-z_][A-Za-z0-9_]*$/.test(y)
              if (!simpleIdent) throw new Error('non-simple y; fallback to spec')
            }
            const qIdent = (nm: string) => `[${String(nm).replace(/]/g, ']]')}]`
            const qSource = (src: string) => String(src).split('.').map(qIdent).join('.')
            const qLit = (v: any) => {
              if (v == null) return 'NULL'
              if (typeof v === 'number' && Number.isFinite(v)) return String(v)
              if (v instanceof Date) return `'${v.toISOString().slice(0,19).replace('T',' ')}'`
              const s = String(v)
              return `'${s.replace(/'/g, "''")}'`
            }
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
            const valueExpr = (
              agg === 'avg' ? `AVG(${qIdent(y)})` :
              agg === 'count' ? 'COUNT(*)' :
              agg === 'min' ? `MIN(${qIdent(y)})` :
              agg === 'max' ? `MAX(${qIdent(y)})` :
              agg === 'distinct' ? `COUNT(DISTINCT ${qIdent(y)})` :
              /* sum + none -> sum */ `SUM(${qIdent(y)})`
            )
            // SQL Server-style: DATEPART returns 1..7 for Sunday..Saturday; remap to Mon=0..Sun=6
            const hourExpr = `DATEPART(HOUR, ${qIdent(x)})`
            const weekdayExpr = `((DATEPART(WEEKDAY, ${qIdent(x)}) + 5) % 7)`
            const sql = `SELECT ${hourExpr} AS [hour], ${weekdayExpr} AS [weekday], ${valueExpr} AS [value] FROM ${qSource(String(base.source))}${buildWhere(where)} GROUP BY ${hourExpr}, ${weekdayExpr} ORDER BY 2, 1`
            const wid = String(widgetId || title || 'heatmap')
            const { promise: __p } = Api.queryForWidget(wid, { sql, datasourceId, limit: 100000, offset: 0 }, user?.id)
            const r = await __p
            return r
          } catch { /* fall through to spec path */ }
        }
        // Default: fetch as-is, but for weekdayHour prefer RAW rows (remove groupBy/agg and derive base time field)
        if (preset === 'weekdayHour') {
          try {
            let tField: any = options?.heatmap?.weekdayHour?.timeField || base.x || (Array.isArray(base.x) ? base.x[0] : undefined)
            try {
              const m = String(tField || '').match(/^(.*)\s\((Year|Quarter|Month(?: Name| Short)?|Week|Day(?: Name| Short)?)\)$/)
              if (m) tField = m[1]
            } catch {}
            const yField = resolveYField(base, options, 'weekdayHour')
            const select = [tField, yField].filter(Boolean) as string[]
            const rawSpec: any = { source: base.source, select: (select.length ? select : [tField]), where: Object.keys(where||{}).length ? where : undefined, limit: 100000, offset: 0 }
            return QueryApi.querySpec({ spec: rawSpec, datasourceId, limit: 100000, offset: 0, includeTotal: false })
          } catch { /* fall back to generic */ }
        }
        const specSafe: any = { ...base, where }
        if (Array.isArray(specSafe.x)) specSafe.x = specSafe.x[0]
        if (Array.isArray(specSafe.y)) specSafe.y = specSafe.y[0]
        if (Array.isArray(specSafe.select) && specSafe.select.length === 0) delete specSafe.select
        if (Array.isArray(specSafe.legend)) delete specSafe.legend
        if (Array.isArray(specSafe.series)) {
          if (specSafe.series.length === 0) {
            delete specSafe.series
          } else if (specSafe.series.length === 1) {
            const s0 = specSafe.series[0] || {}
            if (!specSafe.y && (s0.y || s0.measure)) specSafe.y = s0.y || s0.measure
            if (!specSafe.agg && s0.agg) specSafe.agg = s0.agg
            delete specSafe.series
          }
        }
        return QueryApi.querySpec({ spec: specSafe, datasourceId, limit: 100000, offset: 0, includeTotal: false })
      }
      const wid = String(widgetId || title || 'heatmap')
      const { promise: __p } = Api.queryForWidget(wid, { sql, datasourceId, limit: 100000, offset: 0, params: ignoreGlobal ? {} : (filters as any) }, user?.id)
      return __p
    },
    enabled: visible && !wantTabs,
  })

  const { columns, rows } = useMemo(() => {
    const cols = (q.data?.columns as string[]) || []
    const rawRows = (q.data?.rows as any[]) || []
    return { columns: cols, rows: rawRows }
  }, [q.data?.columns, q.data?.rows])

  // Convert row arrays to objects keyed by column names
  const namedRows: Array<Record<string, any>> = useMemo(() => {
    if (!Array.isArray(rows) || !rows.length) return []
    if (!Array.isArray(rows[0])) return rows as any
    return (rows as any[]).map((arr: any[]) => {
      const o: Record<string, any> = {}
      columns.forEach((c, i) => { o[c] = arr[i] })
      return o
    })
  }, [columns, rows])

  // Helper: resolve numeric value from row
  const num = (v: any) => { const n = Number(v); return Number.isFinite(n) ? n : 0 }

  const preset = (options?.heatmap?.preset || 'calendarMonthly') as NonNullable<WidgetConfig['options']>['heatmap'] extends infer HM ? HM extends { preset?: infer P } ? P : never : never

  // Map global yAxisFormat to heatmap value formatter
  const heatValueFmt = useMemo(() => {
    const yfmt = String(options?.yAxisFormat || 'none')
    switch (yfmt) {
      case 'short': return 'short'
      case 'currency': return 'currency'
      case 'percent':
      case 'percentWhole':
      case 'percentOneDecimal': return 'percent'
      case 'bytes': return 'bytes'
      case 'oneDecimal': return 'oneDecimal'
      case 'twoDecimals': return 'twoDecimals'
      default: return 'none'
    }
  }, [options?.yAxisFormat]) as any

  // Map global legend options to visualMap placement (cannot hide here without preset change)
  const vmMap = useMemo(() => {
    const show = (options?.showLegend ?? true)
    const pos = (options?.legendPosition || 'bottom') as 'top'|'bottom'|'none'
    const orient = (pos === 'top' || pos === 'bottom') ? 'horizontal' : 'horizontal'
    const position = (pos === 'top' || pos === 'bottom') ? pos : 'top'
    return show ? { orient, position } as any : { orient, position }
  }, [options?.showLegend, options?.legendPosition])

  // Data shapers per preset
  const monthlyData = useMemo(() => {
    if (preset !== 'calendarMonthly') return null
    // Fast path: backend provided canonical x/value columns
    if (columns.includes('x') && columns.includes('value') && Array.isArray(rows) && rows.length) {
      const idxX = columns.indexOf('x')
      const idxV = columns.indexOf('value')
      return (rows as any[])
        .map((arr: any[]) => [String(arr[idxX]), Number(arr[idxV])])
        .filter((p) => !!p[0] && Number.isFinite(p[1])) as Array<[string, number]>
    }
    // Fallback: aggregate by day from named rows
    const dateField = options?.heatmap?.calendarMonthly?.dateField || (querySpec as any)?.x || columns[0]
    const valueField = options?.heatmap?.calendarMonthly?.valueField || (querySpec as any)?.y || 'value'
    const acc = new Map<string, number>()
    namedRows.forEach((r) => {
      const d = r?.[dateField!]
      const v = num(r?.[valueField!])
      if (d == null) return
      const key = (() => {
        try {
          const dt = new Date(String(d))
          const yy = dt.getFullYear()
          const mm = String(dt.getMonth() + 1).padStart(2, '0')
          const dd = String(dt.getDate()).padStart(2, '0')
          return `${yy}-${mm}-${dd}`
        } catch { return String(d) }
      })()
      acc.set(key, (acc.get(key) || 0) + v)
    })
    return Array.from(acc.entries()) as Array<[string, number]>
  }, [preset, rows, columns, namedRows, options?.heatmap?.calendarMonthly, querySpec])

  const annualData = useMemo(() => {
    if (preset !== 'calendarAnnual') return null
    if (columns.includes('x') && columns.includes('value') && Array.isArray(rows) && rows.length) {
      const idxX = columns.indexOf('x')
      const idxV = columns.indexOf('value')
      return (rows as any[])
        .map((arr: any[]) => [String(arr[idxX]), Number(arr[idxV])])
        .filter((p) => !!p[0] && Number.isFinite(p[1])) as Array<[string, number]>
    }
    const dateField = options?.heatmap?.calendarAnnual?.dateField || (querySpec as any)?.x || columns[0]
    const valueField = options?.heatmap?.calendarAnnual?.valueField || (querySpec as any)?.y || 'value'
    const acc = new Map<string, number>()
    namedRows.forEach((r) => {
      const d = r?.[dateField!]
      const v = num(r?.[valueField!])
      if (d == null) return
      const key = (() => {
        try {
          const dt = new Date(String(d))
          const yy = dt.getFullYear()
          const mm = String(dt.getMonth() + 1).padStart(2, '0')
          const dd = String(dt.getDate()).padStart(2, '0')
          return `${yy}-${mm}-${dd}`
        } catch { return String(d) }
      })()
      acc.set(key, (acc.get(key) || 0) + v)
    })
    return Array.from(acc.entries()) as Array<[string, number]>
  }, [preset, rows, columns, namedRows, options?.heatmap?.calendarAnnual, querySpec])

  const weekdayHourData = useMemo(() => {
    if (preset !== 'weekdayHour') return null
    // If server-prebucket returned [hour, weekday, value] columns, consume directly
    const colLower = (s: string) => String(s || '').toLowerCase()
    const hasHour = columns.some((c) => colLower(c) === 'hour')
    const hasWeekday = columns.some((c) => colLower(c) === 'weekday')
    const hasValue = columns.some((c) => colLower(c) === 'value')
    if (hasHour && hasWeekday && hasValue) {
      try {
        const idxHour = columns.findIndex((c)=>colLower(c)==='hour')
        const idxWeek = columns.findIndex((c)=>colLower(c)==='weekday')
        const idxVal = columns.findIndex((c)=>colLower(c)==='value')
        const out: Array<[number, number, number]> = (rows as any[]).map((arr:any[]) => [Number(arr[idxHour]||0), Number(arr[idxWeek]||0), Number(arr[idxVal]||0)])
        return out
      } catch { /* fallback to client path */ }
    }
    let tField: any = options?.heatmap?.weekdayHour?.timeField || (querySpec as any)?.x || columns[0]
    try {
      const m = String(tField || '').match(/^(.*)\s\((Year|Quarter|Month(?: Name| Short)?|Week|Day(?: Name| Short)?)\)$/)
      if (m) tField = m[1]
    } catch {}
    let valueField = resolveYField(querySpec as any, options, 'weekdayHour') || (columns.includes('value') ? 'value' : (columns.includes('Value') ? 'Value' : 'value'))
    const agg = (String((options?.heatmap?.weekdayHour as any)?.agg || (querySpec as any)?.agg || 'sum').toLowerCase() as 'sum'|'count'|'avg'|'min'|'max'|'distinct'|'none')
    // accumulator: key (hour,weekday) -> { sum, count }
    const acc = new Map<string, { s: number; c: number; min: number; max: number; set?: Set<string> }>()
    // If valueField appears non-numeric or missing, try to infer a numeric column
    try {
      const isNumericFor = (c: string) => {
        const limit = Math.min(50, namedRows.length)
        for (let i = 0; i < limit; i++) { const r = namedRows[i]; const v = Number((r as any)?.[c]); if (Number.isFinite(v) && v !== 0) return true }
        // accept zeros if some entries are finite numbers
        let seenFinite = false
        for (let i = 0; i < limit; i++) { const r = namedRows[i]; const v = Number((r as any)?.[c]); if (Number.isFinite(v)) { seenFinite = true; break } }
        return seenFinite
      }
      if (!valueField || !isNumericFor(valueField)) {
        const ignore = new Set<string>([String(tField || ''), 'x', 'hour', 'weekday'])
        const candidate = (columns || []).find((c) => !ignore.has(c) && isNumericFor(c))
        if (candidate) valueField = candidate
      }
    } catch {}
    namedRows.forEach((r) => {
      const t = r?.[tField!]
      const v = num(r?.[valueField!])
      if (t == null) return
      const d = new Date(String(t))
      if (isNaN(d.getTime())) return
      const hour = d.getHours()
      const jsDay = d.getDay() // 0=Sun..6=Sat
      const weekday = ((jsDay + 6) % 7) // 0=Mon..6=Sun
      const key = `${hour}_${weekday}`
      const node = acc.get(key) || { s: 0, c: 0, min: Number.POSITIVE_INFINITY, max: Number.NEGATIVE_INFINITY }
      // count increments by 1; others accumulate value
      if (agg === 'count') {
        node.c += 1
      } else if (agg === 'distinct') {
        const dk = (r?.[valueField!] != null) ? String(r?.[valueField!]) : undefined
        if (dk !== undefined) { if (!node.set) node.set = new Set<string>(); node.set.add(dk) }
        node.c += 1
      } else {
        node.s += v
        node.c += 1
        if (v < node.min) node.min = v
        if (v > node.max) node.max = v
      }
      acc.set(key, node)
    })
    const out: Array<[number, number, number]> = []
    acc.forEach((node, key) => {
      const [h, w] = key.split('_').map(Number)
      const aggCalc = (agg === 'none') ? 'sum' : agg
      const val = (
        aggCalc === 'avg' ? (node.c ? node.s / node.c : 0) :
        aggCalc === 'min' ? (Number.isFinite(node.min) ? node.min : 0) :
        aggCalc === 'max' ? (Number.isFinite(node.max) ? node.max : 0) :
        aggCalc === 'count' ? node.c :
        aggCalc === 'distinct' ? ((node.set && node.set.size) ? node.set.size : 0) :
        /* sum + none -> sum */ node.s
      )
      out.push([h, w, val])
    })
    return out
  }, [preset, namedRows, options?.heatmap?.weekdayHour, querySpec, columns])

  // correlation preset removed

  if (q.isLoading) return (<div ref={containerRef} className="space-y-2 animate-pulse"><div className="h-6 bg-muted rounded w-1/2" /><div className="h-[280px] bg-muted rounded" /></div>)
  if (q.error) return (<div ref={containerRef} className="text-sm text-red-600">Failed to load heatmap</div>)

  const keyBase = `${preset}|${(namedRows || []).length}|${wantTabs ? 'tabs' : 'no-tabs'}`

  // Render Tabs after all hooks to keep hook order stable
  if (wantTabs) {
    const shown = tabsMaxItems > 0 ? tabVals.slice(0, tabsMaxItems) : tabVals
    const initial = tabsShowAll ? '__ALL__' : (shown[0] || '')
    const triggerClass = tabsVariant === 'solid' ? 'px-2 py-1 rounded data-[state=active]:bg-[hsl(var(--btn2))] data-[state=active]:text-black text-[12px]' : 'px-2 py-1 text-[12px]'
    return (
      <ErrorBoundary name="HeatmapCard@Tabs">
        <div className="h-full flex flex-col" ref={containerRef}>
          <Tabs defaultValue={initial} className="flex-1 min-h-0 flex flex-col">
            <TabsList variant={tabsVariant} className={`justify-start overflow-x-auto shrink-0 relative z-10`}>
              {tabsShowAll && (
                <TabsTrigger value="__ALL__" className={triggerClass}>All</TabsTrigger>
              )}
              {shown.map((v) => (
                <TabsTrigger key={v} value={String(v)} className={triggerClass}>
                  <span title={String(v)}>{applyTabsCase(String(v))}</span>
                </TabsTrigger>
              ))}
            </TabsList>
            <div className="mt-0 flex-1 min-h-0">
              {tabsShowAll && (
                <TabsContent key="__ALL__" value="__ALL__" className="h-full" forceMount>
                  <div className="h-full relative">
                    <HeatmapCard
                      title={title}
                      sql={sql}
                      datasourceId={datasourceId}
                      options={{ ...(options || {}), tabsField: undefined }}
                      queryMode={queryMode}
                      querySpec={querySpec}
                      widgetId={widgetId ? `${widgetId}::tab:__ALL__` : undefined}
                      tabbedGuard={true}
                      tabbedField={tabsField}
                    />
                  </div>
                </TabsContent>
              )}
              {shown.map((v) => (
                <TabsContent key={v} value={String(v)} className="h-full" forceMount>
                  <div className="h-full relative">
                    <HeatmapCard
                      title={title}
                      sql={sql}
                      datasourceId={datasourceId}
                      options={{ ...(options || {}), tabsField: undefined }}
                      queryMode={queryMode}
                      querySpec={querySpec ? { ...(querySpec as any), where: { ...(((querySpec as any)?.where) || {}), [tabsField as string]: [v] } } : undefined}
                      widgetId={widgetId ? `${widgetId}::tab:${String(v)}` : undefined}
                      tabbedGuard={true}
                      tabbedField={tabsField}
                    />
                  </div>
                </TabsContent>
              ))}
            </div>
          </Tabs>
        </div>
      </ErrorBoundary>
    )
  }

  if (preset === 'calendarMonthly') {
    const pairs = (monthlyData || []) as Array<[string, number]>
    const vals = pairs.map(([, v]) => Number(v)).filter(Number.isFinite)
    let min = 0; let max = Math.max(...vals)
    if (!isFinite(max)) max = 1
    if (!isFinite(max) || max === min) max = min === 0 ? 1 : min * 2
    const month = options?.heatmap?.calendarMonthly?.month || (pairs[0]?.[0] ? pairs[0][0].slice(0,7) : new Date().toISOString().slice(0,7))
    const grad = gradientFromOptions(options)
    const legendPos = (options?.legendPosition || 'bottom')
    const preview = !!options?.heatmap?.preview
    const xField = (querySpec as any)?.x || 'Date'
    const yField = (querySpec as any)?.y || 'Value'
    const aggName = resolveAgg(querySpec, options, 'calendar')
    const option = {
      textStyle: { color: isDark() ? 'rgba(226,232,240,0.85)' : '#334155' },
      tooltip: {
        position: 'top' as const,
        backgroundColor: 'transparent', borderWidth: 0, extraCssText: 'box-shadow:none;padding:0;',
        formatter: (p: any) => {
          const raw = String(p?.value?.[0] ?? '')
          const xLabel = fmtXLabel(raw, options, querySpec)
          const v = Number(p?.value?.[1] || 0)
          return buildTooltipHtml(String(xLabel), [ { label: yField, value: formatHeat(v, heatValueFmt, options?.valueCurrency), right: aggName } ])
        }
      },
      visualMap: {
        show: preview ? false : (options?.showLegend ?? true),
        min, max, calculable: true, orient: 'horizontal' as const,
        left: 'center', top: (legendPos==='top'? 6 : undefined), bottom: (legendPos==='bottom'? 6 : undefined),
        inRange: { color: grad, opacity: [0.2, 1] },
        outOfRange: { color: isDark() ? 'rgba(255,255,255,0.06)' : '#f8fafc', opacity: 0.25 },
        textStyle: { color: isDark() ? 'rgba(226,232,240,0.85)' : '#334155' },
        formatter: (val: number) => formatHeat(val, heatValueFmt, options?.valueCurrency),
      },
      calendar: {
        top: preview ? 12 : 64, bottom: preview ? 8 : 56, left: preview ? 12 : 56, right: preview ? 8 : 24,
        cellSize: (preview ? [12, 12] : [22, 22]) as any,
        range: month,
        itemStyle: {
          borderWidth: 0.5,
          borderColor: isDark() ? 'rgba(148,163,184,.3)' : 'rgba(148,163,184,.35)',
          color: isDark() ? 'rgba(255,255,255,0.06)' : '#f8fafc',
        },
        splitLine: { lineStyle: { color: isDark() ? 'rgba(148,163,184,.25)' : 'rgba(148,163,184,.4)', width: 1 } },
        dayLabel: { show: !preview, color: isDark() ? 'rgba(226,232,240,0.7)' : '#475569' },
        monthLabel: { show: !preview, color: isDark() ? 'rgba(226,232,240,0.7)' : '#475569' },
        yearLabel: { show: false },
      },
      series: [{ type: 'heatmap', coordinateSystem: 'calendar', data: pairs }],
    }
    return (
      <ErrorBoundary name="HeatmapCard@Monthly">
        <div className="absolute inset-0" ref={containerRef}>
          <ReactECharts key={keyBase} option={option} style={{ height: '100%' }} notMerge={true} lazyUpdate={true} />
        </div>
      </ErrorBoundary>
    )
  }
  if (preset === 'calendarAnnual') {
    const pairs = (annualData || []) as Array<[string, number]>
    const vals = pairs.map(([, v]) => Number(v)).filter(Number.isFinite)
    let min = 0; let max = Math.max(...vals)
    if (!isFinite(max)) max = 1
    if (!isFinite(max) || max === min) max = min === 0 ? 1 : min * 2
    const year = options?.heatmap?.calendarAnnual?.year || (pairs[0]?.[0] ? pairs[0][0].slice(0,4) : String(new Date().getFullYear()))
    const grad = gradientFromOptions(options)
    const legendPos = (options?.legendPosition || 'bottom')
    const preview = !!options?.heatmap?.preview
    const xField = (querySpec as any)?.x || 'Date'
    const yField = (querySpec as any)?.y || 'Value'
    const aggName = (querySpec as any)?.agg || 'sum'
    const option = {
      textStyle: { color: isDark() ? 'rgba(226,232,240,0.85)' : '#334155' },
      tooltip: { position: 'top' as const, formatter: (p: any) => {
        const raw = String(p?.value?.[0] ?? '')
        const dt = new Date(raw)
        const xLabel = isNaN(dt.getTime()) ? raw : dt.toLocaleDateString()
        const v = Number(p?.value?.[1] || 0)
        return `${xField}: ${xLabel} · ${yField} (${aggName}): ${formatHeat(v, heatValueFmt, options?.valueCurrency)}`
      } },
      visualMap: {
        show: preview ? false : (options?.showLegend ?? true), min, max, calculable: true, orient: 'horizontal' as const,
        left: 'center', top: (legendPos==='top'? 6 : undefined), bottom: (legendPos==='bottom'? 6 : undefined),
        inRange: { color: grad, opacity: [0.2, 1] },
        outOfRange: { color: isDark() ? 'rgba(255,255,255,0.06)' : '#f8fafc', opacity: 0.25 },
        textStyle: { color: isDark() ? 'rgba(226,232,240,0.85)' : '#334155' },
        formatter: (val: number) => formatHeat(val, heatValueFmt, options?.valueCurrency),
      },
      calendar: {
        top: preview ? 12 : 64, bottom: preview ? 8 : 56, left: preview ? 12 : 56, right: preview ? 8 : 24,
        cellSize: (preview ? [12, 12] : [22, 22]) as any,
        range: year,
        itemStyle: {
          borderWidth: 0.5,
          borderColor: isDark() ? 'rgba(148,163,184,.3)' : 'rgba(148,163,184,.35)',
          color: isDark() ? 'rgba(255,255,255,0.06)' : '#f8fafc',
        },
        splitLine: { lineStyle: { color: isDark() ? 'rgba(148,163,184,.25)' : 'rgba(148,163,184,.4)', width: 1 } },
        dayLabel: { show: !preview, color: isDark() ? 'rgba(226,232,240,0.7)' : '#475569' },
        monthLabel: { show: !preview, color: isDark() ? 'rgba(226,232,240,0.7)' : '#475569' },
        yearLabel: { show: !preview, color: isDark() ? 'rgba(226,232,240,0.7)' : '#475569' },
      },
      series: [{ type: 'heatmap', coordinateSystem: 'calendar', data: pairs }],
    }
    return (
      <ErrorBoundary name="HeatmapCard@Annual">
        <div className="absolute inset-0" ref={containerRef}>
          <ReactECharts key={keyBase} option={option} style={{ height: '100%' }} notMerge={true} lazyUpdate={true} />
        </div>
      </ErrorBoundary>
    )
  }
  if (preset === 'weekdayHour') {
    const triples = (weekdayHourData || []) as Array<[number, number, number]>
    // Fill missing cells with zeros so empties render with lower opacity
    const set = new Map<string, number>()
    for (const t of triples) set.set(`${t[0]}|${t[1]}`, Number(t[2]||0))
    const complete: Array<[number, number, number]> = []
    for (let d=0; d<7; d++) for (let h=0; h<24; h++) complete.push([h, d, Number(set.get(`${h}|${d}`) || 0)])
    const vals = complete.map((t) => Number(t?.[2] ?? 0)).filter(Number.isFinite)
    let min = 0; let max = Math.max(...vals)
    if (!isFinite(max)) max = 1
    if (!isFinite(max) || max === min) max = min === 0 ? 1 : min * 2
    const grad = gradientFromOptions(options)
    const legendPos = (options?.legendPosition || 'bottom') as 'top'|'bottom'|'none'
    const preview = !!options?.heatmap?.preview
    const hours = Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2,'0')}:00`)
    const days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    const aggName = resolveAgg(querySpec, options, 'weekdayHour')
    const aggLower = String(aggName).toLowerCase()
    const yField = (options?.heatmap?.weekdayHour?.valueField || (querySpec as any)?.y || 'Value') as string
    const yLabel = aggLower === 'count' ? 'Count' : (aggLower === 'distinct' ? `Distinct ${yField}` : yField)
    const aggLabel = (aggLower === 'none') ? 'sum' : aggName
    const option = {
      textStyle: { color: isDark() ? 'rgba(226,232,240,0.85)' : '#334155' },
      tooltip: {
        position: 'top' as const,
        backgroundColor: 'transparent', borderWidth: 0, extraCssText: 'box-shadow:none;padding:0;',
        formatter: (p: any) => {
          const h = Number(p?.data?.[0] ?? 0), d = Number(p?.data?.[1] ?? 0), v = Number(p?.data?.[2] ?? 0)
          const header = `${days[d]} ${String(h).padStart(2,'0')}:00`
          return buildTooltipHtml(header, [ { label: yLabel || 'Value', value: formatHeat(v, heatValueFmt, options?.valueCurrency), right: aggLabel } ])
        }
      },
      grid: { left: preview ? 24 : 72, right: preview ? 12 : 28, top: preview ? 24 : 72, bottom: preview ? 16 : 56, containLabel: !preview },
      xAxis: { type: 'category' as const, data: hours, splitArea: { show: true }, axisLabel: { show: !preview, color: isDark() ? 'rgba(226,232,240,0.7)' : '#475569' } },
      yAxis: { type: 'category' as const, data: days, splitArea: { show: true }, axisLabel: { show: !preview, color: isDark() ? 'rgba(226,232,240,0.7)' : '#475569' } },
      visualMap: {
        show: preview ? false : (options?.showLegend ?? true), min, max, calculable: true,
        orient: 'horizontal' as const,
        left: 'center',
        top: (legendPos==='top'?'top':undefined),
        bottom: (legendPos==='bottom'? 6 : undefined),
        inRange: { color: grad, opacity: [0.2, 1] },
        outOfRange: { color: isDark() ? 'rgba(255,255,255,0.06)' : '#f8fafc', opacity: 0.25 },
        textStyle: { color: isDark() ? 'rgba(226,232,240,0.85)' : '#334155' },
        formatter: (val: number) => formatHeat(val, heatValueFmt, options?.valueCurrency),
      },
      series: [{ type: 'heatmap', data: complete, label: { show: false } }],
    }
    return (
      <ErrorBoundary name="HeatmapCard@WeekdayHour">
        <div className="absolute inset-0" ref={containerRef}>
          <ReactECharts key={keyBase} option={option} style={{ height: '100%' }} notMerge={true} lazyUpdate={true} />
        </div>
      </ErrorBoundary>
    )
  }
  return (<div ref={containerRef} className="text-sm text-muted-foreground">No data</div>)
}
