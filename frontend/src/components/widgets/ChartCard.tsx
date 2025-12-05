"use client"

import React, { useMemo, useState, useEffect, useLayoutEffect, useRef } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Api } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import type { WidgetConfig } from '@/types/widgets'
import { useFilters } from '@/components/providers/FiltersProvider'
import { QueryApi } from '@/lib/api'
import type { Row } from '@/lib/formula'
import { compileFormula, parseReferences } from '@/lib/formula'
import type { QuerySpec } from '@/lib/api'
import dynamic from 'next/dynamic'
import ErrorBoundary from '@/components/dev/ErrorBoundary'
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/Tabs'
import type { ComponentType, CSSProperties } from 'react'
import FilterbarControl, { FilterbarShell, FilterbarRuleControl } from '@/components/shared/FilterbarControl'
import { tokenToColorKey, tremorNameToHex, chartColors, getPresetPalette, saturateHexBy, hexToRgb, rgbToHsl, hslToHex } from '@/lib/chartUtils'
import { buildTimelineContext, normalizeCategoryLabel, aggregateCategories, computeColMax, aggregateCategoriesAdvanced } from '@/lib/calcEngine'
import { computePeriodDeltas, computeChangePercent } from '@/lib/deltas'
import HexProgressBar from './HexProgressBar'
import TooltipTable, { TooltipRow } from './TooltipTable'
import ReactDOMServer from 'react-dom/server'
import GanttCard from '@/components/widgets/GanttCard'
import { renderAdvancedAreaChart } from './echarts/AreaAdvanced'
import { renderDonut as renderEchartsDonut, renderPie as renderEchartsPie, renderSunburst as renderEchartsSunburst, renderNightingale as renderEchartsNightingale } from './echarts/PiePresets'
import { renderSankey } from './echarts/SankeyChart'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { RiArrowUpSFill, RiArrowDownSFill, RiArrowRightSFill, RiArrowUpLine, RiArrowDownLine, RiArrowRightLine, RiCalendar2Line, RiArrowDownSLine } from '@remixicon/react'
import {
  ResponsiveContainer,
  AreaChart as ReAreaChart,
  Area as ReArea,
  Tooltip as ReTooltip,
  XAxis as ReXAxis,
  YAxis as ReYAxis,
} from 'recharts'

// Tremor dynamically imports recharts under the hood; ensure client-side
const LineChart: any = dynamic(() => import('@tremor/react').then(m => m.LineChart as any), { ssr: false })
const BarChart: any = dynamic(() => import('@tremor/react').then(m => m.BarChart as any), { ssr: false })
const AreaChart: any = dynamic(() => import('@tremor/react').then(m => m.AreaChart as any), { ssr: false })
const TremorDonutChart = dynamic(
  () => import('@tremor/react').then((m) => (m as any).DonutChart as any),
  { ssr: false }
) as ComponentType<any>
const ReactECharts: any = dynamic(() => import('echarts-for-react').then(m => (m as any).default), { ssr: false })
const TremorCategoryBar = dynamic(
  () => import('@tremor/react').then((m) => (m as any).CategoryBar as any),
  { ssr: false }
) as ComponentType<any>
const TremorProgressBar = dynamic(
  () => import('@tremor/react').then((m) => (m as any).ProgressBar as any),
  { ssr: false }
) as ComponentType<any>
const TremorBarList = dynamic(
  () => import('@tremor/react').then((m) => (m as any).BarList as any),
  { ssr: false }
) as ComponentType<any>
// Note: we render badge presets with raw <span> and Tailwind classes to match Tremor Blocks examples exactly
const Tracker = dynamic(
  () => import('./Tracker').then((m) => (m as any).Tracker),
  { ssr: false }
) as ComponentType<any>
// Tremor Table building blocks (dynamic to keep client rendering consistent)
const TremorTable = dynamic(() => import('@tremor/react').then((m) => (m as any).Table as any), { ssr: false }) as ComponentType<any>
const TremorTableHead = dynamic(() => import('@tremor/react').then((m) => (m as any).TableHead as any), { ssr: false }) as ComponentType<any>
const TremorTableHeaderCell = dynamic(() => import('@tremor/react').then((m) => (m as any).TableHeaderCell as any), { ssr: false }) as ComponentType<any>
const TremorTableBody = dynamic(() => import('@tremor/react').then((m) => (m as any).TableBody as any), { ssr: false }) as ComponentType<any>
const TremorTableRow = dynamic(() => import('@tremor/react').then((m) => (m as any).TableRow as any), { ssr: false }) as ComponentType<any>
const TremorTableCell = dynamic(() => import('@tremor/react').then((m) => (m as any).TableCell as any), { ssr: false }) as ComponentType<any>
const TremorBadge = dynamic(() => import('@tremor/react').then((m) => (m as any).Badge as any), { ssr: false }) as ComponentType<any>

// Helper: proper-case labels (Title Case, replace _ and - with spaces)
function toProperCase(s: string): string {
  const str = String(s ?? '')
  return str
    .replace(/[_-]+/g, ' ')
    .split(/\s+/)
    .map(w => (w ? (w[0].toUpperCase() + w.slice(1).toLowerCase()) : w))
    .join(' ')
}

// Helper: split legend label into base and category parts
// Format: "SeriesName (Category)" => { base: "SeriesName", cat: "Category" }
function splitLegend(label: string): { base: string; cat: string } {
  const str = String(label || '').trim()
  const match = str.match(/^(.+?)\s*\(([^)]+)\)\s*$/)
  if (match) {
    return { base: match[1].trim(), cat: match[2].trim() }
  }
  return { base: str, cat: '' }
}

// Helper: extract base label without category suffix
function extractBaseLabel(label: string): string {
  return splitLegend(label).base
}

// Fallback string values from current query result rows
function fallbackStringsFor(field: string, qdata?: any): string[] {
  try {
    const cols: string[] = (qdata?.columns as string[]) || []
    const rows: any[] = (qdata?.rows as any[]) || []
    if (!cols.length || !rows.length) return []
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

// Shared, tolerant date parser for filterbar type inference
function parseDateLoose(v: any): Date | null {
  if (v == null) return null
  if (v instanceof Date) return isNaN(v.getTime()) ? null : v
  const s = String(v).trim()
  if (!s) return null
  if (/^\d{10,13}$/.test(s)) { const n = Number(s); const ms = s.length === 10 ? n*1000 : n; const d = new Date(ms); return isNaN(d.getTime())?null:d }
  const norm = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
  let d = new Date(norm); if (!isNaN(d.getTime())) return d
  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/); if (iso) { d = new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`); return isNaN(d.getTime())?null:d }
  // Support YYYY-MM (assume first of month)
  const ym = s.match(/^(\d{4})-(\d{2})$/)
  if (ym) { const yyyy = Number(ym[1]); const mm = Math.max(1, Math.min(12, Number(ym[2]))); d = new Date(`${yyyy}-${String(mm).padStart(2,'0')}-01T00:00:00`); return isNaN(d.getTime())?null:d }
  // Support MMM-YYYY or MMM YYYY (English month names)
  const my = s.match(/^([A-Za-z]{3,9})[-\s](\d{4})$/)
  if (my) {
    const map: Record<string, number> = { jan:0,feb:1,mar:2,apr:3,may:4,jun:5,jul:6,aug:7,sep:8,sept:8,oct:9,nov:10,dec:11 }
    const raw = String(my[1]).toLowerCase()
    const key = raw.startsWith('sept') ? 'sep' : raw.slice(0,3)
    const mi = map[key as keyof typeof map]
    if (mi != null) { const yyyy = Number(my[2]); d = new Date(`${yyyy}-${String(mi+1).padStart(2,'0')}-01T00:00:00`); return isNaN(d.getTime())?null:d }
  }
  const m = s.match(/^([0-1]?\d)\/([0-3]?\d)\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/)
  if (m) { const mm=Number(m[1])-1, dd=Number(m[2]), yyyy=Number(m[3]); const t=m[4]||'00:00:00'; d = new Date(`${yyyy}-${String(mm+1).padStart(2,'0')}-${String(dd).padStart(2,'0')}T${t.length===5?t+':00':t}`); return isNaN(d.getTime())?null:d }
  return null
}

// Helper: load distinct string values for a field (omitting self constraint)
function useDistinctStrings(
  source?: string,
  datasourceId?: string,
  baseWhere?: Record<string, any>,
  customCols?: Array<{ name: string; formula: string }>,
) {
  const [cache, setCache] = useState<Record<string, string[]>>({})
  const [loading, setLoading] = useState<Record<string, boolean>>({})
  // Reset cache whenever the underlying constraints or source change
  useEffect(() => { setCache({}); setLoading({}) }, [source, datasourceId, JSON.stringify(baseWhere || {}), JSON.stringify(customCols || [])])
  const load = async (field: string) => {
    try {
      if (!source) return
      // Mark as loading
      setLoading((prev) => ({ ...prev, [field]: true }))
      const isCustom = !!(customCols || []).find((c) => String(c?.name ?? '').trim().toLowerCase() === String(field ?? '').trim().toLowerCase())
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [DistinctsDebug] start', { field, isCustom, baseWhere, source }) } catch {}
      }
      const isDev = (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production')
      const safeQuerySpec = async (payload: any) => {
        try { return await QueryApi.querySpec(payload) } catch (err: any) {
          if (isDev) { try { console.debug('[ChartCard] [DistinctsDebug] query-spec-error', { field, message: err?.message || String(err) }) } catch {} }
          return undefined as any
        }
      }
      if (!isCustom) {
        const omit = { ...(baseWhere || {}) }
        // Do not constrain by the same field (and any ops like __gte/__in)
        Object.keys(omit).forEach((k) => { if (k === field || k.startsWith(`${field}__`)) delete (omit as any)[k] })
        // Try backend distinct endpoint first for base fields (fast and authoritative), then fall back to paging
        try {
          if (typeof (Api as any).distinct === 'function') {
            if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
              try { console.debug('[ChartCard] [DistinctsDebug] request-base-api', { field, source, where: Object.keys(omit).length ? omit : undefined, datasourceId }) } catch {}
            }
            const res = await (Api as any).distinct({ source: String(source), field: String(field), where: Object.keys(omit).length ? omit : undefined, datasourceId })
            const arr = ((res?.values || []) as any[]).map((v) => (v != null ? String(v) : null)).filter((v) => v != null) as string[]
            const dedup = Array.from(new Set(arr).values()).sort()
            if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
              try { console.debug('[ChartCard] [DistinctsDebug] result-base-api', { field, count: dedup.length, sample: dedup.slice(0, 20) }) } catch {}
            }
            setCache((prev) => ({ ...prev, [field]: dedup }))
            setLoading((prev) => ({ ...prev, [field]: false }))
            return
          }
        } catch (e: any) {
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try {
              const msg = e?.message || String(e)
              const m = String(msg).match(/HTTP\s+(\d{3})/)
              const code = m ? Number(m[1]) : null
              const logger = (code && code >= 500) ? console.error : console.debug
              logger('[ChartCard] [DistinctsDebug] result-base-api-error', { field, code, message: msg })
            } catch {}
          }
        }
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
 
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] [DistinctsDebug] page-base', { field, offset, got: rows.length, setSize: setVals.size, idx, cols }) } catch {}
          }
          if (rows.length === 0 || rows.length < pageSize) break
          offset += pageSize
          if (offset >= 500000) break // safety cap (rows scanned)
        }
        const arr = Array.from(setVals.values()).sort()
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try { console.debug('[ChartCard] [DistinctsDebug] result-base', { field, count: arr.length, sample: arr.slice(0, 20), where: omit }) } catch {}
        }
        setCache((prev) => ({ ...prev, [field]: arr }))
        setLoading((prev) => ({ ...prev, [field]: false }))
        return
      }
      // Custom column path: compute values client-side from base refs
      const c = (customCols || []).find((cc) => cc.name === field)!
      const customNames = new Set<string>((customCols || []).map((cc) => cc.name))
      const DERIVED_RE = /^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/
      const serverWhere: Record<string, any> = {}
      Object.entries(baseWhere || {}).forEach(([k, v]) => { if (!DERIVED_RE.test(k) && !customNames.has(k)) (serverWhere as any)[k] = v })
      // Build dependency graph among custom columns and gather base refs
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
        refs.forEach((r) => {
          if (customMap.has(r)) visit(r)
          else baseRefs.add(r)
        })
        order.push(name)
      }
      ;((parseReferences(c.formula).row || []) as string[]).forEach((r) => { if (customMap.has(r)) visit(r); else baseRefs.add(r) })
      const selectBase = Array.from(baseRefs.values())
      // Ensure we also fetch any base fields used in serverWhere filters
      Object.keys(serverWhere).forEach((k) => { if (!selectBase.includes(k)) selectBase.push(k) })
      if (selectBase.length === 0) { setCache((prev) => ({ ...prev, [field]: [] })); setLoading((prev) => ({ ...prev, [field]: false })); return }
      const pageSize = 5000
      let offset = 0
      const cf = compileFormula(c.formula)
      const compiledDeps = new Map<string, ReturnType<typeof compileFormula>>()
      order.forEach((name) => { const def = customMap.get(name)!; compiledDeps.set(name, compileFormula(def.formula)) })
      const setVals = new Set<string>()
      // 1) Try server DISTINCT of baseRef tuples to avoid scanning full table
      try {
        if (selectBase.length > 0) {
          // Build SQL: SELECT DISTINCT [b1],[b2],... FROM [source] WHERE ... ORDER BY 1
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
            // compute dependent custom columns into env
            order.forEach((dep) => { const fn = compiledDeps.get(dep); try { env[dep] = fn ? fn.exec({ row: env }) : null } catch { env[dep] = null } })
            try { const v = cf.exec({ row: env }); if (v != null) setVals.add(String(v)) } catch {}
          })
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] [DistinctsDebug] result-custom-sql', { field, baseTuples: rows.length, setSize: setVals.size, selectBase }) } catch {}
          }
        }
      } catch (e:any) {
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try { console.error('[ChartCard] [DistinctsDebug] result-custom-sql-error', { field, error: e?.message || String(e) }) } catch {}
        }
      }
      // 2) Fallback to incremental paging only if SQL path yielded nothing
      if (setVals.size === 0) {
        while (true) {
          const spec: any = { source, select: selectBase, where: Object.keys(serverWhere).length ? serverWhere : undefined, limit: pageSize, offset }
          const res = await safeQuerySpec({ spec, datasourceId, limit: pageSize, offset, includeTotal: false })
          const cols = ((res?.columns || []) as string[])
          const rows = ((res?.rows || []) as any[])
          rows.forEach((arr: any[]) => {
            const env: Record<string, any> = {}
            cols.forEach((name, i) => { env[name] = arr[i] })
            // compute dependent custom columns into env
            order.forEach((dep) => {
              const fn = compiledDeps.get(dep)
              try { env[dep] = fn ? fn.exec({ row: env }) : null } catch { env[dep] = null }
            })
            try {
              const v = cf.exec({ row: env })
              if (v !== null && v !== undefined) setVals.add(String(v))
            } catch {}
          })
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] [DistinctsDebug] page-custom', { field, offset, got: rows.length, setSize: setVals.size, selectBase }) } catch {}
          }
          if (rows.length === 0 || rows.length < pageSize) break
          offset += pageSize
          if (offset >= 500000) break // safety cap (rows scanned)
        }
      }
      const out = Array.from(setVals.values()).sort()
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [DistinctsDebug] result-custom', { field, count: out.length, sample: out.slice(0, 20), serverWhere }) } catch {}
      }
      setCache((prev) => ({ ...prev, [field]: out }))
      setLoading((prev) => ({ ...prev, [field]: false }))
    } catch (e: any) {
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.error('[ChartCard] [DistinctsDebug] error', { field, error: e?.message || String(e) }) } catch {}
      }
      setCache((prev) => ({ ...prev, [field]: [] }))
      setLoading((prev) => ({ ...prev, [field]: false }))
    }
  }
  return { cache, load, loading }
}

function useDebounced<T>(val: T, delay = 350): T {
  const [v, setV] = useState<T>(val as T)
  useEffect(() => { const t = setTimeout(() => setV(val), delay); return () => { try { clearTimeout(t) } catch {} } }, [val, delay])
  return v
}

export default function ChartCard({
  title,
  sql,
  datasourceId,
  type = 'line',
  options,
  queryMode = 'sql',
  querySpec: querySpecRaw,
  customColumns,
  widgetId,
  pivot,
  reservedTop = 0,
  layout = 'flex',
  tabbedGuard,
  tabbedField,
}: {
  title: string
  sql: string
  datasourceId?: string
  type?: 'line' | 'bar' | 'area' | 'column' | 'donut' | 'categoryBar' | 'spark' | 'combo' | 'badges' | 'progress' | 'tracker' | 'scatter' | 'tremorTable' | 'barList' | 'gantt' | 'sankey'
  options?: WidgetConfig['options']
  queryMode?: 'sql' | 'spec'
  querySpec?: QuerySpec
  customColumns?: WidgetConfig['customColumns']
  widgetId?: string
  pivot?: WidgetConfig['pivot']
  reservedTop?: number
  layout?: 'flex' | 'measure'
  tabbedGuard?: boolean
  tabbedField?: string
}) {
  // Normalize querySpec: strip root-level agg when series is defined (backend prioritizes root-level agg)
  const querySpec = useMemo(() => {
    const qs = querySpecRaw as any
    if (!qs) return qs
    if (Array.isArray(qs.series) && qs.series.length > 0 && qs.agg) {
      const { agg, ...rest } = qs
      if (typeof window !== 'undefined') {
        try { console.log('[ChartCard] Normalized querySpec - stripped root agg', { originalAgg: agg, series: qs.series, before: qs, after: rest }) } catch {}
      }
      return rest as QuerySpec
    }
    if (typeof window !== 'undefined') {
      try { console.log('[ChartCard] querySpec NOT normalized', { hasAgg: !!qs.agg, hasSeries: Array.isArray(qs.series), seriesLength: Array.isArray(qs.series) ? qs.series.length : 0, qs }) } catch {}
    }
    return qs
  }, [querySpecRaw])
  
  const { env } = useEnvironment()
  const { filters } = useFilters()
  const { user } = useAuth()
  const isPreview = useMemo(() => {
    try { return String(widgetId || '').startsWith('ai_prev_') || ((options as any)?.__preview === true) } catch { return false }
  }, [widgetId, options])
  
  // Store ECharts instance for export
  const echartsRef = useRef<any>(null)
  
  // Handle download actions from kebab menu
  useEffect(() => {
    const handleDownload = (e: CustomEvent) => {
      const { widgetId: targetId, format } = e.detail || {}
      if (targetId !== widgetId) return
      
      const instance = echartsRef.current?.getEchartsInstance?.()
      if (!instance) {
        console.warn('[ChartCard] No ECharts instance available for download')
        return
      }
      
      try {
        const fileName = `${title || 'chart'}_${new Date().toISOString().split('T')[0]}`
        
        if (format === 'png') {
          const url = instance.getDataURL({
            type: 'png',
            pixelRatio: 2,
            backgroundColor: '#fff'
          })
          const link = document.createElement('a')
          link.href = url
          link.download = `${fileName}.png`
          link.click()
        } else if (format === 'svg') {
          const url = instance.getDataURL({
            type: 'svg',
            backgroundColor: '#fff'
          })
          const link = document.createElement('a')
          link.href = url
          link.download = `${fileName}.svg`
          link.click()
        }
      } catch (err) {
        console.error('[ChartCard] Download failed:', err)
      }
    }
    
    window.addEventListener('widget-download-chart' as any, handleDownload as any)
    return () => {
      window.removeEventListener('widget-download-chart' as any, handleDownload as any)
    }
  }, [widgetId, title])
  const isSnap = useMemo(() => {
    try {
      if (typeof document === 'undefined') return false
      const el = document.getElementById('widget-root')
      return !!el && el.getAttribute('data-snap') === '1'
    } catch { return false }
  }, [])
  // Snapshot readiness helper for ECharts: mark an attribute and fire a window event once
  const chartReadyOnce = useRef(false)
  const markChartReady = useMemo(() => {
    const tryMark = (attempt: number) => {
      try {
        const root = (typeof document !== 'undefined') ? document.getElementById('widget-root') : null
        if (!root) { if (attempt < 8) requestAnimationFrame(() => tryMark(attempt + 1)); return }
        const already = chartReadyOnce.current
        if (!already) {
          const c = root.querySelector('canvas') as HTMLCanvasElement | null
          const rw = root.clientWidth || 0, rh = root.clientHeight || 0
          const cw = c ? ((c.clientWidth || 0) || (c.width || 0)) : rw
          const ch = c ? ((c.clientHeight || 0) || (c.height || 0)) : rh
          const ok = (!c) || ((cw >= Math.max(1, rw - 2)) && (ch >= Math.max(1, rh - 2)))
          if (!ok) { if (attempt < 10) { requestAnimationFrame(() => tryMark(attempt + 1)); return } }
          chartReadyOnce.current = true
          root.setAttribute('data-chart-ready', '1')
          if (typeof window !== 'undefined') {
            try { window.dispatchEvent(new CustomEvent('widget-data-ready')) } catch {}
          }
        }
        try { const t = (typeof performance !== 'undefined' && performance && typeof performance.now === 'function') ? performance.now() : Date.now(); root.setAttribute('data-chart-finished-at', String(t)) } catch {}
      } catch { if (attempt < 10) requestAnimationFrame(() => tryMark(attempt + 1)) }
    }
    return () => tryMark(0)
  }, [])
  useEffect(() => {
    try {
      const root = (typeof document !== 'undefined') ? document.getElementById('widget-root') : null
      if (root) root.setAttribute('data-chart-ready', '0')
      chartReadyOnce.current = false
    } catch {}
  }, [widgetId])
  // Adaptive grouping when zooming (applies when largeScale is enabled)
  const [adaptiveGb, setAdaptiveGb] = useState<string | undefined>(undefined)
  const updateAdaptiveGroupingFromZoom = (startVal: any, endVal: any) => {
    try {
      if (!((options as any)?.largeScale)) return
      const baseGb = String(((querySpec as any)?.groupBy || 'none') as any).toLowerCase()
      const ds = parseDateLoose(startVal)
      const de = parseDateLoose(endVal)
      if (!ds || !de) { if (adaptiveGb) setAdaptiveGb(undefined); return }
      const spanDays = Math.abs(de.getTime() - ds.getTime()) / 86400000
      let next: string = 'day'
      if (spanDays > 1200) next = 'year'
      else if (spanDays > 365) next = 'month'
      else if (spanDays > 120) next = 'week'
      else next = 'day'
      // Only override when different from base
      if (next === baseGb || next === 'none') {
        if (adaptiveGb) setAdaptiveGb(undefined)
      } else if (adaptiveGb !== next) {
        setAdaptiveGb(next)
      }
    } catch {}
  }
  // Gridline helpers (Configurator > Grid tab)
  const gridCfg = (options as any)?.chartGrid || {}
  const toRgba = (hex?: string, a?: number, fallback = '#94a3b8') => {
    const h = String(hex || fallback)
    const alpha = (typeof a === 'number') ? Math.max(0, Math.min(1, a)) : 1
    try {
      const m = /^#?([\da-f]{2})([\da-f]{2})([\da-f]{2})$/i.exec(h)
      if (!m) return h
      const r = parseInt(m[1], 16), g = parseInt(m[2], 16), b = parseInt(m[3], 16)
      return `rgba(${r}, ${g}, ${b}, ${alpha})`
    } catch { return h }
  }
  const buildAxisGrid = (axis: 'x'|'y') => {
    const node = axis === 'x' ? (gridCfg.vertical || {}) : (gridCfg.horizontal || {})
    const main = node.main || {}
    const sec = node.secondary || {}
    const mainMode = String(main.mode || 'default')
    const secMode = String(sec.mode || 'default')
    const mainActive = mainMode === 'custom'
    const secActive = secMode === 'custom'
    const mainShow = !!main.show
    const mainType = (main.type || 'solid') as ('solid'|'dashed'|'dotted')
    const mainWidth = Number(main.width ?? 1)
    const mainColor = toRgba(main.color, (typeof main.opacity === 'number') ? main.opacity : 0.25)
    const secShow = !!sec.show
    const secType = (sec.type || 'dashed') as ('solid'|'dashed'|'dotted')
    const secWidth = Number(sec.width ?? 1)
    const secColor = toRgba(sec.color, (typeof sec.opacity === 'number') ? sec.opacity : 0.2)
    const out: any = {}
    if (mainActive) out.splitLine = { show: mainShow, lineStyle: { type: mainType, width: mainWidth, color: mainColor } }
    if (secActive) {
      out.minorTick = secShow ? { show: true, splitNumber: 2 } : { show: false }
      out.minorSplitLine = secShow ? { show: true, lineStyle: { type: secType, width: secWidth, color: secColor } } : { show: false }
    }
    return out
  }
  // Map per-section grid settings to CSS variables for Tremor/Recharts
  const buildTremorGridStyle = () => {
    const style: Record<string, string> = {}
    try {
      const h: any = (gridCfg as any)?.horizontal || {}
      const v: any = (gridCfg as any)?.vertical || {}
      const hMain: any = h.main || {}
      const vMain: any = v.main || {}
      if (String(hMain.mode || 'default') === 'custom') {
        const show = !!hMain.show
        const stroke = show ? toRgba(hMain.color, (typeof hMain.opacity === 'number') ? hMain.opacity : 0.25) : 'transparent'
        const width = String(Number(hMain.width ?? 1))
        const dash = (hMain.type === 'dashed') ? '4 4' : (hMain.type === 'dotted') ? '1 3' : '0'
        style['--grid-h-stroke'] = stroke
        style['--grid-h-width'] = width
        style['--grid-h-dash'] = dash
      }
      if (String(vMain.mode || 'default') === 'custom') {
        const show = !!vMain.show
        const stroke = show ? toRgba(vMain.color, (typeof vMain.opacity === 'number') ? vMain.opacity : 0.25) : 'transparent'
        const width = String(Number(vMain.width ?? 1))
        const dash = (vMain.type === 'dashed') ? '4 4' : (vMain.type === 'dotted') ? '1 3' : '0'
        style['--grid-v-stroke'] = stroke
        style['--grid-v-width'] = width
        style['--grid-v-dash'] = dash
      }
    } catch {}
    return style as React.CSSProperties
  }
  const series = useMemo(() => {
    const s = (querySpec as any)?.series as Array<{ label?: string; x?: string; y?: string; agg?: string; groupBy?: string; measure?: string; colorToken?: number }> | undefined
    if (Array.isArray(s) && s.length > 0) return s
    const pv = ((pivot as any)?.values || []) as Array<{ field?: string; agg?: any; label?: string; secondaryAxis?: boolean }>
    const px = (pivot as any)?.x as string | undefined
    if (Array.isArray(pv) && pv.length > 0) {
      return pv.filter((v) => !!v?.field).map((v, i) => ({
        x: ((querySpec as any)?.x || px) as any,
        y: String(v.field),
        agg: (v.agg as any) || ((querySpec as any)?.agg as any) || 'count',
        label: (v.label as any) || String(v.field) || `series_${i + 1}`,
        secondaryAxis: !!v.secondaryAxis,
      })) as any
    }
    return s
  }, [JSON.stringify((querySpec as any)?.series || []), JSON.stringify(pivot || {})]) as Array<{ label?: string; x?: string; y?: string; agg?: string; groupBy?: string; measure?: string; colorToken?: number }> | undefined
  const legendAny = (((querySpec as any)?.legend) ?? ((pivot as any)?.legend)) as any
  const hasLegend = Array.isArray(legendAny) ? ((legendAny as any[]).length > 0) : !!legendAny
  // Local UI-driven filters (Filterbars) merged into where
  const [uiWhere, setUiWhere] = useState<Record<string, any>>({})
  const setUiWhereLocal = (patch: Record<string, any>, emit = true) => {
    setUiWhere((prev) => {
      const next = { ...prev }
      Object.entries(patch).forEach(([k, v]) => {
        if (v === undefined) delete (next as any)[k]
        else (next as any)[k] = v
      })
      return next
    })
    if (emit && typeof window !== 'undefined' && widgetId) {
      try { window.dispatchEvent(new CustomEvent('chart-where-change', { detail: { widgetId, patch } } as any)) } catch {}
    }
  }
  const setUiWhereAndEmit = (patch: Record<string, any>) => setUiWhereLocal(patch, true)
  const setUiWhereAndDontEmit = (patch: Record<string, any>) => setUiWhereLocal(patch, false)

  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string; patch?: Record<string, any> }
      if (!d?.widgetId || d.widgetId !== widgetId) return
      const patch = d.patch || {}
      // Apply incoming builder/config changes locally without re-emitting to avoid loops
      setUiWhereAndDontEmit(patch)
    }
    if (typeof window !== 'undefined') window.addEventListener('config-where-change', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('config-where-change', handler as EventListener) }
  }, [widgetId])

  // Compute exposed fields from config overrides + base where keys
  const fieldsExposed = useMemo(() => {
    const candidate = new Set<string>()
    ;((pivot?.filters || []) as string[]).forEach((f) => { if (f) candidate.add(f) })
    const ex = (options?.filtersExpose || {})
    Object.keys(ex).forEach((k) => { if (ex[k]) candidate.add(k) })
    const shouldExpose = (f: string) => (
      (options?.filtersExpose && typeof options.filtersExpose[f] === 'boolean')
        ? !!options.filtersExpose[f]
        : ((options?.filtersUI === 'filterbars'))
    )
    return Array.from(candidate).filter(shouldExpose)
  }, [options?.filtersUI, options?.filtersExpose, pivot])

  // Helper: UI is source of truth for exposed fields — remove base where for them and apply UI where
  const mergeUiAsTruth = (base: Record<string, any>): Record<string, any> => {
    const effective: Record<string, any> = { ...base }
    const rmKeysFor = (f: string) => {
      // When rendering inside a tabbed context, preserve the tab filter so panes stay filtered
      if (tabbedGuard && tabbedField && String(f) === String(tabbedField)) return
      delete effective[f]
      delete effective[`${f}__gte`]
      delete effective[`${f}__lte`]
      delete effective[`${f}__gt`]
      delete effective[`${f}__lt`]
    }
    fieldsExposed.forEach((f) => {
      rmKeysFor(f)
      const val = (uiWhere as any)[f]
      const gte = (uiWhere as any)[`${f}__gte`]
      const lte = (uiWhere as any)[`${f}__lte`]
      const gt = (uiWhere as any)[`${f}__gt`]
      const lt = (uiWhere as any)[`${f}__lt`]
      // Skip overriding the tabbed field while inside a tabbed pane
      if (tabbedGuard && tabbedField && String(f) === String(tabbedField)) return
      if (Array.isArray(val) && val.length > 0) (effective as any)[f] = val
      if (gte != null) (effective as any)[`${f}__gte`] = gte
      if (lte != null) (effective as any)[`${f}__lte`] = lte
      if (gt != null) (effective as any)[`${f}__gt`] = gt
      if (lt != null) (effective as any)[`${f}__lt`] = lt
    })
    return effective
  }
  const uiTruthWhere = useMemo(
    () => mergeUiAsTruth((((querySpec as any)?.where || {}) as Record<string, any>)),
    [JSON.stringify((querySpec as any)?.where || {}), JSON.stringify(uiWhere), JSON.stringify(fieldsExposed)]
  )

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
  const specReadySingle = queryMode === 'spec' && !!querySpec && !!(querySpec as any).source && Array.isArray((querySpec as any).select) && ((querySpec as any).select as any[]).length > 0
  const specReadyMulti = queryMode === 'spec' && !!querySpec && !!(querySpec as any).source && Array.isArray(series) && series.length > 0 && (!!(querySpec as any).x || (series || []).some((s) => !!s.x))
  const specReadyAggish = queryMode === 'spec' && !!querySpec && !!(querySpec as any).source && (!!(querySpec as any).x || !!(querySpec as any).y || !!(querySpec as any).measure || !!(querySpec as any).legend)
  const uiKey = useMemo(() => JSON.stringify(uiTruthWhere), [uiTruthWhere])
  const debouncedUiKey = useDebounced(uiKey, 350)

  // Datasource defaults (sort/Top N) indicator
  const dsDefaultsQ = useQuery({
    queryKey: ['ds-transforms', datasourceId],
    queryFn: () => Api.getDatasourceTransforms(String(datasourceId)),
    enabled: !!datasourceId,
    staleTime: 60 * 1000,
  })

  // Derive effective data defaults from querySpec ranking hints when explicit overrides are absent
  const dataDefaultsEff = useMemo(() => {
    try {
      const base: any = (options as any)?.dataDefaults ? { ...(options as any).dataDefaults } : {}
      const qbOrderBy = String(((querySpec as any)?.orderBy || '') as any).toLowerCase()
      const qbOrder = String(((querySpec as any)?.order || '') as any).toLowerCase() as ('asc'|'desc'|'' )
      const qbLimitRaw = (querySpec as any)?.limit
      const qbLimit = (typeof qbLimitRaw === 'number') ? qbLimitRaw : (qbLimitRaw != null ? Number(qbLimitRaw) : undefined)
      if ((!base?.topN || !base.topN?.n) && qbOrderBy === 'value' && qbLimit && qbLimit > 0) {
        base.topN = { n: qbLimit, by: 'value', direction: (qbOrder === 'asc' ? 'asc' : 'desc') }
      }
      if ((!base?.sort || !base.sort?.by) && (qbOrderBy === 'x' || qbOrderBy === 'value')) {
        base.sort = { by: qbOrderBy as any, direction: (qbOrder === 'asc' ? 'asc' : 'desc') }
      }
      if (base?.topN || base?.sort) base.useDatasourceDefaults = false
      return base
    } catch { return (options as any)?.dataDefaults }
  }, [JSON.stringify((options as any)?.dataDefaults || {}), (querySpec as any)?.orderBy, (querySpec as any)?.order, (querySpec as any)?.limit])

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

  // Respect global break-link per widget (disable applying global filters in SQL mode)
  const baseId = useMemo(() => String(widgetId || '').split('::')[0], [widgetId])
  const ignoreGlobal = useMemo(() => {
    if (isPreview) return true
    try { return !!(baseId && localStorage.getItem(`gf_break_${baseId}`) === '1') } catch { return false }
  }, [baseId, isPreview])

  // React to break-link toggles to invalidate queries
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

  // Viewport gating: only enable queries when visible
  const containerRef = useRef<HTMLDivElement | null>(null)
  const headerUIRef = useRef<HTMLDivElement | null>(null)
  const legendTopRef = useRef<HTMLDivElement | null>(null)
  const legendBottomRef = useRef<HTMLDivElement | null>(null)
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

  // Tabs from a filter field: fan-out the same chart into one tab per unique value
  const tabsField = (options as any)?.tabsField as string | undefined
  const tabsVariant = ((options as any)?.tabsVariant || 'line') as 'line' | 'solid'
  const tabsMaxItems = Math.max(1, Number((options as any)?.tabsMaxItems ?? 8))
  const tabsStretch = !!(options as any)?.tabsStretch
  const tabsShowAll = !!(options as any)?.tabsShowAll
  const tabsSort = (options as any)?.tabsSort as { by?: 'x' | 'value'; direction?: 'asc' | 'desc' } | undefined
  const tabsLabelCase = (((options as any)?.tabsLabelCase || 'legend') as 'legend'|'lowercase'|'capitalize'|'proper')
  const { cache: tabsCache, load: loadTabsVals } = useDistinctStrings((querySpec as any)?.source, datasourceId, uiTruthWhere as any, (customColumns || []) as any)
  const tabValues = useMemo(() => {
    if (!tabsField) return [] as string[]
    const vals = (tabsCache?.[tabsField] || []) as string[]
    return vals
  }, [tabsCache, tabsField])
  // Optional per-value totals for Aggregate sorting (base fields only)
  const [tabTotals, setTabTotals] = useState<Record<string, number> | null>(null)
  useEffect(() => {
    const run = async () => {
      try {
        if (!tabsField || tabsSort?.by !== 'value') { setTabTotals(null); return }
        const isCustomTabs = !!(customColumns || []).find((c) => c.name === tabsField)
        const baseVals = [...tabValues]
        const totals: Record<string, number> = {}
        if (!isCustomTabs) {
          // Base field path: server filter by value and sum numeric columns
          for (const v of baseVals) {
            const where: Record<string, any> = { ...(uiTruthWhere || {}) }
            ;(where as any)[tabsField] = [v]
            const specBaseRaw: any = { ...(querySpec as any), where }
            if (Array.isArray(specBaseRaw.x)) specBaseRaw.x = specBaseRaw.x[0]
            const specBase = specBaseRaw
            const skip = new Set<string>([String(tabsField), String((querySpec as any)?.x || ''), String((querySpec as any)?.legend || '')].filter(Boolean))
            let sum = 0
            const pageSize = isPreview ? 100 : 2000
            let offset = 0
            while (true) {
              const res = await QueryApi.querySpec({ spec: specBase as any, datasourceId, limit: pageSize, offset, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
              const cols = (res?.columns || []) as string[]
              const rows = (res?.rows || []) as any[]
              rows.forEach((arr: any[]) => {
                cols.forEach((c, i) => {
                  if (skip.has(c)) return
                  const val = Array.isArray(arr) ? arr[i] : (arr as any)?.[c]
                  const num = Number(val)
                  if (Number.isFinite(num)) sum += num
                })
              })
              if (rows.length < pageSize || isPreview) break
              offset += pageSize
              if (offset >= (isPreview ? 100 : 20000)) break // safety cap
            }
            totals[String(v)] = sum
          }
          setTabTotals(totals)
          return
        }
        // Custom tabs path: compute dependencies and evaluate target per row, then sum numeric fields per computed value
        const target = (customColumns || []).find((cc) => cc.name === tabsField)!
        const customMap = new Map<string, { name: string; formula: string }>((customColumns || []).map((cc) => [cc.name, cc]))
        const visited = new Set<string>()
        const order: string[] = []
        const baseRefs = new Set<string>()
        const visit = (name: string) => {
          if (visited.has(name)) return
          visited.add(name)
          const def = customMap.get(name); if (!def) return
          const refs = (parseReferences(def.formula).row || []) as string[]
          refs.forEach((r) => { if (customMap.has(r)) visit(r); else baseRefs.add(r) })
          order.push(name)
        }
        ;((parseReferences(target.formula).row || []) as string[]).forEach((r) => { if (customMap.has(r)) visit(r); else baseRefs.add(r) })
        const selectBase = Array.from(baseRefs.values())
        if (selectBase.length === 0) { setTabTotals(null); return }
        const compiledDeps = new Map<string, ReturnType<typeof compileFormula>>()
        order.forEach((n) => { const def = customMap.get(n)!; compiledDeps.set(n, compileFormula(def.formula)) })
        const cfTarget = compileFormula(target.formula)
        const allowed = new Set(baseVals.map(String))
        const skipCols = new Set<string>([String((querySpec as any)?.x || ''), String((querySpec as any)?.legend || '')].filter(Boolean))
        const pageSize = isPreview ? 100 : 2000
        let offset = 0
        while (true) {
          const specToSend = { ...(querySpec as any), select: selectBase, where: Object.keys(uiTruthWhere || {}).length ? (uiTruthWhere as any) : undefined } as any
          if (typeof window !== 'undefined') {
            try { console.log('[ChartCard] Sending query to backend', { hasRootAgg: !!specToSend.agg, hasSeries: !!specToSend.series, spec: specToSend }) } catch {}
          }
          const res = await QueryApi.querySpec({ spec: specToSend, datasourceId, limit: pageSize, offset, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
          const cols = (res?.columns || []) as string[]
          const rows = (res?.rows || []) as any[]
          rows.forEach((arr: any[]) => {
            const env: Record<string, any> = {}
            cols.forEach((c, i) => { env[c] = arr[i] })
            // Compute dependent customs
            order.forEach((dep) => { const fn = compiledDeps.get(dep); try { env[dep] = fn ? fn.exec({ row: env }) : null } catch { env[dep] = null } })
            // Compute target value for this row
            let tv: any = null
            try { tv = cfTarget.exec({ row: env }) } catch {}
            if (tv === null || tv === undefined) return
            const key = String(tv)
            if (!allowed.has(key)) return
            // Sum numeric entries in env (excluding dimension fields)
            let sumRow = 0
            Object.entries(env).forEach(([k, v]) => {
              if (k === tabsField || skipCols.has(k)) return
              const num = Number(v)
              if (Number.isFinite(num)) sumRow += num
            })
            totals[key] = (totals[key] || 0) + sumRow
          })
          if (rows.length < pageSize || isPreview) break
          offset += pageSize
          if (offset >= (isPreview ? 100 : 20000)) break
        }
        setTabTotals(totals)
      } catch { setTabTotals(null) }
    }
    void run()
  }, [tabsField, tabsSort?.by, JSON.stringify(uiTruthWhere || {}), JSON.stringify(tabValues), tabsMaxItems, datasourceId, JSON.stringify(querySpec || {}), JSON.stringify(customColumns || [])])
  // Effective tab values
  const effectiveTabValues = useMemo(() => tabValues, [tabValues])
  // Optional sorting of tabs
  const sortedTabValues = useMemo(() => {
    const vals = [...effectiveTabValues]
    if (!tabsSort?.by) return vals
    const asc = (tabsSort.direction || 'asc') === 'asc'
    if (tabsSort.by === 'x') {
      const allDates = vals.every((v) => !!parseDateLoose(String(v)))
      vals.sort((a, b) => {
        if (allDates) {
          const da = parseDateLoose(String(a))!.getTime(); const db = parseDateLoose(String(b))!.getTime()
          return asc ? (da - db) : (db - da)
        }
        const ca = String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' })
        return asc ? ca : -ca
      })
    } else if (tabsSort.by === 'value') {
      if (tabTotals && Object.keys(tabTotals).length) {
        vals.sort((a, b) => {
          const av = Number(tabTotals[String(a)] ?? 0)
          const bv = Number(tabTotals[String(b)] ?? 0)
          const cmp = av - bv
          return asc ? cmp : -cmp
        })
      } else {
        // Fallback to label sort
        vals.sort((a, b) => {
          const ca = String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' })
          return asc ? ca : -ca
        })
      }
    }
    return vals
  }, [effectiveTabValues, tabsSort?.by, tabsSort?.direction, JSON.stringify(tabTotals || {})])
  useEffect(() => { if (tabsField) void loadTabsVals(tabsField) }, [tabsField, (querySpec as any)?.source, datasourceId, uiKey, JSON.stringify(uiTruthWhere || {}), JSON.stringify(customColumns || [])])

  const wantTabs = !!tabsField && !tabbedGuard && ((sortedTabValues.length > 0) || tabsShowAll)
  const allPrefixedValues = useMemo(() => (
    tabsShowAll ? ['__ALL__', ...sortedTabValues] : sortedTabValues
  ), [tabsShowAll, sortedTabValues])
  const tabsListClass = "w-full"
  const defaultTabValue = useMemo(() => (allPrefixedValues.length > 0 ? String(allPrefixedValues[0]) : ''), [allPrefixedValues])
  const [activeTab, setActiveTab] = useState<string>(defaultTabValue)
  useEffect(() => { if (defaultTabValue) setActiveTab(defaultTabValue) }, [defaultTabValue])
  const q = useQuery({
    queryKey: ['chart', sql, datasourceId, type, options, queryMode, querySpec, customColumns, filters, debouncedUiKey, adaptiveGb, breakSeq, (querySpec as any)?.series?.[0]?.agg],
    enabled: visible && (queryMode === 'spec' ? (specReadySingle || specReadyMulti || specReadyAggish) : true),
    placeholderData: (prev) => prev as any,
    queryFn: async () => {
      if (queryMode === 'spec' && querySpec) {
        // Fields validator: if value field is required and empty, do not call backend.
        // Exception: tremorTable does not require a value field.
        try {
          const requiresValue = (type !== 'tremorTable')
          const hasPivotValues = Array.isArray((pivot as any)?.values) && ((pivot as any).values.some((v:any)=>!!(v?.field||v?.measureId)))
          const seriesDefs = Array.isArray((querySpec as any)?.series) ? ((querySpec as any).series as any[]) : []
          const hasSeriesValue = seriesDefs.some((s:any)=>!!(s?.y || s?.measure))
          const hasDirectValue = !!((querySpec as any)?.y || (querySpec as any)?.measure)
          const hasAnyValue = hasPivotValues || hasSeriesValue || hasDirectValue
          if (requiresValue && !hasAnyValue) {
            return { columns: [], rows: [], totalRows: 0, elapsedMs: 0 }
          }
        } catch {}
        const seriesArr = Array.isArray(series) ? series : []
        const customNames = new Set<string>((customColumns || []).map((c) => c.name))
        const rawXField = (querySpec as any)?.x || seriesArr.find((s) => !!s?.x)?.x
        const xField = (Array.isArray(rawXField) ? (rawXField[0] as any) : rawXField) as string | undefined
        const baseWhere: Record<string, any> = { ...(querySpec.where || {}) }
        let df = (options as any)?.deltaDateField as string | undefined
        if (!df) {
          const gb = (((querySpec as any)?.groupBy) || 'none') as string
          const xFmtDatetime = ((options as any)?.xLabelFormat || 'none') === 'datetime'
          // If xField is a derived label like "Date (Month)" use the base column as delta date field
          const xBaseForDelta = (() => { try { const m = String(xField || '').match(/^(.*)\s\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/); return m ? m[1] : xField } catch { return xField } })()
          if (xBaseForDelta && (gb !== 'none' || xFmtDatetime)) df = String(xBaseForDelta)
          
          // For Sankey and other charts without date in X/Y: auto-detect date field
          // This ensures global date filters are applied even when date isn't visualized
          if (!df && (filters.startDate || filters.endDate) && !ignoreGlobal) {
            // Try to infer from querySpec fields using common date field name patterns
            const pick = (name?: string) => {
              const s = String(name || '')
              return s && /(date|time|timestamp|created|updated|_at)$/i.test(s) ? s : undefined
            }
            // Check all querySpec fields (x, y, legend, etc.)
            const candidates = [
              pick((querySpec as any)?.x),
              pick((querySpec as any)?.y),
              pick((querySpec as any)?.legend),
            ].filter(Boolean)
            if (candidates.length > 0) df = candidates[0]
            
            // Log warning if global filters exist but no date field detected
            if (!df && typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
              try {
                console.warn('[ChartCard] Global date filters are active but no date field detected. Set options.deltaDateField to apply filters.', {
                  title, type,
                  hasStartDate: !!filters.startDate,
                  hasEndDate: !!filters.endDate,
                  querySpec: { x: (querySpec as any)?.x, y: (querySpec as any)?.y, legend: (querySpec as any)?.legend }
                })
              } catch {}
            }
          }
        }
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
        // Build effective where using the shared helper (UI is truth for exposed fields),
        // applied over baseWhere that already includes delta date filters.
        const mergedWhere = mergeUiAsTruth(baseWhere)
        const legendField = (querySpec as any)?.legend || (pivot as any)?.legend
        const legendIsArray = Array.isArray(legendField) && ((legendField as any[]).length > 0)
        const legendArrayTooMany = Array.isArray(legendField) && ((legendField as any[]).length > 1)
        const legendPresent = Array.isArray(legendField) ? ((legendField as any[]).length > 0) : !!legendField
        const usingCustomInSeriesY = seriesArr.some((s) => s?.y && customNames.has(String(s.y)))
        const usingCustomInSingleY = (!!(querySpec as any).y && customNames.has(String((querySpec as any).y)))
        const usingCustomX = !!xField && customNames.has(String(xField))
        const usingCustomLegend = legendIsArray
          ? ((legendField as any[]).some((f) => !!f && customNames.has(String(f))))
          : (!!legendField && customNames.has(String(legendField)))
        const whereKeys = Object.keys((mergedWhere as any) || {})
        const DERIVED_RE = /^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/
        const hasDerivedWhere = whereKeys.some((k) => DERIVED_RE.test(k))
        const hasCustomWhere = whereKeys.some((k) => customNames.has(k))
        const serverWhere: Record<string, any> = {}
        whereKeys.forEach((k) => { if (!customNames.has(k)) (serverWhere as any)[k] = (mergedWhere as any)[k] })
        // Only custom column filters require client-side fallback now
        const requireClientWhere = hasCustomWhere
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try {
            // eslint-disable-next-line no-console
            console.debug('[ChartCard] where-build', {
              title,
              hasLegend,
              isMulti,
              whereKeys,
              mergedWhere,
              serverWhere,
              hasDerivedWhere,
              hasCustomWhere,
              requireClientWhere,
            })
            // Additional filter-focused trace
            // eslint-disable-next-line no-console
            console.debug('[ChartCard] [FiltersDebug] where', { title, hasLegend, isMulti, mergedWhere, serverWhere })
          } catch {}
        }
        const gbToken = String(((adaptiveGb ?? (querySpec as any)?.groupBy) || 'none') as any).toLowerCase()
        const serverAggWanted = (
          legendPresent ||
          (gbToken !== 'none') ||
          (Array.isArray(seriesArr) && seriesArr.length > 0) ||
          (String((((querySpec as any)?.agg) || 'none') as any).toLowerCase() !== 'none') ||
          !!(querySpec as any)?.measure
        )
        const hasCustomAnywhere = usingCustomInSeriesY || usingCustomInSingleY || usingCustomX || usingCustomLegend || requireClientWhere
        const safeForServerAgg = !usingCustomInSeriesY && !usingCustomInSingleY && !usingCustomX && !usingCustomLegend && !requireClientWhere && !legendArrayTooMany
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try {
            // eslint-disable-next-line no-console
            console.debug('[ChartCard] [AggGate]', {
              title,
              legendField,
              legendIsArray,
              legendArrayTooMany,
              legendPresent,
              seriesLen: seriesArr.length,
              xField,
              agg: (querySpec as any)?.agg,
              groupBy: (querySpec as any)?.groupBy,
              usingCustomInSeriesY,
              usingCustomInSingleY,
              usingCustomX,
              usingCustomLegend,
              hasDerivedWhere,
              hasCustomWhere,
              requireClientWhere,
              serverAggWanted,
              safeForServerAgg,
              going: (hasCustomAnywhere && !(serverAggWanted && safeForServerAgg)) ? 'client-custom' : 'server-agg',
            })
          } catch {}
        }
        // Prefer server aggregation when semantics are present and safe.
        // Force client pipeline when multi-legend is requested (legend array length > 1)
        // EXCEPTION: Sankey charts MUST use server-side aggregation to get [x, legend, value] format
        const gbOverrideActive = !!((options as any)?.largeScale) && !!adaptiveGb && String(adaptiveGb).toLowerCase() !== String(((querySpec as any)?.groupBy || 'none')).toLowerCase()
        if (type !== 'sankey' && (legendArrayTooMany || (hasCustomAnywhere && !(serverAggWanted && safeForServerAgg)) || gbOverrideActive)) {
          // Determine which custom columns are used
          const usedNames = new Set<string>()
          if (usingCustomInSeriesY) seriesArr.forEach((s) => { if (s?.y && customNames.has(String(s.y))) usedNames.add(String(s.y)) })
          if (usingCustomInSingleY) usedNames.add(String((querySpec as any).y))
          if (usingCustomX) usedNames.add(String(xField))
          if (usingCustomLegend) usedNames.add(String(legendField))
          const used = (customColumns || []).filter((c) => usedNames.has(c.name))
          // Collect base columns needed to compute used customs and derived filters
          const baseRefs = new Set<string>()
          used.forEach((c) => { const refs = parseReferences(c.formula); (refs.row || []).forEach((r: string) => baseRefs.add(r)) })
          // Include base fields referenced by filters (avoid adding op-suffixed keys like field__gte)
          const GLOBAL_KEYS = new Set(['start','startDate','end','endDate'])
          whereKeys.forEach((k) => {
            if (GLOBAL_KEYS.has(k)) return
            const m = k.match(DERIVED_RE)
            if (m) { const base = m[1]; baseRefs.add(base); return }
            if (k.includes('__')) { const base = k.split('__', 1)[0]; if (!customNames.has(base)) baseRefs.add(base); return }
            if (!customNames.has(k)) { baseRefs.add(k) }
          })

          // And collect Y base fields (for non-custom Ys)
          const yBaseFields = new Set<string>()
          if (seriesArr.length > 0) seriesArr.forEach((s) => { const y = String(s.y || ''); if (y && !customNames.has(y)) yBaseFields.add(y) })
          else if ((querySpec as any)?.y && !customNames.has(String((querySpec as any).y))) yBaseFields.add(String((querySpec as any).y))
          // Select fields: x (if base), legend (if base), baseRefs, y base fields
          const selectFields: string[] = []
          if (xField && !customNames.has(String(xField))) selectFields.push(String(xField))
          // If X is a derived date-part like "field (Month)", also include the base field
          const X_DERIVED = (() => { try { const m = String(xField || '').match(/^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/); return m ? { base: m[1], part: m[2] } : null } catch { return null } })()
          if (X_DERIVED && X_DERIVED.base && !customNames.has(String(X_DERIVED.base))) selectFields.push(String(X_DERIVED.base))
          if (legendField) {
            if (Array.isArray(legendField)) {
              (legendField as any[]).forEach((lf) => { if (lf && !customNames.has(String(lf))) selectFields.push(String(lf)) })
            } else {
              if (!customNames.has(String(legendField))) selectFields.push(String(legendField))
            }
          }
          baseRefs.forEach((b) => { if (!selectFields.includes(b)) selectFields.push(b) })
          yBaseFields.forEach((b) => { if (!selectFields.includes(b)) selectFields.push(b) })
          // Always include xField so we can partition rows by time/category
          if (xField && !selectFields.includes(String(xField))) selectFields.push(String(xField))
          const rawSpec: QuerySpec = { source: querySpec.source, where: serverWhere, select: selectFields, limit: querySpec.limit ?? 1000, offset: querySpec.offset ?? 0 }
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] branch=client-custom/serverFetch', { selectFields, serverWhere }) } catch {}
          }
          // Compile once
          const compiled = new Map<string, ReturnType<typeof compileFormula>>()
          used.forEach((c) => compiled.set(c.name, compileFormula(c.formula)))
          // Fetch strategy:
          // - If grouping by coarse time (year/quarter/month) and a global date range exists, fetch in time windows
          //   to avoid truncation to early records only. Else fall back to offset paging.
          const pageSize = Math.max(1000, Math.min(5000, Number((querySpec.limit ?? 5000))))
          const rows: Row[] = []
          const hasGlobalRange = !!(filters?.startDate) && !ignoreGlobal
          const gbLocal = (() => { try { const sGb = (Array.isArray(seriesArr) ? seriesArr.map((s:any)=>s?.groupBy).find((v:any)=>v && v!=='none') : undefined); return String((sGb || (querySpec as any)?.groupBy || 'none') as any) } catch { return String(((querySpec as any)?.groupBy || 'none') as any) } })()
          const gbCoarse = ['year', 'quarter', 'month'].includes(String(gbLocal))
          const dfField = String((() => {
            try {
              if (df) return df
              const m = String(xField || '').match(/^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/)
              const xBase = m ? m[1] : undefined
              return xBase || xField || ''
            } catch { return xField || '' }
          })())
          const baseNoDf: Record<string, any> = { ...serverWhere }
          // Remove any existing df constraints; we will add per-window below
          Object.keys(baseNoDf).forEach((k) => { if (k === dfField || k.startsWith(`${dfField}__`)) delete (baseNoDf as any)[k] })

          const ymd = (d: Date) => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
          const addDays = (d: Date, n: number) => { const x = new Date(d); x.setDate(x.getDate() + n); return x }
          const nextMonth = (d: Date) => { const x = new Date(d.getFullYear(), d.getMonth() + 1, 1); return x }
          const nextQuarter = (d: Date) => { const m = d.getMonth(); const nm = m - (m % 3) + 3; return new Date(d.getFullYear(), nm, 1) }
          const nextYear = (d: Date) => new Date(d.getFullYear() + 1, 0, 1)

          const fetchWindow = async (start: Date, end: Date) => {
            let offset = 0
            while (true) {
              const where = { ...baseNoDf, [`${dfField}__gte`]: ymd(start), [`${dfField}__lt`]: ymd(end) }
              const res = await QueryApi.querySpec({ spec: { ...rawSpec, where, limit: (isPreview ? 100 : pageSize), offset }, datasourceId, limit: (isPreview ? 100 : pageSize), offset, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
              const cols = (res?.columns || []) as string[]
              const pageRows = ((res?.rows || []) as any[]).map((arr) => {
                const obj: Row = {}
                cols.forEach((c, i) => { obj[c] = arr[i] })
                compiled.forEach((cf, name) => { try { (obj as any)[name] = cf.exec({ row: obj }) } catch { (obj as any)[name] = null } })
                return obj
              })
              rows.push(...pageRows)
              if (pageRows.length < pageSize) break
              offset += pageSize
              if (offset >= 20000) break
            }
          }

          if (gbCoarse && hasGlobalRange && dfField) {
            try {
              const s = new Date(`${filters.startDate}T00:00:00`)
              const e = new Date(`${(filters.endDate || `${s.getFullYear()}-12-31`)}T00:00:00`)
              let cur = new Date(s.getFullYear(), s.getMonth(), 1)
              const endExclusive = addDays(new Date(e.getFullYear(), e.getMonth(), e.getDate()), 1)
              while (cur < endExclusive) {
                const next = (gbLocal === 'year') ? nextYear(cur) : (gbLocal === 'quarter') ? nextQuarter(cur) : nextMonth(cur)
                const stop = next < endExclusive ? next : endExclusive
                await fetchWindow(cur, stop)
                cur = next
                if (rows.length >= 20000) break
              }
            } catch {
              // Fall back to simple paging on error
              let offset = Number(querySpec.offset ?? 0)
              while (true) {
                const res = await QueryApi.querySpec({ spec: { ...rawSpec, limit: (isPreview ? 100 : pageSize), offset }, datasourceId, limit: (isPreview ? 100 : pageSize), offset, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
                const cols = (res?.columns || []) as string[]
                const pageRows = ((res?.rows || []) as any[]).map((arr) => {
                  const obj: Row = {}
                  cols.forEach((c, i) => { obj[c] = arr[i] })
                  compiled.forEach((cf, name) => { try { (obj as any)[name] = cf.exec({ row: obj }) } catch { (obj as any)[name] = null } })
                  return obj
                })
                rows.push(...pageRows)
                if (pageRows.length < pageSize) break
                offset += pageSize
                if (offset >= 20000) break
              }
            }
          } else {
            // Simple paging
            let offset = Number(querySpec.offset ?? 0)
            while (true) {
              const res = await QueryApi.querySpec({ spec: { ...rawSpec, limit: pageSize, offset }, datasourceId, limit: pageSize, offset, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
              if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production' && offset === 0) {
                try { console.debug('[ChartCard] [RAW BACKEND RESPONSE] client-path', { columns: res?.columns, rowCount: res?.rows?.length, firstRow: res?.rows?.[0], secondRow: res?.rows?.[1] }) } catch {}
              }
              const cols = (res?.columns || []) as string[]
              const pageRows = ((res?.rows || []) as any[]).map((arr) => {
                const obj: Row = {}
                cols.forEach((c, i) => { obj[c] = arr[i] })
                compiled.forEach((cf, name) => { try { (obj as any)[name] = cf.exec({ row: obj }) } catch { (obj as any)[name] = null } })
                return obj
              })
              rows.push(...pageRows)
              if (pageRows.length < pageSize) break
              offset += pageSize
              if (offset >= 20000) break
            }
          }
          // Apply client-side filters including derived date parts and custom columns
          const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December']
          const monthShort = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
          const dayNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']
          const dayShort = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']
          const toDate = (v: any): Date | null => {
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
          const weekNumber = (d: Date): number => {
  const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || env.weekStart) === 'sun')
  if (useSun) {
    // Sunday-based week number (1-53)
    const jan1 = new Date(d.getFullYear(), 0, 1)
    const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000)
    return Math.floor((day0 + jan1.getDay()) / 7) + 1
  }
  // ISO week number (Mon-based)
  const date = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()))
  date.setUTCDate(date.getUTCDate() + 4 - (date.getUTCDay() || 7))
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1))
  return Math.ceil(((date.getTime() - yearStart.getTime()) / 86400000 + 1) / 7)
}
          const derive = (part: string, baseVal: any): any => {
            const d = toDate(baseVal); if (!d) return null
            switch (part) {
              case 'Year': return d.getFullYear()
              case 'Quarter': return Math.floor(d.getMonth()/3)+1
              case 'Month': return d.getMonth()+1
              case 'Month Name': return monthNames[d.getMonth()]
              case 'Month Short': return monthShort[d.getMonth()]
              case 'Week': return weekNumber(d)
              case 'Day': return d.getDate()
              case 'Day Name': return dayNames[d.getDay()]
              case 'Day Short': return dayShort[d.getDay()]
              default: return null
            }
          }
          // Only apply client-side filtering for keys that cannot be pushed down to the server:
          // - Custom column names (derived filters are now handled server-side)
          const whereAny = (mergedWhere as any) as Record<string, any>
          const whereKeys2 = Object.keys(whereAny).filter((k) => customNames.has(k))
          const rowsFiltered = whereKeys2.length === 0 ? rows : rows.filter((r) => {
            for (const k of whereKeys2) {
              const rawVal = whereAny[k]
              const vals = Array.isArray(rawVal) ? rawVal.map((v: any) => String(v)) : []
              const dm = k.match(DERIVED_RE)
              let rv: any = null
              if (dm) { rv = derive(dm[2], (r as any)[dm[1]]) }
              else if (customNames.has(k)) {
                const cf = compiled.get(k)
                try { rv = cf ? cf.exec({ row: r }) : null } catch { rv = null }
              } else {
                // Should not happen since we filtered keys above, but keep safe
                rv = (r as any)[k]
              }
              if (vals.length > 0 && !vals.includes(String(rv))) return false
            }
            return true
          })
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] branch=client-custom/filter', { before: rows.length, after: rowsFiltered.length, whereKeys: whereKeys2 }) } catch {}
          }
          const rowsToUse = rowsFiltered
          // Normalize X by groupBy to ensure consistent bucketing on client (e.g., YYYY-MM for month)
          const gbEffective = (() => {
            if ((options as any)?.largeScale && adaptiveGb) return String(adaptiveGb)
            try {
              const sGb = (Array.isArray(seriesArr) ? seriesArr.map((s:any)=>s?.groupBy).find((v:any)=>v && v!=='none') : undefined)
              return String((sGb || (querySpec as any)?.groupBy || 'none') as any)
            } catch { return String(((querySpec as any)?.groupBy || 'none') as any) }
          })()
          const wkStart = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || env.weekStart) as any) || 'mon'
          const normalizeX = (raw: any): any => {
            try {
              if (!raw) return raw
              const s = String(raw)
              const d = parseDateLoose(s)
              if (!d) return raw
              const pad = (n: number) => String(n).padStart(2, '0')
              const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
              const weekSun = (date: Date) => { const jan1 = new Date(date.getFullYear(), 0, 1); const day0 = Math.floor((new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime() - jan1.getTime()) / 86400000); return Math.floor((day0 + jan1.getDay()) / 7) + 1 }
              switch (gbEffective) {
                case 'year': return String(d.getFullYear())
                case 'quarter': return `${d.getFullYear()}-Q${Math.floor(d.getMonth()/3)+1}`
                case 'month': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
                case 'week': {
                  const wn = (wkStart === 'sun') ? weekSun(d) : isoWeek(d)
                  return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}`
                }
                case 'day': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
                default: return raw
              }
            } catch { return raw }
          }
          // Partition helper using normalized X and possibly custom Legend now present on the row
          const X_DERIVED2 = (() => { try { const m = String(xField || '').match(/^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/); return m ? { base: m[1], part: m[2] } : null } catch { return null } })()
          const keyOf = (r: Row) => {
            const raw = (() => {
              if (X_DERIVED2 && X_DERIVED2.base) return (r as any)[String(X_DERIVED2.base)]
              return (xField ? (r as any)[String(xField)] : 'all')
            })()
            return normalizeX(raw)
          }
          const legendKeyOf = (r: Row) => {
            if (!legendField) return undefined
            if (Array.isArray(legendField)) {
              try { return (legendField as any[]).map((f) => String((r as any)[String(f)])) .join(' • ') } catch { return undefined }
            }
            return (r as any)[String(legendField)]
          }
          const partitions = new Map<string | number | 'all', Row[]>()
          rowsToUse.forEach((r) => {
            const k = keyOf(r) as any
            const arr = partitions.get(k) || []
            arr.push(r)
            partitions.set(k, arr)
          })
          const keysSorted = (() => {
            try {
              const keys = Array.from(partitions.keys()) as any[]
              console.log('[ChartCard] Partition keys (insertion order):', keys)
              if (gbEffective === 'none') return keys
              // Check for 12-month seasonality (x=date, groupBy=month, legend=Year)
              const checkSeasonality = (() => {
                try {
                  const gb = String(gbEffective || 'none').toLowerCase()
                  const xF = String(xField || '')
                  const lgRaw = (() => {
                    const lg: any = legendField
                    if (Array.isArray(lg) && lg.length > 0) return String(lg[0] ?? '')
                    return typeof lg === 'string' ? lg : ''
                  })()
                  const m = lgRaw.match(/^(.*)\s*\(Year\)$/i)
                  if (m && gb === 'month' && m[1].trim() === xF) {
                    console.log('[ChartCard] ✅ 12-MONTH SEASONALITY ENABLED (client-custom keysSorted)', { gb, xF, lgRaw })
                    return true
                  }
                  return false
                } catch { return false }
              })()
              // In seasonality mode, preserve backend month order (assume keys are already ordered)
              if (checkSeasonality && gbEffective === 'month') {
                console.log('[ChartCard] ✅ Skipping client-side key sorting (seasonality mode - preserving order)')
                return keys
              }
              const asDates = keys.map((k) => parseDateLoose(String(k))).filter(Boolean) as Date[]
              if (asDates.length === keys.length) return keys.sort((a, b) => (parseDateLoose(String(a))!.getTime() - parseDateLoose(String(b))!.getTime()))
              const nums = keys.filter((k) => !isNaN(Number(k)))
              if (nums.length === keys.length) return keys.sort((a, b) => (Number(a) - Number(b)))
              return keys.sort((a, b) => String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' }))
            } catch { return Array.from(partitions.keys()) as any[] }
          })()
          // Aggregator helper with 'none' support
          function aggregate(arr: Row[], fieldOrCustom: string, agg: any) {
            const isCustom = customNames.has(fieldOrCustom)
            if (!isCustom) {
              const rawVals = arr.map((r) => (r as any)[fieldOrCustom])
              switch (agg) {
                case 'none': {
                  const v = rawVals.find((v) => v !== null && v !== undefined)
                  return typeof v === 'number' ? v : (Number(v as any) || v)
                }
                case 'sum': return rawVals.reduce((a, b) => a + (Number(b) || 0), 0)
                case 'avg': return rawVals.length ? rawVals.reduce((a, b) => a + (Number(b) || 0), 0) / rawVals.length : 0
                case 'min': return rawVals.length ? Math.min(...rawVals.map((v) => Number(v) || 0)) : 0
                case 'max': return rawVals.length ? Math.max(...rawVals.map((v) => Number(v) || 0)) : 0
                case 'distinct': return new Set(rawVals.map((v) => String(v))).size
                case 'count': default: return rawVals.filter((v) => v !== null && v !== undefined).length
              }
            }
            const cf = compiled.get(fieldOrCustom)!
            const vals = arr.map((r) => cf.exec({ row: r }))
            switch (agg) {
              case 'none': {
                const v = vals.find((v) => v !== null && v !== undefined)
                return typeof v === 'number' ? v : (Number(v as any) || v)
              }
              case 'sum': return vals.reduce((a, b) => a + (Number(b) || 0), 0)
              case 'avg': return vals.length ? vals.reduce((a, b) => a + (Number(b) || 0), 0) / vals.length : 0
              case 'min': return vals.length ? Math.min(...vals.map((v) => Number(v) || 0)) : 0
              case 'max': return vals.length ? Math.max(...vals.map((v) => Number(v) || 0)) : 0
              case 'distinct': return new Set(vals.map((v) => String(v))).size
              case 'count': default: return vals.filter((v) => v !== null && v !== undefined).length
            }
          }
          // Multi-series WITH legend (client path): virtualize categories per series -> "Category - Series"
          if (legendField && seriesArr.length > 0) {
            const labelFor = (s: any, idx: number) => s.label || s.y || s.measure || `series_${idx + 1}`
            const outMap = new Map<any, any>()
            const catsSet = new Set<string>()
            const virtualMeta: Record<string, { baseSeriesIndex: number; baseSeriesLabel: string; categoryLabel: string; agg?: string }> = {}
            const hasMultipleSeries = seriesArr.length > 1
            keysSorted.forEach((k) => {
              const arr = partitions.get(k) || []
              const obj: any = { x: k }
              seriesArr.forEach((s, i) => {
                const baseLabel = labelFor(s, i)
                const aggEff = (s.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
                const aggLabel = String(aggEff)
                // group by composite legend inside this partition
                const byLegend = new Map<any, Row[]>()
                arr.forEach((r) => {
                  const lk = legendKeyOf(r) as any
                  const bucket = byLegend.get(lk) || []
                  bucket.push(r)
                  byLegend.set(lk, bucket)
                })
                byLegend.forEach((subset, lk) => {
                  const cat = String(lk)
                  const v = aggregate(subset, String(s.y), aggEff)
                  // Format: "LegendKey - DataSeriesName" if multiple series, else just "LegendKey"
                  const virtualKey = hasMultipleSeries ? `${cat} - ${baseLabel}` : cat
                  catsSet.add(virtualKey)
                  obj[virtualKey] = v
                  virtualMeta[virtualKey] = { baseSeriesIndex: i, baseSeriesLabel: baseLabel, categoryLabel: cat, agg: aggLabel }
                })
              })
              outMap.set(k, obj)
            })
            const categories = Array.from(catsSet.values())
            return { columns: ['x', ...categories], rows: Array.from(outMap.values()), categories, virtualMeta } as any
          }
          // Multi-series values mode (no legend)
          if (!hasLegend && seriesArr.length > 0) {
            // Multi-series values mode
            const labelFor = (s: any, idx: number) => s.label || s.y || s.measure || `series_${idx + 1}`
            const cats = seriesArr.map((s, i) => labelFor(s, i))
            const virtualMeta: Record<string, { baseSeriesIndex: number; baseSeriesLabel: string; agg?: string }> = {}
            seriesArr.forEach((s, i) => {
              const label = labelFor(s, i)
              const aggEff = ((s as any)?.agg ?? ((querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count')))
              virtualMeta[label] = { baseSeriesIndex: i, baseSeriesLabel: label, agg: String(aggEff) }
            })
            const outMap = new Map<any, any>()
            keysSorted.forEach((k) => {
              const arr = partitions.get(k) || []
              const obj: any = { x: k }
              seriesArr.forEach((s, i) => {
                const label = labelFor(s, i)
                const field = String(s.y)
                const aggEff = (s.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
                const v = aggregate(arr, field, aggEff)
                obj[label] = v
              })
              outMap.set(k, obj)
            })
            return { columns: ['x', ...cats], rows: Array.from(outMap.values()).map((o) => cats.map((c) => (o[c] ?? 0)).reduce((acc, _, i) => o, o)), categories: cats, virtualMeta } as any
          }
          // Single series or legend split (only when NO series array is configured)
          if (legendField && seriesArr.length === 0) {
            // If no X field is provided, create multiple series with a single "Total" x-category
            if (!xField) {
              const byLegend = new Map<any, Row[]>()
              rowsToUse.forEach((r) => {
                const lk = legendKeyOf(r) as any
                const bucket = byLegend.get(lk) || []
                bucket.push(r)
                byLegend.set(lk, bucket)
              })
              const categories: string[] = []
              const obj: any = { x: (querySpec as any)?.y || 'Total' }
              byLegend.forEach((subset, lk) => {
                const legendKey = String(lk)
                categories.push(legendKey)
                const field = String((querySpec as any).y)
                const seriesAgg = (Array.isArray((querySpec as any)?.series) && (querySpec as any).series.length > 0) ? (querySpec as any).series[0]?.agg : undefined
                const aggEff = seriesAgg || (querySpec as any).agg || (field ? 'sum' : 'count')
                const v = aggregate(subset, field, aggEff)
                obj[legendKey] = v
              })
              return { columns: ['x', ...categories], rows: [obj], categories } as any
            }
            // Otherwise pivot into categories per legend for each X bucket
            const catsSet = new Set<string>()
            const outer = new Map<any, any>()
            keysSorted.forEach((k) => {
              const arr = partitions.get(k) || []
              const obj: any = { x: k }
              // group by legend inside
              const byLegend = new Map<any, Row[]>()
              arr.forEach((r) => {
                const lk = legendKeyOf(r) as any
                const bucket = byLegend.get(lk) || []
                bucket.push(r)
                byLegend.set(lk, bucket)
              })
              byLegend.forEach((subset, lk) => {
                catsSet.add(String(lk))
                const field = String((querySpec as any).y)
                // Use series[0].agg if available, fallback to querySpec.agg, then y-based default
                const seriesAgg = (Array.isArray((querySpec as any)?.series) && (querySpec as any).series.length > 0) ? (querySpec as any).series[0]?.agg : undefined
                const aggEff = seriesAgg || (querySpec as any).agg || (field ? 'sum' : 'count')
                const v = aggregate(subset, field, aggEff)
                obj[String(lk)] = v
              })
              outer.set(k, obj)
            })
            const categories = Array.from(catsSet.values())
            return { columns: ['x', ...categories], rows: Array.from(outer.values()), categories } as any
          }
          const legendVal = (() => { try { const lg: any = (querySpec as any)?.legend; return Array.isArray(lg) ? (lg[0] as any) : lg } catch { return (querySpec as any)?.legend } })()
          const promises = (series || []).map((s) => {
            // Important: use the per-series aggregator only; do not fall back to querySpec.agg here,
            // because querySpec.agg may reflect a single-agg spec compiled from the first value.
            const agg = (s.agg ?? 'count') as any
            // If 'none', fetch raw fields instead
            if (agg === 'none') {
              const sel: string[] = []
              const xf = (s.x || (querySpec as any).x) as any
              if (xf) sel.push(String(xf))
              if (s.y) sel.push(String(s.y))
              const raw: QuerySpec = { source: querySpec.source, where: mergedWhere, select: sel, limit: (querySpec.limit ?? 1000), offset: (querySpec.offset ?? 0) }
              if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
                try { console.debug('[ChartCard] branch=multi-series/raw', { sel, where: mergedWhere }) } catch {}
              }
              return QueryApi.querySpec({ spec: raw, datasourceId, limit: isPreview ? Math.min(100, raw.limit ?? 1000) : (raw.limit ?? 1000), offset: raw.offset ?? 0, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
            }
            const merged: QuerySpec = {
              source: querySpec.source,
              where: mergedWhere,
              x: ((s as any).x ?? (querySpec as any).x) as any,
              y: s.y as any,
              agg,
              legend: (legendVal as any),
              groupBy: (s.groupBy || (querySpec as any).groupBy) as any,
              weekStart: ((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') as any,
              measure: s.measure as any,
              select: undefined, // aggregated path returns x,value
              limit: (querySpec.limit ?? 1000),
              offset: (querySpec.offset ?? 0),
              // Respect Top-N ranking on the server
              orderBy: ((querySpec as any)?.orderBy as any),
              order: ((querySpec as any)?.order as any),
            }
            if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
              try { console.debug('[ChartCard] branch=multi-series/agg REQUEST', { spec: merged, where: mergedWhere, y: s.y, agg, legend: merged.legend, x: merged.x }) } catch {}
            }
            return QueryApi.querySpec({ spec: merged as any, datasourceId, limit: merged.limit ?? 1000, offset: merged.offset ?? 0, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
          })
          const results = await Promise.all(promises)
          
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try {
              console.debug('[ChartCard] multi-series RAW BACKEND RESPONSE', {
                results: results.map((r, idx) => ({
                  index: idx,
                  columns: r?.columns,
                  rowCount: r?.rows?.length,
                  firstRow: r?.rows?.[0],
                  lastRow: r?.rows?.[r.rows.length - 1]
                }))
              })
            } catch {}
          }
          const map = new Map<string | number, any>()
          const catsSet = new Set<string>()
          const labelFor = (s: any, idx: number) => s?.label || s?.y || s?.measure || `series_${idx + 1}`
          const legendPresent = results.some((res) => {
            const cols: string[] = ((res?.columns || []) as string[])
            return cols.indexOf('legend') >= 0
          })
          const hasXColumn = results.some((res) => {
            const cols: string[] = ((res?.columns || []) as string[])
            return cols.indexOf('x') >= 0
          })
          const virtualMeta: Record<string, { baseSeriesIndex: number; baseSeriesLabel: string; categoryLabel: string; agg?: string }> = {}
          
          // Check if any result has an X column
          
          // Check if all X values are the same (backend returns default 'x' when no X specified)
          const allXValuesSame = (() => {
            if (!hasXColumn || results.length === 0) return false
            const xValues = new Set<any>()
            results.forEach((res) => {
              const cols: string[] = ((res?.columns || []) as string[])
              const ix = cols.indexOf('x')
              if (ix >= 0) {
                res.rows.forEach((row: any[]) => {
                  xValues.add(row[ix])
                })
              }
            })
            return xValues.size === 1
          })()
          
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try {
              console.debug('[ChartCard] multi-series no-X check', {
                hasXColumn,
                allXValuesSame,
                legendPresent,
                seriesCount: (series || []).length,
                resultColumns: results.map(r => ((r?.columns || []) as string[])),
                resultRowCounts: results.map(r => r?.rows?.length || 0)
              })
            } catch {}
          }
          
          // If no X column OR all X values are the same (backend default), but has legend, create single x-category with multiple series
          if (((!hasXColumn || allXValuesSame) && legendPresent)) {
            const obj: any = { x: (querySpec as any)?.y || (querySpec as any)?.measure || 'Total' }
            const hasMultipleSeries = (series || []).length > 1
            results.forEach((res, idx) => {
              const baseLabel = labelFor((series || [])[idx], idx)
              const cols: string[] = ((res?.columns || []) as string[])
              // Default to column 0 if 'legend' column not found (backend returns actual field name like "Merchant")
              const il = cols.indexOf('legend') !== -1 ? cols.indexOf('legend') : 0
              const iv = cols.indexOf('value') >= 0 ? cols.indexOf('value') : 1
              res.rows.forEach((row: any[]) => {
                const v = Number(row[iv] ?? 0)
                const cat = String(row[il])
                // Format: "LegendKey - DataSeriesName" if multiple series, else just "LegendKey"
                const key = hasMultipleSeries ? `${cat} - ${baseLabel}` : cat
                catsSet.add(key)
                obj[key] = v
                virtualMeta[key] = { baseSeriesIndex: idx, baseSeriesLabel: baseLabel, categoryLabel: cat, agg: String(((series || [])[idx] as any)?.agg ?? ((querySpec as any)?.agg ?? 'count')) }
              })
            })
            const categories = Array.from(catsSet.values())
            if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
              try {
                console.debug('[ChartCard] multi-series no-X result', { obj, categories, virtualMeta })
              } catch {}
            }
            return { columns: ['x', ...categories], rows: [obj], categories, virtualMeta } as any
          }
          
          // Normal path with X column
          const hasMultipleSeries = (series || []).length > 1
          results.forEach((res, idx) => {
            const baseLabel = labelFor((series || [])[idx], idx)
            const cols: string[] = ((res?.columns || []) as string[])
            const ix = Math.max(0, cols.indexOf('x'))
            const il = cols.indexOf('legend')
            const iv = Math.max(1, cols.indexOf('value'))
            res.rows.forEach((row: any[]) => {
              const x = row[ix] as any
              const v = Number(row[iv] ?? 0)
              const cat = il >= 0 ? String(row[il]) : baseLabel
              // Format: "LegendKey - DataSeriesName" if multiple series, else just "LegendKey" or baseLabel
              const key = il >= 0 ? (hasMultipleSeries ? `${cat} - ${baseLabel}` : cat) : baseLabel
              catsSet.add(key)
              if (!map.has(x)) map.set(x, { x })
              if (map.get(x)![key] === undefined) map.get(x)![key] = v
              virtualMeta[key] = { baseSeriesIndex: idx, baseSeriesLabel: baseLabel, categoryLabel: (il >= 0 ? cat : baseLabel), agg: String(((series || [])[idx] as any)?.agg ?? ((querySpec as any)?.agg ?? (((series || [])[idx] as any)?.y || ((series || [])[idx] as any)?.measure ? 'sum' : 'count'))) }
            })
          })
          const categories = Array.from(catsSet.values())
          
          // For Sankey charts, return raw backend response without pivoting
          if ((type as string) === 'sankey' && results.length === 1) {
            const res = results[0]
            return { 
              columns: res.columns, 
              rows: res.rows, 
              categories: (res as any).categories 
            } as any
          }
          
          // Do not force 0s; ensure keys exist but keep nulls for missing points
          const rowsShaped = Array.from(map.values()).map((o) => { categories.forEach((c) => { if (o[c] === undefined) o[c] = null }) ; return o })
          return { columns: ['x', ...categories], rows: rowsShaped, categories, virtualMeta } as any
        }
        if (!hasLegend && Array.isArray(series) && series.length > 0) {
          const promises = (series || []).map((s) => {
            const agg = (s.agg ?? 'count') as any
            if (agg === 'none') {
              const sel: string[] = []
              const xf = (s.x || (querySpec as any).x) as any
              if (xf) sel.push(String(xf))
              if (s.y) sel.push(String(s.y))
              const raw: QuerySpec = { source: querySpec.source, where: mergedWhere, select: sel, limit: (querySpec.limit ?? 1000), offset: (querySpec.offset ?? 0) }
              return QueryApi.querySpec({ spec: raw, datasourceId, limit: isPreview ? Math.min(100, raw.limit ?? 1000) : (raw.limit ?? 1000), offset: raw.offset ?? 0, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
            }
            const merged: QuerySpec = {
              source: querySpec.source,
              where: mergedWhere,
              // Preserve full X spec (string | string[]) to allow multi-level X on backend
              x: ((s as any).x ?? (querySpec as any).x) as any,
              y: s.y as any,
              agg,
              groupBy: (s.groupBy || (querySpec as any).groupBy) as any,
              weekStart: ((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') as any,
              measure: s.measure as any,
              select: undefined,
              limit: (querySpec.limit ?? 1000),
              offset: (querySpec.offset ?? 0),
              orderBy: ((querySpec as any)?.orderBy as any),
              order: ((querySpec as any)?.order as any),
            }
            // Send merged spec as-is; QueryApi will preserve x as string | string[]
            return QueryApi.querySpec({ spec: merged as any, datasourceId, limit: merged.limit ?? 1000, offset: merged.offset ?? 0, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
          })
          const results = await Promise.all(promises)
          const map = new Map<string | number, any>()
          const labelFor = (s: any, idx: number) => s?.label || s?.y || s?.measure || `series_${idx + 1}`
          const categories = (series || []).map((s, i) => labelFor(s, i))
          results.forEach((res, idx) => {
            const label = labelFor((series || [])[idx], idx)
            const cols: string[] = ((res?.columns || []) as string[])
            const ix = Math.max(0, cols.indexOf('x'))
            const iv = cols.indexOf('value') !== -1 ? cols.indexOf('value') : ((series || [])[idx]?.y ? cols.indexOf(String((series || [])[idx]?.y)) : 1)
            res.rows.forEach((row: any[]) => {
              const x = row[ix] as any
              const v = Number(row[iv] ?? 0)
              if (!map.has(x)) map.set(x, { x })
              map.get(x)![label] = v
            })
          })
          return { columns: ['x', ...categories], rows: Array.from(map.values()).map((o) => categories.map((c) => (o[c] ?? 0)).reduce((acc, _, i) => o, o)), categories } as any
        }
        // Single-series or legend split
        // Make sure legend is forwarded (from pivot if missing on querySpec), and if user configured a groupBy
        // (or legend) but no agg, default to count so the server performs aggregation instead of returning raw rows.
        const effectiveLegendAny = (((querySpec as any)?.legend) ?? (pivot as any)?.legend) as any
        const effectiveLegend = Array.isArray(effectiveLegendAny) ? ((effectiveLegendAny as any[]).length > 0 ? effectiveLegendAny : undefined) : effectiveLegendAny
        // Check if X field is a derived date part BEFORE inferring agg
        const xFieldStr = String(xField || '')
        const isDerivedDatePart = /\s\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/.test(xFieldStr)
        
        const inferredAgg = (() => {
          // Check series[0].agg first (for single-series with legend)
          const seriesAgg = (Array.isArray((querySpec as any)?.series) && (querySpec as any).series.length > 0) ? (querySpec as any)?.series[0]?.agg : undefined
          
          // IMPORTANT: If X is a derived date part and series has agg='none', override to 'count'
          if (seriesAgg && String(seriesAgg).toLowerCase() === 'none' && isDerivedDatePart) {
            return 'count' as any
          }
          if (seriesAgg) {
            return String(seriesAgg).toLowerCase() as any
          }
          
          const cur = String(((querySpec as any)?.agg || 'none') as any).toLowerCase()
          if (cur && cur !== 'none') return cur as any
          const gb = String(((querySpec as any)?.groupBy || 'none') as any).toLowerCase()
          const legendBool = Array.isArray(effectiveLegend) ? (effectiveLegend.length > 0) : !!effectiveLegend
          
          if ((gb && gb !== 'none') || legendBool || isDerivedDatePart) {
            return 'count' as any
          }
          return 'none' as any
        })()
        const merged: QuerySpec = {
          ...querySpec,
          where: mergedWhere,
          weekStart: ((options as any)?.xWeekStart || (querySpec as any)?.weekStart || env.weekStart) as any,
          legend: effectiveLegend,
          agg: (inferredAgg as any),
          // CRITICAL FIX: Ensure groupBy is set for derived date parts so backend knows to GROUP BY
          groupBy: isDerivedDatePart && !((querySpec as any)?.groupBy) ? 'none' : ((querySpec as any)?.groupBy as any),
          // Respect Top-N ranking on the server when present
          orderBy: ((querySpec as any)?.orderBy as any),
          order: ((querySpec as any)?.order as any),
        }
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try { console.log('[ChartCard] merged query spec', { inferredAgg, mergedAgg: (merged as any).agg, merged, hasLegend }) } catch {}
        }
        // If agg === 'none' (and no series, no legend), fetch raw select [x,y]
        if (!hasLegend && (!Array.isArray(series) || (series || []).length === 0) && (merged as any).agg === 'none' && (merged as any).x && (merged as any).y) {
          const sel: string[] = []
          if ((merged as any).x) sel.push(String((merged as any).x))
          if ((merged as any).y) sel.push(String((merged as any).y))
          const raw: QuerySpec = { source: merged.source, where: mergedWhere, select: sel, limit: merged.limit ?? 1000, offset: merged.offset ?? 0 }
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] branch=single/raw', { sel, where: mergedWhere }) } catch {}
          }
          const r = await QueryApi.querySpec({ spec: raw, datasourceId, limit: isPreview ? Math.min(100, raw.limit ?? 1000) : (raw.limit ?? 1000), offset: raw.offset ?? 0, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
          const map = new Map<string | number, any>()
          r.rows.forEach((row: any[]) => {
            const x = row[0] as any
            const v = Number(row[1] ?? 0)
            if (!map.has(x)) map.set(x, [x, v])
          })
          return { columns: ['x', 'value'], rows: Array.from(map.values()) } as any
        }
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try {
            console.log('[ChartCard] ⚠️ BRANCH: single/agg', { where: mergedWhere, hasLegend })
            const specView = { x: (merged as any).x, y: (merged as any).y, agg: (merged as any).agg, legend: (merged as any).legend, measure: (merged as any).measure, groupBy: (merged as any).groupBy }
            // eslint-disable-next-line no-console
            console.log('[ChartCard] [FiltersDebug] request.single-agg', { where: mergedWhere, spec: specView })
          } catch {}
        }
        const mergedSafe = { ...merged, x: (Array.isArray((merged as any).x) ? (merged as any).x[0] : (merged as any).x) } as any
        const res = await QueryApi.querySpec({ spec: { ...mergedSafe, where: mergedWhere }, datasourceId, limit: isPreview ? Math.min(100, mergedSafe.limit ?? 1000) : (mergedSafe.limit ?? 1000), offset: mergedSafe.offset ?? 0, includeTotal: false, preferLocalDuck: (options as any)?.preferLocalDuck })
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try {
            // eslint-disable-next-line no-console
            console.log('[ChartCard] 📥 RESPONSE: single/agg', { columns: res?.columns, rows: Array.isArray(res?.rows) ? res.rows.length : 0, sample: Array.isArray(res?.rows) ? res.rows[0] : undefined })
          } catch {}
        }
        // Server single-agg path with legend - pivot when response has legend column
        const hasLegendCol = res?.columns?.includes('legend')
        console.log('[ChartCard] 🔍 Checking pivot condition:', { 
          hasLegend, 
          hasLegendCol,
          seriesIsArray: Array.isArray(series), 
          seriesLength: Array.isArray(series) ? series.length : 'N/A',
          hasRows: !!res?.rows?.length,
          willPivot: hasLegend && hasLegendCol && res?.rows?.length
        })
        if (hasLegend && hasLegendCol && res?.rows?.length) {
          // Robustly map by column names; when x is absent, backend returns [legend, value]
          const cols: string[] = ((res?.columns || []) as string[])
          const ix = cols.indexOf('x')
          const il = cols.indexOf('legend') !== -1 ? cols.indexOf('legend') : 0
          let iv = cols.indexOf('value')
          if (iv === -1) {
            // pick the first numeric-like column different from legend/x; else fallback to 1
            const candidates = cols.map((c, i) => i).filter((i) => i !== il && i !== ix)
            iv = candidates.length ? candidates[0] : 1
          }
          // If there is no X column, create multiple series with a single "Total" x-category
          if (ix === -1) {
            const categories: string[] = []
            const obj: any = { x: (querySpec as any)?.y || (querySpec as any)?.measure || 'Total' }
            res.rows.forEach((r: any[]) => {
              const legendKey = String(r[il] as any)
              categories.push(legendKey)
              const v = Number(r[iv] ?? 0)
              obj[legendKey] = v
            })
            return { columns: ['x', ...categories], rows: [obj], categories } as any
          }
          // Otherwise, pivot into categories per legend for each X bucket
          const map = new Map<string | number, any>()
          const catsSet = new Set<string>()
          // DEBUG: Log first 20 rows to see backend order
          console.log('[ChartCard] Backend response row order (first 20):', res.rows.slice(0, 20).map((r: any[]) => ({ x: r[ix], legend: r[il], value: r[iv] })))
          res.rows.forEach((r: any[]) => {
            const x = r[ix] as any
            const legendVal = String(r[il] as any)
            const v = Number(r[iv] ?? 0)
            catsSet.add(legendVal)
            if (!map.has(x)) map.set(x, { x })
            // If 'none' agg is used with legend, take the first value per (x,legend)
            if (map.get(x)![legendVal] === undefined) map.get(x)![legendVal] = v
          })
          // DEBUG: Log map key order
          console.log('[ChartCard] Map keys after insertion:', Array.from(map.keys()))
          let categories = Array.from(catsSet)
          // Safety net: enforce legend filter client-side if present
          const legendFieldRaw = ((querySpec as any)?.legend || (pivot as any)?.legend)
          const legendField = (Array.isArray(legendFieldRaw) ? legendFieldRaw[0] : legendFieldRaw) as string | undefined
          const legendFilterArr = legendField ? (mergedWhere as any)?.[legendField] as any[] | undefined : undefined
          if (legendField && Array.isArray(legendFilterArr) && legendFilterArr.length > 0) {
            const allowed = new Set<string>(legendFilterArr.map((v) => String(v)))
            // Handle composite legends like "2024 - OrderUID" when filter has just "2024"
            const extractBase = (cat: string): string => {
              const s = String(cat)
              if (s.includes(' - ')) return s.split(' - ')[0]
              if (s.includes(' • ')) return s.split(' • ')[0]
              return s
            }
            categories = categories.filter((c) => allowed.has(String(c)) || allowed.has(extractBase(String(c))))
            // Strip disallowed legend keys from each row
            Array.from(map.values()).forEach((obj) => {
              Object.keys(obj).forEach((k) => {
                if (k !== 'x' && !allowed.has(String(k)) && !allowed.has(extractBase(String(k)))) delete obj[k]
              })
            })
            if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
              try { console.debug('[ChartCard] [FiltersDebug] pivot.legend-safety-filter', { legendField, allowed: Array.from(allowed.values()), cats: categories }) } catch {}
            }
          }
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try {
              console.log('[ChartCard] 🔄 PIVOT: single/agg+legend', { categories })
              const sample = Array.from(map.values())[0]
              // eslint-disable-next-line no-console
              console.log('[ChartCard] [FiltersDebug] pivot.single-agg', { categories, sample })
            } catch {}
          }
          // For Sankey charts, return raw backend response without pivoting
          if (type === 'sankey') {
            return res
          }
          return { columns: ['x', ...categories], rows: Array.from(map.values()), categories } as any
        }
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try { console.debug('[ChartCard] [RAW BACKEND RESPONSE] server-agg-path', { columns: res?.columns, rowCount: res?.rows?.length, firstRow: res?.rows?.[0], secondRow: res?.rows?.[1], hasCategories: !!(res as any)?.categories }) } catch {}
        }
        return res
      }
      const wid = String(widgetId || title || 'chart')
      const { promise: __p } = Api.queryForWidget(
        wid,
        {
          sql,
          datasourceId,
          limit: 1000,
          params: ignoreGlobal ? {} : (filters as any),
          preferLocalDuck: (options as any)?.preferLocalDuck,
          preferLocalTable: ((querySpec as any)?.source as string | undefined),
        },
        user?.id,
      )
      return __p
    },
  })

  // Transform rows -> Tremor format: first column as index key 'x', second column as category 'value'
  type XType = string | number | Date
  const isMulti = (Array.isArray(series) && (series || []).length > 0 && !!(querySpec as any)?.source) || hasLegend
  // Infer categories robustly when missing - MUST use raw q.data before any transformation
  const categories: string[] = useMemo(() => {
    // CRITICAL: Extract from RAW backend response columns/rows, not transformed data
    const rawCols = ((q.data as any)?.columns as string[] | undefined) || []
    const rawRows = ((q.data as any)?.rows as any[] | undefined) || []
    
    if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
      try {
        console.debug('[ChartCard] [categories.useMemo] START - RAW DATA', {
          hasCategories: !!((q.data as any)?.categories),
          categoriesValue: (q.data as any)?.categories,
          isMulti,
          rawCols,
          rawRowsCount: rawRows.length,
          firstRawRow: rawRows[0]
        })
      } catch {}
    }
    const got = (q.data as any)?.categories as string[] | undefined
    if (Array.isArray(got) && got.length > 0) {
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [FiltersDebug] categories.source', { source: 'q.data.categories', cats: got }) } catch {}
      }
      // Apply Top N override on categories if configured
      const applyTopNToCats = (cats: string[]): string[] => {
        try {
          const dd: any = (dataDefaultsEff || (options as any)?.dataDefaults || {})
          const n = Number(dd?.topN?.n || 0)
          if (!Array.isArray(cats) || cats.length === 0 || n <= 0) return cats
          const ascTop = String(dd.topN.direction || 'desc') === 'asc'
          const cols: string[] = ((q.data as any)?.columns as string[]) || []
          const rows: any[] = ((q.data as any)?.rows as any[]) || []
          const totals: Record<string, number> = {}
          const legendIdx = cols.indexOf('legend')
          const valueIdx = cols.indexOf('value')
          if (legendIdx !== -1 && valueIdx !== -1 && rows.length) {
            rows.forEach((r: any[]) => { const lg = String(r?.[legendIdx]); const v = Number(r?.[valueIdx] ?? 0); if (Number.isFinite(v)) totals[lg] = (totals[lg] || 0) + v })
          } else if (rows.length && typeof rows[0] === 'object' && !Array.isArray(rows[0])) {
            rows.forEach((obj: any) => { cats.forEach((c) => { const v = Number(obj?.[c] ?? 0); if (Number.isFinite(v)) totals[c] = (totals[c] || 0) + v }) })
          } else {
            return cats
          }
          const uniqueCats = Array.from(new Set(cats))
          uniqueCats.sort((a, b) => {
            const av = totals[a] || 0
            const bv = totals[b] || 0
            return ascTop ? (av - bv) : (bv - av)
          })
          return uniqueCats.slice(0, Math.max(1, n))
        } catch { return cats }
      }
      return applyTopNToCats(got)
    }
    if (isMulti) {
      // Use RAW columns/rows from backend response, not transformed data
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [categories.useMemo] isMulti branch', { rawCols, rawRowsCount: rawRows.length }) } catch {}
      }
      const legendIdx0 = rawCols.indexOf('legend')
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [categories.useMemo] legendIdx0 check', { legendIdx0, rawCols, hasRows: rawRows.length > 0 }) } catch {}
      }
      if (legendIdx0 !== -1 && rawRows.length > 0) {
        const s = new Set<string>()
        rawRows.forEach((r: any) => { const v = Array.isArray(r) ? r[legendIdx0] : r?.legend; if (v != null) s.add(String(v)) })
        const arr0 = Array.from(s)
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try { console.debug('[ChartCard] [categories.useMemo] extracted from legendIdx0', { arr0, returning: arr0.length > 0 }) } catch {}
        }
        if (arr0.length > 0) return arr0
      }
      // Heuristic: when columns metadata is absent but rows look like [x, legend, value]
      if ((rawCols.length === 0) && rawRows.length > 0 && Array.isArray(rawRows[0]) && (rawRows[0] as any[]).length >= 3) {
        const setH = new Set<string>()
        rawRows.forEach((r: any) => { try { const v = Array.isArray(r) ? r[1] : r?.legend; if (v != null) setH.add(String(v)) } catch {} })
        const arrH = Array.from(setH)
        if (arrH.length > 0) return arrH
      }
      // If server sent [x,legend,value] arrays
      const legendIdx = rawCols.indexOf('legend')
      if (legendIdx !== -1) {
        const set = new Set<string>()
        rawRows.forEach((r) => { const v = r?.[legendIdx]; if (v != null) set.add(String(v)) })
        const arr = Array.from(set)
        if (set.size > 0) {
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] [FiltersDebug] categories.source', { source: 'array-legend', cats: arr }) } catch {}
          }
          // Apply Top N override here too
          const applyTopNToCats = (cats: string[]) => {
            try {
              const dd: any = (dataDefaultsEff || (options as any)?.dataDefaults || {})
              const n = Number(dd?.topN?.n || 0)
              if (!Array.isArray(cats) || cats.length === 0 || n <= 0) return cats
              const ascTop = String(dd.topN.direction || 'desc') === 'asc'
              const valueIdx = rawCols.indexOf('value')
              const totals: Record<string, number> = {}
              if (valueIdx !== -1) rawRows.forEach((r: any[]) => { const lg = String(r?.[legendIdx]); const v = Number(r?.[valueIdx] ?? 0); if (Number.isFinite(v)) totals[lg] = (totals[lg] || 0) + v })
              const uniqueCats = Array.from(new Set(cats))
              uniqueCats.sort((a, b) => { const av = totals[a] || 0; const bv = totals[b] || 0; return ascTop ? (av - bv) : (bv - av) })
              return uniqueCats.slice(0, Math.max(1, n))
            } catch { return cats }
          }
          return applyTopNToCats(arr)
        }
      }
      // If rows are objects { x, cat1, cat2, ... }
      if (rawRows.length > 0 && rawRows.every((r) => r && typeof r === 'object' && !Array.isArray(r))) {
        const keys = Object.keys(rawRows[0] as any).filter((k) => k !== 'x' && k !== 'value')
        if (keys.length > 0) {
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            try { console.debug('[ChartCard] [FiltersDebug] categories.source', { source: 'object-keys', cats: keys }) } catch {}
          }
          const applyTopNToCats = (cats: string[]) => {
            try {
              const dd: any = (options as any)?.dataDefaults || {}
              const n = Number(dd?.topN?.n || 0)
              if (!Array.isArray(cats) || cats.length === 0 || n <= 0) return cats
              const ascTop = String(dd.topN.direction || 'desc') === 'asc'
              const totals: Record<string, number> = {}
              rawRows.forEach((obj: any) => { cats.forEach((c) => { const v = Number(obj?.[c] ?? 0); if (Number.isFinite(v)) totals[c] = (totals[c] || 0) + v }) })
              const uniqueCats = Array.from(new Set(cats))
              uniqueCats.sort((a, b) => { const av = totals[a] || 0; const bv = totals[b] || 0; return ascTop ? (av - bv) : (bv - av) })
              return uniqueCats.slice(0, Math.max(1, n))
            } catch { return cats }
          }
          return applyTopNToCats(keys)
        }
      }
      // Prefer legend values from rows if present, otherwise fallback to declared series
      const fromRows = (() => {
        try {
          const li = rawCols.indexOf('legend')
          if (li !== -1 && rawRows.length > 0) {
            const s = new Set<string>()
            rawRows.forEach((r: any) => { const v = Array.isArray(r) ? r[li] : r?.legend; if (v != null) s.add(String(v)) })
            const arr2 = Array.from(s)
            if (arr2.length > 0) return arr2
          }
        } catch {}
        return [] as string[]
      })()
      const fallback = fromRows.length > 0
        ? fromRows
        : (series || []).map((s, i) => (s?.label || s?.y || s?.measure || `series_${i + 1}`) as string)
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [FiltersDebug] categories.source', { source: 'series-fallback', cats: fallback }) } catch {}
      }
      // Apply Top N override for series labels as categories by scanning rows
      const applyTopNToCats = (cats: string[]) => {
        try {
          const dd: any = (options as any)?.dataDefaults || {}
          const n = Number(dd?.topN?.n || 0)
          if (!Array.isArray(cats) || cats.length === 0 || n <= 0) return cats
          const ascTop = String(dd.topN.direction || 'desc') === 'asc'
          const rows: any[] = ((q.data as any)?.rows as any[]) || []
          const totals: Record<string, number> = {}
          if (rows.length && typeof rows[0] === 'object' && !Array.isArray(rows[0])) {
            rows.forEach((obj: any) => { cats.forEach((c) => { const v = Number(obj?.[c] ?? 0); if (Number.isFinite(v)) totals[c] = (totals[c] || 0) + v }) })
          } else {
            return cats
          }
          const uniqueCats = Array.from(new Set(cats))
          uniqueCats.sort((a, b) => { const av = totals[a] || 0; const bv = totals[b] || 0; return ascTop ? (av - bv) : (bv - av) })
          return uniqueCats.slice(0, Math.max(1, n))
        } catch { return cats }
      }
      return applyTopNToCats(fallback)
    }
    if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
      try { console.debug('[ChartCard] [FiltersDebug] categories.source', { source: 'single-series-default', cats: ['value'] }) } catch {}
    }
    return ['value']
  }, [q.data, isMulti, series])
  // Shape rows into the consistent object form the renderers expect
  const data = useMemo(() => {
    const rows: any[] = (q.data?.rows as any[]) || []
    if (!rows.length) return [] as any[]
    if (isMulti) {
      const first = rows[0]
      if (Array.isArray(first)) {
        const cols = ((q.data as any)?.columns as string[] | undefined) || []
        const xIdx = Math.max(cols.indexOf('x'), 0)
        const legendIdx = cols.indexOf('legend')
        const valueIdx = cols.indexOf('value')
        // For Sankey charts, keep long format [x, legend, value] - don't pivot
        if (type === 'sankey' && legendIdx !== -1 && valueIdx !== -1) {
          // Return rows as-is for Sankey (it needs long format)
          return rows
        }
        // If shape is [x, legend, value], pivot into { x, [legend]: value }
        if (legendIdx !== -1 && valueIdx !== -1) {
          const byX = new Map<any, any>()
          rows.forEach((r) => {
            const x = r?.[xIdx]
            const lg = String(r?.[legendIdx])
            const val = toNum(r?.[valueIdx])
            const obj = byX.get(x) || { x }
            const prev = toNum((obj as any)[lg], 0)
            ;(obj as any)[lg] = prev + val
            byX.set(x, obj)
          })
          const out = Array.from(byX.values())
          // Preserve gaps: keep missing (x, legend) pairs as null so lines don't drop to 0
          out.forEach((o) => { (categories || []).forEach((c) => { if (o[c] == null) o[c] = null }) })
          return out
        }
        // Otherwise, map arrays by column names if present
        if (cols.length >= first.length) {
          return rows.map((r) => {
            const o: any = {}
            cols.forEach((c, i) => { o[c] = r?.[i] })
            return o
          })
        }
      }
      // Ensure object rows have all categories present but preserve nulls for missing values
      const out = rows as any[]
      out.forEach((o) => {
        (categories || []).forEach((c) => {
          const v = (o as any)[c]
          if (v === undefined || v === null || v === '') {
            (o as any)[c] = null
          } else {
            const num = Number(v)
            ;(o as any)[c] = Number.isFinite(num) ? num : null
          }
        })
        if ((o as any).value !== undefined) {
          const vv = (o as any).value
          if (vv === null || vv === '') {
            (o as any).value = null
          } else {
            const num = Number(vv)
            ;(o as any).value = Number.isFinite(num) ? num : null
          }
        }
      })
      return out
    }
    // Single-series: expect [x, value]
    const base = rows.map((row: any) => ({ x: row[0] as XType, value: toNum(row[1]) }))
    return base
  }, [q.data, isMulti, categories, type])

  


  // X label casing (row) and mapped data for Tremor charts
  const applyXCase = useMemo(() => {
    const mode = (options as any)?.xLabelCase as ('lowercase'|'capitalize'|'uppercase'|'capitalcase'|'proper') | undefined
    if (!mode) return (s: any) => s
    return (s: any) => {
      const str = String(s ?? '')
      switch (mode) {
        case 'lowercase': return str.toLowerCase()
        case 'uppercase':
        case 'capitalcase':
          return str.toUpperCase()
        case 'proper': default: return str.replace(/[_-]+/g, ' ').split(/\s+/).map(w => w ? (w[0].toUpperCase() + w.slice(1).toLowerCase()) : w).join(' ')
      }
    }
  }, [options?.xLabelCase])

  const displayData = useMemo(() => {
    try {
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] displayData.useMemo', { excludeZeroValues: (options as any)?.excludeZeroValues, dataLen: (data as any[])?.length }) } catch {}
      }
      // Step 1: apply optional X label casing
      const mode = (options as any)?.xLabelCase as ('lowercase'|'capitalize'|'proper') | undefined
      let arr: any[] = Array.isArray(data) ? (data as any[]) : []
      if (mode) {
        arr = arr.map((row) => (row && typeof row.x === 'string') ? { ...row, x: applyXCase(row.x) } : row)
      }
      // Step 2: apply per-widget overrides (Sort only; Top N handled in categories useMemo)
      const dd = (dataDefaultsEff || (options as any)?.dataDefaults)
      const hasOverrides = !!(dd?.sort?.by || (dd?.topN?.n && Number(dd.topN.n) > 0))
      const useDs = (dd?.useDatasourceDefaults !== false) && !hasOverrides // default true, unless overrides provided
      const cats = (categories || []) as string[]
      const rowVal = (r: any) => (isMulti ? cats.reduce((s, c) => s + (Number(r?.[c]) || 0), 0) : Number(r?.value || 0))
      const cmpAsc = (a: any, b: any) => (a < b ? -1 : a > b ? 1 : 0)

      // Detect 12-month seasonality pattern (x=date, legend=Year(x), groupBy=month)
      const gbDisp = String(((querySpec as any)?.groupBy || 'none') as any).toLowerCase()
      const xFieldDisp = String(((querySpec as any)?.x || '') as any)
      const legendDispRaw = (() => {
        const lg: any = (querySpec as any)?.legend
        if (Array.isArray(lg) && lg.length > 0) return String(lg[0] ?? '')
        return typeof lg === 'string' ? lg : ''
      })()
      let seasonalityModeDisp = false
      try {
        const m = legendDispRaw.match(/^(.*)\s*\(Year\)$/i)
        if (m) {
          const base = m[1].trim()
          if (gbDisp === 'month' && base === xFieldDisp) {
            seasonalityModeDisp = true
            console.log('[ChartCard] ✅ 12-MONTH SEASONALITY ENABLED (displayData)', { gbDisp, xFieldDisp, legendDispRaw, base })
          }
        }
        if (!seasonalityModeDisp) {
          console.log('[ChartCard] ❌ Seasonality NOT detected (displayData)', { gbDisp, xFieldDisp, legendDispRaw, legendMatch: !!m })
        }
      } catch {}

      const doSort = (by?: 'x'|'value', dir?: 'asc'|'desc') => {
        const asc = (dir || 'desc') === 'asc'
        if (by === 'x') {
          // In 12-month seasonality mode, preserve backend month order (Jan..Dec)
          if (seasonalityModeDisp) {
            console.log('[ChartCard] ✅ Skipping client-side sort by x (seasonality mode - preserving backend order)')
            return
          }
          arr = [...arr].sort((a, b) => {
            const ax = a?.x; const bx = b?.x
            const ca = (typeof ax === 'number' && typeof bx === 'number') ? cmpAsc(ax, bx) : cmpAsc(String(ax ?? ''), String(bx ?? ''))
            return asc ? ca : -ca
          })
        } else if (by === 'value') {
          arr = [...arr].sort((a, b) => {
            const ca = cmpAsc(rowVal(a), rowVal(b))
            return asc ? ca : -ca
          })
        }
      }
      if (!useDs) {
        // Apply widget overrides last (so Top N selection doesn't force its own order)
        if (dd?.sort?.by) doSort(dd.sort.by as any, (dd.sort.direction as any) || 'desc')
      }
      // Final sanitize: coerce numeric fields and drop rows with invalid x
      const catsList = (categories || []) as string[]
      arr = arr.map((row) => {
        const r: any = { ...row }
        catsList.forEach((c) => {
          const v = r[c]
          if (v === undefined || v === null || v === '') {
            r[c] = null
          } else {
            const num = Number(v)
            r[c] = Number.isFinite(num) ? num : null
          }
        })
        if (r.value !== undefined) {
          const vv = r.value
          if (vv === null || vv === '') r.value = null
          else { const num = Number(vv); r.value = Number.isFinite(num) ? num : null }
        }
        return r
      }).filter((r) => {
        const x = (r as any).x
        if (x === null || x === undefined) return false
        if (typeof x === 'number' && !Number.isFinite(x)) return false
        return true
      })
      // Filter out rows where all values are zero if option is enabled
      if ((options as any)?.excludeZeroValues) {
        const beforeLen = arr.length
        // Check if categories exist as actual columns in the data (pivot format)
        const firstRow = arr[0] || {}
        const hasCategoryColumns = catsList.length > 0 && catsList.every((c) => c in firstRow)
        
        arr = arr.filter((r) => {
          // For multi-series with category columns (e.g., pivot table format)
          if (isMulti && hasCategoryColumns) {
            const categoryValues = catsList.map((c) => {
              const raw = r[c]
              // Treat null, undefined, and 0 as "no value"
              const isEmpty = raw === null || raw === undefined || Number(raw) === 0
              return { cat: c, raw, isEmpty }
            })
            const hasNonZero = categoryValues.some((cv) => !cv.isEmpty)
            const allEmpty = categoryValues.every((cv) => cv.isEmpty)
            
            // Debug: log rows with all empty values
            if (allEmpty && typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
              try { console.debug('[ChartCard] Row with all null/zero:', { x: r.x, values: categoryValues }) } catch {}
            }
            
            return hasNonZero
          }
          // For single series or legend-based charts (row format), check the value field
          if (r.value !== null && r.value !== undefined) {
            return Number(r.value) !== 0
          }
          // Fallback: keep the row if we can't determine
          return true
        })
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try { 
            const allXValues = arr.map((r) => r.x)
            const firstFive = allXValues.slice(0, 5)
            const lastFive = allXValues.slice(-5)
            console.debug('[ChartCard] excludeZeroValues:', { 
              before: beforeLen, 
              after: arr.length, 
              filtered: beforeLen - arr.length, 
              isMulti, 
              hasCategoryColumns, 
              catsList: catsList.length,
              firstRow: arr[0],
              lastRow: arr[arr.length - 1],
              xRange: { first: firstFive, last: lastFive, total: allXValues.length }
            }) 
          } catch {}
        }
      }
      return arr
    } catch { return data }
  }, [data, options?.xLabelCase, applyXCase, options?.dataDefaults, isMulti, JSON.stringify(categories), (options as any)?.excludeZeroValues])

  const chartInstanceKey = useMemo(() => {
    try {
      const catsSig = (categories || []).join('|')
      const len = Array.isArray(displayData) ? (displayData as any[]).length : 0
      return `${type}|${catsSig}|${len}`
    } catch {
      return `${type}|${(categories || []).length}`
    }
  }, [type, categories, displayData])

  // Final verification log: categories + first data row as rendered input
  if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
    try {
      // eslint-disable-next-line no-console
      console.debug('[ChartCard] [FiltersDebug] data.memo', { title, categories, len: (data as any[])?.length, first: (data as any[])?.[0] })
    } catch {}
  }

  // Dev-only debug to validate data shape for Tremor charts
  if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
    try {
      // eslint-disable-next-line no-console
      console.debug('[ChartCard] debug', { title, type, isMulti, categories, rows: q.data?.rows?.length, sample: data?.[0] })
    } catch {}
  }

  const yLooksNumeric = useMemo(() => {
    if (isMulti) return true
    if (!q.data?.rows) return true
    return q.data.rows.some((r: any) => typeof r[1] === 'number' || !Number.isNaN(Number(r[1])))
  }, [q.data, isMulti])

  // (moved below after chartBoxH computation)

  // Spark layout helpers (must be defined before rendering content)
  const sparkAreaRef = useRef<HTMLDivElement | null>(null)
  const [sparkRowH, setSparkRowH] = useState<number>(60)
  const [sparkSizeKey, setSparkSizeKey] = useState<number>(0)
  useEffect(() => {
    const measure = () => {
      try {
        const el = sparkAreaRef.current
        if (!el) return
        const count = Math.max(1, (categories || []).length)
        const gap = 8 // px between rows
        const total = el.clientHeight
        const h = Math.max(60, Math.floor((total - gap * (count - 1)) / count))
        setSparkRowH(h)
        setSparkSizeKey((k) => k + 1)
      } catch {}
    }
    const ro = new ResizeObserver(measure)
    if (sparkAreaRef.current) ro.observe(sparkAreaRef.current)
    if (typeof window !== 'undefined') window.addEventListener('resize', measure)
    // Post-mount double RAF to ensure layout is stable before measuring
    if (typeof window !== 'undefined') requestAnimationFrame(() => requestAnimationFrame(measure))
    return () => { try { ro.disconnect() } catch {}; if (typeof window !== 'undefined') window.removeEventListener('resize', measure) }
  }, [JSON.stringify(categories)])

  // Delta helper (server-side): compute period deltas when configured (component scope)
  const deltaUI = options?.deltaUI || 'none'
  const preconfiguredMode = options?.deltaMode && options.deltaMode !== 'off' ? options.deltaMode : undefined
  const [filterbarMode, setFilterbarMode] = useState<'TD_YSTD'|'TW_LW'|'MONTH_LMONTH'|'MTD_LMTD'|'TY_LY'|'YTD_LYTD'|'TQ_LQ'|'Q_TY_VS_Q_LY'|'QTD_TY_VS_QTD_LY'|'M_TY_VS_M_LY'|'MTD_TY_VS_MTD_LY'|undefined>(preconfiguredMode as any)
  const activeDeltaMode = (deltaUI === 'filterbar' ? filterbarMode : preconfiguredMode)
  const deltaDateField = useMemo(() => {
    try {
      if ((options as any)?.deltaDateField) return String((options as any).deltaDateField)
      // Derive from x when grouped/time-labeled
      const seriesArr = Array.isArray(series) ? series : []
      const rawXField = (querySpec as any)?.x || seriesArr.find((s) => !!s?.x)?.x
      const xField = (Array.isArray(rawXField) ? (rawXField[0] as any) : rawXField) as string | undefined
      const gb = String(((querySpec as any)?.groupBy || 'none')).toLowerCase()
      const xFmtDatetime = ((options as any)?.xLabelFormat || 'none') === 'datetime'
      if (!xField) return undefined
      const m = String(xField).match(/^(.*)\s\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/)
      const base = m ? m[1] : xField
      if (gb !== 'none' || xFmtDatetime) return String(base)
      return undefined
    } catch { return undefined }
  }, [options?.deltaDateField, JSON.stringify((querySpec as any)?.x || ''), (querySpec as any)?.groupBy, (options as any)?.xLabelFormat, JSON.stringify(series || [])])
  const deltaWeekStart = (options?.deltaWeekStart || env.weekStart) as 'sat'|'sun'|'mon'
  const effectiveDeltaMode = useMemo(() => {
    if (activeDeltaMode) return activeDeltaMode
    try {
      const s = (filters as any)?.startDate as string | undefined
      const e = (filters as any)?.endDate as string | undefined
      if (!s) return undefined
      const now = new Date()
      const y = now.getFullYear()
      const sYear = Number(String(s).slice(0, 4))
      if (sYear === y) {
        const fullYear = !e || /^\d{4}-12-31$/.test(String(e))
        return (fullYear ? 'TY_LY' : 'YTD_LYTD') as any
      }
      return undefined
    } catch { return undefined }
  }, [activeDeltaMode, (filters as any)?.startDate, (filters as any)?.endDate])
  const prevTotalsCacheRef = useRef<Record<string, Record<string, number>>>({})
  const inflightPrevFetchesRef = useRef<Record<string, boolean>>({})
  const tzOffsetMinutes = (typeof window !== 'undefined') ? new Date().getTimezoneOffset() : 0

  // Shift by active/effective delta mode
  function shiftByMode(date: Date): Date | null {
    switch (effectiveDeltaMode as any) {
      case 'TD_YSTD': { const x = new Date(date); x.setDate(x.getDate()-1); return x }
      case 'TW_LW': { const x = new Date(date); x.setDate(x.getDate()-7); return x }
      case 'MONTH_LMONTH': { const x = new Date(date); x.setMonth(x.getMonth()-1); return x }
      case 'MTD_LMTD': { const x = new Date(date); x.setMonth(x.getMonth()-1); return x }
      case 'TQ_LQ': { const x = new Date(date); x.setMonth(x.getMonth()-3); return x }
      case 'TY_LY':
      case 'YTD_LYTD':
      case 'Q_TY_VS_Q_LY':
      case 'QTD_TY_VS_QTD_LY':
      case 'M_TY_VS_M_LY':
      case 'MTD_TY_VS_MTD_LY': { const x = new Date(date); x.setFullYear(x.getFullYear()-1); return x }
      default: return null
    }
  }

  // Compute bucket [start,end) for a given date and groupBy
  function bucketRangeForDate(d: Date, gb: string, weekStart: 'sat'|'sun'|'mon') {
    // Use local date parts from the parsed date (so month aligns with label),
    // but build boundaries in UTC midnight to avoid TZ-drift in .toISOString().
    const y = d.getFullYear(); const m = d.getMonth(); const day = d.getDate()
    switch ((gb || 'day')) {
      case 'year': {
        const start = new Date(Date.UTC(y, 0, 1))
        const end = new Date(Date.UTC(y + 1, 0, 1))
        return { start: start.toISOString(), end: end.toISOString() }
      }
      case 'quarter': {
        const qm = Math.floor(m / 3) * 3
        const start = new Date(Date.UTC(y, qm, 1))
        const end = new Date(Date.UTC(y, qm + 3, 1))
        return { start: start.toISOString(), end: end.toISOString() }
      }
      case 'month': {
        const start = new Date(Date.UTC(y, m, 1))
        const end = new Date(Date.UTC(y, m + 1, 1))
        return { start: start.toISOString(), end: end.toISOString() }
      }
      case 'week': {
        // Compute local start-of-week, then normalize to 00:00Z for both bounds
        const dt = new Date(d); dt.setHours(0,0,0,0)
        const dow = dt.getDay()
        let shift = 0
        if (weekStart === 'sun') shift = dow
        else if (weekStart === 'mon') shift = (dow + 6) % 7
        else if (weekStart === 'sat') shift = (dow + 1) % 7
        const startLocal = new Date(dt); startLocal.setDate(dt.getDate() - shift)
        const endLocal = new Date(startLocal); endLocal.setDate(startLocal.getDate() + 7)
        const start = new Date(Date.UTC(startLocal.getFullYear(), startLocal.getMonth(), startLocal.getDate()))
        const end = new Date(Date.UTC(endLocal.getFullYear(), endLocal.getMonth(), endLocal.getDate()))
        return { start: start.toISOString(), end: end.toISOString() }
      }
      case 'day':
      default: {
        const start = new Date(Date.UTC(y, m, day))
        const end = new Date(Date.UTC(y, m, day + 1))
        return { start: start.toISOString(), end: end.toISOString() }
      }
    }
  }
  function prevRangeForRawX(rawX: any): { start: string; end: string } | null {
    try {
      const s = String(rawX ?? '')
      let d = parseDateLoose(s)
      // If parsing fails and we're in month groupBy, try to reconstruct from formatted month name
      if (!d) {
        const gb = String((querySpec as any)?.groupBy || 'day').toLowerCase()
        if (gb === 'month' && typeof rawX === 'string') {
          const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
          const monthIdx = months.indexOf(rawX)
          if (monthIdx >= 0) {
            const year = filters.startDate ? new Date(filters.startDate).getFullYear() : new Date().getFullYear()
            const reconstructed = `${year}-${String(monthIdx + 1).padStart(2, '0')}-01`
            d = parseDateLoose(reconstructed)
          }
        }
      }
      if (!d) return null
      const prev = shiftByMode(d)
      if (!prev) return null
      const gb = String((querySpec as any)?.groupBy || 'day')
      return bucketRangeForDate(prev, gb, deltaWeekStart)
    } catch { return null }
  }

  const prevRangeKeyForRawX = (rawX: any) => { const r = prevRangeForRawX(rawX); return r ? `${r.start}..${r.end}` : null }

  // Prefetch prev totals for any x bucket whose prev lies outside history (per-series)
  useEffect(() => {
    try {
      if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevPrefetch] effect start') } catch {} }
      if (typeof window !== 'undefined') {
        try { console.log('[ChartCard] [PrevPrefetch] ctx', { effectiveDeltaMode, deltaDateField, haveDisplayData: Array.isArray(displayData), displayCount: Array.isArray(displayData) ? (displayData as any[]).length : 0, filters: { startDate: (filters as any)?.startDate, endDate: (filters as any)?.endDate } }) } catch {}
      }
      if (!effectiveDeltaMode) {
        if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevPrefetch] gated: no effectiveDeltaMode') } catch {} }
        return
      }
      if (!deltaDateField) {
        if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevPrefetch] gated: no deltaDateField') } catch {} }
        return
      }
      // Reconstruct actual date values from formatted x values using groupBy and filter range
      const rawData = Array.isArray(data) ? (data as any[]) : []
      const gb = String((querySpec as any)?.groupBy || 'day').toLowerCase()
      const xSeq = rawData.map((d:any)=> {
        const x = d?.x
        // Try to parse as date first
        const parsed = parseDateLoose(String(x ?? ''))
        if (parsed) return x
        // For formatted values like "Jan", "Feb", reconstruct ISO date from filter range
        if (gb === 'month' && typeof x === 'string') {
          const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
          const monthIdx = months.indexOf(x)
          if (monthIdx >= 0) {
            const year = filters.startDate ? new Date(filters.startDate).getFullYear() : new Date().getFullYear()
            return `${year}-${String(monthIdx + 1).padStart(2, '0')}-01`
          }
        }
        return x
      })
      if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevPrefetch] xSeq', { count: xSeq.length, sample: xSeq.slice(0, 3), rawDataCount: rawData.length, groupBy: gb }) } catch {} }
      if (xSeq.length === 0) {
        if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevPrefetch] gated: empty xSeq') } catch {} }
        return
      }
      // Build per-series contexts for agg/y/measure
      const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
      const seriesCtxs = (seriesDefs.length > 0)
        ? seriesDefs.map((s: any, i: number) => ({
            label: (s?.label || s?.y || s?.measure || `series_${i+1}`),
            agg: (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count')) as any,
            y: s?.y as any,
            measure: s?.measure as any,
          }))
        : [{ label: (querySpec as any)?.y || (querySpec as any)?.measure || 'value', agg: (((querySpec as any)?.agg ?? (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')) as any), y: (querySpec as any)?.y as any, measure: (querySpec as any)?.measure as any }]
      // Collect missing composite keys
      const missingKeys: Array<{ rangeKey: string; start: string; end: string; agg: any; y?: any; measure?: any }> = []
      xSeq.forEach((x:any, i:number) => {
        const prevIdx = resolvePrevIndex(i, xSeq)
        if (typeof window !== 'undefined' && i < 3) { try { console.log('[ChartCard] [PrevPrefetch] checking x', { x, i, prevIdx, hasPrev: prevIdx >= 0 }) } catch {} }
        if (prevIdx >= 0) return
        const r = prevRangeForRawX(x)
        if (!r) {
          if (typeof window !== 'undefined' && i < 3) { try { console.log('[ChartCard] [PrevPrefetch] no range for x', { x, i }) } catch {} }
          return
        }
        const baseRangeKey = `${r.start}..${r.end}`
        if (typeof window !== 'undefined' && i < 3) { try { console.log('[ChartCard] [PrevPrefetch] adding missing key', { x, i, rangeKey: baseRangeKey, seriesCount: seriesCtxs.length }) } catch {} }
        for (const ctx of seriesCtxs) {
          const cacheKey = `${baseRangeKey}::${ctx.agg}|${ctx.y || ''}|${ctx.measure || ''}`
          if (!prevTotalsCacheRef.current[cacheKey]) missingKeys.push({ rangeKey: baseRangeKey, start: r.start, end: r.end, agg: ctx.agg, y: ctx.y, measure: ctx.measure })
        }
      })
      if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevPrefetch] after loop', { missingKeysCount: missingKeys.length, xSeqSample: xSeq.slice(0, 3) }) } catch {} }
      if (missingKeys.length === 0) return
      if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevPrefetch] missingKeys', { count: missingKeys.length, sample: missingKeys.slice(0, 5) }) } catch {} }
      const fetchAll = async () => {
        const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
        const xField = (querySpec as any)?.x as string | undefined
        const legendAny = (querySpec as any)?.legend as any
        const partTitle = gb === 'year' ? 'Year' : gb === 'quarter' ? 'Quarter' : gb === 'month' ? 'Month' : gb === 'week' ? 'Week' : gb === 'day' ? 'Day' : undefined
        const X_DER = (() => { try { const m = String(xField || '').match(/^(.*)\s\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/); return m ? { base: m[1], part: m[2] } : null } catch { return null } })()
        const xPartExpr = X_DER ? String(xField) : ((xField && partTitle) ? `${xField} (${partTitle})` : undefined)
        const hasLegend = (Array.isArray(legendAny) ? legendAny.length > 0 : (typeof legendAny === 'string' && legendAny.trim() !== ''))
        const whereEffBase = (() => {
          try {
            const w: any = { ...(uiTruthWhere || {}) }
            const df = deltaDateField as string | undefined
            // Strip ALL filters referencing deltaDateField or its derived parts (e.g., "OrderDate (Year)")
            if (df) {
              const keysToDelete = Object.keys(w).filter((k) => {
                const key = String(k)
                // Match exact field or derived expressions like "OrderDate (Year)"
                return key === df || key.startsWith(`${df} (`) || key.startsWith(`${df}__`)
              })
              keysToDelete.forEach((k) => delete w[k])
            }
            // Strip filters for x-axis field (base and derived)
            const xBaseField = (X_DER ? X_DER.base : xField) as string | undefined
            if (xBaseField) {
              const keysToDelete = Object.keys(w).filter((k) => {
                const key = String(k)
                return key === xBaseField || key.startsWith(`${xBaseField} (`) || key.startsWith(`${xBaseField}__`)
              })
              keysToDelete.forEach((k) => delete w[k])
            }
            // Strip xPartExpr if different from xBaseField
            const xPart = xPartExpr as string | undefined
            if (xPart && xPart !== xBaseField) {
              const keysToDelete = Object.keys(w).filter((k) => String(k) === xPart || String(k).startsWith(`${xPart}__`))
              keysToDelete.forEach((k) => delete w[k])
            }
            return Object.keys(w).length ? w : undefined
          } catch { return undefined }
        })()
        const requests = missingKeys.map((r) => {
          // Pass legend to get per-legend prev totals (e.g., per customer type)
          // Only strip legend items that are derived from x-axis field
          const legendAny = (querySpec as any)?.legend
          let legendArg: any = undefined
          const xf = (X_DER ? X_DER.base : xField) as string | undefined
          if (Array.isArray(legendAny) && legendAny.length > 0) {
            const filtered = legendAny.filter((item: any) => {
              const s = String(item || '')
              // Keep legend unless it's the x-axis field or derived from it
              if (xf && (s === xf || s.startsWith(`${xf} (`))) return false
              return true
            })
            legendArg = filtered.length > 0 ? filtered : undefined
          } else if (typeof legendAny === 'string' && legendAny.trim()) {
            const s = String(legendAny)
            // Keep unless it's x-axis derived
            if (!(xf && (s === xf || s.startsWith(`${xf} (`)))) {
              legendArg = legendAny
            }
          }
          const key = `${r.rangeKey}::${r.agg}|${r.y || ''}|${r.measure || ''}`
          return {
            key,
            source: (querySpec as any)?.source,
            datasourceId,
            dateField: deltaDateField!,
            start: r.start,
            end: r.end,
            where: whereEffBase as any,
            legend: legendArg,
            agg: r.agg,
            y: r.y,
            measure: r.measure,
            weekStart: (deltaWeekStart as any) || undefined,
          }
        })
        try {
          const resp = await Api.periodTotalsBatch({ requests })
          const results = (resp?.results || {}) as Record<string, { total?: number; totals?: Record<string, number> }>
          Object.entries(results).forEach(([key, val]) => {
            const map: Record<string, number> = { ...((val?.totals as any) || {}) }
            const t = Number(val?.total)
            if (Number.isFinite(t)) {
              map['__total__'] = t
            } else {
              const sum = Object.values(map || {}).reduce((a: number, b: any) => a + (Number(b) || 0), 0)
              map['__total__'] = sum
            }
            prevTotalsCacheRef.current[key] = map
          })
          if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevPrefetch] batch response', { keys: Object.keys(results || {}) }) } catch {} }
        } catch {}
      }
      void fetchAll()
    } catch {}
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify((data || []).map((d:any)=>d?.x)), effectiveDeltaMode, deltaWeekStart, deltaDateField, datasourceId, JSON.stringify(uiTruthWhere || {}), JSON.stringify((querySpec || {})), JSON.stringify(((querySpec as any)?.series || []))])
  // Auto-select a sensible delta mode from global date filters when using filterbar
  useEffect(() => {
    if (deltaUI !== 'filterbar') return
    if (filterbarMode) return
    try {
      const s = filters.startDate
      const e = filters.endDate
      if (!s) return
      const now = new Date()
      const y = now.getFullYear()
      const sYear = Number(String(s).slice(0, 4))
      if (sYear === y) {
        const fullYear = !e || /^\d{4}-12-31$/.test(String(e))
        setFilterbarMode((fullYear ? 'TY_LY' : 'YTD_LYTD') as any)
      }
    } catch {}
  }, [deltaUI, filterbarMode, filters.startDate, filters.endDate])
  
  // Helper hook for distinct values for string filters
  const { cache: distinctCache, load: loadDistinct, loading: distinctLoading } = useDistinctStrings((querySpec as any)?.source, datasourceId, uiTruthWhere, (customColumns || []) as any)

  // Shared Filterbar labels/modes for delta UI
  const deltaLabels: Record<string, string> = {
    TD_YSTD: 'Today vs Yesterday',
    TW_LW: 'This Week vs Last Week',
    MONTH_LMONTH: 'This Month vs Last Month',
    MTD_LMTD: 'MTD vs LMTD',
    TY_LY: 'This Year vs Last Year',
    YTD_LYTD: 'YTD vs LYTD',
    TQ_LQ: 'This Quarter vs Last Quarter',
    Q_TY_VS_Q_LY: 'Q-TY vs Q-LY',
    QTD_TY_VS_QTD_LY: 'QTD-TY vs QTD-LY',
    M_TY_VS_M_LY: 'M-TY vs M-LY',
    MTD_TY_VS_MTD_LY: 'MTD-TY vs MTD-LY',
  }
  const deltaModes = ['TD_YSTD','TW_LW','MONTH_LMONTH','MTD_LMTD','TY_LY','YTD_LYTD','TQ_LQ','Q_TY_VS_Q_LY','QTD_TY_VS_QTD_LY','M_TY_VS_M_LY','MTD_TY_VS_MTD_LY'] as const

  const deltaKey = useMemo(() => JSON.stringify(uiTruthWhere), [uiTruthWhere])
  const deltaQ = useQuery({
    queryKey: ['delta', title, datasourceId, queryMode, querySpec, activeDeltaMode, deltaDateField, deltaWeekStart, deltaKey],
    enabled: (() => {
      const isEnabled = !!activeDeltaMode && !!deltaDateField && queryMode === 'spec' && !!(querySpec as any)?.source
      if (typeof window !== 'undefined') {
        try { console.log('[ChartCard] [DeltaQ] enabled check', { isEnabled, activeDeltaMode, deltaDateField, queryMode, hasSource: !!(querySpec as any)?.source, effectiveDeltaMode }) } catch {}
      }
      return isEnabled
    })(),
    queryFn: async () => {
      if (typeof window !== 'undefined') {
        try { console.log('[ChartCard] [DeltaQ] queryFn running', { activeDeltaMode, effectiveDeltaMode, deltaDateField }) } catch {}
      }
      const mode = effectiveDeltaMode as any
      const source = (querySpec as any).source as string
      // Build whereEff: strip dateField and x/xPart filters to avoid conflicts in prev compare
      const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
      const xField = (querySpec as any)?.x as string | undefined
      const partTitle = gb === 'year' ? 'Year' : gb === 'quarter' ? 'Quarter' : gb === 'month' ? 'Month' : gb === 'week' ? 'Week' : gb === 'day' ? 'Day' : undefined
      const X_DER = (() => { try { const m = String(xField || '').match(/^(.*)\s\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/); return m ? { base: m[1], part: m[2] } : null } catch { return null } })()
      const xPartExpr = X_DER ? String(xField) : ((xField && partTitle) ? `${xField} (${partTitle})` : undefined)
      const whereEff = (() => {
        try {
          const w: any = { ...(uiTruthWhere || {}) }
          const df = deltaDateField as string | undefined
          if (df) {
            const keysToDelete = Object.keys(w).filter((k) => {
              const key = String(k)
              return key === df || key.startsWith(`${df} (`) || key.startsWith(`${df}__`)
            })
            keysToDelete.forEach((k) => delete w[k])
          }
          const xBaseField = (X_DER ? X_DER.base : xField) as string | undefined
          if (xBaseField) {
            const keysToDelete = Object.keys(w).filter((k) => {
              const key = String(k)
              return key === xBaseField || key.startsWith(`${xBaseField} (`) || key.startsWith(`${xBaseField}__`)
            })
            keysToDelete.forEach((k) => delete w[k])
          }
          const xPart = xPartExpr as string | undefined
          if (xPart && xPart !== xBaseField) {
            const keysToDelete = Object.keys(w).filter((k) => String(k) === xPart || String(k).startsWith(`${xPart}__`))
            keysToDelete.forEach((k) => delete w[k])
          }
          return Object.keys(w).length ? w : undefined
        } catch { return uiTruthWhere as any }
      })()
      return computePeriodDeltas({
        source,
        datasourceId,
        dateField: deltaDateField!,
        where: whereEff as any,
        legend: (querySpec as any)?.legend as any,
        series: (Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : undefined) as any,
        agg: ((querySpec as any)?.agg || 'count') as any,
        y: (querySpec as any)?.y as any,
        measure: (querySpec as any)?.measure as any,
        mode,
        tzOffsetMinutes,
        weekStart: deltaWeekStart as any,
      })
    },
  })

  // Load time tracking (seconds)
  const [loadingSeconds, setLoadingSeconds] = useState<number>(0)
  const loadStartRef = useRef<number | null>(null)
  const loadingTimerRef = useRef<any>(null)
  useEffect(() => {
    // Start counter on load begin
    if (q.isLoading) {
      loadStartRef.current = Date.now()
      setLoadingSeconds(0)
      if (loadingTimerRef.current) { try { clearInterval(loadingTimerRef.current) } catch {} }
      loadingTimerRef.current = setInterval(() => {
        if (loadStartRef.current != null) {
          setLoadingSeconds(Math.max(0, Math.floor((Date.now() - loadStartRef.current) / 1000)))
        }
      }, 1000)
    }
    // Stop counter and emit final time on success/error
    if (!q.isLoading) {
      if (loadingTimerRef.current) { try { clearInterval(loadingTimerRef.current) } catch {} }
      loadingTimerRef.current = null
      if (loadStartRef.current != null && q.data) {
        const secs = Math.max(0, Math.round((Date.now() - loadStartRef.current) / 1000))
        if (typeof window !== 'undefined' && widgetId && (options as any)?.showLoadTime) {
          try { window.dispatchEvent(new CustomEvent('chart-load-time', { detail: { widgetId, seconds: secs } } as any)) } catch {}
        }
      }
      loadStartRef.current = null
    }
    return () => { if (loadingTimerRef.current) { try { clearInterval(loadingTimerRef.current) } catch {} } }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q.isLoading, !!q.data, widgetId, (options as any)?.showLoadTime])

  // Resolve previous index for tooltips based on active delta mode and current grouping.
  // Fallback to immediate previous neighbor when mode is off or target not found.
  function resolvePrevIndex(curIdx: number, xArr: any[]): number {
    try {
      if (!Array.isArray(xArr) || xArr.length === 0) return -1
      if (curIdx <= 0) return -1
      const mode = (effectiveDeltaMode || 'off') as any
      if (!mode || mode === 'off') return -1

      // Determine effective groupBy
      const seriesGb = (() => {
        try { return (Array.isArray((querySpec as any)?.series) ? (querySpec as any).series.map((s: any) => s?.groupBy).find((v: any) => v && v !== 'none') : undefined) } catch { return undefined }
      })()
      const gbLocal = String(seriesGb || (querySpec as any)?.groupBy || 'none')

      const raw = xArr[curIdx]
      if (raw == null) return -1
      const s = String(raw)

      const toDate = (val: any): Date | null => {
        try {
          if (val instanceof Date) return new Date(val.getTime())
          if (typeof val === 'number' && Number.isFinite(val)) return new Date(Number(val))
          const ss = String(val ?? '')
          let dd: Date | null = parseDateLoose(ss)
          if ((!dd || isNaN(dd.getTime())) && /^(\d{4})-Q([1-4])$/.test(ss)) {
            const m = ss.match(/^(\d{4})-Q([1-4])$/)!
            const y = Number(m[1]), q = Number(m[2])
            dd = new Date(y, (q - 1) * 3, 1)
          }
          if ((!dd || isNaN(dd.getTime())) && /^(\d{4})-W(\d{1,2})$/.test(ss)) {
            const m = ss.match(/^(\d{4})-W(\d{1,2})$/)!
            const y = Number(m[1]), w = Number(m[2])
            const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || env.weekStart) === 'sun')
            if (useSun) {
              const jan1 = new Date(y, 0, 1)
              return new Date(y, 0, 1 + (w - 1) * 7 - jan1.getDay())
            } else {
              const simple = new Date(Date.UTC(y, 0, 1 + (w - 1) * 7))
              const dow = simple.getUTCDay() || 7
              const isoMon = new Date(simple)
              isoMon.setUTCDate(simple.getUTCDate() + 1 - dow)
              return new Date(isoMon.getTime())
            }
          }
          return (dd && !isNaN(dd.getTime())) ? dd : null
        } catch { return null }
      }

      const shiftDate = (d: Date, opts: { days?: number; months?: number; years?: number }) => {
        const nd = new Date(d)
        if (opts.years) nd.setFullYear(nd.getFullYear() + (opts.years || 0))
        if (opts.months) nd.setMonth(nd.getMonth() + (opts.months || 0))
        if (opts.days) nd.setDate(nd.getDate() + (opts.days || 0))
        return nd
      }
      const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
      const weekSun = (date: Date) => { const jan1 = new Date(date.getFullYear(), 0, 1); const day0 = Math.floor((new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime() - jan1.getTime()) / 86400000); return Math.floor((day0 + jan1.getDay()) / 7) + 1 }
      const sameByGb = (a: Date, b: Date) => {
        switch (gbLocal) {
          case 'year': return a.getFullYear() === b.getFullYear()
          case 'quarter': return a.getFullYear() === b.getFullYear() && Math.floor(a.getMonth()/3) === Math.floor(b.getMonth()/3)
          case 'month': return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth()
          case 'week': {
            const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || env.weekStart) === 'sun')
            const wa = useSun ? weekSun(a) : isoWeek(a)
            const wb = useSun ? weekSun(b) : isoWeek(b)
            return a.getFullYear() === b.getFullYear() && wa === wb
          }
          case 'day': return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate()
          default: return a.getTime() === b.getTime()
        }
      }

      const curDate = toDate(raw)
      if (!curDate) return -1
      const shifted = (() => {
        switch (mode) {
          case 'TD_YSTD': return shiftDate(curDate, { days: -1 })
          case 'TW_LW': return shiftDate(curDate, { days: -7 })
          case 'MONTH_LMONTH': return shiftDate(curDate, { months: -1 })
          case 'MTD_LMTD': return shiftDate(curDate, { months: -1 })
          case 'TQ_LQ': return shiftDate(curDate, { months: -3 })
          case 'TY_LY':
          case 'YTD_LYTD':
          case 'Q_TY_VS_Q_LY':
          case 'QTD_TY_VS_QTD_LY':
          case 'M_TY_VS_M_LY':
          case 'MTD_TY_VS_MTD_LY': return shiftDate(curDate, { years: -1 })
          default: return null
        }
      })()
      if (!shifted) return -1

      // Find entry in xArr that matches shifted at the current granularity
      const prevIdx = xArr.findIndex((x: any) => {
        const dx = toDate(x)
        return !!(dx && sameByGb(dx, shifted))
      })
      return (prevIdx >= 0) ? prevIdx : -1
    } catch { return -1 }
  }

  // Custom tooltip (smaller font, themed background; no glow)
  const renderTooltip = ({ active, payload, label }: any) => {
    // Honor global showTooltip toggle for Tremor/Recharts path
    if ((options as any)?.showTooltip === false) return null
    if (typeof window !== 'undefined') {
      try { console.log('[ChartCard] [Tooltip] RECHARTS renderTooltip called', { active, payloadLength: payload?.length, label }) } catch {}
    }
    if (!active || !payload || payload.length === 0) return null
    const idx = Array.isArray(displayData) ? (displayData as any[]).findIndex((d) => String((d as any)?.x) === String(label)) : -1
    if (typeof window !== 'undefined') {
      const dataPoint = Array.isArray(displayData) && idx >= 0 ? displayData[idx] : null
      try { console.log('[ChartCard] [Tooltip] RECHARTS processing', { label, idx, displayDataCount: Array.isArray(displayData) ? displayData.length : 0, dataPoint }) } catch {}
    }
    const applyXCase = (s: string) => {
      const mode = (options as any)?.xLabelCase as 'lowercase'|'capitalize'|'uppercase'|'capitalcase'|'proper'|undefined
      const str = String(s ?? '')
      if (!mode) return str
      switch (mode) {
        case 'lowercase': return str.toLowerCase()
        case 'uppercase':
        case 'capitalcase': return str.toUpperCase()
        case 'capitalize': { const lower = str.toLowerCase(); return lower.length ? (lower[0].toUpperCase() + lower.slice(1)) : lower }
        case 'proper': default: return str.replace(/[_-]+/g, ' ').split(/\s+/).map(w => w ? (w[0].toUpperCase() + w.slice(1).toLowerCase()) : w).join(' ')
      }
    }
    const formatHeaderCase = (s: string) => {
      const mode = (options as any)?.legendLabelCase as 'lowercase'|'capitalize'|'uppercase'|'capitalcase'|'proper'|undefined
      const str = String(s ?? '')
      if (!mode) return str
      switch (mode) {
        case 'lowercase': return str.toLowerCase()
        case 'uppercase':
        case 'capitalcase': return str.toUpperCase()
        case 'capitalize': { const lower = str.toLowerCase(); return lower.length ? (lower[0].toUpperCase() + lower.slice(1)) : lower }
        case 'proper': default: return str.replace(/[_-]+/g, ' ').split(/\s+/).map(w => w ? (w[0].toUpperCase() + w.slice(1).toLowerCase()) : w).join(' ')
      }
    }
    const items = (options?.tooltipHideZeros ? payload.filter((p: any) => Number(p.value ?? 0) !== 0) : payload)
    // Group totals per base series when virtualized; else group all under a single bucket
    const vmeta = (((q?.data as any)?.virtualMeta) || {}) as Record<string, { baseSeriesLabel: string; agg?: string }>
    const groupKeyOf = (name: string) => (vmeta[name]?.baseSeriesLabel || '__group__')
    const groupTotals: Record<string, number> = {}
    items.forEach((p: any) => {
      const v = Number(p.value ?? 0)
      const gk = groupKeyOf(String(p.name))
      groupTotals[gk] = (groupTotals[gk] || 0) + v
    })
    // Build rows
    const xSeq = Array.isArray(displayData) ? (displayData as any[]).map((d:any)=>d?.x) : []
    const prevIdx = resolvePrevIndex(idx, xSeq)
    const hasPrev = prevIdx >= 0
    const rows: TooltipRow[] = items.map((p: any) => {
      const v = Number(p.value ?? 0)
      const rawName = String(p.name)
      if (typeof window !== 'undefined') {
        try { console.log('[ChartCard] [Tooltip] RECHARTS row data', { rawName, payloadValue: p.value, payloadColor: p.color, payload: p }) } catch {}
      }
      const prevFromIndex = hasPrev ? Number((displayData as any[])[prevIdx]?.[rawName] ?? 0) : 0
      const rawX = String(label ?? '')
      const rk = prevRangeKeyForRawX(rawX)
      // Resolve agg/y/measure for this series
      const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
      if (typeof window !== 'undefined') {
        try { console.log('[ChartCard] [Tooltip] RECHARTS seriesDefs check', { 
          rawName, 
          hasQuerySpec: !!(querySpec as any),
          hasSeries: Array.isArray((querySpec as any)?.series),
          seriesLength: seriesDefs.length,
          series: (querySpec as any)?.series,
          querySpec: querySpec
        }) } catch {}
      }
      let sAgg: any = (vmeta[rawName]?.agg as any) || ((querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count'))
      let sY: any = (querySpec as any)?.y as any
      let sMeasure: any = (querySpec as any)?.measure as any
      if (seriesDefs.length > 0) {
        // If there's only one series definition but legend splits it into multiple lines, use that series for all
        if (seriesDefs.length === 1) {
          const s = seriesDefs[0]
          if (typeof window !== 'undefined') {
            try { console.log('[ChartCard] [Tooltip] RECHARTS single series', { rawName, s, sAgg_before: sAgg, s_agg: s?.agg, s_y: s?.y, s_measure: s?.measure }) } catch {}
          }
          sAgg = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
          sY = s?.y
          sMeasure = s?.measure
          if (typeof window !== 'undefined') {
            try { console.log('[ChartCard] [Tooltip] RECHARTS after single series', { sAgg, sY, sMeasure }) } catch {}
          }
        } else {
          // Multiple series - match by label
          const base = extractBaseLabel(String(rawName)).trim()
          for (let i = 0; i < seriesDefs.length; i++) {
            const s = seriesDefs[i]
            const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
            if (String(lab).trim() === base) { sAgg = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count')); sY = s?.y; sMeasure = s?.measure; break }
          }
        }
      }
      const ck = rk ? `${rk}::${sAgg}|${sY || ''}|${sMeasure || ''}` : null
      const totals = ck ? (prevTotalsCacheRef.current[ck] || {}) : {}
      if (typeof window !== 'undefined') {
        try { console.log('[ChartCard] [Tooltip] RECHARTS cache lookup', { rawX, rawName, rk, sAgg, sY, sMeasure, ck, totalsKeys: Object.keys(totals), allCacheKeys: Object.keys(prevTotalsCacheRef.current), querySpec, vmeta, vmetaForThis: vmeta[rawName] }) } catch {}
      }
      // Compute legend/server keys to resolve prev total
      const baseCandidates = (() => {
        try {
          const parts = splitLegend(String(rawName))
          const a = extractBaseLabel(String(rawName)).trim()
          const b = String((vmeta as any)?.[rawName]?.baseSeriesLabel || a)
          const legendPart = parts.cat || ''
          const list: string[] = []
          if (legendPart) list.push(a)
          if (!list.includes(b)) list.push(b)
          return list.length ? list : [a]
        } catch { return [String(rawName)] }
      })()
      const serverPart = (() => {
        try {
          const cur = parseDateLoose(String(rawX))
          if (!cur) return ''
          const pv = shiftByMode(cur)
          if (!pv) return ''
          const pad = (n:number)=>String(n).padStart(2,'0')
          const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
          if (gb === 'year') return `${pv.getFullYear()}`
          if (gb === 'quarter') { const q = Math.floor(pv.getMonth()/3)+1; return `${pv.getFullYear()}-Q${q}` }
          if (gb === 'month') return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}`
          if (gb === 'day') return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}-${pad(pv.getDate())}`
          if (gb === 'week') {
            const ws = (deltaWeekStart || 'mon')
            const date = new Date(Date.UTC(pv.getFullYear(), pv.getMonth(), pv.getDate()))
            if (ws === 'sun') {
              const onejan = new Date(pv.getFullYear(),0,1)
              const week = Math.ceil((((pv as any)- (onejan as any))/86400000 + onejan.getDay()+1)/7)
              return `${pv.getFullYear()}-W${pad(week)}`
            } else {
              const dayNr = (date.getUTCDay() + 6) % 7
              date.setUTCDate(date.getUTCDate() - dayNr + 3)
              const firstThursday = new Date(Date.UTC(date.getUTCFullYear(),0,4))
              const week = 1 + Math.round(((date.getTime() - firstThursday.getTime())/86400000 - 3) / 7)
              return `${date.getUTCFullYear()}-W${pad(week)}`
            }
          }
          return ''
        } catch { return '' }
      })()
      const resolvePrevFromCache = (): number => {
        try {
          if (!ck || !Object.keys(totals || {}).length) return 0
          if (typeof window !== 'undefined') {
            try { console.log('[ChartCard] [Tooltip] RECHARTS resolvePrevFromCache', { rawName, baseCandidates, serverPart, totals, totalsKeys: Object.keys(totals) }) } catch {}
          }
          for (const b of baseCandidates) {
            const keyVirtual = serverPart ? `${b} • ${serverPart}` : b
            const seq = [keyVirtual, b, '__total__']
            if (typeof window !== 'undefined') {
              try { console.log('[ChartCard] [Tooltip] RECHARTS trying keys', { b, keyVirtual, seq }) } catch {}
            }
            for (const k of seq) {
              const n = Number((totals as any)?.[k] ?? 0)
              if (Number.isFinite(n) && (k === '__total__' ? true : (k in (totals as any)))) {
                if (typeof window !== 'undefined') {
                  try { console.log('[ChartCard] [Tooltip] RECHARTS found prev', { key: k, value: n }) } catch {}
                }
                return n
              }
            }
          }
          return 0
        } catch { return 0 }
      }
      const prev = hasPrev ? prevFromIndex : resolvePrevFromCache()
      const delta = v - prev
      const gk = groupKeyOf(String(p.name))
      const tot = Number(groupTotals[gk] || 0)
      const shareStr = tot > 0 ? `${((v / tot) * 100).toFixed(1)}%` : '0.0%'
      const changePct = computeChangePercent(v, prev)
      const changeStr = (hasPrev || prev > 0) ? `${changePct >= 0 ? '+' : '-'}${Math.abs(changePct).toFixed(1)}%` : ''
      const invert = !!(options as any)?.downIsGood
      const changeColor = invert
        ? ((changePct < 0) ? '#22c55e' : (changePct > 10 ? '#ef4444' : '#9CA3AF'))
        : ((changePct < 0) ? '#ef4444' : (changePct > 10 ? '#22c55e' : '#9CA3AF'))
      const deltaStr = (hasPrev || prev > 0) ? `${delta >= 0 ? '+' : '-'}${formatNumber(Math.abs(delta), (options?.yAxisFormat || 'none') as any)}` : ''
      const name = formatHeaderCase(rawName)
      const aggLabel = (() => {
        const fromMeta = (vmeta[rawName]?.agg as any)
        if (fromMeta) {
          if (typeof window !== 'undefined') {
            try { console.log('[ChartCard] [Tooltip] RECHARTS aggLabel from vmeta', { rawName, fromMeta }) } catch {}
          }
          return toProperCase(String(fromMeta))
        }
        try {
          if (seriesDefs.length > 0) {
            // If there's only one series, use it for all legend-split lines
            if (seriesDefs.length === 1) {
              const s = seriesDefs[0]
              const ag = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
              if (typeof window !== 'undefined') {
                try { console.log('[ChartCard] [Tooltip] RECHARTS aggLabel from single series', { rawName, seriesDef: s, ag }) } catch {}
              }
              return toProperCase(String(ag))
            }
            // Multiple series - match by label
            const base = extractBaseLabel(String(rawName)).trim()
            for (let i = 0; i < seriesDefs.length; i++) {
              const s = seriesDefs[i]
              const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
              if (String(lab).trim() === base) {
                const ag = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
                if (typeof window !== 'undefined') {
                  try { console.log('[ChartCard] [Tooltip] RECHARTS aggLabel from matched series', { rawName, base, seriesDef: s, ag }) } catch {}
                }
                return toProperCase(String(ag))
              }
            }
          }
        } catch {}
        const fallback = (querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')
        if (typeof window !== 'undefined') {
          try { console.log('[ChartCard] [Tooltip] RECHARTS aggLabel fallback', { rawName, qsAgg: (querySpec as any)?.agg, qsY: (querySpec as any)?.y, qsMeasure: (querySpec as any)?.measure, fallback }) } catch {}
        }
        return toProperCase(String(fallback))
      })()
      return {
        color: String(p.color || '#94a3b8'),
        name,
        valueStr: formatNumber(v, (options?.yAxisFormat || 'none') as any),
        shareStr,
        aggLabel,
        changeStr,
        deltaStr,
        prevStr: (hasPrev || prev > 0) ? formatNumber(prev, (options?.yAxisFormat || 'none') as any) : '',
        changeColor,
      }
    })
    const prevLabel = (() => {
      if (hasPrev) return applyXCase(String(xSeq[prevIdx]))
      // Derive fallback prev label from current label when in delta mode
      try {
        if (!effectiveDeltaMode) return undefined
        const cur = parseDateLoose(String(label))
        if (!cur) return undefined
        const pv = (() => {
          switch (effectiveDeltaMode as any) {
            case 'TD_YSTD': { const d = new Date(cur); d.setDate(d.getDate()-1); return d }
            case 'TW_LW': { const d = new Date(cur); d.setDate(d.getDate()-7); return d }
            case 'MONTH_LMONTH': { const d = new Date(cur); d.setMonth(d.getMonth()-1); return d }
            case 'MTD_LMTD': { const d = new Date(cur); d.setMonth(d.getMonth()-1); return d }
            case 'TQ_LQ': { const d = new Date(cur); d.setMonth(d.getMonth()-3); return d }
            case 'TY_LY':
            case 'YTD_LYTD':
            case 'Q_TY_VS_Q_LY':
            case 'QTD_TY_VS_QTD_LY':
            case 'M_TY_VS_M_LY':
            case 'MTD_TY_VS_MTD_LY': { const d = new Date(cur); d.setFullYear(d.getFullYear()-1); return d }
            default: return null
          }
        })()
        if (!pv) return undefined
        const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
        const pad = (n:number)=>String(n).padStart(2,'0')
        const fmt = (options as any)?.xDateFormat
          || (gb === 'year' ? 'YYYY'
            : gb === 'quarter' ? 'YYYY-[Q]q'
            : gb === 'month' ? 'MMM-YYYY'
            : gb === 'week' ? 'YYYY-[W]ww'
            : 'YYYY-MM-DD')
        // Basic formatter to avoid bringing moment: match existing simple formats
        const monthShort = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][pv.getMonth()]
        const q = Math.floor(pv.getMonth()/3)+1
        const isoWeek = (() => { const d=new Date(Date.UTC(pv.getFullYear(), pv.getMonth(), pv.getDate())); d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay()||7)); const yStart=new Date(Date.UTC(d.getUTCFullYear(),0,1)); return Math.ceil((((d.getTime()-yStart.getTime())/86400000)+1)/7) })()
        const str = fmt
          .replace('YYYY', String(pv.getFullYear()))
          .replace('[Q]q', `Q${q}`)
          .replace('MMM', monthShort)
          .replace('MM', pad(pv.getMonth()+1))
          .replace('DD', pad(pv.getDate()))
          .replace('[W]ww', `W${pad(isoWeek)}`)
        return applyXCase(str)
      } catch { return undefined }
    })()
    const header = applyXCase(String(label))
    return (<TooltipTable header={header} prevLabel={prevLabel} rows={rows} showDeltas={!!effectiveDeltaMode} />)
  }

  // Compute chart colors (Tremor tokens) and matching legend hex swatches
  const chartColorsTokens = useMemo(() => {
    const base = getPresetPalette(options?.colorPreset as any) || chartColors
    if (isMulti) {
      const cats = Array.isArray(categories) ? (categories as string[]) : []
      const N = base.length || 1
      const step = N >= 11 ? 7 : (N >= 7 ? 5 : 3)
      const hash = (s: string) => {
        let h = 0
        for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0
        return h >>> 0
      }
      if (cats.length > 0) {
        // Use sequential assignment when we have fewer categories than colors for guaranteed distinction
        // Use hash-based assignment for many categories to maintain consistency across charts
        if (cats.length <= N) {
          return cats.map((label, i) => base[i % N] as any)
        }
        // For many categories, use hash-based assignment
        return cats.map((label, i) => {
          const idx = ((hash(String(label)) * step) % N)
          return base[idx] as any
        })
      }
      const count = Math.max(1, (series?.length ?? 0))
      return Array.from({ length: count }, (_, i) => {
        const s = series?.[i] as any
        if (s?.colorKey) return s.colorKey as any
        return (s?.colorToken ? tokenToColorKey(s.colorToken as any) : (base[(i * step) % N])) as any
      })
    }
    const single = options?.colorToken ? tokenToColorKey(options.colorToken as any) : ((options?.color as any) || 'blue')
    return [single] as any[]
  }, [series, options, isMulti, categories])
  const legendHexColors = useMemo(() => {
    const count = Math.max(1, (categories?.length ?? 1))
    // Value gradient: legend shows base color (series points vary by value)
    if ((options as any)?.colorMode === 'valueGradient') {
      const baseKey = (options as any)?.colorBaseKey || chartColorsTokens[0] || 'blue'
      const baseHex = tremorNameToHex(baseKey as any)
      return Array.from({ length: count }, () => baseHex)
    }
    // Detect if advanced mode is effectively on for bar/column/line/area
    const denseThreshold = options?.xDenseThreshold ?? 12
    const autoCondense = options?.autoCondenseXLabels ?? true
    const denseX = autoCondense && ((Array.isArray(data) ? (data as any[]).length : 0) > denseThreshold)
    const hasRotate = Math.max(-90, Math.min(90, Number((options as any)?.xTickAngle ?? 0))) !== 0
    const wantSecondaryAxis = Array.isArray(series) && series.some((s: any) => !!(s as any)?.secondaryAxis)
    const typeIsAdvanced = (type === 'bar' || type === 'column' || type === 'line' || type === 'area')
    // If Rich Tooltip is explicitly off, don't force advanced just for dense/rotate; allow simple Tremor tooltip
    const ignoreDenseRotate = ((options as any)?.richTooltip === false)
    const forceAdvanced = ((((options as any)?.colorMode === 'valueGradient') || (ignoreDenseRotate ? false : (denseX || hasRotate)) || ((type === 'column') && !!(options as any)?.dataLabelsShow) || wantSecondaryAxis) && typeIsAdvanced)
    const advanced = ((((options as any)?.advancedMode) || forceAdvanced) && typeIsAdvanced)
    if (!advanced) {
      return chartColorsTokens.map((t: any) => tremorNameToHex(t as any))
    }
    // Advanced mode: assign distinct colors per full category label for strong separation
    try {
      return (categories || []).map((_, idx) => tremorNameToHex(chartColorsTokens[idx % chartColorsTokens.length] as any))
    } catch {
      return chartColorsTokens.map((t: any) => tremorNameToHex(t as any))
    }
  }, [chartColorsTokens, (options as any)?.colorMode, (options as any)?.colorBaseKey, categories, type, data, series, (options as any)?.xTickAngle, (options as any)?.advancedMode, (options as any)?.dataLabelsShow, options?.xDenseThreshold, options?.autoCondenseXLabels, q?.data])

  const content = (() => {
    const config = { options }
    const showLegend = config.options?.showLegend ?? true
    // Try to get format from first series/value, fallback to global yAxisFormat
    const seriesFormat = (() => {
      // Check querySpec.series[0].format
      const seriesArr = (querySpec as any)?.series
      if (Array.isArray(seriesArr) && seriesArr.length > 0 && seriesArr[0]?.format) {
        return seriesArr[0].format
      }
      // Check pivot.values[0].format
      const pivotValues = (pivot as any)?.values
      if (Array.isArray(pivotValues) && pivotValues.length > 0 && pivotValues[0]?.format) {
        return pivotValues[0].format
      }
      return null
    })()
    const fmt = seriesFormat || config.options?.yAxisFormat || 'none'
    const valueFormatter = (n: number) => {
      // Coerce non-finite to 0 before any formatting
      if (!Number.isFinite(n)) n = 0
      // Currency with custom locale/currency
      if ((seriesFormat || options?.yAxisFormat || 'none') === 'currency' && options?.valueCurrency) {
        try {
          const s = new Intl.NumberFormat(options?.valueFormatLocale || 'en-US', { style: 'currency', currency: options.valueCurrency, maximumFractionDigits: 2 }).format(n)
          return `${options?.valuePrefix || ''}${s}${options?.valueSuffix || ''}`
        } catch {}
      }
      const base = formatNumber(n, fmt)
      return `${options?.valuePrefix || ''}${base}${options?.valueSuffix || ''}`
    }
    // Hide built-in legends; we'll render a custom legend below
    const chartLegend = false
    const denseThreshold = options?.xDenseThreshold ?? 12
    const autoCondense = options?.autoCondenseXLabels ?? true
    const denseX = autoCondense && (data?.length ?? 0) > denseThreshold

    // UI theme-aware axis text color (default: white in dark, black in light)
    const axisTextColor = (() => {
      try {
        if (typeof window !== 'undefined' && document.documentElement.classList.contains('dark')) return '#FFFFFF'
      } catch {}
      return '#000000'
    })()

    // Utility: convert hex like #3b82f6 to rgba(r,g,b,a)
    const hexToRgba = (hex: string, a = 1) => {
      const m = /^#?([\da-f]{2})([\da-f]{2})([\da-f]{2})$/i.exec(hex)
      if (!m) return hex
      const r = parseInt(m[1], 16), g = parseInt(m[2], 16), b = parseInt(m[3], 16)
      return `rgba(${r}, ${g}, ${b}, ${a})`
    }

    // Header formatter for series/legend labels (hoisted to avoid TDZ in ECharts formatters)
    function formatHeader(key: string): string {
      const mode = (options as any)?.legendLabelCase as ('lowercase'|'capitalize'|'uppercase'|'capitalcase'|'proper'|undefined)
      const str = String(key ?? '')
      if (!mode) return str
      switch (mode) {
        case 'lowercase': return str.toLowerCase()
        case 'uppercase':
        case 'capitalcase': return str.toUpperCase()
        case 'capitalize': {
          const lower = str.toLowerCase()
          return lower.length ? (lower[0].toUpperCase() + lower.slice(1)) : lower
        }
        case 'proper': default:
          return str.replace(/[_-]+/g, ' ').split(/\s+/).map(w => w ? (w[0].toUpperCase() + w.slice(1).toLowerCase()) : w).join(' ')
      }
    }

    // Legend label helpers: allow different case for category part and drop series prefix when single series
    const isSingleSeries = Array.isArray(series) ? (series.length <= 1) : true
    function splitLegend(raw: string): { base: string; cat?: string } {
      const s = String(raw || '')
      // New format: "Category - Series" (was "Series • Category")
      const parts = s.includes(' - ')
        ? s.split(' - ')
        : (s.includes(' • ') // Support legacy format
          ? s.split(' • ')
          : (s.includes(' · ')
            ? s.split(' · ')
            : [s]))
      if (parts.length >= 2) {
        // New format has category first, base second
        if (s.includes(' - ')) return { cat: parts[0], base: parts.slice(1).join(' - ') }
        // Legacy format has base first, cat second
        return { base: parts[0], cat: parts.slice(1).join(' • ') }
      }
      return { base: s }
    }
    // Helper to extract base series label from category name (supports both new and legacy formats)
    function extractBaseLabel(categoryName: string): string {
      const parts = splitLegend(categoryName)
      return parts.base || categoryName
    }
    function formatCategoryCase(name: string): string {
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
          return str.replace(/[_-]+/g, ' ').split(/\s+/).map(w=>w?(w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
      }
    }
    function legendDisplayName(rawName: string): string {
      try {
        const s = String(rawName ?? '')
        if (s.toLowerCase() === 'null') return 'None'
        const parts = splitLegend(s)
        if (isSingleSeries && parts.cat) return formatCategoryCase(parts.cat)
        if (!parts.cat) return formatHeader(s)
        // New format: "Category - Series"
        return `${formatCategoryCase(parts.cat)} - ${formatHeader(parts.base)}`
      } catch { return String(rawName ?? '') }
    }

    // Advanced mode uses ECharts for more control
    // Also auto-switch to advanced when x is dense or when rotation is non-zero so we can apply tick rotation.
    const hasRotate = Math.max(-90, Math.min(90, Number((options as any)?.xTickAngle ?? 0))) !== 0
    // Detect if any series opts into the secondary axis (per-chip toggle)
    const wantSecondaryAxis = Array.isArray(series) && series.some((s: any) => !!(s as any)?.secondaryAxis)
    // Also force advanced when column charts show data labels so we can rotate labels vertically via labelLayout
    const ignoreDenseRotate2 = ((options as any)?.richTooltip === false)
    const forceAdvanced = (
      (((options as any)?.colorMode === 'valueGradient') || (ignoreDenseRotate2 ? false : (denseX || hasRotate)) || ((type === 'column') && !!(options as any)?.dataLabelsShow) || wantSecondaryAxis)
    ) && (type === 'bar' || type === 'column' || type === 'line' || type === 'area')
    if ((((options as any)?.advancedMode) || forceAdvanced) && (type === 'bar' || type === 'column' || type === 'line' || type === 'area')) {
      const xLabels = (displayData as any[]).map((d) => d.x)
      const gb = String(((querySpec as any)?.groupBy || 'none') as any).toLowerCase()
      const rawXSpec = (querySpec as any)?.x
      const isMultiLevelX = Array.isArray(rawXSpec) && rawXSpec.length > 1
      const xFieldStr = String(Array.isArray(rawXSpec) ? rawXSpec[0] : (rawXSpec || ''))
      // Multi-level X axis helpers (Excel-style hierarchical grouping)
      const MULTI_LEVEL_SEP = '|'
      const parseMultiLevelX = (val: any): string[] => {
        const s = String(val ?? '')
        return s.includes(MULTI_LEVEL_SEP) ? s.split(MULTI_LEVEL_SEP) : [s]
      }
      // Smart formatter: shows outer level (e.g., Year) only when it changes from previous value
      const formatMultiLevelLabel = (val: any, idx: number, allLabels: any[]): string => {
        if (!isMultiLevelX) return String(val ?? '')
        const parts = parseMultiLevelX(val)
        if (parts.length < 2) return parts[0] || ''
        const innerLabel = parts[0]  // e.g., Day "1"
        const outerLabel = parts[1]  // e.g., Year "2024"
        // Check if outer level changed from previous
        const prevParts = idx > 0 ? parseMultiLevelX(allLabels[idx - 1]) : []
        const outerChanged = prevParts.length < 2 || prevParts[1] !== outerLabel
        // Return multi-line label with outer level only when it changes
        return outerChanged ? `${innerLabel}\n{groupLabel|${outerLabel}}` : innerLabel
      }
      const legendRaw = (() => {
        const lg: any = (querySpec as any)?.legend
        if (Array.isArray(lg) && lg.length > 0) return String(lg[0] ?? '')
        return typeof lg === 'string' ? lg : ''
      })()
      let seasonalityMode = false
      try {
        const m = legendRaw.match(/^(.*)\s*\(Year\)$/i)
        if (m) {
          const base = m[1].trim()
          if (gb === 'month' && base === xFieldStr) {
            seasonalityMode = true
            console.log('[ChartCard] ✅ 12-MONTH SEASONALITY ENABLED (advanced)', { gb, xFieldStr, legendRaw, base })
          }
        }
        if (!seasonalityMode) {
          console.log('[ChartCard] ❌ Seasonality NOT detected (advanced)', { gb, xFieldStr, legendRaw, legendMatch: !!m })
        }
      } catch {}
      const stacked = (options?.barMode === 'stacked')
      const barGap = typeof options?.barGap === 'number' ? `${options!.barGap}%` : '30%'
      const dataLabelsShow = !!options?.dataLabelsShow
      const rawLabelPos = options?.dataLabelPosition as any
      const wantValueGrad = ((options as any)?.colorMode === 'valueGradient')
      const baseKeyForVG = ((options as any)?.colorBaseKey || chartColorsTokens[0] || 'blue')
      const baseHexVG = tremorNameToHex(baseKeyForVG as any)
      const seriesNames: string[] = (() => {
        try {
          const first = (displayData as any[])[0]
          if (first && typeof first === 'object' && !Array.isArray(first)) {
            const keys = Object.keys(first).filter((k) => k !== 'x' && k !== 'value')
            if (keys.length > 0) return keys
          }
          const cats = (categories || []) as string[]
          if (Array.isArray(cats) && cats.length > 0) return cats
        } catch {}
        return (categories || []) as string[]
      })()
      const rowTotals = (displayData as any[]).map((d) => (seriesNames || []).reduce((s, c) => s + Number((d as any)?.[c] ?? 0), 0))
      const seriesMax: Record<string, number> = {}
      // Map per-series metadata by label for overrides (style, stack, secondary axis)
      const metaByName = new Map<string, any>()
      ;(Array.isArray(series) ? series : []).forEach((s: any, i: number) => {
        const label = s?.label || s?.y || s?.measure || `series_${i + 1}`
        metaByName.set(String(label), s)
      })
      ;(seriesNames || []).forEach((c) => { seriesMax[c] = (displayData as any[]).reduce((m, d) => Math.max(m, Number((d as any)?.[c] ?? 0)), 0) })
      // Color derivations per base series for virtual categories
      const vmetaColor = (((q?.data as any)?.virtualMeta) || {}) as Record<string, { baseSeriesLabel: string }>
      const groupOrders: Record<string, { pos: number; count: number; base: string; baseFirstIdx: number }> = {}
      ;(() => {
        try {
          const tmp = new Map<string, string[]>()
          ;(seriesNames || []).forEach((cc: any) => {
            const name = String(cc)
            const base = vmetaColor[name]?.baseSeriesLabel || extractBaseLabel(name)
            const arr = tmp.get(base) || []
            arr.push(name)
            tmp.set(base, arr)
          })
          tmp.forEach((arr, base) => {
            const firstIdx = Math.max(0, (seriesNames as string[]).indexOf(arr[0] || ''))
            arr.forEach((name, i) => { groupOrders[name] = { pos: i, count: arr.length, base, baseFirstIdx: firstIdx } })
          })
        } catch {}
      })()
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.log('[ChartCard] ===  CHART RENDER START ===', { type, categories, displayDataLen: (displayData as any[]).length, xLabelsLen: xLabels.length }) } catch {}
      }
      const seriesList = (seriesNames || []).map((c, idx) => {
        const nameC = String(c)
        const baseHex = wantValueGrad ? baseHexVG : tremorNameToHex(chartColorsTokens[idx % chartColorsTokens.length])
        const rounded = options?.barRounded ? (type === 'bar' ? [0, 6, 6, 0] : [6, 6, 0, 0]) : 0
        // Per-series overrides: resolve by base series label when categories are virtualized
        const baseLabelForMeta = (vmetaColor[nameC]?.baseSeriesLabel || extractBaseLabel(String(nameC)))
        const sMeta: any = Array.isArray(series) ? (metaByName.get(String(baseLabelForMeta)) || series?.[idx]) : undefined
        const gradientWanted = ((type === 'bar' || type === 'column') ? !!options?.barGradient : false)
        const style = (sMeta?.style as any) || (gradientWanted ? 'gradient' : 'solid')
        const gradient = style === 'gradient'
          ? {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: hexToRgba(baseHex, 0.85) },
                { offset: 1, color: hexToRgba(baseHex, 1) },
              ],
            }
          : baseHex

        // Conditional formatting per point (bars only for now)
        const rules = (sMeta?.conditionalRules && sMeta.conditionalRules.length > 0) ? sMeta.conditionalRules : (options?.conditionalRules || [])
        const calcColor = (v: number) => {
          for (const r of rules) {
            const color = r.color ? tremorNameToHex(r.color as any) : undefined
            if (!color) continue
            if (r.when === '>' && v > (r.value as number)) return color
            if (r.when === '>=' && v >= (r.value as number)) return color
            if (r.when === '<' && v < (r.value as number)) return color
            if (r.when === '<=' && v <= (r.value as number)) return color
            if (r.when === 'equals' && v === (r.value as number)) return color
            if (r.when === 'between' && Array.isArray(r.value)) {
              const [a, b] = r.value as [number, number]
              if (v >= a && v <= b) return color
            }
          }
          return undefined
        }

        const seriesType = (type === 'line' || type === 'area') ? 'line' : 'bar'
        // For line/area charts, preserve null values so lines stop at last data point instead of dropping to zero
        const rawValues = (displayData as any[]).map((d) => {
          const val = (d as any)[c] ?? (c === 'value' ? (d as any).value : undefined)
          if (val === null || val === undefined) {
            return (seriesType === 'line') ? null : 0
          }
          return toNum(val, 0)
        })
        // Check if we're using time-series mode
        const gb = String(((querySpec as any)?.groupBy || 'none') as any).toLowerCase()
        const isTimeSeries = (gb && gb !== 'none') && !seasonalityMode
        // For horizontal bars and area charts (which use category axis), use simple values
        // Only line and column charts can use [x,y] pairs with time axis
        const useTimeSeriesFormat = (isTimeSeries && type !== 'bar' && type !== 'area')
        const seriesData = (() => {
          if (wantValueGrad) {
            return rawValues.map((v, i) => {
              if (v === null) return null
              const override = rules.length > 0 ? calcColor(v) : undefined
              const pct = ((categories || []).length > 1)
                ? (rowTotals[i] > 0 ? (v / rowTotals[i]) : 0)
                : ((seriesMax[c] > 0) ? (v / seriesMax[c]) : 0)
              const colorHex = override || saturateHexBy(baseHexVG, Math.max(0, Math.min(1, pct)))
              const baseVal = useTimeSeriesFormat ? [xLabels[i], v] : v
              return { value: baseVal, itemStyle: { color: colorHex, ...(seriesType === 'bar' ? { borderRadius: rounded } : {}) } }
            })
          }
          if (seriesType === 'bar' && rules.length > 0) {
            return rawValues.map((v, i) => {
              if (v === null) return null
              const override = calcColor(v)
              const useColor = override ? (options?.barGradient ? {
                type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
                colorStops: [
                  { offset: 0, color: hexToRgba(override, 0.85) },
                  { offset: 1, color: hexToRgba(override, 1) },
                ],
              } : override) : gradient
              const baseVal = useTimeSeriesFormat ? [xLabels[i], v] : v
              return { value: baseVal, itemStyle: { color: useColor, borderRadius: rounded } }
            })
          }
          // For time-series (except horizontal bars), return [x, y] pairs; otherwise just y values
          return useTimeSeriesFormat ? rawValues.map((v, i) => [xLabels[i], v]) : rawValues
        })()

        // Stack mapping: series item stackId OR options.seriesStackMap[c]
        const stackFromMap = options?.seriesStackMap ? (options.seriesStackMap as any)[c] : undefined
        const stackId = sMeta?.stackId || stackFromMap || (stacked ? 'stack1' : undefined)

        // Map semantic label positions to ECharts positions by orientation
        const isHorizontal = (type === 'bar')
        const mapLabelPos = (raw: any): any => {
          // defaults
          const barDefault = isHorizontal ? 'right' : 'top'
          const lineDefault = 'top'
          if (seriesType === 'line') {
            switch (raw) {
              case 'center': return 'top'
              case 'insideEnd': return 'top'
              case 'insideBase': return 'bottom'
              case 'outsideEnd': return 'top'
              case 'callout': return 'top'
              case 'inside':
              case 'outside':
              case 'top':
              case 'right':
              case 'bottom':
              case 'left':
                return raw
              default:
                return lineDefault
            }
          }
          // bar/column mapping
          switch (raw) {
            case 'center': return 'inside'
            case 'insideEnd': return isHorizontal ? 'insideRight' : 'insideTop'
            case 'insideBase': return isHorizontal ? 'insideLeft' : 'insideBottom'
            case 'outsideEnd': return barDefault
            case 'callout': return barDefault
            // Legacy values passthrough
            case 'inside':
            case 'outside':
            case 'top':
            case 'right':
              return raw
            default:
              return barDefault
          }
        }
        const labelPos = mapLabelPos(rawLabelPos)

        return {
          name: c,
          type: seriesType,
          data: seriesData,
          areaStyle: (type === 'area') ? { opacity: 0.2, color: gradient } : undefined,
          itemStyle: (seriesType === 'bar') ? { color: gradient, borderRadius: rounded } : { color: baseHex },
          ...(seriesType === 'line' ? { lineStyle: { width: (options?.lineWidth ?? 2) }, connectNulls: false } : {}),
          // Secondary axis: for horizontal bars, use xAxis; otherwise yAxis
          ...((type === 'bar')
            ? ((sMeta?.secondaryAxis) ? { xAxisIndex: 1 } : {})
            : ((sMeta?.secondaryAxis) ? { yAxisIndex: 1 } : {})),
          emphasis: { focus: 'series' },
          smooth: true,
          ...(stackId ? { stack: stackId } : {}),
          ...(seriesType === 'bar' ? { barGap, barCategoryGap: barGap } : {}),
          ...(dataLabelsShow
            ? (
                // For column charts (vertical bars), rotate labels vertically
                isHorizontal
                  ? { label: { show: true, position: labelPos, color: axisTextColor, textShadowColor: 'rgba(0,0,0,0.5)', textShadowBlur: 2, textShadowOffsetY: 1, formatter: (p: any) => {
                        const v = Number(p?.value?.value ?? p?.value ?? 0)
                        if (!isFinite(v) || v === 0) return ''
                        return valueFormatter(v)
                      } } }
                  : {
                      label: { show: true, position: labelPos, color: axisTextColor, textShadowColor: 'rgba(0,0,0,0.5)', textShadowBlur: 2, textShadowOffsetY: 1, formatter: (p: any) => {
                        const v = Number(p?.value?.value ?? p?.value ?? 0)
                        if (!isFinite(v) || v === 0) return ''
                        return valueFormatter(v)
                      } },
                      // Use ECharts labelLayout to rotate data labels
                      labelLayout: { rotate: 90, align: 'center', verticalAlign: 'middle' }
                    }
              )
            : {}),
        }
      })
      // Tooltip selection for advanced mode
      const hasSeriesData = Array.isArray(seriesList) && seriesList.length > 0 && seriesList.some((s: any) => Array.isArray(s?.data) && s.data.length > 0)
      const normParams = (p: any): any[] => {
        try { return Array.isArray(p) ? p : (p ? [p] : []) } catch { return [] }
      }
      const useItemTrigger = ((options as any)?.tooltipTrigger === 'item') || (((options as any)?.colorMode === 'valueGradient') && (type === 'line' || type === 'area'))
      const axisPointerType = (type === 'bar' || type === 'column') ? 'shadow' : 'line'
      const axisPtr = (!useItemTrigger && hasSeriesData) ? { type: axisPointerType } : undefined
      const advTooltip = (options?.richTooltip)
        ? {
            show: (((options as any)?.showTooltip ?? true) && hasSeriesData && xLabels.length > 0),
            trigger: useItemTrigger ? 'item' : 'axis',
            axisPointer: axisPtr,
            backgroundColor: 'transparent',
            borderColor: 'transparent',
            textStyle: { color: undefined as any },
            renderMode: 'html',
            confine: true,
            position: (pos: [number, number], _params: any, dom: HTMLElement, _rect: any, size: any) => {
              const x = Number(pos?.[0] || 0)
              const y = Number(pos?.[1] || 0)
              const viewW = Number((size?.viewSize?.[0]) || 0)
              const viewH = Number((size?.viewSize?.[1]) || 0)
              const boxW = Number((dom?.offsetWidth) || (size?.contentSize?.[0]) || 0)
              const boxH = Number((dom?.offsetHeight) || (size?.contentSize?.[1]) || 0)
              const pad = 8
              const offset = 16
              let left = x + offset
              if (left + boxW > viewW - pad) left = x - boxW - offset
              left = Math.max(pad, Math.min(viewW - boxW - pad, left))
              let top = y - Math.round(boxH / 2)
              top = Math.max(pad, Math.min(viewH - boxH - pad, top))
              return [left, top]
            },
            extraCssText: 'padding:0;border:none;background:transparent;box-shadow:none;z-index:99999;',
            formatter: (paramsAny: any) => {
              try {
                const params = normParams(paramsAny)
                if (!params.length) return ''
                const idx = params?.[0]?.dataIndex ?? 0
                const rawX = xLabels[idx]
                const xLabel = fmtXLabel(rawX)
                // Resolve values from data row first, fallback to param.value when needed
                const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
                const hasSeriesSpecs = seriesDefs.length > 0
                const vmeta = (((q?.data as any)?.virtualMeta) || {}) as Record<string, { baseSeriesLabel: string; agg?: string }>
                const groupKeyOf = (rawName: string) => (vmeta[rawName]?.baseSeriesLabel || (hasSeriesSpecs ? rawName : '__legend__'))
                const getValAt = (p:any, i:number): number => {
                  const rawName = String(p?.seriesName ?? '')
                  const row = i >= 0 && i < (displayData as any[]).length ? (displayData as any[])[i] : undefined
                  // Prefer the exact series key present in the shaped row (e.g. "Base • Legend")
                  const vKeyFirst = row
                    ? (Array.isArray(categories) && (categories as any[]).length > 0
                        ? (row as any)?.[rawName]
                        : (row as any)?.value)
                    : undefined
                  let vFromData = Number(vKeyFirst)
                  if (!Number.isFinite(vFromData)) {
                    // Fallback: try the base series label if available
                    const baseKey = String(vmeta[rawName]?.baseSeriesLabel || '')
                    if (baseKey && row && Array.isArray(categories) && (categories as any[]).length > 0) {
                      const alt = Number((row as any)?.[baseKey])
                      if (Number.isFinite(alt)) vFromData = alt
                    }
                  }
                  const valAny = (p?.value as any)
                  const vFromParam = Array.isArray(valAny)
                    ? Number(valAny?.[1] ?? 0)
                    : Number((valAny as any)?.value ?? valAny ?? 0)
                  return Number.isFinite(vFromData) ? vFromData : (Number.isFinite(vFromParam) ? vFromParam : 0)
                }
                const withValues = params.map((p:any)=>({ p, v: getValAt(p, idx) }))
                const filtered = (options?.tooltipHideZeros ? withValues.filter((x:any)=>Number(x.v)!==0) : withValues)
                const groupTotals: Record<string, number> = {}
                filtered.forEach((pp:any) => {
                  const p = pp.p
                  const vv = Number(pp.v ?? 0)
                  const gk = groupKeyOf(String(p.seriesName))
                  groupTotals[gk] = (groupTotals[gk] || 0) + vv
                })
                const xSeq = xLabels
                const prevIdx = resolvePrevIndex(idx, xSeq)
                const hasPrev = prevIdx >= 0
                const prevLabel = (() => {
                  try {
                    const s = String(rawX ?? '')
                    const d = parseDateLoose(s)
                    if (!d) return null
                    const shifted = (() => {
                      switch (effectiveDeltaMode as any) {
                        case 'TD_YSTD': { const x = new Date(d); x.setDate(x.getDate()-1); return x }
                        case 'TW_LW': { const x = new Date(d); x.setDate(x.getDate()-7); return x }
                        case 'MONTH_LMONTH':
                        case 'MTD_LMTD': { const x = new Date(d); x.setMonth(x.getMonth()-1); return x }
                        case 'TQ_LQ': { const x = new Date(d); x.setMonth(x.getMonth()-3); return x }
                        case 'TY_LY':
                        case 'YTD_LYTD':
                        case 'Q_TY_VS_Q_LY':
                        case 'QTD_TY_VS_QTD_LY':
                        case 'M_TY_VS_M_LY':
                        case 'MTD_TY_VS_MTD_LY': { const x = new Date(d); x.setFullYear(x.getFullYear()-1); return x }
                        default: return null
                      }
                    })()
                    if (!shifted) return null
                    const pad = (n:number)=>String(n).padStart(2,'0')
                    const rawShift = `${shifted.getFullYear()}-${pad(shifted.getMonth()+1)}-${pad(shifted.getDate())}`
                    return fmtXLabel(rawShift)
                  } catch { return null }
                })()
                const rows: TooltipRow[] = filtered.map((pp:any) => {
                  const p = pp.p
                  const v = Number(pp.v ?? 0)
                  const rawName = String(p.seriesName)
                  const prev = hasPrev ? (() => {
                    const prevRow = (displayData as any[])[prevIdx] as any
                    const base = (vmeta[rawName]?.baseSeriesLabel || String(rawName).split(' • ')[0])
                    try {
                      const prevKey = `${base} • ${fmtXLabel(xSeq[prevIdx])}`
                      const val = Number(prevRow?.[prevKey] ?? prevRow?.[rawName] ?? 0)
                      return Number.isFinite(val) ? val : 0
                    } catch { return Number(prevRow?.[rawName] ?? 0) }
                  })() : (() => {
                    const rk = prevRangeKeyForRawX(rawX)
                    if (!rk) return 0
                    // Determine per-series agg/y/measure based on the series label prefix
                    const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
                    const sLabelBase = extractBaseLabel(String(rawName)).trim()
                    let sAgg: any = ((vmeta[rawName]?.agg as any) || ((querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')) as any)
                    let sY: any = (querySpec as any)?.y as any
                    let sMeasure: any = (querySpec as any)?.measure as any
                    if (seriesDefs.length > 0) {
                      // If there's only one series, use it for all legend-split lines
                      if (seriesDefs.length === 1) {
                        const s = seriesDefs[0]
                        sAgg = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
                        sY = s?.y
                        sMeasure = s?.measure
                      } else {
                        // Multiple series - match by label
                        for (let i = 0; i < seriesDefs.length; i++) {
                          const s = seriesDefs[i]
                          const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
                          if (String(lab).trim() === sLabelBase) { sAgg = (s?.agg ?? (querySpec as any)?.agg ?? sAgg); sY = s?.y; sMeasure = s?.measure; break }
                        }
                      }
                    }
                    const cacheKey = `${rk}::${sAgg}|${sY || ''}|${sMeasure || ''}`
                    const totals = prevTotalsCacheRef.current[cacheKey] || {}
                    if (typeof window !== 'undefined') {
                      try { console.log('[ChartCard] [Tooltip] cache lookup', { rawX, rawName, sLabelBase, rk, sAgg, sY, sMeasure, cacheKey, totalsKeys: Object.keys(totals), cacheKeys: Object.keys(prevTotalsCacheRef.current) }) } catch {}
                    }
                    // If cache is empty, trigger an on-demand fetch for this specific range to warm the cache.
                    if (Object.keys(totals).length === 0) {
                      const r = prevRangeForRawX(rawX)
                      const source = (querySpec as any)?.source as string | undefined
                      if (r && deltaDateField && source && !inflightPrevFetchesRef.current[cacheKey]) {
                        inflightPrevFetchesRef.current[cacheKey] = true
                        // Build legend arg (match virtual keys) and effective where (strip delta field)
                        const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
                        const xField = (querySpec as any)?.x as string | undefined
                        const legendAny = (querySpec as any)?.legend as any
                        const partTitle = gb === 'year' ? 'Year' : gb === 'quarter' ? 'Quarter' : gb === 'month' ? 'Month' : gb === 'week' ? 'Week' : gb === 'day' ? 'Day' : undefined
                        const X_DER = (() => { try { const m = String(xField || '').match(/^(.*)\s\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/); return m ? { base: m[1], part: m[2] } : null } catch { return null } })()
                        const xPartExpr = X_DER ? String(xField) : ((xField && partTitle) ? `${xField} (${partTitle})` : undefined)
                        let legendArg: any = undefined
                        const hasLegend = (Array.isArray(legendAny) ? legendAny.length > 0 : (typeof legendAny === 'string' && legendAny.trim() !== ''))
                        if (hasLegend) {
                          if (Array.isArray(legendAny)) { const arr = legendAny.slice(); if (xPartExpr) arr.push(xPartExpr); legendArg = arr }
                          else { legendArg = xPartExpr ? [legendAny, xPartExpr] : legendAny }
                        }
                        const whereEff = (() => {
                          try {
                            const w: any = { ...(uiTruthWhere || {}) }
                            const df = deltaDateField as string | undefined
                            if (df) {
                              const keysToDelete = Object.keys(w).filter((k) => {
                                const key = String(k)
                                return key === df || key.startsWith(`${df} (`) || key.startsWith(`${df}__`)
                              })
                              keysToDelete.forEach((k) => delete w[k])
                            }
                            const xBaseField = (X_DER ? X_DER.base : xField) as string | undefined
                            if (xBaseField) {
                              const keysToDelete = Object.keys(w).filter((k) => {
                                const key = String(k)
                                return key === xBaseField || key.startsWith(`${xBaseField} (`) || key.startsWith(`${xBaseField}__`)
                              })
                              keysToDelete.forEach((k) => delete w[k])
                            }
                            const xPart = xPartExpr as string | undefined
                            if (xPart && xPart !== xBaseField) {
                              const keysToDelete = Object.keys(w).filter((k) => String(k) === xPart || String(k).startsWith(`${xPart}__`))
                              keysToDelete.forEach((k) => delete w[k])
                            }
                            return Object.keys(w).length ? w : undefined
                          } catch { return undefined }
                        })()
                        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
                          try { console.debug('[ChartCard] [PrevFallback] ondemand request', { cacheKey, rangeKey: rk, start: r.start, end: r.end, dateField: deltaDateField, legend: legendArg, agg: sAgg, y: sY, measure: sMeasure, where: whereEff }) } catch {}
                        }
                        void (async () => {
                          try {
                            const resp = await Api.periodTotals({
                              source,
                              datasourceId,
                              dateField: deltaDateField!,
                              start: r.start,
                              end: r.end,
                              where: whereEff as any,
                              legend: legendArg,
                              agg: sAgg as any,
                              y: sY as any,
                              measure: sMeasure as any,
                              weekStart: (deltaWeekStart as any) || undefined,
                            })
                            const map: Record<string, number> = { ...(resp?.totals || {}) } as any
                            const t = Number((resp as any)?.total)
                            if (Number.isFinite(t)) map['__total__'] = t
                            prevTotalsCacheRef.current[cacheKey] = map
                            if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevFallback] ondemand response', { cacheKey, rangeKey: rk, totalsKeys: Object.keys(map || {}), total: t }) } catch {} }
                          } catch {} finally { inflightPrevFetchesRef.current[cacheKey] = false }
                        })()
                      }
                    }
                    const baseCandidates = (() => {
                      try {
                        const parts = splitLegend(String(rawName))
                        const a = extractBaseLabel(String(rawName)).trim()
                        const legendPart = parts.cat || ''
                        const b = String(vmeta[rawName]?.baseSeriesLabel || '')
                        // Try to infer legend from the current row by matching the current value v
                        const legendFromRow = (() => {
                          try {
                            const row = (displayData as any[])[idx] as any
                            if (!row || typeof row !== 'object') return ''
                            const prefix = a ? `${a} • ` : ''
                            const keys = Object.keys(row || {}).filter((k) => prefix ? String(k).startsWith(prefix) : false)
                            for (const k of keys) {
                              const valN = Number((row as any)?.[k] ?? 0)
                              if (Number.isFinite(valN) && Math.abs(valN - v) < 1e-9) {
                                return String(k).slice(prefix.length)
                              }
                            }
                            return ''
                          } catch { return '' }
                        })()
                        const list: string[] = []
                        if (legendFromRow) list.push(legendFromRow)
                        if (legendPart && !list.includes(legendPart)) list.push(legendPart)
                        if (a && !list.includes(a)) list.push(a)
                        if (b && !list.includes(b)) list.push(b)
                        return list.length ? list : [String(rawName)]
                      } catch { return [String(rawName)] }
                    })()
                    const pLabel = String(prevLabel || '')
                    // Compute server-formatted part matching backend legend_expr
                    const serverPart = (() => {
                      try {
                        const s = String(rawX ?? '')
                        const cur = parseDateLoose(s)
                        if (!cur) return ''
                        const pv = shiftByMode(cur)
                        if (!pv) return ''
                        const pad = (n:number)=>String(n).padStart(2,'0')
                        const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
                        if (gb === 'year') return `${pv.getFullYear()}`
                        if (gb === 'quarter') { const q = Math.floor(pv.getMonth()/3)+1; return `${pv.getFullYear()}-Q${q}` }
                        if (gb === 'month') return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}`
                        if (gb === 'day') return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}-${pad(pv.getDate())}`
                        if (gb === 'week') {
                          // ISO-week approx (Mon start); if sun, use US week number
                          const ws = (deltaWeekStart || 'mon')
                          const date = new Date(Date.UTC(pv.getFullYear(), pv.getMonth(), pv.getDate()))
                          if (ws === 'sun') {
                            const onejan = new Date(pv.getFullYear(),0,1)
                            const week = Math.ceil((((pv as any)- (onejan as any))/86400000 + onejan.getDay()+1)/7)
                            return `${pv.getFullYear()}-W${pad(week)}`
                          } else {
                            // ISO week: Thursday in current week decides the year
                            const dayNr = (date.getUTCDay() + 6) % 7
                            date.setUTCDate(date.getUTCDate() - dayNr + 3)
                            const firstThursday = new Date(Date.UTC(date.getUTCFullYear(),0,4))
                            const week = 1 + Math.round(((date.getTime() - firstThursday.getTime())/86400000 - 3) / 7)
                            return `${date.getUTCFullYear()}-W${pad(week)}`
                          }
                        }
                        return ''
                      } catch { return '' }
                    })()
                    const resolveFromTotals = () => {
                      for (const b of baseCandidates) {
                        const keyVirtual = pLabel ? `${b} • ${pLabel}` : b
                        const serverKey = serverPart ? `${b} • ${serverPart}` : b
                        const seq = [keyVirtual, serverKey, b, '__total__']
                        for (const k of seq) {
                          const n = Number((totals as any)?.[k] ?? 0)
                          if (Number.isFinite(n) && (k === '__total__' ? true : (k in (totals as any)))) return n
                        }
                      }
                      return 0
                    }
                    const cand = resolveFromTotals()
                    if (typeof window !== 'undefined') {
                      try {
                        console.log('[ChartCard] [PrevFallback] resolve', {
                          x: String(rawX ?? ''),
                          rangeKey: rk,
                          baseCandidates,
                          prevLabel: pLabel,
                          serverPart,
                          totalsKeys: Object.keys(totals || {}),
                          resolved: Number.isFinite(cand) ? cand : 0,
                        })
                      } catch {}
                    }
                    return Number.isFinite(cand) ? cand : 0
                  })()
                  const delta = v - prev
                  const gk = groupKeyOf(String(p.seriesName))
                  const tot = Number(groupTotals[gk] || 0)
                  const shareStr = tot > 0 ? `${((v / tot) * 100).toFixed(1)}%` : '0.0%'
                  const changePct = computeChangePercent(v, prev)
                  const totalsPresentForRange = (() => {
                    try {
                      const partsRN = String(rawName)
                      const partsSplit = partsRN.includes(' • ') ? partsRN.split(' • ') : (partsRN.includes(' · ') ? partsRN.split(' · ') : partsRN.split(' • '))
                      const sLabelBase = (partsSplit[0] || '').trim()
                      const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
                      let sAgg: any = ((vmeta[rawName]?.agg as any) || ((querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')) as any)
                      let sY: any = (querySpec as any)?.y as any
                      let sMeasure: any = (querySpec as any)?.measure as any
                      if (seriesDefs.length > 0) {
                        for (let i = 0; i < seriesDefs.length; i++) {
                          const s = seriesDefs[i]
                          const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
                          if (String(lab).trim() === sLabelBase) { sAgg = (s?.agg ?? (querySpec as any)?.agg ?? sAgg); sY = s?.y; sMeasure = s?.measure; break }
                        }
                      }
                      const _rk = prevRangeKeyForRawX(rawX)
                      const _ck = _rk ? `${_rk}::${sAgg}|${sY || ''}|${sMeasure || ''}` : null
                      return !!(_ck && Object.keys(prevTotalsCacheRef.current[_ck] || {}).length)
                    } catch { return false }
                  })()
                  const havePrevVal = hasPrev || totalsPresentForRange || Number(prev) !== 0
                  const changeStr = havePrevVal ? `${changePct >= 0 ? '+' : '-'}${Math.abs(changePct).toFixed(1)}%` : ''
                  const invert = !!(options as any)?.downIsGood
                  const changeColor = invert
                    ? ((changePct < 0) ? '#22c55e' : (changePct > 10 ? '#ef4444' : '#9CA3AF'))
                    : ((changePct < 0) ? '#ef4444' : (changePct > 10 ? '#22c55e' : '#9CA3AF'))
                  const deltaStr = havePrevVal ? `${delta >= 0 ? '+' : '-'}${valueFormatter(Math.abs(delta))}` : ''
                  const name = legendDisplayName(rawName)
                  const aggLabel = (() => {
                    const fromMeta = (vmeta[rawName]?.agg as any)
                    if (fromMeta) {
                      if (typeof window !== 'undefined') {
                        try { console.log('[ChartCard] [Tooltip] aggLabel from vmeta', { rawName, fromMeta }) } catch {}
                      }
                      return toProperCase(String(fromMeta))
                    }
                    try {
                      const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
                      if (seriesDefs.length > 0) {
                        // If there's only one series, use it for all legend-split lines
                        if (seriesDefs.length === 1) {
                          const s = seriesDefs[0]
                          const ag = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
                          if (typeof window !== 'undefined') {
                            try { console.log('[ChartCard] [Tooltip] aggLabel from single series', { rawName, seriesDef: s, ag }) } catch {}
                          }
                          return toProperCase(String(ag))
                        }
                        // Multiple series - match by label
                        const base = String(rawName).split(' • ')[0].trim()
                        for (let i = 0; i < seriesDefs.length; i++) {
                          const s = seriesDefs[i]
                          const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
                          if (String(lab).trim() === base) {
                            const ag = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
                            if (typeof window !== 'undefined') {
                              try { console.log('[ChartCard] [Tooltip] aggLabel from matched series', { rawName, base, seriesDef: s, ag }) } catch {}
                            }
                            return toProperCase(String(ag))
                          }
                        }
                      }
                    } catch {}
                    const fallback = (querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')
                    if (typeof window !== 'undefined') {
                      try { console.log('[ChartCard] [Tooltip] aggLabel fallback', { rawName, qsAgg: (querySpec as any)?.agg, qsY: (querySpec as any)?.y, qsMeasure: (querySpec as any)?.measure, fallback }) } catch {}
                    }
                    return toProperCase(String(fallback))
                  })()
                  const idxC = Array.isArray(categories) ? Math.max(0, (categories as string[]).indexOf(rawName)) : 0
                  const colorHex = Array.isArray(categories) ? (legendHexColors[idxC % legendHexColors.length] as any) : (String(p.color || '#94a3b8'))
                  return {
                    color: colorHex,
                    name,
                    valueStr: valueFormatter(v),
                    shareStr,
                    aggLabel,
                    changeStr,
                    deltaStr,
                    prevStr: hasPrev ? valueFormatter(prev) : '',
                    changeColor,
                  }
                })
                return ReactDOMServer.renderToString(<TooltipTable header={xLabel} prevLabel={prevLabel || undefined} rows={rows} showDeltas={!!effectiveDeltaMode} />)
              } catch { return '' }
            },
          }
        : { show: (((options as any)?.showTooltip ?? true) && hasSeriesData && xLabels.length > 0), trigger: useItemTrigger ? 'item' : 'axis', axisPointer: axisPtr, backgroundColor: 'transparent', borderColor: 'transparent', textStyle: { color: undefined as any }, renderMode: 'html', confine: true, extraCssText: 'padding:0;border:none;background:transparent;box-shadow:none;z-index:99999;', position: (pos: [number, number], _params: any, dom: HTMLElement, _rect: any, size: any) => {
              const x = Number(pos?.[0] || 0)
              const y = Number(pos?.[1] || 0)
              const viewW = Number((size?.viewSize?.[0]) || 0)
              const viewH = Number((size?.viewSize?.[1]) || 0)
              const boxW = Number((dom?.offsetWidth) || (size?.contentSize?.[0]) || 0)
              const boxH = Number((dom?.offsetHeight) || (size?.contentSize?.[1]) || 0)
              const pad = 8
              const offset = 16
              let left = x + offset
              if (left + boxW > viewW - pad) left = x - boxW - offset
              left = Math.max(pad, Math.min(viewW - boxW - pad, left))
              let top = y - Math.round(boxH / 2)
              top = Math.max(pad, Math.min(viewH - boxH - pad, top))
              return [left, top]
            },
            formatter: (paramsAny: any) => {
              try {
                const params = normParams(paramsAny)
                if (!params.length) return ''
                const idx = params?.[0]?.dataIndex ?? 0
                const rawX = xLabels[idx]
                const xLabel = fmtXLabel(rawX)
                const filtered = (options?.tooltipHideZeros ? params.filter((p:any)=>Number(p?.value?.value ?? p?.value ?? 0)!==0) : params)
                const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
                const hasSeriesSpecs = seriesDefs.length > 0
                const vmeta = (((q?.data as any)?.virtualMeta) || {}) as Record<string, { baseSeriesLabel: string; agg?: string }>
                const groupKeyOf = (rawName: string) => (vmeta[rawName]?.baseSeriesLabel || (hasSeriesSpecs ? rawName : '__legend__'))
                const groupTotals: Record<string, number> = {}
                filtered.forEach((pp:any) => {
                  const vv = Number(pp.value?.value ?? pp.value ?? 0)
                  const gk = groupKeyOf(String(pp.seriesName))
                  groupTotals[gk] = (groupTotals[gk] || 0) + vv
                })
                const xSeq = xLabels
                const prevIdx = resolvePrevIndex(idx, xSeq)
                const hasPrev = prevIdx >= 0
                const prevLabel = (() => {
                  try {
                    const s = String(rawX ?? '')
                    const d = parseDateLoose(s)
                    if (!d) return undefined
                    const shifted = (() => {
                      switch (effectiveDeltaMode as any) {
                        case 'TD_YSTD': { const x = new Date(d); x.setDate(x.getDate()-1); return x }
                        case 'TW_LW': { const x = new Date(d); x.setDate(x.getDate()-7); return x }
                        case 'MONTH_LMONTH':
                        case 'MTD_LMTD': { const x = new Date(d); x.setMonth(x.getMonth()-1); return x }
                        case 'TQ_LQ': { const x = new Date(d); x.setMonth(x.getMonth()-3); return x }
                        case 'TY_LY':
                        case 'YTD_LYTD':
                        case 'Q_TY_VS_Q_LY':
                        case 'QTD_TY_VS_QTD_LY':
                        case 'M_TY_VS_M_LY':
                        case 'MTD_TY_VS_MTD_LY': { const x = new Date(d); x.setFullYear(x.getFullYear()-1); return x }
                        default: return null
                      }
                    })()
                    if (!shifted) return undefined
                    const pad = (n:number)=>String(n).padStart(2,'0')
                    const rawShift = `${shifted.getFullYear()}-${pad(shifted.getMonth()+1)}-${pad(shifted.getDate())}`
                    return fmtXLabel(rawShift)
                  } catch { return undefined }
                })()
                const rows: TooltipRow[] = filtered.map((pp:any) => {
                  const p = pp.p
                  const v = Number(pp.v ?? 0)
                  const rawName = String(p.seriesName)
                  const prev = hasPrev ? (() => {
                    const prevRow = (displayData as any[])[prevIdx] as any
                    const base = (vmeta[rawName]?.baseSeriesLabel || String(rawName).split(' • ')[0])
                    try {
                      const prevKey = `${base} • ${fmtXLabel(xSeq[prevIdx])}`
                      const val = Number(prevRow?.[prevKey] ?? prevRow?.[rawName] ?? 0)
                      return Number.isFinite(val) ? val : 0
                    } catch { return Number(prevRow?.[rawName] ?? 0) }
                  })() : (() => {
                    // Aggregator-aware fallback to server totals cache for previous period bucket
                    const rk = prevRangeKeyForRawX(rawX)
                    if (!rk) return 0
                    // Determine per-series agg/y/measure by matching series label base (supports ' • ' or ' · ')
                    const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
                    const partsRN = String(rawName)
                    const partsSplit = partsRN.includes(' • ') ? partsRN.split(' • ') : (partsRN.includes(' · ') ? partsRN.split(' · ') : partsRN.split(' • '))
                    const sLabelBase = (partsSplit[0] || '').trim()
                    let sAgg: any = ((vmeta[rawName]?.agg as any) || ((querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')) as any)
                    let sY: any = (querySpec as any)?.y as any
                    let sMeasure: any = (querySpec as any)?.measure as any
                    if (seriesDefs.length > 0) {
                      for (let i = 0; i < seriesDefs.length; i++) {
                        const s = seriesDefs[i]
                        const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
                        if (String(lab).trim() === sLabelBase) { sAgg = (s?.agg ?? (querySpec as any)?.agg ?? sAgg); sY = s?.y; sMeasure = s?.measure; break }
                      }
                    }
                    const cacheKey = `${rk}::${sAgg}|${sY || ''}|${sMeasure || ''}`
                    const totals = prevTotalsCacheRef.current[cacheKey] || {}
                    if (Object.keys(totals).length === 0) {
                      // Warm cache on-demand for this series config
                      const r = prevRangeForRawX(rawX)
                      const source = (querySpec as any)?.source as string | undefined
                      if (r && deltaDateField && source && !inflightPrevFetchesRef.current[cacheKey]) {
                        inflightPrevFetchesRef.current[cacheKey] = true
                        const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
                        const xField = (querySpec as any)?.x as string | undefined
                        const legendAny = (querySpec as any)?.legend as any
                        const partTitle = gb === 'year' ? 'Year' : gb === 'quarter' ? 'Quarter' : gb === 'month' ? 'Month' : gb === 'week' ? 'Week' : gb === 'day' ? 'Day' : undefined
                        const X_DER = (() => { try { const m = String(xField || '').match(/^(.*)\s\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/); return m ? { base: m[1], part: m[2] } : null } catch { return null } })()
                        const xPartExpr = X_DER ? String(xField) : ((xField && partTitle) ? `${xField} (${partTitle})` : undefined)
                        let legendArg: any = undefined
                        if (Array.isArray(legendAny)) { const arr = legendAny.slice(); if (xPartExpr) arr.push(xPartExpr); legendArg = arr }
                        else if (typeof legendAny === 'string' && legendAny.trim()) { legendArg = xPartExpr ? [legendAny, xPartExpr] : legendAny }
                        else if (xPartExpr) { legendArg = xPartExpr }
                        const whereEff = (() => {
                          try {
                            const w: any = { ...(uiTruthWhere || {}) }
                            const df = deltaDateField as string | undefined
                            if (df) {
                              const keysToDelete = Object.keys(w).filter((k) => {
                                const key = String(k)
                                return key === df || key.startsWith(`${df} (`) || key.startsWith(`${df}__`)
                              })
                              keysToDelete.forEach((k) => delete w[k])
                            }
                            const xBaseField = (X_DER ? X_DER.base : xField) as string | undefined
                            if (xBaseField) {
                              const keysToDelete = Object.keys(w).filter((k) => {
                                const key = String(k)
                                return key === xBaseField || key.startsWith(`${xBaseField} (`) || key.startsWith(`${xBaseField}__`)
                              })
                              keysToDelete.forEach((k) => delete w[k])
                            }
                            const xPart = xPartExpr as string | undefined
                            if (xPart && xPart !== xBaseField) {
                              const keysToDelete = Object.keys(w).filter((k) => String(k) === xPart || String(k).startsWith(`${xPart}__`))
                              keysToDelete.forEach((k) => delete w[k])
                            }
                            return Object.keys(w).length ? w : undefined
                          } catch { return undefined }
                        })()
                        if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevFallbackAxis] ondemand request', { cacheKey, rangeKey: rk, start: r.start, end: r.end, dateField: deltaDateField, legend: legendArg, agg: sAgg, y: sY, measure: sMeasure, where: whereEff }) } catch {} }
                        void (async () => {
                          try {
                            const resp = await Api.periodTotals({ source, datasourceId, dateField: deltaDateField!, start: r.start, end: r.end, where: whereEff as any, legend: legendArg, agg: (sAgg as any), y: (sY as any), measure: (sMeasure as any) })
                            const map: Record<string, number> = { ...(resp?.totals || {}) } as any
                            const t = Number((resp as any)?.total)
                            if (Number.isFinite(t)) map['__total__'] = t
                            prevTotalsCacheRef.current[cacheKey] = map
                            if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevFallbackAxis] ondemand response', { cacheKey, rangeKey: rk, totalsKeys: Object.keys(map || {}), total: t }) } catch {} }
                          } catch {} finally { inflightPrevFetchesRef.current[cacheKey] = false }
                        })()
                      }
                    }
                    // Prefer legend-derived base candidates when resolving totals
                    const baseCandidates = (() => {
                      try {
                        const a = (partsSplit[0] || '').trim()
                        const legendPart = partsSplit.slice(1).join(' • ').trim()
                        const b = String(vmeta[rawName]?.baseSeriesLabel || '')
                        // Infer legend from current row by matching the current value v
                        const legendFromRow = (() => {
                          try {
                            const row = (displayData as any[])[idx] as any
                            if (!row || typeof row !== 'object') return ''
                            const prefix = a ? `${a} • ` : ''
                            const keys = Object.keys(row || {}).filter((k) => prefix ? String(k).startsWith(prefix) : false)
                            for (const k of keys) {
                              const valN = Number((row as any)?.[k] ?? 0)
                              if (Number.isFinite(valN) && Math.abs(valN - v) < 1e-9) return String(k).slice(prefix.length)
                            }
                            return ''
                          } catch { return '' }
                        })()
                        const list: string[] = []
                        if (legendFromRow) list.push(legendFromRow)
                        if (legendPart && !list.includes(legendPart)) list.push(legendPart)
                        if (a && !list.includes(a)) list.push(a)
                        if (b && !list.includes(b)) list.push(b)
                        return list.length ? list : [String(rawName)]
                      } catch { return [String(rawName)] }
                    })()
                    const pLabel = String(prevLabel || '')
                    const serverPart = (() => {
                      try {
                        const s = String(rawX ?? '')
                        const cur = parseDateLoose(s); if (!cur) return ''
                        const pv = shiftByMode(cur); if (!pv) return ''
                        const pad = (n:number)=>String(n).padStart(2,'0')
                        const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
                        if (gb === 'year') return `${pv.getFullYear()}`
                        if (gb === 'quarter') { const q = Math.floor(pv.getMonth()/3)+1; return `${pv.getFullYear()}-Q${q}` }
                        if (gb === 'month') return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}`
                        if (gb === 'day') return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}-${pad(pv.getDate())}`
                        if (gb === 'week') {
                          const ws = (deltaWeekStart || 'mon')
                          const date = new Date(Date.UTC(pv.getFullYear(), pv.getMonth(), pv.getDate()))
                          if (ws === 'sun') {
                            const onejan = new Date(pv.getFullYear(),0,1)
                            const week = Math.ceil((((pv as any)- (onejan as any))/86400000 + onejan.getDay()+1)/7)
                            return `${pv.getFullYear()}-W${String(week).padStart(2,'0')}`
                          } else {
                            const dayNr = (date.getUTCDay() + 6) % 7
                            date.setUTCDate(date.getUTCDate() - dayNr + 3)
                            const firstThursday = new Date(Date.UTC(date.getUTCFullYear(),0,4))
                            const week = 1 + Math.round(((date.getTime() - firstThursday.getTime())/86400000 - 3) / 7)
                            return `${date.getUTCFullYear()}-W${String(week).padStart(2,'0')}`
                          }
                        }
                        return ''
                      } catch { return '' }
                    })()
                    const resolveFromTotals = () => {
                      for (const b of baseCandidates) {
                        const keyVirtual = pLabel ? `${b} • ${pLabel}` : b
                        const serverKey = serverPart ? `${b} • ${serverPart}` : b
                        const seq = [keyVirtual, serverKey, b, '__total__']
                        for (const k of seq) {
                          const n = Number((totals as any)?.[k] ?? 0)
                          if (Number.isFinite(n) && (k === '__total__' ? true : (k in (totals as any)))) return n
                        }
                      }
                      return 0
                    }
                    const cand = resolveFromTotals()
                    if (typeof window !== 'undefined') { try { console.log('[ChartCard] [PrevFallbackAxis] resolve', { x: String(rawX ?? ''), rangeKey: rk, cacheKey, baseCandidates, prevLabel: pLabel, serverPart, totalsKeys: Object.keys(totals || {}), resolved: Number.isFinite(cand) ? cand : 0 }) } catch {} }
                    return Number.isFinite(cand) ? cand : 0
                  })()
                  const delta = v - prev
                  const gk = groupKeyOf(String(p.seriesName))
                  const tot = Number(groupTotals[gk] || 0)
                  const shareStr = tot > 0 ? `${((v / tot) * 100).toFixed(1)}%` : '0.0%'
                  const changePct = computeChangePercent(v, prev)
                  const totalsPresentForRange = (() => {
                    try {
                      const partsRN = String(rawName)
                      const partsSplit = partsRN.includes(' • ') ? partsRN.split(' • ') : (partsRN.includes(' · ') ? partsRN.split(' · ') : partsRN.split(' • '))
                      const sLabelBase = (partsSplit[0] || '').trim()
                      const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
                      let sAgg: any = ((vmeta[rawName]?.agg as any) || ((querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')) as any)
                      let sY: any = (querySpec as any)?.y as any
                      let sMeasure: any = (querySpec as any)?.measure as any
                      if (seriesDefs.length > 0) {
                        for (let i = 0; i < seriesDefs.length; i++) {
                          const s = seriesDefs[i]
                          const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
                          if (String(lab).trim() === sLabelBase) { sAgg = (s?.agg ?? (querySpec as any)?.agg ?? sAgg); sY = s?.y; sMeasure = s?.measure; break }
                        }
                      }
                      const _rk = prevRangeKeyForRawX(rawX)
                      const _ck = _rk ? `${_rk}::${sAgg}|${sY || ''}|${sMeasure || ''}` : null
                      return !!(_ck && Object.keys(prevTotalsCacheRef.current[_ck] || {}).length)
                    } catch { return false }
                  })()
                  const havePrevVal = hasPrev || totalsPresentForRange || Number(prev) !== 0
                  const changeStr = havePrevVal ? `${changePct >= 0 ? '+' : '-'}${Math.abs(changePct).toFixed(1)}%` : ''
                  const changeColor = (changePct < 0) ? '#ef4444' : (changePct > 10 ? '#22c55e' : '#9CA3AF')
                  const deltaStr = havePrevVal ? `${delta >= 0 ? '+' : '-'}${valueFormatter(Math.abs(delta))}` : ''
                  const name = (typeof formatHeader === 'function') ? formatHeader(rawName) : rawName
                  const aggLabel = (() => {
                    const fromMeta = (vmeta[rawName]?.agg as any)
                    if (fromMeta) return toProperCase(String(fromMeta))
                    try {
                      const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
                      if (seriesDefs.length > 0) {
                        const base = String(rawName).split(' • ')[0].trim()
                        for (let i = 0; i < seriesDefs.length; i++) {
                          const s = seriesDefs[i]
                          const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
                          if (String(lab).trim() === base) {
                            const ag = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
                            return toProperCase(String(ag))
                          }
                        }
                      }
                    } catch {}
                    const fallback = (querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')
                    return toProperCase(String(fallback))
                  })()
                  const idxC = Array.isArray(categories) ? Math.max(0, (categories as string[]).indexOf(rawName)) : 0
                  const colorHex = Array.isArray(categories) ? (legendHexColors[idxC % legendHexColors.length] as any) : (String(p.color || '#94a3b8'))
                  return {
                    color: colorHex,
                    name,
                    valueStr: valueFormatter(v),
                    shareStr,
                    aggLabel,
                    changeStr,
                    deltaStr,
                    prevStr: hasPrev ? valueFormatter(prev) : '',
                    changeColor,
                  }
                })
                return ReactDOMServer.renderToString(<TooltipTable header={xLabel} prevLabel={prevLabel || undefined} rows={rows} showDeltas={!!effectiveDeltaMode} />)
              } catch { return '' }
            }
          }

      // Choose an interval so ~tickCount labels show (fallback to 8)
      const desiredTickCount = Math.max(2, Number(options?.xTickCount || 8))
      const xInterval = Math.max(0, Math.ceil(Math.max(1, xLabels.length) / desiredTickCount) - 1)
      // Base axis size fallback: prefer explicit per-axis sizes, else 11
      const fontSize = Math.max(8, Number(((options as any)?.xAxisFontSize ?? (options as any)?.yAxisFontSize ?? 11)))
      const xRotate = Math.max(-90, Math.min(90, Number((options as any)?.xTickAngle ?? 0)))
      const wantLabels = !!(options as any)?.dataLabelsShow
      const gridTopPad = wantLabels ? 24 : 0
      const gridBottomPad = Math.abs(xRotate) >= 60 ? Math.max(28, fontSize * 2) : 0
      // Optional visualMap to vary line/area stroke color by value
      const visualMap = (() => {
        if (!wantValueGrad) return undefined
        if (!(type === 'line' || type === 'area')) return undefined
        // Global max across all categories for the y dimension
        const gmax = Math.max(1, ...((categories || []).map((c) => (data as any[]).reduce((m, d) => Math.max(m, Number((d as any)?.[c] ?? 0)), 0))))
        const c1 = saturateHexBy(baseHexVG, 0.25)
        const c2 = saturateHexBy(baseHexVG, 0.95)
        // All series are line type in this mode
        const lineIdxs = Array.from({ length: (categories || []).length }, (_, i) => i)
        return [{ show: false, type: 'continuous', min: 0, max: gmax, dimension: 0, seriesIndex: lineIdxs, inRange: { color: [c1, c2] } }] as any
      })()
      
      // X label formatter (dates) — declared as a function to avoid TDZ issues when used inside tooltip/axis formatters
      function fmtXLabel(raw: any): string {
        const s = String(raw ?? '')
        const d = parseDateLoose(s)
        // Preferred format: explicit option, else by groupBy, else infer time-of-day if present
        const inferredTimeFmt = (() => {
          try {
            const first = (xLabels || []).map((v:any)=>parseDateLoose(String(v??''))).find((dd:any)=>!!dd) as Date|undefined
            if (first && (first.getHours()!==0 || first.getMinutes()!==0 || first.getSeconds()!==0)) return 'h:mm a'
          } catch {}
          return undefined
        })()
        const fmt = (options as any)?.xDateFormat
          || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
            : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
            : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
            : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
            : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
            : inferredTimeFmt)
        if (!d || !fmt) return s
        const pad = (n: number) => String(n).padStart(2, '0')
        const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
        const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
        switch (fmt) {
          case 'YYYY': return String(d.getFullYear())
          case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
          case 'YYYY-[W]ww': {
            const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun')
            const jan1 = new Date(d.getFullYear(), 0, 1)
            const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000)
            const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1
            const wn = useSun ? wnSun : isoWeek(d)
            return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}`
          }
          case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
          case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
          case 'h:mm a': {
            let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}`
          }
          case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
          case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
          case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
          default: return formatDatePattern(d, String(fmt))
        }
      }
      // Pre-format labels to avoid index/value drift inside ECharts axis formatter callbacks
      // For multi-level X, use smart formatting that shows outer level only when it changes
      const xLabelsFmt = isMultiLevelX
        ? xLabels.map((v: any, idx: number) => formatMultiLevelLabel(v, idx, xLabels))
        : xLabels.map((v:any) => {
            try { return fmtXLabel(v) } catch { return String(v ?? '') }
          })
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] SeriesList built', { type, seriesCount: seriesList.length, firstSeries: seriesList[0], dataLength: seriesList[0]?.data?.length }) } catch {}
      }

      // Determine if a secondary axis is needed
      const hasSecondaryY = (type !== 'bar') && seriesList.some((s: any) => s.yAxisIndex === 1)
      const hasSecondaryXBar = (type === 'bar') && seriesList.some((s: any) => s.xAxisIndex === 1)
      
      // Calculate margins based on maximum Y values to prevent label cutoff
      // Using larger estimates and no upper limit to ensure labels fit
      const calculateMargins = (data: any[], categories?: string[], hasSecondary?: boolean): { left: number; right: number } => {
        try {
          const allValues: number[] = []
          if (categories && categories.length > 0) {
            // Multi-series: check all category values
            data.forEach((row: any) => {
              categories.forEach((cat) => {
                const val = Number(row?.[cat] ?? 0)
                if (!isNaN(val)) allValues.push(Math.abs(val))
              })
            })
          } else {
            // Single series: check value column
            data.forEach((row: any) => {
              const val = Number(row?.value ?? row?.[1] ?? 0)
              if (!isNaN(val)) allValues.push(Math.abs(val))
            })
          }
          if (allValues.length === 0) return { left: 60, right: hasSecondary ? 60 : 30 }
          
          const maxVal = Math.max(...allValues)
          const formatted = valueFormatter(maxVal)
          // More aggressive estimation: ~10px per character + 30px padding
          const estimatedWidth = Math.ceil((formatted.length * 10) + 30)
          // Higher minimum (60px)
          const margin = Math.max(estimatedWidth, 60)
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            console.log('[ChartCard] Margin calculation:', { maxVal, formatted, charCount: formatted.length, estimatedWidth, finalMargin: margin })
          }
          return { 
            left: margin,
            right: hasSecondary ? margin : 30
          }
        } catch {
          return { left: 60, right: hasSecondary ? 60 : 30 }
        }
      }
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        console.log('[ChartCard] About to calculate margins', { type, isMulti: (categories && categories.length > 0), categoriesCount: categories?.length, dataLength: data?.length, hasSecondaryY })
      }
      const margins = calculateMargins(data, categories, hasSecondaryY)
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        console.log('[ChartCard] Margins calculated:', margins)
      }

      // Delegate advanced Area to specialized renderer (refactor step)
      if (type === 'area') {
        return renderAdvancedAreaChart({
          chartInstanceKey,
          seriesList,
          xLabelsFmt,
          xRotate,
          xInterval,
          fontSize,
          advTooltip,
          axisTextColor,
          buildAxisGridAction: buildAxisGrid,
          options,
          gridTopPad,
          gridBottomPad,
          hasSecondaryY,
          visualMap,
          xLabelsRaw: (displayData as any[]).map((d) => d.x),
          onZoomAction: ({ startIndex, endIndex, startVal, endVal }: { startIndex: number; endIndex: number; startVal: any; endVal: any }) => {
            updateAdaptiveGroupingFromZoom(startVal, endVal)
          },
          onReadyAction: () => { try { markChartReady() } catch {} },
          noAnim: isSnap,
          echartsRef,
        })
      }

      const dzEnabled = !!(((options as any)?.zoomPan) || ((options as any)?.dataZoom))
      const dataZoom = dzEnabled
        ? ((type === 'bar')
            ? [
                { type: 'inside', yAxisIndex: 0, filterMode: 'filter', zoomOnMouseWheel: true, moveOnMouseWheel: true, moveOnMouseMove: true, throttle: 50 },
                { type: 'slider', yAxisIndex: 0, right: 0, width: 16, height: 120, handleSize: 10, showDetail: false, filterMode: 'filter' },
              ]
            : [
                { type: 'inside', xAxisIndex: 0, filterMode: 'filter', zoomOnMouseWheel: true, moveOnMouseWheel: true, moveOnMouseMove: true, throttle: 50 },
                { type: 'slider', xAxisIndex: 0, bottom: 6, height: 24, handleSize: 10, showDetail: false, filterMode: 'filter' },
              ])

        : undefined

      const option = {
        backgroundColor: 'rgba(0,0,0,0)',
        animation: !isSnap,
        animationDuration: isSnap ? 0 : 200,
        animationDurationUpdate: isSnap ? 0 : 300,
        tooltip: advTooltip,
        legend: { show: false },
        grid: { left: margins.left, right: margins.right, top: 10 + gridTopPad, bottom: 24 + gridBottomPad + (isMultiLevelX ? 16 : 0) + (dataZoom && type !== 'bar' ? 48 : 0), containLabel: false },
        xAxis: (type === 'bar')
          ? (hasSecondaryXBar
              ? [
                  { type: 'value', axisLabel: { fontSize: ((options as any)?.xAxisFontSize ?? fontSize), fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.xAxisFontColor || axisTextColor) }, ...(buildAxisGrid('x') as any) },
                  { type: 'value', axisLabel: { fontSize: ((options as any)?.xAxisFontSize ?? fontSize), fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.xAxisFontColor || axisTextColor) }, position: 'top', ...(buildAxisGrid('x') as any) },
                ]
              : { type: 'value', axisLabel: { fontSize: ((options as any)?.xAxisFontSize ?? fontSize), fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.xAxisFontColor || axisTextColor) }, ...(buildAxisGrid('x') as any) })
          : ((() => { 
              const gb = String(((querySpec as any)?.groupBy || 'none') as any).toLowerCase()
              const rawX = (querySpec as any)?.x
              const xFieldStrLocal = String(Array.isArray(rawX) ? rawX[0] : (rawX || ''))
              const isDerivedDatePart = /\s+\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/.test(xFieldStrLocal)
              // Detect 12-month seasonality (legend like "OrderDate (Year)", groupBy=month, x=OrderDate)
              const legendRawAxis = (() => { const lg: any = (querySpec as any)?.legend; if (Array.isArray(lg) && lg.length > 0) return String(lg[0] ?? ''); return (typeof lg === 'string') ? lg : '' })()
              const seasonalityModeAxis = (() => { try { const m = legendRawAxis.match(/^(.*)\s*\(Year\)$/i); if (m) { const base = m[1].trim(); return (gb === 'month' && base === xFieldStrLocal) } } catch {} return false })()
              // Use time-series axis only if groupBy is set, x-field is not a derived part, and NOT in seasonality mode
              const isTimeSeries = (gb && gb !== 'none') && !isDerivedDatePart && !seasonalityModeAxis
              // Multi-level X: add rich text styling for group labels
              const multiLevelRich = isMultiLevelX ? {
                groupLabel: { 
                  fontSize: Math.max(8, ((options as any)?.xAxisFontSize ?? fontSize) - 2), 
                  color: ((options as any)?.xAxisFontColor || axisTextColor),
                  fontWeight: 'bold' as const,
                  padding: [6, 0, 0, 0]
                }
              } : undefined
              const categoryAxisLabel = {
                rotate: xRotate,
                interval: (xInterval as any),
                fontSize: ((options as any)?.xAxisFontSize ?? fontSize),
                fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' as const : 'normal' as const,
                margin: isMultiLevelX ? 16 : (Math.abs(xRotate) >= 60 ? 12 : 8),
                color: ((options as any)?.xAxisFontColor || axisTextColor),
                ...(multiLevelRich ? { rich: multiLevelRich } : {})
              }
              return isTimeSeries 
                ? { type: 'time', axisLabel: { rotate: xRotate, fontSize: ((options as any)?.xAxisFontSize ?? fontSize), fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', margin: Math.abs(xRotate) >= 60 ? 12 : 8, color: ((options as any)?.xAxisFontColor || axisTextColor), formatter: (val: any) => { try { return fmtXLabel(val) } catch { return String(val ?? '') } } }, ...(buildAxisGrid('x') as any) } 
                : { type: 'category', data: xLabelsFmt, axisLabel: categoryAxisLabel, ...(buildAxisGrid('x') as any) } })()),
        yAxis: (type === 'bar')
          ? { type: 'category', data: xLabelsFmt, axisLabel: { 
              fontSize: ((options as any)?.yAxisFontSize ?? fontSize), 
              fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', 
              color: ((options as any)?.yAxisFontColor || axisTextColor),
              ...(isMultiLevelX ? { rich: { groupLabel: { fontSize: Math.max(8, ((options as any)?.yAxisFontSize ?? fontSize) - 2), color: ((options as any)?.yAxisFontColor || axisTextColor), fontWeight: 'bold', padding: [0, 6, 0, 0] } } } : {})
            }, ...(buildAxisGrid('y') as any) }
          : (hasSecondaryY
              ? [
                  { type: 'value', splitNumber: options?.yTickCount || undefined, axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) }, ...(buildAxisGrid('y') as any) },
                  { type: 'value', splitNumber: options?.yTickCount || undefined, axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) }, position: 'right', ...(buildAxisGrid('y') as any) },
                ]
              : { type: 'value', splitNumber: options?.yTickCount || undefined, axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) }, ...(buildAxisGrid('y') as any) }),
        series: (Array.isArray(seriesList) ? seriesList.map((s: any) => ({
          ...s,
          animation: !isSnap,
          animationDuration: isSnap ? 0 : (s?.animationDuration ?? 200),
          animationDurationUpdate: isSnap ? 0 : (s?.animationDurationUpdate ?? 300),
          progressive: isSnap ? 0 : (s?.progressive ?? undefined),
          progressiveThreshold: isSnap ? 0 : (s?.progressiveThreshold ?? undefined),
        })) : seriesList),
        ...(visualMap ? { visualMap } : {}),
        ...(dataZoom ? { dataZoom } : {}),
      }
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [EChartsDebug] mount-advanced-line-area-bar', { seriesCount: seriesList.length, xCount: xLabels.length }) } catch {}
      }
      return (
        <div className="absolute inset-0">
          <ReactECharts
            ref={echartsRef}
            key={chartInstanceKey}
            option={option}
            notMerge={true}
            lazyUpdate
            style={{ height: '100%' }}
            opts={isSnap ? ({ renderer: 'svg' } as any) : undefined}
            onChartReady={(ec:any) => { try { ec && ec.resize && ec.resize(); ec && ec.getZr && ec.getZr().refreshImmediately && ec.getZr().refreshImmediately(); } catch {} try { requestAnimationFrame(() => requestAnimationFrame(() => { markChartReady() })) } catch {} }}
            onEvents={(options as any)?.largeScale ? {
              dataZoom: (ev: any) => {
                try {
                  const xLabels = (displayData as any[]).map((d) => d.x)
                  const total = xLabels.length
                  const b = (Array.isArray(ev?.batch) && ev.batch.length ? ev.batch[0] : ev) || {}
                  let si = 0, ei = Math.max(0, total - 1)
                  if (typeof b.startValue !== 'undefined' || typeof b.endValue !== 'undefined') {
                    si = Math.max(0, Math.min(total - 1, Number(b.startValue ?? 0)))
                    ei = Math.max(0, Math.min(total - 1, Number(b.endValue ?? (total - 1))))
                  } else {
                    const sp = Math.max(0, Math.min(100, Number(b.start ?? 0)))
                    const ep = Math.max(0, Math.min(100, Number(b.end ?? 100)))
                    si = Math.round((sp / 100) * (total - 1))
                    ei = Math.round((ep / 100) * (total - 1))
                  }
                  const sv = xLabels[si]
                  const evv = xLabels[ei]
                  updateAdaptiveGroupingFromZoom(sv, evv)
                } catch {}
              },
              finished: () => { markChartReady() },
              rendered: () => { markChartReady() }
            } : { finished: () => { markChartReady() }, rendered: () => { markChartReady() }}}
          />
        </div>
      )
    }

    // Gantt (ECharts custom series)
    if (type === 'gantt') {
      return (
        <div className="absolute inset-0">
          <GanttCard
            title={title}
            datasourceId={datasourceId}
            options={options}
            queryMode={queryMode}
            querySpec={querySpec as any}
            widgetId={widgetId}
            pivot={pivot as any}
          />
        </div>
      )
    }

    // Tremor Bar List (Top-N categories by total)
    if (type === 'barList') {
      const rowsArr: any[] = Array.isArray(displayData) ? (displayData as any[]) : []
      const cats: string[] = Array.isArray(categories) ? (categories as string[]) : []
      const totals = cats.map((c) => ({
        name: String(c),
        value: rowsArr.reduce((s, r) => s + Number(((r as any)?.[c] ?? 0)), 0),
      }))
      totals.sort((a, b) => b.value - a.value)
      const topN = Math.max(1, Number(((options as any)?.barList?.topN ?? 8)))
      const list = totals.slice(0, topN)
      // Provide explicit Tremor colors so bars are visible in all themes
      const palette = getPresetPalette((options?.colorPreset || 'default') as any)
      const colored = list.map((it, i) => ({ ...it, name: legendDisplayName(String(it.name)), color: palette[i % palette.length] }))
      return (
        <div className="min-h-[160px]">
          <TremorBarList
            data={colored}
            valueFormatter={(n: number) => valueFormatter(Number(n))}
            className="mt-1"
          />
        </div>
      )
    }

    if (type === 'tremorTable') {
      const rowsArr: any[] = Array.isArray(displayData) ? (displayData as any[]) : []
      const cols = ['x', ...((isMulti ? (categories || []) : ['value']) as string[])]
      const tt = (options?.tremorTable || {}) as NonNullable<WidgetConfig['options']>['tremorTable']
      const badgeCols = new Set<string>(tt?.badgeColumns || [])
      const progressCols = new Set<string>(tt?.progressColumns || [])
      const colMax = computeColMax(rowsArr, (isMulti ? (categories || []) : ['value']) as string[])
      const xHeader = (querySpec as any)?.x || 'x'
      const fmtXCell = (raw: any): string => {
        const s = String(raw ?? '')
        const d = parseDateLoose(s)
        const fmt = (options as any)?.xDateFormat
          || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
            : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
            : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
            : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
            : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
            : undefined)
        if (d && fmt) {
          const pad = (n: number) => String(n).padStart(2, '0')
          const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
          const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
          switch (fmt) {
            case 'YYYY': return String(d.getFullYear())
            case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
            case 'YYYY-[W]ww': { const jan1 = new Date(d.getFullYear(), 0, 1); const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000); const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1; const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || env.weekStart) === 'sun'); const wn = useSun ? wnSun : isoWeek(d); return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}` }
            case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
            case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
            case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
            case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
            case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
            case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
            default: return formatDatePattern(d, String(fmt))
          }
        }
        const mode = (options as any)?.xLabelCase as 'lowercase'|'capitalize'|'capitalcase'|'proper'|undefined
        if (!mode) return s
        switch (mode) {
          case 'lowercase': return s.toLowerCase()
          case 'capitalize':
          case 'capitalcase': return s.toUpperCase()
          case 'proper': default: return s.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
        }
      }
      return (
        <div className="min-h-[160px] overflow-auto">
          <TremorTable className="w-full text-[12px]">
            <TremorTableHead>
              <TremorTableRow>
                {cols.map((c) => (
                  <TremorTableHeaderCell key={String(c)} className="whitespace-nowrap">{formatHeader(String(c) === 'x' ? String(xHeader) : String(c))}</TremorTableHeaderCell>
                ))}
              </TremorTableRow>
            </TremorTableHead>
            <TremorTableBody>
              {rowsArr.map((r: any, i: number) => (
                <TremorTableRow
                  key={i}
                  className={(tt?.alternatingRows !== false) ? 'odd:bg-[hsl(var(--secondary))]/40' : ''}
                  onClick={() => {
                    if (tt?.rowClick?.type === 'emit' && typeof window !== 'undefined') {
                      try {
                        const ev = tt?.rowClick?.eventName || 'tremor-table-click'
                        window.dispatchEvent(new CustomEvent(ev, { detail: { widgetId, row: r } } as any))
                      } catch {}
                    }
                  }}
                >
                  {cols.map((c, ci) => {
                    const key = String(c)
                    const isX = key === 'x'
                    const raw = isX ? r.x : (isMulti ? r[key] : r.value)
                    const v = Number(raw)
                    const isNum = Number.isFinite(v)
                    const colFmt = (tt?.formatByColumn || {})[key] as any
                    const formatForCol = (num: number) => (colFmt && colFmt !== 'none') ? formatNumber(num, colFmt) : valueFormatter(num)
                    const colorKey = chartColorsTokens[(ci - 1 + chartColorsTokens.length) % chartColorsTokens.length]
                    if (!isX && progressCols.has(key) && isNum) {
                      const max = colMax[key] || 100
                      const pct = max > 0 ? Math.min(100, Math.max(0, (v / max) * 100)) : 0
                      return (
                        <TremorTableCell key={ci}>
                          <div className="flex items-center gap-2">
                            <TremorProgressBar value={pct} color={colorKey} className="w-28" />
                            <span className="text-xs text-muted-foreground">{pct.toFixed(1)}%</span>
                          </div>
                        </TremorTableCell>
                      )
                    }
                    if (!isX && badgeCols.has(key)) {
                      return (
                        <TremorTableCell key={ci}>
                          <TremorBadge color={colorKey}>{isNum ? formatForCol(v) : String(raw)}</TremorBadge>
                        </TremorTableCell>
                      )
                    }
                    return (
                      <TremorTableCell key={ci} className="whitespace-nowrap">
                        {isX ? fmtXCell(raw) : (isNum ? formatForCol(v) : String(raw))}
                      </TremorTableCell>
                    )
                  })}
                </TremorTableRow>
              ))}
              {tt?.showTotalRow && (
                <TremorTableRow>
                  {cols.map((c, ci) => {
                    const key = String(c)
                    if (ci === 0) return <TremorTableCell key={ci} className="font-medium">Total</TremorTableCell>
                    const sum = rowsArr.reduce((acc, r) => acc + Number((isMulti ? (r as any)[key] : (r as any).value) ?? 0), 0)
                    const colFmt = (tt?.formatByColumn || {})[key] as any
                    const formatted = (colFmt && colFmt !== 'none') ? formatNumber(sum, colFmt) : valueFormatter(sum)
                    return <TremorTableCell key={ci} className="font-medium">{formatted}</TremorTableCell>
                  })}
                </TremorTableRow>
              )}
            </TremorTableBody>
          </TremorTable>
        </div>
      )
    }

    // ECharts scatter plot
    if (type === 'scatter') {
      const rowsArr: any[] = Array.isArray(data) ? (data as any[]) : []
      // Per-series metadata by label (for secondary axis toggle)
      const metaByName = new Map<string, any>()
      ;(Array.isArray(series) ? series : []).forEach((s: any, i: number) => {
        const label = s?.label || s?.y || s?.measure || `series_${i + 1}`
        metaByName.set(String(label), s)
      })
      const xVals = rowsArr.map((r: any) => (r as any)?.x)
      const isTime = xVals.some((x) => (x instanceof Date) || (!!parseDateLoose(x)))
      const isNumericX = xVals.every((x) => typeof x === 'number' || !isNaN(Number(x)))
      const fontSize = Math.max(8, Number(((options as any)?.xAxisFontSize ?? (options as any)?.yAxisFontSize ?? 11)))
      const xRotate = Math.max(-90, Math.min(90, Number((options as any)?.xTickAngle ?? 0)))
      // Option B — normalize bubble radius by the maximum value
      const sizeField = (options as any)?.sizeField || (options as any)?.scatterSizeField
      const sizeMin = Number(((options as any)?.sizeRange?.[0] ?? 6))
      const sizeMax = Number(((options as any)?.sizeRange?.[1] ?? 28))
      // Compute global max for the chosen size source (field, per-series y, or single-series value)
      let maxSize = 0
      if (sizeField) {
        for (const r of rowsArr) {
          const v = Number((r as any)?.[String(sizeField)] ?? 0)
          if (Number.isFinite(v)) maxSize = Math.max(maxSize, v)
        }
      } else if ((isMulti && Array.isArray(categories) && (categories as any[]).length > 0)) {
        for (const r of rowsArr) {
          for (const c of (categories as any[])) {
            const v = Number((r as any)?.[String(c)] ?? 0)
            if (Number.isFinite(v)) maxSize = Math.max(maxSize, v)
          }
        }
      } else {
        for (const r of rowsArr) {
          const v = Number((r as any)?.value ?? 0)
          if (Number.isFinite(v)) maxSize = Math.max(maxSize, v)
        }
      }
      if (!Number.isFinite(maxSize) || maxSize <= 0) maxSize = 1
      const mkSeries = (label: string, idx: number, accessor: (r: any) => number) => {
        const pts = rowsArr.map((r: any, i: number) => {
          const xv = (r as any).x
          const xvNum = isTime ? (new Date(xv)).getTime() : (isNumericX ? Number(xv) : i)
          const yv = accessor(r)
          const yNum = Number.isFinite(Number(yv)) ? Number(yv) : 0
          // Derive size source: explicit sizeField or fallback to this series' y value
          const sizeRaw = sizeField ? Number((r as any)?.[String(sizeField)] ?? 0) : yNum
          const ratio = Math.max(0, Math.min(1, sizeRaw / (maxSize || 1)))
          const radius = sizeMin + (sizeMax - sizeMin) * ratio
          return { value: [xvNum, yNum], symbolSize: radius }
        })
        const sMeta = metaByName.get(String(label))
        return { name: label, type: 'scatter', data: pts, ...(sMeta?.secondaryAxis ? { yAxisIndex: 1 } : {}) }
      }
      const seriesList = (isMulti && (categories || []).length > 0)
        ? (categories || []).map((c, i) => mkSeries(String(c), i, (r) => Number((r as any)[String(c)] ?? 0)))
        : [mkSeries('value', 0, (r) => Number((r as any).value ?? 0))]
      const hasSecondaryY = seriesList.some((s:any) => s.yAxisIndex === 1)
      const dzEnabled = !!(((options as any)?.zoomPan) || ((options as any)?.dataZoom))
      const dataZoom = dzEnabled ? [
        { type: 'inside', xAxisIndex: 0, filterMode: 'filter', zoomOnMouseWheel: true, moveOnMouseWheel: true, moveOnMouseMove: true, throttle: 50 },
        { type: 'slider', xAxisIndex: 0, bottom: 10, height: 26, handleSize: 12, showDetail: false, filterMode: 'filter' },
      ] : undefined
      // Calculate margins for Y-axis labels (single series)
      const marginsSingle = (() => {
        try {
          const allValues = (data as any[]).map((row: any) => Math.abs(Number(row?.value ?? row?.[1] ?? 0))).filter((v: number) => !isNaN(v))
          if (allValues.length === 0) return { left: 60, right: hasSecondaryY ? 60 : 30 }
          const maxVal = Math.max(...allValues)
          const formatted = valueFormatter(maxVal)
          const estimatedWidth = Math.ceil((formatted.length * 10) + 30)
          const margin = Math.max(estimatedWidth, 60)
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            console.log('[ChartCard] [Single] Margin calculation:', { maxVal, formatted, charCount: formatted.length, estimatedWidth, finalMargin: margin })
          }
          return { left: margin, right: hasSecondaryY ? margin : 30 }
        } catch {
          return { left: 60, right: hasSecondaryY ? 60 : 30 }
        }
      })()
      const option = {
        tooltip: { show: (Array.isArray(data) && data.length > 0), trigger: 'item', backgroundColor: 'transparent', borderColor: 'transparent', textStyle: { color: undefined as any }, extraCssText: 'border:1px solid hsl(var(--border));background:hsl(var(--card));color:hsl(var(--foreground));padding:4px 8px;border-radius:6px;box-shadow:none;',
          formatter: (p: any) => {
            if (typeof window !== 'undefined') {
              try { console.log('[ChartCard] [Tooltip] ITEM formatter triggered', { p, isTime, querySpec }) } catch {}
            }
            try {
              // 1) X label (formatted) and index resolution
              const xLabel = (() => {
                if (isTime) {
                  const xv = Number(p?.value?.[0] ?? 0)
                  const d = new Date(xv)
                  const fmt = (options as any)?.xDateFormat
                    || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
                      : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
                      : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
                      : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
                      : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
                      : undefined)
                  if (!isNaN(d.getTime()) && fmt) {
                    const pad = (n: number) => String(n).padStart(2, '0')
                    const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
                    const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
                    switch (fmt) {
                      case 'YYYY': return String(d.getFullYear())
                      case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
                      case 'YYYY-[W]ww': { const jan1 = new Date(d.getFullYear(), 0, 1); const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000); const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1; const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || env.weekStart) === 'sun'); const wn = useSun ? wnSun : isoWeek(d); return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}` }
                      case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
                      case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
                      case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
                      case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
                      case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
                      case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
                      default: return formatDatePattern(d, String(fmt))
                    }
                  }
                  return d.toLocaleString()
                }
                if (!isNumericX) {
                  const ii = Number(p?.dataIndex ?? 0)
                  const raw = xVals[ii]
                  const s = String(raw ?? '')
                  const d = parseDateLoose(s)
                  const fmt = (options as any)?.xDateFormat
                    || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
                      : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
                      : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
                      : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
                      : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
                      : undefined)
                  if (d && fmt) {
                    const pad = (n: number) => String(n).padStart(2, '0')
                    const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
                    const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
                    switch (fmt) {
                      case 'YYYY': return String(d.getFullYear())
                      case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
                      case 'YYYY-[W]ww': { const jan1 = new Date(d.getFullYear(), 0, 1); const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000); const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1; const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun'); const wn = useSun ? wnSun : isoWeek(d); return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}` }
                      case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
                      case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
                      case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
                      case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
                      case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
                      case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
                      default: return formatDatePattern(d, String(fmt))
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
                const xv = Number(p?.value?.[0] ?? 0)
                return Number.isFinite(xv) ? xv.toLocaleString() : String(xv)
              })()

              // Resolve data index for prev computations
              const resolveIndex = (): number => {
                if (!Array.isArray(xVals)) return -1
                if (isTime) {
                  const target = Number(p?.value?.[0] ?? 0)
                  const arr = xVals.map((x:any)=>{ try { return (new Date(x)).getTime() } catch { return NaN } })
                  return arr.indexOf(target)
                }
                if (!isNumericX) return Number(p?.dataIndex ?? 0)
                const target = Number(p?.value?.[0] ?? 0)
                const arr = xVals.map((x:any)=>Number(x))
                return arr.indexOf(target)
              }
              const idx = resolveIndex()
              const rawX = idx >= 0 && idx < (data as any[]).length ? ((data as any[])[idx] as any)?.x : undefined

              const rawName = String(p?.seriesName)
              if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
                const dataRow = idx >= 0 && idx < (data as any[]).length ? (data as any[])[idx] : null
                console.debug('[ChartCard] Tooltip value debug:', { 
                  pValue: p?.value, 
                  pValue0: p?.value?.[0],
                  pValue1: p?.value?.[1],
                  pData: p?.data,
                  seriesName: rawName,
                  idx,
                  dataRow,
                  dataRowKeys: dataRow ? Object.keys(dataRow) : [],
                  hasCategories: Array.isArray(categories) && (categories as any[]).length > 0,
                  categories: categories,
                  valueFromDataRow: dataRow ? dataRow[rawName] : 'N/A',
                  valueFromDataRowValue: dataRow ? dataRow.value : 'N/A'
                })
              }
              // Extract current value: try from data array first (matches prev logic), then from p.value
              const vFromData = idx >= 0 && idx < (data as any[]).length
                ? (Array.isArray(categories) && (categories as any[]).length > 0
                    ? Number(((data as any[])[idx] as any)?.[rawName] ?? 0)
                    : Number(((data as any[])[idx] as any)?.value ?? 0))
                : 0
              const vFromParam = Number(p?.value?.[1] ?? p?.value ?? 0)
              const v = vFromData || vFromParam
              if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
                console.debug('[ChartCard] Extracted values:', { vFromData, vFromParam, finalV: v })
              }
              const xSeq = xVals
              const prevIdx = resolvePrevIndex(idx, xSeq)
              const hasPrev = prevIdx >= 0
              const prev = hasPrev
                ? (Array.isArray(categories) && (categories as any[]).length > 0
                    ? Number(((data as any[])[prevIdx] as any)?.[rawName] ?? 0)
                    : Number(((data as any[])[prevIdx] as any)?.value ?? 0))
                : 0

              // Share: group by base series if virtualized; else sum row at idx
              const vmeta = (((q?.data as any)?.virtualMeta) || {}) as Record<string, { baseSeriesLabel: string; agg?: string }>
              const base = vmeta[rawName]?.baseSeriesLabel
              let tot = 0
              if (base && Array.isArray(categories) && (categories as any[]).length > 0) {
                ;(categories as any[]).forEach((c:any) => {
                  if (vmeta[String(c)]?.baseSeriesLabel === base) tot += Number(((data as any[])[idx] as any)?.[String(c)] ?? 0)
                })
              } else if (Array.isArray(categories) && (categories as any[]).length > 0) {
                ;(categories as any[]).forEach((c:any) => { tot += Number(((data as any[])[idx] as any)?.[String(c)] ?? 0) })
              } else {
                tot = Number(((data as any[])[idx] as any)?.value ?? v)
              }
              const shareStr = tot > 0 ? `${((v / tot) * 100).toFixed(1)}%` : '0.0%'

              // Change/Delta
              const changePct = computeChangePercent(v, prev)
              const changeStr = hasPrev ? `${changePct >= 0 ? '+' : '-'}${Math.abs(changePct).toFixed(1)}%` : ''
              const invert = !!(options as any)?.downIsGood
              const changeColor = invert
                ? ((changePct < 0) ? '#22c55e' : (changePct > 10 ? '#ef4444' : '#9CA3AF'))
                : ((changePct < 0) ? '#ef4444' : (changePct > 10 ? '#22c55e' : '#9CA3AF'))
              const deltaStr = hasPrev ? `${(v - prev) >= 0 ? '+' : '-'}${valueFormatter(Math.abs(v - prev))}` : ''

              // Prev label derived from effective delta mode (ignore immediate previous neighbor)
              const prevLabel = (() => {
                try {
                  if (!effectiveDeltaMode) return undefined
                  const s = String(rawX ?? '')
                  const d = parseDateLoose(s)
                  if (!d) return undefined
                  const pv = (() => {
                    switch (effectiveDeltaMode as any) {
                      case 'TD_YSTD': { const x = new Date(d); x.setDate(x.getDate()-1); return x }
                      case 'TW_LW': { const x = new Date(d); x.setDate(x.getDate()-7); return x }
                      case 'MONTH_LMONTH':
                      case 'MTD_LMTD': { const x = new Date(d); x.setMonth(x.getMonth()-1); return x }
                      case 'TQ_LQ': { const x = new Date(d); x.setMonth(x.getMonth()-3); return x }
                      case 'TY_LY':
                      case 'YTD_LYTD':
                      case 'Q_TY_VS_Q_LY':
                      case 'QTD_TY_VS_QTD_LY':
                      case 'M_TY_VS_M_LY':
                      case 'MTD_TY_VS_MTD_LY': { const x = new Date(d); x.setFullYear(x.getFullYear()-1); return x }
                      default: return null
                    }
                  })()
                  if (!pv) return undefined
                  const pad = (n:number)=>String(n).padStart(2,'0')
                  const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
                  if (gb === 'year') return String(pv.getFullYear())
                  if (gb === 'quarter') { const q = Math.floor(pv.getMonth()/3)+1; return `${pv.getFullYear()}-Q${q}` }
                  if (gb === 'month') return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}`
                  if (gb === 'week') {
                    const date = new Date(Date.UTC(pv.getFullYear(), pv.getMonth(), pv.getDate()))
                    const ws = (deltaWeekStart || 'mon')
                    if (ws === 'sun') {
                      const onejan = new Date(pv.getFullYear(),0,1)
                      const week = Math.ceil((((pv as any)- (onejan as any))/86400000 + onejan.getDay()+1)/7)
                      return `${pv.getFullYear()}-W${pad(week)}`
                    } else {
                      const dayNr = (date.getUTCDay() + 6) % 7
                      date.setUTCDate(date.getUTCDate() - dayNr + 3)
                      const firstThursday = new Date(Date.UTC(date.getUTCFullYear(),0,4))
                      const week = 1 + Math.round(((date.getTime() - firstThursday.getTime())/86400000 - 3) / 7)
                      return `${date.getUTCFullYear()}-W${pad(week)}`
                    }
                  }
                  return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}-${pad(pv.getDate())}`
                } catch { return undefined }
              })()

              // Build row
              const aggLabel = (() => {
                const fromMeta = (vmeta[rawName]?.agg as any)
                if (fromMeta) return toProperCase(String(fromMeta))
                try {
                  const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
                  if (seriesDefs.length > 0) {
                    const base = String(rawName).split(' • ')[0].trim()
                    for (let i = 0; i < seriesDefs.length; i++) {
                      const s = seriesDefs[i]
                      const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
                      if (String(lab).trim() === base) {
                        const ag = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
                        return toProperCase(String(ag))
                      }
                    }
                  }
                } catch {}
                const fallback = (querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')
                return toProperCase(String(fallback))
              })()
              const name = (typeof formatHeader === 'function') ? formatHeader(rawName) : rawName
              const rows: TooltipRow[] = [{
                color: String(p?.color || '#94a3b8'),
                name,
                valueStr: valueFormatter(v),
                shareStr,
                aggLabel,
                changeStr,
                deltaStr,
                prevStr: hasPrev ? valueFormatter(prev) : '',
                changeColor,
              }]
              return ReactDOMServer.renderToString(<TooltipTable header={xLabel} prevLabel={prevLabel} rows={rows} showDeltas={!!effectiveDeltaMode} />)
            } catch { return '' }
          }
        },
        legend: { show: false },
        grid: { left: marginsSingle.left, right: marginsSingle.right, top: 10, bottom: 24 + (dataZoom ? 48 : 0), containLabel: false },
        xAxis: isTime
          ? { type: 'time', axisLabel: { fontSize: ((options as any)?.xAxisFontSize ?? fontSize), fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.xAxisFontColor || axisTextColor), formatter: (val: any) => {
                const s = String(val ?? '')
                const d = parseDateLoose(s)
                const fmt = (options as any)?.xDateFormat
                  || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
                    : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
                    : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
                    : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
                    : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
                    : undefined)
                if (d && fmt) {
                  const pad = (n: number) => String(n).padStart(2, '0')
                  const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
                  const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
                  switch (fmt) {
                    case 'YYYY': return String(d.getFullYear())
                    case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
                    case 'YYYY-[W]ww': { const jan1 = new Date(d.getFullYear(), 0, 1); const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000); const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1; const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun'); const wn = useSun ? wnSun : isoWeek(d); return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}` }
                    case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
                    case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
                    case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
                    case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
                    case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
                    case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
                    default: return formatDatePattern(d, String(fmt))
                  }
                }
                const mode = (options as any)?.xLabelCase as 'lowercase'|'capitalize'|'proper'|undefined
                const str = s
                if (!mode) return str
                switch (mode) {
                  case 'lowercase': return str.toLowerCase()
                  case 'capitalize': { const lower = str.toLowerCase(); return lower.length ? (lower[0].toUpperCase()+lower.slice(1)) : lower }
                  case 'proper': default: return str.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
                }
              } } }
          : (isNumericX
              ? { type: 'value', axisLabel: { fontSize: ((options as any)?.xAxisFontSize ?? fontSize), fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.xAxisFontColor || axisTextColor) } }
              : { type: 'category', data: xVals.map((x, i) => String(x ?? i)), axisLabel: { rotate: xRotate, margin: Math.abs(xRotate) >= 60 ? 12 : 8, fontSize: ((options as any)?.xAxisFontSize ?? fontSize), fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.xAxisFontColor || axisTextColor), formatter: (_val: any, i: number) => {
                  const s = String((xVals?.[i] ?? _val) ?? '')
                  const d = parseDateLoose(s)
                  const fmt = (options as any)?.xDateFormat
                    || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
                      : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
                      : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
                      : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
                      : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
                      : undefined)
                  if (d && fmt) {
                    const pad = (n: number) => String(n).padStart(2, '0')
                    const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
                    const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
                    switch (fmt) {
                      case 'YYYY': return String(d.getFullYear())
                      case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
                      case 'YYYY-[W]ww': { const jan1 = new Date(d.getFullYear(), 0, 1); const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000); const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1; const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun'); const wn = useSun ? wnSun : isoWeek(d); return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}` }
                      case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
                      case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
                      case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
                      case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
                      case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
                      case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
                      default: return formatDatePattern(d, String(fmt))
                    }
                  }
                  const mode = (options as any)?.xLabelCase as 'lowercase'|'capitalize'|'proper'|undefined
                  const str = s
                  if (!mode) return str
                  switch (mode) {
                    case 'lowercase': return str.toLowerCase()
                    case 'capitalize': { const lower = str.toLowerCase(); return lower.length ? (lower[0].toUpperCase()+lower.slice(1)) : lower }
                    case 'proper': default: return str.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
                  }
                } } }
            ),
        yAxis: hasSecondaryY
          ? [
              { type: 'value', axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) } },
              { type: 'value', axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) }, position: 'right' },
            ]
          : { type: 'value', axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) } },
        series: seriesList,
        ...(dataZoom ? { dataZoom } : {}),
      }
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[ChartCard] [EChartsDebug] mount-scatter', { seriesCount: seriesList.length }) } catch {}
      }
      return (
        <div className="absolute inset-0" style={{ paddingBottom: dataZoom ? 48 : 0 }}>
          <ReactECharts
            ref={echartsRef}
            key={chartInstanceKey}
            option={option}
            notMerge={true}
            lazyUpdate
            style={{ height: '100%' }}
            onChartReady={(ec:any) => { try { ec && ec.resize && ec.resize() } catch {} try { requestAnimationFrame(() => requestAnimationFrame(() => { markChartReady() })) } catch {} }}
            onEvents={((options as any)?.largeScale && isTime) ? {
              dataZoom: (ev: any) => {
                try {
                  const total = xVals.length
                  const b = (Array.isArray(ev?.batch) && ev.batch.length ? ev.batch[0] : ev) || {}
                  let si = 0, ei = Math.max(0, total - 1)
                  if (typeof b.startValue !== 'undefined' || typeof b.endValue !== 'undefined') {
                    si = Math.max(0, Math.min(total - 1, Number(b.startValue ?? 0)))
                    ei = Math.max(0, Math.min(total - 1, Number(b.endValue ?? (total - 1))))
                  } else {
                    const sp = Math.max(0, Math.min(100, Number(b.start ?? 0)))
                    const ep = Math.max(0, Math.min(100, Number(b.end ?? 100)))
                    si = Math.round((sp / 100) * (total - 1))
                    ei = Math.round((ep / 100) * (total - 1))
                  }
                  const sv = xVals[si]
                  const evv = xVals[ei]
                  updateAdaptiveGroupingFromZoom(sv, evv)
                } catch {}
              },
              finished: () => { markChartReady() }
            } : { finished: () => { markChartReady() }}}
          />
        </div>
      )
    }

    if (type === 'categoryBar') {
      const rowsArr: any[] = Array.isArray(data) ? (data as any[]) : []
      const cats = (categories || []) as string[]
      const ctx = buildTimelineContext(rowsArr, cats, { yMax: (options?.yMax as any) })
      const labelsUsed = ctx.labels
      const rowsAligned = labelsUsed.map((l) => ctx.rowsByLabel[l])
      const byCat = aggregateCategoriesAdvanced(rowsAligned, cats, 'sum')
      const sums: number[] = cats.map((c) => Number(byCat[c] || 0))
      const totalAgg = sums.reduce((a, b) => a + Number(b || 0), 0)
      const lastRow: any = rowsAligned.length > 0 ? rowsAligned[rowsAligned.length - 1] : undefined
      const prevRow: any = rowsAligned.length > 1 ? rowsAligned[rowsAligned.length - 2] : undefined
      return (
        <div className="relative min-h-[60px] flex items-center">
          {(() => {
            const safeValues = Array.isArray(sums) ? sums.map((n: any) => (Number.isFinite(Number(n)) ? Number(n) : 0)) : []
            const safeColors = Array.isArray(chartColorsTokens) ? chartColorsTokens : []
            const nonZero = safeValues.some((v: number) => v > 0)
            if (safeValues.length === 0 || safeColors.length === 0 || !nonZero) return null
            return (<TremorCategoryBar values={safeValues} colors={safeColors} className="w-full" />)
          })()}
          {options?.dataLabelsShow && totalAgg > 0 && (
            <div className="absolute inset-0 flex">
              {(categories || []).map((c, i) => {
                const vAgg = Number(sums[i] || 0)
                const pct = totalAgg > 0 ? (vAgg / totalAgg) : 0
                if (vAgg === 0 || pct === 0) return <div key={String(c)} style={{ flex: 0, flexBasis: 0 }} />
                const prev = Number((prevRow as any)?.[c] ?? 0)
                const v = Number((lastRow as any)?.[c] ?? 0)
                const delta = v - prev
                const tip = [
                  `${String(c)}: ${valueFormatter(vAgg)}`,
                  options?.tooltipShowPercent ? `Share: ${(pct * 100).toFixed(1)}%` : '',
                  options?.tooltipShowDelta && (Array.isArray(displayData) && (displayData as any[]).length > 1) ? `Δ: ${delta>=0?'+':''}${delta.toLocaleString()}` : ''
                ].filter(Boolean).join(' · ')
                return (
                  <div key={String(c)} style={{ flexGrow: vAgg, flexBasis: 0 }} className="group relative flex items-center justify-center">
                    <span className="text-[10px] font-medium bg-card/70 px-1 rounded pointer-events-none" style={{ color: axisTextColor as any, textShadow: '0 1px 2px rgba(0,0,0,0.6)' }}>
                      {valueFormatter(vAgg)}
                    </span>
                    <div className="pointer-events-none absolute -top-6 left-1/2 -translate-x-1/2 whitespace-nowrap px-2 py-1 rounded-md border bg-card text-xs opacity-0 group-hover:opacity-100 transition-opacity">
                      {tip}
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      )
    }
       if (type === 'progress') {
      const rowsArr = Array.isArray(displayData) ? (displayData as any[]) : []
      const cats = (categories || []) as string[]
      const ctx = buildTimelineContext(rowsArr, cats, { yMax: (options?.yMax as any) })
      const labelsUsed = ctx.labels
      const rowsAligned = labelsUsed.map((l) => ctx.rowsByLabel[l])
      const lastRow: any = rowsAligned.length > 0 ? rowsAligned[rowsAligned.length - 1] : undefined
      const prevRow: any = rowsAligned.length > 1 ? rowsAligned[rowsAligned.length - 2] : undefined
      const totalLast = (cats || []).reduce((acc, c) => acc + Number((lastRow?.[c]) ?? 0), 0)

      const labelPos = (options?.dataLabelPosition || 'outsideEnd') as 'outsideEnd' | 'insideEnd' | 'center'
      const showLabels = options?.dataLabelsShow !== false
      const labelClass =
        labelPos === 'center'
          ? 'justify-center px-2 gap-3'
          : labelPos === 'insideEnd'
          ? 'justify-end pr-2 gap-3'
          : 'justify-between px-2'

      const applyCase = (label: string) => {
        const mode = (options as any)?.legendLabelCase as ('lowercase' | 'capitalize' | 'proper' | undefined)
        const str = String(label ?? '')
        if (!mode) return str
        switch (mode) {
          case 'lowercase':
            return str.toLowerCase()
          case 'capitalize': {
            const lower = str.toLowerCase()
            return lower.length ? lower[0].toUpperCase() + lower.slice(1) : lower
          }
          case 'proper':
          default:
            return str
              .replace(/[_-]+/g, ' ')
              .split(/\s+/)
              .map((w) => (w ? w[0].toUpperCase() + w.slice(1).toLowerCase() : w))
              .join(' ')
        }
      }

      const wantVG = ((options as any)?.colorMode === 'valueGradient')
      const baseKey = ((options as any)?.colorBaseKey || chartColorsTokens[0] || 'blue')
      const baseHex = tremorNameToHex(baseKey as any)
      return (
        <div className="space-y-3">
          {cats.map((c, i) => {
            const current = Number((lastRow?.[c]) ?? 0)
            const prev = Number((prevRow?.[c]) ?? 0)
            const pct = Math.max(0, Math.min(100, totalLast > 0 ? (current / totalLast) * 100 : 0))
            const deltaPct =
              options?.tooltipShowDelta && Math.abs(prev) > 0 ? ((current - prev) / Math.abs(prev)) * 100 : null
            const title = [
              `${applyCase(c)}: ${valueFormatter(current)}`,
              options?.tooltipShowPercent ? `Percent: ${pct.toFixed(1)}%` : '',
              options?.tooltipShowDelta && rowsAligned.length > 1
                ? `Δ: ${(current - prev) >= 0 ? '+' : ''}${(current - prev).toLocaleString()} (${Math.abs(prev) > 0 ? (
                    ((current - prev) / Math.abs(prev)) * 100
                  ).toFixed(1) : '0.0'}%)`
                : '',
            ]
              .filter(Boolean)
              .join(' · ')
            const color = chartColorsTokens[i % chartColorsTokens.length]

            return (
              <div key={String(c)} className="h-[60px] relative flex items-center group">
                {wantVG ? (
                  <HexProgressBar
                    value={current}
                    total={Math.max(totalLast, current, 1)}
                    color={saturateHexBy(baseHex, Math.max(0, Math.min(1, totalLast > 0 ? (current / totalLast) : 0)))}
                    className="w-full"
                  />
                ) : (
                  <TremorProgressBar
                    value={current}
                    total={Math.max(totalLast, current, 1)}
                    color={color}
                    className="w-full"
                  />
                )}
                {showLabels ? (
                  <div className={`absolute inset-0 flex items-center text-[11px] font-medium ${labelClass}`}>
                    <span className="text-muted-foreground">{applyCase(c)}</span>
                    <span className="flex items-center gap-2 text-foreground">
                      {valueFormatter(current)}
                      {options?.tooltipShowPercent && (
                        <span className="text-muted-foreground">({pct.toFixed(1)}%)</span>
                      )}
                      {options?.tooltipShowDelta && rowsAligned.length > 1 && (
                        <span
                          className={`${
                            deltaPct != null
                              ? deltaPct > 0
                                ? 'text-emerald-600'
                                : deltaPct < 0
                                ? 'text-rose-600'
                                : 'text-gray-500'
                              : ''
                          }`}
                        >
                          {deltaPct != null && (deltaPct >= 0 ? '+' : '')}
                          {(deltaPct ?? 0).toFixed(1)}%
                        </span>
                      )}
                    </span>
                  </div>
                ) : (
                  <div className="absolute left-2 top-1 text-[11px] font-medium text-muted-foreground">
                    {applyCase(c)} · {valueFormatter(current)}
                  </div>
                )}
                <div className="pointer-events-none absolute -top-6 left-1/2 -translate-x-1/2 whitespace-nowrap px-2 py-1 rounded-md border bg-card text-xs opacity-0 group-hover:opacity-100 transition-opacity">
                  {title}
                </div>
              </div>
            )
          })}
        </div>
      )
    }
    if (type === 'badges') {
      const preset = (options?.badgesPreset || 'badge1') as 'badge1'|'badge2'|'badge3'|'badge4'|'badge5'
      // Build items per category: numeric sum or last non-empty string
      const rowsArr: any[] = Array.isArray(data) ? (data as any[]) : []
      const cats = (categories || []) as string[]
      const ctx = buildTimelineContext(rowsArr as any[], cats, { yMax: (options?.yMax as any), trackerMaxPills: (options as any)?.trackerMaxPills })
      const labelsUsed = ctx.labels
      const rowsAligned = labelsUsed.map((l) => ctx.rowsByLabel[l])
      const aggMode = (((options as any)?.kpi?.aggregationMode) || 'count') as any
      const sumsByCat = aggregateCategoriesAdvanced(rowsAligned, cats, aggMode)
      const lastAlignedRow: any = rowsAligned.length > 0 ? rowsAligned[rowsAligned.length - 1] : undefined
      const prevAlignedRow: any = rowsAligned.length > 1 ? rowsAligned[rowsAligned.length - 2] : undefined
      const items = (categories || []).map((c) => {
        const vals = rowsArr.map((r) => (r?.[c]))
        const nums = vals.map((v) => Number(v)).filter((v) => Number.isFinite(v))
        const isNumeric = nums.length > 0 && vals.filter((v) => v !== undefined && v !== null).every((v) => Number.isFinite(Number(v)))
        let value: number | string = 0
        if (isNumeric) {
          value = Number(sumsByCat[String(c)] ?? 0)
        } else {
          const rev = [...vals].reverse()
          const str = rev.find((v) => v !== null && v !== undefined && String(v).trim() !== '')
          value = (str === undefined ? '' : String(str))
        }
        return { label: String(c), value, isNumeric }
      })
      const total = items.reduce((a, b) => a + (typeof b.value === 'number' ? Number(b.value || 0) : 0), 0)

      return (
        <div className="flex flex-wrap justify-center gap-4">
          {items.map((it, i) => {
            const share = (it.isNumeric && total > 0) ? ((toNum(it.value) / total) * 100).toFixed(1) : null
            // Prefer server-side period delta when configured; fallback to last-row delta
            const sdRaw = (deltaQ.data?.deltas && it.isNumeric) ? ((deltaQ.data.deltas as any as Record<string, number>)[String(it.label)] as any) : undefined
            const serverDelta = Number.isFinite(Number(sdRaw)) ? Number(sdRaw) : null
            const cur = it.isNumeric ? toNum((lastAlignedRow as any)?.[String(it.label)], NaN) : NaN
            const prev = it.isNumeric ? toNum((prevAlignedRow as any)?.[String(it.label)], NaN) : NaN
            const fallbackDelta = (Number.isFinite(cur) && Number.isFinite(prev)) ? (cur - prev) : null
            const delta = (serverDelta !== null) ? serverDelta : fallbackDelta
            const dir: 'up'|'down'|'flat' = (delta == null) ? 'flat' : (delta > 0 ? 'up' : (delta < 0 ? 'down' : 'flat'))
            const deltaPct = (it.isNumeric && Number.isFinite(prev) && Math.abs(prev)>0)
              ? (((cur - prev) / Math.abs(prev)) * 100)
              : null
            const pctForDisplay: number | null = (deltaPct != null)
              ? deltaPct
              : (delta == null ? null : (delta === 0 ? 0 : (delta > 0 ? 100 : null)))
            // Direction and tone by percent change (1% threshold for green)
            const pval = (typeof pctForDisplay === 'number') ? pctForDisplay : null
            const arrowDir: 'up'|'down'|'flat' = (pval == null) ? 'flat' : (pval > 1 ? 'up' : (pval < 0 ? 'down' : 'flat'))
            const arrowTone = (() => {
              if (pval == null) return 'text-gray-700'
              if (pval < 0) return 'text-rose-700 dark:text-rose-500'
              if (pval > 1) return 'text-emerald-700 dark:text-emerald-500'
              return 'text-gray-700'
            })()
            // Use Remix Icons per preset: Badge 1 uses SFill, others use Line
            const arrow = (() => {
              if (preset === 'badge1') {
                return arrowDir === 'up' ? (
                  <RiArrowUpSFill className="-ml-0.5 size-4" aria-hidden />
                ) : arrowDir === 'down' ? (
                  <RiArrowDownSFill className="-ml-0.5 size-4" aria-hidden />
                ) : (
                  <RiArrowRightSFill className="-ml-0.5 size-4" aria-hidden />
                )
              }
              return arrowDir === 'up' ? (
                <RiArrowUpLine className="-ml-0.5 size-4" aria-hidden />
              ) : arrowDir === 'down' ? (
                <RiArrowDownLine className="-ml-0.5 size-4" aria-hidden />
              ) : (
                <RiArrowRightLine className="-ml-0.5 size-4" aria-hidden />
              )
            })()
            // Display model for badges:
            //  - label (category)
            //  - current value (from last row)
            //  - delta (cur - prev) with sign
            //  - percent change ((cur-prev)/|prev|)
            const valStr = it.isNumeric ? valueFormatter(Number(it.value)) : String(it.value)
            const deltaStr = (it.isNumeric && delta != null)
              ? `${delta>0?'+':(delta<0?'-':'')}${Math.abs(Number(delta || 0)).toLocaleString()}`
              : ''
            const pctLabel = (pctForDisplay != null)
              ? `${pctForDisplay>0?'+':(pctForDisplay<0?'-':'')}${Math.abs(Number(pctForDisplay) || 0).toFixed(1)}%`
              : ''

            // Feature toggles
            const showLabelOutside = (options as any)?.badgesShowCategoryLabel ?? true
            const showLabelInside = (options as any)?.badgesLabelInside ?? false
            const showValue = (options as any)?.badgesShowValue ?? false
            const showDeltaNum = (options as any)?.badgesShowDelta ?? false
            const showPct = ((options as any)?.badgesShowDeltaPct ?? (options as any)?.tooltipShowDelta) ?? false
            const showShare = (options as any)?.badgesShowPercentOfTotal ?? false
            const effectiveOutside = !!showLabelOutside && !showLabelInside
            const effectiveInside = !!showLabelInside && !showLabelOutside

            // Apply legend label case to category label
            const labelText = (() => {
              const raw = String(it.label ?? '')
              const str = (raw.toLowerCase() === 'null') ? 'None' : raw
              const mode = (options as any)?.legendLabelCase as ('lowercase'|'capitalize'|'proper'|undefined)
              if (!mode) return str
              switch (mode) {
                case 'lowercase': return str.toLowerCase()
                case 'capitalize': {
                  const lower = str.toLowerCase()
                  return lower.length ? (lower[0].toUpperCase() + lower.slice(1)) : lower
                }
                case 'proper': default:
                  return str.replace(/[_-]+/g, ' ').split(/\s+/).map(w => w ? (w[0].toUpperCase() + w.slice(1).toLowerCase()) : w).join(' ')
              }
            })()

            // Tooltip
            const titleDelta = (showPct && delta != null)
              ? `${(delta>0?'+':(delta<0?'-':''))}${Math.abs(delta).toLocaleString()}${pctLabel?` ${pctLabel}`:''}`
              : ''
            const titleParts = [
              `${labelText}`,
              it.isNumeric ? `Value: ${valueFormatter(Number(cur))}` : '',
              titleDelta,
            ].filter(Boolean)
            const titleStr = titleParts.join(' · ')
            if (preset === 'badge4') {
              // Label + percent inside the chip, no arrow, no value
              const containerCls = 'inline-flex items-center gap-x-1 rounded-md px-2 py-1 ring-1 ring-inset ring-gray-200 dark:ring-gray-600 min-w-[88px] justify-center'
              return (
                <span key={i} className="relative group inline-flex items-center gap-2">
                  {effectiveOutside && (<span className="font-normal text-gray-700">{labelText}</span>)}
                  <span className={containerCls}>
                    {effectiveInside && (<span className="text-gray-700 font-normal">{labelText}</span>)}
                    {showValue && (<span className={`text-tremor-label font-normal ${arrowTone}`}>{valStr}</span>)}
                    {(showValue && (showDeltaNum || showPct)) && (<span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" />)}
                    {showDeltaNum && (<span className={`font-normal ${arrowTone}`}>{deltaStr}</span>)}
                    {showPct && (<span className={`font-normal ${arrowTone}`}>{pctLabel}</span>)}
                    {showShare && share!=null && (
                      <>
                        <span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" />
                        <span className="underline text-blue-600 dark:text-blue-400">{share}%</span>
                      </>
                    )}
                  </span>
                  <div className="pointer-events-none absolute -top-7 left-1/2 -translate-x-1/2 whitespace-nowrap px-2 py-1 rounded-md border bg-card text-xs opacity-0 group-hover:opacity-100 transition-opacity">{titleStr}</div>
                </span>
              )
            }

            if (preset === 'badge5') {
              // Tinted background chip: arrow + percent, label outside
              const bg = arrowDir === 'up' ? 'bg-emerald-50 dark:bg-emerald-400/10' : arrowDir === 'down' ? 'bg-rose-50 dark:bg-rose-400/10' : 'bg-gray-100 dark:bg-gray-500/20'
              const containerCls = `inline-flex items-center gap-x-1 rounded-md px-2.5 py-1 ring-1 ring-inset ring-gray-200 dark:ring-gray-600 min-w-[88px] justify-center ${bg}`
              return (
                <span key={i} className="relative group inline-flex items-center gap-2">
                  {effectiveOutside && (<span className="font-normal text-gray-700">{labelText}</span>)}
                  <span className={containerCls}>
                    {effectiveInside && (<span className={`font-normal ${arrowTone}`}>{labelText}</span>)}
                    <span className={`${arrowTone}`}>{arrow}</span>
                    {showValue && (<span className={`text-tremor-label font-normal ${arrowTone}`}>{valStr}</span>)}
                    {(showValue && (showDeltaNum || showPct)) && (<span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" />)}
                    {showDeltaNum && (<span className={`font-normal ${arrowTone}`}>{deltaStr}</span>)}
                    {showPct && (<span className={`font-normal ${arrowTone}`}>{pctLabel}</span>)}
                    {showShare && share!=null && (<><span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" /><span className="underline text-blue-600 dark:text-blue-400">{share}%</span></>)}
                  </span>
                  <div className="pointer-events-none absolute -top-7 left-1/2 -translate-x-1/2 whitespace-nowrap px-2 py-1 rounded-md border bg-card text-xs opacity-0 group-hover:opacity-100 transition-opacity">{titleStr}</div>
                </span>
              )
            }

            // Badge 1: outline chip, arrow + percent inside chip, label outside
            if (preset === 'badge1') {
              const base = 'inline-flex items-center gap-x-1 rounded-md px-2 py-1 text-tremor-label font-normal ring-1 ring-inset ring-gray-200 min-w-[88px] justify-center'
              const color = arrowTone
              const pctText = (typeof pctForDisplay === 'number') ? `${pctForDisplay>0?'+':(pctForDisplay<0?'-':'')}${Math.abs(pctForDisplay).toFixed(1)}%` : '0.0%'
              return (
                <span key={i} className="relative group inline-flex items-center gap-2">
                  {effectiveOutside && (<span className="font-normal text-gray-700">{labelText}</span>)}
                  <span className={`${base} ${color}`}>
                    {effectiveInside && (<span className="text-gray-700 font-normal">{labelText}</span>)}
                    {arrow}
                    {showValue && (<span className={`text-tremor-label font-normal ${arrowTone}`}>{valStr}</span>)}
                    {(showValue && (showDeltaNum || showPct)) && (<span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" />)}
                    {showDeltaNum && (<span className={`font-normal ${arrowTone}`}>{deltaStr}</span>)}
                    {showPct && (<span className={`font-normal ${arrowTone}`}>{pctText}</span>)}
                    {showShare && share!=null && (<><span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" /><span className="underline text-blue-600 dark:text-blue-400">{share}%</span></>)}
                  </span>
                  <div className="pointer-events-none absolute -top-7 left-1/2 -translate-x-1/2 whitespace-nowrap px-2 py-1 rounded-md border bg-card text-xs opacity-0 group-hover:opacity-100 transition-opacity">{titleStr}</div>
                </span>
              )
            }

            // Badge 2: tinted background chip, arrow + percent, label outside
            if (preset === 'badge2') {
              const bg = arrowDir === 'up' ? 'bg-emerald-50 dark:bg-emerald-400/10' : arrowDir === 'down' ? 'bg-rose-50 dark:bg-rose-400/10' : 'bg-gray-100 dark:bg-gray-500/20'
              const cls = `inline-flex items-center gap-x-1 rounded-md px-2 py-1 ring-1 ring-inset ring-gray-200 dark:ring-gray-600 min-w-[88px] justify-center ${bg}`
              return (
                <span key={i} className="relative group inline-flex items-center gap-2">
                  {effectiveOutside && (<span className="font-normal text-gray-700">{labelText}</span>)}
                  <span className={cls}>
                    {effectiveInside && (<span className={`font-normal ${arrowTone}`}>{labelText}</span>)}
                    <span className={`${arrowTone}`}>{arrow}</span>
                    {showValue && (<span className={`text-tremor-label font-normal ${arrowTone}`}>{valStr}</span>)}
                    {(showValue && (showDeltaNum || showPct)) && (<span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" />)}
                    {showDeltaNum && (<span className={`font-normal ${arrowTone}`}>{deltaStr}</span>)}
                    {showPct && (<span className={`font-normal ${arrowTone}`}>{pctLabel}</span>)}
                    {showShare && share!=null && (<><span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" /><span className="underline text-blue-600 dark:text-blue-400">{share}%</span></>)}
                  </span>
                  <div className="pointer-events-none absolute -top-7 left-1/2 -translate-x-1/2 whitespace-nowrap px-2 py-1 rounded-md border bg-card text-xs opacity-0 group-hover:opacity-100 transition-opacity">{titleStr}</div>
                </span>
              )
            }

            // Badge 3: outline chip with percent left and tinted arrow bubble right, label outside
            if (preset === 'badge3') {
              const cls = 'inline-flex items-center gap-x-1 rounded-md px-2 py-1 ring-1 ring-inset ring-gray-200 dark:ring-gray-600 min-w-[88px] justify-center'
              const bubble = arrowDir === 'up'
                ? 'inline-flex items-center justify-center rounded-md bg-emerald-100 px-1.5 py-0.5'
                : arrowDir === 'down'
                ? 'inline-flex items-center justify-center rounded-md bg-rose-100 px-1.5 py-0.5'
                : 'inline-flex items-center justify-center rounded-md bg-gray-200 px-1.5 py-0.5'
              return (
                <span key={i} className="relative group inline-flex items-center gap-2">
                  {effectiveOutside && (<span className="font-normal text-gray-700">{labelText}</span>)}
                  <span className={cls}>
                    {effectiveInside && (<span className={`font-normal ${arrowTone}`}>{labelText}</span>)}
                    {showValue && (<span className={`text-tremor-label font-normal ${arrowTone}`}>{valStr}</span>)}
                    {(showValue && (showDeltaNum || showPct)) && (<span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" />)}
                    {showDeltaNum && (<span className={`font-normal ${arrowTone}`}>{deltaStr}</span>)}
                    {showPct && (<span className={`font-normal ${arrowTone}`}>{pctLabel}</span>)}
                    <span className={bubble}>
                      <span className={`${arrowTone}`}>{arrow}</span>
                    </span>
                    {showShare && share!=null && (<><span className="h-4 w-px bg-gray-200 dark:bg-gray-800 mx-1" /><span className="underline text-blue-600 dark:text-blue-400">{share}%</span></>)}
                  </span>
                  <div className="pointer-events-none absolute -top-7 left-1/2 -translate-x-1/2 whitespace-nowrap px-2 py-1 rounded-md border bg-card text-xs opacity-0 group-hover:opacity-100 transition-opacity">{titleStr}</div>
                </span>
              )
            }
          })}
        </div>
      )
    }
    if (type === 'tracker') {
      const cats = categories || []
      const rows = Array.isArray(displayData) ? (displayData as any[]) : []
      const useGlobalMax = typeof options?.yMax === 'number'
      let rowMaxByCat: Record<string, number> = {}

      const applyCase = (label: string) => {
        const mode = (options as any)?.legendLabelCase as ('lowercase'|'capitalize'|'proper'|undefined)
        const str = String(label ?? '')
        if (!mode) return str
        switch (mode) {
          case 'lowercase': return str.toLowerCase()
          case 'capitalize': {
            const lower = str.toLowerCase()
            return lower.length ? lower[0].toUpperCase() + lower.slice(1) : lower
          }
          case 'proper': default:
            return str
              .replace(/[_-]+/g, ' ')
              .split(/\s+/)
              .map((w) => (w ? w[0].toUpperCase() + w.slice(1).toLowerCase() : w))
              .join(' ')
        }
      }

      // Unified calculations via calcEngine
      const ctx = buildTimelineContext(rows as any[], cats as string[], { yMax: (options?.yMax as any), trackerMaxPills: (options as any)?.trackerMaxPills })
      const labelsUsed = ctx.labels
      const rowsByLabel = ctx.rowsByLabel
      const totalByLabel = ctx.totalsByLabel
      rowMaxByCat = ctx.rowMaxByCat

      const wantVG = ((options as any)?.colorMode === 'valueGradient')
      const baseKey = ((options as any)?.colorBaseKey || chartColorsTokens[0] || 'blue')
      const baseHex = tremorNameToHex(baseKey as any)

      return (
        <div className="space-y-4">
          {cats.map((c, i) => {
            // Align series values with the axisLabels timeline so last/prev match the rendered pills
            const perRow = labelsUsed.map((label) => Number((rowsByLabel[label]?.[c]) ?? 0))
            const lastVal = perRow.length ? perRow[perRow.length - 1] : 0
            const prevVal = perRow.length > 1 ? perRow[perRow.length - 2] : 0
            const sumAgg = perRow.reduce((a, b) => a + (Number.isFinite(b) ? b : 0), 0)
            const colorKey = chartColorsTokens[i % chartColorsTokens.length]
            const dataArr = labelsUsed.map((label, idx) => {
              const row = rowsByLabel[label]
              const value = Number(row?.[c] ?? 0)
              const hasValue = Number.isFinite(value) && value !== 0
              const total = totalByLabel[label] || 0
              const pctShare = (cats.length > 1) ? (total > 0 ? (value / total) : 0) : ((rowMaxByCat[c] > 0) ? (value / rowMaxByCat[c]) : 0)
              const hex = hasValue ? saturateHexBy(baseHex, Math.max(0, Math.min(1, pctShare))) : '#e5e7eb'
              return {
                color: wantVG ? (hasValue ? hex : '#e5e7eb') : (hasValue ? `bg-${colorKey}-600` : 'bg-gray-300'),
                tooltip: hasValue ? `${valueFormatter(value)} · ${label}` : undefined,
              }
            })
            const title = [
              `${applyCase(normalizeCategoryLabel(String(c)))}: ${valueFormatter(sumAgg)}`,
              options?.tooltipShowPercent ? `Last: ${lastVal.toLocaleString()}` : '',
              options?.tooltipShowDelta && perRow.length > 1
                ? `Δ (last): ${(lastVal - prevVal) >= 0 ? '+' : ''}${(lastVal - prevVal).toLocaleString()}`
                : '',
            ]
              .filter(Boolean)
              .join(' · ')

            return (
              <div
                key={String(c)}
                className="relative group rounded-md border border-dashed border-muted-foreground/30 bg-muted/40 px-3 py-2"
              >
                <div className="flex items-center justify-between text-[12px] font-medium text-muted-foreground pb-1">
                  <span>{applyCase(normalizeCategoryLabel(String(c)))}</span>
                  <span className="text-foreground">{valueFormatter(sumAgg)}</span>
                </div>
                <div className="relative flex items-center gap-3">
                  <Tracker className="w-full" data={dataArr} />
                  {options?.dataLabelsShow && sumAgg !== 0 && (
                    <span className="text-[11px] font-medium whitespace-nowrap" style={{ color: axisTextColor as any, textShadow: '0 1px 2px rgba(0,0,0,0.6)' }}>{sumAgg.toLocaleString()}</span>
                  )}
                </div>
                <div className="pointer-events-none absolute -top-6 left-1/2 -translate-x-1/2 whitespace-nowrap px-2 py-1 rounded-md border bg-card text-xs opacity-0 group-hover:opacity-100 transition-opacity">
                  {title}
                </div>
              </div>
            )
          })}
        </div>
      )
    }
    if (type === 'combo') {
      // Support multi-series with legend: virtual categories like "2023 - OrderUID", "2024 - Amount_NIS"
      // Extract series label from virtual category name and check series metadata for type
      const xLabels = (data as any[]).map((d) => d.x)
      const cats = categories || []
      const baseSeries: any[] = []
      const metaByName = new Map<string, any>()
      const seriesLabels: string[] = []
      ;(Array.isArray(series) ? series : []).forEach((s: any, i: number) => {
        const label = s.label || s.y || s.measure || `series_${i + 1}`
        seriesLabels.push(String(label))
        metaByName.set(String(label), s)
      })
      const wantValueGrad = ((options as any)?.colorMode === 'valueGradient')
      const baseKeyForVG = ((options as any)?.colorBaseKey || chartColorsTokens[0] || 'blue')
      const baseHexVG = tremorNameToHex(baseKeyForVG as any)
      const rowTotals = (data as any[]).map((d) => (cats || []).reduce((s, c) => s + Number((d as any)?.[c] ?? 0), 0))
      // Layout helpers for labels / rotated ticks
      const fontSize = Math.max(8, Number(((options as any)?.xAxisFontSize ?? (options as any)?.yAxisFontSize ?? 11)))
      const xRotate = Math.max(-90, Math.min(90, Number((options as any)?.xTickAngle ?? 0)))
      const wantLabels = !!(options as any)?.dataLabelsShow
      const gridTopPad = wantLabels ? 24 : 0
      const gridBottomPad = Math.abs(xRotate) >= 60 ? Math.max(28, fontSize * 2) : 0
      
      // Helper: extract series label from virtual category name (e.g., "2023 - OrderUID" -> "OrderUID")
      const extractSeriesLabel = (catName: string): string | null => {
        // Pattern 1: "legendValue - seriesLabel" or "legendValue • seriesLabel"
        const match1 = catName.match(/^.+?\s*[-•·]\s*(.+)$/)
        if (match1) return match1[1].trim()
        // Pattern 2: "seriesLabel • legendValue" or "seriesLabel - legendValue"
        const match2 = catName.match(/^(.+?)\s*[-•·]\s*.+$/)
        if (match2) return match2[1].trim()
        // No separator: return as-is
        return catName
      }
      
      // Default rule: last series as line, others as bars (if no explicit series metadata)
      const defaultLastAsLine = seriesLabels.length === 0 || !Array.isArray(series) || series.length === 0
      const lastSeriesLabel = seriesLabels.length > 0 ? seriesLabels[seriesLabels.length - 1] : null
      
      cats.forEach((c, idx) => {
        const baseHex = wantValueGrad ? baseHexVG : tremorNameToHex(chartColorsTokens[idx % chartColorsTokens.length])
        const values = (data as any[]).map((d) => {
          const val = (d as any)?.[c]
          if (val === null || val === undefined) return null
          return Number(val)
        })
        
        // Extract series label and lookup metadata
        const seriesLabel = extractSeriesLabel(String(c))
        const sMeta = seriesLabel ? metaByName.get(seriesLabel) : null
        
        // Determine type: secondary axis series are lines, primary axis are bars
        // Fallback: last series as line if no series metadata
        const isLine = sMeta?.secondaryAxis === true || (defaultLastAsLine && idx === cats.length - 1)
        
        const seriesType = isLine ? 'line' : 'bar'
        
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try { 
            console.log('[ChartCard] [Combo] Series type determination:', { 
              category: c, 
              seriesLabel, 
              secondaryAxis: sMeta?.secondaryAxis, 
              type: seriesType 
            }) 
          } catch {}
        }
        
        const seriesData = wantValueGrad
          ? values.map((v, i) => (v === null ? null : { value: v, itemStyle: { color: saturateHexBy(baseHexVG, Math.max(0, Math.min(1, rowTotals[i] > 0 ? (v / rowTotals[i]) : 0))) } }))
          : values
        
        if (seriesType === 'line') {
          baseSeries.push({ 
            name: c, 
            type: 'line', 
            data: seriesData, 
            smooth: true, 
            connectNulls: false,
            lineStyle: { width: (options?.lineWidth ?? 2), color: baseHex }, 
            itemStyle: { color: baseHex },
            emphasis: { focus: 'series' },
            ...(sMeta?.secondaryAxis ? { yAxisIndex: 1 } : {})
          })
        } else {
          baseSeries.push({ 
            name: c, 
            type: 'bar', 
            data: seriesData, 
            itemStyle: { color: baseHex }, 
            emphasis: { focus: 'series' },
            ...(wantLabels ? { label: { show: true, position: 'top', fontSize: 10, color: axisTextColor, textShadowColor: 'rgba(0,0,0,0.5)', textShadowBlur: 2, textShadowOffsetY: 1 } } : {}),
            ...(sMeta?.secondaryAxis ? { yAxisIndex: 1 } : {})
          })
        }
      })
      const hasSecondary = baseSeries.some((s) => s.yAxisIndex === 1)
      const visualMap = (() => {
        if (!wantValueGrad) return undefined
        const idxLine = baseSeries.findIndex((s) => s.type === 'line')
        if (idxLine < 0) return undefined
        const lineSeries = baseSeries[idxLine]
        const lineData = Array.isArray(lineSeries?.data) ? lineSeries.data : []
        const gmax = Math.max(1, ...lineData.map((v: any) => Number(v ?? 0)))
        const c1 = saturateHexBy(baseHexVG, 0.25)
        const c2 = saturateHexBy(baseHexVG, 0.95)
        return [{ show: false, type: 'continuous', min: 0, max: gmax, dimension: 0, seriesIndex: idxLine, inRange: { color: [c1, c2] } }] as any
      })()
      const dzEnabledCombo = !!(((options as any)?.zoomPan) || ((options as any)?.dataZoom))
      const dataZoomCombo = dzEnabledCombo ? [
        { type: 'inside', xAxisIndex: 0, filterMode: 'filter', zoomOnMouseWheel: true, moveOnMouseWheel: true, moveOnMouseMove: true, throttle: 50 },
        { type: 'slider', xAxisIndex: 0, bottom: 10, height: 26, handleSize: 12, showDetail: false, filterMode: 'filter' },
      ] : undefined
      const baseSeriesAug = (((options as any)?.largeScale) && Array.isArray(baseSeries))
        ? baseSeries.map((s:any) => (s?.type === 'line' ? { ...s, sampling: 'lttb', progressive: 20000, progressiveThreshold: 3000 } : s))
        : baseSeries
      // Calculate margins for Y-axis labels (combo chart)
      const marginsCombo = (() => {
        try {
          const allValues: number[] = []
          if (Array.isArray(baseSeries)) {
            baseSeries.forEach((s: any) => {
              if (Array.isArray(s?.data)) {
                s.data.forEach((point: any) => {
                  const val = Array.isArray(point) ? Number(point[1] ?? 0) : Number(point ?? 0)
                  if (!isNaN(val)) allValues.push(Math.abs(val))
                })
              }
            })
          }
          if (allValues.length === 0) return { left: 60, right: hasSecondary ? 60 : 30 }
          const maxVal = Math.max(...allValues)
          const formatted = valueFormatter(maxVal)
          const estimatedWidth = Math.ceil((formatted.length * 10) + 30)
          const margin = Math.max(estimatedWidth, 60)
          if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
            console.log('[ChartCard] [Combo] Margin calculation:', { maxVal, formatted, charCount: formatted.length, estimatedWidth, finalMargin: margin })
          }
          return { left: margin, right: hasSecondary ? margin : 30 }
        } catch {
          return { left: 60, right: hasSecondary ? 60 : 30 }
        }
      })()
      const option = {
        tooltip: {
          show: (Array.isArray(baseSeries) && baseSeries.length > 0 && baseSeries.some((s:any)=>Array.isArray(s?.data) && s.data.length>0) && Array.isArray(data) && data.length>0),
          trigger: 'axis',
          axisPointer: ((Array.isArray(baseSeries) && baseSeries.length > 0 && baseSeries.some((s:any)=>Array.isArray(s?.data) && s.data.length>0)) ? { type: 'shadow' } : undefined),
          backgroundColor: 'transparent',
          borderColor: 'transparent',
          textStyle: { color: undefined as any },
          extraCssText: 'border:1px solid hsl(var(--border));background:hsl(var(--card));color:hsl(var(--foreground));padding:4px 8px;border-radius:6px;box-shadow:none;',
          formatter: (paramsAny: any) => {
            if (typeof window !== 'undefined') {
              try { console.log('[ChartCard] [Tooltip] AXIS formatter triggered', { paramsAny, querySpec }) } catch {}
            }
            const params = Array.isArray(paramsAny) ? paramsAny : (paramsAny ? [paramsAny] : [])
            if (!params.length) return ''
            const idx = params?.[0]?.dataIndex ?? 0
            const xLabels = (data as any[]).map((d)=>d.x)
            const rawX = xLabels[idx]
            const xLabel = (()=>{
              const s = String(rawX ?? '')
              const d = parseDateLoose(s)
              const fmt = (options as any)?.xDateFormat
                || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
                  : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
                  : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
                  : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
                  : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
                  : undefined)
              if (d && fmt) {
                const pad = (n: number) => String(n).padStart(2, '0')
                const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
                const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
                switch (fmt) {
                  case 'YYYY': return String(d.getFullYear())
                  case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
                  case 'YYYY-[W]ww': {
                    const jan1 = new Date(d.getFullYear(), 0, 1)
                    const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000)
                    const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1
                    const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun')
                    const wn = useSun ? wnSun : isoWeek(d)
                    return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}`
                  }
                  case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
                  case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
                  case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
                  case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
                  case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
                  case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
                  default: return formatDatePattern(d, String(fmt))
                }
              }
              const mode = (options as any)?.xLabelCase as 'lowercase'|'capitalize'|'proper'|undefined
              const str = s
              if (!mode) return str
              switch (mode) {
                case 'lowercase': return str.toLowerCase()
                case 'capitalize': { const lower = str.toLowerCase(); return lower.length ? (lower[0].toUpperCase()+lower.slice(1)) : lower }
                case 'proper': default: return str.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
              }
            })()
            const getValAt = (p:any, i:number): number => {
              const rawName = String(p?.seriesName ?? '')
              const vmeta = (((q?.data as any)?.virtualMeta) || {}) as Record<string, { baseSeriesLabel: string; agg?: string }>
              const keyName = (vmeta[rawName]?.baseSeriesLabel) ? String(vmeta[rawName].baseSeriesLabel) : rawName
              const row = i >= 0 && i < (data as any[]).length ? (data as any[])[i] : undefined
              const vFromData = row
                ? (Array.isArray(categories) && (categories as any[]).length > 0
                    ? Number((row as any)?.[keyName] ?? 0)
                    : Number((row as any)?.value ?? 0))
                : NaN
              const valAny = (p?.value as any)
              const vFromParam = Array.isArray(valAny)
                ? Number(valAny?.[1] ?? 0)
                : Number((valAny as any)?.value ?? valAny ?? 0)
              return Number.isFinite(vFromData) ? vFromData : (Number.isFinite(vFromParam) ? vFromParam : 0)
            }
            const withValues = params.map((p:any) => ({ p, v: getValAt(p, idx) }))
            const filtered = (options?.tooltipHideZeros ? withValues.filter((x:any)=>Number(x.v)!==0) : withValues)
            const total = filtered.reduce((sum:number,x:any)=> sum + Number(x.v ?? 0), 0)
            let html = `<div style=\"font-weight:600;margin-bottom:4px\">${xLabel}</div>`
            const prevIdx = resolvePrevIndex(idx, xLabels)
            // Debug: log first param to see value format
            if (filtered.length > 0 && typeof console !== 'undefined') {
              console.log('[ChartCard] Tooltip param:', { idx, p: filtered[0]?.p, v: filtered[0]?.v, dataRow: (data as any[])[idx], allParams: params })
            }
            filtered.forEach((pp:any)=>{
              const p = pp.p
              const v = Number(pp.v ?? 0)
              const pct = total > 0 ? ((v / total) * 100).toFixed(1) : '0.0'
              const prev = prevIdx >= 0 ? getValAt(p, prevIdx) : 0
              const delta = v - prev
              const color = p.color
              const showPct = !!options?.tooltipShowPercent
              const showDelta = !!options?.tooltipShowDelta
              const name = legendDisplayName(String(p.seriesName))
              html += `<div style=\"display:flex;align-items:center;gap:6px\">`+
                `<span style=\"display:inline-block;width:8px;height:8px;background:${color};border-radius:2px\"></span>`+
                `<span>${name}</span>`+
                `<span style=\"opacity:.8;margin-left:auto\">${valueFormatter(v)}${showPct?` (${pct}%)`:''}${showDelta && prevIdx>=0?` · ${delta>=0?'+':''}${delta.toLocaleString()}`:''}</span>`+
              `</div>`
            })
            return html
          }
        },
        legend: { show: false },
        grid: { left: marginsCombo.left, right: marginsCombo.right, top: 10 + gridTopPad, bottom: 24 + gridBottomPad + (dataZoomCombo ? 48 : 0), containLabel: false },
        xAxis: { type: 'category', data: (() => { const fmt = (val:any)=>{ const s = String(val ?? ''); const d = parseDateLoose(s); const f = (options as any)?.xDateFormat
              || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
                : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
                : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
                : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
                : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
                : undefined); if (d && f) { const pad = (n: number) => String(n).padStart(2, '0'); const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }; const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1); switch (f) { case 'YYYY': return String(d.getFullYear()); case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`; case 'YYYY-[W]ww': { const jan1 = new Date(d.getFullYear(), 0, 1); const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime()) - jan1.getTime()) / 86400000; const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1; const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || env.weekStart) === 'sun'); const wn = useSun ? wnSun : isoWeek(d); return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}`; } case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`; case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`; case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` } case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' }); case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' }); case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-'); default: return s; } } const mode = (options as any)?.xLabelCase as 'lowercase'|'capitalize'|'proper'|undefined; if (!mode) return s; switch (mode) { case 'lowercase': return s.toLowerCase(); case 'capitalize': { const lower = s.toLowerCase(); return lower.length ? (lower[0].toUpperCase()+lower.slice(1)) : lower } case 'proper': default: return s.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' '); } }; return (xLabels || []).map(fmt); })(), axisLabel: { rotate: xRotate, fontSize: ((options as any)?.xAxisFontSize ?? fontSize), fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.xAxisFontColor || axisTextColor), margin: Math.abs(xRotate) >= 60 ? 12 : 8 } }
,
        yAxis: hasSecondary ? [
          { type: 'value', splitNumber: options?.yTickCount || undefined, axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) } },
          { type: 'value', splitNumber: options?.yTickCount || undefined, axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) }, position: 'right' },
        ] : { type: 'value', splitNumber: options?.yTickCount || undefined, axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) } },
        series: baseSeriesAug,
        ...(visualMap ? { visualMap } : {}),
        ...(dataZoomCombo ? { dataZoom: dataZoomCombo } : {}),
      }
      // Apply grid styling for scatter charts
      try {
        if ((option as any).xAxis) {
          if (Array.isArray((option as any).xAxis)) (option as any).xAxis = (option as any).xAxis.map((ax: any) => ({ ...ax, ...(buildAxisGrid('x') as any) }))
          else (option as any).xAxis = { ...((option as any).xAxis || {}), ...(buildAxisGrid('x') as any) }
        }
        if ((option as any).yAxis) {
          if (Array.isArray((option as any).yAxis)) (option as any).yAxis = (option as any).yAxis.map((ay: any) => ({ ...ay, ...(buildAxisGrid('y') as any) }))
          else (option as any).yAxis = { ...((option as any).yAxis || {}), ...(buildAxisGrid('y') as any) }
        }
      } catch {}
      // Apply grid styling for combo charts
      try {
        if ((option as any).xAxis) {
          if (Array.isArray((option as any).xAxis)) (option as any).xAxis = (option as any).xAxis.map((ax: any) => ({ ...ax, ...(buildAxisGrid('x') as any) }))
          else (option as any).xAxis = { ...((option as any).xAxis || {}), ...(buildAxisGrid('x') as any) }
        }
        if ((option as any).yAxis) {
          if (Array.isArray((option as any).yAxis)) (option as any).yAxis = (option as any).yAxis.map((ay: any) => ({ ...ay, ...(buildAxisGrid('y') as any) }))
          else (option as any).yAxis = { ...((option as any).yAxis || {}), ...(buildAxisGrid('y') as any) }
        }
      } catch {}
      return (
        <div className="absolute inset-0" style={{ paddingBottom: dataZoomCombo ? 48 : 0 }}>
          <ReactECharts
            ref={echartsRef}
            key={chartInstanceKey}
            option={option}
            notMerge={true}
            lazyUpdate
            style={{ height: '100%' }}
            onChartReady={(ec:any) => { try { ec && ec.resize && ec.resize() } catch {} try { requestAnimationFrame(() => requestAnimationFrame(() => { markChartReady() })) } catch {} }}
            onEvents={(options as any)?.largeScale ? {
              dataZoom: (ev: any) => {
                try {
                  const xLabels = (displayData as any[]).map((d) => d.x)
                  const total = xLabels.length
                  const b = (Array.isArray(ev?.batch) && ev.batch.length ? ev.batch[0] : ev) || {}
                  let si = 0, ei = Math.max(0, total - 1)
                  if (typeof b.startValue !== 'undefined' || typeof b.endValue !== 'undefined') {
                    si = Math.max(0, Math.min(total - 1, Number(b.startValue ?? 0)))
                    ei = Math.max(0, Math.min(total - 1, Number(b.endValue ?? (total - 1))))
                  } else {
                    const sp = Math.max(0, Math.min(100, Number(b.start ?? 0)))
                    const ep = Math.max(0, Math.min(100, Number(b.end ?? 100)))
                    si = Math.round((sp / 100) * (total - 1))
                    ei = Math.round((ep / 100) * (total - 1))
                  }
                  const sv = xLabels[si]
                  const evv = xLabels[ei]
                  updateAdaptiveGroupingFromZoom(sv, evv)
                } catch {}
              },
              finished: () => { markChartReady() }
            } : { finished: () => { markChartReady() }}}
          />
        </div>
      )
    }
    if (type === 'spark') {
      const sparkDownIsGood = !!(options as any)?.sparkDownIsGood
      // SVG <linearGradient id> must be a valid DOM id; sanitize to avoid spaces or special chars breaking url(#id)
      const toDomId = (s: any) => String(s ?? '').replace(/[^A-Za-z0-9_-]/g, '_')
      const labelMaxLines = Math.max(1, Math.min(3, Number((options as any)?.sparkLabelMaxLines ?? 2)))
      const clampStyle = labelMaxLines
        ? ({
            display: '-webkit-box',
            WebkitBoxOrient: 'vertical',
            overflow: 'hidden',
            WebkitLineClamp: labelMaxLines,
            lineClamp: labelMaxLines,
          } as CSSProperties)
        : undefined
      const showDelta = !!options?.tooltipShowDelta
      const colorForPct = (pct: number) => {
        if (pct > 0) {
          return sparkDownIsGood ? 'rose' : 'emerald'
        }
        if (pct < 0) {
          return sparkDownIsGood ? 'emerald' : 'rose'
        }
        return 'gray'
      }
      const textToneForPct = (pct: number) => {
        if (pct > 0) {
          return sparkDownIsGood ? 'text-rose-700' : 'text-emerald-700'
        }
        if (pct < 0) {
          return sparkDownIsGood ? 'text-emerald-700' : 'text-rose-700'
        }
        return 'text-gray-700'
      }
      const deltaFromServer = (key: string): number | null => {
        const raw = (deltaQ.data?.deltas ? (deltaQ.data.deltas as any as Record<string, number>)[key] : undefined)
        return Number.isFinite(Number(raw)) ? Number(raw) : null
      }
      // Compute without hooks to avoid hook-order changes when switching chart types
      const periodCurSums: Record<string, number> = (() => {
        const acc: Record<string, number> = {}
        try {
          if (Array.isArray(displayData) && Array.isArray(categories) && categories.length > 0) {
            for (const row of displayData as any[]) {
              for (const k of categories as any[]) {
                const v = Number((row as any)?.[String(k)] ?? 0)
                if (Number.isFinite(v)) acc[String(k)] = (acc[String(k)] || 0) + v
              }
            }
          } else if (Array.isArray(displayData)) {
            // single-series: use label 'value'
            let s = 0
            for (const row of displayData as any[]) s += Number((row as any)?.value ?? 0)
            acc['value'] = s
          }
        } catch {}
        return acc
      })()
      const buildSparkTooltip = (seriesKey: string, cats: string[]) => ({ active, payload, label }: any) => {
        if (!active || !payload || payload.length === 0) return null
        const datumIdx = Array.isArray(displayData) ? (displayData as any[]).findIndex((row) => String(row?.x) === String(label)) : -1
        const v = Number(payload[0]?.value ?? 0)
        const xSeq = Array.isArray(displayData) ? (displayData as any[]).map((r:any)=>r?.x) : []
        const prevIdx = resolvePrevIndex(datumIdx, xSeq)
        const hasPrev = prevIdx >= 0
        const prevFromIndex = hasPrev ? Number((displayData as any[])[prevIdx]?.[seriesKey] ?? 0) : 0
        // Try cached prev totals for prev period when the previous x bucket isn't present
        const rawX = String(label ?? '')
        const rk = prevRangeKeyForRawX(rawX)
        const vmeta = (((q?.data as any)?.virtualMeta) || {}) as Record<string, { baseSeriesLabel?: string; agg?: string }>
        const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
        let sAgg: any = (vmeta[seriesKey]?.agg as any) || ((querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count'))
        let sY: any = (querySpec as any)?.y as any
        let sMeasure: any = (querySpec as any)?.measure as any
        if (seriesDefs.length > 0) {
          const base = String(seriesKey).split(' • ')[0].trim()
          for (let i = 0; i < seriesDefs.length; i++) {
            const s = seriesDefs[i]
            const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
            if (String(lab).trim() === base) { sAgg = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count')); sY = s?.y; sMeasure = s?.measure; break }
          }
        }
        const ck = rk ? `${rk}::${sAgg}|${sY || ''}|${sMeasure || ''}` : null
        const totals = ck ? (prevTotalsCacheRef.current[ck] || {}) : {}
        if (ck && Object.keys(totals).length === 0) {
          try {
            const r = prevRangeForRawX(rawX)
            const source = (querySpec as any)?.source as string | undefined
            if (r && deltaDateField && source && !inflightPrevFetchesRef.current[ck]) {
              inflightPrevFetchesRef.current[ck] = true
              const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
              const xField = (querySpec as any)?.x as string | undefined
              const partTitle = gb === 'year' ? 'Year' : gb === 'quarter' ? 'Quarter' : gb === 'month' ? 'Month' : gb === 'week' ? 'Week' : gb === 'day' ? 'Day' : undefined
              const X_DER = (() => { try { const m = String(xField || '').match(/^(.*)\s\((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/); return m ? { base: m[1], part: m[2] } : null } catch { return null } })()
              const xPartExpr = X_DER ? String(xField) : ((xField && partTitle) ? `${xField} (${partTitle})` : undefined)
              const legendAny = (querySpec as any)?.legend as any
              let legendArg: any = undefined
              const hasLegend = (Array.isArray(legendAny) ? legendAny.length > 0 : (typeof legendAny === 'string' && legendAny.trim() !== ''))
              if (hasLegend) {
                if (Array.isArray(legendAny)) { const arr = legendAny.slice(); if (xPartExpr) arr.push(xPartExpr); legendArg = arr }
                else { legendArg = xPartExpr ? [legendAny, xPartExpr] : legendAny }
              } else if (xPartExpr) { legendArg = xPartExpr }
              const whereEff = (() => {
                try {
                  const w: any = { ...(uiTruthWhere || {}) }
                  const df = deltaDateField as string | undefined
                  if (df) {
                    const keysToDelete = Object.keys(w).filter((k) => {
                      const key = String(k)
                      return key === df || key.startsWith(`${df} (`) || key.startsWith(`${df}__`)
                    })
                    keysToDelete.forEach((k) => delete w[k])
                  }
                  const xBaseField = (X_DER ? X_DER.base : xField) as string | undefined
                  if (xBaseField) {
                    const keysToDelete = Object.keys(w).filter((k) => {
                      const key = String(k)
                      return key === xBaseField || key.startsWith(`${xBaseField} (`) || key.startsWith(`${xBaseField}__`)
                    })
                    keysToDelete.forEach((k) => delete w[k])
                  }
                  const xPart = xPartExpr as string | undefined
                  if (xPart && xPart !== xBaseField) {
                    const keysToDelete = Object.keys(w).filter((k) => String(k) === xPart || String(k).startsWith(`${xPart}__`))
                    keysToDelete.forEach((k) => delete w[k])
                  }
                  return Object.keys(w).length ? w : undefined
                } catch { return undefined }
              })()
              void (async () => {
                try {
                  const resp = await Api.periodTotals({
                    source,
                    datasourceId,
                    dateField: deltaDateField!,
                    start: r.start,
                    end: r.end,
                    where: whereEff as any,
                    legend: legendArg,
                    agg: sAgg as any,
                    y: sY as any,
                    measure: sMeasure as any,
                    weekStart: (deltaWeekStart as any) || undefined,
                  })
                  const map: Record<string, number> = { ...(resp?.totals || {}) } as any
                  const t = Number((resp as any)?.total)
                  if (Number.isFinite(t)) map['__total__'] = t
                  prevTotalsCacheRef.current[ck] = map
                } catch {} finally { inflightPrevFetchesRef.current[ck] = false }
              })()
            }
          } catch {}
        }
        const serverPart = (() => {
          try {
            const cur = parseDateLoose(String(label))
            if (!cur) return ''
            const pv = shiftByMode(cur)
            if (!pv) return ''
            const pad = (n:number)=>String(n).padStart(2,'0')
            const gb = String((querySpec as any)?.groupBy || 'none').toLowerCase()
            if (gb === 'year') return `${pv.getFullYear()}`
            if (gb === 'quarter') { const q = Math.floor(pv.getMonth()/3)+1; return `${pv.getFullYear()}-Q${q}` }
            if (gb === 'month') return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}`
            if (gb === 'day') return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}-${pad(pv.getDate())}`
            if (gb === 'week') {
              const ws = (deltaWeekStart || 'mon')
              const date = new Date(Date.UTC(pv.getFullYear(), pv.getMonth(), pv.getDate()))
              if (ws === 'sun') {
                const onejan = new Date(pv.getFullYear(),0,1)
                const week = Math.ceil((((pv as any)- (onejan as any))/86400000 + onejan.getDay()+1)/7)
                return `${pv.getFullYear()}-W${pad(week)}`
              } else {
                const dayNr = (date.getUTCDay() + 6) % 7
                date.setUTCDate(date.getUTCDate() - dayNr + 3)
                const firstThursday = new Date(Date.UTC(date.getUTCFullYear(),0,4))
                const week = 1 + Math.round(((date.getTime() - firstThursday.getTime())/86400000 - 3) / 7)
                return `${date.getUTCFullYear()}-W${pad(week)}`
              }
            }
            return ''
          } catch { return '' }
        })()
        const baseCandidates = (() => {
          try {
            const rn = String(seriesKey)
            const parts = rn.includes(' • ') ? rn.split(' • ') : (rn.includes(' · ') ? rn.split(' · ') : rn.split(' • '))
            const a = (parts[0] || '').trim()
            const b = String((vmeta as any)?.[seriesKey]?.baseSeriesLabel || a)
            const legendPart = parts.slice(1).join(' • ').trim()
            const list: string[] = []
            if (legendPart) list.push(a)
            if (!list.includes(b)) list.push(b)
            return list.length ? list : [a]
          } catch { return [String(seriesKey)] }
        })()
        const prevFromCache = (() => {
          try {
            if (!ck || !Object.keys(totals || {}).length) return 0
            for (const b of baseCandidates) {
              const keyVirtual = serverPart ? `${b} • ${serverPart}` : b
              const seq = [keyVirtual, b, '__total__']
              for (const k of seq) {
                const n = Number((totals as any)?.[k] ?? 0)
                if (Number.isFinite(n) && (k === '__total__' ? true : (k in (totals as any)))) return n
              }
            }
            return 0
          } catch { return 0 }
        })()
        const prev = hasPrev ? prevFromIndex : prevFromCache
        const rowPayload = payload[0]?.payload || {}
        const rowTotal = cats.length > 0 ? cats.reduce((sum, key) => sum + Number(rowPayload?.[key] ?? 0), 0) : Number(rowPayload?.[seriesKey] ?? v)
        const shareStr = rowTotal > 0 ? `${((v / rowTotal) * 100).toFixed(1)}%` : '0.0%'
        const changePct = computeChangePercent(v, prev)
        const havePrevVal = hasPrev || Number(prev) !== 0
        const changeStr = havePrevVal ? `${changePct >= 0 ? '+' : '-'}${Math.abs(changePct).toFixed(1)}%` : ''
        const changeColor = (changePct < 0) ? '#ef4444' : (changePct > 10 ? '#22c55e' : '#9CA3AF')
        const deltaStr = havePrevVal ? `${(v - prev) >= 0 ? '+' : '-'}${valueFormatter(Math.abs(v - prev))}` : ''
        const name = (options as any)?.legendLabelCase ? ((): string => { const s = String(seriesKey); return (options as any)?.legendLabelCase === 'lowercase' ? s.toLowerCase() : (options as any)?.legendLabelCase === 'capitalize' ? (s.toLowerCase().replace(/^(.)/, (m:any)=>m.toUpperCase())) : s.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()) : w).join(' ') })() : String(seriesKey)
        const aggLabel = (() => {
          const fromMeta = (vmeta[seriesKey]?.agg as any)
          if (fromMeta) return toProperCase(String(fromMeta))
          try {
            const seriesDefs: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
            if (seriesDefs.length > 0) {
              const base = String(seriesKey).split(' • ')[0].trim()
              for (let i = 0; i < seriesDefs.length; i++) {
                const s = seriesDefs[i]
                const lab = (s?.label || s?.y || s?.measure || `series_${i+1}`)
                if (String(lab).trim() === base) {
                  const ag = (s?.agg ?? (querySpec as any)?.agg ?? ((s?.y || s?.measure) ? 'sum' : 'count'))
                  return toProperCase(String(ag))
                }
              }
            }
          } catch {}
          const fallback = (querySpec as any)?.agg || (((querySpec as any)?.y || (querySpec as any)?.measure) ? 'sum' : 'count')
          // If any series defines a numeric measure/y, prefer SUM for the Total spark display
          try {
            const seriesDefsArr: any[] = Array.isArray((querySpec as any)?.series) ? (querySpec as any).series : []
            if (!fallback && seriesDefsArr.some((s:any)=>!!(s?.y || s?.measure))) return 'Sum'
          } catch {}
          return toProperCase(String(fallback))
        })()
        // Header and prev label
        const hdr = (() => {
          const s = String(label ?? '')
          const d = parseDateLoose(s)
          const fmt = (options as any)?.xDateFormat
            || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
              : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
              : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
              : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
              : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
              : undefined)
          if (d && fmt) {
            const pad = (n: number) => String(n).padStart(2, '0')
            const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
            const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
            switch (fmt) {
              case 'YYYY': return String(d.getFullYear())
              case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
              case 'YYYY-[W]ww': { const jan1 = new Date(d.getFullYear(), 0, 1); const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000); const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1; const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun'); const wn = useSun ? wnSun : isoWeek(d); return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}` }
              case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
              case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
              case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
              case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
              case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
              case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
              default: return formatDatePattern(d, String(fmt))
            }
          }
          return String(label)
        })()
        const prevLabel = hasPrev ? (() => {
          const prevX = (displayData as any[])[prevIdx]?.x
          if (prevX == null) return undefined
          const s = String(prevX ?? '')
          const d = parseDateLoose(s)
          const fmt = (options as any)?.xDateFormat
            || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
              : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
              : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
              : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
              : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
              : undefined)
          if (d && fmt) {
            const pad = (n: number) => String(n).padStart(2, '0')
            const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
            const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
            switch (fmt) {
              case 'YYYY': return String(d.getFullYear())
              case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
              case 'YYYY-[W]ww': { const jan1 = new Date(d.getFullYear(), 0, 1); const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000); const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1; const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun'); const wn = useSun ? wnSun : isoWeek(d); return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}` }
              case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
              case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
              case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
              case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
              case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
              case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
              default: return formatDatePattern(d, String(fmt))
            }
          }
          return String(prevX)
        })() : (() => {
          try {
            if (!effectiveDeltaMode) return undefined
            const s = String(label ?? '')
            const d = parseDateLoose(s)
            if (!d) return undefined
            const pv = (() => {
              switch (effectiveDeltaMode as any) {
                case 'TD_YSTD': { const x = new Date(d); x.setDate(x.getDate()-1); return x }
                case 'TW_LW': { const x = new Date(d); x.setDate(x.getDate()-7); return x }
                case 'MONTH_LMONTH':
                case 'MTD_LMTD': { const x = new Date(d); x.setMonth(x.getMonth()-1); return x }
                case 'TQ_LQ': { const x = new Date(d); x.setMonth(x.getMonth()-3); return x }
                case 'TY_LY':
                case 'YTD_LYTD':
                case 'Q_TY_VS_Q_LY':
                case 'QTD_TY_VS_QTD_LY':
                case 'M_TY_VS_M_LY':
                case 'MTD_TY_VS_MTD_LY': { const x = new Date(d); x.setFullYear(x.getFullYear()-1); return x }
                default: return null
              }
            })()
            if (!pv) return undefined
            const fmt = (options as any)?.xDateFormat
              || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
                : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
                : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
                : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
                : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
                : undefined)
            const pad = (n: number) => String(n).padStart(2, '0')
            const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
            const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
            if (fmt) {
              switch (fmt) {
                case 'YYYY': return String(pv.getFullYear())
                case 'YYYY-[Q]q': return `${pv.getFullYear()}-Q${quarter(pv)}`
                case 'YYYY-[W]ww': { const jan1 = new Date(pv.getFullYear(), 0, 1); const day0 = Math.floor((new Date(pv.getFullYear(), pv.getMonth(), pv.getDate()).getTime() - jan1.getTime()) / 86400000); const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1; const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun'); const wn = useSun ? wnSun : isoWeek(pv); return `${pv.getFullYear()}-W${String(wn).padStart(2,'0')}` }
                case 'YYYY-MM': return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}`
                case 'YYYY-MM-DD': return `${pv.getFullYear()}-${pad(pv.getMonth()+1)}-${pad(pv.getDate())}`
                case 'h:mm a': { let h = pv.getHours(); const m = pad(pv.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
                case 'dddd': return pv.toLocaleDateString('en-US', { weekday: 'long' })
                case 'MMMM': return pv.toLocaleDateString('en-US', { month: 'long' })
                case 'MMM-YYYY': return pv.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
                default: return formatDatePattern(pv, String(fmt))
              }
            }
            return String(pv)
          } catch { return undefined }
        })()
        const rows: TooltipRow[] = [{
          color: (payload && payload[0] && payload[0].color) ? String(payload[0].color) : '#94a3b8',
          name,
          valueStr: valueFormatter(v),
          shareStr,
          aggLabel,
          changeStr,
          deltaStr,
          prevStr: hasPrev ? valueFormatter(prev) : '',
          changeColor,
        }]
        return (<TooltipTable header={hdr} prevLabel={prevLabel} rows={rows} showDeltas={!!effectiveDeltaMode} />)
      }
      const multi = (categories?.length ?? 0) > 1
      if (multi) {
        return (
          <div className="absolute inset-0 space-y-2" ref={sparkAreaRef}>
            {(categories || []).map((c) => {
              const vals = (data as any[]).map((row: any) => Number(row?.[c] ?? 0))
              const first = vals.length ? Number(vals[0] || 0) : 0
              const last = vals.length ? Number(vals[vals.length - 1] || 0) : 0
              const prevVal = vals.length > 1 ? Number(vals[vals.length - 2] || 0) : first
              const totalSum = vals.reduce((sum, v) => sum + (Number(v) || 0), 0)
              // Extract legend value from virtual category key like "Deposit ID • AMAN Test" -> "AMAN Test"
              const legendValue = String(c).includes(' • ') ? String(c).split(' • ').slice(1).join(' • ') : String(c)
              const serverDelta = deltaFromServer(legendValue)
              const deltaVal = serverDelta !== null ? serverDelta : (last - prevVal)
              const previousBaseline = serverDelta !== null ? (totalSum - serverDelta) : prevVal
              // Calculate pct from trend when no server delta is available
              const pct = (serverDelta !== null && previousBaseline !== 0)
                ? ((deltaVal / Math.abs(previousBaseline)) * 100)
                : (last > first ? 50 : last < first ? -50 : 0) // Use simple trend comparison when no delta
              const toneText = textToneForPct(pct)
              const labelText = (options as any)?.legendLabelCase ? (/** same transformer as legend */ ((): string => { const s = String(c); return (options as any)?.legendLabelCase === 'lowercase' ? s.toLowerCase() : (options as any)?.legendLabelCase === 'capitalize' ? (s.toLowerCase().replace(/^(.)/, (m:any)=>m.toUpperCase())) : s.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()) : w).join(' ') })()) : String(c)
              const colorKey = colorForPct(pct)
              const colorHex = tremorNameToHex(colorKey as any)
              const gradientId = `${toDomId(chartInstanceKey)}-spark-${toDomId(String(c))}-${colorKey}`
              return (
                <div key={String(c)} className="relative" style={{ height: `${sparkRowH}px` }}>
                  {/* Header: left label/value, right delta/percent */}
                  <div className="absolute left-0 right-0 top-1 z-[2] grid grid-cols-[minmax(0,2fr)_auto] items-start gap-2">
                    <div className="min-w-0 flex flex-col items-start leading-[1.1] space-y-1">
                      <span className="text-foreground font-semibold text-[15px]" style={clampStyle}>{labelText}</span>
                      <span className="text-foreground text-[15px]">{valueFormatter(totalSum)}</span>
                    </div>
                    {showDelta ? (
                      <div className={`flex flex-col items-end leading-[1.1] space-y-1 ${toneText}`}>
                        <span className="text-[13px]">{deltaVal>0?'+':(deltaVal<0?'-':'')}{valueFormatter(Math.abs(deltaVal))}</span>
                        <span className="text-[13px]">({pct>0?'+':(pct<0?'-':'')}{Math.abs(pct).toFixed(1)}%)</span>
                      </div>
                    ) : null}
                  </div>
                  <div className="absolute inset-x-0" style={{ top: showDelta ? 40 : 32, bottom: 4 }}>
                    <ResponsiveContainer width="100%" height="100%">
                      <ReAreaChart data={data as any[]} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
                        <defs>
                          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor={colorHex} stopOpacity={0.3} />
                            <stop offset="95%" stopColor={colorHex} stopOpacity={0} />
                          </linearGradient>
                        </defs>
                        <ReYAxis hide domain={[options?.yMin ?? 'auto', options?.yMax ?? 'auto']} />
                        <ReXAxis hide dataKey="x" />
                        <ReTooltip
                          content={buildSparkTooltip(String(c), categories || [])}
                          cursor={{ stroke: colorHex, strokeOpacity: 0.2 }}
                          wrapperStyle={{ zIndex: 99999, position: 'fixed', pointerEvents: 'none' }}
                          allowEscapeViewBox={{ x: true, y: true }}
                        />
                        <ReArea
                          type="monotone"
                          dataKey={String(c)}
                          stroke={colorHex}
                          fill={`url(#${gradientId})`}
                          strokeWidth={1.5}
                          dot={false}
                          isAnimationActive={false}
                          connectNulls
                        />
                      </ReAreaChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )
            })}
          </div>
        )
      }
      // Single series spark — use same container + observer as multi for consistent resizing
      const sparkData = (data as any[]).map((row: any) => ({
        x: row.x,
        Total: (categories && categories.length > 0)
          ? (categories as string[]).reduce((acc: number, cc: string) => acc + Number(row?.[cc] ?? 0), 0)
          : Number((row as any)?.value ?? 0),
      }))
      const sums = sparkData.map((r: any) => Number(r.Total || 0))
      const first = sums.length ? Number(sums[0] || 0) : 0
      const last = sums.length ? Number(sums[sums.length - 1] || 0) : 0
      const prevVal = sums.length > 1 ? Number(sums[sums.length - 2] || 0) : first
      const totalSum = sums.reduce((a, b) => a + (Number(b) || 0), 0)
      const serverDelta = deltaFromServer('value') ?? deltaFromServer('Total')
      const deltaVal = serverDelta !== null ? serverDelta : (last - prevVal)
      const previousBaseline = serverDelta !== null ? (totalSum - serverDelta) : prevVal
      // Calculate pct from trend when no server delta is available
      const pct = (serverDelta !== null && previousBaseline !== 0)
        ? ((deltaVal / Math.abs(previousBaseline)) * 100)
        : (last > first ? 50 : last < first ? -50 : 0) // Use simple trend comparison when no delta
      const toneText = textToneForPct(pct)
      const colorKey = colorForPct(pct)
      const colorHex = tremorNameToHex(colorKey as any)
      const gradientId = `${toDomId(chartInstanceKey)}-spark-total-${colorKey}`
      return (
        <div className="absolute inset-0 space-y-2" ref={sparkAreaRef}>
          <div className="relative" style={{ height: `${sparkRowH}px` }}>
            {/* Header */}
            <div className="absolute left-0 right-0 top-1 z-[2] grid grid-cols-[minmax(0,2fr)_auto] items-start gap-2">
              <div className="min-w-0 flex flex-col items-start leading-[1.1] space-y-1">
                <span className="text-foreground font-semibold text-[15px]" style={clampStyle}>Total</span>
                <span className="text-foreground text-[15px]">{valueFormatter(totalSum)}</span>
              </div>
              {showDelta ? (
                <div className={`flex flex-col items-end leading-[1.1] space-y-1 ${toneText}`}>
                  <span className="text-[13px]">{deltaVal>0?'+':(deltaVal<0?'-':'')}{valueFormatter(Math.abs(deltaVal))}</span>
                  <span className="text-[13px]">({pct>0?'+':(pct<0?'-':'')}{Math.abs(pct).toFixed(1)}%)</span>
                </div>
              ) : null}
            </div>
            <div className="absolute inset-x-0" style={{ top: showDelta ? 40 : 32, bottom: 4 }}>
              <ResponsiveContainer width="100%" height="100%">
                <ReAreaChart data={sparkData} margin={{ top: 0, right: 0, left: 0, bottom: 0 }}>
                  <defs>
                    <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%" stopColor={colorHex} stopOpacity={0.3} />
                      <stop offset="95%" stopColor={colorHex} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <ReYAxis hide domain={[options?.yMin ?? 'auto', options?.yMax ?? 'auto']} />
                  <ReXAxis hide dataKey="x" />
                  <ReTooltip
                    content={buildSparkTooltip('Total', categories || ['Total'])}
                    cursor={{ stroke: colorHex, strokeOpacity: 0.2 }}
                    wrapperStyle={{ zIndex: 99999, position: 'fixed', pointerEvents: 'none' }}
                    allowEscapeViewBox={{ x: true, y: true }}
                  />
                  <ReArea
                    type="monotone"
                    dataKey="Total"
                    stroke={colorHex}
                    fill={`url(#${gradientId})`}
                    strokeWidth={1.5}
                    dot={false}
                    isAnimationActive={false}
                    connectNulls
                  />
                </ReAreaChart>
              </ResponsiveContainer>
            </div>
          </div>
        </div>
      )
    }
    if (type === 'bar' || type === 'column') {
      // Non-advanced path: pre-format X labels for Tremor/Recharts
      const inferredTimeFmt = (() => {
        try {
          const first = (Array.isArray(displayData) ? (displayData as any[]) : []).map(r=>parseDateLoose(String(r?.x??''))).find((dd:any)=>!!dd) as Date|undefined
          if (first && (first.getHours()!==0 || first.getMinutes()!==0 || first.getSeconds()!==0)) return 'h:mm a'
        } catch {}
        return undefined
      })()
      const dateFmt = (options as any)?.xDateFormat
        || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
          : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
          : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
          : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
          : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
          : inferredTimeFmt)
      const formatX = (x: any) => {
        if (!dateFmt) return x
        const s = String(x ?? '')
        const d = parseDateLoose(s)
        if (!d) return s
        const pad = (n: number) => String(n).padStart(2, '0')
        const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
        const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
        switch (dateFmt) {
          case 'YYYY': return String(d.getFullYear())
          case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
          case 'YYYY-[W]ww': {
            const jan1 = new Date(d.getFullYear(), 0, 1)
            const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000)
            const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1
            const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun')
            const wn = useSun ? wnSun : isoWeek(d)
            return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}`
          }
          case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
          case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
          case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
          case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
          case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
          case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
          default: return formatDatePattern(d, String(dateFmt))
        }
      }
      const displayData2 = Array.isArray(displayData) ? (displayData as any[]).map((r) => ({ ...r, x: formatX(r?.x) })) : displayData
      // Safety: filter categories and sanitize row values to never pass NaN to Tremor/Recharts
      const catsSafe: string[] = Array.isArray(categories)
        ? (categories as string[]).map((c) => String(c ?? '')).filter((s) => s.trim() !== '')
        : ([] as string[])
      const cleanData: any[] = Array.isArray(displayData2)
        ? (displayData2 as any[]).map((row) => {
            const out: any = { ...row }
            catsSafe.forEach((c) => {
              const v: any = out[c]
              if (v === null || v === undefined || v === '') { out[c] = 0; return }
              const n = Number(v)
              out[c] = Number.isFinite(n) ? n : 0
            })
            return out
          })
        : ([] as any[])
      const rowCats = (() => { try { const r = cleanData?.[0] || {}; return Object.keys(r).filter((k)=>k!=='x' && k!=='value') } catch { return [] as string[] } })()
      const inter = new Set<string>(rowCats.filter((k)=>catsSafe.includes(k)))
      const catsEff = (inter.size === 0 && rowCats.length > 0) ? rowCats : catsSafe
      const yMinSafe = (() => { const n = Number((options as any)?.yMin); return Number.isFinite(n) ? n : undefined })()
      const yMaxSafe = (() => { const n = Number((options as any)?.yMax); return Number.isFinite(n) ? n : undefined })()
      // Compute y-axis label width for horizontal bars to avoid clipping
      const yFontSize = Math.max(8, Number(((options as any)?.yAxisFontSize ?? 11)))
      const labelStrings = Array.isArray(displayData2) ? (displayData2 as any[]).map((r) => String(r?.x ?? '')) : []
      const maxLabelLen = Math.max(0, ...labelStrings.map((s) => s.length))
      const approxCharW = yFontSize * 0.6
      const computedYAxisWidth = Math.min(240, Math.max(40, Math.round(maxLabelLen * approxCharW) + 12))
      const yAxisWidthPx = type === 'bar' ? (Number((options as any)?.yAxisWidth) || computedYAxisWidth) : 40
      return (
        <div className="absolute inset-0 chart-axis-scope chart-grid-scope" style={{ ['--x-axis-color' as any]: ((options as any)?.xAxisFontColor || axisTextColor), ['--y-axis-color' as any]: ((options as any)?.yAxisFontColor || axisTextColor), ['--x-axis-weight' as any]: ((options as any)?.xAxisFontWeight === 'bold' ? 'bold' : 'normal'), ['--y-axis-weight' as any]: ((options as any)?.yAxisFontWeight === 'bold' ? 'bold' : 'normal'), ['--x-axis-size' as any]: `${(options as any)?.xAxisFontSize ?? 11}px`, ['--y-axis-size' as any]: `${(options as any)?.yAxisFontSize ?? 11}px`, ...(buildTremorGridStyle() as any) } as any}>
          <BarChart
          key={chartInstanceKey}
          data={cleanData}
          index="x"
          categories={catsEff}
          colors={chartColorsTokens}
          showLegend={chartLegend}
          yAxisWidth={yAxisWidthPx}
          valueFormatter={(v: number) => valueFormatter(v)}
          minValue={yMinSafe}
          maxValue={yMaxSafe}
          className="h-full text-[11px]"
          {...(type === 'bar' ? ({ layout: 'vertical' } as any) : {})}
          customTooltip={options?.richTooltip ? renderTooltip : undefined}
          startEndOnly={denseX}
          />
        </div>
      )
    }
    if (type === 'area') {
      const inferredTimeFmt = (() => {
        try {
          const first = (Array.isArray(displayData) ? (displayData as any[]) : []).map(r=>parseDateLoose(String(r?.x??''))).find((dd:any)=>!!dd) as Date|undefined
          if (first && (first.getHours()!==0 || first.getMinutes()!==0 || first.getSeconds()!==0)) return 'h:mm a'
        } catch {}
        return undefined
      })()
      const dateFmt = (options as any)?.xDateFormat
        || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
          : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
          : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
          : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
          : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
          : inferredTimeFmt)
      const formatX = (x: any) => {
        if (!dateFmt) return x
        const s = String(x ?? '')
        const d = parseDateLoose(s)
        if (!d) return s
        const pad = (n: number) => String(n).padStart(2, '0')
        const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
        const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
        switch (dateFmt) {
          case 'YYYY': return String(d.getFullYear())
          case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
          case 'YYYY-[W]ww': {
            const jan1 = new Date(d.getFullYear(), 0, 1)
            const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000)
            const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1
            const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun')
            const wn = useSun ? wnSun : isoWeek(d)
            return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}`
          }
          case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
          case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
          case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
          case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
          case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
          case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
          default: return formatDatePattern(d, String(dateFmt))
        }
      }
      const displayData2 = Array.isArray(displayData) ? (displayData as any[]).map((r) => ({ ...r, x: formatX(r?.x) })) : displayData
      // Safety: sanitize categories and data (avoid NaN/null in Tremor)
      const catsSafe: string[] = Array.isArray(categories)
        ? (categories as string[]).map((c) => String(c ?? '')).filter((s) => s.trim() !== '')
        : ([] as string[])
      const cleanData: any[] = Array.isArray(displayData2)
        ? (displayData2 as any[]).map((row) => {
            const out: any = { ...row }
            catsSafe.forEach((c) => {
              const v: any = out[c]
              // For area charts, preserve null for missing data so lines stop instead of dropping to zero
              if (v === null || v === undefined || v === '') { out[c] = null; return }
              const n = Number(v)
              out[c] = Number.isFinite(n) ? n : null
            })
            return out
          })
        : ([] as any[])
      const yMinSafe = (() => { const n = Number((options as any)?.yMin); return Number.isFinite(n) ? n : undefined })()
      const yMaxSafe = (() => { const n = Number((options as any)?.yMax); return Number.isFinite(n) ? n : undefined })()
      // Calculate Y-axis width based on maximum Y value to prevent label cutoff
      const yAxisWidthArea = (() => {
        try {
          const allYValues: number[] = []
          if (Array.isArray(cleanData) && Array.isArray(catsSafe)) {
            cleanData.forEach((row: any) => {
              catsSafe.forEach((cat: string) => {
                const val = Number(row?.[cat] ?? 0)
                if (!isNaN(val)) allYValues.push(Math.abs(val))
              })
            })
          }
          if (allYValues.length === 0) return 60
          const maxVal = Math.max(...allYValues)
          const formatted = valueFormatter(maxVal)
          const estimatedWidth = Math.ceil((formatted.length * 8) + 20)
          return Math.max(estimatedWidth, 60)
        } catch {
          return 60
        }
      })()
      return (
        <div className="absolute inset-0 chart-axis-scope chart-grid-scope" style={{ ['--x-axis-color' as any]: ((options as any)?.xAxisFontColor || axisTextColor), ['--y-axis-color' as any]: ((options as any)?.yAxisFontColor || axisTextColor), ['--x-axis-weight' as any]: ((options as any)?.xAxisFontWeight === 'bold' ? 'bold' : 'normal'), ['--y-axis-weight' as any]: ((options as any)?.yAxisFontWeight === 'bold' ? 'bold' : 'normal'), ['--x-axis-size' as any]: `${(options as any)?.xAxisFontSize ?? 11}px`, ['--y-axis-size' as any]: `${(options as any)?.yAxisFontSize ?? 11}px`, ...(buildTremorGridStyle() as any) } as any}>
          <AreaChart
          key={chartInstanceKey}
          data={cleanData}
          index="x"
          categories={catsSafe}
          colors={chartColorsTokens}
          showLegend={chartLegend}
          yAxisWidth={yAxisWidthArea}
          curveType="monotone"
          valueFormatter={(v: number) => valueFormatter(v)}
          minValue={yMinSafe}
          maxValue={yMaxSafe}
          className="h-full text-[11px]"
          customTooltip={options?.richTooltip ? renderTooltip : undefined}
          startEndOnly={denseX}
          />
        </div>
      )
    }
    if (type === 'sankey') {
      // Transform data for Sankey diagram: expects { nodes: [], links: [] }
      // Data comes in long format from the pivot query
      // Column structure: [row_dimension, column_dimension, value]
      // For pivots: [FromClientCode, ToClientCode, value] or [x, legend, value]
      const cols: string[] = ((q.data as any)?.columns as string[]) || []
      const rowsArr: any[] = Array.isArray(data) ? (data as any[]) : []
      
      // Try to find standardized column names first ('x', 'legend', 'value')
      let sourceIdx = cols.indexOf('x')
      let targetIdx = cols.indexOf('legend')
      let valueIdx = cols.indexOf('value')
      
      // If standardized names not found, use column positions
      // (First col = source, second col = target, third col = value)
      if (sourceIdx === -1 || targetIdx === -1 || valueIdx === -1) {
        if (cols.length >= 3) {
          sourceIdx = 0
          targetIdx = 1
          valueIdx = 2
          console.log('[Sankey] Using column positions:', { source: cols[0], target: cols[1], value: cols[2] })
        } else {
          return <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
            Sankey requires X-axis (source) and Legend (target) to be configured in the pivot builder
          </div>
        }
      }
      
      console.log('[Sankey] Data structure:', { cols, rowCount: rowsArr.length, sourceIdx, targetIdx, valueIdx })
      console.log('[Sankey] First 3 rows:', rowsArr.slice(0, 3))
      if (rowsArr.length > 0 && rowsArr[0] && typeof rowsArr[0] === 'object') {
        console.log('[Sankey] Column keys in first row:', Object.keys(rowsArr[0]))
        console.log('[Sankey] First row values:', rowsArr[0])
        console.log('[Sankey] Sample values:', {
          x: rowsArr[0].x,
          firstKey: Object.keys(rowsArr[0])[1],
          firstValue: rowsArr[0][Object.keys(rowsArr[0])[1]],
          firstValueType: typeof rowsArr[0][Object.keys(rowsArr[0])[1]]
        })
      }
      
      // Collect all flows (including bidirectional)
      const allFlows: Array<{ source: string; target: string; value: number }> = []
      
      // Handle pivoted data format where each row has x and multiple category columns
      rowsArr.forEach((row: any) => {
        if (Array.isArray(row)) {
          // Array format: [source, target, value]
          const source = String(row[sourceIdx] || '')
          const target = String(row[targetIdx] || '')
          const value = Number(row[valueIdx] || 0)
          
          // Allow self-loops (source === target) for intra-region flows
          if (source && target && value > 0) {
            allFlows.push({ source, target, value })
          }
        } else if (row && typeof row === 'object') {
          // Object format: {x: 'source', 'target1 - measure': value1, 'target2 - measure': value2, ...}
          const source = String(row.x || '')
          if (!source) return
          
          // Iterate through all keys except 'x' to find target-value pairs
          Object.keys(row).forEach(key => {
            if (key === 'x') return
            
            const value = Number(row[key])
            if (!value || value <= 0 || !Number.isFinite(value)) return
            
            // Extract target name - handle various formats:
            // 'WB - OrderUID' -> 'WB'
            // 'WB - sum(amount)' -> 'WB'
            // 'WB' -> 'WB'
            let target = key
            if (key.includes(' - ')) {
              target = key.split(' - ')[0]
            } else if (key.includes('-')) {
              target = key.split('-')[0].trim()
            }
            
            // Allow self-loops for intra-region flows
            allFlows.push({ source, target, value })
          })
        }
      })
      
      // For bidirectional flows, split nodes into "From" and "To" groups
      // This allows showing both A→B and B→A without creating cycles
      const sourceNodes = new Set<string>()
      const targetNodes = new Set<string>()
      
      allFlows.forEach(flow => {
        sourceNodes.add(flow.source)
        targetNodes.add(flow.target)
      })
      
      // Create node list with suffixes to distinguish source/target sides
      const nodes: Array<{ name: string }> = []
      sourceNodes.forEach(name => nodes.push({ name: `${name} (From)` }))
      targetNodes.forEach(name => nodes.push({ name: `${name} (To)` }))
      
      // Transform links to use the suffixed node names
      const finalLinks = allFlows.map(flow => ({
        source: `${flow.source} (From)`,
        target: `${flow.target} (To)`,
        value: flow.value
      }))
      
      console.log('[Sankey] Built data:', { nodeCount: nodes.length, linkCount: finalLinks.length, nodes: nodes.slice(0, 10), links: finalLinks.slice(0, 5) })
      
      // Validate data before rendering to prevent ECharts errors
      if (nodes.length === 0 || finalLinks.length === 0) {
        return <div className="flex items-center justify-center h-full text-sm text-muted-foreground">
          {q.isLoading ? 'Loading...' : 'No data available for Sankey chart'}
        </div>
      }
      
      const hexColors = legendHexColors || (chartColorsTokens || [])
      const vf = (n: number) => valueFormatter(n)
      const showLabels = (options?.dataLabelsShow ?? true)
      const labelPosition = ((options?.dataLabelPosition as any) || 'right') as 'left' | 'right' | 'top' | 'bottom'
      const orient = ((options as any)?.sankeyOrient || 'horizontal') as 'horizontal' | 'vertical'
      const nodeWidth = Number((options as any)?.sankeyNodeWidth ?? 20)
      const nodeGap = Number((options as any)?.sankeyNodeGap ?? 8)
      
      return renderSankey({
        chartInstanceKey,
        data: { nodes, links: finalLinks },
        colors: hexColors as any,
        valueFormatterAction: vf,
        orient,
        nodeWidth,
        nodeGap,
        showLabels,
        labelPosition,
        echartsRef
      })
    }
    if (type === 'donut') {
      // Unified sums via calcEngine (reuse existing shaping)
      const rowsArr: any[] = Array.isArray(displayData)
        ? (displayData as any[])
        : (Array.isArray((q.data as any)?.rows) ? ((q.data as any).rows as any[]) : [])
      const cats = (categories && categories.length > 0) ? (categories as string[]) : (['value'] as string[])
      const ctx = buildTimelineContext(rowsArr, cats, { yMax: (options?.yMax as any) })
      const labelsUsed = ctx.labels
      const rowsAligned = labelsUsed.map((l) => ctx.rowsByLabel[l])
      const sumsByCat = aggregateCategoriesAdvanced(rowsAligned, cats, 'sum')
      const pieData = cats.map((c) => ({ name: c, value: Number(sumsByCat[c] || 0) }))
      const previewData = (options as any)?.previewPieData as Array<{ name: string; value: number }> | undefined
      const dataForChart = (Array.isArray(previewData) && previewData.length > 0) ? previewData : pieData
      // Colors: prefer hex palette used in legend
      const hexColors = legendHexColors || (chartColorsTokens || [])
      const variant = (options?.donutVariant || 'donut') as 'donut'|'pie'|'sunburst'|'nightingale'
      const vf = (n: number) => valueFormatter(n)
      const showLabels = !!options?.dataLabelsShow
      const labelPosition = (options?.dataLabelPosition as any) || 'outsideEnd'
      if (variant === 'pie') return renderEchartsPie({ chartInstanceKey, data: dataForChart, colors: hexColors as any, valueFormatterAction: vf, showLabels, labelPosition, echartsRef })
      if (variant === 'sunburst') return renderEchartsSunburst({ chartInstanceKey, data: dataForChart, colors: hexColors as any, valueFormatterAction: vf, showLabels, echartsRef })
      if (variant === 'nightingale') return renderEchartsNightingale({ chartInstanceKey, data: dataForChart, colors: hexColors as any, valueFormatterAction: vf, showLabels, labelPosition, echartsRef })
      return renderEchartsDonut({ chartInstanceKey, data: dataForChart, colors: hexColors as any, valueFormatterAction: vf, showLabels, labelPosition, echartsRef })
    }

    // Default: line chart
    const dateFmt = (options as any)?.xDateFormat
      || (((querySpec as any)?.groupBy === 'year') ? 'YYYY'
        : ((querySpec as any)?.groupBy === 'quarter') ? 'YYYY-[Q]q'
        : ((querySpec as any)?.groupBy === 'month') ? 'MMM-YYYY'
        : ((querySpec as any)?.groupBy === 'week') ? 'YYYY-[W]ww'
        : ((querySpec as any)?.groupBy === 'day') ? 'YYYY-MM-DD'
        : undefined)
    const formatX = (x: any) => {
      if (!dateFmt) return x
      const s = String(x ?? '')
      const d = parseDateLoose(s)
      if (!d) return s
      const pad = (n: number) => String(n).padStart(2, '0')
      const isoWeek = (date: Date) => { const _d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate())); _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(_d.getUTCFullYear(),0,1)); return Math.ceil((((_d.getTime()-yearStart.getTime())/86400000)+1)/7) }
      const quarter = (date: Date) => (Math.floor(date.getMonth()/3)+1)
      switch (dateFmt) {
        case 'YYYY': return String(d.getFullYear())
        case 'YYYY-[Q]q': return `${d.getFullYear()}-Q${quarter(d)}`
        case 'YYYY-[W]ww': {
          const jan1 = new Date(d.getFullYear(), 0, 1)
          const day0 = Math.floor((new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime() - jan1.getTime()) / 86400000)
          const wnSun = Math.floor((day0 + jan1.getDay()) / 7) + 1
          const useSun = (((options as any)?.xWeekStart || (querySpec as any)?.weekStart || 'mon') === 'sun')
          const wn = useSun ? wnSun : isoWeek(d)
          return `${d.getFullYear()}-W${String(wn).padStart(2,'0')}`
        }
        case 'YYYY-MM': return `${d.getFullYear()}-${pad(d.getMonth()+1)}`
        case 'YYYY-MM-DD': return `${d.getFullYear()}-${pad(d.getMonth()+1)}-${pad(d.getDate())}`
        case 'h:mm a': { let h = d.getHours(); const m = pad(d.getMinutes()); const am = h < 12; h = h % 12 || 12; return `${h}:${m} ${am ? 'AM' : 'PM'}` }
        case 'dddd': return d.toLocaleDateString('en-US', { weekday: 'long' })
        case 'MMMM': return d.toLocaleDateString('en-US', { month: 'long' })
        case 'MMM-YYYY': return d.toLocaleDateString('en-US', { month: 'short', year: 'numeric' }).replace(' ', '-')
        default: return formatDatePattern(d, String(dateFmt))
      }
    }
    const displayData2 = Array.isArray(displayData) ? (displayData as any[]).map((r) => ({ ...r, x: formatX(r?.x) })) : displayData
    // Calculate Y-axis width based on maximum Y value to prevent label cutoff
    const yAxisWidthLine = (() => {
      try {
        const allYValues: number[] = []
        if (Array.isArray(displayData2) && Array.isArray(categories)) {
          displayData2.forEach((row: any) => {
            categories.forEach((cat: string) => {
              const val = Number(row?.[cat] ?? 0)
              if (!isNaN(val)) allYValues.push(Math.abs(val))
            })
          })
        }
        if (allYValues.length === 0) return 60
        const maxVal = Math.max(...allYValues)
        const formatted = valueFormatter(maxVal)
        const estimatedWidth = Math.ceil((formatted.length * 8) + 20)
        const calculatedWidth = Math.max(estimatedWidth, 60)
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          console.log('[ChartCard] [Tremor LineChart] Y-axis width calculation:', { maxVal, formatted, charCount: formatted.length, estimatedWidth, calculatedWidth })
        }
        return calculatedWidth
      } catch (e) {
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          console.error('[ChartCard] [Tremor LineChart] Y-axis width calculation error:', e)
        }
        return 60
      }
    })()
    return (
      <div className="absolute inset-0 chart-axis-scope chart-grid-scope" style={{ ['--x-axis-color' as any]: ((options as any)?.xAxisFontColor || axisTextColor), ['--y-axis-color' as any]: ((options as any)?.yAxisFontColor || axisTextColor), ['--x-axis-weight' as any]: ((options as any)?.xAxisFontWeight === 'bold' ? 'bold' : 'normal'), ['--y-axis-weight' as any]: ((options as any)?.yAxisFontWeight === 'bold' ? 'bold' : 'normal'), ['--x-axis-size' as any]: `${(options as any)?.xAxisFontSize ?? 11}px`, ['--y-axis-size' as any]: `${(options as any)?.yAxisFontSize ?? 11}px`, ...(buildTremorGridStyle() as any) } as any}>
      {(() => {
        // Sanitize categories and data for Tremor LineChart
        const catsSafe: string[] = Array.isArray(categories)
          ? (categories as string[]).map((c) => String(c ?? '')).filter((s) => s.trim() !== '')
          : ([] as string[])
        const yMinSafe = (() => { const n = Number((options as any)?.yMin); return Number.isFinite(n) ? n : undefined })()
        const yMaxSafe = (() => { const n = Number((options as any)?.yMax); return Number.isFinite(n) ? n : undefined })()
        const displayData2 = Array.isArray(displayData)
          ? (displayData as any[]).map((r) => ({ ...r, x: (r?.x ?? r?.x === 0) ? r.x : r?.x }))
          : ([] as any[])
        const cleanData = Array.isArray(displayData2)
          ? (displayData2 as any[]).map((row) => {
              const out: any = { ...row }
              catsSafe.forEach((c) => {
                const v: any = out[c]
                // For line charts, preserve null for missing data so lines stop instead of dropping to zero
                if (v === null || v === undefined || v === '') { out[c] = null; return }
                const n = Number(v)
                out[c] = Number.isFinite(n) ? n : null
              })
              return out
            })
          : ([] as any[])
        const rowCats = (() => { try { const r = cleanData?.[0] || {}; return Object.keys(r).filter((k)=>k!=='x' && k!=='value') } catch { return [] as string[] } })()
        const inter = new Set<string>(rowCats.filter((k)=>catsSafe.includes(k)))
        const catsEff = (inter.size === 0 && rowCats.length > 0) ? rowCats : catsSafe
        return (
          <LineChart
            key={chartInstanceKey}
            data={cleanData}
            index="x"
            categories={catsEff}
            colors={chartColorsTokens}
            showLegend={chartLegend}
            yAxisWidth={yAxisWidthLine}
            curveType="monotone"
            valueFormatter={(v: number) => valueFormatter(v)}
            minValue={yMinSafe}
            maxValue={yMaxSafe}
            className="h-full text-[11px]"
            customTooltip={options?.richTooltip ? renderTooltip : undefined}
            startEndOnly={denseX}
          />
        )
      })()}
      </div>
    )
  })()

  if (typeof window !== 'undefined') {
    try {
      // eslint-disable-next-line no-console
      console.log('[ChartCard] COLOR ASSIGNMENT:', {
        categories,
        isMulti,
        chartColorsTokens,
        legendHexColors,
        paletteSize: getPresetPalette(options?.colorPreset as any).length,
        mapping: categories?.map((cat, i) => ({ category: cat, token: chartColorsTokens[i], hex: legendHexColors[i] }))
      })
    } catch {}
  }

  // Nested legend expand/collapse state (per base series label)
  const [legendOpen, setLegendOpen] = useState<Record<string, boolean>>({})
  const renderLegend = () => {
    const wantLegend = (options?.showLegend ?? true)
    const pos = options?.legendPosition ?? 'bottom'
    // Only hide legend for spark and badges; allow legend for categoryBar, progress, tracker
    if (!wantLegend || pos === 'none' || type === 'spark' || type === 'badges' || type === 'tremorTable') return null
    const maxItems = options?.maxLegendItems && options.maxLegendItems > 0 ? options.maxLegendItems : undefined
    const shownCats = maxItems ? categories.slice(0, maxItems) : categories
    const more = (categories?.length ?? 0) - (shownCats?.length ?? 0)
    const dotShape = options?.legendDotShape || 'square'
    const swatchClass = dotShape === 'circle' ? 'w-2 h-2 rounded-full' : dotShape === 'rect' ? 'w-3 h-2 rounded-[2px]' : 'w-2 h-2 rounded-[2px]'
    const applyCase = (s: string) => {
      const mode = (options as any)?.legendLabelCase as 'lowercase'|'capitalize'|'uppercase'|'capitalcase'|'proper' | undefined
      const str = String(s ?? '')
      if (!mode) return str
      switch (mode) {
        case 'lowercase': return str.toLowerCase()
        case 'uppercase':
        case 'capitalcase': return str.toUpperCase()
        case 'capitalize': {
          const lower = str.toLowerCase()
          return lower.length ? (lower[0].toUpperCase() + lower.slice(1)) : lower
        }
        case 'proper': default:
          return str.replace(/[_-]+/g, ' ').split(/\s+/).map(w => w ? (w[0].toUpperCase() + w.slice(1).toLowerCase()) : w).join(' ')
      }
    }
    // Local helpers for legend labels: drop series prefix if single series and apply per-category casing
    const isSingleSeriesLegend = Array.isArray(series) ? (series.length <= 1) : true
    const formatCategoryCaseLegend = (name: string): string => {
      const str = String(name ?? '')
      const map = (((options as any)?.categoryLabelCaseMap) || {}) as Record<string, string>
      const mode = (map[str] || (options as any)?.categoryLabelCase) as ('lowercase'|'capitalize'|'uppercase'|'capitalcase'|'proper'|undefined)
      if (!mode) return str
      switch (mode) {
        case 'lowercase': return str.toLowerCase()
        case 'uppercase':
        case 'capitalcase': return str.toUpperCase()
        case 'capitalize': { const lower = str.toLowerCase(); return lower.length ? (lower[0].toUpperCase()+lower.slice(1)) : lower }
        case 'proper': default: return str.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w?(w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
      }
    }
    const displayLabel = (raw: string): string => {
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
        if (isSingleSeriesLegend) return formatCategoryCaseLegend(cat)
        return `${applyCase(base)} • ${formatCategoryCaseLegend(cat)}`
      }
      return applyCase(s)
    }
    // Nested legend mode for virtual categories: group by base series label
    const nested = (options as any)?.legendMode === 'nested'
    const vmeta = (((q?.data as any)?.virtualMeta) || null) as Record<string, { baseSeriesLabel: string }> | null
    if (nested && vmeta && Array.isArray(categories) && categories.length > 0) {
      const groups = new Map<string, string[]>()
      ;(categories as string[]).forEach((c) => {
        const base = vmeta[String(c)]?.baseSeriesLabel || String(c).split(' • ')[0]
        const arr = groups.get(base) || []
        arr.push(String(c))
        groups.set(base, arr)
      })
      return (
        <div className="flex flex-col items-center gap-3 text-[11px]">
          {Array.from(groups.entries()).map(([base, childs], gi) => {
            const firstIdx = Math.max(0, (categories as string[]).indexOf(childs[0] || ''))
            const baseColor = legendHexColors[firstIdx % legendHexColors.length]
            const isOpen = legendOpen[base] !== false
            return (
              <div key={`grp-${gi}`} className="flex flex-col items-center gap-1">
                <button type="button" className="flex items-center gap-2 hover:opacity-90" onClick={() => setLegendOpen((m) => ({ ...m, [base]: !isOpen }))}>
                  <span className={`transition-transform ${isOpen ? '' : '-rotate-90'}`}>
                    <RiArrowDownSLine size={14} />
                  </span>
                  <span className={`inline-block ${swatchClass}`} style={{ backgroundColor: baseColor as any }} />
                  <span className="text-foreground font-medium">{applyCase(base)}</span>
                </button>
                {isOpen && (
                  <div className="flex flex-wrap items-center justify-center gap-3">
                    {childs.map((c) => {
                      const idx = Math.max(0, (categories as string[]).indexOf(String(c)))
                      const col = legendHexColors[idx % legendHexColors.length]
                      const label = displayLabel(String(c))
                      return (
                        <div key={String(c)} className="flex items-center gap-2">
                          <span className={`inline-block ${swatchClass}`} style={{ backgroundColor: col as any }} />
                          <span className="text-muted-foreground">{label}</span>
                        </div>
                      )
                    })}
                  </div>
                )}
              </div>
            )
          })}
          {more > 0 && <span className="text-muted-foreground">+{more} more</span>}
        </div>
      )
    }
    // Flat legend fallback
    return (
      <div className={`flex flex-wrap items-center justify-center gap-3 text-[11px]`}>
        {shownCats.map((c, i) => (
          <div key={String(c)} className="flex items-center gap-2">
            <span className={`inline-block ${swatchClass}`} style={{ backgroundColor: legendHexColors[i % legendHexColors.length] as any }} />
            <span className="text-muted-foreground">{displayLabel(String(c))}</span>
          </div>
        ))}
        {more > 0 && <span className="text-muted-foreground">+{more} more</span>}
      </div>
    )
  }

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

  

  const autoFit = options?.autoFitCardContent !== false
  const cardFill = options?.cardFill || 'default'
  const bgStyle = cardFill === 'transparent' ? { backgroundColor: 'transparent' } : cardFill === 'custom' ? { backgroundColor: options?.cardCustomColor || '#ffffff' } : undefined
  const cardClass = [
    'h-full',
    'flex', 'flex-col', 'min-h-0',
    'rounded-lg',
    'relative',
    cardFill === 'transparent' ? 'bg-transparent' : 'bg-card',
  ].join(' ')

  // Measure-only layout: header + legend + in-body titles are flex-none; chart height = container - reserved
  const titleAboveRef = useRef<HTMLDivElement | null>(null)
  const titleBelowRef = useRef<HTMLDivElement | null>(null)
  const [chartBoxH, setChartBoxH] = useState<number | undefined>(undefined)
  useLayoutEffect(() => {
    if (layout !== 'measure') { setChartBoxH(undefined); return }
    const measure = () => {
      try {
        const el = containerRef.current
        if (!el) return
        const total = el.clientHeight
        const withMargins = (node: HTMLElement | null | undefined) => {
          if (!node) return 0
          const cs = getComputedStyle(node)
          const mt = parseFloat(cs.marginTop || '0') || 0
          const mb = parseFloat(cs.marginBottom || '0') || 0
          return (node.offsetHeight || 0) + mt + mb
        }
        let reserved = 0
        reserved += Number(reservedTop || 0)
        reserved += withMargins(headerUIRef.current || null)
        // Account for in-body titles (above/below)
        reserved += withMargins(titleAboveRef.current || null)
        reserved += withMargins(titleBelowRef.current || null)
        const wantLegend = (options?.showLegend ?? true)
        if (wantLegend) {
          const pos = options?.legendPosition ?? 'bottom'
          if (pos === 'top') reserved += withMargins(legendTopRef.current || null)
          else reserved += withMargins(legendBottomRef.current || null)
        }
        const computed = Math.max(0, total - reserved)
        setChartBoxH(computed)
        if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
          try {
            console.debug('[ChartCard] measure', {
              total,
              header: withMargins(headerUIRef.current || null),
              legend: (options?.legendPosition ?? 'bottom') === 'top' ? withMargins(legendTopRef.current || null) : withMargins(legendBottomRef.current || null),
              titleAbove: withMargins(titleAboveRef.current || null),
              titleBelow: withMargins(titleBelowRef.current || null),
              reservedTop,
              computed,
            })
          } catch {}
        }
      } catch {}
    }
    // after paint for accurate offsetHeight
    if (typeof window !== 'undefined') requestAnimationFrame(() => requestAnimationFrame(measure))
    const ro = new ResizeObserver(measure)
    if (containerRef.current) ro.observe(containerRef.current)
    if (headerUIRef.current) ro.observe(headerUIRef.current)
    if (legendTopRef.current) ro.observe(legendTopRef.current)
    if (legendBottomRef.current) ro.observe(legendBottomRef.current)
    if (titleAboveRef.current) ro.observe(titleAboveRef.current)
    if (titleBelowRef.current) ro.observe(titleBelowRef.current)
    const onResize = () => measure()
    if (typeof window !== 'undefined') window.addEventListener('resize', onResize)
    return () => {
      try { ro.disconnect() } catch {}
      if (typeof window !== 'undefined') window.removeEventListener('resize', onResize)
    }
  }, [
    layout,
    reservedTop,
    options?.legendPosition,
    options?.showLegend,
    options?.maxLegendItems,
    (categories || []).join('|'),
    JSON.stringify(fieldsExposed),
    // Re-measure when title formatting that can affect size changes
    (options as any)?.chartTitlePosition,
    (options as any)?.chartTitleSize,
    (options as any)?.chartTitleMargin,
    (options as any)?.chartTitleOutline,
    (options as any)?.chartTitleGapTop,
    (options as any)?.chartTitleGapBottom,
    (options as any)?.chartTitleGapLeft,
    (options as any)?.chartTitleGapRight,
    (options as any)?.chartTitleBgMode,
    (options as any)?.chartTitleBgColor,
  ])

  // Extra padding to avoid clipping when labels are outside or ticks are rotated
  const extraPadTop = ((options as any)?.dataLabelsShow && ((options as any)?.dataLabelPosition === 'outsideEnd')) ? 12 : 0
  const hasZoomSlider = ((['line','area','column','combo','scatter'] as const).includes(type as any) && ((((options as any)?.zoomPan) || ((options as any)?.dataZoom) || ((options as any)?.largeScale) || ((options as any)?.areaLargeScale))))
  const extraPadBottom = (Math.abs(Number((options as any)?.xTickAngle ?? 0)) >= 60 ? 20 : 0) + (hasZoomSlider ? 48 : 0)
  // Chart title options
  const chartTitlePosition = (options as any)?.chartTitlePosition || 'none'
  const chartTitleAlign = (options as any)?.chartTitleAlign || 'left'
  const chartTitleSize = (options as any)?.chartTitleSize ?? 13
  const chartTitleEmphasis = (options as any)?.chartTitleEmphasis || 'normal' // 'normal' | 'bold' | 'italic' | 'underline'
  const chartTitleColorMode = (options as any)?.chartTitleColorMode || 'auto' // 'auto' | 'custom'
  const chartTitleColorCustom = (options as any)?.chartTitleColor || undefined
  const chartTitleMargin = (options as any)?.chartTitleMargin || 'sm' // 'none'|'sm'|'md'|'lg'
  const chartTitleOutline = !!((options as any)?.chartTitleOutline)
  const chartTitleBgMode = (options as any)?.chartTitleBgMode || 'none' // 'none' | 'custom'
  const chartTitleBgColor = (options as any)?.chartTitleBgColor || undefined
  // Gap adjusters: top/bottom used as outer margins; left/right used as extra inner padding
  const chartTitleGapTop = Number((options as any)?.chartTitleGapTop ?? 0)
  const chartTitleGapRight = Number((options as any)?.chartTitleGapRight ?? 0)
  const chartTitleGapBottom = Number((options as any)?.chartTitleGapBottom ?? 0)
  const chartTitleGapLeft = Number((options as any)?.chartTitleGapLeft ?? 0)
  const titlePad = chartTitleMargin === 'none' ? 0 : chartTitleMargin === 'sm' ? 4 : chartTitleMargin === 'md' ? 8 : 12
  const isDark = (() => { try { return typeof window !== 'undefined' && document.documentElement.classList.contains('dark') } catch { return false } })()
  const chartTitleResolvedColor = (chartTitleColorMode === 'custom')
    ? (chartTitleColorCustom || (isDark ? '#FFFFFF' : '#111827'))
    : (isDark ? '#FFFFFF' : '#111827')
  const chartTitleStyle: React.CSSProperties = {
    fontSize: `${chartTitleSize}px`,
    color: chartTitleResolvedColor,
    fontWeight: chartTitleEmphasis === 'bold' ? 'bold' : 'normal',
    fontStyle: chartTitleEmphasis === 'italic' ? 'italic' : 'normal',
    textDecoration: chartTitleEmphasis === 'underline' ? 'underline' : 'none',
    textAlign: chartTitleAlign as any,
    paddingTop: titlePad,
    paddingBottom: titlePad,
    paddingLeft: titlePad + chartTitleGapLeft,
    paddingRight: titlePad + chartTitleGapRight,
    backgroundColor: chartTitleBgMode === 'custom' ? (chartTitleBgColor || 'transparent') : 'transparent',
    border: chartTitleOutline ? '1px solid hsl(var(--border))' : undefined,
    borderRadius: chartTitleOutline ? 6 : undefined,
  }
  const renderChartTitle = () => {
    if (!title) return null
    if (chartTitlePosition !== 'above' && chartTitlePosition !== 'below') return null
    return (
      <div className="flex-none w-full">
        <div className="w-full truncate" style={chartTitleStyle as any} title={title}>{title}</div>
      </div>
    )
  }

  // Tabs rendering path (placed here AFTER all hooks, to keep hook order stable across renders)
  if (wantTabs) {
    const defaultVal = defaultTabValue || (allPrefixedValues.length>0?String(allPrefixedValues[0]):'')
    return (
      <ErrorBoundary name="ChartCard">
      <div className={cardClass} style={bgStyle as any} ref={containerRef}>
        <div className="h-full flex flex-col">
          <Tabs value={activeTab || defaultVal} defaultValue={defaultVal} className="h-full flex flex-col" onValueChangeAction={(v) => { setActiveTab(v); if (typeof window !== 'undefined') { if ('requestAnimationFrame' in window) { window.requestAnimationFrame(() => window.dispatchEvent(new Event('resize'))) } else { setTimeout(() => window.dispatchEvent(new Event('resize')), 16) } } }}>
            <TabsList variant={tabsVariant} className={tabsListClass}>
              {allPrefixedValues.map((v) => {
                const isAll = v === '__ALL__'
                const raw = isAll ? 'All' : normalizeCategoryLabel(String(v))
                const label = (() => {
                  const normalized = String(raw)
                  const mode = (tabsLabelCase === 'legend') ? ((options as any)?.legendLabelCase as ('lowercase'|'capitalize'|'proper'|undefined)) : (tabsLabelCase as ('lowercase'|'capitalize'|'proper'))
                  if (!mode) return toProperCase(normalizeCategoryLabel(normalized))
                  const s = normalizeCategoryLabel(normalized)
                  switch (mode) {
                    case 'lowercase': return s.toLowerCase()
                    // Capitalize = UPPERCASE all characters (per request)
                    case 'capitalize': return s.toUpperCase()
                    case 'proper': default: return s.replace(/[_-]+/g,' ').split(/\s+/).map(w=>w? (w[0].toUpperCase()+w.slice(1).toLowerCase()):w).join(' ')
                  }
                })()
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
              {/* All tab content */}
              {tabsShowAll && (
                <TabsContent key="__ALL__" value="__ALL__" className="h-full" forceMount>
                  <div className="h-full">
                    <ChartCard
                      key={`${widgetId || title || 'chart'}::tab:__ALL__::active:${activeTab === '__ALL__' ? '1' : '0'}`}
                      title={title}
                      sql={sql}
                      datasourceId={datasourceId}
                      type={type}
                      options={{ ...(options || {}), tabsField: undefined }}
                      queryMode={queryMode}
                      querySpec={querySpec}
                      customColumns={customColumns}
                      widgetId={widgetId ? `${widgetId}::tab:__ALL__` : undefined}
                      pivot={pivot}
                      reservedTop={reservedTop}
                      layout={layout}
                      tabbedGuard={true}
                      tabbedField={tabsField as string}
                    />
                  </div>
                </TabsContent>
              )}
              {/* Individual value tabs */}
              {sortedTabValues.map((v) => (
                <TabsContent key={v} value={String(v)} className="h-full" forceMount>
                  <div className="h-full">
                    <ChartCard
                      key={`${widgetId || title || 'chart'}::tab:${String(v)}::active:${activeTab === String(v) ? '1' : '0'}`}
                      title={title}
                      sql={sql}
                      datasourceId={datasourceId}
                      type={type}
                      options={{ ...(options || {}), tabsField: undefined }}
                      queryMode={queryMode}
                      querySpec={querySpec ? { ...querySpec, where: { ...((querySpec as any)?.where || {}), [tabsField as string]: [v] } } : undefined}
                      customColumns={customColumns}
                      widgetId={widgetId ? `${widgetId}::tab:${String(v)}` : undefined}
                      pivot={pivot}
                      reservedTop={reservedTop}
                      layout={layout}
                      tabbedGuard={true}
                      tabbedField={tabsField as string}
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
    <ErrorBoundary name="ChartCard">
    <div className={cardClass} style={bgStyle as any} ref={containerRef}>
      {/* Dark theme overrides for Recharts axis when using Tremor charts */}
      <style jsx global>{`
        .dark .recharts-cartesian-axis-tick tspan { fill: #9CA3AF !important; }
        .dark .recharts-cartesian-grid-horizontal line, .dark .recharts-cartesian-grid-vertical line { stroke: rgba(148,163,184,0.25) !important; }
        /* Per-widget gridline styling via CSS variables */
        .chart-grid-scope .recharts-cartesian-grid-horizontal line {
          stroke: var(--grid-h-stroke, rgba(148,163,184,0.25)) !important;
          stroke-width: var(--grid-h-width, 1) !important;
          stroke-dasharray: var(--grid-h-dash, 0) !important;
        }
        .chart-grid-scope .recharts-cartesian-grid-vertical line {
          stroke: var(--grid-v-stroke, rgba(148,163,184,0.25)) !important;
          stroke-width: var(--grid-v-width, 1) !important;
          stroke-dasharray: var(--grid-v-dash, 0) !important;
        }
        /* Per-axis font controls for Tremor/Recharts within the scoped container */
        .chart-axis-scope .recharts-xAxis .recharts-cartesian-axis-tick tspan {
          fill: var(--x-axis-color, inherit) !important;
          font-weight: var(--x-axis-weight, normal) !important;
          font-size: var(--x-axis-size, 11px) !important;
        }
        .chart-axis-scope .recharts-yAxis .recharts-cartesian-axis-tick tspan {
          fill: var(--y-axis-color, inherit) !important;
          font-weight: var(--y-axis-weight, normal) !important;
          font-size: var(--y-axis-size, 11px) !important;
        }
        /* Ensure tooltips overlay above card and are not clipped */
        .recharts-wrapper { overflow: visible !important; }
        .recharts-tooltip-wrapper { z-index: 2147483647 !important; overflow: visible !important; pointer-events: none; }
        .recharts-default-tooltip { overflow: visible !important; }
        /* ECharts tooltip dom (when renderMode html) */
        .echarts-tooltip, .echarts-tooltip * { z-index: 2147483647 !important; }
        .echarts-for-react, .echarts-for-react > div { overflow: visible !important; }
        @keyframes square-bounce {
          0%, 80%, 100% { transform: scale(0.9); opacity: 0.6 }
          40% { transform: scale(1); opacity: 1 }
        }
      `}</style>
      {q.isLoading ? (
        <div className="flex flex-col items-center justify-center h-[220px]">
          <div className="grid grid-cols-2 gap-2">
            {[0,1,2,3].map((i)=> (
              <span key={i} className="w-3.5 h-3.5 rounded-sm" style={{ backgroundColor: 'hsl(var(--primary))', animation: `square-bounce 1.2s ${i*0.12}s infinite ease-in-out` }} />
            ))}
          </div>
          {(options as any)?.showLoadTime ? (
            <div className="mt-2 text-[11px] text-muted-foreground">{Math.max(0, loadingSeconds)}s</div>
          ) : null}
        </div>
      ) : q.error ? (
        <div className="text-sm text-red-600">Failed to load chart</div>
      ) : (!isMulti && queryMode === 'spec' && !yLooksNumeric && (querySpec as any)?.select?.length >= 2) ? (
        <div className="text-sm text-muted-foreground">
          Y-axis must be numeric. Pick a numeric column for Y or apply an aggregation.
        </div>
      ) : data.length === 0 ? (
        <div className="text-sm text-muted-foreground">No data</div>
      ) : (
        <>
          {/* Delta + Filters UI group (no header title) */}
          {(deltaUI === 'filterbar' || fieldsExposed.length > 0) ? (
            <div className="flex-none flex items-center justify-between gap-2 mb-1" ref={headerUIRef}>
              <div className="min-w-0" />
              <div className="flex flex-wrap items-center justify-end gap-2">
                {/* Delta filterbar */}
                {deltaUI === 'filterbar' ? (
                  <FilterbarControl
                    active={activeDeltaMode as any}
                    options={deltaModes}
                    labels={deltaLabels}
                    onChange={(m: string) => setFilterbarMode(m as any)}
                  />
                ) : null}
                {/* Sort/Top N indicators (badges). When disabled, fall back to old defaults pill. */}
                {(() => {
                    const dd: any = (options as any)?.dataDefaults || {}
                    const overrides = !!(dd?.sort?.by || (dd?.topN?.n && Number(dd.topN.n) > 0))
                    const showBadges = !!(((options as any)?.showDataDefaultsBadges) || dd?.showHeaderBadges || dd?.showOverrideBadges)
                    const defs: any = (dsDefaultsQ.data as any)?.defaults || {}
                    const dsSort = defs?.sort?.by ? `Sort ${String(defs.sort.by)} ${(String(defs.sort.direction || 'desc')).toLowerCase()}` : ''
                    const dsTop = defs?.limitTopN?.n ? `Top ${defs.limitTopN.n}${defs.limitTopN.by?` by ${defs.limitTopN.by} ${(String(defs.limitTopN.direction||'desc')).toLowerCase()}`:''}` : ''
                    const ddUsed = (dd?.useDatasourceDefaults === false) || overrides
                    const ddSort = dd?.sort?.by ? `Sort ${String(dd.sort.by)} ${(String(dd.sort.direction || 'desc')).toLowerCase()}` : ''
                    const ddTop = (dd?.topN?.n && Number(dd.topN.n) > 0) ? `Top ${dd.topN.n}${dd.topN.by?` by ${dd.topN.by} ${(String(dd.topN.direction||'desc')).toLowerCase()}`:''}` : ''
                    if (!showBadges) {
                      return dsDefaultsText ? (
                        <span className="text-[10px] px-2 py-0.5 rounded-md border bg-card text-muted-foreground" title="Datasource defaults">Using defaults: {dsDefaultsText}</span>
                      ) : null
                    }
                    return (
                      <>
                        {dsSort && <TremorBadge className="text-[10px]" >Default · {dsSort}</TremorBadge>}
                        {dsTop && <TremorBadge className="text-[10px]" >Default · {dsTop}</TremorBadge>}
                        {ddUsed && ddSort && <TremorBadge className="text-[10px]" >Override · {ddSort}</TremorBadge>}
                        {ddUsed && ddTop && <TremorBadge className="text-[10px]" >Override · {ddTop}</TremorBadge>}
                      </>
                    )
                  })()}
                {fieldsExposed.map((field) => {
                  // Determine type by sampling distincts, fallback to current query rows
                  if (!distinctCache[field]) { void loadDistinct(field) }
                  let sample: any[] = (distinctCache[field] || []).slice(0, 12)
                  if (sample.length === 0) {
                    try {
                      const rows = (Array.isArray((q.data as any)?.rows) ? ((q.data as any).rows as any[]) : [])
                      const cols = ((q.data as any)?.columns as string[]) || []
                      const idx = cols.indexOf(field)
                      const vals = rows.slice(0, 48).map((r: any) => (Array.isArray(r) && idx>=0) ? r[idx] : (r?.[field]))
                      sample = vals.filter((v) => v !== null && v !== undefined).slice(0, 12).map((v) => String(v))
                    } catch {}
                  }
                  let kind: 'string'|'number'|'date' = 'string'
                  const numHits = sample.filter((s) => Number.isFinite(Number(s))).length
                  const dateHits = sample.filter((s) => { const d = parseDateLoose(s); return !!d }).length
                  // Prefer number if both match
                  if (numHits >= Math.max(1, Math.ceil(sample.length/2))) kind = 'number'
                  else if (dateHits >= Math.max(1, Math.ceil(sample.length/2))) kind = 'date'

                  // Current label reflecting selection
                  const sel = uiWhere[field] as any
                  const baseWhere = ((querySpec as any)?.where || {}) as Record<string, any>
                  const label = (() => {
                    if (kind === 'string') {
                      const arr: any[] = Array.isArray(sel) ? sel : []
                      return arr && arr.length ? `${field} (${arr.length})` : field
                    }
                    if (kind === 'date') {
                      const a = (uiWhere[`${field}__gte`] as string|undefined)
                      const b = (uiWhere[`${field}__lt`] as string|undefined)
                      return (a||b) ? `${field} (${a||''}–${b||''})` : field
                    }
                    // number
                    const ops = ['gte','lte','gt','lt'] as const
                    const exp = ops.map(op => (uiWhere[`${field}__${op}`])).some(v => v!=null)
                    return exp ? `${field} (filtered)` : field
                  })()

                  const mergedWhere: Record<string, any> = (uiTruthWhere as any)
                  return (
                    <FilterbarRuleControl
                      key={`${field}:${kind}`}
                      label={label}
                      kind={kind}
                      field={field}
                      where={mergedWhere}
                      distinctCache={distinctCache as any}
                      loadingCache={distinctLoading as any}
                      loadDistinctAction={loadDistinct as any}
                      onPatchAction={(patch: Record<string, any>) => setUiWhereAndEmit(patch)}
                    />
                  )
                })}
              </div>
            </div>
          ) : null}

          {(options?.legendPosition ?? 'bottom') === 'top' ? (
            <div className="flex-none mt-1" ref={legendTopRef}>{renderLegend()}</div>
          ) : null}

          {/* Above title inside chart body */}
          {title && chartTitlePosition === 'above' ? (
            <div ref={titleAboveRef} style={{ marginTop: chartTitleGapTop, marginBottom: chartTitleGapBottom }}>
              {renderChartTitle()}
            </div>
          ) : null}

          {layout === 'measure' ? (
            <div className="relative flex-none overflow-visible" style={{ height: Math.max(0, chartBoxH ?? 0), paddingTop: 8 + extraPadTop, paddingBottom: 8 + extraPadBottom }} key={`h:${chartBoxH ?? 'auto'}`}>
              {content}
            </div>
          ) : (
            <div className="relative flex-1 min-h-[160px] overflow-visible" style={{ paddingTop: 8 + extraPadTop, paddingBottom: 8 + extraPadBottom }}>
              {content}
            </div>
          )}

          {/* Below title inside chart body */}
          {title && chartTitlePosition === 'below' ? (
            <div ref={titleBelowRef} style={{ marginTop: chartTitleGapTop, marginBottom: chartTitleGapBottom }}>
              {renderChartTitle()}
            </div>
          ) : null}

          {(options?.legendPosition ?? 'bottom') !== 'top' ? (
            <div className="flex-none mt-1" ref={legendBottomRef}>{renderLegend()}</div>
          ) : null}
        </>
      )}
  </div>
  </ErrorBoundary>
)

}

type FormatMode =
  | 'none'
  | 'short'
  | 'abbrev'
  | 'currency'
  | 'percent'
  | 'bytes'
  | 'wholeNumber'
  | 'number'
  | 'thousands'
  | 'millions'
  | 'billions'
  | 'oneDecimal'
  | 'twoDecimals'
  | 'percentWhole'
  | 'percentOneDecimal'
  | 'timeHours'
  | 'timeMinutes'
  | 'distance-km'
  | 'distance-mi'

function formatNumber(n: number, mode: FormatMode): string {
  // Avoid propagating NaN/Infinity to UI
  if (!isFinite(n)) return '0'
  switch (mode) {
    case 'abbrev': {
      const abs = Math.abs(n)
      if (abs >= 1_000_000_000) return `${(n / 1_000_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}B`
      if (abs >= 1_000_000) return `${(n / 1_000_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}M`
      if (abs >= 1_000) return `${(n / 1_000).toLocaleString(undefined, { maximumFractionDigits: 2 })}K`
      return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
    }
    case 'short': {
      const abs = Math.abs(n)
      if (abs >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`
      if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
      if (abs >= 1_000) return `${(n / 1_000).toFixed(1)}K`
      return String(n)
    }
    case 'wholeNumber': {
      return Math.round(n).toLocaleString()
    }
    case 'number': {
      return n.toLocaleString(undefined, { maximumFractionDigits: 2 })
    }
    case 'oneDecimal': {
      return n.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })
    }
    case 'twoDecimals': {
      return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    }
    case 'thousands': {
      const abs = Math.abs(n)
      // Always express in thousands, allowing fractions (e.g., 500 -> 0.5K)
      const v = n / 1_000
      return `${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}K`
    }
    case 'millions': {
      const abs = Math.abs(n)
      // Always express in millions, allowing fractions (e.g., 500,000 -> 0.5M)
      const v = n / 1_000_000
      return `${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}M`
    }
    case 'billions': {
      const abs = Math.abs(n)
      // Always express in billions, allowing fractions
      const v = n / 1_000_000_000
      return `${v.toLocaleString(undefined, { maximumFractionDigits: 2 })}B`
    }
    case 'currency': {
      try {
        return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(n)
      } catch {
        return `$${n.toFixed(2)}`
      }
    }
    case 'percent': {
      const v = Math.abs(n) <= 1 ? n * 100 : n
      return `${v.toFixed(1)}%`
    }
    case 'percentWhole': {
      const v = Math.abs(n) <= 1 ? n * 100 : n
      return `${Math.round(v)}%`
    }
    case 'percentOneDecimal': {
      const v = Math.abs(n) <= 1 ? n * 100 : n
      return `${v.toFixed(1)}%`
    }
    case 'bytes': {
      const units = ['B', 'KB', 'MB', 'GB', 'TB']
      let v = n
      let i = 0
      while (Math.abs(v) >= 1024 && i < units.length - 1) {
        v /= 1024
        i++
      }
      return `${v.toFixed(1)} ${units[i]}`
    }
    case 'timeHours': {
      // Interpret value as hours; show 1 decimal for non-integers
      const abs = Math.abs(n)
      const withDec = abs % 1 !== 0
      return withDec ? `${n.toFixed(1)}h` : `${Math.round(n)}h`
    }
    case 'timeMinutes': {
      // Interpret value as minutes; whole numbers preferred
      const abs = Math.abs(n)
      const withDec = abs % 1 !== 0
      return withDec ? `${n.toFixed(1)}m` : `${Math.round(n)}m`
    }
    case 'distance-km': {
      const abs = Math.abs(n)
      const digits = abs >= 100 ? 0 : 1
      return `${n.toFixed(digits)} km`
    }
    case 'distance-mi': {
      const abs = Math.abs(n)
      const digits = abs >= 100 ? 0 : 1
      return `${n.toFixed(digits)} mi`
    }
    default:
      return String(n)
  }
}

// Safe numeric coercion: returns 0 for non-finite values
function toNum(x: any, fallback = 0): number {
  const n = Number(x)
  return Number.isFinite(n) ? n : fallback
}

// Custom date formatter supporting patterns like:
// YYYY, YY, MM, M, DD, D, MMM, MMMM, ddd/DDD, dddd/DDDD, HH, H, hh, h, mm, m, a, A, q/Q, ww, and bracketed literals [text].
function formatDatePattern(d: Date, pattern: string): string {
  try {
    if (!pattern) return d.toString()
    const pad2 = (n: number) => String(n).padStart(2, '0')
    const monthShort = d.toLocaleDateString('en-US', { month: 'short' })
    const monthLong = d.toLocaleDateString('en-US', { month: 'long' })
    const weekdayShort = d.toLocaleDateString('en-US', { weekday: 'short' })
    const weekdayLong = d.toLocaleDateString('en-US', { weekday: 'long' })
    const q = Math.floor(d.getMonth() / 3) + 1
    const isoWeek = (() => {
      const _d = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate()))
      _d.setUTCDate(_d.getUTCDate() + 4 - (_d.getUTCDay() || 7))
      const yearStart = new Date(Date.UTC(_d.getUTCFullYear(), 0, 1))
      return Math.ceil((((_d.getTime() - yearStart.getTime()) / 86400000) + 1) / 7)
    })()
    const h24 = d.getHours()
    const h12 = h24 % 12 || 12
    const am = h24 < 12

    // Extract literals inside [ ... ] and protect them
    const literals: string[] = []
    let fmt = pattern.replace(/\[(.*?)\]/g, (_m, p1) => {
      literals.push(String(p1))
      return `__L${literals.length - 1}__`
    })

    // Token map (longer tokens first)
    const map: Record<string, string> = {
      'YYYY': String(d.getFullYear()),
      'YY': String(d.getFullYear()).slice(-2),
      'MMMM': monthLong,
      'MMM': monthShort,
      'MM': pad2(d.getMonth() + 1),
      'M': String(d.getMonth() + 1),
      'DD': pad2(d.getDate()),
      'D': String(d.getDate()),
      'dddd': weekdayLong,
      'ddd': weekdayShort,
      'DDDD': weekdayLong, // accept uppercase variants
      'DDD': weekdayShort,
      'HH': pad2(h24),
      'H': String(h24),
      'hh': pad2(h12),
      'h': String(h12),
      'mm': pad2(d.getMinutes()),
      'm': String(d.getMinutes()),
      'a': am ? 'am' : 'pm',
      'A': am ? 'AM' : 'PM',
      'ww': pad2(isoWeek),
      'q': String(q),
      'Q': String(q),
    }
    const tokens = ['YYYY','MMMM','MMM','YY','MM','M','DDDD','dddd','DDD','ddd','DD','D','HH','H','hh','h','mm','m','A','a','ww','Q','q']
    tokens.forEach(tok => {
      fmt = fmt.replace(new RegExp(tok, 'g'), map[tok])
    })
    // Restore literals
    fmt = fmt.replace(/__L(\d+)__/g, (_m, idx) => literals[Number(idx)] ?? '')
    return fmt
  } catch { return d.toString() }
}
