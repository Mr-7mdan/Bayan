"use client"

import { Fragment, useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useQuery } from '@tanstack/react-query'
import { Api, DatasourceOut, IntrospectResponse, QueryApi } from '@/lib/api'
import { colorKeyToToken, tokenToColorKey, chartColors, type AvailableChartColorsKeys } from '@/lib/chartUtils'
import { PivotBuilder, type PivotAssignments } from '@/components/builder/PivotBuilder'
import type { WidgetConfig, CompositionComponent } from '@/types/widgets'
import { TabGroup, TabList, Tab, TabPanels, TabPanel, TextInput } from '@tremor/react'
import { Select, SelectTrigger, SelectContent, SelectItem, SelectValue } from '@/components/Select'
import { Switch } from '@/components/Switch'
import { Accordion, AccordionItem, AccordionTrigger, AccordionContent } from '@/components/Accordion'
import { RiPaletteLine, RiInformationLine, RiRulerLine, RiGridLine } from '@remixicon/react'
import { RiArrowUpSFill, RiArrowDownSFill, RiArrowRightSFill, RiArrowUpLine, RiArrowDownLine, RiArrowRightLine } from '@remixicon/react'
import { RiAddLine, RiAddFill, RiSubtractLine, RiSubtractFill, RiArrowRightSLine, RiArrowDownSLine, RiArrowRightWideLine, RiArrowDownWideLine, RiArrowDropRightLine, RiArrowDropDownLine } from '@remixicon/react'
import { RiAlignLeft, RiAlignCenter, RiAlignRight, RiAlignTop, RiAlignVertically, RiAlignBottom, RiBold, RiItalic, RiUnderline } from '@remixicon/react'
import { useConfigUpdate } from '@/components/builder/ConfigUpdateContext'
import { compileFormula, parseReferences } from '@/lib/formula'
import RichTextEditor from '@/components/ui/RichTextEditor'
import CompositionBuilderModal from '@/components/builder/CompositionBuilderModal'
import AdvancedSqlDialog from '@/components/builder/AdvancedSqlDialog'
import { useAuth } from '@/components/providers/AuthProvider'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import * as SchemaCache from '@/lib/schemaCache'
import HeatmapCard from '@/components/widgets/HeatmapCard'
import ChartCard from '@/components/widgets/ChartCard'
import TabsControls from '@/components/builder/TabsControls'

export default function ConfiguratorPanel({ selected, allWidgets, quickAddAction }: { selected: WidgetConfig | null; allWidgets?: Record<string, WidgetConfig>; quickAddAction?: (kind: 'kpi'|'chart'|'table', opts?: { addToLayout?: boolean }) => string }) {
  const updateConfig = useConfigUpdate()
  const { user } = useAuth()
  const { env } = useEnvironment()
  const [local, setLocal] = useState<WidgetConfig | null>(selected)
  // Debounce heavy pivot recomputes while configuring
  const pivotUpdateTimer = useRef<number | null>(null)
  const schedulePivotUpdate = (next: WidgetConfig) => {
    setLocal(next)
    if (typeof window !== 'undefined') {
      if (pivotUpdateTimer.current) window.clearTimeout(pivotUpdateTimer.current)
      pivotUpdateTimer.current = window.setTimeout(() => { updateConfig(next) }, 1500) as any
    } else {
      updateConfig(next)
    }
  }
  const [chartTab, setChartTab] = useState<'appearance' | 'tooltip' | 'axis' | 'grid'>('appearance')
  const [kpiTab, setKpiTab] = useState<'appearance' | 'deltas'>('appearance')
  
  
  const [gridSubTab, setGridSubTab] = useState<'hMain'|'hSecondary'|'vMain'|'vSecondary'>('hMain')
  const [sampleRows, setSampleRows] = useState<Array<Record<string, any>>>([])
  const [editingCustom, setEditingCustom] = useState<{ id?: string; name: string; formula: string; type?: 'number'|'string'|'date'|'boolean' } | null>(null)
  const [editNonce, setEditNonce] = useState(0)
  const [compOpen, setCompOpen] = useState(false)
  const [advOpen, setAdvOpen] = useState(false)
  // Tremor Table helpers (format mapping editor)
  const [ttFmtCol, setTtFmtCol] = useState<string>('')
  const [ttFmtMode, setTtFmtMode] = useState<'none'|'short'|'currency'|'percent'|'bytes'>('none')
  // Resolved period preview (Delta settings)
  const [deltaResolved, setDeltaResolved] = useState<{ curStart: string; curEnd: string; prevStart: string; prevEnd: string } | null>(null)
  // around other useState hooks
  const [axisSubtab, setAxisSubtab] = useState<'main'|'secondary'>('main')
    

  // Listen for composition inner edit events to open the builder automatically
  useEffect(() => {
    const handler = (e: Event) => {
      try {
        const detail = (e as CustomEvent).detail as { widgetId?: string; compIndex?: number }
        if (!detail?.widgetId) return
        const id = (local as any)?.id
        const isComposition = (local as any)?.type === 'composition'
        if (isComposition && id === detail.widgetId) {
          setCompOpen(true)
        }
      } catch {}
    }
    if (typeof window !== 'undefined') {
      window.addEventListener('composition-edit-component', handler)
    }
    return () => { if (typeof window !== 'undefined') window.removeEventListener('composition-edit-component', handler) }
  }, [local?.id, (local as any)?.type])
  const [deltaSampleNow, setDeltaSampleNow] = useState<string | null>(null)
  const [deltaPreviewLoading, setDeltaPreviewLoading] = useState(false)
  const [deltaPreviewError, setDeltaPreviewError] = useState<string | undefined>(undefined)
  const dsQ = useQuery({ queryKey: ['datasources'], queryFn: () => Api.listDatasources(undefined, user?.id) })

  // Default datasource selection (per-user, saved in localStorage by Data Model page)
  const [defaultDsId, setDefaultDsId] = useState<string | null>(null)
  useEffect(() => {
    try {
      if (typeof window !== 'undefined') {
        const read = () => { try { setDefaultDsId(localStorage.getItem('default_ds_id')) } catch { setDefaultDsId(null) } }
        read()
        const onStorage = (e: StorageEvent) => { if (e.key === 'default_ds_id') read() }
        const onCustom = () => read()
        window.addEventListener('storage', onStorage as EventListener)
        window.addEventListener('default-ds-change', onCustom as EventListener)
        return () => { window.removeEventListener('storage', onStorage as EventListener); window.removeEventListener('default-ds-change', onCustom as EventListener) }
      }
    } catch {}
    return () => {}
  }, [])

  const dsId = (local?.datasourceId as string | undefined) ?? (defaultDsId || undefined)
  const initialSchema = dsId ? (SchemaCache.get(dsId) || undefined) : undefined
  const schemaQ = useQuery({
    queryKey: ['ds-schema', dsId ?? '_local'],
    queryFn: ({ signal }) => (dsId ? Api.introspect(dsId as string, signal) : Api.introspectLocal(signal)),
    retry: 0,
    refetchOnWindowFocus: false,
    staleTime: 5 * 60 * 1000,
    gcTime: 10 * 60 * 1000,
    initialData: initialSchema,
  })

  // (moved below dsTransformsQ)

  // Datasource-level transforms (to expose saved customColumns in field picker)
  const dsTransformsQ = useQuery({
    queryKey: ['ds-transforms', dsId],
    queryFn: () => Api.getDatasourceTransforms(String(dsId)),
    enabled: !!dsId,
    staleTime: 60 * 1000,
  })

  // (moved below fallbackQ)

  // (relocated below fallbackQ to avoid TDZ)

  useEffect(() => {
    if (dsId && schemaQ.data) SchemaCache.set(dsId, schemaQ.data as IntrospectResponse)
  }, [dsId, schemaQ.data])

  // Lightweight tables-only list (includes views). Used to ensure views appear quickly even if schema cache is stale.
  const [tablesFast, setTablesFast] = useState<Array<{ value: string; label: string }>>([])
  useEffect(() => {
    if (!dsId) { setTablesFast([]); return }
    const ac = new AbortController()
    ;(async () => {
      try {
        const fast = await Api.tablesOnly(String(dsId), ac.signal)
        if (ac.signal.aborted) return
        const items: Array<{ value: string; label: string }> = []
        ;(fast?.schemas || []).forEach((sch: any) => {
          ;(sch?.tables || []).forEach((t: string) => {
            const key = sch.name ? `${sch.name}.${t}` : t
            items.push({ value: key, label: t })
          })
        })
        setTablesFast(items)
      } catch {
        if (!ac.signal.aborted) setTablesFast([])
      }
    })()
    return () => { try { ac.abort() } catch {} }
  }, [dsId])

  // Determine datasource type for the selected widget
  const dsType = useMemo(() => {
    try { return (dsQ.data || []).find((d: DatasourceOut) => d.id === dsId)?.type?.toLowerCase() || null } catch { return null }
  }, [dsQ.data, dsId])

  // Fallback: use information_schema when above fails (e.g., endpoint unavailable)
  const fallbackQ = useQuery({
    queryKey: ['ds-schema-fallback', dsId ?? '_local'],
    enabled: !!schemaQ.error && (!!dsType ? dsType !== 'mssql' : true),
    queryFn: async () => {
      const sql = `
        select table_schema, table_name, column_name
        from information_schema.columns
        order by table_schema, table_name, ordinal_position
      `
      const res = await Api.query({ sql, datasourceId: dsId || undefined, limit: 5000 })
      // Transform to IntrospectResponse shape
      type Row = [string, string, string]
      const map = new Map<string, Map<string, { name: string; type?: string | null }[]>>()
      ;(res.rows as any[]).forEach((r: Row) => {
        const [sch, tbl, col] = r
        if (!map.has(sch)) map.set(sch, new Map())
        const tmap = map.get(sch)!
        if (!tmap.has(tbl)) tmap.set(tbl, [])
        tmap.get(tbl)!.push({ name: String(col), type: null })
      })
      return {
        schemas: Array.from(map.entries()).map(([sch, tmap]) => ({
          name: sch,
          tables: Array.from(tmap.entries()).map(([tbl, cols]) => ({ name: tbl, columns: cols })),
        })),
      } as IntrospectResponse
    },
    retry: 0,
    refetchOnWindowFocus: false,
    staleTime: 5 * 60 * 1000,
    gcTime: 10 * 60 * 1000,
  })

  const isRoleSchema = (name: string) => {
    const n = String(name || '').toLowerCase()
    if (!n) return false
    if (n.startsWith('db_')) return true
    if (n === 'information_schema' || n === 'sys' || n === 'guest' || n === 'pg_catalog') return true
    return false
  }
  const preferredOwners = (t?: string | null) => {
    const s = String(t || '').toLowerCase()
    if (!s) return [] as string[]
    if (s.includes('mssql') || s.includes('sqlserver')) return ['dbo']
    if (s.includes('postgres')) return ['public']
    if (s.includes('duckdb') || s.includes('sqlite')) return ['main']
    return [] as string[]
  }
  const sourceItems = useMemo(() => {
    const out: { value: string; label: string }[] = []
    const owners = new Set(preferredOwners(dsType))
    // Prefer fast tables-only list (includes views) when available
    if (tablesFast.length > 0) {
      const filtered = tablesFast.filter((it) => {
        const sch = String(it.value || '').split('.')[0]
        return !isRoleSchema(String(sch || ''))
      })
      const uniq = new Map<string, { value: string; label: string }>()
      filtered.forEach((it) => { if (!uniq.has(it.value)) uniq.set(it.value, it) })
      return Array.from(uniq.values())
    }
    const data = (schemaQ.data as IntrospectResponse | undefined) || (fallbackQ.data as IntrospectResponse | undefined)
    ;(data?.schemas || [])
      .filter((sch) => !isRoleSchema(sch.name) && (owners.size ? owners.has(sch.name.toLowerCase()) : true))
      .forEach((sch) => {
        (sch.tables || []).forEach((t) => out.push({ value: `${sch.name}.${t.name}`, label: t.name }))
      })
    const uniq = new Map<string, { value: string; label: string }>()
    out.forEach((it) => { if (!uniq.has(it.value)) uniq.set(it.value, it) })
    return Array.from(uniq.values())
  }, [schemaQ.data, fallbackQ.data, dsType, tablesFast])

  const [srcFilter, setSrcFilter] = useState('')
  const filteredSources = useMemo(() => {
    const q = srcFilter.trim().toLowerCase()
    if (!q) return sourceItems
    return sourceItems.filter((it) => it.label.toLowerCase().includes(q) || it.value.toLowerCase().includes(q))
  }, [sourceItems, srcFilter])

  const columns = useMemo(() => {
    const src = local?.querySpec?.source
    const data = (schemaQ.data as IntrospectResponse | undefined) || (fallbackQ.data as IntrospectResponse | undefined)
    if (!src || !data) return [] as { name: string; type?: string | null }[]
    const parts = src.split('.')
    if (parts.length < 2) return []
    const tblName = parts.pop() as string
    const schName = parts.join('.')
    const sch = data.schemas.find((s) => s.name === schName)
    const tbl = sch?.tables.find((t) => t.name === tblName)
    return tbl?.columns || []
  }, [schemaQ.data, fallbackQ.data, local?.querySpec?.source])

  // Numeric field list for gating aggregators in value chips
  const numericFields = useMemo(() => {
    const cols = columns || []
    const isNumericType = (t?: string | null) => {
      if (t == null) return true // unknown types: allow all aggs rather than over-restrict
      const s = String(t).toLowerCase()
      return /(int|bigint|smallint|tinyint|float|double|decimal|numeric|real|money|number)/i.test(s)
    }
    return cols.filter(c => isNumericType(c.type)).map(c => c.name)
  }, [columns])

  // Live result columns from TableCard; fall back to schema columns
  const [resultColumns, setResultColumns] = useState<string[]>([])
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string; columns?: string[] }
      if (!d?.widgetId || d.widgetId !== local?.id) return
      if (!Array.isArray(d.columns)) return
      setResultColumns(d.columns)
    }
    if (typeof window !== 'undefined') window.addEventListener('table-columns-change', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('table-columns-change', handler as EventListener) }
  }, [local?.id])

  // When datasource transforms are saved from Advanced SQL dialog, refresh our transforms query
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { datasourceId?: string }
      if (!d?.datasourceId || String(d.datasourceId) !== String(dsId || '')) return
      try { dsTransformsQ.refetch() } catch {}
      // Also request live columns/rows/samples from TableCard to clear stale alias names
      try {
        if (typeof window !== 'undefined' && local?.id) {
          window.dispatchEvent(new CustomEvent('request-table-columns', { detail: { widgetId: local.id } } as any))
          window.dispatchEvent(new CustomEvent('request-table-rows', { detail: { widgetId: local.id } } as any))
          window.dispatchEvent(new CustomEvent('request-table-samples', { detail: { widgetId: local.id } } as any))
        }
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('datasource-transforms-saved', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('datasource-transforms-saved', handler as EventListener) }
  }, [dsId])

  // Sync from ChartCard filterbars: when ChartCard emits changes, reflect them here.
  // IMPORTANT: Defer updates to avoid React warning about updating another component during render.
  useEffect(() => {
    const onChartWhere = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string; patch?: Record<string, any> }
      if (!d?.widgetId || d.widgetId !== local?.id) return
      const patch = d.patch || {}
      // Schedule after current render/frame to avoid render-phase updates in a different component
      setTimeout(() => {
        let snapshot: any = null
        setLocal((prev: any) => {
          if (!prev) return prev
          const curr = { ...((prev.querySpec?.where || {}) as any) }
          const nextWhere = { ...curr }
          Object.entries(patch).forEach(([k,v]) => { if (v === undefined) delete nextWhere[k]; else (nextWhere as any)[k] = v })
          let changed = false
          for (const [k, v] of Object.entries(patch)) {
            const prevV = (curr as any)[k]
            if (v === undefined) { if (prevV !== undefined) { changed = true; break } }
            else if (prevV !== v) { changed = true; break }
          }
          if (!changed) return prev
          const next = { ...prev, querySpec: { ...(prev.querySpec || { source: '' }), where: nextWhere } }
          snapshot = next
          return next
        })
        if (snapshot) setTimeout(() => updateConfig(snapshot), 0)
      }, 0)
    }
    window.addEventListener('chart-where-change', onChartWhere as EventListener)
    return () => window.removeEventListener('chart-where-change', onChartWhere as EventListener)
  }, [local?.id, updateConfig])

  // Direct preview rows fetcher (works even if TableCard isn't visible)
  async function fetchPreviewRowsForFormula(formula?: string) {
    try {
      const source = local?.querySpec?.source
      if (!source || !dsId) return
      const refs = parseReferences(String(formula || '')).row || []
      const select = Array.from(new Set(refs)).slice(0, 16)
      if (select.length === 0) return
      const spec = { source, select, limit: 5, offset: 0 } as any
      const res = await QueryApi.querySpec({ spec, datasourceId: dsId, limit: 5, offset: 0, includeTotal: false })
      const cols = (res.columns || []) as string[]
      const rows = (res.rows || []).map((arr: any[]) => {
        const obj: Record<string, any> = {}
        cols.forEach((c, i) => { obj[c] = arr[i] })
        return obj
      })
      setSampleRows(rows)
    } catch {
      setSampleRows([])
    }
  }
  const schemaColumnNames: string[] = columns.map((c) => c.name)
  // Always expose full schema field list for builders; union with live result columns so label/derived placeholders still appear
  const columnNames: string[] = useMemo(() => {
    const base = schemaColumnNames
    const reserved = new Set(['value', '__metric__'])
    const resRaw = (resultColumns && resultColumns.length > 0) ? resultColumns : []
    const res = resRaw.filter((n) => !reserved.has(String(n || '').toLowerCase()))
    return Array.from(new Set<string>([...base, ...res]).values())
  }, [JSON.stringify(schemaColumnNames), JSON.stringify(resultColumns)])


  // Merge base column names with custom columns for builders, and Unpivot outputs
  const allFieldNames: string[] = useMemo(() => {
    const customs = (local?.customColumns || []).map((c) => c.name).filter(Boolean)
    // Include ALL datasource-level custom columns unconditionally so users can select them
    const dsCustoms: string[] = (() => {
      try {
        const items = ((dsTransformsQ.data as any)?.customColumns || []) as Array<{ name?: string }>
        return items.map((c) => String(c?.name || '')).filter(Boolean)
      } catch { return [] }
    })()
    // Unpivot: add key/value columns for matching-scope transforms and hide the sourceColumns they replace
    const unpivotAdds: string[] = []
    const unpivotHide: Set<string> = new Set()
    try {
      const list = Array.isArray((dsTransformsQ.data as any)?.transforms) ? ((dsTransformsQ.data as any).transforms as any[]) : []
      const srcNow = String(local?.querySpec?.source || '')
      const widNow = String((local as any)?.id || '')
      const norm = (s: string) => String(s || '').trim().replace(/^\[|\]|^"|"$/g, '')
      const tblEq = (a: string, b: string) => {
        const na = norm(a).split('.').pop() || ''
        const nb = norm(b).split('.').pop() || ''
        return na.toLowerCase() === nb.toLowerCase()
      }
      for (const t of list) {
        if (String(t?.type || '').toLowerCase() !== 'unpivot') continue
        const sc = (t?.scope || {}) as any
        const lvl = String(sc?.level || 'datasource').toLowerCase()
        const match = (
          lvl === 'datasource' ||
          (lvl === 'table' && sc?.table && srcNow && tblEq(String(sc.table), srcNow)) ||
          (lvl === 'widget' && sc?.widgetId && widNow && String(sc.widgetId) === widNow)
        )
        if (!match) continue
        const kc = String(t?.keyColumn || '').trim()
        const vc = String(t?.valueColumn || '').trim()
        if (kc) unpivotAdds.push(kc)
        if (vc) unpivotAdds.push(vc)
        const srcCols = Array.isArray(t?.sourceColumns) ? (t.sourceColumns as any[]).map((x) => String(x || '')) : []
        srcCols.forEach((s) => { if (s) unpivotHide.add(s) })
      }
    } catch {}
    // Joins: include selected column aliases/names and aggregate alias for matching-scope joins
    const joinAdds: string[] = []
    try {
      const joins = Array.isArray((dsTransformsQ.data as any)?.joins) ? ((dsTransformsQ.data as any).joins as any[]) : []
      const srcNow = String(local?.querySpec?.source || '')
      const widNow = String((local as any)?.id || '')
      const norm = (s: string) => String(s || '').trim().replace(/^\[|\]|^"|"$/g, '')
      const tblEq = (a: string, b: string) => {
        const na = norm(a).split('.').pop() || ''
        const nb = norm(b).split('.').pop() || ''
        return na.toLowerCase() === nb.toLowerCase()
      }
      for (const j of joins) {
        const sc = (j?.scope || {}) as any
        const lvl = String(sc?.level || 'datasource').toLowerCase()
        const match = (
          lvl === 'datasource' ||
          (lvl === 'table' && sc?.table && srcNow && tblEq(String(sc.table), srcNow)) ||
          (lvl === 'widget' && sc?.widgetId && widNow && String(sc.widgetId) === widNow)
        )
        if (!match) continue
        const cols = Array.isArray(j?.columns) ? (j.columns as any[]) : []
        cols.forEach((c: any) => {
          const nm = String((c?.alias || c?.name || '')).trim()
          if (nm) joinAdds.push(nm)
        })
        const agg = (j?.aggregate || {}) as any
        const aggAlias = String(agg?.alias || '').trim()
        if (aggAlias) joinAdds.push(aggAlias)
      }
    } catch {}
    const set = new Set<string>([...columnNames, ...dsCustoms, ...customs, ...unpivotAdds])
    // Hide the pivoted columns if present
    unpivotHide.forEach((n) => { if (set.has(n)) set.delete(n) })
    joinAdds.forEach((n) => { if (n) set.add(n) })
    return Array.from(set.values())
  }, [columnNames, local?.customColumns, dsTransformsQ.data, local?.querySpec?.source, (local as any)?.id])

  // Defaults: when KPI is selected, DO NOT auto-enable deltas. Keep UI 'preconfigured' but set deltaMode to 'off'.
  useEffect(() => {
    if (!local || local.type !== 'kpi') return
    const opts = { ...(local.options || {}) } as any
    let changed = false
    if (!opts.deltaUI) { opts.deltaUI = 'preconfigured'; changed = true }
    // Default to OFF to avoid surprising zeroes from narrow periods
    if (!opts.deltaMode) { opts.deltaMode = 'off'; changed = true }
    if (!opts.deltaDateField) {
      // Prefer real date-like types from introspection when available
      const typed = (columns || []).find((c) => /date|time|timestamp/i.test(String(c.type || '')))
      let pick = typed?.name
      if (!pick) pick = (columnNames || []).find((n) => /date|time|timestamp|created|updated|_at$/i.test(n))
      if (!pick) pick = (local?.querySpec as any)?.x // last resort: use x if set
      if (pick) { opts.deltaDateField = pick; changed = true }
    }
    if (changed) {
      const next = { ...local, options: opts }
      setLocal(next)
      updateConfig(next)
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [local?.id, local?.type, local?.querySpec?.source, JSON.stringify(columnNames)])

  // Helper to refresh resolved-period preview using first row of selected deltaDateField
  const refreshDeltaPreview = async () => {
    try {
      setDeltaPreviewLoading(true)
      setDeltaPreviewError(undefined)
      setDeltaResolved(null)
      const source = local?.querySpec?.source
      const field = (local?.options as any)?.deltaDateField as string | undefined
      const mode = (local?.options as any)?.deltaMode as any
      const weekStart = ((local?.options as any)?.deltaWeekStart || env.weekStart) as any
      const where = (local?.querySpec?.where || {}) as Record<string, any>
      if (!source || !field || !mode || mode === 'off') { setDeltaPreviewLoading(false); return }
      // Fetch first row sample for selected field
      const spec: any = { source, select: [field], where: Object.keys(where || {}).length ? where : undefined, limit: 1, offset: 0 }
      const r = await QueryApi.querySpec({ spec, datasourceId: local?.datasourceId, limit: 1, offset: 0, includeTotal: false })
      const cols = (r?.columns || []) as string[]
      const idx = Math.max(0, cols.indexOf(field))
      const v = Array.isArray(r?.rows) && r.rows[0] ? r.rows[0][idx] : undefined
      const nowStr = v != null ? String(v) : undefined
      setDeltaSampleNow(nowStr || null)
      const resolved = await Api.resolvePeriods({ mode, tzOffsetMinutes: (typeof window !== 'undefined') ? new Date().getTimezoneOffset() : 0, weekStart })
      setDeltaResolved(resolved)
    } catch (e: any) {
      setDeltaPreviewError(String(e?.message || 'Failed to resolve period'))
    } finally {
      setDeltaPreviewLoading(false)
    }
  }

// String rule editor for Details panel (pattern-based)
function StringRuleDetails({ field, where, onPatch }: { field: string; where?: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  type StrOp = 'contains'|'not_contains'|'eq'|'ne'|'starts_with'|'ends_with'
  const [op, setOp] = useState<StrOp>('contains')
  const [val, setVal] = useState<string>('')
  useEffect(() => {
    const patch: Record<string, any> = { [field]: undefined, [`${field}__contains`]: undefined, [`${field}__notcontains`]: undefined, [`${field}__startswith`]: undefined, [`${field}__endswith`]: undefined, [`${field}__ne`]: undefined }
    const v = String(val || '').trim()
    if (!v) { onPatch(patch); return }
    switch (op) {
      case 'eq': patch[field] = [v]; break
      case 'ne': patch[`${field}__ne`] = v; break
      case 'contains': patch[`${field}__contains`] = v; break
      case 'not_contains': patch[`${field}__notcontains`] = v; break
      case 'starts_with': patch[`${field}__startswith`] = v; break
      case 'ends_with': patch[`${field}__endswith`] = v; break
    }
    onPatch(patch)
  }, [op, val])
  return (
    <div className="rounded-md border bg-card p-2">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">String rule: {field}</div>
        <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { setOp('contains'); setVal(''); onPatch({ [field]: undefined, [`${field}__contains`]: undefined, [`${field}__notcontains`]: undefined, [`${field}__startswith`]: undefined, [`${field}__endswith`]: undefined, [`${field}__ne`]: undefined }) }}>Clear</button>
      </div>
      <div className="grid grid-cols-3 gap-2 mt-2 items-center">
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
    </div>
  )
}

function FilterDetailsTabs({ kind, selField, local, setLocal, updateConfig }: { kind: 'string'|'number'|'date'; selField: string; local: any; setLocal: (v: any)=>void; updateConfig: (v:any)=>void }) {
  const [tab, setTab] = useState<'manual'|'rule'>(() => {
    try { const k = `cfg-filter-tab:${selField}`; const v = typeof window !== 'undefined' ? localStorage.getItem(k) : null; return v === 'rule' ? 'rule' : 'manual' } catch { return 'manual' }
  })
  useEffect(() => {
    try {
      const k = `cfg-filter-tab:${selField}`
      const v = typeof window !== 'undefined' ? localStorage.getItem(k) : null
      if (v === 'manual' || v === 'rule') setTab(v)
      else setTab('manual')
    } catch { setTab('manual') }
  }, [selField])
  const setTabPersist = (t: 'manual'|'rule') => {
    setTab(t)
    try { if (typeof window !== 'undefined') localStorage.setItem(`cfg-filter-tab:${selField}`, t) } catch {}
  }
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <button type="button" className={`text-[11px] px-2 py-1 rounded-md border ${tab==='manual'?'bg-[hsl(var(--secondary))]':''}`} onClick={()=>setTabPersist('manual')}>Manual</button>
        <button type="button" className={`text-[11px] px-2 py-1 rounded-md border ${tab==='rule'?'bg-[hsl(var(--secondary))]':''}`} onClick={()=>setTabPersist('rule')}>Rule</button>
      </div>
      {tab==='manual' ? (
        kind === 'string' ? (
          <FilterEditor
            field={selField}
            source={local.querySpec?.source || ''}
            datasourceId={dsId}
            values={(local.querySpec?.where as any)?.[selField] as any[]}
            where={(local.querySpec?.where as any)}
            onChange={(vals) => {
              const where = { ...(local.querySpec?.where || {}) as any }
              const hasVals = Array.isArray(vals) && vals.length > 0
              if (hasVals) where[selField] = vals
              else delete where[selField]
              const next = { ...local, querySpec: { ...(local.querySpec || { source: '' }), where } }
              setLocal(next)
              updateConfig(next)
              try { const patch: Record<string, any> = { [selField]: hasVals ? vals : undefined }; window.dispatchEvent(new CustomEvent('config-where-change', { detail: { widgetId: local.id, patch } } as any)) } catch {}
            }}
          />
        ) : (
          <div className="text-[11px] text-muted-foreground">Manual selection is not available for this field type.</div>
        )
      ) : (
        kind === 'number' ? (
          <NumberFilterDetails
            field={selField}
            where={(local.querySpec?.where as any)}
            onPatch={(patch: Record<string, any>) => {
              const curr = { ...(local.querySpec?.where || {}) as any }
              const nextWhere = { ...curr }
              Object.entries(patch).forEach(([k,v]) => { if (v === undefined) delete nextWhere[k]; else (nextWhere as any)[k] = v })
              let changed = false
              for (const [k, v] of Object.entries(patch)) {
                const prevV = (curr as any)[k]
                if (v === undefined) { if (prevV !== undefined) { changed = true; break } }
                else if (prevV !== v) { changed = true; break }
              }
              if (!changed) return
              const nextCfg = { ...local, querySpec: { ...(local.querySpec || { source: '' }), where: nextWhere } }
              setLocal(nextCfg)
              updateConfig(nextCfg)
              try { window.dispatchEvent(new CustomEvent('config-where-change', { detail: { widgetId: local.id, patch } } as any)) } catch {}
            }}
          />
        ) : kind === 'date' ? (
          <DateRuleDetails
            field={selField}
            where={(local.querySpec?.where as any)}
            onPatch={(patch: Record<string, any>) => {
              const curr = { ...(local.querySpec?.where || {}) as any }
              const nextWhere = { ...curr }
              Object.entries(patch).forEach(([k,v]) => { if (v === undefined) delete nextWhere[k]; else (nextWhere as any)[k] = v })
              let changed = false
              for (const [k, v] of Object.entries(patch)) {
                const prevV = (curr as any)[k]
                if (v === undefined) { if (prevV !== undefined) { changed = true; break } }
                else if (prevV !== v) { changed = true; break }
              }
              if (!changed) return
              const nextCfg = { ...local, querySpec: { ...(local.querySpec || { source: '' }), where: nextWhere } }
              setLocal(nextCfg)
              updateConfig(nextCfg)
              try { window.dispatchEvent(new CustomEvent('config-where-change', { detail: { widgetId: local.id, patch } } as any)) } catch {}
            }}
          />
        ) : (
          <StringRuleDetails
            field={selField}
            where={(local.querySpec?.where as any)}
            onPatch={(patch: Record<string, any>) => {
              const curr = { ...(local.querySpec?.where || {}) as any }
              const nextWhere = { ...curr }
              Object.entries(patch).forEach(([k,v]) => { if (v === undefined) delete nextWhere[k]; else (nextWhere as any)[k] = v })
              let changed = false
              for (const [k, v] of Object.entries(patch)) {
                const prevV = (curr as any)[k]
                if (v === undefined) { if (prevV !== undefined) { changed = true; break } }
                else if (prevV !== v) { changed = true; break }
              }
              if (!changed) return
              const nextCfg = { ...local, querySpec: { ...(local.querySpec || { source: '' }), where: nextWhere } }
              setLocal(nextCfg)
              updateConfig(nextCfg)
              try { window.dispatchEvent(new CustomEvent('config-where-change', { detail: { widgetId: local.id, patch } } as any)) } catch {}
            }}
          />
        )
      )}
    </div>
  )
}

// [removed duplicate StringRuleDetails] moved below with FilterDetailsTabs

// Date rule editor for Details panel: presets + custom After/Before/Between
function DateRuleDetails({ field, where, onPatch }: { field: string; where?: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  type Mode = 'preset'|'custom'
  type Preset = 'today'|'yesterday'|'this_month'|'last_month'|'this_quarter'|'last_quarter'|'this_year'|'last_year'
  type CustomOp = 'after'|'before'|'between'
  const storageKey = `cfg-date:${field}`
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
      editTimerRef.current = window.setTimeout(() => { editingRef.current = false }, 600) as any
    }
  }
  useEffect(() => {
    try {
      const raw = typeof window !== 'undefined' ? localStorage.getItem(storageKey) : null
      if (raw) {
        const st = JSON.parse(raw) as { mode?: Mode; preset?: Preset; op?: CustomOp; a?: string; b?: string }
        if (st.mode) setMode(st.mode)
        if (st.preset) setPreset(st.preset)
        if (st.op) setOp(st.op)
        if (typeof st.a === 'string') setA(st.a)
        if (typeof st.b === 'string') setB(st.b)
      }
    } catch {}
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [field])
  useEffect(() => {
    try { if (typeof window !== 'undefined') localStorage.setItem(storageKey, JSON.stringify({ mode, preset, op, a, b })) } catch {}
  }, [storageKey, mode, preset, op, a, b])

  function rangeForPreset(p: Preset): { gte?: string; lt?: string } {
    const now = new Date()
    const ymd = (d: Date) => d.toISOString().slice(0,10)
    const startOfMonth = (d: Date) => new Date(d.getFullYear(), d.getMonth(), 1)
    const endOfMonth = (d: Date) => new Date(d.getFullYear(), d.getMonth()+1, 1)
    const quarter = Math.floor(now.getMonth()/3)
    const startOfQuarter = (y: number, q: number) => new Date(y, q*3, 1)
    const endOfQuarter = (y: number, q: number) => new Date(y, q*3 + 3, 1)
    const startOfYear = (d: Date) => new Date(d.getFullYear(), 0, 1)
    const endOfYear = (d: Date) => new Date(d.getFullYear()+1, 0, 1)
    switch (p) {
      case 'today': {
        const s = new Date(now.getFullYear(), now.getMonth(), now.getDate())
        const e = new Date(s); e.setDate(e.getDate()+1)
        return { gte: ymd(s), lt: ymd(e) }
      }
      case 'yesterday': {
        const e = new Date(now.getFullYear(), now.getMonth(), now.getDate())
        const s = new Date(e); s.setDate(s.getDate()-1)
        return { gte: ymd(s), lt: ymd(e) }
      }
      case 'this_month': return { gte: ymd(startOfMonth(now)), lt: ymd(endOfMonth(now)) }
      case 'last_month': { const s = startOfMonth(now); s.setMonth(s.getMonth()-1); const e = new Date(s.getFullYear(), s.getMonth()+1, 1); return { gte: ymd(s), lt: ymd(e) } }
      case 'this_quarter': return { gte: ymd(startOfQuarter(now.getFullYear(), quarter)), lt: ymd(endOfQuarter(now.getFullYear(), quarter)) }
      case 'last_quarter': { const q = (quarter+3-1)%4; const yr = quarter===0 ? now.getFullYear()-1 : now.getFullYear(); return { gte: ymd(startOfQuarter(yr, q)), lt: ymd(endOfQuarter(yr, q)) } }
      case 'this_year': return { gte: ymd(startOfYear(now)), lt: ymd(endOfYear(now)) }
      case 'last_year': { const s = new Date(now.getFullYear()-1, 0, 1); const e = new Date(now.getFullYear(), 0, 1); return { gte: ymd(s), lt: ymd(e) } }
    }
  }

  const lastSigRef = useRef<string>('')
  useEffect(() => {
    if (!interactedRef.current) return
    const patch: Record<string, any> = { [`${field}__gte`]: undefined, [`${field}__lt`]: undefined }
    if (mode === 'preset') {
      const r = rangeForPreset(preset)
      patch[`${field}__gte`] = r.gte
      patch[`${field}__lt`] = r.lt
      const sig = JSON.stringify(patch)
      if (sig !== lastSigRef.current) { lastSigRef.current = sig; onPatch(patch) }
      return
    }
    // custom
    if (op === 'after') {
      patch[`${field}__gte`] = a || undefined
    } else if (op === 'before') {
      // before end-of-day: set lt to next day
      if (b) { const d = new Date(`${b}T00:00:00`); d.setDate(d.getDate()+1); patch[`${field}__lt`] = d.toISOString().slice(0,10) }
    } else if (op === 'between') {
      patch[`${field}__gte`] = a || undefined
      if (b) { const d = new Date(`${b}T00:00:00`); d.setDate(d.getDate()+1); patch[`${field}__lt`] = d.toISOString().slice(0,10) }
    }
    const sig = JSON.stringify(patch)
    if (sig !== lastSigRef.current) { lastSigRef.current = sig; onPatch(patch) }
  }, [mode, preset, op, a, b])

  return (
    <div className="rounded-md border bg-card p-2">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">Date rule: {field}</div>
        <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { setMode('preset'); setPreset('today'); setOp('between'); setA(''); setB(''); onPatch({ [`${field}__gte`]: undefined, [`${field}__lt`]: undefined }) }}>Clear</button>
      </div>
      <div className="mt-2 flex items-center gap-2 text-xs">
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='preset'} onChange={()=>{ interactedRef.current = true; markEditing(); setMode('preset') }} /> Preset</label>
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='custom'} onChange={()=>{ interactedRef.current = true; markEditing(); setMode('custom') }} /> Custom</label>
      </div>
      {mode === 'preset' ? (
        <div className="mt-2">
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
        </div>
      ) : (
        <div className="grid grid-cols-3 gap-2 mt-2 items-center">
          <select className="col-span-3 sm:col-span-1 px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={op} onChange={(e)=>{ interactedRef.current = true; markEditing(); setOp(e.target.value as CustomOp) }}>
            <option value="after">After</option>
            <option value="before">Before</option>
            <option value="between">Between</option>
          </select>
          {op !== 'between' ? (
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

  // Auto-refresh when delta inputs change
  useEffect(() => {
    if (!local) return
    const mode = (local.options as any)?.deltaMode
    const field = (local.options as any)?.deltaDateField
    const source = local.querySpec?.source
    if (mode && mode !== 'off' && field && source) {
      void refreshDeltaPreview()
    } else {
      setDeltaResolved(null); setDeltaSampleNow(null)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [local?.datasourceId, (local?.options as any)?.deltaMode, (local?.options as any)?.deltaDateField, (local?.options as any)?.deltaWeekStart, env.weekStart, local?.querySpec?.source, JSON.stringify(local?.querySpec?.where || {})])

  // Sample values for filters UI
  const [samplesByField, setSamplesByField] = useState<Record<string, string[]>>({})
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string; samples?: Record<string, string[]> }
      if (!d?.widgetId || d.widgetId !== local?.id) return
      if (!d.samples) return
      setSamplesByField((prev) => {
        try {
          const next = d.samples || {}
          const pk = Object.keys(prev || {})
          const nk = Object.keys(next)
          if (pk.length === nk.length && nk.every((k) => {
            const a = prev?.[k] || []
            const b = next?.[k] || []
            if (a.length !== b.length) return false
            for (let i = 0; i < a.length; i++) { if (a[i] !== b[i]) return false }
            return true
          })) return prev
          return next
        } catch { return d.samples as Record<string, string[]> }
      })
    }
    if (typeof window !== 'undefined') window.addEventListener('table-sample-values-change', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('table-sample-values-change', handler as EventListener) }
  }, [local?.id])

  // Listen for sample named rows (for custom column preview)
  useEffect(() => {
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string; rows?: Array<Record<string, any>> }
      if (!d?.widgetId || d.widgetId !== local?.id) return
      if (!Array.isArray(d.rows)) return
      setSampleRows(d.rows)
    }
    if (typeof window !== 'undefined') window.addEventListener('table-sample-rows-change', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('table-sample-rows-change', handler as EventListener) }
  }, [local?.id])

  // (removed) numeric helper not needed in current pivot flow

  // Auto-detect date/datetime fields to surface date parts in PivotBuilder
  const dateLikeFields = useMemo(() => {
    try {
      const set = new Set<string>()
      const isDateType = (t?: string | null) => /date|time|timestamp/i.test(String(t || ''))
      // 1) From schema types
      ;(columns || []).forEach((c) => { if (isDateType(c.type)) set.add(c.name) })
      // 2) From sample values
      const isDateSample = (s: string) => {
        if (!s) return false
        if (/^\d{10,13}$/.test(s)) return true
        if (/^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?$/.test(s)) return true
        if (/^[0-1]?\d\/[0-3]?\d\/\d{4}(?:\s+\d{2}:\d{2}(?::\d{2})?)?$/.test(s)) return true
        if (/^\d{4}-\d{2}$/.test(s)) return true // YYYY-MM
        if (/^[A-Za-z]{3,9}[-\s]\d{4}$/.test(s)) return true // MMM-YYYY
        return false
      }
      Object.keys(samplesByField || {}).forEach((k) => {
        const arr = (samplesByField?.[k] || []).slice(0, 10)
        if (arr.some((v) => isDateSample(String(v)))) set.add(k)
      })
      // 3) Fallback: name heuristics when no samples/types
      const names = columnNames || []
      names.forEach((n) => {
        if (/date|time|timestamp|created|updated|_at$/i.test(n)) set.add(n)
      })
      // Return only known columns to avoid dangling deriveds
      const allowed = new Set<string>(schemaColumnNames)
      return Array.from(set.values()).filter((n) => allowed.has(n)).sort()
    } catch { return [] as string[] }
  }, [columns, samplesByField, columnNames, schemaColumnNames])

// New small component to create a named measure and return it via onCreate
function NewMeasureButton({ columns, onCreate }: { columns: string[]; onCreate: (name: string, formula: string) => void }) {
  const [open, setOpen] = useState(false)
  const [name, setName] = useState('')
  const [text, setText] = useState('')
  return (
    <>
      <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-foreground" onClick={() => setOpen(true)}>
        New measure
      </button>
      {open && typeof window !== 'undefined' && createPortal(
        <div className="fixed inset-0 z-[999] flex items-center justify-center">
          <div className="absolute inset-0 bg-black/40 overlay-close" onClick={() => { setOpen(false); setEditNonce(editNonce + 1) }} />
          <div className="relative z-[1000] w-[560px] max-w-[95vw] rounded-lg border bg-card p-4 shadow-none">
            <div className="flex items-center justify-between mb-2">
              <div className="text-sm font-medium">New Measure</div>
              <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { setOpen(false); setEditNonce(editNonce + 1) }}>✕</button>
            </div>

          

            <div className="grid grid-cols-[180px,1fr] gap-3">
              <div className="rounded-md p-2 bg-[hsl(var(--secondary)/0.6)] max-h-56 overflow-auto">
                <div className="text-xs mb-1 text-muted-foreground">Columns</div>
                <ul className="space-y-1 text-xs">
                  {columns.map((c) => (
                    <li key={c} className="flex items-center justify-between">
                      <span className="truncate">{c}</span>
                      <button
                        className="text-[10px] px-1 py-0.5 rounded bg-[hsl(var(--btn4))] text-[hsl(var(--primary-foreground))]"
                        onClick={() => setText((t) => (t ? `${t} ${c}` : c))}
                      >
                        Insert
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
              <div className="flex flex-col gap-2">
                <input className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" placeholder="Measure name" value={name} onChange={(e) => setName(e.target.value)} />
                <textarea
                  className="w-full min-h-[140px] px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] font-mono text-xs"
                  value={text}
                  onChange={(e) => setText(e.target.value)}
                  placeholder="e.g., SUM(price) / NULLIF(COUNT(DISTINCT order_id), 0)"
                />
                <div className="flex gap-2 justify-end">
                  <button className="text-xs px-3 py-1.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-foreground" onClick={() => setText('')}>Clear</button>
                  <button
                    className="text-xs px-3 py-1.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-foreground"
                    onClick={() => { if (name && text) { onCreate(name, text); setOpen(false); setName(''); setText('') } }}
                  >
                    Save
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}
      
    </>
  )
}

// Component to create or edit a Custom Column with Excel-like DSL (QuerySpec only)
function NewCustomColumnButton({
  columns,
  onCreate,
  mode = 'create',
  initial,
  onSave,
  sampleRows,
  onRequestRows,
  autoOpen,
  hideTriggerButton,
  samplesByField,
  onClose,
}: {
  columns: string[]
  onCreate?: (name: string, formula: string, type?: 'number'|'string'|'date'|'boolean') => void
  mode?: 'create'|'edit'
  initial?: { id?: string; name: string; formula: string; type?: 'number'|'string'|'date'|'boolean' }
  onSave?: (id: string | undefined, name: string, formula: string, type?: 'number'|'string'|'date'|'boolean') => void
  sampleRows?: Array<Record<string, any>>
  onRequestRows?: (formula?: string) => void
  autoOpen?: boolean
  hideTriggerButton?: boolean
  samplesByField?: Record<string, string[]>
  onClose?: () => void
}) {
  const [open, setOpen] = useState(false)
  const [name, setName] = useState(initial?.name || '')
  const [text, setText] = useState(initial?.formula || '')
  const [ctype, setCtype] = useState<'number'|'string'|'date'|'boolean'>(initial?.type || 'number')
  const [error, setError] = useState<string|undefined>(undefined)
  const [preview, setPreview] = useState<any[]>([])
  const fnGroups: Record<string, string[]> = {
    Math: ['SUM([col])', 'AVG([col])', 'MIN([col])', 'MAX([col])', 'COUNT([col])'],
    Logic: ['IF( , , )', 'AND( , )', 'OR( , )', 'NOT( )', 'COALESCE( , )', 'IFERROR( , )'],
    Text: ['CONCAT( , )', 'LOWER([@col])', 'UPPER([@col])', 'TRIM([@col])'],
    Date: ['YEAR([@date])', 'QUARTER([@date])', 'MONTH([@date])', 'WEEKNUM([@date])', 'DAY([@date])', 'HOUR([@date])', 'MINUTE([@date])'],
  }
  // When opening, request sample rows to preview
  useEffect(() => {
    if (open) onRequestRows?.(text)
  }, [open])
  useEffect(() => {
    if (autoOpen) setOpen(true)
  }, [autoOpen])
  // Recompute preview when formula or samples change
  useEffect(() => {
    if (!open) return
    setError(undefined)
    try {
      // quick validation for unclosed row references like "[@col"
      if (/\[@[^\]]*$/.test(text || '')) {
        setError('Unclosed row reference [@...]')
        setPreview([])
        return
      }
      const compiled = compileFormula(text || '')
      let take = Array.isArray(sampleRows) ? sampleRows.slice(0, 5) : []
      if (!take.length && samplesByField) {
        // Build synthetic rows from available samples for referenced columns
        const refs = parseReferences(text || '').row || []
        const maxLen = Math.max(1, ...refs.map((r) => (samplesByField[r]?.length || 0)))
        take = Array.from({ length: Math.min(5, maxLen) }).map((_, i) => {
          const obj: Record<string, any> = {}
          refs.forEach((r) => {
            const arr = samplesByField![r] || []
            obj[r] = arr.length ? arr[i % arr.length] : undefined
          })
          return obj
        })
      }
      let firstErr: string | undefined
      const out = take.map((r: any, i: number) => {
        try { return { i: i + 1, result: compiled.execDebug({ row: r }) } }
        catch (e: any) { if (!firstErr) firstErr = String(e?.message || e); return { i: i + 1, result: null } }
      })
      if (firstErr) setError(firstErr)
      setPreview(out)
    } catch (e: any) {
      setError(String(e?.message || 'Invalid formula'))
      setPreview([])
    }
  }, [open, text, sampleRows])
  return (
    <>
      {!hideTriggerButton && (
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-foreground" onClick={() => setOpen(true)}>
          {mode === 'edit' ? 'Edit custom column' : 'New custom column'}
        </button>
      )}
      {open && typeof window !== 'undefined' && createPortal(
        <div className="fixed inset-0 z-[999] flex items-center justify-center">
          <div className="absolute inset-0 bg-black/40" onClick={() => { setOpen(false); onClose?.() }} />
          <div className="relative z-[1000] w-[720px] max-w-[95vw] rounded-lg border bg-card p-4 shadow-none">
            <div className="flex items-center justify-between mb-2">
              <div className="text-sm font-medium">{mode === 'edit' ? 'Edit Custom Column' : 'New Custom Column'}</div>
              <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { setOpen(false); onClose?.() }}>✕</button>
            </div>
            <div className="grid grid-cols-[200px,1fr] gap-3">
              <div className="rounded-md p-2 bg-[hsl(var(--secondary)/0.6)] max-h-64 overflow-auto space-y-3">
                <div>
                  <div className="text-xs mb-1 text-muted-foreground">Columns</div>
                  <ul className="space-y-1 text-xs">
                    {columns.map((c) => (
                      <li key={c} className="flex items-center justify-between gap-1">
                        <span className="truncate" title={c}>{c}</span>
                        <span className="flex items-center gap-1">
                          <button className="text-[10px] px-1 py-0.5 rounded bg-[hsl(var(--btn4))] text-[hsl(var(--primary-foreground))]" onClick={() => setText((t) => (t ? `${t} [@${c}]` : `[@${c}]`))}>[@]</button>
                          <button className="text-[10px] px-1 py-0.5 rounded bg-[hsl(var(--btn3))] text-[hsl(var(--foreground))]" onClick={() => setText((t) => (t ? `${t} [${c}]` : `[${c}]`))}>[ ]</button>
                        </span>
                      </li>
                    ))}
                  </ul>
                </div>
                <div>
                  <div className="text-xs mb-1 text-muted-foreground">Functions</div>
                  {Object.entries(fnGroups).map(([grp, items]) => (
                    <div key={grp} className="mb-2">
                      <div className="text-[11px] font-medium">{grp}</div>
                      <div className="flex flex-wrap gap-1 mt-1">
                        {items.map((it) => (
                          <button key={it} className="text-[10px] px-1.5 py-0.5 rounded bg-[hsl(var(--muted))] border" onClick={() => setText((t) => (t ? `${t} ${it}` : it))}>{it}</button>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
              <div className="flex flex-col gap-2">
                <input className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" placeholder="Column name" value={name} onChange={(e) => setName(e.target.value)} />
                <textarea className="w-full min-h-[140px] px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] font-mono text-xs" value={text} onChange={(e) => setText(e.target.value)} placeholder="e.g., IF([@amount] > 100, 'High', 'Low')" />
                <div className="flex items-center gap-2">
                  <label className="text-xs text-muted-foreground">Type</label>
                  <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={ctype} onChange={(e) => setCtype(e.target.value as any)}>
                    {['number','string','date','boolean'].map((t) => (<option key={t} value={t}>{t}</option>))}
                  </select>
                </div>
                {/* Live preview */}
                <div className="rounded border bg-[hsl(var(--secondary)/0.6)] p-2">
                  <div className="flex items-center justify-between mb-1">
                    <div className="text-[11px] font-medium">Preview</div>
                    <button
                      type="button"
                      className="text-[11px] px-2 py-0.5 rounded border hover:bg-[hsl(var(--secondary)/0.6)]"
                      onClick={() => onRequestRows?.(text)}
                      title="Fetch fresh sample rows for this formula"
                    >
                      Test
                    </button>
                  </div>
                  {error && <div className="text-[11px] text-red-600">{error}</div>}
                  {!error && (
                    <div className="text-[11px] space-y-0.5">
                      {preview.length === 0 ? (
                        <div className="text-muted-foreground">No rows available for preview</div>
                      ) : (
                        preview.map((p) => (
                          <div key={p.i} className="flex items-center justify-between">
                            <span className="opacity-70">Row {p.i}</span>
                            <span className="font-mono">{String(p.result)}</span>
                          </div>
                        ))
                      )}
                      {/* Show first-row referenced field raw values for context */
}
                      {(() => {
                        const refs = parseReferences(text || '').row || []
                        if (!refs.length || !Array.isArray(sampleRows) || sampleRows.length === 0) return null
                        const first = sampleRows[0] || {}
                        return (
                          <div className="mt-2 text-[11px] opacity-80">
                            <div className="font-medium mb-0.5">Inputs (Row 1)</div>
                            {refs.map((r) => (
                              <div key={r} className="flex items-center justify-between">
                                <span className="mr-2">[@{r}]</span>
                                <span className="font-mono truncate max-w-[260px]" title={String(first[r])}>{String(first[r])}</span>
                              </div>
                            ))}
                          </div>
                        )
                      })()}
                    </div>
                  )}
                </div>
                <div className="flex gap-2 justify-end">
                  <button className="text-xs px-3 py-1.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-foreground" onClick={() => setText('')}>Clear</button>
                  <button className="text-xs px-3 py-1.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-foreground" onClick={() => {
                    if (!name || !text) return
                    if (mode === 'edit') { onSave?.(initial?.id, name, text, ctype) }
                    else { onCreate?.(name, text, ctype) }
                    setOpen(false); setName(''); setText(''); onClose?.()
                  }}>Save</button>
                </div>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}
    </>
  )
}

// Field filter editor for chart-level filters (multi-select values)
function FilterEditor({ field, source, datasourceId, values, where, onChange }: { field: string; source: string; datasourceId?: string; values?: any[]; where?: Record<string, any>; onChange: (vals: any[]) => void }) {
  const [selected, setSelected] = useState<any[]>(values || [])
  const [filterQuery, setFilterQuery] = useState('')
  useEffect(() => {
    const next = Array.isArray(values) ? values : []
    setSelected((prev) => {
      if (prev.length === next.length && prev.every((v, i) => v === next[i])) return prev
      return next
    })
  }, [JSON.stringify(values || [])])
  // Request fresh samples from TableCard whenever field or widget changes
  useEffect(() => {
    if (!local?.id) return
    try { window.dispatchEvent(new CustomEvent('request-table-samples', { detail: { widgetId: local.id } })) } catch {}
  }, [field, local?.id])
  // Fallback: fetch distinct values ignoring self constraint (server path for base fields)
  const [extraSamples, setExtraSamples] = useState<string[]>([])
  useEffect(() => {
    let abort = false
    async function run() {
      try {
        if (!source) return
        const omitWhere: Record<string, any> = { ...((where || {}) as any) }
        // Remove self constraints for the same field, including range ops
        Object.keys(omitWhere).forEach((k) => { if (k === field || k.startsWith(`${field}__`)) delete (omitWhere as any)[k] })
        // Try server-side DISTINCT endpoint first (fast, authoritative) if available
        if (typeof (Api as any).distinct === 'function') {
          try {
            const res = await (Api as any).distinct({ source: String(source), field: String(field), where: Object.keys(omitWhere).length ? omitWhere : undefined, datasourceId })
            const values = ((res?.values || []) as any[]).map((v) => (v != null ? String(v) : null)).filter((v) => v != null) as string[]
            const dedup = Array.from(new Set(values).values()).sort()
            if (!abort) setExtraSamples(dedup)
            return
          } catch {}
        }
        // Fallback: page through all rows to compute full distinct values
        const pageSize = 5000
        let offset = 0
        const set = new Set<string>()
        for (let i = 0; i < 50; i++) {
          const spec: any = { source, select: [field], where: Object.keys(omitWhere).length ? omitWhere : undefined, limit: pageSize, offset }
          const res = await QueryApi.querySpec({ spec, datasourceId, limit: pageSize, offset, includeTotal: true })
          const cols = (res.columns || []) as string[]
          const idx = Math.max(0, cols.indexOf(field))
          ;(res.rows || []).forEach((arr: any) => {
            const v = Array.isArray(arr) ? arr[idx] : undefined
            if (v !== null && v !== undefined) set.add(String(v))
          })
          const got = (res.rows || []).length
          const total = Number(res.totalRows || 0)
          offset += got
          if (got < pageSize || (total > 0 && offset >= total) || abort) break
        }
        if (!abort) setExtraSamples(Array.from(set.values()).sort())
      } catch {
        if (!abort) setExtraSamples([])
      }
    }
    // Only run this base-field loader when the field is not a custom column; custom path added below
    const isCustomName = (local?.customColumns || []).some((c) => c.name === field)
    if (!isCustomName) run()
    else setExtraSamples([])
    return () => { abort = true }
  }, [field, source, datasourceId, JSON.stringify(where), local?.customColumns])
  // Helpers to detect derived date part fields
  const DERIVED_RE = /^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/
  const isDerived = DERIVED_RE.test(field)
  const baseField = isDerived ? (field.match(DERIVED_RE) as RegExpMatchArray)[1] : null
  const partName = isDerived ? (field.match(DERIVED_RE) as RegExpMatchArray)[2] : null
  const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December']
  const monthShort = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
  const dayNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']
  const dayShort = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']
  const toDate = (v: any): Date | null => {
    if (v == null) return null
    if (v instanceof Date) return isNaN(v.getTime()) ? null : v
    const s = String(v).trim()
    if (!s) return null
    if (/^\d{10,13}$/.test(s)) { const n = Number(s); const ms = s.length === 10 ? n*1000 : n; const d = new Date(ms); return isNaN(d.getTime()) ? null : d }
    const norm = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
    let d = new Date(norm); if (!isNaN(d.getTime())) return d
    const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/); if (iso) { d = new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`); return isNaN(d.getTime())?null:d }
    const m = s.match(/^([0-1]?\d)\/([0-3]?\d)\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/)
    if (m) { const mm=Number(m[1])-1, dd=Number(m[2]), yyyy=Number(m[3]); const t=m[4]||'00:00:00'; d = new Date(`${yyyy}-${String(mm+1).padStart(2,'0')}-${String(dd).padStart(2,'0')}T${t.length===5?t+':00':t}`); return isNaN(d.getTime())?null:d }
    return null
  }
  const weekNumber = (d: Date): number => { const date = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate())); date.setUTCDate(date.getUTCDate() + 4 - (date.getUTCDay() || 7)); const yearStart = new Date(Date.UTC(date.getUTCFullYear(),0,1)); return Math.ceil(((date.getTime()-yearStart.getTime())/86400000+1)/7) }
  const derive = (baseVal: any): any => {
    const d = toDate(baseVal); if (!d) return null
    switch (partName) {
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
  const custom = (local?.customColumns || []).find(c => c.name === field)
  let computedSamples: string[] = []
  if (isDerived) {
    // Prefer sampleRows for accuracy; fallback to mapping samples of the base field
    if (Array.isArray(sampleRows) && sampleRows.length > 0 && baseField) {
      const set = new Set<string>()
      sampleRows.forEach((r) => { const v = derive((r as any)[baseField]); if (v !== null && v !== undefined) set.add(String(v)) })
      computedSamples = Array.from(set.values()).sort()
    } else if (baseField && samplesByField?.[baseField]) {
      const set = new Set<string>()
      ;(samplesByField[baseField] || []).forEach((s) => { const v = derive(s); if (v !== null && v !== undefined) set.add(String(v)) })
      computedSamples = Array.from(set.values()).sort()
    }
  } else if (custom) {
    if (Array.isArray(sampleRows) && sampleRows.length > 0) {
      try {
        const cf = compileFormula(custom.formula)
        const set = new Set<string>()
        sampleRows.forEach((r) => { const v = cf.exec({ row: r }); if (v !== null && v !== undefined) set.add(String(v)) })
        computedSamples = Array.from(set.values()).sort()
      } catch {}
    }
  }
  // Client-side fallback to compute distinct values for custom columns when no sampleRows
  // (moved outside conditional to comply with React Hooks rules)
  useEffect(() => {
    let abort = false
    async function runCustom() {
      try {
        if (!custom || !source || !datasourceId) return
        // If we already have sampleRows-derived values, skip
        if (Array.isArray(sampleRows) && sampleRows.length > 0) return
        const refs = Array.from(new Set((parseReferences(custom.formula).row || []) as string[]))
        if (refs.length === 0) { if (!abort) setExtraSamples([]); return }
        const DERIVED_RE = /^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/
        const customNames = new Set<string>((local?.customColumns || []).map((c) => c.name))
        const serverWhere: Record<string, any> = {}
        Object.entries((where || {}) as Record<string, any>).forEach(([k, v]) => { if (!DERIVED_RE.test(k) && !customNames.has(k)) (serverWhere as any)[k] = v })
        const spec: any = { source, select: refs, where: Object.keys(serverWhere).length ? serverWhere : undefined, limit: 500, offset: 0 }
        const res = await QueryApi.querySpec({ spec, datasourceId, limit: 500, offset: 0, includeTotal: false })
        const cols = (res.columns || []) as string[]
        const cf = compileFormula(custom.formula)
        const set = new Set<string>()
        ;(res.rows || []).forEach((arr: any[]) => {
          const row: Record<string, any> = {}
          cols.forEach((c, i) => { row[c] = arr[i] })
          try { const v = cf.exec({ row }); if (v !== null && v !== undefined) set.add(String(v)) } catch {}
        })
        if (!abort) setExtraSamples(Array.from(set.values()).sort())
      } catch {
        if (!abort) setExtraSamples([])
      }
    }
    runCustom()
    return () => { abort = true }
  }, [custom?.formula, source, datasourceId, JSON.stringify(where), Array.isArray(sampleRows) ? sampleRows.length : 0, local?.customColumns])
  const baseSamples = (samplesByField?.[field] || []) as string[]
  // Merge fallback distinct values with existing samples/computed
  const mergedPool = Array.from(new Set<string>([...computedSamples, ...baseSamples, ...extraSamples]))
  const samples = mergedPool.filter((v) => String(v).toLowerCase().includes(filterQuery.toLowerCase()))
  const toggle = (v: any) => {
    const exists = selected.some((x) => x === v)
    const next = exists ? selected.filter((x) => x !== v) : [...selected, v]
    setSelected(next)
    onChange(next)
  }
  
  return (
    <div className="rounded-md border bg-card p-2">
      <div className="flex items-center justify-between">
        <div className="text-xs font-medium">Filter values: {field}</div>
        <div className="flex items-center gap-2">
          <button
            className="text-xs px-2 py-1 rounded-md border hover:bg-muted"
            onClick={() => { setSelected([]); onChange([]) }}
            title="Clear all selections"
          >
            Clear
          </button>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => onChange(selected)}>Apply</button>
        </div>
      </div>
      <div className="mt-2">
        <input className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" placeholder="Search values" value={filterQuery} onChange={(e) => setFilterQuery(e.target.value)} />
      </div>
      <div className="max-h-56 overflow-auto mt-2">
        <ul className="space-y-1">
          {samples.map((v: any, i: number) => (
            <li key={i} className="flex items-center gap-2 text-xs">
              <Switch
                checked={selected.some((x) => x === v)}
                onChangeAction={() => toggle(v)}
              />
              <span className="truncate max-w-[240px]" title={String(v)}>{String(v)}</span>
            </li>
          ))}
          {samples.length === 0 && (
            <li className="text-xs text-muted-foreground">No samples available</li>
          )}
        </ul>
    </div>
    </div>
  )
}

// Number filter editor for Details panel (real-time sync)
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
  const [op, setOp] = useState<NumberOp>(initial.op)
  const [a, setA] = useState<number | ''>(initial.a ?? '')
  const [b, setB] = useState<number | ''>(initial.b ?? '')

  const emit = (nextOp: NumberOp, av: number | '', bv: number | '') => {
    const patch: Record<string, any> = { [`${field}__gt`]: undefined, [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined, [field]: undefined }
    const hasNum = (x: any) => typeof x === 'number' && !isNaN(x)
    switch (nextOp) {
      case 'eq': if (hasNum(av)) patch[field] = [av]; break
      case 'ne': if (hasNum(av)) patch[`${field}__ne`] = av; break
      case 'gt': if (hasNum(av)) patch[`${field}__gt`] = av; break
      case 'gte': if (hasNum(av)) patch[`${field}__gte`] = av; break
      case 'lt': if (hasNum(av)) patch[`${field}__lt`] = av; break
      case 'lte': if (hasNum(av)) patch[`${field}__lte`] = av; break
      case 'between': if (hasNum(av)) patch[`${field}__gte`] = av; if (hasNum(bv)) patch[`${field}__lte`] = bv; break
    }
    onPatch(patch)
  }

  useEffect(() => { emit(op, a, b) }, [op, a, b])

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

// Date range editor for Details panel (real-time sync, end-inclusive UX)
function DateRangeDetails({ field, where, onPatch }: { field: string; where?: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  const a0 = (where as any)?.[`${field}__gte`] as string | undefined
  const b0 = (where as any)?.[`${field}__lt`] as string | undefined
  const normalizeEnd = (b?: string) => {
    if (!b) return undefined
    const d = new Date(`${b}T00:00:00`); if (isNaN(d.getTime())) return undefined
    d.setDate(d.getDate() + 1)
    return d.toISOString().slice(0, 10)
  }
  const [start, setStart] = useState<string>(a0 || '')
  const [end, setEnd] = useState<string>(b0 ? (() => { const d = new Date(b0 + 'T00:00:00'); d.setDate(d.getDate() - 1); return d.toISOString().slice(0, 10) })() : '')

  useEffect(() => {
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

  // Pivot assignments derived from current config
  const initialPivot: PivotAssignments = useMemo(() => {
    const ql: any = (local?.querySpec || {})
    const vals = (ql.series && ql.series.length > 0)
      ? ql.series.filter((s: any) => !!s?.y).map((s: any) => ({
          field: s.y as string,
          agg: (s.agg as any) || 'count',
          label: s.label,
          colorToken: s.colorToken as 1|2|3|4|5 | undefined,
          stackId: s.stackId,
          style: s.style,
          secondaryAxis: !!s.secondaryAxis,
          conditionalRules: s.conditionalRules,
        }))
      : (ql.measure || ql.y)
        ? ([{ field: (ql.measure || ql.y) as string, agg: (ql.agg as any) || 'count' }])
        : (((selected as any).series || []).filter((s: any) => !!s.y).map((s: any) => ({ field: s.y, agg: (s.agg as any) || 'count', label: s.name })))
    const legend = (() => {
      const spec: any = local?.querySpec || {}
      // For Data Table mode, use select as the set of columns (multi)
      if (local?.type === 'table' && ((local?.options?.table?.tableType || 'data') === 'data')) {
        return (Array.isArray(spec.select) && spec.select.length > 0) ? spec.select : undefined
      }
      return spec.legend
    })()
    const filters = Array.from(new Set<string>([
      ...(((local?.pivot?.filters || []) as string[])),
      ...Object.keys(((local?.querySpec?.where as any) || {})),
    ]))
    return {
      x: (local?.querySpec as any)?.x || local?.series?.[0]?.x,
      values: vals,
      legend,
      filters,
    }
  }, [local?.querySpec, local?.series])

  const [pivot, setPivot] = useState<PivotAssignments>(initialPivot)
  // Only re-derive pivot when the selected widget changes, not on every local mutation
  useEffect(() => {
    if (!selected) return
    const q: any = (selected as any).querySpec || {}
    const vals = (q.series && q.series.length > 0)
      ? q.series.filter((s: any) => !!s?.y).map((s: any) => ({
          field: s.y as string,
          agg: (s.agg as any) || 'count',
          label: s.label,
          colorToken: s.colorToken as 1|2|3|4|5 | undefined,
          stackId: s.stackId,
          style: s.style,
          secondaryAxis: !!s.secondaryAxis,
          conditionalRules: s.conditionalRules,
        }))
      : (q.measure || q.y)
        ? ([{ field: (q.measure || q.y) as string, agg: (q.agg as any) || 'count' }])
        : (((selected as any).series || []).filter((s: any) => !!s.y).map((s: any) => ({ field: s.y, agg: (s.agg as any) || 'count', label: s.name })))
    const nextP: PivotAssignments = {
      x: q.x,
      values: vals,
      legend: Array.isArray(q.legend)
        ? (Array.isArray(q.select) ? q.select : undefined)
        : q.legend,
      filters: (() => {
        const prev = ((((selected as any)?.pivot?.filters) || []) as string[])
        const whereKeys = Object.keys(q.where || {})
        // Normalize to base field: strip __suffix operators (gte,lte,gt,lt,contains,notcontains,startswith,endswith,ne,eq)
        const baseOf = (k: string) => String(k).replace(/__(?:gte|lte|gt|lt|contains|notcontains|startswith|endswith|ne|eq)$/i, '')
        const merged = new Set<string>([...prev, ...whereKeys.map(baseOf)])
        return Array.from(merged.values())
      })(),
    }
    setPivot(nextP)
  }, [selected])

  const [selKind, setSelKind] = useState<'x'|'value'|'legend'|'filter'|null>(null)
  const [selField, setSelField] = useState<string | undefined>(undefined)

  function applyPivot(p: PivotAssignments) {
    // Clear Details selection if the selected chip/axis/value was removed
    try {
      if (selKind === 'filter' && selField && !(p.filters || []).includes(selField)) { setSelKind(null); setSelField(undefined) }
      if (selKind === 'x') {
        if ((pivot.x && p.x !== pivot.x) || (!p.x)) { setSelKind(null); setSelField(undefined) }
      }
      if (selKind === 'legend') {
        const nextLegends = Array.isArray((p as any).legend) ? ((p as any).legend as string[]) : ((p.legend ? [String(p.legend)] : []))
        if (selField && !nextLegends.includes(selField)) { setSelKind(null); setSelField(undefined) }
      }
      if (selKind === 'value') {
        const nextKeys = (p.values || []).map((v) => (v.measureId ? v.measureId : v.field)).filter(Boolean)
        if (selField && !nextKeys.includes(selField)) { setSelKind(null); setSelField(undefined) }
      }
    } catch {}
    // Determine which filters were removed (chips X)
    const removedFilters = (pivot?.filters || []).filter((f) => !(p.filters || []).includes(f))
    setPivot(p)
    // If this is a Data Table (non-pivot), use Columns zone as selected columns and update querySpec.select
    if (local?.type === 'table' && ((local?.options?.table?.tableType || 'data') === 'data')) {
      const fields = Array.isArray((p as any).legend) ? ((p as any).legend as string[]) : (p.legend ? [p.legend] : [])
      // Merge where keys to reflect filters chips; keep existing values if any
      const curWhere = { ...((local?.querySpec?.where as any) || {}) }
      Object.keys(curWhere).forEach((k) => { if (!(p.filters || []).includes(k)) delete curWhere[k] })
      const nextSpec = {
        ...(local?.querySpec || { source: '' }),
        select: (fields.length > 0 ? fields : undefined),
        where: curWhere,
      } as any
      const next = {
        ...local!,
        queryMode: 'spec' as const,
        querySpec: nextSpec,
        // persist pivot UI state for data table too
        pivot: {
          x: p.x,
          legend: p.legend,
          values: p.values,
          filters: p.filters,
        } as any,
        options: (() => {
          const prevExpose = { ...((local?.options?.filtersExpose) || {}) }
          removedFilters.forEach((f) => { if (f in prevExpose) prevExpose[f] = false })
          return { ...(local?.options || {}), filtersExpose: Object.keys(prevExpose).length ? prevExpose : undefined }
        })(),
      } as WidgetConfig
      setLocal(next)
      updateConfig(next)
      return
    }
    // Build series[] candidates for multi-series mode (Charts)
    const nextSeriesQs = (p.values || []).map((v) => {
      if (v.measureId) {
        const m = (local?.measures || []).find(mm => mm.id === v.measureId)
        return { label: v.label || m?.name, measure: m?.formula, colorToken: v.colorToken, stackId: v.stackId, style: v.style, conditionalRules: v.conditionalRules, secondaryAxis: v.secondaryAxis }
      }
      return { label: v.label, y: v.field, agg: (v.agg || 'count') as any, colorToken: v.colorToken, stackId: v.stackId, style: v.style, conditionalRules: v.conditionalRules, secondaryAxis: v.secondaryAxis }
    })
    const nextSeriesTop = (p.values || []).map((v, i) => ({ id: `s${i + 1}`, x: p.x || '', y: v.field || '', agg: (v.agg || 'count') as any, secondaryAxis: v.secondaryAxis }))

    // Start from existing spec to preserve where/limit/offset
    const baseSpec: any = { ...(local?.querySpec || { source: '' }) }
    baseSpec.x = p.x
    baseSpec.legend = p.legend
    // Preserve existing where; do not add empty constraints for newly exposed filters
    const curWhere = { ...(baseSpec.where || {}) }
    // prune removed filters
    for (const k of Object.keys(curWhere)) {
      if (!(p.filters || []).includes(k)) delete curWhere[k]
    }
    baseSpec.where = curWhere

    // For KPI widgets, keep top-level aggregator (y/agg/measure) instead of multi-series
    if ((local?.type || '').toLowerCase() === 'kpi') {
      if ((p.values?.length || 0) > 0) {
        const v0 = p.values[0]
        if (v0?.measureId) {
          const m = (local?.measures || []).find(mm => mm.id === v0.measureId)
          delete baseSpec.y
          delete baseSpec.agg
          baseSpec.measure = m?.formula
        } else if (v0?.field) {
          baseSpec.y = v0.field
          baseSpec.agg = (v0.agg || 'count') as any
          delete baseSpec.measure
        }
      } else {
        delete baseSpec.y
        delete baseSpec.agg
        delete baseSpec.measure
      }
      delete baseSpec.series
    } else {
      // Charts: prefer multi-series QuerySpec so ChartCard can virtualize categories per series when legend is set
      if ((p.values?.length || 0) > 0) {
        baseSpec.series = nextSeriesQs as any
        delete baseSpec.y
        delete baseSpec.agg
        delete baseSpec.measure
      } else {
        delete baseSpec.series
      }
    }

    const next = {
      ...local!,
      queryMode: 'spec' as const,
      series: nextSeriesTop,
      querySpec: baseSpec,
      pivot: {
        x: p.x,
        legend: p.legend,
        values: p.values.map(v => ({ field: v.field, measureId: v.measureId, agg: v.agg, label: v.label, secondaryAxis: v.secondaryAxis, sort: (v as any).sort } as any)),
        filters: p.filters,
      } as any,
      options: (() => {
        const prevExpose = { ...((local?.options?.filtersExpose) || {}) }
        removedFilters.forEach((f) => { prevExpose[f] = false })
        return { ...(local?.options || {}), filtersExpose: Object.keys(prevExpose).length ? prevExpose : undefined }
      })(),
    } as WidgetConfig
    setLocal(next)
    updateConfig(next)
  }

  useEffect(() => {
    setLocal(selected)
  }, [selected])

  // Prefill sensible defaults for table widgets (one-time on mount/selection change)
  useEffect(() => {
    if (!local || local.type !== 'table') return
    const existing = local.options?.table
    if (existing) return
    const defaults = {
      tableType: 'data' as const,
      // Disable Pivot controls (PivotTableUI) by default; users configure via Configurator selectors
      pivotUI: false,
      // Default Pivot styling (matches provided screenshot)
      pivotStyle: { headerRowHeight: 16, headerFontSize: 13, cellRowHeight: 20, cellFontSize: 14, hideColAxisLabel: true } as any,
      theme: 'quartz' as const,
      density: 'compact' as const,
      defaultCol: { sortable: true, filter: true, resizable: true, floatingFilter: true },
      aggregation: { omitAggNameInHeader: true, grandTotalRow: 'bottom' as const },
      filtering: { quickFilter: true },
      selection: { mode: 'none' as const, checkbox: false },
      interactions: { columnMove: true, columnResize: true, columnHoverHighlight: true, suppressRowHoverHighlight: false },
      performance: { domLayout: 'autoHeight' as const },
    }
    const next = { ...local, options: { ...(local.options || {}), table: defaults } }
    setLocal(next)
    updateConfig(next)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [local?.id])

  if (!local) {
    return (
      <div className="text-sm text-muted-foreground">Select a widget on the canvas to configure it.</div>
    )
  }

  return (
    <div className="space-y-3">
      {/* Card Config */}
      {local && (
        <Section title="Card Config" defaultOpen>
          <div className="grid grid-cols-2 gap-2">
            <div className="col-span-2">
              <label className="block text-xs text-muted-foreground mb-1">Card Title</label>
              <input
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
                value={local.title}
                onChange={(e) => {
                  const next = { ...local, title: e.target.value }
                  setLocal(next)
                  updateConfig(next)
                }}
              />
            </div>
            <label className="flex items-center gap-2 text-xs">
              <Switch
                checked={local.options?.showCardHeader !== false}
                onChangeAction={(checked) => { const opts = { ...(local.options || {}), showCardHeader: checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}
              />
              Show card header
            </label>
            {/* Removed: Disable elements outline (always disabled globally) */}
            <div>
              <label className="block text-xs text-muted-foreground mb-1">Card fill</label>
              <select
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
                value={local.options?.cardFill || 'default'}
                onChange={(e) => { const val = e.target.value as any; const opts = { ...(local.options || {}), cardFill: val }; if (val === 'custom' && !opts.cardCustomColor) opts.cardCustomColor = '#ffffff'; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}
              >
                {(['default','transparent','custom'] as const).map((p) => <option key={p} value={p}>{p === 'transparent' ? 'No Fill' : p}</option>)}
              </select>
            </div>
            {local.options?.cardFill === 'custom' && (
              <div>
                <label className="block text-xs text-muted-foreground mb-1">Custom color</label>
                <input
                  type="color"
                  className="w-full h-8 rounded-md bg-[hsl(var(--secondary)/0.6)]"
                  value={local.options?.cardCustomColor || '#ffffff'}
                  onChange={(e) => { const opts = { ...(local.options || {}), cardCustomColor: e.target.value }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}
                />
              </div>
            )}
            <label className="flex items-center gap-2 text-xs">
              <Switch
                checked={local.options?.autoFitCardContent !== false}
                onChangeAction={(checked) => { const opts = { ...(local.options || {}), autoFitCardContent: checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}
              />
              Autofit card content
            </label>
            <label className="flex items-center gap-2 text-xs">
              <Switch
                checked={!!((local.options as any)?.showLoadTime)}
                onChangeAction={(checked) => {
                  const opts = { ...(local.options || {}), showLoadTime: checked || undefined }
                  const next = { ...local, options: opts }
                  setLocal(next); updateConfig(next)
                }}
              />
              Show load time
            </label>
          </div>
          {local?.type === 'chart' ? (
          <Accordion type="single" collapsible>
            <AccordionItem value="format-title">
              <AccordionTrigger>Format Title</AccordionTrigger>
              <AccordionContent>
                <div className="border rounded-md p-2 space-y-2">
                  <div className="grid grid-cols-2 gap-2">
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Position</label>
                      <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={(local.options as any)?.chartTitlePosition || 'none'}
                        onChange={(e) => { const opts = { ...(local.options || {}), chartTitlePosition: e.target.value as any }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }}>
                        {(['none','above','below'] as const).map(p => <option key={p} value={p}>{p}</option>)}
                      </select>
                    </div>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Align</label>
                      <div className="flex items-center gap-1 justify-start">
                        {(['left','center','right'] as const).map(a => (
                          <button key={a} type="button" title={a}
                            className={`px-2 py-1 rounded-md border text-xs ${(((local.options as any)?.chartTitleAlign || 'left') === a) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`}
                            onClick={() => { const opts = { ...(local.options || {}), chartTitleAlign: a as any }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }}>
                            {a === 'left' ? (<RiAlignLeft className="h-4 w-4" aria-hidden="true" />) : a === 'center' ? (<RiAlignCenter className="h-4 w-4" aria-hidden="true" />) : (<RiAlignRight className="h-4 w-4" aria-hidden="true" />)}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Size (px)</label>
                      <input type="number" min={10} max={24} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={(local.options as any)?.chartTitleSize ?? 13}
                        onChange={(e) => { const v = Math.max(10, Math.min(24, Number(e.target.value||13))); const opts = { ...(local.options || {}), chartTitleSize: v }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                    </div>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Style</label>
                      <div className="flex items-center gap-1">
                        {(['normal','bold','italic','underline'] as const).map(s => (
                          <label key={s} className={`px-2 py-1 rounded-md border text-xs cursor-pointer ${(((local.options as any)?.chartTitleEmphasis || 'normal') === s) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`} title={s}>
                            <input type="radio" name="chartTitleStyle" className="sr-only" checked={((local.options as any)?.chartTitleEmphasis || 'normal') === s} onChange={() => {
                              const opts = { ...(local.options || {}), chartTitleEmphasis: s as any };
                              const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next)
                            }} />
                            {s === 'normal' ? 'N' : s === 'bold' ? (<RiBold className="h-4 w-4" aria-hidden="true" />) : s === 'italic' ? (<RiItalic className="h-4 w-4" aria-hidden="true" />) : (<RiUnderline className="h-4 w-4" aria-hidden="true" />)}
                          </label>
                        ))}
                      </div>
                    </div>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Font Color</label>
                      <div className="flex items-center gap-2">
                        {(['auto','custom'] as const).map(m => (
                          <label key={m} className={`px-2 py-1 rounded-md border text-xs cursor-pointer ${(((local.options as any)?.chartTitleColorMode || 'auto') === m) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`}>
                            <input type="radio" name="titleColorMode" className="sr-only" checked={((local.options as any)?.chartTitleColorMode || 'auto') === m} onChange={() => { const opts = { ...(local.options || {}), chartTitleColorMode: m as any }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                            {m}
                          </label>
                        ))}
                        {(((local.options as any)?.chartTitleColorMode || 'auto') === 'custom') && (
                          <input type="color" className="h-[30px] w-[44px] rounded-md bg-[hsl(var(--secondary))]" value={(local.options as any)?.chartTitleColor || '#111827'}
                            onChange={(e) => { const val = e.target.value || undefined; const opts = { ...(local.options || {}), chartTitleColor: val }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                        )}
                      </div>
                    </div>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Background</label>
                      <div className="flex items-center gap-2">
                        {(['none','custom'] as const).map(m => (
                          <label key={m} className={`px-2 py-1 rounded-md border text-xs cursor-pointer ${(((local.options as any)?.chartTitleBgMode || 'none') === m) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`}>
                            <input type="radio" name="titleBgMode" className="sr-only" checked={((local.options as any)?.chartTitleBgMode || 'none') === m} onChange={() => { const opts = { ...(local.options || {}), chartTitleBgMode: m as any }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                            {m === 'none' ? 'No fill' : 'Custom'}
                          </label>
                        ))}
                        {(((local.options as any)?.chartTitleBgMode || 'none') === 'custom') && (
                          <input type="color" className="h-[30px] w-[44px] rounded-md bg-[hsl(var(--secondary))]" value={(local.options as any)?.chartTitleBgColor || '#ffffff'}
                            onChange={(e) => { const val = e.target.value || undefined; const opts = { ...(local.options || {}), chartTitleBgColor: val }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                        )}
                      </div>
                    </div>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Margin</label>
                      <div className="flex items-center gap-1">
                        {(['none','sm','md','lg'] as const).map(m => (
                          <button key={m} type="button" title={`Margin ${m}`}
                            className={`px-2 py-1 rounded-md border text-xs ${(((local.options as any)?.chartTitleMargin || 'sm') === m) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`}
                            onClick={() => { const opts = { ...(local.options || {}), chartTitleMargin: m as any }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }}>
                            {m === 'none' ? '0' : m === 'sm' ? 'S' : m === 'md' ? 'M' : 'L'}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <label className="text-xs text-muted-foreground">Outline</label>
                      <Switch checked={!!((local.options as any)?.chartTitleOutline)}
                        onChangeAction={(checked) => { const opts = { ...(local.options || {}), chartTitleOutline: checked }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                    </div>
                    <div className="col-span-2">
                      <label className="block text-xs text-muted-foreground mb-1">Gaps (px)</label>
                      <div className="grid grid-cols-4 gap-2">
                        <div>
                          <label className="block text-[11px] text-muted-foreground">Top</label>
                          <input type="number" min={0} max={48} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartTitleGapTop ?? 0)}
                            onChange={(e) => { const v = Math.max(0, Math.min(48, Number(e.target.value||0))); const opts = { ...(local.options || {}), chartTitleGapTop: v }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                        </div>
                        <div>
                          <label className="block text-[11px] text-muted-foreground">Right</label>
                          <input type="number" min={0} max={48} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartTitleGapRight ?? 0)}
                            onChange={(e) => { const v = Math.max(0, Math.min(48, Number(e.target.value||0))); const opts = { ...(local.options || {}), chartTitleGapRight: v }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                        </div>
                        <div>
                          <label className="block text-[11px] text-muted-foreground">Bottom</label>
                          <input type="number" min={0} max={48} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartTitleGapBottom ?? 0)}
                            onChange={(e) => { const v = Math.max(0, Math.min(48, Number(e.target.value||0))); const opts = { ...(local.options || {}), chartTitleGapBottom: v }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                        </div>
                        <div>
                          <label className="block text-[11px] text-muted-foreground">Left</label>
                          <input type="number" min={0} max={48} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartTitleGapLeft ?? 0)}
                            onChange={(e) => { const v = Math.max(0, Math.min(48, Number(e.target.value||0))); const opts = { ...(local.options || {}), chartTitleGapLeft: v }; const next = { ...local!, options: opts } as WidgetConfig; setLocal(next); updateConfig(next) }} />
                        </div>
                      </div>
                    </div>
                  </div>
                  <div className="text-[11px] text-muted-foreground">Above/Below render the label inside the chart body. Color &quot;auto&quot; uses dark gray in light mode and white in dark mode. Background fill optional. Gaps add extra spacing around the title.</div>
                </div>
              </AccordionContent>
            </AccordionItem>
          </Accordion>
          ) : null}
        </Section>
      )}
      {local?.type === 'text' && (
        <Section title="Text Content" defaultOpen>
          <div className="space-y-2">
            <div className="text-[11px] text-muted-foreground">Use the editor to format your content: bold, italic, underline, links, alignment, font size, weight, and color.</div>
            <RichTextEditor
              value={local?.options?.text?.html || ''}
              onChange={(html) => {
                const text = { ...(local?.options?.text || {}), html }
                const next = { ...local!, options: { ...(local?.options || {}), text } }
                setLocal(next)
                updateConfig(next as any)
              }}
              height={220}
            />
            <label className="flex items-center gap-2 text-xs">
              <Switch
                checked={!!local?.options?.text?.sanitizeHtml}
                onChangeAction={(checked) => {
                  const text = { ...(local?.options?.text || {}), sanitizeHtml: checked }
                  const next = { ...local!, options: { ...(local?.options || {}), text } }
                  setLocal(next)
                  updateConfig(next as any)
                }}
              />
              Sanitize HTML (recommended for untrusted content)
            </label>
            <div className="grid grid-cols-2 gap-2 items-center">
              <label className="text-xs text-muted-foreground">Image URL</label>
              <input
                className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
                value={String(local?.options?.text?.imageUrl || '')}
                onChange={(e) => {
                  const text = { ...(local?.options?.text || {}), imageUrl: e.target.value }
                  const next = { ...local!, options: { ...(local?.options || {}), text } }
                  setLocal(next)
                  updateConfig(next as any)
                }}
                placeholder="https://..."
              />
              <label className="text-xs text-muted-foreground">Alt text</label>
              <input
                className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
                value={String(local?.options?.text?.imageAlt || '')}
                onChange={(e) => {
                  const text = { ...(local?.options?.text || {}), imageAlt: e.target.value }
                  const next = { ...local!, options: { ...(local?.options || {}), text } }
                  setLocal(next)
                  updateConfig(next as any)
                }}
                placeholder="description"
              />
              <label className="text-xs text-muted-foreground">Image align</label>
              <select
                className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
                value={String(local?.options?.text?.imageAlign || 'left')}
                onChange={(e) => {
                  const text = { ...(local?.options?.text || {}), imageAlign: e.target.value as any }
                  const next = { ...local!, options: { ...(local?.options || {}), text } }
                  setLocal(next)
                  updateConfig(next as any)
                }}
              >
                {['left','center','right'].map((a) => (<option key={a} value={a}>{a}</option>))}
              </select>
              <label className="text-xs text-muted-foreground">Image width (px)</label>
              <input
                type="number"
                min={16}
                max={2048}
                className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
                value={Number(local?.options?.text?.imageWidth || 64)}
                onChange={(e) => {
                  const v = Math.max(16, Math.min(2048, Number(e.target.value) || 64))
                  const text = { ...(local?.options?.text || {}), imageWidth: v }
                  const next = { ...local!, options: { ...(local?.options || {}), text } }
                  setLocal(next)
                  updateConfig(next as any)
                }}
              />
            </div>
          </div>
        </Section>
      )}
      {local?.type === 'spacer' && (
        <Section title="Spacer Options" defaultOpen>
          <div className="grid grid-cols-2 gap-2">
            <label className="text-xs text-muted-foreground self-center">Min width (columns)</label>
            <input
              type="number"
              min={1}
              max={12}
              className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
              value={Number((local?.options?.spacer?.minW ?? 2) as any)}
              onChange={(e) => {
                const v = Math.max(1, Math.min(12, Number(e.target.value) || 1))
                const spacer = { ...(local?.options?.spacer || {}), minW: v }
                const next = { ...local!, options: { ...(local?.options || {}), spacer } }
                setLocal(next)
                updateConfig(next as any)
              }}
            />
          </div>
          <div className="text-[11px] text-muted-foreground mt-1">This constrains the card&#39;s minimum width in the grid.</div>
        </Section>
      )}
      {local?.type === 'composition' && (
        <Section title="Composition Options" defaultOpen>
          <div className="grid grid-cols-2 gap-2 items-center">
            <label className="text-xs text-muted-foreground">Columns</label>
            <select
              className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
              value={String(local?.options?.composition?.columns || 12)}
              onChange={(e) => {
                const cols = Number(e.target.value) as 6|8|12
                const composition = { ...(local?.options?.composition || { components: [] }), columns: cols }
                const next = { ...local!, options: { ...(local?.options || {}), composition } }
                setLocal(next)
                updateConfig(next as any)
              }}
            >
              {[6,8,12].map((n) => (<option key={n} value={n}>{n}</option>))}
            </select>
            <label className="text-xs text-muted-foreground">Layout</label>
            <select
              className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
              value={String((local?.options?.composition?.layout || 'grid'))}
              onChange={(e) => {
                const layout = (e.target.value === 'stack' ? 'stack' : 'grid') as 'grid'|'stack'
                const composition = { ...(local?.options?.composition || { components: [] }), layout }
                const next = { ...local!, options: { ...(local?.options || {}), composition } }
                setLocal(next)
                updateConfig(next as any)
              }}
            >
              <option value="grid">Grid</option>
              <option value="stack">Stack</option>
            </select>
            <label className="text-xs text-muted-foreground">Gap</label>
            <select
              className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
              value={String((local?.options?.composition?.gap ?? 2))}
              onChange={(e) => {
                const gap = Number(e.target.value)
                const composition = { ...(local?.options?.composition || { components: [] }), gap }
                const next = { ...local!, options: { ...(local?.options || {}), composition } }
                setLocal(next)
                updateConfig(next as any)
              }}
            >
              {[0,1,2,3,4,5,6].map((n) => (<option key={n} value={n}>{n}</option>))}
            </select>
          </div>
          <div className="mt-2">
            <button
              className="text-xs px-2 py-1 rounded-md border hover:bg-muted"
              onClick={() => setCompOpen(true)}
            >
              Open Card Builder
            </button>
          </div>
          {typeof window !== 'undefined' && (
            <CompositionBuilderModal
              open={compOpen}
              onClose={() => setCompOpen(false)}
              value={local?.options?.composition?.components || []}
              columns={(local?.options?.composition?.columns as 6|8|12) || 12}
              choices={Object.values(allWidgets || {}).map((w) => ({ id: w.id, title: w.title, type: w.type }))}
              onQuickAdd={quickAddAction}
              onChange={(next: CompositionComponent[]) => {
                const composition = { ...(local?.options?.composition || {}), components: next }
                const cfg = { ...local!, options: { ...(local?.options || {}), composition } } as WidgetConfig
                setLocal(cfg)
                updateConfig(cfg)
              }}
            />
          )}
        </Section>
      )}
      {/* Chart Config */}
      {local.type === 'chart' && (
        <Section title="Chart Config" defaultOpen>
          <div className="space-y-4">
            {/* Title moved to Card Config */}
            {/* Chart types selector (always visible) */}
            <div>
              <label className="block text-xs text-muted-foreground mb-1">Chart Type</label>
              <div className="grid grid-cols-3 gap-2 h-[240px] overflow-y-scroll overflow-x-hidden p-1" style={{ scrollbarGutter: 'stable' }}>
                {(['column', 'bar', 'area', 'line', 'donut', 'categoryBar', 'spark', 'combo', 'progress', 'tracker', 'badges', 'scatter', 'gantt', 'tremorTable', 'heatmap', 'barList'] as const).map((t) => (
                  <label key={t} className={`w-full flex flex-col items-center gap-1 p-2 rounded-md border cursor-pointer text-xs ${((local.chartType || 'line') === t) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))] ring-offset-2 ring-offset-[hsl(var(--card))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`}>
                    <input
                      type="radio"
                      name="chartType"
                      className="sr-only"
                      checked={(local.chartType || 'line') === t}
                      onChange={() => {
                        const next = { ...local, chartType: t }
                        setLocal(next)
                        updateConfig(next)
                      }}
                    />
                    <span className="w-10 h-8 bg-card rounded relative overflow-hidden">
                      {t === 'column' && (
                        <>
                          <span className="absolute bottom-1 left-1 w-2 h-5 bg-blue-500" />
                          <span className="absolute bottom-1 left-4 w-2 h-3 bg-blue-400" />
                          <span className="absolute bottom-1 left-7 w-2 h-6 bg-blue-300" />
                        </>
                      )}
                      {t === 'bar' && (
                        <>
                          <span className="absolute left-1 top-1 h-1 w-7 bg-blue-500" />
                          <span className="absolute left-1 top-3 h-1 w-5 bg-blue-400" />
                          <span className="absolute left-1 top-5 h-1 w-8 bg-blue-300" />
                        </>
                      )}
                      {t === 'line' && (
                        <svg className="w-full h-full" viewBox="0 0 40 32">
                          <polyline points="2,28 12,18 22,22 32,8 38,12" fill="none" stroke="#3b82f6" strokeWidth="2" />
                        </svg>
                      )}
                      {t === 'area' && (
                        <svg className="w-full h-full" viewBox="0 0 40 32">
                          <polyline points="2,28 12,18 22,22 32,8 38,12" fill="#93c5fd" stroke="#60a5fa" strokeWidth="1" />
                        </svg>
                      )}
                      {t === 'donut' && (
                        <svg className="w-full h-full" viewBox="0 0 40 32">
                          <circle cx="20" cy="16" r="10" fill="#e5e7eb" />
                          <path d="M20 6 A10 10 0 0 1 30 16 L20 16 Z" fill="#3b82f6" />
                          <circle cx="20" cy="16" r="5" fill="#fff" />
                        </svg>
                      )}
                      {t === 'categoryBar' && (
                        <div className="w-full h-full flex items-center px-1 gap-1">
                          <span className="h-2 flex-1 bg-blue-500 rounded-sm" />
                          <span className="h-2 flex-1 bg-emerald-500 rounded-sm" />
                          <span className="h-2 flex-1 bg-amber-500 rounded-sm" />
                        </div>
                      )}
                      {t === 'spark' && (
                        <svg className="w-full h-full" viewBox="0 0 40 32">
                          <polyline points="2,24 8,18 12,20 18,10 26,16 34,8 38,12" fill="none" stroke="#3b82f6" strokeWidth="1" />
                        </svg>
                      )}
                      {t === 'combo' && (
                        <svg className="w-full h-full" viewBox="0 0 40 32">
                          <rect x="6" y="18" width="6" height="10" fill="#60a5fa" />
                          <rect x="16" y="12" width="6" height="16" fill="#34d399" />
                          <polyline points="2,28 12,16 22,20 32,8 38,12" fill="none" stroke="#f59e0b" strokeWidth="2" />
                        </svg>
                      )}
                      {t === 'progress' && (
                        <div className="w-full h-full flex items-center px-1 gap-1">
                          <span className="h-1 flex-1 bg-blue-500 rounded-sm" />
                          <span className="h-1 w-3 bg-[hsl(var(--border))] rounded-sm" />
                        </div>
                      )}
                      {t === 'tracker' && (
                        <div className="w-full h-full flex items-center px-1 gap-[2px]">
                          {Array.from({length:6}).map((_,i)=>(<span key={i} className={`w-1 h-2 ${i<4?'bg-blue-500':'bg-[hsl(var(--border))]'} rounded-sm`} />))}
                        </div>
                      )}
                      {t === 'badges' && (
                        <div className="w-full h-full flex items-center justify-center gap-1">
                          <span className="px-1 rounded bg-blue-100 text-[10px]">A</span>
                          <span className="px-1 rounded bg-emerald-100 text-[10px]">B</span>
                        </div>
                      )}
                      {t === 'scatter' && (
                        <svg className="w-full h-full" viewBox="0 0 40 32">
                          <circle cx="8" cy="20" r="2" fill="#3b82f6" />
                          <circle cx="16" cy="14" r="2" fill="#3b82f6" />
                          <circle cx="24" cy="10" r="2" fill="#3b82f6" />
                          <circle cx="32" cy="18" r="2" fill="#3b82f6" />
                        </svg>
                      )}
                      {t === 'gantt' && (
                        <svg className="w-full h-full" viewBox="0 0 40 32" aria-hidden="true">
                          <rect x="4" y="6" width="12" height="4" rx="2" fill="#60a5fa" />
                          <rect x="8" y="12" width="20" height="4" rx="2" fill="#34d399" />
                          <rect x="18" y="18" width="14" height="4" rx="2" fill="#f59e0b" />
                          <rect x="10" y="24" width="8" height="4" rx="2" fill="#a78bfa" />
                          <line x1="4" y1="4" x2="36" y2="4" stroke="hsl(var(--border))" />
                          <line x1="4" y1="30" x2="36" y2="30" stroke="hsl(var(--border))" />
                        </svg>
                      )}
                      {t === 'tremorTable' && (
                        <svg className="w-full h-full" viewBox="0 0 40 32" aria-hidden="true">
                          {/* table container */}
                          <rect x="2" y="6" width="36" height="20" fill="hsl(var(--card))" stroke="hsl(var(--border))" strokeWidth="1" rx="2" />
                          {/* header band */}
                          <rect x="2" y="6" width="36" height="6" fill="hsl(var(--muted))" />
                          {/* column divider */}
                          <line x1="12" y1="6" x2="12" y2="26" stroke="hsl(var(--border))" strokeWidth="1" />
                          <line x1="2" y1="12" x2="38" y2="12" stroke="hsl(var(--border))" strokeWidth="1" />
                          <line x1="2" y1="16" x2="38" y2="16" stroke="hsl(var(--border))" strokeWidth="1" />
                          <line x1="2" y1="20" x2="38" y2="20" stroke="hsl(var(--border))" strokeWidth="1" />
                          <line x1="2" y1="24" x2="38" y2="24" stroke="hsl(var(--border))" strokeWidth="1" />
                          {/* badges/progress hints per row */}
                          <circle cx="7" cy="14" r="1.5" fill="#3b82f6" />
                          <rect x="14" y="13" width="16" height="2" fill="#93c5fd" rx="1" />
                          <circle cx="7" cy="18" r="1.5" fill="#10b981" />
                          <rect x="14" y="17" width="12" height="2" fill="#86efac" rx="1" />
                          <circle cx="7" cy="22" r="1.5" fill="#f59e0b" />
                          <rect x="14" y="21" width="20" height="2" fill="#fde68a" rx="1" />
                        </svg>
                      )}
                      {t === 'heatmap' && (
                        <div className="w-full h-full grid grid-cols-5 grid-rows-3 gap-[2px] p-[3px]">
                          {Array.from({ length: 15 }).map((_, i) => (
                            <span key={i} className={`rounded-sm ${i%3===0?'bg-blue-400':i%3===1?'bg-blue-300':'bg-blue-200'}`} />
                          ))}
                        </div>
                      )}
                      {t === 'barList' && (
                        <div className="w-full h-full flex flex-col items-start justify-center gap-[2px] px-1">
                          <span className="h-1.5 w-8 bg-blue-500 rounded-sm" />
                          <span className="h-1.5 w-7 bg-emerald-500 rounded-sm" />
                          <span className="h-1.5 w-6 bg-amber-500 rounded-sm" />
                          <span className="h-1.5 w-4 bg-violet-500 rounded-sm" />
                        </div>
                      )}
                    </span>
                    <span className="capitalize">{t === 'tremorTable' ? 'table' : (t === 'barList' ? 'bar list' : t)}</span>
                  </label>
                ))}
              </div>
            </div>
            {/* Tremor Tabs with icons under Chart Type */}
            <TabGroup
              index={chartTab==='appearance'?0:chartTab==='tooltip'?1:chartTab==='axis'?2:3}
              onIndexChange={(i) => setChartTab(i===0?'appearance': i===1?'tooltip': i===2?'axis':'grid')}
            >
              <TabList variant="solid" className="text-xs bg-[hsl(var(--secondary)/0.6)] rounded-md p-1">
                <Tab className="px-3 py-1 rounded-md text-muted-foreground whitespace-nowrap data-[state=active]:bg-card data-[state=active]:text-foreground data-[state=active]:shadow-sm data-[state=active]:border data-[state=active]:border-[hsl(var(--border))]">
                  <span className="flex items-center gap-1">
                    <RiPaletteLine className="size-4" aria-hidden="true" />
                    <span>Appearance</span>
                  </span>
                </Tab>
                <Tab className="px-3 py-1 rounded-md text-muted-foreground whitespace-nowrap data-[state=active]:bg-card data-[state=active]:text-foreground data-[state=active]:shadow-sm data-[state=active]:border data-[state=active]:border-[hsl(var(--border))]">
                  <span className="flex items-center gap-1">
                    <RiInformationLine className="size-4" aria-hidden="true" />
                    <span>Tooltip</span>
                  </span>
                </Tab>
                <Tab className="px-3 py-1 rounded-md text-muted-foreground whitespace-nowrap data-[state=active]:bg-card data-[state=active]:text-foreground data-[state=active]:shadow-sm data-[state=active]:border data-[state=active]:border-[hsl(var(--border))]">
                  <span className="flex items-center gap-1">
                    <RiRulerLine className="size-4" aria-hidden="true" />
                    <span>Axis</span>
                  </span>
                </Tab>
                <Tab className="px-3 py-1 rounded-md text-muted-foreground whitespace-nowrap data-[state=active]:bg-card data-[state=active]:text-foreground data-[state=active]:shadow-sm data-[state=active]:border data-[state=active]:border-[hsl(var(--border))]">
                  <span className="flex items-center gap-1">
                    <RiGridLine className="size-4" aria-hidden="true" />
                    <span>Grid</span>
                  </span>
                </Tab>
              </TabList>
              <TabPanels className="relative z-10 overflow-visible">
                <TabPanel>
                  <div className="max-h-[300px] overflow-y-auto pr-1 space-y-3">
                    {/* Behavior (moved to Card Config) */}
                    {/* Tabs from filter field */}
                    <TabsControls local={local} setLocalAction={setLocal} updateConfigAction={updateConfig} allFieldNames={allFieldNames} />
                    {/* Sort / Top N (aggregated) */}
                    <div className="border rounded-md p-2 space-y-2">
                      <div className="text-[11px] font-medium text-muted-foreground">Per-widget Sort / Top N</div>
                      {(() => {
                        const optsAny = (local.options || {}) as any
                        const dd = (optsAny.dataDefaults || {}) as any
                        const useDs = dd.useDatasourceDefaults !== false // default true
                        return (
                          <>
                            <label className="flex items-center gap-2 text-xs">
                              <Switch
                                checked={!!optsAny.showDataDefaultsBadges}
                                onChangeAction={(checked) => {
                                  const opts = { ...(local.options || {}), showDataDefaultsBadges: checked || undefined } as any
                                  const next = { ...local, options: opts }
                                  setLocal(next); updateConfig(next)
                                }}
                              />
                              <span>Show Sort/Top N badges in header (includes datasource defaults)</span>
                            </label>
                            <div className="grid grid-cols-2 gap-2 items-center">
                              <label className="text-xs text-muted-foreground">Use datasource defaults</label>
                              <label className="flex items-center gap-2 text-xs">
                                <Switch
                                  checked={useDs}
                                  onChangeAction={(checked) => {
                                    const nextDd = { ...(dd || {}), useDatasourceDefaults: !!checked } as any
                                    const opts = { ...(local.options || {}), dataDefaults: nextDd }
                                    const next = { ...local, options: opts }
                                    setLocal(next); updateConfig(next)
                                  }}
                                />
                                <span>{useDs ? 'On' : 'Off (override below)'}</span>
                              </label>
                            </div>
                            {!useDs && (
                              <div className="space-y-2">
                                <label className="flex items-center gap-2 text-xs">
                                  <Switch
                                    checked={!!dd.showHeaderBadges}
                                    onChangeAction={(checked) => {
                                      const nextDd = { ...(dd || {}), showHeaderBadges: checked || undefined }
                                      const opts = { ...(local.options || {}), dataDefaults: nextDd }
                                      const next = { ...local, options: opts }
                                      setLocal(next); updateConfig(next)
                                    }}
                                  />
                                  <span>Show override badges</span>
                                </label>
                                <div className="grid grid-cols-2 gap-2 items-center">
                                  <label className="text-xs text-muted-foreground">Sort by</label>
                                  <select
                                    className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
                                    value={String(dd?.sort?.by || '')}
                                    onChange={(e) => {
                                      const prev = dd.sort || {}
                                      const by = e.target.value as any
                                      const nextSort = by ? { ...prev, by } : undefined
                                      const nextDd = { ...dd, ...(nextSort ? { sort: nextSort } : { sort: undefined }) }
                                      const opts = { ...(local.options || {}), dataDefaults: nextDd }
                                      const next = { ...local, options: opts }
                                      setLocal(next); updateConfig(next)
                                    }}
                                  >
                                    <option value="">None</option>
                                    <option value="x">X Axis value</option>
                                    <option value="value">Aggregate value</option>
                                  </select>
                                  <label className="text-xs text-muted-foreground">Direction</label>
                                  <select
                                    className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
                                    value={String(dd?.sort?.direction || 'desc')}
                                    onChange={(e) => {
                                      const prev = dd.sort || {}
                                      const nextSort = { ...prev, direction: e.target.value as any }
                                      const nextDd = { ...dd, sort: nextSort }
                                      const opts = { ...(local.options || {}), dataDefaults: nextDd }
                                      const next = { ...local, options: opts }
                                      setLocal(next); updateConfig(next)
                                    }}
                                  >
                                    <option value="desc">Desc</option>
                                    <option value="asc">Asc</option>
                                  </select>
                                  <label className="text-xs text-muted-foreground">Top N (by value)</label>
                                  <input
                                    type="number" min={0} max={50}
                                    className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                    value={Number(dd?.topN?.n ?? 0)}
                                    onChange={(e) => {
                                      const n = Math.max(0, Math.min(50, Number(e.target.value||0)))
                                      const nextTop = n > 0 ? { n, by: 'value', direction: String(dd?.topN?.direction || 'desc') as any } : undefined
                                      const nextDd = { ...dd, ...(nextTop ? { topN: nextTop } : { topN: undefined }) }
                                      const opts = { ...(local.options || {}), dataDefaults: nextDd }
                                      const next = { ...local, options: opts }
                                      setLocal(next); updateConfig(next)
                                    }}
                                  />
                                  <label className="text-xs text-muted-foreground">Top N direction</label>
                                  <select
                                    className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs"
                                    value={String(dd?.topN?.direction || 'desc')}
                                    onChange={(e) => {
                                      const n = Number(dd?.topN?.n || 0)
                                      const nextTop = n > 0 ? { n, by: 'value' as const, direction: e.target.value as any } : undefined
                                      const nextDd = { ...dd, ...(nextTop ? { topN: nextTop } : { topN: undefined }) }
                                      const opts = { ...(local.options || {}), dataDefaults: nextDd }
                                      const next = { ...local, options: opts }
                                      setLocal(next); updateConfig(next)
                                    }}
                                  >
                                    <option value="desc">Largest → Smallest</option>
                                    <option value="asc">Smallest → Largest</option>
                                  </select>
                                </div>
                                <div className="text-[11px] text-muted-foreground">Top N is applied after aggregation by category and always uses the aggregated value. Sort applies after Top N.</div>
                              </div>
                            )}
                          </>
                        )
                      })()}
                    </div>
                    {local.chartType === 'donut' && (
                      <div className="border rounded-md p-2 space-y-3">
                        <div className="text-[11px] font-medium text-muted-foreground">Donut/Pie Presets</div>
                        {(() => {
                          const variantVal = (((local.options as any)?.donutVariant) || 'donut') as 'donut'|'pie'|'sunburst'|'nightingale'
                          const setVariant = (val: 'donut'|'pie'|'sunburst'|'nightingale') => {
                            const opts = { ...(local.options || {}), donutVariant: val } as any
                            const next = { ...local!, options: opts }
                            setLocal(next); updateConfig(next)
                          }
                          const baseOpts = { ...(local.options || {}), showCardHeader: false } as any
                          const mkOpts = (key: 'donut'|'pie'|'sunburst'|'nightingale') => ({ ...baseOpts, donutVariant: key })
                          const items: Array<{ key: 'donut'|'pie'|'sunburst'|'nightingale'; label: string }>= [
                            { key: 'donut', label: 'Donut' },
                            { key: 'pie', label: 'Pie' },
                            { key: 'sunburst', label: 'Sunburst' },
                            { key: 'nightingale', label: 'Nightingale' },
                          ]
                          return (
                            <div className="grid grid-cols-2 gap-2">
                              {items.map((it) => (
                                <label key={it.key} className="block rounded-md border bg-[hsl(var(--secondary)/0.4)] hover:bg-[hsl(var(--secondary)/0.6)]">
                                  <div className="flex items-center gap-2 p-2">
                                    <input
                                      type="radio"
                                      name="donutPreset"
                                      className="accent-[hsl(var(--primary))]"
                                      checked={variantVal === it.key}
                                      onChange={() => setVariant(it.key)}
                                    />
                                    <span className="text-xs">{it.label}</span>
                                  </div>
                                  <div className="h-[140px] w-full border-t overflow-hidden">
                                    <div className="relative h-full w-full">
                                      <ChartCard
                                        title=""
                                        sql={local.sql}
                                        datasourceId={local.datasourceId}
                                        type="donut"
                                        options={mkOpts(it.key)}
                                        queryMode={local.queryMode || 'sql'}
                                        querySpec={local.querySpec as any}
                                        widgetId={`${local.id}-donut-preview-${it.key}`}
                                      />
                                    </div>
                                  </div>
                                </label>
                              ))}
                            </div>
                          )
                        })()}
                      </div>
                    )}
                    {local.chartType === 'gantt' && (
                      <div className="border rounded-md p-2 space-y-3">
                        <div className="text-[11px] font-medium text-muted-foreground">Gantt</div>
                        <div className="grid grid-cols-2 gap-2 text-xs">
                          <label className="flex items-center gap-2">
                            <span className="w-28 text-muted-foreground">Category field</span>
                            <input
                              type="text"
                              className="flex-1 h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]"
                              value={String((local.options as any)?.gantt?.categoryField || '')}
                              onChange={(e) => {
                                const g = { ...((local.options as any)?.gantt || {}), categoryField: e.target.value || undefined }
                                const next = { ...local!, options: { ...(local!.options || {}), gantt: g } }
                                setLocal(next); updateConfig(next)
                              }}
                              placeholder="e.g., Owner"
                            />
                          </label>
                          <label className="flex items-center gap-2">
                            <span className="w-28 text-muted-foreground">Start field</span>
                            <input
                              type="text"
                              className="flex-1 h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]"
                              value={String((local.options as any)?.gantt?.startField || '')}
                              onChange={(e) => {
                                const g = { ...((local.options as any)?.gantt || {}), startField: e.target.value || undefined }
                                const next = { ...local!, options: { ...(local!.options || {}), gantt: g } }
                                setLocal(next); updateConfig(next)
                              }}
                              placeholder="datetime column"
                            />
                          </label>
                          <label className="flex items-center gap-2">
                            <span className="w-28 text-muted-foreground">End field</span>
                            <input
                              type="text"
                              className="flex-1 h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]"
                              value={String((local.options as any)?.gantt?.endField || '')}
                              onChange={(e) => {
                                const g = { ...((local.options as any)?.gantt || {}), endField: e.target.value || undefined }
                                const next = { ...local!, options: { ...(local!.options || {}), gantt: g } }
                                setLocal(next); updateConfig(next)
                              }}
                              placeholder="datetime column"
                            />
                          </label>
                          <label className="flex items-center gap-2">
                            <span className="w-28 text-muted-foreground">Color field</span>
                            <input
                              type="text"
                              className="flex-1 h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]"
                              value={String((local.options as any)?.gantt?.colorField || '')}
                              onChange={(e) => {
                                const g = { ...((local.options as any)?.gantt || {}), colorField: e.target.value || undefined }
                                const next = { ...local!, options: { ...(local!.options || {}), gantt: g } }
                                setLocal(next); updateConfig(next)
                              }}
                              placeholder="optional"
                            />
                          </label>
                          <label className="flex items-center gap-2 col-span-2">
                            <span className="w-28 text-muted-foreground">Bar height</span>
                            <input
                              type="number"
                              min={6}
                              max={24}
                              className="w-24 h-8 px-2 rounded-md border bg-[hsl(var(--secondary)/0.6)]"
                              value={Number((local.options as any)?.gantt?.barHeight ?? 10)}
                              onChange={(e) => {
                                const n = Math.max(6, Math.min(24, Number(e.target.value || 10)))
                                const g = { ...((local.options as any)?.gantt || {}), barHeight: n }
                                const next = { ...local!, options: { ...(local!.options || {}), gantt: g } }
                                setLocal(next); updateConfig(next)
                              }}
                            />
                          </label>
                        </div>
                        <div className="text-[11px] text-muted-foreground">Map the fields for your Gantt. Category is shown on Y, Start/End define each bar. Color field is optional.</div>
                      </div>
                    )}
                    {local.chartType === 'heatmap' && (
                      <div className="border rounded-md p-2 space-y-3">
                        <div className="text-[11px] font-medium text-muted-foreground">Calendar HeatMap</div>
                        {(() => {
                          const presetVal = (((local.options as any)?.heatmap?.preset) || 'calendarMonthly') as 'calendarMonthly'|'weekdayHour'|'calendarAnnual'
                          const setPreset = (val: 'calendarMonthly'|'weekdayHour'|'calendarAnnual') => {
                            const opts = { ...(local.options || {}), heatmap: { ...(local.options as any)?.heatmap, preset: val } } as any
                            const next = { ...local!, options: opts }
                            setLocal(next); updateConfig(next)
                          }
                          const baseOpts = { ...(local.options || {}), showCardHeader: false } as any
                          const mkOpts = (key: 'calendarMonthly'|'weekdayHour'|'calendarAnnual') => ({
                            ...baseOpts,
                            showLegend: false, // hide legend in compact preview tiles
                            heatmap: { ...(baseOpts.heatmap || {}), preset: key, preview: true },
                          })
                          const items: Array<{ key: 'calendarMonthly'|'weekdayHour'|'calendarAnnual'; label: string }>= [
                            { key: 'calendarMonthly', label: 'Calendar Monthly (per Day)' },
                            { key: 'weekdayHour', label: 'Weekday × Hour' },
                            { key: 'calendarAnnual', label: 'Calendar Annual' },
                          ]
                          return (
                            <div className="grid grid-cols-3 gap-2">
                              {items.map((it) => (
                                <label key={it.key} className="block rounded-md border bg-[hsl(var(--secondary)/0.4)] hover:bg-[hsl(var(--secondary)/0.6)]">
                                  <div className="flex items-center gap-2 p-2">
                                    <input
                                      type="radio"
                                      name="heatmapPreset"
                                      className="accent-[hsl(var(--primary))]"
                                      checked={presetVal === it.key}
                                      onChange={() => setPreset(it.key)}
                                    />
                                    <span className="text-xs">{it.label}</span>
                                  </div>
                                  <div className="h-[140px] w-full border-t overflow-hidden">
                                    <div className="relative h-full w-full">
                                      <HeatmapCard
                                        title=""
                                        sql={local.sql}
                                        datasourceId={local.datasourceId}
                                        options={mkOpts(it.key)}
                                        queryMode={local.queryMode || 'sql'}
                                        querySpec={local.querySpec as any}
                                        widgetId={`${local.id}-preview-${it.key}`}
                                      />
                                    </div>
                                  </div>
                                </label>
                              ))}
                            </div>
                          )
                        })()}
                        <div className="text-[11px] text-muted-foreground">Heatmap uses your Data Series (pivot builder) x/y/agg and series definitions. No extra field mapping is required.</div>
                      </div>
                    )}
                    {local.chartType === 'tremorTable' && (
                      <div className="border rounded-md p-2 space-y-3">
                        <div className="text-[11px] font-medium text-muted-foreground">Tremor Table</div>
                        <label className="flex items-center gap-2 text-xs">
                          <Switch
                            checked={local.options?.tremorTable?.alternatingRows !== false}
                            onChangeAction={(checked) => {
                              const tt = { ...(local.options?.tremorTable || {}), alternatingRows: checked }
                              const opts = { ...(local.options || {}), tremorTable: tt }
                              const next = { ...local, options: opts }
                              setLocal(next)
                              updateConfig(next)
                            }}
                          />
                          Alternating row background
                        </label>
                        <label className="flex items-center gap-2 text-xs">
                          <Switch
                            checked={!!local.options?.tremorTable?.showTotalRow}
                            onChangeAction={(checked) => {
                              const tt = { ...(local.options?.tremorTable || {}), showTotalRow: checked }
                              const opts = { ...(local.options || {}), tremorTable: tt }
                              const next = { ...local, options: opts }
                              setLocal(next)
                              updateConfig(next)
                            }}
                          />
                          Show total row
                        </label>
                        <div className="grid grid-cols-2 gap-2">
                          <div>
                            <label className="block text-xs text-muted-foreground mb-1">Badge columns</label>
                            <select
                              multiple
                              className="w-full h-24 px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                              value={(local.options?.tremorTable?.badgeColumns || []) as string[]}
                              onChange={(e) => {
                                const selected = Array.from(e.currentTarget.selectedOptions).map((o) => o.value)
                                const tt = { ...(local.options?.tremorTable || {}), badgeColumns: selected }
                                const opts = { ...(local.options || {}), tremorTable: tt }
                                const next = { ...local, options: opts }
                                setLocal(next)
                                updateConfig(next)
                              }}
                            >
                              {columnNames.map((c) => (
                                <option key={c} value={c}>{c}</option>
                              ))}
                            </select>
                          </div>
                          <div>
                            <label className="block text-xs text-muted-foreground mb-1">Progress columns</label>
                            <select
                              multiple
                              className="w-full h-24 px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                              value={(local.options?.tremorTable?.progressColumns || []) as string[]}
                              onChange={(e) => {
                                const selected = Array.from(e.currentTarget.selectedOptions).map((o) => o.value)
                                const tt = { ...(local.options?.tremorTable || {}), progressColumns: selected }
                                const opts = { ...(local.options || {}), tremorTable: tt }
                                const next = { ...local, options: opts }
                                setLocal(next)
                                updateConfig(next)
                              }}
                            >
                              {columnNames.map((c) => (
                                <option key={c} value={c}>{c}</option>
                              ))}
                            </select>
                            <div className="text-[11px] text-muted-foreground mt-1">Percent is computed relative to the maximum value in each column.</div>
                          </div>
                        </div>
                        <div className="border-t pt-2 space-y-2">
                          <div className="text-[11px] font-medium text-muted-foreground">Format by column</div>
                          {/* Current mappings */}
                          <div className="space-y-1">
                            {Object.entries((local.options?.tremorTable?.formatByColumn || {}) as Record<string,'none'|'short'|'currency'|'percent'|'bytes'>).length === 0 && (
                              <div className="text-[11px] text-muted-foreground">No overrides</div>
                            )}
                            {Object.entries((local.options?.tremorTable?.formatByColumn || {}) as Record<string,'none'|'short'|'currency'|'percent'|'bytes'>).map(([col, mode]) => (
                              <div key={col} className="flex items-center justify-between text-xs">
                                <span className="truncate mr-2">{col} → {String(mode)}</span>
                                <button
                                  className="text-[11px] px-2 py-0.5 rounded-md border hover:bg-muted"
                                  onClick={() => {
                                    const cur = { ...((local.options?.tremorTable?.formatByColumn || {}) as Record<string,'none'|'short'|'currency'|'percent'|'bytes'>) }
                                    delete cur[col]
                                    const tt = { ...(local.options?.tremorTable || {}), formatByColumn: cur }
                                    const next = { ...local, options: { ...(local.options || {}), tremorTable: tt } }
                                    setLocal(next); updateConfig(next)
                                  }}
                                >Remove</button>
                              </div>
                            ))}
                          </div>
                          {/* Add / update */}
                          <div className="grid grid-cols-[1fr,1fr,auto] gap-2 items-center">
                            <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={ttFmtCol}
                              onChange={(e) => setTtFmtCol(e.target.value)}
                            >
                              <option value="">Select column…</option>
                              {columnNames.map((c) => <option key={c} value={c}>{c}</option>)}
                            </select>
                            <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={ttFmtMode}
                              onChange={(e) => setTtFmtMode(e.target.value as 'none'|'short'|'currency'|'percent'|'bytes')}
                            >
                              {(['none','short','currency','percent','bytes'] as const).map((m) => <option key={m} value={m}>{m}</option>)}
                            </select>
                            <button
                              className="text-[11px] px-2 py-1 rounded-md border bg-[hsl(var(--btn3))] text-black disabled:opacity-50"
                              disabled={!ttFmtCol}
                              onClick={() => {
                                if (!ttFmtCol) return
                                const map = { ...((local.options?.tremorTable?.formatByColumn || {}) as Record<string,'none'|'short'|'currency'|'percent'|'bytes'>), [ttFmtCol]: ttFmtMode }
                                const tt = { ...(local.options?.tremorTable || {}), formatByColumn: map }
                                const next = { ...local, options: { ...(local.options || {}), tremorTable: tt } }
                                setLocal(next); updateConfig(next)
                              }}
                            >Set</button>
                          </div>
                        </div>
                        <div className="border-t pt-2 space-y-2">
                          <div className="text-[11px] font-medium text-muted-foreground">Row action</div>
                          <label className="flex items-center gap-2 text-xs">
                            <Switch
                              checked={((local.options?.tremorTable?.rowClick?.type || '') === 'emit')}
                              onChangeAction={(checked) => {
                                const on = checked
                                const tt = on
                                  ? { ...(local.options?.tremorTable || {}), rowClick: { type: 'emit' as const, eventName: local.options?.tremorTable?.rowClick?.eventName || 'tremor-table-click' } }
                                  : (() => { const { rowClick, ...rest } = (local.options?.tremorTable || {}); return rest })()
                                const next = { ...local, options: { ...(local.options || {}), tremorTable: tt as any } }
                                setLocal(next); updateConfig(next)
                              }}
                            />
                            Emit DOM event on row click
                          </label>
                          {((local.options?.tremorTable?.rowClick?.type || '') === 'emit') && (
                            <div className="grid grid-cols-[1fr] gap-2">
                              <label className="block text-xs text-muted-foreground mb-1">Event name</label>
                              <input
                                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                value={local.options?.tremorTable?.rowClick?.eventName || 'tremor-table-click'}
                                onChange={(e) => {
                                  const tt = { ...(local.options?.tremorTable || {}), rowClick: { type: 'emit' as const, eventName: e.target.value || 'tremor-table-click' } }
                                  const next = { ...local, options: { ...(local.options || {}), tremorTable: tt } }
                                  setLocal(next); updateConfig(next)
                                }}
                              />
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                    {local.chartType === 'badges' && (
                      <div className="border rounded-md p-2 space-y-2">
                        <div className="text-[11px] font-medium text-muted-foreground">Badges</div>
                        {/* KPI Aggregation Mode for Badges */}
                        <div className="grid grid-cols-2 gap-2 items-center">
                          <label className="text-xs text-muted-foreground">Aggregation mode</label>
                          <select
                            className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                            value={String((local.options?.kpi?.aggregationMode || 'count') as any)}
                            onChange={(e) => {
                              const kpi = { ...((local.options?.kpi || {}) as any), aggregationMode: e.target.value as any }
                              const opts = { ...(local.options || {}), kpi }
                              const next = { ...local, options: opts }
                              setLocal(next); updateConfig(next)
                            }}
                          >
                            {['none','sum','count','distinctCount','avg','min','max','first','last'].map((m) => (
                              <option key={m} value={m}>{m}</option>
                            ))}
                          </select>
                        </div>
                        {(() => {
                          const selected = (local.options?.badgesPreset || 'badge1') as 'badge1'|'badge2'|'badge3'|'badge4'|'badge5'
                          const presets = ['badge1','badge2','badge3','badge4','badge5'] as const
                          const dirOf = (p: number): 'up'|'down'|'flat' => (p > 1 ? 'up' : p < 0 ? 'down' : 'flat')
                          const tone = (p: number) => (p < 0 ? 'text-rose-700 dark:text-rose-500' : p > 1 ? 'text-emerald-700 dark:text-emerald-500' : 'text-[hsl(var(--muted-foreground))]')
                          const bgTint = (p: number) => (p > 1 ? 'bg-emerald-50 dark:bg-emerald-400/10' : p < 0 ? 'bg-rose-50 dark:bg-rose-400/10' : 'bg-[hsl(var(--muted))]')
                          const pctText = (p: number) => `${p>0?'+':(p<0?'-':'')}${Math.abs(p).toFixed(1)}%`
                          const ArrowS = ({ p }: { p: number }) => dirOf(p) === 'up' ? <RiArrowUpSFill className="-ml-0.5 size-4" /> : dirOf(p) === 'down' ? <RiArrowDownSFill className="-ml-0.5 size-4" /> : <RiArrowRightSFill className="-ml-0.5 size-4" />
                          const ArrowL = ({ p }: { p: number }) => dirOf(p) === 'up' ? <RiArrowUpLine className="-ml-0.5 size-4" /> : dirOf(p) === 'down' ? <RiArrowDownLine className="-ml-0.5 size-4" /> : <RiArrowRightLine className="-ml-0.5 size-4" />
                          const renderChip = (preset: typeof presets[number], p: number) => {
                            if (preset === 'badge1') {
                              return (
                                <span className={`inline-flex items-center gap-x-1 rounded-md px-2 py-1 text-[11px] font-normal ring-1 ring-inset ring-[hsl(var(--border))] min-w-[88px] justify-center ${tone(p)}`}>
                                  <ArrowS p={p} />
                                  <span>{pctText(p)}</span>
                                </span>
                              )
                            }
                            if (preset === 'badge2') {
                              return (
                                <span className={`inline-flex items-center gap-x-1 rounded-md px-2 py-1 ring-1 ring-inset ring-[hsl(var(--border))] min-w-[88px] justify-center ${bgTint(p)}`}>
                                  <span className={`${tone(p)}`}><ArrowL p={p} /></span>
                                  <span className={`font-normal ${tone(p)}`}>{pctText(p)}</span>
                                </span>
                              )
                            }
                            if (preset === 'badge3') {
                              return (
                                <span className={`inline-flex items-center gap-x-1 rounded-md px-2 py-1 ring-1 ring-inset ring-[hsl(var(--border))] min-w-[88px] justify-center`}>
                                  <span className={`font-normal ${tone(p)}`}>{pctText(p)}</span>
                                  <span className={`inline-flex items-center justify-center rounded-md px-1.5 py-0.5 ${p>1?'bg-emerald-100':p<0?'bg-rose-100':'bg-[hsl(var(--muted))]'}`}>
                                    <span className={`${tone(p)}`}><ArrowL p={p} /></span>
                                  </span>
                                </span>
                              )
                            }
                            if (preset === 'badge4') {
                              return (
                                <span className={`inline-flex items-center gap-x-1 rounded-md px-2 py-1 ring-1 ring-inset ring-[hsl(var(--border))] min-w-[88px] justify-center`}>
                                  <span className={`inline-flex items-center justify-center rounded-md px-1.5 py-0.5 ${p>1?'bg-emerald-100':p<0?'bg-rose-100':'bg-[hsl(var(--muted))]'}`}>
                                  </span>
                                </span>
                              )
                            }
                            // badge5
                            return (
                              <span className={`inline-flex items-center gap-x-1 rounded-md px-2.5 py-1 ring-1 ring-inset ring-[hsl(var(--border))] min-w-[88px] justify-center ${bgTint(p)}`}>
                                <span className={`${tone(p)}`}><ArrowL p={p} /></span>
                                <span className={`font-normal ${tone(p)}`}>{pctText(p)}</span>
                              </span>
                            )
                          }
                          return (
                            <div className="grid grid-cols-3 gap-2 h-[240px] overflow-y-auto overflow-x-hidden p-1" style={{ scrollbarGutter: 'stable' }}>
                              {presets.map((p) => (
                                <label key={p} className={`w-full flex flex-col items-center gap-2 p-2 rounded-md border cursor-pointer text-xs ${selected===p ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))] ring-offset-2 ring-offset-[hsl(var(--card))]' : 'bg-[hsl(var(--secondary))] hover:bg-[hsl(var(--muted))]'}`}>
                                  <input type="radio" name="badges-preset" className="sr-only" checked={selected===p}
                                    onChange={() => { const opts = { ...(local.options || {}), badgesPreset: p }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                                  <div className="flex flex-col items-center gap-1">
                                    {renderChip(p, +20)}
                                    {renderChip(p, -20)}
                                    {renderChip(p, 0)}
                                  </div>
                                  <div className="opacity-70">{p}</div>
                                </label>
                              ))}
                            </div>
                          )
                        })()}
                        {/* Fine-tune toggles */}
                        <div className="grid grid-cols-2 gap-2 pt-2">
                          <label className="flex items-center gap-2 text-xs">
                            <Switch checked={!!local.options?.badgesShowCategoryLabel}
                              onChangeAction={(checked) => {
                                const outside = checked
                                const inside = outside ? false : true
                                const opts = { ...(local.options || {}), badgesShowCategoryLabel: outside, badgesLabelInside: inside }
                                const next = { ...local, options: opts }; setLocal(next); updateConfig(next)
                              }} />
                            Show Category Label (outside)
                          </label>
                          <label className="flex items-center gap-2 text-xs">
                            <Switch checked={!!local.options?.badgesLabelInside}
                              onChangeAction={(checked) => {
                                const inside = checked
                                const outside = inside ? false : true
                                const opts = { ...(local.options || {}), badgesLabelInside: inside, badgesShowCategoryLabel: outside }
                                const next = { ...local, options: opts }; setLocal(next); updateConfig(next)
                              }} />
                            Show Category Label (inside pill)
                          </label>
                          <label className="flex items-center gap-2 text-xs">
                            <Switch checked={!!local.options?.badgesShowValue}
                              onChangeAction={(checked) => { const opts = { ...(local.options || {}), badgesShowValue: checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                            Show aggregated value
                          </label>
                          <label className="flex items-center gap-2 text-xs">
                            <Switch checked={!!local.options?.badgesShowDelta}
                              onChangeAction={(checked) => { const opts = { ...(local.options || {}), badgesShowDelta: checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                            Show delta (based on timeperiod)
                          </label>
                          <label className="flex items-center gap-2 text-xs">
                            <Switch checked={!!local.options?.badgesShowDeltaPct}
                              onChangeAction={(checked) => { const opts = { ...(local.options || {}), badgesShowDeltaPct: checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                            Show delta change percentage
                          </label>
                          <label className="flex items-center gap-2 text-xs">
                            <Switch checked={!!local.options?.badgesShowPercentOfTotal}
                              onChangeAction={(checked) => { const opts = { ...(local.options || {}), badgesShowPercentOfTotal: checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                            Show percent of total
                          </label>
                        </div>
                      </div>
                    )}
                    {local.chartType !== 'spark' && (
                    <div className="border rounded-md p-2 space-y-2">
                      <div className="text-[11px] font-medium text-muted-foreground">Data Series</div>
                      <label className="flex items-center gap-2 text-xs">
                        <Switch checked={!!local.options?.advancedMode}
                          onChangeAction={(checked) => { const opts = { ...(local.options || {}), advancedMode: checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                        Advanced mode (ECharts)
                      </label>
                      {(local.options?.advancedMode && (local.chartType === 'bar' || local.chartType === 'column')) && (
                        <label className="flex items-center gap-2 text-xs">
                          <Switch checked={!!local.options?.barRounded}
                            onChangeAction={(checked) => { const opts = { ...(local.options || {}), barRounded: checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                          Rounded bars
                        </label>
                      )}
                      {(local.options?.advancedMode && (local.chartType === 'bar' || local.chartType === 'column')) && (
                        <label className="flex items-center gap-2 text-xs">
                          <Switch checked={!!local.options?.barGradient}
                            onChangeAction={(checked) => { const opts = { ...(local.options || {}), barGradient: checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                          Gradient fill
                        </label>
                      )}
                      {(local.options?.advancedMode && (local.chartType === 'line' || local.chartType === 'area')) && (
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Line width (px)</label>
                          <input type="number" min={1} max={8} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                            value={local.options?.lineWidth ?? 2}
                            onChange={(e) => { const val = Math.max(1, Math.min(8, Number(e.target.value || 2))); const opts = { ...(local.options || {}), lineWidth: val }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                        </div>
                      )}
                      {(local.options?.advancedMode && local.chartType === 'area') && (
                        <div className="grid grid-cols-2 gap-2">
                          <label className="flex items-center gap-2 text-xs">
                            <input
                              type="checkbox"
                              className="accent-[hsl(var(--primary))]"
                              checked={!!(local.options as any)?.areaStacked}
                              onChange={(e) => { const opts = { ...(local.options || {}), areaStacked: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}
                            />
                            Stacked
                          </label>
                        </div>
                      )}
                      {(local.options?.advancedMode && (['line','area','bar','column','combo','scatter'] as const).includes(local.chartType as any)) && (
                        <div className="grid grid-cols-2 gap-2">
                          <label className="flex items-center gap-2 text-xs">
                            <input
                              type="checkbox"
                              className="accent-[hsl(var(--primary))]"
                              checked={!!(local.options as any)?.zoomPan}
                              onChange={(e) => { const opts = { ...(local.options || {}), zoomPan: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}
                            />
                            Zoom & Pan
                          </label>
                          <label className="flex items-center gap-2 text-xs">
                            <input
                              type="checkbox"
                              className="accent-[hsl(var(--primary))]"
                              checked={!!(local.options as any)?.largeScale}
                              onChange={(e) => { const opts = { ...(local.options || {}), largeScale: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}
                            />
                            Large scale (sampling)
                          </label>
                        </div>
                      )}
                      {(local.options?.advancedMode && (local.chartType === 'bar' || local.chartType === 'column')) && (
                        <div className="grid grid-cols-2 gap-2">
                          <div>
                            <label className="block text-xs text-muted-foreground mb-1">Bar mode</label>
                            <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.barMode || 'default'}
                              onChange={(e) => { const opts = { ...(local.options || {}), barMode: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                              {['default','grouped','stacked'].map(m => <option key={m} value={m}>{m}</option>)}
                            </select>
                          </div>
                          <div>
                            <label className="block text-xs text-muted-foreground mb-1">Bar gap (%)</label>
                            <input type="number" min={0} max={100} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.barGap ?? 30}
                              onChange={(e) => { const val = Math.max(0, Math.min(100, Number(e.target.value || 30))); const opts = { ...(local.options || {}), barGap: val }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                          </div>
                        </div>
                      )}
                      {(local.options?.advancedMode) && (
                        <div className="grid grid-cols-2 gap-2">
                          <label className="flex items-center gap-2 text-xs">
                            <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!local.options?.dataLabelsShow}
                              onChange={(e) => { const opts = { ...(local.options || {}), dataLabelsShow: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                            Show data labels
                          </label>
                          <div>
                            <label className="block text-xs text-muted-foreground mb-1">Data labels position</label>
                            <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.dataLabelPosition || 'outsideEnd'}
                              onChange={(e) => { const opts = { ...(local.options || {}), dataLabelPosition: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                              {['center','insideEnd','insideBase','outsideEnd','callout'].map(p => <option key={p} value={p}>{p}</option>)}
                            </select>
                          </div>
                        </div>
                      )}
                    </div>
                    )}
                    {local.chartType === 'spark' && (
                      <div className="border rounded-md p-2 space-y-3">
                        <div className="text-[11px] font-medium text-muted-foreground">Spark display</div>
                        <label className="flex items-center gap-2 text-xs">
                          <input
                            type="checkbox"
                            className="accent-[hsl(var(--primary))]"
                            checked={!!local.options?.sparkDownIsGood}
                            onChange={(e) => {
                              const opts = { ...(local.options || {}), sparkDownIsGood: e.target.checked }
                              const next = { ...local, options: opts }
                              setLocal(next)
                              updateConfig(next)
                            }}
                          />
                          Down is good (invert colors)
                        </label>
                        <div className="flex flex-col gap-1 text-xs">
                          <span className="text-muted-foreground">Category label lines</span>
                          <select
                            className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))]"
                            value={String(local.options?.sparkLabelMaxLines || 2)}
                            onChange={(e) => {
                              const maxLines = Math.max(1, Math.min(3, Number(e.target.value) || 2))
                              const opts = { ...(local.options || {}), sparkLabelMaxLines: maxLines }
                              const next = { ...local, options: opts }
                              setLocal(next)
                              updateConfig(next)
                            }}
                          >
                            <option value="1">Single line</option>
                            <option value="2">Wrap up to 2 lines</option>
                            <option value="3">Wrap up to 3 lines</option>
                          </select>
                        </div>
                      </div>
                    )}
                    {!(local.chartType === 'spark' || local.chartType === 'badges' || local.chartType === 'tremorTable') && (
                    <div className="border rounded-md p-2 space-y-2">
                      <div className="text-[11px] font-medium text-muted-foreground">Legend</div>
                      <label className="flex items-center gap-2 text-xs">
                        <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!local.options?.showLegend}
                          onChange={(e) => { const opts = { ...(local.options || {}), showLegend: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                        Show legend
                      </label>
                      <div className="grid grid-cols-3 gap-2">
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Position</label>
                          <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.legendPosition || 'bottom'}
                            onChange={(e) => { const opts = { ...(local.options || {}), legendPosition: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                            {['bottom','top','none'].map(p => <option key={p} value={p}>{p}</option>)}
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Dot shape</label>
                          <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.legendDotShape || 'square'}
                            onChange={(e) => { const opts = { ...(local.options || {}), legendDotShape: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                            {['square','circle','rect'].map(p => <option key={p} value={p}>{p}</option>)}
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Max items</label>
                          <input type="number" min={0} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.maxLegendItems ?? 0}
                            onChange={(e) => { const n = Math.max(0, Number(e.target.value||0)); const opts = { ...(local.options || {}), maxLegendItems: n }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                        </div>
                      </div>
                      <div className="grid grid-cols-3 gap-2">
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Mode</label>
                          <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={(local.options as any)?.legendMode || 'flat'}
                            onChange={(e) => { const opts = { ...(local.options || {}), legendMode: (e.target.value as any) }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                            {(['flat','nested'] as const).map(p => <option key={p} value={p}>{p}</option>)}
                          </select>
                        </div>
                      </div>
                    </div>
                    )}
                    <div className="border rounded-md p-2 space-y-2">
                      <div className="text-[11px] font-medium text-muted-foreground">Colors</div>
                      <div>
                        <label className="block text-xs text-muted-foreground mb-1">Color preset</label>
                        <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.colorPreset || 'default'}
                          onChange={(e) => { const opts = { ...(local.options || {}), colorPreset: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                          {['default','muted','vibrant','corporate'].map(p => <option key={p} value={p}>{p}</option>)}
                        </select>
                      </div>
                      <div className="grid grid-cols-2 gap-2">
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Color mode</label>
                          <select
                            className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                            value={(local.options as any)?.colorMode || 'palette'}
                            onChange={(e) => {
                              const mode = e.target.value as 'palette'|'valueGradient'
                              const autoAdv = (local.chartType === 'bar' || local.chartType === 'column' || local.chartType === 'line' || local.chartType === 'area' || local.chartType === 'combo')
                              const opts = { ...(local.options || {}), colorMode: mode, ...(mode==='valueGradient' && autoAdv ? { advancedMode: true } : {}) }
                              const next = { ...local, options: opts }
                              setLocal(next); updateConfig(next)
                            }}
                          >
                            <option value="palette">Palette (per-series colors)</option>
                            <option value="valueGradient">Value-based Gradient</option>
                          </select>
                        </div>
                        {(((local.options as any)?.colorMode || 'palette') === 'valueGradient') && (
                          <div>
                            <label className="block text-xs text-muted-foreground mb-1">Base color</label>
                            <div className="grid grid-cols-6 gap-1">
                              {(['blue','emerald','violet','amber','gray','rose','indigo','cyan','pink','lime','fuchsia'] as const).map((k) => (
                                <button
                                  key={k}
                                  type="button"
                                  className={`h-6 rounded border ${((local.options as any)?.colorBaseKey || 'blue')===k ? 'ring-2 ring-[hsl(var(--primary))] ring-offset-2 ring-offset-[hsl(var(--card))]' : ''}`}
                                  style={{ backgroundColor: (() => { const m: Record<string,string> = { blue:'#3b82f6', emerald:'#10b981', violet:'#8b5cf6', amber:'#f59e0b', gray:'#6b7280', rose:'#f43f5e', indigo:'#6366f1', cyan:'#06b6d4', pink:'#ec4899', lime:'#84cc16', fuchsia:'#d946ef' }; return m[k] })() }}
                                  onClick={() => { const opts = { ...(local.options || {}), colorBaseKey: k as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}
                                  title={k}
                                />
                              ))}
                            </div>
                            <div className="text-[10px] text-muted-foreground mt-1">Saturation scales with value share. Works in Advanced charts and Tracker.</div>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                </TabPanel>
                <TabPanel>
                  <div className="space-y-2 max-h-[260px] overflow-y-auto pr-1">
                    <label className="flex items-center gap-2 text-xs">
                      <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!local.options?.richTooltip}
                        onChange={(e) => { const opts = { ...(local.options || {}), richTooltip: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                      Rich tooltip
                    </label>
                    <label className="flex items-center gap-2 text-xs">
                      <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!local.options?.tooltipShowPercent}
                        onChange={(e) => { const opts = { ...(local.options || {}), tooltipShowPercent: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                      Show percentage of total
                    </label>
                    <label className="flex items-center gap-2 text-xs">
                      <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!local.options?.tooltipShowDelta}
                        onChange={(e) => { const opts = { ...(local.options || {}), tooltipShowDelta: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                      Show deltas
                    </label>
                    {(!!local.options?.richTooltip && !!local.options?.tooltipShowDelta) && (
                      <div className="ml-4 grid gap-2">
                        <label className="flex items-center gap-2 text-xs">
                          <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!(local.options as any)?.tooltipShowPeriodGrowth}
                            onChange={(e) => { const opts = { ...(local.options || {}), tooltipShowPeriodGrowth: e.target.checked }; const next = { ...local, options: opts as any }; setLocal(next); updateConfig(next) }} />
                          Show growth/drop value (period)
                        </label>
                        <label className="flex items-center gap-2 text-xs">
                          <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!(local.options as any)?.tooltipShowPeriodChangePct}
                            onChange={(e) => { const opts = { ...(local.options || {}), tooltipShowPeriodChangePct: e.target.checked }; const next = { ...local, options: opts as any }; setLocal(next); updateConfig(next) }} />
                          Show change percentage (period)
                        </label>
                        <label className="flex items-center gap-2 text-xs">
                          <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!(local.options as any)?.tooltipShowPeriodAggs}
                            onChange={(e) => { const opts = { ...(local.options || {}), tooltipShowPeriodAggs: e.target.checked }; const next = { ...local, options: opts as any }; setLocal(next); updateConfig(next) }} />
                          Show current & previous period values
                        </label>
                      </div>
                    )}
                    <label className="flex items-center gap-2 text-xs">
                      <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!local.options?.tooltipHideZeros}
                        onChange={(e) => { const opts = { ...(local.options || {}), tooltipHideZeros: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                      Hide zero values
                    </label>
                    <label className="flex items-center gap-2 text-xs">
                      <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!(local.options as any)?.downIsGood}
                        onChange={(e) => { const opts = { ...(local.options || {}), downIsGood: e.target.checked }; const next = { ...local, options: opts as any }; setLocal(next); updateConfig(next) }} />
                      Down is good (invert change coloring)
                    </label>
                    <div className="border rounded-md p-2 space-y-2">
                      <div className="text-[11px] font-medium text-muted-foreground">Deltas</div>
                      <div className="grid grid-cols-2 gap-2">
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">UI Mode</label>
                          <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.deltaUI || 'none'}
                            onChange={(e) => { const opts = { ...(local.options || {}), deltaUI: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                            {(['none','filterbar','preconfigured'] as const).map((m) => <option key={m} value={m}>{m}</option>)}
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Week start</label>
                          <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.deltaWeekStart || 'mon'}
                            onChange={(e) => { const opts = { ...(local.options || {}), deltaWeekStart: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                            {(['sat','sun','mon'] as const).map((w) => <option key={w} value={w}>{w.toUpperCase()}</option>)}
                          </select>
                        </div>
                      </div>
                      {local.options?.deltaUI === 'preconfigured' && (
                        <div className="grid grid-cols-2 gap-2">
                          <div>
                            <label className="block text-xs text-muted-foreground mb-1">Delta mode</label>
                            <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.deltaMode || 'off'}
                              onChange={(e) => { const opts = { ...(local.options || {}), deltaMode: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                              {(['off','TD_YSTD','TW_LW','MONTH_LMONTH','MTD_LMTD','TY_LY','YTD_LYTD','TQ_LQ'] as const).map((m) => <option key={m} value={m}>{m}</option>)}
                            </select>
                          </div>
                          <div>
                            <label className="block text-xs text-muted-foreground mb-1">Date field</label>
                            <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.deltaDateField || ''}
                              onChange={(e) => { const opts = { ...(local.options || {}), deltaDateField: e.target.value || undefined }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                              <option value="">Select date field…</option>
                              {schemaColumnNames.map((c) => <option key={c} value={c}>{c}</option>)}
                            </select>
                          </div>
                        </div>
                      )}
                      {local.options?.deltaUI === 'preconfigured' && (local.options?.deltaMode && local.options.deltaMode !== 'off') && local.options?.deltaDateField && (
                        <div className="mt-2 rounded-md border bg-[hsl(var(--secondary))] p-2">
                          <div className="text-[11px] font-medium text-muted-foreground mb-1">Resolved Period Preview</div>
                          <div className="text-[11px] text-muted-foreground">
                            <div>Mode: <span className="font-mono">{String(local.options?.deltaMode)}</span></div>
                            <div>Date field: <span className="font-mono">{String(local.options?.deltaDateField)}</span></div>
                            <div className="mt-1">
                              {deltaPreviewLoading ? (
                                <span>Resolving…</span>
                              ) : deltaPreviewError ? (
                                <span className="text-red-600">{deltaPreviewError}</span>
                              ) : deltaResolved ? (
                                <div className="space-y-0.5">
                                  <div>Current: <span className="font-mono">{deltaResolved.curStart}</span> → <span className="font-mono">{deltaResolved.curEnd}</span></div>
                                  <div>Previous: <span className="font-mono">{deltaResolved.prevStart}</span> → <span className="font-mono">{deltaResolved.prevEnd}</span></div>
                                </div>
                              ) : (
                                <span>—</span>
                              )}
                            </div>
                            <div className="mt-1">First row in <span className="font-mono">{String(local.options?.deltaDateField)}</span>: <span className="font-mono">{deltaSampleNow ?? '—'}</span></div>
                            <div className="mt-1 flex items-center gap-2">
                              <button className="text-[11px] px-2 py-0.5 rounded-md border hover:bg-muted" onClick={() => void refreshDeltaPreview()}>Refresh</button>
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </TabPanel>
                {/* Per-axis font controls */}
                <TabPanel>
                  <div className="grid grid-cols-2 gap-2 max-h-[280px] overflow-y-auto pr-1">
                    <label className="flex items-center gap-2 text-xs">
                      <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={local.options?.autoCondenseXLabels !== false}
                        onChange={(e) => { const opts = { ...(local.options || {}), autoCondenseXLabels: e.target.checked }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                      Auto-condense X labels
                    </label>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Dense threshold</label>
                      <input type="number" min={4} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.xDenseThreshold ?? 12}
                        onChange={(e) => { const val = Math.max(4, Number(e.target.value || 12)); const opts = { ...(local.options || {}), xDenseThreshold: val }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                    </div>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">X tick angle</label>
                      <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.xTickAngle || 0}
                        onChange={(e) => { const opts = { ...(local.options || {}), xTickAngle: Number(e.target.value) as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                        {[0,30,45,60,90].map((a) => <option key={a} value={a}>{a}°</option>)}
                      </select>
                    </div>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">X tick count</label>
                      <input type="number" min={0} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.xTickCount ?? 0}
                        onChange={(e) => { const val = Math.max(0, Number(e.target.value||0)); const opts = { ...(local.options || {}), xTickCount: val }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                    </div>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Y tick count</label>
                      <input type="number" min={0} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.yTickCount ?? 0}
                        onChange={(e) => { const val = Math.max(0, Number(e.target.value||0)); const opts = { ...(local.options || {}), yTickCount: val }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                    </div>
                    
                    {/* Axis limits & formatting */}
                    {(() => {
                      const hasSecondary =
                        (['line','area','column','scatter'] as const).includes(local.chartType as any) &&
                        (
                          Array.isArray((local as any)?.pivot?.values)
                            ? ((local as any).pivot.values as any[]).some(v => !!v?.secondaryAxis)
                            : Array.isArray((local as any)?.querySpec?.series)
                              ? ((local as any).querySpec.series as any[]).some((s: any) => !!s?.secondaryAxis)
                              : false
                        )
                      const fmtOptions = [
                        'none','short','abbrev','currency','percent','bytes','wholeNumber','number',
                        'thousands','millions','billions','oneDecimal','twoDecimals',
                        'percentWhole','percentOneDecimal','timeHours','timeMinutes','distance-km','distance-mi'
                      ] as const

                      return (
                        <div className="col-span-2 border rounded-md p-2">
                          <div className="text-[11px] font-medium text-muted-foreground mb-2">Axis limits & formatting</div>

                          {hasSecondary ? (
                            <div className="flex gap-3">
                              {/* vertical tabs */}
                              <div className="flex flex-col w-28">
                                <button
                                  type="button"
                                  className={`text-left text-xs px-2 py-1 rounded-md border ${axisSubtab==='main' ? 'bg-[hsl(var(--secondary))]' : ''}`}
                                  onClick={() => setAxisSubtab('main')}
                                >Main</button>
                                <button
                                  type="button"
                                  className={`mt-1 text-left text-xs px-2 py-1 rounded-md border ${axisSubtab==='secondary' ? 'bg-[hsl(var(--secondary))]' : ''}`}
                                  onClick={() => setAxisSubtab('secondary')}
                                >Secondary</button>
                              </div>

                              {/* panel content */}
                              <div className="flex-1 grid grid-cols-3 gap-2">
                                {axisSubtab === 'main' ? (
                                  <>
                                    <div>
                                      <label className="block text-xs text-muted-foreground mb-1">Format</label>
                                      <select
                                        className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                        value={(local.options as any)?.yAxisFormat || 'none'}
                                        onChange={(e) => {
                                          const opts = { ...(local.options || {}), yAxisFormat: e.target.value as any }
                                          const next = { ...local!, options: opts } as any
                                          setLocal(next); updateConfig(next)
                                        }}
                                      >
                                        {fmtOptions.map(f => <option key={f} value={f}>{f}</option>)}
                                      </select>
                                    </div>
                                    <div>
                                      <label className="block text-xs text-muted-foreground mb-1">Y min</label>
                                      <input
                                        type="number"
                                        className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                        value={(local.options as any)?.yMin ?? ''}
                                        onChange={(e) => {
                                          const val = e.target.value === '' ? undefined : Number(e.target.value)
                                          const opts = { ...(local.options || {}), yMin: val as any }
                                          const next = { ...local!, options: opts } as any
                                          setLocal(next); updateConfig(next)
                                        }}
                                      />
                                    </div>
                                    <div>
                                      <label className="block text-xs text-muted-foreground mb-1">Y max</label>
                                      <input
                                        type="number"
                                        className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                        value={(local.options as any)?.yMax ?? ''}
                                        onChange={(e) => {
                                          const val = e.target.value === '' ? undefined : Number(e.target.value)
                                          const opts = { ...(local.options || {}), yMax: val as any }
                                          const next = { ...local!, options: opts } as any
                                          setLocal(next); updateConfig(next)
                                        }}
                                      />
                                    </div>
                                  </>
                                ) : (
                                  <>
                                    <div>
                                      <label className="block text-xs text-muted-foreground mb-1">Format (y2)</label>
                                      <select
                                        className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                        value={(local.options as any)?.y2AxisFormat || ((local.options as any)?.yAxisFormat || 'none')}
                                        onChange={(e) => {
                                          const opts = { ...(local.options || {}), y2AxisFormat: e.target.value as any }
                                          const next = { ...local!, options: opts } as any
                                          setLocal(next); updateConfig(next)
                                        }}
                                      >
                                        {fmtOptions.map(f => <option key={f} value={f}>{f}</option>)}
                                      </select>
                                    </div>
                                    <div>
                                      <label className="block text-xs text-muted-foreground mb-1">Y2 min</label>
                                      <input
                                        type="number"
                                        className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                        value={(local.options as any)?.y2Min ?? ''}
                                        onChange={(e) => {
                                          const val = e.target.value === '' ? undefined : Number(e.target.value)
                                          const opts = { ...(local.options || {}), y2Min: val as any }
                                          const next = { ...local!, options: opts } as any
                                          setLocal(next); updateConfig(next)
                                        }}
                                      />
                                    </div>
                                    <div>
                                      <label className="block text-xs text-muted-foreground mb-1">Y2 max</label>
                                      <input
                                        type="number"
                                        className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                        value={(local.options as any)?.y2Max ?? ''}
                                        onChange={(e) => {
                                          const val = e.target.value === '' ? undefined : Number(e.target.value)
                                          const opts = { ...(local.options || {}), y2Max: val as any }
                                          const next = { ...local!, options: opts } as any
                                          setLocal(next); updateConfig(next)
                                        }}
                                      />
                                    </div>
                                  </>
                                )}
                              </div>
                            </div>
                          ) : (
                            <div className="grid grid-cols-3 gap-2">
                              <div>
                                <label className="block text-xs text-muted-foreground mb-1">Format</label>
                                <select
                                  className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                  value={(local.options as any)?.yAxisFormat || 'none'}
                                  onChange={(e) => {
                                    const opts = { ...(local.options || {}), yAxisFormat: e.target.value as any }
                                    const next = { ...local!, options: opts } as any
                                    setLocal(next); updateConfig(next)
                                  }}
                                >
                                  {fmtOptions.map(f => <option key={f} value={f}>{f}</option>)}
                                </select>
                              </div>
                              <div>
                                <label className="block text-xs text-muted-foreground mb-1">Y min</label>
                                <input
                                  type="number"
                                  className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                  value={(local.options as any)?.yMin ?? ''}
                                  onChange={(e) => {
                                    const val = e.target.value === '' ? undefined : Number(e.target.value)
                                    const opts = { ...(local.options || {}), yMin: val as any }
                                    const next = { ...local!, options: opts } as any
                                    setLocal(next); updateConfig(next)
                                  }}
                                />
                              </div>
                              <div>
                                <label className="block text-xs text-muted-foreground mb-1">Y max</label>
                                <input
                                  type="number"
                                  className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                  value={(local.options as any)?.yMax ?? ''}
                                  onChange={(e) => {
                                    const val = e.target.value === '' ? undefined : Number(e.target.value)
                                    const opts = { ...(local.options || {}), yMax: val as any }
                                    const next = { ...local!, options: opts } as any
                                    setLocal(next); updateConfig(next)
                                  }}
                                />
                              </div>
                            </div>
                          )}
                        </div>
                      )
                    })()}

                    {/* Per-axis font controls */}
                    <div className="col-span-2 border rounded-md p-2">
                      <div className="text-[11px] font-medium text-muted-foreground mb-1">X Axis Font</div>
                      <div className="grid grid-cols-3 gap-2">
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Weight</label>
                          <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={(local.options as any)?.xAxisFontWeight || 'normal'}
                            onChange={(e) => { const opts = { ...(local.options || {}), xAxisFontWeight: (e.target.value as any) }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                            {(['normal','bold'] as const).map(w => <option key={w} value={w}>{w}</option>)}
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Size</label>
                          <input type="number" min={8} max={18} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={(local.options as any)?.xAxisFontSize ?? 11}
                            onChange={(e) => { const v = Math.max(8, Math.min(18, Number(e.target.value||11))); const opts = { ...(local.options || {}), xAxisFontSize: v }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                        </div>
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Color</label>
                          <input type="color" className="w-full h-[30px] rounded-md bg-[hsl(var(--secondary))]" value={(local.options as any)?.xAxisFontColor || ''}
                            onChange={(e) => { const val = e.target.value || undefined; const opts = { ...(local.options || {}), xAxisFontColor: val }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                        </div>
                      </div>
                      <div className="text-[11px] font-medium text-muted-foreground mt-2 mb-1">Y Axis Font</div>
                      <div className="grid grid-cols-3 gap-2">
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Weight</label>
                          <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={(local.options as any)?.yAxisFontWeight || 'normal'}
                            onChange={(e) => { const opts = { ...(local.options || {}), yAxisFontWeight: (e.target.value as any) }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                            {(['normal','bold'] as const).map(w => <option key={w} value={w}>{w}</option>)}
                          </select>
                        </div>
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Size</label>
                          <input type="number" min={8} max={18} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={(local.options as any)?.yAxisFontSize ?? 11}
                            onChange={(e) => { const v = Math.max(8, Math.min(18, Number(e.target.value||11))); const opts = { ...(local.options || {}), yAxisFontSize: v }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                        </div>
                        <div>
                          <label className="block text-xs text-muted-foreground mb-1">Color</label>
                          <input type="color" className="w-full h-[30px] rounded-md bg-[hsl(var(--secondary))]" value={(local.options as any)?.yAxisFontColor || ''}
                            onChange={(e) => { const val = e.target.value || undefined; const opts = { ...(local.options || {}), yAxisFontColor: val }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
                        </div>
                      </div>
                    </div>
                    {/* Chart Title customization moved into Card Config > Format Title */}
                  </div>
                </TabPanel>
                <TabPanel>
                  <div className="max-h-[300px] overflow-y-auto pr-1">
                    <div className="grid gap-3" style={{ gridTemplateColumns: '80px minmax(0,1fr)' }}>
                      <div className="flex flex-col gap-1">
                        {([
                          { key: 'hMain', label: 'Horizontal Main' },
                          { key: 'hSecondary', label: 'Horizontal Secondary' },
                          { key: 'vMain', label: 'Vertical Main' },
                          { key: 'vSecondary', label: 'Vertical Secondary' },
                        ] as Array<{key: 'hMain'|'hSecondary'|'vMain'|'vSecondary'; label: string}>).map((t) => (
                          <button
                            key={t.key}
                            type="button"
                            onClick={() => setGridSubTab(t.key)}
                            className={`text-left ${gridSubTab===t.key ? 'px-2 py-0.5' : 'px-2 py-1'} rounded-md text-[11px] ${gridSubTab===t.key ? 'bg-card text-foreground border border-[hsl(var(--border))] shadow-sm' : 'text-muted-foreground hover:bg-[hsl(var(--secondary)/0.6)]'}`}
                          >
                            {t.label}
                          </button>
                        ))}
                      </div>
                      <div className="space-y-3">
                        {gridSubTab === 'hMain' && (
                          <>
                            <div className="border rounded-md p-2 space-y-2">
                              <div className="text-[11px] font-medium text-muted-foreground">Mode</div>
                              <div className="grid grid-cols-2 gap-2 items-center">
                                <label className="text-xs text-muted-foreground">Grid style</label>
                                <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={String(((local.options as any)?.chartGrid?.horizontal?.main?.mode || 'default'))}
                                  onChange={(e)=>{ const mode=(e.target.value==='custom')?'custom':'default'; const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),main:{...(prev.horizontal?.main||{}),mode}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }}>
                                  <option value="default">Default</option>
                                  <option value="custom">Custom</option>
                                </select>
                              </div>
                              <div className="text-[11px] text-muted-foreground">Default uses chart library styling. Choose Custom to configure gridlines.</div>
                            </div>
                            {String(((local.options as any)?.chartGrid?.horizontal?.main?.mode || 'default')) === 'custom' && (
                              <div className="border rounded-md p-2 grid grid-cols-2 gap-2 items-center">
                                <label className="text-xs text-muted-foreground">Show</label>
                                <input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={!!(local.options as any)?.chartGrid?.horizontal?.main?.show}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),main:{...(prev.horizontal?.main||{}),show:e.target.checked}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Type</label>
                                <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={String((local.options as any)?.chartGrid?.horizontal?.main?.type||'solid')}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),main:{...(prev.horizontal?.main||{}),type:e.target.value}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }}>
                                  {['solid','dashed','dotted'].map(t=>(<option key={t} value={t}>{t}</option>))}
                                </select>
                                <label className="text-xs text-muted-foreground">Width</label>
                                <input type="number" min={0} max={10} step={0.5} className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartGrid?.horizontal?.main?.width ?? 1)}
                                  onChange={(e)=>{ const n=Number(e.target.value||0); const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),main:{...(prev.horizontal?.main||{}),width:n}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Color</label>
                                <input type="color" className="h-8 w-12 rounded-md border" value={String((local.options as any)?.chartGrid?.horizontal?.main?.color || '#94a3b8')}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),main:{...(prev.horizontal?.main||{}),color:e.target.value}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Opacity</label>
                                <input type="number" min={0} max={1} step={0.05} className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartGrid?.horizontal?.main?.opacity ?? 0.25)}
                                  onChange={(e)=>{ const n=Math.max(0,Math.min(1,Number(e.target.value||0))); const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),main:{...(prev.horizontal?.main||{}),opacity:n}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                              </div>
                            )}
                          </>
                        )}

                        {gridSubTab === 'hSecondary' && (
                          <>
                            <div className="border rounded-md p-2 space-y-2">
                              <div className="text-[11px] font-medium text-muted-foreground">Mode</div>
                              <div className="grid grid-cols-2 gap-2 items-center">
                                <label className="text-xs text-muted-foreground">Grid style</label>
                                <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={String(((local.options as any)?.chartGrid?.horizontal?.secondary?.mode || 'default'))}
                                  onChange={(e)=>{ const mode=(e.target.value==='custom')?'custom':'default'; const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),secondary:{...(prev.horizontal?.secondary||{}),mode}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }}>
                                  <option value="default">Default</option>
                                  <option value="custom">Custom</option>
                                </select>
                              </div>
                              <div className="text-[11px] text-muted-foreground">Default uses chart library styling. Choose Custom to configure gridlines.</div>
                            </div>
                            {String(((local.options as any)?.chartGrid?.horizontal?.secondary?.mode || 'default')) === 'custom' && (
                              <div className="border rounded-md p-2 grid grid-cols-2 gap-2 items-center">
                                <label className="text-xs text-muted-foreground">Show</label>
                                <input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={!!(local.options as any)?.chartGrid?.horizontal?.secondary?.show}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),secondary:{...(prev.horizontal?.secondary||{}),show:e.target.checked}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Type</label>
                                <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={String((local.options as any)?.chartGrid?.horizontal?.secondary?.type||'dashed')}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),secondary:{...(prev.horizontal?.secondary||{}),type:e.target.value}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }}>
                                  {['solid','dashed','dotted'].map(t=>(<option key={t} value={t}>{t}</option>))}
                                </select>
                                <label className="text-xs text-muted-foreground">Width</label>
                                <input type="number" min={0} max={10} step={0.5} className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartGrid?.horizontal?.secondary?.width ?? 1)}
                                  onChange={(e)=>{ const n=Number(e.target.value||0); const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),secondary:{...(prev.horizontal?.secondary||{}),width:n}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Color</label>
                                <input type="color" className="h-8 w-12 rounded-md border" value={String((local.options as any)?.chartGrid?.horizontal?.secondary?.color || '#94a3b8')}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),secondary:{...(prev.horizontal?.secondary||{}),color:e.target.value}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Opacity</label>
                                <input type="number" min={0} max={1} step={0.05} className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartGrid?.horizontal?.secondary?.opacity ?? 0.2)}
                                  onChange={(e)=>{ const n=Math.max(0,Math.min(1,Number(e.target.value||0))); const prev=(local.options as any)?.chartGrid||{}; const next={...prev,horizontal:{...(prev.horizontal||{}),secondary:{...(prev.horizontal?.secondary||{}),opacity:n}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                              </div>
                            )}
                          </>
                        )}

                        {gridSubTab === 'vMain' && (
                          <>
                            <div className="border rounded-md p-2 space-y-2">
                              <div className="text-[11px] font-medium text-muted-foreground">Mode</div>
                              <div className="grid grid-cols-2 gap-2 items-center">
                                <label className="text-xs text-muted-foreground">Grid style</label>
                                <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={String(((local.options as any)?.chartGrid?.vertical?.main?.mode || 'default'))}
                                  onChange={(e)=>{ const mode=(e.target.value==='custom')?'custom':'default'; const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),main:{...(prev.vertical?.main||{}),mode}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }}>
                                  <option value="default">Default</option>
                                  <option value="custom">Custom</option>
                                </select>
                              </div>
                              <div className="text-[11px] text-muted-foreground">Default uses chart library styling. Choose Custom to configure gridlines.</div>
                            </div>
                            {String(((local.options as any)?.chartGrid?.vertical?.main?.mode || 'default')) === 'custom' && (
                              <div className="border rounded-md p-2 grid grid-cols-2 gap-2 items-center">
                                <label className="text-xs text-muted-foreground">Show</label>
                                <input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={!!(local.options as any)?.chartGrid?.vertical?.main?.show}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),main:{...(prev.vertical?.main||{}),show:e.target.checked}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Type</label>
                                <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={String((local.options as any)?.chartGrid?.vertical?.main?.type||'solid')}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),main:{...(prev.vertical?.main||{}),type:e.target.value}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }}>
                                  {['solid','dashed','dotted'].map(t=>(<option key={t} value={t}>{t}</option>))}
                                </select>
                                <label className="text-xs text-muted-foreground">Width</label>
                                <input type="number" min={0} max={10} step={0.5} className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartGrid?.vertical?.main?.width ?? 1)}
                                  onChange={(e)=>{ const n=Number(e.target.value||0); const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),main:{...(prev.vertical?.main||{}),width:n}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Color</label>
                                <input type="color" className="h-8 w-12 rounded-md border" value={String((local.options as any)?.chartGrid?.vertical?.main?.color || '#94a3b8')}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),main:{...(prev.vertical?.main||{}),color:e.target.value}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Opacity</label>
                                <input type="number" min={0} max={1} step={0.05} className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartGrid?.vertical?.main?.opacity ?? 0.25)}
                                  onChange={(e)=>{ const n=Math.max(0,Math.min(1,Number(e.target.value||0))); const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),main:{...(prev.vertical?.main||{}),opacity:n}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                              </div>
                            )}
                          </>
                        )}

                        {gridSubTab === 'vSecondary' && (
                          <>
                            <div className="border rounded-md p-2 space-y-2">
                              <div className="text-[11px] font-medium text-muted-foreground">Mode</div>
                              <div className="grid grid-cols-2 gap-2 items-center">
                                <label className="text-xs text-muted-foreground">Grid style</label>
                                <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={String(((local.options as any)?.chartGrid?.vertical?.secondary?.mode || 'default'))}
                                  onChange={(e)=>{ const mode=(e.target.value==='custom')?'custom':'default'; const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),secondary:{...(prev.vertical?.secondary||{}),mode}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }}>
                                  <option value="default">Default</option>
                                  <option value="custom">Custom</option>
                                </select>
                              </div>
                              <div className="text-[11px] text-muted-foreground">Default uses chart library styling. Choose Custom to configure gridlines.</div>
                            </div>
                            {String(((local.options as any)?.chartGrid?.vertical?.secondary?.mode || 'default')) === 'custom' && (
                              <div className="border rounded-md p-2 grid grid-cols-2 gap-2 items-center">
                                <label className="text-xs text-muted-foreground">Show</label>
                                <input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={!!(local.options as any)?.chartGrid?.vertical?.secondary?.show}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),secondary:{...(prev.vertical?.secondary||{}),show:e.target.checked}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Type</label>
                                <select className="px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={String((local.options as any)?.chartGrid?.vertical?.secondary?.type||'dashed')}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),secondary:{...(prev.vertical?.secondary||{}),type:e.target.value}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }}>
                                  {['solid','dashed','dotted'].map(t=>(<option key={t} value={t}>{t}</option>))}
                                </select>
                                <label className="text-xs text-muted-foreground">Width</label>
                                <input type="number" min={0} max={10} step={0.5} className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartGrid?.vertical?.secondary?.width ?? 1)}
                                  onChange={(e)=>{ const n=Number(e.target.value||0); const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),secondary:{...(prev.vertical?.secondary||{}),width:n}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Color</label>
                                <input type="color" className="h-8 w-12 rounded-md border" value={String((local.options as any)?.chartGrid?.vertical?.secondary?.color || '#94a3b8')}
                                  onChange={(e)=>{ const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),secondary:{...(prev.vertical?.secondary||{}),color:e.target.value}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                                <label className="text-xs text-muted-foreground">Opacity</label>
                                <input type="number" min={0} max={1} step={0.05} className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={Number((local.options as any)?.chartGrid?.vertical?.secondary?.opacity ?? 0.2)}
                                  onChange={(e)=>{ const n=Math.max(0,Math.min(1,Number(e.target.value||0))); const prev=(local.options as any)?.chartGrid||{}; const next={...prev,vertical:{...(prev.vertical||{}),secondary:{...(prev.vertical?.secondary||{}),opacity:n}}}; const cfg={...local, options:{...(local.options||{}), chartGrid: next}}; setLocal(cfg); updateConfig(cfg) }} />
                              </div>
                            )}
                          </>
                        )}
                      </div>
                    </div>
                  </div>
                </TabPanel>
              </TabPanels>
            </TabGroup>
          </div>
        </Section>
      )}

      {/* Table Config */}
      {local.type === 'table' && (
        <Section title="Table Config" defaultOpen>
          <div className="space-y-3">
            {/* Table Type selector */}
            <div>
              <label className="block text-xs text-muted-foreground mb-1">Table Type</label>
              <div className="grid grid-cols-2 gap-2">
                {(['data','pivot'] as const).map((t) => (
                  <label key={t} className={`flex items-center justify-center gap-1 p-2 rounded-md border cursor-pointer text-xs ${((local.options?.table?.tableType || 'data') === t) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'} `}>
                    <input
                      type="radio"
                      name="tableType"
                      className="sr-only"
                      checked={(local.options?.table?.tableType || 'data') === t}
                      onChange={() => {
                        const table = { ...(local.options?.table || {}), tableType: t }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next)
                        updateConfig(next)
                      }}
                    />
                    <span className="capitalize">{t === 'data' ? 'Data Table' : 'Pivot Table'}</span>
                  </label>
                ))}
              </div>
            </div>

            {/* Tabs from filter field */}
            <TabsControls local={local} setLocalAction={setLocal} updateConfigAction={updateConfig} allFieldNames={allFieldNames} />

            {(local.options?.table?.tableType || 'data') === 'data' && (
            <>
            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="block text-xs text-muted-foreground mb-1">Theme</label>
                <select
                  className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                  value={local.options?.table?.theme || 'quartz'}
                  onChange={(e) => { const opts = { ...(local.options || {}), table: { ...(local.options?.table || {}), theme: e.target.value as any } }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}
                >
                  {['quartz','balham','material','alpine'].map(t => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
              <div>
                <label className="block text-xs text-muted-foreground mb-1">Density</label>
                <select
                  className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                  value={local.options?.table?.density || 'compact'}
                  onChange={(e) => {
                    const t = { ...(local.options?.table || {}), density: e.target.value as any }
                    // Clear manual heights so density can drive sizes
                    delete (t as any).rowHeight
                    delete (t as any).headerHeight
                    const opts = { ...(local.options || {}), table: t }
                    const next = { ...local, options: opts }
                    setLocal(next); updateConfig(next)
                  }}
                >
                  {['compact','comfortable'].map(d => <option key={d} value={d}>{d}</option>)}
                </select>
              </div>
            </div>
  
            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="block text-xs text-muted-foreground mb-1">Row height</label>
                <input type="number" min={20} max={56} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                  value={local.options?.table?.rowHeight ?? 28}
                  onChange={(e) => { const v = Math.max(20, Math.min(56, Number(e.target.value||28))); const opts = { ...(local.options || {}), table: { ...(local.options?.table || {}), rowHeight: v } }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
              </div>
              <div>
                <label className="block text-xs text-muted-foreground mb-1">Header height</label>
                <input type="number" min={20} max={56} className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                  value={local.options?.table?.headerHeight ?? 28}
                  onChange={(e) => { const v = Math.max(20, Math.min(56, Number(e.target.value||28))); const opts = { ...(local.options || {}), table: { ...(local.options?.table || {}), headerHeight: v } }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }} />
              </div>
            </div>
            <div className="border rounded-md p-2 space-y-2">
              <div className="text-[11px] font-medium text-muted-foreground">Column Auto-fit</div>
              <div className="grid grid-cols-2 gap-2 items-center">
                <label className="text-xs text-muted-foreground">Mode</label>
                <select
                  className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                  value={local.options?.table?.autoFit?.mode || ''}
                  onChange={(e) => {
                    const mode = (e.target.value || undefined) as any
                    const table = { ...(local.options?.table || {}), autoFit: { ...(local.options?.table?.autoFit || {}), mode } }
                    const next = { ...local, options: { ...(local.options || {}), table } }
                    setLocal(next); updateConfig(next)
                  }}
                >
                  <option value="">Off</option>
                  <option value="content">Based on Content</option>
                  <option value="window">Based on Window</option>
                </select>

                <label className="text-xs text-muted-foreground">Sample rows</label>
                <input
                  type="number"
                  min={1}
                  max={100}
                  className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                  value={local.options?.table?.autoFit?.sampleRows ?? 10}
                  onChange={(e) => {
                    const n = Math.max(1, Math.min(100, Number(e.target.value||10)))
                    const table = { ...(local.options?.table || {}), autoFit: { ...(local.options?.table?.autoFit || {}), sampleRows: n } }
                    const next = { ...local, options: { ...(local.options || {}), table } }
                    setLocal(next); updateConfig(next)
                  }}
                />
              </div>
            </div>

            <div className="border rounded-md p-2 space-y-2">
              <div className="text-[11px] font-medium text-muted-foreground">Filtering & Sorting</div>
              <label className="flex items-center gap-2 text-xs">
                <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!local.options?.table?.filtering?.quickFilter}
                  onChange={(e) => { const table = { ...(local.options?.table || {}), filtering: { ...(local.options?.table?.filtering || {}), quickFilter: e.target.checked } }; const next = { ...local, options: { ...(local.options || {}), table } }; setLocal(next); updateConfig(next) }} />
                Show quick filter box
              </label>
            </div>

            <div className="border rounded-md p-2 space-y-2">
              <div className="text-[11px] font-medium text-muted-foreground">Interactions</div>
                            <div className="grid grid-cols-2 gap-2">
                              <label className="flex items-center gap-2 text-xs">
                                <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={local.options?.table?.interactions?.columnMove !== false}
                                  onChange={(e) => { const table = { ...(local.options?.table || {}), interactions: { ...(local.options?.table?.interactions || {}), columnMove: e.target.checked } }; const next = { ...local, options: { ...(local.options || {}), table } }; setLocal(next); updateConfig(next) }} />
                                Column move
                              </label>
                              <label className="flex items-center gap-2 text-xs">
                                <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={local.options?.table?.interactions?.columnResize !== false}
                                  onChange={(e) => { const table = { ...(local.options?.table || {}), interactions: { ...(local.options?.table?.interactions || {}), columnResize: e.target.checked } }; const next = { ...local, options: { ...(local.options || {}), table } }; setLocal(next); updateConfig(next) }} />
                                Column resize
                              </label>
                              <label className="flex items-center gap-2 text-xs">
                                <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!local.options?.table?.interactions?.columnHoverHighlight}
                                  onChange={(e) => { const table = { ...(local.options?.table || {}), interactions: { ...(local.options?.table?.interactions || {}), columnHoverHighlight: e.target.checked } }; const next = { ...local, options: { ...(local.options || {}), table } }; setLocal(next); updateConfig(next) }} />
                                Column hover highlight
                              </label>
                              <label className="flex items-center gap-2 text-xs">
                                <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={!!local.options?.table?.interactions?.suppressRowHoverHighlight}
                                  onChange={(e) => { const table = { ...(local.options?.table || {}), interactions: { ...(local.options?.table?.interactions || {}), suppressRowHoverHighlight: e.target.checked } }; const next = { ...local, options: { ...(local.options || {}), table } }; setLocal(next); updateConfig(next) }} />
                                Suppress row hover highlight
                              </label>
                            </div>
                          </div>
                        </>
                      )}
            {(local.options?.table?.tableType || 'data') === 'pivot' && (
              <div className="space-y-2">
                {/* Pivot Styling */}
                <div className="pt-2 pb-3 overflow-x-auto">
                  <div className="w-full min-w-0 grid grid-cols-[minmax(72px,100px),minmax(0,1fr),minmax(0,1fr)] gap-x-3 gap-y-3 items-center">
                    <div></div>
                    <div className="text-[11px] font-medium text-muted-foreground">Headers</div>
                    <div className="text-[11px] font-medium text-muted-foreground">Cells</div>

                    <label className="text-[11px] text-muted-foreground text-left">Row height (px)</label>
                    <input type="number" min={16} className="h-8 w-full px-2 rounded-md border bg-[hsl(var(--secondary))] text-xs"
                      value={local.options?.table?.pivotStyle?.headerRowHeight ?? ''}
                      onChange={(e) => {
                        const raw = e.target.value
                        const val = raw === '' ? undefined : Number(raw)
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any }
                        if (val == null || isNaN(val)) delete pivotStyle.headerRowHeight; else pivotStyle.headerRowHeight = val
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }} />
                    <input type="number" min={16} className="h-8 w-full px-2 rounded-md border bg-[hsl(var(--secondary))] text-xs"
                      value={local.options?.table?.pivotStyle?.cellRowHeight ?? ''}
                      onChange={(e) => {
                        const raw = e.target.value
                        const val = raw === '' ? undefined : Number(raw)
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any }
                        if (val == null || isNaN(val)) delete pivotStyle.cellRowHeight; else pivotStyle.cellRowHeight = val
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }} />

                    <label className="text-[11px] text-muted-foreground text-left">Font size (px)</label>
                    <input type="number" min={8} className="h-8 w-full px-2 rounded-md border bg-[hsl(var(--secondary))] text-xs"
                      value={local.options?.table?.pivotStyle?.headerFontSize ?? ''}
                      onChange={(e) => {
                        const raw = e.target.value
                        const val = raw === '' ? undefined : Number(raw)
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any }
                        if (val == null || isNaN(val)) delete pivotStyle.headerFontSize; else pivotStyle.headerFontSize = val
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }} />
                    <input type="number" min={8} className="h-8 w-full px-2 rounded-md border bg-[hsl(var(--secondary))] text-xs"
                      value={local.options?.table?.pivotStyle?.cellFontSize ?? ''}
                      onChange={(e) => {
                        const raw = e.target.value
                        const val = raw === '' ? undefined : Number(raw)
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any }
                        if (val == null || isNaN(val)) delete pivotStyle.cellFontSize; else pivotStyle.cellFontSize = val
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }} />

                    <label className="text-[11px] text-muted-foreground text-left">Font weight</label>
                    <select className="h-8 w-full px-2 rounded-md border bg-[hsl(var(--secondary))] text-xs"
                      value={local.options?.table?.pivotStyle?.headerFontWeight || 'semibold'}
                      onChange={(e) => {
                        const v = e.target.value as 'normal'|'medium'|'semibold'|'bold'
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, headerFontWeight: v }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}>
                      {(['normal','medium','semibold','bold'] as const).map((x) => <option key={x} value={x}>{x}</option>)}
                    </select>
                    <select className="h-8 w-full px-2 rounded-md border bg-[hsl(var(--secondary))] text-xs"
                      value={local.options?.table?.pivotStyle?.cellFontWeight || 'normal'}
                      onChange={(e) => {
                        const v = e.target.value as 'normal'|'medium'|'semibold'|'bold'
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, cellFontWeight: v }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}>
                      {(['normal','medium','semibold','bold'] as const).map((x) => <option key={x} value={x}>{x}</option>)}
                    </select>

                    <label className="text-[11px] text-muted-foreground text-left">Font style</label>
                    <select className="h-8 px-2 rounded-md border bg-[hsl(var(--secondary))] text-xs"
                      value={local.options?.table?.pivotStyle?.headerFontStyle || 'normal'}
                      onChange={(e) => {
                        const v = e.target.value as 'normal'|'italic'
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, headerFontStyle: v }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}>
                      {(['normal','italic'] as const).map((x) => <option key={x} value={x}>{x}</option>)}
                    </select>
                    <select className="h-8 px-2 rounded-md border bg-[hsl(var(--secondary))] text-xs"
                      value={local.options?.table?.pivotStyle?.cellFontStyle || 'normal'}
                      onChange={(e) => {
                        const v = e.target.value as 'normal'|'italic'
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, cellFontStyle: v }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}>
                      {(['normal','italic'] as const).map((x) => <option key={x} value={x}>{x}</option>)}
                    </select>

                    <label className="text-[11px] text-muted-foreground text-left">Horizontal Align</label>
                    <div className="grid grid-cols-3 gap-1 w-full">
                      {(['left','center','right'] as const).map(a => (
                        <button
                          key={a}
                          type="button"
                          className={`w-8 h-8 flex items-center justify-center rounded-md border ${(((local.options?.table?.pivotStyle as any)?.headerHAlign || 'left') === a) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`}
                          title={a}
                          onClick={() => {
                            const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, headerHAlign: a }
                            const table = { ...(local.options?.table || {}), pivotStyle }
                            const next = { ...local, options: { ...(local.options || {}), table } }
                            setLocal(next); updateConfig(next as any)
                          }}
                        >
                          {a === 'left' ? (<RiAlignLeft className="h-4 w-4" aria-hidden="true" />) : a === 'center' ? (<RiAlignCenter className="h-4 w-4" aria-hidden="true" />) : (<RiAlignRight className="h-4 w-4" aria-hidden="true" />)}
                        </button>
                      ))}
                    </div>
                    <div className="grid grid-cols-3 gap-1 w-full">
                      {(['left','center','right'] as const).map(a => (
                        <button
                          key={a}
                          type="button"
                          className={`w-8 h-8 flex items-center justify-center rounded-md border ${(((local.options?.table?.pivotStyle as any)?.cellHAlign || 'left') === a) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`}
                          title={a}
                          onClick={() => {
                            const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, cellHAlign: a }
                            const table = { ...(local.options?.table || {}), pivotStyle }
                            const next = { ...local, options: { ...(local.options || {}), table } }
                            setLocal(next); updateConfig(next as any)
                          }}
                        >
                          {a === 'left' ? (<RiAlignLeft className="h-4 w-4" aria-hidden="true" />) : a === 'center' ? (<RiAlignCenter className="h-4 w-4" aria-hidden="true" />) : (<RiAlignRight className="h-4 w-4" aria-hidden="true" />)}
                        </button>
                      ))}
                    </div>

                    <label className="text-[11px] text-muted-foreground text-left">Vertical Align</label>
                    <div className="grid grid-cols-3 gap-1 w-full">
                      {(['top','center','bottom'] as const).map(v => (
                        <button
                          key={v}
                          type="button"
                          className={`w-8 h-8 flex items-center justify-center rounded-md border ${(((local.options?.table?.pivotStyle as any)?.headerVAlign || 'top') === v) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`}
                          title={v}
                          onClick={() => {
                            const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, headerVAlign: v }
                            const table = { ...(local.options?.table || {}), pivotStyle }
                            const next = { ...local, options: { ...(local.options || {}), table } }
                            setLocal(next); updateConfig(next as any)
                          }}
                        >
                          {v === 'top' ? (<RiAlignTop className="h-4 w-4" aria-hidden="true" />) : v === 'center' ? (<RiAlignVertically className="h-4 w-4" aria-hidden="true" />) : (<RiAlignBottom className="h-4 w-4" aria-hidden="true" />)}
                        </button>
                      ))}
                    </div>
                    <div className="grid grid-cols-3 gap-1 w-full">
                      {(['top','center','bottom'] as const).map(v => (
                        <button
                          key={v}
                          type="button"
                          className={`w-8 h-8 flex items-center justify-center rounded-md border ${(((local.options?.table?.pivotStyle as any)?.cellVAlign || 'top') === v) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'}`}
                          title={v}
                          onClick={() => {
                            const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, cellVAlign: v }
                            const table = { ...(local.options?.table || {}), pivotStyle }
                            const next = { ...local, options: { ...(local.options || {}), table } }
                            setLocal(next); updateConfig(next as any)
                          }}
                        >
                          {v === 'top' ? (<RiAlignTop className="h-4 w-4" aria-hidden="true" />) : v === 'center' ? (<RiAlignVertically className="h-4 w-4" aria-hidden="true" />) : (<RiAlignBottom className="h-4 w-4" aria-hidden="true" />)}
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
                <div className="border rounded-md p-2 space-y-2">
                  <div className="text-[11px] font-medium text-muted-foreground">Pivot Options</div>
                  <label className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      className="accent-[hsl(var(--primary))]"
                      checked={(local.options?.table?.serverPivot !== false)}
                      onChange={(e) => {
                        const table = { ...(local.options?.table || {}), serverPivot: e.target.checked }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    Compute pivot on server
                  </label>
                  <div className="grid grid-cols-2 gap-2">
                    <label className="flex items-center gap-2 text-xs">
                      <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={local.options?.table?.pivotConfig?.rowTotals !== false}
                        onChange={(e) => { const table = { ...(local.options?.table || {}), pivotConfig: { ...(local.options?.table?.pivotConfig || {}), rowTotals: e.target.checked } }; const next = { ...local, options: { ...(local.options || {}), table } }; setLocal(next); updateConfig(next) }} />
                      Row totals
                    </label>
                    <label className="flex items-center gap-2 text-xs">
                      <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={local.options?.table?.pivotConfig?.colTotals !== false}
                        onChange={(e) => { const table = { ...(local.options?.table || {}), pivotConfig: { ...(local.options?.table?.pivotConfig || {}), colTotals: e.target.checked } }; const next = { ...local, options: { ...(local.options || {}), table } }; setLocal(next); updateConfig(next) }} />
                      Column totals
                    </label>
                  </div>
                  <label className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      className="accent-[hsl(var(--primary))]"
                      checked={(local.options?.table?.pivotStyle?.alternateRows !== false)}
                      onChange={(e) => {
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, alternateRows: e.target.checked }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    Alternating row background
                  </label>
                  <label className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      className="accent-[hsl(var(--primary))]"
                      checked={(local.options?.table?.pivotStyle?.rowHover !== false)}
                      onChange={(e) => {
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, rowHover: e.target.checked }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    Row hover highlight
                  </label>
                  <label className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      className="accent-[hsl(var(--primary))]"
                      checked={!!local.options?.table?.pivotStyle?.leafRowEmphasis}
                      onChange={(e) => {
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, leafRowEmphasis: e.target.checked }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    Emphasize leaf rows
                  </label>
                  <label className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      className="accent-[hsl(var(--primary))]"
                      checked={!!local.options?.table?.pivotStyle?.rowHeaderDepthHue}
                      onChange={(e) => {
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, rowHeaderDepthHue: e.target.checked }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    Hue-tint row header by depth
                  </label>
                  <label className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      className="accent-[hsl(var(--primary))]"
                      checked={!!local.options?.table?.pivotStyle?.colHeaderDepthHue}
                      onChange={(e) => {
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, colHeaderDepthHue: e.target.checked }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    Hue-tint column header by depth
                  </label>
                  <label className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      className="accent-[hsl(var(--primary))]"
                      checked={!!local.options?.table?.pivotStyle?.showSubtotals}
                      onChange={(e) => {
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, showSubtotals: e.target.checked }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    Show Subtotals (per parent)
                  </label>
                  <div className="grid grid-cols-2 gap-2 items-center">
                    <label className="text-[11px] text-muted-foreground">Expand icon style</label>
                    <Select
                      value={local.options?.table?.pivotStyle?.expandIconStyle || 'plusMinusLine'}
                      onValueChangeAction={(v) => {
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, expandIconStyle: v }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    >
                      <SelectTrigger className="h-8 px-2 rounded-md border bg-[hsl(var(--secondary))] text-xs">
                        <SelectValue placeholder="Select style" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="plusMinusLine">
                          <span className="inline-flex items-center gap-2">
                            <RiAddLine className="w-4 h-4" /> plusMinusLine
                          </span>
                        </SelectItem>
                        <SelectItem value="plusMinusFill">
                          <span className="inline-flex items-center gap-2">
                            <RiAddFill className="w-4 h-4" /> plusMinusFill
                          </span>
                        </SelectItem>
                        <SelectItem value="arrowLine">
                          <span className="inline-flex items-center gap-2">
                            <RiArrowRightSLine className="w-4 h-4" /> arrowLine
                          </span>
                        </SelectItem>
                        <SelectItem value="arrowFill">
                          <span className="inline-flex items-center gap-2">
                            <RiArrowRightSFill className="w-4 h-4" /> arrowFill
                          </span>
                        </SelectItem>
                        <SelectItem value="arrowWide">
                          <span className="inline-flex items-center gap-2">
                            <RiArrowRightWideLine className="w-4 h-4" /> arrowWide
                          </span>
                        </SelectItem>
                        <SelectItem value="arrowDrop">
                          <span className="inline-flex items-center gap-2">
                            <RiArrowDropRightLine className="w-5 h-5" /> arrowDrop
                          </span>
                        </SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <label className="flex items-center gap-2 text-xs">
                    <input
                      type="checkbox"
                      className="accent-[hsl(var(--primary))]"
                      checked={(local.options?.table?.pivotStyle?.collapseBorders !== false)}
                      onChange={(e) => {
                        const pivotStyle = { ...(local.options?.table?.pivotStyle || {}) as any, collapseBorders: e.target.checked }
                        const table = { ...(local.options?.table || {}), pivotStyle }
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    Collapse table borders
                  </label>
                  <div className="text-[11px] text-muted-foreground">
                    Server-side pivot reduces browser memory/CPU for large datasets. It groups on selected rows/columns and returns aggregated values.
                  </div>
                  {(local.options?.table?.serverPivot === false) && (
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
                    <label className="text-xs text-muted-foreground sm:col-span-1">Pivot chunk size (rows)</label>
                    <input
                      type="number"
                      min={500}
                      max={5000}
                      className="h-8 px-2 rounded-md border bg-card text-xs sm:col-span-2"
                      value={local.options?.table?.pivotChunkSize ?? ''}
                      onChange={(e) => {
                        const raw = e.target.value
                        const n = raw === '' ? NaN : Number(raw)
                        const clamped = isNaN(n) ? undefined : Math.max(500, Math.min(5000, n))
                        const table: any = { ...(local.options?.table || {}) }
                        if (clamped == null) delete table.pivotChunkSize; else table.pivotChunkSize = clamped
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                  </div>
                  )}
                  {(local.options?.table?.serverPivot === false) && (
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
                    <label className="text-xs text-muted-foreground sm:col-span-1">Pivot max rows</label>
                    <input
                      type="number"
                      min={1000}
                      className="h-8 px-2 rounded-md border bg-card text-xs sm:col-span-2"
                      value={local.options?.table?.pivotMaxRows ?? ''}
                      onChange={(e) => {
                        const raw = e.target.value
                        const n = raw === '' ? NaN : Number(raw)
                        const chunk = Number(local.options?.table?.pivotChunkSize ?? 2000)
                        const val = isNaN(n) ? undefined : Math.max(chunk || 1, n)
                        const table: any = { ...(local.options?.table || {}) }
                        if (val == null) delete table.pivotMaxRows; else table.pivotMaxRows = val
                        const next = { ...local, options: { ...(local.options || {}), table } }
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    <div className="sm:col-span-3 text-[11px] text-muted-foreground">Client-side pivot fetches chunked pages up to this cap. Larger values may impact performance.</div>
                  </div>
                  )}
                </div>

                
              </div>
            )}
            </div>
          </Section>
      )}

      {local?.type === 'kpi' && (
        <Section title="KPI Config" defaultOpen>
          <div className="space-y-3">
            <TabGroup index={kpiTab==='appearance'?0:1} onIndexChange={(i)=>setKpiTab(i===0?'appearance':'deltas')}>
              <TabList variant="solid" className="text-xs bg-[hsl(var(--secondary))] rounded-md p-1">
                <Tab className="px-3 py-1 rounded-md text-muted-foreground whitespace-nowrap data-[state=active]:bg-card data-[state=active]:text-foreground data-[state=active]:shadow-sm data-[state=active]:border data-[state=active]:border-[hsl(var(--border))]">Appearance</Tab>
                <Tab className="px-3 py-1 rounded-md text-muted-foreground whitespace-nowrap data-[state=active]:bg-card data-[state=active]:text-foreground data-[state=active]:shadow-sm data-[state=active]:border data-[state=active]:border-[hsl(var(--border))]">Deltas</Tab>
              </TabList>
              <TabPanels className="relative z-10 overflow-visible">
                <TabPanel>
                  <div className="space-y-3">
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Preset</label>
                      <div className="grid grid-cols-2 gap-3">
                {(['basic','badge','withPrevious','spark','donut','progress','categoryBar','multiProgress'] as const).map((p) => (
                  <label key={p} className={`flex items-start gap-2 p-2 rounded-lg border cursor-pointer text-xs ${((local.options?.kpi?.preset || 'basic') === p) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))] ring-offset-2 ring-offset-[hsl(var(--card))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'} `}>
                    <input
                      type="radio"
                      name="kpiPreset"
                      className="sr-only"
                      checked={(local.options?.kpi?.preset || 'basic') === p}
                      onChange={() => {
                        const kpi = { ...(local.options?.kpi || {}), preset: p }
                        const next = { ...local!, options: { ...(local.options || {}), kpi } }
                        setLocal(next as any); updateConfig(next as any)
                      }}
                    />
                    <div className="flex-1 min-w-0">
                      <div className="text-[11px] font-semibold mb-1">
                        {p === 'basic' ? 'Basic' : p === 'badge' ? 'Badge' : p === 'withPrevious' ? 'With Previous' : p === 'spark' ? 'Spark' : p === 'donut' ? 'Donut' : p === 'progress' ? 'Progress' : p === 'categoryBar' ? 'CategoryBar' : 'MultiProgress'}
                      </div>
                      <div className="rounded-md border bg-[hsl(var(--card))] p-2 w-full min-w-0 h-[84px] flex items-center justify-start overflow-hidden">
                        {p === 'basic' && (
                          <div className="leading-tight min-w-0">
                            <div className="text-[10px] text-muted-foreground mb-0.5 truncate">Unique visitors</div>
                            <div className="text-[18px] font-semibold whitespace-nowrap">10,450 <span className="text-[11px] text-rose-600 align-middle">-12.5%</span></div>
                          </div>
                        )}
                        {p === 'badge' && (
                          <div className="leading-tight w-full min-w-0">
                            <div className="text-[10px] text-muted-foreground mb-0.5 truncate">Bounce rate</div>
                            <div className="flex items-center justify-between gap-2">
                              <div className="text-[18px] font-semibold whitespace-nowrap">56.1%</div>
                              <span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-700">+1.8%</span>
                            </div>
                          </div>
                        )}
                        {p === 'withPrevious' && (
                          <div className="leading-tight min-w-0">
                            <div className="text-[10px] text-muted-foreground mb-0.5 truncate">Visit duration</div>
                            <div className="text-[18px] font-semibold whitespace-nowrap">5.2min</div>
                            <div className="text-[10px] text-muted-foreground whitespace-nowrap">from 4.3</div>
                          </div>
                        )}
                        {p === 'donut' && (
                          <div className="flex items-center gap-3 w-full min-w-0">
                            <div className="relative w-10 h-10">
                              <div className="w-10 h-10 rounded-full border-4 border-emerald-500 border-r-slate-300 border-b-slate-300" />
                              <div className="absolute inset-0 m-auto w-4 h-4 rounded-full bg-[hsl(var(--card))]" />
                              <div className="absolute inset-0 flex items-center justify-center">
                                <span className="text-[10px] font-medium text-emerald-700">64%</span>
                              </div>
                            </div>
                            <div className="leading-tight">
                              <div className="text-[16px] font-semibold">6.4k</div>
                              <div className="text-[10px] text-muted-foreground">of 10k</div>
                            </div>
                          </div>
                        )}
                        {p === 'spark' && (
                          <div className="w-full min-w-0">
                            <div className="flex items-start justify-between">
                              <div className="text-[16px] font-semibold text-emerald-600 whitespace-nowrap">$129.10</div>
                              <div className="text-[10px] text-emerald-700">+7.1%</div>
                            </div>
                            <div className="mt-1 h-8 w-full rounded-md bg-emerald-500/10 relative overflow-hidden">
                              <div className="absolute inset-x-0 bottom-0 h-1/2 bg-emerald-500/15" />
                              <svg className="absolute inset-0 w-full h-full" viewBox="0 0 100 32" preserveAspectRatio="none">
                                <polyline fill="none" stroke="#10b981" strokeWidth="2" points="2,26 12,22 24,24 36,16 48,18 60,14 72,17 84,12 96,14" />
                              </svg>
                            </div>
                          </div>
                        )}
                        {p === 'progress' && (
                          <div className="w-full min-w-0">
                            <div className="text-[16px] font-semibold mb-1 whitespace-nowrap">65%</div>
                            <div className="h-2 w-full rounded bg-emerald-200/50 overflow-hidden">
                              <div className="h-2 bg-emerald-500" style={{ width: '65%' }} />
                            </div>
                          </div>
                        )}
                        {p === 'categoryBar' && (
                          <div className="w-full min-w-0">
                            <div className="text-[16px] font-semibold">10,000</div>
                            <div className="mt-1 w-full h-2.5 rounded-full bg-muted overflow-hidden flex">
                              <div className="h-2.5 bg-cyan-500" style={{ width: '35%' }} />
                              <div className="h-2.5 bg-violet-500" style={{ width: '65%' }} />
                            </div>
                            <div className="mt-1 flex items-center gap-3 text-[10px] text-muted-foreground">
                              <span className="inline-flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-sm bg-cyan-500" /> 3.5k Completion</span>
                              <span className="inline-flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-sm bg-violet-500" /> 6.5k Prompt</span>
                            </div>
                          </div>
                        )}
                        {p === 'multiProgress' && (
                          <div className="w-full min-w-0 space-y-1">
                            <div className="flex items-center justify-between text-[10px]">
                              <span className="truncate">Alpha</span><span className="whitespace-nowrap">120 (24%)</span>
                            </div>
                            <div className="h-1.5 w-full rounded bg-blue-200/50 overflow-hidden"><div className="h-1.5 bg-blue-500" style={{ width: '24%' }} /></div>
                            <div className="flex items-center justify-between text-[10px]">
                              <span className="truncate">Beta</span><span className="whitespace-nowrap">180 (36%)</span>
                            </div>
                            <div className="h-1.5 w-full rounded bg-violet-200/50 overflow-hidden"><div className="h-1.5 bg-violet-500" style={{ width: '36%' }} /></div>
                            <div className="flex items-center justify-between text-[10px]">
                              <span className="truncate">Gamma</span><span className="whitespace-nowrap">200 (40%)</span>
                            </div>
                            <div className="h-1.5 w-full rounded bg-amber-200/50 overflow-hidden"><div className="h-1.5 bg-amber-500" style={{ width: '40%' }} /></div>
                          </div>
                        )}
                      </div>
                    </div>
                  </label>
                ))}
                      </div>
                      {(local.options?.kpi?.preset === 'spark') && (
                        <div className="mt-2 grid grid-cols-2 gap-2">
                          <div className="col-span-2">
                            <label className="block text-xs text-muted-foreground mb-1">Spark Type</label>
                            <select
                              className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                              value={(local.options?.kpi?.sparkType || 'line') as any}
                              onChange={(e) => {
                                const kpi = { ...(local.options?.kpi || {}), sparkType: (e.target.value as any) }
                                const next = { ...local!, options: { ...(local.options || {}), kpi } }
                                setLocal(next as any); updateConfig(next as any)
                              }}
                            >
                              <option value="line">SparkLineChart</option>
                              <option value="area">SparkAreaChart</option>
                              <option value="bar">SparkBarChart</option>
                            </select>
                          </div>
                        </div>
                      )}
                      {(['categoryBar','multiProgress'] as const).includes(((local.options?.kpi?.preset || 'basic') as any)) && !(local?.querySpec as any)?.legend && (
                        <div className="mt-2 text-[11px] text-muted-foreground rounded-md border p-2 bg-[hsl(var(--secondary))]">
                          Tip: Category presets work best when a Legend (Columns) field is set in Tables & Fields. Drag a category into Columns (Legend).
                        </div>
                      )}
                    </div>
                    <div className="grid grid-cols-2 gap-2">
                      <label className="flex items-center gap-2 text-xs">
                        <input
                          type="checkbox"
                          className="accent-[hsl(var(--primary))]"
                          checked={!!local.options?.kpi?.downIsGood}
                          onChange={(e) => {
                            const kpi = { ...(local.options?.kpi || {}), downIsGood: e.target.checked }
                            const next = { ...local!, options: { ...(local.options || {}), kpi } }
                            setLocal(next as any); updateConfig(next as any)
                          }}
                        />
                        Down is good (invert delta color)
                      </label>
                      <div>
                        <label className="block text-xs text-muted-foreground mb-1">Top N (Category presets)</label>
                        <input
                          type="number"
                          className="w-full h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]"
                          value={typeof local.options?.kpi?.topN === 'number' ? local.options.kpi.topN : 3}
                          onChange={(e) => {
                            const v = e.target.value === '' ? undefined : Number(e.target.value)
                            const kpi = { ...(local.options?.kpi || {}), topN: v }
                            const next = { ...local!, options: { ...(local.options || {}), kpi } }
                            setLocal(next as any); updateConfig(next as any)
                          }}
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-muted-foreground mb-1">Wrap every N (KPI tiles per row)</label>
                        <input
                          type="number"
                          min={1}
                          className="w-full h-8 px-2 rounded-md border text-[hsl(var(--foreground))] bg-[hsl(var(--secondary)/0.6)]"
                          value={typeof (local.options as any)?.kpi?.wrapEveryN === 'number' ? (local.options as any).kpi.wrapEveryN : 3}
                          onChange={(e) => {
                            const v = e.target.value === '' ? undefined : Number(e.target.value)
                            const kpi = { ...(local.options?.kpi || {}), wrapEveryN: v }
                            const next = { ...local!, options: { ...(local.options || {}), kpi } }
                            setLocal(next as any); updateConfig(next as any)
                          }}
                        />
                      </div>
                      {/* Removed: Target (Donut/Progress) */}
                    </div>
                  </div>
                </TabPanel>
                <TabPanel>
                  <div className="space-y-2">
                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <label className="block text-xs text-muted-foreground mb-1">UI Mode</label>
                        <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.deltaUI || 'preconfigured'}
                          onChange={(e) => { const opts = { ...(local.options || {}), deltaUI: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                          {(['preconfigured','filterbar','none'] as const).map((m) => <option key={m} value={m}>{m}</option>)}
                        </select>
                      </div>
                      <div>
                        <label className="block text-xs text-muted-foreground mb-1">Week start</label>
                        <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.deltaWeekStart || 'mon'}
                          onChange={(e) => { const opts = { ...(local.options || {}), deltaWeekStart: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                          {(['sat','sun','mon'] as const).map((w) => <option key={w} value={w}>{w.toUpperCase()}</option>)}
                        </select>
                      </div>
                    </div>
                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <label className="block text-xs text-muted-foreground mb-1">Delta mode</label>
                        <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.deltaMode || 'TD_YSTD'}
                          onChange={(e) => { const opts = { ...(local.options || {}), deltaMode: e.target.value as any }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                          {(['TD_YSTD','TW_LW','MONTH_LMONTH','MTD_LMTD','TY_LY','YTD_LYTD','TQ_LQ','off'] as const).map((m) => <option key={m} value={m}>{m}</option>)}
                        </select>
                      </div>
                      <div>
                        <label className="block text-xs text-muted-foreground mb-1">Date field</label>
                        <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs" value={local.options?.deltaDateField || ''}
                          onChange={(e) => { const opts = { ...(local.options || {}), deltaDateField: e.target.value || undefined }; const next = { ...local, options: opts }; setLocal(next); updateConfig(next) }}>
                          <option value="">Select date field…</option>
                          {columnNames.map((c) => <option key={c} value={c}>{c}</option>)}
                        </select>
                      </div>
                    </div>
                    {(local.options?.deltaMode && local.options.deltaMode !== 'off') && local.options?.deltaDateField && (
                      <div className="mt-2 rounded-md border bg-[hsl(var(--secondary))] p-2">
                        <div className="text-[11px] font-medium text-muted-foreground mb-1">Resolved Period Preview</div>
                        <div className="text-[11px] text-muted-foreground">
                          <div>Mode: <span className="font-mono">{String(local.options?.deltaMode)}</span></div>
                          <div>Date field: <span className="font-mono">{String(local.options?.deltaDateField)}</span></div>
                          <div className="mt-1">
                            {deltaPreviewLoading ? (
                              <span>Resolving…</span>
                            ) : deltaPreviewError ? (
                              <span className="text-red-600">{deltaPreviewError}</span>
                            ) : deltaResolved ? (
                              <div className="space-y-0.5">
                                <div>Current: <span className="font-mono">{deltaResolved.curStart}</span> → <span className="font-mono">{deltaResolved.curEnd}</span></div>
                                <div>Previous: <span className="font-mono">{deltaResolved.prevStart}</span> → <span className="font-mono">{deltaResolved.prevEnd}</span></div>
                              </div>
                            ) : (
                              <span>—</span>
                            )}
                          </div>
                          <div className="mt-1">First row in <span className="font-mono">{String(local.options?.deltaDateField)}</span>: <span className="font-mono">{deltaSampleNow ?? '—'}</span></div>
                          <div className="mt-1 flex items-center gap-2">
                            <button className="text-[11px] px-2 py-0.5 rounded-md border hover:bg-muted" onClick={() => void refreshDeltaPreview()}>Refresh</button>
                          </div>
                        </div>
                      </div>
                    )}
                  </div>
                </TabPanel>
              </TabPanels>
            </TabGroup>
          </div>
        </Section>
      )}
    {local?.type !== 'composition' && (
      <Section title="Data" defaultOpen>
        <div className="grid grid-cols-1 gap-3">
          <div>
            <label className="block text-xs text-muted-foreground mb-1">Query Mode</label>
            <div className="grid grid-cols-2 gap-2">
              {(['sql', 'spec'] as const).map((m) => (
                <label key={m} className={`flex items-center justifycenter gap-1 p-2 rounded-md border cursor-pointer text-xs ${((local.queryMode || 'sql') === m) ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary)/0.6)]'} `}>
                  <input
                    type="radio"
                    name="queryMode"
                    className="sr-only"
                    checked={(local.queryMode || 'sql') === m}
                    onChange={() => {
                      const next = { ...local, queryMode: m }
                      if (m === 'spec' && !next.querySpec) next.querySpec = { source: '', select: [] }
                      setLocal(next)
                      updateConfig(next)
                    }}
                  />
                  <span>{m === 'sql' ? 'SQL' : 'Spec (Ibis)'}</span>
                </label>
              ))}
            </div>
          </div>
          <div>
            <label className="block text-xs text-muted-foreground mb-1">Datasource</label>
            <div className="relative z-[70]">
              <Select
                value={local.datasourceId || ''}
                onValueChangeAction={(val: string) => {
                  const next = { ...local, datasourceId: val || undefined } as WidgetConfig
                  // clear spec source/columns when switching ds
                  if (next.querySpec) next.querySpec = { ...next.querySpec, source: '', select: [] }
                  setLocal(next)
                  updateConfig(next)
                }}
              >
                <SelectTrigger className="h-8 text-xs rounded-md bg-[hsl(var(--secondary))] px-3 py-1.5 border border-[hsl(var(--border))]">
                  <span className="truncate">
                    {(() => {
                      if (!local.datasourceId) return '(Default: DuckDB)'
                      const ds = (dsQ.data || []).find((d: DatasourceOut) => d.id === local.datasourceId)
                      return ds ? `${ds.name} (${ds.type})` : String(local.datasourceId)
                    })()}
                  </span>
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="">(Default: DuckDB)</SelectItem>
                  {(dsQ.data || []).map((ds: DatasourceOut) => (
                    <SelectItem key={ds.id} value={ds.id}>{ds.name} ({ds.type})</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          {/* Query routing preference (tri-state) */}
          <div>
            <label className="block text-xs text-muted-foreground mb-1">Query routing</label>
            <select
              className="h-8 px-2 rounded-md bg-card text-xs"
              value={(local.options?.preferLocalDuck === true) ? 'local' : (local.options?.preferLocalDuck === false) ? 'remote' : ''}
              onChange={(e) => {
                const v = e.target.value
                const opts: any = { ...(local.options || {}) }
                if (!v) delete opts.preferLocalDuck
                else if (v === 'local') opts.preferLocalDuck = true
                else if (v === 'remote') opts.preferLocalDuck = false
                const next = { ...local, options: opts } as WidgetConfig
                setLocal(next); updateConfig(next)
              }}
            >
              <option value="">Server default</option>
              <option value="local">Prefer local DuckDB</option>
              <option value="remote">Force remote datasource</option>
            </select>
          </div>
          {(!local.queryMode || local.queryMode === 'sql') && (
            <div>
              <label className="block text-xs text-muted-foreground mb-1">SQL</label>
              <textarea
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] font-mono text-xs"
                rows={8}
                value={local.sql}
                onChange={(e) => {
                  const next = { ...local, sql: e.target.value }
                  setLocal(next)
                  updateConfig(next)
                }}
              />
            </div>
          )}
          {/* SQL Advanced Mode trigger */}
          <div className="flex items-center justify-between">
            <div className="text-[11px] text-muted-foreground">Advanced transforms, custom columns, and joins at datasource level</div>
            <button
              className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-foreground"
              onClick={() => setAdvOpen(true)}
            >
              SQL Advanced Mode
            </button>
          </div>
          {local?.queryMode === 'spec' && (
            <div className="rounded-md p-2 bg-[hsl(var(--secondary))] space-y-2 mt-2">
              <div className="text-xs font-medium">Per-widget Sort/Top N</div>
              <label className="text-xs flex items-center gap-2">
                <input
                  type="checkbox"
                  checked={(local?.options?.dataDefaults?.useDatasourceDefaults !== false)}
                  onChange={(e) => {
                    const opts: any = { ...(local?.options || {}) }
                    const dd: any = { ...(opts.dataDefaults || {}) }
                    dd.useDatasourceDefaults = e.target.checked
                    opts.dataDefaults = dd
                    const next = { ...(local as WidgetConfig), options: opts } as WidgetConfig
                    setLocal(next); updateConfig(next)
                  }}
                />
                <span className="opacity-80">Use datasource defaults</span>
              </label>
              {local?.options?.dataDefaults?.useDatasourceDefaults === false && (
                <div className="space-y-2">
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
                    <label className="text-xs text-muted-foreground sm:col-span-1">Sort by</label>
                    <select
                      className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-1"
                      value={String(local?.options?.dataDefaults?.sort?.by || '')}
                      onChange={(e) => {
                        const val = e.target.value
                        const opts: any = { ...(local?.options || {}) }
                        const dd: any = { ...(opts.dataDefaults || {}) }
                        if (!val) { if (dd.sort) delete dd.sort }
                        else { dd.sort = { ...(dd.sort || {}), by: val } }
                        opts.dataDefaults = dd
                        const next = { ...(local as WidgetConfig), options: opts } as WidgetConfig
                        setLocal(next); updateConfig(next)
                      }}
                    >
                      <option value="">(none)</option>
                      <option value="x">x</option>
                      <option value="value">value</option>
                    </select>
                    <select
                      className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-1"
                      value={String(local?.options?.dataDefaults?.sort?.direction || 'desc')}
                      onChange={(e) => {
                        const opts: any = { ...(local?.options || {}) }
                        const dd: any = { ...(opts.dataDefaults || {}) }
                        dd.sort = { ...(dd.sort || { by: 'value' }), direction: e.target.value }
                        opts.dataDefaults = dd
                        const next = { ...(local as WidgetConfig), options: opts } as WidgetConfig
                        setLocal(next); updateConfig(next)
                      }}
                    >
                      <option value="asc">asc</option>
                      <option value="desc">desc</option>
                    </select>
                  </div>
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
                    <label className="text-xs text-muted-foreground sm:col-span-1">Top N</label>
                    <input
                      className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-1"
                      type="number"
                      min={0}
                      value={String((local?.options?.dataDefaults?.topN?.n ?? '') as any)}
                      onChange={(e) => {
                        const n = parseInt(e.target.value || '', 10)
                        const opts: any = { ...(local?.options || {}) }
                        const dd: any = { ...(opts.dataDefaults || {}) }
                        if (isNaN(n) || n <= 0) { if (dd.topN) delete dd.topN }
                        else { dd.topN = { ...(dd.topN || {}), n } }
                        opts.dataDefaults = dd
                        const next = { ...(local as WidgetConfig), options: opts } as WidgetConfig
                        setLocal(next); updateConfig(next)
                      }}
                    />
                    <div className="sm:col-span-1 grid grid-cols-2 gap-2">
                      <select
                        className="h-8 px-2 rounded-md bg-card text-xs"
                        value={String(local?.options?.dataDefaults?.topN?.by || 'value')}
                        onChange={(e) => {
                          const opts: any = { ...(local?.options || {}) }
                          const dd: any = { ...(opts.dataDefaults || {}) }
                          dd.topN = { ...(dd.topN || {}), by: e.target.value }
                          opts.dataDefaults = dd
                          const next = { ...(local as WidgetConfig), options: opts } as WidgetConfig
                          setLocal(next); updateConfig(next)
                        }}
                      >
                        <option value="x">x</option>
                        <option value="value">value</option>
                      </select>
                      <select
                        className="h-8 px-2 rounded-md bg-card text-xs"
                        value={String(local?.options?.dataDefaults?.topN?.direction || 'desc')}
                        onChange={(e) => {
                          const opts: any = { ...(local?.options || {}) }
                          const dd: any = { ...(opts.dataDefaults || {}) }
                          dd.topN = { ...(dd.topN || {}), direction: e.target.value }
                          opts.dataDefaults = dd
                          const next = { ...(local as WidgetConfig), options: opts } as WidgetConfig
                          setLocal(next); updateConfig(next)
                        }}
                      >
                        <option value="asc">asc</option>
                        <option value="desc">desc</option>
                      </select>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
          
        </div>
      </Section>
    )}
    {/* Advanced SQL Dialog */}
    <AdvancedSqlDialog
      open={advOpen}
      onCloseAction={() => setAdvOpen(false)}
      datasourceId={local?.datasourceId}
      dsType={dsType}
      schema={(schemaQ.data as any) || (fallbackQ.data as any)}
      source={local?.querySpec?.source}
      select={(local?.querySpec?.select as any) || undefined}
      widgetId={(local as any)?.id}
    />
    {local.queryMode === 'spec' && local?.type !== 'composition' && (
      <Section title="Tables & Fields" defaultOpen>
        {/* Data specifics: table / fields / SQL within the Data section */}
          <div className="space-y-3">
            {dsId ? (
              <div>
                <label className="block text-xs text-muted-foreground mb-1">Source (table/view)</label>
                <div className="grid grid-cols-[1fr,auto] gap-2 mb-1">
                  <TextInput
                    className="h-8 text-xs rounded-md bg-[hsl(var(--secondary))]"
                    placeholder="Search tables/views"
                    value={srcFilter}
                    onChange={(e) => setSrcFilter(e.target.value)}
                  />
                  <div className="text-xs text-muted-foreground flex items-center">{filteredSources.length} items</div>
                </div>
                <div className="relative z-[70]">
                  <Select
                    value={local.querySpec?.source || ''}
                    onValueChangeAction={(src: string) => {
                      const next = { ...local, querySpec: { source: src, select: [], where: undefined, x: undefined as any, y: undefined as any, legend: undefined as any, measure: undefined as any, agg: undefined as any, groupBy: undefined as any } }
                      next.options = { ...(local.options || {}), deltaDateField: undefined } as any
                      setLocal(next)
                      updateConfig(next)
                    }}
                  >
                    <SelectTrigger className="h-8 w-full text-xs rounded-md bg-[hsl(var(--secondary))] px-3 py-1.5 border border-[hsl(var(--border))]" disabled={schemaQ.isLoading || fallbackQ.isLoading}>
                      <SelectValue placeholder={(schemaQ.isLoading || fallbackQ.isLoading) ? 'Loading tables…' : 'Select…'} />
                    </SelectTrigger>
                    <SelectContent>
                      {filteredSources.map((it) => (
                        <SelectItem key={it.value} value={it.value}>{it.label}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="flex items-center justify-end mt-2">
                  <button
                    className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
                    onClick={() => {
                      if (typeof window !== 'undefined' && local?.id) {
                        try { dsTransformsQ.refetch() } catch {}
                        try { window.dispatchEvent(new CustomEvent('request-table-columns', { detail: { widgetId: local.id } } as any)) } catch {}
                        try { window.dispatchEvent(new CustomEvent('request-table-rows', { detail: { widgetId: local.id } } as any)) } catch {}
                        try { window.dispatchEvent(new CustomEvent('request-table-samples', { detail: { widgetId: local.id } } as any)) } catch {}
                      }
                    }}
                  >
                    Refresh fields
                  </button>
                </div>
              </div>
            ) : null}
            {(local.type === 'chart' || local.type === 'kpi') ? (
              <div className="rounded-md p-2 bg-[hsl(var(--secondary))]">
                {(() => {
                  const pivotAssignmentsNormalized = (() => {
                    try {
                      const legPivot = Array.isArray((pivot as any)?.legend)
                        ? ((pivot as any).legend as string[])
                        : ((pivot as any)?.legend ? [String((pivot as any).legend)] : [])
                      const legSpecRaw: any = (local?.querySpec as any)?.legend
                      const legSpec = Array.isArray(legSpecRaw) ? (legSpecRaw as string[]) : (legSpecRaw ? [String(legSpecRaw)] : [])
                      const legend = (legPivot && legPivot.length > 0) ? legPivot : legSpec
                      return { ...pivot, legend }
                    } catch { return pivot }
                  })()
                  return (
                    <PivotBuilder
                  fields={allFieldNames}
                  measures={local.measures || []}
                  assignments={pivotAssignmentsNormalized as any}
                  numericFields={numericFields}
                  dateLikeFields={dateLikeFields}
                  update={(p: PivotAssignments) => applyPivot(p)}
                  selectFieldAction={(kind: 'x'|'value'|'legend'|'filter', field: string) => { setSelKind(kind); setSelField(field) }}
                  selected={selKind && selField ? { kind: selKind, id: selField } : undefined}
                  disableRows={false}
                  disableValues={false}
                  allowMultiLegend
                  datasourceId={dsId as any}
                  source={local?.querySpec?.source}
                  widgetId={local?.id}
                  valueRequired={local.type === 'chart' ? (local.chartType !== 'tremorTable') : (local.type === 'kpi')}
                />
                  )
                })()}
              </div>
            ) : ((local.type === 'table') && ((local.options?.table?.tableType || 'data') === 'data')) ? (
              <div className="rounded-md p-2 bg-[hsl(var(--secondary))]">
                {(() => {
                  const pivotAssignmentsNormalized = (() => {
                    try {
                      const legPivot = Array.isArray((pivot as any)?.legend)
                        ? ((pivot as any).legend as string[])
                        : ((pivot as any)?.legend ? [String((pivot as any).legend)] : [])
                      const legSpecRaw: any = (local?.querySpec as any)?.legend
                      const legSpec = Array.isArray(legSpecRaw) ? (legSpecRaw as string[]) : (legSpecRaw ? [String(legSpecRaw)] : [])
                      const legend = (legPivot && legPivot.length > 0) ? legPivot : legSpec
                      return { ...pivot, legend }
                    } catch { return pivot }
                  })()
                  return (
                    <PivotBuilder
                  fields={allFieldNames}
                  measures={local.measures || []}
                  assignments={pivotAssignmentsNormalized as any}
                  numericFields={numericFields}
                  dateLikeFields={dateLikeFields}
                  update={(p: PivotAssignments) => applyPivot(p)}
                  selectFieldAction={(kind: 'x'|'value'|'legend'|'filter', field: string) => { setSelKind(kind); setSelField(field) }}
                  selected={selKind && selField ? { kind: selKind, id: selField } : undefined}
                  disableRows
                  disableValues
                  allowMultiLegend
                  datasourceId={dsId as any}
                  source={local?.querySpec?.source}
                  widgetId={local?.id}
                  valueRequired={false}
                />
                  )
                })()}
              </div>
            ) : ((local.type === 'table') && ((local.options?.table?.tableType || 'data') === 'pivot')) ? (
              <div className="rounded-md p-2 bg-[hsl(var(--secondary))]">
                {(() => {
                  const pcfg = (local.options?.table?.pivotConfig || {}) as any
                  const presetFilters = Array.isArray((local as any)?.pivot?.filters) ? ((local as any)?.pivot?.filters as string[]) : undefined
                  const prevVals = Array.isArray((local as any)?.pivot?.values) ? (((local as any).pivot.values) as any[]) : []
                  const assignments = {
                    x: Array.isArray(pcfg.rows) ? pcfg.rows : [],
                    legend: Array.isArray(pcfg.cols) ? pcfg.cols : (pcfg.cols ? [pcfg.cols] : []),
                    values: Array.isArray(pcfg.vals)
                      ? pcfg.vals.map((f: string) => {
                          const prev = prevVals.find((v: any) => (v?.field === f || v?.measureId === f)) || {}
                          return { field: f, agg: prev.agg, label: prev.label }
                        })
                      : [],
                    filters: presetFilters || Object.keys(pcfg.filters || {}),
                  }
                  const onAssign = (p: PivotAssignments) => {
                    const rows: string[] = Array.isArray(p.x) ? (p.x as string[]) : (p.x ? [p.x as string] : [])
                    const cols = Array.isArray(p.legend) ? p.legend : (p.legend ? [String(p.legend)] : [])
                    const vals = (p.values || []).map((v) => v.field || 'value')
                    const filtersArr = Array.isArray(p.filters) ? p.filters.filter(Boolean) : []
                    // aggregatorName removed; PivotMatrixView reads aggregator from pivot.values
                    const nextCfgRaw = { ...(local.options?.table?.pivotConfig || {}), rows, cols, vals } as any
                    delete nextCfgRaw.aggregatorName
                    const nextCfg = nextCfgRaw
                    const table = { ...(local.options?.table || {}), pivotConfig: nextCfg }
                    // Also keep top-level pivot in sync for filter exposures and other UIs
                    const nextPivot = {
                      x: p.x,
                      legend: p.legend,
                      values: p.values,
                      filters: filtersArr,
                    }
                    const updated = { ...local, options: { ...(local.options || {}), table }, pivot: nextPivot as any }
                    schedulePivotUpdate(updated)
                  }
                  return (
                    <>
                      <PivotBuilder
                        fields={allFieldNames}
                        measures={local.measures || []}
                        assignments={assignments as any}
                        numericFields={numericFields}
                        dateLikeFields={dateLikeFields}
                        update={onAssign as any}
                        selectFieldAction={(kind: 'x'|'value'|'legend'|'filter', field: string) => { setSelKind(kind); setSelField(field) }}
                        selected={selKind && selField ? { kind: selKind, id: selField } : undefined}
                        allowMultiLegend
                        allowMultiRows
                        datasourceId={dsId as any}
                        source={local?.querySpec?.source}
                        widgetId={local?.id}
                        valueRequired={true}
                      />
                      <div className="mt-2 text-[10px] text-muted-foreground space-y-1">
                        <div>
                          <div className="font-semibold mb-1">Debug: datasource customColumns</div>
                          <div className="flex flex-wrap gap-1">
                            {(() => {
                              try {
                                const list = (((dsTransformsQ.data as any)?.customColumns || []) as Array<{ name?: string }>)
                                  .map((c) => String(c?.name || '')).filter(Boolean)
                                return list.length ? list.map((n) => (
                                  <span key={n} className="px-1.5 py-0.5 rounded border bg-[hsl(var(--secondary))]">{n}</span>
                                )) : <span className="opacity-70">(none)</span>
                              } catch {
                                return <span className="opacity-70">(error reading)</span>
                              }
                            })()}
                          </div>
                        </div>
                        <div>
                          <div className="font-semibold mb-1">Debug: live result columns</div>
                          <div className="flex flex-wrap gap-1">
                            {(resultColumns && resultColumns.length > 0) ? resultColumns.map((n) => (
                              <span key={n} className={`px-1.5 py-0.5 rounded border ${n==='VaultName'?'bg-[hsl(var(--secondary)/0.6)] ring-1 ring-[hsl(var(--primary))]':'bg-[hsl(var(--secondary))]'}`}>{n}</span>
                            )) : <span className="opacity-70">(none yet)</span>}
                          </div>
                        </div>
                      </div>
                    </>
                  )
                })()}
              </div>
            ) : null}
            {local?.type === 'kpi' && (
              <Section title="KPI Options" defaultOpen>
                <div className="grid grid-cols-[140px,1fr] gap-2 items-center">
                  <label className="text-xs text-muted-foreground">Aggregation mode</label>
                  <Select value={String((local.options?.kpi?.aggregationMode || 'count') as any)} onValueChangeAction={(val: string) => {
                    const kpi = { ...((local.options?.kpi || {}) as any), aggregationMode: val as any }
                    const opts = { ...(local.options || {}), kpi }
                    const next = { ...local, options: opts }
                    setLocal(next); updateConfig(next)
                  }}>
                    <SelectTrigger className="w-full rounded-md border border-[hsl(var(--border))] bg-transparent px-3 py-2 text-xs">
                      <SelectValue placeholder="Select..." />
                    </SelectTrigger>
                    <SelectContent>
                      {(['none','sum','count','distinctCount','avg','min','max','first','last'] as const).map((m) => (
                        <SelectItem key={m} value={m}>{m}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                <div className="text-[11px] text-muted-foreground mt-1">Applies to KPI tiles that show category totals (e.g., badges/category tiles). Defaults to count.</div>
              </Section>
            )}
            <div>
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-muted-foreground">Measures</span>
                <NewMeasureButton
                  columns={columns.map((c) => c.name)}
                  onCreate={(name: string, formula: string) => {
                    const m = { id: Math.random().toString(36).slice(2), name, formula }
                    const next = { ...local!, measures: [...(local?.measures || []), m] } as WidgetConfig
                    setLocal(next)
                    updateConfig(next)
                  }}
                />
              </div>
              <div className="flex flex-wrap gap-1">
                {(local.measures || []).map((m) => (
                  <span
                    key={m.id}
                    className="inline-flex items-center gap-1 px-2 py-1 rounded border bg-[hsl(var(--secondary))] text-xs cursor-grab"
                    draggable
                    onDragStart={(e) => {
                      const p = JSON.stringify({ kind: 'measure', id: m.id })
                      e.dataTransfer.setData('application/json', p)
                      try { e.dataTransfer.setData('text/plain', p) } catch {}
                      e.dataTransfer.effectAllowed = 'move'
                    }}
                  >
                    ∑ {m.name}
                    <button
                      className="text-[10px] opacity-70 hover:opacity-100 ml-1"
                      onClick={(ev) => {
                        ev.preventDefault(); ev.stopPropagation()
                        const next = { ...local!, measures: (local?.measures || []).filter(x => x.id !== m.id) } as WidgetConfig
                        setLocal(next)
                        updateConfig(next)
                      }}
                      title="Remove measure"
                    >
                      ✕
                    </button>
                  </span>
                ))}
                {!local.measures?.length && <span className="text-[10px] opacity-70">No measures yet</span>}
              </div>
            </div>
            {/* Custom Columns */}
            <div className="mt-3">
              <div className="flex items-center justify-between mb-1">
                <span className="text-xs text-muted-foreground">Custom Columns</span>
                <NewCustomColumnButton
                  columns={columnNames}
                  onCreate={(name: string, formula: string, type?: any) => {
                    const cc = { id: Math.random().toString(36).slice(2), name, formula, type }
                    const next = { ...local!, customColumns: [...(local?.customColumns || []), cc] } as WidgetConfig
                    setLocal(next)
                    updateConfig(next)
                  }}
                  sampleRows={sampleRows}
                  onRequestRows={fetchPreviewRowsForFormula}
                  samplesByField={samplesByField}
                />
              </div>
              <div className="flex flex-wrap gap-1">
                {(local.customColumns || []).map((c) => (
                  <span
                    key={c.id || c.name}
                    className="inline-flex items-center gap-1 px-2 py-1 rounded border bg-[hsl(var(--secondary))] text-xs cursor-pointer"
                    onClick={() => setEditingCustom(c)}
                    title="Click to edit"
                  >
                    <span className="truncate max-w-[120px]" title={c.name}>{c.name}</span>
                    <button
                      className="text-[10px] opacity-70 hover:opacity-100 ml-1"
                      onClick={(ev) => {
                        ev.preventDefault(); ev.stopPropagation()
                        const next = { ...local!, customColumns: (local?.customColumns || []).filter(x => (x.id || x.name) !== (c.id || c.name)) } as WidgetConfig
                        setLocal(next)
                        updateConfig(next)
                      }}
                      title="Remove custom column"
                    >
                      ✕
                    </button>
                  </span>
                ))}
                {!local.customColumns?.length && <span className="text-[10px] opacity-70">No custom columns yet</span>}
              </div>
              {editingCustom && (
                <div className="mt-2">
                  <NewCustomColumnButton
                    columns={columnNames}
                    mode="edit"
                    initial={editingCustom}
                    onSave={(id, name, formula, type) => {
                      const list = [...(local?.customColumns || [])]
                      const idx = list.findIndex(x => (x.id || x.name) === (id || editingCustom.name))
                      const nextItem = { id: id || editingCustom.id, name, formula, type }
                      if (idx >= 0) list[idx] = nextItem
                      else list.push(nextItem)
                      const next = { ...local!, customColumns: list } as WidgetConfig
                      setLocal(next)
                      updateConfig(next)
                      setEditingCustom(null)
                    }}
                    sampleRows={sampleRows}
                    onRequestRows={fetchPreviewRowsForFormula}
                    autoOpen
                    hideTriggerButton
                    samplesByField={samplesByField}
                    onClose={() => setEditingCustom(null)}
                  />
                </div>
              )}
            </div>
          </div>
        </Section>
      )}
      

      {/* Details panel for selected field */}
      {selKind && local?.type !== 'composition' && (
      <Section title="Details" defaultOpen>
        <div className="flex items-center justify-between text-xs font-medium mb-2">
          <div>
            Editing: <span className="px-1.5 py-0.5 rounded border bg-[hsl(var(--secondary))]">{selKind}</span>
            {selField && <span className="ml-2 px-1.5 py-0.5 rounded border bg-[hsl(var(--secondary))]">{selField}</span>}
          </div>
          <button
            className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted"
            onClick={() => { setSelKind(null); setSelField(undefined) }}
            title="Close details"
          >
            Close
          </button>
        </div>
        {/* Data labels (Y) format */}
        <div>
          <label className="block text-xs text-muted-foreground mb-1">Data Labels format</label>
          <select
            className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
            value={local.options?.yAxisFormat || 'none'}
            onChange={(e) => {
              const opts = { ...(local.options || {}), yAxisFormat: e.target.value as any }
              const next = { ...local, options: opts }
              setLocal(next)
              updateConfig(next)
            }}
          >
            <option value="none">None</option>
            <option value="short">Short (K/M/B)</option>
            <option value="abbrev">Number Abbreviation</option>
            <option value="currency">Currency</option>
            <option value="percent">Percent</option>
            <option value="bytes">Bytes</option>
            <option value="wholeNumber">Whole Number</option>
            <option value="number">Number (with thousands)</option>
            <option value="thousands">Thousands (e.g., 100K)</option>
            <option value="millions">Millions (e.g., 100M)</option>
            <option value="billions">Billions (e.g., 100B)</option>
            <option value="oneDecimal">1 Decimal</option>
            <option value="twoDecimals">2 Decimals</option>
            <option value="percentWhole">Percent (Whole)</option>
            <option value="percentOneDecimal">Percent (1 Decimal)</option>
            <option value="timeHours">Time (Hours)</option>
            <option value="timeMinutes">Time (Minutes)</option>
            <option value="distance-km">Distance (km)</option>
            <option value="distance-mi">Distance (mi)</option>
          </select>
          {local.options?.yAxisFormat === 'currency' && (
            <div className="mt-2 grid grid-cols-2 gap-2">
              {(() => {
                // Build currency options from Intl for codes, symbols, and names
                const locale = local.options?.valueFormatLocale || 'en-US'
                let items: Array<{ code: string; symbol: string; name: string }> = []
                try {
                  const codes: string[] = (Intl as any)?.supportedValuesOf ? (Intl as any).supportedValuesOf('currency') : []
                  const dn: any = (Intl as any)?.DisplayNames ? new (Intl as any).DisplayNames([locale], { type: 'currency' }) : null
                  items = codes.map((code) => {
                    let symbol = ''
                    try {
                      const parts = new Intl.NumberFormat(locale, { style: 'currency', currency: code, maximumFractionDigits: 0 }).formatToParts(1)
                      symbol = (parts.find((p) => p.type === 'currency')?.value) || ''
                    } catch {}
                    const name = (dn && typeof dn.of === 'function') ? (dn.of(code) || code) : code
                    return { code, symbol, name }
                  }).sort((a, b) => a.code.localeCompare(b.code))
                } catch {}
                return (
                  <>
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Currency</label>
                      {items.length > 0 ? (
                        <Select
                          value={local.options?.valueCurrency || ''}
                          onValueChangeAction={(val: string) => {
                            const opts = { ...(local.options || {}), valueCurrency: val || undefined }
                            const next = { ...local!, options: opts }
                            setLocal(next); updateConfig(next)
                          }}
                        >
                          <SelectTrigger className="w-full rounded-md border border-[hsl(var(--border))] bg-transparent px-3 py-2 text-xs">
                            <SelectValue placeholder="Select currency..." />
                          </SelectTrigger>
                          <SelectContent>
                            {items.map((it) => (
                              <SelectItem key={it.code} value={it.code}>{it.code} · {it.symbol || ''} · {it.name}</SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      ) : (
                        <input
                          className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                          placeholder="e.g., USD, EUR"
                          value={local.options?.valueCurrency || ''}
                          onChange={(e) => {
                            const opts = { ...(local.options || {}), valueCurrency: e.target.value || undefined }
                            const next = { ...local!, options: opts }
                            setLocal(next); updateConfig(next)
                          }}
                        />
                      )}
                    </div>
                  </>
                )
              })()}
            </div>
          )}
        </div>
        {selKind === 'x' ? (
          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="block text-xs text-muted-foreground mb-1">Group by</label>
              <select
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                value={local.xAxis?.groupBy || 'none'}
                onChange={(e) => {
                  const groupBy = e.target.value as any
                  const next = { ...local, xAxis: { ...(local.xAxis || {}), groupBy }, querySpec: { ...(local.querySpec || { source: '' }), groupBy } }
                  setLocal(next)
                  updateConfig(next)
                }}
              >
                {['none', 'hour', 'day', 'week', 'month', 'quarter', 'year'].map((g) => (
                  <option key={g} value={g}>{g}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-xs text-muted-foreground mb-1">Label case (row)</label>
              <select
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                value={(local.options as any)?.xLabelCase || 'proper'}
                onChange={(e) => {
                  const opts = { ...(local.options || {}), xLabelCase: e.target.value as any }
                  const next = { ...local, options: opts }
                  setLocal(next)
                  updateConfig(next)
                }}
              >
                <option value="proper">Proper</option>
                <option value="uppercase">Uppercase</option>
                <option value="lowercase">Lowercase</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-muted-foreground mb-1">X-axis date format</label>
              <select
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                value={(() => { const v = (local.options as any)?.xDateFormat as string | undefined; if (!v) return 'none'; const presets = new Set(['YYYY','YYYY-MM','YYYY-MM-DD','h:mm a','dddd','MMMM','MMM-YYYY']); return presets.has(v) ? v : 'custom' })()}
                onChange={(e) => {
                  const val = e.target.value
                  let nextFmt: any
                  if (val === 'none') nextFmt = undefined
                  else if (val === 'custom') nextFmt = (local.options as any)?.xDateFormat || ''
                  else nextFmt = val as any
                  const opts = { ...(local.options || {}), xDateFormat: nextFmt }
                  const next = { ...local, options: opts }
                  setLocal(next)
                  updateConfig(next)
                }}
              >
                <option value="none">Auto (by grouping)</option>
                <option value="YYYY">YYYY</option>
                <option value="YYYY-MM">YYYY-MM</option>
                <option value="YYYY-MM-DD">YYYY-MM-DD</option>
                <option value="h:mm a">HR:MM AM/PM</option>
                <option value="dddd">DDDD (weekday)</option>
                <option value="MMMM">MMMM (month name)</option>
                <option value="MMM-YYYY">MMM-YYYY</option>
                <option value="custom">Custom…</option>
              </select>
              {(() => {
                const v = (local.options as any)?.xDateFormat as string | undefined
                const presets = new Set(['YYYY','YYYY-MM','YYYY-MM-DD','h:mm a','dddd','MMMM','MMM-YYYY'])
                const isCustom = !!v && !presets.has(v)
                return (
                  isCustom ? (
                    <input
                      type="text"
                      className="mt-1 w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                      placeholder="e.g. MM-YYYY, MMM-YYYY, DD-MMM, dddd, ddd/MM"
                      value={String(v || '')}
                      onChange={(e) => {
                        const opts = { ...(local.options || {}), xDateFormat: e.target.value }
                        const next = { ...local, options: opts }
                        setLocal(next)
                        updateConfig(next)
                      }}
                    />
                  ) : null
                )
              })()}
            </div>
            <div>
              <label className="block text-xs text-muted-foreground mb-1">Week starts on</label>
              <select
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                value={(local.options as any)?.xWeekStart || 'mon'}
                onChange={(e) => {
                  const val = e.target.value as 'mon' | 'sun'
                  const opts = { ...(local.options || {}), xWeekStart: val }
                  const next = { ...local, options: opts }
                  setLocal(next)
                  updateConfig(next)
                }}
              >
                <option value="mon">Monday</option>
                <option value="sun">Sunday</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-muted-foreground mb-1">Max X ticks</label>
              <input
                type="number"
                min={2}
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                value={typeof (local.options as any)?.xTickCount === 'number' ? (local.options as any).xTickCount : 8}
                onChange={(e) => {
                  const raw = e.target.value
                  const n = raw === '' ? undefined : Math.max(2, Number(raw))
                  const opts = { ...(local.options || {}), xTickCount: n }
                  const next = { ...local, options: opts }
                  setLocal(next)
                  updateConfig(next)
                }}
              />
            </div>
            <div>
              <label className="block text-xs text-muted-foreground mb-1">X tick angle (°)</label>
              <select
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                value={String(((local.options as any)?.xTickAngle ?? 0))}
                onChange={(e) => {
                  const raw = e.target.value
                  const n = raw === '' ? undefined : (parseInt(raw, 10) as 0|30|45|60|90)
                  const opts = { ...(local.options || {}), xTickAngle: n }
                  const next = { ...local, options: opts }
                  setLocal(next)
                  updateConfig(next)
                }}
              >
                <option value="0">0</option>
                <option value="30">30</option>
                <option value="45">45</option>
                <option value="60">60</option>
                <option value="90">90</option>
              </select>
            </div>
            <div>
              <label className="block text-xs text-muted-foreground mb-1">Dense threshold</label>
              <input
                type="number"
                min={0}
                className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                value={typeof (local.options as any)?.xDenseThreshold === 'number' ? (local.options as any).xDenseThreshold : 12}
                onChange={(e) => {
                  const raw = e.target.value
                  const n = raw === '' ? undefined : Math.max(0, Number(raw))
                  const opts = { ...(local.options || {}), xDenseThreshold: n }
                  const next = { ...local, options: opts }
                  setLocal(next)
                  updateConfig(next)
                }}
              />
            </div>
            <div className="col-span-2">
              <label className="flex items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  className="accent-[hsl(var(--primary))]"
                  checked={(local.options as any)?.autoCondenseXLabels !== false}
                  onChange={(e) => {
                    const opts = { ...(local.options || {}), autoCondenseXLabels: e.target.checked }
                    const next = { ...local, options: opts }
                    setLocal(next)
                    updateConfig(next)
                  }}
                />
                Auto condense X labels
              </label>
            </div>
          </div>
        ) : selKind === 'value' ? (
          <div className="grid grid-cols-2 gap-2">
            {(() => {
              const sel = pivot.values.find(v => ((v.measureId ? v.measureId : v.field) === selField))
              const isMeasure = !!sel?.measureId
              return (
                <>
                  {!isMeasure && (
                    <div>
                      <label className="block text-xs text-muted-foreground mb-1">Aggregation</label>
                      <select
                        className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                        value={sel?.agg || 'count'}
                        onChange={(e) => {
                          const agg = e.target.value as any
                          const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, agg } : v) }
                          applyPivot(nextP)
                        }}
                      >
                        {['none', 'count', 'distinct', 'avg', 'sum', 'min', 'max'].map((a) => (
                          <option key={a} value={a}>{a}</option>
                        ))}
                      </select>
                    </div>
                  )}
                  {(local.chartType && ['combo','line','area','bar','column','scatter'].includes(local.chartType)) && (
                    <label className="flex items-center gap-2 text-xs">
                      <input
                        type="checkbox"
                        className="accent-[hsl(var(--primary))]"
                        checked={!!sel?.secondaryAxis}
                        onChange={(e) => {
                          const secondaryAxis = e.target.checked
                          const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, secondaryAxis } : v) }
                          applyPivot(nextP)
                        }}
                      />
                      Secondary axis
                    </label>
                  )}
                  <div>
                    <label className="block text-xs text-muted-foreground mb-1">Label</label>
                    <input
                      className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                      value={sel?.label || ''}
                      onChange={(e) => {
                        const label = e.target.value
                        const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, label } : v) }
                        applyPivot(nextP)
                      }}
                    />
                  </div>
                  <div>
                    <label className="block text-xs text-muted-foreground mb-1">Color</label>
                    <select
                      className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                      value={tokenToColorKey((sel?.colorToken as 1|2|3|4|5|undefined) || 1)}
                      onChange={(e) => {
                        const key = e.target.value as AvailableChartColorsKeys
                        const colorToken = colorKeyToToken(key) as 1|2|3|4|5
                        const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, colorToken } : v) }
                        applyPivot(nextP)
                      }}
                    >
                      {chartColors.slice(0,5).map((c) => (
                        <option key={c} value={c}>{c}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs text-muted-foreground mb-1">Sort by</label>
                    <select
                      className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                      value={String((sel as any)?.sort?.by || '')}
                      onChange={(e) => {
                        const byRaw = e.target.value as ('x'|'value'|'')
                        const sort = byRaw ? ({ by: (byRaw as 'x'|'value'), direction: ((sel as any)?.sort?.direction || 'desc') as ('asc'|'desc') }) : undefined
                        const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? ({ ...v, sort } as any) : v) }
                        applyPivot(nextP)
                      }}
                    >
                      <option value="">None</option>
                      <option value="x">X label</option>
                      <option value="value">Value</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-xs text-muted-foreground mb-1">Direction</label>
                    <select
                      className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                      disabled={!((sel as any)?.sort?.by)}
                      value={String((sel as any)?.sort?.direction || 'desc')}
                      onChange={(e) => {
                        const dir = (e.target.value as 'asc'|'desc')
                        const sPrev = (sel as any)?.sort
                        const sort = sPrev?.by ? ({ by: sPrev.by as ('x'|'value'), direction: dir }) : undefined
                        const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? ({ ...v, sort } as any) : v) }
                        applyPivot(nextP)
                      }}
                    >
                      <option value="asc">asc</option>
                      <option value="desc">desc</option>
                    </select>
                  </div>
                  {local.options?.advancedMode && (
                    <>
                      <div>
                        <label className="block text-xs text-muted-foreground mb-1">Stack group</label>
                        <input
                          className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                          value={sel?.stackId || ''}
                          onChange={(e) => {
                            const stackId = e.target.value
                            const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, stackId } : v) }
                            applyPivot(nextP)
                          }}
                          placeholder="e.g., A"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-muted-foreground mb-1">Fill style</label>
                        <select
                          className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                          value={sel?.style || 'solid'}
                          onChange={(e) => {
                            const style = e.target.value as any
                            const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, style } : v) }
                            applyPivot(nextP)
                          }}
                        >
                          <option value="solid">solid</option>
                          <option value="gradient">gradient</option>
                        </select>
                      </div>
                      <div className="col-span-2">
                        <div className="flex items-center justify-between mb-1">
                          <label className="text-xs text-muted-foreground">Conditional formatting rules</label>
                          <button
                            className="text-[10px] px-2 py-0.5 rounded border hover:bg-muted"
                            onClick={() => {
                              const rules = [...(sel?.conditionalRules || []), { when: '>', value: 0, color: 'rose' } as any]
                              const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, conditionalRules: rules } : v) }
                              applyPivot(nextP)
                            }}
                          >Add rule</button>
                        </div>
                        <div className="space-y-1 max-h-[220px] overflow-auto pr-1">
                          {(sel?.conditionalRules || []).map((r, idx) => (
                            <div key={idx} className="grid grid-cols-[110px,1fr,120px,auto] items-center gap-2">
                              <select
                                className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                value={r.when}
                                onChange={(e) => {
                                  const rules = (sel?.conditionalRules || []).map((rr, i) => i===idx ? { ...rr, when: e.target.value as any } : rr)
                                  const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, conditionalRules: rules } : v) }
                                  applyPivot(nextP)
                                }}
                              >
                                {['>','>=','<','<=','equals','between'].map(w => <option key={w} value={w}>{w}</option>)}
                              </select>
                              {r.when === 'between' ? (
                                <input
                                  className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                  placeholder="min,max"
                                  value={Array.isArray(r.value) ? `${r.value[0]},${r.value[1]}` : ''}
                                  onChange={(e) => {
                                    const parts = e.target.value.split(',').map(v => Number(v.trim()))
                                    const val: [number, number] = [Number(parts[0]||0), Number(parts[1]||0)]
                                    const rules = (sel?.conditionalRules || []).map((rr, i) => i===idx ? { ...rr, value: val } : rr)
                                    const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, conditionalRules: rules } : v) }
                                    applyPivot(nextP)
                                  }}
                                />
                              ) : (
                                <input
                                  type="number"
                                  className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                  value={Array.isArray(r.value) ? 0 : Number(r.value)}
                                  onChange={(e) => {
                                    const val = Number(e.target.value)
                                    const rules = (sel?.conditionalRules || []).map((rr, i) => i===idx ? { ...rr, value: val } : rr)
                                    const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, conditionalRules: rules } : v) }
                                    applyPivot(nextP)
                                  }}
                                />
                              )}
                              <select
                                className="px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                                value={r.color || 'rose'}
                                onChange={(e) => {
                                  const color = e.target.value as any
                                  const rules = (sel?.conditionalRules || []).map((rr, i) => i===idx ? { ...rr, color } : rr)
                                  const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, conditionalRules: rules } : v) }
                                  applyPivot(nextP)
                                }}
                              >
                                {['blue','emerald','violet','amber','gray','rose','indigo','cyan','pink','lime','fuchsia'].map(c => <option key={c} value={c}>{c}</option>)}
                              </select>
                              <button
                                className="text-[10px] px-2 py-0.5 rounded border hover:bg-muted"
                                onClick={() => {
                                  const rules = (sel?.conditionalRules || []).filter((_, i) => i !== idx)
                                  const nextP: PivotAssignments = { ...pivot, values: pivot.values.map(v => ((v.measureId ? v.measureId : v.field) === selField) ? { ...v, conditionalRules: rules } : v) }
                                  applyPivot(nextP)
                                }}
                              >Remove</button>
                            </div>
                          ))}
                        </div>
                      </div>
                    </>
                  )}
                </>
              )
            })()}
          </div>
        ) : selKind === 'legend' ? (
          <div className="grid grid-cols-2 gap-2">
            {local?.type === 'chart' && (
              <div>
                <label className="block text-xs text-muted-foreground mb-1">Label case (legend)</label>
                <select
                  className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                  value={(local.options as any)?.legendLabelCase || 'proper'}
                  onChange={(e) => {
                    const opts = { ...(local.options || {}), legendLabelCase: e.target.value as any }
                    const next = { ...local, options: opts }
                    setLocal(next)
                    updateConfig(next)
                  }}
                >
                  <option value="proper">Proper</option>
                  <option value="uppercase">Uppercase</option>
                  <option value="lowercase">Lowercase</option>
                </select>
              </div>
            )}
            {local?.type === 'kpi' && (
              <div>
                <label className="block text-xs text-muted-foreground mb-1">Label case (categories)</label>
                <select
                  className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                  value={((local.options as any)?.kpi?.labelCase || 'proper') as any}
                  onChange={(e) => {
                    const kpi = { ...((local.options as any)?.kpi || {}), labelCase: e.target.value as any }
                    const next = { ...local!, options: { ...(local.options || {}), kpi } }
                    setLocal(next as any)
                    updateConfig(next as any)
                  }}
                >
                  <option value="proper">Proper</option>
                  <option value="capitalize">Capitalize</option>
                  <option value="uppercase">Uppercase</option>
                  <option value="lowercase">Lowercase</option>
                </select>
              </div>
            )}
          </div>
        ) : selKind === 'filter' ? (
          <>
            {(() => {
              // infer kind from available samples
              const base = (samplesByField?.[selField!] || []) as string[]
              const numHits = base.filter((s) => Number.isFinite(Number(s))).length
              const dateHits = base.filter((s) => {
                const str = String(s).trim()
                if (!str) return false
                if (/^\d{10,13}$/.test(str)) return true
                if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return true
                if (/^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$/.test(str)) return true
                if (/^[A-Za-z]{3,9}[-\s]\d{4}$/.test(str)) return true
                if (/^([0-1]?\d)\/([0-3]?\d)\/(\d{4})(?:\s+(\d{2}:\d{2}(?::\d{2})?))?$/.test(str)) return true
                const norm = str.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
                const d = new Date(norm)
                return !isNaN(d.getTime())
              }).length
              let kind: 'string'|'number'|'date' = 'string'
              const half = Math.max(1, Math.ceil(base.length/2))
              if (numHits >= half) kind = 'number'
              else if (dateHits >= half) kind = 'date'
              if (kind === 'string') {
                const f = String(selField||'')
                const where = (local.querySpec?.where || {}) as Record<string, any>
                const hasDateOps = ["__gte","__lte","__gt","__lt"].some(op => (where as any)[`${f}${op}`] != null)
                const nameLooksDate = /(date|time|timestamp|_at|created|updated)/i.test(f)
                if (hasDateOps || nameLooksDate) kind = 'date'
              }
              return (
                <FilterDetailsTabs key={selField!} kind={kind} selField={selField!} local={local} setLocal={setLocal} updateConfig={updateConfig} />
              )
            })()}
            <div className="mt-2 flex items-start justify-between gap-2">
              <label className="flex items-center gap-2 text-xs">
                <input
                  type="checkbox"
                  className="accent-[hsl(var(--primary))]"
                  checked={!!local.options?.filtersExpose?.[selField!]}
                  onChange={(e) => {
                    const filtersExpose = { ...(local.options?.filtersExpose || {}) }
                    filtersExpose[selField!] = e.target.checked
                    const next = { ...local, options: { ...(local.options || {}), filtersExpose } } as WidgetConfig
                    setLocal(next)
                    updateConfig(next)
                  }}
                />
                Expose in chart filterbar
              </label>
              <div className="text-[11px] text-muted-foreground">Overrides Filters UI for this field</div>
            </div>
          </>
        ) : (
          <div className="text-xs text-muted-foreground">No options for this field.</div>
        )}
      </Section>
      )}
      {local?.type === 'chart' && (
        <Section title="Filters UI" defaultOpen>
          <div className="space-y-2">
            <label className="block text-xs text-muted-foreground mb-1">Show pivot filters as filterbars</label>
            <select
              className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
              value={local?.options?.filtersUI || 'off'}
              onChange={(e) => {
                const opts = { ...(local?.options || {}), filtersUI: e.target.value as any }
                const next = { ...local!, options: opts } as WidgetConfig
                setLocal(next)
                updateConfig(next)
              }}
            >
              <option value="off">Off</option>
              <option value="filterbars">Filterbars</option>
            </select>
            <div className="text-[11px] text-muted-foreground">When enabled, fields added to Filters in the pivot will render as filterbars above the chart (date, number, or string).</div>
          </div>
        </Section>
      )}
    </div>
  
  )
}

function Section({ title, children, defaultOpen }: { title: string; children: React.ReactNode; defaultOpen?: boolean }) {
  return (
    <Accordion type="single" collapsible defaultValue={defaultOpen ? 'item' : undefined}>
      <AccordionItem value="item">
        <AccordionTrigger>{title}</AccordionTrigger>
        <AccordionContent>
          {children}
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  )
}
// Removed unused MeasureBuilder component
