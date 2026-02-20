"use client"

import { useEffect, useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Api, type IntrospectResponse } from '@/lib/api'
import * as SchemaCache from '@/lib/schemaCache'
import type { WidgetConfig } from '@/types/widgets'
import { useConfigUpdate } from '@/components/builder/ConfigUpdateContext'
import { useAuth } from '@/components/providers/AuthProvider'
import { GeneralTab } from './GeneralTab'
import { DataTab } from './DataTab'
import { VisualizeTab } from './VisualizeTab'
import { ActiveBadge } from './shared'
import {
  RiSearchLine,
  RiArrowGoBackLine,
  RiArrowGoForwardLine,
  RiGridLine,
} from '@remixicon/react'

type Tab = 'general' | 'data' | 'visualize'

interface Props {
  selected: WidgetConfig | null
  allWidgets?: Record<string, WidgetConfig>
  quickAddAction?: (kind: 'kpi' | 'chart' | 'table', opts?: { addToLayout?: boolean }) => string
}

// ── Active counts for tab badges ──────────────────────────────────────────────
function useTabCounts(local: WidgetConfig | null) {
  return useMemo(() => {
    if (!local) return { general: 0, data: 0, visualize: 0 }
    const whereCount = local.querySpec?.where
      ? Object.values(local.querySpec.where).filter(v => v !== undefined && v !== null).length
      : 0
    const filterCount = (local.pivot?.filters?.length || 0) + whereCount
    const hasChartType = local.type === 'chart' && !!local.chartType
    const hasKpiPreset = local.type === 'kpi' && !!local.options?.kpi?.preset
    return {
      general: 0,
      data: filterCount,
      visualize: (hasChartType || hasKpiPreset) ? 1 : 0,
    }
  }, [local])
}

// ── Main Component ────────────────────────────────────────────────────────────
export default function ConfiguratorPanelV2({ selected, allWidgets, quickAddAction }: Props) {
  const updateConfig = useConfigUpdate()
  const { user } = useAuth()

  const [local, setLocal] = useState<WidgetConfig | null>(selected)
  const [activeTab, setActiveTab] = useState<Tab>('general')
  const [search, setSearch] = useState('')
  const [history, setHistory] = useState<WidgetConfig[]>([])
  const [historyIndex, setHistoryIndex] = useState(-1)
  const historyPaused = useRef(false)

  // Sync incoming selected → local (reset on widget switch)
  useEffect(() => {
    setLocal(selected)
    setHistory(selected ? [selected] : [])
    setHistoryIndex(selected ? 0 : -1)
    setResultColumns([])
    setSamplesByField({})
  }, [selected?.id])

  // ── Samples from widget events ───────────────────────────────────────────────
  const [samplesByField, setSamplesByField] = useState<Record<string, string[]>>({})
  useEffect(() => {
    if (typeof window === 'undefined') return
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail
      if (detail?.widgetId !== local?.id) return
      setSamplesByField(prev => ({ ...prev, ...detail.samples }))
    }
    window.addEventListener('table-sample-values-change', handler)
    return () => window.removeEventListener('table-sample-values-change', handler)
  }, [local?.id])

  // ── Schema query — mirrors old ConfiguratorPanel ─────────────────────────────
  const defaultDsId = useMemo(() => {
    try { return typeof window !== 'undefined' ? localStorage.getItem('default_ds_id') : null } catch { return null }
  }, [])

  const dsId = local?.datasourceId as string | undefined
  const effectiveDsId = dsId ?? (defaultDsId || undefined)

  const schemaQ = useQuery({
    queryKey: ['ds-schema', effectiveDsId ?? '_local'],
    queryFn: () => effectiveDsId ? Api.introspect(effectiveDsId) : Api.introspectLocal(),
    retry: 0,
    refetchOnWindowFocus: false,
    staleTime: 5 * 60 * 1000,
    gcTime: 10 * 60 * 1000,
    initialData: effectiveDsId ? (SchemaCache.get(effectiveDsId) || undefined) : undefined,
  })

  useEffect(() => {
    if (effectiveDsId && schemaQ.data) SchemaCache.set(effectiveDsId, schemaQ.data as IntrospectResponse)
  }, [effectiveDsId, schemaQ.data])

  // ── Extract columns for the currently selected table/view ───────────────────
  const schemaColumns = useMemo(() => {
    const src = local?.querySpec?.source
    const data = schemaQ.data as IntrospectResponse | undefined
    if (!src || !data) return [] as { name: string; type?: string | null }[]

    // Try "schema.table" split first
    const parts = src.split('.')
    if (parts.length >= 2) {
      const tblName = parts[parts.length - 1]
      const schName = parts.slice(0, parts.length - 1).join('.')
      const sch = (data.schemas || []).find(s => s.name === schName)
      const tbl = sch?.tables.find(t => t.name === tblName)
      if (tbl) return tbl.columns || []
    }

    // Fallback: search all schemas by table name
    for (const sch of (data.schemas || [])) {
      const tbl = sch.tables.find(t => t.name === src || `${sch.name}.${t.name}` === src)
      if (tbl) return tbl.columns || []
    }
    return []
  }, [schemaQ.data, local?.querySpec?.source])

  // ── Live result columns from widget (table-columns-change event) ─────────────
  const [resultColumns, setResultColumns] = useState<string[]>([])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string; columns?: string[] }
      if (!d?.widgetId || d.widgetId !== local?.id) return
      if (!Array.isArray(d.columns)) return
      setResultColumns(d.columns)
    }
    window.addEventListener('table-columns-change', handler)
    return () => window.removeEventListener('table-columns-change', handler)
  }, [local?.id])

  // Reset live columns when table source changes
  useEffect(() => { setResultColumns([]) }, [local?.querySpec?.source])

  // ── Derived field lists — same logic as old ConfiguratorPanel ────────────────
  const columnNames = useMemo(() => {
    const baseNames = schemaColumns.map(c => c.name)
    const reserved = new Set(['value', '__metric__'])
    const res = resultColumns.filter(n => !reserved.has(String(n || '').toLowerCase()))
    return Array.from(new Set([...baseNames, ...res]))
  }, [schemaColumns, resultColumns])

  const numericFields = useMemo(() => {
    const isNum = (t?: string | null) => {
      if (t == null) return true // unknown type: allow all aggs
      return /(int|bigint|smallint|tinyint|float|double|decimal|numeric|real|money|number)/i.test(String(t))
    }
    // Prefer schema types; fall back to sample-value heuristic
    if (schemaColumns.length > 0) return schemaColumns.filter(c => isNum(c.type)).map(c => c.name)
    return columnNames.filter(c => {
      const vals = samplesByField[c] || []
      return vals.length > 0 && vals.every(v => !isNaN(Number(v)) && v !== '')
    })
  }, [schemaColumns, columnNames, samplesByField])

  const dateLikeFields = useMemo(() => {
    const isDate = (t?: string | null) =>
      t ? /(date|time|timestamp)/i.test(String(t)) : false
    // Prefer schema types; fall back to sample-value heuristic
    if (schemaColumns.length > 0) {
      const fromSchema = schemaColumns.filter(c => isDate(c.type)).map(c => c.name)
      if (fromSchema.length > 0) return fromSchema
    }
    return columnNames.filter(c => {
      const vals = samplesByField[c] || []
      return vals.length > 0 && vals.some(v => /\d{4}-\d{2}/.test(String(v)))
    })
  }, [schemaColumns, columnNames, samplesByField])

  const tabCounts = useTabCounts(local)

  // ── Undo / Redo ───────────────────────────────────────────────────────────────
  const setLocalWithHistory = (next: WidgetConfig) => {
    setLocal(next)
    if (historyPaused.current) return
    setHistory(prev => {
      const trimmed = prev.slice(0, historyIndex + 1)
      const capped = [...trimmed, next].slice(-25)
      setHistoryIndex(capped.length - 1)
      return capped
    })
  }

  const undo = () => {
    if (historyIndex <= 0) return
    const prev = history[historyIndex - 1]
    historyPaused.current = true
    setLocal(prev)
    updateConfig(prev)
    setHistoryIndex(i => i - 1)
    setTimeout(() => { historyPaused.current = false }, 50)
  }

  const redo = () => {
    if (historyIndex >= history.length - 1) return
    const next = history[historyIndex + 1]
    historyPaused.current = true
    setLocal(next)
    updateConfig(next)
    setHistoryIndex(i => i + 1)
    setTimeout(() => { historyPaused.current = false }, 50)
  }

  // Tab visibility
  const hasVisualTab = local?.type === 'chart' || local?.type === 'kpi' || local?.type === 'table'
  const hasDataTab = !['composition', 'report', 'text', 'spacer'].includes(local?.type || '')

  useEffect(() => {
    if (activeTab === 'visualize' && !hasVisualTab) setActiveTab('general')
    if (activeTab === 'data' && !hasDataTab) setActiveTab('general')
  }, [local?.type])

  // ── Empty state ───────────────────────────────────────────────────────────────
  if (!local) {
    return (
      <div className="flex flex-col items-center justify-center h-48 gap-3 text-muted-foreground">
        <RiGridLine className="size-8 opacity-30" />
        <div className="text-center">
          <div className="text-sm font-medium">No widget selected</div>
          <div className="text-xs mt-0.5 opacity-70">Click a widget on the canvas to configure it</div>
        </div>
      </div>
    )
  }

  const searchLower = search.toLowerCase().trim()

  const tabs: { key: Tab; label: string; count: number; hidden?: boolean }[] = [
    { key: 'general',   label: 'General',   count: tabCounts.general },
    { key: 'data',      label: 'Data',      count: tabCounts.data,     hidden: !hasDataTab },
    { key: 'visualize', label: 'Visualize', count: tabCounts.visualize, hidden: !hasVisualTab },
  ]

  return (
    <div className="flex flex-col gap-0 min-h-0">

      {/* ── Search + Undo bar ─────────────────────────────────────────────── */}
      <div className="flex items-center gap-1.5 px-0 pb-2">
        <div className="relative flex-1">
          <RiSearchLine className="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground pointer-events-none" />
          <input
            className="w-full h-8 pl-8 pr-2.5 text-xs rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] focus:outline-none focus:ring-1 focus:ring-[hsl(var(--primary))] placeholder:text-muted-foreground"
            placeholder="Search settings…"
            value={search}
            onChange={e => setSearch(e.target.value)}
          />
          {search && (
            <button className="absolute right-2 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground cursor-pointer"
              onClick={() => setSearch('')}>✕</button>
          )}
        </div>
        <button
          className={`h-8 w-8 flex items-center justify-center rounded-md border transition-colors duration-150 ${historyIndex > 0 ? 'hover:bg-muted cursor-pointer' : 'opacity-30 cursor-not-allowed'}`}
          onClick={undo} disabled={historyIndex <= 0} title="Undo">
          <RiArrowGoBackLine className="size-4" />
        </button>
        <button
          className={`h-8 w-8 flex items-center justify-center rounded-md border transition-colors duration-150 ${historyIndex < history.length - 1 ? 'hover:bg-muted cursor-pointer' : 'opacity-30 cursor-not-allowed'}`}
          onClick={redo} disabled={historyIndex >= history.length - 1} title="Redo">
          <RiArrowGoForwardLine className="size-4" />
        </button>
      </div>

      {/* ── Tab navigation ────────────────────────────────────────────────── */}
      <div className="flex items-center gap-0.5 border-b mb-3">
        {tabs.filter(t => !t.hidden).map(t => (
          <button key={t.key} type="button" onClick={() => setActiveTab(t.key)}
            className={`relative flex items-center gap-1.5 px-3 py-2 text-xs font-medium transition-colors duration-150 cursor-pointer border-b-2 -mb-px ${
              activeTab === t.key
                ? 'border-[hsl(var(--primary))] text-[hsl(var(--primary))]'
                : 'border-transparent text-muted-foreground hover:text-foreground hover:border-[hsl(var(--border))]'
            }`}>
            {t.label}
            {t.count > 0 && <ActiveBadge count={t.count} />}
          </button>
        ))}
      </div>

      {/* ── Search hint ──────────────────────────────────────────────────── */}
      {searchLower && (
        <div className="mb-2 px-1 text-xs text-muted-foreground flex items-center gap-1.5">
          <RiSearchLine className="size-3.5 shrink-0" />
          Showing settings matching <span className="font-mono font-semibold text-foreground">&quot;{search}&quot;</span>
          <span>— check all tabs</span>
        </div>
      )}

      {/* ── Tab content ──────────────────────────────────────────────────── */}
      <div className="min-h-0">
        {activeTab === 'general' && (
          <GeneralTab local={local} setLocal={setLocalWithHistory} updateConfig={updateConfig} search={search} allWidgets={allWidgets} quickAddAction={quickAddAction} />
        )}
        {activeTab === 'data' && hasDataTab && (
          <DataTab
            local={local}
            setLocal={setLocalWithHistory}
            updateConfig={updateConfig}
            samplesByField={samplesByField}
            allFieldNames={columnNames}
            numericFields={numericFields}
            dateLikeFields={dateLikeFields}
            search={search}
          />
        )}
        {activeTab === 'visualize' && hasVisualTab && (
          <VisualizeTab local={local} setLocal={setLocalWithHistory} updateConfig={updateConfig} allFieldNames={columnNames} search={search} />
        )}
      </div>

    </div>
  )
}
