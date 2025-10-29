"use client"

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { AgGridReact } from 'ag-grid-react'
import type { ColDef, GridApi, IAggFuncParams } from 'ag-grid-community'
import { ModuleRegistry, AllCommunityModule, themeQuartz, themeBalham, themeMaterial, themeAlpine } from 'ag-grid-community'
import type { TableOptions } from '@/types/widgets'

// Register all Community features once
try { ModuleRegistry.registerModules([AllCommunityModule]) } catch {}

// Custom distinct aggregator
const distinctAgg = (params: IAggFuncParams): number => {
  const set = new Set<any>()
  ;(params.values || []).forEach((v: any) => set.add(v))
  return set.size
}

function isNumericColumn(samples: any[]): boolean {
  // Consider numeric if most of the non-null samples are numbers
  let num = 0, tot = 0
  for (const s of samples) {
    if (s === null || s === undefined || s === '') continue
    tot++
    if (typeof s === 'number' && isFinite(s)) num++
    else if (typeof s === 'string' && s.trim() !== '' && !Number.isNaN(Number(s))) num++
  }
  return tot > 0 && num / tot >= 0.7
}

function formatValue(mode: 'none'|'short'|'currency'|'percent'|'bytes'|undefined, v: any): string {
  const n = Number(v)
  if (!isFinite(n)) return String(v)
  switch (mode) {
    case 'short': {
      const abs = Math.abs(n)
      if (abs >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(1)}B`
      if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
      if (abs >= 1_000) return `${(n / 1_000).toFixed(1)}K`
      return String(n)
    }
    case 'currency': {
      try { return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(n) } catch { return `$${n.toFixed(2)}` }
    }
    case 'percent': return `${n.toFixed(1)}%`
    case 'bytes': {
      const units = ['B','KB','MB','GB','TB']
      let val = n, i = 0
      while (Math.abs(val) >= 1024 && i < units.length - 1) { val /= 1024; i++ }
      return `${val.toFixed(1)} ${units[i]}`
    }
    default: return String(v)
  }
}

export default function AgTable({
  columns,
  rows,
  tableOptions,
  onFilterWhereChangeAction,
}: {
  columns: string[]
  rows: Array<Record<string, any>>
  tableOptions?: TableOptions
  onFilterWhereChangeAction?: (where: Record<string, any>) => void
}) {
  const gridApi = useRef<GridApi | null>(null)
  const columnApi = useRef<any | null>(null)
  const [quick, setQuick] = useState('')
  const wrapRef = useRef<HTMLDivElement | null>(null)

  // Simplified view: no pivoting; show raw columns/rows
  const { viewColumns, viewRows } = useMemo(() => {
    return { viewColumns: columns, viewRows: rows }
  }, [columns, rows])

  const fields = useMemo(() => viewColumns.map((_, i) => `c${i}`), [viewColumns])
  const autoFitMode = (tableOptions as any)?.autoFit?.mode as ('content'|'window'|undefined)
  const sampleRowsCount = Math.max(1, Math.min(100, Number(((tableOptions as any)?.autoFit?.sampleRows ?? 10))))

  const [colWidths, setColWidths] = useState<Record<string, number> | null>(null)

  // Compute content-based widths and optionally scale to window
  useEffect(() => {
    const mode = autoFitMode
    if (!mode) { setColWidths(null); return }
    const measure = (containerW?: number) => {
      const charPx = 8
      const padPx = 24
      const minPx = 80
      const maxPx = 600
      const sampleN = Math.min(sampleRowsCount, viewRows.length)
      const natural: Array<{ field: string; w: number }> = []
      for (let idx = 0; idx < viewColumns.length; idx++) {
        const f = `c${idx}`
        const headerChars = String(viewColumns[idx] ?? '').length
        let maxChars = headerChars
        for (let i = 0; i < sampleN; i++) {
          const v = (viewRows[i] || ({} as any))[f]
          const s = (v === null || v === undefined) ? '' : String(v)
          if (s.length > maxChars) maxChars = s.length
        }
        const px = Math.max(minPx, Math.min(maxPx, Math.round(maxChars * charPx + padPx)))
        natural.push({ field: f, w: px })
      }
      if (mode === 'window' && containerW && containerW > 0) {
        const total = natural.reduce((a, b) => a + b.w, 0)
        if (total <= 0) { setColWidths(Object.fromEntries(natural.map(n => [n.field, n.w]))); return }
        const target = Math.max(200, Math.floor(containerW - 16))
        const scale = target / total
        const scaled = natural.map(n => ({ field: n.field, w: Math.max(minPx, Math.floor(n.w * scale)) }))
        let sum = scaled.reduce((a, b) => a + b.w, 0)
        let i = 0
        while (sum < target && i < scaled.length) { scaled[i].w += 1; sum += 1; i++ }
        setColWidths(Object.fromEntries(scaled.map(n => [n.field, n.w])))
      } else {
        setColWidths(Object.fromEntries(natural.map(n => [n.field, n.w])))
      }
    }
    const run = () => measure(autoFitMode === 'window' ? (wrapRef.current?.clientWidth || 0) : undefined)
    run()
    if (autoFitMode === 'window' && wrapRef.current) {
      const ro = new ResizeObserver(() => run())
      try { ro.observe(wrapRef.current) } catch {}
      return () => { try { ro.disconnect() } catch {} }
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [autoFitMode, sampleRowsCount, JSON.stringify(viewColumns), JSON.stringify(viewRows.slice(0, sampleRowsCount))])

  // Build column definitions for the effective view
  const colDefs: ColDef[] = useMemo(() => {
    const sampleCount = Math.min(viewRows.length, 50)
    const samplesByField: Record<string, any[]> = {}
    fields.forEach((f) => {
      samplesByField[f] = []
      for (let i = 0; i < sampleCount; i++) samplesByField[f].push(viewRows[i]?.[f])
    })
    // Community build: pivot features removed

    return viewColumns.map((name, idx) => {
      const f = `c${idx}`
      const numeric = isNumericColumn(samplesByField[f])
      const base: ColDef = {
        headerName: name,
        field: f,
        type: numeric ? 'numericColumn' : undefined,
      }
      // Pivot features removed

      const perCol = tableOptions?.columns?.[name]
      if (perCol) {
        if (perCol.headerName) base.headerName = perCol.headerName
        if (perCol.hide != null) base.hide = perCol.hide
        if (perCol.pinned) base.pinned = perCol.pinned
        if (perCol.width) base.width = perCol.width
        if (perCol.minWidth) base.minWidth = perCol.minWidth
        if (perCol.maxWidth) base.maxWidth = perCol.maxWidth
        if (perCol.type) base.type = perCol.type
        if (perCol.aggFunc) base.aggFunc = perCol.aggFunc as any
        if (perCol.valueFormatter && perCol.valueFormatter !== 'none') {
          const mode = perCol.valueFormatter as any
          base.valueFormatter = (p: any) => formatValue(mode, p.value)
        }
      }
      const w = (colWidths || undefined)?.[f]
      if (w && Number.isFinite(w)) base.width = w
      return base
    })
  }, [viewColumns, viewRows, fields, tableOptions, colWidths])

  const defaultColDef = useMemo<ColDef>(() => {
    const sortable = tableOptions?.defaultCol?.sortable ?? true
    const filter = false
    const resizable = tableOptions?.defaultCol?.resizable ?? true
    const floating = false
    return {
      sortable,
      filter,
      resizable,
      floatingFilter: floating,
    }
  }, [tableOptions?.defaultCol])

  // Pivot mode removed

  // Theming API (v33+): choose built-in theme (no params to avoid version mismatch issues)
  const themeObj = useMemo(() => {
    const t = tableOptions?.theme || 'quartz'
    return t === 'balham' ? themeBalham : t === 'material' ? themeMaterial : t === 'alpine' ? themeAlpine : themeQuartz
  }, [tableOptions?.theme])

  const rowSelection = useMemo(() => {
    const m = tableOptions?.selection?.mode || 'none'
    if (m === 'single') return 'single' as const
    if (m === 'multiple') return 'multiple' as const
    return undefined
  }, [tableOptions?.selection?.mode])

  const domLayout = tableOptions?.performance?.domLayout || 'autoHeight'
  const density = tableOptions?.density || 'compact'
  const rowHeight = tableOptions?.rowHeight ?? (density === 'compact' ? 28 : 36)
  const headerHeight = tableOptions?.headerHeight ?? (density === 'compact' ? 28 : 36)
  // Community edition path

  // Grid ready: register agg funcs, load column state
  const onGridReady = () => {
    const api = gridApi.current
    const cApi = columnApi.current
    if (!api || !cApi) return
    // Register custom aggregators
    try { (api as any).addAggFuncs?.({ distinct: distinctAgg }) } catch {}
    // Restore column state if present
    const stateToApply = tableOptions?.state?.columnState
    if (stateToApply) {
      try { cApi.applyColumnState({ state: stateToApply, applyOrder: true }) } catch {}
    }
    if (quick) api.setGridOption('quickFilterText', quick)
    // Apply hover highlights
    try {
      api.setGridOption('suppressRowHoverHighlight', !!tableOptions?.interactions?.suppressRowHoverHighlight)
      api.setGridOption('columnHoverHighlight', !!tableOptions?.interactions?.columnHoverHighlight)
    } catch {}
  }

  // Save column state on changes
  const onColumnChanged = () => {
    const api = gridApi.current
    const cApi = columnApi.current
    if (!api || !cApi) return
    // If you want to persist column state in the future, call cApi.getColumnState() here
  }

  // AG Grid now uses new prop names in v31+; we set them via setGridOption when needed
  // React to option changes after grid is ready
  React.useEffect(() => {
    const api = gridApi.current
    if (!api) return
    try {
      api.setGridOption('suppressRowHoverHighlight', !!tableOptions?.interactions?.suppressRowHoverHighlight)
      api.setGridOption('columnHoverHighlight', !!tableOptions?.interactions?.columnHoverHighlight)
      api.setGridOption('quickFilterText', quick)
    } catch {}
  }, [tableOptions?.interactions?.suppressRowHoverHighlight, tableOptions?.interactions?.columnHoverHighlight, quick])

  // Map AG Grid filter model to server WHERE patch and emit to parent
  const onFilterChanged = () => {
    const api = gridApi.current
    if (!api) return
    try {
      const model: Record<string, any> = (api as any).getFilterModel?.() || {}
      const where: Record<string, any> = {}
      const toYmd = (d: any): string | null => {
        try {
          if (!d) return null
          if (typeof d === 'string') {
            const s = d.slice(0, 10)
            return /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null
          }
          const dt = (d instanceof Date) ? d : new Date(d)
          if (isNaN(dt.getTime())) return null
          return dt.toISOString().slice(0, 10)
        } catch { return null }
      }
      const addDateRange = (field: string, from?: any, to?: any) => {
        const a = toYmd(from)
        const b = toYmd(to)
        if (a) where[`${field}__gte`] = a
        if (b) {
          const d = new Date(`${b}T00:00:00`); if (!isNaN(d.getTime())) { d.setDate(d.getDate() + 1); where[`${field}__lt`] = d.toISOString().slice(0, 10) }
        }
      }
      const entries = Object.entries(model)
      for (const [colId, fm] of entries) {
        // Map c{idx} -> column name
        const idx = Number(String(colId).replace(/^c/, ''))
        const name = Number.isFinite(idx) && idx >= 0 && idx < columns.length ? columns[idx] : undefined
        if (!name || !fm) continue
        // Combined conditions support: take first condition for now
        const node = fm?.operator ? (fm?.condition1 || fm) : fm
        const type = String(node?.filterType || '').toLowerCase()
        const kind = String(node?.type || '').toLowerCase()
        if (type === 'text') {
          const val = node?.filter
          if (val != null && val !== '') {
            if (kind === 'equals') where[name] = [String(val)]
            // contains/startsWith/endsWith not supported server-side here
          }
        } else if (type === 'number') {
          const v = Number(node?.filter)
          const v2 = Number(node?.filterTo)
          if (kind === 'equals' && Number.isFinite(v)) where[name] = [v]
          else if (kind === 'greaterthan' && Number.isFinite(v)) where[`${name}__gt`] = v
          else if (kind === 'greaterthanequals' && Number.isFinite(v)) where[`${name}__gte`] = v
          else if (kind === 'lessthan' && Number.isFinite(v)) where[`${name}__lt`] = v
          else if (kind === 'lessthanequals' && Number.isFinite(v)) where[`${name}__lte`] = v
          else if (kind === 'inrange' && Number.isFinite(v) && Number.isFinite(v2)) { where[`${name}__gte`] = v; where[`${name}__lte`] = v2 }
        } else if (type === 'date') {
          if (kind === 'equals') addDateRange(name, node?.dateFrom, node?.dateFrom)
          else if (kind === 'inrange') addDateRange(name, node?.dateFrom, node?.dateTo)
          else if (kind === 'greaterthan' || kind === 'greaterthanequals') addDateRange(name, node?.dateFrom, undefined)
          else if (kind === 'lessthan' || kind === 'lessthanequals') addDateRange(name, undefined, node?.dateFrom)
        }
      }
      onFilterWhereChangeAction?.(where)
    } catch {}
  }

  return (
    <div className="space-y-2">
      {tableOptions?.filtering?.quickFilter && (
        <input
          value={quick}
          onChange={(e) => { setQuick(e.target.value); gridApi.current?.setGridOption('quickFilterText', e.target.value) }}
          placeholder="Quick filter..."
          className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
        />
      )}
      {/* The wrapper sets AG Grid CSS variables to align with Tremor table colors in both light/dark */}
      <div
        ref={wrapRef}
        className="rounded-md border bg-card overflow-x-auto"
        style={{
          width: '100%',
          // Core surface/text/border
          ['--ag-background-color' as any]: 'hsl(var(--card))',
          ['--ag-foreground-color' as any]: 'hsl(var(--foreground))',
          ['--ag-border-color' as any]: 'hsl(var(--border))',
          // Header styling akin to Tremor table head
          ['--ag-header-background-color' as any]: 'hsl(var(--muted))',
          ['--ag-header-foreground-color' as any]: 'hsl(var(--foreground))',
          ['--ag-header-column-resize-handle-color' as any]: 'hsl(var(--border))',
          ['--ag-header-column-separator-display' as any]: 'block',
          ['--ag-header-column-separator-height' as any]: '70%',
          ['--ag-header-column-separator-color' as any]: 'color-mix(in oklab, hsl(var(--foreground)) 32%, hsl(var(--border)))',
          // Row hover and zebra striping similar to Tremor Table
          ['--ag-row-hover-color' as any]: 'color-mix(in oklab, hsl(var(--muted)) 45%, transparent)',
          ['--ag-odd-row-background-color' as any]: 'color-mix(in oklab, hsl(var(--secondary)) 40%, transparent)',
          // Grid chrome
          ['--ag-input-border-color' as any]: 'hsl(var(--border))',
          ['--ag-control-panel-background-color' as any]: 'hsl(var(--secondary))',
        }}
      >
        <AgGridReact
          theme={themeObj as any}
          rowData={viewRows}
          columnDefs={colDefs}
          defaultColDef={defaultColDef}
          // Pivot features removed
          suppressAggFuncInHeader={!!tableOptions?.aggregation?.omitAggNameInHeader}
          autoGroupColumnDef={{ minWidth: 200 }}
          animateRows={true}
          rowSelection={rowSelection as any}
          rowHeight={rowHeight}
          headerHeight={headerHeight}
          domLayout={domLayout as any}
          suppressMovableColumns={tableOptions?.interactions?.columnMove === false}
          onGridReady={(e: any) => { gridApi.current = e.api; columnApi.current = e.columnApi; onGridReady() }}
          onColumnMoved={onColumnChanged}
          onColumnVisible={onColumnChanged}
          onColumnPinned={onColumnChanged}
          // No pinned totals in simple data table
        />
      </div>
    </div>
  )
}
