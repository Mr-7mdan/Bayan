"use client"

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { RiFilter3Line, RiArrowUpDownLine, RiBrushLine, RiGridLine, RiRulerLine, RiChat3Line, RiPriceTag3Line, RiArrowRightSLine, RiArrowLeftSLine, RiArrowLeftLine } from '@remixicon/react'
import { TextInput } from '@tremor/react'
import { QueryApi, Api, type AlertOut } from '@/lib/api'
import { Switch } from '@/components/Switch'
import type { WidgetConfig } from '@/types/widgets'
import { Select as CSelect, SelectTrigger, SelectContent, SelectItem as CSelectItem, SelectValue } from '@/components/Select'
import AlertDialog from '@/components/alerts/AlertDialog'
import { RiNotification3Line } from '@remixicon/react'

export type WidgetActionsMenuProps = {
  widgetId: string
  cfg: WidgetConfig
  anchorEl?: HTMLElement | null
  open: boolean
  onCloseAction: () => void
  parentDashboardId?: string | null
  parentPublicId?: string | null
  parentToken?: string | null
}

// Text criterion -> selects subset of provided values
function TextCriterionInput({ criterion, values, onApply }: { criterion: string; values: string[]; onApply: (vals: string[]) => void }) {
  const [q, setQ] = React.useState<string>('')
  const apply = () => {
    const base = (values || []).map(String)
    const s = q.trim().toLowerCase()
    let sel: string[] = []
    const includes = (v: string) => v.toLowerCase().includes(s)
    if (!s) { sel = base }
    else if (/^equals$/i.test(criterion)) sel = base.filter((v) => v.toLowerCase() === s)
    else if (/doesn'?t equal/i.test(criterion)) sel = base.filter((v) => v.toLowerCase() !== s)
    else if (/contains/i.test(criterion) && !/doesn'?t/i.test(criterion)) sel = base.filter(includes)
    else if (/doesn'?t.*contain/i.test(criterion)) sel = base.filter((v) => !includes(v))
    else if (/starts? with/i.test(criterion)) sel = base.filter((v) => v.toLowerCase().startsWith(s))
    else if (/ends? with/i.test(criterion)) sel = base.filter((v) => v.toLowerCase().endsWith(s))
    else sel = base
    onApply(sel)
  }
  return (
    <div className="space-y-2">
      <input type="text" className="w-full h-8 px-2 rounded-md border border-[hsl(var(--border))] bg-transparent" placeholder="Enter text" value={q} onChange={(e) => setQ(e.target.value)} />
      <div className="flex items-center gap-2">
        <button type="button" className="h-8 px-2 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={apply}>Apply</button>
      </div>
    </div>
  )
}

// Number criterion -> selects subset of provided values
function NumberCriterionInput({ criterion, values, onApply }: { criterion: string; values: string[]; onApply: (vals: string[]) => void }) {
  const [a, setA] = React.useState<string>('')
  const [b, setB] = React.useState<string>('')
  const apply = () => {
    const basePairs = (values || []).map((s) => ({ s, n: Number(s) })).filter((x) => Number.isFinite(x.n))
    const va = Number(a)
    const vb = Number(b)
    let sel: string[] = []
    if (/between/i.test(criterion)) {
      const lo = Math.min(va, vb)
      const hi = Math.max(va, vb)
      sel = basePairs.filter((x) => x.n >= lo && x.n <= hi).map((x) => x.s)
    } else if (/above or equal/i.test(criterion)) sel = basePairs.filter((x) => x.n >= va).map((x) => x.s)
    else if (/above/i.test(criterion)) sel = basePairs.filter((x) => x.n > va).map((x) => x.s)
    else if (/less than or equal/i.test(criterion)) sel = basePairs.filter((x) => x.n <= va).map((x) => x.s)
    else if (/less than/i.test(criterion)) sel = basePairs.filter((x) => x.n < va).map((x) => x.s)
    else if (/doesn'?t equal/i.test(criterion)) sel = basePairs.filter((x) => x.n !== va).map((x) => x.s)
    else /* equals */ sel = basePairs.filter((x) => x.n === va).map((x) => x.s)
    onApply(sel)
  }
  return (
    <div className="space-y-2">
      {(/between/i.test(criterion)) ? (
        <div className="grid grid-cols-2 gap-2">
          <input type="number" className="h-8 px-2 rounded-md border bg-transparent" placeholder="Min" value={a} onChange={(e) => setA(e.target.value)} />
          <input type="number" className="h-8 px-2 rounded-md border bg-transparent" placeholder="Max" value={b} onChange={(e) => setB(e.target.value)} />
        </div>
      ) : (
        <input type="number" className="w-full h-8 px-2 rounded-md border bg-transparent" placeholder="Enter number" value={a} onChange={(e) => setA(e.target.value)} />
      )}
      <div className="flex items-center gap-2">
        <button type="button" className="h-8 px-2 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={apply}>Apply</button>
      </div>
    </div>
  )
}

// Date criterion -> selects subset of provided values
function DateCriterionInput({ criterion, values, onApply }: { criterion: string; values: string[]; onApply: (vals: string[]) => void }) {
  const [a, setA] = React.useState<string>('')
  const [b, setB] = React.useState<string>('')
  const toYMD = (s: string): string => {
    if (!s) return ''
    const m = s.match(/^\d{4}-\d{2}-\d{2}/)
    if (m) return m[0]
    const d = new Date(s)
    if (isNaN(d.getTime())) return ''
    const y = d.getFullYear()
    const mo = String(d.getMonth() + 1).padStart(2, '0')
    const da = String(d.getDate()).padStart(2, '0')
    return `${y}-${mo}-${da}`
  }
  const apply = () => {
    const basePairs = (values || []).map((s) => ({ s, d: toYMD(s) })).filter((x) => !!x.d)
    const da = toYMD(a)
    const db = toYMD(b)
    let sel: string[] = []
    if (/between/i.test(criterion)) {
      const lo = da <= db ? da : db
      const hi = da <= db ? db : da
      sel = basePairs.filter((x) => x.d >= lo && x.d <= hi).map((x) => x.s)
    } else if (/before/i.test(criterion)) sel = basePairs.filter((x) => x.d < da).map((x) => x.s)
    else if (/after/i.test(criterion)) sel = basePairs.filter((x) => x.d > da).map((x) => x.s)
    else /* equals */ sel = basePairs.filter((x) => x.d === da).map((x) => x.s)
    onApply(sel)
  }
  return (
    <div className="space-y-2">
      {(/between/i.test(criterion)) ? (
        <div className="grid grid-cols-2 gap-2">
          <input type="date" className="h-8 px-2 rounded-md border bg-transparent" value={a} onChange={(e) => setA(e.target.value)} />
          <input type="date" className="h-8 px-2 rounded-md border bg-transparent" value={b} onChange={(e) => setB(e.target.value)} />
        </div>
      ) : (
        <input type="date" className="w-full h-8 px-2 rounded-md border bg-transparent" value={a} onChange={(e) => setA(e.target.value)} />
      )}
      <div className="flex items-center gap-2">
        <button type="button" className="h-8 px-2 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={apply}>Apply</button>
      </div>
    </div>
  )
}

// Basic outside click + Escape handling
function useDismiss(ref: React.RefObject<HTMLElement | null>, onClose: () => void, deps: any[] = []) {
  useEffect(() => {
    function onDocMouseDown(e: MouseEvent) {
      const el = ref.current
      if (!el) return
      if (e.target instanceof Node && !el.contains(e.target as Node)) onClose()
    }
    function onEsc(e: KeyboardEvent) { if (e.key === 'Escape') onClose() }
    if (typeof window !== 'undefined') {
      window.addEventListener('mousedown', onDocMouseDown)
      window.addEventListener('keydown', onEsc)
    }
    return () => {
      if (typeof window !== 'undefined') {
        window.removeEventListener('mousedown', onDocMouseDown)
        window.removeEventListener('keydown', onEsc)
      }
    }
  }, deps) // eslint-disable-line react-hooks/exhaustive-deps
}

// Simple menu/submenu implementation rendered into a Portal near anchor rect, to avoid overflow clipping
export default function WidgetActionsMenu({ widgetId, cfg, anchorEl, open, onCloseAction, parentDashboardId, parentPublicId, parentToken }: WidgetActionsMenuProps) {
  const ref = useRef<HTMLDivElement | null>(null)
  useDismiss(ref, onCloseAction, [open])
  // Single-pane navigation path (root -> child -> grandchild)
  const [nav, setNav] = useState<string[]>([])
  useEffect(() => { if (!open) setNav([]) }, [open])
  // Signal to the page that actions menu is open (to gate configurator hover)
  useEffect(() => {
    if (typeof document === 'undefined') return
    if (open) document.body.dataset.actionsMenuOpen = '1'
    return () => { try { delete (document.body as any).dataset.actionsMenuOpen } catch {} }
  }, [open])
  const [alertsOpen, setAlertsOpen] = useState<boolean>(false)
  const [existingAlert, setExistingAlert] = useState<AlertOut | null>(null)
  useEffect(() => {
    let cancelled = false
    async function run() {
      try {
        if (!open) { setExistingAlert(null); return }
        const all = await Api.listAlerts()
        const found = (Array.isArray(all) ? all : []).find((a) => String(a?.widgetId || '') === String(cfg.id)) || null
        if (!cancelled) setExistingAlert(found)
      } catch { if (!cancelled) setExistingAlert(null) }
    }
    void run(); return () => { cancelled = true }
  }, [open, cfg.id])
  // Ensure Tremor Select popovers render above this menu
  useEffect(() => {
    if (typeof document === 'undefined') return
    const id = 'wa-menu-popover-zfix'
    if (!document.getElementById(id)) {
      const style = document.createElement('style')
      style.id = id
      style.innerHTML = `
        /* Try common Tremor / HeadlessUI popover/listbox containers */
        .tremor-Select-popover, .tremor-base__Select-popover, [role='listbox'] { z-index: 1100 !important; }
      `
      document.head.appendChild(style)
    }
  }, [])

  // Helper: emit an options patch for this widget
  const emitOptions = (opts: Record<string, any>) => {
    if (typeof window === 'undefined') return
    window.dispatchEvent(new CustomEvent('widget-config-patch', { detail: { widgetId, patch: { options: opts } } }))
  }
  // Helper: emit an arbitrary widget patch (for querySpec etc.)
  const emitPatch = (patch: Partial<WidgetConfig>) => {
    if (typeof window === 'undefined') return
    window.dispatchEvent(new CustomEvent('widget-config-patch', { detail: { widgetId, patch } }))
  }
  // Merge into options.dataDefaults safely
  const updateDataDefaults = (patch: Partial<NonNullable<WidgetConfig['options']>['dataDefaults']>) => {
    const prev = (cfg?.options?.dataDefaults || {}) as any
    emitOptions({ dataDefaults: { ...prev, ...(patch || {}) } })
  }
  const opt = (k: string, def?: any) => (cfg?.options as any)?.[k] ?? def
  const optChartGrid = () => ((cfg?.options as any)?.chartGrid || {}) as any
  const updateChartGrid = (axis: 'x'|'y', show: boolean) => {
    const grid = optChartGrid()
    const nodeKey = axis === 'x' ? 'vertical' : 'horizontal'
    const node = grid[nodeKey] || {}
    const main = node.main || {}
    const next = {
      ...grid,
      [nodeKey]: {
        ...(grid[nodeKey] || {}),
        main: { ...main, mode: 'custom', show },
      },
    }
    emitOptions({ chartGrid: next })
  }

  // Deep patch utility for chartGrid paths e.g., ('horizontal','main')
  const patchGridNode = (axis: 'horizontal' | 'vertical', node: 'main' | 'secondary', patch: Record<string, any>) => {
    const prev = optChartGrid()
    const next = {
      ...prev,
      [axis]: {
        ...(prev[axis] || {}),
        [node]: { ...((prev as any)[axis]?.[node] || {}), ...(patch || {}) },
      },
    }
    emitOptions({ chartGrid: next })
  }

  const rect = useMemo(() => {
    if (!anchorEl) return null
    const r = anchorEl.getBoundingClientRect()
    return { x: r.left, y: r.bottom, w: r.width, h: r.height }
  }, [anchorEl, open])

  const rootLeft = useMemo(() => {
    if (!rect) return 0
    const approxRootWidth = 280
    const vw = (typeof window !== 'undefined') ? window.innerWidth : 1024
    return Math.max(8, Math.min(rect.x, vw - approxRootWidth - 8))
  }, [rect])

  // Draggable menu position (defaults to anchored position; persists while open)
  const [menuPos, setMenuPos] = useState<{ x: number; y: number } | null>(null)
  const [menuDragging, setMenuDragging] = useState(false)
  useEffect(() => {
    if (open) {
      setMenuPos((p) => p ?? { x: rootLeft, y: ((rect?.y || 0) + 6) })
    } else {
      setMenuPos(null)
      setMenuDragging(false)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, rootLeft, rect?.y])

  const startHeaderDrag = (e: React.PointerEvent<HTMLDivElement>) => {
    // Allow clicking Back button without initiating drag
    const targetEl = e.target as HTMLElement
    if (targetEl && targetEl.closest('button')) return
    e.preventDefault()
    e.stopPropagation()
    const startX = e.clientX
    const startY = e.clientY
    const origin = { x: (menuPos?.x ?? rootLeft), y: (menuPos?.y ?? ((rect?.y || 0) + 6)) }
    setMenuDragging(true)
    const bounds = (() => {
      const vw = window.visualViewport?.width || window.innerWidth
      const vh = window.visualViewport?.height || window.innerHeight
      const r = ref.current?.getBoundingClientRect()
      const w = r?.width ?? 300
      const h = r?.height ?? 280
      const minX = 8, minY = 8
      const maxX = Math.max(minX, vw - w - 8)
      const maxY = Math.max(minY, vh - 8)
      return { minX, minY, maxX, maxY }
    })()
    const onMove = (ev: PointerEvent) => {
      const dx = ev.clientX - startX
      const dy = ev.clientY - startY
      const nx = Math.min(Math.max(origin.x + dx, bounds.minX), bounds.maxX)
      const ny = Math.min(Math.max(origin.y + dy, bounds.minY), bounds.maxY)
      setMenuPos({ x: nx, y: ny })
    }
    const onUp = () => {
      setMenuDragging(false)
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
    }
    window.addEventListener('pointermove', onMove)
    window.addEventListener('pointerup', onUp, { once: true })
  }

  // Pane builders
  type Pane = { title: string; content: React.ReactNode }
  const formatTargets = ['X Axis', 'Y Axis (Values)', 'Categories'] as const
  // Derive fields from QuerySpec
  const spec: any = (cfg as any)?.querySpec || {}
  const seriesSpecs: Array<any> = Array.isArray(spec?.series) ? (spec.series as any[]) : []
  const xField: string | undefined = Array.isArray(spec?.x) ? (spec.x?.[0] as any) : (spec?.x as any)
  const legendArr: string[] = Array.isArray(spec?.legend) ? (spec.legend as any[]).filter(Boolean) : (spec?.legend ? [spec.legend] : [])
  const yField: string | undefined = (spec?.y as any)
  const measure: string | undefined = (spec?.measure as any)
  const groupBy: string | undefined = (spec?.groupBy as any)
  const isDateLike = (name?: string): boolean => {
    const s = String(name || '')
    if (!s) return false
    return /(date|time|timestamp|created|updated|_at)$/i.test(s)
  }
  const isDateX = !!(xField && ((groupBy && groupBy !== 'none') || isDateLike(xField)))
  const filterableFields = useMemo(() => {
    const set = new Map<string, 'text'|'number'|'date'>()
    if (xField) set.set(String(xField), isDateX ? 'date' : 'text')
    legendArr.forEach((l) => { if (l) set.set(String(l), 'text') })
    if (yField) set.set(String(yField), 'number')
    seriesSpecs.forEach((s: any) => { const n = (s?.y as any) || (s?.measure as any); if (n) set.set(String(n), 'number') })
    if (measure) set.set(String(measure), 'number')
    return Array.from(set.entries()).map(([name, kind]) => ({ name, kind }))
  }, [JSON.stringify(spec)])
  // Sort groups built from presence
  const sortGroupsDyn = useMemo(() => ([
    ...(legendArr.length > 0 ? [{ key: 'strings', label: 'Sort Strings', fields: legendArr, choices: ['A → Z', 'Z → A'] }] : []),
    { key: 'values', label: 'Sort Values', fields: [yField || measure || 'value'].filter(Boolean) as string[], choices: ['Smallest → Largest', 'Largest → Smallest'] },
    ...(xField ? [{ key: 'dates', label: 'Sort Dates', fields: [xField], choices: ['Oldest → Latest', 'Latest → Oldest'] }] : []),
  ] as const), [JSON.stringify(spec)])
  const seriesList = useMemo(() => {
    if (seriesSpecs.length > 0) return seriesSpecs.map((s: any, i: number) => (s?.label || s?.y || s?.measure || `Series ${i+1}`))
    return ['Value']
  }, [JSON.stringify(seriesSpecs)])

  // Distinct values cache for Filter pane
  const [distinctCache, setDistinctCache] = useState<Record<string, string[]>>({})
  const [distinctLoading, setDistinctLoading] = useState<Record<string, boolean>>({})
  const [distinctSearch, setDistinctSearch] = useState<Record<string, string>>({})
  const [distinctInitApplied, setDistinctInitApplied] = useState<Record<string, boolean>>({})
  const loadDistinct = async (field: string) => {
    try {
      if (!((cfg as any)?.querySpec?.source)) return
      setDistinctLoading((p) => ({ ...p, [field]: true }))
      const source = (cfg as any)?.querySpec?.source as string
      const baseWhere = (((cfg as any)?.querySpec?.where) || {}) as Record<string, any>
      const where: Record<string, any> = { ...baseWhere }
      delete where[field]
      const spec: any = { source, select: [field], where: Object.keys(where).length ? where : undefined, limit: 1000, offset: 0 }
      const res = await QueryApi.querySpec({ spec, datasourceId: (cfg as any)?.datasourceId, limit: 1000, offset: 0, includeTotal: false })
      const cols = (res.columns || []) as string[]
      const idx = Math.max(0, cols.indexOf(field))
      const setVals = new Set<string>()
      ;(res.rows || []).forEach((row: any) => { const arr = Array.isArray(row) ? row : []; const v = arr[idx]; if (v != null) setVals.add(String(v)) })
      const arr = Array.from(setVals.values()).sort()
      setDistinctCache((prev) => ({ ...prev, [field]: arr }))
      // Default state: select all values if nothing set yet
      const prevWhere = (((cfg as any)?.querySpec?.where) || {}) as Record<string, any>
      if (!distinctInitApplied[field] && (prevWhere[field] == null)) {
        const nextWhere: Record<string, any> = { ...prevWhere, [field]: arr }
        emitPatch({ querySpec: ({ ...((cfg as any)?.querySpec || {}), where: nextWhere } as any) })
        setDistinctInitApplied((p) => ({ ...p, [field]: true }))
      }
    } catch {
      setDistinctCache((prev) => ({ ...prev, [field]: [] }))
    } finally {
      setDistinctLoading((p) => ({ ...p, [field]: false }))
    }
  }

  // Auto-load distincts when entering a field filter pane
  useEffect(() => {
    if (nav[0] === 'filter' && nav.length >= 2) {
      const f = nav[1]
      if (!distinctCache[f] && !distinctLoading[f]) { void loadDistinct(f) }
    }
  }, [nav.join('|')])

  // Determine widget type and available features
  const widgetType = cfg?.type || 'chart'
  const chartType = (cfg as any)?.chartType || 'line'
  
  // Feature availability by widget type
  // Widget types: chart, table, tremorTable, kpi, text, etc.
  // Chart types: line, bar, area, column, donut, badges, progress, tracker, scatter, etc.
  const hasFilter = ['chart', 'table', 'tremorTable'].includes(widgetType)
  const hasSort = ['chart', 'table', 'tremorTable'].includes(widgetType)
  const hasFormat = true // All widgets support formatting
  const hasGrid = ['chart'].includes(widgetType) && !['donut', 'badges', 'progress', 'tracker'].includes(chartType) // Charts with axes
  const hasAxis = ['chart'].includes(widgetType) && !['donut', 'badges', 'progress', 'tracker'].includes(chartType) // Charts with axes
  const hasTooltip = ['chart', 'table'].includes(widgetType)
  const hasLegend = ['chart'].includes(widgetType) && !['badges', 'progress', 'tracker'].includes(chartType) // Charts with series
  const hasAlerts = true // All widgets can have alerts

  const makeRootPane = (): Pane => ({
    title: 'Actions',
    content: (
      <MenuList>
        {hasFilter && <MenuItem label="Filter" icon={<RiFilter3Line className="h-4 w-4" aria-hidden="true" />} hasSubmenu onClick={() => setNav(['filter'])} />}
        {hasSort && <MenuItem label="Sort" icon={<RiArrowUpDownLine className="h-4 w-4" aria-hidden="true" />} hasSubmenu onClick={() => setNav(['sort'])} />}
        {hasFormat && <MenuItem label="Format" icon={<RiBrushLine className="h-4 w-4" aria-hidden="true" />} hasSubmenu onClick={() => setNav(['format'])} />}
        {hasGrid && <MenuItem label="Grid" icon={<RiGridLine className="h-4 w-4" aria-hidden="true" />} hasSubmenu onClick={() => setNav(['grid'])} />}
        {hasAxis && <MenuItem label="Axis" icon={<RiRulerLine className="h-4 w-4" aria-hidden="true" />} hasSubmenu onClick={() => setNav(['axis'])} />}
        {hasTooltip && <MenuItem label="Tooltip" icon={<RiChat3Line className="h-4 w-4" aria-hidden="true" />} hasSubmenu onClick={() => setNav(['tooltip'])} />}
        {hasLegend && <MenuItem label="Legend" icon={<RiPriceTag3Line className="h-4 w-4" aria-hidden="true" />} hasSubmenu onClick={() => setNav(['legend'])} />}
        {hasAlerts && <MenuItem label={existingAlert ? (`Edit ${existingAlert.kind === 'notification' ? 'Notification' : 'Alert'}`) : 'Set Notification/Alert'} icon={<RiNotification3Line className="h-4 w-4" aria-hidden="true" />} onClick={() => setAlertsOpen(true)} />}
      </MenuList>
    )
  })

  const makeFilterPane = (p: string[]): Pane => {
    if (p.length === 1) {
      return {
        title: 'Filter',
        content: (
          <MenuList>
            {filterableFields.map((f) => (
              <MenuItem key={f.name} label={`${f.name} (${f.kind})`} hasSubmenu onClick={() => setNav(['filter', f.name])} />
            ))}
          </MenuList>
        )
      }
    }
    // Field details: criteria + Values list (distinct) with search
    const field = p[1]
    const kind = (() => (filterableFields.find((ff) => ff.name === field)?.kind || 'text'))() as 'text'|'number'|'date'
    const isText = kind === 'text'
    const isNum = kind === 'number'
    const criteria = isText
      ? ['equals', "doesn't equal", 'contains', "doesn't contain", 'starts with', 'ends with']
      : isNum
      ? ['equals', "doesn't equal", 'above', 'above or equal', 'less than', 'less than or equal', 'between']
      : ['equals', 'before', 'after', 'between']
    // Criteria input path
    if (p.length >= 3) {
      const crit = p[2]
      return {
        title: `${field} • ${crit}`,
        content: (
          <MenuList>
            <MenuGroup title="Enter value(s)">
              <div className="px-2 py-1 space-y-2">
                {(() => {
                  const vals = distinctCache[field] || []
                  const apply = (selected: string[]) => {
                    const prevWhere = (((cfg as any)?.querySpec?.where) || {}) as Record<string, any>
                    const nextWhere: Record<string, any> = { ...prevWhere }
                    if (!selected || selected.length === 0) delete nextWhere[field]; else nextWhere[field] = selected
                    emitPatch({ querySpec: ({ ...((cfg as any)?.querySpec || {}), where: nextWhere } as any) })
                    setNav(['filter', field])
                  }
                  if (isText) {
                    return (
                      <TextCriterionInput criterion={crit} values={vals} onApply={apply} />
                    )
                  }
                  if (isNum) {
                    return (
                      <NumberCriterionInput criterion={crit} values={vals} onApply={apply} />
                    )
                  }
                  return (
                    <DateCriterionInput criterion={crit} values={vals} onApply={apply} />
                  )
                })()}
              </div>
            </MenuGroup>
          </MenuList>
        )
      }
    }
    return {
      title: `${field} • Filter`,
      content: (
        <MenuList>
          <MenuGroup title="Criteria">
            {criteria.map((c: string) => (
              <MenuItem key={c} label={c} onClick={() => {
                // Navigate to input pane for this criterion
                setNav(['filter', field, c])
              }} />
            ))}
          </MenuGroup>
          <MenuGroup title="Values">
            <div className="p-2 pt-0">
              <input
                type="text"
                className="w-full h-8 px-2 rounded-md border border-[hsl(var(--border))] bg-transparent text-[12px]"
                placeholder="Search values"
                value={distinctSearch[field] || ''}
                onChange={(e) => {
                  const q = e.target.value
                  setDistinctSearch((prev) => ({ ...prev, [field]: q }))
                  // Search results are always selected
                  const base = distinctCache[field] || []
                  const matches = (q.trim().length === 0) ? base : base.filter((v) => String(v).toLowerCase().includes(q.trim().toLowerCase()))
                  const prevWhere = (((cfg as any)?.querySpec?.where) || {}) as Record<string, any>
                  const nextWhere: Record<string, any> = { ...prevWhere }
                  if (matches.length === 0) delete nextWhere[field]; else nextWhere[field] = matches
                  emitPatch({ querySpec: ({ ...((cfg as any)?.querySpec || {}), where: nextWhere } as any) })
                }}
              />
            </div>
            <div className="px-2 pb-2 flex items-center gap-2">
              <button type="button" className="text-[11px] px-2 py-0.5 rounded border" onClick={() => {
                const all = distinctCache[field] || []
                const prevWhere = (((cfg as any)?.querySpec?.where) || {}) as Record<string, any>
                const nextWhere: Record<string, any> = { ...prevWhere }
                if (all.length === 0) delete nextWhere[field]; else nextWhere[field] = all
                emitPatch({ querySpec: ({ ...((cfg as any)?.querySpec || {}), where: nextWhere } as any) })
              }}>Select all</button>
              <button type="button" className="text-[11px] px-2 py-0.5 rounded border" onClick={() => {
                const prevWhere = (((cfg as any)?.querySpec?.where) || {}) as Record<string, any>
                const nextWhere: Record<string, any> = { ...prevWhere }; delete nextWhere[field]
                emitPatch({ querySpec: ({ ...((cfg as any)?.querySpec || {}), where: nextWhere } as any) })
              }}>Clear</button>
            </div>
            <div className="max-h-48 overflow-auto px-2 pb-2 space-y-1">
              {(() => {
                const currentRaw = (((cfg as any)?.querySpec?.where || {}) as any)[field]
                const current: string[] = Array.isArray(currentRaw) ? currentRaw.map(String) : (currentRaw != null ? [String(currentRaw)] : [])
                const all = distinctCache[field]
                if (!all) { return <div className="text-xs text-muted-foreground px-1">Loading values…</div> }
                const q = (distinctSearch[field] || '').trim().toLowerCase()
                const vals = q ? all.filter((v) => v.toLowerCase().includes(q)) : all
                if (vals.length === 0) { return <div className="text-xs text-muted-foreground px-1">No values.</div> }
                const toggle = (v: string) => {
                  const exists = current.includes(v)
                  const next = exists ? current.filter((x) => x !== v) : [...current, v]
                  const prevWhere = (((cfg as any)?.querySpec?.where) || {}) as Record<string, any>
                  const nextWhere: Record<string, any> = { ...prevWhere }
                  if (next.length === 0) delete nextWhere[field]; else nextWhere[field] = next
                  emitPatch({ querySpec: ({ ...((cfg as any)?.querySpec || {}), where: nextWhere } as any) })
                }
                return vals.map((v) => (
                  <label key={v} className="flex items-center gap-2 text-xs">
                    <input type="checkbox" className="h-3 w-3 accent-[hsl(var(--primary))]" checked={current.includes(v)} onChange={() => toggle(v)} />
                    <span className="truncate">{v}</span>
                  </label>
                ))
              })()}
            </div>
          </MenuGroup>
        </MenuList>
      )
    }
  }

  const makeSortPane = (p: string[]): Pane => {
    if (p.length === 1) {
      return {
        title: 'Sort',
        content: (
          <MenuList>
            {sortGroupsDyn.map((g) => (
              <MenuItem key={g.key as any} label={g.label as any} hasSubmenu onClick={() => setNav(['sort', g.key as any])} />
            ))}
            <MenuGroup title="Data Filtering">
              <SwitchRow label="Exclude zero values" checked={opt('excludeZeroValues', false)} onChangeAction={(v) => emitOptions({ excludeZeroValues: v })} />
            </MenuGroup>
          </MenuList>
        )
      }
    }
    const grp = (sortGroupsDyn as any).find((g: any) => g.key === p[1])
    if (p.length === 2 && grp) {
      return {
        title: grp.label,
        content: (
          <MenuList>
            {(grp.fields as string[]).map((f: string) => (
              <MenuItem key={f} label={f} hasSubmenu onClick={() => setNav(['sort', grp.key, f])} />
            ))}
          </MenuList>
        )
      }
    }
    if (p.length === 3 && grp) {
      const field = p[2]
      return {
        title: `${field} • ${grp.label}`,
        content: (
          <MenuList>
            {grp.choices.map((c: string) => (
              <MenuItem
                key={c}
                label={c}
                onClick={() => {
                  // Map groups/choices to dataDefaults.sort
                  const by: 'x' | 'value' = grp.key === 'values' ? 'value' : 'x'
                  const direction: 'asc' | 'desc' = (/A → Z|Smallest|Oldest/.test(c)) ? 'asc' : 'desc'
                  updateDataDefaults({ sort: { by, direction } })
                }}
              />
            ))}
          </MenuList>
        )
      }
    }
    return { title: 'Sort', content: null }
  }

  const makeFormatPane = (p: string[]): Pane => {
    if (p.length === 1) {
      return {
        title: 'Format',
        content: (
          <MenuList>
            {formatTargets.map((t) => (
              <MenuItem key={t} label={t} hasSubmenu onClick={() => setNav(['format', t])} />
            ))}
          </MenuList>
        )
      }
    }
    const sec = p[1]
    if (sec === 'Y Axis (Values)') {
      if (p.length === 2) {
        return {
          title: 'Y Axis (Values)',
          content: (
            <MenuList>
              {seriesList.map((s) => (<MenuItem key={s} label={s} hasSubmenu onClick={() => setNav(['format', sec, s])} />))}
            </MenuList>
          )
        }
      }
      const s = p[2]
      return {
        title: `${s} • Format`,
        content: (
          <MenuList>
            {[
              'Format: none','Format: short','Format: abbrev','Format: currency','Format: percent','Format: bytes','Format: wholeNumber','Format: number','Format: thousands','Format: millions','Format: billions','Format: oneDecimal','Format: twoDecimals','Format: percentWhole','Format: percentOneDecimal','Format: timeHours','Format: timeMinutes','Format: distance-km','Format: distance-mi',
              'Label casing: lowercase','Label casing: capitalize','Label casing: uppercase','Label casing: capitalcase','Label casing: proper'
            ].map((label) => (
              <MenuItem
                key={label}
                label={label}
                onClick={() => {
                  if (label.startsWith('Format: ')) {
                    const v = label.replace('Format: ', '')
                    emitOptions({ yAxisFormat: v })
                  } else if (label.startsWith('Label casing: ')) {
                    const v = label.replace('Label casing: ', '') as any
                    emitOptions({ legendLabelCase: v })
                  }
                }}
              />
            ))}
          </MenuList>
        )
      }
    }
    // X Axis / Categories
    return {
      title: sec,
      content: (
        <MenuList>
          <MenuGroup title="Label casing">
            <div className="px-2 py-1">
              {sec === 'X Axis' ? (
                <FieldSelect value={opt('xLabelCase', '')} onValueChangeAction={(v: string) => emitOptions({ xLabelCase: v || undefined })}>
                  <CSelectItem value="lowercase">lowercase</CSelectItem>
                  <CSelectItem value="capitalize">capitalize</CSelectItem>
                  <CSelectItem value="uppercase">uppercase</CSelectItem>
                  <CSelectItem value="capitalcase">capitalcase</CSelectItem>
                  <CSelectItem value="proper">proper</CSelectItem>
                </FieldSelect>
              ) : (
                <FieldSelect value={opt('categoryLabelCase', '')} onValueChangeAction={(v: string) => emitOptions({ categoryLabelCase: v || undefined })}>
                  <CSelectItem value="lowercase">lowercase</CSelectItem>
                  <CSelectItem value="capitalize">capitalize</CSelectItem>
                  <CSelectItem value="uppercase">uppercase</CSelectItem>
                  <CSelectItem value="capitalcase">capitalcase</CSelectItem>
                  <CSelectItem value="proper">proper</CSelectItem>
                </FieldSelect>
              )}
            </div>
          </MenuGroup>
        </MenuList>
      )
    }
  }

  const makeLegendPane = (): Pane => ({
    title: 'Legend',
    content: (
      <MenuList>
        <MenuGroup title="Display">
          <SwitchRow label="Show legend" checked={opt('showLegend', true)} onChangeAction={(v) => emitOptions({ showLegend: v })} />
        </MenuGroup>
        <MenuGroup title="Position">
          <div className="px-2 py-1">
            <FieldSelect value={opt('legendPosition', '')} onValueChangeAction={(v: string) => emitOptions({ legendPosition: v || undefined })}>
              <CSelectItem value="top">Top</CSelectItem>
              <CSelectItem value="bottom">Bottom</CSelectItem>
              <CSelectItem value="none">None</CSelectItem>
            </FieldSelect>
          </div>
        </MenuGroup>
        <MenuGroup title="Style">
          <div className="grid grid-cols-2 gap-2 px-2 py-1">
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Mode</div>
              <FieldSelect value={opt('legendMode', '')} onValueChangeAction={(v: string) => emitOptions({ legendMode: v || undefined })}>
                <CSelectItem value="flat">flat</CSelectItem>
                <CSelectItem value="nested">nested</CSelectItem>
              </FieldSelect>
            </div>
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Dot shape</div>
              <FieldSelect value={opt('legendDotShape', '')} onValueChangeAction={(v: string) => emitOptions({ legendDotShape: v || undefined })}>
                <CSelectItem value="circle">circle</CSelectItem>
                <CSelectItem value="square">square</CSelectItem>
                <CSelectItem value="rect">rect</CSelectItem>
              </FieldSelect>
            </div>
          </div>
        </MenuGroup>
        <MenuGroup title="Labels">
          <div className="px-2 py-1">
            <FieldSelect value={opt('legendLabelCase', '')} onValueChangeAction={(v: string) => emitOptions({ legendLabelCase: v || undefined })}>
              <CSelectItem value="lowercase">lowercase</CSelectItem>
              <CSelectItem value="capitalize">capitalize</CSelectItem>
              <CSelectItem value="uppercase">uppercase</CSelectItem>
              <CSelectItem value="capitalcase">capitalcase</CSelectItem>
              <CSelectItem value="proper">proper</CSelectItem>
            </FieldSelect>
          </div>
        </MenuGroup>
        <MenuGroup title="Limits">
          <div className="px-2 py-1">
            <input
              type="number"
              className="w-full h-8 px-2 rounded-lg border border-[hsl(var(--border))] bg-transparent"
              value={String(opt('maxLegendItems', ''))}
              onChange={(e) => emitOptions({ maxLegendItems: e.target.value ? Number(e.target.value) : undefined })}
              placeholder="Max legend items"
            />
          </div>
        </MenuGroup>
      </MenuList>
    )
  })

  const makeAxisPane = (): Pane => ({
    title: 'Axis',
    content: (
      <MenuList>
        <MenuGroup title="X Axis">
          <div className="grid grid-cols-2 gap-2 px-2 py-1">
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Tick angle</div>
              <FieldSelect value={String(opt('xTickAngle', ''))} onValueChangeAction={(v: string) => emitOptions({ xTickAngle: v ? Number(v) : undefined })}>
                {[0,30,45,60,90].map((v)=> (<CSelectItem key={v} value={String(v)}>{v}°</CSelectItem>))}
              </FieldSelect>
            </div>
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Tick count</div>
              <FieldSelect value={String(opt('xTickCount', ''))} onValueChangeAction={(v: string) => emitOptions({ xTickCount: v ? Number(v) : undefined })}>
                {[4,6,8,10].map((v)=> (<CSelectItem key={v} value={String(v)}>{v}</CSelectItem>))}
              </FieldSelect>
            </div>
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Label format</div>
              <FieldSelect value={opt('xLabelFormat', '')} onValueChangeAction={(v: string) => emitOptions({ xLabelFormat: v || undefined })}>
                {['none','short','datetime'].map((v)=> (<CSelectItem key={v} value={v}>{v}</CSelectItem>))}
              </FieldSelect>
            </div>
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Week start</div>
              <FieldSelect value={opt('xWeekStart', '')} onValueChangeAction={(v: string) => emitOptions({ xWeekStart: v || undefined })}>
                {['mon','sun'].map((v)=> (<CSelectItem key={v} value={v}>{v}</CSelectItem>))}
              </FieldSelect>
            </div>
          </div>
        </MenuGroup>
        <MenuGroup title="Y Axis">
          <div className="grid grid-cols-2 gap-2 px-2 py-1">
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Tick count</div>
              <FieldSelect value={String(opt('yTickCount', ''))} onValueChangeAction={(v: string) => emitOptions({ yTickCount: v ? Number(v) : undefined })}>
                {[4,6,8,10].map((v)=> (<CSelectItem key={v} value={String(v)}>{v}</CSelectItem>))}
              </FieldSelect>
            </div>
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Scale</div>
              <FieldSelect value={String((cfg?.yAxis?.scale || ''))} onValueChangeAction={(v: string) => emitPatch({ yAxis: { ...(cfg?.yAxis || {}), scale: (v || undefined) as any } })}>
                {['linear','log'].map((v)=> (<CSelectItem key={v} value={v}>{v}</CSelectItem>))}
              </FieldSelect>
            </div>
          </div>
        </MenuGroup>
        <MenuGroup title="Fonts">
          <div className="grid grid-cols-2 gap-2 px-2 py-1">
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">X font size</div>
              <FieldSelect value={String(opt('xAxisFontSize', ''))} onValueChangeAction={(v: string) => emitOptions({ xAxisFontSize: v ? Number(v) : undefined })}>
                {[12,14,16].map((v)=> (<CSelectItem key={v} value={String(v)}>{v}</CSelectItem>))}
              </FieldSelect>
            </div>
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">X font weight</div>
              <FieldSelect value={String(opt('xAxisFontWeight', ''))} onValueChangeAction={(v: string) => emitOptions({ xAxisFontWeight: (v || undefined) as any })}>
                {['normal','bold'].map((v)=> (<CSelectItem key={v} value={v}>{v}</CSelectItem>))}
              </FieldSelect>
            </div>
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Y font size</div>
              <FieldSelect value={String(opt('yAxisFontSize', ''))} onValueChangeAction={(v: string) => emitOptions({ yAxisFontSize: v ? Number(v) : undefined })}>
                {[12,14,16].map((v)=> (<CSelectItem key={v} value={String(v)}>{v}</CSelectItem>))}
              </FieldSelect>
            </div>
            <div>
              <div className="text-[11px] text-muted-foreground mb-1">Y font weight</div>
              <FieldSelect value={String(opt('yAxisFontWeight', ''))} onValueChangeAction={(v: string) => emitOptions({ yAxisFontWeight: (v || undefined) as any })}>
                {['normal','bold'].map((v)=> (<CSelectItem key={v} value={v}>{v}</CSelectItem>))}
              </FieldSelect>
            </div>
          </div>
        </MenuGroup>
      </MenuList>
    )
  })

  const makeTooltipPane = (): Pane => ({
    title: 'Tooltip',
    content: (
      <MenuList>
        <MenuGroup title="Display">
          <SwitchRow label="Show tooltip" checked={opt('showTooltip', true)} onChangeAction={(v) => emitOptions({ showTooltip: v })} />
          <SwitchRow label="Hide zeros" checked={opt('tooltipHideZeros', false)} onChangeAction={(v) => emitOptions({ tooltipHideZeros: v })} />
          <SwitchRow label="Rich tooltip" checked={opt('richTooltip', true)} onChangeAction={(v) => emitOptions({ richTooltip: v })} />
        </MenuGroup>
        <MenuGroup title="Mode">
          <div className="px-2 py-1">
            <FieldSelect value={opt('tooltipMode', '')} onValueChangeAction={(v: string) => { emitOptions({ tooltipMode: v || undefined, tooltipTrigger: (v === 'single' ? 'item' : v ? 'axis' : undefined) }) }}>
              <CSelectItem value="shared">shared</CSelectItem>
              <CSelectItem value="single">single</CSelectItem>
            </FieldSelect>
          </div>
        </MenuGroup>
        <MenuGroup title="Content">
          <SwitchRow label="Show percent" checked={opt('tooltipShowPercent', false)} onChangeAction={(v) => emitOptions({ tooltipShowPercent: v })} />
          <SwitchRow label="Show delta" checked={opt('tooltipShowDelta', false)} onChangeAction={(v) => emitOptions({ tooltipShowDelta: v })} />
        </MenuGroup>
        <MenuGroup title="Color preset">
          <div className="px-2 py-1">
            <FieldSelect value={opt('colorPreset', '')} onValueChangeAction={(v: string) => emitOptions({ colorPreset: v || undefined })}>
              <CSelectItem value="default">default</CSelectItem>
              <CSelectItem value="muted">muted</CSelectItem>
              <CSelectItem value="vibrant">vibrant</CSelectItem>
              <CSelectItem value="corporate">corporate</CSelectItem>
            </FieldSelect>
          </div>
        </MenuGroup>
      </MenuList>
    )
  })

  const makeGridPane = (): Pane => ({
    title: 'Grid',
    content: (
      <MenuList>
        <MenuGroup title="Horizontal (Y axis grid)">
          <div className="px-2 py-1">
            <div className="text-[11px] font-medium text-muted-foreground mb-1">Main</div>
            <div className="grid grid-cols-2 gap-2 items-center mb-2">
              <label className="text-xs text-muted-foreground">Mode</label>
              <FieldSelect value={String(((optChartGrid().horizontal || {}).main || {}).mode || 'default')} onValueChangeAction={(v: string) => patchGridNode('horizontal','main',{ mode: (v === 'custom' ? 'custom' : 'default') })}>
                <CSelectItem value="default">default</CSelectItem>
                <CSelectItem value="custom">custom</CSelectItem>
              </FieldSelect>
            </div>
            {String(((optChartGrid().horizontal || {}).main || {}).mode || 'default') === 'custom' && (
              <div className="grid grid-cols-2 gap-2 items-center border rounded-md p-2">
                <label className="text-xs text-muted-foreground">Show</label>
                <input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={!!(((optChartGrid().horizontal || {}).main || {}).show)} onChange={(e) => patchGridNode('horizontal','main',{ show: e.target.checked })} />
                <label className="text-xs text-muted-foreground">Type</label>
                <FieldSelect value={String((((optChartGrid().horizontal || {}).main || {}).type || 'solid'))} onValueChangeAction={(v: string) => patchGridNode('horizontal','main',{ type: v })}>
                  {['solid','dashed','dotted'].map((t) => (<CSelectItem key={t} value={t}>{t}</CSelectItem>))}
                </FieldSelect>
                <label className="text-xs text-muted-foreground">Width</label>
                <FieldSelect value={String((((optChartGrid().horizontal || {}).main || {}).width || '1'))} onValueChangeAction={(v: string) => patchGridNode('horizontal','main',{ width: Number(v) })}>
                  {['1','2','3'].map((t) => (<CSelectItem key={t} value={t}>{t}px</CSelectItem>))}
                </FieldSelect>
              </div>
            )}
          </div>
        </MenuGroup>
        <MenuGroup title="Vertical (X axis grid)">
          <div className="px-2 py-1">
            <div className="text-[11px] font-medium text-muted-foreground mb-1">Main</div>
            <div className="grid grid-cols-2 gap-2 items-center mb-2">
              <label className="text-xs text-muted-foreground">Mode</label>
              <FieldSelect value={String(((optChartGrid().vertical || {}).main || {}).mode || 'default')} onValueChangeAction={(v: string) => patchGridNode('vertical','main',{ mode: (v === 'custom' ? 'custom' : 'default') })}>
                <CSelectItem value="default">default</CSelectItem>
                <CSelectItem value="custom">custom</CSelectItem>
              </FieldSelect>
            </div>
            {String(((optChartGrid().vertical || {}).main || {}).mode || 'default') === 'custom' && (
              <div className="grid grid-cols-2 gap-2 items-center border rounded-md p-2">
                <label className="text-xs text-muted-foreground">Show</label>
                <input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={!!(((optChartGrid().vertical || {}).main || {}).show)} onChange={(e) => patchGridNode('vertical','main',{ show: e.target.checked })} />
                <label className="text-xs text-muted-foreground">Type</label>
                <FieldSelect value={String((((optChartGrid().vertical || {}).main || {}).type || 'solid'))} onValueChangeAction={(v: string) => patchGridNode('vertical','main',{ type: v })}>
                  {['solid','dashed','dotted'].map((t) => (<CSelectItem key={t} value={t}>{t}</CSelectItem>))}
                </FieldSelect>
                <label className="text-xs text-muted-foreground">Width</label>
                <FieldSelect value={String((((optChartGrid().vertical || {}).main || {}).width || '1'))} onValueChangeAction={(v: string) => patchGridNode('vertical','main',{ width: Number(v) })}>
                  {['1','2','3'].map((t) => (<CSelectItem key={t} value={t}>{t}px</CSelectItem>))}
                </FieldSelect>
              </div>
            )}
          </div>
        </MenuGroup>
      </MenuList>
    )
  })

  // Render root menu via Portal near anchor rect
  const pane: Pane = (() => {
    const k = nav[0]
    if (!k) return makeRootPane()
    // Guard against accessing unavailable features
    if (k === 'filter' && hasFilter) return makeFilterPane(nav)
    if (k === 'sort' && hasSort) return makeSortPane(nav)
    if (k === 'format' && hasFormat) return makeFormatPane(nav)
    if (k === 'grid' && hasGrid) return makeGridPane()
    if (k === 'axis' && hasAxis) return makeAxisPane()
    if (k === 'tooltip' && hasTooltip) return makeTooltipPane()
    if (k === 'legend' && hasLegend) return makeLegendPane()
    // Fallback to root if feature not available
    return makeRootPane()
  })()

  return (
    <>
      {open && createPortal(
        <div ref={ref} className={`fixed z-[1000] ${menuDragging ? 'cursor-grabbing' : ''}`} style={{ left: (menuPos?.x ?? rootLeft), top: (menuPos?.y ?? ((rect?.y || 0) + 6)) }}>
          <MenuSurface>
            <div className="flex items-center justify-between px-2 py-1 border-b bg-card rounded-t-md" onPointerDown={startHeaderDrag}>
              {nav.length > 0 ? (
                <button
                  type="button"
                  className="h-7 w-7 flex items-center justify-center rounded hover:bg-secondary/70 border border-[hsl(var(--border))]"
                  onClick={() => { if (nav.length === 0) onCloseAction(); else setNav((prev) => prev.slice(0, -1)) }}
                  aria-label="Back"
                >
                  <RiArrowLeftLine className="h-4 w-4" aria-hidden="true" />
                </button>
              ) : (
                <div className="h-7 w-7" />
              )}
              <div className="text-[13px] font-medium">{pane.title}</div>
              <button className="h-7 w-7 flex items-center justify-center rounded hover:bg-secondary/70 border border-[hsl(var(--border))]" onClick={onCloseAction} aria-label="Close">✕</button>
            </div>
            <div className="max-h-[70vh] overflow-auto">
              {pane.content}
            </div>
          </MenuSurface>
        </div>,
        document.body
      )}
      {/* Alerts & Notifications Dialog (Unified) */}
      <AlertDialog
        open={alertsOpen}
        mode={existingAlert ? 'edit' : 'create'}
        onCloseAction={() => setAlertsOpen(false)}
        onSavedAction={(a) => { setExistingAlert(a); setAlertsOpen(false) }}
        widget={existingAlert ? undefined : cfg}
        alert={existingAlert || undefined}
        parentDashboardId={parentDashboardId}
        defaultKind={existingAlert ? undefined : 'notification'}
        defaultTemplate={existingAlert ? undefined : '{{KPI_IMG}}'}
      />
    </>
  )
}

function MenuSurface({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-md border border-[hsl(var(--border))] shadow-card bg-secondary/90 min-w-[220px] text-[13px]">
      {children}
    </div>
  )
}

function MenuList({ children }: { children: React.ReactNode }) {
  return (
    <ul className="py-1">
      {children}
    </ul>
  )
}

function MenuGroup({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <li className="px-2 py-1">
      <div className="px-2 py-1 text-[11px] uppercase tracking-wide text-muted-foreground">{title}</div>
      <ul className="rounded-md p-1 space-y-1 max-w-[340px]">
        {children}
      </ul>
    </li>
  )
}

function MenuItem({ label, icon, hasSubmenu, active, onHover, onClick, children, side = 'right', itemRef }: {
  label: string
  icon?: React.ReactNode
  hasSubmenu?: boolean
  active?: boolean
  onHover?: () => void
  onClick?: () => void
  children?: React.ReactNode
  side?: 'left' | 'right'
  itemRef?: (el: HTMLLIElement | null) => void
}) {
  return (
    <li className="relative" onMouseEnter={onHover} ref={itemRef}>
      <button
        type="button"
        className={`w-full flex items-center justify-between gap-3 px-3 py-2 text-left transition-colors hover:bg-secondary/60 hover:ring-1 hover:ring-inset hover:ring-[hsl(var(--border))] ${active ? 'bg-secondary/70 ring-1 ring-inset ring-[hsl(var(--border))]' : ''}`}
        onClick={onClick}
      >
        <span className="inline-flex items-center gap-2">
          {icon && <span className="w-4 text-center" aria-hidden>{icon}</span>}
          <span className="truncate">{label}</span>
        </span>
        {hasSubmenu && (
          side === 'left' ? <RiArrowLeftSLine className="h-4 w-4 text-muted-foreground" aria-hidden="true" /> : <RiArrowRightSLine className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
        )}
      </button>
      {active && children && (
        <div className={`absolute top-0 ${side === 'left' ? 'right-full mr-1' : 'left-full ml-1'}`}>
          <MenuSurface>
            <MenuList>{children}</MenuList>
          </MenuSurface>
        </div>
      )}
    </li>
  )
}

function SwitchRow({ label, checked, onChangeAction, defaultChecked }: { label: string; checked?: boolean; onChangeAction?: (v: boolean) => void; defaultChecked?: boolean }) {
  const [local, setLocal] = React.useState<boolean>(!!defaultChecked)
  useEffect(() => { if (checked != null) setLocal(!!checked) }, [checked])
  const onChange = (v: boolean) => {
    setLocal(v)
    onChangeAction && onChangeAction(v)
  }
  return (
    <div className="flex items-center justify-between px-2 py-1">
      <span className="text-sm">{label}</span>
      <Switch checked={checked != null ? checked : local} onChangeAction={onChange} />
    </div>
  )
}

function FieldSelect({ className, children, value, onValueChangeAction, onValueChange }: any) {
  const cb = onValueChangeAction ?? onValueChange
  const valueProp = (value === '' || value === null || value === undefined) ? undefined : value
  return (
    <CSelect value={valueProp} onValueChangeAction={cb}>
      <SelectTrigger className={`w-full rounded-lg border border-[hsl(var(--border))] bg-transparent px-3 py-2 text-left ${className || ''}`}>
        <SelectValue placeholder="Select..." />
      </SelectTrigger>
      <SelectContent>
        {children}
      </SelectContent>
    </CSelect>
  )
}

function FieldInput({ className, ...rest }: any) {
  return (
    <TextInput {...rest} className={`w-full rounded-lg border border-[hsl(var(--border))] bg-transparent ${className || ''}`} />
  )
}
