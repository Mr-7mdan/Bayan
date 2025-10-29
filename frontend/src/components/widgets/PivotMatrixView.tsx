"use client"

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { RiAddLine, RiAddFill, RiSubtractLine, RiSubtractFill, RiArrowRightSLine, RiArrowDownSLine, RiArrowRightSFill, RiArrowDownSFill, RiArrowRightWideLine, RiArrowDownWideLine, RiArrowDropRightLine, RiArrowDropDownLine } from '@remixicon/react'
import type { TableOptions } from '@/types/widgets'

export type PivotMatrixViewProps = {
  rows: Array<Record<string, any>>
  columns: string[]
  widgetId?: string
  tableOptions?: TableOptions
  where?: Record<string, any>
  pivot?: { values?: Array<{ agg?: 'none'|'count'|'distinct'|'avg'|'sum'|'min'|'max' }> }
}

// Small helper for stable string keys
function keyOf(rec: Record<string, any>, dims: string[]): string {
  if (!dims.length) return '__no_key__'
  return dims.map((d) => String(rec?.[d] ?? '')).join('|')
}

function splitKey(key: string): string[] {
  return key === '__no_key__' ? [] : key.split('|')
}

// Build a tree for headers (either row or column dims)
function buildTree(data: Record<string, any>[], dims: string[], orientation: 'row'|'col') {
  type Node = { label: string; children: Map<string, Node>; depth: number; leafCount: number }
  const root: Node = { label: '', children: new Map(), depth: 0, leafCount: 0 }
  const orderByLevel: Array<string[]> = dims.map(() => [])

  // Insertion order tracking per level
  const seenByLevel: Array<Set<string>> = dims.map(() => new Set<string>())

  for (const r of data) {
    let node = root
    for (let i = 0; i < dims.length; i++) {
      const d = dims[i]
      const label = String(r?.[d] ?? '')
      if (!node.children.has(label)) {
        const child: Node = { label, children: new Map(), depth: i + 1, leafCount: 0 }
        node.children.set(label, child)
        if (!seenByLevel[i].has(label)) { seenByLevel[i].add(label); orderByLevel[i].push(label) }
      }
      node = node.children.get(label)!
    }
  }

  // Compute leaf counts
  const computeLeaves = (n: Node): number => {
    if (n.children.size === 0) { n.leafCount = 1; return 1 }
    let sum = 0
    // preserve insertion order using orderByLevel at this depth
    const depth = n.depth
    const labels = orderByLevel[depth] || []
    for (const lb of labels) {
      const c = n.children.get(lb)
      if (c) sum += computeLeaves(c)
    }
    n.leafCount = sum
    return sum
  }
  computeLeaves(root)

  // Collect leaf keys order
  const leafKeys: string[] = []
  const collect = (n: Node, path: string[]) => {
    if (n.children.size === 0) {
      if (path.length) leafKeys.push(path.join('|'))
      return
    }
    const depth = n.depth
    const labels = orderByLevel[depth] || []
    for (const lb of labels) {
      const c = n.children.get(lb)
      if (c) collect(c, [...path, lb])
    }
  }
  collect(root, [])

  // Build header rows for columns
  const headerRows: Array<Array<{ text: string; colSpan: number }>> = []
  if (orientation === 'col') {
    for (let level = 0; level < dims.length; level++) {
      const row: Array<{ text: string; colSpan: number }> = []
      const walk = (n: Node, depth: number) => {
        if (depth === level) {
          const labels = orderByLevel[depth] || []
          for (const lb of labels) {
            const c = n.children.get(lb)
            if (c) row.push({ text: c.label, colSpan: c.leafCount })
          }
          return
        }
        const labels = orderByLevel[depth] || []
        for (const lb of labels) {
          const c = n.children.get(lb)
          if (c) walk(c, depth + 1)
        }
      }
      walk(root, 0)
      headerRows.push(row)
    }
  }

  return { root, leafKeys, headerRows }
}

export default function PivotMatrixView({ rows, columns: _cols, widgetId, tableOptions, pivot }: PivotMatrixViewProps) {
  const cfg = (tableOptions?.pivotConfig || {}) as any
  const rowDims: string[] = Array.isArray(cfg.rows) ? cfg.rows : []
  const colDims: string[] = Array.isArray(cfg.cols) ? cfg.cols : []
  const measure: string = Array.isArray(cfg.vals) && cfg.vals[0] ? cfg.vals[0] : 'value'
  const showRowTotals = cfg.rowTotals !== false
  const showColTotals = cfg.colTotals !== false
  const aggName = String((pivot?.values?.[0]?.agg) || 'count').toLowerCase()

  // Derive display label for single-series header (no legend)
  const valueLabel: string = useMemo(() => {
    try {
      const chip = (pivot?.values && Array.isArray(pivot.values) && pivot.values.length > 0) ? (pivot.values[0] as any) : undefined
      const lbl = String(chip?.label || '').trim()
      if (lbl) return lbl
      const field = String(chip?.field || chip?.measureId || '').trim()
      if (field) return field
      const first = (Array.isArray(cfg.vals) && cfg.vals[0]) ? String(cfg.vals[0]) : ''
      return first || 'Value'
    } catch { return 'Value' }
  }, [pivot?.values, cfg.vals])

  // Normalize input rows: convert shape { c0, c1, ... } into named-object rows using columns
  const namedRows = useMemo(() => {
    const cols = Array.isArray(_cols) ? _cols : []
    if (!Array.isArray(rows) || rows.length === 0) return [] as Array<Record<string, any>>
    if (cols.length === 0) return rows as Array<Record<string, any>>
    return (rows as Array<Record<string, any>>).map((r) => {
      const o: Record<string, any> = {}
      cols.forEach((name, idx) => {
        const v = (r as any)[`c${idx}`]
        o[name] = (v !== undefined) ? v : (r as any)[name]
      })
      return o
    })
  }, [rows, _cols])

  // Prune rows with blank/0 on any dimension; keep only records with measure present
  const baseData = useMemo(() => {
    return (namedRows || []).filter((r) => {
      for (const d of [...rowDims, ...colDims]) {
        const v = r?.[d]
        if (v == null) return false
        const s = String(v).trim()
        if (s === '') return false
      }
      return true
    })
  }, [namedRows, rowDims, colDims, measure])

  // Build column/row header trees and leaf orders
  const { leafKeys: colLeaves } = useMemo(() => buildTree(baseData, colDims, 'col'), [baseData, colDims])
  const { leafKeys: rowLeaves } = useMemo(() => buildTree(baseData, rowDims, 'row'), [baseData, rowDims])

  // Aggregate values into matrix map based on aggregator (count, distinct, sum, avg, min, max)
  const matrix = useMemo(() => {
    const out = new Map<string, Map<string, number>>()
    const mode = aggName.includes('distinct') ? 'distinct' : (aggName.startsWith('avg') ? 'avg' : (aggName.startsWith('min') ? 'min' : (aggName.startsWith('max') ? 'max' : (aggName.startsWith('sum') ? 'sum' : 'count'))))
    if (mode === 'count') {
      for (const r of baseData) {
        const rk = keyOf(r, rowDims)
        const ck = keyOf(r, colDims)
        if (!out.has(rk)) out.set(rk, new Map())
        const inner = out.get(rk)!
        inner.set(ck, (inner.get(ck) || 0) + 1)
      }
      return out
    }
    if (mode === 'sum') {
      for (const r of baseData) {
        const rk = keyOf(r, rowDims)
        const ck = keyOf(r, colDims)
        const v = Number(r?.[measure] ?? 0)
        if (!out.has(rk)) out.set(rk, new Map())
        const inner = out.get(rk)!
        inner.set(ck, (inner.get(ck) || 0) + (isNaN(v) ? 0 : v))
      }
      return out
    }
    if (mode === 'avg') {
      const sum = new Map<string, Map<string, number>>()
      const cnt = new Map<string, Map<string, number>>()
      for (const r of baseData) {
        const rk = keyOf(r, rowDims)
        const ck = keyOf(r, colDims)
        const v = Number(r?.[measure] ?? 0)
        if (!sum.has(rk)) { sum.set(rk, new Map()); cnt.set(rk, new Map()) }
        const sIn = sum.get(rk)!; const cIn = cnt.get(rk)!
        sIn.set(ck, (sIn.get(ck) || 0) + (isNaN(v) ? 0 : v))
        cIn.set(ck, (cIn.get(ck) || 0) + 1)
      }
      // finalize
      for (const [rk, sIn] of sum.entries()) {
        const cIn = cnt.get(rk)!
        const inner = new Map<string, number>()
        for (const [ck, s] of sIn.entries()) {
          const c = cIn.get(ck) || 0
          inner.set(ck, c === 0 ? 0 : s / c)
        }
        out.set(rk, inner)
      }
      return out
    }
    if (mode === 'min' || mode === 'max') {
      for (const r of baseData) {
        const rk = keyOf(r, rowDims)
        const ck = keyOf(r, colDims)
        const v = Number(r?.[measure] ?? 0)
        if (!out.has(rk)) out.set(rk, new Map())
        const inner = out.get(rk)!
        if (!inner.has(ck)) inner.set(ck, isNaN(v) ? 0 : v)
        else inner.set(ck, mode === 'min' ? Math.min(inner.get(ck) || 0, isNaN(v) ? 0 : v) : Math.max(inner.get(ck) || 0, isNaN(v) ? 0 : v))
      }
      return out
    }
    // distinct count by measure value
    const sets = new Map<string, Map<string, Set<string>>>()
    for (const r of baseData) {
      const rk = keyOf(r, rowDims)
      const ck = keyOf(r, colDims)
      const mv = r?.[measure]
      if (!sets.has(rk)) sets.set(rk, new Map())
      const inner = sets.get(rk)!
      if (!inner.has(ck)) inner.set(ck, new Set())
      inner.get(ck)!.add(String(mv))
    }
    for (const [rk, innerSets] of sets.entries()) {
      const inner = new Map<string, number>()
      for (const [ck, s] of innerSets.entries()) inner.set(ck, s.size)
      out.set(rk, inner)
    }
    return out
  }, [baseData, rowDims, colDims, measure, aggName])

  // Prune zero-only rows/columns
  const { visRowLeaves, visColLeaves } = useMemo(() => {
    // Special case: no column dimensions => keep all rows; columns empty
    if (colLeaves.length === 0) return { visRowLeaves: rowLeaves.slice(), visColLeaves: [] as string[] }
    return { visRowLeaves: rowLeaves.slice(), visColLeaves: colLeaves.slice() }
  }, [rowLeaves, colLeaves])

  // (moved) row sorting will be defined below after valueAt

  // Expand/Collapse state for row hierarchy (by prefix path)
  const [collapsed, setCollapsed] = useState<Set<string>>(new Set())
  const [repByPrefix, setRepByPrefix] = useState<Map<string, string>>(new Map())
  const [closingPrefix, setClosingPrefix] = useState<string | null>(null)
  const [openingPrefix, setOpeningPrefix] = useState<string | null>(null)
  const toggle = (path: string, leafKey?: string) => {
    const isOpen = !collapsed.has(path)
    if (isOpen) {
      // Start roll-up animation, then commit collapse
      setClosingPrefix(path)
      if (leafKey) setRepByPrefix((m) => { const c = new Map(m); c.set(path, leafKey); return c })
      window.setTimeout(() => {
        setCollapsed((prev) => new Set<string>([...prev, path]))
        setClosingPrefix(null)
      }, 220)
    } else {
      // Remove collapse first to reveal rows, then play roll-down animation
      setCollapsed((prev) => {
        const next = new Set(prev); next.delete(path); return next
      })
      setOpeningPrefix(path)
      window.setTimeout(() => setOpeningPrefix(null), 220)
      setRepByPrefix((m) => { const c = new Map(m); c.delete(path); return c })
    }
  }
  const isCollapsed = (path: string) => collapsed.has(path)

  // Column expand/collapse state and helpers
  const [colCollapsed, setColCollapsed] = useState<Set<string>>(new Set())
  const [colRepByPrefix, setColRepByPrefix] = useState<Map<string, string>>(new Map())
  const [closingColPrefix, setClosingColPrefix] = useState<string | null>(null)
  const [openingColPrefix, setOpeningColPrefix] = useState<string | null>(null)
  const isColCollapsed = (path: string) => colCollapsed.has(path)
  const toggleCol = (path: string, leafKey?: string) => {
    const isOpen = !colCollapsed.has(path)
    if (isOpen) {
      setClosingColPrefix(path)
      if (leafKey) setColRepByPrefix((m) => { const c = new Map(m); c.set(path, leafKey); return c })
      window.setTimeout(() => {
        setColCollapsed((prev) => new Set<string>([...prev, path]))
        setClosingColPrefix(null)
      }, 220)
    } else {
      setColCollapsed((prev) => { const n = new Set(prev); n.delete(path); return n })
      setOpeningColPrefix(path)
      window.setTimeout(() => setOpeningColPrefix(null), 220)
      setColRepByPrefix((m) => { const c = new Map(m); c.delete(path); return c })
    }
  }

  // Visible column leaves honoring collapsed parents (for rendering) and a helper to collect leaves under any prefix
  const visibleColLeaves = useMemo<string[]>(() => {
    const parts = colLeaves.map(splitKey)
    const visible: string[] = []
    for (let idx = 0; idx < parts.length; idx++) {
      const ck = colLeaves[idx]
      const bits = parts[idx]
      let collapsedUnder: string | null = null
      for (let i = bits.length - 1; i >= 1; i--) {
        const pref = bits.slice(0, i).join('|')
        if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
      }
      if (!collapsedUnder) { visible.push(ck); continue }
      const first = colLeaves.find((c: string) => splitKey(c).slice(0, splitKey(collapsedUnder!).length).join('|') === collapsedUnder)
      const rep = colRepByPrefix.get(collapsedUnder!) || first
      if (rep === ck) visible.push(ck)
    }
    return visible
  }, [colLeaves, colCollapsed, colRepByPrefix])

  // Signal ready for snapshot after matrix is laid out
  const readyOnce = useRef(false)
  useEffect(() => {
    try {
      if (readyOnce.current) return
      const afterPaint = () => {
        try {
          if (typeof window !== 'undefined') {
            try { window.dispatchEvent(new CustomEvent('widget-data-ready')) } catch {}
            try { const el = document.getElementById('widget-root'); if (el) el.setAttribute('data-widget-ready','1') } catch {}
          }
        } catch {}
      }
      if (typeof window !== 'undefined') {
        requestAnimationFrame(() => requestAnimationFrame(afterPaint))
      }
      readyOnce.current = true
    } catch {}
  }, [visRowLeaves.length, visibleColLeaves.length])

  const colLeavesUnder = (prefix?: string | null, visibleOnly = false): string[] => {
    if (colDims.length === 0) return ['__no_key__']
    const source = visibleOnly ? visibleColLeaves : colLeaves
    if (!prefix) return source.slice()
    const partsP = splitKey(prefix)
    if (partsP.length >= colDims.length) return [prefix]
    return source.filter((ck: string) => {
      const p = splitKey(ck)
      for (let i = 0; i < partsP.length; i++) { if (p[i] !== partsP[i]) return false }
      return true
    })
  }

  // Compute which prefixes have children (to decide where to show toggles)
  const prefixesWithChildren = useMemo(() => {
    const set = new Set<string>()
    for (const rk of visRowLeaves) {
      const parts = splitKey(rk)
      // Only include strict prefixes (ancestors), not the full leaf path
      for (let i = 1; i < parts.length; i++) {
        set.add(parts.slice(0, i).join('|'))
      }
    }
    return set
  }, [visRowLeaves])

  // Global expand/collapse for parent rows (triggered from TableCard buttons)
  useEffect(() => {
    const onExpandAll = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { widgetId?: string }
        if (!d?.widgetId || d.widgetId !== widgetId) return
        setCollapsed(new Set())
        setClosingPrefix(null)
        setOpeningPrefix(null)
        setRepByPrefix(new Map())
      } catch {}
    }
    const onCollapseAll = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { widgetId?: string }
        if (!d?.widgetId || d.widgetId !== widgetId) return
        const all = new Set<string>(Array.from(prefixesWithChildren.values()))
        setCollapsed(all)
        setClosingPrefix(null)
        setOpeningPrefix(null)
      } catch {}
    }
    if (typeof window !== 'undefined') {
      window.addEventListener('pivot-expand-all', onExpandAll as EventListener)
      window.addEventListener('pivot-collapse-all', onCollapseAll as EventListener)
    }
    return () => {
      if (typeof window !== 'undefined') {
        window.removeEventListener('pivot-expand-all', onExpandAll as EventListener)
        window.removeEventListener('pivot-collapse-all', onCollapseAll as EventListener)
      }
    }
  }, [widgetId, prefixesWithChildren])

  // Map of prefix -> ordered leaves under it, and first representative leaf per collapsed prefix
  const leavesByPrefix = useMemo(() => {
    const m = new Map<string, string[]>()
    for (const rk of visRowLeaves) {
      const parts = splitKey(rk)
      for (let i = 1; i < parts.length; i++) {
        const pref = parts.slice(0, i).join('|')
        if (!m.has(pref)) m.set(pref, [])
        m.get(pref)!.push(rk)
      }
    }
    return m
  }, [visRowLeaves])
  const firstLeafForPrefix = useMemo(() => {
    const map = new Map<string, string>()
    for (const [pref, arr] of leavesByPrefix.entries()) { if (arr.length > 0) map.set(pref, arr[0]) }
    return map
  }, [leavesByPrefix])
  const nearestCollapsedPrefixFor = (rk: string): string | null => {
    const parts = splitKey(rk)
    for (let i = parts.length - 1; i >= 1; i--) {
      const pref = parts.slice(0, i).join('|')
      if (collapsed.has(pref)) return pref
    }
    return null
  }
  const isUnderPrefix = (rk: string, pref: string | null): boolean => {
    if (!pref) return false
    const pA = splitKey(pref)
    const rA = splitKey(rk)
    if (pA.length === 0) return false
    for (let i = 0; i < pA.length; i++) { if (rA[i] !== pA[i]) return false }
    return true
  }

  // (moved below) visibleRowKeys computed after sorting/value helpers

  // Row header rowSpan calculation to merge repeated labels vertically
  // (moved below) rowHeaderSpans computed after visibleRowKeys

  const density = tableOptions?.density || 'compact'
  const headerHeight = (tableOptions as any)?.pivotStyle?.headerRowHeight ?? (density === 'compact' ? 28 : 36)
  const rowHeight = (tableOptions as any)?.pivotStyle?.cellRowHeight ?? (density === 'compact' ? 28 : 36)
  const headerFontSize = (tableOptions as any)?.pivotStyle?.headerFontSize ?? (density === 'compact' ? 13 : 14)
  const cellFontSize = (tableOptions as any)?.pivotStyle?.cellFontSize ?? (density === 'compact' ? 14 : 15)
  const headerFontWeight = (tableOptions as any)?.pivotStyle?.headerFontWeight || 'semibold'
  const cellFontWeight = (tableOptions as any)?.pivotStyle?.cellFontWeight || 'normal'
  const headerFontStyle = (tableOptions as any)?.pivotStyle?.headerFontStyle || 'normal'
  const cellFontStyle = (tableOptions as any)?.pivotStyle?.cellFontStyle || 'normal'
  const leafRowEmphasis = ((tableOptions as any)?.pivotStyle?.leafRowEmphasis) === true
  const rowHeaderDepthHue = ((tableOptions as any)?.pivotStyle?.rowHeaderDepthHue) === true
  const colHeaderDepthHue = ((tableOptions as any)?.pivotStyle?.colHeaderDepthHue) === true
  const showSubtotals = ((tableOptions as any)?.pivotStyle?.showSubtotals) === true
  const valueFormat = (tableOptions as any)?.pivotStyle?.valueFormat as string | undefined
  const valuePrefix = (tableOptions as any)?.pivotStyle?.valuePrefix as string | undefined
  const valueSuffix = (tableOptions as any)?.pivotStyle?.valueSuffix as string | undefined
  const rowHeaderAlignClass = 'align-top'
  // Cell alignment (defaults): horizontal left, vertical top
  const cellHAlign = (tableOptions as any)?.pivotStyle?.cellHAlign || 'left'
  const cellVAlign = (tableOptions as any)?.pivotStyle?.cellVAlign || 'top'
  const cellTextAlignClass = cellHAlign === 'center' ? 'text-center' : (cellHAlign === 'right' ? 'text-right' : 'text-left')
  const cellValignClass = (cellVAlign === 'middle' || cellVAlign === 'center') ? 'align-middle' : (cellVAlign === 'bottom' ? 'align-bottom' : 'align-top')
  // Header alignment (defaults): horizontal left, vertical top
  const headerHAlign = (tableOptions as any)?.pivotStyle?.headerHAlign || 'left'
  const headerVAlign = (tableOptions as any)?.pivotStyle?.headerVAlign || 'top'
  const headerTextAlignClass = headerHAlign === 'center' ? 'text-center' : (headerHAlign === 'right' ? 'text-right' : 'text-left')
  const headerValignClass = (headerVAlign === 'middle' || headerVAlign === 'center') ? 'align-middle' : (headerVAlign === 'bottom' ? 'align-bottom' : 'align-top')
  const headerVFlexClass = headerVAlign === 'middle' || headerVAlign === 'center' ? 'items-center' : (headerVAlign === 'bottom' ? 'items-end' : 'items-start')
  const headerHJustifyClass = headerHAlign === 'center' ? 'justify-center' : (headerHAlign === 'right' ? 'justify-end' : 'justify-start')

  const collapseBorders = ((tableOptions as any)?.pivotStyle?.collapseBorders) !== false
  const tableClass = `pvt-matrix pvt-theme-quartz min-w-full w-max text-sm ${collapseBorders ? 'border-collapse' : 'border-separate border-spacing-[1px]'}`
  const animCss = `
  @keyframes pvtRollUp { 0% { opacity: 1; transform: scaleY(1) translateZ(0) } 100% { opacity: 0; transform: scaleY(0) translateZ(0) } }
  @keyframes pvtRollDown { 0% { opacity: 0; transform: scaleY(0) translateZ(0) } 100% { opacity: 1; transform: scaleY(1) translateZ(0) } }
  @keyframes pvtColRollOut { 0% { opacity: 1; transform: scaleX(1) translateZ(0) } 100% { opacity: 0; transform: scaleX(0) translateZ(0) } }
  @keyframes pvtColRollIn { 0% { opacity: 0; transform: scaleX(0) translateZ(0) } 100% { opacity: 1; transform: scaleX(1) translateZ(0) } }
  `

  // Number formatting for matrix values
  const formatNumber = (n: number): string => {
    const mode = String(valueFormat || 'number')
    const prefix = valuePrefix || ''
    const suffix = valueSuffix || ''
    const safe = (x: number) => (Number.isFinite(x) ? x : 0)
    const wrap = (s: string) => `${prefix}${s}${suffix}`
    const abs = Math.abs(n)
    switch (mode) {
      case 'abbrev': {
        if (abs >= 1_000_000_000) return wrap(`${(n/1_000_000_000).toLocaleString(undefined,{maximumFractionDigits:2})}B`)
        if (abs >= 1_000_000) return wrap(`${(n/1_000_000).toLocaleString(undefined,{maximumFractionDigits:2})}M`)
        if (abs >= 1_000) return wrap(`${(n/1_000).toLocaleString(undefined,{maximumFractionDigits:2})}K`)
        return wrap(n.toLocaleString(undefined,{maximumFractionDigits:2}))
      }
      case 'short': return wrap(n.toLocaleString(undefined,{maximumFractionDigits:1}))
      case 'twoDecimals': return wrap(safe(n).toLocaleString(undefined,{minimumFractionDigits:2, maximumFractionDigits:2}))
      case 'oneDecimal': return wrap(safe(n).toLocaleString(undefined,{minimumFractionDigits:1, maximumFractionDigits:1}))
      case 'wholeNumber': return wrap(Math.round(safe(n)).toLocaleString())
      case 'thousands': return wrap(safe(n).toLocaleString())
      case 'millions': return wrap((safe(n)/1_000_000).toLocaleString(undefined,{maximumFractionDigits:2}))
      case 'billions': return wrap((safe(n)/1_000_000_000).toLocaleString(undefined,{maximumFractionDigits:2}))
      case 'percent': return wrap(`${(safe(n)*100).toLocaleString(undefined,{maximumFractionDigits:1})}%`)
      case 'bytes': {
        const units = ['B','KB','MB','GB','TB']; let x = abs; let u = 0; while (x>=1024 && u<units.length-1){x/=1024;u++}
        const sign = n<0?'-':''; return wrap(`${sign}${x.toLocaleString(undefined,{maximumFractionDigits:2})} ${units[u]}`)
      }
      case 'number': default: return wrap(safe(n).toLocaleString())
    }
  }

  // Styling toggles
  const altRows = ((tableOptions as any)?.pivotStyle?.alternateRows) !== false
  const rowHover = ((tableOptions as any)?.pivotStyle?.rowHover) !== false

  // Expand/collapse icon style mapping
  const expandIconStyle = (tableOptions as any)?.pivotStyle?.expandIconStyle || 'plusMinusLine'
  const showIconOutline = expandIconStyle === 'plusMinusLine' || expandIconStyle === 'plusMinusFill'
  const ToggleIcon = ({ open }: { open: boolean }) => {
    switch (expandIconStyle) {
      case 'plusMinusFill': return open ? <RiSubtractFill className="w-3 h-3" /> : <RiAddFill className="w-3 h-3" />
      case 'arrowLine': return open ? <RiArrowDownSLine className="w-3 h-3" /> : <RiArrowRightSLine className="w-3 h-3" />
      case 'arrowFill': return open ? <RiArrowDownSFill className="w-3 h-3" /> : <RiArrowRightSFill className="w-3 h-3" />
      case 'arrowWide': return open ? <RiArrowDownWideLine className="w-3 h-3" /> : <RiArrowRightWideLine className="w-3 h-3" />
      case 'arrowDrop': return open ? <RiArrowDropDownLine className="w-4 h-4" /> : <RiArrowDropRightLine className="w-4 h-4" />
      case 'plusMinusLine': default: return open ? <RiSubtractLine className="w-3 h-3" /> : <RiAddLine className="w-3 h-3" />
    }
  }

  // Excel export support via window event from TableCard
  const tableRef = useRef<HTMLTableElement | null>(null)
  useEffect(() => {
    const handler = async (e: Event) => {
      const d = (e as CustomEvent).detail as { widgetId?: string; filename?: string }
      if (!d?.widgetId || d.widgetId !== widgetId) return
      try {
        const table = tableRef.current
        if (!table) return

        const ExcelJS: any = await import('exceljs')
        const wb = new ExcelJS.Workbook()
        const ws = wb.addWorksheet('Pivot')

        // Force LIGHT MODE palette for Excel export (do not depend on current UI theme)
        const BORDER = { style: 'thin', color: { argb: 'FFD0D5DD' } } as const // gray-200
        const HEADER_BG = 'FFF3F4F6' // slate-100-ish
        const ALT_BG = 'FFF8FAFC'    // slate-50-ish

        // Helper to sanitize visible text
        const cellText = (el: Element) => (el.textContent || '').replace(/[+−]\s*/g, '').replace(/\u00a0/g, ' ').trim()

        // Track merges that span into future rows so column positions stay aligned
        const futureSkips: Map<number, Set<number>> = new Map()
        const markSkip = (r: number, c: number) => {
          if (!futureSkips.has(r)) futureSkips.set(r, new Set())
          futureSkips.get(r)!.add(c)
        }
        const isSkipped = (r: number, c: number) => futureSkips.get(r)?.has(c) || false

        let excelRow = 1
        const applyBorders = (r: number, c: number) => {
          const cell = ws.getCell(r, c)
          cell.border = { top: BORDER, right: BORDER, bottom: BORDER, left: BORDER }
        }

        // Headers
        const thead = table.querySelector('thead')
        if (thead) {
          const headerRows = Array.from(thead.querySelectorAll('tr'))
          for (const tr of headerRows) {
            let col = 1
            const cells = Array.from(tr.children) as HTMLElement[]
            for (const th of cells) {
              // Skip occupied positions from rowSpan merges
              while (isSkipped(excelRow, col)) col++
              const cs = Math.max(1, Number(th.getAttribute('colspan') || 1))
              const rs = Math.max(1, Number(th.getAttribute('rowspan') || 1))
              const text = cellText(th)
              const anchor = ws.getCell(excelRow, col)
              anchor.value = text
              anchor.alignment = { vertical: 'middle', horizontal: 'left', wrapText: true }
              anchor.font = { bold: true }
              anchor.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: HEADER_BG } }
              applyBorders(excelRow, col)
              if (cs > 1 || rs > 1) {
                ws.mergeCells(excelRow, col, excelRow + rs - 1, col + cs - 1)
                // Mark future row/col skips for rowSpan region
                for (let rr = 1; rr < rs; rr++) {
                  for (let cc = 0; cc < cs; cc++) markSkip(excelRow + rr, col + cc)
                }
              }
              col += cs
            }
            excelRow++
          }
        }

        // Body
        const tbody = table.querySelector('tbody')
        let bodyRowIdx = 0
        if (tbody) {
          const rows = Array.from(tbody.querySelectorAll('tr'))
          for (const tr of rows) {
            let col = 1
            const cells = Array.from(tr.children) as HTMLElement[]
            for (const td of cells) {
              while (isSkipped(excelRow, col)) col++
              const cs = Math.max(1, Number(td.getAttribute('colspan') || 1))
              const rs = Math.max(1, Number(td.getAttribute('rowspan') || 1))
              const raw = cellText(td)
              const anchor = ws.getCell(excelRow, col)
              // Assign value with numeric typing when possible (non-TH cells)
              let assigned = false
              if (td.tagName.toUpperCase() !== 'TH') {
                const esc = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
                const pfx = (typeof valuePrefix === 'string' && valuePrefix) ? valuePrefix : ''
                const sfx = (typeof valueSuffix === 'string' && valueSuffix) ? valueSuffix : ''
                const stripped = raw
                  .replace(new RegExp(`^\\s*${pfx ? esc(pfx) : ''}\\s*`), '')
                  .replace(new RegExp(`\\s*${sfx ? esc(sfx) : ''}\\s*$`), '')
                const pctLike = /^(?:[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?|[-+]?\d+(?:\.\d+)?)%$/.test(stripped)
                if (pctLike) {
                  const n = Number(stripped.replace(/,/g, '').replace('%','')) / 100
                  if (Number.isFinite(n)) {
                    anchor.value = n
                    anchor.numFmt = stripped.includes('.') ? '0.0%' : '0%'
                    assigned = true
                  }
                } else {
                  const numLike = /^(?:[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?|[-+]?\d+(?:\.\d+)?)$/.test(stripped)
                  if (numLike) {
                    const n = Number(stripped.replace(/,/g, ''))
                    if (Number.isFinite(n)) {
                      anchor.value = n
                      const baseFmt = stripped.includes('.') ? '#,##0.00' : '#,##0'
                      const q = (z: string) => z.replace(/"/g, '""')
                      let fmt = baseFmt
                      if (pfx) fmt = `"${q(pfx)}"${pfx.endsWith(' ') ? '' : ' '}${fmt}`.replace(/\s+$/, '')
                      if (sfx) fmt = `${fmt}${sfx.startsWith(' ') ? '' : ' '}"${q(sfx)}"`.replace(/^\s+/, '')
                      anchor.numFmt = fmt
                      assigned = true
                    }
                  }
                }
              }
              if (!assigned) anchor.value = raw
              // Treat TH inside tbody as row header cells: header styling
              if (td.tagName.toUpperCase() === 'TH') {
                anchor.font = { bold: true }
                anchor.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: HEADER_BG } }
              }
              // Alignment based on utility classes
              const cls = td.className || ''
              const horiz = cls.includes('text-right') ? 'right' : (cls.includes('text-center') ? 'center' : 'left')
              const vert = cls.includes('align-middle') ? 'middle' : (cls.includes('align-bottom') ? 'bottom' : 'top')
              anchor.alignment = { horizontal: horiz as any, vertical: vert as any, wrapText: true }
              // Alternating row background
              if (altRows && (bodyRowIdx % 2 === 1)) {
                anchor.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: ALT_BG } }
              }
              applyBorders(excelRow, col)
              if (cs > 1 || rs > 1) {
                ws.mergeCells(excelRow, col, excelRow + rs - 1, col + cs - 1)
                for (let rr = 1; rr < rs; rr++) {
                  for (let cc = 0; cc < cs; cc++) markSkip(excelRow + rr, col + cc)
                }
              }
              col += cs
            }
            excelRow++
            bodyRowIdx++
          }
        }

        // Autosize columns (rough heuristic based on value length)
        const maxCols = ws.columnCount
        for (let c = 1; c <= maxCols; c++) {
          let maxLen = 10
          ws.getColumn(c).eachCell({ includeEmpty: true }, (cell: any) => {
            const v = cell.value
            const s = v == null ? '' : (typeof v === 'string' ? v : String(v))
            maxLen = Math.max(maxLen, Math.min(50, s.length + 2))
          })
          ws.getColumn(c).width = maxLen
        }

        // Build filename: <title>-YYYY-MM-DD h mmpm.xlsx
        const sanitize = (s: string) => s.replace(/[\\/:*?"<>|]/g, '-').replace(/\s+/g, ' ').trim()
        const tsFmt = (() => {
          const d0 = new Date()
          const yyyy = d0.getFullYear()
          const mm = String(d0.getMonth() + 1).padStart(2, '0')
          const dd = String(d0.getDate()).padStart(2, '0')
          let hh = d0.getHours()
          const ampm = hh >= 12 ? 'pm' : 'am'
          hh = hh % 12; if (hh === 0) hh = 12
          const min = String(d0.getMinutes()).padStart(2, '0')
          return `${yyyy}-${mm}-${dd} ${hh} ${min}${ampm}`
        })()
        const base = sanitize(String(d.filename || 'Pivot'))
        const filename = `${base}-${tsFmt}.xlsx`

        const buf = await wb.xlsx.writeBuffer()
        const blob = new Blob([buf], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
        const a = document.createElement('a')
        a.href = URL.createObjectURL(blob)
        a.download = filename
        document.body.appendChild(a)
        a.click()
        a.remove()
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('pivot-export-excel', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('pivot-export-excel', handler as EventListener) }
  }, [widgetId, altRows])

  // Value accessor that aggregates across collapsed row and column groups when needed
  const valueAt = (rk: string, ck: string, opts?: { ignoreRowCollapse?: boolean }): number => {
    const rPref = opts?.ignoreRowCollapse ? null : nearestCollapsedPrefixFor(rk)
    const rowKeys = rPref ? (leavesByPrefix.get(rPref) || []) : [rk]
    const colKeys = (() => {
      if (colDims.length === 0) return ['__no_key__']
      if (ck === '__no_key__') return ['__no_key__']
      if (ck === '__TOTAL__') return colLeavesUnder(null)
      // Treat ck as either a leaf key or a parent prefix
      const parts = splitKey(ck)
      if (parts.length === colDims.length) return [ck]
      return colLeavesUnder(ck)
    })()
    const mode = aggName.includes('distinct') ? 'distinct' : (aggName.startsWith('avg') ? 'avg' : (aggName.startsWith('min') ? 'min' : (aggName.startsWith('max') ? 'max' : (aggName.startsWith('sum') ? 'sum' : 'count'))))
    if (mode === 'min') {
      let m = Number.POSITIVE_INFINITY
      for (const r of rowKeys) for (const c of colKeys) { const v = Number(matrix.get(r)?.get(c) || 0); if (v < m) m = v }
      return m === Number.POSITIVE_INFINITY ? 0 : m
    }
    if (mode === 'max') {
      let m = Number.NEGATIVE_INFINITY
      for (const r of rowKeys) for (const c of colKeys) { const v = Number(matrix.get(r)?.get(c) || 0); if (v > m) m = v }
      return m === Number.NEGATIVE_INFINITY ? 0 : m
    }
    let s = 0
    for (const r of rowKeys) for (const c of colKeys) s += Number(matrix.get(r)?.get(c) || 0)
    return s
  }

  // Sorting state
  const cycle = (m: 'none'|'asc'|'desc'): 'none'|'asc'|'desc' => (m === 'none' ? 'desc' : (m === 'desc' ? 'asc' : 'none'))
  const [valueSort, setValueSort] = useState<{ colKey?: string; mode: 'none'|'asc'|'desc' }>({ mode: 'none' })
  const [headerSort, setHeaderSort] = useState<Array<'none'|'asc'|'desc'>>(Array.from({ length: Math.max(1, rowDims.length) }, () => 'none'))
  useEffect(() => {
    setHeaderSort((prev) => {
      const n = Math.max(1, rowDims.length)
      const next = Array.from({ length: n }, (_, i) => prev[i] ?? 'none')
      return next
    })
  }, [rowDims.length])
  useEffect(() => {
    try {
      const s: any = (pivot as any)?.values?.[0]?.sort
      const dir = ((s?.direction || 'desc') as 'asc'|'desc')
      if (s?.by === 'x') {
        setHeaderSort((prev) => {
          const n = Math.max(1, rowDims.length)
          const next = Array.from({ length: n }, (_, i) => prev[i] ?? 'none')
          next[0] = dir
          return next
        })
      } else if (s?.by === 'value') {
        setValueSort({ colKey: '__TOTAL__', mode: dir })
      }
    } catch {}
  }, [pivot, rowDims.length, colDims.length])
  const onRowHeaderClick = (level: number) => setHeaderSort((prev) => {
    const next = prev.slice()
    next[level] = cycle(prev[level] || 'none')
    return next
  })
  const onColHeaderClick = (colKey: string) => setValueSort((s) => ({ colKey, mode: (s.colKey === colKey ? cycle(s.mode) : 'desc') }))

  const sortedVisRowLeaves = useMemo<string[]>(() => {
    try {
      const keys = visRowLeaves.slice()
      if (!rowDims.length) return keys

      // Group leaves by immediate parent (one level above leaf)
      const parentLen = Math.max(0, rowDims.length - 1)
      const parentToLeaves: Map<string, string[]> = new Map()
      for (const rk of keys) {
        const parts = splitKey(rk)
        const parent = parts.slice(0, parentLen).join('|')
        if (!parentToLeaves.has(parent)) parentToLeaves.set(parent, [])
        parentToLeaves.get(parent)!.push(rk)
      }

      const hasValueSort = valueSort.mode !== 'none' && !!valueSort.colKey
      const isTotal = valueSort.colKey === '__TOTAL__'
      const isNoKey = valueSort.colKey === '__no_key__'
      const isLeafCol = !isTotal && !isNoKey && !!valueSort.colKey && (splitKey(valueSort.colKey!).length === colDims.length)

      // Parent ordering: headerSort[0] (label) takes precedence; else by value parent/total; else current order
      let parentEntries = Array.from(parentToLeaves.entries())
      if (headerSort[0] !== 'none') {
        parentEntries.sort((a, b) => {
          const aLabel = (splitKey(a[0])[0] || '').toLowerCase()
          const bLabel = (splitKey(b[0])[0] || '').toLowerCase()
          return headerSort[0] === 'asc' ? (aLabel < bLabel ? -1 : aLabel > bLabel ? 1 : 0) : (aLabel > bLabel ? -1 : aLabel < bLabel ? 1 : 0)
        })
      } else if (hasValueSort) {
        parentEntries.sort((a, b) => {
          const sumFor = (arr: string[]) => arr.reduce((acc, rk) => acc + (Number(valueAt(rk, valueSort.colKey!, { ignoreRowCollapse: true })) || 0), 0)
          const av = sumFor(a[1])
          const bv = sumFor(b[1])
          const cmp = valueSort.mode === 'asc' ? (av - bv) : (bv - av)
          if (cmp !== 0) return cmp
          // tie-breaker: alphabetical parent label for determinism across expand/collapse
          const aLabel = (splitKey(a[0])[0] || '').toLowerCase()
          const bLabel = (splitKey(b[0])[0] || '').toLowerCase()
          return aLabel < bLabel ? -1 : (aLabel > bLabel ? 1 : 0)
        })
      }

      // Within each parent group: primary value sort (leaf col), then headerSort for deeper levels
      const out: string[] = []
      for (const [, arrRaw] of parentEntries) {
        const arr = arrRaw.slice()
        arr.sort((a, b) => {
          // Do NOT sort children by valueSort; only parent groups move on value sorts
          for (let L = 1; L < rowDims.length; L++) {
            const mode = headerSort[L]
            if (mode === 'none') continue
            const aLabel = (splitKey(a)[L] || '').toLowerCase()
            const bLabel = (splitKey(b)[L] || '').toLowerCase()
            if (aLabel !== bLabel) return mode === 'asc' ? (aLabel < bLabel ? -1 : 1) : (aLabel > bLabel ? -1 : 1)
          }
          return 0
        })
        out.push(...arr)
      }
      return out
    } catch { return visRowLeaves }
  }, [visRowLeaves, rowDims.length, colDims.length, valueSort, headerSort, valueAt])

  const visibleRowKeys = useMemo<string[]>(() => {
    if (rowDims.length === 0) return visRowLeaves
    const out: string[] = []
    const arr = sortedVisRowLeaves
    for (const rk of arr) {
      const pref = nearestCollapsedPrefixFor(rk)
      if (!pref) { out.push(rk); continue }
      const rep = repByPrefix.get(pref) || firstLeafForPrefix.get(pref)
      if (rep === rk) out.push(rk)
    }
    return out
  }, [sortedVisRowLeaves, firstLeafForPrefix, repByPrefix, collapsed, rowDims.length])

  const rowHeaderSpans = useMemo<number[][]>(() => {
    const levelRuns: Array<number[]> = rowDims.map(() => [])
    const keys = visibleRowKeys
    const parts = keys.map(splitKey)
    for (let level = 0; level < rowDims.length; level++) {
      let i = 0
      while (i < parts.length) {
        const val = parts[i]?.[level] ?? ''
        let j = i + 1
        while (j < parts.length && parts[j]?.[level] === val) j++
        levelRuns[level][i] = j - i
        for (let k = i + 1; k < j; k++) levelRuns[level][k] = 0
        i = j
      }
    }
    return levelRuns
  }, [visibleRowKeys, rowDims])

  // Render
  return (
    <div className="rounded-md border bg-transparent flex flex-col overflow-hidden">
      <style>{animCss}</style>
      <table ref={tableRef} className={tableClass}>
        <thead>
          {/* Column header rows */}
          {colDims.length > 0 ? (
            (() => {
              // Derive visible columns honoring collapsed column parents
              const parts = colLeaves.map(splitKey)
              const visible: string[] = []
              for (let idx = 0; idx < parts.length; idx++) {
                const ck = colLeaves[idx]
                const bits = parts[idx]
                let collapsedUnder: string | null = null
                for (let i = bits.length - 1; i >= 1; i--) {
                  const pref = bits.slice(0, i).join('|')
                  if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                }
                if (!collapsedUnder) { visible.push(ck); continue }
                // keep only representative child for collapsed parent
                const rep = (() => {
                  const first = colLeaves.find(c => splitKey(c).slice(0, splitKey(collapsedUnder!).length).join('|') === collapsedUnder)
                  return (colRepByPrefix.get(collapsedUnder) || first)
                })()
                if (rep === ck) visible.push(ck)
              }
              const partsV = visible.map(splitKey)
              // Build header rows for currently visible columns
              const rows: Array<Array<{ text: string; colSpan: number; path: string }>> = []
              for (let level = 0; level < colDims.length; level++) {
                const r: Array<{ text: string; colSpan: number; path: string }> = []
                let i = 0
                while (i < partsV.length) {
                  const val = partsV[i]?.[level] ?? ''
                  const basePrefix = (partsV[i] || []).slice(0, level).join('|')
                  let j = i + 1
                  // group until label changes OR parent prefix changes
                  while (j < partsV.length && (partsV[j]?.[level] ?? '') === val && ((partsV[j] || []).slice(0, level).join('|') === basePrefix)) j++
                  const path = (partsV[i] || []).slice(0, level + 1).join('|')
                  r.push({ text: val, colSpan: Math.max(1, j - i), path })
                  i = j
                }
                rows.push(r)
              }
              return rows.map((cells, rowIdx) => (
                <tr key={`ch-${rowIdx}`}>
                  {rowDims.map((d, i) => (
                    rowIdx === 0 ? (
                      <th
                        key={`rhp-${rowIdx}-${i}`}
                        rowSpan={Math.max(1, colDims.length)}
                        className={`${headerTextAlignClass} ${headerValignClass} border px-2 font-semibold bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)] cursor-pointer select-none`}
                        style={{ height: headerHeight, fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any, filter: rowHeaderDepthHue ? `hue-rotate(${i*16}deg) saturate(1.25)` : undefined }}
                        onClick={() => onRowHeaderClick(i)}
                      >
                        <div className="relative">
                          <span className="block">{d}</span>
                          {(() => {
                            const mode = headerSort[i]
                            if (!mode || mode==='none') return null
                            return <span className="absolute right-0 top-1/2 -translate-y-1/2 text-[10px] opacity-70">{mode==='asc' ? '▲' : '▼'}</span>
                          })()}
                        </div>
                      </th>
                    ) : null
                  ))}
                  {cells.map((c: { text: string; colSpan: number; path: string }, i: number) => {
                    const showToggle = splitKey(c.path).length < colDims.length
                    const ancestorCollapsed = (() => {
                      const parts = splitKey(c.path)
                      for (let k = 1; k < parts.length; k++) { if (colCollapsed.has(parts.slice(0, k).join('|'))) return true }
                      return false
                    })()
                    // last row indicates value columns
                    // If parent at this level is collapsed, blank child label
                    const hereParts = splitKey(c.path)
                    const parentPath = hereParts.slice(0, Math.max(0, rowIdx)).join('|')
                    const underCollapsedParent = !!(parentPath && colCollapsed.has(parentPath))
                    const label = underCollapsedParent && rowIdx > 0 ? '' : c.text
                    return (
                      <th
                        key={`chc-${rowIdx}-${i}`}
                        className={`${headerTextAlignClass} ${headerValignClass} border px-2 font-semibold bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)]`}
                        style={{ height: headerHeight, fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any, transformOrigin: 'left', willChange: 'transform, opacity', filter: colHeaderDepthHue ? `hue-rotate(${rowIdx*16}deg) saturate(1.25)` : undefined }}
                        colSpan={c.colSpan}
                      >
                        <div className="relative">
                          {showToggle && !ancestorCollapsed && (
                            <button data-pvt-toggle="1" className={`absolute left-0 top-1/2 -translate-y-1/2 text-xs px-1 py-0.5 rounded ${showIconOutline ? 'border' : 'border-0'} hover:bg-muted`} onClick={(e) => { e.stopPropagation(); toggleCol(c.path) }} title={isColCollapsed(c.path) ? 'Expand' : 'Collapse'}>
                              <ToggleIcon open={!isColCollapsed(c.path)} />
                            </button>
                          )}
                          <span className="block cursor-pointer select-none" onClick={() => onColHeaderClick(c.path)}>{label}</span>
                          {(() => {
                            const active = valueSort.colKey===c.path && valueSort.mode!=='none'
                            if (!active) return null
                            return <span className="absolute right-0 top-1/2 -translate-y-1/2 text-[10px] opacity-70">{valueSort.mode==='asc' ? '▲' : '▼'}</span>
                          })()}
                        </div>
                      </th>
                    )
                  })}
                  {showRowTotals && (
                    rowIdx === 0 ? (
                      <th
                        rowSpan={Math.max(1, colDims.length)}
                        className={`${headerTextAlignClass} ${headerValignClass} border px-2 font-semibold bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)] cursor-pointer select-none`}
                        style={{ height: headerHeight, fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any }}
                        onClick={() => onColHeaderClick('__TOTAL__')}
                      >
                        <div className="relative">
                          <span>Total</span>
                          {(() => {
                            const active = valueSort.colKey==='__TOTAL__' && valueSort.mode!=='none'
                            if (!active) return null
                            return <span className="absolute right-0 top-1/2 -translate-y-1/2 text-[10px] opacity-70">{valueSort.mode==='asc' ? '▲' : '▼'}</span>
                          })()}
                        </div>
                      </th>
                    ) : null
                  )}
                </tr>
              ))
            })()
          ) : (
            // If no column dims, render a single header row for values
            <tr>
              {rowDims.map((d, i) => (
                <th
                  key={`rh-${i}`}
                  className={`${headerTextAlignClass} ${headerValignClass} border px-2 font-semibold bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)] cursor-pointer select-none`}
                  style={{ height: headerHeight, fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any }}
                  onClick={() => onRowHeaderClick(i)}
                >
                  <div className="relative">
                    <span className="block">{d}</span>
                    {(() => {
                      const mode = headerSort[i]
                      if (!mode || mode==='none') return null
                      return <span className="absolute right-0 top-1/2 -translate-y-1/2 text-[10px] opacity-70">{mode==='asc' ? '▲' : '▼'}</span>
                    })()}
                  </div>
                </th>
              ))}
              <th
                className={`${headerTextAlignClass} ${headerValignClass} border px-2 font-semibold bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)] cursor-pointer select-none`}
                style={{ height: headerHeight, fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any }}
                onClick={() => onColHeaderClick('__no_key__')}
              >
                <div className="relative">
                  <span>{valueLabel}</span>
                  {(() => {
                    const active = valueSort.colKey==='__no_key__' && valueSort.mode!=='none'
                    if (!active) return null
                    return <span className="absolute right-0 top-1/2 -translate-y-1/2 text-[10px] opacity-70">{valueSort.mode==='asc' ? '▲' : '▼'}</span>
                  })()}
                </div>
              </th>
              {showRowTotals && (
                <th
                  className={`${headerTextAlignClass} ${headerValignClass} border px-2 font-semibold bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)] cursor-pointer select-none`}
                  style={{ height: headerHeight, fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any }}
                  onClick={() => onColHeaderClick('__TOTAL__')}
                >
                  <div className="relative">
                    <span>Total</span>
                    {(() => {
                      const active = valueSort.colKey==='__TOTAL__' && valueSort.mode!=='none'
                      if (!active) return null
                      return <span className="absolute right-0 top-1/2 -translate-y-1/2 text-[10px] opacity-70">{valueSort.mode==='asc' ? '▲' : '▼'}</span>
                    })()}
                  </div>
                </th>
              )}
            </tr>
          )}
        </thead>
        <tbody>
          {visibleRowKeys.length === 0 ? (
            <tr>
              <td colSpan={Math.max(1, rowDims.length) + Math.max(1, visColLeaves.length) + (showRowTotals ? 1 : 0)} className="text-sm text-muted-foreground text-center py-6">No data</td>
            </tr>
          ) : (
            visibleRowKeys.map((rk: string, rowIdx: number) => {
              const parts = splitKey(rk)
              const closing = isUnderPrefix(rk, closingPrefix)
              const opening = isUnderPrefix(rk, openingPrefix)
              const repForClosing = closingPrefix ? (repByPrefix.get(closingPrefix) || firstLeafForPrefix.get(closingPrefix!)) : null
              const isLeafRow = parts.length === rowDims.length
              const rowBase = `
                ${rowHover ? 'hover:bg-blue-50 dark:hover:bg-[hsl(var(--muted)/0.38)] transition-colors' : ''}
              `
              const cellBgClass = `
                ${altRows && (rowIdx % 2 === 1) ? 'bg-[hsl(var(--muted)/0.55)] dark:bg-[hsl(var(--secondary)/0.22)]' : 'bg-transparent'}
                ${rowHover ? 'group-hover:bg-blue-50 dark:group-hover:bg-[hsl(var(--muted)/0.38)] transition-colors' : ''}
              `
              return (
                <React.Fragment key={`rk-${rowIdx}-${rk}`}>
                <tr
                  key={`r-${rowIdx}`}
                  style={{
                    overflow: (closing || opening) ? 'hidden' as const : undefined,
                    animation: closing && rk !== repForClosing ? `pvtRollUp 300ms ease-in forwards` : (opening ? `pvtRollDown 300ms ease-out forwards` : undefined),
                    transformOrigin: 'top', willChange: 'transform, opacity', backfaceVisibility: 'hidden', WebkitFontSmoothing: 'antialiased',
                  }}
                  className={`group ${rowBase}`}
                >
                  {rowDims.map((_: string, level: number) => (
                    rowHeaderSpans[level][rowIdx] > 0 ? (
                      <th
                        key={`rh-${rowIdx}-${level}`}
                        rowSpan={rowHeaderSpans[level][rowIdx]}
                        className={`${headerTextAlignClass} ${headerValignClass} border px-2 font-semibold bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)]`}
                        style={{ height: rowHeight, fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any, filter: rowHeaderDepthHue ? `hue-rotate(${level*16}deg) saturate(1.25)` : undefined }}
                        onClick={() => onRowHeaderClick(level)}
                      >
                        <div className={`relative flex h-full w-full ${headerVFlexClass} ${headerHJustifyClass} gap-1`}>
                          {(() => {
                            const prefix = parts.slice(0, level + 1).join('|')
                            const showToggle = prefixesWithChildren.has(prefix)
                            // If any ancestor is collapsed, hide deeper-level toggle
                            const ancestorCollapsed = (() => {
                              for (let i = 0; i < level; i++) {
                                const p = parts.slice(0, i + 1).join('|')
                                if (isCollapsed(p)) return true
                              }
                              return false
                            })()
                            if (!showToggle || ancestorCollapsed) return null
                            return (
                              <button
                                className={`text-xs px-1 py-0.5 rounded ${showIconOutline ? 'border' : 'border-0'} hover:bg-muted flex-shrink-0`}
                                onClick={(e) => { e.stopPropagation(); toggle(prefix, rk) }}
                                title={isCollapsed(prefix) ? 'Expand' : 'Collapse'}
                              >
                                <ToggleIcon open={!isCollapsed(prefix)} />
                              </button>
                            )
                          })()}
                          {(() => {
                            // Hide child labels when an ancestor is collapsed to avoid misleading values
                            const ancestorCollapsed = (() => {
                              for (let i = 0; i < level; i++) {
                                const p = parts.slice(0, i + 1).join('|')
                                if (isCollapsed(p)) return true
                              }
                              return false
                            })()
                            return ancestorCollapsed ? null : (
                              <span className={`block cursor-pointer select-none ${headerTextAlignClass}`}>
                                {parts[level] || ''}
                              </span>
                            )
                          })()}
                        </div>
                      </th>
                    ) : null
                  ))}
                  {colDims.length === 0 ? (
                    <td className={`${cellTextAlignClass} ${cellValignClass} border px-2 ${cellBgClass}`} style={{ height: rowHeight }}>
                      <span style={{ fontSize: cellFontSize, fontStyle: cellFontStyle as any, fontWeight: cellFontWeight as any }}>{formatNumber(valueAt(rk, '__no_key__'))}</span>
                    </td>
                  ) : (
                    colLeaves.map((ck: string, i: number) => {
                      // honor column collapse: show only representative child when parent is collapsed
                      const partsC = splitKey(ck)
                      let collapsedUnder: string | null = null
                      for (let k = partsC.length - 1; k >= 1; k--) {
                        const pref = partsC.slice(0, k).join('|')
                        if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                      }
                      if (collapsedUnder) {
                        const first = colLeaves.find(c => splitKey(c).slice(0, splitKey(collapsedUnder!).length).join('|') === collapsedUnder)
                        const rep = colRepByPrefix.get(collapsedUnder) || first
                        if (rep !== ck) return null
                      }
                      const colClosing = (() => {
                        if (!closingColPrefix) return false
                        const p = splitKey(ck)
                        const c = splitKey(closingColPrefix)
                        for (let i=0;i<c.length;i++){ if (p[i]!==c[i]) return false }
                        return true
                      })()
                      const colOpening = (() => {
                        if (!openingColPrefix) return false
                        const p = splitKey(ck)
                        const c = splitKey(openingColPrefix)
                        for (let i=0;i<c.length;i++){ if (p[i]!==c[i]) return false }
                        return true
                      })()
                      return (
                        <td key={`c-${rowIdx}-${i}`} className={`${cellTextAlignClass} ${cellValignClass} border px-2 ${cellBgClass}`} style={{ height: rowHeight, fontSize: cellFontSize, fontStyle: cellFontStyle as any, fontWeight: cellFontWeight as any, overflow: (colClosing||colOpening)?'hidden':undefined, animation: (colClosing ? `pvtColRollOut 280ms ease-in forwards` : (colOpening ? `pvtColRollIn 280ms ease-out forwards` : undefined)), transformOrigin: 'left', willChange: 'transform, opacity' }}>
                          {(() => {
                            // If this column is a representative for a collapsed parent, show the parent's subtotal
                            let collapsedUnder: string | null = null
                            for (let k = partsC.length - 1; k >= 1; k--) {
                              const pref = partsC.slice(0, k).join('|')
                              if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                            }
                            const ckAgg = collapsedUnder || ck
                            return formatNumber(valueAt(rk, ckAgg))
                          })()}
                        </td>
                      )
                    })
                  )}
                  {showRowTotals && (
                    <td className={`${cellTextAlignClass} ${cellValignClass} font-semibold border px-2 bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)]`} style={{ height: rowHeight, fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any }}>
                      {(() => {
                        if (colDims.length === 0) {
                          const n = Number(valueAt(rk, '__no_key__') || 0)
                          return Number.isFinite(n) ? formatNumber(n) : '0'
                        }
                        let s = 0
                        for (const ck of colLeaves) {
                          // respect collapsed parents by summing once per visible representative
                          const partsC = splitKey(ck)
                          let collapsedUnder: string | null = null
                          for (let k = partsC.length - 1; k >= 1; k--) {
                            const pref = partsC.slice(0, k).join('|')
                            if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                          }
                          if (collapsedUnder) {
                            const first = colLeaves.find(c => splitKey(c).slice(0, splitKey(collapsedUnder!).length).join('|') === collapsedUnder)
                            const rep = colRepByPrefix.get(collapsedUnder!) || first
                            if (rep !== ck) continue
                          }
                          const ckAgg = collapsedUnder || ck
                          s += valueAt(rk, ckAgg)
                        }
                        const n = Number(s || 0)
                        return Number.isFinite(n) ? formatNumber(n) : '0'
                      })()}
                    </td>
                  )}
                </tr>
                {(() => {
                  if (!showSubtotals || rowDims.length < 2) return null
                  const parentLen = rowDims.length - 1
                  const parts2 = splitKey(rk)
                  const curParent = parts2.slice(0, parentLen).join('|')
                  const next = visibleRowKeys[rowIdx + 1]
                  const nextParent = next ? splitKey(next).slice(0, parentLen).join('|') : null
                  if (!curParent || curParent === nextParent) return null
                  // compute visible columns under collapse
                  const partsCols = colLeaves.map(splitKey)
                  const visibleCols: string[] = []
                  for (let idx = 0; idx < partsCols.length; idx++) {
                    const ck = colLeaves[idx]
                    const bits = partsCols[idx]
                    let collapsedUnder: string | null = null
                    for (let i = bits.length - 1; i >= 1; i--) {
                      const pref = bits.slice(0, i).join('|')
                      if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                    }
                    if (!collapsedUnder) { visibleCols.push(ck); continue }
                    const first = colLeaves.find(c => splitKey(c).slice(0, splitKey(collapsedUnder!).length).join('|') === collapsedUnder)
                    const rep = colRepByPrefix.get(collapsedUnder) || first
                    if (rep === ck) visibleCols.push(ck)
                  }
                  const childKeys = visibleRowKeys.filter((k: string) => splitKey(k).slice(0, parentLen).join('|') === curParent)
                  const subtotalFor = (ck: string) => childKeys.reduce((acc: number, r2: string) => acc + valueAt(r2, ck), 0)
                  return (
                    <>
                      <tr>
                        {rowDims.map((_, level) => (
                          <th key={`sub-${rowIdx}-${level}`} className={`${headerTextAlignClass} ${headerValignClass} border px-2 font-medium bg-gradient-to-r from-[hsl(var(--secondary)/0.2)] to-[hsl(var(--secondary)/0.1)]`} style={{ height: rowHeight, fontSize: cellFontSize, fontStyle: cellFontStyle as any, fontWeight: cellFontWeight as any }}>
                            {level === parentLen - 1 ? 'Subtotal' : ''}
                          </th>
                        ))}
                        {colDims.length === 0 ? (
                          <td className={`${cellTextAlignClass} ${cellValignClass} border px-2`} style={{ height: rowHeight, fontSize: cellFontSize, fontStyle: cellFontStyle as any, fontWeight: cellFontWeight as any }}>{formatNumber(subtotalFor('__no_key__'))}</td>
                        ) : (
                          visibleCols.map((ck: string, i: number) => {
                            const partsC = splitKey(ck)
                            let collapsedUnder: string | null = null
                            for (let k = partsC.length - 1; k >= 1; k--) {
                              const pref = partsC.slice(0, k).join('|')
                              if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                            }
                            const ckAgg = collapsedUnder || ck
                            return (
                              <td key={`subs-${rowIdx}-${i}`} className={`${cellTextAlignClass} ${cellValignClass} border px-2`} style={{ height: rowHeight, fontSize: cellFontSize, fontStyle: cellFontStyle as any, fontWeight: cellFontWeight as any }}>{formatNumber(subtotalFor(ckAgg))}</td>
                            )
                          })
                        )}
                        {showRowTotals && (
                          <td className={`${cellTextAlignClass} ${cellValignClass} font-semibold border px-2 bg-gradient-to-b from-[hsl(var(--muted)/0.25)] to-[hsl(var(--muted)/0.1)]`} style={{ height: rowHeight, fontSize: cellFontSize, fontStyle: cellFontStyle as any, fontWeight: cellFontWeight as any }}>
                            {(() => {
                              let s = 0
                              if (colDims.length === 0) s = subtotalFor('__no_key__')
                              else for (const ck of visibleCols) s += subtotalFor(ck)
                              return formatNumber(s)
                            })()}
                          </td>
                        )}
                      </tr>
                      <tr><td colSpan={Math.max(1, rowDims.length) + (colDims.length === 0 ? 1 : visibleCols.length) + (showRowTotals ? 1 : 0)} className="h-[6px]" /></tr>
                    </>
                  )
                })()}
                </React.Fragment>
              )
            })
          )}
          {/* Column totals row */}
          {showColTotals && (
            <tr className="bg-gradient-to-r from-[hsl(var(--muted)/0.85)] to-[hsl(var(--muted)/0.6)]">
              {/* Row header total label */}
              {rowDims.length > 0 ? (
                <th colSpan={rowDims.length} className="text-left border px-2 font-semibold">Total</th>
              ) : null}
              {colDims.length === 0 ? (
                <td className={`${cellTextAlignClass} ${cellValignClass} font-semibold border px-2 bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)]`} style={{ fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any }}>
                  {(() => {
                    // Grand total when no columns
                    let s = 0
                    const rks = visibleRowKeys
                    for (const rk of rks) s += valueAt(rk, '__no_key__')
                    return formatNumber(s)
                  })()}
                </td>
              ) : (
                <>
                  {(() => {
                    // compute currently visible column leaves under collapse
                    const parts = colLeaves.map(splitKey)
                    const visible: string[] = []
                    for (let idx = 0; idx < parts.length; idx++) {
                      const ck = colLeaves[idx]
                      const bits = parts[idx]
                      let collapsedUnder: string | null = null
                      for (let i = bits.length - 1; i >= 1; i--) {
                        const pref = bits.slice(0, i).join('|')
                        if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                      }
                      if (!collapsedUnder) { visible.push(ck); continue }
                      const first = colLeaves.find(c => splitKey(c).slice(0, splitKey(collapsedUnder!).length).join('|') === collapsedUnder)
                      const rep = colRepByPrefix.get(collapsedUnder!) || first
                      if (rep === ck) visible.push(ck)
                    }
                    return visible.map((ck: string, i: number) => {
                      const partsC = splitKey(ck)
                      let collapsedUnder: string | null = null
                      for (let k = partsC.length - 1; k >= 1; k--) {
                        const pref = partsC.slice(0, k).join('|')
                        if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                      }
                      const ckAgg = collapsedUnder || ck
                      return (
                        <td key={`totc-${i}`} className={`${cellTextAlignClass} ${cellValignClass} font-semibold border px-2 bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)]`} style={{ fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any }}>
                          {(() => {
                            let s = 0
                            for (const rk of visibleRowKeys) s += valueAt(rk, ckAgg)
                            return formatNumber(s)
                          })()}
                        </td>
                      )
                    })
                  })()}
                  {showRowTotals && (
                    <td className={`${cellTextAlignClass} ${cellValignClass} font-bold border px-2 bg-gradient-to-b from-[hsl(var(--muted)/0.9)] to-[hsl(var(--muted)/0.6)]`} style={{ fontSize: headerFontSize, fontStyle: headerFontStyle as any, fontWeight: headerFontWeight as any }}>
                      {(() => {
                        let s = 0
                        // sum across currently visible columns only
                        const parts = colLeaves.map(splitKey)
                        const visible: string[] = []
                        for (let idx = 0; idx < parts.length; idx++) {
                          const ck = colLeaves[idx]
                          const bits = parts[idx]
                          let collapsedUnder: string | null = null
                          for (let i = bits.length - 1; i >= 1; i--) {
                            const pref = bits.slice(0, i).join('|')
                            if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                          }
                          if (!collapsedUnder) { visible.push(ck); continue }
                          const first = colLeaves.find(c => splitKey(c).slice(0, splitKey(collapsedUnder!).length).join('|') === collapsedUnder)
                          const rep = colRepByPrefix.get(collapsedUnder!) || first
                          if (rep === ck) visible.push(ck)
                        }
                        for (const rk of visibleRowKeys) for (const ck of visible) {
                          const partsC = splitKey(ck)
                          let collapsedUnder: string | null = null
                          for (let k = partsC.length - 1; k >= 1; k--) {
                            const pref = partsC.slice(0, k).join('|')
                            if (colCollapsed.has(pref)) { collapsedUnder = pref; break }
                          }
                          const ckAgg = collapsedUnder || ck
                          s += valueAt(rk, ckAgg)
                        }
                        return formatNumber(s)
                      })()}
                    </td>
                  )}
                </>
              )}
            </tr>
          )}
        </tbody>
      </table>
    </div>
  )
}
