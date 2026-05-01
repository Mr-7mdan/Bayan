"use client"

import React, { useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import {
  RiCloseLine,
  RiArrowUpLine,
  RiArrowDownLine,
  RiAddLine,
  RiDeleteBinLine,
  RiMergeCellsHorizontal,
  RiSplitCellsHorizontal,
  RiDraggable,
  RiInformationLine,
  RiTimeLine,
} from '@remixicon/react'
import type { ReportElement, ReportTableCell, ReportVariable } from '@/types/widgets'

type TableT = NonNullable<ReportElement['table']>
type RowStyle = NonNullable<TableT['rowStyles']>[number]

type Props = {
  open: boolean
  table: TableT
  variables: ReportVariable[]
  onCloseAction: () => void
  onChangeAction: (next: TableT) => void
}

type Sel = { r1: number; c1: number; r2: number; c2: number } | null

const BORDER_STYLES = ['solid', 'dashed', 'dotted', 'double', 'none'] as const

// ── Geometry helpers ──────────────────────────────────────────────────
function normRect(s: NonNullable<Sel>) {
  return {
    r1: Math.min(s.r1, s.r2),
    r2: Math.max(s.r1, s.r2),
    c1: Math.min(s.c1, s.c2),
    c2: Math.max(s.c1, s.c2),
  }
}

function actualColsFromHeaders(headers: TableT['headers']): number {
  return headers.reduce((sum, h) => {
    const hd = typeof h === 'string' ? { colspan: 1 } : h
    return sum + (hd.colspan || 1)
  }, 0)
}

// ── Merge engine ──────────────────────────────────────────────────────
function applyMerge(
  cells: ReportTableCell[][],
  rect: { r1: number; r2: number; c1: number; c2: number },
): ReportTableCell[][] {
  const w = rect.c2 - rect.c1 + 1
  const h = rect.r2 - rect.r1 + 1
  if (w === 1 && h === 1) return cells
  return cells.map((row, r) =>
    row.map((cell, c) => {
      if (r === rect.r1 && c === rect.c1) {
        return { ...cell, colspan: w, rowspan: h, _merged: false }
      }
      if (r >= rect.r1 && r <= rect.r2 && c >= rect.c1 && c <= rect.c2) {
        return { ...cell, _merged: true }
      }
      return cell
    }),
  )
}

function applyUnmergeAt(
  cells: ReportTableCell[][],
  r: number,
  c: number,
): ReportTableCell[][] {
  const anchor = findAnchor(cells, r, c)
  if (!anchor) return cells
  const w = cells[anchor.r][anchor.c].colspan || 1
  const h = cells[anchor.r][anchor.c].rowspan || 1
  if (w <= 1 && h <= 1) return cells
  return cells.map((row, rr) =>
    row.map((cell, cc) => {
      if (rr === anchor.r && cc === anchor.c) {
        const { colspan: _cs, rowspan: _rs, _merged: _mg, ...rest } = cell
        return rest
      }
      if (rr >= anchor.r && rr < anchor.r + h && cc >= anchor.c && cc < anchor.c + w) {
        const { _merged: _mg, ...rest } = cell
        return rest
      }
      return cell
    }),
  )
}

function findAnchor(
  cells: ReportTableCell[][],
  r: number,
  c: number,
): { r: number; c: number } | null {
  for (let rr = 0; rr <= r; rr++) {
    for (let cc = 0; cc <= c; cc++) {
      const cell = cells[rr]?.[cc]
      if (!cell) continue
      const w = cell.colspan || 1
      const h = cell.rowspan || 1
      if (w > 1 || h > 1) {
        if (r >= rr && r < rr + h && c >= cc && c < cc + w) {
          return { r: rr, c: cc }
        }
      }
    }
  }
  return null
}

function buildCoveredSet(cells: ReportTableCell[][]): Set<string> {
  const out = new Set<string>()
  for (let rr = 0; rr < cells.length; rr++) {
    for (let cc = 0; cc < cells[rr].length; cc++) {
      const cell = cells[rr][cc]
      const w = cell.colspan || 1
      const h = cell.rowspan || 1
      if (w > 1 || h > 1) {
        for (let dr = 0; dr < h; dr++) {
          for (let dc = 0; dc < w; dc++) {
            if (dr === 0 && dc === 0) continue
            out.add(`${rr + dr},${cc + dc}`)
          }
        }
      }
    }
  }
  return out
}

// ── Cell preview rendering (read-only) ──────────────────────────────
function CellPreview({
  cell,
  variables,
}: {
  cell: ReportTableCell
  variables: ReportVariable[]
}) {
  if (cell.type === 'spaceholder') {
    const v = variables.find(vv => vv.id === cell.variableId)
    const label = v?.name || (cell.variableId ? '?' : '—')
    return (
      <span
        className="inline-flex items-center px-1.5 py-0.5 rounded font-mono text-[10px] bg-[hsl(var(--primary)/0.1)] text-[hsl(var(--primary))] truncate max-w-full"
        title={v ? `Variable: ${v.name}` : 'Unassigned variable'}
      >
        {`{{${label}}}`}
      </span>
    )
  }
  if (cell.type === 'period') {
    return (
      <span
        className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded font-mono text-[10px] bg-[hsl(var(--secondary))] text-muted-foreground truncate max-w-full"
        title={`Period: ${cell.datetimeExpr || '—'}`}
      >
        <RiTimeLine className="h-2.5 w-2.5 shrink-0" />
        {cell.datetimeExpr || '—'}
      </span>
    )
  }
  // Plain text — show truncated content or a muted dash
  const text = (cell.text || '').trim()
  if (!text) {
    return <span className="text-muted-foreground/50 text-[10px] italic">empty</span>
  }
  return (
    <span className="text-[11px] truncate max-w-full" title={text}>
      {text}
    </span>
  )
}

export default function TableCellsEditorModal({
  open,
  table,
  variables,
  onCloseAction,
  onChangeAction,
}: Props) {
  // ── State ────────────────────────────────────────────────────────────
  const [tbl, setTbl] = useState(table)
  useEffect(() => { if (open) setTbl(table) }, [open, table])

  const [sel, setSel] = useState<Sel>(null)
  const [dragging, setDragging] = useState(false)

  const totalCols = useMemo(() => actualColsFromHeaders(tbl.headers), [tbl.headers])
  const covered = useMemo(() => buildCoveredSet(tbl.cells), [tbl.cells])
  const mergedRegionsCount = useMemo(() => {
    let n = 0
    for (const row of tbl.cells) {
      for (const cell of row) {
        const w = cell.colspan || 1
        const h = cell.rowspan || 1
        if (w > 1 || h > 1) n++
      }
    }
    return n
  }, [tbl.cells])

  // Self-heal row width if it ever drifts from totalCols
  useEffect(() => {
    const cells = tbl.cells
    let needsFix = false
    for (const row of cells) {
      if (row.length !== totalCols) { needsFix = true; break }
    }
    if (!needsFix) return
    const fixed = cells.map(row => {
      if (row.length === totalCols) return row
      if (row.length < totalCols) {
        const pad = Array.from({ length: totalCols - row.length }, () => ({ type: 'text' as const, text: '' }))
        return [...row, ...pad]
      }
      return row.slice(0, totalCols)
    })
    setTbl(t => ({ ...t, cells: fixed }))
  }, [totalCols])

  // ── Mutations (commit immediately to parent) ─────────────────────────
  const commit = (next: TableT) => { setTbl(next); onChangeAction(next) }

  const swapRows = (a: number, b: number) => {
    if (a === b || a < 0 || b < 0 || a >= tbl.cells.length || b >= tbl.cells.length) return
    const cells = tbl.cells.slice()
    ;[cells[a], cells[b]] = [cells[b], cells[a]]
    let rowStyles = tbl.rowStyles
    if (rowStyles) {
      rowStyles = rowStyles.slice()
      const sa = rowStyles[a] || {}
      const sb = rowStyles[b] || {}
      rowStyles[a] = sb
      rowStyles[b] = sa
    }
    commit({ ...tbl, cells, ...(rowStyles ? { rowStyles } : {}) })
  }

  const moveRowUp = (r: number) => swapRows(r, r - 1)
  const moveRowDown = (r: number) => swapRows(r, r + 1)

  const insertRowAt = (atIdx: number) => {
    const newRow: ReportTableCell[] = Array.from({ length: totalCols }, () => ({ type: 'text' as const, text: '' }))
    const cells = [...tbl.cells.slice(0, atIdx), newRow, ...tbl.cells.slice(atIdx)]
    const rowStyles = tbl.rowStyles
      ? [...tbl.rowStyles.slice(0, atIdx), {}, ...tbl.rowStyles.slice(atIdx)]
      : undefined
    commit({ ...tbl, rows: tbl.rows + 1, cells, ...(rowStyles ? { rowStyles } : {}) })
  }

  const deleteRow = (r: number) => {
    if (tbl.rows <= 1) return
    const cells = tbl.cells.filter((_, i) => i !== r)
    const rowStyles = tbl.rowStyles ? tbl.rowStyles.filter((_, i) => i !== r) : undefined
    commit({ ...tbl, rows: tbl.rows - 1, cells, ...(rowStyles ? { rowStyles } : {}) })
    setSel(null)
  }

  // ── Selection ────────────────────────────────────────────────────────
  const startSel = (r: number, c: number, e: React.MouseEvent) => {
    if (e.shiftKey && sel) {
      setSel({ r1: sel.r1, c1: sel.c1, r2: r, c2: c })
    } else {
      setSel({ r1: r, c1: c, r2: r, c2: c })
    }
    setDragging(true)
  }
  const extendSel = (r: number, c: number) => {
    if (!dragging || !sel) return
    setSel({ ...sel, r2: r, c2: c })
  }
  const endSel = () => setDragging(false)

  const selectRow = (r: number) => setSel({ r1: r, c1: 0, r2: r, c2: totalCols - 1 })
  const selectAll = () => setSel({ r1: 0, c1: 0, r2: tbl.rows - 1, c2: totalCols - 1 })

  const isInSel = (r: number, c: number) => {
    if (!sel) return false
    const n = normRect(sel)
    return r >= n.r1 && r <= n.r2 && c >= n.c1 && c <= n.c2
  }

  const selSize = useMemo(() => {
    if (!sel) return { w: 0, h: 0 }
    const n = normRect(sel)
    return { w: n.c2 - n.c1 + 1, h: n.r2 - n.r1 + 1 }
  }, [sel])

  // ── Merge actions ────────────────────────────────────────────────────
  const canMerge = sel && (selSize.w > 1 || selSize.h > 1)
  const canUnmerge = (() => {
    if (!sel) return false
    const n = normRect(sel)
    for (let r = n.r1; r <= n.r2; r++) {
      for (let c = n.c1; c <= n.c2; c++) {
        const cell = tbl.cells[r]?.[c]
        if (!cell) continue
        if ((cell.colspan || 1) > 1 || (cell.rowspan || 1) > 1) return true
        if (cell._merged) return true
      }
    }
    return false
  })()

  const doMerge = () => {
    if (!sel || (selSize.w === 1 && selSize.h === 1)) return
    const n = normRect(sel)
    let cells = tbl.cells
    for (let r = n.r1; r <= n.r2; r++) {
      for (let c = n.c1; c <= n.c2; c++) cells = applyUnmergeAt(cells, r, c)
    }
    cells = applyMerge(cells, n)
    commit({ ...tbl, cells })
  }

  const doUnmerge = () => {
    if (!sel) return
    const n = normRect(sel)
    let cells = tbl.cells
    for (let r = n.r1; r <= n.r2; r++) {
      for (let c = n.c1; c <= n.c2; c++) cells = applyUnmergeAt(cells, r, c)
    }
    commit({ ...tbl, cells })
  }

  const doMergeHorizontal = () => {
    if (!sel) return
    const n = normRect(sel)
    let cells = tbl.cells
    for (let r = n.r1; r <= n.r2; r++) {
      for (let c = n.c1; c <= n.c2; c++) cells = applyUnmergeAt(cells, r, c)
      cells = applyMerge(cells, { r1: r, r2: r, c1: n.c1, c2: n.c2 })
    }
    commit({ ...tbl, cells })
  }

  const doMergeVertical = () => {
    if (!sel) return
    const n = normRect(sel)
    let cells = tbl.cells
    for (let c = n.c1; c <= n.c2; c++) {
      for (let r = n.r1; r <= n.r2; r++) cells = applyUnmergeAt(cells, r, c)
      cells = applyMerge(cells, { r1: n.r1, r2: n.r2, c1: c, c2: c })
    }
    commit({ ...tbl, cells })
  }

  // ── Keyboard shortcuts ───────────────────────────────────────────────
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      // Don't intercept when typing in an input/textarea inside the modal
      const target = e.target as HTMLElement | null
      if (target && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA')) return
      if (e.key === 'Escape') { onCloseAction(); return }
      const meta = e.metaKey || e.ctrlKey
      if (meta && e.key.toLowerCase() === 'm') {
        e.preventDefault()
        if (e.shiftKey) doUnmerge()
        else doMerge()
        return
      }
      if (meta && e.key.toLowerCase() === 'a') {
        e.preventDefault()
        selectAll()
        return
      }
      // Arrow extends selection when shift is held
      if (sel && e.shiftKey && (e.key === 'ArrowUp' || e.key === 'ArrowDown' || e.key === 'ArrowLeft' || e.key === 'ArrowRight')) {
        e.preventDefault()
        const dr = e.key === 'ArrowDown' ? 1 : e.key === 'ArrowUp' ? -1 : 0
        const dc = e.key === 'ArrowRight' ? 1 : e.key === 'ArrowLeft' ? -1 : 0
        const r2 = Math.max(0, Math.min(tbl.rows - 1, sel.r2 + dr))
        const c2 = Math.max(0, Math.min(totalCols - 1, sel.c2 + dc))
        setSel({ ...sel, r2, c2 })
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, sel, tbl.rows, totalCols, doMerge, doUnmerge, onCloseAction])

  if (!open || typeof document === 'undefined') return null

  // ── Column header labels (one per actual sub-column) ─────────────────
  const colHeaderLabels: string[] = (() => {
    const out: string[] = []
    let absC = 0
    for (const h of tbl.headers) {
      const hd = typeof h === 'string' ? { text: h, colspan: 1 } : h
      const span = hd.colspan || 1
      for (let i = 0; i < span; i++) {
        if (tbl.subheaders && tbl.subheaders[absC]) {
          out.push(tbl.subheaders[absC] || `${hd.text}`)
        } else {
          out.push(span > 1 ? `${hd.text} ${i + 1}` : hd.text || `Col ${absC + 1}`)
        }
        absC++
      }
    }
    return out
  })()

  return createPortal(
    <div className="fixed inset-0 z-[1300]" role="dialog" aria-modal="true" aria-label="Table formatter">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/55 backdrop-blur-sm"
        onClick={onCloseAction}
      />
      {/* Modal frame */}
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[1240px] max-w-[98vw] h-[88vh] flex flex-col rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))] shadow-2xl overflow-hidden">
        {/* ── Header ───────────────────────────────────────────────────── */}
        <header className="flex items-start justify-between px-5 py-3 border-b border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="min-w-0">
            <h2 className="text-sm font-semibold tracking-tight">Table Formatter</h2>
            <p className="text-[11px] text-muted-foreground mt-0.5">
              Format rows, merge cells, and reorder. Edit cell content from the table preview in the main view.
            </p>
          </div>
          <button
            type="button"
            className="text-muted-foreground hover:text-foreground p-1.5 rounded-md hover:bg-[hsl(var(--secondary)/0.6)] transition-colors duration-150 cursor-pointer focus:outline-none focus-visible:ring-2 focus-visible:ring-[hsl(var(--primary))]"
            onClick={onCloseAction}
            aria-label="Close (Esc)"
            title="Close (Esc)"
          >
            <RiCloseLine className="h-5 w-5" />
          </button>
        </header>

        {/* ── Toolbar ──────────────────────────────────────────────────── */}
        <div className="flex items-center gap-3 px-4 py-2 border-b border-[hsl(var(--border))] bg-[hsl(var(--secondary)/0.25)] flex-wrap">
          {/* Merge group */}
          <ToolbarGroup label="Merge">
            <ToolbarButton onClick={doMerge} disabled={!canMerge} title="Merge selection (⌘M)" icon={<RiMergeCellsHorizontal className="h-3.5 w-3.5" />}>
              Merge
            </ToolbarButton>
            <ToolbarButton onClick={doMergeHorizontal} disabled={!sel || selSize.w <= 1} title="Merge each row in the selection horizontally">
              <span className="font-mono">↔</span> Rows
            </ToolbarButton>
            <ToolbarButton onClick={doMergeVertical} disabled={!sel || selSize.h <= 1} title="Merge each column in the selection vertically">
              <span className="font-mono">↕</span> Columns
            </ToolbarButton>
            <ToolbarButton onClick={doUnmerge} disabled={!canUnmerge} title="Unmerge selection (⌘⇧M)" icon={<RiSplitCellsHorizontal className="h-3.5 w-3.5" />}>
              Unmerge
            </ToolbarButton>
          </ToolbarGroup>

          {/* Row ops group */}
          <ToolbarGroup label="Rows">
            <ToolbarButton
              onClick={() => insertRowAt(tbl.rows)}
              title="Add a new row at the bottom"
              icon={<RiAddLine className="h-3.5 w-3.5" />}
            >
              Add row
            </ToolbarButton>
          </ToolbarGroup>

          {/* Selection info */}
          <div className="ml-auto flex items-center gap-2 text-[11px] text-muted-foreground">
            <RiInformationLine className="h-3.5 w-3.5" />
            {sel
              ? <span><span className="font-medium text-foreground">{selSize.h}×{selSize.w}</span> selected</span>
              : <span>Click a cell to start. <span className="opacity-70">Shift+click extends.</span></span>}
          </div>
        </div>

        {/* ── Body ─────────────────────────────────────────────────────── */}
        <div className="flex-1 flex min-h-0">
          {/* Spreadsheet grid */}
          <div
            className="flex-1 min-w-0 overflow-auto"
            onMouseUp={endSel}
            onMouseLeave={endSel}
          >
            <table className="w-full text-xs border-collapse select-none" style={{ tableLayout: 'fixed' }}>
              <thead>
                <tr>
                  {/* Top-left corner = select all */}
                  <th
                    className="sticky top-0 left-0 z-20 bg-[hsl(var(--muted))] border-r border-b border-[hsl(var(--border))] w-[64px] text-[10px] font-semibold text-muted-foreground p-0 cursor-pointer hover:bg-[hsl(var(--secondary))] transition-colors duration-150"
                    onClick={selectAll}
                    title="Select all cells"
                  >
                    <div className="h-7 flex items-center justify-center">⌗</div>
                  </th>
                  {colHeaderLabels.map((lbl, i) => (
                    <th
                      key={i}
                      className="sticky top-0 z-10 bg-[hsl(var(--muted))] border-r border-b border-[hsl(var(--border))] px-2 h-7 text-[10px] font-semibold text-muted-foreground truncate"
                      title={lbl}
                    >
                      {lbl || `Col ${i + 1}`}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {tbl.cells.map((row, ri) => {
                  const rs = tbl.rowStyles?.[ri]
                  const rowHasFmt = !!rs && Object.keys(rs).some(k => (rs as any)[k] !== undefined)
                  return (
                    <tr key={ri} className="group/row">
                      {/* Row handle (sticky left) */}
                      <td
                        className={`sticky left-0 z-[5] border-r border-b border-[hsl(var(--border))] p-0 cursor-pointer transition-colors duration-150 ${sel && (ri >= Math.min(sel.r1, sel.r2) && ri <= Math.max(sel.r1, sel.r2)) ? 'bg-[hsl(var(--primary)/0.12)]' : 'bg-[hsl(var(--muted)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}
                        onClick={() => selectRow(ri)}
                        title={`Select row ${ri + 1}`}
                      >
                        <div className="h-9 px-1 flex items-center gap-0.5">
                          <RiDraggable className="h-3 w-3 text-muted-foreground/60 shrink-0" />
                          <span className="text-[10px] tabular-nums font-mono text-muted-foreground w-4 text-right">{ri + 1}</span>
                          {rowHasFmt && (
                            <span
                              className="ml-0.5 inline-block w-1 h-1 rounded-full bg-[hsl(var(--primary))]"
                              title="Row has custom formatting"
                              aria-label="Row formatted"
                            />
                          )}
                          <div className="ml-auto flex items-center opacity-0 group-hover/row:opacity-100 transition-opacity duration-150">
                            <IconBtn onClick={(e) => { e.stopPropagation(); moveRowUp(ri) }} disabled={ri === 0} title="Move up">
                              <RiArrowUpLine className="h-3 w-3" />
                            </IconBtn>
                            <IconBtn onClick={(e) => { e.stopPropagation(); moveRowDown(ri) }} disabled={ri >= tbl.rows - 1} title="Move down">
                              <RiArrowDownLine className="h-3 w-3" />
                            </IconBtn>
                            <IconBtn onClick={(e) => { e.stopPropagation(); deleteRow(ri) }} disabled={tbl.rows <= 1} title="Delete row" danger>
                              <RiDeleteBinLine className="h-3 w-3" />
                            </IconBtn>
                          </div>
                        </div>
                      </td>
                      {/* Cells */}
                      {row.map((cell, ci) => {
                        if (covered.has(`${ri},${ci}`)) return null
                        const selectedCell = isInSel(ri, ci)
                        const cspan = cell.colspan || 1
                        const rspan = cell.rowspan || 1
                        const isMergedAnchor = cspan > 1 || rspan > 1
                        return (
                          <td
                            key={ci}
                            colSpan={cspan}
                            rowSpan={rspan}
                            className={`border-r border-b border-[hsl(var(--border))] p-0 align-middle relative transition-colors duration-150 ${selectedCell ? 'bg-[hsl(var(--primary)/0.08)]' : 'bg-[hsl(var(--background))] hover:bg-[hsl(var(--secondary)/0.4)]'}`}
                            style={{
                              outline: selectedCell ? '2px solid hsl(var(--primary))' : undefined,
                              outlineOffset: -2,
                              cursor: 'cell',
                            }}
                            onMouseDown={(e) => { e.preventDefault(); startSel(ri, ci, e) }}
                            onMouseEnter={() => extendSel(ri, ci)}
                          >
                            <div className="h-9 px-2 flex items-center min-w-0">
                              <CellPreview cell={cell} variables={variables} />
                            </div>
                            {/* Merge anchor badge */}
                            {isMergedAnchor && (
                              <span
                                className="absolute top-0.5 right-0.5 text-[8px] font-semibold tabular-nums px-1 rounded bg-[hsl(var(--primary))] text-[hsl(var(--primary-foreground))]"
                                title={`Merged ${rspan}×${cspan}`}
                              >
                                {rspan}×{cspan}
                              </span>
                            )}
                          </td>
                        )
                      })}
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {/* ── Side panel ─────────────────────────────────────────────── */}
          <aside className="w-[300px] shrink-0 border-l border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-auto">
            <div className="p-4 space-y-4">
              <div>
                <h3 className="text-xs font-semibold text-foreground tracking-tight">Row formatting</h3>
                <p className="text-[10px] text-muted-foreground mt-0.5">Apply background, text and border styles to selected rows.</p>
              </div>
              {sel ? (
                <RowFormattingPanel
                  selectedRows={(() => { const n = normRect(sel); return Array.from({ length: n.r2 - n.r1 + 1 }, (_, i) => n.r1 + i) })()}
                  rowStyles={tbl.rowStyles}
                  onApplyAction={(patch) => {
                    const n = normRect(sel)
                    const styles = [...(tbl.rowStyles || Array.from({ length: tbl.rows }, () => ({})))]
                    while (styles.length < tbl.rows) styles.push({})
                    for (let r = n.r1; r <= n.r2; r++) {
                      styles[r] = { ...(styles[r] || {}), ...patch }
                    }
                    commit({ ...tbl, rowStyles: styles })
                  }}
                  onResetAction={() => {
                    const n = normRect(sel)
                    const styles = [...(tbl.rowStyles || Array.from({ length: tbl.rows }, () => ({})))]
                    while (styles.length < tbl.rows) styles.push({})
                    for (let r = n.r1; r <= n.r2; r++) styles[r] = {}
                    commit({ ...tbl, rowStyles: styles })
                  }}
                />
              ) : (
                <div className="rounded-md border border-dashed border-[hsl(var(--border))] p-4 text-center">
                  <p className="text-[11px] text-muted-foreground leading-snug">
                    Select a cell or click a row number to start formatting.
                  </p>
                </div>
              )}
            </div>
          </aside>
        </div>

        {/* ── Status bar ──────────────────────────────────────────────── */}
        <footer className="flex items-center justify-between gap-3 px-4 h-9 border-t border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[11px] text-muted-foreground">
          <div className="flex items-center gap-3">
            <span><span className="font-medium text-foreground">{tbl.rows}</span> rows × <span className="font-medium text-foreground">{totalCols}</span> cols</span>
            {mergedRegionsCount > 0 && (
              <>
                <span aria-hidden>•</span>
                <span><span className="font-medium text-foreground">{mergedRegionsCount}</span> merged region{mergedRegionsCount === 1 ? '' : 's'}</span>
              </>
            )}
            {sel && (
              <>
                <span aria-hidden>•</span>
                <span>Selection <span className="font-medium text-foreground">{selSize.h}×{selSize.w}</span></span>
              </>
            )}
          </div>
          <div className="flex items-center gap-2 text-[10px]">
            <Kbd>⌘M</Kbd> merge <span className="opacity-50">·</span>
            <Kbd>⌘⇧M</Kbd> unmerge <span className="opacity-50">·</span>
            <Kbd>Shift</Kbd>+arrows extend <span className="opacity-50">·</span>
            <Kbd>Esc</Kbd> close
          </div>
        </footer>
      </div>
    </div>,
    document.body,
  )
}

// ─────────────────────────────────────────────────────────────────────
// Toolbar primitives
// ─────────────────────────────────────────────────────────────────────
function ToolbarGroup({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-center gap-1" role="group" aria-label={label}>
      {children}
      <span className="h-5 w-px bg-[hsl(var(--border))] mx-1" aria-hidden />
    </div>
  )
}

function ToolbarButton({
  children,
  onClick,
  disabled,
  title,
  icon,
}: {
  children: React.ReactNode
  onClick: () => void
  disabled?: boolean
  title?: string
  icon?: React.ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={title}
      className="text-[11px] h-7 px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] hover:bg-[hsl(var(--muted))] disabled:opacity-40 disabled:cursor-not-allowed cursor-pointer transition-colors duration-150 inline-flex items-center gap-1 focus:outline-none focus-visible:ring-2 focus-visible:ring-[hsl(var(--primary))]"
    >
      {icon}{children}
    </button>
  )
}

function IconBtn({
  children,
  onClick,
  disabled,
  title,
  danger,
}: {
  children: React.ReactNode
  onClick: (e: React.MouseEvent) => void
  disabled?: boolean
  title?: string
  danger?: boolean
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={title}
      className={`p-1 rounded transition-colors duration-150 cursor-pointer disabled:opacity-30 disabled:cursor-not-allowed focus:outline-none focus-visible:ring-1 focus-visible:ring-[hsl(var(--primary))] ${danger ? 'text-destructive hover:bg-destructive/10' : 'hover:bg-[hsl(var(--muted))]'}`}
    >
      {children}
    </button>
  )
}

function Kbd({ children }: { children: React.ReactNode }) {
  return (
    <kbd className="inline-flex items-center px-1 py-0.5 rounded border border-[hsl(var(--border))] bg-[hsl(var(--muted))] font-mono text-[9px] tabular-nums text-foreground">
      {children}
    </kbd>
  )
}

// ─────────────────────────────────────────────────────────────────────
// Row formatting side panel
// ─────────────────────────────────────────────────────────────────────
function RowFormattingPanel({
  selectedRows,
  rowStyles,
  onApplyAction,
  onResetAction,
}: {
  selectedRows: number[]
  rowStyles: TableT['rowStyles']
  onApplyAction: (patch: Partial<RowStyle>) => void
  onResetAction: () => void
}) {
  const single = selectedRows.length === 1 ? rowStyles?.[selectedRows[0]] : undefined

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <p className="text-[11px] text-muted-foreground">
          {selectedRows.length === 1
            ? <>Editing <span className="font-medium text-foreground">row {selectedRows[0] + 1}</span></>
            : <>Editing <span className="font-medium text-foreground">{selectedRows.length} rows</span></>}
        </p>
        <button
          type="button"
          className="text-[10px] text-muted-foreground hover:text-foreground transition-colors duration-150 cursor-pointer underline-offset-2 hover:underline"
          onClick={onResetAction}
        >
          Reset
        </button>
      </div>

      {/* Background + Text */}
      <Section title="Colors">
        <div className="grid grid-cols-2 gap-3">
          <ColorField
            label="Background"
            value={single?.bg}
            onChangeAction={(v) => onApplyAction({ bg: v })}
            placeholder="#ffffff"
          />
          <ColorField
            label="Text"
            value={single?.color}
            onChangeAction={(v) => onApplyAction({ color: v })}
            placeholder="#111827"
          />
        </div>
      </Section>

      {/* Typography */}
      <Section title="Typography">
        <div className="grid grid-cols-2 gap-2">
          <Field label="Weight">
            <select
              className="w-full h-7 text-[11px] rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-2 cursor-pointer focus:ring-2 focus:ring-[hsl(var(--primary))] outline-none transition-shadow duration-150"
              value={single?.fontWeight || 'normal'}
              onChange={(e) => onApplyAction({ fontWeight: e.target.value as any })}
            >
              <option value="normal">Normal</option>
              <option value="semibold">Semibold</option>
              <option value="bold">Bold</option>
            </select>
          </Field>
          <Field label="Size (px)">
            <input
              type="number"
              className="w-full h-7 text-[11px] rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-2 focus:ring-2 focus:ring-[hsl(var(--primary))] outline-none transition-shadow duration-150"
              min={8} max={48} step={1}
              value={single?.fontSize ?? ''} placeholder="auto"
              onChange={(e) => onApplyAction({ fontSize: e.target.value ? +e.target.value : undefined })}
            />
          </Field>
        </div>
      </Section>

      {/* Borders */}
      <Section title="Borders">
        <BorderEditor
          label="Top"
          style={single?.borderTopStyle}
          color={single?.borderTopColor}
          width={single?.borderTopWidth}
          onChangeStyleAction={(v) => onApplyAction({ borderTopStyle: v as any })}
          onChangeColorAction={(v) => onApplyAction({ borderTopColor: v })}
          onChangeWidthAction={(v) => onApplyAction({ borderTopWidth: v })}
        />
        <BorderEditor
          label="Bottom"
          style={single?.borderBottomStyle}
          color={single?.borderBottomColor}
          width={single?.borderBottomWidth}
          onChangeStyleAction={(v) => onApplyAction({ borderBottomStyle: v as any })}
          onChangeColorAction={(v) => onApplyAction({ borderBottomColor: v })}
          onChangeWidthAction={(v) => onApplyAction({ borderBottomWidth: v })}
        />
      </Section>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="space-y-2">
      <h4 className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{title}</h4>
      <div className="space-y-2">{children}</div>
    </div>
  )
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div>
      <label className="block text-[10px] font-medium text-muted-foreground mb-1">{label}</label>
      {children}
    </div>
  )
}

function ColorField({
  label,
  value,
  onChangeAction,
  placeholder,
}: {
  label: string
  value: string | undefined
  onChangeAction: (v: string | undefined) => void
  placeholder: string
}) {
  return (
    <div>
      <label className="block text-[10px] font-medium text-muted-foreground mb-1">{label}</label>
      <div className="flex items-center gap-1.5">
        <div className="relative">
          <input
            type="color"
            className="w-7 h-7 rounded-md border border-[hsl(var(--border))] cursor-pointer focus:ring-2 focus:ring-[hsl(var(--primary))] outline-none"
            value={value || placeholder}
            onChange={(e) => onChangeAction(e.target.value)}
            aria-label={`${label} color`}
          />
        </div>
        <input
          type="text"
          className="flex-1 min-w-0 h-7 text-[11px] rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-2 font-mono uppercase focus:ring-2 focus:ring-[hsl(var(--primary))] outline-none transition-shadow duration-150"
          value={value || ''}
          placeholder={placeholder}
          onChange={(e) => onChangeAction(e.target.value || undefined)}
          aria-label={`${label} hex`}
        />
        {value && (
          <button
            type="button"
            className="text-[10px] text-muted-foreground hover:text-foreground transition-colors duration-150 cursor-pointer"
            onClick={() => onChangeAction(undefined)}
            title="Clear"
            aria-label={`Clear ${label}`}
          >
            ✕
          </button>
        )}
      </div>
    </div>
  )
}

function BorderEditor({
  label,
  style,
  color,
  width,
  onChangeStyleAction,
  onChangeColorAction,
  onChangeWidthAction,
}: {
  label: string
  style: string | undefined
  color: string | undefined
  width: number | undefined
  onChangeStyleAction: (v: string | undefined) => void
  onChangeColorAction: (v: string | undefined) => void
  onChangeWidthAction: (v: number | undefined) => void
}) {
  return (
    <div className="rounded-md border border-[hsl(var(--border))] p-2 bg-[hsl(var(--secondary)/0.25)]">
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-[10px] font-medium text-muted-foreground">{label} border</span>
        {(style || color || width != null) && (
          <button
            type="button"
            className="text-[10px] text-muted-foreground hover:text-foreground transition-colors duration-150 cursor-pointer"
            onClick={() => { onChangeStyleAction(undefined); onChangeColorAction(undefined); onChangeWidthAction(undefined) }}
            title={`Clear ${label} border`}
            aria-label={`Clear ${label} border`}
          >
            Clear
          </button>
        )}
      </div>
      <div className="grid grid-cols-[1fr_auto_50px] gap-1.5">
        <select
          className="h-7 text-[11px] rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-1.5 cursor-pointer focus:ring-2 focus:ring-[hsl(var(--primary))] outline-none transition-shadow duration-150"
          value={style || ''}
          onChange={(e) => onChangeStyleAction(e.target.value || undefined)}
          aria-label={`${label} border style`}
        >
          <option value="">inherit</option>
          {BORDER_STYLES.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <input
          type="color"
          className="w-7 h-7 rounded-md border border-[hsl(var(--border))] cursor-pointer focus:ring-2 focus:ring-[hsl(var(--primary))] outline-none"
          value={color || '#e5e7eb'}
          onChange={(e) => onChangeColorAction(e.target.value)}
          aria-label={`${label} border color`}
        />
        <input
          type="number"
          className="h-7 text-[11px] rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] px-1.5 focus:ring-2 focus:ring-[hsl(var(--primary))] outline-none transition-shadow duration-150"
          min={0} max={10} step={1}
          value={width ?? ''} placeholder="px"
          onChange={(e) => onChangeWidthAction(e.target.value ? +e.target.value : undefined)}
          aria-label={`${label} border width`}
        />
      </div>
    </div>
  )
}
