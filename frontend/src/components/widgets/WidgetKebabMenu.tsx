"use client"

import React, { useEffect, useMemo, useRef } from 'react'
import { createPortal } from 'react-dom'

export type WidgetKebabMenuProps = {
  open: boolean
  anchorEl: HTMLElement | null
  onCloseAction: () => void
  onAction: (action: 'duplicate' | 'delete' | 'viewSpec' | 'viewSql' | 'viewJson' | 'aiAssist' | 'embed' | 'downloadPNG' | 'downloadSVG') => void
  widgetType?: string
  chartType?: string
}

export default function WidgetKebabMenu({ open, anchorEl, onCloseAction, onAction, widgetType, chartType }: WidgetKebabMenuProps) {
  const ref = useRef<HTMLDivElement | null>(null)
  
  // Determine if this widget supports chart downloads
  // Supported: chart type widgets (except non-ECharts types like badges, progress, tracker, categoryBar, barList, tremorTable)
  const supportsDownload = useMemo(() => {
    if (widgetType !== 'chart') return false
    // Non-ECharts chart types that don't support download
    const nonEChartsTypes = ['badges', 'progress', 'tracker', 'categoryBar', 'barList', 'tremorTable', 'spark']
    if (chartType && nonEChartsTypes.includes(chartType)) return false
    return true
  }, [widgetType, chartType])

  useEffect(() => {
    function onDocMouseDown(e: MouseEvent) {
      const el = ref.current
      if (!el) return
      if (e.target instanceof Node && !el.contains(e.target as Node)) onCloseAction()
    }
    function onEsc(e: KeyboardEvent) { if (e.key === 'Escape') onCloseAction() }
    if (typeof window !== 'undefined' && open) {
      window.addEventListener('mousedown', onDocMouseDown)
      window.addEventListener('keydown', onEsc)
    }
    return () => {
      if (typeof window !== 'undefined') {
        window.removeEventListener('mousedown', onDocMouseDown)
        window.removeEventListener('keydown', onEsc)
      }
    }
  }, [open, onCloseAction])

  // Signal "actionsMenuOpen" while the kebab menu is open to prevent hover-based panels from closing
  useEffect(() => {
    if (typeof document === 'undefined') return
    if (open) document.body.dataset.actionsMenuOpen = '1'
    return () => { try { delete (document.body as any).dataset.actionsMenuOpen } catch {} }
  }, [open])

  const pos = useMemo(() => {
    if (!anchorEl) return { x: 0, y: 0 }
    const r = anchorEl.getBoundingClientRect()
    const x = Math.max(8, Math.min(r.left, (typeof window !== 'undefined' ? window.innerWidth : 1024) - 220))
    const y = r.bottom + 6
    return { x, y }
  }, [anchorEl, open])

  if (!open || typeof window === 'undefined') return null

  return createPortal(
    <div
      ref={ref}
      className="fixed z-[2000] rounded-md border bg-card shadow-card w-[220px] max-h-[70vh] overflow-auto"
      style={{ left: pos.x, top: pos.y }}
      role="menu"
    >
      <div className="py-1">
        <button className="w-full text-left text-xs px-3 py-1.5 hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { onAction('aiAssist'); onCloseAction() }}>✨ AI Assist</button>
        <button className="w-full text-left text-xs px-3 py-1.5 hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { onAction('viewSpec'); onCloseAction() }}>View SpecQuery</button>
        <button className="w-full text-left text-xs px-3 py-1.5 hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { onAction('viewSql'); onCloseAction() }}>View SQL Statement</button>
        <button className="w-full text-left text-xs px-3 py-1.5 hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { onAction('viewJson'); onCloseAction() }}>View JSON Config</button>
        <button className="w-full text-left text-xs px-3 py-1.5 hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { onAction('embed'); onCloseAction() }}>Embed</button>
        {supportsDownload && (
          <>
            <div className="my-1 h-px bg-[hsl(var(--border))]" />
            <button className="w-full text-left text-xs px-3 py-1.5 hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { onAction('downloadPNG'); onCloseAction() }}>Download PNG</button>
            <button className="w-full text-left text-xs px-3 py-1.5 hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { onAction('downloadSVG'); onCloseAction() }}>Download SVG</button>
          </>
        )}
        <div className="my-1 h-px bg-[hsl(var(--border))]" />
        <button className="w-full text-left text-xs px-3 py-1.5 hover:bg-[hsl(var(--secondary)/0.6)]" onClick={() => { onAction('duplicate'); onCloseAction() }}>Duplicate</button>
        <button className="w-full text-left text-xs px-3 py-1.5 hover:bg-[hsl(var(--secondary)/0.6)] text-red-600" onClick={() => { onAction('delete'); onCloseAction() }}>Delete</button>
      </div>
    </div>,
    document.body
  )
}
