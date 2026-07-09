// Responsive dashboard grid helpers (spec 19).
// Per-breakpoint layout derivation so a single saved `layout` still renders on
// tablet/phone without any persisted `layouts`. Math is lifted verbatim from the
// builder's existing gridSize-rescale and orphan-reconcile logic in app/page.tsx.
import type { RGLLayout } from '@/lib/api'
import type { WidgetConfig } from '@/types/widgets'

export type BreakpointKey = 'desktop' | 'tablet' | 'phone'

// react-grid-layout Responsive breakpoints (min container width in px).
// phone must be 0 so it always matches the smallest viewport.
export const BREAKPOINTS: Record<BreakpointKey, number> = { desktop: 996, tablet: 600, phone: 0 }

// Desktop column counts keyed by the `gridSize` density option (NOT viewport).
// Previously duplicated across app/page.tsx and app/v/[id]/page.tsx.
export const GRIDSIZE_COLS: Record<string, number> = { sm: 24, md: 18, lg: 12, xl: 8 }

// Columns per breakpoint. Desktop follows the density option; tablet/phone are fixed.
export function colsFor(gridSize?: string): Record<BreakpointKey, number> {
  return { desktop: GRIDSIZE_COLS[gridSize ?? 'lg'] ?? 12, tablet: 8, phone: 2 }
}

// Proportionally rescale x/w from the layout's implied source cols to dstCols, then clamp.
export function rescaleLayout(layout: RGLLayout[], dstCols: number): RGLLayout[] {
  const srcCols = Math.max(1, layout.reduce((acc, it) => Math.max(acc, Number(it.x || 0) + Number(it.w || 0)), 0))
  const ratio = dstCols / srcCols
  return layout.map((it) => {
    const scaledW = Math.max(1, Math.round(it.w * ratio))
    const w = Math.min(scaledW, dstCols)
    const scaledX = Math.max(0, Math.round(it.x * ratio))
    const x = Math.min(scaledX, Math.max(0, dstCols - w))
    return { ...it, w, x }
  })
}

// Stack every item full-width in (y, x) reading order.
export function stackLayout(layout: RGLLayout[], cols: number): RGLLayout[] {
  const sorted = [...layout].sort((a, b) => (a.y - b.y) || (a.x - b.x))
  let runningY = 0
  return sorted.map((it) => {
    const item = { ...it, x: 0, w: cols, y: runningY }
    runningY += it.h
    return item
  })
}

// Keep layout consistent with widgets: append widgets missing a layout item (same
// type-based default sizes as the old inline block) and drop layout items with no widget.
export function reconcileOrphans(layout: RGLLayout[], widgets: Record<string, WidgetConfig>): RGLLayout[] {
  const layoutIds = new Set(layout.map((ly) => ly.i))
  const present = layout.filter((ly) => widgets[ly.i])
  const orphanIds = Object.keys(widgets).filter((id) => !layoutIds.has(id))
  if (!orphanIds.length) return present
  const maxY = present.reduce((acc, ly) => Math.max(acc, ly.y + ly.h), 0)
  const orphanLayouts: RGLLayout[] = orphanIds.map((id, idx) => {
    const type = (widgets[id] as WidgetConfig | undefined)?.type || 'chart'
    const size = (type === 'table' || type === 'composition') ? { w: 9, h: 6 } : type === 'chart' ? { w: 6, h: 6 } : { w: 3, h: 2 }
    return { i: id, x: 0, y: maxY + (idx * 7), w: size.w, h: size.h }
  })
  return [...present, ...orphanLayouts]
}

// Derive all three breakpoint layouts. Stored layouts win; missing ones are derived
// from desktop (tablet = rescale to 8 cols, phone = full-width stack at 2 cols).
export function deriveLayouts(
  def: { layout?: RGLLayout[]; layouts?: Partial<Record<BreakpointKey, RGLLayout[]>> },
  widgets: Record<string, WidgetConfig>,
  gridSize?: string,
): Record<BreakpointKey, RGLLayout[]> {
  const stored = def.layouts || {}
  const desktop = stored.desktop ?? def.layout ?? []
  const tablet = stored.tablet ?? rescaleLayout(desktop, colsFor(gridSize).tablet)
  const phone = stored.phone ?? stackLayout(desktop, colsFor(gridSize).phone)
  return {
    desktop: reconcileOrphans(desktop, widgets),
    tablet: reconcileOrphans(tablet, widgets),
    phone: reconcileOrphans(phone, widgets),
  }
}
