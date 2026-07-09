import { describe, it, expect } from 'vitest'
import {
  colsFor,
  rescaleLayout,
  stackLayout,
  reconcileOrphans,
  deriveLayouts,
} from '@/lib/gridBreakpoints'

describe('colsFor', () => {
  it('maps gridSize density to desktop cols, fixed tablet/phone', () => {
    expect(colsFor('sm')).toEqual({ desktop: 24, tablet: 8, phone: 2 })
    expect(colsFor('lg')).toEqual({ desktop: 12, tablet: 8, phone: 2 })
    expect(colsFor(undefined)).toEqual({ desktop: 12, tablet: 8, phone: 2 })
    expect(colsFor('bogus')).toEqual({ desktop: 12, tablet: 8, phone: 2 })
  })
})

describe('rescaleLayout', () => {
  it('proportionally rescales x/w to dstCols and clamps within bounds', () => {
    const src = [{ i: 'a', x: 0, y: 0, w: 12, h: 4 }, { i: 'b', x: 12, y: 0, w: 12, h: 4 }]
    // srcCols = 24, dst = 12 => ratio 0.5
    const out = rescaleLayout(src, 12)
    expect(out[0]).toMatchObject({ x: 0, w: 6 })
    expect(out[1]).toMatchObject({ x: 6, w: 6 })
    // nothing exceeds dstCols
    for (const it of out) expect(it.x + it.w).toBeLessThanOrEqual(12)
  })
})

describe('stackLayout', () => {
  it('stacks every item full-width in reading order with cumulative y', () => {
    const src = [{ i: 'b', x: 3, y: 5, w: 4, h: 3 }, { i: 'a', x: 0, y: 0, w: 4, h: 2 }]
    const out = stackLayout(src, 2)
    expect(out.map((i) => i.i)).toEqual(['a', 'b']) // sorted by y then x
    expect(out[0]).toMatchObject({ x: 0, w: 2, y: 0 })
    expect(out[1]).toMatchObject({ x: 0, w: 2, y: 2 }) // y advanced by first item's h
  })
})

describe('reconcileOrphans', () => {
  it('drops layout items with no widget and appends widgets missing a layout item', () => {
    const layout = [{ i: 'keep', x: 0, y: 0, w: 6, h: 6 }, { i: 'gone', x: 6, y: 0, w: 6, h: 6 }]
    const widgets: any = { keep: { type: 'chart' }, orphan: { type: 'table' } }
    const out = reconcileOrphans(layout, widgets)
    const ids = out.map((l) => l.i)
    expect(ids).toContain('keep')
    expect(ids).not.toContain('gone') // no widget -> dropped
    expect(ids).toContain('orphan') // widget without layout -> appended
    const orphan = out.find((l) => l.i === 'orphan')!
    expect(orphan).toMatchObject({ w: 9, h: 6 }) // table default size
  })
})

describe('deriveLayouts', () => {
  it('derives tablet/phone from desktop when not stored', () => {
    const def = { layout: [{ i: 'a', x: 0, y: 0, w: 12, h: 4 }] }
    const widgets: any = { a: { type: 'chart' } }
    const out = deriveLayouts(def, widgets, 'lg')
    expect(out.desktop).toHaveLength(1)
    expect(out.phone[0]).toMatchObject({ x: 0, w: 2 }) // stacked at phone cols
    expect(out.tablet[0].w).toBeLessThanOrEqual(8)
  })

  it('prefers stored per-breakpoint layouts over derived', () => {
    const stored = [{ i: 'a', x: 1, y: 1, w: 3, h: 3 }]
    const def = { layout: [{ i: 'a', x: 0, y: 0, w: 12, h: 4 }], layouts: { phone: stored } }
    const out = deriveLayouts(def, { a: { type: 'chart' } } as any, 'lg')
    expect(out.phone[0]).toMatchObject({ x: 1, w: 3 })
  })
})
