---
id: 19-responsive-dashboard-grid
title: Responsive dashboard grid and mobile layout
priority: P2
effort: L
depends_on: []
area: frontend
---

## Problem

Dashboards are desktop-only. Both the builder and the public viewer render a plain
`GridLayout` from `react-grid-layout@^1.3.4` with a fixed column count and a manually
measured width clamped to a desktop minimum. The public route additionally wraps the grid
in `min-w-[720px]`, so phones get a horizontally scrolling desktop layout instead of a
stacked mobile view. Only one layout array is persisted — there is no per-breakpoint
layout storage or derivation.

## Current State

All refs verified 2026-07-07 on branch `feature/alpha-themes-foundation`.

**Builder** — `frontend/src/app/page.tsx` (2,125 lines):
- L10: `import GridLayout from 'react-grid-layout'` (plain, no `Responsive`/`WidthProvider`).
- L59: single layout state: `const [layoutState, setLayoutState] = useState<RGLLayout[]>(defaultLayout)`.
- L955-965: `ResizeObserver` measures canvas, clamps `setCanvasW(Math.max(480, Math.floor(rect.width - 24)))`.
- L1055-1058: cols come from a *density* option, not viewport: `{ sm: 24, md: 18, lg: 12, xl: 8 }[dashOptions.gridSize]` (note: `gridSize` keys are card-density labels, NOT viewport breakpoints — do not conflate).
- L1088-1092: `gridWidth` = fixed px when `dashOptions.gridCanvasMode === 'fixed'`, else measured `canvasW`.
- L1423-1451: `<GridLayout layout={effectiveLayout} cols={cols} rowHeight={24} width={gridWidth} …>` with `onLayoutChange`/`onDragStop`/`onResizeStop` all writing the single `layoutState` and calling `scheduleServerSave({ layout: next, widgets: configs })`.
- L672-704: `scheduleServerSave(nextDef?, nextOptions?, immediate?)` — 600 ms debounce, saves `definition: { layout, widgets, options }` via `Api.saveDashboard`. ~20 call sites (L84, 176, 254, 281, 289, 884, 908, 918, 929, 1099, 1105, 1140, 1159, 1193, 1213, 1233, 1346, 1354-1358, 1415, 1442, 1450, 1502).
- L645-648: draft persistence: `localStorage.setItem('dashboardDraft', JSON.stringify({ layout: layoutState, widgets: configs, options: dashOptions }))`.
- L546-562: orphaned-widget reconciliation on load — widgets present in `def.widgets` but missing from `def.layout` are appended at `maxY` with type-based default sizes (`table|composition: 9x6`, `chart: 6x6`, else `3x2`).
- L236-259: on `gridSize` change, x/w are proportionally rescaled from implied source cols to new cols and clamped.
- L140-182 `packRowsLeftAndFill`, L1110-1146 `normalizeLayoutToFullCols`: row helpers operating on `layoutState` + `cols`.
- L967-1019: auto-fit height (`recalcHeights`) mutates `h` on `layoutState` items.

**Public viewer** — `frontend/src/app/v/[id]/page.tsx` (477 lines):
- L7: plain `GridLayout` import; L40 single `layout` state.
- L62-71: canvas measure with mobile-hostile clamp `Math.max(480, …)`.
- L285-294: cols from `dashOptions.gridSize`, falling back to implied cols from the layout.
- L315: `<div className="min-w-[720px] space-y-2">` — forces horizontal scroll on phones.
- L323-333: `<GridLayout … isResizable={false} isDraggable={false}>`.
- L136: loads only `res.definition.layout`.

**Embed** — `frontend/src/app/render/embed/widget/page.tsx` (300 lines): renders ONE widget sized by `w`/`h` query params; no grid at all (verified — no `GridLayout` import). No grid change needed here.

**Types/schema:**
- `frontend/src/lib/api.ts` L1038-1055: `RGLLayout` and `DashboardDefinition = { layout: RGLLayout[]; widgets; options? }`.
- `frontend/src/types/react-grid-layout.d.ts`: local shim exporting only the default component — `Responsive`/`WidthProvider` are not declared and will not typecheck.
- `backend/app/schemas.py` L165-169: Pydantic `DashboardDefinition(layout, widgets, options)` — **unknown keys are dropped on validation**, so a new `layouts` key will be silently stripped unless added here. Storage itself is opaque JSON (`backend/app/models.py` L110, L312-320).

## Desired State

- Definition stores per-breakpoint layouts: `definition.layouts: { desktop, tablet, phone }` (breakpoint names deliberately distinct from the `gridSize` density keys). Legacy `definition.layout` keeps being written as a mirror of `layouts.desktop` for backward compat.
- Breakpoints: `desktop >= 996px`, `tablet >= 600px`, `phone < 600px`. Cols: `desktop` = existing gridSize map value, `tablet` = 8, `phone` = 2.
- Missing breakpoint layouts are **derived, never persisted until edited**: tablet by proportional rescale (reuse the existing L236-259 math), phone by stacking full-width in (y, x) order.
- Public viewer `/v/[id]` uses `WidthProvider(Responsive)` with the three layouts, read-only, no `min-w-[720px]`, no 480px width clamp — phones get the stacked layout.
- Builder gains a breakpoint switcher (Desktop / Tablet / Phone) that swaps which layout array `layoutState` edits; the grid itself stays plain `GridLayout` (this preserves the `gridCanvasMode: 'fixed'` explicit-width feature, which `WidthProvider` cannot express, and avoids rewriting 20+ save call sites).
- Orphaned-widget reconciliation runs per breakpoint via one shared helper; widgets added/removed while editing any breakpoint stay consistent across all stored layouts because reconciliation also runs centrally inside `scheduleServerSave`.

## Implementation Plan

Ordered; each step compiles independently.

**1. Backend schema (1 line)** — `backend/app/schemas.py` L166, inside `DashboardDefinition` add:
```python
layouts: Optional[Dict[str, List[Dict[str, Any]]]] = None
```
No migration needed (definition is stored as JSON text; old rows simply lack the key).

**2. Frontend types** — `frontend/src/lib/api.ts` after L1049:
```ts
export type BreakpointKey = 'desktop' | 'tablet' | 'phone'
```
and in `DashboardDefinition` (L1051-1055) add `layouts?: Partial<Record<BreakpointKey, RGLLayout[]>>`.

**3. Type shim** — `frontend/src/types/react-grid-layout.d.ts`, extend:
```ts
export const Responsive: React.ComponentType<Record<string, unknown>>
export function WidthProvider<P>(c: React.ComponentType<P>): React.ComponentType<P>
```

**4. New helper module** — `frontend/src/lib/gridBreakpoints.ts` (single new file, ~80 lines):
- `export const BREAKPOINTS = { desktop: 996, tablet: 600, phone: 0 }`
- `export const GRIDSIZE_COLS: Record<string, number> = { sm: 24, md: 18, lg: 12, xl: 8 }` (moved here; today duplicated at page.tsx L238, L1056, L1074, and v/[id] L286)
- `export function colsFor(gridSize: string | undefined): Record<BreakpointKey, number>` → `{ desktop: GRIDSIZE_COLS[gridSize ?? 'lg'] ?? 12, tablet: 8, phone: 2 }`
- `export function rescaleLayout(layout: RGLLayout[], dstCols: number): RGLLayout[]` — extract the exact math from page.tsx L240-258 (implied srcCols → ratio → round → clamp).
- `export function stackLayout(layout: RGLLayout[], cols: number): RGLLayout[]` — sort by `(y, x)`, emit `{ ...it, x: 0, w: cols, y: runningY }` accumulating `runningY += it.h`.
- `export function reconcileOrphans(layout: RGLLayout[], widgets: Record<string, WidgetConfig>): RGLLayout[]` — extract page.tsx L546-562 verbatim (same type-based default sizes), PLUS drop layout items whose `i` has no widget.
- `export function deriveLayouts(def: { layout?: RGLLayout[]; layouts?: Partial<Record<BreakpointKey, RGLLayout[]>> }, widgets: Record<string, WidgetConfig>, gridSize?: string): Record<BreakpointKey, RGLLayout[]>` — desktop = `layouts.desktop ?? layout ?? []`; tablet = `layouts.tablet ?? rescaleLayout(desktop, 8)`; phone = `layouts.phone ?? stackLayout(desktop, 2)`; run `reconcileOrphans` on each.

**5. Public viewer** — `frontend/src/app/v/[id]/page.tsx`:
- Import `Responsive, WidthProvider` and `const ResponsiveGrid = WidthProvider(Responsive)` at module scope (must be module-level so the HOC isn't recreated per render).
- L136: also capture `res.definition.layouts` into state (extend the existing `layout` state to `layouts: Partial<Record<BreakpointKey, RGLLayout[]>>` or add a sibling state — keep `layout` for the L281/L290 fallbacks).
- L285-312: compute `const layouts = deriveLayouts(def, widgets, dashOptions.gridSize)` then apply the existing per-item clamp/spacer-minW mapping (L296-307) to each breakpoint's array; `colsFor(dashOptions.gridSize)` for the `cols` prop.
- L323-333: replace `<GridLayout layout cols width>` with `<ResponsiveGrid layouts={layouts} breakpoints={BREAKPOINTS} cols={colsFor(...)} rowHeight={24} margin={[10,10]} containerPadding={[10,10]} isResizable={false} isDraggable={false}>`. Drop the `width` prop (WidthProvider supplies it) and the `gridWidth` IIFE at L308-312 — but keep honoring `gridCanvasMode: 'fixed'` on desktop by setting `style={{ maxWidth: fixedPx }}` on the wrapper when set and viewport >= 996px.
- L315: change `min-w-[720px]` → `min-w-0`.
- L67: remove the 480 clamp (`Math.max(480, …)` → `Math.max(320, …)`) — canvasW is still used elsewhere in that file's debug globals; the grid no longer consumes it.
- Item children: iterate `layouts.desktop` for keys (children keys must be the union of widget ids; RGL matches children to layout items by key, so rendering one child per widget id works for every breakpoint).

**6. Builder breakpoint switcher** — `frontend/src/app/page.tsx`:
- Add `const [editBp, setEditBp] = useState<BreakpointKey>('desktop')` and `const layoutsRef = useRef<Partial<Record<BreakpointKey, RGLLayout[]>>>({})`.
- Hydration (L534-537, L545-567, L574-599): populate `layoutsRef.current` from `def.layouts ?? { desktop: def.layout }`; keep the existing orphan reconciliation but call the new `reconcileOrphans` helper instead of the inline block; `setLayoutState(layoutsRef.current.desktop ?? [])`.
- Switcher handler: on change to `bp`, stash `layoutsRef.current[editBp] = layoutState`, then `setLayoutState(layoutsRef.current[bp] ?? deriveLayouts({...}, configs, dashOptions.gridSize)[bp])`, `setEditBp(bp)`. Derived layouts become "stored" only after the user drags/resizes at that breakpoint (i.e., first `scheduleServerSave` while `editBp === bp`).
- `cols` memo (L1055-1058): `colsFor(dashOptions.gridSize)[editBp]`.
- `gridWidth` (L1088-1092): when `editBp === 'phone'` return `390`; when `'tablet'` return `Math.min(gridWidth, 820)`; desktop unchanged. Center the grid with `mx-auto` when narrowed so the builder previews the device width.
- `scheduleServerSave` (L672-704): inside `doSave`, build
  ```ts
  const layouts = { ...layoutsRef.current, [editBp]: def.layout }
  for (const bp of Object.keys(layouts)) layouts[bp] = reconcileOrphans(layouts[bp], sanitizedWidgets)
  layoutsRef.current = layouts
  definition: { layout: layouts.desktop ?? def.layout, layouts, widgets: sanitizedWidgets, options }
  ```
  The `{ layout, widgets }` parameter shape at all ~20 call sites is unchanged — `layout` now means "active-breakpoint layout"; no call-site edits needed. `reconcileOrphans` in the save path is what keeps add-widget (L874-908), remove (L918), and duplicate (L925-929) consistent across the non-active stored breakpoints.
- Draft persistence (L646-648 and L417, L449, L567, L599): include `layouts: { ...layoutsRef.current, [editBp]: layoutState }` in the `dashboardDraft` JSON; load path prefers `draft.layouts`.
- `gridSize` rescale effect (L236-259): scope it to the desktop layout only (`if (editBp !== 'desktop') also rescale layoutsRef.current.desktop`; tablet/phone cols are fixed at 8/2 and unaffected).
- `packRowsLeftAndFill` (L140) and `normalizeLayoutToFullCols` (L1110) already read the `cols` memo — they work per active breakpoint with no change.
- UI: add a 3-button segmented control (Desktop / Tablet / Phone) next to the existing grid-size control wired through `TitleBar` (`frontend/src/components/builder/TitleBar.tsx` — `gridSize` select at L284; add `editBp` + `onEditBpChangeAction` props following the same pattern as `gridSize`/`onGridSizeChangeAction` at page.tsx L1341-1343).

**7. Backward compat / rollout**
- Old dashboards (no `layouts`): viewer derives tablet/phone on the fly; builder writes `layouts` on first save. Nothing breaks if the spec ships frontend-first, EXCEPT step 1 must land first or the backend strips `layouts` from every save (Pydantic default behavior) — **merge order: backend schema line → frontend**.
- Old frontend reading new definitions: ignores `layouts`, keeps using `layout` (which is always written as the desktop mirror). Safe.

## Files to Modify

- `backend/app/schemas.py` — add `layouts` field to `DashboardDefinition` (L165-169).
- `frontend/src/lib/api.ts` — `BreakpointKey` type; `layouts?` on `DashboardDefinition` (L1038-1055).
- `frontend/src/lib/gridBreakpoints.ts` — NEW: breakpoints/cols constants, `rescaleLayout`, `stackLayout`, `reconcileOrphans`, `deriveLayouts`.
- `frontend/src/types/react-grid-layout.d.ts` — declare `Responsive` + `WidthProvider`.
- `frontend/src/app/v/[id]/page.tsx` — `WidthProvider(Responsive)`, per-breakpoint layouts, remove `min-w-[720px]` and 480px clamp.
- `frontend/src/app/page.tsx` — `editBp` state + `layoutsRef`, switcher wiring, `scheduleServerSave` layouts merge + reconcile, hydration/draft handling, cols/gridWidth per breakpoint, replace inline orphan block and gridSize-cols map with helpers.
- `frontend/src/components/builder/TitleBar.tsx` — breakpoint switcher control (props + segmented buttons).

No new dependencies. No DB migration.

## Acceptance Criteria

- [ ] Backend `DashboardSaveRequest` round-trips `definition.layouts` (save then GET returns it unchanged).
- [ ] `definition.layout` is still written on every save and equals `layouts.desktop`.
- [ ] Public `/v/{publicId}` on a 375px-wide viewport shows widgets stacked full-width with no horizontal page scroll; on >= 996px it matches the pre-change desktop rendering.
- [ ] A dashboard saved before this change (no `layouts` key) renders correctly at all three breakpoints without resaving.
- [ ] Builder switcher edits tablet/phone layouts independently; switching back to Desktop shows the untouched desktop layout; edits at any breakpoint persist across reload.
- [ ] Adding a widget while editing Phone makes it appear in Desktop/Tablet layouts too (appended at bottom via reconcile); deleting a widget removes it from all stored breakpoint layouts.
- [ ] Orphaned widgets (in `widgets`, missing from layout) are still auto-appended on load — behavior identical to the old page.tsx L546-562 block for the desktop layout.
- [ ] `gridCanvasMode: 'fixed'` still constrains the desktop grid width in builder and public view.
- [ ] `pnpm tsc --noEmit` (or `npm run build`) passes in `frontend/`.

## Verification

```bash
# 1. Types/build
cd /Users/mohammed/Documents/Bayan/frontend && npx tsc --noEmit && npm run build

# 2. Backend round-trip (backend running on :8000)
curl -s -X POST http://localhost:8000/api/dashboards \
  -H 'Content-Type: application/json' \
  -d '{"name":"bp-test","userId":"dev_user","definition":{"layout":[{"i":"a","x":0,"y":0,"w":6,"h":4}],"layouts":{"desktop":[{"i":"a","x":0,"y":0,"w":6,"h":4}],"phone":[{"i":"a","x":0,"y":0,"w":2,"h":4}]},"widgets":{"a":{"id":"a","type":"kpi","title":"t"}}}}' \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); assert d["definition"].get("layouts",{}).get("phone"), "layouts stripped"; print("OK", d["id"])'

# 3. Manual: builder (frontend on :3000)
#    - Open /?id=<existing pre-change dashboard>. Confirm layout renders (legacy path).
#    - Switch to Phone in the TitleBar switcher: widgets appear stacked at ~390px width.
#    - Drag one widget on Phone, wait >1s (debounce), reload, switch to Phone: order kept.
#    - Switch to Desktop: original desktop layout unchanged.
#    - Add a widget while on Phone; switch to Desktop: widget present at bottom.
# 4. Manual: public view
#    - Publish; open /v/<publicId> in devtools responsive mode at 375px: stacked, no
#      horizontal scroll. At 1280px: identical to pre-change screenshot.
#    - window.__layout / __cols debug globals still populated.
# 5. Embed regression: open an existing /render/embed/widget?... iframe URL — unchanged.
```

## Out of Scope

- `render/embed/widget` route changes (single fixed-size widget, no grid — verified no `GridLayout` usage).
- Editable mobile layouts on the public route (viewer stays read-only).
- Per-breakpoint widget visibility (hide widget on phone) — future option, schema already leaves room under `layouts`.
- Touch drag-and-drop polish in builder on actual mobile devices (builder remains a desktop tool; the switcher only previews device widths).
- Replacing the local `react-grid-layout` type shim with `@types/react-grid-layout`.
