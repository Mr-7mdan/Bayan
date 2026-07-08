---
id: 20-list-virtualization-and-memoization
title: Virtualize large lists/tables and add memoization
priority: P2
effort: L
depends_on: []
area: frontend
---

## Problem

No list virtualization exists anywhere in the frontend (`react-window` / `@tanstack/react-virtual` absent from `frontend/package.json`). Three hot spots render unbounded DOM:

1. **PivotMatrixView** (`frontend/src/components/widgets/PivotMatrixView.tsx`, 1395 lines) renders a raw `<table>` with every visible row × column leaf. A pivot with 2 row dims × 1 col dim can easily produce thousands of `<tr>` and tens of thousands of `<td>`. It also has O(n²) hot paths and `console.log` inside cell render.
2. **AgTable** (`frontend/src/components/widgets/AgTable.tsx`) defaults `domLayout` to `'autoHeight'`, which **disables AG Grid's built-in row virtualization** — all rows render into the DOM. TableCard page sizes go up to 500 rows.
3. **FilterbarControl** manual-value pickers render every distinct value as a `<li>`; `loadDistinct` in TableCard pages up to 500k values, so high-cardinality fields render tens of thousands of checkboxes with an O(n²) `sel.includes(v)` per item.

Additionally, no widget component is wrapped in `React.memo`. Every `page.tsx` state change (widget selection, kebab menu open, load-time events, drag) re-renders **all** widget cards, and TableCard passes freshly-allocated `rows`/`columns`/options objects to its children on every render, defeating any child memoization.

## Current State

All refs verified 2026-07-07 on branch `feature/alpha-themes-foundation`.

- `frontend/src/components/widgets/AgTable.tsx:199` — `const domLayout = tableOptions?.performance?.domLayout || 'autoHeight'` → row virtualization off. Line 56: `export default function AgTable({` (no memo).
- `frontend/src/components/widgets/TableCard.tsx:701-705` — rows re-allocated every render, new identity passed to AgTable/PivotMatrixView:
  ```ts
  const rows: Row[] = ((q.data?.rows as Array<Array<unknown>>) || []).map((r) => { ... })
  ```
- `frontend/src/components/widgets/TableCard.tsx:228-233` — `console.debug('[TableCard] [FiltersDebug] ...')` executed in render body (dev).
- `frontend/src/components/widgets/TableCard.tsx:806-823` — `DateRangeMenu` component defined inside TableCard body (new component type per render → remount).
- `frontend/src/components/widgets/TableCard.tsx:1060-1070` — `pivotViewTableOptions` / `pivotForMatrix` built inline in JSX per render.
- `frontend/src/components/widgets/TableCard.tsx:1101-1108` — `<PivotMatrixView rows={rows} columns={(q.data?.columns as string[]) || []} ... />` (fresh arrays).
- `frontend/src/components/widgets/TableCard.tsx:1115-1120` — `<AgTable ... onFilterWhereChangeAction={(w) => {...}} />` inline lambda.
- `frontend/src/components/widgets/TableCard.tsx:1132` — page sizes `[10,25,50,100,200,500]`.
- `frontend/src/components/widgets/PivotMatrixView.tsx`:
  - `:721-747` — `valueAt` is a plain closure recreated every render.
  - `:844` — `sortedVisRowLeaves` useMemo lists `valueAt` in deps → **memo recomputes every render** (full sort of all leaves).
  - `:865` — `visibleRowKeys` useMemo lists `nearestCollapsedPrefixFor` (plain closure) in deps → same defect.
  - `:1082` — `visibleRowKeys.map(...)` renders every row; `:1162` inner `colLeaves.map(...)` renders every cell.
  - `:1171, :1222, :1255, :1318-1320 (approx 1349), :920` — `colLeaves.find(...)` linear scans inside per-cell/per-row render → O(rows × cols²).
  - `:1205-1233` — row-total cell loops all `colLeaves` per row inside JSX.
  - `:1379, :1382` — `console.log('[Pivot] Total row grand total...')` inside grand-total cell render, per visible row per render.
  - `:109` — `export default function PivotMatrixView(...)` (no memo). Only consumer: TableCard's dynamic import (`TableCard.tsx:18`).
  - `:447` — fixed `rowHeight` already derived from options/density (needed for virtualization).
  - `:899` — root `<div className="rounded-md border ... overflow-hidden">`; the scroll container is currently the TableCard wrapper (`TableCard.tsx:1074` `overflow-auto`).
- `frontend/src/components/shared/FilterbarControl.tsx`:
  - `:171-179` (NumberRuleInline) and `:370-` / `:418-430` (StringRuleInline) — `opts`/`sortedOpts`/`filtered` computed unmemoized every render (full sort), then `:233` / `:420` `filtered.map(...)` renders all values inside a `max-h-56 overflow-auto` div; `:235` `sel.includes(v)` is O(n) per item.
- `frontend/src/app/page.tsx:1453-1576` — GridLayout children map renders `KpiCard`/`ChartCard`/`TableCard`/`HeatmapCard` inline with cfg-derived props. None memoized: `ChartCard.tsx:369`, `KpiCard.tsx:40`, `HeatmapCard.tsx:221` all `export default function ...`.
- `frontend/package.json` — has `ag-grid-community`/`ag-grid-react` ^34.2.0 and `@tanstack/react-query`; no virtualization lib.

## Desired State

- AG Grid's native row virtualization is active for large data tables (lean on the installed dependency; no custom windowing for AgTable).
- PivotMatrixView windows its body rows via `@tanstack/react-virtual` above a row threshold, keeping DOM `<tr>` count bounded (~50) regardless of data size; O(n²) scans and render-path console logs are gone.
- FilterbarControl value lists are virtualized and selection lookups are O(1).
- `React.memo` boundaries on TableCard, ChartCard, KpiCard, HeatmapCard, AgTable, PivotMatrixView so page-level state changes (selection, menus, drag) no longer re-render every widget; TableCard passes referentially-stable props so the memos actually hold.
- A documented React Profiler measurement procedure with before/after numbers.

## Implementation Plan

### Phase 0 — baseline measurement (do first, keep numbers)
1. `cd frontend && npm run dev`, open a dashboard containing (a) a data table widget with page size 500 and (b) a pivot widget whose row dims produce 500+ leaf rows.
2. React DevTools → Profiler → "Record why each component rendered". Record: (i) click empty dashboard area (selection change), (ii) click a pivot column header to sort, (iii) change page on the data table. Note commit durations and which Card components re-rendered. Also note `document.querySelectorAll('.pvt-matrix tbody tr').length`.

### Phase 1 — memo boundaries + render-path fixes (no new dependency)
3. **PivotMatrixView.tsx**
   - Delete the two `console.log` calls at `:1379` and `:1382`.
   - Convert `valueAt` (`:721`) to `useCallback` (deps: `matrix`, `aggName`, `colDims.length`, `leavesByPrefix`, `collapsed`, `colLeaves`, `visibleColLeaves`). Convert `nearestCollapsedPrefixFor` (`:423`) and `colLeavesUnder` (`:345`) to `useCallback` similarly. This makes the `sortedVisRowLeaves` (`:783-844`) and `visibleRowKeys` (`:846-865`) memos actually cache.
   - Add one `useMemo` `colRepInfo: Map<string /*leaf ck*/, { collapsedUnder: string | null; rep: string | undefined }>` computed once from `colLeaves` + `colCollapsed` + `colRepByPrefix`, and replace every inline `colLeaves.find(...)` scan (`:318`, `:920`, `:1171`, `:1222`, `:1255`, and the two copies inside the totals row around `:1349`) with a map lookup.
   - Memoize the column-totals row values (`:1305-1390`): `useMemo` producing `{ perCol: Map<ck, number>, grand: number }` keyed on `visibleRowKeys`, `visibleColLeaves`, `valueAt`.
   - Change export to `export default React.memo(PivotMatrixView)` (keep the named function for devtools).
4. **AgTable.tsx** — wrap default export in `React.memo`.
5. **TableCard.tsx**
   - Memoize rows (`:701-705`): `const rows: Row[] = useMemo(() => ..., [q.data?.rows])`. Memoize `const columns = useMemo(() => (q.data?.columns as string[]) || [], [q.data?.columns])` and use it at `:1103` and `:1117`.
   - Move the `:228-233` `console.debug` into a `useEffect` (or delete it).
   - Hoist `DateRangeMenu` (`:806-823`) to module scope (it closes over nothing from TableCard).
   - Hoist the pivot-branch IIFE computations (`:1060-1070`) into `useMemo`s above the return: `pivotViewTableOptions` (deps: `options?.table`, `options?.yAxisFormat`, `(options as any)?.valueCurrency`, `pivot`) and `pivotForMatrix` (deps: `pivot`).
   - Wrap the AgTable filter callback (`:1119`) in `useCallback`: `const onGridFilterChange = useCallback((w) => { setPage(0); setGridWhere(w || {}) }, [])`.
   - Change export to `export default React.memo(TableCard)` — note TableCard renders itself recursively for tabs (`:922`, `:942`); memo is compatible.
6. **ChartCard.tsx (:369), KpiCard.tsx (:40), HeatmapCard.tsx (:221)** — convert `export default function X(...)` to named function + `export default React.memo(X)`. No prop changes needed: `page.tsx:1518-1576` already passes cfg-derived references that are stable per widget (configs state is spread-updated per id). Do NOT memo CompositionCard (receives inline `onUpdate`/`onSelectWidget` lambdas from `page.tsx:1594-1601`; out of scope).

### Phase 2 — lean on AG Grid virtualization
7. **AgTable.tsx:199** — enable row virtualization for large pages while preserving explicit user config:
   ```ts
   const domLayout = tableOptions?.performance?.domLayout
     || (rows.length > 100 ? 'normal' : 'autoHeight')
   ```
   When resolved layout is `'normal'`, give the grid a height: on the wrapper div (`:322-344`) add `style={{ ..., height: domLayout === 'normal' ? '65vh' : undefined }}`. AG Grid v34 virtualizes rows automatically in `'normal'` layout — no other changes.

### Phase 3 — @tanstack/react-virtual for pivot + filter lists
8. `cd frontend && npm install @tanstack/react-virtual@^3` (single small dependency; serves both consumers below).
9. **PivotMatrixView.tsx — windowed tbody**
   - Add `const scrollRef = useRef<HTMLDivElement|null>(null)` and put it on the root div (`:899`); change root class `overflow-hidden` → `overflow-auto` and add `style={{ maxHeight: '70vh' }}` when virtualizing (TableCard's wrapper at `TableCard.tsx:1074` stays as-is; nested scroll is fine because only one will actually overflow).
   - Gate: `const virtualize = visibleRowKeys.length > 150 && !showSubtotals` (subtotal rows inject extra `<tr>`s that break index math; skip virtualization there — `// ponytail: subtotal mode renders full; virtualize it if someone ships a 1000-parent subtotal pivot`).
   - `const rowVirtualizer = useVirtualizer({ count: visibleRowKeys.length, getScrollElement: () => scrollRef.current, estimateSize: () => rowHeight, overscan: 12, enabled: virtualize })` (`rowHeight` from `:447`).
   - Extract the current row-render body (`:1082-1302`, the `visibleRowKeys.map` callback) into a function `renderRow(rk: string, rowIdx: number)` (no behavior change), then in `<tbody>`:
     ```tsx
     {virtualize ? (
       <>
         <tr style={{ height: rowVirtualizer.getVirtualItems()[0]?.start ?? 0 }} />
         {rowVirtualizer.getVirtualItems().map(vi => renderRow(visibleRowKeys[vi.index], vi.index))}
         <tr style={{ height: rowVirtualizer.getTotalSize() - (rowVirtualizer.getVirtualItems().at(-1)?.end ?? 0) }} />
       </>
     ) : visibleRowKeys.map(renderRow)}
     ```
   - **rowSpan compat**: `rowHeaderSpans` (`:867-895`) merges repeated row-header labels via `rowSpan`, which cannot cross a window boundary. When `virtualize` is true, bypass spans — render every level cell with `rowSpan={1}` (i.e., treat `rowHeaderSpans[level][rowIdx]` as `1` and never skip a `th`). Repeated labels per row are the accepted trade-off; the collapsed/expand toggles still work because they key off `prefixesWithChildren`, not spans.
   - Make `<thead>` sticky so headers survive scrolling: add `className="sticky top-0 z-10"` to `<thead>` (`:902`) — harmless in non-virtual mode too.
   - Keep the existing column-totals `<tr>` (`:1305`) outside the windowed region (after the bottom spacer) — it renders once.
10. **FilterbarControl.tsx — NumberRuleInline (`:46`) and StringRuleInline (`:260`)**
    - Wrap `opts`/`sortedOpts`/`filtered` (`:171-179` and the StringRuleInline copies `:370`, `:418`) in `useMemo` keyed on `distinctCache?.[field]` and `q`.
    - `const selSet = useMemo(() => new Set(sel), [sel])`; replace `sel.includes(v)` (`:235`, `:422`) with `selSet.has(v)`.
    - Virtualize the value `<ul>` (`:231-248` and StringRuleInline equivalent `:418-434`): put a ref on the `max-h-56 overflow-auto` div, `useVirtualizer({ count: filtered.length, getScrollElement, estimateSize: () => 24, overscan: 10 })`, render items absolutely positioned inside a relative container of height `getTotalSize()` (standard tanstack list pattern; the rows are simple fixed-height `<li>`s so this is mechanical). Keep the loading/empty `<li>` fallbacks outside the virtual container.
    - `DateRuleInline` (`:447`) uses small static selects — leave alone.

### Phase 4 — re-measure
11. Repeat Phase 0 with the same dashboard. Record the same three interactions + DOM row counts into the PR description.

## Files to Modify

- `frontend/package.json` — add `@tanstack/react-virtual@^3` dependency.
- `frontend/src/components/widgets/PivotMatrixView.tsx` — useCallback for `valueAt`/helpers, `colRepInfo` map, memoized totals, delete console.logs, windowed tbody, sticky thead, `React.memo` export.
- `frontend/src/components/widgets/TableCard.tsx` — memoize `rows`/`columns`/pivot props, hoist `DateRangeMenu`, useCallback grid filter handler, move render-body debug log, `React.memo` export.
- `frontend/src/components/widgets/AgTable.tsx` — row-count-aware `domLayout` default + wrapper height, `React.memo` export.
- `frontend/src/components/widgets/ChartCard.tsx` — `React.memo` export.
- `frontend/src/components/widgets/KpiCard.tsx` — `React.memo` export.
- `frontend/src/components/widgets/HeatmapCard.tsx` — `React.memo` export.
- `frontend/src/components/shared/FilterbarControl.tsx` — memoized option lists, `Set` selection lookup, virtualized value lists in NumberRuleInline/StringRuleInline.

## Acceptance Criteria

- [ ] `@tanstack/react-virtual` installed; no other new dependencies.
- [ ] Pivot widget with 500+ visible rows renders ≤ ~60 `<tr>` in `.pvt-matrix tbody` (window + overscan + totals), and scrolling shows all rows with correct values, sorting, expand/collapse, and Excel export unchanged for ≤150-row pivots (export uses the DOM table — see note in Out of Scope for large pivots).
- [ ] Pivot with `showSubtotals` on renders identically to today (virtualization gated off).
- [ ] Data table with page size 500 uses AG Grid `domLayout='normal'` and DOM row count stays bounded while scrolling; `tableOptions.performance.domLayout`, if set by a user, is still honored.
- [ ] FilterbarControl manual-value list with 10k+ distinct values opens and filters without visible lag; DOM `<li>` count bounded (~20).
- [ ] Clicking dashboard background / opening a widget kebab menu re-renders **zero** TableCard/ChartCard/KpiCard/HeatmapCard instances (verified in React Profiler "why did this render").
- [ ] No `console.log` in PivotMatrixView render path (`grep -n "console.log" frontend/src/components/widgets/PivotMatrixView.tsx` → empty).
- [ ] `npm run build` passes.

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/frontend
npm install @tanstack/react-virtual@^3
grep -n "console.log" src/components/widgets/PivotMatrixView.tsx   # expect: no output
npm run build                                                       # expect: success
npm run dev
```

Manual (http://localhost:3000):
1. Open a dashboard with a pivot widget on a large source (row dims yielding 500+ leaves). In browser console: `document.querySelectorAll('.pvt-matrix tbody tr').length` → ≤ ~60. Scroll the pivot: rows fill in; totals row correct; click a column header to sort; expand/collapse a parent; "Download Excel" still produces a correct file for a ≤150-row pivot.
2. Data table widget → Rows per page 500 → grid scrolls with bounded DOM rows (inspect `.ag-center-cols-container` children while scrolling); column filters still emit server WHERE (change a filter, verify network request).
3. Filterbar: expose a high-cardinality field on a table widget, open manual mode, confirm smooth scroll and search; Select All / Deselect All still work.
4. React DevTools Profiler ("Record why each component rendered"): repeat the Phase-0 interactions. Selection click and kebab-menu open must show no Card commits; pivot sort commit duration should drop versus baseline (record both numbers in the PR).

## Out of Scope

- Virtualizing pivot **columns** (horizontal windowing) — column counts are typically small; revisit only if a real dashboard ships 200+ column leaves.
- Virtualizing subtotal-mode pivots (gated off; ceiling documented in code comment).
- Excel export for >150-row virtualized pivots exports only DOM-present rows today; fixing export to build from `visibleRowKeys`/`valueAt` instead of the DOM is a separate follow-up (note it in the PR).
- CompositionCard memoization (inline callbacks from page.tsx would need broader page.tsx refactor).
- ChartCard internal memoization (7,999 lines — the `React.memo` boundary is the cheap win; internal audit is its own spec).
- Server-side pivot pagination / row limits (backend change).
- Replacing PivotMatrixView with AG Grid enterprise pivoting (license cost; different feature set).
