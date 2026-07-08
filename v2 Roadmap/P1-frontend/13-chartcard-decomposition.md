---
id: 13-chartcard-decomposition
title: Decompose ChartCard and other frontend monoliths
priority: P1
effort: XL
depends_on: []
area: frontend
---

## Problem

Three frontend files are unmaintainable monoliths that re-render expensively and defeat code-splitting:

- `frontend/src/components/widgets/ChartCard.tsx` — **7,999 lines**, one component function spanning lines 369–7802. Renders 16 chart types via a chain of `if (type === ...)` blocks inside a single IIFE. Statically imports `recharts` (used only by the `spark` type) and `react-dom/server`, so every consumer pays for them. Not memoized, so any parent state change in the dashboard grid re-runs ~7,400 lines of hook/shaping logic per chart widget.
- `frontend/src/components/builder/ConfiguratorPanel.tsx` — **6,595 lines** (58 `useState` calls). A decomposed replacement `ConfiguratorPanelV2/` (2,518 lines across 6 files) already exists but is gated behind a UI toggle defaulting to off.
- `frontend/src/components/builder/ReportBuilderModal.tsx` — **3,973 lines**, but already internally structured as ~10 self-contained subcomponents in one file — a mechanical file split.

Bonus rot: `plotly.js` (^3.1.0) and `react-plotly.js` (^2.6.0) are in `frontend/package.json` (lines 45, 51) with **zero imports anywhere in `src/`** — dead weight in `node_modules` and lockfile.

## Current State

All refs verified 2026-07-07 on branch `feature/alpha-themes-foundation`.

### ChartCard.tsx (`frontend/src/components/widgets/ChartCard.tsx`)

Module layout:
- L3–37: static imports, including eager `recharts` (L30–37: `ResponsiveContainer, AreaChart as ReAreaChart, ...`) and `ReactDOMServer` (L23) — both bundled into ChartCard's chunk for every widget type.
- L40–72: `next/dynamic` wrappers for Tremor components (`LineChart`, `BarChart`, `AreaChart`, `TremorDonutChart`, `TremorCategoryBar`, `TremorProgressBar`, `TremorBarList`, Tremor table parts, `TremorBadge`) and `ReactECharts` (L47). These are lazily chunked, but all declared in one module.
- L75–367: module-level helpers — `toProperCase` (75), `splitLegend` (86), `extractBaseLabel` (96), `fallbackStringsFor` (101), `parseDateLoose` (121), `useDistinctStrings` (148), `useDebounced` (363).
- L369: `export default function ChartCard({ title, sql, datasourceId, type, options, queryMode, querySpec, customColumns, widgetId, pivot, reservedTop, layout, tabbedGuard, tabbedField })` — accepted `type` union at L389: `'line' | 'bar' | 'area' | 'column' | 'donut' | 'categoryBar' | 'spark' | 'combo' | 'badges' | 'progress' | 'tracker' | 'scatter' | 'tremorTable' | 'barList' | 'gantt' | 'sankey'`.
- L401–3335: data-shaping hook region: `querySpec` normalization (401), `uiWhere` state (619), datasource defaults query (722), tab values/totals (806–957), **main data `useQuery` (959)**, `categories` memo (1981), `data` memo (2155), `displayData` memo (2244), `chartInstanceKey` (2391), filterbar/delta mode state (2453–2483), **delta `useQuery` (2786)**, `chartColorsTokens` (3273), `legendHexColors` (3306).
- L3336: `const content = (() => { ... })()` — giant render IIFE with type dispatch:
  - 3468: advanced ECharts path for `bar|column|line|area` (`advancedMode || forceAdvanced`), area delegated at 4568–4576 to `renderAdvancedAreaChart` from `./echarts/AreaAdvanced` (comment: "refactor step")
  - 4780 `gantt` → delegates to `GanttCard` (already a separate 290-line component)
  - 4797 `barList`, 4821 `tremorTable` → Tremor
  - 4944 `scatter` → ECharts
  - 5392 `categoryBar`, 5442 `progress` → Tremor + `HexProgressBar`
  - 5563 `badges` → raw spans/Tailwind
  - 5805 `tracker` → `./Tracker` (dynamic)
  - 5896 `combo` → ECharts
  - 6222 `spark` → **recharts** (the only consumer of the eager import)
  - 6737 `bar|column`, 6831 `area` → Tremor simple mode
  - 6936 `sankey` → `renderSankey` from `./echarts/SankeyChart`
  - 7073 `donut` → ECharts pie presets (`./echarts/PiePresets`) + Tremor fallback
  - tail → `line` default (Tremor `LineChart`)
- L7230 `renderLegend`, L7505 `renderChartTitle`, L7750–7802 final JSX chrome (tabs, filterbar, title above/below, legend top/bottom).
- L7803–7999: trailing helpers `FormatMode` type + `formatNumber` (7827), `toNum` (7932), `formatDatePattern` (7939).

`formatNumber`/`FormatMode` are **duplicated in 4 files**: `ChartCard.tsx`, `widgets/echarts/AreaAdvanced.tsx` (L9–130), `widgets/KpiCard.tsx`, `widgets/PivotMatrixView.tsx`.

Existing decomposition patterns to extend (all already in repo):
- `frontend/src/components/widgets/echarts/` — per-preset renderer modules: `AreaAdvanced.tsx` (321 ln, `renderAdvancedAreaChart(args)` at L129), `PiePresets.tsx` (206), `SankeyChart.tsx` (208), `HeatmapPresets.tsx` (281). Each does its own local `dynamic(() => import('echarts-for-react'))`.
- `frontend/src/components/widgets/useKpiData.ts` (525 ln) — data hook extracted from KpiCard; the model for ChartCard's hook extraction.

ChartCard importers (6): `app/page.tsx:13`, `app/render/embed/widget/page.tsx:6`, `app/v/[id]/page.tsx:12`, `components/ai/AiAssistDialog.tsx:12`, `components/builder/ConfiguratorPanel.tsx:31`, `components/widgets/CompositionCard.tsx:6`. Render sites: `app/page.tsx:1545`, `CompositionCard.tsx:273`. No `React.memo` anywhere in `ChartCard.tsx` or at the `app/page.tsx` call site.

### ConfiguratorPanel (`frontend/src/components/builder/`)

- `ConfiguratorPanel.tsx` (6,595 ln) — legacy; still the **default** panel.
- `ConfiguratorPanelV2/` — `index.tsx` (368), `GeneralTab.tsx` (197), `DataTab.tsx` (435), `DataTabHelpers.tsx` (733), `VisualizeTab.tsx` (732), `shared.tsx` (53).
- Toggle: `app/page.tsx:307` `const [useV2Panel, setUseV2Panel] = useState(false)`; switch at `app/page.tsx:1729–1731` renders V2 or V1 with identical props `{ selected, allWidgets, quickAddAction }`.
- `ConfiguratorPanelV2/GeneralTab.tsx:10` already lazy-loads ReportBuilderModal via `dynamic()`; legacy `ConfiguratorPanel.tsx:23` imports it statically.

### ReportBuilderModal (`frontend/src/components/builder/ReportBuilderModal.tsx`)

Single file containing distinct components: `ExpressionBuilder` (L42), `VariableEditor` (249), `ManualFilterValues` (1138), `DateRuleEditor` (1244), `StringRuleEditor` (1467), `NumberRuleEditor` (1520), `ReportFieldFilter` (1580), `FilterEditor` (1671), `ImageProps` (1787), `ElementProps` (1870), constants (2470–2476), main `export default function ReportBuilderModal` (3498).

### Build

No test runner configured (`package.json` scripts: `dev`, `build` = `next build --no-lint`, `start`, `lint`, `clean`). Verification is `npm run build` + manual dashboard smoke.

## Desired State

- `ChartCard.tsx` is a <600-line orchestrator: props → data hooks → lazy per-type renderer → shared chrome (legend/title/tabs/filterbar). Each chart-type renderer lives in its own file and dynamic-imports only the chart lib it uses. `recharts` and `react-dom/server` are no longer in the eager ChartCard chunk. `ChartCard` and each renderer are memoized so unrelated dashboard state changes don't re-run shaping logic.
- One shared `formatNumber`/`FormatMode` in `lib/format.ts`; 4 duplicates deleted.
- `plotly.js` + `react-plotly.js` removed from `package.json`.
- `ConfiguratorPanelV2` is the default panel; legacy `ConfiguratorPanel.tsx` deleted after parity bake.
- `ReportBuilderModal` split into `components/builder/report/` files; import path unchanged via re-export.

### Target module structure

```
frontend/src/lib/format.ts                       # FormatMode + formatNumber + toNum + formatDatePattern (moved from ChartCard tail)
frontend/src/components/widgets/chart/
  ChartCard.tsx                                  # orchestrator (moved; old path re-exports)
  types.ts                                       # ChartType union, ChartRenderContext interface
  useChartQuery.ts                               # querySpec normalize (L401), uiWhere/global-filter events (L619–777), ds defaults (722), tabs (806–957), main useQuery (959)
  useChartData.ts                                # categories (1981), data (2155), displayData (2244), applyXCase, chartInstanceKey, yLooksNumeric
  useChartDeltas.ts                              # filterbarMode (2453), deltaDateField, effectiveDeltaMode, delta useQuery (2786)
  useChartColors.ts                              # chartColorsTokens (3273), legendHexColors (3306)
  helpers.ts                                     # toProperCase, splitLegend, extractBaseLabel, fallbackStringsFor, parseDateLoose, useDistinctStrings, useDebounced
  chrome/ChartLegend.tsx                         # renderLegend (7230)
  chrome/ChartTitle.tsx                          # renderChartTitle (7505)
  renderers/
    registry.ts                                  # Record<ChartType, dynamic(() => import('./X'))>
    AdvancedEcharts.tsx                          # 3468–4778 advanced bar/column/line/area (keeps AreaAdvanced delegation)
    TremorSimple.tsx                             # simple line (default) + bar/column (6737) + area (6831)
    Scatter.tsx                                  # 4944 (echarts)
    Combo.tsx                                    # 5896 (echarts)
    Donut.tsx                                    # 7073 (echarts PiePresets + tremor fallback)
    Sankey.tsx                                   # 6936 (wraps echarts/SankeyChart)
    Spark.tsx                                    # 6222 (recharts — the ONLY file importing recharts)
    BarList.tsx                                  # 4797 (tremor)
    TremorTable.tsx                              # 4821 (tremor)
    CategoryBar.tsx                              # 5392 (tremor)
    Progress.tsx                                 # 5442 (tremor)
    Badges.tsx                                   # 5563 (no chart lib)
    Tracker.tsx                                  # 5805 (wraps widgets/Tracker)
    Gantt.tsx                                    # 4780 (wraps widgets/GanttCard)
frontend/src/components/widgets/echarts/         # unchanged, consumed by renderers/
frontend/src/components/builder/report/          # ReportBuilderModal split (see plan step 8)
```

Renderers receive a single typed props object (`ChartRenderContext` in `chart/types.ts`): `{ type, options, querySpec, displayData, data, categories, series, colors: { tokens, hex }, q (query result), deltas, markChartReady, isSnap, echartsRef, widgetId, layout }` — assembled once in the orchestrator, passed down; renderers are pure functions of it.

## Implementation Plan

Ordered so the app builds and works after every step. Each step is an independent commit.

**Step 0 — dead deps (5 min).** Remove `"plotly.js"` and `"react-plotly.js"` from `frontend/package.json`; run `npm install` to update lockfile. (Verified zero imports: `grep -rin plotly frontend/src/` is empty.)

**Step 1 — shared format lib.** Create `frontend/src/lib/format.ts` exporting `FormatMode`, `formatNumber`, `toNum`, `formatDatePattern` — lift verbatim from `ChartCard.tsx:7803–7999` (it is the superset copy). Replace the local copies in `ChartCard.tsx`, `widgets/echarts/AreaAdvanced.tsx` (L9–130), `widgets/KpiCard.tsx`, `widgets/PivotMatrixView.tsx` with imports. Diff the deleted copies against `lib/format.ts` first; if a copy has divergent cases, merge the union into the lib version.

**Step 2 — scaffold `widgets/chart/`.** Create `chart/types.ts` (move the `type` union from ChartCard L389 to an exported `ChartType`; define `ChartRenderContext`). Create `chart/helpers.ts` (move ChartCard L75–367 helpers verbatim; they take no component state). ChartCard imports from both. No behavior change.

**Step 3 — extract leaf renderers (one commit per type, safest first).** Order: `Gantt` (already a delegation, 4780), `Badges` (5563, no lib), `Tracker` (5805), `BarList` (4797), `TremorTable` (4821), `CategoryBar` (5392), `Progress` (5442), `Spark` (6222 — move the recharts import here and **delete the static recharts import at ChartCard L30–37**), `Donut` (7073), `Sankey` (6936), `Scatter` (4944), `Combo` (5896), `TremorSimple` (6737 + 6831 + line default), `AdvancedEcharts` (3468–4778, last — biggest). Mechanics per type: cut the `if (type === 'X') { ... }` block into `renderers/X.tsx` as `export default function XRenderer(ctx: ChartRenderContext)`, add whatever `ctx` fields the block closes over (grow `ChartRenderContext` as needed), replace the block in the IIFE with `return <XRenderer {...ctx} />`. Each renderer that uses a chart lib declares its own `dynamic()` wrapper locally (copy the pattern from `echarts/AreaAdvanced.tsx:7`). Delete the corresponding module-level dynamic wrappers from ChartCard (L40–72) once their last in-file user is gone.

**Step 4 — registry + lazy loading.** Create `renderers/registry.ts`: `const RENDERERS: Record<ChartType, ComponentType<ChartRenderContext>> = { line: dynamic(() => import('./TremorSimple')), ... }`. Replace the remaining `content` IIFE dispatch with `const Renderer = RENDERERS[type] ?? RENDERERS.line; content = <Renderer {...ctx} />`. Keep the pre-dispatch advanced-mode decision (L3459–3468 `forceAdvanced` logic) in the orchestrator — it picks `AdvancedEcharts` vs `TremorSimple` for `bar|column|line|area`.

**Step 5 — extract hooks.** Move hook regions into `useChartQuery.ts`, `useChartData.ts`, `useChartDeltas.ts`, `useChartColors.ts` per the target structure. Rule: move code verbatim, pass dependencies as arguments, return what the orchestrator/ctx needs. The `window` event listeners (`config-where-change` L649, `global-filters-break-change` L777) move with `useChartQuery`. Also move `ReactDOMServer` tooltip usage into whichever renderer/hook consumes it (grep `ReactDOMServer` in file) so the static import at L23 leaves the orchestrator.

**Step 6 — memo boundaries.** Wrap each renderer export in `React.memo`. Wrap the orchestrator: `export default React.memo(ChartCard)` — its props from `app/page.tsx:1545` are config-object references, stable unless the widget config actually changes. Wrap `chrome/ChartLegend` and `chrome/ChartTitle` in `memo`. Do NOT add prop-diffing custom comparators; default shallow compare is enough because parents pass object refs from stored config. Move `ChartCard.tsx` to `chart/ChartCard.tsx` and leave `components/widgets/ChartCard.tsx` as a one-line re-export (`export { default } from './chart/ChartCard'`) so the 6 importers are untouched.

**Step 7 — ConfiguratorPanel V2 cutover.** (a) Flip default: `app/page.tsx:307` → `useState(true)`. (b) Parity audit: open each widget kind (kpi/chart/table/pivot/text/report) in V2, compare against V1 controls; V1 sections missing from V2 get ported into the matching `ConfiguratorPanelV2/*Tab.tsx`. (c) After one release bake with the toggle still present, delete `ConfiguratorPanel.tsx`, the toggle state/button (`app/page.tsx:307, 1699–1704, 1729–1731`), and the `import ConfiguratorPanel` at `app/page.tsx:21`.

**Step 8 — ReportBuilderModal file split (mechanical).** Create `components/builder/report/` and move: `ExpressionBuilder.tsx` (L42–248), `VariableEditor.tsx` (249–1137), `filters.tsx` (`ManualFilterValues`, `DateRuleEditor`, `StringRuleEditor`, `NumberRuleEditor`, `ReportFieldFilter`, `FilterEditor` — L1138–1786), `ElementProps.tsx` (`ImageProps` + `ElementProps`, L1787–2469), `constants.ts` (`PERIOD_PRESETS` etc., L2470–2497), `ReportBuilderModal.tsx` (main, L3498+). Keep `components/builder/ReportBuilderModal.tsx` as a re-export of `./report/ReportBuilderModal` (both importers — `ConfiguratorPanel.tsx:23`, `ConfiguratorPanelV2/GeneralTab.tsx:10` — keep working, including the `dynamic()` one).

Backward compat: no prop, API, or widget-config schema changes anywhere; old import paths preserved via re-export files; per-type render output must be pixel-identical (code is moved, not rewritten).

## Files to Modify

| Path | Change |
|---|---|
| `frontend/package.json` + lockfile | remove `plotly.js`, `react-plotly.js` |
| `frontend/src/lib/format.ts` | NEW — shared `FormatMode`/`formatNumber`/`toNum`/`formatDatePattern` |
| `frontend/src/components/widgets/ChartCard.tsx` | becomes re-export of `chart/ChartCard` |
| `frontend/src/components/widgets/chart/**` | NEW — orchestrator, types, helpers, 4 hooks, chrome/, renderers/ (16 files) per target structure |
| `frontend/src/components/widgets/echarts/AreaAdvanced.tsx` | delete local formatNumber copy, import from `lib/format` |
| `frontend/src/components/widgets/KpiCard.tsx`, `PivotMatrixView.tsx` | same dedupe |
| `frontend/src/app/page.tsx` | L307 flip `useV2Panel` default; later delete toggle + V1 import (L21, 1699–1704, 1729–1731) |
| `frontend/src/components/builder/ConfiguratorPanel.tsx` | DELETE (step 7c, after bake) |
| `frontend/src/components/builder/ConfiguratorPanelV2/*.tsx` | port any parity gaps found in audit |
| `frontend/src/components/builder/ReportBuilderModal.tsx` | becomes re-export of `report/ReportBuilderModal` |
| `frontend/src/components/builder/report/**` | NEW — 6 split files |

## Acceptance Criteria

- [ ] `npm run build` passes after every step (each step is a working commit).
- [ ] `plotly.js`/`react-plotly.js` absent from `package.json` and lockfile.
- [ ] Exactly one `formatNumber` definition in `frontend/src/` (in `lib/format.ts`): `grep -rn "function formatNumber" frontend/src | wc -l` → 1.
- [ ] `chart/ChartCard.tsx` orchestrator ≤ 600 lines; no `if (type === '...')` render blocks remain in it.
- [ ] `recharts` imported only from `chart/renderers/Spark.tsx`; `echarts-for-react` only from renderer/echarts files; no chart-lib import in the orchestrator.
- [ ] All 16 chart types render identically pre/post (manual smoke on a dashboard containing each type).
- [ ] `ChartCard` and every renderer wrapped in `React.memo`; typing in an unrelated dashboard input no longer re-renders chart widgets (check React DevTools Profiler).
- [ ] `useV2Panel` defaults to true; V1 panel deleted after bake with no console errors editing each widget kind.
- [ ] `ReportBuilderModal` opens and saves a report unchanged; `components/builder/report/` contains the split files.
- [ ] Old import paths (`@/components/widgets/ChartCard`, `@/components/builder/ReportBuilderModal`) still resolve — all 6 + 2 importers untouched.

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/frontend

# dead deps + dedupe
grep -rin plotly src/ package.json | grep -v lock   # expect: no hits
grep -rn "function formatNumber" src | wc -l         # expect: 1

# lib isolation
grep -rln "from 'recharts'" src                      # expect: only chart/renderers/Spark.tsx
grep -rln "echarts-for-react" src                    # expect: only renderers/* and widgets/echarts/*

# build
npm run build                                        # must pass at every step

# bundle check (chunk split per lib)
ANALYZE=1 npx next build --no-lint 2>/dev/null || du -sh .next/static/chunks | sort -h | tail -20
```

Manual smoke (app on :3000, backend running):
1. Open a dashboard containing each chart type (line, bar, column, area, donut, categoryBar, spark, combo, badges, progress, tracker, scatter, tremorTable, barList, gantt, sankey) — visually compare with pre-refactor screenshots.
2. Toggle a global filter → all charts refetch; toggle advanced mode on a bar chart → echarts path renders.
3. Embed page `/render/embed/widget?...` and share page `/v/[id]` still render charts (they import ChartCard directly).
4. Configurator V2: select each widget kind, change a setting in every tab, confirm it persists after save + reload.
5. Report builder: open from configurator, add a variable + filter + image element, save, reopen.
6. React DevTools Profiler: record while typing in dashboard title input → chart widgets show no re-render.

## Out of Scope

- Rewriting any chart rendering logic or visual changes (pure code motion).
- `HeatmapCard`, `TableCard`/`AgTable`, `KpiCard`, `PivotMatrixView` decomposition (separate files, already reasonable or separate effort).
- `app/page.tsx` (itself large) decomposition.
- Replacing Tremor with ECharts (or any lib consolidation) — the per-renderer split makes that a follow-up per-file swap.
- Adding a test runner; verification stays build + manual smoke per current repo convention.
- Server components / RSC migration.
