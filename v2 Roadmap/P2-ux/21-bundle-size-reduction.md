---
id: 21-bundle-size-reduction
title: Reduce bundle size and consolidate chart libraries
priority: P2
effort: L
depends_on: ['13-chartcard-decomposition']
area: frontend
---

## Problem

`frontend/package.json` declares four+ overlapping heavy visualization libraries. Audit (2026-07-07, verified by grepping every import in `frontend/src`) shows:

- **8 dependencies with ZERO imports anywhere in `src/`** — pure dead weight in `node_modules`, lockfile, and audit surface: `plotly.js` (~3.5 MB min), `react-plotly.js`, `@mui/material`, `@mui/x-data-grid`, `@emotion/react`, `@emotion/styled`, `react-quill`, `echarts-countries-js`.
- **`recharts` is used but NOT declared** — `ChartCard.tsx` and `KpiCard.tsx` import it directly, resolved only as a phantom transitive dep of `@tremor/react@3` (which pins `recharts: ^2.13.3`). A tremor upgrade/removal silently breaks the build.
- **Two chart libs render widgets**: Tremor/recharts (basic line/bar/area/donut/barlist) and ECharts (sankey, sunburst, nightingale, heatmap, gantt, advanced area). Duplicate charting runtime shipped to every dashboard viewer.
- **ag-grid and reactflow are statically imported** into route entry chunks instead of loading per-widget.
- No bundle analysis tooling exists; `next.config.js` has no analyzer, and `build` runs `next build --no-lint` with `typescript.ignoreBuildErrors: true`, so nothing catches regressions.

## Current State

All refs verified 2026-07-07.

**Dead deps** — `frontend/package.json`:
- `@mui/material` (:16), `@mui/x-data-grid` (:17), `@emotion/react` (:14), `@emotion/styled` (:15), `plotly.js` (:45), `react-plotly.js` (:51), `react-quill` (:52), `echarts-countries-js` (:39). Grep for each across `src/`, `next.config.js`, `tailwind.config.*` returns only `package.json`/`package-lock.json` hits.

**Phantom recharts** — `frontend/src/components/widgets/ChartCard.tsx:30-37`:
```ts
import {
  ResponsiveContainer,
  AreaChart as ReAreaChart,
  Area as ReArea,
  Tooltip as ReTooltip,
  XAxis as ReXAxis,
  YAxis as ReYAxis,
} from 'recharts'
```
Same pattern at `frontend/src/components/widgets/KpiCard.tsx:15-21` (sparkline area chart). `recharts` absent from `package.json` dependencies.

**Chart lib split** — `frontend/src/components/widgets/ChartCard.tsx:40-59`: Tremor chart components loaded via `next/dynamic` (`LineChart`, `BarChart`, `AreaChart`, `TremorDonutChart`, `TremorCategoryBar`, `TremorProgressBar`, `TremorBarList`) plus `ReactECharts` (:47). Tremor render sites in ChartCard: `<TremorBarList` :4812, `<TremorProgressBar` :4904, :5516, `<TremorCategoryBar` :5410, `<BarChart` :6812, `<AreaChart` :6917, `<LineChart` :7192. KpiCard also dynamically loads Tremor `DonutChart` (:24-26) and uses `Metric` (:3).

**ECharts already dynamic + established pattern** — `frontend/src/components/widgets/echarts/` contains `AreaAdvanced.tsx`, `PiePresets.tsx` (donut/pie/sunburst/nightingale), `SankeyChart.tsx`, `HeatmapPresets.tsx`; plus `GanttCard.tsx:13`, `HeatmapCard.tsx:17`. All use `dynamic(() => import('echarts-for-react'), { ssr: false })`.

**Static heavy imports leaking into route chunks**:
- `frontend/src/components/widgets/AgTable.tsx:4-6` — static `ag-grid-react` + `ag-grid-community` (`ModuleRegistry`, `AllCommunityModule`, 4 themes). `AgTable` statically imported at `frontend/src/components/widgets/TableCard.tsx:11`; `TableCard` statically imported by `src/app/page.tsx:15` (builder) and `src/app/v/[id]/page.tsx:13` (viewer) → ag-grid lands in both main route chunks even when no table widget exists.
- `frontend/src/components/datasources/SchemaGraph.tsx:4` — static `reactflow`; statically imported at `src/app/(app)/datasources/[id]/page.tsx:6`.
- `frontend/src/components/datasources/ExecuteSqlDialog.tsx:9` — static `import * as ExcelJS from 'exceljs'` (~900 KB); contrast `PivotMatrixView.tsx:543` which already does `await import('exceljs')`.

**Tremor UI primitives (NOT chart) usage** — `Card`, `Title`, `Text`, `Select`, `Tabs`, `TextInput`, `Badge` statically imported across ~20 files (`app/(app)/**/page.tsx`, `DatasourceWizard.tsx`, etc.). Tailwind scans tremor at `tailwind.config` :9-10.

**Quill** — already lazy (`await import('quill')` at `contacts/page.tsx:108`); only its CSS is global (`src/app/globals.css:1`). Fine as-is.

**No analyzer** — `frontend/next.config.js` (42 lines) has no `@next/bundle-analyzer`; `package.json:8` build script is `next build --no-lint`.

## Desired State

- Dead deps removed; `recharts` never phantom (declared during transition, deleted at the end).
- **ECharts is the single chart library.** Justification: it already powers every advanced widget (sankey, sunburst, nightingale, heatmap, gantt), canvas rendering scales to large BI result sets, and the basic charts Tremor/recharts provide (line/bar/area/donut/barlist/progress) are trivial ECharts options. Migrating the other direction (recharts) would require rewriting the 6 advanced widget types. Plotly is unused — nothing to migrate.
- Tremor retained **only** for UI primitives (Card/Text/Select/Tabs); its chart components and all direct recharts imports removed. (Full tremor→radix/shadcn migration is a separate future spec.)
- ag-grid, reactflow, exceljs load per-use via `next/dynamic`/`await import()` so route chunks don't carry them.
- `@next/bundle-analyzer` wired in; a size-budget script exists for CI (spec 22 wires it into the pipeline).

## Implementation Plan

### Phase 1 — dead weight + lazy loading (no visual change, ship first)

1. Remove dead deps and declare the phantom:
   ```bash
   cd frontend
   npm uninstall plotly.js react-plotly.js @mui/material @mui/x-data-grid @emotion/react @emotion/styled react-quill echarts-countries-js
   npm install recharts@^2.13.3   # matches tremor's pin; temporary, removed in Phase 3
   ```
2. Lazy-load ag-grid: in `frontend/src/components/widgets/TableCard.tsx` replace line 11 with
   ```ts
   const AgTable = dynamic(() => import('@/components/widgets/AgTable'), { ssr: false })
   ```
   (`dynamic` from `next/dynamic`; TableCard is already `"use client"`). `AgTable` default-exports a component and its only render site is `TableCard.tsx:1115` — no other callers (verified by grep).
3. Lazy-load reactflow: in `frontend/src/app/(app)/datasources/[id]/page.tsx` replace line 6 with `const SchemaGraph = dynamic(() => import('@/components/datasources/SchemaGraph'), { ssr: false })`. Keep the `import 'reactflow/dist/style.css'` inside `SchemaGraph.tsx` (check it's there; if the css import lives in the page, move it into SchemaGraph.tsx).
4. Lazy-load exceljs in `frontend/src/components/datasources/ExecuteSqlDialog.tsx`: delete line 9 static import; inside the export handler use `const ExcelJS: any = await import('exceljs')` — copy the exact pattern from `PivotMatrixView.tsx:543`.
5. Add to `frontend/next.config.js`:
   ```js
   experimental: { optimizePackageImports: ['@tremor/react', '@remixicon/react', 'lucide-react'] },
   ```

### Phase 2 — measurement

6. `npm install -D @next/bundle-analyzer`, wrap `next.config.js`:
   ```js
   const withBundleAnalyzer = require('@next/bundle-analyzer')({ enabled: process.env.ANALYZE === 'true' })
   module.exports = withBundleAnalyzer(nextConfig)
   ```
7. Record baseline: `ANALYZE=true npm run build`, save the three HTML reports (`.next/analyze/`) and note First Load JS for `/` and `/v/[id]` from build output.
8. Add `frontend/scripts/check-bundle-size.mjs` — reads `.next/app-build-manifest.json` + stats each referenced chunk file, sums per-route First Load JS, fails (exit 1) if `/` or `/v/[id]` exceeds budget. Budget: set to Phase-1 measured value + 5% headroom, as consts at top of script. Add `"size": "node scripts/check-bundle-size.mjs"` to package.json scripts. CI wiring → spec 22.

### Phase 3 — chart consolidation onto ECharts (after spec 13 lands)

Spec 13 decomposes ChartCard into per-chart-type renderer modules; do this migration against those modules, not the 7,999-line monolith.

9. Build ECharts equivalents in `frontend/src/components/widgets/echarts/` following the existing module pattern (`PiePresets.tsx` exports `renderDonut`/`renderPie`/... functions receiving data + options):
   - `BasicCharts.tsx`: `renderLine`, `renderBar`, `renderArea` — replace Tremor `<LineChart>` (:7192), `<BarChart>` (:6812), `<AreaChart>` (:6917) render sites. Map existing props: `categories`→series, `colors` via `tremorNameToHex`/`getPresetPalette` from `frontend/src/lib/chartUtils.ts` (already imported in ChartCard:18), custom tooltips via the existing `TooltipTable` + `ReactDOMServer.renderToString` pattern ChartCard already uses for ECharts tooltips.
   - `Bars.tsx`: `renderBarList`, `renderCategoryBar`, `renderProgressBar` — replace `<TremorBarList>` (:4812), `<TremorCategoryBar>` (:5410), `<TremorProgressBar>` (:4904, :5516). CategoryBar/ProgressBar are simple enough that plain divs + Tailwind may beat an ECharts instance — prefer divs (they are stacked/percentage bars, no axes).
   - Donut: `renderEchartsDonut` already exists in `PiePresets.tsx` — route the Tremor `TremorDonutChart` code path (ChartCard:43-46 and KpiCard:24-26) to it; delete the Tremor dynamic imports.
10. KpiCard sparkline: replace recharts `ResponsiveContainer/AreaChart/Area/XAxis/YAxis` (KpiCard:15-21) with a minimal ECharts area option (grid all-zero margins, both axes `show:false`). Same for the ChartCard recharts sparkline usage (:30-37). Replace tremor `Metric` (KpiCard:3) with a styled `<span>` — it's a text component.
11. Delete all `recharts` and Tremor-chart dynamic imports from ChartCard/KpiCard; `npm uninstall recharts`. Tremor stays (UI primitives) so recharts remains in node_modules transitively but is no longer in any app chunk — verify via analyzer that no route chunk contains recharts.
12. Backward compat: widget configs in SQLite reference chart *type* strings, not libraries — renderer swap requires no config migration. Do a visual pass over each migrated chart type in the builder (`/`) and viewer (`/v/[id]`) with an existing dashboard; palettes must round-trip through `chartUtils.ts` helpers so saved color tokens keep resolving.

## Files to Modify

- `frontend/package.json` — remove 8 dead deps; add `recharts` (temp) then remove in Phase 3; add `@next/bundle-analyzer` devDep; add `size` script.
- `frontend/next.config.js` — analyzer wrapper + `optimizePackageImports`.
- `frontend/src/components/widgets/TableCard.tsx` — dynamic-import AgTable (line 11).
- `frontend/src/app/(app)/datasources/[id]/page.tsx` — dynamic-import SchemaGraph (line 6).
- `frontend/src/components/datasources/ExecuteSqlDialog.tsx` — lazy exceljs (line 9).
- `frontend/scripts/check-bundle-size.mjs` — new, size budget check.
- `frontend/src/components/widgets/echarts/BasicCharts.tsx` — new (Phase 3).
- `frontend/src/components/widgets/echarts/Bars.tsx` — new or divs inline (Phase 3).
- `frontend/src/components/widgets/ChartCard.tsx` (or its spec-13 decomposed renderers) — remove recharts + Tremor chart imports/render sites.
- `frontend/src/components/widgets/KpiCard.tsx` — remove recharts + Tremor `Metric`/`DonutChart`.

## Acceptance Criteria

- [ ] `plotly.js`, `react-plotly.js`, `@mui/*`, `@emotion/*`, `react-quill`, `echarts-countries-js` absent from `package.json` and `package-lock.json`; `npm run build` succeeds.
- [ ] `grep -rn "from 'recharts'" frontend/src` returns nothing after Phase 3; until then `recharts` is a declared dependency.
- [ ] ag-grid, reactflow, exceljs each live in their own async chunk — `ANALYZE=true npm run build` shows none of them inside the `/` or `/v/[id]` First Load JS.
- [ ] All chart widget types (line, bar, area, donut, pie, sunburst, nightingale, sankey, heatmap, gantt, barlist, categorybar, progress, KPI sparkline) render via ECharts or plain DOM; zero Tremor chart components imported.
- [ ] First Load JS for `/` and `/v/[id]` reduced vs recorded baseline; `npm run size` passes with the committed budget.
- [ ] Existing saved dashboards render without config migration (palette tokens resolve through `chartUtils.ts`).

## Verification

```bash
cd frontend
# dead deps really dead
for d in plotly react-plotly @mui @emotion react-quill echarts-countries; do grep -rn "$d" src && echo "FAIL $d" ; done
npm run build                      # must pass (note: --no-lint, ignoreBuildErrors:true — build passing is weak; also run:)
npx tsc --noEmit                   # catches what the build config suppresses
ANALYZE=true npm run build         # open .next/analyze/client.html; confirm no ag-grid/reactflow/exceljs/recharts in route entry chunks
npm run size                       # budget check
```
Manual: open builder `/`, add one widget of each chart type, save; open `/v/[id]` for an existing dashboard with table + charts — table (ag-grid) loads on demand, charts match pre-migration visuals; `datasources/[id]` schema tab renders the graph; ExecuteSqlDialog Excel export downloads a valid .xlsx.

## Out of Scope

- Replacing Tremor UI primitives (Card/Text/Select/Tabs across ~20 files) — future spec; Tremor stays as a UI kit.
- CI pipeline wiring for the size budget (spec 22, P2-ops) — this spec only provides the runnable script.
- Selective ECharts module registration (`echarts/core` + per-chart imports via `echarts-for-react/lib/core`) — worthwhile follow-up once consolidation lands; noted, not required.
- Re-enabling lint/type checks in the build (`--no-lint`, `ignoreBuildErrors`) — belongs to the CI spec.
- react-grid-layout, quill, @rjsf, json-edit-react — used and either small or already lazy.
