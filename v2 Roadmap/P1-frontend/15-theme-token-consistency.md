---
id: 15-theme-token-consistency
title: Theme token consistency and restore alpha themes
priority: P1
effort: L
depends_on: []
area: frontend
---

## Problem

1. ~358 hardcoded hex colors across `frontend/src/components/**` bypass the HSL CSS-var token system, so widgets (ChartCard, HeatmapCard, DataExplorer, Configurator, etc.) keep light-mode blues/grays under dark mode and under any theme variant.
2. The alpha theme foundation (three opt-in visual variants: `nova-estate` light, `core-interface` + `platform-architecture` dark) was fully built in commits `ce7e20cc` + `37800f53` on this branch, then **accidentally reverted** by the release commit `8022d92e` ("chore: release 4.5 - include pending changes"). At HEAD: `frontend/src/app/themes/` does not exist, `ThemeProvider.tsx` has no alpha support, `postcss.config.js` lost `postcss-import`, and the Environment-page picker section is gone.

## Current State

All verified at HEAD of `feature/alpha-themes-foundation`.

**Token system:**
- `frontend/src/app/globals.css:19-72` — light tokens on `:root` (`--background`, `--foreground`, `--primary: 197 80% 54%`, `--success/--warning/--danger` at :48-50, `--chart-1..--chart-5` at :52-56, surfaces, topbar).
- `globals.css:75-119` — `.dark` block (chart tokens at :107-111); `globals.css:122-161` — `.dark[data-variant="blackish"]` (chart tokens at :149-153). Tokens are HSL triples consumed as `hsl(var(--x))`.
- `frontend/tailwind.config.ts:31-60` — maps tokens to Tailwind classes incl. `chart.1..5` (:54-60). `:12-27` safelists Tremor `fill-*/stroke-*` classes.
- `frontend/src/components/providers/ThemeProvider.tsx` (133 lines) — only `DarkVariant = 'bluish' | 'blackish'` (:7), sets `data-variant` on `<html>` (:66), applies `localStorage.theme_overrides_{light,dark}` (:69-75), dispatches `window` event `'themechange'` (:81). No `LightVariant`, no alpha gate.
- `frontend/postcss.config.js` — only `tailwindcss` + `autoprefixer`; no `postcss-import`.
- `postcss-import` exists in `frontend/node_modules/` (transitive dep) but is NOT in `frontend/package.json`.

**Chart color pipeline:**
- `frontend/src/lib/chartUtils.ts:21-33` — 11-name palette (`blue, amber, emerald, violet, rose, cyan, lime, fuchsia, pink, indigo, gray`); `:92-107` `tremorNameToHex()` hardcodes hex per name; `:112-152` has `hexToRgb`/`rgbToHsl`/`hslToHex` helpers; `:155-161` `saturateHexBy()` does hex math. Consumers: `chartUtils.ts`, `widgets/ChartCard.tsx`, `widgets/KpiCard.tsx`.
- The `--chart-1..5` CSS tokens are consumed almost nowhere (only `builder/ReportBuilderModal.tsx` + tailwind config) — actual chart rendering uses the hardcoded hex table above and Tremor safelist classes. Tokens and reality have diverged.

**Hardcoded hex hot spots (grep-verified counts per file):**
- `builder/ConfiguratorPanel.tsx` (41) — `<input type="color">` defaults (`:2547`, `:2562`, `:2468`) and chart-type preview SVGs with `fill="#3b82f6"` etc. (`:2870-2900`).
- `widgets/HeatmapCard.tsx` (36) — `:134` `const fg = dark ? 'hsl(var(--foreground))' : '#0f172a'`; `:194-205` ordinal color ramp table (light/mid/dark per hue — deliberate data-viz scale); `:215-216` neutral ramp endpoints `#f3f4f6`, `#e5e7eb`, `#94a3b8`.
- `builder/DataExplorerDialogV2.tsx` (31) — Tailwind arbitrary-value classes for selection state, e.g. `:136` `'border-[#1E40AF] bg-[#1E40AF]/6'`, `:144` `text-[#1E40AF]`, `:143` `text-[#3B82F6]/70`.
- `alerts/EmailConfigDialog.tsx` (27) — hex inside **generated email HTML templates** (`:60-69`, `:101-103`) — must STAY hex (email clients have no CSS vars).
- `widgets/ChartCard.tsx` (25) — delta colors repeated 6×: `:3166-3167`, `:4079-4080`, `:4397`, `:5176-5177`, `:6438` pattern `(changePct < 0) ? '#22c55e' : (changePct > 10 ? '#ef4444' : '#9CA3AF')`; ECharts `backgroundColor: '#fff'` (`:446`, `:455`); fallback `'#94a3b8'` (`:537`, `:3211`, `:5251`); contrast constants `#FFFFFF`/`#000000` (`:3376-3378`).
- Remainder: `ConfiguratorPanelV2/VisualizeTab.tsx` (14), `ReportBuilderModal.tsx` (13, partly email/report HTML), `alerts/AlertsDialog.tsx` (8), `widgets/GanttCard.tsx` (5), `alerts/AlertDialog.tsx` (4), `echarts/PiePresets.tsx` (3), `TableCellsEditorModal.tsx` (3), `HexProgressBar.tsx`, `echarts/SankeyChart.tsx`, `shell/Navbar.tsx`, `GeneralTab.tsx` (2 each), and 8 files with 1 each (`TextCard`, `TableCard`, `SpacerCard`, `ReportCard`, `KpiCard`, `echarts/HeatmapPresets`, `CompositionCard`, `RemoteAttachmentBuilder`).

**Alpha themes (in git history, gone at HEAD):**
- `git show 37800f53:...` contains the final working state: `frontend/src/app/themes/{README.md,nova-estate.css,core-interface.css,platform-architecture.css}` (complete token shells covering every token incl. `--chart-1..5`, scoped under `.dark[data-variant="core-interface"]`, `.dark[data-variant="platform-architecture"]`, `:root[data-light-variant="nova-estate"]:not(.dark)`), extended `ThemeProvider.tsx` (LightVariant, `alpha_themes`/`light_variant` localStorage keys, master gate, `wipeInlineVarOverrides`), `postcss.config.js` with `postcss-import`, `globals.css` with `@import './themes/*.css'` placed ABOVE `@tailwind` (the `37800f53` fix — postcss-import silently drops late `@import`s), and an "Alpha themes" picker section in `frontend/src/app/(app)/admin/environment/AdminEnvironmentClient.tsx:728+`.
- Divergence check (verified): `git diff ce7e20cc^ HEAD -- ThemeProvider.tsx postcss.config.js` is EMPTY, and `git diff 37800f53 HEAD -- AdminEnvironmentClient.tsx globals.css` shows ONLY the alpha deletions (217 deletions, 0 insertions). A wholesale `git checkout 37800f53 --` of these files loses nothing.

## Desired State

- Alpha themes restored and selectable: Environment page toggle + variant cards; `<html data-variant>`/`data-light-variant` drives fully-scoped CSS variant files; disabled toggle = production look, byte-for-byte.
- One source of truth for chart colors: `--chart-1..--chart-11` CSS tokens defined in every theme block; `chartUtils.ts` resolves them at runtime (hex fallback for SSR); charts re-render on `themechange`.
- UI-chrome hex in components replaced by token classes/vars. Legitimate hex stays (email HTML, `<input type="color">` values, heatmap ordinal ramps, contrast-math constants) — each marked with a `// theme-exempt:` comment so future greps can filter.

## Implementation Plan

### Phase A — Restore alpha themes (pure git restore, ~30 min)

1. `git checkout 37800f53 -- frontend/postcss.config.js frontend/src/app/themes frontend/src/components/providers/ThemeProvider.tsx frontend/src/app/globals.css "frontend/src/app/(app)/admin/environment/AdminEnvironmentClient.tsx"`
   (Safe per divergence check above. Keep the `@import`s above `@tailwind` exactly as in 37800f53.)
2. Add `"postcss-import": "^16"` to `devDependencies` in `frontend/package.json` and `npm install` — today it only works because it's a transitive Tailwind dep.
3. `npm run build` in `frontend/`; confirm compiled CSS contains all three variant selectors:
   `grep -rl 'data-light-variant="nova-estate"' frontend/.next/static/css/` (and the two `data-variant` dark selectors).
4. Manual smoke: Environment page → enable "Alpha themes" → pick each variant → palette flips; disable toggle → production palette returns; `localStorage` keys `alpha_themes`, `light_variant`, `dark_variant` persist across reload.

### Phase B — Chart token plumbing (`globals.css`, `chartUtils.ts`, `ThemeProvider.tsx`)

5. In `globals.css`, redefine `--chart-1..5` and add `--chart-6..11` in `:root`, `.dark`, and `.dark[data-variant="blackish"]` to the HSL equivalents of the **actual rendering palette** from `chartUtils.ts:21-33` order (so nothing visually changes):
   `1:#3b82f6→217 91% 60%, 2:#f59e0b→38 92% 50%, 3:#10b981→160 84% 39%, 4:#8b5cf6→258 90% 66%, 5:#f43f5e→350 89% 60%, 6:#06b6d4→189 94% 43%, 7:#84cc16→84 81% 44%, 8:#d946ef→292 84% 61%, 9:#ec4899→330 81% 60%, 10:#6366f1→239 84% 67%, 11:#6b7280→220 9% 46%`.
   For `.dark` blocks, bump lightness +6-8% on 1/3/10 for contrast (match existing dark chart-token spirit at `globals.css:107-111`). Add `chart: {6..11}` entries to `tailwind.config.ts:54-60`.
6. Extend each restored alpha theme CSS file with its own `--chart-6..11` (derive from each theme's accent family; document in `themes/README.md` token contract, which currently lists only `--chart-1..5`).
7. In `chartUtils.ts` add (reusing existing `hslToHex` at `:135`):
   ```ts
   const chartTokenIndex: Record<AvailableChartColorsKeys, number> = { blue:1, amber:2, emerald:3, violet:4, rose:5, cyan:6, lime:7, fuchsia:8, pink:9, indigo:10, gray:11 }
   // Reads '--chart-N' ("H S% L%") off documentElement; falls back to the legacy hex table (SSR / var missing).
   export function chartNameToHex(name: AvailableChartColorsKeys): string { ... parse triple, hslToHex(h, s/100, l/100), else tremorNameToHex(name) }
   export function cssVarHsl(varName: string, fallback: string): string { ... returns `hsl(${triple})` or fallback }
   ```
   Rename-migrate: keep `tremorNameToHex` as the private fallback table; point all callers (`ChartCard.tsx`, `KpiCard.tsx`, internal uses in `chartUtils.ts`) at `chartNameToHex`. `saturateHexBy` (`:155`) keeps working — it now receives theme-resolved hex.
8. Re-render on theme change: add to `ThemeProvider.tsx` a tiny exported hook:
   ```ts
   export function useThemeTick(): number  // useState counter incremented by a window 'themechange' listener
   ```
   (the event already fires: `ThemeProvider.tsx:81` and on variant change). Call it in `ChartCard.tsx`, `KpiCard.tsx`, `HeatmapCard.tsx`, `echarts/*.tsx` and include the tick in the deps of the memo/effect that builds chart options, so switching theme/variant recolors live charts.
9. Tremor safelist classes (`fill-blue-500` etc., `tailwind.config.ts:12-27`) cannot read CSS vars — leave as-is. Alpha themes may override them per the component-override section of `themes/README.md`. `// ponytail: class-based Tremor colors stay static; upgrade path = fork Tremor color prop to style-based colors if a theme ships that needs it.`

### Phase C — Mechanical hex migration (grep-driven)

Audit command (run before/after; the ONLY allowed remaining matches are lines tagged `theme-exempt`):
```bash
grep -rnE '#[0-9a-fA-F]{6}\b|#[0-9a-fA-F]{3}\b' frontend/src/components --include='*.tsx' --include='*.ts' | grep -v 'theme-exempt'
```

10. **Conversion table** (apply per match; JSX classes → Tailwind token classes, JS style values → `hsl(var(--x))` or `cssVarHsl()`):

| Hardcoded | Replace with |
|---|---|
| `#1E40AF` (sel. state, 35×) | class: `border-primary text-primary bg-primary/10`; JS: `hsl(var(--primary))` |
| `#3b82f6 #60a5fa #2563eb #93c5fd` | `--primary` / `--chart-1` (series context) |
| `#ffffff #fff #FFFFFF` (UI bg) | `--card` or `--background` |
| `#111827 #0f172a #1f2937` | `--foreground` |
| `#6b7280 #9ca3af #9CA3AF #94a3b8 #475569 #334155` | `--muted-foreground` |
| `#e5e7eb #f3f4f6 #f9fafb #f8fafc #fafafa` | `--border` (strokes) / `--muted` (fills) |
| `#ef4444 #f43f5e` | `--danger` |
| `#22c55e #10b981 #10B981 #34d399 #86efac` | `--success` |
| `#f59e0b #F59E0B #fde68a` | `--warning` |
| other one-offs | nearest semantic token; if genuinely a categorical series color → `--chart-N` |

11. **ChartCard.tsx** — extract ONE helper (root cause for the 6 duplicated ternaries at `:3166`, `:4079`, `:4397`, `:5176`, `:6438`):
    ```ts
    function deltaColor(changePct: number, invert: boolean): string {
      const good = 'hsl(var(--success))', bad = 'hsl(var(--danger))', flat = 'hsl(var(--muted-foreground))'
      ...
    }
    ```
    Also: `:446`/`:455` `backgroundColor: '#fff'` → `'transparent'` (ECharts over a token-colored card); `'#94a3b8'` fallbacks → `cssVarHsl('--muted-foreground', '#94a3b8')`. Keep `:3376-3378` `#FFFFFF`/`#000000` — contrast-math constants, tag `// theme-exempt: contrast calc`.
12. **HeatmapCard.tsx** — `:134` drop the ternary: `const fg = 'hsl(var(--foreground))'`. `:215-216` neutral endpoints → resolve via `cssVarHsl('--muted', ...)`/`cssVarHsl('--border', ...)`. Keep the `:194-205` hue ramp table (ordinal data scale), tag `// theme-exempt: ordinal color ramp`.
13. **DataExplorerDialogV2.tsx** — replace all `[#1E40AF]`/`[#3B82F6]` arbitrary classes per table row 1. Same pattern in `AlertsDialog.tsx`, `AlertDialog.tsx`, `RemoteAttachmentBuilder.tsx`, `ReportBuilderModal.tsx` (UI parts only).
14. **ConfiguratorPanel.tsx / VisualizeTab.tsx / GeneralTab.tsx** — preview SVGs: `fill="#3b82f6"` → `fill="hsl(var(--chart-1))"` etc. (inline SVG resolves CSS vars). `<input type="color">` `value=` defaults MUST stay hex (browser requirement) — tag `// theme-exempt: color input`.
15. **Email/report HTML templates** (`EmailConfigDialog.tsx:60-69,101+`, email-HTML sections of `ReportBuilderModal.tsx`, `AlertsDialog.tsx`) — no change; tag the template blocks once: `// theme-exempt: email HTML`.
16. Sweep the remaining 1-2-hex files from the Current State list with the conversion table.

## Files to Modify

- `frontend/postcss.config.js` — restore `postcss-import` plugin (git checkout 37800f53)
- `frontend/package.json` — add `postcss-import` devDependency
- `frontend/src/app/globals.css` — restore `@import './themes/*'` block; redefine/extend `--chart-1..11` in `:root`, `.dark`, `.dark[data-variant="blackish"]`
- `frontend/src/app/themes/{README.md,nova-estate.css,core-interface.css,platform-architecture.css}` — restore from 37800f53; add `--chart-6..11` per theme; update README token contract
- `frontend/src/components/providers/ThemeProvider.tsx` — restore alpha version from 37800f53; add `useThemeTick()` hook
- `frontend/src/app/(app)/admin/environment/AdminEnvironmentClient.tsx` — restore alpha picker section (37800f53)
- `frontend/tailwind.config.ts` — `chart.6..11` color entries
- `frontend/src/lib/chartUtils.ts` — `chartNameToHex`, `cssVarHsl`, token index; demote `tremorNameToHex` to fallback
- `frontend/src/components/widgets/ChartCard.tsx` — `deltaColor()` helper, token migration, `useThemeTick`
- `frontend/src/components/widgets/{KpiCard,HeatmapCard,GanttCard,TextCard,TableCard,SpacerCard,ReportCard,CompositionCard,HexProgressBar}.tsx` — token migration
- `frontend/src/components/widgets/echarts/{PiePresets,SankeyChart,HeatmapPresets,AreaAdvanced}.tsx` — token migration + `useThemeTick`
- `frontend/src/components/builder/{ConfiguratorPanel,DataExplorerDialogV2,ReportBuilderModal,TableCellsEditorModal,RemoteAttachmentBuilder}.tsx`, `builder/ConfiguratorPanelV2/{VisualizeTab,GeneralTab}.tsx` — token migration
- `frontend/src/components/alerts/{AlertsDialog,AlertDialog,EmailConfigDialog}.tsx` — UI-part migration + `theme-exempt` tags on email HTML
- `frontend/src/components/shell/Navbar.tsx` — token migration (2 hexes)

## Acceptance Criteria

- [ ] `frontend/src/app/themes/` exists with README + 3 variant CSS files; all rules scoped under their attribute selectors (no bare `:root`/`.dark` rules in variant files)
- [ ] Environment page shows "Alpha themes" toggle + variant cards; picking each of the 3 variants visibly re-skins the app; toggle off restores production look
- [ ] `localStorage`: `alpha_themes`, `light_variant`, `dark_variant` persist and survive reload; alpha off + saved alpha variant falls back to `bluish`/`default` without erasing the saved choice
- [ ] `--chart-1..11` defined in `:root`, `.dark`, `.dark[data-variant="blackish"]`, and all 3 alpha files; default-theme chart colors are pixel-identical to before (token values = old hex converted)
- [ ] Charts (ECharts + inline-style paths) recolor live when switching theme/dark-variant, no page reload
- [ ] Audit grep (Phase C header) returns ONLY `theme-exempt`-tagged lines in `frontend/src/components/**`
- [ ] No hex in email-HTML templates was changed
- [ ] `npm run build` passes; `tsc` clean (`npx tsc --noEmit`)

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/frontend

# 1. Build + compiled CSS contains variant selectors
npm run build
grep -rl 'data-light-variant="nova-estate"' .next/static/css/ && \
grep -rl 'data-variant="core-interface"' .next/static/css/ && \
grep -rl 'data-variant="platform-architecture"' .next/static/css/

# 2. Hex audit — expect zero untagged matches
grep -rnE '#[0-9a-fA-F]{6}\b|#[0-9a-fA-F]{3}\b' src/components --include='*.tsx' --include='*.ts' | grep -v 'theme-exempt' | wc -l   # → 0

# 3. Chart tokens complete everywhere
for f in src/app/globals.css src/app/themes/*.css; do echo "$f: $(grep -c -- '--chart-11' $f)"; done  # ≥1 per theme block

# 4. Type check
npx tsc --noEmit
```
Manual: `npm run dev` → :3000 → Admin > Environment → toggle Alpha themes, cycle nova-estate / core-interface / platform-architecture; open a dashboard with a bar chart, pie, heatmap, KPI with delta — verify series colors + delta green/red follow the active theme; switch light/dark via ThemeToggle — charts recolor without reload; disable alpha → production palette returns exactly.

## Out of Scope

- Component-level alpha treatments (glass surfaces, gradient borders, 2px radius clamp, WebGL backgrounds) — the "worker" passes described in `themes/README.md`; this spec ships palette-complete variants only
- Migrating Tremor class-based series colors (`fill-blue-500` safelist) to tokens — static, documented ceiling in Phase B step 9
- Hex in `frontend/src/styles/{ag-theme-overrides,pivottable-overrides}.css` and anything outside `src/components/**`
- Redesigning generated email/report HTML colors; backend-rendered exports
- The `theme_overrides_*` custom-palette editor UX (unchanged; alpha variants clear it per restored ThemeProvider logic)
