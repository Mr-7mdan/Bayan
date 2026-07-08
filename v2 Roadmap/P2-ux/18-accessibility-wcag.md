---
id: 18-accessibility-wcag
title: Accessibility (WCAG 2.1 AA) baseline
priority: P2
effort: L
depends_on: []
area: frontend
---

## Problem

The frontend is effectively unusable by keyboard and screen readers. Modals are hand-rolled portals with no focus trap, no focus restore, no `role="dialog"`, and mostly no Escape handling. The custom `Select` renders non-focusable `div` options with no arrow-key navigation. Widget menus declare `role="menu"` but contain plain buttons with no roving focus. The dashboard grid is mouse-only drag/drop. ECharts canvases have no accessible name or data fallback. Global CSS actively suppresses focus outlines. For a BI platform sold into corporate environments, WCAG 2.1 AA is a procurement checkbox; today the app fails it broadly.

## Current State

All paths relative to `frontend/` unless noted. All refs verified 2026-07-07.

**Modals — two coexisting patterns.**
- Radix Dialog (accessible: focus trap, Escape, aria built in) already used in ~10 files: `src/app/page.tsx:11`, `src/components/dashboards/CreateDashboardDialog.tsx:56`, `src/app/(app)/admin/users/page.tsx:6`, `src/app/login/page.tsx:7`, etc. `@radix-ui/react-dialog ^1.0.5` is in `package.json`.
- Hand-rolled `createPortal` modals with none of that. Canonical example `src/components/alerts/AlertDialog.tsx:1523-1529` (2,502 lines, 5 aria attrs total, no Escape handler, backdrop click only):
  ```tsx
  return createPortal(
    <div className="fixed inset-0 z-[1200]">
      <div className="absolute inset-0 bg-black/40" onClick={() => onCloseAction()} />
      <div className="absolute left-1/2 top-1/2 ...">
        ...
        <button ... onClick={onCloseAction}>✕</button>
  ```
  Siblings (grep `createPortal` under `src/components`): `alerts/AlertsDialog.tsx`, `ai/AiAssistDialog.tsx:486`, `builder/ReportBuilderModal.tsx:3646`, `builder/CompositionBuilderModal.tsx:49`, `builder/AdvancedSqlDialog.tsx`, `builder/ImportTableDialog.tsx`, `builder/DataExplorerDialog.tsx`, `builder/DataExplorerDialogV2.tsx`, `builder/TablePreviewDialog.tsx`, `builder/TableCellsEditorModal.tsx:534` (has `role="dialog" aria-modal` but still no focus trap/restore), `datasources/ExecuteSqlDialog.tsx`, `shared/FilterbarControl.tsx`. Some handle Escape (`TableCellsEditorModal`, `ReportBuilderModal`, `AiAssistDialog`), most do not (`AlertDialog`, `CompositionBuilderModal`, `AlertsDialog`, `ImportTableDialog`).

**Custom Select** — `src/components/Select.tsx`: trigger button (`:74-88`) has no `aria-haspopup`/`aria-expanded`; `SelectContent` (`:130-141`) is a portal `div role="listbox"`; `SelectItem` (`:159-168`) is a click-only `div role="option"` — not focusable, no arrow keys, no typeahead. Escape close exists (`:118`). `@radix-ui/react-select ^2.0.0` is already installed but unused here; the exported API (`Select/SelectTrigger/SelectValue/SelectContent/SelectItem`) mirrors Radix.

**Widget menus** — `src/components/widgets/WidgetKebabMenu.tsx:90-96`: portal `div role="menu"` positioned off an anchor; items are plain `<button>` (no `role="menuitem"`, focus never moves into the menu, no arrow keys). Escape works (`:40`). Same pattern in `src/components/widgets/WidgetActionsMenu.tsx`.

**Dashboard grid** — `src/app/page.tsx:1423-1433` `<GridLayout ... isDraggable draggableHandle=".widget-title">` (react-grid-layout ^1.3.4); viewer `src/app/v/[id]/page.tsx:323`. Drag/resize/reorder is mouse-only; react-grid-layout has no built-in keyboard support.

**Charts** — `src/components/widgets/ChartCard.tsx` (7,999 lines, zero `aria-label`): three `<ReactECharts>` mounts at `:4741`, `:5357`, `:6187` with option objects built at `:4623`, `:5023`, `:6040`. No ECharts `aria` option, no accessible name on the canvas container, no data-table fallback.

**Focus visibility** — `src/app/globals.css:675` `.btn:focus { box-shadow: none; outline: none; }` kills focus indication on every `.btn`; 35 tsx files use Tailwind `outline-none`/`focus:ring-0` (e.g. `SelectTrigger` at `Select.tsx:83`). A good reusable utility already exists and is barely used: `.bayan-focus-ring:focus-visible` at `globals.css:438-442` (2px `hsl(var(--primary))` outline, offset 2).

**Layout/landmarks** — `src/app/(app)/layout.tsx:135` already has `<main>`; no skip link; toast notifications (`:141-149`) have no `aria-live`; `Navbar.tsx` has 3 `aria-label`s, `Sidebar.tsx` has 1. `Switch.tsx:29` is correct (`role="switch" aria-checked` on a real button).

**Theme tokens** — HSL token pairs live at `globals.css:20-60` (`--background`, `--foreground`, `--muted-foreground`, `--primary`, `--card`, three theme variants incl. bluish/blackish dark, see `Navbar.tsx:125,133`). No contrast audit has ever run. (Related to planned theme-tokens spec 15 — audit the same token set; no hard dependency.)

**Tooling** — `.eslintrc.json` extends `next/core-web-vitals` (ships only the small jsx-a11y subset). `playwright ^1.55.1` is a devDependency but there is no `playwright.config.*` or test directory. `npm run build` uses `--no-lint`, so lint is advisory-only.

## Desired State

- Every modal traps focus, restores focus to its opener on close, closes on Escape, and exposes `role="dialog" aria-modal="true"` with an accessible name — via one shared shell built on the already-installed Radix Dialog.
- `Select`, widget menus, tabs, and switches are fully keyboard-operable with correct ARIA patterns.
- Dashboard widgets are focusable; builder-mode move/resize has a keyboard alternative.
- Charts have accessible names, ECharts aria descriptions enabled, and a "View as table" fallback.
- Focus is always visible; the global suppressions are removed.
- Theme token pairs pass 4.5:1 (text) / 3:1 (UI components) in all three themes.
- `eslint-plugin-jsx-a11y` (recommended) runs in lint; an axe-core Playwright scan of key routes passes with no serious/critical violations and runs in CI.

## Implementation Plan

Ordered. Steps 1-3 are the highest-leverage 20%; do them first.

### 1. Shared modal shell (fixes ~14 dialogs at their root)
1. Create `src/components/ui/ModalShell.tsx` wrapping `@radix-ui/react-dialog` (already installed; copy the working pattern from `src/components/dashboards/CreateDashboardDialog.tsx:56`). Props: `open`, `onCloseAction`, `title` (feeds `Dialog.Title` — visually the existing header row), `zIndex` (default 1200; callers use 200-2000 today, preserve each file's current value), `panelClassName` (callers keep their exact width/height classes), `children`. Render: `Dialog.Root open onOpenChange` → `Dialog.Portal` → `Dialog.Overlay` (`fixed inset-0 bg-black/40`) → `Dialog.Content` with the caller's panel classes. Radix supplies focus trap, focus restore, Escape, `role`/`aria-modal`, and `aria-labelledby` for free. Give the ✕ button `aria-label="Close"`.
2. Migrate hand-rolled modals to `ModalShell`, replacing only the outer portal shell (lines cited in Current State); inner content untouched. Priority order: `AlertDialog.tsx:1523`, `AiAssistDialog.tsx:486`, `CompositionBuilderModal.tsx:49`, `ReportBuilderModal.tsx:3646`, `TableCellsEditorModal.tsx:534`, `AlertsDialog.tsx`, `AdvancedSqlDialog.tsx`, `ImportTableDialog.tsx`, `DataExplorerDialogV2.tsx`, `DataExplorerDialog.tsx`, `TablePreviewDialog.tsx`, `ExecuteSqlDialog.tsx`. Note: `ReportBuilderModal` and `AlertDialog` open nested portals (dropdowns at `ReportBuilderModal.tsx:140,1731`) — set `modal` interaction so nested portals stay clickable (Radix `Dialog.Content` `onInteractOutside`: ignore events whose target is inside a `[data-modalshell-nested]` container; tag those dropdown portals).

### 2. Focus visibility (global, one file)
1. In `src/app/globals.css`: delete `outline: none` from `.btn:focus` (`:675`) and add `.btn:focus-visible { outline: 2px solid hsl(var(--primary)); outline-offset: 2px; }` (same recipe as `.bayan-focus-ring` at `:438`).
2. Add a global rule so Tailwind `outline-none` stragglers still show keyboard focus: `:where(button, a, input, select, textarea, [tabindex]):focus-visible { outline: 2px solid hsl(var(--primary)); outline-offset: 2px; }`. Do NOT chase all 35 files individually — the `:where()` (zero specificity) rule plus removing the `.btn` suppression covers them; only fix a file if axe still flags it.

### 3. Select rebuilt on Radix (fixes every dropdown in the app)
Reimplement `src/components/Select.tsx` internals on `@radix-ui/react-select` (installed, unused) keeping the exported names and prop signatures exactly (`Select({value, onValueChangeAction, children})`, `SelectTrigger`, `SelectValue({placeholder})`, `SelectContent`, `SelectItem({value, children, onClickAction})`) so zero call sites change. Map `onValueChangeAction` → Radix `onValueChange`; fire `SelectItem.onClickAction` inside `onValueChange` by value match. Keep current styling classes on `Trigger`/`Content`/`Item`. Delete the label pre-scan machinery (`:26-69`) — Radix `Select.Value` renders the selected item's children natively. Keep portal z-index 1100.

### 4. Widget menus
Add devDependency `@radix-ui/react-dropdown-menu` (sibling of 6 Radix packages already in tree; hand-rolling roving tabindex in two menus is more code). Rebuild `WidgetKebabMenu.tsx` and `WidgetActionsMenu.tsx` on `DropdownMenu.Root/Trigger(asChild on the existing kebab button)/Portal/Content/Item` — arrow keys, Home/End, typeahead, Escape, focus return all free. Preserve: the `anchorEl` positioning becomes `DropdownMenu.Content` `align="start"`; keep the `body.dataset.actionsMenuOpen` gate effect (`WidgetKebabMenu.tsx:54-78`) verbatim, driven by Radix `onOpenChange`. Kebab trigger buttons (in `ChartCard.tsx` / widget headers) get `aria-label="Widget actions"` + `aria-haspopup="menu"`.

### 5. Dashboard grid keyboard operability (builder page only)
In `src/app/page.tsx` widget wrapper (each GridLayout child at `:1423-1630`):
- `tabIndex={0}`, `role="group"`, `aria-label={widget title}`, `.bayan-focus-ring` class.
- `onKeyDown` on the focused wrapper: Enter opens the widget's configurator (existing settings handler at `:1491` area); `Ctrl/Cmd+ArrowKeys` moves the widget one grid unit, `Ctrl/Cmd+Shift+ArrowKeys` resizes by one unit — implement by cloning `layout`, mutating that item's `x/y/w/h` (clamp to grid bounds/cols), and calling the existing `onLayoutChange` handler that GridLayout already uses. No library change.
- Viewer `src/app/v/[id]/page.tsx:323` grid is static — only add `tabIndex={0}` + `aria-label` to widget wrappers so content is reachable/announced.

### 6. Chart accessibility
In `ChartCard.tsx`:
- Add `aria: { enabled: true }` to the three option objects (`:4623`, `:5023`, `:6040`) — ECharts generates a natural-language series description on the canvas.
- On each `<ReactECharts>` wrapper div (`:4740`, `:5356`, `:6186`): `role="img"` and `aria-label` = widget title + chart type (title is available in component props/spec).
- Data-table fallback: add a `viewData` action to `WidgetKebabMenu` (union type at `WidgetKebabMenu.tsx:10`) that opens the existing `src/components/builder/TablePreviewDialog.tsx` fed with the widget's already-computed `displayData` rows. Wire in the same switch that handles `viewSpec`/`viewSql` in `ChartCard.tsx`/`page.tsx`.
- `KpiCard.tsx`, `Tracker.tsx`, `HexProgressBar.tsx`: ensure the rendered value + label are real text (they are) and decorative SVG parts get `aria-hidden="true"`.

### 7. Landmarks, skip link, live regions
- `src/app/(app)/layout.tsx`: add `<a href="#main" className="sr-only focus:not-sr-only fixed top-2 left-2 z-[9999] bayan-focus-ring ...">Skip to content</a>` as first child of the grid (`:131`), and `id="main"` on the existing `<main>` (`:135`).
- Notifications container (`:142`): add `role="status" aria-live="polite"`.
- `Sidebar.tsx`: wrap nav links in `<nav aria-label="Primary">`; `aria-current="page"` on the active item.

### 8. Contrast audit tied to theme tokens
For each theme block in `globals.css:20-60` (+ the two dark variants), check pairs `--foreground`/`--background`, `--muted-foreground`/`--background`, `--muted-foreground`/`--card`, `--primary-foreground`/`--primary`, `--secondary-foreground`/`--secondary` at 4.5:1, and `--border`/`--background` at 3:1. One-off node script in scratchpad using WCAG relative-luminance math (~30 lines, no new dependency) reading the HSL triples; adjust failing lightness values in place. Rendered-page failures are caught by step 9's axe scan regardless.

### 9. Tooling: lint rule + axe in CI
- `package.json` devDependencies: add `eslint-plugin-jsx-a11y` (already in node_modules transitively via eslint-config-next — pin it top-level) and `@axe-core/playwright`.
- `.eslintrc.json`: add `"plugin:jsx-a11y/recommended"` to `extends`. Downgrade noisy rules to `warn` initially (`jsx-a11y/no-static-element-interactions`, `jsx-a11y/click-events-have-key-events`) so lint stays passing; ratchet later.
- New `frontend/playwright.config.ts` (webServer: `npm run dev`, baseURL `http://localhost:3000`) and `frontend/e2e/a11y.spec.ts`: log in (test user via env), run `new AxeBuilder({ page }).analyze()` on `/login`, `/home`, `/dashboards/mine`, `/` (builder with a dashboard open), `/alerts`, and with `AlertDialog` open; assert zero `serious`/`critical` violations. Add script `"test:a11y": "playwright test e2e/a11y.spec.ts"`. Wire into whatever CI runs frontend checks (none exists today — add to the release script or run manually per Verification).

### Prioritized component checklist
| Priority | Component | Fix |
|---|---|---|
| 1 | All hand-rolled modals (14 files) | ModalShell (step 1) |
| 1 | `globals.css` focus suppression | step 2 |
| 1 | `Select.tsx` | Radix rebuild (step 3) |
| 2 | `WidgetKebabMenu`/`WidgetActionsMenu` | DropdownMenu (step 4) |
| 2 | Builder grid `page.tsx` | keyboard move/resize (step 5) |
| 2 | Layout skip link, live region, nav landmarks | step 7 |
| 3 | `ChartCard` + widget cards | aria + view-data (step 6) |
| 3 | Theme token contrast | step 8 |
| 3 | Lint + axe CI | step 9 |

## Files to Modify

- `frontend/src/components/ui/ModalShell.tsx` — new; Radix-Dialog-based shared modal shell
- `frontend/src/components/alerts/AlertDialog.tsx`, `alerts/AlertsDialog.tsx`, `ai/AiAssistDialog.tsx`, `builder/ReportBuilderModal.tsx`, `builder/CompositionBuilderModal.tsx`, `builder/TableCellsEditorModal.tsx`, `builder/AdvancedSqlDialog.tsx`, `builder/ImportTableDialog.tsx`, `builder/DataExplorerDialog.tsx`, `builder/DataExplorerDialogV2.tsx`, `builder/TablePreviewDialog.tsx`, `datasources/ExecuteSqlDialog.tsx` — swap portal shell for ModalShell
- `frontend/src/components/Select.tsx` — reimplement on @radix-ui/react-select, same exported API
- `frontend/src/components/widgets/WidgetKebabMenu.tsx`, `WidgetActionsMenu.tsx` — rebuild on @radix-ui/react-dropdown-menu; add `viewData` action
- `frontend/src/components/widgets/ChartCard.tsx` — ECharts `aria.enabled`, `role="img"` + label on wrappers, viewData wiring
- `frontend/src/app/page.tsx` — widget wrapper focus + keyboard move/resize
- `frontend/src/app/v/[id]/page.tsx` — focusable, labeled widget wrappers
- `frontend/src/app/(app)/layout.tsx` — skip link, `id="main"`, aria-live on toasts
- `frontend/src/components/shell/Sidebar.tsx` — nav landmark, aria-current
- `frontend/src/app/globals.css` — remove focus suppression, global `:focus-visible` rule, contrast token fixes
- `frontend/.eslintrc.json` — add `plugin:jsx-a11y/recommended`
- `frontend/package.json` — devDeps `eslint-plugin-jsx-a11y`, `@axe-core/playwright`, `@radix-ui/react-dropdown-menu`; `test:a11y` script
- `frontend/playwright.config.ts`, `frontend/e2e/a11y.spec.ts` — new; axe scans

## Acceptance Criteria

- [ ] Opening any migrated dialog moves focus inside; Tab cycles within it; Escape closes it; focus returns to the trigger (verify on AlertDialog, ReportBuilderModal, AiAssistDialog).
- [ ] Every migrated dialog exposes `role="dialog"`, `aria-modal="true"`, and an accessible name (axe confirms).
- [ ] `Select` is operable with keyboard only: open with Enter/Space/ArrowDown, navigate with arrows, select with Enter, close with Escape; trigger reports `aria-expanded`.
- [ ] Widget kebab menu: focus moves into menu on open, arrow keys navigate, items are `role="menuitem"`, Escape returns focus to the kebab button.
- [ ] In the builder, a focused widget moves with Ctrl+Arrows and resizes with Ctrl+Shift+Arrows; layout persists via the normal save path.
- [ ] Tabbing anywhere in the app always shows a visible focus indicator; `.btn:focus { outline:none }` is gone.
- [ ] Charts render with ECharts aria descriptions; chart containers have `role="img"` and a meaningful `aria-label`; "View as table" appears in the kebab for ECharts widgets and opens a data table.
- [ ] Skip link is first tab stop and jumps to `#main`; toasts are announced via `aria-live`.
- [ ] All token pairs listed in step 8 meet 4.5:1 (3:1 for borders) in all three themes.
- [ ] `npm run lint` passes with `plugin:jsx-a11y/recommended` enabled.
- [ ] `npm run test:a11y` reports zero serious/critical axe violations on the scanned routes.

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/frontend
npm install                      # picks up new devDeps
npm run lint                     # jsx-a11y active, passes
npx playwright install chromium
npm run test:a11y                # axe scan green
```
Manual keyboard pass (app on :3000, backend on :8000):
1. Log in; press Tab — first stop is "Skip to content"; Enter lands in `<main>`.
2. Dashboards → open a dashboard in builder (`/`). Tab to a widget; Ctrl+ArrowRight moves it; Ctrl+Shift+ArrowDown grows it; save; reload; layout persisted.
3. Tab to widget kebab → Enter → arrows through items → Escape → focus back on kebab.
4. Widget kebab → "View as table" → data table dialog opens, Escape closes.
5. Alerts page → Create Alert → AlertDialog: Tab is trapped inside, Escape closes, focus returns to the Create button.
6. Any `Select` (e.g. builder configurator): full keyboard operation per criteria.
7. VoiceOver spot-check (macOS, Cmd+F5): chart announces its label + ECharts summary; toasts are announced.

## Out of Scope

- AG Grid / MUI DataGrid internal accessibility (both ship their own ARIA; config-level gaps get their own ticket if axe flags them).
- Keyboard drag-drop for the builder DataNavigator / field-well drag interactions (pivot builder) — separate follow-up.
- WCAG AAA targets, high-contrast theme, reduced-motion audit.
- Rewriting `ReportBuilderModal`/`AlertDialog` internals beyond the shell swap.
- Public embed route (`/render/embed/widget`) — inherits ChartCard fixes automatically; no dedicated work.
- Migrating pages already on Radix Dialog.
