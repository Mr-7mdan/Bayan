# Bayan v3 — Frontend UX/UI Revamp Plan

Goal: take the frontend from "homegrown BI tool" to commercial-grade SaaS quality — fast-feeling,
professional, elegant, intuitive. Built from three principal-designer reviews of the dashboard
builder, widget configurator, report builder, and app shell (all findings cite `file:line`).

Branch: `bayan-v3` (off `feature/v2-security`, so it includes all v2 work: route states, i18n/RTL,
virtualization, a11y foundation, bundle cuts).

---

## Root causes identified (why it reads homegrown)

1. **No type scale.** ~370× `text-[11px]`, ~310× `text-[10px]`, 7–9px text in the report builder.
   `--text-1/2/3` tokens are *colors*, not sizes. No heading system.
2. **Elevation is globally destroyed.** `globals.css:242-244` kills every `shadow-*` with
   `!important` — modals, cards, menus are all flat-on-flat. Authors keep writing `shadow-2xl`
   that silently does nothing.
3. **4 parallel button systems, 3 input systems, 3 card surface styles.** No canonical primitives.
4. **No feedback loops.** Autosave is invisible (errors swallowed), no undo/redo in the builder,
   no empty states, spinner-or-blank instead of skeletons, ~10 pages re-implement their own toast.
5. **The floating auto-collapsing configurator fights the user**; V2 (tabbed, searchable, undo)
   already exists behind a default-off toggle and is the right vehicle.
6. **Perf-feel:** per-frame `setLayoutState` during drag re-renders every chart; ResizeObserver →
   setState reflow loops; `MutationObserver` on `document.body`.
7. **Dark mode bugs:** `--background` uses comma HSL (breaks `/opacity`), red `--header-accent`
   in a cyan brand, amber focus rings vs cyan brand focus.

## Design direction (from design-system research)

- Style: **data-dense dashboard** — space-efficient, maximum data visibility, no ornament.
- Existing cyan brand (`--primary`) stays; introduce a curated 3-level elevation scale,
  4pt spacing grid, and a 6-step type ramp. Motion: 150–250ms transforms/opacity only,
  `prefers-reduced-motion` respected (already in globals).
- Icons: Remix only (already true) — kill the emoji/glyph stragglers (`⋮ ✕ ✨ ∑ 🗓 Q`).

---

## Wave F — Foundation (sequential, everything depends on it)

### F1. Design tokens + globals.css surgery
- Type ramp: `--font-2xs..2xl` (11/12/13/14/16/20/24) + `.text-token` utilities; heading styles.
- Elevation: DELETE the global shadow-kill rule (`globals.css:242-244`); add curated
  `--shadow-1/2/3` (card / popover / modal) tuned per light+dark; map `shadow-card` etc.
- Spacing/radius: document 4pt grid; unify radius to `--radius-sm/md/lg` (6/8/12).
- Dark fixes: `--background` comma→space HSL (`:78`); `--header-accent` red→brand (`:113`).
- Focus: one focus language — `--ring` = primary; remove amber rings (Navbar:106, login:165).
- Motion tokens: `--ease-out-quart`, `--dur-fast/base` + toast/menu enter-exit keyframes.

### F2. Canonical primitives (`src/components/ui/`)
- `Button` (primary/secondary/ghost/danger/outline; sm/md; loading + icon slots),
  `Input`/`Select` (one height system: h-8 sm, h-9 md), `Card` (surface tokens, no opacity hacks),
  `Modal` (portal + useModalFocus + elevation + unified scrim `bg-black/40`),
  `EmptyState` (icon+title+hint+CTA — extends existing feedback/EmptyState),
  `Skeleton` (shimmer, chart/table/kpi shapes), `StatusPill` (saving/saved/error),
  `Toast` unification: everything through ProgressToastProvider; kill per-page setToast boxes.
- New primitives adopted incrementally — do NOT mass-migrate every page in this wave.

## Wave B — Dashboard builder (sequential within lane; owns `app/page.tsx` + TitleBar)

### B1. Builder chrome + feedback
- Visible **Save button + autosave StatusPill** ("Saving… / Saved · 2s ago / Retry"); Cmd/Ctrl+S.
  Stop swallowing autosave errors (page.tsx:709-711).
- **Empty state** on blank canvas: "Add your first widget" CTA grid with widget-type cards.
- **Add-Card picker**: icons + names + one-line descriptions (replace 7 UPPERCASE text buttons).
- **Merge gear+kebab into one widget menu**; replace glyphs with Remix icons.
- TitleBar redesign: Save/Publish visible; layout utilities ("Pack rows", "Normalize") into a
  "Layout" submenu with plain-language labels; grid/lock/refresh as icon buttons w/ tooltips.
- Chart-shaped skeletons + error card with Retry in ChartCard states; strip console.debug.

### B2. Builder interactions
- **Undo/redo**: history stack (25 steps) over layout+configs; Cmd+Z/Shift+Z; toolbar buttons.
- **Drag perf**: commit layout only on onDragStop/onResizeStop, memoize grid children,
  CSS transform previews during drag; fix ResizeObserver→setState loop (single shared observer,
  rAF-batched); scope the body MutationObserver.
- **Docked inspector**: configurator becomes a docked right panel (resizable 320–480px,
  collapse to icon rail) replacing floating auto-collapse+hover timers. Keep detach as option.
- Whole-card move affordance: grab cursor on header, dedicated drag handle when header hidden.

## Wave C — Configurator: finish V2 and flip (parallel with R/S; owns ConfiguratorPanelV2/)

- Port V1 exclusives into V2: delta resolved-period preview (V1:623,3963), chart-title Format
  accordion (V1:2493-2613), Details section, missing KPI presets.
- Delete duplicated inline `Switch_` (DataTab.tsx:426) — import shared Switch.
- Replace raw `<input type=color>` with token palette control (+ custom hex fallback).
- Polish V2: consistent FormRow density, sticky tab bar, section jump; validation surfacing
  (SQL/formula errors inline).
- **Flip default to V2** (page.tsx:309); keep V1 behind the toggle for one release as fallback.

## Wave R — Report builder (parallel; owns ReportBuilderModal.tsx + TableCellsEditorModal)

- Legibility: eliminate 7–9px text (≥ token 2xs=11px); apply type ramp; fix modal elevation.
- Replace emoji variable badges (`∑ 🗓 Q`) with Remix icons.
- **Dirty state + confirm-on-close + autosave draft** (localStorage) — no more silent loss.
- Surface **export/preview**: wire the existing export path into the builder header
  (button + progress); full preview pane documented as follow-up (needs render service work).
- Position/size controls: proper labeled inputs + align/distribute buttons.
- Keyboard hints into a real help popover; rename affordance visible on hover.

## Wave S — Shell & first impressions (parallel; owns login/home/shell/dialogs)

- **Login**: brand-primary CTA, product framing panel (brand, tagline, subtle data-viz visual),
  fixed dialogs elevation, unified focus.
- **Home**: fix contrast (text-gray-500 → tokens), EmptyStates for favorites/recent/collections,
  onboarding hint card for first-run (no datasource yet → guide to create one).
- Dashboards list: EmptyState + card hover polish.
- Toast unification behind ProgressToastProvider (retire ~10 inline setToast boxes).
- Wrap remaining hand-rolled portals in Modal primitive / useModalFocus (13 of 18 missing).
- Strip console.log (Sidebar:75,155,156,192).
- Micro-interactions: button press states, menu/toast enter-exit animations, card hover
  elevation — all via F1 motion tokens.

## Verification gates (every wave)

- `npx tsc --noEmit` — 0 new errors (baseline: 13 pre-existing).
- `npm test` — vitest green.
- `npm run build` — green at wave end.
- Commit per sub-wave; user eyeballs the running app at checkpoints.

## Wave P — Skill-guided review + polish (FINAL, after B2/C/R/S land)

Added per skill-guided re-review (design-review, design-tokens, ui-animation,
tailwind-design-system, shadcn-ui):

### P1. Screenshot design review (mandatory, replaces informal eyeballing)
- Boot the app; Playwright captures: login, home, builder (populated + empty),
  configurator open, report builder — × desktop 1280 / tablet 768 / mobile 375
  × light + dark. Save under `.design/bayan-v3/screenshots/`.
- Critique each against the review checklist (hierarchy, consistency, states,
  contrast, dark-mode shadows); produce DESIGN_REVIEW.md; fix Must/Should items.

### P2. Motion QA (ui-animation standards)
- Grep-audit: no `transition-all`, no layout-prop transitions (width/height/top/left),
  every animation has a reduced-motion path.
- Rules encoded into implementation: high-frequency ephemeral UI (panel toggles,
  menus, tooltips) enters instantly/fast and exits 100–150ms; NEVER animate
  keyboard-initiated actions (Cmd+S state changes are instant); popovers/menus
  get `transform-origin` at the trigger; modal + scrim share duration/easing.

### P3. Token additions from audit (applied)
- `--ease-enter` (0.22,1,0.36,1) alongside `--ease-out` (move curve); `--shadow-focus`.
- Known debt (accepted): `[class*="ring-"]` !important rule forces ring color to
  --border, so Tailwind ring-color utilities are neutralized — keyboard focus is
  carried by `:focus-visible` outline instead. Revisit only if per-component ring
  colors become needed.

## Out of scope (documented follow-ups)

- Report builder live PDF preview pane (needs snapshot/render service integration).
- Full ChartCard/AlertDialog decomposition (v2 roadmap specs 13/14 partials continue separately).
- V1 configurator deletion (one release after V2 flip).
- Marketing/landing surfaces.
- Tailwind v4 migration (CSS-first `@theme`, OKLCH palette) — real project, own branch.
- shadcn/ui adoption — rejected: repo has fresh canonical primitives mirroring its
  API shape; swapping component stacks now is churn without user value.
