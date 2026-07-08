---
id: 14-alertdialog-decomposition
title: Refactor AlertDialog state sprawl
priority: P1
effort: L
depends_on: []
area: frontend
---

## Problem

`frontend/src/components/alerts/AlertDialog.tsx` is a single 2,502-line component with 102 `useState` calls, 164 `as any` casts, and 21 `useEffect`s (16 in the main component, 5 in the three in-file helper components). Form hydration is a ~220-line effect (lines 546–767) that imperatively calls ~50 setters; `buildPayload()` (lines 1267–1474) reads ~30 pieces of state through untyped casts. Every render-mode/widget-pick interaction is coordinated through effects that write state read by other effects, which is exactly how the `render.mode='report'` clobbering bug happened (see the uncommitted `hydratedPickRef` fix below). The component is effectively unmaintainable and untypecheckable.

## Current State

All references verified 2026-07-07 against the working tree (includes uncommitted changes).

- **File**: `frontend/src/components/alerts/AlertDialog.tsx` — 2,502 lines.
- **Callers** (public API must not change):
  - `frontend/src/app/(app)/alerts/page.tsx:558,561` — create and edit instances.
  - `frontend/src/components/widgets/WidgetActionsMenu.tsx:998` — passes `widget={cfg}`, `parentDashboardId`, `defaultKind='notification'`, `defaultTemplate='{{KPI_IMG}}'`.
- **Props signature** (line 280): `{ open, mode, onCloseAction, onSavedAction, alert?, widget?, parentDashboardId?, defaultKind?, defaultTemplate? }`.
- **In-file helpers** (pure, no dialog-state deps): `ymd` (13), `parseCron` (20), `buildCron` (42), `defaultAggFromSpec` (60).
- **In-file subcomponents** (self-contained, own local state): `NumberFilterDetails` (70–133), `DateRangeDetails` (135–170), `ValuesFilterPicker` + `DISTINCT_CACHE`/`DISTINCT_TTL_MS` (172–278).
- **Main component** (280–2502). State clusters:
  - Header/renderer: `editAlertId, name, kind, enabled, chanEmail, chanSms, renderMode, attachPdf, pdfLandscape, snapWidth, snapHeight, useWidgetAspect, rendererUseCarried, pickDashId, pickWidgetId, pickWidgetCfg` (285–318, 407–415).
  - Recipients: `recipTokens, recipInput, recipSuggestions, recipSugOpen, recipSel, tagExpansions, emailTo, smsTo` (289–290, 319–404).
  - Message/templates: `template, templateSms, customPlaceholders, fmtSize, fmtFontColor, fmtBgColor, fmtBorderColor, fmtTarget, fmtMarginTop, fmtMarginBottom, msgKind, msgView` + refs `phAreaRefs, rawAreaRef` (291–305).
  - Trigger/schedule: `triggerType, operator, value, triggerTimeEnabled, triggerThresholdEnabled, triggerLogic, timeOfDay, daysOfWeek, daysOfMonth, everyHours, scheduleKind, aggSel, measureSel, xValueSel` (417–432).
  - Advanced builder: `advOpen, advDatasourceId, advSource, advAgg, advMeasure, advWhere, advXField, advXValue, advXPick, advXFrom, advXTo, advPivot, detailKind, detailField, advCalcMode` (434–462).
  - Server data caches: `dsList, tableList, advTablesLoading, tablesMeta, columns, dsTransforms, dashList, pickWidgets, pickDashLayouts, refLayout, runs` (408–450, 939).
  - Test/preview: `testHtml, testEmailHtml, testSmsText, testContext, testSummary, testActiveTab, testEvaluating, localKpi, localKpiLoading, perCatStats, perCatLoading, perCatMatches, showPayload, lastPayload` (464–478).
  - `uiSection` (305) drives the five tabs: `'header'|'recipients'|'trigger'|'insert'|'preview'`.
- **Key effects in main component**: create-defaults (481), giant edit/create hydration (546–767), dsList load (769), dashList load (776), picked-dashboard widgets load (787), `pickWidgetCfg` derive (809 — pure derivation, no async), auto-derive `renderMode` (819–838), tables load (840), columns compute (884), carried-widget layout load (912), aspect-ratio height sync (931 — pure derivation), dsTransforms load (940), local KPI compute (1000–1104), per-category matches compute (1107–1242).
- **Verified dead state** (written, never read / never written):
  - `triggerType` (417) — only `setTriggerType` calls at 616, 628, 738; value never read.
  - `xValueSel` (432) — read at 1258, 1285, 1448, 1829 but `setXValueSel` is never called; always `''`.
  - `testSummary` (474) — set at 1483, 1492; never rendered.
  - `emailTo`/`smsTo` (289–290) — only ever set to `''` (565–566, 731–732); no bound inputs remain; reads in `buildPayload` (1352–1353) always see `''`.
- **`as any` hotspots**: 27× `(widget as any)` (WidgetConfig in `frontend/src/types/widgets.ts:291` has `id/type/title/querySpec/datasourceId` typed, but NOT `dashboardId` — callers attach it ad hoc); 41× casts on `cfg`/`th`/`tm`/`r` because `AlertConfig` in `frontend/src/lib/api.ts:1224` types `triggers/actions/render` as `Record<string, any>`.
- **UNCOMMITTED working-tree change (must be preserved semantically, do not revert)**: `hydratedPickRef` (`useRef<string|null>`, lines 311–316) set/cleared at 596–609, 730 and guarding the auto-derive effect at 828–832 (dep `pickWidgetId` added at 838); plus Attach PDF visibility `chanEmail && (renderMode === 'report' || attachPdf)` at 1557. Purpose: async-loaded picked-widget cfg must not clobber a saved `render.mode='report'`.

## Desired State

- `AlertDialog.tsx` shrinks to a ~300-line shell: portal/backdrop, tab nav, one `useReducer` for all form state, async data-loading effects, `onSave`/`onTestEvaluate`.
- All form state lives in one typed `DialogState` managed by a reducer in `dialog/state.ts`; hydration and payload building are pure, unit-testable functions.
- Five section components (one per `uiSection` tab) receive `{ state, dispatch }` plus the server-data they render.
- `as any` count in the alerts dialog code drops from 164 to <20 (remaining only at true unknown-JSON boundaries, e.g. `tablesMeta`).
- Main-component effect count drops from 16 to ≤10 by deleting pure-derivation effects and dead state.
- The `hydratedPickRef` fix semantics are preserved as reducer state (`header.hydratedPickWidgetId`).

## Implementation Plan

Do this on top of the current working tree (the uncommitted `AlertDialog.tsx` change is part of the input). All new files go in `frontend/src/components/alerts/dialog/`.

**Step 1 — `dialog/types.ts` (new).** Typed shapes for the untyped JSON config:

```ts
import type { WidgetConfig } from '@/types/widgets'
export type CarriedWidget = WidgetConfig & { dashboardId?: string }   // callers attach dashboardId ad hoc
export type XPick = 'custom'|'range'|'today'|'yesterday'|'this_month'|'last'|'min'|'max'
export type AggKind = 'count'|'sum'|'avg'|'min'|'max'|'distinct'
export type TimeTrigger = { type: 'time'; cron: string }
export type ThresholdTrigger = {
  type: 'threshold'; source: string; aggregator: AggKind; measure?: string; y?: string
  where?: Record<string, unknown>; xField?: string; xValue?: string | number
  legendField?: string; legendFields?: string[]; rowFields?: string[]
  xMode?: 'custom'|'token'|'special'|'range'; xToken?: string; xSpecial?: string
  xRange?: { from?: string; to?: string }
  operator: string; value: number | number[]; calcMode?: 'query'|'pivot'
}
export type EmailAction = { type: 'email'; to: string[]; subject?: string; attachPdf?: boolean; pdfLandscape?: boolean }
export type SmsAction = { type: 'sms'; to: string[]; message?: string }
export type AlertAction = EmailAction | SmsAction
export type RenderConfig = {
  mode: 'kpi'|'table'|'chart'|'report'; label?: string; querySpec?: Record<string, unknown>
  width?: number; height?: number; widgetRef?: { dashboardId: string; widgetId: string }
}
export type TriggersGroup = {
  logic: 'AND'|'OR'
  time: { enabled: boolean; time?: string; schedule: { kind: 'hourly'|'weekly'|'monthly'; everyHours?: number; dows?: number[]; doms?: number[] } }
  threshold: ({ enabled: true } & Omit<ThresholdTrigger,'type'>) | { enabled: false }
}
export type RecipientToken = { kind: 'contact'|'email'|'phone'|'tag'; label: string; value: string; email?: string; phone?: string; id?: string; name?: string; tag?: string }
```

Then in `frontend/src/lib/api.ts:1224` narrow `AlertConfig`: `triggers: Array<TimeTrigger | ThresholdTrigger | Record<string, any>>`, `actions: AlertAction[]`, `render?: RenderConfig`, `triggersGroup?: TriggersGroup` — keep the existing `[key: string]: any` index signature for backward compat with saved configs. Import the new types from `@/components/alerts/dialog/types` (or, to avoid a lib→components import, put the types in `frontend/src/types/alerts.ts` and import from both — pick this if `lib/api.ts` importing from `components/` feels wrong; either compiles).

**Step 2 — `dialog/state.ts` (new).** Move `ymd`, `parseCron`, `buildCron`, `defaultAggFromSpec` here verbatim. Define the reducer:

```ts
export type DialogState = {
  ui: { section: 'header'|'recipients'|'trigger'|'insert'|'preview'; msgKind: 'email'|'sms'; msgView: 'preview'|'raw'; testActiveTab: 'email'|'sms'|'context'|'raw'; showPayload: boolean }
  header: {
    editAlertId: string | null; name: string; kind: 'alert'|'notification'; enabled: boolean
    chanEmail: boolean; chanSms: boolean
    renderMode: 'kpi'|'table'|'chart'|'report'; attachPdf: boolean; pdfLandscape: boolean
    snapWidth: number; snapHeight: number; useWidgetAspect: boolean
    rendererUseCarried: boolean; pickDashId: string; pickWidgetId: string
    hydratedPickWidgetId: string | null   // replaces hydratedPickRef — same semantics
  }
  recipients: { tokens: RecipientToken[]; input: string; sugOpen: boolean; selected: string[] }  // selected as array (Set is awkward in reducers)
  message: { template: string; templateSms: string; customPlaceholders: Array<{ name: string; html: string }>
             fmt: { size: string; fontColor: string; bgColor: string; borderColor: string; target: string; marginTop: number; marginBottom: number } }
  trigger: { timeEnabled: boolean; thresholdEnabled: boolean; logic: 'AND'|'OR'; operator: string; value: string
             timeOfDay: string; daysOfWeek: number[]; daysOfMonth: number[]; everyHours: number; scheduleKind: 'hourly'|'weekly'|'monthly'
             aggSel: string; measureSel: string }
  adv: { open: boolean; datasourceId: string; source: string; agg: AggKind; measure: string; where: string
         xField: string; xValue: string; xPick: XPick; xFrom: string; xTo: string
         pivot: PivotAssignments; calcMode: 'query'|'pivot'; detailKind: 'filter'|'x'|'value'|'legend'|null; detailField?: string }
}

export type DialogAction =
  | { type: 'hydrateEdit'; alert: AlertOut; widget?: CarriedWidget | null; parentDashboardId?: string | null }
  | { type: 'resetCreate'; widget?: CarriedWidget | null; defaultKind?: 'alert'|'notification'; defaultTemplate?: string }
  | { type: 'patch'; slice: keyof DialogState; patch: Partial<DialogState[keyof DialogState]> }  // see typed helper below
  | { type: 'pickDashboard'; id: string }                       // sets pickDashId, clears pickWidgetId
  | { type: 'pickWidget'; id: string }                          // user re-pick: clears hydratedPickWidgetId when id differs
  | { type: 'deriveRenderMode'; widgetType: string; source: 'carried'|'picked'; pickedWidgetId?: string }
  | { type: 'setRendererUseCarried'; carried: boolean }
  | { type: 'addRecipToken'; token: RecipientToken }            // dedupe logic from addRecipToken (line 332)
  | { type: 'removeRecipToken'; index: number }
  | { type: 'toggleRecipSel'; key: string }
  | { type: 'pivotUpdated'; pivot: PivotAssignments }           // includes removed-filter where-cleanup (lines 2210–2216)
```

Do NOT create one action per field — the `patch` action with a typed dispatch helper covers the ~70 plain `setX(v)` call sites:

```ts
export function patch<K extends keyof DialogState>(slice: K, p: Partial<DialogState[K]>): DialogAction {
  return { type: 'patch', slice, patch: p }
}
// reducer case: return { ...state, [a.slice]: { ...state[a.slice], ...a.patch } }
```

`deriveRenderMode` reducer case ports the uncommitted guard verbatim: if `source==='picked' && state.header.hydratedPickWidgetId && pickedWidgetId === state.header.hydratedPickWidgetId` → return state unchanged; else map `report/table/chart/*→kpi` (logic from lines 819–838).

Pure hydration functions replacing the effect at 546–767 (move the setter bodies into object literals, including the `wref` same-widget check at 588–611 that sets `hydratedPickWidgetId`): `export function stateFromAlert(alert: AlertOut, widget: CarriedWidget | null | undefined, parentDashboardId: string | null | undefined): DialogState` and `export function stateForCreate(widget, defaultKind, defaultTemplate): DialogState`. Reducer handles `hydrateEdit`/`resetCreate` by calling them. Fold the create-defaults effect (481–487) into `resetCreate`.

`export function buildAlertPayload(state: DialogState, ctx: { mode: 'create'|'edit'; alert?: AlertOut | null; widget?: CarriedWidget | null; parentDashboardId?: string | null; pickWidgetCfg: WidgetConfig | null; pickWidgets: Array<{id:string;title:string;cfg:WidgetConfig}>; tagExpansions: Record<string,{emails:string[];phones:string[]}>; recipSuggestions: RecipSuggestion[] }): AlertCreate` — verbatim port of `buildPayload` (1267–1474) typed with `ThresholdTrigger`/`RenderConfig`/`TriggersGroup`. Drop the dead `emailTo`/`smsTo` splitList block (1349–1354) and all `xValueSel` reads (substitute `undefined`/`''` — it is provably always `''`).

**Step 3 — `dialog/FilterDetails.tsx` (new).** Verbatim move of `NumberFilterDetails`, `DateRangeDetails`, `ValuesFilterPicker`, `DISTINCT_CACHE`, `DISTINCT_TTL_MS` (lines 70–278). Export all three. No behavior change.

**Step 4 — section components (new, JSX moved verbatim, `setX(...)` → `dispatch(patch('slice',{x:...}))`):**
- `dialog/HeaderSection.tsx` — lines 1547–1625 (name/type/enabled, channels incl. the uncommitted Attach PDF condition at 1557, renderer pick, snapshot dims). Props: `state, dispatch, dashList, pickWidgets, refLayout, widget, parentDashboardId`.
- `dialog/RecipientsSection.tsx` — lines 1627–1678 + the token helpers `recipKey/isValidEmail/isValidPhone/tryCommitRecipInput/addSelectedRecipients` (328–357). Props: `state, dispatch, recipSuggestions`. Keep `isValidEmail`/`isValidPhone` exported from `state.ts` (buildAlertPayload needs them too).
- `dialog/TemplateSection.tsx` — lines 1680–2032 (chips, formatting toolbar, custom placeholders, raw/preview). Owns `phAreaRefs`/`rawAreaRef` and the `wrapSelAt`/`spanStyleAt`/`divStyleAt`/`bulletsAt`/`presetNormalAt`/`presetTableAt` callbacks (490–528) plus the token-context preview IIFE (1824–2029). Props: `state, dispatch, localKpi`.
- `dialog/TriggerSection.tsx` — lines 2034–2258 (trigger toggles, threshold, schedule, advanced builder with `PivotBuilder` and `FilterDetails`). Props: `state, dispatch, dsList, tableList, advTablesLoading, numericFields, dateLikeFields, allFieldNames, measureOptions`.
- `dialog/PreviewSection.tsx` — lines 2262–2490 (inline test, per-legend chips, grouped preview, carried summary, runs table). Props: `state, dispatch, testResult, testEvaluating, localKpi, localKpiLoading, perCatStats, perCatLoading, perCatMatches, carriedSummary, runs, onTestEvaluate, buildPayloadForDisplay`.

**Step 5 — rewrite `AlertDialog.tsx` as the shell.** Same default export, same props type (use `CarriedWidget` for `widget`). Contains:
- `const [state, dispatch] = useReducer(dialogReducer, undefined, () => stateForCreate(widget, defaultKind, defaultTemplate))`.
- One hydration effect replacing 546–767: on `[open, mode, alert?.id, widget?.id]` dispatch `hydrateEdit` or `resetCreate`; keep the `Api.listAlertRuns` fetch (694) here as a separate async load into `runs` local state.
- Server-data as plain `useState` (NOT in the reducer — it is cache, not form state): `dsList, dashList, pickWidgets, pickDashLayouts, tableList, tablesMeta, columns, dsTransforms, recipSuggestions, tagExpansions, refLayout, runs, localKpi(+loading), perCatStats/perCatMatches(+loading)`, and one `testResult` object `{ html, emailHtml, smsText, context }` replacing 5 separate useStates (drop dead `testSummary`).
- Keep async effects, retargeted to `state.*` fields: recip suggestions debounce (358), tagExpansions (383), dsList (769), dashList (776), picked-dash widgets (787), tables (840), columns (884), carried layout (912), dsTransforms (940), local KPI (1000), per-cat (1107).
- **Delete** these effects/state:
  - `pickWidgetCfg` state + effect (809–816) → `const pickWidgetCfg = useMemo(() => pickWidgets.find(w => w.id === state.header.pickWidgetId)?.cfg ?? null, [pickWidgets, state.header.pickWidgetId])`; same memo pattern for `refLayout` from `pickDashLayouts` when not carried.
  - Aspect-height sync effect (931–936) → `const effectiveSnapHeight = state.header.useWidgetAspect && refLayout?.w && refLayout?.h ? Math.max(80, Math.round(state.header.snapWidth * (refLayout.h / refLayout.w))) : state.header.snapHeight`; pass to HeaderSection and `buildAlertPayload` ctx.
  - Auto-derive renderMode effect (819–838) → keep as ONE small effect that only dispatches `deriveRenderMode` when the relevant widget type is known; the guard lives in the reducer.
  - Dead state: `triggerType`, `xValueSel`, `testSummary`, `emailTo`, `smsTo`.
- `onSave` (1504) and `onTestEvaluate` (1476) stay here, calling `buildAlertPayload(state, ctx)`.
- Keep `carriedSummary` memo (1244) here (or move to `state.ts` as a pure fn) — retype without `as any` using `ThresholdTrigger`.

**Step 6 — typing sweep.** Replace `(widget as any)` with `CarriedWidget` accesses; replace `cfg`/`th`/`tm`/`r` casts with the Step-1 types (use type guards `t.type === 'threshold'` on the trigger union). `tablesMeta` may stay loosely typed (`IntrospectResult`-shaped JSON) — do not invent a full schema for it.

**Step 7 — cleanup.** Confirm nothing else imports the moved helpers (grep `parseCron|buildCron|defaultAggFromSpec|ValuesFilterPicker` repo-wide — currently only defined/used inside AlertDialog.tsx). Run verification below.

Migration order matters: Steps 1–3 are additive and compile independently; Step 4 components can be extracted one tab at a time (each commit keeps AlertDialog.tsx compiling by rendering the extracted component in place of the inline JSX); Step 5 lands the reducer last.

## Files to Modify

- `frontend/src/components/alerts/dialog/types.ts` — NEW: `CarriedWidget`, trigger/action/render/group/recipient types.
- `frontend/src/components/alerts/dialog/state.ts` — NEW: `DialogState`, `DialogAction`, `dialogReducer`, `patch()`, `stateFromAlert`, `stateForCreate`, `buildAlertPayload`, moved cron/date/agg helpers, `isValidEmail`/`isValidPhone`.
- `frontend/src/components/alerts/dialog/FilterDetails.tsx` — NEW: verbatim `NumberFilterDetails`, `DateRangeDetails`, `ValuesFilterPicker`, `DISTINCT_CACHE`.
- `frontend/src/components/alerts/dialog/HeaderSection.tsx` — NEW: header tab JSX.
- `frontend/src/components/alerts/dialog/RecipientsSection.tsx` — NEW: recipients tab JSX + token helpers.
- `frontend/src/components/alerts/dialog/TemplateSection.tsx` — NEW: insert-template tab JSX + textarea formatting callbacks.
- `frontend/src/components/alerts/dialog/TriggerSection.tsx` — NEW: trigger tab JSX incl. advanced builder.
- `frontend/src/components/alerts/dialog/PreviewSection.tsx` — NEW: preview tab JSX (test, chips, runs).
- `frontend/src/components/alerts/AlertDialog.tsx` — REWRITE as shell (same path/export/props; callers untouched).
- `frontend/src/lib/api.ts` — narrow `AlertConfig` fields (line 1224) keeping the index signature.

## Acceptance Criteria

- [ ] `AlertDialog.tsx` ≤ 400 lines; no extracted section exceeds ~600 lines.
- [ ] Exactly one `useReducer` holds all form state; `grep -c useState` across `AlertDialog.tsx` + `dialog/*.tsx` ≤ 30 (server-data caches + subcomponent-local state only).
- [ ] `grep -c "as any"` across `AlertDialog.tsx` + `dialog/*` ≤ 20.
- [ ] `grep -c useEffect frontend/src/components/alerts/AlertDialog.tsx` ≤ 10 (shell only).
- [ ] Dead state removed: `triggerType`, `xValueSel`, `testSummary`, `emailTo`, `smsTo`.
- [ ] Uncommitted-fix semantics preserved: editing a saved alert whose `render.widgetRef` points at a different widget and `render.mode='report'` keeps `report` after the picked widget cfg loads async; re-picking a different widget re-derives the mode; Attach PDF/Landscape checkboxes visible when `renderMode==='report'` or `attachPdf` already set.
- [ ] Saved payload shape unchanged: `buildAlertPayload` output deep-equals old `buildPayload` for the same inputs (top-level `source/where/agg/measure` back-compat fields, `triggersGroup`, `customPlaceholders`, `render.widgetRef` fallback chain all intact).
- [ ] `AlertDialog` props and default export unchanged; `alerts/page.tsx` and `WidgetActionsMenu.tsx` compile without edits.
- [ ] `npx tsc --noEmit` and `npm run build` pass in `frontend/`.

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/frontend
npx tsc --noEmit
npm run build
grep -c "useState" src/components/alerts/AlertDialog.tsx src/components/alerts/dialog/*.tsx
grep -c "as any"   src/components/alerts/AlertDialog.tsx src/components/alerts/dialog/*.tsx src/components/alerts/dialog/*.ts
grep -c "useEffect" src/components/alerts/AlertDialog.tsx
```

Manual (app running on :3000, backend up):
1. Alerts page → Create: fill name, add recipient token (typed email + contact suggestion + `#tag`), enable Time+Threshold triggers with Advanced builder (pick datasource/table, one Value, a filter with the values picker), Test evaluate → Email/SMS/Context tabs render; Save succeeds.
2. Re-open saved alert in Edit: every field re-hydrates (schedule days, threshold operator/value, advanced pivot chips, custom placeholders, recipient tokens); removing a token then saving does not resurrect the address.
3. Regression (the uncommitted fix): create a notification via a widget's actions menu, switch renderer to "Pick from dashboard", pick a table widget, set View-as `report` + Attach PDF, save; re-open → mode still `report`, Attach PDF checked and visible. Then pick a different widget → mode re-derives from that widget's type.
4. Notification with carried widget: "Use widget dimensions/aspect ratio" disables height and computes it from layout; saved `render.width/height` match.

## Out of Scope

- Any behavior/UX changes, visual redesign, or new features in the dialog.
- Refactoring sibling dialogs (`AlertEditDialog.tsx`, `AlertsDialog.tsx` in the same folder) or `PivotBuilder`.
- Backend `AlertConfig` schema changes; the payload wire format is frozen.
- Adding a state-machine library (XState etc.) — plain `useReducer` only.
- Unit-test scaffolding beyond what exists (add tests only if a test runner is already configured; none was found for frontend/).
