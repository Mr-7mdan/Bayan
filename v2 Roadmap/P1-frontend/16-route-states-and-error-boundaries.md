---
id: 16-route-states-and-error-boundaries
title: Add loading/error/not-found route states and error boundaries
priority: P1
effort: M
depends_on: []
area: frontend
---

## Problem

The App Router (Next 15.5.3, `frontend/`) has almost no route-level states:

- Exactly ONE `loading.tsx` exists: `frontend/src/app/(app)/datasources/sources/loading.tsx`. No `error.tsx`, `global-error.tsx`, or `not-found.tsx` anywhere. An unmatched URL renders Next's default unstyled 404; a render error in any page shows Next's default error screen (prod) with no report and no recovery.
- The only app-wide React error boundary is inline in `ErrorReporterProvider` — and it is broken: `componentDidCatch` reports the issue then does `this.setState({ hasError: false })` and re-renders the SAME children (`frontend/src/components/providers/ErrorReporterProvider.tsx:147-179`). A persistent render error therefore re-throws immediately → report/re-render loop with no fallback UI ever shown.
- 633 empty `catch {}` blocks across `.tsx` files (`grep -rEn "catch \{\s*\}" src --include='*.tsx' | wc -l`) swallow errors silently, including user-facing mutations (delete contact, remove share, toggle favorite) that fail with zero feedback.
- No shared empty-state component; each page hand-rolls (or omits) empty/error UI.

## Current State

Verified 2026-07-07 on branch `feature/alpha-themes-foundation`.

**Route segments** (`frontend/src/app/`):
```
(app)/{about,admin/{environment,holidays,metrics,schedules,users},alerts,builder,
       contacts,dashboards/{mine,shared},datasources/{[id],data-model,mine,new,sources},
       home,home/monitoring,users/change-password}   ← authed shell (Sidebar+Navbar)
page.tsx                                              ← builder root (client)
login/ logout/ reset-password/ demos/kpi/             ← public/aux
v/[id]/                                               ← public dashboard viewer (client)
render/embed/widget/                                  ← iframe embed (client)
```
All pages are `"use client"` with client-side `Api.*` fetching — route `loading.tsx` covers chunk-load/navigation only, not data fetching.

**Special files present:** `src/app/layout.tsx`, `src/app/(app)/layout.tsx`, `src/app/(app)/datasources/sources/loading.tsx` (3-line "Loading…"). Nothing else. Zero `notFound()` callers in `src/`.

**Root layout** `frontend/src/app/layout.tsx:61-87`: provider stack `Environment > Theme > Filters > Branding > Query > Auth > ErrorReporter > ProgressToast > {children}` inside a `Suspense` with fallback `<div className="p-3 text-sm">Loading…</div>`.

**ErrorReporterProvider** `frontend/src/components/providers/ErrorReporterProvider.tsx`:
- Lines 72-145: `window.addEventListener('error'/'unhandledrejection')` handlers with 60s dedupe (`report()` at :61-70, posts via `Api.reportIssue` → `POST /issues/report`, `frontend/src/lib/api.ts:925-929`) and `env.bugReportMode` (`auto|ask|off`) handling incl. an "ask" dialog.
- Lines 147-179: the broken inline `ErrorBoundary` described above:
```tsx
componentDidCatch(error, info) { ...build payload... void report(payload); this.setState({ hasError: false }) }
render() { return this.props.children as any }
```

**Widget boundary** `frontend/src/components/dev/ErrorBoundary.tsx` — small, works, wraps widget cards (KpiCard, ChartCard, TableCard, etc.). Keep as-is.

**Toasts:** no toast lib. `ProgressToastProvider` is sync-progress-specific. `ErrorReporterProvider` renders its own inline toast (lines 203-207). No reusable toast utility exists.

**Empty catches touching user actions** (`grep -rEn "catch \{\s*\}" src/app --include='*.tsx' | grep "Api\."` → 15 hits). The ones that hide real failures from users:
- `src/app/(app)/contacts/page.tsx:254` (`deactivateContact`) and `:259` (`deleteContact`)
- `src/app/(app)/datasources/sources/page.tsx:192` (`deleteDatasourceShare`)
- `src/app/(app)/dashboards/shared/page.tsx:119` (`addFavorite`/`removeFavorite`)
- `src/app/(app)/alerts/page.tsx:268` (`getAlert` row refresh)

(The heartbeat/telemetry ones — `dashboardsOpen/Close` in `src/app/page.tsx:106-113`, `src/app/v/[id]/page.tsx:95-102`, `updatesCheck` in `(app)/layout.tsx:87-88` — are correctly swallowed; leave them.)

**ESLint:** `frontend/.eslintrc.json` — `next/core-web-vitals` + `next/typescript`, eslint 8.57.1. `npm run build` uses `--no-lint`, so new warn-level rules cannot break builds.

## Desired State

1. Route-level `error.tsx` / `loading.tsx` / `not-found.tsx` at the segments that matter (root, `(app)`, public viewer, embed) — errors bubble to the nearest boundary, so per-page files are NOT needed.
2. `ErrorReporterProvider`'s boundary becomes report-then-rethrow so route `error.tsx` renders the UI while reporting still happens.
3. One shared `EmptyState` component reused by error/not-found pages and available for data-empty states.
4. Empty-catch policy: lint rule surfaces new offenders as warnings; the 5 user-facing mutation catches above get toast feedback; a tiny `swallow(err, ctx)` helper exists for deliberate swallows.

## Implementation Plan

### 1. Shared components

Create `frontend/src/components/ui/EmptyState.tsx` (client-safe, no "use client" needed — pure props):
```tsx
export default function EmptyState({ icon, title, description, action }: {
  icon?: React.ReactNode; title: string; description?: string; action?: React.ReactNode
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 py-16 text-center">
      {icon && <div className="text-muted-foreground/60">{icon}</div>}
      <div className="text-sm font-semibold">{title}</div>
      {description && <div className="text-xs text-muted-foreground max-w-md">{description}</div>}
      {action && <div className="mt-2">{action}</div>}
    </div>
  )}
```
Use existing Tailwind tokens (`text-muted-foreground`, `bg-card`, `border`) — same vocabulary as `ErrorReporterProvider`'s dialog. Icons from `@remixicon/react` (already a dependency, see imports in `(app)/layout.tsx:5`).

Create `frontend/src/lib/log.ts`:
```ts
// ponytail: dev-only visibility for deliberately swallowed errors; upgrade to reporting if needed
export function swallow(err: unknown, ctx: string) {
  if (process.env.NODE_ENV !== 'production') console.warn(`[swallowed:${ctx}]`, err)
}
```

### 2. Route state files

All `error.tsx` files are `"use client"` (Next requirement) and follow this shape — report via the existing window listener path (reuses dedupe + `bugReportMode` logic in ErrorReporterProvider, zero new plumbing):
```tsx
"use client"
import { useEffect } from 'react'
import EmptyState from '@/components/ui/EmptyState'

export default function Error({ error, reset }: { error: Error & { digest?: string }; reset: () => void }) {
  useEffect(() => {
    // Route error boundaries stop propagation to window.onerror; re-dispatch so
    // ErrorReporterProvider's existing listener reports it (dedupe + bugReportMode).
    try { window.dispatchEvent(new ErrorEvent('error', { error, message: error.message })) } catch {}
  }, [error])
  return (
    <EmptyState
      title="Something went wrong"
      description={error.digest ? `Error reference: ${error.digest}` : error.message}
      action={<button className="text-xs px-3 py-1.5 rounded-md border hover:bg-muted" onClick={reset}>Try again</button>}
    />
  )}
```

Create, in this order:
1. `frontend/src/app/error.tsx` — the shape above, wrapped in a full-height centering div (it replaces page content inside root layout, so providers/theme are available).
2. `frontend/src/app/global-error.tsx` — catches root-layout/provider crashes. Must render its own `<html><body>` (Next requirement); NO providers/Tailwind assumptions — inline styles, plain text, a "Reload" button (`onClick={() => location.reload()}`). Keep under 30 lines.
3. `frontend/src/app/not-found.tsx` — global 404 for unmatched URLs. `EmptyState` with title "Page not found", description, and `<Link href="/home">Go home</Link>` (`next/link`).
4. `frontend/src/app/loading.tsx` — reuse the existing 3-line pattern from `datasources/sources/loading.tsx`.
5. `frontend/src/app/(app)/error.tsx` — same shape as root `error.tsx` (renders inside the Sidebar/Navbar shell, so navigation stays usable).
6. `frontend/src/app/(app)/loading.tsx` — same 3-line pattern.
7. `frontend/src/app/(app)/not-found.tsx` — `EmptyState` "Page not found" + link to `/home` (enables future `notFound()` calls inside the shell).
8. `frontend/src/app/v/[id]/error.tsx` — public viewer: same shape but NO link to `/home` (viewers may be unauthenticated); description "This dashboard could not be displayed." + Try again.
9. `frontend/src/app/v/[id]/loading.tsx` — 3-line pattern.
10. `frontend/src/app/render/embed/widget/error.tsx` — iframe-safe: tiny centered text `Widget failed to load` + retry button, no navigation, no EmptyState padding (embeds can be 100px tall). Inline minimal markup.
11. Delete `frontend/src/app/(app)/datasources/sources/loading.tsx` — now redundant with `(app)/loading.tsx`.

Do NOT add files under `login/`, `logout/`, `reset-password/`, `demos/` — root-level `error.tsx`/`loading.tsx` covers them.

### 3. Fix ErrorReporterProvider boundary (report-then-rethrow)

In `frontend/src/components/providers/ErrorReporterProvider.tsx`, replace the inline `ErrorBoundary` class (lines 147-179):
- Add `static getDerivedStateFromError(error: Error) { return { error } }` (state becomes `{ error: Error | null }`).
- In `componentDidCatch`: keep the payload build + `report()` call exactly as-is, but DELETE every `this.setState({ hasError: false })` line (three occurrences at :173, :174, :176).
- In `render()`: `if (this.state.error) throw this.state.error` then `return this.props.children`.

Effect: errors thrown by the providers themselves get reported once, then rethrow to `global-error.tsx`. Page errors never reach this boundary anymore (route `error.tsx` is nearer) and are reported via the re-dispatch in step 2.

### 4. Empty-catch policy + targeted fixes

1. `frontend/.eslintrc.json`: add `"no-empty": ["warn", { "allowEmptyCatch": false }]` to `rules`. Warn-only — 633 existing hits must not block anything (`next build --no-lint` ignores lint anyway; `npm run lint` shows them as debt).
2. Fix the 5 user-facing catches (Current State list). Pattern for each — replace `catch {}` with a toast-ish inline error. These pages have no toast util; use the page's existing error/status state if present, else the minimal fix: `catch (e) { alert(...) }` is NOT acceptable — instead add a transient error banner state. Concretely:
   - `contacts/page.tsx:254,259`: the page has `const [err, setErr] = useState(...)`-style state? Check top of file; if a `msg`/`error` state exists reuse it, else add `const [actionErr, setActionErr] = useState<string | null>(null)` rendered as a dismissible `text-xs text-rose-600` line above the table; set it in both catches (`'Failed to update contact'` / `'Failed to delete contact'`).
   - `datasources/sources/page.tsx:192`: same approach in the shares dialog (`'Failed to remove share'`).
   - `dashboards/shared/page.tsx:119`: favorite toggle — on failure revert the optimistic state and set the page's error state (`'Failed to update favorite'`).
   - `alerts/page.tsx:268`: row refresh after action — `catch (e) { swallow(e, 'alerts.refreshRow') }` (non-critical; the action itself already surfaced).
3. Codemod plan for the remaining ~628 (FOLLOW-UP, not this spec — document only): mechanical pass converting `catch {}` → `catch (e) { swallow(e, '<file>:<fn>') }` is noise; instead the policy is: new code must use `swallow()` or handle the error; existing swallows get upgraded opportunistically when a file is touched. Record this in the PR description.

### 5. Ordering / compat

No migration, no backend changes, no API changes. Files are additive except the `ErrorReporterProvider` edit and the one deleted `loading.tsx`. `error.tsx` files must NOT import `ErrorReporterProvider` or any provider hooks (they render even when providers crash — hence the `window.dispatchEvent` pattern instead of a context hook).

## Files to Modify

- `frontend/src/components/ui/EmptyState.tsx` — NEW shared empty/error-state component
- `frontend/src/lib/log.ts` — NEW `swallow()` helper
- `frontend/src/app/error.tsx` — NEW root error boundary UI + re-dispatch reporting
- `frontend/src/app/global-error.tsx` — NEW self-contained root-layout crash page
- `frontend/src/app/not-found.tsx` — NEW global 404
- `frontend/src/app/loading.tsx` — NEW root loading
- `frontend/src/app/(app)/error.tsx` — NEW in-shell error boundary
- `frontend/src/app/(app)/loading.tsx` — NEW in-shell loading
- `frontend/src/app/(app)/not-found.tsx` — NEW in-shell 404
- `frontend/src/app/v/[id]/error.tsx` — NEW public viewer error
- `frontend/src/app/v/[id]/loading.tsx` — NEW public viewer loading
- `frontend/src/app/render/embed/widget/error.tsx` — NEW iframe-safe error
- `frontend/src/app/(app)/datasources/sources/loading.tsx` — DELETE (superseded)
- `frontend/src/components/providers/ErrorReporterProvider.tsx` — fix boundary to report-then-rethrow (lines 147-179)
- `frontend/.eslintrc.json` — add `no-empty` warn rule
- `frontend/src/app/(app)/contacts/page.tsx` — error feedback on 2 mutation catches (:254, :259)
- `frontend/src/app/(app)/datasources/sources/page.tsx` — error feedback on share-remove catch (:192)
- `frontend/src/app/(app)/dashboards/shared/page.tsx` — revert + error on favorite catch (:119)
- `frontend/src/app/(app)/alerts/page.tsx` — `swallow()` on row-refresh catch (:268)

## Acceptance Criteria

- [ ] `npm run build` (in `frontend/`) succeeds; `npx tsc --noEmit` clean for new/modified files
- [ ] Visiting `http://localhost:3000/does-not-exist` renders the styled global 404 with a working "Go home" link
- [ ] A thrown render error in an `(app)` page shows the in-shell error UI (Sidebar/Navbar still visible), "Try again" calls `reset()`, and exactly one `POST /issues/report` fires (dedupe holds on re-throw within 60s)
- [ ] A thrown render error in `/v/[id]` shows the chrome-less public error UI
- [ ] `ErrorReporterProvider` boundary no longer resets `hasError`/re-renders erroring children; it rethrows after reporting
- [ ] `bugReportMode: 'off'` suppresses the report but the error UI still renders; `'ask'` shows the existing submit dialog
- [ ] Failed contact delete/deactivate, share removal, and favorite toggle each show visible error feedback
- [ ] `npm run lint` reports `no-empty` warnings (not errors); build unaffected
- [ ] No `error.tsx`/`not-found.tsx` imports provider hooks or `Api` directly

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/frontend
npx tsc --noEmit
npm run build
npm run lint 2>&1 | grep -c "no-empty"   # > 0 warnings, exit code still usable
# confirm no leftover self-reset in the provider boundary:
grep -n "hasError: false" src/components/providers/ErrorReporterProvider.tsx  # expect no matches
```
Manual (dev server `npm run dev`, backend on :8000):
1. Open `/nonexistent-route` → styled 404.
2. Temporarily add `if (search.get('boom')) throw new Error('test-boom')` at the top of `src/app/(app)/contacts/page.tsx` render; visit `/contacts?boom=1` → in-shell error UI; DevTools Network shows one `POST /issues/report`; click "Try again" without `?boom` → recovers. Remove the test line.
3. Same probe on `/v/<any-id>?boom=1` → chrome-less error UI.
4. Stop the backend, try deleting a contact → error banner appears instead of silent no-op.

## Out of Scope

- Codemodding the remaining ~628 empty catches (policy documented; opportunistic cleanup only)
- A general toast system/library (inline banners suffice; revisit if a third page needs one)
- Per-page `loading.tsx` skeletons (pages fetch client-side; route loading only covers chunk load)
- Server-side `notFound()` adoption in `[id]` pages (pages are client components; API-404 → EmptyState is a separate change)
- Widget-level `ErrorBoundary` (`src/components/dev/ErrorBoundary.tsx`) — unchanged
- Backend `/issues/report` behavior
