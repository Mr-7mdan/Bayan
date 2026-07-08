---
id: 17-i18n-and-rtl
title: Internationalization and RTL support
priority: P1
effort: L
depends_on: []
area: frontend
---

## Problem

Bayan is an Arabic-branded product with zero i18n: no i18n library in `frontend/package.json`, `<html lang="en">` hardcoded with no `dir` attribute, all UI strings inlined in JSX, and hundreds of physical (LTR-only) Tailwind directional classes. Arabic users cannot get an Arabic/RTL UI.

## Current State

All paths relative to `/Users/mohammed/Documents/Bayan/frontend/`.

- `src/app/layout.tsx:63` — hardcoded, no `dir`, `RootLayout` is sync:
  ```tsx
  <html lang="en" className="h-full">
  ```
- `src/app/layout.tsx:16` — `const inter = Inter({ subsets: ['latin'] })`. Inter has no Arabic glyphs.
- `src/middleware.ts:4-16` — only redirects `/` → `/home`, matcher `['/']`. No locale logic.
- `next.config.js:5` — `typedRoutes: true` (a `[locale]` path-segment restructure would churn every typed link; the cookie-based approach below avoids that entirely).
- No i18n library installed (`grep -rn "next-intl\|react-i18next" src/` → no hits).
- `src/config/navigation.ts:6-18` — nav labels are English strings that double as stable keys: `src/components/shell/Sidebar.tsx:32-52` `iconFor(label)` switches on the English label, and `Sidebar.tsx:167-189` `badgeFor(label)` does too. Naively translating `label` at the source breaks both switches.
- Shell strings concentrate in:
  - `src/components/shell/Sidebar.tsx` (267 lines): `'Admin'` group at 110, `About`/`Change Password`/`Logout` at 246/253/260, `'User'` fallback at 233.
  - `src/components/shell/Navbar.tsx` (146 lines): `aria-label={sidebarOpen ? 'Hide sidebar' : 'Show sidebar'}` at 94, `groupLabelOverride` at 42-44, breadcrumb `'Home'` at 66.
  - `src/app/(app)/layout.tsx` (203 lines): `"What's new"` at 156, `Backend/Frontend` at 157/162/168, `"Updated to the latest version."` at 173, `Got it` at 186, `Loading…` at 198.
  - `src/app/login/page.tsx` (251 lines): login form strings.
- Physical-direction Tailwind classes across `src/**/*.tsx` (counted 2026-07-07): **214** `ml-/mr-/pl-/pr-`, **134** `left-/right-` positions, **199** `text-left/text-right`, **9** `space-x-/divide-x-`, **9** `rounded-l/r/tl/...`. Zero logical (`ms-/me-/ps-/pe-`) classes exist yet. Tailwind is `3.4.4` (`tailwind.config.ts`), which ships logical-property utilities natively — no plugin needed.
- RTL-hostile shell specifics:
  - `Sidebar.tsx:123` `border-r` (should be `border-e`).
  - `Sidebar.tsx:193` hide animation `-translate-x-full` (slides the wrong way in RTL).
  - `Sidebar.tsx:66` `pl-6 pr-3` nested indent; `Sidebar.tsx:79` `ml-auto` badge; `Sidebar.tsx:233` `text-left`.
  - `Navbar.tsx:100` `mx-2` divider (fine); `Navbar.tsx:107` `RiArrowRightSLine` breadcrumb chevron (must mirror in RTL).
  - `src/app/(app)/layout.tsx:142,153` toast/changelog `fixed ... right-6` (should be `end-6`).

## Desired State

- `next-intl` (v4) installed and configured in **cookie-based "without i18n routing" mode**: no `app/[locale]` restructure, no URL prefixes, no middleware changes, typedRoutes untouched. Locale persisted in a `NEXT_LOCale`-style cookie (`NEXT_LOCALE`), default `en`, supported: `en`, `ar`.
- `<html lang={locale} dir={locale === 'ar' ? 'rtl' : 'ltr'}>` rendered server-side (no flash).
- Arabic-capable font (`IBM Plex Sans Arabic` via `next/font/google`, subsets `['arabic','latin']`) applied when locale is `ar`; Inter kept for `en`.
- Language switcher (EN/عربية) in the Navbar next to `ThemeToggle`; switching sets the cookie and hard-reloads.
- Phase-1 string externalization (NOT big-bang): shell only — `Sidebar.tsx`, `Navbar.tsx`, `(app)/layout.tsx`, `navigation.ts` labels (translated at render, stable English keys preserved), `login/page.tsx`, root loading fallbacks. Everything else translated incrementally in later specs; untranslated strings simply render in English.
- Shell components converted to logical Tailwind properties so the frame (sidebar, navbar, toasts) is correct in RTL. Repo-wide class conversion is a documented follow-up, not this spec.

## Implementation Plan

1. **Install**: `cd frontend && npm install next-intl` (v4.x; supports Next 15.5 App Router without routing).

2. **Create `frontend/src/i18n/request.ts`**:
   ```ts
   import { getRequestConfig } from 'next-intl/server'
   import { cookies } from 'next/headers'

   export const SUPPORTED_LOCALES = ['en', 'ar'] as const

   export default getRequestConfig(async () => {
     const store = await cookies()
     const raw = store.get('NEXT_LOCALE')?.value
     const locale = SUPPORTED_LOCALES.includes(raw as any) ? (raw as string) : 'en'
     return {
       locale,
       messages: (await import(`../messages/${locale}.json`)).default,
     }
   })
   ```

3. **Wire the plugin in `next.config.js`**: wrap the existing config —
   ```js
   const createNextIntlPlugin = require('next-intl/plugin')
   const withNextIntl = createNextIntlPlugin('./src/i18n/request.ts')
   // ... existing nextConfig unchanged ...
   module.exports = withNextIntl(nextConfig)
   ```
   Keep `headers()` / `rewrites()` / `typedRoutes` exactly as they are.

4. **Create message catalogs** `frontend/src/messages/en.json` and `ar.json`. Namespaces for phase 1: `nav` (all 15 labels from `navigation.ts` + `Admin`, `Users`, `Environment`, `Schedule Workers`, `Holidays`, group labels `Dashboards`/`Datasources`/`Tools`/`Profile`), `shell` (`about`, `changePassword`, `logout`, `hideSidebar`, `showSidebar`, `whatsNew`, `gotIt`, `backend`, `frontend`, `updatedToLatest`, `loading`, `user`), `login` (title, email/password labels+placeholders, submit, error strings from `src/app/login/page.tsx`), `common` (`save`, `cancel`, `delete`, `close`, `search` — seed for later phases). Key convention: camelCase, message keyed by meaning not English text. `en.json` values = current literal strings; `ar.json` = Arabic translations (translator or best-effort, marked for review).

5. **Update `src/app/layout.tsx`**:
   - Make `RootLayout` async; `import { getLocale, getMessages } from 'next-intl/server'` and `NextIntlClientProvider` from `next-intl`.
   - Add `const plexArabic = IBM_Plex_Sans_Arabic({ subsets: ['arabic', 'latin'], weight: ['400','500','600','700'] })` beside the existing `inter` const (line 16).
   - Line 63: `<html lang={locale} dir={locale === 'ar' ? 'rtl' : 'ltr'} className="h-full">`; body font class: `locale === 'ar' ? plexArabic.className : inter.className`.
   - Wrap the existing provider tree (outside `EnvironmentProvider`, inside `Suspense` is fine) in `<NextIntlClientProvider messages={messages}>`. All shell components are `"use client"`, so they need the client provider.
   - Translate the `Loading…` fallback at line 65 only if trivial; a hardcoded ellipsis fallback outside the provider is acceptable — leave it.

6. **Language switcher** — add to `src/components/shell/Navbar.tsx` next to `<ThemeToggle />` (line 141). Inline component (no new file needed unless reused):
   ```tsx
   const locale = useLocale() // from 'next-intl'
   const switchLocale = (l: string) => {
     document.cookie = `NEXT_LOCALE=${l}; path=/; max-age=31536000; samesite=lax`
     window.location.reload() // full reload re-renders html lang/dir server-side
   }
   ```
   Render a small two-option toggle (`EN` / `ع`), styled like the existing dark-variant buttons at Navbar.tsx:122-139. Hard reload is deliberate — avoids server-action plumbing for a rare action.

7. **Externalize shell strings** (translate at render; keep stable keys):
   - `src/config/navigation.ts` — NO structural change. `label` stays the stable English key (it drives `iconFor`, `badgeFor`, `groupHome`, `groupLabelOverride`, `routes`).
   - `src/components/shell/Sidebar.tsx` — in `Item` (line 77) render `t(`nav.${labelKey}`)` instead of `it.label`; since keys are camelCase and labels are English phrases, add a small `labelToKey` map or use the label itself as the message key (next-intl allows arbitrary keys — simplest: use the English label verbatim as the key in the `nav` namespace, e.g. `"My Dashboards": "لوحاتي"`). Use `useTranslations('nav')` / `useTranslations('shell')`. Also translate group headers (line 214), Admin group items (110-116), popover links (246-260), `title` attributes (93/95).
   - `src/components/shell/Navbar.tsx` — translate breadcrumb labels (`t('nav.' + label)` with fallback to raw label for path-segment fallbacks at line 75-78: wrap in try or use `t.has()`), aria-labels (94, 125, 133).
   - `src/app/(app)/layout.tsx` — `useTranslations('shell')` for lines 156, 157, 162, 168, 173, 186, 198.
   - `src/app/login/page.tsx` — externalize form labels/placeholders/buttons/errors to `login.*`.

8. **RTL logical-property pass — shell only**:
   - `Sidebar.tsx:123` `border-r` → `border-e`.
   - `Sidebar.tsx:66` `pl-6 pr-3` → `ps-6 pe-3`.
   - `Sidebar.tsx:79` `ml-auto` → `ms-auto`.
   - `Sidebar.tsx:193` `-translate-x-full` → `-translate-x-full rtl:translate-x-full` (keep `translate-x-0` for visible state).
   - `Sidebar.tsx:233` `text-left` → `text-start`.
   - `Navbar.tsx:107` chevron: add `rtl:rotate-180` to `RiArrowRightSLine`.
   - `src/app/(app)/layout.tsx:142` and `:153` `right-6` → `end-6`.
   - Grep both shell files + `(app)/layout.tsx` for any remaining `\b(ml|mr|pl|pr|left|right)-` and convert.
9. **Document the repo-wide follow-up** in the codebase, not prose: add a one-line note to `frontend/src/messages/en.json` sibling README is overkill — instead leave this spec's Out of Scope as the record. Counted debt: ~560 physical directional classes outside the shell (see Current State). Mechanical conversion table for the follow-up spec: `ml-→ms-`, `mr-→me-`, `pl-→ps-`, `pr-→pe-`, `left-→start-`, `right-→end-`, `text-left→text-start`, `text-right→text-end`, `rounded-l→rounded-s`, `rounded-r→rounded-e`, `space-x-N` → `gap-N` with flex (3 files: `app/(app)/datasources/sources/SourcesPageClient.tsx`, `app/(app)/datasources/sources/page.tsx`, `components/dashboards/DashboardCard.tsx`).

10. **Build + verify** (see Verification). Watch for: `getLocale()` requires the request config from step 2 to be found — path in `createNextIntlPlugin` must match exactly.

## Files to Modify

- `frontend/package.json` — add `next-intl` dependency.
- `frontend/next.config.js` — wrap config with `createNextIntlPlugin('./src/i18n/request.ts')`.
- `frontend/src/i18n/request.ts` — NEW: cookie-based locale resolution + message loading.
- `frontend/src/messages/en.json` — NEW: English catalog (nav/shell/login/common).
- `frontend/src/messages/ar.json` — NEW: Arabic catalog, same keys.
- `frontend/src/app/layout.tsx` — async RootLayout, dynamic `lang`/`dir`, Arabic font, `NextIntlClientProvider`.
- `frontend/src/components/shell/Navbar.tsx` — language switcher, translated breadcrumbs/aria-labels, RTL chevron.
- `frontend/src/components/shell/Sidebar.tsx` — translated labels at render, logical properties, RTL slide animation.
- `frontend/src/app/(app)/layout.tsx` — translated changelog/notification strings, `end-6` positioning.
- `frontend/src/app/login/page.tsx` — externalized form strings.
- `frontend/src/config/navigation.ts` — unchanged structurally (labels remain stable keys); only touch if adding a comment noting labels are i18n keys.

## Acceptance Criteria

- [ ] `next-intl` installed; `npm run build` passes with the plugin wired.
- [ ] Default (no cookie) renders `<html lang="en" dir="ltr">`; with `NEXT_LOCALE=ar` cookie renders `<html lang="ar" dir="rtl">` — server-rendered, no client flash.
- [ ] Navbar shows an EN/ع switcher; clicking it sets the cookie and reloads into the other locale.
- [ ] In Arabic: sidebar labels, group headers, breadcrumbs, popover links (About/Change Password/Logout), changelog card, and login page render in Arabic; Arabic text uses IBM Plex Sans Arabic.
- [ ] In RTL: sidebar sits on the right with its border on the correct (inner) side, hide-animation slides right, badge sits at the row end, breadcrumb chevrons point left, notification/changelog toasts anchor to the visual bottom-left (`end-6`).
- [ ] `iconFor`/`badgeFor` switches in `Sidebar.tsx` and `groupHome`/`groupLabelOverride` in `Navbar.tsx` still work in both locales (icons + badges appear on translated items).
- [ ] English UI is pixel-identical to before (no regressions from class conversions in LTR — logical properties resolve identically in LTR).
- [ ] Untranslated pages (dashboards, builder, etc.) still render fine in `ar` locale (English strings, RTL frame) — no crashes from missing message keys.

## Verification

```bash
cd /Users/mohammed/Documents/Bayan/frontend
npm install next-intl
npm run build                                  # must pass

# No stray physical classes left in shell files:
grep -nE '\b(ml|mr|pl|pr|left|right|text-left|text-right|border-r|border-l)\b|-(ml|mr|pl|pr)-' \
  src/components/shell/Sidebar.tsx src/components/shell/Navbar.tsx "src/app/(app)/layout.tsx" \
  | grep -v rtl: || echo OK

# Catalogs have identical key sets:
node -e "const a=Object.keys(require('./src/messages/en.json')),b=Object.keys(require('./src/messages/ar.json'));console.log(JSON.stringify(a)===JSON.stringify(b)?'OK':'KEY MISMATCH')"
```

Manual (dev server on :3000):
1. `npm run dev`; load `/home` → verify `<html lang="en" dir="ltr">` in devtools.
2. Click the ع switcher → page reloads; verify `<html lang="ar" dir="rtl">`, sidebar on right, Arabic nav labels, Arabic font.
3. Navigate Home → My Dashboards → Alerts: breadcrumbs Arabic, chevrons mirrored, badges render at row end.
4. Toggle sidebar hide/show in RTL → slides toward the right edge.
5. `/login` in `ar` → form fully Arabic.
6. Switch back to EN → UI identical to pre-change (compare against screenshot taken before starting).
7. Hard-refresh in `ar` → no flash of LTR/English shell (locale is server-resolved).

## Out of Scope

- Repo-wide logical-property conversion (~560 occurrences outside shell) — follow-up spec; conversion table provided in step 9.
- Translating dashboards/builder/datasources/alerts/widgets page bodies (36 builder + 20 widget components) — incremental follow-ups per area, reusing the `common` namespace.
- URL-based locale routing (`/ar/...`), `hreflang`, locale-aware `generateMetadata` — cookie mode is sufficient for an authenticated BI tool.
- RTL for embedded chart libraries (ECharts, Plotly, AG Grid, Tremor) and Quill editor.
- Backend-originated strings (alert messages, notification text, API errors) — server-side i18n is a separate backend spec.
- Locale-aware number/date formatting in widgets (use `next-intl`'s `useFormatter` when those areas are translated).
- Persisting locale per-user in the backend (cookie is per-browser; add a user-profile field later if needed).
