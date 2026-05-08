"use client"

import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'

export type Theme = 'light' | 'dark' | 'system'

// Production dark variants — always available.
export type DarkVariantBuiltin = 'bluish' | 'blackish'
// Alpha (experimental) dark variants — only applied when alphaThemesEnabled.
// Each maps to a CSS file under `frontend/src/app/themes/`.
export type DarkVariantAlpha = 'core-interface' | 'platform-architecture'
export type DarkVariant = DarkVariantBuiltin | DarkVariantAlpha

// Light variants. The default (no attribute set) is the existing Bayan look.
// Alpha values map to a CSS file under `frontend/src/app/themes/`.
export type LightVariant = 'default' | 'nova-estate'

const ALPHA_DARK_VARIANTS: ReadonlyArray<DarkVariantAlpha> = ['core-interface', 'platform-architecture']
const ALPHA_LIGHT_VARIANTS: ReadonlyArray<Exclude<LightVariant, 'default'>> = ['nova-estate']

type Ctx = {
  theme: Theme
  resolved: 'light' | 'dark'
  setTheme: (t: Theme) => void
  darkVariant: DarkVariant
  setDarkVariant: (v: DarkVariant) => void
  lightVariant: LightVariant
  setLightVariant: (v: LightVariant) => void
  /** Master gate. When false, alpha variants resolve back to defaults. */
  alphaThemesEnabled: boolean
  setAlphaThemesEnabled: (on: boolean) => void
}

const ThemeContext = createContext<Ctx | null>(null)

function resolveTheme(pref: Theme): 'light' | 'dark' {
  if (pref === 'system') {
    if (typeof window !== 'undefined' && window.matchMedia) {
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
    }
    return 'light'
  }
  return pref
}

export function useTheme(): Ctx {
  const ctx = useContext(ThemeContext)
  if (!ctx) throw new Error('useTheme must be used within ThemeProvider')
  return ctx
}

export default function ThemeProvider({ children }: { children?: ReactNode }) {
  const initialTheme: Theme = (() => {
    if (typeof window === 'undefined') return 'system'
    try { return (localStorage.getItem('theme') as Theme) || 'system' } catch { return 'system' }
  })()
  const [theme, setThemeState] = useState<Theme>(initialTheme)
  const [resolved, setResolved] = useState<'light' | 'dark'>(() => resolveTheme(initialTheme))
  const initialVariant: DarkVariant = (() => {
    if (typeof window === 'undefined') return 'bluish'
    try { return ((localStorage.getItem('dark_variant') as DarkVariant) || 'bluish') } catch { return 'bluish' }
  })()
  const [darkVariant, setDarkVariantState] = useState<DarkVariant>(initialVariant)
  const initialLightVariant: LightVariant = (() => {
    if (typeof window === 'undefined') return 'default'
    try { return ((localStorage.getItem('light_variant') as LightVariant) || 'default') } catch { return 'default' }
  })()
  const [lightVariant, setLightVariantState] = useState<LightVariant>(initialLightVariant)
  const initialAlpha: boolean = (() => {
    if (typeof window === 'undefined') return false
    try { return localStorage.getItem('alpha_themes') === '1' } catch { return false }
  })()
  const [alphaThemesEnabled, setAlphaThemesEnabledState] = useState<boolean>(initialAlpha)

  // initial load handled synchronously via initialTheme to avoid flicker

  useEffect(() => {
    const apply = (value: Theme) => {
      const r = resolveTheme(value)
      setResolved(r)
      const root = document.documentElement
      // Always reset any inline overrides before applying new ones
      try {
        const VAR_KEYS = [
          '--foreground','--muted-foreground',
          '--background','--secondary',
          '--primary','--accent',
          '--surface-0','--surface-1','--surface-2','--surface-3',
          '--card','--card-foreground',
          '--popover','--popover-foreground',
          '--border','--input','--ring',
          '--primary-foreground','--secondary-foreground','--accent-foreground',
          '--topbar-bg','--topbar-fg','--header-accent',
          '--primary-deep'
        ]
        VAR_KEYS.forEach((k) => root.style.removeProperty(k))
      } catch {}
      if (r === 'dark') root.classList.add('dark')
      else root.classList.remove('dark')
      // Resolve effective variants: if alpha is OFF and the saved variant is
      // an alpha one, fall back to the default for that mode. This keeps the
      // master toggle authoritative — flipping alpha off instantly returns
      // to the production palette without losing the user's saved choice.
      const isAlphaDark = (ALPHA_DARK_VARIANTS as ReadonlyArray<string>).includes(darkVariant)
      const effDark: DarkVariant = (alphaThemesEnabled || !isAlphaDark) ? darkVariant : 'bluish'
      const isAlphaLight = (ALPHA_LIGHT_VARIANTS as ReadonlyArray<string>).includes(lightVariant)
      const effLight: LightVariant = (alphaThemesEnabled || !isAlphaLight) ? lightVariant : 'default'
      try {
        if (r === 'dark') {
          root.setAttribute('data-variant', effDark)
          root.removeAttribute('data-light-variant')
        } else {
          root.removeAttribute('data-variant')
          if (effLight !== 'default') root.setAttribute('data-light-variant', effLight)
          else root.removeAttribute('data-light-variant')
        }
      } catch {}
      // Apply stored overrides (custom palette) for this mode — only when no
      // alpha variant is active (alpha variants own the entire palette).
      try {
        const key = r === 'dark' ? 'theme_overrides_dark' : 'theme_overrides_light'
        const raw = localStorage.getItem(key)
        const hasVariantPalette = r === 'dark'
          ? (effDark !== 'bluish')
          : (effLight !== 'default')
        if (raw && !hasVariantPalette) {
          const obj = JSON.parse(raw) as Record<string, string>
          Object.entries(obj).forEach(([k, v]) => root.style.setProperty(k, v))
        }
      } catch {
        // ignore
      }
      // Notify listeners (e.g., BrandingProvider) so they can re-apply/remove inline palette
      try {
        window.dispatchEvent(new CustomEvent('themechange', { detail: r }))
      } catch {
        // noop
      }
    }

    apply(theme)

    // Watch system changes when in system mode
    if (theme === 'system' && typeof window !== 'undefined' && window.matchMedia) {
      const mq = window.matchMedia('(prefers-color-scheme: dark)')
      const handler = () => apply('system')
      mq.addEventListener?.('change', handler)
      return () => mq.removeEventListener?.('change', handler)
    }
  }, [theme, darkVariant, lightVariant, alphaThemesEnabled])

  // Shared "wipe inline overrides" routine — used by every setter that
  // changes which palette CSS file the cascade should pick from. Without
  // wiping, a previously-applied custom palette (or remnants from another
  // variant) leaks into the new variant.
  const wipeInlineVarOverrides = () => {
    try {
      const root = document.documentElement
      const VAR_KEYS = [
        '--foreground','--muted-foreground',
        '--background','--secondary',
        '--primary','--accent',
        '--surface-0','--surface-1','--surface-2','--surface-3',
        '--card','--card-foreground',
        '--popover','--popover-foreground',
        '--border','--input','--ring',
        '--primary-foreground','--secondary-foreground','--accent-foreground',
        '--topbar-bg','--topbar-fg','--header-accent',
        '--primary-deep'
      ]
      VAR_KEYS.forEach((k) => root.style.removeProperty(k))
    } catch {}
  }

  const value = useMemo<Ctx>(() => ({
    theme,
    resolved,
    setTheme: (t) => {
      setThemeState(t)
      try { localStorage.setItem('theme', t) } catch {}
    },
    darkVariant,
    setDarkVariant: (v) => {
      setDarkVariantState(v)
      try { localStorage.setItem('dark_variant', v) } catch {}
      try {
        const root = document.documentElement
        if (resolved === 'dark') root.setAttribute('data-variant', v)
        else root.removeAttribute('data-variant')
        // When picking an alpha variant, clear any custom-palette overrides
        // for that mode so the variant CSS file owns the cascade.
        try {
          if ((ALPHA_DARK_VARIANTS as ReadonlyArray<string>).includes(v) || v === 'bluish') {
            localStorage.removeItem('theme_overrides_dark')
          }
        } catch {}
        wipeInlineVarOverrides()
        try { window.dispatchEvent(new CustomEvent('themechange', { detail: resolved })) } catch {}
      } catch {}
    },
    lightVariant,
    setLightVariant: (v) => {
      setLightVariantState(v)
      try { localStorage.setItem('light_variant', v) } catch {}
      try {
        const root = document.documentElement
        if (resolved !== 'dark') {
          if (v !== 'default') root.setAttribute('data-light-variant', v)
          else root.removeAttribute('data-light-variant')
        }
        try {
          if (v !== 'default') localStorage.removeItem('theme_overrides_light')
        } catch {}
        wipeInlineVarOverrides()
        try { window.dispatchEvent(new CustomEvent('themechange', { detail: resolved })) } catch {}
      } catch {}
    },
    alphaThemesEnabled,
    setAlphaThemesEnabled: (on) => {
      setAlphaThemesEnabledState(on)
      try { localStorage.setItem('alpha_themes', on ? '1' : '0') } catch {}
      // The main effect re-runs because alphaThemesEnabled is in its dep
      // list — it'll re-evaluate effective variants and re-apply attributes.
    },
  }), [theme, resolved, darkVariant, lightVariant, alphaThemesEnabled])

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
}
