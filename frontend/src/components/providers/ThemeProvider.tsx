"use client"

import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'

export type Theme = 'light' | 'dark' | 'system'
export type DarkVariant = 'bluish' | 'blackish'

type Ctx = { theme: Theme; resolved: 'light' | 'dark'; setTheme: (t: Theme) => void; darkVariant: DarkVariant; setDarkVariant: (v: DarkVariant) => void }

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
          '--topbar-bg','--topbar-fg','--header-accent'
        ]
        VAR_KEYS.forEach((k) => root.style.removeProperty(k))
      } catch {}
      if (r === 'dark') root.classList.add('dark')
      else root.classList.remove('dark')
      try { if (r === 'dark') root.setAttribute('data-variant', darkVariant); else root.removeAttribute('data-variant') } catch {}
      // Apply stored overrides (custom palette) for this mode
      try {
        const key = r === 'dark' ? 'theme_overrides_dark' : 'theme_overrides_light'
        const raw = localStorage.getItem(key)
        const hasDarkVariant = (r === 'dark') && !!darkVariant
        if (raw && !hasDarkVariant) {
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
  }, [theme, darkVariant])

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
        try { localStorage.removeItem('theme_overrides_dark') } catch {}
        const VAR_KEYS = [
          '--foreground','--muted-foreground',
          '--background','--secondary',
          '--primary','--accent',
          '--surface-0','--surface-1','--surface-2','--surface-3',
          '--card','--card-foreground',
          '--popover','--popover-foreground',
          '--border','--input','--ring',
          '--primary-foreground','--secondary-foreground','--accent-foreground',
          '--topbar-bg','--topbar-fg','--header-accent'
        ]
        VAR_KEYS.forEach((k) => root.style.removeProperty(k))
        try { window.dispatchEvent(new CustomEvent('themechange', { detail: resolved })) } catch {}
      } catch {}
    },
  }), [theme, resolved, darkVariant])

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
}
