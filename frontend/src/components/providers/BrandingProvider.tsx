"use client"

import { useEffect, useRef } from 'react'
import type { ReactNode } from 'react'
import { Api } from '@/lib/api'

type Palette = Record<string, string>

export default function BrandingProvider({ children }: { children?: ReactNode }) {
  const paletteRef = useRef<Palette | null>(null)

  useEffect(() => {
    let cancelled = false

    const applyPalette = (palette: Palette | null, mode: 'light' | 'dark') => {
      const root = document.documentElement
      if (!palette) return
      if (mode === 'dark') {
        // Clear overrides so .dark tokens in globals.css can take effect
        Object.keys(palette).forEach((k) => root.style.removeProperty(`--${k}`))
      } else {
        Object.entries(palette).forEach(([k, v]) => root.style.setProperty(`--${k}`, String(v)))
      }
    }

    async function run() {
      try {
        const b = await Api.getBranding()
        if (cancelled) return
        paletteRef.current = (b.palette || {}) as Palette
        const isDark = document.documentElement.classList.contains('dark')
        applyPalette(paletteRef.current, isDark ? 'dark' : 'light')
      } catch {
        // ignore branding errors for now
      }
    }
    void run()

    // Respond to theme changes
    const onTheme = (e: Event) => {
      const detail = (e as CustomEvent).detail as 'light' | 'dark' | undefined
      applyPalette(paletteRef.current, detail === 'dark' ? 'dark' : 'light')
    }
    window.addEventListener('themechange', onTheme as EventListener)
    return () => {
      cancelled = true
      window.removeEventListener('themechange', onTheme as EventListener)
    }
  }, [])

  return <>{children}</>
}
