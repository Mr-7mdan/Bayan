"use client"

import { createContext, useContext, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { usePathname, useRouter, useSearchParams } from 'next/navigation'

export type Filters = {
  startDate?: string
  endDate?: string
}

type Ctx = {
  filters: Filters
  setFilters: (f: Filters) => void
  reset: () => void
}

const FiltersContext = createContext<Ctx | null>(null)

export function useFilters(): Ctx {
  const ctx = useContext(FiltersContext)
  if (!ctx) throw new Error('useFilters must be used within FiltersProvider')
  return ctx
}

export default function FiltersProvider({ children }: { children?: ReactNode }) {
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()
  const [filters, setFiltersState] = useState<Filters>({})

  // Compute a storage scope per route: public page uses publicId; builder uses saved dashboardId
  const scopeKey = useMemo(() => {
    try {
      const segs = String(pathname || '').split('/').filter(Boolean)
      if (segs[0] === 'v' && segs[1]) return `gf_pub_${segs[1]}`
      const did = typeof window !== 'undefined' ? (localStorage.getItem('dashboardId') || 'default') : 'default'
      return `gf_dash_${did}`
    } catch {
      return 'gf_dash_default'
    }
  }, [pathname])

  // Only sync start/end with the URL on builder route
  const urlSyncEnabled = useMemo(() => {
    try {
      const segs = String(pathname || '').split('/').filter(Boolean)
      return segs[0] === 'builder'
    } catch { return false }
  }, [pathname])

  // Initialize from URL (preferred) else localStorage for this scope (only on enabled routes). Re-run on path change.
  useEffect(() => {
    if (!urlSyncEnabled) { setFiltersState({}); return }
    const start = searchParams.get('start') || undefined
    const end = searchParams.get('end') || undefined
    if (start || end) { setFiltersState({ startDate: start, endDate: end }); return }
    try {
      const raw = typeof window !== 'undefined' ? localStorage.getItem(scopeKey) : null
      if (raw) {
        const obj = JSON.parse(raw) as Filters
        setFiltersState({ startDate: obj.startDate, endDate: obj.endDate })
      } else {
        setFiltersState({})
      }
    } catch { setFiltersState({}) }
  }, [pathname, searchParams, scopeKey, urlSyncEnabled])
  // Persist to URL and localStorage when filters change
  useEffect(() => {
    // Build params from current location to avoid dependency on useSearchParams
    const params = new URLSearchParams(typeof window !== 'undefined' ? window.location.search : '')
    if (!urlSyncEnabled) {
      // Strip builder-only params on routes where URL sync is disabled
      params.delete('start')
      params.delete('end')
      const qs = params.toString()
      const nextUrl = `${pathname}${qs ? `?${qs}` : ''}`
      const curr = `${pathname}${typeof window !== 'undefined' && window.location.search ? window.location.search : ''}`
      if (nextUrl !== curr) router.replace(nextUrl as any)
      return
    }
    if (filters.startDate) params.set('start', filters.startDate)
    else params.delete('start')
    if (filters.endDate) params.set('end', filters.endDate)
    else params.delete('end')
    const qs = params.toString()
    const nextUrl = `${pathname}${qs ? `?${qs}` : ''}`
    const curr = `${pathname}${typeof window !== 'undefined' && window.location.search ? window.location.search : ''}`
    if (nextUrl !== curr) router.replace(nextUrl as any)
    try {
      if (typeof window !== 'undefined') {
        if (filters.startDate || filters.endDate) localStorage.setItem(scopeKey, JSON.stringify(filters))
        else localStorage.removeItem(scopeKey)
      }
    } catch {}
  }, [filters, pathname, router, scopeKey, urlSyncEnabled])

  const value = useMemo<Ctx>(() => ({
    filters,
    setFilters: (f) => setFiltersState(f || {}),
    reset: () => {
      try { if (typeof window !== 'undefined') localStorage.removeItem(scopeKey) } catch {}
      setFiltersState({})
    },
  }), [filters, scopeKey])

  return <FiltersContext.Provider value={value}>{children}</FiltersContext.Provider>
}
