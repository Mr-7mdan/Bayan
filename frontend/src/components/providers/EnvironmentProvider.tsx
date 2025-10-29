"use client"

import React, { createContext, useContext, useEffect, useMemo, useState } from 'react'
import { usePathname } from 'next/navigation'
import { Api } from '@/lib/api'

export type WeekStart = 'mon' | 'sun' | 'sat'

export type EnvironmentSettings = {
  weekStart: WeekStart
  publicDomain?: string
  aiProvider?: 'gemini' | 'openai' | 'mistral' | 'anthropic' | 'openrouter'
  aiModel?: string
  aiApiKey?: string
  aiBaseUrl?: string
  orgName?: string
  orgLogoLight?: string
  orgLogoDark?: string
  favicon?: string
}

type EnvContextType = {
  env: EnvironmentSettings
  setEnv: (patch: Partial<EnvironmentSettings>) => void
}

const DEFAULTS: EnvironmentSettings = {
  weekStart: 'mon',
  publicDomain: (process.env.NEXT_PUBLIC_PUBLIC_DOMAIN || '').replace(/\/$/, ''),
  aiProvider: 'gemini',
  aiModel: 'gemini-1.5-flash',
  aiApiKey: '',
  aiBaseUrl: '',
  orgName: '',
  orgLogoLight: '',
  orgLogoDark: '',
  favicon: '',
}
const STORAGE_KEY = 'app.environment.settings'

const EnvContext = createContext<EnvContextType | undefined>(undefined)

export default function EnvironmentProvider({ children }: { children: React.ReactNode }) {
  const [env, setEnvState] = useState<EnvironmentSettings>(DEFAULTS)
  const [rl, setRl] = useState<{ show: boolean; text: string }>(() => ({ show: false, text: '' }))
  const pathname = usePathname()
  const [brandingReady, setBrandingReady] = useState(false)

  // Load from localStorage on mount
  useEffect(() => {
    try {
      if (typeof window === 'undefined') return
      const raw = window.localStorage.getItem(STORAGE_KEY)
      if (raw) {
        const parsed = JSON.parse(raw)
        if (parsed && typeof parsed === 'object') {
          setEnvState((prev) => ({ ...prev, ...parsed }))
        }
      }
    } catch {}
  }, [])

  // Persist to localStorage on change
  useEffect(() => {
    try {
      if (typeof window === 'undefined') return
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(env))
    } catch {}
  }, [env])

  useEffect(() => {
    ;(async () => {
      try {
        const b = await Api.getBranding()
        const patch: Partial<EnvironmentSettings> = {}
        if ((b.orgName || '').trim()) patch.orgName = b.orgName
        if ((b.logoLight || '').trim()) patch.orgLogoLight = b.logoLight
        if ((b.logoDark || '').trim()) patch.orgLogoDark = b.logoDark
        if ((b.favicon || '').trim()) patch.favicon = b.favicon
        if (Object.keys(patch).length) setEnvState((prev) => ({ ...prev, ...patch }))
      } catch {}
      finally { setBrandingReady(true) }
    })()
  }, [])

  // Apply dynamic document title and favicon from branding settings
  useEffect(() => {
    try {
      if (typeof document === 'undefined') return
      if (!brandingReady) return
      const base = 'Bayan'
      const org = (env.orgName || '').trim()
      const combined = org ? `${org} · ${base}` : base
      if (org) {
        const current = (document.title || base).trim()
        let nextTitle = current
        if (current === base) nextTitle = combined
        else if (/\s·\sBayan$/.test(current)) nextTitle = current.replace(/\s·\sBayan$/, ` · ${org} · ${base}`)
        else if (/Bayan$/.test(current)) nextTitle = current.replace(/Bayan$/, `${org} · ${base}`)
        else if (!current.includes(org)) nextTitle = `${current} · ${combined}`
        document.title = nextTitle
      }
      const href = (env.favicon || '').trim()
      if (href) {
        const ensureIcon = (rel: string) => {
          let link = document.querySelector(`link[rel="${rel}"]`) as HTMLLinkElement | null
          if (!link) { link = document.createElement('link'); link.rel = rel; document.head.appendChild(link) }
          link.href = href
        }
        ensureIcon('icon')
        ensureIcon('shortcut icon')
      }

      const ensureMetaName = (name: string, content: string) => {
        let m = document.querySelector(`meta[name="${name}"]`) as HTMLMetaElement | null
        if (!m) { m = document.createElement('meta'); m.setAttribute('name', name); document.head.appendChild(m) }
        m.setAttribute('content', content)
      }
      const ensureMetaProp = (prop: string, content: string) => {
        let m = document.querySelector(`meta[property="${prop}"]`) as HTMLMetaElement | null
        if (!m) { m = document.createElement('meta'); m.setAttribute('property', prop); document.head.appendChild(m) }
        m.setAttribute('content', content)
      }
      if (org) {
        ensureMetaName('application-name', combined)
        ensureMetaName('apple-mobile-web-app-title', combined)
        ensureMetaProp('og:site_name', combined)
      }
    } catch {}
  }, [env.orgName, env.favicon, pathname, brandingReady])

  useEffect(() => {
    const onRl = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { path?: string; retryAfter?: number }
        const secs = Math.max(0, Number(d?.retryAfter || 0))
        const t = secs > 0 ? `Rate limited. Retrying in ${secs}s…` : 'Rate limited. Retrying…'
        setRl({ show: true, text: t })
        setTimeout(() => { setRl({ show: false, text: '' }) }, Math.min(4000, Math.max(1500, (secs || 1) * 1000)))
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('rate-limit', onRl as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('rate-limit', onRl as EventListener) }
  }, [])

  const ctx = useMemo<EnvContextType>(() => ({
    env,
    setEnv: (patch: Partial<EnvironmentSettings>) => {
      setEnvState((prev) => ({ ...prev, ...patch }))
    },
  }), [env])

  return (
    <EnvContext.Provider value={ctx}>
      {children}
      {rl.show && (
        <div className="fixed bottom-4 right-4 z-[9999]">
          <div className="px-3 py-2 rounded-md border bg-card text-foreground text-xs shadow-md">
            {rl.text}
          </div>
        </div>
      )}
    </EnvContext.Provider>
  )
}

export function useEnvironment(): EnvContextType {
  const ctx = useContext(EnvContext)
  if (!ctx) throw new Error('useEnvironment must be used within EnvironmentProvider')
  return ctx
}
