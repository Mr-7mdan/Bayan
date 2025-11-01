"use client"

import { Suspense, useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'next/navigation'
import KpiCard from '@/components/widgets/KpiCard'
import ChartCard from '@/components/widgets/ChartCard'
import TableCard from '@/components/widgets/TableCard'
import HeatmapCard from '@/components/widgets/HeatmapCard'
import { Api } from '@/lib/api'
import type { DashboardOut, RGLLayout } from '@/lib/api'
import type { WidgetConfig } from '@/types/widgets'
import BrandingProvider from '@/components/providers/BrandingProvider'
import ThemeProvider from '@/components/providers/ThemeProvider'

export const dynamic = 'force-dynamic'

function useQueryParam(name: string, def?: string): string | undefined {
  const sp = useSearchParams()
  const v = sp.get(name)
  return (v == null || v === '') ? def : v
}

export default function EmbedWidget() {
  const publicId = useQueryParam('publicId')
  const token = useQueryParam('token')
  const et = useQueryParam('et')
  const actorId = useQueryParam('actorId')
  const dashboardId = useQueryParam('dashboardId')
  const datasourceIdParam = useQueryParam('datasourceId')
  const widgetId = useQueryParam('widgetId') || ''
  const themeParam = (useQueryParam('theme', 'dark') || 'dark') as 'light'|'dark'
  const bgParam = (useQueryParam('bg') || '').toLowerCase()
  const snapParam = (useQueryParam('snap') || '').toLowerCase()
  const w = Math.max(200, Math.min(2000, Number(useQueryParam('w', '800')) || 800))
  const h = Math.max(120, Math.min(2000, Number(useQueryParam('h', '360')) || 360))
  const forceTransparent = bgParam === 'transparent'
  const isSnap = snapParam === '1' || snapParam === 'true'
  const controlsParam = (useQueryParam('controls', '') || '').toLowerCase()
  const controlsOff = controlsParam === 'off' || controlsParam === '0' || controlsParam === 'false'
  const ttlParam = Number(useQueryParam('ttl') || 0) || 0
  const tsParam = Number(useQueryParam('ts') || 0) || 0

  try { if (typeof window !== 'undefined') { localStorage.setItem('theme', themeParam) } } catch {}

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [dashboard, setDashboard] = useState<DashboardOut | null>(null)

  useEffect(() => {
    let mounted = true
    async function run() {
      try {
        setLoading(true)
        // TTL check (client-side hardening) applies only to public embeds
        if (publicId) {
          try {
            const nowSec = Math.floor(Date.now() / 1000)
            if (ttlParam > 0 && tsParam > 0 && (nowSec - tsParam) > ttlParam) {
              throw new Error('This embed link has expired')
            }
          } catch {}
          const res: DashboardOut = await Api.getDashboardPublic(publicId, token, { et: et || undefined })
          if (!mounted) return
          setDashboard(res)
          setError(null)
        } else if (dashboardId) {
          // Internal embed: allow server-side snapshots by dashboardId with actorId
          const res: DashboardOut = await Api.getDashboard(dashboardId, actorId || undefined)
          if (!mounted) return
          setDashboard(res)
          setError(null)
        } else {
          throw new Error('dashboardId or publicId is required for embedding')
        }
      } catch (e: any) {
        if (!mounted) return
        setError(e?.message || 'Failed to load')
      } finally {
        if (!mounted) return
        setLoading(false)
      }
    }
    void run(); return () => { mounted = false }
  }, [publicId, dashboardId, actorId, token, et, ttlParam, tsParam])

  // Apply theme class early
  useEffect(() => {
    const root = document.documentElement
    if (themeParam === 'dark') root.classList.add('dark'); else root.classList.remove('dark')
  }, [themeParam])

  const widgetCfg: WidgetConfig | null = useMemo(() => {
    if (!dashboard) return null
    const cfg = (dashboard.definition.widgets as Record<string, WidgetConfig>)[widgetId]
    if (!cfg) return null
    if (datasourceIdParam) {
      return { ...cfg, datasourceId: datasourceIdParam }
    }
    return cfg
  }, [dashboard, widgetId, datasourceIdParam])

  const [widgetReady, setWidgetReady] = useState(false)
  useEffect(() => {
    const handler = () => {
      if (!widgetReady) {
        try { ;(window as any).__WIDGET_DATA_READY__ = true } catch {}
        setWidgetReady(true)
      }
    }
    try { window.addEventListener('widget-data-ready', handler as any, { once: true } as any) } catch {}
    return () => { try { window.removeEventListener('widget-data-ready', handler as any) } catch {} }
  }, [widgetReady])
  useEffect(() => {
    if (widgetReady) {
      try { ;(window as any).__READY__ = true } catch {}
    }
  }, [widgetReady])
  useEffect(() => {
    if (widgetReady) {
      try { const el = document.getElementById('widget-root'); if (el) el.setAttribute('data-widget-ready','1') } catch {}
    }
  }, [widgetReady])
  useEffect(() => {
    const t = setTimeout(() => { try { ;(window as any).__READY__ = true } catch {} }, 6000)
    return () => clearTimeout(t)
  }, [widgetCfg?.id])

  // Snapshot fallback readiness: if the widget doesn't emit widget-data-ready (e.g., some charts),
  // wait until a canvas appears with a non-trivial size, then mark ready.
  useEffect(() => {
    if (!isSnap) return
    let fired = false
    const tryFire = () => {
      try {
        if (fired) return
        const c = document.querySelector('#widget-root canvas') as HTMLCanvasElement | null
        if (c && Math.max(c.width || 0, c.clientWidth || 0) > 8 && Math.max(c.height || 0, c.clientHeight || 0) > 8) {
          fired = true
          try { window.dispatchEvent(new CustomEvent('widget-data-ready')) } catch {}
        }
      } catch {}
    }
    const iv = setInterval(tryFire, 120)
    const to = setTimeout(tryFire, 3500)
    return () => { try { clearInterval(iv) } catch {}; try { clearTimeout(to) } catch {} }
  }, [isSnap, widgetCfg?.id])

  const content = useMemo(() => {
    if (loading) return <div className="text-xs text-muted-foreground">Loading…</div>
    if (error) return <div className="text-xs text-red-600">{error}</div>
    if (!widgetCfg) return <div className="text-xs text-muted-foreground">Widget not found</div>
    const cfg = widgetCfg
    const opts = (forceTransparent ? { ...(cfg.options || {}), cardFill: 'transparent' } : (cfg.options || undefined)) as any
    const transparentVars = forceTransparent ? {
      backgroundColor: 'transparent',
      ['--background' as any]: '0 0% 0% / 0%',
      ['--card' as any]: '0 0% 0% / 0%',
      ['--popover' as any]: '0 0% 0% / 0%',
      ['--surface-1' as any]: '0 0% 0% / 0%',
      ['--surface-2' as any]: '0 0% 0% / 0%',
      ['--surface-3' as any]: '0 0% 0% / 0%',
      ['--secondary' as any]: '0 0% 0% / 0%',
      ['--muted' as any]: '0 0% 0% / 0%',
    } as React.CSSProperties : undefined
    return (
      <div id="widget-root" data-snap={isSnap ? '1' : undefined} className={`rounded-md overflow-hidden ${forceTransparent ? 'bg-transparent border-0 shadow-none' : (cfg.options?.cardFill === 'transparent' ? 'bg-transparent border-0 shadow-none' : 'border bg-card')}`} style={{ width: w, height: h, ...(transparentVars||{}) }}>
        {forceTransparent && (
          <style jsx global>{`
            html, body, #__next { background: transparent !important; }
            #widget-root { background: transparent !important; }
            #widget-root .bg-card, #widget-root .bg-background { background-color: transparent !important; }
            #widget-root * { background: transparent !important; background-color: transparent !important; background-image: none !important; }
            #widget-root canvas { background: transparent !important; background-color: transparent !important; }
            #widget-root [style*="background: hsl(var(--card))"],
            #widget-root [style*="background-color: hsl(var(--card))"],
            #widget-root [style*="background: hsl(var(--background))"],
            #widget-root [style*="background-color: hsl(var(--background))"] { background: transparent !important; background-color: transparent !important; }
          `}</style>
        )}
        {controlsOff && (
          <style jsx global>{`
            #widget-root button,
            #widget-root [role="button"],
            #widget-root [role="menu"],
            #widget-root [role="listbox"],
            #widget-root input,
            #widget-root select,
            #widget-root textarea,
            #widget-root [contenteditable] { pointer-events: none !important; }
            #widget-root .filterbar-popover,
            #widget-root [aria-haspopup="menu"],
            #widget-root .tremor-Select-popover { display: none !important; }
          `}</style>
        )}
        <div className="p-3 h-full min-h-0 flex flex-col">
          <div className="flex-1 min-h-0">
            {cfg.type === 'kpi' && (
              <KpiCard
                title={cfg.title}
                sql={cfg.sql}
                datasourceId={cfg.datasourceId}
                options={opts}
                queryMode={cfg.queryMode as any}
                querySpec={cfg.querySpec as any}
                pivot={cfg.pivot as any}
                widgetId={cfg.id}
              />
            )}
            {cfg.type === 'chart' && (
              ((cfg as any).chartType === 'heatmap') ? (
                <HeatmapCard
                  title={cfg.title}
                  sql={cfg.sql}
                  datasourceId={cfg.datasourceId}
                  options={opts}
                  queryMode={cfg.queryMode as any}
                  querySpec={cfg.querySpec as any}
                  widgetId={cfg.id}
                />
              ) : (
                <ChartCard
                  title={cfg.title}
                  sql={cfg.sql}
                  datasourceId={cfg.datasourceId}
                  type={(cfg as any).chartType || 'line'}
                  options={opts}
                  queryMode={cfg.queryMode as any}
                  querySpec={cfg.querySpec as any}
                  customColumns={cfg.customColumns as any}
                  widgetId={cfg.id}
                  pivot={cfg.pivot as any}
                  layout="measure"
                  reservedTop={0}
                />
              )
            )}
            {cfg.type === 'table' && (
              <TableCard
                title={cfg.title}
                sql={cfg.sql}
                datasourceId={cfg.datasourceId}
                options={{ ...(opts || {}), table: { ...((opts as any)?.table || {}), pivotUI: false } }}
                queryMode={cfg.queryMode as any}
                querySpec={cfg.querySpec as any}
                widgetId={cfg.id}
                customColumns={cfg.customColumns as any}
                pivot={cfg.pivot as any}
              />
            )}
          </div>
          <div className="mt-2 flex items-center justify-center gap-2 text-[10px] text-[hsl(var(--foreground))] opacity-80">
            <img src="/logo.png" alt="Bayan" className="h-3 w-auto inline dark:hidden" />
            <img src="/logo-dark.png" alt="Bayan" className="h-3 w-auto hidden dark:inline" />
            <span>Powered by Bayan © {new Date().getFullYear()}</span>
          </div>
        </div>
      </div>
    )
  }, [loading, error, widgetCfg, w, h, forceTransparent])

  return (
    <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
      <ThemeProvider>
        <BrandingProvider>
          <div className={themeParam === 'dark' ? 'dark' : ''}>
            <div className={`min-h-screen ${forceTransparent ? '' : 'bg-background'} p-2 flex items-center justify-center`} style={{ width: w, height: h }}>
              {content}
            </div>
          </div>
        </BrandingProvider>
      </ThemeProvider>
    </Suspense>
  )
}
