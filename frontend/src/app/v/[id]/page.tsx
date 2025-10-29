"use client"

import { Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useParams, useSearchParams, usePathname, useRouter } from 'next/navigation'
import 'react-grid-layout/css/styles.css'
import 'react-resizable/css/styles.css'
import GridLayout from 'react-grid-layout'
import { Api } from '@/lib/api'
import type { RGLLayout } from '@/lib/api'
import type { WidgetConfig } from '@/types/widgets'
import KpiCard from '@/components/widgets/KpiCard'
import ChartCard from '@/components/widgets/ChartCard'
import TableCard from '@/components/widgets/TableCard'
import GlobalFiltersBar from '@/components/builder/GlobalFiltersBar'
import { useAuth } from '@/components/providers/AuthProvider'
import { RiBookmarkLine, RiBookmarkFill } from '@remixicon/react'
import ThemeToggle from '@/components/ui/ThemeToggle'
import { useTheme } from '@/components/providers/ThemeProvider'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'

export const dynamic = 'force-dynamic'

export default function ViewDashboard() {
  const params = useParams<{ id: string }>()
  const id = params?.id
  const search = useSearchParams()
  const pathname = usePathname()
  const router = useRouter()
  const token = search.get('token') || undefined
  const { user } = useAuth()
  const { env } = useEnvironment()
  const { resolved, darkVariant, setDarkVariant } = useTheme()
  const applyBluish = () => setDarkVariant('bluish')
  const applyBlackish = () => setDarkVariant('blackish')
  const [mounted, setMounted] = useState(false)
  useEffect(() => { setMounted(true) }, [])

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [layout, setLayout] = useState<RGLLayout[]>([])
  const [widgets, setWidgets] = useState<Record<string, WidgetConfig>>({})
  const [dashboardId, setDashboardId] = useState<string | null>(null)
  const [dashboardName, setDashboardName] = useState<string>('')
  const [dashOptions, setDashOptions] = useState<Record<string, any>>({})
  const [needToken, setNeedToken] = useState(false)
  const [tokenInput, setTokenInput] = useState('')
  // For measurement-based reservedTop
  const itemRefs = useRef<Record<string, HTMLDivElement | null>>({})
  function setItemRef(key: string) { return (el: HTMLDivElement | null) => { itemRefs.current[key] = el } }
  const [titleHeights, setTitleHeights] = useState<Record<string, number>>({})
  const [collectionState, setCollectionState] = useState<{
    inCollection: boolean
    collectionId?: string
    loading: boolean
    message?: string
    error?: string
  }>({ inCollection: false, loading: false })

  // Measure canvas width like builder
  const canvasRef = useRef<HTMLDivElement | null>(null)
  const [canvasW, setCanvasW] = useState<number>(980)
  useEffect(() => {
    if (!canvasRef.current) return
    const el = canvasRef.current
    const ro = new ResizeObserver(() => {
      const rect = el.getBoundingClientRect()
      setCanvasW(Math.max(480, Math.floor(rect.width)))
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  useEffect(() => {
    setCollectionState({ inCollection: false, loading: false })
    setDashboardId(null)
  }, [id])

  // Live metrics: dashboards open/close pings (public)
  const sessionIdRef = useRef<string>('')
  useEffect(() => {
    try {
      let sid = ''
      if (typeof window !== 'undefined') sid = localStorage.getItem('dash_session_id') || ''
      if (!sid) {
        try { sid = crypto.randomUUID() } catch { sid = Math.random().toString(36).slice(2) }
        try { if (typeof window !== 'undefined') localStorage.setItem('dash_session_id', sid) } catch {}
      }
      sessionIdRef.current = sid
    } catch { sessionIdRef.current = Math.random().toString(36).slice(2) }
  }, [])
  useEffect(() => {
    if (!dashboardId) return
    const sid = sessionIdRef.current || 'anon'
    let cancelled = false
    ;(async () => { try { await Api.dashboardsOpen('public', dashboardId, sid) } catch {} })()
    const timer = window.setInterval(() => { try { void Api.dashboardsOpen('public', dashboardId, sid) } catch {} }, 60000)
    const onUnload = () => { try { void Api.dashboardsClose('public', dashboardId, sid) } catch {} }
    if (typeof window !== 'undefined') window.addEventListener('beforeunload', onUnload)
    return () => {
      try { window.clearInterval(timer) } catch {}
      if (typeof window !== 'undefined') window.removeEventListener('beforeunload', onUnload)
      if (!cancelled) { try { void Api.dashboardsClose('public', dashboardId, sid) } catch {} }
    }
  }, [dashboardId])
  useEffect(() => {
    const measure = () => {
      const next: Record<string, number> = {}
      Object.entries(itemRefs.current).forEach(([id, el]) => {
        const t = el?.querySelector('.widget-title') as HTMLElement | null
        if (!t) { next[id] = 0; return }
        const cs = getComputedStyle(t)
        const mt = parseFloat(cs.marginTop || '0') || 0
        const mb = parseFloat(cs.marginBottom || '0') || 0
        next[id] = (t.offsetHeight || 0) + mt + mb
      })
      setTitleHeights(next)
    }
    if (typeof window !== 'undefined') requestAnimationFrame(measure)
    const ros: ResizeObserver[] = []
    Object.entries(itemRefs.current).forEach(([id, el]) => {
      const t = el?.querySelector('.widget-title') as HTMLElement | null
      if (t) { const ro = new ResizeObserver(measure); ro.observe(t); ros.push(ro) }
    })
    window.addEventListener('resize', measure)
    return () => { ros.forEach((ro) => { try { ro.disconnect() } catch {} }); window.removeEventListener('resize', measure) }
  }, [layout, widgets])

  useEffect(() => {
    let mounted = true
    async function run() {
      if (!id) return
      try {
        setLoading(true)
        const res = await Api.getDashboardPublic(String(id), token)
        if (!mounted) return
        setLayout(res.definition.layout)
        setWidgets(res.definition.widgets as Record<string, WidgetConfig>)
        setDashOptions(((res.definition as any).options || {}) as Record<string, any>)
        setDashboardId(res.id)
        setDashboardName(res.name || '')
        setError(null)
      } catch (e: any) {
        if (!mounted) return
        const msg = e?.message || 'Failed to load dashboard'
        setError(msg)
        setNeedToken(!!msg && msg.includes('HTTP 401'))
      } finally {
        if (!mounted) return
        setLoading(false)
      }
    }
    void run()
    return () => {
      mounted = false
    }
  }, [id, token])

  // Reflect collection membership on initial load when user and dashboard are known
  useEffect(() => {
    let active = true
    const check = async () => {
      try {
        if (!user?.id || !dashboardId) return
        const items = await Api.listCollectionItems(user.id)
        if (!active) return
        const match = (items || []).find((it) => it.dashboardId === dashboardId)
        if (match) {
          setCollectionState((prev) => ({ ...prev, inCollection: true, collectionId: match.collectionId }))
        }
      } catch {
        // ignore membership fetch errors
      }
    }
    void check()
    return () => { active = false }
  }, [user?.id, dashboardId])

  // Expose debug info for console inspection
  useEffect(() => {
    try {
      if (typeof window !== 'undefined') {
        const m: Record<string, number> = { sm: 24, md: 18, lg: 12, xl: 8 }
        ;(window as any).__dashOptions = dashOptions
        ;(window as any).__layout = layout
        ;(window as any).__gridSize = (dashOptions as any)?.gridSize
        ;(window as any).__impliedCols = Array.isArray(layout) ? layout.reduce((a, it) => Math.max(a, Number(it.x || 0) + Number(it.w || 0)), 0) : 0
        ;(window as any).__cols = m[String((dashOptions as any)?.gridSize || 'lg')] || 12
      }
    } catch {}
  }, [dashOptions, layout])

  const handleAddToCollection = useCallback(async () => {
    if (!user?.id) {
      setCollectionState((prev) => ({ ...prev, error: 'Sign in to save dashboards to a collection.' }))
      return
    }
    if (!dashboardId) {
      setCollectionState((prev) => ({ ...prev, error: 'Dashboard not ready yet. Try again once loaded.' }))
      return
    }
    setCollectionState((prev) => ({ ...prev, loading: true, error: undefined, message: undefined }))
    try {
      const res = await Api.addToCollection(user.id, { userId: user.id, dashboardId })
      setCollectionState({
        inCollection: true,
        collectionId: res.collectionId,
        loading: false,
        message: res.added ? 'Dashboard added to your collection.' : 'Dashboard already exists in your collection.',
      })
      if (typeof window !== 'undefined') {
        window.dispatchEvent(new CustomEvent('sidebar-counts-refresh'))
      }
    } catch (e: any) {
      setCollectionState((prev) => ({
        ...prev,
        loading: false,
        error: e?.message || 'Failed to add to collection.',
      }))
    }
  }, [user?.id, dashboardId])

  const handleRemoveFromCollection = useCallback(async () => {
    if (!user?.id) {
      setCollectionState((prev) => ({ ...prev, error: 'Sign in to manage your collections.' }))
      return
    }
    if (!dashboardId || !collectionState.collectionId) {
      setCollectionState((prev) => ({ ...prev, error: 'Collection not found. Try adding again.' }))
      return
    }
    setCollectionState((prev) => ({ ...prev, loading: true, error: undefined, message: undefined }))
    try {
      await Api.removeFromCollection(user.id, collectionState.collectionId, dashboardId)
      setCollectionState({ inCollection: false, loading: false, message: 'Dashboard removed from your collection.' })
      if (typeof window !== 'undefined') {
        window.dispatchEvent(new CustomEvent('sidebar-counts-refresh'))
      }
    } catch (e: any) {
      setCollectionState((prev) => ({
        ...prev,
        loading: false,
        error: e?.message || 'Failed to remove from collection.',
      }))
    }
  }, [user?.id, dashboardId, collectionState.collectionId])

  const content = useMemo(() => {
    if (loading) return <div className="text-sm text-muted-foreground">Loading…</div>
    if (error) {
      if (needToken) {
        return (
          <div className="space-y-2">
            <div className="text-sm text-muted-foreground">This link is protected. Enter the access token to view.</div>
            <div className="flex items-center gap-2">
              <input
                type="text"
                className="px-2 py-1 rounded-md border bg-background text-sm"
                placeholder="Enter token"
                value={tokenInput}
                onChange={(e) => setTokenInput(e.target.value)}
              />
              <button
                className="text-xs px-3 py-1.5 rounded-md border hover:bg-muted"
                onClick={() => {
                  if (typeof window === 'undefined') return
                  const params = new URLSearchParams(window.location.search)
                  if (tokenInput) params.set('token', tokenInput)
                  else params.delete('token')
                  const qs = params.toString()
                  router.replace((`${pathname}${qs ? `?${qs}` : ''}`) as any)
                }}
              >
                Unlock
              </button>
            </div>
          </div>
        )
      }
      return <div className="text-sm text-red-600">{error}</div>
    }
    if (!layout.length) return <div className="text-sm text-muted-foreground">No layout</div>

    const showFilters = (dashOptions.publicShowFilters ?? true) === true
    const lockFilters = (dashOptions.publicLockFilters ?? false) === true
    const cols = (() => {
      const m: Record<string, number> = { sm: 24, md: 18, lg: 12, xl: 8 }
      const gs = String((dashOptions as any)?.gridSize || '').trim()
      if (gs && m[gs]) return m[gs]
      try {
        const implied = (layout || []).reduce((acc, it) => Math.max(acc, Number(it.x || 0) + Number(it.w || 0)), 0)
        if (implied && implied > 0) return implied
      } catch {}
      return m['lg'] || 12
    })()
    // Match builder runtime: clamp to cols and apply per-card constraints (e.g., spacer minW)
    const effectiveLayout: RGLLayout[] = (() => {
      try {
        return (layout || []).map((it) => {
          const cfg = widgets[it.i]
          let minW: number | undefined
          if (cfg?.type === 'spacer') minW = (cfg.options as any)?.spacer?.minW ?? 2
          const w = Math.min(it.w, cols)
          const x = Math.min(it.x, Math.max(0, cols - w))
          return { ...it, w, x, ...(minW != null ? { minW } : {}) }
        }) as RGLLayout[]
      } catch { return layout }
    })()
    const gridWidth = (() => {
      const mode = String((dashOptions as any)?.gridCanvasMode || 'auto')
      const fixed = Number((dashOptions as any)?.gridCanvasWidthPx || 0)
      return (mode === 'fixed' && fixed > 0) ? fixed : canvasW
    })()
    return (
      <section className="min-h-[60vh] rounded-lg border shadow-card p-3 bg-transparent overflow-auto" ref={canvasRef}>
        <div className="min-w-[720px] space-y-2">
          {showFilters && (
            <div className="rounded-md border bg-card p-2">
              <div className="flex items-center justify-between gap-3">
                <GlobalFiltersBar disabled={lockFilters} />
              </div>
            </div>
          )}
          <GridLayout
            className="layout"
            layout={effectiveLayout}
            cols={cols}
            rowHeight={24}
            width={gridWidth}
            margin={[10, 10]}
            containerPadding={[10, 10]}
            isResizable={false}
            isDraggable={false}
          >
        {effectiveLayout.map((item) => {
          const cfg = widgets[item.i]
          if (!cfg) return (
            <div key={item.i} className="rounded-md border bg-card overflow-hidden" />
          )
          return (
            <div
              key={item.i}
              className={`h-full min-h-0 flex flex-col rounded-md overflow-hidden ${cfg.options?.cardFill === 'transparent' ? 'bg-transparent border-0 shadow-none' : 'border bg-card'}`}
              ref={setItemRef(item.i)}
            >
              {(cfg.options?.showCardHeader !== false) && (
                <div className="widget-title flex-none px-3 py-2 bg-secondary/60 border-b text-sm font-medium border-l-2 border-l-[hsl(var(--header-accent))]">
                  {cfg.title}
                </div>
              )}
              <div className="p-3 flex-1 min-h-0 flex flex-col">
                {cfg.type === 'kpi' && (
                  <KpiCard
                    title={cfg.title}
                    sql={cfg.sql}
                    datasourceId={cfg.datasourceId}
                    options={cfg.options}
                    queryMode={cfg.queryMode as any}
                    querySpec={cfg.querySpec as any}
                    pivot={cfg.pivot as any}
                    widgetId={cfg.id}
                  />
                )}
                {cfg.type === 'chart' && (
                  <ChartCard
                    title={cfg.title}
                    sql={cfg.sql}
                    datasourceId={cfg.datasourceId}
                    type={(cfg as any).chartType || 'line'}
                    options={cfg.options}
                    queryMode={cfg.queryMode as any}
                    querySpec={cfg.querySpec as any}
                    customColumns={cfg.customColumns as any}
                    widgetId={cfg.id}
                    pivot={cfg.pivot as any}
                    layout="measure"
                    reservedTop={titleHeights[item.i] ?? 0}
                  />
                )}
                {cfg.type === 'table' && (
                  <TableCard
                    title={cfg.title}
                    sql={cfg.sql}
                    datasourceId={cfg.datasourceId}
                    options={{
                      ...(cfg.options || {}),
                      table: { ...(cfg.options?.table || {}), pivotUI: false },
                    }}
                    queryMode={cfg.queryMode as any}
                    querySpec={cfg.querySpec as any}
                    widgetId={cfg.id}
                    customColumns={cfg.customColumns as any}
                    pivot={cfg.pivot as any}
                  />
                )}
              </div>
            </div>
          )
        })}
          </GridLayout>
        </div>
      </section>
    )
  }, [loading, error, layout, widgets, needToken, tokenInput, router, pathname, titleHeights, dashOptions.publicShowFilters, dashOptions.publicLockFilters, (dashOptions as any).gridSize, canvasW])

  return (
    <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
    <div className="builder-root min-h-screen bg-background">
      <header className="sticky top-0 z-40 border-b border-[hsl(var(--border))] bg-[hsl(var(--topbar-bg))] text-[hsl(var(--topbar-fg))]">
        <div className="mx-auto w-full px-6 py-3 flex items-center gap-4 justify-between">
          <div className="flex items-center gap-3">
            <img src={(env.orgLogoLight || '/logo.svg') as any} alt={(env.orgName || 'Bayan')} className="h-7 w-auto block dark:hidden" />
            <img src={(env.orgLogoDark || '/logo-dark.svg') as any} alt={(env.orgName || 'Bayan')} className="h-7 w-auto hidden dark:block" />
            <span className="font-semibold truncate text-left" title={dashboardName || 'Dashboard'}>
              {dashboardName || 'Dashboard'}
            </span>
            {collectionState.message && (
              <div className="text-xs text-emerald-600 bg-emerald-50 border border-emerald-200 rounded-md px-2 py-1">
                {collectionState.message}
              </div>
            )}
            {collectionState.error && (
              <div className="text-xs text-red-600 bg-red-50 border border-red-200 rounded-md px-2 py-1">
                {collectionState.error}
              </div>
            )}
          </div>
          <div className="flex items-center gap-2">
            <button
              className="h-10 px-4 text-sm font-medium rounded-lg border border-[hsl(var(--border))] ring-1 ring-inset ring-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--foreground))] transition-colors hover:bg-[hsl(var(--muted))] disabled:opacity-60 disabled:cursor-not-allowed inline-flex items-center gap-2"
              onClick={collectionState.inCollection ? handleRemoveFromCollection : handleAddToCollection}
              disabled={collectionState.loading}
            >
              {collectionState.inCollection ? (
                <>
                  <RiBookmarkFill className="w-4 h-4 text-amber-500" />
                  <span>{collectionState.loading ? 'Removing…' : 'Saved in Collection'}</span>
                </>
              ) : (
                <>
                  <RiBookmarkLine className="w-4 h-4" />
                  <span>{collectionState.loading ? 'Saving…' : 'Save to Collection'}</span>
                </>
              )}
            </button>
            {mounted && resolved === 'dark' && (
              <div className="inline-flex items-center gap-1 ml-1">
                <button
                  type="button"
                  aria-label="Bluish dark theme"
                  onClick={applyBluish}
                  className={`h-6 w-8 rounded-md border ${darkVariant==='bluish' ? 'ring-1 ring-[hsl(var(--ring))]' : 'border-[hsl(var(--border))]'}`}
                  style={{ background: 'linear-gradient(135deg, #0E2F3F 0%, #143245 100%)' }}
                  title="Bluish"
                />
                <button
                  type="button"
                  aria-label="Blackish dark theme"
                  onClick={applyBlackish}
                  className={`h-6 w-8 rounded-md border ${darkVariant==='blackish' ? 'ring-1 ring-[hsl(var(--ring))]' : 'border-[hsl(var(--border))]'}`}
                  style={{ background: 'linear-gradient(135deg, #0b0f15 0%, #111827 100%)' }}
                  title="Blackish"
                />
              </div>
            )}
            {mounted && <ThemeToggle />}
          </div>
        </div>
      </header>
      <main className="w-full px-6 py-4">
        <div className="w-full space-y-3">
          {content}
        </div>
      </main>
    </div>
    </Suspense>
  )
}
