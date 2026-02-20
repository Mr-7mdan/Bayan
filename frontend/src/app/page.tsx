"use client"

import { Suspense, useEffect, useMemo, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useSearchParams } from 'next/navigation'
import { Api, type EmbedTokenRowOut, type ShareEntryOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import 'react-grid-layout/css/styles.css'
import 'react-resizable/css/styles.css'
import GridLayout from 'react-grid-layout'
import * as Dialog from '@radix-ui/react-dialog'
import KpiCard from '@/components/widgets/KpiCard'
import ChartCard from '@/components/widgets/ChartCard'
import HeatmapCard from '@/components/widgets/HeatmapCard'
import TableCard from '@/components/widgets/TableCard'
import TextCard from '@/components/widgets/TextCard'
import SpacerCard from '@/components/widgets/SpacerCard'
import CompositionCard from '@/components/widgets/CompositionCard'
import ReportCard from '@/components/widgets/ReportCard'
import DataNavigator from '@/components/builder/DataNavigator'
import ConfiguratorPanel from '@/components/builder/ConfiguratorPanel'
import ConfiguratorPanelV2 from '@/components/builder/ConfiguratorPanelV2'
import GlobalFiltersBar from '@/components/builder/GlobalFiltersBar'
import { useFilters } from '@/components/providers/FiltersProvider'
import { ConfigUpdateContext } from '@/components/builder/ConfigUpdateContext'
import ErrorBoundary from '@/components/dev/ErrorBoundary'
import TitleBar from '@/components/builder/TitleBar'
import WidgetActionsMenu from '@/components/widgets/WidgetActionsMenu'
import { RiPushpin2Line, RiPushpin2Fill, RiSettings3Line, RiDragMove2Line } from '@remixicon/react'
import { createPortal } from 'react-dom'
import WidgetKebabMenu from '@/components/widgets/WidgetKebabMenu'
import AiAssistDialog from '@/components/ai/AiAssistDialog'
import { JsonEditor, githubDarkTheme, githubLightTheme } from 'json-edit-react'
import type { WidgetConfig, CompositionComponent } from '@/types/widgets'
import type { RGLLayout } from '@/lib/api'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'

export default function HomePage() {
  const { user } = useAuth()
  const { env } = useEnvironment()
  const searchParams = useSearchParams()
  const idParam = typeof window !== 'undefined' ? (searchParams?.get('id') || null) : null
  const defaultLayout: RGLLayout[] = []

  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [dashboardId, setDashboardId] = useState<string | null>(null)
  const [dashboardName, setDashboardName] = useState<string>('New Dashboard')
  const [createdAt, setCreatedAt] = useState<string | null>(null)
  const [publicId, setPublicId] = useState<string | null>(null)
  // Contextual actions menu state
  const [actionsMenuId, setActionsMenuId] = useState<string | null>(null)
  const [actionsAnchorEl, setActionsAnchorEl] = useState<HTMLElement | null>(null)
  // Simple kebab (three-dots) menu state
  const [kebabMenuId, setKebabMenuId] = useState<string | null>(null)
  const [kebabAnchorEl, setKebabAnchorEl] = useState<HTMLElement | null>(null)
  const [hasPublished, setHasPublished] = useState<boolean>(false)
  const [token, setToken] = useState<string>('')
  const [isProtected, setIsProtected] = useState<boolean>(false)
  const [layoutState, setLayoutState] = useState<RGLLayout[]>(defaultLayout)
  const [aiOpen, setAiOpen] = useState(false)
  const [aiWidgetId, setAiWidgetId] = useState<string | null>(null)
  const [embedOpen, setEmbedOpen] = useState<boolean>(false)
  const [embedWidgetId, setEmbedWidgetId] = useState<string | null>(null)
  const [embedWidth, setEmbedWidth] = useState<number>(800)
  const [embedHeight, setEmbedHeight] = useState<number>(360)
  const [embedTheme, setEmbedTheme] = useState<'light'|'dark'>('dark')
  const [embedBg, setEmbedBg] = useState<'default'|'transparent'>('default')
  const [embedHtml, setEmbedHtml] = useState<string>('')
  const [embedExpiry, setEmbedExpiry] = useState<number>(86400)
  const [embedEt, setEmbedEt] = useState<string>('')
  // Dashboard-level options
  const [dashOptions, setDashOptions] = useState<Record<string, any>>({ publicShowFilters: true, publicLockFilters: false, gridSize: 'lg' })
  const [configs, setConfigs] = useState<Record<string, WidgetConfig>>({})
  
  // Access global filters from context
  const { filters, setFilters } = useFilters()

  const selectedConfig = selectedId ? configs[selectedId] : null
  const userEditedRef = useRef<boolean>(false)
  const onConfigChange = (cfg: WidgetConfig) => {
    userEditedRef.current = true
    setConfigs((prev) => {
      const next = { ...prev, [cfg.id]: cfg }
      scheduleServerSave({ layout: layoutState, widgets: next })
      return next
    })
  }

  // Live metrics: dashboards open/close pings (builder)
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
    ;(async () => { try { await Api.dashboardsOpen('builder', dashboardId, sid) } catch {} })()
    const timer = window.setInterval(() => { try { void Api.dashboardsOpen('builder', dashboardId, sid) } catch {} }, 60000)
    const onUnload = () => { try { void Api.dashboardsClose('builder', dashboardId, sid) } catch {} }
    if (typeof window !== 'undefined') window.addEventListener('beforeunload', onUnload)
    return () => {
      try { window.clearInterval(timer) } catch {}
      if (typeof window !== 'undefined') window.removeEventListener('beforeunload', onUnload)
      if (!cancelled) { try { void Api.dashboardsClose('builder', dashboardId, sid) } catch {} }
    }
  }, [dashboardId])

  // Pack rows left: set contiguous x starting at 0, then fill last to cols. Respect minW/maxW; insert a 1-col spacer if needed.
  const packRowsLeftAndFill = () => {
    const C = cols
    const genId = (prefix: string) => `${prefix}_${Math.random().toString(36).slice(2, 8)}`
    setLayoutState((prev) => {
      const idxById: Record<string, number> = {}
      prev.forEach((it, idx) => { idxById[it.i] = idx })
      const byY: Record<number, string[]> = {}
      prev.forEach((it) => { const y = Number(it.y || 0); (byY[y] ||= []).push(it.i) })
      let changed = false
      const next = prev.map((it) => ({ ...it }))
      Object.entries(byY).forEach(([ys, ids]) => {
        const row = ids.map((id) => next[idxById[id]]).filter(Boolean).sort((a, b) => a.x - b.x)
        if (!row.length) return
        // Compact left
        let cursor = 0
        row.forEach((it) => {
          if (it.x !== cursor) { it.x = cursor; changed = true }
          cursor += it.w
        })
        // Try to fill
        const last = row[row.length - 1]
        const edge = last.x + last.w
        if (edge < C) {
          const maxW = (typeof (last as any).maxW === 'number' && (last as any).maxW! > 0) ? Math.min((last as any).maxW as number, C) : C
          const target = Math.min(C - last.x, maxW)
          if (target > last.w) { last.w = target; changed = true }
          // Still not reaching C? Add a spacer
          if (last.x + last.w < C) {
            const spacerId = genId('spacer')
            const gap = Math.max(1, C - (last.x + last.w))
            next.push({ i: spacerId, x: last.x + last.w, y: Number(ys), w: gap, h: Math.max(1, last.h), minW: 1 } as any)
            changed = true
          }
        }
      })
      if (changed) {
        try { scheduleServerSave({ layout: next as RGLLayout[], widgets: configs }, undefined, true) } catch {}
        return next as RGLLayout[]
      }
      return prev
    })
    userEditedRef.current = true
  }

  const makeEmbedUrl = (wid: string, w: number, h: number, theme: 'light'|'dark', bg: 'default'|'transparent'): string => {
    const base = ((env.publicDomain && env.publicDomain.trim()) ? env.publicDomain : (typeof window !== 'undefined' ? window.location.origin : '')).replace(/\/$/, '')
    const qs = new URLSearchParams()
    if (!publicId) return ''
    qs.set('publicId', publicId)
    if (isProtected) {
      if (embedEt) qs.set('et', embedEt)
      else if (token) qs.set('token', token)
    }
    qs.set('widgetId', wid)
    qs.set('theme', theme)
    if (bg === 'transparent') qs.set('bg', 'transparent')
    qs.set('w', String(w))
    qs.set('h', String(h))
    if (embedExpiry && embedExpiry > 0) qs.set('ttl', String(embedExpiry))
    qs.set('ts', String(Math.floor(Date.now() / 1000)))
    qs.set('controls', 'off')
    return `${base}/render/embed/widget?${qs.toString()}`
  }

  const embedPreviewUrl = useMemo(() => {
    if (!embedWidgetId) return ''
    return makeEmbedUrl(embedWidgetId, embedWidth, embedHeight, embedTheme, embedBg)
  }, [embedWidgetId, embedWidth, embedHeight, embedTheme, embedBg, publicId, token, embedEt, env.publicDomain])

  useEffect(() => {
    if (!embedOpen || !embedWidgetId) return
    if (!publicId) { setEmbedHtml(''); return }
    const url = makeEmbedUrl(embedWidgetId, embedWidth, embedHeight, embedTheme, embedBg)
    const html = url ? `<iframe src="${url}" width="${embedWidth}" height="${embedHeight}" style="border:0; width:${embedWidth}px; height:${embedHeight}px;" loading="lazy" referrerpolicy="no-referrer"></iframe>` : ''
    setEmbedHtml(html)
  }, [embedOpen, embedWidgetId, embedWidth, embedHeight, embedTheme, embedBg, embedExpiry, publicId, token, embedEt, env.publicDomain])

  async function generateEmbedEt() {
    if (!publicId) { setToast('Publish to generate token'); window.setTimeout(()=>setToast(''), 1200); return }
    try {
      const res = await Api.createEmbedToken(publicId, embedExpiry, user?.id)
      setEmbedEt(res.token)
      // Regenerate code immediately
      if (embedWidgetId) {
        const url = makeEmbedUrl(embedWidgetId, embedWidth, embedHeight, embedTheme, embedBg)
        const html = url ? `<iframe src="${url}" width="${embedWidth}" height="${embedHeight}" style="border:0; width:${embedWidth}px; height:${embedHeight}px;" loading="lazy" referrerpolicy="no-referrer"></iframe>` : ''
        setEmbedHtml(html)
      }
      // Refresh persisted tokens list
      if (dashboardId) { try { await refreshTokensAndShares() } catch {} }
    } catch (e: any) {
      setToast(e?.message || 'Failed to create token')
      window.setTimeout(()=>setToast(''), 1500)
    }
  }

  // When grid column count changes, proportionally rescale x/w to preserve layout intent and then clamp.
  useEffect(() => {
    const colsMap: Record<string, number> = { sm: 24, md: 18, lg: 12, xl: 8 }
    const dstCols = colsMap[String(dashOptions.gridSize || 'lg')] || 12
    setLayoutState((prev) => {
      const srcCols = Math.max(1, prev.reduce((acc, it) => Math.max(acc, Number(it.x || 0) + Number(it.w || 0)), 0))
      const ratio = dstCols / srcCols
      let changed = false
      const next = prev.map((it) => {
        // Scale to new column system, then clamp to bounds
        const scaledW = Math.max(1, Math.round(it.w * ratio))
        const w = Math.min(scaledW, dstCols)
        const scaledX = Math.max(0, Math.round(it.x * ratio))
        const x = Math.min(scaledX, Math.max(0, dstCols - w))
        if (w !== it.w || x !== it.x) { changed = true; return { ...it, w, x } }
        return it
      })
      if (changed) {
        try { scheduleServerSave({ layout: next, widgets: configs }) } catch {}
        return next
      }
      return prev
    })
  }, [dashOptions.gridSize])

  const [toast, setToast] = useState<string>('')
  const [hydrated, setHydrated] = useState<boolean>(false)
  const queryClient = useQueryClient()
  
  // Sync global filters to dashOptions for persistence in dashboard JSON
  useEffect(() => {
    if (!hydrated || !dashboardId) return
    
    const hasFilters = filters.startDate || filters.endDate || filters.filterPreset
    if (hasFilters) {
      setDashOptions((prev) => {
        const newOptions = {
          ...prev,
          globalFilters: {
            startDate: filters.startDate,
            endDate: filters.endDate,
            filterPreset: filters.filterPreset
          }
        }
        // Save to server with debounce
        scheduleServerSave(undefined, newOptions)
        return newOptions
      })
    } else {
      setDashOptions((prev) => {
        if (!prev.globalFilters) return prev
        // Clear global filters from options if none are set
        const { globalFilters, ...rest } = prev
        scheduleServerSave(undefined, rest)
        return rest
      })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filters.startDate, filters.endDate, filters.filterPreset, hydrated, dashboardId])
  // Navigator visibility and collapse behavior (builder-only)
  const [showNavigator, setShowNavigator] = useState<boolean>(() => {
    try { return localStorage.getItem('show_nav') !== '0' } catch { return true }
  })
  // Sidebar auto-minimize preference (builder-only)
  const [autoMinimizeSidebar, setAutoMinimizeSidebar] = useState<boolean>(() => {
    try { return localStorage.getItem('builder_auto_min_sidebar') !== '0' } catch { return true }
  })
  // Data Navigator defaults to collapsed independently of sidebar preference
  const [leftCollapsed, setLeftCollapsed] = useState<boolean>(true)
  const [rightCollapsed, setRightCollapsed] = useState(false)
  const [configPinned, setConfigPinned] = useState(false)
  const [useV2Panel, setUseV2Panel] = useState(false)
  const [loadTimes, setLoadTimes] = useState<Record<string, number>>({})
  // Visual viewport height to keep right configurator sized correctly when DevTools/mobile UI changes the viewport
  const [vvh, setVvh] = useState<number | null>(null)
  const [panelPos, setPanelPos] = useState<{ x: number; y: number } | null>(null)
  const [dragging, setDragging] = useState(false)
  const [gridInteracting, setGridInteracting] = useState(false) // Track grid drag/resize to gate configurator
  const leftTimerRef = useRef<any>(null)
  const rightTimerRef = useRef<any>(null)
  const collapseDelayMs = 200

  const startPanelDrag = (e: any) => {
    if (!panelPos) return
    // Ignore drags starting on interactive controls inside the header
    const t: HTMLElement | null = (e?.target as HTMLElement) || null
    if (t && t.closest('button, [role="button"], input, select, textarea, a')) return
    setDragging(true)
    const startX = e.clientX
    const startY = e.clientY
    const orig = { ...panelPos }
    const vw = window.visualViewport?.width || window.innerWidth
    const vh = window.visualViewport?.height || window.innerHeight
    const w = (rightCollapsed ? 44 : 360)
    const minX = 12, minY = 12
    const maxX = Math.max(minX, vw - w - 12)
    const maxY = Math.max(minY, (vvh || vh) - 240 - 12)
    const onMove = (ev: PointerEvent) => {
      const dx = ev.clientX - startX
      const dy = ev.clientY - startY
      const nx = Math.min(Math.max(orig.x + dx, minX), maxX)
      const ny = Math.min(Math.max(orig.y + dy, minY), maxY)
      setPanelPos({ x: nx, y: ny })
    }
    const onUp = () => {
      setDragging(false)
      window.removeEventListener('pointermove', onMove)
      window.removeEventListener('pointerup', onUp)
    }
    window.addEventListener('pointermove', onMove)
    window.addEventListener('pointerup', onUp, { once: true })
  }

  // Persist navigator visibility preference
  useEffect(() => { try { localStorage.setItem('show_nav', showNavigator ? '1' : '0') } catch {} }, [showNavigator])
  // Mirror sidebar auto-minimize (builder-only) for shell to read on builder
  useEffect(() => { try { localStorage.setItem('builder_auto_min_sidebar', autoMinimizeSidebar ? '1' : '0') } catch {} }, [autoMinimizeSidebar])

  // Publish dialog state (wire from TitleBar Publish)
  const [publishOpen, setPublishOpen] = useState<boolean>(false)
  const [pubMode, setPubMode] = useState<'public'|'user'>('public')
  const [pubToken, setPubToken] = useState<string>('')
  const [pubBusy, setPubBusy] = useState<boolean>(false)
  // Tokens & shares lists (persisted server-side)
  const [embedTokens, setEmbedTokens] = useState<EmbedTokenRowOut[]>([])
  const [shares, setShares] = useState<ShareEntryOut[]>([])
  const [tokBusy, setTokBusy] = useState<boolean>(false)

  async function refreshTokensAndShares() {
    if (!dashboardId) return
    setTokBusy(true)
    try {
      const [ets, sh] = await Promise.all([
        Api.listEmbedTokens(dashboardId, user?.id).catch(() => []),
        Api.listShares(dashboardId, user?.id).catch(() => []),
      ])
      setEmbedTokens(Array.isArray(ets) ? ets : [])
      setShares(Array.isArray(sh) ? sh : [])
    } finally { setTokBusy(false) }
  }

  useEffect(() => {
    if (!publishOpen || !dashboardId) return
    void refreshTokensAndShares()
  }, [publishOpen, dashboardId])
  const [pubLink, setPubLink] = useState<string>('')
  const [shareUser, setShareUser] = useState<string>('')
  const [sharePerm, setSharePerm] = useState<'ro'|'rw'>('ro')

  async function copyToClipboard(text: string): Promise<boolean> {
    try {
      if (navigator.clipboard && (window.isSecureContext ?? location.protocol === 'https:')) {
        await navigator.clipboard.writeText(text)
        return true
      }
      const ta = document.createElement('textarea')
      ta.value = text
      ta.style.position = 'fixed'
      ta.style.left = '-9999px'
      document.body.appendChild(ta)
      ta.focus(); ta.select()
      const ok = document.execCommand('copy')
      document.body.removeChild(ta)
      return ok
    } catch { return false }
  }

  async function onSave() {
    const payload = {
      id: dashboardId || undefined,
      name: dashboardName || 'New Dashboard',
      userId: user?.id || 'dev_user',
      definition: {
        layout: layoutState,
        widgets: configs,
        options: dashOptions,
      },
    }
    const res = await Api.saveDashboard(payload)
    setDashboardId(res.id)
    try { localStorage.setItem('dashboardId', res.id) } catch {}
    try { localStorage.setItem('dashboardDraft', JSON.stringify(payload.definition)) } catch {}
    // Clear publish state until we refetch status
    setPublicId(null)
    setToast(`Saved. id: ${res.id}`)
    window.setTimeout(() => setToast(''), 1000)
  }

  // Helper function to load dashboard options and apply global filters
  const loadDashboardOptions = (options: any) => {
    const opts = options || { publicShowFilters: true, publicLockFilters: false }
    setDashOptions(opts)
    
    // Load saved global filters from dashboard options and apply them
    if (opts.globalFilters) {
      const { startDate, endDate, filterPreset } = opts.globalFilters
      if (startDate || endDate || filterPreset) {
        setFilters({
          startDate: startDate || undefined,
          endDate: endDate || undefined,
          filterPreset: filterPreset || undefined
        })
      }
    }
  }

  async function onLoad() {
    if (!dashboardId) return
    const res = await Api.getDashboard(dashboardId, user?.id)
    const def = res.definition
    setLayoutState(def.layout)
    setConfigs(def.widgets as Record<string, WidgetConfig>)
    loadDashboardOptions((def as any).options)
    try { localStorage.setItem('dashboardDraft', JSON.stringify(def)) } catch {}
  }

  async function onPublish() {
    if (!dashboardId) return
    const res = await Api.publishDashboard(dashboardId, user?.id)
    setPublicId(res.publicId)
    setIsProtected(!!res.protected)
    setHasPublished(true)
    try {
      localStorage.setItem(`dash_has_published_${dashboardId}`, '1')
      localStorage.setItem(`dash_pub_id_${dashboardId}`, res.publicId)
    } catch {}
  }

  async function onUnpublish() {
    if (!dashboardId) return
    await Api.unpublishDashboard(dashboardId, user?.id)
    setPublicId(null)
    setIsProtected(false)
    setToken('')
    setHasPublished(false)
    try {
      localStorage.removeItem(`dash_has_published_${dashboardId}`)
      localStorage.removeItem(`dash_pub_id_${dashboardId}`)
    } catch {}
  }

  // Fetch publish status when dashboardId changes so initial state reflects server truth
  useEffect(() => {
    let cancelled = false
    async function fetchStatus() {
      if (!dashboardId) return
      try {
        const s = await Api.getPublishStatus(dashboardId)
        if (cancelled) return
        const pid = s.publicId || null
        setPublicId(pid)
        setIsProtected(!!s.protected)
        if (s.protected) {
          try {
            const saved = localStorage.getItem(`dash_pub_token_${dashboardId}`)
            if (saved) setToken(saved)
          } catch {}
        }
        if (pid) {
          setHasPublished(true)
          try {
            localStorage.setItem(`dash_has_published_${dashboardId}`, '1')
            localStorage.setItem(`dash_pub_id_${dashboardId}`, pid)
          } catch {}
        }
      } catch {
        if (!cancelled) {
          setPublicId(null)
        }
      }
    }
    void fetchStatus()
    return () => { cancelled = true }
  }, [dashboardId])

  // Load last draft and id on mount; always fetch from backend if an id exists.
  // Prefer id from URL (?id=) over localStorage.
  useEffect(() => {
    try {
      const urlId = idParam
      const lastId = urlId || localStorage.getItem('dashboardId')
      const draftStr = localStorage.getItem('dashboardDraft')
      let draft: { layout: RGLLayout[]; widgets: Record<string, WidgetConfig> } | null = null
      if (draftStr) {
        try { draft = JSON.parse(draftStr) } catch { draft = null }
      }

      if (lastId) {
        setDashboardId(lastId)
        try { localStorage.setItem('dashboardId', lastId) } catch {}
        // Load per-dashboard publish flags
        try {
          const hp = localStorage.getItem(`dash_has_published_${lastId}`)
          setHasPublished(!!hp)
          const pid = localStorage.getItem(`dash_pub_id_${lastId}`)
          if (pid) setPublicId(pid)
        } catch {}
        // Optionally show draft quickly for perceived loading, but DO NOT mark hydrated yet
        if (draft?.layout && draft?.widgets) {
          setLayoutState(draft.layout)
          setConfigs(draft.widgets)
          loadDashboardOptions((draft as any).options)
        }
        void (async () => {
          try {
            const res = await Api.getDashboard(lastId, user?.id)
            const def = res.definition
            setDashboardName(res.name || 'New Dashboard')
            setCreatedAt(res.createdAt || null)
            if (def?.layout && def?.widgets) {
              // Check for orphaned widgets (exist in widgets but not in layout)
              const layoutIds = new Set(def.layout.map((ly: RGLLayout) => ly.i))
              const widgetIds = Object.keys(def.widgets)
              const orphanedIds = widgetIds.filter(id => !layoutIds.has(id))
              
              let finalLayout = def.layout
              if (orphanedIds.length > 0) {
                // Auto-add orphaned widgets to layout at the bottom
                const maxY = def.layout.reduce((acc: number, ly: RGLLayout) => Math.max(acc, ly.y + ly.h), 0)
                const orphanedLayouts: RGLLayout[] = orphanedIds.map((id, idx) => {
                  const widget = (def.widgets as any)[id]
                  const type = widget?.type || 'chart'
                  const size = (type === 'table' || type === 'composition') ? { w: 9, h: 6 } : type === 'chart' ? { w: 6, h: 6 } : { w: 3, h: 2 }
                  return { i: id, x: 0, y: maxY + (idx * 7), w: size.w, h: size.h }
                })
                finalLayout = [...def.layout, ...orphanedLayouts]
              }
              
              setLayoutState(finalLayout)
              setConfigs(def.widgets as Record<string, WidgetConfig>)
              loadDashboardOptions((def as any).options)
              try { localStorage.setItem('dashboardDraft', JSON.stringify({ ...def, layout: finalLayout })) } catch {}
            }
          } catch { /* ignore */ }
          setHydrated(true)
        })()
      } else {
        // No dashboard id: use draft if present, else defaults; then create one server-side
        if (draft?.layout && draft?.widgets) {
          setLayoutState(draft.layout)
          setConfigs(draft.widgets)
        }
        void (async () => {
          try {
            const res = await Api.saveDashboard({
              name: dashboardName || 'New Dashboard',
              userId: user?.id || 'dev_user',
              definition: { layout: (draft as any)?.layout ?? layoutState, widgets: (draft as any)?.widgets ?? configs, options: (draft as any)?.options ?? dashOptions },
            })
            setDashboardId(res.id)
            setDashboardName(res.name || dashboardName)
            setCreatedAt(res.createdAt || null)
            try { localStorage.setItem('dashboardId', res.id) } catch {}
            // Ensure per-id publish flags start fresh
            try {
              localStorage.removeItem(`dash_has_published_${res.id}`)
              localStorage.removeItem(`dash_pub_id_${res.id}`)
            } catch {}
            const def = res.definition
            if (def?.layout && def?.widgets) {
              setLayoutState(def.layout)
              setConfigs(def.widgets as Record<string, WidgetConfig>)
              loadDashboardOptions((def as any).options)
              try { localStorage.setItem('dashboardDraft', JSON.stringify(def)) } catch {}
            }
          } catch { /* ignore first-run create errors */ }
          setHydrated(true)
        })()
      }
    } catch {}
  }, [])

  // Track visual viewport height so fixed overlays (Configurator) resize correctly when the inspector opens/closes
  useEffect(() => {
    if (typeof window === 'undefined') return
    const update = () => setVvh((window.visualViewport?.height || window.innerHeight))
    update()
    const vv = window.visualViewport
    vv?.addEventListener('resize', update)
    window.addEventListener('resize', update)
    return () => {
      vv?.removeEventListener('resize', update)
      window.removeEventListener('resize', update)
    }
  }, [])

  // Initialize panel position near the right edge once we know viewport
  useEffect(() => {
    if (typeof window === 'undefined') return
    if (panelPos) return
    const vw = window.visualViewport?.width || window.innerWidth
    const w = (rightCollapsed ? 44 : 360)
    const x = Math.max(12, vw - w - 24)
    const y = 72
    setPanelPos({ x, y })
  }, [vvh, rightCollapsed])

  // Keep panel within viewport when viewport or width changes
  useEffect(() => {
    if (typeof window === 'undefined') return
    if (!panelPos) return
    const vw = window.visualViewport?.width || window.innerWidth
    const w = (rightCollapsed ? 44 : 360)
    const maxX = Math.max(12, vw - w - 12)
    const maxY = Math.max(12, (vvh || window.innerHeight) - 240 - 12)
    const next = { x: Math.min(Math.max(panelPos.x, 12), maxX), y: Math.min(Math.max(panelPos.y, 12), maxY) }
    if (next.x !== panelPos.x || next.y !== panelPos.y) setPanelPos(next)
  }, [vvh, rightCollapsed])

  // Persist draft to localStorage on any change
  useEffect(() => {
    try { if (hydrated) localStorage.setItem('dashboardDraft', JSON.stringify({ layout: layoutState, widgets: configs, options: dashOptions })) } catch {}
  }, [layoutState, configs, dashOptions, hydrated])

  // Auto-refresh queries without page reload (builder-only)
  useEffect(() => {
    const sec = Number((dashOptions as any)?.refreshEverySec || 0)
    if (!sec || sec <= 0) return
    const tick = () => {
      try {
        if (typeof document !== 'undefined' && document.hidden) return
        queryClient.invalidateQueries({
          predicate: (q) => {
            const key0 = Array.isArray(q.queryKey) ? (q.queryKey[0] as any) : undefined
            return key0 === 'chart' || key0 === 'table' || key0 === 'kpi' || key0 === 'heatmap' || key0 === 'delta'
          },
          refetchType: 'inactive',
        })
      } catch {}
    }
    const id = window.setInterval(tick, sec * 1000)
    return () => { try { window.clearInterval(id) } catch {} }
  }, [queryClient, (dashOptions as any)?.refreshEverySec])

  // Debounced server save triggered only by explicit user actions
  const autosaveRef = useRef<number | undefined>(undefined)
  const scheduleServerSave = (nextDef?: { layout: RGLLayout[]; widgets: Record<string, WidgetConfig> }, nextOptions?: Record<string, any>, immediate?: boolean) => {
    if (!dashboardId || !hydrated) return
    const def = nextDef ?? { layout: layoutState, widgets: configs }
    const options = nextOptions ?? dashOptions
    if (autosaveRef.current) window.clearTimeout(autosaveRef.current)
    const doSave = async () => {
      try {
        await Api.saveDashboard({ id: dashboardId, name: dashboardName || 'New Dashboard', userId: user?.id || 'dev_user', definition: { ...(def as any), options } })
        userEditedRef.current = false
      } catch {
        // ignore autosave errors
      }
    }
    if (immediate) { void doSave(); return }
    autosaveRef.current = window.setTimeout(doSave, 600) as unknown as number
  }

  // Dialogs: View SpecQuery / SQL / JSON
  const [viewSpecId, setViewSpecId] = useState<string | null>(null)
  const [viewSqlId, setViewSqlId] = useState<string | null>(null)
  const [viewJsonId, setViewJsonId] = useState<string | null>(null)
  const [sqlPreview, setSqlPreview] = useState<string>('')
  const [pivotPayloads, setPivotPayloads] = useState<Record<string, any[]>>({})
  const [pivotSqls, setPivotSqls] = useState<Record<string, Array<{ label: string; sql: string }>>>({})

  // Capture runtime pivot payloads from widgets (TableCard emits these when running pivot)
  useEffect(() => {
    const handler = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { widgetId?: string; payloads?: any[] }
        if (!d?.widgetId || !Array.isArray(d?.payloads)) return
        setPivotPayloads((prev) => ({ ...prev, [d.widgetId!]: d.payloads! }))
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('widget-pivot-payloads', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('widget-pivot-payloads', handler as EventListener) }
  }, [])

  // Capture generated SQL strings emitted by widgets (background fetch in TableCard)
  useEffect(() => {
    const handler = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { widgetId?: string; sqls?: Array<{ label: string; sql: string }> }
        if (!d?.widgetId || !Array.isArray(d?.sqls)) return
        setPivotSqls((prev) => ({ ...prev, [d.widgetId!]: d.sqls! }))
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('widget-pivot-sql', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('widget-pivot-sql', handler as EventListener) }
  }, [])

  // Compute formatted SQL when opening the SQL dialog
  useEffect(() => {
    if (!viewSqlId) { setSqlPreview(''); return }
    const cfg: any = configs[viewSqlId]
    const hasPivotValues = Array.isArray(cfg?.pivot?.values) && (cfg.pivot.values as any[]).length > 0
    const tableTypePivot = ((cfg?.options?.table?.tableType || 'data') === 'pivot')
    const isPivot = tableTypePivot || hasPivotValues
    const hasSpecSource = !!cfg?.querySpec?.source
    if (isPivot && hasSpecSource) {
      ;(async () => {
        try {
          // Prefer SQL captured at execution time for this widget
          const captured = pivotSqls[viewSqlId]
          if (Array.isArray(captured) && captured.length > 0) {
            const parts = captured.filter(s => (s.sql||'').trim()).map((s) => `/* Metric: ${s.label || 'Metric'} */\n${s.sql.trim()}`)
            setSqlPreview(parts.length ? parts.join('\n\n') : 'No SQL generated for this pivot.')
            return
          }
          // Otherwise prefer runtime payloads (exact) if available from this widget
          const runtimePayloads = pivotPayloads[viewSqlId] || []
          let format: (sql: string) => string = (x) => x
          try {
            const mod: any = await import('sql-formatter-plus')
            format = (mod?.format || ((x: string) => x)) as (sql: string) => string
          } catch {}
          const parts: string[] = []
          if (runtimePayloads.length > 0) {
            for (let i = 0; i < runtimePayloads.length; i++) {
              const p = runtimePayloads[i]
              const req = {
                source: String(p?.source || cfg?.querySpec?.source || ''),
                datasourceId: cfg?.datasourceId,
                rows: Array.isArray(p?.rows) ? (p.rows as string[]) : [],
                cols: Array.isArray(p?.cols) ? (p.cols as string[]) : [],
                valueField: (p?.valueField ?? null) as string | null,
                aggregator: (p?.aggregator as any) || 'count',
                where: (p?.where || cfg?.querySpec?.where || undefined) as Record<string, any> | undefined,
                widgetId: cfg?.id,
                groupBy: (cfg?.querySpec?.groupBy || cfg?.xAxis?.groupBy || undefined) as string | undefined,
                weekStart: (cfg?.options?.xWeekStart || cfg?.querySpec?.weekStart || undefined) as string | undefined,
              }
              const res = await Api.pivotSql(req)
              const sql = String((res as any)?.sql || '').trim()
              const label = String((p as any).__label || p.valueField || (i===0 ? 'Count' : `Metric ${i+1}`))
              if (sql) parts.push(`/* Metric: ${label} */\n${format(sql)}`)
            }
          } else {
            // Reconstruct from config if runtime payloads are not available
            const rowsDims: string[] = (() => {
              const tRows = Array.isArray(cfg?.options?.table?.pivotConfig?.rows) ? (cfg.options.table.pivotConfig.rows as string[]) : []
              if (tRows && tRows.length) return tRows
              const px = cfg?.pivot?.x
              if (Array.isArray(px)) return (px as string[]).filter(Boolean)
              return px ? [String(px)] : []
            })()
            const colsDims: string[] = (() => {
              const tCols = Array.isArray(cfg?.options?.table?.pivotConfig?.cols) ? (cfg.options.table.pivotConfig.cols as string[]) : []
              if (tCols && tCols.length) return tCols
              const pl = cfg?.pivot?.legend
              if (Array.isArray(pl)) return (pl as string[]).filter(Boolean)
              return pl ? [String(pl)] : []
            })()
            const pvListRaw: Array<{ field?: string; measureId?: string; agg?: string; label?: string }>
              = Array.isArray(cfg?.pivot?.values) ? (cfg.pivot.values as any[]) : []
            const pvList: Array<{ field?: string; measureId?: string; agg?: string; label?: string }>
              = (pvListRaw.length > 0) ? pvListRaw : ([{ agg: 'count', label: 'Count' }] as any[])
            const mapFor = (agg?: string): 'sum'|'avg'|'min'|'max'|'distinct'|'count' => {
              const s = String(agg||'').toLowerCase()
              if (s.includes('sum')) return 'sum'
              if (s.startsWith('avg')) return 'avg'
              if (s === 'min') return 'min'
              if (s === 'max') return 'max'
              if (s.includes('distinct')) return 'distinct'
              return 'count'
            }
            for (let i = 0; i < pvList.length; i++) {
              const chip = pvList[i] as any
              const valueField = String(chip?.field || chip?.measureId || '')
              const aggregator = mapFor(chip?.agg)
              if (!valueField && aggregator !== 'count') continue
              const res = await Api.pivotSql({
                source: String(cfg.querySpec.source),
                datasourceId: cfg.datasourceId,
                rows: rowsDims,
                cols: colsDims,
                valueField: valueField || null,
                aggregator,
                where: (cfg.querySpec.where || undefined),
                widgetId: cfg.id,
                groupBy: (cfg.querySpec.groupBy || cfg.xAxis?.groupBy || undefined),
                weekStart: (cfg.options?.xWeekStart || cfg.querySpec?.weekStart || undefined),
              })
              const sql = String((res as any)?.sql || '').trim()
              const label = String(chip?.label || chip?.field || chip?.measureId || `Value ${i+1}`)
              if (sql) parts.push(`/* Metric: ${label} */\n${format(sql)}`)
            }
          }
          setSqlPreview(parts.length ? parts.join('\n\n') : 'No SQL generated for this pivot.')
        } catch (e) {
          const msg = (e instanceof Error) ? e.message : ''
          try { setSqlPreview(`Failed to generate pivot SQL${msg ? `: ${msg}` : ''}`) } catch {}
        }
      })()
      return
    }
    // Fallback: raw SQL field (non-pivot or SQL-mode widgets)
    const raw = String(cfg?.sql || '').trim()
    if (!raw) { setSqlPreview('This widget uses Spec Query mode or has no SQL defined.'); return }
    ;(async () => {
      try {
        const mod: any = await import('sql-formatter-plus')
        const formatted = mod?.format ? mod.format(raw) : raw
        setSqlPreview(formatted)
      } catch {
        setSqlPreview(raw)
      }
    })()
  }, [viewSqlId, configs])

  const closeAllViewers = () => { setViewSpecId(null); setViewSqlId(null); setViewJsonId(null) }

  // Add / Duplicate / Delete helpers
  const genId = (prefix: string) => `${prefix}_${Math.random().toString(36).slice(2, 8)}`
  // Quick add API for Composition builder
  const quickAddWidget = (kind: 'kpi'|'chart'|'table', opts?: { addToLayout?: boolean }): string => {
    const addToLayout = (opts?.addToLayout !== false)
    const id = genId(kind)
    const base: Partial<WidgetConfig> = { id, type: kind as any, title: kind === 'kpi' ? 'New KPI' : kind === 'chart' ? 'New Chart' : 'New Table', sql: '', queryMode: 'spec', querySpec: { source: '' } as any }
    let cfg: WidgetConfig
    if (kind === 'kpi') cfg = { ...(base as any), type: 'kpi' }
    else if (kind === 'chart') cfg = { ...(base as any), type: 'chart', chartType: 'line' } as any
    else cfg = { ...(base as any), type: 'table', title: 'New Table' }

    setConfigs((prev: Record<string, WidgetConfig>) => ({ ...prev, [id]: cfg }))
    let nextLayout = layoutState
    if (addToLayout) {
      const maxY = layoutState.reduce((acc: number, it: RGLLayout) => Math.max(acc, it.y + it.h), 0)
      const size = (kind === 'table') ? { w: 9, h: 6 } : kind === 'chart' ? { w: 6, h: 6 } : { w: 3, h: 2 }
      const nextLayoutItem: RGLLayout = { i: id, x: 0, y: maxY, w: size.w, h: size.h }
      setLayoutState((prev: RGLLayout[]) => ([...prev, nextLayoutItem]))
      nextLayout = [...layoutState, nextLayoutItem]
      setSelectedId(id)
    }
    userEditedRef.current = true
    scheduleServerSave({ layout: nextLayout, widgets: { ...configs, [id]: cfg } })
    return id
  }
  const addCard = (kind: WidgetConfig['type']) => {
    const id = genId(kind)
    const base: Partial<WidgetConfig> = { id, type: kind as any, title: kind === 'spacer' ? 'Spacer' : kind === 'text' ? 'Text' : `New ${String(kind).toUpperCase()}`, sql: '', queryMode: 'spec', querySpec: { source: '' } as any }
    let cfg: WidgetConfig
    if (kind === 'kpi') cfg = { ...(base as any), type: 'kpi', title: 'New KPI' }
    else if (kind === 'chart') cfg = { ...(base as any), type: 'chart', title: 'New Chart', chartType: 'line' } as any
    else if (kind === 'table') cfg = { ...(base as any), type: 'table', title: 'New Table' }
    else if (kind === 'text') cfg = { ...(base as any), type: 'text', title: 'Text', sql: '', options: { text: { labels: [{ text: 'New Text', style: 'h3', align: 'left' }], imageAlign: 'left' } } as any }
    else if (kind === 'spacer') cfg = { ...(base as any), type: 'spacer', title: 'Spacer', sql: '' }
    else if (kind === 'report') cfg = { ...(base as any), type: 'report', title: 'New Report', sql: '', options: { report: { gridCols: 12, gridRows: 20, cellSize: 30, elements: [], variables: [], showGridLines: true } } as any }
    else cfg = { ...(base as any), type: 'composition', title: 'Composition', sql: '', options: { composition: { components: [{ kind: 'title', text: 'New Composition', span: 12 }], columns: 12, gap: 2 } } as any }

    setConfigs((prev: Record<string, WidgetConfig>) => ({ ...prev, [id]: cfg }))
    // place at the end: compute next y as max y+h
    const maxY = layoutState.reduce((acc: number, it: RGLLayout) => Math.max(acc, it.y + it.h), 0)
    const size = (kind === 'table' || kind === 'composition' || kind === 'report') ? { w: 9, h: 6 } : kind === 'chart' ? { w: 6, h: 6 } : { w: 3, h: 2 }
    const extra = (kind === 'spacer') ? { minW: 2 } : {}
    const nextLayoutItem: RGLLayout = { i: id, x: 0, y: maxY, w: size.w, h: size.h, ...(extra as any) }
    setLayoutState((prev: RGLLayout[]) => ([...prev, nextLayoutItem]))
    setSelectedId(id)
    userEditedRef.current = true
    scheduleServerSave({ layout: [...layoutState, nextLayoutItem], widgets: { ...configs, [id]: cfg } })
  }
  const deleteCard = (id: string) => {
    setConfigs((prev: Record<string, WidgetConfig>) => {
      const next = { ...prev }; delete (next as any)[id]
      return next
    })
    setLayoutState((prev: RGLLayout[]) => prev.filter((it) => it.i !== id))
    if (selectedId === id) setSelectedId(null)
    userEditedRef.current = true
    scheduleServerSave({ layout: layoutState.filter((it) => it.i !== id), widgets: Object.fromEntries(Object.entries(configs).filter(([k]) => k !== id)) as any })
  }
  const duplicateCard = (id: string) => {
    const src = configs[id]; if (!src) return
    const newId = genId(src.type)
    const cfg: WidgetConfig = { ...src, id: newId, title: `${src.title} (copy)` }
    setConfigs((prev: Record<string, WidgetConfig>) => ({ ...prev, [newId]: cfg }))
    const srcLy = layoutState.find((l) => l.i === id)
    const ly: RGLLayout = srcLy ? { ...srcLy, i: newId, y: srcLy.y + srcLy.h } : { i: newId, x: 0, y: layoutState.reduce((a: number, it: RGLLayout) => Math.max(a, it.y + it.h), 0), w: 3, h: 2 }
    setLayoutState((prev: RGLLayout[]) => ([...prev, ly]))
    userEditedRef.current = true
    scheduleServerSave({ layout: [...layoutState, ly], widgets: { ...configs, [newId]: cfg } })
  }

  async function onSetToken(val?: string) {
    if (typeof val === 'string') setToken(val)
    if (!dashboardId) return
    const t = (typeof val === 'string') ? val : token
    const res = await Api.setPublishToken(dashboardId, t || undefined, user?.id)
    setIsProtected(!!res.protected)
    try {
      const key = `dash_pub_token_${dashboardId}`
      if (t) localStorage.setItem(key, t)
      else localStorage.removeItem(key)
    } catch {}
  }
  async function onRemoveToken() {
    if (!dashboardId) return
    const res = await Api.setPublishToken(dashboardId, undefined, user?.id)
    setIsProtected(!!res.protected)
    try { localStorage.removeItem(`dash_pub_token_${dashboardId}`) } catch {}
    setToken('')
  }

  // Measure canvas width to avoid overlap and enable scrolling
  const canvasRef = useRef<HTMLDivElement | null>(null)
  const [canvasW, setCanvasW] = useState<number>(980)
  useEffect(() => {
    if (!canvasRef.current) return
    const el = canvasRef.current
    const ro = new ResizeObserver(() => {
      const rect = el.getBoundingClientRect()
      // Subtract canvas padding (p-3 = 12px * 2 = 24px total horizontal padding)
      setCanvasW(Math.max(480, Math.floor(rect.width - 24)))
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  // auto-resize grid items to fit content height
  const itemRefs = useRef<Record<string, HTMLDivElement | null>>({})
  function setItemRef(key: string) { return (el: HTMLDivElement | null) => { itemRefs.current[key] = el } }
  const rowH = 24
  const recalcHeights = () => {
    setLayoutState((prev) => {
      const next = prev.map((it) => {
        const el = itemRefs.current[it.i]
        if (!el) return it
        const cfg = configs[it.i]
        const autoFit = cfg?.options?.autoFitCardContent !== false
        if (!autoFit) return it
        
        // Measure actual content height (includes p-3 padding on content div)
        const contentH = el.scrollHeight
        
        // The content div has p-3 (12px top + 12px bottom = 24px vertical padding)
        // We need to ensure after grid snapping, there's enough space for this padding
        // to be symmetric. Calculate rows needed, then ensure remainder >= 24px
        const minPaddingPx = 24 // p-3 top + bottom
        
        // Try initial row count
        let desiredRows = Math.max(2, Math.ceil(contentH / rowH))
        let actualGridHeight = desiredRows * rowH
        let remainder = actualGridHeight - contentH
        
        // If remainder is less than minimum padding needed, add another row
        // This ensures we have at least 12px top + 12px bottom symmetric padding
        if (remainder < minPaddingPx) {
          desiredRows += 1
        }
        
        return desiredRows !== it.h ? { ...it, h: desiredRows } : it
      })
      const changed = next.some((n, i) => n.h !== prev[i]?.h)
      return changed ? next : prev
    })
  }
  useEffect(() => {
    recalcHeights()
  }, [configs, canvasW])
  useEffect(() => {
    // watch content size changes of each item
    const observers: ResizeObserver[] = []
    layoutState.forEach((it) => {
      const el = itemRefs.current[it.i]
      if (!el) return
      const ro = new ResizeObserver(() => recalcHeights())
      ro.observe(el)
      observers.push(ro)
    })
    return () => observers.forEach((o) => o.disconnect())
  }, [layoutState])

  // Measure per-item widget-title heights to provide reservedTop for ChartCard measurement layout
  const [titleHeights, setTitleHeights] = useState<Record<string, number>>({})
  useEffect(() => {
    const measure = () => {
      const next: Record<string, number> = {}
      Object.entries(itemRefs.current).forEach(([id, el]) => {
        if (!el) return
        const t = el.querySelector('.widget-title') as HTMLElement | null
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
      if (t) {
        const ro = new ResizeObserver(measure)
        ro.observe(t)
        ros.push(ro)
      }
    })
    window.addEventListener('resize', measure)
    return () => {
      ros.forEach((ro) => { try { ro.disconnect() } catch {} })
      window.removeEventListener('resize', measure)
    }
  }, [layoutState, configs])

  // Compute current columns and apply per-card constraints; clamp to cols like public view
  const cols = useMemo(() => {
    const m: Record<string, number> = { sm: 24, md: 18, lg: 12, xl: 8 }
    return m[String(dashOptions.gridSize || 'lg')] || 12
  }, [dashOptions.gridSize])
  const effectiveLayout = useMemo(() => {
    return layoutState.map((it) => {
      const cfg = configs[it.i]
      let minW: number | undefined
      if (cfg?.type === 'spacer') minW = cfg.options?.spacer?.minW ?? 2
      const w = Math.min(it.w, cols)
      const x = Math.min(it.x, Math.max(0, cols - w))
      return { ...it, w, x, ...(minW != null ? { minW } : {}) }
    }) as RGLLayout[]
  }, [layoutState, configs, cols])

  // Debug: mirror public view debug globals in builder
  useEffect(() => {
    try {
      if (typeof window !== 'undefined') {
        const m: Record<string, number> = { sm: 24, md: 18, lg: 12, xl: 8 }
        ;(window as any).__dashOptions = dashOptions
        ;(window as any).__layout = layoutState
        ;(window as any).__gridSize = (dashOptions as any)?.gridSize
        ;(window as any).__impliedCols = Array.isArray(layoutState) ? layoutState.reduce((a, it) => Math.max(a, Number(it.x || 0) + Number(it.w || 0)), 0) : 0
        ;(window as any).__cols = m[String((dashOptions as any)?.gridSize || 'lg')] || 12
        ;(window as any).__canvasW = canvasW
        ;(window as any).__gridCanvasMode = (dashOptions as any)?.gridCanvasMode || 'auto'
        ;(window as any).__gridCanvasWidthPx = Number((dashOptions as any)?.gridCanvasWidthPx || 0)
      }
    } catch {}
  }, [dashOptions, layoutState, canvasW])

  // Resolve grid canvas width (builder): fixed from options or measured
  const gridWidth = useMemo(() => {
    const mode = String((dashOptions as any)?.gridCanvasMode || 'auto')
    const fixed = Number((dashOptions as any)?.gridCanvasWidthPx || 0)
    return (mode === 'fixed' && fixed > 0) ? fixed : canvasW
  }, [(dashOptions as any)?.gridCanvasMode, (dashOptions as any)?.gridCanvasWidthPx, canvasW])

  // Quick setters for canvas width mode
  const setCanvasAuto = () => {
    const next = { ...(dashOptions || {}), gridCanvasMode: 'auto' as const }
    setDashOptions(next)
    userEditedRef.current = true
    scheduleServerSave(undefined, next)
  }
  const setCanvasFixed = (w: number) => {
    const next = { ...(dashOptions || {}), gridCanvasMode: 'fixed' as const, gridCanvasWidthPx: Math.max(320, Math.floor(w)) }
    setDashOptions(next)
    userEditedRef.current = true
    scheduleServerSave(undefined, next)
  }
  const setCanvasFixedCurrent = () => setCanvasFixed(canvasW)

  // Normalize each row so the rightmost item expands to fill remaining columns up to 24 (or current cols)
  const normalizeLayoutToFullCols = () => {
    const C = cols
    setLayoutState((prev) => {
      const idxById: Record<string, number> = {}
      prev.forEach((it, idx) => { idxById[it.i] = idx })
      const byY: Record<number, string[]> = {}
      prev.forEach((it) => {
        const y = Number(it.y || 0)
        ;(byY[y] ||= []).push(it.i)
      })
      let changed = false
      const next = prev.map((it) => ({ ...it }))
      Object.entries(byY).forEach(([ys, ids]) => {
        const row = ids
          .map((id) => next[idxById[id]])
          .filter(Boolean)
          .sort((a, b) => a.x - b.x)
        if (!row.length) return
        const last = row[row.length - 1]
        const edge = last.x + last.w
        if (edge < C) {
          const maxW = (typeof (last as any).maxW === 'number' && (last as any).maxW! > 0) ? Math.min((last as any).maxW as number, C) : C
          const target = Math.min(C - last.x, maxW)
          if (target > last.w) {
            last.w = target
            changed = true
          }
        }
      })
      if (changed) {
        try { scheduleServerSave({ layout: next, widgets: configs }, undefined, true) } catch {}
        return next as RGLLayout[]
      }
      return prev
    })
    userEditedRef.current = true
  }

  // Persist pivot config changes coming from PivotTableView (builder only)
  useEffect(() => {
    const handler = (e: Event) => {
      const detail = (e as CustomEvent).detail as { widgetId?: string; pivotConfig?: any }
      if (!detail?.widgetId || !detail?.pivotConfig) return
      setConfigs((prev) => {
        const prevCfg = prev[detail.widgetId!]
        if (!prevCfg || prevCfg.type !== 'table') return prev
        const table = { ...(prevCfg.options?.table || {}), pivotConfig: detail.pivotConfig }
        const nextCfg: WidgetConfig = { ...prevCfg, options: { ...(prevCfg.options || {}), table } }
        const next = { ...prev, [detail.widgetId!]: nextCfg }
        scheduleServerSave({ layout: layoutState, widgets: next })
        return next
      })
    }
    window.addEventListener('pivot-config-change', handler as EventListener)
    return () => window.removeEventListener('pivot-config-change', handler as EventListener)
  }, [layoutState, hydrated, dashboardId])

  // Listen for widget action menu patches and merge into widget config
  useEffect(() => {
    const handler = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { widgetId?: string; patch?: Partial<WidgetConfig> }
        if (!d?.widgetId || !d.patch) return
        setConfigs((prev) => {
          const prevCfg = prev[d.widgetId!]
          if (!prevCfg) return prev
          const patch = d.patch as Partial<WidgetConfig>
          const nextOptions = patch.options ? { ...(prevCfg.options || {}), ...(patch.options as any) } : prevCfg.options
          // Deep-merge querySpec and querySpec.where if present
          let nextQuerySpec = prevCfg.querySpec
          if (patch.querySpec) {
            const prevQS = prevCfg.querySpec || {} as any
            const patchQS = patch.querySpec as any
            const mergedWhere = patchQS.where ? { ...(prevQS.where || {}), ...(patchQS.where || {}) } : prevQS.where
            nextQuerySpec = { ...prevQS, ...patchQS, ...(patchQS.where ? { where: mergedWhere } : {}) }
          }
          const nextCfg: WidgetConfig = {
            ...prevCfg,
            ...patch,
            ...(patch.options ? { options: nextOptions } : {}),
            ...(patch.querySpec ? { querySpec: nextQuerySpec } : {}),
          }
          const next = { ...prev, [d.widgetId!]: nextCfg }
          scheduleServerSave({ layout: layoutState, widgets: next })
          return next
        })
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('widget-config-patch', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('widget-config-patch', handler as EventListener) }
  }, [layoutState])

  // Listen for heatmap month changes
  useEffect(() => {
    const handler = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { widgetId?: string; month?: string }
        if (!d?.widgetId || !d.month) return
        setConfigs((prev) => {
          const cfg = prev[d.widgetId!]
          if (!cfg) return prev
          const opts = { ...cfg.options, heatmap: { ...(cfg.options as any)?.heatmap, calendarMonthly: { ...(cfg.options as any)?.heatmap?.calendarMonthly, month: d.month } } }
          const next = { ...prev, [d.widgetId!]: { ...cfg, options: opts } }
          scheduleServerSave({ layout: layoutState, widgets: next })
          return next
        })
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('heatmap-month-change', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('heatmap-month-change', handler as EventListener) }
  }, [layoutState])

  // Listen for heatmap year changes
  useEffect(() => {
    const handler = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { widgetId?: string; year?: string }
        if (!d?.widgetId || !d.year) return
        setConfigs((prev) => {
          const cfg = prev[d.widgetId!]
          if (!cfg) return prev
          const opts = { ...cfg.options, heatmap: { ...(cfg.options as any)?.heatmap, calendarAnnual: { ...(cfg.options as any)?.heatmap?.calendarAnnual, year: d.year } } }
          const next = { ...prev, [d.widgetId!]: { ...cfg, options: opts } }
          scheduleServerSave({ layout: layoutState, widgets: next })
          return next
        })
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('heatmap-year-change', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('heatmap-year-change', handler as EventListener) }
  }, [layoutState])

  // Listen for load time events from ChartCard
  useEffect(() => {
    const handler = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { widgetId?: string; seconds?: number }
        if (!d?.widgetId || typeof d.seconds !== 'number') return
        const baseId = String(d.widgetId).split('::tab:')[0]
        const secs = Number(d.seconds)
        setLoadTimes((prev) => ({ ...prev, [baseId]: Math.max(0, Math.floor(secs)) }))
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('chart-load-time', handler as EventListener)
    return () => { if (typeof window !== 'undefined') window.removeEventListener('chart-load-time', handler as EventListener) }
  }, [])

  // Global popover detector: watch for Radix/Tremor popovers and gate configurator hover while they're active
  useEffect(() => {
    if (typeof document === 'undefined' || typeof window === 'undefined') return
    const body = document.body as any
    const w = window as any
    const st = (w.__actionsMenuState ||= { count: 0, timeoutId: null as any })
    const popoverSelectors = [
      '[data-radix-popper-content-wrapper]',
      '[data-radix-portal]',
      '[role="listbox"]',
      '[role="menu"]',
      '[role="dialog"]',
      '.tremor-Select-popover',
      '.filterbar-popover',
    ].join(',')
    let activePopovers = new Set<Element>()
    const updateGate = () => {
      const currentPopovers = new Set(Array.from(document.querySelectorAll(popoverSelectors)))
      const added = Array.from(currentPopovers).filter(el => !activePopovers.has(el))
      const removed = Array.from(activePopovers).filter(el => !currentPopovers.has(el))
      added.forEach(() => {
        if (st.timeoutId) { window.clearTimeout(st.timeoutId); st.timeoutId = null }
        st.count = Math.max(0, Number(st.count || 0)) + 1
        body.dataset.actionsMenuOpen = '1'
        console.debug('[PopoverDetector] Gate ON (popover added), count:', st.count)
      })
      removed.forEach(() => {
        const prev = Math.max(0, Number(st.count || 0))
        if (prev <= 0) return
        st.count = prev - 1
        console.debug('[PopoverDetector] Gate dec (popover removed), count:', st.count)
        if (st.count === 0) {
          if (st.timeoutId) window.clearTimeout(st.timeoutId)
          st.timeoutId = window.setTimeout(() => {
            try { delete body.dataset.actionsMenuOpen } catch {}
            console.debug('[PopoverDetector] Gate OFF (cooldown)')
            st.timeoutId = null
          }, 300)
        }
      })
      activePopovers = currentPopovers
    }
    const observer = new MutationObserver(updateGate)
    observer.observe(document.body, { childList: true, subtree: true })
    updateGate() // Initial check
    return () => { observer.disconnect() }
  }, [])

  return (
    <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
    <div className="builder-root min-h-screen">
      <TitleBar
        hydrated={hydrated}
        dashboardId={dashboardId}
        publicId={publicId}
        isProtected={isProtected}
        token={token}
        onTokenChangeAction={setToken}
        onSaveAction={onSave}
        onLoadAction={onLoad}
        onPublishAction={() => setPublishOpen(true)}
        onUnpublishAction={onUnpublish}
        onSetTokenAction={onSetToken}
        onRemoveTokenAction={onRemoveToken}
        title={dashboardName}
        onTitleChangeAction={(v)=>{ 
          setDashboardName(v)
          try { localStorage.setItem('dashboardName', v) } catch {}
          userEditedRef.current = true
          ;(async () => {
            try {
              const res = await Api.saveDashboard({
                id: dashboardId || undefined,
                name: v || 'New Dashboard',
                userId: user?.id || 'dev_user',
                definition: { layout: layoutState, widgets: configs, options: dashOptions },
              })
              setDashboardId(res.id)
              setToast('Saved')
              window.setTimeout(() => setToast(''), 1000)
            } catch {}
          })()
        }}
        createdAt={createdAt || undefined}
        gridSize={(dashOptions.gridSize || 'lg')}
        onGridSizeChangeAction={(v)=>{
          const next = { ...(dashOptions || {}), gridSize: v }
          setDashOptions(next)
          userEditedRef.current = true
          scheduleServerSave(undefined, next)
        }}
        onAddCardAction={(t)=>{ addCard(t as any) }}
        showNavigator={showNavigator}
        onShowNavigatorChangeAction={(v)=>{ setShowNavigator(v); if (v) setLeftCollapsed(true) }}
        autoMinimizeNav={autoMinimizeSidebar}
        onAutoMinimizeNavChangeAction={(v)=>{ setAutoMinimizeSidebar(v); try { localStorage.setItem('builder_auto_min_sidebar', v ? '1' : '0') } catch {} }}
        publicShowFilters={!!(dashOptions.publicShowFilters ?? true)}
        onPublicShowFiltersChangeAction={(v)=>{ const next = { ...(dashOptions || {}), publicShowFilters: v }; setDashOptions(next); userEditedRef.current = true; scheduleServerSave(undefined, next) }}
        publicLockFilters={!!(dashOptions.publicLockFilters ?? false)}
        onPublicLockFiltersChangeAction={(v)=>{ const next = { ...(dashOptions || {}), publicLockFilters: v }; setDashOptions(next); userEditedRef.current = true; scheduleServerSave(undefined, next) }}
        refreshEverySec={Number(dashOptions.refreshEverySec || 0)}
        onRefreshEverySecChangeAction={(sec)=>{ const next = { ...(dashOptions || {}), refreshEverySec: Number(sec || 0) }; setDashOptions(next); userEditedRef.current = true; scheduleServerSave(undefined, next) }}
        onNormalizeLayoutAction={normalizeLayoutToFullCols}
        onPackRowsFillAction={packRowsLeftAndFill}
        onCanvasAutoAction={setCanvasAuto}
        onCanvasFixedAction={setCanvasFixed}
        onCanvasFixedCurrentAction={setCanvasFixedCurrent}
      />
      <main
        className="relative w-full grid gap-4 py-4 min-h-[calc(100vh-56px)] mx-auto px-6"
        style={{ gridTemplateColumns: showNavigator ? `${leftCollapsed ? '44px' : '280px'} 1fr` : '1fr' }}
      >
        {/* Data Navigator (left) */}
        {showNavigator && (
          <aside
            className={`hidden md:block overflow-hidden rounded-lg border shadow-card bg-card ${leftCollapsed ? 'cursor-pointer' : 'p-3'}`}
            title={leftCollapsed ? 'Click to expand' : undefined}
            aria-label="Data Navigator"
            aria-expanded={!leftCollapsed}
            onClick={() => { if (leftTimerRef.current) { try { clearTimeout(leftTimerRef.current) } catch {} } if (leftCollapsed) setLeftCollapsed(false) }}
            onMouseLeave={() => { if (leftTimerRef.current) { try { clearTimeout(leftTimerRef.current) } catch {} } leftTimerRef.current = setTimeout(() => setLeftCollapsed(true), 200) }}
          >
            <div className="relative h-full">
              {leftCollapsed && (
                <div
                  className="absolute left-1/2 -translate-x-1/2 text-[11px] font-medium text-foreground"
                  style={{ top: 8, writingMode: 'vertical-rl', textOrientation: 'mixed' }}
                >
                  Data
                </div>
              )}
              <div className={`${leftCollapsed ? 'opacity-0 pointer-events-none' : ''}`}>
                <DataNavigator />
              </div>
            </div>
          </aside>
        )}

        {/* Canvas (center) */}
        <section className="min-h-[60vh] rounded-lg border shadow-card p-3 bg-transparent overflow-auto" ref={canvasRef}>
          <div className="min-w-[720px] space-y-2">
            <div className="rounded-md border bg-card p-2">
              <div className="flex items-center justify-between gap-3">
                <GlobalFiltersBar
                  widgets={configs}
                  onApplyMappingAction={(map) => {
                    setConfigs((prev) => {
                      const next: Record<string, WidgetConfig> = {}
                      Object.entries(prev).forEach(([id, cfg]) => {
                        if (cfg && (cfg.type === 'kpi' || cfg.type === 'chart' || cfg.type === 'table')) {
                          const chosen = map[id]
                          const opt = { ...(cfg.options || {}), deltaDateField: chosen || undefined }
                          next[id] = { ...cfg, options: opt }
                        } else {
                          next[id] = cfg
                        }
                      })
                      userEditedRef.current = true
                      scheduleServerSave({ layout: layoutState, widgets: next })
                      return next
                    })
                  }}
                />

              </div>
            </div>
            <GridLayout
              className="layout"
              layout={effectiveLayout}
              cols={cols}
              rowHeight={24}
              width={gridWidth}
              margin={[10,10]}
              containerPadding={[10,10]}
              isResizable
              isDraggable
              draggableHandle=".widget-title"
              draggableCancel=".no-drag, .widget-title button"
              onLayoutChange={(ly: any) => setLayoutState(ly as RGLLayout[])}
              onDragStart={() => setGridInteracting(true)}
              onDragStop={(ly: any) => {
                setGridInteracting(false)
                userEditedRef.current = true
                const next = ly as RGLLayout[]
                setLayoutState(next)
                scheduleServerSave({ layout: next, widgets: configs })
              }}
              onResizeStart={() => setGridInteracting(true)}
              onResizeStop={(ly: any) => {
                setGridInteracting(false)
                userEditedRef.current = true
                const next = ly as RGLLayout[]
                setLayoutState(next)
                scheduleServerSave({ layout: next, widgets: configs })
              }}
            >
              {layoutState.map((ly) => {
                const cfg = configs[ly.i]
                if (!cfg) return null
                const showHeader = cfg.options?.showCardHeader !== false
                return (
                  <div
                    key={ly.i}
                    className={`h-full min-h-0 flex flex-col rounded-md overflow-hidden ${cfg.options?.cardFill === 'transparent' ? 'bg-transparent border-0 shadow-none' : 'border bg-card'}`}
                    onMouseDownCapture={(ev) => {
                      try {
                        const t = ev.target as HTMLElement | null
                        if (t && (t.closest('.no-drag') || t.closest('.widget-title button'))) return
                      } catch {}
                      setSelectedId(ly.i)
                    }}
                    ref={setItemRef(ly.i)}
                  >
                    {showHeader && (
                      <div className="widget-title flex-none px-3 py-2 bg-secondary/60 border-b text-sm font-medium border-l-2 border-l-[hsl(var(--header-accent))] flex items-center justify-between">
                        <span className="truncate" title={cfg.title}>
                          {cfg.title}
                          {((cfg.type === 'chart' || cfg.type === 'table') && (cfg.options as any)?.showLoadTime) ? (
                            (() => {
                              const key = String(cfg.id).split('::tab:')[0]
                              const secs = (loadTimes as any)[key]
                              return (secs != null) ? (
                                <span className="ml-2 text-[11px] text-muted-foreground whitespace-nowrap">Loaded in {secs}s</span>
                              ) : null
                            })()
                          ) : null}
                        </span>
                        <span className="flex items-center gap-1 no-drag">
                          <button
                            type="button"
                            className="text-xs px-1 py-0.5 rounded border hover:bg-muted"
                            title="Widget actions"
                            onClick={(e) => { e.stopPropagation(); setActionsMenuId(cfg.id); setActionsAnchorEl(e.currentTarget as HTMLElement) }}
                          >
                            <RiSettings3Line className="h-4 w-4" aria-hidden="true" />
                          </button>
                          {cfg.type === 'composition' && (
                            <button type="button" className="text-[11px] px-2 py-0.5 rounded border hover:bg-muted"
                              title="Toggle inner edit mode"
                              onClick={(e) => {
                                e.stopPropagation()
                                const curr = (cfg.options?.composition as any)?.innerInteractive === true
                                const composition = { ...(cfg.options?.composition || {}), innerInteractive: !curr }
                                const nextCfg = { ...cfg, options: { ...(cfg.options || {}), composition } } as WidgetConfig
                                setConfigs((prev) => ({ ...prev, [cfg.id]: nextCfg }))
                                scheduleServerSave({ layout: layoutState, widgets: { ...configs, [cfg.id]: nextCfg } })
                              }}>
                              {((cfg.options?.composition as any)?.innerInteractive ? 'Disable Inner Edit' : 'Enable Inner Edit')}
                            </button>
                          )}
                          <button
                            type="button"
                            className="text-xs px-1 py-0.5 rounded border hover:bg-muted"
                            title="More actions"
                            onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); console.debug('[Kebab] mousedown open', cfg.id); setKebabMenuId(cfg.id); setKebabAnchorEl(e.currentTarget as HTMLElement) }}
                            onClick={(e) => { e.preventDefault(); e.stopPropagation(); console.debug('[Kebab] click open', cfg.id); setKebabMenuId(cfg.id); setKebabAnchorEl(e.currentTarget as HTMLElement) }}
                          >⋮</button>
                        </span>
                      </div>
                    )}
                    <div className="p-3 flex-1 min-h-0 flex flex-col">
                      {cfg.type === 'kpi' && (
                        <ErrorBoundary name="KpiCard">
                          <KpiCard
                            title={cfg.title}
                            sql={cfg.sql}
                            datasourceId={cfg.datasourceId}
                            queryMode={cfg.queryMode}
                            querySpec={cfg.querySpec as any}
                            options={cfg.options}
                            pivot={cfg.pivot as any}
                            widgetId={cfg.id}
                          />
                        </ErrorBoundary>
                      )}
                      {cfg.type === 'chart' && (
                        <ErrorBoundary name="ChartCard">
                          {((cfg as any).chartType === 'heatmap') ? (
                            <HeatmapCard
                              title={cfg.title}
                              sql={cfg.sql}
                              datasourceId={cfg.datasourceId}
                              options={cfg.options}
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
                              options={cfg.options}
                              queryMode={cfg.queryMode}
                              querySpec={cfg.querySpec as any}
                              customColumns={cfg.customColumns}
                              widgetId={cfg.id}
                              pivot={cfg.pivot}
                              layout="measure"
                              reservedTop={titleHeights[ly.i] ?? 0}
                            />
                          )}
                        </ErrorBoundary>
                      )}
                      {cfg.type === 'table' && (
                        <ErrorBoundary name="TableCard">
                          <TableCard
                            title={cfg.title}
                            sql={cfg.sql}
                            datasourceId={cfg.datasourceId}
                            options={cfg.options as any}
                            queryMode={cfg.queryMode as any}
                            querySpec={cfg.querySpec as any}
                            widgetId={cfg.id}
                            customColumns={cfg.customColumns as any}
                            pivot={cfg.pivot as any}
                          />
                        </ErrorBoundary>
                      )}
                      {cfg.type === 'text' && (
                        <ErrorBoundary name="TextCard">
                          <TextCard title={cfg.title} options={cfg.options} />
                        </ErrorBoundary>
                      )}
                      {cfg.type === 'spacer' && (
                        <ErrorBoundary name="SpacerCard">
                          <SpacerCard options={cfg.options} />
                        </ErrorBoundary>
                      )}
                      {cfg.type === 'composition' && (
                        <ErrorBoundary name="CompositionCard">
                          <CompositionCard
                            title={cfg.title}
                            options={cfg.options}
                            widgets={configs}
                            interactive={!!(cfg.options?.composition as any)?.innerInteractive}
                            onSelectWidget={(id: string) => { setSelectedId(id) }}
                            onUpdate={(next: CompositionComponent[]) => {
                              const composition = { ...(cfg.options?.composition || {}), components: next }
                              const nextCfg = { ...cfg, options: { ...(cfg.options || {}), composition } } as WidgetConfig
                              setConfigs((prev) => ({ ...prev, [cfg.id]: nextCfg }))
                              userEditedRef.current = true
                              scheduleServerSave({ layout: layoutState, widgets: { ...configs, [cfg.id]: nextCfg } })
                            }}
                            widgetId={cfg.id}
                          />
                        </ErrorBoundary>
                      )}
                      {cfg.type === 'report' && (
                        <ErrorBoundary name="ReportCard">
                          <ReportCard
                            title={cfg.title}
                            options={cfg.options}
                            widgetId={cfg.id}
                            datasourceId={cfg.datasourceId}
                          />
                        </ErrorBoundary>
                      )}
                    </div>
                    {/* Contextual actions menu for this card */}
                    <WidgetActionsMenu
                      widgetId={cfg.id}
                      cfg={cfg}
                      anchorEl={actionsMenuId === cfg.id ? actionsAnchorEl : null}
                      open={actionsMenuId === cfg.id}
                      onCloseAction={() => { setActionsMenuId(null); setActionsAnchorEl(null) }}
                      parentDashboardId={dashboardId}
                    />
                  </div>
                )
              })}
            </GridLayout>
          </div>
        </section>

        {/* Configurator overlay (appears only when a widget is selected) */}
        {selectedConfig && (
          <div className="hidden lg:block">
            <div
              className={`fixed rounded-lg border shadow-card bg-card z-50 flex flex-col overflow-visible ${rightCollapsed ? '' : 'px-0'} ${gridInteracting ? 'pointer-events-none' : ''}`}
              style={{
                width: rightCollapsed ? 44 : 360,
                height: vvh ? Math.max(240, vvh - (panelPos?.y ?? 0) - 24) : undefined,
                left: panelPos?.x ?? undefined,
                top: panelPos?.y ?? 72,
              }}
              onMouseEnter={() => {
                if (typeof document !== 'undefined' && document.body?.dataset?.actionsMenuOpen === '1') return
                if (dragging) return
                if (gridInteracting) return
                if (rightTimerRef.current) { try { clearTimeout(rightTimerRef.current) } catch {} }
                // If tucked, expand while preserving the snapped edge
                setRightCollapsed((prev) => {
                  if (prev && typeof window !== 'undefined' && panelPos) {
                    const vw = window.visualViewport?.width || window.innerWidth
                    const margin = 12
                    const collapsedW = 44
                    const expandedW = 360
                    const rightSnapX = Math.max(margin, vw - collapsedW - margin)
                    const isRight = Math.abs(panelPos.x - rightSnapX) < 24
                    const nextX = isRight ? Math.max(margin, vw - expandedW - margin) : margin
                    setPanelPos({ x: nextX, y: panelPos.y })
                  }
                  return false
                })
              }}
              onMouseLeave={() => {
                if (typeof document !== 'undefined' && document.body?.dataset?.actionsMenuOpen === '1') return
                if (dragging) return
                if (gridInteracting) return
                if (rightTimerRef.current) { try { clearTimeout(rightTimerRef.current) } catch {} }
                if (!configPinned) rightTimerRef.current = setTimeout(() => {
                  if (typeof window === 'undefined') { setRightCollapsed(true); return }
                  const vw = window.visualViewport?.width || window.innerWidth
                  const margin = 12
                  const currW = (rightCollapsed ? 44 : 360)
                  const collapsedW = 44
                  const leftDist = Math.max(0, (panelPos?.x ?? margin) - margin)
                  const rightDist = Math.max(0, (vw - ((panelPos?.x ?? 0) + currW) - margin))
                  const snapRight = rightDist <= leftDist
                  const nextX = snapRight ? Math.max(margin, vw - collapsedW - margin) : margin
                  if (panelPos) setPanelPos({ x: nextX, y: panelPos.y })
                  setRightCollapsed(true)
                }, collapseDelayMs)
              }}
            >
              {rightCollapsed && (
                <div
                  className="absolute left-1/2 -translate-x-1/2 text-[11px] font-medium text-foreground"
                  style={{ top: 8, writingMode: 'vertical-rl', textOrientation: 'mixed' }}
                >
                  Configurator
                </div>
              )}
              <div className={`flex items-center justify-between mb-0 ${rightCollapsed ? 'opacity-0 pointer-events-none' : ''} sticky top-0 bg-card z-10 pt-1 px-3 cursor-move select-none`} 
                onPointerDown={startPanelDrag}
              > 
                <h2 className="text-sm font-medium">Configurator</h2>
                <div className="flex items-center gap-1">
                  <button
                    onClick={() => setUseV2Panel(v => !v)}
                    className={`text-[10px] px-1.5 py-0.5 rounded border transition-colors duration-150 cursor-pointer font-semibold ${
                      useV2Panel ? 'bg-[hsl(var(--primary))] text-primary-foreground border-[hsl(var(--primary))]' : 'hover:bg-muted text-muted-foreground'
                    }`}
                    title="Toggle V2 panel"
                  >{useV2Panel ? 'V2 ✓' : 'V2'}</button>
                  <button
                    onClick={() => setConfigPinned((prev) => { const next = !prev; if (next) setRightCollapsed(false); return next })}
                    className={`text-xs px-2 py-1 rounded-md border hover:bg-muted ${configPinned ? 'bg-[hsl(var(--secondary))]' : ''}`}
                    title={configPinned ? 'Unpin' : 'Pin'}
                    aria-label={configPinned ? 'Unpin configurator' : 'Pin configurator'}
                  >
                    {configPinned ? (
                      <RiPushpin2Fill className="h-4 w-4" aria-hidden="true" />
                    ) : (
                      <RiPushpin2Line className="h-4 w-4" aria-hidden="true" />
                    )}
                  </button>
                  <button
                    onClick={() => setSelectedId(null)}
                    className="text-xs px-2 py-1 rounded-md border hover:bg-muted"
                    title="Close"
                    aria-label="Close configurator"
                  >
                    ✕
                  </button>
                </div>
              </div>
              <ConfigUpdateContext.Provider value={onConfigChange}>
                <div className={`flex-1 min-h-0 overflow-y-auto overflow-x-visible overscroll-contain pt-1 pb-2 px-3 ${rightCollapsed ? 'opacity-0 pointer-events-none' : ''}`}>
                  {useV2Panel
                    ? <ConfiguratorPanelV2 selected={selectedConfig} allWidgets={configs} quickAddAction={quickAddWidget} />
                    : <ConfiguratorPanel selected={selectedConfig} allWidgets={configs} quickAddAction={quickAddWidget} />}
                </div>
              </ConfigUpdateContext.Provider>
              {/* Drag handle */}
              <div
                className="flex items-center justify-center py-1.5 mt-2 cursor-move select-none text-muted-foreground border-t bg-[hsl(var(--secondary)/0.6)]"
                title="Drag to move"
                onPointerDown={(e) => {
                  if (!panelPos) return
                  setDragging(true)
                  const startX = e.clientX
                  const startY = e.clientY
                  const orig = { ...panelPos }
                  const vw = window.visualViewport?.width || window.innerWidth
                  const vh = window.visualViewport?.height || window.innerHeight
                  const w = (rightCollapsed ? 44 : 360)
                  const minX = 12, minY = 12
                  const maxX = Math.max(minX, vw - w - 12)
                  const maxY = Math.max(minY, (vvh || vh) - 240 - 12)
                  const onMove = (ev: PointerEvent) => {
                    const dx = ev.clientX - startX
                    const dy = ev.clientY - startY
                    const nx = Math.min(Math.max(orig.x + dx, minX), maxX)
                    const ny = Math.min(Math.max(orig.y + dy, minY), maxY)
                    setPanelPos({ x: nx, y: ny })
                  }
                  const onUp = () => {
                    setDragging(false)
                    window.removeEventListener('pointermove', onMove)
                    window.removeEventListener('pointerup', onUp)
                  }
                  window.addEventListener('pointermove', onMove)
                  window.addEventListener('pointerup', onUp, { once: true })
                }}
              >
                <RiDragMove2Line className="h-4 w-4" aria-hidden="true" />
              </div>
            </div>
          </div>
        )}
      </main>

      {/* Publish / Share Dialog (reused pattern) */}
      <Dialog.Root open={publishOpen} onOpenChange={(v) => { if (!v) setPublishOpen(false) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/20" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[560px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">Publish or Share</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mt-1">Choose whether to publish a public read‑only link, or share with a specific user.</Dialog.Description>
            <div className="mt-4 space-y-4">
              <div className="space-y-2">
                <label className="flex items-center gap-2 text-sm"><input type="radio" name="pubmode" checked={pubMode==='public'} onChange={() => setPubMode('public')} /><span>Public version: read‑only</span></label>
                <label className="flex items-center gap-2 text-sm"><input type="radio" name="pubmode" checked={pubMode==='user'} onChange={() => setPubMode('user')} /><span>Share with a user</span></label>
              </div>
              {pubMode==='public' && (
                <div className="space-y-3">
                  <label className="text-sm block">Token (optional)
                    <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" placeholder="Leave empty for public access" value={pubToken} onChange={(e) => setPubToken(e.target.value)} />
                  </label>
                  <div className="flex gap-2">
                    <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" onClick={() => setPubToken(crypto.randomUUID())}>Generate secure token</button>
                    <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={pubBusy || !dashboardId} onClick={async () => {
                      if (!dashboardId) return; setPubBusy(true); try {
                        const res = await Api.setPublishToken(dashboardId, pubToken || undefined, user?.id)
                        setPublicId(res.publicId)
                        setIsProtected(!!pubToken)
                        if (pubToken) setToken(pubToken)
                        // Persist token for future 'Copy URL'
                        try {
                          const key = `dash_pub_token_${dashboardId}`
                          if (pubToken) localStorage.setItem(key, pubToken)
                          else localStorage.removeItem(key)
                        } catch {}
                        const base = ((env.publicDomain && env.publicDomain.trim()) ? env.publicDomain : (typeof window !== 'undefined' ? window.location.origin : '')).replace(/\/$/, '')
                        const url = `${base}/v/${res.publicId}${pubToken ? `?token=${encodeURIComponent(pubToken)}` : ''}`
                        setPubLink(url)
                      } finally { setPubBusy(false) }
                    }}>Save & generate link</button>
                  </div>
                  {!!pubLink && (
                    <div className="flex items-center gap-2 text-xs text-muted-foreground break-all">
                      <span className="font-mono flex-1">{pubLink}</span>
                      <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" type="button" onClick={async () => { await copyToClipboard(pubLink) }}>Copy</button>
                    </div>
                  )}
                </div>
              )}
              {pubMode==='user' && (
                <div className="space-y-3">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <label className="text-sm block">Share with user (email or id)
                      <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" placeholder="user@example.com" value={shareUser} onChange={(e) => setShareUser(e.target.value)} />
                    </label>
                    <div>
                      <div className="text-sm mb-1">Permission</div>
                      <label className="flex items-center gap-2 text-sm"><input type="radio" name="perm" checked={sharePerm==='ro'} onChange={() => setSharePerm('ro')} /><span>Read‑only</span></label>
                      <label className="flex items-center gap-2 text-sm mt-1"><input type="radio" name="perm" checked={sharePerm==='rw'} onChange={() => setSharePerm('rw')} /><span>Read‑write</span></label>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={pubBusy || !shareUser.trim()} onClick={async () => {
                      if (!dashboardId || !shareUser.trim()) return; setPubBusy(true); try {
                        const me = user?.name || user?.id || 'Someone'
                        await Api.addToCollection(shareUser.trim(), { userId: shareUser.trim(), dashboardId, sharedBy: me, dashboardName, permission: sharePerm })
                        setPublishOpen(false)
                      } finally { setPubBusy(false) }
                    }}>Share dashboard</button>
                  </div>
                </div>
              )}
            {/* Tokens & Shares Panel */}
            <div className="mt-4 border-t pt-3 space-y-3">
              <div className="text-sm font-medium">Tokens & Shares</div>
              <div className="grid grid-cols-1 gap-3">
                <div className="rounded-md border p-2">
                  <div className="flex items-center justify-between mb-1">
                    <div className="text-xs font-semibold">Embed Tokens</div>
                    {tokBusy && <div className="text-[11px] text-muted-foreground">Refreshing…</div>}
                  </div>
                  {embedTokens.length === 0 ? (
                    <div className="text-xs text-muted-foreground">No embed tokens yet.</div>
                  ) : (
                    <div className="space-y-1">
                      {embedTokens.map((t) => {
                        const expText = (() => { try { return new Date((Number(t.exp)||0)*1000).toLocaleString() } catch { return String(t.exp) } })()
                        const revoked = !!t.revokedAt
                        const short = t.token.length > 32 ? `${t.token.slice(0, 20)}…${t.token.slice(-8)}` : t.token
                        return (
                          <div key={t.id} className="flex items-center justify-between text-xs gap-2">
                            <div className="flex-1 truncate">
                              <span className={`font-mono ${revoked ? 'line-through text-muted-foreground' : ''}`}>{short}</span>
                              <span className="ml-2 text-muted-foreground">exp: {expText}</span>
                              {revoked && <span className="ml-2 text-red-500">revoked</span>}
                            </div>
                            <div className="flex items-center gap-2">
                              <button className="px-2 py-0.5 rounded-md border hover:bg-muted" disabled={revoked} onClick={async () => { try { await navigator.clipboard?.writeText(t.token) } catch {} }}>Copy</button>
                              <button className="px-2 py-0.5 rounded-md border hover:bg-muted disabled:opacity-50" disabled={revoked || !dashboardId} onClick={async () => {
                                if (!dashboardId) return
                                await Api.deleteEmbedToken(dashboardId, t.id, user?.id)
                                void refreshTokensAndShares()
                              }}>Delete</button>
                            </div>
                          </div>
                        )
                      })}
                    </div>
                  )}
                </div>
                <div className="rounded-md border p-2">
                  <div className="text-xs font-semibold mb-1">Shared Users</div>
                  {shares.length === 0 ? (
                    <div className="text-xs text-muted-foreground">No users have access via share.</div>
                  ) : (
                    <div className="space-y-1">
                      {shares.map((s) => (
                        <div key={`${s.userId}:${s.permission}`} className="flex items-center justify-between text-xs gap-2">
                          <div className="flex-1 truncate">
                            <span className="font-medium">{s.userName || s.userId}</span>
                            {s.email && <span className="ml-2 text-muted-foreground">{s.email}</span>}
                            <span className="ml-2 inline-flex items-center px-1.5 py-0.5 rounded border">{s.permission.toUpperCase()}</span>
                          </div>
                          <button className="px-2 py-0.5 rounded-md border hover:bg-muted" onClick={async () => { if (!dashboardId) return; await Api.deleteShare(dashboardId, s.userId, user?.id); void refreshTokensAndShares() }}>Remove</button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild><button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Close</button></Dialog.Close>
            </div>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
      {/* Global kebab menu (outside card overlays) */}
      <WidgetKebabMenu
        open={!!kebabMenuId}
        anchorEl={kebabMenuId ? kebabAnchorEl : null}
        onCloseAction={() => { setKebabMenuId(null); setKebabAnchorEl(null) }}
        widgetType={kebabMenuId ? configs[kebabMenuId]?.type : undefined}
        chartType={kebabMenuId ? (configs[kebabMenuId] as any)?.chartType : undefined}
        onAction={(action) => {
          const id = kebabMenuId as string | null
          if (!id) return
          if (action === 'duplicate') duplicateCard(id)
          else if (action === 'delete') deleteCard(id)
          else if (action === 'viewSpec') setViewSpecId(id)
          else if (action === 'viewSql') setViewSqlId(id)
          else if (action === 'viewJson') setViewJsonId(id)
          else if (action === 'aiAssist') { setAiWidgetId(id); setAiOpen(true) }
          else if (action === 'embed') { setEmbedWidgetId(id); setEmbedOpen(true) }
          else if (action === 'downloadPNG') {
            window.dispatchEvent(new CustomEvent('widget-download-chart', { detail: { widgetId: id, format: 'png' } }))
          }
          else if (action === 'downloadSVG') {
            window.dispatchEvent(new CustomEvent('widget-download-chart', { detail: { widgetId: id, format: 'svg' } }))
          }
        }}
      />
      {/* Viewers */}
      <ViewerDialog
        open={!!viewSpecId}
        title="Spec Query"
        copyText={( () => { try { return JSON.stringify((viewSpecId ? (configs[viewSpecId]?.querySpec || {}) : {}), null, 2) } catch { return '{}' } })()}
        onClose={() => setViewSpecId(null)}
      >
        <JsonViewer data={( () => { try { return (viewSpecId ? (configs[viewSpecId]?.querySpec || {}) : {}) } catch { return {} } })()} />
      </ViewerDialog>
      <ViewerDialog open={!!viewSqlId} title="SQL Statement" copyText={sqlPreview} onClose={() => setViewSqlId(null)}>
        <pre className="text-xs font-mono whitespace-pre-wrap leading-5 p-2 rounded-md border bg-[hsl(var(--secondary)/0.4)]">{sqlPreview}</pre>
      </ViewerDialog>
      <ViewerDialog
        open={!!viewJsonId}
        title="Widget JSON Config"
        copyText={( () => { try { return JSON.stringify((viewJsonId ? (configs[viewJsonId] || {}) : {}), null, 2) } catch { return '{}' } })()}
        onClose={() => setViewJsonId(null)}
      >
        <JsonViewer data={( () => { try { return viewJsonId ? (configs[viewJsonId] || {}) : {} } catch { return {} } })()} />
      </ViewerDialog>
      <AiAssistDialog
        open={aiOpen}
        onCloseAction={() => { setAiOpen(false); setAiWidgetId(null) }}
        widget={aiWidgetId ? configs[aiWidgetId] : null}
        onApplyAction={(cfg) => {
          const id = cfg?.id || aiWidgetId
          if (!id) return
          setConfigs((prev) => ({ ...prev, [id]: cfg }))
        }}
      />
      <EmbedDialog
        open={embedOpen}
        onClose={() => { setEmbedOpen(false); setEmbedWidgetId(null) }}
        code={embedHtml}
        onCodeChange={setEmbedHtml}
        urlPreview={embedPreviewUrl}
        width={embedWidth}
        height={embedHeight}
        theme={embedTheme}
        bg={embedBg}
        expiry={embedExpiry}
        canCopy={!!publicId && !!embedHtml.trim() && (!isProtected || !!token || !!embedEt)}
        published={!!publicId}
        isProtected={isProtected}
        hasToken={!!token || !!embedEt}
        onSetToken={(tok) => setToken(tok)}
        onGenerateEt={generateEmbedEt}
        et={embedEt}
        onChange={(opts) => { const { width, height, theme, bg, expiry } = (opts as any); if (typeof width === 'number') setEmbedWidth(width); if (typeof height === 'number') setEmbedHeight(height); if (theme) setEmbedTheme(theme as any); if (bg) setEmbedBg(bg as any); if (typeof expiry === 'number') setEmbedExpiry(expiry) }}
      />
      {toast && (
        <div className="fixed top-16 right-6 z-[60] px-3 py-2 rounded-md bg-[hsl(var(--card))] text-[hsl(var(--foreground))] shadow-card border">
          {toast}
        </div>
      )}
    </div>
    </Suspense>
  )
}

// Lightweight dialog components (portals) for viewing JSON/SQL
function ViewerDialog({ open, title, onClose, children, copyText }: { open: boolean; title: string; onClose: () => void; children: React.ReactNode; copyText?: string }) {
  if (!open || typeof window === 'undefined') return null
  return createPortal(
    <div className="fixed inset-0 z-[1000]">
      <div className="absolute inset-0 bg-black/40" onClick={onClose} />
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[760px] max-w-[95vw] max-h-[85vh] overflow-hidden rounded-lg border bg-card p-4 shadow-none">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <button
              className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]"
              disabled={!copyText}
              onClick={() => { try { copyText && navigator.clipboard?.writeText(copyText) } catch {} }}
            >Copy Raw</button>
            <div className="text-sm font-medium">{title}</div>
          </div>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onClose}>✕</button>
        </div>
        <div className="min-h-[320px] max-h-[70vh] overflow-auto">
          {children}
        </div>
      </div>
    </div>,
    document.body
  )
}

function JsonViewer({ data }: { data: any }) {
  const theme = (typeof document !== 'undefined' && document.documentElement.classList.contains('dark')) ? githubDarkTheme : githubLightTheme
  return (
    <div className="min-h-[320px] max-h-[65vh] overflow-auto">
      <JsonEditor data={data} setData={() => { /* read-only */ }} theme={theme} collapse={3} showCollectionCount rootFontSize={12} />
    </div>
  )
}

function EmbedDialog({ open, onClose, code, onCodeChange, urlPreview, width, height, theme, bg, expiry, canCopy, published, isProtected, hasToken, onSetToken, onGenerateEt, et, onChange }: {
  open: boolean
  onClose: () => void
  code: string
  onCodeChange: (s: string) => void
  urlPreview: string
  width: number
  height: number
  theme: 'light'|'dark'
  bg: 'default'|'transparent'
  expiry: number
  canCopy: boolean
  published: boolean
  isProtected: boolean
  hasToken: boolean
  onSetToken: (tok: string) => void
  onGenerateEt: () => void
  et?: string
  onChange: (opts: { width?: number; height?: number; theme?: 'light'|'dark'; bg?: 'default'|'transparent'; expiry?: number }) => void
}) {
  const [tab, setTab] = useState<'code'|'preview'>('code')
  if (!open || typeof window === 'undefined') return null
  const codeSrc = (() => { try { const m = code.match(/src\s*=\s*"([^"]+)"/i); return m ? m[1] : '' } catch { return '' } })()
  const codeW = (() => { try { const m = code.match(/width\s*=\s*"(\d+)"/i); return m ? Number(m[1]) : width } catch { return width } })()
  const codeH = (() => { try { const m = code.match(/height\s*=\s*"(\d+)"/i); return m ? Number(m[1]) : height } catch { return height } })()
  const expiryDays = Math.max(0, Math.round((expiry || 0) / (24*60*60)))
  const protectedNoToken = !!published && !!isProtected && !hasToken
  return createPortal(
    <div className="fixed inset-0 z-[1000]">
      <div className="absolute inset-0 bg-black/40" onClick={onClose} />
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[920px] max-w-[95vw] max-h-[85vh] overflow-hidden rounded-lg border bg-card p-4 shadow-none">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <div className="text-sm font-medium">Embed Widget</div>
            <div className="ml-3 inline-flex rounded-md border overflow-hidden text-xs">
              <button type="button" className={`px-2 py-1 ${tab==='code'?'bg-[hsl(var(--muted))]':''}`} onClick={() => setTab('code')}>Code</button>
              <button type="button" className={`px-2 py-1 ${tab==='preview'?'bg-[hsl(var(--muted))]':''}`} onClick={() => setTab('preview')}>Preview</button>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]"
              onClick={() => { try { code && canCopy && !protectedNoToken && navigator.clipboard?.writeText(code) } catch {} }}
              disabled={!canCopy || protectedNoToken}
            >Copy</button>
            <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onClose}>✕</button>
          </div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-7 gap-3 mb-3">
          <label className="text-sm">Width<input type="number" className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={width} onChange={(e)=> onChange({ width: Math.max(100, Math.min(4000, Number(e.target.value)||0)), height, theme, bg })} /></label>
          <label className="text-sm">Height<input type="number" className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={height} onChange={(e)=> onChange({ width, height: Math.max(80, Math.min(3000, Number(e.target.value)||0)), theme, bg })} /></label>
          <label className="text-sm">Theme<select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={theme} onChange={(e)=> onChange({ width, height, theme: (e.target.value as 'light'|'dark'), bg })}><option value="dark">Dark</option><option value="light">Light</option></select></label>
          <label className="text-sm">Background<select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={bg} onChange={(e)=> onChange({ width, height, theme, bg: (e.target.value as 'default'|'transparent') })}><option value="default">Default</option><option value="transparent">Transparent</option></select></label>
          <label className="text-sm">Expires in (days)<input type="number" className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={expiryDays} onChange={(e)=> {
            const days = Math.max(0, Math.min(30, Number(e.target.value)||0))
            onChange({ expiry: days * 24 * 60 * 60 })
          }} /></label>
          <div className="flex items-end">
            <button type="button" className="w-full h-8 text-xs px-2 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)] disabled:opacity-60 disabled:cursor-not-allowed" onClick={onGenerateEt} disabled={!published}>
              Generate token
            </button>
          </div>
          {isProtected && (
            <label className="text-sm">Token (preview)
              <input type="password" className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="Paste token to preview/copy" onChange={(e)=> onSetToken(e.target.value)} />
            </label>
          )}
        </div>
        {!published && (
          <div className="mb-2 text-[11px] text-muted-foreground">Publishing is required to generate a public embed link. Copy is disabled until published.</div>
        )}
        {protectedNoToken && (
          <div className="mb-2 text-[11px] text-red-500">Protected link requires a token. Enter a token in Publish to preview/copy.</div>
        )}
        <div className="min-h-[360px] max-h-[60vh] overflow-auto">
          {tab === 'code' ? (
            <textarea className="w-full min-h-[320px] font-mono text-xs leading-5 p-2 rounded-md border bg-[hsl(var(--secondary)/0.4)]" value={code} onChange={(e)=> onCodeChange(e.target.value)} />
          ) : (
            <div className="rounded-md border bg-background p-2 overflow-auto">
              {protectedNoToken ? (
                <div className="text-xs text-center text-muted-foreground py-12">Protected link requires a token. Enter a token in Publish to preview/copy.</div>
              ) : (codeSrc || urlPreview) ? (
                <iframe src={(codeSrc || urlPreview)} width={codeW} height={codeH} style={{ border: 0, width: codeW, height: codeH }} loading="lazy" referrerPolicy="no-referrer" />
              ) : (
                <div className="text-xs text-muted-foreground">No preview.</div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>,
    document.body
  )
}
