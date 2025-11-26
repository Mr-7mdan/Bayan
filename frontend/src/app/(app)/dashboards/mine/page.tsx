"use client"

import { useEffect, useMemo, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Card, Title, Text, Select, SelectItem } from '@tremor/react'
import * as Tabs from '@radix-ui/react-tabs'
import CreateDashboardDialog from '@/components/dashboards/CreateDashboardDialog'
import DashboardCard from '@/components/dashboards/DashboardCard'
import ImportMappingDialog from '@/components/dashboards/ImportMappingDialog'
import { Api, type DashboardListItem, type FavoriteOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { RiCheckLine } from '@remixicon/react'
import * as Dialog from '@radix-ui/react-dialog'

// (no local timeAgo – DashboardCard renders updated/created info)

// Generate a UDID-like random token (8-4-4-4-12 hex)
function genUDID(): string {
  const bytes = new Uint8Array(16)
  crypto.getRandomValues(bytes)
  const toHex = (n: number) => n.toString(16).padStart(2, '0')
  // Set version 4 and variant (RFC 4122 style)
  bytes[6] = (bytes[6] & 0x0f) | 0x40
  bytes[8] = (bytes[8] & 0x3f) | 0x80
  const hex = Array.from(bytes, toHex).join('')
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-${hex.slice(12,16)}-${hex.slice(16,20)}-${hex.slice(20)}`
}

async function copyToClipboard(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard && (window.isSecureContext ?? location.protocol === 'https:')) {
      await navigator.clipboard.writeText(text)
      return true
    }
  } catch { /* fall back */ }
  try {
    const ta = document.createElement('textarea')
    ta.value = text
    ta.style.position = 'fixed'
    ta.style.left = '-9999px'
    document.body.appendChild(ta)
    ta.focus()
    ta.select()
    const ok = document.execCommand('copy')
    document.body.removeChild(ta)
    return ok
  } catch {
    return false
  }
}

// Using shared DashboardCard

export default function MyDashboardsPage() {
  const { user } = useAuth()
  const { env } = useEnvironment()
  const isAdmin = String(user?.role || '').toLowerCase() === 'admin'
  const router = useRouter()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [items, setItems] = useState<DashboardListItem[]>([])
  const [toast, setToast] = useState<string>('')
  // Favorites (as ids set for quick lookup)
  const [favIdsSet, setFavIdsSet] = useState<Set<string>>(new Set())
  const [confirmDeleteFor, setConfirmDeleteFor] = useState<DashboardListItem | null>(null)
  const [delBusy, setDelBusy] = useState(false)
  const [publishFor, setPublishFor] = useState<DashboardListItem | null>(null)
  const [pubToken, setPubToken] = useState<string>('')
  const [pubBusy, setPubBusy] = useState<boolean>(false)
  const [pubLink, setPubLink] = useState<string>('')
  const [pubMode, setPubMode] = useState<'public' | 'user'>('public')
  const [sharePerm, setSharePerm] = useState<'ro' | 'rw'>('ro')
  const [shareUser, setShareUser] = useState<string>('')
  const [tabIndex, setTabIndex] = useState(0)
  const prevTabIndex = useRef(0)
  const [slideDir, setSlideDir] = useState<'left' | 'right'>('right')
  // Export/Import
  const fileRef = useRef<HTMLInputElement | null>(null)
  const [busyImport, setBusyImport] = useState(false)
  const [showMappingDialog, setShowMappingDialog] = useState(false)
  const [pendingImportData, setPendingImportData] = useState<{ dashboards: any[], datasources: any[] } | null>(null)
  // Search & pagination
  const [query, setQuery] = useState('')
  const [pageSize, setPageSize] = useState(8)
  const [pageAll, setPageAll] = useState(0)
  const [pagePub, setPagePub] = useState(0)

  useEffect(() => {
    let cancelled = false
    async function run() {
      setLoading(true)
      setError(null)
      try {
        const res = await Api.listDashboards(user?.id || 'dev_user')
        if (!cancelled) setItems(res || [])
      } catch (e: any) {
        if (!cancelled) setError(e?.message || 'Failed to load dashboards')
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    void run()
    return () => { cancelled = true }
  }, [user?.id])

  // Load favorites ids for quick lookup
  useEffect(() => {
    (async () => {
      try {
        if (!user?.id) return
        const favs = await Api.listFavorites(user.id)
        setFavIdsSet(new Set((favs || []).map((f: FavoriteOut) => f.dashboardId)))
      } catch {}
    })()
  }, [user?.id])

  

  const published = useMemo(() => items.filter((d) => d.published), [items])
  const filteredAll = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return items
    return items.filter((d) => d.name.toLowerCase().includes(q) || (d.userId || '').toLowerCase().includes(q))
  }, [items, query])
  const filteredPublished = useMemo(() => {
    const base = published
    const q = query.trim().toLowerCase()
    if (!q) return base
    return base.filter((d) => d.name.toLowerCase().includes(q) || (d.userId || '').toLowerCase().includes(q))
  }, [published, query])
  const totalPagesAll = Math.max(1, Math.ceil(filteredAll.length / pageSize))
  const totalPagesPub = Math.max(1, Math.ceil(filteredPublished.length / pageSize))
  const visibleAll = filteredAll.slice(pageAll * pageSize, pageAll * pageSize + pageSize)
  const visiblePublished = filteredPublished.slice(pagePub * pageSize, pagePub * pageSize + pageSize)
  useEffect(() => { setPageAll(0); setPagePub(0) }, [query, pageSize])
  useEffect(() => { if (pageAll > totalPagesAll - 1) setPageAll(0) }, [totalPagesAll])
  useEffect(() => { if (pagePub > totalPagesPub - 1) setPagePub(0) }, [totalPagesPub])

  // Actions
  const onEdit = (d: DashboardListItem) => {
    try { localStorage.setItem('dashboardId', d.id) } catch {}
    router.push((`/builder?id=${encodeURIComponent(d.id)}`) as any)
  }
  const onPublishOpen = (d: DashboardListItem) => {
    setPublishFor(d)
    setPubToken('')
    setPubLink('')
    setPubMode('public')
    setSharePerm('ro')
    setShareUser('')
  }
  const onUnpublish = async (d: DashboardListItem) => {
    await Api.unpublishDashboard(d.id, user?.id)
    setItems((prev) => prev.map((x) => x.id === d.id ? { ...x, published: false, publicId: null } : x))
    setToast('Unpublished')
    window.setTimeout(() => setToast(''), 1600)
  }
  const onDelete = async (d: DashboardListItem) => {
    await Api.deleteDashboard(d.id, user?.id)
    setItems((prev) => prev.filter((x) => x.id !== d.id))
    try { window.dispatchEvent(new CustomEvent('sidebar-counts-refresh')) } catch {}
    setToast('Deleted')
    window.setTimeout(() => setToast(''), 1600)
  }
  const onCopyLink = async (d: DashboardListItem) => {
    try {
      // Prefer server status to know if link is protected and get the current publicId
      const status = await Api.getPublishStatus(d.id)
      const baseId = status?.publicId || d.publicId
      if (!baseId) { setToast("This dashboard isn't published"); window.setTimeout(() => setToast(''), 1600); return }
      const base = ((env.publicDomain && env.publicDomain.trim()) ? env.publicDomain : (typeof window !== 'undefined' ? window.location.origin : '')).replace(/\/$/, '')
      let url = `${base}/v/${baseId}`
      if (status?.protected) {
        try {
          const token = localStorage.getItem(`dash_pub_token_${d.id}`)
          if (token) url += `?token=${encodeURIComponent(token)}`
        } catch {}
      }
      const ok = await copyToClipboard(url)
      setToast(ok ? 'Link copied' : 'Copy failed. Please copy manually.')
    } catch {
      const baseId = d.publicId
      if (!baseId) { setToast("This dashboard isn't published"); window.setTimeout(() => setToast(''), 1600); return }
      const base = ((env.publicDomain && env.publicDomain.trim()) ? env.publicDomain : (typeof window !== 'undefined' ? window.location.origin : '')).replace(/\/$/, '')
      const url = `${base}/v/${baseId}`
      const ok = await copyToClipboard(url)
      setToast(ok ? 'Link copied' : 'Copy failed. Please copy manually.')
    }
    window.setTimeout(() => setToast(''), 1600)
  }
  const onDuplicate = async (d: DashboardListItem) => {
    try {
      const src = await Api.getDashboard(d.id)
      await Api.saveDashboard({ name: `${d.name} (copy)`, userId: user?.id || 'dev_user', definition: src.definition })
      // Refresh list to include the new dashboard
      const next = await Api.listDashboards(user?.id || 'dev_user')
      setItems(next || [])
      try { window.dispatchEvent(new CustomEvent('sidebar-counts-refresh')) } catch {}
      setToast('Duplicated')
      window.setTimeout(() => setToast(''), 1600)
    } catch (e: any) {
      setToast(e?.message || 'Failed to duplicate')
      window.setTimeout(() => setToast(''), 2000)
    }
  }
  const onExport = async (d: DashboardListItem) => {
    try {
      const data = await Api.exportDashboard(d.id, true, true, user?.id || undefined)
      
      // Log export summary for debugging
      if (data.dashboards && data.dashboards.length > 0) {
        const dash = data.dashboards[0]
        const widgets = Object.values((dash.definition as any)?.widgets || {})
        const widgetsWithCustomCols = widgets.filter((w: any) => Array.isArray(w?.customColumns) && w.customColumns.length > 0)
        console.log('[Export] Dashboard:', dash.name)
        console.log('[Export] Total widgets:', widgets.length)
        console.log('[Export] Widgets with custom columns:', widgetsWithCustomCols.length)
        if (widgetsWithCustomCols.length > 0) {
          widgetsWithCustomCols.forEach((w: any) => {
            console.log('[Export]   -', w.title, ':', w.customColumns.length, 'custom columns')
          })
        }
      }
      
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
      const a = document.createElement('a')
      const ts = new Date()
      const name = `${d.name.replace(/[^a-z0-9_-]/gi, '_')}-export-${ts.getFullYear()}${String(ts.getMonth()+1).padStart(2,'0')}${String(ts.getDate()).padStart(2,'0')}-${String(ts.getHours()).padStart(2,'0')}${String(ts.getMinutes()).padStart(2,'0')}.json`
      a.href = URL.createObjectURL(blob)
      a.download = name
      document.body.appendChild(a)
      a.click()
      a.remove()
      setToast('Exported')
      window.setTimeout(() => setToast(''), 1600)
    } catch (e: any) {
      setToast(e?.message || 'Export failed')
      window.setTimeout(() => setToast(''), 2000)
    }
  }

  // Favorite toggle with optimistic UI and toast
  const toggleFavorite = async (d: DashboardListItem, next: boolean) => {
    setFavIdsSet((prev) => {
      const s = new Set(prev)
      if (next) s.add(d.id)
      else s.delete(d.id)
      return s
    })
    try {
      if (user?.id) {
        if (next) await Api.addFavorite(user.id, d.id)
        else await Api.removeFavorite(user.id, d.id)
      }
      setToast(next ? 'Favorited' : 'Removed from favorites')
      window.setTimeout(() => setToast(''), 1600)
    } catch {
      // best-effort; UI already updated
    }
  }

  // Handle import confirmation after mapping
  const handleImportConfirm = async (datasourceIdMap: Record<string, string>, tableNameMap: Record<string, string>) => {
    if (!pendingImportData) return
    
    setShowMappingDialog(false)
    setBusyImport(true)
    
    try {
      const { dashboards, datasources } = pendingImportData
      
      // Import only datasources that user did NOT map to existing ones
      let finalDsIdMap = { ...datasourceIdMap }
      if (datasources && datasources.length > 0) {
        // Filter out datasources that are already mapped by the user
        const unmappedDatasources = datasources.filter((ds: any) => !datasourceIdMap[ds.id])
        
        if (unmappedDatasources.length > 0) {
          try {
            console.log('[Import] Creating new datasources for unmapped:', unmappedDatasources.map((ds: any) => ds.name))
            const res = await Api.importDatasources(unmappedDatasources, user?.id || undefined)
            // Merge only the auto-generated IDs (for unmapped datasources)
            finalDsIdMap = { ...datasourceIdMap, ...(res?.idMap || {}) }
          } catch (err) {
            console.error('Datasource import failed:', err)
            // Continue with dashboard import even if datasources fail
          }
        } else {
          console.log('[Import] All datasources already mapped, skipping datasource import')
        }
      }
      
      // Log import summary for debugging
      dashboards.forEach((dash: any) => {
        const widgets = Object.values((dash.definition as any)?.widgets || {})
        const widgetsWithCustomCols = widgets.filter((w: any) => Array.isArray(w?.customColumns) && w.customColumns.length > 0)
        console.log('[Import] Dashboard:', dash.name)
        console.log('[Import] Total widgets:', widgets.length)
        console.log('[Import] Widgets with custom columns:', widgetsWithCustomCols.length)
        if (widgetsWithCustomCols.length > 0) {
          widgetsWithCustomCols.forEach((w: any) => {
            console.log('[Import]   -', w.title, ':', w.customColumns.length, 'custom columns')
            w.customColumns.forEach((col: any) => {
              console.log('[Import]     •', col.name, '=', col.formula)
            })
          })
        }
      })
      
      // Import dashboards with mappings
      await Api.importDashboards({ 
        dashboards, 
        datasourceIdMap: Object.keys(finalDsIdMap).length > 0 ? finalDsIdMap : null, 
        tableNameMap: Object.keys(tableNameMap).length > 0 ? tableNameMap : null 
      }, user?.id || undefined)
      
      // Refresh dashboard list
      const next = await Api.listDashboards(user?.id || 'dev_user')
      setItems(next || [])
      try { window.dispatchEvent(new CustomEvent('sidebar-counts-refresh')) } catch {}
      
      setToast(`Imported ${dashboards.length} dashboard${dashboards.length > 1 ? 's' : ''}`)
      window.setTimeout(() => setToast(''), 1600)
    } catch (e: any) {
      setToast(e?.message || 'Import failed')
      window.setTimeout(() => setToast(''), 2000)
    } finally {
      setBusyImport(false)
      setPendingImportData(null)
    }
  }

  return (
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">My Dashboards</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">View/Create/Edit/Publish/Unpublish your dashboards from here</Text>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              className="inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-500 dark:text-gray-400 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]"
              onClick={() => { try { window.dispatchEvent(new CustomEvent('open-create-dashboard')) } catch {} }}
            >
              Build New Dashboard
            </button>
            {isAdmin && (
              <>
                <button
                  type="button"
                  className="inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-500 dark:text-gray-400 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]"
                  onClick={async () => {
                    try {
                      const data = await Api.exportDashboards({ userId: user?.id || 'dev_user', includeDatasources: true, includeSyncTasks: true, actorId: user?.id || undefined })
                      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
                      const a = document.createElement('a')
                      const ts = new Date()
                      const name = `dashboards-export-${ts.getFullYear()}${String(ts.getMonth()+1).padStart(2,'0')}${String(ts.getDate()).padStart(2,'0')}-${String(ts.getHours()).padStart(2,'0')}${String(ts.getMinutes()).padStart(2,'0')}.json`
                      a.href = URL.createObjectURL(blob)
                      a.download = name
                      document.body.appendChild(a)
                      a.click()
                      a.remove()
                    } catch (e: any) {
                      setToast(e?.message || 'Export failed'); window.setTimeout(() => setToast(''), 2000)
                    }
                  }}
                >
                  Export All (.json)
                </button>
                <button
                  type="button"
                  className="inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-500 dark:text-gray-400 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50"
                  disabled={busyImport}
                  onClick={() => fileRef.current?.click()}
                >
                  {busyImport ? 'Importing…' : 'Import JSON'}
                </button>
                <input ref={fileRef} type="file" accept="application/json" hidden onChange={async (e) => {
                  const file = e.target.files?.[0]
                  if (!file) return
                  setBusyImport(true)
                  try {
                    const text = await file.text()
                    const json = JSON.parse(text)
                    
                    // Handle both single dashboard and multiple dashboards JSON formats
                    let dashboards: any[] = []
                    let datasources: any[] = []
                    
                    // Check if it's a DashboardExportResponse (multiple dashboards)
                    if (json?.dashboards && Array.isArray(json.dashboards)) {
                      dashboards = json.dashboards
                      datasources = Array.isArray(json?.datasources) ? json.datasources : []
                    }
                    // Check if it's a single dashboard export (has dashboards array with one item)
                    else if (Array.isArray(json) && json.length > 0) {
                      // Array of dashboards
                      dashboards = json
                    }
                    // Check if it's a single dashboard object (has name, definition, etc.)
                    else if (json?.name && json?.definition) {
                      dashboards = [json]
                    }
                    // Fallback: treat entire JSON as dashboard array
                    else {
                      dashboards = []
                    }
                    
                    if (dashboards.length > 0) {
                      // Show mapping dialog
                      setPendingImportData({ dashboards, datasources })
                      setShowMappingDialog(true)
                    } else {
                      setToast('No valid dashboards found in file'); window.setTimeout(() => setToast(''), 2000)
                    }
                  } catch (e: any) {
                    setToast(e?.message || 'Import failed'); window.setTimeout(() => setToast(''), 2000)
                  } finally {
                    setBusyImport(false)
                    try { if (fileRef.current) fileRef.current.value = '' } catch {}
                  }
                }} />
              </>
            )}
          </div>
        </div>
        {error && <div className="px-4 py-2 text-sm text-red-600">{error}</div>}
        <Tabs.Root
          value={tabIndex === 0 ? 'all' : 'published'}
          onValueChange={(v) => {
            const i = v === 'published' ? 1 : 0
            setSlideDir(i > prevTabIndex.current ? 'left' : 'right')
            prevTabIndex.current = i
            setTabIndex(i)
          }}
        >
          <Tabs.List className="px-3 py-1.5 border-b border-[hsl(var(--border))]">
            <Tabs.Trigger value="all" className="pb-2.5 font-medium hover:border-gray-300">
              <span className="text-gray-500 dark:text-gray-400 data-[state=active]:text-gray-900 data-[state=active]:dark:text-white">All Dashboards</span>
              <span className="ml-2 hidden rounded-tremor-small bg-tremor-background px-2 py-1 text-xs font-semibold tabular-nums ring-1 ring-inset ring-tremor-ring data-[state=active]:text-tremor-content-emphasis dark:bg-dark-tremor-background dark:ring-dark-tremor-ring data-[state=active]:dark:text-dark-tremor-content-emphasis sm:inline-flex">{filteredAll.length}</span>
            </Tabs.Trigger>
            <Tabs.Trigger value="published" className="pb-2.5 font-medium hover:border-gray-300">
              <span className="text-gray-500 dark:text-gray-400 data-[state=active]:text-gray-900 data-[state=active]:dark:text-white">Published</span>
              <span className="ml-2 hidden rounded-tremor-small bg-tremor-background px-2 py-1 text-xs font-semibold tabular-nums ring-1 ring-inset ring-tremor-ring data-[state=active]:text-tremor-content-emphasis dark:bg-dark-tremor-background dark:ring-dark-tremor-ring data-[state=active]:dark:text-dark-tremor-content-emphasis sm:inline-flex">{filteredPublished.length}</span>
            </Tabs.Trigger>
          </Tabs.List>
          <Tabs.Content value="all" className="px-3 pb-3 pt-0">
            <div className="flex items-center py-2 gap-3">
              <div className="flex items-center gap-2">
                <label htmlFor="searchDashAll" className="text-sm text-gray-600 dark:text-gray-300">Search</label>
                <input id="searchDashAll"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Search dashboards..."
                  className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]"
                />
              </div>
              <div className="ml-auto flex items-center gap-2 text-sm shrink-0">
                <span className="whitespace-nowrap min-w-[84px]">Per page</span>
                <div className="min-w-[96px] rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                  [&_*]:!border-0 [&_*]:!border-transparent [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!ring-transparent [&_*]:!outline-none [&_*]:!shadow-none
                  [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                  <Select
                    value={String(pageSize)}
                    onValueChange={(v) => setPageSize(parseInt(v || '8') || 8)}
                    className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                  >
                    <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="6">6</SelectItem>
                    <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="8">8</SelectItem>
                    <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="12">12</SelectItem>
                    <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="24">24</SelectItem>
                  </Select>
                </div>
              </div>
            </div>
            <div className={`space-y-4 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}` }>
              {loading && <Text>Loading…</Text>}
              {!loading && filteredAll.length === 0 && <Text>No dashboards match your search.</Text>}
              {!loading && visibleAll.map((d) => (
                <DashboardCard key={d.id} d={d} widthClass="w-full" showMenu context="dashboard"
                  onOpenAction={onEdit} onDuplicateAction={onDuplicate} onPublishOpenAction={onPublishOpen}
                  onUnpublishAction={onUnpublish} onCopyLinkAction={onCopyLink} onExportAction={onExport}
                  onDeleteAction={() => setConfirmDeleteFor(d)}
                  isFavorite={favIdsSet.has(d.id)} onToggleFavoriteAction={toggleFavorite}
                />
              ))}
            </div>
            {!loading && filteredAll.length > 0 && (
              <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
                <span>
                  Showing {pageAll * pageSize + 1}
                  –{Math.min((pageAll + 1) * pageSize, filteredAll.length)} of {filteredAll.length}
                </span>
                <div className="flex items-center gap-2">
                  <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageAll <= 0} onClick={() => setPageAll((p) => Math.max(0, p - 1))}>Prev</button>
                  <span>Page {pageAll + 1} / {totalPagesAll}</span>
                  <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageAll >= totalPagesAll - 1} onClick={() => setPageAll((p) => Math.min(totalPagesAll - 1, p + 1))}>Next</button>
                </div>
              </div>
            )}
          </Tabs.Content>
          <Tabs.Content value="published" className="px-3 pb-3 pt-0">
              <div className="flex items-center py-2 gap-3">
                <div className="flex items-center gap-2">
                  <label htmlFor="searchDashPub" className="text-sm text-gray-600 dark:text-gray-300">Search</label>
                  <input id="searchDashPub"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Search dashboards..."
                    className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]"
                  />
                </div>
                <div className="ml-auto flex items-center gap-2 text-sm shrink-0">
                  <span>Per page</span>
                  <Select value={String(pageSize)} onValueChange={(v) => setPageSize(parseInt(v || '8') || 8)} className="rounded-md min-w-[76px]">
                    <SelectItem value="6">6</SelectItem>
                    <SelectItem value="8">8</SelectItem>
                    <SelectItem value="12">12</SelectItem>
                    <SelectItem value="24">24</SelectItem>
                  </Select>
                </div>
              </div>
              <div className={`space-y-4 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}` }>
                {loading && <Text>Loading…</Text>}
                {!loading && filteredPublished.length === 0 && <Text>No published dashboards match your search.</Text>}
                {!loading && visiblePublished.map((d) => (
                  <DashboardCard key={d.id} d={d} widthClass="w-full" showMenu context="dashboard"
                    onOpenAction={onEdit} onDuplicateAction={onDuplicate} onPublishOpenAction={onPublishOpen}
                    onUnpublishAction={onUnpublish} onCopyLinkAction={onCopyLink} onExportAction={onExport}
                    onDeleteAction={() => setConfirmDeleteFor(d)}
                    isFavorite={favIdsSet.has(d.id)} onToggleFavoriteAction={toggleFavorite}
                  />
                ))}
              </div>
              {!loading && filteredPublished.length > 0 && (
                <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
                  <span>
                    Showing {pagePub * pageSize + 1}
                    –{Math.min((pagePub + 1) * pageSize, filteredPublished.length)} of {filteredPublished.length}
                  </span>
                  <div className="flex items-center gap-2">
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pagePub <= 0} onClick={() => setPagePub((p) => Math.max(0, p - 1))}>Prev</button>
                    <span>Page {pagePub + 1} / {totalPagesPub}</span>
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pagePub >= totalPagesPub - 1} onClick={() => setPagePub((p) => Math.min(totalPagesPub - 1, p + 1))}>Next</button>
                  </div>
                </div>
              )}
          </Tabs.Content>
        </Tabs.Root>
      </Card>
      {/* Create Dashboard Dialog */}
      <CreateDashboardDialog />

      {/* Delete confirmation */}
      <Dialog.Root open={!!confirmDeleteFor} onOpenChange={(v) => { if (!v) setConfirmDeleteFor(null) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/20" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[520px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">Delete dashboard?</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mt-1">
              This action cannot be undone. This will permanently delete
              {` "${confirmDeleteFor?.name || ''}"`}.
            </Dialog.Description>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild>
                <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Cancel</button>
              </Dialog.Close>
              <button
                type="button"
                className="text-sm px-3 py-1.5 rounded-md border hover:bg-red-50 text-red-600 disabled:opacity-50"
                disabled={delBusy}
                onClick={async () => {
                  if (!confirmDeleteFor) return
                  setDelBusy(true)
                  try { await onDelete(confirmDeleteFor) } finally { setDelBusy(false); setConfirmDeleteFor(null) }
                }}
              >{delBusy ? 'Deleting…' : 'Delete'}</button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {/* Toast */}
      {!!toast && (
        <div className="fixed top-6 right-6 z-[100] flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-3 text-[14px] font-medium text-white">
          <RiCheckLine className="w-5 h-5" />
          <span>{toast}</span>
        </div>
      )}

      {/* Publish / Share Dialog */}
      <Dialog.Root open={!!publishFor} onOpenChange={(v) => { if (!v) setPublishFor(null) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/20" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[560px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">Publish or Share</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mt-1">
              Choose whether to publish a public read‑only link, or share with a specific user.
            </Dialog.Description>
            <div className="mt-4 space-y-4">
              {/* Mode selector */}
              <div className="space-y-2">
                <label className="flex items-center gap-2 text-sm">
                  <input type="radio" name="pubmode" checked={pubMode==='public'} onChange={() => setPubMode('public')} />
                  <span>Public version: read‑only</span>
                </label>
                <label className="flex items-center gap-2 text-sm">
                  <input type="radio" name="pubmode" checked={pubMode==='user'} onChange={() => setPubMode('user')} />
                  <span>Share with a user</span>
                </label>
              </div>

              {/* Public controls */}
              {pubMode === 'public' && (
                <div className="space-y-3">
                  <label className="text-sm block">
                    Token (optional)
                    <input
                      className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                      placeholder="Leave empty for public access"
                      value={pubToken}
                      onChange={(e) => setPubToken(e.target.value)}
                    />
                  </label>
                  <div className="flex gap-2">
                    <button
                      className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted"
                      type="button"
                      onClick={() => setPubToken(genUDID())}
                    >
                      Generate secure token
                    </button>
                    <button
                      className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted"
                      type="button"
                      disabled={pubBusy}
                      onClick={async () => {
                        if (!publishFor) return
                        setPubBusy(true)
                        try {
                          const res = await Api.setPublishToken(publishFor.id, pubToken || undefined, user?.id)
                          setItems((prev) => prev.map((x) => x.id === publishFor.id ? { ...x, published: true, publicId: res.publicId } : x))
                          // Persist token locally for copy-link convenience
                          try {
                            const key = `dash_pub_token_${publishFor.id}`
                            if (pubToken) localStorage.setItem(key, pubToken)
                            else localStorage.removeItem(key)
                          } catch {}
                          const base = ((env.publicDomain && env.publicDomain.trim()) ? env.publicDomain : (typeof window !== 'undefined' ? window.location.origin : '')).replace(/\/$/, '')
                          const url = `${base}/v/${res.publicId}${pubToken ? `?token=${encodeURIComponent(pubToken)}` : ''}`
                          setPubLink(url)
                          setToast('Published')
                          window.setTimeout(() => setToast(''), 1600)
                        } catch (e: any) {
                          setToast(e?.message || 'Failed to publish')
                          window.setTimeout(() => setToast(''), 2000)
                        } finally {
                          setPubBusy(false)
                        }
                      }}
                    >
                      Save & generate link
                    </button>
                  </div>
                  {!!pubLink && (
                    <div className="flex items-center gap-2 text-xs text-muted-foreground break-all">
                      <span className="font-mono flex-1">{pubLink}</span>
                      <button
                        className="text-xs px-2 py-1 rounded-md border hover:bg-muted"
                        type="button"
                        onClick={async () => {
                          const ok = await copyToClipboard(pubLink)
                          setToast(ok ? 'Link copied' : 'Copy failed. Please copy manually.')
                          window.setTimeout(() => setToast(''), 1600)
                        }}
                      >Copy</button>
                    </div>
                  )}
                </div>
              )}

              {/* Share-with-user controls */}
              {pubMode === 'user' && (
                <div className="space-y-3">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <label className="text-sm block">
                      Share with user (email or id)
                      <input
                        className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                        placeholder="user@example.com"
                        value={shareUser}
                        onChange={(e) => setShareUser(e.target.value)}
                      />
                    </label>
                    <div>
                      <div className="text-sm mb-1">Permission</div>
                      <label className="flex items-center gap-2 text-sm">
                        <input type="radio" name="perm" checked={sharePerm==='ro'} onChange={() => setSharePerm('ro')} />
                        <span>Read‑only</span>
                      </label>
                      <label className="flex items-center gap-2 text-sm mt-1">
                        <input type="radio" name="perm" checked={sharePerm==='rw'} onChange={() => setSharePerm('rw')} />
                        <span>Read‑write</span>
                      </label>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <button
                      className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted"
                      type="button"
                      disabled={pubBusy || !shareUser.trim()}
                      onClick={async () => {
                        if (!publishFor || !shareUser.trim()) return
                        setPubBusy(true)
                        try {
                          const me = user?.name || user?.id || 'Someone'
                          await Api.addToCollection(shareUser.trim(), {
                            userId: shareUser.trim(),
                            dashboardId: publishFor.id,
                            sharedBy: me,
                            dashboardName: publishFor.name,
                            permission: sharePerm,
                          })
                          setToast('Shared with user')
                          window.setTimeout(() => setToast(''), 1600)
                          setPublishFor(null)
                        } catch (e: any) {
                          setToast(e?.message || 'Failed to share')
                          window.setTimeout(() => setToast(''), 2000)
                        } finally {
                          setPubBusy(false)
                        }
                      }}
                    >
                      Share dashboard
                    </button>
                  </div>
                </div>
              )}
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild>
                <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Close</button>
              </Dialog.Close>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {/* Import Mapping Dialog */}
      {pendingImportData && (
        <ImportMappingDialog
          open={showMappingDialog}
          onClose={() => {
            setShowMappingDialog(false)
            setPendingImportData(null)
          }}
          onConfirm={handleImportConfirm}
          importData={pendingImportData}
          userId={user?.id || 'dev_user'}
        />
      )}
    </div>
  )
}
