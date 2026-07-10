"use client"

import { useEffect, useMemo, useRef, useState } from 'react'
import { useTranslations } from 'next-intl'
import { useRouter } from 'next/navigation'
import { Card, Title, Text, Select, SelectItem } from '@tremor/react'
import * as Tabs from '@radix-ui/react-tabs'
import CreateDashboardDialog from '@/components/dashboards/CreateDashboardDialog'
import DashboardCard from '@/components/dashboards/DashboardCard'
import ImportMappingDialog from '@/components/dashboards/ImportMappingDialog'
import { Api, type DashboardListItem, type FavoriteOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { useProgressToast } from '@/components/providers/ProgressToastProvider'
import { Button, EmptyState } from '@/components/ui'
import { RiSearchLine, RiDashboardLine, RiAddLine } from '@remixicon/react'
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
  const t = useTranslations('pages')
  const { user } = useAuth()
  const { env } = useEnvironment()
  const isAdmin = String(user?.role || '').toLowerCase() === 'admin'
  const router = useRouter()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [items, setItems] = useState<DashboardListItem[]>([])
  const { notify } = useProgressToast()
  const setToast = (m: string) => { if (m) notify(m, /fail|error|invalid|no valid|isn't published|فشل|تعذّر|خطأ|غير منشورة|صالحة/i.test(m) ? 'error' : 'success') }
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
        if (!cancelled) setError(e?.message || t('dashboardsMine.toasts.loadFailed'))
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
    setToast(t('common.toasts.unpublished'))
    window.setTimeout(() => setToast(''), 1600)
  }
  const onDelete = async (d: DashboardListItem) => {
    await Api.deleteDashboard(d.id, user?.id)
    setItems((prev) => prev.filter((x) => x.id !== d.id))
    try { window.dispatchEvent(new CustomEvent('sidebar-counts-refresh')) } catch {}
    setToast(t('common.toasts.deleted'))
    window.setTimeout(() => setToast(''), 1600)
  }
  const onCopyLink = async (d: DashboardListItem) => {
    try {
      // Prefer server status to know if link is protected and get the current publicId
      const status = await Api.getPublishStatus(d.id)
      const baseId = status?.publicId || d.publicId
      if (!baseId) { setToast(t('common.toasts.notPublished')); window.setTimeout(() => setToast(''), 1600); return }
      const base = ((env.publicDomain && env.publicDomain.trim()) ? env.publicDomain : (typeof window !== 'undefined' ? window.location.origin : '')).replace(/\/$/, '')
      let url = `${base}/v/${baseId}`
      if (status?.protected) {
        try {
          const token = localStorage.getItem(`dash_pub_token_${d.id}`)
          if (token) url += `?token=${encodeURIComponent(token)}`
        } catch {}
      }
      const ok = await copyToClipboard(url)
      setToast(ok ? t('common.toasts.linkCopied') : t('common.toasts.copyFailed'))
    } catch {
      const baseId = d.publicId
      if (!baseId) { setToast(t('common.toasts.notPublished')); window.setTimeout(() => setToast(''), 1600); return }
      const base = ((env.publicDomain && env.publicDomain.trim()) ? env.publicDomain : (typeof window !== 'undefined' ? window.location.origin : '')).replace(/\/$/, '')
      const url = `${base}/v/${baseId}`
      const ok = await copyToClipboard(url)
      setToast(ok ? t('common.toasts.linkCopied') : t('common.toasts.copyFailed'))
    }
    window.setTimeout(() => setToast(''), 1600)
  }
  const onDuplicate = async (d: DashboardListItem) => {
    try {
      const src = await Api.getDashboard(d.id, user?.id)
      await Api.saveDashboard({ name: `${d.name} (copy)`, userId: user?.id || 'dev_user', definition: src.definition })
      // Refresh list to include the new dashboard
      const next = await Api.listDashboards(user?.id || 'dev_user')
      setItems(next || [])
      try { window.dispatchEvent(new CustomEvent('sidebar-counts-refresh')) } catch {}
      setToast(t('common.toasts.duplicated'))
      window.setTimeout(() => setToast(''), 1600)
    } catch (e: any) {
      setToast(e?.message || t('dashboardsMine.toasts.duplicateFailed'))
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
        if (widgetsWithCustomCols.length > 0) {
          widgetsWithCustomCols.forEach((w: any) => {
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
      setToast(t('common.toasts.exported'))
      window.setTimeout(() => setToast(''), 1600)
    } catch (e: any) {
      setToast(e?.message || t('dashboardsMine.toasts.exportFailed'))
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
      setToast(next ? t('common.toasts.favorited') : t('common.toasts.removedFromFavorites'))
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
            const res = await Api.importDatasources(unmappedDatasources, user?.id || undefined)
            // Merge only the auto-generated IDs (for unmapped datasources)
            finalDsIdMap = { ...datasourceIdMap, ...(res?.idMap || {}) }
          } catch (err) {
            console.error('Datasource import failed:', err)
            // Continue with dashboard import even if datasources fail
          }
        } else {
        }
        
        // For mapped datasources, merge transforms (custom columns, joins, etc.) into target datasources
        for (const ds of datasources) {
          const targetDsId = datasourceIdMap[ds.id]
          if (targetDsId && ds.options?.transforms) {
            try {
              
              // Fetch current target datasource
              const targetDs = await Api.getDatasource(targetDsId, user?.id || undefined)
              if (!targetDs) continue
              
              // Parse current options
              const currentOptions = targetDs.options || {}
              const currentTransforms = (currentOptions.transforms || {}) as any
              
              // Clone transforms from export
              const importedTransforms = JSON.parse(JSON.stringify(ds.options.transforms)) as any
              
              // Update scope.table in custom columns, joins, and transforms based on tableNameMap
              if (tableNameMap && Object.keys(tableNameMap).length > 0) {
                // Update custom columns scope
                if (Array.isArray(importedTransforms.customColumns)) {
                  importedTransforms.customColumns.forEach((col: any) => {
                    if (col.scope?.table && tableNameMap[col.scope.table]) {
                      col.scope.table = tableNameMap[col.scope.table]
                    }
                  })
                }
                
                // Update transforms scope
                if (Array.isArray(importedTransforms.transforms)) {
                  importedTransforms.transforms.forEach((t: any) => {
                    if (t.scope?.table && tableNameMap[t.scope.table]) {
                      t.scope.table = tableNameMap[t.scope.table]
                    }
                  })
                }
                
                // Update joins scope
                if (Array.isArray(importedTransforms.joins)) {
                  importedTransforms.joins.forEach((j: any) => {
                    if (j.scope?.table && tableNameMap[j.scope.table]) {
                      j.scope.table = tableNameMap[j.scope.table]
                    }
                  })
                }
              }
              
              // Merge transforms (imported transforms override existing ones with same name/scope)
              const mergedTransforms = { ...currentTransforms }
              
              // Merge custom columns
              const existingCustomCols = currentTransforms.customColumns || []
              const importedCustomCols = importedTransforms.customColumns || []
              const customColsMap = new Map()
              existingCustomCols.forEach((col: any) => {
                const key = `${col.name}__${col.scope?.table || ''}`
                customColsMap.set(key, col)
              })
              importedCustomCols.forEach((col: any) => {
                const key = `${col.name}__${col.scope?.table || ''}`
                customColsMap.set(key, col) // Override
              })
              mergedTransforms.customColumns = Array.from(customColsMap.values())
              
              // Merge transforms
              const existingTransforms = currentTransforms.transforms || []
              const importedTransformsList = importedTransforms.transforms || []
              const transformsMap = new Map()
              existingTransforms.forEach((t: any) => {
                const key = `${t.name}__${t.scope?.table || ''}`
                transformsMap.set(key, t)
              })
              importedTransformsList.forEach((t: any) => {
                const key = `${t.name}__${t.scope?.table || ''}`
                transformsMap.set(key, t) // Override
              })
              mergedTransforms.transforms = Array.from(transformsMap.values())
              
              // Merge joins
              const existingJoins = currentTransforms.joins || []
              const importedJoins = importedTransforms.joins || []
              const joinsMap = new Map()
              existingJoins.forEach((j: any) => {
                const key = `${j.targetTable}__${j.scope?.table || ''}`
                joinsMap.set(key, j)
              })
              importedJoins.forEach((j: any) => {
                const key = `${j.targetTable}__${j.scope?.table || ''}`
                joinsMap.set(key, j) // Override
              })
              mergedTransforms.joins = Array.from(joinsMap.values())
              
              // Update target datasource with merged transforms
              await Api.updateDatasource(targetDsId, {
                options: {
                  ...currentOptions,
                  transforms: mergedTransforms
                }
              })
              
              if (mergedTransforms.customColumns && mergedTransforms.customColumns.length > 0) {
                mergedTransforms.customColumns.forEach((col: any) => {
                })
              }
            } catch (err) {
              console.error(`[Import] Failed to merge transforms for datasource ${ds.name}:`, err)
              // Continue anyway
            }
          }
        }
      }
      
      // Log import summary for debugging
      dashboards.forEach((dash: any) => {
        const widgets = Object.values((dash.definition as any)?.widgets || {})
        const widgetsWithCustomCols = widgets.filter((w: any) => Array.isArray(w?.customColumns) && w.customColumns.length > 0)
        if (widgetsWithCustomCols.length > 0) {
          widgetsWithCustomCols.forEach((w: any) => {
            w.customColumns.forEach((col: any) => {
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
      
      setToast(t('dashboardsMine.toasts.imported', { count: dashboards.length }))
      window.setTimeout(() => setToast(''), 1600)
    } catch (e: any) {
      setToast(e?.message || t('dashboardsMine.toasts.importFailed'))
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
            <Title className="text-foreground">{t('dashboardsMine.title')}</Title>
            <Text className="mt-0 text-muted-foreground">{t('dashboardsMine.subtitle')}</Text>
          </div>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              size="sm"
              variant="primary"
              onClick={() => { try { window.dispatchEvent(new CustomEvent('open-create-dashboard')) } catch {} }}
            >
              {t('dashboardsMine.buildNew')}
            </Button>
            {isAdmin && (
              <>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
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
                      setToast(e?.message || t('dashboardsMine.toasts.exportFailed')); window.setTimeout(() => setToast(''), 2000)
                    }
                  }}
                >
                  {t('dashboardsMine.exportAll')}
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={busyImport}
                  onClick={() => fileRef.current?.click()}
                >
                  {busyImport ? t('dashboardsMine.importing') : t('dashboardsMine.importJson')}
                </Button>
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
                      setToast(t('dashboardsMine.toasts.noValid')); window.setTimeout(() => setToast(''), 2000)
                    }
                  } catch (e: any) {
                    setToast(e?.message || t('dashboardsMine.toasts.importFailed')); window.setTimeout(() => setToast(''), 2000)
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
            <Tabs.Trigger value="all" className="pb-2 px-1 mr-4 font-medium border-b-2 border-transparent transition-colors hover:border-[hsl(var(--primary)/0.4)] data-[state=active]:border-[hsl(var(--primary))]">
              <span className="text-gray-500 dark:text-gray-400 data-[state=active]:text-[hsl(var(--primary-deep))] data-[state=active]:dark:text-[hsl(var(--primary))]">{t('dashboardsMine.tabAll')}</span>
              <span className="ml-2 hidden rounded-tremor-small bg-tremor-background px-2 py-1 text-xs font-semibold tabular-nums ring-1 ring-inset ring-tremor-ring data-[state=active]:text-tremor-content-emphasis dark:bg-dark-tremor-background dark:ring-dark-tremor-ring data-[state=active]:dark:text-dark-tremor-content-emphasis sm:inline-flex">{filteredAll.length}</span>
            </Tabs.Trigger>
            <Tabs.Trigger value="published" className="pb-2 px-1 mr-4 font-medium border-b-2 border-transparent transition-colors hover:border-[hsl(var(--primary)/0.4)] data-[state=active]:border-[hsl(var(--primary))]">
              <span className="text-gray-500 dark:text-gray-400 data-[state=active]:text-[hsl(var(--primary-deep))] data-[state=active]:dark:text-[hsl(var(--primary))]">{t('dashboardsMine.tabPublished')}</span>
              <span className="ml-2 hidden rounded-tremor-small bg-tremor-background px-2 py-1 text-xs font-semibold tabular-nums ring-1 ring-inset ring-tremor-ring data-[state=active]:text-tremor-content-emphasis dark:bg-dark-tremor-background dark:ring-dark-tremor-ring data-[state=active]:dark:text-dark-tremor-content-emphasis sm:inline-flex">{filteredPublished.length}</span>
            </Tabs.Trigger>
          </Tabs.List>
          <Tabs.Content value="all" className="px-3 pb-3 pt-0">
            <div className="flex items-center py-2 gap-3">
              <div className="flex items-center gap-2">
                <label htmlFor="searchDashAll" className="text-sm text-gray-600 dark:text-gray-300">{t('common.search')}</label>
                <input id="searchDashAll"
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder={t('dashboardsMine.searchPlaceholder')}
                  className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]"
                />
              </div>
              <div className="ml-auto flex items-center gap-2 text-sm shrink-0">
                <span className="whitespace-nowrap min-w-[84px]">{t('common.perPage')}</span>
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
              {loading && <Text>{t('common.loading')}</Text>}
              {!loading && filteredAll.length === 0 && (
                query.trim()
                  ? <EmptyState icon={<RiSearchLine className="h-7 w-7" />} title={t('common.noMatches')} hint={t('dashboardsMine.noMatchesHint')} />
                  : <EmptyState icon={<RiDashboardLine className="h-7 w-7" />} title={t('dashboardsMine.noDashboardsTitle')} hint={t('dashboardsMine.noDashboardsHint')} primary={{ label: t('dashboardsMine.newDashboard'), icon: <RiAddLine className="h-4 w-4" />, onClick: () => router.push('/builder' as any) }} />
              )}
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
                  {t('common.showing', { from: pageAll * pageSize + 1, to: Math.min((pageAll + 1) * pageSize, filteredAll.length), total: filteredAll.length })}
                </span>
                <div className="flex items-center gap-2">
                  <Button size="sm" variant="outline" disabled={pageAll <= 0} onClick={() => setPageAll((p) => Math.max(0, p - 1))}>{t('common.prev')}</Button>
                  <span>{t('common.pageOf', { current: pageAll + 1, total: totalPagesAll })}</span>
                  <Button size="sm" variant="outline" disabled={pageAll >= totalPagesAll - 1} onClick={() => setPageAll((p) => Math.min(totalPagesAll - 1, p + 1))}>{t('common.next')}</Button>
                </div>
              </div>
            )}
          </Tabs.Content>
          <Tabs.Content value="published" className="px-3 pb-3 pt-0">
              <div className="flex items-center py-2 gap-3">
                <div className="flex items-center gap-2">
                  <label htmlFor="searchDashPub" className="text-sm text-gray-600 dark:text-gray-300">{t('common.search')}</label>
                  <input id="searchDashPub"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder={t('dashboardsMine.searchPlaceholder')}
                    className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]"
                  />
                </div>
                <div className="ml-auto flex items-center gap-2 text-sm shrink-0">
                  <span>{t('common.perPage')}</span>
                  <Select value={String(pageSize)} onValueChange={(v) => setPageSize(parseInt(v || '8') || 8)} className="rounded-md min-w-[76px]">
                    <SelectItem value="6">6</SelectItem>
                    <SelectItem value="8">8</SelectItem>
                    <SelectItem value="12">12</SelectItem>
                    <SelectItem value="24">24</SelectItem>
                  </Select>
                </div>
              </div>
              <div className={`space-y-4 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}` }>
                {loading && <Text>{t('common.loading')}</Text>}
                {!loading && filteredPublished.length === 0 && (
                  query.trim()
                    ? <EmptyState icon={<RiSearchLine className="h-7 w-7" />} title={t('common.noMatches')} hint={t('dashboardsMine.noPublishedMatchesHint')} />
                    : <EmptyState icon={<RiDashboardLine className="h-7 w-7" />} title={t('dashboardsMine.noPublishedTitle')} hint={t('dashboardsMine.noPublishedHint')} />
                )}
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
                    {t('common.showing', { from: pagePub * pageSize + 1, to: Math.min((pagePub + 1) * pageSize, filteredPublished.length), total: filteredPublished.length })}
                  </span>
                  <div className="flex items-center gap-2">
                    <Button size="sm" variant="outline" disabled={pagePub <= 0} onClick={() => setPagePub((p) => Math.max(0, p - 1))}>{t('common.prev')}</Button>
                    <span>{t('common.pageOf', { current: pagePub + 1, total: totalPagesPub })}</span>
                    <Button size="sm" variant="outline" disabled={pagePub >= totalPagesPub - 1} onClick={() => setPagePub((p) => Math.min(totalPagesPub - 1, p + 1))}>{t('common.next')}</Button>
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
            <Dialog.Title className="text-lg font-semibold">{t('common.deleteDialog.title')}</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mt-1">
              {t('common.deleteDialog.desc', { name: confirmDeleteFor?.name || '' })}
            </Dialog.Description>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild>
                <Button type="button" size="sm" variant="outline">{t('common.cancel')}</Button>
              </Dialog.Close>
              <Button
                type="button"
                size="sm"
                variant="danger"
                disabled={delBusy}
                onClick={async () => {
                  if (!confirmDeleteFor) return
                  setDelBusy(true)
                  try { await onDelete(confirmDeleteFor) } finally { setDelBusy(false); setConfirmDeleteFor(null) }
                }}
              >{delBusy ? t('common.deleting') : t('common.delete')}</Button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {/* Publish / Share Dialog */}
      <Dialog.Root open={!!publishFor} onOpenChange={(v) => { if (!v) setPublishFor(null) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/20" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[560px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">{t('common.publishDialog.title')}</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mt-1">
              {t('common.publishDialog.desc')}
            </Dialog.Description>
            <div className="mt-4 space-y-4">
              {/* Mode selector */}
              <div className="space-y-2">
                <label className="flex items-center gap-2 text-sm">
                  <input type="radio" name="pubmode" checked={pubMode==='public'} onChange={() => setPubMode('public')} />
                  <span>{t('common.publishDialog.publicReadOnly')}</span>
                </label>
                <label className="flex items-center gap-2 text-sm">
                  <input type="radio" name="pubmode" checked={pubMode==='user'} onChange={() => setPubMode('user')} />
                  <span>{t('common.publishDialog.shareWithUser')}</span>
                </label>
              </div>

              {/* Public controls */}
              {pubMode === 'public' && (
                <div className="space-y-3">
                  <label className="text-sm block">
                    {t('common.publishDialog.tokenOptional')}
                    <input
                      className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                      placeholder={t('common.publishDialog.tokenPlaceholder')}
                      value={pubToken}
                      onChange={(e) => setPubToken(e.target.value)}
                    />
                  </label>
                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      variant="outline"
                      type="button"
                      onClick={() => setPubToken(genUDID())}
                    >
                      {t('common.publishDialog.generateToken')}
                    </Button>
                    <Button
                      size="sm"
                      variant="primary"
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
                          setToast(t('common.toasts.published'))
                          window.setTimeout(() => setToast(''), 1600)
                        } catch (e: any) {
                          setToast(e?.message || t('dashboardsMine.toasts.publishFailed'))
                          window.setTimeout(() => setToast(''), 2000)
                        } finally {
                          setPubBusy(false)
                        }
                      }}
                    >
                      {t('common.publishDialog.saveGenerateLink')}
                    </Button>
                  </div>
                  {!!pubLink && (
                    <div className="flex items-center gap-2 text-xs text-muted-foreground break-all">
                      <span className="font-mono flex-1">{pubLink}</span>
                      <Button
                        size="sm"
                        variant="outline"
                        type="button"
                        onClick={async () => {
                          const ok = await copyToClipboard(pubLink)
                          setToast(ok ? t('common.toasts.linkCopied') : t('common.toasts.copyFailed'))
                          window.setTimeout(() => setToast(''), 1600)
                        }}
                      >{t('common.copy')}</Button>
                    </div>
                  )}
                </div>
              )}

              {/* Share-with-user controls */}
              {pubMode === 'user' && (
                <div className="space-y-3">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <label className="text-sm block">
                      {t('common.publishDialog.shareWithUserLabel')}
                      <input
                        className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                        placeholder="user@example.com"
                        value={shareUser}
                        onChange={(e) => setShareUser(e.target.value)}
                      />
                    </label>
                    <div>
                      <div className="text-sm mb-1">{t('common.publishDialog.permission')}</div>
                      <label className="flex items-center gap-2 text-sm">
                        <input type="radio" name="perm" checked={sharePerm==='ro'} onChange={() => setSharePerm('ro')} />
                        <span>{t('common.publishDialog.readOnly')}</span>
                      </label>
                      <label className="flex items-center gap-2 text-sm mt-1">
                        <input type="radio" name="perm" checked={sharePerm==='rw'} onChange={() => setSharePerm('rw')} />
                        <span>{t('common.publishDialog.readWrite')}</span>
                      </label>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      variant="primary"
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
                          setToast(t('common.toasts.sharedWithUser'))
                          window.setTimeout(() => setToast(''), 1600)
                          setPublishFor(null)
                        } catch (e: any) {
                          setToast(e?.message || t('dashboardsMine.toasts.shareFailed'))
                          window.setTimeout(() => setToast(''), 2000)
                        } finally {
                          setPubBusy(false)
                        }
                      }}
                    >
                      {t('common.publishDialog.shareDashboard')}
                    </Button>
                  </div>
                </div>
              )}
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild>
                <Button type="button" size="sm" variant="outline">{t('common.close')}</Button>
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
