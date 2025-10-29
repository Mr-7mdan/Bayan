"use client"

import { useEffect, useMemo, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Card, Title, Text, TabGroup, TabList, Tab, TabPanels, TabPanel, Select, SelectItem } from '@tremor/react'
import { Api, type CollectionItemOut, type FavoriteOut, type DashboardListItem } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import DashboardCard from '@/components/dashboards/DashboardCard'
import { RiCheckLine } from '@remixicon/react'
import * as Dialog from '@radix-ui/react-dialog'

// Using shared DashboardCard

export default function CollectionsPage() {
  const { user } = useAuth()
  const router = useRouter()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [items, setItems] = useState<CollectionItemOut[]>([])
  const [toast, setToast] = useState<string>('')
  const [tabIndex, setTabIndex] = useState(0)
  const prevTabIndex = useRef(0)
  const [slideDir, setSlideDir] = useState<'left' | 'right'>('right')
  // Favorites
  const [favIdsSet, setFavIdsSet] = useState<Set<string>>(new Set())
  // Search & pagination
  const [query, setQuery] = useState('')
  const [pageSize, setPageSize] = useState(8)
  const [pageMine, setPageMine] = useState(0)
  const [pageCollab, setPageCollab] = useState(0)

  useEffect(() => {
    let cancelled = false
    async function run() {
      if (!user?.id) return
      setLoading(true)
      setError(null)
      try {
        const res = await Api.listCollectionItems(user.id)
        if (!cancelled) setItems(res || [])
      } catch (e: any) {
        if (!cancelled) setError(e?.message || 'Failed to load collections')
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

  const collaborations = useMemo(() => items.filter((x) => (x.permission || 'ro') === 'rw'), [items])
  const filteredMine = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return items
    return items.filter((it) => it.name.toLowerCase().includes(q)
      || (it.ownerName || '').toLowerCase().includes(q)
      || (it.ownerId || '').toLowerCase().includes(q))
  }, [items, query])
  const filteredCollab = useMemo(() => {
    const base = collaborations
    const q = query.trim().toLowerCase()
    if (!q) return base
    return base.filter((it) => it.name.toLowerCase().includes(q)
      || (it.ownerName || '').toLowerCase().includes(q)
      || (it.ownerId || '').toLowerCase().includes(q))
  }, [collaborations, query])
  const totalPagesMine = Math.max(1, Math.ceil(filteredMine.length / pageSize))
  const totalPagesCollab = Math.max(1, Math.ceil(filteredCollab.length / pageSize))
  const visibleMine = filteredMine.slice(pageMine * pageSize, pageMine * pageSize + pageSize)
  const visibleCollab = filteredCollab.slice(pageCollab * pageSize, pageCollab * pageSize + pageSize)
  useEffect(() => { setPageMine(0); setPageCollab(0) }, [query, pageSize])
  useEffect(() => { if (pageMine > totalPagesMine - 1) setPageMine(0) }, [totalPagesMine])
  useEffect(() => { if (pageCollab > totalPagesCollab - 1) setPageCollab(0) }, [totalPagesCollab])

  const onOpen = (it: CollectionItemOut) => {
    try { localStorage.setItem('dashboardId', it.dashboardId) } catch {}
    router.push((`/builder?id=${encodeURIComponent(it.dashboardId)}`) as any)
  }
  const onOpenPublic = async (it: CollectionItemOut) => {
    try {
      const status = await Api.getPublishStatus(it.dashboardId)
      const baseId = status?.publicId || it.publicId
      if (!baseId) throw new Error('no public id')
      let url = `/v/${baseId}`
      if (status?.protected) {
        try { const t = localStorage.getItem(`dash_pub_token_${it.dashboardId}`); if (t) url += `?token=${encodeURIComponent(t)}` } catch {}
      }
      router.push(url as `/v/${string}`)
    } catch {
      if (it.publicId) router.push(`/v/${it.publicId}` as `/v/${string}`)
      else {
        setToast("This dashboard isn't published")
        window.setTimeout(() => setToast(''), 1600)
      }
    }
  }
  const onRemove = async (it: CollectionItemOut) => {
    await Api.removeFromCollection(user!.id, it.collectionId, it.dashboardId)
    setItems((prev) => prev.filter((x) => !(x.collectionId === it.collectionId && x.dashboardId === it.dashboardId)))
  }

  const toggleFavorite = async (d: DashboardListItem, next: boolean) => {
    setFavIdsSet((prev) => {
      const s = new Set(prev)
      if (next) s.add(d.id); else s.delete(d.id)
      return s
    })
    try { if (user?.id) { if (next) await Api.addFavorite(user.id, d.id); else await Api.removeFavorite(user.id, d.id) } } catch {}
    setToast(next ? 'Favorited' : 'Removed from favorites'); window.setTimeout(() => setToast(''), 1600)
  }

  return (
    <div className="shared-root space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">My Collections</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">View items in your collection</Text>
          </div>
        </div>
        {error && <div className="px-4 py-2 text-sm text-red-600">{error}</div>}
        <TabGroup index={tabIndex} onIndexChange={(i) => { setSlideDir(i > prevTabIndex.current ? 'left' : 'right'); prevTabIndex.current = i; setTabIndex(i); }}>
          <TabList className="px-3 py-1.5 border-b border-[hsl(var(--border))]">
            <Tab className="pb-2.5 font-medium hover:border-gray-300">
              <span className="text-gray-500 dark:text-gray-400 ui-selected:text-gray-900 ui-selected:dark:text-white">My Collections</span>
              <span className="ml-2 hidden rounded-tremor-small bg-tremor-background px-2 py-1 text-xs font-semibold tabular-nums ring-1 ring-inset ring-tremor-ring ui-selected:text-tremor-content-emphasis dark:bg-dark-tremor-background dark:ring-dark-tremor-ring ui-selected:dark:text-dark-tremor-content-emphasis sm:inline-flex">{filteredMine.length}</span>
            </Tab>
            <Tab className="pb-2.5 font-medium hover:border-gray-300">
              <span className="text-gray-500 dark:text-gray-400 ui-selected:text-gray-900 ui-selected:dark:text-white">Collaborations</span>
              <span className="ml-2 hidden rounded-tremor-small bg-tremor-background px-2 py-1 text-xs font-semibold tabular-nums ring-1 ring-inset ring-tremor-ring ui-selected:text-tremor-content-emphasis dark:bg-dark-tremor-background dark:ring-dark-tremor-ring ui-selected:dark:text-dark-tremor-content-emphasis sm:inline-flex">{filteredCollab.length}</span>
            </Tab>
          </TabList>
          <TabPanels className="pt-0">
            <TabPanel className="px-3 pb-3 pt-0">
              <div className="flex items-center py-2 gap-3">
                <div className="flex items-center gap-2">
                  <label htmlFor="searchCollectionsMine" className="text-sm text-gray-600 dark:text-gray-300">Search</label>
                  <input id="searchCollectionsMine"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Search collections..."
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
              <div className={`space-y-4 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}`}>
                {loading && <Text>Loading…</Text>}
                {!loading && filteredMine.length === 0 && <Text>No items match your search.</Text>}
                {!loading && visibleMine.map((it) => {
                  const d: DashboardListItem = {
                    id: it.dashboardId,
                    name: it.name,
                    userId: it.ownerId,
                    createdAt: it.addedAt,
                    updatedAt: it.addedAt,
                    published: it.published,
                    publicId: it.publicId,
                    widgetsCount: 0,
                    tablesCount: 0,
                    datasourceCount: 0,
                  }
                  return (
                    <DashboardCard key={`${it.collectionId}:${it.dashboardId}`} d={d} widthClass="w-full" context="collection" badgeMode="permission" permission={(it.permission as 'ro'|'rw') || 'ro'} sharedBy={it.ownerName || it.ownerId || undefined} sharedAt={it.addedAt} isFavorite={favIdsSet.has(d.id)} onToggleFavoriteAction={toggleFavorite} onOpenAction={() => onOpen(it)} onOpenPublicAction={() => onOpenPublic(it)} onRemoveFromCollectionAction={() => onRemove(it)} />
                  )
                })}
              </div>
              {!loading && filteredMine.length > 0 && (
                <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
                  <span>
                    Showing {pageMine * pageSize + 1}
                    –{Math.min((pageMine + 1) * pageSize, filteredMine.length)} of {filteredMine.length}
                  </span>
                  <div className="flex items-center gap-2">
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageMine <= 0} onClick={() => setPageMine((p) => Math.max(0, p - 1))}>Prev</button>
                    <span>Page {pageMine + 1} / {totalPagesMine}</span>
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageMine >= totalPagesMine - 1} onClick={() => setPageMine((p) => Math.min(totalPagesMine - 1, p + 1))}>Next</button>
                  </div>
                </div>
              )}
            </TabPanel>
            <TabPanel className="px-3 pb-3 pt-0">
              <div className="flex items-center py-2 gap-3">
                <div className="flex items-center gap-2">
                  <label htmlFor="searchCollectionsCollab" className="text-sm text-gray-600 dark:text-gray-300">Search</label>
                  <input id="searchCollectionsCollab"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                    placeholder="Search collaborations..."
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
              <div className={`space-y-4 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}`}>
                {loading && <Text>Loading…</Text>}
                {!loading && filteredCollab.length === 0 && <Text>No collaborations match your search.</Text>}
                {!loading && visibleCollab.map((it) => {
                  const d: DashboardListItem = {
                    id: it.dashboardId,
                    name: it.name,
                    userId: it.ownerId,
                    createdAt: it.addedAt,
                    updatedAt: it.addedAt,
                    published: it.published,
                    publicId: it.publicId,
                    widgetsCount: 0,
                    tablesCount: 0,
                    datasourceCount: 0,
                  }
                  return (
                    <DashboardCard key={`${it.collectionId}:${it.dashboardId}`} d={d} widthClass="w-full" context="collection" badgeMode="permission" permission={(it.permission as 'ro'|'rw') || 'ro'} sharedBy={it.ownerId || undefined} sharedAt={it.addedAt} isFavorite={favIdsSet.has(d.id)} onToggleFavoriteAction={toggleFavorite} onOpenAction={() => onOpen(it)} onOpenPublicAction={() => onOpenPublic(it)} onRemoveFromCollectionAction={() => onRemove(it)} />
                  )
                })}
              </div>
              {!loading && filteredCollab.length > 0 && (
                <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
                  <span>
                    Showing {pageCollab * pageSize + 1}
                    –{Math.min((pageCollab + 1) * pageSize, filteredCollab.length)} of {filteredCollab.length}
                  </span>
                  <div className="flex items-center gap-2">
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageCollab <= 0} onClick={() => setPageCollab((p) => Math.max(0, p - 1))}>Prev</button>
                    <span>Page {pageCollab + 1} / {totalPagesCollab}</span>
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageCollab >= totalPagesCollab - 1} onClick={() => setPageCollab((p) => Math.min(totalPagesCollab - 1, p + 1))}>Next</button>
                  </div>
                </div>
              )}
            </TabPanel>
          </TabPanels>
        </TabGroup>
      </Card>

      {/* Toast */}
      {!!toast && (
        <div className="fixed top-6 right-6 z-[100] flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-3 text-[14px] font-medium text-white">
          <RiCheckLine className="w-5 h-5" />
          <span>{toast}</span>
        </div>
      )}
    </div>
  )
}
