"use client"

import { useEffect, useMemo, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { Card, Title, Text } from '@tremor/react'
import * as Dialog from '@radix-ui/react-dialog'
import { RiCheckLine } from '@remixicon/react'
import { Api, type DashboardListItem, type CollectionItemOut, type DashboardOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import DashboardCard from '@/components/dashboards/DashboardCard'

// Removed local DashCard in favor of shared DashboardCard

export default function HomeWorkspacePage() {
  const { user } = useAuth()
  const router = useRouter()
  const { env } = useEnvironment()

  // Continue working
  const [last, setLast] = useState<DashboardListItem | null>(null)
  // Favourites / Recent / Collections
  const [favs, setFavs] = useState<DashboardListItem[]>([])
  const [recent, setRecent] = useState<DashboardListItem[]>([])
  const [collab, setCollab] = useState<CollectionItemOut[]>([])
  const [toast, setToast] = useState<string>('')

  const lastKey = useMemo(() => `lastDashId:${user?.id || 'dev_user'}`, [user?.id])

  // Lazy counts for horizontal lists
  const [limFav, setLimFav] = useState(6)
  const [limRecent, setLimRecent] = useState(6)
  const [limCollab, setLimCollab] = useState(6)
  const favEndRef = useRef<HTMLDivElement | null>(null)
  const recEndRef = useRef<HTMLDivElement | null>(null)
  const colEndRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    // Continue working on: per-user last dashboard, guarded by ownership (unless admin)
    try {
      const stored = localStorage.getItem(lastKey)
      if (stored) {
        Api.getDashboard(stored, user?.id).then(async (d: DashboardOut) => {
          // Only show if it belongs to the current user
          if (!user?.id || d.userId !== user.id) {
            try { localStorage.removeItem(lastKey); localStorage.removeItem('dashboardId') } catch {}
            setLast(null); return
          }
          let pub = false
          let pid: string | undefined = undefined
          try {
            const s = await Api.getPublishStatus(d.id)
            pub = !!s?.publicId
            pid = s?.publicId || undefined
          } catch {}
          const li: DashboardListItem = {
            id: d.id,
            name: d.name,
            userId: d.userId,
            createdAt: d.createdAt,
            updatedAt: d.createdAt,
            published: pub,
            publicId: pid,
            widgetsCount: Object.keys(d.definition?.widgets || {}).length,
            tablesCount: 0,
            datasourceCount: 0,
          }
          setLast(li)
        }).catch(() => setLast(null))
      } else {
        setLast(null)
      }
    } catch { setLast(null) }
  }, [lastKey, user?.id, user?.role])

  useEffect(() => {
    let cancelled = false
    async function run() {
      const uid = user?.id
      try {
        // Recent dashboards
        const list = await Api.listDashboards(uid || undefined)
        if (!cancelled) setRecent((list || []).sort((a, b) => (new Date(b.updatedAt || b.createdAt).getTime()) - (new Date(a.updatedAt || a.createdAt).getTime())))
      } catch { if (!cancelled) setRecent([]) }

      try {
        if (!uid) throw new Error('no user')
        // Collections (user collaborations/ownership from shared list)
        const items = await Api.listCollectionItems(uid)
        if (!cancelled) setCollab(items || [])
      } catch { if (!cancelled) setCollab([]) }

      try {
        if (!uid) throw new Error('no user')
        const favRes = await Api.listFavorites(uid)
        if (!cancelled) {
          const mapped = (favRes || []).map((f) => ({
            id: f.dashboardId,
            name: f.name || 'Favorite',
            userId: f.userId,
            createdAt: f.updatedAt || new Date().toISOString(),
            updatedAt: f.updatedAt || undefined,
            published: false,
            publicId: undefined,
            widgetsCount: 0,
            tablesCount: 0,
            datasourceCount: 0,
          } as DashboardListItem))
          setFavs(mapped)
        }
      } catch { if (!cancelled) setFavs([]) }
    }
    void run()
    return () => { cancelled = true }
  }, [user?.id])

  // Horizontal lazy loaders via IntersectionObserver
  useEffect(() => {
    const mk = (ref: React.RefObject<HTMLDivElement | null>, cb: () => void) => {
      const el = ref.current
      if (!el) return
      const io = new IntersectionObserver((entries) => {
        entries.forEach((e) => { if (e.isIntersecting) cb() })
      }, { root: el.parentElement?.parentElement || null, threshold: 0.2 })
      io.observe(el)
      return () => io.disconnect()
    }
    const cleanups: Array<(() => void) | void> = []
    cleanups.push(mk(favEndRef, () => setLimFav((n) => Math.min((favs.length || 0), n + 6))))
    cleanups.push(mk(recEndRef, () => setLimRecent((n) => Math.min((recent.length || 0), n + 6))))
    cleanups.push(mk(colEndRef, () => setLimCollab((n) => Math.min((collab.length || 0), n + 6))))
    return () => { cleanups.forEach((fn) => { try { fn && fn() } catch {} }) }
  }, [favs.length, recent.length, collab.length])

  const onOpen = (d: DashboardListItem) => {
    try { localStorage.setItem(lastKey, d.id); localStorage.setItem('dashboardId', d.id) } catch {}
    router.push((`/builder?id=${encodeURIComponent(d.id)}`) as any)
  }
  const onOpenPublic = async (d: DashboardListItem) => {
    try {
      const status = await Api.getPublishStatus(d.id)
      const baseId = status?.publicId || d.publicId
      if (!baseId) throw new Error('no public id')
      let url = `/v/${baseId}`
      if (status?.protected) {
        try {
          const t = localStorage.getItem(`dash_pub_token_${d.id}`)
          if (t) url += `?token=${encodeURIComponent(t)}`
        } catch {}
      }
      router.push(url as `/v/${string}`)
    } catch {
      if (d.publicId) router.push(`/v/${d.publicId}` as `/v/${string}`)
    }
  }

  const favIds = useMemo(() => new Set(favs.map((f) => f.id)), [favs])
  const toggleFavorite = async (d: DashboardListItem, next: boolean) => {
    setFavs((prev) => {
      const isFav = prev.some((x) => x.id === d.id)
      if (next && !isFav) return [d, ...prev]
      if (!next && isFav) return prev.filter((x) => x.id !== d.id)
      return prev
    })
    try {
      if (user?.id) {
        if (next) await Api.addFavorite(user.id, d.id)
        else await Api.removeFavorite(user.id, d.id)
      }
      setToast(next ? 'Favorited' : 'Removed from favorites')
      window.setTimeout(() => setToast(''), 1600)
    } catch { /* ignore, best-effort */ }
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
      ta.focus(); ta.select()
      const ok = document.execCommand('copy')
      document.body.removeChild(ta)
      return ok
    } catch { return false }
  }

  // Dashboards context actions (minimal best-effort wiring)
  const onDuplicate = async (d: DashboardListItem) => {
    try {
      const src = await Api.getDashboard(d.id, user?.id)
      const res = await Api.saveDashboard({ name: `${d.name} (copy)`, userId: user?.id || 'dev_user', definition: src.definition })
      // Prepend to recents
      setRecent((prev) => [{ ...res, published: false, publicId: undefined, datasourceCount: d.datasourceCount, tablesCount: d.tablesCount, widgetsCount: d.widgetsCount }, ...prev])
      setToast('Duplicated'); window.setTimeout(() => setToast(''), 1600)
    } catch {}
  }
  // Publish / Share dialog state
  const [publishFor, setPublishFor] = useState<DashboardListItem | null>(null)
  const [pubToken, setPubToken] = useState<string>('')
  const [pubBusy, setPubBusy] = useState<boolean>(false)
  const [pubLink, setPubLink] = useState<string>('')
  const [pubMode, setPubMode] = useState<'public' | 'user'>('public')
  const [sharePerm, setSharePerm] = useState<'ro' | 'rw'>('ro')
  const [shareUser, setShareUser] = useState<string>('')
  const onPublishOpen = (d: DashboardListItem) => {
    setPublishFor(d); setPubToken(''); setPubLink(''); setPubMode('public'); setSharePerm('ro'); setShareUser('')
  }
  const onUnpublish = async (d: DashboardListItem) => {
    try {
      await Api.unpublishDashboard(d.id, user?.id)
      setRecent((prev) => prev.map((x) => x.id === d.id ? { ...x, published: false, publicId: null } : x))
      setFavs((prev) => prev.map((x) => x.id === d.id ? { ...x, published: false, publicId: null } : x))
      setToast('Unpublished'); window.setTimeout(() => setToast(''), 1600)
    } catch {}
  }
  // Delete confirmation
  const [confirmDeleteFor, setConfirmDeleteFor] = useState<DashboardListItem | null>(null)
  const [delBusy, setDelBusy] = useState(false)
  const confirmDelete = async () => {
    if (!confirmDeleteFor) return
    setDelBusy(true)
    try {
      await Api.deleteDashboard(confirmDeleteFor.id, user?.id)
      setRecent((prev) => prev.filter((x) => x.id !== confirmDeleteFor.id))
      setFavs((prev) => prev.filter((x) => x.id !== confirmDeleteFor.id))
      setToast('Deleted'); window.setTimeout(() => setToast(''), 1600)
      setConfirmDeleteFor(null)
    } finally { setDelBusy(false) }
  }
  const onCopyLink = async (d: DashboardListItem) => {
    try {
      // Always ask server for current status/publicId and protection
      const status = await Api.getPublishStatus(d.id)
      const baseId = status?.publicId || d.publicId
      if (!baseId) throw new Error('no public id')
      const base = ((env.publicDomain && env.publicDomain.trim()) ? env.publicDomain : (typeof window !== 'undefined' ? window.location.origin : '')).replace(/\/$/, '')
      let url = `${base}/v/${baseId}`
      if (status?.protected) {
        let t: string | null = null
        try { t = localStorage.getItem(`dash_pub_token_${d.id}`) } catch {}
        if (!t && typeof window !== 'undefined') {
          const entered = window.prompt('Enter the access token to include in the link')
          if (entered && entered.trim()) {
            t = entered.trim()
            try { localStorage.setItem(`dash_pub_token_${d.id}`, t) } catch {}
          }
        }
        if (t) url += `?token=${encodeURIComponent(t)}`
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
      window.setTimeout(() => setToast(''), 1600)
    }
  }

  return (
    <div className="space-y-6 overflow-x-hidden min-w-0 w-full">
      <Card className="p-0 bg-[hsl(var(--background))] min-w-0 w-full overflow-hidden">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--card))] border-b border-[hsl(var(--border))] min-w-0">
          <div>
            <Title className="text-gray-500 dark:text-white">Welcome back, {user?.name || 'there'}</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">Pick up where you left off or explore your recent work</Text>
          </div>
        </div>

        {/* Continue Working */}
        {last && user?.id && last.userId === user.id && (
          <div className="px-3 py-3 min-w-0 max-w-full">
            <div className="mb-2 text-[15px] font-medium text-gray-700 dark:text-gray-200 flex items-center justify-between min-w-0 max-w-full">
              <span>Continue working on</span>
            </div>
            <div className="overflow-x-auto no-scrollbar w-full">
              <div className="flex gap-3">{/* single card but keep style consistent */}
                <DashboardCard d={last} onOpenAction={onOpen} onOpenPublicAction={onOpenPublic} isFavorite={favIds.has(last.id)} onToggleFavoriteAction={toggleFavorite} showMenu context="dashboard" onEditAction={onOpen} onDuplicateAction={onDuplicate} onPublishOpenAction={onPublishOpen} onUnpublishAction={onUnpublish} onDeleteAction={(d) => setConfirmDeleteFor(d)} onCopyLinkAction={onCopyLink} />
              </div>
            </div>
          </div>
        )}

        {/* Favourites */}
        <div className="px-3 py-3 min-w-0 max-w-full">
          <div className="mb-2 text-[15px] font-medium text-gray-700 dark:text-gray-200 flex flex-wrap items-center justify-between gap-2 min-w-0 max-w-full">
            <span>Favourites</span>
            <Link href="/dashboards/mine" className="shrink-0 text-xs px-2 py-1 rounded-md border hover:bg-muted">See all</Link>
          </div>
          <div className="overflow-x-auto no-scrollbar w-full">
            <div className="flex gap-3">
              {favs.slice(0, limFav).map((d) => (
                <DashboardCard key={`fav:${d.id}`} d={d} onOpenAction={onOpen} onOpenPublicAction={onOpenPublic} isFavorite={favIds.has(d.id)} onToggleFavoriteAction={toggleFavorite} showMenu context="dashboard" onEditAction={onOpen} onDuplicateAction={onDuplicate} onPublishOpenAction={onPublishOpen} onUnpublishAction={onUnpublish} onDeleteAction={() => setConfirmDeleteFor(d)} onCopyLinkAction={onCopyLink} />
              ))}
              <div ref={favEndRef} className="min-w-[1px]" />
            </div>
          </div>
        </div>

        {/* Recent Dashboards */}
        <div className="px-3 py-3 min-w-0 max-w-full">
          <div className="mb-2 text-[15px] font-medium text-gray-700 dark:text-gray-200 flex flex-wrap items-center justify-between gap-2 min-w-0 max-w-full">
            <span>Recent Dashboards</span>
            <Link href="/dashboards/mine" className="shrink-0 text-xs px-2 py-1 rounded-md border hover:bg-muted">See all</Link>
          </div>
          <div className="overflow-x-auto no-scrollbar w-full">
            <div className="flex gap-3">
              {recent.slice(0, limRecent).map((d) => (
                <DashboardCard key={d.id} d={d} onOpenAction={onOpen} onOpenPublicAction={onOpenPublic} isFavorite={favIds.has(d.id)} onToggleFavoriteAction={toggleFavorite} showMenu context="dashboard" onEditAction={onOpen} onDuplicateAction={onDuplicate} onPublishOpenAction={onPublishOpen} onUnpublishAction={onUnpublish} onDeleteAction={() => setConfirmDeleteFor(d)} onCopyLinkAction={onCopyLink} />
              ))}
              <div ref={recEndRef} className="min-w-[1px]" />
            </div>
          </div>
        </div>

        {/* Recent Collections */}
        <div className="px-3 py-3 min-w-0 max-w-full">
          <div className="mb-2 text-[15px] font-medium text-gray-700 dark:text-gray-200 flex flex-wrap items-center justify-between gap-2 min-w-0 max-w-full">
            <span>Recent Collections</span>
            <Link href="/dashboards/shared" className="shrink-0 text-xs px-2 py-1 rounded-md border hover:bg-muted">See all</Link>
          </div>
          <div className="overflow-x-auto no-scrollbar w-full">
            <div className="flex gap-3">
              {collab.slice(0, limCollab).map((it) => {
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
                  <DashboardCard key={`${it.collectionId}:${it.dashboardId}`} d={d} onOpenAction={onOpen} onOpenPublicAction={onOpenPublic} isFavorite={favIds.has(d.id)} onToggleFavoriteAction={toggleFavorite} showMenu context="collection" badgeMode="permission" permission={(it.permission as 'ro'|'rw') || 'ro'} sharedBy={it.ownerName || it.ownerId || undefined} sharedAt={it.addedAt} onEditAction={onOpen} onRemoveFromCollectionAction={async () => { if (!user?.id) return; await Api.removeFromCollection(user.id, it.collectionId, it.dashboardId); setCollab((prev) => prev.filter((x) => !(x.collectionId === it.collectionId && x.dashboardId === it.dashboardId))) }} />
                )
              })}
              <div ref={colEndRef} className="min-w-[1px]" />
            </div>
          </div>
        </div>
      </Card>
      {/* Toast */}
      {!!toast && (
        <div className="fixed top-6 right-6 z-[100] flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-3 text-[14px] font-medium text-white">
          <RiCheckLine className="w-5 h-5" />
          <span>{toast}</span>
        </div>
      )}

      {/* Delete confirmation */}
      <Dialog.Root open={!!confirmDeleteFor} onOpenChange={(v) => { if (!v) setConfirmDeleteFor(null) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/20" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[520px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">Delete dashboard?</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mt-1">This action cannot be undone. This will permanently delete "{confirmDeleteFor?.name}".</Dialog.Description>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild><button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Cancel</button></Dialog.Close>
              <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-red-50 text-red-600" disabled={delBusy} onClick={confirmDelete}>{delBusy ? 'Deleting…' : 'Delete'}</button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {/* Publish / Share Dialog */}
      <Dialog.Root open={!!publishFor} onOpenChange={(v) => { if (!v) setPublishFor(null) }}>
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
                    <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={pubBusy} onClick={async () => {
                      if (!publishFor) return; setPubBusy(true); try {
                        const res = await Api.setPublishToken(publishFor.id, pubToken || undefined, user?.id)
                        setRecent((prev) => prev.map((x) => x.id === publishFor.id ? { ...x, published: true, publicId: res.publicId } : x))
                        setFavs((prev) => prev.map((x) => x.id === publishFor.id ? { ...x, published: true, publicId: res.publicId } : x))
                        // Persist token locally for copy-link convenience
                        try {
                          const key = `dash_pub_token_${publishFor.id}`
                          if (pubToken) localStorage.setItem(key, pubToken)
                          else localStorage.removeItem(key)
                        } catch {}
                        const base = ((env.publicDomain && env.publicDomain.trim()) ? env.publicDomain : (typeof window !== 'undefined' ? window.location.origin : '')).replace(/\/$/, '')
                        const url = `${base}/v/${res.publicId}${pubToken ? `?token=${encodeURIComponent(pubToken)}` : ''}`
                        setPubLink(url); setToast('Published'); window.setTimeout(() => setToast(''), 1600)
                      } finally { setPubBusy(false) }
                    }}>Save & generate link</button>
                  </div>
                  {!!pubLink && (
                    <div className="flex items-center gap-2 text-xs text-muted-foreground break-all">
                      <span className="font-mono flex-1">{pubLink}</span>
                      <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" type="button" onClick={async () => { await copyToClipboard(pubLink); setToast('Link copied'); window.setTimeout(() => setToast(''), 1600) }}>Copy</button>
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
                      if (!publishFor || !shareUser.trim()) return; setPubBusy(true); try {
                        // Reuse collection API to add to user collection
                        const me = user?.name || user?.id || 'Someone'
                        await Api.addToCollection(shareUser.trim(), { userId: shareUser.trim(), dashboardId: publishFor.id, sharedBy: me, dashboardName: publishFor.name, permission: sharePerm })
                        setToast('Shared with user'); window.setTimeout(() => setToast(''), 1600); setPublishFor(null)
                      } finally { setPubBusy(false) }
                    }}>Share dashboard</button>
                  </div>
                </div>
              )}
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild><button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Close</button></Dialog.Close>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </div>
  )
}
