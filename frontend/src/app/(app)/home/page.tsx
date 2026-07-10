"use client"

import { useEffect, useMemo, useRef, useState } from 'react'
import { useTranslations } from 'next-intl'
import { useRouter } from 'next/navigation'
import Link from 'next/link'
import { Card, Title, Text } from '@tremor/react'
import * as Dialog from '@radix-ui/react-dialog'
import { RiStarLine, RiFolderSharedLine, RiAddLine, RiRocketLine } from '@remixicon/react'
import { Api, type DashboardListItem, type CollectionItemOut, type DashboardOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { useProgressToast } from '@/components/providers/ProgressToastProvider'
import { Button, EmptyState } from '@/components/ui'
import DashboardCard from '@/components/dashboards/DashboardCard'

// Removed local DashCard in favor of shared DashboardCard

export default function HomeWorkspacePage() {
  const t = useTranslations('pages')
  const { user } = useAuth()
  const router = useRouter()
  const { env } = useEnvironment()

  // Continue working
  const [last, setLast] = useState<DashboardListItem | null>(null)
  // Favourites / Recent / Collections
  const [favs, setFavs] = useState<DashboardListItem[]>([])
  const [recent, setRecent] = useState<DashboardListItem[]>([])
  const [collab, setCollab] = useState<CollectionItemOut[]>([])
  const { notify } = useProgressToast()
  // Route the existing setToast(msg) call sites through the unified toast; '' clears are no-ops (auto-dismiss).
  const setToast = (m: string) => { if (m) notify(m, /fail|error|invalid|isn't published|فشل|تعذّر|خطأ|غير منشورة|صالحة/i.test(m) ? 'error' : 'success') }

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
            name: f.name || t('home.favoriteFallback'),
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
      setToast(next ? t('common.toasts.favorited') : t('common.toasts.removedFromFavorites'))
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
      setToast(t('common.toasts.duplicated')); window.setTimeout(() => setToast(''), 1600)
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
      setToast(t('common.toasts.unpublished')); window.setTimeout(() => setToast(''), 1600)
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
      setToast(t('common.toasts.deleted')); window.setTimeout(() => setToast(''), 1600)
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
        let tok: string | null = null
        try { tok = localStorage.getItem(`dash_pub_token_${d.id}`) } catch {}
        if (!tok && typeof window !== 'undefined') {
          const entered = window.prompt(t('home.enterToken'))
          if (entered && entered.trim()) {
            tok = entered.trim()
            try { localStorage.setItem(`dash_pub_token_${d.id}`, tok) } catch {}
          }
        }
        if (tok) url += `?token=${encodeURIComponent(tok)}`
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
      window.setTimeout(() => setToast(''), 1600)
    }
  }

  return (
    <div className="space-y-6 overflow-x-hidden min-w-0 w-full">
      <Card className="p-0 bg-[hsl(var(--background))] min-w-0 w-full overflow-hidden">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--card))] border-b border-[hsl(var(--border))] min-w-0">
          <div>
            <Title className="text-foreground">{t('home.welcome', { name: user?.name || t('home.fallbackName') })}</Title>
            <Text className="mt-0 text-muted-foreground">{t('home.subtitle')}</Text>
          </div>
        </div>

        {/* First-run hint: no dashboards yet */}
        {recent.length === 0 && (
          <div className="px-3 py-3">
            <div className="rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] p-6">
              <EmptyState
                icon={<RiRocketLine className="h-8 w-8" />}
                title={t('home.emptyFirstTitle')}
                hint={t('home.emptyFirstHint')}
                primary={{ label: t('home.newDashboard'), icon: <RiAddLine className="h-4 w-4" />, onClick: () => router.push('/builder' as any) }}
              />
            </div>
          </div>
        )}

        {/* Continue Working */}
        {last && user?.id && last.userId === user.id && (
          <div className="px-3 py-3 min-w-0 max-w-full">
            <div className="mb-2 text-[15px] font-medium text-gray-700 dark:text-gray-200 flex items-center justify-between min-w-0 max-w-full">
              <span>{t('home.continueWorking')}</span>
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
            <span>{t('home.favourites')}</span>
            <Link href="/dashboards/mine" className="shrink-0 text-xs px-2 py-1 rounded-md border hover:bg-muted">{t('home.seeAll')}</Link>
          </div>
          {favs.length === 0 ? (
            <EmptyState
              icon={<RiStarLine className="h-7 w-7" />}
              title={t('home.noFavouritesTitle')}
              hint={t('home.noFavouritesHint')}
            />
          ) : (
          <div className="overflow-x-auto no-scrollbar w-full">
            <div className="flex gap-3">
              {favs.slice(0, limFav).map((d) => (
                <DashboardCard key={`fav:${d.id}`} d={d} onOpenAction={onOpen} onOpenPublicAction={onOpenPublic} isFavorite={favIds.has(d.id)} onToggleFavoriteAction={toggleFavorite} showMenu context="dashboard" onEditAction={onOpen} onDuplicateAction={onDuplicate} onPublishOpenAction={onPublishOpen} onUnpublishAction={onUnpublish} onDeleteAction={() => setConfirmDeleteFor(d)} onCopyLinkAction={onCopyLink} />
              ))}
              <div ref={favEndRef} className="min-w-[1px]" />
            </div>
          </div>
          )}
        </div>

        {/* Recent Dashboards (first-run hint above covers the empty case) */}
        {recent.length > 0 && (
        <div className="px-3 py-3 min-w-0 max-w-full">
          <div className="mb-2 text-[15px] font-medium text-gray-700 dark:text-gray-200 flex flex-wrap items-center justify-between gap-2 min-w-0 max-w-full">
            <span>{t('home.recentDashboards')}</span>
            <Link href="/dashboards/mine" className="shrink-0 text-xs px-2 py-1 rounded-md border hover:bg-muted">{t('home.seeAll')}</Link>
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
        )}

        {/* Recent Collections */}
        <div className="px-3 py-3 min-w-0 max-w-full">
          <div className="mb-2 text-[15px] font-medium text-gray-700 dark:text-gray-200 flex flex-wrap items-center justify-between gap-2 min-w-0 max-w-full">
            <span>{t('home.recentCollections')}</span>
            <Link href="/dashboards/shared" className="shrink-0 text-xs px-2 py-1 rounded-md border hover:bg-muted">{t('home.seeAll')}</Link>
          </div>
          {collab.length === 0 ? (
            <EmptyState
              icon={<RiFolderSharedLine className="h-7 w-7" />}
              title={t('home.noCollectionsTitle')}
              hint={t('home.noCollectionsHint')}
            />
          ) : (
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
          )}
        </div>
      </Card>

      {/* Delete confirmation */}
      <Dialog.Root open={!!confirmDeleteFor} onOpenChange={(v) => { if (!v) setConfirmDeleteFor(null) }}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/20" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[520px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">{t('common.deleteDialog.title')}</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mt-1">{t('common.deleteDialog.desc', { name: confirmDeleteFor?.name || '' })}</Dialog.Description>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild><Button type="button" size="sm" variant="outline">{t('common.cancel')}</Button></Dialog.Close>
              <Button type="button" size="sm" variant="danger" disabled={delBusy} onClick={confirmDelete}>{delBusy ? t('common.deleting') : t('common.delete')}</Button>
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
            <Dialog.Description className="text-sm text-muted-foreground mt-1">{t('common.publishDialog.desc')}</Dialog.Description>
            <div className="mt-4 space-y-4">
              <div className="space-y-2">
                <label className="flex items-center gap-2 text-sm"><input type="radio" name="pubmode" checked={pubMode==='public'} onChange={() => setPubMode('public')} /><span>{t('common.publishDialog.publicReadOnly')}</span></label>
                <label className="flex items-center gap-2 text-sm"><input type="radio" name="pubmode" checked={pubMode==='user'} onChange={() => setPubMode('user')} /><span>{t('common.publishDialog.shareWithUser')}</span></label>
              </div>
              {pubMode==='public' && (
                <div className="space-y-3">
                  <label className="text-sm block">{t('common.publishDialog.tokenOptional')}
                    <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" placeholder={t('common.publishDialog.tokenPlaceholder')} value={pubToken} onChange={(e) => setPubToken(e.target.value)} />
                  </label>
                  <div className="flex gap-2">
                    <Button size="sm" variant="outline" type="button" onClick={() => setPubToken(crypto.randomUUID())}>{t('common.publishDialog.generateToken')}</Button>
                    <Button size="sm" variant="primary" type="button" disabled={pubBusy} onClick={async () => {
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
                        setPubLink(url); setToast(t('common.toasts.published')); window.setTimeout(() => setToast(''), 1600)
                      } finally { setPubBusy(false) }
                    }}>{t('common.publishDialog.saveGenerateLink')}</Button>
                  </div>
                  {!!pubLink && (
                    <div className="flex items-center gap-2 text-xs text-muted-foreground break-all">
                      <span className="font-mono flex-1">{pubLink}</span>
                      <Button size="sm" variant="outline" type="button" onClick={async () => { await copyToClipboard(pubLink); setToast(t('common.toasts.linkCopied')); window.setTimeout(() => setToast(''), 1600) }}>{t('common.copy')}</Button>
                    </div>
                  )}
                </div>
              )}
              {pubMode==='user' && (
                <div className="space-y-3">
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <label className="text-sm block">{t('common.publishDialog.shareWithUserLabel')}
                      <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" placeholder="user@example.com" value={shareUser} onChange={(e) => setShareUser(e.target.value)} />
                    </label>
                    <div>
                      <div className="text-sm mb-1">{t('common.publishDialog.permission')}</div>
                      <label className="flex items-center gap-2 text-sm"><input type="radio" name="perm" checked={sharePerm==='ro'} onChange={() => setSharePerm('ro')} /><span>{t('common.publishDialog.readOnly')}</span></label>
                      <label className="flex items-center gap-2 text-sm mt-1"><input type="radio" name="perm" checked={sharePerm==='rw'} onChange={() => setSharePerm('rw')} /><span>{t('common.publishDialog.readWrite')}</span></label>
                    </div>
                  </div>
                  <div className="flex gap-2">
                    <Button size="sm" variant="primary" type="button" disabled={pubBusy || !shareUser.trim()} onClick={async () => {
                      if (!publishFor || !shareUser.trim()) return; setPubBusy(true); try {
                        // Reuse collection API to add to user collection
                        const me = user?.name || user?.id || 'Someone'
                        await Api.addToCollection(shareUser.trim(), { userId: shareUser.trim(), dashboardId: publishFor.id, sharedBy: me, dashboardName: publishFor.name, permission: sharePerm })
                        setToast(t('common.toasts.sharedWithUser')); window.setTimeout(() => setToast(''), 1600); setPublishFor(null)
                      } finally { setPubBusy(false) }
                    }}>{t('common.publishDialog.shareDashboard')}</Button>
                  </div>
                </div>
              )}
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild><Button type="button" size="sm" variant="outline">{t('common.close')}</Button></Dialog.Close>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </div>
  )
}
