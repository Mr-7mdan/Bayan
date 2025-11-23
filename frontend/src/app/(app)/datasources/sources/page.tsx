"use client"

import { Suspense, useEffect, useMemo, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Card, Title, Text, TabGroup, TabList, Tab, TabPanels, TabPanel, Select, SelectItem } from '@tremor/react'
import { Api, type DatasourceOut, type UserRowOut, type DatasourceShareOut } from '@/lib/api'
import * as Popover from '@radix-ui/react-popover'
import * as Dialog from '@radix-ui/react-dialog'
import { RiBuildingLine, RiMapPin2Line, RiUserLine, RiMore2Line, RiCheckLine } from '@remixicon/react'
import DatasourceDialog, { type DatasourceDialogMode } from '@/components/datasources/DatasourceDialog'
import { useAuth } from '@/components/providers/AuthProvider'

export const dynamic = 'force-dynamic'

function StatusPill({ active }: { active: boolean }) {
  const color = active ? 'bg-emerald-600' : 'bg-gray-400'
  const label = active ? 'Active' : 'Inactive'
  return (
    <span className="inline-flex items-center gap-1.5 whitespace-nowrap rounded-md bg-[hsl(var(--secondary)/0.6)] px-2 py-1 text-[11px] font-medium text-[hsl(var(--muted-foreground))] ring-1 ring-inset ring-[hsl(var(--border))]">
      <span className={`${color} size-2 rounded-full`} aria-hidden={true} />
      {label}
    </span>
  )
}

type DsMeta = { loading?: boolean; error?: string | null; schemas: number; tables: number; views: number; lastUsedAt?: string | null; active: boolean }

function fmt(iso?: string | null) { try { return iso ? new Date(iso).toLocaleString() : '—' } catch { return '—' } }

function SourceRow({ ds, meta, onOpen, onEdit, onDelete, onToggleActive }: { ds: DatasourceOut; meta: DsMeta; onOpen: (ds: DatasourceOut) => void; onEdit: (ds: DatasourceOut) => void; onDelete: (ds: DatasourceOut) => Promise<void>; onToggleActive: (ds: DatasourceOut, next: boolean) => Promise<void> }) {
  const [menuOpen, setMenuOpen] = useState(false)
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [busy, setBusy] = useState<'delete' | null>(null)
  const { user } = useAuth()
  // Share dialog state
  const [shareOpen, setShareOpen] = useState(false)
  const [shareBusy, setShareBusy] = useState(false)
  const [shares, setShares] = useState<DatasourceShareOut[]>([])
  const [users, setUsers] = useState<UserRowOut[]>([])
  const [selectedUserId, setSelectedUserId] = useState<string>('')
  const [perm, setPerm] = useState<'ro'|'rw'>('ro')
  const [shareError, setShareError] = useState<string>('')
  async function refreshShares() {
    try { const s = await Api.listDatasourceShares(ds.id, user?.id); setShares(Array.isArray(s) ? s : []) } catch { setShares([]) }
  }
  async function loadUsers() {
    if (!user?.id) return
    try { const rows = await Api.adminListUsers(user.id); setUsers(rows || []) } catch { setUsers([]) }
  }
  function openShare() {
    setMenuOpen(false)
    setShareOpen(true)
    void refreshShares()
    void loadUsers()
  }
  return (
    <Card className={`group p-2 rounded-2xl border border-[hsl(var(--border))] ring-1 ring-inset ring-[hsl(var(--border))] bg-[hsl(var(--card))] elev-bottom cursor-pointer hover:ring-[hsl(var(--ring))] focus-visible:ring-[hsl(var(--ring))] outline-none ${confirmOpen ? 'pointer-events-none' : ''}`} onClick={() => { if (menuOpen || confirmOpen || shareOpen) return; onOpen(ds) }} role="button" tabIndex={0}>
      <div className={`relative hover-cover rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] p-4 transition-transform duration-150 ring-1 ring-inset ring-[hsl(var(--border))] group-hover:ring-[hsl(var(--ring))] group-hover:border-[hsl(var(--ring))] ${menuOpen ? '' : 'group-hover:-translate-y-[1px]'}`}>
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <h4 className="truncate text-[14px] font-medium text-[hsl(var(--foreground))]">{ds.name}</h4>
            <StatusPill active={!!meta.active} />
            <span className="inline-flex items-center gap-1.5 whitespace-nowrap rounded-md bg-[hsl(var(--secondary)/0.6)] px-2 py-1 text-[11px] font-medium text-[hsl(var(--muted-foreground))] ring-1 ring-inset ring-[hsl(var(--border))]">{ds.type}</span>
          </div>
          <Popover.Root open={menuOpen} onOpenChange={setMenuOpen}>
            <Popover.Trigger asChild>
              <button className="p-1.5 rounded-md hover:bg-[hsl(var(--muted))] focus:outline-none" aria-label="Actions" onClick={(e) => e.stopPropagation()}>
                <RiMore2Line className="w-5 h-5 opacity-80" />
              </button>
            </Popover.Trigger>
            <Popover.Portal>
            <Popover.Content side="bottom" align="end" className="z-50 w-56 rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--popover))] shadow-none p-1">
              <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={(e) => { e.stopPropagation(); setMenuOpen(false); onEdit(ds) }}>Edit</button>
              {(user?.role === 'admin') && (
                <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={(e) => { e.stopPropagation(); openShare() }}>Share with…</button>
              )}
              <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={async (e) => { e.stopPropagation(); setMenuOpen(false); try {
                const data = await Api.exportDatasource(ds.id, true, user?.id)
                const exportPayload = { items: [data] }
                const blob = new Blob([JSON.stringify(exportPayload, null, 2)], { type: 'application/json' })
                const a = document.createElement('a')
                const ts = new Date()
                const safe = ds.name.replace(/[^a-z0-9_-]+/gi, '-').toLowerCase()
                const name = `datasource-${safe}-${ts.getFullYear()}${String(ts.getMonth()+1).padStart(2,'0')}${String(ts.getDate()).padStart(2,'0')}-${String(ts.getHours()).padStart(2,'0')}${String(ts.getMinutes()).padStart(2,'0')}.json`
                a.href = URL.createObjectURL(blob)
                a.download = name
                document.body.appendChild(a)
                a.click()
                a.remove()
              } catch {} }}>Export (.json)</button>
              <button
                className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]"
                onClick={async (e) => {
                  e.stopPropagation();
                  setMenuOpen(false)
                  try {
                    const res = await Api.disposeDatasourceEngine(ds.id)
                    if (res?.disposed) {
                      try { window.alert(`Disposed ${res.target} engine pool`) } catch {}
                    } else {
                      try { window.alert(res?.message || 'No engine disposed') } catch {}
                    }
                  } catch (err: any) {
                    try { window.alert(err?.message || 'Failed to dispose engine pool') } catch {}
                  }
                }}
              >Dispose Engine Pool</button>
              <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={async (e) => { e.stopPropagation(); setMenuOpen(false); await onToggleActive(ds, !meta.active) }}>{meta.active ? 'Deactivate' : 'Activate'}</button>
              <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={(e) => { e.stopPropagation(); setMenuOpen(false); setConfirmOpen(true) }}>Delete</button>
            </Popover.Content>
            </Popover.Portal>
          </Popover.Root>
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-x-6 gap-y-4 text-[13px]">
          <div className="flex items-center space-x-1.5"><RiBuildingLine className="size-5 text-tremor-content-subtle dark:text-dark-tremor-content-subtle" aria-hidden /><p className="text-[hsl(var(--muted-foreground))]">Databases ({meta.schemas})</p></div>
          <div className="flex items-center space-x-1.5"><RiMapPin2Line className="size-5 text-tremor-content-subtle dark:text-dark-tremor-content-subtle" aria-hidden /><p className="text-[hsl(var(--muted-foreground))]">Tables ({meta.tables})</p></div>
          <div className="flex items-center space-x-1.5"><RiUserLine className="size-5 text-tremor-content-subtle dark:text-dark-tremor-content-subtle" aria-hidden /><p className="text-[hsl(var(--muted-foreground))]">Views ({meta.views})</p></div>
        </div>
      </div>
      <div className="px-2 pb-2 pt-4">
        <div className="block sm:flex sm:items-end sm:justify-between">
          <div className="flex items-center gap-2 text-[13px] text-[hsl(var(--muted-foreground))]">
            <span className="inline-block w-3 h-3 rounded-full border-2 border-blue-500" aria-hidden />
            <span>Last used: {fmt(meta.lastUsedAt)}</span>
          </div>
          <p className="mt-2 text-[13px] text-[hsl(var(--muted-foreground))] sm:mt-0">{meta.loading ? 'Analyzing…' : (meta.error ? 'Failed to analyze' : '')}</p>
        </div>
      </div>
      <Dialog.Root open={confirmOpen} onOpenChange={setConfirmOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/40" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[520px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card" onClick={(e) => e.stopPropagation()} onMouseDown={(e) => e.stopPropagation()}>
            <Dialog.Title className="text-lg font-semibold">Delete datasource?</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mt-1">This action cannot be undone. This will permanently delete "{ds.name}".</Dialog.Description>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild><button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Cancel</button></Dialog.Close>
              <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-red-50 text-red-600" disabled={busy === 'delete'} onClick={async () => { setBusy('delete'); try { await onDelete(ds) } finally { setBusy(null); setConfirmOpen(false) } }}>{busy === 'delete' ? 'Deleting…' : 'Delete'}</button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
      {/* Share dialog */}
      <Dialog.Root open={shareOpen} onOpenChange={setShareOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/40" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[560px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card" onClick={(e) => e.stopPropagation()} onMouseDown={(e) => e.stopPropagation()}>
            <Dialog.Title className="text-lg font-semibold">Share datasource</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mt-1">Grant access to another user. Only admins can share.</Dialog.Description>
            {/* Add share */}
            <div className="mt-4 grid grid-cols-[1fr_auto_auto] gap-2 items-end">
              <div>
                <div className="text-xs text-muted-foreground mb-1">User</div>
                <select className="w-full text-sm px-2 py-1.5 rounded-md border bg-background" value={selectedUserId} onChange={(e) => setSelectedUserId(e.target.value)}>
                  <option value="">Select user…</option>
                  {users.filter(u => !shares.some(s => s.userId === u.id)).map(u => (
                    <option key={u.id} value={u.id}>{u.name} ({u.email})</option>
                  ))}
                </select>
              </div>
              <div>
                <div className="text-xs text-muted-foreground mb-1">Permission</div>
                <select className="text-sm px-2 py-1.5 rounded-md border bg-background" value={perm} onChange={(e) => setPerm((e.target.value as 'ro'|'rw') || 'ro')}>
                  <option value="ro">Read‑only</option>
                  <option value="rw">Read‑write</option>
                </select>
              </div>
              <div className="pb-1">
                <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted disabled:opacity-50" disabled={shareBusy || !selectedUserId} onClick={async () => {
                  setShareBusy(true); setShareError('')
                  try { await Api.addDatasourceShare(ds.id, { userId: selectedUserId, permission: perm }, user?.id); setSelectedUserId(''); await refreshShares() } catch (e: any) { setShareError(e?.message || 'Failed') } finally { setShareBusy(false) }
                }}>{shareBusy ? 'Adding…' : 'Add'}</button>
              </div>
            </div>
            {!!shareError && <div className="mt-2 text-sm text-red-600">{shareError}</div>}
            {/* Existing shares */}
            <div className="mt-4">
              <div className="text-sm font-medium mb-1">Shared with</div>
              <div className="rounded-md border divide-y">
                {shares.length === 0 ? (
                  <div className="px-2 py-2 text-sm text-muted-foreground">No shares yet.</div>
                ) : shares.map((s) => (
                  <div key={s.userId} className="px-2 py-2 flex items-center justify-between gap-2">
                    <div className="text-sm">
                      <span className="font-medium">{s.name || s.userId}</span>
                      {s.email && <span className="ml-2 text-muted-foreground">{s.email}</span>}
                      <span className="ml-2 inline-flex items-center px-1.5 py-0.5 rounded border">{s.permission.toUpperCase()}</span>
                    </div>
                    <button className="px-2 py-0.5 rounded-md border hover:bg-muted" onClick={async () => { try { await Api.deleteDatasourceShare(ds.id, s.userId, user?.id); await refreshShares() } catch {} }}>Remove</button>
                  </div>
                ))}
              </div>
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <Dialog.Close asChild><button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Close</button></Dialog.Close>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>
    </Card>
  )
}

function MyDatasourcesPageInner() {
  const { user } = useAuth()
  const router = useRouter()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [items, setItems] = useState<DatasourceOut[]>([])
  const [toast, setToast] = useState<string>('')
  const [tabIndex, setTabIndex] = useState(0)
  const prevTabIndex = useRef(0)
  const [slideDir, setSlideDir] = useState<'left' | 'right'>('right')
  const [query, setQuery] = useState('')
  const [pageSize, setPageSize] = useState(8)
  const [pageActive, setPageActive] = useState(0)
  const [pageInactive, setPageInactive] = useState(0)
  const [metaById, setMetaById] = useState<Record<string, DsMeta>>({})
  const [dlgOpen, setDlgOpen] = useState(false)
  const [dlgMode, setDlgMode] = useState<DatasourceDialogMode>('create')
  const [dlgInitial, setDlgInitial] = useState<DatasourceOut | undefined>(undefined)
  const fileRef = useRef<HTMLInputElement | null>(null)
  const [busyImport, setBusyImport] = useState(false)

  useEffect(() => {
    let cancelled = false
    async function run() {
      setLoading(true); setError(null)
      try {
        const res = await Api.listDatasources(user?.id || undefined);
        if (!cancelled) {
          setItems(res || [])
          // Initialize meta active state from API
          setMetaById((m) => {
            const next = { ...m }
            ;(res || []).forEach((ds) => {
              if (!next[ds.id]) next[ds.id] = { schemas: 0, tables: 0, views: 0, lastUsedAt: null, loading: false, error: null, active: ds.active ?? true }
              else next[ds.id] = { ...next[ds.id], active: ds.active ?? next[ds.id].active }
            })
            return next
          })
        }
      } catch (e: any) { if (!cancelled) setError(e?.message || 'Failed to load datasources') } finally { if (!cancelled) setLoading(false) }
    }
    void run(); return () => { cancelled = true }
  }, [user?.id])

  // Auto-open Add dialog when navigated from sidebar (routes to /datasources/sources?add=1)
  useEffect(() => {
    try {
      if (typeof window !== 'undefined') {
        const sp = new URLSearchParams(window.location.search)
        if (sp.get('add') === '1') {
          setDlgInitial(undefined)
          setDlgMode('create')
          setDlgOpen(true)
        }
      }
    } catch {}
  }, [])

  const filtered = useMemo(() => { const q = query.trim().toLowerCase(); if (!q) return items; return items.filter((d) => d.name.toLowerCase().includes(q) || (d.type || '').toLowerCase().includes(q)) }, [items, query])
  const isActive = (id: string) => (metaById[id] ? !!metaById[id].active : true)
  const activeItems = useMemo(() => filtered.filter((d) => isActive(d.id)), [filtered, metaById])
  const inactiveItems = useMemo(() => filtered.filter((d) => metaById[d.id]?.active === false), [filtered, metaById])
  const totalPagesActive = Math.max(1, Math.ceil(activeItems.length / pageSize))
  const totalPagesInactive = Math.max(1, Math.ceil(inactiveItems.length / pageSize))
  const visibleActive = activeItems.slice(pageActive * pageSize, pageActive * pageSize + pageSize)
  const visibleInactive = inactiveItems.slice(pageInactive * pageSize, pageInactive * pageSize + pageSize)
  useEffect(() => { setPageActive(0); setPageInactive(0) }, [query, pageSize])
  useEffect(() => { if (pageActive > totalPagesActive - 1) setPageActive(0) }, [totalPagesActive])
  useEffect(() => { if (pageInactive > totalPagesInactive - 1) setPageInactive(0) }, [totalPagesInactive])

  // Compute schema-derived counts for visible cards
  useEffect(() => {
    let stop = false
    async function compute(list: DatasourceOut[]) {
      for (const ds of list) {
        if (stop) return
        if (metaById[ds.id]?.loading) continue
        setMetaById((m) => ({ ...m, [ds.id]: { ...(m[ds.id] || { schemas: 0, tables: 0, views: 0 }), loading: true, active: m[ds.id]?.active ?? true } }))
        try {
          const s = await Api.introspect(ds.id)
          const schemas = (s?.schemas || []).length
          const tables = (s?.schemas || []).reduce((a: number, sch: any) => a + (sch.tables?.length || 0), 0)
          let views = 0
          try { const r = await Api.query({ sql: 'select count(*) from information_schema.views', datasourceId: ds.id, limit: 1, offset: 0 }); views = (Array.isArray(r?.rows) && r.rows[0] && typeof r.rows[0][0] === 'number') ? r.rows[0][0] as number : 0 } catch { views = 0 }
          if (stop) return
          setMetaById((m) => {
            const prevActive = m[ds.id]?.active ?? ds.active ?? true
            return { ...m, [ds.id]: { ...(m[ds.id] || {}), loading: false, error: null, schemas, tables, views, lastUsedAt: new Date().toISOString(), active: prevActive } }
          })
        } catch (e: any) {
          if (stop) return
          setMetaById((m) => {
            const prevActive = m[ds.id]?.active ?? ds.active ?? true
            return { ...m, [ds.id]: { ...(m[ds.id] || {}), loading: false, error: String(e?.message || 'Failed'), schemas: 0, tables: 0, views: 0, lastUsedAt: null, active: prevActive } }
          })
        }
      }
    }
    const list = tabIndex === 0 ? visibleActive : visibleInactive
    if (list.length) void compute(list)
    return () => { stop = true }
  }, [tabIndex, JSON.stringify(visibleActive.map((x) => x.id)), JSON.stringify(visibleInactive.map((x) => x.id))])

  const onOpen = (ds: DatasourceOut) => { router.push(`/datasources/${ds.id}` as `/datasources/${string}`) }
  const onEdit = (ds: DatasourceOut) => { setDlgInitial(ds); setDlgMode('edit'); setDlgOpen(true) }
  const onDelete = async (ds: DatasourceOut) => { await Api.deleteDatasource(ds.id); setItems((prev) => prev.filter((x) => x.id !== ds.id)); setToast('Deleted'); window.setTimeout(() => setToast(''), 1600) }

  const onToggleActive = async (ds: DatasourceOut, next: boolean) => {
    try {
      await Api.setDatasourceActive(ds.id, next, user?.id)
      setMetaById((m) => ({ ...m, [ds.id]: { ...(m[ds.id] || { schemas: 0, tables: 0, views: 0, lastUsedAt: null }), active: next, loading: false, error: null } }))
      setItems((prev) => prev.map((x) => x.id === ds.id ? { ...x, active: next } as DatasourceOut : x))
      setToast(next ? 'Activated' : 'Deactivated'); window.setTimeout(() => setToast(''), 1500)
    } catch (e: any) {
      setToast(e?.message || 'Failed to update status'); window.setTimeout(() => setToast(''), 1800)
    }
  }

  const onSaved = (ds: DatasourceOut) => {
    setItems((prev) => prev.map((x) => x.id === ds.id ? { ...x, ...ds } : x))
    setMetaById((m) => ({ ...m, [ds.id]: { ...(m[ds.id] || { schemas: 0, tables: 0, views: 0 }), active: ds.active ?? (m[ds.id]?.active ?? true), loading: false, error: null, lastUsedAt: m[ds.id]?.lastUsedAt || null } }))
    setToast('Saved'); window.setTimeout(() => setToast(''), 1600)
  }
  const onCreated = (ds: DatasourceOut) => {
    setItems((prev) => [ds, ...prev])
    setMetaById((m) => ({ ...m, [ds.id]: { schemas: 0, tables: 0, views: 0, lastUsedAt: null, loading: false, error: null, active: ds.active ?? true } }))
    setToast('Created'); window.setTimeout(() => setToast(''), 1600)
  }

  const renderList = (list: DatasourceOut[]) => (
    <div className={`space-y-4 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}`}>
      {loading && <Text>Loading…</Text>}
      {!loading && list.length === 0 && <Text>No datasources match your search.</Text>}
      {!loading && list.map((ds) => (
        <SourceRow key={ds.id} ds={ds} meta={metaById[ds.id] || { schemas: 0, tables: 0, views: 0, active: true }} onOpen={onOpen} onEdit={onEdit} onDelete={onDelete} onToggleActive={onToggleActive} />
      ))}
    </div>
  )

  return (
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">My Datasources</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">Manage your datasources here</Text>
          </div>
          <div className="flex items-center gap-2">
            <button onClick={() => { setDlgInitial(undefined); setDlgMode('create'); setDlgOpen(true) }} className="inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-500 dark:text-gray-400 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]">Add Datasource</button>
            <button
              className="inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-500 dark:text-gray-400 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]"
              onClick={async () => {
                try {
                  const data = await Api.exportDatasources({ includeSyncTasks: true, actorId: user?.id || undefined })
                  const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' })
                  const a = document.createElement('a')
                  const ts = new Date()
                  const name = `datasources-export-${ts.getFullYear()}${String(ts.getMonth()+1).padStart(2,'0')}${String(ts.getDate()).padStart(2,'0')}-${String(ts.getHours()).padStart(2,'0')}${String(ts.getMinutes()).padStart(2,'0')}.json`
                  a.href = URL.createObjectURL(blob)
                  a.download = name
                  document.body.appendChild(a)
                  a.click()
                  a.remove()
                } catch (e) {
                  setToast('Export failed'); window.setTimeout(() => setToast(''), 2000)
                }
              }}
            >Export All (.json)</button>
            <button
              className="inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-500 dark:text-gray-400 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50"
              disabled={busyImport}
              onClick={() => fileRef.current?.click()}
            >{busyImport ? 'Importing…' : 'Import JSON'}</button>
            <input ref={fileRef} hidden type="file" accept="application/json" onChange={async (e) => {
              const file = e.target.files?.[0]
              if (!file) return
              setBusyImport(true)
              try {
                const text = await file.text()
                const json = JSON.parse(text)
                let items: any[] = []
                if (Array.isArray(json)) items = json
                else if (Array.isArray(json?.items)) items = json.items
                else if (Array.isArray(json?.datasources)) items = json.datasources
                if (!items.length) { setToast('No datasources found'); window.setTimeout(() => setToast(''), 2000); return }
                await Api.importDatasources(items, user?.id || undefined)
                const res = await Api.listDatasources(user?.id || undefined, user?.id || undefined)
                setItems(res || [])
                setToast('Imported'); window.setTimeout(() => setToast(''), 1600)
              } catch (err: any) {
                setToast(err?.message || 'Import failed'); window.setTimeout(() => setToast(''), 2000)
              } finally {
                setBusyImport(false)
                try { if (fileRef.current) fileRef.current.value = '' } catch {}
              }
            }} />
          </div>
        </div>
        {error && <div className="px-4 py-2 text-sm text-red-600">{error}</div>}
        <TabGroup index={tabIndex} onIndexChange={(i) => { setSlideDir(i > prevTabIndex.current ? 'left' : 'right'); prevTabIndex.current = i; setTabIndex(i); }}>
          <TabList className="px-3 py-1.5 border-b border-[hsl(var(--border))]">
            <Tab className="pb-2.5 font-medium hover:border-gray-300"><span className="text-gray-500 dark:text-gray-400 ui-selected:text-gray-900 ui-selected:dark:text-white">Active</span><span className="ml-2 hidden rounded-tremor-small bg-tremor-background px-2 py-1 text-xs font-semibold tabular-nums ring-1 ring-inset ring-tremor-ring ui-selected:text-tremor-content-emphasis dark:bg-dark-tremor-background dark:ring-dark-tremor-ring ui-selected:dark:text-dark-tremor-content-emphasis sm:inline-flex">{activeItems.length}</span></Tab>
            <Tab className="pb-2.5 font-medium hover:border-gray-300"><span className="text-gray-500 dark:text-gray-400 ui-selected:text-gray-900 ui-selected:dark:text-white">Inactive</span><span className="ml-2 hidden rounded-tremor-small bg-tremor-background px-2 py-1 text-xs font-semibold tabular-nums ring-1 ring-inset ring-tremor-ring ui-selected:text-tremor-content-emphasis dark:bg-dark-tremor-background dark:ring-dark-tremor-ring ui-selected:dark:text-dark-tremor-content-emphasis sm:inline-flex">{inactiveItems.length}</span></Tab>
          </TabList>
          <TabPanels className="pt-0">
            <TabPanel className="px-3 pb-3 pt-0">
              <div className="flex items-center py-2 gap-2">
                <div className="flex items-center gap-2">
                  <label htmlFor="searchSourcesActive" className="text-sm mr-2 text-gray-600 dark:text-gray-300">Search</label>
                  <input id="searchSourcesActive" value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search datasources..." className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]" />
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
              {renderList(visibleActive)}
              {!loading && activeItems.length > 0 && (
                <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
                  <span>Showing {pageActive * pageSize + 1}–{Math.min((pageActive + 1) * pageSize, activeItems.length)} of {activeItems.length}</span>
                  <div className="flex items-center gap-2">
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageActive <= 0} onClick={() => setPageActive((p) => Math.max(0, p - 1))}>Prev</button>
                    <span>Page {pageActive + 1} / {totalPagesActive}</span>
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageActive >= totalPagesActive - 1} onClick={() => setPageActive((p) => Math.min(totalPagesActive - 1, p + 1))}>Next</button>
                  </div>
                </div>
              )}
            </TabPanel>
            <TabPanel className="px-3 pb-3 pt-0">
              <div className="flex items-center py-2 gap-2">
                <div className="flex items-center gap-2">
                  <label htmlFor="searchSourcesInactive" className="text-sm mr-2 text-gray-600 dark:text-gray-300">Search</label>
                  <input id="searchSourcesInactive" value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search datasources..." className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]" />
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
              {renderList(visibleInactive)}
              {!loading && inactiveItems.length > 0 && (
                <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
                  <span>Showing {pageInactive * pageSize + 1}–{Math.min((pageInactive + 1) * pageSize, inactiveItems.length)} of {inactiveItems.length}</span>
                  <div className="flex items-center gap-2">
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageInactive <= 0} onClick={() => setPageInactive((p) => Math.max(0, p - 1))}>Prev</button>
                    <span>Page {pageInactive + 1} / {totalPagesInactive}</span>
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={pageInactive >= totalPagesInactive - 1} onClick={() => setPageInactive((p) => Math.min(totalPagesInactive - 1, p + 1))}>Next</button>
                  </div>
                </div>
              )}
            </TabPanel>
          </TabPanels>
        </TabGroup>
      </Card>
      {!!toast && (
        <div className="fixed top-6 right-6 z-[100] flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-3 text-[14px] font-medium text-white">
          <RiCheckLine className="w-5 h-5" /><span>{toast}</span>
        </div>
      )}
      <DatasourceDialog open={dlgOpen} onOpenChangeAction={setDlgOpen} mode={dlgMode} initial={dlgInitial} onCreatedAction={onCreated} onSavedAction={onSaved} />
    </div>
  )
}

 export default function MyDatasourcesPage() {
   return (
     <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
       <MyDatasourcesPageInner />
     </Suspense>
   )
 }
