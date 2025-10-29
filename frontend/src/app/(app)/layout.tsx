"use client"

import { Suspense, useEffect, useState } from 'react'
import { Api, type NotificationOut } from '@/lib/api'
import { RiCheckLine } from '@remixicon/react'
import { usePathname, useRouter } from 'next/navigation'
import Sidebar from '@/components/shell/Sidebar'
import Navbar from '@/components/shell/Navbar'
import { useAuth } from '@/components/providers/AuthProvider'
import CreateDashboardDialog from '@/components/dashboards/CreateDashboardDialog'

function AppLayoutContent({ children }: { children: React.ReactNode }) {
  const { user, loading } = useAuth()
  const router = useRouter()
  const pathname = usePathname()
  const [notifs, setNotifs] = useState<NotificationOut[]>([])
  // Changelog dialog state
  const [showChangelog, setShowChangelog] = useState(false)
  const [clBackendVer, setClBackendVer] = useState<string|undefined>(undefined)
  const [clFrontendVer, setClFrontendVer] = useState<string|undefined>(undefined)
  const [clNotesBackend, setClNotesBackend] = useState<string>('')
  const [clNotesFrontend, setClNotesFrontend] = useState<string>('')

  useEffect(() => {
    if (loading) return
    // allow unauthenticated access to public view pages
    if (!user) router.replace('/login')
  }, [loading, user, router])

  // Fetch one-shot notifications after login (popped server-side)
  useEffect(() => {
    let cancelled = false
    async function run() {
      if (!user?.id) return
      try {
        const items = await Api.getNotifications(user.id)
        if (!cancelled && items && items.length) {
          setNotifs(items)
          // Auto-dismiss after a few seconds
          setTimeout(() => { if (!cancelled) setNotifs([]) }, 6000)
        }
      } catch { /* ignore */ }
    }
    run()
    return () => { cancelled = true }
  }, [user?.id])

  // Show changelog on first login after an update
  useEffect(() => {
    let cancelled = false
    async function run() {
      try {
        if (!user?.id) return
        const vers = await Api.updatesVersion()
        const backendV = (vers?.backend || '').trim() || undefined
        const frontendV = (vers?.frontend || '').trim() || undefined
        // Read last seen
        const raw = typeof window !== 'undefined' ? window.localStorage.getItem('last_seen_versions') : null
        let lastSeen: { backend?: string; frontend?: string } = {}
        if (raw) {
          try { lastSeen = JSON.parse(raw) || {} } catch {}
        }
        // If versions are placeholders/empty (e.g., backend 0.0.0 and no frontend), do not show; instead prime storage.
        const isPlaceholder = ((!backendV || backendV === '0.0.0') && (!frontendV || frontendV === ''))
        if (isPlaceholder) {
          try { if (typeof window !== 'undefined') window.localStorage.setItem('last_seen_versions', JSON.stringify({ backend: backendV || '', frontend: frontendV || '' })) } catch {}
          return
        }
        // First run: if nothing stored yet, store current and exit silently
        if (!raw) {
          try { if (typeof window !== 'undefined') window.localStorage.setItem('last_seen_versions', JSON.stringify({ backend: backendV || '', frontend: frontendV || '' })) } catch {}
          return
        }
        const changed = (backendV && backendV !== (lastSeen.backend || '')) || (frontendV && frontendV !== (lastSeen.frontend || ''))
        if (!changed) return
        // Fetch release notes for both components (best-effort)
        let nb = ''
        let nf = ''
        try { const r = await Api.updatesCheck('backend'); nb = String(r?.releaseNotes || '') } catch {}
        try { const r = await Api.updatesCheck('frontend'); nf = String(r?.releaseNotes || '') } catch {}
        if (cancelled) return
        setClBackendVer(backendV)
        setClFrontendVer(frontendV)
        setClNotesBackend(nb)
        setClNotesFrontend(nf)
        setShowChangelog(true)
      } catch {}
    }
    run()
    return () => { cancelled = true }
  }, [user?.id])

  // Sidebar state: apply auto-minimize only on the builder route; elsewhere default open
  const [sidebarOpen, setSidebarOpen] = useState<boolean>(() => {
    try {
      const onBuilder = pathname === '/' || (pathname?.startsWith('/builder') ?? false)
      if (onBuilder) {
        return localStorage.getItem('builder_auto_min_sidebar') === '0'
      }
      return true
    } catch {
      const onBuilder = pathname === '/' || (pathname?.startsWith('/builder') ?? false)
      return onBuilder ? false : true
    }
  })
  const toggleSidebar = () => setSidebarOpen((v) => !v)

  // When navigating into the builder route, auto-minimize if the builder-only pref is enabled
  useEffect(() => {
    try {
      const onBuilder = pathname === '/' || (pathname?.startsWith('/builder') ?? false)
      if (onBuilder) {
        const enabled = localStorage.getItem('builder_auto_min_sidebar') !== '0'
        if (enabled) setSidebarOpen(false)
      }
    } catch { /* ignore */ }
  }, [pathname])

  // Render nothing until auth resolves to avoid flicker
  if (loading || !user) return null

  return (
    <div className={`h-screen grid ${sidebarOpen ? 'grid-cols-[272px,1fr]' : 'grid-cols-[0px,1fr]'} transition-[grid-template-columns] duration-200`}>
      <Sidebar hidden={!sidebarOpen} />
      <div className="h-screen flex flex-col min-h-0 min-w-0">
        <Navbar sidebarOpen={sidebarOpen} onToggleSidebarAction={toggleSidebar} />
        <main className={`p-0 min-h-0 min-w-0 flex-1 overflow-auto bg-[hsl(var(--background))]`}>
          {children}
        </main>
        {/* Global dialog mount so sidebar 'Build New Dashboard' opens anywhere */}
        <CreateDashboardDialog />
        {/* Global notifications */}
        {!!notifs.length && (
          <div className="fixed top-6 right-6 z-[200] space-y-2">
            {notifs.map((n) => (
              <div key={n.id} className="flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-3 text-[14px] font-medium text-white shadow-card">
                <RiCheckLine className="w-5 h-5" />
                <span>{n.message}</span>
              </div>
            ))}
          </div>
        )}
        {/* Floating Changelog */}
        {showChangelog && (
          <div className="fixed bottom-6 right-6 z-[300] w-[420px] max-w-[90vw]">
            <div className="rounded-lg border bg-card shadow-lg">
              <div className="px-3 py-2 border-b">
                <div className="text-sm font-semibold">What's new</div>
                <div className="text-xs text-muted-foreground">Backend {clBackendVer || '—'} · Frontend {clFrontendVer || '—'}</div>
              </div>
              <div className="p-3 max-h-[50vh] overflow-auto space-y-3">
                {clNotesBackend ? (
                  <div>
                    <div className="text-xs font-medium mb-1">Backend</div>
                    <div className="text-xs whitespace-pre-wrap">{clNotesBackend}</div>
                  </div>
                ) : null}
                {clNotesFrontend ? (
                  <div>
                    <div className="text-xs font-medium mb-1">Frontend</div>
                    <div className="text-xs whitespace-pre-wrap">{clNotesFrontend}</div>
                  </div>
                ) : null}
                {!clNotesBackend && !clNotesFrontend && (
                  <div className="text-xs text-muted-foreground">Updated to the latest version.</div>
                )}
              </div>
              <div className="px-3 py-2 border-t flex items-center justify-end gap-2">
                <button
                  className="text-xs px-2 py-1 rounded-md border hover:bg-muted"
                  onClick={() => {
                    try {
                      const payload = JSON.stringify({ backend: clBackendVer || '', frontend: clFrontendVer || '' })
                      if (typeof window !== 'undefined') window.localStorage.setItem('last_seen_versions', payload)
                    } catch {}
                    setShowChangelog(false)
                  }}
                >Got it</button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
      <AppLayoutContent>{children}</AppLayoutContent>
    </Suspense>
  )
}
