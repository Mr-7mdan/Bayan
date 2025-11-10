"use client"

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useMemo, useState, useEffect, useCallback, useRef } from 'react'
import { navConfig, SidebarGroup, SidebarItem } from '@/config/navigation'
import { useTheme } from '@/components/providers/ThemeProvider'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { useAuth } from '@/components/providers/AuthProvider'
import * as Popover from '@radix-ui/react-popover'
import { Api, SidebarCountsResponse } from '@/lib/api'
import {
  RiHome2Line,
  RiLayout4Line,
  RiBarChart2Line,
  RiShareLine,
  RiDatabase2Line,
  RiAddCircleLine,
  RiKey2Line,
  RiLogoutBoxLine,
  RiArrowDownSLine,
  RiUserLine,
  RiSettings3Line,
  RiCalendar2Line,
  RiNotification2Line,
  RiContactsBook3Line,
  RiInformationLine,
  RiDatabaseLine,
} from '@remixicon/react'

function iconFor(label: string) {
  switch (label) {
    case 'Home': return <RiHome2Line className="w-4 h-4" />
    case 'Build New Dashboard': return <RiLayout4Line className="w-4 h-4" />
    case 'My Dashboards': return <RiBarChart2Line className="w-4 h-4" />
    case 'Shared With Me': return <RiShareLine className="w-4 h-4" />
    case 'Alerts & Notifications': return <RiNotification2Line className="w-4 h-4" />
    case 'Add Datasource': return <RiAddCircleLine className="w-4 h-4" />
    case 'My Datasources': return <RiDatabase2Line className="w-4 h-4" />
    case 'Data Model': return <RiDatabaseLine className="w-4 h-4" />
    case 'Contacts Manager': return <RiContactsBook3Line className="w-4 h-4" />
    case 'Change Password': return <RiKey2Line className="w-4 h-4" />
    case 'Logout': return <RiLogoutBoxLine className="w-4 h-4" />
    // Admin group
    case 'Users': return <RiUserLine className="w-4 h-4" />
    case 'Environment': return <RiSettings3Line className="w-4 h-4" />
    case 'Schedule Workers': return <RiCalendar2Line className="w-4 h-4" />
    default: return <span className="w-4 h-4 inline-block" />
  }
}

type SidebarCounts = {
  dashboards: number
  datasources: number
  shared: number
  collections: number
  alerts: number
}

function Item({ it, active, nested, badge }: { it: SidebarItem; active: boolean; nested?: boolean; badge?: number }) {
  const { resolved } = useTheme()
  const base = resolved === 'dark' ? 'sidebar-item-dark' : 'sidebar-item-light'
  const activeCls = resolved === 'dark' ? 'sidebar-item-active-dark' : 'sidebar-item-active-light'
  const cls = `${base} ${active ? activeCls : ''} ${nested ? 'pl-6 pr-3' : ''}`
  
  // Debug logging for badge
  const shouldShowBadge = typeof badge === 'number' && badge > 0
  if (badge !== undefined) {
    console.log(`[Item] ${it.label}: badge=${badge}, type=${typeof badge}, shouldShow=${shouldShowBadge}`)
  }
  
  const content = (
    <div className="flex items-center gap-3 w-full">
      <span className={`shrink-0 ${active ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))]'}`}>{iconFor(it.label)}</span>
      <span className={`truncate ${active ? 'text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))]'}`}>{it.label}</span>
      {shouldShowBadge && (
        <span className="ml-auto px-1.5 py-0.5 text-[11px] rounded-md bg-[hsl(var(--secondary)/0.6)] text-[hsl(var(--muted-foreground))] ring-1 ring-[hsl(var(--border))]">
          {badge}
        </span>
      )}
    </div>
  )
  const onClick = (e: React.MouseEvent) => {
    if (it.label === 'Build New Dashboard') {
      e.preventDefault()
      try { window.dispatchEvent(new CustomEvent('open-create-dashboard')) } catch {}
      return
    }
  }
  return it.href ? (
    <Link href={it.href as any} onClick={onClick} className={cls} title={it.description || it.label}>{content}</Link>
  ) : (
    <div className={cls} onClick={onClick} title={it.description || it.label}>{content}</div>
  )
}

export default function Sidebar({ hidden = false }: { hidden?: boolean }) {
  const pathname = usePathname()
  const { resolved } = useTheme()
  const { user } = useAuth()
  const { env } = useEnvironment()

  const nodes = useMemo(() => {
    const base = navConfig.sidebar.filter((x) => (x as SidebarGroup).position !== 'bottom')
    if ((user?.role || '').toLowerCase() === 'admin') {
      const adminGroup: SidebarGroup = {
        type: 'group',
        label: 'Admin',
        items: [
          { type: 'item', label: 'Users', href: '/admin/users' },
          { type: 'item', label: 'Environment', href: '/admin/environment' },
          { type: 'item', label: 'Schedule Workers', href: '/admin/schedules' },
        ],
      }
      return [...base, adminGroup]
    }
    return base
  }, [user?.role])

  const wrapCls = 'bg-[hsl(var(--background))] border-r border-[hsl(var(--border))] text-[hsl(var(--foreground))]'

  const [counts, setCounts] = useState<SidebarCounts>({ dashboards: 0, datasources: 0, shared: 0, collections: 0, alerts: 0 })

  const lastRunRef = useRef<number>(0)
  const busyRef = useRef<boolean>(false)
  const loadCounts = useCallback(() => {
    const w: any = typeof window !== 'undefined' ? window : {} as any
    if (busyRef.current || w.__sidebarCountsBusy) return
    const now = Date.now()
    const last = Math.max((lastRunRef.current || 0), Number(w.__sidebarCountsLastRunMs || 0))
    if (now - last < 2000) return
    busyRef.current = true
    w.__sidebarCountsBusy = true
    const userId = user?.id || 'dev_user'
    Promise.all([
      Api.getSidebarCounts(userId).catch(() => null),
      Api.listAlerts().catch(() => null),
    ]).then(([res, alerts]) => {
      const newCounts = {
        dashboards: (res as SidebarCountsResponse | null)?.dashboardCount || 0,
        datasources: (res as SidebarCountsResponse | null)?.datasourceCount || 0,
        shared: (res as SidebarCountsResponse | null)?.sharedCount || 0,
        collections: (res as SidebarCountsResponse | null)?.collectionCount || 0,
        alerts: Array.isArray(alerts) ? alerts.length : 0,
      }
      console.log('[Sidebar] API response:', res)
      console.log('[Sidebar] Setting counts:', newCounts)
      setCounts(newCounts)
    }).catch(() => {})
    .finally(() => { busyRef.current = false; lastRunRef.current = Date.now(); try { if (typeof window !== 'undefined') { (window as any).__sidebarCountsLastRunMs = Date.now(); (window as any).__sidebarCountsBusy = false } } catch {} })
  }, [user?.id])

  useEffect(() => {
    loadCounts()
  }, [loadCounts])

  useEffect(() => {
    if (typeof window === 'undefined') return
    const handler = () => loadCounts()
    window.addEventListener('sidebar-counts-refresh', handler)
    return () => window.removeEventListener('sidebar-counts-refresh', handler)
  }, [loadCounts])

  const badgeFor = (label: string): number | undefined => {
    let badge: number | undefined
    switch (label) {
      case 'My Dashboards':
        badge = counts.dashboards
        break
      case 'My Datasources':
        badge = counts.datasources
        break
      case 'Shared With Me':
        badge = counts.shared
        break
      case 'Alerts & Notifications':
        badge = counts.alerts
        break
      default:
        badge = undefined
    }
    if (badge !== undefined) {
      console.log(`[Sidebar] Badge for "${label}":`, badge, 'counts:', counts)
    }
    return badge
  }

  return (
    <aside
      className={`${wrapCls} h-full w-[272px] flex flex-col transform ${hidden ? '-translate-x-full opacity-0' : 'translate-x-0 opacity-100'} transition-transform duration-200 ease-out`}
    >
      <div className="px-4 py-3 border-b border-[hsl(var(--border))] flex justify-center">
        <Link href="/home" aria-label="Home" className="inline-flex items-center">
          <img src={(env.orgLogoLight || '/logo.svg') as any} alt={(env.orgName || 'Bayan')} className="h-8 w-auto block dark:hidden" />
          <img src={(env.orgLogoDark || '/logo-dark.svg') as any} alt={(env.orgName || 'Bayan')} className="h-8 w-auto hidden dark:block" />
        </Link>
      </div>

      <div className="flex-1 overflow-auto px-2 py-3 space-y-1">
        {nodes.map((node, i) => {
          if ((node as any).type === 'item') {
            const it = node as SidebarItem
            const active = !!(it.href && (it.href === '/' ? pathname === '/' : pathname?.startsWith(it.href)))
            return <Item key={`i-${i}`} it={it} active={active} badge={badgeFor(it.label)} />
          }
          const grp = node as SidebarGroup
          const items = grp.items || []
          if (items.length === 0) return null
          return (
            <div key={`g-${grp.label}`}>
              <div className="px-4 mt-3 mb-2 text-[11px] uppercase tracking-wider text-[hsl(var(--muted-foreground))]">{grp.label}</div>
              <div className="space-y-1">
                {items.map((it, j) => {
                  const active = !!(it.href && (it.href === '/' ? pathname === '/' : pathname?.startsWith(it.href)))
                  return <Item key={`${grp.label}-${j}`} it={it} active={active} nested badge={badgeFor(it.label)} />
                })}
              </div>
            </div>
          )
        })}
      </div>

      <div className="px-2 py-3 border-t border-[hsl(var(--border))]">
        <Popover.Root>
          <Popover.Trigger asChild>
            <button className="w-full flex items-center gap-3 px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))] transition-colors focus:outline-none focus-visible:outline-none">
              <div className="w-8 h-8 rounded-full bg-[hsl(var(--secondary))] flex items-center justify-center text-[12px] text-[hsl(var(--muted-foreground))]">
                {(user?.name || user?.email || 'U').slice(0, 2).toUpperCase()}
              </div>
              <div className="flex-1 text-sm text-left opacity-90 truncate">{user?.name || user?.email || 'User'}</div>
              <RiArrowDownSLine className="w-4 h-4 opacity-70" />
            </button>
          </Popover.Trigger>
          <Popover.Content side="top" align="start" className="z-50 w-64 rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--popover))] shadow-none p-1">
            <div className="px-3 py-2 text-sm text-[hsl(var(--muted-foreground))] truncate border-b border-[hsl(var(--border))]">
              {user?.email || 'user@example.com'}
            </div>
            <Link
              href={"/about" as any}
              className="flex items-center gap-2 px-3 py-2 rounded-md text-sm hover:bg-[hsl(var(--muted))]"
            >
              <RiInformationLine className="w-4 h-4 text-[hsl(var(--muted-foreground))]" />
              <span>About</span>
            </Link>
            <Link
              href={"/users/change-password" as any}
              className="flex items-center gap-2 px-3 py-2 rounded-md text-sm hover:bg-[hsl(var(--muted))]"
            >
              <RiKey2Line className="w-4 h-4 text-[hsl(var(--muted-foreground))]" />
              <span>Change Password</span>
            </Link>
            <Link
              href={"/logout" as any}
              className="flex items-center gap-2 px-3 py-2 rounded-md text-sm hover:bg-[hsl(var(--muted))]"
            >
              <RiLogoutBoxLine className="w-4 h-4 text-[hsl(var(--muted-foreground))]" />
              <span>Logout</span>
            </Link>
          </Popover.Content>
        </Popover.Root>
      </div>
    </aside>
  )
}
