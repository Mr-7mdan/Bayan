"use client"

import Link from 'next/link'
import { usePathname } from 'next/navigation'
import { useMemo } from 'react'
import { useTheme } from '@/components/providers/ThemeProvider'
import ThemeToggle from '@/components/ui/ThemeToggle'
import { RiLayout4Line, RiArrowRightSLine } from '@remixicon/react'
import { navConfig, SidebarGroup, SidebarItem } from '@/config/navigation'

type Props = { sidebarOpen?: boolean; onToggleSidebarAction?: () => void }

export default function Navbar({ sidebarOpen = true, onToggleSidebarAction }: Props) {
  const { resolved, darkVariant, setDarkVariant } = useTheme()
  const pathname = usePathname()
  const wrap = 'bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]'
  // page-level tabs were removed; keep navbar minimal with breadcrumbs only

  const routeIndex = useMemo(() => {
    const out: Array<{ href: string; label: string; group?: string }> = []
    navConfig.sidebar.forEach((node) => {
      if ((node as any).type === 'item') {
        const it = node as SidebarItem
        if (it.href) out.push({ href: it.href, label: it.label })
      } else {
        const grp = node as SidebarGroup
        ;(grp.items || []).forEach((it) => {
          if (it.href) out.push({ href: it.href, label: it.label, group: grp.label })
        })
      }
    })
    return out
  }, [])

  const groupHome: Record<string, string> = {
    Dashboards: '/dashboards/mine',
    Datasources: '/datasources/sources',
    Admin: '/admin/users',
    Profile: '/users/change-password',
  }

  const groupLabelOverride: Record<string, string> = {
    Dashboards: 'Dashboard',
  }

  const activeEntry = useMemo(() => {
    if (!pathname) return undefined
    return routeIndex
      .filter((e) => pathname.startsWith(e.href))
      .sort((a, b) => b.href.length - a.href.length)[0]
  }, [pathname, routeIndex])

  const crumbs = useMemo(() => {
    const list: Array<{ label: string; href?: string }> = []
    const titleCase = (s: string) => s.replace(/[-_]/g, ' ').replace(/\b\w/g, (m) => m.toUpperCase())
    if (activeEntry?.group) {
      const g = activeEntry.group
      list.push({ label: groupLabelOverride[g] || g, href: groupHome[g] })
    }
    if (activeEntry) {
      list.push({ label: activeEntry.label, href: activeEntry.href })
    } else if (pathname) {
      // Fallback: build from path segments like: Admin › Schedules
      const segs = pathname.split('/').filter(Boolean)
      if (segs.length === 0) {
        list.push({ label: 'Home', href: '/home' })
      } else {
        const baseHref = (() => {
          const seg0 = segs[0]
          if (seg0 === 'datasources') return '/datasources/sources'
          if (seg0 === 'dashboards') return '/dashboards/mine'
          if (seg0 === 'admin') return '/admin/users'
          return '/' + seg0
        })()
        list.push({ label: titleCase(segs[0]), href: baseHref })
        if (segs[1]) {
          list.push({ label: titleCase(segs[1]), href: baseHref + '/' + segs[1] })
        }
      }
    }
    return list
  }, [activeEntry, pathname])

  const applyBluish = () => setDarkVariant('bluish')
  const applyBlackish = () => setDarkVariant('blackish')

  return (
    <header className={`${wrap} sticky top-0 z-30`}> 
      <div className="px-6 py-3 flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm">
          {/* Sidebar toggle with hover (gray bg) and focus ring (amber) */}
          <button
            type="button"
            aria-label={sidebarOpen ? 'Hide sidebar' : 'Show sidebar'}
            onClick={onToggleSidebarAction}
            className="group inline-flex items-center justify-center h-8 w-8 rounded-lg text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-white/10 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-400 focus-visible:ring-offset-2 focus-visible:ring-offset-white dark:focus-visible:ring-offset-gray-950"
          >
            <RiLayout4Line className="w-4 h-4" />
          </button>
          <span className="h-5 w-px bg-[hsl(var(--border))] mx-2" />
          {/* Dynamic breadcrumbs */}
          <nav className="flex items-center gap-2">
            {crumbs.map((c, i) => {
              const isLast = i === crumbs.length - 1
              return (
                <div key={`${c.label}-${i}`} className="flex items-center gap-2">
                  {i > 0 && <RiArrowRightSLine className="w-4 h-4 opacity-50" />}
                  {isLast ? (
                    <span className="font-medium text-gray-900 dark:text-white">{c.label}</span>
                  ) : (
                    <Link href={(c.href || '#') as any} className="inline-flex items-center h-8 px-2 rounded-lg text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-white/10 transition-colors">
                      {c.label}
                    </Link>
                  )}
                </div>
              )
            })}
          </nav>
        </div>
        <div className="flex items-center gap-2">
          {resolved === 'dark' && (
            <div className="inline-flex items-center gap-1">
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
          <ThemeToggle />
        </div>
      </div>
    </header>
  )
}
