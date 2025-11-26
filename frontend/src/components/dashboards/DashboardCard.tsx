"use client"

import { useState } from 'react'
import { Card } from '@tremor/react'
import * as Popover from '@radix-ui/react-popover'
import { RiMore2Line, RiStarFill, RiStarLine, RiBuildingLine, RiMapPin2Line, RiUserLine } from '@remixicon/react'
import type { DashboardListItem } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'

function StatusPill({ published }: { published: boolean }) {
  const color = published ? 'bg-emerald-600' : 'bg-blue-500'
  const label = published ? 'Published' : 'In progress'
  return (
    <span className="inline-flex items-center gap-1.5 whitespace-nowrap rounded-md bg-[hsl(var(--secondary)/0.6)] px-2 py-1 text-[11px] font-medium text-[hsl(var(--muted-foreground))] ring-1 ring-inset ring-[hsl(var(--border))]">
      <span className={`${color} size-2 rounded-full`} aria-hidden={true} />
      {label}
    </span>
  )
}

function PermPill({ perm }: { perm: 'ro' | 'rw' }) {
  const color = perm === 'rw' ? 'bg-emerald-600' : 'bg-blue-500'
  const label = perm === 'rw' ? 'Read‑write' : 'Read‑only'
  return (
    <span className="inline-flex items-center gap-1.5 whitespace-nowrap rounded-md bg-[hsl(var(--secondary)/0.6)] px-2 py-1 text-[11px] font-medium text-[hsl(var(--muted-foreground))] ring-1 ring-inset ring-[hsl(var(--border))]">
      <span className={`${color} size-2 rounded-full`} aria-hidden={true} />
      {label}
    </span>
  )
}

function timeAgo(iso?: string | Date | null): string {
  if (!iso) return 'just now'
  const then = (iso instanceof Date) ? iso.getTime() : new Date(iso).getTime()
  const now = Date.now()
  const diff = Math.max(0, now - then)
  const m = Math.floor(diff / 60000)
  if (m < 1) return 'just now'
  if (m < 60) return `${m}min ago`
  const h = Math.floor(m / 60)
  if (h < 24) return `${h}h ago`
  const d = Math.floor(h / 24)
  return `${d}d ago`
}

export default function DashboardCard({
  d,
  onOpenAction,
  onOpenPublicAction,
  isFavorite,
  onToggleFavoriteAction,
  showMenu = false,
  badgeMode = 'status',
  permission,
  // Dashboard actions
  onEditAction,
  onDuplicateAction,
  onPublishOpenAction,
  onUnpublishAction,
  onDeleteAction,
  onCopyLinkAction,
  onExportAction,
  // Collections
  onRemoveFromCollectionAction,
  context = 'dashboard',
  widthClass,
  sharedBy,
  sharedAt,
}: {
  d: DashboardListItem
  onOpenAction: (d: DashboardListItem) => void
  onOpenPublicAction?: (d: DashboardListItem) => void
  isFavorite?: boolean
  onToggleFavoriteAction?: (d: DashboardListItem, next: boolean) => void | Promise<void>
  showMenu?: boolean
  badgeMode?: 'status' | 'permission'
  permission?: 'ro' | 'rw'
  onEditAction?: (d: DashboardListItem) => void | Promise<void>
  onDuplicateAction?: (d: DashboardListItem) => void | Promise<void>
  onPublishOpenAction?: (d: DashboardListItem) => void | Promise<void>
  onUnpublishAction?: (d: DashboardListItem) => void | Promise<void>
  onDeleteAction?: (d: DashboardListItem) => void | Promise<void>
  onCopyLinkAction?: (d: DashboardListItem) => void | Promise<void>
  onExportAction?: (d: DashboardListItem) => void | Promise<void>
  onRemoveFromCollectionAction?: (d: DashboardListItem) => void | Promise<void>
  context?: 'dashboard' | 'collection'
  widthClass?: string
  sharedBy?: string
  sharedAt?: string | Date
}) {
  const [menuOpen, setMenuOpen] = useState(false)
  const [opening, setOpening] = useState(false)
  const Star = isFavorite ? RiStarFill : RiStarLine
  const { user } = useAuth()
  const handlePrimaryOpen = (force = false) => {
    if (!force && menuOpen) return
    if (opening) return
    setOpening(true)
    if (context === 'collection' && permission === 'ro' && onOpenPublicAction) onOpenPublicAction(d)
    else onOpenAction(d)
  }
  const openLabel = (context === 'collection' && permission === 'ro') ? 'Open read‑only' : 'Open'

  const cardWidth = widthClass || 'w-[calc(33.333%_-_12px)] min-w-[320px] max-w-[520px]'

  return (
    <Card
      className={`group flex-shrink-0 ${cardWidth} p-2 rounded-2xl border border-[hsl(var(--border))] ring-1 ring-inset ring-[hsl(var(--border))] bg-[hsl(var(--card))] elev-bottom ${opening ? 'cursor-wait ring-[hsl(var(--ring))]' : 'cursor-pointer hover:ring-[hsl(var(--ring))]'} focus-visible:ring-[hsl(var(--ring))] outline-none`}
      onClick={() => { if (!opening) handlePrimaryOpen() }}
      onKeyDown={(e) => { if (e.key === 'Enter' && !menuOpen && !opening) handlePrimaryOpen() }}
      role="button"
      tabIndex={0}
      aria-busy={opening || undefined}
      aria-disabled={opening || undefined}
    >
      <div className={`relative hover-cover rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] p-4 transition-transform duration-150 ring-1 ring-inset ring-[hsl(var(--border))] ${opening ? 'pulse-border' : `group-hover:ring-[hsl(var(--ring))] group-hover:border-[hsl(var(--ring))] ${menuOpen ? '' : 'group-hover:-translate-y-[1px]'}`}`}>
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 min-w-0">
            <h4 className="truncate text-[14px] font-medium text-[hsl(var(--foreground))]">{d.name}</h4>
            {badgeMode === 'permission' && permission ? (
              <PermPill perm={permission} />
            ) : (
              <StatusPill published={!!d.published} />
            )}
          </div>
          <div className="flex items-center gap-1">
            {!!onToggleFavoriteAction && (
              <button
                className="p-1.5 rounded-md hover:bg-[hsl(var(--muted))] focus:outline-none"
                aria-label={isFavorite ? 'Unfavorite' : 'Favorite'}
                onClick={(e) => { e.stopPropagation(); onToggleFavoriteAction?.(d, !isFavorite) }}
              >
                <Star className={`w-5 h-5 ${isFavorite ? 'text-amber-500' : 'opacity-80'}`} />
              </button>
            )}
            {showMenu && (
              <Popover.Root open={menuOpen} onOpenChange={setMenuOpen}>
                <Popover.Trigger asChild>
                  <button className="p-1.5 rounded-md hover:bg-[hsl(var(--muted))] focus:outline-none" aria-label="Actions" onClick={(e) => e.stopPropagation()}>
                    <RiMore2Line className="w-5 h-5 opacity-80" />
                  </button>
                </Popover.Trigger>
                <Popover.Portal>
                <Popover.Content side="bottom" align="end" className="z-50 w-48 rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--popover))] shadow-none p-1">
                  {/* Common primary open */}
                  <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={(e) => { e.stopPropagation(); setMenuOpen(false); handlePrimaryOpen(true) }}>{openLabel}</button>
                  {/* Extra read-only option only when in dashboard context and published */}
                  {context === 'dashboard' && !!onOpenPublicAction && d.published && (
                    <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={(e) => { e.stopPropagation(); setMenuOpen(false); onOpenPublicAction(d) }}>Open read‑only</button>
                  )}
                  {/* Dashboards context */}
                  {context === 'dashboard' && (
                    <>
                      {!!onEditAction && (
                        <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={(e) => { e.stopPropagation(); setMenuOpen(false); onEditAction(d) }}>Edit</button>
                      )}
                      {!!onDuplicateAction && (
                        <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={async (e) => { e.stopPropagation(); setMenuOpen(false); await onDuplicateAction(d) }}>Duplicate</button>
                      )}
                      {!!onPublishOpenAction && !d.published && (
                        <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={(e) => { e.stopPropagation(); setMenuOpen(false); onPublishOpenAction(d) }}>Publish</button>
                      )}
                      {!!onUnpublishAction && d.published && (
                        <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={async (e) => { e.stopPropagation(); setMenuOpen(false); await onUnpublishAction(d) }}>Unpublish</button>
                      )}
                      {!!onCopyLinkAction && d.published && (
                        <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={async (e) => { e.stopPropagation(); setMenuOpen(false); await onCopyLinkAction(d) }}>Copy link</button>
                      )}
                      {!!onExportAction && (
                        <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={async (e) => { e.stopPropagation(); setMenuOpen(false); await onExportAction(d) }}>Export (.json)</button>
                      )}
                      {!!onDeleteAction && (
                        <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={async (e) => { e.stopPropagation(); setMenuOpen(false); await onDeleteAction(d) }}>Delete</button>
                      )}
                    </>
                  )}
                  {/* Collections context */}
                  {context === 'collection' && (
                    <>
                      {permission === 'rw' && !!onEditAction && (
                        <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={(e) => { e.stopPropagation(); setMenuOpen(false); onEditAction(d) }}>Edit</button>
                      )}
                      {!!onRemoveFromCollectionAction && (
                        <button className="w-full text-left text-sm px-3 py-2 rounded-md hover:bg-[hsl(var(--muted))]" onClick={async (e) => { e.stopPropagation(); setMenuOpen(false); await onRemoveFromCollectionAction(d) }}>Remove from collection</button>
                      )}
                    </>
                  )}
                </Popover.Content>
                </Popover.Portal>
              </Popover.Root>
            )}
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-x-6 gap-y-4 text-[13px]">
          <div className="flex items-center space-x-1.5">
            <RiBuildingLine className="size-5 text-tremor-content-subtle dark:text-dark-tremor-content-subtle" aria-hidden={true} />
            <p className="text-[hsl(var(--muted-foreground))]">Data sources ({d.datasourceCount})</p>
          </div>
          <div className="flex items-center space-x-1.5">
            <RiMapPin2Line className="size-5 text-tremor-content-subtle dark:text-dark-tremor-content-subtle" aria-hidden={true} />
            <p className="text-[hsl(var(--muted-foreground))]">Tables ({d.tablesCount})</p>
          </div>
          <div className="flex items-center space-x-1.5">
            <RiUserLine className="size-5 text-tremor-content-subtle dark:text-dark-tremor-content-subtle" aria-hidden={true} />
            <p className="text-[hsl(var(--muted-foreground))]">Widgets ({d.widgetsCount})</p>
          </div>
        </div>
      </div>
      <div className="px-2 pb-2 pt-4">
        <div className="block sm:flex sm:items-end sm:justify-between">
          {context === 'collection' ? (
            <>
              <div className="flex items-center gap-2 text:[13px] text-[hsl(var(--muted-foreground))]">
                <span className="inline-block w-3 h-3 rounded-full border-2 border-blue-500" aria-hidden="true" />
                <span>Shared by: {sharedBy || d.userId || 'Unknown'} on {new Date(sharedAt || d.createdAt).toLocaleDateString(undefined, { day: '2-digit', month: '2-digit', year: 'numeric' })}</span>
              </div>
              <p className="mt-2 text-[13px] text-[hsl(var(--muted-foreground))] sm:mt-0">Shared {timeAgo(sharedAt || d.createdAt)}</p>
            </>
          ) : (
            <>
              <div className="flex items-center gap-2 text-[13px] text-[hsl(var(--muted-foreground))]">
                <span className="inline-block w-3 h-3 rounded-full border-2 border-blue-500" aria-hidden="true" />
                <span>
                  Created by: {user && d.userId === user.id ? (user.name || 'You') : (d.userId || 'Unknown')} on {new Date(d.createdAt).toLocaleDateString(undefined, { day: '2-digit', month: '2-digit', year: 'numeric' })}
                </span>
              </div>
              <p className="mt-2 text-[13px] text-[hsl(var(--muted-foreground))] sm:mt-0">Updated {timeAgo(d.updatedAt || d.createdAt)}</p>
            </>
          )}
        </div>
      </div>
    </Card>
  )
}
