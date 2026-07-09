"use client"

import { useState, useEffect, useRef, type ComponentType } from 'react'
import {
  RiLayoutMasonryLine,
  RiLayoutGridLine,
  RiRefreshLine,
  RiLockLine,
  RiLockUnlockLine,
  RiAddLine,
  RiBarChartBoxLine,
  RiTableLine,
  RiNumbersLine,
  RiText,
  RiSpace,
  RiFileTextLine,
} from '@remixicon/react'
import { Button, StatusPill } from '@/components/ui'
import type { StatusPillState } from '@/components/ui'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'

type AddKind = 'kpi'|'chart'|'table'|'text'|'spacer'|'composition'|'report'

type TitleBarProps = {
  hydrated: boolean
  dashboardId: string | null
  publicId: string | null
  isProtected: boolean
  token: string
  onTokenChangeAction: (value: string) => void
  onSaveAction: () => void
  onLoadAction: () => void
  onPublishAction: () => void
  onUnpublishAction: () => void
  onSetTokenAction: () => void
  onRemoveTokenAction: () => void
  title?: string
  onTitleChangeAction?: (v: string) => void
  createdAt?: string
  gridSize?: 'sm'|'md'|'lg'|'xl'
  onGridSizeChangeAction?: (v: 'sm'|'md'|'lg'|'xl') => void
  onAddCardAction?: (kind: AddKind) => void
  showNavigator?: boolean
  onShowNavigatorChangeAction?: (v: boolean) => void
  autoMinimizeNav?: boolean
  onAutoMinimizeNavChangeAction?: (v: boolean) => void
  publicShowFilters?: boolean
  onPublicShowFiltersChangeAction?: (v: boolean) => void
  publicLockFilters?: boolean
  onPublicLockFiltersChangeAction?: (v: boolean) => void
  refreshEverySec?: number
  onRefreshEverySecChangeAction?: (sec: number) => void
  onNormalizeLayoutAction?: () => void
  onPackRowsFillAction?: () => void
  onCanvasAutoAction?: () => void
  onCanvasFixedAction?: (w: number) => void
  onCanvasFixedCurrentAction?: () => void
  // Autosave lifecycle (wired to page.tsx scheduleServerSave)
  saveStatus?: StatusPillState
  onRetrySaveAction?: () => void
}

const ADD_CARDS: Array<{ kind: AddKind; label: string; desc: string; Icon: ComponentType<any> }> = [
  { kind: 'chart', label: 'Chart', desc: 'Line, bar, area and more', Icon: RiBarChartBoxLine },
  { kind: 'table', label: 'Table', desc: 'Rows, columns and pivots', Icon: RiTableLine },
  { kind: 'kpi', label: 'KPI', desc: 'A single headline metric', Icon: RiNumbersLine },
  { kind: 'text', label: 'Text', desc: 'A note or markdown block', Icon: RiText },
  { kind: 'spacer', label: 'Spacer', desc: 'Empty layout spacing', Icon: RiSpace },
  { kind: 'composition', label: 'Composition', desc: 'Group widgets in one card', Icon: RiLayoutMasonryLine },
  { kind: 'report', label: 'Report', desc: 'A composed report widget', Icon: RiFileTextLine },
]

const REFRESH_OPTS: Array<{ v: number; label: string }> = [
  { v: 0, label: 'Off' }, { v: 60, label: 'Every 1m' }, { v: 180, label: 'Every 3m' },
  { v: 300, label: 'Every 5m' }, { v: 900, label: 'Every 15m' }, { v: 1800, label: 'Every 30m' },
  { v: 3600, label: 'Every 1h' }, { v: 21600, label: 'Every 6h' }, { v: 86400, label: 'Every 24h' },
]

const GRID_OPTS: Array<{ v: 'sm'|'md'|'lg'|'xl'; label: string }> = [
  { v: 'sm', label: 'Small (24 cols)' }, { v: 'md', label: 'Medium (18 cols)' },
  { v: 'lg', label: 'Large (12 cols)' }, { v: 'xl', label: 'XL (8 cols)' },
]

type MenuKey = 'add' | 'layout' | 'grid' | 'refresh' | null

export default function TitleBar({
  hydrated,
  dashboardId,
  publicId,
  isProtected,
  token,
  onTokenChangeAction,
  onSaveAction,
  onLoadAction,
  onPublishAction,
  onUnpublishAction,
  onSetTokenAction,
  onRemoveTokenAction,
  title,
  onTitleChangeAction,
  createdAt,
  gridSize,
  onGridSizeChangeAction,
  onAddCardAction,
  showNavigator,
  onShowNavigatorChangeAction,
  autoMinimizeNav,
  onAutoMinimizeNavChangeAction,
  publicShowFilters,
  onPublicShowFiltersChangeAction,
  publicLockFilters,
  onPublicLockFiltersChangeAction,
  refreshEverySec,
  onRefreshEverySecChangeAction,
  onNormalizeLayoutAction,
  onPackRowsFillAction,
  onCanvasAutoAction,
  onCanvasFixedCurrentAction,
  saveStatus = 'idle',
  onRetrySaveAction,
}: TitleBarProps) {
  const { env } = useEnvironment()
  const trimmedToken = token?.trim() ?? ''
  const hasToken = trimmedToken.length > 0

  // Inline title editing state
  const [editing, setEditing] = useState(false)
  const [localTitle, setLocalTitle] = useState<string>(title || 'Dashboard')
  useEffect(() => { setLocalTitle(title || 'Dashboard') }, [title])

  const commitTitle = () => {
    const next = (localTitle || '').trim() || 'Untitled Dashboard'
    if (onTitleChangeAction) onTitleChangeAction(next)
    setEditing(false)
  }

  const publicBase = () => ((env.publicDomain && env.publicDomain.trim())
    ? env.publicDomain
    : (typeof window !== 'undefined' ? window.location.origin : '')
  ).replace(/\/$/, '')

  const handleView = () => {
    if (!publicId) return
    const url = `${publicBase()}/v/${publicId}${hasToken ? `?token=${encodeURIComponent(trimmedToken)}` : ''}`
    window.open(url, '_blank', 'noopener,noreferrer')
  }

  const handleCopyLink = () => {
    if (!publicId) return
    try {
      let tok = trimmedToken
      if (isProtected && !tok && typeof window !== 'undefined') {
        const entered = window.prompt('Enter the access token to include in the link')
        if (entered && entered.trim()) { tok = entered.trim(); onTokenChangeAction?.(tok) }
      }
      const url = `${publicBase()}/v/${publicId}${(isProtected && tok) ? `?token=${encodeURIComponent(tok)}` : ''}`
      if (navigator?.clipboard?.writeText) navigator.clipboard.writeText(url)
    } catch {}
  }

  const showViewButton = Boolean(publicId) && (!isProtected || hasToken)

  const [openMenu, setOpenMenu] = useState<MenuKey>(null)
  // ponytail: canvas lock has no source-of-truth prop; track locally. Resets on reload.
  const [canvasLocked, setCanvasLocked] = useState(false)
  const rootRef = useRef<HTMLDivElement | null>(null)

  // Close menus on outside click / Escape
  useEffect(() => {
    const onDocPointer = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) setOpenMenu(null)
    }
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpenMenu(null) }
    document.addEventListener('mousedown', onDocPointer)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDocPointer)
      document.removeEventListener('keydown', onKey)
    }
  }, [])

  const toggle = (k: Exclude<MenuKey, null>) => setOpenMenu((v) => (v === k ? null : k))

  const iconBtnClass =
    'h-8 w-8 inline-flex items-center justify-center rounded-md border border-[hsl(var(--border))] bg-card text-foreground transition-colors hover:bg-muted'
  const rowClass =
    'w-full text-start text-sm px-3 py-2 rounded-md transition-colors hover:bg-muted disabled:opacity-50'

  return (
    <header className="sticky top-0 z-40 border-b border-[hsl(var(--border))] bg-[hsl(var(--topbar-bg))] text-[hsl(var(--topbar-fg))]">
      <div className="mx-auto w-full px-6 py-3 flex items-center gap-4 justify-between">
        <div className="flex items-center gap-3 min-w-0">
          {editing ? (
            <input
              type="text"
              className="h-8 px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--topbar-bg))] text-[hsl(var(--topbar-fg))] font-semibold"
              value={localTitle}
              autoFocus
              onChange={(e) => setLocalTitle(e.target.value)}
              onBlur={commitTitle}
              onKeyDown={(e) => {
                if (e.key === 'Enter') commitTitle()
                if (e.key === 'Escape') { setLocalTitle(title || 'Dashboard'); setEditing(false) }
              }}
              aria-label="Edit dashboard title"
            />
          ) : (
            <button
              type="button"
              className="font-semibold truncate text-start hover:underline"
              title={title || 'Dashboard'}
              onClick={() => setEditing(true)}
            >
              {title || 'Dashboard'}
            </button>
          )}
          {createdAt && (
            <span className="opacity-80 whitespace-nowrap text-xs">
              Created {new Date(createdAt).toLocaleDateString(undefined, { day: '2-digit', month: '2-digit', year: 'numeric' })}
            </span>
          )}
        </div>

        <div className="flex items-center gap-2" ref={rootRef}>
          {/* Autosave status */}
          <StatusPill state={saveStatus} onRetry={onRetrySaveAction} className="me-1" />

          {/* Save */}
          <Button variant="primary" size="sm" onClick={onSaveAction} disabled={!hydrated} title="Save dashboard (Ctrl/Cmd+S)">
            Save
          </Button>

          {/* Publish / Unpublish */}
          {!publicId ? (
            <Button variant="secondary" size="sm" onClick={onPublishAction} disabled={!dashboardId} title="Publish a public link">
              Publish
            </Button>
          ) : (
            <Button variant="secondary" size="sm" onClick={onUnpublishAction} disabled={!dashboardId} title="Remove the public link">
              Unpublish
            </Button>
          )}

          {/* Token protection (only when published) */}
          {publicId && (
            !hasToken ? (
              <Button variant="secondary" size="sm" onClick={onSetTokenAction} title="Protect the link with a token">Set token</Button>
            ) : (
              <Button variant="secondary" size="sm" onClick={onRemoveTokenAction} title="Remove token protection">Remove token</Button>
            )
          )}

          {publicId && (
            <Button variant="secondary" size="sm" onClick={handleCopyLink} title="Copy public link">Copy link</Button>
          )}

          {showViewButton && (
            <Button variant="secondary" size="sm" onClick={handleView} title="Open published view">View</Button>
          )}

          <div className="mx-1 h-6 w-px bg-[hsl(var(--border))]" />

          {/* Add Card picker */}
          <div className="relative">
            <Button variant="secondary" size="sm" icon={<RiAddLine className="h-4 w-4" />} onClick={() => toggle('add')}>
              Add Card
            </Button>
            {openMenu === 'add' && (
              <div className="anim-menu-in absolute end-0 mt-1 z-50 rounded-lg border border-[hsl(var(--border))] bg-card shadow-popover p-1 w-[280px]">
                {ADD_CARDS.map(({ kind, label, desc, Icon }) => (
                  <button
                    key={kind}
                    type="button"
                    className="w-full flex items-start gap-3 px-2.5 py-2 rounded-md text-start transition-colors hover:bg-muted"
                    onClick={() => { onAddCardAction?.(kind); setOpenMenu(null) }}
                  >
                    <Icon className="h-4 w-4 mt-0.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                    <span className="min-w-0">
                      <span className="block text-sm font-medium">{label}</span>
                      <span className="block text-2xs text-muted-foreground truncate">{desc}</span>
                    </span>
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Grid density */}
          <div className="relative">
            <button type="button" className={iconBtnClass} onClick={() => toggle('grid')} title="Grid density" aria-label="Grid density">
              <RiLayoutGridLine className="h-4 w-4" aria-hidden="true" />
            </button>
            {openMenu === 'grid' && (
              <div className="anim-menu-in absolute end-0 mt-1 z-50 rounded-lg border border-[hsl(var(--border))] bg-card shadow-popover p-1 w-[200px]">
                {GRID_OPTS.map(({ v, label }) => (
                  <button
                    key={v}
                    type="button"
                    className={`${rowClass} ${(gridSize || 'lg') === v ? 'bg-muted' : ''}`}
                    onClick={() => { onGridSizeChangeAction?.(v); setOpenMenu(null) }}
                  >
                    {label}
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Auto-refresh */}
          <div className="relative">
            <button type="button" className={iconBtnClass} onClick={() => toggle('refresh')} title="Auto-refresh queries" aria-label="Auto-refresh queries">
              <RiRefreshLine className="h-4 w-4" aria-hidden="true" />
            </button>
            {openMenu === 'refresh' && (
              <div className="anim-menu-in absolute end-0 mt-1 z-50 rounded-lg border border-[hsl(var(--border))] bg-card shadow-popover p-1 w-[180px]">
                {REFRESH_OPTS.map(({ v, label }) => (
                  <button
                    key={v}
                    type="button"
                    className={`${rowClass} ${Number(refreshEverySec || 0) === v ? 'bg-muted' : ''}`}
                    onClick={() => { onRefreshEverySecChangeAction?.(v); setOpenMenu(null) }}
                  >
                    {label}
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Lock canvas width */}
          <button
            type="button"
            className={iconBtnClass}
            title={canvasLocked ? 'Unlock canvas width (auto)' : 'Lock canvas width to current'}
            aria-label={canvasLocked ? 'Unlock canvas width' : 'Lock canvas width'}
            aria-pressed={canvasLocked}
            onClick={() => {
              if (canvasLocked) { onCanvasAutoAction?.(); setCanvasLocked(false) }
              else { onCanvasFixedCurrentAction?.(); setCanvasLocked(true) }
            }}
          >
            {canvasLocked ? <RiLockLine className="h-4 w-4" aria-hidden="true" /> : <RiLockUnlockLine className="h-4 w-4" aria-hidden="true" />}
          </button>

          {/* Layout utilities + display options */}
          <div className="relative">
            <button type="button" className={iconBtnClass} onClick={() => toggle('layout')} title="Layout options" aria-label="Layout options">
              <RiLayoutMasonryLine className="h-4 w-4" aria-hidden="true" />
            </button>
            {openMenu === 'layout' && (
              <div className="anim-menu-in absolute end-0 mt-1 z-50 rounded-lg border border-[hsl(var(--border))] bg-card shadow-popover p-2 w-[280px]">
                <button className={rowClass} onClick={() => { onPackRowsFillAction?.(); setOpenMenu(null) }} disabled={!hydrated}>
                  Compact layout (fill gaps)
                </button>
                <button className={rowClass} onClick={() => { onNormalizeLayoutAction?.(); setOpenMenu(null) }} disabled={!hydrated}>
                  Convert to 24-column grid
                </button>
                <div className="h-px bg-[hsl(var(--border))] my-2" />
                <label className="flex items-center justify-between gap-2 px-3 py-1.5 text-sm rounded-md hover:bg-muted">
                  <span>Show data navigator</span>
                  <input type="checkbox" className="h-4 w-4" checked={showNavigator !== false} onChange={(e) => onShowNavigatorChangeAction?.(e.target.checked)} />
                </label>
                <label className="flex items-center justify-between gap-2 px-3 py-1.5 text-sm rounded-md hover:bg-muted">
                  <span>Minimize sidebar on load</span>
                  <input type="checkbox" className="h-4 w-4" checked={autoMinimizeNav !== false} onChange={(e) => onAutoMinimizeNavChangeAction?.(e.target.checked)} />
                </label>
                <div className="h-px bg-[hsl(var(--border))] my-2" />
                <label className="flex items-center justify-between gap-2 px-3 py-1.5 text-sm rounded-md hover:bg-muted">
                  <span>Show global filters on public page</span>
                  <input type="checkbox" className="h-4 w-4" checked={publicShowFilters !== false} onChange={(e) => onPublicShowFiltersChangeAction?.(e.target.checked)} />
                </label>
                <label className="flex items-center justify-between gap-2 px-3 py-1.5 text-sm rounded-md hover:bg-muted">
                  <span>Lock global filters</span>
                  <input type="checkbox" className="h-4 w-4" checked={!!publicLockFilters} onChange={(e) => onPublicLockFiltersChangeAction?.(e.target.checked)} />
                </label>
              </div>
            )}
          </div>
        </div>
      </div>
    </header>
  )
}
