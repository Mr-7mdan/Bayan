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
  RiArrowGoBackLine,
  RiArrowGoForwardLine,
} from '@remixicon/react'
import { Button, StatusPill } from '@/components/ui'
import type { StatusPillState } from '@/components/ui'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { useTranslations } from 'next-intl'

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
  // Undo/redo (history stack lives in page.tsx)
  canUndo?: boolean
  canRedo?: boolean
  onUndoAction?: () => void
  onRedoAction?: () => void
}

// Labels are resolved from the `builder.addCard.*` messages at render time using `kind`.
const ADD_CARDS: Array<{ kind: AddKind; Icon: ComponentType<any> }> = [
  { kind: 'chart', Icon: RiBarChartBoxLine },
  { kind: 'table', Icon: RiTableLine },
  { kind: 'kpi', Icon: RiNumbersLine },
  { kind: 'text', Icon: RiText },
  { kind: 'spacer', Icon: RiSpace },
  { kind: 'composition', Icon: RiLayoutMasonryLine },
  { kind: 'report', Icon: RiFileTextLine },
]

// `key` maps to `builder.refreshOpts.*` messages.
const REFRESH_OPTS: Array<{ v: number; key: string }> = [
  { v: 0, key: 'off' }, { v: 60, key: '1m' }, { v: 180, key: '3m' },
  { v: 300, key: '5m' }, { v: 900, key: '15m' }, { v: 1800, key: '30m' },
  { v: 3600, key: '1h' }, { v: 21600, key: '6h' }, { v: 86400, key: '24h' },
]

// `v` maps directly to `builder.gridOpts.*` messages.
const GRID_OPTS: Array<{ v: 'sm'|'md'|'lg'|'xl' }> = [
  { v: 'sm' }, { v: 'md' }, { v: 'lg' }, { v: 'xl' },
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
  canUndo = false,
  canRedo = false,
  onUndoAction,
  onRedoAction,
}: TitleBarProps) {
  const { env } = useEnvironment()
  const t = useTranslations('builder')
  const trimmedToken = token?.trim() ?? ''
  const hasToken = trimmedToken.length > 0

  // Inline title editing state
  const [editing, setEditing] = useState(false)
  const [localTitle, setLocalTitle] = useState<string>(title || t('toolbar.defaultTitle'))
  useEffect(() => { setLocalTitle(title || t('toolbar.defaultTitle')) }, [title, t])

  const commitTitle = () => {
    const next = (localTitle || '').trim() || t('toolbar.untitled')
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
        const entered = window.prompt(t('toolbar.copyLinkPrompt'))
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
                if (e.key === 'Escape') { setLocalTitle(title || t('toolbar.defaultTitle')); setEditing(false) }
              }}
              aria-label={t('toolbar.editTitleAria')}
            />
          ) : (
            <button
              type="button"
              className="font-semibold truncate text-start hover:underline"
              title={title || t('toolbar.defaultTitle')}
              onClick={() => setEditing(true)}
            >
              {title || t('toolbar.defaultTitle')}
            </button>
          )}
          {createdAt && (
            <span className="opacity-80 whitespace-nowrap text-xs">
              {t('toolbar.created', { date: new Date(createdAt).toLocaleDateString(undefined, { day: '2-digit', month: '2-digit', year: 'numeric' }) })}
            </span>
          )}
        </div>

        <div className="flex items-center gap-2" ref={rootRef}>
          {/* Undo / redo */}
          <button
            type="button"
            className={`${iconBtnClass} disabled:opacity-40 disabled:cursor-not-allowed`}
            onClick={onUndoAction}
            disabled={!canUndo}
            title={t('toolbar.undoTooltip')}
            aria-label={t('toolbar.undo')}
          >
            <RiArrowGoBackLine className="h-4 w-4" aria-hidden="true" />
          </button>
          <button
            type="button"
            className={`${iconBtnClass} disabled:opacity-40 disabled:cursor-not-allowed`}
            onClick={onRedoAction}
            disabled={!canRedo}
            title={t('toolbar.redoTooltip')}
            aria-label={t('toolbar.redo')}
          >
            <RiArrowGoForwardLine className="h-4 w-4" aria-hidden="true" />
          </button>

          <div className="mx-1 h-6 w-px bg-[hsl(var(--border))]" />

          {/* Autosave status */}
          <StatusPill state={saveStatus} onRetry={onRetrySaveAction} className="me-1" />

          {/* Save */}
          <Button variant="primary" size="sm" onClick={onSaveAction} disabled={!hydrated} title={t('toolbar.saveTooltip')}>
            {t('toolbar.save')}
          </Button>

          {/* Publish / Unpublish */}
          {!publicId ? (
            <Button variant="secondary" size="sm" onClick={onPublishAction} disabled={!dashboardId} title={t('toolbar.publishTooltip')}>
              {t('toolbar.publish')}
            </Button>
          ) : (
            <Button variant="secondary" size="sm" onClick={onUnpublishAction} disabled={!dashboardId} title={t('toolbar.unpublishTooltip')}>
              {t('toolbar.unpublish')}
            </Button>
          )}

          {/* Token protection (only when published) */}
          {publicId && (
            !hasToken ? (
              <Button variant="secondary" size="sm" onClick={onSetTokenAction} title={t('toolbar.setTokenTooltip')}>{t('toolbar.setToken')}</Button>
            ) : (
              <Button variant="secondary" size="sm" onClick={onRemoveTokenAction} title={t('toolbar.removeTokenTooltip')}>{t('toolbar.removeToken')}</Button>
            )
          )}

          {publicId && (
            <Button variant="secondary" size="sm" onClick={handleCopyLink} title={t('toolbar.copyLinkTooltip')}>{t('toolbar.copyLink')}</Button>
          )}

          {showViewButton && (
            <Button variant="secondary" size="sm" onClick={handleView} title={t('toolbar.viewTooltip')}>{t('toolbar.view')}</Button>
          )}

          <div className="mx-1 h-6 w-px bg-[hsl(var(--border))]" />

          {/* Add Card picker */}
          <div className="relative">
            <Button variant="secondary" size="sm" icon={<RiAddLine className="h-4 w-4" />} onClick={() => toggle('add')}>
              {t('toolbar.addCard')}
            </Button>
            {openMenu === 'add' && (
              <div className="anim-menu-in absolute end-0 mt-1 z-50 rounded-lg border border-[hsl(var(--border))] bg-card shadow-popover p-1 w-[280px]">
                {ADD_CARDS.map(({ kind, Icon }) => (
                  <button
                    key={kind}
                    type="button"
                    className="w-full flex items-start gap-3 px-2.5 py-2 rounded-md text-start transition-colors hover:bg-muted"
                    onClick={() => { onAddCardAction?.(kind); setOpenMenu(null) }}
                  >
                    <Icon className="h-4 w-4 mt-0.5 shrink-0 text-muted-foreground" aria-hidden="true" />
                    <span className="min-w-0">
                      <span className="block text-sm font-medium">{t(`addCard.${kind}`)}</span>
                      <span className="block text-2xs text-muted-foreground truncate">{t(`addCard.${kind}Desc`)}</span>
                    </span>
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Grid density */}
          <div className="relative">
            <button type="button" className={iconBtnClass} onClick={() => toggle('grid')} title={t('toolbar.gridDensity')} aria-label={t('toolbar.gridDensity')}>
              <RiLayoutGridLine className="h-4 w-4" aria-hidden="true" />
            </button>
            {openMenu === 'grid' && (
              <div className="anim-menu-in absolute end-0 mt-1 z-50 rounded-lg border border-[hsl(var(--border))] bg-card shadow-popover p-1 w-[200px]">
                {GRID_OPTS.map(({ v }) => (
                  <button
                    key={v}
                    type="button"
                    className={`${rowClass} ${(gridSize || 'lg') === v ? 'bg-muted' : ''}`}
                    onClick={() => { onGridSizeChangeAction?.(v); setOpenMenu(null) }}
                  >
                    {t(`gridOpts.${v}`)}
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Auto-refresh */}
          <div className="relative">
            <button type="button" className={iconBtnClass} onClick={() => toggle('refresh')} title={t('toolbar.autoRefresh')} aria-label={t('toolbar.autoRefresh')}>
              <RiRefreshLine className="h-4 w-4" aria-hidden="true" />
            </button>
            {openMenu === 'refresh' && (
              <div className="anim-menu-in absolute end-0 mt-1 z-50 rounded-lg border border-[hsl(var(--border))] bg-card shadow-popover p-1 w-[180px]">
                {REFRESH_OPTS.map(({ v, key }) => (
                  <button
                    key={v}
                    type="button"
                    className={`${rowClass} ${Number(refreshEverySec || 0) === v ? 'bg-muted' : ''}`}
                    onClick={() => { onRefreshEverySecChangeAction?.(v); setOpenMenu(null) }}
                  >
                    {t(`refreshOpts.${key}`)}
                  </button>
                ))}
              </div>
            )}
          </div>

          {/* Lock canvas width */}
          <button
            type="button"
            className={iconBtnClass}
            title={canvasLocked ? t('toolbar.unlockCanvas') : t('toolbar.lockCanvasToCurrent')}
            aria-label={canvasLocked ? t('toolbar.unlockCanvasAria') : t('toolbar.lockCanvasAria')}
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
            <button type="button" className={iconBtnClass} onClick={() => toggle('layout')} title={t('toolbar.layoutOptions')} aria-label={t('toolbar.layoutOptions')}>
              <RiLayoutMasonryLine className="h-4 w-4" aria-hidden="true" />
            </button>
            {openMenu === 'layout' && (
              <div className="anim-menu-in absolute end-0 mt-1 z-50 rounded-lg border border-[hsl(var(--border))] bg-card shadow-popover p-2 w-[280px]">
                <button className={rowClass} onClick={() => { onPackRowsFillAction?.(); setOpenMenu(null) }} disabled={!hydrated}>
                  {t('layoutMenu.compact')}
                </button>
                <button className={rowClass} onClick={() => { onNormalizeLayoutAction?.(); setOpenMenu(null) }} disabled={!hydrated}>
                  {t('layoutMenu.convert24')}
                </button>
                <div className="h-px bg-[hsl(var(--border))] my-2" />
                <label className="flex items-center justify-between gap-2 px-3 py-1.5 text-sm rounded-md hover:bg-muted">
                  <span>{t('layoutMenu.showNavigator')}</span>
                  <input type="checkbox" className="h-4 w-4" checked={showNavigator !== false} onChange={(e) => onShowNavigatorChangeAction?.(e.target.checked)} />
                </label>
                <label className="flex items-center justify-between gap-2 px-3 py-1.5 text-sm rounded-md hover:bg-muted">
                  <span>{t('layoutMenu.minimizeSidebar')}</span>
                  <input type="checkbox" className="h-4 w-4" checked={autoMinimizeNav !== false} onChange={(e) => onAutoMinimizeNavChangeAction?.(e.target.checked)} />
                </label>
                <div className="h-px bg-[hsl(var(--border))] my-2" />
                <label className="flex items-center justify-between gap-2 px-3 py-1.5 text-sm rounded-md hover:bg-muted">
                  <span>{t('layoutMenu.showPublicFilters')}</span>
                  <input type="checkbox" className="h-4 w-4" checked={publicShowFilters !== false} onChange={(e) => onPublicShowFiltersChangeAction?.(e.target.checked)} />
                </label>
                <label className="flex items-center justify-between gap-2 px-3 py-1.5 text-sm rounded-md hover:bg-muted">
                  <span>{t('layoutMenu.lockFilters')}</span>
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
