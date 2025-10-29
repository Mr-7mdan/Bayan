"use client"

import { useState, useEffect, useRef } from 'react'
import { Button, Select, SelectItem } from '@tremor/react'
import { RiMore2Fill, RiLayoutGrid2Line } from '@remixicon/react'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'

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
  onAddCardAction?: (kind: 'kpi'|'chart'|'table'|'text'|'spacer'|'composition') => void
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
}

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
  onCanvasFixedAction,
  onCanvasFixedCurrentAction,
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

  const handlePublishToggle = () => {
    if (publicId) onUnpublishAction()
    else onPublishAction()
  }

  const handleView = () => {
    if (!publicId) return
    const base = ((env.publicDomain && env.publicDomain.trim())
      ? env.publicDomain
      : (typeof window !== 'undefined' ? window.location.origin : '')
    ).replace(/\/$/, '')
    const url = `${base}/v/${publicId}${hasToken ? `?token=${encodeURIComponent(trimmedToken)}` : ''}`
    window.open(url, '_blank', 'noopener,noreferrer')
  }
 
  const showViewButton = Boolean(publicId) && (!isProtected || hasToken)

  // Target look: Secondary buttons with subtle border and surface for both themes
  const secondaryBtnClass =
    'h-10 px-4 text-sm font-medium rounded-lg border border-[hsl(var(--border))] ring-1 ring-inset ring-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--foreground))] transition-colors hover:bg-[hsl(var(--muted))]'
  const iconBtnClass = secondaryBtnClass + ' w-10 px-0 flex items-center justify-center'
  const [showAddPicker, setShowAddPicker] = useState(false)
  const [actionsOpen, setActionsOpen] = useState(false)
  const addRef = useRef<HTMLDivElement | null>(null)
  const actionsRef = useRef<HTMLDivElement | null>(null)

  // Close pickers on outside click or Escape
  useEffect(() => {
    const onDocPointer = (e: MouseEvent) => {
      const t = e.target as Node
      const el = t as Element
      // If the click lands inside an open Select/Listbox popover (often portaled), do not close the actions menu.
      const inSelectPopover = !!(
        el && (
          el.closest('[role="listbox"]') ||
          el.closest('[data-headlessui-portal]') ||
          el.closest('[data-radix-popper-content-wrapper]') ||
          el.closest('.tremor-Select') ||
          el.closest('.tremor-Listbox')
        )
      )
      if (showAddPicker && addRef.current && !addRef.current.contains(t) && !inSelectPopover) setShowAddPicker(false)
      if (actionsOpen && actionsRef.current && !actionsRef.current.contains(t) && !inSelectPopover) setActionsOpen(false)
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') { setShowAddPicker(false); setActionsOpen(false) }
    }
    document.addEventListener('mousedown', onDocPointer)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDocPointer)
      document.removeEventListener('keydown', onKey)
    }
  }, [showAddPicker, actionsOpen])

  return (
    <header className="sticky top-0 z-40 border-b border-[hsl(var(--border))] bg-[hsl(var(--topbar-bg))] text-[hsl(var(--topbar-fg))]">
      <div className="mx-auto w-full px-6 py-3 flex items-center gap-4 justify-between">
        <div className="flex items-center gap-3">
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
              className="font-semibold truncate text-left hover:underline"
              title={title || 'Dashboard'}
              onClick={() => setEditing(true)}
            >
              {title || 'Dashboard'}
            </button>
          )}
          {createdAt && (
            <span className="opacity-80 whitespace-nowrap">
              Created on {new Date(createdAt).toLocaleDateString(undefined, { day: '2-digit', month: '2-digit', year: 'numeric' })}
            </span>
          )}
        </div>
        <div className="flex items-center gap-3">
          {publicId && (
            <div className="flex items-center gap-2">
              {!hasToken ? (
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={onSetTokenAction}
                  title="Protect the link with a token"
                >
                  Set token
                </Button>
              ) : (
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={onRemoveTokenAction}
                  title="Remove token protection"
                >
                  Remove token
                </Button>
              )}
            </div>
          )}
          {/* Add Card moved into header */}
          <div className="relative" ref={addRef}>
            <Button
              variant="secondary"
              size="sm"
              onClick={() => setShowAddPicker((v) => !v)}
              className={secondaryBtnClass}
            >
              + Add Card
            </Button>
            {showAddPicker && (
              <div className="absolute right-0 mt-1 z-50 rounded-md border bg-card shadow-card p-2 grid grid-cols-2 gap-2 w-[260px]">
                {(['kpi','chart','table','text','spacer','composition'] as const).map((t) => (
                  <button key={t} className="text-xs px-2 py-1 rounded border hover:bg-muted" onClick={() => { onAddCardAction && onAddCardAction(t); setShowAddPicker(false) }}>{t.toUpperCase()}</button>
                ))}
              </div>
            )}
          </div>
          {/* Actions menu (Save / Publish / Unpublish + Grid Size) */}
          <div className="relative" ref={actionsRef}>
            <Button
              variant="secondary"
              size="sm"
              onClick={() => setActionsOpen((v) => !v)}
              aria-label="More actions"
              className={iconBtnClass}
            >
              <RiMore2Fill className="h-4 w-4" />
            </Button>
            {actionsOpen && (
              <div className="absolute right-0 mt-1 z-50 rounded-md border bg-card shadow-card p-2 w-[280px]">
                <div className="flex flex-col gap-1">
                  <button className="w-full text-left text-sm px-3 py-2 rounded-md transition-colors hover:bg-secondary/60 hover:ring-1 hover:ring-inset hover:ring-[hsl(var(--border))] disabled:opacity-50" onClick={() => { onSaveAction?.(); setActionsOpen(false) }} disabled={!hydrated}>Save</button>
                  <button className="w-full text-left text-sm px-3 py-2 rounded-md transition-colors hover:bg-secondary/60 hover:ring-1 hover:ring-inset hover:ring-[hsl(var(--border))] disabled:opacity-50" onClick={() => { onPackRowsFillAction?.(); setActionsOpen(false) }} disabled={!hydrated}>Pack rows left + Fill</button>
                  <button className="w-full text-left text-sm px-3 py-2 rounded-md transition-colors hover:bg-secondary/60 hover:ring-1 hover:ring-inset hover:ring-[hsl(var(--border))] disabled:opacity-50" onClick={() => { onNormalizeLayoutAction?.(); setActionsOpen(false) }} disabled={!hydrated}>Normalize to 24 cols</button>
                  <div className="h-px bg-[hsl(var(--border))] my-1" />
                  <button className="w-full text-left text-sm px-3 py-2 rounded-md transition-colors hover:bg-secondary/60 hover:ring-1 hover:ring-inset hover:ring-[hsl(var(--border))] disabled:opacity-50" onClick={() => { onCanvasFixedCurrentAction?.(); setActionsOpen(false) }} disabled={!hydrated}>Lock canvas width to current</button>
                  <button className="w-full text-left text-sm px-3 py-2 rounded-md transition-colors hover:bg-secondary/60 hover:ring-1 hover:ring-inset hover:ring-[hsl(var(--border))] disabled:opacity-50" onClick={() => { onCanvasAutoAction?.(); setActionsOpen(false) }} disabled={!hydrated}>Unlock canvas width (auto)</button>
                  {!publicId && (
                    <button className="w-full text-left text-sm px-3 py-2 rounded-md transition-colors hover:bg-secondary/60 hover:ring-1 hover:ring-inset hover:ring-[hsl(var(--border))] disabled:opacity-50" onClick={() => { onPublishAction?.(); setActionsOpen(false) }} disabled={!dashboardId}>Publish</button>
                  )}
                  {!!publicId && (
                    <>
                      <button className="w-full text-left text-sm px-3 py-2 rounded-md transition-colors hover:bg-secondary/60 hover:ring-1 hover:ring-inset hover:ring-[hsl(var(--border))] disabled:opacity-50" onClick={() => { onUnpublishAction?.(); setActionsOpen(false) }} disabled={!dashboardId}>Unpublish</button>
                      <button
                        className="w-full text-left text-sm px-3 py-2 rounded-md transition-colors hover:bg-secondary/60 hover:ring-1 hover:ring-inset hover:ring-[hsl(var(--border))] disabled:opacity-50"
                        onClick={() => {
                          try {
                            const base = ((env.publicDomain && env.publicDomain.trim())
                              ? env.publicDomain
                              : (typeof window !== 'undefined' ? window.location.origin : '')
                            ).replace(/\/$/, '')
                            let tokenToUse = trimmedToken
                            if (isProtected && !tokenToUse) {
                              // Ask once and persist for future copies in this browser
                              const entered = typeof window !== 'undefined' ? window.prompt('Enter the access token to include in the link') : ''
                              if (entered && entered.trim()) {
                                tokenToUse = entered.trim()
                                try {
                                  onTokenChangeAction?.(tokenToUse)
                                  if (dashboardId) localStorage.setItem(`dash_pub_token_${dashboardId}`, tokenToUse)
                                } catch {}
                              }
                            }
                            const url = `${base}/v/${publicId}${(isProtected && tokenToUse) ? `?token=${encodeURIComponent(tokenToUse)}` : ''}`
                            if (navigator?.clipboard?.writeText) navigator.clipboard.writeText(url)
                          } catch {}
                          setActionsOpen(false)
                        }}
                        disabled={!dashboardId}
                      >Copy public URL</button>
                    </>
                  )}
                  <div className="h-px bg-[hsl(var(--border))] my-1" />
                  <div className="flex items-center gap-2 px-1">
                    <RiLayoutGrid2Line className="h-4 w-4 opacity-80" />
                    <Select
                      value={(gridSize || 'lg')}
                      onValueChange={(v)=> onGridSizeChangeAction && onGridSizeChangeAction(v as any)}
                      className="w-[180px] text-xs rounded-md"
                    >
                      <SelectItem value="sm">Small (24 cols)</SelectItem>
                      <SelectItem value="md">Medium (18 cols)</SelectItem>
                      <SelectItem value="lg">Large (12 cols)</SelectItem>
                      <SelectItem value="xl">XL (8 cols)</SelectItem>
                    </Select>
                  </div>
                  <div className="flex items-center justify-between gap-2 px-1 text-sm py-1 rounded-md">
                    <span>Refresh Queries every</span>
                    <Select
                      value={String(refreshEverySec || 0)}
                      onValueChange={(v) => onRefreshEverySecChangeAction && onRefreshEverySecChangeAction(Number(v))}
                      className="w-[140px] text-xs rounded-md"
                    >
                      <SelectItem value="0">Off</SelectItem>
                      <SelectItem value="60">1m</SelectItem>
                      <SelectItem value="180">3m</SelectItem>
                      <SelectItem value="300">5m</SelectItem>
                      <SelectItem value="900">15m</SelectItem>
                      <SelectItem value="1800">30m</SelectItem>
                      <SelectItem value="3600">1hr</SelectItem>
                      <SelectItem value="21600">6hr</SelectItem>
                      <SelectItem value="86400">24hr</SelectItem>
                    </Select>
                  </div>
                  <div className="h-px bg-[hsl(var(--border))] my-2" />
                  <label className="flex items-center justify-between gap-2 px-1 text-sm py-1 rounded-md hover:bg-secondary/60">
                    <span>Show Data Navigator</span>
                    <input
                      type="checkbox"
                      className="h-4 w-4"
                      checked={showNavigator !== false}
                      onChange={(e) => { onShowNavigatorChangeAction?.(e.target.checked); setActionsOpen(false) }}
                    />
                  </label>
                  <label className="flex items-center justify-between gap-2 px-1 text-sm py-1 rounded-md hover:bg-secondary/60">
                    <span>Minimize sidebar on load</span>
                    <input
                      type="checkbox"
                      className="h-4 w-4"
                      checked={autoMinimizeNav !== false}
                      onChange={(e) => { onAutoMinimizeNavChangeAction?.(e.target.checked); setActionsOpen(false) }}
                    />
                  </label>
                  <div className="h-px bg-[hsl(var(--border))] my-2" />
                  <label className="flex items-center justify-between gap-2 px-1 text-sm py-1 rounded-md hover:bg-secondary/60">
                    <span>Show Global Filters in Public Page</span>
                    <input
                      type="checkbox"
                      className="h-4 w-4"
                      checked={publicShowFilters !== false}
                      onChange={(e) => { onPublicShowFiltersChangeAction?.(e.target.checked); setActionsOpen(false) }}
                    />
                  </label>
                  <label className="flex items-center justify-between gap-2 px-1 text-sm py-1 rounded-md hover:bg-secondary/60">
                    <span>Lock Global Filters</span>
                    <input
                      type="checkbox"
                      className="h-4 w-4"
                      checked={!!publicLockFilters}
                      onChange={(e) => { onPublicLockFiltersChangeAction?.(e.target.checked); setActionsOpen(false) }}
                    />
                  </label>
                </div>
              </div>
            )}
          </div>
          {showViewButton && (
            <Button
              variant="secondary"
              size="sm"
              onClick={handleView}
              title="Open published view"
              className={secondaryBtnClass}
            >
              View
            </Button>
          )}
        </div>
      </div>
    </header>
  )
}
