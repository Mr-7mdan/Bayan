"use client"

import React, { useEffect, useMemo, useState } from 'react'
import { useTranslations } from 'next-intl'
import {
  RiCheckLine, RiErrorWarningLine, RiAlertLine, RiRefreshLine,
  RiAddCircleLine, RiPencilLine, RiSearchLine,
} from '@remixicon/react'
import { Api, type DatasourceImportItem, type DatasourceImportResponse, type DatasourceOut } from '@/lib/api'
import { Button, Input, Modal } from '@/components/ui'

interface Props {
  open: boolean
  onClose: () => void
  /** Raw items parsed from the export file (already unwrapped from any envelope). */
  parsed: DatasourceImportItem[]
  /** Datasources already visible to this user — used to detect name collisions. */
  existing: DatasourceOut[]
  actorId?: string
  /** Called after a completed import so the caller can refresh its list. */
  onImported: (res: DatasourceImportResponse) => void
}

type Row = {
  key: string
  raw: DatasourceImportItem
  name: string
  originalName: string
  selected: boolean
}

/** `pcma` → `pcma (2)` → `pcma (3)`, skipping anything already taken. */
function uniqueName(base: string, taken: Set<string>) {
  const stripped = base.replace(/\s*\(\d+\)$/, '')
  for (let n = 2; n < 1000; n++) {
    const candidate = `${stripped} (${n})`
    if (!taken.has(candidate.toLowerCase())) return candidate
  }
  return `${stripped} (${Date.now()})`
}

export default function ImportDatasourcesWizard({
  open, onClose, parsed, existing, actorId, onImported,
}: Props) {
  // messages/<loc>/data.json is mounted under the 'data' namespace by
  // messages/loader.ts, so the full path carries that prefix.
  const t = useTranslations('data.datasources.list.importWizard')
  const [rows, setRows] = useState<Row[]>([])
  const [query, setQuery] = useState('')
  const [busy, setBusy] = useState(false)
  const [fatal, setFatal] = useState<string | null>(null)
  const [result, setResult] = useState<DatasourceImportResponse | null>(null)

  // Re-seed whenever a new file is opened. Everything starts selected: the
  // common case is "import all of it", and deselecting is cheaper than hunting.
  useEffect(() => {
    if (!open) return
    setRows(parsed.map((raw, i) => ({
      key: `${raw.id || raw.name || 'item'}-${i}`,
      raw,
      name: raw.name || '',
      originalName: raw.name || '',
      selected: true,
    })))
    setQuery('')
    setResult(null)
    setFatal(null)
    setBusy(false)
  }, [open, parsed])

  const existingNames = useMemo(
    () => new Set((existing || []).map((d) => (d.name || '').toLowerCase())),
    [existing],
  )

  // A name collides if it matches something already here, or another selected
  // row in this same file (two rows importing to one name is a silent overwrite).
  const nameCounts = useMemo(() => {
    const m = new Map<string, number>()
    rows.filter((r) => r.selected).forEach((r) => {
      const k = r.name.trim().toLowerCase()
      m.set(k, (m.get(k) || 0) + 1)
    })
    return m
  }, [rows])

  const statusOf = (r: Row): 'new' | 'overwrite' | 'duplicate' | 'invalid' => {
    const k = r.name.trim().toLowerCase()
    if (!k) return 'invalid'
    if ((nameCounts.get(k) || 0) > 1) return 'duplicate'
    return existingNames.has(k) ? 'overwrite' : 'new'
  }

  const visible = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q) return rows
    return rows.filter((r) =>
      r.name.toLowerCase().includes(q) ||
      r.originalName.toLowerCase().includes(q) ||
      (r.raw.type || '').toLowerCase().includes(q))
  }, [rows, query])

  const selected = rows.filter((r) => r.selected)
  const counts = {
    total: rows.length,
    selected: selected.length,
    new: selected.filter((r) => statusOf(r) === 'new').length,
    overwrite: selected.filter((r) => statusOf(r) === 'overwrite').length,
    blocked: selected.filter((r) => ['duplicate', 'invalid'].includes(statusOf(r))).length,
  }

  const setRow = (key: string, patch: Partial<Row>) =>
    setRows((p) => p.map((r) => (r.key === key ? { ...r, ...patch } : r)))

  const selectAll = (v: boolean) => setRows((p) => p.map((r) => ({ ...r, selected: v })))

  const selectOnlyNew = () =>
    setRows((p) => p.map((r) => ({ ...r, selected: !existingNames.has(r.name.trim().toLowerCase()) })))

  /** Give every colliding selected row a free name, so nothing gets overwritten. */
  const renameConflicts = () => {
    const taken = new Set(existingNames)
    setRows((p) => p.map((r) => {
      if (!r.selected) { taken.add(r.name.trim().toLowerCase()); return r }
      const k = r.name.trim().toLowerCase()
      if (!taken.has(k)) { taken.add(k); return r }
      const next = uniqueName(r.name.trim() || r.originalName, taken)
      taken.add(next.toLowerCase())
      return { ...r, name: next }
    }))
  }

  const resetNames = () => setRows((p) => p.map((r) => ({ ...r, name: r.originalName })))

  async function runImport() {
    setBusy(true)
    setFatal(null)
    try {
      const items: DatasourceImportItem[] = selected.map((r) => {
        // userId is deliberately dropped: it belongs to the instance that
        // produced the file. The backend also guards this, but not sending a
        // foreign owner keeps the intent obvious in the request itself.
        const { userId, createdAt, ...rest } = r.raw
        return { ...rest, name: r.name.trim() }
      })
      const res = await Api.importDatasources(items, actorId)
      setResult(res)
      onImported(res)
    } catch (e: any) {
      setFatal(e?.message || t('failedUnknown'))
    } finally {
      setBusy(false)
    }
  }

  const canImport = counts.selected > 0 && counts.blocked === 0 && !busy

  const badge = (r: Row) => {
    const s = statusOf(r)
    const map = {
      new: ['text-[hsl(var(--success))] bg-[hsl(var(--success)/0.12)]', RiAddCircleLine, t('badgeNew')],
      overwrite: ['text-amber-600 dark:text-amber-500 bg-amber-500/12', RiPencilLine, t('badgeOverwrite')],
      duplicate: ['text-[hsl(var(--danger))] bg-[hsl(var(--danger)/0.12)]', RiErrorWarningLine, t('badgeDuplicate')],
      invalid: ['text-[hsl(var(--danger))] bg-[hsl(var(--danger)/0.12)]', RiErrorWarningLine, t('badgeNoName')],
    } as const
    const [cls, Icon, label] = map[s]
    return (
      <span className={`inline-flex shrink-0 items-center gap-1 rounded-md px-1.5 py-0.5 text-[11px] font-medium ${cls}`}>
        <Icon className="h-3 w-3" />{label}
      </span>
    )
  }

  // ── Step 2: outcome ────────────────────────────────────────────────────────
  if (result) {
    const problems = result.results.filter((r) => r.status === 'failed' || r.warnings.length || r.syncTasksFailed > 0)
    return (
      <Modal
        open={open}
        onClose={onClose}
        size="lg"
        title={t('doneTitle')}
        description={t('doneSummary', {
          created: result.created, updated: result.updated, failed: result.failed,
        })}
        footer={<div className="flex justify-end"><Button onClick={onClose}>{t('close')}</Button></div>}
      >
        <div className="max-h-[55vh] space-y-2">
          {!problems.length && (
            <div className="flex items-center gap-2 rounded-md bg-[hsl(var(--success)/0.1)] px-3 py-2 text-sm text-[hsl(var(--success))]">
              <RiCheckLine className="h-4 w-4" />{t('allClean')}
            </div>
          )}
          {problems.map((r, i) => {
            const bad = r.status === 'failed'
            return (
              <div
                key={`${r.sourceName}-${i}`}
                className={`rounded-md border px-3 py-2 ${bad
                  ? 'border-[hsl(var(--danger)/0.4)] bg-[hsl(var(--danger)/0.08)]'
                  : 'border-amber-500/40 bg-amber-500/8'}`}
              >
                <div className="flex items-center gap-2 text-sm font-medium">
                  {bad
                    ? <RiErrorWarningLine className="h-4 w-4 text-[hsl(var(--danger))]" />
                    : <RiAlertLine className="h-4 w-4 text-amber-600 dark:text-amber-500" />}
                  <span className="truncate">{r.sourceName}</span>
                  {r.name !== r.sourceName && (
                    <span className="text-xs text-muted-foreground">→ {r.name}</span>
                  )}
                </div>
                {r.message && <p className="mt-1 text-xs font-mono text-muted-foreground break-words">{r.message}</p>}
                {r.warnings.map((w, j) => (
                  <p key={j} className="mt-1 text-xs text-muted-foreground">• {w}</p>
                ))}
                {r.syncTasksFailed > 0 && (
                  <p className="mt-1 text-xs text-muted-foreground">
                    {t('syncTaskSummary', { ok: r.syncTasksImported, bad: r.syncTasksFailed })}
                  </p>
                )}
              </div>
            )
          })}

          <details className="pt-2">
            <summary className="cursor-pointer text-xs text-muted-foreground">{t('showAll')}</summary>
            <div className="mt-2 space-y-1">
              {result.results.map((r, i) => (
                <div key={i} className="flex items-center justify-between gap-3 text-xs border-b border-[hsl(var(--border))] py-1">
                  <span className="truncate">{r.name}</span>
                  <span className="shrink-0 text-muted-foreground">{t(`status.${r.status}`)}</span>
                </div>
              ))}
            </div>
          </details>
        </div>
      </Modal>
    )
  }

  // ── Step 1: review & select ────────────────────────────────────────────────
  return (
    <Modal
      open={open}
      onClose={busy ? () => {} : onClose}
      size="lg"
      title={t('title')}
      description={t('subtitle', { count: rows.length })}
      footer={(
        <div className="flex w-full items-center justify-between gap-3">
          <div className="text-xs text-muted-foreground">
            {counts.blocked > 0
              ? <span className="text-[hsl(var(--danger))]">{t('blocked', { count: counts.blocked })}</span>
              : t('footerSummary', { total: counts.selected, new: counts.new, overwrite: counts.overwrite })}
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={onClose} disabled={busy}>{t('cancel')}</Button>
            <Button onClick={runImport} disabled={!canImport}>
              {busy ? t('importing') : t('importN', { count: counts.selected })}
            </Button>
          </div>
        </div>
      )}
    >
      {/* -m-4 cancels Modal's body padding so the toolbar rule and the row
          hover states run edge to edge. */}
      <div className="-m-4 flex flex-col min-h-0">
        {fatal && (
          <div className="mx-4 mt-3 flex items-start gap-2 rounded-md border border-[hsl(var(--danger)/0.4)] bg-[hsl(var(--danger)/0.08)] px-3 py-2 text-xs text-[hsl(var(--danger))]">
            <RiErrorWarningLine className="mt-0.5 h-4 w-4 shrink-0" />
            <span className="break-words">{fatal}</span>
          </div>
        )}

        <div className="flex flex-wrap items-center gap-2 border-b border-[hsl(var(--border))] px-4 py-2">
          <div className="relative flex-1 min-w-[160px]">
            <RiSearchLine className="pointer-events-none absolute start-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
            <Input
              size="sm"
              className="ps-7"
              placeholder={t('searchPlaceholder')}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
            />
          </div>
          <Button size="sm" variant="outline" onClick={() => selectAll(true)}>{t('selectAll')}</Button>
          <Button size="sm" variant="outline" onClick={() => selectAll(false)}>{t('selectNone')}</Button>
          <Button size="sm" variant="outline" onClick={selectOnlyNew}>{t('onlyNew')}</Button>
          <Button size="sm" variant="outline" onClick={renameConflicts}>{t('renameConflicts')}</Button>
          <Button size="sm" variant="ghost" onClick={resetNames} title={t('resetNames')}>
            <RiRefreshLine className="h-3.5 w-3.5" />
          </Button>
        </div>

        <div className="max-h-[50vh] min-h-0 overflow-y-auto px-2 py-2">
          {!visible.length && (
            <p className="px-2 py-6 text-center text-sm text-muted-foreground">{t('noMatch')}</p>
          )}
          {visible.map((r) => {
            const s = statusOf(r)
            return (
              <div
                key={r.key}
                className={`flex items-start gap-3 rounded-md px-2 py-2 hover:bg-[hsl(var(--muted))]/40 ${r.selected ? '' : 'opacity-55'}`}
              >
                <input
                  type="checkbox"
                  className="mt-2 h-4 w-4 shrink-0 cursor-pointer accent-[hsl(var(--primary))]"
                  checked={r.selected}
                  aria-label={t('selectRow', { name: r.originalName })}
                  onChange={(e) => setRow(r.key, { selected: e.target.checked })}
                />
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <Input
                      size="sm"
                      value={r.name}
                      disabled={!r.selected}
                      error={r.selected && ['duplicate', 'invalid'].includes(s)}
                      onChange={(e) => setRow(r.key, { name: e.target.value })}
                      aria-label={t('targetName', { name: r.originalName })}
                    />
                    {badge(r)}
                  </div>
                  <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-0.5 text-[11px] text-muted-foreground">
                    <span className="font-mono">{r.raw.type}</span>
                    {r.name.trim() !== r.originalName && (
                      <span>{t('wasNamed', { name: r.originalName })}</span>
                    )}
                    {!!r.raw.syncTasks?.length && (
                      <span>{t('syncTaskCount', { count: r.raw.syncTasks.length })}</span>
                    )}
                    {!r.raw.connectionUri && (
                      <span className="text-amber-600 dark:text-amber-500">{t('noConnection')}</span>
                    )}
                    {r.raw.active === false && <span>{t('inactive')}</span>}
                  </div>
                  {r.selected && s === 'overwrite' && (
                    <p className="mt-1 text-[11px] text-amber-600 dark:text-amber-500">{t('overwriteHint')}</p>
                  )}
                  {r.selected && s === 'duplicate' && (
                    <p className="mt-1 text-[11px] text-[hsl(var(--danger))]">{t('duplicateHint')}</p>
                  )}
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </Modal>
  )
}
