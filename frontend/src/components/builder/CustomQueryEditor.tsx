'use client'

import React, { useState, useCallback, useMemo } from 'react'
import { RiCodeSSlashLine, RiListCheck2, RiAddLine, RiMagicLine } from '@remixicon/react'
import * as sqlFormatter from 'sql-formatter-plus'
import {
  type CM, type Condition, type ColKind, type Connector,
  inferKind, opsForKind, uid, buildQuery,
  ColumnSelector, ConditionRow,
} from './CustomQueryEditorParts'

function tryFmt(sql: string) {
  try { return (sqlFormatter as any).format(sql, { language: 'sql', indent: '  ' }) } catch { return sql }
}

// ─────────────────────────────────────────────────────────────────────────────
// Props
// ─────────────────────────────────────────────────────────────────────────────
interface Props {
  value: string
  onChange: (v: string) => void
  columns: string[]
  columnMeta?: CM[]
  sourceTable: string
  sourceSchema?: string | null
  className?: string
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────
export default function CustomQueryEditor({
  value, onChange, columns, columnMeta, sourceTable, sourceSchema, className,
}: Props) {
  const [mode, setMode]       = useState<'sql' | 'builder'>('sql')
  const [conds, setConds]     = useState<Condition[]>([])
  const [selCols, setSelCols] = useState<Set<string>>(new Set())
  const [bErr, setBErr]       = useState('')

  // Merge columns + meta
  const effMeta: CM[] = useMemo(
    () => (columnMeta?.length ? columnMeta : columns.map((n) => ({ name: n }))),
    [columnMeta, columns],
  )

  // kind map: column name → ColKind
  const km: Record<string, ColKind> = useMemo(() => {
    const m: Record<string, ColKind> = {}
    effMeta.forEach((c) => { m[c.name] = inferKind(c.type) })
    return m
  }, [effMeta])

  // ── SQL mode ────────────────────────────────────────────────────────────────
  const handleFormat = useCallback(() => {
    if (value.trim()) onChange(tryFmt(value))
  }, [value, onChange])

  // ── Column selector ─────────────────────────────────────────────────────────
  const toggleCol = (c: string) =>
    setSelCols((p) => { const n = new Set(p); n.has(c) ? n.delete(c) : n.add(c); return n })
  const selAll  = () => setSelCols(new Set(effMeta.map((c) => c.name)))
  const selNone = () => setSelCols(new Set())

  // ── Conditions ──────────────────────────────────────────────────────────────
  const newCond = (): Condition => {
    const col = effMeta[0]?.name ?? ''
    const op  = opsForKind(km[col] ?? 'unknown')[0]?.value ?? '='
    return { id: uid(), column: col, operator: op, value: '', value2: '', connector: 'AND', dateTrunc: '' }
  }
  const addCond = () => setConds((p: Condition[]) => [...p, newCond()])
  const updCond = (id: string, patch: Partial<Condition>) =>
    setConds((p: Condition[]) => p.map((c: Condition) => c.id === id ? { ...c, ...patch } : c))
  const remCond = (id: string) =>
    setConds((p: Condition[]) => p.filter((c: Condition) => c.id !== id))
  const setConn = (id: string, conn: Connector) =>
    setConds((p: Condition[]) => p.map((c: Condition) => c.id === id ? { ...c, connector: conn } : c))

  // ── Apply builder → SQL ─────────────────────────────────────────────────────
  const applyBuilder = () => {
    setBErr('')
    if (conds.some((c: Condition) => !c.column)) { setBErr('Select a column for every condition.'); return }
    onChange(tryFmt(buildQuery(sourceSchema, sourceTable, [...selCols], conds, km)))
    setMode('sql')
  }

  const switchToBuilder = () => {
    if (!conds.length && effMeta.length) setConds([newCond()])
    setMode('builder')
  }

  // ── Live preview ─────────────────────────────────────────────────────────────
  const preview = useMemo(
    () => tryFmt(buildQuery(sourceSchema, sourceTable, [...selCols], conds, km)),
    [sourceSchema, sourceTable, selCols, conds, km],
  )

  // ── Tab button helper ────────────────────────────────────────────────────────
  const tabCls = (m: 'sql' | 'builder') =>
    ['flex items-center gap-1.5 px-2.5 py-1 rounded-md text-[11px] font-medium transition-all',
      mode === m ? 'bg-[hsl(var(--card))] text-foreground shadow-sm' : 'text-muted-foreground hover:text-foreground',
    ].join(' ')

  return (
    <div className={['rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--card))] overflow-hidden shadow-sm', className ?? ''].join(' ')}>

      {/* ── Toolbar ── */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-[hsl(var(--border))] bg-[hsl(var(--muted))]/20">
        <div className="flex p-0.5 gap-0.5 rounded-lg bg-[hsl(var(--muted))]/60 border border-[hsl(var(--border))]">
          <button type="button" onClick={() => setMode('sql')} className={tabCls('sql')}>
            <RiCodeSSlashLine className="h-3.5 w-3.5" />SQL
          </button>
          <button type="button" onClick={switchToBuilder} className={tabCls('builder')}>
            <RiListCheck2 className="h-3.5 w-3.5" />Builder
          </button>
        </div>

        <div className="flex gap-1.5">
          {mode === 'sql' && (
            <button type="button" onClick={handleFormat} disabled={!value.trim()}
              className="flex items-center gap-1 text-[11px] px-2.5 py-1 rounded-lg border border-[hsl(var(--border))] text-muted-foreground hover:text-foreground hover:bg-[hsl(var(--muted))] disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
              <RiMagicLine className="h-3.5 w-3.5" />Format
            </button>
          )}
          {mode === 'builder' && (
            <button type="button" onClick={addCond} disabled={!effMeta.length}
              className="flex items-center gap-1 text-[11px] px-2.5 py-1 rounded-lg border border-[hsl(var(--border))] text-muted-foreground hover:text-foreground hover:bg-[hsl(var(--muted))] disabled:opacity-40 disabled:cursor-not-allowed transition-colors">
              <RiAddLine className="h-3.5 w-3.5" />Add filter
            </button>
          )}
        </div>
      </div>

      {/* ── SQL mode ── */}
      {mode === 'sql' && (
        <>
          <textarea
            className="w-full px-4 py-3 text-xs font-mono bg-transparent outline-none resize-y min-h-[110px] max-h-[320px] text-foreground placeholder:text-muted-foreground leading-relaxed"
            placeholder={"SELECT d.*, c.Name\nFROM mt5_deals d\nJOIN clients c ON d.ClientId = c.Id\nWHERE d.Status = 'Active'"}
            value={value}
            onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => onChange(e.target.value)}
            spellCheck={false}
          />
          <div className="px-4 py-2 border-t border-[hsl(var(--border))] text-[10px] text-muted-foreground bg-[hsl(var(--muted))]/10 flex flex-wrap gap-x-1.5 items-start">
            <span>Write a complete <code className="font-mono bg-[hsl(var(--muted))] px-1 py-0.5 rounded">SELECT … FROM … [JOIN …]</code> query.</span>
            <span>JOINs across multiple tables are fully supported.</span>
            <span className="mt-0.5">The sync engine uses it as a derived table:</span>
            <code className="font-mono bg-[hsl(var(--muted))] px-1 py-0.5 rounded mt-0.5">SELECT … FROM (your query) AS _src WHERE seq_col &gt; last_value</code>
            <span className="mt-0.5">— the sequence column must appear in your result set for incremental sync.</span>
          </div>
        </>
      )}

      {/* ── Builder mode ── */}
      {mode === 'builder' && (
        <div className="flex flex-col divide-y divide-[hsl(var(--border))]">

          {/* Two-panel: SELECT | WHERE */}
          <div className="grid grid-cols-1 md:grid-cols-2 divide-y md:divide-y-0 md:divide-x divide-[hsl(var(--border))]">

            {/* Left: column selector */}
            <div className="p-3 min-h-[200px]">
              {!effMeta.length
                ? (
                  <div className="flex flex-col items-center justify-center h-full py-8 gap-2">
                    <RiListCheck2 className="h-8 w-8 text-muted-foreground/30" />
                    <p className="text-xs text-muted-foreground">Select a source table to see columns.</p>
                  </div>
                )
                : (
                  <ColumnSelector
                    meta={effMeta} selected={selCols}
                    onToggle={toggleCol} onAll={selAll} onNone={selNone}
                  />
                )
              }
            </div>

            {/* Right: WHERE conditions */}
            <div className="p-3 flex flex-col gap-2">
              <div className="flex items-center justify-between mb-0.5">
                <span className="text-xs font-semibold">WHERE filters</span>
                {conds.length > 0 && (
                  <span className="text-[10px] text-muted-foreground">
                    {conds.length} condition{conds.length !== 1 ? 's' : ''}
                  </span>
                )}
              </div>

              {!effMeta.length && (
                <div className="flex-1 py-6 text-center">
                  <p className="text-xs text-muted-foreground">Select a source table first.</p>
                </div>
              )}

              {effMeta.length > 0 && conds.length === 0 && (
                <div className="flex flex-col items-center justify-center flex-1 py-6 gap-2 rounded-lg border border-dashed border-[hsl(var(--border))]">
                  <p className="text-xs text-muted-foreground">No filters — returns all rows.</p>
                  <button type="button" onClick={addCond}
                    className="flex items-center gap-1 text-[11px] px-2.5 py-1 rounded-lg border border-[hsl(var(--border))] text-muted-foreground hover:text-foreground hover:bg-[hsl(var(--muted))] transition-colors">
                    <RiAddLine className="h-3.5 w-3.5" />Add filter
                  </button>
                </div>
              )}

              {conds.map((cond: Condition, i: number) => (
                <ConditionRow
                  key={cond.id} cond={cond} index={i}
                  meta={effMeta} km={km}
                  onChange={updCond} onRemove={remCond} onConn={setConn}
                />
              ))}

              {bErr && (
                <p className="text-[11px] text-red-500 bg-red-50 dark:bg-red-900/20 px-2 py-1.5 rounded-lg">{bErr}</p>
              )}
            </div>
          </div>

          {/* SQL preview */}
          <div className="bg-[hsl(var(--muted))]/20">
            <div className="px-3 pt-2 pb-1 flex items-center justify-between">
              <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">Preview</span>
              <button type="button" onClick={applyBuilder}
                className="flex items-center gap-1.5 text-xs font-medium px-3 py-1 rounded-lg bg-[hsl(var(--primary))] text-white hover:opacity-90 active:opacity-80 transition-opacity">
                Apply to SQL editor
              </button>
            </div>
            <pre className="px-3 pb-3 text-[11px] font-mono text-foreground whitespace-pre-wrap leading-relaxed overflow-x-auto">
              {preview || <span className="text-muted-foreground italic">— select a table to preview —</span>}
            </pre>
          </div>

        </div>
      )}
    </div>
  )
}
