'use client'

import React, { useState, useMemo, useRef, useEffect } from 'react'
import {
  RiListCheck2, RiAddLine, RiDeleteBinLine,
  RiSearchLine, RiCheckboxBlankLine, RiCheckboxLine, RiErrorWarningLine,
  RiArrowDownSLine,
} from '@remixicon/react'

// ─────────────────────────────────────────────────────────────────────────────
// Column type inference
// ─────────────────────────────────────────────────────────────────────────────
export type ColKind = 'text' | 'number' | 'date' | 'boolean' | 'unknown'

export function inferKind(raw?: string | null): ColKind {
  if (!raw) return 'unknown'
  const t = raw.toLowerCase()
  if (/int|float|double|decimal|numeric|real|money|bigint|smallint|tinyint/.test(t)) return 'number'
  if (/date|time|timestamp|datetime/.test(t)) return 'date'
  if (/bool/.test(t)) return 'boolean'
  if (/char|text|varchar|nvarchar|string|clob|uuid|guid/.test(t)) return 'text'
  return 'unknown'
}

export const KIND_LABEL: Record<ColKind, string> = {
  number: '#', date: 'date', boolean: '✓', text: 'Aa', unknown: '?',
}
export const KIND_CLS: Record<ColKind, string> = {
  number:  'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300',
  date:    'bg-violet-100 text-violet-700 dark:bg-violet-900/30 dark:text-violet-300',
  boolean: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300',
  text:    'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-300',
  unknown: 'bg-gray-100 text-gray-400 dark:bg-gray-800 dark:text-gray-500',
}

// ─────────────────────────────────────────────────────────────────────────────
// Operators
// ─────────────────────────────────────────────────────────────────────────────
export type OpDef = {
  value: string; label: string
  noValue?: boolean; twoValues?: boolean; multiValue?: boolean
  kinds?: ColKind[]
}

export const ALL_OPS: OpDef[] = [
  { value: '=',           label: '= equals' },
  { value: '!=',          label: '≠ not equals' },
  { value: '>',           label: '> greater than',  kinds: ['number','date','unknown'] },
  { value: '>=',          label: '≥ or equal',      kinds: ['number','date','unknown'] },
  { value: '<',           label: '< less than',     kinds: ['number','date','unknown'] },
  { value: '<=',          label: '≤ or equal',      kinds: ['number','date','unknown'] },
  { value: 'BETWEEN',     label: '↔ BETWEEN',       kinds: ['number','date','unknown'], twoValues: true },
  { value: 'NOT BETWEEN', label: '↮ NOT BETWEEN',   kinds: ['number','date','unknown'], twoValues: true },
  { value: 'LIKE',        label: '~ LIKE',          kinds: ['text','unknown'] },
  { value: 'NOT LIKE',    label: '!~ NOT LIKE',     kinds: ['text','unknown'] },
  { value: 'IN',          label: '∈ IN (list)',     multiValue: true },
  { value: 'NOT IN',      label: '∉ NOT IN',        multiValue: true },
  { value: 'IS NULL',     label: '∅ IS NULL',       noValue: true },
  { value: 'IS NOT NULL', label: '◉ NOT NULL',      noValue: true },
]

export function opsForKind(k: ColKind): OpDef[] {
  return ALL_OPS.filter((op) => !op.kinds || op.kinds.includes(k) || k === 'unknown')
}

export const DATE_TRUNC_LEVELS = [
  { value: '',         label: '— no truncation —' },
  { value: 'year',     label: 'Year'    },
  { value: 'quarter',  label: 'Quarter' },
  { value: 'month',    label: 'Month'   },
  { value: 'week',     label: 'Week'    },
  { value: 'day',      label: 'Day'     },
  { value: 'hour',     label: 'Hour'    },
  { value: 'minute',   label: 'Minute'  },
]

// ─────────────────────────────────────────────────────────────────────────────
// Validation
// ─────────────────────────────────────────────────────────────────────────────
export function validateValue(val: string, kind: ColKind): string {
  if (!val.trim()) return ''
  if (kind === 'number' && isNaN(Number(val.trim())))
    return `"${val}" is not a valid number`
  if (kind === 'date' && isNaN(new Date(val.trim()).getTime()))
    return `"${val}" is not a valid date (use YYYY-MM-DD)`
  if (kind === 'boolean' && !['true','false','1','0','yes','no'].includes(val.trim().toLowerCase()))
    return `Use true / false`
  return ''
}

// ─────────────────────────────────────────────────────────────────────────────
// SQL generation
// ─────────────────────────────────────────────────────────────────────────────
export type Connector = 'AND' | 'OR'
export type Condition = {
  id: string; column: string; operator: string
  value: string; value2: string
  connector: Connector; dateTrunc: string
}

export function uid() { return Math.random().toString(36).slice(2, 9) }

function qi(n: string) { return `"${n.replace(/"/g, '""')}"` }

function lhs(col: string, dt: string) {
  return dt ? `DATE_TRUNC('${dt}', ${qi(col)})` : qi(col)
}

function lit(v: string, kind: ColKind) {
  const t = v.trim()
  if (kind === 'number' && t !== '' && !isNaN(Number(t))) return t
  if (kind === 'boolean' && ['true','false','1','0'].includes(t.toLowerCase())) return t
  return `'${t.replace(/'/g, "''")}'`
}

export function buildWhere(conds: Condition[], km: Record<string, ColKind>): string {
  const valid = conds.filter((c) => c.column)
  if (!valid.length) return ''
  const parts: string[] = []
  valid.forEach((c, i) => {
    const op = ALL_OPS.find((o) => o.value === c.operator)
    const kind = km[c.column] ?? 'unknown'
    const l = lhs(c.column, c.dateTrunc)
    let expr = ''
    if (op?.noValue) {
      expr = `${l} ${c.operator}`
    } else if (op?.twoValues) {
      expr = `${l} ${c.operator} ${lit(c.value, kind)} AND ${lit(c.value2, kind)}`
    } else if (op?.multiValue) {
      const vals = c.value.split(',').map((v) => v.trim()).filter(Boolean)
        .map((v) => lit(v, kind)).join(', ')
      expr = `${l} ${c.operator} (${vals || "''"})`
    } else {
      expr = `${l} ${c.operator} ${lit(c.value, kind)}`
    }
    if (i > 0) parts.push(c.connector)
    parts.push(expr)
  })
  return parts.join('\n  ')
}

export function buildQuery(
  schema: string | null | undefined, table: string,
  selCols: string[], conds: Condition[], km: Record<string, ColKind>,
): string {
  if (!table) return '-- select a source table first'
  const tbl = schema ? `${qi(schema)}.${qi(table)}` : qi(table)
  const sel = selCols.length ? selCols.map(qi).join(',\n  ') : '*'
  let sql = `SELECT ${sel.includes('\n') ? '\n  ' + sel : sel}\nFROM ${tbl}`
  const w = buildWhere(conds, km)
  if (w) sql += `\nWHERE ${w}`
  return sql
}

// ─────────────────────────────────────────────────────────────────────────────
// ColumnSelector
// ─────────────────────────────────────────────────────────────────────────────
export type CM = { name: string; type?: string | null }

export function ColumnSelector({ meta, selected, onToggle, onAll, onNone }: {
  meta: CM[]; selected: Set<string>
  onToggle: (c: string) => void; onAll: () => void; onNone: () => void
}) {
  const [q, setQ] = useState('')
  const filtered = useMemo(() => meta.filter((c) => c.name.toLowerCase().includes(q.toLowerCase())), [meta, q])

  return (
    <div className="flex flex-col min-h-0">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-1.5">
          <span className="text-xs font-semibold">SELECT columns</span>
          {selected.size > 0
            ? <span className="px-1.5 rounded-full text-[10px] font-bold bg-[hsl(var(--primary))] text-white">{selected.size}</span>
            : <span className="px-1.5 rounded-full text-[10px] bg-[hsl(var(--muted))] text-muted-foreground">ALL *</span>}
        </div>
        <div className="flex gap-1">
          <button type="button" onClick={onAll}
            className="text-[10px] px-2 py-0.5 rounded border border-[hsl(var(--border))] text-muted-foreground hover:text-foreground hover:bg-[hsl(var(--muted))] transition-colors">All</button>
          <button type="button" onClick={onNone}
            className="text-[10px] px-2 py-0.5 rounded border border-[hsl(var(--border))] text-muted-foreground hover:text-foreground hover:bg-[hsl(var(--muted))] transition-colors">None</button>
        </div>
      </div>

      <div className="relative mb-2">
        <RiSearchLine className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground pointer-events-none" />
        <input type="text" value={q}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => setQ(e.target.value)}
          placeholder="Search columns…"
          className="w-full pl-7 pr-2 py-1 text-xs rounded border border-[hsl(var(--border))] bg-[hsl(var(--background))] outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]/40" />
      </div>

      <div className="overflow-y-auto rounded border border-[hsl(var(--border))] bg-[hsl(var(--background))] divide-y divide-[hsl(var(--border))]" style={{ maxHeight: 220 }}>
        {!filtered.length && (
          <div className="px-3 py-3 text-xs text-muted-foreground italic text-center">No match for "{q}"</div>
        )}
        {filtered.map((col) => {
          const chk = selected.has(col.name)
          const kind = inferKind(col.type)
          return (
            <button key={col.name} type="button" onClick={() => onToggle(col.name)}
              className={['w-full flex items-center gap-2 px-2.5 py-1.5 text-left text-xs hover:bg-[hsl(var(--muted))]/50 transition-colors', chk ? 'bg-[hsl(var(--primary))]/5' : ''].join(' ')}>
              {chk
                ? <RiCheckboxLine className="h-3.5 w-3.5 text-[hsl(var(--primary))] flex-shrink-0" />
                : <RiCheckboxBlankLine className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />}
              <span className={['font-mono truncate flex-1', chk ? 'text-foreground font-medium' : 'text-muted-foreground'].join(' ')}>{col.name}</span>
              {col.type && (
                <span className={['text-[9px] px-1 rounded font-mono flex-shrink-0', KIND_CLS[kind]].join(' ')}>{col.type}</span>
              )}
            </button>
          )
        })}
      </div>
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
// ConditionRow
// ─────────────────────────────────────────────────────────────────────────────
// Searchable column dropdown
// ─────────────────────────────────────────────────────────────────────────────
function SearchableColSelect({ meta, km, value, onChange }: {
  meta: CM[]; km: Record<string, ColKind>
  value: string; onChange: (col: string) => void
}) {
  const [open, setOpen]   = useState(false)
  const [q, setQ]         = useState('')
  const containerRef      = useRef<HTMLDivElement>(null)
  const inputRef          = useRef<HTMLInputElement>(null)

  const filtered = useMemo(
    () => meta.filter((c) => c.name.toLowerCase().includes(q.toLowerCase())),
    [meta, q],
  )

  // Close on outside click
  useEffect(() => {
    if (!open) return
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false)
        setQ('')
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const select = (name: string) => { onChange(name); setOpen(false); setQ('') }

  const current = meta.find((c) => c.name === value)
  const kind    = current ? (km[current.name] ?? 'unknown') : 'unknown'

  return (
    <div ref={containerRef} className="relative flex-1 min-w-0">
      {/* Trigger */}
      <button
        type="button"
        onClick={() => { setOpen((v) => !v); setTimeout(() => inputRef.current?.focus(), 30) }}
        className="w-full flex items-center gap-1.5 h-6 px-1.5 text-xs rounded border border-[hsl(var(--border))] bg-[hsl(var(--muted))]/40 hover:bg-[hsl(var(--muted))]/70 outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]/40 transition-colors text-left">
        {current ? (
          <>
            <span className={['text-[9px] px-1 rounded font-mono flex-shrink-0', KIND_CLS[kind]].join(' ')}>{KIND_LABEL[kind]}</span>
            <span className="font-mono truncate flex-1 text-foreground">{current.name}</span>
          </>
        ) : (
          <span className="flex-1 text-muted-foreground">— column —</span>
        )}
        <RiArrowDownSLine className={['h-3.5 w-3.5 text-muted-foreground flex-shrink-0 transition-transform', open ? 'rotate-180' : ''].join(' ')} />
      </button>

      {/* Dropdown */}
      {open && (
        <div className="absolute z-50 top-full left-0 mt-0.5 w-full min-w-[180px] rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] shadow-lg overflow-hidden">
          {/* Search */}
          <div className="p-1.5 border-b border-[hsl(var(--border))]">
            <div className="relative">
              <RiSearchLine className="absolute left-2 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground pointer-events-none" />
              <input
                ref={inputRef}
                type="text"
                value={q}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setQ(e.target.value)}
                placeholder="Search columns…"
                className="w-full pl-6 pr-2 py-1 text-xs rounded border border-[hsl(var(--border))] bg-[hsl(var(--background))] outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]/40"
              />
            </div>
          </div>

          {/* List */}
          <div className="overflow-y-auto" style={{ maxHeight: 180 }}>
            {!filtered.length && (
              <div className="px-3 py-2 text-xs text-muted-foreground italic text-center">No match</div>
            )}
            {filtered.map((col) => {
              const k = km[col.name] ?? 'unknown'
              const active = col.name === value
              return (
                <button
                  key={col.name}
                  type="button"
                  onMouseDown={(e) => { e.preventDefault(); select(col.name) }}
                  className={['w-full flex items-center gap-2 px-2.5 py-1.5 text-xs text-left transition-colors hover:bg-[hsl(var(--muted))]/60',
                    active ? 'bg-[hsl(var(--primary))]/8 font-medium text-[hsl(var(--primary))]' : 'text-foreground',
                  ].join(' ')}>
                  <span className={['text-[9px] px-1 rounded font-mono flex-shrink-0', KIND_CLS[k]].join(' ')}>{KIND_LABEL[k]}</span>
                  <span className="font-mono truncate flex-1">{col.name}</span>
                  {col.type && <span className="text-[9px] text-muted-foreground flex-shrink-0 truncate max-w-[60px]">{col.type}</span>}
                </button>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}

// ─────────────────────────────────────────────────────────────────────────────
export function ConditionRow({ cond, index, meta, km, onChange, onRemove, onConn }: {
  cond: Condition; index: number; meta: CM[]; km: Record<string, ColKind>
  onChange: (id: string, p: Partial<Condition>) => void
  onRemove: (id: string) => void
  onConn: (id: string, c: Connector) => void
}) {
  const kind = km[cond.column] ?? 'unknown'
  const ops = opsForKind(kind)
  const effOp = ops.some((o) => o.value === cond.operator) ? cond.operator : (ops[0]?.value ?? '=')
  const opDef = ops.find((o) => o.value === effOp) ?? ops[0]

  const showTrunc = cond.column && !opDef?.noValue && (kind === 'date' || !!cond.dateTrunc)
  const err1 = (!opDef?.noValue && !opDef?.twoValues && !opDef?.multiValue) ? validateValue(cond.value, kind) : ''
  const err2 = opDef?.twoValues ? validateValue(cond.value2, kind) : ''
  const inputType = kind === 'date' ? 'date' : kind === 'number' ? 'number' : 'text'
  const inputCls = (err: string) =>
    ['flex-1 min-w-0 text-xs h-6 px-1.5 rounded border outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]/40',
      err ? 'border-red-400 bg-red-50 dark:bg-red-900/10' : 'border-[hsl(var(--border))] bg-[hsl(var(--muted))]/40',
    ].join(' ')

  return (
    <div className="flex flex-col gap-1">
      {index > 0 && (
        <div className="flex gap-1 pl-0.5">
          {(['AND','OR'] as Connector[]).map((c) => (
            <button key={c} type="button" onClick={() => onConn(cond.id, c)}
              className={['px-2 py-0.5 rounded text-[10px] font-bold border transition-colors',
                cond.connector === c ? 'bg-[hsl(var(--primary))] text-white border-transparent'
                  : 'border-[hsl(var(--border))] text-muted-foreground hover:text-foreground',
              ].join(' ')}>{c}</button>
          ))}
        </div>
      )}

      <div className="flex flex-col gap-1.5 bg-[hsl(var(--background))] border border-[hsl(var(--border))] rounded-lg px-2 py-2">
        {/* Column + operator + delete */}
        <div className="flex items-center gap-1.5">
          <SearchableColSelect
            meta={meta}
            km={km}
            value={cond.column}
            onChange={(nc) => {
              const nk = km[nc] ?? 'unknown'
              const nops = opsForKind(nk)
              const nop = nops.some((o) => o.value === cond.operator) ? cond.operator : (nops[0]?.value ?? '=')
              onChange(cond.id, { column: nc, operator: nop, value: '', value2: '', dateTrunc: '' })
            }}
          />

          <select value={effOp}
            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => onChange(cond.id, { operator: e.target.value, value: '', value2: '' })}
            className="text-xs h-6 px-1.5 rounded border border-[hsl(var(--border))] bg-[hsl(var(--muted))]/40 outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]/40 flex-shrink-0">
            {ops.map((op) => <option key={op.value} value={op.value}>{op.label}</option>)}
          </select>

          <button type="button" onClick={() => onRemove(cond.id)}
            className="h-6 w-6 flex items-center justify-center rounded text-muted-foreground hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors flex-shrink-0">
            <RiDeleteBinLine className="h-3.5 w-3.5" />
          </button>
        </div>

        {/* DATE_TRUNC row */}
        {showTrunc && (
          <div className="flex items-center gap-1.5">
            <span className="text-[10px] text-muted-foreground w-20 flex-shrink-0">DATE_TRUNC</span>
            <select value={cond.dateTrunc}
              onChange={(e: React.ChangeEvent<HTMLSelectElement>) => onChange(cond.id, { dateTrunc: e.target.value })}
              className="flex-1 text-xs h-6 px-1.5 rounded border border-[hsl(var(--border))] bg-[hsl(var(--muted))]/40 outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]/40">
              {DATE_TRUNC_LEVELS.map((l) => <option key={l.value} value={l.value}>{l.label}</option>)}
            </select>
          </div>
        )}

        {/* Value inputs */}
        {!opDef?.noValue && (
          opDef?.twoValues ? (
            <div className="flex items-center gap-1.5">
              <div className="flex-1 flex flex-col gap-0.5">
                <input type={inputType} value={cond.value} placeholder="from"
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => onChange(cond.id, { value: e.target.value })}
                  className={inputCls(err1)} />
                {err1 && <span className="flex items-center gap-0.5 text-[10px] text-red-500"><RiErrorWarningLine className="h-3 w-3" />{err1}</span>}
              </div>
              <span className="text-[10px] text-muted-foreground flex-shrink-0">AND</span>
              <div className="flex-1 flex flex-col gap-0.5">
                <input type={inputType} value={cond.value2} placeholder="to"
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => onChange(cond.id, { value2: e.target.value })}
                  className={inputCls(err2)} />
                {err2 && <span className="flex items-center gap-0.5 text-[10px] text-red-500"><RiErrorWarningLine className="h-3 w-3" />{err2}</span>}
              </div>
            </div>
          ) : (
            <div className="flex flex-col gap-0.5">
              <input
                type={opDef?.multiValue ? 'text' : inputType}
                value={cond.value}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => onChange(cond.id, { value: e.target.value })}
                placeholder={opDef?.multiValue ? 'val1, val2, …' : kind === 'date' ? 'YYYY-MM-DD' : kind === 'boolean' ? 'true / false' : 'value'}
                className={inputCls(err1)} />
              {err1 && <span className="flex items-center gap-0.5 text-[10px] text-red-500"><RiErrorWarningLine className="h-3 w-3" />{err1}</span>}
            </div>
          )
        )}
      </div>
    </div>
  )
}

export { RiListCheck2, RiAddLine }
