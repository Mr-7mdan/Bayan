'use client'

import { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { Api, type DatasourceOut, type IntrospectResponse } from '@/lib/api'
import {
  RiCloseLine, RiSearchLine, RiTableLine, RiDatabase2Line,
  RiBracesLine, RiRefreshLine, RiArrowRightSLine, RiArrowDownSLine,
  RiLoader4Line,
} from '@remixicon/react'
import { inferKind, KIND_CLS } from './CustomQueryEditorParts'

// ─── Types ────────────────────────────────────────────────────────────────────
type Sel = { schema: string; table: string; column: string | null }

// ─── Helpers ──────────────────────────────────────────────────────────────────
const SYS = new Set(['information_schema', 'sys', 'guest', 'pg_catalog', 'pg_toast'])
function isSys(n: string) { const l = n.toLowerCase(); return l.startsWith('db_') || SYS.has(l) }
function fmtCell(v: any) { if (v == null) return ''; if (typeof v === 'boolean') return v ? 'true' : 'false'; return String(v) }

// ─── SchemaTree ───────────────────────────────────────────────────────────────
function SchemaTree({ schema, loading, sel, onSelect, onRefresh, refreshing, singleTable }: {
  schema: IntrospectResponse | null; loading: boolean; refreshing: boolean
  sel: Sel | null; onSelect: (s: Sel) => void; onRefresh: () => void
  singleTable?: string
}) {
  const [q, setQ] = useState('')
  const [openSchemas, setOpenSchemas] = useState<Set<string>>(new Set())
  const [openTables, setOpenTables]   = useState<Set<string>>(new Set())

  useEffect(() => {
    if (!schema) return
    const vis = (schema.schemas || []).filter(s => !isSys(s.name))
    setOpenSchemas(new Set(vis.map(s => s.name)))
    if (singleTable) {
      for (const s of vis) {
        if ((s.tables || []).some(t => t.name === singleTable)) {
          setOpenTables(new Set([`${s.name}.${singleTable}`]))
          break
        }
      }
    }
  }, [schema, singleTable])

  const filtered = useMemo(() => {
    if (!schema) return []
    const ql = q.toLowerCase()
    return (schema.schemas || []).filter(s => !isSys(s.name)).map(s => ({
      ...s,
      tables: (s.tables || []).filter(t => {
        if (singleTable) return t.name === singleTable
        return !ql || t.name.toLowerCase().includes(ql) ||
          (t.columns || []).some(c => c.name.toLowerCase().includes(ql))
      }),
    })).filter(s => s.tables.length > 0)
  }, [schema, q, singleTable])

  const togSchema = (n: string) => setOpenSchemas(p => { const s = new Set(p); s.has(n) ? s.delete(n) : s.add(n); return s })
  const togTable  = (k: string) => setOpenTables(p => { const s = new Set(p); s.has(k) ? s.delete(k) : s.add(k); return s })
  const multi = filtered.length > 1

  return (
    <div className="flex flex-col h-full min-h-0">
      {/* Search + refresh */}
      <div className="flex items-center gap-1.5 px-2 py-2 border-b border-[hsl(var(--border))] flex-shrink-0">
        {!singleTable && (
          <div className="relative flex-1">
            <RiSearchLine className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3 w-3 text-muted-foreground pointer-events-none" />
            <input type="text" value={q} onChange={e => setQ(e.target.value)} placeholder="Filter…"
              className="w-full pl-7 pr-2 py-1.5 text-xs rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--background))] outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]/40" />
          </div>
        )}
        {singleTable && <span className="flex-1 px-1 text-xs font-medium text-muted-foreground truncate">{singleTable}</span>}
        <button onClick={onRefresh} disabled={refreshing} title="Refresh schema"
          className="p-1.5 rounded-lg border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))] disabled:opacity-50 transition-colors">
          <RiRefreshLine className={['h-3.5 w-3.5 text-muted-foreground', refreshing ? 'animate-spin' : ''].join(' ')} />
        </button>
      </div>

      {/* Tree body */}
      <div className="flex-1 overflow-y-auto min-h-0 py-1">
        {(loading && !schema) && [1,2,3,4].map(i => (
          <div key={i} className="mx-3 my-1 h-5 rounded bg-[hsl(var(--muted))] animate-pulse" style={{ width: `${60 + i * 8}%` }} />
        ))}
        {!loading && !schema && <p className="px-4 py-6 text-xs text-muted-foreground italic">No schema loaded.</p>}
        {filtered.length === 0 && q && <p className="px-4 py-4 text-xs text-muted-foreground italic text-center">No match for "{q}"</p>}

        {filtered.map(sch => {
          const schOpen = openSchemas.has(sch.name)
          return (
            <div key={sch.name}>
              {multi && (
                <button type="button" onClick={() => togSchema(sch.name)}
                  className="w-full flex items-center gap-1.5 px-3 py-1.5 text-xs hover:bg-[hsl(var(--muted))]/40 transition-colors text-left">
                  {schOpen ? <RiArrowDownSLine className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
                            : <RiArrowRightSLine className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />}
                  <RiDatabase2Line className="h-3.5 w-3.5 text-muted-foreground flex-shrink-0" />
                  <span className="font-medium text-muted-foreground truncate">{sch.name}</span>
                  <span className="ml-auto text-[10px] text-muted-foreground flex-shrink-0">{sch.tables.length}</span>
                </button>
              )}
              {(schOpen || !multi) && sch.tables.map(t => {
                const tk = `${sch.name}.${t.name}`
                const tOpen = openTables.has(tk)
                const isSelTbl = sel?.schema === sch.name && sel?.table === t.name
                const ql = q.toLowerCase()
                const visCols = ql ? (t.columns || []).filter(c => c.name.toLowerCase().includes(ql) || t.name.toLowerCase().includes(ql)) : (t.columns || [])
                return (
                  <div key={tk} className={multi ? 'ml-3' : ''}>
                    {/* Table row */}
                    <div className={['flex items-center gap-1 px-2 py-1.5 transition-colors border-l-2',
                      isSelTbl && !sel?.column ? 'border-[hsl(var(--primary))] bg-[hsl(var(--primary))]/6' : 'border-transparent hover:bg-[hsl(var(--muted))]/40',
                    ].join(' ')}>
                      <button type="button" onClick={() => togTable(tk)} className="p-0.5 flex-shrink-0 hover:bg-[hsl(var(--muted))] rounded">
                        {tOpen ? <RiArrowDownSLine className="h-3 w-3 text-muted-foreground" /> : <RiArrowRightSLine className="h-3 w-3 text-muted-foreground" />}
                      </button>
                      <button type="button" onClick={() => { onSelect({ schema: sch.name, table: t.name, column: null }); if (!tOpen) togTable(tk) }}
                        className="flex items-center gap-1.5 flex-1 min-w-0 text-xs text-left">
                        <RiTableLine className="h-3.5 w-3.5 text-[hsl(var(--primary))]/70 flex-shrink-0" />
                        <span className={['font-medium truncate', isSelTbl && !sel?.column ? 'text-[hsl(var(--primary))]' : 'text-foreground'].join(' ')}>{t.name}</span>
                        <span className="ml-auto text-[10px] text-muted-foreground flex-shrink-0 pl-1">{t.columns?.length ?? 0}</span>
                      </button>
                    </div>
                    {/* Columns */}
                    {tOpen && visCols.map(c => {
                      const kind = inferKind(c.type)
                      const isSelCol = isSelTbl && sel?.column === c.name
                      return (
                        <button key={c.name} type="button"
                          onClick={() => onSelect({ schema: sch.name, table: t.name, column: c.name })}
                          className={['w-full flex items-center gap-2 py-1 text-xs text-left transition-colors border-l-2',
                            multi ? 'pl-9 pr-3' : 'pl-6 pr-3',
                            isSelCol ? 'border-[hsl(var(--primary))] bg-[hsl(var(--primary))]/8 text-[hsl(var(--primary))]' : 'border-transparent hover:bg-[hsl(var(--muted))]/30 text-muted-foreground',
                          ].join(' ')}>
                          <RiBracesLine className="h-3 w-3 opacity-50 flex-shrink-0" />
                          <span className={['font-mono truncate flex-1', isSelCol ? 'font-semibold' : ''].join(' ')}>{c.name}</span>
                          {c.type && <span className={['text-[9px] px-1 rounded font-mono flex-shrink-0', KIND_CLS[kind]].join(' ')}>{c.type.length > 14 ? c.type.slice(0, 14) + '…' : c.type}</span>}
                        </button>
                      )
                    })}
                  </div>
                )
              })}
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ─── PreviewPanel ─────────────────────────────────────────────────────────────
const LIMIT = 500

function PreviewPanel({ dsId, sel }: { dsId: string; sel: Sel | null }) {
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState<string | null>(null)
  const [cols, setCols]       = useState<string[]>([])
  const [rows, setRows]       = useState<any[][]>([])
  const [total, setTotal]     = useState<number | null>(null)
  const [offset, setOffset]   = useState(0)
  const colRefs = useRef<Record<string, HTMLTableCellElement | null>>({})

  useEffect(() => { setOffset(0); setCols([]); setRows([]) }, [sel?.table, sel?.schema])

  useEffect(() => {
    if (!sel?.table) return
    let cancelled = false
    setLoading(true); setError(null)
    const { table, schema } = sel
    async function run() {
      const forms = [
        schema && schema !== 'main' ? `"${schema.replace(/"/g,'""')}"."${table.replace(/"/g,'""')}"` : null,
        `"${table.replace(/"/g,'""')}"`,
        table,
        `main."${table.replace(/"/g,'""')}"`,
      ].filter(Boolean) as string[]
      let done = false
      for (const form of forms) {
        try {
          const res = await Api.query({ sql: `SELECT * FROM ${form}`, datasourceId: dsId, limit: LIMIT, offset, includeTotal: true, preferLocalDuck: true, preferLocalTable: table } as any)
          if (cancelled) return
          if ((res.columns?.length || 0) > 0 || (res.rows?.length || 0) > 0) {
            setCols(res.columns as string[])
            setRows(res.rows as any[][])
            setTotal(typeof (res as any).totalRows === 'number' ? (res as any).totalRows : null)
            done = true; break
          }
        } catch { continue }
      }
      if (!done && !cancelled) setError('No data returned — table may be empty or inaccessible.')
      if (!cancelled) setLoading(false)
    }
    void run()
    return () => { cancelled = true }
  }, [dsId, sel?.table, sel?.schema, offset])

  useEffect(() => {
    if (!sel?.column || cols.length === 0) return
    const el = colRefs.current[sel.column]
    if (el) el.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' })
  }, [sel?.column, cols])

  if (!sel?.table) return (
    <div className="flex flex-col items-center justify-center h-full gap-3 text-center px-8">
      <RiTableLine className="h-14 w-14 text-muted-foreground/15" />
      <p className="text-sm text-muted-foreground">Select a table or column from the schema tree to preview its data.</p>
    </div>
  )

  return (
    <div className="flex flex-col h-full min-h-0">
      {/* Preview header */}
      <div className="flex items-center justify-between gap-3 px-4 py-2.5 border-b border-[hsl(var(--border))] flex-shrink-0 bg-[hsl(var(--muted))]/20">
        <div className="flex items-center gap-2 min-w-0 text-sm">
          <RiTableLine className="h-4 w-4 text-[hsl(var(--primary))] flex-shrink-0" />
          <span className="font-semibold truncate">{sel.schema !== 'main' ? `${sel.schema}.` : ''}{sel.table}</span>
          {sel.column && (
            <span className="flex items-center gap-1 px-2 py-0.5 rounded-full bg-[hsl(var(--primary))]/12 text-[hsl(var(--primary))] text-[11px] font-medium border border-[hsl(var(--primary))]/20 flex-shrink-0">
              <RiBracesLine className="h-3 w-3" />{sel.column}
            </span>
          )}
          {typeof total === 'number' && <span className="text-[11px] text-muted-foreground flex-shrink-0">{total.toLocaleString()} rows</span>}
        </div>
        <div className="flex items-center gap-1.5 flex-shrink-0">
          {rows.length > 0 && <span className="text-[10px] text-muted-foreground">{offset + 1}–{offset + rows.length}</span>}
          <button disabled={loading || offset <= 0} onClick={() => setOffset(o => Math.max(0, o - LIMIT))}
            className="text-xs px-2 py-1 rounded border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))] disabled:opacity-40 disabled:cursor-not-allowed transition-colors">Prev</button>
          <button disabled={loading || (typeof total === 'number' ? offset + LIMIT >= total : rows.length < LIMIT)} onClick={() => setOffset(o => o + LIMIT)}
            className="text-xs px-2 py-1 rounded border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))] disabled:opacity-40 disabled:cursor-not-allowed transition-colors">Next</button>
        </div>
      </div>

      {/* Table */}
      <div className="flex-1 min-h-0 overflow-auto">
        {loading && cols.length === 0 && (
          <div className="flex items-center justify-center gap-2 h-full text-sm text-muted-foreground">
            <RiLoader4Line className="h-4 w-4 animate-spin" />Loading…
          </div>
        )}
        {error && <div className="p-4 text-sm text-red-500 bg-red-50/50 dark:bg-red-900/10 m-3 rounded-lg">{error}</div>}
        {!loading && !error && cols.length === 0 && sel.table && <div className="p-4 text-sm text-muted-foreground">No rows.</div>}
        {cols.length > 0 && (
          <table className="min-w-full text-[11px] border-collapse">
            <thead className="sticky top-0 z-10">
              <tr>
                {cols.map((c, ci) => {
                  const hl = c === sel.column
                  return (
                    <th key={c + ci} ref={el => { colRefs.current[c] = el }}
                      className={['text-left font-semibold px-3 py-2 whitespace-nowrap border-b border-[hsl(var(--border))] select-none',
                        hl ? 'bg-[hsl(var(--primary))]/15 text-[hsl(var(--primary))]' : 'bg-[hsl(var(--muted))] text-foreground',
                      ].join(' ')}>
                      {hl && <span className="inline-block w-1.5 h-1.5 rounded-full bg-[hsl(var(--primary))] mr-1.5 align-middle" />}
                      {c}
                    </th>
                  )
                })}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, ri) => (
                <tr key={ri} className="border-b border-[hsl(var(--border))]/40 hover:bg-[hsl(var(--muted))]/25 transition-colors">
                  {cols.map((c, ci) => {
                    const hl = c === sel.column
                    return (
                      <td key={c + ci} title={r[ci] != null ? String(r[ci]) : ''}
                        className={['px-3 py-1.5 align-top whitespace-nowrap max-w-[280px] overflow-hidden text-ellipsis',
                          hl ? 'bg-[hsl(var(--primary))]/5 text-[hsl(var(--primary))]/80 font-medium' : 'text-foreground',
                        ].join(' ')}>
                        {fmtCell(r[ci])}
                      </td>
                    )
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}

// ─── Main Dialog ──────────────────────────────────────────────────────────────
interface Props {
  open: boolean
  onClose: () => void
  datasource: DatasourceOut
  initialTable?: string
  initialSchema?: string
  singleTable?: boolean
}

export default function DataExplorerDialog({ open, onClose, datasource, initialTable, initialSchema, singleTable }: Props) {
  const [schema, setSchema]       = useState<IntrospectResponse | null>(null)
  const [loading, setLoading]     = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [sel, setSel]             = useState<Sel | null>(null)

  const fetchSchema = async (quiet = false, autoTable?: string, autoSchema?: string) => {
    if (quiet) setRefreshing(true); else setLoading(true)
    try {
      const data = await Api.introspect(datasource.id)
      setSchema(data)
      if (autoTable) {
        for (const sch of data.schemas || []) {
          if (autoSchema && sch.name !== autoSchema) continue
          const found = (sch.tables || []).find(t => t.name === autoTable)
          if (found) { setSel({ schema: sch.name, table: autoTable, column: null }); break }
        }
      }
    } catch {}
    finally { setLoading(false); setRefreshing(false) }
  }

  useEffect(() => {
    if (open) { setSel(null); void fetchSchema(false, initialTable, initialSchema) }
  }, [open, datasource.id])

  useEffect(() => {
    if (!open) return
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [open, onClose])

  if (!open || typeof window === 'undefined') return null

  return createPortal(
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/50 backdrop-blur-[2px]" onClick={onClose}>
      <div className="w-[95vw] max-w-[1400px] h-[90vh] bg-[hsl(var(--card))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl flex flex-col overflow-hidden"
        onClick={e => e.stopPropagation()}>

        {/* ── Header ── */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-[hsl(var(--border))] flex-shrink-0 bg-[hsl(var(--muted))]/20">
          <div className="flex items-center gap-3">
            <RiDatabase2Line className="h-5 w-5 text-[hsl(var(--primary))]" />
            <div>
              <h2 className="text-sm font-semibold leading-tight">{datasource.name}</h2>
              <p className="text-[11px] text-muted-foreground leading-tight">{datasource.type} · Data Explorer</p>
            </div>
          </div>
          <button onClick={onClose}
            className="p-1.5 rounded-lg hover:bg-[hsl(var(--muted))] transition-colors">
            <RiCloseLine className="h-5 w-5 text-muted-foreground" />
          </button>
        </div>

        {/* ── Body: schema tree + preview ── */}
        <div className="flex flex-1 min-h-0 divide-x divide-[hsl(var(--border))]">
          {/* Left: schema tree */}
          <div className="w-72 flex-shrink-0 flex flex-col min-h-0">
            <SchemaTree schema={schema} loading={loading} refreshing={refreshing}
              sel={sel} onSelect={setSel} onRefresh={() => fetchSchema(true)}
              singleTable={singleTable ? initialTable : undefined} />
          </div>

          {/* Right: preview */}
          <div className="flex-1 min-w-0 flex flex-col min-h-0">
            <PreviewPanel dsId={datasource.id} sel={sel} />
          </div>
        </div>

      </div>
    </div>,
    document.body,
  )
}
