"use client"

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useQuery } from '@tanstack/react-query'
import { Api, QueryApi } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import type { WidgetConfig, ReportElement, ReportVariable, ReportTableCell } from '@/types/widgets'
import { RiAddLine, RiDeleteBinLine, RiDragMoveLine, RiSettings3Line, RiTableLine, RiText, RiHashtag, RiCloseLine, RiArrowLeftLine, RiSave3Line, RiImageLine, RiFileCopyLine, RiAlignLeft, RiAlignCenter, RiAlignRight, RiAlignTop, RiAlignVertically, RiAlignBottom, RiDatabase2Line } from '@remixicon/react'
import DataExplorerDialogV2 from './DataExplorerDialogV2'

const genId = () => Math.random().toString(36).slice(2, 10)

type ReportState = {
  gridCols: number
  gridRows: number
  cellSize: number
  elements: ReportElement[]
  variables: ReportVariable[]
  showGridLines: boolean
}

// ─── Expression Builder ───────────────────────────────────────────────
type ExprToken = { kind: 'var'; name: string } | { kind: 'op'; value: string } | { kind: 'num'; value: string }

function parseExprTokens(expr: string, knownVarNames: string[]): ExprToken[] {
  if (!expr?.trim()) return []
  return expr.trim().split(/\s+/).map(p => {
    if (knownVarNames.includes(p)) return { kind: 'var' as const, name: p }
    if (['+', '-', '*', '/', '(', ')'].includes(p)) return { kind: 'op' as const, value: p }
    return { kind: 'num' as const, value: p }
  })
}

function serializeTokens(tokens: ExprToken[]): string {
  return tokens.map(t => t.kind === 'var' ? t.name : t.value).join(' ')
}

function ExpressionBuilder({ expression, allVarNames, onChange }: {
  expression: string
  allVarNames: string[]
  onChange: (expr: string) => void
}) {
  const tokens = parseExprTokens(expression, allVarNames)
  const [open, setOpen] = useState(false)
  const [editIdx, setEditIdx] = useState<number | null>(null)
  const [dropPos, setDropPos] = useState<{ top: number; left: number; maxHeight: number }>({ top: 0, left: 0, maxHeight: 400 })
  const [numInput, setNumInput] = useState('')
  const [varSearch, setVarSearch] = useState('')
  const btnRef = useRef<HTMLButtonElement>(null)
  const dropRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const handler = (e: MouseEvent) => {
      if (
        dropRef.current && !dropRef.current.contains(e.target as Node) &&
        btnRef.current && !btnRef.current.contains(e.target as Node)
      ) { setOpen(false); setEditIdx(null) }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const openAt = (rect: DOMRect, idx: number | null) => {
    const maxHeight = Math.max(160, window.innerHeight - rect.bottom - 16)
    setDropPos({ top: rect.bottom + 6, left: rect.left, maxHeight })
    setEditIdx(idx)
    setOpen(true)
  }

  const handleToggle = () => {
    if (!open && btnRef.current) {
      const r = btnRef.current.getBoundingClientRect()
      const maxHeight = Math.max(160, window.innerHeight - r.bottom - 16)
      setDropPos({ top: r.bottom + 6, left: r.left, maxHeight })
      setEditIdx(null)
    }
    setOpen(v => !v)
    if (open) { setEditIdx(null); setVarSearch('') }
  }

  const applyToken = (token: ExprToken) => {
    let next: ExprToken[]
    if (editIdx !== null) {
      next = [...tokens]
      next[editIdx] = token
    } else {
      next = [...tokens, token]
    }
    onChange(serializeTokens(next))
    setOpen(false)
    setEditIdx(null)
    setNumInput('')
  }

  const removeToken = (idx: number) => {
    const next = [...tokens]
    next.splice(idx, 1)
    onChange(serializeTokens(next))
  }

  const OPS = ['+', '-', '*', '/', '(', ')']
  const isEditing = editIdx !== null

  const dropdown = open ? createPortal(
    <div
      ref={dropRef}
      style={{ position: 'fixed', top: dropPos.top, left: dropPos.left, zIndex: 9999, maxHeight: dropPos.maxHeight, overflowY: 'auto', backgroundColor: 'hsl(var(--popover))', color: 'hsl(var(--popover-foreground))' }}
      className="w-52 rounded-md border shadow-xl p-1 text-[11px]"
    >
      {isEditing && (
        <p className="text-[9px] text-primary font-semibold uppercase tracking-wide px-2 pt-1 pb-0.5">Replace with…</p>
      )}
      {allVarNames.length > 0 && (
        <>
          <p className="text-[9px] text-muted-foreground uppercase tracking-wide px-2 pt-1 pb-0.5">Variables</p>
          <div className="px-1 pb-1">
            <input
              autoFocus
              type="text"
              value={varSearch}
              onChange={e => setVarSearch(e.target.value)}
              placeholder="Search…"
              style={{ backgroundColor: 'hsl(var(--background))' }}
              className="w-full h-6 text-[11px] rounded border px-2 outline-none focus:ring-1 focus:ring-primary/40"
            />
          </div>
          <div className="max-h-48 overflow-y-auto">
            {allVarNames.filter(n => n.toLowerCase().includes(varSearch.toLowerCase())).map(name => (
              <button key={name} onClick={() => { applyToken({ kind: 'var', name }); setVarSearch('') }}
                className="w-full text-left px-2 py-1 rounded hover:bg-primary/10 hover:text-primary transition-colors truncate">
                {name}
              </button>
            ))}
            {allVarNames.filter(n => n.toLowerCase().includes(varSearch.toLowerCase())).length === 0 && (
              <p className="text-[10px] text-muted-foreground px-2 py-1">No matches</p>
            )}
          </div>
        </>
      )}
      <p className="text-[9px] text-muted-foreground uppercase tracking-wide px-2 pt-2 pb-0.5">Operators</p>
      <div className="flex flex-wrap gap-1 px-2 pb-1">
        {OPS.map(op => (
          <button key={op} onClick={() => applyToken({ kind: 'op', value: op })}
            className="w-7 h-7 rounded border font-mono text-sm hover:bg-primary/10 hover:border-primary hover:text-primary transition-colors flex items-center justify-center">
            {op}
          </button>
        ))}
      </div>
      <p className="text-[9px] text-muted-foreground uppercase tracking-wide px-2 pt-1 pb-0.5">Number</p>
      <div className="flex gap-1 px-2 pb-1.5">
        <input
          type="number"
          value={numInput}
          onChange={e => setNumInput(e.target.value)}
          onKeyDown={e => { if (e.key === 'Enter' && numInput.trim()) applyToken({ kind: 'num', value: numInput.trim() }) }}
          style={{ backgroundColor: 'hsl(var(--background))' }}
          className="flex-1 h-6 text-xs rounded border px-1.5 outline-none focus:ring-1 focus:ring-primary/40"
          placeholder="e.g. 1.15"
        />
        <button
          disabled={!numInput.trim()}
          onClick={() => { if (numInput.trim()) applyToken({ kind: 'num', value: numInput.trim() }) }}
          className="px-2 h-6 text-xs rounded border hover:bg-primary/10 hover:text-primary disabled:opacity-40 transition-colors"
        >Add</button>
      </div>
    </div>,
    document.body
  ) : null

  return (
    <div>
      <label className="block text-[10px] font-medium text-muted-foreground mb-1">Expression</label>
      <div className="min-h-8 flex flex-wrap gap-1 p-1.5 rounded-md border bg-secondary/40 items-center">
        {/* + button always at the start */}
        <button
          ref={btnRef}
          onClick={handleToggle}
          className={`h-5 w-5 rounded-full border flex items-center justify-center transition-colors text-xs shrink-0 ${open && editIdx === null ? 'bg-primary text-primary-foreground border-primary' : 'border-dashed text-muted-foreground hover:text-primary hover:border-primary'}`}
        >+</button>
        {tokens.length === 0 && <span className="text-[10px] text-muted-foreground/50 select-none">Pick variables and operators…</span>}
        {tokens.map((tok, i) => (
          <span key={i} className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-medium leading-none border ${
            tok.kind === 'var'
              ? `bg-primary/15 border-primary/25 text-primary ${open && editIdx === i ? 'ring-1 ring-primary' : 'cursor-pointer hover:bg-primary/25'}`
              : tok.kind === 'op'
              ? `bg-secondary text-foreground border-border font-mono text-[13px] ${open && editIdx === i ? 'ring-1 ring-border' : 'cursor-pointer hover:bg-secondary/80'}`
              : `bg-amber-500/15 text-amber-700 dark:text-amber-400 border-amber-500/25 font-mono ${open && editIdx === i ? 'ring-1 ring-amber-500/50' : 'cursor-pointer hover:bg-amber-500/25'}`
          }`}>
            <button
              onClick={(e) => {
                const r = (e.currentTarget.closest('span') as HTMLElement).getBoundingClientRect()
                openAt(r, i)
              }}
              className={`hover:underline underline-offset-2 ${tok.kind !== 'var' ? 'font-mono' : ''}`}
            >{tok.kind === 'var' ? tok.name : tok.value}</button>
            <button onClick={() => removeToken(i)} className="ml-0.5 opacity-50 hover:opacity-100 transition-opacity leading-none text-[12px]">×</button>
          </span>
        ))}
      </div>
      {dropdown}
    </div>
  )
}

// ─── Variable Editor Panel ───────────────────────────────────────────
function VariableEditor({
  variable,
  allVariables,
  onUpdate,
  onDelete,
  onDuplicate,
  widgetId,
}: {
  variable: ReportVariable
  allVariables: ReportVariable[]
  onUpdate: (v: ReportVariable) => void
  onDelete: () => void
  onDuplicate?: () => void
  widgetId?: string
}) {
  const { user } = useAuth()
  const dsQ = useQuery({ queryKey: ['datasources'], queryFn: () => Api.listDatasources(undefined, user?.id) })
  const datasources = dsQ.data || []

  const [dsId, setDsId] = useState(variable.datasourceId || '')
  const [source, setSource] = useState(variable.source || '')
  const [showExplorer, setShowExplorer] = useState(false)

  // Sync local state when variable changes externally
  useEffect(() => {
    setDsId(variable.datasourceId || '')
    setSource(variable.source || '')
  }, [variable.datasourceId, variable.source])

  const tablesQ = useQuery({
    queryKey: ['report-tables', dsId, variable.id],
    queryFn: async () => {
      if (!dsId) return null
      try {
        return await Api.introspect(dsId)
      } catch (err) {
        console.error('Failed to fetch tables:', err)
        return null
      }
    },
    enabled: !!dsId,
    staleTime: 0,
    refetchOnMount: 'always',
  })
  const tables = useMemo(() => {
    const raw = tablesQ.data as any
    if (!raw) return []
    
    // Handle schemas[].tables structure
    if (Array.isArray(raw.schemas)) {
      const allTables: string[] = []
      for (const schema of raw.schemas) {
        if (Array.isArray(schema.tables)) {
          for (const t of schema.tables) {
            const tableName = t.name || String(t)
            // Prefix with schema name if not 'main' or 'pg_catalog'
            if (schema.name && schema.name !== 'main' && schema.name !== 'pg_catalog') {
              allTables.push(`${schema.name}.${tableName}`)
            } else if (schema.name === 'main') {
              allTables.push(tableName)
            }
            // Skip pg_catalog tables by default
          }
        }
      }
      return allTables
    }
    
    // Fallback: handle direct tables array
    if (Array.isArray(raw.tables)) return raw.tables.map((t: any) => t.name || t)
    if (Array.isArray(raw)) return raw.map((t: any) => t.name || String(t))
    return []
  }, [tablesQ.data])
  const columnsQ = useQuery({
    queryKey: ['columns', dsId, source, variable.id],
    queryFn: async () => {
      if (!dsId || !source) return []
      const r = await Api.introspect(dsId)
      const raw = r as any
      
      // Handle schemas[].tables[].columns structure
      if (Array.isArray(raw?.schemas)) {
        for (const schema of raw.schemas) {
          if (Array.isArray(schema.tables)) {
            const tbl = schema.tables.find((t: any) => {
              const tableName = t.name || String(t)
              // Match with or without schema prefix
              return tableName === source || `${schema.name}.${tableName}` === source
            })
            if (tbl?.columns) {
              return tbl.columns.map((c: any) => ({ name: c.name || String(c), type: c.type ?? null }))
            }
          }
        }
      }
      
      // Fallback: direct tables array
      const tbl = (Array.isArray(raw?.tables) ? raw.tables : []).find((t: any) => (t.name || t) === source)
      return tbl?.columns?.map((c: any) => ({ name: c.name || String(c), type: c.type ?? null })) || []
    },
    enabled: !!(dsId && source),
  })
  const columnsMeta: Array<{ name: string; type: string | null }> = columnsQ.data || []

  // Include join-added columns from datasource transforms
  const dsTransformsQ = useQuery({
    queryKey: ['ds-transforms', dsId],
    queryFn: () => dsId ? Api.getDatasourceTransforms(dsId) : null,
    enabled: !!dsId,
    staleTime: 0, // Always fetch fresh to prevent cross-datasource pollution
    refetchOnMount: true, // Refetch when component mounts
    placeholderData: undefined, // Don't show previous data while refetching
  })
  const transformColumns = useMemo(() => {
    try {
      // Safety check: only process transforms if they match the currently selected datasource
      // This prevents React Query cache timing issues when switching datasources
      if (!dsId || !dsTransformsQ.data || !source) return []
      
      const norm = (s: string) => String(s || '').trim().replace(/^\[|\]|^"|"$/g, '')
      const tblEq = (a: string, b: string) => {
        const na = norm(a).split('.').pop() || ''
        const nb = norm(b).split('.').pop() || ''
        return na.toLowerCase() === nb.toLowerCase()
      }
      
      // Build set of available base columns from schema
      const baseColsSet = new Set(columnsMeta.map(c => c.name.toLowerCase()))
      const hasSchemaData = columnsMeta.length > 0
      
      // Helper to extract column references from expression
      const extractRefs = (expr: string): Set<string> => {
        const refs = new Set<string>()
        // Match unquoted identifiers and quoted identifiers
        const pattern = /\b[a-zA-Z_][a-zA-Z0-9_]*\b|"([^"]+)"|'([^']+)'|\[([^\]]+)\]/g
        let match
        const SQL_KEYWORDS = new Set(['and', 'or', 'not', 'case', 'when', 'then', 'else', 'end', 'null', 'true', 'false',
          'cast', 'as', 'left', 'right', 'varchar', 'int', 'double', 'float', 'char', 'nvarchar', 'varchar2',
          'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'bit', 'date', 'datetime', 'timestamp',
          'text', 'ntext', 'string', 'boolean', 'bool', 'unsigned', 'signed', 'binary', 'varbinary',
          'coalesce', 'isnull', 'ifnull', 'nullif', 'if', 'substr', 'substring', 'len', 'length',
          'trim', 'ltrim', 'rtrim', 'upper', 'lower', 'replace', 'concat', 'like', 'in', 'between',
          'is', 'over', 'partition', 'by', 'from', 'select', 'where', 'having', 'group', 'order'])
        while ((match = pattern.exec(expr)) !== null) {
          const ref = (match[1] || match[3] || match[0]).toLowerCase()
          if (ref && !SQL_KEYWORDS.has(ref)) {
            refs.add(ref)
          }
        }
        return refs
      }
      
      const out: string[] = []
      
      // Custom columns - only include if dependencies exist in base table
      const customCols = Array.isArray((dsTransformsQ.data as any)?.customColumns) ? ((dsTransformsQ.data as any).customColumns as any[]) : []
      
      for (const cc of customCols) {
        const sc = (cc?.scope || {}) as any
        const lvl = String(sc?.level || 'datasource').toLowerCase()
        const scopeMatch = (
          lvl === 'datasource' ||
          (lvl === 'table' && sc?.table && source && tblEq(String(sc.table), source)) ||
          (lvl === 'widget' && widgetId && String(sc?.widgetId || '') === String(widgetId))
        )
        if (!scopeMatch || !cc?.name) continue
        
        // For datasource-scoped columns, check if dependencies exist in current table
        // Only filter if we have schema data, otherwise allow through (will be validated at query time)
        if (lvl === 'datasource' && cc?.expr && hasSchemaData) {
          const refs = extractRefs(String(cc.expr))
          // Only skip if we can confirm dependencies are missing
          if (refs.size > 0) {
            const allDepsExist = Array.from(refs).every(ref => baseColsSet.has(ref))
            if (!allDepsExist) continue // Skip if dependencies don't exist
          }
        }
        
        out.push(String(cc.name).trim())
      }
      
      // Transforms (computed, case, etc.) - check dependencies for datasource-scoped
      const transforms = Array.isArray((dsTransformsQ.data as any)?.transforms) ? ((dsTransformsQ.data as any).transforms as any[]) : []
      for (const tr of transforms) {
        const sc = (tr?.scope || {}) as any
        const lvl = String(sc?.level || 'datasource').toLowerCase()
        const scopeMatch = (
          lvl === 'datasource' ||
          (lvl === 'table' && sc?.table && source && tblEq(String(sc.table), source)) ||
          (lvl === 'widget' && widgetId && String(sc?.widgetId || '') === String(widgetId))
        )
        if (!scopeMatch) continue
        
        const type = String(tr?.type || '').toLowerCase()
        let name: string | null = null
        let expr: string | null = null
        
        if (type === 'computed' && tr?.name) {
          name = String(tr.name).trim()
          expr = tr?.expr ? String(tr.expr) : null
        } else if ((type === 'case' || type === 'replace' || type === 'translate' || type === 'nullhandling') && tr?.target) {
          name = String(tr.target).trim()
          // These transform types modify existing columns, so target column must exist
          if (lvl === 'datasource' && !baseColsSet.has(name.toLowerCase())) continue
        }
        
        if (!name) continue
        
        // For datasource-scoped computed transforms, check dependencies
        if (lvl === 'datasource' && expr) {
          const refs = extractRefs(expr)
          const allDepsExist = Array.from(refs).every(ref => baseColsSet.has(ref))
          if (!allDepsExist) continue
        }
        
        out.push(name)
      }
      
      // Join columns - check if sourceKey exists in base table for datasource-scoped joins
      const joins = Array.isArray((dsTransformsQ.data as any)?.joins) ? ((dsTransformsQ.data as any).joins as any[]) : []
      for (const j of joins) {
        const sc = (j?.scope || {}) as any
        const lvl = String(sc?.level || 'datasource').toLowerCase()
        const scopeMatch = (
          lvl === 'datasource' ||
          (lvl === 'table' && sc?.table && source && tblEq(String(sc.table), source)) ||
          (lvl === 'widget' && widgetId && String(sc?.widgetId || '') === String(widgetId))
        )
        if (!scopeMatch) continue
        
        // For datasource-scoped joins, check if sourceKey exists in base table
        if (lvl === 'datasource' && j?.sourceKey) {
          const sourceKey = String(j.sourceKey).toLowerCase()
          if (!baseColsSet.has(sourceKey)) continue
        }
        
        const cols = Array.isArray(j?.columns) ? (j.columns as any[]) : []
        cols.forEach((c: any) => { const nm = String((c?.alias || c?.name || '')).trim(); if (nm) out.push(nm) })
        const aggAlias = String((j?.aggregate as any)?.alias || '').trim()
        if (aggAlias) out.push(aggAlias)
      }
      
      return out
    } catch { return [] }
  }, [dsId, dsTransformsQ.data, source, widgetId, columnsMeta])
  const columns = [...new Set([...columnsMeta.map((c) => c.name), ...transformColumns])]

  const FORMAT_OPTIONS = ['none', 'short', 'currency', 'percent', 'wholeNumber', 'oneDecimal', 'twoDecimals'] as const

  const handleChange = (patch: Partial<ReportVariable>) => {
    onUpdate({ ...variable, ...patch })
  }

  const varType = variable.type || 'query'

  return (
      <div className="p-3 space-y-2.5">
        {/* Type selector */}
        <div>
          <label className="block text-[10px] font-medium text-muted-foreground mb-1">Type</label>
          <select className="w-full h-7 text-[11px] rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={varType} onChange={(e) => handleChange({ type: e.target.value as any })}>
            <option value="query">Query (from datasource)</option>
            <option value="expression">Expression (calculated)</option>
            <option value="datetime">Date / Time</option>
          </select>
        </div>

        {/* Type-specific fields */}
        {varType === 'expression' && (
          <div>
            <ExpressionBuilder
              expression={variable.expression || ''}
              allVarNames={allVariables.filter(v => v.id !== variable.id && !!v.name).map(v => v.name)}
              onChange={(expr) => handleChange({ expression: expr })}
            />
          </div>
        )}

        {varType === 'datetime' && (
          <div className="space-y-2">
            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="block text-[10px] font-medium text-muted-foreground mb-1">Value</label>
                <select className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={variable.datetimeExpr || 'now'} onChange={(e) => handleChange({ datetimeExpr: e.target.value as any })}>
                  <optgroup label="Now">
                    <option value="now">now()</option>
                    <option value="today">today()</option>
                  </optgroup>
                  <optgroup label="Days">
                    <option value="yesterday">YTDY – Yesterday</option>
                    <option value="last_working_day">LWDay – Last Working Day</option>
                    <option value="day_before_last_working_day">DBLWDay – Day Before Last Working Day</option>
                  </optgroup>
                  <optgroup label="Weeks">
                    <option value="this_week">TW – This Week</option>
                    <option value="last_week">LW – Last Week</option>
                    <option value="last_working_week">LWWeek – Last Working Week</option>
                    <option value="week_before_last_working_week">WBLWWeek – Week Before Last Working Week</option>
                  </optgroup>
                  <optgroup label="Months">
                    <option value="this_month">TMonth – This Month</option>
                    <option value="last_month">LMonth – Last Month</option>
                  </optgroup>
                  <optgroup label="Years">
                    <option value="this_year">TYear – This Year</option>
                    <option value="last_year">LYear – Last Year</option>
                  </optgroup>
                  <optgroup label="Cumulative">
                    <option value="ytd">YTD – Year to Date</option>
                    <option value="mtd">MTD – Month to Date</option>
                  </optgroup>
                </select>
              </div>
              {['now','today','yesterday','last_working_day','day_before_last_working_day'].includes(variable.datetimeExpr || 'now') && (
              <div>
                <label className="block text-[10px] font-medium text-muted-foreground mb-1">Format</label>
                <select
                  className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer"
                  value={variable.dateFormat || 'dd MMM yyyy'}
                  onChange={(e) => handleChange({ dateFormat: e.target.value })}
                >
                  <option value="dd MMM yyyy">22 Feb 2025</option>
                  <option value="dd/MM/yyyy">22/02/2025</option>
                  <option value="dd-MM-yyyy">22-02-2025</option>
                  <option value="MM/dd/yyyy">02/22/2025</option>
                  <option value="yyyy-MM-dd">2025-02-22</option>
                  <option value="dd MMMM yyyy">22 February 2025</option>
                  <option value="ddd, dd MMM yyyy">Sat, 22 Feb 2025</option>
                  <option value="dddd, dd MMMM yyyy">Saturday, 22 February 2025</option>
                  <option value="MMM yyyy">Feb 2025</option>
                  <option value="MMMM yyyy">February 2025</option>
                  <option value="dd MMM yyyy HH:mm">22 Feb 2025 14:30</option>
                  <option value="HH:mm">14:30</option>
                </select>
              </div>
              )}
            </div>
          </div>
        )}

        {varType === 'query' && (
          <>
            <div className="grid grid-cols-2 gap-2">
              <div className="relative z-10">
                <label className="block text-[10px] font-medium text-muted-foreground mb-1">Datasource</label>
                <div className="flex gap-1">
                  <select className="flex-1 h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={dsId} onChange={(e) => { setDsId(e.target.value); handleChange({ datasourceId: e.target.value, source: '' }) }}>
                    <option value="">Select…</option>
                    {datasources.map((ds: any) => <option key={ds.id} value={ds.id}>{ds.name || ds.id}</option>)}
                  </select>
                  <button
                    type="button"
                    className="h-7 w-7 flex items-center justify-center rounded-md border bg-secondary/40 hover:bg-secondary/60 transition-colors disabled:opacity-50 disabled:cursor-not-allowed relative z-10"
                    onClick={() => setShowExplorer(true)}
                    disabled={!dsId || dsQ.isLoading || !datasources.find((ds: any) => ds.id === dsId)}
                    title="Open Data Explorer"
                  >
                    <RiDatabase2Line size={14} />
                  </button>
                </div>
              </div>
              <div className="relative">
                <label className="block text-[10px] font-medium text-muted-foreground mb-1">Table</label>
                <select className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={source} onChange={(e) => { setSource(e.target.value); handleChange({ source: e.target.value }) }}>
                  <option value="">Select…</option>
                  {tables.map((t: string) => <option key={t} value={t}>{t}</option>)}
                </select>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-2">
              <div>
                <label className="block text-[10px] font-medium text-muted-foreground mb-1">Column</label>
                <select className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={variable.value?.field || ''} onChange={(e) => handleChange({ value: { ...variable.value, field: e.target.value } })}>
                  <option value="">Select…</option>
                  {columns.map((c) => <option key={c} value={c}>{c}</option>)}
                </select>
              </div>
              <div>
                <label className="block text-[10px] font-medium text-muted-foreground mb-1">Aggregation</label>
                <select className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={variable.value?.agg || 'none'} onChange={(e) => handleChange({ value: { ...variable.value, agg: e.target.value as any, avgDateField: undefined } })}>
                  <optgroup label="Standard">
                    {(['none','count','distinct','avg','sum','min','max'] as const).map((a) => <option key={a} value={a}>{a}</option>)}
                  </optgroup>
                  <optgroup label="Period Average">
                    <option value="avg_daily">Avg / Day</option>
                    <option value="avg_wday">Avg / WDay (working days)</option>
                    <option value="avg_weekly">Avg / Week</option>
                    <option value="avg_monthly">Avg / Month</option>
                  </optgroup>
                </select>
              </div>
            </div>
            {['avg_daily','avg_wday','avg_weekly','avg_monthly'].includes(variable.value?.agg || '') && (
              <div className="space-y-2">
                <div className="grid grid-cols-2 gap-2">
                  <div>
                    <label className="block text-[10px] font-medium text-muted-foreground mb-1">Numerator</label>
                    <select className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={variable.value?.avgNumerator || 'sum'} onChange={(e) => handleChange({ value: { ...variable.value, avgNumerator: e.target.value as any } })}>
                      <option value="sum">SUM(column)</option>
                      <option value="count">COUNT(column)</option>
                      <option value="distinct">COUNT DISTINCT(column)</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-[10px] font-medium text-muted-foreground mb-1">Date column <span className="text-primary">*</span></label>
                    <select className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={variable.value?.avgDateField || ''} onChange={(e) => handleChange({ value: { ...variable.value, avgDateField: e.target.value } })}>
                      <option value="">Select…</option>
                      {columns.map((c) => <option key={c} value={c}>{c}</option>)}
                    </select>
                  </div>
                </div>
                <p className="text-[9px] text-muted-foreground">{variable.value?.avgNumerator === 'distinct' ? 'COUNT(DISTINCT column)' : variable.value?.avgNumerator === 'count' ? 'COUNT(column)' : 'SUM(column)'} ÷ COUNT(DISTINCT {variable.value?.agg === 'avg_daily' ? 'day' : variable.value?.agg === 'avg_wday' ? 'working day' : variable.value?.agg === 'avg_weekly' ? 'week' : 'month'}){variable.value?.agg === 'avg_wday' ? ' — weekends excluded per app config' : ''}</p>
              </div>
            )}
          </>
        )}

        {/* Format / Prefix / Suffix - collapsible */}
        {varType !== 'datetime' && (
          <details className="group/fmt">
            <summary className="text-[10px] font-medium text-muted-foreground cursor-pointer select-none flex items-center gap-1 py-1 hover:text-foreground transition-colors">
              <span className="transition-transform duration-150 group-open/fmt:rotate-90 text-[8px]">&#9654;</span>
              Formatting
            </summary>
            <div className="grid grid-cols-3 gap-2 pt-1.5">
              <div>
                <label className="block text-[10px] font-medium text-muted-foreground mb-1">Format</label>
                <select className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={variable.format || 'none'} onChange={(e) => handleChange({ format: e.target.value as any })}>
                  {FORMAT_OPTIONS.map((f) => <option key={f} value={f}>{f}</option>)}
                </select>
              </div>
              <div>
                <label className="block text-[10px] font-medium text-muted-foreground mb-1">Prefix</label>
                <input className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={variable.prefix || ''} onChange={(e) => handleChange({ prefix: e.target.value })} placeholder="$" />
              </div>
              <div>
                <label className="block text-[10px] font-medium text-muted-foreground mb-1">Suffix</label>
                <input className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={variable.suffix || ''} onChange={(e) => handleChange({ suffix: e.target.value })} placeholder="%" />
              </div>
            </div>
          </details>
        )}

        {/* Calculations - collapsible */}
        {varType !== 'datetime' && (
          <details className="group/calc">
            <summary className="text-[10px] font-medium text-muted-foreground cursor-pointer select-none flex items-center gap-1 py-1 hover:text-foreground transition-colors">
              <span className="transition-transform duration-150 group-open/calc:rotate-90 text-[8px]">&#9654;</span>
              Calculations
            </summary>
            <div className="space-y-2 pt-1.5">
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="block text-[10px] font-medium text-muted-foreground mb-1">Multiply by</label>
                  <input
                    type="number"
                    step="any"
                    className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow"
                    value={variable.multiplyBy ?? ''}
                    onChange={(e) => handleChange({ multiplyBy: e.target.value ? parseFloat(e.target.value) : undefined })}
                    placeholder="1"
                  />
                </div>
                <div>
                  <label className="block text-[10px] font-medium text-muted-foreground mb-1">Divide by</label>
                  <input
                    type="number"
                    step="any"
                    className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow"
                    value={variable.divideBy ?? ''}
                    onChange={(e) => handleChange({ divideBy: e.target.value ? parseFloat(e.target.value) : undefined })}
                    placeholder="1"
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <label className="block text-[10px] font-medium text-muted-foreground mb-1">Rounding</label>
                  <select
                    className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer"
                    value={variable.roundMode || 'none'}
                    onChange={(e) => handleChange({ roundMode: e.target.value as any })}
                  >
                    <option value="none">None</option>
                    <option value="round">Round</option>
                    <option value="roundup">Round Up</option>
                    <option value="rounddown">Round Down</option>
                  </select>
                </div>
                {variable.roundMode && variable.roundMode !== 'none' && (
                  <div>
                    <label className="block text-[10px] font-medium text-muted-foreground mb-1">Decimal places</label>
                    <input
                      type="number"
                      min="0"
                      step="1"
                      className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow"
                      value={variable.roundDecimals ?? 0}
                      onChange={(e) => handleChange({ roundDecimals: e.target.value ? parseInt(e.target.value) : 0 })}
                      placeholder="0"
                    />
                  </div>
                )}
              </div>
            </div>
          </details>
        )}

        {/* Data Explorer Dialog */}
        {showExplorer && dsId && (() => {
          const ds = datasources.find((ds: any) => ds.id === dsId)
          if (!ds) return null
          return (
            <DataExplorerDialogV2
              open={true}
              onClose={() => setShowExplorer(false)}
              datasource={ds}
              initialTable={source || undefined}
            />
          )
        })()}

        {/* Filters */}
        {varType === 'query' && (
          <div className="border-t pt-2.5">
            <label className="block text-[10px] font-medium text-muted-foreground mb-1.5">Filters (WHERE)</label>
            <FilterEditor columns={columns} columnMeta={columnsMeta} where={variable.where || {}} onChange={(w) => handleChange({ where: w })} source={source} datasourceId={dsId} widgetId={widgetId} />
          </div>
        )}
        {/* Reverse Sign */}
        {varType !== 'datetime' && (
          <label className="flex items-center gap-2 text-[11px] text-muted-foreground cursor-pointer hover:text-foreground transition-colors select-none">
            <input type="checkbox" className="rounded border-border accent-primary" checked={!!variable.reverseSign} onChange={(e) => handleChange({ reverseSign: e.target.checked })} />
            Reverse sign (multiply result by −1)
          </label>
        )}
      </div>
  )
}

// ─── Rich Filter Editor (mirrors ConfiguratorPanel per-chip details) ──

// Detect field kind from DB type string (BIGINT, TIMESTAMP, VARCHAR, etc.)
function detectKindFromDbType(dbType?: string | null): 'date' | 'number' | 'string' | null {
  if (!dbType) return null
  const t = dbType.toUpperCase()
  if (/^(TIMESTAMP|DATE|DATETIME|TIME)/.test(t)) return 'date'
  if (/^(INT|INTEGER|BIGINT|SMALLINT|TINYINT|HUGEINT|UBIGINT|UINTEGER|USMALLINT|UTINYINT|FLOAT|DOUBLE|REAL|DECIMAL|NUMERIC|NUMBER)/.test(t)) return 'number'
  return 'string'
}

// Detect field kind from sample values (fallback when DB type is unavailable)
function detectFieldKind(samples: string[]): 'date' | 'number' | 'string' {
  if (!samples.length) return 'string'
  const dateRe = /^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?)?$/
  const dateLike = samples.filter(s => dateRe.test(String(s || '').trim())).length
  if (dateLike >= Math.ceil(samples.length / 2)) return 'date'
  const numLike = samples.filter(s => s !== '' && !isNaN(Number(s))).length
  if (numLike >= Math.ceil(samples.length / 2)) return 'number'
  return 'string'
}

// Manual values tab: fetches distinct values, checkboxes, search, select/deselect all
function ManualFilterValues({ field, source, datasourceId, widgetId, selected, onApply }: {
  field: string; source: string; datasourceId?: string; widgetId?: string; selected: any[]; onApply: (vals: any[]) => void
}) {
  const [sel, setSel] = useState<any[]>(selected || [])
  const [search, setSearch] = useState('')
  const [samples, setSamples] = useState<string[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => { setSel(selected || []) }, [JSON.stringify(selected)])

  // Fetch distinct values
  useEffect(() => {
    let abort = false
    async function run() {
      if (!source || !field) return
      setLoading(true)
      try {
        // Try DISTINCT endpoint first
        if (typeof (Api as any).distinct === 'function') {
          try {
            const res = await (Api as any).distinct({ source: String(source), field: String(field), where: undefined, datasourceId, widgetId })
            const vals = ((res?.values || []) as any[]).map(v => v != null ? String(v) : null).filter(Boolean) as string[]
            if (!abort) { setSamples(Array.from(new Set(vals)).sort()); setLoading(false) }
            return
          } catch {}
        }
        // Fallback: querySpec
        const spec: any = { source, select: [field], where: undefined, limit: 5000, offset: 0 }
        const res = await QueryApi.querySpec({ spec, datasourceId, limit: 5000, offset: 0, includeTotal: false })
        const cols = (res.columns || []) as string[]
        const idx = Math.max(0, cols.indexOf(field))
        const set = new Set<string>()
        ;(res.rows || []).forEach((arr: any) => { const v = Array.isArray(arr) ? arr[idx] : undefined; if (v != null) set.add(String(v)) })
        if (!abort) { setSamples(Array.from(set).sort()); setLoading(false) }
      } catch { if (!abort) { setSamples([]); setLoading(false) } }
    }
    run()
    return () => { abort = true }
  }, [field, source, datasourceId, widgetId])

  const filtered = samples.filter(v => String(v).toLowerCase().includes(search.toLowerCase()))
  const toggle = (v: any) => setSel(prev => prev.includes(v) ? prev.filter(x => x !== v) : [...prev, v])

  return (
    <div className="rounded-md border bg-card p-2 space-y-2">
      <div className="flex items-center justify-between">
        <div className="text-[11px] font-medium">Filter values: {field}</div>
        <div className="flex items-center gap-1.5">
          <button className="text-[10px] px-1.5 py-0.5 rounded border hover:bg-muted" onClick={() => { setSel([]); onApply([]) }}>Clear</button>
          <button className="text-[10px] px-1.5 py-0.5 rounded border bg-primary text-primary-foreground hover:bg-primary/90" onClick={() => onApply(sel)}>Apply</button>
        </div>
      </div>
      <input className="w-full px-2 py-1 rounded bg-secondary/60 text-[11px]" placeholder="Search values" value={search} onChange={e => setSearch(e.target.value)} />
      <div className="flex items-center justify-between text-[10px] text-muted-foreground">
        <span>{sel.length} of {samples.length} selected{loading && <span className="ml-1 opacity-60">(loading…)</span>}</span>
        <div className="flex gap-2">
          <button className="hover:text-foreground" onClick={() => setSel([...filtered])}>Select All</button>
          <button className="hover:text-foreground" onClick={() => setSel([])}>Deselect All</button>
        </div>
      </div>
      <div className="max-h-44 overflow-auto">
        <ul className="space-y-0.5">
          {filtered.map((v, i) => (
            <li key={i} className="flex items-center gap-2 text-[11px]">
              <input type="checkbox" className="rounded" checked={sel.includes(v)} onChange={() => toggle(v)} />
              <span className="truncate max-w-[200px]" title={String(v)}>{String(v)}</span>
            </li>
          ))}
          {sel.filter(v => !filtered.includes(v)).map((v, i) => (
            <li key={`s-${i}`} className="flex items-center gap-2 text-[11px] opacity-60">
              <input type="checkbox" className="rounded" checked onChange={() => toggle(v)} />
              <span className="truncate max-w-[200px]">{String(v)} (selected)</span>
            </li>
          ))}
          {filtered.length === 0 && !loading && <li className="text-[10px] text-muted-foreground">No values available</li>}
          {loading && filtered.length === 0 && <li className="text-[10px] text-muted-foreground">Loading…</li>}
        </ul>
      </div>
    </div>
  )
}

// Date rule tab: presets (Today, Yesterday…) + custom (After/Before/Between)
// Week-start-day: 0=Sunday (default via NEXT_PUBLIC_WEEK_START_DAY), 1=Monday
const _DEFAULT_WEEK_START = (typeof process !== 'undefined' && process.env?.NEXT_PUBLIC_WEEK_START_DAY) || 'SUN'
const _DEFAULT_WEEKENDS = (typeof process !== 'undefined' && process.env?.NEXT_PUBLIC_WEEKENDS) || 'SAT_SUN'
const WEEK_PRESETS = new Set(['this_week', 'last_week', 'week_before_last'])
const WORKING_DAY_PRESETS = new Set(['last_working_day', 'day_before_last_working_day', 'last_working_week', 'week_before_last_working_week'])
const WORKING_WEEK_PRESETS = new Set(['last_working_week', 'week_before_last_working_week'])

function DateRuleEditor({ field, where, onPatch }: { field: string; where: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  type Preset = 'today'|'yesterday'|'day_before_yesterday'|'last_working_day'|'day_before_last_working_day'|'last_working_week'|'week_before_last_working_week'|'this_week'|'last_week'|'week_before_last'|'this_month'|'last_month'|'this_quarter'|'last_quarter'|'this_year'|'last_year'
  type DateOp = 'eq'|'ne'|'gt'|'gte'|'lt'|'lte'|'between'
  const [mode, setMode] = useState<'preset'|'custom'>('preset')
  const [preset, setPreset] = useState<Preset>('today')
  const [weekStartDay, setWeekStartDay] = useState<string>(() => String(where?.['__week_start_day'] ?? _DEFAULT_WEEK_START).toUpperCase())
  const [weekends, setWeekends] = useState<string>(() => String(where?.['__weekends'] ?? _DEFAULT_WEEKENDS).toUpperCase())
  const [op, setOp] = useState<DateOp>('eq')
  const [a, setA] = useState(''); const [b, setB] = useState('')

  const isWeekPreset = WEEK_PRESETS.has(preset)
  const isWorkingDayPreset = WORKING_DAY_PRESETS.has(preset)
  const isWorkingWeekPreset = WORKING_WEEK_PRESETS.has(preset)

  function rangeForPreset(p: Preset): { gte?: string; lt?: string } {
    const now = new Date()
    const ymd = (d: Date) => `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`
    const som = (d: Date) => new Date(d.getFullYear(), d.getMonth(), 1)
    const eom = (d: Date) => new Date(d.getFullYear(), d.getMonth()+1, 1)
    const q = Math.floor(now.getMonth()/3)
    const soq = (y: number, qq: number) => new Date(y, qq*3, 1)
    const eoq = (y: number, qq: number) => new Date(y, qq*3+3, 1)
    // Week helpers — respects weekStartDay (DDD: SUN/MON/TUE/WED/THU/FRI/SAT). JS getDay(): 0=Sun…6=Sat
    const _WSD_MAP: Record<string, number> = { SUN: 0, MON: 1, TUE: 2, WED: 3, THU: 4, FRI: 5, SAT: 6 }
    const wsdNum = _WSD_MAP[weekStartDay] ?? 0
    const startOfWeek = (d: Date) => {
      const s = new Date(d.getFullYear(), d.getMonth(), d.getDate())
      const dow = s.getDay() // 0=Sun…6=Sat
      const offset = (dow - wsdNum + 7) % 7
      s.setDate(s.getDate() - offset)
      return s
    }
    // JS getDay(): 0=Sun,1=Mon,2=Tue,3=Wed,4=Thu,5=Fri,6=Sat
    const weekendDaysJs = weekends === 'FRI_SAT' ? [5, 6] : [0, 6]
    const prevWorkday = (d: Date) => {
      const c = new Date(d); c.setDate(c.getDate() - 1)
      while (weekendDaysJs.includes(c.getDay())) c.setDate(c.getDate() - 1)
      return c
    }
    // Working week starts on Monday (SAT_SUN weekends) or Sunday (FRI_SAT weekends)
    const workingWeekStartDay = weekends === 'FRI_SAT' ? 0 : 1 // JS getDay(): 0=Sun, 1=Mon
    const startOfWorkingWeek = (d: Date) => {
      const s = new Date(d.getFullYear(), d.getMonth(), d.getDate())
      while (s.getDay() !== workingWeekStartDay) s.setDate(s.getDate() - 1)
      return s
    }
    switch (p) {
      case 'today': { const s = new Date(now.getFullYear(), now.getMonth(), now.getDate()); const e = new Date(s); e.setDate(e.getDate()+1); return { gte: ymd(s), lt: ymd(e) } }
      case 'yesterday': { const e = new Date(now.getFullYear(), now.getMonth(), now.getDate()); const s = new Date(e); s.setDate(s.getDate()-1); return { gte: ymd(s), lt: ymd(e) } }
      case 'day_before_yesterday': { const lt = new Date(now.getFullYear(), now.getMonth(), now.getDate()); lt.setDate(lt.getDate()-1); const s = new Date(lt); s.setDate(s.getDate()-1); return { gte: ymd(s), lt: ymd(lt) } }
      case 'last_working_day': { const t0 = new Date(now.getFullYear(),now.getMonth(),now.getDate()); const lwd = prevWorkday(t0); const e = new Date(lwd); e.setDate(e.getDate()+1); return { gte: ymd(lwd), lt: ymd(e) } }
      case 'day_before_last_working_day': { const t0 = new Date(now.getFullYear(),now.getMonth(),now.getDate()); const dlwd = prevWorkday(prevWorkday(t0)); const e = new Date(dlwd); e.setDate(e.getDate()+1); return { gte: ymd(dlwd), lt: ymd(e) } }
      case 'last_working_week': { const ws = startOfWorkingWeek(now); const s = new Date(ws); s.setDate(s.getDate()-7); return { gte: ymd(s), lt: ymd(ws) } }
      case 'week_before_last_working_week': { const ws = startOfWorkingWeek(now); const s = new Date(ws); s.setDate(s.getDate()-14); const e = new Date(ws); e.setDate(e.getDate()-7); return { gte: ymd(s), lt: ymd(e) } }
      case 'this_week': { const ws = startOfWeek(now); const e = new Date(ws); e.setDate(e.getDate()+7); return { gte: ymd(ws), lt: ymd(e) } }
      case 'last_week': { const ws = startOfWeek(now); const s = new Date(ws); s.setDate(s.getDate()-7); return { gte: ymd(s), lt: ymd(ws) } }
      case 'week_before_last': { const ws = startOfWeek(now); const s = new Date(ws); s.setDate(s.getDate()-14); const e = new Date(ws); e.setDate(e.getDate()-7); return { gte: ymd(s), lt: ymd(e) } }
      case 'this_month': return { gte: ymd(som(now)), lt: ymd(eom(now)) }
      case 'last_month': { const s = som(now); s.setMonth(s.getMonth()-1); return { gte: ymd(s), lt: ymd(new Date(s.getFullYear(), s.getMonth()+1, 1)) } }
      case 'this_quarter': return { gte: ymd(soq(now.getFullYear(), q)), lt: ymd(eoq(now.getFullYear(), q)) }
      case 'last_quarter': { const pq = (q+3)%4; const yr = q===0 ? now.getFullYear()-1 : now.getFullYear(); return { gte: ymd(soq(yr, pq)), lt: ymd(eoq(yr, pq)) } }
      case 'this_year': return { gte: ymd(new Date(now.getFullYear(),0,1)), lt: ymd(new Date(now.getFullYear()+1,0,1)) }
      case 'last_year': return { gte: ymd(new Date(now.getFullYear()-1,0,1)), lt: ymd(new Date(now.getFullYear(),0,1)) }
    }
  }

  const applyPreset = (p: Preset, wsd?: string, wkends?: string, operator?: DateOp) => {
    const effectiveWsd = wsd ?? weekStartDay
    const effectiveWkends = wkends ?? weekends
    const effectiveOp = operator ?? op
    const range = rangeForPreset(p)
    
    // Clear all date-related keys first
    const patch: Record<string, any> = {
      [`${field}__gte`]: undefined,
      [`${field}__gt`]: undefined,
      [`${field}__lt`]: undefined,
      [`${field}__lte`]: undefined,
      [`${field}__ne`]: undefined,
      [field]: undefined,
      [`${field}__date_preset`]: p,
      __week_start_day: effectiveWsd,
      __weekends: effectiveWkends,
      [`${field}__op`]: effectiveOp,
    }
    
    // Apply based on operator
    if (range.gte && range.lt) {
      switch (effectiveOp) {
        case 'eq':
          // Equals: date is in the preset range (use between logic)
          patch[`${field}__gte`] = range.gte
          patch[`${field}__lt`] = range.lt
          break
        case 'ne':
          // Not equals: date is NOT in the preset range
          patch[`${field}__ne`] = range.gte // Store the start date as reference
          break
        case 'gt':
          // Greater than: after the preset range end
          patch[`${field}__gt`] = range.lt
          break
        case 'gte':
          // Greater or equal: on or after the preset range start
          patch[`${field}__gte`] = range.gte
          break
        case 'lt':
          // Less than: before the preset range start
          patch[`${field}__lt`] = range.gte
          break
        case 'lte':
          // Less or equal: on or before the preset range end
          patch[`${field}__lte`] = range.lt
          break
        case 'between':
          // Between: the full preset range
          patch[`${field}__gte`] = range.gte
          patch[`${field}__lt`] = range.lt
          break
      }
    }
    
    onPatch(patch)
  }

  const applyCustom = () => {
    const patch: Record<string, any> = {
      [`${field}__gte`]: undefined,
      [`${field}__gt`]: undefined,
      [`${field}__lt`]: undefined,
      [`${field}__lte`]: undefined,
      [`${field}__ne`]: undefined,
      [field]: undefined,
      [`${field}__date_preset`]: undefined,
      __week_start_day: undefined,
      __weekends: undefined,
      [`${field}__op`]: op,
    }
    
    const has = (x: string) => x && x.trim() !== ''
    
    switch (op) {
      case 'eq':
        if (has(a)) {
          // Equals: on this specific date
          patch[`${field}__gte`] = a
          const nextDay = new Date(`${a}T00:00:00`)
          nextDay.setDate(nextDay.getDate() + 1)
          patch[`${field}__lt`] = `${nextDay.getFullYear()}-${String(nextDay.getMonth()+1).padStart(2,'0')}-${String(nextDay.getDate()).padStart(2,'0')}`
        }
        break
      case 'ne':
        if (has(a)) {
          // Not equals: not on this specific date
          patch[field] = a // Store for reference, backend will handle as NOT equals
        }
        break
      case 'gt':
        if (has(a)) {
          // Greater than: after this date
          const nextDay = new Date(`${a}T00:00:00`)
          nextDay.setDate(nextDay.getDate() + 1)
          patch[`${field}__gt`] = `${nextDay.getFullYear()}-${String(nextDay.getMonth()+1).padStart(2,'0')}-${String(nextDay.getDate()).padStart(2,'0')}`
        }
        break
      case 'gte':
        if (has(a)) {
          // Greater or equal: on or after this date
          patch[`${field}__gte`] = a
        }
        break
      case 'lt':
        if (has(a)) {
          // Less than: before this date
          patch[`${field}__lt`] = a
        }
        break
      case 'lte':
        if (has(a)) {
          // Less or equal: on or before this date
          const nextDay = new Date(`${a}T00:00:00`)
          nextDay.setDate(nextDay.getDate() + 1)
          patch[`${field}__lt`] = `${nextDay.getFullYear()}-${String(nextDay.getMonth()+1).padStart(2,'0')}-${String(nextDay.getDate()).padStart(2,'0')}`
        }
        break
      case 'between':
        if (has(a)) patch[`${field}__gte`] = a
        if (has(b)) {
          const nextDay = new Date(`${b}T00:00:00`)
          nextDay.setDate(nextDay.getDate() + 1)
          patch[`${field}__lt`] = `${nextDay.getFullYear()}-${String(nextDay.getMonth()+1).padStart(2,'0')}-${String(nextDay.getDate()).padStart(2,'0')}`
        }
        break
    }
    
    onPatch(patch)
  }

  // Hydrate from where on mount
  useEffect(() => {
    const existingPreset = where?.[`${field}__date_preset`] as string | undefined
    const savedOp = where?.[`${field}__op`] as DateOp | undefined
    if (savedOp && ['eq','ne','gt','gte','lt','lte','between'].includes(savedOp)) {
      setOp(savedOp)
    }
    if (existingPreset) {
      const allPresets: Preset[] = ['today','yesterday','day_before_yesterday','last_working_day','day_before_last_working_day','last_working_week','week_before_last_working_week','this_week','last_week','week_before_last','this_month','last_month','this_quarter','last_quarter','this_year','last_year']
      if (allPresets.includes(existingPreset as Preset)) { setMode('preset'); setPreset(existingPreset as Preset) }
      const savedWsd = where?.['__week_start_day']
      if (savedWsd != null) setWeekStartDay(String(savedWsd).toUpperCase())
      const savedWkends = where?.['__weekends']
      if (savedWkends != null) setWeekends(String(savedWkends).toUpperCase())
      return
    }
    const gte = where?.[`${field}__gte`] as string | undefined
    const gt = where?.[`${field}__gt`] as string | undefined
    const lt = where?.[`${field}__lt`] as string | undefined
    const lte = where?.[`${field}__lte`] as string | undefined
    const ne = where?.[`${field}__ne`] as string | undefined
    const eq = where?.[field]
    
    if (gte || gt || lt || lte || ne || eq) {
      setMode('custom')
      // Detect operator from where
      if (gte && lt) { setOp('between'); setA(gte); try { const d = new Date(`${lt}T00:00:00`); d.setDate(d.getDate()-1); setB(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`) } catch {} }
      else if (gte && !lt) { setOp('gte'); setA(gte) }
      else if (gt) { setOp('gt'); setA(gt) }
      else if (lt && !gte) { setOp('lt'); setA(lt) }
      else if (lte) { setOp('lte'); setA(lte) }
      else if (ne) { setOp('ne'); setA(String(ne)) }
      else if (eq) { setOp('eq'); setA(String(eq)) }
    }
  }, [field])

  return (
    <div className="rounded-md border bg-card p-2 space-y-2">
      <div className="flex items-center justify-between">
        <div className="text-[11px] font-medium">Date rule: {field}</div>
        <button className="text-[10px] px-1.5 py-0.5 rounded border hover:bg-muted" onClick={() => { setMode('preset'); setPreset('today'); setOp('between'); setA(''); setB(''); onPatch({ [field]: undefined, [`${field}__gte`]: undefined, [`${field}__gt`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined, [`${field}__ne`]: undefined, [`${field}__date_preset`]: undefined, [`${field}__op`]: undefined, __week_start_day: undefined, __weekends: undefined }) }}>Clear</button>
      </div>
      <div className="flex items-center gap-3 text-[11px]">
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='preset'} onChange={() => setMode('preset')} /> Preset</label>
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='custom'} onChange={() => setMode('custom')} /> Custom</label>
      </div>
      
      {/* Operator selector - shown for both modes */}
      <select className="w-full px-2 py-1 rounded bg-secondary/60 text-[11px]" value={op} onChange={e => { const newOp = e.target.value as DateOp; setOp(newOp); if (mode === 'preset') applyPreset(preset, undefined, undefined, newOp) }}>
        <option value="eq">Equals</option>
        <option value="ne">Not equals</option>
        <option value="gt">Greater than (after)</option>
        <option value="gte">Greater or equal (on or after)</option>
        <option value="lt">Less than (before)</option>
        <option value="lte">Less or equal (on or before)</option>
        {mode === 'custom' && <option value="between">Between</option>}
      </select>
      
      {mode === 'preset' ? (
        <div className="space-y-1.5">
          <select className="w-full px-2 py-1 rounded bg-secondary/60 text-[11px]" value={preset} onChange={e => { const p = e.target.value as Preset; setPreset(p); applyPreset(p) }}>
            <optgroup label="Days">
              <option value="today">Today</option>
              <option value="yesterday">Yesterday</option>
              <option value="day_before_yesterday">Day Before Yesterday</option>
              <option value="last_working_day">Last Working Day</option>
              <option value="day_before_last_working_day">Day Before Last Working Day</option>
            </optgroup>
            <optgroup label="Working Weeks">
              <option value="last_working_week">Last Working Week</option>
              <option value="week_before_last_working_week">Week Before Last Working Week</option>
            </optgroup>
            <optgroup label="Weeks">
              <option value="this_week">This Week</option>
              <option value="last_week">Last Week</option>
              <option value="week_before_last">Week Before Last</option>
            </optgroup>
            <optgroup label="Months">
              <option value="this_month">This Month</option>
              <option value="last_month">Last Month</option>
            </optgroup>
            <optgroup label="Quarters">
              <option value="this_quarter">This Quarter</option>
              <option value="last_quarter">Last Quarter</option>
            </optgroup>
            <optgroup label="Years">
              <option value="this_year">This Year</option>
              <option value="last_year">Last Year</option>
            </optgroup>
          </select>
          {(isWeekPreset || isWorkingWeekPreset) && (
            <div className="flex items-center gap-2">
              <span className="text-[10px] text-muted-foreground shrink-0">Week starts on</span>
              <select
                className="flex-1 px-2 py-0.5 rounded bg-secondary/60 text-[11px]"
                value={weekStartDay}
                onChange={e => { setWeekStartDay(e.target.value); applyPreset(preset, e.target.value) }}
              >
                <option value="SUN">Sunday</option>
                <option value="MON">Monday</option>
                <option value="TUE">Tuesday</option>
                <option value="WED">Wednesday</option>
                <option value="THU">Thursday</option>
                <option value="FRI">Friday</option>
                <option value="SAT">Saturday</option>
              </select>
            </div>
          )}
          {isWorkingDayPreset && (
            <div className="flex items-center gap-2">
              <span className="text-[10px] text-muted-foreground shrink-0">Weekends</span>
              <select
                className="flex-1 px-2 py-0.5 rounded bg-secondary/60 text-[11px]"
                value={weekends}
                onChange={e => { setWeekends(e.target.value); applyPreset(preset, undefined, e.target.value) }}
              >
                <option value="SAT_SUN">Sat – Sun</option>
                <option value="FRI_SAT">Fri – Sat</option>
              </select>
            </div>
          )}
          <button className="text-[10px] px-2 py-0.5 rounded bg-primary text-primary-foreground hover:bg-primary/90" onClick={() => applyPreset(preset)}>Apply</button>
        </div>
      ) : (
        <div className="space-y-2">
          {op === 'between' ? (
            <div className="flex gap-2 items-center">
              <input type="date" className="flex-1 h-7 px-1 rounded border text-[11px] bg-secondary/60" placeholder="Start date" value={a} onChange={e => setA(e.target.value)} />
              <span className="text-[10px] text-muted-foreground">to</span>
              <input type="date" className="flex-1 h-7 px-1 rounded border text-[11px] bg-secondary/60" placeholder="End date" value={b} onChange={e => setB(e.target.value)} />
            </div>
          ) : (
            <input type="date" className="w-full h-7 px-1 rounded border text-[11px] bg-secondary/60" value={a} onChange={e => setA(e.target.value)} />
          )}
          <div className="flex justify-end">
            <button className="text-[10px] px-2 py-0.5 rounded bg-primary text-primary-foreground hover:bg-primary/90" onClick={applyCustom}>Apply</button>
          </div>
        </div>
      )}
    </div>
  )
}

// String rule tab: contains, not contains, eq, ne, starts with, ends with
function StringRuleEditor({ field, where, onPatch }: { field: string; where: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  type StrOp = 'contains'|'not_contains'|'eq'|'ne'|'starts_with'|'ends_with'
  const [op, setOp] = useState<StrOp>('contains')
  const [val, setVal] = useState('')

  useEffect(() => {
    const eqArr = where?.[field]; const contains = where?.[`${field}__contains`]; const ne = where?.[`${field}__ne`]
    const starts = where?.[`${field}__startswith`]; const ends = where?.[`${field}__endswith`]; const notc = where?.[`${field}__notcontains`]
    if (Array.isArray(eqArr) && eqArr.length >= 1) { setOp('eq'); setVal(eqArr.map(String).join(', ')) }
    else if (ne) { setOp('ne'); setVal(Array.isArray(ne) ? ne.join(', ') : String(ne)) }
    else if (contains) { setOp('contains'); setVal(Array.isArray(contains) ? contains.join(', ') : String(contains)) }
    else if (notc) { setOp('not_contains'); setVal(Array.isArray(notc) ? notc.join(', ') : String(notc)) }
    else if (starts) { setOp('starts_with'); setVal(Array.isArray(starts) ? starts.join(', ') : String(starts)) }
    else if (ends) { setOp('ends_with'); setVal(Array.isArray(ends) ? ends.join(', ') : String(ends)) }
  }, [field])

  const apply = () => {
    const patch: Record<string, any> = { [field]: undefined, [`${field}__contains`]: undefined, [`${field}__notcontains`]: undefined, [`${field}__startswith`]: undefined, [`${field}__endswith`]: undefined, [`${field}__ne`]: undefined }
    const v = val.trim(); if (!v) { onPatch(patch); return }
    const vals = v.split(',').map(s => s.trim()).filter(Boolean)
    switch (op) {
      case 'eq': patch[field] = vals; break
      case 'ne': patch[`${field}__ne`] = vals; break
      case 'contains': patch[`${field}__contains`] = vals; break
      case 'not_contains': patch[`${field}__notcontains`] = vals; break
      case 'starts_with': patch[`${field}__startswith`] = vals; break
      case 'ends_with': patch[`${field}__endswith`] = vals; break
    }
    onPatch(patch)
  }

  return (
    <div className="rounded-md border bg-card p-2 space-y-2">
      <div className="flex items-center justify-between">
        <div className="text-[11px] font-medium">String rule: {field}</div>
        <div className="flex items-center gap-1.5">
          <button className="text-[10px] px-1.5 py-0.5 rounded border hover:bg-muted" onClick={() => { setVal(''); onPatch({ [field]: undefined, [`${field}__contains`]: undefined, [`${field}__notcontains`]: undefined, [`${field}__startswith`]: undefined, [`${field}__endswith`]: undefined, [`${field}__ne`]: undefined }) }}>Clear</button>
          <button className="text-[10px] px-1.5 py-0.5 rounded border bg-primary text-primary-foreground hover:bg-primary/90" onClick={apply}>Apply</button>
        </div>
      </div>
      <div className="grid grid-cols-3 gap-1.5 items-center">
        <select className="col-span-1 px-1 py-1 rounded bg-secondary/60 text-[11px]" value={op} onChange={e => setOp(e.target.value as StrOp)}>
          <option value="contains">Contains</option><option value="not_contains">Not contains</option>
          <option value="eq">Equals</option><option value="ne">Not equals</option>
          <option value="starts_with">Starts with</option><option value="ends_with">Ends with</option>
        </select>
        <input className="col-span-2 h-7 px-2 rounded border text-[11px] bg-secondary/60" placeholder="Value (comma-separated)" value={val} onChange={e => setVal(e.target.value)} />
      </div>
    </div>
  )
}

// Number rule tab: eq, ne, gt, gte, lt, lte, between
function NumberRuleEditor({ field, where, onPatch }: { field: string; where: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  type NumOp = 'eq'|'ne'|'gt'|'gte'|'lt'|'lte'|'between'
  const [op, setOp] = useState<NumOp>('eq')
  const [a, setA] = useState<number|''>('')
  const [b2, setB2] = useState<number|''>('')

  useEffect(() => {
    const gte = where?.[`${field}__gte`]; const lte = where?.[`${field}__lte`]; const gt = where?.[`${field}__gt`]; const lt = where?.[`${field}__lt`]; const ne = where?.[`${field}__ne`]; const eq = where?.[field]
    if (typeof gte === 'number' && typeof lte === 'number') { setOp('between'); setA(gte); setB2(lte) }
    else if (Array.isArray(eq) && eq.length === 1) { setOp('eq'); setA(Number(eq[0])) }
    else if (typeof ne === 'number') { setOp('ne'); setA(ne) }
    else if (typeof gt === 'number') { setOp('gt'); setA(gt) }
    else if (typeof gte === 'number') { setOp('gte'); setA(gte) }
    else if (typeof lt === 'number') { setOp('lt'); setA(lt) }
    else if (typeof lte === 'number') { setOp('lte'); setA(lte) }
  }, [field])

  const apply = () => {
    const patch: Record<string, any> = { [field]: undefined, [`${field}__gt`]: undefined, [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined, [`${field}__ne`]: undefined }
    const has = (x: any) => typeof x === 'number' && !isNaN(x)
    switch (op) {
      case 'eq': if (has(a)) patch[field] = [a]; break
      case 'ne': if (has(a)) patch[`${field}__ne`] = a; break
      case 'gt': if (has(a)) patch[`${field}__gt`] = a; break
      case 'gte': if (has(a)) patch[`${field}__gte`] = a; break
      case 'lt': if (has(a)) patch[`${field}__lt`] = a; break
      case 'lte': if (has(a)) patch[`${field}__lte`] = a; break
      case 'between': if (has(a)) patch[`${field}__gte`] = a; if (has(b2)) patch[`${field}__lte`] = b2; break
    }
    onPatch(patch)
  }

  return (
    <div className="rounded-md border bg-card p-2 space-y-2">
      <div className="flex items-center justify-between">
        <div className="text-[11px] font-medium">Number filter: {field}</div>
        <div className="flex items-center gap-1.5">
          <button className="text-[10px] px-1.5 py-0.5 rounded border hover:bg-muted" onClick={() => { setA(''); setB2(''); onPatch({ [field]: undefined, [`${field}__gt`]: undefined, [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined, [`${field}__ne`]: undefined }) }}>Clear</button>
          <button className="text-[10px] px-1.5 py-0.5 rounded border bg-primary text-primary-foreground hover:bg-primary/90" onClick={apply}>Apply</button>
        </div>
      </div>
      <div className="grid grid-cols-3 gap-1.5 items-center">
        <select className="col-span-1 px-1 py-1 rounded bg-secondary/60 text-[11px]" value={op} onChange={e => setOp(e.target.value as NumOp)}>
          <option value="eq">Equals</option><option value="ne">Not equals</option>
          <option value="gt">Greater than</option><option value="gte">Greater or equal</option>
          <option value="lt">Less than</option><option value="lte">Less or equal</option>
          <option value="between">Between</option>
        </select>
        {op !== 'between' ? (
          <input type="number" className="col-span-2 h-7 px-2 rounded border text-[11px] bg-secondary/60" value={a} onChange={e => setA(e.target.value === '' ? '' : Number(e.target.value))} />
        ) : (
          <><input type="number" className="col-span-1 h-7 px-1 rounded border text-[11px] bg-secondary/60" placeholder="Min" value={a} onChange={e => setA(e.target.value === '' ? '' : Number(e.target.value))} />
          <input type="number" className="col-span-1 h-7 px-1 rounded border text-[11px] bg-secondary/60" placeholder="Max" value={b2} onChange={e => setB2(e.target.value === '' ? '' : Number(e.target.value))} /></>
        )}
      </div>
    </div>
  )
}

// Per-field filter with Manual/Rule tabs (like ConfiguratorPanel chip details)
function ReportFieldFilter({ field, source, datasourceId, widgetId, dbType, where, onWhereChange, onRemove }: {
  field: string; source: string; datasourceId?: string; widgetId?: string; dbType?: string | null
  where: Record<string, any>; onWhereChange: (w: Record<string, any>) => void; onRemove: () => void
}) {
  const [tab, setTab] = useState<'manual'|'rule'>('manual')
  const [samples, setSamples] = useState<string[]>([])

  // Only fetch samples for type detection when DB type is unavailable
  useEffect(() => {
    if (dbType) return  // DB type known — skip sample fetch for type detection
    let abort = false
    async function run() {
      if (!source || !field) return
      try {
        if (typeof (Api as any).distinct === 'function') {
          const res = await (Api as any).distinct({ source, field, where: undefined, datasourceId })
          const vals = ((res?.values || []) as any[]).map(v => v != null ? String(v) : '').filter(Boolean).slice(0, 20)
          if (!abort) setSamples(vals)
          return
        }
        const spec: any = { source, select: [field], limit: 50, offset: 0 }
        const res = await QueryApi.querySpec({ spec, datasourceId, limit: 50, offset: 0, includeTotal: false })
        const cols = (res.columns || []) as string[]
        const idx = Math.max(0, cols.indexOf(field))
        const vals = (res.rows || []).map((r: any) => Array.isArray(r) ? String(r[idx] ?? '') : '').filter(Boolean).slice(0, 20)
        if (!abort) setSamples(vals)
      } catch { if (!abort) setSamples([]) }
    }
    run()
    return () => { abort = true }
  }, [field, source, datasourceId, dbType])

  const kind = detectKindFromDbType(dbType) ?? detectFieldKind(samples)

  // Extract current selected values for manual tab (field key with array value)
  const currentSelected = Array.isArray(where?.[field]) ? (where[field] as any[]) : []

  const handleManualApply = (vals: any[]) => {
    const next = { ...where }
    if (vals.length > 0) next[field] = vals
    else delete next[field]
    onWhereChange(next)
  }

  const handleRulePatch = (patch: Record<string, any>) => {
    const next = { ...where }
    Object.entries(patch).forEach(([k, v]) => { if (v === undefined) delete next[k]; else next[k] = v })
    onWhereChange(next)
  }

  return (
    <div className="space-y-1.5">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1">
          <span className="text-[11px] font-medium truncate max-w-[140px]" title={field}>{field}</span>
          <span className="text-[9px] px-1 py-0.5 rounded bg-secondary/60 text-muted-foreground">{kind}</span>
        </div>
        <button className="text-destructive hover:bg-destructive/10 rounded p-0.5" onClick={onRemove} title="Remove filter">
          <RiCloseLine className="h-3 w-3" />
        </button>
      </div>
      <div className="flex items-center gap-1.5">
        <button className={`text-[10px] px-2 py-0.5 rounded border ${tab==='manual' ? 'bg-secondary' : ''}`} onClick={() => setTab('manual')}>Manual</button>
        <button className={`text-[10px] px-2 py-0.5 rounded border ${tab==='rule' ? 'bg-secondary' : ''}`} onClick={() => setTab('rule')}>Rule</button>
      </div>
      {tab === 'manual' ? (
        <ManualFilterValues field={field} source={source} datasourceId={datasourceId} widgetId={widgetId} selected={currentSelected} onApply={handleManualApply} />
      ) : kind === 'date' ? (
        <DateRuleEditor field={field} where={where} onPatch={handleRulePatch} />
      ) : kind === 'number' ? (
        <NumberRuleEditor field={field} where={where} onPatch={handleRulePatch} />
      ) : (
        <StringRuleEditor field={field} where={where} onPatch={handleRulePatch} />
      )}
    </div>
  )
}

// Main filter editor: list of per-field filters + field picker
function FilterEditor({ columns, columnMeta, where, onChange, source, datasourceId, widgetId }: {
  columns: string[]; columnMeta?: Array<{ name: string; type: string | null }>; where: Record<string, unknown>; onChange: (w: Record<string, unknown>) => void
  source?: string; datasourceId?: string; widgetId?: string
}) {
  const [picking, setPicking] = useState(false)
  const [pickSearch, setPickSearch] = useState('')
  const [dropPos, setDropPos] = useState<{ top: number; left: number; width: number }>({ top: 0, left: 0, width: 220 })
  const addBtnRef = useRef<HTMLButtonElement>(null)
  const dropRef = useRef<HTMLDivElement>(null)

  // Close dropdown on outside click
  useEffect(() => {
    if (!picking) return
    const handler = (e: MouseEvent) => {
      if (
        dropRef.current && !dropRef.current.contains(e.target as Node) &&
        addBtnRef.current && !addBtnRef.current.contains(e.target as Node)
      ) { setPicking(false); setPickSearch('') }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [picking])

  // Collect active filter fields (base field names, dedup operator suffixes)
  // Exclude meta keys that start with __ (like __weekends, __week_start_day)
  const activeFields = useMemo(() => {
    const set = new Set<string>()
    Object.keys(where).forEach(k => {
      if (where[k] === undefined) return
      if (k.startsWith('__')) return // Skip meta keys
      const base = k.split('__')[0]
      if (base) set.add(base) // Only add non-empty base names
    })
    return Array.from(set)
  }, [where])

  const availableFields = columns.filter(c => !activeFields.includes(c))
  const filteredPick = availableFields.filter(c => c.toLowerCase().includes(pickSearch.toLowerCase()))

  const openPicker = () => {
    if (addBtnRef.current) {
      const r = addBtnRef.current.getBoundingClientRect()
      setDropPos({ top: r.bottom + 4, left: r.left, width: Math.max(220, r.width) })
    }
    setPickSearch('')
    setPicking(true)
  }

  const pickField = (field: string) => {
    onChange({ ...where, [field]: [] })
    setPicking(false)
    setPickSearch('')
  }

  const removeFilter = (field: string) => {
    const next = { ...where }
    Object.keys(next).forEach(k => { if (k === field || k.startsWith(`${field}__`)) delete next[k] })
    onChange(next)
  }

  const dropdown = picking ? createPortal(
    <div
      ref={dropRef}
      style={{ position: 'fixed', top: dropPos.top, left: dropPos.left, width: dropPos.width, zIndex: 9999, backgroundColor: 'hsl(var(--popover))', color: 'hsl(var(--popover-foreground))' }}
      className="rounded-md border shadow-xl p-1.5 text-[11px]"
    >
      <input
        autoFocus
        type="text"
        value={pickSearch}
        onChange={e => setPickSearch(e.target.value)}
        placeholder="Search columns…"
        style={{ backgroundColor: 'hsl(var(--background))' }}
        className="w-full h-6 text-[11px] rounded border px-2 mb-1 outline-none focus:ring-1 focus:ring-primary/40"
      />
      <div className="max-h-52 overflow-y-auto">
        {filteredPick.map(c => (
          <button key={c} onClick={() => pickField(c)}
            className="w-full text-left px-2 py-1 rounded hover:bg-primary/10 hover:text-primary transition-colors truncate">
            {c}
          </button>
        ))}
        {filteredPick.length === 0 && (
          <p className="text-[10px] text-muted-foreground px-2 py-1">
            {availableFields.length === 0 ? 'All fields already added' : 'No matching fields'}
          </p>
        )}
      </div>
    </div>,
    document.body
  ) : null

  return (
    <div className="space-y-2">
      {activeFields.map(field => (
        <ReportFieldFilter
          key={field}
          field={field}
          source={source || ''}
          datasourceId={datasourceId}
          widgetId={widgetId}
          dbType={columnMeta?.find(c => c.name === field)?.type}
          where={where as Record<string, any>}
          onWhereChange={w => onChange(w)}
          onRemove={() => removeFilter(field)}
        />
      ))}
      <button ref={addBtnRef} className="text-[11px] text-primary hover:underline flex items-center gap-1" onClick={openPicker}>
        <RiAddLine className="h-3 w-3" /> Add filter
      </button>
      {dropdown}
    </div>
  )
}

// ─── Element Property Panel ──────────────────────────────────────────
function ElementProps({
  element,
  variables,
  onUpdate,
}: {
  element: ReportElement
  variables: ReportVariable[]
  onUpdate: (el: ReportElement) => void
}) {
  if (element.type === 'label') {
    const lbl = element.label || { text: '' }
    const patch = (p: Partial<typeof lbl>) => onUpdate({ ...element, label: { ...lbl, ...p } })
    return (
      <div className="space-y-2">
        <label className="block text-[11px] text-muted-foreground">Text <span className="text-[10px] opacity-60">Use {'{{varName}}'} for variables</span></label>
        <textarea className="w-full h-16 text-xs rounded border bg-secondary/60 px-2 py-1 resize-none" value={lbl.text} onChange={(e) => patch({ text: e.target.value })} />
        {variables.length > 0 && (
          <div className="flex flex-wrap gap-1">
            <span className="text-[10px] text-muted-foreground">Insert:</span>
            {variables.map((v) => (
              <button
                key={v.id}
                className="text-[10px] px-1.5 py-0.5 rounded border border-primary/30 bg-primary/5 hover:bg-primary/15 text-primary"
                onClick={() => patch({ text: (lbl.text || '') + `{{${v.name}}}` })}
              >
                {`{{${v.name}}}`}
              </button>
            ))}
          </div>
        )}
        <div className="grid grid-cols-3 gap-2">
          <div>
            <label className="block text-[11px] text-muted-foreground mb-0.5">Size</label>
            <input type="number" className="w-full h-6 text-[11px] rounded border bg-secondary/60 px-1" value={lbl.fontSize || 14} onChange={(e) => patch({ fontSize: +e.target.value })} />
          </div>
          <div>
            <label className="block text-[11px] text-muted-foreground mb-0.5">Weight</label>
            <select className="w-full h-6 text-[11px] rounded border bg-secondary/60 px-1" value={lbl.fontWeight || 'normal'} onChange={(e) => patch({ fontWeight: e.target.value as any })}>
              <option value="normal">Normal</option>
              <option value="semibold">Semi</option>
              <option value="bold">Bold</option>
            </select>
          </div>
          <div>
            <label className="block text-[11px] text-muted-foreground mb-0.5">H. Align</label>
            <div className="flex rounded border overflow-hidden h-6">
              {(['left','center','right'] as const).map((a, i) => (
                <button key={a} title={a.charAt(0).toUpperCase()+a.slice(1)} onClick={() => patch({ align: a })}
                  className={`flex-1 flex items-center justify-center transition-colors ${ i > 0 ? 'border-l' : '' } ${ (lbl.align||'left') === a ? 'bg-primary text-primary-foreground' : 'bg-secondary/60 text-muted-foreground hover:bg-secondary' }`}>
                  {a === 'left' && <RiAlignLeft className="h-3 w-3" />}
                  {a === 'center' && <RiAlignCenter className="h-3 w-3" />}
                  {a === 'right' && <RiAlignRight className="h-3 w-3" />}
                </button>
              ))}
            </div>
          </div>
        </div>
        <div className="grid grid-cols-3 gap-2">
          <div>
            <label className="block text-[11px] text-muted-foreground mb-0.5">V. Align</label>
            <div className="flex rounded border overflow-hidden h-6">
              {(['top','middle','bottom'] as const).map((a, i) => (
                <button key={a} title={a.charAt(0).toUpperCase()+a.slice(1)} onClick={() => patch({ verticalAlign: a })}
                  className={`flex-1 flex items-center justify-center transition-colors ${ i > 0 ? 'border-l' : '' } ${ (lbl.verticalAlign||'middle') === a ? 'bg-primary text-primary-foreground' : 'bg-secondary/60 text-muted-foreground hover:bg-secondary' }`}>
                  {a === 'top' && <RiAlignTop className="h-3 w-3" />}
                  {a === 'middle' && <RiAlignVertically className="h-3 w-3" />}
                  {a === 'bottom' && <RiAlignBottom className="h-3 w-3" />}
                </button>
              ))}
            </div>
          </div>
          <div>
            <label className="block text-[11px] text-muted-foreground mb-0.5">Color</label>
            <input type="color" className="w-full h-6 rounded border cursor-pointer" value={lbl.color || '#000000'} onChange={(e) => patch({ color: e.target.value })} />
          </div>
          <div>
            <label className="block text-[11px] text-muted-foreground mb-0.5">Background</label>
            <input type="color" className="w-full h-6 rounded border cursor-pointer" value={lbl.backgroundColor || '#ffffff'} onChange={(e) => patch({ backgroundColor: e.target.value })} />
          </div>
        </div>
        <div className="grid grid-cols-2 gap-2">
          <div>
            <label className="block text-[11px] text-muted-foreground mb-0.5">Border</label>
            <select className="w-full h-6 text-[11px] rounded border bg-secondary/60 px-1" value={lbl.borderStyle || 'none'} onChange={(e) => patch({ borderStyle: e.target.value as any })}>
              <option value="none">None</option>
              <option value="solid">Solid</option>
              <option value="dashed">Dashed</option>
            </select>
          </div>
          <div>
            <label className="block text-[11px] text-muted-foreground mb-0.5">Padding</label>
            <input type="number" className="w-full h-6 text-[11px] rounded border bg-secondary/60 px-1" value={lbl.padding ?? 4} onChange={(e) => patch({ padding: +e.target.value })} />
          </div>
        </div>
      </div>
    )
  }

  if (element.type === 'image') {
    const img = element.image || { url: '', objectFit: 'contain' }
    const patch = (p: Partial<typeof img>) => onUpdate({ ...element, image: { ...img, ...p } })
    return (
      <div className="space-y-2">
        <div>
          <label className="block text-[11px] text-muted-foreground mb-1">Image URL</label>
          <input className="w-full h-7 text-xs rounded border bg-secondary/60 px-2" value={img.url} onChange={(e) => patch({ url: e.target.value })} placeholder="https://..." />
        </div>
        <div>
          <label className="block text-[11px] text-muted-foreground mb-1">Alt Text</label>
          <input className="w-full h-7 text-xs rounded border bg-secondary/60 px-2" value={img.alt || ''} onChange={(e) => patch({ alt: e.target.value })} placeholder="Image description" />
        </div>
        <div>
          <label className="block text-[11px] text-muted-foreground mb-1">Object Fit</label>
          <select className="w-full h-7 text-xs rounded border bg-secondary/60 px-2" value={img.objectFit || 'contain'} onChange={(e) => patch({ objectFit: e.target.value as any })}>
            <option value="contain">Contain</option>
            <option value="cover">Cover</option>
            <option value="fill">Fill</option>
            <option value="none">None</option>
          </select>
        </div>
      </div>
    )
  }

  if (element.type === 'spaceholder') {
    return (
      <div className="space-y-2">
        <label className="block text-[11px] text-muted-foreground">Variable</label>
        <select className="w-full h-7 text-xs rounded border bg-secondary/60 px-2" value={element.variableId || ''} onChange={(e) => onUpdate({ ...element, variableId: e.target.value })}>
          <option value="">Select variable…</option>
          {variables.map((v) => <option key={v.id} value={v.id}>{`{{${v.name}}}`}</option>)}
        </select>
      </div>
    )
  }

  if (element.type === 'table') {
    const tbl = element.table
    if (!tbl) return null
    const patchTbl = (p: Partial<typeof tbl>) => onUpdate({ ...element, table: { ...tbl, ...p } })
    const addRow = () => {
      // Calculate actual column count from header colspans
      const actualCols = tbl.headers.reduce((sum, h) => {
        const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
        return sum + (header.colspan || 1)
      }, 0)
      const newRow: ReportTableCell[] = Array.from({ length: actualCols }, () => ({ type: 'text' as const, text: '' }))
      patchTbl({ rows: tbl.rows + 1, cells: [...tbl.cells, newRow] })
    }
    const addCol = () => {
      const headers = [...tbl.headers, { text: `Col ${tbl.cols + 1}`, colspan: 1 }]
      const cells = tbl.cells.map((row) => [...row, { type: 'text' as const, text: '' } as ReportTableCell])
      
      // If subheaders exist, add one subheader per new actual column (considering colspans)
      let subheaders = tbl.subheaders
      if (subheaders) {
        const newTotalCols = headers.reduce((sum, h) => {
          const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
          return sum + (header.colspan || 1)
        }, 0)
        const oldTotalCols = subheaders.length
        subheaders = [...subheaders, ...Array(newTotalCols - oldTotalCols).fill('')]
      }
      
      patchTbl({ cols: tbl.cols + 1, headers, cells, ...(subheaders && { subheaders }) })
    }
    const removeRow = (ri: number) => {
      if (tbl.rows <= 1) return
      patchTbl({ rows: tbl.rows - 1, cells: tbl.cells.filter((_, i) => i !== ri) })
    }
    const removeCol = (ci: number) => {
      if (tbl.cols <= 1) return
      const newHeaders = tbl.headers.filter((_, i) => i !== ci)
      const cells = tbl.cells.map((row) => row.filter((_, i) => i !== ci))
      
      // If subheaders exist, recalculate total columns and resize
      let subheaders = tbl.subheaders
      if (subheaders) {
        const newTotalCols = newHeaders.reduce((sum, h) => {
          const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
          return sum + (header.colspan || 1)
        }, 0)
        // Remove subheaders from the end to match new total
        subheaders = subheaders.slice(0, newTotalCols)
      }
      
      patchTbl({
        cols: tbl.cols - 1,
        headers: newHeaders,
        cells,
        ...(subheaders && { subheaders }),
      })
    }
    const updateHeader = (ci: number, text: string) => {
      const headers = tbl.headers.map((h, i) => {
        const current = typeof h === 'string' ? { text: h, colspan: 1 } : h
        return i === ci ? { ...current, text } : current
      })
      patchTbl({ headers })
    }
    const updateHeaderColspan = (ci: number, colspan: number) => {
      const oldHeaders = tbl.headers
      const headers = oldHeaders.map((h, i) => {
        const current = typeof h === 'string' ? { text: h, colspan: 1 } : h
        return i === ci ? { ...current, colspan: Math.max(1, colspan) } : current
      })
      
      // Calculate old and new total columns
      const oldTotalCols = oldHeaders.reduce((sum, h) => {
        const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
        return sum + (header.colspan || 1)
      }, 0)
      const newTotalCols = headers.reduce((sum, h) => {
        const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
        return sum + (header.colspan || 1)
      }, 0)
      
      // Adjust cells to match new column count
      let cells = tbl.cells
      if (newTotalCols !== oldTotalCols) {
        cells = tbl.cells.map(row => {
          const newRow = [...row]
          if (newTotalCols > oldTotalCols) {
            // Add empty cells
            return [...newRow, ...Array(newTotalCols - oldTotalCols).fill({ type: 'text' as const, text: '' })]
          } else {
            // Remove excess cells
            return newRow.slice(0, newTotalCols)
          }
        })
      }
      
      // If subheaders exist, resize the array to match new total columns
      let subheaders = tbl.subheaders
      if (subheaders) {
        let newSubheaders = [...subheaders]
        if (newTotalCols > oldTotalCols) {
          newSubheaders = [...newSubheaders, ...Array(newTotalCols - oldTotalCols).fill('')]
        } else if (newTotalCols < oldTotalCols) {
          newSubheaders = newSubheaders.slice(0, newTotalCols)
        }
        subheaders = newSubheaders
      }
      
      patchTbl({ headers, cells, ...(subheaders && { subheaders }) })
    }
    const toggleSubheaders = () => {
      if (tbl.subheaders) {
        patchTbl({ subheaders: undefined })
      } else {
        // Calculate total actual columns based on header colspans
        const totalCols = tbl.headers.reduce((sum, h) => {
          const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
          return sum + (header.colspan || 1)
        }, 0)
        patchTbl({ subheaders: Array(totalCols).fill('') })
      }
    }
    const updateSubheader = (ci: number, text: string) => {
      if (!tbl.subheaders) return
      const subheaders = [...tbl.subheaders]
      subheaders[ci] = text
      patchTbl({ subheaders })
    }
    const updateCell = (ri: number, ci: number, patch: Partial<ReportTableCell>) => {
      const cells = tbl.cells.map((row, r) => row.map((cell, c) => (r === ri && c === ci) ? { ...cell, ...patch } : cell))
      patchTbl({ cells })
    }

    return (
      <div className="space-y-3">
        <div className="flex items-center gap-2">
          <button className="text-[11px] px-2 py-1 rounded border hover:bg-muted flex items-center gap-1" onClick={addRow}><RiAddLine className="h-3 w-3" />Row</button>
          <button className="text-[11px] px-2 py-1 rounded border hover:bg-muted flex items-center gap-1" onClick={addCol}><RiAddLine className="h-3 w-3" />Column</button>
          <span className="text-[11px] text-muted-foreground ml-auto">{tbl.rows}×{tbl.cols}</span>
        </div>

        {/* Table Style */}
        <details className="group/details" open>
          <summary className="text-[11px] font-medium text-muted-foreground cursor-pointer select-none flex items-center gap-1">▸ Table Style</summary>
          <div className="mt-1.5 space-y-1.5 pl-1">
            <div className="grid grid-cols-3 gap-1.5">
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">Border</label>
                <select className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.borderStyle || 'solid'} onChange={(e) => patchTbl({ borderStyle: e.target.value as any })}>
                  <option value="solid">Solid</option>
                  <option value="dashed">Dashed</option>
                  <option value="none">None</option>
                </select>
              </div>
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">Border Color</label>
                <input type="color" className="w-full h-5 rounded border cursor-pointer" value={tbl.borderColor || '#e5e7eb'} onChange={(e) => patchTbl({ borderColor: e.target.value })} />
              </div>
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">Radius</label>
                <input type="number" className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.borderRadius ?? 0} onChange={(e) => patchTbl({ borderRadius: Math.max(0, +e.target.value) })} min="0" />
              </div>
            </div>
            <label className="flex items-center gap-1.5 text-[10px] text-muted-foreground cursor-pointer">
              <input type="checkbox" checked={!!tbl.stripedRows} onChange={(e) => patchTbl({ stripedRows: e.target.checked })} /> Striped rows
            </label>
          </div>
        </details>

        {/* Header Style */}
        <details className="group/details">
          <summary className="text-[11px] font-medium text-muted-foreground cursor-pointer select-none flex items-center gap-1">▸ Header Style</summary>
          <div className="mt-1.5 space-y-1.5 pl-1">
            <div className="grid grid-cols-2 gap-1.5">
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">Background</label>
                <input type="color" className="w-full h-5 rounded border cursor-pointer" value={tbl.headerBg || '#f3f4f6'} onChange={(e) => patchTbl({ headerBg: e.target.value })} />
              </div>
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">Text Color</label>
                <input type="color" className="w-full h-5 rounded border cursor-pointer" value={tbl.headerColor || '#111827'} onChange={(e) => patchTbl({ headerColor: e.target.value })} />
              </div>
            </div>
            <div className="grid grid-cols-3 gap-1.5">
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">Size</label>
                <input type="number" className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.headerFontSize ?? 12} onChange={(e) => patchTbl({ headerFontSize: +e.target.value })} min="8" max="32" />
              </div>
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">Weight</label>
                <select className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.headerFontWeight || 'bold'} onChange={(e) => patchTbl({ headerFontWeight: e.target.value as any })}>
                  <option value="normal">Normal</option>
                  <option value="semibold">Semi</option>
                  <option value="bold">Bold</option>
                </select>
              </div>
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">H-Align</label>
                <select className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.headerAlign || 'left'} onChange={(e) => patchTbl({ headerAlign: e.target.value as any })}>
                  <option value="left">Left</option>
                  <option value="center">Center</option>
                  <option value="right">Right</option>
                </select>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-1.5">
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">V-Align</label>
                <select className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.headerVerticalAlign || 'middle'} onChange={(e) => patchTbl({ headerVerticalAlign: e.target.value as any })}>
                  <option value="top">Top</option>
                  <option value="middle">Middle</option>
                  <option value="bottom">Bottom</option>
                </select>
              </div>
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">Height (px)</label>
                <input type="number" className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.headerHeight ?? ''} onChange={(e) => patchTbl({ headerHeight: e.target.value ? +e.target.value : undefined })} placeholder="Auto" min="16" />
              </div>
            </div>
          </div>
        </details>

        {/* Subheader Style */}
        {tbl.subheaders && (
          <details className="group/details">
            <summary className="text-[11px] font-medium text-muted-foreground cursor-pointer select-none flex items-center gap-1">▸ Subheader Style</summary>
            <div className="mt-1.5 space-y-1.5 pl-1">
              <div className="grid grid-cols-2 gap-1.5">
                <div>
                  <label className="block text-[10px] text-muted-foreground mb-0.5">Background</label>
                  <input type="color" className="w-full h-5 rounded border cursor-pointer" value={tbl.subheaderBg || tbl.headerBg || '#f3f4f6'} onChange={(e) => patchTbl({ subheaderBg: e.target.value })} />
                </div>
                <div>
                  <label className="block text-[10px] text-muted-foreground mb-0.5">Text Color</label>
                  <input type="color" className="w-full h-5 rounded border cursor-pointer" value={tbl.subheaderColor || tbl.headerColor || '#111827'} onChange={(e) => patchTbl({ subheaderColor: e.target.value })} />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-1.5">
                <div>
                  <label className="block text-[10px] text-muted-foreground mb-0.5">Size</label>
                  <input type="number" className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.subheaderFontSize ?? 11} onChange={(e) => patchTbl({ subheaderFontSize: +e.target.value })} min="8" max="32" />
                </div>
                <div>
                  <label className="block text-[10px] text-muted-foreground mb-0.5">Weight</label>
                  <select className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.subheaderFontWeight || 'normal'} onChange={(e) => patchTbl({ subheaderFontWeight: e.target.value as any })}>
                    <option value="normal">Normal</option>
                    <option value="semibold">Semi</option>
                    <option value="bold">Bold</option>
                  </select>
                </div>
                <div>
                  <label className="block text-[10px] text-muted-foreground mb-0.5">H-Align</label>
                  <select className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.subheaderAlign || 'left'} onChange={(e) => patchTbl({ subheaderAlign: e.target.value as any })}>
                    <option value="left">Left</option>
                    <option value="center">Center</option>
                    <option value="right">Right</option>
                  </select>
                </div>
              </div>
              <div>
                <label className="block text-[10px] text-muted-foreground mb-0.5">V-Align</label>
                <select className="w-full h-5 text-[10px] rounded border bg-secondary/60 px-0.5" value={tbl.subheaderVerticalAlign || 'middle'} onChange={(e) => patchTbl({ subheaderVerticalAlign: e.target.value as any })}>
                  <option value="top">Top</option>
                  <option value="middle">Middle</option>
                  <option value="bottom">Bottom</option>
                </select>
              </div>
              <label className="flex items-center gap-1.5 text-[10px] text-muted-foreground cursor-pointer">
                <input type="checkbox" checked={!!tbl.mergeBlankSubheaders} onChange={(e) => patchTbl({ mergeBlankSubheaders: e.target.checked })} /> Merge headers with blank subheaders
              </label>
            </div>
          </details>
        )}

        {/* Headers */}
        <div>
          <div className="flex items-center justify-between mb-1">
            <label className="block text-[11px] text-muted-foreground">Headers</label>
            <button className="text-[10px] px-1.5 py-0.5 rounded border hover:bg-muted" onClick={toggleSubheaders}>
              {tbl.subheaders ? 'Remove' : 'Add'} Subheaders
            </button>
          </div>
          <div className="space-y-1">
            {tbl.headers.map((h, ci) => {
              const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
              return (
                <div key={ci} className="flex items-center gap-1">
                  <input 
                    className="h-6 text-[11px] rounded border bg-secondary/60 px-1 flex-1" 
                    value={header.text} 
                    onChange={(e) => updateHeader(ci, e.target.value)} 
                    placeholder="Header text"
                  />
                  <input 
                    type="number" 
                    className="h-6 text-[11px] rounded border bg-secondary/60 px-1 w-12" 
                    value={header.colspan || 1} 
                    onChange={(e) => updateHeaderColspan(ci, +e.target.value)}
                    min="1"
                    title="Colspan"
                  />
                  {tbl.cols > 1 && <button className="text-destructive hover:bg-destructive/10 rounded p-0.5" onClick={() => removeCol(ci)}><RiCloseLine className="h-3 w-3" /></button>}
                </div>
              )
            })}
          </div>
        </div>

        {/* Subheaders */}
        {tbl.subheaders && (
          <div>
            <label className="block text-[11px] text-muted-foreground mb-1">Subheaders</label>
            <div className="flex gap-1 flex-wrap">
              {tbl.subheaders.map((sh, ci) => (
                <input 
                  key={ci}
                  className="h-6 text-[11px] rounded border bg-secondary/60 px-1 w-20" 
                  value={sh} 
                  onChange={(e) => updateSubheader(ci, e.target.value)} 
                  placeholder={`Sub ${ci + 1}`}
                />
              ))}
            </div>
          </div>
        )}

        {/* Column Widths */}
        <details className="group/details">
          <summary className="text-[11px] font-medium text-muted-foreground cursor-pointer select-none flex items-center gap-1">▸ Column Widths (%)</summary>
          <div className="mt-1.5 pl-1">
            <div className="flex gap-1 flex-wrap">
              {(() => {
                const actualCols = tbl.headers.reduce((sum, h) => {
                  const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
                  return sum + (header.colspan || 1)
                }, 0)
                const widths = tbl.colWidths || Array(actualCols).fill(0)
                return Array.from({ length: actualCols }, (_, ci) => (
                  <div key={ci} className="flex flex-col items-center">
                    <span className="text-[9px] text-muted-foreground">{ci + 1}</span>
                    <input
                      type="number"
                      className="h-5 text-[10px] rounded border bg-secondary/60 px-0.5 w-12 text-center"
                      value={widths[ci] || ''}
                      onChange={(e) => {
                        const w = [...widths]
                        while (w.length < actualCols) w.push(0)
                        w[ci] = Math.max(0, +e.target.value)
                        patchTbl({ colWidths: w })
                      }}
                      placeholder="auto"
                      min="0"
                      max="100"
                    />
                  </div>
                ))
              })()}
            </div>
            <p className="text-[9px] text-muted-foreground mt-1">Set % width per column. Leave 0 or empty for auto.</p>
          </div>
        </details>

        {/* Cell editor */}
        <div>
          <label className="block text-[11px] text-muted-foreground mb-1">Cells</label>
          <div className="space-y-1 max-h-48 overflow-auto">
            {tbl.cells.map((row, ri) => {
              const rowStyle = tbl.rowStyles?.[ri]
              return (
                <div key={ri} className="flex items-center gap-1">
                  <span className="text-[10px] text-muted-foreground w-4 text-right">{ri + 1}</span>
                  <input
                    type="color"
                    className="w-4 h-4 rounded border cursor-pointer shrink-0"
                    title="Row background"
                    value={rowStyle?.bg || '#ffffff'}
                    onChange={(e) => {
                      const styles = [...(tbl.rowStyles || Array(tbl.rows).fill({}))]
                      while (styles.length <= ri) styles.push({})
                      styles[ri] = { ...styles[ri], bg: e.target.value === '#ffffff' ? undefined : e.target.value }
                      patchTbl({ rowStyles: styles })
                    }}
                  />
                  {row.map((cell, ci) => (
                    <div key={ci} className="flex-1 flex items-center gap-0.5 min-w-0">
                      <select className="h-5 text-[10px] rounded border bg-secondary/60 px-0.5 w-12 shrink-0" value={cell.type} onChange={(e) => updateCell(ri, ci, { type: e.target.value as any })}>
                        <option value="text">Text</option>
                        <option value="spaceholder">Var</option>
                      </select>
                      {cell.type === 'text' ? (
                        <input className="h-5 text-[10px] rounded border bg-secondary/60 px-1 flex-1 min-w-0" value={cell.text || ''} onChange={(e) => updateCell(ri, ci, { text: e.target.value })} />
                      ) : (
                        <select className="h-5 text-[10px] rounded border bg-secondary/60 px-0.5 flex-1 min-w-0" value={cell.variableId || ''} onChange={(e) => updateCell(ri, ci, { variableId: e.target.value })}>
                          <option value="">Select…</option>
                          {variables.map((v) => <option key={v.id} value={v.id}>{v.name}</option>)}
                        </select>
                      )}
                    </div>
                  ))}
                  {tbl.rows > 1 && <button className="text-destructive hover:bg-destructive/10 rounded p-0.5 shrink-0" onClick={() => removeRow(ri)}><RiCloseLine className="h-3 w-3" /></button>}
                </div>
              )
            })}
          </div>
        </div>
      </div>
    )
  }

  return null
}

// ─── Period presets shared by cell type and variable editor ──────────
const PERIOD_PRESETS: { value: string; label: string; group: string }[] = [
  { value: 'today',                         label: 'Today',                           group: 'Days' },
  { value: 'yesterday',                     label: 'Yesterday (YTDY)',                group: 'Days' },
  { value: 'last_working_day',              label: 'Last Working Day (LWDay)',        group: 'Days' },
  { value: 'day_before_last_working_day',   label: 'Day Before LWDay (DBLWDay)',      group: 'Days' },
  { value: 'this_week',                     label: 'This Week (TW)',                  group: 'Weeks' },
  { value: 'last_week',                     label: 'Last Week (LW)',                  group: 'Weeks' },
  { value: 'last_working_week',             label: 'Last Working Week (LWWeek)',      group: 'Weeks' },
  { value: 'week_before_last_working_week', label: 'Week Before LWWeek (WBLWWeek)',   group: 'Weeks' },
  { value: 'this_month',                    label: 'This Month (TMonth)',             group: 'Months' },
  { value: 'last_month',                    label: 'Last Month (LMonth)',             group: 'Months' },
  { value: 'this_year',                     label: 'This Year (TYear)',               group: 'Years' },
  { value: 'last_year',                     label: 'Last Year (LYear)',               group: 'Years' },
  { value: 'ytd',                           label: 'Year to Date (YTD)',              group: 'Cumulative' },
  { value: 'mtd',                           label: 'Month to Date (MTD)',             group: 'Cumulative' },
]
const PERIOD_LABEL: Record<string, string> = Object.fromEntries(PERIOD_PRESETS.map(p => [p.value, p.label]))
const PERIOD_GROUPS = [...new Set(PERIOD_PRESETS.map(p => p.group))]

// ─── Inline Table Editor ─────────────────────────────────────────────
function InlineTableEditor({
  table,
  variables,
  onChange,
}: {
  table: ReportElement['table']
  variables: ReportVariable[]
  onChange: (table: NonNullable<ReportElement['table']>) => void
}) {
  if (!table) return null

  const [cellMenuOpen, setCellMenuOpen] = useState<{ row: number; col: number } | null>(null)
  const [deleteConfirmRow, setDeleteConfirmRow] = useState<number | null>(null)
  const [normalized, setNormalized] = useState(false)

  // Close cell menu on outside click
  useEffect(() => {
    if (!cellMenuOpen) return
    const handler = () => setCellMenuOpen(null)
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [cellMenuOpen])

  // Normalize cell structure on mount if needed
  useEffect(() => {
    if (normalized) return
    
    const actualCols = table.headers.reduce((sum, h) => {
      const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
      return sum + (header.colspan || 1)
    }, 0)
    
    // Check if any row has wrong cell count
    const needsNormalization = table.cells.some(row => row.length !== actualCols)
    
    if (needsNormalization) {
      const normalizedCells = table.cells.map(row => {
        if (row.length === actualCols) return row
        if (row.length > actualCols) return row.slice(0, actualCols)
        return [...row, ...Array(actualCols - row.length).fill({ type: 'text' as const, text: '' })]
      })
      onChange({ ...table, cells: normalizedCells })
    }
    
    setNormalized(true)
  }, [table, normalized, onChange])

  const updateHeader = (ci: number, text: string) => {
    const headers = table.headers.map((h, i) => {
      const current = typeof h === 'string' ? { text: h, colspan: 1 } : h
      return i === ci ? { ...current, text } : current
    })
    onChange({ ...table, headers })
  }

  const updateHeaderColspan = (ci: number, colspan: number) => {
    const headers = table.headers.map((h, i) => {
      const current = typeof h === 'string' ? { text: h, colspan: 1 } : h
      return i === ci ? { ...current, colspan: Math.max(1, colspan) } : current
    })
    onChange({ ...table, headers })
  }

  const updateSubheader = (ci: number, text: string) => {
    if (!table.subheaders) return
    const subheaders = [...table.subheaders]
    subheaders[ci] = text
    onChange({ ...table, subheaders })
  }

  const updateCell = (ri: number, ci: number, patch: Partial<ReportTableCell>) => {
    const cells = table.cells.map((row, r) => row.map((cell, c) => (r === ri && c === ci) ? { ...cell, ...patch } : cell))
    onChange({ ...table, cells })
  }

  const deleteRow = (ri: number) => {
    if (table.rows <= 1) return
    const newCells = table.cells.filter((_, i) => i !== ri)
    const newRowStyles = table.rowStyles ? table.rowStyles.filter((_, i) => i !== ri) : undefined
    onChange({ ...table, rows: table.rows - 1, cells: newCells, ...(newRowStyles ? { rowStyles: newRowStyles } : {}) })
    setCellMenuOpen(null)
    setDeleteConfirmRow(null)
  }

  const insertRowAbove = (ri: number) => {
    const actualCols = table.headers.reduce((sum, h) => {
      const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
      return sum + (header.colspan || 1)
    }, 0)
    const newRow: ReportTableCell[] = Array.from({ length: actualCols }, () => ({ type: 'text' as const, text: '' }))
    const newCells = [...table.cells.slice(0, ri), newRow, ...table.cells.slice(ri)]
    const newRowStyles = table.rowStyles ? [...table.rowStyles.slice(0, ri), {}, ...table.rowStyles.slice(ri)] : undefined
    onChange({ ...table, rows: table.rows + 1, cells: newCells, ...(newRowStyles ? { rowStyles: newRowStyles } : {}) })
    setCellMenuOpen(null)
  }

  return (
    <table className="w-full border-collapse text-[11px]" style={{ borderWidth: '1px', borderStyle: table.borderStyle || 'solid', borderColor: table.borderColor || 'hsl(var(--border))' }}>
      <thead>
        <tr>
          {table.headers.map((h, ci) => {
            const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
            return (
              <th
                key={ci}
                colSpan={header.colspan || 1}
                className="px-1 py-0.5"
                style={{
                  backgroundColor: table.headerBg || 'hsl(var(--secondary))',
                  borderWidth: '1px',
                  borderStyle: table.borderStyle || 'solid',
                  borderColor: table.borderColor || 'hsl(var(--border))',
                  color: table.headerColor || undefined,
                  fontSize: table.headerFontSize ? `${table.headerFontSize}px` : undefined,
                  fontWeight: table.headerFontWeight === 'bold' ? 700 : table.headerFontWeight === 'semibold' ? 600 : table.headerFontWeight === 'normal' ? 400 : undefined,
                  textAlign: (table.headerAlign as any) || undefined,
                  verticalAlign: (table.headerVerticalAlign as any) || undefined,
                  height: table.headerHeight ? `${table.headerHeight}px` : undefined,
                }}
              >
                <div className="flex items-center gap-1">
                  <input
                    className="flex-1 bg-transparent border-none outline-none font-[inherit] text-[inherit]"
                    style={{ textAlign: (table.headerAlign as any) || 'left' }}
                    value={header.text}
                    onChange={(e) => updateHeader(ci, e.target.value)}
                    placeholder={`Header ${ci + 1}`}
                    onClick={(e) => e.stopPropagation()}
                  />
                  {(header.colspan || 1) > 1 && (
                    <span className="text-[8px] text-muted-foreground bg-primary/10 px-1 rounded" title={`Spans ${header.colspan} columns`}>
                      ×{header.colspan}
                    </span>
                  )}
                  <input
                    type="number"
                    min="1"
                    max={table.cols}
                    className="w-10 bg-transparent border border-border/40 rounded text-[8px] px-1 py-0.5"
                    value={header.colspan || 1}
                    onChange={(e) => updateHeaderColspan(ci, +e.target.value)}
                    onClick={(e) => e.stopPropagation()}
                    title="Column span"
                  />
                </div>
              </th>
            )
          })}
        </tr>
        {table.subheaders && table.subheaders.length > 0 && (
          <tr>
            {table.subheaders.map((sh, actualCol) => (
              <th
                key={actualCol}
                className="px-1 py-0.5"
                style={{
                  backgroundColor: table.subheaderBg || table.headerBg || 'hsl(var(--secondary))',
                  borderWidth: '1px',
                  borderStyle: table.borderStyle || 'solid',
                  borderColor: table.borderColor || 'hsl(var(--border))',
                  color: table.subheaderColor || table.headerColor || undefined,
                  fontSize: table.subheaderFontSize ? `${table.subheaderFontSize}px` : undefined,
                  fontWeight: table.subheaderFontWeight === 'bold' ? 700 : table.subheaderFontWeight === 'semibold' ? 600 : table.subheaderFontWeight === 'normal' ? 400 : undefined,
                  textAlign: (table.subheaderAlign as any) || undefined,
                  verticalAlign: (table.subheaderVerticalAlign as any) || undefined,
                }}
              >
                <input
                  className="w-full bg-transparent border-none outline-none font-[inherit] text-[inherit]"
                  style={{ textAlign: (table.subheaderAlign as any) || 'left', fontSize: 'inherit' }}
                  value={sh}
                  onChange={(e) => updateSubheader(actualCol, e.target.value)}
                  placeholder={`Col ${actualCol + 1}`}
                  onClick={(e) => e.stopPropagation()}
                />
              </th>
            ))}
          </tr>
        )}
      </thead>
      <tbody>
        {table.cells.map((row, ri) => (
          <tr key={ri}>
            {row.map((cell, ci) => {
              const cs = cell.style
              const isMenuOpen = cellMenuOpen?.row === ri && cellMenuOpen?.col === ci
              return (
              <td
                key={ci}
                className="px-1 py-0.5"
                style={{
                  borderWidth: '1px',
                  borderStyle: table.borderStyle || 'solid',
                  borderColor: table.borderColor || 'hsl(var(--border))',
                  backgroundColor: cs?.backgroundColor || table.rowStyles?.[ri]?.bg || (table.stripedRows && ri % 2 === 1 ? 'hsl(var(--secondary)/0.4)' : undefined),
                  fontSize: cs?.fontSize ? `${cs.fontSize}px` : undefined,
                  fontWeight: cs?.fontWeight === 'bold' ? 700 : cs?.fontWeight === 'semibold' ? 600 : undefined,
                  fontStyle: cs?.fontStyle === 'italic' ? 'italic' : undefined,
                  color: cs?.color || undefined,
                  textAlign: cs?.align || undefined,
                  verticalAlign: cs?.verticalAlign || undefined,
                }}
              >
                <div className="flex items-center gap-0.5 relative">
                  {cell.type === 'text' ? (
                    <input
                      className="flex-1 bg-transparent border-none outline-none text-[10px] min-w-0"
                      style={{ textAlign: cs?.align || 'left' }}
                      value={cell.text || ''}
                      onChange={(e) => updateCell(ri, ci, { text: e.target.value })}
                      placeholder="..."
                      onClick={(e) => e.stopPropagation()}
                    />
                  ) : cell.type === 'period' ? (
                    <span className="flex-1 text-[9px] font-mono text-primary/80 truncate select-none" title={PERIOD_LABEL[cell.datetimeExpr || ''] || cell.datetimeExpr || 'Period'}>
                      {PERIOD_LABEL[cell.datetimeExpr || ''] || cell.datetimeExpr || <span className="text-muted-foreground italic">No period</span>}
                    </span>
                  ) : (
                    <select
                      className="flex-1 bg-transparent border-none outline-none text-[9px] font-mono min-w-0"
                      value={cell.variableId || ''}
                      onChange={(e) => updateCell(ri, ci, { variableId: e.target.value })}
                      onClick={(e) => e.stopPropagation()}
                    >
                      <option value="">Select variable...</option>
                      {variables.map((v) => <option key={v.id} value={v.id}>{v.name}</option>)}
                    </select>
                  )}
                  <button
                    className={`text-[9px] px-0.5 rounded shrink-0 transition-colors ${isMenuOpen ? 'bg-primary/10 text-primary' : 'hover:bg-muted text-muted-foreground'}`}
                    onMouseDown={(e) => e.stopPropagation()}
                    onClick={(e) => { e.stopPropagation(); setCellMenuOpen(isMenuOpen ? null : { row: ri, col: ci }) }}
                    title="Cell options"
                  >
                    ⋮
                  </button>
                  {isMenuOpen && (
                    <div className="absolute z-50 right-0 top-full mt-1 border rounded-lg shadow-lg p-2 min-w-[200px] space-y-2" style={{ backgroundColor: 'hsl(var(--card))' }} onMouseDown={(e) => e.stopPropagation()} onClick={(e) => e.stopPropagation()}>
                      {/* Type toggle */}
                      <div className="flex gap-1">
                        <button className={`flex-1 text-[10px] py-1 rounded-md transition-colors ${cell.type === 'text' ? 'bg-primary text-primary-foreground' : 'border hover:bg-muted'}`}
                          onClick={() => { updateCell(ri, ci, { type: 'text', text: cell.type === 'spaceholder' ? '' : cell.text, variableId: undefined, datetimeExpr: undefined }); }}>
                          Text
                        </button>
                        <button className={`flex-1 text-[10px] py-1 rounded-md transition-colors ${cell.type === 'spaceholder' ? 'bg-primary text-primary-foreground' : 'border hover:bg-muted'}`}
                          onClick={() => { updateCell(ri, ci, { type: 'spaceholder', text: undefined, variableId: cell.variableId || '', datetimeExpr: undefined }); }}>
                          Variable
                        </button>
                        <button className={`flex-1 text-[10px] py-1 rounded-md transition-colors ${cell.type === 'period' ? 'bg-primary text-primary-foreground' : 'border hover:bg-muted'}`}
                          onClick={() => { updateCell(ri, ci, { type: 'period', text: undefined, variableId: undefined, datetimeExpr: cell.datetimeExpr || 'last_week' }); }}>
                          Period
                        </button>
                      </div>

                      {/* Period selector — shown when type is period */}
                      {cell.type === 'period' && (
                        <div>
                          <label className="block text-[9px] font-semibold text-muted-foreground uppercase tracking-wider mb-1">Period</label>
                          <select
                            className="w-full h-6 text-[10px] rounded border bg-secondary/40 px-1 cursor-pointer focus:ring-1 focus:ring-primary/40 outline-none"
                            value={cell.datetimeExpr || 'last_week'}
                            onChange={(e) => updateCell(ri, ci, { datetimeExpr: e.target.value })}
                          >
                            {PERIOD_GROUPS.map(group => (
                              <optgroup key={group} label={group}>
                                {PERIOD_PRESETS.filter(p => p.group === group).map(p => (
                                  <option key={p.value} value={p.value}>{p.label}</option>
                                ))}
                              </optgroup>
                            ))}
                          </select>
                        </div>
                      )}

                      <div className="border-t pt-2">
                        <div className="text-[9px] font-semibold text-muted-foreground uppercase tracking-wider mb-1.5">Formatting</div>
                        {/* Font size + weight + italic */}
                        <div className="grid grid-cols-3 gap-1 mb-1.5">
                          <div>
                            <label className="block text-[8px] text-muted-foreground mb-0.5">Size</label>
                            <input type="number" min="6" max="32" className="w-full h-5 text-[9px] rounded border bg-secondary/40 px-1 focus:ring-1 focus:ring-primary/40 outline-none"
                              value={cs?.fontSize || ''} placeholder="—"
                              onChange={(e) => updateCell(ri, ci, { style: { ...cs, fontSize: e.target.value ? +e.target.value : undefined } })}
                            />
                          </div>
                          <div>
                            <label className="block text-[8px] text-muted-foreground mb-0.5">Weight</label>
                            <select className="w-full h-5 text-[9px] rounded border bg-secondary/40 px-0.5 cursor-pointer focus:ring-1 focus:ring-primary/40 outline-none"
                              value={cs?.fontWeight || 'normal'}
                              onChange={(e) => updateCell(ri, ci, { style: { ...cs, fontWeight: e.target.value as any } })}>
                              <option value="normal">Normal</option>
                              <option value="semibold">Semi</option>
                              <option value="bold">Bold</option>
                            </select>
                          </div>
                          <div>
                            <label className="block text-[8px] text-muted-foreground mb-0.5">Style</label>
                            <select className="w-full h-5 text-[9px] rounded border bg-secondary/40 px-0.5 cursor-pointer focus:ring-1 focus:ring-primary/40 outline-none"
                              value={cs?.fontStyle || 'normal'}
                              onChange={(e) => updateCell(ri, ci, { style: { ...cs, fontStyle: e.target.value as any } })}>
                              <option value="normal">Normal</option>
                              <option value="italic">Italic</option>
                            </select>
                          </div>
                        </div>

                        {/* Colors */}
                        <div className="grid grid-cols-2 gap-1 mb-1.5">
                          <div>
                            <label className="block text-[8px] text-muted-foreground mb-0.5">Text Color</label>
                            <div className="flex items-center gap-1">
                              <input type="color" className="w-5 h-5 rounded border cursor-pointer shrink-0" value={cs?.color || '#000000'}
                                onChange={(e) => updateCell(ri, ci, { style: { ...cs, color: e.target.value } })} />
                              {cs?.color && <button className="text-[8px] text-muted-foreground hover:text-foreground" onClick={() => updateCell(ri, ci, { style: { ...cs, color: undefined } })}>clear</button>}
                            </div>
                          </div>
                          <div>
                            <label className="block text-[8px] text-muted-foreground mb-0.5">Background</label>
                            <div className="flex items-center gap-1">
                              <input type="color" className="w-5 h-5 rounded border cursor-pointer shrink-0" value={cs?.backgroundColor || '#ffffff'}
                                onChange={(e) => updateCell(ri, ci, { style: { ...cs, backgroundColor: e.target.value === '#ffffff' ? undefined : e.target.value } })} />
                              {cs?.backgroundColor && <button className="text-[8px] text-muted-foreground hover:text-foreground" onClick={() => updateCell(ri, ci, { style: { ...cs, backgroundColor: undefined } })}>clear</button>}
                            </div>
                          </div>
                        </div>

                        {/* Alignment */}
                        <div className="grid grid-cols-2 gap-1 mb-1.5">
                          <div>
                            <label className="block text-[8px] text-muted-foreground mb-0.5">H-Align</label>
                            <div className="flex gap-0.5">
                              {(['left', 'center', 'right'] as const).map(a => (
                                <button key={a} className={`flex-1 text-[9px] py-0.5 rounded border transition-colors ${cs?.align === a ? 'bg-primary text-primary-foreground border-primary' : 'hover:bg-muted'}`}
                                  onClick={() => updateCell(ri, ci, { style: { ...cs, align: a } })}>
                                  {a === 'left' ? '⇤' : a === 'center' ? '⇔' : '⇥'}
                                </button>
                              ))}
                            </div>
                          </div>
                          <div>
                            <label className="block text-[8px] text-muted-foreground mb-0.5">V-Align</label>
                            <div className="flex gap-0.5">
                              {(['top', 'middle', 'bottom'] as const).map(a => (
                                <button key={a} className={`flex-1 text-[9px] py-0.5 rounded border transition-colors ${cs?.verticalAlign === a ? 'bg-primary text-primary-foreground border-primary' : 'hover:bg-muted'}`}
                                  onClick={() => updateCell(ri, ci, { style: { ...cs, verticalAlign: a } })}>
                                  {a === 'top' ? '⬆' : a === 'middle' ? '⬌' : '⬇'}
                                </button>
                              ))}
                            </div>
                          </div>
                        </div>

                        {/* Number format */}
                        <div>
                          <label className="block text-[8px] text-muted-foreground mb-0.5">Number Format</label>
                          <select className="w-full h-5 text-[9px] rounded border bg-secondary/40 px-1 cursor-pointer focus:ring-1 focus:ring-primary/40 outline-none"
                            value={cs?.numberFormat || 'none'}
                            onChange={(e) => updateCell(ri, ci, { style: { ...cs, numberFormat: e.target.value as any } })}>
                            <option value="none">None</option>
                            <option value="wholeNumber">Whole Number</option>
                            <option value="oneDecimal">1 Decimal</option>
                            <option value="twoDecimals">2 Decimals</option>
                            <option value="currency">Currency</option>
                            <option value="percent">Percent</option>
                            <option value="short">Short (K/M)</option>
                          </select>
                        </div>
                      </div>

                      {/* Reset */}
                      {cs && Object.keys(cs).some(k => (cs as any)[k] !== undefined) && (
                        <button className="w-full text-[9px] text-muted-foreground hover:text-foreground border rounded py-0.5 hover:bg-muted transition-colors mt-1"
                          onClick={() => updateCell(ri, ci, { style: undefined })}>
                          Reset formatting
                        </button>
                      )}

                      {/* Row actions */}
                      <div className="border-t pt-2 mt-1 space-y-1">
                        <button className="w-full text-[9px] text-left px-2 py-1 rounded hover:bg-muted transition-colors flex items-center gap-1.5"
                          onClick={() => { insertRowAbove(ri); setDeleteConfirmRow(null) }}>
                          <RiAddLine className="h-3 w-3 shrink-0" />Insert row above
                        </button>
                        {deleteConfirmRow === ri ? (
                          <div className="flex items-center gap-1">
                            <span className="text-[9px] text-destructive flex-1">Delete this row?</span>
                            <button className="text-[9px] px-1.5 py-0.5 rounded bg-destructive text-destructive-foreground hover:opacity-90 transition-opacity"
                              onClick={() => deleteRow(ri)}>Yes</button>
                            <button className="text-[9px] px-1.5 py-0.5 rounded border hover:bg-muted transition-colors"
                              onClick={() => setDeleteConfirmRow(null)}>No</button>
                          </div>
                        ) : (
                          <button
                            className={`w-full text-[9px] text-left px-2 py-1 rounded transition-colors flex items-center gap-1.5 ${
                              table.rows <= 1 ? 'opacity-40 cursor-not-allowed text-muted-foreground' : 'hover:bg-destructive/10 text-destructive'
                            }`}
                            disabled={table.rows <= 1}
                            onClick={() => setDeleteConfirmRow(ri)}>
                            <RiDeleteBinLine className="h-3 w-3 shrink-0" />Delete row
                          </button>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              </td>
              )
            })}
          </tr>
        ))}
      </tbody>
    </table>
  )
}

// ─── Grid Canvas ─────────────────────────────────────────────────────
function GridCanvas({
  state,
  selectedId,
  onSelectElement,
  onMoveElement,
  onMoveElements,
  onResizeElement,
  onUpdateElement,
}: {
  state: ReportState
  selectedId: string | null
  onSelectElement: (id: string | null) => void
  onMoveElement: (id: string, gridX: number, gridY: number) => void
  onMoveElements: (moves: { id: string; gridX: number; gridY: number }[]) => void
  onResizeElement: (id: string, gridW: number, gridH: number) => void
  onUpdateElement: (id: string, el: ReportElement) => void
}) {
  const { gridCols, gridRows, cellSize, elements, variables, showGridLines } = state
  const canvasRef = useRef<HTMLDivElement>(null)
  const [dragState, setDragState] = useState<{ id: string; startX: number; startY: number; origGX: number; origGY: number } | null>(null)
  const [resizeState, setResizeState] = useState<{ id: string; startX: number; startY: number; origW: number; origH: number } | null>(null)
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => selectedId ? new Set([selectedId]) : new Set())

  const snap = (px: number) => Math.round(px / cellSize)

  // Sync selectedIds when selectedId prop changes externally (e.g., panel list click)
  useEffect(() => {
    if (selectedId && !selectedIds.has(selectedId)) {
      setSelectedIds(new Set([selectedId]))
    } else if (!selectedId && selectedIds.size > 0) {
      setSelectedIds(new Set())
    }
  }, [selectedId]) // eslint-disable-line react-hooks/exhaustive-deps

  // Drag handling
  useEffect(() => {
    if (!dragState) return
    const onMove = (e: MouseEvent) => {
      const dx = e.clientX - dragState.startX
      const dy = e.clientY - dragState.startY
      const gx = Math.max(0, Math.min(gridCols - 1, dragState.origGX + snap(dx)))
      const gy = Math.max(0, Math.min(gridRows - 1, dragState.origGY + snap(dy)))
      onMoveElement(dragState.id, gx, gy)
    }
    const onUp = () => setDragState(null)
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
    return () => { window.removeEventListener('mousemove', onMove); window.removeEventListener('mouseup', onUp) }
  }, [dragState, cellSize, gridCols, gridRows, onMoveElement])

  // Resize handling
  useEffect(() => {
    if (!resizeState) return
    const onMove = (e: MouseEvent) => {
      const dx = e.clientX - resizeState.startX
      const dy = e.clientY - resizeState.startY
      const w = Math.max(1, resizeState.origW + snap(dx))
      const h = Math.max(1, resizeState.origH + snap(dy))
      onResizeElement(resizeState.id, w, h)
    }
    const onUp = () => setResizeState(null)
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
    return () => { window.removeEventListener('mousemove', onMove); window.removeEventListener('mouseup', onUp) }
  }, [resizeState, cellSize, onResizeElement])

  // Keyboard arrow keys to move selected element(s)
  useEffect(() => {
    if (selectedIds.size === 0) return
    const onKeyDown = (e: KeyboardEvent) => {
      // Skip if user is typing in an input
      if ((e.target as HTMLElement)?.tagName === 'INPUT' || (e.target as HTMLElement)?.tagName === 'SELECT' || (e.target as HTMLElement)?.tagName === 'TEXTAREA') return
      let dx = 0, dy = 0
      if (e.key === 'ArrowLeft') dx = -1
      else if (e.key === 'ArrowRight') dx = 1
      else if (e.key === 'ArrowUp') dy = -1
      else if (e.key === 'ArrowDown') dy = 1
      else return
      e.preventDefault()
      const moves = elements
        .filter(el => selectedIds.has(el.id))
        .map(el => ({
          id: el.id,
          gridX: Math.max(0, Math.min(gridCols - el.gridW, el.gridX + dx)),
          gridY: Math.max(0, Math.min(gridRows - el.gridH, el.gridY + dy)),
        }))
      onMoveElements(moves)
    }
    window.addEventListener('keydown', onKeyDown)
    return () => window.removeEventListener('keydown', onKeyDown)
  }, [selectedIds, elements, gridCols, gridRows, onMoveElements])

  return (
    <div className="overflow-auto flex-1 bg-[hsl(var(--secondary)/0.2)] rounded-lg p-3 border border-border/40">
      <div
        ref={canvasRef}
        className="relative mx-auto"
        tabIndex={0}
        style={{
          width: `${gridCols * cellSize}px`,
          height: `${gridRows * cellSize}px`,
          backgroundSize: `${cellSize}px ${cellSize}px`,
          backgroundImage: showGridLines
            ? `linear-gradient(to right, hsl(var(--border)/0.3) 1px, transparent 1px), linear-gradient(to bottom, hsl(var(--border)/0.3) 1px, transparent 1px)`
            : undefined,
        }}
        onClick={(e) => { if (e.target === canvasRef.current) { onSelectElement(null); setSelectedIds(new Set()) } }}
      >
        {elements.map((el) => {
          const isSelected = selectedIds.has(el.id)
          const isMultiSelected = isSelected && selectedIds.size > 1
          return (
            <div
              key={el.id}
              className={`absolute group cursor-pointer ${
                isMultiSelected ? 'ring-2 ring-primary/60 ring-offset-1 ring-dashed'
                : isSelected ? 'ring-2 ring-primary ring-offset-1'
                : 'hover:ring-1 hover:ring-primary/40'
              }`}
              style={{
                left: `${el.gridX * cellSize}px`,
                top: `${el.gridY * cellSize}px`,
                width: `${el.gridW * cellSize}px`,
                height: `${el.gridH * cellSize}px`,
                zIndex: isSelected ? 10 : 1,
              }}
              onClick={(e) => {
                e.stopPropagation()
                if (e.metaKey || e.ctrlKey) {
                  setSelectedIds(prev => { const next = new Set(prev); next.has(el.id) ? next.delete(el.id) : next.add(el.id); return next })
                } else {
                  setSelectedIds(new Set([el.id]))
                }
                onSelectElement(el.id)
              }}
            >
              {/* Element preview */}
              <div className="h-full w-full overflow-hidden rounded-sm bg-card/80 border border-border/60">
                {el.type === 'label' && (
                  <div className="h-full flex px-1" style={{
                    backgroundColor: el.label?.backgroundColor || undefined,
                    alignItems: el.label?.verticalAlign === 'top' ? 'flex-start' : el.label?.verticalAlign === 'bottom' ? 'flex-end' : 'center',
                  }}>
                    <span className="truncate w-full" style={{
                      fontSize: el.label?.fontSize ? `${el.label.fontSize}px` : '11px',
                      fontWeight: el.label?.fontWeight === 'bold' ? 700 : el.label?.fontWeight === 'semibold' ? 600 : 400,
                      color: el.label?.color || undefined,
                      textAlign: (el.label?.align as any) || 'left',
                    }}>{el.label?.text || 'Label'}</span>
                  </div>
                )}
                {el.type === 'image' && (
                  <div className="h-full flex items-center justify-center text-[11px] text-muted-foreground">
                    {el.image?.url ? (
                      <img src={el.image.url} alt="" className="w-full h-full object-contain" />
                    ) : (
                      <><RiImageLine className="h-4 w-4 mr-1" />Image</>
                    )}
                  </div>
                )}
                {el.type === 'spaceholder' && (
                  <div className="h-full flex items-center justify-center text-[11px] text-primary/70 font-mono">
                    {`{{${variables.find(v => v.id === el.variableId)?.name || '?'}}}`}
                  </div>
                )}
                {el.type === 'table' && el.table && !isSelected && (
                  <div className="h-full w-full overflow-auto p-1">
                    <table className="w-full border-collapse text-[11px]" style={{ borderWidth: '1px', borderStyle: el.table.borderStyle || 'solid', borderColor: el.table.borderColor || 'hsl(var(--border))' }}>
                      <thead>
                        <tr>
                          {el.table.headers.map((h, ci) => {
                            const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
                            return (
                              <th key={ci} colSpan={header.colspan || 1} className="px-1 py-0.5" style={{
                                backgroundColor: el.table!.headerBg || 'hsl(var(--secondary))',
                                borderWidth: '1px', borderStyle: el.table!.borderStyle || 'solid', borderColor: el.table!.borderColor || 'hsl(var(--border))',
                                color: el.table!.headerColor || undefined,
                                fontSize: el.table!.headerFontSize ? `${el.table!.headerFontSize}px` : undefined,
                                fontWeight: el.table!.headerFontWeight === 'bold' ? 700 : el.table!.headerFontWeight === 'semibold' ? 600 : el.table!.headerFontWeight === 'normal' ? 400 : undefined,
                                textAlign: (el.table!.headerAlign as any) || 'left',
                                verticalAlign: (el.table!.headerVerticalAlign as any) || undefined,
                                height: el.table!.headerHeight ? `${el.table!.headerHeight}px` : undefined,
                              }}>{header.text || `Col ${ci+1}`}</th>
                            )
                          })}
                        </tr>
                        {el.table.subheaders && el.table.subheaders.length > 0 && (
                          <tr>
                            {el.table.subheaders.map((sh, ci) => (
                              <th key={ci} className="px-1 py-0.5" style={{
                                backgroundColor: el.table!.subheaderBg || el.table!.headerBg || 'hsl(var(--secondary))',
                                borderWidth: '1px', borderStyle: el.table!.borderStyle || 'solid', borderColor: el.table!.borderColor || 'hsl(var(--border))',
                                color: el.table!.subheaderColor || el.table!.headerColor || undefined,
                                fontSize: el.table!.subheaderFontSize ? `${el.table!.subheaderFontSize}px` : undefined,
                                fontWeight: el.table!.subheaderFontWeight === 'bold' ? 700 : el.table!.subheaderFontWeight === 'semibold' ? 600 : el.table!.subheaderFontWeight === 'normal' ? 400 : undefined,
                                textAlign: (el.table!.subheaderAlign as any) || 'left',
                                verticalAlign: (el.table!.subheaderVerticalAlign as any) || undefined,
                              }}>{sh || ''}</th>
                            ))}
                          </tr>
                        )}
                      </thead>
                      <tbody>
                        {el.table.cells.map((row, ri) => (
                          <tr key={ri}>
                            {row.map((cell, ci) => {
                              const cs = cell.style
                              return (
                              <td key={ci} className="px-1 py-0.5 text-[10px]" style={{
                                borderWidth: '1px', borderStyle: el.table!.borderStyle || 'solid', borderColor: el.table!.borderColor || 'hsl(var(--border))',
                                backgroundColor: cs?.backgroundColor || el.table!.rowStyles?.[ri]?.bg || (el.table!.stripedRows && ri % 2 === 1 ? 'hsl(var(--secondary)/0.4)' : undefined),
                                fontSize: cs?.fontSize ? `${cs.fontSize}px` : undefined,
                                fontWeight: cs?.fontWeight === 'bold' ? 700 : cs?.fontWeight === 'semibold' ? 600 : undefined,
                                fontStyle: cs?.fontStyle === 'italic' ? 'italic' : undefined,
                                color: cs?.color || undefined,
                                textAlign: cs?.align || undefined,
                                verticalAlign: cs?.verticalAlign || undefined,
                              }}>
                                {cell.type === 'text' ? (cell.text || '') : (
                                  <span className="text-primary/60 font-mono text-[9px]">{`{{${variables.find(v => v.id === cell.variableId)?.name || '?'}}}`}</span>
                                )}
                              </td>
                              )
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
                {el.type === 'table' && el.table && isSelected && (
                  <div className="h-full w-full overflow-auto p-1" onClick={(e) => e.stopPropagation()}>
                    <InlineTableEditor
                      table={el.table}
                      variables={state.variables}
                      onChange={(tbl) => {
                        const updated = { ...el, table: tbl }
                        onUpdateElement(el.id, updated)
                      }}
                    />
                  </div>
                )}
              </div>

              {/* Drag handle */}
              <div
                className="absolute top-0 left-0 w-5 h-5 flex items-center justify-center bg-primary/80 text-primary-foreground rounded-br cursor-move opacity-0 group-hover:opacity-100 transition-opacity"
                onMouseDown={(e) => {
                  e.preventDefault(); e.stopPropagation()
                  setDragState({ id: el.id, startX: e.clientX, startY: e.clientY, origGX: el.gridX, origGY: el.gridY })
                }}
              >
                <RiDragMoveLine className="h-3 w-3" />
              </div>

              {/* Resize handle */}
              <div
                className="absolute bottom-0 right-0 w-3 h-3 bg-primary/60 cursor-se-resize rounded-tl opacity-0 group-hover:opacity-100 transition-opacity"
                onMouseDown={(e) => {
                  e.preventDefault(); e.stopPropagation()
                  setResizeState({ id: el.id, startX: e.clientX, startY: e.clientY, origW: el.gridW, origH: el.gridH })
                }}
              />
            </div>
          )
        })}
      </div>
    </div>
  )
}

// ─── Main Modal ──────────────────────────────────────────────────────
export default function ReportBuilderModal({
  open,
  onCloseAction,
  config,
  onSaveAction,
}: {
  open: boolean
  onCloseAction: () => void
  config: WidgetConfig
  onSaveAction: (next: WidgetConfig) => void
}) {
  const report = config.options?.report
  const [state, setState] = useState<ReportState>({
    gridCols: report?.gridCols || 12,
    gridRows: report?.gridRows || 20,
    cellSize: report?.cellSize || 30,
    elements: report?.elements || [],
    variables: report?.variables || [],
    showGridLines: report?.showGridLines !== false,
  })
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [selectedVarId, setSelectedVarId] = useState<string | null>(null)
  const [panel, setPanel] = useState<'elements' | 'variables'>('elements')
  const [confirmDeleteVarId, setConfirmDeleteVarId] = useState<string | null>(null)
  const [varSearch, setVarSearch] = useState('')
  const varListRef = useRef<HTMLDivElement>(null)

  // Scroll selected variable row into view
  useEffect(() => {
    if (!selectedVarId || !varListRef.current) return
    const row = varListRef.current.querySelector(`[data-var-id="${selectedVarId}"]`) as HTMLElement | null
    row?.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
  }, [selectedVarId])

  // Sync state if config changes externally
  useEffect(() => {
    if (!open) return
    const r = config.options?.report
    if (r) {
      setState({
        gridCols: r.gridCols || 12,
        gridRows: r.gridRows || 20,
        cellSize: r.cellSize || 30,
        elements: r.elements || [],
        variables: r.variables || [],
        showGridLines: r.showGridLines !== false,
      })
    }
  }, [open, config.id])

  const selectedEl = state.elements.find((el) => el.id === selectedId) || null

  const updateElement = useCallback((id: string, patch: Partial<ReportElement>) => {
    setState((s) => ({ ...s, elements: s.elements.map((el) => el.id === id ? { ...el, ...patch } : el) }))
  }, [])

  const deleteElement = useCallback((id: string) => {
    setState((s) => ({ ...s, elements: s.elements.filter((el) => el.id !== id) }))
    if (selectedId === id) setSelectedId(null)
  }, [selectedId])

  const addElement = useCallback((type: ReportElement['type']) => {
    const id = genId()
    const base: ReportElement = { id, type, gridX: 0, gridY: 0, gridW: 3, gridH: 2 }
    if (type === 'label') {
      base.label = { text: 'New Label', fontSize: 14, fontWeight: 'normal', align: 'left' }
    } else if (type === 'table') {
      base.gridW = 6; base.gridH = 5
      base.table = {
        rows: 3, cols: 3,
        headers: [{ text: 'Column 1', colspan: 1 }, { text: 'Column 2', colspan: 1 }, { text: 'Column 3', colspan: 1 }],
        cells: Array.from({ length: 3 }, () => Array.from({ length: 3 }, () => ({ type: 'text' as const, text: '' }))),
        borderStyle: 'solid',
      }
    } else if (type === 'spaceholder') {
      base.gridW = 2; base.gridH = 1
    } else if (type === 'image') {
      base.gridW = 4; base.gridH = 3
      base.image = { url: '', objectFit: 'contain' }
    }
    setState((s) => ({ ...s, elements: [...s.elements, base] }))
    setSelectedId(id)
  }, [])

  const addVariable = useCallback(() => {
    const id = genId()
    const name = `Var${state.variables.length + 1}`
    const v: ReportVariable = { id, name, value: { field: '', agg: 'sum' }, format: 'wholeNumber' }
    setState((s) => ({ ...s, variables: [...s.variables, v] }))
    setPanel('variables')
    setSelectedVarId(id)
  }, [state.variables.length])

  const duplicateVariable = useCallback((id: string) => {
    setState((s) => {
      const src = s.variables.find((v) => v.id === id)
      if (!src) return s
      const copy: ReportVariable = { ...src, id: genId(), name: src.name + '_copy' }
      return { ...s, variables: [...s.variables, copy] }
    })
  }, [])

  const updateVariable = useCallback((id: string, v: ReportVariable) => {
    setState((s) => ({ ...s, variables: s.variables.map((vr) => vr.id === id ? v : vr) }))
  }, [])

  const deleteVariable = useCallback((id: string) => {
    setState((s) => ({ ...s, variables: s.variables.filter((v) => v.id !== id) }))
    setSelectedVarId((prev) => (prev === id ? null : prev))
  }, [])

  const handleSave = () => {
    const next: WidgetConfig = {
      ...config,
      options: {
        ...(config.options || {}),
        report: {
          gridCols: state.gridCols,
          gridRows: state.gridRows,
          cellSize: state.cellSize,
          elements: state.elements,
          variables: state.variables,
          showGridLines: state.showGridLines,
        },
      },
    }
    onSaveAction(next)
    onCloseAction()
  }

  if (!open) return null

  return createPortal(
    <div className="fixed inset-0 z-[200] flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-background rounded-xl border shadow-2xl flex flex-col animate-in fade-in zoom-in-95 duration-200" style={{ width: '96vw', height: '92vh', maxWidth: '1600px' }}>
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3 border-b bg-card">
          <div className="flex items-center gap-3">
            <button onClick={onCloseAction} className="text-muted-foreground hover:text-foreground p-1.5 rounded-md hover:bg-secondary transition-colors duration-150" title="Close" aria-label="Close">
              <RiArrowLeftLine className="h-4 w-4" />
            </button>
            <div className="h-5 w-px bg-border" />
            <div className="flex items-center gap-2">
              <div className="flex items-center justify-center h-7 w-7 rounded-lg bg-primary/10">
                <RiSettings3Line className="h-4 w-4 text-primary" />
              </div>
              <div>
                <h2 className="text-sm font-semibold leading-tight">Report Builder</h2>
                <span className="text-[10px] text-muted-foreground leading-none">{config.title}</span>
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              className="flex items-center gap-1.5 text-xs font-medium px-4 py-2 rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 shadow-sm transition-all duration-150 active:scale-[0.97]"
              onClick={handleSave}
            >
              <RiSave3Line className="h-3.5 w-3.5" /> Save Report
            </button>
          </div>
        </div>

        {/* Body */}
        <div className="flex flex-1 min-h-0">
          {/* Left: Element toolbar */}
          <div className="w-56 border-r bg-card/40 flex flex-col overflow-auto">
            {/* Add Element Section */}
            <div className="p-3 pb-0">
              <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-2">Add Element</div>
              <div className="grid grid-cols-2 gap-1.5">
                <button className="flex flex-col items-center gap-1 text-[11px] px-2 py-2.5 rounded-lg border border-border/60 hover:bg-secondary hover:border-primary/30 transition-all duration-150 cursor-pointer group" onClick={() => addElement('label')}>
                  <div className="flex items-center justify-center h-7 w-7 rounded-md bg-[hsl(var(--chart-1)/0.1)] group-hover:bg-[hsl(var(--chart-1)/0.2)] transition-colors"><RiText className="h-3.5 w-3.5 text-[hsl(var(--chart-1))]" /></div>
                  <span className="text-muted-foreground group-hover:text-foreground transition-colors">Label</span>
                </button>
                <button className="flex flex-col items-center gap-1 text-[11px] px-2 py-2.5 rounded-lg border border-border/60 hover:bg-secondary hover:border-primary/30 transition-all duration-150 cursor-pointer group" onClick={() => addElement('image')}>
                  <div className="flex items-center justify-center h-7 w-7 rounded-md bg-[hsl(var(--chart-3)/0.1)] group-hover:bg-[hsl(var(--chart-3)/0.2)] transition-colors"><RiImageLine className="h-3.5 w-3.5 text-[hsl(var(--chart-3))]" /></div>
                  <span className="text-muted-foreground group-hover:text-foreground transition-colors">Image</span>
                </button>
                <button className="flex flex-col items-center gap-1 text-[11px] px-2 py-2.5 rounded-lg border border-border/60 hover:bg-secondary hover:border-primary/30 transition-all duration-150 cursor-pointer group" onClick={() => addElement('table')}>
                  <div className="flex items-center justify-center h-7 w-7 rounded-md bg-[hsl(var(--success)/0.1)] group-hover:bg-[hsl(var(--success)/0.2)] transition-colors"><RiTableLine className="h-3.5 w-3.5 text-success" /></div>
                  <span className="text-muted-foreground group-hover:text-foreground transition-colors">Table</span>
                </button>
                <button className="flex flex-col items-center gap-1 text-[11px] px-2 py-2.5 rounded-lg border border-border/60 hover:bg-secondary hover:border-primary/30 transition-all duration-150 cursor-pointer group" onClick={() => addElement('spaceholder')}>
                  <div className="flex items-center justify-center h-7 w-7 rounded-md bg-[hsl(var(--accent)/0.1)] group-hover:bg-[hsl(var(--accent)/0.2)] transition-colors"><RiHashtag className="h-3.5 w-3.5 text-accent" /></div>
                  <span className="text-muted-foreground group-hover:text-foreground transition-colors">Variable</span>
                </button>
              </div>
            </div>

            <div className="px-3 pt-3">
              <button className="flex items-center justify-center gap-2 text-xs font-medium px-2 py-2 rounded-lg border-2 border-dashed border-primary/30 hover:border-primary/60 hover:bg-primary/5 w-full transition-all duration-150 text-primary cursor-pointer" onClick={addVariable}>
                <RiAddLine className="h-4 w-4" /> New Variable
              </button>
            </div>

            <div className="px-3 pt-3">
              <details className="group/grid">
                <summary className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider cursor-pointer select-none flex items-center gap-1 mb-2">
                  <span className="transition-transform duration-150 group-open/grid:rotate-90 text-[8px]">&#9654;</span>
                  Grid Settings
                </summary>
                <div className="space-y-2 pb-1">
                  <div className="grid grid-cols-3 gap-1.5">
                    <div>
                      <label className="block text-[10px] text-muted-foreground mb-0.5">Cols</label>
                      <input type="number" className="w-full h-7 text-[11px] rounded-md border bg-secondary/50 px-1.5 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={state.gridCols} onChange={(e) => setState(s => ({ ...s, gridCols: Math.max(1, +e.target.value) }))} />
                    </div>
                    <div>
                      <label className="block text-[10px] text-muted-foreground mb-0.5">Rows</label>
                      <input type="number" className="w-full h-7 text-[11px] rounded-md border bg-secondary/50 px-1.5 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={state.gridRows} onChange={(e) => setState(s => ({ ...s, gridRows: Math.max(1, +e.target.value) }))} />
                    </div>
                    <div>
                      <label className="block text-[10px] text-muted-foreground mb-0.5">Size</label>
                      <input type="number" className="w-full h-7 text-[11px] rounded-md border bg-secondary/50 px-1.5 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={state.cellSize} onChange={(e) => setState(s => ({ ...s, cellSize: Math.max(10, +e.target.value) }))} />
                    </div>
                  </div>
                  <label className="flex items-center gap-2 text-[11px] text-muted-foreground cursor-pointer hover:text-foreground transition-colors">
                    <input type="checkbox" className="rounded border-border accent-primary" checked={state.showGridLines} onChange={(e) => setState(s => ({ ...s, showGridLines: e.target.checked }))} />
                    Show grid lines
                  </label>
                </div>
              </details>
            </div>

            {/* Elements list */}
            <div className="px-3 pt-2 flex-1 min-h-0">
              <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider mb-2">Elements <span className="text-primary">({state.elements.length})</span></div>
              <div className="space-y-0.5">
                {state.elements.map((el) => (
                  <div
                    key={el.id}
                    className={`group flex items-center gap-2 text-[11px] px-2 py-1.5 rounded-md cursor-pointer transition-colors duration-100 ${selectedId === el.id ? 'bg-primary/10 text-primary ring-1 ring-primary/20' : 'hover:bg-secondary text-foreground/80'}`}
                    onClick={() => setSelectedId(el.id)}
                  >
                    {el.type === 'label' && <RiText className="h-3 w-3 shrink-0 text-[hsl(var(--chart-1))]" />}
                    {el.type === 'image' && <RiImageLine className="h-3 w-3 shrink-0 text-[hsl(var(--chart-3))]" />}
                    {el.type === 'table' && <RiTableLine className="h-3 w-3 shrink-0 text-success" />}
                    {el.type === 'spaceholder' && <RiHashtag className="h-3 w-3 shrink-0 text-accent" />}
                    <span className="truncate flex-1">{el.type === 'label' ? (el.label?.text?.slice(0, 20) || 'Label') : el.type === 'image' ? 'Image' : el.type === 'table' ? `Table ${el.table?.rows}×${el.table?.cols}` : `{{${state.variables.find(v => v.id === el.variableId)?.name || '?'}}}`}</span>
                    <button className="text-destructive hover:bg-destructive/10 rounded p-0.5 opacity-0 group-hover:opacity-100 transition-opacity" onClick={(e) => { e.stopPropagation(); deleteElement(el.id) }} aria-label="Delete element">
                      <RiDeleteBinLine className="h-3 w-3" />
                    </button>
                  </div>
                ))}
              </div>
            </div>
            <div className="h-3" />
          </div>

          {/* Center: Grid Canvas */}
          <div className="flex-1 flex flex-col min-w-0 p-3">
            <GridCanvas
              state={state}
              selectedId={selectedId}
              onSelectElement={setSelectedId}
              onMoveElement={(id, gx, gy) => updateElement(id, { gridX: gx, gridY: gy })}
              onMoveElements={(moves) => setState((s) => ({ ...s, elements: s.elements.map((el) => { const m = moves.find(m => m.id === el.id); return m ? { ...el, gridX: m.gridX, gridY: m.gridY } : el }) }))}
              onResizeElement={(id, gw, gh) => updateElement(id, { gridW: gw, gridH: gh })}
              onUpdateElement={(id, el) => setState((s) => ({ ...s, elements: s.elements.map((e) => e.id === id ? el : e) }))}
            />
          </div>

          {/* Right: Properties Panel */}
          <div className="w-96 border-l bg-card/40 flex flex-col overflow-auto">
            <div className="flex gap-1 p-3 pb-2 border-b bg-card/60">
              <button className={`text-[11px] font-medium px-3 py-1.5 rounded-md transition-all duration-150 ${panel === 'elements' ? 'bg-primary text-primary-foreground shadow-sm' : 'text-muted-foreground hover:text-foreground hover:bg-secondary'}`} onClick={() => setPanel('elements')}>Properties</button>
              <button className={`text-[11px] font-medium px-3 py-1.5 rounded-md transition-all duration-150 flex items-center gap-1.5 ${panel === 'variables' ? 'bg-primary text-primary-foreground shadow-sm' : 'text-muted-foreground hover:text-foreground hover:bg-secondary'}`} onClick={() => setPanel('variables')}>
                Variables
                {state.variables.length > 0 && <span className={`text-[9px] px-1.5 py-0.5 rounded-full ${panel === 'variables' ? 'bg-primary-foreground/20' : 'bg-primary/10 text-primary'}`}>{state.variables.length}</span>}
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3 space-y-3">
              {panel === 'elements' && selectedEl && (
                <div className="space-y-3">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <div className="flex items-center justify-center h-6 w-6 rounded-md bg-secondary">
                        {selectedEl.type === 'label' && <RiText className="h-3 w-3 text-[hsl(var(--chart-1))]" />}
                        {selectedEl.type === 'image' && <RiImageLine className="h-3 w-3 text-[hsl(var(--chart-3))]" />}
                        {selectedEl.type === 'table' && <RiTableLine className="h-3 w-3 text-success" />}
                        {selectedEl.type === 'spaceholder' && <RiHashtag className="h-3 w-3 text-accent" />}
                      </div>
                      <span className="text-xs font-semibold capitalize">{selectedEl.type}</span>
                    </div>
                    <button className="text-destructive/70 hover:text-destructive hover:bg-destructive/10 rounded-md p-1.5 transition-colors duration-150" onClick={() => deleteElement(selectedEl.id)} title="Delete" aria-label="Delete element">
                      <RiDeleteBinLine className="h-3.5 w-3.5" />
                    </button>
                  </div>

                  {/* Position */}
                  <div className="rounded-lg border bg-secondary/30 p-2">
                    <div className="text-[10px] font-medium text-muted-foreground mb-1.5">Position & Size</div>
                    <div className="grid grid-cols-4 gap-1.5">
                      <div><label className="block text-[9px] text-muted-foreground mb-0.5 text-center">X</label><input type="number" className="w-full h-6 text-[11px] rounded-md border bg-background px-1.5 text-center focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={selectedEl.gridX} onChange={(e) => updateElement(selectedEl.id, { gridX: Math.max(0, +e.target.value) })} /></div>
                      <div><label className="block text-[9px] text-muted-foreground mb-0.5 text-center">Y</label><input type="number" className="w-full h-6 text-[11px] rounded-md border bg-background px-1.5 text-center focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={selectedEl.gridY} onChange={(e) => updateElement(selectedEl.id, { gridY: Math.max(0, +e.target.value) })} /></div>
                      <div><label className="block text-[9px] text-muted-foreground mb-0.5 text-center">W</label><input type="number" className="w-full h-6 text-[11px] rounded-md border bg-background px-1.5 text-center focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={selectedEl.gridW} onChange={(e) => updateElement(selectedEl.id, { gridW: Math.max(1, +e.target.value) })} /></div>
                      <div><label className="block text-[9px] text-muted-foreground mb-0.5 text-center">H</label><input type="number" className="w-full h-6 text-[11px] rounded-md border bg-background px-1.5 text-center focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={selectedEl.gridH} onChange={(e) => updateElement(selectedEl.id, { gridH: Math.max(1, +e.target.value) })} /></div>
                    </div>
                  </div>

                  <div className="border-t pt-3">
                    <ElementProps element={selectedEl} variables={state.variables} onUpdate={(el) => updateElement(el.id, el)} />
                  </div>
                </div>
              )}

              {panel === 'elements' && !selectedEl && (
                <div className="flex flex-col items-center justify-center py-12 text-center">
                  <div className="flex items-center justify-center h-10 w-10 rounded-full bg-secondary mb-3">
                    <RiSettings3Line className="h-5 w-5 text-muted-foreground" />
                  </div>
                  <p className="text-xs text-muted-foreground">Select an element on the canvas<br />to edit its properties</p>
                </div>
              )}

              {panel === 'variables' && (
                <div className="flex flex-col -mx-3 -mt-3" style={{ height: 'calc(100% + 12px)' }}>
                  {/* Top half: + Variable button + scrollable list */}
                  <div className="flex flex-col border-b" style={{ height: '50%' }}>
                    {/* Toolbar */}
                    {(() => {
                      const selectedVar = state.variables.find(v => v.id === selectedVarId)
                      const isConfirming = confirmDeleteVarId === selectedVarId && !!selectedVarId
                      return (
                        <div className="flex items-center px-3 py-1.5 border-b bg-secondary/20 shrink-0 gap-2">
                          <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wide shrink-0">Variables</span>
                          <div className="flex items-center gap-1 ml-auto">
                            {selectedVar && !isConfirming && (
                              <>
                                <button
                                  title="Duplicate selected"
                                  className="flex items-center gap-1 text-[10px] text-muted-foreground hover:text-foreground hover:bg-secondary px-1.5 py-0.5 rounded transition-colors"
                                  onClick={() => duplicateVariable(selectedVar.id)}
                                >
                                  <RiFileCopyLine className="h-3 w-3" />
                                </button>
                                <button
                                  title="Delete selected"
                                  className="flex items-center gap-1 text-[10px] text-destructive/70 hover:text-destructive hover:bg-destructive/10 px-1.5 py-0.5 rounded transition-colors"
                                  onClick={() => setConfirmDeleteVarId(selectedVar.id)}
                                >
                                  <RiDeleteBinLine className="h-3 w-3" />
                                </button>
                                <div className="w-px h-3 bg-border mx-0.5 shrink-0" />
                              </>
                            )}
                            {selectedVar && isConfirming && (
                              <>
                                <span className="text-[10px] text-destructive font-medium shrink-0">Delete?</span>
                                <button
                                  className="text-[10px] font-medium text-destructive hover:bg-destructive/10 px-2 py-0.5 rounded transition-colors"
                                  onClick={() => { deleteVariable(selectedVar.id); setConfirmDeleteVarId(null) }}
                                >Yes</button>
                                <button
                                  className="text-[10px] text-muted-foreground hover:text-foreground hover:bg-secondary px-2 py-0.5 rounded transition-colors"
                                  onClick={() => setConfirmDeleteVarId(null)}
                                >No</button>
                                <div className="w-px h-3 bg-border mx-0.5 shrink-0" />
                              </>
                            )}
                            <button
                              className="flex items-center gap-1 text-[10px] font-medium text-primary hover:text-primary/80 hover:bg-primary/10 px-2 py-0.5 rounded transition-colors shrink-0"
                              onClick={() => { setConfirmDeleteVarId(null); addVariable() }}
                            >
                              <RiAddLine className="h-3 w-3" /> Variable
                            </button>
                          </div>
                        </div>
                      )
                    })()}
                    {/* Search */}
                    {state.variables.length > 0 && (
                      <div className="px-3 py-1.5 border-b bg-secondary/10 shrink-0">
                        <input
                          type="text"
                          value={varSearch}
                          onChange={e => setVarSearch(e.target.value)}
                          placeholder="Search variables…"
                          className="w-full h-6 text-[10px] rounded border bg-secondary/40 px-2 outline-none focus:ring-1 focus:ring-primary/40 transition-shadow"
                        />
                      </div>
                    )}
                    {/* Scrollable table */}
                    <div ref={varListRef} className="overflow-y-auto flex-1">
                      {state.variables.length === 0 ? (
                        <div className="flex flex-col items-center justify-center py-8 text-center px-3">
                          <RiHashtag className="h-6 w-6 text-muted-foreground mb-2" />
                          <p className="text-xs text-muted-foreground">No variables yet — click + Variable</p>
                        </div>
                      ) : (
                        <table className="w-full text-[11px]">
                          <thead className="sticky top-0 z-10">
                            <tr className="bg-secondary/60 border-b">
                              <th className="text-left px-3 py-1.5 text-[10px] font-semibold text-muted-foreground">Name</th>
                              <th className="text-left px-2 py-1.5 text-[10px] font-semibold text-muted-foreground">Type</th>
                            </tr>
                          </thead>
                          <tbody>
                            {state.variables.filter(v => !varSearch.trim() || v.name.toLowerCase().includes(varSearch.toLowerCase())).map((v) => (
                              <tr
                                key={v.id}
                                data-var-id={v.id}
                                className={`group border-b last:border-0 cursor-pointer transition-colors duration-100 ${selectedVarId === v.id ? 'bg-primary/10' : 'hover:bg-secondary/40'}`}
                                onClick={() => setSelectedVarId(v.id)}
                              >
                                <td className="px-3 py-1">
                                  <input
                                    className="bg-transparent border-b border-dashed border-transparent hover:border-border focus:border-primary outline-none w-full text-[10px] font-medium"
                                    value={v.name}
                                    onChange={(e) => updateVariable(v.id, { ...v, name: e.target.value.replace(/[^a-zA-Z0-9_]/g, '') })}
                                    onClick={(e) => e.stopPropagation()}
                                    placeholder="VarName"
                                  />
                                </td>
                                <td className="px-2 py-1">
                                  <span className={`text-[9px] px-1.5 py-0.5 rounded-full font-medium ${v.type === 'expression' ? 'bg-amber-500/10 text-amber-600' : v.type === 'datetime' ? 'bg-blue-500/10 text-blue-500' : 'bg-primary/10 text-primary'}`}>
                                    {v.type || 'query'}
                                  </span>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      )}
                    </div>
                  </div>
                  {/* Bottom half: variable editor */}
                  <div className="overflow-y-auto" style={{ height: '50%' }}>
                    {selectedVarId && (() => {
                      const selVar = state.variables.find((v) => v.id === selectedVarId)
                      if (!selVar) return null
                      return (
                        <VariableEditor
                          key={selectedVarId}
                          variable={selVar}
                          allVariables={state.variables}
                          onUpdate={(vr) => updateVariable(selVar.id, vr)}
                          onDelete={() => deleteVariable(selVar.id)}
                          onDuplicate={() => duplicateVariable(selVar.id)}
                          widgetId={config.id}
                        />
                      )
                    })()}
                    {!selectedVarId && (
                      <div className="flex flex-col items-center justify-center h-full text-center px-3">
                        <p className="text-xs text-muted-foreground">Select a variable to configure it</p>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>,
    document.body
  )
}
