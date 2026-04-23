"use client"

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useQuery } from '@tanstack/react-query'
import { Api, QueryApi } from '@/lib/api'
import { PresetConfig, DEFAULT_PRESET, QUICK_PICKS, QuickPick, PERIOD_OPTIONS, OFFSET_OPTIONS, AS_OF_OPTIONS, RANGE_MODE_OPTIONS, parseLegacyPreset, matchQuickPick, presetConfigToLabel, LEGACY_PRESET_MAP, usePresetPreview } from '@/lib/datePresets'
import { ConditionalRule, ConditionalFormat, OP_OPTIONS, ICON_OPTIONS, ICON_GLYPH, describeRule, presetTrendArrows, preset4CircleSet, presetHeatmap3 } from '@/lib/conditionalFormat'
import { useAuth } from '@/components/providers/AuthProvider'
import type { WidgetConfig, ReportElement, ReportVariable, ReportTableCell } from '@/types/widgets'
import { RiAddLine, RiDeleteBinLine, RiDragMoveLine, RiSettings3Line, RiTableLine, RiText, RiHashtag, RiCloseLine, RiArrowLeftLine, RiSave3Line, RiImageLine, RiFileCopyLine, RiAlignLeft, RiAlignCenter, RiAlignRight, RiAlignTop, RiAlignVertically, RiAlignBottom, RiDatabase2Line, RiArrowDownSLine, RiArrowUpLine, RiArrowDownLine, RiBarChart2Line } from '@remixicon/react'
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
  const [dragIdx, setDragIdx] = useState<number | null>(null)
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

  const reorderTokens = (fromIdx: number, toIdx: number) => {
    if (fromIdx === toIdx) return
    const next = [...tokens]
    const [moved] = next.splice(fromIdx, 1)
    next.splice(toIdx, 0, moved)
    onChange(serializeTokens(next))
  }

  const handleDragStart = (e: React.DragEvent, idx: number) => {
    setDragIdx(idx)
    e.dataTransfer.effectAllowed = 'move'
  }

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault()
    e.dataTransfer.dropEffect = 'move'
  }

  const handleDrop = (e: React.DragEvent, toIdx: number) => {
    e.preventDefault()
    if (dragIdx !== null) {
      reorderTokens(dragIdx, toIdx)
    }
    setDragIdx(null)
  }

  const handleDragEnd = () => {
    setDragIdx(null)
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
          <span
            key={i}
            draggable
            onDragStart={(e) => handleDragStart(e, i)}
            onDragOver={handleDragOver}
            onDrop={(e) => handleDrop(e, i)}
            onDragEnd={handleDragEnd}
            className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-medium leading-none border cursor-move ${
            tok.kind === 'var'
              ? `bg-primary/15 border-primary/25 text-primary ${open && editIdx === i ? 'ring-1 ring-primary' : 'hover:bg-primary/25'}`
              : tok.kind === 'op'
              ? `bg-secondary text-foreground border-border font-mono text-[13px] ${open && editIdx === i ? 'ring-1 ring-border' : 'hover:bg-secondary/80'}`
              : `bg-amber-500/15 text-amber-700 dark:text-amber-400 border-amber-500/25 font-mono ${open && editIdx === i ? 'ring-1 ring-amber-500/50' : 'hover:bg-amber-500/25'}`
          } ${dragIdx === i ? 'opacity-40' : ''}`}>
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

  // Normalize source when tables load:
  // - Promotes short name to qualified name (e.g. 'mt5_deals' → 'mt5.mt5_deals')
  // - Preserves table when switching datasources if the same table exists
  // - Resets source (and stored variable) if table not found in the new datasource
  useEffect(() => {
    if (!source || !tables.length) return
    if (tables.includes(source)) return  // exact match, nothing to do
    const shortName = source.split('.').pop()
    const match = tables.find((t: string) => t.split('.').pop() === shortName)
    if (match) {
      setSource(match)
    } else {
      // Table doesn't exist in this datasource — reset
      setSource('')
      handleChange({ source: '' })
    }
  }, [tables])

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
      console.log('[transformColumns] baseColsSet:', Array.from(baseColsSet).slice(0, 10), '... total:', baseColsSet.size)

      // Also add join column aliases to available set (so transforms can reference them)
      const joinsForDeps = Array.isArray((dsTransformsQ.data as any)?.joins) ? ((dsTransformsQ.data as any).joins as any[]) : []
      console.log('[transformColumns] joinsForDeps:', joinsForDeps.length)
      for (const j of joinsForDeps) {
        const sc = (j?.scope || {}) as any
        const lvl = String(sc?.level || 'datasource').toLowerCase()
        const scopeMatch = (
          lvl === 'datasource' ||
          (lvl === 'table' && sc?.table && source && tblEq(String(sc.table), source)) ||
          (lvl === 'widget' && widgetId && String(sc?.widgetId || '') === String(widgetId))
        )
        if (!scopeMatch) continue
        // Add join column aliases and original names
        const cols = Array.isArray(j?.columns) ? (j.columns as any[]) : []
        for (const c of cols) {
          const alias = String((c?.alias || '')).toLowerCase()
          const orig = String((c?.name || '')).toLowerCase()
          if (alias) baseColsSet.add(alias)
          if (orig) baseColsSet.add(orig)
          console.log('[transformColumns] Added join column to baseColsSet: alias=', alias, 'orig=', orig)
        }
        // Add aggregate alias if present
        const aggAlias = String((j?.aggregate as any)?.alias || '').toLowerCase()
        if (aggAlias) baseColsSet.add(aggAlias)
      }
      console.log('[transformColumns] baseColsSet after joins:', Array.from(baseColsSet).filter(c => c.includes('markup')))
      
      // Helper to extract column references from expression
      const extractRefs = (expr: string): Set<string> => {
        const refs = new Set<string>()
        if (!expr) return refs
        
        // Step 1: Remove single-quoted string literals (they're not identifiers)
        // Replace with empty string to avoid any matching
        let processed = expr.replace(/'[^']*'/g, '')
        
        // Step 2: Extract double-quoted identifiers (e.g., "Type 1 Markup")
        const dqPattern = /"([^"]+)"/g
        let match
        while ((match = dqPattern.exec(processed)) !== null) {
          const ident = match[1]
          if (ident && ident.trim()) {
            refs.add(ident.toLowerCase().trim())
          }
        }
        
        // Step 3: Remove double-quoted identifiers from the string
        processed = processed.replace(/"[^"]*"/g, ' ')
        
        // Step 4: Extract bracket-quoted identifiers (e.g., [Type 1 Markup])
        const brPattern = /\[([^\]]+)\]/g
        while ((match = brPattern.exec(processed)) !== null) {
          const ident = match[1]
          if (ident && ident.trim()) {
            refs.add(ident.toLowerCase().trim())
          }
        }
        
        // Step 5: Remove bracket-quoted identifiers
        processed = processed.replace(/\[[^\]]*\]/g, ' ')
        
        // Step 6: Extract unquoted identifiers (word boundaries, must start with letter or underscore)
        const SQL_KEYWORDS = new Set(['and', 'or', 'not', 'case', 'when', 'then', 'else', 'end', 'null', 'true', 'false',
          'cast', 'as', 'left', 'right', 'varchar', 'int', 'double', 'float', 'char', 'nvarchar', 'varchar2',
          'bigint', 'smallint', 'tinyint', 'decimal', 'numeric', 'bit', 'date', 'datetime', 'timestamp',
          'text', 'ntext', 'string', 'boolean', 'bool', 'unsigned', 'signed', 'binary', 'varbinary',
          'coalesce', 'isnull', 'ifnull', 'nullif', 'if', 'substr', 'substring', 'len', 'length',
          'trim', 'ltrim', 'rtrim', 'upper', 'lower', 'replace', 'concat', 'like', 'in', 'between',
          'is', 'over', 'partition', 'by', 'from', 'select', 'where', 'having', 'group', 'order',
          'try_cast', 'try', 'cast', 'sum', 'count', 'avg', 'min', 'max'])
        
        const idPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g
        while ((match = idPattern.exec(processed)) !== null) {
          const ident = match[1]
          if (ident && !SQL_KEYWORDS.has(ident.toLowerCase())) {
            refs.add(ident.toLowerCase())
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
      // Use iterative acceptance so transforms can reference other transforms
      const transforms = Array.isArray((dsTransformsQ.data as any)?.transforms) ? ((dsTransformsQ.data as any).transforms as any[]) : []
      console.log('[transformColumns] transforms to process:', transforms.map((t: any) => ({ type: t?.type, name: t?.name || t?.target, expr: t?.expr?.substring(0, 50) })))
      const acceptedTransformNames = new Set<string>()
      let changed = true
      let iterations = 0
      const maxIterations = 10 // Prevent infinite loops
      
      while (changed && iterations < maxIterations) {
        changed = false
        iterations++
        
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
            name = String(tr.name).trim().toLowerCase()
            expr = tr?.expr ? String(tr.expr) : null
          } else if ((type === 'case' || type === 'replace' || type === 'translate' || type === 'nullhandling') && tr?.target) {
            name = String(tr.target).trim().toLowerCase()
            // These transform types modify existing columns, so target column must exist
            if (lvl === 'datasource' && !baseColsSet.has(name)) continue
          }
          
          if (!name) continue
          
          // Skip if already accepted
          if (acceptedTransformNames.has(name)) continue
          
          // For datasource-scoped computed transforms, check dependencies
          if (lvl === 'datasource' && expr) {
            const refs = extractRefs(expr)
            // Check against baseColsSet (base + joins) AND previously accepted transforms
            const allDepsExist = Array.from(refs).every(ref => baseColsSet.has(ref) || acceptedTransformNames.has(ref))
            console.log('[transformColumns] Checking transform:', name, 'refs:', Array.from(refs), 'allDepsExist:', allDepsExist, 'baseColsSet has type 1 markup:', baseColsSet.has('type 1 markup'), 'acceptedTransformNames has clienttype:', acceptedTransformNames.has('clienttype'))
            if (!allDepsExist) continue
          }
          
          // Accept this transform
          acceptedTransformNames.add(name)
          out.push(name.charAt(0).toUpperCase() + name.slice(1)) // Preserve original case in output
          console.log('[transformColumns] Accepted transform:', name, 'deps:', Array.from(extractRefs(expr || '')))
          changed = true
        }
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
        {/* Variable Name */}
        <div>
          <label className="block text-[10px] font-medium text-muted-foreground mb-1">Name</label>
          <input
            className="w-full h-7 text-[11px] rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow font-mono"
            value={variable.name}
            onChange={(e) => handleChange({ name: e.target.value })}
            placeholder="variableName"
          />
        </div>
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
                  {Object.entries(QUICK_PICKS.reduce<Record<string, QuickPick[]>>((acc, qp) => {
                    (acc[qp.group] ??= []).push(qp); return acc
                  }, {})).map(([group, picks]) => (
                    <optgroup key={group} label={group}>
                      {picks.map(qp => <option key={qp.label} value={JSON.stringify(qp.config)}>{qp.label}</option>)}
                    </optgroup>
                  ))}
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
                  <select className="flex-1 h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={dsId} onChange={(e) => { setDsId(e.target.value); handleChange({ datasourceId: e.target.value }) }}>
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
                  <optgroup label="Last Period">
                    <option value="last_daily_sum">Last Daily Sum</option>
                  </optgroup>
                  <optgroup label="Moving Average (MA)">
                    <option value="ma7">MA-7 (7-day)</option>
                    <option value="ma14">MA-14 (14-day)</option>
                    <option value="ma30">MA-30 (30-day)</option>
                    <option value="ma60">MA-60 (60-day)</option>
                  </optgroup>
                </select>
              </div>
            </div>
            {variable.value?.agg === 'last_daily_sum' && (
              <div className="space-y-2">
                <div>
                  <label className="block text-[10px] font-medium text-muted-foreground mb-1">Date column <span className="text-primary">*</span></label>
                  <select className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={variable.value?.avgDateField || ''} onChange={(e) => handleChange({ value: { ...variable.value, avgDateField: e.target.value } })}>
                    <option value="">Select…</option>
                    {columns.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <p className="text-[9px] text-muted-foreground">SUM(column) for rows where {variable.value?.avgDateField || 'date col'} = last day in the filtered window</p>
              </div>
            )}
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
                {variable.value?.agg === 'avg_wday' && (
                  <label className="inline-flex items-center gap-1.5 text-[10px]">
                    <input type="checkbox" className="rounded border-input" checked={!!variable.value?.applyHolidays} onChange={(e) => handleChange({ value: { ...variable.value, applyHolidays: e.target.checked } })} />
                    <span className="text-muted-foreground">Exclude holidays from working days</span>
                  </label>
                )}
              </div>
            )}
            {['ma7','ma14','ma30','ma60'].includes(variable.value?.agg || '') && (
              <div className="space-y-2">
                <div>
                  <label className="block text-[10px] font-medium text-muted-foreground mb-1">Date column <span className="text-primary">*</span></label>
                  <select className="w-full h-7 text-xs rounded-md border bg-secondary/40 px-2 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow cursor-pointer" value={variable.value?.avgDateField || ''} onChange={(e) => handleChange({ value: { ...variable.value, avgDateField: e.target.value } })}>
                    <option value="">Select…</option>
                    {columns.map((c) => <option key={c} value={c}>{c}</option>)}
                  </select>
                </div>
                <p className="text-[9px] text-muted-foreground">
                  Average of daily SUM({variable.value?.field || 'column'}) over the last {variable.value?.agg === 'ma7' ? '7' : variable.value?.agg === 'ma14' ? '14' : variable.value?.agg === 'ma30' ? '30' : '60'} days ending at the last date in the filtered window.
                </p>
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

        {/* Conditional Formatting - collapsible */}
        {varType !== 'datetime' && (
          <details className="group/cond">
            <summary className="text-[10px] font-medium text-muted-foreground cursor-pointer select-none flex items-center gap-1 py-1 hover:text-foreground transition-colors">
              <span className="transition-transform duration-150 group-open/cond:rotate-90 text-[8px]">&#9654;</span>
              Conditional Formatting
              {variable.conditionalFormat?.enabled && variable.conditionalFormat.rules?.length > 0 && (
                <span className="ml-auto text-[9px] text-primary/70">{variable.conditionalFormat.rules.length} rule{variable.conditionalFormat.rules.length === 1 ? '' : 's'}</span>
              )}
            </summary>
            <div className="space-y-2 pt-1.5">
              {/* Enable toggle + preset buttons */}
              <div className="flex items-center gap-2 flex-wrap">
                <label className="flex items-center gap-1 text-[10px] cursor-pointer">
                  <input
                    type="checkbox"
                    checked={!!variable.conditionalFormat?.enabled}
                    onChange={(e) => {
                      const cf = variable.conditionalFormat || { enabled: false, rules: [] }
                      handleChange({ conditionalFormat: { ...cf, enabled: e.target.checked, rules: cf.rules || [] } })
                    }}
                  />
                  Enabled
                </label>
                <span className="text-[9px] text-muted-foreground">Presets:</span>
                <button type="button" className="text-[9px] px-2 py-0.5 rounded border hover:bg-muted transition-colors"
                  onClick={() => handleChange({ conditionalFormat: presetTrendArrows() })}>
                  ▲▼ Arrows
                </button>
                <button type="button" className="text-[9px] px-2 py-0.5 rounded border hover:bg-muted transition-colors"
                  onClick={() => handleChange({ conditionalFormat: preset4CircleSet() })}>
                  ●●●● 4-Circle
                </button>
                <button type="button" className="text-[9px] px-2 py-0.5 rounded border hover:bg-muted transition-colors"
                  onClick={() => handleChange({ conditionalFormat: presetHeatmap3() })}>
                  Heatmap
                </button>
              </div>

              {/* Rules list */}
              <div className="space-y-1.5">
                {(variable.conditionalFormat?.rules || []).map((rule, idx) => {
                  const rules = variable.conditionalFormat?.rules || []
                  const patchRule = (p: Partial<ConditionalRule>) => {
                    const next = rules.map((r, i) => i === idx ? { ...r, ...p } : r)
                    handleChange({ conditionalFormat: { ...(variable.conditionalFormat || { enabled: true }), rules: next } })
                  }
                  const removeRule = () => {
                    const next = rules.filter((_, i) => i !== idx)
                    handleChange({ conditionalFormat: { ...(variable.conditionalFormat || { enabled: true }), rules: next } })
                  }
                  const moveRule = (dir: -1 | 1) => {
                    const j = idx + dir
                    if (j < 0 || j >= rules.length) return
                    const next = rules.slice()
                    ;[next[idx], next[j]] = [next[j], next[idx]]
                    handleChange({ conditionalFormat: { ...(variable.conditionalFormat || { enabled: true }), rules: next } })
                  }
                  return (
                    <div key={idx} className="border rounded-md p-1.5 bg-secondary/20 space-y-1">
                      <div className="flex items-center gap-1">
                        <span className="text-[9px] text-muted-foreground">#{idx + 1}</span>
                        <div className="flex-1" />
                        <button type="button" className="text-[9px] px-1 py-0.5 rounded hover:bg-muted disabled:opacity-40 disabled:cursor-not-allowed"
                          disabled={idx === 0} onClick={() => moveRule(-1)} title="Move up">▲</button>
                        <button type="button" className="text-[9px] px-1 py-0.5 rounded hover:bg-muted disabled:opacity-40 disabled:cursor-not-allowed"
                          disabled={idx === rules.length - 1} onClick={() => moveRule(1)} title="Move down">▼</button>
                        <button type="button" className="text-[9px] px-1 py-0.5 rounded text-destructive hover:bg-destructive/10"
                          onClick={removeRule} title="Delete rule">×</button>
                      </div>

                      {/* Row 1: operator + value(s) */}
                      <div className="flex items-center gap-1">
                        <select className="h-6 text-[10px] rounded border bg-secondary/40 px-1 cursor-pointer"
                          value={rule.op}
                          onChange={(e) => patchRule({ op: e.target.value as any })}>
                          {OP_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
                        </select>
                        <input type="number" step="any" className="h-6 text-[10px] rounded border bg-secondary/40 px-1 w-20"
                          value={Number.isFinite(rule.value) ? rule.value : ''}
                          onChange={(e) => patchRule({ value: parseFloat(e.target.value) })}
                          placeholder="value" />
                        {rule.op === 'between' && (
                          <>
                            <span className="text-[9px] text-muted-foreground">and</span>
                            <input type="number" step="any" className="h-6 text-[10px] rounded border bg-secondary/40 px-1 w-20"
                              value={rule.value2 != null && Number.isFinite(rule.value2) ? rule.value2 : ''}
                              onChange={(e) => patchRule({ value2: parseFloat(e.target.value) })}
                              placeholder="value" />
                          </>
                        )}
                      </div>

                      {/* Row 2: icon + colors */}
                      <div className="flex items-center gap-1 flex-wrap">
                        <select className="h-6 text-[10px] rounded border bg-secondary/40 px-1 cursor-pointer"
                          value={rule.icon || 'none'}
                          onChange={(e) => patchRule({ icon: e.target.value as any })}>
                          {ICON_OPTIONS.map(i => <option key={i.value} value={i.value}>{i.glyph ? `${i.glyph} ${i.label}` : i.label}</option>)}
                        </select>
                        {rule.icon && rule.icon !== 'none' && (
                          <div className="flex items-center gap-0.5" title="Icon color">
                            <span className="text-[9px] text-muted-foreground">Icon</span>
                            <input type="color" className="w-5 h-5 rounded border cursor-pointer"
                              value={rule.iconColor || '#16a34a'}
                              onChange={(e) => patchRule({ iconColor: e.target.value })} />
                          </div>
                        )}
                        <div className="flex items-center gap-0.5" title="Text color">
                          <span className="text-[9px] text-muted-foreground">Text</span>
                          <input type="color" className="w-5 h-5 rounded border cursor-pointer"
                            value={rule.textColor || '#111827'}
                            onChange={(e) => patchRule({ textColor: e.target.value })} />
                          {rule.textColor && (
                            <button type="button" className="text-[8px] text-muted-foreground hover:text-foreground"
                              onClick={() => patchRule({ textColor: undefined })}>clear</button>
                          )}
                        </div>
                        <div className="flex items-center gap-0.5" title="Background color">
                          <span className="text-[9px] text-muted-foreground">Bg</span>
                          <input type="color" className="w-5 h-5 rounded border cursor-pointer"
                            value={rule.bgColor || '#ffffff'}
                            onChange={(e) => patchRule({ bgColor: e.target.value })} />
                          {rule.bgColor && (
                            <button type="button" className="text-[8px] text-muted-foreground hover:text-foreground"
                              onClick={() => patchRule({ bgColor: undefined })}>clear</button>
                          )}
                        </div>
                      </div>

                      {/* Preview */}
                      <div className="text-[9px] text-muted-foreground flex items-center gap-1">
                        <span>When value is {describeRule(rule)} →</span>
                        <span
                          className="inline-flex items-center gap-0.5 px-1 rounded"
                          style={{
                            color: rule.textColor || undefined,
                            backgroundColor: rule.bgColor || undefined,
                          }}
                        >
                          {rule.icon && rule.icon !== 'none' && (
                            <span style={{ color: rule.iconColor || undefined }}>{ICON_GLYPH[rule.icon]}</span>
                          )}
                          <span>123</span>
                        </span>
                      </div>
                    </div>
                  )
                })}
              </div>

              {/* Add rule */}
              <button type="button"
                className="w-full text-[10px] py-1 rounded border border-dashed hover:bg-muted transition-colors flex items-center justify-center gap-1"
                onClick={() => {
                  const cf = variable.conditionalFormat || { enabled: true, rules: [] }
                  const newRule: ConditionalRule = { op: '>', value: 0, icon: 'none' }
                  handleChange({ conditionalFormat: { ...cf, enabled: cf.enabled ?? true, rules: [...(cf.rules || []), newRule] } })
                }}>
                + Add Rule
              </button>

              {(variable.conditionalFormat?.rules?.length || 0) > 0 && (
                <p className="text-[9px] text-muted-foreground leading-tight">
                  Rules are evaluated top-to-bottom; the first match wins. Order rules from most-specific (highest threshold) to least-specific (catch-all) for continuous coverage.
                </p>
              )}
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
  const dateRe = /^\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?$/
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
  const [expanded, setExpanded] = useState(false)
  const [sel, setSel] = useState<any[]>(selected || [])
  const [search, setSearch] = useState('')
  const [samples, setSamples] = useState<string[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => { setSel(selected || []) }, [JSON.stringify(selected)])

  // Fetch distinct values only when expanded
  useEffect(() => {
    if (!expanded) return
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
  }, [expanded, field, source, datasourceId, widgetId])

  const filtered = samples.filter(v => String(v).toLowerCase().includes(search.toLowerCase()))
  const toggle = (v: any) => setSel(prev => prev.includes(v) ? prev.filter(x => x !== v) : [...prev, v])
  const activeCount = selected.length

  return (
    <div className="rounded-md border bg-card">
      <button
        type="button"
        className="w-full flex items-center justify-between px-2.5 py-1.5 text-[11px] font-medium hover:bg-muted/40 transition-colors rounded-md"
        onClick={() => setExpanded(e => !e)}
      >
        <span className="flex items-center gap-1.5">
          <svg className={`size-2.5 transition-transform ${expanded ? 'rotate-90' : ''}`} viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M7.21 14.77a.75.75 0 01.02-1.06L11.168 10 7.23 6.29a.75.75 0 111.04-1.08l4.5 4.25a.75.75 0 010 1.08l-4.5 4.25a.75.75 0 01-1.06-.02z" clipRule="evenodd" /></svg>
          Filter: {field}
        </span>
        {activeCount > 0 && (
          <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-primary/15 text-primary font-medium">{activeCount} active</span>
        )}
      </button>
      {expanded && (
        <div className="px-2.5 pb-2.5 border-t space-y-2 pt-2">
          <div className="flex items-center justify-between">
            <div className="text-[10px] text-muted-foreground">Select values</div>
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
      )}
    </div>
  )
}

// Date rule tab: presets (Today, Yesterday…) + custom (After/Before/Between)
// Week-start-day: 0=Sunday (default via NEXT_PUBLIC_WEEK_START_DAY), 1=Monday
const _DEFAULT_WEEK_START = (typeof process !== 'undefined' && process.env?.NEXT_PUBLIC_WEEK_START_DAY) || 'SUN'
const _DEFAULT_WEEKENDS = (typeof process !== 'undefined' && process.env?.NEXT_PUBLIC_WEEKENDS) || 'SAT_SUN'
function DateRuleEditor({ field, where, onPatch }: { field: string; where: Record<string, any>; onPatch: (patch: Record<string, any>) => void }) {
  type DateOp = 'eq'|'ne'|'gt'|'gte'|'lt'|'lte'|'between'
  const [mode, setMode] = useState<'preset'|'custom'>('preset')
  const [config, setConfig] = useState<PresetConfig>({ ...DEFAULT_PRESET })
  const [selectedQuickPick, setSelectedQuickPick] = useState<string | null>(null)
  const [op, setOp] = useState<DateOp>('eq')
  const [a, setA] = useState(''); const [b, setB] = useState('')
  const preview = usePresetPreview(mode === 'preset' ? config : null)

  // On mount: detect legacy string or structured object
  useEffect(() => {
    const existingPreset = where?.[`${field}__date_preset`]
    const savedOp = where?.[`${field}__op`] as DateOp | undefined
    if (savedOp && ['eq','ne','gt','gte','lt','lte','between'].includes(savedOp)) {
      setOp(savedOp)
    }
    if (existingPreset) {
      if (typeof existingPreset === 'string') {
        // Legacy string preset — convert
        const converted = parseLegacyPreset(existingPreset)
        if (converted) { setMode('preset'); setConfig(converted) }
      } else if (typeof existingPreset === 'object') {
        // New structured preset
        setMode('preset'); setConfig(existingPreset as PresetConfig)
      }
      return
    }
    // Hydrate custom mode from explicit dates
    const gte = where?.[`${field}__gte`] as string | undefined
    const gt = where?.[`${field}__gt`] as string | undefined
    const lt = where?.[`${field}__lt`] as string | undefined
    const lte = where?.[`${field}__lte`] as string | undefined
    const ne = where?.[`${field}__ne`] as string | undefined
    const eq = where?.[field]
    if (gte || gt || lt || lte || ne || eq) {
      setMode('custom')
      if (gte && lt) { setOp('between'); setA(gte); try { const d = new Date(`${lt}T00:00:00`); d.setDate(d.getDate()-1); setB(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`) } catch {} }
      else if (gte && !lt) { setOp('gte'); setA(gte) }
      else if (gt) { setOp('gt'); setA(gt) }
      else if (lt && !gte) { setOp('lt'); setA(lt) }
      else if (lte) { setOp('lte'); setA(lte) }
      else if (ne) { setOp('ne'); setA(String(ne)) }
      else if (eq) { setOp('eq'); setA(String(eq)) }
    }
  }, [field])

  // On config change: match quick pick
  useEffect(() => {
    const match = matchQuickPick(config)
    setSelectedQuickPick(match?.label ?? null)
  }, [config])

  const applyPreset = (cfg: PresetConfig, operator?: DateOp) => {
    const effectiveOp = operator ?? op
    const patch: Record<string, any> = {
      [`${field}__gte`]: undefined,
      [`${field}__gt`]: undefined,
      [`${field}__lt`]: undefined,
      [`${field}__lte`]: undefined,
      [`${field}__ne`]: undefined,
      [field]: undefined,
      [`${field}__date_preset`]: cfg,
      [`${field}__op`]: effectiveOp,
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
      [`${field}__op`]: op,
    }
    const has = (x: string) => x && x.trim() !== ''
    switch (op) {
      case 'eq':
        if (has(a)) {
          patch[`${field}__gte`] = a
          const nextDay = new Date(`${a}T00:00:00`); nextDay.setDate(nextDay.getDate() + 1)
          patch[`${field}__lt`] = `${nextDay.getFullYear()}-${String(nextDay.getMonth()+1).padStart(2,'0')}-${String(nextDay.getDate()).padStart(2,'0')}`
        }
        break
      case 'ne':
        if (has(a)) patch[field] = a
        break
      case 'gt':
        if (has(a)) {
          const nextDay = new Date(`${a}T00:00:00`); nextDay.setDate(nextDay.getDate() + 1)
          patch[`${field}__gt`] = `${nextDay.getFullYear()}-${String(nextDay.getMonth()+1).padStart(2,'0')}-${String(nextDay.getDate()).padStart(2,'0')}`
        }
        break
      case 'gte':
        if (has(a)) patch[`${field}__gte`] = a
        break
      case 'lt':
        if (has(a)) patch[`${field}__lt`] = a
        break
      case 'lte':
        if (has(a)) {
          const nextDay = new Date(`${a}T00:00:00`); nextDay.setDate(nextDay.getDate() + 1)
          patch[`${field}__lt`] = `${nextDay.getFullYear()}-${String(nextDay.getMonth()+1).padStart(2,'0')}-${String(nextDay.getDate()).padStart(2,'0')}`
        }
        break
      case 'between':
        if (has(a)) patch[`${field}__gte`] = a
        if (has(b)) {
          const nextDay = new Date(`${b}T00:00:00`); nextDay.setDate(nextDay.getDate() + 1)
          patch[`${field}__lt`] = `${nextDay.getFullYear()}-${String(nextDay.getMonth()+1).padStart(2,'0')}-${String(nextDay.getDate()).padStart(2,'0')}`
        }
        break
    }
    onPatch(patch)
  }

  // Group quick picks for the dropdown
  const groups = QUICK_PICKS.reduce<Record<string, QuickPick[]>>((acc, qp) => {
    (acc[qp.group] ??= []).push(qp); return acc
  }, {})

  return (
    <div className="rounded-md border bg-card p-2 space-y-2">
      <div className="flex items-center justify-between">
        <div className="text-[11px] font-medium">Date rule: {field}</div>
        <button className="text-[10px] px-1.5 py-0.5 rounded border hover:bg-muted" onClick={() => { setMode('preset'); setConfig({ ...DEFAULT_PRESET }); setOp('eq'); setA(''); setB(''); onPatch({ [field]: undefined, [`${field}__gte`]: undefined, [`${field}__gt`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined, [`${field}__ne`]: undefined, [`${field}__date_preset`]: undefined, [`${field}__op`]: undefined }) }}>Clear</button>
      </div>
      <div className="flex items-center gap-3 text-[11px]">
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='preset'} onChange={() => setMode('preset')} /> Preset</label>
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='custom'} onChange={() => setMode('custom')} /> Custom</label>
      </div>
      
      {/* Operator selector - shown for both modes */}
      <select className="w-full px-2 py-1 rounded bg-secondary/60 text-[11px]" value={op} onChange={e => { const newOp = e.target.value as DateOp; setOp(newOp); if (mode === 'preset') applyPreset(config, newOp) }}>
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
          {/* Quick Pick dropdown */}
          <select className="w-full px-2 py-1 rounded bg-secondary/60 text-[11px]" value={selectedQuickPick ?? ''} onChange={e => {
            const label = e.target.value
            const qp = QUICK_PICKS.find(q => q.label === label)
            if (qp) { setConfig({ ...qp.config }); applyPreset(qp.config) }
          }}>
            {!selectedQuickPick && <option value="">— Select a preset —</option>}
            {Object.entries(groups).map(([group, picks]) => (
              <optgroup key={group} label={group}>
                {picks.map(qp => <option key={qp.label} value={qp.label}>{qp.label}</option>)}
              </optgroup>
            ))}
          </select>

          {/* Composable dimension controls */}
          <div className="grid grid-cols-2 gap-1.5">
            <div>
              <span className="text-[10px] text-muted-foreground">Period</span>
              <select className="w-full px-1.5 py-0.5 rounded bg-secondary/60 text-[11px]" value={config.period} onChange={e => { const next = { ...config, period: e.target.value as any }; setConfig(next); applyPreset(next) }}>
                {PERIOD_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </div>
            <div>
              <span className="text-[10px] text-muted-foreground">Offset</span>
              <select className="w-full px-1.5 py-0.5 rounded bg-secondary/60 text-[11px]" value={config.offset} onChange={e => { const next = { ...config, offset: e.target.value as any }; setConfig(next); applyPreset(next) }}>
                {OFFSET_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </div>
            <div>
              <span className="text-[10px] text-muted-foreground">As Of</span>
              <select className="w-full px-1.5 py-0.5 rounded bg-secondary/60 text-[11px]" value={config.as_of} onChange={e => { const next = { ...config, as_of: e.target.value as any }; setConfig(next); applyPreset(next) }}>
                {AS_OF_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </div>
            <div>
              <span className="text-[10px] text-muted-foreground">Range Mode</span>
              <select className="w-full px-1.5 py-0.5 rounded bg-secondary/60 text-[11px]" value={config.range_mode} onChange={e => { const next = { ...config, range_mode: e.target.value as any }; setConfig(next); applyPreset(next) }}>
                {RANGE_MODE_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <label className="inline-flex items-center gap-1 text-[10px]">
              <input type="checkbox" checked={config.include_weekends} onChange={e => { const next = { ...config, include_weekends: e.target.checked }; setConfig(next); applyPreset(next) }} />
              <span className="text-muted-foreground">Include Weekends</span>
            </label>
            <label className="inline-flex items-center gap-1 text-[10px]">
              <input type="checkbox" checked={config.apply_holidays} onChange={e => { const next = { ...config, apply_holidays: e.target.checked }; setConfig(next); applyPreset(next) }} />
              <span className="text-muted-foreground">Apply Holidays</span>
            </label>
          </div>
          {preview.label && <p className="text-[10px] text-muted-foreground">{preview.loading ? 'Resolving…' : preview.label}</p>}
          <button className="text-[10px] px-2 py-0.5 rounded bg-primary text-primary-foreground hover:bg-primary/90" onClick={() => applyPreset(config)}>Apply</button>
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
  const initialTab: 'manual'|'rule' = Array.isArray(where?.[field]) && (where[field] as any[]).length > 0 ? 'manual' : 'rule'
  const [tab, setTab] = useState<'manual'|'rule'>(initialTab)
  const [samples, setSamples] = useState<string[]>([])

  // Only fetch samples for type detection when DB type is unavailable AND the field name
  // gives no signal — avoids a /distinct call for obvious date/number field names like "Time"
  const _nameKind = (() => {
    const hasDateOps = ['__gte', '__lte', '__gt', '__lt'].some(op => where[`${field}${op}`] != null)
    const nameLooksDate = /(date|time|timestamp|_at$|created$|updated$)/i.test(field)
    if (hasDateOps || nameLooksDate) return 'date'
    const nameLooksNum = /(amount|price|qty|quantity|count|total|sum|avg|rate|value|balance|profit|volume|commission|fee|margin|score|age|year|month|week|day|hour|minute|second)/i.test(field)
    if (nameLooksNum) return 'number'
    return null
  })()
  useEffect(() => {
    if (dbType) return  // DB type known — skip sample fetch for type detection
    if (_nameKind) return  // field name is sufficient — skip fetch
    if (tab === 'rule') return  // don't detect kind eagerly — wait until user opens manual tab
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
  }, [field, source, datasourceId, dbType, _nameKind, tab])

  let kind: 'date' | 'number' | 'string' = detectKindFromDbType(dbType) ?? _nameKind ?? detectFieldKind(samples)

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

// ─── Image Properties Sub-component ─────────────────────────────────
function ImageProps({ element, onUpdate }: { element: ReportElement; onUpdate: (el: ReportElement) => void }) {
  const img = element.image || { url: '', objectFit: 'contain' as const }
  const patch = (p: Partial<typeof img>) => onUpdate({ ...element, image: { ...img, ...p } })
  const fileInputRef = useRef<HTMLInputElement | null>(null)
  const [tab, setTab] = useState<'url' | 'upload'>(img.url?.startsWith('data:') ? 'upload' : 'url')

  function handleFile(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = () => { patch({ url: reader.result as string }) }
    reader.readAsDataURL(file)
  }

  return (
    <div className="space-y-2">
      {/* URL / Upload toggle */}
      <div className="flex rounded border overflow-hidden h-6 text-[11px]">
        <button
          className={`flex-1 transition-colors ${tab === 'url' ? 'bg-primary text-primary-foreground' : 'bg-secondary/60 text-muted-foreground hover:bg-secondary'}`}
          onClick={() => setTab('url')}
        >URL</button>
        <button
          className={`flex-1 border-l transition-colors ${tab === 'upload' ? 'bg-primary text-primary-foreground' : 'bg-secondary/60 text-muted-foreground hover:bg-secondary'}`}
          onClick={() => setTab('upload')}
        >Upload</button>
      </div>

      {tab === 'url' ? (
        <div>
          <label className="block text-[11px] text-muted-foreground mb-1">Image URL</label>
          <input
            className="w-full h-7 text-xs rounded border bg-secondary/60 px-2"
            value={img.url?.startsWith('data:') ? '' : (img.url || '')}
            onChange={(e) => patch({ url: e.target.value })}
            placeholder="https://..."
          />
        </div>
      ) : (
        <div>
          <label className="block text-[11px] text-muted-foreground mb-1">Image File</label>
          <input
            ref={fileInputRef}
            type="file"
            accept="image/*"
            className="hidden"
            onChange={handleFile}
          />
          <button
            className="w-full h-7 text-xs rounded border bg-secondary/60 px-2 flex items-center gap-2 hover:bg-secondary transition-colors"
            onClick={() => fileInputRef.current?.click()}
          >
            <RiImageLine className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            <span className="truncate text-muted-foreground">
              {img.url?.startsWith('data:') ? 'Replace image…' : 'Choose file…'}
            </span>
          </button>
          {img.url?.startsWith('data:') && (
            <div className="mt-1.5 rounded border overflow-hidden bg-secondary/40 flex items-center justify-center" style={{ height: 64 }}>
              <img src={img.url} alt="preview" className="max-h-full max-w-full object-contain" />
            </div>
          )}
        </div>
      )}

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

// ─── Element Property Panel ──────────────────────────────────────────
function ElementProps({
  element,
  variables,
  allWidgets,
  onUpdate,
}: {
  element: ReportElement
  variables: ReportVariable[]
  allWidgets?: Record<string, WidgetConfig>
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

  if (element.type === 'chart') {
    const chartWidgets = Object.values(allWidgets || {}).filter(w => w.type === 'chart' || w.type === 'kpi')
    const current = element.chart
    return (
      <div className="space-y-3">
        <div>
          <label className="block text-[11px] text-muted-foreground mb-1">Link to Dashboard Widget</label>
          <select
            className="w-full h-7 text-xs rounded border bg-secondary/60 px-2 focus:ring-1 focus:ring-primary/40 outline-none"
            value={current?.widgetId || ''}
            onChange={(e) => {
              const wid = e.target.value
              const w = allWidgets?.[wid]
              onUpdate({ ...element, chart: w ? { widgetId: wid, title: w.title, widgetType: w.type } : undefined })
            }}
          >
            <option value="">— Select a widget —</option>
            {chartWidgets.map(w => (
              <option key={w.id} value={w.id}>{w.title} ({w.type})</option>
            ))}
          </select>
          {chartWidgets.length === 0 && (
            <p className="text-[10px] text-muted-foreground/70 mt-1">No chart or KPI widgets found on this dashboard.</p>
          )}
        </div>
        {current?.widgetId && (
          <div className="rounded-md border bg-primary/5 px-3 py-2 flex items-start gap-2">
            <div className="flex-1 min-w-0">
              <p className="text-[10px] text-primary font-medium truncate">Linked: {current.title || current.widgetId}</p>
              <p className="text-[10px] text-muted-foreground mt-0.5">Renders live inside the report.</p>
            </div>
            <button
              title="Unlink widget"
              className="shrink-0 text-destructive/70 hover:text-destructive hover:bg-destructive/10 rounded p-0.5 transition-colors"
              onClick={() => onUpdate({ ...element, chart: undefined })}
            >
              <RiDeleteBinLine className="h-3.5 w-3.5" />
            </button>
          </div>
        )}
      </div>
    )
  }

  if (element.type === 'image') {
    return <ImageProps element={element} onUpdate={onUpdate} />
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
    const insertColAt = (afterHi: number) => {
      // Calculate the actual column index after header[afterHi] (sum of colspans 0..afterHi)
      const actualInsertIdx = tbl.headers.slice(0, afterHi + 1).reduce((sum, h) => {
        const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
        return sum + (header.colspan || 1)
      }, 0)
      const newHeaders = [
        ...tbl.headers.slice(0, afterHi + 1),
        { text: '', colspan: 1 },
        ...tbl.headers.slice(afterHi + 1),
      ]
      const cells = tbl.cells.map((row) => [
        ...row.slice(0, actualInsertIdx),
        { type: 'text' as const, text: '' },
        ...row.slice(actualInsertIdx),
      ])
      const newTotalCols = newHeaders.reduce((sum, h) => {
        const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
        return sum + (header.colspan || 1)
      }, 0)
      let subheaders = tbl.subheaders
      if (subheaders) {
        subheaders = [
          ...subheaders.slice(0, actualInsertIdx),
          '',
          ...subheaders.slice(actualInsertIdx),
        ]
      }
      patchTbl({ cols: tbl.cols + 1, headers: newHeaders, cells, ...(subheaders && { subheaders }) })
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
          <div className="space-y-0.5">
            {tbl.headers.map((h, ci) => {
              const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
              return (
                <React.Fragment key={ci}>
                  <div className="flex items-center gap-1">
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
                  {ci < tbl.headers.length - 1 && (
                    <div className="flex items-center gap-1 pl-1">
                      <button
                        className="text-[9px] text-muted-foreground hover:text-foreground hover:bg-muted px-1 rounded border border-dashed border-muted-foreground/40 leading-4"
                        title="Insert header after this one"
                        onClick={() => insertColAt(ci)}
                      >+ insert</button>
                    </div>
                  )}
                </React.Fragment>
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
                        <div className="flex-1 min-w-0 h-5 flex items-center border rounded bg-secondary/60 px-0.5">
                          <CellVarSelect
                            value={cell.variableId || ''}
                            variables={variables}
                            onChange={(id) => updateCell(ri, ci, { variableId: id })}
                          />
                        </div>
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
const PERIOD_PRESETS: { value: string; label: string; group: string }[] = QUICK_PICKS.map(qp => ({
  value: JSON.stringify(qp.config),
  label: qp.label,
  group: qp.group,
}))
const PERIOD_LABEL: Record<string, string> = Object.fromEntries(PERIOD_PRESETS.map(p => [p.value, p.label]))
const PERIOD_GROUPS = [...new Set(PERIOD_PRESETS.map(p => p.group))]

// ─── Inline Table Editor ─────────────────────────────────────────────
function CellVarSelect({ value, variables, onChange }: {
  value: string
  variables: ReportVariable[]
  onChange: (id: string) => void
}) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState('')
  const containerRef = useRef<HTMLDivElement>(null)
  const searchRef = useRef<HTMLInputElement>(null)

  useEffect(() => {
    if (!open) { setSearch(''); return }
    setTimeout(() => searchRef.current?.focus(), 0)
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  const selected = variables.find(v => v.id === value)
  const filtered = search
    ? variables.filter(v => v.name.toLowerCase().includes(search.toLowerCase()))
    : variables

  return (
    <div ref={containerRef} className="relative flex-1 min-w-0">
      <button
        type="button"
        className="w-full flex items-center gap-0.5 bg-transparent text-[9px] font-mono text-left min-w-0 outline-none"
        onClick={(e) => { e.stopPropagation(); setOpen(o => !o) }}
        onMouseDown={(e) => e.stopPropagation()}
        title={selected?.name || 'Select variable…'}
      >
        <span className="flex-1 truncate">{selected ? selected.name : <span className="text-muted-foreground italic">Select…</span>}</span>
        <RiArrowDownSLine className="h-3 w-3 shrink-0 text-muted-foreground" />
      </button>
      {open && (
        <div
          className="absolute z-[60] left-0 top-full mt-0.5 border rounded-lg shadow-xl min-w-[180px] w-max max-w-[260px]"
          style={{ backgroundColor: 'hsl(var(--card))' }}
          onMouseDown={(e) => e.stopPropagation()}
          onClick={(e) => e.stopPropagation()}
        >
          <div className="p-1.5 border-b">
            <input
              ref={searchRef}
              className="w-full h-6 text-[10px] rounded border bg-secondary/60 px-1.5 outline-none focus:ring-1 focus:ring-primary/40"
              placeholder="Search variables…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
          <div className="max-h-52 overflow-y-auto py-0.5">
            <button
              type="button"
              className="w-full text-left text-[9px] font-mono px-2 py-1 hover:bg-muted text-muted-foreground"
              onClick={() => { onChange(''); setOpen(false) }}
            >
              (none)
            </button>
            {filtered.length === 0 ? (
              <div className="text-[9px] text-muted-foreground text-center py-3 italic">No results</div>
            ) : filtered.map(v => (
              <button
                key={v.id}
                type="button"
                className={`w-full text-left text-[9px] font-mono px-2 py-1 hover:bg-muted truncate block ${v.id === value ? 'text-primary font-semibold bg-primary/5' : ''}`}
                onClick={() => { onChange(v.id); setOpen(false) }}
              >
                {v.name}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

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
  const [deleteConfirmCol, setDeleteConfirmCol] = useState<number | null>(null)
  const [normalized, setNormalized] = useState(false)
  const tableRef = useRef<HTMLTableElement | null>(null)
  const resizeDragRef = useRef<{ col: number; startX: number; startWidths: number[] } | null>(null)

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
    const cells = table.cells.map((row, r) => row.map((cell, c) => {
      if (!(r === ri && c === ci)) return cell
      // If patch.style is explicitly undefined (reset), clear it.
      // If patch.style is provided, deep-merge onto the current cell.style so
      // partial style patches don't overwrite sibling fields (e.g. setting
      // numberFormat doesn't drop a previously-set fontSize).
      if ('style' in patch) {
        if (patch.style === undefined) {
          const { style: _omit, ...rest } = cell
          return { ...rest, ...patch, style: undefined }
        }
        return { ...cell, ...patch, style: { ...(cell.style || {}), ...patch.style } }
      }
      return { ...cell, ...patch }
    }))
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
    const actualColsLocal = table.headers.reduce((sum, h) => {
      const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
      return sum + (header.colspan || 1)
    }, 0)
    const newRow: ReportTableCell[] = Array.from({ length: actualColsLocal }, () => ({ type: 'text' as const, text: '' }))
    const newCells = [...table.cells.slice(0, ri), newRow, ...table.cells.slice(ri)]
    const newRowStyles = table.rowStyles ? [...table.rowStyles.slice(0, ri), {}, ...table.rowStyles.slice(ri)] : undefined
    onChange({ ...table, rows: table.rows + 1, cells: newCells, ...(newRowStyles ? { rowStyles: newRowStyles } : {}) })
    setCellMenuOpen(null)
  }

  const swapRows = (a: number, b: number) => {
    if (a === b || a < 0 || b < 0 || a >= table.cells.length || b >= table.cells.length) return
    const newCells = table.cells.slice()
    ;[newCells[a], newCells[b]] = [newCells[b], newCells[a]]
    let newRowStyles: typeof table.rowStyles | undefined = undefined
    if (table.rowStyles) {
      newRowStyles = table.rowStyles.slice()
      const sa = newRowStyles[a] || {}
      const sb = newRowStyles[b] || {}
      newRowStyles[a] = sb
      newRowStyles[b] = sa
    }
    onChange({ ...table, cells: newCells, ...(newRowStyles ? { rowStyles: newRowStyles } : {}) })
    setCellMenuOpen(null)
  }

  const moveRowUp = (ri: number) => swapRows(ri, ri - 1)
  const moveRowDown = (ri: number) => swapRows(ri, ri + 1)

  const deleteColumn = (ci: number) => {
    if (table.cols <= 1) return
    // Walk headers to find which header owns absolute column ci
    let hiOwner = -1
    let runningStart = 0
    for (let hi = 0; hi < table.headers.length; hi++) {
      const h = table.headers[hi]
      const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
      const span = header.colspan || 1
      if (ci >= runningStart && ci < runningStart + span) {
        hiOwner = hi
        break
      }
      runningStart += span
    }
    if (hiOwner < 0) return

    const ownerHdr = (() => {
      const h = table.headers[hiOwner]
      return typeof h === 'string' ? { text: h, colspan: 1 } : h
    })()
    const ownerSpan = ownerHdr.colspan || 1

    let newHeaders
    if (ownerSpan > 1) {
      newHeaders = table.headers.map((h, i) => {
        if (i !== hiOwner) return h
        const hdr = typeof h === 'string' ? { text: h, colspan: 1 } : h
        return { ...hdr, colspan: (hdr.colspan || 1) - 1 }
      })
    } else {
      newHeaders = table.headers.filter((_, i) => i !== hiOwner)
    }

    const newCells = table.cells.map(row => row.filter((_, c) => c !== ci))
    const newSubheaders = table.subheaders ? table.subheaders.filter((_, c) => c !== ci) : undefined
    const newColWidths = table.colWidths ? table.colWidths.filter((_, c) => c !== ci) : undefined

    onChange({
      ...table,
      cols: table.cols - 1,
      headers: newHeaders,
      cells: newCells,
      ...(newSubheaders ? { subheaders: newSubheaders } : {}),
      ...(newColWidths ? { colWidths: newColWidths } : {}),
    })
    setCellMenuOpen(null)
    setDeleteConfirmCol(null)
  }

  const actualCols = table.headers.reduce((sum, h) => {
    const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
    return sum + (header.colspan || 1)
  }, 0)
  const headerAbsLastCols: number[] = []
  {
    let ci = 0
    for (const h of table.headers) {
      const header = typeof h === 'string' ? { colspan: 1 } : h
      ci += header.colspan || 1
      headerAbsLastCols.push(ci - 1)
    }
  }

  function startColResize(e: React.MouseEvent, absColIdx: number) {
    e.preventDefault(); e.stopPropagation()
    const tbl = tableRef.current
    if (!tbl) return
    const totalW = tbl.offsetWidth
    const existing = table.colWidths || []
    const pixWidths = Array.from({ length: actualCols }, (_, i) => {
      const pct = existing[i] || 0
      return pct > 0 ? (pct / 100) * totalW : totalW / actualCols
    })
    const dragState = { col: absColIdx, startX: e.clientX, startWidths: pixWidths }
    resizeDragRef.current = dragState
    function onMove(ev: MouseEvent) {
      if (!resizeDragRef.current || !tableRef.current) return
      const dx = ev.clientX - dragState.startX
      const newWidths = [...dragState.startWidths]
      newWidths[dragState.col] = Math.max(30, newWidths[dragState.col] + dx)
      const cols = tableRef.current.querySelectorAll<HTMLElement>('col')
      newWidths.forEach((w, i) => { if (cols[i]) cols[i].style.width = `${w}px` })
    }
    function onUp(ev: MouseEvent) {
      resizeDragRef.current = null
      document.removeEventListener('mousemove', onMove)
      document.removeEventListener('mouseup', onUp)
      if (!tableRef.current) return
      const totalW2 = tableRef.current.offsetWidth
      const dx = ev.clientX - dragState.startX
      const newWidths = [...dragState.startWidths]
      newWidths[dragState.col] = Math.max(30, newWidths[dragState.col] + dx)
      const pctWidths = newWidths.map(w => Math.round((w / totalW2) * 1000) / 10)
      onChange({ ...table, colWidths: pctWidths })
    }
    document.addEventListener('mousemove', onMove)
    document.addEventListener('mouseup', onUp)
  }

  return (
    <table ref={tableRef} className="w-full border-collapse text-[11px]" style={{ tableLayout: 'fixed', borderWidth: '1px', borderStyle: table.borderStyle || 'solid', borderColor: table.borderColor || 'hsl(var(--border))' }}>
      <colgroup>
        {Array.from({ length: actualCols }, (_, i) => (
          <col key={i} style={{ width: table.colWidths?.[i] ? `${table.colWidths[i]}%` : undefined }} />
        ))}
      </colgroup>
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
                  position: 'relative',
                  backgroundColor: table.headerBg || 'hsl(var(--secondary))',
                  borderWidth: '1px',
                  borderStyle: table.borderStyle || 'solid',
                  borderColor: table.borderColor || 'hsl(var(--border))',
                  color: table.headerColor || 'hsl(var(--foreground))',
                  fontSize: table.headerFontSize ? `${table.headerFontSize}px` : '12px',
                  fontWeight: table.headerFontWeight === 'semibold' ? 600 : table.headerFontWeight === 'bold' || !table.headerFontWeight ? 700 : 400,
                  textAlign: (table.headerAlign as any) || 'left',
                  verticalAlign: (table.headerVerticalAlign as any) || 'middle',
                  height: table.headerHeight ? `${table.headerHeight}px` : undefined,
                  whiteSpace: 'nowrap',
                  overflow: 'hidden',
                }}
              >
                <div className="flex items-center gap-1 overflow-hidden">
                  <input
                    className="flex-1 bg-transparent border-none outline-none font-[inherit] text-[inherit] min-w-0"
                    style={{ textAlign: (table.headerAlign as any) || 'left' }}
                    value={header.text}
                    onChange={(e) => updateHeader(ci, e.target.value)}
                    placeholder={`Header ${ci + 1}`}
                    onClick={(e) => e.stopPropagation()}
                  />
                  {(header.colspan || 1) > 1 && (
                    <span className="text-[8px] text-muted-foreground bg-primary/10 px-1 rounded shrink-0" title={`Spans ${header.colspan} columns`}>
                      ×{header.colspan}
                    </span>
                  )}
                  <input
                    type="number"
                    min="1"
                    max={table.cols}
                    className="w-8 bg-transparent border border-border/40 rounded text-[8px] px-1 py-0.5 shrink-0"
                    value={header.colspan || 1}
                    onChange={(e) => updateHeaderColspan(ci, +e.target.value)}
                    onClick={(e) => e.stopPropagation()}
                    title="Column span"
                  />
                </div>
                <div
                  style={{ position: 'absolute', right: 0, top: 0, bottom: 0, width: 5, cursor: 'col-resize', zIndex: 10 }}
                  onMouseDown={(e) => startColResize(e, headerAbsLastCols[ci])}
                  className="hover:bg-primary/40"
                />
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
                  position: 'relative',
                  backgroundColor: table.subheaderBg || table.headerBg || 'hsl(var(--secondary))',
                  borderWidth: '1px',
                  borderStyle: table.borderStyle || 'solid',
                  borderColor: table.borderColor || 'hsl(var(--border))',
                  color: table.subheaderColor || table.headerColor || 'hsl(var(--foreground))',
                  fontSize: table.subheaderFontSize ? `${table.subheaderFontSize}px` : '11px',
                  fontWeight: table.subheaderFontWeight === 'bold' ? 700 : table.subheaderFontWeight === 'semibold' ? 600 : 400,
                  textAlign: (table.subheaderAlign as any) || 'left',
                  verticalAlign: (table.subheaderVerticalAlign as any) || 'middle',
                  whiteSpace: 'nowrap',
                  overflow: 'hidden',
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
                <div
                  style={{ position: 'absolute', right: 0, top: 0, bottom: 0, width: 5, cursor: 'col-resize', zIndex: 10 }}
                  onMouseDown={(e) => startColResize(e, actualCol)}
                  className="hover:bg-primary/40"
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
                    <CellVarSelect
                      value={cell.variableId || ''}
                      variables={variables}
                      onChange={(id) => updateCell(ri, ci, { variableId: id })}
                    />
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
                        <button
                          className={`w-full text-[9px] text-left px-2 py-1 rounded transition-colors flex items-center gap-1.5 ${ri === 0 ? 'opacity-40 cursor-not-allowed text-muted-foreground' : 'hover:bg-muted'}`}
                          disabled={ri === 0}
                          onClick={() => { moveRowUp(ri); setDeleteConfirmRow(null) }}>
                          <RiArrowUpLine className="h-3 w-3 shrink-0" />Move row up
                        </button>
                        <button
                          className={`w-full text-[9px] text-left px-2 py-1 rounded transition-colors flex items-center gap-1.5 ${ri >= table.rows - 1 ? 'opacity-40 cursor-not-allowed text-muted-foreground' : 'hover:bg-muted'}`}
                          disabled={ri >= table.rows - 1}
                          onClick={() => { moveRowDown(ri); setDeleteConfirmRow(null) }}>
                          <RiArrowDownLine className="h-3 w-3 shrink-0" />Move row down
                        </button>
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
                        {(() => {
                          // Determine what deleting this col means: whole column vs. subheader only
                          let runningStart = 0
                          let ownerSpan = 1
                          for (const h of table.headers) {
                            const hdr = typeof h === 'string' ? { text: h, colspan: 1 } : h
                            const span = hdr.colspan || 1
                            if (ci >= runningStart && ci < runningStart + span) { ownerSpan = span; break }
                            runningStart += span
                          }
                          const isWhole = ownerSpan <= 1
                          const confirmMsg = isWhole ? 'Delete this column?' : 'Delete this sub-column?'
                          const btnLabel = isWhole ? 'Delete column' : 'Delete sub-column'
                          return deleteConfirmCol === ci ? (
                            <div className="flex items-center gap-1">
                              <span className="text-[9px] text-destructive flex-1">{confirmMsg}</span>
                              <button className="text-[9px] px-1.5 py-0.5 rounded bg-destructive text-destructive-foreground hover:opacity-90 transition-opacity"
                                onClick={() => deleteColumn(ci)}>Yes</button>
                              <button className="text-[9px] px-1.5 py-0.5 rounded border hover:bg-muted transition-colors"
                                onClick={() => setDeleteConfirmCol(null)}>No</button>
                            </div>
                          ) : (
                            <button
                              className={`w-full text-[9px] text-left px-2 py-1 rounded transition-colors flex items-center gap-1.5 ${
                                table.cols <= 1 ? 'opacity-40 cursor-not-allowed text-muted-foreground' : 'hover:bg-destructive/10 text-destructive'
                              }`}
                              disabled={table.cols <= 1}
                              onClick={() => { setDeleteConfirmCol(ci); setDeleteConfirmRow(null) }}>
                              <RiDeleteBinLine className="h-3 w-3 shrink-0" />{btnLabel}
                            </button>
                          )
                        })()}
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
                {el.type === 'chart' && (
                  <div className="h-full flex flex-col items-center justify-center gap-1 text-[11px] text-muted-foreground">
                    <RiBarChart2Line className="h-5 w-5 text-[hsl(var(--chart-2))]" />
                    <span className="text-[10px] text-center px-1 truncate w-full text-center">{el.chart?.title || 'Unlinked Chart'}</span>
                  </div>
                )}
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
                    <table className="w-full border-collapse text-[11px]" style={{ tableLayout: 'fixed', borderWidth: '1px', borderStyle: el.table.borderStyle || 'solid', borderColor: el.table.borderColor || 'hsl(var(--border))' }}>
                      <colgroup>
                        {(() => {
                          const previewCols = el.table!.headers.reduce((s, h) => s + ((typeof h === 'string' ? 1 : h.colspan) || 1), 0)
                          return Array.from({ length: previewCols }, (_, i) => (
                            <col key={i} style={{ width: el.table!.colWidths?.[i] ? `${el.table!.colWidths[i]}%` : undefined }} />
                          ))
                        })()}
                      </colgroup>
                      <thead>
                        <tr>
                          {el.table.headers.map((h, ci) => {
                            const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
                            return (
                              <th key={ci} colSpan={header.colspan || 1} className="px-1 py-0.5" style={{
                                backgroundColor: el.table!.headerBg || 'hsl(var(--secondary))',
                                borderWidth: '1px', borderStyle: el.table!.borderStyle || 'solid', borderColor: el.table!.borderColor || 'hsl(var(--border))',
                                color: el.table!.headerColor || 'hsl(var(--foreground))',
                                fontSize: el.table!.headerFontSize ? `${el.table!.headerFontSize}px` : '12px',
                                fontWeight: el.table!.headerFontWeight === 'semibold' ? 600 : el.table!.headerFontWeight === 'bold' || !el.table!.headerFontWeight ? 700 : 400,
                                textAlign: (el.table!.headerAlign as any) || 'left',
                                verticalAlign: (el.table!.headerVerticalAlign as any) || 'middle',
                                height: el.table!.headerHeight ? `${el.table!.headerHeight}px` : undefined,
                                whiteSpace: 'nowrap',
                                overflow: 'hidden',
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
                                color: el.table!.subheaderColor || el.table!.headerColor || 'hsl(var(--foreground))',
                                fontSize: el.table!.subheaderFontSize ? `${el.table!.subheaderFontSize}px` : '11px',
                                fontWeight: el.table!.subheaderFontWeight === 'bold' ? 700 : el.table!.subheaderFontWeight === 'semibold' ? 600 : 400,
                                textAlign: (el.table!.subheaderAlign as any) || 'left',
                                verticalAlign: (el.table!.subheaderVerticalAlign as any) || 'middle',
                                whiteSpace: 'nowrap',
                                overflow: 'hidden',
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
  allWidgets,
}: {
  open: boolean
  onCloseAction: () => void
  config: WidgetConfig
  onSaveAction: (next: WidgetConfig) => void
  allWidgets?: Record<string, WidgetConfig>
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
  const [leftTab, setLeftTab] = useState<'elements' | 'variables' | 'settings'>('elements')
  const [confirmDeleteVarId, setConfirmDeleteVarId] = useState<string | null>(null)
  const [varSearch, setVarSearch] = useState('')
  const [renamingVarId, setRenamingVarId] = useState<string | null>(null)
  const [renameValue, setRenameValue] = useState('')
  const renameInputRef = useRef<HTMLInputElement>(null)
  const varListRef = useRef<HTMLDivElement>(null)

  // Suspend background query recalculations while the builder is open
  useEffect(() => {
    if (typeof window === 'undefined') return
    window.dispatchEvent(new CustomEvent(open ? 'report-builder-open' : 'report-builder-close'))
    return () => {
      if (open) window.dispatchEvent(new CustomEvent('report-builder-close'))
    }
  }, [open])

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
    } else if (type === 'chart') {
      base.gridW = 6; base.gridH = 6
    }
    setState((s) => ({ ...s, elements: [...s.elements, base] }))
    setSelectedId(id)
  }, [])

  const addVariable = useCallback(() => {
    const id = genId()
    const name = `Var${state.variables.length + 1}`
    const v: ReportVariable = { id, name, value: { field: '', agg: 'sum' }, format: 'wholeNumber' }
    setState((s) => ({ ...s, variables: [...s.variables, v] }))
    setLeftTab('variables')
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
        <div className="flex items-center justify-between px-4 py-2.5 border-b bg-card shrink-0">
          <div className="flex items-center gap-3">
            <button onClick={onCloseAction} className="text-muted-foreground hover:text-foreground p-1.5 rounded-md hover:bg-secondary transition-colors" title="Close">
              <RiArrowLeftLine className="h-4 w-4" />
            </button>
            <div className="h-5 w-px bg-border" />
            <div className="flex items-center gap-2">
              <div className="flex items-center justify-center h-7 w-7 rounded-lg bg-primary/10">
                <RiSettings3Line className="h-4 w-4 text-primary" />
              </div>
              <div>
                <h2 className="text-sm font-semibold leading-tight">Report Builder</h2>
                <span className="text-[10px] text-muted-foreground leading-none">{config.title} · {state.elements.length} elements · {state.variables.length} variables</span>
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button className="flex items-center gap-1.5 text-xs font-medium px-4 py-2 rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 shadow-sm transition-all active:scale-[0.97]" onClick={handleSave}>
              <RiSave3Line className="h-3.5 w-3.5" /> Save Report
            </button>
          </div>
        </div>

        {/* Body: Rail | Drawer | Canvas | Smart Panel */}
        <div className="flex flex-1 min-h-0 overflow-hidden">

          {/* Icon Rail */}
          <div className="w-[52px] border-r bg-card/60 flex flex-col items-center py-2 gap-1 shrink-0">
            {(['elements', 'variables', 'settings'] as const).map((tab, i) => (
              <React.Fragment key={tab}>
                {i === 2 && <div className="w-7 h-px bg-border my-1" />}
                <button
                  className={`w-9 h-9 rounded-lg border flex flex-col items-center justify-center gap-0.5 transition-all ${leftTab === tab ? 'bg-primary/10 border-primary/30 text-primary' : 'border-transparent text-muted-foreground hover:bg-secondary hover:text-foreground'}`}
                  onClick={() => setLeftTab(tab)}
                  title={tab.charAt(0).toUpperCase() + tab.slice(1)}
                >
                  {tab === 'elements' && <RiTableLine className="h-3.5 w-3.5" />}
                  {tab === 'variables' && <RiHashtag className="h-3.5 w-3.5" />}
                  {tab === 'settings' && <RiSettings3Line className="h-3.5 w-3.5" />}
                  <span className="text-[7px] font-semibold tracking-wide leading-none capitalize">{tab}</span>
                </button>
              </React.Fragment>
            ))}
          </div>

          {/* Drawer */}
          <div className="w-52 border-r bg-card/40 flex flex-col overflow-hidden shrink-0">

            {/* Elements tab */}
            {leftTab === 'elements' && (<>
              <div className="px-3 py-2 border-b bg-card/60 shrink-0">
                <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">Add Element</div>
              </div>
              <div className="p-2.5 border-b shrink-0">
                <div className="grid grid-cols-2 gap-1.5">
                  <button className="flex flex-col items-center gap-1 text-[11px] px-2 py-2.5 rounded-lg border border-border/60 hover:bg-secondary hover:border-primary/30 transition-all cursor-pointer group" onClick={() => addElement('label')}>
                    <div className="flex items-center justify-center h-7 w-7 rounded-md bg-[hsl(var(--chart-1)/0.1)] group-hover:bg-[hsl(var(--chart-1)/0.2)] transition-colors"><RiText className="h-3.5 w-3.5 text-[hsl(var(--chart-1))]" /></div>
                    <span className="text-muted-foreground group-hover:text-foreground transition-colors">Label</span>
                  </button>
                  <button className="flex flex-col items-center gap-1 text-[11px] px-2 py-2.5 rounded-lg border border-border/60 hover:bg-secondary hover:border-primary/30 transition-all cursor-pointer group" onClick={() => addElement('image')}>
                    <div className="flex items-center justify-center h-7 w-7 rounded-md bg-[hsl(var(--chart-3)/0.1)] group-hover:bg-[hsl(var(--chart-3)/0.2)] transition-colors"><RiImageLine className="h-3.5 w-3.5 text-[hsl(var(--chart-3))]" /></div>
                    <span className="text-muted-foreground group-hover:text-foreground transition-colors">Image</span>
                  </button>
                  <button className="flex flex-col items-center gap-1 text-[11px] px-2 py-2.5 rounded-lg border border-border/60 hover:bg-secondary hover:border-primary/30 transition-all cursor-pointer group" onClick={() => addElement('table')}>
                    <div className="flex items-center justify-center h-7 w-7 rounded-md bg-[hsl(var(--success)/0.1)] group-hover:bg-[hsl(var(--success)/0.2)] transition-colors"><RiTableLine className="h-3.5 w-3.5 text-success" /></div>
                    <span className="text-muted-foreground group-hover:text-foreground transition-colors">Table</span>
                  </button>
                  <button className="flex flex-col items-center gap-1 text-[11px] px-2 py-2.5 rounded-lg border border-border/60 hover:bg-secondary hover:border-primary/30 transition-all cursor-pointer group" onClick={() => addElement('spaceholder')}>
                    <div className="flex items-center justify-center h-7 w-7 rounded-md bg-[hsl(var(--accent)/0.1)] group-hover:bg-[hsl(var(--accent)/0.2)] transition-colors"><RiHashtag className="h-3.5 w-3.5 text-accent" /></div>
                    <span className="text-muted-foreground group-hover:text-foreground transition-colors">Var Slot</span>
                  </button>
                  <button className="flex flex-col items-center gap-1 text-[11px] px-2 py-2.5 rounded-lg border border-border/60 hover:bg-secondary hover:border-primary/30 transition-all cursor-pointer group" onClick={() => addElement('chart')}>
                    <div className="flex items-center justify-center h-7 w-7 rounded-md bg-[hsl(var(--chart-2)/0.1)] group-hover:bg-[hsl(var(--chart-2)/0.2)] transition-colors"><RiBarChart2Line className="h-3.5 w-3.5 text-[hsl(var(--chart-2))]" /></div>
                    <span className="text-muted-foreground group-hover:text-foreground transition-colors">Chart</span>
                  </button>
                </div>
              </div>
              <div className="flex items-center justify-between px-3 py-2 border-b bg-card/60 shrink-0">
                <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">Layers <span className="text-primary">({state.elements.length})</span></span>
              </div>
              <div className="flex-1 overflow-y-auto px-2 py-1">
                {state.elements.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-8 text-center">
                    <p className="text-[10px] text-muted-foreground">No elements yet</p>
                  </div>
                ) : state.elements.map((el) => (
                  <div
                    key={el.id}
                    className={`group flex items-center gap-2 text-[11px] px-2 py-1.5 rounded-md cursor-pointer transition-colors ${selectedId === el.id ? 'bg-primary/10 text-primary ring-1 ring-primary/20' : 'hover:bg-secondary text-foreground/80'}`}
                    onClick={() => { setSelectedId(el.id); setSelectedVarId(null) }}
                  >
                    {el.type === 'label' && <RiText className="h-3 w-3 shrink-0 text-[hsl(var(--chart-1))]" />}
                    {el.type === 'image' && <RiImageLine className="h-3 w-3 shrink-0 text-[hsl(var(--chart-3))]" />}
                    {el.type === 'table' && <RiTableLine className="h-3 w-3 shrink-0 text-success" />}
                    {el.type === 'spaceholder' && <RiHashtag className="h-3 w-3 shrink-0 text-accent" />}
                    {el.type === 'chart' && <RiBarChart2Line className="h-3 w-3 shrink-0 text-[hsl(var(--chart-2))]" />}
                    <span className="truncate flex-1 text-[10px]">{el.type === 'label' ? (el.label?.text?.slice(0, 20) || 'Label') : el.type === 'image' ? 'Image' : el.type === 'table' ? `Table ${el.table?.rows}×${el.table?.cols}` : el.type === 'chart' ? (el.chart?.title || 'Chart (unlinked)') : `{{${state.variables.find(v => v.id === el.variableId)?.name || '?'}}}`}</span>
                    <button className="text-destructive hover:bg-destructive/10 rounded p-0.5 opacity-0 group-hover:opacity-100 transition-opacity shrink-0" onClick={(e) => { e.stopPropagation(); deleteElement(el.id) }}>
                      <RiDeleteBinLine className="h-3 w-3" />
                    </button>
                  </div>
                ))}
              </div>
            </>)}

            {/* Variables tab */}
            {leftTab === 'variables' && (<>
              <div className="px-3 py-2 border-b bg-card/60 shrink-0">
                <span className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">Variables <span className="text-primary">({state.variables.length})</span></span>
              </div>
              <div className="px-2 py-1.5 border-b shrink-0">
                <input type="text" value={varSearch} onChange={e => setVarSearch(e.target.value)} placeholder="Search variables…" className="w-full h-6 text-[10px] rounded border bg-secondary/40 px-2 outline-none focus:ring-1 focus:ring-primary/40 transition-shadow" />
              </div>
              <div ref={varListRef} className="flex-1 overflow-y-auto">
                {state.variables.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-8 px-3 text-center">
                    <RiHashtag className="h-5 w-5 text-muted-foreground/50 mb-2" />
                    <p className="text-[10px] text-muted-foreground">No variables yet</p>
                  </div>
                ) : state.variables.filter(v => !varSearch.trim() || v.name.toLowerCase().includes(varSearch.toLowerCase())).map((v) => (
                  <div
                    key={v.id}
                    data-var-id={v.id}
                    className={`flex items-center gap-2 px-3 py-2 cursor-pointer border-b border-border/40 transition-colors ${selectedVarId === v.id ? 'bg-primary/10' : 'hover:bg-secondary/50'}`}
                    onClick={() => { if (renamingVarId !== v.id) { setSelectedVarId(v.id); setSelectedId(null) } }}
                  >
                    <div className="flex-1 min-w-0">
                      {renamingVarId === v.id ? (
                        <input
                          ref={renameInputRef}
                          className="w-full h-5 text-[10px] font-medium rounded px-1 border bg-background outline-none focus:ring-1 focus:ring-primary/50"
                          value={renameValue}
                          onChange={e => setRenameValue(e.target.value)}
                          onBlur={() => { if (renameValue.trim()) updateVariable(v.id, { ...v, name: renameValue.trim() }); setRenamingVarId(null) }}
                          onKeyDown={e => { if (e.key === 'Enter') { if (renameValue.trim()) updateVariable(v.id, { ...v, name: renameValue.trim() }); setRenamingVarId(null) } else if (e.key === 'Escape') setRenamingVarId(null) }}
                          onClick={e => e.stopPropagation()}
                          autoFocus
                        />
                      ) : (
                        <div
                          className="text-[10px] font-medium truncate"
                          onDoubleClick={e => { e.stopPropagation(); setRenamingVarId(v.id); setRenameValue(v.name); setSelectedVarId(v.id); setSelectedId(null) }}
                          title="Double-click to rename"
                        >{v.name}</div>
                      )}
                      <div className="text-[9px] text-muted-foreground truncate">{v.value?.field || v.expression || v.datetimeExpr || '—'}</div>
                    </div>
                    <span className={`text-[8px] px-1.5 py-0.5 rounded-full font-bold shrink-0 ${v.type === 'expression' ? 'bg-amber-500/10 text-amber-600' : v.type === 'datetime' ? 'bg-blue-500/10 text-blue-500' : 'bg-primary/10 text-primary'}`}>
                      {v.type === 'expression' ? '∑' : v.type === 'datetime' ? '🗓' : 'Q'}
                    </span>
                  </div>
                ))}
              </div>
              <div className="p-2 border-t shrink-0">
                <button className="flex items-center justify-center gap-1.5 w-full py-2 rounded-lg border-2 border-dashed border-primary/30 hover:border-primary/60 hover:bg-primary/5 text-[11px] font-medium text-primary transition-all" onClick={addVariable}>
                  <RiAddLine className="h-3.5 w-3.5" /> New Variable
                </button>
              </div>
            </>)}

            {/* Settings tab */}
            {leftTab === 'settings' && (<>
              <div className="px-3 py-2 border-b bg-card/60 shrink-0">
                <div className="text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">Grid Settings</div>
              </div>
              <div className="p-3 space-y-3">
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
                    <label className="block text-[10px] text-muted-foreground mb-0.5">Cell px</label>
                    <input type="number" className="w-full h-7 text-[11px] rounded-md border bg-secondary/50 px-1.5 focus:ring-1 focus:ring-primary/40 outline-none transition-shadow" value={state.cellSize} onChange={(e) => setState(s => ({ ...s, cellSize: Math.max(10, +e.target.value) }))} />
                  </div>
                </div>
                <label className="flex items-center gap-2 text-[11px] text-muted-foreground cursor-pointer hover:text-foreground transition-colors">
                  <input type="checkbox" className="rounded border-border accent-primary" checked={state.showGridLines} onChange={(e) => setState(s => ({ ...s, showGridLines: e.target.checked }))} />
                  Show grid lines
                </label>
              </div>
            </>)}

          </div>

          {/* Canvas */}
          <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
            {/* Canvas toolbar */}
            <div className="flex items-center gap-3 px-3 py-1.5 border-b bg-card/40 shrink-0">
              <span className="text-[9px] text-muted-foreground">Del to remove · Arrow keys to move</span>
              <div className="flex-1" />
              <label className="flex items-center gap-1.5 text-[10px] text-muted-foreground cursor-pointer hover:text-foreground transition-colors">
                <input type="checkbox" className="rounded border-border accent-primary" checked={state.showGridLines} onChange={(e) => setState(s => ({ ...s, showGridLines: e.target.checked }))} />
                Grid
              </label>
            </div>
            {/* Canvas body */}
            <div className="flex-1 overflow-auto p-3">
              <GridCanvas
                state={state}
                selectedId={selectedId}
                onSelectElement={(id) => { setSelectedId(id); if (id) setSelectedVarId(null) }}
                onMoveElement={(id, gx, gy) => updateElement(id, { gridX: gx, gridY: gy })}
                onMoveElements={(moves) => setState((s) => ({ ...s, elements: s.elements.map((el) => { const m = moves.find(m => m.id === el.id); return m ? { ...el, gridX: m.gridX, gridY: m.gridY } : el }) }))}
                onResizeElement={(id, gw, gh) => updateElement(id, { gridW: gw, gridH: gh })}
                onUpdateElement={(id, el) => setState((s) => ({ ...s, elements: s.elements.map((e) => e.id === id ? el : e) }))}
              />
            </div>
            {/* Status bar */}
            <div className="flex items-center gap-3 px-3 py-1 border-t bg-card/40 shrink-0">
              {selectedEl ? (
                <><span className="text-[9px] font-medium text-foreground/80 capitalize">{selectedEl.type}</span><span className="text-[9px] text-muted-foreground">X:{selectedEl.gridX} Y:{selectedEl.gridY} W:{selectedEl.gridW} H:{selectedEl.gridH}</span></>
              ) : (
                <span className="text-[9px] text-muted-foreground">Click an element to select</span>
              )}
              <div className="flex-1" />
              <span className="text-[9px] text-muted-foreground">{state.gridCols} cols · {state.gridRows} rows · {state.cellSize}px</span>
              <span className="text-[9px] text-primary/80">{state.elements.length} el · {state.variables.length} var</span>
            </div>
          </div>

          {/* Smart Panel (right) — context-aware */}
          <div className="w-80 border-l bg-card/40 flex flex-col overflow-hidden shrink-0">
            {/* Context header */}
            <div className="px-4 py-2.5 border-b bg-card/60 shrink-0">
              {selectedEl ? (
                <div className="flex items-center gap-2">
                  <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-semibold border ${selectedEl.type === 'label' ? 'bg-[hsl(var(--chart-1)/0.1)] text-[hsl(var(--chart-1))] border-[hsl(var(--chart-1)/0.25)]' : selectedEl.type === 'image' ? 'bg-[hsl(var(--chart-3)/0.1)] text-[hsl(var(--chart-3))] border-[hsl(var(--chart-3)/0.25)]' : selectedEl.type === 'table' ? 'bg-success/10 text-success border-success/25' : selectedEl.type === 'chart' ? 'bg-[hsl(var(--chart-2)/0.1)] text-[hsl(var(--chart-2))] border-[hsl(var(--chart-2)/0.25)]' : 'bg-accent/10 text-accent border-accent/25'}`}>
                    {selectedEl.type === 'label' && <RiText className="h-3 w-3" />}
                    {selectedEl.type === 'image' && <RiImageLine className="h-3 w-3" />}
                    {selectedEl.type === 'table' && <RiTableLine className="h-3 w-3" />}
                    {selectedEl.type === 'spaceholder' && <RiHashtag className="h-3 w-3" />}
                    {selectedEl.type === 'chart' && <RiBarChart2Line className="h-3 w-3" />}
                    <span className="capitalize">{selectedEl.type}</span>
                  </span>
                  <button className="ml-auto text-destructive/70 hover:text-destructive hover:bg-destructive/10 rounded-md p-1 transition-colors" onClick={() => deleteElement(selectedEl.id)} title="Delete">
                    <RiDeleteBinLine className="h-3.5 w-3.5" />
                  </button>
                </div>
              ) : selectedVarId ? (
                <div className="flex items-center gap-2">
                  <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[10px] font-semibold bg-accent/10 text-accent border border-accent/25">
                    <RiHashtag className="h-3 w-3" /> Variable Editor
                  </span>
                  {(() => {
                    const sv = state.variables.find(v => v.id === selectedVarId)
                    return sv ? (
                      <div className="flex items-center gap-1 ml-auto">
                        {confirmDeleteVarId === selectedVarId ? (
                          <>
                            <span className="text-[9px] text-destructive font-medium">Delete?</span>
                            <button className="text-[9px] px-1.5 py-0.5 rounded bg-destructive text-destructive-foreground" onClick={() => { deleteVariable(sv.id); setConfirmDeleteVarId(null) }}>Yes</button>
                            <button className="text-[9px] px-1.5 py-0.5 rounded border hover:bg-muted transition-colors" onClick={() => setConfirmDeleteVarId(null)}>No</button>
                          </>
                        ) : (
                          <>
                            <button title="Duplicate" className="text-muted-foreground hover:text-foreground hover:bg-secondary p-1 rounded transition-colors" onClick={() => duplicateVariable(sv.id)}><RiFileCopyLine className="h-3.5 w-3.5" /></button>
                            <button title="Delete" className="text-destructive/70 hover:text-destructive hover:bg-destructive/10 p-1 rounded transition-colors" onClick={() => setConfirmDeleteVarId(sv.id)}><RiDeleteBinLine className="h-3.5 w-3.5" /></button>
                          </>
                        )}
                      </div>
                    ) : null
                  })()}
                </div>
              ) : (
                <p className="text-[11px] text-muted-foreground">Select an element or variable</p>
              )}
            </div>

            {/* Smart panel content */}
            <div className="flex-1 overflow-y-auto">
              {selectedEl && (
                <div className="p-3 space-y-3">
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
                    <ElementProps element={selectedEl} variables={state.variables} allWidgets={allWidgets} onUpdate={(el) => updateElement(el.id, el)} />
                  </div>
                </div>
              )}
              {!selectedEl && selectedVarId && (() => {
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
              {!selectedEl && !selectedVarId && (
                <div className="flex flex-col items-center justify-center h-full py-12 text-center px-4">
                  <div className="flex items-center justify-center h-10 w-10 rounded-full bg-secondary mb-3">
                    <RiSettings3Line className="h-5 w-5 text-muted-foreground" />
                  </div>
                  <p className="text-xs text-muted-foreground">Select an element on the canvas<br />or a variable from the list to edit</p>
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
