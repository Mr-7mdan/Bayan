'use client'

import { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { Api, QueryApi, type DatasourceOut, type IntrospectResponse } from '@/lib/api'
import type { DatasourceTransforms } from '@/lib/dsl'
import {
  RiCloseLine, RiSearchLine, RiTableLine, RiDatabase2Line,
  RiBracesLine, RiRefreshLine, RiArrowRightSLine, RiArrowDownSLine,
  RiLoader4Line, RiAddLine, RiArrowUpDownLine, RiArrowUpLine, RiArrowDownLine,
  RiPencilLine, RiDeleteBinLine, RiFilterLine,
  RiSettings5Line, RiArrowLeftSLine,
} from '@remixicon/react'
import { inferKind, KIND_CLS } from './CustomQueryEditorParts'
import AdvancedSqlCustomColumnBuilder from './AdvancedSqlCustomColumnBuilder'
import AdvancedSqlJoinBuilder from './AdvancedSqlJoinBuilder'
import AdvancedSqlComputedBuilder from './AdvancedSqlComputedBuilder'
import AdvancedSqlCaseBuilder from './AdvancedSqlCaseBuilder'
import AdvancedSqlReplaceBuilder from './AdvancedSqlReplaceBuilder'
import AdvancedSqlNullBuilder from './AdvancedSqlNullBuilder'
import AdvancedSqlUnpivotBuilder from './AdvancedSqlUnpivotBuilder'

// ─── Types ────────────────────────────────────────────────────────────────────
type Sel = { schema: string; table: string; column: string | null }
type SortState = { column: string; direction: 'asc' | 'desc' } | null
type TransformType = 'custom' | 'join' | 'computed' | 'case' | 'replace' | 'null' | 'unpivot'
type EditingTransform = { type: TransformType; index: number | null; initial?: any } | null

// ─── Helpers ──────────────────────────────────────────────────────────────────
const SYS = new Set(['information_schema', 'sys', 'guest', 'pg_catalog', 'pg_toast'])
function isSys(n: string) { const l = n.toLowerCase(); return l.startsWith('db_') || SYS.has(l) }
function fmtCell(v: any) { if (v == null) return ''; if (typeof v === 'boolean') return v ? 'true' : 'false'; return String(v) }

// ─── SchemaTree (Same as original) ────────────────────────────────────────────
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
                      isSelTbl && !sel?.column ? 'border-[#1E40AF] bg-[#1E40AF]/6' : 'border-transparent hover:bg-[hsl(var(--muted))]/40',
                    ].join(' ')}>
                      <button type="button" onClick={() => togTable(tk)} className="p-0.5 flex-shrink-0 hover:bg-[hsl(var(--muted))] rounded">
                        {tOpen ? <RiArrowDownSLine className="h-3 w-3 text-muted-foreground" /> : <RiArrowRightSLine className="h-3 w-3 text-muted-foreground" />}
                      </button>
                      <button type="button" onClick={() => { onSelect({ schema: sch.name, table: t.name, column: null }); if (!tOpen) togTable(tk) }}
                        className="flex items-center gap-1.5 flex-1 min-w-0 text-xs text-left">
                        <RiTableLine className="h-3.5 w-3.5 text-[#3B82F6]/70 flex-shrink-0" />
                        <span className={['font-medium truncate', isSelTbl && !sel?.column ? 'text-[#1E40AF]' : 'text-foreground'].join(' ')}>{t.name}</span>
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
                            isSelCol ? 'border-[#1E40AF] bg-[#1E40AF]/8 text-[#1E40AF]' : 'border-transparent hover:bg-[hsl(var(--muted))]/30 text-muted-foreground',
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

// ─── TransformationPanel ──────────────────────────────────────────────────────
function TransformationPanel({ 
  dsId, 
  source, 
  schema,
  transforms, 
  onTransformsChange,
  collapsed,
  onToggleCollapse,
  widgetId,
  onSaveSuccess,
}: {
  dsId: string
  source: string | null
  schema: IntrospectResponse | null
  transforms: DatasourceTransforms
  onTransformsChange: (t: DatasourceTransforms) => void
  collapsed: boolean
  onToggleCollapse: () => void
  widgetId?: string
  onSaveSuccess?: () => void
}) {
  const [editing, setEditing] = useState<EditingTransform>(null)
  const [saving, setSaving] = useState(false)

  const handleSave = async () => {
    if (!dsId) return
    setSaving(true)
    try {
      await Api.saveDatasourceTransforms(dsId, transforms)
      // Notify other components
      if (typeof window !== 'undefined') {
        window.dispatchEvent(new CustomEvent('datasource-transforms-saved', { detail: { datasourceId: dsId } }))
      }
      // Small delay to ensure database commit completes
      await new Promise(resolve => setTimeout(resolve, 300))
      // Trigger refetch to ensure preview updates with saved transforms
      if (onSaveSuccess) {
        onSaveSuccess()
      }
    } catch (err) {
      console.error('Failed to save transforms:', err)
    } finally {
      setSaving(false)
    }
  }

  const handleAddCustomColumn = () => {
    setEditing({ type: 'custom', index: null })
  }

  const handleAddJoin = () => {
    setEditing({ type: 'join', index: null })
  }

  const handleAddComputed = () => {
    setEditing({ type: 'computed', index: null })
  }

  const handleAddCase = () => {
    setEditing({ type: 'case', index: null })
  }

  const handleAddReplace = () => {
    setEditing({ type: 'replace', index: null })
  }

  const handleAddUnpivot = () => {
    setEditing({ type: 'unpivot', index: null })
  }

  const handleAddNull = () => {
    setEditing({ type: 'null', index: null })
  }

  const handleEditCustom = (idx: number) => {
    setEditing({ type: 'custom', index: idx, initial: transforms.customColumns?.[idx] })
  }

  const handleDeleteCustom = (idx: number) => {
    const updated = { ...transforms, customColumns: transforms.customColumns?.filter((_, i) => i !== idx) || [] }
    onTransformsChange(updated)
  }

  const handleEditTransform = (idx: number) => {
    const transform = transforms.transforms?.[idx]
    if (!transform) return
    setEditing({ type: transform.type as TransformType, index: idx, initial: transform })
  }

  const handleDeleteTransform = (idx: number) => {
    const updated = { ...transforms, transforms: transforms.transforms?.filter((_, i) => i !== idx) || [] }
    onTransformsChange(updated)
  }

  const handleEditJoin = (idx: number) => {
    setEditing({ type: 'join', index: idx, initial: transforms.joins?.[idx] })
  }

  const handleDeleteJoin = (idx: number) => {
    const updated = { ...transforms, joins: transforms.joins?.filter((_, i) => i !== idx) || [] }
    onTransformsChange(updated)
  }

  const handleSaveCustom = (col: any) => {
    if (editing && editing.index !== null && editing.index !== undefined) {
      // Edit existing
      const updated = [...(transforms.customColumns || [])]
      updated[editing.index] = col
      onTransformsChange({ ...transforms, customColumns: updated })
    } else {
      // Add new
      onTransformsChange({ ...transforms, customColumns: [...(transforms.customColumns || []), col] })
    }
    setEditing(null)
  }

  const handleSaveJoin = (join: any) => {
    if (editing && editing.index !== null && editing.index !== undefined) {
      const updated = [...(transforms.joins || [])]
      updated[editing.index] = join
      onTransformsChange({ ...transforms, joins: updated })
    } else {
      onTransformsChange({ ...transforms, joins: [...(transforms.joins || []), join] })
    }
    setEditing(null)
  }

  const handleSaveTransform = (transform: any) => {
    if (editing && editing.index !== null && editing.index !== undefined) {
      // Edit existing
      const updated = [...(transforms.transforms || [])]
      updated[editing.index] = transform
      onTransformsChange({ ...transforms, transforms: updated })
    } else {
      // Add new
      onTransformsChange({ ...transforms, transforms: [...(transforms.transforms || []), transform] })
    }
    setEditing(null)
  }

  // Get available columns for builders
  const baseColumns = useMemo(() => {
    if (!source || !schema) return []
    const parts = source.split('.')
    const tbl = parts.pop() as string
    const sch = parts.join('.') || 'main'
    const schNode = (schema.schemas || []).find(s => s.name === sch)
    const tblNode = schNode?.tables.find(t => t.name === tbl)
    return (tblNode?.columns || []).map(c => c.name)
  }, [source, schema])

  const [showAddMenu, setShowAddMenu] = useState(false)

  if (collapsed) {
    return (
      <div className="w-12 border-r border-[hsl(var(--border))] flex flex-col items-center py-3 gap-2 bg-[hsl(var(--background))]">
        <button 
          onClick={onToggleCollapse}
          className="p-2 rounded hover:bg-[hsl(var(--muted))] transition-colors"
          title="Show transformations"
        >
          <RiArrowRightSLine className="h-4 w-4 text-muted-foreground" />
        </button>
        <div className="writing-mode-vertical text-xs text-muted-foreground font-medium">
          Transformations ({(transforms.customColumns?.length || 0) + (transforms.joins?.length || 0) + (transforms.transforms?.length || 0)})
        </div>
      </div>
    )
  }

  return (
    <div className="w-[400px] border-r border-[hsl(var(--border))] flex flex-col min-h-0 bg-[hsl(var(--background))]">
      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2.5 border-b border-[hsl(var(--border))] flex-shrink-0">
        <div className="flex items-center gap-2">
          <RiSettings5Line className="h-4 w-4 text-muted-foreground" />
          <h3 className="text-sm font-medium text-foreground">Transformations</h3>
        </div>
        <div className="flex items-center gap-1.5">
          <button 
            onClick={handleSave}
            disabled={saving}
            className="text-xs px-3 py-1.5 rounded-md bg-[#F59E0B] text-white hover:bg-[#D97706] disabled:opacity-50 transition-colors font-medium"
          >
            {saving ? 'Saving…' : 'Save'}
          </button>
          <button 
            onClick={onToggleCollapse}
            className="p-1.5 rounded-md hover:bg-[hsl(var(--muted))] transition-colors"
            title="Collapse panel"
          >
            <RiArrowLeftSLine className="h-4 w-4 text-muted-foreground" />
          </button>
        </div>
      </div>

      {/* Add button with dropdown */}
      <div className="px-3 py-2 border-b border-[hsl(var(--border))] flex-shrink-0 relative">
        <button 
          onClick={() => setShowAddMenu(!showAddMenu)}
          className="w-full text-xs px-3 py-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--muted))]/30 hover:bg-[hsl(var(--muted))] transition-colors font-medium flex items-center justify-center gap-2 text-foreground"
        >
          <RiAddLine className="h-4 w-4" />
          Add Transformation
          <RiArrowDownSLine className="h-4 w-4 ml-auto" />
        </button>
        {showAddMenu && (
          <>
            <div className="fixed inset-0 z-40" onClick={() => setShowAddMenu(false)} />
            <div className="absolute top-full left-3 right-3 mt-1 py-1 bg-[hsl(var(--popover))] border border-[hsl(var(--border))] rounded-md shadow-lg z-50">
              <button onClick={() => { handleAddCustomColumn(); setShowAddMenu(false) }} className="w-full px-3 py-2 text-left text-xs hover:bg-[hsl(var(--muted))] transition-colors text-foreground">Custom Column</button>
              <button onClick={() => { handleAddJoin(); setShowAddMenu(false) }} className="w-full px-3 py-2 text-left text-xs hover:bg-[hsl(var(--muted))] transition-colors text-foreground">Join Table</button>
              <button onClick={() => { handleAddComputed(); setShowAddMenu(false) }} className="w-full px-3 py-2 text-left text-xs hover:bg-[hsl(var(--muted))] transition-colors text-foreground">Computed Expression</button>
              <button onClick={() => { handleAddCase(); setShowAddMenu(false) }} className="w-full px-3 py-2 text-left text-xs hover:bg-[hsl(var(--muted))] transition-colors text-foreground">Case/When</button>
              <button onClick={() => { handleAddReplace(); setShowAddMenu(false) }} className="w-full px-3 py-2 text-left text-xs hover:bg-[hsl(var(--muted))] transition-colors text-foreground">Replace Values</button>
              <button onClick={() => { handleAddUnpivot(); setShowAddMenu(false) }} className="w-full px-3 py-2 text-left text-xs hover:bg-[hsl(var(--muted))] transition-colors text-foreground">Unpivot</button>
              <button onClick={() => { handleAddNull(); setShowAddMenu(false) }} className="w-full px-3 py-2 text-left text-xs hover:bg-[hsl(var(--muted))] transition-colors text-foreground">Null Handling</button>
            </div>
          </>
        )}
      </div>

      {/* Inline builder */}
      {editing && (
        <div className="border-b border-[hsl(var(--border))] p-3 bg-[hsl(var(--muted))]/20 flex-shrink-0 max-h-[400px] overflow-y-auto">
          {editing.type === 'custom' && (
            <AdvancedSqlCustomColumnBuilder
              columns={baseColumns}
              onAddAction={handleSaveCustom}
              onCancelAction={() => setEditing(null)}
              initial={editing.initial}
              source={source || undefined}
              widgetId={widgetId}
            />
          )}
          {editing.type === 'join' && (
            <AdvancedSqlJoinBuilder
              baseColumns={baseColumns}
              baseSource={source || undefined}
              schema={schema || undefined}
              onAddAction={handleSaveJoin}
              onCancelAction={() => setEditing(null)}
              initial={editing.initial}
            />
          )}
          {editing.type === 'computed' && (
            <AdvancedSqlComputedBuilder
              columns={baseColumns}
              onAddAction={handleSaveTransform}
              onCancelAction={() => setEditing(null)}
              initial={editing.initial}
              dsId={dsId}
              tableName={source}
            />
          )}
          {editing.type === 'case' && (
            <AdvancedSqlCaseBuilder
              columns={baseColumns}
              onAddAction={handleSaveTransform}
              onCancelAction={() => setEditing(null)}
              initial={editing.initial}
              dsId={dsId}
              tableName={source}
            />
          )}
          {editing.type === 'replace' && (
            <AdvancedSqlReplaceBuilder
              columns={baseColumns}
              onAddAction={handleSaveTransform}
              onCancelAction={() => setEditing(null)}
              initial={editing.initial}
              dsId={dsId}
              tableName={source}
            />
          )}
          {editing.type === 'unpivot' && (
            <AdvancedSqlUnpivotBuilder
              columns={baseColumns}
              onAddAction={handleSaveTransform}
              onCancelAction={() => setEditing(null)}
              initial={editing.initial}
              dsId={dsId}
              tableName={source}
            />
          )}
          {editing.type === 'null' && (
            <AdvancedSqlNullBuilder
              columns={baseColumns}
              onAddAction={handleSaveTransform}
              onCancelAction={() => setEditing(null)}
              initial={editing.initial}
              dsId={dsId}
              tableName={source}
            />
          )}
        </div>
      )}

      {/* Active items list */}
      <div className="flex-1 overflow-y-auto min-h-0 p-3 space-y-2">
        {/* Custom columns */}
        {transforms.customColumns?.map((cc, idx) => (
          <div 
            key={`cc-${idx}`}
            className="p-2.5 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted))]/30 transition-colors group"
          >
            <div className="flex items-start justify-between gap-2 mb-1">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-1.5 mb-1">
                  <span className="text-xs font-medium text-foreground truncate">
                    {cc.name}
                  </span>
                  <span className="text-[10px] px-1.5 py-0.5 rounded-md bg-[hsl(var(--muted))] text-muted-foreground font-medium">
                    Custom Column
                  </span>
                </div>
                {cc.scope && (
                  <span className={[
                    'text-[10px] px-1.5 py-0.5 rounded-md font-medium inline-block',
                    cc.scope.level === 'datasource' ? 'bg-[hsl(var(--muted))] text-muted-foreground' :
                    cc.scope.level === 'table' ? 'bg-[#F59E0B]/20 text-[#F59E0B]' :
                    'bg-[#10B981]/20 text-[#10B981]'
                  ].join(' ')}>
                    {cc.scope.level === 'datasource' ? 'Datasource' :
                     cc.scope.level === 'table' ? `Table: ${cc.scope.table}` :
                     `Widget: ${cc.scope.widgetId}`}
                  </span>
                )}
              </div>
              <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                <button 
                  onClick={() => handleEditCustom(idx)}
                  className="p-1 rounded hover:bg-[hsl(var(--muted))] transition-colors"
                  title="Edit"
                >
                  <RiPencilLine className="h-3 w-3 text-muted-foreground" />
                </button>
                <button 
                  onClick={() => handleDeleteCustom(idx)}
                  className="p-1 rounded hover:bg-red-500/20 transition-colors"
                  title="Delete"
                >
                  <RiDeleteBinLine className="h-3 w-3 text-red-400" />
                </button>
              </div>
            </div>
            {cc.expr && (
              <div className="text-[10px] font-mono bg-[hsl(var(--muted))]/40 px-2 py-1 rounded text-muted-foreground overflow-x-auto whitespace-nowrap" style={{ fontFamily: "'Fira Code', monospace" }}>
                {String(cc.expr).length > 60 ? String(cc.expr).slice(0, 60) + '…' : cc.expr}
              </div>
            )}
          </div>
        ))}

        {/* Transforms (computed, case, replace, unpivot, null) */}
        {transforms.transforms?.map((tr, idx) => {
          const type = String(tr.type || '').toLowerCase()
          
          // Safely extract display name based on type
          const displayName = (() => {
            if (type === 'computed' && 'name' in tr) return tr.name
            if (type === 'case' && 'target' in tr) return `${tr.target} (Case)`
            if (type === 'replace' && 'target' in tr) return `${tr.target} (Replace)`
            if (type === 'nullhandling' && 'target' in tr) return `${tr.target} (Null)`
            if (type === 'unpivot' && 'keyColumn' in tr && 'valueColumn' in tr) return `${tr.keyColumn} / ${tr.valueColumn}`
            return 'Transform'
          })()
          
          const typeLabel = type === 'computed' ? 'Computed' :
                           type === 'case' ? 'Case/When' :
                           type === 'replace' ? 'Replace' :
                           type === 'nullhandling' ? 'Null Handling' :
                           type === 'unpivot' ? 'Unpivot' :
                           type.charAt(0).toUpperCase() + type.slice(1)
          
          return (
            <div 
              key={`tr-${idx}`}
              className="p-2.5 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted))]/30 transition-colors group"
            >
              <div className="flex items-start justify-between gap-2 mb-1">
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-1.5 mb-1">
                    <span className="text-xs font-medium text-foreground truncate">
                      {displayName}
                    </span>
                    <span className="text-[10px] px-1.5 py-0.5 rounded-md bg-[hsl(var(--muted))] text-muted-foreground font-medium">
                      {typeLabel}
                    </span>
                  </div>
                  {tr.scope && (
                    <span className={[
                      'text-[10px] px-1.5 py-0.5 rounded-md font-medium inline-block',
                      tr.scope.level === 'datasource' ? 'bg-[hsl(var(--muted))] text-muted-foreground' :
                      tr.scope.level === 'table' ? 'bg-[#F59E0B]/20 text-[#F59E0B]' :
                      'bg-[#10B981]/20 text-[#10B981]'
                    ].join(' ')}>
                      {tr.scope.level === 'datasource' ? 'Datasource' :
                       tr.scope.level === 'table' ? `Table: ${tr.scope.table}` :
                       `Widget: ${tr.scope.widgetId}`}
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                  <button 
                    onClick={() => handleEditTransform(idx)}
                    className="p-1 rounded hover:bg-[hsl(var(--muted))] transition-colors"
                    title="Edit"
                  >
                    <RiPencilLine className="h-3 w-3 text-muted-foreground" />
                  </button>
                  <button 
                    onClick={() => handleDeleteTransform(idx)}
                    className="p-1 rounded hover:bg-red-500/20 transition-colors"
                    title="Delete"
                  >
                    <RiDeleteBinLine className="h-3 w-3 text-red-400" />
                  </button>
                </div>
              </div>
              {type === 'computed' && 'expr' in tr && tr.expr && (
                <div className="text-[10px] font-mono bg-[hsl(var(--muted))]/40 px-2 py-1 rounded text-muted-foreground overflow-x-auto whitespace-nowrap" style={{ fontFamily: "'Fira Code', monospace" }}>
                  {String(tr.expr).length > 60 ? String(tr.expr).slice(0, 60) + '…' : tr.expr}
                </div>
              )}
            </div>
          )
        })}

        {/* Joins */}
        {transforms.joins?.map((j, idx) => (
          <div 
            key={`j-${idx}`}
            className="p-2.5 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted))]/30 transition-colors group"
          >
            <div className="flex items-start justify-between gap-2 mb-1">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-1.5 mb-1">
                  <span className="text-xs font-medium text-foreground truncate">
                    {j.targetTable}
                  </span>
                  <span className="text-[10px] px-1.5 py-0.5 rounded-md bg-[hsl(var(--muted))] text-muted-foreground font-medium">
                    {j.joinType.toUpperCase()} Join
                  </span>
                </div>
                <div className="text-[10px] text-muted-foreground">
                  {j.targetTable} on {j.sourceKey} = {j.targetKey}
                </div>
              </div>
              <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
                <button 
                  onClick={() => handleEditJoin(idx)}
                  className="p-1 rounded hover:bg-[hsl(var(--muted))] transition-colors"
                  title="Edit"
                >
                  <RiPencilLine className="h-3 w-3 text-muted-foreground" />
                </button>
                <button 
                  onClick={() => handleDeleteJoin(idx)}
                  className="p-1 rounded hover:bg-red-500/20 transition-colors"
                  title="Delete"
                >
                  <RiDeleteBinLine className="h-3 w-3 text-red-400" />
                </button>
              </div>
            </div>
          </div>
        ))}

        {/* Empty state */}
        {!transforms.customColumns?.length && !transforms.joins?.length && !transforms.transforms?.length && (
          <div className="flex flex-col items-center justify-center py-16 px-4 text-center">
            <RiFilterLine className="h-10 w-10 text-muted-foreground/15 mb-3" />
            <p className="text-xs text-muted-foreground mb-1.5">No transformations yet</p>
            <p className="text-[11px] text-muted-foreground/50">
              Click "Add Transformation" to enhance your data
            </p>
          </div>
        )}
      </div>
    </div>
  )
}

// ─── PreviewPanel (Enhanced) ──────────────────────────────────────────────────
const LIMIT = 500

function PreviewPanel({ dsId, sel, transforms, refreshTrigger }: { 
  dsId: string
  sel: Sel | null
  transforms: DatasourceTransforms
  refreshTrigger: number
}) {
  const [loading, setLoading] = useState(false)
  const [error, setError]     = useState<string | null>(null)
  const [cols, setCols]       = useState<string[]>([])
  const [rows, setRows]       = useState<any[][]>([])
  const [total, setTotal]     = useState<number | null>(null)
  const [offset, setOffset]   = useState(0)
  const [columnFilter, setColumnFilter] = useState('')
  const [sortState, setSortState] = useState<SortState>(null)
  const colRefs = useRef<Record<string, HTMLTableCellElement | null>>({})

  useEffect(() => { setOffset(0); setCols([]); setRows([]); setSortState(null) }, [sel?.table, sel?.schema])

  // Fetch data with transforms applied
  useEffect(() => {
    if (!sel?.table) return
    let cancelled = false
    setLoading(true); setError(null)
    const { table, schema } = sel
    async function run() {
      try {
        // Use querySpec API to apply transforms
        const source = schema && schema !== 'main' ? `${schema}.${table}` : table
        console.log('[PreviewPanel] Querying with:', { source, dsId, transforms })
        const res = await QueryApi.querySpec({
          spec: {
            source,
            select: ['*'],
          },
          datasourceId: dsId,
          limit: LIMIT,
          offset,
          includeTotal: true,
        })
        if (cancelled) return
        console.log('[PreviewPanel] Query result:', { columns: res.columns, rowCount: res.rows?.length })
        setCols(res.columns as string[])
        setRows(res.rows as any[][])
        setTotal(typeof (res as any).totalRows === 'number' ? (res as any).totalRows : null)
        setLoading(false)
      } catch (err) {
        console.error('[PreviewPanel] Query error:', err)
        if (!cancelled) {
          setError('Failed to load data. Table may not exist or transforms may have errors.')
          setLoading(false)
        }
      }
    }
    void run()
    return () => { cancelled = true }
  }, [dsId, sel?.table, sel?.schema, offset, transforms, refreshTrigger])

  // Scroll to selected column
  useEffect(() => {
    if (!sel?.column || cols.length === 0) return
    const el = colRefs.current[sel.column]
    if (el) el.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' })
  }, [sel?.column, cols])

  // Filter columns
  const filteredCols = useMemo(() => {
    if (!columnFilter) return cols
    const q = columnFilter.toLowerCase()
    return cols.filter(c => c.toLowerCase().includes(q))
  }, [cols, columnFilter])

  // Sort rows
  const sortedRows = useMemo(() => {
    if (!sortState || !sortState.column) return rows
    const colIdx = cols.indexOf(sortState.column)
    if (colIdx === -1) return rows
    
    const sorted = [...rows].sort((a, b) => {
      const aVal = a[colIdx]
      const bVal = b[colIdx]
      
      if (aVal == null && bVal == null) return 0
      if (aVal == null) return 1
      if (bVal == null) return -1
      
      if (typeof aVal === 'number' && typeof bVal === 'number') {
        return sortState.direction === 'asc' ? aVal - bVal : bVal - aVal
      }
      
      const aStr = String(aVal)
      const bStr = String(bVal)
      return sortState.direction === 'asc' 
        ? aStr.localeCompare(bStr) 
        : bStr.localeCompare(aStr)
    })
    
    return sorted
  }, [rows, sortState, cols])

  const handleHeaderClick = (col: string) => {
    setSortState(prev => {
      if (!prev || prev.column !== col) return { column: col, direction: 'asc' }
      if (prev.direction === 'asc') return { column: col, direction: 'desc' }
      return null // Reset to no sort
    })
  }

  const getSortIcon = (col: string) => {
    if (!sortState || sortState.column !== col) {
      return <RiArrowUpDownLine className="h-3 w-3 text-muted-foreground/40" />
    }
    if (sortState.direction === 'asc') {
      return <RiArrowUpLine className="h-3 w-3 text-[#1E40AF]" />
    }
    return <RiArrowDownLine className="h-3 w-3 text-[#1E40AF]" />
  }

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
          <RiTableLine className="h-4 w-4 text-[#1E40AF] flex-shrink-0" />
          <span className="font-semibold truncate">{sel.schema !== 'main' ? `${sel.schema}.` : ''}{sel.table}</span>
          {sel.column && (
            <span className="flex items-center gap-1 px-2 py-0.5 rounded-full bg-[#1E40AF]/12 text-[#1E40AF] text-[11px] font-medium border border-[#1E40AF]/20 flex-shrink-0">
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

      {/* Column filter */}
      {cols.length > 0 && (
        <div className="px-4 py-2 border-b border-[hsl(var(--border))] flex-shrink-0 bg-[hsl(var(--background))]">
          <div className="relative">
            <RiSearchLine className="absolute left-2.5 top-1/2 -translate-y-1/2 h-3.5 w-3.5 text-muted-foreground pointer-events-none" />
            <input 
              type="text" 
              value={columnFilter} 
              onChange={e => setColumnFilter(e.target.value)} 
              placeholder="Filter columns…"
              className="w-full pl-8 pr-3 py-2 text-xs rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))] text-foreground placeholder:text-muted-foreground outline-none focus:ring-2 focus:ring-[#F59E0B]/40 transition-shadow"
            />
          </div>
          {columnFilter && (
            <div className="text-[10px] text-muted-foreground mt-1.5">
              Showing {filteredCols.length} of {cols.length} columns
            </div>
          )}
        </div>
      )}

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
          <table className="min-w-full text-[11px] border-collapse" style={{ fontFamily: "'Fira Code', monospace" }}>
            <thead className="sticky top-0 z-10">
              <tr>
                {cols.map((c, ci) => {
                  const hl = c === sel.column
                  const visible = filteredCols.includes(c)
                  if (!visible) return null
                  return (
                    <th 
                      key={c + ci} 
                      ref={el => { colRefs.current[c] = el }}
                      onClick={() => handleHeaderClick(c)}
                      className={['text-left font-semibold px-3 py-2 whitespace-nowrap select-none bg-[hsl(var(--muted))] cursor-pointer group',
                        hl ? 'text-[#1E40AF] border-b-2 border-b-[#1E40AF]' : 'text-foreground border-b border-[hsl(var(--border))]',
                      ].join(' ')}>
                      <div className="flex items-center justify-between gap-2">
                        <div className="flex items-center gap-1.5">
                          {hl && <span className="inline-block w-1.5 h-1.5 rounded-full bg-[#1E40AF]" />}
                          {c}
                        </div>
                        <div className="opacity-0 group-hover:opacity-100 transition-opacity">
                          {getSortIcon(c)}
                        </div>
                      </div>
                    </th>
                  )
                })}
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((r, ri) => (
                <tr key={ri} className="border-b border-[hsl(var(--border))]/40 hover:bg-[hsl(var(--muted))]/25 transition-colors">
                  {cols.map((c, ci) => {
                    const hl = c === sel.column
                    const visible = filteredCols.includes(c)
                    if (!visible) return null
                    return (
                      <td key={c + ci} title={r[ci] != null ? String(r[ci]) : ''}
                        className={['px-3 py-1.5 align-top whitespace-nowrap max-w-[280px] overflow-hidden text-ellipsis',
                          hl ? 'bg-[#1E40AF]/5 text-[#1E40AF]/80 font-medium' : 'text-foreground',
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

export default function DataExplorerDialogV2({ open, onClose, datasource, initialTable, initialSchema, singleTable }: Props) {
  const [schema, setSchema]       = useState<IntrospectResponse | null>(null)
  const [loading, setLoading]     = useState(false)
  const [refreshing, setRefreshing] = useState(false)
  const [sel, setSel]             = useState<Sel | null>(null)
  const [transforms, setTransforms] = useState<DatasourceTransforms>({ customColumns: [], transforms: [], joins: [] })
  const [transformPanelCollapsed, setTransformPanelCollapsed] = useState(false)
  const [refreshTrigger, setRefreshTrigger] = useState(0)

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

  const fetchTransforms = async () => {
    try {
      const data = await Api.getDatasourceTransforms(datasource.id)
      console.log('[DataExplorerV2] Fetched transforms:', data)
      setTransforms(data)
      // Increment refresh trigger to force preview refetch
      setRefreshTrigger(prev => prev + 1)
    } catch {
      setTransforms({ customColumns: [], transforms: [], joins: [] })
    }
  }

  useEffect(() => {
    if (open) { 
      setSel(null)
      void fetchSchema(false, initialTable, initialSchema)
      void fetchTransforms()
    }
  }, [open, datasource.id])

  useEffect(() => {
    if (!open) return
    const handler = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose() }
    document.addEventListener('keydown', handler)
    return () => document.removeEventListener('keydown', handler)
  }, [open, onClose])

  if (!open || typeof window === 'undefined') return null

  return createPortal(
    <div className="fixed inset-0 z-[300] flex items-center justify-center bg-black/50 backdrop-blur-[2px]" onClick={onClose}>
      <div className="w-[95vw] max-w-[1600px] h-[90vh] bg-[hsl(var(--card))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl flex flex-col overflow-hidden"
        onClick={e => e.stopPropagation()}>

        {/* ── Header ── */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-[hsl(var(--border))] flex-shrink-0 bg-[hsl(var(--muted))]/20">
          <div className="flex items-center gap-3">
            <RiDatabase2Line className="h-5 w-5 text-[#1E40AF]" />
            <div>
              <h2 className="text-sm font-semibold leading-tight" style={{ fontFamily: "'Fira Sans', sans-serif" }}>{datasource.name}</h2>
              <p className="text-[11px] text-muted-foreground leading-tight">{datasource.type} · Data Explorer & Transformations</p>
            </div>
          </div>
          <button onClick={onClose}
            className="p-1.5 rounded-lg hover:bg-[hsl(var(--muted))] transition-colors">
            <RiCloseLine className="h-5 w-5 text-muted-foreground" />
          </button>
        </div>

        {/* ── Body: schema tree + transformation panel + preview ── */}
        <div className="flex flex-1 min-h-0">
          {/* Left: schema tree */}
          <div className="w-60 flex-shrink-0 flex flex-col min-h-0 border-r border-[hsl(var(--border))]">
            <SchemaTree schema={schema} loading={loading} refreshing={refreshing}
              sel={sel} onSelect={setSel} onRefresh={() => fetchSchema(true)}
              singleTable={singleTable ? initialTable : undefined} />
          </div>

          {/* Middle: transformation panel */}
          <TransformationPanel
            dsId={datasource.id}
            source={sel?.table || null}
            schema={schema}
            transforms={transforms}
            onTransformsChange={setTransforms}
            collapsed={transformPanelCollapsed}
            onToggleCollapse={() => setTransformPanelCollapsed(p => !p)}
            onSaveSuccess={fetchTransforms}
          />

          {/* Right: preview */}
          <div className="flex-1 min-w-0 flex flex-col min-h-0">
            <PreviewPanel dsId={datasource.id} sel={sel} transforms={transforms} refreshTrigger={refreshTrigger} />
          </div>
        </div>

      </div>
    </div>,
    document.body,
  )
}
