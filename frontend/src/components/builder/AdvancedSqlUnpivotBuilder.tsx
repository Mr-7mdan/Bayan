"use client"

import { useEffect, useMemo, useState } from 'react'
import { Api } from '@/lib/api'

export type AdvancedSqlUnpivotBuilderProps = {
  columns: string[]
  dsType?: string | null
  widgetId?: string
  datasourceId?: string
  source?: string
  onAddAction: (tr: { type: 'unpivot'; sourceColumns: string[]; keyColumn: string; valueColumn: string; mode?: 'auto'|'unpivot'|'union'; omitZeroNull?: boolean; scope?: { level: 'widget'|'table'|'datasource'; widgetId?: string; table?: string } }) => void
  onCancelAction?: () => void
  initial?: { type: 'unpivot'; sourceColumns: string[]; keyColumn: string; valueColumn: string; mode?: 'auto'|'unpivot'|'union'; omitZeroNull?: boolean; scope?: { level: 'widget'|'table'|'datasource'; widgetId?: string; table?: string } }
  submitLabel?: string
  dsId?: string
  tableName?: string | null
}

function supportsUnpivot(dsType?: string | null): boolean {
  const s = String(dsType || '').toLowerCase()
  return s.includes('mssql') || s.includes('sqlserver')
}

export default function AdvancedSqlUnpivotBuilder({ columns, dsType, widgetId, datasourceId, source, onAddAction, onCancelAction, initial, submitLabel }: AdvancedSqlUnpivotBuilderProps) {
  const [filter, setFilter] = useState('')
  const [selected, setSelected] = useState<string[]>([])
  const [keyColumn, setKeyColumn] = useState<string>('Denomination')
  const [valueColumn, setValueColumn] = useState<string>('Amount')
  const [mode, setMode] = useState<'auto'|'unpivot'|'union'>('auto')
  const [omitZeroNull, setOmitZeroNull] = useState<boolean>(true)
  const [scope, setScope] = useState<'widget'|'table'|'datasource'>(() => (source ? 'table' : 'datasource'))
  const [useAliases, setUseAliases] = useState<boolean>(true)

  // Load alias map from datasource customColumns (simple proxy columns)
  const [aliasMap, setAliasMap] = useState<Record<string, string>>({})
  useEffect(() => {
    let stop = false
    async function load() {
      try {
        if (!datasourceId) return
        const cfg = await Api.getDatasourceTransforms(String(datasourceId))
        const cols = Array.isArray((cfg as any)?.customColumns) ? (cfg as any).customColumns : []
        const map: Record<string,string> = {}
        for (const cc of cols) {
          const name = String(cc?.name || '').trim()
          const expr = String(cc?.expr || '').trim()
          if (!name || !expr) continue
          const m = expr.match(/^\s*(?:\[s\]\.\[([^\]]+)\]|\[([^\]]+)\]|s\.\"?([A-Za-z0-9_]+)\"?|\"([^\"]+)\")\s*$/)
          const base = (m?.[1] || m?.[2] || m?.[3] || m?.[4] || '').trim()
          if (base && name && base !== name) map[base] = name
        }
        if (!stop) setAliasMap(map)
      } catch {}
    }
    load(); return () => { stop = true }
  }, [datasourceId])

  useEffect(() => {
    if (!initial) return
    try {
      setSelected(Array.isArray(initial.sourceColumns) ? initial.sourceColumns.slice() : [])
      setKeyColumn(String(initial.keyColumn || 'Denomination'))
      setValueColumn(String(initial.valueColumn || 'Amount'))
      setMode(((initial.mode as any) || 'auto') as any)
      setOmitZeroNull(Boolean(initial.omitZeroNull))
      const sc: any = (initial as any).scope
      if (sc && sc.level) {
        if (sc.level === 'datasource') setScope('datasource')
        else if (sc.level === 'table') setScope('table')
        else if (sc.level === 'widget') setScope('widget')
      }
    } catch {}
  }, [initial])

  const avail = useMemo(() => (columns || []).filter(c => c.toLowerCase().includes(filter.toLowerCase())), [columns, filter])
  const canUnpivot = supportsUnpivot(dsType)

  const canAdd = useMemo(() => selected.length >= 2 && keyColumn.trim() !== '' && valueColumn.trim() !== '' && keyColumn.trim() !== valueColumn.trim(), [selected, keyColumn, valueColumn])

  const toggle = (c: string) => {
    setSelected(prev => prev.includes(c) ? prev.filter(x => x !== c) : [...prev, c])
  }

  return (
    <div className="rounded-md border p-2 bg-[hsl(var(--secondary)/0.6)] text-[12px]">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Search columns</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="Filter columns" value={filter} onChange={(e)=>setFilter(e.target.value)} />
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-start mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Columns to unpivot</label>
        <div className="sm:col-span-2 max-h-40 overflow-auto rounded-md border bg-card p-2 space-y-1">
          {avail.length === 0 ? (
            <div className="text-[11px] text-muted-foreground">No columns match.</div>
          ) : (
            avail.map((c) => (
              <label key={c} className="flex items-center gap-2 text-[12px]">
                <input type="checkbox" className="scale-90" checked={selected.includes(c)} onChange={()=>toggle(c)} />
                <span className="truncate" title={c}>{c}</span>
              </label>
            ))
          )}
        </div>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">New key column</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="e.g., Denomination" value={keyColumn} onChange={(e)=>setKeyColumn(e.target.value)} />
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">New value column</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="e.g., Amount" value={valueColumn} onChange={(e)=>setValueColumn(e.target.value)} />
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Mode</label>
        <div className="sm:col-span-2 flex items-center gap-2">
          <select className="h-8 px-2 rounded-md bg-card text-xs" value={mode} onChange={(e)=>setMode(e.target.value as any)}>
            <option value="auto">Auto (UNPIVOT if supported, else UNION)</option>
            <option value="union">UNION (portable)</option>
            <option value="unpivot" disabled={!canUnpivot}>UNPIVOT (SQL Server)</option>
          </select>
          {!canUnpivot && <span className="text-[11px] text-muted-foreground">UNPIVOT disabled for this datasource</span>}
        </div>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Use alias names</label>
        <div className="sm:col-span-2 flex items-center gap-2">
          <input id="useAliases" type="checkbox" className="scale-90" checked={useAliases} onChange={(e)=>setUseAliases(e.target.checked)} />
          <label htmlFor="useAliases" className="text-[11px]">Use alias columns for Unpivot instead of base names</label>
        </div>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-3">
        <label className="text-xs text-muted-foreground sm:col-span-1">Filter zero/null values</label>
        <div className="sm:col-span-2 flex items-center gap-2">
          <input id="omitzn" type="checkbox" className="scale-90" checked={omitZeroNull} onChange={(e)=>setOmitZeroNull(e.target.checked)} />
          <label htmlFor="omitzn" className="text-[11px]">Omit rows where value is 0 or NULL</label>
        </div>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-3">
        <label className="text-xs text-muted-foreground sm:col-span-1">Scope</label>
        <div className="sm:col-span-2 flex items-center gap-2">
          <select className="h-8 px-2 rounded-md bg-card text-xs" value={scope} onChange={(e)=>setScope(e.target.value as any)}>
            <option value="datasource">This Datasource</option>
            <option value="table" disabled={!source}>This Table{source?` (${source})`:''}</option>
            <option value="widget" disabled={!widgetId}>This Widget{widgetId?` (${widgetId})`:''}</option>
          </select>
          {!source && scope==='table' && (
            <span className="text-[11px] text-amber-600">Select a table first</span>
          )}
        </div>
      </div>
      <div className="text-[11px] text-muted-foreground mb-3">Example: wide columns 1,2,5,10 → long rows with {keyColumn} and {valueColumn}.</div>
      <div className="flex items-center justify-end gap-2">
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCancelAction}>Cancel</button>
        <button
          className={`text-xs px-2 py-1 rounded-md border ${canAdd? 'bg-[hsl(var(--btn3))] text-black':'opacity-60 cursor-not-allowed'}`}
          disabled={!canAdd}
          onClick={() => {
            const payload: any = {
              type: 'unpivot' as const,
              sourceColumns: selected.map((c) => useAliases ? (aliasMap[c] ?? c) : c),
              keyColumn: keyColumn.trim(),
              valueColumn: valueColumn.trim(),
            }
            // Decide mode explicitly to satisfy backend validation
            const effMode = (mode === 'auto') ? (supportsUnpivot(dsType) ? 'unpivot' : 'union') : mode
            payload.mode = effMode
            if (omitZeroNull) payload.omitZeroNull = true
            if (scope === 'datasource') payload.scope = { level: 'datasource' }
            else if (scope === 'table' && source) payload.scope = { level: 'table', table: source }
            else if (scope === 'widget' && widgetId) payload.scope = { level: 'widget', widgetId }
            onAddAction(payload)
          }}
        >{submitLabel || 'Add Unpivot / Union'}</button>
      </div>
    </div>
  )
}
