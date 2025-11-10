"use client"

import React, { useEffect, useMemo, useState } from 'react'
import { Api } from '@/lib/api'

export type PivotValue = {
  field?: string
  measureId?: string
  agg?: 'none'|'count'|'distinct'|'avg'|'sum'|'min'|'max'
  label?: string
  colorToken?: 1|2|3|4|5
  colorKey?: 'blue' | 'emerald' | 'violet' | 'amber' | 'gray' | 'rose' | 'indigo' | 'cyan' | 'pink' | 'lime' | 'fuchsia'
  stackId?: string
  style?: 'solid' | 'gradient'
  secondaryAxis?: boolean
  conditionalRules?: Array<{
    when: '>' | '>=' | '<' | '<=' | 'between' | 'equals'
    value: number | [number, number]
    color?: 'blue' | 'emerald' | 'violet' | 'amber' | 'gray' | 'rose' | 'indigo' | 'cyan' | 'pink' | 'lime' | 'fuchsia'
  }>
}
export type PivotAssignments = {
  x?: string | string[]
  values: PivotValue[]
  legend?: string | string[]
  filters: string[]
}

export function PivotBuilder({
  fields,
  measures = [],
  assignments,
  update,
  selectFieldAction,
  selected,
  disableRows,
  disableValues,
  allowMultiLegend,
  allowMultiRows,
  numericFields,
  dateLikeFields,
  datasourceId,
  source,
  widgetId,
  valueRequired,
}: {
  fields: string[]
  measures?: Array<{ id: string; name: string; formula: string }>
  assignments: PivotAssignments
  // typed as any to satisfy Next.js "serializable props" lint in client entry files
  // it's safe because this component is a Client Component and the parent is also client-side
  update: any
  selectFieldAction?: (kind: 'x'|'value'|'legend'|'filter', field: string) => void
  selected?: { kind: 'x'|'value'|'legend'|'filter'; id: string }
  disableRows?: boolean
  disableValues?: boolean
  allowMultiLegend?: boolean
  allowMultiRows?: boolean
  numericFields?: string[]
  dateLikeFields?: string[]
  datasourceId?: string
  source?: string
  widgetId?: string
  valueRequired?: boolean
}) {
  const [query, setQuery] = useState('')
  const [hover, setHover] = useState<'x'|'value'|'legend'|'filter'|null>(null)
  // Track simple alias mapping: base -> alias (created as a proxy custom column)
  const [aliasMap, setAliasMap] = useState<Record<string, string>>({})
  // Helper: find base for a displayed name (if displayed is an alias)
  const baseForDisplay = (name: string): string => {
    for (const [b, a] of Object.entries(aliasMap)) if (a === name) return b
    return name
  }
  // Initialize alias map from datasource transforms (only simple proxies)
  useEffect(() => {
    let stop = false
    async function load() {
      try {
        if (!datasourceId) return
        const cfg = await Api.getDatasourceTransforms(String(datasourceId))
        const cols = Array.isArray((cfg as any)?.customColumns) ? (cfg as any).customColumns : []
        const map: Record<string,string> = {}
        
        // Helper to normalize table names for comparison
        const norm = (s: string) => String(s || '').trim().replace(/^\[|\]|^"|"$/g, '')
        const tblEq = (a: string, b: string) => {
          const na = norm(a).split('.').pop() || ''
          const nb = norm(b).split('.').pop() || ''
          return na.toLowerCase() === nb.toLowerCase()
        }
        
        for (const cc of cols) {
          const name = String(cc?.name || '').trim()
          const expr = String(cc?.expr || '').trim()
          if (!name || !expr) continue
          
          // Check scope: only include if datasource-level OR matches current table
          const sc = cc?.scope || {}
          const lvl = String(sc?.level || 'datasource').toLowerCase()
          const scopeMatch = (lvl === 'datasource' || (lvl === 'table' && sc?.table && source && tblEq(String(sc.table), source)))
          if (!scopeMatch) continue
          
          // Accept simple proxy expressions like [Base], "Base", s.Base, s."Base"
          const m = expr.match(/^\s*(?:\[s\]\.\[([^\]]+)\]|\[([^\]]+)\]|s\.\"?([A-Za-z0-9_]+)\"?|\"([^\"]+)\")\s*$/)
          const base = (m?.[1] || m?.[2] || m?.[3] || m?.[4] || '').trim()
          if (base && name && base !== name) map[base] = name
        }
        if (!stop) setAliasMap(map)
      } catch {}
    }
    load()
    // Listen for transforms saved to refresh alias map
    const onSaved = (e: Event) => {
      try {
        const d = (e as CustomEvent).detail as { datasourceId?: string }
        if (!datasourceId || !d?.datasourceId || String(d.datasourceId) !== String(datasourceId)) return
        void load()
      } catch {}
    }
    if (typeof window !== 'undefined') window.addEventListener('datasource-transforms-saved', onSaved as EventListener)
    return () => { stop = true; if (typeof window !== 'undefined') window.removeEventListener('datasource-transforms-saved', onSaved as EventListener) }
  }, [datasourceId])
  const fieldEntries = useMemo(() => {
    const aliasValues = new Set(Object.values(aliasMap))
    const reserved = new Set(['value', '__metric__'])
    const entries: Array<{ base: string; display: string }> = []
    for (const f of fields) {
      if (reserved.has(String(f || '').toLowerCase())) continue
      // Skip duplicates if alias value also appears as a field
      if (aliasValues.has(f)) continue
      const display = aliasMap[f] ?? f
      if (display.toLowerCase().includes(query.toLowerCase())) entries.push({ base: f, display })
    }
    return entries
  }, [fields, aliasMap, query])
  const availableMeasures = useMemo(() => measures.filter(m => m.name.toLowerCase().includes(query.toLowerCase())), [measures, query])

  const [renaming, setRenaming] = useState<string | null>(null)
  const [tempName, setTempName] = useState('')

  async function saveAlias(base: string, alias: string) {
    try {
      const a = (alias || '').trim()
      if (!datasourceId || !a || a === base) return
      const cfg = await Api.getDatasourceTransforms(String(datasourceId))
      const model = cfg || { customColumns: [], transforms: [], joins: [] }
      const taken = new Set<string>([
        ...fields,
        ...((model.customColumns || []).map((c: any) => String(c?.name || '')).filter(Boolean)),
      ])
      if (taken.has(a)) return // avoid conflicts silently
      const cc: any = { name: a, expr: `[${base}]` }
      // default alias scope: table (preferred), widget if widgetId is set, datasource only as last resort
      if (source) cc.scope = { level: 'table', table: source }
      else if (widgetId) cc.scope = { level: 'widget', widgetId }
      else cc.scope = { level: 'datasource' }
      const next = { ...model, customColumns: [...(model.customColumns || []), cc] }
      await Api.saveDatasourceTransforms(String(datasourceId), next as any)
      setAliasMap((prev) => ({ ...prev, [base]: a }))
      // Notify other components to refresh transforms, columns, rows, and samples
      try { window.dispatchEvent(new CustomEvent('datasource-transforms-saved', { detail: { datasourceId } } as any)) } catch {}
      try { if (typeof window !== 'undefined' && widgetId) window.dispatchEvent(new CustomEvent('request-table-columns', { detail: { widgetId } } as any)) } catch {}
      try { if (typeof window !== 'undefined' && widgetId) window.dispatchEvent(new CustomEvent('request-table-rows', { detail: { widgetId } } as any)) } catch {}
      try { if (typeof window !== 'undefined' && widgetId) window.dispatchEvent(new CustomEvent('request-table-samples', { detail: { widgetId } } as any)) } catch {}
      // Replace in assignments
      const replLegend = (leg: PivotAssignments['legend']) => {
        if (Array.isArray(leg)) return leg.map((x) => (x === base ? a : x))
        return leg === base ? a : leg
      }
      const nextAssign: PivotAssignments = {
        ...assignments,
        x: Array.isArray(assignments.x)
          ? (assignments.x as string[]).map((x) => (x === base ? a : x))
          : (assignments.x === base ? a : assignments.x),
        legend: replLegend(assignments.legend),
        filters: (assignments.filters || []).map((f) => (f === base ? a : f)),
        values: (assignments.values || []).map((v) => (v.field === base ? { ...v, field: a } : v)),
      }
      update(nextAssign)
    } catch {}
  }

  async function removeAlias(baseOrAlias: string) {
    try {
      if (!datasourceId) { setRenaming(null); return }
      const base = baseForDisplay(baseOrAlias) // resolve to base if alias provided
      const alias = aliasMap[base]
      if (!alias) { setRenaming(null); return }
      const cfg = await Api.getDatasourceTransforms(String(datasourceId))
      const model = cfg || { customColumns: [], transforms: [], joins: [] }
      const cur = Array.isArray((model as any).customColumns) ? (model as any).customColumns : []
      const nextCC = cur.filter((cc: any) => String(cc?.name || '') !== alias)
      const next = { ...model, customColumns: nextCC }
      await Api.saveDatasourceTransforms(String(datasourceId), next as any)
      setAliasMap((prev) => { const { [base]: _omit, ...rest } = prev; return rest })
      // Replace alias back to base across assignments
      const back = (arr: string[] | undefined) => (Array.isArray(arr) ? arr.map((x) => (x === alias ? base : x)) : arr)
      const nextAssign: PivotAssignments = {
        ...assignments,
        x: Array.isArray(assignments.x)
          ? (assignments.x as string[]).map((x) => (x === alias ? base : x))
          : (assignments.x === alias ? base : assignments.x),
        legend: back(assignments.legend as any) as any,
        filters: (assignments.filters || []).map((f) => (f === alias ? base : f)),
        values: (assignments.values || []).map((v) => (v.field === alias ? { ...v, field: base } : v)),
      }
      update(nextAssign)
      setRenaming(null)
      // notify
      try { window.dispatchEvent(new CustomEvent('datasource-transforms-saved', { detail: { datasourceId } } as any)) } catch {}
    } catch {}
  }

  function startRename(name: string) {
    setRenaming(name)
    setTempName(name)
  }
  function commitRename(base: string) {
    const alias = tempName.trim()
    setRenaming(null)
    if (alias && alias !== base) saveAlias(base, alias)
  }

  function onDragStartField(field: string) {
    return (e: React.DragEvent) => {
      const payload = JSON.stringify({ kind: 'field', field })
      e.dataTransfer.setData('application/json', payload)
      try { e.dataTransfer.setData('text/plain', payload) } catch {}
      try { e.dataTransfer.setData('text', field) } catch {}
      e.dataTransfer.effectAllowed = 'move'
    }
  }
  function onDragStartMeasure(id: string) {
    return (e: React.DragEvent) => {
      const payload = JSON.stringify({ kind: 'measure', id })
      e.dataTransfer.setData('application/json', payload)
      try { e.dataTransfer.setData('text/plain', payload) } catch {}
      try { e.dataTransfer.setData('text', id) } catch {}
      e.dataTransfer.effectAllowed = 'move'
    }
  }
  // Reorder helpers
  const move = <T,>(arr: T[], from: number, to: number) => {
    const a = arr.slice()
    const [it] = a.splice(from, 1)
    a.splice(Math.max(0, Math.min(a.length, to)), 0, it)
    return a
  }
  function onDragStartReorder(kind: 'value'|'legend'|'x', fromIndex: number) {
    return (e: React.DragEvent) => {
      const payload = JSON.stringify({ kind: 'reorder', zone: kind, index: fromIndex })
      e.dataTransfer.setData('application/json', payload)
      try { e.dataTransfer.setData('text/plain', payload) } catch {}
      e.dataTransfer.effectAllowed = 'move'
    }
  }
  function onDropReorder(kind: 'value'|'legend'|'x', toIndex: number) {
    return (e: React.DragEvent) => {
      e.preventDefault()
      const raw = e.dataTransfer.getData('application/json') || e.dataTransfer.getData('text/plain') || ''
      if (!raw) return
      let data: any = null
      try { data = JSON.parse(raw) } catch { return }
      if (data?.kind !== 'reorder' || data?.zone !== kind) return
      const fromIndex = Number(data.index ?? -1)
      if (!Number.isFinite(fromIndex) || fromIndex < 0) return
      if (kind === 'value') {
        const next = move(assignments.values, fromIndex, toIndex)
        update({ ...assignments, values: next })
      } else if (kind === 'legend' && allowMultiLegend) {
        const cur = Array.isArray((assignments as any).legend) ? ((assignments as any).legend as string[]) : []
        const next = move(cur, fromIndex, toIndex)
        update({ ...assignments, legend: next })
      } else if (kind === 'x' && allowMultiRows) {
        const cur = Array.isArray(assignments.x) ? (assignments.x as string[]) : []
        const next = move(cur, fromIndex, toIndex)
        update({ ...assignments, x: next })
      }
    }
  }
  const allowDropReorder = (e: React.DragEvent) => { e.preventDefault(); try { e.dataTransfer.dropEffect = 'move' } catch {} }

  function onDrop(kind: 'x'|'value'|'legend'|'filter') {
    return (e: React.DragEvent) => {
      e.preventDefault()
      setHover(null)
      const raw = e.dataTransfer.getData('application/json') || e.dataTransfer.getData('text/plain') || e.dataTransfer.getData('text')
      if (!raw) return
      let data: any = null
      try { data = JSON.parse(raw) } catch {
        // Fallback: treat raw as a bare field name
        data = { kind: 'field', field: raw }
      }
      if (typeof window !== 'undefined' && process.env.NODE_ENV !== 'production') {
        try { console.debug('[PivotBuilder] drop', { kind, raw, data }) } catch {}
      }
      // Reorder drop on zone container (append to end)
      if (data.kind === 'reorder' && (data.zone === 'value' || data.zone === 'legend' || data.zone === 'x') && data.zone === kind) {
        const fromIndex = Number(data.index ?? -1)
        if (Number.isFinite(fromIndex) && fromIndex >= 0) {
          if (kind === 'value') {
            const toIndex = Math.max(0, assignments.values.length - 1)
            const next = move(assignments.values, fromIndex, toIndex)
            update({ ...assignments, values: next })
          } else if (kind === 'legend' && allowMultiLegend) {
            const cur = Array.isArray((assignments as any).legend) ? ((assignments as any).legend as string[]) : []
            const toIndex = Math.max(0, cur.length - 1)
            const next = move(cur, fromIndex, toIndex)
            update({ ...assignments, legend: next })
          } else if (kind === 'x' && allowMultiRows) {
            const cur = Array.isArray(assignments.x) ? (assignments.x as string[]) : []
            const toIndex = Math.max(0, cur.length - 1)
            const next = move(cur, fromIndex, toIndex)
            update({ ...assignments, x: next })
          }
        }
        return
      }
      if (kind === 'x' || kind === 'legend' || kind === 'filter') {
        if (data.kind !== 'field') return
        const field = data.field as string
        if (kind === 'x') {
          if (allowMultiRows) {
            const current = Array.isArray(assignments.x) ? (assignments.x as string[]) : (assignments.x ? [String(assignments.x)] : [])
            if (current.includes(field)) return
            update({ ...assignments, x: [...current, field] })
          } else {
            update({ ...assignments, x: field })
          }
        }
        else if (kind === 'legend') {
          if (allowMultiLegend) {
            const current = Array.isArray((assignments as any).legend) ? ((assignments as any).legend as string[]) : ((assignments as any).legend ? [String((assignments as any).legend)] : [])
            if (Array.isArray(current) && current.includes(field)) return
            update({ ...assignments, legend: [...(Array.isArray(current) ? current : []), field] })
          } else {
            update({ ...assignments, legend: field })
          }
        }
        else if (kind === 'filter') {
          const cur = Array.isArray(assignments.filters) ? assignments.filters : []
          if (cur.includes(field)) return
          update({ ...assignments, filters: [...cur, field] })
        }
        return
      }
      if (kind === 'value') {
        if (data.kind === 'field') {
          const field = data.field as string
          const vals = Array.isArray(assignments.values) ? assignments.values : []
          if (vals.some(v => v.field === field)) return
          const base = baseForDisplay(field)
          const isNumeric = Array.isArray(numericFields) ? (numericFields.includes(field) || numericFields.includes(base)) : false
          const agg: PivotValue['agg'] = isNumeric ? 'sum' : 'count'
          update({ ...assignments, values: [...vals, { field, agg }] })
        } else if (data.kind === 'measure') {
          const id = data.id as string
          const vals = Array.isArray(assignments.values) ? assignments.values : []
          if (vals.some(v => v.measureId === id)) return
          const m = measures.find(mm => mm.id === id)
          update({ ...assignments, values: [...vals, { measureId: id, label: m?.name, agg: 'sum' }] })
        }
      }
    }
  }
  function allowDrop(e: React.DragEvent) {
    e.preventDefault()
    setHover((h) => h) // keep current
    try { e.dataTransfer.dropEffect = 'move' } catch {}
  }

  function remove(kind: 'x'|'value'|'legend'|'filter', field: string) {
    if (kind === 'x') {
      if (allowMultiRows) {
        const cur = Array.isArray(assignments.x) ? (assignments.x as string[]) : []
        update({ ...assignments, x: cur.filter(f => f !== field) })
      } else {
        update({ ...assignments, x: undefined })
      }
    }
    else if (kind === 'legend') {
      if (allowMultiLegend) {
        const current = Array.isArray((assignments as any).legend) ? ((assignments as any).legend as string[]) : []
        update({ ...assignments, legend: current.filter((f) => f !== field) })
      } else {
        update({ ...assignments, legend: undefined })
      }
    }
    else if (kind === 'filter') update({ ...assignments, filters: assignments.filters.filter(f => f !== field) })
    else update({
      ...assignments,
      values: assignments.values.filter((v, i) => {
        if (typeof field === 'string' && /^v\d+$/.test(field)) {
          const idx = Number(field.slice(1))
          return i !== idx
        }
        return (v.field ? v.field !== field : true) && (v.measureId ? v.measureId !== field : true)
      })
    })
  }

  function Chip({ children, onRemove, onClick, onDoubleClick, active, draggable, onDragStart, onDrop, onDragOver }: { children: React.ReactNode; onRemove?: () => void; onClick?: () => void; onDoubleClick?: () => void; active?: boolean; draggable?: boolean; onDragStart?: (e: React.DragEvent) => void; onDrop?: (e: React.DragEvent) => void; onDragOver?: (e: React.DragEvent) => void }) {
    return (
      <span
        onClick={onClick}
        onDoubleClick={onDoubleClick}
        draggable={draggable}
        onDragStart={onDragStart}
        onDrop={onDrop}
        onDragOver={onDragOver}
        className={`inline-flex items-center gap-1 px-2 py-1 rounded border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] text-xs ${draggable ? 'cursor-grab' : 'cursor-pointer'}${active ? ' ring-2 ring-[hsl(var(--border))]' : ''} max-w-full min-w-0`}
      >
        <span className="flex items-center gap-1 overflow-hidden min-w-0">{children}</span>
        {onRemove && (
          <button className="text-[10px] opacity-70 hover:opacity-100" onClick={(e) => { e.stopPropagation(); onRemove() }}>✕</button>
        )}
      </span>
    )
  }

  function Zone({ title, kind, children, onDrop, disabled }: { title: string; kind: 'x'|'value'|'legend'|'filter'; children?: React.ReactNode; onDrop: (e: React.DragEvent) => void; disabled?: boolean }) {
    return (
      <div
        onDrop={disabled ? undefined : onDrop}
        onDragOver={disabled ? undefined : allowDrop}
        onDragEnter={() => { if (!disabled) setHover(kind) }}
        onDragLeave={() => { if (!disabled) setHover((h) => (h === kind ? null : h)) }}
        className={`rounded-md border border-[hsl(var(--border))] bg-card p-2 min-h-[64px] transition-colors${hover === kind ? ' border-dashed ring-2 ring-[hsl(var(--border))] bg-[hsl(var(--secondary))]' : ''} ${disabled ? 'opacity-50 pointer-events-none' : ''}`}
      >
        <div className="text-xs font-medium mb-1">{title}</div>
        <div className="flex flex-wrap gap-1 min-w-0">
          {children}
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      {((valueRequired ?? false) && (!Array.isArray(assignments.values) || assignments.values.length === 0)) && (
        <div className="text-[11px] px-2 py-1 rounded border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] text-muted-foreground">
          Select at least one <span className="font-semibold">Value</span> (drag a field or measure) to run this widget.
        </div>
      )}
      <div className="rounded-md border border-[hsl(var(--border))] bg-card p-2">
        <div className="text-xs font-semibold mb-1">Fields</div>
        <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search fields" className="w-full mb-2 px-2 py-1 rounded border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] text-xs" />
        <div className="flex flex-wrap gap-1">
          {fieldEntries.map(({ base: f, display }) => (
            <span
              key={f}
              draggable={renaming !== display}
              onDragStart={renaming !== display ? onDragStartField(aliasMap[f] ?? f) : undefined}
              onDoubleClick={() => startRename(display)}
              className="inline-flex items-center gap-1 px-2 py-1 rounded border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] text-xs min-w-0"
              title={f}
            >
              {renaming === display ? (
                <span className="inline-flex items-center gap-1">
                  <input
                    autoFocus
                    className="text-[11px] px-1 py-0.5 rounded border border-[hsl(var(--border))] bg-card min-w-[80px]"
                    value={tempName}
                    onChange={(e) => setTempName(e.target.value)}
                    onBlur={() => commitRename(baseForDisplay(display))}
                    onKeyDown={(e) => { if (e.key === 'Enter') commitRename(baseForDisplay(display)); if (e.key === 'Escape') setRenaming(null) }}
                  />
                  <button
                    type="button"
                    className="text-[10px] opacity-70 hover:opacity-100"
                    title="Remove alias"
                    onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); removeAlias(display) }}
                  >✕</button>
                </span>
              ) : (
                <span className="truncate min-w-0 max-w-full cursor-grab">{display}</span>
              )}
            </span>
          ))}
        </div>
        {availableMeasures.length > 0 && (
          <div className="mt-3">
            <div className="text-xs font-semibold mb-1">Measures</div>
            <div className="flex flex-wrap gap-1">
              {availableMeasures.map((m) => (
                <span key={m.id} draggable onDragStart={onDragStartMeasure(m.id)} className="inline-flex items-center gap-1 px-2 py-1 rounded border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] text-xs cursor-grab">
                  ∑ {m.name}
                </span>
              ))}
            </div>
          </div>
        )}
        {Array.isArray(dateLikeFields) && dateLikeFields.length > 0 && (
          <div className="mt-3">
            <div className="text-xs font-semibold mb-1">Date parts</div>
            <div className="space-y-2 max-h-56 overflow-auto">
              {dateLikeFields
                .filter((f) => f.toLowerCase().includes(query.toLowerCase()))
                .map((base) => (
                  <div key={base}>
                    <div className="text-[11px] opacity-80 mb-1">{base}</div>
                    <div className="flex flex-wrap gap-1 max-h-[84px] overflow-y-auto pr-1">
                      {([`${base} (Year)`, `${base} (Quarter)`, `${base} (Month)`, `${base} (Month Name)`, `${base} (Month Short)`, `${base} (Week)`, `${base} (Day)`, `${base} (Day Name)`, `${base} (Day Short)`] as string[]).map((dp) => (
                        <span key={dp} draggable onDragStart={onDragStartField(dp)} className="inline-flex items-center gap-1 px-2 py-1 rounded border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] text-xs cursor-grab">
                          {dp}
                        </span>
                      ))}
                    </div>
                  </div>
                ))}
            </div>
          </div>
        )}
      </div>

      <div className="grid grid-cols-2 gap-3">
        <Zone title="Filters" kind="filter" onDrop={onDrop('filter')}>
          {assignments.filters.map((f) => (
            <Chip
              key={f}
              active={selected?.kind === 'filter' && selected.id === f}
              onRemove={() => remove('filter', f)}
              onClick={() => selectFieldAction?.('filter', f)}
              onDoubleClick={() => startRename(f)}
              draggable
              onDragStart={(e) => { const p = JSON.stringify({ kind: 'field', field: f }); e.dataTransfer.setData('application/json', p); try { e.dataTransfer.setData('text/plain', p) } catch {}; e.dataTransfer.effectAllowed = 'move' }}
            >
              {renaming === f ? (
                <span className="inline-flex items-center gap-1">
                  <input
                    autoFocus
                    className="text-[11px] px-1 py-0.5 rounded border border-[hsl(var(--border))] bg-card min-w-[80px]"
                    value={tempName}
                    onChange={(e) => setTempName(e.target.value)}
                    onBlur={() => commitRename(baseForDisplay(f))}
                    onKeyDown={(e) => { if (e.key === 'Enter') commitRename(baseForDisplay(f)); if (e.key === 'Escape') setRenaming(null) }}
                  />
                  <button type="button" className="text-[10px] opacity-70 hover:opacity-100" title="Remove alias" onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); removeAlias(f) }}>✕</button>
                </span>
              ) : (
                f
              )}
            </Chip>
          ))}
        </Zone>
        <Zone title={allowMultiLegend ? 'Columns (Legend)' : 'Columns (Legend)'} kind="legend" onDrop={onDrop('legend')}>
          {allowMultiLegend ? (
            (Array.isArray((assignments as any).legend)
              ? ((assignments as any).legend as string[])
              : (((assignments as any).legend ? [String((assignments as any).legend)] : []) as string[])
            ).map((f, i) => (
              <Chip
                key={f}
                active={selected?.kind === 'legend' && selected.id === f}
                onRemove={() => remove('legend', f)}
                onClick={() => selectFieldAction?.('legend', f)}
                onDrop={onDropReorder('legend', i)}
                onDragOver={allowDropReorder}
              >
                <span
                  title="Drag to reorder"
                  className="opacity-70 hover:opacity-100 cursor-grab select-none"
                  draggable
                  onDragStart={onDragStartReorder('legend', i)}
                >≡</span>
                <span onDoubleClick={() => startRename(f)} className="truncate min-w-0">
                  {renaming === f ? (
                    <span className="inline-flex items-center gap-1">
                      <input
                        autoFocus
                        className="text-[11px] px-1 py-0.5 rounded border border-[hsl(var(--border))] bg-card min-w-[80px]"
                        value={tempName}
                        onChange={(e) => setTempName(e.target.value)}
                        onBlur={() => commitRename(baseForDisplay(f))}
                        onKeyDown={(e) => { if (e.key === 'Enter') commitRename(baseForDisplay(f)); if (e.key === 'Escape') setRenaming(null) }}
                      />
                      <button type="button" className="text-[10px] opacity-70 hover:opacity-100" title="Remove alias" onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); removeAlias(f) }}>✕</button>
                    </span>
                  ) : f}
                </span>
              </Chip>
            ))
          ) : (
            assignments.legend && (
              <Chip
                active={selected?.kind === 'legend' && selected.id === String(assignments.legend!)}
                onRemove={() => remove('legend', String(assignments.legend!))}
                onClick={() => selectFieldAction?.('legend', String(assignments.legend!))}
              >
                <span onDoubleClick={() => startRename(String(assignments.legend))} className="truncate min-w-0">
                  {renaming === String(assignments.legend) ? (
                    <span className="inline-flex items-center gap-1">
                      <input
                        autoFocus
                        className="text-[11px] px-1 py-0.5 rounded border bg-card min-w-[80px]"
                        value={tempName}
                        onChange={(e) => setTempName(e.target.value)}
                        onBlur={() => commitRename(baseForDisplay(String(assignments.legend)))}
                        onKeyDown={(e) => { if (e.key === 'Enter') commitRename(baseForDisplay(String(assignments.legend))); if (e.key === 'Escape') setRenaming(null) }}
                      />
                      <button type="button" className="text-[10px] opacity-70 hover:opacity-100" title="Remove alias" onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); removeAlias(String(assignments.legend)) }}>✕</button>
                    </span>
                  ) : String(assignments.legend)}
                </span>
              </Chip>
            )
          )}
        </Zone>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <Zone title="Rows (X Axis)" kind="x" onDrop={onDrop('x')} disabled={!!disableRows}>
          {allowMultiRows ? (
            (Array.isArray(assignments.x)
              ? (assignments.x as string[])
              : ((assignments.x ? [String(assignments.x)] : []) as string[])
            ).map((f, i) => (
              <Chip
                key={`x-${f}-${i}`}
                active={selected?.kind === 'x' && selected.id === f}
                onRemove={() => remove('x', f)}
                onClick={() => selectFieldAction?.('x', f)}
                onDrop={onDropReorder('x', i)}
                onDragOver={allowDropReorder}
              >
                <span
                  title="Drag to reorder"
                  className="opacity-70 hover:opacity-100 cursor-grab select-none"
                  draggable
                  onDragStart={onDragStartReorder('x', i)}
                >≡</span>
                <span onDoubleClick={() => startRename(f)} className="truncate min-w-0">
                  {renaming === f ? (
                    <input
                      autoFocus
                      className="text-[11px] px-1 py-0.5 rounded border border-[hsl(var(--border))] bg-card min-w-[80px]"
                      value={tempName}
                      onChange={(e) => setTempName(e.target.value)}
                      onBlur={() => commitRename(f)}
                      onKeyDown={(e) => { if (e.key === 'Enter') commitRename(f); if (e.key === 'Escape') setRenaming(null) }}
                    />
                  ) : f}
                </span>
              </Chip>
            ))
          ) : (
            assignments.x && typeof assignments.x === 'string' && (
              <Chip
                active={selected?.kind === 'x' && selected.id === assignments.x}
                onRemove={() => remove('x', String(assignments.x))}
                onClick={() => selectFieldAction?.('x', String(assignments.x))}
                draggable
                onDragStart={(e) => { const p = JSON.stringify({ kind: 'field', field: String(assignments.x) }); e.dataTransfer.setData('application/json', p); try { e.dataTransfer.setData('text/plain', p) } catch {}; e.dataTransfer.effectAllowed = 'move' }}
              >
                {renaming === String(assignments.x) ? (
                  <span className="inline-flex items-center gap-1">
                    <input
                      autoFocus
                      className="text-[11px] px-1 py-0.5 rounded border border-[hsl(var(--border))] bg-card min-w-[80px]"
                      value={tempName}
                      onChange={(e) => setTempName(e.target.value)}
                      onBlur={() => commitRename(baseForDisplay(String(assignments.x)))}
                      onKeyDown={(e) => { if (e.key === 'Enter') commitRename(baseForDisplay(String(assignments.x))); if (e.key === 'Escape') setRenaming(null) }}
                    />
                    <button type="button" className="text-[10px] opacity-70 hover:opacity-100" title="Remove alias" onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); removeAlias(String(assignments.x)) }}>✕</button>
                  </span>
                ) : (
                  <span onDoubleClick={() => startRename(String(assignments.x))}>{String(assignments.x)}</span>
                )}
              </Chip>
            )
          )}
        </Zone>
        <Zone title="Values" kind="value" onDrop={onDrop('value')} disabled={!!disableValues}>
          {assignments.values.map((v, idx) => {
            const key = v.measureId ? v.measureId : (v.field || `v${idx}`)
            const label = v.measureId
              ? (measures.find(m => m.id === v.measureId)?.name || v.label || 'Measure')
              : (v.label || v.field || (v.agg ? String(v.agg) : 'value'))
            // Allow full aggregator set regardless of numeric inference; backend will enforce types
            const allowedAggs = ['none','count','distinct','sum','avg','min','max'] as const
            const currentAgg = (v.agg || 'count') as any
            const safeAgg = (allowedAggs as readonly string[]).includes(currentAgg) ? currentAgg : 'count'
            if (!v.measureId && currentAgg !== safeAgg) {
              // Auto-correct invalid aggregator; schedule to avoid render updates
              setTimeout(() => {
                const nextVals = assignments.values.map((it, j) => j === idx ? { ...it, agg: safeAgg as any } : it)
                update({ ...assignments, values: nextVals })
              }, 0)
            }
            return (
              <Chip
                key={key}
                active={selected?.kind === 'value' && selected.id === key!}
                onRemove={() => remove('value', key!)}
                onClick={() => selectFieldAction?.('value', key!)}
                onDrop={onDropReorder('value', idx)}
                onDragOver={allowDropReorder}
              >
                <span className="flex items-center gap-1 min-w-0 overflow-hidden">
                  <span
                    title="Drag to reorder"
                    className="opacity-70 hover:opacity-100 cursor-grab select-none"
                    draggable
                    onDragStart={onDragStartReorder('value', idx)}
                  >≡</span>
                  {v.measureId ? (
                    <span className="truncate min-w-0 max-w-full" title={String(label)}>{label}</span>
                  ) : (
                    <span className="truncate min-w-0 max-w-full" title={String(label)} onDoubleClick={() => startRename(String(v.field))}>
                      {renaming === String(v.field) ? (
                        <span className="inline-flex items-center gap-1">
                          <input
                            autoFocus
                            className="text-[11px] px-1 py-0.5 rounded border border-[hsl(var(--border))] bg-card min-w-[80px]"
                            value={tempName}
                            onChange={(e) => setTempName(e.target.value)}
                            onBlur={() => commitRename(baseForDisplay(String(v.field)))}
                            onKeyDown={(e) => { if (e.key === 'Enter') commitRename(baseForDisplay(String(v.field))); if (e.key === 'Escape') setRenaming(null) }}
                          />
                          <button type="button" className="text-[10px] opacity-70 hover:opacity-100" title="Remove alias" onMouseDown={(e) => { e.preventDefault(); e.stopPropagation(); removeAlias(String(v.field || '')) }}>✕</button>
                        </span>
                      ) : (
                        label
                      )}
                    </span>
                  )}
                  <select
                    className="text-[11px] px-1 py-0.5 rounded border border-[hsl(var(--border))] bg-[hsl(var(--secondary))] shrink-0 focus:outline-none focus:ring-1 focus:ring-[hsl(var(--border))] focus:border-[hsl(var(--border))]"
                    value={safeAgg}
                    onChange={(e) => {
                      const agg = e.target.value as 'none'|'count'|'distinct'|'avg'|'sum'|'min'|'max'
                      const nextVals = assignments.values.map((it, j) => j === idx ? { ...it, agg } : it)
                      update({ ...assignments, values: nextVals })
                    }}
                  >
                    {(allowedAggs as readonly string[]).map(a => (<option key={a} value={a}>{a}</option>))}
                  </select>
                </span>
              </Chip>
            )
          })}
        </Zone>
      </div>
      
    </div>
  )
}
