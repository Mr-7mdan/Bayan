"use client"

import React, { useEffect, useMemo, useState } from 'react'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { Api } from '@/lib/api'

export type PivotLayoutState = {
  rows: string[]
  cols: string[]
  vals: string[]
  filters: Record<string, string[]>
  labels: Record<string, string>
}

export default function PivotLayout({
  fields,
  state,
  onChange,
  samplesByField,
  widgetId,
  datasourceId,
}: {
  fields: string[]
  state: PivotLayoutState
  // typed as any to satisfy Next.js "serializable props" lint in client entry files
  // safe: parent and child are client components
  onChange: any
  samplesByField?: Record<string, string[]>
  widgetId?: string
  datasourceId?: string
}) {
  const { env } = useEnvironment()
  const [query, setQuery] = useState('')
  const [editingLabel, setEditingLabel] = useState<string | null>(null)
  const [editingFilter, setEditingFilter] = useState<string | null>(null)
  const [filterQuery, setFilterQuery] = useState('')
  const available = useMemo(() => fields.filter(f => f.toLowerCase().includes(query.toLowerCase())), [fields, query])

  // Date-like detection using samplesByField
  const isDateSample = (s: string) => {
    if (!s) return false
    if (/^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?$/.test(s)) return true
    if (/^\d{10,13}$/.test(s)) return true
    if (/^[0-1]?\d\/[0-3]?\d\/\d{4}(?:\s+\d{2}:\d{2}(?::\d{2})?)?$/.test(s)) return true
    return false
  }
  const dateLikeFields = useMemo(() => {
    const keys = Object.keys(samplesByField || {})
    const out: string[] = []
    keys.forEach((k) => {
      const arr = (samplesByField?.[k] || []).slice(0, 10)
      if (arr.some((v) => isDateSample(String(v)))) out.push(k)
    })
    // Also include fields list as fallback when no samples present
    if (out.length === 0 && fields.length > 0) {
      fields.forEach((k) => {
        if ((samplesByField?.[k] || []).length === 0) {
          // heuristic: names containing date/time
          if (/date|time|dt/i.test(k)) out.push(k)
        }
      })
    }
    return out
  }, [samplesByField, fields])

  const datePartsFor = (base: string): string[] => [
    `${base} (Year)`,
    `${base} (Quarter)`,
    `${base} (Month)`,
    `${base} (Month Name)`,
    `${base} (Month Short)`,
    `${base} (Week)`,
    `${base} (Day)`,
    `${base} (Day Name)`,
    `${base} (Day Short)`,
  ]

  // When opening a filter, request fresh samples from the TableCard
  useEffect(() => {
    if (!editingFilter || !widgetId) return
    try { window.dispatchEvent(new CustomEvent('request-table-samples', { detail: { widgetId } })) } catch {}
  }, [editingFilter, widgetId])

  function onDragStartField(field: string) {
    return (e: React.DragEvent) => {
      const payload = JSON.stringify({ kind: 'field', field })
      e.dataTransfer.setData('application/json', payload)
      try { e.dataTransfer.setData('text/plain', payload) } catch {}
      try { e.dataTransfer.setData('text', field) } catch {}
      e.dataTransfer.effectAllowed = 'move'
    }
  }
  function allowDrop(e: React.DragEvent) {
    e.preventDefault()
    try { e.dataTransfer.dropEffect = 'move' } catch {}
  }
  function onDrop(zone: 'rows'|'cols'|'vals'|'filters') {
    return (e: React.DragEvent) => {
      e.preventDefault()
      const raw = e.dataTransfer.getData('application/json') || e.dataTransfer.getData('text/plain') || e.dataTransfer.getData('text')
      if (!raw) return
      let data: any = null
      try { data = JSON.parse(raw) } catch { data = { kind: 'field', field: raw } }
      if (data.kind !== 'field') return
      const field = String(data.field)
      if (zone === 'rows') {
        if (Array.isArray(state.rows) && state.rows.includes(field)) return
        onChange({ ...state, rows: [...(Array.isArray(state.rows) ? state.rows : []), field] })
      } else if (zone === 'cols') {
        if (Array.isArray(state.cols) && state.cols.includes(field)) return
        onChange({ ...state, cols: [...(Array.isArray(state.cols) ? state.cols : []), field] })
      } else if (zone === 'vals') {
        if (Array.isArray(state.vals) && state.vals.includes(field)) return
        onChange({ ...state, vals: [...(Array.isArray(state.vals) ? state.vals : []), field] })
      } else if (zone === 'filters') {
        if (state.filters[field]) return
        onChange({ ...state, filters: { ...state.filters, [field]: [] } })
      }
    }
  }
  function remove(zone: 'rows'|'cols'|'vals'|'filters', field: string) {
    if (zone === 'rows') onChange({ ...state, rows: state.rows.filter(f => f !== field) })
    if (zone === 'cols') onChange({ ...state, cols: state.cols.filter(f => f !== field) })
    if (zone === 'vals') onChange({ ...state, vals: state.vals.filter(f => f !== field) })
    if (zone === 'filters') {
      const { [field]: _, ...rest } = state.filters
      onChange({ ...state, filters: rest })
    }
  }

  function Chip({ children, onRemove, onClick, onDoubleClick, active, draggable, onDragStart }: { children: React.ReactNode; onRemove?: () => void; onClick?: () => void; onDoubleClick?: () => void; active?: boolean; draggable?: boolean; onDragStart?: (e: React.DragEvent) => void }) {
    return (
      <span
        onClick={onClick}
        onDoubleClick={onDoubleClick}
        draggable={draggable}
        onDragStart={onDragStart}
        className={`inline-flex items-center gap-1 px-2 py-1 rounded border bg-[hsl(var(--secondary))] text-xs ${draggable ? 'cursor-grab' : 'cursor-pointer'}${active ? ' ring-2 ring-[hsl(var(--primary))]' : ''}`}
      >
        {children}
        {onRemove && (
          <button className="text-[10px] opacity-70 hover:opacity-100" onClick={(e) => { e.stopPropagation(); onRemove() }}>✕</button>
        )}
      </span>
    )
  }

  function Zone({ title, zone, children }: { title: string; zone: 'rows'|'cols'|'vals'|'filters'; children?: React.ReactNode }) {
    return (
      <div
        onDrop={onDrop(zone)}
        onDragOver={allowDrop}
        className="rounded-md border bg-card p-2 min-h-[64px]"
      >
        <div className="text-xs font-medium mb-1">{title}</div>
        <div className="flex flex-wrap gap-1">
          {children}
        </div>
      </div>
    )
  }

  function LabelEditor({ field }: { field: string }) {
    const label = state.labels?.[field] || ''
    return (
      <input
        autoFocus
        className="text-[11px] px-1 py-0.5 rounded border bg-[hsl(var(--secondary))]"
        placeholder="Label"
        value={label}
        onChange={(e) => onChange({ ...state, labels: { ...(state.labels || {}), [field]: e.target.value } })}
        onBlur={async () => {
          const alias = (state.labels?.[field] || '').trim()
          setEditingLabel(null)
          try {
            if (!datasourceId) return
            if (!alias || alias === field) return
            const cfg = await Api.getDatasourceTransforms(String(datasourceId))
            const model = cfg || { customColumns: [], transforms: [], joins: [] }
            const taken = new Set<string>([
              ...fields,
              ...((model.customColumns || []).map((c: any) => String(c?.name || '')).filter(Boolean)),
            ])
            if (taken.has(alias)) return
            const cc = { name: alias, expr: `[${field}]` }
            const next = { ...model, customColumns: [...(model.customColumns || []), cc] }
            await Api.saveDatasourceTransforms(String(datasourceId), next as any)
          } catch {}
        }}
      />
    )
  }

  return (
    <div className="space-y-3">
      <div className="rounded-md border bg-card p-2">
        <div className="text-xs font-semibold mb-1">Fields</div>
        <input value={query} onChange={(e) => setQuery(e.target.value)} placeholder="Search fields" className="w-full mb-2 px-2 py-1 rounded bg-[hsl(var(--secondary))] text-xs" />
        <div className="flex flex-wrap gap-1">
          {available.map((f) => (
            <span key={f} draggable onDragStart={onDragStartField(f)} className="inline-flex items-center gap-1 px-2 py-1 rounded border bg-[hsl(var(--secondary))] text-xs cursor-grab">
              {f}
            </span>
          ))}
        </div>
      </div>

      {dateLikeFields.length > 0 && (
        <div className="rounded-md border bg-card p-2">
          <div className="text-xs font-semibold mb-1">Date parts</div>
          <div className="space-y-2 max-h-56 overflow-auto">
            {dateLikeFields
              .filter((f) => f.toLowerCase().includes(query.toLowerCase()))
              .map((base) => (
                <div key={base}>
                  <div className="text-[11px] opacity-80 mb-1">{base}</div>
                  <div className="flex flex-wrap gap-1 max-h-[84px] overflow-y-auto pr-1">
                    {datePartsFor(base).map((dp) => (
                      <span key={dp} draggable onDragStart={onDragStartField(dp)} className="inline-flex items-center gap-1 px-2 py-1 rounded border bg-[hsl(var(--secondary))] text-xs cursor-grab">
                        {dp}
                      </span>
                    ))}
                  </div>
                </div>
              ))}
          </div>
        </div>
      )}

      <div className="grid grid-cols-2 gap-3">
        <Zone title="Rows" zone="rows">
          {state.rows.map((f) => (
            <Chip key={`r-${f}`} onRemove={() => remove('rows', f)} onDoubleClick={() => setEditingLabel(f)}>
              <span className="truncate max-w-[140px]" title={f}>{state.labels?.[f] || f}</span>
              {editingLabel === f && <LabelEditor field={f} />}
            </Chip>
          ))}
        </Zone>
        <Zone title="Columns" zone="cols">
          {state.cols.map((f) => (
            <Chip key={`c-${f}`} onRemove={() => remove('cols', f)} onDoubleClick={() => setEditingLabel(f)}>
              <span className="truncate max-w-[140px]" title={f}>{state.labels?.[f] || f}</span>
              {editingLabel === f && <LabelEditor field={f} />}
            </Chip>
          ))}
        </Zone>
      </div>
      <div className="grid grid-cols-2 gap-3">
        <Zone title="Values" zone="vals">
          {state.vals.map((f) => (
            <Chip key={`v-${f}`} onRemove={() => remove('vals', f)} onDoubleClick={() => setEditingLabel(f)}>
              <span className="truncate max-w-[140px]" title={f}>{state.labels?.[f] || f}</span>
              {editingLabel === f && <LabelEditor field={f} />}
            </Chip>
          ))}
        </Zone>
        <Zone title="Filters" zone="filters">
          {Object.keys(state.filters).map((f) => (
            <Chip key={`flt-${f}`} onRemove={() => remove('filters', f)} onClick={() => setEditingFilter(f)} onDoubleClick={() => setEditingLabel(f)}>
              <span className="truncate max-w-[140px]" title={f}>{state.labels?.[f] || f}</span>
              {editingLabel === f && <LabelEditor field={f} />}
            </Chip>
          ))}
          {editingFilter && (
            <div className="mt-2 w-full rounded border bg-[hsl(var(--secondary))] p-2">
              <div className="flex items-center justify-between mb-2">
                <div className="text-[11px] font-medium">Filter values: {state.labels?.[editingFilter] || editingFilter}</div>
                <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { setEditingFilter(null); setFilterQuery('') }}>Close</button>
              </div>
              <input
                value={filterQuery}
                onChange={(e) => setFilterQuery(e.target.value)}
                placeholder="Search values"
                className="w-full mb-2 px-2 py-1 rounded bg-card text-xs"
              />
              {(() => {
                const DERIVED_RE = /^(.*) \((Year|Quarter|Month|Month Name|Month Short|Week|Day|Day Name|Day Short)\)$/
                const m = editingFilter.match(DERIVED_RE)
                const monthNames = ['January','February','March','April','May','June','July','August','September','October','November','December']
                const monthShort = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
                const dayNames = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']
                const dayShort = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']
                const toDate = (v: any): Date | null => {
                  if (v == null) return null
                  const s = String(v)
                  if (/^\d{10,13}$/.test(s)) { const n = Number(s); const ms = s.length === 10 ? n*1000 : n; const d = new Date(ms); return isNaN(d.getTime())?null:d }
                  const norm = s.replace(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}(:\d{2})?)$/, '$1T$2')
                  const d = new Date(norm); if (!isNaN(d.getTime())) return d
                  const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/); if (iso) { const d0 = new Date(`${iso[1]}-${iso[2]}-${iso[3]}T00:00:00`); return isNaN(d0.getTime())?null:d0 }
                  return null
                }
                const weekNum = (date: Date): number => {
                  if (env.weekStart === 'sun') {
                    const jan1 = new Date(date.getFullYear(), 0, 1)
                    const day0 = Math.floor((new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime() - jan1.getTime()) / 86400000)
                    return Math.floor((day0 + jan1.getDay()) / 7) + 1
                  }
                  // ISO week (Mon-based)
                  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()))
                  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7))
                  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1))
                  return Math.ceil(((d.getTime() - yearStart.getTime()) / 86400000 + 1) / 7)
                }
                const derive = (part: string, base: any): any => {
                  const d = toDate(base); if (!d) return null
                  switch (part) {
                    case 'Year': return d.getFullYear()
                    case 'Quarter': return Math.floor(d.getMonth()/3)+1
                    case 'Month': return d.getMonth()+1
                    case 'Month Name': return monthNames[d.getMonth()]
                    case 'Month Short': return monthShort[d.getMonth()]
                    case 'Week': return weekNum(d)
                    case 'Day': return d.getDate()
                    case 'Day Name': return dayNames[d.getDay()]
                    case 'Day Short': return dayShort[d.getDay()]
                    default: return null
                  }
                }
                let options: string[] = (samplesByField?.[editingFilter] || [])
                if (m) {
                  const base = m[1]; const part = m[2]
                  const baseSamples = samplesByField?.[base] || []
                  const set = new Set<string>()
                  baseSamples.forEach((s) => { const v = derive(part, s); if (v !== null && v !== undefined) set.add(String(v)) })
                  options = Array.from(set.values())
                }
                const filtered = options.filter((v) => v.toLowerCase().includes(filterQuery.toLowerCase()))
                return (
                  <div className="max-h-40 overflow-auto rounded border bg-card p-1">
                    <select
                  multiple
                  size={8}
                  className="w-full text-[12px] px-1 py-1 bg-transparent"
                      value={state.filters[editingFilter] || []}
                      onChange={(e) => {
                        const vals = Array.from(e.target.selectedOptions).map(o => o.value)
                        onChange({ ...state, filters: { ...state.filters, [editingFilter]: vals } })
                      }}
                    >
                      {filtered.map((v) => <option key={v} value={v}>{v}</option>)}
                    </select>
                  </div>
                )
              })()}
            </div>
          )}
        </Zone>
      </div>
    </div>
  )
}
