"use client"

import { useEffect, useMemo, useState } from 'react'
import type { IntrospectResponse } from '@/lib/api'

export type JoinColumn = { name: string; alias?: string }
export type JoinAggregate = { fn: 'sum'|'avg'|'min'|'max'|'count'|'string_agg'|'array_agg'; column: string; alias: string }

export type AdvancedSqlJoinBuilderProps = {
  schema?: IntrospectResponse
  baseSource?: string
  baseColumns: string[]
  onAddAction: (j: { joinType: 'left'|'inner'|'right'; targetTable: string; sourceKey: string; targetKey: string; columns?: JoinColumn[]; aggregate?: JoinAggregate }) => void
  onCancelAction?: () => void
  initial?: { joinType?: 'left'|'inner'|'right'; targetTable?: string; sourceKey?: string; targetKey?: string; columns?: JoinColumn[]; aggregate?: JoinAggregate; filter?: { op: any; left: string; right: any } }
  submitLabel?: string
}

export default function AdvancedSqlJoinBuilder({ schema, baseColumns, onAddAction, onCancelAction, initial, submitLabel }: AdvancedSqlJoinBuilderProps) {
  const [joinType, setJoinType] = useState<'left'|'inner'|'right'>('left')
  const [targetTable, setTargetTable] = useState<string>('')
  const [sourceKey, setSourceKey] = useState<string>('')
  const [targetKey, setTargetKey] = useState<string>('')
  const [cols, setCols] = useState<JoinColumn[]>([])
  const [aggEnabled, setAggEnabled] = useState<boolean>(false)
  const [aggFn, setAggFn] = useState<JoinAggregate['fn']>('count')
  const [aggCol, setAggCol] = useState<string>('')
  const [aggAlias, setAggAlias] = useState<string>('total')
  const [fltEnabled, setFltEnabled] = useState<boolean>(false)
  const [fltSide, setFltSide] = useState<'target'|'source'>('target')
  const [fltLeft, setFltLeft] = useState<string>('')
  const [fltOp, setFltOp] = useState<'eq'|'ne'|'gt'|'gte'|'lt'|'lte'|'in'|'like'>('eq')
  const [fltVal, setFltVal] = useState<string>('')

  // Prefill when editing
  useEffect(() => {
    if (!initial) return
    try {
      if (initial.joinType) setJoinType(initial.joinType)
      if (initial.targetTable) setTargetTable(initial.targetTable)
      if (initial.sourceKey) setSourceKey(initial.sourceKey)
      if (initial.targetKey) setTargetKey(initial.targetKey)
      if (Array.isArray(initial.columns)) setCols(initial.columns)
      if (initial.aggregate) {
        setAggEnabled(true)
        setAggFn((initial.aggregate.fn as any) || 'count')
        setAggCol(String(initial.aggregate.column || ''))
        setAggAlias(String(initial.aggregate.alias || 'total'))
      } else {
        setAggEnabled(false)
      }
      if ((initial as any).filter) {
        const flt: any = (initial as any).filter
        setFltEnabled(true)
        const left = String(flt.left || '')
        let side: 'source'|'target' = 'target'
        let name = left
        if (left.startsWith('s.')) { side = 'source'; name = left.slice(2) }
        else if (left.startsWith('t.')) { side = 'target'; name = left.slice(2) }
        else {
          // Infer from membership in baseColumns
          side = baseColumns.includes(left) ? 'source' : 'target'
          name = left
        }
        setFltSide(side)
        setFltLeft(name)
        setFltOp((flt.op as any) || 'eq')
        setFltVal(Array.isArray(flt.right) ? (flt.right as any[]).join(',') : String(flt.right ?? ''))
      } else {
        setFltEnabled(false)
      }
    } catch {}
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [initial])

  const fieldErrors = useMemo(() => {
    const errs: Record<string, string | undefined> = {}
    if (!targetTable) errs.targetTable = 'Target table is required'
    if (!sourceKey) errs.sourceKey = 'Source key is required'
    if (!targetKey) errs.targetKey = 'Target key is required'
    if (aggEnabled) {
      if (!aggAlias) errs.aggAlias = 'Alias is required when aggregating'
      if (aggFn !== 'count' && !aggCol) errs.aggCol = 'Column is required for this aggregate'
    }
    if (fltEnabled) {
      if (!fltLeft) errs.fltLeft = 'Pick a column for the left side'
      if (fltOp === 'in' && fltVal && !/,/.test(fltVal.trim())) {
        // Not strictly an error, but a hint that user can enter CSV
        errs.fltVal = 'For IN operator, provide comma-separated values'
      }
    }
    return errs
  }, [targetTable, sourceKey, targetKey, aggEnabled, aggFn, aggCol, aggAlias, fltEnabled, fltLeft, fltOp, fltVal])

  const tables = useMemo(() => {
    if (!schema) return [] as string[]
    const out: string[] = []
    for (const sch of schema.schemas || []) {
      for (const t of sch.tables || []) out.push(`${sch.name}.${t.name}`)
    }
    return out
  }, [schema])

  const targetCols = useMemo(() => {
    const raw = String(targetTable || '')
    if (!schema || !raw.includes('.')) return [] as string[]
    const [sch, tbl] = [raw.split('.').slice(0, -1).join('.'), raw.split('.').slice(-1)[0]]
    const s = (schema.schemas || []).find(s => s.name === sch)
    const t = s?.tables.find(t => t.name === tbl)
    return (t?.columns || []).map(c => c.name)
  }, [schema, targetTable])

  const filterCols = useMemo(() => fltSide === 'source' ? baseColumns : targetCols, [fltSide, baseColumns, targetCols])

  // Alias validation: highlight duplicates and conflicts with base columns
  const aliasCounts = useMemo(() => {
    const map: Record<string, number> = {}
    cols.forEach(c => {
      const a = String(c.alias || '').trim()
      if (!a) return
      map[a] = (map[a] || 0) + 1
    })
    return map
  }, [cols])
  const aliasConflict = (alias?: string) => {
    const a = String(alias || '').trim()
    if (!a) return false
    return (aliasCounts[a] || 0) > 1 || baseColumns.includes(a)
  }
  const hasAliasConflict = useMemo(() => Object.entries(aliasCounts).some(([a, n]) => n > 1 || baseColumns.includes(a)), [aliasCounts, baseColumns])

  const canAdd = useMemo(() => {
    return !!joinType && !!targetTable && !!sourceKey && !!targetKey && !hasAliasConflict
  }, [joinType, targetTable, sourceKey, targetKey, hasAliasConflict])

  function suggestAlias(raw: string) {
    const tbl = String(targetTable || '').split('.').pop() || ''
    const base = `${tbl}_${raw}`
    const taken = new Set<string>([...baseColumns, ...cols.map(c => String(c.alias || '').trim()).filter(Boolean)])
    if (!taken.has(base)) return base
    let i = 2
    while (taken.has(`${base}_${i}`)) i++
    return `${base}_${i}`
  }

  function addColumn(name: string) {
    if (!name) return
    if (cols.some(c => c.name === name)) return
    setCols([...cols, { name, alias: suggestAlias(name) }])
  }

  function removeColumn(name: string) {
    setCols(cols.filter(c => c.name !== name))
  }

  return (
    <div className="rounded-md border p-2 bg-[hsl(var(--secondary)/0.6)] text-[12px]">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Join type</label>
        <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={joinType} onChange={(e)=>setJoinType(e.target.value as any)}>
          <option value="left">left</option>
          <option value="inner">inner</option>
          <option value="right">right</option>
        </select>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Target table</label>
        <div className="sm:col-span-2 flex gap-2">
          <select className="h-8 px-2 rounded-md bg-card text-xs flex-1" value={targetTable} onChange={(e)=>setTargetTable(e.target.value)}>
            <option value="">(select)</option>
            {tables.map(t => (<option key={t} value={t}>{t}</option>))}
          </select>
        </div>
      </div>
      {!!fieldErrors.targetTable && <div className="text-[11px] text-red-600 mb-2">{fieldErrors.targetTable}</div>}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Match keys</label>
        <div className="sm:col-span-2 grid grid-cols-2 gap-2">
          <select className="h-8 px-2 rounded-md bg-card text-xs" value={sourceKey} onChange={(e)=>setSourceKey(e.target.value)}>
            <option value="">source key (base)</option>
            {baseColumns.map(c => (<option key={c} value={c}>{c}</option>))}
          </select>
          <select className="h-8 px-2 rounded-md bg-card text-xs" value={targetKey} onChange={(e)=>setTargetKey(e.target.value)}>
            <option value="">target key (joined)</option>
            {targetCols.map(c => (<option key={c} value={c}>{c}</option>))}
          </select>
        </div>
      </div>
      {(!sourceKey || !targetKey) && <div className="text-[11px] text-red-600 mb-2">{!sourceKey ? 'Source key' : ''}{(!sourceKey && !targetKey) ? ' and ' : ''}{!targetKey ? 'Target key' : ''} required</div>}
      <div className="mb-2">
        <div className="text-xs text-muted-foreground mb-1">Columns to bring from joined table</div>
        <div className="flex items-center gap-2 mb-2">
          <select className="h-8 px-2 rounded-md bg-card text-xs" onChange={(e)=>{ addColumn(e.target.value); e.currentTarget.selectedIndex = 0 }}>
            <option value="">(pick column)</option>
            {targetCols.map(c => (<option key={c} value={c}>{c}</option>))}
          </select>
        </div>
        {cols.length > 0 && (
          <div className="space-y-1">
            {cols.map((c, idx) => (
              <div key={c.name} className="grid grid-cols-[auto,1fr,auto,auto] gap-2 items-center">
                <span className="text-[11px] px-2 py-1 bg-card rounded-md border" title={c.name}>{c.name}</span>
                <input
                  className={`h-8 px-2 rounded-md bg-card text-[11px] ${aliasConflict(c.alias) ? 'ring-2 ring-red-500' : ''}`}
                  placeholder="alias (new column name)"
                  value={c.alias || ''}
                  title={aliasConflict(c.alias) ? 'Alias duplicates another or conflicts with a base column' : ''}
                  onChange={(e) => {
                    const next = [...cols]
                    next[idx] = { ...c, alias: e.target.value }
                    setCols(next)
                  }}
                />
                <button
                  type="button"
                  className="text-[11px] px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
                  onClick={() => {
                    const next = [...cols]
                    next[idx] = { ...c, alias: suggestAlias(c.name) }
                    setCols(next)
                  }}
                >Suggest</button>
                <button
                  type="button"
                  className="text-[11px] px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
                  onClick={() => removeColumn(c.name)}
                >Remove</button>
              </div>
            ))}
            {hasAliasConflict && (
              <div className="text-[11px] text-amber-600 mt-1">One or more aliases duplicate each other or conflict with base columns. Consider renaming.</div>
            )}
          </div>
        )}
      </div>
      <div className="mb-3">
        <label className="text-xs flex items-center gap-2"><input type="checkbox" checked={fltEnabled} onChange={(e)=>setFltEnabled(e.target.checked)} /> Add join filter</label>
        {fltEnabled && (
          <div className="mt-2 space-y-2">
            <div className="flex items-center gap-4">
              <label className="text-xs flex items-center gap-1"><input type="radio" checked={fltSide==='target'} onChange={()=>setFltSide('target')} /> joined table</label>
              <label className="text-xs flex items-center gap-1"><input type="radio" checked={fltSide==='source'} onChange={()=>setFltSide('source')} /> base table</label>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
              <label className="text-xs text-muted-foreground sm:col-span-1">Left</label>
              <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={fltLeft} onChange={(e)=>setFltLeft(e.target.value)}>
                <option value="">(column)</option>
                {filterCols.map(c => (<option key={c} value={c}>{c}</option>))}
              </select>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
              <label className="text-xs text-muted-foreground sm:col-span-1">Operator</label>
              <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={fltOp} onChange={(e)=>setFltOp(e.target.value as any)}>
                {['eq','ne','gt','gte','lt','lte','in','like','regex'].map(x => (<option key={x} value={x}>{x}</option>))}
              </select>
            </div>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
              <label className="text-xs text-muted-foreground sm:col-span-1">Value</label>
              <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder={fltOp==='in'? 'comma-separated' : ''} value={fltVal} onChange={(e)=>setFltVal(e.target.value)} />
            </div>
          </div>
        )}
      </div>
      <div className="mb-3">
        <label className="text-xs flex items-center gap-2"><input type="checkbox" checked={aggEnabled} onChange={(e)=>setAggEnabled(e.target.checked)} /> Aggregate 1:N join</label>
        {aggEnabled && (
          <div className="mt-2 grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
            <label className="text-xs text-muted-foreground sm:col-span-1">Function</label>
            <div className="sm:col-span-2 grid grid-cols-3 gap-2">
              <select className="h-8 px-2 rounded-md bg-card text-xs" value={aggFn} onChange={(e)=>setAggFn(e.target.value as any)}>
                {['sum','avg','min','max','count','string_agg','array_agg'].map(x => (<option key={x} value={x}>{x}</option>))}
              </select>
              <select className="h-8 px-2 rounded-md bg-card text-xs" value={aggCol} onChange={(e)=>setAggCol(e.target.value)}>
                <option value="">(column)</option>
                {targetCols.map(c => (<option key={c} value={c}>{c}</option>))}
              </select>
              <input className="h-8 px-2 rounded-md bg-card text-xs" placeholder="alias" value={aggAlias} onChange={(e)=>setAggAlias(e.target.value)} />
            </div>
          </div>
        )}
        {aggEnabled && !!fieldErrors.aggCol && aggFn !== 'count' && <div className="text-[11px] text-red-600 mt-1">Column is required for this aggregate</div>}
        {aggEnabled && !!fieldErrors.aggAlias && <div className="text-[11px] text-red-600 mt-1">Alias is required when aggregating</div>}
      </div>

      <div className="mt-3">
        <div className="text-[11px] font-medium mb-1">Preview</div>
        <pre className="text-[11px] font-mono rounded-md border bg-card p-2 whitespace-pre-wrap">{JSON.stringify((() => {
          const payload: any = { joinType, targetTable, sourceKey, targetKey }
          if (cols.length) payload.columns = cols
          if (aggEnabled && aggAlias && (aggFn === 'count' || aggCol)) payload.aggregate = { fn: aggFn, column: aggCol, alias: aggAlias }
          if (fltEnabled && fltLeft) {
            const left = (fltSide === 'source' ? `s.${fltLeft}` : `t.${fltLeft}`)
            const right: any = fltOp === 'in' ? fltVal.split(',').map(s=>s.trim()).filter(Boolean) : fltVal
            payload.filter = { op: fltOp, left, right }
          }
          return payload
        })(), null, 2)}</pre>
      </div>
      <div className="flex items-center justify-end gap-2 mt-2">
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCancelAction}>Cancel</button>
        <button className={`text-xs px-2 py-1 rounded-md border ${canAdd? 'bg-[hsl(var(--btn3))] text-black':'opacity-60 cursor-not-allowed'}`} disabled={!canAdd} onClick={()=>{
          const payload: any = { joinType, targetTable, sourceKey, targetKey }
          if (cols.length) payload.columns = cols
          if (aggEnabled && aggAlias && (aggFn === 'count' || aggCol)) payload.aggregate = { fn: aggFn, column: aggCol, alias: aggAlias }
          if (fltEnabled && fltLeft) {
            const left = (fltSide === 'source' ? `s.${fltLeft}` : `t.${fltLeft}`)
            const right: any = fltOp === 'in' ? fltVal.split(',').map(s=>s.trim()).filter(Boolean) : fltVal
            payload.filter = { op: fltOp, left, right }
          }
          onAddAction(payload)
        }}>{submitLabel || 'Add Join'}</button>
      </div>
    </div>
  )
}
