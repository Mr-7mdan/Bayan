"use client"

import { useEffect, useMemo, useState } from 'react'

export type AdvancedSqlNullBuilderProps = {
  columns: string[]
  onAddAction: (tr: { type: 'nullHandling'; target: string; mode: 'coalesce'|'isnull'|'ifnull'; value: any; scope?: any }) => void
  onCancelAction?: () => void
  initial?: { type: 'nullHandling'; target: string; mode: 'coalesce'|'isnull'|'ifnull'; value: any; scope?: any }
  submitLabel?: string
  dsId?: string
  tableName?: string | null
}

export default function AdvancedSqlNullBuilder({ columns, onAddAction, onCancelAction, initial, submitLabel, dsId, tableName }: AdvancedSqlNullBuilderProps) {
  const [target, setTarget] = useState<string>(columns[0] || '')
  const [mode, setMode] = useState<'coalesce'|'isnull'|'ifnull'>('coalesce')
  const [value, setValue] = useState<string>('')
  const [scopeLevel, setScopeLevel] = useState<'datasource' | 'table'>('datasource')

  useEffect(() => {
    if (!initial) return
    try {
      setTarget(String(initial.target || columns[0] || ''))
      setMode((initial.mode as any) || 'coalesce')
      setValue(String(initial.value ?? ''))
      const initScope = (initial as any).scope
      if (initScope?.level === 'table') {
        setScopeLevel('table')
      } else {
        setScopeLevel('datasource')
      }
    } catch {}
  }, [initial, columns])

  // Default to table scope if table is selected
  useEffect(() => {
    if (tableName && !initial) {
      setScopeLevel('table')
    }
  }, [tableName, initial])

  const canAdd = useMemo(() => !!target && value !== '', [target, value])

  return (
    <div className="rounded-md border p-2 bg-[hsl(var(--secondary)/0.6)] text-[12px]">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Target column</label>
        <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={target} onChange={(e)=>setTarget(e.target.value)}>
          {columns.map(c => (<option key={c} value={c}>{c}</option>))}
        </select>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Mode</label>
        <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={mode} onChange={(e)=>setMode(e.target.value as any)}>
          <option value="coalesce">coalesce</option>
          <option value="isnull">isnull</option>
          <option value="ifnull">ifnull</option>
        </select>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Value</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="replacement value" value={value} onChange={(e)=>setValue(e.target.value)} />
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-3">
        <label className="text-xs text-muted-foreground sm:col-span-1">Scope</label>
        <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={scopeLevel} onChange={(e)=>setScopeLevel(e.target.value as any)}>
          <option value="datasource">Datasource-wide</option>
          {tableName && <option value="table">Table: {tableName}</option>}
        </select>
      </div>
      <div className="flex items-center justify-end gap-2">
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCancelAction}>Cancel</button>
        <button className={`text-xs px-2 py-1 rounded-md border ${canAdd? 'bg-[hsl(var(--btn3))] text-black':'opacity-60 cursor-not-allowed'}`} disabled={!canAdd} onClick={()=>{
          const payload: any = { type: 'nullHandling', target, mode, value }
          // Add scope
          if (scopeLevel === 'table' && tableName) {
            payload.scope = { level: 'table', table: tableName }
          } else {
            payload.scope = { level: 'datasource' }
          }
          onAddAction(payload)
        }}>{submitLabel || 'Add NULL handling'}</button>
      </div>
    </div>
  )
}
