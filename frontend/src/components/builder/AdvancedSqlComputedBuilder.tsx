"use client"

import { useEffect, useMemo, useState } from 'react'

export type AdvancedSqlComputedBuilderProps = {
  columns: string[]
  onAddAction: (tr: { type: 'computed'; name: string; expr: string; valueType?: 'string'|'number'|'date'|'boolean' }) => void
  onCancelAction?: () => void
  initial?: { type: 'computed'; name: string; expr: string; valueType?: 'string'|'number'|'date'|'boolean' }
  submitLabel?: string
}

export default function AdvancedSqlComputedBuilder({ columns, onAddAction, onCancelAction, initial, submitLabel }: AdvancedSqlComputedBuilderProps) {
  const [name, setName] = useState<string>('')
  const [expr, setExpr] = useState<string>('')
  const [valueType, setValueType] = useState<'string'|'number'|'date'|'boolean' | ''>('')

  useEffect(() => {
    if (!initial) return
    try {
      setName(String(initial.name || ''))
      setExpr(String(initial.expr || ''))
      setValueType(((initial.valueType as any) || '') as any)
    } catch {}
  }, [initial])

  const canAdd = useMemo(() => name.trim() !== '' && expr.trim() !== '', [name, expr])

  return (
    <div className="rounded-md border p-2 bg-[hsl(var(--secondary)/0.6)] text-[12px]">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Name</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="new column name" value={name} onChange={(e)=>setName(e.target.value)} />
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Expression</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="SQL expression, e.g., price * qty" value={expr} onChange={(e)=>setExpr(e.target.value)} />
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-3">
        <label className="text-xs text-muted-foreground sm:col-span-1">Value type</label>
        <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={valueType} onChange={(e)=>setValueType(e.target.value as any)}>
          <option value="">(infer)</option>
          <option value="string">string</option>
          <option value="number">number</option>
          <option value="date">date</option>
          <option value="boolean">boolean</option>
        </select>
      </div>
      <div className="flex items-center justify-end gap-2">
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCancelAction}>Cancel</button>
        <button className={`text-xs px-2 py-1 rounded-md border ${canAdd? 'bg-[hsl(var(--btn3))] text-black':'opacity-60 cursor-not-allowed'}`} disabled={!canAdd} onClick={()=>{
          const payload: any = { type: 'computed' as const, name: name.trim(), expr: expr.trim() }
          if (valueType) payload.valueType = valueType
          onAddAction(payload)
        }}>{submitLabel || 'Add Computed'}</button>
      </div>
    </div>
  )
}
