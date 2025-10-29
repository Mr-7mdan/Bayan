"use client"

import { useEffect, useMemo, useState } from 'react'
import type { Condition } from '@/lib/dsl'

export type AdvancedSqlCaseBuilderProps = {
  columns: string[]
  onAddAction: (tr: { type: 'case'; target: string; cases: { when: Condition; then: any }[]; else?: any }) => void
  onCancelAction?: () => void
  initial?: { type: 'case'; target: string; cases: { when: Condition; then: any }[]; else?: any }
  submitLabel?: string
}

export default function AdvancedSqlCaseBuilder({ columns, onAddAction, onCancelAction, initial, submitLabel }: AdvancedSqlCaseBuilderProps) {
  const [target, setTarget] = useState<string>(columns[0] || '')
  type Row = { op: Condition['op']; value: string; then: string }
  const [rows, setRows] = useState<Row[]>([{ op: 'eq', value: '', then: '' }])
  const [elseVal, setElseVal] = useState<string>('')

  // Prefill from initial when editing
  useEffect(() => {
    if (!initial) return
    try {
      setTarget(String(initial.target || columns[0] || ''))
      const rws: Row[] = (initial.cases || []).map((c) => {
        const op = (c.when?.op || 'eq') as Condition['op']
        const right: any = (c.when as any)?.right
        const value = Array.isArray(right) ? right.join(',') : String(right ?? '')
        return { op, value, then: String(c.then ?? '') }
      })
      setRows(rws.length ? rws : [{ op: 'eq', value: '', then: '' }])
      setElseVal(initial.else != null ? String(initial.else) : '')
    } catch {}
  }, [initial, columns])

  const canAdd = useMemo(() => !!target && rows.every(r => r.value !== '' && r.then !== ''), [target, rows])

  return (
    <div className="rounded-md border p-2 bg-[hsl(var(--secondary)/0.6)] text-[12px]">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Target column</label>
        <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={target} onChange={(e)=>setTarget(e.target.value)}>
          {columns.map(c => (<option key={c} value={c}>{c}</option>))}
        </select>
      </div>
      <div className="text-xs font-medium mb-1">Conditions</div>
      <div className="space-y-2 mb-2">
        {rows.map((r, i) => (
          <div key={i} className="grid grid-cols-[72px,120px,1fr,1fr,auto] gap-2 items-center">
            <span className="text-[11px] text-muted-foreground">WHEN</span>
            <select className="h-8 px-2 rounded-md bg-card text-xs" value={r.op} onChange={(e)=>{
              const next = [...rows]; next[i] = { ...r, op: e.target.value as any }; setRows(next)
            }}>
              {['eq','ne','gt','gte','lt','lte','in','like','regex'].map(op => (<option key={op} value={op}>{op}</option>))}
            </select>
            <input className="h-8 px-2 rounded-md bg-card text-xs" placeholder="Find value (comma-separated for IN)" value={r.value} onChange={(e)=>{
              const next = [...rows]; next[i] = { ...r, value: e.target.value }; setRows(next)
            }}/>
            <input className="h-8 px-2 rounded-md bg-card text-xs" placeholder="Replace with (THEN)" value={r.then} onChange={(e)=>{
              const next = [...rows]; next[i] = { ...r, then: e.target.value }; setRows(next)
            }}/>
            <button className="text-[11px] px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setRows(rows.filter((_,j)=>j!==i))}>Remove</button>
          </div>
        ))}
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setRows([...rows, { op: 'eq', value: '', then: '' }])}>+ Add condition</button>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-3">
        <label className="text-xs text-muted-foreground sm:col-span-1">ELSE</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="Else value (optional)" value={elseVal} onChange={(e)=>setElseVal(e.target.value)} />
      </div>
      <div className="flex items-center justify-end gap-2">
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCancelAction}>Cancel</button>
        <button className={`text-xs px-2 py-1 rounded-md border ${canAdd? 'bg-[hsl(var(--btn3))] text-black':'opacity-60 cursor-not-allowed'}`} disabled={!canAdd} onClick={()=>{
          const cases = rows.map(r => {
            const right = r.op === 'in' ? r.value.split(',').map(s=>s.trim()) : r.value
            const when: Condition = { op: r.op, left: `s.${target}`, right }
            return { when, then: r.then }
          })
          const tr = { type: 'case' as const, target, cases, else: elseVal ? elseVal : undefined }
          onAddAction(tr)
        }}>{submitLabel || 'Add CASE transform'}</button>
      </div>
    </div>
  )
}
