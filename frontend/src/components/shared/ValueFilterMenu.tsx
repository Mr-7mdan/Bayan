"use client"

import { useEffect, useState } from 'react'

export type NumberOp = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'between'

export default function ValueFilterMenu(props: any) {
  // typed as any to satisfy Next.js "serializable props" lint; this component is only used by Client Components
  const { field, initial, onApply, onClear } = props as {
    field: string
    initial: { op: NumberOp; a?: number | ''; b?: number | '' }
    onApply: (patch: Record<string, any>) => void
    onClear: () => void
  }
  const [op, setOp] = useState<NumberOp>(initial.op)
  const [a, setA] = useState<number | ''>(initial.a ?? '')
  const [b, setB] = useState<number | ''>(initial.b ?? '')

  useEffect(() => { setOp(initial.op); setA(initial.a ?? ''); setB(initial.b ?? '') }, [initial.op, initial.a, initial.b])

  const apply = () => {
    const patch: Record<string, any> = {}
    const hasNum = (x: any) => typeof x === 'number' && !isNaN(x)
    switch (op) {
      case 'eq': if (hasNum(a)) patch[`${field}__eq`] = a; break
      case 'ne': if (hasNum(a)) patch[`${field}__ne`] = a; break
      case 'gt': if (hasNum(a)) patch[`${field}__gt`] = a; break
      case 'gte': if (hasNum(a)) patch[`${field}__gte`] = a; break
      case 'lt': if (hasNum(a)) patch[`${field}__lt`] = a; break
      case 'lte': if (hasNum(a)) patch[`${field}__lte`] = a; break
      case 'between':
        if (hasNum(a)) patch[`${field}__gte`] = a
        if (hasNum(b)) patch[`${field}__lte`] = b
        break
    }
    onApply(patch)
  }

  return (
    <div className="w-[260px] p-1 space-y-2">
      <select className="w-full h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--card))]" value={op} onChange={(e) => setOp(e.target.value as NumberOp)}>
        <option value="eq">Is equal to</option>
        <option value="ne">Is not equal to</option>
        <option value="gt">Is greater than</option>
        <option value="gte">Is greater or equal</option>
        <option value="lt">Is less than</option>
        <option value="lte">Is less than or equal</option>
        <option value="between">Is between</option>
      </select>
      <div className="grid grid-cols-2 gap-2 items-center">
        {(op !== 'between') && (
          <>
            <input type="number" className="col-span-2 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--card))]" value={a} onChange={(e) => setA(e.target.value === '' ? '' : Number(e.target.value))} />
          </>
        )}
        {op === 'between' && (
          <>
            <input type="number" className="h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--card))]" placeholder="Min" value={a} onChange={(e) => setA(e.target.value === '' ? '' : Number(e.target.value))} />
            <input type="number" className="h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--card))]" placeholder="Max" value={b} onChange={(e) => setB(e.target.value === '' ? '' : Number(e.target.value))} />
          </>
        )}
      </div>
      <div className="flex items-center justify-end gap-2">
        <button className="text-[11px] px-2 py-1 rounded-md border hover:bg-muted" onClick={onClear}>Clear</button>
        <button className="text-[11px] px-2 py-1 rounded-md border bg-[hsl(var(--btn3))] text-black" onClick={apply}>Apply</button>
      </div>
    </div>
  )
}
