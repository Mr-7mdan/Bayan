"use client"

import { useEffect, useRef, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'
import { RiArrowDownSLine, RiCalendar2Line, RiHashtag, RiTextWrap } from '@remixicon/react'
import { PresetConfig, DEFAULT_PRESET, QUICK_PICKS, QuickPick, PERIOD_OPTIONS, OFFSET_OPTIONS, AS_OF_OPTIONS, RANGE_MODE_OPTIONS, parseLegacyPreset, matchQuickPick } from '@/lib/datePresets'

export type FilterbarControlProps = {
  active?: string
  options: readonly string[]
  labels: Record<string, string>
  onChange: (value: string) => void
  className?: string
  disabled?: boolean
}

export function FilterbarRuleControl(props: {
  label: string
  kind: 'string'|'number'|'date'
  field: string
  where?: Record<string, any>
  onPatchAction: (patch: Record<string, any>) => void
  className?: string
  disabled?: boolean
  distinctCache?: Record<string, string[]>
  loadingCache?: Record<string, boolean>
  loadDistinctAction?: (field: string) => void
}) {
  const { label, kind, field, where, onPatchAction, className, disabled, distinctCache, loadingCache, loadDistinctAction } = props
  const icon = kind === 'date' ? <RiCalendar2Line className="size-4 shrink-0 text-[hsl(var(--muted-foreground))]" aria-hidden /> : kind === 'number' ? <RiHashtag className="size-4 shrink-0 text-[hsl(var(--muted-foreground))]" aria-hidden /> : <RiTextWrap className="size-4 shrink-0 text-[hsl(var(--muted-foreground))]" aria-hidden />
  return (
    <FilterbarShell label={label} icon={icon} className={className}>
      {kind === 'number' && (
        <NumberRuleInline field={field} where={where} onPatchAction={onPatchAction} distinctCache={distinctCache} loadingCache={loadingCache} loadDistinctAction={loadDistinctAction} />
      )}
      {kind === 'date' && (
        <DateRuleInline field={field} where={where} onPatchAction={onPatchAction} distinctCache={distinctCache} loadingCache={loadingCache} loadDistinctAction={loadDistinctAction} />
      )}
      {kind === 'string' && (
        <StringRuleInline field={field} where={where} onPatchAction={onPatchAction} distinctCache={distinctCache} loadingCache={loadingCache} loadDistinctAction={loadDistinctAction} />
      )}
    </FilterbarShell>
  )
}

function NumberRuleInline({ field, where, onPatchAction, distinctCache, loadingCache, loadDistinctAction }: { field: string; where?: Record<string, any>; onPatchAction: (patch: Record<string, any>) => void; distinctCache?: Record<string, string[]>; loadingCache?: Record<string, boolean>; loadDistinctAction?: (field: string) => void }) {
  type NumberOp = 'eq'|'ne'|'gt'|'gte'|'lt'|'lte'|'between'
  type Mode = 'rule' | 'manual'
  const gte = (where as any)?.[`${field}__gte`] as number | undefined
  const lte = (where as any)?.[`${field}__lte`] as number | undefined
  const gt = (where as any)?.[`${field}__gt`] as number | undefined
  const lt = (where as any)?.[`${field}__lt`] as number | undefined
  const ne = (where as any)?.[`${field}__ne`] as number | undefined
  const eqArr = (where as any)?.[field] as number[] | undefined
  const initialArr = Array.isArray(eqArr) ? eqArr.map((v) => String(v)) : []
  const singleEq = (Array.isArray(eqArr) && eqArr.length === 1) ? Number(eqArr[0]) : undefined
  const initial: { op: NumberOp; a?: number | ''; b?: number | '' } = (() => {
    if (typeof singleEq === 'number') return { op: 'eq', a: singleEq }
    if (typeof ne === 'number') return { op: 'ne', a: ne }
    if (typeof gt === 'number') return { op: 'gt', a: gt }
    if (typeof gte === 'number' && typeof lte === 'number') return { op: 'between', a: gte, b: lte }
    if (typeof gte === 'number') return { op: 'gte', a: gte }
    if (typeof lt === 'number') return { op: 'lt', a: lt }
    if (typeof lte === 'number') return { op: 'lte', a: lte }
    return { op: 'eq', a: '' }
  })()
  const [mode, setMode] = useState<Mode>(initialArr.length > 1 ? 'manual' : 'rule')
  const [op, setOp] = useState<NumberOp>(initial.op)
  const [a, setA] = useState<number|''>(initial.a ?? '')
  const [b, setB] = useState<number|''>(initial.b ?? '')
  const [sel, setSel] = useState<string[]>(initialArr)
  const [q, setQ] = useState<string>('')
  const interactedRef = useRef(false)
  const lastPatchSigRef = useRef<string>('')
  
  // Load distinct values for manual mode
  useEffect(() => {
    if (mode !== 'manual') return
    const arr = distinctCache?.[field]
    const isLoading = loadingCache?.[field]
    if (!isLoading && arr == null && typeof loadDistinctAction === 'function') {
      loadDistinctAction(field)
    }
  }, [mode, field, distinctCache?.[field]])
  
  // Initialize with all values selected when entering manual mode
  useEffect(() => {
    if (mode !== 'manual') return
    const opts = distinctCache?.[field] || []
    if (opts.length > 0 && sel.length === 0) {
      setSel(opts.map(v => String(v)))
    }
  }, [mode, distinctCache?.[field]])
  
  // Reflect external where for manual mode
  useEffect(() => {
    const arr = Array.isArray((where as any)?.[field]) ? ((where as any)?.[field] as any[]).map((v) => String(v)) : []
    if (arr.length && JSON.stringify(arr) !== JSON.stringify(sel)) setSel(arr)
  }, [field, JSON.stringify((where as any)?.[field] || [])])
  
  // Reflect external where changes for rule mode
  useEffect(() => {
    if (interactedRef.current || mode !== 'rule') return
    const currentGte = (where as any)?.[`${field}__gte`] as number | undefined
    const currentLte = (where as any)?.[`${field}__lte`] as number | undefined
    const currentGt = (where as any)?.[`${field}__gt`] as number | undefined
    const currentLt = (where as any)?.[`${field}__lt`] as number | undefined
    const currentNe = (where as any)?.[`${field}__ne`] as number | undefined
    const currentEqArr = (where as any)?.[field] as number[] | undefined
    const currentEq = (Array.isArray(currentEqArr) && currentEqArr.length === 1) ? Number(currentEqArr[0]) : undefined
    
    if (typeof currentEq === 'number') {
      if (op !== 'eq') setOp('eq')
      if (a !== currentEq) setA(currentEq)
      if (b !== '') setB('')
    } else if (typeof currentNe === 'number') {
      if (op !== 'ne') setOp('ne')
      if (a !== currentNe) setA(currentNe)
      if (b !== '') setB('')
    } else if (typeof currentGt === 'number') {
      if (op !== 'gt') setOp('gt')
      if (a !== currentGt) setA(currentGt)
      if (b !== '') setB('')
    } else if (typeof currentGte === 'number' && typeof currentLte === 'number') {
      if (op !== 'between') setOp('between')
      if (a !== currentGte) setA(currentGte)
      if (b !== currentLte) setB(currentLte)
    } else if (typeof currentGte === 'number') {
      if (op !== 'gte') setOp('gte')
      if (a !== currentGte) setA(currentGte)
      if (b !== '') setB('')
    } else if (typeof currentLt === 'number') {
      if (op !== 'lt') setOp('lt')
      if (a !== currentLt) setA(currentLt)
      if (b !== '') setB('')
    } else if (typeof currentLte === 'number') {
      if (op !== 'lte') setOp('lte')
      if (a !== currentLte) setA(currentLte)
      if (b !== '') setB('')
    }
  }, [field, (where as any)?.[`${field}__gte`], (where as any)?.[`${field}__lte`], (where as any)?.[`${field}__gt`], (where as any)?.[`${field}__lt`], (where as any)?.[`${field}__ne`], JSON.stringify((where as any)?.[field])])
  
  // Emit patch for rule mode
  useEffect(() => {
    if (mode !== 'rule') return
    const patch: Record<string, any> = { [`${field}__gt`]: undefined, [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined, [field]: undefined, [`${field}__ne`] : undefined }
    const hasNum = (x: any) => typeof x === 'number' && !isNaN(x)
    switch (op) {
      case 'eq': if (hasNum(a)) patch[field] = [a]; break
      case 'ne': if (hasNum(a)) patch[`${field}__ne`] = a; break
      case 'gt': if (hasNum(a)) patch[`${field}__gt`] = a; break
      case 'gte': if (hasNum(a)) patch[`${field}__gte`] = a; break
      case 'lt': if (hasNum(a)) patch[`${field}__lt`] = a; break
      case 'lte': if (hasNum(a)) patch[`${field}__lte`] = a; break
      case 'between': if (hasNum(a)) patch[`${field}__gte`] = a; if (hasNum(b)) patch[`${field}__lte`] = b; break
    }
    const sig = JSON.stringify(patch)
    if (sig !== lastPatchSigRef.current) { lastPatchSigRef.current = sig; onPatchAction(patch) }
  }, [mode, op, a, b])
  
  // Emit patch for manual mode
  useEffect(() => {
    if (mode !== 'manual') return
    const patch: Record<string, any> = { [`${field}__gt`]: undefined, [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [`${field}__lte`]: undefined, [field]: undefined, [`${field}__ne`]: undefined }
    patch[field] = sel.length ? sel : undefined
    const sig = JSON.stringify(patch)
    if (sig !== lastPatchSigRef.current) { lastPatchSigRef.current = sig; onPatchAction(patch) }
  }, [mode, JSON.stringify(sel)])
  
  // Smart sort for numbers
  const opts = ((distinctCache?.[field] || []) as string[]).map((s) => String(s))
  const sortedOpts = [...opts].sort((a, b) => {
    const na = Number(a), nb = Number(b)
    if (isNaN(na) && isNaN(nb)) return String(a).localeCompare(String(b))
    if (isNaN(na)) return 1
    if (isNaN(nb)) return -1
    return na - nb
  })
  const filtered = sortedOpts.filter((s) => s.toLowerCase().includes(q.toLowerCase()))
  
  // Auto-select search results
  useEffect(() => {
    if (mode !== 'manual' || !q) return
    const set = new Set<string>(sel)
    filtered.forEach((v) => set.add(v))
    setSel(Array.from(set.values()))
  }, [q])
  
  const toggle = (v: string) => {
    const exists = sel.includes(v)
    setSel(exists ? sel.filter((x) => x !== v) : [...sel, v])
  }
  const selectAll = () => setSel([...sortedOpts])
  const deselectAll = () => setSel([])
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='rule'} onChange={()=> setMode('rule')} /> Rule</label>
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='manual'} onChange={()=> setMode('manual')} /> Manual</label>
      </div>
      {mode === 'rule' ? (
        <div className="grid grid-cols-3 gap-2">
          <select className="col-span-3 sm:col-span-1 px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={op} onChange={(e)=>{interactedRef.current = true; setOp(e.target.value as NumberOp)}}>
            <option value="eq">Is equal to</option>
            <option value="ne">Is not equal to</option>
            <option value="gt">Is greater than</option>
            <option value="gte">Is greater or equal</option>
            <option value="lt">Is less than</option>
            <option value="lte">Is less than or equal</option>
            <option value="between">Is between</option>
          </select>
          {op !== 'between' ? (
            <input type="number" className="col-span-3 sm:col-span-2 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={a} onChange={(e)=>{interactedRef.current = true; setA(e.target.value === '' ? '' : Number(e.target.value))}} />
          ) : (
            <>
              <input type="number" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Min" value={a} onChange={(e)=>{interactedRef.current = true; setA(e.target.value === '' ? '' : Number(e.target.value))}} />
              <input type="number" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Max" value={b} onChange={(e)=>{interactedRef.current = true; setB(e.target.value === '' ? '' : Number(e.target.value))}} />
            </>
          )}
        </div>
      ) : (
        <div className="space-y-2">
          <input className="w-full h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Search values" value={q} onChange={(e)=>setQ(e.target.value)} />
          <div className="flex items-center justify-between text-[11px] text-muted-foreground">
            <span>{sel.length} of {sortedOpts.length} selected</span>
            <div className="flex gap-2">
              <button className="hover:text-foreground" onClick={selectAll}>Select All</button>
              <button className="hover:text-foreground" onClick={deselectAll}>Deselect All</button>
            </div>
          </div>
          <div className="max-h-56 overflow-auto border rounded-md">
            <ul className="p-2 space-y-1">
              {filtered.map((v) => (
                <li key={v} className="flex items-center gap-2 text-xs">
                  <input type="checkbox" className="size-3" checked={sel.includes(v)} onChange={()=>toggle(v)} />
                  <span className="truncate" title={v}>{v}</span>
                </li>
              ))}
              {filtered.length === 0 && loadingCache?.[field] && (
                <li className="text-xs text-muted-foreground flex items-center gap-2">
                  <span className="inline-block size-3 border-2 border-muted-foreground/30 border-t-muted-foreground rounded-full animate-spin" />
                  Loading values...
                </li>
              )}
              {filtered.length === 0 && !loadingCache?.[field] && (
                <li className="text-xs text-muted-foreground">No values</li>
              )}
            </ul>
          </div>
          <div className="flex items-center gap-2">
            <button className="flex-1 px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-xs" onClick={deselectAll}>Clear</button>
            <button className="flex-1 px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-xs font-medium" onClick={()=>{}}>Apply</button>
          </div>
        </div>
      )}
    </div>
  )
}

function StringRuleInline({ field, where, onPatchAction, distinctCache, loadingCache, loadDistinctAction }: { field: string; where?: Record<string, any>; onPatchAction: (patch: Record<string, any>) => void; distinctCache?: Record<string, string[]>; loadingCache?: Record<string, boolean>; loadDistinctAction?: (field: string) => void }) {
  type StrOp = 'contains'|'not_contains'|'eq'|'ne'|'starts_with'|'ends_with'
  type Mode = 'criteria' | 'manual'
  const initialArr = Array.isArray((where as any)?.[field]) ? ((where as any)?.[field] as any[]).map((v) => String(v)) : []
  const [mode, setMode] = useState<Mode>(initialArr.length ? 'manual' : 'criteria')
  const [op, setOp] = useState<StrOp>('contains')
  const [val, setVal] = useState<string>('')
  const [sel, setSel] = useState<string[]>(initialArr)
  const [q, setQ] = useState<string>('')
  const interactedRef = useRef(false)
  const lastPatchSigRef = useRef<string>('')
  
  // Ensure distinct values are available when needed
  useEffect(() => {
    try {
      if (mode !== 'manual') return
      const arr = distinctCache?.[field]
      const isLoading = loadingCache?.[field]
      if (!isLoading && arr == null && typeof loadDistinctAction === 'function') {
        loadDistinctAction(field)
      }
    } catch {}
  }, [mode, field, distinctCache?.[field]])
  
  // Reflect external where changes for manual mode
  useEffect(() => {
    const arr = Array.isArray((where as any)?.[field]) ? ((where as any)?.[field] as any[]).map((v) => String(v)) : []
    if (arr.length && JSON.stringify(arr) !== JSON.stringify(sel)) setSel(arr)
  }, [field, JSON.stringify((where as any)?.[field] || [])])
  
  // Reflect external where changes for criteria mode
  useEffect(() => {
    if (interactedRef.current || mode !== 'criteria') return
    const eqArr = (where as any)?.[field]
    const contains = (where as any)?.[`${field}__contains`]
    const notcontains = (where as any)?.[`${field}__notcontains`]
    const startswith = (where as any)?.[`${field}__startswith`]
    const endswith = (where as any)?.[`${field}__endswith`]
    const ne = (where as any)?.[`${field}__ne`]
    
    if (Array.isArray(eqArr) && eqArr.length >= 1) {
      if (op !== 'eq') setOp('eq')
      const v = eqArr.map(x => String(x)).join(', ')
      if (val !== v) setVal(v)
    } else if (ne !== undefined) {
      if (op !== 'ne') setOp('ne')
      const v = Array.isArray(ne) ? ne.map(x => String(x)).join(', ') : String(ne)
      if (val !== v) setVal(v)
    } else if (contains !== undefined) {
      if (op !== 'contains') setOp('contains')
      const v = Array.isArray(contains) ? contains.map(x => String(x)).join(', ') : String(contains)
      if (val !== v) setVal(v)
    } else if (notcontains !== undefined) {
      if (op !== 'not_contains') setOp('not_contains')
      const v = Array.isArray(notcontains) ? notcontains.map(x => String(x)).join(', ') : String(notcontains)
      if (val !== v) setVal(v)
    } else if (startswith !== undefined) {
      if (op !== 'starts_with') setOp('starts_with')
      const v = Array.isArray(startswith) ? startswith.map(x => String(x)).join(', ') : String(startswith)
      if (val !== v) setVal(v)
    } else if (endswith !== undefined) {
      if (op !== 'ends_with') setOp('ends_with')
      const v = Array.isArray(endswith) ? endswith.map(x => String(x)).join(', ') : String(endswith)
      if (val !== v) setVal(v)
    }
  }, [mode, field, (where as any)?.[field], (where as any)?.[`${field}__contains`], (where as any)?.[`${field}__notcontains`], (where as any)?.[`${field}__startswith`], (where as any)?.[`${field}__endswith`], (where as any)?.[`${field}__ne`]])
  useEffect(() => {
    if (mode !== 'criteria') return
    const patch: Record<string, any> = { [field]: undefined, [`${field}__contains`]: undefined, [`${field}__notcontains`]: undefined, [`${field}__startswith`]: undefined, [`${field}__endswith`]: undefined, [`${field}__ne`]: undefined }
    const v = String(val || '').trim()
    if (!v) { 
      const sig = JSON.stringify(patch)
      if (sig !== lastPatchSigRef.current) { lastPatchSigRef.current = sig; onPatchAction(patch) }
      return 
    }
    // Support comma-separated multi-values with OR logic: "Bank, Retail" -> ["Bank", "Retail"]
    const values = v.split(',').map(s => s.trim()).filter(s => s.length > 0)
    const valuesOrSingle = values.length > 0 ? values : [v]
    
    switch (op) {
      case 'eq': 
        patch[field] = valuesOrSingle
        break
      case 'ne': 
        // For ne, use array if multiple values (AND logic: not A and not B)
        patch[`${field}__ne`] = valuesOrSingle
        break
      case 'contains': 
        patch[`${field}__contains`] = valuesOrSingle
        break
      case 'not_contains': 
        patch[`${field}__notcontains`] = valuesOrSingle
        break
      case 'starts_with': 
        patch[`${field}__startswith`] = valuesOrSingle
        break
      case 'ends_with': 
        patch[`${field}__endswith`] = valuesOrSingle
        break
    }
    const sig = JSON.stringify(patch)
    if (sig !== lastPatchSigRef.current) { lastPatchSigRef.current = sig; onPatchAction(patch) }
  }, [mode, op, val])
  useEffect(() => {
    if (mode !== 'manual') return
    const patch: Record<string, any> = { [field]: undefined, [`${field}__contains`]: undefined, [`${field}__notcontains`]: undefined, [`${field}__startswith`]: undefined, [`${field}__endswith`]: undefined, [`${field}__ne`]: undefined }
    patch[field] = sel.length ? sel : undefined
    const sig = JSON.stringify(patch)
    if (sig !== lastPatchSigRef.current) { lastPatchSigRef.current = sig; onPatchAction(patch) }
  }, [mode, JSON.stringify(sel)])
  const opts = ((distinctCache?.[field] || []) as string[]).map((s) => String(s))
  const filtered = opts.filter((s) => s.toLowerCase().includes(q.toLowerCase()))
  // Auto-select filtered results by default when searching (matches Details behavior)
  useEffect(() => {
    if (mode !== 'manual') return
    if (!q) return
    if (filtered.length === 0) return
    setSel((prev) => {
      const set = new Set<string>(prev)
      filtered.forEach((v) => set.add(v))
      return Array.from(set.values())
    })
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [mode, q])
  const toggle = (v: string) => {
    const exists = sel.includes(v)
    setSel(exists ? sel.filter((x) => x !== v) : [...sel, v])
  }
  const selectAll = () => setSel([...opts])
  const deselectAll = () => setSel([])
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='criteria'} onChange={()=> setMode('criteria')} /> Search</label>
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='manual'} onChange={()=> setMode('manual')} /> Manual</label>
      </div>
      {mode === 'criteria' ? (
        <div className="grid grid-cols-3 gap-2">
          <select className="col-span-3 sm:col-span-1 px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={op} onChange={(e)=>{interactedRef.current = true; setOp(e.target.value as StrOp)}}>
            <option value="contains">Contains</option>
            <option value="not_contains">Does not contain</option>
            <option value="eq">Is equal to</option>
            <option value="ne">Is not equal to</option>
            <option value="starts_with">Starts with</option>
            <option value="ends_with">Ends with</option>
          </select>
          <input className="col-span-3 sm:col-span-2 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Value (comma-separated for OR)" value={val} onChange={(e)=>{interactedRef.current = true; setVal(e.target.value)}} />
        </div>
      ) : (
        <div className="space-y-2">
          <input className="w-full h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Search values" value={q} onChange={(e)=>setQ(e.target.value)} />
          <div className="flex items-center justify-between text-[11px] text-muted-foreground">
            <span>{sel.length} of {opts.length} selected</span>
            <div className="flex gap-2">
              <button className="hover:text-foreground" onClick={selectAll}>Select All</button>
              <button className="hover:text-foreground" onClick={deselectAll}>Deselect All</button>
            </div>
          </div>
          <div className="max-h-56 overflow-auto border rounded-md">
            <ul className="p-2 space-y-1">
              {filtered.map((v) => (
                <li key={v} className="flex items-center gap-2 text-xs">
                  <input type="checkbox" className="size-3" checked={sel.includes(v)} onChange={()=>toggle(v)} />
                  <span className="truncate" title={v}>{v}</span>
                </li>
              ))}
              {filtered.length === 0 && loadingCache?.[field] && (
                <li className="text-xs text-muted-foreground flex items-center gap-2">
                  <span className="inline-block size-3 border-2 border-muted-foreground/30 border-t-muted-foreground rounded-full animate-spin" />
                  Loading values...
                </li>
              )}
              {filtered.length === 0 && !loadingCache?.[field] && (
                <li className="text-xs text-muted-foreground">No values</li>
              )}
            </ul>
          </div>
          <div className="flex items-center gap-2">
            <button className="flex-1 px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-xs" onClick={deselectAll}>Clear</button>
            <button className="flex-1 px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] text-xs font-medium" onClick={()=>{}}>Apply</button>
          </div>
        </div>
      )}
    </div>
  )
}

function DateRuleInline({ field, where, onPatchAction, distinctCache, loadingCache, loadDistinctAction }: { field: string; where?: Record<string, any>; onPatchAction: (patch: Record<string, any>) => void; distinctCache?: Record<string, string[]>; loadingCache?: Record<string, boolean>; loadDistinctAction?: (field: string) => void }) {
  type Mode = 'preset'|'custom'|'manual'
  type CustomOp = 'after'|'before'|'between'
  const storageKey = `frc-date:${field}`
  const initialArr = Array.isArray((where as any)?.[field]) ? ((where as any)?.[field] as any[]).map((v) => String(v)) : []
  const [mode, setMode] = useState<Mode>(initialArr.length > 1 ? 'manual' : 'preset')
  const [config, setConfig] = useState<PresetConfig>({ ...DEFAULT_PRESET })
  const [selectedQuickPick, setSelectedQuickPick] = useState<string | null>(null)
  const [op, setOp] = useState<CustomOp>('between')
  const [a, setA] = useState<string>('')
  const [b, setB] = useState<string>('')
  const [sel, setSel] = useState<string[]>(initialArr)
  const [q, setQ] = useState<string>('')
  const interactedRef = useRef(false)
  const editingRef = useRef(false)
  const editTimerRef = useRef<number | null>(null)
  
  // Load distinct values for manual mode
  useEffect(() => {
    if (mode !== 'manual') return
    const arr = distinctCache?.[field]
    const isLoading = loadingCache?.[field]
    if (!isLoading && arr == null && typeof loadDistinctAction === 'function') {
      loadDistinctAction(field)
    }
  }, [mode, field, distinctCache?.[field]])
  
  // Reflect external where for manual mode
  useEffect(() => {
    const arr = Array.isArray((where as any)?.[field]) ? ((where as any)?.[field] as any[]).map((v) => String(v)) : []
    if (arr.length && JSON.stringify(arr) !== JSON.stringify(sel)) setSel(arr)
  }, [field, JSON.stringify((where as any)?.[field] || [])])

  const markEditing = () => {
    editingRef.current = true
    if (typeof window !== 'undefined') {
      if (editTimerRef.current) window.clearTimeout(editTimerRef.current)
      editTimerRef.current = window.setTimeout(() => { editingRef.current = false }, 1200) as any
    }
  }

  // On config change: match quick pick
  useEffect(() => {
    const match = matchQuickPick(config)
    setSelectedQuickPick(match?.label ?? null)
  }, [config])

  // Hydrate UI state on field change
  useEffect(() => {
    try {
      // Check for structured or legacy preset
      const existingPreset = (where as any)?.[`${field}__date_preset`]
      if (existingPreset) {
        if (typeof existingPreset === 'string') {
          const converted = parseLegacyPreset(existingPreset)
          if (converted) { setMode('preset'); setConfig(converted) }
        } else if (typeof existingPreset === 'object') {
          setMode('preset'); setConfig(existingPreset as PresetConfig)
        }
        return
      }
      // Fallback: hydrate from gte/lt
      const gte = (where as any)?.[`${field}__gte`] as string | undefined
      const lt = (where as any)?.[`${field}__lt`] as string | undefined
      if (gte || lt) {
        setMode('custom')
        if (gte && lt) { setOp('between'); setA(gte); try { const d = new Date(`${lt}T00:00:00`); d.setDate(d.getDate()-1); setB(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`) } catch {} }
        else if (gte) { setOp('after'); setA(gte) }
        else if (lt) { setOp('before'); try { const d = new Date(`${lt}T00:00:00`); d.setDate(d.getDate()-1); setB(`${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`) } catch {} }
      } else {
        const raw = typeof window !== 'undefined' ? localStorage.getItem(storageKey) : null
        if (raw) {
          try {
            const st = JSON.parse(raw)
            if (st.mode) setMode(st.mode)
            if (st.config) setConfig(st.config)
            if (st.op) setOp(st.op)
            if (typeof st.a === 'string') setA(st.a)
            if (typeof st.b === 'string') setB(st.b)
          } catch {}
        }
      }
    } catch {}
  }, [field])

  // Persist UI state only after user has interacted
  useEffect(() => {
    if (!interactedRef.current) return
    try { if (typeof window !== 'undefined') localStorage.setItem(storageKey, JSON.stringify({ mode, config, op, a, b })) } catch {}
  }, [storageKey, mode, JSON.stringify(config), op, a, b])

  function ymd(d: Date) {
    return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`
  }

  // Reflect external where->UI
  useEffect(() => {
    try {
      if (editingRef.current) return
      const existingPreset = (where as any)?.[`${field}__date_preset`]
      if (existingPreset) {
        if (typeof existingPreset === 'object') {
          if (mode !== 'preset') setMode('preset')
          setConfig(existingPreset as PresetConfig)
        } else if (typeof existingPreset === 'string') {
          const converted = parseLegacyPreset(existingPreset)
          if (converted) { if (mode !== 'preset') setMode('preset'); setConfig(converted) }
        }
        return
      }
      const gte = (where as any)?.[`${field}__gte`] as string | undefined
      const lt = (where as any)?.[`${field}__lt`] as string | undefined
      if (!gte && !lt) return
      if (mode !== 'custom') setMode('custom')
      if (gte && lt) {
        if (op !== 'between') setOp('between')
        if (a !== (gte || '')) setA(gte || '')
        try { if (lt) { const d = new Date(`${lt}T00:00:00`); d.setDate(d.getDate()-1); const prev = ymd(d); if (b !== prev) setB(prev) } } catch {}
      } else if (gte) {
        if (op !== 'after') setOp('after')
        if (a !== (gte || '')) setA(gte || '')
        if (b !== '') setB('')
      } else if (lt) {
        if (op !== 'before') setOp('before')
        try { if (lt) { const d = new Date(`${lt}T00:00:00`); d.setDate(d.getDate()-1); const prev = ymd(d); if (b !== prev) setB(prev) } } catch {}
        if (a !== '') setA('')
      }
    } catch {}
  }, [field, (where as any)?.[`${field}__date_preset`], (where as any)?.[`${field}__gte`], (where as any)?.[`${field}__lt`]])

  const lastSigRef = useRef<string>('')
  useEffect(() => {
    if (mode === 'manual') return
    if (!interactedRef.current) return
    const patch: Record<string, any> = { [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [field]: undefined, [`${field}__date_preset`]: undefined }
    if (mode === 'preset') {
      patch[`${field}__date_preset`] = config
      const sig = JSON.stringify(patch)
      if (sig !== lastSigRef.current) { lastSigRef.current = sig; onPatchAction(patch) }
      return
    }
    if (op === 'after') {
      patch[`${field}__gte`] = a || undefined
    } else if (op === 'before') {
      if (b) { const d = new Date(`${b}T00:00:00`); d.setDate(d.getDate()+1); patch[`${field}__lt`] = ymd(d) }
    } else if (op === 'between') {
      patch[`${field}__gte`] = a || undefined
      if (b) { const d = new Date(`${b}T00:00:00`); d.setDate(d.getDate()+1); patch[`${field}__lt`] = ymd(d) }
    }
    const sig = JSON.stringify(patch)
    if (sig !== lastSigRef.current) { lastSigRef.current = sig; onPatchAction(patch) }
  }, [mode, JSON.stringify(config), op, a, b])
  
  // Emit patch for manual mode
  useEffect(() => {
    if (mode !== 'manual') return
    const patch: Record<string, any> = { [`${field}__gte`]: undefined, [`${field}__lt`]: undefined, [field]: undefined }
    patch[field] = sel.length ? sel : undefined
    const sig = JSON.stringify(patch)
    if (sig !== lastSigRef.current) { lastSigRef.current = sig; onPatchAction(patch) }
  }, [mode, JSON.stringify(sel)])
  
  // Smart sort for dates
  const opts = ((distinctCache?.[field] || []) as string[]).map((s) => String(s))
  const sortedOpts = [...opts].sort((a, b) => {
    const da = new Date(a), db = new Date(b)
    const ta = da.getTime(), tb = db.getTime()
    if (isNaN(ta) && isNaN(tb)) return String(a).localeCompare(String(b))
    if (isNaN(ta)) return 1
    if (isNaN(tb)) return -1
    return ta - tb
  })
  const filtered = sortedOpts.filter((s) => s.toLowerCase().includes(q.toLowerCase()))
  
  // Auto-select search results
  useEffect(() => {
    if (mode !== 'manual' || !q) return
    const set = new Set<string>(sel)
    filtered.forEach((v) => set.add(v))
    setSel(Array.from(set.values()))
  }, [q])
  
  const toggle = (v: string) => {
    const exists = sel.includes(v)
    setSel(exists ? sel.filter((x) => x !== v) : [...sel, v])
  }
  const selectAll = () => setSel([...sortedOpts])
  const deselectAll = () => setSel([])

  // Group quick picks for the dropdown
  const groups = QUICK_PICKS.reduce<Record<string, QuickPick[]>>((acc, qp) => {
    (acc[qp.group] ??= []).push(qp); return acc
  }, {})

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='preset'} onChange={()=>{ interactedRef.current = true; markEditing(); setMode('preset') }} /> Preset</label>
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='custom'} onChange={()=>{ interactedRef.current = true; markEditing(); setMode('custom') }} /> Custom</label>
        <label className="inline-flex items-center gap-1"><input type="radio" checked={mode==='manual'} onChange={()=>{ interactedRef.current = true; markEditing(); setMode('manual') }} /> Manual</label>
      </div>
      {mode==='preset' ? (
        <>
        {/* Quick Pick dropdown */}
        <select className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={selectedQuickPick ?? ''} onChange={(e)=>{
          interactedRef.current = true; markEditing()
          const label = e.target.value
          const qp = QUICK_PICKS.find(q => q.label === label)
          if (qp) setConfig({ ...qp.config })
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
            <select className="w-full px-1.5 py-0.5 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={config.period} onChange={e => { interactedRef.current = true; markEditing(); setConfig(c => ({ ...c, period: e.target.value as any })) }}>
              {PERIOD_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>
          <div>
            <span className="text-[10px] text-muted-foreground">Offset</span>
            <select className="w-full px-1.5 py-0.5 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={config.offset} onChange={e => { interactedRef.current = true; markEditing(); setConfig(c => ({ ...c, offset: e.target.value as any })) }}>
              {OFFSET_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>
          <div>
            <span className="text-[10px] text-muted-foreground">As Of</span>
            <select className="w-full px-1.5 py-0.5 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={config.as_of} onChange={e => { interactedRef.current = true; markEditing(); setConfig(c => ({ ...c, as_of: e.target.value as any })) }}>
              {AS_OF_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>
          <div>
            <span className="text-[10px] text-muted-foreground">Range Mode</span>
            <select className="w-full px-1.5 py-0.5 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={config.range_mode} onChange={e => { interactedRef.current = true; markEditing(); setConfig(c => ({ ...c, range_mode: e.target.value as any })) }}>
              {RANGE_MODE_OPTIONS.map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <label className="inline-flex items-center gap-1 text-[10px]">
            <input type="checkbox" checked={config.include_weekends} onChange={e => { interactedRef.current = true; markEditing(); setConfig(c => ({ ...c, include_weekends: e.target.checked })) }} />
            <span className="text-muted-foreground">Include Weekends</span>
          </label>
          <label className="inline-flex items-center gap-1 text-[10px]">
            <input type="checkbox" checked={config.apply_holidays} onChange={e => { interactedRef.current = true; markEditing(); setConfig(c => ({ ...c, apply_holidays: e.target.checked })) }} />
            <span className="text-muted-foreground">Apply Holidays</span>
          </label>
        </div>
        </>
      ) : mode==='custom' ? (
        <div className="grid grid-cols-3 gap-2">
          <select className="col-span-3 sm:col-span-1 px-2 py-1 rounded-md bg-[hsl(var(--secondary)/0.6)] text-xs" value={op} onChange={(e)=>{ interactedRef.current = true; markEditing(); setOp(e.target.value as CustomOp) }}>
            <option value="after">After</option>
            <option value="before">Before</option>
            <option value="between">Between</option>
          </select>
          {op!=='between' ? (
            <input type="date" className="col-span-3 sm:col-span-2 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" value={op==='after'?a:b} onChange={(e)=> { interactedRef.current = true; markEditing(); (op==='after'? setA(e.target.value) : setB(e.target.value)) }} />
          ) : (
            <>
              <input type="date" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Start" value={a} onChange={(e)=>{ interactedRef.current = true; markEditing(); setA(e.target.value) }} />
              <input type="date" className="col-span-3 sm:col-span-1 h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="End" value={b} onChange={(e)=>{ interactedRef.current = true; markEditing(); setB(e.target.value) }} />
            </>
          )}
        </div>
      ) : (
        <div className="space-y-2">
          <input className="w-full h-8 px-2 rounded-md border text-[12px] bg-[hsl(var(--secondary)/0.6)]" placeholder="Search values" value={q} onChange={(e)=>setQ(e.target.value)} />
          <div className="flex items-center justify-between text-[11px] text-muted-foreground">
            <span>{sel.length} of {sortedOpts.length} selected</span>
            <div className="flex gap-2">
              <button className="hover:text-foreground" onClick={selectAll}>Select All</button>
              <button className="hover:text-foreground" onClick={deselectAll}>Deselect All</button>
            </div>
          </div>
          <div className="max-h-56 overflow-auto border rounded-md">
            <ul className="p-2 space-y-1">
              {filtered.map((v) => (
                <li key={v} className="flex items-center gap-2 text-xs">
                  <input type="checkbox" className="size-3" checked={sel.includes(v)} onChange={()=>toggle(v)} />
                  <span className="truncate" title={v}>{v}</span>
                </li>
              ))}
              {filtered.length === 0 && loadingCache?.[field] && (
                <li className="text-xs text-muted-foreground flex items-center gap-2">
                  <span className="inline-block size-3 border-2 border-muted-foreground/30 border-t-muted-foreground rounded-full animate-spin" />
                  Loading values...
                </li>
              )}
            </ul>
          </div>
        </div>
      )}
    </div>
  )
}

export function FilterbarShell(props: { label: string; icon?: ReactNode; className?: string; children: ReactNode }) {
  const { label, icon, className, children } = props
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [menuWidth, setMenuWidth] = useState<number | undefined>(undefined)
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null)
  // Gate configurator hover expansion while this filterbar popover is open.
  useEffect(() => {
    if (typeof document === 'undefined' || typeof window === 'undefined') return
    const body = document.body as any
    const w = window as any
    const st = (w.__actionsMenuState ||= { count: 0, timeoutId: null as any })
    if (open) {
      if (st.timeoutId) { window.clearTimeout(st.timeoutId); st.timeoutId = null }
      st.count = Math.max(0, Number(st.count || 0)) + 1
      body.dataset.actionsMenuOpen = '1'
      console.debug('[FilterbarShell] Gate ON, count:', st.count, 'flag:', body.dataset.actionsMenuOpen)
    } else {
      const prev = Math.max(0, Number(st.count || 0))
      if (prev <= 0) return
      st.count = prev - 1
      console.debug('[FilterbarShell] Gate dec, count:', st.count)
      if (st.count === 0) {
        if (st.timeoutId) window.clearTimeout(st.timeoutId)
        st.timeoutId = window.setTimeout(() => {
          try { delete body.dataset.actionsMenuOpen } catch {}
          console.debug('[FilterbarShell] Gate OFF (cooldown)')
          st.timeoutId = null
        }, 300)
      }
    }
  }, [open])
  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (!open) return
      const el = containerRef.current
      const tgt = e.target as Node
      if (el && (tgt === el || el.contains(tgt))) return
      if (e.target instanceof Element && (e.target as Element).closest('.filterbar-popover')) return
      setOpen(false)
    }
    document.addEventListener('mousedown', onDocClick)
    return () => document.removeEventListener('mousedown', onDocClick)
  }, [open])
  useEffect(() => {
    if (open && containerRef.current) {
      const { width, height, top, left } = containerRef.current.getBoundingClientRect()
      setMenuWidth(width)
      setPos({ top: top + height + window.scrollY + 4, left: left + window.scrollX })
    }
  }, [open])
  return (
    <div ref={containerRef} className={`group relative z-10 inline-flex items-center rounded-tremor-small text-[12px] leading-none font-medium shadow-tremor-input ${className||''}`} onClick={(e) => e.stopPropagation()}>
      <span className="inline-flex items-center h-8 rounded-l-tremor-small border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-2 text-[hsl(var(--muted-foreground))] transition-colors group-hover:bg-[hsl(var(--muted))] focus:z-10 cursor-pointer">
        {icon || <RiCalendar2Line className="size-4 shrink-0 text-[hsl(var(--muted-foreground))]" aria-hidden />}
      </span>
      <button
        type="button"
        className="-ml-px inline-flex items-center gap-x-2 h-8 rounded-r-tremor-small border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-3 text-[hsl(var(--foreground))] transition-colors hover:bg-[hsl(var(--muted))] group-hover:bg-[hsl(var(--muted))] focus:z-10 focus:outline-none focus:ring-2 focus:ring-[hsl(var(--ring))] focus:border-[hsl(var(--ring))] cursor-pointer"
        aria-expanded={open}
        aria-haspopup="menu"
        onClick={(e) => { e.stopPropagation(); setOpen(v => !v) }}
      >
        {label}
        <RiArrowDownSLine className="-mr-1 size-4 shrink-0" aria-hidden />
      </button>
      {open && pos && typeof window !== 'undefined' && createPortal(
        <div
          className="filterbar-popover z-[80] rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--popover))] text-[12px] shadow-none p-2"
          style={{ position: 'absolute', top: pos.top, left: pos.left, width: menuWidth ? `${Math.max(menuWidth, 300)}px` : '300px', maxWidth: '480px' }}
          role="menu"
          onClick={(e) => e.stopPropagation()}
        >
          {children}
        </div>,
        document.body
      )}
    </div>
  )
}

export default function FilterbarControl(props: any) {
  const { active, options, labels, onChange, className, disabled } = props as FilterbarControlProps
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [menuWidth, setMenuWidth] = useState<number | undefined>(undefined)
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null)
  // Gate configurator hover expansion while the global filterbar popover is open.
  useEffect(() => {
    if (typeof document === 'undefined' || typeof window === 'undefined') return
    const body = document.body as any
    const w = window as any
    const st = (w.__actionsMenuState ||= { count: 0, timeoutId: null as any })
    const gate = open && !disabled
    if (gate) {
      if (st.timeoutId) { window.clearTimeout(st.timeoutId); st.timeoutId = null }
      st.count = Math.max(0, Number(st.count || 0)) + 1
      body.dataset.actionsMenuOpen = '1'
      console.debug('[FilterbarControl] Gate ON, count:', st.count, 'flag:', body.dataset.actionsMenuOpen)
    } else {
      const prev = Math.max(0, Number(st.count || 0))
      if (prev <= 0) return
      st.count = prev - 1
      console.debug('[FilterbarControl] Gate dec, count:', st.count)
      if (st.count === 0) {
        if (st.timeoutId) window.clearTimeout(st.timeoutId)
        st.timeoutId = window.setTimeout(() => {
          try { delete body.dataset.actionsMenuOpen } catch {}
          console.debug('[FilterbarControl] Gate OFF (cooldown)')
          st.timeoutId = null
        }, 300)
      }
    }
  }, [open, disabled])
  useEffect(() => {
    function onDocClick(e: MouseEvent) {
      if (!open) return
      const el = containerRef.current
      const tgt = e.target as Node
      if (el && (tgt === el || el.contains(tgt))) return
      if (e.target instanceof Element && (e.target as Element).closest('.filterbar-popover')) return
      setOpen(false)
    }
    document.addEventListener('mousedown', onDocClick)
    return () => document.removeEventListener('mousedown', onDocClick)
  }, [open])
  useEffect(() => {
    if (open && containerRef.current) {
      const { width, height, top, left } = containerRef.current.getBoundingClientRect()
      setMenuWidth(width)
      setPos({ top: top + height + window.scrollY + 4, left: left + window.scrollX })
    }
  }, [open])
  const activeLabel = (active && labels[active]) ? labels[active] : Object.values(labels)[0] || 'Select'
  return (
    <div ref={containerRef} className={`group relative z-10 inline-flex items-center rounded-tremor-small text-[12px] leading-none font-medium shadow-tremor-input dark:shadow-dark-tremor-input ${className||''} ${disabled?'opacity-60 cursor-not-allowed':''}`} onClick={(e) => e.stopPropagation()}>
      <span className="inline-flex items-center h-8 rounded-l-tremor-small border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-2 text-[hsl(var(--muted-foreground))] transition-colors ${disabled?'':'group-hover:bg-[hsl(var(--muted))]'} focus:z-10 ${disabled?'cursor-not-allowed':'cursor-pointer'}">
        <RiCalendar2Line className="size-4 shrink-0 text-[hsl(var(--muted-foreground))]" aria-hidden />
      </span>
      <button
        type="button"
        className="-ml-px inline-flex items-center gap-x-2 h-8 rounded-r-tremor-small border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-3 text-[hsl(var(--foreground))] transition-colors ${disabled?'':'hover:bg-[hsl(var(--muted))] group-hover:bg-[hsl(var(--muted))]'} focus:z-10 focus:outline-none focus:ring-2 focus:ring-[hsl(var(--ring))] focus:border-[hsl(var(--ring))] ${disabled?'cursor-not-allowed':'cursor-pointer'}"
        aria-expanded={open}
        aria-haspopup="menu"
        onClick={(e) => { e.stopPropagation(); if (disabled) return; setOpen(v => !v) }}
        disabled={!!disabled}
        aria-disabled={!!disabled}
      >
        {activeLabel}
        <RiArrowDownSLine className="-mr-1 size-4 shrink-0" aria-hidden />
      </button>
      {open && !disabled && pos && typeof window !== 'undefined' && createPortal(
        <div
          className="filterbar-popover z-[80] rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--popover))] text-[12px] shadow-none p-1 whitespace-normal break-words"
          style={{ position: 'absolute', top: pos.top, left: pos.left, width: menuWidth ? `${Math.max(menuWidth, 220)}px` : '240px', maxWidth: '320px' }}
          role="menu"
          onClick={(e) => e.stopPropagation()}
        >
          {options.map((opt) => (
            <button
              key={opt}
              className={`w-full text-left px-3 py-1.5 rounded-md mx-0.5 my-0.5 text-[hsl(var(--foreground))] transition-colors hover:bg-[hsl(var(--muted))] ${active===opt?'font-medium':''}`}
              role="menuitem"
              onClick={() => { if (disabled) return; onChange(opt); setOpen(false) }}
            >
              {labels[opt] ?? opt}
            </button>
          ))}
        </div>,
        document.body
      )}
    </div>
  )
}
