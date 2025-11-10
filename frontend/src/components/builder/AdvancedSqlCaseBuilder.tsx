"use client"

import { useEffect, useMemo, useState } from 'react'
import type { Condition } from '@/lib/dsl'

export type AdvancedSqlCaseBuilderProps = {
  columns: string[]
  onAddAction: (tr: { type: 'case'; target: string; cases: { when: Condition; then: any }[]; else?: any } | { type: 'computed'; name: string; expr: string }) => void
  onCancelAction?: () => void
  initial?: (
    { type: 'case'; target: string; cases: { when: Condition; then: any }[]; else?: any } |
    { type: 'computed'; name: string; expr: string }
  )
  submitLabel?: string
}

export default function AdvancedSqlCaseBuilder({ columns, onAddAction, onCancelAction, initial, submitLabel }: AdvancedSqlCaseBuilderProps) {
  const [target, setTarget] = useState<string>('')
  type Row = { op: Condition['op']; value: string; then: string }
  const [rows, setRows] = useState<Row[]>([{ op: 'eq', value: '', then: '' }])
  const [elseVal, setElseVal] = useState<string>('')
  // Base column for simple mode (left-hand side)
  const [baseCol, setBaseCol] = useState<string>(columns[0] || '')
  type Pred = { leftKind: 'col'|'val'; left: string; op: Condition['op']; rightKind: 'col'|'val'; right: string; join?: 'AND'|'OR' }
  type Group = { preds: Pred[]; then: string }
  const [useAdvanced, setUseAdvanced] = useState<boolean>(false)
  const [groups, setGroups] = useState<Group[]>([])

  // Prefill from initial when editing
  useEffect(() => {
    if (!initial) return
    console.log('[AdvancedSqlCaseBuilder] Prefilling from initial:', initial)
    try {
      const initType = String((initial as any)?.type || '').toLowerCase()
      if (initType === 'case') {
        const init = initial as any
        setTarget(String(init.target || ''))
        const rws: Row[] = (init.cases || []).map((c: any) => {
          const op = (c.when?.op || 'eq') as Condition['op']
          const right: any = (c.when as any)?.right
          const value = Array.isArray(right) ? right.join(',') : String(right ?? '')
          return { op, value, then: String(c.then ?? '') }
        })
        setRows(rws.length ? rws : [{ op: 'eq', value: '', then: '' }])
        setElseVal(init.else != null ? String(init.else) : '')
        setUseAdvanced(false)
      } else if (initType === 'computed') {
        const init = initial as any
        const expr = String(init.expr || '')
        console.log('[AdvancedSqlCaseBuilder] Parsing computed CASE expr:', expr)
        setTarget(String(init.name || ''))
        const s = expr
        const whenRe = /WHEN\s*\((.*?)\)\s*THEN\s*'((?:[^']|'')*)'/gis
        const groupsParsed: Group[] = []
        let m: RegExpExecArray | null
        while ((m = whenRe.exec(s)) !== null) {
          const inner = m[1]
          const thenVal = m[2].replace(/''/g, "'")
          const preds: Pred[] = []
          const tokens: string[] = []
          const joins: Array<'AND'|'OR'> = []
          let i = 0, depth = 0, inStr = false, buf = ''
          while (i < inner.length) {
            const ch = inner[i]
            if (ch === "'") { inStr = !inStr; buf += ch; i++; continue }
            if (!inStr) {
              if (ch === '(') { depth++; buf += ch; i++; continue }
              if (ch === ')') { depth = Math.max(0, depth-1); buf += ch; i++; continue }
              const rest = inner.slice(i).toUpperCase()
              if (depth === 0 && (rest.startsWith(' AND ') || rest.startsWith(' OR '))) {
                tokens.push(buf.trim()); buf = ''
                joins.push(rest.startsWith(' AND ') ? 'AND' : 'OR')
                i += rest.startsWith(' AND ') ? 5 : 4
                continue
              }
            }
            buf += ch; i++
          }
          if (buf.trim()) tokens.push(buf.trim())
          tokens.forEach((tok, idx) => {
            const jn = idx === 0 ? undefined : joins[idx-1]
            const t = tok.trim()
            let p: Pred | null = null
            // Handle both IN (...) and IN [...] formats
            const inM = t.match(/^(.*?)\s+IN\s*[\(\[](.*)[ \)\]]\s*$/i)
            if (inM) {
              const L = inM[1].trim()
              const R = inM[2].trim()
              const vals: string[] = []
              // Try to extract quoted values
              const qre = /'((?:[^']|'')*)'/g
              let qm: RegExpExecArray | null
              while ((qm = qre.exec(R)) !== null) {
                const val = qm[1].replace(/''/g, "'")
                // If the value contains commas, split it
                if (val.includes(',')) {
                  vals.push(...val.split(',').map(s => s.trim()).filter(Boolean))
                } else {
                  vals.push(val)
                }
              }
              // Fallback: if no quoted values found, try splitting by comma
              if (vals.length === 0) {
                vals.push(...R.split(',').map(s => s.trim()).filter(Boolean))
              }
              const leftIsCol = /^\s*\[[^\]]+\]\s*$/i.test(L)
              const left = leftIsCol ? L.replace(/^\s*\[|\]\s*$/g,'').replace(/\]\]/g,']') : L.replace(/^'|'$/g,'').replace(/''/g, "'")
              p = { leftKind: leftIsCol ? 'col' : 'val', left, op: 'in', rightKind: 'val', right: vals.join(',') }
            } else {
              const cmp = t.match(/^(.*?)\s*(=|<>|>=|<=|>|<|LIKE)\s*(.*)$/i)
              if (cmp) {
                const L = cmp[1].trim()
                const opSym = cmp[2].toUpperCase()
                const R = cmp[3].trim()
                const map: Record<string, Pred['op']> = { '=':'eq', '<>':'ne', '>':'gt', '>=':'gte', '<':'lt', '<=':'lte', 'LIKE':'like' }
                const op = (map[opSym] || 'eq') as Pred['op']
                const leftIsCol = /^\s*\[[^\]]+\]\s*$/i.test(L)
                const rightIsCol = /^\s*\[[^\]]+\]\s*$/i.test(R)
                const left = leftIsCol ? L.replace(/^\s*\[|\]\s*$/g,'').replace(/\]\]/g,']') : L.replace(/^'|'$/g,'').replace(/''/g, "'")
                const right = rightIsCol ? R.replace(/^\s*\[|\]\s*$/g,'').replace(/\]\]/g,']') : R.replace(/^'|'$/g,'').replace(/''/g, "'")
                p = { leftKind: leftIsCol ? 'col' : 'val', left, op, rightKind: rightIsCol ? 'col' : 'val', right }
              }
            }
            if (p) preds.push({ ...p, join: jn })
          })
          groupsParsed.push({ preds, then: thenVal })
        }
        const elseM = s.match(/ELSE\s*'((?:[^']|'')*)'/i)
        if (elseM) setElseVal(elseM[1].replace(/''/g, "'"))
        console.log('[AdvancedSqlCaseBuilder] Parsed groups:', groupsParsed)
        setGroups(groupsParsed)
        setUseAdvanced(true)
      }
    } catch (e) {
      console.error('[AdvancedSqlCaseBuilder] Parse error:', e)
    }
  }, [initial, columns])

  const canAdd = useMemo(() => {
    if (!target) return false
    if (useAdvanced) {
      if (!groups.length) return false
      return groups.every(g => g.then !== '' && g.preds.length > 0 && g.preds.every(p => (
        (p.leftKind === 'col' ? !!p.left : p.left !== '') && (p.rightKind === 'col' ? !!p.right : (p.op === 'in' ? p.right.split(',').filter(s=>s.trim()!=='').length>0 : p.right !== ''))
      )))
    }
    return rows.every(r => r.value !== '' && r.then !== '')
  }, [target, rows, useAdvanced, groups])

  return (
    <div className="rounded-md border p-2 bg-[hsl(var(--secondary)/0.6)] text-[12px]">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Output alias</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={target} onChange={(e)=>setTarget(e.target.value)} />
      </div>
      {!useAdvanced && (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
          <label className="text-xs text-muted-foreground sm:col-span-1">Base column</label>
          <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={baseCol} onChange={(e)=>setBaseCol(e.target.value)}>
            {columns.map(c => (<option key={c} value={c}>{c}</option>))}
          </select>
        </div>
      )}
      <div className="text-xs font-medium mb-1">Conditions</div>
      <div className="space-y-2 mb-2">
        <label className="flex items-center gap-2 text-xs">
          <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={useAdvanced} onChange={(e)=>setUseAdvanced(e.target.checked)} />
          Advanced AND/OR mode
        </label>
        {!useAdvanced && (
          <div className="space-y-2">
            {rows.map((r, i) => (
              <div key={i} className="grid grid-cols-[72px,120px,1fr,1fr,auto] gap-2 items-center">
                <span className="text-[11px] text-muted-foreground">WHEN</span>
                <select className="h-8 px-2 rounded-md bg-card text-xs" value={r.op} onChange={(e)=>{
                  const next = [...rows]; next[i] = { ...r, op: e.target.value as any }; setRows(next)
                }}>
                  {['eq','ne','gt','gte','lt','lte','in','like','regex'].map(op => (<option key={op} value={op}>{op}</option>))}
                </select>
                <input className="h-8 px-2 rounded-md bg-card text-xs" placeholder={r.op==='in' ? 'Find value (comma-separated)' : 'Find value'} value={r.value} 
                  onKeyDown={(e)=>{
                    if (r.op !== 'in' && e.key === ',') {
                      e.preventDefault()
                    }
                  }}
                  onChange={(e)=>{
                    let val = e.target.value
                    // Remove commas if operator doesn't support them
                    if (r.op !== 'in') {
                      val = val.replace(/,/g, '')
                    }
                    const next = [...rows]; next[i] = { ...r, value: val }; setRows(next)
                  }}/>
                <input className="h-8 px-2 rounded-md bg-card text-xs" placeholder="Replace with (THEN)" value={r.then} onChange={(e)=>{
                  const next = [...rows]; next[i] = { ...r, then: e.target.value }; setRows(next)
                }}/>
                <button className="text-[11px] px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setRows(rows.filter((_,j)=>j!==i))}>Remove</button>
              </div>
            ))}
            <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setRows([...rows, { op: 'eq', value: '', then: '' }])}>+ Add condition</button>
          </div>
        )}

        {useAdvanced && (
          <div className="space-y-2">
            {groups.map((g, gi) => (
              <div key={gi} className="space-y-2 border rounded-md p-2 pl-8 bg-card/30 relative cursor-move" draggable onDragStart={(e)=>{
                e.dataTransfer.effectAllowed = 'move'
                e.dataTransfer.setData('text/plain', String(gi))
              }} onDragOver={(e)=>{
                e.preventDefault()
                e.dataTransfer.dropEffect = 'move'
              }} onDrop={(e)=>{
                e.preventDefault()
                const fromIdx = parseInt(e.dataTransfer.getData('text/plain'))
                if (fromIdx !== gi && !isNaN(fromIdx)) {
                  const next = [...groups]
                  const [moved] = next.splice(fromIdx, 1)
                  next.splice(gi, 0, moved)
                  setGroups(next)
                }
              }}>
                <div className="absolute left-2 top-2 text-muted-foreground text-sm cursor-move" title="Drag to reorder">⋮⋮</div>
                {g.preds.map((p, pi) => (
                  <div key={pi} className="grid grid-cols-1 md:grid-cols-[72px,84px,1fr,84px,1fr,auto] items-center gap-2">
                    <div className="text-[11px] text-muted-foreground">{pi===0 ? 'WHEN' : ''}</div>
                    {pi>0 ? (
                      <select className="h-8 px-2 rounded-md bg-card text-xs" value={p.join || 'AND'} onChange={(e)=>{
                        const next = groups.slice(); next[gi] = { ...g, preds: g.preds.map((pp, idx)=> idx===pi ? { ...pp, join: (e.target.value as any) } : pp) }; setGroups(next)
                      }}>
                        {['AND','OR'].map(j => (<option key={j} value={j}>{j}</option>))}
                      </select>
                    ) : (
                      <div />
                    )}
                    <div className="flex items-center gap-2">
                      <select className="h-8 px-2 rounded-md bg-card text-xs" value={p.leftKind} onChange={(e)=>{
                        const next = groups.slice(); next[gi] = { ...g, preds: g.preds.map((pp, idx)=> idx===pi ? { ...pp, leftKind: (e.target.value as any), left: '' } : pp) }; setGroups(next)
                      }}>
                        <option value="col">Column</option>
                        <option value="val">Value</option>
                      </select>
                      {p.leftKind === 'col' ? (
                        <select className="h-8 px-2 rounded-md bg-card text-xs w-full" value={p.left} onChange={(e)=>{
                          const next = groups.slice(); next[gi] = { ...g, preds: g.preds.map((pp, idx)=> idx===pi ? { ...pp, left: e.target.value } : pp) }; setGroups(next)
                        }}>
                          <option value=""></option>
                          {columns.map(c => (<option key={c} value={c}>{c}</option>))}
                        </select>
                      ) : (
                        <input className="h-8 px-2 rounded-md bg-card text-xs w-full" value={p.left} onChange={(e)=>{
                          const next = groups.slice(); next[gi] = { ...g, preds: g.preds.map((pp, idx)=> idx===pi ? { ...pp, left: e.target.value } : pp) }; setGroups(next)
                        }} />
                      )}
                    </div>
                    <select className="h-8 px-2 rounded-md bg-card text-xs" value={p.op} onChange={(e)=>{
                      const next = groups.slice(); next[gi] = { ...g, preds: g.preds.map((pp, idx)=> idx===pi ? { ...pp, op: (e.target.value as any) } : pp) }; setGroups(next)
                    }}>
                      {['eq','ne','gt','gte','lt','lte','in','like','regex'].map(op => (<option key={op} value={op}>{op}</option>))}
                    </select>
                    <div className="flex items-center gap-2">
                      <select className="h-8 px-2 rounded-md bg-card text-xs" value={p.rightKind} onChange={(e)=>{
                        const next = groups.slice(); next[gi] = { ...g, preds: g.preds.map((pp, idx)=> idx===pi ? { ...pp, rightKind: (e.target.value as any), right: '' } : pp) }; setGroups(next)
                      }}>
                        <option value="val">Value</option>
                        <option value="col">Column</option>
                      </select>
                      {p.rightKind === 'col' ? (
                        <select className="h-8 px-2 rounded-md bg-card text-xs w-full" value={p.right} onChange={(e)=>{
                          const next = groups.slice(); next[gi] = { ...g, preds: g.preds.map((pp, idx)=> idx===pi ? { ...pp, right: e.target.value } : pp) }; setGroups(next)
                        }}>
                          <option value=""></option>
                          {columns.map(c => (<option key={c} value={c}>{c}</option>))}
                        </select>
                      ) : (
                        <input className="h-8 px-2 rounded-md bg-card text-xs w-full" placeholder={p.op==='in' ? 'a,b,c' : ''} value={p.right} 
                          onKeyDown={(e)=>{
                            if (p.op !== 'in' && e.key === ',') {
                              e.preventDefault()
                            }
                          }}
                          onChange={(e)=>{
                            let val = e.target.value
                            // Remove commas if operator doesn't support them
                            if (p.op !== 'in') {
                              val = val.replace(/,/g, '')
                            }
                            const next = groups.slice(); next[gi] = { ...g, preds: g.preds.map((pp, idx)=> idx===pi ? { ...pp, right: val } : pp) }; setGroups(next)
                          }} />
                      )}
                    </div>
                    <button className="text-[11px] px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=>{
                      const next = groups.slice(); next[gi] = { ...g, preds: g.preds.filter((_,idx)=>idx!==pi) }; setGroups(next)
                    }}>Remove</button>
                  </div>
                ))}
                <div className="flex items-center justify-between gap-2">
                  <button className="text-[11px] px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=>{
                    const next = groups.slice(); const base: Pred = { leftKind:'col', left:'', op:'eq', rightKind:'val', right:'', join:'AND' }; next[gi] = { ...g, preds:[...g.preds, base] }; setGroups(next)
                  }}>+ Add predicate</button>
                  <input className="h-8 px-2 rounded-md bg-card text-xs" placeholder="THEN value" value={g.then} onChange={(e)=>{
                    const next = groups.slice(); next[gi] = { ...g, then: e.target.value }; setGroups(next)
                  }} />
                </div>
                <div className="flex items-center justify-end">
                  <button className="text-[11px] px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setGroups(groups.filter((_,idx)=>idx!==gi))}>Remove WHEN</button>
                </div>
              </div>
            ))}
            <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setGroups([...groups, { preds: [{ leftKind:'col', left:'', op:'eq', rightKind:'val', right:'', join: 'AND' }], then: '' }])}>+ Add WHEN</button>
          </div>
        )}
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-3">
        <label className="text-xs text-muted-foreground sm:col-span-1">ELSE</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="Else value (optional)" value={elseVal} onChange={(e)=>setElseVal(e.target.value)} />
      </div>
      <div className="flex items-center justify-end gap-2">
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCancelAction}>Cancel</button>
        <button className={`text-xs px-2 py-1 rounded-md border ${canAdd? 'bg-[hsl(var(--btn3))] text-black':'opacity-60 cursor-not-allowed'}`} disabled={!canAdd} onClick={()=>{
          if (useAdvanced) {
            const esc = (s: string) => String(s || '').replace(/'/g, "''")
            const qcol = (c: string) => `[${String(c || '').replace(/]/g, ']]')}]`
            const mapOp = (op: string) => ({ eq:'=', ne:'<>', gt:'>', gte:'>=', lt:'<', lte:'<=', like:'LIKE', regex:'LIKE' } as any)[op] || '='
            const isNumeric = (v: string) => /^\d+(\.\d+)?$/.test(String(v || '').trim())
            const qval = (v: string) => isNumeric(v) ? v : `'${esc(v)}'`
            const condSql = (p: Pred) => {
              const L = p.leftKind === 'col' ? qcol(p.left) : qval(p.left)
              if (p.op === 'in') {
                const arr = (p.rightKind === 'val' ? p.right.split(',').map(s=>s.trim()).filter(Boolean) : [p.right]).map(v => qval(v)).join(', ')
                return `${L} IN (${arr})`
              }
              const R = p.rightKind === 'col' ? qcol(p.right) : qval(p.right)
              return `${L} ${mapOp(p.op)} ${R}`
            }
            const parts: string[] = ['CASE']
            groups.forEach((g) => {
              const inner = g.preds.map((p, idx) => (idx>0 ? ` ${p.join || 'AND'} ${condSql(p)}` : condSql(p))).join('')
              parts.push(` WHEN (${inner}) THEN '${esc(g.then)}'`)
            })
            if (elseVal) parts.push(` ELSE '${esc(elseVal)}'`)
            parts.push(' END')
            const expr = parts.join('')
            const tr = { type: 'computed' as const, name: target, expr }
            onAddAction(tr)
          } else {
            const cases = rows.map(r => {
              const right = r.op === 'in' ? r.value.split(',').map(s=>s.trim()) : r.value
              const when: Condition = { op: r.op, left: `s.${baseCol}`, right }
              return { when, then: r.then }
            })
            const tr = { type: 'case' as const, target, cases, else: elseVal ? elseVal : undefined }
            onAddAction(tr)
          }
        }}>{submitLabel || 'Add CASE transform'}</button>
      </div>
    </div>
  )
}
