"use client"

import { useEffect, useMemo, useRef, useState } from 'react'

export type AdvancedSqlCustomColumnBuilderProps = {
  columns?: string[]
  takenNames?: string[]
  onAddAction: (col: { name: string; expr: string; type?: 'string'|'number'|'date'|'boolean'; scope?: { level: 'widget'|'table'|'datasource'; widgetId?: string; table?: string } }) => void
  onCancelAction?: () => void
  widgetId?: string
  source?: string
  initial?: { name: string; expr: string; type?: 'string'|'number'|'date'|'boolean'; scope?: { level: 'widget'|'table'|'datasource'; widgetId?: string; table?: string } }
  submitLabel?: string
  ignoreName?: string
  registerInsertSinkAction?: (fn: (txt: string) => void) => void
}

export default function AdvancedSqlCustomColumnBuilder({ columns = [], takenNames = [], onAddAction, onCancelAction, widgetId, source, initial, submitLabel, ignoreName, registerInsertSinkAction }: AdvancedSqlCustomColumnBuilderProps) {
  const [mode, setMode] = useState<'expr'|'map'>('expr')
  const [name, setName] = useState('')
  const [expr, setExpr] = useState('')
  const [type, setType] = useState<''|'string'|'number'|'date'|'boolean'>('')
  const [scope, setScope] = useState<'widget'|'table'|'datasource'>(() => (source ? 'table' : 'datasource'))
  // Mapping mode state
  const [sourceCol, setSourceCol] = useState<string>(columns[0] || '')
  const [findLines, setFindLines] = useState<string>('')
  const [replaceLines, setReplaceLines] = useState<string>('')
  const [pasteGrid, setPasteGrid] = useState<string>('')
  const [elseVal, setElseVal] = useState<string>('')
  const exprRef = useRef<HTMLInputElement | null>(null)

  const pairs = useMemo(() => {
    const f = findLines.split(/\r?\n/).map(s => s.trim()).filter(Boolean)
    const r = replaceLines.split(/\r?\n/).map(s => s.trim())
    const max = Math.max(f.length, r.length)
    const out: Array<{ find: string; replace: string }> = []
    for (let i = 0; i < max; i++) {
      const find = f[i]
      if (!find) continue
      out.push({ find, replace: r[i] ?? '' })
    }
    return out
  }, [findLines, replaceLines])

  // Prefill for edit mode
  useEffect(() => {
    if (!initial) return
    try {
      setMode('expr')
      setName(String(initial.name || ''))
      setExpr(String(initial.expr || ''))
      setType((initial.type as any) || '')
      const sc = (initial as any).scope
      if (sc && sc.level) {
        if (sc.level === 'datasource') setScope('datasource')
        else if (sc.level === 'table') setScope('table')
        else if (sc.level === 'widget') setScope('widget')
      }
    } catch {}
  }, [initial])

  // Register this builder's Expression field as the current insertion sink
  useEffect(() => {
    if (!registerInsertSinkAction) return
    // Provide a function that inserts text at the caret in the Expression input
    const insertFn = (txt: string) => {
      setExpr((prev) => {
        const el = exprRef.current
        try {
          const start = (el?.selectionStart ?? prev.length)
          const end = (el?.selectionEnd ?? start)
          const next = prev.slice(0, start) + txt + prev.slice(end)
          // Restore caret after state update
          setTimeout(() => {
            try {
              el?.focus()
              const pos = start + txt.length
              el?.setSelectionRange(pos, pos)
            } catch {}
          }, 0)
          return next
        } catch {
          return prev + txt
        }
      })
    }
    // Register once and also re-register when mode switches to expr
    if (mode === 'expr') registerInsertSinkAction(insertFn)
  }, [registerInsertSinkAction, mode])

  function parseGrid() {
    const rows = pasteGrid.split(/\r?\n/).map(line => line.trim()).filter(Boolean)
    const finds: string[] = []
    const reps: string[] = []
    for (const row of rows) {
      const parts = row.split(/\t|,/)
      const a = (parts[0] || '').trim()
      const b = (parts[1] || '').trim()
      if (a) { finds.push(a); reps.push(b) }
    }
    setFindLines(finds.join('\n'))
    setReplaceLines(reps.join('\n'))
  }

  function sqlQuote(v: string) { return `'${String(v).replace(/'/g, "''")}'` }
  function bracket(id: string) { return `[${String(id).replace(/\]/g, ']]')}]` }
  function qualifyBase(col: string) {
    // If already contains a dot or bracketed alias, assume qualified
    if (/\.|\[.*\]\./.test(col)) return col
    return `${bracket('s')}.${bracket(col)}`
  }
  function buildCaseExpr() {
    const qualified = qualifyBase(sourceCol)
    const whenBlocks = pairs.map(p => `WHEN ${qualified} = ${sqlQuote(p.find)} THEN ${sqlQuote(p.replace)}`)
    const elseBlock = elseVal !== '' ? ` ELSE ${sqlQuote(elseVal)}` : ''
    return `CASE\n  ${whenBlocks.join('\n  ')}${elseBlock}\nEND`
  }

  const canAdd = useMemo(() => {
    if (mode === 'expr') return name.trim() !== '' && expr.trim() !== ''
    return name.trim() !== '' && !!sourceCol && pairs.length > 0
  }, [mode, name, expr, sourceCol, pairs])

  const nameConflict = useMemo(() => {
    const n = name.trim()
    if (!n) return false
    const taken = new Set(takenNames.map(s => String(s || '').trim()).filter(Boolean))
    if (ignoreName && taken.has(ignoreName)) taken.delete(ignoreName)
    return taken.has(n)
  }, [name, takenNames])

  return (
    <div className="rounded-md border p-2 bg-[hsl(var(--secondary)/0.6)] text-[12px]">
      <div className="flex items-center gap-2 mb-2">
        <button type="button" className={`text-[11px] px-2 py-1 rounded-md border ${mode==='expr'?'bg-[hsl(var(--secondary))]':''}`} onClick={()=>setMode('expr')}>Expression</button>
        <button type="button" className={`text-[11px] px-2 py-1 rounded-md border ${mode==='map'?'bg-[hsl(var(--secondary))]':''}`} onClick={()=>setMode('map')}>Mapping</button>
      </div>

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Name</label>
        <input
          className={`h-8 px-2 rounded-md bg-card text-xs sm:col-span-2 ${nameConflict ? 'ring-2 ring-red-500' : ''}`}
          placeholder="new column name"
          value={name}
          onChange={(e)=>setName(e.target.value)}
          title={nameConflict ? 'Name conflicts with an existing column or custom column' : ''}
        />
      </div>
      {nameConflict && <div className="text-[11px] text-amber-600 mb-1">Name conflicts with an existing column or another custom column. Please choose a unique name.</div>}

      {mode === 'expr' && (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
            <label className="text-xs text-muted-foreground sm:col-span-1">Expression</label>
            <input ref={exprRef} onFocus={() => { try { registerInsertSinkAction && registerInsertSinkAction((txt: string) => {
              setExpr((prev) => {
                const el = exprRef.current
                try {
                  const start = (el?.selectionStart ?? prev.length)
                  const end = (el?.selectionEnd ?? start)
                  const next = prev.slice(0, start) + txt + prev.slice(end)
                  setTimeout(() => {
                    try { el?.focus(); const pos = start + txt.length; el?.setSelectionRange(pos, pos) } catch {}
                  }, 0)
                  return next
                } catch { return prev + txt }
              })
            }) } catch {} }} className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="SQL expression, e.g., price * qty" value={expr} onChange={(e)=>setExpr(e.target.value)} />
          </div>
        </>
      )}

      {mode === 'map' && (
        <>
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
            <label className="text-xs text-muted-foreground sm:col-span-1">Source column</label>
            <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={sourceCol} onChange={(e)=>setSourceCol(e.target.value)}>
              {columns.map(c => (<option key={c} value={c}>{c}</option>))}
            </select>
          </div>

          <div className="text-xs font-medium mb-1">Paste grid (optional)</div>
          <div className="grid grid-cols-1 gap-2 mb-2">
            <textarea
              className="w-full min-h-[80px] px-2 py-1 rounded-md bg-card font-mono text-[11px]"
              placeholder={'Paste two columns from Excel: find\treplace or find,replace'}
              value={pasteGrid}
              onChange={(e)=>setPasteGrid(e.target.value)}
            />
            <div className="flex items-center justify-end">
              <button type="button" className="text-[11px] px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={parseGrid}>Parse to lists</button>
            </div>
          </div>

          <div className="text-xs font-medium mb-1">Find/Replace lists</div>
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 mb-2">
            <div>
              <label className="block text-[11px] text-muted-foreground mb-1">Find values (one per line)</label>
              <textarea className="w-full min-h-[120px] px-2 py-1 rounded-md bg-card font-mono text-[11px]" value={findLines} onChange={(e)=>setFindLines(e.target.value)} />
            </div>
            <div>
              <label className="block text-[11px] text-muted-foreground mb-1">Replace with (one per line)</label>
              <textarea className="w-full min-h-[120px] px-2 py-1 rounded-md bg-card font-mono text-[11px]" value={replaceLines} onChange={(e)=>setReplaceLines(e.target.value)} />
            </div>
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
            <label className="text-xs text-muted-foreground sm:col-span-1">ELSE</label>
            <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="Else value (optional)" value={elseVal} onChange={(e)=>setElseVal(e.target.value)} />
          </div>
        </>
      )}

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

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-3">
        <label className="text-xs text-muted-foreground sm:col-span-1">Type</label>
        <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={type} onChange={(e)=>setType(e.target.value as any)}>
          <option value="">(infer)</option>
          <option value="string">string</option>
          <option value="number">number</option>
          <option value="date">date</option>
          <option value="boolean">boolean</option>
        </select>
      </div>

      <div className="flex items-center justify-end gap-2">
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCancelAction}>Cancel</button>
        <button className={`text-xs px-2 py-1 rounded-md border ${(canAdd && !nameConflict)? 'bg-[hsl(var(--btn3))] text-black':'opacity-60 cursor-not-allowed'}`} disabled={!canAdd || nameConflict} onClick={() => {
          const finalExpr = (mode === 'expr') ? expr.trim() : buildCaseExpr()
          const payload: any = { name: name.trim(), expr: finalExpr }
          if (type) payload.type = type
          // attach scope
          if (scope === 'datasource') payload.scope = { level: 'datasource' }
          else if (scope === 'table' && source) payload.scope = { level: 'table', table: source }
          else if (scope === 'widget' && widgetId) payload.scope = { level: 'widget', widgetId }
          onAddAction(payload)
        }}>{submitLabel || 'Add Custom Column'}</button>
      </div>
    </div>
  )
}
