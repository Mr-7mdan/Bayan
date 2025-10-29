"use client"

import { useMemo, useState } from 'react'
import type { Condition } from '@/lib/dsl'

export type AdvancedSqlBulkCasesBuilderProps = {
  columns: string[]
  onAddAction: (tr: { type: 'case'; target: string; cases: { when: Condition; then: any }[]; else?: any }) => void
  onCancelAction?: () => void
}

export default function AdvancedSqlBulkCasesBuilder({ columns, onAddAction, onCancelAction }: AdvancedSqlBulkCasesBuilderProps) {
  const [target, setTarget] = useState<string>(columns[0] || '')
  const [findLines, setFindLines] = useState<string>('')
  const [replaceLines, setReplaceLines] = useState<string>('')
  const [pasteGrid, setPasteGrid] = useState<string>('')
  const [elseVal, setElseVal] = useState<string>('')

  const pairs = useMemo(() => {
    const f = findLines.split(/\r?\n/).map(s => s.trim()).filter(s => s !== '')
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

  const canAdd = useMemo(() => !!target && pairs.length > 0, [target, pairs])

  function parseGrid() {
    const rows = pasteGrid.split(/\r?\n/).map(line => line.trim()).filter(Boolean)
    const finds: string[] = []
    const reps: string[] = []
    for (const row of rows) {
      // support tab or comma separated two columns
      const parts = row.split(/\t|,/)
      const a = (parts[0] || '').trim()
      const b = (parts[1] || '').trim()
      if (a) {
        finds.push(a)
        reps.push(b)
      }
    }
    setFindLines(finds.join('\n'))
    setReplaceLines(reps.join('\n'))
  }

  return (
    <div className="rounded-md border p-2 bg-[hsl(var(--secondary)/0.6)] text-[12px]">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Target column</label>
        <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={target} onChange={(e)=>setTarget(e.target.value)}>
          {columns.map(c => (<option key={c} value={c}>{c}</option>))}
        </select>
      </div>

      <div className="text-xs font-medium mb-1">Paste grid (optional)</div>
      <div className="grid grid-cols-1 gap-2 mb-2">
        <textarea
          className="w-full min-h-[80px] px-2 py-1 rounded-md bg-card font-mono text-[11px]"
          placeholder={"Paste two columns from Excel: find\treplace or find,replace"}
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

      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-3">
        <label className="text-xs text-muted-foreground sm:col-span-1">ELSE</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" placeholder="Else value (optional)" value={elseVal} onChange={(e)=>setElseVal(e.target.value)} />
      </div>

      <div className="flex items-center justify-end gap-2">
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCancelAction}>Cancel</button>
        <button
          className={`text-xs px-2 py-1 rounded-md border ${canAdd? 'bg-[hsl(var(--btn3))] text-black':'opacity-60 cursor-not-allowed'}`}
          disabled={!canAdd}
          onClick={() => {
            const cases = pairs.map(p => {
              const when: Condition = { op: 'eq', left: target, right: p.find }
              return { when, then: p.replace }
            })
            const tr = { type: 'case' as const, target, cases, else: elseVal ? elseVal : undefined }
            onAddAction(tr)
          }}
        >Add Cases</button>
      </div>
    </div>
  )
}
