"use client"

import { useEffect, useMemo, useState } from 'react'

export type AdvancedSqlReplaceBuilderProps = {
  columns: string[]
  onAddAction: (tr: { type: 'replace'|'translate'; target: string; search: string | string[]; replace: string | string[] }) => void
  onCancelAction?: () => void
  initial?: { type: 'replace'|'translate'; target: string; search: string | string[]; replace: string | string[] }
  submitLabel?: string
}

export default function AdvancedSqlReplaceBuilder({ columns, onAddAction, onCancelAction, initial, submitLabel }: AdvancedSqlReplaceBuilderProps) {
  const [target, setTarget] = useState<string>(columns[0] || '')
  const [mode, setMode] = useState<'replace'|'translate'>('replace')
  const [search, setSearch] = useState<string>('')
  const [replace, setReplace] = useState<string>('')
  const [multi, setMulti] = useState<boolean>(false)

  // Prefill for edit mode
  useEffect(() => {
    if (!initial) return
    try {
      setTarget(String(initial.target || columns[0] || ''))
      setMode((initial.type as any) || 'replace')
      const s = (initial.search as any)
      const r = (initial.replace as any)
      if (Array.isArray(s)) { setMulti(true); setSearch(s.join(',')) } else { setMulti(false); setSearch(String(s ?? '')) }
      if (Array.isArray(r)) { setReplace(r.join(',')) } else { setReplace(String(r ?? '')) }
    } catch {}
  }, [initial, columns])

  const canAdd = useMemo(() => !!target && (search !== ''), [target, search])

  function buildPayload() {
    if (mode === 'translate') {
      return { type: 'translate' as const, target, search, replace }
    }
    if (multi) {
      const s = search.split(',').map(s=>s.trim()).filter(Boolean)
      const r = replace.split(',').map(s=>s.trim()).filter(()=>true)
      return { type: 'replace' as const, target, search: s, replace: r.length ? r : [''] }
    }
    return { type: 'replace' as const, target, search, replace }
  }

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
        <div className="sm:col-span-2 flex items-center gap-3">
          <label className="text-xs flex items-center gap-1"><input type="radio" checked={mode==='replace'} onChange={()=>setMode('replace')} /> replace</label>
          <label className="text-xs flex items-center gap-1"><input type="radio" checked={mode==='translate'} onChange={()=>setMode('translate')} /> translate</label>
        </div>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-2">
        <label className="text-xs text-muted-foreground sm:col-span-1">Search</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={search} onChange={(e)=>setSearch(e.target.value)} placeholder={mode==='replace' && multi ? 'comma-separated' : ''} />
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center mb-3">
        <label className="text-xs text-muted-foreground sm:col-span-1">Replace</label>
        <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={replace} onChange={(e)=>setReplace(e.target.value)} placeholder={mode==='replace' && multi ? 'comma-separated' : ''} />
      </div>
      {mode==='replace' && (
        <div className="flex items-center gap-2 mb-3">
          <label className="text-xs flex items-center gap-1"><input type="checkbox" checked={multi} onChange={(e)=>setMulti(e.target.checked)} /> multi (comma-separated)</label>
        </div>
      )}
      <div className="flex items-center justify-end gap-2">
        <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCancelAction}>Cancel</button>
        <button className={`text-xs px-2 py-1 rounded-md border ${canAdd? 'bg-[hsl(var(--btn3))] text-black':'opacity-60 cursor-not-allowed'}`} disabled={!canAdd} onClick={()=>{
          onAddAction(buildPayload())
        }}>{submitLabel || `Add ${mode} transform`}</button>
      </div>
    </div>
  )
}
