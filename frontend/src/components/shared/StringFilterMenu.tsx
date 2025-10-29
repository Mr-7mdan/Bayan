"use client"

import { useEffect, useMemo, useState } from 'react'

export default function StringFilterMenu(props: any) {
  // typed as any to satisfy Next.js "serializable props" lint; this component is only used by Client Components
  const { field, values, selected, onChange, onClear } = props as {
    field: string
    values: string[]
    selected: string[]
    onChange: (next: string[]) => void
    onClear: () => void
  }
  // Defensive: coerce to arrays to avoid runtime errors if caller passes undefined/null
  const safeValues: string[] = Array.isArray(values) ? values : []
  const safeSelected: string[] = Array.isArray(selected) ? selected : []
  const [q, setQ] = useState('')
  const [debounced, setDebounced] = useState('')
  useEffect(() => {
    const t = setTimeout(() => setDebounced(q), 150)
    return () => clearTimeout(t)
  }, [q])

  const filtered = useMemo(() => {
    if (!debounced) return safeValues
    const d = debounced.toLowerCase()
    return safeValues.filter((v) => String(v).toLowerCase().includes(d))
  }, [safeValues, debounced])

  const allChecked = filtered.length > 0 && filtered.every((v) => safeSelected.includes(v))
  const someChecked = filtered.some((v) => safeSelected.includes(v)) && !allChecked
  const hasSelection = safeSelected.length > 0

  const toggle = (v: string) => {
    const exists = safeSelected.includes(v)
    const next = exists ? safeSelected.filter((x) => x !== v) : [...safeSelected, v]
    onChange(next)
  }

  // Semantics:
  // - All -> remove filter entirely (show all rows)
  // - None -> unselect everything (caller treats [] as undefined => remove filter)
  const selectAll = () => { onClear() }
  const clearAll = () => { onChange([]) }

  return (
    <div className="w-full min-w-[220px]">
      <div className="px-1 pb-1 flex items-center">
        <input
          type="text"
          placeholder={`Search ${field}`}
          className="w-full min-w-0 h-8 px-2 rounded-md border border-[hsl(var(--border))] text-[12px] bg-[hsl(var(--card))] text-gray-700 placeholder:text-[hsl(var(--muted-foreground))] focus:outline-none focus:ring-2 focus:ring-tremor-brand-muted transition-colors dark:bg-gray-900 dark:text-gray-300 dark:placeholder:text-gray-500"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />
      </div>
      <div className="px-1 pt-2 pb-1 flex items-center justify-end gap-2">
        <button
          className={`h-8 text-[11px] px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-700 hover:bg-[hsl(var(--muted))] focus:outline-none focus:ring-2 focus:ring-tremor-brand-muted transition-colors disabled:opacity-50 disabled:cursor-not-allowed dark:bg-gray-900 dark:text-gray-300 dark:hover:bg-gray-800 ${!hasSelection? 'opacity-60' : ''}`}
          onClick={selectAll}
          disabled={!hasSelection}
          title="Show all (remove filter)"
        >All</button>
        <button
          className={`h-8 text-[11px] px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-700 hover:bg-[hsl(var(--muted))] focus:outline-none focus:ring-2 focus:ring-tremor-brand-muted transition-colors disabled:opacity-50 disabled:cursor-not-allowed dark:bg-gray-900 dark:text-gray-300 dark:hover:bg-gray-800 ${!hasSelection? 'opacity-60' : ''}`}
          onClick={clearAll}
          disabled={!hasSelection}
          title="Unselect all"
        >None</button>
      </div>
      <div role="group" className="max-h-56 overflow-auto">
        {filtered.length === 0 ? (
          <div className="px-2 py-1 text-xs text-[hsl(var(--muted-foreground))]">No values</div>
        ) : filtered.map((v) => {
          const checked = safeSelected.includes(v)
          return (
            <label key={v} className="flex items-center gap-2 px-2 py-1 text-[12px] cursor-pointer hover:bg-[hsl(var(--muted))] rounded-md">
              <input
                type="checkbox"
                className="size-3"
                checked={checked}
                onChange={() => toggle(v)}
              />
              <span className="truncate max-w-[200px]" title={v}>{v}</span>
            </label>
          )
        })}
      </div>
      <div className="flex items-center justify-end gap-2 px-1 pt-2">
        <button className="h-8 text-[11px] px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-700 hover:bg-[hsl(var(--muted))] focus:outline-none focus:ring-2 focus:ring-tremor-brand-muted transition-colors dark:bg-gray-900 dark:text-gray-300 dark:hover:bg-gray-800" onClick={onClear}>Clear</button>
      </div>
    </div>
  )
}
