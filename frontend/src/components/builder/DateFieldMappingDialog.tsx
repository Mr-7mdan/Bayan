"use client"

import { useEffect, useMemo, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { Api } from '@/lib/api'
import * as SchemaCache from '@/lib/schemaCache'
import type { WidgetConfig } from '@/types/widgets'

export default function DateFieldMappingDialog({
  open,
  onOpenChangeAction,
  widgets,
  onApplyAction,
}: {
  open: boolean
  onOpenChangeAction: (open: boolean) => void
  widgets: Record<string, WidgetConfig>
  onApplyAction: (mapping: Record<string, string | undefined>) => void
}) {
  const rows = useMemo(() => Object.values(widgets).filter((w) => (
    w && (w.type === 'kpi' || w.type === 'chart' || w.type === 'table')
  )), [widgets])

  const [loading, setLoading] = useState(false)
  const [byWidgetOptions, setByWidgetOptions] = useState<Record<string, string[]>>({})
  const [selection, setSelection] = useState<Record<string, string | undefined>>({})

  // Helpers: normalize identifiers and resolve schema/table from a source string
  const stripIdent = (s?: string) => String(s || '').trim()
    .replace(/^\[|\]$/g, '')
    .replace(/^"|"$/g, '')
    .replace(/^`|`$/g, '')
    .replace(/^'|'$/g, '')
  const parseSource = (src?: string): { schema?: string; table?: string } => {
    try {
      if (!src) return {}
      // Remove alias: take first token before whitespace
      const base = String(src).trim().split(/\s+/)[0]
      // Split by dots not inside quotes/brackets
      const parts: string[] = []
      let buf = ''
      let inSq = false, inDq = false, inBq = false, inBr = false
      for (let i = 0; i < base.length; i++) {
        const ch = base[i]
        if (ch === "'" && !inDq && !inBq && !inBr) { inSq = !inSq; buf += ch; continue }
        if (ch === '"' && !inSq && !inBq && !inBr) { inDq = !inDq; buf += ch; continue }
        if (ch === '`' && !inSq && !inDq && !inBr) { inBq = !inBq; buf += ch; continue }
        if (ch === '[' && !inSq && !inDq && !inBq) { inBr = true; buf += ch; continue }
        if (ch === ']' && inBr) { inBr = false; buf += ch; continue }
        if (ch === '.' && !inSq && !inDq && !inBq && !inBr) { parts.push(buf); buf = ''; continue }
        buf += ch
      }
      if (buf) parts.push(buf)
      const table = stripIdent(parts.pop())
      const schema = parts.map(stripIdent).join('.') || undefined
      return { schema, table }
    } catch { return {} }
  }
  const isDateLike = (c: { name: string; type?: string | null }): boolean => {
    try {
      const t = String(c.type || '').toLowerCase()
      if (t && (t.includes('date') || t.includes('time'))) return true
      const n = String(c.name || '').toLowerCase()
      // DuckDB sometimes omits specific type info; use common name patterns as fallback
      if (/date|time|timestamp/.test(n)) return true
      if (/(^|_)created(_|$)|(^|_)updated(_|$)|(^|_)inserted(_|$)|(^|_)modified(_|$)/.test(n)) return true
      if (/(^|_)dt(_|$)|(^|_)ymd(_|$)/.test(n)) return true
      return false
    } catch { return false }
  }
  const resolveColumns = (schemaObj: any, targetSchema: string | undefined, targetTable: string | undefined): Array<{ name: string; type?: string|null }>|undefined => {
    try {
      const schemas: any[] = Array.isArray(schemaObj?.schemas) ? schemaObj.schemas : []
      const lcEq = (a?: string, b?: string) => String(a || '').toLowerCase() === String(b || '').toLowerCase()
      const findTable = (sch: any, tblName?: string) => (sch?.tables || []).find((t: any) => lcEq(t?.name, tblName))
      let tbl: any | undefined
      if (targetSchema) {
        const sch = schemas.find((s: any) => lcEq(s?.name, targetSchema))
        tbl = findTable(sch, targetTable)
      }
      if (!tbl && targetTable) {
        for (const sch of schemas) { tbl = findTable(sch, targetTable); if (tbl) break }
      }
      return (tbl?.columns || [])
    } catch { return undefined }
  }

  // Seed selection and load options when opened
  useEffect(() => {
    if (!open) return
    // Seed selection from current configs
    setSelection(() => {
      const init: Record<string, string | undefined> = {}
      rows.forEach((w) => { init[w.id] = w.options?.deltaDateField })
      return init
    })
    ;(async () => {
      setLoading(true)
      try {
        const cache: Record<string, any> = {}
        const map: Record<string, string[]> = {}
        for (const w of rows) {
          try {
            const source = (w.querySpec as any)?.source as string | undefined
            if (!source) { map[w.id] = []; continue }
            const dsKey = w.datasourceId || '__local__'
            // 1) Seed from SchemaCache if available
            if (!cache[dsKey]) {
              const seeded = SchemaCache.get(dsKey)
              if (seeded) cache[dsKey] = seeded
              else cache[dsKey] = undefined
            }
            // 2) If no cached schema yet, fetch immediately (blocking for first paint of this dsKey)
            if (!cache[dsKey]) {
              cache[dsKey] = w.datasourceId ? await Api.introspect(w.datasourceId) : await Api.introspectLocal()
              // persist
              try { SchemaCache.set(dsKey, cache[dsKey]) } catch {}
            } else {
              // 3) Refresh in background to keep cache fresh
              (async () => {
                try {
                  const fresh = w.datasourceId ? await Api.introspect(w.datasourceId) : await Api.introspectLocal()
                  SchemaCache.set(dsKey, fresh)
                  // If rows still open, update options for widgets of this dsKey
                  setByWidgetOptions((prev) => {
                    try {
                      const out = { ...prev }
                      rows.filter(r => (r.datasourceId || '__local__') === dsKey).forEach((rw) => {
                        const { schema: sch, table } = parseSource((rw.querySpec as any)?.source)
                        const cols = resolveColumns(fresh, sch, table) as Array<{ name: string; type?: string | null }>
                        const opts = (cols || []).filter(isDateLike).map((c) => c.name)
                        out[rw.id] = opts
                      })
                      return out
                    } catch { return prev }
                  })
                } catch {}
              })()
            }
            const schema = cache[dsKey]
            const { schema: sch, table } = parseSource(source)
            const cols = resolveColumns(schema, sch, table) as Array<{ name: string; type?: string | null }>
            const opts = (cols || []).filter(isDateLike).map((c) => c.name)
            map[w.id] = opts
          } catch {
            map[w.id] = []
          }
        }
        setByWidgetOptions(map)
      } finally {
        setLoading(false)
      }
    })()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChangeAction}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/20" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[720px] max-h-[80vh] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card overflow-auto">
          <Dialog.Title className="text-lg font-semibold">Map date fields for global filters</Dialog.Title>
          <Dialog.Description className="text-sm text-muted-foreground mb-3">
            Choose the date/datetime column for each widget. Global Start/End will restrict that column.
          </Dialog.Description>

          <div className="space-y-2">
            {rows.map((w) => {
              const opts = byWidgetOptions[w.id] || []
              const hasSpec = !!(w.querySpec as any)?.source
              return (
                <div key={w.id} className="grid grid-cols-12 items-center gap-2 border rounded-md p-2">
                  <div className="col-span-4">
                    <div className="text-sm font-medium truncate" title={`${w.title} (${w.type})`}>{w.title}</div>
                    <div className="text-[11px] text-muted-foreground">[{w.type}] {hasSpec ? (w.querySpec as any)?.source : 'SQL mode'}</div>
                  </div>
                  <div className="col-span-8">
                    {hasSpec ? (
                      <select
                        className="w-full px-2 py-1 rounded-md bg-[hsl(var(--secondary))] text-xs"
                        value={selection[w.id] || ''}
                        onChange={(e) => setSelection((prev) => ({ ...prev, [w.id]: e.target.value || undefined }))}
                      >
                        <option value="">Auto (detect)</option>
                        {opts.map((name) => (
                          <option key={name} value={name}>{name}</option>
                        ))}
                      </select>
                    ) : (
                      <input
                        className="w-full px-2 py-1 rounded-md border bg-[hsl(var(--card))] text-xs"
                        placeholder="Date column (for SQL mode)"
                        value={selection[w.id] || ''}
                        onChange={(e) => setSelection((prev) => ({ ...prev, [w.id]: e.target.value || undefined }))}
                      />
                    )}
                  </div>
                </div>
              )
            })}
            {rows.length === 0 && (
              <div className="text-sm text-muted-foreground">No widgets to configure.</div>
            )}
          </div>

          <div className="mt-3 flex items-center justify-end gap-2">
            <Dialog.Close asChild>
              <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">
                Cancel
              </button>
            </Dialog.Close>
            <button
              type="button"
              className="text-sm px-3 py-1.5 rounded-md border bg-[hsl(var(--btn3))] text-black"
              disabled={loading}
              onClick={() => { onApplyAction(selection); onOpenChangeAction(false) }}
            >
              {loading ? 'Loading…' : 'Apply'}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
