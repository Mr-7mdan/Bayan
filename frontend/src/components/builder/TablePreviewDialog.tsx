"use client"

import { useEffect, useState } from 'react'
import { createPortal } from 'react-dom'
import { Api, QueryApi } from '@/lib/api'

export default function TablePreviewDialog({
  open,
  onOpenChangeAction,
  datasourceId,
  table,
  limit = 1000,
}: {
  open: boolean
  onOpenChangeAction: (open: boolean) => void
  datasourceId?: string
  table: string
  limit?: number
}) {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [columns, setColumns] = useState<string[]>([])
  const [rows, setRows] = useState<any[][]>([])
  const [offset, setOffset] = useState(0)
  const [total, setTotal] = useState<number | null>(null)

  useEffect(() => {
    let cancelled = false
    async function fetchPreview() {
      if (!open || !table) return
      setLoading(true)
      setError(null)
      try {
        const max = Math.min(Math.max(1, limit || 1000), 1000)
        const base = String(table)
        const esc = base.replace(/"/g, '""')
        const forms = [
          `"${esc}"`,
          base, // bare
          `main."${esc}"`,
        ]
        let done = false
        let lastErr: any = null
        for (const form of forms) {
          try {
            const sql = `SELECT * FROM ${form}`
            const res = await Api.query({ sql, datasourceId, limit: max, offset, includeTotal: true, preferLocalDuck: true, preferLocalTable: table })
            if (cancelled) return
            const cols = (res.columns || []) as string[]
            const rows = (res.rows || []) as any[][]
            const tot = typeof (res as any)?.totalRows === 'number' ? (res as any).totalRows as number : null
            if ((cols && cols.length) || (rows && rows.length) || (typeof tot === 'number' && tot > 0)) {
              setColumns(cols)
              setRows(rows)
              setTotal(tot)
              done = true
              break
            }
            // No data; try next form
          } catch (err: any) {
            lastErr = err
            continue
          }
        }
        if (!done) {
          // Fallback to /query/spec path
          try {
            const spec = { source: table, select: ['*'], limit: max, offset } as any
            const res2 = await QueryApi.querySpec({ spec, datasourceId, limit: max, offset, includeTotal: true, preferLocalDuck: true })
            if (cancelled) return
            setColumns((res2.columns || []) as string[])
            setRows((res2.rows || []) as any[][])
            setTotal(typeof (res2 as any)?.totalRows === 'number' ? (res2 as any).totalRows as number : null)
            done = true
          } catch (err2: any) {
            lastErr = lastErr || err2
          }
        }
        if (!done) {
          throw lastErr || new Error('No rows')
        }
      } catch (e: any) {
        if (cancelled) return
        setError(String(e?.message || 'Failed to load preview'))
      } finally {
        if (!cancelled) setLoading(false)
      }
    }
    fetchPreview()
    return () => { cancelled = true }
  }, [open, datasourceId, table, limit, offset])

  useEffect(() => {
    if (!open) setOffset(0)
  }, [open, table])

  if (!open || typeof window === 'undefined') return null

  return createPortal(
    <div className="fixed inset-0 z-[100] flex items-center justify-center bg-black/40" onClick={() => onOpenChangeAction(false)}>
      <div className="max-w-[92vw] max-h-[86vh] w-[960px] bg-card border rounded-md shadow flex flex-col overflow-hidden" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between px-3 py-2 border-b">
          <div className="text-sm font-medium">Preview — {table} <span className="text-xs text-muted-foreground">(limit {Math.min(limit || 1000, 1000)} offset {offset}{typeof total === 'number' ? ` of ${total}` : ''})</span></div>
          <div className="flex items-center gap-2">
            <button
              className="text-xs px-2 py-1 rounded-md border hover:bg-muted disabled:opacity-50"
              disabled={loading || offset <= 0}
              onClick={() => setOffset((o) => Math.max(0, o - Math.min(limit || 1000, 1000)))}
            >Prev</button>
            <button
              className="text-xs px-2 py-1 rounded-md border hover:bg-muted disabled:opacity-50"
              disabled={loading || (typeof total === 'number' ? (offset + Math.min(limit || 1000, 1000) >= total) : rows.length < Math.min(limit || 1000, 1000))}
              onClick={() => setOffset((o) => o + Math.min(limit || 1000, 1000))}
            >Next</button>
            <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => onOpenChangeAction(false)}>Close</button>
          </div>
        </div>
        <div className="p-3 text-xs flex-1 min-h-0 overflow-auto max-w-full">
          {loading && <div className="text-muted-foreground">Loading…</div>}
          {error && <div className="text-red-600">{error}</div>}
          {!loading && !error && (
            rows.length === 0 ? (
              <div className="text-muted-foreground">No rows.</div>
            ) : (
              <div className="border rounded overflow-x-auto">
                <table className="min-w-full text-[11px]">
                  <thead className="sticky top-0 bg-[hsl(var(--muted))]">
                    <tr>
                      {columns.map((c) => (
                        <th key={c} className="text-left font-medium px-2 py-1 whitespace-nowrap">{c}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((r, i) => (
                      <tr key={i} className="border-t hover:bg-muted/40">
                        {columns.map((c, j) => (
                          <td key={c+':'+j} className="px-2 py-1 align-top whitespace-nowrap max-w-[300px] overflow-hidden text-ellipsis" title={r[j] != null ? String(r[j]) : ''}>
                            {formatCell(r[j])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          )}
        </div>
      </div>
    </div>,
    document.body
  )
}

function formatCell(v: any): string {
  if (v == null) return ''
  if (typeof v === 'number') return String(v)
  if (typeof v === 'boolean') return v ? 'true' : 'false'
  if (v instanceof Date) return v.toISOString()
  return String(v)
}
