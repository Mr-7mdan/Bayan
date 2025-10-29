"use client"

import { useEffect, useMemo, useState, Fragment } from 'react'
import { Api, type DatasourceOut, type IntrospectResponse, type LocalStatsResponse, type SyncTaskOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import TablePreviewDialog from '@/components/builder/TablePreviewDialog'
import AdvancedSqlDialog from '@/components/builder/AdvancedSqlDialog'
import type { IntrospectResponse as IR } from '@/lib/api'
import { Select, SelectItem } from '@tremor/react'

type Row = {
  table: string
  rowCount?: number | null
  lastSyncAt?: string | null
  datasourceId: string
  datasourceName: string
  datasourceType: string
  sourceSchema?: string | null
  sourceTable?: string | null
}

export default function DataModelPage() {
  const { user } = useAuth()
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [rows, setRows] = useState<Row[]>([])
  const [filter, setFilter] = useState('')
  const [columnsByTable, setColumnsByTable] = useState<Record<string, Array<{ name: string; type?: string | null }>>>({})
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const [tasksByDs, setTasksByDs] = useState<Record<string, SyncTaskOut[]>>({})
  const [preview, setPreview] = useState<{ open: boolean; table?: string }>({ open: false })
  const [adv, setAdv] = useState<{ open: boolean; dsId?: string; dsType?: string; source?: string; schema?: IR }>(() => ({ open: false }))
  const [dropping, setDropping] = useState<string | null>(null)
  const [pageSize, setPageSize] = useState(8)
  const [page, setPage] = useState(0)

  useEffect(() => {
    let stop = false
    ;(async () => {
      setLoading(true); setError(null)
      try {
        const isAdmin = (user?.role === 'admin')
        const datasources = await Api.listDatasources(isAdmin ? undefined : user?.id, user?.id)
        if (stop) return
        const dsMap: Record<string, DatasourceOut> = {}
        datasources.forEach((d) => { dsMap[d.id] = d })
        const statsList = await Promise.all(datasources.map(async (d) => {
          try { return await Api.getLocalStats(d.id) } catch { return null as unknown as LocalStatsResponse | null }
        }))
        if (stop) return
        const agg: Row[] = []
        statsList.forEach((ls, i) => {
          if (!ls) return
          const ds = datasources[i]
          for (const t of (ls.tables || [])) {
            agg.push({
              table: t.table,
              rowCount: t.rowCount ?? null,
              lastSyncAt: t.lastSyncAt || null,
              datasourceId: ds.id,
              datasourceName: ds.name,
              datasourceType: ds.type,
              sourceSchema: t.sourceSchema || null,
              sourceTable: t.sourceTable || null,
            })
          }
        })
        setRows(agg)
        try {
          const schemaLocal = await Api.introspectLocal()
          if (!stop && schemaLocal) {
            const map: Record<string, Array<{ name: string; type?: string | null }>> = {}
            ;(schemaLocal.schemas || []).forEach((s) => {
              ;(s.tables || []).forEach((t) => { map[t.name] = t.columns || [] })
            })
            setColumnsByTable(map)
          }
        } catch {}
        const taskLists = await Promise.all(datasources.map(async (d) => {
          try { return await Api.getSyncStatus(d.id, user?.id) } catch { return [] as SyncTaskOut[] }
        }))
        if (stop) return
        const tmap: Record<string, SyncTaskOut[]> = {}
        datasources.forEach((d, i) => { tmap[d.id] = taskLists[i] || [] })
        setTasksByDs(tmap)
      } catch (e: any) {
        if (!stop) setError(String(e?.message || 'Failed to load Data Model'))
      } finally {
        if (!stop) setLoading(false)
      }
    })()
    return () => { stop = true }
  }, [user?.id])

  const filtered = useMemo(() => {
    const q = (filter || '').trim().toLowerCase()
    if (!q) return rows
    return rows.filter((r) => (
      r.table.toLowerCase().includes(q) ||
      r.datasourceName.toLowerCase().includes(q) ||
      (r.sourceTable || '').toLowerCase().includes(q)
    ))
  }, [rows, filter])
  const totalPages = Math.max(1, Math.ceil((filtered.length || 0) / pageSize))
  const visible = useMemo(() => filtered.slice(page * pageSize, page * pageSize + pageSize), [filtered, page, pageSize])
  useEffect(() => { setPage(0) }, [filter, pageSize])

  return (
    <div className="p-4 space-y-4">
      <div>
        <h1 className="text-base font-medium">Data Model</h1>
        <div className="text-xs text-muted-foreground">Manage local DuckDB tables, view columns, preview data, and delete tables. Create custom columns and joins via SQL Advanced.</div>
      </div>

      <div className="flex items-center py-2 gap-2">
        <div className="flex items-center gap-2">
          <label htmlFor="searchDataModel" className="text-sm mr-2 text-gray-600 dark:text-gray-300">Search</label>
          <input id="searchDataModel" value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Search tables..." className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]" />
        </div>
        <div className="ml-auto flex items-center gap-2 text-sm shrink-0">
          <span className="whitespace-nowrap min-w-[84px]">Per page</span>
          <div className="min-w-[96px] rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
            [&_*]:!border-0 [&_*]:!border-transparent [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!ring-transparent [&_*]:!outline-none [&_*]:!shadow-none
            [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
            <Select
              value={String(pageSize)}
              onValueChange={(v) => setPageSize(parseInt(v || '8') || 8)}
              className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
            >
              <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="6">6</SelectItem>
              <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="8">8</SelectItem>
              <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="12">12</SelectItem>
              <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="24">24</SelectItem>
            </Select>
          </div>
        </div>
      </div>

      <div className="overflow-auto rounded-xl border-2 border-[hsl(var(--border))]">
        <table className="min-w-full text-sm">
          <thead className="bg-[hsl(var(--card))] border-b border-[hsl(var(--border))]">
            <tr>
              <th className="text-left px-3 py-2 font-medium">Table</th>
              <th className="text-left px-3 py-2 font-medium">Records</th>
              <th className="text-left px-3 py-2 font-medium">Last Synced</th>
              <th className="text-left px-3 py-2 font-medium">Source Datasource</th>
              <th className="text-left px-3 py-2 font-medium">Next Sync</th>
              <th className="text-left px-3 py-2 font-medium">Actions</th>
            </tr>
          </thead>
          <tbody className="bg-[hsl(var(--background))]">
            {loading && (
              <tr><td className="px-3 py-3 text-muted-foreground" colSpan={6}>Loading…</td></tr>
            )}
            {error && !loading && (
              <tr><td className="px-3 py-3 text-red-600" colSpan={6}>{error}</td></tr>
            )}
            {!loading && !error && filtered.length === 0 && (
              <tr><td className="px-3 py-3 text-muted-foreground" colSpan={6}>No tables.</td></tr>
            )}
            {!loading && !error && visible.map((r) => {
              const tasks = tasksByDs[r.datasourceId] || []
              const task = tasks.find((t) => String(t.destTableName || '') === r.table)
              const nextSync = task?.scheduleCron || null
              const cols = columnsByTable[r.table] || []
              const isExp = !!expanded[r.table]
              return (
                <Fragment key={`${r.datasourceId}:${r.table}`}>
                  <tr key={`${r.datasourceId}:${r.table}`} className="border-t border-[hsl(var(--border))]">
                    <td className="px-3 py-2">
                      <button className="font-mono hover:underline" onClick={() => setExpanded((m) => ({ ...m, [r.table]: !m[r.table] }))}>{r.table}</button>
                    </td>
                    <td className="px-3 py-2">{typeof r.rowCount === 'number' ? r.rowCount.toLocaleString() : '—'}</td>
                    <td className="px-3 py-2">{r.lastSyncAt ? new Date(r.lastSyncAt).toLocaleString() : '—'}</td>
                    <td className="px-3 py-2">{r.datasourceName}</td>
                    <td className="px-3 py-2">{nextSync ? nextSync : '—'}</td>
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-2">
                        <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={() => setPreview({ open: true, table: r.table })}>View</button>
                        <button
                          className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]"
                          onClick={() => {
                            const schemaOne: IR = { schemas: [{ name: 'main', tables: [{ name: r.table, columns: cols }] }] }
                            setAdv({ open: true, dsId: r.datasourceId, dsType: r.datasourceType, source: r.table, schema: schemaOne })
                          }}
                        >Advanced SQL</button>
                        {dropping === `${r.datasourceId}:${r.table}` ? (
                          <>
                            <span className="text-xs text-muted-foreground">Drop?</span>
                            <button
                              className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--danger))/0.12] text-[hsl(var(--danger))]"
                              onClick={async () => {
                                try {
                                  setDropping(null)
                                  await Api.dropLocalTable(r.datasourceId, r.table, user?.id)
                                  setRows((arr) => arr.filter((x) => !(x.datasourceId === r.datasourceId && x.table === r.table)))
                                } catch {}
                              }}
                            >Confirm</button>
                            <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={() => setDropping(null)}>Cancel</button>
                          </>
                        ) : (
                          <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--danger))/0.12] text-[hsl(var(--danger))]" onClick={() => setDropping(`${r.datasourceId}:${r.table}`)}>Delete</button>
                        )}
                      </div>
                    </td>
                  </tr>
                  {isExp && (
                    <tr key={`${r.datasourceId}:${r.table}#exp`} className="border-t border-[hsl(var(--border))] bg-[hsl(var(--muted))]/40">
                      <td className="px-3 py-2" colSpan={6}>
                        <div className="text-[11px]">Columns</div>
                        <div className="mt-1 overflow-x-auto">
                          <table className="w-full table-fixed text-[11px]">
                            <tbody>
                              {cols.length === 0 ? (
                                <tr><td className="px-2 py-1 text-muted-foreground">No columns</td></tr>
                              ) : cols.map((c) => (
                                <tr key={c.name} className="align-top">
                                  <td className="w-[220px] px-2 py-0.5"><span className="inline-flex px-1.5 py-0.5 rounded bg-card font-mono">{c.name}</span></td>
                                  <td className="px-2 py-0.5 text-muted-foreground">{c.type || ''}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>
              )
            })}
          </tbody>
        </table>
      </div>
      {!loading && filtered.length > 0 && (
        <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
          <span>Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, filtered.length)} of {filtered.length}</span>
          <div className="flex items-center gap-2">
            <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={page <= 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>Prev</button>
            <span>Page {page + 1} / {totalPages}</span>
            <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={page >= totalPages - 1} onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}>Next</button>
          </div>
        </div>
      )}

      {preview.open && preview.table && (
        <TablePreviewDialog open={preview.open} onOpenChangeAction={(o) => setPreview({ open: o })} table={preview.table} limit={100} />
      )}

      {adv.open && adv.dsId && (
        <AdvancedSqlDialog open={adv.open} onCloseAction={() => setAdv({ open: false })} datasourceId={adv.dsId} dsType={adv.dsType} source={adv.source} schema={adv.schema}
        />
      )}
    </div>
  )
}
