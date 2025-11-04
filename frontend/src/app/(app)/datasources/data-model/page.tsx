"use client"

import { useEffect, useMemo, useState, Fragment } from 'react'
import { Api, type DatasourceOut, type IntrospectResponse, type LocalStatsResponse, type SyncTaskOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import TablePreviewDialog from '@/components/builder/TablePreviewDialog'
import AdvancedSqlDialog from '@/components/builder/AdvancedSqlDialog'
import type { IntrospectResponse as IR } from '@/lib/api'
import { Select, SelectItem, Card, TextInput } from '@tremor/react'
import * as Dialog from '@radix-ui/react-dialog'

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
  const [datasources, setDatasources] = useState<DatasourceOut[]>([])
  const [rows, setRows] = useState<Row[]>([])
  const [filter, setFilter] = useState('')
  const [columnsByTable, setColumnsByTable] = useState<Record<string, Array<{ name: string; type?: string | null }>>>({})
  const [expanded, setExpanded] = useState<Record<string, boolean>>({})
  const [tasksByDs, setTasksByDs] = useState<Record<string, SyncTaskOut[]>>({})
  const [preview, setPreview] = useState<{ open: boolean; dsId?: string; table?: string }>({ open: false })
  const [adv, setAdv] = useState<{ open: boolean; dsId?: string; dsType?: string; source?: string; schema?: IR }>(() => ({ open: false }))
  const [pageSize, setPageSize] = useState(8)
  // Confirmation dialogs
  const [confirmDeleteTable, setConfirmDeleteTable] = useState<{ open: boolean; dsId?: string; table?: string }>({ open: false })
  const [confirmDeleteDuck, setConfirmDeleteDuck] = useState<{ open: boolean; duck?: DatasourceOut }>({ open: false })
  const [page, setPage] = useState(0)
  // Local DuckDB management
  const [defaultDsId, setDefaultDsId] = useState<string | null>(null)
  const [creating, setCreating] = useState(false)
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('Local DuckDB')
  const [activeDuckPath, setActiveDuckPath] = useState<string | null>(null)
  const [activeDuckId, setActiveDuckId] = useState<string | null>(null)
  // User-selected DuckDB for viewing tables (separate from default)
  const [viewingDuckId, setViewingDuckId] = useState<string | null>(null)

  // Keep defaultDsId in sync with localStorage and custom events
  useEffect(() => {
    try {
      if (typeof window !== 'undefined') {
        const read = () => { try { setDefaultDsId(localStorage.getItem('default_ds_id')) } catch { setDefaultDsId(null) } }
        read()
        const onStorage = (e: StorageEvent) => { if (e.key === 'default_ds_id') read() }
        const onCustom = () => read()
        window.addEventListener('storage', onStorage as EventListener)
        window.addEventListener('default-ds-change', onCustom as EventListener)
        return () => { window.removeEventListener('storage', onStorage as EventListener); window.removeEventListener('default-ds-change', onCustom as EventListener) }
      }
    } catch {}
    return () => {}
  }, [])

  useEffect(() => {
    let stop = false
    ;(async () => {
      setLoading(true); setError(null)
      try {
        const isAdmin = (user?.role === 'admin')
        const dsList = await Api.listDatasources(isAdmin ? undefined : user?.id, user?.id)
        if (stop) return
        setDatasources(dsList)
        const dsMap: Record<string, DatasourceOut> = {}
        dsList.forEach((d) => { dsMap[d.id] = d })
        const duckOnly = dsList.filter((d) => String(d.type||'').toLowerCase().includes('duckdb'))
        const statsList = await Promise.all(duckOnly.map(async (d) => {
          try { return await Api.getLocalStats(d.id) } catch { return null as unknown as LocalStatsResponse | null }
        }))
        if (stop) return
        const agg: Row[] = []
        statsList.forEach((ls, i) => {
          if (!ls) return
          const ds = duckOnly[i]
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
        const taskLists = await Promise.all(duckOnly.map(async (d) => {
          try { return await Api.getSyncStatus(d.id, user?.id) } catch { return [] as SyncTaskOut[] }
        }))
        if (stop) return
        const tmap: Record<string, SyncTaskOut[]> = {}
        duckOnly.forEach((d, i) => { tmap[d.id] = taskLists[i] || [] })
        setTasksByDs(tmap)
        // Load default ds id from localStorage (initial)
        try { if (typeof window !== 'undefined') setDefaultDsId((prev)=> prev ?? localStorage.getItem('default_ds_id')) } catch {}
        // Fetch current global active DuckDB (admin-only)
        try {
          const res = await Api.duckActiveGet(user?.id)
          const path = (res as any)?.path || null
          setActiveDuckPath(path)
          // Find which datasource matches this path
          if (path) {
            const matchingDs = duckOnly.find(d => {
              const uri = d.connectionUri || ''
              const extracted = uri.replace(/^duckdb:\/\/\//, '')
              return path.includes(extracted) || extracted.includes(path)
            })
            setActiveDuckId(matchingDs?.id || null)
          } else {
            setActiveDuckId(null)
          }
        } catch {}
      } catch (e: any) {
        if (!stop) setError(String(e?.message || 'Failed to load Data Model'))
      } finally {
        if (!stop) setLoading(false)
      }
    })()
    return () => { stop = true }
  }, [user?.id])

  // Determine which DuckDB is selected for the grid: user selection, default id if valid, else first DuckDB
  const selectedDuckId = useMemo(() => {
    const ducks = datasources.filter((d) => String(d.type||'').toLowerCase().includes('duckdb'))
    if (viewingDuckId && ducks.some((d) => d.id === viewingDuckId)) return viewingDuckId
    if (defaultDsId && ducks.some((d) => d.id === defaultDsId)) return defaultDsId
    return (ducks[0]?.id || null)
  }, [viewingDuckId, defaultDsId, datasources])

  // Keep columns listing in sync with selected DuckDB
  useEffect(() => {
    let stop = false
    ;(async () => {
      try {
        setColumnsByTable({})
        if (!selectedDuckId) return
        const schemaSel = await Api.introspect(selectedDuckId)
        if (stop) return
        const map: Record<string, Array<{ name: string; type?: string | null }>> = {}
        ;(schemaSel.schemas || []).forEach((s) => {
          ;(s.tables || []).forEach((t) => { map[t.name] = t.columns || [] })
        })
        setColumnsByTable(map)
      } catch (err) {
        console.error('[DataModel] Failed to load columns:', err)
      }
    })()
    return () => { stop = true }
  }, [selectedDuckId])

  // Base rows filtered by selected DuckDB
  const baseRows = useMemo(() => {
    if (!selectedDuckId) return rows
    return rows.filter((r) => r.datasourceId === selectedDuckId)
  }, [rows, selectedDuckId])

  const filtered = useMemo(() => {
    const q = (filter || '').trim().toLowerCase()
    if (!q) return baseRows
    return baseRows.filter((r) => (
      r.table.toLowerCase().includes(q) ||
      r.datasourceName.toLowerCase().includes(q) ||
      (r.sourceTable || '').toLowerCase().includes(q)
    ))
  }, [baseRows, filter])
  const totalPages = Math.max(1, Math.ceil((filtered.length || 0) / pageSize))
  const visible = useMemo(() => filtered.slice(page * pageSize, page * pageSize + pageSize), [filtered, page, pageSize])
  useEffect(() => { setPage(0) }, [filter, pageSize])

  return (
    <div className="p-4 space-y-4">
      <div>
        <h1 className="text-base font-medium">Data Model</h1>
        <div className="text-xs text-muted-foreground">Manage local DuckDB tables, view columns, preview data, and delete tables. Create custom columns and joins via SQL Advanced.</div>
      </div>

      {/* Local DuckDBs management */}
      <div className="rounded-xl border-2 border-[hsl(var(--border))] p-3">
        <div className="flex items-center justify-between">
          <div>
            <div className="text-sm font-medium">Local DuckDBs</div>
            <div className="text-xs text-muted-foreground">Create a new local DuckDB file, set default for new widgets, or delete old entries.</div>
          </div>
          <div className="flex items-center gap-2 text-xs">
            <button
              className="px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]"
              onClick={() => setCreateOpen(true)}
            >Create New DuckDB</button>
          </div>
        </div>
        <div className="mt-2 text-[11px] text-muted-foreground">
          <span>Active DuckDB for scheduled syncs:</span>
          <code className="ml-1 px-1 py-0.5 rounded bg-[hsl(var(--card))] border">{activeDuckPath || '-'}</code>
        </div>
        <div className="mt-2 overflow-x-auto">
          <table className="min-w-full text-xs">
            <thead>
              <tr className="text-left border-b border-[hsl(var(--border))]">
                <th className="px-2 py-1">Name</th>
                <th className="px-2 py-1">Type</th>
                <th className="px-2 py-1">Default (UI)</th>
                <th className="px-2 py-1">Actions</th>
              </tr>
            </thead>
            <tbody>
              {datasources.filter(d=>String(d.type||'').toLowerCase().includes('duckdb')).map((d)=>{
                const isDefault = defaultDsId && d.id === defaultDsId
                const isViewing = selectedDuckId === d.id
                const isActiveSync = activeDuckId === d.id
                return (
                  <tr 
                    key={d.id} 
                    className={`border-b border-[hsl(var(--border))] cursor-pointer transition-colors ${
                      isViewing 
                        ? 'bg-blue-50 dark:bg-blue-950/20 border-l-4 !border-l-blue-500' 
                        : 'hover:bg-gray-50 dark:hover:bg-gray-800/30 border-l-4 border-l-transparent'
                    }`}
                    onClick={() => setViewingDuckId(d.id)}
                    title="Click to view tables from this DuckDB"
                  >
                    <td className="px-2 py-1">
                      <div className="flex items-center gap-2">
                        {isActiveSync && (
                          <span 
                            className="inline-block w-2 h-2 rounded-full bg-emerald-500 flex-shrink-0" 
                            title="Active for scheduled syncs"
                          ></span>
                        )}
                        <span className={isViewing ? 'font-semibold' : ''}>{d.name}</span>
                        {isViewing && <span className="text-xs text-blue-600 dark:text-blue-400">(viewing)</span>}
                      </div>
                    </td>
                    <td className="px-2 py-1">{d.type}</td>
                    <td className="px-2 py-1">{isDefault ? 'Yes' : 'No'}</td>
                    <td className="px-2 py-1">
                      <div className="flex items-center gap-2">
                        {!isDefault && (
                          <button className="px-2 py-0.5 rounded border hover:bg-[hsl(var(--muted))]" onClick={(e)=>{
                            e.stopPropagation()
                            try { if (typeof window !== 'undefined') { localStorage.setItem('default_ds_id', d.id); window.dispatchEvent(new CustomEvent('default-ds-change')); setDefaultDsId(d.id) } } catch {}
                          }}>Make Default</button>
                        )}
                        {!isActiveSync && (
                          <button
                            className="px-2 py-0.5 rounded border hover:bg-[hsl(var(--muted))]"
                            title="Use this database for scheduled sync tasks (admin only)"
                            onClick={async (e) => {
                              e.stopPropagation()
                              try {
                                const res = await Api.duckActiveSet({ datasourceId: d.id }, user?.id)
                                const path = (res as any)?.path || null
                                setActiveDuckPath(path)
                                setActiveDuckId(d.id)
                              } catch (e) {
                                console.error('Set active failed', e)
                              }
                            }}
                          >Set Active (Sync)</button>
                        )}
                        <button 
                          className="px-2 py-0.5 rounded border hover:bg-[hsl(var(--danger))/0.12] text-[hsl(var(--danger))]" 
                          onClick={(e)=>{
                            e.stopPropagation()
                            setConfirmDeleteDuck({ open: true, duck: d })
                          }}
                        >Delete</button>
                      </div>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      <div className="mt-4 mb-2 flex items-center justify-between">
        <div className="text-sm font-medium">
          Tables from: <span className="text-blue-600 dark:text-blue-400">{datasources.find(d => d.id === selectedDuckId)?.name || 'No DuckDB selected'}</span>
        </div>
        <div className="text-xs text-muted-foreground">
          {baseRows.length} table{baseRows.length !== 1 ? 's' : ''}
        </div>
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
                        <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={() => setPreview({ open: true, dsId: r.datasourceId, table: r.table })}>View</button>
                        <button
                          className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]"
                          onClick={() => {
                            const schemaOne: IR = { schemas: [{ name: 'main', tables: [{ name: r.table, columns: cols }] }] }
                            setAdv({ open: true, dsId: r.datasourceId, dsType: r.datasourceType, source: r.table, schema: schemaOne })
                          }}
                        >Advanced SQL</button>
                        <button 
                          className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--danger))/0.12] text-[hsl(var(--danger))]" 
                          onClick={() => setConfirmDeleteTable({ open: true, dsId: r.datasourceId, table: r.table })}
                        >Delete</button>
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
        <TablePreviewDialog open={preview.open} onOpenChangeAction={(o) => setPreview({ open: o })} datasourceId={preview.dsId} table={preview.table} limit={100} />
      )}

      {adv.open && adv.dsId && (
        <AdvancedSqlDialog open={adv.open} onCloseAction={() => setAdv({ open: false })} datasourceId={adv.dsId} dsType={adv.dsType} source={adv.source} schema={adv.schema}
        />
      )}

      {/* Confirm Delete Table Dialog */}
      <Dialog.Root open={confirmDeleteTable.open} onOpenChange={(open) => setConfirmDeleteTable({ open })}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[80] bg-black/40" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[81] w-[440px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-[hsl(var(--card))] p-6 shadow-lg">
            <Dialog.Title className="text-lg font-semibold mb-2">Delete Table</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mb-4">
              Are you sure you want to delete the table <span className="font-mono font-semibold text-foreground">{confirmDeleteTable.table}</span>? This action cannot be undone.
            </Dialog.Description>
            <div className="flex items-center justify-end gap-2">
              <button 
                className="px-3 py-1.5 text-sm rounded-md border hover:bg-[hsl(var(--muted))]"
                onClick={() => setConfirmDeleteTable({ open: false })}
              >
                Cancel
              </button>
              <button 
                className="px-3 py-1.5 text-sm rounded-md border bg-red-600 text-white hover:bg-red-700"
                onClick={async () => {
                  try {
                    await Api.dropLocalTable(confirmDeleteTable.dsId!, confirmDeleteTable.table!, user?.id)
                    setRows((arr) => arr.filter((x) => !(x.datasourceId === confirmDeleteTable.dsId && x.table === confirmDeleteTable.table)))
                    setConfirmDeleteTable({ open: false })
                  } catch (err) {
                    console.error('Failed to delete table:', err)
                  }
                }}
              >
                Delete
              </button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {/* Confirm Delete DuckDB Dialog */}
      <Dialog.Root open={confirmDeleteDuck.open} onOpenChange={(open) => setConfirmDeleteDuck({ open })}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[80] bg-black/40" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[81] w-[440px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-[hsl(var(--card))] p-6 shadow-lg">
            <Dialog.Title className="text-lg font-semibold mb-2">Delete DuckDB</Dialog.Title>
            <Dialog.Description className="text-sm text-muted-foreground mb-4">
              Are you sure you want to delete the DuckDB datasource <span className="font-semibold text-foreground">{confirmDeleteDuck.duck?.name}</span>? All tables and data will be permanently deleted. This action cannot be undone.
            </Dialog.Description>
            <div className="flex items-center justify-end gap-2">
              <button 
                className="px-3 py-1.5 text-sm rounded-md border hover:bg-[hsl(var(--muted))]"
                onClick={() => setConfirmDeleteDuck({ open: false })}
              >
                Cancel
              </button>
              <button 
                className="px-3 py-1.5 text-sm rounded-md border bg-red-600 text-white hover:bg-red-700"
                onClick={async () => {
                  try {
                    const duckId = confirmDeleteDuck.duck?.id
                    if (!duckId) return
                    await Api.deleteDatasource(duckId)
                    setDatasources((prev) => prev.filter(x => x.id !== duckId))
                    if (defaultDsId === duckId && typeof window !== 'undefined') {
                      localStorage.removeItem('default_ds_id')
                      setDefaultDsId(null)
                      try { window.dispatchEvent(new CustomEvent('default-ds-change')) } catch {}
                    }
                    if (viewingDuckId === duckId) setViewingDuckId(null)
                    setConfirmDeleteDuck({ open: false })
                  } catch (err) {
                    console.error('Failed to delete DuckDB:', err)
                  }
                }}
              >
                Delete
              </button>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {/* Create New DuckDB Dialog */}
      {createOpen && (
        <div className="fixed inset-0 z-[80] flex items-center justify-center bg-black/40" onClick={() => setCreateOpen(false)}>
          <Card className="w-[440px] p-0" onClick={(e:any)=>e.stopPropagation()}>
            <div className="px-4 py-3 border-b">
              <div className="text-sm font-semibold">Create Local DuckDB</div>
            </div>
            <div className="p-4 space-y-3">
              <label className="text-xs">Name
                <TextInput className="mt-1" value={newName} onChange={(e:any)=>setNewName(e.target.value)} placeholder="Local DuckDB" />
              </label>
            </div>
            <div className="px-4 py-3 border-t flex items-center justify-end gap-2">
              <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={()=>{ setCreateOpen(false) }}>Cancel</button>
              <button
                type="button"
                className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]"
                disabled={creating || !(newName||'').trim()}
                onClick={async ()=>{
                  setCreating(true)
                  try {
                    const ts = new Date(); const y=ts.getFullYear(); const m=String(ts.getMonth()+1).padStart(2,'0'); const d=String(ts.getDate()).padStart(2,'0')
                    const hh=String(ts.getHours()).padStart(2,'0'); const mm=String(ts.getMinutes()).padStart(2,'0')
                    const safe = (newName||'Local DuckDB').replace(/[^A-Za-z0-9_.-]+/g,'-').slice(0,48)
                    const relPath = `.data/${safe.toLowerCase()}-${y}${m}${d}-${hh}${mm}.duckdb`
                    const dsn = `duckdb:///${relPath}`
                    const created = await Api.createDatasource({ name: (newName||'Local DuckDB').trim(), type: 'duckdb', connectionUri: dsn, userId: user?.id })
                    setDatasources((prev)=>[...prev, created])
                    setCreateOpen(false)
                    setNewName('Local DuckDB')
                  } catch (e) {
                    console.error('Create DS failed', e)
                  } finally {
                    setCreating(false)
                  }
                }}
              >{creating ? 'Creating…' : 'Create'}</button>
            </div>
          </Card>
        </div>
      )}
    </div>
  )
}
