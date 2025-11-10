"use client"

import { useEffect, useMemo, useState, Fragment } from 'react'
import { Api, type DatasourceOut, type IntrospectResponse, type LocalStatsResponse, type SyncTaskOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import TablePreviewDialog from '@/components/builder/TablePreviewDialog'
import AdvancedSqlDialog from '@/components/builder/AdvancedSqlDialog'
import type { IntrospectResponse as IR } from '@/lib/api'
import { Select, SelectItem, Card, TextInput } from '@tremor/react'
import * as Dialog from '@radix-ui/react-dialog'
import { RiEditBoxLine } from '@remixicon/react'

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
  // Table rename state
  const [renamingTable, setRenamingTable] = useState<{ dsId: string; oldName: string; newName: string } | null>(null)
  const [users, setUsers] = useState<Array<{ id: string; username?: string; email?: string }>>([])
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null)
  const [customColumnsByDs, setCustomColumnsByDs] = useState<Record<string, Array<{ name: string; type: string; scope?: string | { level: string; table?: string | null; widgetId?: string | null }; formula?: string }>>>({})

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
        // Load users for owner display
        try {
          const usersList = await Api.listUsers(user?.id)
          if (!stop) setUsers(usersList || [])
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
        // Don't clear customColumnsByDs - just update the specific datasource
        if (!selectedDuckId) return
        if (datasources.length === 0) return
        const schemaSel = await Api.introspect(selectedDuckId)
        if (stop) return
        const map: Record<string, Array<{ name: string; type?: string | null }>> = {}
        ;(schemaSel.schemas || []).forEach((s) => {
          ;(s.tables || []).forEach((t) => { map[t.name] = t.columns || [] })
        })
        setColumnsByTable(map)
        // Fetch custom columns (transforms) for all datasources
        try {
          const allDucks = datasources.filter((d) => String(d.type||'').toLowerCase().includes('duckdb'))
          if (allDucks.length === 0) return  // No DuckDB datasources
          
          // Fetch for all DuckDB datasources (will update state even if already loaded)
          const ducksToFetch = allDucks
          if (ducksToFetch.length === 0) return
          
          const allTransforms = await Promise.all(
            ducksToFetch.map(async (d) => {
              const t = await Api.getDatasourceTransforms(d.id)
              return { dsId: d.id, transforms: t }
            })
          )
          
          if (!stop) {
            // Store transforms for all datasources
            const transformsMap: Record<string, any[]> = {}
            allTransforms.forEach(({ dsId, transforms }) => {
              const allCustomCols = [
                ...(transforms?.customColumns || []).map((c: any) => ({ 
                  name: c.name, 
                  type: 'custom',
                  scope: c.scope || 'datasource',
                  formula: c.formula || c.expression || c.expr || ''
                })),
                ...(transforms?.transforms || []).map((t: any) => ({ 
                  name: t.name, 
                  type: t.type || 'case',
                  scope: t.scope || 'datasource',
                  formula: t.formula || t.expr || t.expression || ''
                })),
                ...(transforms?.joins || []).map((j: any) => ({ 
                  name: `Join: ${j.rightTable || 'unknown'}`,
                  type: 'join',
                  scope: j.scope || 'datasource',
                  formula: `${j.leftKey} = ${j.rightKey}`
                }))
              ]
              transformsMap[dsId] = allCustomCols
            })
            
            setCustomColumnsByDs((prev) => ({ ...prev, ...transformsMap }))
          }
        } catch (err) {
          console.error('[DataModel] Failed to load custom columns:', err)
        }
      } catch (err) {
        console.error('[DataModel] Failed to load columns:', err)
      }
    })()
    return () => { stop = true }
  }, [selectedDuckId, datasources.length, datasources])

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
                <th className="px-2 py-1">Owner</th>
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
                      {(() => {
                        const owner = users.find(u => u.id === (d as any).userId)
                        return owner ? (owner.username || owner.email || 'Unknown') : '—'
                      })()}
                    </td>
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
        <table className="min-w-full text-sm table-fixed">
          <thead className="bg-[hsl(var(--card))] border-b border-[hsl(var(--border))]">
            <tr>
              <th className="text-left px-3 py-2 font-medium w-[35%]">Table</th>
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
                  <tr key={`${r.datasourceId}:${r.table}`} className="group border-t border-[hsl(var(--border))]">
                    <td className="px-3 py-2">
                      <div className="flex items-center gap-1.5 min-w-0 flex-1">
                        {renamingTable?.oldName === r.table && renamingTable?.dsId === r.datasourceId ? (
                          <input
                            autoFocus
                            className="font-mono px-1 py-0.5 text-sm border rounded bg-[hsl(var(--card))] w-full min-w-0 flex-1"
                            value={renamingTable.newName}
                            onChange={(e) => setRenamingTable({ ...renamingTable, newName: e.target.value })}
                            onKeyDown={async (e) => {
                              if (e.key === 'Enter') {
                                const oldName = r.table
                                const newName = renamingTable.newName
                                try {
                                  await Api.renameLocalTable(r.datasourceId, oldName, newName, user?.id)
                                  // Update rows state
                                  setRows((arr) => arr.map((x) => 
                                    (x.datasourceId === r.datasourceId && x.table === oldName) 
                                      ? { ...x, table: newName } 
                                      : x
                                  ))
                                  // Update columnsByTable state
                                  setColumnsByTable((prev) => {
                                    const cols = prev[oldName]
                                    if (!cols) return prev
                                    const next = { ...prev }
                                    delete next[oldName]
                                    next[newName] = cols
                                    return next
                                  })
                                  // Update expanded state
                                  setExpanded((prev) => {
                                    if (!prev[oldName]) return prev
                                    const next = { ...prev }
                                    delete next[oldName]
                                    next[newName] = true
                                    return next
                                  })
                                  setRenamingTable(null)
                                  setToast({ message: `Table renamed to "${newName}"`, type: 'success' })
                                  setTimeout(() => setToast(null), 3000)
                                } catch (err) {
                                  console.error('Failed to rename table:', err)
                                  setToast({ message: `Failed to rename table: ${(err as any)?.message || 'Unknown error'}`, type: 'error' })
                                  setTimeout(() => setToast(null), 4000)
                                }
                              } else if (e.key === 'Escape') {
                                setRenamingTable(null)
                              }
                            }}
                            onBlur={() => setRenamingTable(null)}
                          />
                        ) : (
                          <>
                            <button 
                              className="font-mono hover:underline" 
                              onClick={() => setExpanded((m) => ({ ...m, [r.table]: !m[r.table] }))}
                            >
                              {r.table}
                            </button>
                            <button
                              className="opacity-0 group-hover:opacity-60 hover:!opacity-100 transition-opacity p-0.5 hover:bg-[hsl(var(--muted))] rounded"
                              onClick={(e) => {
                                e.stopPropagation()
                                setRenamingTable({ dsId: r.datasourceId, oldName: r.table, newName: r.table })
                              }}
                              title="Rename table"
                            >
                              <RiEditBoxLine className="w-3.5 h-3.5" />
                            </button>
                          </>
                        )}
                      </div>
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
                        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                          {/* Regular Columns */}
                          <div>
                            <div className="text-[11px] font-medium mb-1">Columns</div>
                            <div className="overflow-x-auto">
                              <table className="w-full table-fixed text-[11px]">
                                <tbody>
                                  {cols.length === 0 ? (
                                    <tr><td className="px-2 py-1 text-muted-foreground">No columns</td></tr>
                                  ) : cols.map((c) => (
                                    <tr key={c.name} className="align-top">
                                      <td className="w-[180px] px-2 py-0.5"><span className="inline-flex px-1.5 py-0.5 rounded bg-card font-mono">{c.name}</span></td>
                                      <td className="px-2 py-0.5 text-muted-foreground">{c.type || ''}</td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </div>
                          
                          {/* Custom Columns */}
                          <div>
                            <div className="flex items-center justify-between mb-1">
                              <div className="text-[11px] font-medium">Custom Columns & Transforms</div>
                              <button 
                                className="text-[10px] px-1.5 py-0.5 rounded border hover:bg-[hsl(var(--muted))]"
                                onClick={() => {
                                  const schemaOne: IR = { schemas: [{ name: 'main', tables: [{ name: r.table, columns: cols }] }] }
                                  setAdv({ open: true, dsId: r.datasourceId, dsType: r.datasourceType, source: r.table, schema: schemaOne })
                                }}
                              >
                                Edit
                              </button>
                            </div>
                            <div className="overflow-x-auto">
                              {(() => {
                                const allCustomCols = customColumnsByDs[r.datasourceId] || []
                                // Filter to show: datasource-scoped OR scoped to this specific table
                                const customCols = allCustomCols.filter((cc) => {
                                  if (!cc.scope || cc.scope === 'datasource') return true
                                  // If scope is an object with level field
                                  if (typeof cc.scope === 'object') {
                                    if (cc.scope.level === 'datasource') return true
                                    if (cc.scope.level === 'table' && cc.scope.table === r.table) return true
                                  }
                                  return false
                                })
                                
                                if (customCols.length === 0) {
                                  return <div className="px-2 py-1 text-[11px] text-muted-foreground">No custom columns or transforms</div>
                                }
                                
                                // Check which custom columns are active (have all dependencies in this table)
                                const colNames = new Set(cols.map(c => c.name.toLowerCase()))
                                const colNamesOriginal = new Map(cols.map(c => [c.name.toLowerCase(), c.name]))
                                
                                // Comprehensive SQL keywords and functions to exclude
                                const sqlKeywords = new Set([
                                  'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not', 'in', 'null', 'true', 'false',
                                  'select', 'from', 'where', 'as', 'on', 'join', 'left', 'right', 'inner', 'outer', 'full',
                                  'group', 'by', 'order', 'having', 'limit', 'offset', 'distinct', 'count', 'sum', 'avg',
                                  'min', 'max', 'between', 'like', 'is', 'exists', 'all', 'any', 'some', 'union', 'intersect',
                                  'except', 'date', 'time', 'timestamp', 'interval', 'cast', 'extract', 'substring', 'trim',
                                  'upper', 'lower', 'coalesce', 'nullif', 'length', 'concat', 'replace', 'position',
                                  'year', 'month', 'day', 'hour', 'minute', 'second', 'now', 'current', 'values', 'row'
                                ])
                                
                                const customColsWithDeps = customCols.map(cc => {
                                  const formula = cc.formula || ''
                                  
                                  // Remove string literals first to avoid false positives
                                  // Remove single-quoted strings: 'text'
                                  let cleanFormula = formula.replace(/'[^']*'/g, ' ')
                                  
                                  const refs = new Set<string>()
                                  
                                  // Extract double-quoted identifiers (column names in DuckDB: "ColumnName")
                                  const quotedRegex = /"([^"]+)"/g
                                  let match
                                  while ((match = quotedRegex.exec(cleanFormula)) !== null) {
                                    refs.add(match[1].toLowerCase())
                                  }
                                  
                                  // Extract dotted identifiers (s.ColumnName, table.Column)
                                  const dottedRegex = /(\w+)\.([a-zA-Z_][a-zA-Z0-9_]*)/g
                                  while ((match = dottedRegex.exec(cleanFormula)) !== null) {
                                    // match[2] is the column name after the dot
                                    refs.add(match[2].toLowerCase())
                                  }
                                  
                                  // Extract unquoted identifiers (but not if they were part of dotted notation)
                                  // First, remove dotted patterns from cleanFormula to avoid double-counting
                                  let noDottedFormula = cleanFormula.replace(/\b\w+\.([a-zA-Z_][a-zA-Z0-9_]*)\b/g, ' ')
                                  const unquotedRegex = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g
                                  while ((match = unquotedRegex.exec(noDottedFormula)) !== null) {
                                    const ref = match[1].toLowerCase()
                                    // Filter out SQL keywords and single-letter aliases (like s, t, etc.)
                                    if (!sqlKeywords.has(ref) && ref.length > 1) {
                                      refs.add(ref)
                                    }
                                  }
                                  
                                  // Filter to only actual table columns
                                  const deps = Array.from(refs).filter(ref => colNames.has(ref))
                                  const missingDeps = Array.from(refs).filter(ref => !colNames.has(ref) && !sqlKeywords.has(ref))
                                  
                                  // A custom column is active if:
                                  // 1. We found at least one column reference AND all of them exist in the table
                                  // 2. OR it's a join (no column dependencies, just table joins)
                                  const isJoin = cc.type === 'join'
                                  const hasColumnRefs = refs.size > 0
                                  const allDepsExist = hasColumnRefs && missingDeps.length === 0 && deps.length > 0
                                  const isActive = isJoin || allDepsExist
                                  
                                  return {
                                    ...cc,
                                    deps: deps.map(d => colNamesOriginal.get(d) || d),
                                    missingDeps: missingDeps,
                                    isActive
                                  }
                                })
                                
                                const active = customColsWithDeps.filter(cc => cc.isActive)
                                const inactive = customColsWithDeps.filter(cc => !cc.isActive)
                                
                                return (
                                  <div className="space-y-2">
                                    {active.length > 0 && (
                                      <div>
                                        <div className="text-[10px] font-semibold text-emerald-600 dark:text-emerald-400 mb-1">Active</div>
                                        <table className="w-full table-fixed text-[11px]">
                                          <tbody>
                                            {active.map((cc, idx) => (
                                              <tr key={idx} className="align-top">
                                                <td className="w-[180px] px-2 py-0.5">
                                                  <div className="flex flex-col gap-0.5">
                                                    <span className="inline-flex px-1.5 py-0.5 rounded bg-emerald-50 dark:bg-emerald-950 border border-emerald-200 dark:border-emerald-800 font-mono text-emerald-700 dark:text-emerald-300">
                                                      {cc.name}
                                                    </span>
                                                    {cc.deps && cc.deps.length > 0 && (
                                                      <div className="text-[9px] text-gray-500 dark:text-gray-400 pl-1">
                                                        {cc.deps.map((dep: string, i: number) => (
                                                          <span key={i} className="inline-flex items-center gap-0.5 mr-1">
                                                            <span className="text-emerald-600 dark:text-emerald-400">✓</span>
                                                            <span className="font-mono">{dep}</span>
                                                          </span>
                                                        ))}
                                                      </div>
                                                    )}
                                                  </div>
                                                </td>
                                                <td className="px-2 py-0.5 text-muted-foreground">
                                                  {cc.type.toUpperCase()} {(() => {
                                                    if (!cc.scope) return ''
                                                    if (typeof cc.scope === 'string') return `(${cc.scope})`
                                                    if (typeof cc.scope === 'object') {
                                                      if (cc.scope.level === 'table') {
                                                        return `(table: ${cc.scope.table || 'unknown'})`
                                                      }
                                                      return `(${cc.scope.level || 'datasource'})`
                                                    }
                                                    return ''
                                                  })()}
                                                </td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    )}
                                    
                                    {inactive.length > 0 && (
                                      <div>
                                        <div className="text-[10px] font-semibold text-gray-400 dark:text-gray-600 mb-1">Inactive (missing dependencies)</div>
                                        <table className="w-full table-fixed text-[11px]">
                                          <tbody>
                                            {inactive.map((cc, idx) => (
                                              <tr key={idx} className="align-top opacity-50">
                                                <td className="w-[180px] px-2 py-0.5">
                                                  <div className="flex flex-col gap-0.5">
                                                    <span className="inline-flex px-1.5 py-0.5 rounded bg-gray-50 dark:bg-gray-900 border border-gray-200 dark:border-gray-800 font-mono text-gray-500 dark:text-gray-500">
                                                      {cc.name}
                                                    </span>
                                                    {cc.deps && cc.deps.length > 0 && (
                                                      <div className="text-[9px] text-gray-500 dark:text-gray-400 pl-1">
                                                        {cc.deps.map((dep: string, i: number) => (
                                                          <span key={i} className="inline-flex items-center gap-0.5 mr-1">
                                                            <span className="text-emerald-600 dark:text-emerald-400">✓</span>
                                                            <span className="font-mono">{dep}</span>
                                                          </span>
                                                        ))}
                                                      </div>
                                                    )}
                                                    {cc.missingDeps && cc.missingDeps.length > 0 && (
                                                      <div className="text-[9px] text-red-500 dark:text-red-400 pl-1">
                                                        {cc.missingDeps.map((dep: string, i: number) => (
                                                          <span key={i} className="inline-flex items-center gap-0.5 mr-1">
                                                            <span>✗</span>
                                                            <span className="font-mono">{dep}</span>
                                                          </span>
                                                        ))}
                                                      </div>
                                                    )}
                                                  </div>
                                                </td>
                                                <td className="px-2 py-0.5 text-muted-foreground">
                                                  {cc.type.toUpperCase()} {(() => {
                                                    if (!cc.scope) return ''
                                                    if (typeof cc.scope === 'string') return `(${cc.scope})`
                                                    if (typeof cc.scope === 'object') {
                                                      if (cc.scope.level === 'table') {
                                                        return `(table: ${cc.scope.table || 'unknown'})`
                                                      }
                                                      return `(${cc.scope.level || 'datasource'})`
                                                    }
                                                    return ''
                                                  })()}
                                                </td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    )}
                                  </div>
                                )
                              })()}
                            </div>
                          </div>
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
        <AdvancedSqlDialog 
          open={adv.open} 
          onCloseAction={async () => {
            setAdv({ open: false })
            // Refresh custom columns after closing Advanced SQL dialog
            if (selectedDuckId) {
              try {
                const transforms = await Api.getDatasourceTransforms(selectedDuckId)
                // Combine customColumns, transforms, and joins into a single list
                const allCustomCols = [
                  ...(transforms?.customColumns || []).map((c: any) => ({ 
                    name: c.name, 
                    type: 'custom',
                    scope: c.scope || 'datasource',
                    formula: c.formula || c.expression || c.expr || ''
                  })),
                  ...(transforms?.transforms || []).map((t: any) => ({ 
                    name: t.name, 
                    type: t.type || 'case',
                    scope: t.scope || 'datasource',
                    formula: t.formula || t.expr || t.expression || ''
                  })),
                  ...(transforms?.joins || []).map((j: any) => ({ 
                    name: `Join: ${j.rightTable || 'unknown'}`,
                    type: 'join',
                    scope: j.scope || 'datasource',
                    formula: `${j.leftKey} = ${j.rightKey}`
                  }))
                ]
                setCustomColumnsByDs((prev) => ({ ...prev, [selectedDuckId]: allCustomCols }))
              } catch (err) {
                console.error('[DataModel] Failed to refresh custom columns:', err)
              }
            }
          }} 
          datasourceId={adv.dsId} 
          dsType={adv.dsType} 
          source={adv.source} 
          schema={adv.schema}
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

      {/* Toast notification */}
      {toast && (
        <div className="fixed bottom-4 right-4 z-50 animate-in slide-in-from-bottom-2">
          <div 
            className={`px-4 py-3 rounded-lg shadow-lg border ${
              toast.type === 'success' 
                ? 'bg-emerald-50 dark:bg-emerald-950 border-emerald-200 dark:border-emerald-800 text-emerald-900 dark:text-emerald-100' 
                : 'bg-red-50 dark:bg-red-950 border-red-200 dark:border-red-800 text-red-900 dark:text-red-100'
            }`}
          >
            <div className="flex items-center gap-2">
              <span className="text-sm font-medium">{toast.message}</span>
              <button 
                onClick={() => setToast(null)} 
                className="ml-2 opacity-70 hover:opacity-100"
              >
                ✕
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
