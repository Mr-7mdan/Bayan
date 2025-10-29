"use client"

import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Api, DatasourceCreate, DatasourceOut, IntrospectResponse, type LocalStatsResponse } from '@/lib/api'
import { useRouter } from 'next/navigation'
import { useAuth } from '@/components/providers/AuthProvider'
import { RiDatabase2Line, RiTableLine, RiBracesLine, RiFocus2Line } from '@remixicon/react'
import TablePreviewDialog from '@/components/builder/TablePreviewDialog'
import * as SchemaCache from '@/lib/schemaCache'

function middleEllipsis(text: string, maxLen = 28): string {
  const s = String(text || '')
  if (!s) return ''
  if (s.length <= maxLen) return s
  const keep = Math.max(2, maxLen - 2)
  const head = s.slice(0, Math.ceil(keep * 0.6))
  const tail = s.slice(-Math.floor(keep * 0.4))
  return `${head}..${tail}`
}

function preferredOwners(dbType?: string | null): string[] {
  const t = String(dbType || '').toLowerCase()
  if (!t) return []
  if (t.includes('mssql') || t.includes('sqlserver')) return ['dbo']
  if (t.includes('postgres')) return ['public']
  if (t.includes('duckdb')) return ['main']
  if (t.includes('sqlite')) return ['main']
  return []
}

function isRoleSchema(name: string): boolean {
  const n = String(name || '').toLowerCase()
  if (!n) return false
  if (n.startsWith('db_')) return true
  if (n === 'information_schema' || n === 'sys' || n === 'guest' || n === 'pg_catalog') return true
  return false
}

export default function DataNavigator() {
  const qc = useQueryClient()
  const router = useRouter()
  const { user } = useAuth()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [confirmDrop, setConfirmDrop] = useState<string | null>(null)
  const [previewTable, setPreviewTable] = useState<string | null>(null)

  const listQ = useQuery<DatasourceOut[], Error>({ queryKey: ['datasources'], queryFn: () => Api.listDatasources(undefined, user?.id) })

  const initialSchema = selectedId ? (SchemaCache.get(selectedId) || undefined) : undefined
  const schemaQ = useQuery<IntrospectResponse, Error>({
    queryKey: ['ds-schema', selectedId],
    queryFn: ({ signal }) => Api.introspect(selectedId as string, signal),
    enabled: !!selectedId,
    retry: 0,
    refetchOnWindowFocus: false,
    staleTime: 5 * 60 * 1000,
    gcTime: 10 * 60 * 1000,
    // seed from cache for instant UI
    initialData: initialSchema,
  })

  // Persist to cache when fresh data arrives
  useEffect(() => {
    if (selectedId && schemaQ.data) {
      SchemaCache.set(selectedId, schemaQ.data)
    }
  }, [selectedId, schemaQ.data])

  const localStatsQ = useQuery<LocalStatsResponse, Error>({
    queryKey: ['ds-local-stats', selectedId],
    queryFn: () => Api.getLocalStats(selectedId as string),
    enabled: !!selectedId,
    refetchInterval: 6000,
  })

  const dropTable = useMutation({
    mutationFn: async (table: string) => {
      if (!selectedId) throw new Error('no datasource')
      return await Api.dropLocalTable(selectedId, table, user?.id)
    },
    onSuccess: async () => {
      setConfirmDrop(null)
      await qc.invalidateQueries({ queryKey: ['ds-local-stats', selectedId] })
    },
  })

  // Ensure stable array for rendering even while loading/undefined
  const dsList: DatasourceOut[] = Array.isArray(listQ.data) ? listQ.data : []

  const createDuck = useMutation({
    mutationFn: async () => {
      const payload: DatasourceCreate = {
        name: 'Local DuckDB',
        type: 'duckdb',
        userId: user?.id,
      }
      return Api.createDatasource(payload)
    },
    onSuccess: async (ds: DatasourceOut) => {
      await qc.invalidateQueries({ queryKey: ['datasources'] })
      setSelectedId(ds.id)
    },
  })

  const selectedDsType = (() => {
    try { return (dsList.find(d => d.id === selectedId)?.type || '').toLowerCase() } catch { return '' }
  })()

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-center justify-between gap-2">
        <h2 className="text-sm font-medium">Data Navigator</h2>
        <div className="flex gap-2">
          <button
            className="text-xs px-2 py-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))]"
            onClick={() => qc.invalidateQueries({ queryKey: ['datasources'] })}
            title="Refresh"
          >
            ⟳
          </button>
          <button
            className="text-xs px-2 py-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--foreground))] hover:bg-[hsl(var(--muted))]"
            disabled={createDuck.isPending}
            onClick={() => createDuck.mutate()}
            title="Create default local DuckDB datasource"
          >
            +DB
          </button>
        </div>
      </div>

      <div className="text-xs text-muted-foreground">
        {listQ.isLoading && !listQ.data && (
          <div className="space-y-2">
            <div className="h-4 bg-muted rounded animate-pulse" />
            <div className="h-4 bg-muted rounded animate-pulse w-4/5" />
            <div className="h-4 bg-muted rounded animate-pulse w-3/5" />
          </div>
        )}
        {listQ.error && <div className="text-red-600">Failed to load datasources</div>}
      </div>

      <ul className="text-sm space-y-1">
        {dsList.map((ds) => (
          <li key={ds.id}>
            <div className="relative">
              <button
                className={`w-full text-left px-2 py-2 rounded-md hover:bg-muted border-l-2 transition-colors pr-8 ${
                  selectedId === ds.id ? 'bg-secondary/60 border-l-[hsl(var(--header-accent))]' : 'border-l-transparent'
                }`}
                onClick={() => setSelectedId(ds.id)}
              >
                <div className="font-medium leading-tight">{ds.name}</div>
                <div className="text-xs text-muted-foreground leading-tight">{ds.type}</div>
              </button>
              {selectedId === ds.id && (
                <SchemaRefreshButton datasourceId={ds.id} onRefreshed={(data: IntrospectResponse) => { qc.setQueryData(['ds-schema', ds.id], data) }} />
              )}
            </div>
          </li>
        ))}
        {dsList.length === 0 && <li className="text-xs text-muted-foreground">No datasources yet.</li>}
      </ul>

      <div className="mt-2">
        {selectedId && (
          <div>
            <div className="text-xs font-medium mb-1">Schema</div>
            {schemaQ.isLoading && <div className="text-xs text-muted-foreground">Loading schema…</div>}
            {schemaQ.error && (
              <div className="text-xs text-red-600">Failed to introspect schema</div>
            )}
            <div className="max-h-64 overflow-y-auto overflow-x-hidden pr-1">
              {(() => {
                const owners = new Set(preferredOwners(selectedDsType))
                const schemas = (schemaQ.data?.schemas || []).filter((s) => !isRoleSchema(s.name) && (owners.size ? owners.has(s.name.toLowerCase()) : true))
                const tables = schemas.flatMap((sch) => sch.tables || [])
                return (
                  <ul className="p-2 space-y-1">
                    {tables.map((t) => (
                      <li key={t.name} className="text-sm">
                        <details className="rounded">
                          <summary className="flex items-center gap-2 px-1 py-0.5 hover:bg-muted/50 rounded cursor-pointer min-w-0">
                            <RiTableLine className="h-4 w-4 opacity-80" />
                            {/* Show table name only (without schema prefix) */}
                            <span className="px-1.5 py-0.5 text-xs rounded bg-muted max-w-[260px] truncate" title={t.name}>{t.name}</span>
                            <span className="ml-auto flex items-center gap-2">
                              <button
                                className="text-[11px] px-1.5 py-0.5 rounded-md border hover:bg-muted"
                                title="Preview top 1000 rows"
                                onClick={(e) => { e.preventDefault(); e.stopPropagation(); setPreviewTable(t.name) }}
                              >
                                <RiFocus2Line className="h-3.5 w-3.5" />
                              </button>
                              <span className="text-xs text-muted-foreground">{t.columns?.length || 0} cols</span>
                            </span>
                          </summary>
                          <div className="mt-1 ml-6">
                            <table className="w-full table-fixed text-[11px]">
                              <tbody>
                                {(t.columns || []).map((c) => (
                                  <tr key={c.name} className="align-top">
                                    <td className="w-[160px] pr-2 py-0.5 overflow-hidden">
                                      <div className="flex items-center gap-1 min-w-0 max-w-[160px]">
                                        <RiBracesLine className="h-3.5 w-3.5 opacity-70 flex-none" />
                                        <span className="truncate whitespace-nowrap" title={c.name}>{c.name}</span>
                                      </div>
                                    </td>
                                    <td className="w-[220px] py-0.5 overflow-hidden">
                                      <span
                                        className="inline-flex max-w-[220px] items-center px-1.5 py-0.5 rounded bg-muted text-[10px] text-muted-foreground whitespace-nowrap overflow-hidden"
                                        title={c.type || '-'}
                                      >
                                        <span className="truncate">{middleEllipsis(c.type || '', 28)}</span>
                                      </span>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </details>
                      </li>
                    ))}
                  </ul>
                )
              })()}
            </div>

            <div className="mt-3">
              <div className="text-xs font-medium mb-1">Local Synced</div>
              {localStatsQ.isLoading && <div className="text-xs text-muted-foreground">Loading local stats…</div>}
              {localStatsQ.error && <div className="text-xs text-red-600">Failed to load local stats</div>}
              {localStatsQ.data && (
                <div className="rounded-md border overflow-hidden">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="bg-[hsl(var(--muted))] text-[hsl(var(--foreground))]">
                        <th className="text-left font-medium px-2 py-1">Table</th>
                        <th className="text-left font-medium px-2 py-1">Rows</th>
                        <th className="text-left font-medium px-2 py-1">Last Sync</th>
                        <th className="text-left font-medium px-2 py-1">Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {localStatsQ.data.tables.length === 0 ? (
                        <tr><td className="px-2 py-2 text-muted-foreground" colSpan={4}>No materialized tables yet.</td></tr>
                      ) : (
                        localStatsQ.data.tables.map((t) => (
                          <tr key={t.table} className="border-t hover:bg-[hsl(var(--muted))]/40">
                            <td className="px-2 py-1 font-mono">{t.table}</td>
                            <td className="px-2 py-1">{typeof t.rowCount === 'number' ? t.rowCount.toLocaleString() : '—'}</td>
                            <td className="px-2 py-1">{t.lastSyncAt ? new Date(t.lastSyncAt).toLocaleString() : '—'}</td>
                            <td className="px-2 py-1">
                              <div className="flex items-center gap-2">
                                <button
                                  className="text-xs px-2 py-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted))]"
                                  onClick={() => router.push(`/datasources/${selectedId}?q=${encodeURIComponent(t.table)}`)}
                                  title="Explore"
                                >Explore</button>
                                {confirmDrop === t.table ? (
                                  <>
                                    <span className="text-xs text-muted-foreground">Drop “{t.table}”?</span>
                                    <button className="text-xs px-2 py-1 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--danger))/0.12] text-[hsl(var(--danger))]" onClick={() => dropTable.mutate(t.table)} disabled={dropTable.isPending}>Confirm</button>
                                    <button className="text-xs px-2 py-1 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]" onClick={() => setConfirmDrop(null)}>Cancel</button>
                                  </>
                                ) : (
                                  <button
                                    className="text-xs px-2 py-1 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--danger))/0.12] text-[hsl(var(--danger))]"
                                    onClick={() => setConfirmDrop(t.table)}
                                    title="Drop materialized table"
                                  >Drop</button>
                                )}
                              </div>
                            </td>
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
      <TablePreviewDialog
        open={!!previewTable}
        onOpenChangeAction={(o: boolean) => { if (!o) setPreviewTable(null) }}
        datasourceId={selectedId || ''}
        table={previewTable || ''}
        limit={1000}
      />
    </div>
  )
}

function SchemaRefreshButton({ datasourceId, onRefreshed }: { datasourceId: string; onRefreshed?: (data: IntrospectResponse) => void }) {
  const [busy, setBusy] = useState(false)
  return (
    <button
      className={`absolute right-2 top-1/2 -translate-y-1/2 text-[12px] leading-none rounded px-2 py-0.5 border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-[hsl(var(--muted))] ${busy ? 'opacity-60' : ''}`}
      onClick={async (e) => {
        e.stopPropagation()
        setBusy(true)
        try {
          const data = await SchemaCache.refresh(datasourceId)
          onRefreshed?.(data)
        } catch {
          // ignore
        } finally {
          setBusy(false)
        }
      }}
      title="Refresh schema"
      disabled={busy}
    >
      {busy ? 'Refreshing…' : 'Refresh'}
    </button>
  )
}
