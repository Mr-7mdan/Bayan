"use client"

import { Suspense } from 'react'
import { Card, Title, Text, TabGroup, TabList, Tab, TabPanels, TabPanel, Badge, TextInput } from '@tremor/react'
import { Api, type IntrospectResponse, type DatasourceDetailOut, type SyncTaskOut, type LocalStatsResponse } from '@/lib/api'
import SchemaGraph from '@/components/datasources/SchemaGraph'
import TablePreviewDialog from '@/components/builder/TablePreviewDialog'
import { useEffect, useMemo, useState } from 'react'
import { useParams, useSearchParams } from 'next/navigation'
import { useAuth } from '@/components/providers/AuthProvider'
import { useProgressToast } from '@/components/providers/ProgressToastProvider'
import { RiDatabase2Line, RiTableLine, RiBracesLine } from '@remixicon/react'

export const dynamic = 'force-dynamic'

export default function DatasourceDetailPage() {
  const { id } = useParams<{ id: string }>()
  const { user } = useAuth()
  const searchParams = useSearchParams()
  const { startMonitoring, show } = useProgressToast()
  const [ds, setDs] = useState<DatasourceDetailOut | null>(null)
  const [dsLoading, setDsLoading] = useState(true)
  const [dsError, setDsError] = useState<string | null>(null)
  const [schema, setSchema] = useState<IntrospectResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [filter, setFilter] = useState('')
  const [revealDsn, setRevealDsn] = useState(false)
  const [tasks, setTasks] = useState<SyncTaskOut[] | null>(null)
  const [tasksLoading, setTasksLoading] = useState(true)
  const [tasksError, setTasksError] = useState<string | null>(null)
  const [localStats, setLocalStats] = useState<LocalStatsResponse | null>(null)
  const [localLoading, setLocalLoading] = useState(true)
  const [localError, setLocalError] = useState<string | null>(null)
  const [previewOpen, setPreviewOpen] = useState(false)
  const [previewTable, setPreviewTable] = useState<string | null>(null)

  useEffect(() => {
    let stop = false
    ;(async () => {
      setDsLoading(true); setTasksLoading(true); setLocalLoading(true); setLoading(true)
      try {
        const detail = await Api.getDatasource(id, user?.id)
        if (stop) return
        setDs(detail)
        const isApi = (detail?.type || '').toLowerCase() === 'api'
        const [s, t, ls] = await Promise.all([
          isApi ? Promise.resolve({ schemas: [] } as IntrospectResponse) : Api.introspect(id),
          Api.getSyncStatus(id, user?.id).catch((e) => { throw e }),
          Api.getLocalStats(id).catch((e) => { throw e }),
        ])
        if (stop) return
        setSchema(s)
        setTasks(t)
        setLocalStats(ls)
        // Clear schema error for API datasources
        if (isApi) { setError(null) }
      } catch (e: unknown) {
        if (!stop) {
          const msg = e instanceof Error ? e.message : 'Failed to load datasource'
          setDsError(msg)
          setTasksError(msg)
          setLocalError(msg)
          // Only set main error if not API
          try { if ((ds?.type || '').toLowerCase() !== 'api') setError(msg) } catch {}
        }
      } finally { if (!stop) { setLoading(false); setDsLoading(false); setTasksLoading(false); setLocalLoading(false) } }
    })()
    return () => { stop = true }
  }, [id, user?.id])

  // Prefill filter from ?q= param
  useEffect(() => {
    const q = (searchParams?.get('q') || '').trim()
    if (q) setFilter(q)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams])

  const filtered = useMemo(() => {
    if (!schema) return null
    const q = filter.trim().toLowerCase()
    if (!q) return schema
    const out: IntrospectResponse = { schemas: [] }
    for (const sch of schema.schemas || []) {
      const tables = (sch.tables || []).filter((t) => {
        if (t.name.toLowerCase().includes(q)) return true
        return (t.columns || []).some((c) => c.name.toLowerCase().includes(q))
      })
      if (tables.length) out.schemas.push({ name: sch.name, tables })
    }
    return out
  }, [schema, filter])

  function middleEllipsis(text: string, maxLen = 28): string {
    const s = String(text || '')
    if (!s) return ''
    if (s.length <= maxLen) return s
    const keep = Math.max(2, maxLen - 2)
    const head = s.slice(0, Math.ceil(keep * 0.6))
    const tail = s.slice(-Math.floor(keep * 0.4))
    return `${head}..${tail}`
  }

  function isRoleSchema(name: string): boolean {
    const n = String(name || '').toLowerCase()
    if (!n) return false
    if (n.startsWith('db_')) return true
    if (n === 'information_schema' || n === 'sys' || n === 'guest') return true
    return false
  }

  function preferredOwners(dbType?: string | null): string[] {
    const t = String(dbType || '').toLowerCase()
    if (!t) return []
    if (t.includes('mssql') || t.includes('sqlserver')) return ['dbo']
    if (t.includes('postgres')) return ['public']
    if (t.includes('duckdb') || t.includes('sqlite')) return ['main']
    return []
  }

  const Tree = ({ data }: { data: IntrospectResponse }) => {
    const owners = new Set(preferredOwners(ds?.type))
    const schemas = (data.schemas || []).filter((s) => !isRoleSchema(s.name) && (owners.size ? owners.has(s.name.toLowerCase()) : true))
    const tables = schemas.flatMap((s, si) => (s.tables || []).map((t, ti) => ({ ...t, __schema: s.name, __key: `${String(s.name)}.${t.name}#${si}-${ti}` }))) as Array<any>
    return (
      <div className="text-sm max-h-[420px] overflow-y-auto overflow-x-hidden">
        <ul className="p-2 space-y-2">
          {tables.map((t) => (
            <li key={(t as any).__key} className="text-sm">
              <details open className="rounded">
                <summary className="flex items-center gap-2 px-1 py-0.5 hover:bg-muted/50 rounded cursor-pointer min-w-0">
                  <RiTableLine className="h-4 w-4 opacity-80" />
                  <span className="px-1.5 py-0.5 text-xs rounded bg-muted max-w-[260px] truncate" title={t.name}>{t.name}</span>
                  <span className="ml-auto text-xs text-muted-foreground">{t.columns?.length || 0} cols</span>
                </summary>
                <div className="mt-1 ml-6">
                  <table className="w-full table-fixed text-[11px]">
                    <tbody>
                      {(t.columns || []).map((c: { name: string; type?: string | null }) => (
                        <tr key={`${String((t as any).__key)}.${c.name}`} className="align-top">
                          <td className="w-[200px] pr-2 py-0.5 overflow-hidden">
                            <div className="flex items-center gap-1 min-w-0 max-w-[200px]">
                              <RiBracesLine className="h-3.5 w-3.5 opacity-70 flex-none" />
                              <span className="truncate whitespace-nowrap" title={c.name}>{c.name}</span>
                            </div>
                          </td>
                          <td className="w-[260px] py-0.5 overflow-hidden">
                            <span
                              className="inline-flex max-w-[260px] items-center px-1.5 py-0.5 rounded bg-muted text-[10px] text-muted-foreground whitespace-nowrap overflow-hidden"
                              title={c.type || '-'}
                            >
                              <span className="truncate">{middleEllipsis(c.type || '', 32)}</span>
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
      </div>
    )
  }

  const maskedDsn = useMemo(() => {
    const raw = ds?.connectionUri || ''
    if (!raw) return ''
    // Mask password/secret segment if present: protocol://user:pass@host -> keep user, mask pass
    try {
      const m = raw.match(/^(.*:\/\/)([^:@\/]*)(?::([^@\/]*))?@/)
      if (m) {
        const prefix = m[1] || ''
        const user = m[2] || ''
        const hasPass = typeof m[3] === 'string'
        return raw.replace(/^(.*:\/\/)([^:@\/]*)(?::([^@\/]*))?@/, `${prefix}${user}${hasPass ? ':••••••' : ''}@`)
      }
      return raw
    } catch {
      return raw
    }
  }, [ds?.connectionUri])

  const syncSummary = useMemo(() => {
    const s: { snapshot: number; sequence: number; running: number; lastRun?: string } = { snapshot: 0, sequence: 0, running: 0 }
    if (!tasks || !tasks.length) return s
    let last: number | null = null
    for (const t of tasks) {
      if ((t.mode || '').toLowerCase() === 'snapshot') s.snapshot += 1
      else s.sequence += 1
      if (t.inProgress) s.running += 1
      if (t.lastRunAt) {
        const ts = new Date(t.lastRunAt).getTime()
        if (isFinite(ts) && (last === null || ts > last)) last = ts
      }
    }
    if (last) s.lastRun = new Date(last).toLocaleString()
    return s
  }, [tasks])

  return (
    <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <Title className="text-gray-700 dark:text-gray-100">{ds?.name || 'Datasource'}</Title>
          <div className="mt-1 flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
            <span className="inline-flex items-center gap-1.5 whitespace-nowrap rounded-md bg-white/60 px-2 py-1 text-[11px] font-medium text-gray-700 ring-1 ring-inset ring-gray-200 dark:bg-white/5 dark:text-gray-200 dark:ring-gray-800">{ds?.type || '—'}</span>
            <span className="opacity-80">ID:</span>
            <span className="font-mono">{id}</span>
          </div>
        </div>
      </div>

      {/* Datasource Config */}
      <Card className="p-3">
        <div className="flex items-center justify-between border-b border-[hsl(var(--border))] pb-2">
          <h3 className="text-sm font-semibold">Datasource Config</h3>
          <div className="text-xs text-muted-foreground flex items-center gap-2">
            {typeof ds?.active === 'boolean' && (
              <span className={`inline-flex items-center gap-1.5 whitespace-nowrap rounded-md bg-white/60 px-2 py-1 text-[11px] font-medium ring-1 ring-inset ${ds.active ? 'text-emerald-700 ring-emerald-200 dark:text-emerald-300 dark:ring-emerald-900/60' : 'text-gray-600 ring-gray-200 dark:text-gray-300 dark:ring-gray-800'}`}>{ds.active ? 'Active' : 'Inactive'}</span>
            )}
            {dsLoading && <Text>Loading…</Text>}
            {dsError && <Text className="text-red-600">{dsError}</Text>}
            {!!(tasks && tasks.length) && (
              <button
                className="ml-2 inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-700 dark:text-gray-200 px-2 py-1"
                disabled={ds?.active === false}
                onClick={async () => {
                  try {
                    const res = await Api.runSyncNow(id, undefined, user?.id)
                    if (res?.ok === false) {
                      show('Sync', res?.message || 'Failed to start')
                      return
                    }
                    startMonitoring(id, user?.id)
                  } catch (e: any) {
                    show('Sync', e?.message || 'Failed to start')
                  }
                }}
                title={ds?.active === false ? 'Datasource is inactive' : 'Run all tasks now'}
              >Run now</button>
            )}
          </div>
        </div>
        <div className="mt-3 grid grid-cols-1 md:grid-cols-2 gap-3 text-sm">
          <div className="space-y-1">
            <div className="text-xs text-muted-foreground">Type</div>
            <div className="font-medium">{ds?.type || '—'}</div>
          </div>
          <div className="space-y-1">
            <div className="text-xs text-muted-foreground">Created</div>
            <div>{ds?.createdAt ? new Date(ds.createdAt).toLocaleString() : '—'}</div>
          </div>
          {/* Compact Sync Summary */}
          <div className="space-y-1">
            <div className="text-xs text-muted-foreground">Sync Summary</div>
            {tasksLoading ? (
              <div>Loading…</div>
            ) : tasksError ? (
              <div className="text-danger">{tasksError}</div>
            ) : (
              <div className="text-sm text-gray-700 dark:text-gray-200">
                <span className="mr-3">Snapshot: <span className="font-medium">{syncSummary.snapshot}</span></span>
                <span className="mr-3">Sequence: <span className="font-medium">{syncSummary.sequence}</span></span>
                <span className="mr-3">Running: <span className="font-medium">{syncSummary.running}</span></span>
                <span>Last run: <span className="font-medium">{syncSummary.lastRun || '—'}</span></span>
              </div>
            )}
          </div>
          {/* Sync options quick view */}
          <div className="space-y-1">
            <div className="text-xs text-muted-foreground">Sync Options</div>
            <div className="text-sm text-gray-700 dark:text-gray-200">
              {(() => {
                const sync = (ds?.options as any)?.sync || {}
                const maxc = Number(sync?.maxConcurrentQueries) || 1
                const blk = Array.isArray(sync?.blackoutDaily) ? sync.blackoutDaily : []
                return (
                  <>
                    <span className="mr-3">Max concurrent: <span className="font-medium">{maxc}</span></span>
                    <span>Blackouts: <span className="font-medium">{blk.length ? blk.map((w: any) => `${w.start}–${w.end}`).join(', ') : '—'}</span></span>
                  </>
                )
              })()}
            </div>
          </div>
          <div className="space-y-1 md:col-span-2">
            <div className="text-xs text-muted-foreground">Connection URI</div>
            <div className="flex items-center gap-2">
              <code className="text-xs font-mono break-all px-2 py-1 rounded-md border bg-background w-full">{revealDsn ? (ds?.connectionUri || '') : (maskedDsn || '—')}</code>
              <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={() => setRevealDsn((v) => !v)}>{revealDsn ? 'Hide' : 'Reveal'}</button>
              <button
                type="button"
                className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]"
                onClick={() => { const txt = revealDsn ? (ds?.connectionUri || '') : (maskedDsn || ''); if (txt) void navigator.clipboard.writeText(txt) }}
              >Copy</button>
            </div>
          </div>
          <div className="space-y-1 md:col-span-2">
            <div className="text-xs text-muted-foreground">Options</div>
            <div className="rounded-md border p-2 bg-background max-h-[180px] overflow-auto">
              <pre className="text-xs whitespace-pre-wrap">{ds?.options ? JSON.stringify(ds.options, null, 2) : '{ }'}</pre>
            </div>
          </div>
        </div>
      </Card>

      {/* Local Stats Panel */}
      <Card className="p-3">
        <div className="flex items-center justify-between border-b border-[hsl(var(--border))] pb-2">
          <h3 className="text-sm font-semibold">Local Stats</h3>
          {localLoading && <Text>Loading…</Text>}
          {localError && <Text className="text-red-600">{localError}</Text>}
        </div>
        {localStats && (
          <div className="mt-3 text-sm">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div className="space-y-1">
                <div className="text-xs text-muted-foreground">Engine Path</div>
                <div className="font-mono break-all">{localStats.enginePath}</div>
              </div>
              <div className="space-y-1">
                <div className="text-xs text-muted-foreground">File Size</div>
                <div>{(localStats.fileSize / (1024 * 1024)).toFixed(2)} MB</div>
              </div>
            </div>
            <div className="mt-3">
              <div className="text-xs text-muted-foreground mb-1">Materialized Tables</div>
              <div className="rounded-md border overflow-hidden">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="bg-[hsl(var(--muted))] text-gray-700 dark:text-gray-200">
                      <th className="text-left font-medium px-2 py-1">Table</th>
                      <th className="text-left font-medium px-2 py-1">Row Count</th>
                      <th className="text-left font-medium px-2 py-1">Last Sync</th>
                      <th className="text-left font-medium px-2 py-1">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {localStats.tables.length === 0 ? (
                      <tr><td className="px-2 py-2 text-muted-foreground" colSpan={4}>No materialized tables yet.</td></tr>
                    ) : (
                      localStats.tables.map((t) => (
                        <tr key={t.table} className="border-t">
                          <td className="px-2 py-1 font-mono">{t.table}</td>
                          <td className="px-2 py-1">{typeof t.rowCount === 'number' ? t.rowCount.toLocaleString() : '—'}</td>
                          <td className="px-2 py-1">{t.lastSyncAt ? new Date(t.lastSyncAt).toLocaleString() : '—'}</td>
                          <td className="px-2 py-1">
                            <button
                              type="button"
                              className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]"
                              onClick={() => { setPreviewTable(t.table); setPreviewOpen(true) }}
                              title="Preview table"
                            >Preview</button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}
      </Card>

      {(ds?.type || '').toLowerCase() !== 'api' && (
        <Card>
          {loading ? <Text>Loading…</Text> : error ? (
            <Text className="text-red-600">{error}</Text>
          ) : (
            <TabGroup>
              <TabList>
                <Tab>Tree</Tab>
                <Tab>Raw</Tab>
                <Tab>Graph</Tab>
              </TabList>
              <TabPanels>
                <TabPanel>
                  <div className="mb-2 flex items-center gap-2">
                    <Text className="text-sm">Search</Text>
                    <TextInput className="w-72" value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="Filter tables or columns" />
                    {filtered && (
                      <Badge color="blue">{filtered.schemas?.reduce((a, s) => a + (s.tables?.length || 0), 0)} tables</Badge>
                    )}
                  </div>
                  {filtered ? <Tree data={filtered} /> : <Text>No data.</Text>}
                </TabPanel>
                <TabPanel>
                  <div className="text-sm max-h-[420px] overflow-auto">
                    <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(schema, null, 2)}</pre>
                  </div>
                </TabPanel>
                <TabPanel>
                  {schema ? <SchemaGraph schema={schema} height={560} /> : <Text>No schema.</Text>}
                </TabPanel>
              </TabPanels>
            </TabGroup>
          )}
        </Card>
      )}

      {previewOpen && previewTable && (
        <TablePreviewDialog
          open={previewOpen}
          onOpenChangeAction={(o) => setPreviewOpen(o)}
          table={previewTable}
          limit={100}
        />
      )}
    </div>
    </Suspense>
  )
}
