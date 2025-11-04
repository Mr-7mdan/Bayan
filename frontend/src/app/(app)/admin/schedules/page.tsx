"use client"

import { Suspense, useEffect, useMemo, useRef, useState } from 'react'
import type React from 'react'
import { useRouter } from 'next/navigation'
import { Card, Title, Text, TextInput, Badge, TabGroup, TabList, Tab, TabPanels, TabPanel, Select, SelectItem } from '@tremor/react'
import * as Popover from '@radix-ui/react-popover'
import { RiFocus2Line, RiArrowDownSLine } from '@remixicon/react'
import { useAuth } from '@/components/providers/AuthProvider'
import { Api, type DatasourceOut, type DatasourceDetailOut, type SyncTaskOut, type SyncTaskCreate, type SyncRunOut, type IntrospectResponse, type TablesOnlyResponse } from '@/lib/api'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useProgressToast } from '@/components/providers/ProgressToastProvider'
import TablePreviewDialog from '@/components/builder/TablePreviewDialog'

export const dynamic = 'force-dynamic'

function AdminSchedulesInner() {
  const { user } = useAuth()
  const router = useRouter()
  const isAdmin = (user?.role || '').toLowerCase() === 'admin'
  const qc = useQueryClient()
  const { startMonitoring, show } = useProgressToast()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [form, setForm] = useState<SyncTaskCreate>({ mode: 'snapshot', sourceTable: '', destTableName: '', scheduleCron: '0 2 * * *', enabled: true, selectColumns: [] })
  const [destEdited, setDestEdited] = useState(false)
  // Tabs behavior to replicate My Dashboards animation
  const [tabIndex, setTabIndex] = useState(0)
  const prevTabIndex = useRef(0)
  const [slideDir, setSlideDir] = useState<'left' | 'right'>('right')
  const [previewOpen, setPreviewOpen] = useState(false)
  const [cronMode, setCronMode] = useState<'custom' | 'every_n_hours'>('custom')
  const [cronHoursInterval, setCronHoursInterval] = useState<number>(4)
  const [editingTaskId, setEditingTaskId] = useState<string | null>(null)

  useEffect(() => {
    if (!isAdmin) router.replace('/home')
  }, [isAdmin, router])

  if (!isAdmin) return null

  const dsQ = useQuery<DatasourceOut[], Error>({ queryKey: ['admin-ds'], queryFn: () => Api.listDatasources(undefined, user?.id) })
  const selDs = useMemo(() => (dsQ.data || []).find((d) => d.id === selectedId) || null, [dsQ.data, selectedId])
  const isApiDs = ((selDs?.type || '').toLowerCase() === 'api')

  const tasksQ = useQuery<SyncTaskOut[], Error>({
    queryKey: ['sync-tasks', selectedId],
    queryFn: () => Api.listSyncTasks(selectedId as string, user?.id),
    enabled: !!selectedId,
  })

  // Cached tables-only metadata for schema/table selection
  const tablesOnlyQ = useQuery<TablesOnlyResponse, Error>({
    queryKey: ['tables-only', selectedId],
    queryFn: () => Api.tablesOnly(selectedId as string),
    enabled: !!selectedId && !isApiDs,
    staleTime: 5 * 60 * 1000,
  })

  // Optional: datasource details (to parse owner/user for default schema)
  const dsDetailQ = useQuery<DatasourceDetailOut, Error>({
    queryKey: ['ds-detail', selectedId],
    queryFn: () => Api.getDatasource(selectedId as string, user?.id),
    enabled: !!selectedId && !isApiDs,
    staleTime: 5 * 60 * 1000,
  })

  // Load detailed columns info only when a table is chosen (reduce load)
  const introspectQ = useQuery<IntrospectResponse, Error>({
    queryKey: ['introspect', selectedId, form.sourceSchema, form.sourceTable],
    queryFn: () => Api.introspect(selectedId as string),
    enabled: !!selectedId && !isApiDs && !!form.sourceSchema && !!form.sourceTable,
  })

  const schemaNames = useMemo(() => (tablesOnlyQ.data?.schemas || []).map(s => s.name), [tablesOnlyQ.data])
  const tablesForSchema = useMemo(() => {
    const sch = (form.sourceSchema || '').trim()
    if (!tablesOnlyQ.data || !sch) return [] as string[]
    const found = tablesOnlyQ.data.schemas?.find((s) => s.name === sch)
    return (found?.tables || [])
  }, [tablesOnlyQ.data, form.sourceSchema])

  // Default source schema to the likely owner schema per dialect
  useEffect(() => {
    if (!selectedId) return
    if (isApiDs) return
    const names = schemaNames || []
    if (!names.length) return
    const sel = (dsQ.data || []).find((d) => d.id === selectedId)
    const dsType = String(sel?.type || '').toLowerCase()
    const dsn = dsDetailQ.data?.connectionUri || ''
    const parseUser = () => {
      try { const u = new URL(dsn); return (u.username || '').trim() } catch { return '' }
    }
    const parseDb = () => {
      try { const u = new URL(dsn); const p = (u.pathname || '').replace(/^\//, ''); return p } catch { return '' }
    }
    let preferred = ''
    if (dsType.includes('postgres') || dsType.includes('redshift')) {
      preferred = names.includes('public') ? 'public' : ''
    } else if (dsType.includes('sqlserver') || dsType.includes('mssql')) {
      preferred = names.includes('dbo') ? 'dbo' : ''
    } else if (dsType.includes('duckdb')) {
      preferred = names.includes('main') ? 'main' : ''
    } else if (dsType.includes('mysql') || dsType.includes('mariadb')) {
      const db = parseDb()
      preferred = db && names.includes(db) ? db : ''
    } else if (dsType.includes('oracle')) {
      const u = parseUser().toUpperCase()
      preferred = u && names.includes(u) ? u : ''
    }
    const next = preferred || names[0]
    // Only set if not yet set or invalid
    setForm((f) => {
      if (!f.sourceSchema || !names.includes(f.sourceSchema)) return { ...f, sourceSchema: next }
      return f
    })
  }, [selectedId, schemaNames.length, dsQ.data, dsDetailQ.data])

  // Ensure API datasources don't require schema/table; set a sentinel sourceTable
  useEffect(() => {
    if (!selectedId) return
    if (!isApiDs) return
    setForm((f) => ({ ...f, sourceSchema: undefined, sourceTable: (f.sourceTable || 'api') }))
  }, [selectedId, isApiDs])

  // Derive cron expression for preset modes
  useEffect(() => {
    if (cronMode === 'every_n_hours') {
      const n = Math.max(1, Math.min(24, Number.isFinite(cronHoursInterval) ? cronHoursInterval : 1))
      setForm((f) => ({ ...f, scheduleCron: `0 */${n} * * *` }))
    }
  }, [cronMode, cronHoursInterval])

  const availableColumns = useMemo(() => {
    const schName = (form.sourceSchema || '').trim()
    const tblName = (form.sourceTable || '').trim()
    const meta = introspectQ.data
    if (!meta || !tblName) return [] as string[]
    let columns: string[] = []
    for (const sch of meta.schemas || []) {
      if (schName && sch.name !== schName) continue
      for (const t of sch.tables || []) {
        if (t.name === tblName) {
          columns = (t.columns || []).map((c) => c.name)
          break
        }
      }
      if (columns.length) break
    }
    return columns.sort((a, b) => a.localeCompare(b))
  }, [introspectQ.data, form.sourceSchema, form.sourceTable])

  const logsQ = useQuery<SyncRunOut[], Error>({
    queryKey: ['sync-logs', selectedId],
    queryFn: () => Api.getSyncLogs(selectedId as string, undefined, 50, user?.id),
    enabled: !!selectedId,
    refetchInterval: 5000,
  })

  const createTask = useMutation({
    mutationFn: async (payload: SyncTaskCreate) => Api.createSyncTask(selectedId as string, { ...payload, datasourceId: selectedId as string }, user?.id),
    onSuccess: async () => { await qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] }) },
  })

  const deleteTask = useMutation({
    mutationFn: async (taskId: string) => Api.deleteSyncTask(selectedId as string, taskId, user?.id),
    onSuccess: async () => { await qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] }) },
  })

  const saveTask = useMutation({
    mutationFn: async () => {
      if (!selectedId || !editingTaskId) return null as any
      const payload: SyncTaskCreate = { ...form, datasourceId: selectedId }
      return await Api.updateSyncTask(selectedId as string, editingTaskId as string, payload, user?.id)
    },
    onSuccess: async () => {
      show('Task', 'Saved')
      setEditingTaskId(null)
      setDestEdited(false)
      setCronMode('custom')
      setForm({ mode: 'snapshot', sourceTable: '', destTableName: '', scheduleCron: '0 2 * * *', enabled: true, selectColumns: [] })
      await qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] })
    },
  })

  function beginEdit(t: SyncTaskOut) {
    setEditingTaskId(t.id)
    setDestEdited(true)
    setForm({
      mode: (t.mode as any) === 'sequence' ? 'sequence' : 'snapshot',
      sourceSchema: t.sourceSchema || undefined,
      sourceTable: t.sourceTable,
      destTableName: t.destTableName,
      sequenceColumn: t.sequenceColumn || undefined,
      batchSize: t.batchSize || undefined,
      pkColumns: t.pkColumns || [],
      selectColumns: t.selectColumns || [],
      scheduleCron: t.scheduleCron || undefined,
      enabled: t.enabled,
    })
    const m = String(t.scheduleCron || '').match(/^0\s+\*\/(\d+)\s+\*\s+\*\s+\*$/)
    if (m) {
      const n = Math.max(1, Math.min(24, parseInt(m[1], 10) || 1))
      setCronMode('every_n_hours')
      setCronHoursInterval(n)
    } else {
      setCronMode('custom')
    }
  }

  function cancelEdit() {
    setEditingTaskId(null)
    setDestEdited(false)
    setCronMode('custom')
    setForm({ mode: 'snapshot', sourceTable: '', destTableName: '', scheduleCron: '0 2 * * *', enabled: true, selectColumns: [] })
  }

  const runAll = useMutation({
    mutationFn: async () => Api.runSyncNow(selectedId as string, undefined, user?.id),
    onSuccess: (res) => {
      if (res?.ok === false) { show('Sync', res?.message || 'Failed to start'); return }
      startMonitoring(selectedId as string, user?.id)
      void qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] })
      void qc.invalidateQueries({ queryKey: ['sync-logs', selectedId] })
    },
    onError: (e: any) => show('Sync', e?.message || 'Failed to start'),
  })

  const runOne = useMutation({
    mutationFn: async (taskId: string) => Api.runSyncNow(selectedId as string, taskId, user?.id),
    onSuccess: (res) => {
      if (res?.ok === false) { show('Sync', res?.message || 'Failed to start'); return }
      if (selectedId) startMonitoring(selectedId, user?.id)
      void qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] })
      void qc.invalidateQueries({ queryKey: ['sync-logs', selectedId] })
    },
    onError: (e: any) => show('Sync', e?.message || 'Failed to start'),
  })

  const abortOne = useMutation({
    mutationFn: async (taskId?: string) => Api.abortSync(selectedId as string, taskId, user?.id),
    onSuccess: () => {
      show('Sync', 'Abort requested')
      void qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] })
      void qc.invalidateQueries({ queryKey: ['sync-logs', selectedId] })
    },
    onError: (e: any) => show('Sync', e?.message || 'Abort failed'),
  })

  const clearLogs = useMutation({
    mutationFn: async () => {
      if (!selectedId) return { deleted: 0 }
      return await Api.clearSyncLogs(selectedId as string, undefined, user?.id)
    },
    onSuccess: async (res) => {
      show('Logs', `Cleared ${res?.deleted ?? 0} entries`)
      await qc.invalidateQueries({ queryKey: ['sync-logs', selectedId] })
    },
    onError: (e: any) => show('Logs', e?.message || 'Failed to clear logs'),
  })

  const runningCount = useMemo(() => (tasksQ.data || []).filter((t) => t.inProgress).length, [tasksQ.data])
  const scheduledCount = useMemo(() => (tasksQ.data || []).filter((t) => !!t.scheduleCron).length, [tasksQ.data])

  // Scheduler jobs (admin)
  const jobsQ = useQuery<{ id: string; nextRunAt?: string | null; dsId?: string; taskId?: string }[], Error>({
    queryKey: ['scheduler-jobs'],
    queryFn: () => user?.id ? Api.adminSchedulerJobs(user.id) : Promise.resolve([]),
    enabled: !!user?.id,
    refetchInterval: 15000,
  })

  const refreshSchedules = useMutation({
    mutationFn: async () => { if (!user?.id) return { added: 0, updated: 0, removed: 0, total: 0 }; return await Api.adminSchedulerRefresh(user.id) },
    onSuccess: () => { void qc.invalidateQueries({ queryKey: ['scheduler-jobs'] }) },
  })

  // Pagination states
  const [taskPage, setTaskPage] = useState(1)
  const [taskPageSize, setTaskPageSize] = useState(10)
  const [logPage, setLogPage] = useState(1)
  const [logPageSize, setLogPageSize] = useState(10)

  const tasksData = tasksQ.data || []
  const totalTaskPages = Math.max(1, Math.ceil(tasksData.length / taskPageSize))
  const pagedTasks = tasksData.slice((taskPage - 1) * taskPageSize, taskPage * taskPageSize)

  const logsData = logsQ.data || []
  const totalLogPages = Math.max(1, Math.ceil(logsData.length / logPageSize))
  const pagedLogs = logsData.slice((logPage - 1) * logPageSize, logPage * logPageSize)

  // Map last run duration per task (in seconds) using logs (latest entry per task)
  const durationByTask = useMemo(() => {
    const map: Record<string, number> = {}
    for (const r of logsData) {
      // logs are already ordered by startedAt desc
      if (r.taskId && map[r.taskId] === undefined) {
        if (r.startedAt && r.finishedAt) {
          const sec = Math.max(0, Math.round((new Date(r.finishedAt).getTime() - new Date(r.startedAt).getTime()) / 1000))
          map[r.taskId] = sec
        }
      }
    }
    return map
  }, [logsData])

  return (
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--card))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">Schedule Workers</Title>
            <div className="mt-1 flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
              <span className="opacity-80">Manage snapshot & incremental tasks, run them, and view logs.</span>
              {selectedId && (
                <>
                  <Badge color="emerald">Running: {runningCount}</Badge>
                  <Badge color="indigo">Scheduled: {scheduledCount}</Badge>
                </>
              )}
            </div>
          </div>
          <div className="flex items-center gap-2">
            <div className="min-w-[220px] rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
              [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
              [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
              <Select value={selectedId || ''} onValueChange={(v) => setSelectedId(v || null)} placeholder="Select datasource…" className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent">
                {(dsQ.data || []).map((d) => (
                  <SelectItem key={d.id} value={d.id}>{d.name} ({d.type})</SelectItem>
                ))}
              </Select>
            </div>
            <button
              className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed"
              disabled={!selectedId || runAll.isPending}
              onClick={() => runAll.mutate()}
            >
              Run all now
            </button>
            <button
              className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed"
              onClick={() => refreshSchedules.mutate()}
            >
              Refresh schedules
            </button>
          </div>
        </div>
        <div className="p-4 space-y-4">
          <TabGroup index={tabIndex} onIndexChange={(i) => { setSlideDir(i > prevTabIndex.current ? 'left' : 'right'); prevTabIndex.current = i; setTabIndex(i) }}>
            <TabList className="px-3 py-1.5 border-b border-[hsl(var(--border))]">
              <Tab className="pb-2.5 font-medium hover:border-gray-300">
                <span className="text-gray-500 dark:text-gray-400 ui-selected:text-gray-800 ui-selected:dark:text-white">Create Tasks</span>
              </Tab>
              <Tab className="pb-2.5 font-medium hover:border-gray-300">
                <span className="text-gray-500 dark:text-gray-400 ui-selected:text-gray-800 ui-selected:dark:text-white">Running Tasks</span>
              </Tab>
              <Tab className="pb-2.5 font-medium hover:border-gray-300">
                <span className="text-gray-500 dark:text-gray-400 ui-selected:text-gray-800 ui-selected:dark:text-white">Scheduled Tasks</span>
              </Tab>
            </TabList>
            <TabPanels className="pt-0">
              <TabPanel className={`px-3 pb-3 pt-0 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}`}>
                {/* Create Task */}
                <div className="rounded-md border bg-[hsl(var(--card))]">
                  <div className="px-3 py-2 border-b text-sm font-medium">Create Task</div>
                  <div className="p-3 grid grid-cols-1 md:grid-cols-6 gap-3 text-sm">
                    <div>
                      <div className="text-xs text-muted-foreground">Mode</div>
                      <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                        [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                        [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                        <Select value={form.mode} onValueChange={(v) => setForm((f) => ({ ...f, mode: (v as 'snapshot'|'sequence') }))} className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent">
                          <SelectItem value="snapshot">snapshot</SelectItem>
                          <SelectItem value="sequence">sequence</SelectItem>
                        </Select>
                      </div>
                    </div>
                    {!isApiDs && (
                      <div>
                        <div className="text-xs text-muted-foreground">Source schema (auto‑selected)</div>
                        <div className="relative rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                          [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                          [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                          <Select
                            value={(form.sourceSchema || '') as string}
                            onValueChange={(v) => { /* schema fixed by default; we keep it readonly */ }}
                            placeholder={form.sourceSchema ? `Schema: ${form.sourceSchema}` : (selectedId ? 'Determining schema…' : 'Select a datasource')}
                            className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                            disabled={true}
                          >
                            {form.sourceSchema && <SelectItem key={form.sourceSchema} value={form.sourceSchema}>{form.sourceSchema}</SelectItem>}
                          </Select>
                          {selectedId && tablesOnlyQ.isLoading && (
                            <div className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 opacity-70">
                              <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"></path></svg>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                    {!isApiDs && (
                      <div>
                        <div className="text-xs text-muted-foreground">Source table</div>
                        <div className="flex items-center gap-2">
                          <div className="relative flex-1 rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                            [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                            [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                            <Select
                              value={form.sourceTable}
                              onValueChange={(v) => { setForm((f) => ({ ...f, sourceTable: v || '', selectColumns: [] })); if (!destEdited && v) setForm((f) => ({ ...f, destTableName: v })); }}
                              placeholder={!form.sourceSchema ? 'Detecting schema…' : (tablesOnlyQ.isFetching ? 'Loading tables…' : 'Select table')}
                              className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                              disabled={!form.sourceSchema || tablesOnlyQ.isFetching}
                            >
                              {!tablesOnlyQ.isFetching && tablesForSchema.map((t) => (
                                <SelectItem key={t} value={t}>{t}</SelectItem>
                              ))}
                            </Select>
                            {form.sourceSchema && tablesOnlyQ.isFetching && (
                              <div className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 opacity-70">
                                <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"></path></svg>
                              </div>
                            )}
                          </div>
                          <button
                            type="button"
                            className="shrink-0 text-[11px] px-1.5 py-0.5 rounded-md border hover:bg-muted"
                            title="Preview top 1000 rows"
                            disabled={!selectedId || !form.sourceTable}
                            onClick={() => setPreviewOpen(true)}
                          >
                            <RiFocus2Line className="h-3.5 w-3.5" />
                          </button>
                        </div>
                      </div>
                    )}
                    <div>
                      <div className="text-xs text-muted-foreground">Dest table</div>
                      <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                        [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none">
                        <TextInput className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent" value={form.destTableName} onChange={(e) => { setDestEdited(true); setForm((f) => ({ ...f, destTableName: e.target.value })) }} placeholder="orders_mat" />
                      </div>
                    </div>
                    {form.mode === 'sequence' && !isApiDs && (
                      <div>
                        <div className="text-xs text-muted-foreground">Sequence column</div>
                        <div className="relative rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                          [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                          [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                          <Select
                            value={(form.sequenceColumn || '') as string}
                            onValueChange={(v) => setForm((f) => ({ ...f, sequenceColumn: v || undefined }))}
                            placeholder={!form.sourceTable ? 'Select a table first' : (introspectQ.isFetching ? 'Loading columns…' : 'Select sequence column')}
                            className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                            disabled={!form.sourceTable || introspectQ.isFetching || availableColumns.length === 0}
                          >
                            {!introspectQ.isFetching && availableColumns.map((c) => (
                              <SelectItem key={c} value={c}>{c}</SelectItem>
                            ))}
                          </Select>
                          {form.sourceTable && introspectQ.isFetching && (
                            <div className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 opacity-70">
                              <svg className="animate-spin h-3 w-3" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"></path></svg>
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                    {!isApiDs && (
                      <div>
                        <div className="text-xs text-muted-foreground">PK columns</div>
                        <div className="flex flex-col gap-1">
                          <div className="relative flex-1 min-w-0 rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]">
                            <Popover.Root>
                              <Popover.Trigger asChild>
                                <button
                                  type="button"
                                  className="w-full h-9 px-3 inline-flex items-center justify-between text-sm text-[hsl(var(--foreground))]"
                                  disabled={!form.sourceTable || availableColumns.length === 0}
                                >
                                  <span className="truncate">{(form.pkColumns || []).length ? `${(form.pkColumns || []).length} selected` : 'Select columns'}</span>
                                  <RiArrowDownSLine className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
                                </button>
                              </Popover.Trigger>
                              <Popover.Portal>
                                <Popover.Content side="bottom" align="start" className="z-50 w-64 rounded-lg border bg-card p-2 shadow-card">
                                  <div className="max-h-64 overflow-auto text-xs">
                                    {availableColumns.length === 0 && (
                                      <div className="text-muted-foreground">No columns (select a table)</div>
                                    )}
                                    {availableColumns.map((c) => (
                                      <label key={c} className="flex items-center gap-2 px-1 py-0.5">
                                        <input
                                          type="checkbox"
                                          checked={!!(form.pkColumns || []).includes(c)}
                                          onChange={(e) => setForm((f) => {
                                            const cur = new Set(f.pkColumns || [])
                                            if (e.target.checked) cur.add(c); else cur.delete(c)
                                            const ordered = availableColumns.filter((x) => cur.has(x))
                                            return { ...f, pkColumns: ordered }
                                          })}
                                        />
                                        <span className="font-mono">{c}</span>
                                      </label>
                                    ))}
                                  </div>
                                  <Popover.Arrow className="fill-[hsl(var(--card))]" />
                                </Popover.Content>
                              </Popover.Portal>
                            </Popover.Root>
                          </div>
                          {(form.pkColumns || []).length > 0 && (
                            <div className="flex flex-wrap gap-1">
                              {(form.pkColumns || []).map((c) => (
                                <span key={c} className="inline-flex items-center gap-1 text-[11px] px-1.5 py-0.5 rounded border bg-[hsl(var(--secondary)/0.6)]">
                                  <span className="font-mono">{c}</span>
                                  <button className="opacity-70 hover:opacity-100" onClick={() => setForm((f) => ({ ...f, pkColumns: (f.pkColumns || []).filter((x) => x !== c) }))}>×</button>
                                </span>
                              ))}
                            </div>
                          )}
                        </div>
                      </div>
                    )}
                    <div>
                      <div className="text-xs text-muted-foreground">Batch size</div>
                      <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                        [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none">
                        <TextInput className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent" value={(form.batchSize as any) || ''} onChange={(e) => setForm((f) => ({ ...f, batchSize: Number(e.target.value) || undefined }))} placeholder="10000" />
                      </div>
                    </div>
                    <div className="md:col-span-2 md:col-start-1 flex flex-col justify-end gap-2">
                      <div>
                        <div className="text-xs text-muted-foreground">Cron mode</div>
                        <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                          [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                          [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                          <Select value={cronMode} onValueChange={(v) => setCronMode((v as 'custom'|'every_n_hours'))} className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent">
                            <SelectItem value="custom">Custom cron</SelectItem>
                            <SelectItem value="every_n_hours">Every N hours</SelectItem>
                          </Select>
                        </div>
                      </div>
                      {cronMode === 'every_n_hours' && (
                        <div>
                          <div className="text-xs text-muted-foreground">Every N hours</div>
                          <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                            [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none">
                            <TextInput
                              type="number"
                              min={1}
                              max={24}
                              className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                              value={String(cronHoursInterval)}
                              onChange={(e: React.ChangeEvent<HTMLInputElement>) => setCronHoursInterval(Math.max(1, Math.min(24, parseInt(e.target.value || '1', 10))))}
                              placeholder="4"
                            />
                          </div>
                        </div>
                      )}
                      <div>
                        <div className="text-xs text-muted-foreground">Cron</div>
                        <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                          [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none">
                          <TextInput
                            className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                            value={(form.scheduleCron || '') as string}
                            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setForm((f) => ({ ...f, scheduleCron: e.target.value }))}
                            placeholder="0 2 * * *"
                            disabled={cronMode === 'every_n_hours'}
                          />
                        </div>
                      </div>
                    </div>
                    <div className="md:col-span-2 md:col-start-3 flex items-end" />
                    <div className="md:col-start-5 flex items-end md:justify-start">
                      <label className="inline-flex items-center gap-2 text-sm"><input type="checkbox" checked={!!form.enabled} onChange={(e) => setForm((f) => ({ ...f, enabled: e.target.checked }))} /> Enabled</label>
                    </div>
                    <div className="md:col-start-6 flex items-end md:justify-start gap-2">
                      <button
                        className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed h-9"
                        disabled={!selectedId || (!!editingTaskId ? saveTask.isPending : createTask.isPending)}
                        onClick={() => {
                          if (!selectedId || !form.sourceTable || !form.destTableName) return
                          if (editingTaskId) saveTask.mutate()
                          else createTask.mutate(form)
                        }}
                      >
                        {editingTaskId ? 'Save' : 'Create'}
                      </button>
                      {editingTaskId && (
                        <button
                          className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed h-9"
                          onClick={cancelEdit}
                          disabled={saveTask.isPending}
                        >Cancel</button>
                      )}
                    </div>
                  </div>
                  {/* Hints */}
                  <div className="px-3 pb-3">
                    <div className="mt-2 rounded-md border p-2 bg-[hsl(var(--background))]">
                      <div className="text-xs font-semibold mb-1">Hints</div>
                      <ul className="text-[11px] space-y-1 text-muted-foreground list-disc pl-4">
                        <li><strong>Snapshot</strong>: copies the full table (stage then swap). Good for small-to-medium tables or daily refresh.</li>
                        <li><strong>Sequence</strong>: incremental upsert using a monotonic sequence column (e.g., auto-increment id or updated_at timestamp).</li>
                        <li><strong>PK columns</strong>: the primary key columns that uniquely identify a row. Use comma for composite keys (e.g., <code>order_id,line_no</code>). Required for correct upserts in sequence mode.</li>
                        <li><strong>Cron</strong>: schedules automatic runs (e.g., <code>0 2 * * *</code> runs daily at 02:00). Times are evaluated on the server.</li>
                      </ul>
                    </div>
                  </div>
                </div>

                {/* Tasks table with pagination */}
                <div className="rounded-md border overflow-hidden mt-4 bg-[hsl(var(--card))]">
                  <div className="px-3 py-2 border-b text-sm font-medium flex items-center justify-between">
                    <span>Tasks</span>
                    <div className="flex items-center gap-2 text-xs">
                      <span className="min-w-[140px] whitespace-nowrap">Rows per page</span>
                      <div className="min-w-[96px] rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                        [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                        [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                        <Select value={String(taskPageSize)} onValueChange={(v) => { setTaskPageSize(Number(v)); setTaskPage(1) }} className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent">
                          {[5,10,20,50].map((n) => <SelectItem key={n} value={String(n)}>{n}</SelectItem>)}
                        </Select>
                      </div>
                    </div>
                  </div>
                  <div className="overflow-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-[hsl(var(--muted))] text-gray-700 dark:text-gray-200">
                          <th className="text-left font-medium px-2 py-1">Mode</th>
                          <th className="text-left font-medium px-2 py-1">Source</th>
                          <th className="text-left font-medium px-2 py-1">Dest</th>
                          <th className="text-left font-medium px-2 py-1">PKs</th>
                          <th className="text-left font-medium px-2 py-1">Seq</th>
                          <th className="text-left font-medium px-2 py-1">Batch</th>
                          <th className="text-left font-medium px-2 py-1">Cron</th>
                          <th className="text-left font-medium px-2 py-1">Enabled</th>
                          <th className="text-left font-medium px-2 py-1">Last run</th>
                          <th className="text-left font-medium px-2 py-1">Rows</th>
                          <th className="text-left font-medium px-2 py-1">Duration (s)</th>
                          <th className="text-left font-medium px-2 py-1">Status</th>
                          <th className="text-left font-medium px-2 py-1">Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {pagedTasks.map((t, idx) => (
                          <tr key={t.id} className={`border-t ${idx % 2 === 1 ? 'bg-[hsl(var(--muted))]/20' : ''} hover:bg-[hsl(var(--muted))]/40`}>
                            <td className="px-2 py-1">{t.mode}</td>
                            <td className="px-2 py-1 whitespace-normal break-words">{t.sourceSchema ? `${t.sourceSchema}.` : ''}{t.sourceTable}</td>
                            <td className="px-2 py-1 font-mono whitespace-normal break-words">{t.destTableName}</td>
                            <td className="px-2 py-1">{t.pkColumns?.join(', ')}</td>
                            <td className="px-2 py-1">{t.sequenceColumn || '—'}</td>
                            <td className="px-2 py-1">{t.batchSize || '—'}</td>
                            <td className="px-2 py-1">{t.scheduleCron || '—'}</td>
                            <td className="px-2 py-1">{t.enabled ? 'Yes' : 'No'}</td>
                            <td className="px-2 py-1">{t.lastRunAt ? new Date(t.lastRunAt).toLocaleString() : '—'}</td>
                            <td className="px-2 py-1">{typeof t.lastRowCount === 'number' ? t.lastRowCount.toLocaleString() : '—'}</td>
                            <td className="px-2 py-1">{typeof durationByTask[t.id] === 'number' ? durationByTask[t.id].toLocaleString() : '—'}</td>
                            <td className="px-2 py-1">{t.inProgress ? (
                              <span className="text-emerald-700 dark:text-emerald-300">Running {typeof t.progressCurrent === 'number' && typeof t.progressTotal === 'number' ? `(${t.progressCurrent}/${t.progressTotal})` : ''}</span>
                            ) : (t.error ? <span className="text-red-600">Error</span> : 'Idle')}</td>
                            <td className="px-2 py-1">
                              <div className="flex items-center gap-2">
                                <button
                                  className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 text-xs font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed"
                                  title="Edit"
                                  onClick={() => beginEdit(t)}
                                >Edit</button>
                                {t.inProgress ? (
                                  <button
                                    className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 px-2 py-1 text-xs font-medium hover:bg-red-100 dark:hover:bg-red-900/30 disabled:opacity-50 disabled:cursor-not-allowed"
                                    title="Abort"
                                    onClick={() => abortOne.mutate(t.id)}
                                  >Abort</button>
                                ) : (
                                  <button
                                    className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 text-xs font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed"
                                    title="Run now"
                                    onClick={() => runOne.mutate(t.id)}
                                  >Run</button>
                                )}
                                <button
                                  className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-2 py-1 text-xs font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed text-red-600"
                                  title="Delete"
                                  onClick={() => deleteTask.mutate(t.id)}
                                >Delete</button>
                              </div>
                            </td>
                          </tr>
                        ))}
                        {tasksData.length === 0 && (
                          <tr><td colSpan={13} className="px-2 py-2 text-muted-foreground">No tasks yet. Create one above.</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                  {/* Pagination controls */}
                  <div className="flex items-center justify-between px-3 py-2 border-t text-xs">
                    <div>Page {taskPage} of {totalTaskPages}</div>
                    <div className="flex items-center gap-2">
                      <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 text-xs font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={taskPage<=1} onClick={() => setTaskPage((p) => Math.max(1, p-1))}>Prev</button>
                      <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 text-xs font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={taskPage>=totalTaskPages} onClick={() => setTaskPage((p) => Math.min(totalTaskPages, p+1))}>Next</button>
                    </div>
                  </div>
                </div>

                {/* Logs with pagination */}
                <div className="rounded-md border overflow-hidden mt-4 bg-[hsl(var(--card))]">
                  <div className="px-3 py-2 border-b text-sm font-medium flex items-center justify-between">
                    <span>Logs</span>
                    <div className="flex items-center gap-2 text-xs">
                      <button
                        className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 text-xs font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed"
                        disabled={!selectedId || (logsData.length === 0) || clearLogs.isPending}
                        onClick={() => {
                          if (!selectedId) return
                          const ok = window.confirm('Clear logs for this datasource? This will permanently delete the shown run entries.')
                          if (!ok) return
                          clearLogs.mutate()
                        }}
                      >Clear logs</button>
                      <span className="min-w-[140px] whitespace-nowrap">Rows per page</span>
                      <div className="min-w-[96px] rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                        [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                        [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                        <Select value={String(logPageSize)} onValueChange={(v) => { setLogPageSize(Number(v)); setLogPage(1) }} className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent">
                          {[5,10,20,50].map((n) => <SelectItem key={n} value={String(n)}>{n}</SelectItem>)}
                        </Select>
                      </div>
                    </div>
                  </div>
                  <div className="overflow-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-[hsl(var(--muted))] text-gray-700 dark:text-gray-200">
                          <th className="text-left font-medium px-2 py-1">Started</th>
                          <th className="text-left font-medium px-2 py-1">Mode</th>
                          <th className="text-left font-medium px-2 py-1">Task</th>
                          <th className="text-left font-medium px-2 py-1">Rows</th>
                          <th className="text-left font-medium px-2 py-1">Finished</th>
                          <th className="text-left font-medium px-2 py-1">Error</th>
                        </tr>
                      </thead>
                      <tbody>
                        {pagedLogs.map((r, idx) => (
                          <tr key={r.id} className={`border-t ${idx % 2 === 1 ? 'bg-[hsl(var(--muted))]/20' : ''} hover:bg-[hsl(var(--muted))]/40`}>
                            <td className="px-2 py-1">{new Date(r.startedAt).toLocaleString()}</td>
                            <td className="px-2 py-1">{r.mode}</td>
                            <td className="px-2 py-1 font-mono">{r.taskId}</td>
                            <td className="px-2 py-1">{typeof r.rowCount === 'number' ? r.rowCount.toLocaleString() : '—'}</td>
                            <td className="px-2 py-1">{r.finishedAt ? new Date(r.finishedAt).toLocaleString() : '—'}</td>
                            <td className="px-2 py-1">{r.error ? <span className="text-red-600">{r.error}</span> : '—'}</td>
                          </tr>
                        ))}
                        {logsData.length === 0 && (
                          <tr><td colSpan={6} className="px-2 py-2 text-muted-foreground">No logs.</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                  {/* Pagination controls */}
                  <div className="flex items-center justify-between px-3 py-2 border-t text-xs">
                    <div>Page {logPage} of {totalLogPages}</div>
                    <div className="flex items-center gap-2">
                      <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 text-xs font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={logPage<=1} onClick={() => setLogPage((p) => Math.max(1, p-1))}>Prev</button>
                      <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 text-xs font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={logPage>=totalLogPages} onClick={() => setLogPage((p) => Math.min(totalLogPages, p+1))}>Next</button>
                    </div>
                  </div>
                </div>
              </TabPanel>

              <TabPanel className={`px-3 pb-3 pt-0 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}`}>
                {/* Running Tasks */}
                <div className="rounded-md border overflow-hidden bg-[hsl(var(--card))]">
                  <div className="px-3 py-2 border-b text-sm font-medium">Running Tasks</div>
                  <div className="overflow-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-[hsl(var(--muted))] text-gray-700 dark:text-gray-200">
                          <th className="text-left font-medium px-2 py-1">Task</th>
                          <th className="text-left font-medium px-2 py-1">Mode</th>
                          <th className="text-left font-medium px-2 py-1">Progress</th>
                          <th className="text-left font-medium px-2 py-1">Actions</th>
                        </tr>
                      </thead>
                      <tbody>
                        {!selectedId ? (
                          <tr><td colSpan={4} className="px-2 py-2 text-muted-foreground">Please select a datasource from the dropdown above.</td></tr>
                        ) : (
                          <>
                            {(tasksQ.data || []).filter((t) => t.inProgress).map((t, idx) => (
                              <tr key={t.id} className={`border-t ${idx % 2 === 1 ? 'bg-[hsl(var(--muted))]/20' : ''} hover:bg-[hsl(var(--muted))]/40`}>
                                <td className="px-2 py-1 font-mono">{t.destTableName}</td>
                                <td className="px-2 py-1">{t.mode}</td>
                                <td className="px-2 py-1">{typeof t.progressTotal === 'number' ? `${t.progressCurrent || 0}/${t.progressTotal}` : (t.progressCurrent || 0)}</td>
                                <td className="px-2 py-1">
                                  <button
                                    className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-300 px-2 py-1 text-xs font-medium hover:bg-red-100 dark:hover:bg-red-900/30 disabled:opacity-50 disabled:cursor-not-allowed"
                                    onClick={() => abortOne.mutate(t.id)}
                                  >Abort</button>
                                </td>
                              </tr>
                            ))}
                            {runningCount === 0 && (
                              <tr><td colSpan={4} className="px-2 py-2 text-muted-foreground">No running tasks.</td></tr>
                            )}
                          </>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              </TabPanel>

              <TabPanel className={`px-3 pb-3 pt-0 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}`}>
                {/* Scheduled Tasks + Jobs */}
                <div className="rounded-md border overflow-hidden bg-[hsl(var(--card))]">
                  <div className="px-3 py-2 border-b text-sm font-medium flex items-center justify-between">
                    <span>Scheduled Tasks</span>
                    <button
                      className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed"
                      onClick={() => refreshSchedules.mutate()}
                      disabled={refreshSchedules.isPending}
                    >
                      Refresh schedules
                    </button>
                  </div>
                  <div className="overflow-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-[hsl(var(--muted))] text-gray-700 dark:text-gray-200">
                          <th className="text-left font-medium px-2 py-1">Task</th>
                          <th className="text-left font-medium px-2 py-1">Cron</th>
                          <th className="text-left font-medium px-2 py-1">Next run</th>
                        </tr>
                      </thead>
                      <tbody>
                        {!selectedId ? (
                          <tr><td colSpan={3} className="px-2 py-2 text-muted-foreground">Please select a datasource from the dropdown above.</td></tr>
                        ) : (
                          <>
                            {(tasksQ.data || []).filter((t) => !!t.scheduleCron).map((t, idx) => {
                              const j = (jobsQ.data || []).find((x) => x.taskId === t.id)
                              return (
                                <tr key={t.id} className={`border-t ${idx % 2 === 1 ? 'bg-[hsl(var(--muted))]/20' : ''} hover:bg-[hsl(var(--muted))]/40`}>
                                  <td className="px-2 py-1 font-mono">{t.destTableName}</td>
                                  <td className="px-2 py-1">{t.scheduleCron}</td>
                                  <td className="px-2 py-1">{j?.nextRunAt ? new Date(j.nextRunAt).toLocaleString() : '—'}</td>
                                </tr>
                              )
                            })}
                            {scheduledCount === 0 && (
                              <tr><td colSpan={3} className="px-2 py-2 text-muted-foreground">No scheduled tasks.</td></tr>
                            )}
                          </>
                        )}
                      </tbody>
                    </table>
                  </div>
                </div>
              </TabPanel>
            </TabPanels>
          </TabGroup>
        </div>
      </Card>
    </div>
  )
}

export default function AdminSchedulesPage() {
  return (
    <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
      <AdminSchedulesInner />
    </Suspense>
  )
}
