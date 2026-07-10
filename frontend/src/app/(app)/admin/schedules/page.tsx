"use client"

import { Suspense, useEffect, useMemo, useRef, useState } from 'react'
import type React from 'react'
import { useRouter } from 'next/navigation'
import { useTranslations } from 'next-intl'
import { Card, Title, Text, TextInput, Badge, TabGroup, TabList, Tab, TabPanels, TabPanel, Select, SelectItem } from '@tremor/react'
import * as Popover from '@radix-ui/react-popover'
import { RiFocus2Line, RiArrowDownSLine, RiClipboardLine } from '@remixicon/react'
import { useAuth } from '@/components/providers/AuthProvider'
import { Button } from '@/components/ui'
import { Api, parseUtcDate, type DatasourceOut, type DatasourceDetailOut, type SyncTaskOut, type SyncTaskCreate, type SyncRunOut, type IntrospectResponse, type TablesOnlyResponse } from '@/lib/api'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { useProgressToast } from '@/components/providers/ProgressToastProvider'
import TablePreviewDialog from '@/components/builder/TablePreviewDialog'
import CustomQueryEditor from '@/components/builder/CustomQueryEditor'

export const dynamic = 'force-dynamic'

function AdminSchedulesInner() {
  const t = useTranslations('data')
  const tt = t // ponytail: alias for use inside .map((t) => ...) blocks where `t` is the row and shadows the translator
  const { user } = useAuth()
  const router = useRouter()
  const isAdmin = (user?.role || '').toLowerCase() === 'admin'
  const qc = useQueryClient()
  const { startMonitoring, show } = useProgressToast()
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [form, setForm] = useState<SyncTaskCreate>({ mode: 'snapshot', sourceTable: '', destTableName: '', scheduleCron: '0 2 * * *', enabled: true, selectColumns: [], customQuery: '' })
  const [destEdited, setDestEdited] = useState(false)
  // Tabs behavior to replicate My Dashboards animation
  const [tabIndex, setTabIndex] = useState(0)
  const prevTabIndex = useRef(0)
  const [slideDir, setSlideDir] = useState<'left' | 'right'>('right')
  const [previewOpen, setPreviewOpen] = useState(false)
  const [cronMode, setCronMode] = useState<'custom' | 'every_n_hours' | 'manual'>('custom')
  const [cronHoursInterval, setCronHoursInterval] = useState<number>(4)
  const [editingTaskId, setEditingTaskId] = useState<string | null>(null)
  const [pkSearch, setPkSearch] = useState('')
  const [seqSearch, setSeqSearch] = useState('')
  const [seqOpen, setSeqOpen] = useState(false)

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
    } else if (cronMode === 'manual') {
      setForm((f) => ({ ...f, scheduleCron: undefined }))
    }
  }, [cronMode, cronHoursInterval])

  const availableColumnMeta = useMemo(() => {
    const schName = (form.sourceSchema || '').trim()
    const tblName = (form.sourceTable || '').trim()
    const meta = introspectQ.data
    if (!meta || !tblName) return [] as Array<{ name: string; type?: string | null }>
    let cols: Array<{ name: string; type?: string | null }> = []
    for (const sch of meta.schemas || []) {
      if (schName && sch.name !== schName) continue
      for (const t of sch.tables || []) {
        if (t.name === tblName) {
          cols = t.columns || []
          break
        }
      }
      if (cols.length) break
    }
    return [...cols].sort((a, b) => a.name.localeCompare(b.name))
  }, [introspectQ.data, form.sourceSchema, form.sourceTable])

  const availableColumns = useMemo(
    () => availableColumnMeta.map((c) => c.name),
    [availableColumnMeta],
  )

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
      show(t('admin.schedules.toastTask'), t('admin.schedules.savedMsg'))
      setEditingTaskId(null)
      setDestEdited(false)
      setCronMode('custom')
      setForm({ mode: 'snapshot', sourceTable: '', destTableName: '', scheduleCron: '0 2 * * *', enabled: true, selectColumns: [], customQuery: '' })
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
      customQuery: t.customQuery || '',
    })
    const m = String(t.scheduleCron || '').match(/^0\s+\*\/(\d+)\s+\*\s+\*\s+\*$/)
    if (!t.scheduleCron) {
      setCronMode('manual')
    } else if (m) {
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
    setForm({ mode: 'snapshot', sourceTable: '', destTableName: '', scheduleCron: '0 2 * * *', enabled: true, selectColumns: [], customQuery: '' })
  }

  const runAll = useMutation({
    mutationFn: async () => Api.runSyncNow(selectedId as string, undefined, user?.id),
    onSuccess: (res) => {
      if (res?.ok === false) { show(t('admin.schedules.toastSync'), res?.message || t('admin.schedules.failedToStart')); return }
      startMonitoring(selectedId as string, user?.id)
      void qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] })
      void qc.invalidateQueries({ queryKey: ['sync-logs', selectedId] })
    },
    onError: (e: any) => show(t('admin.schedules.toastSync'), e?.message || t('admin.schedules.failedToStart')),
  })

  const runOne = useMutation({
    mutationFn: async (taskId: string) => Api.runSyncNow(selectedId as string, taskId, user?.id),
    onSuccess: (res) => {
      if (res?.ok === false) { show(t('admin.schedules.toastSync'), res?.message || t('admin.schedules.failedToStart')); return }
      if (selectedId) startMonitoring(selectedId, user?.id)
      void qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] })
      void qc.invalidateQueries({ queryKey: ['sync-logs', selectedId] })
    },
    onError: (e: any) => {
      const isTimeout = e?.message?.toLowerCase().includes('timeout') || e?.message?.toLowerCase().includes('timed out')
      if (isTimeout) {
        // Sync likely started but response timed out - start monitoring anyway
        show(t('admin.schedules.toastSyncStarted'), t('admin.schedules.syncTimedOutMsg'))
        if (selectedId) startMonitoring(selectedId, user?.id)
        void qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] })
        void qc.invalidateQueries({ queryKey: ['sync-logs', selectedId] })
      } else {
        show(t('admin.schedules.toastSync'), e?.message || t('admin.schedules.failedToStart'))
      }
    },
  })

  const abortOne = useMutation({
    mutationFn: async (taskId?: string) => Api.abortSync(selectedId as string, taskId, user?.id),
    onSuccess: (result) => {
      // Show detailed feedback based on what actually happened
      const { cancel_requested, force_reset, message } = result
      if (force_reset > 0 && cancel_requested > 0) {
        show(t('admin.schedules.toastSyncAbort'), t('admin.schedules.abortResetAndFlag', { resetCount: force_reset, flagCount: cancel_requested }))
      } else if (force_reset > 0) {
        show(t('admin.schedules.toastSyncAbort'), t('admin.schedules.abortForceReset', { count: force_reset }))
      } else if (cancel_requested > 0) {
        show(t('admin.schedules.toastSyncAbort'), t('admin.schedules.abortFlagged', { count: cancel_requested }))
      } else {
        show(t('admin.schedules.toastSyncAbort'), message || t('admin.schedules.noSyncsToAbort'))
      }
      void qc.invalidateQueries({ queryKey: ['sync-tasks', selectedId] })
      void qc.invalidateQueries({ queryKey: ['sync-logs', selectedId] })
    },
    onError: (e: any) => show(t('admin.schedules.toastSyncAbort'), e?.message || t('admin.schedules.abortFailed')),
  })

  const clearLogs = useMutation({
    mutationFn: async () => {
      if (!selectedId) return { deleted: 0 }
      return await Api.clearSyncLogs(selectedId as string, undefined, user?.id)
    },
    onSuccess: async (res) => {
      show(t('admin.schedules.toastLogs'), t('admin.schedules.clearedEntries', { count: res?.deleted ?? 0 }))
      await qc.invalidateQueries({ queryKey: ['sync-logs', selectedId] })
    },
    onError: (e: any) => show(t('admin.schedules.toastLogs'), e?.message || t('admin.schedules.failedToClearLogs')),
  })

  const runningCount = useMemo(() => (tasksQ.data || []).filter((t) => t.inProgress).length, [tasksQ.data])
  const scheduledCount = useMemo(() => (tasksQ.data || []).filter((t) => !!t.scheduleCron && t.enabled).length, [tasksQ.data])

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
          const sec = Math.max(0, Math.round(((parseUtcDate(r.finishedAt)?.getTime() ?? 0) - (parseUtcDate(r.startedAt)?.getTime() ?? 0)) / 1000))
          map[r.taskId] = sec
        }
      }
    }
    return map
  }, [logsData])

  // Map task details by taskId for logs display
  const taskDetailsById = useMemo(() => {
    const map: Record<string, { sourceTable: string; sourceSchema?: string; destTableName: string }> = {}
    for (const t of tasksData) {
      map[t.id] = {
        sourceTable: t.sourceTable,
        sourceSchema: t.sourceSchema || undefined,
        destTableName: t.destTableName,
      }
    }
    return map
  }, [tasksData])

  return (
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--card))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">{t('admin.schedules.pageTitle')}</Title>
            <div className="mt-1 flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
              <span className="opacity-80">{t('admin.schedules.pageSubtitle')}</span>
              {selectedId && (
                <>
                  <Badge color="emerald">{t('admin.schedules.runningBadge', { count: runningCount })}</Badge>
                  <Badge color="indigo">{t('admin.schedules.scheduledBadge', { count: scheduledCount })}</Badge>
                </>
              )}
            </div>
          </div>
          <div className="flex items-center gap-2">
            <div className="min-w-[220px] rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
              [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
              [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
              <Select value={selectedId || ''} onValueChange={(v) => setSelectedId(v || null)} placeholder={t('admin.schedules.selectDatasourcePlaceholder')} className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent">
                {(dsQ.data || []).map((d) => (
                  <SelectItem key={d.id} value={d.id}>{d.name} ({d.type})</SelectItem>
                ))}
              </Select>
            </div>
            <Button
              size="sm"
              variant="outline"
              disabled={!selectedId || runAll.isPending}
              onClick={() => runAll.mutate()}
            >
              {t('admin.schedules.runAllNow')}
            </Button>
            <Button
              size="sm"
              variant="outline"
              onClick={() => refreshSchedules.mutate()}
            >
              {t('admin.schedules.refreshSchedules')}
            </Button>
          </div>
        </div>
        <div className="p-4 space-y-4">
          <TabGroup index={tabIndex} onIndexChange={(i) => { setSlideDir(i > prevTabIndex.current ? 'left' : 'right'); prevTabIndex.current = i; setTabIndex(i) }}>
            <TabList className="px-3 py-1.5 border-b border-[hsl(var(--border))]">
              <Tab className="pb-2 px-1 mr-4 font-medium border-b-2 border-transparent transition-colors hover:border-[hsl(var(--primary)/0.4)] ui-selected:border-[hsl(var(--primary))]">
                <span className="text-gray-500 dark:text-gray-400 ui-selected:text-[hsl(var(--primary-deep))] ui-selected:dark:text-[hsl(var(--primary))]">{t('admin.schedules.tabCreateTasks')}</span>
              </Tab>
              <Tab className="pb-2 px-1 mr-4 font-medium border-b-2 border-transparent transition-colors hover:border-[hsl(var(--primary)/0.4)] ui-selected:border-[hsl(var(--primary))]">
                <span className="text-gray-500 dark:text-gray-400 ui-selected:text-[hsl(var(--primary-deep))] ui-selected:dark:text-[hsl(var(--primary))]">{t('admin.schedules.tabRunningTasks')}</span>
              </Tab>
              <Tab className="pb-2 px-1 mr-4 font-medium border-b-2 border-transparent transition-colors hover:border-[hsl(var(--primary)/0.4)] ui-selected:border-[hsl(var(--primary))]">
                <span className="text-gray-500 dark:text-gray-400 ui-selected:text-[hsl(var(--primary-deep))] ui-selected:dark:text-[hsl(var(--primary))]">{t('admin.schedules.tabScheduledTasks')}</span>
              </Tab>
            </TabList>
            <TabPanels className="pt-0">
              <TabPanel className={`px-3 pb-3 pt-0 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}`}>
                {/* Create Task */}
                <div className="rounded-md border bg-[hsl(var(--card))]">
                  <div className="px-3 py-2 border-b text-sm font-medium">{t('admin.schedules.createTask')}</div>
                  <div className="p-3 grid grid-cols-1 md:grid-cols-6 gap-3 text-sm">
                    <div>
                      <div className="text-xs text-muted-foreground">{t('admin.schedules.mode')}</div>
                      <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                        [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                        [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                        <Select value={form.mode} onValueChange={(v) => setForm((f) => ({ ...f, mode: (v as 'snapshot'|'sequence') }))} className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent">
                          <SelectItem value="snapshot">{t('admin.schedules.modeSnapshot')}</SelectItem>
                          <SelectItem value="sequence">{t('admin.schedules.modeSequence')}</SelectItem>
                        </Select>
                      </div>
                    </div>
                    {!isApiDs && (
                      <div>
                        <div className="text-xs text-muted-foreground">{t('admin.schedules.sourceSchemaLabel')}</div>
                        <div className="relative rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                          [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                          [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                          <Select
                            value={(form.sourceSchema || '') as string}
                            onValueChange={(v) => { /* schema fixed by default; we keep it readonly */ }}
                            placeholder={form.sourceSchema ? t('admin.schedules.schemaPrefix', { name: form.sourceSchema }) : (selectedId ? t('admin.schedules.determiningSchema') : t('admin.schedules.selectADatasource'))}
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
                        <div className="text-xs text-muted-foreground">{t('admin.schedules.sourceTable')}</div>
                        <div className="flex items-center gap-2">
                          <div className="relative flex-1 rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                            [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                            [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                            <Select
                              value={form.sourceTable}
                              onValueChange={(v) => { setForm((f) => ({ ...f, sourceTable: v || '', selectColumns: [] })); if (!destEdited && v) setForm((f) => ({ ...f, destTableName: v })); }}
                              placeholder={!form.sourceSchema ? t('admin.schedules.detectingSchema') : (tablesOnlyQ.isFetching ? t('admin.schedules.loadingTables') : t('admin.schedules.selectTable'))}
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
                            title={t('admin.schedules.previewTop1000')}
                            disabled={!selectedId || !form.sourceTable}
                            onClick={() => setPreviewOpen(true)}
                          >
                            <RiFocus2Line className="h-3.5 w-3.5" />
                          </button>
                        </div>
                      </div>
                    )}
                    <div>
                      <div className="text-xs text-muted-foreground">{t('admin.schedules.destTable')}</div>
                      <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                        [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none">
                        <TextInput className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent" value={form.destTableName} onChange={(e) => { setDestEdited(true); setForm((f) => ({ ...f, destTableName: e.target.value })) }} placeholder="orders_mat" />
                      </div>
                    </div>
                    {form.mode === 'sequence' && !isApiDs && (
                      <div>
                        <div className="text-xs text-muted-foreground">{t('admin.schedules.sequenceColumn')}</div>
                        <div className="relative rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]">
                          <Popover.Root open={seqOpen} onOpenChange={(o) => { setSeqOpen(o); if (!o) setSeqSearch('') }}>
                            <Popover.Trigger asChild>
                              <button
                                type="button"
                                className="w-full h-9 px-3 inline-flex items-center justify-between text-sm text-[hsl(var(--foreground))] disabled:opacity-50"
                                disabled={!form.sourceTable || introspectQ.isFetching || availableColumns.length === 0}
                              >
                                <span className="truncate">
                                  {introspectQ.isFetching ? t('admin.schedules.loadingColumns') : (form.sequenceColumn || (!form.sourceTable ? t('admin.schedules.selectTableFirst') : t('admin.schedules.selectSequenceColumn')))}
                                </span>
                                {form.sourceTable && introspectQ.isFetching
                                  ? <svg className="animate-spin h-3 w-3 opacity-70" viewBox="0 0 24 24"><circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"/><path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4a4 4 0 00-4 4H4z"/></svg>
                                  : <RiArrowDownSLine className="h-4 w-4 text-muted-foreground" aria-hidden="true" />}
                              </button>
                            </Popover.Trigger>
                            <Popover.Portal>
                              <Popover.Content side="bottom" align="start" className="z-50 w-64 rounded-lg border bg-card p-2 shadow-card">
                                <input
                                  autoFocus
                                  className="w-full mb-1.5 px-2 py-1 text-xs rounded border border-[hsl(var(--border))] bg-[hsl(var(--background))] outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]/40"
                                  placeholder={t('admin.schedules.searchColumns')}
                                  value={seqSearch}
                                  onChange={(e) => setSeqSearch(e.target.value)}
                                />
                                <div className="max-h-56 overflow-auto text-xs">
                                  {form.sequenceColumn && (
                                    <button
                                      className="w-full text-left px-2 py-1 rounded text-muted-foreground hover:bg-[hsl(var(--muted))] italic"
                                      onClick={() => { setForm((f) => ({ ...f, sequenceColumn: undefined })); setSeqOpen(false); setSeqSearch('') }}
                                    >{t('admin.schedules.clearSelection')}</button>
                                  )}
                                  {availableColumns
                                    .filter((c) => !seqSearch || c.toLowerCase().includes(seqSearch.toLowerCase()))
                                    .map((c) => (
                                      <button
                                        key={c}
                                        className={`w-full text-left px-2 py-1 rounded font-mono hover:bg-[hsl(var(--muted))] ${form.sequenceColumn === c ? 'bg-[hsl(var(--primary))]/10 font-semibold' : ''}`}
                                        onClick={() => { setForm((f) => ({ ...f, sequenceColumn: c })); setSeqOpen(false); setSeqSearch('') }}
                                      >{c}</button>
                                    ))}
                                  {availableColumns.filter((c) => !seqSearch || c.toLowerCase().includes(seqSearch.toLowerCase())).length === 0 && (
                                    <div className="text-muted-foreground px-2 py-1">{t('admin.schedules.noMatches')}</div>
                                  )}
                                </div>
                                <Popover.Arrow className="fill-[hsl(var(--card))]" />
                              </Popover.Content>
                            </Popover.Portal>
                          </Popover.Root>
                        </div>
                      </div>
                    )}
                    {!isApiDs && (
                      <div>
                        <div className="text-xs text-muted-foreground">{t('admin.schedules.pkColumns')}</div>
                        <div className="flex flex-col gap-1">
                          <div className="relative flex-1 min-w-0 rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]">
                            <Popover.Root onOpenChange={(o) => { if (!o) setPkSearch('') }}>
                              <Popover.Trigger asChild>
                                <button
                                  type="button"
                                  className="w-full h-9 px-3 inline-flex items-center justify-between text-sm text-[hsl(var(--foreground))]"
                                  disabled={!form.sourceTable || availableColumns.length === 0}
                                >
                                  <span className="truncate">{(form.pkColumns || []).length ? t('admin.schedules.nSelected', { count: (form.pkColumns || []).length }) : t('admin.schedules.selectColumns')}</span>
                                  <RiArrowDownSLine className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
                                </button>
                              </Popover.Trigger>
                              <Popover.Portal>
                                <Popover.Content side="bottom" align="start" className="z-50 w-64 rounded-lg border bg-card p-2 shadow-card">
                                  <input
                                    autoFocus
                                    className="w-full mb-1.5 px-2 py-1 text-xs rounded border border-[hsl(var(--border))] bg-[hsl(var(--background))] outline-none focus:ring-1 focus:ring-[hsl(var(--primary))]/40"
                                    placeholder={t('admin.schedules.searchColumns')}
                                    value={pkSearch}
                                    onChange={(e) => setPkSearch(e.target.value)}
                                  />
                                  <div className="max-h-56 overflow-auto text-xs">
                                    {availableColumns.length === 0 && (
                                      <div className="text-muted-foreground px-1 py-0.5">{t('admin.schedules.noColumnsSelectTable')}</div>
                                    )}
                                    {availableColumns
                                      .filter((c) => !pkSearch || c.toLowerCase().includes(pkSearch.toLowerCase()))
                                      .map((c) => (
                                        <label key={c} className="flex items-center gap-2 px-1 py-0.5 rounded hover:bg-[hsl(var(--muted))] cursor-pointer">
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
                                    {availableColumns.length > 0 && availableColumns.filter((c) => !pkSearch || c.toLowerCase().includes(pkSearch.toLowerCase())).length === 0 && (
                                      <div className="text-muted-foreground px-1 py-0.5">{t('admin.schedules.noMatches')}</div>
                                    )}
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
                      <div className="text-xs text-muted-foreground">{t('admin.schedules.batchSize')}</div>
                      <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                        [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none">
                        <TextInput className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent" value={(form.batchSize as any) || ''} onChange={(e) => setForm((f) => ({ ...f, batchSize: Number(e.target.value) || undefined }))} placeholder="10000" />
                      </div>
                    </div>
                    {/* Custom query — full width row */}
                    <div className="md:col-span-6">
                      <button
                        type="button"
                        className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground mb-1"
                        onClick={() => setForm((f) => ({ ...f, customQuery: f.customQuery === undefined ? '' : (f.customQuery === null ? '' : undefined) }))}
                      >
                        <RiArrowDownSLine className={`h-3.5 w-3.5 transition-transform ${form.customQuery !== undefined && form.customQuery !== null ? 'rotate-0' : '-rotate-90'}`} />
                        {t('admin.schedules.customBaseQuery')}
                        {form.customQuery ? <span className="ml-1 text-[10px] px-1 py-0 rounded bg-[hsl(var(--primary))]/15 text-[hsl(var(--primary))]">{t('admin.schedules.active')}</span> : null}
                      </button>
                      {form.customQuery !== undefined && form.customQuery !== null && (
                        <CustomQueryEditor
                          value={form.customQuery || ''}
                          onChange={(v) => setForm((f) => ({ ...f, customQuery: v }))}
                          columns={availableColumns}
                          columnMeta={availableColumnMeta}
                          sourceTable={form.sourceTable || ''}
                          sourceSchema={form.sourceSchema}
                        />
                      )}
                    </div>
                    <div className="md:col-span-2 md:col-start-1 flex flex-col justify-end gap-2">
                      <div>
                        <div className="text-xs text-muted-foreground">{t('admin.schedules.cronMode')}</div>
                        <div className="rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                          [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                          [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                          <Select value={cronMode} onValueChange={(v) => setCronMode((v as 'custom'|'every_n_hours'|'manual'))} className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent">
                            <SelectItem value="manual">{t('admin.schedules.cronManualOnly')}</SelectItem>
                            <SelectItem value="custom">{t('admin.schedules.cronCustom')}</SelectItem>
                            <SelectItem value="every_n_hours">{t('admin.schedules.cronEveryNHours')}</SelectItem>
                          </Select>
                        </div>
                      </div>
                      {cronMode === 'every_n_hours' && (
                        <div>
                          <div className="text-xs text-muted-foreground">{t('admin.schedules.everyNHoursLabel')}</div>
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
                      {cronMode === 'manual' ? (
                        <div className="rounded-md border border-dashed border-[hsl(var(--border))] px-3 py-2 text-xs text-muted-foreground">
                          {t('admin.schedules.noScheduleManual')}
                        </div>
                      ) : (
                        <div>
                          <div className="text-xs text-muted-foreground">{t('admin.schedules.cron')}</div>
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
                      )}
                    </div>
                    <div className="md:col-span-2 md:col-start-3 flex items-end" />
                    <div className="md:col-start-5 flex items-end md:justify-start">
                      <label className="inline-flex items-center gap-2 text-sm"><input type="checkbox" checked={!!form.enabled} onChange={(e) => setForm((f) => ({ ...f, enabled: e.target.checked }))} /> {t('admin.schedules.enabled')}</label>
                    </div>
                    <div className="md:col-start-6 flex items-end md:justify-start gap-2">
                      <Button
                        size="sm"
                        variant="primary"
                        disabled={!selectedId || (!!editingTaskId ? saveTask.isPending : createTask.isPending) || (!form.sourceTable && !form.customQuery) || !form.destTableName}
                        onClick={() => {
                          if (!selectedId || (!form.sourceTable && !form.customQuery) || !form.destTableName) return
                          if (editingTaskId) saveTask.mutate()
                          else createTask.mutate(form)
                        }}
                      >
                        {editingTaskId ? t('admin.schedules.save') : t('admin.schedules.create')}
                      </Button>
                      {editingTaskId && (
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={cancelEdit}
                          disabled={saveTask.isPending}
                        >{t('admin.schedules.cancel')}</Button>
                      )}
                    </div>
                  </div>
                  {/* Hints */}
                  <div className="px-3 pb-3">
                    <div className="mt-2 rounded-md border p-2 bg-[hsl(var(--background))]">
                      <div className="text-xs font-semibold mb-1">{t('admin.schedules.hints')}</div>
                      <ul className="text-[11px] space-y-1 text-muted-foreground list-disc pl-4">
                        <li>{t.rich('admin.schedules.hintSnapshot', { strong: (c) => <strong>{c}</strong> })}</li>
                        <li>{t.rich('admin.schedules.hintSequence', { strong: (c) => <strong>{c}</strong> })}</li>
                        <li>{t.rich('admin.schedules.hintPk', { strong: (c) => <strong>{c}</strong>, code: (c) => <code>{c}</code> })}</li>
                        <li>{t.rich('admin.schedules.hintCron', { strong: (c) => <strong>{c}</strong>, code: (c) => <code>{c}</code> })}</li>
                      </ul>
                    </div>
                  </div>
                </div>

                {/* Tasks table with pagination */}
                <div className="rounded-md border overflow-hidden mt-4 bg-[hsl(var(--card))]">
                  <div className="px-3 py-2 border-b text-sm font-medium flex items-center justify-between">
                    <span>{t('admin.schedules.tasks')}</span>
                    <div className="flex items-center gap-2 text-xs">
                      <span className="min-w-[140px] whitespace-nowrap">{t('admin.schedules.rowsPerPage')}</span>
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
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colMode')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colSource')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colDest')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colPks')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colSeq')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colBatch')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colCron')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colQuery')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colEnabled')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colLastRun')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colRows')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colDuration')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colStatus')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colActions')}</th>
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
                            <td className="px-2 py-1">{t.scheduleCron || <span className="text-muted-foreground text-[11px]">{tt('admin.schedules.manual')}</span>}</td>
                            <td className="px-2 py-1">
                              {t.customQuery ? (
                                <span className="inline-block text-[10px] px-1.5 py-0.5 rounded bg-[hsl(var(--primary))]/15 text-[hsl(var(--primary))] font-mono" title={t.customQuery}>{tt('admin.schedules.customBadge')}</span>
                              ) : '—'}
                            </td>
                            <td className="px-2 py-1">{t.enabled ? tt('admin.schedules.yes') : tt('admin.schedules.no')}</td>
                            <td className="px-2 py-1">{t.lastRunAt ? (parseUtcDate(t.lastRunAt)?.toLocaleString() ?? '—') : '—'}</td>
                            <td className="px-2 py-1">{typeof t.lastRowCount === 'number' ? t.lastRowCount.toLocaleString() : '—'}</td>
                            <td className="px-2 py-1">{typeof durationByTask[t.id] === 'number' ? durationByTask[t.id].toLocaleString() : '—'}</td>
                            <td className="px-2 py-1 whitespace-nowrap">{t.inProgress ? (
                              <span className="text-emerald-700 dark:text-emerald-300">{typeof t.progressCurrent === 'number' && typeof t.progressTotal === 'number' ? tt('admin.schedules.runningWithProgress', { current: t.progressCurrent, total: t.progressTotal }) : tt('admin.schedules.running')}</span>
                            ) : (t.error ? (
                              <span className="inline-flex items-center gap-1">
                                <span className="text-red-600">{tt('admin.schedules.error')}</span>
                                <button
                                  title={tt('admin.schedules.copyErrorToClipboard')}
                                  className="p-0.5 rounded hover:bg-[hsl(var(--muted))] text-red-500"
                                  onClick={() => navigator.clipboard.writeText(t.error ?? '')}
                                ><RiClipboardLine className="w-3.5 h-3.5" /></button>
                              </span>
                            ) : tt('admin.schedules.idle'))}
                            </td>
                            <td className="px-2 py-1">
                              <div className="flex items-center gap-2">
                                <Button
                                  size="sm"
                                  variant="outline"
                                  title={tt('admin.schedules.edit')}
                                  onClick={() => beginEdit(t)}
                                >{tt('admin.schedules.edit')}</Button>
                                {t.inProgress ? (
                                  <Button
                                    size="sm"
                                    variant="danger"
                                    title={tt('admin.schedules.abort')}
                                    onClick={() => abortOne.mutate(t.id)}
                                  >{tt('admin.schedules.abort')}</Button>
                                ) : (
                                  <Button
                                    size="sm"
                                    variant="outline"
                                    title={tt('admin.schedules.runNow')}
                                    onClick={() => runOne.mutate(t.id)}
                                  >{tt('admin.schedules.run')}</Button>
                                )}
                                <Button
                                  size="sm"
                                  variant="danger"
                                  title={tt('admin.schedules.delete')}
                                  onClick={() => deleteTask.mutate(t.id)}
                                >{tt('admin.schedules.delete')}</Button>
                              </div>
                            </td>
                          </tr>
                        ))}
                        {tasksData.length === 0 && (
                          <tr><td colSpan={14} className="px-2 py-2 text-muted-foreground">{t('admin.schedules.noTasksYet')}</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                  {/* Pagination controls */}
                  <div className="flex items-center justify-between px-3 py-2 border-t text-xs">
                    <div>{t('admin.schedules.pageOf', { page: taskPage, total: totalTaskPages })}</div>
                    <div className="flex items-center gap-2">
                      <Button size="sm" variant="outline" disabled={taskPage<=1} onClick={() => setTaskPage((p) => Math.max(1, p-1))}>{t('admin.schedules.prev')}</Button>
                      <Button size="sm" variant="outline" disabled={taskPage>=totalTaskPages} onClick={() => setTaskPage((p) => Math.min(totalTaskPages, p+1))}>{t('admin.schedules.next')}</Button>
                    </div>
                  </div>
                </div>

                {/* Logs with pagination */}
                <div className="rounded-md border overflow-hidden mt-4 bg-[hsl(var(--card))]">
                  <div className="px-3 py-2 border-b text-sm font-medium flex items-center justify-between">
                    <span>{t('admin.schedules.logs')}</span>
                    <div className="flex items-center gap-2 text-xs">
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={!selectedId || (logsData.length === 0) || clearLogs.isPending}
                        onClick={() => {
                          if (!selectedId) return
                          const ok = window.confirm(t('admin.schedules.clearLogsConfirm'))
                          if (!ok) return
                          clearLogs.mutate()
                        }}
                      >{t('admin.schedules.clearLogs')}</Button>
                      <span className="min-w-[140px] whitespace-nowrap">{t('admin.schedules.rowsPerPage')}</span>
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
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.logStarted')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colMode')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.logTask')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.logSourceTable')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.logDestTable')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colRows')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.logFinished')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.error')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {pagedLogs.map((r, idx) => {
                          const task = taskDetailsById[r.taskId]
                          const sourceTableDisplay = task ? (task.sourceSchema ? `${task.sourceSchema}.${task.sourceTable}` : task.sourceTable) : '—'
                          const destTableDisplay = task?.destTableName || '—'
                          return (
                            <tr key={r.id} className={`border-t ${idx % 2 === 1 ? 'bg-[hsl(var(--muted))]/20' : ''} hover:bg-[hsl(var(--muted))]/40`}>
                              <td className="px-2 py-1">{parseUtcDate(r.startedAt)?.toLocaleString() ?? '—'}</td>
                              <td className="px-2 py-1">{r.mode}</td>
                              <td className="px-2 py-1 font-mono">{r.taskId}</td>
                              <td className="px-2 py-1 font-mono">{sourceTableDisplay}</td>
                              <td className="px-2 py-1 font-mono">{destTableDisplay}</td>
                              <td className="px-2 py-1">{typeof r.rowCount === 'number' ? r.rowCount.toLocaleString() : '—'}</td>
                              <td className="px-2 py-1">{r.finishedAt ? (parseUtcDate(r.finishedAt)?.toLocaleString() ?? '—') : '—'}</td>
                              <td className="px-2 py-1 whitespace-nowrap">{r.error ? (
                                <span className="inline-flex items-center gap-1">
                                  <span className="text-red-600">{t('admin.schedules.error')}</span>
                                  <button
                                    title={t('admin.schedules.copyErrorToClipboard')}
                                    className="p-0.5 rounded hover:bg-[hsl(var(--muted))] text-red-500"
                                    onClick={() => navigator.clipboard.writeText(r.error ?? '')}
                                  ><RiClipboardLine className="w-3.5 h-3.5" /></button>
                                </span>
                              ) : '—'}</td>
                            </tr>
                          )
                        })}
                        {logsData.length === 0 && (
                          <tr><td colSpan={8} className="px-2 py-2 text-muted-foreground">{t('admin.schedules.noLogs')}</td></tr>
                        )}
                      </tbody>
                    </table>
                  </div>
                  {/* Pagination controls */}
                  <div className="flex items-center justify-between px-3 py-2 border-t text-xs">
                    <div>{t('admin.schedules.pageOf', { page: logPage, total: totalLogPages })}</div>
                    <div className="flex items-center gap-2">
                      <Button size="sm" variant="outline" disabled={logPage<=1} onClick={() => setLogPage((p) => Math.max(1, p-1))}>{t('admin.schedules.prev')}</Button>
                      <Button size="sm" variant="outline" disabled={logPage>=totalLogPages} onClick={() => setLogPage((p) => Math.min(totalLogPages, p+1))}>{t('admin.schedules.next')}</Button>
                    </div>
                  </div>
                </div>
              </TabPanel>

              <TabPanel className={`px-3 pb-3 pt-0 ${slideDir === 'left' ? 'anim-slide-left' : 'anim-slide-right'}`}>
                {/* Running Tasks */}
                <div className="rounded-md border overflow-hidden bg-[hsl(var(--card))]">
                  <div className="px-3 py-2 border-b text-sm font-medium">{t('admin.schedules.runningTasksTitle')}</div>
                  <div className="overflow-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-[hsl(var(--muted))] text-gray-700 dark:text-gray-200">
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colTask')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colMode')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colProgress')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colActions')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {!selectedId ? (
                          <tr><td colSpan={4} className="px-2 py-2 text-muted-foreground">{t('admin.schedules.selectDatasourcePrompt')}</td></tr>
                        ) : (
                          <>
                            {(tasksQ.data || []).filter((t) => t.inProgress).map((t, idx) => (
                              <tr key={t.id} className={`border-t ${idx % 2 === 1 ? 'bg-[hsl(var(--muted))]/20' : ''} hover:bg-[hsl(var(--muted))]/40`}>
                                <td className="px-2 py-1 font-mono">{t.destTableName}</td>
                                <td className="px-2 py-1">{t.mode}</td>
                                <td className="px-2 py-1">{typeof t.progressTotal === 'number' ? `${t.progressCurrent || 0}/${t.progressTotal}` : (t.progressCurrent || 0)}</td>
                                <td className="px-2 py-1">
                                  <Button
                                    size="sm"
                                    variant="danger"
                                    onClick={() => abortOne.mutate(t.id)}
                                  >{tt('admin.schedules.abort')}</Button>
                                </td>
                              </tr>
                            ))}
                            {runningCount === 0 && (
                              <tr><td colSpan={4} className="px-2 py-2 text-muted-foreground">{t('admin.schedules.noRunningTasks')}</td></tr>
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
                    <span>{t('admin.schedules.scheduledTasksTitle')}</span>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => refreshSchedules.mutate()}
                      disabled={refreshSchedules.isPending}
                    >
                      {t('admin.schedules.refreshSchedules')}
                    </Button>
                  </div>
                  <div className="overflow-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="bg-[hsl(var(--muted))] text-gray-700 dark:text-gray-200">
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colTask')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colCron')}</th>
                          <th className="text-left font-medium px-2 py-1">{t('admin.schedules.colNextRun')}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {!selectedId ? (
                          <tr><td colSpan={3} className="px-2 py-2 text-muted-foreground">{t('admin.schedules.selectDatasourcePrompt')}</td></tr>
                        ) : (
                          <>
                            {(tasksQ.data || []).filter((t) => !!t.scheduleCron).map((t, idx) => {
                              const j = (jobsQ.data || []).find((x) => x.taskId === t.id)
                              return (
                                <tr key={t.id} className={`border-t ${idx % 2 === 1 ? 'bg-[hsl(var(--muted))]/20' : ''} hover:bg-[hsl(var(--muted))]/40`}>
                                  <td className="px-2 py-1 font-mono">{t.destTableName}</td>
                                  <td className="px-2 py-1">{t.scheduleCron}</td>
                                  <td className="px-2 py-1">{j?.nextRunAt ? (parseUtcDate(j.nextRunAt)?.toLocaleString() ?? '—') : '—'}</td>
                                </tr>
                              )
                            })}
                            {scheduledCount === 0 && (
                              <tr><td colSpan={3} className="px-2 py-2 text-muted-foreground">{t('admin.schedules.noScheduledTasks')}</td></tr>
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
  const t = useTranslations('data')
  return (
    <Suspense fallback={<div className="p-3 text-sm">{t('admin.schedules.loading')}</div>}>
      <AdminSchedulesInner />
    </Suspense>
  )
}
