export type DatasourceOut = {
  id: string
  name: string
  type: string
  createdAt: string
  active?: boolean
  connectionUri?: string
  options?: Record<string, unknown>
}

export type DatasourceDetailOut = {
  id: string
  name: string
  type: string
  createdAt: string
  active?: boolean
  connectionUri?: string
  options?: Record<string, unknown>
}

export type SyncTaskOut = {
  id: string
  datasourceId: string
  sourceSchema?: string | null
  sourceTable: string
  destTableName: string
  mode: string
  pkColumns: string[]
  selectColumns?: string[] | null
  sequenceColumn?: string | null
  batchSize?: number | null
  scheduleCron?: string | null
  enabled: boolean
  groupKey: string
  createdAt: string
  lastRunAt?: string | null
  lastRowCount?: number | null
  inProgress: boolean
  error?: string | null
  progressCurrent?: number | null
  progressTotal?: number | null
}

export type LocalTableStat = {
  table: string
  rowCount?: number | null
  lastSyncAt?: string | null
  datasourceId?: string | null
  sourceSchema?: string | null
  sourceTable?: string | null
}

export type LocalStatsResponse = {
  enginePath: string
  fileSize: number
  tables: LocalTableStat[]
}

export type SyncTaskCreate = {
  datasourceId?: string
  sourceSchema?: string | null
  sourceTable: string
  destTableName: string
  mode: 'sequence' | 'snapshot'
  pkColumns?: string[]
  selectColumns?: string[] | null
  sequenceColumn?: string | null
  batchSize?: number | null
  scheduleCron?: string | null
  enabled?: boolean
}

export type SyncRunOut = {
  id: string
  taskId: string
  datasourceId: string
  mode: string
  startedAt: string
  finishedAt?: string | null
  rowCount?: number | null
  error?: string | null
}

// --- Alerts run history / evaluate ---
export type AlertRunOut = {
  id: string
  alertId: string
  startedAt: string
  finishedAt?: string | null
  status?: string | null
  message?: string | null
}

export type EvaluateResponse = {
  html: string
  kpi?: number | null
}

export type EvaluateV2Response = {
  emailHtml: string
  smsText?: string | null
  kpi?: number | null
  context: Record<string, any>
  humanSummary?: string | null
}

// --- Publish & Tokens (builder dialog) ---
export type EmbedTokenRowOut = {
  id: string
  token: string
  exp: number
  createdAt: string
  revokedAt?: string | null
}

export type ShareEntryOut = {
  userId: string
  permission: 'ro' | 'rw'
  createdAt: string
  userName?: string | null
  email?: string | null
}

// --- Export / Import types ---
export type DatasourceExportItem = {
  id: string
  name: string
  type: string
  connectionUri?: string | null
  options?: Record<string, any> | null
  userId?: string | null
  active?: boolean | null
  createdAt: string
  syncTasks?: SyncTaskExportItem[] | null
}

export type DatasourceImportItem = {
  id?: string | null
  name: string
  type: string
  connectionUri?: string | null
  options?: Record<string, any> | null
  userId?: string | null
  active?: boolean | null
  syncTasks?: SyncTaskImportItem[] | null
}

export type DatasourceImportRequest = { items: DatasourceImportItem[] }
export type DatasourceImportResponse = { created: number; updated: number; items: DatasourceOut[]; idMap?: Record<string, string> | null }

export type DashboardExportItem = {
  id: string
  name: string
  userId?: string | null
  definition: DashboardDefinition
  createdAt: string
  updatedAt?: string | null
}

export type DashboardExportResponse = {
  dashboards: DashboardExportItem[]
  datasources?: DatasourceExportItem[] | null
}

export type DashboardImportItem = {
  id?: string
  name: string
  userId?: string | null
  definition: DashboardDefinition
}

export type DashboardImportRequest = {
  dashboards: DashboardImportItem[]
  datasourceIdMap?: Record<string, string> | null
  datasources?: DatasourceImportItem[] | null
}

export type SyncTaskExportItem = {
  id: string
  datasourceId: string
  sourceSchema?: string | null
  sourceTable: string
  destTableName: string
  mode: string
  pkColumns: string[]
  selectColumns: string[]
  sequenceColumn?: string | null
  batchSize?: number | null
  scheduleCron?: string | null
  enabled: boolean
  groupKey: string
  createdAt: string
}

export type SyncTaskImportItem = {
  id?: string | null
  sourceSchema?: string | null
  sourceTable: string
  destTableName: string
  mode: string
  pkColumns?: string[] | null
  selectColumns?: string[] | null
  sequenceColumn?: string | null
  batchSize?: number | null
  scheduleCron?: string | null
  enabled?: boolean
}

function parseStatus(err: unknown): number | null {
  const msg = err instanceof Error ? err.message : String(err)
  const m = msg.match(/HTTP\s+(\d{3})/)
  return m ? Number(m[1]) : null
}

export type DatasourceCreate = {
  name: string
  type: string
  connectionUri?: string
  options?: Record<string, unknown>
  userId?: string
}

export type DatasourceUpdate = {
  name?: string
  type?: string
  connectionUri?: string
  options?: Record<string, unknown>
  active?: boolean
}

export type IntrospectResponse = {
  schemas: Array<{
    name: string
    tables: Array<{
      name: string
      columns: Array<{ name: string; type?: string | null }>
    }>
  }>
}

export type TablesOnlyResponse = {
  schemas: Array<{
    name: string
    tables: string[]
  }>
}

function getApiBase(): string {
  // Allow runtime override for admin via localStorage key 'api_base_override'
  try {
    if (typeof window !== 'undefined') {
      const o = localStorage.getItem('api_base_override')
      if (o && /^https?:\/\//i.test(o)) return o.replace(/\/$/, '')
    }
  } catch {}
  let base = (process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000/api').replace(/\/$/, '')
  // If the page is served over HTTPS but base is HTTP, fallback to same-origin '/api' to avoid mixed-content and CORS issues
  try {
    if (typeof window !== 'undefined') {
      const isHttps = (window.location.protocol || '').toLowerCase() === 'https:'
      if (isHttps && /^http:\/\//i.test(base)) {
        base = `${window.location.origin.replace(/\/$/, '')}/api`
      }
      // Also prefer same-origin API in Next dev (port 3000) to avoid cross-origin on LAN
      try {
        const origin = window.location.origin.replace(/\/$/, '')
        const host = window.location.host || ''
        // When running via Next dev on :3000, use the local proxy regardless of env base
        if (/(:|^)3000$/.test(host)) {
          const b = new URL(base)
          const sameHost = (`${b.protocol}//${b.host}` === origin)
          if (!sameHost) base = `${origin}/api`
        }
      } catch {}
    }
  } catch {}
  return base
}

const _widgetControllers: Map<string, AbortController> = new Map()
const _latestRequestIdByWidget: Record<string, string> = Object.create(null)
function _newRequestId(): string {
  try { return (Math.random().toString(36).slice(2) + Date.now().toString(36)).slice(0, 24) } catch { return String(Date.now()) }
}
export const RequestGuard = {
  getLatestRequestId(widgetId: string): string | undefined { return _latestRequestIdByWidget[widgetId] },
  isLatest(widgetId: string, requestId: string): boolean { return _latestRequestIdByWidget[widgetId] === requestId },
}

// Lightweight concurrency limiter for widget queries
const MAX_WIDGET_CONCURRENCY = Number(process.env.NEXT_PUBLIC_WIDGET_CONCURRENCY || 3)
let _widgetRunning = 0
const _widgetQueue: Array<() => void> = []
async function _acquireWidgetSlot(): Promise<() => void> {
  if (_widgetRunning < MAX_WIDGET_CONCURRENCY) {
    _widgetRunning += 1
    return () => {
      _widgetRunning = Math.max(0, _widgetRunning - 1)
      const next = _widgetQueue.shift(); if (next) next()
    }
  }
  return new Promise<() => void>((resolve) => {
    _widgetQueue.push(() => {
      _widgetRunning += 1
      resolve(() => {
        _widgetRunning = Math.max(0, _widgetRunning - 1)
        const next = _widgetQueue.shift(); if (next) next()
      })
    })
  })
}

async function http<T>(path: string, init?: RequestInit, timeoutMs = 15000): Promise<T> {
  const controller = new AbortController()
  const externalSignal: AbortSignal | undefined = (init as any)?.signal as AbortSignal | undefined
  const onExternalAbort = externalSignal ? () => { try { controller.abort((externalSignal as any).reason) } catch { controller.abort() } } : undefined
  if (externalSignal?.aborted) {
    controller.abort((externalSignal as any).reason)
  } else if (externalSignal && onExternalAbort) {
    try { externalSignal.addEventListener('abort', onExternalAbort, { once: true } as any) } catch { /* ignore */ }
  }
  const id = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const hasBody = !!init?.body
    const headers: Record<string, string> = { ...(init?.headers as any || {}) }
    if (hasBody) headers['Content-Type'] = headers['Content-Type'] || 'application/json'
    const { signal: _ignored, ...restInit } = (init || {}) as any
    // Auto-attach publicId/token for public view queries
    let finalPath = path
    try {
      if (typeof window !== 'undefined') {
        const isQueryEndpoint = /^\/?query(\/|$)/.test(finalPath.replace(/^\//, ''))
        const m = window.location.pathname.match(/^\/v\/([^/?#]+)/)
        if (isQueryEndpoint && m) {
          const pubId = m[1]
          const params = new URLSearchParams(window.location.search)
          const token = params.get('token') || ''
          const alreadyHasPub = /([?&])publicId=/.test(finalPath)
          if (!alreadyHasPub) {
            const sep = finalPath.includes('?') ? '&' : '?'
            finalPath = `${finalPath}${sep}publicId=${encodeURIComponent(pubId)}${token ? `&token=${encodeURIComponent(token)}` : ''}`
          }
        }
      }
    } catch {}
    let attempt = 0
    const max429 = 2
    let res: Response
    while (true) {
      res = await fetch(`${getApiBase()}${finalPath}`.replace(/\/$/, ''), {
        ...restInit,
        headers,
        cache: 'no-store',
        signal: controller.signal,
      })
      if (res.status === 429 && attempt < max429) {
        const raRaw = (res.headers.get('retry-after') || '').trim()
        const ra = Number.isFinite(Number(raRaw)) ? parseInt(raRaw, 10) : 0
        try { if (typeof window !== 'undefined') window.dispatchEvent(new CustomEvent('rate-limit', { detail: { path: finalPath, retryAfter: ra } } as any)) } catch {}
        const base = ra > 0 ? ra * 1000 : Math.min(2000 * (attempt + 1), 5000)
        const jitter = Math.floor(Math.random() * 250)
        await new Promise((r) => setTimeout(r, base + jitter))
        attempt += 1
        continue
      }
      break
    }
    if (!res.ok) {
      const body = await res.text().catch(() => '')
      throw new Error(`HTTP ${res.status}: ${body}`)
    }
    if (res.status === 204) {
      // No Content
      return undefined as unknown as T
    }
    // Some servers omit content-type or send invalid JSON; be tolerant
    const ct = (res.headers.get('content-type') || '').toLowerCase()
    if (ct.includes('application/json')) {
      try {
        return (await res.json()) as T
      } catch {
        // Fallback to text parsing if JSON parser fails
        const text = await res.text().catch(() => '')
        const trimmed = (text || '').trim()
        if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
          try { return JSON.parse(trimmed) as T } catch { /* fall through */ }
        }
        return undefined as unknown as T
      }
    } else {
      const text = await res.text().catch(() => '')
      const trimmed = (text || '').trim()
      if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
        try { return JSON.parse(trimmed) as T } catch { /* fall through */ }
      }
      // Non-JSON or empty body
      return undefined as unknown as T
    }
  } catch (e: any) {
    const aborted = controller.signal.aborted
    const extAborted = !!externalSignal?.aborted
    if (e?.name === 'AbortError') {
      if (extAborted) { throw e }
      throw new Error('Request timed out')
    }
    if (aborted && extAborted) {
      try { throw new DOMException('Aborted', 'AbortError') } catch { throw e }
    }
    throw e
  } finally {
    clearTimeout(id)
    if (externalSignal && onExternalAbort) {
      try { externalSignal.removeEventListener('abort', onExternalAbort as any) } catch { /* ignore */ }
    }
  }
}

export const Api = {
  listDatasources: (userId?: string, actorId?: string) => {
    const params: string[] = []
    if (userId) params.push(`userId=${encodeURIComponent(userId)}`)
    if (actorId) params.push(`actorId=${encodeURIComponent(actorId)}`)
    const qs = params.length ? `?${params.join('&')}` : ''
    return http<DatasourceOut[]>(`/datasources${qs}`).then((res: any) => Array.isArray(res) ? res : [])
  },
  listEmbedTokens: (dashId: string, actorId?: string) =>
    http<EmbedTokenRowOut[]>(`/dashboards/${encodeURIComponent(dashId)}/embed-tokens${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`),
  deleteEmbedToken: (dashId: string, tokenId: string, actorId?: string) =>
    http<{ deleted: number }>(`/dashboards/${encodeURIComponent(dashId)}/embed-tokens/${encodeURIComponent(tokenId)}${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'DELETE' }),
  listShares: (dashId: string, actorId?: string) =>
    http<ShareEntryOut[]>(`/dashboards/${encodeURIComponent(dashId)}/shares${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`),
  deleteShare: (dashId: string, userId: string, actorId?: string) =>
    http<{ deleted: number }>(`/dashboards/${encodeURIComponent(dashId)}/shares/${encodeURIComponent(userId)}${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'DELETE' }),
  getDatasource: (id: string, actorId?: string) => http<DatasourceDetailOut>(`/datasources/${id}${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`),
  createDatasource: (payload: DatasourceCreate) =>
    http<DatasourceOut>('/datasources', { method: 'POST', body: JSON.stringify(payload) }),
  updateDatasource: async (id: string, payload: DatasourceUpdate) => {
    try {
      return await http<DatasourceOut>(`/datasources/${id}`, { method: 'PATCH', body: JSON.stringify(payload) })
    } catch (e) {
      if (parseStatus(e) === 405) {
        // Fallback to PUT if PATCH not allowed
        return await http<DatasourceOut>(`/datasources/${id}`, { method: 'PUT', body: JSON.stringify(payload) })
      }
      throw e
    }
  },
  setDatasourceActive: async (id: string, active: boolean, actorId?: string) => {
    try {
      return await http<DatasourceOut>(`/datasources/${id}`, { method: 'PATCH', body: JSON.stringify({ active }) })
    } catch (e) {
      if (parseStatus(e) === 405) {
        // Try explicit endpoints if server uses action routes
        const path = (active ? `/datasources/${id}/activate` : `/datasources/${id}/deactivate`) + (actorId ? `?actorId=${encodeURIComponent(actorId)}` : '')
        return await http<DatasourceOut>(path, { method: 'POST' })
      }
      throw e
    }
  },
  getSyncStatus: (id: string, actorId?: string) => http<SyncTaskOut[]>(`/datasources/${id}/sync/status${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`),
  getLocalStats: (id: string) => http<LocalStatsResponse>(`/datasources/${id}/local/stats`),
  listSyncTasks: (id: string, actorId?: string) => http<SyncTaskOut[]>(`/datasources/${id}/sync-tasks${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`),
  createSyncTask: (id: string, payload: SyncTaskCreate, actorId?: string) => http<SyncTaskOut>(`/datasources/${id}/sync-tasks${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'POST', body: JSON.stringify(payload) }),
  updateSyncTask: (id: string, taskId: string, payload: SyncTaskCreate, actorId?: string) => http<SyncTaskOut>(`/datasources/${id}/sync-tasks/${taskId}${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'PATCH', body: JSON.stringify(payload) }),
  deleteSyncTask: (id: string, taskId: string, actorId?: string) => http<void>(`/datasources/${id}/sync-tasks/${taskId}${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'DELETE' }),
  runSyncNow: (id: string, taskId?: string, actorId?: string) => http<{ ok: boolean; count?: number; results?: any[]; message?: string }>(`/datasources/${id}/sync/run${taskId ? `?taskId=${encodeURIComponent(taskId)}` : ''}${(!taskId && actorId) ? `?actorId=${encodeURIComponent(actorId)}` : (taskId && actorId) ? `&actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'POST' }),
  abortSync: (id: string, taskId?: string, actorId?: string) =>
    http<{ ok: boolean; updated?: number }>(
      `/datasources/${id}/sync/abort${taskId ? `?taskId=${encodeURIComponent(taskId)}` : ''}${actorId ? `${taskId ? '&' : '?'}actorId=${encodeURIComponent(actorId)}` : ''}`,
      { method: 'POST' }
    ),
  getSyncLogs: (id: string, taskId?: string, limit: number = 50, actorId?: string) => http<SyncRunOut[]>(`/datasources/${id}/sync/logs${taskId ? `?taskId=${encodeURIComponent(taskId)}&limit=${limit}` : `?limit=${limit}`}${actorId ? `${taskId ? '&' : '&'}actorId=${encodeURIComponent(actorId)}` : ''}`),
  clearSyncLogs: (id: string, taskId?: string, actorId?: string) =>
    http<{ deleted: number }>(
      `/datasources/${id}/sync/logs${taskId ? `?taskId=${encodeURIComponent(taskId)}` : ''}${actorId ? `${taskId ? '&' : '?' }actorId=${encodeURIComponent(actorId)}` : ''}`,
      { method: 'DELETE' }
    ),
  dropLocalTable: (id: string, table: string, actorId?: string) => http<{ ok: boolean; dropped: number }>(`/datasources/${id}/local/drop-table${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'POST', body: JSON.stringify({ table }) }),
  // Export / Import: Datasources
  exportDatasources: (opts?: { ids?: string[]; includeSyncTasks?: boolean; actorId?: string }) => {
    const params: string[] = []
    if (Array.isArray(opts?.ids) && opts!.ids!.length) opts!.ids!.forEach((v) => params.push(`ids=${encodeURIComponent(v)}`))
    if (typeof opts?.includeSyncTasks === 'boolean') params.push(`includeSyncTasks=${opts.includeSyncTasks ? 'true' : 'false'}`)
    if (opts?.actorId) params.push(`actorId=${encodeURIComponent(opts.actorId)}`)
    const qs = params.length ? `?${params.join('&')}` : ''
    return http<DatasourceExportItem[]>(`/datasources/export${qs}`)
  },
  exportDatasource: (id: string, includeSyncTasks?: boolean, actorId?: string) => {
    const params: string[] = []
    if (typeof includeSyncTasks === 'boolean') params.push(`includeSyncTasks=${includeSyncTasks ? 'true' : 'false'}`)
    if (actorId) params.push(`actorId=${encodeURIComponent(actorId)}`)
    const qs = params.length ? `?${params.join('&')}` : ''
    return http<DatasourceExportItem>(`/datasources/${id}/export${qs}`)
  },
  importDatasources: (items: DatasourceImportItem[], actorId?: string) => http<DatasourceImportResponse>(`/datasources/import${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'POST', body: JSON.stringify({ items }) }),
  // Export / Import: Dashboards
  exportDashboards: (opts?: { userId?: string; ids?: string[]; includeDatasources?: boolean; includeSyncTasks?: boolean; actorId?: string }) => {
    const params: string[] = []
    if (opts?.userId) params.push(`userId=${encodeURIComponent(opts.userId)}`)
    if (Array.isArray(opts?.ids)) opts!.ids!.forEach((v) => params.push(`ids=${encodeURIComponent(v)}`))
    if (typeof opts?.includeDatasources === 'boolean') params.push(`includeDatasources=${opts.includeDatasources ? 'true' : 'false'}`)
    if (typeof opts?.includeSyncTasks === 'boolean') params.push(`includeSyncTasks=${opts.includeSyncTasks ? 'true' : 'false'}`)
    if (opts?.actorId) params.push(`actorId=${encodeURIComponent(opts.actorId)}`)
    const qs = params.length ? `?${params.join('&')}` : ''
    return http<DashboardExportResponse>(`/dashboards/export${qs}`)
  },
  exportDashboard: (id: string, includeDatasources?: boolean, includeSyncTasks?: boolean, actorId?: string) => {
    const params: string[] = []
    if (typeof includeDatasources === 'boolean') params.push(`includeDatasources=${includeDatasources ? 'true' : 'false'}`)
    if (typeof includeSyncTasks === 'boolean') params.push(`includeSyncTasks=${includeSyncTasks ? 'true' : 'false'}`)
    if (actorId) params.push(`actorId=${encodeURIComponent(actorId)}`)
    const qs = params.length ? `?${params.join('&')}` : ''
    return http<DashboardExportResponse>(`/dashboards/${id}/export${qs}`)
  },
  importDashboards: (payload: DashboardImportRequest, actorId?: string) => http<{ imported: number; items: DashboardOut[] }>(`/dashboards/import${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'POST', body: JSON.stringify(payload) }),
  // Admin: live metrics
  getMetricsLive: (actorId?: string) => http<any>(`/admin/metrics-live${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`),
  // Metrics: dashboards open/close pings
  dashboardsOpen: (kind: 'builder'|'public', dashboardId: string, sessionId: string) => (
    http<{ ok: boolean }>(`/metrics/dashboards/open`, { method: 'POST', body: JSON.stringify({ kind, dashboardId, sessionId }) })
  ),
  dashboardsClose: (kind: 'builder'|'public', dashboardId: string, sessionId: string) => (
    http<{ ok: boolean }>(`/metrics/dashboards/close`, { method: 'POST', body: JSON.stringify({ kind, dashboardId, sessionId }) })
  ),
  introspect: (id: string, signal?: AbortSignal) => http<IntrospectResponse>(`/datasources/${id}/schema`, { signal }, 60000),
  introspectLocal: (signal?: AbortSignal) => http<IntrospectResponse>(`/datasources/_local/schema`, { signal }, 60000),
  deleteDatasource: (id: string) => http<void>(`/datasources/${id}`, { method: 'DELETE' }),
  health: () => http<{ status: string; app: string; env: string }>(`/healthz`),
  query: (payload: QueryRequest) => http<QueryResponse>('/query', { method: 'POST', body: JSON.stringify(payload) }),
  pivot: (payload: PivotRequest) => http<QueryResponse>('/query/pivot', { method: 'POST', body: JSON.stringify(payload) }),
  // SQL preview generation may also be slow with complex transforms; extend timeout a bit
  pivotSql: (payload: PivotRequest) => http<PivotSqlResponse>('/query/pivot/sql', { method: 'POST', body: JSON.stringify(payload) }, 30000),
  queryForWidget: (widgetId: string, payload: QueryRequest, actorId?: string) => {
    const rid = _newRequestId()
    const prev = _widgetControllers.get(widgetId)
    if (prev) { try { prev.abort('superseded') } catch {} }
    const ctr = new AbortController()
    _widgetControllers.set(widgetId, ctr)
    _latestRequestIdByWidget[widgetId] = rid
    const path = actorId ? `/query?actorId=${encodeURIComponent(actorId)}` : '/query'
    const promise = (async () => {
      // If already aborted, fail fast
      if (ctr.signal.aborted) throw new DOMException('Aborted', 'AbortError')
      const release = await _acquireWidgetSlot()
      try {
        return await http<QueryResponse>(path, { method: 'POST', body: JSON.stringify({ ...payload, requestId: rid }), signal: ctr.signal })
      } finally {
        release()
      }
    })()
    return { requestId: rid, promise }
  },
  pivotForWidget: (widgetId: string, payload: PivotRequest, actorId?: string, signal?: AbortSignal) => {
    const rid = _newRequestId()
    _latestRequestIdByWidget[widgetId] = rid
    const path = actorId ? `/query/pivot?actorId=${encodeURIComponent(actorId)}` : '/query/pivot'
    // Pivot queries can be heavier than standard table queries; allow more time; rely on external signal for cancellation
    const promise = (async () => {
      const release = await _acquireWidgetSlot()
      try {
        return await http<QueryResponse>(path, { method: 'POST', body: JSON.stringify({ ...payload, requestId: rid as any }), signal }, 60000)
      } finally {
        release()
      }
    })()
    return { requestId: rid, promise }
  },
  testConnection: (dsn?: string) => http<TestConnResponse>('/test-connection', { method: 'POST', body: JSON.stringify({ dsn }) }),
  detectDb: (payload: DetectDbRequest) => http<DetectDbResponse>('/detect-db', { method: 'POST', body: JSON.stringify(payload) }),
  saveDashboard: (payload: DashboardSaveRequest) => http<DashboardOut>('/dashboards', { method: 'POST', body: JSON.stringify(payload) }),
  getDashboard: (id: string, actorId?: string) => http<DashboardOut>(`/dashboards/${id}${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`),
  deleteDashboard: (id: string, userId?: string) =>
    http<{ deleted: number }>(`/dashboards/${id}${userId ? `?userId=${encodeURIComponent(userId)}` : ''}`, { method: 'DELETE' }),
  getPublishStatus: (id: string) => http<PublishOut>(`/dashboards/${id}/publish`),
  publishDashboard: (id: string, userId?: string) =>
    http<PublishOut>(`/dashboards/${id}/publish${userId ? `?userId=${encodeURIComponent(userId)}` : ''}`, { method: 'POST' }),
  unpublishDashboard: (id: string, userId?: string) =>
    http<{ unpublished: number }>(`/dashboards/${id}/unpublish${userId ? `?userId=${encodeURIComponent(userId)}` : ''}`, { method: 'POST' }),
  setPublishToken: (id: string, token?: string, userId?: string) =>
    http<PublishOut>(`/dashboards/${id}/publish/token${userId ? `?userId=${encodeURIComponent(userId)}` : ''}`, { method: 'POST', body: JSON.stringify({ token }) }),
  getDashboardPublic: (publicId: string, token?: string, opts?: { et?: string }) => {
    const params: string[] = []
    if (token) params.push(`token=${encodeURIComponent(token)}`)
    if (opts?.et) params.push(`et=${encodeURIComponent(opts.et)}`)
    const qs = params.length ? `?${params.join('&')}` : ''
    return http<DashboardOut>(`/dashboards/public/${publicId}${qs}`)
  },
  createEmbedToken: (publicId: string, ttl: number, actorId?: string) =>
    http<{ token: string; exp: number }>(
      `/dashboards/public/${encodeURIComponent(publicId)}/embed-token?ttl=${encodeURIComponent(String(ttl))}${actorId ? `&actorId=${encodeURIComponent(actorId)}` : ''}`,
      { method: 'POST' }
    ),
  getBranding: () => http<BrandingOut>('/branding'),
  putAdminBranding: (payload: { orgName?: string; logoLight?: string; logoDark?: string; favicon?: string }, actorId?: string) =>
    http<BrandingOut>(`/admin/branding${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'PUT', body: JSON.stringify(payload) }),
  getSidebarCounts: (userId: string) => http<SidebarCountsResponse>(`/users/${encodeURIComponent(userId)}/counts`),
  // Engine pool disposal
  disposeDatasourceEngine: (id: string) => http<{ disposed: boolean; target: string; message?: string }>(`/datasources/${encodeURIComponent(id)}/engine/dispose`, { method: 'POST' }),
  disposeAllEngines: () => http<{ disposed: number }>(`/datasources/engines/dispose-all`, { method: 'POST' }),
  // Admin: global active DuckDB for sync
  duckActiveGet: (actorId?: string) => http<{ path: string }>(`/admin/duckdb/active${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`),
  duckActiveSet: (payload: { datasourceId?: string; path?: string }, actorId?: string) =>
    http<{ path: string }>(`/admin/duckdb/active${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'POST', body: JSON.stringify(payload) }),
  getNotifications: (userId: string) => http<NotificationOut[]>(`/users/${encodeURIComponent(userId)}/notifications`),
  listCollectionItems: (userId: string) => http<CollectionItemOut[]>(`/users/${encodeURIComponent(userId)}/collections/items`),
  // Datasource-level transforms (Advanced SQL Mode)
  getDatasourceTransforms: async (id: string) => {
    try {
      const res = await http<DatasourceTransforms>(`/datasources/${id}/transforms`)
      return (res ?? ({ customColumns: [], transforms: [], joins: [] } as DatasourceTransforms))
    } catch {
      return { customColumns: [], transforms: [], joins: [] } as DatasourceTransforms
    }
  },
  saveDatasourceTransforms: (id: string, payload: DatasourceTransforms) =>
    http<DatasourceTransforms>(`/datasources/${id}/transforms`, { method: 'PUT', body: JSON.stringify(payload) }),
  previewDatasourceTransforms: (id: string, payload: DatasourceTransforms & { source?: string; select?: string[]; limit?: number }) =>
    http<PreviewResponse>(`/datasources/${id}/transforms/preview`, { method: 'POST', body: JSON.stringify(payload) }),
  // Favorites endpoints (backend should implement). Safe to no-op if 404.
  listFavorites: async (userId: string) => {
    try { return await http<FavoriteOut[]>(`/users/${encodeURIComponent(userId)}/favorites`) } catch { return [] as FavoriteOut[] }
  },
  addFavorite: async (userId: string, dashboardId: string) => {
    try { return await http<{ ok: boolean }>(`/users/${encodeURIComponent(userId)}/favorites`, { method: 'POST', body: JSON.stringify({ dashboardId }) }) } catch { return { ok: false } }
  },
  removeFavorite: async (userId: string, dashboardId: string) => {
    try { return await http<{ ok: boolean }>(`/users/${encodeURIComponent(userId)}/favorites/${encodeURIComponent(dashboardId)}`, { method: 'DELETE' }) } catch { return { ok: false } }
  },
  listDashboards: (userId?: string, published?: boolean) => {
    const qs = new URLSearchParams()
    if (userId) qs.set('userId', userId)
    if (typeof published === 'boolean') qs.set('published', String(published))
    const q = qs.toString()
    return http<DashboardListItem[]>(`/dashboards${q ? `?${q}` : ''}`)
  },
  addToCollection: (userId: string, payload: AddToCollectionRequest) =>
    http<AddToCollectionResponse>(`/users/${encodeURIComponent(userId)}/collections`, {
      method: 'POST',
      body: JSON.stringify({ ...payload, userId }),
    }),
  removeFromCollection: (userId: string, collectionId: string, dashboardId: string) =>
    http<AddToCollectionResponse>(`/users/${encodeURIComponent(userId)}/collections/${encodeURIComponent(collectionId)}/${encodeURIComponent(dashboardId)}`, {
      method: 'DELETE',
    }),
  resolvePeriods: (payload: { mode: 'TD_YSTD' | 'TW_LW' | 'MONTH_LMONTH' | 'MTD_LMTD' | 'TY_LY' | 'YTD_LYTD' | 'TQ_LQ'; now?: string; tzOffsetMinutes?: number; weekStart?: 'sat' | 'sun' | 'mon' }) =>
    http<{ curStart: string; curEnd: string; prevStart: string; prevEnd: string }>(
      '/periods/resolve',
      { method: 'POST', body: JSON.stringify(payload) }
    ),
  periodTotals: (payload: { source: string; datasourceId?: string; y?: string; measure?: string; agg?: 'none'|'count'|'distinct'|'avg'|'sum'|'min'|'max'; dateField: string; start: string; end: string; where?: Record<string, unknown>; legend?: string | string[] }) =>
    http<{ total?: number; totals?: Record<string, number> }>('/query/period-totals', { method: 'POST', body: JSON.stringify(payload) }),
  periodTotalsBatch: (payload: { requests: Array<({ key?: string } & { source: string; datasourceId?: string; y?: string; measure?: string; agg?: 'none'|'count'|'distinct'|'avg'|'sum'|'min'|'max'; dateField: string; start: string; end: string; where?: Record<string, unknown>; legend?: string | string[]; weekStart?: 'sat'|'sun'|'mon' })> }) =>
    http<{ results: Record<string, { total?: number; totals?: Record<string, number> }> }>('/query/period-totals/batch', { method: 'POST', body: JSON.stringify(payload) }),
  periodTotalsCompare: (payload: { source: string; datasourceId?: string; y?: string; measure?: string; agg?: 'none'|'count'|'distinct'|'avg'|'sum'|'min'|'max'; dateField: string; start: string; end: string; prevStart: string; prevEnd: string; where?: Record<string, unknown>; legend?: string | string[]; weekStart?: 'sat'|'sun'|'mon' }) =>
    http<{ cur: { total?: number; totals?: Record<string, number> }; prev: { total?: number; totals?: Record<string, number> } }>('/query/period-totals/compare', { method: 'POST', body: JSON.stringify(payload) }),
  distinct: (payload: DistinctRequest) => http<DistinctResponse>('/query/distinct', { method: 'POST', body: JSON.stringify(payload) }),
  // --- Auth / Users ---
  signup: (payload: { name: string; email: string; password: string; role?: 'admin'|'user' }) =>
    http<UserOut>('/users/signup', { method: 'POST', body: JSON.stringify(payload) }),
  login: (payload: { email: string; password: string }) =>
    http<UserOut>('/users/login', { method: 'POST', body: JSON.stringify(payload) }),
  changePassword: (payload: { userId: string; oldPassword: string; newPassword: string }) =>
    http<{ ok: boolean }>('/users/change-password', { method: 'POST', body: JSON.stringify(payload) }),
  resetPassword: (payload: { email: string; newPassword: string }) =>
    http<{ ok: boolean }>('/users/reset-password', { method: 'POST', body: JSON.stringify(payload) }),
  // --- Admin ---
  adminListUsers: (actorId: string) => http<UserRowOut[]>(`/users/admin/list?actorId=${encodeURIComponent(actorId)}`),
  adminCreateUser: (actorId: string, payload: { name: string; email: string; password: string; role: 'admin'|'user' }) =>
    http<UserOut>(`/users/admin?actorId=${encodeURIComponent(actorId)}`, { method: 'POST', body: JSON.stringify(payload) }),
  adminSetActive: (actorId: string, userId: string, active: boolean) =>
    http<{ ok: boolean }>(`/users/admin/${encodeURIComponent(userId)}/set-active?actorId=${encodeURIComponent(actorId)}`, { method: 'POST', body: JSON.stringify({ active }) }),
  adminSetPassword: (actorId: string, userId: string, newPassword: string) =>
    http<{ ok: boolean }>(`/users/admin/${encodeURIComponent(userId)}/set-password?actorId=${encodeURIComponent(actorId)}`, { method: 'POST', body: JSON.stringify({ newPassword }) }),
  // Scheduler (admin)
  adminSchedulerJobs: (actorId: string) => http<Array<{ id: string; nextRunAt?: string | null; dsId?: string; taskId?: string }>>(`/admin/scheduler/jobs?actorId=${encodeURIComponent(actorId)}`),
  adminSchedulerRefresh: (actorId: string) => http<{ added: number; updated: number; removed: number; total: number }>(`/admin/scheduler/refresh?actorId=${encodeURIComponent(actorId)}`, { method: 'POST' }),
  // --- AI Assist ---
  aiDescribe: (payload: { provider: 'gemini'|'openai'|'mistral'|'anthropic'|'openrouter'; model: string; apiKey: string; baseUrl?: string; schema: { table: string; columns: Array<{ name: string; type?: string|null }> }; samples: any[] }) =>
    http<{ description: string }>(`/ai/describe`, { method: 'POST', body: JSON.stringify(payload) }),
  aiEnhance: (payload: { provider: 'gemini'|'openai'|'mistral'|'anthropic'|'openrouter'; model: string; apiKey: string; baseUrl?: string; schema: { table: string; columns: Array<{ name: string; type?: string|null }> }; description: string; userPrompt: string; allowedTypes: Array<WidgetConfig['type']|WidgetConfig['chartType']> }) =>
    http<{ enhancedPrompt: string }>(`/ai/enhance`, { method: 'POST', body: JSON.stringify(payload) }),
  aiPlan: (payload: { provider: 'gemini'|'openai'|'mistral'|'anthropic'|'openrouter'; model: string; apiKey: string; baseUrl?: string; schema: { table: string; columns: Array<{ name: string; type?: string|null }> }; samples: any[]; prompt: string; customColumns?: string[]; targetType?: 'chart'|'table'|'kpi' }, signal?: AbortSignal) =>
    http<{ plan: string }>(`/ai/plan`, { method: 'POST', body: JSON.stringify(payload), signal }),
  aiSuggest: (payload: { provider: 'gemini'|'openai'|'mistral'|'anthropic'|'openrouter'; model: string; apiKey: string; baseUrl?: string; schema: { table: string; columns: Array<{ name: string; type?: string|null }> }; samples: any[]; prompt: string; plan?: string; variantOffset?: number; targetType?: 'chart'|'table'|'kpi' }, signal?: AbortSignal) =>
    http<{ variants: WidgetConfig[] }>(`/ai/suggest`, { method: 'POST', body: JSON.stringify(payload), signal }),
  getAiConfig: () => http<{ provider?: string|null; model?: string|null; hasKey: boolean; baseUrl?: string|null }>(`/ai/config`),
  putAiConfig: (payload: { provider?: string; model?: string; apiKey?: string; baseUrl?: string }, actorId?: string) =>
    http<{ ok: boolean }>(`/ai/config${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`, { method: 'PUT', body: JSON.stringify(payload) }),
  // Lightweight schema endpoints (tables only)
  tablesOnly: (id: string, signal?: AbortSignal) => http<TablesOnlyResponse>(`/datasources/${id}/tables`, { signal }, 30000),
  tablesOnlyLocal: (signal?: AbortSignal) => http<TablesOnlyResponse>(`/datasources/_local/tables`, { signal }, 30000),
  // --- Alerts & Notifications ---
  listAlerts: () => http<AlertOut[]>(`/alerts`),
  createAlert: (payload: AlertCreate) => http<AlertOut>(`/alerts`, { method: 'POST', body: JSON.stringify(payload) }),
  getAlert: (id: string) => http<AlertOut>(`/alerts/${encodeURIComponent(id)}`),
  updateAlert: (id: string, payload: AlertCreate) => http<AlertOut>(`/alerts/${encodeURIComponent(id)}`, { method: 'PUT', body: JSON.stringify(payload) }),
  deleteAlert: (id: string) => http<{ deleted: number }>(`/alerts/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  runAlertNow: (id: string) => http<{ ok: boolean; message?: string }>(`/alerts/${encodeURIComponent(id)}/run`, { method: 'POST' }, 60000),
  listAlertRuns: (id: string, limit: number = 50) => http<AlertRunOut[]>(`/alerts/${encodeURIComponent(id)}/runs?limit=${encodeURIComponent(String(limit))}`),
  evaluateAlert: (payload: { name: string; dashboardId?: string | null; config: AlertConfig }, actorId?: string) =>
    http<EvaluateResponse>(`/alerts/evaluate${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`,
      { method: 'POST', body: JSON.stringify(payload) }, 60000),
  evaluateAlertV2: (payload: { name: string; dashboardId?: string | null; config: AlertConfig }, actorId?: string) =>
    http<EvaluateV2Response>(`/alerts/evaluate-v2${actorId ? `?actorId=${encodeURIComponent(actorId)}` : ''}`,
      { method: 'POST', body: JSON.stringify(payload) }, 60000),
  getEmailConfig: () => http<EmailConfigPayload>(`/alerts/config/email`),
  putEmailConfig: (payload: EmailConfigPayload) => http<{ ok: boolean }>(`/alerts/config/email`, { method: 'PUT', body: JSON.stringify(payload) }),
  getSmsConfigHadara: () => http<SmsConfigPayload>(`/alerts/config/sms/hadara`),
  putSmsConfigHadara: (payload: SmsConfigPayload) => http<{ ok: boolean }>(`/alerts/config/sms/hadara`, { method: 'PUT', body: JSON.stringify(payload) }),
  testEmail: (payload: TestEmailPayload) => http<{ ok: boolean }>(`/alerts/test-email`, { method: 'POST', body: JSON.stringify(payload) }),
  testSms: (payload: TestSmsPayload) => http<{ ok: boolean }>(`/alerts/test-sms`, { method: 'POST', body: JSON.stringify(payload) }),
  // --- Updates ---
  updatesVersion: () => http<{ backend?: string|null; frontend?: string|null }>(`/updates/version`),
  updatesCheck: (component: 'backend'|'frontend'|'both' = 'backend') => http<{ enabled: boolean; component: string; currentVersion?: string|null; latestVersion?: string|null; updateType?: 'auto'|'manual'; requiresMigrations?: boolean; releaseNotes?: string|null; manifestUrl?: string|null }>(`/updates/check?component=${encodeURIComponent(component)}`),
  updatesApply: (component: 'backend'|'frontend', actorId: string) => http<{ ok: boolean; component: string; version: string; stagedPath?: string; requiresRestart: boolean }>(`/updates/apply?component=${encodeURIComponent(component)}&actorId=${encodeURIComponent(actorId)}`, { method: 'POST' }),
  // --- Contacts Manager ---
  listContacts: (opts?: { search?: string; tags?: string[]; active?: boolean; page?: number; pageSize?: number }) => {
    const qs = new URLSearchParams()
    if (opts?.search) qs.set('search', opts.search)
    if (Array.isArray(opts?.tags) && opts!.tags!.length) qs.set('tags', opts!.tags!.join(','))
    if (typeof opts?.active === 'boolean') qs.set('active', String(opts.active))
    if (typeof opts?.page === 'number') qs.set('page', String(opts.page))
    if (typeof opts?.pageSize === 'number') qs.set('pageSize', String(opts.pageSize))
    const q = qs.toString()
    return http<{ items: ContactOut[]; total: number; page: number; pageSize: number }>(`/contacts${q ? `?${q}` : ''}`)
  },
  createContact: (payload: ContactIn) => http<ContactOut>(`/contacts`, { method: 'POST', body: JSON.stringify(payload) }),
  updateContact: (id: string, payload: ContactIn) => http<ContactOut>(`/contacts/${encodeURIComponent(id)}`, { method: 'PUT', body: JSON.stringify(payload) }),
  deactivateContact: (id: string, active: boolean) => http<{ ok: boolean; active: boolean }>(`/contacts/${encodeURIComponent(id)}/deactivate?active=${encodeURIComponent(String(active))}`, { method: 'POST' }),
  deleteContact: (id: string) => http<{ deleted: number }>(`/contacts/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  importContacts: (items: ContactIn[]) => http<{ imported: number; total: number }>(`/contacts/import`, { method: 'POST', body: JSON.stringify({ items }) }),
  exportContacts: (ids?: string[]) => {
    const qs = (ids && ids.length) ? `?ids=${ids.map(encodeURIComponent).join(',')}` : ''
    return http<{ items: ContactOut[] }>(`/contacts/export${qs}`)
  },
  contactsSendEmail: (payload: { ids?: string[]; emails?: string[]; tags?: string[]; subject: string; html: string; rateLimitPerMinute?: number; queue?: boolean; notifyEmail?: string }) =>
    http<{ ok: boolean; count: number; queued?: boolean; jobId?: string; success?: number; failed?: number; failures?: Array<{ recipient: string; error: string }> }>(`/contacts/send-email`, { method: 'POST', body: JSON.stringify(payload) }),
  contactsSendSms: (payload: { ids?: string[]; numbers?: string[]; tags?: string[]; message: string; rateLimitPerMinute?: number; queue?: boolean; notifyEmail?: string }) =>
    http<{ ok: boolean; count: number; queued?: boolean; jobId?: string; success?: number; failed?: number; failures?: Array<{ recipient: string; error: string }> }>(`/contacts/send-sms`, { method: 'POST', body: JSON.stringify(payload) }),
  contactsSendStatus: (jobId: string) =>
    http<{ type: 'email'|'sms'; total: number; success: number; failed: number; failures: Array<{ recipient: string; error: string }>; done: boolean }>(`/contacts/send-status?jobId=${encodeURIComponent(jobId)}`),
}

export type QueryRequest = {
  sql: string
  datasourceId?: string
  limit?: number
  offset?: number
  params?: Record<string, unknown>
  includeTotal?: boolean
  requestId?: string
  // Prefer routing queries to local DuckDB when available (true), or force remote (false). Omit to use server default.
  preferLocalDuck?: boolean
  // Optional hint for local table name when using SQL mode
  preferLocalTable?: string
}

export type QueryResponse = {
  columns: string[]
  rows: Array<Array<unknown>>
  elapsedMs?: number
  totalRows?: number
}

// Distinct endpoint types
export type DistinctRequest = {
  source: string
  field: string
  where?: Record<string, unknown>
  datasourceId?: string
}
export type DistinctResponse = { values: any[] }

export type TestConnResponse = {
  ok: boolean
  error?: string | null
}

export type DetectDbRequest = {
  dsn?: string
  host?: string
  port?: number
  user?: string
  password?: string
  db?: string
  driver?: string
  timeout?: number
}

export type DetectDbResponse = {
  ok: boolean
  detected?: 'postgres' | 'mysql' | 'mssql' | 'oracle' | 'duckdb' | 'sqlite' | 'unknown' | null
  method?: 'dsn' | 'version_query' | 'handshake' | 'port_hint' | null
  versionString?: string | null
  candidates?: string[] | null
  error?: string | null
}

import type { WidgetConfig } from '@/types/widgets'
import type { DatasourceTransforms, PreviewResponse } from '@/lib/dsl'
export type ContactIn = { name: string; email?: string | null; phone?: string | null; tags?: string[] | null; userId?: string | null }
export type ContactOut = { id: string; name: string; email?: string | null; phone?: string | null; tags: string[]; active: boolean; createdAt: string }

export type RGLLayout = {
  i: string
  x: number
  y: number
  w: number
  h: number
  minW?: number
  minH?: number
  maxW?: number
  maxH?: number
  static?: boolean
}

export type DashboardDefinition = {
  layout: RGLLayout[]
  widgets: Record<string, WidgetConfig>
  options?: Record<string, any>
}

export type DashboardSaveRequest = {
  id?: string
  name: string
  userId?: string
  definition: DashboardDefinition
}

export type DashboardOut = {
  id: string
  name: string
  userId?: string
  createdAt: string
  definition: DashboardDefinition
}

export type PublishOut = {
  publicId: string
  protected: boolean
}

// Ibis QuerySpec types
export type QuerySpec = {
  source: string
  select?: string[]
  where?: Record<string, unknown>
  limit?: number
  offset?: number
  // Optional chart semantics for aggregated queries
  x?: string
  y?: string
  agg?: 'none' | 'count' | 'distinct' | 'avg' | 'sum' | 'min' | 'max'
  groupBy?: 'none' | 'day' | 'week' | 'month' | 'quarter' | 'year'
  weekStart?: 'mon' | 'sun'
  measure?: string
  legend?: string
  // Ranking hints for Top-N
  orderBy?: 'x' | 'value'
  order?: 'asc' | 'desc'
  series?: Array<{
    label?: string
    x?: string
    y?: string
    agg?: 'none' | 'count' | 'distinct' | 'avg' | 'sum' | 'min' | 'max'
    groupBy?: 'none' | 'day' | 'week' | 'month' | 'quarter' | 'year'
    measure?: string
    colorToken?: 1 | 2 | 3 | 4 | 5
  }>
}

export type QuerySpecRequest = {
  spec: QuerySpec
  datasourceId?: string
  limit?: number
  offset?: number
  includeTotal?: boolean
  // Prefer routing to local DuckDB when available (omit to use server default)
  preferLocalDuck?: boolean
}

export const QueryApi = {
  querySpec: async (payload: QuerySpecRequest) => {
    try {
      const specAny: any = payload?.spec
      // Require a spec and a non-empty source; otherwise, return empty result to avoid 422 spam
      if (!specAny || typeof specAny.source !== 'string' || !specAny.source.trim()) {
        return { columns: [], rows: [], elapsedMs: 0, totalRows: 0 } as QueryResponse
      }
      // Normalize optional fields that sometimes arrive as arrays
      const spec: any = { ...specAny }
      if (Array.isArray(spec.x)) spec.x = spec.x[0]
      if (Array.isArray(spec.legend)) spec.legend = spec.legend[0]
      // Forward the normalized payload
      return await http<QueryResponse>('/query/spec', { method: 'POST', body: JSON.stringify({ ...payload, spec }) })
    } catch (e) {
      throw e
    }
  },
}

export type PivotRequest = {
  source: string
  rows?: string[]
  cols?: string[]
  valueField?: string | null
  aggregator?: 'count' | 'sum' | 'avg' | 'min' | 'max' | 'distinct'
  where?: Record<string, unknown>
  datasourceId?: string
  limit?: number
  widgetId?: string
}

export type PivotSqlResponse = { sql?: string }

export type BrandingOut = {
  fonts: Record<string, string>
  palette: Record<string, string>
  orgName?: string
  logoLight?: string
  logoDark?: string
  favicon?: string
}

export type SidebarCountsResponse = {
  dashboardCount: number
  datasourceCount: number
  sharedCount: number
  collectionCount: number
}

export type DashboardListItem = {
  id: string
  name: string
  userId?: string | null
  createdAt: string
  updatedAt?: string | null
  published: boolean
  publicId?: string | null
  widgetsCount: number
  tablesCount: number
  datasourceCount: number
}

export type AddToCollectionRequest = {
  userId: string
  dashboardId: string
  collectionName?: string
  sharedBy?: string
  dashboardName?: string
  permission?: 'ro' | 'rw'
}

export type AddToCollectionResponse = {
  collectionId: string
  collectionName: string
  added: boolean
  totalItems: number
  collectionsCount: number
  collectionItemsCount: number
}

export type NotificationOut = {
  id: string
  message: string
  createdAt: string
}

export type CollectionItemOut = {
  collectionId: string
  dashboardId: string
  name: string
  ownerId?: string
  ownerName?: string
  permission?: 'ro' | 'rw'
  addedAt: string
  published: boolean
  publicId?: string | null
}

// --- Alerts & Notifications types ---
export type AlertConfig = {
  datasourceId?: string
  triggers: Array<Record<string, any>>
  actions: Array<Record<string, any>>
  render?: Record<string, any>
  template?: string
}

export type AlertCreate = {
  name: string
  kind?: 'alert' | 'notification'
  widgetId?: string
  dashboardId?: string
  enabled?: boolean
  config: AlertConfig
}

export type AlertOut = {
  id: string
  name: string
  kind: 'alert' | 'notification'
  widgetId?: string | null
  dashboardId?: string | null
  enabled: boolean
  config: AlertConfig
  lastRunAt?: string | null
  lastStatus?: string | null
}

export type EmailConfigPayload = {
  host?: string
  port?: number
  username?: string
  password?: string
  fromName?: string
  fromEmail?: string
  useTls?: boolean
  baseTemplateHtml?: string
  logoUrl?: string
}

export type SmsConfigPayload = {
  apiKey?: string
  defaultSender?: string
}

export type TestEmailPayload = { to: string[]; subject?: string; html?: string }
export type TestSmsPayload = { to: string[]; message: string }

// Favorites (per-user) — backend should implement these endpoints.
// We keep types minimal so UI can render a list; caller can fetch full dashboard details if needed.
export type FavoriteOut = {
  userId: string
  dashboardId: string
  name?: string
  updatedAt?: string
}

export type UserOut = {
  id: string
  name: string
  email: string
  role: 'admin' | 'user'
}

export type UserRowOut = UserOut & {
  active: boolean
  createdAt: string
}
