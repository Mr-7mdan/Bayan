"use client"

import { useEffect, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import Form from '@rjsf/core'
import validator from '@rjsf/validator-ajv8'
import type { IChangeEvent } from '@rjsf/core'
import type { RJSFSchema, UiSchema, ValidatorType } from '@rjsf/utils'
import { Api, DatasourceCreate, DatasourceOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'

const schema: RJSFSchema = {
  title: 'Add Datasource',
  type: 'object',
  required: ['name', 'type'],
  properties: {
    name: { type: 'string', title: 'Name' },
    type: {
      type: 'string',
      title: 'Type',
      enum: ['duckdb', 'postgres', 'supabase', 'mysql', 'mssql', 'sqlite', 'bigquery', 'snowflake', 'api'],
      default: 'duckdb',
    },
    connectionUri: {
      type: 'string',
      title: 'Connection URI (DSN)',
      description:
        'Examples: duckdb:///.data/local.duckdb | postgresql+psycopg://user:pass@host:5432/db | mysql+pymysql://user:pass@host:3306/db?charset=utf8mb4 | mssql+pyodbc://user:pass@host:1433/db?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes | mssql+pytds://user:pass@host:1433/db',
    },
    // userId is bound from AuthProvider; not exposed in the form
    options: {
      type: 'object',
      title: 'Options (optional)',
      additionalProperties: true,
    },
  },
}

const uiSchema: UiSchema<FormData, RJSFSchema> = {
  type: {
    'ui:enumNames': ['DuckDB', 'Postgres', 'Supabase', 'MySQL', 'MS SQL Server', 'SQLite', 'BigQuery', 'Snowflake', 'API (HTTP)'],
  },
} as const

type FormData = {
  name?: string
  type?: string
  connectionUri?: string
  options?: Record<string, unknown>
  userId?: string
}

export default function AddDatasourceDialog({
  open,
  onOpenChange,
  onCreated,
  mode = 'create',
  initial,
  onSavedAction,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onCreated?: (ds: DatasourceOut) => void
  mode?: 'create' | 'edit'
  initial?: Partial<DatasourceOut & { connectionUri?: string; options?: Record<string, unknown> }>
  onSavedAction?: (ds: DatasourceOut) => void
}) {
  const { user } = useAuth()
  const [formData, setFormData] = useState<FormData>({ type: 'duckdb' })
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState<null | { ok: boolean; error?: string }>(null)
  const [detecting, setDetecting] = useState(false)
  const [detectResult, setDetectResult] = useState<null | { detected?: string | null; method?: string | null; versionString?: string | null; candidates?: string[] | null; error?: string | null }>(null)
  const [saving, setSaving] = useState(false)
  const [saveError, setSaveError] = useState<string | null>(null)
  // Sync settings (high-level rules)
  const [syncMaxConcurrent, setSyncMaxConcurrent] = useState<number>(1)
  const [blackout, setBlackout] = useState<Array<{ start: string; end: string }>>([])
  // API datasource config (stored under options.api)
  const [apiCfg, setApiCfg] = useState<{
    endpoint: string
    method: 'GET' | 'POST'
    headers: Array<{ key: string; value: string }>
    query: Array<{ key: string; value: string }>
    body: string
    placeholders: Array<{ name: string; kind: 'static' | 'date'; value?: string; format?: string }>
    destTable: string
    jsonRoot: string
    gapFill: { enabled: boolean; keyFields: string; dateField: string }
    sequence: { enabled: boolean; mode: 'date-range'; startParam: string; endParam: string; windowDays: number; dateField: string }
    auth: { type: 'none'|'bearer'|'apiKeyHeader'|'apiKeyQuery'|'basic'|'oauth2_client_credentials'; header?: string; valueTemplate?: string; token?: string; tokenTemplate?: string; param?: string; username?: string; password?: string; tokenUrl?: string; clientId?: string; clientSecret?: string; scope?: string }
    pagination: { type: 'none'|'page'|'cursor'; pageParam?: string; pageSizeParam?: string; pageSize?: number; pageStart?: number; maxPages?: number; cursorParam?: string; nextCursorPath?: string }
    writeMode: 'append'|'replace'
  }>({
    endpoint: '',
    method: 'GET',
    headers: [],
    query: [],
    body: '',
    placeholders: [],
    destTable: '',
    jsonRoot: '',
    gapFill: { enabled: true, keyFields: 'base,quote', dateField: 'rate_date' },
    sequence: { enabled: true, mode: 'date-range', startParam: 'start', endParam: 'end', windowDays: 7, dateField: 'rate_date' },
    auth: { type: 'none' },
    pagination: { type: 'none', pageParam: 'page', pageSizeParam: 'limit', pageSize: 100, pageStart: 1, maxPages: 10, cursorParam: 'cursor', nextCursorPath: '' },
    writeMode: 'append',
  })

  // DSN Builders
  const [pg, setPg] = useState<{ host: string; port: number; db: string; user: string; password: string }>({
    host: 'localhost',
    port: 5432,
    db: 'postgres',
    user: 'postgres',
    password: '',
  })
  const [sb, setSb] = useState<{ projectUrl: string; serviceRoleKey: string }>({ projectUrl: '', serviceRoleKey: '' })
  const [mysql, setMysql] = useState<{ host: string; port: number; db: string; user: string; password: string; charset: string; connectTimeout: number }>({
    host: 'localhost',
    port: 3306,
    db: 'mysql',
    user: 'root',
    password: '',
    charset: 'utf8mb4',
    connectTimeout: 5,
  })
  const [mssql, setMssql] = useState<{
    host: string
    port: number
    db: string
    user: string
    password: string
    driverType: 'pyodbc' | 'pytds'
    driver: 'ODBC Driver 18 for SQL Server' | 'ODBC Driver 17 for SQL Server'
    encrypt: boolean
    trustServerCertificate: boolean
    connectTimeout: number
    connectRetryCount: number
    connectRetryInterval: number
    pooling: boolean
    useOdbcConnect: boolean
  }>({
    host: 'localhost',
    port: 1433,
    db: 'master',
    user: 'sa',
    password: '',
    driverType: 'pyodbc',
    driver: 'ODBC Driver 18 for SQL Server',
    encrypt: false,
    trustServerCertificate: true,
    connectTimeout: 30,
    connectRetryCount: 3,
    connectRetryInterval: 10,
    pooling: false,
    useOdbcConnect: false,
  })
  const [defaultPivotPar, setDefaultPivotPar] = useState<number>(2)

  // Auto-build DSN when builder fields change
  useEffect(() => {
    if (formData.type === 'postgres') {
      const dsn = `postgresql+psycopg://${encodeURIComponent(pg.user)}:${encodeURIComponent(pg.password)}@${pg.host}:${pg.port}/${pg.db}`
      if (dsn !== formData.connectionUri) setFormData((fd) => ({ ...(fd || {}), connectionUri: dsn }))
    } else if (formData.type === 'supabase') {
      // Derive project ref from URL like https://<ref>.supabase.co
      let ref = ''
      try {
        const u = new URL(sb.projectUrl)
        const parts = u.hostname.split('.')
        ref = parts[0] || ''
      } catch {
        // ignore parse errors
      }
      if (ref) {
        const host = `db.${ref}.supabase.co`
        const dsn = `postgresql+psycopg://${encodeURIComponent('postgres')}:${encodeURIComponent(sb.serviceRoleKey)}@${host}:5432/postgres`
        if (dsn !== formData.connectionUri) setFormData((fd) => ({ ...(fd || {}), connectionUri: dsn }))
      }
    } else if (formData.type === 'mysql') {
      const qp: string[] = []
      if (mysql.charset) qp.push(`charset=${encodeURIComponent(mysql.charset)}`)
      if (mysql.connectTimeout) qp.push(`connect_timeout=${encodeURIComponent(String(mysql.connectTimeout))}`)
      const q = qp.length ? `?${qp.join('&')}` : ''
      const dsn = `mysql+pymysql://${encodeURIComponent(mysql.user)}:${encodeURIComponent(mysql.password)}@${mysql.host}:${mysql.port}/${mysql.db}${q}`
      if (dsn !== formData.connectionUri) setFormData((fd) => ({ ...(fd || {}), connectionUri: dsn }))
    } else if (formData.type === 'mssql') {
      if (mssql.driverType === 'pytds') {
        const dsn = `mssql+pytds://${encodeURIComponent(mssql.user)}:${encodeURIComponent(mssql.password)}@${mssql.host}:${mssql.port}/${mssql.db}`
        if (dsn !== formData.connectionUri) setFormData((fd) => ({ ...(fd || {}), connectionUri: dsn }))
      } else {
        // pyodbc: Two modes: standard URL params or odbc_connect encoded string
        if (mssql.useOdbcConnect) {
          const server = `${mssql.host}${mssql.port ? ',' + mssql.port : ''}`
          const parts = [
            `DRIVER={${mssql.driver}}`,
            `SERVER=${server}`,
            `DATABASE=${mssql.db}`,
            `UID=${mssql.user}`,
            `PWD=${mssql.password}`,
            `Encrypt=${mssql.encrypt ? 'yes' : 'no'}`,
            `TrustServerCertificate=${mssql.trustServerCertificate ? 'yes' : 'no'}`,
            `LoginTimeout=${mssql.connectTimeout}`,
            `ConnectRetryCount=${mssql.connectRetryCount}`,
            `ConnectRetryInterval=${mssql.connectRetryInterval}`,
            `Pooling=${mssql.pooling ? 'Yes' : 'No'}`,
          ]
          const odbc = encodeURIComponent(parts.join(';'))
          const dsn = `mssql+pyodbc:///?odbc_connect=${odbc}`
          if (dsn !== formData.connectionUri) setFormData((fd) => ({ ...(fd || {}), connectionUri: dsn }))
        } else {
          const qp: string[] = []
          qp.push(`driver=${encodeURIComponent(mssql.driver)}`)
          qp.push(`Encrypt=${mssql.encrypt ? 'yes' : 'no'}`)
          qp.push(`TrustServerCertificate=${mssql.trustServerCertificate ? 'yes' : 'no'}`)
          if (mssql.connectTimeout) qp.push(`LoginTimeout=${encodeURIComponent(String(mssql.connectTimeout))}`)
          if (typeof mssql.connectRetryCount === 'number') qp.push(`ConnectRetryCount=${encodeURIComponent(String(mssql.connectRetryCount))}`)
          if (typeof mssql.connectRetryInterval === 'number') qp.push(`ConnectRetryInterval=${encodeURIComponent(String(mssql.connectRetryInterval))}`)
          qp.push(`Pooling=${mssql.pooling ? 'True' : 'False'}`)
          const q = qp.length ? `?${qp.join('&')}` : ''
          const dsn = `mssql+pyodbc://${encodeURIComponent(mssql.user)}:${encodeURIComponent(mssql.password)}@${mssql.host}:${mssql.port}/${mssql.db}${q}`
          if (dsn !== formData.connectionUri) setFormData((fd) => ({ ...(fd || {}), connectionUri: dsn }))
        }
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [formData.type, pg.host, pg.port, pg.db, pg.user, pg.password, sb.projectUrl, sb.serviceRoleKey, mysql.host, mysql.port, mysql.db, mysql.user, mysql.password, mysql.charset, mysql.connectTimeout, mssql.host, mssql.port, mssql.db, mssql.user, mssql.password, mssql.driverType, mssql.driver, mssql.encrypt, mssql.trustServerCertificate, mssql.connectTimeout, mssql.useOdbcConnect, mssql.connectRetryCount, mssql.connectRetryInterval, mssql.pooling])

  // Prefill fields when opening in edit mode
  useEffect(() => {
    if (!open) return
    if (mode === 'edit' && initial) {
      // Seed top-level form
      setFormData({
        name: initial.name,
        type: initial.type,
        connectionUri: initial.connectionUri,
        options: initial.options,
        userId: user?.id,
      })
      try { const dpo = Number(((initial.options as any)?.defaultPivotParallelism ?? NaN)); if (Number.isFinite(dpo)) setDefaultPivotPar(Math.max(1, Math.min(8, Math.floor(dpo)))) } catch {}
      try { if (initial.connectionUri) prefillFromDsn(initial.connectionUri, (initial.type as any) || undefined) } catch {}
      try {
        const o = (initial.options || {}) as any
        if (o && o.api) setApiCfg((prev) => ({ ...prev, ...o.api }))
      } catch {}
    } else if (mode === 'create') {
      setFormData((fd) => ({ ...(fd || {}), type: (fd?.type as any) || 'duckdb' }))
      setDefaultPivotPar(2)
      setApiCfg((prev) => ({ ...prev }))
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, mode, initial?.id])

  // Parse DSN and prefill builder fields
  function prefillFromDsn(s: string, t?: 'postgres'|'supabase'|'mysql'|'mssql'|'duckdb'|'sqlite'|'bigquery'|'snowflake') {
    const d = (s || '').trim()
    const low = d.toLowerCase()
    const asUrl = (proto: string, url: string) => { try { return new URL(url.replace(proto, 'http://')) } catch { return null } }
    if ((t === 'postgres' || low.startsWith('postgres'))) {
      const u = asUrl('postgresql+psycopg://', d) || asUrl('postgres://', d)
      if (u) {
        setPg({ host: u.hostname || 'localhost', port: Number(u.port || 5432), db: decodeURIComponent((u.pathname||'').replace(/^\//,'')) || 'postgres', user: decodeURIComponent(u.username||'postgres'), password: decodeURIComponent(u.password||'') })
      }
      // Supabase heuristic
      const m = low.match(/@db\.([a-z0-9-]+)\.supabase\.co:\d+\/postgres/)
      if (m) {
        try {
          const u2 = asUrl('postgresql+psycopg://', d)
          setSb({ projectUrl: `https://${m[1]}.supabase.co`, serviceRoleKey: u2 ? decodeURIComponent(u2.password || '') : '' })
        } catch {}
      }
    } else if ((t === 'mysql' || low.startsWith('mysql'))) {
      const u = asUrl('mysql+pymysql://', d) || asUrl('mysql://', d)
      if (u) {
        setMysql({ host: u.hostname || 'localhost', port: Number(u.port || 3306), db: decodeURIComponent((u.pathname||'').replace(/^\//,'')) || 'mysql', user: decodeURIComponent(u.username||'root'), password: decodeURIComponent(u.password||''), charset: mysql.charset, connectTimeout: mysql.connectTimeout })
      }
    } else if ((t === 'mssql' || low.includes('mssql'))) {
      if (low.startsWith('mssql+pytds://')) {
        const u = asUrl('mssql+pytds://', d)
        if (u) setMssql((prev) => ({ ...prev, driverType: 'pytds', host: u.hostname || 'localhost', port: Number(u.port || 1433), db: decodeURIComponent((u.pathname||'').replace(/^\//,'')) || 'master', user: decodeURIComponent(u.username||''), password: decodeURIComponent(u.password||''), useOdbcConnect: false }))
      } else if (low.startsWith('mssql+pyodbc:///?odbc_connect=')) {
        try {
          const enc = d.split('odbc_connect=')[1] || ''
          const cs = decodeURIComponent(enc)
          const parts = Object.fromEntries(cs.split(';').filter(Boolean).map(kv => { const [k,...rest] = kv.split('='); return [k.trim().toLowerCase(), (rest.join('=')||'').trim()] })) as Record<string,string>
          const server = parts['server'] || ''
          const [host, portStr] = server.split(',')
          setMssql((prev) => ({ ...prev, driverType: 'pyodbc', useOdbcConnect: true, driver: (parts['driver']||prev.driver) as any, host: host || prev.host, port: Number(portStr || prev.port || 1433), db: parts['database'] || prev.db, user: parts['uid'] || prev.user, password: parts['pwd'] || prev.password, encrypt: String(parts['encrypt']||'').toLowerCase()==='yes', trustServerCertificate: String(parts['trustservercertificate']||'').toLowerCase()==='yes', connectTimeout: Number(parts['logintimeout'] || prev.connectTimeout || 30), connectRetryCount: Number(parts['connectretrycount'] || prev.connectRetryCount || 3), connectRetryInterval: Number(parts['connectretryinterval'] || prev.connectRetryInterval || 10), pooling: String(parts['pooling']||'').toLowerCase() === 'yes' }))
        } catch {}
      } else if (low.startsWith('mssql+pyodbc://')) {
        const u = asUrl('mssql+pyodbc://', d)
        if (u) {
          const q = new URLSearchParams(u.search || '')
          setMssql((prev) => ({ ...prev, driverType: 'pyodbc', useOdbcConnect: false, driver: (q.get('driver') || prev.driver) as any, host: u.hostname || prev.host, port: Number(u.port || prev.port || 1433), db: decodeURIComponent((u.pathname||'').replace(/^\//,'')) || prev.db, user: decodeURIComponent(u.username||prev.user), password: decodeURIComponent(u.password||prev.password), encrypt: (q.get('Encrypt')||'').toLowerCase()==='yes', trustServerCertificate: (q.get('TrustServerCertificate')||'').toLowerCase()==='yes', connectTimeout: Number(q.get('LoginTimeout') || prev.connectTimeout || 30), connectRetryCount: Number(q.get('ConnectRetryCount') || prev.connectRetryCount || 3), connectRetryInterval: Number(q.get('ConnectRetryInterval') || prev.connectRetryInterval || 10), pooling: String(q.get('Pooling')||'').toLowerCase() === 'true' }))
        }
      }
    }
  }

  const onTest = async () => {
    setTesting(true)
    setTestResult(null)
    try {
      const res = await Api.testConnection(formData.connectionUri || undefined)
      setTestResult({ ok: res.ok, error: res.error || undefined })
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : 'Failed'
      setTestResult({ ok: false, error: message })
    } finally {
      setTesting(false)
    }
  }
  const onDetect = async () => {
    setDetecting(true)
    setDetectResult(null)
    try {
      const dsn = (formData.connectionUri || '').trim() || undefined
      let payload: Parameters<typeof Api.detectDb>[0]
      if (dsn) {
        payload = { dsn }
      } else if (formData.type === 'postgres') {
        payload = { host: pg.host, port: Number(pg.port) || 5432 }
      } else if (formData.type === 'mysql') {
        payload = { host: mysql.host, port: Number(mysql.port) || 3306 }
      } else if (formData.type === 'mssql') {
        payload = { host: mssql.host, port: Number(mssql.port) || 1433 }
      } else if (formData.type === 'supabase') {
        let host = ''
        try {
          const u = new URL(sb.projectUrl)
          const ref = (u.hostname.split('.')[0] || '').trim()
          if (ref) host = `db.${ref}.supabase.co`
        } catch {}
        payload = host ? { host, port: 5432 } : { host: '' }
      } else {
        payload = { host: '' }
      }
      const res = await Api.detectDb(payload)
      setDetectResult({
        detected: res.detected ?? null,
        method: res.method ?? null,
        versionString: res.versionString ?? null,
        candidates: res.candidates ?? null,
        error: res.error ?? null,
      })
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : 'Detection failed'
      setDetectResult({ error: message })
    } finally {
      setDetecting(false)
    }
  }
  const handleSubmit = async (e: IChangeEvent<FormData, RJSFSchema>) => {
    setSaving(true)
    setSaveError(null)
    try {
      const fd = (e.formData || {}) as FormData
      const sync = {
        maxConcurrentQueries: Math.max(1, Number(syncMaxConcurrent) || 1),
        blackoutDaily: blackout.filter((w) => (w.start || '').includes(':') && (w.end || '').includes(':')),
      }
      const baseOptions = { ...(fd.options || {}), sync, defaultPivotParallelism: Math.max(1, Math.min(8, Number(defaultPivotPar) || 2)) }
      const finalOptions = (fd.type === 'api') ? { ...baseOptions, api: apiCfg } : baseOptions
      if (mode === 'edit' && initial?.id) {
        const updated = await Api.updateDatasource(initial.id, {
          name: fd.name,
          type: fd.type,
          connectionUri: fd.connectionUri || undefined,
          options: finalOptions,
        })
        onSavedAction?.(updated)
        onOpenChange(false)
      } else {
        const payload: DatasourceCreate = {
          name: fd.name!,
          type: fd.type!,
          connectionUri: fd.connectionUri || undefined,
          options: finalOptions,
          userId: user?.id,
        }
        const created = await Api.createDatasource(payload)
        onCreated?.(created)
        onOpenChange(false)
        setFormData({ type: 'duckdb' })
        setSyncMaxConcurrent(1)
        setBlackout([])
        setDefaultPivotPar(2)
      }
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Failed to create datasource'
      setSaveError(message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChange}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/20" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[520px] -translate-x-1/2 -translate-y-1/2 rounded-lg border bg-card p-4 shadow-card ds-dialog">
          <Dialog.Title className="text-lg font-semibold">Add Datasource</Dialog.Title>
          <Dialog.Description className="text-sm text-muted-foreground mb-3">
            Provide a name, type, and optional connection URI. You can test the connection before saving.
          </Dialog.Description>

          <div className="max-h-[60vh] overflow-auto pr-1">
            <Form<FormData, RJSFSchema>
              schema={schema}
              formData={formData}
              validator={validator as ValidatorType<FormData, RJSFSchema, Record<string, unknown>>}
              uiSchema={uiSchema}
              onChange={(e) => setFormData(e.formData as FormData)}
              onSubmit={(e) => { void handleSubmit(e as IChangeEvent<FormData, RJSFSchema>) }}
            >
              {formData.type !== 'api' && (
                <div className="flex items-center justify-between gap-2 mt-2">
                  <button
                    type="button"
                    className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted"
                    onClick={onTest}
                    disabled={testing}
                  >
                    {testing ? (
                      <span className="inline-flex items-center gap-1">
                        <span className="h-3 w-3 border border-[hsl(var(--border))] border-l-transparent rounded-full animate-spin" aria-hidden="true"></span>
                        <span>Testing…</span>
                      </span>
                    ) : (
                      'Test Connection'
                    )}
                  </button>
                  <button
                    type="button"
                    className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted"
                    onClick={onDetect}
                    disabled={detecting}
                  >
                    {detecting ? (
                      <span className="inline-flex items-center gap-1">
                        <span className="h-3 w-3 border border-[hsl(var(--border))] border-l-transparent rounded-full animate-spin" aria-hidden="true"></span>
                        <span>Detecting…</span>
                      </span>
                    ) : (
                      'Detect Server'
                    )}
                  </button>
                  <div className="flex flex-col items-end gap-1 text-xs min-w-[160px]">
                    {testResult && (
                      testResult.ok ? (
                        <span className="text-green-700">Connection OK</span>
                      ) : (
                        <span className="text-red-700">{testResult.error || 'Failed'}</span>
                      )
                    )}
                    {detectResult && (
                      detectResult.error ? (
                        <span className="text-red-700">Detect: {detectResult.error}</span>
                      ) : (
                        <div className="text-right">
                          <div className="text-muted-foreground">
                            Detected: <b>{detectResult.detected || (detectResult.candidates?.join('/') || 'unknown')}</b>{' '}
                            {detectResult.method ? `(${detectResult.method})` : ''}
                          </div>
                          {(() => {
                            const selected = (formData.type || '').toLowerCase()
                            const det = (detectResult.detected || '').toLowerCase()
                            const cand = (detectResult.candidates || []).map((x) => (x || '').toLowerCase())
                            const match = det ? (det === selected) : cand.includes(selected)
                            return (
                              <div className={match ? 'text-green-700' : 'text-amber-700'}>
                                {match ? 'Matches selected type' : `Does not match selected: ${formData.type || ''}`}
                              </div>
                            )
                          })()}
                        </div>
                      )
                    )}
                  </div>
                </div>
              )}
              {saveError && <div className="text-sm text-red-700 mt-2">{saveError}</div>}

              {/* Sync Settings */}
              <div className="mt-4 rounded-md border p-3 space-y-3">
                <div className="text-sm font-medium">Sync Settings</div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <label className="text-xs">Maximum concurrent queries
                    <input
                      className="w-full px-2 py-1 rounded-md border bg-background"
                      type="number"
                      min={1}
                      value={syncMaxConcurrent}
                      onChange={(e) => setSyncMaxConcurrent(Math.max(1, Number(e.target.value) || 1))}
                    />
                  </label>
                  <div className="text-xs text-muted-foreground md:mt-6">Limits simultaneous sync tasks for this datasource.</div>
                </div>
                <div>
                  <div className="text-xs font-medium mb-1">Blackout periods (no sync allowed)</div>
                  <div className="space-y-2">
                    {blackout.map((w, idx) => (
                      <div key={idx} className="flex items-center gap-2">
                        <label className="text-xs">Start
                          <input
                            className="ml-1 w-[110px] px-2 py-1 rounded-md border bg-background"
                            type="time"
                            value={w.start || ''}
                            onChange={(e) => setBlackout((arr) => arr.map((x, i) => i === idx ? { ...x, start: e.target.value } : x))}
                          />
                        </label>
                        <label className="text-xs">End
                          <input
                            className="ml-1 w-[110px] px-2 py-1 rounded-md border bg-background"
                            type="time"
                            value={w.end || ''}
                            onChange={(e) => setBlackout((arr) => arr.map((x, i) => i === idx ? { ...x, end: e.target.value } : x))}
                          />
                        </label>
                        <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => setBlackout((arr) => arr.filter((_, i) => i !== idx))}>Remove</button>
                      </div>
                    ))}
                    <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => setBlackout((arr) => [...arr, { start: '22:00', end: '06:00' }])}>+ Add Blackout</button>
                  </div>
                </div>
              </div>

              <div className="mt-3 flex items-center justify-end gap-2">
                <Dialog.Close asChild>
                  <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">
                    Cancel
                  </button>
                </Dialog.Close>
                <button
                  type="submit"
                  className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted"
                  disabled={saving}
                >
                  {saving ? 'Saving…' : 'Create'}
                </button>
              </div>
            </Form>
            {/* API Datasource Builder */}
            {formData.type === 'api' && (
              <div className="mt-3 rounded-md border p-3 space-y-3">
                <div className="text-sm font-medium">API Endpoint</div>
                <label className="text-xs">Endpoint URL (supports placeholders like {`{start}`}, {`{end}`}, {`{base}`}, {`{quote}`})
                  <input className="mt-1 w-full px-2 py-1 rounded-md border bg-background" placeholder="https://api.example.com/rates?base={base}&quote={quote}&start={start}&end={end}" value={apiCfg.endpoint} onChange={(e)=>setApiCfg({ ...apiCfg, endpoint: e.target.value })} />
                </label>
                <div className="grid grid-cols-2 gap-2">
                  <label className="text-xs">Method
                    <select className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.method} onChange={(e)=>setApiCfg({ ...apiCfg, method: (e.target.value as any) })}>
                      <option value="GET">GET</option>
                      <option value="POST">POST</option>
                    </select>
                  </label>
                  <label className="text-xs">Destination table in DuckDB
                    <input className="w-full px-2 py-1 rounded-md border bg-background" placeholder="fx_rates" value={apiCfg.destTable} onChange={(e)=>setApiCfg({ ...apiCfg, destTable: e.target.value })} />
                  </label>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <div>
                    <div className="text-xs font-medium mb-1">Headers</div>
                    <div className="space-y-1">
                      {apiCfg.headers.map((h, i) => (
                        <div key={i} className="flex items-center gap-1">
                          <input className="w-2/5 px-2 py-1 rounded-md border bg-background" placeholder="Key" value={h.key} onChange={(e)=>setApiCfg({ ...apiCfg, headers: apiCfg.headers.map((x,j)=> j===i ? { ...x, key: e.target.value } : x) })} />
                          <input className="w-3/5 px-2 py-1 rounded-md border bg-background" placeholder="Value (supports placeholders)" value={h.value} onChange={(e)=>setApiCfg({ ...apiCfg, headers: apiCfg.headers.map((x,j)=> j===i ? { ...x, value: e.target.value } : x) })} />
                          <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, headers: apiCfg.headers.filter((_,j)=>j!==i) })}>✕</button>
                        </div>
                      ))}
                      <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, headers: [...apiCfg.headers, { key: '', value: '' }] })}>+ Add Header</button>
                    </div>
                  </div>
                  <div>
                    <div className="text-xs font-medium mb-1">Query Params</div>
                    <div className="space-y-1">
                      {apiCfg.query.map((q, i) => (
                        <div key={i} className="flex items-center gap-1">
                          <input className="w-2/5 px-2 py-1 rounded-md border bg-background" placeholder="Key" value={q.key} onChange={(e)=>setApiCfg({ ...apiCfg, query: apiCfg.query.map((x,j)=> j===i ? { ...x, key: e.target.value } : x) })} />
                          <input className="w-3/5 px-2 py-1 rounded-md border bg-background" placeholder="Value (supports placeholders)" value={q.value} onChange={(e)=>setApiCfg({ ...apiCfg, query: apiCfg.query.map((x,j)=> j===i ? { ...x, value: e.target.value } : x) })} />
                          <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, query: apiCfg.query.filter((_,j)=>j!==i) })}>✕</button>
                        </div>
                      ))}
                      <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, query: [...apiCfg.query, { key: '', value: '' }] })}>+ Add Param</button>
                    </div>
                  </div>
                </div>
                {apiCfg.method === 'POST' && (
                  <label className="text-xs">Body (JSON; supports placeholders)
                    <textarea className="mt-1 w-full px-2 py-1 rounded-md border bg-background min-h-[80px] font-mono" placeholder='{"start":"{start}","end":"{end}","base":"{base}","quote":"{quote}"}' value={apiCfg.body} onChange={(e)=>setApiCfg({ ...apiCfg, body: e.target.value })} />
                  </label>
                )}
                <div>
                  <div className="text-xs font-medium mb-1">Placeholders</div>
                  <div className="space-y-1">
                    {apiCfg.placeholders.map((p, i) => (
                      <div key={i} className="grid grid-cols-5 gap-1 items-center">
                        <input className="col-span-2 px-2 py-1 rounded-md border bg-background" placeholder="name (e.g., start)" value={p.name} onChange={(e)=>setApiCfg({ ...apiCfg, placeholders: apiCfg.placeholders.map((x,j)=> j===i ? { ...x, name: e.target.value } : x) })} />
                        <select className="px-2 py-1 rounded-md border bg-background" value={p.kind} onChange={(e)=>setApiCfg({ ...apiCfg, placeholders: apiCfg.placeholders.map((x,j)=> j===i ? { ...x, kind: (e.target.value as any) } : x) })}>
                          <option value="static">static</option>
                          <option value="date">date</option>
                        </select>
                        <input className="px-2 py-1 rounded-md border bg-background" placeholder={p.kind==='date' ? 'macro (e.g., today-1d, startOfMonth, YYYY-MM-DD)' : 'value'} value={p.value || ''} onChange={(e)=>setApiCfg({ ...apiCfg, placeholders: apiCfg.placeholders.map((x,j)=> j===i ? { ...x, value: e.target.value } : x) })} />
                        <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, placeholders: apiCfg.placeholders.filter((_,j)=>j!==i) })}>✕</button>
                      </div>
                    ))}
                    <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, placeholders: [...apiCfg.placeholders, { name: '', kind: 'static', value: '' }] })}>+ Add Placeholder</button>
                  </div>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <label className="text-xs">JSON Root Path (e.g., $.rates or data.items)
                    <input className="w-full px-2 py-1 rounded-md border bg-background" placeholder="$.rates" value={apiCfg.jsonRoot} onChange={(e)=>setApiCfg({ ...apiCfg, jsonRoot: e.target.value })} />
                  </label>
                  <div className="rounded-md border p-2">
                    <div className="text-xs font-medium">Gap Fill (forward-fill by date)</div>
                    <label className="text-xs block mt-1"><input type="checkbox" className="mr-2" checked={apiCfg.gapFill.enabled} onChange={(e)=>setApiCfg({ ...apiCfg, gapFill: { ...apiCfg.gapFill, enabled: e.target.checked } })} />Enable</label>
                    <label className="text-xs block">Key fields (comma-separated)
                      <input className="w-full px-2 py-1 rounded-md border bg-background" placeholder="base,quote" value={apiCfg.gapFill.keyFields} onChange={(e)=>setApiCfg({ ...apiCfg, gapFill: { ...apiCfg.gapFill, keyFields: e.target.value } })} />
                    </label>
                    <label className="text-xs block">Date field name in dest table
                      <input className="w-full px-2 py-1 rounded-md border bg-background" placeholder="rate_date" value={apiCfg.gapFill.dateField} onChange={(e)=>setApiCfg({ ...apiCfg, gapFill: { ...apiCfg.gapFill, dateField: e.target.value } })} />
                    </label>
                  </div>
                </div>
                <div className="rounded-md border p-2">
                  <div className="text-xs font-medium">Incremental (sequence) mode</div>
                  <label className="text-xs block mt-1"><input type="checkbox" className="mr-2" checked={apiCfg.sequence.enabled} onChange={(e)=>setApiCfg({ ...apiCfg, sequence: { ...apiCfg.sequence, enabled: e.target.checked } })} />Enable</label>
                  <div className="grid grid-cols-2 gap-2">
                    <label className="text-xs">Start param name
                      <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.sequence.startParam} onChange={(e)=>setApiCfg({ ...apiCfg, sequence: { ...apiCfg.sequence, startParam: e.target.value } })} />
                    </label>
                    <label className="text-xs">End param name
                      <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.sequence.endParam} onChange={(e)=>setApiCfg({ ...apiCfg, sequence: { ...apiCfg.sequence, endParam: e.target.value } })} />
                    </label>
                    <label className="text-xs">Window days per run
                      <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={1} value={apiCfg.sequence.windowDays} onChange={(e)=>setApiCfg({ ...apiCfg, sequence: { ...apiCfg.sequence, windowDays: Math.max(1, Number(e.target.value)||1) } })} />
                    </label>
                    <label className="text-xs">Date field name in dest table
                      <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.sequence.dateField} onChange={(e)=>setApiCfg({ ...apiCfg, sequence: { ...apiCfg.sequence, dateField: e.target.value } })} />
                    </label>
                  </div>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <div className="rounded-md border p-2">
                    <div className="text-xs font-medium mb-1">Authentication</div>
                    <label className="text-xs block">Type
                      <select className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.auth.type} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, type: e.target.value as any } })}>
                        <option value="none">None</option>
                        <option value="bearer">Bearer (static)</option>
                        <option value="apiKeyHeader">API Key (Header)</option>
                        <option value="apiKeyQuery">API Key (Query)</option>
                        <option value="basic">Basic</option>
                        <option value="oauth2_client_credentials">OAuth2 Client Credentials</option>
                      </select>
                    </label>
                    {apiCfg.auth.type === 'bearer' && (
                      <label className="text-xs block mt-1">Token (or use {'{{'}secret:NAME{'}}'})
                        <input className="w-full px-2 py-1 rounded-md border bg-background" type="password" value={apiCfg.auth.token || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, token: e.target.value } })} />
                      </label>
                    )}
                    {apiCfg.auth.type === 'apiKeyHeader' && (
                      <div className="grid grid-cols-2 gap-2 mt-1">
                        <label className="text-xs">Header
                          <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.auth.header || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, header: e.target.value } })} />
                        </label>
                        <label className="text-xs">Value
                          <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.auth.valueTemplate || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, valueTemplate: e.target.value } })} />
                        </label>
                      </div>
                    )}
                    {apiCfg.auth.type === 'apiKeyQuery' && (
                      <div className="grid grid-cols-2 gap-2 mt-1">
                        <label className="text-xs">Param
                          <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.auth.param || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, param: e.target.value } })} />
                        </label>
                        <label className="text-xs">Value
                          <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.auth.valueTemplate || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, valueTemplate: e.target.value } })} />
                        </label>
                      </div>
                    )}
                    {apiCfg.auth.type === 'basic' && (
                      <div className="grid grid-cols-2 gap-2 mt-1">
                        <label className="text-xs">Username
                          <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.auth.username || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, username: e.target.value } })} />
                        </label>
                        <label className="text-xs">Password
                          <input className="w-full px-2 py-1 rounded-md border bg-background" type="password" value={apiCfg.auth.password || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, password: e.target.value } })} />
                        </label>
                      </div>
                    )}
                    {apiCfg.auth.type === 'oauth2_client_credentials' && (
                      <div className="space-y-1 mt-1">
                        <label className="text-xs block">Token URL
                          <input className="w-full px-2 py-1 rounded-md border bg-background" placeholder="https://.../oauth2/token" value={apiCfg.auth.tokenUrl || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, tokenUrl: e.target.value } })} />
                        </label>
                        <div className="grid grid-cols-2 gap-2">
                          <label className="text-xs">Client ID
                            <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.auth.clientId || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, clientId: e.target.value } })} />
                          </label>
                          <label className="text-xs">Client Secret
                            <input className="w-full px-2 py-1 rounded-md border bg-background" type="password" value={apiCfg.auth.clientSecret || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, clientSecret: e.target.value } })} />
                          </label>
                        </div>
                        <label className="text-xs block">Scope (optional)
                          <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.auth.scope || ''} onChange={(e)=>setApiCfg({ ...apiCfg, auth: { ...apiCfg.auth, scope: e.target.value } })} />
                        </label>
                      </div>
                    )}
                  </div>
                  <div className="rounded-md border p-2">
                    <div className="text-xs font-medium mb-1">Pagination</div>
                    <label className="text-xs block">Type
                      <select className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.pagination.type} onChange={(e)=>setApiCfg({ ...apiCfg, pagination: { ...apiCfg.pagination, type: e.target.value as any } })}>
                        <option value="none">None</option>
                        <option value="page">Page</option>
                        <option value="cursor">Cursor</option>
                      </select>
                    </label>
                    {apiCfg.pagination.type === 'page' && (
                      <div className="grid grid-cols-2 gap-2 mt-1">
                        <label className="text-xs">Page param
                          <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.pagination.pageParam || ''} onChange={(e)=>setApiCfg({ ...apiCfg, pagination: { ...apiCfg.pagination, pageParam: e.target.value } })} />
                        </label>
                        <label className="text-xs">Size param
                          <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.pagination.pageSizeParam || ''} onChange={(e)=>setApiCfg({ ...apiCfg, pagination: { ...apiCfg.pagination, pageSizeParam: e.target.value } })} />
                        </label>
                        <label className="text-xs">Page size
                          <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={1} value={apiCfg.pagination.pageSize || 100} onChange={(e)=>setApiCfg({ ...apiCfg, pagination: { ...apiCfg.pagination, pageSize: Math.max(1, Number(e.target.value)||1) } })} />
                        </label>
                        <label className="text-xs">Start page
                          <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={1} value={apiCfg.pagination.pageStart || 1} onChange={(e)=>setApiCfg({ ...apiCfg, pagination: { ...apiCfg.pagination, pageStart: Math.max(1, Number(e.target.value)||1) } })} />
                        </label>
                        <label className="text-xs">Max pages
                          <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={1} value={apiCfg.pagination.maxPages || 10} onChange={(e)=>setApiCfg({ ...apiCfg, pagination: { ...apiCfg.pagination, maxPages: Math.max(1, Number(e.target.value)||1) } })} />
                        </label>
                      </div>
                    )}
                    {apiCfg.pagination.type === 'cursor' && (
                      <div className="grid grid-cols-2 gap-2 mt-1">
                        <label className="text-xs">Cursor param
                          <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.pagination.cursorParam || ''} onChange={(e)=>setApiCfg({ ...apiCfg, pagination: { ...apiCfg.pagination, cursorParam: e.target.value } })} />
                        </label>
                        <label className="text-xs">Next cursor JSONPath
                          <input className="w-full px-2 py-1 rounded-md border bg-background" placeholder="$.next_cursor" value={apiCfg.pagination.nextCursorPath || ''} onChange={(e)=>setApiCfg({ ...apiCfg, pagination: { ...apiCfg.pagination, nextCursorPath: e.target.value } })} />
                        </label>
                        <label className="text-xs">Max pages
                          <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={1} value={apiCfg.pagination.maxPages || 10} onChange={(e)=>setApiCfg({ ...apiCfg, pagination: { ...apiCfg.pagination, maxPages: Math.max(1, Number(e.target.value)||1) } })} />
                        </label>
                      </div>
                    )}
                  </div>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  <label className="text-xs">Write mode
                    <select className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.writeMode} onChange={(e)=>setApiCfg({ ...apiCfg, writeMode: e.target.value as any })}>
                      <option value="append">Append</option>
                      <option value="replace">Replace</option>
                    </select>
                  </label>
                </div>
              </div>
            )}
            {/* DSN Builders */}
            {formData.type === 'postgres' && (
              <div className="mt-3 rounded-md border p-3 space-y-2">
                <div className="text-xs font-medium">Build Postgres DSN</div>
                <div className="grid grid-cols-2 gap-2">
                  <label className="text-xs">Host
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={pg.host} onChange={(e) => setPg({ ...pg, host: e.target.value })} />
                  </label>
                  <label className="text-xs">Port
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" value={pg.port} onChange={(e) => setPg({ ...pg, port: Number(e.target.value) || 5432 })} />
                  </label>
                  <label className="text-xs">Database
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={pg.db} onChange={(e) => setPg({ ...pg, db: e.target.value })} />
                  </label>
                  <label className="text-xs">User
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={pg.user} onChange={(e) => setPg({ ...pg, user: e.target.value })} />
                  </label>
                  <label className="text-xs col-span-2">Password
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="password" value={pg.password} onChange={(e) => setPg({ ...pg, password: e.target.value })} />
                  </label>
                </div>
                <div className="text-xs text-muted-foreground">Built DSN: <span className="font-mono break-all">{formData.connectionUri || '(fill fields)'}</span></div>
              </div>
            )}
            {formData.type === 'supabase' && (
              <div className="mt-3 rounded-md border p-3 space-y-2">
                <div className="text-xs font-medium">Build Supabase DSN</div>
                <label className="text-xs">Project URL (https://&lt;ref&gt;.supabase.co)
                  <input className="w-full px-2 py-1 rounded-md border bg-background" placeholder="https://xxx.supabase.co" value={sb.projectUrl} onChange={(e) => setSb({ ...sb, projectUrl: e.target.value })} />
                </label>
                <label className="text-xs">Service Role Key
                  <input className="w-full px-2 py-1 rounded-md border bg-background" type="password" value={sb.serviceRoleKey} onChange={(e) => setSb({ ...sb, serviceRoleKey: e.target.value })} />
                </label>
                <div className="text-xs text-muted-foreground">Built DSN: <span className="font-mono break-all">{formData.connectionUri || '(fill fields)'}</span></div>
              </div>
            )}
            {formData.type === 'mysql' && (
              <div className="mt-3 rounded-md border p-3 space-y-2">
                <div className="text-xs font-medium">Build MySQL DSN</div>
                <div className="grid grid-cols-2 gap-2">
                  <label className="text-xs">Host
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={mysql.host} onChange={(e) => setMysql({ ...mysql, host: e.target.value })} />
                  </label>
                  <label className="text-xs">Port
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" value={mysql.port} onChange={(e) => setMysql({ ...mysql, port: Number(e.target.value) || 3306 })} />
                  </label>
                  <label className="text-xs">Database
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={mysql.db} onChange={(e) => setMysql({ ...mysql, db: e.target.value })} />
                  </label>
                  <label className="text-xs">User
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={mysql.user} onChange={(e) => setMysql({ ...mysql, user: e.target.value })} />
                  </label>
                  <label className="text-xs col-span-2">Password
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="password" value={mysql.password} onChange={(e) => setMysql({ ...mysql, password: e.target.value })} />
                  </label>
                  <label className="text-xs">Charset
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={mysql.charset} onChange={(e) => setMysql({ ...mysql, charset: e.target.value })} />
                  </label>
                  <label className="text-xs">Connect timeout (sec)
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={0} value={mysql.connectTimeout} onChange={(e) => setMysql({ ...mysql, connectTimeout: Math.max(0, Number(e.target.value) || 0) })} />
                  </label>
                </div>
                <div className="text-xs text-muted-foreground">Built DSN: <span className="font-mono break-all">{formData.connectionUri || '(fill fields)'}</span></div>
              </div>
            )}
            {formData.type === 'mssql' && (
              <div className="mt-3 rounded-md border p-3 space-y-2">
                <div className="text-xs font-medium">Build MS SQL Server DSN</div>
                <div className="grid grid-cols-2 gap-2">
                  <label className="text-xs col-span-2">Driver Type
                    <select className="w-full px-2 py-1 rounded-md border bg-background" value={mssql.driverType} onChange={(e) => setMssql({ ...mssql, driverType: e.target.value as any })}>
                      <option value="pyodbc">pyodbc (requires system ODBC driver)</option>
                      <option value="pytds">pytds (fallback; no ODBC driver)</option>
                    </select>
                  </label>
                  <label className="text-xs">Host
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={mssql.host} onChange={(e) => setMssql({ ...mssql, host: e.target.value })} />
                  </label>
                  <label className="text-xs">Port
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" value={mssql.port} onChange={(e) => setMssql({ ...mssql, port: Number(e.target.value) || 1433 })} />
                  </label>
                  <label className="text-xs">Database
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={mssql.db} onChange={(e) => setMssql({ ...mssql, db: e.target.value })} />
                  </label>
                  <label className="text-xs">User
                    <input className="w-full px-2 py-1 rounded-md border bg-background" value={mssql.user} onChange={(e) => setMssql({ ...mssql, user: e.target.value })} />
                  </label>
                  <label className="text-xs col-span-2">Password
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="password" value={mssql.password} onChange={(e) => setMssql({ ...mssql, password: e.target.value })} />
                  </label>
                  <label className="text-xs">Driver
                    <select className="w-full px-2 py-1 rounded-md border bg-background" value={mssql.driver} onChange={(e) => setMssql({ ...mssql, driver: e.target.value as any })} disabled={mssql.driverType !== 'pyodbc'}>
                      <option value="ODBC Driver 18 for SQL Server">ODBC Driver 18 for SQL Server</option>
                      <option value="ODBC Driver 17 for SQL Server">ODBC Driver 17 for SQL Server</option>
                    </select>
                  </label>
                  <label className="text-xs flex items-center gap-2 mt-6">
                    <input type="checkbox" checked={mssql.encrypt} onChange={(e) => setMssql({ ...mssql, encrypt: e.target.checked })} disabled={mssql.driverType !== 'pyodbc'} />
                    Encrypt
                  </label>
                  <label className="text-xs flex items-center gap-2 mt-6">
                    <input type="checkbox" checked={mssql.trustServerCertificate} onChange={(e) => setMssql({ ...mssql, trustServerCertificate: e.target.checked })} disabled={mssql.driverType !== 'pyodbc'} />
                    Trust server certificate
                  </label>
                  <label className="text-xs">Login timeout (sec)
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={0} value={mssql.connectTimeout} onChange={(e) => setMssql({ ...mssql, connectTimeout: Math.max(0, Number(e.target.value) || 0) })} disabled={mssql.driverType !== 'pyodbc'} />
                  </label>
                  <label className="text-xs flex items-center gap-2 mt-6">
                    <input type="checkbox" checked={mssql.useOdbcConnect} onChange={(e) => setMssql({ ...mssql, useOdbcConnect: e.target.checked })} disabled={mssql.driverType !== 'pyodbc'} />
                    Use odbc_connect style
                  </label>
                  <label className="text-xs">Connect retry count
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={0} value={mssql.connectRetryCount} onChange={(e) => setMssql({ ...mssql, connectRetryCount: Math.max(0, Number(e.target.value) || 0) })} disabled={mssql.driverType !== 'pyodbc'} />
                  </label>
                  <label className="text-xs">Connect retry interval (sec)
                    <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={0} value={mssql.connectRetryInterval} onChange={(e) => setMssql({ ...mssql, connectRetryInterval: Math.max(0, Number(e.target.value) || 0) })} disabled={mssql.driverType !== 'pyodbc'} />
                  </label>
                  <label className="text-xs flex items-center gap-2 mt-6">
                    <input type="checkbox" checked={mssql.pooling} onChange={(e) => setMssql({ ...mssql, pooling: e.target.checked })} disabled={mssql.driverType !== 'pyodbc'} />
                    Pooling
                  </label>
                </div>
                <div className="text-xs text-muted-foreground">Built DSN: <span className="font-mono break-all">{formData.connectionUri || '(fill fields)'}</span></div>
                <div className="text-xs text-muted-foreground">{mssql.driverType === 'pyodbc' ? 'pyodbc requires a system ODBC driver (e.g., Microsoft ODBC Driver 18).' : 'pytds does not require a system ODBC driver.'}</div>
                <div className="text-xs text-muted-foreground">Tip: odbc_connect is more robust for instance names and special characters.</div>
              </div>
            )}
            <div className="mt-3 rounded-md border p-3 space-y-2">
              <div className="text-xs font-medium">Query defaults</div>
              <div className="grid grid-cols-2 gap-2">
                <label className="text-xs">Default pivot parallelism (1–8)
                  <input className="w-full px-2 py-1 rounded-md border bg-background" type="number" min={1} max={8} value={defaultPivotPar} onChange={(e) => setDefaultPivotPar(Math.max(1, Math.min(8, Number(e.target.value) || 2)))} />
                </label>
              </div>
            </div>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
