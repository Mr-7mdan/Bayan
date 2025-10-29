"use client"

import { useEffect, useMemo, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { Api, type DatasourceOut, type DatasourceCreate } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import {
  RiDatabase2Line,
  RiCloudLine,
  RiServerLine,
  RiHardDrive2Line,
  RiBarChart2Line,
  RiSnowflakeLine,
  RiBracesLine,
} from '@remixicon/react'

export type DatasourceDialogMode = 'create' | 'edit'

const CONNECTORS = [
  { id: 'duckdb', label: 'DuckDB', icon: RiDatabase2Line },
  { id: 'postgres', label: 'Postgres', icon: RiServerLine },
  { id: 'supabase', label: 'Supabase', icon: RiCloudLine },
  { id: 'mysql', label: 'MySQL', icon: RiServerLine },
  { id: 'mssql', label: 'MS SQL Server', icon: RiServerLine },
  { id: 'sqlite', label: 'SQLite', icon: RiHardDrive2Line },
  { id: 'bigquery', label: 'BigQuery', icon: RiBarChart2Line },
  { id: 'snowflake', label: 'Snowflake', icon: RiSnowflakeLine },
  { id: 'api', label: 'API (HTTP)', icon: RiBracesLine },
] as const

export default function DatasourceDialog({
  open,
  onOpenChangeAction,
  mode,
  initial,
  onCreatedAction,
  onSavedAction,
}: {
  open: boolean
  onOpenChangeAction: (v: boolean) => void
  mode: DatasourceDialogMode
  initial?: Partial<DatasourceOut & { connectionUri?: string; options?: Record<string, unknown> }>
  onCreatedAction?: (ds: DatasourceOut) => void
  onSavedAction?: (ds: DatasourceOut) => void
}) {
  const { user } = useAuth()
  const DEFAULT_DUCK = process.env.NEXT_PUBLIC_DEFAULT_DUCKDB_PATH || ''
  const DEFAULT_SQLITE = process.env.NEXT_PUBLIC_DEFAULT_SQLITE_PATH || ''
  const [type, setType] = useState<'duckdb'|'postgres'|'supabase'|'mysql'|'mssql'|'sqlite'|'bigquery'|'snowflake'|'api'>('duckdb')
  const [name, setName] = useState<string>('New Datasource')
  const [dsn, setDsn] = useState<string>('')
  const [active, setActive] = useState<boolean>(true)
  const [optionsJson, setOptionsJson] = useState<string>('')
  const [builderDirty, setBuilderDirty] = useState(false)
  // Sync settings (high-level rules)
  const [syncMaxConcurrent, setSyncMaxConcurrent] = useState<number>(1)
  const [blackout, setBlackout] = useState<Array<{ start: string; end: string }>>([])

  const [testing, setTesting] = useState(false)
  const [test, setTest] = useState<{ ok: boolean; error?: string } | null>(null)
  const [detecting, setDetecting] = useState(false)
  const [detect, setDetect] = useState<{ detected?: string | null; method?: string | null; versionString?: string | null; candidates?: string[] | null; error?: string | null } | null>(null)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Per-connector state
  const [pg, setPg] = useState<{ host: string; port: number; db: string; user: string; password: string }>({ host: 'localhost', port: 5432, db: 'postgres', user: 'postgres', password: '' })
  const [sb, setSb] = useState<{ projectUrl: string; serviceRoleKey: string }>({ projectUrl: '', serviceRoleKey: '' })
  const [mysql, setMysql] = useState<{ host: string; port: number; db: string; user: string; password: string }>({ host: 'localhost', port: 3306, db: 'mysql', user: 'root', password: '' })
  const [mssql, setMssql] = useState<{ host: string; port: number; db: string; user: string; password: string; driverType: 'pyodbc'|'pytds'; driver: 'ODBC Driver 18 for SQL Server'|'ODBC Driver 17 for SQL Server'; encrypt: boolean; trustServerCertificate: boolean; connectTimeout: number; connectRetryCount: number; connectRetryInterval: number; pooling: boolean; useOdbcConnect: boolean }>({ host: 'localhost', port: 1433, db: 'master', user: 'sa', password: '', driverType: 'pyodbc', driver: 'ODBC Driver 18 for SQL Server', encrypt: false, trustServerCertificate: true, connectTimeout: 30, connectRetryCount: 3, connectRetryInterval: 10, pooling: false, useOdbcConnect: false })
  // SQLAlchemy pool clamp controls (for mssql+pyodbc)
  const [sqlPoolClamp, setSqlPoolClamp] = useState<boolean>(false)
  const [sqlPoolSize, setSqlPoolSize] = useState<number>(1)
  const [sqlPoolOverflow, setSqlPoolOverflow] = useState<number>(0)
  const [sqlPoolTimeout, setSqlPoolTimeout] = useState<number>(5)
  const [defaultPivotPar, setDefaultPivotPar] = useState<number>(2)
  const [duckdbPath, setDuckdbPath] = useState<string>(DEFAULT_DUCK)
  const [sqlitePath, setSqlitePath] = useState<string>(DEFAULT_SQLITE)
  const [snow, setSnow] = useState<{ account: string; user: string; password: string; warehouse: string; database: string; schema: string }>({ account: '', user: '', password: '', warehouse: '', database: '', schema: '' })
  const [bq, setBq] = useState<{ projectId: string; credentialsJson: string }>({ projectId: '', credentialsJson: '' })
  // API builder state
  const [apiCfg, setApiCfg] = useState<{
    endpoint: string
    method: 'GET' | 'POST'
    headers: Array<{ key: string; value: string }>
    query: Array<{ key: string; value: string }>
    body: string
    placeholders: Array<{ name: string; kind: 'static' | 'date'; value?: string; format?: string }>
    destTable: string
    jsonRoot: string
    parse?: 'csv' | 'json'
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
    parse: undefined,
    gapFill: { enabled: true, keyFields: 'base,quote', dateField: 'rate_date' },
    sequence: { enabled: true, mode: 'date-range', startParam: 'start', endParam: 'end', windowDays: 7, dateField: 'rate_date' },
    auth: { type: 'none' },
    pagination: { type: 'none', pageParam: 'page', pageSizeParam: 'limit', pageSize: 100, pageStart: 1, maxPages: 10, cursorParam: 'cursor', nextCursorPath: '' },
    writeMode: 'append',
  })

  useEffect(() => {
    if (!open) return
    setType((initial?.type as any) || 'duckdb')
    setName(mode === 'edit' ? (initial?.name || '') : 'New Datasource')
    setDsn(initial?.connectionUri || '')
    setActive(initial?.active ?? true)
    setOptionsJson(initial?.options ? JSON.stringify(initial.options, null, 2) : '')
    setBuilderDirty(false)
    setTest(null)
    setDetect(null)
    setError(null)
    // Reset SQLAlchemy pool clamp to defaults on open; prefill may override
    setSqlPoolClamp(false)
    setSqlPoolSize(1)
    setSqlPoolOverflow(0)
    setSqlPoolTimeout(5)
    // Prefill sync settings from options if present
    try {
      const opts: any = initial?.options || (optionsJson.trim() ? JSON.parse(optionsJson) : {})
      const sync = (opts?.sync) || {}
      const mc = Number(sync?.maxConcurrentQueries)
      setSyncMaxConcurrent(mc && mc > 0 ? mc : 1)
      const bo = Array.isArray(sync?.blackoutDaily) ? sync.blackoutDaily : []
      setBlackout(bo.map((w: any) => ({ start: String(w?.start || ''), end: String(w?.end || '') })))
      const dpp = Number(opts?.defaultPivotParallelism)
      setDefaultPivotPar(Number.isFinite(dpp) && dpp > 0 ? Math.max(1, Math.min(8, Math.floor(dpp))) : 2)
    } catch {
      setSyncMaxConcurrent(1)
      setBlackout([])
      setDefaultPivotPar(2)
    }
    // Prefill connector builder fields from existing DSN (edit mode)
    try { if (initial?.connectionUri) prefillFromDsn(initial.connectionUri, (initial.type as any) || undefined) } catch {}

    // If editing, fetch full details to ensure we have connectionUri and options
    ;(async () => {
      if (!open) return
      if (mode === 'edit' && initial?.id) {
        try {
          const full = await Api.getDatasource(initial.id, user?.id)
          setType((full?.type as any) || 'duckdb')
          setName(full?.name || '')
          setDsn(full?.connectionUri || '')
          setActive(full?.active ?? true)
          setOptionsJson(full?.options ? JSON.stringify(full.options, null, 2) : '')
          try { const o: any = full?.options || {}; if (o.api) setApiCfg((prev) => ({ ...prev, ...o.api })) } catch {}
          try { if (full?.connectionUri) prefillFromDsn(full.connectionUri, (full.type as any) || undefined) } catch {}
        } catch {}
      }
    })()
  }, [open, mode, initial])

  // Auto-build DSN only if builder fields were touched
  useEffect(() => {
    if (!open || !builderDirty) return
    if (type === 'postgres') {
      setDsn(`postgresql+psycopg://${encodeURIComponent(pg.user)}:${encodeURIComponent(pg.password)}@${pg.host}:${pg.port}/${pg.db}`)
    } else if (type === 'supabase') {
      let ref = ''
      try { const u = new URL(sb.projectUrl); ref = (u.hostname.split('.')[0] || '') } catch {}
      const host = ref ? `db.${ref}.supabase.co` : ''
      setDsn(host ? `postgresql+psycopg://${encodeURIComponent('postgres')}:${encodeURIComponent(sb.serviceRoleKey)}@${host}:5432/postgres` : '')
    } else if (type === 'mysql') {
      setDsn(`mysql+pymysql://${encodeURIComponent(mysql.user)}:${encodeURIComponent(mysql.password)}@${mysql.host}:${mysql.port}/${mysql.db}`)
    } else if (type === 'mssql') {
      if (mssql.driverType === 'pytds') {
        setDsn(`mssql+pytds://${encodeURIComponent(mssql.user)}:${encodeURIComponent(mssql.password)}@${mssql.host}:${mssql.port}/${mssql.db}`)
      } else {
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
          const base = `mssql+pyodbc:///?odbc_connect=${encodeURIComponent(parts.join(';'))}`
          const sa: string[] = []
          if (sqlPoolClamp) {
            sa.push('sa_pool_clamp=true')
            sa.push(`sa_pool_size=${encodeURIComponent(String(sqlPoolSize))}`)
            sa.push(`sa_max_overflow=${encodeURIComponent(String(sqlPoolOverflow))}`)
            sa.push(`sa_pool_timeout=${encodeURIComponent(String(sqlPoolTimeout))}`)
          }
          setDsn(base + (sa.length ? `&${sa.join('&')}` : ''))
        } else {
          const qp: string[] = []
          qp.push(`driver=${encodeURIComponent(mssql.driver)}`)
          qp.push(`Encrypt=${mssql.encrypt ? 'yes' : 'no'}`)
          qp.push(`TrustServerCertificate=${mssql.trustServerCertificate ? 'yes' : 'no'}`)
          if (mssql.connectTimeout) qp.push(`LoginTimeout=${encodeURIComponent(String(mssql.connectTimeout))}`)
          qp.push(`ConnectRetryCount=${encodeURIComponent(String(mssql.connectRetryCount))}`)
          qp.push(`ConnectRetryInterval=${encodeURIComponent(String(mssql.connectRetryInterval))}`)
          qp.push(`Pooling=${mssql.pooling ? 'True' : 'False'}`)
          // SQLAlchemy pool clamp custom params
          if (sqlPoolClamp) {
            qp.push('sa_pool_clamp=true')
            qp.push(`sa_pool_size=${encodeURIComponent(String(sqlPoolSize))}`)
            qp.push(`sa_max_overflow=${encodeURIComponent(String(sqlPoolOverflow))}`)
            qp.push(`sa_pool_timeout=${encodeURIComponent(String(sqlPoolTimeout))}`)
          }
          const q = qp.length ? `?${qp.join('&')}` : ''
          setDsn(`mssql+pyodbc://${encodeURIComponent(mssql.user)}:${encodeURIComponent(mssql.password)}@${mssql.host}:${mssql.port}/${mssql.db}${q}`)
        }
      }
    } else if (type === 'duckdb') {
      setDsn(duckdbPath?.trim() ? `duckdb:///${duckdbPath}` : '')
    } else if (type === 'sqlite') {
      setDsn(sqlitePath?.trim() ? `sqlite:///${sqlitePath}` : '')
    } else if (type === 'bigquery') {
      setDsn(`bigquery://${bq.projectId}`)
    } else if (type === 'snowflake') {
      const acct = snow.account.replace(/\s+/g, '')
      setDsn(`snowflake://${encodeURIComponent(snow.user)}:${encodeURIComponent(snow.password)}@${acct}/${snow.database}/${snow.schema}?warehouse=${snow.warehouse}`)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, builderDirty, type, pg, sb, mysql, mssql, duckdbPath, sqlitePath, bq, snow])

  const onTest = async () => {
    setTesting(true); setTest(null)
    try { const res = await Api.testConnection(dsn || undefined); setTest({ ok: !!res.ok, error: res.error || undefined }) }
    catch (e: any) { setTest({ ok: false, error: e?.message || 'Failed' }) }
    finally { setTesting(false) }
  }

  const onDetect = async () => {
    setDetecting(true); setDetect(null)
    try {
      const trimmed = (dsn || '').trim()
      let payload: Parameters<typeof Api.detectDb>[0]
      if (trimmed) {
        payload = { dsn: trimmed }
      } else if (type === 'postgres') {
        payload = { host: pg.host, port: Number(pg.port) || 5432 }
      } else if (type === 'mysql') {
        payload = { host: mysql.host, port: Number(mysql.port) || 3306 }
      } else if (type === 'mssql') {
        payload = { host: mssql.host, port: Number(mssql.port) || 1433 }
      } else if (type === 'supabase') {
        let host = ''
        try { const u = new URL(sb.projectUrl); const ref = (u.hostname.split('.')[0] || '').trim(); if (ref) host = `db.${ref}.supabase.co` } catch {}
        payload = host ? { host, port: 5432 } : { host: '' }
      } else {
        payload = { host: '' }
      }
      const res = await Api.detectDb(payload)
      setDetect({
        detected: res.detected ?? null,
        method: res.method ?? null,
        versionString: res.versionString ?? null,
        candidates: res.candidates ?? null,
        error: res.error ?? null,
      })
    } catch (e: any) {
      setDetect({ error: e?.message || 'Detection failed' })
    } finally { setDetecting(false) }
  }

  // Parse a DSN string and prefill per-connector builder fields
  function prefillFromDsn(s: string, t?: 'postgres'|'supabase'|'mysql'|'mssql'|'duckdb'|'sqlite'|'bigquery'|'snowflake') {
    const d = (s || '').trim()
    const low = d.toLowerCase()
    const safeUrl = (proto: string, url: string) => {
      try { return new URL(url.replace(proto, 'http://')) } catch { return null }
    }
    if ((t === 'postgres' || low.startsWith('postgres'))) {
      const u = safeUrl('postgresql+psycopg://', d) || safeUrl('postgres://', d)
      if (u) {
        setPg({
          host: u.hostname || 'localhost',
          port: Number(u.port || 5432),
          db: decodeURIComponent((u.pathname || '').replace(/^\//, '')) || 'postgres',
          user: decodeURIComponent(u.username || 'postgres'),
          password: decodeURIComponent(u.password || ''),
        })
      }
    } else if ((t === 'mysql' || low.startsWith('mysql'))) {
      const u = safeUrl('mysql+pymysql://', d) || safeUrl('mysql://', d)
      if (u) {
        setMysql({
          host: u.hostname || 'localhost',
          port: Number(u.port || 3306),
          db: decodeURIComponent((u.pathname || '').replace(/^\//, '')) || 'mysql',
          user: decodeURIComponent(u.username || 'root'),
          password: decodeURIComponent(u.password || ''),
        })
      }
    } else if ((t === 'mssql' || low.includes('mssql'))) {
      if (low.startsWith('mssql+pytds://')) {
        const u = safeUrl('mssql+pytds://', d)
        if (u) {
          setMssql((prev) => ({
            ...prev,
            driverType: 'pytds',
            host: u.hostname || 'localhost',
            port: Number(u.port || 1433),
            db: decodeURIComponent((u.pathname || '').replace(/^\//, '')) || 'master',
            user: decodeURIComponent(u.username || ''),
            password: decodeURIComponent(u.password || ''),
            useOdbcConnect: false,
          }))
        }
      } else if (low.startsWith('mssql+pyodbc:///?odbc_connect=')) {
        try {
          const enc = d.split('odbc_connect=')[1] || ''
          const cs = decodeURIComponent(enc)
          const parts = Object.fromEntries(cs.split(';').filter(Boolean).map(kv => {
            const [k, ...rest] = kv.split('='); return [k.trim().toLowerCase(), (rest.join('=') || '').trim()]
          })) as Record<string, string>
          const server = parts['server'] || ''
          const [host, portStr] = server.split(',')
          setMssql((prev) => ({
            ...prev,
            driverType: 'pyodbc',
            useOdbcConnect: true,
            driver: (parts['driver'] || prev.driver) as any,
            host: host || prev.host,
            port: Number(portStr || prev.port || 1433),
            db: parts['database'] || prev.db,
            user: parts['uid'] || prev.user,
            password: parts['pwd'] || prev.password,
            encrypt: String(parts['encrypt'] || '').toLowerCase() === 'yes',
            trustServerCertificate: String(parts['trustservercertificate'] || '').toLowerCase() === 'yes',
            connectTimeout: Number(parts['logintimeout'] || prev.connectTimeout || 30),
            connectRetryCount: Number(parts['connectretrycount'] || prev.connectRetryCount || 3),
            connectRetryInterval: Number(parts['connectretryinterval'] || prev.connectRetryInterval || 10),
            pooling: String(parts['pooling'] || '').toLowerCase() === 'yes',
          }))
          // Also parse outer SQLAlchemy query params for sa_* keys
          const u = safeUrl('mssql+pyodbc://', d)
          if (u) {
            const q = new URLSearchParams(u.search || '')
            const clamp = (q.get('sa_pool_clamp') || '').toLowerCase()
            setSqlPoolClamp(clamp === 'true' || clamp === '1' || clamp === 'yes')
            const ps = Number(q.get('sa_pool_size') || '1')
            const mo = Number(q.get('sa_max_overflow') || '0')
            const pt = Number(q.get('sa_pool_timeout') || '5')
            setSqlPoolSize(Number.isFinite(ps) ? ps : 1)
            setSqlPoolOverflow(Number.isFinite(mo) ? mo : 0)
            setSqlPoolTimeout(Number.isFinite(pt) ? pt : 5)
          }
        } catch {}
      } else if (low.startsWith('mssql+pyodbc://')) {
        const u = safeUrl('mssql+pyodbc://', d)
        if (u) {
          const q = new URLSearchParams(u.search || '')
          setMssql((prev) => ({
            ...prev,
            driverType: 'pyodbc',
            useOdbcConnect: false,
            driver: (q.get('driver') || prev.driver) as any,
            host: u.hostname || prev.host,
            port: Number(u.port || prev.port || 1433),
            db: decodeURIComponent((u.pathname || '').replace(/^\//, '')) || prev.db,
            user: decodeURIComponent(u.username || prev.user),
            password: decodeURIComponent(u.password || prev.password),
            encrypt: (q.get('Encrypt') || '').toLowerCase() === 'yes' || (q.get('encrypt') || '').toLowerCase() === 'yes',
            trustServerCertificate: (q.get('TrustServerCertificate') || q.get('trustservercertificate') || '').toLowerCase() === 'yes',
            connectTimeout: Number(q.get('LoginTimeout') || q.get('logintimeout') || prev.connectTimeout || 30),
            connectRetryCount: Number(q.get('ConnectRetryCount') || q.get('connectretrycount') || prev.connectRetryCount || 3),
            connectRetryInterval: Number(q.get('ConnectRetryInterval') || q.get('connectretryinterval') || prev.connectRetryInterval || 10),
            pooling: String(q.get('Pooling') || q.get('pooling') || '').toLowerCase() === 'true',
          }))
          // sa_* params
          const clamp = (q.get('sa_pool_clamp') || '').toLowerCase()
          setSqlPoolClamp(clamp === 'true' || clamp === '1' || clamp === 'yes')
          const ps = Number(q.get('sa_pool_size') || '1')
          const mo = Number(q.get('sa_max_overflow') || '0')
          const pt = Number(q.get('sa_pool_timeout') || '5')
          setSqlPoolSize(Number.isFinite(ps) ? ps : 1)
          setSqlPoolOverflow(Number.isFinite(mo) ? mo : 0)
          setSqlPoolTimeout(Number.isFinite(pt) ? pt : 5)
        } else {
          // Fallback regex parser if URL failed (edge cases)
          const m = d.match(/^mssql\+pyodbc:\/\/([^:]+):([^@]+)@([^:\/?#]+)(?::(\d+))?\/([^?]+)(\?.*)?$/i)
          if (m) {
            const user = decodeURIComponent(m[1])
            const pass = decodeURIComponent(m[2])
            const host = m[3]
            const port = Number(m[4] || '1433')
            const db = decodeURIComponent(m[5])
            const q = new URLSearchParams(m[6] || '')
            setMssql((prev) => ({
              ...prev,
              driverType: 'pyodbc',
              useOdbcConnect: false,
              driver: (q.get('driver') || prev.driver) as any,
              host,
              port,
              db,
              user,
              password: pass,
              encrypt: (q.get('Encrypt') || q.get('encrypt') || '').toLowerCase() === 'yes',
              trustServerCertificate: (q.get('TrustServerCertificate') || q.get('trustservercertificate') || '').toLowerCase() === 'yes',
              connectTimeout: Number(q.get('LoginTimeout') || q.get('logintimeout') || prev.connectTimeout || 30),
              connectRetryCount: Number(q.get('ConnectRetryCount') || q.get('connectretrycount') || prev.connectRetryCount || 3),
              connectRetryInterval: Number(q.get('ConnectRetryInterval') || q.get('connectretryinterval') || prev.connectRetryInterval || 10),
              pooling: String(q.get('Pooling') || q.get('pooling') || '').toLowerCase() === 'true',
            }))
            const clamp = (q.get('sa_pool_clamp') || '').toLowerCase()
            setSqlPoolClamp(clamp === 'true' || clamp === '1' || clamp === 'yes')
            const ps = Number(q.get('sa_pool_size') || '1')
            const mo = Number(q.get('sa_max_overflow') || '0')
            const pt = Number(q.get('sa_pool_timeout') || '5')
            setSqlPoolSize(Number.isFinite(ps) ? ps : 1)
            setSqlPoolOverflow(Number.isFinite(mo) ? mo : 0)
            setSqlPoolTimeout(Number.isFinite(pt) ? pt : 5)
          }
        }
      }
    }
  }

  const onSubmit = async () => {
    setSaving(true); setError(null)
    try {
      let options: Record<string, unknown> | undefined
      if (optionsJson.trim()) { try { options = JSON.parse(optionsJson) } catch { setError('Options JSON is invalid'); setSaving(false); return } }
      // Merge Sync Settings and Query defaults into options
      const sync = {
        maxConcurrentQueries: Math.max(1, Number(syncMaxConcurrent) || 1),
        blackoutDaily: blackout.filter((w) => (w.start || '').includes(':') && (w.end || '').includes(':')),
      }
      const dpp = Math.max(1, Math.min(8, Number(defaultPivotPar) || 2))
      options = { ...(options || {}), sync, defaultPivotParallelism: dpp }
      if (type === 'api') {
        const prevApi = (options && (options as any).api) ? (options as any).api : {}
        options = { ...(options || {}), api: { ...prevApi, ...apiCfg } }
      }
      if (mode === 'create') {
        const payload: DatasourceCreate = { name, type, connectionUri: dsn || undefined, options, userId: user?.id }
        const res = await Api.createDatasource(payload)
        onCreatedAction?.(res)
      } else {
        if (!initial?.id) return
        // Keep existing owner; backend update route does not change owner
        const res = await Api.updateDatasource(initial.id, { name, type, connectionUri: dsn || undefined, options, active })
        onSavedAction?.(res)
      }
      onOpenChangeAction(false)
    } catch (e: any) {
      setError(e?.message || 'Failed to save')
    } finally { setSaving(false) }
  }

  const renderConnectorFields = () => {
    if (type === 'api') {
      // Build preview URL with current placeholders
      const tokenReplace = (s: string, ctx: Record<string, string>) => (s || '').replace(/\{([^}]+)\}/g, (_, k) => (ctx[k] ?? ''))
      const pad2 = (n: number) => String(n).padStart(2, '0')
      const startOfWeek = (d: Date) => { const x = new Date(d); const day = (x.getDay() + 6) % 7; x.setDate(x.getDate() - day); x.setHours(0,0,0,0); return x }
      const startOfQuarter = (d: Date) => { const x = new Date(d); const q = Math.floor(x.getMonth() / 3) * 3; x.setMonth(q, 1); x.setHours(0,0,0,0); return x }
      const endOfMonth = (d: Date) => { const x = new Date(d); const m = x.getMonth(); const y = x.getFullYear(); const last = new Date(y, m + 1, 0); last.setHours(0,0,0,0); return last }
      const endOfYear = (d: Date) => { const x = new Date(d); const last = new Date(x.getFullYear(), 11, 31); last.setHours(0,0,0,0); return last }
      const addMonths = (d: Date, n: number) => { const x = new Date(d); x.setMonth(x.getMonth() + n); return x }
      const addYears = (d: Date, n: number) => { const x = new Date(d); x.setFullYear(x.getFullYear() + n); return x }
      const applyOffset = (dt: Date, off?: string) => {
        if (!off) return dt
        const m = off.match(/([+-])(\d+)([dhwmy])/i)
        if (!m) return dt
        const sign = m[1] === '-' ? -1 : 1
        const num = Number(m[2]) * sign
        const unit = (m[3] || '').toLowerCase()
        const x = new Date(dt)
        if (unit === 'd') x.setDate(x.getDate() + num)
        else if (unit === 'h') x.setHours(x.getHours() + num)
        else if (unit === 'w') x.setDate(x.getDate() + num * 7)
        else if (unit === 'm') return addMonths(x, num)
        else if (unit === 'y') return addYears(x, num)
        return x
      }
      const formatDate = (dt: Date, fmt?: string) => {
        const f = (fmt || 'YYYY-MM-DD')
        const Y = dt.getFullYear()
        const YY = String(Y).slice(-2)
        const MM = pad2(dt.getMonth() + 1)
        const DD = pad2(dt.getDate())
        const HH = pad2(dt.getHours())
        const mm = pad2(dt.getMinutes())
        const ss = pad2(dt.getSeconds())
        return f
          .replace(/YYYY/g, String(Y))
          .replace(/YYY/g, String(Y))
          .replace(/YY/g, YY)
          .replace(/MM/g, MM)
          .replace(/DD/g, DD)
          .replace(/HH/g, HH)
          .replace(/mm/g, mm)
          .replace(/ss/g, ss)
      }
      const resolveDate = (macro?: string, fmt?: string) => {
        const val = (macro || 'today').trim()
        const m = val.match(/^([a-zA-Z]+)([+-]\d+[dhwmy])?$/)
        const base = (m?.[1] || 'today').toLowerCase()
        const off = m?.[2]
        const now = new Date()
        let dt = new Date(now)
        if (base === 'today') { dt.setHours(0,0,0,0) }
        else if (base === 'yesterday') { dt.setDate(dt.getDate() - 1); dt.setHours(0,0,0,0) }
        else if (base === 'startofday') { dt.setHours(0,0,0,0) }
        else if (base === 'endofday') { dt.setHours(23,59,59,999) }
        else if (base === 'startofweek') { dt = startOfWeek(now) }
        else if (base === 'startofmonth') { dt = new Date(now.getFullYear(), now.getMonth(), 1); dt.setHours(0,0,0,0) }
        else if (base === 'startofquarter') { dt = startOfQuarter(now) }
        else if (base === 'startofyear') { dt = new Date(now.getFullYear(), 0, 1); dt.setHours(0,0,0,0) }
        else if (base === 'endofmonth' || base === 'eom') { dt = endOfMonth(now) }
        else if (base === 'endofyear' || base === 'eoy') { dt = endOfYear(now) }
        dt = applyOffset(dt, off)
        return formatDate(dt, fmt)
      }
      const ctx: Record<string, string> = {}
      for (const p of apiCfg.placeholders) {
        if (!p?.name) continue
        if (p.kind === 'date') ctx[p.name] = resolveDate(p.value || 'today', p.format)
        else ctx[p.name] = String(p.value || '')
      }
      const safeDecode = (s: string) => {
        try { return decodeURIComponent(s) } catch { return s }
      }
      const buildPreviewUrl = (): string => {
        const replacedBase = tokenReplace(apiCfg.endpoint || '', ctx)
        try {
          const u = new URL(replacedBase)
          for (const it of apiCfg.query) {
            if (!it.key) continue
            const k = it.key.includes('%') ? safeDecode(it.key) : it.key
            u.searchParams.set(k, tokenReplace(it.value || '', ctx))
          }
          return tokenReplace(u.toString(), ctx)
        } catch {
          const sp = new URLSearchParams()
          for (const it of apiCfg.query) {
            if (!it.key) continue
            const k = it.key.includes('%') ? safeDecode(it.key) : it.key
            sp.set(k, tokenReplace(it.value || '', ctx))
          }
          const q = sp.toString()
          if (!q) return replacedBase
          return replacedBase.includes('?') ? `${replacedBase}&${q}` : `${replacedBase}?${q}`
        }
      }
      const previewUrl = buildPreviewUrl()

      return (
      <div className="space-y-3">
        <div className="text-sm font-medium">API Endpoint</div>
        <label className="text-sm">Endpoint URL (supports placeholders like {`{start}`}, {`{end}`}, {`{base}`}, {`{quote}`})
          <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" placeholder="https://api.example.com/rates?base={base}&quote={quote}&start={start}&end={end}" value={apiCfg.endpoint} onChange={(e)=>setApiCfg({ ...apiCfg, endpoint: e.target.value })} />
        </label>
        <div className="grid grid-cols-2 gap-2">
          <label className="text-sm">Method
            <select className="w-full px-2 py-1.5 rounded-md border bg-background" value={apiCfg.method} onChange={(e)=>setApiCfg({ ...apiCfg, method: (e.target.value as any) })}>
              <option value="GET">GET</option>
              <option value="POST">POST</option>
            </select>
          </label>
          <label className="text-sm">Destination table in DuckDB
            <input className="w-full px-2 py-1.5 rounded-md border bg-background" placeholder="fx_rates" value={apiCfg.destTable} onChange={(e)=>setApiCfg({ ...apiCfg, destTable: e.target.value })} />
          </label>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <div>
            <div className="text-xs font-medium mb-1">Headers</div>
            <div className="space-y-1">
              {apiCfg.headers.map((h, i) => (
                <div key={i} className="flex items-center gap-1">
                  <input className="w-2/5 px-2 py-1.5 rounded-md border bg-background" placeholder="Key" value={h.key} onChange={(e)=>setApiCfg({ ...apiCfg, headers: apiCfg.headers.map((x,j)=> j===i ? { ...x, key: e.target.value } : x) })} />
                  <input className="w-3/5 px-2 py-1.5 rounded-md border bg-background" placeholder="Value (supports placeholders)" value={h.value} onChange={(e)=>setApiCfg({ ...apiCfg, headers: apiCfg.headers.map((x,j)=> j===i ? { ...x, value: e.target.value } : x) })} />
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
                  <input className="w-2/5 px-2 py-1.5 rounded-md border bg-background" placeholder="Key" value={q.key} onChange={(e)=>setApiCfg({ ...apiCfg, query: apiCfg.query.map((x,j)=> j===i ? { ...x, key: e.target.value } : x) })} />
                  <input className="w-3/5 px-2 py-1.5 rounded-md border bg-background" placeholder="Value (supports placeholders)" value={q.value} onChange={(e)=>setApiCfg({ ...apiCfg, query: apiCfg.query.map((x,j)=> j===i ? { ...x, value: e.target.value } : x) })} />
                  <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, query: apiCfg.query.filter((_,j)=>j!==i) })}>✕</button>
                </div>
              ))}
              <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, query: [...apiCfg.query, { key: '', value: '' }] })}>+ Add Param</button>
            </div>
          </div>
        </div>
        <div className="rounded-md border p-2">
          <div className="text-xs font-medium mb-1">Preview URL</div>
          <div className="flex items-center gap-2">
            <input className="flex-1 px-2 py-1.5 rounded-md border bg-background font-mono text-xs" readOnly value={previewUrl} />
            <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { if (previewUrl) window.open(previewUrl, '_blank') }}>Open</button>
            <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { if (previewUrl && navigator?.clipboard) navigator.clipboard.writeText(previewUrl) }}>Copy</button>
          </div>
          <div className="text-[11px] text-muted-foreground mt-1">Placeholders resolved with current values. Headers/auth are not applied in this URL.</div>
        </div>
        <div className="rounded-md border p-2">
          <div className="text-xs font-medium">Parsing</div>
          <label className="text-xs block mt-1">
            <input
              type="checkbox"
              className="mr-2"
              checked={apiCfg.parse === 'csv'}
              onChange={(e)=>setApiCfg({ ...apiCfg, parse: e.target.checked ? 'csv' : undefined })}
            />
            Force CSV parsing (adds {`{"parse":"csv"}`} to options)
          </label>
        </div>
        {apiCfg.method === 'POST' && (
          <label className="text-sm">Body (JSON; supports placeholders)
            <textarea className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background min-h-[80px] font-mono" placeholder='{"start":"{start}","end":"{end}","base":"{base}","quote":"{quote}"}' value={apiCfg.body} onChange={(e)=>setApiCfg({ ...apiCfg, body: e.target.value })} />
          </label>
        )}
        <div>
          <div className="text-xs font-medium mb-1">Placeholders</div>
          <div className="space-y-1">
            {apiCfg.placeholders.map((p, i) => (
              <div key={i} className="grid grid-cols-5 gap-1 items-center">
                <input className="col-span-2 px-2 py-1.5 rounded-md border bg-background" placeholder="name (e.g., start)" value={p.name} onChange={(e)=>setApiCfg({ ...apiCfg, placeholders: apiCfg.placeholders.map((x,j)=> j===i ? { ...x, name: e.target.value } : x) })} />
                <select className="px-2 py-1.5 rounded-md border bg-background" value={p.kind} onChange={(e)=>setApiCfg({ ...apiCfg, placeholders: apiCfg.placeholders.map((x,j)=> j===i ? { ...x, kind: (e.target.value as any) } : x) })}>
                  <option value="static">static</option>
                  <option value="date">date</option>
                </select>
                <input className="px-2 py-1.5 rounded-md border bg-background" placeholder={p.kind==='date' ? 'macro (e.g., today, today-1d, startOfMonth, EOM, EOY)' : 'value'} value={p.value || ''} onChange={(e)=>setApiCfg({ ...apiCfg, placeholders: apiCfg.placeholders.map((x,j)=> j===i ? { ...x, value: e.target.value } : x) })} />
                <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, placeholders: apiCfg.placeholders.filter((_,j)=>j!==i) })}>✕</button>
                {p.kind === 'date' && (
                  <div className="col-span-5">
                    <input
                      className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                      placeholder="format (e.g., YYYY-MM-DD, DD/MM/YYYY, YYYY/MM/DD, DD-MM-YYYY)"
                      value={p.format || ''}
                      onChange={(e)=>setApiCfg({ ...apiCfg, placeholders: apiCfg.placeholders.map((x,j)=> j===i ? { ...x, format: e.target.value } : x) })}
                    />
                  </div>
                )}
              </div>
            ))}
            <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setApiCfg({ ...apiCfg, placeholders: [...apiCfg.placeholders, { name: '', kind: 'static', value: '' }] })}>+ Add Placeholder</button>
          </div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <label className="text-sm">JSON Root Path (e.g., $.rates or data.items)
            <input className="w-full px-2 py-1.5 rounded-md border bg-background" placeholder="$.rates" value={apiCfg.jsonRoot} onChange={(e)=>setApiCfg({ ...apiCfg, jsonRoot: e.target.value })} />
          </label>
          <div className="rounded-md border p-2">
            <div className="text-xs font-medium">Gap Fill (forward-fill by date)</div>
            <label className="text-xs block mt-1"><input type="checkbox" className="mr-2" checked={apiCfg.gapFill.enabled} onChange={(e)=>setApiCfg({ ...apiCfg, gapFill: { ...apiCfg.gapFill, enabled: e.target.checked } })} />Enable</label>
            <label className="text-xs block">Key fields (comma-separated)
              <input className="w-full px-2 py-1 rounded-md border bg-background" placeholder="base,quote" value={apiCfg.gapFill.keyFields} onChange={(e)=>setApiCfg({ ...apiCfg, gapFill: { ...apiCfg.gapFill, keyFields: e.target.value } })} />
            </label>
            <label className="text-xs block">Date field name in dest table
              <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.gapFill.dateField} onChange={(e)=>setApiCfg({ ...apiCfg, gapFill: { ...apiCfg.gapFill, dateField: e.target.value } })} />
            </label>
          </div>
        </div>
        <div className="rounded-md border p-2">
          <div className="text-xs font-medium">Sequence (incremental windows)</div>
          <label className="text-xs block mt-1"><input type="checkbox" className="mr-2" checked={apiCfg.sequence.enabled} onChange={(e)=>setApiCfg({ ...apiCfg, sequence: { ...apiCfg.sequence, enabled: e.target.checked } })} />Enable</label>
          <div className="grid grid-cols-2 gap-2 mt-1">
            <label className="text-xs">Start param
              <input className="w-full px-2 py-1 rounded-md border bg-background" value={apiCfg.sequence.startParam} onChange={(e)=>setApiCfg({ ...apiCfg, sequence: { ...apiCfg.sequence, startParam: e.target.value } })} />
            </label>
            <label className="text-xs">End param
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
          <label className="text-sm">Write mode
            <select className="w-full px-2 py-1.5 rounded-md border bg-background" value={apiCfg.writeMode} onChange={(e)=>setApiCfg({ ...apiCfg, writeMode: e.target.value as any })}>
              <option value="append">Append</option>
              <option value="replace">Replace</option>
            </select>
          </label>
        </div>
      </div>
    )
    }
    if (type === 'postgres') return (
      <div className="grid grid-cols-2 gap-3 mt-2">
        <div><label className="text-sm">Host</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={pg.host} onChange={(e) => { setPg({ ...pg, host: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">Port</label><input type="number" className="w-full px-2 py-1.5 rounded-md border bg-background" value={pg.port} onChange={(e) => { setPg({ ...pg, port: Number(e.target.value || 5432) }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">Database</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={pg.db} onChange={(e) => { setPg({ ...pg, db: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">User</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={pg.user} onChange={(e) => { setPg({ ...pg, user: e.target.value }); setBuilderDirty(true) }} /></div>
        <div className="col-span-2"><label className="text-sm">Password</label><input type="password" className="w-full px-2 py-1.5 rounded-md border bg-background" value={pg.password} onChange={(e) => { setPg({ ...pg, password: e.target.value }); setBuilderDirty(true) }} /></div>
      </div>
    )
    if (type === 'supabase') return (
      <div className="grid grid-cols-2 gap-3 mt-2">
        <div className="col-span-2"><label className="text-sm">Project URL</label><input placeholder="https://xxx.supabase.co" className="w-full px-2 py-1.5 rounded-md border bg-background" value={sb.projectUrl} onChange={(e) => { setSb({ ...sb, projectUrl: e.target.value }); setBuilderDirty(true) }} /></div>
        <div className="col-span-2"><label className="text-sm">Service Role Key</label><input type="password" className="w-full px-2 py-1.5 rounded-md border bg-background" value={sb.serviceRoleKey} onChange={(e) => { setSb({ ...sb, serviceRoleKey: e.target.value }); setBuilderDirty(true) }} /></div>
      </div>
    )
    if (type === 'mysql') return (
      <div className="grid grid-cols-2 gap-3 mt-2">
        <div><label className="text-sm">Host</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={mysql.host} onChange={(e) => { setMysql({ ...mysql, host: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">Port</label><input type="number" className="w-full px-2 py-1.5 rounded-md border bg-background" value={mysql.port} onChange={(e) => { setMysql({ ...mysql, port: Number(e.target.value || 3306) }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">Database</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={mysql.db} onChange={(e) => { setMysql({ ...mysql, db: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">User</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={mysql.user} onChange={(e) => { setMysql({ ...mysql, user: e.target.value }); setBuilderDirty(true) }} /></div>
        <div className="col-span-2"><label className="text-sm">Password</label><input type="password" className="w-full px-2 py-1.5 rounded-md border bg-background" value={mysql.password} onChange={(e) => { setMysql({ ...mysql, password: e.target.value }); setBuilderDirty(true) }} /></div>
      </div>
    )
    if (type === 'mssql') return (
      <div className="grid grid-cols-2 gap-3 mt-2">
        <div className="col-span-2"><label className="text-sm">Driver Type</label>
          <select className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.driverType} onChange={(e) => { setMssql({ ...mssql, driverType: e.target.value as any }); setBuilderDirty(true) }}>
            <option value="pyodbc">pyodbc (requires system ODBC driver)</option>
            <option value="pytds">pytds (no ODBC driver)</option>
          </select>
        </div>
        <div><label className="text-sm">Host</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.host} onChange={(e) => { setMssql({ ...mssql, host: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">Port</label><input type="number" className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.port} onChange={(e) => { setMssql({ ...mssql, port: Number(e.target.value || 1433) }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">Database</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.db} onChange={(e) => { setMssql({ ...mssql, db: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">User</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.user} onChange={(e) => { setMssql({ ...mssql, user: e.target.value }); setBuilderDirty(true) }} /></div>
        <div className="col-span-2"><label className="text-sm">Password</label><input type="password" className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.password} onChange={(e) => { setMssql({ ...mssql, password: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">ODBC Driver</label>
          <select className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.driver} onChange={(e) => { setMssql({ ...mssql, driver: e.target.value as any }); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'}>
            <option value="ODBC Driver 18 for SQL Server">ODBC Driver 18 for SQL Server</option>
            <option value="ODBC Driver 17 for SQL Server">ODBC Driver 17 for SQL Server</option>
          </select>
        </div>
        <div className="flex items-center gap-2 mt-7">
          <input type="checkbox" className="scale-110" checked={mssql.encrypt} onChange={(e) => { setMssql({ ...mssql, encrypt: e.target.checked }); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} />
          <label className="text-sm">Encrypt</label>
        </div>
        <div className="flex items-center gap-2 mt-7">
          <input type="checkbox" className="scale-110" checked={mssql.trustServerCertificate} onChange={(e) => { setMssql({ ...mssql, trustServerCertificate: e.target.checked }); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} />
          <label className="text-sm">Trust server certificate</label>
        </div>
        <div><label className="text-sm">Login timeout (sec)</label><input type="number" className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.connectTimeout} onChange={(e) => { setMssql({ ...mssql, connectTimeout: Math.max(0, Number(e.target.value) || 0) }); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} /></div>
        <div className="flex items-center gap-2 mt-7">
          <input type="checkbox" className="scale-110" checked={mssql.useOdbcConnect} onChange={(e) => { setMssql({ ...mssql, useOdbcConnect: e.target.checked }); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} />
          <label className="text-sm">Use odbc_connect style</label>
        </div>
        <div><label className="text-sm">Connect retry count</label><input type="number" className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.connectRetryCount} onChange={(e) => { setMssql({ ...mssql, connectRetryCount: Math.max(0, Number(e.target.value) || 0) }); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} /></div>
        <div><label className="text-sm">Connect retry interval (sec)</label><input type="number" className="w-full px-2 py-1.5 rounded-md border bg-background" value={mssql.connectRetryInterval} onChange={(e) => { setMssql({ ...mssql, connectRetryInterval: Math.max(0, Number(e.target.value) || 0) }); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} /></div>
        <div className="flex items-center gap-2 mt-7">
          <input type="checkbox" className="scale-110" checked={mssql.pooling} onChange={(e) => { setMssql({ ...mssql, pooling: e.target.checked }); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} />
          <label className="text-sm">Pooling</label>
        </div>
        {/* SQLAlchemy pool clamp */}
        <div className="col-span-2 mt-2">
          <label className="flex items-center gap-2 text-sm">
            <input type="checkbox" className="scale-110" checked={sqlPoolClamp} onChange={(e) => { setSqlPoolClamp(e.target.checked); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} />
            Clamp pool (size=1, overflow=0, timeout=5)
          </label>
        </div>
        {sqlPoolClamp && (
          <>
            <div><label className="text-sm">Pool size</label><input type="number" className="w-full px-2 py-1.5 rounded-md border bg-background" value={sqlPoolSize} onChange={(e) => { setSqlPoolSize(Math.max(0, Number(e.target.value) || 0)); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} /></div>
            <div><label className="text-sm">Max overflow</label><input type="number" className="w-full px-2 py-1.5 rounded-md border bg-background" value={sqlPoolOverflow} onChange={(e) => { setSqlPoolOverflow(Math.max(0, Number(e.target.value) || 0)); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} /></div>
            <div><label className="text-sm">Pool timeout (sec)</label><input type="number" className="w-full px-2 py-1.5 rounded-md border bg-background" value={sqlPoolTimeout} onChange={(e) => { setSqlPoolTimeout(Math.max(0, Number(e.target.value) || 0)); setBuilderDirty(true) }} disabled={mssql.driverType !== 'pyodbc'} /></div>
          </>
        )}
        <div className="col-span-2 text-[12px] text-muted-foreground">{mssql.driverType === 'pyodbc' ? 'pyodbc requires a system ODBC driver (msodbcsql18).' : 'pytds does not require a system ODBC driver.'}</div>
      </div>
    )
    if (type === 'duckdb') return (
      <div className="grid grid-cols-2 gap-3 mt-2">
        <div className="col-span-2"><label className="text-sm">File path</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={duckdbPath} onChange={(e) => { setDuckdbPath(e.target.value); setBuilderDirty(true) }} /></div>
      </div>
    )
    if (type === 'sqlite') return (
      <div className="grid grid-cols-2 gap-3 mt-2">
        <div className="col-span-2"><label className="text-sm">File path</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={sqlitePath} onChange={(e) => { setSqlitePath(e.target.value); setBuilderDirty(true) }} /></div>
      </div>
    )
    if (type === 'snowflake') return (
      <div className="grid grid-cols-2 gap-3 mt-2">
        <div><label className="text-sm">Account</label><input placeholder="xy12345.eu-central-1" className="w-full px-2 py-1.5 rounded-md border bg-background" value={snow.account} onChange={(e) => { setSnow({ ...snow, account: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">User</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={snow.user} onChange={(e) => { setSnow({ ...snow, user: e.target.value }); setBuilderDirty(true) }} /></div>
        <div className="col-span-2"><label className="text-sm">Password</label><input type="password" className="w-full px-2 py-1.5 rounded-md border bg-background" value={snow.password} onChange={(e) => { setSnow({ ...snow, password: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">Warehouse</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={snow.warehouse} onChange={(e) => { setSnow({ ...snow, warehouse: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">Database</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={snow.database} onChange={(e) => { setSnow({ ...snow, database: e.target.value }); setBuilderDirty(true) }} /></div>
        <div><label className="text-sm">Schema</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={snow.schema} onChange={(e) => { setSnow({ ...snow, schema: e.target.value }); setBuilderDirty(true) }} /></div>
      </div>
    )
    if (type === 'bigquery') return (
      <div className="grid grid-cols-2 gap-3 mt-2">
        <div><label className="text-sm">Project ID</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={bq.projectId} onChange={(e) => { setBq({ ...bq, projectId: e.target.value }); setBuilderDirty(true) }} /></div>
        <div className="col-span-2"><label className="text-sm">Credentials JSON (optional)</label><input className="w-full px-2 py-1.5 rounded-md border bg-background" value={bq.credentialsJson} onChange={(e) => { setBq({ ...bq, credentialsJson: e.target.value }); setBuilderDirty(true) }} /></div>
      </div>
    )
    return null
  }

  const Icon = useMemo(() => CONNECTORS.find((c) => c.id === type)?.icon || RiDatabase2Line, [type])

  return (
    <Dialog.Root open={open} onOpenChange={onOpenChangeAction}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/30" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[920px] max-w-[95vw] h-[90vh] max-h-[90vh] -translate-x-1/2 -translate-y-1/2 rounded-xl border bg-[hsl(var(--card))] shadow-none p-0 overflow-hidden">
          <div className="flex h-full min-h-0">
            {/* Vertical connector tabs */}
            <aside className="w-56 shrink-0 bg-sidebar-light dark:bg-sidebar-dark border-r border-[hsl(var(--border))] p-3 overflow-auto">
              <div className="flex items-center gap-2 px-2 py-1.5 mb-2">
                <Icon className="w-5 h-5 text-blue-600" />
                <span className="text-sm font-medium">{mode === 'create' ? 'Add' : 'Edit'} Datasource</span>
              </div>
              <nav className="space-y-1">
                {CONNECTORS.map((c) => (
                  <button key={c.id} className={`w-full text-left sidebar-item-light dark:sidebar-item-dark ${type===c.id ? 'sidebar-item-active-light dark:sidebar-item-active-dark' : ''}`} onClick={() => { setType(c.id as any); setBuilderDirty(false) }}>
                    <span className="inline-flex items-center gap-2">
                      <c.icon className="w-4 h-4" />
                      {c.label}
                    </span>
                  </button>
                ))}
              </nav>
            </aside>

            {/* Main content */}
            <div className="flex-1 flex flex-col min-h-0">
              <div className="flex items-center justify-between px-4 py-3 border-b border-[hsl(var(--border))]">
                <Dialog.Title className="text-base font-semibold flex items-center gap-2">
                  <Icon className="w-5 h-5 text-blue-600" />
                  {mode === 'create' ? 'Add Datasource' : `Edit ${initial?.name || 'Datasource'}`}
                </Dialog.Title>
                <Dialog.Close asChild>
                  <button className="text-sm px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]">✕</button>
                </Dialog.Close>
              </div>

              <div className="flex-1 overflow-y-auto p-4 space-y-4">

              {/* General */}
              <section className="rounded-md border p-3">
                <h3 className="text-sm font-semibold mb-2">General</h3>
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="text-sm">Name</label>
                    <input className="w-full px-2 py-1.5 rounded-md border bg-background" value={name} onChange={(e) => setName(e.target.value)} />
                  </div>
                  <div>
                    <label className="text-sm">Type</label>
                    <div className="w-full px-2 py-1.5 rounded-md border bg-background flex items-center justify-between">
                      <span className="text-sm text-gray-600 dark:text-gray-300">{CONNECTORS.find((c) => c.id === type)?.label}</span>
                      <Icon className="w-4 h-4 text-blue-600" />
                    </div>
                    <p className="text-[12px] text-muted-foreground mt-1">Change type via the left sidebar.</p>
                  </div>
                </div>
                {type !== 'api' && (
                  <div className="mt-3">
                    <label className="text-sm">Connection URI (DSN)</label>
                    <input className="w-full px-2 py-1.5 rounded-md border bg-background" value={dsn} onChange={(e) => setDsn(e.target.value)} placeholder="driver://user:pass@host:port/db" />
                  </div>
                )}
                <div className="mt-3">
                  <label className="flex items-center gap-2 text-sm">
                    <input type="checkbox" className="scale-110" checked={active} onChange={(e) => setActive(e.target.checked)} />
                    Active
                  </label>
                </div>
                <div className="mt-3">
                  <label className="text-sm">Options (JSON)</label>
                  <textarea className="w-full min-h-[140px] px-2 py-1 rounded-md border bg-background font-mono text-xs" value={optionsJson} onChange={(e) => setOptionsJson(e.target.value)} placeholder='e.g. {"sslmode":"require"}' />
                </div>
              </section>

              {/* Query defaults */}
              <section className="rounded-md border p-3">
                <h3 className="text-sm font-semibold mb-2">Query defaults</h3>
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="text-sm">Default pivot parallelism (1–8)</label>
                    <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" type="number" min={1} max={8} value={defaultPivotPar} onChange={(e) => setDefaultPivotPar(Math.max(1, Math.min(8, Number(e.target.value) || 2)))} />
                  </div>
                </div>
              </section>

              {/* Connector (flat) */}
              <section className="rounded-md border p-3">
                <h3 className="text-sm font-semibold mb-2">Connector</h3>
                {renderConnectorFields()}
              </section>

              {/* Test (flat) */}
              {type !== 'api' && (
                <section className="rounded-md border p-3">
                  <h3 className="text-sm font-semibold mb-2">Test</h3>
                  <div className="flex items-center gap-2 flex-wrap">
                    <button className="px-3 py-1.5 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={onTest} disabled={testing}>{testing ? 'Testing…' : 'Test Connection'}</button>
                    <button className="px-3 py-1.5 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={onDetect} disabled={detecting}>{detecting ? 'Detecting…' : 'Detect Server'}</button>
                    {test && (test.ok ? <span className="text-sm text-emerald-600">OK</span> : <span className="text-sm text-rose-600">{test.error || 'Failed'}</span>)}
                  </div>
                  {detect && (
                    <div className="mt-2 text-sm">
                      {detect.error ? (
                        <div className="text-rose-600">Detect: {detect.error}</div>
                      ) : (
                        <div className="text-muted-foreground">
                          Detected: <b>{detect.detected || (detect.candidates?.join('/') || 'unknown')}</b>{' '}
                          {detect.method ? `(${detect.method})` : ''}
                        </div>
                      )}
                    </div>
                  )}
                </section>
                )}

                {/* Sync Settings */}
                <section className="rounded-md border p-3">
                  <h3 className="text-sm font-semibold mb-2">Sync Settings</h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                    <label className="text-sm">Maximum concurrent queries
                      <input
                        className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                        type="number"
                        min={1}
                        value={syncMaxConcurrent}
                        onChange={(e) => setSyncMaxConcurrent(Math.max(1, Number(e.target.value) || 1))}
                      />
                    </label>
                    <div className="text-[12px] text-muted-foreground md:mt-7">Limits simultaneous sync tasks for this datasource.</div>
                  </div>
                  <div className="mt-3">
                    <div className="text-sm font-medium mb-1">Blackout periods (no sync allowed)</div>
                    <div className="space-y-2">
                      {blackout.map((w, idx) => (
                        <div key={idx} className="flex items-center gap-2">
                          <label className="text-xs">Start
                            <input
                              className="ml-1 w-[120px] px-2 py-1 rounded-md border bg-background"
                              type="time"
                              value={w.start || ''}
                              onChange={(e) => setBlackout((arr) => arr.map((x, i) => i === idx ? { ...x, start: e.target.value } : x))}
                            />
                          </label>
                          <label className="text-xs">End
                            <input
                              className="ml-1 w-[120px] px-2 py-1 rounded-md border bg-background"
                              type="time"
                              value={w.end || ''}
                              onChange={(e) => setBlackout((arr) => arr.map((x, i) => i === idx ? { ...x, end: e.target.value } : x))}
                            />
                          </label>
                          <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={() => setBlackout((arr) => arr.filter((_, i) => i !== idx))}>Remove</button>
                        </div>
                      ))}
                      <button type="button" className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={() => setBlackout((arr) => [...arr, { start: '22:00', end: '06:00' }])}>+ Add Blackout</button>
                    </div>
                  </div>
                </section>
              </div>

              <div className="px-4 py-3 border-t border-[hsl(var(--border))] flex items-center justify-end gap-2">
                <Dialog.Close asChild>
                  <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-[hsl(var(--muted))]">Cancel</button>
                </Dialog.Close>
                <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-[hsl(var(--muted))]" onClick={onSubmit} disabled={saving}>{saving ? (mode === 'create' ? 'Creating…' : 'Saving…') : (mode === 'create' ? 'Create' : 'Save')}</button>
              </div>
            </div>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
