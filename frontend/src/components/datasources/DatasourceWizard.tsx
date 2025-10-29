"use client"

import { useEffect, useMemo, useState } from 'react'
import { Card, Title, Text, TabGroup, TabList, Tab, TabPanels, TabPanel, TextInput, Select, SelectItem, NumberInput, Button, Badge, Callout } from '@tremor/react'
import { Api, DatasourceCreate, DatasourceOut } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'

export type WizardProps = {
  onCreatedAction?: (ds: DatasourceOut) => void
}

type Pg = { host: string; port: number; db: string; user: string; password: string }

type Sb = { projectUrl: string; serviceRoleKey: string }

type Mysql = { host: string; port: number; db: string; user: string; password: string }

type Snow = { account: string; user: string; password: string; warehouse: string; database: string; schema: string }

type Bq = { projectId: string; credentialsJson: string }

export default function DatasourceWizard({ onCreatedAction }: WizardProps) {
  const { user } = useAuth()
  const DEFAULT_DUCK = process.env.NEXT_PUBLIC_DEFAULT_DUCKDB_PATH || ''
  const DEFAULT_SQLITE = process.env.NEXT_PUBLIC_DEFAULT_SQLITE_PATH || ''
  const [type, setType] = useState<'duckdb'|'postgres'|'supabase'|'mysql'|'sqlite'|'bigquery'|'snowflake'>('duckdb')
  const [name, setName] = useState('New Datasource')
  const [dsn, setDsn] = useState('')
  // Sync settings
  const [syncMaxConcurrent, setSyncMaxConcurrent] = useState<number>(1)
  const [blackout, setBlackout] = useState<Array<{ start: string; end: string }>>([])

  // Per-connector state
  const [pg, setPg] = useState<Pg>({ host: 'localhost', port: 5432, db: 'postgres', user: 'postgres', password: '' })
  const [sb, setSb] = useState<Sb>({ projectUrl: '', serviceRoleKey: '' })
  const [mysql, setMysql] = useState<Mysql>({ host: 'localhost', port: 3306, db: 'mysql', user: 'root', password: '' })
  const [snow, setSnow] = useState<Snow>({ account: '', user: '', password: '', warehouse: '', database: '', schema: '' })
  const [bq, setBq] = useState<Bq>({ projectId: '', credentialsJson: '' })
  const [duckdbPath, setDuckdbPath] = useState<string>(DEFAULT_DUCK)
  const [sqlitePath, setSqlitePath] = useState<string>(DEFAULT_SQLITE)

  useEffect(() => {
    // Auto-build DSN for known connectors
    if (type === 'postgres') {
      const next = `postgresql+psycopg://${encodeURIComponent(pg.user)}:${encodeURIComponent(pg.password)}@${pg.host}:${pg.port}/${pg.db}`
      setDsn(next)
    } else if (type === 'supabase') {
      // https://<ref>.supabase.co => db.<ref>.supabase.co
      let ref = ''
      try { const u = new URL(sb.projectUrl); ref = (u.hostname.split('.')[0] || '') } catch {}
      const host = ref ? `db.${ref}.supabase.co` : ''
      const next = host ? `postgresql+psycopg://${encodeURIComponent('postgres')}:${encodeURIComponent(sb.serviceRoleKey)}@${host}:5432/postgres` : ''
      setDsn(next)
    } else if (type === 'mysql') {
      const next = `mysql+pymysql://${encodeURIComponent(mysql.user)}:${encodeURIComponent(mysql.password)}@${mysql.host}:${mysql.port}/${mysql.db}`
      setDsn(next)
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
  }, [type, pg, sb, mysql, snow, bq, duckdbPath, sqlitePath])

  const [testing, setTesting] = useState(false)
  const [test, setTest] = useState<{ ok: boolean; error?: string } | null>(null)
  const [creating, setCreating] = useState(false)
  const [created, setCreated] = useState<DatasourceOut | null>(null)
  const [loadingSchema, setLoadingSchema] = useState(false)
  const [schema, setSchema] = useState<any | null>(null)

  const onTest = async () => {
    setTesting(true)
    setTest(null)
    try {
      const res = await Api.testConnection(dsn || undefined)
      setTest({ ok: !!res.ok, error: res.error || undefined })
    } catch (e: unknown) {
      setTest({ ok: false, error: e instanceof Error ? e.message : 'Failed' })
    } finally { setTesting(false) }
  }

  const onCreate = async () => {
    setCreating(true)
    setCreated(null)
    try {
      const sync = {
        maxConcurrentQueries: Math.max(1, Number(syncMaxConcurrent) || 1),
        blackoutDaily: blackout.filter((w) => (w.start || '').includes(':') && (w.end || '').includes(':')),
      }
      const payload: DatasourceCreate = { name, type, connectionUri: dsn || undefined, options: { sync }, userId: user?.id }
      const res = await Api.createDatasource(payload)
      setCreated(res)
      onCreatedAction?.(res)
    } catch (e: unknown) {
      setTest({ ok: false, error: e instanceof Error ? e.message : 'Failed to create' })
    } finally { setCreating(false) }
  }

  const onLoadSchema = async () => {
    if (!created) return
    setLoadingSchema(true)
    try {
      const s = await Api.introspect(created.id)
      setSchema(s)
    } catch (e: unknown) {
      setTest({ ok: false, error: e instanceof Error ? e.message : 'Failed to introspect' })
    } finally { setLoadingSchema(false) }
  }

  return (
    <Card>
      <Title>Add Datasource</Title>
      <Text className="mt-1">Connector specific forms, test connection, and optional load/sync steps.</Text>
      <div className="grid grid-cols-2 gap-3 mt-4">
        <div>
          <Text>Name</Text>
          <TextInput value={name} onChange={(e) => setName(e.target.value)} />
        </div>
        <div>
          <Text>Type</Text>
          <Select value={type} onValueChange={(v) => setType(v as any)}>
            <SelectItem value="duckdb">DuckDB</SelectItem>
            <SelectItem value="postgres">Postgres</SelectItem>
            <SelectItem value="supabase">Supabase</SelectItem>
            <SelectItem value="mysql">MySQL</SelectItem>
            <SelectItem value="sqlite">SQLite</SelectItem>
            <SelectItem value="bigquery">BigQuery</SelectItem>
            <SelectItem value="snowflake">Snowflake</SelectItem>
          </Select>
        </div>
      </div>

      <TabGroup className="mt-4">
        <TabList>
          <Tab>Connector</Tab>
          <Tab>Test</Tab>
          <Tab disabled={!created}>Load</Tab>
          <Tab disabled={!created}>Sync</Tab>
        </TabList>
        <TabPanels>
          <TabPanel>
            {/* Connector specific fields */}
            {type === 'postgres' && (
              <div className="grid grid-cols-2 gap-3 mt-2">
                <div><Text>Host</Text><TextInput value={pg.host} onChange={(e) => setPg({ ...pg, host: e.target.value })} /></div>
                <div><Text>Port</Text><NumberInput value={pg.port} onValueChange={(v) => setPg({ ...pg, port: Number(v || 5432) })} /></div>
                <div><Text>Database</Text><TextInput value={pg.db} onChange={(e) => setPg({ ...pg, db: e.target.value })} /></div>
                <div><Text>User</Text><TextInput value={pg.user} onChange={(e) => setPg({ ...pg, user: e.target.value })} /></div>
                <div className="col-span-2"><Text>Password</Text><TextInput type="password" value={pg.password} onChange={(e) => setPg({ ...pg, password: e.target.value })} /></div>
              </div>
            )}
            {type === 'supabase' && (
              <div className="grid grid-cols-2 gap-3 mt-2">
                <div className="col-span-2"><Text>Project URL</Text><TextInput placeholder="https://xxx.supabase.co" value={sb.projectUrl} onChange={(e) => setSb({ ...sb, projectUrl: e.target.value })} /></div>
                <div className="col-span-2"><Text>Service Role Key</Text><TextInput type="password" value={sb.serviceRoleKey} onChange={(e) => setSb({ ...sb, serviceRoleKey: e.target.value })} /></div>
              </div>
            )}
            {type === 'mysql' && (
              <div className="grid grid-cols-2 gap-3 mt-2">
                <div><Text>Host</Text><TextInput value={mysql.host} onChange={(e) => setMysql({ ...mysql, host: e.target.value })} /></div>
                <div><Text>Port</Text><NumberInput value={mysql.port} onValueChange={(v) => setMysql({ ...mysql, port: Number(v || 3306) })} /></div>
                <div><Text>Database</Text><TextInput value={mysql.db} onChange={(e) => setMysql({ ...mysql, db: e.target.value })} /></div>
                <div><Text>User</Text><TextInput value={mysql.user} onChange={(e) => setMysql({ ...mysql, user: e.target.value })} /></div>
                <div className="col-span-2"><Text>Password</Text><TextInput type="password" value={mysql.password} onChange={(e) => setMysql({ ...mysql, password: e.target.value })} /></div>
              </div>
            )}
            {type === 'duckdb' && (
              <div className="grid grid-cols-2 gap-3 mt-2">
                <div className="col-span-2"><Text>File path</Text><TextInput value={duckdbPath} onChange={(e) => setDuckdbPath(e.target.value)} /></div>
              </div>
            )}
            {type === 'sqlite' && (
              <div className="grid grid-cols-2 gap-3 mt-2">
                <div className="col-span-2"><Text>File path</Text><TextInput value={sqlitePath} onChange={(e) => setSqlitePath(e.target.value)} /></div>
              </div>
            )}
            {type === 'snowflake' && (
              <div className="grid grid-cols-2 gap-3 mt-2">
                <div><Text>Account</Text><TextInput placeholder="xy12345.eu-central-1" value={snow.account} onChange={(e) => setSnow({ ...snow, account: e.target.value })} /></div>
                <div><Text>User</Text><TextInput value={snow.user} onChange={(e) => setSnow({ ...snow, user: e.target.value })} /></div>
                <div className="col-span-2"><Text>Password</Text><TextInput type="password" value={snow.password} onChange={(e) => setSnow({ ...snow, password: e.target.value })} /></div>
                <div><Text>Warehouse</Text><TextInput value={snow.warehouse} onChange={(e) => setSnow({ ...snow, warehouse: e.target.value })} /></div>
                <div><Text>Database</Text><TextInput value={snow.database} onChange={(e) => setSnow({ ...snow, database: e.target.value })} /></div>
                <div><Text>Schema</Text><TextInput value={snow.schema} onChange={(e) => setSnow({ ...snow, schema: e.target.value })} /></div>
              </div>
            )}
            {type === 'bigquery' && (
              <div className="grid grid-cols-2 gap-3 mt-2">
                <div><Text>Project ID</Text><TextInput value={bq.projectId} onChange={(e) => setBq({ ...bq, projectId: e.target.value })} /></div>
                <div className="col-span-2"><Text>Credentials JSON (optional)</Text><TextInput value={bq.credentialsJson} onChange={(e) => setBq({ ...bq, credentialsJson: e.target.value })} /></div>
              </div>
            )}

            <div className="mt-4">
              <Text>Connection URI (DSN)</Text>
              <TextInput value={dsn} onChange={(e) => setDsn(e.target.value)} placeholder="driver://user:pass@host:port/db" />
              <div className="mt-3 flex items-center gap-2">
                <Button onClick={onTest} loading={testing}>Test Connection</Button>
                <Button onClick={onCreate} loading={creating} variant="secondary">Create</Button>
                {test && (
                  test.ok ? <Badge color="emerald">Connection OK</Badge> : <Callout color="rose" title="Connection failed">{test.error || 'Failed'}</Callout>
                )}
              </div>
              {created && (
                <div className="mt-2 text-sm">Created datasource <span className="font-mono">{created.name}</span> (id: <span className="font-mono">{created.id}</span>)</div>
              )}
            </div>
          </TabPanel>

          <TabPanel>
            <div className="mt-2">
              <Text>Use the connector step to build a DSN, then test it here.</Text>
              <div className="mt-3 flex items-center gap-2">
                <Button onClick={onTest} loading={testing}>Test Connection</Button>
                {test && (test.ok ? <Badge color="emerald">OK</Badge> : <Badge color="rose">Failed</Badge>)}
              </div>
            </div>
          </TabPanel>

          <TabPanel>
            <div className="mt-2">
              <Text>Load metadata from the source (schemas & tables).</Text>
              <div className="mt-3 flex items-center gap-2">
                <Button onClick={onLoadSchema} loading={loadingSchema}>Load schema</Button>
                {schema && <Badge color="blue">{schema.schemas?.length || 0} schemas</Badge>}
              </div>
              {schema && (
                <div className="mt-3 text-sm max-h-[200px] overflow-auto">
                  <pre className="text-xs whitespace-pre-wrap">{JSON.stringify(schema, null, 2)}</pre>
                </div>
              )}
            </div>
          </TabPanel>

          <TabPanel>
            <div className="mt-2 space-y-3">
              <Title>Sync Settings</Title>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                <div>
                  <Text>Maximum concurrent queries</Text>
                  <NumberInput min={1} value={syncMaxConcurrent} onValueChange={(v) => setSyncMaxConcurrent(Math.max(1, Number(v) || 1))} />
                </div>
                <div className="md:mt-6"><Text className="opacity-70">Limits simultaneous sync tasks for this datasource.</Text></div>
              </div>
              <div>
                <Text>Blackout periods (no sync allowed)</Text>
                <div className="space-y-2 mt-2">
                  {blackout.map((w, idx) => (
                    <div key={idx} className="flex items-center gap-2">
                      <div>
                        <Text className="text-xs">Start</Text>
                        <input type="time" className="px-2 py-1 rounded-md border bg-background" value={w.start || ''} onChange={(e) => setBlackout((arr) => arr.map((x, i) => i === idx ? { ...x, start: e.target.value } : x))} />
                      </div>
                      <div>
                        <Text className="text-xs">End</Text>
                        <input type="time" className="px-2 py-1 rounded-md border bg-background" value={w.end || ''} onChange={(e) => setBlackout((arr) => arr.map((x, i) => i === idx ? { ...x, end: e.target.value } : x))} />
                      </div>
                      <Button variant="secondary" onClick={() => setBlackout((arr) => arr.filter((_, i) => i !== idx))}>Remove</Button>
                    </div>
                  ))}
                  <Button variant="secondary" onClick={() => setBlackout((arr) => [...arr, { start: '22:00', end: '06:00' }])}>+ Add Blackout</Button>
                </div>
              </div>
            </div>
          </TabPanel>
        </TabPanels>
      </TabGroup>
    </Card>
  )
}
