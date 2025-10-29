"use client"

import { Suspense, useEffect, useMemo, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Card, Title, Text, Badge, BarList } from '@tremor/react'
import { useAuth } from '@/components/providers/AuthProvider'
import { Api } from '@/lib/api'

export const dynamic = 'force-dynamic'

export default function AdminMetricsPage() {
  const { user } = useAuth()
  const router = useRouter()
  const isAdmin = (user?.role || '').toLowerCase() === 'admin'

  const [data, setData] = useState<any | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState<boolean>(true)
  const [poll, setPoll] = useState<boolean>(true)

  useEffect(() => {
    if (!isAdmin) router.replace('/home')
  }, [isAdmin, router])

  const load = async () => {
    try {
      setErr(null)
      const res = await Api.getMetricsLive(user?.id)
      setData(res)
    } catch (e: any) {
      setErr(e?.message || 'Failed to load metrics')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    let timer: any
    const tick = async () => { await load() }
    if (poll) {
      tick()
      timer = setInterval(tick, 3000)
    } else {
      // one-shot
      void load()
    }
    return () => { try { if (timer) clearInterval(timer) } catch {} }
  }, [poll, user?.id])

  const q = data?.query || {}
  const inflight = q?.inflight || {}
  const cache = q?.cache || {}
  const rates = q?.rateLimited || 0
  const durs = q?.durationsMs || {}
  const scheduler = data?.scheduler || {}
  const actors = (data?.actors?.recent || []) as Array<{ id: string; lastAt: string }>
  const alerts = (data?.alerts?.scheduled || []) as Array<any>
  const embeddings = (data?.embeddings?.jobs || []) as Array<any>
  const openDash = data?.dashboards?.open || {}
  const notifications = data?.notifications || {}

  const inflightBreakdown = useMemo(() => {
    const items: Array<{ name: string; value: number }> = []
    const add = (k: string, label: string) => { const v = Number(inflight?.[k] || 0); if (v > 0) items.push({ name: label, value: v }) }
    add('duckdb', 'Query (DuckDB)')
    add('sqlalchemy', 'Query (SQLAlchemy)')
    add('distinct', 'Distinct')
    add('period_totals', 'Period Totals')
    add('period_totals_batch', 'Period Totals (Batch)')
    add('period_totals_compare', 'Period Totals (Compare)')
    return items.sort((a,b)=>b.value-a.value)
  }, [JSON.stringify(inflight || {})])

  if (!isAdmin) return null

  return (
    <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-semibold">Live Metrics</h1>
          <p className="text-sm text-muted-foreground">Server load and tasks. Auto-refresh {poll ? 'on' : 'off'}.</p>
        </div>
        <div className="flex items-center gap-2">
          <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" onClick={()=> setPoll((v)=>!v)}>{poll ? 'Pause' : 'Resume'}</button>
          <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" onClick={()=> void load()}>Refresh</button>
          <a className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" href="/api/metrics" target="_blank" rel="noreferrer">Prometheus</a>
        </div>
      </div>

      {err && (
        <div className="text-sm text-rose-600">{err}</div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Running Queries</Title></div>
          <div className="p-3 space-y-3">
            <div className="flex items-center justify-between">
              <Text>Total in-flight</Text>
              <Badge color={Number(inflight?.total||0) > 0 ? 'blue' : 'gray'}>{Number(inflight?.total||0)}</Badge>
            </div>
            <BarList data={inflightBreakdown.map(it => ({ name: it.name, value: it.value }))} className="mt-2"/>
          </div>
        </Card>
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Cache</Title></div>
          <div className="p-3 space-y-2">
            <div className="flex items-center justify-between"><Text>Hits</Text><Badge color="emerald">{Number(cache?.hits||0)}</Badge></div>
            <div className="flex items-center justify-between"><Text>Misses</Text><Badge color="orange">{Number(cache?.misses||0)}</Badge></div>
            <div className="flex items-center justify-between"><Text>Hit Ratio</Text><Badge color="blue">{cache?.hitRatio != null ? Math.round(Number(cache?.hitRatio)*100)+'%' : '—'}</Badge></div>
          </div>
        </Card>
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Latency & Limits</Title></div>
          <div className="p-3 space-y-2">
            <div className="flex items-center justify-between"><Text>Avg duration</Text><Badge color="violet">{durs?.avg != null ? `${Math.round(Number(durs?.avg))} ms` : '—'}</Badge></div>
            <div className="flex items-center justify-between"><Text>Samples</Text><Badge>{Number(durs?.count||0)}</Badge></div>
            <div className="flex items-center justify-between"><Text>Rate limited (total)</Text><Badge color="rose">{Number(rates||0)}</Badge></div>
          </div>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-3">
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Open Dashboards</Title></div>
          <div className="p-3 space-y-2">
            <div className="flex items-center justify-between"><Text>Total</Text><Badge color="blue">{Number(openDash?.total||0)}</Badge></div>
            <div className="flex items-center justify-between"><Text>Builder</Text><Badge>{Number(openDash?.builder?.total||0)}</Badge></div>
            <div className="flex items-center justify-between"><Text>Public</Text><Badge>{Number(openDash?.public?.total||0)}</Badge></div>
            <div className="mt-2">
              <Text className="text-xs text-muted-foreground">Top Builder Sessions by Dashboard</Text>
              <div className="mt-1 space-y-1">
                {(openDash?.builder?.byId || []).slice(0,6).map((it: any) => (
                  <div key={it.dashboardId} className="flex items-center justify-between text-xs">
                    <span className="truncate max-w-[220px] font-mono">{it.dashboardId}</span>
                    <Badge>{it.sessions}</Badge>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </Card>
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Notifications (Email)</Title></div>
          <div className="p-3 space-y-2">
            <div className="flex items-center justify-between"><Text>Sent</Text><Badge color="emerald">{Number(notifications?.email?.sent||0)}</Badge></div>
            <div className="flex items-center justify-between"><Text>Failed</Text><Badge color="rose">{Number(notifications?.email?.failed||0)}</Badge></div>
          </div>
        </Card>
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Notifications (SMS)</Title></div>
          <div className="p-3 space-y-2">
            <div className="flex items-center justify-between"><Text>Sent</Text><Badge color="emerald">{Number(notifications?.sms?.sent||0)}</Badge></div>
            <div className="flex items-center justify-between"><Text>Failed</Text><Badge color="rose">{Number(notifications?.sms?.failed||0)}</Badge></div>
          </div>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Scheduler Jobs</Title></div>
          <div className="p-3">
            {(scheduler?.jobs || []).length === 0 && (<Text className="text-muted-foreground">No jobs scheduled</Text>)}
            <div className="space-y-2">
              {(scheduler?.jobs || []).map((j: any) => (
                <div key={j.id} className="flex items-center justify-between text-sm border rounded-md px-2 py-1">
                  <div className="truncate"><span className="font-mono text-xs bg-muted rounded px-1 py-0.5 mr-2">{j.id}</span>{j.dsId ? `DS ${j.dsId}` : j.alertId ? `Alert ${j.alertId}` : ''}</div>
                  <div className="text-muted-foreground text-xs whitespace-nowrap">next: {j.nextRunAt || '—'}</div>
                </div>
              ))}
            </div>
          </div>
        </Card>
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Recent Actors</Title></div>
          <div className="p-3">
            {actors.length === 0 && (<Text className="text-muted-foreground">No recent actors</Text>)}
            <div className="space-y-2">
              {actors.map((a) => (
                <div key={a.id} className="flex items-center justify-between text-sm border rounded-md px-2 py-1">
                  <div className="truncate"><span className="font-mono text-xs bg-muted rounded px-1 py-0.5 mr-2">{a.id}</span></div>
                  <div className="text-muted-foreground text-xs whitespace-nowrap">{a.lastAt}</div>
                </div>
              ))}
            </div>
          </div>
        </Card>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Alerts (scheduled)</Title></div>
          <div className="p-3">
            {(alerts || []).length === 0 && (<Text className="text-muted-foreground">No alert jobs</Text>)}
            <div className="space-y-2">
              {(alerts || []).map((j: any) => (
                <div key={j.id} className="flex items-center justify-between text-sm border rounded-md px-2 py-1">
                  <div className="truncate"><span className="font-mono text-xs bg-muted rounded px-1 py-0.5 mr-2">{j.id}</span></div>
                  <div className="text-muted-foreground text-xs whitespace-nowrap">next: {j.nextRunAt || '—'}</div>
                </div>
              ))}
            </div>
          </div>
        </Card>
        <Card className="p-0">
          <div className="px-3 py-2 border-b"><Title>Embeddings</Title></div>
          <div className="p-3">
            {(embeddings || []).length === 0 && (<Text className="text-muted-foreground">No embedding jobs</Text>)}
            <div className="space-y-2">
              {(embeddings || []).map((j: any, i: number) => (
                <div key={i} className="flex items-center justify-between text-sm border rounded-md px-2 py-1">
                  <div className="truncate"><span className="font-mono text-xs bg-muted rounded px-1 py-0.5 mr-2">{j.name || 'job'}</span></div>
                  <div className="text-muted-foreground text-xs whitespace-nowrap">{j.status || ''}</div>
                </div>
              ))}
            </div>
          </div>
        </Card>
      </div>

      <Card className="p-0">
        <div className="px-3 py-2 border-b"><Title>Raw Snapshot</Title></div>
        <div className="p-3">
          <pre className="text-xs overflow-auto max-h-[320px]">{JSON.stringify(data?.raw || {}, null, 2)}</pre>
        </div>
      </Card>
    </div>
    </Suspense>
  )
}
