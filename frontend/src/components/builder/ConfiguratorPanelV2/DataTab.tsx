"use client"
import { useEffect, useMemo, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Api, type DatasourceOut, type IntrospectResponse } from '@/lib/api'
import { PivotBuilder, type PivotAssignments } from '@/components/builder/PivotBuilder'
import AdvancedSqlDialog from '@/components/builder/AdvancedSqlDialog'
import type { WidgetConfig } from '@/types/widgets'
import { Select, SelectTrigger, SelectContent, SelectItem, SelectValue } from '@/components/Select'
import { useAuth } from '@/components/providers/AuthProvider'
import * as SchemaCache from '@/lib/schemaCache'
import { SectionCard, FormRow, inputCls, selectCls, ActiveBadge } from './shared'
import { FieldDetailPanel, InlineCustomColEditor, InlineMeasureEditor } from './DataTabHelpers'

export function DataTab({ local, setLocal, updateConfig, samplesByField, allFieldNames, numericFields, dateLikeFields, search = '' }: {
  local: WidgetConfig; setLocal: (c: WidgetConfig) => void; updateConfig: (c: WidgetConfig) => void
  samplesByField: Record<string,string[]>; allFieldNames: string[]; numericFields: string[]; dateLikeFields: string[]; search?: string
}) {
  const { user } = useAuth()
  const s = search.toLowerCase().trim()
  const matches = (...terms: string[]) => !s || terms.some(t => t.toLowerCase().includes(s))

  const [srcFilter, setSrcFilter] = useState('')
  const [selKind, setSelKind] = useState<'x'|'value'|'legend'|'filter'|null>(null)
  const [selField, setSelField] = useState<string|undefined>()
  const [advOpen, setAdvOpen] = useState(false)
  const [editingCustom, setEditingCustom] = useState<{id?:string;name:string;formula:string;type?:string}|null>(null)
  const [editingMeasure, setEditingMeasure] = useState<{id?:string;name:string;formula:string}|null>(null)

  // ── Datasource queries (pass user?.id to scope the list correctly) ──────────
  const dsQ = useQuery({
    queryKey: ['datasources', user?.id],
    queryFn: () => Api.listDatasources(undefined, user?.id),
  })

  const dsId = local.datasourceId as string|undefined
  const defaultDsId = useMemo(() => {
    try { return typeof window !== 'undefined' ? localStorage.getItem('default_ds_id') : null } catch { return null }
  }, [])
  const effectiveDsId = dsId ?? (defaultDsId || undefined)

  // ── Schema (full introspect, cached) ─────────────────────────────────────────
  const schemaQ = useQuery({
    queryKey: ['ds-schema', effectiveDsId ?? '_local'],
    queryFn: () => effectiveDsId ? Api.introspect(effectiveDsId) : Api.introspectLocal(),
    retry: 0, refetchOnWindowFocus: false, staleTime: 5*60*1000,
    initialData: effectiveDsId ? (SchemaCache.get(effectiveDsId)||undefined) : undefined,
  })
  useEffect(() => {
    if (effectiveDsId && schemaQ.data) SchemaCache.set(effectiveDsId, schemaQ.data as IntrospectResponse)
  }, [effectiveDsId, schemaQ.data])

  // ── Fast table list via tablesOnly (same pattern as old panel) ───────────────
  const [tablesFast, setTablesFast] = useState<{value:string;label:string}[]>([])
  useEffect(() => {
    if (!effectiveDsId) { setTablesFast([]); return }
    let cancelled = false
    ;(async () => {
      try {
        const fast = await (Api as any).tablesOnly(effectiveDsId)
        if (cancelled) return
        const items: {value:string;label:string}[] = []
        ;(fast?.schemas||[]).forEach((sch:any) => {
          ;(sch?.tables||[]).forEach((t:string) => {
            const key = sch.name ? `${sch.name}.${t}` : t
            items.push({ value: key, label: t })
          })
        })
        setTablesFast(items)
      } catch { if (!cancelled) setTablesFast([]) }
    })()
    return () => { cancelled = true }
  }, [effectiveDsId])

  const isRoleSchema = (n: string) => {
    const v = String(n||'').toLowerCase()
    return v.startsWith('db_') || ['information_schema','sys','guest','pg_catalog'].includes(v)
  }

  const sourceItems = useMemo(() => {
    if (tablesFast.length > 0) {
      const filtered = tablesFast.filter(it => !isRoleSchema(String(it.value||'').split('.')[0]))
      const uniq = new Map<string,{value:string;label:string}>()
      filtered.forEach(it => { if (!uniq.has(it.value)) uniq.set(it.value, it) })
      return Array.from(uniq.values())
    }
    const data = schemaQ.data as IntrospectResponse|undefined
    if (!data) return [] as {value:string;label:string}[]
    const out: {value:string;label:string}[] = []
    ;(data.schemas||[]).filter(sch=>!isRoleSchema(sch.name)).forEach(sch=>{
      ;(sch.tables||[]).forEach(t => out.push({ value:`${sch.name}.${t.name}`, label:t.name }))
    })
    return out
  }, [tablesFast, schemaQ.data])

  const filteredSources = useMemo(() => {
    const q = srcFilter.trim().toLowerCase()
    return q ? sourceItems.filter(it=>it.label.toLowerCase().includes(q)||it.value.toLowerCase().includes(q)) : sourceItems
  }, [sourceItems, srcFilter])

  // ── Pivot ─────────────────────────────────────────────────────────────────────
  const pivot: PivotAssignments = useMemo(() => {
    const p = local.pivot || { values:[], filters:[] }
    return { x:p.x, values:Array.isArray(p.values)?p.values:[], legend:p.legend, filters:Array.isArray(p.filters)?p.filters:[] }
  }, [local.pivot])
  const applyPivot = (p: PivotAssignments) => { const next={...local,pivot:p as any}; setLocal(next); updateConfig(next) }

  const activeFilterCount = useMemo(() =>
    local.querySpec?.where ? Object.values(local.querySpec.where).filter(v=>v!=null).length : 0
  , [local.querySpec?.where])

  const patch = (p: Partial<WidgetConfig>) => { const next={...local,...p}; setLocal(next); updateConfig(next) }
  const patchOpt = (p: Partial<NonNullable<WidgetConfig['options']>>) => {
    const next={...local,options:{...(local.options||{}),...p}}; setLocal(next); updateConfig(next)
  }

  if (['composition','report','text','spacer'].includes(local.type))
    return <div className="text-xs text-muted-foreground p-2">No data configuration for this widget type.</div>

  const dsEntry = useMemo(() => {
    const list = (dsQ.data as DatasourceOut[]|undefined)||[]
    return list.find(d => d.id === (local.datasourceId||''))
  }, [dsQ.data, local.datasourceId])

  const schemaForAdv = schemaQ.data as IntrospectResponse|undefined

  return (
    <div className="space-y-3">

      {/* ── Source ──────────────────────────────────────────────────────────── */}
      {matches('source','query mode','sql','spec','datasource','routing') && (
        <SectionCard title="Source">
          {/* Query mode toggle */}
          <div>
            <label className="block text-xs text-muted-foreground mb-1.5">Query mode</label>
            <div className="grid grid-cols-2 gap-2">
              {(['sql','spec'] as const).map(m=>(
                <label key={m} className={`flex items-center justify-center p-2 rounded-md border cursor-pointer text-xs transition-colors duration-150 ${(local.queryMode||'sql')===m?'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))] font-semibold':'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>
                  <input type="radio" className="sr-only" name="qmV2" checked={(local.queryMode||'sql')===m}
                    onChange={()=>{
                      const next={...local,queryMode:m}
                      if(m==='spec'&&!next.querySpec) next.querySpec={source:'',select:[]}
                      setLocal(next); updateConfig(next)
                    }} />
                  {m==='sql' ? 'SQL' : 'Spec (Ibis)'}
                </label>
              ))}
            </div>
          </div>

          {/* Datasource picker — shows name not ID via SelectValue */}
          <FormRow label="Datasource" full>
            <Select value={local.datasourceId||''} onValueChangeAction={(val:string)=>{
              const next={...local,datasourceId:val||undefined} as WidgetConfig
              if(next.querySpec) next.querySpec={...next.querySpec,source:'',select:[]}
              setLocal(next); updateConfig(next)
            }}>
              <SelectTrigger className="h-8 text-xs rounded-md bg-[hsl(var(--secondary))] px-3 border border-[hsl(var(--border))] w-full">
                <span className="truncate text-xs">
                  {dsEntry ? `${dsEntry.name} (${dsEntry.type})` : '(Default: DuckDB)'}
                </span>
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="">(Default: DuckDB)</SelectItem>
                {((dsQ.data as DatasourceOut[]|undefined)||[]).map(ds=>(
                  <SelectItem key={ds.id} value={ds.id}>{ds.name} ({ds.type})</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </FormRow>

          {/* Prefer local DuckDB */}
          <FormRow label="Prefer local DuckDB">
            <Switch_
              checked={!!local.options?.preferLocalDuck}
              onChangeAction={v=>patchOpt({preferLocalDuck:v||undefined})} />
          </FormRow>
          <div className="flex justify-end pt-1">
            <button
              className="text-xs px-2.5 py-1 rounded-md border hover:bg-muted transition-colors duration-150 cursor-pointer"
              onClick={()=>setAdvOpen(true)}>
              Advanced SQL…
            </button>
          </div>
        </SectionCard>
      )}

      {/* ── Advanced SQL Dialog (shared across both query modes) ─────────────── */}
      {advOpen && (
        <AdvancedSqlDialog
          open={advOpen}
          onCloseAction={()=>setAdvOpen(false)}
          datasourceId={effectiveDsId}
          schema={schemaForAdv}
          source={local.querySpec?.source}
          widgetId={local.id}
        />
      )}

      {/* ── SQL Editor ──────────────────────────────────────────────────────── */}
      {(!local.queryMode||local.queryMode==='sql') && matches('sql','query') && (
        <SectionCard title="SQL Query">
          <textarea
            className="w-full px-2.5 py-2 rounded-md bg-[hsl(var(--secondary))] font-mono text-xs border border-[hsl(var(--border))] focus:outline-none focus:ring-1 focus:ring-[hsl(var(--primary))] resize-y"
            rows={10} value={local.sql||''}
            onChange={e=>patch({sql:e.target.value})}
            placeholder="SELECT * FROM my_table LIMIT 1000" />
        </SectionCard>
      )}

      {/* ── Table / View picker ─────────────────────────────────────────────── */}
      {local.queryMode==='spec' && matches('table','view','source','refresh') && (
        <SectionCard title="Table / View">
          <div className="space-y-2">
            <input className={inputCls()} placeholder="Search tables…" value={srcFilter} onChange={e=>setSrcFilter(e.target.value)} />
            <Select
              value={local.querySpec?.source||''}
              onValueChangeAction={(src:string)=>{
                const next={...local,querySpec:{
                  source:src,
                  sourceTableId:effectiveDsId?`${effectiveDsId}__${src}`:src,
                  select:[], where:undefined,
                  x:undefined as any, y:undefined as any,
                  legend:undefined as any, measure:undefined as any,
                  agg:undefined as any, groupBy:undefined as any,
                }}
                setLocal(next); updateConfig(next)
              }}>
              <SelectTrigger
                className="h-8 w-full text-xs rounded-md bg-[hsl(var(--secondary))] px-3 border border-[hsl(var(--border))]"
                disabled={schemaQ.isLoading && tablesFast.length===0}>
                <SelectValue placeholder={schemaQ.isLoading&&tablesFast.length===0?'Loading…':'Select table or view…'} />
              </SelectTrigger>
              <SelectContent>
                {filteredSources.length===0 && (
                  <div className="text-xs text-muted-foreground px-3 py-2">
                    {schemaQ.isLoading ? 'Loading tables…' : 'No tables found'}
                  </div>
                )}
                {filteredSources.map(it=><SelectItem key={it.value} value={it.value}>{it.label}</SelectItem>)}
              </SelectContent>
            </Select>
            <div className="flex justify-end">
              <button
                className="text-xs px-2.5 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] transition-colors duration-150 cursor-pointer"
                onClick={()=>{
                  if(typeof window!=='undefined'&&local?.id){
                    try{window.dispatchEvent(new CustomEvent('request-table-columns',{detail:{widgetId:local.id}} as any))}catch{}
                    try{window.dispatchEvent(new CustomEvent('request-table-rows',{detail:{widgetId:local.id}} as any))}catch{}
                    try{window.dispatchEvent(new CustomEvent('request-table-samples',{detail:{widgetId:local.id}} as any))}catch{}
                  }
                }}>
                Refresh fields
              </button>
            </div>
          </div>
        </SectionCard>
      )}

      {/* ── Data Fields (Pivot) ─────────────────────────────────────────────── */}
      {local.queryMode==='spec' && ['chart','kpi','table'].includes(local.type) && matches('data fields','pivot','filters','x axis','values','legend') && (
        <SectionCard title="Data Fields" badge={activeFilterCount>0?<ActiveBadge count={activeFilterCount}/>:undefined}>
          <div className="rounded-md p-2 bg-[hsl(var(--secondary))]">
            <PivotBuilder
              fields={allFieldNames}
              measures={local.measures||[]}
              assignments={pivot}
              numericFields={numericFields}
              dateLikeFields={dateLikeFields}
              update={applyPivot}
              selectFieldAction={(kind,field)=>{
                if(selKind===kind&&selField===field){ setSelKind(null); setSelField(undefined) }
                else { setSelKind(kind as any); setSelField(field) }
              }}
              selected={selKind&&selField?{kind:selKind,id:selField}:undefined}
              disableRows={false} disableValues={false}
              allowMultiLegend allowMultiRows
              datasourceId={effectiveDsId as any}
            />
          </div>
          {selKind&&selField&&(
            <FieldDetailPanel
              selKind={selKind} selField={selField}
              local={local} setLocal={setLocal} updateConfig={updateConfig}
              pivot={pivot} applyPivot={applyPivot}
              samplesByField={samplesByField} numericFields={numericFields} dateLikeFields={dateLikeFields}
              onClose={()=>{ setSelKind(null); setSelField(undefined) }}
            />
          )}
        </SectionCard>
      )}

      {/* ── Custom Columns ─────────────────────────────────────────────────── */}
      {local.queryMode==='spec' && matches('custom column','formula','computed') && (
        <SectionCard title="Custom Columns">
          <div className="space-y-2">
            {(local.customColumns||[]).length===0&&!editingCustom&&(
              <div className="text-xs text-muted-foreground">No custom columns yet.</div>
            )}
            {(local.customColumns||[]).map((col,i)=>(
              editingCustom?.id===col.id ? (
                <InlineCustomColEditor key={col.id||i} value={editingCustom!}
                  onSave={v=>{
                    const cols=(local.customColumns||[]).map((c,j)=>j===i?{...c,...v,type:v.type as 'number'|'string'|'date'|'boolean'}:c)
                    const next={...local,customColumns:cols}; setLocal(next); updateConfig(next); setEditingCustom(null)
                  }}
                  onCancel={()=>setEditingCustom(null)} />
              ) : (
                <div key={col.id||i} className="flex items-center justify-between gap-2 rounded-md border bg-[hsl(var(--secondary)/0.4)] px-2.5 py-1.5">
                  <div className="flex-1 min-w-0">
                    <div className="text-xs font-semibold truncate">{col.name}</div>
                    <div className="text-[10px] text-muted-foreground font-mono truncate">{col.formula}</div>
                  </div>
                  <div className="flex items-center gap-1 shrink-0">
                    <button className="text-xs px-1.5 py-0.5 rounded border hover:bg-muted transition-colors duration-150 cursor-pointer"
                      onClick={()=>setEditingCustom({id:col.id,name:col.name,formula:col.formula,type:col.type})}>Edit</button>
                    <button className="text-xs px-1.5 py-0.5 rounded border hover:bg-muted transition-colors duration-150 cursor-pointer"
                      onClick={()=>{ const next={...local,customColumns:(local.customColumns||[]).filter((_,j)=>j!==i)}; setLocal(next); updateConfig(next) }}>✕</button>
                  </div>
                </div>
              )
            ))}
            {editingCustom&&!editingCustom.id&&(
              <InlineCustomColEditor value={editingCustom}
                onSave={v=>{
                  const cols=[...(local.customColumns||[]),{id:crypto.randomUUID(),name:v.name,formula:v.formula,type:v.type as 'number'|'string'|'date'|'boolean'}]
                  const next={...local,customColumns:cols}; setLocal(next); updateConfig(next); setEditingCustom(null)
                }}
                onCancel={()=>setEditingCustom(null)} />
            )}
            {!editingCustom&&(
              <button className="text-xs px-2.5 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] transition-colors duration-150 cursor-pointer"
                onClick={()=>setEditingCustom({name:'',formula:''})}>+ New custom column</button>
            )}
          </div>
        </SectionCard>
      )}

      {/* ── Measures ───────────────────────────────────────────────────────── */}
      {local.queryMode==='spec' && ['chart','kpi','table'].includes(local.type) && matches('measure','formula') && (
        <SectionCard title="Measures">
          <div className="space-y-2">
            {(local.measures||[]).length===0&&!editingMeasure&&(
              <div className="text-xs text-muted-foreground">No measures yet.</div>
            )}
            {(local.measures||[]).map((m,i)=>(
              editingMeasure?.id===m.id ? (
                <InlineMeasureEditor key={m.id} value={editingMeasure}
                  onSave={v=>{
                    const ms=(local.measures||[]).map((x,j)=>j===i?{...x,...v}:x)
                    const next={...local,measures:ms}; setLocal(next); updateConfig(next); setEditingMeasure(null)
                  }}
                  onCancel={()=>setEditingMeasure(null)} />
              ) : (
                <div key={m.id||i} className="flex items-center justify-between gap-2 rounded-md border bg-[hsl(var(--secondary)/0.4)] px-2.5 py-1.5">
                  <div className="flex-1 min-w-0">
                    <div className="text-xs font-semibold truncate">{m.name}</div>
                    <div className="text-[10px] text-muted-foreground font-mono truncate">{m.formula}</div>
                  </div>
                  <div className="flex items-center gap-1 shrink-0">
                    <button className="text-xs px-1.5 py-0.5 rounded border hover:bg-muted transition-colors duration-150 cursor-pointer"
                      onClick={()=>setEditingMeasure({id:m.id,name:m.name,formula:m.formula})}>Edit</button>
                    <button className="text-xs px-1.5 py-0.5 rounded border hover:bg-muted transition-colors duration-150 cursor-pointer"
                      onClick={()=>{ const next={...local,measures:(local.measures||[]).filter((_,j)=>j!==i)}; setLocal(next); updateConfig(next) }}>✕</button>
                  </div>
                </div>
              )
            ))}
            {editingMeasure&&!editingMeasure.id&&(
              <InlineMeasureEditor value={editingMeasure}
                onSave={v=>{
                  const ms=[...(local.measures||[]),v]
                  const next={...local,measures:ms}; setLocal(next); updateConfig(next); setEditingMeasure(null)
                }}
                onCancel={()=>setEditingMeasure(null)} />
            )}
            {!editingMeasure&&(
              <button className="text-xs px-2.5 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)] transition-colors duration-150 cursor-pointer"
                onClick={()=>setEditingMeasure({name:'',formula:''})}>+ New measure</button>
            )}
          </div>
        </SectionCard>
      )}

      {/* ── Sort & Limit ────────────────────────────────────────────────────── */}
      {local.queryMode==='spec' && matches('sort','limit','top n','direction') && (
        <SectionCard title="Sort & Limit">
          <FormRow label="Sort by">
            <select className={selectCls()} value={local.options?.dataDefaults?.sort?.by||''}
              onChange={e=>{
                const dd={...(local.options?.dataDefaults||{}),sort:{...(local.options?.dataDefaults?.sort||{}),by:e.target.value as any||undefined}}
                patchOpt({dataDefaults:dd})
              }}>
              <option value="">Default</option>
              <option value="x">X</option>
              <option value="value">Value</option>
            </select>
          </FormRow>
          <FormRow label="Direction">
            <select className={selectCls()} value={local.options?.dataDefaults?.sort?.direction||'desc'}
              onChange={e=>{
                const dd={...(local.options?.dataDefaults||{}),sort:{...(local.options?.dataDefaults?.sort||{}),direction:e.target.value as any}}
                patchOpt({dataDefaults:dd})
              }}>
              <option value="asc">Ascending</option>
              <option value="desc">Descending</option>
            </select>
          </FormRow>
          <FormRow label="Top N">
            <input type="number" min={1} className={inputCls('w-20')}
              value={local.options?.dataDefaults?.topN?.n ?? ''}
              placeholder="All"
              onChange={e=>{
                const v = e.target.value===''?undefined:{n:Number(e.target.value)}
                const dd={...(local.options?.dataDefaults||{}),topN:v}
                patchOpt({dataDefaults:dd})
              }} />
          </FormRow>
        </SectionCard>
      )}

    </div>
  )
}

// inline Switch to avoid circular import issues
function Switch_({ checked, onChangeAction }: { checked: boolean; onChangeAction: (v: boolean) => void }) {
  return (
    <button
      role="switch" aria-checked={checked}
      onClick={()=>onChangeAction(!checked)}
      className={`relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200 focus:outline-none ${checked?'bg-[hsl(var(--primary))]':'bg-[hsl(var(--input))]'}`}>
      <span className={`pointer-events-none inline-block h-4 w-4 rounded-full bg-white shadow-lg ring-0 transition-transform duration-200 ${checked?'translate-x-4':'translate-x-0'}`} />
    </button>
  )
}
