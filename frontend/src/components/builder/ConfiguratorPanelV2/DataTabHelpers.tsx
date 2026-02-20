"use client"
import { useState, useEffect } from 'react'
import type { WidgetConfig } from '@/types/widgets'
import type { PivotAssignments } from '@/components/builder/PivotBuilder'
import { Switch } from '@/components/Switch'
import { FormRow, inputCls, selectCls } from './shared'
import { chartColors, tokenToColorKey, colorKeyToToken, type AvailableChartColorsKeys } from '@/lib/chartUtils'
import { Api } from '@/lib/api'

const DATE_PRESETS = [
  'today','yesterday','this_week','last_week','this_month','last_month',
  'this_quarter','last_quarter','this_year','last_year','last_7_days','last_30_days','last_90_days',
]
const FORMAT_OPTIONS = [
  'none','short','abbrev','currency','percent','bytes','wholeNumber','number',
  'thousands','millions','billions','oneDecimal','twoDecimals','percentWhole','percentOneDecimal','timeHours','timeMinutes',
]
const X_DATE_PRESETS = [
  {value:'none',label:'Auto (by grouping)'},{value:'YYYY',label:'YYYY'},{value:'YYYY-MM',label:'YYYY-MM'},
  {value:'YYYY-MM-DD',label:'YYYY-MM-DD'},{value:'h:mm a',label:'HR:MM AM/PM'},{value:'dddd',label:'DDDD (weekday)'},
  {value:'MMMM',label:'MMMM (month name)'},{value:'MMM-YYYY',label:'MMM-YYYY'},{value:'custom',label:'Custom…'},
]
const X_DATE_PRESET_SET = new Set(['YYYY','YYYY-MM','YYYY-MM-DD','h:mm a','dddd','MMMM','MMM-YYYY'])

// ── Filter rule sub-components ───────────────────────────────────────────────
function StringRuleDetails({ field, onPatch }: { field: string; onPatch: (p: Record<string,any>) => void }) {
  type StrOp = 'contains'|'not_contains'|'eq'|'ne'|'starts_with'|'ends_with'
  const [op, setOp] = useState<StrOp>('contains')
  const [val, setVal] = useState('')
  return (
    <div className="space-y-2">
      <FormRow label="Operator">
        <select className={selectCls()} value={op} onChange={e=>setOp(e.target.value as StrOp)}>
          <option value="contains">contains</option>
          <option value="not_contains">not contains</option>
          <option value="eq">equals</option>
          <option value="ne">not equals</option>
          <option value="starts_with">starts with</option>
          <option value="ends_with">ends with</option>
        </select>
      </FormRow>
      <FormRow label="Value" full>
        <input className={inputCls()} value={val} onChange={e=>setVal(e.target.value)} placeholder="value…" />
      </FormRow>
      <button className="text-xs px-2.5 py-1 rounded-md border bg-primary text-primary-foreground hover:bg-primary/90 cursor-pointer"
        onClick={()=>{ if(val.trim()) onPatch({[`${field}__${op}`]:val.trim()}) }}>Apply rule</button>
    </div>
  )
}

function NumberFilterDetails({ field, onPatch }: { field: string; onPatch: (p: Record<string,any>) => void }) {
  type NumberOp = 'eq'|'ne'|'gt'|'gte'|'lt'|'lte'|'between'
  const [op, setOp] = useState<NumberOp>('gte')
  const [val, setVal] = useState('')
  const [val2, setVal2] = useState('')
  return (
    <div className="space-y-2">
      <FormRow label="Operator">
        <select className={selectCls()} value={op} onChange={e=>setOp(e.target.value as NumberOp)}>
          <option value="eq">= equals</option>
          <option value="ne">≠ not equals</option>
          <option value="gt">&gt; greater than</option>
          <option value="gte">≥ at least</option>
          <option value="lt">&lt; less than</option>
          <option value="lte">≤ at most</option>
          <option value="between">between</option>
        </select>
      </FormRow>
      <FormRow label={op==='between'?'From':'Value'}>
        <input type="number" className={inputCls()} value={val} onChange={e=>setVal(e.target.value)} placeholder="0" />
      </FormRow>
      {op==='between' && (
        <FormRow label="To">
          <input type="number" className={inputCls()} value={val2} onChange={e=>setVal2(e.target.value)} placeholder="0" />
        </FormRow>
      )}
      <button className="text-xs px-2.5 py-1 rounded-md border bg-primary text-primary-foreground hover:bg-primary/90 cursor-pointer"
        onClick={()=>{
          if(!val) return
          if(op==='between') onPatch({[`${field}__gte`]:Number(val),[`${field}__lte`]:Number(val2||val)})
          else onPatch({[`${field}__${op}`]:Number(val)})
        }}>Apply rule</button>
    </div>
  )
}

function DateRuleDetails({ field, onPatch }: { field: string; onPatch: (p: Record<string,any>) => void }) {
  type DateMode = 'preset'|'custom'
  type DatePreset = 'today'|'yesterday'|'this_week'|'last_week'|'this_month'|'last_month'|'this_quarter'|'last_quarter'|'this_year'|'last_year'
  type CustomOp = 'after'|'before'|'between'
  const [mode, setMode] = useState<DateMode>('preset')
  const [preset, setPreset] = useState<DatePreset>('this_month')
  const [customOp, setCustomOp] = useState<CustomOp>('after')
  const [d1, setD1] = useState('')
  const [d2, setD2] = useState('')
  const DATE_RULE_PRESETS: DatePreset[] = ['today','yesterday','this_week','last_week','this_month','last_month','this_quarter','last_quarter','this_year','last_year']
  return (
    <div className="space-y-2">
      <div className="flex gap-1 rounded-md border p-0.5 bg-muted/30 w-fit">
        {(['preset','custom'] as DateMode[]).map(m=>(
          <button key={m} onClick={()=>setMode(m)}
            className={`text-xs px-2.5 py-0.5 rounded cursor-pointer capitalize ${mode===m?'bg-background shadow-sm font-medium':''}`}>{m}</button>
        ))}
      </div>
      {mode==='preset' ? (
        <div className="space-y-2">
          <select className={selectCls('w-full')} value={preset} onChange={e=>setPreset(e.target.value as DatePreset)}>
            {DATE_RULE_PRESETS.map(p=><option key={p} value={p}>{p.replace(/_/g,' ')}</option>)}
          </select>
          <button className="text-xs px-2.5 py-1 rounded-md border bg-primary text-primary-foreground hover:bg-primary/90 cursor-pointer"
            onClick={()=>onPatch({[`${field}__date_preset`]:preset,[`${field}__gte`]:undefined,[`${field}__lt`]:undefined})}>Apply</button>
        </div>
      ) : (
        <div className="space-y-2">
          <FormRow label="Condition">
            <select className={selectCls()} value={customOp} onChange={e=>setCustomOp(e.target.value as CustomOp)}>
              <option value="after">after</option>
              <option value="before">before</option>
              <option value="between">between</option>
            </select>
          </FormRow>
          <FormRow label={customOp==='between'?'From':'Date'}>
            <input type="date" className={inputCls()} value={d1} onChange={e=>setD1(e.target.value)} />
          </FormRow>
          {customOp==='between' && (
            <FormRow label="To">
              <input type="date" className={inputCls()} value={d2} onChange={e=>setD2(e.target.value)} />
            </FormRow>
          )}
          <button className="text-xs px-2.5 py-1 rounded-md border bg-primary text-primary-foreground hover:bg-primary/90 cursor-pointer"
            onClick={()=>{
              if(!d1) return
              if(customOp==='after') onPatch({[`${field}__gte`]:d1,[`${field}__date_preset`]:undefined})
              else if(customOp==='before') onPatch({[`${field}__lt`]:d1,[`${field}__date_preset`]:undefined})
              else onPatch({[`${field}__gte`]:d1,[`${field}__lt`]:d2||d1,[`${field}__date_preset`]:undefined})
            }}>Apply</button>
        </div>
      )}
    </div>
  )
}

// ── FieldDetails ──────────────────────────────────────────────────────────────
export function FieldDetails({ kind, field, local, setLocal, updateConfig, pivot, applyPivot, samplesByField, numericFields, dateLikeFields }: {
  kind: 'x'|'value'|'legend'|'filter'; field: string
  local: WidgetConfig; setLocal: (c: WidgetConfig) => void; updateConfig: (c: WidgetConfig) => void
  pivot: PivotAssignments; applyPivot: (p: PivotAssignments) => void
  samplesByField: Record<string,string[]>; numericFields: string[]; dateLikeFields: string[]
}) {
  const [filterTab, setFilterTab] = useState<'manual'|'rule'>('manual')
  const [ruleType, setRuleType] = useState<'auto'|'string'|'number'|'date'>('auto')
  // Local samples state — populated from prop OR from live event
  const [localSamples, setLocalSamples] = useState<string[]>(() => samplesByField[field] || [])

  // Keep local samples in sync when parent provides new ones
  useEffect(() => {
    const s = samplesByField[field] || []
    if (s.length > 0) setLocalSamples(s)
  }, [samplesByField, field])

  // When filter panel opens: fetch distinct values via API, fall back to widget event
  useEffect(() => {
    if (kind !== 'filter') return
    const source = (local.querySpec as any)?.source as string | undefined
    const datasourceId = (local as any).datasourceId as string | undefined
    const widgetId = local.id

    // Primary: server-side distinct endpoint
    let aborted = false
    if (source && field) {
      Api.distinct({ source, field, datasourceId })
        .then(res => {
          if (aborted) return
          const vals = ((res?.values || []) as any[]).map(v => v != null ? String(v) : null).filter(Boolean) as string[]
          const dedup = Array.from(new Set(vals)).sort()
          if (dedup.length > 0) setLocalSamples(dedup)
        })
        .catch(() => {})
    }

    // Secondary: widget event (widget may have already computed samples)
    if (typeof window === 'undefined' || !widgetId) return
    try { window.dispatchEvent(new CustomEvent('request-table-samples', { detail: { widgetId } })) } catch {}
    const handler = (e: Event) => {
      const d = (e as CustomEvent).detail
      if (d?.widgetId !== widgetId) return
      const s: string[] = d?.samples?.[field] || []
      if (s.length > 0) setLocalSamples(prev => prev.length > 0 ? prev : s)
    }
    window.addEventListener('table-sample-values-change', handler)
    return () => { aborted = true; window.removeEventListener('table-sample-values-change', handler) }
  }, [kind, field, local.id, (local.querySpec as any)?.source, (local as any).datasourceId])

  const ve = pivot.values.find(v => (v.measureId || v.field) === field)
  const isNum = numericFields.includes(field)
  const isDate = dateLikeFields.includes(field)
  const samples = localSamples.length > 0 ? localSamples : (samplesByField[field] || [])
  const where = (local.querySpec?.where as any) || {}
  const advancedMode = !!(local.options as any)?.advancedMode

  const patchWhere = (p: Record<string,any>) => {
    const w = { ...where, ...p }
    Object.keys(w).forEach(k => { if (w[k] === undefined) delete w[k] })
    const next = { ...local, querySpec: { ...(local.querySpec || { source:'', select:[] }), where: w } }
    setLocal(next); updateConfig(next)
  }
  const patchValue = (p: object) =>
    applyPivot({ ...pivot, values: pivot.values.map(v => (v.measureId||v.field) === field ? { ...v, ...p } : v) })
  const patchOpt = (p: Partial<NonNullable<WidgetConfig['options']>>) => {
    const next = { ...local, options: { ...(local.options || {}), ...p } }
    setLocal(next); updateConfig(next)
  }

  // ── value ───────────────────────────────────────────────────────────────────
  if (kind === 'value') {
    const isMeasure = !!ve?.measureId
    const currentColor = tokenToColorKey((ve?.colorToken as 1|2|3|4|5|undefined) || 1)
    const condRules: any[] = (ve as any)?.conditionalRules || []
    const isChart = local.type === 'chart'
    const hasSecondaryAxis = (['combo','line','area','column','scatter','bar'] as string[]).includes(local.chartType||'')
    return (
      <div className="space-y-2">
        {!isMeasure && (
          <FormRow label="Aggregation">
            <select className={selectCls()} value={ve?.agg||'sum'} onChange={e=>patchValue({agg:e.target.value})}>
              {['none','sum','count','distinct','avg','min','max'].map(a=><option key={a} value={a}>{a}</option>)}
            </select>
          </FormRow>
        )}
        <FormRow label="Format">
          <select className={selectCls()} value={ve?.format||'none'} onChange={e=>patchValue({format:e.target.value})}>
            {FORMAT_OPTIONS.map(f=><option key={f} value={f}>{f}</option>)}
          </select>
        </FormRow>
        <FormRow label="Label" full>
          <input className={inputCls()} placeholder={field} value={ve?.label||''} onChange={e=>patchValue({label:e.target.value||undefined})} />
        </FormRow>
        <FormRow label="Prefix">
          <input className={inputCls('w-24')} value={(ve as any)?.prefix||''} onChange={e=>patchValue({prefix:e.target.value||undefined})} />
        </FormRow>
        <FormRow label="Suffix">
          <input className={inputCls('w-24')} value={(ve as any)?.suffix||''} onChange={e=>patchValue({suffix:e.target.value||undefined})} />
        </FormRow>
        {isChart && (
          <FormRow label="Color">
            <select className={selectCls()} value={currentColor}
              onChange={e=>{ const k=e.target.value as AvailableChartColorsKeys; patchValue({colorToken:colorKeyToToken(k)}) }}>
              {chartColors.slice(0,5).map(c=><option key={c} value={c}>{c}</option>)}
            </select>
          </FormRow>
        )}
        {hasSecondaryAxis && (
          <FormRow label="Secondary axis">
            <Switch checked={!!ve?.secondaryAxis} onChangeAction={v=>patchValue({secondaryAxis:v})} />
          </FormRow>
        )}
        <FormRow label="Sort by">
          <select className={selectCls()} value={(ve as any)?.sort?.by||''}
            onChange={e=>{ const by=e.target.value; patchValue({sort:by?{by,direction:(ve as any)?.sort?.direction||'desc'}:undefined}) }}>
            <option value="">None</option>
            <option value="x">X label</option>
            <option value="value">Value</option>
          </select>
        </FormRow>
        {(ve as any)?.sort?.by && (
          <FormRow label="Direction">
            <select className={selectCls()} value={(ve as any)?.sort?.direction||'desc'}
              onChange={e=>patchValue({sort:{...((ve as any)?.sort||{by:'value'}),direction:e.target.value}})}>
              <option value="desc">Descending</option>
              <option value="asc">Ascending</option>
            </select>
          </FormRow>
        )}
        {advancedMode && (
          <>
            <FormRow label="Stack group">
              <input className={inputCls('w-24')} placeholder="e.g. A" value={ve?.stackId||''}
                onChange={e=>patchValue({stackId:e.target.value||undefined})} />
            </FormRow>
            <FormRow label="Fill style">
              <select className={selectCls()} value={ve?.style||'solid'} onChange={e=>patchValue({style:e.target.value})}>
                <option value="solid">Solid</option>
                <option value="gradient">Gradient</option>
              </select>
            </FormRow>
            <div className="space-y-1.5">
              <div className="flex items-center justify-between">
                <span className="text-xs text-muted-foreground">Conditional rules</span>
                <button className="text-[10px] px-2 py-0.5 rounded border hover:bg-muted cursor-pointer"
                  onClick={()=>patchValue({conditionalRules:[...condRules,{when:'>',value:0,color:'rose'}]})}>+ Add</button>
              </div>
              {condRules.length===0 && <div className="text-xs text-muted-foreground">No rules yet.</div>}
              <div className="space-y-1 max-h-48 overflow-auto">
                {condRules.map((r,idx)=>(
                  <div key={idx} className="grid grid-cols-[80px,1fr,90px,auto] items-center gap-1">
                    <select className={selectCls()} value={r.when}
                      onChange={e=>patchValue({conditionalRules:condRules.map((rr,i)=>i===idx?{...rr,when:e.target.value}:rr)})}>
                      {['>','>=','<','<=','equals','between'].map(w=><option key={w} value={w}>{w}</option>)}
                    </select>
                    {r.when==='between'
                      ? <input className={inputCls('font-mono')} placeholder="min,max" value={Array.isArray(r.value)?`${r.value[0]},${r.value[1]}`:''}
                          onChange={e=>{ const pts=e.target.value.split(',').map(v=>Number(v.trim())); patchValue({conditionalRules:condRules.map((rr,i)=>i===idx?{...rr,value:[Number(pts[0]||0),Number(pts[1]||0)]}:rr)}) }} />
                      : <input type="number" className={inputCls()} value={Array.isArray(r.value)?0:Number(r.value)}
                          onChange={e=>patchValue({conditionalRules:condRules.map((rr,i)=>i===idx?{...rr,value:Number(e.target.value)}:rr)})} />
                    }
                    <select className={selectCls()} value={r.color||'rose'}
                      onChange={e=>patchValue({conditionalRules:condRules.map((rr,i)=>i===idx?{...rr,color:e.target.value}:rr)})}>
                      {['blue','emerald','violet','amber','gray','rose','indigo','cyan','pink','lime','fuchsia'].map(c=><option key={c} value={c}>{c}</option>)}
                    </select>
                    <button className="text-[10px] px-1.5 py-0.5 rounded border hover:bg-muted cursor-pointer"
                      onClick={()=>patchValue({conditionalRules:condRules.filter((_,i)=>i!==idx)})}>✕</button>
                  </div>
                ))}
              </div>
            </div>
          </>
        )}
      </div>
    )
  }

  // ── x axis ──────────────────────────────────────────────────────────────────
  if (kind === 'x') {
    const xDateFmtRaw = (local.options as any)?.xDateFormat as string|undefined
    const xDatePreset = !xDateFmtRaw ? 'none' : X_DATE_PRESET_SET.has(xDateFmtRaw) ? xDateFmtRaw : 'custom'
    return (
      <div className="space-y-2">
        <FormRow label="Group by">
          <select className={selectCls()} value={local.xAxis?.groupBy||'none'}
            onChange={e=>{
              const groupBy = e.target.value as any
              const next = { ...local, xAxis:{...(local.xAxis||{}),groupBy}, querySpec:{...(local.querySpec||{source:'',select:[]}),groupBy} }
              setLocal(next); updateConfig(next)
            }}>
            {['none','hour','day','week','month','quarter','year'].map(g=><option key={g} value={g}>{g}</option>)}
          </select>
        </FormRow>
        <FormRow label="Label case">
          <select className={selectCls()} value={(local.options as any)?.xLabelCase||'proper'}
            onChange={e=>patchOpt({xLabelCase:e.target.value as any})}>
            <option value="proper">Proper</option>
            <option value="uppercase">Uppercase</option>
            <option value="lowercase">Lowercase</option>
          </select>
        </FormRow>
        <FormRow label="Date format" full>
          <select className={selectCls('w-full')} value={xDatePreset}
            onChange={e=>{
              const v = e.target.value
              if (v==='none') patchOpt({xDateFormat:undefined} as any)
              else if (v==='custom') patchOpt({xDateFormat:'DD-MMM-YYYY'} as any)
              else patchOpt({xDateFormat:v} as any)
            }}>
            {X_DATE_PRESETS.map(p=><option key={p.value} value={p.value}>{p.label}</option>)}
          </select>
          {xDatePreset==='custom' && (
            <input className={`${inputCls()} mt-1 font-mono`} placeholder="e.g. DD-MMM-YYYY, MMM-YYYY"
              value={String(xDateFmtRaw||'')}
              onChange={e=>patchOpt({xDateFormat:e.target.value||undefined} as any)} />
          )}
        </FormRow>
        <FormRow label="Week starts on">
          <select className={selectCls()} value={(local.options as any)?.xWeekStart||'mon'}
            onChange={e=>patchOpt({xWeekStart:e.target.value as any})}>
            <option value="mon">Monday</option>
            <option value="sun">Sunday</option>
          </select>
        </FormRow>
        <FormRow label="Max ticks">
          <input type="number" min={2} className={inputCls('w-20')} placeholder="Auto"
            value={typeof (local.options as any)?.xTickCount==='number'?(local.options as any).xTickCount:''}
            onChange={e=>patchOpt({xTickCount:e.target.value===''?undefined:Math.max(2,Number(e.target.value))})} />
        </FormRow>
      </div>
    )
  }

  // ── legend ──────────────────────────────────────────────────────────────────
  if (kind === 'legend') {
    if (local.type === 'kpi') return (
      <div className="space-y-2">
        <FormRow label="Category label case">
          <select className={selectCls()} value={(local.options as any)?.kpi?.labelCase||'proper'}
            onChange={e=>patchOpt({kpi:{...((local.options as any)?.kpi||{}),labelCase:e.target.value}} as any)}>
            <option value="proper">Proper</option>
            <option value="capitalize">Capitalize</option>
            <option value="uppercase">Uppercase</option>
            <option value="lowercase">Lowercase</option>
          </select>
        </FormRow>
      </div>
    )
    return (
      <div className="space-y-2">
        <FormRow label="Label case">
          <select className={selectCls()} value={(local.options as any)?.legendLabelCase||'proper'}
            onChange={e=>patchOpt({legendLabelCase:e.target.value as any})}>
            <option value="proper">Proper</option>
            <option value="uppercase">Uppercase</option>
            <option value="lowercase">Lowercase</option>
          </select>
        </FormRow>
      </div>
    )
  }

  // ── filter ──────────────────────────────────────────────────────────────────
  if (kind === 'filter') {
    const filtersExpose = (local.options?.filtersExpose || {}) as Record<string,boolean>
    const sel: string[] = Array.isArray(where[field]) ? where[field] : (where[field] ? [String(where[field])] : [])
    const patchSel = (vals: string[]) => patchWhere({ [field]: vals.length ? vals : undefined })
    return (
      <div className="space-y-2">
        <div className="flex gap-1 rounded-md border p-0.5 bg-muted/30 w-fit">
          {(['manual','rule'] as const).map(t=>(
            <button key={t} onClick={()=>setFilterTab(t)}
              className={`text-xs px-2.5 py-0.5 rounded cursor-pointer capitalize ${filterTab===t?'bg-background shadow-sm font-medium':''}`}>{t}</button>
          ))}
        </div>
        {filterTab==='rule'
          ? (() => {
              const effectiveIsNum = ruleType === 'number' || (ruleType === 'auto' && isNum)
              const effectiveIsDate = ruleType === 'date' || (ruleType === 'auto' && isDate)
              return (
                <>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-muted-foreground shrink-0">Type:</span>
                    <div className="flex gap-1">
                      {(['auto','string','number','date'] as const).map(t=>(
                        <button key={t} type="button" onClick={()=>setRuleType(t)}
                          className={`text-[10px] px-2 py-0.5 rounded border cursor-pointer capitalize ${ruleType===t?'bg-[hsl(var(--primary))] text-primary-foreground':'hover:bg-muted'}`}>
                          {t==='auto'?`auto (${effectiveIsNum?'num':effectiveIsDate?'date':'str'})`:t}
                        </button>
                      ))}
                    </div>
                  </div>
                  {effectiveIsNum ? <NumberFilterDetails field={field} onPatch={patchWhere} />
                    : effectiveIsDate ? <DateRuleDetails field={field} onPatch={patchWhere} />
                    : <StringRuleDetails field={field} onPatch={patchWhere} />}
                </>
              )
            })()
          : isNum ? (
            <>
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground">Min (≥)</label>
                  <input type="number" className={inputCls()} value={String(where[`${field}__gte`]??'')} placeholder="—"
                    onChange={e=>patchWhere({[`${field}__gte`]:e.target.value!==''?Number(e.target.value):undefined})} />
                </div>
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground">Max (≤)</label>
                  <input type="number" className={inputCls()} value={String(where[`${field}__lte`]??'')} placeholder="—"
                    onChange={e=>patchWhere({[`${field}__lte`]:e.target.value!==''?Number(e.target.value):undefined})} />
                </div>
              </div>
              <button className="text-xs px-2.5 py-1 rounded-md border hover:bg-muted cursor-pointer"
                onClick={()=>patchWhere({[`${field}__gte`]:undefined,[`${field}__lte`]:undefined})}>Clear</button>
            </>
          ) : isDate ? (
            <>
              <FormRow label="Preset" full>
                <select className={selectCls('w-full')} value={where[`${field}__date_preset`]||''}
                  onChange={e=>patchWhere({[`${field}__date_preset`]:e.target.value||undefined,[`${field}__gte`]:undefined,[`${field}__lt`]:undefined})}>
                  <option value="">— None —</option>
                  {DATE_PRESETS.map(p=><option key={p} value={p}>{p.replace(/_/g,' ')}</option>)}
                </select>
              </FormRow>
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground">From</label>
                  <input type="date" className={inputCls()} value={String(where[`${field}__gte`]||'')}
                    onChange={e=>patchWhere({[`${field}__gte`]:e.target.value||undefined,[`${field}__date_preset`]:undefined})} />
                </div>
                <div className="space-y-1">
                  <label className="text-xs text-muted-foreground">To</label>
                  <input type="date" className={inputCls()} value={String(where[`${field}__lt`]||'')}
                    onChange={e=>patchWhere({[`${field}__lt`]:e.target.value||undefined,[`${field}__date_preset`]:undefined})} />
                </div>
              </div>
              <button className="text-xs px-2.5 py-1 rounded-md border hover:bg-muted cursor-pointer"
                onClick={()=>patchWhere({[`${field}__date_preset`]:undefined,[`${field}__gte`]:undefined,[`${field}__lt`]:undefined})}>Clear</button>
            </>
          ) : (
            <>
              {samples.length===0 && (
                <div className="flex items-center justify-between text-xs text-muted-foreground rounded-md border p-2 bg-[hsl(var(--secondary)/0.4)]">
                  <span>No samples loaded yet</span>
                  <button type="button" className="text-xs px-2 py-0.5 rounded border hover:bg-muted cursor-pointer"
                    onClick={()=>{ if(local.id&&typeof window!=='undefined') try{window.dispatchEvent(new CustomEvent('request-table-samples',{detail:{widgetId:local.id}}))}catch{} }}>
                    Refresh
                  </button>
                </div>
              )}
              <div className="max-h-40 overflow-y-auto space-y-1 rounded-md border p-2 bg-[hsl(var(--secondary)/0.4)]" style={samples.length===0?{display:'none'}:{}}>
                {samples.map(sv=>(
                  <label key={sv} className="flex items-center gap-2 text-xs cursor-pointer">
                    <input type="checkbox" className="accent-[hsl(var(--primary))]" checked={sel.includes(sv)}
                      onChange={e=>patchSel(e.target.checked?[...sel,sv]:sel.filter(x=>x!==sv))} />
                    <span className="truncate">{sv}</span>
                  </label>
                ))}
              </div>
              {sel.length>0 && (
                <button className="text-xs px-2.5 py-1 rounded-md border hover:bg-muted cursor-pointer"
                  onClick={()=>patchSel([])}>Clear ({sel.length})</button>
              )}
            </>
          )
        }
        <div className="flex items-center justify-between pt-2 border-t mt-1">
          <span className="text-xs text-muted-foreground">Expose in chart filterbar</span>
          <Switch checked={!!filtersExpose[field]}
            onChangeAction={v=>{
              const next={...local,options:{...(local.options||{}),filtersExpose:{...filtersExpose,[field]:v}}} as WidgetConfig
              setLocal(next); updateConfig(next)
            }} />
        </div>
      </div>
    )
  }

  return (
    <div className="text-xs text-muted-foreground">
      <span className="font-mono text-foreground">{field}</span> → <span className="capitalize">{kind}</span>
    </div>
  )
}

// ── InlineCustomColEditor ─────────────────────────────────────────────────────
export function InlineCustomColEditor({ value, onSave, onCancel }: {
  value: { id?: string; name: string; formula: string; type?: string }
  onSave: (v: { id?: string; name: string; formula: string; type?: string }) => void
  onCancel: () => void
}) {
  const [name, setName] = useState(value.name)
  const [formula, setFormula] = useState(value.formula)
  const [type, setType] = useState(value.type || 'number')
  return (
    <div className="rounded-lg border bg-[hsl(var(--secondary)/0.3)] p-3 space-y-2">
      <div className="text-xs font-semibold text-muted-foreground mb-1">{value.id ? 'Edit' : 'New'} Custom Column</div>
      <FormRow label="Name" full>
        <input className={inputCls()} value={name} onChange={e=>setName(e.target.value)} placeholder="column_name" />
      </FormRow>
      <FormRow label="Formula" full>
        <input className={inputCls('font-mono')} value={formula} onChange={e=>setFormula(e.target.value)} placeholder='CASE WHEN x > 0 THEN "Yes" ELSE "No" END' />
      </FormRow>
      <FormRow label="Type">
        <select className={selectCls()} value={type} onChange={e=>setType(e.target.value)}>
          {['number','string','date','boolean'].map(t=><option key={t} value={t}>{t}</option>)}
        </select>
      </FormRow>
      <div className="flex gap-2 justify-end pt-1">
        <button className="text-xs px-2.5 py-1 rounded-md border hover:bg-muted cursor-pointer" onClick={onCancel}>Cancel</button>
        <button className="text-xs px-2.5 py-1 rounded-md border bg-primary text-primary-foreground hover:bg-primary/90 cursor-pointer"
          onClick={()=>{ if(name.trim()&&formula.trim()) onSave({id:value.id,name:name.trim(),formula:formula.trim(),type}) }}>
          Save column
        </button>
      </div>
    </div>
  )
}

// ── InlineMeasureEditor ───────────────────────────────────────────────────────
export function InlineMeasureEditor({ value, onSave, onCancel }: {
  value: { id?: string; name: string; formula: string }
  onSave: (v: { id: string; name: string; formula: string }) => void
  onCancel: () => void
}) {
  const [name, setName] = useState(value.name)
  const [formula, setFormula] = useState(value.formula)
  return (
    <div className="rounded-lg border bg-[hsl(var(--secondary)/0.3)] p-3 space-y-2">
      <div className="text-xs font-semibold text-muted-foreground mb-1">{value.id ? 'Edit' : 'New'} Measure</div>
      <FormRow label="Name" full>
        <input className={inputCls()} value={name} onChange={e=>setName(e.target.value)} placeholder="Total Revenue" />
      </FormRow>
      <FormRow label="Formula" full>
        <input className={inputCls('font-mono')} value={formula} onChange={e=>setFormula(e.target.value)} placeholder="SUM(price * quantity)" />
      </FormRow>
      <div className="flex gap-2 justify-end pt-1">
        <button className="text-xs px-2.5 py-1 rounded-md border hover:bg-muted cursor-pointer" onClick={onCancel}>Cancel</button>
        <button className="text-xs px-2.5 py-1 rounded-md border bg-primary text-primary-foreground hover:bg-primary/90 cursor-pointer"
          onClick={()=>{ if(name.trim()&&formula.trim()) onSave({id:value.id||crypto.randomUUID(),name:name.trim(),formula:formula.trim()}) }}>
          Save measure
        </button>
      </div>
    </div>
  )
}

// ── FieldDetailPanel (inline detail card shown inside Data Fields) ─────────────
export function FieldDetailPanel({ selKind, selField, local, setLocal, updateConfig, pivot, applyPivot, samplesByField, numericFields, dateLikeFields, onClose }: {
  selKind: 'x'|'value'|'legend'|'filter'; selField: string
  local: WidgetConfig; setLocal: (c: WidgetConfig) => void; updateConfig: (c: WidgetConfig) => void
  pivot: PivotAssignments; applyPivot: (p: PivotAssignments) => void
  samplesByField: Record<string,string[]>; numericFields: string[]; dateLikeFields: string[]
  onClose: () => void
}) {
  return (
    <div className="rounded-lg border bg-[hsl(var(--secondary)/0.3)] p-3 space-y-2 animate-in fade-in slide-in-from-top-1 duration-150">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-1.5 text-xs font-semibold">
          <span className="px-1.5 py-0.5 rounded border bg-card capitalize text-muted-foreground">{selKind}</span>
          <span>{selField}</span>
        </div>
        <button className="text-xs px-2 py-0.5 rounded border hover:bg-muted cursor-pointer" onClick={onClose}>✕</button>
      </div>
      <FieldDetails kind={selKind} field={selField} local={local} setLocal={setLocal} updateConfig={updateConfig}
        pivot={pivot} applyPivot={applyPivot} samplesByField={samplesByField} numericFields={numericFields} dateLikeFields={dateLikeFields} />
    </div>
  )
}
