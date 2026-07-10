"use client"
import { useState } from 'react'
import { useTranslations } from 'next-intl'
import type { WidgetConfig } from '@/types/widgets'
import { Switch } from '@/components/Switch'
import { Api, QueryApi } from '@/lib/api'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { SectionCard, FormRow, selectCls, inputCls, ColorField } from './shared'
import TabsControls from '@/components/builder/TabsControls'
import { RiAlignLeft, RiAlignCenter, RiAlignRight, RiBold, RiItalic, RiUnderline } from '@remixicon/react'

// ── Chart type mini-preview ───────────────────────────────────────────────────
function ChartPreview({ t }: { t: string }) {
  return (
    <span className="w-10 h-8 bg-card rounded relative overflow-hidden block">
      {t === 'column' && (<><span className="absolute bottom-1 left-1 w-2 h-5 bg-blue-500" /><span className="absolute bottom-1 left-4 w-2 h-3 bg-blue-400" /><span className="absolute bottom-1 left-7 w-2 h-6 bg-blue-300" /></>)}
      {t === 'bar' && (<><span className="absolute left-1 top-1 h-1 w-7 bg-blue-500" /><span className="absolute left-1 top-3 h-1 w-5 bg-blue-400" /><span className="absolute left-1 top-5 h-1 w-8 bg-blue-300" /></>)}
      {t === 'line' && (<svg className="w-full h-full" viewBox="0 0 40 32"><polyline points="2,28 12,18 22,22 32,8 38,12" fill="none" stroke="#3b82f6" strokeWidth="2" /></svg>)}
      {t === 'area' && (<svg className="w-full h-full" viewBox="0 0 40 32"><polyline points="2,28 12,18 22,22 32,8 38,12" fill="#93c5fd" stroke="#60a5fa" strokeWidth="1" /></svg>)}
      {t === 'donut' && (<svg className="w-full h-full" viewBox="0 0 40 32"><circle cx="20" cy="16" r="10" fill="#e5e7eb" /><path d="M20 6 A10 10 0 0 1 30 16 L20 16 Z" fill="#3b82f6" /><circle cx="20" cy="16" r="5" fill="#fff" /></svg>)}
      {t === 'categoryBar' && (<div className="w-full h-full flex items-center px-1 gap-1"><span className="h-2 flex-1 bg-blue-500 rounded-sm" /><span className="h-2 flex-1 bg-emerald-500 rounded-sm" /><span className="h-2 flex-1 bg-amber-500 rounded-sm" /></div>)}
      {t === 'spark' && (<svg className="w-full h-full" viewBox="0 0 40 32"><polyline points="2,24 8,18 12,20 18,10 26,16 34,8 38,12" fill="none" stroke="#3b82f6" strokeWidth="1" /></svg>)}
      {t === 'combo' && (<svg className="w-full h-full" viewBox="0 0 40 32"><rect x="6" y="18" width="6" height="10" fill="#60a5fa" /><rect x="16" y="12" width="6" height="16" fill="#34d399" /><polyline points="2,28 12,16 22,20 32,8 38,12" fill="none" stroke="#f59e0b" strokeWidth="2" /></svg>)}
      {t === 'progress' && (<div className="w-full h-full flex items-center px-1 gap-1"><span className="h-1 flex-1 bg-blue-500 rounded-sm" /><span className="h-1 w-3 bg-[hsl(var(--border))] rounded-sm" /></div>)}
      {t === 'tracker' && (<div className="w-full h-full flex items-center px-1 gap-[2px]">{Array.from({length:6}).map((_,i)=>(<span key={i} className={`w-1 h-2 ${i<4?'bg-blue-500':'bg-[hsl(var(--border))]'} rounded-sm`} />))}</div>)}
      {t === 'badges' && (<div className="w-full h-full flex items-center justify-center gap-1"><span className="px-1 rounded bg-blue-100 text-[10px]">A</span><span className="px-1 rounded bg-emerald-100 text-[10px]">B</span></div>)}
      {t === 'scatter' && (<svg className="w-full h-full" viewBox="0 0 40 32"><circle cx="8" cy="20" r="2" fill="#3b82f6" /><circle cx="16" cy="14" r="2" fill="#3b82f6" /><circle cx="24" cy="10" r="2" fill="#3b82f6" /><circle cx="32" cy="18" r="2" fill="#3b82f6" /></svg>)}
      {t === 'gantt' && (<svg className="w-full h-full" viewBox="0 0 40 32"><rect x="4" y="6" width="12" height="4" rx="2" fill="#60a5fa" /><rect x="8" y="12" width="20" height="4" rx="2" fill="#34d399" /><rect x="18" y="18" width="14" height="4" rx="2" fill="#f59e0b" /><rect x="10" y="24" width="8" height="4" rx="2" fill="#a78bfa" /></svg>)}
      {t === 'sankey' && (<svg className="w-full h-full" viewBox="0 0 40 32"><rect x="2" y="8" width="3" height="6" rx="1" fill="#60a5fa" /><rect x="2" y="18" width="3" height="6" rx="1" fill="#34d399" /><rect x="35" y="6" width="3" height="8" rx="1" fill="#f59e0b" /><rect x="35" y="18" width="3" height="8" rx="1" fill="#a78bfa" /><path d="M5 11 Q20 11 35 9" fill="none" stroke="#60a5fa" strokeWidth="2.5" opacity="0.4" /><path d="M5 21 Q20 19 35 13" fill="none" stroke="#34d399" strokeWidth="2" opacity="0.4" /></svg>)}
      {t === 'tremorTable' && (<svg className="w-full h-full" viewBox="0 0 40 32"><rect x="2" y="6" width="36" height="20" fill="hsl(var(--card))" stroke="hsl(var(--border))" strokeWidth="1" rx="2" /><rect x="2" y="6" width="36" height="6" fill="hsl(var(--muted))" /><line x1="12" y1="6" x2="12" y2="26" stroke="hsl(var(--border))" strokeWidth="1" /><line x1="2" y1="12" x2="38" y2="12" stroke="hsl(var(--border))" strokeWidth="1" /><line x1="2" y1="16" x2="38" y2="16" stroke="hsl(var(--border))" strokeWidth="1" /><line x1="2" y1="20" x2="38" y2="20" stroke="hsl(var(--border))" strokeWidth="1" /><circle cx="7" cy="14" r="1.5" fill="#3b82f6" /><rect x="14" y="13" width="16" height="2" fill="#93c5fd" rx="1" /><circle cx="7" cy="18" r="1.5" fill="#10b981" /><rect x="14" y="17" width="12" height="2" fill="#86efac" rx="1" /></svg>)}
      {t === 'heatmap' && (<div className="w-full h-full grid grid-cols-5 grid-rows-3 gap-[2px] p-[3px]">{Array.from({length:15}).map((_,i)=>(<span key={i} className={`rounded-sm ${i%3===0?'bg-blue-400':i%3===1?'bg-blue-300':'bg-blue-200'}`} />))}</div>)}
      {t === 'barList' && (<div className="w-full h-full flex flex-col items-start justify-center gap-[2px] px-1"><span className="h-1.5 w-8 bg-blue-500 rounded-sm" /><span className="h-1.5 w-7 bg-emerald-500 rounded-sm" /><span className="h-1.5 w-6 bg-amber-500 rounded-sm" /><span className="h-1.5 w-4 bg-violet-500 rounded-sm" /></div>)}
    </span>
  )
}

// ── KPI preset preview ────────────────────────────────────────────────────────
function KpiPreview({ p }: { p: string }) {
  const t = useTranslations('configurator')
  return (
    <div className="rounded-md border bg-[hsl(var(--card))] p-2 w-full h-[84px] flex items-center justify-start overflow-hidden">
      {p === 'basic' && (<div className="leading-tight min-w-0"><div className="text-[10px] text-muted-foreground mb-0.5 truncate">{t('visualize.previewUniqueVisitors')}</div><div className="text-[18px] font-semibold whitespace-nowrap">10,450 <span className="text-xs text-rose-600 align-middle">-12.5%</span></div></div>)}
      {p === 'badge' && (<div className="leading-tight w-full min-w-0"><div className="text-[10px] text-muted-foreground mb-0.5 truncate">{t('visualize.previewBounceRate')}</div><div className="flex items-center justify-between gap-2"><div className="text-[18px] font-semibold whitespace-nowrap">56.1%</div><span className="text-[10px] px-1.5 py-0.5 rounded bg-emerald-100 text-emerald-700">+1.8%</span></div></div>)}
      {p === 'withPrevious' && (<div className="leading-tight min-w-0"><div className="text-[10px] text-muted-foreground mb-0.5 truncate">{t('visualize.previewVisitDuration')}</div><div className="text-[18px] font-semibold whitespace-nowrap">5.2min</div><div className="text-[10px] text-muted-foreground whitespace-nowrap">from 4.3</div></div>)}
      {p === 'donut' && (<div className="flex items-center gap-3 w-full min-w-0"><div className="relative w-10 h-10 shrink-0"><div className="w-10 h-10 rounded-full border-4 border-emerald-500 border-r-slate-300 border-b-slate-300" /><div className="absolute inset-0 m-auto w-4 h-4 rounded-full bg-[hsl(var(--card))]" /><div className="absolute inset-0 flex items-center justify-center"><span className="text-[10px] font-medium text-emerald-700">64%</span></div></div><div className="leading-tight"><div className="text-[16px] font-semibold">6.4k</div><div className="text-[10px] text-muted-foreground">of 10k</div></div></div>)}
      {p === 'spark' && (<div className="w-full min-w-0"><div className="flex items-start justify-between"><div className="text-[16px] font-semibold text-emerald-600 whitespace-nowrap">$129.10</div><div className="text-[10px] text-emerald-700">+7.1%</div></div><div className="mt-1 h-8 w-full rounded-md bg-emerald-500/10 relative overflow-hidden"><div className="absolute inset-x-0 bottom-0 h-1/2 bg-emerald-500/15" /><svg className="absolute inset-0 w-full h-full" viewBox="0 0 100 32" preserveAspectRatio="none"><polyline fill="none" stroke="#10b981" strokeWidth="2" points="2,26 12,22 24,24 36,16 48,18 60,14 72,17 84,12 96,14" /></svg></div></div>)}
      {p === 'progress' && (<div className="w-full min-w-0"><div className="text-[16px] font-semibold mb-1 whitespace-nowrap">65%</div><div className="h-2 w-full rounded bg-emerald-200/50 overflow-hidden"><div className="h-2 bg-emerald-500" style={{width:'65%'}} /></div></div>)}
      {p === 'categoryBar' && (<div className="w-full min-w-0"><div className="text-[16px] font-semibold">10,000</div><div className="mt-1 w-full h-2.5 rounded-full bg-muted overflow-hidden flex"><div className="h-2.5 bg-cyan-500" style={{width:'35%'}} /><div className="h-2.5 bg-violet-500" style={{width:'65%'}} /></div><div className="mt-1 flex items-center gap-2 text-[10px] text-muted-foreground"><span className="inline-flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-sm bg-cyan-500" />3.5k</span><span className="inline-flex items-center gap-1"><span className="inline-block w-2 h-2 rounded-sm bg-violet-500" />6.5k</span></div></div>)}
      {p === 'multiProgress' && (<div className="w-full min-w-0 space-y-1"><div className="flex items-center justify-between text-[10px]"><span>Alpha</span><span>24%</span></div><div className="h-1.5 w-full rounded bg-blue-200/50 overflow-hidden"><div className="h-1.5 bg-blue-500" style={{width:'24%'}} /></div><div className="flex items-center justify-between text-[10px]"><span>Beta</span><span>36%</span></div><div className="h-1.5 w-full rounded bg-violet-200/50 overflow-hidden"><div className="h-1.5 bg-violet-500" style={{width:'36%'}} /></div><div className="flex items-center justify-between text-[10px]"><span>Gamma</span><span>40%</span></div><div className="h-1.5 w-full rounded bg-amber-200/50 overflow-hidden"><div className="h-1.5 bg-amber-500" style={{width:'40%'}} /></div></div>)}
    </div>
  )
}

const CHART_TYPES = ['column','bar','area','line','donut','categoryBar','spark','combo','progress','tracker','badges','scatter','gantt','sankey','tremorTable','heatmap','barList'] as const
const KPI_PRESETS = ['basic','badge','withPrevious','spark','donut','progress','categoryBar','multiProgress'] as const
const FORMAT_OPTIONS = ['none','short','currency','percent','bytes','wholeNumber','oneDecimal','twoDecimals'] as const
const COLOR_PRESETS = ['default','muted','vibrant','corporate'] as const

export function VisualizeTab({ local, setLocal, updateConfig, allFieldNames = [], search = '' }: {
  local: WidgetConfig
  setLocal: (c: WidgetConfig) => void
  updateConfig: (c: WidgetConfig) => void
  allFieldNames?: string[]
  search?: string
}) {
  const { env } = useEnvironment()
  const t = useTranslations('configurator')
  const [ttFmtCol, setTtFmtCol] = useState('')
  const [ttFmtMode, setTtFmtMode] = useState<'none'|'short'|'currency'|'percent'|'bytes'>('none')
  const [gridSubTab, setGridSubTab] = useState<'hMain'|'hSecondary'|'vMain'|'vSecondary'>('hMain')

  // ── Delta resolved-period preview (ported from V1 refreshDeltaPreview) ────────
  const [deltaResolved, setDeltaResolved] = useState<{ curStart: string; curEnd: string; prevStart: string; prevEnd: string } | null>(null)
  const [deltaSampleNow, setDeltaSampleNow] = useState<string | null>(null)
  const [deltaPreviewLoading, setDeltaPreviewLoading] = useState(false)
  const [deltaPreviewError, setDeltaPreviewError] = useState<string | undefined>(undefined)
  const refreshDeltaPreview = async () => {
    try {
      setDeltaPreviewLoading(true)
      setDeltaPreviewError(undefined)
      setDeltaResolved(null)
      const source = local?.querySpec?.source
      const field = (local?.options as any)?.deltaDateField as string | undefined
      const mode = (local?.options as any)?.deltaMode as any
      const weekStart = ((local?.options as any)?.deltaWeekStart || (env as any)?.weekStart || 'mon') as any
      const where = (local?.querySpec?.where || {}) as Record<string, any>
      if (!source || !field || !mode || mode === 'off') { setDeltaPreviewLoading(false); return }
      const spec: any = { source, select: [field], where: Object.keys(where || {}).length ? where : undefined, limit: 1, offset: 0 }
      const r = await QueryApi.querySpec({ spec, datasourceId: local?.datasourceId, limit: 1, offset: 0, includeTotal: false })
      const cols = (r?.columns || []) as string[]
      const idx = Math.max(0, cols.indexOf(field))
      const v = Array.isArray(r?.rows) && r.rows[0] ? r.rows[0][idx] : undefined
      setDeltaSampleNow(v != null ? String(v) : null)
      const resolved = await Api.resolvePeriods({ mode, tzOffsetMinutes: (typeof window !== 'undefined') ? new Date().getTimezoneOffset() : 0, weekStart })
      setDeltaResolved(resolved)
    } catch (e: any) {
      setDeltaPreviewError(String(e?.message || 'Failed to resolve period'))
    } finally {
      setDeltaPreviewLoading(false)
    }
  }

  const patchOpt = (p: Partial<NonNullable<WidgetConfig['options']>>) => {
    const next = { ...local, options: { ...(local.options || {}), ...p } }
    setLocal(next); updateConfig(next)
  }
  const patchTable = (p: any) => {
    const table = { ...(local.options?.table || {}), ...p }
    const next = { ...local, options: { ...(local.options || {}), table } }
    setLocal(next); updateConfig(next)
  }
  const patchPivotStyle = (p: any) => {
    const pivotStyle = { ...(local.options?.table?.pivotStyle || {}), ...p }
    const table = { ...(local.options?.table || {}), pivotStyle }
    const next = { ...local, options: { ...(local.options || {}), table } }
    setLocal(next); updateConfig(next)
  }

  if (local.type !== 'chart' && local.type !== 'kpi' && local.type !== 'table') {
    return <div className="text-xs text-muted-foreground p-2">{t('visualize.noVisualization')}</div>
  }

  const s = search.toLowerCase().trim()
  const matches = (...terms: string[]) => !s || terms.some(term => term.toLowerCase().includes(s))

  return (
    <div className="space-y-3">

      {/* Chart type */}
      {local.type === 'chart' && matches('chart type','column','bar','line','area','donut','scatter','combo','spark','progress','tracker','badges','heatmap','gantt','sankey','table','barlist') && (
        <SectionCard title={t('sections.chartType')}>
          <div className="grid grid-cols-3 gap-2 max-h-[260px] overflow-y-auto p-0.5" style={{ scrollbarGutter: 'stable' }}>
            {CHART_TYPES.map((ct) => {
              const active = (local.chartType || 'line') === ct
              return (
                <label key={ct} className={`w-full flex flex-col items-center gap-1 p-2 rounded-md border cursor-pointer text-xs transition-colors duration-150 ${active ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))] ring-offset-2 ring-offset-[hsl(var(--card))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>
                  <input type="radio" name="chartTypeV2" className="sr-only" checked={active}
                    onChange={() => { const next = { ...local, chartType: ct }; setLocal(next); updateConfig(next) }} />
                  <ChartPreview t={ct} />
                  <span className="capitalize">{t(`options.chartTypes.${ct}`)}</span>
                </label>
              )
            })}
          </div>
        </SectionCard>
      )}

      {/* KPI preset */}
      {local.type === 'kpi' && matches('kpi preset','basic','badge','spark','donut','progress','categorybar','multiprogress') && (
        <SectionCard title={t('sections.kpiPreset')}>
          <div className="grid grid-cols-2 gap-3">
            {KPI_PRESETS.map((p) => {
              const active = (local.options?.kpi?.preset || 'basic') === p
              return (
                <label key={p} className={`flex items-start gap-2 p-2 rounded-lg border cursor-pointer text-xs transition-colors duration-150 ${active ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))] ring-offset-2 ring-offset-[hsl(var(--card))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>
                  <input type="radio" name="kpiPresetV2" className="sr-only" checked={active}
                    onChange={() => {
                      const kpi = { ...(local.options?.kpi || {}), preset: p }
                      const next = { ...local, options: { ...(local.options || {}), kpi } }
                      setLocal(next as any); updateConfig(next as any)
                    }} />
                  <div className="flex-1 min-w-0">
                    <div className="text-xs font-semibold mb-1">{t(`options.kpiPresets.${p}`)}</div>
                    <KpiPreview p={p} />
                  </div>
                </label>
              )
            })}
          </div>
        </SectionCard>
      )}

      {/* Colors */}
      {(local.type === 'chart' || local.type === 'kpi') && matches('color','palette','format','y axis','yaxis','color mode','gradient','base color') && (
        <SectionCard title={t('sections.colorsFormat')}>
          <FormRow label={t('visualize.colorPalette')} full>
            <div className="grid grid-cols-4 gap-1.5">
              {COLOR_PRESETS.map(p => {
                const active = (local.options?.colorPreset || 'default') === p
                return (
                  <button key={p} type="button" onClick={() => patchOpt({ colorPreset: p })}
                    className={`p-1.5 rounded-md border text-xs cursor-pointer transition-colors duration-150 capitalize ${active ? 'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))] font-semibold' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>
                    {t(`options.colorPresets.${p}`)}
                  </button>
                )
              })}
            </div>
          </FormRow>
          <FormRow label={t('visualize.colorMode')}>
            <select className={selectCls()} value={(local.options as any)?.colorMode||'palette'}
              onChange={e=>{ const mode=e.target.value as 'palette'|'valueGradient'; const autoAdv=['bar','column','line','area','combo'].includes(local.chartType||''); patchOpt({colorMode:mode,...(mode==='valueGradient'&&autoAdv?{advancedMode:true}:{})} as any) }}>
              <option value="palette">{t('visualize.colorModePalette')}</option>
              <option value="valueGradient">{t('visualize.colorModeGradient')}</option>
            </select>
          </FormRow>
          {((local.options as any)?.colorMode||'palette')==='valueGradient' && (
            <FormRow label={t('visualize.baseColor')} full>
              <div className="grid grid-cols-6 gap-1">
                {(['blue','emerald','violet','amber','gray','rose','indigo','cyan','pink','lime','fuchsia'] as const).map(k=>{
                  const colorMap: Record<string,string>={blue:'#3b82f6',emerald:'#10b981',violet:'#8b5cf6',amber:'#f59e0b',gray:'#6b7280',rose:'#f43f5e',indigo:'#6366f1',cyan:'#06b6d4',pink:'#ec4899',lime:'#84cc16',fuchsia:'#d946ef'}
                  return <button key={k} type="button" title={t(`options.colors.${k}`)} onClick={()=>patchOpt({colorBaseKey:k} as any)} className={`h-6 rounded border cursor-pointer hover:scale-110 transition-transform ${((local.options as any)?.colorBaseKey||'blue')===k?'ring-2 ring-[hsl(var(--primary))] ring-offset-1':''}`} style={{backgroundColor:colorMap[k]}} />
                })}
              </div>
            </FormRow>
          )}
          <FormRow label={t('visualize.yAxisFormat')}>
            <select className={selectCls()} value={local.options?.yAxisFormat || 'none'}
              onChange={(e) => patchOpt({ yAxisFormat: e.target.value as any })}>
              {FORMAT_OPTIONS.map(f => <option key={f} value={f}>{t(`options.formats.${f}`)}</option>)}
            </select>
          </FormRow>
        </SectionCard>
      )}

      {/* Title Format */}
      {local.type === 'chart' && matches('title','title format','format title','heading','title color','title align','title font','title background','title margin','title outline') && (
        <SectionCard title={t('sections.titleFormat')}>
          {(() => { const o = (local.options || {}) as any; return (
            <>
              <FormRow label={t('visualize.position')}>
                <select className={selectCls()} value={o.chartTitlePosition || 'none'} onChange={e => patchOpt({ chartTitlePosition: e.target.value } as any)}>
                  {(['none','above','below'] as const).map(p => <option key={p} value={p}>{t(`options.titlePosition.${p}`)}</option>)}
                </select>
              </FormRow>
              <FormRow label={t('visualize.align')}>
                <div className="flex gap-1">
                  {([['left',RiAlignLeft],['center',RiAlignCenter],['right',RiAlignRight]] as const).map(([a, Icon]) => (
                    <button key={a} type="button" onClick={() => patchOpt({ chartTitleAlign: a } as any)}
                      className={`h-7 w-7 flex items-center justify-center rounded border cursor-pointer transition-colors duration-150 ${(o.chartTitleAlign || 'left') === a ? 'bg-[hsl(var(--muted))] ring-1 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>
                      <Icon className="size-4" />
                    </button>
                  ))}
                </div>
              </FormRow>
              <FormRow label={t('visualize.sizePx')}>
                <input type="number" min={10} max={24} className={inputCls('w-20')} value={o.chartTitleSize ?? 13}
                  onChange={e => patchOpt({ chartTitleSize: Math.max(10, Math.min(24, Number(e.target.value || 13))) } as any)} />
              </FormRow>
              <FormRow label={t('visualize.style')}>
                <div className="flex gap-1">
                  {([['normal','N'],['bold',RiBold],['italic',RiItalic],['underline',RiUnderline]] as const).map(([sName, Icon]) => (
                    <button key={sName} type="button" onClick={() => patchOpt({ chartTitleEmphasis: sName } as any)}
                      className={`h-7 w-7 flex items-center justify-center rounded border cursor-pointer text-xs transition-colors duration-150 ${(o.chartTitleEmphasis || 'normal') === sName ? 'bg-[hsl(var(--muted))] ring-1 ring-[hsl(var(--primary))]' : 'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>
                      {typeof Icon === 'string' ? Icon : <Icon className="size-4" />}
                    </button>
                  ))}
                </div>
              </FormRow>
              <FormRow label={t('visualize.fontColor')}>
                <select className={selectCls()} value={o.chartTitleColorMode || 'auto'} onChange={e => patchOpt({ chartTitleColorMode: e.target.value } as any)}>
                  <option value="auto">{t('visualize.auto')}</option><option value="custom">{t('visualize.custom')}</option>
                </select>
              </FormRow>
              {o.chartTitleColorMode === 'custom' && (
                <FormRow label={t('visualize.color')}><ColorField className="w-16" value={o.chartTitleColor || '#111827'} onChange={v => patchOpt({ chartTitleColor: v } as any)} /></FormRow>
              )}
              <FormRow label={t('visualize.background')}>
                <select className={selectCls()} value={o.chartTitleBgMode || 'none'} onChange={e => patchOpt({ chartTitleBgMode: e.target.value } as any)}>
                  <option value="none">{t('visualize.noFill')}</option><option value="custom">{t('visualize.custom')}</option>
                </select>
              </FormRow>
              {o.chartTitleBgMode === 'custom' && (
                <FormRow label={t('visualize.fillColor')}><ColorField className="w-16" value={o.chartTitleBgColor || '#ffffff'} onChange={v => patchOpt({ chartTitleBgColor: v } as any)} /></FormRow>
              )}
              <FormRow label={t('visualize.margin')}>
                <select className={selectCls()} value={o.chartTitleMargin || 'sm'} onChange={e => patchOpt({ chartTitleMargin: e.target.value } as any)}>
                  {(['none','sm','md','lg'] as const).map(m => <option key={m} value={m}>{m}</option>)}
                </select>
              </FormRow>
              <FormRow label={t('visualize.outline')}>
                <Switch checked={!!o.chartTitleOutline} onChangeAction={v => patchOpt({ chartTitleOutline: v } as any)} />
              </FormRow>
              <div className="border-t pt-2 space-y-2">
                <div className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">{t('visualize.gapsPx')}</div>
                <div className="grid grid-cols-4 gap-2">
                  {([['gapTop','chartTitleGapTop'],['gapRight','chartTitleGapRight'],['gapBottom','chartTitleGapBottom'],['gapLeft','chartTitleGapLeft']] as const).map(([lblKey, key]) => (
                    <div key={key}>
                      <label className="block text-xs text-muted-foreground mb-1">{t(`visualize.${lblKey}`)}</label>
                      <input type="number" min={0} max={48} className={inputCls()} value={o[key] ?? 0}
                        onChange={e => patchOpt({ [key]: Math.max(0, Math.min(48, Number(e.target.value || 0))) } as any)} />
                    </div>
                  ))}
                </div>
              </div>
            </>
          )})()}
        </SectionCard>
      )}

      {/* Legend */}
      {local.type === 'chart' && !['spark','badges','tremorTable'].includes(local.chartType || '') && matches('legend','position','dot shape','max items','nested') && (
        <SectionCard title={t('sections.legend')}>
          <FormRow label={t('visualize.showLegend')}>
            <Switch checked={local.options?.showLegend ?? true} onChangeAction={(v) => patchOpt({ showLegend: v })} />
          </FormRow>
          <FormRow label={t('visualize.position')}>
            <select className={selectCls()} value={local.options?.legendPosition || 'bottom'}
              onChange={(e) => patchOpt({ legendPosition: e.target.value as any })}>
              <option value="top">{t('visualize.legendTop')}</option>
              <option value="bottom">{t('visualize.legendBottom')}</option>
              <option value="none">{t('visualize.legendHidden')}</option>
            </select>
          </FormRow>
          <FormRow label={t('visualize.dotShape')}>
            <select className={selectCls()} value={local.options?.legendDotShape||'square'} onChange={e=>patchOpt({legendDotShape:e.target.value as any})}>
              {['square','circle','rect'].map(sh=><option key={sh} value={sh}>{t(`options.dotShape.${sh}`)}</option>)}
            </select>
          </FormRow>
          <FormRow label={t('visualize.maxItems')}>
            <input type="number" min={0} className={inputCls('w-20')} value={local.options?.maxLegendItems??0} onChange={e=>patchOpt({maxLegendItems:Math.max(0,Number(e.target.value||0))})} />
          </FormRow>
          <FormRow label={t('visualize.mode')}>
            <select className={selectCls()} value={(local.options as any)?.legendMode||'flat'} onChange={e=>patchOpt({legendMode:e.target.value as any} as any)}>
              <option value="flat">{t('visualize.legendFlat')}</option><option value="nested">{t('visualize.legendNested')}</option>
            </select>
          </FormRow>
        </SectionCard>
      )}

      {/* Tooltip */}
      {local.type === 'chart' && matches('tooltip','zero','percent','rich','delta') && (
        <SectionCard title={t('sections.tooltip')}>
          <FormRow label={t('visualize.hideZeroValues')}>
            <Switch checked={!!local.options?.tooltipHideZeros} onChangeAction={(v) => patchOpt({ tooltipHideZeros: v })} />
          </FormRow>
          <FormRow label={t('visualize.showPercent')}>
            <Switch checked={!!local.options?.tooltipShowPercent} onChangeAction={(v) => patchOpt({ tooltipShowPercent: v })} />
          </FormRow>
          <FormRow label={t('visualize.richTooltip')}>
            <Switch checked={!!local.options?.richTooltip} onChangeAction={(v) => patchOpt({ richTooltip: v })} />
          </FormRow>
          <FormRow label={t('visualize.downIsGood')}>
            <Switch checked={!!(local.options as any)?.downIsGood} onChangeAction={(v) => patchOpt({ downIsGood: v } as any)} />
          </FormRow>
        </SectionCard>
      )}

      {/* Axis */}
      {local.type === 'chart' && matches('axis','tick','angle','label format','y min','y max','auto-condense','dense','x axis font','y axis font','font weight','font size','font color') && (
        <SectionCard title={t('sections.axis')}>
          <FormRow label={t('visualize.xTickAngle')}>
            <select className={selectCls()} value={String(local.options?.xTickAngle ?? 0)}
              onChange={(e) => patchOpt({ xTickAngle: Number(e.target.value) as any })}>
              {[0,30,45,60,90].map(a => <option key={a} value={a}>{a}°</option>)}
            </select>
          </FormRow>
          <FormRow label={t('visualize.xLabelFormat')}>
            <select className={selectCls()} value={local.options?.xLabelFormat || 'none'}
              onChange={(e) => patchOpt({ xLabelFormat: e.target.value as any })}>
              <option value="none">{t('visualize.xLabelNone')}</option>
              <option value="short">{t('visualize.xLabelShort')}</option>
              <option value="datetime">{t('visualize.xLabelDatetime')}</option>
            </select>
          </FormRow>
          <FormRow label={t('visualize.autoCondenseX')}><Switch checked={!!((local.options as any)?.autoCondenseX)} onChangeAction={v=>patchOpt({autoCondenseX:v} as any)} /></FormRow>
          {!!(local.options as any)?.autoCondenseX && (
            <FormRow label={t('visualize.denseThreshold')}><input type="number" min={40} max={300} className={inputCls('w-20')} value={(local.options as any)?.denseThreshold??80} onChange={e=>patchOpt({denseThreshold:Math.max(40,Math.min(300,Number(e.target.value||80)))} as any)} /></FormRow>
          )}
          <FormRow label={t('visualize.yMin')}>
            <input type="number" className={selectCls('w-20')} placeholder="auto"
              value={local.options?.yMin ?? ''}
              onChange={(e) => patchOpt({ yMin: e.target.value !== '' ? Number(e.target.value) : undefined })} />
          </FormRow>
          <FormRow label={t('visualize.yMax')}>
            <input type="number" className={selectCls('w-20')} placeholder="auto"
              value={local.options?.yMax ?? ''}
              onChange={(e) => patchOpt({ yMax: e.target.value !== '' ? Number(e.target.value) : undefined })} />
          </FormRow>
          <div className="border-t pt-2 space-y-2">
            <div className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">{t('visualize.xAxisFont')}</div>
            <div className="grid grid-cols-3 gap-2">
              <div><label className="block text-xs text-muted-foreground mb-1">{t('visualize.weight')}</label><select className={selectCls('w-full')} value={(local.options as any)?.xAxisFontWeight||'normal'} onChange={e=>patchOpt({xAxisFontWeight:e.target.value} as any)}>{['normal','bold'].map(w=><option key={w} value={w}>{t(`options.fontWeight.${w}`)}</option>)}</select></div>
              <div><label className="block text-xs text-muted-foreground mb-1">{t('visualize.size')}</label><input type="number" min={8} max={18} className={inputCls()} value={(local.options as any)?.xAxisFontSize??11} onChange={e=>patchOpt({xAxisFontSize:Math.max(8,Math.min(18,Number(e.target.value||11)))} as any)} /></div>
              <div><label className="block text-xs text-muted-foreground mb-1">{t('visualize.color')}</label><ColorField className="w-full" value={(local.options as any)?.xAxisFontColor||'#94a3b8'} onChange={v=>patchOpt({xAxisFontColor:v} as any)} /></div>
            </div>
          </div>
          <div className="border-t pt-2 space-y-2">
            <div className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">{t('visualize.yAxisFont')}</div>
            <div className="grid grid-cols-3 gap-2">
              <div><label className="block text-xs text-muted-foreground mb-1">{t('visualize.weight')}</label><select className={selectCls('w-full')} value={(local.options as any)?.yAxisFontWeight||'normal'} onChange={e=>patchOpt({yAxisFontWeight:e.target.value} as any)}>{['normal','bold'].map(w=><option key={w} value={w}>{t(`options.fontWeight.${w}`)}</option>)}</select></div>
              <div><label className="block text-xs text-muted-foreground mb-1">{t('visualize.size')}</label><input type="number" min={8} max={18} className={inputCls()} value={(local.options as any)?.yAxisFontSize??11} onChange={e=>patchOpt({yAxisFontSize:Math.max(8,Math.min(18,Number(e.target.value||11)))} as any)} /></div>
              <div><label className="block text-xs text-muted-foreground mb-1">{t('visualize.color')}</label><ColorField className="w-full" value={(local.options as any)?.yAxisFontColor||'#94a3b8'} onChange={v=>patchOpt({yAxisFontColor:v} as any)} /></div>
            </div>
          </div>
        </SectionCard>
      )}

      {/* Grid Lines */}
      {local.type === 'chart' && matches('grid','gridlines','horizontal','vertical') && (
        <SectionCard title={t('sections.gridLines')}>
          {(()=>{
            const cg = (local.options as any)?.chartGrid || {}
            const patchGrid = (axis: 'horizontal'|'vertical', sub: 'main'|'secondary', p: any) => {
              const updated = { ...cg, [axis]: { ...(cg[axis]||{}), [sub]: { ...(cg[axis]?.[sub]||{}), ...p } } }
              patchOpt({chartGrid: updated} as any)
            }
            const tabDefs = [
              { key:'hMain', labelKey:'gridHMain', axis:'horizontal' as const, sub:'main' as const, defOpacity:0.25 },
              { key:'hSecondary', labelKey:'gridHSecondary', axis:'horizontal' as const, sub:'secondary' as const, defOpacity:0.2 },
              { key:'vMain', labelKey:'gridVMain', axis:'vertical' as const, sub:'main' as const, defOpacity:0.25 },
              { key:'vSecondary', labelKey:'gridVSecondary', axis:'vertical' as const, sub:'secondary' as const, defOpacity:0.2 },
            ]
            const active = tabDefs.find(td=>td.key===gridSubTab) || tabDefs[0]
            const gl = cg[active.axis]?.[active.sub] || {}
            return (
              <>
                <div className="flex gap-1 flex-wrap">
                  {tabDefs.map(td=>(
                    <button key={td.key} type="button" onClick={()=>setGridSubTab(td.key as any)} className={`px-2 py-1 text-xs rounded-md border cursor-pointer ${gridSubTab===td.key?'bg-[hsl(var(--muted))] font-semibold ring-1 ring-[hsl(var(--primary))]':'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>{t(`visualize.${td.labelKey}`)}</button>
                  ))}
                </div>
                <FormRow label={t('visualize.mode')}>
                  <select className={selectCls()} value={gl.mode||'default'} onChange={e=>patchGrid(active.axis,active.sub,{mode:e.target.value})}>
                    <option value="default">{t('visualize.modeDefault')}</option><option value="custom">{t('visualize.modeCustom')}</option>
                  </select>
                </FormRow>
                {(gl.mode||'default')==='custom' && (
                  <>
                    <FormRow label={t('visualize.show')}><Switch checked={!!gl.show} onChangeAction={v=>patchGrid(active.axis,active.sub,{show:v})} /></FormRow>
                    <FormRow label={t('visualize.typeLabel')}>
                      <select className={selectCls()} value={gl.type||'solid'} onChange={e=>patchGrid(active.axis,active.sub,{type:e.target.value})}>
                        {['solid','dashed','dotted'].map(ls=><option key={ls} value={ls}>{t(`options.lineStyle.${ls}`)}</option>)}
                      </select>
                    </FormRow>
                    <FormRow label={t('visualize.widthPx')}><input type="number" min={0} max={10} step={0.5} className={inputCls('w-20')} value={gl.width??1} onChange={e=>patchGrid(active.axis,active.sub,{width:Number(e.target.value||0)})} /></FormRow>
                    <FormRow label={t('visualize.color')}><ColorField className="w-16" value={gl.color||'#94a3b8'} onChange={v=>patchGrid(active.axis,active.sub,{color:v})} /></FormRow>
                    <FormRow label={t('visualize.opacity')}><input type="number" min={0} max={1} step={0.05} className={inputCls('w-20')} value={gl.opacity??active.defOpacity} onChange={e=>patchGrid(active.axis,active.sub,{opacity:Math.max(0,Math.min(1,Number(e.target.value||0)))})} /></FormRow>
                  </>
                )}
              </>
            )
          })()}
        </SectionCard>
      )}

      {/* Table Config */}
      {local.type === 'table' && (
        <>
          {matches('table type','data table','pivot table') && (
            <SectionCard title={t('sections.tableType')}>
              <div className="grid grid-cols-2 gap-2">
                {(['data','pivot'] as const).map(tt=>(
                  <label key={tt} className={`flex items-center justify-center p-2 rounded-md border cursor-pointer text-xs ${(local.options?.table?.tableType||'data')===tt?'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]':'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>
                    <input type="radio" name="tableTypeV2" className="sr-only" checked={(local.options?.table?.tableType||'data')===tt} onChange={()=>patchTable({tableType:tt})} />
                    <span>{tt==='data'?t('visualize.dataTable'):t('visualize.pivotTable')}</span>
                  </label>
                ))}
              </div>
            </SectionCard>
          )}
          {matches('tabs','tab field','tab variant','tab sort','tab label') && (
            <SectionCard title={t('sections.tabs')}>
              <TabsControls local={local} setLocalAction={setLocal} updateConfigAction={updateConfig} allFieldNames={allFieldNames} />
            </SectionCard>
          )}
          {(local.options?.table?.tableType||'data')==='data' && matches('table','theme','density','row height','header','autofit','filter','resize','column','interactions') && (
            <SectionCard title={t('sections.tableLayout')}>
              <FormRow label={t('visualize.theme')}><select className={selectCls()} value={local.options?.table?.theme||'quartz'} onChange={e=>patchTable({theme:e.target.value as any})}>{['quartz','balham','material','alpine'].map(th=><option key={th} value={th}>{th}</option>)}</select></FormRow>
              <FormRow label={t('visualize.density')}><select className={selectCls()} value={local.options?.table?.density||'compact'} onChange={e=>{ const d=e.target.value as any; const tbl={...(local.options?.table||{}),density:d} as any; delete tbl.rowHeight; delete tbl.headerHeight; const n={...local,options:{...(local.options||{}),table:tbl}}; setLocal(n); updateConfig(n) }}><option value="compact">{t('visualize.densityCompact')}</option><option value="comfortable">{t('visualize.densityComfortable')}</option></select></FormRow>
              <FormRow label={t('visualize.rowHeightPx')}><input type="number" min={20} max={56} className={inputCls('w-20')} value={local.options?.table?.rowHeight??28} onChange={e=>patchTable({rowHeight:Math.max(20,Math.min(56,Number(e.target.value||28)))})} /></FormRow>
              <FormRow label={t('visualize.headerHeightPx')}><input type="number" min={20} max={56} className={inputCls('w-20')} value={local.options?.table?.headerHeight??28} onChange={e=>patchTable({headerHeight:Math.max(20,Math.min(56,Number(e.target.value||28)))})} /></FormRow>
              <div className="border-t pt-2 space-y-2">
                <div className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">{t('visualize.autoFit')}</div>
                <FormRow label={t('visualize.mode')}><select className={selectCls()} value={local.options?.table?.autoFit?.mode||''} onChange={e=>patchTable({autoFit:{...(local.options?.table?.autoFit||{}),mode:(e.target.value||undefined) as any}})}><option value="">{t('visualize.autoFitOff')}</option><option value="content">{t('visualize.autoFitContent')}</option><option value="window">{t('visualize.autoFitWindow')}</option></select></FormRow>
                <FormRow label={t('visualize.sampleRows')}><input type="number" min={1} max={100} className={inputCls('w-20')} value={local.options?.table?.autoFit?.sampleRows??10} onChange={e=>patchTable({autoFit:{...(local.options?.table?.autoFit||{}),sampleRows:Math.max(1,Math.min(100,Number(e.target.value||10)))}})} /></FormRow>
              </div>
              <div className="border-t pt-2 space-y-2">
                <div className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">{t('visualize.interactions')}</div>
                <FormRow label={t('visualize.quickFilterBox')}><Switch checked={!!local.options?.table?.filtering?.quickFilter} onChangeAction={v=>patchTable({filtering:{...(local.options?.table?.filtering||{}),quickFilter:v}})} /></FormRow>
                <FormRow label={t('visualize.columnMove')}><Switch checked={local.options?.table?.interactions?.columnMove!==false} onChangeAction={v=>patchTable({interactions:{...(local.options?.table?.interactions||{}),columnMove:v}})} /></FormRow>
                <FormRow label={t('visualize.columnResize')}><Switch checked={local.options?.table?.interactions?.columnResize!==false} onChangeAction={v=>patchTable({interactions:{...(local.options?.table?.interactions||{}),columnResize:v}})} /></FormRow>
                <FormRow label={t('visualize.columnHoverHighlight')}><Switch checked={!!local.options?.table?.interactions?.columnHoverHighlight} onChangeAction={v=>patchTable({interactions:{...(local.options?.table?.interactions||{}),columnHoverHighlight:v}})} /></FormRow>
                <FormRow label={t('visualize.suppressRowHover')}><Switch checked={!!(local.options?.table?.interactions as any)?.suppressRowHoverHighlight} onChangeAction={v=>patchTable({interactions:{...(local.options?.table?.interactions||{}),suppressRowHoverHighlight:v}})} /></FormRow>
              </div>
            </SectionCard>
          )}
          {(local.options?.table?.tableType||'data')==='pivot' && matches('pivot','styling','alignment','font','row height') && (
            <SectionCard title={t('sections.pivotStyling')}>
              {(()=>{ const ps=(local.options?.table?.pivotStyle||{}) as any; return (
                <div className="grid grid-cols-[minmax(80px,100px),1fr,1fr] gap-x-2 gap-y-2 items-center">
                  <div/><div className="text-[10px] font-medium text-muted-foreground">{t('visualize.headers')}</div><div className="text-[10px] font-medium text-muted-foreground">{t('visualize.cells')}</div>
                  <span className="text-xs text-muted-foreground">{t('visualize.rowHeight')}</span>
                  <input type="number" min={16} className={inputCls()} placeholder="auto" value={ps.headerRowHeight??''} onChange={e=>patchPivotStyle({headerRowHeight:e.target.value===''?undefined:Number(e.target.value)})} />
                  <input type="number" min={16} className={inputCls()} placeholder="auto" value={ps.cellRowHeight??''} onChange={e=>patchPivotStyle({cellRowHeight:e.target.value===''?undefined:Number(e.target.value)})} />
                  <span className="text-xs text-muted-foreground">{t('visualize.fontSize')}</span>
                  <input type="number" min={8} className={inputCls()} placeholder="auto" value={ps.headerFontSize??''} onChange={e=>patchPivotStyle({headerFontSize:e.target.value===''?undefined:Number(e.target.value)})} />
                  <input type="number" min={8} className={inputCls()} placeholder="auto" value={ps.cellFontSize??''} onChange={e=>patchPivotStyle({cellFontSize:e.target.value===''?undefined:Number(e.target.value)})} />
                  <span className="text-xs text-muted-foreground">{t('visualize.fontWeight')}</span>
                  <select className={selectCls('w-full')} value={ps.headerFontWeight||'semibold'} onChange={e=>patchPivotStyle({headerFontWeight:e.target.value})}>{['normal','medium','semibold','bold'].map(w=><option key={w} value={w}>{t(`options.fontWeight.${w}`)}</option>)}</select>
                  <select className={selectCls('w-full')} value={ps.cellFontWeight||'normal'} onChange={e=>patchPivotStyle({cellFontWeight:e.target.value})}>{['normal','medium','semibold','bold'].map(w=><option key={w} value={w}>{t(`options.fontWeight.${w}`)}</option>)}</select>
                  <span className="text-xs text-muted-foreground">{t('visualize.fontStyle')}</span>
                  <select className={selectCls('w-full')} value={ps.headerFontStyle||'normal'} onChange={e=>patchPivotStyle({headerFontStyle:e.target.value})}>{['normal','italic'].map(w=><option key={w} value={w}>{t(`options.fontStyle.${w}`)}</option>)}</select>
                  <select className={selectCls('w-full')} value={ps.cellFontStyle||'normal'} onChange={e=>patchPivotStyle({cellFontStyle:e.target.value})}>{['normal','italic'].map(w=><option key={w} value={w}>{t(`options.fontStyle.${w}`)}</option>)}</select>
                  <span className="text-xs text-muted-foreground">{t('visualize.hAlign')}</span>
                  <div className="flex gap-1">{(['left','center','right'] as const).map(a=><button key={a} onClick={()=>patchPivotStyle({headerHAlign:a})} className={`flex-1 h-7 text-xs rounded border cursor-pointer ${(ps.headerHAlign||'left')===a?'bg-[hsl(var(--muted))] ring-1 ring-[hsl(var(--primary))]':'bg-[hsl(var(--secondary)/0.6)]'}`}>{a==='left'?'L':a==='center'?'C':'R'}</button>)}</div>
                  <div className="flex gap-1">{(['left','center','right'] as const).map(a=><button key={a} onClick={()=>patchPivotStyle({cellHAlign:a})} className={`flex-1 h-7 text-xs rounded border cursor-pointer ${(ps.cellHAlign||'left')===a?'bg-[hsl(var(--muted))] ring-1 ring-[hsl(var(--primary))]':'bg-[hsl(var(--secondary)/0.6)]'}`}>{a==='left'?'L':a==='center'?'C':'R'}</button>)}</div>
                  <span className="text-xs text-muted-foreground">{t('visualize.vAlign')}</span>
                  <div className="flex gap-1">{(['top','center','bottom'] as const).map(a=><button key={a} onClick={()=>patchPivotStyle({headerVAlign:a})} className={`flex-1 h-7 text-xs rounded border cursor-pointer ${(ps.headerVAlign||'top')===a?'bg-[hsl(var(--muted))] ring-1 ring-[hsl(var(--primary))]':'bg-[hsl(var(--secondary)/0.6)]'}`}>{a==='top'?'T':a==='center'?'M':'B'}</button>)}</div>
                  <div className="flex gap-1">{(['top','center','bottom'] as const).map(a=><button key={a} onClick={()=>patchPivotStyle({cellVAlign:a})} className={`flex-1 h-7 text-xs rounded border cursor-pointer ${(ps.cellVAlign||'top')===a?'bg-[hsl(var(--muted))] ring-1 ring-[hsl(var(--primary))]':'bg-[hsl(var(--secondary)/0.6)]'}`}>{a==='top'?'T':a==='center'?'M':'B'}</button>)}</div>
                </div>
              )})()}
            </SectionCard>
          )}
          {(local.options?.table?.tableType||'data')==='pivot' && matches('pivot','options','totals','server','controls','alternate','hover','leaf','subtotals','expand') && (
            <SectionCard title={t('sections.pivotOptions')}>
              <FormRow label={t('visualize.computeOnServer')}><Switch checked={local.options?.table?.serverPivot!==false} onChangeAction={v=>patchTable({serverPivot:v})} /></FormRow>
              <FormRow label={t('visualize.showControls')}><Switch checked={local.options?.table?.showControls??true} onChangeAction={v=>patchTable({showControls:v})} /></FormRow>
              <FormRow label={t('visualize.rowTotals')}><Switch checked={local.options?.table?.pivotConfig?.rowTotals!==false} onChangeAction={v=>patchTable({pivotConfig:{...(local.options?.table?.pivotConfig||{}),rowTotals:v}})} /></FormRow>
              <FormRow label={t('visualize.columnTotals')}><Switch checked={local.options?.table?.pivotConfig?.colTotals!==false} onChangeAction={v=>patchTable({pivotConfig:{...(local.options?.table?.pivotConfig||{}),colTotals:v}})} /></FormRow>
              <FormRow label={t('visualize.alternatingRowBg')}><Switch checked={(local.options?.table?.pivotStyle as any)?.alternateRows!==false} onChangeAction={v=>patchPivotStyle({alternateRows:v})} /></FormRow>
              <FormRow label={t('visualize.rowHoverHighlight')}><Switch checked={(local.options?.table?.pivotStyle as any)?.rowHover!==false} onChangeAction={v=>patchPivotStyle({rowHover:v})} /></FormRow>
              <FormRow label={t('visualize.emphasizeLeafRows')}><Switch checked={!!(local.options?.table?.pivotStyle as any)?.leafRowEmphasis} onChangeAction={v=>patchPivotStyle({leafRowEmphasis:v})} /></FormRow>
              <FormRow label={t('visualize.hueTintRowHeader')}><Switch checked={!!(local.options?.table?.pivotStyle as any)?.rowHeaderDepthHue} onChangeAction={v=>patchPivotStyle({rowHeaderDepthHue:v})} /></FormRow>
              <FormRow label={t('visualize.hueTintColHeader')}><Switch checked={!!(local.options?.table?.pivotStyle as any)?.colHeaderDepthHue} onChangeAction={v=>patchPivotStyle({colHeaderDepthHue:v})} /></FormRow>
              <FormRow label={t('visualize.showSubtotals')}><Switch checked={!!(local.options?.table?.pivotStyle as any)?.showSubtotals} onChangeAction={v=>patchPivotStyle({showSubtotals:v})} /></FormRow>
              <FormRow label={t('visualize.collapseBorders')}><Switch checked={(local.options?.table?.pivotStyle as any)?.collapseBorders!==false} onChangeAction={v=>patchPivotStyle({collapseBorders:v})} /></FormRow>
              <FormRow label={t('visualize.expandIconStyle')}>
                <select className={selectCls()} value={(local.options?.table?.pivotStyle as any)?.expandIconStyle||'plusMinusLine'} onChange={e=>patchPivotStyle({expandIconStyle:e.target.value})}>
                  {['plusMinusLine','plusMinusFill','arrowLine','arrowFill','arrowWide','arrowDrop'].map(s=><option key={s} value={s}>{s}</option>)}
                </select>
              </FormRow>
              {local.options?.table?.serverPivot===false && (
                <>
                  <FormRow label={t('visualize.chunkSizeRows')}><input type="number" min={500} max={5000} className={inputCls('w-24')} value={local.options?.table?.pivotChunkSize??''} onChange={e=>patchTable({pivotChunkSize:e.target.value===''?undefined:Math.max(500,Math.min(5000,Number(e.target.value)))})} /></FormRow>
                  <FormRow label={t('visualize.maxRows')}><input type="number" min={1000} className={inputCls('w-24')} value={local.options?.table?.pivotMaxRows??''} onChange={e=>patchTable({pivotMaxRows:e.target.value===''?undefined:Number(e.target.value)})} /></FormRow>
                </>
              )}
            </SectionCard>
          )}
        </>
      )}

      {/* KPI Extras */}
      {local.type === 'kpi' && matches('kpi','down is good','top n','wrap','spark type','label case','target','aggregation') && (
        <SectionCard title={t('sections.kpiOptions')}>
          <FormRow label={t('visualize.downIsGood')}>
            <Switch checked={!!local.options?.kpi?.downIsGood}
              onChangeAction={v => { const kpi={...(local.options?.kpi||{}),downIsGood:v}; const next={...local,options:{...(local.options||{}),kpi}}; setLocal(next as any); updateConfig(next as any) }} />
          </FormRow>
          <FormRow label={t('visualize.topNCategory')}>
            <input type="number" min={1} className={inputCls('w-20')}
              value={local.options?.kpi?.topN ?? ''}
              placeholder="3"
              onChange={e => { const kpi={...(local.options?.kpi||{}),topN:e.target.value===''?undefined:Number(e.target.value)}; const next={...local,options:{...(local.options||{}),kpi}}; setLocal(next as any); updateConfig(next as any) }} />
          </FormRow>
          <FormRow label={t('visualize.wrapEveryN')}>
            <input type="number" min={1} className={inputCls('w-20')}
              value={(local.options?.kpi as any)?.wrapEveryN ?? ''}
              placeholder="3"
              onChange={e => { const kpi={...(local.options?.kpi||{}),wrapEveryN:e.target.value===''?undefined:Number(e.target.value)}; const next={...local,options:{...(local.options||{}),kpi}}; setLocal(next as any); updateConfig(next as any) }} />
          </FormRow>
          {local.options?.kpi?.preset === 'spark' && (
            <FormRow label={t('visualize.sparkType')}>
              <select className={selectCls()} value={local.options?.kpi?.sparkType || 'line'}
                onChange={e => { const kpi={...(local.options?.kpi||{}),sparkType:e.target.value as any}; const next={...local,options:{...(local.options||{}),kpi}}; setLocal(next as any); updateConfig(next as any) }}>
                <option value="line">{t('visualize.sparkLine')}</option>
                <option value="area">{t('visualize.sparkArea')}</option>
                <option value="bar">{t('visualize.sparkBar')}</option>
              </select>
            </FormRow>
          )}
          {(local.options?.kpi?.preset === 'donut' || local.options?.kpi?.preset === 'progress') && (
            <FormRow label={t('visualize.target')}>
              <input type="number" className={inputCls('w-24')} placeholder="auto"
                value={local.options?.kpi?.target ?? ''}
                onChange={e => { const kpi={...(local.options?.kpi||{}),target:e.target.value===''?undefined:Number(e.target.value)}; const next={...local,options:{...(local.options||{}),kpi}}; setLocal(next as any); updateConfig(next as any) }} />
            </FormRow>
          )}
          <FormRow label={t('data.labelCase')}>
            <select className={selectCls()} value={local.options?.kpi?.labelCase || ''}
              onChange={e => { const kpi={...(local.options?.kpi||{}),labelCase:e.target.value as any||undefined}; const next={...local,options:{...(local.options||{}),kpi}}; setLocal(next as any); updateConfig(next as any) }}>
              <option value="">{t('options.kpiCase.default')}</option>
              <option value="lowercase">{t('options.kpiCase.lowercase')}</option>
              <option value="capitalize">{t('options.kpiCase.capitalize')}</option>
              <option value="uppercase">{t('options.kpiCase.uppercase')}</option>
              <option value="capitalcase">{t('options.kpiCase.capitalcase')}</option>
            </select>
          </FormRow>
          <FormRow label={t('visualize.aggregationMode')}>
            <select className={selectCls()} value={local.options?.kpi?.aggregationMode || 'sum'}
              onChange={e => { const kpi={...(local.options?.kpi||{}),aggregationMode:e.target.value as any}; const next={...local,options:{...(local.options||{}),kpi}}; setLocal(next as any); updateConfig(next as any) }}>
              {['sum','count','distinctCount','avg','min','max','first','last','none'].map(a=><option key={a} value={a}>{t(`options.agg.${a}`)}</option>)}
            </select>
          </FormRow>
        </SectionCard>
      )}

      {/* Chart Appearance extras */}
      {local.type === 'chart' && matches('bar mode','stacked','grouped','data labels','line width','spark','filters') && (
        <SectionCard title={t('sections.appearance')}>
          {(local.chartType === 'bar' || local.chartType === 'column') && (
            <>
              <FormRow label={t('visualize.barMode')}>
                <select className={selectCls()} value={local.options?.barMode || 'default'}
                  onChange={e => patchOpt({barMode: e.target.value as any})}>
                  <option value="default">{t('visualize.barModeDefault')}</option>
                  <option value="grouped">{t('visualize.barModeGrouped')}</option>
                  <option value="stacked">{t('visualize.barModeStacked')}</option>
                </select>
              </FormRow>
              <FormRow label={t('visualize.barGap')}>
                <input type="number" min={0} max={100} className={inputCls('w-20')}
                  value={local.options?.barGap ?? 30}
                  onChange={e => patchOpt({barGap: Math.max(0, Math.min(100, Number(e.target.value || 30)))})} />
              </FormRow>
            </>
          )}
          {(local.chartType === 'line' || local.chartType === 'area' || local.chartType === 'combo') && (
            <FormRow label={t('visualize.lineWidth')}>
              <input type="number" min={1} max={8} className={inputCls('w-20')}
                value={local.options?.lineWidth ?? 2}
                onChange={e => patchOpt({lineWidth: Math.max(1, Math.min(8, Number(e.target.value || 2)))})} />
            </FormRow>
          )}
          {local.chartType === 'spark' && (
            <>
              <FormRow label={t('visualize.downIsGood')}>
                <Switch checked={!!local.options?.sparkDownIsGood} onChangeAction={v => patchOpt({sparkDownIsGood: v})} />
              </FormRow>
              <FormRow label={t('visualize.labelLines')}>
                <select className={selectCls()} value={String(local.options?.sparkLabelMaxLines || 2)}
                  onChange={e => patchOpt({sparkLabelMaxLines: Math.max(1, Math.min(3, Number(e.target.value)))})}>
                  <option value="1">{t('visualize.labelLines1')}</option>
                  <option value="2">{t('visualize.labelLines2')}</option>
                  <option value="3">{t('visualize.labelLines3')}</option>
                </select>
              </FormRow>
            </>
          )}
          <FormRow label={t('visualize.showDataLabels')}>
            <Switch checked={!!local.options?.dataLabelsShow} onChangeAction={v => patchOpt({dataLabelsShow: v})} />
          </FormRow>
          {local.options?.dataLabelsShow && (
            <FormRow label={t('visualize.labelPosition')}>
              <select className={selectCls()} value={local.options?.dataLabelPosition || 'outsideEnd'}
                onChange={e => patchOpt({dataLabelPosition: e.target.value as any})}>
                {['outsideEnd','insideEnd','insideBase','center','callout'].map(p=><option key={p} value={p}>{p}</option>)}
              </select>
            </FormRow>
          )}
          <FormRow label={t('visualize.filtersUI')}>
            <select className={selectCls()} value={local.options?.filtersUI || 'off'}
              onChange={e => patchOpt({filtersUI: e.target.value as any})}>
              <option value="off">{t('visualize.filtersUIOff')}</option>
              <option value="filterbars">{t('visualize.filtersUIBars')}</option>
            </select>
          </FormRow>
        </SectionCard>
      )}

      {/* Donut / Pie variants */}
      {local.type === 'chart' && local.chartType === 'donut' && matches('donut','pie','sunburst','nightingale','variant') && (
        <SectionCard title={t('sections.donutPie')}>
          <div className="grid grid-cols-2 gap-2">
            {(['donut','pie','sunburst','nightingale'] as const).map(v=>(
              <label key={v} className={`flex items-center gap-2 p-2 rounded-md border cursor-pointer text-xs ${((local.options as any)?.donutVariant||'donut')===v?'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]':'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>
                <input type="radio" name="donutVariantV2" className="sr-only" checked={((local.options as any)?.donutVariant||'donut')===v} onChange={()=>patchOpt({donutVariant:v} as any)} />
                <span className="capitalize">{t(`options.donutVariants.${v}`)}</span>
              </label>
            ))}
          </div>
        </SectionCard>
      )}

      {/* Gantt options */}
      {local.type === 'chart' && local.chartType === 'gantt' && matches('gantt','mode','duration','bar height') && (
        <SectionCard title={t('sections.ganttOptions')}>
          <FormRow label={t('visualize.ganttMode')} full>
            <div className="flex gap-4">
              {(['startEnd','startDuration'] as const).map(m=>(
                <label key={m} className="flex items-center gap-2 text-xs cursor-pointer">
                  <input type="radio" name="ganttModeV2" className="accent-[hsl(var(--primary))]" checked={((local.options as any)?.gantt?.mode||'startEnd')===m} onChange={()=>patchOpt({gantt:{...((local.options as any)?.gantt||{}),mode:m}} as any)} />
                  <span>{m==='startEnd'?t('visualize.ganttStartEnd'):t('visualize.ganttStartDuration')}</span>
                </label>
              ))}
            </div>
          </FormRow>
          {((local.options as any)?.gantt?.mode)==='startDuration' && (
            <FormRow label={t('visualize.durationUnit')}>
              <select className={selectCls()} value={(local.options as any)?.gantt?.durationUnit||'hours'} onChange={e=>patchOpt({gantt:{...((local.options as any)?.gantt||{}),durationUnit:e.target.value}} as any)}>
                {['seconds','minutes','hours','days','weeks','months'].map(u=><option key={u} value={u}>{t(`options.durationUnits.${u}`)}</option>)}
              </select>
            </FormRow>
          )}
          <FormRow label={t('visualize.barHeightPx')}>
            <input type="number" min={6} max={24} className={inputCls('w-20')} value={(local.options as any)?.gantt?.barHeight??10} onChange={e=>patchOpt({gantt:{...((local.options as any)?.gantt||{}),barHeight:Math.max(6,Math.min(24,Number(e.target.value||10)))}} as any)} />
          </FormRow>
        </SectionCard>
      )}

      {/* HeatMap presets */}
      {local.type === 'chart' && local.chartType === 'heatmap' && matches('heatmap','calendar','weekday','preset') && (
        <SectionCard title={t('sections.heatmapPreset')}>
          <div className="space-y-2">
            {([{key:'calendarMonthly',labelKey:'heatmapCalendarMonthly'},{key:'weekdayHour',labelKey:'heatmapWeekdayHour'},{key:'calendarAnnual',labelKey:'heatmapCalendarAnnual'}] as const).map(it=>(
              <label key={it.key} className={`flex items-center gap-2 p-2 rounded-md border cursor-pointer text-xs ${((local.options as any)?.heatmap?.preset||'calendarMonthly')===it.key?'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))]':'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>
                <input type="radio" name="heatmapPresetV2" className="sr-only" checked={((local.options as any)?.heatmap?.preset||'calendarMonthly')===it.key} onChange={()=>patchOpt({heatmap:{...((local.options as any)?.heatmap||{}),preset:it.key}} as any)} />
                <span>{t(`visualize.${it.labelKey}`)}</span>
              </label>
            ))}
          </div>
        </SectionCard>
      )}

      {/* TremorTable options */}
      {local.type === 'chart' && local.chartType === 'tremorTable' && matches('tremor','table','alternating','badge column','progress column','format','row click') && (
        <SectionCard title={t('sections.tremorTable')}>
          <FormRow label={t('visualize.alternatingRows')}><Switch checked={local.options?.tremorTable?.alternatingRows!==false} onChangeAction={v=>patchOpt({tremorTable:{...(local.options?.tremorTable||{}),alternatingRows:v}})} /></FormRow>
          <FormRow label={t('visualize.showTotalRow')}><Switch checked={!!local.options?.tremorTable?.showTotalRow} onChangeAction={v=>patchOpt({tremorTable:{...(local.options?.tremorTable||{}),showTotalRow:v}})} /></FormRow>
          <FormRow label={t('visualize.badgeColumns')} full>
            <select multiple className={`${inputCls()} h-20`} value={(local.options?.tremorTable?.badgeColumns||[]) as string[]} onChange={e=>{ const s=Array.from(e.currentTarget.selectedOptions).map(o=>o.value); patchOpt({tremorTable:{...(local.options?.tremorTable||{}),badgeColumns:s}}) }}>
              {allFieldNames.map(c=><option key={c} value={c}>{c}</option>)}
            </select>
          </FormRow>
          <FormRow label={t('visualize.progressColumns')} full>
            <select multiple className={`${inputCls()} h-20`} value={(local.options?.tremorTable?.progressColumns||[]) as string[]} onChange={e=>{ const s=Array.from(e.currentTarget.selectedOptions).map(o=>o.value); patchOpt({tremorTable:{...(local.options?.tremorTable||{}),progressColumns:s}}) }}>
              {allFieldNames.map(c=><option key={c} value={c}>{c}</option>)}
            </select>
          </FormRow>
          <div className="space-y-1.5 pt-1 border-t">
            <div className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">{t('visualize.formatByColumn')}</div>
            {Object.entries((local.options?.tremorTable?.formatByColumn||{}) as Record<string,string>).map(([col,mode])=>(
              <div key={col} className="flex items-center justify-between text-xs">
                <span className="truncate mr-2">{col} → {t(`options.formats.${mode}`)}</span>
                <button type="button" className="text-xs px-2 py-0.5 rounded border hover:bg-muted cursor-pointer" onClick={()=>{ const cur={...(local.options?.tremorTable?.formatByColumn||{})} as Record<string,'none'|'short'|'currency'|'percent'|'bytes'>; delete cur[col]; patchOpt({tremorTable:{...(local.options?.tremorTable||{}),formatByColumn:cur}}) }}>{t('common.remove')}</button>
              </div>
            ))}
            <div className="grid grid-cols-[1fr,1fr,auto] gap-2 items-center">
              <select className={selectCls('w-full')} value={ttFmtCol} onChange={e=>setTtFmtCol(e.target.value)}><option value="">{t('visualize.columnPlaceholder')}</option>{allFieldNames.map(c=><option key={c} value={c}>{c}</option>)}</select>
              <select className={selectCls('w-full')} value={ttFmtMode} onChange={e=>setTtFmtMode(e.target.value as any)}>{(['none','short','currency','percent','bytes'] as const).map(m=><option key={m} value={m}>{t(`options.formats.${m}`)}</option>)}</select>
              <button type="button" className="h-8 px-2 text-xs rounded-md border bg-[hsl(var(--primary))] text-primary-foreground disabled:opacity-50 cursor-pointer" disabled={!ttFmtCol} onClick={()=>{ if(!ttFmtCol) return; const map={...(local.options?.tremorTable?.formatByColumn||{}),[ttFmtCol]:ttFmtMode} as Record<string,'none'|'short'|'currency'|'percent'|'bytes'>; patchOpt({tremorTable:{...(local.options?.tremorTable||{}),formatByColumn:map}}) }}>{t('common.set')}</button>
            </div>
          </div>
          <div className="space-y-1.5 pt-1 border-t">
            <div className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">{t('visualize.rowAction')}</div>
            <FormRow label={t('visualize.emitRowClick')}><Switch checked={(local.options?.tremorTable?.rowClick?.type||'')==='emit'} onChangeAction={v=>{ const tt=v?{...(local.options?.tremorTable||{}),rowClick:{type:'emit' as const,eventName:local.options?.tremorTable?.rowClick?.eventName||'tremor-table-click'}}:(({rowClick:_,...r})=>r)(local.options?.tremorTable||{} as any); patchOpt({tremorTable:tt as any}) }} /></FormRow>
            {(local.options?.tremorTable?.rowClick?.type||'')==='emit' && (
              <FormRow label={t('visualize.eventName')} full><input className={inputCls()} value={local.options?.tremorTable?.rowClick?.eventName||'tremor-table-click'} onChange={e=>patchOpt({tremorTable:{...(local.options?.tremorTable||{}),rowClick:{type:'emit',eventName:e.target.value||'tremor-table-click'}}})} /></FormRow>
            )}
          </div>
        </SectionCard>
      )}

      {/* Badges options */}
      {local.type === 'chart' && local.chartType === 'badges' && matches('badges','preset','aggregation','category label','delta','value') && (
        <SectionCard title={t('sections.badges')}>
          <FormRow label={t('visualize.aggregationMode')}>
            <select className={selectCls()} value={(local.options?.kpi?.aggregationMode||'count') as string} onChange={e=>patchOpt({kpi:{...(local.options?.kpi||{}),aggregationMode:e.target.value as any}} as any)}>
              {['none','sum','count','distinctCount','avg','min','max','first','last'].map(m=><option key={m} value={m}>{t(`options.agg.${m}`)}</option>)}
            </select>
          </FormRow>
          <FormRow label={t('visualize.preset')} full>
            <div className="grid grid-cols-5 gap-1.5">
              {(['badge1','badge2','badge3','badge4','badge5'] as const).map(p=>(
                <button key={p} type="button" onClick={()=>patchOpt({badgesPreset:p} as any)} className={`p-1.5 rounded-md border text-xs cursor-pointer capitalize ${((local.options as any)?.badgesPreset||'badge1')===p?'bg-[hsl(var(--muted))] ring-2 ring-[hsl(var(--primary))] font-semibold':'bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]'}`}>{p}</button>
              ))}
            </div>
          </FormRow>
          <FormRow label={t('visualize.categoryLabelOutside')}><Switch checked={!!((local.options as any)?.badgesShowCategoryLabel)} onChangeAction={v=>patchOpt({badgesShowCategoryLabel:v,...(v?{badgesLabelInside:false}:{})} as any)} /></FormRow>
          <FormRow label={t('visualize.categoryLabelInside')}><Switch checked={!!((local.options as any)?.badgesLabelInside)} onChangeAction={v=>patchOpt({badgesLabelInside:v,...(v?{badgesShowCategoryLabel:false}:{})} as any)} /></FormRow>
          <FormRow label={t('visualize.showAggregatedValue')}><Switch checked={!!((local.options as any)?.badgesShowValue)} onChangeAction={v=>patchOpt({badgesShowValue:v} as any)} /></FormRow>
          <FormRow label={t('visualize.showDelta')}><Switch checked={!!((local.options as any)?.badgesShowDelta)} onChangeAction={v=>patchOpt({badgesShowDelta:v} as any)} /></FormRow>
          <FormRow label={t('visualize.showDeltaPct')}><Switch checked={!!((local.options as any)?.badgesShowDeltaPct)} onChangeAction={v=>patchOpt({badgesShowDeltaPct:v} as any)} /></FormRow>
          <FormRow label={t('visualize.showPercentOfTotal')}><Switch checked={!!((local.options as any)?.badgesShowPercentOfTotal)} onChangeAction={v=>patchOpt({badgesShowPercentOfTotal:v} as any)} /></FormRow>
        </SectionCard>
      )}

      {/* Advanced mode (ECharts) */}
      {local.type === 'chart' && local.chartType !== 'spark' && matches('advanced mode','echarts','rounded bars','gradient','bar mode') && (
        <SectionCard title={t('sections.advancedMode')}>
          <FormRow label={t('visualize.advancedModeECharts')}><Switch checked={!!local.options?.advancedMode} onChangeAction={v=>patchOpt({advancedMode:v})} /></FormRow>
          {local.options?.advancedMode && (local.chartType==='bar'||local.chartType==='column') && (
            <>
              <FormRow label={t('visualize.roundedBars')}><Switch checked={!!local.options?.barRounded} onChangeAction={v=>patchOpt({barRounded:v})} /></FormRow>
              <FormRow label={t('visualize.gradientFill')}><Switch checked={!!((local.options as any)?.barGradient)} onChangeAction={v=>patchOpt({barGradient:v} as any)} /></FormRow>
              <FormRow label={t('visualize.barMode')}>
                <select className={selectCls()} value={local.options?.barMode||'default'} onChange={e=>patchOpt({barMode:e.target.value as any})}>
                  <option value="default">{t('visualize.barModeDefault')}</option><option value="grouped">{t('visualize.barModeGrouped')}</option><option value="stacked">{t('visualize.barModeStacked')}</option>
                </select>
              </FormRow>
            </>
          )}
          {local.options?.advancedMode && (local.chartType==='line'||local.chartType==='area') && (
            <FormRow label={t('visualize.lineWidthPx')}><input type="number" min={1} max={8} className={inputCls('w-20')} value={local.options?.lineWidth??2} onChange={e=>patchOpt({lineWidth:Math.max(1,Math.min(8,Number(e.target.value||2)))})} /></FormRow>
          )}
        </SectionCard>
      )}

      {/* Delta Configuration */}
      {(local.type === 'chart' || local.type === 'kpi') && matches('delta','comparison','period','week start','date field','ui mode','resolved period preview','current','previous') && (
        <SectionCard title={t('sections.deltaComparison')}>
          <FormRow label={t('visualize.deltaMode')}>
            <select className={selectCls()} value={local.options?.deltaMode || 'off'}
              onChange={e => patchOpt({deltaMode: e.target.value as any})}>
              <option value="off">{t('visualize.deltaOff')}</option>
              {['TD_YSTD','TW_LW','MONTH_LMONTH','MTD_LMTD','TY_LY','YTD_LYTD','TQ_LQ','Q_TY_VS_Q_LY','QTD_TY_VS_QTD_LY','M_TY_VS_M_LY','MTD_TY_VS_MTD_LY'].map(m=>(
                <option key={m} value={m}>{m.replace(/_/g,' ')}</option>
              ))}
            </select>
          </FormRow>
          {local.options?.deltaMode && local.options.deltaMode !== 'off' && (
            <>
              <FormRow label={t('visualize.dateField')} full>
                <select className={selectCls('w-full')} value={local.options?.deltaDateField || ''}
                  onChange={e => patchOpt({deltaDateField: e.target.value || undefined})}>
                  <option value="">{t('visualize.selectDateField')}</option>
                  {allFieldNames.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
              </FormRow>
              <FormRow label={t('visualize.weekStart')}>
                <select className={selectCls()} value={local.options?.deltaWeekStart || 'mon'}
                  onChange={e => patchOpt({deltaWeekStart: e.target.value as any})}>
                  <option value="mon">{t('visualize.weekMon')}</option>
                  <option value="sun">{t('visualize.weekSun')}</option>
                  <option value="sat">{t('visualize.weekSat')}</option>
                </select>
              </FormRow>
              <FormRow label={t('visualize.uiMode')}>
                <select className={selectCls()} value={local.options?.deltaUI || 'preconfigured'}
                  onChange={e => patchOpt({deltaUI: e.target.value as any})}>
                  <option value="preconfigured">{t('visualize.uiPreconfigured')}</option>
                  <option value="filterbar">{t('visualize.uiFilterbar')}</option>
                  <option value="none">{t('visualize.uiNone')}</option>
                </select>
              </FormRow>
              {/* Resolved-period preview (ported from V1) */}
              {local.options?.deltaDateField && (
                <div className="mt-1 rounded-md border bg-[hsl(var(--secondary))] p-2 space-y-0.5 text-xs text-muted-foreground">
                  <div className="font-medium text-foreground mb-0.5">{t('visualize.resolvedPreview')}</div>
                  <div>{t('visualize.modeColon')} <span className="font-mono">{String(local.options?.deltaMode)}</span></div>
                  <div>{t('visualize.dateFieldColon')} <span className="font-mono">{String(local.options?.deltaDateField)}</span></div>
                  <div className="pt-0.5">
                    {deltaPreviewLoading ? (
                      <span>{t('data.resolving')}</span>
                    ) : deltaPreviewError ? (
                      <span className="text-[hsl(var(--destructive))]">{deltaPreviewError}</span>
                    ) : deltaResolved ? (
                      <div className="space-y-0.5">
                        <div>{t('visualize.current')} <span className="font-mono">{deltaResolved.curStart}</span> → <span className="font-mono">{deltaResolved.curEnd}</span></div>
                        <div>{t('visualize.previous')} <span className="font-mono">{deltaResolved.prevStart}</span> → <span className="font-mono">{deltaResolved.prevEnd}</span></div>
                      </div>
                    ) : (
                      <span>—</span>
                    )}
                  </div>
                  <div>{t('visualize.firstRowIn',{field:String(local.options?.deltaDateField)})} <span className="font-mono">{deltaSampleNow ?? '—'}</span></div>
                  <div className="pt-1">
                    <button type="button" className="text-xs px-2 py-0.5 rounded-md border bg-card hover:bg-muted transition-colors duration-150 cursor-pointer"
                      onClick={() => void refreshDeltaPreview()}>{t('common.refresh')}</button>
                  </div>
                </div>
              )}
            </>
          )}
        </SectionCard>
      )}

    </div>
  )
}
