"use client"
import { useState } from 'react'
import { Switch } from '@/components/Switch'
import type { WidgetConfig, CompositionComponent } from '@/types/widgets'
import { SectionCard, FormRow, inputCls, selectCls } from './shared'
import dynamic from 'next/dynamic'

const RichTextEditor = dynamic(() => import('@/components/ui/RichTextEditor'), { ssr: false })
const CompositionBuilderModal = dynamic(() => import('@/components/builder/CompositionBuilderModal'), { ssr: false })
const ReportBuilderModal = dynamic(() => import('@/components/builder/ReportBuilderModal'), { ssr: false })

export function GeneralTab({ local, setLocal, updateConfig, search = '', allWidgets, quickAddAction }: {
  local: WidgetConfig
  setLocal: (c: WidgetConfig) => void
  updateConfig: (c: WidgetConfig) => void
  search?: string
  allWidgets?: Record<string, WidgetConfig>
  quickAddAction?: (kind: 'kpi' | 'chart' | 'table', opts?: { addToLayout?: boolean }) => string
}) {
  const [compOpen, setCompOpen] = useState(false)
  const [reportOpen, setReportOpen] = useState(false)
  const s = search.toLowerCase().trim()
  const matches = (...terms: string[]) => !s || terms.some(t => t.toLowerCase().includes(s))
  const patch = (p: Partial<WidgetConfig>) => {
    const next = { ...local, ...p }
    setLocal(next); updateConfig(next)
  }
  const patchOpt = (p: Partial<NonNullable<WidgetConfig['options']>>) => {
    const next = { ...local, options: { ...(local.options || {}), ...p } }
    setLocal(next); updateConfig(next)
  }

  return (
    <div className="space-y-3">

      {/* Identity */}
      {matches('identity','title','type','widget') && <SectionCard title="Identity">
        <FormRow label="Widget type">
          <span className="text-xs px-2 py-0.5 rounded border bg-[hsl(var(--secondary))] text-muted-foreground capitalize">{local.type}</span>
        </FormRow>
        <FormRow label="Card title" full>
          <input
            className={inputCls()}
            value={local.title}
            onChange={(e) => patch({ title: e.target.value })}
            placeholder="Enter card title…"
          />
        </FormRow>
      </SectionCard>}

      {/* Appearance */}
      {matches('appearance','header','fill','color','autofit','load time') && <SectionCard title="Appearance">
        <FormRow label="Show card header">
          <Switch
            checked={local.options?.showCardHeader !== false}
            onChangeAction={(v) => patchOpt({ showCardHeader: v })}
          />
        </FormRow>
        <FormRow label="Card fill" full>
          <select
            className={selectCls('w-full')}
            value={local.options?.cardFill || 'default'}
            onChange={(e) => {
              const val = e.target.value as any
              const opts: any = { ...(local.options || {}), cardFill: val }
              if (val === 'custom' && !opts.cardCustomColor) opts.cardCustomColor = '#ffffff'
              const next = { ...local, options: opts }
              setLocal(next); updateConfig(next)
            }}
          >
            <option value="default">Default</option>
            <option value="transparent">No fill</option>
            <option value="custom">Custom color</option>
          </select>
        </FormRow>
        {local.options?.cardFill === 'custom' && (
          <FormRow label="Custom color">
            <input
              type="color"
              className="h-8 w-16 rounded-md border bg-[hsl(var(--secondary))] cursor-pointer"
              value={local.options?.cardCustomColor || '#ffffff'}
              onChange={(e) => patchOpt({ cardCustomColor: e.target.value })}
            />
          </FormRow>
        )}
        <FormRow label="Autofit card content">
          <Switch
            checked={local.options?.autoFitCardContent !== false}
            onChangeAction={(v) => patchOpt({ autoFitCardContent: v })}
          />
        </FormRow>
        <FormRow label="Show load time">
          <Switch
            checked={!!((local.options as any)?.showLoadTime)}
            onChangeAction={(v) => { const next = { ...local, options: { ...(local.options || {}), showLoadTime: v || undefined } as any }; setLocal(next); updateConfig(next) }}
          />
        </FormRow>
      </SectionCard>}

      {/* Spacer */}
      {local.type === 'spacer' && matches('spacer','min width','cols') && (
        <SectionCard title="Spacer Options">
          <FormRow label="Min width (cols)">
            <input
              type="number" min={1} max={12}
              className={inputCls('w-20')}
              value={Number((local.options?.spacer?.minW ?? 2) as any)}
              onChange={(e) => {
                const v = Math.max(1, Math.min(12, Number(e.target.value) || 1))
                const next = { ...local, options: { ...(local.options || {}), spacer: { ...(local.options?.spacer || {}), minW: v } } }
                setLocal(next); updateConfig(next as any)
              }}
            />
          </FormRow>
        </SectionCard>
      )}

      {/* Text Content */}
      {local.type === 'text' && matches('text','content','html','image','sanitize') && (
        <SectionCard title="Text Content">
          <div className="text-xs text-muted-foreground">Bold, italic, underline, links, alignment, font size, weight, and color.</div>
          <RichTextEditor
            value={local.options?.text?.html || ''}
            onChange={(html: string) => {
              const text = { ...(local.options?.text || {}), html }
              const next = { ...local, options: { ...(local.options || {}), text } }
              setLocal(next); updateConfig(next as any)
            }}
            height={220}
          />
          <FormRow label="Sanitize HTML">
            <Switch checked={!!local.options?.text?.sanitizeHtml}
              onChangeAction={v => { const text = { ...(local.options?.text || {}), sanitizeHtml: v }; const next = { ...local, options: { ...(local.options || {}), text } }; setLocal(next); updateConfig(next as any) }} />
          </FormRow>
          <div className="border-t pt-2 space-y-2">
            <div className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">Image</div>
            <FormRow label="URL" full><input className={inputCls()} placeholder="https://…" value={local.options?.text?.imageUrl||''} onChange={e=>{ const text={...(local.options?.text||{}),imageUrl:e.target.value}; const next={...local,options:{...(local.options||{}),text}}; setLocal(next); updateConfig(next as any) }} /></FormRow>
            <FormRow label="Alt text" full><input className={inputCls()} placeholder="description" value={local.options?.text?.imageAlt||''} onChange={e=>{ const text={...(local.options?.text||{}),imageAlt:e.target.value}; const next={...local,options:{...(local.options||{}),text}}; setLocal(next); updateConfig(next as any) }} /></FormRow>
            <FormRow label="Align"><select className={selectCls()} value={local.options?.text?.imageAlign||'left'} onChange={e=>{ const text={...(local.options?.text||{}),imageAlign:e.target.value as any}; const next={...local,options:{...(local.options||{}),text}}; setLocal(next); updateConfig(next as any) }}>{['left','center','right'].map(a=><option key={a} value={a}>{a}</option>)}</select></FormRow>
            <FormRow label="Width (px)"><input type="number" min={16} max={2048} className={inputCls('w-24')} value={Number(local.options?.text?.imageWidth||64)} onChange={e=>{ const v=Math.max(16,Math.min(2048,Number(e.target.value)||64)); const text={...(local.options?.text||{}),imageWidth:v}; const next={...local,options:{...(local.options||{}),text}}; setLocal(next); updateConfig(next as any) }} /></FormRow>
          </div>
        </SectionCard>
      )}

      {/* Composition Options */}
      {local.type === 'composition' && matches('composition','columns','layout','gap','card builder') && (
        <SectionCard title="Composition Options">
          <FormRow label="Columns">
            <select className={selectCls()} value={String(local.options?.composition?.columns||12)} onChange={e=>{ const cols=Number(e.target.value) as 6|8|12; const composition={...(local.options?.composition||{components:[]}),columns:cols}; const next={...local,options:{...(local.options||{}),composition}}; setLocal(next); updateConfig(next as any) }}>{[6,8,12].map(n=><option key={n} value={n}>{n}</option>)}</select>
          </FormRow>
          <FormRow label="Layout">
            <select className={selectCls()} value={String(local.options?.composition?.layout||'grid')} onChange={e=>{ const layout=(e.target.value==='stack'?'stack':'grid') as 'grid'|'stack'; const composition={...(local.options?.composition||{components:[]}),layout}; const next={...local,options:{...(local.options||{}),composition}}; setLocal(next); updateConfig(next as any) }}><option value="grid">Grid</option><option value="stack">Stack</option></select>
          </FormRow>
          <FormRow label="Gap">
            <select className={selectCls()} value={String(local.options?.composition?.gap??2)} onChange={e=>{ const gap=Number(e.target.value); const composition={...(local.options?.composition||{components:[]}),gap}; const next={...local,options:{...(local.options||{}),composition}}; setLocal(next); updateConfig(next as any) }}>{[0,1,2,3,4,5,6].map(n=><option key={n} value={n}>{n}</option>)}</select>
          </FormRow>
          <button type="button" className="text-xs px-2.5 py-1 rounded-md border hover:bg-muted cursor-pointer" onClick={()=>setCompOpen(true)}>Open Card Builder</button>
          <CompositionBuilderModal
            open={compOpen}
            onClose={()=>setCompOpen(false)}
            value={local.options?.composition?.components||[]}
            columns={(local.options?.composition?.columns as 6|8|12)||12}
            choices={Object.values(allWidgets||{}).map(w=>({id:w.id,title:w.title,type:w.type}))}
            onQuickAdd={quickAddAction}
            onChange={(next: CompositionComponent[])=>{ const composition={...(local.options?.composition||{}),components:next}; const cfg={...local,options:{...(local.options||{}),composition}} as WidgetConfig; setLocal(cfg); updateConfig(cfg) }}
          />
        </SectionCard>
      )}

      {/* Report Builder */}
      {local.type === 'report' && matches('report','builder','elements','variables') && (
        <SectionCard title="Report Builder">
          <div className="text-xs text-muted-foreground">Design your report layout with labels, tables, and data-bound spaceholders.</div>
          <div className="grid grid-cols-2 gap-2">
            <div className="rounded border p-2 bg-[hsl(var(--secondary)/0.4)] text-center">
              <div className="text-sm font-medium">{(local.options?.report?.elements||[]).length}</div>
              <div className="text-xs text-muted-foreground">Elements</div>
            </div>
            <div className="rounded border p-2 bg-[hsl(var(--secondary)/0.4)] text-center">
              <div className="text-sm font-medium">{(local.options?.report?.variables||[]).length}</div>
              <div className="text-xs text-muted-foreground">Variables</div>
            </div>
          </div>
          <button type="button" className="w-full text-xs px-3 py-2 rounded-md border bg-[hsl(var(--primary))] text-primary-foreground hover:bg-[hsl(var(--primary)/0.9)] cursor-pointer" onClick={()=>setReportOpen(true)}>Open Report Builder</button>
          <ReportBuilderModal
            open={reportOpen}
            onCloseAction={()=>setReportOpen(false)}
            config={local}
            onSaveAction={(next: WidgetConfig)=>{ setLocal(next); updateConfig(next) }}
          />
        </SectionCard>
      )}

    </div>
  )
}
