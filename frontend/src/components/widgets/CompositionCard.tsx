"use client"

import ErrorBoundary from '@/components/dev/ErrorBoundary'
import type { WidgetConfig, CompositionComponent } from '@/types/widgets'
import KpiCard from '@/components/widgets/KpiCard'
import ChartCard from '@/components/widgets/ChartCard'
import HeatmapCard from '@/components/widgets/HeatmapCard'
import TableCard from '@/components/widgets/TableCard'
import { useEffect, useRef, useState } from 'react'
import type React from 'react'

export default function CompositionCard({ title, options, widgets, interactive = false, onSelectWidget, onUpdate, widgetId }: { title: string; options?: WidgetConfig['options']; widgets?: Record<string, WidgetConfig>; interactive?: boolean; onSelectWidget?: (id: string) => void; onUpdate?: (next: CompositionComponent[]) => void; widgetId?: string }) {
  const autoFit = options?.autoFitCardContent !== false
  const cardFill = options?.cardFill || 'default'
  const bgStyle = cardFill === 'transparent' ? { backgroundColor: 'transparent' } : cardFill === 'custom' ? { backgroundColor: options?.cardCustomColor || '#ffffff' } : undefined
  const cardClass = `${autoFit ? '' : 'h-full'} !border-0 shadow-none rounded-lg ${cardFill === 'transparent' ? 'bg-transparent' : 'bg-card'}`

  const comp = options?.composition
  const cols = comp?.columns || 12
  const colClass = cols === 6 ? 'grid-cols-6' : cols === 8 ? 'grid-cols-8' : 'grid-cols-12'
  const gap = typeof comp?.gap === 'number' ? comp!.gap : 2
  const gapClass = ({ 0: 'gap-0', 1: 'gap-1', 2: 'gap-2', 3: 'gap-3', 4: 'gap-4', 5: 'gap-5', 6: 'gap-6' } as Record<number, string>)[gap] || 'gap-2'
  const spanClassFor = (n: number) => ({
    1:'col-span-1',2:'col-span-2',3:'col-span-3',4:'col-span-4',5:'col-span-5',6:'col-span-6',
    7:'col-span-7',8:'col-span-8',9:'col-span-9',10:'col-span-10',11:'col-span-11',12:'col-span-12'
  } as Record<number,string>)[n] || 'col-span-12'

  const components = comp?.components || []
  const [activeIdx, setActiveIdx] = useState<number | null>(null)
  // Grid container ref for resize math
  const gridRef = useRef<HTMLDivElement | null>(null)
  // Per-item element refs (by index)
  const itemRefs = useRef<Record<number, HTMLDivElement | null>>({})
  const move = (from: number, to: number) => {
    if (!onUpdate) return
    if (from === to) return
    const arr = [...components]
    const [item] = arr.splice(from, 1)
    arr.splice(to, 0, item)
    onUpdate(arr)
  }
  const setSpan = (i: number, span: number) => {
    if (!onUpdate) return
    const s = Math.min(cols, Math.max(1, span))
    onUpdate(components.map((c, idx) => idx === i ? ({ ...c, span: s }) : c))
  }
  // Drag-to-resize: compute span from mouse X relative to item left edge
  const startResize = (i: number, e: React.MouseEvent<HTMLDivElement>) => {
    e.preventDefault()
    e.stopPropagation()
    setActiveIdx(i)
    const container = gridRef.current
    const itemEl = itemRefs.current[i]
    if (!container || !itemEl) return
    const containerRect = container.getBoundingClientRect()
    const itemRect = itemEl.getBoundingClientRect()
    const colW = containerRect.width / Math.max(1, cols)
    const onMove = (ev: MouseEvent) => {
      const x = ev.clientX
      const widthPx = Math.max(1, x - itemRect.left)
      const raw = Math.round(widthPx / colW)
      setSpan(i, raw)
    }
    const onUp = () => {
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }
  const onEditComponent = (i: number) => {
    setActiveIdx(i)
    // notify Configurator to open builder focused on this component
    if (typeof window !== 'undefined' && widgetId) {
      try { window.dispatchEvent(new CustomEvent('composition-edit-component', { detail: { widgetId, compIndex: i, compId: (components[i] as any)?.id } } as any)) } catch {}
    }
  }

  return (
    <ErrorBoundary name="CompositionCard">
      <div className={cardClass} style={bgStyle as any}>
        {(!comp || !Array.isArray(comp.components) || comp.components.length === 0) ? (
          <div className="text-xs text-muted-foreground">Composition layout empty. Use the builder to add components.</div>
        ) : (
          <div ref={gridRef} className={`grid ${colClass} ${gapClass}`}>
            {components.map((c, idx) => {
              const span = Math.min(cols, Math.max(1, c.span || cols))
              const spanClass = spanClassFor(span)
              const isSmall = span <= 2
              const glyphBoxW = isSmall ? 'w-[12px]' : 'w-[14px]'
              const glyphW = isSmall ? 'w-[6px]' : 'w-[8px]'
              const glyphRadius = isSmall ? 'rounded-r-[1px]' : 'rounded-r-[2px]'
              const interactiveAttrs = interactive ? {
                draggable: true,
                onDragStart: (e: React.DragEvent<HTMLDivElement>) => { e.dataTransfer.setData('text/plain', String(idx)); e.dataTransfer.effectAllowed = 'move' },
                onDragOver: (e: React.DragEvent<HTMLDivElement>) => { e.preventDefault(); e.dataTransfer.dropEffect = 'move' },
                onDrop: (e: React.DragEvent<HTMLDivElement>) => { e.preventDefault(); const from = Number(e.dataTransfer.getData('text/plain')); move(from, idx) },
                onClick: () => setActiveIdx(idx),
                tabIndex: 0,
                onKeyDown: (e: React.KeyboardEvent<HTMLDivElement>) => {
                  if (e.key === 'ArrowLeft') { e.preventDefault(); setSpan(idx, span - 1) }
                  if (e.key === 'ArrowRight') { e.preventDefault(); setSpan(idx, span + 1) }
                  if ((e.key === 'Enter' || e.key === ' ') && (c.kind === 'title' || c.kind === 'subtitle')) { e.preventDefault(); onEditComponent(idx) }
                },
                'aria-grabbed': activeIdx === idx ? 'true' : 'false',
              } : {}
              const wrapClass = `${spanClass} relative overflow-visible ${interactive ? 'group' : ''} ${interactive && activeIdx === idx ? 'ring-2 ring-[hsl(var(--primary))] ring-offset-1 ring-offset-[hsl(var(--card))] rounded-md' : ''}`
              if (c.kind === 'title') {
                return (
                  <div
                    key={c.id || idx}
                    ref={(el) => { itemRefs.current[idx] = el }}
                    className={`${wrapClass} ${c.align === 'center' ? 'text-center' : c.align === 'right' ? 'text-right' : 'text-left'} text-xl font-semibold`}
                    onDoubleClick={() => interactive && onEditComponent(idx)}
                    {...(interactiveAttrs as any)}
                  >
                    {c.text || ''}
                    {interactive && (
                      <div className={`absolute right-0 top-0 h-full ${glyphBoxW} pointer-events-none`}>
                        <div
                          className={`absolute left-0 top-0 h-full ${glyphW} cursor-ew-resize border-l border-dashed ${activeIdx === idx ? 'border-[hsl(var(--primary))]' : 'border-[hsl(var(--muted-foreground))]/40'} ${activeIdx === idx ? 'bg-[hsl(var(--primary))]/10' : 'bg-transparent'} hover:bg-[hsl(var(--primary))]/15 ${glyphRadius} pointer-events-auto`}
                          onMouseDown={(e) => startResize(idx, e)}
                          role="separator"
                          aria-orientation="horizontal"
                          aria-label="Resize"
                          title="Drag right edge to resize"
                        />
                        {activeIdx === idx && (
                          <button
                            type="button"
                            className="absolute top-1 right-1 text-[10px] leading-none px-1.5 py-0.5 rounded border bg-[hsl(var(--btn1))] text-black shadow-sm pointer-events-auto"
                            onClick={(e) => { e.stopPropagation(); onEditComponent(idx) }}
                            title="Edit text"
                            aria-label="Edit text"
                          >✎</button>
                        )}
                      </div>
                    )}
                  </div>
                )
              }
              if (c.kind === 'subtitle') {
                return (
                  <div
                    key={c.id || idx}
                    ref={(el) => { itemRefs.current[idx] = el }}
                    className={`${wrapClass} ${c.align === 'center' ? 'text-center' : c.align === 'right' ? 'text-right' : 'text-left'} text-sm text-muted-foreground`}
                    onDoubleClick={() => interactive && onEditComponent(idx)}
                    {...(interactiveAttrs as any)}
                  >
                    {c.text || ''}
                    {interactive && (
                      <div className={`absolute right-0 top-0 h-full ${glyphBoxW} pointer-events-none`}>
                        <div
                          className={`absolute left-0 top-0 h-full ${glyphW} cursor-ew-resize border-l border-dashed ${activeIdx === idx ? 'border-[hsl(var(--primary))]' : 'border-[hsl(var(--muted-foreground))]/40'} ${activeIdx === idx ? 'bg-[hsl(var(--primary))]/10' : 'bg-transparent'} hover:bg-[hsl(var(--primary))]/15 ${glyphRadius} pointer-events-auto`}
                          onMouseDown={(e) => startResize(idx, e)}
                          role="separator"
                          aria-orientation="horizontal"
                          aria-label="Resize"
                          title="Drag right edge to resize"
                        />
                        {activeIdx === idx && (
                          <button
                            type="button"
                            className="absolute top-1 right-1 text-[10px] leading-none px-1.5 py-0.5 rounded border bg-[hsl(var(--btn1))] text-black shadow-sm pointer-events-auto"
                            onClick={(e) => { e.stopPropagation(); onEditComponent(idx) }}
                            title="Edit text"
                            aria-label="Edit text"
                          >✎</button>
                        )}
                      </div>
                    )}
                  </div>
                )
              }
              // Render referenced widgets by id when available
              if ((c.kind === 'kpi' || c.kind === 'chart' || c.kind === 'table') && (c as any).refId && widgets) {
                const refId = (c as any).refId as string
                const ref = widgets[refId]
                if (ref && ref.type === 'kpi' && c.kind === 'kpi') {
                  return (
                    <div
                      key={c.id || idx}
                      ref={(el) => { itemRefs.current[idx] = el }}
                      className={`${wrapClass}`}
                      {...(interactiveAttrs as any)}
                      onClick={(e) => { if (!interactive) return; e.stopPropagation(); setActiveIdx(idx); onSelectWidget && onSelectWidget(ref.id) }}
                    >
                      {(c as any).label && (
                        <div className="text-[11px] text-muted-foreground mb-1">{(c as any).label}</div>
                      )}
                      <ErrorBoundary name="KpiCard@Composition">
                        <KpiCard
                          title={ref.title}
                          sql={ref.sql}
                          datasourceId={ref.datasourceId}
                          queryMode={ref.queryMode}
                          querySpec={ref.querySpec as any}
                          options={ref.options}
                          pivot={ref.pivot as any}
                          widgetId={ref.id}
                        />
                      </ErrorBoundary>
                      {interactive && (
                        <div className={`absolute right-0 top-0 h-full ${glyphBoxW} pointer-events-none`} title="Click widget to configure; drag right edge to resize">
                          <div
                            className={`absolute left-0 top-0 h-full ${glyphW} cursor-ew-resize border-l border-dashed ${activeIdx === idx ? 'border-[hsl(var(--primary))]' : 'border-[hsl(var(--muted-foreground))]/40'} ${activeIdx === idx ? 'bg-[hsl(var(--primary))]/10' : 'bg-transparent'} hover:bg-[hsl(var(--primary))]/15 ${glyphRadius} pointer-events-auto`}
                            onMouseDown={(e) => startResize(idx, e)}
                            role="separator"
                            aria-orientation="horizontal"
                            aria-label="Resize"
                          />
                        </div>
                      )}
                    </div>
                  )
                }
                if (ref && ref.type === 'chart' && c.kind === 'chart') {
                  return (
                    <div
                      key={c.id || idx}
                      ref={(el) => { itemRefs.current[idx] = el }}
                      className={`${wrapClass}`}
                      {...(interactiveAttrs as any)}
                      onClick={(e) => { if (!interactive) return; e.stopPropagation(); setActiveIdx(idx); onSelectWidget && onSelectWidget(ref.id) }}
                    >
                      {(c as any).label && (
                        <div className="text-[11px] text-muted-foreground mb-1">{(c as any).label}</div>
                      )}
                      <ErrorBoundary name="ChartCard@Composition">
                        {((ref as any).chartType === 'heatmap') ? (
                          <HeatmapCard
                            title={ref.title}
                            sql={ref.sql}
                            datasourceId={ref.datasourceId}
                            options={ref.options}
                            queryMode={ref.queryMode}
                            querySpec={ref.querySpec as any}
                            widgetId={ref.id}
                          />
                        ) : (
                          <ChartCard
                            title={ref.title}
                            sql={ref.sql}
                            datasourceId={ref.datasourceId}
                            type={(ref as any).chartType || 'line'}
                            options={ref.options}
                            queryMode={ref.queryMode}
                            querySpec={ref.querySpec as any}
                            customColumns={ref.customColumns}
                            widgetId={ref.id}
                            pivot={ref.pivot}
                          />
                        )}
                      </ErrorBoundary>
                      {interactive && (
                        <div className={`absolute right-0 top-0 h-full ${glyphBoxW} pointer-events-none`} title="Click widget to configure; drag right edge to resize">
                          <div
                            className={`absolute left-0 top-0 h-full ${glyphW} cursor-ew-resize border-l border-dashed ${activeIdx === idx ? 'border-[hsl(var(--primary))]' : 'border-[hsl(var(--muted-foreground))]/40'} ${activeIdx === idx ? 'bg-[hsl(var(--primary))]/10' : 'bg-transparent'} hover:bg-[hsl(var(--primary))]/15 ${glyphRadius} pointer-events-auto`}
                            onMouseDown={(e) => startResize(idx, e)}
                            role="separator"
                            aria-orientation="horizontal"
                            aria-label="Resize"
                          />
                        </div>
                      )}
                    </div>
                  )
                }
                if (ref && ref.type === 'table' && c.kind === 'table') {
                  return (
                    <div
                      key={c.id || idx}
                      ref={(el) => { itemRefs.current[idx] = el }}
                      className={`${wrapClass}`}
                      {...(interactiveAttrs as any)}
                      onClick={(e) => { if (!interactive) return; e.stopPropagation(); setActiveIdx(idx); onSelectWidget && onSelectWidget(ref.id) }}
                    >
                      {(c as any).label && (
                        <div className="text-[11px] text-muted-foreground mb-1">{(c as any).label}</div>
                      )}
                      <ErrorBoundary name="TableCard@Composition">
                        <TableCard
                          title={ref.title}
                          sql={ref.sql}
                          datasourceId={ref.datasourceId}
                          options={ref.options as any}
                          queryMode={ref.queryMode}
                          querySpec={ref.querySpec as any}
                          widgetId={ref.id}
                          customColumns={ref.customColumns}
                          pivot={ref.pivot as any}
                        />
                      </ErrorBoundary>
                      {interactive && (
                        <div
                          className={`absolute right-0 top-0 h-full w-[6px] cursor-ew-resize ${activeIdx === idx ? 'bg-[hsl(var(--primary))]/40' : 'bg-transparent'} hover:bg-[hsl(var(--primary))]/60 rounded-r-sm`}
                          onMouseDown={(e) => startResize(idx, e)}
                          role="separator"
                          aria-orientation="horizontal"
                          aria-label="Resize"
                          title="Drag to resize"
                        />
                      )}
                    </div>
                  )
                }
                // fallback if ref not found
                return (
                  <div key={c.id || idx} className={`${spanClass} rounded border border-dashed p-2 text-[11px] text-muted-foreground`}>
                    {c.kind} refId not found
                  </div>
                )
              }
              // Placeholder when no refId is set
              return (
                <div key={c.id || idx} className={`${spanClass} rounded border border-dashed p-2 text-[11px] text-muted-foreground`}>
                  {c.kind} component placeholder
                </div>
              )
            })}
          </div>
        )}
      </div>
    </ErrorBoundary>
  )
}
