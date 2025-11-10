"use client"

import React, { ComponentType } from 'react'
import dynamic from 'next/dynamic'

// Local dynamic import to keep client rendering consistent
const ReactECharts: any = dynamic(() => import('echarts-for-react').then(m => (m as any).default), { ssr: false }) as ComponentType<any>

export type AreaAdvancedArgs = {
  chartInstanceKey: any
  // Prebuilt ECharts series from ChartCard advanced path
  seriesList: any[]
  // Pre-formatted X labels for category mode
  xLabelsFmt: string[]
  // Raw X labels as provided by data (for mapping zoom indices back to values)
  xLabelsRaw?: any[]
  xRotate: number
  xInterval: number
  fontSize: number
  advTooltip: any
  axisTextColor: string
  buildAxisGridAction: (axis: 'x'|'y') => any
  options: any
  gridTopPad: number
  gridBottomPad: number
  hasSecondaryY: boolean
  visualMap?: any
  onZoomAction?: (args: { startIndex: number; endIndex: number; startVal: any; endVal: any }) => void
  noAnim?: boolean
  onReadyAction?: () => void
  echartsRef?: React.RefObject<any>
}

// Helper to possibly augment series for area-specific options
function prepareAreaSeries(seriesIn: any[], opts: any) {
  const stacked = !!(opts?.areaStacked)
  const largeScale = !!(opts?.areaLargeScale || opts?.largeScale)
  return (seriesIn || []).map((s) => {
    const next = { ...s }
    // Ensure line type + smooth + areaStyle
    next.type = 'line'
    next.smooth = true
    // Preserve existing areaStyle if present; otherwise add a mild opacity
    if (!next.areaStyle) next.areaStyle = { opacity: 0.2 }
    // Stacking
    if (stacked && !next.stack) next.stack = 'stack1'
    // Large scale sampling (lttb works well for time series)
    if (largeScale) {
      next.sampling = 'lttb'
      next.progressive = 20000
      next.progressiveThreshold = 3000
    }
    return next
  })
}

export function renderAdvancedAreaChart(args: AreaAdvancedArgs) {
  const {
    chartInstanceKey,
    seriesList,
    xLabelsFmt,
    xLabelsRaw,
    xRotate,
    xInterval,
    fontSize,
    advTooltip,
    axisTextColor,
    buildAxisGridAction,
    options,
    gridTopPad,
    gridBottomPad,
    hasSecondaryY,
    visualMap,
    onZoomAction,
    noAnim,
    onReadyAction,
    echartsRef,
  } = args

  const xAxis: any = {
    type: 'category',
    data: xLabelsFmt,
    axisLabel: {
      rotate: xRotate,
      interval: (xInterval as any),
      fontSize: ((options as any)?.xAxisFontSize ?? fontSize),
      fontWeight: (((options as any)?.xAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal',
      margin: Math.abs(xRotate) >= 60 ? 12 : 8,
      color: ((options as any)?.xAxisFontColor || axisTextColor),
    },
    ...(buildAxisGridAction('x') as any),
  }

  const yAxis: any = hasSecondaryY
    ? [
        { type: 'value', splitNumber: (options as any)?.yTickCount || undefined, axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) }, ...(buildAxisGridAction('y') as any) },
        { type: 'value', splitNumber: (options as any)?.yTickCount || undefined, axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) }, position: 'right', ...(buildAxisGridAction('y') as any) },
      ]
    : { type: 'value', splitNumber: (options as any)?.yTickCount || undefined, axisLabel: { fontSize: ((options as any)?.yAxisFontSize ?? fontSize), fontWeight: (((options as any)?.yAxisFontWeight || 'normal') === 'bold') ? 'bold' : 'normal', color: ((options as any)?.yAxisFontColor || axisTextColor) }, ...(buildAxisGridAction('y') as any) }

  const dataZoom = (() => {
    const enabled = !!((options as any)?.areaZoomPan || (options as any)?.zoomPan || (options as any)?.dataZoom)
    if (!enabled) return undefined
    const len = Array.isArray(xLabelsFmt) ? xLabelsFmt.length : 0
    const pctFromPoints = (points: number) => {
      if (!len || points <= 0) return 100
      return Math.min(100, Math.max(0, (points / len) * 100))
    }
    return [
      {
        type: 'inside',
        xAxisIndex: 0,
        filterMode: 'filter',
        zoomOnMouseWheel: true,
        moveOnMouseWheel: true,
        moveOnMouseMove: true,
        throttle: 50,
      },
      {
        type: 'slider',
        xAxisIndex: 0,
        bottom: 10,
        height: 26,
        handleSize: 12,
        showDetail: false,
        filterMode: 'filter',
      },
    ]
  })()

  const series = prepareAreaSeries(seriesList, (options as any)).map((s: any) => ({
    ...s,
    animation: !(noAnim === true),
    animationDuration: (noAnim === true) ? 0 : (s?.animationDuration ?? undefined),
    animationDurationUpdate: (noAnim === true) ? 0 : (s?.animationDurationUpdate ?? undefined),
    progressive: (noAnim === true) ? 0 : (s?.progressive ?? undefined),
    progressiveThreshold: (noAnim === true) ? 0 : (s?.progressiveThreshold ?? undefined),
  }))

  const option = {
    backgroundColor: 'rgba(0,0,0,0)',
    animation: !(noAnim === true),
    animationDuration: (noAnim === true) ? 0 : 200,
    animationDurationUpdate: (noAnim === true) ? 0 : 300,
    tooltip: advTooltip,
    legend: { show: false },
    grid: { left: 40, right: 16, top: 10 + gridTopPad, bottom: 24 + gridBottomPad + (dataZoom ? 48 : 0), containLabel: true },
    xAxis,
    yAxis,
    series,
    ...(dataZoom ? { dataZoom } : {}),
    ...(visualMap ? { visualMap } : {}),
  }

  const onEvents: any = {
    finished: () => { try { onReadyAction && onReadyAction() } catch {} },
    rendered: () => { try { onReadyAction && onReadyAction() } catch {} },
  }
  if (onZoomAction && dataZoom) {
    onEvents.dataZoom = (ev: any) => {
      try {
        const labels = (Array.isArray(xLabelsRaw) && (xLabelsRaw as any[]).length) ? (xLabelsRaw as any[]) : (xLabelsFmt as any[])
        const total = labels.length
        if (!total) return
        const b = (Array.isArray(ev?.batch) && ev.batch.length ? ev.batch[0] : ev) || {}
        let si = 0, ei = Math.max(0, total - 1)
        if (typeof b.startValue !== 'undefined' || typeof b.endValue !== 'undefined') {
          si = Math.max(0, Math.min(total - 1, Number(b.startValue ?? 0)))
          ei = Math.max(0, Math.min(total - 1, Number(b.endValue ?? (total - 1))))
        } else {
          const sp = Math.max(0, Math.min(100, Number(b.start ?? 0)))
          const ep = Math.max(0, Math.min(100, Number(b.end ?? 100)))
          si = Math.round((sp / 100) * (total - 1))
          ei = Math.round((ep / 100) * (total - 1))
        }
        const sv = labels[si]
        const evv = labels[ei]
        onZoomAction({ startIndex: si, endIndex: ei, startVal: sv, endVal: evv })
      } catch {}
    }
  }

  return (
    <div className="absolute inset-0">
      <ReactECharts
        ref={echartsRef}
        key={chartInstanceKey}
        option={option}
        notMerge={true}
        lazyUpdate
        style={{ height: '100%' }}
        opts={noAnim ? ({ renderer: 'svg' } as any) : undefined}
        onChartReady={() => { try { requestAnimationFrame(() => requestAnimationFrame(() => { try { onReadyAction && onReadyAction() } catch {} })) } catch {} }}
        onEvents={onEvents}
      />
    </div>
  )
}
