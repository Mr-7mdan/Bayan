"use client"

import React from 'react'
import dynamic from 'next/dynamic'

// ECharts client-only wrapper
const ReactECharts: any = dynamic(() => import('echarts-for-react').then(m => (m as any).default), { ssr: false }) as any

function isDark(): boolean {
  if (typeof window === 'undefined') return false
  return document.documentElement.classList.contains('dark')
}

function buildTooltipHtml(header: string, lines: Array<{ label: string; value: string; right?: string }>) {
  const dark = isDark()
  const bg = dark ? 'hsla(199, 98.5%, 8.1%, 0.9)' : 'rgba(255,255,255,0.95)'
  const fg = dark ? 'hsl(var(--foreground))' : '#0f172a'
  const border = dark ? '1px solid hsl(var(--border))' : '1px solid rgba(148,163,184,.35)'
  const rows = lines.map(l => (
    `<tr><td style="padding:2px 6px;opacity:.85;text-align:left;white-space:nowrap">${l.label}</td>`+
    `<td style="padding:2px 6px;text-align:right">${l.value}</td>`+
    (l.right?`<td style=\"padding:2px 6px;opacity:.75;text-align:right\">${l.right}</td>`:'')+`</tr>`
  )).join('')
  return `<div style="padding:6px 8px;border:${border};background:${bg};color:${fg};border-radius:6px;font-size:12px;line-height:1.1;font-variant-numeric:tabular-nums;">`+
         `<div style="font-weight:600;margin-bottom:6px;text-align:left">${header}</div>`+
         `<table style="border-collapse:separate;border-spacing:0 2px;min-width:240px"><tbody>${rows}</tbody></table>`+
         `</div>`
}

export interface SankeyNode {
  name: string
  itemStyle?: {
    color?: string
    borderColor?: string
  }
}

export interface SankeyLink {
  source: string
  target: string
  value: number
}

export interface SankeyChartProps {
  chartInstanceKey: string
  data: {
    nodes: SankeyNode[]
    links: SankeyLink[]
  }
  colors?: string[]
  valueFormatterAction: (n: number) => string
  orient?: 'horizontal' | 'vertical'
  nodeWidth?: number
  nodeGap?: number
  layoutIterations?: number
  showLabels?: boolean
  labelPosition?: 'left' | 'right' | 'top' | 'bottom'
  echartsRef?: React.RefObject<any>
}

export function renderSankey({
  chartInstanceKey,
  data,
  colors,
  valueFormatterAction,
  orient = 'horizontal',
  nodeWidth = 20,
  nodeGap = 8,
  layoutIterations = 32,
  showLabels = true,
  labelPosition = 'right',
  echartsRef
}: SankeyChartProps) {
  const dark = isDark()
  const textColor = dark ? '#e5e7eb' : '#0f172a'
  const shadowColor = dark ? 'rgba(0,0,0,0.5)' : 'rgba(255,255,255,0.8)'

  // Validate data structure to prevent ECharts errors
  const validNodes = Array.isArray(data?.nodes) ? data.nodes.filter(n => n && n.name) : []
  const nodeNames = new Set(validNodes.map(n => n.name))
  
  // Filter links to only include those with valid source and target nodes
  const validLinks = Array.isArray(data?.links) 
    ? data.links.filter(l => 
        l && 
        l.source && 
        l.target && 
        nodeNames.has(l.source) && 
        nodeNames.has(l.target) &&
        typeof l.value === 'number' &&
        !isNaN(l.value)
      )
    : []

  // If no valid data, return empty state
  if (validNodes.length === 0 || validLinks.length === 0) {
    console.warn('[Sankey] Invalid data filtered:', {
      originalNodes: data?.nodes?.length ?? 0,
      validNodes: validNodes.length,
      originalLinks: data?.links?.length ?? 0,
      validLinks: validLinks.length
    })
    return (
      <div className="absolute inset-0 flex items-center justify-center text-sm text-muted-foreground">
        No valid data for Sankey chart
      </div>
    )
  }
  
  // Log data validation results
  if (data?.nodes?.length !== validNodes.length || data?.links?.length !== validLinks.length) {
    console.log('[Sankey] Data validation:', {
      nodesFiltered: (data?.nodes?.length ?? 0) - validNodes.length,
      linksFiltered: (data?.links?.length ?? 0) - validLinks.length,
      validNodesCount: validNodes.length,
      validLinksCount: validLinks.length
    })
  }

  const option = {
    color: Array.isArray(colors) && colors.length ? colors : undefined,
    tooltip: {
      trigger: 'item',
      triggerOn: 'mousemove',
      backgroundColor: 'transparent',
      borderWidth: 0,
      extraCssText: 'box-shadow:none;padding:0;',
      formatter: (params: any) => {
        // Defensive check: params can be undefined during ECharts initialization
        if (!params) return ''
        
        try {
          if (params.dataType === 'edge') {
            // Link tooltip
            const source = String(params.data?.source ?? '')
            const target = String(params.data?.target ?? '')
            const value = Number(params.data?.value ?? 0)
            return buildTooltipHtml(`${source} → ${target}`, [
              { label: 'Value', value: valueFormatterAction(value) }
            ])
          } else {
            // Node tooltip
            const name = String(params.name ?? '')
            const value = Number(params.value ?? 0)
            return buildTooltipHtml(name, [
              { label: 'Total', value: valueFormatterAction(value) }
            ])
          }
        } catch (e) {
          // Fallback in case of any formatter error
          console.error('[Sankey] Tooltip formatter error:', e)
          return ''
        }
      }
    },
    series: [
      {
        type: 'sankey',
        layout: 'none',
        orient,
        emphasis: {
          focus: 'adjacency'
        },
        nodeAlign: 'justify',
        nodeWidth,
        nodeGap,
        layoutIterations,
        data: validNodes,
        links: validLinks,
        label: {
          show: showLabels,
          position: labelPosition,
          color: textColor,
          fontSize: 12,
          textShadowColor: shadowColor,
          textShadowBlur: 3,
          formatter: (params: any) => {
            if (!params) return ''
            return String(params.name ?? '')
          }
        },
        lineStyle: {
          color: 'gradient',
          curveness: 0.5,
          opacity: 0.3
        },
        itemStyle: {
          borderWidth: 1,
          borderColor: dark ? 'rgba(0,0,0,0.3)' : 'rgba(255,255,255,0.3)'
        }
      }
    ]
  }

  return (
    <div className="absolute inset-0">
      <ReactECharts
        ref={echartsRef}
        key={chartInstanceKey}
        option={option}
        style={{ height: '100%', width: '100%' }}
        notMerge={true}
        lazyUpdate={true}
        opts={{ renderer: 'svg' }}
      />
    </div>
  )
}
