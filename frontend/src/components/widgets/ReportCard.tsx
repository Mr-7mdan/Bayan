"use client"

import { useEffect, useMemo, useRef, useState } from 'react'
import { useQueries } from '@tanstack/react-query'
import { QueryApi } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'
import { useFilters } from '@/components/providers/FiltersProvider'
import type { WidgetConfig, ReportElement, ReportVariable, ReportTableCell } from '@/types/widgets'
import ErrorBoundary from '@/components/dev/ErrorBoundary'

// Format date using a pattern like "dd MMM yyyy", "dd/MM/yyyy", "MMM yyyy", "HH:mm"
function formatDateStr(date: Date, pattern: string): string {
  const months = ['January','February','March','April','May','June','July','August','September','October','November','December']
  const monthsShort = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
  const days = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']
  const daysShort = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat']
  const pad = (n: number, len = 2) => String(n).padStart(len, '0')

  let result = pattern
  result = result.replace(/yyyy/g, String(date.getFullYear()))
  result = result.replace(/yy/g, String(date.getFullYear()).slice(-2))
  result = result.replace(/MMMM/g, months[date.getMonth()])
  result = result.replace(/MMM/g, monthsShort[date.getMonth()])
  result = result.replace(/MM/g, pad(date.getMonth() + 1))
  result = result.replace(/dddd/g, days[date.getDay()])
  result = result.replace(/ddd/g, daysShort[date.getDay()])
  result = result.replace(/dd/g, pad(date.getDate()))
  result = result.replace(/HH/g, pad(date.getHours()))
  result = result.replace(/mm/g, pad(date.getMinutes()))
  result = result.replace(/ss/g, pad(date.getSeconds()))
  return result
}

// Format a resolved numeric value according to the variable's format setting
function formatValue(raw: unknown, variable: ReportVariable): string {
  // Handle datetime variables
  if (variable.type === 'datetime') {
    if (typeof raw === 'string' || raw instanceof Date) {
      const date = raw instanceof Date ? raw : new Date(raw)
      if (!isNaN(date.getTime())) {
        const fmt = variable.dateFormat || 'dd MMM yyyy'
        return `${variable.prefix || ''}${formatDateStr(date, fmt)}${variable.suffix || ''}`
      }
    }
    return String(raw ?? '')
  }

  const num = Number(raw)
  const prefix = variable.prefix || ''
  const suffix = variable.suffix || ''
  if (raw == null || raw === '' || isNaN(num)) return `${prefix}${String(raw ?? '—')}${suffix}`
  const fmt = variable.format || 'none'
  let str: string
  switch (fmt) {
    case 'short': str = num >= 1e9 ? `${(num/1e9).toFixed(1)}B` : num >= 1e6 ? `${(num/1e6).toFixed(1)}M` : num >= 1e3 ? `${(num/1e3).toFixed(1)}K` : num.toFixed(0); break
    case 'currency': str = num.toLocaleString(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }); break
    case 'percent': str = `${(num * 100).toFixed(1)}%`; break
    case 'wholeNumber': str = Math.round(num).toLocaleString(); break
    case 'oneDecimal': str = num.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 }); break
    case 'twoDecimals': str = num.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }); break
    default: str = String(raw)
  }
  return `${prefix}${str}${suffix}`
}

// Build query options for a single variable (used by useQueries)
// Uses the same query path as KPI/Chart widgets: y + agg (no x → no GROUP BY)
function buildVarQueryOptions(variable: ReportVariable, globalFilters: Record<string, any>) {
  return {
    queryKey: ['report-var', variable.id, variable.datasourceId, variable.source, variable.value?.field, variable.value?.agg, JSON.stringify(variable.where), JSON.stringify(globalFilters)],
    queryFn: async () => {
      if (!variable.source || !variable.value?.field) return null
      const agg = variable.value.agg && variable.value.agg !== 'none' ? variable.value.agg : null
      const field = variable.value.field
      const where = { ...(variable.where || {}), ...(globalFilters || {}) }
      const hasWhere = Object.keys(where).length > 0

      if (agg) {
        // Same pattern as KpiCard: set y + agg, NO x (avoids GROUP BY)
        const spec: any = {
          source: variable.source,
          agg,
          y: field,
          where: hasWhere ? where : undefined,
        }
        const r = await QueryApi.querySpec({ spec, datasourceId: variable.datasourceId, limit: 1000, offset: 0, includeTotal: false })
        if (!r?.rows?.length) return null
        // Backend returns columns like ['value'] or [fieldName] for a simple aggregate
        const cols = (r.columns || []) as string[]
        const rows = r.rows as any[]
        // Sum all rows in case backend returns multiple (shouldn't for no-x aggregate)
        const valIdx = cols.includes('value') ? cols.indexOf('value') : (cols.length > 1 ? cols.length - 1 : 0)
        let total = 0
        for (const row of rows) {
          const v = Array.isArray(row) ? Number(row[valIdx] ?? 0) : Number(row ?? 0)
          if (!isNaN(v)) total += v
        }
        return total
      } else {
        // No aggregation: fetch raw first row value
        const spec: any = {
          source: variable.source,
          select: [field],
          where: hasWhere ? where : undefined,
          limit: 1,
          offset: 0,
        }
        const r = await QueryApi.querySpec({ spec, datasourceId: variable.datasourceId, limit: 1, offset: 0, includeTotal: false })
        if (!r?.rows?.length) return null
        return r.rows[0]?.[0] ?? null
      }
    },
    enabled: !!(variable.source && variable.value?.field),
    staleTime: 30_000,
  }
}

// Render a single report element
function ReportElementView({ element, variables, resolvedValues }: {
  element: ReportElement
  variables: ReportVariable[]
  resolvedValues: Record<string, { value: unknown; loading: boolean }>
}) {
  const resolveText = (text: string): string => {
    return text.replace(/\{\{(\w+)\}\}/g, (_, name) => {
      const v = variables.find(v => v.name === name)
      if (!v) return `{{${name}}}`
      const rv = resolvedValues[v.id]
      if (!rv) return `{{${name}}}`
      if (rv.loading) return '…'
      return formatValue(rv.value, v)
    })
  }

  if (element.type === 'label') {
    const lbl = element.label
    if (!lbl) return null
    return (
      <div
        className="h-full w-full overflow-hidden"
        style={{
          backgroundColor: lbl.backgroundColor || undefined,
          textAlign: lbl.align || 'left',
          display: 'flex',
          alignItems: lbl.verticalAlign === 'bottom' ? 'flex-end' : lbl.verticalAlign === 'middle' ? 'center' : 'flex-start',
          border: lbl.borderStyle && lbl.borderStyle !== 'none' ? `1px ${lbl.borderStyle} ${lbl.borderColor || 'hsl(var(--border))'}` : undefined,
          padding: lbl.padding != null ? `${lbl.padding}px` : '4px',
        }}
      >
        <span 
          className="w-full"
          style={{
            fontSize: lbl.fontSize ? `${lbl.fontSize}px` : undefined,
            fontWeight: lbl.fontWeight === 'bold' ? 700 : lbl.fontWeight === 'semibold' ? 600 : 400,
            fontStyle: lbl.fontStyle === 'italic' ? 'italic' : undefined,
            color: lbl.color || undefined,
          }}
        >{resolveText(lbl.text)}</span>
      </div>
    )
  }

  if (element.type === 'image') {
    const img = element.image
    if (!img?.url) return <div className="h-full w-full flex items-center justify-center text-muted-foreground text-xs">No image URL</div>
    return (
      <div className="h-full w-full overflow-hidden">
        <img
          src={img.url}
          alt={img.alt || ''}
          className="w-full h-full"
          style={{
            objectFit: img.objectFit || 'contain',
          }}
        />
      </div>
    )
  }

  if (element.type === 'spaceholder') {
    const v = variables.find(v => v.id === element.variableId)
    if (!v) return <div className="h-full w-full flex items-center justify-center text-muted-foreground text-xs">No variable</div>
    const rv = resolvedValues[v.id]
    if (!rv || rv.loading) return <div className="h-full w-full flex items-center justify-center text-muted-foreground text-xs animate-pulse">Loading…</div>
    return (
      <div className="h-full w-full flex items-center justify-center text-lg font-semibold">
        {formatValue(rv.value, v)}
      </div>
    )
  }

  if (element.type === 'table') {
    const tbl = element.table
    if (!tbl) return null

    // Compute which headers should merge with subheaders (rowSpan=2)
    // A header merges if mergeBlankSubheaders is on and ALL subheader cells under it are blank
    const mergeMap: boolean[] = [] // per-header: true = rowSpan=2 (merged)
    if (tbl.subheaders && tbl.mergeBlankSubheaders) {
      let subIdx = 0
      for (const h of tbl.headers) {
        const header = typeof h === 'string' ? { text: h, colspan: 1 } : h
        const span = header.colspan || 1
        let allBlank = true
        for (let k = 0; k < span; k++) {
          if (tbl.subheaders[subIdx + k] && tbl.subheaders[subIdx + k].trim() !== '') {
            allBlank = false
            break
          }
        }
        mergeMap.push(allBlank)
        subIdx += span
      }
    }
    const hasSubRow = tbl.subheaders && tbl.subheaders.length > 0 && (!tbl.mergeBlankSubheaders || mergeMap.some(m => !m))
    // Build subheader cells to render (skip cells under merged headers)
    const subCells: { text: string; colIdx: number }[] = []
    if (hasSubRow && tbl.subheaders) {
      let subIdx = 0
      for (let hi = 0; hi < tbl.headers.length; hi++) {
        const rawH = tbl.headers[hi]
        const header = typeof rawH === 'string' ? { text: rawH, colspan: 1 } : rawH
        const span = header.colspan || 1
        if (!mergeMap[hi]) {
          for (let k = 0; k < span; k++) {
            subCells.push({ text: tbl.subheaders[subIdx + k] || '', colIdx: subIdx + k })
          }
        }
        subIdx += span
      }
    }

    const bStyle = tbl.borderStyle || 'solid'
    const bWidth = bStyle === 'none' ? 0 : '1px'
    const bColor = tbl.borderColor || 'hsl(var(--border))'

    return (
      <div className="h-full w-full overflow-auto">
        <table
          className="w-full border-collapse"
          style={{
            borderStyle: bStyle,
            borderWidth: bWidth,
            borderColor: bColor,
            borderRadius: tbl.borderRadius ? `${tbl.borderRadius}px` : undefined,
            overflow: tbl.borderRadius ? 'hidden' : undefined,
          }}
        >
          {tbl.colWidths && tbl.colWidths.some(w => w > 0) && (
            <colgroup>
              {tbl.colWidths.map((w, i) => (
                <col key={i} style={{ width: w > 0 ? `${w}%` : undefined }} />
              ))}
            </colgroup>
          )}
          {tbl.headers.length > 0 && (
            <thead>
              <tr style={{ height: tbl.headerHeight ? `${tbl.headerHeight}px` : undefined }}>
                {tbl.headers.map((h, i) => {
                  const header = typeof h === 'string' ? { text: h } : h
                  const merged = mergeMap[i]
                  return (
                    <th
                      key={i}
                      colSpan={header.colspan || 1}
                      rowSpan={merged && hasSubRow ? 2 : undefined}
                      className="px-2 py-1"
                      style={{
                        backgroundColor: tbl.headerBg || 'hsl(var(--secondary))',
                        color: tbl.headerColor || 'hsl(var(--foreground))',
                        fontSize: tbl.headerFontSize ? `${tbl.headerFontSize}px` : '12px',
                        fontWeight: tbl.headerFontWeight === 'semibold' ? 600 : tbl.headerFontWeight === 'bold' || !tbl.headerFontWeight ? 700 : 400,
                        textAlign: tbl.headerAlign || 'left',
                        borderStyle: bStyle,
                        borderWidth: bWidth,
                        borderColor: bColor,
                        verticalAlign: 'middle',
                      }}
                    >
                      {resolveText(header.text)}
                    </th>
                  )
                })}
              </tr>
              {hasSubRow && (
                <tr>
                  {subCells.map((sc) => (
                    <th
                      key={sc.colIdx}
                      className="px-2 py-1"
                      style={{
                        backgroundColor: tbl.subheaderBg || tbl.headerBg || 'hsl(var(--secondary))',
                        color: tbl.subheaderColor || tbl.headerColor || 'hsl(var(--foreground))',
                        fontSize: tbl.subheaderFontSize ? `${tbl.subheaderFontSize}px` : '11px',
                        fontWeight: tbl.subheaderFontWeight === 'bold' ? 700 : tbl.subheaderFontWeight === 'semibold' ? 600 : 400,
                        textAlign: tbl.subheaderAlign || tbl.headerAlign || 'left',
                        borderStyle: bStyle,
                        borderWidth: bWidth,
                        borderColor: bColor,
                        width: tbl.colWidths?.[sc.colIdx] ? `${tbl.colWidths[sc.colIdx]}%` : undefined,
                      }}
                    >
                      {resolveText(sc.text)}
                    </th>
                  ))}
                </tr>
              )}
            </thead>
          )}
          <tbody>
            {(tbl.cells || []).map((row, ri) => {
              const rs = tbl.rowStyles?.[ri]
              const rowBg = rs?.bg || (tbl.stripedRows && ri % 2 === 1 ? 'hsl(var(--secondary)/0.4)' : undefined)
              return (
                <tr
                  key={ri}
                  style={{
                    backgroundColor: rowBg,
                    height: tbl.rowHeights?.[ri] ? `${tbl.rowHeights[ri]}px` : undefined,
                  }}
                >
                  {row.map((cell, ci) => (
                    <td
                      key={ci}
                      className="px-2 py-1"
                      style={{
                        fontSize: rs?.fontSize ? `${rs.fontSize}px` : cell.style?.fontSize ? `${cell.style.fontSize}px` : '12px',
                        fontWeight: rs?.fontWeight === 'bold' ? 700 : rs?.fontWeight === 'semibold' ? 600 : cell.style?.fontWeight === 'bold' ? 700 : cell.style?.fontWeight === 'semibold' ? 600 : 400,
                        fontStyle: cell.style?.fontStyle === 'italic' ? 'italic' : undefined,
                        color: rs?.color || cell.style?.color || undefined,
                        backgroundColor: cell.style?.backgroundColor || undefined,
                        textAlign: cell.style?.align || 'left',
                        verticalAlign: cell.style?.verticalAlign || 'middle',
                        borderStyle: bStyle,
                        borderWidth: bWidth,
                        borderColor: bColor,
                      }}
                    >
                      {cell.type === 'spaceholder'
                        ? (() => {
                            const v = variables.find(v => v.id === cell.variableId)
                            if (!v) return <span className="text-muted-foreground">—</span>
                            const rv = resolvedValues[v.id]
                            if (!rv || rv.loading) return <span className="text-muted-foreground animate-pulse">…</span>
                            return formatValue(rv.value, v)
                          })()
                        : resolveText(cell.text || '')
                      }
                    </td>
                  ))}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    )
  }

  return null
}

// Main ReportCard component
export default function ReportCard({
  title,
  options,
  widgetId,
  datasourceId,
}: {
  title: string
  options?: WidgetConfig['options']
  widgetId?: string
  datasourceId?: string
}) {
  const { user } = useAuth()
  const { filters } = useFilters()
  const report = options?.report

  const elements = report?.elements || []
  const variables = report?.variables || []

  // Resolve query-based variables using useQueries (stable hook count)
  const queryVars = useMemo(() => variables.filter(v => v.type !== 'expression' && v.type !== 'datetime'), [variables])
  const queryResults = useQueries({
    queries: queryVars.map(v => buildVarQueryOptions(v, filters || {})),
  })

  // Build resolved values map
  const resolvedValues = useMemo(() => {
    const rv: Record<string, { value: unknown; loading: boolean }> = {}

    // Query-based variables
    queryVars.forEach((v, i) => {
      const q = queryResults[i]
      rv[v.id] = { value: q?.data ?? null, loading: q?.isLoading ?? false }
    })

    // Datetime variables (client-side)
    for (const v of variables) {
      if (v.type !== 'datetime') continue
      const now = new Date()
      if (v.datetimeExpr === 'today') {
        const today = new Date(now.getFullYear(), now.getMonth(), now.getDate())
        rv[v.id] = { value: today.toISOString(), loading: false }
      } else {
        rv[v.id] = { value: now.toISOString(), loading: false }
      }
    }

    // Expression-based variables
    for (const v of variables) {
      if (v.type !== 'expression' || !v.expression) continue
      try {
        let expr = v.expression
        let allResolved = true
        for (const refVar of variables) {
          if (refVar.id === v.id) continue
          const ref = rv[refVar.id]
          if (!ref || ref.loading) { allResolved = false; break }
          const val = typeof ref.value === 'number' ? ref.value : parseFloat(String(ref.value || 0))
          expr = expr.replace(new RegExp(`\\b${refVar.name}\\b`, 'g'), String(val))
        }
        if (allResolved) {
          const result = Function('"use strict"; return (' + expr + ')')() as number
          rv[v.id] = { value: result, loading: false }
        } else {
          rv[v.id] = { value: null, loading: true }
        }
      } catch (err) {
        console.error(`Expression evaluation error for ${v.name}:`, err)
        rv[v.id] = { value: null, loading: false }
      }
    }

    return rv
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [queryVars, queryResults, variables])

  if (!report) return <div className="h-full w-full flex items-center justify-center text-muted-foreground text-sm">No report configured. Click the gear icon to open the report builder.</div>

  const gridCols = report.gridCols || 12
  const gridRows = report.gridRows || 20
  const cellSize = report.cellSize || 30

  return (
    <div className="h-full w-full overflow-auto">
      <div
        className="relative"
        style={{
          width: `${gridCols * cellSize}px`,
          height: `${gridRows * cellSize}px`,
        }}
      >
        {elements.map((el) => (
          <div
            key={el.id}
            className="absolute"
            style={{
              left: `${el.gridX * cellSize}px`,
              top: `${el.gridY * cellSize}px`,
              width: `${el.gridW * cellSize}px`,
              height: `${el.gridH * cellSize}px`,
            }}
          >
            <ErrorBoundary name={`report-el-${el.id}`}>
              <ReportElementView
                element={el}
                variables={variables}
                resolvedValues={resolvedValues}
              />
            </ErrorBoundary>
          </div>
        ))}
      </div>
    </div>
  )
}
