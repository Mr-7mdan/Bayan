"use client"

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useModalFocus } from '@/hooks/useModalFocus'
import { Api, type DatasourceOut, type TablesOnlyResponse, type IntrospectResponse } from '@/lib/api'
import CustomQueryEditor from '@/components/builder/CustomQueryEditor'
import { Select, SelectItem } from '@tremor/react'
import { RiCloseLine, RiPlayFill, RiDownload2Line, RiFilterOffLine } from '@remixicon/react'
import { useAuth } from '@/components/providers/AuthProvider'
import { useQuery } from '@tanstack/react-query'

interface Props {
  open: boolean
  onClose: () => void
  datasource: DatasourceOut | null
}

export default function ExecuteSqlDialog({ open, onClose, datasource }: Props) {
  const { user } = useAuth()
  const [sql, setSql] = useState('')
  const [builderSql, setBuilderSql] = useState('')
  const [sourceSchema, setSourceSchema] = useState<string>('')
  const [sourceTable, setSourceTable] = useState<string>('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [columns, setColumns] = useState<string[]>([])
  const [rows, setRows] = useState<any[][]>([])
  // The grid used to render every returned row. A 30k x 44 result is ~1.4M
  // <td> nodes, which is what actually pins the tab's memory — the row array
  // itself is cheap. So page the DOM and keep the full set in JS for Excel.
  const [page, setPage] = useState(0)
  // A successful query returning 0 rows used to render the identical "Ready to
  // query" panel as a query that was never run, so the button looked dead.
  const [ranSql, setRanSql] = useState<string | null>(null)

  // Ensure Tremor Select popovers render above this modal
  useEffect(() => {
    if (typeof document === 'undefined') return
    const id = 'exec-sql-popover-zfix'
    if (!document.getElementById(id)) {
      const style = document.createElement('style')
      style.id = id
      style.innerHTML = `
        .tremor-Select-popover, .tremor-base__Select-popover, [role='listbox'] { z-index: 9999 !important; }
      `
      document.head.appendChild(style)
    }
  }, [])

  // Reset state on open
  useEffect(() => {
    if (open) {
      setSql('')
      setBuilderSql('')
      setSourceSchema('')
      setSourceTable('')
      setColumns([])
      setRows([])
      setPage(0)
      setRanSql(null)
      setError(null)
      setLoading(false)
    }
  }, [open, datasource])

  const isApiDs = ((datasource?.type || '').toLowerCase() === 'api')

  const tablesOnlyQ = useQuery<TablesOnlyResponse, Error>({
    queryKey: ['tables-only', datasource?.id],
    queryFn: () => Api.tablesOnly(datasource!.id),
    enabled: open && !!datasource?.id && !isApiDs,
    staleTime: 5 * 60 * 1000,
  })

  const introspectQ = useQuery<IntrospectResponse, Error>({
    queryKey: ['introspect', datasource?.id, sourceSchema, sourceTable],
    queryFn: () => Api.introspect(datasource!.id),
    enabled: open && !!datasource?.id && !isApiDs && !!sourceSchema && !!sourceTable,
  })

  const schemaNames = useMemo(() => (tablesOnlyQ.data?.schemas || []).map(s => s.name), [tablesOnlyQ.data])
  const tablesForSchema = useMemo(() => {
    const sch = (sourceSchema || '').trim()
    if (!tablesOnlyQ.data || !sch) return [] as string[]
    const found = tablesOnlyQ.data.schemas?.find((s) => s.name === sch)
    return (found?.tables || [])
  }, [tablesOnlyQ.data, sourceSchema])

  // Default schema logic
  useEffect(() => {
    if (!open || !datasource || isApiDs) return
    const names = schemaNames || []
    if (!names.length) return
    if (!sourceSchema || !names.includes(sourceSchema)) {
      let preferred = ''
      const dsType = String(datasource?.type || '').toLowerCase()
      if (dsType.includes('postgres') || dsType.includes('redshift')) preferred = names.includes('public') ? 'public' : ''
      else if (dsType.includes('sqlserver') || dsType.includes('mssql')) preferred = names.includes('dbo') ? 'dbo' : ''
      else if (dsType.includes('duckdb')) preferred = names.includes('main') ? 'main' : ''
      setSourceSchema(preferred || names[0])
    }
  }, [open, datasource, schemaNames, sourceSchema, isApiDs])

  const availableColumnMeta = useMemo(() => {
    const schName = (sourceSchema || '').trim()
    const tblName = (sourceTable || '').trim()
    const meta = introspectQ.data
    if (!meta || !tblName) return [] as Array<{ name: string; type?: string | null }>
    let cols: Array<{ name: string; type?: string | null }> = []
    for (const sch of meta.schemas || []) {
      if (schName && sch.name !== schName) continue
      for (const t of sch.tables || []) {
        if (t.name === tblName) {
          cols = t.columns || []
          break
        }
      }
      if (cols.length) break
    }
    return [...cols].sort((a, b) => a.name.localeCompare(b.name))
  }, [introspectQ.data, sourceSchema, sourceTable])

  const availableColumns = useMemo(
    () => availableColumnMeta.map((c) => c.name),
    [availableColumnMeta],
  )

  const handleExecute = async (queryToRun?: string) => {
    const finalSql = queryToRun || sql
    if (!datasource || !finalSql.trim()) return
    setLoading(true)
    setError(null)
    try {
      const res = await Api.query({ sql: finalSql, datasourceId: datasource.id })
      setColumns(res.columns || [])
      setRows(res.rows || [])
      setPage(0)
      setRanSql(finalSql)
    } catch (err: any) {
      setError(err.message || 'Failed to execute query')
      setRanSql(finalSql)
    } finally {
      setLoading(false)
    }
  }

  const handleDownloadExcel = async () => {
    if (!columns.length || !rows.length) return
    try {
      const ExcelJS: any = await import('exceljs')
      const workbook = new ExcelJS.Workbook()
      const sheet = workbook.addWorksheet('Results')

      // Add headers
      sheet.addRow(columns)
      // Add rows
      rows.forEach(row => sheet.addRow(row))

      const buffer = await workbook.xlsx.writeBuffer()
      const blob = new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `query_results_${new Date().getTime()}.xlsx`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } catch (err: any) {
      console.error("Excel download failed", err)
      setError('Failed to download Excel file')
    }
  }

  const PAGE_SIZE = 100
  const pageCount = Math.max(1, Math.ceil(rows.length / PAGE_SIZE))
  // Clamp rather than trust `page`: a new, shorter result can leave the index
  // past the end before the reset effect runs.
  const safePage = Math.min(page, pageCount - 1)
  const pageRows = useMemo(
    () => rows.slice(safePage * PAGE_SIZE, safePage * PAGE_SIZE + PAGE_SIZE),
    [rows, safePage],
  )
  const firstRow = rows.length ? safePage * PAGE_SIZE + 1 : 0
  const lastRow = Math.min(rows.length, (safePage + 1) * PAGE_SIZE)
  const scrollRef = useRef<HTMLDivElement | null>(null)
  useEffect(() => { scrollRef.current?.scrollTo({ top: 0 }) }, [safePage])

  const formatCell = (val: any) => {
    if (val === null || val === undefined) return ''
    if (typeof val === 'object') return JSON.stringify(val)
    return String(val)
  }

  const panelRef = useModalFocus<HTMLDivElement>(open, onClose)

  if (!open || typeof window === 'undefined') return null

  return createPortal(
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-black/50 backdrop-blur-[2px]" onClick={onClose}>
      <div ref={panelRef} role="dialog" aria-modal="true" aria-label="Execute SQL" className="w-[95vw] max-w-[1400px] h-[90vh] z-[70] bg-[hsl(var(--card))] border border-[hsl(var(--border))] rounded-2xl shadow-2xl flex flex-col overflow-hidden" onClick={(e) => e.stopPropagation()}>
        <div className="flex items-center justify-between px-5 py-4 border-b border-[hsl(var(--border))] bg-[hsl(var(--muted))]/20 flex-shrink-0">
          <div className="flex items-center gap-3">
            <div className="p-2 bg-[hsl(var(--primary))]/10 rounded-lg">
              <RiPlayFill className="h-5 w-5 text-[hsl(var(--primary))]" />
            </div>
            <div>
              <h2 className="text-base font-semibold leading-tight">Execute SQL</h2>
              <div className="flex items-center gap-1.5 mt-0.5">
                <span className="text-[11px] text-muted-foreground">{datasource?.type}</span>
                <span className="text-[10px] text-muted-foreground/50">•</span>
                <span className="text-[11px] font-medium text-foreground">{datasource?.name}</span>
              </div>
            </div>
          </div>
          <button onClick={onClose} aria-label="Close" className="p-2 rounded-xl hover:bg-[hsl(var(--muted))] text-muted-foreground transition-colors focus:outline-none focus:ring-2 focus:ring-[hsl(var(--ring))]">
            <RiCloseLine className="h-5 w-5" />
          </button>
        </div>

        <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
          {/* Top Half: Editor & Reference */}
          <div className="flex flex-col lg:h-[55%] border-b border-[hsl(var(--border))] bg-[hsl(var(--background))] overflow-hidden flex-shrink-0 relative">
            
            {/* Toolbar for Schema/Table */}
            {!isApiDs && (
              <div className="flex items-center px-4 py-2 border-b border-[hsl(var(--border))] bg-[hsl(var(--muted))]/10">
                <div className="flex items-center gap-4">
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-muted-foreground w-12">Schema</span>
                    <div className="w-48 rounded-lg border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                      [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                      [&_button]:rounded-lg [&_[role=combobox]]:rounded-lg">
                      <Select
                        value={sourceSchema}
                        onValueChange={setSourceSchema}
                        className="w-full h-8 min-h-0 rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                        placeholder={tablesOnlyQ.isFetching ? 'Loading...' : 'Select schema'}
                        disabled={tablesOnlyQ.isFetching}
                      >
                        {schemaNames.map((s) => (
                          <SelectItem key={s} value={s}>{s}</SelectItem>
                        ))}
                      </Select>
                    </div>
                  </div>
                  
                  <div className="flex items-center gap-2">
                    <span className="text-xs font-medium text-muted-foreground w-10">Table</span>
                    <div className="w-64 rounded-lg border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                      [&_*]:!border-0 [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!outline-none [&_*]:!shadow-none
                      [&_button]:rounded-lg [&_[role=combobox]]:rounded-lg">
                      <Select
                        value={sourceTable}
                        onValueChange={(v) => { setSourceTable(v); setSql(''); setBuilderSql('') }}
                        className="w-full h-8 min-h-0 rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                        placeholder={!sourceSchema ? 'Detecting schema…' : (tablesOnlyQ.isFetching ? 'Loading…' : 'Select table')}
                        disabled={!sourceSchema || tablesOnlyQ.isFetching}
                      >
                        {!tablesOnlyQ.isFetching && tablesForSchema.map((t) => (
                          <SelectItem key={t} value={t}>{t}</SelectItem>
                        ))}
                      </Select>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {isApiDs && (
              <div className="px-4 py-2 border-b border-[hsl(var(--border))] bg-[hsl(var(--muted))]/10 text-xs text-muted-foreground italic">
                Query builder not fully supported for API datasources.
              </div>
            )}

            {/* Editor Area */}
            <div className="flex-1 p-4 flex flex-col min-h-0 overflow-hidden relative bg-[hsl(var(--muted))]/5">
              <CustomQueryEditor
                value={sql}
                onChange={setSql}
                onPreviewChange={setBuilderSql}
                columns={availableColumns}
                columnMeta={availableColumnMeta}
                sourceTable={sourceTable}
                sourceSchema={sourceSchema}
                dialect={datasource?.type}
                className="flex-1 min-h-0 flex flex-col shadow-sm border border-[hsl(var(--border))] rounded-xl bg-[hsl(var(--card))]"
              />
            </div>
          </div>

          {/* Bottom Half: Results */}
          <div className="flex-1 flex flex-col min-h-0 bg-[hsl(var(--card))]">
            <div className="flex items-center justify-between px-5 py-3 border-b border-[hsl(var(--border))] flex-shrink-0">
              <div className="text-sm font-semibold flex items-center gap-3">
                Results
                {rows.length > 0 && <span className="text-[11px] font-medium text-muted-foreground px-2 py-0.5 bg-[hsl(var(--muted))] rounded-full border border-[hsl(var(--border))]">{rows.length} rows</span>}
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => {
                     const finalSql = sql.trim() || builderSql.trim()
                     if (finalSql) {
                       setSql(finalSql)
                       handleExecute(finalSql)
                     }
                  }}
                  disabled={loading || (!sql.trim() && !builderSql.trim())}
                  className="flex items-center gap-1.5 px-4 py-1.5 bg-[hsl(var(--primary))] text-[hsl(var(--primary-foreground))] rounded-lg text-xs font-semibold shadow-sm hover:shadow-md disabled:opacity-50 transition-all"
                >
                  {loading ? <span className="animate-spin inline-block w-3.5 h-3.5 rounded-full border-[2px] border-[hsl(var(--primary-foreground))] border-t-transparent" /> : <RiPlayFill className="h-3.5 w-3.5" />}
                  Run Query
                </button>
                {rows.length > 0 && (
                  <button
                    onClick={handleDownloadExcel}
                    className="flex items-center gap-1.5 text-xs px-3 py-1.5 border border-[hsl(var(--border))] rounded-lg hover:bg-[hsl(var(--muted))] transition-colors font-medium bg-[hsl(var(--background))] shadow-sm"
                  >
                    <RiDownload2Line className="h-3.5 w-3.5" />
                    Download Excel{rows.length > PAGE_SIZE ? ` (${rows.length.toLocaleString()} rows)` : ''}
                  </button>
                )}
              </div>
            </div>
            
            <div className="flex-1 overflow-auto p-4 relative">
              {error && (
                <div className="max-w-3xl mx-auto mt-4 p-4 text-sm text-red-600 bg-red-50 dark:bg-red-500/10 border border-red-200 dark:border-red-500/20 rounded-xl shadow-sm flex items-start gap-3">
                  <span className="text-red-500 mt-0.5">⚠️</span>
                  <div className="flex-1 leading-relaxed font-mono text-[13px] whitespace-pre-wrap">{error}</div>
                </div>
              )}
              
              {!error && !loading && rows.length === 0 && !ranSql && (
                <div className="flex flex-col items-center justify-center h-full text-center">
                  <div className="h-14 w-14 rounded-2xl bg-[hsl(var(--muted))]/50 border border-[hsl(var(--border))] flex items-center justify-center mb-4">
                    <RiPlayFill className="h-7 w-7 text-muted-foreground/60 ml-1" />
                  </div>
                  <h3 className="text-sm font-semibold text-foreground">Ready to query</h3>
                  <p className="text-xs text-muted-foreground mt-1.5 max-w-sm leading-relaxed">Write a SQL query or use the visual builder, then click Run Query to see results here.</p>
                </div>
              )}

              {!error && !loading && rows.length === 0 && ranSql && (
                <div className="flex flex-col items-center justify-center h-full text-center px-6">
                  <div className="h-14 w-14 rounded-2xl bg-amber-500/10 border border-amber-500/25 flex items-center justify-center mb-4">
                    <RiFilterOffLine className="h-7 w-7 text-amber-600 dark:text-amber-500" />
                  </div>
                  <h3 className="text-sm font-semibold text-foreground">Query ran — 0 rows returned</h3>
                  <p className="text-xs text-muted-foreground mt-1.5 max-w-md leading-relaxed">
                    The query executed without error but matched nothing. Widen or remove the WHERE filters and run again.
                  </p>
                  <pre className="mt-4 max-w-full overflow-x-auto text-start text-[11px] font-mono leading-relaxed px-3 py-2 rounded-lg bg-[hsl(var(--muted))]/50 border border-[hsl(var(--border))] text-muted-foreground whitespace-pre-wrap">{ranSql}</pre>
                </div>
              )}
              
              {loading && (
                <div className="flex flex-col items-center justify-center h-full text-center">
                  <span className="animate-spin inline-block w-8 h-8 rounded-full border-[3px] border-[hsl(var(--primary))] border-t-transparent mb-4" />
                  <p className="text-sm font-medium text-muted-foreground animate-pulse">Executing query...</p>
                </div>
              )}
              
              {!loading && !error && rows.length > 0 && (
                <div className="border border-[hsl(var(--border))] rounded-xl bg-[hsl(var(--background))] overflow-hidden absolute inset-4 shadow-sm flex flex-col">
                  <div ref={scrollRef} className="flex-1 overflow-auto">
                    <table className="min-w-full text-[13px] text-start border-collapse">
                      <thead className="bg-[hsl(var(--muted))]/80 sticky top-0 z-10 backdrop-blur-md">
                        <tr>
                          {columns.map((c, i) => (
                            <th key={i} className="font-semibold px-4 py-2.5 border-b border-[hsl(var(--border))] whitespace-nowrap text-foreground shadow-sm">{c}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-[hsl(var(--border))]">
                        {pageRows.map((r, i) => (
                          <tr key={i} className="hover:bg-[hsl(var(--muted))]/50 transition-colors group">
                            {r.map((cell, j) => (
                              <td key={j} className="px-4 py-2 whitespace-nowrap max-w-[300px] truncate text-[hsl(var(--foreground))]/90 group-hover:text-foreground transition-colors" title={formatCell(cell)}>
                                {formatCell(cell)}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  <div className="flex flex-wrap items-center justify-between gap-2 border-t border-[hsl(var(--border))] bg-[hsl(var(--muted))]/20 px-4 py-2 flex-shrink-0">
                    <span className="text-[11px] text-muted-foreground tabular-nums">
                      Showing {firstRow.toLocaleString()}–{lastRow.toLocaleString()} of {rows.length.toLocaleString()} rows
                      {rows.length > PAGE_SIZE && <> · use Download Excel for the full set</>}
                    </span>
                    {pageCount > 1 && (
                      <div className="flex items-center gap-1">
                        <button
                          onClick={() => setPage(0)}
                          disabled={safePage === 0}
                          className="px-2 py-1 rounded-md border border-[hsl(var(--border))] text-[11px] disabled:opacity-40 disabled:cursor-not-allowed hover:bg-[hsl(var(--muted))] transition-colors"
                        >First</button>
                        <button
                          onClick={() => setPage(Math.max(0, safePage - 1))}
                          disabled={safePage === 0}
                          className="px-2 py-1 rounded-md border border-[hsl(var(--border))] text-[11px] disabled:opacity-40 disabled:cursor-not-allowed hover:bg-[hsl(var(--muted))] transition-colors"
                        >Prev</button>
                        <span className="px-2 text-[11px] text-muted-foreground tabular-nums">
                          Page {(safePage + 1).toLocaleString()} / {pageCount.toLocaleString()}
                        </span>
                        <button
                          onClick={() => setPage(Math.min(pageCount - 1, safePage + 1))}
                          disabled={safePage >= pageCount - 1}
                          className="px-2 py-1 rounded-md border border-[hsl(var(--border))] text-[11px] disabled:opacity-40 disabled:cursor-not-allowed hover:bg-[hsl(var(--muted))] transition-colors"
                        >Next</button>
                        <button
                          onClick={() => setPage(pageCount - 1)}
                          disabled={safePage >= pageCount - 1}
                          className="px-2 py-1 rounded-md border border-[hsl(var(--border))] text-[11px] disabled:opacity-40 disabled:cursor-not-allowed hover:bg-[hsl(var(--muted))] transition-colors"
                        >Last</button>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>,
    document.body
  )
}
