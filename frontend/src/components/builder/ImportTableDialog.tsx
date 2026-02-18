"use client"

import React, { useCallback, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { Api } from '@/lib/api'
import { RiUpload2Line, RiFileLine, RiCloseLine, RiCheckLine, RiErrorWarningLine, RiTableLine } from '@remixicon/react'

type Step = 'input' | 'preview' | 'done'
type Mode = 'sql' | 'file'
type IfExists = 'replace' | 'append' | 'fail'

interface PreviewResult {
  columns: string[]
  rows: Record<string, unknown>[]
  rowCount: number
  fileName?: string
}

interface Props {
  open: boolean
  dsId: string
  onCloseAction: () => void
  onImported: (tableName: string, rowCount: number) => void
}

export default function ImportTableDialog({ open, dsId, onCloseAction, onImported }: Props) {
  const [step, setStep] = useState<Step>('input')
  const [mode, setMode] = useState<Mode>('file')
  const [sql, setSql] = useState('')
  const [file, setFile] = useState<File | null>(null)
  const [dragging, setDragging] = useState(false)
  const [previewing, setPreviewing] = useState(false)
  const [preview, setPreview] = useState<PreviewResult | null>(null)
  const [previewError, setPreviewError] = useState<string | null>(null)
  const [tableName, setTableName] = useState('')
  const [ifExists, setIfExists] = useState<IfExists>('replace')
  const [committing, setCommitting] = useState(false)
  const [commitError, setCommitError] = useState<string | null>(null)
  const [result, setResult] = useState<{ tableName: string; rowCount: number } | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)

  const reset = () => {
    setStep('input'); setSql(''); setFile(null); setPreview(null)
    setPreviewError(null); setTableName(''); setCommitError(null); setResult(null)
    setIfExists('replace')
  }

  const handleClose = () => { reset(); onCloseAction() }

  // ---- File drop ----
  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault(); setDragging(false)
    const f = e.dataTransfer.files?.[0]
    if (f) { setFile(f); setTableName(f.name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase()) }
  }, [])

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files?.[0] || null
    setFile(f)
    if (f) setTableName(f.name.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase())
  }

  // ---- Preview ----
  const handlePreview = async () => {
    setPreviewError(null); setPreviewing(true)
    try {
      let res: PreviewResult
      if (mode === 'sql') {
        res = await Api.importSqlPreview(dsId, { sql, limit: 100 })
      } else {
        if (!file) throw new Error('No file selected')
        res = await Api.importFilePreview(dsId, file)
        if (!tableName && res.fileName) setTableName(res.fileName.replace(/\.[^.]+$/, '').replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase())
      }
      setPreview(res)
      setStep('preview')
    } catch (e: any) {
      setPreviewError(e?.message || 'Preview failed')
    } finally {
      setPreviewing(false)
    }
  }

  // ---- Commit ----
  const handleCommit = async () => {
    if (!tableName.trim()) { setCommitError('Table name is required'); return }
    setCommitError(null); setCommitting(true)
    try {
      let res: { ok: boolean; tableName: string; rowCount: number }
      if (mode === 'sql') {
        res = await Api.importSqlCommit(dsId, { sql, tableName: tableName.trim(), ifExists })
      } else {
        if (!file) throw new Error('No file available')
        res = await Api.importFileCommit(dsId, file, tableName.trim(), ifExists)
      }
      setResult({ tableName: res.tableName, rowCount: res.rowCount })
      setStep('done')
      onImported(res.tableName, res.rowCount)
    } catch (e: any) {
      setCommitError(e?.message || 'Import failed')
    } finally {
      setCommitting(false)
    }
  }

  if (!open || typeof document === 'undefined') return null

  const canPreview = mode === 'sql' ? sql.trim().length > 0 : file !== null

  return createPortal(
    <div className="fixed inset-0 z-[1200]">
      <div className="absolute inset-0 bg-black/50" onClick={handleClose} />
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[780px] max-w-[96vw] max-h-[90vh] flex flex-col rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))] shadow-[0_8px_40px_rgba(0,0,0,0.18)]">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 border-b border-[hsl(var(--border))]">
          <div className="flex items-center gap-2.5">
            <RiTableLine className="w-4 h-4 text-[hsl(var(--primary))]" />
            <span className="text-sm font-semibold">Import Table</span>
            <div className="flex items-center gap-1 ml-2">
              {(['input', 'preview', 'done'] as Step[]).map((s, i) => (
                <React.Fragment key={s}>
                  <span className={`text-[11px] px-2 py-0.5 rounded-full font-medium ${step === s ? 'bg-[hsl(var(--primary))] text-white' : ((['input','preview','done'].indexOf(step) > i) ? 'bg-[hsl(var(--success))] text-white' : 'bg-[hsl(var(--muted))] text-[hsl(var(--muted-foreground))]')}`}>
                    {s === 'input' ? '1 Source' : s === 'preview' ? '2 Preview' : '3 Done'}
                  </span>
                  {i < 2 && <span className="text-[hsl(var(--muted-foreground))] text-[10px]">›</span>}
                </React.Fragment>
              ))}
            </div>
          </div>
          <button className="p-1.5 rounded-md hover:bg-[hsl(var(--muted))] text-[hsl(var(--muted-foreground))]" onClick={handleClose} aria-label="Close">
            <RiCloseLine className="w-4 h-4" />
          </button>
        </div>

        {/* Body */}
        <div className="flex-1 overflow-auto p-5">
          {/* ---- STEP: INPUT ---- */}
          {step === 'input' && (
            <div className="space-y-4">
              {/* Mode tabs */}
              <div className="flex gap-1 p-0.5 rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--muted))] w-fit">
                {(['file', 'sql'] as Mode[]).map((m) => (
                  <button
                    key={m}
                    onClick={() => setMode(m)}
                    className={`text-xs px-4 py-1.5 rounded-md font-medium transition-colors ${mode === m ? 'bg-[hsl(var(--background))] shadow text-[hsl(var(--foreground))]' : 'text-[hsl(var(--muted-foreground))] hover:text-[hsl(var(--foreground))]'}`}
                  >
                    {m === 'file' ? 'Upload File (CSV / Excel)' : 'SQL Query'}
                  </button>
                ))}
              </div>

              {/* File upload */}
              {mode === 'file' && (
                <div
                  className={`relative flex flex-col items-center justify-center gap-3 rounded-xl border-2 border-dashed p-10 text-center cursor-pointer transition-colors ${dragging ? 'border-[hsl(var(--primary))] bg-[hsl(var(--primary))]/5' : 'border-[hsl(var(--border))] hover:border-[hsl(var(--primary))]/60 hover:bg-[hsl(var(--muted))]/50'}`}
                  onDragOver={(e) => { e.preventDefault(); setDragging(true) }}
                  onDragLeave={() => setDragging(false)}
                  onDrop={handleDrop}
                  onClick={() => fileInputRef.current?.click()}
                >
                  <input ref={fileInputRef} type="file" accept=".csv,.tsv,.xlsx,.xls" className="sr-only" onChange={handleFileChange} />
                  {file ? (
                    <>
                      <div className="w-12 h-12 rounded-full bg-[hsl(var(--success))]/10 flex items-center justify-center">
                        <RiFileLine className="w-6 h-6 text-[hsl(var(--success))]" />
                      </div>
                      <div>
                        <div className="text-sm font-medium">{file.name}</div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))] mt-0.5">{(file.size / 1024).toFixed(1)} KB — click to change</div>
                      </div>
                    </>
                  ) : (
                    <>
                      <div className="w-12 h-12 rounded-full bg-[hsl(var(--primary))]/10 flex items-center justify-center">
                        <RiUpload2Line className="w-6 h-6 text-[hsl(var(--primary))]" />
                      </div>
                      <div>
                        <div className="text-sm font-medium">Drop a file here or click to browse</div>
                        <div className="text-xs text-[hsl(var(--muted-foreground))] mt-1">Supports CSV, TSV, Excel (.xlsx / .xls)</div>
                      </div>
                    </>
                  )}
                </div>
              )}

              {/* SQL input */}
              {mode === 'sql' && (
                <div className="space-y-2">
                  <label className="text-xs text-[hsl(var(--muted-foreground))]">
                    Write a SELECT query. The result will be imported as a new table.
                  </label>
                  <textarea
                    className="w-full h-48 px-3 py-2.5 rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] font-mono text-xs resize-y focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary))]/40"
                    value={sql}
                    onChange={(e) => setSql(e.target.value)}
                    placeholder={"SELECT\n  date,\n  SUM(amount) AS total\nFROM transactions\nGROUP BY date\nORDER BY date DESC"}
                    spellCheck={false}
                  />
                </div>
              )}

              {previewError && (
                <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-[hsl(var(--danger))]/8 border border-[hsl(var(--danger))]/20 text-[hsl(var(--danger))] text-xs">
                  <RiErrorWarningLine className="w-4 h-4 flex-shrink-0" />
                  <span>{previewError}</span>
                </div>
              )}
            </div>
          )}

          {/* ---- STEP: PREVIEW ---- */}
          {step === 'preview' && preview && (
            <div className="space-y-4">
              {/* Stats */}
              <div className="flex items-center gap-4 text-xs text-[hsl(var(--muted-foreground))]">
                <span><span className="font-medium text-[hsl(var(--foreground))]">{preview.columns.length}</span> columns</span>
                <span><span className="font-medium text-[hsl(var(--foreground))]">{preview.rowCount}</span> preview rows</span>
                {preview.fileName && <span>from <span className="font-mono">{preview.fileName}</span></span>}
              </div>

              {/* Data grid */}
              <div className="overflow-auto rounded-lg border border-[hsl(var(--border))] max-h-[320px]">
                <table className="min-w-full text-xs">
                  <thead className="sticky top-0 bg-[hsl(var(--card))] border-b border-[hsl(var(--border))] z-10">
                    <tr>
                      {preview.columns.map((col) => (
                        <th key={col} className="px-3 py-2 text-left font-medium whitespace-nowrap text-[hsl(var(--muted-foreground))]">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {preview.rows.map((row, ri) => (
                      <tr key={ri} className={`border-t border-[hsl(var(--border))] ${ri % 2 === 0 ? '' : 'bg-[hsl(var(--muted))]/30'}`}>
                        {preview.columns.map((col) => (
                          <td key={col} className="px-3 py-1.5 whitespace-nowrap max-w-[200px] truncate text-[hsl(var(--foreground))]">
                            {row[col] === null || row[col] === undefined ? (
                              <span className="text-[hsl(var(--muted-foreground))] italic">null</span>
                            ) : String(row[col])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {/* Table name + options */}
              <div className="grid grid-cols-2 gap-3 pt-1">
                <div>
                  <label className="block text-xs font-medium mb-1">Table name <span className="text-[hsl(var(--danger))]">*</span></label>
                  <input
                    className="w-full h-8 px-2.5 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-sm font-mono focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary))]/40"
                    value={tableName}
                    onChange={(e) => setTableName(e.target.value.replace(/[^a-zA-Z0-9_]/g, '_'))}
                    placeholder="my_table"
                    spellCheck={false}
                  />
                  <div className="text-[10px] text-[hsl(var(--muted-foreground))] mt-1">Letters, numbers and underscores only</div>
                </div>
                <div>
                  <label className="block text-xs font-medium mb-1">If table exists</label>
                  <select
                    className="w-full h-8 px-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-sm focus:outline-none focus:ring-2 focus:ring-[hsl(var(--primary))]/40"
                    value={ifExists}
                    onChange={(e) => setIfExists(e.target.value as IfExists)}
                  >
                    <option value="replace">Replace (drop & recreate)</option>
                    <option value="append">Append rows</option>
                    <option value="fail">Fail if exists</option>
                  </select>
                </div>
              </div>

              {commitError && (
                <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-[hsl(var(--danger))]/8 border border-[hsl(var(--danger))]/20 text-[hsl(var(--danger))] text-xs">
                  <RiErrorWarningLine className="w-4 h-4 flex-shrink-0" />
                  <span>{commitError}</span>
                </div>
              )}
            </div>
          )}

          {/* ---- STEP: DONE ---- */}
          {step === 'done' && result && (
            <div className="flex flex-col items-center gap-4 py-8">
              <div className="w-16 h-16 rounded-full bg-[hsl(var(--success))]/10 flex items-center justify-center">
                <RiCheckLine className="w-8 h-8 text-[hsl(var(--success))]" />
              </div>
              <div className="text-center">
                <div className="text-base font-semibold">Import complete</div>
                <div className="text-sm text-[hsl(var(--muted-foreground))] mt-1">
                  Table <code className="px-1 py-0.5 rounded bg-[hsl(var(--muted))] font-mono text-xs">{result.tableName}</code> created with{' '}
                  <span className="font-medium">{result.rowCount.toLocaleString()}</span> rows.
                </div>
              </div>
              <div className="flex gap-2 mt-2">
                <button
                  className="text-sm px-4 py-2 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]"
                  onClick={() => { reset() }}
                >
                  Import Another
                </button>
                <button
                  className="text-sm px-4 py-2 rounded-md bg-[hsl(var(--primary))] text-white hover:bg-[hsl(var(--primary))]/90"
                  onClick={handleClose}
                >
                  Done
                </button>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        {step !== 'done' && (
          <div className="flex items-center justify-between px-5 py-3 border-t border-[hsl(var(--border))] bg-[hsl(var(--card))]/50">
            <div className="flex gap-2">
              {step === 'preview' && (
                <button
                  className="text-xs px-3 py-1.5 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]"
                  onClick={() => { setStep('input'); setPreview(null); setCommitError(null) }}
                >
                  ← Back
                </button>
              )}
            </div>
            <div className="flex gap-2">
              <button
                className="text-xs px-3 py-1.5 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]"
                onClick={handleClose}
              >
                Cancel
              </button>
              {step === 'input' && (
                <button
                  disabled={!canPreview || previewing}
                  className="text-xs px-4 py-1.5 rounded-md bg-[hsl(var(--primary))] text-white hover:bg-[hsl(var(--primary))]/90 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1.5"
                  onClick={handlePreview}
                >
                  {previewing && <span className="w-3 h-3 border border-white/40 border-t-white rounded-full animate-spin" />}
                  Preview Data
                </button>
              )}
              {step === 'preview' && (
                <button
                  disabled={!tableName.trim() || committing}
                  className="text-xs px-4 py-1.5 rounded-md bg-[hsl(var(--primary))] text-white hover:bg-[hsl(var(--primary))]/90 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-1.5"
                  onClick={handleCommit}
                >
                  {committing && <span className="w-3 h-3 border border-white/40 border-t-white rounded-full animate-spin" />}
                  Import Table
                </button>
              )}
            </div>
          </div>
        )}
      </div>
    </div>,
    document.body
  )
}
