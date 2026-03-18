"use client"

import { Suspense, useEffect, useMemo, useRef, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Card, Title, Text, Select, SelectItem } from '@tremor/react'
import * as Dialog from '@radix-ui/react-dialog'
import { useAuth } from '@/components/providers/AuthProvider'
import { Api, type HolidayRuleCreate, type HolidayRuleOut } from '@/lib/api'

export const dynamic = 'force-dynamic'

export default function AdminHolidaysPage() {
  const { user } = useAuth()
  const router = useRouter()
  const isAdmin = (user?.role || '').toLowerCase() === 'admin'

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [rows, setRows] = useState<HolidayRuleOut[]>([])
  const [toast, setToast] = useState('')

  // Create dialog state
  const [createOpen, setCreateOpen] = useState(false)
  const [crName, setCrName] = useState('')
  const [crRuleType, setCrRuleType] = useState<'specific' | 'recurring'>('specific')
  const [crSpecificDate, setCrSpecificDate] = useState('')
  const [crRecurrenceExpr, setCrRecurrenceExpr] = useState('')
  const [crBusy, setCrBusy] = useState(false)
  const [crError, setCrError] = useState<string | null>(null)

  // Edit dialog state
  const [editOpen, setEditOpen] = useState(false)
  const [editTarget, setEditTarget] = useState<HolidayRuleOut | null>(null)
  const [edName, setEdName] = useState('')
  const [edRuleType, setEdRuleType] = useState<'specific' | 'recurring'>('specific')
  const [edSpecificDate, setEdSpecificDate] = useState('')
  const [edRecurrenceExpr, setEdRecurrenceExpr] = useState('')
  const [edBusy, setEdBusy] = useState(false)
  const [edError, setEdError] = useState<string | null>(null)

  // Delete confirm state
  const [deleteOpen, setDeleteOpen] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState<HolidayRuleOut | null>(null)
  const [delBusy, setDelBusy] = useState(false)

  // CSV upload
  const fileRef = useRef<HTMLInputElement>(null)
  const [uploading, setUploading] = useState(false)

  // Search & pagination
  const [search, setSearch] = useState('')
  const [pageSize, setPageSize] = useState(8)
  const [page, setPage] = useState(0)

  const filteredRows = useMemo(() => {
    const q = (search || '').trim().toLowerCase()
    if (!q) return rows
    return rows.filter((r) =>
      (r.name || '').toLowerCase().includes(q) ||
      (r.rule_type || '').toLowerCase().includes(q) ||
      (r.specific_date || '').toLowerCase().includes(q) ||
      (r.recurrence_expr || '').toLowerCase().includes(q)
    )
  }, [rows, search])

  const totalPages = Math.max(1, Math.ceil((filteredRows.length || 0) / pageSize))
  useEffect(() => {
    if (page >= totalPages) setPage(Math.max(0, totalPages - 1))
  }, [totalPages, page])
  useEffect(() => {
    setPage(0)
  }, [search, pageSize])
  const startIdx = page * pageSize
  const endIdx = Math.min(startIdx + pageSize, filteredRows.length)
  const pageRows = useMemo(() => filteredRows.slice(startIdx, endIdx), [filteredRows, startIdx, endIdx])

  useEffect(() => {
    if (!isAdmin) router.replace('/home')
  }, [isAdmin, router])

  const load = async () => {
    setLoading(true); setError(null)
    try {
      const list = await Api.listHolidays()
      setRows(list || [])
    } catch (e: any) {
      setError(e?.message || 'Failed to load holidays')
    } finally { setLoading(false) }
  }

  useEffect(() => { void load() }, [])

  const showToast = (msg: string) => {
    setToast(msg)
    window.setTimeout(() => setToast(''), 1500)
  }

  // --- Create ---
  const resetCreateForm = () => {
    setCrName(''); setCrRuleType('specific'); setCrSpecificDate(''); setCrRecurrenceExpr(''); setCrError(null)
  }
  const onCreate = async () => {
    setCrBusy(true); setCrError(null)
    try {
      const payload: HolidayRuleCreate = {
        name: crName,
        rule_type: crRuleType,
        specific_date: crRuleType === 'specific' ? crSpecificDate || null : null,
        recurrence_expr: crRuleType === 'recurring' ? crRecurrenceExpr || null : null,
      }
      await Api.createHoliday(payload)
      setCreateOpen(false); resetCreateForm()
      await load()
      showToast('Holiday created')
    } catch (e: any) {
      setCrError(e?.message || 'Failed to create holiday')
    } finally { setCrBusy(false) }
  }

  // --- Edit ---
  const openEdit = (r: HolidayRuleOut) => {
    setEditTarget(r)
    setEdName(r.name)
    setEdRuleType((r.rule_type as 'specific' | 'recurring') || 'specific')
    setEdSpecificDate(r.specific_date || '')
    setEdRecurrenceExpr(r.recurrence_expr || '')
    setEdError(null)
    setEditOpen(true)
  }
  const onEdit = async () => {
    if (!editTarget) return
    setEdBusy(true); setEdError(null)
    try {
      const payload: HolidayRuleCreate = {
        name: edName,
        rule_type: edRuleType,
        specific_date: edRuleType === 'specific' ? edSpecificDate || null : null,
        recurrence_expr: edRuleType === 'recurring' ? edRecurrenceExpr || null : null,
      }
      await Api.updateHoliday(editTarget.id, payload)
      setEditOpen(false); setEditTarget(null)
      await load()
      showToast('Holiday updated')
    } catch (e: any) {
      setEdError(e?.message || 'Failed to update holiday')
    } finally { setEdBusy(false) }
  }

  // --- Delete ---
  const onDelete = async () => {
    if (!deleteTarget) return
    setDelBusy(true)
    try {
      await Api.deleteHoliday(deleteTarget.id)
      setDeleteOpen(false); setDeleteTarget(null)
      await load()
      showToast('Holiday deleted')
    } catch (e: any) {
      showToast(e?.message || 'Failed to delete')
    } finally { setDelBusy(false) }
  }

  // --- CSV upload ---
  const onUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    setUploading(true)
    try {
      const result = await Api.uploadHolidays(file)
      await load()
      showToast(`Uploaded ${result.created} holiday(s)`)
    } catch (err: any) {
      showToast(err?.message || 'Upload failed')
    } finally {
      setUploading(false)
      if (fileRef.current) fileRef.current.value = ''
    }
  }

  const table = useMemo(() => (
    <div className="overflow-auto rounded-xl border-2 border-[hsl(var(--border))]">
      <table className="min-w-full text-sm">
        <thead className="bg-[hsl(var(--card))] border-b border-[hsl(var(--border))]">
          <tr>
            <th className="text-left px-3 py-2 font-medium">Name</th>
            <th className="text-left px-3 py-2 font-medium">Type</th>
            <th className="text-left px-3 py-2 font-medium">Specific Date</th>
            <th className="text-left px-3 py-2 font-medium">Recurrence</th>
            <th className="text-left px-3 py-2 font-medium">Actions</th>
          </tr>
        </thead>
        <tbody className="bg-[hsl(var(--background))]">
          {pageRows.map((r) => (
            <tr key={r.id} className="border-t border-[hsl(var(--border))]">
              <td className="px-3 py-2">{r.name}</td>
              <td className="px-3 py-2 capitalize">{r.rule_type}</td>
              <td className="px-3 py-2">{r.specific_date || '\u2014'}</td>
              <td className="px-3 py-2">{r.recurrence_expr || '\u2014'}</td>
              <td className="px-3 py-2">
                <div className="flex items-center gap-2">
                  <button
                    className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]"
                    onClick={() => openEdit(r)}
                  >Edit</button>
                  <button
                    className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]"
                    onClick={() => { setDeleteTarget(r); setDeleteOpen(true) }}
                  >Delete</button>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  ), [pageRows])

  if (!isAdmin) return null

  return (
    <Suspense fallback={<div className="p-3 text-sm">Loading&hellip;</div>}>
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">Holidays</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">Manage holiday rules for date presets</Text>
          </div>
          <div className="flex items-center gap-2">
            <input ref={fileRef} type="file" accept=".csv" className="hidden" onChange={onUpload} />
            <button
              className="inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]"
              onClick={() => fileRef.current?.click()}
              disabled={uploading}
            >{uploading ? 'Uploading\u2026' : 'Upload CSV'}</button>
            <button
              className="inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]"
              onClick={() => { resetCreateForm(); setCreateOpen(true) }}
            >Add Holiday</button>
          </div>
        </div>
        <div className="p-3 space-y-3">
          <div className="flex items-center py-2 gap-2">
            <div className="flex items-center gap-2">
              <label htmlFor="searchHolidays" className="text-sm mr-2 text-gray-600 dark:text-gray-300">Search</label>
              <input id="searchHolidays" value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search holidays..." className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]" />
            </div>
            <div className="ml-auto flex items-center gap-2 text-sm shrink-0">
              <span className="whitespace-nowrap min-w-[84px]">Per page</span>
              <div className="min-w-[96px] rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
                [&_*]:!border-0 [&_*]:!border-transparent [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!ring-transparent [&_*]:!outline-none [&_*]:!shadow-none
                [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
                <Select
                  value={String(pageSize)}
                  onValueChange={(v) => setPageSize(parseInt(v || '8') || 8)}
                  className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
                >
                  <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="6">6</SelectItem>
                  <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="8">8</SelectItem>
                  <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="12">12</SelectItem>
                  <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="24">24</SelectItem>
                </Select>
              </div>
            </div>
          </div>
          {error && <div className="text-sm text-red-600">{error}</div>}
          {loading ? <Text>Loading&hellip;</Text> : (
            <>
              {table}
              <div className="mt-1 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
                <span>Showing {filteredRows.length === 0 ? 0 : page * pageSize + 1}&ndash;{Math.min((page + 1) * pageSize, filteredRows.length)} of {filteredRows.length}</span>
                <div className="flex items-center gap-2">
                  <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={page <= 0} onClick={() => setPage((p) => Math.max(0, p - 1))}>Prev</button>
                  <span>Page {page + 1} / {totalPages}</span>
                  <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={page >= totalPages - 1} onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}>Next</button>
                </div>
              </div>
            </>
          )}
        </div>
      </Card>

      {/* Create Holiday Dialog */}
      <Dialog.Root open={createOpen} onOpenChange={setCreateOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/30" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[460px] -translate-x-1/2 -translate-y-1/2 rounded-xl border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">Add holiday</Dialog.Title>
            <div className="mt-3 space-y-3">
              <label className="text-sm block">Name
                <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={crName} onChange={(e) => setCrName(e.target.value)} placeholder="e.g. National Day" />
              </label>
              <label className="text-sm block">Rule Type
                <select className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={crRuleType} onChange={(e) => setCrRuleType(e.target.value as 'specific' | 'recurring')}>
                  <option value="specific">Specific Date</option>
                  <option value="recurring">Recurring</option>
                </select>
              </label>
              {crRuleType === 'specific' && (
                <label className="text-sm block">Date
                  <input type="date" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={crSpecificDate} onChange={(e) => setCrSpecificDate(e.target.value)} />
                </label>
              )}
              {crRuleType === 'recurring' && (
                <label className="text-sm block">Recurrence Expression
                  <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={crRecurrenceExpr} onChange={(e) => setCrRecurrenceExpr(e.target.value)} placeholder="e.g. MM-DD or cron" />
                </label>
              )}
              {crError && <div className="text-sm text-red-600">{crError}</div>}
              <div className="flex items-center justify-end gap-2">
                <Dialog.Close asChild>
                  <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Cancel</button>
                </Dialog.Close>
                <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" disabled={crBusy || !crName} onClick={onCreate}>{crBusy ? 'Creating\u2026' : 'Create'}</button>
              </div>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {/* Edit Holiday Dialog */}
      <Dialog.Root open={editOpen} onOpenChange={setEditOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/30" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[460px] -translate-x-1/2 -translate-y-1/2 rounded-xl border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">Edit holiday</Dialog.Title>
            <div className="mt-3 space-y-3">
              <label className="text-sm block">Name
                <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={edName} onChange={(e) => setEdName(e.target.value)} />
              </label>
              <label className="text-sm block">Rule Type
                <select className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={edRuleType} onChange={(e) => setEdRuleType(e.target.value as 'specific' | 'recurring')}>
                  <option value="specific">Specific Date</option>
                  <option value="recurring">Recurring</option>
                </select>
              </label>
              {edRuleType === 'specific' && (
                <label className="text-sm block">Date
                  <input type="date" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={edSpecificDate} onChange={(e) => setEdSpecificDate(e.target.value)} />
                </label>
              )}
              {edRuleType === 'recurring' && (
                <label className="text-sm block">Recurrence Expression
                  <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={edRecurrenceExpr} onChange={(e) => setEdRecurrenceExpr(e.target.value)} placeholder="e.g. MM-DD or cron" />
                </label>
              )}
              {edError && <div className="text-sm text-red-600">{edError}</div>}
              <div className="flex items-center justify-end gap-2">
                <Dialog.Close asChild>
                  <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Cancel</button>
                </Dialog.Close>
                <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" disabled={edBusy || !edName} onClick={onEdit}>{edBusy ? 'Saving\u2026' : 'Save'}</button>
              </div>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {/* Delete Confirmation Dialog */}
      <Dialog.Root open={deleteOpen} onOpenChange={setDeleteOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/30" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[420px] -translate-x-1/2 -translate-y-1/2 rounded-xl border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">Delete holiday</Dialog.Title>
            <div className="mt-3 space-y-3">
              <Text>Are you sure you want to delete <strong>{deleteTarget?.name}</strong>?</Text>
              <div className="flex items-center justify-end gap-2">
                <Dialog.Close asChild>
                  <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Cancel</button>
                </Dialog.Close>
                <button type="button" className="text-sm px-3 py-1.5 rounded-md border text-red-600 hover:bg-red-50 dark:hover:bg-red-950" disabled={delBusy} onClick={onDelete}>{delBusy ? 'Deleting\u2026' : 'Delete'}</button>
              </div>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {!!toast && (
        <div className="fixed top-6 right-6 z-[100] flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-3 text-[14px] font-medium text-white">
          <span>{toast}</span>
        </div>
      )}
    </div>
    </Suspense>
  )
}
