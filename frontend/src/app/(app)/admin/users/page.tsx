"use client"

import { Suspense, useEffect, useMemo, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Card, Title, Text, Select, SelectItem } from '@tremor/react'
import * as Dialog from '@radix-ui/react-dialog'
import { useAuth } from '@/components/providers/AuthProvider'
import { Api, type UserOut, type UserRowOut } from '@/lib/api'

export const dynamic = 'force-dynamic'

export default function AdminUsersPage() {
  const { user } = useAuth()
  const router = useRouter()
  const isAdmin = (user?.role || '').toLowerCase() === 'admin'

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [rows, setRows] = useState<UserRowOut[]>([])
  const [toast, setToast] = useState('')

  // Create user dialog
  const [createOpen, setCreateOpen] = useState(false)
  const [cuName, setCuName] = useState('')
  const [cuEmail, setCuEmail] = useState('')
  const [cuPassword, setCuPassword] = useState('')
  const [cuRole, setCuRole] = useState<'user'|'admin'>('user')
  const [cuBusy, setCuBusy] = useState(false)
  const [cuError, setCuError] = useState<string | null>(null)

  // Change password dialog
  const [pwdOpen, setPwdOpen] = useState(false)
  const [pwdTarget, setPwdTarget] = useState<UserRowOut | null>(null)
  const [pwdNew, setPwdNew] = useState('')
  const [pwdBusy, setPwdBusy] = useState(false)
  const [pwdError, setPwdError] = useState<string | null>(null)

  // Search & pagination
  const [search, setSearch] = useState('')
  const [pageSize, setPageSize] = useState(8)
  const [page, setPage] = useState(0)

  const filteredRows = useMemo(() => {
    const q = (search || '').trim().toLowerCase()
    if (!q) return rows
    return rows.filter((r) =>
      (r.name || '').toLowerCase().includes(q) ||
      (r.email || '').toLowerCase().includes(q) ||
      (r.role || '').toLowerCase().includes(q)
    )
  }, [rows, search])

  const totalPages = Math.max(1, Math.ceil((filteredRows.length || 0) / pageSize))
  useEffect(() => {
    if (page >= totalPages) setPage(Math.max(0, totalPages - 1))
  }, [totalPages, page])
  useEffect(() => {
    // Reset to first page when search or pageSize changes
    setPage(0)
  }, [search, pageSize])
  const startIdx = page * pageSize
  const endIdx = Math.min(startIdx + pageSize, filteredRows.length)
  const pageRows = useMemo(() => filteredRows.slice(startIdx, endIdx), [filteredRows, startIdx, endIdx])

  useEffect(() => {
    if (!isAdmin) router.replace('/home')
  }, [isAdmin, router])

  const load = async () => {
    if (!user?.id) return
    setLoading(true); setError(null)
    try {
      const list = await Api.adminListUsers(user.id)
      setRows(list || [])
    } catch (e: any) {
      setError(e?.message || 'Failed to load users')
    } finally { setLoading(false) }
  }

  useEffect(() => { void load() }, [user?.id])

  const onSetActive = async (row: UserRowOut, next: boolean) => {
    if (!user?.id) return
    try {
      await Api.adminSetActive(user.id, row.id, next)
      setRows((prev) => prev.map((r) => r.id === row.id ? { ...r, active: next } : r))
    } catch (e: any) {
      setToast(e?.message || 'Failed to update')
      window.setTimeout(() => setToast(''), 1600)
    }
  }

  const onCreate = async () => {
    if (!user?.id) return
    setCuBusy(true); setCuError(null)
    try {
      await Api.adminCreateUser(user.id, { name: cuName || cuEmail.split('@')[0], email: cuEmail, password: cuPassword, role: cuRole })
      setCreateOpen(false)
      setCuName(''); setCuEmail(''); setCuPassword(''); setCuRole('user')
      await load()
      setToast('User created'); window.setTimeout(() => setToast(''), 1500)
    } catch (e: any) {
      setCuError(e?.message || 'Failed to create user')
    } finally {
      setCuBusy(false)
    }
  }

  const onSetPassword = async () => {
    if (!user?.id || !pwdTarget) return
    setPwdBusy(true); setPwdError(null)
    try {
      await Api.adminSetPassword(user.id, pwdTarget.id, pwdNew)
      setPwdOpen(false); setPwdNew(''); setPwdTarget(null)
      setToast('Password updated'); window.setTimeout(() => setToast(''), 1500)
    } catch (e: any) {
      setPwdError(e?.message || 'Failed to update password')
    } finally { setPwdBusy(false) }
  }

  const table = useMemo(() => (
    <div className="overflow-auto rounded-xl border-2 border-[hsl(var(--border))]">
      <table className="min-w-full text-sm">
        <thead className="bg-[hsl(var(--card))] border-b border-[hsl(var(--border))]">
          <tr>
            <th className="text-left px-3 py-2 font-medium">Name</th>
            <th className="text-left px-3 py-2 font-medium">Email</th>
            <th className="text-left px-3 py-2 font-medium">Role</th>
            <th className="text-left px-3 py-2 font-medium">Active</th>
            <th className="text-left px-3 py-2 font-medium">Actions</th>
          </tr>
        </thead>
        <tbody className="bg-[hsl(var(--background))]">
          {pageRows.map((r) => (
            <tr key={r.id} className="border-t border-[hsl(var(--border))]">
              <td className="px-3 py-2">{r.name}</td>
              <td className="px-3 py-2">{r.email}</td>
              <td className="px-3 py-2 capitalize">{r.role}</td>
              <td className="px-3 py-2">{r.active ? 'Yes' : 'No'}</td>
              <td className="px-3 py-2">
                <div className="flex items-center gap-2">
                  <button
                    className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]"
                    onClick={() => onSetActive(r, !r.active)}
                  >{r.active ? 'Deactivate' : 'Activate'}</button>
                  <button
                    className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]"
                    onClick={() => { setPwdTarget(r); setPwdOpen(true) }}
                  >Change Password</button>
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
    <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">Users</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">Manage system users</Text>
          </div>
          <button className="inline-flex items-center rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]" onClick={() => setCreateOpen(true)}>Create User</button>
        </div>
        <div className="p-3 space-y-3">
          <div className="flex items-center py-2 gap-2">
            <div className="flex items-center gap-2">
              <label htmlFor="searchUsers" className="text-sm mr-2 text-gray-600 dark:text-gray-300">Search</label>
              <input id="searchUsers" value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search users..." className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]" />
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
          {loading ? <Text>Loading…</Text> : (
            <>
              {table}
              <div className="mt-1 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
                <span>Showing {page * pageSize + 1}–{Math.min((page + 1) * pageSize, filteredRows.length)} of {filteredRows.length}</span>
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

      {/* Create User Dialog */}
      <Dialog.Root open={createOpen} onOpenChange={setCreateOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/30" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[460px] -translate-x-1/2 -translate-y-1/2 rounded-xl border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">Create user</Dialog.Title>
            <div className="mt-3 space-y-3">
              <label className="text-sm block">Full Name
                <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={cuName} onChange={(e) => setCuName(e.target.value)} />
              </label>
              <label className="text-sm block">Email
                <input type="email" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={cuEmail} onChange={(e) => setCuEmail(e.target.value)} />
              </label>
              <label className="text-sm block">Password
                <input type="password" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={cuPassword} onChange={(e) => setCuPassword(e.target.value)} />
              </label>
              <label className="text-sm block">Role
                <select className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={cuRole} onChange={(e) => setCuRole((e.target.value as 'user'|'admin') || 'user')}>
                  <option value="user">User</option>
                  <option value="admin">Admin</option>
                </select>
              </label>
              {cuError && <div className="text-sm text-red-600">{cuError}</div>}
              <div className="flex items-center justify-end gap-2">
                <Dialog.Close asChild>
                  <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Cancel</button>
                </Dialog.Close>
                <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" disabled={cuBusy || !cuEmail || !cuPassword} onClick={onCreate}>{cuBusy ? 'Creating…' : 'Create'}</button>
              </div>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog.Root>

      {/* Change Password Dialog */}
      <Dialog.Root open={pwdOpen} onOpenChange={setPwdOpen}>
        <Dialog.Portal>
          <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/30" />
          <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[420px] -translate-x-1/2 -translate-y-1/2 rounded-xl border bg-card p-4 shadow-card">
            <Dialog.Title className="text-lg font-semibold">Change password ({pwdTarget?.email})</Dialog.Title>
            <div className="mt-3 space-y-3">
              <label className="text-sm block">New Password
                <input type="password" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={pwdNew} onChange={(e) => setPwdNew(e.target.value)} />
              </label>
              {pwdError && <div className="text-sm text-red-600">{pwdError}</div>}
              <div className="flex items-center justify-end gap-2">
                <Dialog.Close asChild>
                  <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Cancel</button>
                </Dialog.Close>
                <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" disabled={pwdBusy || !pwdNew} onClick={onSetPassword}>{pwdBusy ? 'Saving…' : 'Save'}</button>
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
