"use client"

import { Card, Title, Text } from '@tremor/react'
import { useAuth } from '@/components/providers/AuthProvider'
import { useState } from 'react'
import { Api } from '@/lib/api'

export default function ChangePasswordPage() {
  const { user } = useAuth()
  const [oldPassword, setOldPassword] = useState('')
  const [newPassword, setNewPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!user?.id) return
    if (!newPassword || newPassword !== confirm) {
      setErr('New passwords do not match')
      return
    }
    setBusy(true)
    setErr(null)
    setMsg(null)
    try {
      await Api.changePassword({ userId: user.id, oldPassword, newPassword })
      setMsg('Password updated successfully.')
      setOldPassword('')
      setNewPassword('')
      setConfirm('')
    } catch (e: any) {
      setErr(e?.message || 'Failed to change password')
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">Change Password</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">Update your account password</Text>
          </div>
        </div>
        <form onSubmit={onSubmit} className="px-4 py-4 space-y-3">
          <label className="text-sm block">Current password
            <input type="password" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={oldPassword} onChange={(e) => setOldPassword(e.target.value)} required />
          </label>
          <label className="text-sm block">New password
            <input type="password" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={newPassword} onChange={(e) => setNewPassword(e.target.value)} required />
          </label>
          <label className="text-sm block">Confirm new password
            <input type="password" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={confirm} onChange={(e) => setConfirm(e.target.value)} required />
          </label>
          {err && <div className="text-sm text-red-600">{err}</div>}
          {msg && <div className="text-sm text-emerald-600">{msg}</div>}
          <div className="pt-1">
            <button type="submit" disabled={busy} className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted disabled:opacity-60">{busy ? 'Updating…' : 'Update password'}</button>
          </div>
        </form>
      </Card>
    </div>
  )
}
