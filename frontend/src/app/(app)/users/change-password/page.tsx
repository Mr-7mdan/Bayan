"use client"

import { Card, Title, Text } from '@tremor/react'
import { useAuth } from '@/components/providers/AuthProvider'
import { useState } from 'react'
import { useTranslations } from 'next-intl'
import { Api } from '@/lib/api'
import { Button, Input } from '@/components/ui'

export default function ChangePasswordPage() {
  const { user } = useAuth()
  const t = useTranslations('pages.changePassword')
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
      setErr(t('mismatch'))
      return
    }
    setBusy(true)
    setErr(null)
    setMsg(null)
    try {
      await Api.changePassword({ userId: user.id, oldPassword, newPassword })
      setMsg(t('success'))
      setOldPassword('')
      setNewPassword('')
      setConfirm('')
    } catch (e: any) {
      setErr(e?.message || t('failure'))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">{t('title')}</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">{t('subtitle')}</Text>
          </div>
        </div>
        <form onSubmit={onSubmit} className="px-4 py-4 space-y-3 max-w-md">
          <Input type="password" label={t('current')} value={oldPassword} onChange={(e) => setOldPassword(e.target.value)} required />
          <Input type="password" label={t('new')} value={newPassword} onChange={(e) => setNewPassword(e.target.value)} required />
          <Input type="password" label={t('confirm')} value={confirm} onChange={(e) => setConfirm(e.target.value)} required />
          {err && <div className="text-sm text-[hsl(var(--danger))]">{err}</div>}
          {msg && <div className="text-sm text-[hsl(var(--success))]">{msg}</div>}
          <div className="pt-1">
            <Button type="submit" variant="primary" size="sm" loading={busy}>{t('submit')}</Button>
          </div>
        </form>
      </Card>
    </div>
  )
}
