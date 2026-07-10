"use client"

import React, { useEffect, useState } from 'react'
import { useTranslations } from 'next-intl'
import { Api, type SmsConfigPayload, type TestSmsPayload } from '@/lib/api'

export default function SmsConfigDialog({ open, onCloseAction }: { open: boolean; onCloseAction: () => void }) {
  const t = useTranslations('comms')
  const [form, setForm] = useState<SmsConfigPayload>({ apiKey: '', defaultSender: '' })
  const [testTo, setTestTo] = useState('')
  const [message, setMessage] = useState(t('alertDialog.sms.defaultMessage'))
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [toast, setToast] = useState('')

  useEffect(() => {
    let cancelled = false
    async function run() {
      if (!open) return
      setError(null)
      try {
        const cur = await Api.getSmsConfigHadara()
        if (!cancelled && cur) setForm({ ...cur })
      } catch { /* ignore */ }
    }
    void run(); return () => { cancelled = true }
  }, [open])

  const onSave = async () => {
    try {
      setBusy(true); setError(null)
      await Api.putSmsConfigHadara(form)
      setToast(t('alertDialog.toasts.saved')); window.setTimeout(() => setToast(''), 1600)
      onCloseAction()
    } catch (e: any) { setError(e?.message || t('alertDialog.toasts.failedSave')) } finally { setBusy(false) }
  }

  const onTest = async () => {
    try {
      setBusy(true); setError(null)
      const to = testTo.split(',').map((s)=>s.trim()).filter(Boolean)
      if (!to.length) { setError(t('alertDialog.toasts.enterRecipients')); setBusy(false); return }
      const payload: TestSmsPayload = { to, message }
      await Api.testSms(payload)
      setToast(t('alertDialog.toasts.testSent')); window.setTimeout(() => setToast(''), 1600)
    } catch (e: any) { setError(e?.message || t('alertDialog.toasts.failedSendTest')) } finally { setBusy(false) }
  }

  if (!open) return null
  return (
    <div className="fixed inset-0 z-[1200]">
      <div className="absolute inset-0 bg-black/40" onClick={() => !busy && onCloseAction()} />
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[720px] max-w-[95vw] rounded-lg border bg-card p-4">
        <div className="flex items-center justify-between mb-2">
          <div className="text-sm font-medium">{t('alertDialog.sms.title')}</div>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={onCloseAction} disabled={busy}>✕</button>
        </div>
        {error && <div className="mb-2 text-xs text-rose-600">{error}</div>}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <label className="text-sm">{t('alertDialog.sms.apiKey')}<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={(form as any).apiKey || ''} onChange={(e)=>setForm((f)=>({ ...f, apiKey: e.target.value } as any))} /></label>
          <label className="text-sm">{t('alertDialog.sms.defaultSender')}<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={form.defaultSender || ''} onChange={(e)=>setForm((f)=>({ ...f, defaultSender: e.target.value }))} /></label>
        </div>
        <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-3">
          <label className="text-sm">{t('alertDialog.sms.testTo')}<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="059xxxxxxx,056xxxxxxx" value={testTo} onChange={(e)=>setTestTo(e.target.value)} /></label>
          <label className="text-sm md:col-span-2">{t('alertDialog.sms.message')}<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={message} onChange={(e)=>setMessage(e.target.value)} /></label>
        </div>
        <div className="mt-4 flex items-center gap-2">
          <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" disabled={busy} onClick={onTest}>{t('alertDialog.sms.sendTest')}</button>
          <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" disabled={busy} onClick={onSave}>{t('alertDialog.common.save')}</button>
        </div>
        {!!toast && <div className="mt-3 text-xs text-emerald-700">{toast}</div>}
      </div>
    </div>
  )
}
