"use client"

import { useEffect, useMemo, useRef, useState } from 'react'
import * as Tabs from '@radix-ui/react-tabs'
import { Select, SelectItem, Card, Title, Text } from '@tremor/react'
import { Switch } from '@/components/Switch'
import { Api, type ContactOut, type ContactIn } from '@/lib/api'
import { useAuth } from '@/components/providers/AuthProvider'

export default function ContactsPage() {
  const { user } = useAuth()
  const isAdmin = String(user?.role || '').toLowerCase() === 'admin'
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [items, setItems] = useState<ContactOut[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [pageSize, setPageSize] = useState(20)
  const [search, setSearch] = useState('')
  const [activeOnly, setActiveOnly] = useState(true)
  const [selectedIds, setSelectedIds] = useState<Record<string, boolean>>({})

  // Dialogs
  const [editOpen, setEditOpen] = useState(false)
  const [editId, setEditId] = useState<string | null>(null)
  const [editModel, setEditModel] = useState<ContactIn>({ name: '', email: '', phone: '', tags: [] })
  const [tagInput, setTagInput] = useState<string>('')
  const [bulkEmailOpen, setBulkEmailOpen] = useState(false)
  const [bulkSmsOpen, setBulkSmsOpen] = useState(false)
  const [bulkEmailDraft, setBulkEmailDraft] = useState<{ to: string; subject: string; html: string }>({ to: '', subject: '', html: '' })
  const [bulkSmsDraft, setBulkSmsDraft] = useState<{ to: string; message: string }>({ to: '', message: '' })
  const [bulkEmailSending, setBulkEmailSending] = useState(false)
  const [bulkSmsSending, setBulkSmsSending] = useState(false)
  const [notice, setNotice] = useState<{ type: 'success'|'error'; text: string; details?: Array<{ recipient: string; error: string }> } | null>(null)
  const [emailSendSummary, setEmailSendSummary] = useState(true)
  const [smsSendSummary, setSmsSendSummary] = useState(true)
  const pollTimerRef = useRef<any>(null)
  // Throttling
  const [emailRateLimit, setEmailRateLimit] = useState<string>('')
  const [emailQueue, setEmailQueue] = useState<boolean>(false)
  const [smsRateLimit, setSmsRateLimit] = useState<string>('')
  const [smsQueue, setSmsQueue] = useState<boolean>(false)
  // Outlook-like To field state (email)
  type RecipientToken = { kind: 'email'|'phone'|'tag'; label: string; value: string }
  const [emailToTokens, setEmailToTokens] = useState<RecipientToken[]>([])
  const [emailToInput, setEmailToInput] = useState<string>('')
  const [emailSuggestions, setEmailSuggestions] = useState<Array<{ type: 'contact'|'tag'; label: string; email?: string; tag?: string }>>([])
  const [emailSugOpen, setEmailSugOpen] = useState<boolean>(false)
  // Outlook-like To field state (sms)
  const [smsToTokens, setSmsToTokens] = useState<RecipientToken[]>([])
  const [smsToInput, setSmsToInput] = useState<string>('')
  const [smsSuggestions, setSmsSuggestions] = useState<Array<{ type: 'contact'|'tag'; label: string; phone?: string; tag?: string }>>([])
  const [smsSugOpen, setSmsSugOpen] = useState<boolean>(false)
  // Multi-select for suggestions
  const [emailSel, setEmailSel] = useState<Set<string>>(new Set())
  const [smsSel, setSmsSel] = useState<Set<string>>(new Set())
  const emailKey = (s: { type:'contact'|'tag'; email?: string; tag?: string; label: string }) => (s.type==='contact' && s.email) ? `e:${s.email.toLowerCase()}` : (s.type==='tag' && s.tag) ? `t:${s.tag.toLowerCase()}` : `x:${s.label}`
  const smsKey = (s: { type:'contact'|'tag'; phone?: string; tag?: string; label: string }) => (s.type==='contact' && s.phone) ? `p:${s.phone.toLowerCase()}` : (s.type==='tag' && s.tag) ? `t:${s.tag.toLowerCase()}` : `x:${s.label}`
  function toggleEmailSel(s: { type:'contact'|'tag'; email?: string; tag?: string; label: string }) { const k=emailKey(s); setEmailSel(prev=>{ const n=new Set(prev); if(n.has(k)) n.delete(k); else n.add(k); return n }) }
  function toggleSmsSel(s: { type:'contact'|'tag'; phone?: string; tag?: string; label: string }) { const k=smsKey(s); setSmsSel(prev=>{ const n=new Set(prev); if(n.has(k)) n.delete(k); else n.add(k); return n }) }
  function addSelectedEmails() {
    const selected = new Set(emailSel)
    emailSuggestions.forEach(s => {
      const k = emailKey(s)
      if (!selected.has(k)) return
      if (s.type==='contact' && s.email) addEmailToken({ kind:'email', label:s.label, value:s.email })
      if (s.type==='tag' && s.tag) addEmailToken({ kind:'tag', label:`#${s.tag}`, value:s.tag })
    })
    setEmailSel(new Set()); setEmailSugOpen(false); setEmailToInput('')
  }

  function startPolling(jobId: string, kind: 'email'|'sms') {
    try { if (pollTimerRef.current) clearTimeout(pollTimerRef.current) } catch {}
    const tick = async () => {
      try {
        const s = await Api.contactsSendStatus(jobId)
        const txt = `${kind.toUpperCase()} ${s.done ? 'completed' : 'sending'}: ${s.success}/${s.total} sent, failed ${s.failed}`
        setNotice({ type: s.failed > 0 && s.done ? 'error' : 'success', text: txt, details: s.done ? (s.failures || []) : undefined })
        if (!s.done) {
          pollTimerRef.current = setTimeout(tick, 1500)
        }
      } catch (e: any) {
        setNotice({ type: 'error', text: e?.message || 'Failed to fetch send status' })
      }
    }
    pollTimerRef.current = setTimeout(tick, 600)
  }
  function addSelectedSms() {
    const selected = new Set(smsSel)
    smsSuggestions.forEach(s => {
      const k = smsKey(s)
      if (!selected.has(k)) return
      if (s.type==='contact' && s.phone) addSmsToken({ kind:'phone', label:s.label, value:s.phone })
      if (s.type==='tag' && s.tag) addSmsToken({ kind:'tag', label:`#${s.tag}`, value:s.tag })
    })
    setSmsSel(new Set()); setSmsSugOpen(false); setSmsToInput('')
  }

  // Quill editor refs (avoid findDOMNode by controlling DOM directly)
  const quillContainerRef = useRef<HTMLDivElement | null>(null)
  const quillInstanceRef = useRef<any>(null)

  useEffect(() => {
    if (!bulkEmailOpen) return
    let disposed = false
    ;(async () => {
      const { default: Quill } = await import('quill')
      if (disposed) return
      if (!quillContainerRef.current) return
      // Destroy previous instance if any
      try { if (quillInstanceRef.current) { quillInstanceRef.current.off('text-change') } } catch {}
      const q = new Quill(quillContainerRef.current, {
        theme: 'snow',
        modules: {
          toolbar: [
            ['bold', 'italic', 'underline'],
            [{ list: 'ordered' }, { list: 'bullet' }],
            ['link'],
            ['clean']
          ],
        },
      })
      quillInstanceRef.current = q
      q.root.innerHTML = bulkEmailDraft.html || ''
      q.on('text-change', () => {
        const html = q.root.innerHTML
        setBulkEmailDraft((d) => ({ ...d, html }))
      })
    })()
    return () => { disposed = true }
  }, [bulkEmailOpen])

  useEffect(() => { return () => { try { if (pollTimerRef.current) clearTimeout(pollTimerRef.current) } catch {} } }, [])


  const selectedList = useMemo(() => items.filter(it => selectedIds[it.id]), [items, selectedIds])

  function isValidEmail(s: string): boolean {
    const v = (s || '').trim()
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v)
  }
  function isValidPhone(s: string): boolean {
    const v = (s || '').trim()
    return /^[+]?[-(). \d]{6,}$/.test(v)
  }
  function addEmailToken(t: RecipientToken) {
    const key = t.kind + ':' + t.value.toLowerCase()
    setEmailToTokens((prev)=> {
      const exists = prev.some(x => (x.kind + ':' + x.value.toLowerCase()) === key)
      return exists ? prev : [...prev, t]
    })
  }
  function addSmsToken(t: RecipientToken) {
    const key = t.kind + ':' + t.value.toLowerCase()
    setSmsToTokens((prev)=> {
      const exists = prev.some(x => (x.kind + ':' + x.value.toLowerCase()) === key)
      return exists ? prev : [...prev, t]
    })
  }
  function tryCommitEmailInput() {
    const raw = (emailToInput || '').trim().replace(/[;,]+$/,'')
    if (!raw) return
    if (isValidEmail(raw)) addEmailToken({ kind: 'email', label: raw, value: raw })
    else if (raw.startsWith('#')) addEmailToken({ kind: 'tag', label: raw, value: raw.slice(1) })
    else addEmailToken({ kind: 'tag', label: `#${raw}`, value: raw })
    setEmailToInput(''); setEmailSugOpen(false)
  }
  function tryCommitSmsInput() {
    const raw = (smsToInput || '').trim().replace(/[;,]+$/,'')
    if (!raw) return
    if (isValidPhone(raw)) addSmsToken({ kind: 'phone', label: raw, value: raw })
    else if (raw.startsWith('#')) addSmsToken({ kind: 'tag', label: raw, value: raw.slice(1) })
    else addSmsToken({ kind: 'tag', label: `#${raw}`, value: raw })
    setSmsToInput(''); setSmsSugOpen(false)
  }
  useEffect(() => {
    if (!bulkEmailOpen) return
    const q = (emailToInput || '').trim()
    if (!q) { setEmailSuggestions([]); return }
    const h = setTimeout(async () => {
      try {
        const res = await Api.listContacts({ search: q, active: true, page: 1, pageSize: 50 })
        const out: Array<{ type: 'contact'|'tag'; label: string; email?: string; tag?: string }> = []
        ;(res.items || []).forEach(c => {
          const em = (c.email || '').trim()
          if (em) out.push({ type: 'contact', label: `${c.name} <${em}>`, email: em })
        })
        const tags = Array.from(new Set((res.items || []).flatMap(c => (c.tags || []))))
        tags.filter(t => t.toLowerCase().includes(q.toLowerCase())).forEach(t => out.push({ type: 'tag', label: `#${t}`, tag: t }))
        setEmailSuggestions(out.slice(0, 50)); setEmailSugOpen(true)
      } catch { setEmailSuggestions([]) }
    }, 150)
    return () => clearTimeout(h)
  }, [emailToInput, bulkEmailOpen])
  useEffect(() => {
    if (!bulkSmsOpen) return
    const q = (smsToInput || '').trim()
    if (!q) { setSmsSuggestions([]); return }
    const h = setTimeout(async () => {
      try {
        const res = await Api.listContacts({ search: q, active: true, page: 1, pageSize: 50 })
        const out: Array<{ type: 'contact'|'tag'; label: string; phone?: string; tag?: string }> = []
        ;(res.items || []).forEach(c => {
          const ph = (c.phone || '').trim()
          if (ph) out.push({ type: 'contact', label: `${c.name} <${ph}>`, phone: ph })
        })
        const tags = Array.from(new Set((res.items || []).flatMap(c => (c.tags || []))))
        tags.filter(t => t.toLowerCase().includes(q.toLowerCase())).forEach(t => out.push({ type: 'tag', label: `#${t}`, tag: t }))
        setSmsSuggestions(out.slice(0, 50)); setSmsSugOpen(true)
      } catch { setSmsSuggestions([]) }
    }, 150)
    return () => clearTimeout(h)
  }, [smsToInput, bulkSmsOpen])

  async function load() {
    try {
      setLoading(true); setError(null)
      const res = await Api.listContacts({ search: (search || '').trim() || undefined, active: activeOnly, page, pageSize })
      setItems(res.items || [])
      setTotal(res.total || 0)
    } catch (e: any) {
      setError(e?.message || 'Failed to load contacts')
    } finally { setLoading(false) }
  }
  useEffect(() => { void load() }, [page, pageSize, activeOnly])
  useEffect(() => {
    const t = setTimeout(() => { setPage(1); void load() }, 250)
    return () => clearTimeout(t)
  }, [search])

  function resetSelections() { setSelectedIds({}) }

  function openAdd() { setEditId(null); setEditModel({ name: '', email: '', phone: '', tags: [] }); setEditOpen(true) }
  function openEdit(it: ContactOut) { setEditId(it.id); setEditModel({ name: it.name, email: it.email || '', phone: it.phone || '', tags: it.tags || [] }); setEditOpen(true) }

  async function saveEdit() {
    const payload: ContactIn = { name: (editModel.name||'').trim(), email: (editModel.email||'') || undefined, phone: (editModel.phone||'') || undefined, tags: editModel.tags || [] }
    if (!payload.name) return
    if (items.some(it => it.name === payload.name && !selectedIds[it.id])) {
      // allow duplicates; skip unique enforcement for UX simplicity
    }
    try {
      setLoading(true)
      if (editId) await Api.updateContact(editId, payload)
      else await Api.createContact(payload)
      setEditOpen(false)
      setEditId(null)
      await load()
    } finally { setLoading(false) }
  }

  async function toggleActive(it: ContactOut) {
    try { await Api.deactivateContact(it.id, !it.active); await load() } catch {}
  }

  async function remove(it: ContactOut) {
    if (!confirm(`Delete contact ${it.name}?`)) return
    try { await Api.deleteContact(it.id); await load() } catch {}
  }

  function exportSelected() {
    const ids = items.filter(it => selectedIds[it.id]).map(it => it.id)
    Api.exportContacts(ids).then((res) => {
      const blob = new Blob([JSON.stringify(res.items || [], null, 2)], { type: 'application/json' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url; a.download = 'contacts.json'; a.click(); URL.revokeObjectURL(url)
    })
  }

  function importJson(files: FileList | null) {
    if (!files || !files[0]) return
    const f = files[0]
    const reader = new FileReader()
    reader.onload = async () => {
      try {
        const arr = JSON.parse(String(reader.result || '[]'))
        const items: ContactIn[] = Array.isArray(arr) ? arr.map((r: any) => ({ name: String(r.name||'').trim(), email: (r.email||undefined), phone: (r.phone||undefined), tags: (Array.isArray(r.tags)? r.tags: []) })) : []
        await Api.importContacts(items)
        await load()
      } catch {}
    }
    reader.readAsText(f)
  }

  function openBulkEmail() {
    const tos = new Set<string>()
    selectedList.forEach(c => { const e=(c.email||'').trim(); if (e) tos.add(e) })
    setBulkEmailDraft({ to: Array.from(tos).join(', '), subject: '', html: '' })
    setEmailToTokens(Array.from(tos).map(v => ({ kind: 'email', label: v, value: v })))
    setEmailToInput(''); setEmailSugOpen(false)
    setBulkEmailOpen(true)
  }
  function openBulkSms() {
    const tos = new Set<string>()
    selectedList.forEach(c => { const p=(c.phone||'').trim(); if (p) tos.add(p) })
    setBulkSmsDraft({ to: Array.from(tos).join(', '), message: '' })
    setSmsToTokens(Array.from(tos).map(v => ({ kind: 'phone', label: v, value: v })))
    setSmsToInput(''); setSmsSugOpen(false)
    setBulkSmsOpen(true)
  }

  async function sendBulkEmail() {
    if (bulkEmailSending) return
    const ids = items.filter(it => selectedIds[it.id]).map(it => it.id)
    const tokens = [...emailToTokens]
    const raw = (emailToInput || '').trim().replace(/[;,]+$/,'')
    if (raw) {
      if (isValidEmail(raw)) tokens.push({ kind: 'email', label: raw, value: raw })
      else if (raw.startsWith('#')) tokens.push({ kind: 'tag', label: raw, value: raw.slice(1) })
      else tokens.push({ kind: 'tag', label: `#${raw}`, value: raw })
    }
    const emails = Array.from(new Set(tokens.filter(t => t.kind==='email').map(t => t.value)))
    const tags = Array.from(new Set(tokens.filter(t => t.kind==='tag').map(t => t.value)))
    if (emails.length === 0 && ids.length === 0 && tags.length === 0) { setNotice({ type: 'error', text: 'Please add at least one recipient (email, selection, or tag)' }); return }
    if (tokens.some(t => t.kind==='email' && !isValidEmail(t.value))) { setNotice({ type: 'error', text: 'One or more emails are invalid' }); return }
    try {
      setBulkEmailSending(true)
      const rate = parseInt(emailRateLimit || '')
      const res = await Api.contactsSendEmail({ ids, emails, tags, subject: bulkEmailDraft.subject || '(no subject)', html: bulkEmailDraft.html || '<div/>', rateLimitPerMinute: Number.isFinite(rate) ? rate : undefined, queue: !!emailQueue, notifyEmail: (emailSendSummary && (user?.email || '').trim()) ? user!.email : undefined })
      setBulkEmailOpen(false)
      if (res.queued && res.jobId) {
        setNotice({ type: 'success', text: `Queued email to ${res.count} recipient(s)` })
        startPolling(res.jobId, 'email')
      } else {
        const succ = Number(res.success ?? res.count ?? 0)
        const fail = Number(res.failed ?? 0)
        const total = Number(res.count ?? (succ + fail))
        setNotice({ type: fail > 0 ? 'error' : 'success', text: `Email sent: ${succ}/${total}. Failed: ${fail}`, details: res.failures || [] })
      }
    } catch (e: any) {
      setNotice({ type: 'error', text: e?.message || 'Failed to send email' })
    } finally { setBulkEmailSending(false) }
  }
  async function sendBulkSms() {
    if (bulkSmsSending) return
    const ids = items.filter(it => selectedIds[it.id]).map(it => it.id)
    const tokens = [...smsToTokens]
    const raw = (smsToInput || '').trim().replace(/[;,]+$/,'')
    if (raw) {
      if (isValidPhone(raw)) tokens.push({ kind: 'phone', label: raw, value: raw })
      else if (raw.startsWith('#')) tokens.push({ kind: 'tag', label: raw, value: raw.slice(1) })
      else tokens.push({ kind: 'tag', label: `#${raw}`, value: raw })
    }
    const numbers = Array.from(new Set(tokens.filter(t => t.kind==='phone').map(t => t.value)))
    const tags = Array.from(new Set(tokens.filter(t => t.kind==='tag').map(t => t.value)))
    if (numbers.length === 0 && ids.length === 0 && tags.length === 0) { setNotice({ type: 'error', text: 'Please add at least one recipient (number, selection, or tag)' }); return }
    if (!((bulkSmsDraft.message || '').trim())) { setNotice({ type: 'error', text: 'Message is required' }); return }
    if (tokens.some(t => t.kind==='phone' && !isValidPhone(t.value))) { setNotice({ type: 'error', text: 'One or more phone numbers are invalid' }); return }
    try {
      setBulkSmsSending(true)
      const rate = parseInt(smsRateLimit || '')
      const res = await Api.contactsSendSms({ ids, numbers, tags, message: bulkSmsDraft.message || '', rateLimitPerMinute: Number.isFinite(rate) ? rate : undefined, queue: !!smsQueue, notifyEmail: (smsSendSummary && (user?.email || '').trim()) ? user!.email : undefined })
      setBulkSmsOpen(false)
      if (res.queued && res.jobId) {
        setNotice({ type: 'success', text: `Queued SMS to ${res.count} recipient(s)` })
        startPolling(res.jobId, 'sms')
      } else {
        const succ = Number(res.success ?? res.count ?? 0)
        const fail = Number(res.failed ?? 0)
        const total = Number(res.count ?? (succ + fail))
        setNotice({ type: fail > 0 ? 'error' : 'success', text: `SMS sent: ${succ}/${total}. Failed: ${fail}`, details: res.failures || [] })
      }
    } catch (e: any) {
      setNotice({ type: 'error', text: e?.message || 'Failed to send SMS' })
    } finally { setBulkSmsSending(false) }
  }

  const pages = Math.max(1, Math.ceil(total / pageSize))

  return (
    <div className="p-4">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--card))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">Contacts</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">Create and manage contacts, and send email/SMS</Text>
          </div>
          <div className="flex items-center gap-2">
            <button className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]" onClick={openAdd}>Add Contact</button>
            {isAdmin && (
              <>
                <label className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] cursor-pointer">
                  Import
                  <input type="file" accept="application/json" className="hidden" onChange={(e)=> importJson(e.target.files)} />
                </label>
                <button className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" onClick={exportSelected} disabled={!Object.values(selectedIds).some(Boolean)}>Export</button>
                <button className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]" onClick={openBulkEmail}>Send Bulk Email</button>
                <button className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]" onClick={openBulkSms}>Send Bulk SMS</button>
              </>
            )}
          </div>
        </div>
        <div className="px-3 py-2">
          <div className="flex items-center py-2 gap-2">
            <div className="flex items-center gap-2">
              <label htmlFor="searchContacts" className="text-sm mr-2 text-gray-600 dark:text-gray-300">Search</label>
              <input id="searchContacts" className="w-56 md:w-72 px-2 py-1.5 rounded-md border bg-[hsl(var(--card))]" placeholder="Search contacts..." value={search} onChange={(e)=> setSearch(e.target.value)} />
            </div>
            <label className="text-xs inline-flex items-center gap-2">
              <input type="checkbox" checked={activeOnly} onChange={(e)=> setActiveOnly(e.target.checked)} /> Active only
            </label>
            <div className="ml-auto text-xs">{loading ? 'Loading…' : `${total} contacts`}</div>
          </div>
        </div>
      </Card>

      {notice && (
        <div className={`mb-3 rounded-md border px-3 py-2 text-sm ${notice.type==='success' ? 'bg-emerald-50 border-emerald-200 text-emerald-700' : 'bg-rose-50 border-rose-200 text-rose-700'}`}>
          <div className="flex items-center justify-between gap-2">
            <span>{notice.text}</span>
            <button className="inline-flex items-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))]" onClick={()=> setNotice(null)}>Dismiss</button>
          </div>
          {Array.isArray(notice.details) && notice.details.length > 0 && (
            <div className="mt-2 max-h-40 overflow-auto text-xs">
              <table className="w-full">
                <thead>
                  <tr><th className="text-left pr-2">Recipient</th><th className="text-left">Error</th></tr>
                </thead>
                <tbody>
                  {notice.details.map((d, i) => (
                    <tr key={i}><td className="pr-2">{d.recipient}</td><td>{d.error}</td></tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      <div className="rounded-md border overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-[hsl(var(--muted))]">
            <tr>
              <th className="px-2 py-2 w-8 text-left"><input type="checkbox" checked={items.length>0 && items.every(it=>selectedIds[it.id])} onChange={(e)=> setSelectedIds(items.reduce((acc, it)=> (acc[it.id]=e.target.checked, acc), {} as Record<string, boolean>))} /></th>
              <th className="px-2 py-2 text-left">Full name</th>
              <th className="px-2 py-2 text-left">Email</th>
              <th className="px-2 py-2 text-left">Phone</th>
              <th className="px-2 py-2 text-left">Tags</th>
              <th className="px-2 py-2 text-left">Active</th>
              <th className="px-2 py-2 text-left">Actions</th>
            </tr>
          </thead>
          <tbody>
            {items.map((it) => (
              <tr key={it.id} className="border-t">
                <td className="px-2 py-2"><input type="checkbox" checked={!!selectedIds[it.id]} onChange={(e)=> setSelectedIds((prev)=> ({ ...prev, [it.id]: e.target.checked }))} /></td>
                <td className="px-2 py-2">{it.name}</td>
                <td className="px-2 py-2">{it.email || ''}</td>
                <td className="px-2 py-2">{it.phone || ''}</td>
                <td className="px-2 py-2 text-xs">{(it.tags || []).map((t)=> (<span key={t} className="inline-block px-2 py-0.5 rounded-full border mr-1 mb-1">{t}</span>))}</td>
                <td className="px-2 py-2">
                  <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 hover:bg-[hsl(var(--muted))]" onClick={()=> toggleActive(it)}>{it.active ? 'Active' : 'Inactive'}</button>
                </td>
                <td className="px-2 py-2">
                  <div className="flex items-center gap-2">
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-2 py-1 hover:bg-[hsl(var(--muted))]" onClick={()=> openEdit(it)}>Edit</button>
                    <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] px-2 py-1 hover:bg-[hsl(var(--muted))] text-red-600" onClick={()=> remove(it)}>Delete</button>
                  </div>
                </td>
              </tr>
            ))}
            {items.length === 0 && (
              <tr><td className="px-2 py-8 text-center text-xs text-muted-foreground" colSpan={7}>{loading ? 'Loading…' : 'No contacts found.'}</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div className="mt-3 flex items-center justify-between text-sm text-gray-600 dark:text-gray-300">
        <span>
          Showing {(page-1)*pageSize + 1}
          –{Math.min(page*pageSize, total)} of {total}
        </span>
        <div className="flex items-center gap-2">
          <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={page<=1} onClick={()=> setPage((p)=> Math.max(1, p-1))}>Prev</button>
          <span>Page {page} / {pages}</span>
          <button className="inline-flex items-center justify-center gap-1 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-gray-600 dark:text-gray-300 px-3 py-1.5 text-sm font-medium hover:bg-[hsl(var(--muted))] disabled:opacity-50 disabled:cursor-not-allowed" disabled={page>=pages} onClick={()=> setPage((p)=> Math.min(pages, p+1))}>Next</button>
          <span className="whitespace-nowrap min-w-[84px] ml-4">Per page</span>
          <div className="min-w-[96px] rounded-[10px] border border-[hsl(var(--border))] overflow-hidden bg-[hsl(var(--card))]
            [&_*]:!border-0 [&_*]:!border-transparent [&_*]:!ring-0 [&_*]:!ring-offset-0 [&_*]:!ring-transparent [&_*]:!outline-none [&_*]:!shadow-none
            [&_button]:rounded-[10px] [&_[role=combobox]]:rounded-[10px]">
            <Select
              value={String(pageSize)}
              onValueChange={(v) => { setPageSize(parseInt(v || '20') || 20); setPage(1) }}
              className="w-full rounded-none ring-0 focus:ring-0 shadow-none focus:shadow-none bg-transparent"
            >
              <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="10">10</SelectItem>
              <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="20">20</SelectItem>
              <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="50">50</SelectItem>
              <SelectItem className="border-b border-[hsl(var(--border))] last:border-b-0" value="100">100</SelectItem>
            </Select>
          </div>
        </div>
      </div>

      {/* Add / Edit dialog (simple) */}
      {editOpen && (
        <div className="fixed inset-0 z-[1000]">
          <div className="absolute inset-0 bg-black/40" onClick={()=> setEditOpen(false)} />
          <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[520px] max-w-[95vw] rounded-lg border bg-card p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="text-sm font-medium">{editId ? 'Edit Contact' : 'Add Contact'}</div>
              <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=> setEditOpen(false)}>✕</button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <label className="text-sm">Full name<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={editModel.name||''} onChange={(e)=> setEditModel((m)=> ({ ...m, name: e.target.value }))} /></label>
              <label className="text-sm">Email<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={editModel.email||''} onChange={(e)=> setEditModel((m)=> ({ ...m, email: e.target.value }))} /></label>
              <label className="text-sm">Phone<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={editModel.phone||''} onChange={(e)=> setEditModel((m)=> ({ ...m, phone: e.target.value }))} /></label>
              <div className="text-sm">
                <div>Tags</div>
                <div className="mt-1 flex flex-wrap items-center gap-2 rounded-md border bg-background p-2 min-h-8">
                  {(editModel.tags || []).map((t) => (
                    <span key={t} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-xs">
                      {t}
                      <button type="button" className="opacity-70 hover:opacity-100" onClick={() => setEditModel((m)=> ({ ...m, tags: (m.tags || []).filter((x)=> x !== t) }))}>✕</button>
                    </span>
                  ))}
                  <input
                    className="flex-1 min-w-[120px] h-6 bg-transparent outline-none text-xs"
                    placeholder="Type and press Enter"
                    value={tagInput}
                    onChange={(e)=> setTagInput(e.target.value)}
                    onKeyDown={(e)=> {
                      const k = e.key
                      if (k === 'Enter' || k === 'Tab' || k === ',') {
                        e.preventDefault()
                        const t = tagInput.trim()
                        if (t) {
                          setEditModel((m)=> ({ ...m, tags: Array.from(new Set([...(m.tags || []), t])) }))
                          setTagInput('')
                        }
                      }
                    }}
                    onBlur={() => {
                      const t = tagInput.trim()
                      if (t) {
                        setEditModel((m)=> ({ ...m, tags: Array.from(new Set([...(m.tags || []), t])) }))
                        setTagInput('')
                      }
                    }}
                  />
                </div>
              </div>
            </div>
            <div className="mt-4 flex items-center gap-2">
              <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" onClick={saveEdit}>Save</button>
              <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" onClick={()=> setEditOpen(false)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {/* Bulk Email dialog (Outlook-like simplified) */}
      {bulkEmailOpen && (
        <div className="fixed inset-0 z-[1000]">
          <div className="absolute inset-0 bg-black/40" onClick={()=> setBulkEmailOpen(false)} />
          <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[720px] max-w-[95vw] rounded-lg border bg-card p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="text-sm font-medium">New message</div>
              <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=> setBulkEmailOpen(false)}>✕</button>
            </div>
            <div className="space-y-2 text-sm">
              <div className="text-sm">
                <div>To</div>
                <div className="mt-1 relative">
                  <div className="min-h-9 rounded-md border bg-background p-1 flex flex-wrap gap-1 items-center">
                    {emailToTokens.map((t, i) => (
                      <span key={t.kind+':'+t.value} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-xs">
                        {t.label}
                        <button type="button" className="opacity-70 hover:opacity-100" onClick={()=> setEmailToTokens(prev => prev.filter((_, idx)=> idx!==i))}>✕</button>
                      </span>
                    ))}
                    <input
                      className="flex-1 min-w-[160px] h-7 bg-transparent outline-none text-xs px-2"
                      placeholder="Type name, email, or #tag; press Enter"
                      value={emailToInput}
                      onChange={(e)=> setEmailToInput(e.target.value)}
                      onFocus={()=> setEmailSugOpen(true)}
                      onKeyDown={(e)=> {
                        if (e.key === 'Enter' || e.key === 'Tab' || e.key === ',' || e.key === ';') { e.preventDefault(); tryCommitEmailInput() }
                        if (e.key === 'Backspace' && !emailToInput) { setEmailToTokens(prev => prev.slice(0, -1)) }
                      }}
                      onBlur={()=> setTimeout(()=> setEmailSugOpen(false), 120)}
                    />
                  </div>
                  {emailSugOpen && emailSuggestions.length>0 && (
                    <div className="absolute z-[1001] mt-1 w-full max-h-56 overflow-auto rounded-md shadow suggest-menu">
                      <div className="sticky top-0 z-10 flex items-center justify-between px-2 py-1 border-b bg-[hsl(var(--card))] text-[11px]">
                        <div>{Array.from(emailSel).length} selected</div>
                        <div className="flex items-center gap-2">
                          <button className="px-2 py-0.5 rounded border" onMouseDown={(e)=>e.preventDefault()} onClick={addSelectedEmails}>Add selected</button>
                          <button className="px-2 py-0.5 rounded border" onMouseDown={(e)=>e.preventDefault()} onClick={()=> setEmailSel(new Set())}>Clear</button>
                        </div>
                      </div>
                      {emailSuggestions.map((s, idx) => (
                        <button key={idx} className="w-full text-left text-xs px-2 py-1 hover:bg-[hsl(var(--muted))] inline-flex items-center gap-2" onMouseDown={(e)=> e.preventDefault()} onClick={()=> toggleEmailSel(s)}>
                          <input type="checkbox" readOnly checked={emailSel.has(emailKey(s))} className="h-3 w-3" />
                          <span>{s.label}</span>
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </div>
              <label className="text-sm">Subject<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={bulkEmailDraft.subject} onChange={(e)=> setBulkEmailDraft((d)=> ({ ...d, subject: e.target.value }))} /></label>
              <Tabs.Root defaultValue="body" className="w-full">
                <Tabs.List className="flex items-center gap-2 text-xs mt-2">
                  <Tabs.Trigger value="body" className="px-2 py-1 rounded-md border data-[state=active]:bg-[hsl(var(--muted))]">Body (HTML)</Tabs.Trigger>
                  <Tabs.Trigger value="preview" className="px-2 py-1 rounded-md border data-[state=active]:bg-[hsl(var(--muted))]">Preview</Tabs.Trigger>
                </Tabs.List>
                <Tabs.Content value="body" className="mt-2">
                  <div className="rounded-md bg-background">
                    <div className="px-2 pt-2">
                      <div ref={quillContainerRef} className="min-h-40 max-h-80 overflow-auto text-xs" />
                    </div>
                  </div>
                </Tabs.Content>
                <Tabs.Content value="preview" className="mt-2">
                  <div className="rounded-md border bg-background p-2 max-h-64 overflow-auto">
                    <div className="text-[11px] mb-1 opacity-70">Preview</div>
                    <div className="prose prose-sm max-w-none" dangerouslySetInnerHTML={{ __html: bulkEmailDraft.html || '<div style=\"opacity:.6\">(Empty)</div>' }} />
                  </div>
                </Tabs.Content>
              </Tabs.Root>
              <div className="mt-2 flex items-center gap-2 text-sm">
                <span>Queue in background</span>
                <Switch checked={emailQueue} onChangeAction={setEmailQueue} />
              </div>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-2 mt-2">
                <label className="text-sm">Max/minute (optional)
                  <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" type="number" min={1} placeholder="e.g., 60" value={emailRateLimit} onChange={(e)=> setEmailRateLimit(e.target.value)} />
                </label>
              </div>
              <label className="mt-2 inline-flex items-center gap-2 text-sm">
                <input type="checkbox" checked={emailSendSummary} onChange={(e)=> setEmailSendSummary(e.target.checked)} />
                Email me the summary
              </label>
            </div>
            <div className="mt-3 flex items-center gap-2">
              <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted disabled:opacity-60" disabled={bulkEmailSending} onClick={sendBulkEmail}>{bulkEmailSending ? 'Sending…' : 'Send'}</button>
              <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" onClick={()=> setBulkEmailOpen(false)}>Cancel</button>
            </div>
          </div>
        </div>
      )}

      {/* Bulk SMS dialog */}
      {bulkSmsOpen && (
        <div className="fixed inset-0 z-[1000]">
          <div className="absolute inset-0 bg-black/40" onClick={()=> setBulkSmsOpen(false)} />
          <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[640px] max-w-[95vw] rounded-lg border bg-card p-4">
            <div className="flex items-center justify-between mb-3">
              <div className="text-sm font-medium">New SMS</div>
              <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={()=> setBulkSmsOpen(false)}>✕</button>
            </div>
            <div className="space-y-2 text-sm">
              <div className="text-sm">
                <div>To</div>
                <div className="mt-1 relative">
                  <div className="min-h-9 rounded-md border bg-background p-1 flex flex-wrap gap-1 items-center">
                    {smsToTokens.map((t, i) => (
                      <span key={t.kind+':'+t.value} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-xs">
                        {t.label}
                        <button type="button" className="opacity-70 hover:opacity-100" onClick={()=> setSmsToTokens(prev => prev.filter((_, idx)=> idx!==i))}>✕</button>
                      </span>
                    ))}
                    <input
                      className="flex-1 min-w-[160px] h-7 bg-transparent outline-none text-xs px-2"
                      placeholder="Type name, number, or #tag; press Enter"
                      value={smsToInput}
                      onChange={(e)=> setSmsToInput(e.target.value)}
                      onFocus={()=> setSmsSugOpen(true)}
                      onKeyDown={(e)=> {
                        if (e.key === 'Enter' || e.key === 'Tab' || e.key === ',' || e.key === ';') { e.preventDefault(); tryCommitSmsInput() }
                        if (e.key === 'Backspace' && !smsToInput) { setSmsToTokens(prev => prev.slice(0, -1)) }
                      }}
                      onBlur={()=> setTimeout(()=> setSmsSugOpen(false), 120)}
                    />
                  </div>
                  {smsSugOpen && smsSuggestions.length>0 && (
                    <div className="absolute z-[1001] mt-1 w-full max-h-56 overflow-auto rounded-md shadow suggest-menu">
                      <div className="sticky top-0 z-10 flex items-center justify-between px-2 py-1 border-b bg-[hsl(var(--card))] text-[11px]">
                        <div>{Array.from(smsSel).length} selected</div>
                        <div className="flex items-center gap-2">
                          <button className="px-2 py-0.5 rounded border" onMouseDown={(e)=>e.preventDefault()} onClick={addSelectedSms}>Add selected</button>
                          <button className="px-2 py-0.5 rounded border" onMouseDown={(e)=>e.preventDefault()} onClick={()=> setSmsSel(new Set())}>Clear</button>
                        </div>
                      </div>
                      {smsSuggestions.map((s, idx) => (
                        <button key={idx} className="w-full text-left text-xs px-2 py-1 hover:bg-[hsl(var(--muted))] inline-flex items-center gap-2" onMouseDown={(e)=> e.preventDefault()} onClick={()=> toggleSmsSel(s)}>
                          <input type="checkbox" readOnly checked={smsSel.has(smsKey(s))} className="h-3 w-3" />
                          <span>{s.label}</span>
                        </button>
                      ))}
                    </div>
                  )}
                </div>
              </div>
              <label className="text-sm">Message<textarea className="mt-1 w-full h-36 px-2 py-2 rounded-md border bg-background text-xs" value={bulkSmsDraft.message} onChange={(e)=> setBulkSmsDraft((d)=> ({ ...d, message: e.target.value }))} /></label>
              <div className="mt-2 flex items-center gap-2 text-sm">
                <span>Queue in background</span>
                <Switch checked={smsQueue} onChangeAction={setSmsQueue} />
              </div>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-2 mt-2">
                <label className="text-sm">Max/minute (optional)
                  <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" type="number" min={1} placeholder="e.g., 120" value={smsRateLimit} onChange={(e)=> setSmsRateLimit(e.target.value)} />
                </label>
              </div>
              <label className="mt-2 inline-flex items-center gap-2 text-sm">
                <input type="checkbox" checked={smsSendSummary} onChange={(e)=> setSmsSendSummary(e.target.checked)} />
                Email me the summary
              </label>
            </div>
            <div className="mt-3 flex items-center gap-2">
              <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted disabled:opacity-60" disabled={bulkSmsSending} onClick={sendBulkSms}>{bulkSmsSending ? 'Sending…' : 'Send'}</button>
              <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" onClick={()=> setBulkSmsOpen(false)}>Cancel</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
