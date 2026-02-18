"use client"

import React, { useEffect, useMemo, useState } from 'react'
import { Api, type AlertOut, type AlertCreate, type AlertConfig, type AlertRunOut } from '@/lib/api'

function parseCron(cron?: string) {
  try {
    const s = String(cron || '')
    const parts = s.trim().split(/\s+/)
    if (parts.length < 5) return { hh: '09', mm: '00', dows: [1,2,3,4,5] as number[], doms: [] as number[], mode: 'weekly' as const }
    const mm = parts[0]
    const hh = parts[1]
    const domPart = (parts[2] || '*').trim()
    const dowPart = (parts[4] || '*').trim()
    const doms = domPart === '*' ? [] : domPart.split(',').map((x)=>parseInt(x,10)).filter((n)=>!isNaN(n) && n>=1 && n<=31)
    const dows = dowPart === '*' ? [] : dowPart.split(',').map((x) => parseInt(x, 10)).filter((n) => !isNaN(n) && n>=0 && n<=6)
    const mode = doms.length ? 'monthly' as const : 'weekly' as const
    const wk = dows.length ? dows : [1,2,3,4,5]
    return { hh: String(hh).padStart(2,'0'), mm: String(mm).padStart(2,'0'), dows: wk, doms, mode }
  } catch { return { hh: '09', mm: '00', dows: [1,2,3,4,5] as number[], doms: [] as number[], mode: 'weekly' as const } }
}

function buildCron(time: string, opts: { mode: 'weekly'|'monthly'; dows: number[]; doms: number[] }) {
  try {
    const [hh, mm] = time.split(':').map((t)=>parseInt(t,10));
    if (opts.mode === 'monthly') {
      const domList = (opts.doms||[]).join(',') || '*'
      return `${isNaN(mm)?0:mm} ${isNaN(hh)?0:hh} ${domList} * *`
    }
    const dowList = (opts.dows||[]).join(',') || '*'
    return `${isNaN(mm)?0:mm} ${isNaN(hh)?0:hh} * * ${dowList}`
  } catch { return '0 9 * * 1,2,3,4,5' }
}

export default function AlertEditDialog({ open, alert, onCloseAction, onSavedAction }: { open: boolean; alert: AlertOut | null; onCloseAction: () => void; onSavedAction: (a: AlertOut) => void }) {
  const [name, setName] = useState('')
  const [kind, setKind] = useState<'alert'|'notification'>('alert')
  const [enabled, setEnabled] = useState(true)
  // Tokenized recipients (email + #tag) and (phone + #tag)
  type RecipientToken = { kind: 'email'|'phone'|'tag'; label: string; value: string }
  const [emailToTokens, setEmailToTokens] = useState<RecipientToken[]>([])
  const [emailToInput, setEmailToInput] = useState('')
  const [emailSuggestions, setEmailSuggestions] = useState<Array<{ type: 'contact'|'tag'; label: string; email?: string; tag?: string }>>([])
  const [emailSugOpen, setEmailSugOpen] = useState(false)
  const [smsToTokens, setSmsToTokens] = useState<RecipientToken[]>([])
  const [smsToInput, setSmsToInput] = useState('')
  const [smsSuggestions, setSmsSuggestions] = useState<Array<{ type: 'contact'|'tag'; label: string; phone?: string; tag?: string }>>([])
  const [smsSugOpen, setSmsSugOpen] = useState(false)
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
  function isValidEmail(s: string): boolean { return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test((s||'').trim()) }
  function isValidPhone(s: string): boolean { return /^[+]?[-(). \d]{6,}$/.test((s||'').trim()) }
  function addEmailToken(t: RecipientToken) {
    const key = `${t.kind}:${t.value.toLowerCase()}`
    setEmailToTokens(prev => prev.some(x => `${x.kind}:${x.value.toLowerCase()}`===key) ? prev : [...prev, t])
  }
  function addSmsToken(t: RecipientToken) {
    const key = `${t.kind}:${t.value.toLowerCase()}`
    setSmsToTokens(prev => prev.some(x => `${x.kind}:${x.value.toLowerCase()}`===key) ? prev : [...prev, t])
  }
  function tryCommitEmailInput() {
    const raw = (emailToInput||'').trim().replace(/[;,]+$/,'')
    if (!raw) return
    if (isValidEmail(raw)) addEmailToken({ kind: 'email', label: raw, value: raw })
    else if (raw.startsWith('#')) addEmailToken({ kind: 'tag', label: raw, value: raw.slice(1) })
    else addEmailToken({ kind: 'tag', label: `#${raw}`, value: raw })
    setEmailToInput(''); setEmailSugOpen(false)
  }
  function tryCommitSmsInput() {
    const raw = (smsToInput||'').trim().replace(/[;,]+$/,'')
    if (!raw) return
    if (isValidPhone(raw)) addSmsToken({ kind: 'phone', label: raw, value: raw })
    else if (raw.startsWith('#')) addSmsToken({ kind: 'tag', label: raw, value: raw.slice(1) })
    else addSmsToken({ kind: 'tag', label: `#${raw}`, value: raw })
    setSmsToInput(''); setSmsSugOpen(false)
  }
  const [template, setTemplate] = useState('')
  const [renderMode, setRenderMode] = useState<'kpi'|'table'|'chart'|'report'>('kpi')
  const [attachPdf, setAttachPdf] = useState<boolean>(false)
  const [triggerType, setTriggerType] = useState<'threshold'|'time'>('threshold')
  const [operator, setOperator] = useState('>')
  const [value, setValue] = useState('0')
  const [timeOfDay, setTimeOfDay] = useState('09:00')
  const [daysOfWeek, setDaysOfWeek] = useState<number[]>([1,2,3,4,5])
  const [daysOfMonth, setDaysOfMonth] = useState<number[]>([])
  const [scheduleKind, setScheduleKind] = useState<'weekly'|'monthly'>('weekly')
  const [runs, setRuns] = useState<AlertRunOut[]>([])
  const [previewHtml, setPreviewHtml] = useState<string>('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  // Advanced KPI editor (optional override or when no widget is present)
  const [advOpen, setAdvOpen] = useState(false)
  const [advDatasourceId, setAdvDatasourceId] = useState<string>('')
  const [advSource, setAdvSource] = useState<string>('')
  const [advAgg, setAdvAgg] = useState<'count'|'sum'|'avg'|'min'|'max'|'distinct'>('count')
  const [advMeasure, setAdvMeasure] = useState<string>('')
  const [advWhere, setAdvWhere] = useState<string>('')
  const [advXField, setAdvXField] = useState<string>('')
  const [advXValue, setAdvXValue] = useState<string>('')

  useEffect(() => {
    if (!open || !alert) return
    setName(alert.name)
    setKind(alert.kind)
    setEnabled(!!alert.enabled)
    const cfg = alert.config || {} as any
    const acts = Array.isArray(cfg.actions) ? cfg.actions : []
    const email = acts.find((a: any) => String(a?.type) === 'email') || {}
    const sms = acts.find((a: any) => String(a?.type) === 'sms') || {}
    try {
      const emails = Array.isArray(email.to) ? (email.to as string[]).filter(Boolean) : []
      setEmailToTokens(emails.map(v => ({ kind: 'email', label: v, value: v })))
    } catch { setEmailToTokens([]) }
    try {
      const phones = Array.isArray(sms.to) ? (sms.to as string[]).filter(Boolean) : []
      setSmsToTokens(phones.map(v => ({ kind: 'phone', label: v, value: v })))
    } catch { setSmsToTokens([]) }
    setEmailToInput(''); setSmsToInput(''); setEmailSugOpen(false); setSmsSugOpen(false)
    setTemplate(String(cfg.template || ''))
    const r = cfg.render || { mode: 'kpi' }
    setRenderMode((r.mode || 'kpi') as any)
    try { setAttachPdf(!!((Array.isArray(cfg.actions) ? cfg.actions : []).find((a: any) => String(a?.type) === 'email') || {}).attachPdf) } catch {}
    const t0 = Array.isArray(cfg.triggers) ? cfg.triggers[0] : null
    if (t0 && String(t0.type) === 'time') {
      setTriggerType('time')
      const parsed = parseCron(t0.cron)
      setTimeOfDay(`${parsed.hh}:${parsed.mm}`)
      setDaysOfWeek(parsed.dows)
      setDaysOfMonth(parsed.doms || [])
      setScheduleKind(parsed.mode)
    } else {
      setTriggerType('threshold')
      setOperator(String(t0?.operator || '>'))
      const v = (t0?.value != null && Array.isArray(t0.value)) ? (t0.value as any[]).join(',') : String(t0?.value ?? '0')
      setValue(v)
    }
    // Prefill Advanced editor from existing config (if threshold exists)
    try {
      setAdvDatasourceId(String(cfg.datasourceId || ''))
      if (t0 && String(t0.type) === 'threshold') {
        setAdvSource(String(t0.source || ''))
        setAdvAgg(String(t0.aggregator || 'count') as any)
        setAdvMeasure(String(t0.measure || ''))
        setAdvWhere((t0.where && typeof t0.where === 'object') ? JSON.stringify(t0.where) : '')
        setAdvXField(String(t0.xField || ''))
        setAdvXValue(String(t0.xValue ?? ''))
      } else {
        setAdvSource(''); setAdvAgg('count'); setAdvMeasure(''); setAdvWhere(''); setAdvXField(''); setAdvXValue('')
      }
    } catch {}
    // Load runs
    try {
      (async () => { if (alert) { const rs = await Api.listAlertRuns(alert.id, 50); setRuns(Array.isArray(rs)?rs:[]) } })()
    } catch {}
  }, [open, alert?.id])

  // Live suggestions for recipients (email)
  useEffect(() => {
    if (!open) return
    const q = (emailToInput || '').trim()
    if (!q) { setEmailSuggestions([]); return }
    const h = setTimeout(async () => {
      try {
        const res = await Api.listContacts({ search: q, active: true, page: 1, pageSize: 50 })
        const out: Array<{ type: 'contact'|'tag'; label: string; email?: string; tag?: string }> = []
        ;(res.items || []).forEach(c => { const em=(c.email||'').trim(); if (em) out.push({ type:'contact', label: `${c.name} <${em}>`, email: em }) })
        const tags = Array.from(new Set((res.items || []).flatMap(c => (c.tags || []))))
        tags.filter(t => t.toLowerCase().includes(q.toLowerCase())).forEach(t => out.push({ type:'tag', label: `#${t}`, tag: t }))
        setEmailSuggestions(out.slice(0, 50)); setEmailSugOpen(true)
      } catch { setEmailSuggestions([]) }
    }, 150)
    return () => clearTimeout(h)
  }, [emailToInput, open])

  // Live suggestions for recipients (sms)
  useEffect(() => {
    if (!open) return
    const q = (smsToInput || '').trim()
    if (!q) { setSmsSuggestions([]); return }
    const h = setTimeout(async () => {
      try {
        const res = await Api.listContacts({ search: q, active: true, page: 1, pageSize: 50 })
        const out: Array<{ type: 'contact'|'tag'; label: string; phone?: string; tag?: string }> = []
        ;(res.items || []).forEach(c => { const ph=(c.phone||'').trim(); if (ph) out.push({ type:'contact', label: `${c.name} <${ph}>`, phone: ph }) })
        const tags = Array.from(new Set((res.items || []).flatMap(c => (c.tags || []))))
        tags.filter(t => t.toLowerCase().includes(q.toLowerCase())).forEach(t => out.push({ type:'tag', label: `#${t}`, tag: t }))
        setSmsSuggestions(out.slice(0, 50)); setSmsSugOpen(true)
      } catch { setSmsSuggestions([]) }
    }, 150)
    return () => clearTimeout(h)
  }, [smsToInput, open])

  const buildPayload = (emails: string[], phones: string[]): AlertCreate => {
    const cfgIn = (alert?.config || {}) as any
    const triggers: any[] = []
    if (triggerType === 'time') {
      triggers.push({ type: 'time', cron: buildCron(timeOfDay, { mode: scheduleKind, dows: daysOfWeek, doms: daysOfMonth }) })
    } else {
      const tPrev = Array.isArray(cfgIn.triggers) ? cfgIn.triggers.find((t: any) => String(t?.type) === 'threshold') : null
      // If Advanced editor is open, use those values; otherwise keep previous (widget-carried) settings
      if (advOpen) {
        let whereObj: any = undefined
        try { whereObj = advWhere.trim() ? JSON.parse(advWhere) : undefined } catch { whereObj = undefined }
        const xValCoerced = advXValue.trim() === '' ? undefined : (isNaN(Number(advXValue)) ? advXValue : Number(advXValue))
        triggers.push({
          type: 'threshold',
          source: advSource || String(tPrev?.source || ''),
          aggregator: advAgg || (tPrev?.aggregator || 'count'),
          measure: (advAgg === 'count') ? undefined : (advMeasure || tPrev?.measure),
          where: whereObj ?? tPrev?.where,
          xField: advXField || tPrev?.xField,
          xValue: xValCoerced ?? tPrev?.xValue,
          operator,
          value: value.includes(',') ? value.split(',').map((s)=>Number(s)) : Number(value),
        })
      } else {
        const t: any = {
          type: 'threshold',
          source: String(tPrev?.source || ''),
          aggregator: tPrev?.aggregator || 'sum',
          measure: tPrev?.measure,
          where: tPrev?.where,
          xField: tPrev?.xField,
          xValue: tPrev?.xValue,
          operator,
          value: value.includes(',') ? value.split(',').map((s)=>Number(s)) : Number(value),
        }
        triggers.push(t)
      }
    }
    const actions: any[] = []
    if (emails.length) actions.push({ type: 'email', to: emails, subject: name, ...(attachPdf ? { attachPdf: true } : {}) })
    if (phones.length) actions.push({ type: 'sms', to: phones, message: template || name })
    const prevRender = (cfgIn.render || {}) as Record<string, any>
    const renderBase: Record<string, any> = {}
    if (prevRender.widgetRef) renderBase.widgetRef = prevRender.widgetRef
    if (prevRender.width) renderBase.width = prevRender.width
    if (prevRender.height) renderBase.height = prevRender.height
    if (prevRender.theme) renderBase.theme = prevRender.theme
    const render = kind === 'notification'
      ? (renderMode === 'report' ? { ...renderBase, mode: 'report', querySpec: prevRender.querySpec }
        : renderMode === 'table' ? { ...renderBase, mode: 'table', querySpec: prevRender.querySpec }
        : renderMode === 'chart' ? { ...renderBase, mode: 'chart', url: prevRender.url || '' }
        : { ...renderBase, mode: 'kpi', label: prevRender.label || name })
      : { ...renderBase, mode: 'kpi', label: prevRender.label || name }
    const cfg: AlertConfig = { datasourceId: (advOpen ? (advDatasourceId || cfgIn.datasourceId) : cfgIn.datasourceId), triggers, actions, render, template }
    if (cfgIn.triggersGroup) cfg.triggersGroup = cfgIn.triggersGroup
    if (cfgIn.customPlaceholders) cfg.customPlaceholders = cfgIn.customPlaceholders as any
    // Preserve any extra top-level config keys from the original config
    for (const k of Object.keys(cfgIn)) { if (!(k in cfg)) (cfg as any)[k] = (cfgIn as any)[k] }
    const payload: AlertCreate = { name, kind, widgetId: (alert as any)?.widgetId || undefined, dashboardId: (alert as any)?.dashboardId || undefined, enabled, config: cfg }
    return payload
  }

  const onSave = async () => {
    try {
      if (!alert) return
      setSaving(true); setError(null)
      const { emails, phones } = await resolveRecipientLists()
      const payload = buildPayload(emails, phones)
      const res = await Api.updateAlert(alert.id, payload)
      onSavedAction(res)
      onCloseAction()
    } catch (e: any) { setError(e?.message || 'Failed to save') } finally { setSaving(false) }
  }

  const onTestEvaluate = async () => {
    try {
      setError(null)
      const { emails, phones } = await resolveRecipientLists()
      const cfg = buildPayload(emails, phones).config
      const res = await Api.evaluateAlert({ name, dashboardId: (alert as any)?.dashboardId, config: cfg })
      setPreviewHtml(res?.html || '')
    } catch (e: any) {
      setError(e?.message || 'Failed to evaluate')
    }
  }

  // Expand tokens (including #tag) into email and phone arrays
  async function resolveRecipientLists(): Promise<{ emails: string[]; phones: string[] }> {
    const eTokens = [...emailToTokens]
    const eRaw = (emailToInput||'').trim().replace(/[;,]+$/,'')
    if (eRaw) { if (isValidEmail(eRaw)) eTokens.push({ kind:'email', label:eRaw, value:eRaw }); else if (eRaw.startsWith('#')) eTokens.push({ kind:'tag', label:eRaw, value:eRaw.slice(1) }); else eTokens.push({ kind:'tag', label:`#${eRaw}`, value:eRaw }) }
    const sTokens = [...smsToTokens]
    const sRaw = (smsToInput||'').trim().replace(/[;,]+$/,'')
    if (sRaw) { if (isValidPhone(sRaw)) sTokens.push({ kind:'phone', label:sRaw, value:sRaw }); else if (sRaw.startsWith('#')) sTokens.push({ kind:'tag', label:sRaw, value:sRaw.slice(1) }); else sTokens.push({ kind:'tag', label:`#${sRaw}`, value:sRaw }) }
    const emailSet = new Set<string>(eTokens.filter(t=>t.kind==='email').map(t=>t.value))
    const phoneSet = new Set<string>(sTokens.filter(t=>t.kind==='phone').map(t=>t.value))
    const tagSet = new Set<string>([...eTokens, ...sTokens].filter(t=>t.kind==='tag').map(t=>t.value))
    // Expand tags via Contacts API (best effort, first page sufficient for common cases)
    for (const tag of tagSet) {
      try {
        const res = await Api.listContacts({ tags: [tag], active: true, page: 1, pageSize: 200 })
        ;(res.items || []).forEach(c => { const em=(c.email||'').trim(); const ph=(c.phone||'').trim(); if (em) emailSet.add(em); if (ph) phoneSet.add(ph) })
      } catch {}
    }
    // Validate
    const invalidEmail = [...emailSet].find(e => !isValidEmail(e))
    const invalidPhone = [...phoneSet].find(p => !isValidPhone(p))
    if (invalidEmail) throw new Error(`Invalid email: ${invalidEmail}`)
    if (invalidPhone) throw new Error(`Invalid phone: ${invalidPhone}`)
    return { emails: [...emailSet], phones: [...phoneSet] }
  }

  if (!open || !alert) return null
  return (
    <div className="fixed inset-0 z-[1200]">
      <div className="absolute inset-0 bg-black/40" onClick={() => !saving && onCloseAction()} />
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[880px] max-w-[95vw] max-h-[90vh] overflow-auto rounded-lg border bg-background p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="text-sm font-medium">Edit Alert</div>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCloseAction} disabled={saving}>✕</button>
        </div>
        {error && <div className="mb-2 text-xs text-rose-600">{error}</div>}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
          <label className="text-sm">Name<input className="mt-1 w-full h-8 px-2 rounded-md border bg-card" value={name} onChange={(e)=>setName(e.target.value)} /></label>
          <label className="text-sm">Type<select className="mt-1 w-full h-8 px-2 rounded-md border bg-card" value={kind} onChange={(e)=>setKind(e.target.value as any)}><option value="alert">Alert</option><option value="notification">Notification</option></select></label>
          <label className="text-sm inline-flex items-center gap-2 mt-6"><input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={enabled} onChange={(e)=>setEnabled(e.target.checked)} /><span>Enabled</span></label>
          <div className="text-sm col-span-1 md:col-span-2">
            <div>Email recipients</div>
            <div className="mt-1 relative">
              <div className="min-h-9 rounded-md border bg-card p-1 flex flex-wrap gap-1 items-center">
                {emailToTokens.map((t, i) => (
                  <span key={`${t.kind}:${t.value}`} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-xs">
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
                  onKeyDown={(e)=> { if (e.key==='Enter'||e.key==='Tab'||e.key===','||e.key===';') { e.preventDefault(); tryCommitEmailInput() } if (e.key==='Backspace' && !emailToInput) setEmailToTokens(prev => prev.slice(0,-1)) }}
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
          <div className="text-sm">
            <div>SMS recipients</div>
            <div className="mt-1 relative">
              <div className="min-h-9 rounded-md border bg-card p-1 flex flex-wrap gap-1 items-center">
                {smsToTokens.map((t, i) => (
                  <span key={`${t.kind}:${t.value}`} className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full border text-xs">
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
                  onKeyDown={(e)=> { if (e.key==='Enter'||e.key==='Tab'||e.key===','||e.key===';') { e.preventDefault(); tryCommitSmsInput() } if (e.key==='Backspace' && !smsToInput) setSmsToTokens(prev => prev.slice(0,-1)) }}
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
          <label className="text-sm md:col-span-3">Template<input className="mt-1 w-full h-8 px-2 rounded-md border bg-card" placeholder="Current KPI value: {{kpi}}" value={template} onChange={(e)=>setTemplate(e.target.value)} /></label>
        </div>
        <div className="mt-4">
          <div className="text-sm font-medium mb-2">Trigger</div>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
            <label className="text-sm">Type<select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={triggerType} onChange={(e)=>setTriggerType(e.target.value as any)}><option value="threshold">Threshold</option><option value="time">Time of day</option></select></label>
            {triggerType === 'threshold' ? (
              <>
                <label className="text-sm">Operator<select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={operator} onChange={(e)=>setOperator(e.target.value)}><option value=">">&gt;</option><option value=">=">&gt;=</option><option value="<">&lt;</option><option value="<=">&lt;=</option><option value="==">==</option><option value="between">between (enter A,B)</option></select></label>
                <label className="text-sm">Value<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder={operator==='between'? 'A,B' : 'number'} value={value} onChange={(e)=>setValue(e.target.value)} /></label>
              </>
            ) : (
              <>
                <label className="text-sm">Time (HH:mm)<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="09:00" value={timeOfDay} onChange={(e)=>setTimeOfDay(e.target.value)} /></label>
                <div className="text-sm">
                  <div className="mb-1">Schedule</div>
                  <div className="inline-flex rounded-md border overflow-hidden">
                    <button type="button" className={`px-2 py-1 text-xs ${scheduleKind==='weekly'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setScheduleKind('weekly')}>Weekly</button>
                    <button type="button" className={`px-2 py-1 text-xs border-l ${scheduleKind==='monthly'?'bg-[hsl(var(--muted))]':''}`} onClick={()=>setScheduleKind('monthly')}>Monthly</button>
                  </div>
                </div>
                {scheduleKind === 'weekly' ? (
                  <div className="md:col-span-2">
                    <div className="text-sm">Days of week</div>
                    <div className="mt-1 flex flex-wrap gap-1 text-xs">
                      {[{v:0,l:'Sun'},{v:1,l:'Mon'},{v:2,l:'Tue'},{v:3,l:'Wed'},{v:4,l:'Thu'},{v:5,l:'Fri'},{v:6,l:'Sat'}].map(d => (
                        <button key={d.v} type="button" className={`px-2 py-1 rounded-md border ${daysOfWeek.includes(d.v)?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setDaysOfWeek((prev)=> prev.includes(d.v) ? prev.filter(x=>x!==d.v) : [...prev, d.v].sort((a,b)=>a-b))}>{d.l}</button>
                      ))}
                    </div>
                  </div>
                ) : (
                  <div className="md:col-span-2">
                    <div className="text-sm">Days of month</div>
                    <div className="mt-1 grid grid-cols-7 gap-1 text-xs">
                      {Array.from({ length: 31 }).map((_,i)=> i+1).map((d)=> (
                        <button key={d} type="button" className={`px-2 py-1 rounded-md border ${daysOfMonth.includes(d)?'bg-[hsl(var(--muted))]':''}`} onClick={()=> setDaysOfMonth((prev)=> prev.includes(d) ? prev.filter(x=>x!==d) : [...prev, d].sort((a,b)=>a-b))}>{d}</button>
                      ))}
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
        <div className="mt-4">
          <div className="text-sm font-medium mb-2">Render</div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <label className="text-sm">Mode<select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={renderMode} onChange={(e)=>setRenderMode(e.target.value as any)}><option value="kpi">KPI</option><option value="table">Table</option><option value="chart">Chart (link)</option><option value="report">Report</option></select></label>
            {renderMode === 'report' && (
              <label className="text-sm inline-flex items-center gap-2 mt-5"><input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={attachPdf} onChange={(e)=> setAttachPdf(e.target.checked)} /> Attach PDF</label>
            )}
          </div>
        </div>

        {/* Inline test evaluate */}
        <div className="mt-4">
          <div className="flex items-center justify-between mb-2">
            <div className="text-sm font-medium">Inline Test</div>
            <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onTestEvaluate}>Test evaluate</button>
          </div>
          {!!previewHtml && (
            <div className="rounded-md border bg-white overflow-auto" style={{ minHeight: 120 }}>
              <iframe title="alert-eval-preview" className="w-full h-[260px]" srcDoc={previewHtml} />
            </div>
          )}
        </div>

        {/* Carried KPI summary (from current config) */}
        <div className="mt-4">
          <div className="text-sm font-medium mb-2">Current KPI (from config)</div>
          <div className="text-xs rounded-md border p-2 bg-[hsl(var(--card))]">
            {(() => {
              try {
                const cfg: any = (alert?.config || {}) as any
                const t0 = Array.isArray(cfg.triggers) ? (cfg.triggers.find((t: any) => String(t?.type) === 'threshold') || null) : null
                if (!t0) return <div>No threshold defined.</div>
                return (
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-2">
                    <div>Datasource: <span className="font-mono">{String(cfg.datasourceId || '—')}</span></div>
                    <div>Source: <span className="font-mono">{String(t0.source || '—')}</span></div>
                    <div>Aggregator: <span className="font-mono">{String(t0.aggregator || '—')}</span></div>
                    <div>Measure: <span className="font-mono">{String(t0.measure || '—')}</span></div>
                    <div>X Field: <span className="font-mono">{String(t0.xField || '—')}</span></div>
                    <div>X Value: <span className="font-mono">{String(t0.xValue ?? '—')}</span></div>
                    <div className="md:col-span-3">Where: <span className="font-mono break-all">{t0.where ? JSON.stringify(t0.where) : '—'}</span></div>
                  </div>
                )
              } catch { return <div>—</div> }
            })()}
          </div>
        </div>

        {/* Advanced KPI editor */}
        <div className="mt-4">
          <div className="flex items-center justify-between mb-2">
            <div className="text-sm font-medium">Advanced (define/override KPI)</div>
            <label className="text-xs inline-flex items-center gap-2"><input type="checkbox" className="h-3 w-3 accent-[hsl(var(--primary))]" checked={advOpen} onChange={(e)=>setAdvOpen(e.target.checked)} /> Enable</label>
          </div>
          {advOpen && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 text-sm">
              <label>Datasource ID<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="datasource id" value={advDatasourceId} onChange={(e)=>setAdvDatasourceId(e.target.value)} /></label>
              <label>Table / Source<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="schema.table or table" value={advSource} onChange={(e)=>setAdvSource(e.target.value)} /></label>
              <label>Aggregator<select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={advAgg} onChange={(e)=>setAdvAgg(e.target.value as any)}><option value="count">count</option><option value="sum">sum</option><option value="avg">avg</option><option value="min">min</option><option value="max">max</option><option value="distinct">distinct</option></select></label>
              <label className="md:col-span-2">Measure<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="column (omit for count)" value={advMeasure} onChange={(e)=>setAdvMeasure(e.target.value)} disabled={advAgg==='count'} /></label>
              <label>X Field<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="optional, category/date field" value={advXField} onChange={(e)=>setAdvXField(e.target.value)} /></label>
              <label>X Value<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="value to target (optional)" value={advXValue} onChange={(e)=>setAdvXValue(e.target.value)} /></label>
              <label className="md:col-span-3">Filters JSON (where)
                <textarea className="mt-1 w-full h-20 px-2 py-2 rounded-md border bg-background font-mono text-[12px]" placeholder='{"status":"paid"}' value={advWhere} onChange={(e)=>setAdvWhere(e.target.value)} />
              </label>
              <div className="md:col-span-3 text-xs text-muted-foreground">Advanced overrides will be used for evaluation, save, and sending.</div>
            </div>
          )}
        </div>

        {/* Runs history */}
        <div className="mt-4">
          <div className="text-sm font-medium mb-2">Runs</div>
          <div className="overflow-auto rounded-md border bg-[hsl(var(--card))]">
            <table className="min-w-full text-sm">
              <thead className="bg-[hsl(var(--muted))]">
                <tr>
                  <th className="text-left px-2 py-1 font-medium">Started</th>
                  <th className="text-left px-2 py-1 font-medium">Finished</th>
                  <th className="text-left px-2 py-1 font-medium">Status</th>
                  <th className="text-left px-2 py-1 font-medium">Message</th>
                </tr>
              </thead>
              <tbody>
                {runs.length ? runs.map((r, idx) => (
                  <tr key={r.id} className={`border-t ${idx % 2 === 1 ? 'bg-[hsl(var(--muted))]/20' : ''}`}>
                    <td className="px-2 py-1">{r.startedAt ? new Date(r.startedAt).toLocaleString() : '—'}</td>
                    <td className="px-2 py-1">{r.finishedAt ? new Date(r.finishedAt).toLocaleString() : '—'}</td>
                    <td className="px-2 py-1">{r.status || '—'}</td>
                    <td className="px-2 py-1 truncate max-w-[420px]" title={r.message || ''}>{r.message || '—'}</td>
                  </tr>
                )) : (
                  <tr><td className="px-2 py-2" colSpan={4}>No runs yet.</td></tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
        <div className="mt-4 flex items-center gap-2"><button className="text-xs px-3 py-2 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" disabled={saving} onClick={onSave}>{saving ? 'Saving…' : 'Save'}</button><button className="text-xs px-3 py-2 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" disabled={saving} onClick={onCloseAction}>Cancel</button></div>
      </div>
    </div>
  )
}
