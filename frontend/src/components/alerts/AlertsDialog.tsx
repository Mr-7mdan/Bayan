"use client"

import React, { useEffect, useMemo, useState } from 'react'
import { createPortal } from 'react-dom'
import type { WidgetConfig } from '@/types/widgets'
import { Api, type AlertCreate, type AlertConfig } from '@/lib/api'
import type { ContactOut } from '@/lib/api'

export default function AlertsDialog({ open, onCloseAction, widget }: { open: boolean; onCloseAction: () => void; widget: WidgetConfig }) {
  const [name, setName] = useState<string>('')
  const [kind, setKind] = useState<'alert'|'notification'>('alert')
  const [emailTo, setEmailTo] = useState<string>('')
  const [smsTo, setSmsTo] = useState<string>('')
  const [triggerType, setTriggerType] = useState<'threshold'|'time'>('threshold')
  const [operator, setOperator] = useState<string>('>')
  const [value, setValue] = useState<string>('0')
  const [timeOfDay, setTimeOfDay] = useState<string>('09:00')
  const [daysOfWeek, setDaysOfWeek] = useState<Array<number>>([1,2,3,4,5])
  const [template, setTemplate] = useState<string>('')
  const [renderMode, setRenderMode] = useState<'kpi'|'table'|'chart'>('kpi')
  const [saving, setSaving] = useState<boolean>(false)
  const [error, setError] = useState<string| null>(null)
  const [emailBaseTpl, setEmailBaseTpl] = useState<string>('')
  const [emailLogoUrl, setEmailLogoUrl] = useState<string>('')
  const [previewSubject, setPreviewSubject] = useState<string>('')
  // Advanced threshold controls
  const [aggSel, setAggSel] = useState<string>('count')
  const [measureSel, setMeasureSel] = useState<string>('')
  const [xValueSel, setXValueSel] = useState<string>('')
  const [testHtml, setTestHtml] = useState<string>('')
  // Recipients: contacts and datasource-sourced values
  const [contactsSearch, setContactsSearch] = useState<string>('')
  const [contactsResults, setContactsResults] = useState<ContactOut[]>([])
  const [selectedContacts, setSelectedContacts] = useState<ContactOut[]>([])
  const [emailSource, setEmailSource] = useState<string>('')
  const [emailField, setEmailField] = useState<string>('')
  const [emailCandidates, setEmailCandidates] = useState<string[]>([])
  const [emailSelected, setEmailSelected] = useState<Record<string, boolean>>({})
  const [emailLoading, setEmailLoading] = useState<boolean>(false)
  const [phoneSource, setPhoneSource] = useState<string>('')
  const [phoneField, setPhoneField] = useState<string>('')
  const [phoneCandidates, setPhoneCandidates] = useState<string[]>([])
  const [phoneSelected, setPhoneSelected] = useState<Record<string, boolean>>({})
  const [phoneLoading, setPhoneLoading] = useState<boolean>(false)

  const spec: any = (widget as any)?.querySpec || {}
  const datasourceId = (widget as any)?.datasourceId as string | undefined

  useEffect(() => {
    if (!open) return
    setName((widget as any)?.title || 'New Alert')
    setKind('alert')
    setEmailTo('')
    setSmsTo('')
    setTriggerType('threshold')
    setOperator('>')
    setValue('0')
    setTemplate('')
    setRenderMode('kpi')
    // Prefill template with KPI placeholder
    setTemplate('Current KPI value: {{kpi}}')
    setPreviewSubject((widget as any)?.title || 'Notification')
    // Initialize threshold defaults from widget spec
    try {
      const s: any = spec || {}
      const defaultAgg = (s?.agg && s.agg !== 'none') ? String(s.agg) : ((s?.measure || s?.y) ? 'sum' : 'count')
      const firstSeriesY = (Array.isArray(s?.series) && s.series.length) ? (s.series[0]?.y || '') : ''
      const defaultMeasure = (s?.measure || s?.y || firstSeriesY || '') as string
      setAggSel(defaultAgg)
      setMeasureSel(defaultMeasure)
      setXValueSel('')
      setTestHtml('')
    } catch {}
    ;(async () => { try { const cfg = await Api.getEmailConfig(); setEmailBaseTpl((cfg as any)?.baseTemplateHtml || ''); setEmailLogoUrl((cfg as any)?.logoUrl || '') } catch {} })()
  }, [open, widget?.id])

  // Contacts search (debounced simple fetch)
  useEffect(() => {
    let stop = false
    const t = setTimeout(async () => {
      try {
        const s = (contactsSearch || '').trim()
        if (!s) { setContactsResults([]); return }
        const res = await Api.listContacts({ search: s, page: 1, pageSize: 8 })
        if (!stop) setContactsResults(res.items || [])
      } catch {}
    }, 200)
    return () => { stop = true; clearTimeout(t) }
  }, [contactsSearch])

  const cronFromTime = (time: string, dows: number[]): string => {
    // time HH:mm; dows 0-6 (Sun-Sat); build "m H * * dowlist"
    try {
      const [hh, mm] = time.split(':').map((t) => parseInt(t, 10))
      const list = (dows && dows.length) ? dows.join(',') : '*'
      return `${isNaN(mm)?0:mm} ${isNaN(hh)?0:hh} * * ${list}`
    } catch { return '0 9 * * 1,2,3,4,5' }
  }

  const onTestEvaluate = async () => {
    try {
      setError(null)
      setTestHtml('')
      const payload = buildPayload()
      const res = await Api.evaluateAlert({ name: payload.name, dashboardId: (widget as any)?.dashboardId, config: payload.config })
      setTestHtml(res?.html || '')
    } catch (e: any) {
      setError(e?.message || 'Failed to evaluate')
    }
  }

  const defaultTemplate = (logo?: string) => {
    const lu = (logo || '').trim()
    const logoImg = lu ? `<img src='${lu}' alt='Logo' style='max-height:40px;display:block'/>` : ''
    return `<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>{{subject}}</title>
  <style>
    body{margin:0;padding:0;background:#f7f7f8;color:#111827;font-family:Inter,Arial,sans-serif;}
    .wrap{width:100%;padding:24px 0;}
    .container{max-width:640px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;box-shadow:0 1px 2px rgba(0,0,0,0.04);overflow:hidden;}
    .header{padding:16px 20px;border-bottom:1px solid #e5e7eb;background:#fafafa;display:flex;align-items:center;gap:12px;}
    .brand{font-size:14px;font-weight:600;color:#111827;}
    .content{padding:20px;}
    .footer{padding:16px 20px;border-top:1px solid #e5e7eb;color:#6b7280;font-size:12px;background:#fafafa;}
  </style>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap' rel='stylesheet'>
  <style> @media (prefers-color-scheme: dark) { body{background:#0b0f15;color:#e5e7eb} .container{background:#0f1720;border-color:#1f2937} .header{background:#0f1720;border-color:#1f2937} .footer{background:#0f1720;border-color:#1f2937;color:#9ca3af} } </style>
  <style> img{border:0;} a{color:#2563eb;text-decoration:none} .logo{display:flex;align-items:center;gap:12px} .subject{font-size:14px;font-weight:600} table{border-collapse:collapse} th,td{border:1px solid #e5e7eb;padding:6px} th{background:#f3f4f6} .card{border:1px solid #e5e7eb;border-radius:10px;padding:16px} </style>
</head>
<body>
  <div class='wrap'>
    <div class='container'>
      <div class='header'>
        <div class='logo'>${logoImg}</div>
        <div class='brand'>{{subject}}</div>
      </div>
      <div class='content'>
        {{content}}
      </div>
      <div class='footer'>
        © {{year}}
      </div>
    </div>
  </div>
</body>
</html>`
  }

  const applyBaseTemplate = (subject: string, bodyHtml: string) => {
    const tpl = (emailBaseTpl || '').trim() || defaultTemplate(emailLogoUrl)
    return tpl
      .replace('{{content}}', bodyHtml)
      .replace(/\{\{subject\}\}/g, subject || '')
      .replace(/\{\{logoUrl\}\}/g, (emailLogoUrl || ''))
      .replace(/\{\{year\}\}/g, String(new Date().getFullYear()))
  }

  const applyPlaceholders = (s: string, repl: Record<string, string>) => {
    return Object.entries(repl).reduce((acc, [k, v]) => acc.replace(new RegExp(`\\{\\{${k}\\}\\}`, 'g'), v), s)
  }

  const previewReplacements = (): Record<string, string> => ({
    dashboardName: 'Sample Dashboard',
    alertName: name || 'Notification',
    runAt: new Date().toLocaleString(),
    range: 'current period',
    kpi: '1234',
  })

  const renderKpiBlock = (label: string, value: any) => (
    `<div style='font-family:Inter,Arial,sans-serif'>
      <div style='font-size:12px;color:#6b7280;margin-bottom:4px'>${label}</div>
      <div style='font-size:24px;font-weight:600'>${value}</div>
    </div>`
  )

  const buildPreviewHtml = () => {
    const parts: string[] = []
    parts.push(`<div style='font-family:Inter,Arial,sans-serif;font-size:13px'>Rule: ${name}</div>`)
    if (kind === 'notification') {
      if (renderMode === 'kpi') parts.push(renderKpiBlock((widget as any)?.title || 'KPI', 1234))
      else if (renderMode === 'table') parts.push('<div class="card">Table preview will render when sent.</div>')
      else parts.push('<div class="card">Chart link will be included.</div>')
    }
    const per = String(template || '')
    if (per.trim()) parts.push(`<div>${per}</div>`)
    const bodyRaw = parts.join('\n')
    const mapping = previewReplacements()
    const body = applyPlaceholders(bodyRaw, mapping)
    const subjFinal = applyPlaceholders(previewSubject || name || 'Notification', mapping)
    const html = applyBaseTemplate(subjFinal, body)
    return applyPlaceholders(html, mapping)
  }

  const buildPayload = (): AlertCreate => {
    const triggers: any[] = []
    if (triggerType === 'threshold') {
      // Use selections; fall back to widget defaults
      const agg = (aggSel || '').trim() || ((spec?.agg && spec.agg !== 'none') ? String(spec.agg) : ((spec?.measure || spec?.y) ? 'sum' : 'count'))
      const chosenMeasure = (agg === 'count') ? undefined : ((measureSel || spec?.measure || spec?.y) as string | undefined)
      const xField = spec?.x || undefined
      const xVal = (xValueSel || '').trim()
      const xValue = xField ? (xVal === '' ? undefined : (isNaN(Number(xVal)) ? xVal : Number(xVal))) : undefined
      const t = {
        type: 'threshold',
        source: String(spec?.source || ''),
        aggregator: agg,
        measure: chosenMeasure,
        where: (spec?.where || undefined),
        xField,
        xValue,
        operator,
        value: value.includes(',') ? value.split(',').map((s)=>Number(s)) : Number(value),
      }
      triggers.push(t)
    } else {
      triggers.push({ type: 'time', cron: cronFromTime(timeOfDay, daysOfWeek) })
    }
    const actions: any[] = []
    const emailsSet = new Set<string>()
    const phonesSet = new Set<string>()
    emailTo.split(',').map((s)=>s.trim()).filter(Boolean).forEach((v)=>emailsSet.add(v))
    smsTo.split(',').map((s)=>s.trim()).filter(Boolean).forEach((v)=>phonesSet.add(v))
    // From selected contacts
    selectedContacts.forEach((c) => { const e=(c.email||'').trim(); const p=(c.phone||'').trim(); if (e) emailsSet.add(e); if (p) phonesSet.add(p) })
    // From datasource selections
    Object.entries(emailSelected).forEach(([k, on]) => { if (on) emailsSet.add(k) })
    Object.entries(phoneSelected).forEach(([k, on]) => { if (on) phonesSet.add(k) })
    const emails = Array.from(emailsSet)
    const phones = Array.from(phonesSet)
    if (emails.length) actions.push({ type: 'email', to: emails, subject: (previewSubject || name) })
    if (kind !== 'notification' && phones.length) actions.push({ type: 'sms', to: phones, message: template || name })
    const render: any = (kind === 'notification') ? (renderMode === 'table' ? { mode: 'table', querySpec: spec } : (renderMode === 'chart' ? { mode: 'chart', url: '' } : { mode: 'kpi', label: (widget as any)?.title || 'KPI' })) : { mode: 'kpi', label: (widget as any)?.title || 'KPI' }
    const cfg: AlertConfig = { datasourceId, triggers, actions, render, template }
    const payload: AlertCreate = { name, kind, widgetId: (widget as any)?.id, dashboardId: (widget as any)?.dashboardId, enabled: true, config: cfg }
    return payload
  }

  const onSave = async () => {
    try {
      setSaving(true); setError(null)
      const payload = buildPayload()
      await Api.createAlert(payload)
      onCloseAction()
    } catch (e: any) {
      setError(e?.message || 'Failed to save')
    } finally { setSaving(false) }
  }

  if (!open || typeof document === 'undefined') return null
  return createPortal((
    <div className="fixed inset-0 z-[1200]">
      <div className="absolute inset-0 bg-black/40" onClick={() => !saving && onCloseAction()} />
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[880px] max-w-[95vw] max-h-[90vh] overflow-auto rounded-lg border bg-card p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="text-sm font-medium">Alerts & Notifications</div>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCloseAction} disabled={saving}>✕</button>
        </div>
        {error && <div className="mb-2 text-xs text-rose-600">{error}</div>}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <label className="text-sm">Name
            <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={name} onChange={(e)=>setName(e.target.value)} />
          </label>
          <label className="text-sm">Type
            <select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={kind} onChange={(e)=>setKind(e.target.value as any)}>
              <option value="alert">Alert</option>
              <option value="notification">Notification</option>
            </select>
          </label>
          <div className="md:col-span-2 grid grid-cols-1 md:grid-cols-3 gap-3">
            <label className="text-sm">Email to (comma-separated)
              <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="user@org.com,another@org.com" value={emailTo} onChange={(e)=>setEmailTo(e.target.value)} />
            </label>
            {kind !== 'notification' && (
              <label className="text-sm">SMS to (comma-separated)
                <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="059xxxxxxx,056xxxxxxx" value={smsTo} onChange={(e)=>setSmsTo(e.target.value)} />
              </label>
            )}
            <label className="text-sm">Template (supports {'{{kpi}}'})
              <textarea className="mt-1 w-full h-24 px-2 py-2 rounded-md border bg-background" placeholder="Current KPI value: {{kpi}}" value={template} onChange={(e)=>setTemplate(e.target.value)} />
            </label>
          </div>
        </div>

        {/* Contacts & Datasource selectors */}
        <div className="mt-4">
          <div className="text-sm font-medium mb-2">Recipients</div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="space-y-2">
              <div className="text-xs font-medium">Contacts</div>
              <div className="flex items-center gap-2">
                <input className="w-full h-8 px-2 rounded-md border bg-background" placeholder="Search contacts by name/email/phone" value={contactsSearch} onChange={(e)=>setContactsSearch(e.target.value)} />
                <a href="/contacts" className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]">Contacts Manager</a>
              </div>
              {contactsResults.length > 0 && (
                <div className="rounded-md border bg-background p-2 max-h-36 overflow-auto text-xs">
                  {contactsResults.map((c) => {
                    const sel = selectedContacts.some((x)=>x.id===c.id)
                    return (
                      <div key={c.id} className="flex items-center justify-between py-1">
                        <div className="truncate"><span className="font-medium">{c.name}</span> <span className="opacity-70">{c.email || ''} {c.phone ? `| ${c.phone}`: ''}</span></div>
                        <button className="text-[11px] px-2 py-0.5 rounded-md border hover:bg-muted" onClick={()=> setSelectedContacts((prev)=> sel ? prev.filter(x=>x.id!==c.id) : [...prev, c])}>{sel ? 'Remove' : 'Add'}</button>
                      </div>
                    )
                  })}
                </div>
              )}
              {selectedContacts.length > 0 && (
                <div className="rounded-md border bg-background p-2 max-h-28 overflow-auto text-xs">
                  {selectedContacts.map((c) => (
                    <div key={c.id} className="flex items-center justify-between py-0.5">
                      <div className="truncate"><span className="font-medium">{c.name}</span> <span className="opacity-70">{c.email || ''} {c.phone ? `| ${c.phone}`: ''}</span></div>
                      <button className="text-[11px] px-2 py-0.5 rounded-md border hover:bg-muted" onClick={()=> setSelectedContacts((prev)=> prev.filter(x=>x.id!==c.id))}>✕</button>
                    </div>
                  ))}
                </div>
              )}
            </div>
            <div className="space-y-3">
              <div className="text-xs font-medium">Load recipients from datasource</div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-xs">
                <label>Source
                  <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="schema.table or table" value={emailSource} onChange={(e)=>setEmailSource(e.target.value)} />
                </label>
                <label>Email field
                  <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="email_column" value={emailField} onChange={(e)=>setEmailField(e.target.value)} />
                </label>
                <div className="col-span-2 flex items-center gap-2">
                  <button className="text-[11px] px-2 py-1 rounded-md border hover:bg-muted disabled:opacity-60" disabled={emailLoading || !emailSource || !emailField} onClick={async()=>{
                    if (!datasourceId) return;
                    try { setEmailLoading(true); const res = await Api.distinct({ source: emailSource, field: emailField, datasourceId }); setEmailCandidates(Array.isArray(res.values)? res.values.map(v=>String(v)).filter(Boolean) : []) } finally { setEmailLoading(false) }
                  }}>{emailLoading ? 'Loading…' : 'Load emails'}</button>
                  <button className="text-[11px] px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>{ const picked = Object.entries(emailSelected).filter(([k,v])=>v).map(([k])=>k); const merged = new Set<string>(emailTo.split(',').map(s=>s.trim()).filter(Boolean)); picked.forEach((e)=>merged.add(e)); setEmailTo(Array.from(merged).join(', ')) }}>Add to Email</button>
                </div>
                {emailCandidates.length>0 && (
                  <div className="col-span-2 rounded-md border bg-background p-2 max-h-32 overflow-auto">
                    {emailCandidates.map((v)=> (
                      <label key={v} className="flex items-center gap-2 text-xs py-0.5">
                        <input type="checkbox" checked={!!emailSelected[v]} onChange={(e)=> setEmailSelected((prev)=> ({ ...prev, [v]: e.target.checked }))} />
                        <span className="truncate">{v}</span>
                      </label>
                    ))}
                  </div>
                )}
              </div>
              {kind !== 'notification' && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-2 text-xs">
                  <label>Source
                    <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="schema.table or table" value={phoneSource} onChange={(e)=>setPhoneSource(e.target.value)} />
                  </label>
                  <label>Phone field
                    <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="phone_column" value={phoneField} onChange={(e)=>setPhoneField(e.target.value)} />
                  </label>
                  <div className="col-span-2 flex items-center gap-2">
                    <button className="text-[11px] px-2 py-1 rounded-md border hover:bg-muted disabled:opacity-60" disabled={phoneLoading || !phoneSource || !phoneField} onClick={async()=>{
                      if (!datasourceId) return;
                      try { setPhoneLoading(true); const res = await Api.distinct({ source: phoneSource, field: phoneField, datasourceId }); setPhoneCandidates(Array.isArray(res.values)? res.values.map(v=>String(v)).filter(Boolean) : []) } finally { setPhoneLoading(false) }
                    }}>{phoneLoading ? 'Loading…' : 'Load numbers'}</button>
                    <button className="text-[11px] px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>{ const picked = Object.entries(phoneSelected).filter(([k,v])=>v).map(([k])=>k); const merged = new Set<string>(smsTo.split(',').map(s=>s.trim()).filter(Boolean)); picked.forEach((p)=>merged.add(p)); setSmsTo(Array.from(merged).join(', ')) }}>Add to SMS</button>
                  </div>
                  {phoneCandidates.length>0 && (
                    <div className="col-span-2 rounded-md border bg-background p-2 max-h-32 overflow-auto">
                      {phoneCandidates.map((v)=> (
                        <label key={v} className="flex items-center gap-2 text-xs py-0.5">
                          <input type="checkbox" checked={!!phoneSelected[v]} onChange={(e)=> setPhoneSelected((prev)=> ({ ...prev, [v]: e.target.checked }))} />
                          <span className="truncate">{v}</span>
                        </label>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="mt-4">
          <div className="text-sm font-medium mb-2">Trigger</div>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-3">
            <label className="text-sm">Type
              <select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={triggerType} onChange={(e)=>setTriggerType(e.target.value as any)}>
                <option value="threshold">Threshold</option>
                <option value="time">Time of day</option>
              </select>
            </label>
            {triggerType === 'threshold' ? (
              <>
                {/* Aggregator & Measure selections to support multiple value chips */}
                <label className="text-sm">Aggregator
                  <select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={aggSel} onChange={(e)=>setAggSel(e.target.value)}>
                    {['count','sum','avg','min','max','distinct'].map((a)=> (<option key={a} value={a}>{a}</option>))}
                  </select>
                </label>
                <label className="text-sm">Measure / Value
                  <select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={measureSel} onChange={(e)=>setMeasureSel(e.target.value)} disabled={aggSel==='count'}>
                    {/* Build options from widget spec */}
                    {(() => {
                      const opts: Array<{label: string, value: string}> = []
                      try {
                        const s: any = spec || {}
                        if (Array.isArray(s?.series) && s.series.length) {
                          for (const it of s.series) {
                            const lab = String(it?.name || it?.y || '').trim()
                            const val = String(it?.y || '').trim()
                            if (val) opts.push({ label: lab || val, value: val })
                          }
                        }
                        const single = String(s?.measure || s?.y || '').trim()
                        if (single) opts.push({ label: single, value: single })
                      } catch {}
                      // de-dupe by value
                      const seen = new Set<string>()
                      return opts.filter(o => { const k = o.value; if (seen.has(k)) return false; seen.add(k); return true }).map(o => (
                        <option key={o.value} value={o.value}>{o.label}</option>
                      ))
                    })()}
                  </select>
                </label>
                <label className="text-sm">Operator
                  <select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={operator} onChange={(e)=>setOperator(e.target.value)}>
                    <option value=">">&gt;</option>
                    <option value=">=">&gt;=</option>
                    <option value="<">&lt;</option>
                    <option value="<=">&lt;=</option>
                    <option value="==">==</option>
                    <option value="between">between (enter A,B)</option>
                  </select>
                </label>
                <label className="text-sm">Value
                  <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder={operator==='between'? 'A,B' : 'number'} value={value} onChange={(e)=>setValue(e.target.value)} />
                </label>
                {spec?.x && (
                  <label className="text-sm">X value to compare
                    <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder={`Enter ${String(spec?.x)} value (optional)`} value={xValueSel} onChange={(e)=>setXValueSel(e.target.value)} />
                  </label>
                )}
              </>
            ) : (
              <>
                <label className="text-sm">Time (HH:mm)
                  <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="09:00" value={timeOfDay} onChange={(e)=>setTimeOfDay(e.target.value)} />
                </label>
                <label className="text-sm">Week days (0=Sun..6=Sat)
                  <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="1,2,3,4,5" value={daysOfWeek.join(',')} onChange={(e)=>{
                    const arr = e.target.value.split(',').map((s)=>parseInt(s,10)).filter((n)=>!isNaN(n) && n>=0 && n<=6)
                    setDaysOfWeek(arr)
                  }} />
                </label>
              </>
            )}
          </div>
        </div>

        <div className="mt-4">
          <div className="text-sm font-medium mb-2">Render (for Notifications)</div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <label className="text-sm">Mode
              <select className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={renderMode} onChange={(e)=>setRenderMode(e.target.value as any)}>
                <option value="kpi">KPI</option>
                <option value="table">Table</option>
                <option value="chart">Chart (link)</option>
              </select>
            </label>
          </div>
        </div>

        {kind === 'notification' && (
          <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3">
            <div className="space-y-2">
              <div className="text-sm font-medium">Template Placeholders</div>
              <div className="flex items-center gap-2 text-xs">
                <span className="px-2 py-1 rounded-md border cursor-pointer" onClick={() => setTemplate((t)=>t + (t.endsWith(' ')?'':' ') + '{{kpi}}')}>{'{{kpi}}'}</span>
              </div>
            </div>
            <div className="space-y-2">
              <div className="text-sm font-medium">Live Email Preview</div>
              <label className="text-sm">Subject
                <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={previewSubject} onChange={(e)=>setPreviewSubject(e.target.value)} />
              </label>
              <div className="rounded-md border bg-white overflow-auto" style={{ minHeight: 160 }}>
                <iframe title="email-preview" className="w-full h-[260px]" srcDoc={buildPreviewHtml()} />
              </div>
            </div>
          </div>
        )}

        <div className="mt-4 flex items-center gap-2">
          <button className="text-xs px-3 py-2 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" disabled={saving} onClick={onSave}>{saving ? 'Saving…' : 'Save'}</button>
          <button className="text-xs px-3 py-2 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" disabled={saving} onClick={onCloseAction}>Cancel</button>
        </div>
      </div>
    </div>
  ), document.body)
}
