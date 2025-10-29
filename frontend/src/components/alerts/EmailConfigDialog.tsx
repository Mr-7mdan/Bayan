"use client"

import React, { useEffect, useState, useRef } from 'react'
import { Api, type EmailConfigPayload, type TestEmailPayload } from '@/lib/api'

export default function EmailConfigDialog({ open, onCloseAction }: { open: boolean; onCloseAction: () => void }) {
  const [form, setForm] = useState<EmailConfigPayload>({ host: '', port: 587, username: '', password: '', fromName: '', fromEmail: '', useTls: true, baseTemplateHtml: '', logoUrl: '' })
  const [testTo, setTestTo] = useState('')
  const [previewSubject, setPreviewSubject] = useState('Sample Notification')
  const [previewContent, setPreviewContent] = useState('<div class="card"><div class="subject">Weekly Report</div><p>Hello, this is a sample content block for preview purposes.</p></div>')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [toast, setToast] = useState('')
  const fileInputRef = useRef<HTMLInputElement | null>(null)

  useEffect(() => {
    let cancelled = false
    async function run() {
      if (!open) return
      setError(null)
      try {
        const cur = await Api.getEmailConfig()
        if (!cancelled && cur) setForm({ ...cur })
      } catch { /* ignore */ }
    }
    void run(); return () => { cancelled = true }
  }, [open])

  const onPickLogo = () => { try { fileInputRef.current?.click() } catch {} }
  const onLogoFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    try {
      const f = e.target.files?.[0]
      if (!f) return
      const rd = new FileReader()
      rd.onload = () => { const data = String(rd.result || ''); setForm((prev)=>({ ...prev, logoUrl: data })) }
      rd.readAsDataURL(f)
      e.currentTarget.value = ''
    } catch {}
  }

  const onSave = async () => {
    try {
      setBusy(true); setError(null)
      await Api.putEmailConfig({ ...form, fromEmail: form.username })
      setToast('Saved'); window.setTimeout(() => setToast(''), 1600)
      onCloseAction()
    } catch (e: any) { setError(e?.message || 'Failed to save') } finally { setBusy(false) }
  }

  const defaultTemplate = () => {
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
    .brand{font-size:14px;font-weight:600;}
    .content{padding:20px;}
    .footer{padding:16px 20px;border-top:1px solid #e5e7eb;color:#6b7280;font-size:12px;background:#fafafa;}
  </style>
  <link href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap' rel='stylesheet'>
  <style> @media (prefers-color-scheme: dark) { body{background:#0b0f15;color:#e5e7eb} .container{background:#0f1720;border-color:#1f2937} .header{background:#0f1720;border-color:#1f2937} .footer{background:#0f1720;border-color:#1f2937;color:#9ca3af} .card{background:#0f1720;border-color:#1f2937} th,td{border-color:#1f2937} th{background:#111827} } </style>
  <style> img{border:0;} a{color:#2563eb;text-decoration:none} .logo{display:flex;align-items:center;gap:12px} .subject{font-size:14px;font-weight:600} table{border-collapse:collapse} th,td{border:1px solid #e5e7eb;padding:6px} th{background:#f3f4f6} .card{border:1px solid #e5e7eb;border-radius:10px;padding:16px} .card + .card{margin-top:12px} @media (prefers-color-scheme: dark) { .card{background:#0f1720;border-color:#1f2937} th,td{border-color:#1f2937} th{background:#111827} } </style>
  
</head>
<body>
  <div class='wrap'>
    <div class='container'>
      <div class='header'>
        <div class='logo'><img src='{{logoUrl}}' alt='Logo' style='max-height:40px;display:block'/></div>
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
 
  const compactTemplate = () => {
    return `<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>{{subject}}</title>
  <style>
    body{margin:0;padding:0;background:#f8fafc;color:#111827;font-family:Inter,Arial,sans-serif;}
    .container{max-width:640px;margin:12px auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden}
    .header{padding:10px 14px;border-bottom:1px solid #e5e7eb;background:#fafafa;display:flex;align-items:center;gap:10px}
    .brand{font-size:13px;font-weight:600}
    .content{padding:14px}
    .footer{padding:10px 14px;border-top:1px solid #e5e7eb;color:#6b7280;font-size:12px;background:#fafafa}
    table{border-collapse:collapse;width:100%}
    th,td{border:1px solid #e5e7eb;padding:6px;text-align:left}
    thead th{background:#f3f4f6}
    tbody tr:nth-child(even){background:#f9fafb}
    .card + .card{margin-top:12px}
  </style>
  <style> @media (prefers-color-scheme: dark) { body{background:#0b0f15;color:#e5e7eb} .container{background:#0f1720;border-color:#1f2937} .header{background:#0f1720;border-color:#1f2937} .brand{color:#e5e7eb} .footer{background:#0f1720;border-color:#1f2937;color:#9ca3af} th,td{border-color:#1f2937} thead th{background:#111827} tbody tr:nth-child(even){background:#0b1220} .card{border-color: #1f2937} } </style>
</head>
<body>
  <div class='container'>
    <div class='header'>
      <div class='logo'><img src='{{logoUrl}}' alt='Logo' style='max-height:32px;display:block'/></div>
      <div class='brand'>{{subject}}</div>
    </div>
    <div class='content'>
      {{content}}
    </div>
    <div class='footer'>&copy; {{year}}</div>
  </div>
</body>
</html>`
  }

  const kpiHeroTemplate = () => {
    return `<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>{{subject}}</title>
  <style>
    body{margin:0;padding:0;background:#0b0f15;color:#e5e7eb;font-family:Inter,Arial,sans-serif}
    .container{max-width:640px;margin:0 auto}
    .hero{padding:28px 20px;text-align:center}
    .logo{margin-bottom:8px}
    .title{font-size:16px;font-weight:600;color:#e5e7eb}
    .card{background:#0f1720;border:1px solid #1f2937;border-radius:12px;padding:22px;margin:0 12px}
    .content{padding:20px}
    .footer{color:#9ca3af;font-size:12px;padding:12px 20px;text-align:center}
    a{color:#60a5fa;text-decoration:none}
    table{border-collapse:collapse;width:100%}
    th,td{border:1px solid #1f2937;padding:6px;text-align:left}
    thead th{background:#111827}
    tbody tr:nth-child(even){background:#0b1220}
    .card + .card{margin-top:12px}
  </style>
</head>
<body>
  <div class='container'>
    <div class='hero'>
      <div class='logo'><img src='{{logoUrl}}' alt='Logo' style='max-height:40px;display:inline-block'/></div>
      <div class='title'>{{subject}}</div>
    </div>
    <div class='card'>
      {{content}}
    </div>
    <div class='footer'>© {{year}}</div>
  </div>
</body>
</html>`
  }

  const tableFocusedTemplate = () => {
    return `<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>{{subject}}</title>
  <style>
    body{margin:0;padding:0;background:#f7f7f8;color:#111827;font-family:Inter,Arial,sans-serif}
    .container{max-width:820px;margin:16px auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden}
    .header{padding:14px 18px;border-bottom:1px solid #e5e7eb;background:#fafafa;display:flex;align-items:center;gap:12px}
    .brand{font-size:14px;font-weight:600}
    .content{padding:12px}
    .footer{padding:12px 18px;border-top:1px solid #e5e7eb;color:#6b7280;font-size:12px;background:#fafafa}
    table{border-collapse:collapse;width:100%}
    th,td{border:1px solid #e5e7eb;padding:8px;text-align:left}
    thead th{background:#f3f4f6}
    tbody tr:nth-child(even){background:#f9fafb}
  </style>
</head>
<body>
  <div class='container'>
    <div class='header'>
      <div class='logo'><img src='{{logoUrl}}' alt='Logo' style='max-height:36px;display:block'/></div>
      <div class='brand'>{{subject}}</div>
    </div>
    <div class='content'>
      {{content}}
    </div>
    <div class='footer'>© {{year}}</div>
  </div>
</body>
</html>`
  }

  const applyBaseTemplate = (subject: string, bodyHtml: string) => {
    const tpl = (form.baseTemplateHtml || '').trim() || defaultTemplate()
    let out = tpl
      .replace('{{content}}', bodyHtml)
      .replace(/\{\{subject\}\}/g, subject || '')
      .replace(/\{\{logoUrl\}\}/g, (form.logoUrl || ''))
      .replace(/\{\{year\}\}/g, String(new Date().getFullYear()))
    // sample replacements for preview-only placeholders
    const previewRepl: Record<string,string> = {
      dashboardName: 'Sample Dashboard',
      alertName: subject || 'Sample Notification',
      runAt: new Date().toLocaleString(),
      range: 'current period',
    }
    out = Object.entries(previewRepl).reduce((acc, [k, v]) => acc.replace(new RegExp(`\\{\\{${k}\\}\\}`, 'g'), v), out)
    try {
      const prefersDark = typeof window !== 'undefined' && !!window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches
      if (prefersDark && out.includes('</head>')) {
        const force = `<style id="force-dark-email">.container,.header,.footer{background:#0f1720 !important;border-color:#1f2937 !important;color:#e5e7eb !important}.brand{color:#e5e7eb !important}.card{background:#0f1720 !important;border-color:#1f2937 !important}.card + .card{margin-top:12px !important}th,td{border-color:#1f2937 !important}thead th{background:#111827 !important}</style>`
        out = out.replace('</head>', `${force}</head>`)
      }
    } catch {}
    return out
  }

  const onTest = async () => {
    try {
      setBusy(true); setError(null)
      const to = testTo.split(',').map((s)=>s.trim()).filter(Boolean)
      if (!to.length) { setError('Enter 1+ recipients'); setBusy(false); return }
      const html = applyBaseTemplate(previewSubject, previewContent)
      const payload: TestEmailPayload = { to, subject: previewSubject || 'Test Email', html }
      await Api.testEmail(payload)
      setToast('Test sent'); window.setTimeout(() => setToast(''), 1600)
    } catch (e: any) { setError(e?.message || 'Failed to send test') } finally { setBusy(false) }
  }

  if (!open) return null
  return (
    <div className="fixed inset-0 z-[1200]">
      <div className="absolute inset-0 bg-black/40" onClick={() => !busy && onCloseAction()} />
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[980px] max-w-[98vw] max-h-[95vh] overflow-auto rounded-lg border bg-card p-4">
        <div className="flex items-center justify-between mb-2">
          <div className="text-sm font-medium">Email (Office 365) Configuration</div>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-muted" onClick={onCloseAction} disabled={busy}>✕</button>
        </div>
        {error && <div className="mb-2 text-xs text-rose-600">{error}</div>}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <label className="text-sm">SMTP Host<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={form.host || ''} onChange={(e)=>setForm((f)=>({ ...f, host: e.target.value }))} /></label>
          <label className="text-sm">Port<input type="number" className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={Number(form.port || 587)} onChange={(e)=>setForm((f)=>({ ...f, port: Number(e.target.value||587) }))} /></label>
          <label className="text-sm">Username<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={form.username || ''} onChange={(e)=>setForm((f)=>({ ...f, username: e.target.value }))} /></label>
          <label className="text-sm">Password<input type="password" className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={(form as any).password || ''} onChange={(e)=>setForm((f)=>({ ...f, password: e.target.value } as any))} /></label>
          <label className="text-sm">From Name<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={form.fromName || ''} onChange={(e)=>setForm((f)=>({ ...f, fromName: e.target.value }))} /></label>
          <label className="text-sm inline-flex items-center gap-2 mt-1"><input type="checkbox" className="h-4 w-4 accent-[hsl(var(--primary))]" checked={!!form.useTls} onChange={(e)=>setForm((f)=>({ ...f, useTls: e.target.checked }))} /><span>Use TLS</span></label>
        </div>
        <div className="mt-4 grid grid-cols-1 md:grid-cols-2 gap-3">
          <div className="space-y-2 min-w-0">
            <div className="text-sm font-medium">Branding & Base Template</div>
            <label className="text-sm">Logo URL
              <input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="https://.../logo.png or data:image/..." value={form.logoUrl || ''} onChange={(e)=>setForm((f)=>({ ...f, logoUrl: e.target.value }))} />
            </label>
            <div className="flex items-center gap-2 text-xs">
              <input ref={fileInputRef} type="file" accept="image/*" className="hidden" onChange={onLogoFileChange} />
              <button type="button" className="px-2 py-1 rounded-md border hover:bg-muted" onClick={onPickLogo} disabled={busy}>Upload & embed</button>
              {String(form.logoUrl || '').startsWith('data:') && (<span className="px-2 py-1 rounded-md border bg-[hsl(var(--muted))]">Embedded</span>)}
              {!!form.logoUrl && (<button type="button" className="px-2 py-1 rounded-md border hover:bg-muted" onClick={()=>setForm((f)=>({ ...f, logoUrl: '' }))} disabled={busy}>Clear</button>)}
            </div>
            <label className="text-sm">Base HTML template
              <textarea
                className="mt-1 w-full h-48 px-2 py-2 rounded-md border bg-background font-mono text-[12px]"
                placeholder="Paste your HTML template here (use {{subject}}, {{content}}, {{logoUrl}}, {{year}})"
                value={form.baseTemplateHtml || ''}
                onChange={(e)=>setForm((f)=>({ ...f, baseTemplateHtml: e.target.value }))}
                onDragOver={(e)=>{ e.preventDefault() }}
                onDrop={(e)=>{
                  e.preventDefault();
                  const ph = e.dataTransfer.getData('text/plain')
                  if (!ph) return
                  const el = e.currentTarget as HTMLTextAreaElement
                  const start = (el.selectionStart ?? (form.baseTemplateHtml?.length || 0)) as number
                  const end = (el.selectionEnd ?? start) as number
                  const cur = form.baseTemplateHtml || ''
                  const next = cur.slice(0, start) + ph + cur.slice(end)
                  setForm((f)=>({ ...f, baseTemplateHtml: next }))
                  setTimeout(() => { try { el.focus(); el.selectionStart = el.selectionEnd = start + ph.length } catch {} }, 0)
                }}
              />
            </label>
            <div className="flex flex-wrap items-center gap-2 text-xs relative z-10 w-full max-w-full md:pr-2">
              {(['{{subject}}','{{content}}','{{logoUrl}}','{{year}}','{{dashboardName}}','{{alertName}}','{{runAt}}','{{range}}'] as const).map((ph)=> (
                <span
                  key={ph}
                  className="px-2 py-1 rounded-md border cursor-move select-none bg-[hsl(var(--muted))] whitespace-nowrap"
                  title="Drag into the template"
                  draggable
                  onDragStart={(e)=>{ e.dataTransfer.setData('text/plain', ph) }}
                  onClick={()=> setForm((f)=>({ ...f, baseTemplateHtml: (f.baseTemplateHtml||'') + (f.baseTemplateHtml?.endsWith(' ')?'':' ') + ph }))}
                >{ph}</span>
              ))}
            </div>
            <div className="w-full flex flex-wrap items-center gap-2 text-xs mt-2">
              <span>Presets:</span>
              <button className="px-2 py-1 rounded-md border hover:bg-muted" onClick={() => setForm((f)=>({ ...f, baseTemplateHtml: defaultTemplate() }))}>Default</button>
              <button className="px-2 py-1 rounded-md border hover:bg-muted" onClick={() => setForm((f)=>({ ...f, baseTemplateHtml: compactTemplate() }))}>Compact</button>
              <button className="px-2 py-1 rounded-md border hover:bg-muted" onClick={() => setForm((f)=>({ ...f, baseTemplateHtml: kpiHeroTemplate() }))}>KPI Hero</button>
              <button className="px-2 py-1 rounded-md border hover:bg-muted" onClick={() => setForm((f)=>({ ...f, baseTemplateHtml: tableFocusedTemplate() }))}>Table Focused</button>
            </div>
          </div>
          <div className="space-y-2">
            <div className="text-sm font-medium">Live Preview</div>
            <label className="text-sm">Subject<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" value={previewSubject} onChange={(e)=>setPreviewSubject(e.target.value)} /></label>
            <label className="text-sm">Body Content (inserts into {'{{content}}'})
              <textarea className="mt-1 w-full h-32 px-2 py-2 rounded-md border bg-background" value={previewContent} onChange={(e)=>setPreviewContent(e.target.value)} />
            </label>
            <div className="rounded-md border bg-card overflow-auto" style={{ minHeight: 160 }}>
              <iframe title="email-preview" className="w-full h-[260px]" srcDoc={applyBaseTemplate(previewSubject, previewContent)} />
            </div>
          </div>
        </div>
        <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-3">
          <label className="text-sm">Test to (comma-separated)<input className="mt-1 w-full h-8 px-2 rounded-md border bg-background" placeholder="user@org.com,another@org.com" value={testTo} onChange={(e)=>setTestTo(e.target.value)} /></label>
          <div className="flex items-end gap-2">
            <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" disabled={busy} onClick={onTest}>Send test</button>
            <button className="text-xs px-3 py-2 rounded-md border hover:bg-muted" disabled={busy} onClick={onSave}>Save</button>
          </div>
        </div>
        {!!toast && <div className="mt-3 text-xs text-emerald-700">{toast}</div>}
      </div>
    </div>
  )
}
