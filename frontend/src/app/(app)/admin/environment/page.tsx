"use client"

import { Suspense, useEffect, useMemo, useState } from 'react'
import { useRouter } from 'next/navigation'
import { Card, Title, Text } from '@tremor/react'
import { useAuth } from '@/components/providers/AuthProvider'
import { Api } from '@/lib/api'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'

export const dynamic = 'force-dynamic'

function resolveDefaultApi(): string {
  return (process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000/api').replace(/\/$/, '')
}

export default function AdminEnvironmentPage() {
  const { user } = useAuth()
  const router = useRouter()
  const isAdmin = (user?.role || '').toLowerCase() === 'admin'
  const { env, setEnv } = useEnvironment()
  const defaultPublicDomain = useMemo(() => (process.env.NEXT_PUBLIC_PUBLIC_DOMAIN || '').replace(/\/$/, ''), [])
  const effectivePublicDomain = (env.publicDomain && env.publicDomain.trim()) ? env.publicDomain.replace(/\/$/, '') : defaultPublicDomain

  const [apiOverride, setApiOverride] = useState('')
  const [effectiveApi, setEffectiveApi] = useState('')
  const [frontendOrigin, setFrontendOrigin] = useState('')
  const [frontendPort, setFrontendPort] = useState('')
  const [msg, setMsg] = useState<string | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [testing, setTesting] = useState(false)
  const [testingAI, setTestingAI] = useState(false)
  const [aiMsg, setAiMsg] = useState<string | null>(null)
  const [aiErr, setAiErr] = useState<string | null>(null)
  const [serverHasKey, setServerHasKey] = useState<boolean>(false)
  const [serverBaseUrl, setServerBaseUrl] = useState<string>('')
  const [savingAI, setSavingAI] = useState<boolean>(false)
  const [aiSaveMsg, setAiSaveMsg] = useState<string | null>(null)
  const [aiSaveErr, setAiSaveErr] = useState<string | null>(null)
  const [savingBranding, setSavingBranding] = useState<boolean>(false)
  const [brandingMsg, setBrandingMsg] = useState<string | null>(null)
  const [brandingErr, setBrandingErr] = useState<string | null>(null)
  // Updates
  const [updB, setUpdB] = useState<any>(null)
  const [updF, setUpdF] = useState<any>(null)
  const [checkingUpd, setCheckingUpd] = useState<boolean>(false)
  const [applyBusy, setApplyBusy] = useState<null | 'backend' | 'frontend'>(null)

  useEffect(() => {
    if (!isAdmin) router.replace('/home')
  }, [isAdmin, router])

  useEffect(() => {
    try {
      if (typeof window !== 'undefined') {
        const o = localStorage.getItem('api_base_override') || ''
        setApiOverride(o)
        const eff = (o && /^https?:\/\//i.test(o)) ? o.replace(/\/$/, '') : resolveDefaultApi()
        setEffectiveApi(eff)
        setFrontendOrigin(window.location.origin)
        try {
          const url = new URL(window.location.href)
          setFrontendPort(url.port || (url.protocol === 'https:' ? '443' : '80'))
        } catch {
          setFrontendPort('')
        }
      }
    } catch {}
  }, [])

  const checkUpdates = async () => {
    setCheckingUpd(true)
    try {
      const b = await Api.updatesCheck('backend')
      const f = await Api.updatesCheck('frontend')
      setUpdB(b); setUpdF(f)
    } catch (e: any) {
      setErr(e?.message || 'Failed to check updates')
    } finally { setCheckingUpd(false) }
  }

  const applyUpdate = async (component: 'backend'|'frontend') => {
    if (!user?.id) { setErr('Login required'); return }
    setApplyBusy(component)
    try {
      const res = await Api.updatesApply(component, user.id)
      setMsg(`Staged ${component} ${res.version}. Restart required to take effect`)
      window.setTimeout(() => setMsg(null), 2000)
    } catch (e: any) {
      setErr(e?.message || 'Failed to apply update')
    } finally { setApplyBusy(null) }
  }

  useEffect(() => {
    (async () => {
      try {
        const cfg = await Api.getAiConfig()
        setServerHasKey(!!cfg?.hasKey)
        if (cfg?.provider) setEnv({ aiProvider: cfg.provider as any })
        if (cfg?.model) setEnv({ aiModel: cfg.model })
        if ((cfg as any)?.baseUrl) setServerBaseUrl(String((cfg as any).baseUrl))
      } catch {}
    })()
  }, [])

  // Load server branding on mount and hydrate environment for single source of truth
  useEffect(() => {
    (async () => {
      try {
        const b = await Api.getBranding()
        const patch: any = {}
        if ((b.orgName || '').trim()) patch.orgName = b.orgName
        if ((b.logoLight || '').trim()) patch.orgLogoLight = b.logoLight
        if ((b.logoDark || '').trim()) patch.orgLogoDark = b.logoDark
        if ((b.favicon || '').trim()) patch.favicon = b.favicon
        if (Object.keys(patch).length) setEnv(patch)
      } catch {}
    })()
  }, [])

  const onSave = () => {
    try {
      if (apiOverride && /^https?:\/\//i.test(apiOverride)) {
        localStorage.setItem('api_base_override', apiOverride)
        setEffectiveApi(apiOverride.replace(/\/$/, ''))
        setMsg('Saved API override')
      } else {
        localStorage.removeItem('api_base_override')
        setEffectiveApi(resolveDefaultApi())
        setMsg('Cleared API override (using default)')
      }
      setErr(null)
      window.setTimeout(() => setMsg(null), 1600)
    } catch (e: any) {
      setErr(e?.message || 'Failed to save override')
      setMsg(null)
    }
  }

  const onTest = async () => {
    setTesting(true); setErr(null); setMsg(null)
    try {
      // Trigger a simple request against current effective base
      await Api.getBranding()
      setMsg('API reachable')
      window.setTimeout(() => setMsg(null), 1600)
    } catch (e: any) {
      setErr(e?.message || 'Failed to reach API')
    } finally { setTesting(false) }
  }

  const onTestAI = async () => {
    setTestingAI(true); setAiErr(null); setAiMsg(null)
    try {
      const provider = (env.aiProvider || 'gemini') as any
      const model = env.aiModel || (provider === 'openai' ? 'gpt-4o-mini' : (provider === 'mistral' ? 'mistral-small' : 'gemini-1.5-flash'))
      const apiKey = env.aiApiKey || ''
      if (!apiKey && !serverHasKey) throw new Error('Enter API key or save one on server')
      await Api.aiDescribe({ provider, model, apiKey, schema: { table: 'test', columns: [{ name: 'id', type: 'string' }] }, samples: [] })
      setAiMsg('AI endpoint OK')
      window.setTimeout(() => setAiMsg(null), 2000)
    } catch (e: any) {
      setAiErr(e?.message || 'AI endpoint failed')
    } finally { setTestingAI(false) }
  }

  const saveAiToServer = async (clearKey?: boolean) => {
    setSavingAI(true); setAiSaveErr(null); setAiSaveMsg(null)
    try {
      const provider = (env.aiProvider || 'gemini') as any
      const model = env.aiModel || (provider === 'openai' ? 'gpt-4o-mini' : (provider === 'mistral' ? 'mistral-small' : 'gemini-1.5-flash'))
      const body: any = { provider, model }
      if (clearKey === true) body.apiKey = ''
      else if ((env.aiApiKey || '').trim()) body.apiKey = env.aiApiKey
      await Api.putAiConfig(body, user?.id)
      if (clearKey === true) setServerHasKey(false)
      else if ((env.aiApiKey || '').trim()) setServerHasKey(true)
      setAiSaveMsg('Saved')
      window.setTimeout(() => setAiSaveMsg(null), 1500)
    } catch (e: any) {
      setAiSaveErr(e?.message || 'Failed to save')
    } finally { setSavingAI(false) }
  }

  const saveBrandingToServer = async () => {
    setSavingBranding(true); setBrandingMsg(null); setBrandingErr(null)
    try {
      const res = await Api.putAdminBranding({
        orgName: env.orgName || '',
        logoLight: env.orgLogoLight || '',
        logoDark: env.orgLogoDark || '',
        favicon: env.favicon || '',
      }, user?.id)
      setEnv({
        orgName: res.orgName || '',
        orgLogoLight: res.logoLight || '',
        orgLogoDark: res.logoDark || '',
        favicon: res.favicon || '',
      })
      setBrandingMsg('Saved')
      window.setTimeout(() => setBrandingMsg(null), 1500)
    } catch (e: any) {
      setBrandingErr(e?.message || 'Failed to save')
    } finally { setSavingBranding(false) }
  }

  const reloadBrandingFromServer = async () => {
    setSavingBranding(true); setBrandingMsg(null); setBrandingErr(null)
    try {
      const b = await Api.getBranding()
      setEnv({
        orgName: b.orgName || '',
        orgLogoLight: b.logoLight || '',
        orgLogoDark: b.logoDark || '',
        favicon: b.favicon || '',
      })
      setBrandingMsg('Loaded from server')
      window.setTimeout(() => setBrandingMsg(null), 1500)
    } catch (e: any) {
      setBrandingErr(e?.message || 'Failed to load')
    } finally { setSavingBranding(false) }
  }

  if (!isAdmin) return null

  return (
    <Suspense fallback={<div className="p-3 text-sm">Loading…</div>}>
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">Environment</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">Set backend URL override and view frontend info</Text>
          </div>
        </div>
        <div className="p-4 space-y-4">
          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">Backend API</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div>
                <label className="text-sm block">Current effective API base
                  <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={effectiveApi} readOnly />
                </label>
              </div>
              <div>
                <label className="text-sm block">Override API base (optional)
                  <input placeholder="https://api.example.com/api" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={apiOverride} onChange={(e) => setApiOverride(e.target.value)} />
                </label>
              </div>
            </div>
            <div className="mt-3 flex items-center gap-2">
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" onClick={onSave}>Save</button>
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={testing} onClick={onTest}>{testing ? 'Testing…' : 'Test API'}</button>
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" onClick={() => { setApiOverride(''); onSave() }}>Reset to default</button>
            </div>
          </section>

          {/* Updates */}
          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">Updates</h3>
            <p className="text-xs text-muted-foreground mb-3">Check for new versions and apply auto updates. Manual updates will display instructions.</p>
            <div className="flex items-center gap-2 mb-3">
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={checkingUpd} onClick={checkUpdates}>{checkingUpd ? 'Checking…' : 'Check updates'}</button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div className="rounded-md border p-2">
                <div className="text-sm font-medium">Backend</div>
                <div className="text-xs text-muted-foreground">Current: {updB?.currentVersion || '—'} · Latest: {updB?.latestVersion || '—'}</div>
                <div className="text-xs mt-1">Type: {updB?.updateType || '—'}{updB?.requiresMigrations ? ' (requires migrations)' : ''}</div>
                {updB?.releaseNotes && (
                  <details className="mt-2 text-xs">
                    <summary className="cursor-pointer select-none">Release notes</summary>
                    <div className="mt-1 whitespace-pre-wrap">{updB.releaseNotes}</div>
                  </details>
                )}
                <div className="mt-2">
                  <button
                    className="text-xs px-2 py-1 rounded-md border hover:bg-muted"
                    disabled={!updB?.enabled || updB?.updateType !== 'auto' || updB?.requiresMigrations || applyBusy === 'backend'}
                    onClick={() => applyUpdate('backend')}
                  >{applyBusy === 'backend' ? 'Applying…' : 'Apply auto update'}</button>
                </div>
              </div>
              <div className="rounded-md border p-2">
                <div className="text-sm font-medium">Frontend</div>
                <div className="text-xs text-muted-foreground">Current: {updF?.currentVersion || '—'} · Latest: {updF?.latestVersion || '—'}</div>
                <div className="text-xs mt-1">Type: {updF?.updateType || '—'}{updF?.requiresMigrations ? ' (requires migrations)' : ''}</div>
                {updF?.releaseNotes && (
                  <details className="mt-2 text-xs">
                    <summary className="cursor-pointer select-none">Release notes</summary>
                    <div className="mt-1 whitespace-pre-wrap">{updF.releaseNotes}</div>
                  </details>
                )}
                <div className="mt-2">
                  <button
                    className="text-xs px-2 py-1 rounded-md border hover:bg-muted"
                    disabled={!updF?.enabled || updF?.updateType !== 'auto' || updF?.requiresMigrations || applyBusy === 'frontend'}
                    onClick={() => applyUpdate('frontend')}
                  >{applyBusy === 'frontend' ? 'Applying…' : 'Apply auto update'}</button>
                </div>
              </div>
            </div>
          </section>

          {/* AI Features */}
          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">AI Features</h3>
            <p className="text-xs text-muted-foreground mb-3">Configure the AI provider, model, and API key. Defaults to Gemini Flash. Keys can be saved securely on the server.</p>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <label className="text-sm block">Provider
                <select
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  value={env.aiProvider || 'gemini'}
                  onChange={(e)=> setEnv({ aiProvider: e.target.value as any })}
                >
                  <option value="gemini">Gemini</option>
                  <option value="openai">OpenAI</option>
                  <option value="mistral">Mistral</option>
                  <option value="anthropic">Anthropic</option>
                  <option value="openrouter">OpenRouter</option>
                </select>
              </label>
              <label className="text-sm block">Model
                <input
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  placeholder={env.aiProvider === 'gemini' ? 'gemini-1.5-flash' : env.aiProvider === 'openai' ? 'gpt-4o-mini' : 'mistral-small'}
                  value={env.aiModel || ''}
                  onChange={(e)=> setEnv({ aiModel: e.target.value })}
                />
              </label>
              <label className="text-sm block">API Key
                <input
                  type="password"
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  placeholder="Enter API key"
                  value={env.aiApiKey || ''}
                  onChange={(e)=> setEnv({ aiApiKey: e.target.value })}
                />
              </label>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-3">
              <label className="text-sm block">AI Base URL (optional)
                <input
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  placeholder={env.aiProvider === 'openai' ? 'https://api.openai.com/v1' : env.aiProvider === 'mistral' ? 'https://api.mistral.ai/v1' : env.aiProvider === 'anthropic' ? 'https://api.anthropic.com/v1' : env.aiProvider === 'openrouter' ? 'https://openrouter.ai/api/v1' : ''}
                  value={env.aiBaseUrl || ''}
                  onChange={(e)=> setEnv({ aiBaseUrl: e.target.value })}
                />
              </label>
              <label className="text-sm block">Server Base URL
                <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={serverBaseUrl} readOnly />
              </label>
            </div>
            <div className="mt-2 text-[11px] text-muted-foreground">Server key: {serverHasKey ? 'set' : 'not set'}</div>
            <div className="mt-3 flex items-center gap-2">
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={testingAI} onClick={onTestAI}>{testingAI ? 'Testing…' : 'Test endpoint'}</button>
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={savingAI} onClick={() => saveAiToServer(false)}>{savingAI ? 'Saving…' : 'Save to server'}</button>
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={savingAI} onClick={() => saveAiToServer(true)}>Clear server key</button>
              {aiMsg && <span className="text-xs text-emerald-600">{aiMsg}</span>}
              {aiErr && <span className="text-xs text-rose-600">{aiErr}</span>}
              {aiSaveMsg && <span className="text-xs text-emerald-600">{aiSaveMsg}</span>}
              {aiSaveErr && <span className="text-xs text-rose-600">{aiSaveErr}</span>}
            </div>
          </section>
          {/* Branding */}
          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">Branding</h3>
            <p className="text-xs text-muted-foreground mb-3">Used in document title, favicon, and UI logos when provided. Values are stored in your browser.</p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <label className="text-sm block">Organization Name
                <input
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  placeholder="e.g., Bayan Holdings"
                  value={env.orgName || ''}
                  onChange={(e)=> setEnv({ orgName: e.target.value })}
                />
              </label>
              <label className="text-sm block">Favicon URL
                <input
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  placeholder="/favicon.svg"
                  value={env.favicon || ''}
                  onChange={(e)=> setEnv({ favicon: e.target.value })}
                />
              </label>
              <label className="text-sm block">Light mode logo URL
                <input
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  placeholder="/logo.svg"
                  value={env.orgLogoLight || ''}
                  onChange={(e)=> setEnv({ orgLogoLight: e.target.value })}
                />
              </label>
              <label className="text-sm block">Dark mode logo URL
                <input
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  placeholder="/logo-dark.svg"
                  value={env.orgLogoDark || ''}
                  onChange={(e)=> setEnv({ orgLogoDark: e.target.value })}
                />
              </label>
            </div>
            <div className="mt-3 flex items-center gap-4">
              <div className="flex items-center gap-2">
                <img src={(env.orgLogoLight || '/logo.svg') as any} alt="Light logo preview" className="h-10 w-auto rounded border bg-white p-1" />
                <span className="text-xs text-muted-foreground">Light</span>
              </div>
              <div className="flex items-center gap-2">
                <img src={(env.orgLogoDark || '/logo-dark.svg') as any} alt="Dark logo preview" className="h-10 w-auto rounded border bg-black p-1" />
                <span className="text-xs text-muted-foreground">Dark</span>
              </div>
            </div>
            <div className="mt-3 flex items-center gap-2">
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={savingBranding} onClick={saveBrandingToServer}>{savingBranding ? 'Saving…' : 'Save to server'}</button>
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" disabled={savingBranding} onClick={reloadBrandingFromServer}>Reload from server</button>
              {brandingMsg && <span className="text-xs text-emerald-600">{brandingMsg}</span>}
              {brandingErr && <span className="text-xs text-rose-600">{brandingErr}</span>}
            </div>
          </section>

          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">Frontend</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <label className="text-sm block">Current origin
                <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={frontendOrigin} readOnly />
              </label>
              <label className="text-sm block">Port (info)
                <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={frontendPort} readOnly />
              </label>
            </div>
            <p className="text-xs text-muted-foreground mt-2">To change the frontend port, restart Next.js with a different port (e.g. <code>next dev -p 3001</code>). This panel only displays the current port.</p>
          </section>

          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">Public Links</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <label className="text-sm block">Effective public domain
                <input className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={effectivePublicDomain} readOnly />
              </label>
              <label className="text-sm block">Override public domain (optional)
                <input
                  placeholder={defaultPublicDomain || 'https://example.com'}
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  value={env.publicDomain || ''}
                  onChange={(e)=> setEnv({ publicDomain: e.target.value.replace(/\/$/, '') })}
                />
              </label>
            </div>
            <div className="mt-3 flex items-center gap-2">
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" onClick={()=> setEnv({ publicDomain: defaultPublicDomain })}>Use default</button>
              <button className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" type="button" onClick={()=> setEnv({ publicDomain: '' })}>Clear override</button>
            </div>
            <p className="text-xs text-muted-foreground mt-2">This domain is used when generating share links (Publish/View). If empty, we use the current origin.</p>
          </section>

          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">Week start</h3>
            <p className="text-xs text-muted-foreground mb-3">Controls how weeks are computed in charts, KPIs, and date parts when a per-widget setting is not specified.</p>
            <div className="flex items-center gap-2">
              <label className={`px-2 py-1 rounded-md border cursor-pointer ${env.weekStart === 'mon' ? 'bg-[hsl(var(--secondary))]' : ''}`}>
                <input type="radio" name="weekStart" className="mr-1" checked={env.weekStart === 'mon'} onChange={() => setEnv({ weekStart: 'mon' })} />
                Monday
              </label>
              <label className={`px-2 py-1 rounded-md border cursor-pointer ${env.weekStart === 'sun' ? 'bg-[hsl(var(--secondary))]' : ''}`}>
                <input type="radio" name="weekStart" className="mr-1" checked={env.weekStart === 'sun'} onChange={() => setEnv({ weekStart: 'sun' })} />
                Sunday
              </label>
              <label className={`px-2 py-1 rounded-md border cursor-pointer ${env.weekStart === 'sat' ? 'bg-[hsl(var(--secondary))]' : ''}`}>
                <input type="radio" name="weekStart" className="mr-1" checked={env.weekStart === 'sat'} onChange={() => setEnv({ weekStart: 'sat' })} />
                Saturday
              </label>
            </div>
            <p className="text-xs text-muted-foreground mt-2">This is a global default. Widgets can still override week start locally (e.g., x-axis or Deltas panel).</p>
          </section>

          {msg && <div className="text-sm text-emerald-600">{msg}</div>}
          {err && <div className="text-sm text-rose-600">{err}</div>}
        </div>
      </Card>
    </div>
    </Suspense>
  )
}
