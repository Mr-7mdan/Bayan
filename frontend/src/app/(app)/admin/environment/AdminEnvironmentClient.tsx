"use client"

import { Suspense, useEffect, useMemo, useState, useRef } from 'react'
import { useRouter } from 'next/navigation'
import { useTranslations } from 'next-intl'
import { Card, Title, Text } from '@tremor/react'
import { useAuth } from '@/components/providers/AuthProvider'
import { Button } from '@/components/ui'
import { Api } from '@/lib/api'
import { useEnvironment, BAYAN_DEFAULTS } from '@/components/providers/EnvironmentProvider'

function resolveDefaultApi(): string {
  return (process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:8000/api').replace(/\/$/, '')
}

export default function AdminEnvironmentPage() {
  const t = useTranslations('data')
  const { user, loading } = useAuth() as any
  const router = useRouter()
  const isAdmin = (user?.role || '').toLowerCase() === 'admin'
  const { env, setEnv } = useEnvironment()
  const defaultPublicDomain = useMemo(() => (process.env.NEXT_PUBLIC_PUBLIC_DOMAIN || '').replace(/\/$/, ''), [])
  const effectivePublicDomain = (env.publicDomain && env.publicDomain.trim()) ? env.publicDomain.replace(/\/$/, '') : defaultPublicDomain

  const [apiOverride, setApiOverride] = useState('')
  const [effectiveApi, setEffectiveApi] = useState('')
  const [frontendOrigin, setFrontendOrigin] = useState('')
  const [frontendPort, setFrontendPort] = useState('')

  // Local copies of env text fields — prevents global re-render on every keystroke.
  // setEnv (global) is called only on onBlur to avoid layout shifts / scroll jumps.
  const [localAiProvider, setLocalAiProvider] = useState<string>('')
  const [localAiModel, setLocalAiModel] = useState('')
  const [localAiApiKey, setLocalAiApiKey] = useState('')
  const [localAiBaseUrl, setLocalAiBaseUrl] = useState('')
  const [localOrgName, setLocalOrgName] = useState('')
  const [localFavicon, setLocalFavicon] = useState('')
  const [localLogoLight, setLocalLogoLight] = useState('')
  const [localLogoDark, setLocalLogoDark] = useState('')
  const [localPublicDomain, setLocalPublicDomain] = useState('')
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
  const [promoteBusy, setPromoteBusy] = useState<null | 'backend' | 'frontend'>(null)
  const [issuesBusy, setIssuesBusy] = useState(false)
  const [issuesMsg, setIssuesMsg] = useState<string | null>(null)
  const [issuesErr, setIssuesErr] = useState<string | null>(null)

  useEffect(() => {
    if (loading) return
    if (user && !isAdmin) router.replace('/home')
  }, [loading, user, isAdmin, router])

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

  // Sync local text state from env whenever env changes (e.g. after server load / branding reload).
  // For branding fields we treat values that match the Bayan default as "no override" so the
  // input box stays empty (placeholder visible) and the "using default" indicator is correct.
  useEffect(() => {
    setLocalAiProvider(env.aiProvider || 'gemini')
    setLocalAiModel(env.aiModel || '')
    setLocalAiApiKey(env.aiApiKey || '')
    setLocalAiBaseUrl(env.aiBaseUrl || '')
    setLocalOrgName((env.orgName && env.orgName !== BAYAN_DEFAULTS.orgName) ? env.orgName : '')
    setLocalFavicon((env.favicon && env.favicon !== BAYAN_DEFAULTS.favicon) ? env.favicon : '')
    setLocalLogoLight((env.orgLogoLight && env.orgLogoLight !== BAYAN_DEFAULTS.orgLogoLight) ? env.orgLogoLight : '')
    setLocalLogoDark((env.orgLogoDark && env.orgLogoDark !== BAYAN_DEFAULTS.orgLogoDark) ? env.orgLogoDark : '')
    setLocalPublicDomain(env.publicDomain || '')
  }, [env])

  const checkUpdates = async () => {
    setCheckingUpd(true)
    try {
      const b = await Api.updatesCheck('backend')
      const f = await Api.updatesCheck('frontend')
      setUpdB(b); setUpdF(f)
    } catch (e: any) {
      setErr(e?.message || t('admin.environment.failedCheckUpdates'))
    } finally { setCheckingUpd(false) }
  }

  const applyUpdate = async (component: 'backend'|'frontend') => {
    if (!user?.id) { setErr(t('admin.environment.loginRequired')); return }
    setApplyBusy(component)
    try {
      const res = await Api.updatesApply(component, user.id)
      setMsg(t('admin.environment.staged', { component, version: res.version }))
      window.setTimeout(() => setMsg(null), 2000)
    } catch (e: any) {
      setErr(e?.message || t('admin.environment.failedApplyUpdate'))
    } finally { setApplyBusy(null) }
  }

  const promoteUpdate = async (component: 'backend'|'frontend') => {
    if (!user?.id) { setErr(t('admin.environment.loginRequired')); return }
    setPromoteBusy(component)
    try {
      const res = await Api.updatesPromote(component, user.id, { restart: true })
      const extra = res.restarted ? ' ' + t('admin.environment.serviceRestarted') : (res.message ? ` (${res.message})` : '')
      setMsg(t('admin.environment.promoted', { component, version: res.version, extra }))
      window.setTimeout(() => setMsg(null), 2500)
      // Refresh check panel after promotion
      try { await checkUpdates() } catch {}
    } catch (e: any) {
      setErr(e?.message || t('admin.environment.failedPromoteUpdate'))
    } finally { setPromoteBusy(null) }
  }

  const didRunAiRef = useRef(false)
  useEffect(() => {
    if (didRunAiRef.current) return
    try { if (typeof window !== 'undefined') { if (window.sessionStorage.getItem('once_ai_config') === '1') return; window.sessionStorage.setItem('once_ai_config', '1') } } catch {}
    didRunAiRef.current = true
    ;(async () => {
      try {
        const cfg = await Api.getAiConfig()
        setServerHasKey(!!cfg?.hasKey)
        if (cfg?.provider) setEnv({ aiProvider: cfg.provider as any })
        if (cfg?.model) setEnv({ aiModel: cfg.model })
        if ((cfg as any)?.baseUrl) setServerBaseUrl(String((cfg as any).baseUrl))
      } catch {}
    })()
  }, [])

  // Branding is already loaded by EnvironmentProvider; this panel reads and optionally saves.

  const onSave = () => {
    try {
      if (apiOverride && /^https?:\/\//i.test(apiOverride)) {
        localStorage.setItem('api_base_override', apiOverride)
        setEffectiveApi(apiOverride.replace(/\/$/, ''))
        setMsg(t('admin.environment.savedApiOverride'))
      } else {
        localStorage.removeItem('api_base_override')
        setEffectiveApi(resolveDefaultApi())
        setMsg(t('admin.environment.clearedApiOverride'))
      }
      setErr(null)
      window.setTimeout(() => setMsg(null), 1600)
    } catch (e: any) {
      setErr(e?.message || t('admin.environment.failedSaveOverride'))
      setMsg(null)
    }
  }

  const onTest = async () => {
    setTesting(true); setErr(null); setMsg(null)
    try {
      // Trigger a simple request against current effective base
      await Api.getBranding()
      setMsg(t('admin.environment.apiReachable'))
      window.setTimeout(() => setMsg(null), 1600)
    } catch (e: any) {
      setErr(e?.message || t('admin.environment.failedReachApi'))
    } finally { setTesting(false) }
  }

  const onTestAI = async () => {
    setTestingAI(true); setAiErr(null); setAiMsg(null)
    try {
      const provider = (localAiProvider || 'gemini') as any
      const model = localAiModel || (provider === 'openai' ? 'gpt-4o-mini' : (provider === 'mistral' ? 'mistral-small' : 'gemini-1.5-flash'))
      const apiKey = localAiApiKey || ''
      if (!apiKey && !serverHasKey) throw new Error(t('admin.environment.enterApiKeyOrSave'))
      await Api.aiDescribe({ provider, model, apiKey, schema: { table: 'test', columns: [{ name: 'id', type: 'string' }] }, samples: [] })
      setAiMsg(t('admin.environment.aiEndpointOk'))
      window.setTimeout(() => setAiMsg(null), 2000)
    } catch (e: any) {
      setAiErr(e?.message || t('admin.environment.aiEndpointFailed'))
    } finally { setTestingAI(false) }
  }

  const onTestIssues = async () => {
    setIssuesBusy(true); setIssuesErr(null); setIssuesMsg(null)
    try {
      const r = await Api.issuesTest()
      const m = r?.issueUrl ? t('admin.environment.issueCreated', { url: r.issueUrl }) : t('admin.environment.ok')
      setIssuesMsg(m)
      window.setTimeout(() => setIssuesMsg(null), 2000)
    } catch (e: any) {
      setIssuesErr(e?.message || t('admin.environment.failedCreateIssue'))
    } finally { setIssuesBusy(false) }
  }

  const saveAiToServer = async (clearKey?: boolean) => {
    setSavingAI(true); setAiSaveErr(null); setAiSaveMsg(null)
    // Flush local state to env before saving
    setEnv({ aiProvider: localAiProvider as any, aiModel: localAiModel, aiApiKey: localAiApiKey, aiBaseUrl: localAiBaseUrl })
    try {
      const provider = (localAiProvider || 'gemini') as any
      const model = localAiModel || (provider === 'openai' ? 'gpt-4o-mini' : (provider === 'mistral' ? 'mistral-small' : 'gemini-1.5-flash'))
      const body: any = { provider, model }
      if (clearKey === true) body.apiKey = ''
      else if (localAiApiKey.trim()) body.apiKey = localAiApiKey
      await Api.putAiConfig(body, user?.id)
      if (clearKey === true) setServerHasKey(false)
      else if (localAiApiKey.trim()) setServerHasKey(true)
      setAiSaveMsg(t('admin.environment.saved'))
      window.setTimeout(() => setAiSaveMsg(null), 1500)
    } catch (e: any) {
      setAiSaveErr(e?.message || t('admin.environment.failedSave'))
    } finally { setSavingAI(false) }
  }

  const saveBrandingToServer = async () => {
    setSavingBranding(true); setBrandingMsg(null); setBrandingErr(null)
    // Flush local state to env before saving
    setEnv({ orgName: localOrgName, orgLogoLight: localLogoLight, orgLogoDark: localLogoDark, favicon: localFavicon })
    try {
      const res = await Api.putAdminBranding({
        orgName: localOrgName,
        logoLight: localLogoLight,
        logoDark: localLogoDark,
        favicon: localFavicon,
      }, user?.id)
      setEnv({
        orgName: res.orgName || '',
        orgLogoLight: res.logoLight || '',
        orgLogoDark: res.logoDark || '',
        favicon: res.favicon || '',
      })
      setBrandingMsg(t('admin.environment.saved'))
      window.setTimeout(() => setBrandingMsg(null), 1500)
    } catch (e: any) {
      setBrandingErr(e?.message || t('admin.environment.failedSave'))
    } finally { setSavingBranding(false) }
  }

  const restoreBayanDefaults = async () => {
    if (!confirm(t('admin.environment.confirmRestoreDefaults'))) return
    setSavingBranding(true); setBrandingMsg(null); setBrandingErr(null)
    try {
      const res = await Api.resetAdminBranding(user?.id)
      // Server returns the effective values (Bayan defaults filled in).
      setLocalOrgName(''); setLocalLogoLight(''); setLocalLogoDark(''); setLocalFavicon('')
      setEnv({
        orgName: res.orgName || BAYAN_DEFAULTS.orgName,
        orgLogoLight: res.logoLight || BAYAN_DEFAULTS.orgLogoLight,
        orgLogoDark: res.logoDark || BAYAN_DEFAULTS.orgLogoDark,
        favicon: res.favicon || BAYAN_DEFAULTS.favicon,
      })
      setBrandingMsg(t('admin.environment.restoredDefaults'))
      window.setTimeout(() => setBrandingMsg(null), 1800)
    } catch (e: any) {
      setBrandingErr(e?.message || t('admin.environment.failedResetBranding'))
    } finally { setSavingBranding(false) }
  }

  // Reset a single branding override (PUT empty string → server clears it,
  // GET will fall back to the Bayan default for that field).
  const resetBrandingField = async (key: 'orgName' | 'logoLight' | 'logoDark' | 'favicon') => {
    setSavingBranding(true); setBrandingMsg(null); setBrandingErr(null)
    try {
      const payload: Record<string, string> = { [key]: '' }
      const res = await Api.putAdminBranding(payload as any, user?.id)
      const next = {
        orgName: res.orgName || BAYAN_DEFAULTS.orgName,
        orgLogoLight: res.logoLight || BAYAN_DEFAULTS.orgLogoLight,
        orgLogoDark: res.logoDark || BAYAN_DEFAULTS.orgLogoDark,
        favicon: res.favicon || BAYAN_DEFAULTS.favicon,
      }
      setEnv(next)
      // Clear the local input so the placeholder (showing the default) is visible
      if (key === 'orgName') setLocalOrgName('')
      if (key === 'logoLight') setLocalLogoLight('')
      if (key === 'logoDark') setLocalLogoDark('')
      if (key === 'favicon') setLocalFavicon('')
      setBrandingMsg(t('admin.environment.resetToBayanDefault'))
      window.setTimeout(() => setBrandingMsg(null), 1500)
    } catch (e: any) {
      setBrandingErr(e?.message || t('admin.environment.failedReset'))
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
      setBrandingMsg(t('admin.environment.loadedFromServer'))
      window.setTimeout(() => setBrandingMsg(null), 1500)
    } catch (e: any) {
      setBrandingErr(e?.message || t('admin.environment.failedLoad'))
    } finally { setSavingBranding(false) }
  }

  if (!isAdmin) return null

  return (
    <Suspense fallback={<div className="p-3 text-sm">{t('admin.environment.loading')}</div>}>
    <div className="space-y-3">
      <Card className="p-0 bg-[hsl(var(--background))]">
        <div className="flex items-center justify-between px-3 py-2 bg-[hsl(var(--background))] border-b border-[hsl(var(--border))]">
          <div>
            <Title className="text-gray-500 dark:text-white">{t('admin.environment.title')}</Title>
            <Text className="mt-0 text-gray-500 dark:text-white">{t('admin.environment.subtitle')}</Text>
          </div>
        </div>
        <div className="p-4 space-y-4">
          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">{t('admin.environment.backendApi')}</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div>
                <label className="text-sm block">{t('admin.environment.currentEffectiveApiBase')}
                  <input name="effective_api_base" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={effectiveApi} readOnly autoComplete="off" />
                </label>
              </div>
              <div>
                <label className="text-sm block">{t('admin.environment.overrideApiBase')}
                  <input name="api_base_override" placeholder="https://api.example.com/api" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={apiOverride} onChange={(e) => setApiOverride(e.target.value)} autoComplete="url" />
                </label>
              </div>
            </div>
            <div className="mt-3 flex items-center gap-2">
              <Button size="sm" variant="primary" type="button" onClick={onSave}>{t('admin.environment.save')}</Button>
              <Button size="sm" variant="outline" type="button" disabled={testing} onClick={onTest}>{testing ? t('admin.environment.testing') : t('admin.environment.testApi')}</Button>
              <Button size="sm" variant="outline" type="button" onClick={() => { setApiOverride(''); onSave() }}>{t('admin.environment.resetToDefault')}</Button>
            </div>
          </section>

          {/* Updates */}
          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">{t('admin.environment.updates')}</h3>
            <p className="text-xs text-muted-foreground mb-3">{t('admin.environment.updatesDesc')}</p>
            <div className="flex items-center gap-2 mb-3">
              <Button size="sm" variant="outline" type="button" disabled={checkingUpd} onClick={checkUpdates}>{checkingUpd ? t('admin.environment.checking') : t('admin.environment.checkUpdates')}</Button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <div className="rounded-md border p-2">
                <div className="text-sm font-medium">{t('admin.environment.backend')}</div>
                <div className="text-xs text-muted-foreground">{t('admin.environment.currentLatest', { current: updB?.currentVersion || '—', latest: updB?.latestVersion || '—' })}</div>
                <div className="text-xs mt-1">{t('admin.environment.type', { type: updB?.updateType || '—' })}{updB?.requiresMigrations ? ' ' + t('admin.environment.requiresMigrations') : ''}</div>
                {updB?.releaseNotes && (
                  <details className="mt-2 text-xs">
                    <summary className="cursor-pointer select-none">{t('admin.environment.releaseNotes')}</summary>
                    <div className="mt-1 whitespace-pre-wrap">{updB.releaseNotes}</div>
                  </details>
                )}
                <div className="mt-2">
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={!updB?.enabled || updB?.updateType !== 'auto' || updB?.requiresMigrations || applyBusy === 'backend'}
                    onClick={() => applyUpdate('backend')}
                  >{applyBusy === 'backend' ? t('admin.environment.applying') : t('admin.environment.applyAutoUpdate')}</Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="ml-2"
                    disabled={promoteBusy === 'backend'}
                    onClick={() => promoteUpdate('backend')}
                  >{promoteBusy === 'backend' ? t('admin.environment.promoting') : t('admin.environment.promoteRestart')}</Button>
                </div>
              </div>
              <div className="rounded-md border p-2">
                <div className="text-sm font-medium">{t('admin.environment.frontend')}</div>
                <div className="text-xs text-muted-foreground">{t('admin.environment.currentLatest', { current: updF?.currentVersion || '—', latest: updF?.latestVersion || '—' })}</div>
                <div className="text-xs mt-1">{t('admin.environment.type', { type: updF?.updateType || '—' })}{updF?.requiresMigrations ? ' ' + t('admin.environment.requiresMigrations') : ''}</div>
                {updF?.releaseNotes && (
                  <details className="mt-2 text-xs">
                    <summary className="cursor-pointer select-none">{t('admin.environment.releaseNotes')}</summary>
                    <div className="mt-1 whitespace-pre-wrap">{updF.releaseNotes}</div>
                  </details>
                )}
                <div className="mt-2">
                  <Button
                    size="sm"
                    variant="primary"
                    disabled={!updF?.enabled || updF?.updateType !== 'auto' || updF?.requiresMigrations || applyBusy === 'frontend'}
                    onClick={() => applyUpdate('frontend')}
                  >{applyBusy === 'frontend' ? t('admin.environment.applying') : t('admin.environment.applyAutoUpdate')}</Button>
                  <Button
                    size="sm"
                    variant="outline"
                    className="ml-2"
                    disabled={promoteBusy === 'frontend'}
                    onClick={() => promoteUpdate('frontend')}
                  >{promoteBusy === 'frontend' ? t('admin.environment.promoting') : t('admin.environment.promoteRestart')}</Button>
                </div>
              </div>
            </div>
          </section>

          {/* AI Features */}
          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">{t('admin.environment.aiFeatures')}</h3>
            <form onSubmit={(e)=>{e.preventDefault()}}>
              <p className="text-xs text-muted-foreground mb-3">{t('admin.environment.aiFeaturesDesc')}</p>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                <label className="text-sm block">{t('admin.environment.provider')}
                  <select
                    name="ai_provider"
                    className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                    value={localAiProvider || 'gemini'}
                    onChange={(e)=> { setLocalAiProvider(e.target.value); setEnv({ aiProvider: e.target.value as any }) }}
                  >
                    <option value="gemini">Gemini</option>
                    <option value="openai">OpenAI</option>
                    <option value="mistral">Mistral</option>
                    <option value="anthropic">Anthropic</option>
                    <option value="openrouter">OpenRouter</option>
                  </select>
                </label>
                <label className="text-sm block">{t('admin.environment.model')}
                  <input
                    name="ai_model"
                    className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                    placeholder={localAiProvider === 'gemini' ? 'gemini-1.5-flash' : localAiProvider === 'openai' ? 'gpt-4o-mini' : 'mistral-small'}
                    value={localAiModel}
                    onChange={(e)=> setLocalAiModel(e.target.value)}
                    onBlur={(e)=> setEnv({ aiModel: e.target.value })}
                    autoComplete="off"
                  />
                </label>
                <label className="text-sm block">{t('admin.environment.apiKey')}
                  <input
                    name="ai_api_key"
                    type="password"
                    className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                    placeholder={t('admin.environment.enterApiKey')}
                    value={localAiApiKey}
                    onChange={(e)=> setLocalAiApiKey(e.target.value)}
                    onBlur={(e)=> setEnv({ aiApiKey: e.target.value })}
                    autoComplete="new-password"
                  />
                </label>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mt-3">
                <label className="text-sm block">{t('admin.environment.aiBaseUrl')}
                  <input
                    name="ai_base_url"
                    className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                    placeholder={localAiProvider === 'openai' ? 'https://api.openai.com/v1' : localAiProvider === 'mistral' ? 'https://api.mistral.ai/v1' : localAiProvider === 'anthropic' ? 'https://api.anthropic.com/v1' : localAiProvider === 'openrouter' ? 'https://openrouter.ai/api/v1' : ''}
                    value={localAiBaseUrl}
                    onChange={(e)=> setLocalAiBaseUrl(e.target.value)}
                    onBlur={(e)=> setEnv({ aiBaseUrl: e.target.value })}
                    autoComplete="url"
                  />
                </label>
                <label className="text-sm block">{t('admin.environment.serverBaseUrl')}
                  <input name="ai_server_base_url" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={serverBaseUrl} readOnly autoComplete="off" />
                </label>
              </div>
              <div className="mt-2 text-[11px] text-muted-foreground">{t('admin.environment.serverKey', { status: serverHasKey ? t('admin.environment.set') : t('admin.environment.notSet') })}</div>
              <div className="mt-3 flex items-center gap-2">
                <Button size="sm" variant="outline" type="button" disabled={testingAI} onClick={onTestAI}>{testingAI ? t('admin.environment.testing') : t('admin.environment.testEndpoint')}</Button>
                <Button size="sm" variant="primary" type="button" disabled={savingAI} onClick={() => saveAiToServer(false)}>{savingAI ? t('admin.environment.saving') : t('admin.environment.saveToServer')}</Button>
                <Button size="sm" variant="outline" type="button" disabled={savingAI} onClick={() => saveAiToServer(true)}>{t('admin.environment.clearServerKey')}</Button>
                {aiMsg && <span className="text-xs text-emerald-600">{aiMsg}</span>}
                {aiErr && <span className="text-xs text-rose-600">{aiErr}</span>}
                {aiSaveMsg && <span className="text-xs text-emerald-600">{aiSaveMsg}</span>}
                {aiSaveErr && <span className="text-xs text-rose-600">{aiSaveErr}</span>}
              </div>
            </form>
          </section>
          {/* Branding */}
          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <div className="flex items-start justify-between gap-3 mb-2">
              <div>
                <h3 className="text-sm font-semibold">{t('admin.environment.branding')}</h3>
                <p className="text-xs text-muted-foreground mt-0.5">
                  {t('admin.environment.brandingDesc')}{' '}
                  <span className="font-medium text-foreground">{t('admin.environment.bayanDefault')}</span>.
                </p>
              </div>
              <Button
                type="button"
                size="sm"
                variant="outline"
                className="shrink-0"
                disabled={savingBranding}
                onClick={restoreBayanDefaults}
                title={t('admin.environment.restoreDefaultsTitle')}
              >
                {t('admin.environment.restoreDefaults')}
              </Button>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <BrandingField
                label={t('admin.environment.orgName')}
                name="org_name"
                value={localOrgName}
                placeholder={BAYAN_DEFAULTS.orgName}
                isUsingDefault={!localOrgName.trim()}
                disabled={savingBranding}
                onChange={(v)=> setLocalOrgName(v)}
                onCommit={(v)=> setEnv({ orgName: v })}
                onResetAction={()=> resetBrandingField('orgName')}
                autoComplete="organization"
              />
              <BrandingField
                label={t('admin.environment.faviconUrl')}
                name="org_favicon"
                value={localFavicon}
                placeholder={BAYAN_DEFAULTS.favicon}
                isUsingDefault={!localFavicon.trim()}
                disabled={savingBranding}
                onChange={(v)=> setLocalFavicon(v)}
                onCommit={(v)=> setEnv({ favicon: v })}
                onResetAction={()=> resetBrandingField('favicon')}
                autoComplete="url"
              />
              <BrandingField
                label={t('admin.environment.logoLightUrl')}
                name="org_logo_light"
                value={localLogoLight}
                placeholder={BAYAN_DEFAULTS.orgLogoLight}
                isUsingDefault={!localLogoLight.trim()}
                disabled={savingBranding}
                onChange={(v)=> setLocalLogoLight(v)}
                onCommit={(v)=> setEnv({ orgLogoLight: v })}
                onResetAction={()=> resetBrandingField('logoLight')}
                autoComplete="url"
              />
              <BrandingField
                label={t('admin.environment.logoDarkUrl')}
                name="org_logo_dark"
                value={localLogoDark}
                placeholder={BAYAN_DEFAULTS.orgLogoDark}
                isUsingDefault={!localLogoDark.trim()}
                disabled={savingBranding}
                onChange={(v)=> setLocalLogoDark(v)}
                onCommit={(v)=> setEnv({ orgLogoDark: v })}
                onResetAction={()=> resetBrandingField('logoDark')}
                autoComplete="url"
              />
            </div>
            <div className="mt-3 flex items-center gap-4 flex-wrap">
              <div className="flex items-center gap-2">
                <img src={(localLogoLight || BAYAN_DEFAULTS.orgLogoLight) as any} alt={t('admin.environment.lightLogoPreview')} className="h-10 w-auto rounded border bg-white p-1" />
                <span className="text-xs text-muted-foreground">{t('admin.environment.light')}{!localLogoLight.trim() && <span className="ml-1 text-[10px] text-muted-foreground/70">{t('admin.environment.dotDefault')}</span>}</span>
              </div>
              <div className="flex items-center gap-2">
                <img src={(localLogoDark || BAYAN_DEFAULTS.orgLogoDark) as any} alt={t('admin.environment.darkLogoPreview')} className="h-10 w-auto rounded border bg-black p-1" />
                <span className="text-xs text-muted-foreground">{t('admin.environment.dark')}{!localLogoDark.trim() && <span className="ml-1 text-[10px] text-muted-foreground/70">{t('admin.environment.dotDefault')}</span>}</span>
              </div>
            </div>
            <div className="mt-3 flex items-center gap-2 flex-wrap">
              <Button size="sm" variant="primary" type="button" disabled={savingBranding} onClick={saveBrandingToServer}>{savingBranding ? t('admin.environment.saving') : t('admin.environment.saveToServer')}</Button>
              <Button size="sm" variant="outline" type="button" disabled={savingBranding} onClick={reloadBrandingFromServer}>{t('admin.environment.reloadFromServer')}</Button>
              {brandingMsg && <span className="text-xs text-emerald-600">{brandingMsg}</span>}
              {brandingErr && <span className="text-xs text-rose-600">{brandingErr}</span>}
            </div>
          </section>

          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">{t('admin.environment.frontend')}</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <label className="text-sm block">{t('admin.environment.currentOrigin')}
                <input name="frontend_origin" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={frontendOrigin} readOnly autoComplete="off" />
              </label>
              <label className="text-sm block">{t('admin.environment.portInfo')}
                <input name="frontend_port" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={frontendPort} readOnly autoComplete="off" />
              </label>
            </div>
            <p className="text-xs text-muted-foreground mt-2">{t('admin.environment.frontendPortDescBefore')}<code>next dev -p 3001</code>{t('admin.environment.frontendPortDescAfter')}</p>
          </section>

          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">{t('admin.environment.publicLinks')}</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <label className="text-sm block">{t('admin.environment.effectivePublicDomain')}
                <input name="effective_public_domain" className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={effectivePublicDomain} readOnly autoComplete="off" />
              </label>
              <label className="text-sm block">{t('admin.environment.overridePublicDomain')}
                <input
                  name="public_domain_override"
                  placeholder={defaultPublicDomain || 'https://example.com'}
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  value={localPublicDomain}
                  onChange={(e)=> setLocalPublicDomain(e.target.value)}
                  onBlur={(e)=> setEnv({ publicDomain: e.target.value.replace(/\/$/, '') })}
                  autoComplete="url"
                />
              </label>
            </div>
            <div className="mt-3 flex items-center gap-2">
              <Button size="sm" variant="outline" type="button" onClick={()=> { setLocalPublicDomain(defaultPublicDomain); setEnv({ publicDomain: defaultPublicDomain }) }}>{t('admin.environment.useDefault')}</Button>
              <Button size="sm" variant="outline" type="button" onClick={()=> { setLocalPublicDomain(''); setEnv({ publicDomain: '' }) }}>{t('admin.environment.clearOverride')}</Button>
            </div>
            <p className="text-xs text-muted-foreground mt-2">{t('admin.environment.publicLinksDesc')}</p>
          </section>

          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">{t('admin.environment.issuesBugReporting')}</h3>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
              <label className="text-sm block">{t('admin.environment.reportingMode')}
                <select
                  name="bug_report_mode"
                  className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background"
                  value={env.bugReportMode || 'off'}
                  onChange={(e)=> setEnv({ bugReportMode: e.target.value as any })}
                >
                  <option value="auto">{t('admin.environment.submitAutomatically')}</option>
                  <option value="ask">{t('admin.environment.askUser')}</option>
                  <option value="off">{t('admin.environment.dontReport')}</option>
                </select>
              </label>
            </div>
            <div className="mt-3 flex items-center gap-2">
              <Button size="sm" variant="outline" type="button" disabled={issuesBusy} onClick={onTestIssues}>{issuesBusy ? t('admin.environment.testing') : t('admin.environment.testGithubToken')}</Button>
              {issuesMsg && <span className="text-xs text-emerald-600">{issuesMsg}</span>}
              {issuesErr && <span className="text-xs text-rose-600">{issuesErr}</span>}
            </div>
          </section>

          <section className="rounded-md border p-3 bg-[hsl(var(--card))]">
            <h3 className="text-sm font-semibold mb-2">{t('admin.environment.weekStart')}</h3>
            <p className="text-xs text-muted-foreground mb-3">{t('admin.environment.weekStartDesc')}</p>
            <div className="flex items-center gap-2">
              <label className={`px-2 py-1 rounded-md border cursor-pointer ${env.weekStart === 'mon' ? 'bg-[hsl(var(--secondary))]' : ''}`}>
                <input type="radio" name="weekStart" className="mr-1" checked={env.weekStart === 'mon'} onChange={() => setEnv({ weekStart: 'mon' })} />
                {t('admin.environment.monday')}
              </label>
              <label className={`px-2 py-1 rounded-md border cursor-pointer ${env.weekStart === 'sun' ? 'bg-[hsl(var(--secondary))]' : ''}`}>
                <input type="radio" name="weekStart" className="mr-1" checked={env.weekStart === 'sun'} onChange={() => setEnv({ weekStart: 'sun' })} />
                {t('admin.environment.sunday')}
              </label>
              <label className={`px-2 py-1 rounded-md border cursor-pointer ${env.weekStart === 'sat' ? 'bg-[hsl(var(--secondary))]' : ''}`}>
                <input type="radio" name="weekStart" className="mr-1" checked={env.weekStart === 'sat'} onChange={() => setEnv({ weekStart: 'sat' })} />
                {t('admin.environment.saturday')}
              </label>
            </div>
            <p className="text-xs text-muted-foreground mt-2">{t('admin.environment.weekStartNote')}</p>
          </section>

          {msg && <div className="text-sm text-emerald-600">{msg}</div>}
          {err && <div className="text-sm text-rose-600">{err}</div>}
        </div>
      </Card>
    </div>
    </Suspense>
  )
}

// ── Branding field with inline "default" indicator + reset button ──────
function BrandingField({
  label,
  name,
  value,
  placeholder,
  isUsingDefault,
  disabled,
  onChange,
  onCommit,
  onResetAction,
  autoComplete,
}: {
  label: string
  name: string
  value: string
  placeholder: string
  isUsingDefault: boolean
  disabled?: boolean
  onChange: (v: string) => void
  onCommit: (v: string) => void
  onResetAction: () => void
  autoComplete?: string
}) {
  const t = useTranslations('data')
  return (
    <div className="text-sm block">
      <div className="flex items-center justify-between mb-1">
        <label htmlFor={name} className="block">{label}</label>
        {isUsingDefault ? (
          <span
            className="text-[10px] text-muted-foreground/70 italic"
            title={t('admin.environment.usingDefaultTitle')}
          >
            {t('admin.environment.usingDefault')}
          </span>
        ) : (
          <button
            type="button"
            className="text-[10px] text-muted-foreground hover:text-foreground transition-colors duration-150 cursor-pointer underline-offset-2 hover:underline disabled:opacity-50 disabled:cursor-not-allowed"
            onClick={onResetAction}
            disabled={disabled}
            title={t('admin.environment.resetFieldTitle')}
          >
            {t('admin.environment.resetToDefault')}
          </button>
        )}
      </div>
      <input
        id={name}
        name={name}
        className="w-full px-2 py-1.5 rounded-md border bg-background"
        placeholder={placeholder}
        value={value}
        onChange={(e)=> onChange(e.target.value)}
        onBlur={(e)=> onCommit(e.target.value)}
        autoComplete={autoComplete}
        disabled={disabled}
      />
    </div>
  )
}
