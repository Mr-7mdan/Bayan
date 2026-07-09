"use client"

import { Card, Title, Text } from '@tremor/react'
import { useTranslations } from 'next-intl'
import { useAuth } from '@/components/providers/AuthProvider'
import { useRouter, useSearchParams } from 'next/navigation'
import { useEffect, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { Api } from '@/lib/api'
import { useTheme } from '@/components/providers/ThemeProvider'
import ThemeToggle from '@/components/ui/ThemeToggle'
import { Button, Input } from '@/components/ui'

export default function LoginPage() {
  const t = useTranslations('login')
  const { login } = useAuth()
  const router = useRouter()
  const searchParams = useSearchParams()
  // Only honor same-origin relative paths from ?next= to avoid open redirects.
  const nextPath = (() => {
    const n = searchParams?.get('next') || ''
    return n.startsWith('/') && !n.startsWith('//') ? n : '/home'
  })()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [remember, setRemember] = useState(true)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  // Versions for footer
  const [backendVer, setBackendVer] = useState<string | null>(null)
  const [frontendVer, setFrontendVer] = useState<string | null>(null)
  // Signup dialog state
  const [signupOpen, setSignupOpen] = useState(false)
  const [suName, setSuName] = useState('')
  const [suEmail, setSuEmail] = useState('')
  const [suPassword, setSuPassword] = useState('')
  const [suBusy, setSuBusy] = useState(false)
  const [suError, setSuError] = useState<string | null>(null)
  // Reset password dialog state
  const [resetOpen, setResetOpen] = useState(false)
  const [rpEmail, setRpEmail] = useState('')
  const [rpBusy, setRpBusy] = useState(false)
  const [rpMsg, setRpMsg] = useState<string | null>(null)
  const { resolved, darkVariant, setDarkVariant } = useTheme()
  const [mounted, setMounted] = useState(false)
  useEffect(() => setMounted(true), [])

  // Load remembered email if enabled
  useEffect(() => {
    try {
      const flag = localStorage.getItem('remember_me')
      if (flag === '1' || flag === 'true') {
        setRemember(true)
        const saved = localStorage.getItem('saved_email') || ''
        if (saved) setEmail(saved)
      } else if (flag === '0' || flag === 'false') {
        setRemember(false)
      }
    } catch {}
  }, [])
  // Load versions (best-effort)
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const v = await Api.updatesVersion()
        if (cancelled) return
        setBackendVer((v?.backend || '') || null)
        setFrontendVer((v?.frontend || '') || null)
      } catch {}
    })()
    return () => { cancelled = true }
  }, [])

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setLoading(true)
    setError(null)
    try {
      await login(email, password, remember)
      // Persist or clear remembered email
      try {
        if (remember) {
          localStorage.setItem('remember_me', '1')
          localStorage.setItem('saved_email', email)
        } else {
          localStorage.removeItem('remember_me')
          localStorage.removeItem('saved_email')
        }
      } catch {}
      router.replace(nextPath as any)
    } catch (err: unknown) {
      // Friendly error messages for common auth failures
      let msg = t('errorGeneric')
      try {
        const m = (err instanceof Error ? err.message : String(err || '')).toLowerCase()
        if (m.includes('http 401') || m.includes('invalid credentials')) {
          msg = t('errorInvalid')
        } else if (m.includes('http 429')) {
          msg = t('errorRateLimited')
        } else if (m.includes('http 5')) {
          msg = t('errorServer')
        } else if (m.includes('timed out')) {
          msg = t('errorTimeout')
        }
      } catch {}
      setError(msg)
    } finally { setLoading(false) }
  }

  return (
    <div className="min-h-screen flex bg-[hsl(var(--background))]">
      {/* Brand / product framing panel (desktop only) */}
      <aside className="relative hidden lg:flex lg:w-1/2 xl:w-3/5 flex-col justify-between overflow-hidden border-e border-[hsl(var(--border))] bg-[hsl(var(--card))] p-12">
        <div className="flex items-center gap-2">
          <img src="/logo.svg" alt="Bayan" className="h-9 w-auto block dark:hidden" />
          <img src="/logo-dark.svg" alt="Bayan" className="h-9 w-auto hidden dark:block" />
        </div>
        <div className="relative z-[1] max-w-lg">
          <h1 className="text-2xl font-semibold text-foreground">{t('title')}</h1>
          <p className="mt-3 text-base text-muted-foreground">{t('subtitle')}</p>
        </div>
        {/* Subtle inline data-viz silhouette in token colors */}
        <svg
          aria-hidden
          viewBox="0 0 480 200"
          className="pointer-events-none absolute -bottom-2 end-0 w-[110%] max-w-none text-[hsl(var(--primary))] opacity-[0.18]"
          fill="none"
        >
          <rect x="20" y="120" width="34" height="70" rx="4" fill="currentColor" opacity="0.5" />
          <rect x="72" y="90" width="34" height="100" rx="4" fill="currentColor" opacity="0.6" />
          <rect x="124" y="140" width="34" height="50" rx="4" fill="currentColor" opacity="0.5" />
          <rect x="176" y="70" width="34" height="120" rx="4" fill="currentColor" opacity="0.7" />
          <rect x="228" y="110" width="34" height="80" rx="4" fill="currentColor" opacity="0.5" />
          <rect x="280" y="50" width="34" height="140" rx="4" fill="currentColor" opacity="0.7" />
          <rect x="332" y="100" width="34" height="90" rx="4" fill="currentColor" opacity="0.55" />
          <rect x="384" y="130" width="34" height="60" rx="4" fill="currentColor" opacity="0.5" />
          <path
            d="M20 96 L89 70 L141 104 L193 44 L245 82 L297 30 L349 74 L418 54"
            stroke="currentColor"
            strokeWidth="3"
            strokeLinecap="round"
            strokeLinejoin="round"
            opacity="0.9"
          />
        </svg>
      </aside>

      {/* Form panel */}
      <div className="flex flex-1 items-center justify-center p-6">
      <Card className="w-full max-w-md p-0 overflow-hidden rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))]">
        <div className="px-5 py-6">
          {mounted && (
            <div className="flex items-center justify-end gap-2">
              {resolved === 'dark' && (
                <div className="inline-flex items-center gap-1">
                  <button
                    type="button"
                    aria-label="Bluish dark theme"
                    onClick={() => setDarkVariant('bluish')}
                    className={`h-6 w-8 rounded-md border ${darkVariant==='bluish' ? 'ring-1 ring-[hsl(var(--ring))]' : 'border-[hsl(var(--border))]'}`}
                    style={{ background: 'linear-gradient(135deg, #0E2F3F 0%, #143245 100%)' }}
                    title="Bluish"
                  />
                  <button
                    type="button"
                    aria-label="Blackish dark theme"
                    onClick={() => setDarkVariant('blackish')}
                    className={`h-6 w-8 rounded-md border ${darkVariant==='blackish' ? 'ring-1 ring-[hsl(var(--ring))]' : 'border-[hsl(var(--border))]'}`}
                    style={{ background: 'linear-gradient(135deg, #0b0f15 0%, #111827 100%)' }}
                    title="Blackish"
                  />
                </div>
              )}
              <ThemeToggle />
            </div>
          )}
          <div className="flex flex-col items-center gap-2">
            <img src="/logo.svg" alt="Bayan" className="h-10 w-auto block dark:hidden" />
            <img src="/logo-dark.svg" alt="Bayan" className="h-10 w-auto hidden dark:block" />
            <Title className="text-gray-900 dark:text-white">{t('title')}</Title>
            <Text className="mt-0 text-gray-600 dark:text-gray-300">{t('subtitle')}</Text>
          </div>
          <form onSubmit={onSubmit} className="mt-5 space-y-3">
            <Input type="email" label={t('email')} aria-label={t('email')} value={email} onChange={(e) => setEmail(e.target.value)} required />
            <Input type="password" label={t('password')} aria-label={t('password')} value={password} onChange={(e) => setPassword(e.target.value)} required />
            <div className="flex items-center justify-between">
              <label className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
                <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
                <span>{t('rememberMe')}</span>
              </label>
              <button type="button" className="text-sm text-blue-600 dark:text-blue-400 hover:underline" onClick={() => setResetOpen(true)}>{t('forgotPassword')}</button>
            </div>
            {error && <div className="text-sm text-red-600">{error}</div>}
            <Button type="submit" variant="primary" loading={loading} className="w-full">
              {loading ? t('submitting') : t('submit')}
            </Button>
          </form>
          <div className="mt-4 text-center text-sm text-gray-600 dark:text-gray-300">
            {t('noAccount')}{' '}
            <button type="button" className="text-blue-600 dark:text-blue-400 hover:underline" onClick={() => setSignupOpen(true)}>{t('createOne')}</button>
          </div>
        </div>

        {/* Signup Dialog */}
        <Dialog.Root open={signupOpen} onOpenChange={setSignupOpen}>
          <Dialog.Portal>
            <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/40" />
            <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[440px] -translate-x-1/2 -translate-y-1/2 rounded-xl border border-[hsl(var(--border))] bg-card p-4 shadow-modal">
              <Dialog.Title className="text-lg font-semibold">Create account</Dialog.Title>
              <div className="mt-3 space-y-3">
                <label className="text-sm block">Full Name
                  <input className="mt-1 w-full px-2 py-1.5 rounded-md border border-[hsl(var(--border))] bg-background" value={suName} onChange={(e) => setSuName(e.target.value)} />
                </label>
                <label className="text-sm block">Email
                  <input type="email" className="mt-1 w-full px-2 py-1.5 rounded-md border border-[hsl(var(--border))] bg-background" value={suEmail} onChange={(e) => setSuEmail(e.target.value)} />
                </label>
                <label className="text-sm block">Password
                  <input type="password" className="mt-1 w-full px-2 py-1.5 rounded-md border border-[hsl(var(--border))] bg-background" value={suPassword} onChange={(e) => setSuPassword(e.target.value)} />
                </label>
                {/* Role selection removed; all signups create 'user' role */}
                {suError && <div className="text-sm text-red-600">{suError}</div>}
                <div className="flex items-center justify-end gap-2">
                  <Dialog.Close asChild>
                    <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Cancel</button>
                  </Dialog.Close>
                  <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" disabled={suBusy || !suEmail || !suPassword} onClick={async () => {
                    setSuBusy(true); setSuError(null);
                    try {
                      await Api.signup({ name: suName || suEmail.split('@')[0], email: suEmail, password: suPassword })
                      // Auto login after signup
                      await login(suEmail, suPassword, true)
                      setSignupOpen(false)
                      router.replace(nextPath as any)
                    } catch (e: any) {
                      setSuError(e?.message || 'Failed to signup')
                    } finally { setSuBusy(false) }
                  }}>{suBusy ? 'Creating…' : 'Create account'}</button>
                </div>
              </div>
            </Dialog.Content>
          </Dialog.Portal>
        </Dialog.Root>

        {/* Reset Password Dialog */}
        <Dialog.Root open={resetOpen} onOpenChange={setResetOpen}>
          <Dialog.Portal>
            <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/40" />
            <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[440px] -translate-x-1/2 -translate-y-1/2 rounded-xl border border-[hsl(var(--border))] bg-card p-4 shadow-modal">
              <Dialog.Title className="text-lg font-semibold">Reset password</Dialog.Title>
              <div className="mt-3 space-y-3">
                <p className="text-sm text-muted-foreground">Enter your email address and we'll send you a link to reset your password.</p>
                <label className="text-sm block">Email
                  <input type="email" className="mt-1 w-full px-2 py-1.5 rounded-md border border-[hsl(var(--border))] bg-background" value={rpEmail} onChange={(e) => setRpEmail(e.target.value)} />
                </label>
                {rpMsg && <div className="text-sm text-emerald-600">{rpMsg}</div>}
                <div className="flex items-center justify-end gap-2">
                  <Dialog.Close asChild>
                    <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Close</button>
                  </Dialog.Close>
                  <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" disabled={rpBusy || !rpEmail} onClick={async () => {
                    setRpBusy(true); setRpMsg(null);
                    try {
                      await Api.requestPasswordReset(rpEmail)
                      setRpMsg('If an account exists with that email, a reset link has been sent. Check your inbox.')
                    } catch (e: any) {
                      setRpMsg(e?.message || 'Failed to send reset email')
                    } finally { setRpBusy(false) }
                  }}>{rpBusy ? 'Sending…' : 'Send reset link'}</button>
                </div>
              </div>
            </Dialog.Content>
          </Dialog.Portal>
        </Dialog.Root>
        {/* Footer */}
        <div className="px-5 py-3 border-t border-[hsl(var(--border))] bg-[hsl(var(--card))]">
          <div className="mx-auto flex items-center justify-center gap-2">
            <img src="/bayan-logo.svg" alt="Bayan" className="h-7 w-auto opacity-80 dark:opacity-90 self-center" />
            <div className="text-[11px] text-muted-foreground leading-tight">
              <div>All rights reserved to Bayan © {new Date().getFullYear()}</div>
              <div className="mt-0.5">Frontend {frontendVer || '-'} · Backend {backendVer || '-'}</div>
            </div>
          </div>
        </div>
      </Card>
      </div>
    </div>
  )
}
