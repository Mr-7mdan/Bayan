"use client"

import { Card, Title, Text } from '@tremor/react'
import { useAuth } from '@/components/providers/AuthProvider'
import { useRouter } from 'next/navigation'
import { useEffect, useState } from 'react'
import * as Dialog from '@radix-ui/react-dialog'
import { Api } from '@/lib/api'

export default function LoginPage() {
  const { login } = useAuth()
  const router = useRouter()
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [remember, setRemember] = useState(true)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
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
  const [rpNew, setRpNew] = useState('')
  const [rpBusy, setRpBusy] = useState(false)
  const [rpMsg, setRpMsg] = useState<string | null>(null)

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
      router.replace('/home')
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to login')
    } finally { setLoading(false) }
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-6 bg-[hsl(var(--background))]">
      <Card className="w-full max-w-md p-0 overflow-hidden rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))]">
        <div className="px-5 py-6">
          <div className="flex flex-col items-center gap-2">
            <img src="/logo.svg" alt="Bayan" className="h-10 w-auto block dark:hidden" />
            <img src="/logo-dark.svg" alt="Bayan" className="h-10 w-auto hidden dark:block" />
            <Title className="text-gray-900 dark:text-white">Sign in</Title>
            <Text className="mt-0 text-gray-600 dark:text-gray-300">Use your account to continue.</Text>
          </div>
          <form onSubmit={onSubmit} className="mt-5 space-y-3">
            <div>
              <Text className="text-sm text-gray-600 dark:text-gray-300">Email</Text>
              <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required className="mt-1 w-full px-2 py-1.5 rounded-md border border-[hsl(var(--border))] bg-card" />
            </div>
            <div>
              <Text className="text-sm text-gray-600 dark:text-gray-300">Password</Text>
              <input type="password" value={password} onChange={(e) => setPassword(e.target.value)} required className="mt-1 w-full px-2 py-1.5 rounded-md border border-[hsl(var(--border))] bg-card" />
            </div>
            <div className="flex items-center justify-between">
              <label className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
                <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} />
                <span>Remember me</span>
              </label>
              <button type="button" className="text-sm text-blue-600 dark:text-blue-400 hover:underline" onClick={() => setResetOpen(true)}>Forgot password?</button>
            </div>
            {error && <div className="text-sm text-red-600">{error}</div>}
            <button
              type="submit"
              disabled={loading}
              className="w-full text-sm px-3 py-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--card))] hover:bg-muted disabled:opacity-60 transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-400 focus-visible:ring-offset-2 focus-visible:ring-offset-white dark:focus-visible:ring-offset-gray-950"
            >
              {loading ? 'Continuing…' : 'Continue'}
            </button>
          </form>
          <div className="mt-4 text-center text-sm text-gray-600 dark:text-gray-300">
            Don't have an account?{' '}
            <button type="button" className="text-blue-600 dark:text-blue-400 hover:underline" onClick={() => setSignupOpen(true)}>Create one</button>
          </div>
        </div>

        {/* Signup Dialog */}
        <Dialog.Root open={signupOpen} onOpenChange={setSignupOpen}>
          <Dialog.Portal>
            <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/40" />
            <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[440px] -translate-x-1/2 -translate-y-1/2 rounded-xl border border-[hsl(var(--border))] bg-card p-4 shadow-card">
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
                      router.replace('/home')
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
            <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[440px] -translate-x-1/2 -translate-y-1/2 rounded-xl border border-[hsl(var(--border))] bg-card p-4 shadow-card">
              <Dialog.Title className="text-lg font-semibold">Reset password</Dialog.Title>
              <div className="mt-3 space-y-3">
                <label className="text-sm block">Email
                  <input type="email" className="mt-1 w-full px-2 py-1.5 rounded-md border border-[hsl(var(--border))] bg-background" value={rpEmail} onChange={(e) => setRpEmail(e.target.value)} />
                </label>
                <label className="text-sm block">New Password
                  <input type="password" className="mt-1 w-full px-2 py-1.5 rounded-md border border-[hsl(var(--border))] bg-background" value={rpNew} onChange={(e) => setRpNew(e.target.value)} />
                </label>
                {rpMsg && <div className="text-sm text-emerald-600">{rpMsg}</div>}
                <div className="flex items-center justify-end gap-2">
                  <Dialog.Close asChild>
                    <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted">Close</button>
                  </Dialog.Close>
                  <button type="button" className="text-sm px-3 py-1.5 rounded-md border hover:bg-muted" disabled={rpBusy || !rpEmail || !rpNew} onClick={async () => {
                    setRpBusy(true); setRpMsg(null);
                    try {
                      await Api.resetPassword({ email: rpEmail, newPassword: rpNew })
                      setRpMsg('Password has been reset. You can now sign in with your new password.')
                    } catch (e: any) {
                      setRpMsg(e?.message || 'Failed to reset password')
                    } finally { setRpBusy(false) }
                  }}>{rpBusy ? 'Resetting…' : 'Reset password'}</button>
                </div>
              </div>
            </Dialog.Content>
          </Dialog.Portal>
        </Dialog.Root>
      </Card>
    </div>
  )
}
