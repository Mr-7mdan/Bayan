"use client"

import { useSearchParams, useRouter } from 'next/navigation'
import { useState, Suspense } from 'react'
import { Api } from '@/lib/api'

function ResetPasswordForm() {
  const searchParams = useSearchParams()
  const router = useRouter()
  const token = searchParams.get('token') || ''

  const [password, setPassword] = useState('')
  const [confirm, setConfirm] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState(false)

  const valid = password.length >= 6 && password === confirm && !!token

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    if (!valid) return
    setBusy(true)
    setError(null)
    try {
      await Api.confirmPasswordReset(token, password)
      setSuccess(true)
    } catch (err: any) {
      setError(err?.message || 'Failed to reset password. The link may have expired.')
    } finally {
      setBusy(false)
    }
  }

  if (!token) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <div className="w-[420px] rounded-xl border border-[hsl(var(--border))] bg-card p-6 shadow-card text-center">
          <h1 className="text-xl font-semibold mb-2">Invalid Reset Link</h1>
          <p className="text-sm text-muted-foreground mb-4">This password reset link is missing or invalid.</p>
          <a href="/login" className="text-sm text-blue-600 hover:underline">Back to login</a>
        </div>
      </div>
    )
  }

  if (success) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <div className="w-[420px] rounded-xl border border-[hsl(var(--border))] bg-card p-6 shadow-card text-center">
          <div className="text-3xl mb-3">&#10003;</div>
          <h1 className="text-xl font-semibold mb-2">Password Reset</h1>
          <p className="text-sm text-muted-foreground mb-4">Your password has been updated successfully.</p>
          <button
            onClick={() => router.push('/login')}
            className="px-4 py-2 rounded-md bg-blue-600 text-white text-sm font-medium hover:bg-blue-700"
          >
            Sign in
          </button>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-background">
      <div className="w-[420px] rounded-xl border border-[hsl(var(--border))] bg-card p-6 shadow-card">
        <h1 className="text-xl font-semibold mb-1">Set New Password</h1>
        <p className="text-sm text-muted-foreground mb-4">Enter your new password below.</p>
        <form onSubmit={handleSubmit} className="space-y-3">
          <label className="text-sm block">
            New Password
            <input
              type="password"
              className="mt-1 w-full px-3 py-2 rounded-md border border-[hsl(var(--border))] bg-background"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              minLength={6}
              required
              autoFocus
            />
          </label>
          <label className="text-sm block">
            Confirm Password
            <input
              type="password"
              className="mt-1 w-full px-3 py-2 rounded-md border border-[hsl(var(--border))] bg-background"
              value={confirm}
              onChange={(e) => setConfirm(e.target.value)}
              minLength={6}
              required
            />
          </label>
          {password && confirm && password !== confirm && (
            <p className="text-sm text-red-500">Passwords do not match</p>
          )}
          {error && <p className="text-sm text-red-500">{error}</p>}
          <button
            type="submit"
            disabled={!valid || busy}
            className="w-full py-2 rounded-md bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
          >
            {busy ? 'Resetting...' : 'Reset Password'}
          </button>
        </form>
        <div className="mt-4 text-center">
          <a href="/login" className="text-sm text-muted-foreground hover:underline">Back to login</a>
        </div>
      </div>
    </div>
  )
}

export default function ResetPasswordPage() {
  return (
    <Suspense fallback={<div className="min-h-screen flex items-center justify-center">Loading...</div>}>
      <ResetPasswordForm />
    </Suspense>
  )
}
