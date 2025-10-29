"use client"

import * as Dialog from "@radix-ui/react-dialog"
import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { Api, type DashboardSaveRequest } from "@/lib/api"
import { useAuth } from "@/components/providers/AuthProvider"

export default function CreateDashboardDialog() {
  const router = useRouter()
  const { user } = useAuth()
  const [open, setOpen] = useState(false)
  const [title, setTitle] = useState("")
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Open on global event
  useEffect(() => {
    function onOpen() {
      setTitle("")
      setError(null)
      setOpen(true)
    }
    window.addEventListener("open-create-dashboard", onOpen as any)
    return () => window.removeEventListener("open-create-dashboard", onOpen as any)
  }, [])

  async function handleCreate() {
    const name = title.trim()
    if (!name) {
      setError("Please enter a title")
      return
    }
    setBusy(true)
    setError(null)
    try {
      const payload: DashboardSaveRequest = {
        name,
        userId: user?.id || "dev_user",
        definition: { layout: [], widgets: {} },
      }
      const res = await Api.saveDashboard(payload)
      try { localStorage.setItem("dashboardId", res.id) } catch {}
      setOpen(false)
      router.push("/builder")
    } catch (e: any) {
      setError(e?.message || "Failed to create dashboard")
    } finally {
      setBusy(false)
    }
  }

  return (
    <Dialog.Root open={open} onOpenChange={setOpen}>
      <Dialog.Portal>
        <Dialog.Overlay className="fixed inset-0 z-[60] bg-black/30" />
        <Dialog.Content className="fixed left-1/2 top-1/2 z-[70] w-[520px] -translate-x-1/2 -translate-y-1/2 rounded-lg border border-[hsl(var(--border))] bg-[hsl(var(--card))] text-[hsl(var(--foreground))] p-4 shadow-card">
          <Dialog.Title className="text-lg font-semibold">Create new dashboard</Dialog.Title>
          <Dialog.Description className="text-sm text-gray-600 dark:text-gray-300 mt-1">
            Enter a title for your dashboard.
          </Dialog.Description>
          <div className="mt-4 space-y-2">
            <label className="text-sm block">
              Title
              <input
                className="mt-1 w-full px-3 py-2 rounded-md border border-[hsl(var(--border))] bg-[hsl(var(--background))]"
                placeholder="e.g., Sales Overview"
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter') handleCreate() }}
                autoFocus
              />
            </label>
            {error && <div className="text-sm text-red-600">{error}</div>}
          </div>
          <div className="mt-4 flex items-center justify-end gap-2">
            <Dialog.Close asChild>
              <button type="button" className="text-sm px-3 py-1.5 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))]">Cancel</button>
            </Dialog.Close>
            <button
              type="button"
              disabled={busy || !title.trim()}
              onClick={handleCreate}
              className="text-sm px-3 py-1.5 rounded-md bg-blue-600 text-white disabled:opacity-50"
            >
              {busy ? 'Creating…' : 'Create'}
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  )
}
