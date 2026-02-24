"use client"

import { useEffect, useState } from 'react'
import type { DatasourceOut } from '@/lib/api'
import type { RemoteAttachment } from '@/lib/dsl'

type Props = {
  datasources: DatasourceOut[]
  onAddAction: (att: RemoteAttachment) => void
  onCancelAction: () => void
  initial?: RemoteAttachment
}

function slugify(s: string) {
  return s.toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/_+/g, '_').replace(/^_|_$/g, '')
}

export default function RemoteAttachmentBuilder({ datasources, onAddAction, onCancelAction, initial }: Props) {
  const remoteDatasources = datasources.filter(d => !/duckdb/i.test(d.type || ''))

  const [alias, setAlias]     = useState(initial?.alias ?? '')
  const [dsId, setDsId]       = useState(initial?.datasourceId ?? (remoteDatasources[0]?.id ?? ''))
  const [database, setDatabase] = useState(initial?.database ?? '')
  const [aliasManual, setAliasManual] = useState(!!initial?.alias)

  // Auto-suggest alias from datasource name
  useEffect(() => {
    if (aliasManual) return
    const ds = remoteDatasources.find(d => d.id === dsId)
    if (ds) setAlias(slugify(ds.name).slice(0, 32) || 'remote')
  }, [dsId, aliasManual])

  const canSave = alias.trim() && dsId

  function handleSave() {
    if (!canSave) return
    onAddAction({
      id: initial?.id ?? crypto.randomUUID(),
      alias: alias.trim(),
      datasourceId: dsId,
      database: database.trim() || undefined,
    })
  }

  return (
    <div className="rounded-md border p-3 bg-[hsl(var(--secondary)/0.6)] text-[12px] space-y-3">
      <div className="grid grid-cols-3 gap-2 items-center">
        <label className="text-xs text-muted-foreground">Remote datasource</label>
        <select
          className="col-span-2 h-8 px-2 rounded-md bg-card text-xs border border-[hsl(var(--border))]"
          value={dsId}
          onChange={e => setDsId(e.target.value)}
        >
          {remoteDatasources.length === 0 && <option value="">(no MySQL / PostgreSQL datasources)</option>}
          {remoteDatasources.map(d => <option key={d.id} value={d.id}>{d.name} [{d.type}]</option>)}
        </select>
      </div>

      <div className="grid grid-cols-3 gap-2 items-center">
        <label className="text-xs text-muted-foreground">Alias</label>
        <input
          className="col-span-2 h-8 px-2 rounded-md bg-card text-xs border border-[hsl(var(--border))] font-mono"
          placeholder="e.g. remote_mt5"
          value={alias}
          onChange={e => { setAlias(e.target.value); setAliasManual(true) }}
        />
      </div>
      <p className="text-[10px] text-muted-foreground -mt-1">
        Use this alias in join target tables: <code className="font-mono bg-[hsl(var(--muted))] px-1 rounded">alias.database.table</code>
      </p>

      <div className="grid grid-cols-3 gap-2 items-center">
        <label className="text-xs text-muted-foreground">Database override</label>
        <input
          className="col-span-2 h-8 px-2 rounded-md bg-card text-xs border border-[hsl(var(--border))] font-mono"
          placeholder="(use datasource default)"
          value={database}
          onChange={e => setDatabase(e.target.value)}
        />
      </div>

      <div className="flex justify-end gap-2 pt-1">
        <button
          type="button"
          onClick={onCancelAction}
          className="text-xs px-3 py-1.5 rounded-md border border-[hsl(var(--border))] hover:bg-[hsl(var(--muted))] transition-colors"
        >
          Cancel
        </button>
        <button
          type="button"
          disabled={!canSave}
          onClick={handleSave}
          className="text-xs px-3 py-1.5 rounded-md bg-[#1E40AF] text-white hover:bg-[#1d3a9e] disabled:opacity-50 transition-colors font-medium"
        >
          {initial ? 'Update' : 'Add connection'}
        </button>
      </div>
    </div>
  )
}
