"use client"

import { createPortal } from 'react-dom'
import { useEffect, useMemo, useRef, useState } from 'react'
import { Api } from '@/lib/api'
import { JsonEditor, githubDarkTheme, githubLightTheme } from 'json-edit-react'
import AdvancedSqlCaseBuilder from '@/components/builder/AdvancedSqlCaseBuilder'
import AdvancedSqlReplaceBuilder from '@/components/builder/AdvancedSqlReplaceBuilder'
import AdvancedSqlComputedBuilder from '@/components/builder/AdvancedSqlComputedBuilder'
import AdvancedSqlUnpivotBuilder from '@/components/builder/AdvancedSqlUnpivotBuilder'
import AdvancedSqlNullBuilder from '@/components/builder/AdvancedSqlNullBuilder'
import AdvancedSqlJoinBuilder from '@/components/builder/AdvancedSqlJoinBuilder'
import AdvancedSqlBulkCasesBuilder from './AdvancedSqlBulkCasesBuilder'
import AdvancedSqlCustomColumnBuilder from '@/components/builder/AdvancedSqlCustomColumnBuilder'
import type { IntrospectResponse } from '@/lib/api'
import type { DatasourceTransforms } from '@/lib/dsl'

export type AdvancedSqlDialogProps = {
  open: boolean
  onCloseAction: () => void
  datasourceId?: string
  dsType?: string | null
  schema?: IntrospectResponse
  source?: string
  select?: string[]
  widgetId?: string
}

export default function AdvancedSqlDialog({ open, onCloseAction, datasourceId, dsType, schema, source, select, widgetId }: AdvancedSqlDialogProps) {
  const [tab, setTab] = useState<'custom'|'transform'|'joins'|'sort'|'preview'>('custom')
  const [loading, setLoading] = useState(false)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string|undefined>(undefined)
  const [editJson, setEditJson] = useState<string>('')
  const [baselineJson, setBaselineJson] = useState<string>('')
  const [jsonError, setJsonError] = useState<string|undefined>(undefined)
  const [model, setModel] = useState<DatasourceTransforms>({ customColumns: [], transforms: [], joins: [] })
  const [showJsonPanel, setShowJsonPanel] = useState<boolean>(false)
  const dialogRef = useRef<HTMLDivElement>(null)
  const [jsonPanelText, setJsonPanelText] = useState<string>('')
  const [jsonPanelError, setJsonPanelError] = useState<string|undefined>(undefined)
  const [jsonPanelPos, setJsonPanelPos] = useState<{ top: number; left: number; height: number }>({ top: 0, left: 8, height: 0 })
  const JSON_PANEL_WIDTH = 320
  const [pvSql, setPvSql] = useState<string>('')
  const [pvCols, setPvCols] = useState<string[]>([])
  const [pvRows, setPvRows] = useState<any[][]>([])
  const [pvWarn, setPvWarn] = useState<string[]>([])
  const [pvLoading, setPvLoading] = useState(false)
  const [schemaLocal, setSchemaLocal] = useState<IntrospectResponse | undefined>(schema)
  const [showCaseBuilder, setShowCaseBuilder] = useState(false)
  const [showReplaceBuilder, setShowReplaceBuilder] = useState(false)
  const [showComputedBuilder, setShowComputedBuilder] = useState(false)
  const [showNullBuilder, setShowNullBuilder] = useState(false)
  const [showJoinBuilder, setShowJoinBuilder] = useState(false)
  const [showBulkCasesBuilder, setShowBulkCasesBuilder] = useState(false)
  const [showCustomBuilder, setShowCustomBuilder] = useState(false)
  const [showUnpivotBuilder, setShowUnpivotBuilder] = useState(false)
  const [insertAsNumber, setInsertAsNumber] = useState<boolean>(false)
  const [colFilter, setColFilter] = useState<string>('')
  const insertSinkRef = useRef<((txt: string) => void) | null>(null)
  // Fallback datasource selection when missing
  const [dsIdLocal, setDsIdLocal] = useState<string | undefined>(undefined)
  const [dsList, setDsList] = useState<Array<{ id: string; name: string; type?: string }>>([])
  const effectiveDsId = useMemo(() => {
    const fromProp = (datasourceId || '').trim()
    return fromProp ? fromProp : (dsIdLocal || undefined)
  }, [datasourceId, dsIdLocal])
  // Editing states for prefilled builders
  const [editingCustomIndex, setEditingCustomIndex] = useState<number | null>(null)
  const [initialCustom, setInitialCustom] = useState<any | undefined>(undefined)
  const [editingTransformIndex, setEditingTransformIndex] = useState<number | null>(null)
  const [initialTransform, setInitialTransform] = useState<any | undefined>(undefined)
  const [editingJoinIndex, setEditingJoinIndex] = useState<number | null>(null)
  const [initialJoin, setInitialJoin] = useState<any | undefined>(undefined)
  const [sortBy, setSortBy] = useState<string>('')
  const [sortDir, setSortDir] = useState<'asc'|'desc'>('desc')
  const [limitN, setLimitN] = useState<string>('')
  // Examples panel (right-side) visibility; default collapsed
  const [examplesOpen, setExamplesOpen] = useState<boolean>(false)
  const [toast, setToast] = useState<{ message: string; type: 'success' | 'error' } | null>(null)
  const mainColsClass = examplesOpen ? 'lg:grid-cols-[1fr,320px]' : 'lg:grid-cols-[1fr,auto]'

  const colsAvailable = useMemo(() => {
    // Prefer select provided by caller
    if (Array.isArray(select) && select.length) return select
    const baseCols: string[] = []
    try {
      const src = String(source || '')
      const sc = (schemaLocal || schema)
      if (src && sc) {
        const parts = src.split('.')
        const tbl = parts.pop() as string
        const sch = parts.join('.') || 'main'  // Default to 'main' schema if no schema prefix
        const schNode = (sc.schemas || []).find(s => s.name === sch)
        const tblNode = schNode?.tables.find(t => t.name === tbl)
        baseCols.push(...(tblNode?.columns || []).map(c => c.name))
      }
    } catch {}
    
    // Add custom columns and transforms from datasource/table scope
    const customAliases: string[] = []
    try {
      const parsed = JSON.parse(editJson || '{}') as any
      const src = String(source || '')
      const norm = (s: string) => String(s || '').trim().replace(/^\[|\]|^"|"$/g, '')
      const tblEq = (a: string, b: string) => {
        const na = norm(a).split('.').pop() || ''
        const nb = norm(b).split('.').pop() || ''
        return na.toLowerCase() === nb.toLowerCase()
      }
      
      // From customColumns
      const ccList = Array.isArray(parsed?.customColumns) ? parsed.customColumns : []
      for (const cc of ccList) {
        const sc = cc?.scope || {}
        const lvl = String(sc?.level || 'datasource').toLowerCase()
        const match = (lvl === 'datasource' || (lvl === 'table' && sc?.table && src && tblEq(String(sc.table), src)))
        if (match && cc?.name) customAliases.push(String(cc.name))
      }
      
      // From transforms
      const trList = Array.isArray(parsed?.transforms) ? parsed.transforms : []
      for (const tr of trList) {
        const sc = tr?.scope || {}
        const lvl = String(sc?.level || 'datasource').toLowerCase()
        const match = (lvl === 'datasource' || (lvl === 'table' && sc?.table && src && tblEq(String(sc.table), src)))
        if (!match) continue
        const ty = String(tr?.type || '').toLowerCase()
        if (ty === 'computed' && tr?.name) customAliases.push(String(tr.name))
        else if ((ty === 'case' || ty === 'replace' || ty === 'translate' || ty === 'nullhandling') && tr?.target) customAliases.push(String(tr.target))
      }
    } catch {}
    
    return [...baseCols, ...customAliases]
  }, [schema, schemaLocal, source, JSON.stringify(select || []), editJson])

  const colsFiltered = useMemo(() => {
    const f = String(colFilter || '').toLowerCase()
    if (!f) return colsAvailable
    return colsAvailable.filter(n => n.toLowerCase().includes(f))
  }, [colsAvailable, colFilter])

  function qIdent(name: string): string {
    const nm = String(name || '')
    const d = String(dsType || '').toLowerCase()
    if (d.includes('mssql') || d.includes('sqlserver')) return `[${nm.replace(/\]/g, ']]')}]`
    if (d.includes('mysql') || d.includes('mariadb')) return `\`${nm.replace(/`/g, '``')}\``
    return `"${nm.replace(/"/g, '""')}"`
  }
  function numWrap(ident: string): string {
    const d = String(dsType || '').toLowerCase()
    if (d.includes('duckdb')) return `COALESCE(try_cast(regexp_replace(CAST(${ident} AS VARCHAR), '[^0-9\\.-]', '') AS DOUBLE), try_cast(${ident} AS DOUBLE), 0.0)`
    if (d.includes('postgres')) return `CAST(${ident} AS DOUBLE PRECISION)`
    if (d.includes('mysql') || d.includes('mariadb')) return `CAST(${ident} AS DECIMAL(38,10))`
    if (d.includes('sqlite')) return `CAST(${ident} AS REAL)`
    if (d.includes('mssql') || d.includes('sqlserver')) return `TRY_CAST(${ident} AS DECIMAL(38,10))`
    return `CAST(${ident} AS DOUBLE)`
  }
  function insertText(txt: string) {
    try {
      // Prefer a registered sink (the active Expression input) if available
      if (insertSinkRef.current) {
        insertSinkRef.current(txt)
        return
      }
      const el = document.activeElement as any
      if (!el) return
      const isText = el && (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA')
      if (!isText) return
      const start = el.selectionStart ?? el.value?.length ?? 0
      const end = el.selectionEnd ?? start
      const before = String(el.value || '').slice(0, start)
      const after = String(el.value || '').slice(end)
      const next = `${before}${txt}${after}`
      el.value = next
      el.selectionStart = el.selectionEnd = start + txt.length
      el.dispatchEvent(new Event('input', { bubbles: true }))
    } catch {}
  }
  function insertColumn(name: string) {
    const id = qIdent(name)
    const token = insertAsNumber ? numWrap(id) : id
    insertText(token)
  }

  // (Insert tab removed)

  // Keep schemaLocal synced with incoming prop
  useEffect(() => { if (schema) setSchemaLocal(schema) }, [schema])

  // Fallback: fetch schema if not provided
  useEffect(() => {
    const ac = new AbortController()
    async function run() {
      try {
        if (!open) return
        if (schemaLocal) return
        const sc = await (effectiveDsId ? Api.introspect(effectiveDsId, ac.signal) : Api.introspectLocal(ac.signal))
        if (!ac.signal.aborted) setSchemaLocal(sc as any)
      } catch {}
    }
    run()
    return () => { try { ac.abort() } catch {} }
  }, [open, effectiveDsId, schemaLocal])

  // Load datasources list when needed for fallback selection
  useEffect(() => {
    let stop = false
    ;(async () => {
      try {
        if (!open) return
        if (effectiveDsId) return
        const list = await Api.listDatasources()
        if (!stop) setDsList(Array.isArray(list) ? list.map((d:any)=>({ id: d.id, name: d.name, type: d.type })) : [])
      } catch {}
    })()
    return () => { stop = true }
  }, [open, effectiveDsId])

  // load transforms on open
  useEffect(() => {
    let abort = false
    async function run() {
      if (!open || !datasourceId) return
      setLoading(true); setError(undefined)
      try {
        const cfg = await Api.getDatasourceTransforms(datasourceId)
        if (!abort) {
          setModel(cfg || { customColumns: [], transforms: [], joins: [] })
          const js = JSON.stringify(cfg || { customColumns: [], transforms: [], joins: [] }, null, 2)
          setEditJson(js)
          setBaselineJson(js)
          try {
            const d = (cfg as any)?.defaults || {}
            const s = d?.sort || {}
            if (s?.by) setSortBy(String(s.by))
            if (s?.direction) setSortDir(String(s.direction).toLowerCase() as any)
            const tn = d?.limitTopN
            if (tn?.n != null) setLimitN(String(tn.n))
          } catch {}
        }
      } catch (e: any) {
        if (!abort) setError(String(e?.message || 'Failed to load transforms'))
      } finally {
        if (!abort) setLoading(false)
      }
    }
    run()
    return () => { abort = true }
  }, [open, datasourceId])

  // Keep JSON panel text in sync and compute its position
  useEffect(() => {
    if (showJsonPanel) {
      try { setJsonPanelText(JSON.stringify(JSON.parse(editJson || '{}'), null, 2)); setJsonPanelError(undefined) }
      catch { setJsonPanelText(editJson || '{}'); setJsonPanelError(undefined) }
    }
  }, [showJsonPanel, editJson])

  useEffect(() => {
    function updatePos() {
      try {
        const el = dialogRef.current
        if (!el) return
        const r = el.getBoundingClientRect()
        const left = Math.max(8, r.left - JSON_PANEL_WIDTH - 8)
        const top = Math.max(8, r.top)
        const height = Math.min(window.innerHeight - 16, r.height)
        setJsonPanelPos({ top, left, height })
      } catch {}
    }
    updatePos()
    window.addEventListener('resize', updatePos)
    window.addEventListener('scroll', updatePos, true)
    return () => {
      window.removeEventListener('resize', updatePos)
      window.removeEventListener('scroll', updatePos, true)
    }
  }, [showJsonPanel, open])

  // When editing JSON manually, reflect defaults into Sort & Limit controls
  useEffect(() => {
    try {
      const parsed = JSON.parse(editJson || '{}') as any
      const d = parsed?.defaults || {}
      const s = d?.sort || {}
      setSortBy(String(s?.by || ''))
      setSortDir((String(s?.direction || 'desc').toLowerCase() as any))
      const tn = d?.limitTopN
      setLimitN(tn?.n != null ? String(tn.n) : '')
    } catch {
      // ignore JSON parse errors here; jsonError banner already shows
    }
  }, [editJson])

  // Helpers to open editors from unified grid
  function openEditCustom(i: number, c: any) {
    setTab('custom')
    setEditingCustomIndex(i)
    setInitialCustom(c)
    setShowCustomBuilder(true)
  }
  function openEditTransform(i: number, t: any) {
    setTab('transform')
    setEditingTransformIndex(i)
    setInitialTransform(t)
    const typ = String(t?.type || '').toLowerCase()
    const expr = String((t as any)?.expr || '')
    const looksLikeCase = (typ === 'computed') && /^\s*case\b/i.test(expr)
    setShowCaseBuilder(typ==='case' || looksLikeCase)
    setShowReplaceBuilder(typ==='replace' || typ==='translate')
    setShowComputedBuilder(typ==='computed' && !looksLikeCase)
    setShowNullBuilder(typ==='nullhandling' || typ==='nullHandling')
    setShowUnpivotBuilder(typ==='unpivot')
  }
  function openEditJoin(i: number, j: any) {
    setTab('joins')
    setEditingJoinIndex(i)
    setInitialJoin(j)
    setShowJoinBuilder(true)
  }

  function shorten(s: string, n = 120) {
    const ex = String(s || '').replace(/\s+/g, ' ')
    return ex.length > n ? ex.slice(0, n) + '…' : ex
  }
  function scopeBadge(sc: any): string {
    if (!sc || !sc.level) return ''
    if (sc.level === 'table' && sc.table) return `table:${sc.table}`
    if (sc.level === 'widget' && (sc.widgetId || widgetId)) return `widget:${sc.widgetId || widgetId}`
    return String(sc.level)
  }

  function scopeLevelOf(obj: any): 'datasource'|'table'|'widget' {
    const lvl = String(obj?.scope?.level || '').toLowerCase()
    if (lvl === 'table') return 'table'
    if (lvl === 'widget') return 'widget'
    return 'datasource'
  }
  function setScopeOnItem(kind: 'customColumns'|'transforms'|'joins', idx: number, level: 'datasource'|'table'|'widget') {
    try {
      const parsed = JSON.parse(editJson || '{}') as any
      const next = {
        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
        defaults: parsed?.defaults || undefined,
      } as any
      const arr = next[kind] as any[]
      if (!Array.isArray(arr) || idx < 0 || idx >= arr.length) return
      const draft = { ...(arr[idx] || {}) }
      if (level === 'datasource') draft.scope = { level: 'datasource' }
      else if (level === 'table') {
        if (!source) return // guard
        draft.scope = { level: 'table', table: source }
      } else if (level === 'widget') {
        if (!widgetId) return // guard
        draft.scope = { level: 'widget', widgetId }
      }
      arr[idx] = draft
      setEditJson(JSON.stringify(next, null, 2))
    } catch {}
  }

  // Validate JSON continuously
  useEffect(() => {
    if (!editJson) { setJsonError(undefined); return }
    try { JSON.parse(editJson); setJsonError(undefined) } catch (e: any) { setJsonError(String(e?.message || 'Invalid JSON')) }
  }, [editJson])

  const isDirty = useMemo(() => (baselineJson || '') !== (editJson || ''), [baselineJson, editJson])
  const canSave = useMemo(() => !!effectiveDsId && !!editJson && !jsonError && isDirty, [effectiveDsId, editJson, jsonError, isDirty])

  // Helpers to show and mutate current items without manual JSON editing
  const parsedModel = useMemo(() => {
    try {
      return JSON.parse(editJson || '{}') as Partial<DatasourceTransforms>
    } catch {
      return {} as Partial<DatasourceTransforms>
    }
  }, [editJson])
  const listCC = Array.isArray((parsedModel as any)?.customColumns) ? (parsedModel as any).customColumns as any[] : []
  const listTR = Array.isArray((parsedModel as any)?.transforms) ? (parsedModel as any).transforms as any[] : []
  const listJN = Array.isArray((parsedModel as any)?.joins) ? (parsedModel as any).joins as any[] : []

  const takenNames = useMemo(() => {
    const ccNames = listCC.map((c: any) => String(c?.name || '').trim()).filter(Boolean)
    return [...colsAvailable, ...ccNames]
  }, [colsAvailable, listCC])

  function applyNext(next: any) {
    try { setEditJson(JSON.stringify(next ?? { customColumns: [], transforms: [], joins: [] }, null, 2)) } catch {}
  }
  function removeCustomColumn(idx: number) {
    try {
      const pm: any = JSON.parse(editJson || '{}')
      pm.customColumns = Array.isArray(pm.customColumns) ? pm.customColumns : []
      pm.customColumns.splice(idx, 1)
      applyNext(pm)
    } catch {}
  }
  function removeTransform(idx: number) {
    try {
      const pm: any = JSON.parse(editJson || '{}')
      pm.transforms = Array.isArray(pm.transforms) ? pm.transforms : []
      pm.transforms.splice(idx, 1)
      applyNext(pm)
    } catch {}
  }
  function removeJoin(idx: number) {
    try {
      const pm: any = JSON.parse(editJson || '{}')
      pm.joins = Array.isArray(pm.joins) ? pm.joins : []
      pm.joins.splice(idx, 1)
      applyNext(pm)
    } catch {}
  }

  // (Insert tab removed)

  async function runPreview() {
    if (!effectiveDsId) return
    if (jsonError) return
    const parsed = JSON.parse(editJson || '{}') as DatasourceTransforms
    setPvLoading(true); setError(undefined)
    try {
      const payload: any = {
        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
        defaults: parsed?.defaults || undefined,
        source: source || '',
        select: Array.isArray(select) && select.length ? select : ['*'],
        limit: 50,
        context: widgetId ? { widgetId } : undefined,
      }
      const res = await Api.previewDatasourceTransforms(effectiveDsId, payload)
      setPvSql(String(res?.sql || ''))
      setPvCols((res?.columns as any) || [])
      setPvRows((res?.rows as any) || [])
      setPvWarn((res?.warnings as any) || [])
    } catch (e: any) {
      setError(String(e?.message || 'Preview failed'))
    } finally {
      setPvLoading(false)
    }
  }

  // Close on Escape
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onCloseAction() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, onCloseAction])

  const TabButton = ({ id, label }: { id: typeof tab; label: string }) => (
    <button
      className={`text-xs px-2 py-1 rounded-md border ${tab===id? 'bg-[hsl(var(--secondary))]' : 'bg-card hover:bg-[hsl(var(--secondary)/0.6)]'}`}
      onClick={() => setTab(id)}
    >{label}</button>
  )

  // Counts for labels and defaults indicator
  const tabCounts = useMemo(() => {
    try {
      const parsed = JSON.parse(editJson || '{}') as any
      const cc = Array.isArray(parsed?.customColumns) ? parsed.customColumns.length : 0
      const tr = Array.isArray(parsed?.transforms) ? parsed.transforms.length : 0
      const jn = Array.isArray(parsed?.joins) ? parsed.joins.length : 0
      const hasDefaults = !!parsed?.defaults && (!!parsed?.defaults?.sort || !!parsed?.defaults?.limitTopN)
      return { cc, tr, jn, hasDefaults }
    } catch { return { cc: 0, tr: 0, jn: 0, hasDefaults: false } }
  }, [editJson])

  // After hooks: guard render when closed or no window
  if (!open || typeof window === 'undefined') return null

  const mainPortal = createPortal(
    <div className="fixed inset-0 z-[1000]">
      <div className="absolute inset-0 bg-black/40" onClick={onCloseAction} />
      {/* External JSON panel positioned to the left of the dialog */}
      {showJsonPanel && (
        <div
          className="fixed z-[1001] rounded-md border bg-card p-2 shadow text-[11px] flex flex-col"
          style={{ top: jsonPanelPos.top, left: jsonPanelPos.left, height: jsonPanelPos.height, width: JSON_PANEL_WIDTH }}
        >
          <div className="flex items-center justify-between mb-2">
            <div className="font-medium">Config JSON</div>
            <div className="flex items-center gap-2">
              <button className="px-2 py-0.5 rounded-md border bg-[hsl(var(--secondary)/0.6)]" onClick={()=>{ try { navigator.clipboard?.writeText(editJson || '') } catch {} }}>Copy</button>
              <button className="px-2 py-0.5 rounded-md border bg-card" title="Hide" onClick={()=>setShowJsonPanel(false)}>✕</button>
            </div>
          </div>
          {jsonPanelError && <div className="text-[11px] text-red-600 mb-1">{jsonPanelError}</div>}
          <div className="flex-1 min-h-0 overflow-hidden">
            <JsonEditor
              data={( () => { try { return JSON.parse(editJson || '{}') } catch { return {} } })()}
              setData={(data: any) => { try { setEditJson(JSON.stringify(data, null, 2)); setJsonPanelError(undefined) } catch (e:any) { setJsonPanelError(String(e?.message || 'Serialize error')) } }}
              theme={(typeof document !== 'undefined' && document.documentElement.classList.contains('dark')) ? githubDarkTheme : githubLightTheme}
              collapse={2}
              showCollectionCount
              minWidth={JSON_PANEL_WIDTH - 8}
              maxWidth={JSON_PANEL_WIDTH - 8}
              rootFontSize={12}
              onError={(msg: any) => setJsonPanelError(String(msg || ''))}
            />
          </div>
          <div className="mt-2 flex items-center justify-end gap-2">
            <button className={`px-2 py-0.5 rounded-md border ${saving? 'opacity-60 cursor-not-allowed' : 'bg-[hsl(var(--btn3))] text-black'}`} disabled={!!jsonPanelError || saving} onClick={async () => {
              if (!effectiveDsId) return
              try {
                const parsed = JSON.parse(editJson || '{}') as DatasourceTransforms
                const payload: DatasourceTransforms = {
                  customColumns: Array.isArray((parsed as any)?.customColumns) ? (parsed as any).customColumns : [],
                  transforms: Array.isArray((parsed as any)?.transforms) ? (parsed as any).transforms : [],
                  joins: Array.isArray((parsed as any)?.joins) ? (parsed as any).joins : [],
                  ...((parsed as any)?.defaults ? { defaults: (parsed as any).defaults } : {}),
                }
                setJsonPanelError(undefined)
                setEditJson(JSON.stringify(payload, null, 2))
                // Save immediately
                setSaving(true); setError(undefined)
                await Api.saveDatasourceTransforms(effectiveDsId, payload)
                setModel(payload)
                setBaselineJson(JSON.stringify(payload, null, 2))
                try {
                  if (typeof window !== 'undefined') {
                    window.dispatchEvent(new CustomEvent('datasource-transforms-saved', { detail: { datasourceId: effectiveDsId } }))
                    // Request other widgets/panels to refresh columns/rows/samples
                    if (widgetId) {
                      window.dispatchEvent(new CustomEvent('request-table-columns', { detail: { widgetId } }))
                      window.dispatchEvent(new CustomEvent('request-table-rows', { detail: { widgetId } }))
                      window.dispatchEvent(new CustomEvent('request-table-samples', { detail: { widgetId } }))
                    }
                  }
                } catch {}
              } catch (e:any) {
                setJsonPanelError(String(e?.message || 'Invalid JSON'))
              } finally {
                setSaving(false)
              }
            }}>Save</button>
          </div>
        </div>
      )}
      <div ref={dialogRef} className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[880px] max-w-[95vw] max-h-[90vh] overflow-auto rounded-xl border border-[hsl(var(--border))] bg-[hsl(var(--background))] p-4">
        <div className="flex items-center justify-between mb-3">
          <div>
            <div className="text-sm font-medium">SQL Advanced Mode</div>
            <div className="text-[11px] text-muted-foreground">Datasource: <span className="font-mono">{effectiveDsId || 'none'}</span> · Type: {dsType || 'unknown'}</div>
          </div>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCloseAction}>✕</button>
        </div>

        {!effectiveDsId && (
          <div className="mb-3 rounded-md border p-2 bg-[hsl(var(--secondary))]">
            <div className="text-[11px] mb-1">Select a datasource to enable Advanced SQL</div>
            <select
              className="text-xs px-2 py-1.5 rounded-md border bg-background"
              value={dsIdLocal || ''}
              onChange={(e)=> setDsIdLocal(e.target.value || undefined)}
            >
              <option value="">— Select datasource —</option>
              {dsList.map((d) => (
                <option key={d.id} value={d.id}>{d.name}{d.type ? ` (${d.type})` : ''}</option>
              ))}
            </select>
          </div>
        )}

        <div className="flex items-center gap-2 mb-3 relative">
          {/* Left chevron to toggle JSON viewer */}
          <button
            className="absolute -left-6 top-1/2 -translate-y-1/2 h-6 w-6 rounded-full border bg-card hover:bg-[hsl(var(--secondary)/0.6)] hidden md:inline-flex items-center justify-center"
            title={showJsonPanel ? 'Hide JSON' : 'Show JSON'}
            onClick={() => setShowJsonPanel(v => !v)}
          >{showJsonPanel ? '⟩' : '⟨'}</button>
          <TabButton id="custom" label={`Custom Columns${tabCounts.cc?` (${tabCounts.cc})`:''}`} />
          <TabButton id="transform" label={`Transformations${tabCounts.tr?` (${tabCounts.tr})`:''}`} />
          <TabButton id="joins" label={`Joins${tabCounts.jn?` (${tabCounts.jn})`:''}`} />
          <TabButton id="sort" label={`Sort & Limit${tabCounts.hasDefaults?' •':''}`} />
          <TabButton id="preview" label="Preview" />
        </div>

        <div className="mb-3 rounded-md border p-2 bg-card">
          <div className="flex flex-wrap items-center gap-2 mb-2">
            <div className="flex items-center gap-1">
              <input
                className="h-7 px-2 rounded-md border bg-background text-[11px]"
                placeholder="Filter columns"
                value={colFilter}
                onChange={(e)=>setColFilter(e.target.value)}
              />
            </div>
            <label className="text-[11px] flex items-center gap-1">
              <input type="checkbox" checked={insertAsNumber} onChange={(e)=>setInsertAsNumber(e.target.checked)} />
              Insert as number
            </label>
          </div>
          <div className="flex flex-wrap gap-1 max-h-24 overflow-y-auto">
            {colsFiltered.map((c) => (
              <button key={c} className="text-[11px] px-2 py-0.5 rounded-md border bg-background hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=>insertColumn(c)}>{c}</button>
            ))}
          </div>
          <div className="mt-2 flex flex-wrap items-center gap-1">
            {[' + ', ' - ', ' * ', ' / ', ' ( ', ' ) ', ', '].map(op => (
              <button key={op} className="text-[11px] px-2 py-0.5 rounded-md border bg-background hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=>insertText(op)}>{op.trim()}</button>
            ))}
          </div>
        </div>

        {/* Main content grid (JSON panel is now external) */}
        <div className={`min-h-[360px] grid grid-cols-1 ${mainColsClass} gap-3`}>
          <div className="min-w-0">
          {tab === 'custom' && (
            <div className="space-y-2 text-[12px]">
              <div className="flex items-center justify-between gap-2 rounded-md border bg-[hsl(var(--secondary)/0.4)] p-2">
                <div className="text-muted-foreground">Add custom columns using the builder. Use the left JSON panel to inspect config.</div>
                <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setShowCustomBuilder(v=>!v)}>{showCustomBuilder ? 'Hide Custom Column' : 'Add Custom Column'}</button>
              </div>
              {showCustomBuilder && (
                <AdvancedSqlCustomColumnBuilder
                  columns={colsAvailable}
                  widgetId={widgetId}
                  source={source}
                  takenNames={takenNames}
                  initial={initialCustom}
                  submitLabel={editingCustomIndex !== null ? 'Update Custom Column' : 'Add Custom Column'}
                  ignoreName={editingCustomIndex !== null ? String(initialCustom?.name || '') : undefined}
                  registerInsertSinkAction={(fn: (txt: string) => void) => { insertSinkRef.current = fn }}
                  onAddAction={(col: any) => {
                    try {
                      const parsed = JSON.parse(editJson || '{}') as any
                      const next = {
                        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                        defaults: parsed?.defaults || undefined,
                      }
                      if (editingCustomIndex !== null && editingCustomIndex >= 0 && editingCustomIndex < next.customColumns.length) {
                        next.customColumns[editingCustomIndex] = col
                      } else {
                        next.customColumns.push(col)
                      }
                      setEditJson(JSON.stringify(next, null, 2))
                    } catch (e) {
                      const next = { customColumns: [col], transforms: [], joins: [] }
                      setEditJson(JSON.stringify(next, null, 2))
                    } finally {
                      setShowCustomBuilder(false)
                      setEditingCustomIndex(null)
                      setInitialCustom(undefined)
                    }
                  }}
                  onCancelAction={() => { setShowCustomBuilder(false); setEditingCustomIndex(null); setInitialCustom(undefined) }}
                />
              )}
              {loading && <div className="text-[11px]">Loading…</div>}
              {error && <div className="text-[11px] text-red-600">{error}</div>}
            </div>
          )}
          {/* Insert tab removed; Mapping moved into Custom column builder */}
          {tab === 'joins' && (
            <div className="space-y-2 text-[12px]">
              <div className="flex items-center justify-between gap-2 rounded-md border bg-[hsl(var(--secondary)/0.4)] p-2">
                <div className="text-muted-foreground">Define joins from the base source to other tables.</div>
                <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setShowJoinBuilder(v=>!v)}>{showJoinBuilder ? 'Hide Join Builder' : 'Add Join'}</button>
              </div>
              {showJoinBuilder && (
                <AdvancedSqlJoinBuilder
                  schema={schemaLocal || schema}
                  baseColumns={colsAvailable}
                  initial={editingJoinIndex !== null ? initialJoin as any : undefined}
                  submitLabel={editingJoinIndex !== null ? 'Update Join' : 'Add Join'}
                  onAddAction={(j: any) => {
                    try {
                      const parsed = JSON.parse(editJson || '{}') as any
                      const next = {
                        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                        defaults: parsed?.defaults || undefined,
                      }
                      if (editingJoinIndex !== null && editingJoinIndex >= 0 && editingJoinIndex < next.joins.length) {
                        next.joins[editingJoinIndex] = j
                      } else {
                        next.joins.push(j)
                      }
                      setEditJson(JSON.stringify(next, null, 2))
                    } catch (e) {
                      const next = { customColumns: [], transforms: [], joins: [j] }
                      setEditJson(JSON.stringify(next, null, 2))
                    } finally {
                      setShowJoinBuilder(false)
                      setEditingJoinIndex(null)
                      setInitialJoin(undefined)
                    }
                  }}
                  onCancelAction={() => { setShowJoinBuilder(false); setEditingJoinIndex(null); setInitialJoin(undefined) }}
                />
              )}
            </div>
          )}
          {tab === 'transform' && (
            <div className="space-y-2 text-[12px]">
              <div className="flex items-center justify-between gap-2 rounded-md border bg-[hsl(var(--secondary)/0.4)] p-2">
                <div className="text-muted-foreground">Add transformations using the builders. Use the left JSON panel to inspect config.</div>
                <div className="flex flex-wrap items-center gap-2 justify-end">
                  <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setShowCaseBuilder(v=>!v)}>{showCaseBuilder ? 'Hide CASE' : 'Add CASE'}</button>
                  <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setShowBulkCasesBuilder(v=>!v)}>{showBulkCasesBuilder ? 'Hide Cases' : 'Add Cases'}</button>
                  <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setShowReplaceBuilder(v=>!v)}>{showReplaceBuilder ? 'Hide Replace/Translate' : 'Add Replace/Translate'}</button>
                  <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setShowComputedBuilder(v=>!v)}>{showComputedBuilder ? 'Hide Computed' : 'Add Computed'}</button>
                  <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setShowNullBuilder(v=>!v)}>{showNullBuilder ? 'Hide NULL' : 'Add NULL'}</button>
                  <button className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=> setShowUnpivotBuilder(v=>!v)}>{showUnpivotBuilder ? 'Hide Unpivot/Union' : 'Add Unpivot/Union'}</button>
                </div>
              </div>
              {showUnpivotBuilder && (
                <AdvancedSqlUnpivotBuilder
                  columns={colsAvailable}
                  dsType={dsType}
                  widgetId={widgetId}
                  datasourceId={datasourceId}
                  source={source}
                  initial={editingTransformIndex !== null && (initialTransform?.type === 'unpivot') ? (initialTransform as any) : undefined}
                  submitLabel={editingTransformIndex !== null ? 'Update Unpivot / Union' : 'Add Unpivot / Union'}
                  onAddAction={(tr: any) => {
                    try {
                      const parsed = JSON.parse(editJson || '{}') as any
                      const next = {
                        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                        defaults: parsed?.defaults || undefined,
                      }
                      if (editingTransformIndex !== null && editingTransformIndex >= 0 && editingTransformIndex < next.transforms.length) {
                        next.transforms[editingTransformIndex] = tr as any
                      } else {
                        next.transforms.push(tr as any)
                      }
                      setEditJson(JSON.stringify(next, null, 2))
                    } catch (e) {
                      const next = { customColumns: [], transforms: [tr], joins: [] }
                      setEditJson(JSON.stringify(next, null, 2))
                    } finally {
                      setShowUnpivotBuilder(false)
                      setEditingTransformIndex(null)
                      setInitialTransform(undefined)
                    }
                  }}
                  onCancelAction={() => { setShowUnpivotBuilder(false); setEditingTransformIndex(null); setInitialTransform(undefined) }}
                />
              )}
              {showCaseBuilder && (
                <AdvancedSqlCaseBuilder
                  columns={colsAvailable}
                  initial={(() => {
                    if (editingTransformIndex === null) return undefined
                    if (initialTransform?.type === 'case') return (initialTransform as any)
                    const expr = String((initialTransform as any)?.expr || '')
                    const isComputedCase = String(initialTransform?.type||'').toLowerCase()==='computed' && /^\s*case\b/i.test(expr)
                    if (isComputedCase) return (initialTransform as any)
                    return undefined
                  })()}
                  submitLabel={editingTransformIndex !== null ? 'Update CASE transform' : 'Add CASE transform'}
                  onAddAction={(tr: any) => {
                    try {
                      const parsed = JSON.parse(editJson || '{}') as any
                      const next = {
                        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                        defaults: parsed?.defaults || undefined,
                      }
                      if (editingTransformIndex !== null && editingTransformIndex >= 0 && editingTransformIndex < next.transforms.length) {
                        next.transforms[editingTransformIndex] = tr as any
                      } else {
                        next.transforms.push(tr as any)
                      }
                      setEditJson(JSON.stringify(next, null, 2))
                    } catch (e) {
                      // fallback: create minimal structure
                      const next = { customColumns: [], transforms: [tr], joins: [] }
                      setEditJson(JSON.stringify(next, null, 2))
                    } finally {
                      setShowCaseBuilder(false)
                      setEditingTransformIndex(null)
                      setInitialTransform(undefined)
                    }
                  }}
                  onCancelAction={() => { setShowCaseBuilder(false); setEditingTransformIndex(null); setInitialTransform(undefined) }}
                />
              )}
              {showBulkCasesBuilder && (
                <AdvancedSqlBulkCasesBuilder
                  columns={colsAvailable}
                  onAddAction={(tr: any) => {
                    try {
                      const parsed = JSON.parse(editJson || '{}') as any
                      const next = {
                        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                        defaults: parsed?.defaults || undefined,
                      }
                      next.transforms.push(tr as any)
                      setEditJson(JSON.stringify(next, null, 2))
                      setShowBulkCasesBuilder(false)
                    } catch (e) {
                      const next = { customColumns: [], transforms: [tr], joins: [] }
                      setEditJson(JSON.stringify(next, null, 2))
                      setShowBulkCasesBuilder(false)
                    }
                  }}
                  onCancelAction={() => setShowBulkCasesBuilder(false)}
                />
              )}
              {showNullBuilder && (
                <AdvancedSqlNullBuilder
                  columns={colsAvailable}
                  initial={editingTransformIndex !== null && (initialTransform?.type === 'nullHandling') ? (initialTransform as any) : undefined}
                  submitLabel={editingTransformIndex !== null ? 'Update NULL handling' : 'Add NULL handling'}
                  onAddAction={(tr: any) => {
                    try {
                      const parsed = JSON.parse(editJson || '{}') as any
                      const next = {
                        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                        defaults: parsed?.defaults || undefined,
                      }
                      if (editingTransformIndex !== null && editingTransformIndex >= 0 && editingTransformIndex < next.transforms.length) {
                        next.transforms[editingTransformIndex] = tr as any
                      } else {
                        next.transforms.push(tr as any)
                      }
                      setEditJson(JSON.stringify(next, null, 2))
                    } catch (e) {
                      const next = { customColumns: [], transforms: [tr], joins: [] }
                      setEditJson(JSON.stringify(next, null, 2))
                    } finally {
                      setShowNullBuilder(false)
                      setEditingTransformIndex(null)
                      setInitialTransform(undefined)
                    }
                  }}
                  onCancelAction={() => { setShowNullBuilder(false); setEditingTransformIndex(null); setInitialTransform(undefined) }}
                />
              )}
              {showReplaceBuilder && (
                <AdvancedSqlReplaceBuilder
                  columns={colsAvailable}
                  initial={editingTransformIndex !== null && ((initialTransform?.type === 'replace') || (initialTransform?.type === 'translate')) ? (initialTransform as any) : undefined}
                  submitLabel={editingTransformIndex !== null ? `Update ${String((initialTransform?.type || 'replace')).toLowerCase()} transform` : undefined}
                  onAddAction={(tr: any) => {
                    try {
                      const parsed = JSON.parse(editJson || '{}') as any
                      const next = {
                        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                        defaults: parsed?.defaults || undefined,
                      }
                      if (editingTransformIndex !== null && editingTransformIndex >= 0 && editingTransformIndex < next.transforms.length) {
                        next.transforms[editingTransformIndex] = tr as any
                      } else {
                        next.transforms.push(tr as any)
                      }
                      setEditJson(JSON.stringify(next, null, 2))
                    } catch (e) {
                      const next = { customColumns: [], transforms: [tr], joins: [] }
                      setEditJson(JSON.stringify(next, null, 2))
                    } finally {
                      setShowReplaceBuilder(false)
                      setEditingTransformIndex(null)
                      setInitialTransform(undefined)
                    }
                  }}
                  onCancelAction={() => { setShowReplaceBuilder(false); setEditingTransformIndex(null); setInitialTransform(undefined) }}
                />
              )}
              {showComputedBuilder && (
                <AdvancedSqlComputedBuilder
                  columns={colsAvailable}
                  initial={editingTransformIndex !== null && (initialTransform?.type === 'computed') ? (initialTransform as any) : undefined}
                  submitLabel={editingTransformIndex !== null ? 'Update Computed' : 'Add Computed'}
                  onAddAction={(tr: any) => {
                    try {
                      const parsed = JSON.parse(editJson || '{}') as any
                      const next = {
                        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                        defaults: parsed?.defaults || undefined,
                      }
                      if (editingTransformIndex !== null && editingTransformIndex >= 0 && editingTransformIndex < next.transforms.length) {
                        next.transforms[editingTransformIndex] = tr as any
                      } else {
                        next.transforms.push(tr as any)
                      }
                      setEditJson(JSON.stringify(next, null, 2))
                    } catch (e) {
                      const next = { customColumns: [], transforms: [tr], joins: [] }
                      setEditJson(JSON.stringify(next, null, 2))
                    } finally {
                      setShowComputedBuilder(false)
                      setEditingTransformIndex(null)
                      setInitialTransform(undefined)
                    }
                  }}
                  onCancelAction={() => { setShowComputedBuilder(false); setEditingTransformIndex(null); setInitialTransform(undefined) }}
                />
              )}
              {/* per-tab lists removed in favor of unified grid */}
            </div>
          )}
          {tab === 'sort' && (
            <div className="space-y-3 text-[12px]">
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
                <label className="text-xs text-muted-foreground sm:col-span-1">Sort by</label>
                <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={sortBy} onChange={(e)=>setSortBy(e.target.value)}>
                  <option value="">(none)</option>
                  {colsAvailable.map(c => (<option key={c} value={c}>{c}</option>))}
                </select>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
                <label className="text-xs text-muted-foreground sm:col-span-1">Direction</label>
                <select className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" value={sortDir} onChange={(e)=>setSortDir(e.target.value as any)}>
                  <option value="asc">asc</option>
                  <option value="desc">desc</option>
                </select>
              </div>
              <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 items-center">
                <label className="text-xs text-muted-foreground sm:col-span-1">Limit (Top N)</label>
                <input className="h-8 px-2 rounded-md bg-card text-xs sm:col-span-2" type="number" min={0} placeholder="e.g., 50" value={limitN} onChange={(e)=>setLimitN(e.target.value)} />
              </div>
              <div className="flex items-center justify-end">
                <button
                  className="text-xs px-3 py-1.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
                  onClick={() => {
                    try {
                      const parsed = JSON.parse(editJson || '{}') as any
                      const next = {
                        customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                        transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                        joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                        defaults: parsed?.defaults || {},
                      } as any
                      next.defaults = next.defaults || {}
                      if (sortBy) next.defaults.sort = { by: sortBy, direction: sortDir }
                      else delete next.defaults.sort
                      const n = parseInt(limitN || '', 10)
                      if (!isNaN(n) && n > 0) next.defaults.limitTopN = { n, by: sortBy || colsAvailable[0] || '1', direction: sortDir }
                      else delete next.defaults.limitTopN
                      setEditJson(JSON.stringify(next, null, 2))
                    } catch (e) {
                      const n = parseInt(limitN || '', 10)
                      const def: any = {}
                      if (sortBy) def.sort = { by: sortBy, direction: sortDir }
                      if (!isNaN(n) && n > 0) def.limitTopN = { n, by: sortBy || '1', direction: sortDir }
                      const next = { customColumns: [], transforms: [], joins: [], defaults: def }
                      setEditJson(JSON.stringify(next, null, 2))
                    }
                  }}
                >Apply</button>
              </div>
            </div>
          )}
          {tab === 'preview' && (
            <div className="space-y-2 text-[12px] max-w-full min-w-0">
              <div className="flex items-center justify-between">
                <div className="text-muted-foreground">Preview the generated SQL and sample rows.</div>
                <button
                  className="text-xs px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
                  onClick={() => runPreview()}
                  disabled={!effectiveDsId || !source || pvLoading || !!jsonError}
                >{pvLoading ? 'Running…' : 'Run Preview'}</button>
              </div>
              {!source && <div className="text-[11px] text-amber-600">Select a source table in the Data section to enable preview.</div>}
              {!!error && (
                <div className="text-[11px] text-red-600">{error}</div>
              )}
              {pvWarn.length > 0 && (
                <div className="text-[11px] text-amber-600">Warnings: {pvWarn.join(' · ')}</div>
              )}
              <div>
                <div className="text-[11px] font-medium mb-1">SQL</div>
                <div className="max-w-full overflow-x-auto">
                  <pre className="text-[11px] font-mono rounded-md border bg-[hsl(var(--secondary))] p-2 max-h-48 overflow-y-auto whitespace-pre min-w-0">{pvSql || '—'}</pre>
                </div>
              </div>
              <div>
                <div className="text-[11px] font-medium mb-1">Rows ({pvRows.length})</div>
                <div className="rounded-md border overflow-x-auto max-h-[400px] overflow-y-auto max-w-full">
                  <table className="min-w-max text-[11px]">
                    <thead className="sticky top-0 z-10">
                      {(() => {
                        const cols = pvCols.filter(c => c !== '*')
                        return (
                          <tr>{cols.map((c) => (<th key={c} className="text-left px-2 py-1 border-b bg-[hsl(var(--secondary)/0.6)] sticky top-0 whitespace-pre">{c}</th>))}</tr>
                        )
                      })()}
                    </thead>
                    <tbody>
                      {pvRows.slice(0, 50).map((r, i) => {
                        const cols = pvCols.filter(c => c !== '*')
                        const baseCount = Array.isArray(r) ? Math.max(0, r.length - cols.length) : 0
                        return (
                          <tr key={i}>
                            {cols.map((c, j) => (
                              <td key={j} className="px-2 py-1 border-b whitespace-pre">{String(Array.isArray(r) ? r[baseCount + j] : (r as any)?.[c])}</td>
                            ))}
                          </tr>
                        )
                      })}
                      {pvRows.length === 0 && (
                        <tr><td className="px-2 py-2 text-muted-foreground" colSpan={Math.max(1, (pvCols.filter(c=>c!=='*').length || 1))}>No rows</td></tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          )}
          </div>
          {/* Right-side examples panel */}
          {/* Collapsed stub: show a small button to open the panel when hidden (lg and up) */}
          {!examplesOpen && (
            <div className="hidden lg:block text-[11px]">
              <div className="sticky top-2">
                <button
                  type="button"
                  className="px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
                  onClick={() => setExamplesOpen(true)}
                  title="Show Examples"
                >Show Examples</button>
              </div>
            </div>
          )}
          {examplesOpen && (
          <aside className="hidden lg:block text-[11px]">
            <div className="sticky top-2 mb-2 flex items-center justify-end">
              <button
                type="button"
                className="px-2 py-1 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
                onClick={() => setExamplesOpen(false)}
                title="Hide Examples"
              >Hide Examples</button>
            </div>
            {tab === 'custom' && (
              <div className="sticky top-2 space-y-2">
                <div className="font-medium">Examples: Custom Columns</div>
                <pre className="p-2 rounded-md border bg-[hsl(var(--secondary))] whitespace-pre-wrap">{`{
  "customColumns": [
    { "name": "net", "expr": "price * qty", "type": "number" }
  ],
  "transforms": [],
  "joins": []
}`}</pre>
              </div>
            )}
            {tab === 'joins' && (
              <div className="sticky top-2 space-y-2">
                <div className="font-medium">Examples: Join</div>
                <pre className="p-2 rounded-md border bg-[hsl(var(--secondary))] whitespace-pre-wrap">{`{
  "joins": [
    {
      "joinType": "left",
      "targetTable": "public.customers",
      "sourceKey": "customer_id",
      "targetKey": "id",
      "columns": [ { "name": "name", "alias": "customer_name" } ],
      "filter": { "op": "eq", "left": "t.status", "right": "active" },
      "aggregate": { "fn": "count", "column": "id", "alias": "orders_count" }
    }
  ]
}`}</pre>
              </div>
            )}
            {tab === 'transform' && (
              <div className="sticky top-2 space-y-2 max-h-[320px] overflow-y-auto pr-1">
                <div className="font-medium">Examples: Transforms</div>
                <div>CASE (single)</div>
                <pre className="p-2 rounded-md border bg-[hsl(var(--secondary))] whitespace-pre-wrap">{`{ "type": "case", "target": "status", "cases": [ { "when": { "op": "eq", "left": "status", "right": "3" }, "then": "BOP" } ], "else": "Other" }`}</pre>
                <div>CASE (bulk)</div>
                <pre className="p-2 rounded-md border bg-[hsl(var(--secondary))] whitespace-pre-wrap">{`{ "type": "case", "target": "clientID", "cases": [ { "when": { "op": "eq", "left": "clientID", "right": "3" }, "then": "BOP" }, { "when": { "op": "eq", "left": "clientID", "right": "4" }, "then": "ACME" } ], "else": "Other" }`}</pre>
                <div>Replace</div>
                <pre className="p-2 rounded-md border bg-[hsl(var(--secondary))] whitespace-pre-wrap">{`{ "type": "replace", "target": "city", "search": ["SF","LA"], "replace": ["San Francisco","Los Angeles"] }`}</pre>
                <div>Computed</div>
                <pre className="p-2 rounded-md border bg-[hsl(var(--secondary))] whitespace-pre-wrap">{`{ "type": "computed", "name": "amount", "expr": "price * qty", "valueType": "number" }`}</pre>
                <pre className="p-2 rounded-md border bg-[hsl(var(--secondary))] whitespace-pre-wrap">{`{ "type": "nullHandling", "target": "name", "mode": "coalesce", "value": "Unknown" }`}</pre>
              </div>
            )}
            {tab === 'sort' && (
              <div className="sticky top-2 space-y-2">
                <div className="font-medium">Examples: Defaults</div>
                <pre className="p-2 rounded-md border bg-[hsl(var(--secondary))] whitespace-pre-wrap">{`{
  "defaults": { "sort": { "by": "created_at", "direction": "desc" }, "limitTopN": { "n": 50, "by": "created_at", "direction": "desc" } }
}`}</pre>
              </div>
            )}
          </aside>
          )}
        </div>
        <div className="mt-4 flex items-center justify-between gap-2">
          <div className="flex items-center gap-2">
            <button
              className="text-xs px-3 py-1.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
              onClick={() => {
                try {
                  const blob = new Blob([editJson || '{}'], { type: 'application/json' })
                  const url = URL.createObjectURL(blob)
                  const a = document.createElement('a')
                  a.href = url
                  a.download = `transforms-${effectiveDsId || 'export'}-${Date.now()}.json`
                  document.body.appendChild(a)
                  a.click()
                  document.body.removeChild(a)
                  URL.revokeObjectURL(url)
                } catch (e: any) {
                  setError(String(e?.message || 'Export failed'))
                }
              }}
              title="Export all transforms to JSON file"
            >Export</button>
            <button
              className="text-xs px-3 py-1.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]"
              onClick={() => {
                try {
                  const input = document.createElement('input')
                  input.type = 'file'
                  input.accept = 'application/json,.json'
                  input.onchange = async (e: any) => {
                    try {
                      const file = e.target?.files?.[0]
                      if (!file) {
                        console.log('[Import] No file selected')
                        return
                      }
                      console.log('[Import] File selected:', file.name, 'size:', file.size)
                      const text = await file.text()
                      console.log('[Import] File text length:', text.length)
                      console.log('[Import] First 200 chars:', text.substring(0, 200))
                      const imported = JSON.parse(text) as any
                      
                      // Debug: Log what we're importing
                      console.log('[Import] Parsed imported data:', imported)
                      console.log('[Import] Type of imported:', typeof imported)
                      console.log('[Import] Is object?', imported && typeof imported === 'object')
                      console.log('[Import] Custom columns:', imported?.customColumns, 'isArray:', Array.isArray(imported?.customColumns))
                      console.log('[Import] Transforms:', imported?.transforms, 'isArray:', Array.isArray(imported?.transforms))
                      console.log('[Import] Joins:', imported?.joins, 'isArray:', Array.isArray(imported?.joins))
                      
                      // Merge logic: update existing by name, add new ones
                      const current = JSON.parse(editJson || '{}') as any
                      const merged = {
                        customColumns: Array.isArray(current?.customColumns) ? [...current.customColumns] : [],
                        transforms: Array.isArray(current?.transforms) ? [...current.transforms] : [],
                        joins: Array.isArray(current?.joins) ? [...current.joins] : [],
                        defaults: current?.defaults || undefined,
                      }
                      
                      // Merge custom columns by name (if name exists, otherwise just add)
                      if (Array.isArray(imported?.customColumns)) {
                        imported.customColumns.forEach((ic: any) => {
                          if (!ic || typeof ic !== 'object') return
                          const name = String(ic?.name || '').trim()
                          if (name) {
                            const idx = merged.customColumns.findIndex((c: any) => String(c?.name || '').trim().toLowerCase() === name.toLowerCase())
                            if (idx >= 0) merged.customColumns[idx] = ic
                            else merged.customColumns.push(ic)
                          } else {
                            // No name, just append
                            merged.customColumns.push(ic)
                          }
                        })
                      }
                      
                      // Merge transforms by target/name (if exists, otherwise just add)
                      if (Array.isArray(imported?.transforms)) {
                        imported.transforms.forEach((it: any) => {
                          if (!it || typeof it !== 'object') return
                          const key = String(it?.target || it?.name || '').trim()
                          if (key) {
                            const idx = merged.transforms.findIndex((t: any) => {
                              const tk = String(t?.target || t?.name || '').trim()
                              return tk.toLowerCase() === key.toLowerCase()
                            })
                            if (idx >= 0) merged.transforms[idx] = it
                            else merged.transforms.push(it)
                          } else {
                            // No key, just append
                            merged.transforms.push(it)
                          }
                        })
                      }
                      
                      // Merge joins by targetTable (if exists, otherwise just add)
                      if (Array.isArray(imported?.joins)) {
                        imported.joins.forEach((ij: any) => {
                          if (!ij || typeof ij !== 'object') return
                          const table = String(ij?.targetTable || '').trim()
                          if (table) {
                            const idx = merged.joins.findIndex((j: any) => String(j?.targetTable || '').trim().toLowerCase() === table.toLowerCase())
                            if (idx >= 0) merged.joins[idx] = ij
                            else merged.joins.push(ij)
                          } else {
                            // No targetTable, just append
                            merged.joins.push(ij)
                          }
                        })
                      }
                      
                      // Merge defaults
                      if (imported?.defaults) {
                        merged.defaults = { ...merged.defaults, ...imported.defaults }
                      }
                      
                      // Count what was imported (validate with actual object presence, not just name)
                      const importedCCCount = Array.isArray(imported?.customColumns) ? imported.customColumns.length : 0
                      const importedTRCount = Array.isArray(imported?.transforms) ? imported.transforms.length : 0
                      const importedJNCount = Array.isArray(imported?.joins) ? imported.joins.length : 0
                      const totalImported = importedCCCount + importedTRCount + importedJNCount
                      
                      // Debug counts
                      console.log('[Import] Counts:', { importedCCCount, importedTRCount, importedJNCount, totalImported })
                      console.log('[Import] Merged result:', merged)
                      
                      setEditJson(JSON.stringify(merged, null, 2))
                      setError(undefined)
                      
                      // Show success feedback and scroll to statements panel
                      if (totalImported > 0) {
                        const parts: string[] = []
                        if (importedCCCount > 0) parts.push(`${importedCCCount} custom column${importedCCCount > 1 ? 's' : ''}`)
                        if (importedTRCount > 0) parts.push(`${importedTRCount} transform${importedTRCount > 1 ? 's' : ''}`)
                        if (importedJNCount > 0) parts.push(`${importedJNCount} join${importedJNCount > 1 ? 's' : ''}`)
                        
                        // Scroll to All Statements panel after a brief delay to let React render
                        setTimeout(() => {
                          try {
                            const statementsPanel = document.querySelector('[class*="All Statements"]')?.parentElement
                            if (statementsPanel) {
                              statementsPanel.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
                            }
                          } catch {}
                        }, 100)
                        
                        setToast({ message: `Import successful: ${parts.join(', ')}`, type: 'success' })
                        setTimeout(() => setToast(null), 5000)
                      } else {
                        // Provide diagnostic info in the error message
                        const ccLen = imported?.customColumns?.length || 0
                        const trLen = imported?.transforms?.length || 0
                        const jnLen = imported?.joins?.length || 0
                        const diagnostic = `Detected: ${ccLen} columns, ${trLen} transforms, ${jnLen} joins`
                        console.error('[Import] No items found.', diagnostic, 'Raw:', { cc: imported?.customColumns, tr: imported?.transforms, jn: imported?.joins })
                        setToast({ message: `No valid items in file. ${diagnostic}. Check console for details.`, type: 'error' })
                        setTimeout(() => setToast(null), 8000)
                      }
                    } catch (e: any) {
                      const errMsg = String(e?.message || 'Import failed - invalid JSON')
                      setError(errMsg)
                      setToast({ message: `Import failed: ${errMsg}`, type: 'error' })
                      setTimeout(() => setToast(null), 5000)
                    }
                  }
                  input.click()
                } catch (e: any) {
                  const errMsg = String(e?.message || 'Import failed')
                  setError(errMsg)
                  setToast({ message: `Import failed: ${errMsg}`, type: 'error' })
                  setTimeout(() => setToast(null), 5000)
                }
              }}
              title="Import and merge transforms from JSON file"
            >Import</button>
          </div>
          <div className="flex items-center gap-2">
            <button className="text-xs px-3 py-1.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCloseAction}>Close</button>
            <button
              className={`text-xs px-3 py-1.5 rounded-md border ${canSave ? 'bg-[hsl(var(--btn3))] text-black' : 'opacity-60 cursor-not-allowed'}`}
              disabled={!canSave || saving}
              onClick={async () => {
              if (!effectiveDsId) return
              setSaving(true); setError(undefined)
              try {
                const parsed = JSON.parse(editJson || '{}') as any
                const payload: DatasourceTransforms = {
                  customColumns: Array.isArray(parsed?.customColumns) ? parsed.customColumns : [],
                  transforms: Array.isArray(parsed?.transforms) ? parsed.transforms : [],
                  joins: Array.isArray(parsed?.joins) ? parsed.joins : [],
                  ...(parsed?.defaults ? { defaults: parsed.defaults } : {}),
                }
                await Api.saveDatasourceTransforms(effectiveDsId, payload)
                setModel(payload)
                setBaselineJson(JSON.stringify(payload, null, 2))
                try {
                  if (typeof window !== 'undefined') {
                    window.dispatchEvent(new CustomEvent('datasource-transforms-saved', { detail: { datasourceId: effectiveDsId } }))
                  }
                } catch {}
                onCloseAction()
              } catch (e: any) {
                setError(String(e?.message || 'Failed to save'))
              } finally {
                setSaving(false)
              }
            }}
            title={!effectiveDsId ? 'Select a datasource first' : ''}
          >
            {saving ? 'Saving…' : 'Save'}
          </button>
          </div>
        </div>
        {/* Unified bottom grid for all statements */}
        <div className="mt-4">
          <div className="text-[11px] font-medium mb-1">All Statements</div>
          {jsonError ? (
            <div className="text-[11px] text-amber-600">Fix JSON to view items.</div>
          ) : (
            <div className="rounded-md border overflow-x-auto max-w-full">
              <table className="min-w-full text-[11px]">
                <thead className="bg-[hsl(var(--secondary)/0.6)]">
                  <tr>
                    <th className="text-left px-2 py-1 border-b">Type</th>
                    <th className="text-left px-2 py-1 border-b">Summary</th>
                    <th className="text-left px-2 py-1 border-b">Scope</th>
                    <th className="text-left px-2 py-1 border-b">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {/* Custom Columns */}
                  {listCC.map((c: any, i: number) => (
                    <tr key={`cc-${i}`}>
                      <td className="px-2 py-1 border-b whitespace-pre">custom</td>
                      <td className="px-2 py-1 border-b whitespace-pre">
                        <span className="font-medium">{String(c?.name || '(unnamed)')}</span>
                        {' — '}<span className="text-muted-foreground" title={String(c?.expr||'')}>{shorten(c?.expr || '', 80)}</span>
                      </td>
                      <td className="px-2 py-1 border-b whitespace-pre">
                        <div className="flex items-center gap-1">
                          <select className="h-6 px-1 rounded-md bg-card text-[11px]" value={scopeLevelOf(c)} onChange={(e)=>setScopeOnItem('customColumns', i, e.target.value as any)}>
                            <option value="datasource">datasource</option>
                            <option value="table" disabled={!source}>table</option>
                            <option value="widget" disabled={!widgetId}>widget</option>
                          </select>
                          <span className="text-muted-foreground">{scopeBadge(c?.scope)}</span>
                        </div>
                      </td>
                      <td className="px-2 py-1 border-b">
                        <div className="flex items-center gap-2">
                          <button className="px-2 py-0.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=>openEditCustom(i, c)}>Edit</button>
                          <button className="px-2 py-0.5 rounded-md border bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]" onClick={()=>removeCustomColumn(i)}>Delete</button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {/* Transforms */}
                  {listTR.map((t: any, i: number) => (
                    <tr key={`tr-${i}`}>
                      <td className="px-2 py-1 border-b whitespace-pre">{(() => { const typ=String(t?.type||''); const expr=String((t as any)?.expr||''); const looks=/^\s*case\b/i.test(expr) && typ.toLowerCase()==='computed'; return looks ? 'case' : typ })()}</td>
                      <td className="px-2 py-1 border-b whitespace-pre">
                        {(() => { const typ=String(t?.type||''); const expr=String((t as any)?.expr||''); const looks=/^\s*case\b/i.test(expr) && typ.toLowerCase()==='computed';
                          if (t?.type === 'case' || looks) return (<span>{String((t as any)?.target || (t as any)?.name || '')} — {Array.isArray((t as any)?.cases) ? `${(t as any).cases.length} cases` : (looks ? 'CASE expr' : '0 cases')}</span>)
                          if (t?.type === 'replace') return (<span>{String(t?.target || '')} — replace</span>)
                          if (t?.type === 'translate') return (<span>{String(t?.target || '')} — translate</span>)
                          if (t?.type === 'computed') return (<span>{String(t?.name || '')} — {shorten(t?.expr || '', 80)}</span>)
                          if (t?.type === 'nullHandling') return (<span>{String(t?.target || '')} — {String(t?.mode || '')}</span>)
                          if (t?.type === 'unpivot') return (<span>{String(t?.keyColumn || '')}/{String(t?.valueColumn || '')} — {Array.isArray(t?.sourceColumns)? `${t.sourceColumns.length} cols` : ''}, {String(t?.mode || 'auto')}</span>)
                          return null })()}
                      </td>
                      <td className="px-2 py-1 border-b whitespace-pre">
                        <div className="flex items-center gap-1">
                          <select className="h-6 px-1 rounded-md bg-card text-[11px]" value={scopeLevelOf(t)} onChange={(e)=>setScopeOnItem('transforms', i, e.target.value as any)}>
                            <option value="datasource">datasource</option>
                            <option value="table" disabled={!source}>table</option>
                            <option value="widget" disabled={!widgetId}>widget</option>
                          </select>
                          <span className="text-muted-foreground">{scopeBadge(t?.scope)}</span>
                        </div>
                      </td>
                      <td className="px-2 py-1 border-b">
                        <div className="flex items-center gap-2">
                          <button className="px-2 py-0.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=>openEditTransform(i, t)}>Edit</button>
                          <button className="px-2 py-0.5 rounded-md border bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]" onClick={()=>removeTransform(i)}>Delete</button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {/* Joins */}
                  {listJN.map((j: any, i: number) => (
                    <tr key={`jn-${i}`}>
                      <td className="px-2 py-1 border-b whitespace-pre">join</td>
                      <td className="px-2 py-1 border-b whitespace-pre">{String(j?.joinType || 'left')} → {String(j?.targetTable || '')} on {String(j?.sourceKey || '')} = {String(j?.targetKey || '')}</td>
                      <td className="px-2 py-1 border-b whitespace-pre">
                        <div className="flex items-center gap-1">
                          <select className="h-6 px-1 rounded-md bg-card text-[11px]" value={scopeLevelOf(j)} onChange={(e)=>setScopeOnItem('joins', i, e.target.value as any)}>
                            <option value="datasource">datasource</option>
                            <option value="table" disabled={!source}>table</option>
                            <option value="widget" disabled={!widgetId}>widget</option>
                          </select>
                          <span className="text-muted-foreground">{scopeBadge(j?.scope)}</span>
                        </div>
                      </td>
                      <td className="px-2 py-1 border-b">
                        <div className="flex items-center gap-2">
                          <button className="px-2 py-0.5 rounded-md border bg-card hover:bg-[hsl(var(--secondary)/0.6)]" onClick={()=>openEditJoin(i, j)}>Edit</button>
                          <button className="px-2 py-0.5 rounded-md border bg-[hsl(var(--secondary)/0.6)] hover:bg-[hsl(var(--secondary))]" onClick={()=>removeJoin(i)}>Delete</button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {(listCC.length + listTR.length + listJN.length === 0) && (
                    <tr><td className="px-2 py-2 text-muted-foreground" colSpan={4}>No statements</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>,
    document.body
  )

  // Toast notification (outside portal)
  const toastPortal = toast && typeof window !== 'undefined' ? createPortal(
    <div className="fixed bottom-4 right-4 z-[9999] animate-in slide-in-from-bottom-2">
      <div 
        className={`px-4 py-3 rounded-lg shadow-lg border ${
          toast.type === 'success' 
            ? 'bg-emerald-50 dark:bg-emerald-950 border-emerald-200 dark:border-emerald-800 text-emerald-900 dark:text-emerald-100' 
            : 'bg-red-50 dark:bg-red-950 border-red-200 dark:border-red-800 text-red-900 dark:text-red-100'
        }`}
      >
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium">{toast.message}</span>
          <button 
            onClick={() => setToast(null)} 
            className="text-xs opacity-60 hover:opacity-100"
          >✕</button>
        </div>
      </div>
    </div>,
    document.body
  ) : null

  return <>{mainPortal}{toastPortal}</>
}
