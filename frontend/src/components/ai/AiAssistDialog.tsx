"use client"

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { Api, QueryApi, type IntrospectResponse, type TablesOnlyResponse } from '@/lib/api'
import * as SchemaCache from '@/lib/schemaCache'
import type { WidgetConfig } from '@/types/widgets'
import { useEnvironment } from '@/components/providers/EnvironmentProvider'
import { useAuth } from '@/components/providers/AuthProvider'
import KpiCard from '@/components/widgets/KpiCard'
import ChartCard from '@/components/widgets/ChartCard'
import TableCard from '@/components/widgets/TableCard'
import { RiSparkling2Fill, RiStopFill } from '@remixicon/react'

export default function AiAssistDialog({
  open,
  onCloseAction,
  widget,
  onApplyAction,
}: {
  open: boolean
  onCloseAction: () => void
  widget?: WidgetConfig | null
  onApplyAction: (cfg: WidgetConfig) => void
}) {
  const { env } = useEnvironment()
  const [datasourceId, setDatasourceId] = useState<string | undefined>(widget?.datasourceId)
  const [schema, setSchema] = useState<IntrospectResponse | null>(null)
  const [source, setSource] = useState<string | undefined>((widget?.querySpec as any)?.source)
  const [columns, setColumns] = useState<Array<{ name: string; type?: string | null }>>([])
  const [samples, setSamples] = useState<any[]>([])
  const [description, setDescription] = useState<string>('')
  const [descOpen, setDescOpen] = useState<boolean>(false)
  const [userPrompt, setUserPrompt] = useState<string>('')
  const [enhancedPrompt, setEnhancedPrompt] = useState<string>('')
  const [variants, setVariants] = useState<WidgetConfig[]>([])
  const [variantStats, setVariantStats] = useState<{ received: number; kept: number; dropped: number }>({ received: 0, kept: 0, dropped: 0 })
  const [loading, setLoading] = useState<{ describe?: boolean; enhance?: boolean; suggest?: boolean }>({})
  const [error, setError] = useState<string | null>(null)
  const [loadingSchema, setLoadingSchema] = useState<boolean>(false)
  const [tablesFast, setTablesFast] = useState<Array<{ key: string; label: string }>>([])
  const [customColNames, setCustomColNames] = useState<string[]>([])
  const suggestAbortRef = useRef<AbortController | null>(null)
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const loadMoreRef = useRef<HTMLDivElement | null>(null)
  const lastAutoSuggestAt = useRef<number>(0)
  // Prompt IntelliSense
  const promptRef = useRef<HTMLTextAreaElement | null>(null)
  const bracketStartRef = useRef<number | null>(null)
  const nextCaretRef = useRef<number | null>(null)
  const [hintOpen, setHintOpen] = useState<boolean>(false)
  const [hintIndex, setHintIndex] = useState<number>(0)
  const [hintQuery, setHintQuery] = useState<string>('')
  const [hintCandidates, setHintCandidates] = useState<string[]>([])

  useEffect(() => {
    if (!open) return
    setDatasourceId(widget?.datasourceId)
    setSource((widget?.querySpec as any)?.source)
    setColumns([]); setSamples([])
    setDescription(''); setUserPrompt(''); setEnhancedPrompt(''); setVariants([])
    setVariantStats({ received: 0, kept: 0, dropped: 0 })
    setError(null)
  }, [open, widget?.id])

  useEffect(() => {
    if (!open || !datasourceId) return
    let cancelled = false
    ;(async () => {
      try {
        setLoadingSchema(true)
        // 1) Seed from SchemaCache without refetch
        try {
          const cached = SchemaCache.get(datasourceId)
          if (cached) setSchema(cached)
        } catch {}
        // 2) Always fetch lightweight tables-only list (includes views) so list is fresh even if schema is cached
        try {
          // Don't pass abort signal - let the request complete
          const fast = await Api.tablesOnly(datasourceId)
          if (!cancelled) {
            const pairs: Array<{ key: string; label: string }> = []
            ;(fast?.schemas || []).forEach((sch) => {
              ;(sch?.tables || []).forEach((t) => {
                const k = sch.name ? `${sch.name}.${t}` : t
                pairs.push({ key: k, label: k })
              })
            })
            // Deduplicate by key
            const uniq = Array.from(new Map(pairs.map((it) => [it.key.toLowerCase(), it])).values())
            setTablesFast(uniq)
          }
        } catch { /* ignore */ }
        // Fetch custom columns for this datasource to prioritize in planning
        try {
          const tr = await Api.getDatasourceTransforms(String(datasourceId))
          const names = ((tr?.customColumns || []) as any[]).map((c: any) => String(c?.name || '')).filter(Boolean)
          if (!cancelled) setCustomColNames(names)
        } catch {
          if (!cancelled) setCustomColNames([])
        }
      } catch {
        if (!cancelled) { setSchema((prev)=>prev); setTablesFast([]) }
      } finally {
        if (!cancelled) setLoadingSchema(false)
      }
    })()
    return () => { cancelled = true }
  }, [open, datasourceId])

  const tables: Array<{ key: string; label: string; cols: Array<{ name: string; type?: string | null }> }> = useMemo(() => {
    // Prefer fast tables-only if present (when schema not cached yet)
    if (tablesFast.length > 0) {
      return tablesFast.map((it) => ({ key: it.key, label: it.label, cols: [] as Array<{ name: string; type?: string | null }> }))
    }
    const out: Array<{ key: string; label: string; cols: Array<{ name: string; type?: string | null }> }> = []
    try {
      ;(schema?.schemas || []).forEach((sch) => {
        (sch.tables || []).forEach((t) => {
          const key = sch.name ? `${sch.name}.${t.name}` : t.name
          out.push({ key, label: key, cols: t.columns || [] })
        })
      })
    } catch {}
    // Deduplicate by key
    return Array.from(new Map(out.map((it) => [it.key.toLowerCase(), it])).values())
  }, [schema, tablesFast])

  useEffect(() => {
    if (!open || !datasourceId || !source) return
    // Clear existing description and samples when table changes
    setDescription('')
    setSamples([])
    setError(null)
    const t = tables.find((x) => String(x.key).toLowerCase() === String(source).toLowerCase())
    setColumns(t?.cols || [])
    ;(async () => {
      try {
        const res = await QueryApi.querySpec({ spec: { source, limit: 10 }, datasourceId, limit: 10, offset: 0 })
        const cols = (res?.columns || []) as string[]
        const rows = (res?.rows || []) as any[]
        try { setColumns(cols.map((n) => ({ name: String(n) })) as any) } catch {}
        const named = rows.slice(0, 10).map((arr: any[]) => {
          const o: Record<string, any> = {}
          cols.forEach((c, i) => { o[c] = arr[i] })
          return o
        })
        setSamples(named)
      } catch { setSamples([]) }
    })()
  }, [open, datasourceId, source, JSON.stringify(tables.map(t => t.key))])

  // Auto-trigger describe only after samples are available
  useEffect(() => {
    if (!open || !datasourceId || !source) return
    if (!description && !loading.describe && samples.length > 0) {
      onDescribe()
    }
  }, [open, datasourceId, source, samples.length])

  const allowedTypes = useMemo(() => {
    const chartTypes: WidgetConfig['chartType'][] = ['line','bar','area','column','donut','categoryBar','spark','combo','scatter','tremorTable','heatmap','barList','gantt']
    return ['chart','table','kpi', ...chartTypes] as Array<any>
  }, [])

  const wantsChart = useMemo(() => /\b(chart|line|bar|column|spark|donut|pie|area|scatter)\b/i.test(userPrompt || ''), [userPrompt])
  const wantedChartType = useMemo(() => {
    const p = (userPrompt || '').toLowerCase()
    if (p.includes('bar')) return 'bar'
    if (p.includes('column')) return 'column'
    if (p.includes('line')) return 'line'
    if (p.includes('donut') || p.includes('pie')) return 'donut'
    if (p.includes('area')) return 'area'
    if (p.includes('scatter')) return 'scatter'
    return 'bar'
  }, [userPrompt])

  // Normalize model output to a single-line natural-language instruction
  const normalizeEnhancedPrompt = (text: string): string => {
    try {
      let s = String(text || '').trim()
      // Remove markdown code fences and leading language hints
      s = s.replace(/^```[a-zA-Z]*\n?|```$/g, '').trim()
      s = s.replace(/^json\s*/i, '').trim()
      // If JSON, convert to NL
      if (s.startsWith('{') || s.startsWith('[')) {
        try {
          const data = JSON.parse(s)
          const obj = Array.isArray(data) ? (data[0] || data) : data
          const type = (obj.chartType || obj.type || 'chart').toString()
          // fields: [{ column, axis, timeUnit?, aggregate? }]
          const fields = Array.isArray(obj.fields) ? obj.fields : []
          const x = fields.find((f: any) => (f?.axis || '').toLowerCase() === 'x') || { column: obj.xField, timeUnit: obj.timeUnit }
          const y = fields.find((f: any) => (f?.axis || '').toLowerCase() === 'y') || { column: obj.yField, aggregate: obj.aggregate || obj.agg }
          const parts: string[] = []
          parts.push(`Create a ${type} chart`)
          if (y?.column) {
            parts.push(`showing ${(y.aggregate || 'sum').toString()} of [${y.column}]`)
          }
          if (x?.column) {
            const unit = x.timeUnit ? ` (${x.timeUnit})` : ''
            parts.push(`by [${x.column}]${unit}`)
          }
          s = parts.join(' ').replace(/\s+/g, ' ').trim()
        } catch {
          // fall back to plain text below
        }
      }
      // Ensure single line imperative
      s = s.replace(/[\r\n]+/g, ' ').trim()
      if (!/^\w+\s/i.test(s)) s = `Create ${s}`
      return s
    } catch { return String(text || '') }
  }

  const disabled = !env.aiApiKey || !env.aiProvider || !env.aiModel

  const onDescribe = async () => {
    setError(null); setLoading((p) => ({ ...p, describe: true }))
    try {
      if (!datasourceId || !source) throw new Error('Select datasource and table')
      const resp = await Api.aiDescribe({ provider: (env.aiProvider || 'gemini') as any, model: env.aiModel || 'gemini-1.5-flash', apiKey: env.aiApiKey || '', schema: { table: String(source), columns }, samples })
      setDescription(String(resp?.description || ''))
    } catch (e: any) { setError(e?.message || 'Describe failed') }
    finally { setLoading((p) => ({ ...p, describe: false })) }
  }

  const onEnhance = async (): Promise<string | null> => {
    setError(null); setLoading((p) => ({ ...p, enhance: true }))
    try {
      if (!datasourceId || !source) throw new Error('Select datasource and table')
      const chartTypes: WidgetConfig['chartType'][] = ['line','bar','area','column','donut','categoryBar','spark','combo','scatter','tremorTable','heatmap','barList','gantt']
      const allowed = wantsChart ? (['chart', ...chartTypes] as Array<any>) : allowedTypes
      const resp = await Api.aiEnhance({ provider: (env.aiProvider || 'gemini') as any, model: env.aiModel || 'gemini-1.5-flash', apiKey: env.aiApiKey || '', schema: { table: String(source), columns }, description, userPrompt, allowedTypes: allowed })
      const raw = String(resp?.enhancedPrompt || '')
      const enhanced = normalizeEnhancedPrompt(raw)
      setEnhancedPrompt(enhanced)
      if (enhanced) setUserPrompt(enhanced)
      return enhanced
    } catch (e: any) { setError(e?.message || 'Enhance failed') }
    finally { setLoading((p) => ({ ...p, enhance: false })) }
    return null
  }

  const onSuggest = async (promptOverride?: string, planOverride?: string, reset?: boolean) => {
    setError(null); setLoading((p) => ({ ...p, suggest: true }))
    try {
      if (!datasourceId || !source) throw new Error('Select datasource and table')
      const promptText = (promptOverride ?? userPrompt)
      // If this is a new Suggest click, clear existing previews and start from offset 0
      if (reset) {
        setVariants([])
        setVariantStats({ received: 0, kept: 0, dropped: 0 })
        lastAutoSuggestAt.current = 0
      }
      const offset = reset ? 0 : variants.length
      // Start abortable request
      try { suggestAbortRef.current?.abort() } catch {}
      const ac = new AbortController(); suggestAbortRef.current = ac
      // Ensure we have columns; fetch a 1-row sample if needed
      let effCols = columns
      if ((!effCols || effCols.length === 0) && datasourceId && source) {
        try {
          const res1 = await QueryApi.querySpec({ spec: { source, limit: 1 }, datasourceId, limit: 1, offset: 0 })
          const names1 = (res1?.columns || []) as string[]
          if (names1.length > 0) {
            effCols = names1.map((n) => ({ name: String(n) })) as any
            setColumns(effCols as any)
          }
        } catch {}
      }
      // Prefer prompt intent: if it asks for a chart, request chart variants even if current widget type differs
      const targetType = ((wantsChart ? 'chart' : (widget?.type ? String(widget.type).toLowerCase() : undefined)) as any)
      const resp = await Api.aiSuggest({ provider: (env.aiProvider || 'gemini') as any, model: env.aiModel || 'gemini-1.5-flash', apiKey: env.aiApiKey || '', schema: { table: String(source), columns: effCols }, samples, prompt: promptText, plan: planOverride, variantOffset: offset, targetType }, ac.signal)
      const rawArr = ((resp?.variants || []) as any[])
      const sanitize = (v: any) => {
        const copy: any = { ...(v || {}) }
        // Enforce widget type if provided; otherwise coerce when prompt asks for chart
        const widgetType = (widget?.type ? String(widget.type).toLowerCase() : undefined)
        if (widgetType) {
          copy.type = widgetType
          if (widgetType === 'chart') { if (!copy.chartType) copy.chartType = wantedChartType }
          if (widgetType !== 'chart') { delete copy.chartType }
        } else if (wantsChart) {
          copy.type = 'chart'; if (!copy.chartType) copy.chartType = wantedChartType
        }
        // Default queryMode and stitch source
        copy.queryMode = (copy.queryMode === 'spec' || copy.querySpec) ? 'spec' : (copy.sql ? 'sql' : 'spec')
        const qs = { ...((copy.querySpec || {}) as any), source }
        copy.querySpec = qs
        copy.datasourceId = datasourceId
        // Trim strings
        if (typeof copy.title === 'string') copy.title = copy.title.trim()
        if (typeof copy.sql === 'string') copy.sql = copy.sql.trim()
        return copy
      }
      const isValid = (v: any): boolean => {
        try {
          if (!v || typeof v !== 'object') return false
          const t = String(v.type || (v.chartType ? 'chart' : 'chart')).toLowerCase()
          const nameOf = (s: any) => String(s ?? '').trim().replace(/^\[(.*)\]$/, '$1')
          const colSet = new Set((columns || []).map(c => String(c.name || '').trim().toLowerCase()))
          const fieldExists = (f?: any) => {
            if (!f) return false
            const n = nameOf(f)
            return colSet.has(n.toLowerCase())
          }
          if (t === 'chart') {
            const okChartType = ['line','bar','area','column','donut','categoryBar','spark','combo','scatter','tremorTable','heatmap','barList','gantt'].includes(String(v.chartType || '').toLowerCase())
            if (!okChartType) return false
            if (v.queryMode === 'sql') return typeof v.sql === 'string' && v.sql.length > 0
            const qs = v.querySpec || {}
            if (!qs || typeof qs !== 'object') return false
            if (!qs.source || typeof qs.source !== 'string') return false
            // Validate fields against schema
            const hasSeries = Array.isArray(qs.series) && qs.series.length > 0 && qs.series.every((s: any) => !!s && fieldExists(s.y))
            const hasXY = (!!qs.x && (typeof qs.y === 'string' || typeof qs.measure === 'string'))
            if (hasSeries) {
              // Optional series x fields
              const sxOk = qs.series.every((s: any) => !s.x || fieldExists(s.x))
              if (!sxOk) return false
            }
            if (hasXY) {
              const xOk = fieldExists(qs.x)
              const yOk = (typeof qs.y === 'string') ? fieldExists(qs.y) : true
              const mOk = (typeof qs.measure === 'string') ? fieldExists(qs.measure) : true
              if (!xOk || (!yOk && !mOk)) return false
            }
            // Legend can be string or array; if provided, ensure all exist
            const lg = (qs.legend as any)
            if (Array.isArray(lg)) { if (!lg.every((f) => fieldExists(f))) return false }
            else if (typeof lg === 'string') { if (!fieldExists(lg)) return false }
            return hasSeries || hasXY
          }
          if (t === 'table') {
            const qs = v.querySpec || {}
            if (!(qs && typeof qs.source === 'string' && qs.source.length > 0)) return false
            // If select exists, ensure fields exist
            if (Array.isArray((qs as any).select) && (qs as any).select.length > 0) {
              const ok = (qs as any).select.every((f: any) => fieldExists(f))
              if (!ok) return false
            }
            return true
          }
          if (t === 'kpi') {
            if (v.queryMode === 'sql') return typeof v.sql === 'string' && v.sql.length > 0
            const qs = v.querySpec || {}
            if (!(qs && typeof qs.source === 'string' && qs.source.length > 0 && (typeof qs.y === 'string' || typeof qs.measure === 'string'))) return false
            const yOk = (typeof qs.y === 'string') ? fieldExists(qs.y) : true
            const mOk = (typeof qs.measure === 'string') ? fieldExists(qs.measure) : true
            return yOk && mOk
          }
          return false
        } catch { return false }
      }
      const arr = rawArr.map(sanitize).filter(isValid)
      setVariantStats((prev) => ({
        received: prev.received + rawArr.length,
        kept: prev.kept + arr.length,
        dropped: prev.dropped + Math.max(0, rawArr.length - arr.length),
      }))
      if (arr.length === 0) { return }
      setVariants((prev) => ([...prev, ...arr]))
    } catch (e: any) { setError(e?.message || 'Suggest failed') }
    finally { setLoading((p) => ({ ...p, suggest: false })) }
  }

  const onPlan = async (promptText: string): Promise<string | null> => {
    try {
      if (!datasourceId || !source) throw new Error('Select datasource and table')
      const resp = await Api.aiPlan({
        provider: (env.aiProvider || 'gemini') as any,
        model: env.aiModel || 'gemini-1.5-flash',
        apiKey: env.aiApiKey || '',
        schema: { table: String(source), columns },
        samples,
        prompt: promptText,
        customColumns: customColNames,
        targetType: (widget?.type ? String(widget.type).toLowerCase() : (wantsChart ? 'chart' : undefined)) as any,
      })
      const plan = String(resp?.plan || '').trim()
      return plan || null
    } catch {
      return null
    }
  }

  const onEnhanceAndSuggest = async () => {
    const hasEnhanced = !!(enhancedPrompt || '').trim()
    const editedAfterEnhance = hasEnhanced && ((userPrompt || '').trim() !== (enhancedPrompt || '').trim())
    if (editedAfterEnhance) {
      const plan = await onPlan(userPrompt)
      await onSuggest(userPrompt, plan || undefined, true)
      return
    }
    const e = await onEnhance()
    const promptText = e ?? userPrompt
    const plan = await onPlan(promptText)
    await onSuggest(promptText, plan || undefined, true)
  }

  // Apply next caret position after text updates
  useEffect(() => {
    if (nextCaretRef.current != null && promptRef.current) {
      try {
        promptRef.current.selectionStart = nextCaretRef.current
        promptRef.current.selectionEnd = nextCaretRef.current
      } catch {}
      nextCaretRef.current = null
    }
  }, [userPrompt])

  // Helpers for IntelliSense
  const escapeHtml = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  const highlightedPromptHtml = useMemo(() => {
    try {
      const esc = escapeHtml(userPrompt || '')
      // Replace [Column] tokens with blue badges
      return esc.replace(/\[([^\]\n]{1,120})\]/g, (_m, p1) => `<span class=\"inline-block bg-blue-200 text-blue-900 rounded px-1 py-0.5\">${escapeHtml(p1)}</span>`)
    } catch { return escapeHtml(userPrompt || '') }
  }, [userPrompt])

  const updateHintStateWith = (value: string) => {
    try {
      const el = promptRef.current
      if (!el) { setHintOpen(false); return }
      const caret = el.selectionStart ?? value.length
      // Find last '[' before caret that is not closed by a ']' before caret
      const before = value.slice(0, caret)
      const lastOpen = before.lastIndexOf('[')
      const lastClose = before.lastIndexOf(']')
      if (lastOpen < 0 || lastClose > lastOpen) { setHintOpen(false); bracketStartRef.current = null; return }
      const query = before.slice(lastOpen + 1)
      bracketStartRef.current = lastOpen
      const qnorm = query.trim().toLowerCase()
      const names = (columns || []).map((c) => c.name).filter(Boolean) as string[]
      const filtered = names.filter((n) => String(n).toLowerCase().includes(qnorm))
      setHintCandidates(filtered.slice(0, 20))
      setHintIndex(0)
      setHintQuery(query)
      setHintOpen(true)
    } catch { setHintOpen(false); bracketStartRef.current = null }
  }

  const insertHint = (name: string) => {
    try {
      const el = promptRef.current
      if (!el) return
      const caret = el.selectionStart ?? (userPrompt || '').length
      const start = bracketStartRef.current ?? (caret - 1)
      const prev = userPrompt || ''
      const next = `${prev.slice(0, start)}[${name}]${prev.slice(caret)}`
      const newPos = start + name.length + 2
      nextCaretRef.current = newPos
      setUserPrompt(next)
      setHintOpen(false)
    } catch { /* noop */ }
  }

  // Infinite scroll: auto-suggest more when the sentinel enters view
  useEffect(() => {
    if (!open) return
    const root = scrollRef.current
    const target = loadMoreRef.current
    if (!root || !target) return

    const obs = new IntersectionObserver((entries) => {
      const hit = entries.some((en) => en.isIntersecting)
      if (!hit) return
      // Throttle and guard
      const now = Date.now()
      if (now - lastAutoSuggestAt.current < 1200) return
      if (disabled || loading.suggest || !source || !datasourceId) return
      if (variants.length === 0) return
      lastAutoSuggestAt.current = now
      onSuggest()
    }, { root, rootMargin: '200px 0px', threshold: 0.1 })

    obs.observe(target)
    return () => { try { obs.disconnect() } catch {} }
  }, [open, disabled, loading.suggest, source, datasourceId, variants.length])

  if (!open || typeof window === 'undefined') return null

  return createPortal(
    <div className="fixed inset-0 z-[1200]">
      <div className="absolute inset-0 bg-black/40" onClick={onCloseAction} />
      <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[920px] max-w-[95vw] max-h-[90vh] overflow-hidden rounded-lg border bg-card p-4">
        <div className="flex items-center justify-between mb-3">
          <div className="text-sm font-medium">AI Assist</div>
          <button className="text-xs px-2 py-1 rounded-md border hover:bg-[hsl(var(--secondary)/0.6)]" onClick={onCloseAction}>✕</button>
        </div>

        {!env.aiApiKey && (
          <div className="mb-3 text-xs text-amber-600">Add an API key in Environment → AI Features to enable calls. Defaults to Gemini Flash.</div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-3">
          <label className="text-sm block">Datasource
            <select className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={datasourceId || ''} onChange={(e)=> setDatasourceId(e.target.value || undefined)}>
              <option value="">Select datasource…</option>
              {/* Fetch datasources */}
              <DatasourceOptions />
            </select>
          </label>
          <label className="text-sm block md:col-span-2">
            <span className="inline-flex items-center gap-1">
              {datasourceId && loadingSchema && (
                <span className="inline-block h-3 w-3 rounded-full border-2 border-muted-foreground/50 border-t-transparent animate-spin" />
              )}
              <span>
                {`Table${datasourceId && !loadingSchema ? ` (${tables.length} tables)` : ''}`}
              </span>
            </span>
            <select className="mt-1 w-full px-2 py-1.5 rounded-md border bg-background" value={source || ''} onChange={(e)=> setSource(e.target.value || undefined)}>
              <option value="">Select table…</option>
              {tables.map((t) => (<option key={t.key} value={t.key}>{t.label}</option>))}
            </select>
          </label>
        </div>

        {/* Description Accordion */}
        <div className="mb-3">
          <div className={`w-full border rounded-xl ${loading.describe ? 'animate-pulse' : ''} bg-[hsl(var(--card))]`}> 
            <button type="button" className="w-full flex items-center justify-between px-3 py-2 rounded-xl" onClick={() => setDescOpen((v) => !v)} disabled={loading.describe}>
              <div className="text-sm font-medium">Data Description</div>
              <div className="flex items-center gap-2 text-xs text-muted-foreground min-w-0">
                {loading.describe ? (
                  <>
                    <span className="inline-block h-3 w-3 rounded-full border-2 border-amber-400 border-t-transparent animate-spin" />
                    <span>Describing ...</span>
                  </>
                ) : (
                  <>
                    {!descOpen && (
                      <span className="truncate overflow-hidden text-left max-w-[60vw]">{description ? description : '—'}</span>
                    )}
                    <span className="ml-2 text-[10px] shrink-0">Click to edit</span>
                  </>
                )}
              </div>
            </button>
            {loading.describe ? (
              <div className="px-3 pb-3">
                <div className="py-1 space-y-2">
                  <div className="h-3 w-11/12 bg-muted rounded animate-pulse" />
                  <div className="h-3 w-9/12 bg-muted rounded animate-pulse" />
                  <div className="h-3 w-7/12 bg-muted rounded animate-pulse" />
                </div>
              </div>
            ) : descOpen ? (
              <div className="px-3 pb-3">
                <textarea className="mt-1 w-full h-[120px] px-2 py-1.5 rounded-md border bg-background text-xs" value={description} onChange={(e)=> setDescription(e.target.value)} placeholder="Model will summarize the table here" />
              </div>
            ) : null}
          </div>
        </div>

        {/* Prompt + Suggest inline */}
        <div className="mb-3">
          <div className="relative">
            {/* Highlight overlay for inline badges */}
            <div className="pointer-events-none absolute inset-0 px-3 py-3 pr-32 rounded-xl whitespace-pre-wrap break-words text-sm" dangerouslySetInnerHTML={{ __html: highlightedPromptHtml || '&nbsp;' }} />
            <textarea
              ref={promptRef}
              className="w-full min-h-[120px] px-3 py-3 pr-32 rounded-xl border bg-transparent text-transparent text-sm"
              style={{ caretColor: 'hsl(var(--foreground))' }}
              value={userPrompt}
              onChange={(e)=> { setUserPrompt(e.target.value); updateHintStateWith(e.target.value) }}
              onKeyDown={(e) => {
                if (hintOpen && hintCandidates.length) {
                  if (e.key === 'ArrowDown') { e.preventDefault(); setHintIndex((i) => (i + 1) % hintCandidates.length); return }
                  if (e.key === 'ArrowUp') { e.preventDefault(); setHintIndex((i) => (i - 1 + hintCandidates.length) % hintCandidates.length); return }
                  if (e.key === 'Enter' || e.key === 'Tab') { e.preventDefault(); insertHint(hintCandidates[Math.max(0, Math.min(hintIndex, hintCandidates.length-1))]); return }
                  if (e.key === 'Escape') { e.preventDefault(); setHintOpen(false); return }
                }
              }}
              onKeyUp={() => updateHintStateWith(userPrompt)}
              onClick={() => updateHintStateWith(userPrompt)}
              placeholder="e.g., Create a bar chart for monthly orders. Type [ to insert a column name"
            />
            {/* Suggestions popover */}
            {hintOpen && hintCandidates.length > 0 && (
              <div className="absolute left-0 top-full mt-2 z-20 bg-card border rounded-md shadow p-2 w-full max-h-48 overflow-auto">
                <div className="flex flex-wrap gap-1">
                  {hintCandidates.map((name, idx) => (
                    <button
                      key={name+idx}
                      type="button"
                      className={`text-[11px] inline-flex items-center rounded px-1.5 py-0.5 ${idx===hintIndex? 'ring-2 ring-blue-400' : ''} bg-blue-200 text-blue-900`}
                      onMouseDown={(e) => { e.preventDefault(); insertHint(name) }}
                    >{name}</button>
                  ))}
                </div>
              </div>
            )}
            <div className="absolute right-2 top-2">
              <button
                className="group text-xs px-3 py-2 rounded-lg border hover:bg-muted flex items-center gap-1"
                onClick={() => {
                  if (loading.suggest) { try { suggestAbortRef.current?.abort() } catch {}; setLoading((p) => ({ ...p, suggest: false })); return }
                  onEnhanceAndSuggest()
                }}
                disabled={disabled || loading.enhance || !source || !datasourceId}
                title={loading.suggest ? 'Stop' : 'Suggest'}
              >
                {loading.suggest ? (
                  <span className="relative inline-block w-4 h-4">
                    <span className="absolute inset-0 inline-block h-3 w-3 rounded-full border-2 border-muted-foreground/50 border-t-transparent animate-spin group-hover:hidden" />
                    <RiStopFill className="hidden group-hover:inline-block text-rose-500 w-4 h-4" />
                  </span>
                ) : (
                  <RiSparkling2Fill className="text-amber-400" />
                )}
                <span>Suggest</span>
              </button>
            </div>
          </div>
        </div>

        {error && <div className="mt-2 text-xs text-rose-600">{error}</div>}
        <div className="mt-2 text-[11px] text-muted-foreground">
          <span className="mr-3">Variants kept: <span className="font-medium text-foreground">{variantStats.kept}</span></span>
          <span className="mr-3">Received: <span className="font-medium text-foreground">{variantStats.received}</span></span>
          <span>Filtered: <span className="font-medium text-foreground">{variantStats.dropped}</span></span>
        </div>

        <div ref={scrollRef} className="mt-3 max-h-[50vh] overflow-y-auto pr-1">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 min-h-[120px]">
            {variants.map((v, i) => (
              <div key={i} className="border rounded-md p-2 bg-[hsl(var(--secondary)/0.3)]">
                <div className="text-xs font-semibold mb-1">Variant {i+1}</div>
                <div className="rounded-md border bg-[hsl(var(--card))]">
                  <LazyVariantPreview cfg={v as any} datasourceId={datasourceId} source={source} idx={i} open={open} rootRef={scrollRef} />
                </div>
                <div className="mt-2 text-[11px] text-muted-foreground">
                  <VariantSummary cfg={v as any} />
                </div>
                <div className="mt-2 flex items-center gap-2">
                  <button className="text-[11px] px-2 py-1 rounded-md border hover:bg-muted" onClick={() => { if (!widget) return; onApplyAction({ ...(widget as any), ...(v as any), id: widget.id, datasourceId: datasourceId, querySpec: { ...(v as any)?.querySpec, source } } as any); onCloseAction() }}>Use this</button>
                </div>
              </div>
            ))}
          </div>
          <div ref={loadMoreRef} className="h-8 flex items-center justify-center">
            {loading.suggest && (
              <span className="inline-block h-4 w-4 rounded-full border-2 border-muted-foreground/50 border-t-transparent animate-spin" />
            )}
          </div>
        </div>
      </div>
    </div>,
    document.body
  )
}

function DatasourceOptions() {
  const { user } = useAuth()
  const [items, setItems] = useState<Array<{ id: string; name: string }>>([])
  useEffect(() => { (async () => {
    try {
      const arr = await Api.listDatasources(undefined, user?.id)
      setItems((arr || []).map((d: any) => ({ id: d.id, name: d.name })))
    } catch { setItems([]) }
  })() }, [user?.id])
  return (
    <>
      {items.map((d) => (<option key={d.id} value={d.id}>{d.name}</option>))}
    </>
  )
}

function LazyVariantPreview({ cfg, datasourceId, source, idx, open, rootRef }: { cfg: any; datasourceId?: string; source?: string; idx: number; open: boolean; rootRef: React.RefObject<HTMLDivElement | null> }) {
  const holderRef = useRef<HTMLDivElement | null>(null)
  const [visible, setVisible] = useState(false)
  useEffect(() => {
    if (!open) { setVisible(false); return }
    const root = rootRef?.current || null
    const el = holderRef.current
    if (!el) return
    let raf = 0
    const obs = new IntersectionObserver((entries) => {
      const hit = entries.some((en) => en.isIntersecting)
      if (!hit) return
      // Stagger mount to avoid blocking main thread
      raf = window.requestAnimationFrame(() => {
        setTimeout(() => setVisible(true), 0)
      })
      try { obs.disconnect() } catch {}
    }, { root, rootMargin: '80px 0px', threshold: 0.01 })
    obs.observe(el)
    return () => { if (raf) cancelAnimationFrame(raf); try { obs.disconnect() } catch {} }
  }, [open, rootRef?.current])
  return (
    <div ref={holderRef} className="min-h-[220px]">
      {visible ? (
        <VariantPreview cfg={cfg} datasourceId={datasourceId} source={source} idx={idx} />
      ) : (
        <div className="h-[220px] animate-pulse bg-[hsl(var(--muted)/0.3)] rounded-md" />
      )}
    </div>
  )
}

function VariantPreview({ cfg, datasourceId, source, idx }: { cfg: any; datasourceId?: string; source?: string; idx: number }) {
  try {
    const type = String((cfg?.type || (cfg?.chartType ? 'chart' : 'chart'))).toLowerCase()
    const title = String(cfg?.title || 'Preview')
    const widgetId = `ai_prev_${idx}`
    const queryMode = (cfg?.queryMode === 'spec' || cfg?.querySpec) ? 'spec' : 'sql'
    const querySpec = queryMode === 'spec' ? ({ ...(cfg?.querySpec || {}), source } as any) : undefined
    const sql = queryMode === 'sql' ? String(cfg?.sql || '') : ''
    const options = (cfg?.options || {}) as any
    const chartType = (cfg?.chartType || 'line') as any
    const customColumns = (cfg?.customColumns || undefined) as any
    if (type === 'kpi') {
      return (
        <div className="p-2">
          <KpiCard
            title={title}
            sql={sql}
            datasourceId={datasourceId}
            queryMode={queryMode as any}
            querySpec={querySpec}
            options={options}
            widgetId={widgetId}
          />
        </div>
      )
    }
    if (type === 'table') {
      return (
        <div className="p-2">
          <TableCard
            title={title}
            sql={sql}
            datasourceId={datasourceId}
            queryMode={queryMode as any}
            querySpec={querySpec}
            options={options}
            widgetId={widgetId}
            customColumns={customColumns}
            pageSize={5}
          />
        </div>
      )
    }
    // chart (default)
    return (
      <div className="p-2">
        <div className="relative h-[220px] pointer-events-none">
          <ChartCard
            title={title}
            sql={sql}
            datasourceId={datasourceId}
            type={chartType}
            options={{ ...(options || {}), showLegend: false, legendPosition: 'none' as any }}
            queryMode={queryMode as any}
            querySpec={querySpec}
            customColumns={customColumns}
            widgetId={widgetId}
            reservedTop={0}
            layout="flex"
          />
        </div>
      </div>
    )
  } catch {
    return <div className="text-[11px] text-muted-foreground p-2">Preview unavailable</div>
  }
}

function VariantSummary({ cfg }: { cfg: any }) {
  try {
    const chartType = String((cfg?.chartType || cfg?.type || 'chart')).toLowerCase()
    const typeLabelMap: Record<string, string> = {
      line: 'Line Chart', bar: 'Bar Chart', area: 'Area Chart', column: 'Column Chart',
      donut: 'Donut Chart', categorybar: 'Bar Chart', spark: 'Sparkline', combo: 'Combo Chart',
      scatter: 'Scatter Plot', tremortable: 'Table', heatmap: 'Heatmap', barlist: 'Bar List', gantt: 'Gantt Chart',
      chart: 'Chart', table: 'Table', kpi: 'KPI'
    }
    const typeLabel = typeLabelMap[chartType.replace(/\s+/g,'')] || 'Chart'
    const qs = (cfg?.querySpec || {}) as any
    const series = Array.isArray(qs.series) ? qs.series : []
    const primary = series.length ? series[0] : null
    const agg = String((primary?.agg || qs.agg || 'count')).toLowerCase()
    const aggLabel = agg === 'count' ? 'Count' : agg === 'sum' ? 'Sum' : agg === 'avg' ? 'Average' : agg === 'min' ? 'Min' : agg === 'max' ? 'Max' : agg === 'distinct' ? 'Distinct Count' : 'Value'
    const yField = primary?.y || qs.y || qs.measure
    const xField = qs.x || primary?.x || qs.legend
    const unit = String((qs.groupBy || (cfg?.options?.xTimeUnit))) || ''

    const Badge = ({ name }: { name?: string }) => {
      const n = (name || '').toString().replace(/^\[(.*)\]$/, '$1')
      if (!n) return null
      return <span className="inline-block bg-blue-200 text-blue-900 rounded px-1 py-0.5">{n}</span>
    }

    return (
      <div className="flex flex-wrap items-center gap-x-1 gap-y-1">
        <span className="text-foreground/90 font-medium">{typeLabel}</span>
        <span> - </span>
        <span>{aggLabel}</span>
        <span>of</span>
        <Badge name={yField} />
        {xField ? (<>
          <span>per</span>
          <Badge name={xField} />
        </>) : null}
        {(unit && typeof unit === 'string') ? (<>
          <span>grouped by</span>
          <span className="capitalize">{unit}</span>
        </>) : null}
      </div>
    )
  } catch {
    return null
  }
}
