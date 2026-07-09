// Typed shapes for the alert dialog. The saved AlertConfig JSON is loosely
// typed (`Record<string, any>` in lib/api.ts), so these narrow the shapes the
// dialog reads/writes without touching the wire format.
import type { WidgetConfig } from '@/types/widgets'

// Callers (WidgetActionsMenu, dashboard builder) attach `dashboardId` to a
// widget config ad hoc; WidgetConfig itself does not declare it.
export type CarriedWidget = WidgetConfig & { dashboardId?: string }

export type XPick = 'custom' | 'range' | 'today' | 'yesterday' | 'this_month' | 'last' | 'min' | 'max'
export type AggKind = 'count' | 'sum' | 'avg' | 'min' | 'max' | 'distinct'

export type TimeTrigger = { type: 'time'; cron: string }
export type ThresholdTrigger = {
  type: 'threshold'; source: string; aggregator: AggKind; measure?: string; y?: string
  where?: Record<string, unknown>; xField?: string; xValue?: string | number
  legendField?: string; legendFields?: string[]; rowFields?: string[]
  xMode?: 'custom' | 'token' | 'special' | 'range'; xToken?: string; xSpecial?: string
  xRange?: { from?: string; to?: string }
  operator: string; value: number | number[]; calcMode?: 'query' | 'pivot'
}

export type EmailAction = { type: 'email'; to: string[]; subject?: string; attachPdf?: boolean; pdfLandscape?: boolean }
export type SmsAction = { type: 'sms'; to: string[]; message?: string }
export type AlertAction = EmailAction | SmsAction

export type RenderConfig = {
  mode: 'kpi' | 'table' | 'chart' | 'report'; label?: string; querySpec?: Record<string, unknown>
  width?: number; height?: number; widgetRef?: { dashboardId: string; widgetId: string }
}

export type TriggersGroup = {
  logic: 'AND' | 'OR'
  time: { enabled: boolean; time?: string; schedule: { kind: 'hourly' | 'weekly' | 'monthly'; everyHours?: number; dows?: number[]; doms?: number[] } }
  threshold: ({ enabled: true } & Omit<ThresholdTrigger, 'type'>) | { enabled: false }
}

export type RecipientToken = { kind: 'contact' | 'email' | 'phone' | 'tag'; label: string; value: string; email?: string; phone?: string; id?: string; name?: string; tag?: string }
