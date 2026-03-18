/**
 * Composable date preset types and constants.
 * Single source of truth for the frontend.
 */

import { useState, useEffect, useRef } from "react";
import { Api } from "./api";

// ── Dimension types ──────────────────────────────────────────────────────

export type Period = "day" | "week" | "month" | "quarter" | "year";
export type OffsetType = "this" | "previous";
export type AsOfType = "today" | "last_working_day";
export type RangeModeType = "full" | "to_date" | "end_of_period";

export interface PresetConfig {
  period: Period;
  offset: OffsetType;
  as_of: AsOfType;
  range_mode: RangeModeType;
  include_weekends: boolean;
  apply_holidays: boolean;
}

export const DEFAULT_PRESET: PresetConfig = {
  period: "week",
  offset: "this",
  as_of: "today",
  range_mode: "full",
  include_weekends: true,
  apply_holidays: false,
};

// ── Dimension labels (for UI dropdowns) ──────────────────────────────────

export const PERIOD_OPTIONS: { value: Period; label: string }[] = [
  { value: "day", label: "Day" },
  { value: "week", label: "Week" },
  { value: "month", label: "Month" },
  { value: "quarter", label: "Quarter" },
  { value: "year", label: "Year" },
];

export const OFFSET_OPTIONS: { value: OffsetType; label: string }[] = [
  { value: "this", label: "This / Current" },
  { value: "previous", label: "Previous" },
];

export const AS_OF_OPTIONS: { value: AsOfType; label: string }[] = [
  { value: "today", label: "Today (calendar)" },
  { value: "last_working_day", label: "Last Working Day" },
];

export const RANGE_MODE_OPTIONS: { value: RangeModeType; label: string }[] = [
  { value: "full", label: "Full Period" },
  { value: "to_date", label: "To Date" },
  { value: "end_of_period", label: "End of Period (single day)" },
];

// ── Quick Picks ──────────────────────────────────────────────────────────

export interface QuickPick {
  label: string;
  group: string;
  config: PresetConfig;
}

export const QUICK_PICKS: QuickPick[] = [
  // Days
  { label: "Today", group: "Days", config: { period: "day", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "Yesterday", group: "Days", config: { period: "day", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "Last Working Day", group: "Days", config: { period: "day", offset: "this", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false } },
  { label: "Day Before Last Working Day", group: "Days", config: { period: "day", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false } },
  // Weeks
  { label: "This Week", group: "Weeks", config: { period: "week", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "This Week to Date", group: "Weeks", config: { period: "week", offset: "this", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false } },
  { label: "This Working Week to Date", group: "Weeks", config: { period: "week", offset: "this", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false } },
  { label: "End of This Week", group: "Weeks", config: { period: "week", offset: "this", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false } },
  { label: "End of This Working Week", group: "Weeks", config: { period: "week", offset: "this", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false } },
  { label: "Previous Week", group: "Weeks", config: { period: "week", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "Previous Week to Date", group: "Weeks", config: { period: "week", offset: "previous", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false } },
  { label: "Previous Working Week", group: "Weeks", config: { period: "week", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false } },
  { label: "Previous Working Week to Date", group: "Weeks", config: { period: "week", offset: "previous", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false } },
  { label: "End of Previous Week", group: "Weeks", config: { period: "week", offset: "previous", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false } },
  { label: "End of Previous Working Week", group: "Weeks", config: { period: "week", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false } },
  // Months
  { label: "This Month", group: "Months", config: { period: "month", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "Month to Date", group: "Months", config: { period: "month", offset: "this", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false } },
  { label: "Working Month to Date", group: "Months", config: { period: "month", offset: "this", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false } },
  { label: "End of This Month", group: "Months", config: { period: "month", offset: "this", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false } },
  { label: "End of This Working Month", group: "Months", config: { period: "month", offset: "this", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false } },
  { label: "Previous Month", group: "Months", config: { period: "month", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "Previous Month to Date", group: "Months", config: { period: "month", offset: "previous", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false } },
  { label: "Previous Working Month", group: "Months", config: { period: "month", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false } },
  { label: "Previous Working Month to Date", group: "Months", config: { period: "month", offset: "previous", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false } },
  { label: "End of Previous Month", group: "Months", config: { period: "month", offset: "previous", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false } },
  { label: "End of Previous Working Month", group: "Months", config: { period: "month", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false } },
  // Quarters
  { label: "This Quarter", group: "Quarters", config: { period: "quarter", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "Quarter to Date", group: "Quarters", config: { period: "quarter", offset: "this", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false } },
  { label: "Working Quarter to Date", group: "Quarters", config: { period: "quarter", offset: "this", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false } },
  { label: "End of This Quarter", group: "Quarters", config: { period: "quarter", offset: "this", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false } },
  { label: "End of This Working Quarter", group: "Quarters", config: { period: "quarter", offset: "this", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false } },
  { label: "Previous Quarter", group: "Quarters", config: { period: "quarter", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "Previous Quarter to Date", group: "Quarters", config: { period: "quarter", offset: "previous", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false } },
  { label: "Previous Working Quarter", group: "Quarters", config: { period: "quarter", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false } },
  { label: "Previous Working Quarter to Date", group: "Quarters", config: { period: "quarter", offset: "previous", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false } },
  { label: "End of Previous Quarter", group: "Quarters", config: { period: "quarter", offset: "previous", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false } },
  { label: "End of Previous Working Quarter", group: "Quarters", config: { period: "quarter", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false } },
  // Years
  { label: "This Year", group: "Years", config: { period: "year", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "Year to Date", group: "Years", config: { period: "year", offset: "this", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false } },
  { label: "Working Year to Date", group: "Years", config: { period: "year", offset: "this", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false } },
  { label: "End of This Year", group: "Years", config: { period: "year", offset: "this", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false } },
  { label: "End of This Working Year", group: "Years", config: { period: "year", offset: "this", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false } },
  { label: "Previous Year", group: "Years", config: { period: "year", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false } },
  { label: "Previous Year to Date", group: "Years", config: { period: "year", offset: "previous", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false } },
  { label: "Previous Working Year", group: "Years", config: { period: "year", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false } },
  { label: "Previous Working Year to Date", group: "Years", config: { period: "year", offset: "previous", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false } },
  { label: "End of Previous Year", group: "Years", config: { period: "year", offset: "previous", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false } },
  { label: "End of Previous Working Year", group: "Years", config: { period: "year", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false } },
];

// ── Legacy preset mapping ────────────────────────────────────────────────

/** Legacy preset string -> PresetConfig mapping for frontend detection */
export const LEGACY_PRESET_MAP: Record<string, PresetConfig> = {
  today:                          { period: "day", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  yesterday:                      { period: "day", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  last_working_day:               { period: "day", offset: "this", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false },
  day_before_last_working_day:    { period: "day", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false },
  day_before_yesterday:           { period: "day", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  twwtlwd:                        { period: "week", offset: "this", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false },
  last_working_week:              { period: "week", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false },
  week_before_last_working_week:  { period: "week", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false },
  lwwtlwd:                        { period: "week", offset: "previous", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false },
  this_week:                      { period: "week", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  last_week:                      { period: "week", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  week_before_last:               { period: "week", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  this_month:                     { period: "month", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  tmtlwd:                         { period: "month", offset: "this", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false },
  last_month:                     { period: "month", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  last_working_month:             { period: "month", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false },
  month_before_last_working_month:{ period: "month", offset: "previous", as_of: "last_working_day", range_mode: "full", include_weekends: false, apply_holidays: false },
  lwmtlwd:                        { period: "month", offset: "previous", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false },
  ytlwd:                          { period: "year", offset: "this", as_of: "last_working_day", range_mode: "to_date", include_weekends: false, apply_holidays: false },
  ytd:                            { period: "year", offset: "this", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false },
  mtd:                            { period: "month", offset: "this", as_of: "today", range_mode: "to_date", include_weekends: true, apply_holidays: false },
  this_quarter:                   { period: "quarter", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  last_quarter:                   { period: "quarter", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  this_year:                      { period: "year", offset: "this", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  last_year:                      { period: "year", offset: "previous", as_of: "today", range_mode: "full", include_weekends: true, apply_holidays: false },
  eof_last_working_week:          { period: "week", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false },
  eof_week_before_last_working_week: { period: "week", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false },
  eof_lwwtlwd:                    { period: "week", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false },
  eof_this_week:                  { period: "week", offset: "this", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false },
  eof_last_week:                  { period: "week", offset: "previous", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false },
  eof_this_month:                 { period: "month", offset: "this", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false },
  eof_last_month:                 { period: "month", offset: "previous", as_of: "today", range_mode: "end_of_period", include_weekends: true, apply_holidays: false },
  eof_last_working_month:         { period: "month", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false },
  eof_month_before_last_working_month: { period: "month", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false },
  eof_lwmtlwd:                    { period: "month", offset: "previous", as_of: "last_working_day", range_mode: "end_of_period", include_weekends: false, apply_holidays: false },
};

/**
 * Legacy "last N days" presets sent by DataTabHelpers.
 * These are relative-day ranges (today - N … tomorrow) and don't map cleanly
 * to the composable PresetConfig model. The backend handles them via a regex
 * pattern match (`last_(\d+)_days`).
 *
 * Listed here so the frontend can recognize them and display a label.
 */
export const LAST_N_DAYS_PRESETS: Record<string, { days: number; label: string }> = {
  last_7_days:  { days: 7,  label: "Last 7 Days" },
  last_30_days: { days: 30, label: "Last 30 Days" },
  last_90_days: { days: 90, label: "Last 90 Days" },
};

// ── Utility functions ────────────────────────────────────────────────────

/**
 * Detect if a __date_preset value is a legacy string and convert to PresetConfig.
 * Returns null if the string is not recognized.
 *
 * Note: "last_N_days" presets (e.g. last_7_days) are not convertible to
 * PresetConfig — they are handled directly by the backend. Use
 * {@link isLastNDaysPreset} to check for them separately.
 */
export function parseLegacyPreset(value: string | PresetConfig): PresetConfig | null {
  if (typeof value === "string") {
    return LEGACY_PRESET_MAP[value.toLowerCase().trim()] ?? null;
  }
  return null;
}

/**
 * Check if a legacy preset string is a "last N days" type.
 * Returns the matching entry from LAST_N_DAYS_PRESETS or null.
 */
export function isLastNDaysPreset(value: string): { days: number; label: string } | null {
  const key = value.toLowerCase().trim();
  return LAST_N_DAYS_PRESETS[key] ?? null;
}

/**
 * Find matching quick pick for a PresetConfig, or null if custom.
 */
export function matchQuickPick(config: PresetConfig): QuickPick | null {
  return (
    QUICK_PICKS.find(
      (qp) =>
        qp.config.period === config.period &&
        qp.config.offset === config.offset &&
        qp.config.as_of === config.as_of &&
        qp.config.range_mode === config.range_mode &&
        qp.config.include_weekends === config.include_weekends,
    ) ?? null
  );
}

/**
 * Generate a human-readable label from a PresetConfig.
 * Used when displaying structured presets in read-only contexts (e.g. ReportCard).
 */
export function presetConfigToLabel(config: PresetConfig): string {
  const match = matchQuickPick(config);
  if (match) return match.label;

  // Build a descriptive label from dimensions
  const parts: string[] = [];

  if (config.range_mode === "end_of_period") parts.push("End of");
  parts.push(config.offset === "this" ? "This" : "Previous");

  if (config.as_of === "last_working_day" && !config.include_weekends) {
    parts.push("Working");
  }

  const periodLabel: Record<Period, string> = {
    day: "Day",
    week: "Week",
    month: "Month",
    quarter: "Quarter",
    year: "Year",
  };
  parts.push(periodLabel[config.period]);

  if (config.range_mode === "to_date") parts.push("to Date");
  if (config.apply_holidays) parts.push("(excl. holidays)");

  return parts.join(" ");
}

// ── Preview hook ─────────────────────────────────────────────────────────

export interface PresetPreview {
  gte: string;
  lt: string;
  label: string;
  loading: boolean;
}

/**
 * React hook that debounces a PresetConfig and calls POST /date-presets/preview.
 * Returns { gte, lt, label, loading }.
 */
export function usePresetPreview(config: PresetConfig | null): PresetPreview {
  const [result, setResult] = useState<Omit<PresetPreview, "loading">>({ gte: "", lt: "", label: "" });
  const [loading, setLoading] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (!config) {
      setResult({ gte: "", lt: "", label: "" });
      setLoading(false);
      return;
    }

    setLoading(true);

    // Clear previous debounce timer
    if (timerRef.current) clearTimeout(timerRef.current);

    timerRef.current = setTimeout(() => {
      // Abort any in-flight request
      if (abortRef.current) abortRef.current.abort();
      abortRef.current = new AbortController();

      Api.previewDatePreset({
        period: config.period,
        offset: config.offset,
        as_of: config.as_of,
        range_mode: config.range_mode,
        include_weekends: config.include_weekends,
        apply_holidays: config.apply_holidays,
      })
        .then((res) => {
          setResult({ gte: res.gte, lt: res.lt, label: res.label });
          setLoading(false);
        })
        .catch((err) => {
          if (err?.name !== "AbortError") setLoading(false);
        });
    }, 300);

    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
      if (abortRef.current) abortRef.current.abort();
    };
  }, [
    config?.period,
    config?.offset,
    config?.as_of,
    config?.range_mode,
    config?.include_weekends,
    config?.apply_holidays,
  ]);

  return { ...result, loading };
}
