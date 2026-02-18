// Shared DSL types for datasource-level transforms
export type Value = string | number | boolean | null

export type Condition = {
  op: 'eq'|'ne'|'gt'|'gte'|'lt'|'lte'|'in'|'like'|'regex'
  left: string // column reference
  right?: Value | Value[]
}

export type Expr = string // expression string; UI may use a formula DSL; server can translate to SQL

export type Scope = { level: 'widget'|'table'|'datasource'; widgetId?: string; table?: string }

export type CustomColumn = {
  name: string
  expr: Expr
  type?: 'string'|'number'|'date'|'boolean'
  scope?: Scope
}

export type Transform = (
  | { type: 'case'; target: string; cases: { when: Condition; then: Value }[]; else?: Value }
  | { type: 'replace'; target: string; search: string | string[]; replace: string | string[] }
  | { type: 'translate'; target: string; search: string; replace: string }
  | { type: 'nullHandling'; target: string; mode: 'coalesce'|'isnull'|'ifnull'; value: Value }
  | { type: 'computed'; name: string; expr: Expr; valueType?: 'string'|'number'|'date'|'boolean' }
  | { type: 'unpivot'; sourceColumns: string[]; keyColumn: string; valueColumn: string; mode?: 'auto'|'unpivot'|'union'; omitZeroNull?: boolean }
) & { scope?: Scope }

export type JoinSpec = {
  joinType: 'left'|'inner'|'right'|'lateral'
  targetTable: string
  sourceKey: string
  targetKey: string
  columns?: Array<{ name: string; alias?: string }>
  aggregate?: { fn: 'sum'|'avg'|'min'|'max'|'count'|'string_agg'|'array_agg'; column: string; alias: string }
  filter?: Condition
  scope?: Scope
  lateral?: {
    correlations: Array<{ sourceCol: string; op: 'eq'|'ne'|'gt'|'gte'|'lt'|'lte'; targetCol: string }>
    orderBy?: Array<{ column: string; direction: 'ASC'|'DESC' }>
    limit?: number
    subqueryAlias?: string
  }
}

export type SortSpec = { by: string; direction: 'asc'|'desc'; semantic?: 'numeric'|'alpha'|'date'|'latest_to_oldest'|'oldest_to_latest' }
export type TopNSpec = { n: number; by: string; direction: 'asc'|'desc'; scope?: 'pre-agg'|'post-agg' }

export type DatasourceTransforms = {
  customColumns: CustomColumn[]
  transforms: Transform[]
  joins: JoinSpec[]
  defaults?: { sort?: SortSpec; limitTopN?: TopNSpec }
}

export type PreviewResponse = { sql?: string; columns?: string[]; rows?: any[]; warnings?: string[] }
