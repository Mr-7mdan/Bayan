// ponytail: dev-only visibility for deliberately swallowed errors; upgrade to reporting if needed
export function swallow(err: unknown, ctx: string) {
  if (process.env.NODE_ENV !== 'production') console.warn(`[swallowed:${ctx}]`, err)
}
