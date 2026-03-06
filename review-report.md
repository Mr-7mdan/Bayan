Application Review: Weaknesses, Performance Bottlenecks & Enhancements
🔴 Critical Performance Bottlenecks
1. Widget Query Concurrency Too Restrictive
Location:  frontend/src/lib/api.ts 
// typescript
const MAX_WIDGET_CONCURRENCY = Number(process.env.NEXT_PUBLIC_WIDGET_CONCURRENCY || 3)
- Only 3 concurrent widget queries allowed - causes UI blocking on dashboards with many widgets
- Queue-based but no priority for visible widgets
2. Frontend Cache TTL Excessively Short
// typescript
const GET_CACHE_MS = (() => { 
  const v = Number(process.env.NEXT_PUBLIC_GET_CACHE_MS || 3000); 
  return Number.isFinite(v) ? Math.max(0, v) : 3000 
})()
- Default 3-second cache causes redundant API calls
- Metadata endpoints (counts, notifications) have slightly longer TTL but still too short
3. Backend Query Spec Concurrency Limiter
Location:  backend/app/routers/query.py 
// python
_SPEC_LIMIT = 4
try:
    _SPEC_LIMIT = int(os.environ.get("SPEC_QUERY_CONCURRENCY", "7") or "7")
except Exception:
    _SPEC_LIMIT = 4
- Max 4-7 concurrent spec queries - insufficient for dashboards firing many queries simultaneously
- Causes thread-pool starvation
4. Alert Snapshot Generation is Blocking
Location:  backend/app/alerts_service.py 
- Playwright-based PNG generation waits 4-20+ seconds per widget
- Runs synchronously in alert execution thread
- No async queue for snapshot generation
────────────────────────────────────────────────────────────────────────────
🟠 Architecture & Design Weaknesses
5. Dual SQL Generation Systems
- Both  sqlgen.py  (1700+ lines) and  sqlgen_glot.py  (2000+ lines) run side-by-side
- Massive code duplication and maintenance burden
- Complex regex-based string manipulation prone to edge-case bugs
6. In-Memory State Only
Location:  backend/app/metrics_state.py 
- No Redis or distributed cache
-  _recent_actors ,  _open_builder  are process-local
- Lost on restart, doesn't scale horizontally
7. No Query Result Caching
- Repeated identical queries hit the database every time
- No materialized query cache for dashboard refreshes
8. Deterministic Password Hashing
Location:  backend/app/security.py 
// python
def hash_password(password: str) -> str:
    """NOTE: For production systems, use a dedicated password hashing algorithm
    like argon2 or bcrypt with per-user salts..."""
    digest = hashlib.sha256((settings.secret_key + ":" + (password or "")).encode("utf-8")).hexdigest()
- Uses SHA256 without salt - vulnerable to rainbow tables
- Acknowledged as "demo only" but still in production code
────────────────────────────────────────────────────────────────────────────
🟡 Code Quality Issues
9. Debug Print Statements in Production
- Hundreds of  print()  statements in Python files (e.g.,  sqlgen.py ,  sqlgen_glot.py )
- Should use proper logging with levels
10. Type Safety Gaps
- TypeScript uses  any  extensively in API layer
- No runtime validation of API responses
11. Large Monolithic Files
-  sqlgen.py  and  sqlgen_glot.py  are 1700-2000+ lines each
- Hard to maintain and test
12. No Database Migrations
- No visible Alembic or SQLAlchemy migration system
- Schema changes require manual intervention
────────────────────────────────────────────────────────────────────────────
🟢 Recommended Enhancements
Priority 1: Performance Fixes
1. Increase widget concurrency:  MAX_WIDGET_CONCURRENCY  → 8-10
2. Extend cache TTLs:
- Metadata: 30-60 seconds
- Dashboard definitions: 5-10 minutes
3. Add query result caching: Redis-based cache for repeated queries with configurable TTL
Priority 2: Architecture Improvements
4. Deprecate legacy SQL generator: Choose one path (SQLGlot preferred)
5. Add Redis for distributed state: Cache, sessions, metrics
6. Implement async alert queue: Use Celery or FastAPI BackgroundTasks
Priority 3: Security & Reliability
7. Replace password hashing: Use bcrypt or argon2 with per-user salts
8. Add rate limiting: Use FastAPI's built-in rate limiter
9. Add observability: Integrate Sentry or similar
10. Implement database migrations: Add Alembic
──────────────────────────────────