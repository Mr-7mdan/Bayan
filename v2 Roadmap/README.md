# Bayan v2 Roadmap

Corporate-readiness roadmap derived from a full code review of the Bayan platform (FastAPI backend + Next.js frontend, SQLite metadata + DuckDB analytics). Each spec under this folder is self-contained and written so an LLM subagent (or engineer) can implement it directly: verified `file:line` references, ordered steps, acceptance criteria, and end-to-end verification.

Read `SPEC_TEMPLATE.md` for the structure every spec follows.

---

## ⚠️ URGENT — do before anything else (do NOT wait for v2)

`backend/.env` is committed to git with **live** secrets. Rotate now; the roadmap only fixes the process afterward.

1. **Revoke the GitHub PAT** in `backend/.env` (GitHub → Settings → Developer settings → revoke).
2. **Rotate `SECRET_KEY`** — it derives the Fernet key encrypting every stored datasource credential. Rotating it invalidates all encrypted DSNs; use `backend/migrate_secret_key.py` to re-encrypt, or plan to re-enter datasource credentials.
3. **Change `ADMIN_PASSWORD`** and any DB passwords that were exposed.
4. Then execute **spec 01** (purge from history + fix `.gitignore`).

Full procedure and consequences: `P0-security/01-secret-rotation-and-history-purge.md`.

---

## Spec index

| ID | Spec | Priority | Effort | Depends on | Area |
|----|------|----------|--------|-----------|------|
| 01 | [Rotate leaked secrets & purge git history](P0-security/01-secret-rotation-and-history-purge.md) | P0 | M | — | ops |
| 02 | [Token/session authentication](P0-security/02-authentication-tokens-and-sessions.md) | P0 | L | — | fullstack |
| 03 | [SQL injection hardening](P0-security/03-sql-injection-hardening.md) | P0 | L | 02 | backend |
| 04 | [RBAC & per-resource authorization](P0-security/04-rbac-and-authorization.md) | P0 | L | 02 | backend |
| 05 | [Audit logging](P0-security/05-audit-logging.md) | P0 | M | 02 | backend |
| 06 | [Alembic migrations](P1-backend/06-alembic-migrations.md) | P1 | M | — | backend |
| 07 | [Structured logging](P1-backend/07-structured-logging.md) | P1 | M | — | backend |
| 08 | [Metrics & observability](P1-backend/08-metrics-and-observability.md) | P1 | M | — | backend |
| 09 | [DuckDB pool hardening](P1-backend/09-duckdb-pool-hardening.md) | P1 | M | — | backend |
| 10 | [Sync pipeline reliability](P1-backend/10-sync-pipeline-reliability.md) | P1 | M | — | backend |
| 11 | [Query engine decomposition + SQLGlot](P1-backend/11-query-engine-refactor-sqlglot.md) | P1 | XL | 03 | backend |
| 12 | [Rate limiting defaults](P1-backend/12-rate-limiting-defaults.md) | P1 | S | — | backend |
| 13 | [ChartCard decomposition](P1-frontend/13-chartcard-decomposition.md) | P1 | XL | — | frontend |
| 14 | [AlertDialog state refactor](P1-frontend/14-alertdialog-decomposition.md) | P1 | L | — | frontend |
| 15 | [Theme token consistency + alpha themes](P1-frontend/15-theme-token-consistency.md) | P1 | L | — | frontend |
| 16 | [Route states & error boundaries](P1-frontend/16-route-states-and-error-boundaries.md) | P1 | M | — | frontend |
| 17 | [i18n & RTL](P1-frontend/17-i18n-and-rtl.md) | P1 | L | — | frontend |
| 18 | [Accessibility (WCAG 2.1 AA)](P2-ux/18-accessibility-wcag.md) | P2 | L | — | frontend |
| 19 | [Responsive dashboard grid](P2-ux/19-responsive-dashboard-grid.md) | P2 | M | — | frontend |
| 20 | [List virtualization & memoization](P2-ux/20-list-virtualization-and-memoization.md) | P2 | M | — | frontend |
| 21 | [Bundle size reduction](P2-ux/21-bundle-size-reduction.md) | P2 | M | 13 | frontend |
| 22 | [CI/CD pipeline](P2-ops/22-ci-cd-pipeline.md) | P2 | M | — | ops |
| 23 | [Docker deployment](P2-ops/23-docker-deployment.md) | P2 | M | — | ops |
| 24 | [Backup & dashboard versioning](P2-ops/24-backup-and-dashboard-versioning.md) | P2 | L | 06 | fullstack |
| 25 | [Test coverage foundation](P2-ops/25-test-coverage-foundation.md) | P2 | L | 22 | fullstack |

Effort: S <1d · M 1–3d · L 3–7d · XL >1wk.

---

## Suggested implementation order

Ordered so dependencies land first and each wave is safely shippable.

**Wave 0 — emergency (hours):** manual secret rotation (see above), then **01**.

**Wave 1 — security foundation (P0):** **02** → then **03**, **04**, **05** in parallel (all build on 02). Nothing else ships to corporate users until this wave lands.

**Wave 2 — backend hardening (P1), parallelizable:** **06**, **07**, **08**, **09**, **10**, **12** are independent. **22** (CI) is worth pulling forward here so waves 2–4 land against a green pipeline.

**Wave 3 — frontend hardening (P1), parallelizable:** **13**, **14**, **15**, **16**, **17** are independent. Do **13** before **21**.

**Wave 4 — scale & polish (P2):** **11** (needs 03), **18**, **19**, **20**, **21** (needs 13), **23**, **24** (needs 06), **25** (needs 22).

---

## Dependency graph

```
02 ──┬─► 03 ──► 11
     ├─► 04
     └─► 05
06 ──► 24
13 ──► 21
22 ──► 25
(01, 07, 08, 09, 10, 12, 14, 15, 16, 17, 18, 19, 20, 23 — no prerequisites)
```

## Notes from drafting

- ChartCard actually lives at `frontend/src/components/widgets/ChartCard.tsx`; `plotly.js`/`react-plotly.js` are dead dependencies; a `ConfiguratorPanelV2` already exists behind a default-off toggle (`app/page.tsx:307`). See specs 13 and 21.
- `migrate_secret_key.py` is at `backend/` (not `scripts/`). Additional tracked junk beyond the plan: `frontend/.env.local`, 7 log files, 9 `.playwright-mcp` logs — all folded into spec 01's purge scope.
- Spec 25 records concrete CI baselines to contain: 63 frontend lint errors, 13 `tsc` errors, 3 failing backend tests, 70 ruff `F821`s — see spec 22 for the tested green-path approach.
- Specs contain no secret values (verified by the consistency pass).
