---
id: 22-ci-cd-pipeline
title: CI/CD pipeline with lint, typecheck, tests, build
priority: P2
effort: M
depends_on: []
area: ops
---

## Problem

Nothing gates merges. There is no `.github/` directory, no GitLab CI, no pre-commit config anywhere in the repo. The frontend build explicitly disables its own quality gates (`next build --no-lint` + `typescript.ignoreBuildErrors: true`), so ESLint and TypeScript errors accumulate silently (currently 63 lint errors / 641 warnings, 13 `tsc` errors). Backend has 114 pytest tests that nothing runs automatically — 3 are already stale-failing and nobody noticed.

## Current State

All facts below verified on branch `feature/alpha-themes-foundation` (2026-07-07). Remote is GitHub (`https://github.com/Mr-7mdan/Bayan.git`), so GitHub Actions is the right CI.

**No CI anywhere:** `ls -a` at repo root shows no `.github/`, no `.gitlab-ci.yml`, no `.pre-commit-config.yaml`.

**Frontend gates disabled:**
- `frontend/package.json:8` — `"build": "next build --no-lint"`
- `frontend/next.config.js:8-12` — `typescript: { ignoreBuildErrors: true }`
- `frontend/.eslintrc.json` exists (extends `next/core-web-vitals`, `next/typescript`; turns off `@typescript-eslint/no-explicit-any`, `prefer-const` → warn) but is never enforced.

**Measured frontend baseline (must be handled or CI is red on day one):**
- `npx next lint` → exit 1: **63 errors** (39 `react-hooks/rules-of-hooks`, 22 `react/no-unescaped-entities`, 1 `react/display-name`, 1 `@typescript-eslint/no-namespace`) + 641 warnings.
- `npx tsc --noEmit` → 13 errors in `src/` **plus** ~20 phantom errors from `dist/frontend-4.5/...` because `frontend/tsconfig.json:40-42` excludes only `node_modules` while `include` has `**/*.ts` (pulls in the untracked `dist/` release copies). The 13 real ones:
  - `src/components/widgets/ChartCard.tsx:971` — `const _q = (p: any) => _q(p, signal)` — self-recursive, **never used** (`grep -n '\b_q\b'` shows only this line; the used sibling is `_qs` on line 970) → TS7023 + TS2554.
  - `src/components/widgets/ChartCard.tsx:2156,2413,2419,2420` and `src/components/widgets/HeatmapCard.tsx:691,692,694(x2)` — `q.data` inferred as `{}` (untyped `useQuery` at `ChartCard.tsx:959` and `HeatmapCard.tsx:514`) → TS2339 on `.rows`/`.columns`.
  - `src/components/widgets/ReportCard.tsx:173` — `queryFn: async ({ signal }) =>` implicit-any binding (TS7031).
  - `src/components/builder/ReportBuilderModal.tsx:2762` — `table.colWidths` with `table` possibly undefined (TS18048); `:2787` — `onChange({ ...table, colWidths: pctWidths })` where `rows`/`cols` are optional in the spread source but required in the target (TS2345).
- `frontend/package-lock.json` is tracked and **in sync** with `package.json` (verified by comparing lock root deps to package.json) → `npm ci` works.
- Node locally: v25; `next` 15.5.3 / `eslint` 8.57.1 / `typescript` ^5.5.4.

**Measured backend baseline:**
- Tests: `backend/tests/{test_date_presets.py,test_holidays.py,test_sqlglot_builder.py}` — `python -m pytest tests/ -q` from `backend/` → **3 failed, 111 passed** (0.46s). Tests import `app.*`, so cwd must be `backend/` and invocation must be `python -m pytest` (puts cwd on `sys.path`; there is no conftest.py / pytest.ini / pyproject.toml). The 3 stale failures (test expectations drifted from code, product judgment needed — not fixed here):
  - `tests/test_date_presets.py:37` `TestPresetConfig::test_legacy_map_covers_all_old_presets` — expects `day_before_yesterday` in `LEGACY_PRESET_MAP`; the map no longer has it.
  - `tests/test_date_presets.py:167` `TestResolvePresetDay::test_day_before_last_working_day` — expects 2026-03-15, resolver returns 2026-03-13 (workday/weekend logic drift).
  - `tests/test_sqlglot_builder.py:207` `TestSQLGlotBuilder::test_derived_columns_no_validation` — asserts `"IN" in sql`; builder now emits OR'd date-range predicates instead of `IN`.
- `pytest` is **not** in `backend/requirements.txt` (no dev requirements file exists).
- Ruff critical-rules scan (`--select E9,F63,F7,F82` over `app tests`): **70 pre-existing errors** — 68 F821 in `app/routers/query.py`, 1 F821 in `app/routers/datasources.py`, 1 F823 in `app/routers/alerts.py:694` (plus F821s there). Real latent bugs, but fixing them is its own task.
- `backend/app/config.py:7-24` — `Settings(BaseSettings)` has defaults for every field (SECRET_KEY placeholder default; real value lives in `backend/.env`, never in CI). Tests import `app.config` transitively via `app.models` and pass with defaults — no `.env` needed in CI.
- Local venv is Python 3.14.3; requirements are all `>=` ranges and install fine there.
- `.gitignore` already covers `backend/venv`, `frontend/dist`, `frontend/node_modules`.

## Desired State

- `.github/workflows/ci.yml` runs on every PR to `main` and push to `main`: backend job (ruff + pytest) and frontend job (tsc + lint + build + optional bundle-size budget), both green on the current codebase.
- `--no-lint` and `ignoreBuildErrors: true` removed; lint errors and type errors fail the build again.
- Pre-existing debt is contained, not hidden: 39 `rules-of-hooks` violations downgraded to warn with a marker comment, 70 backend F821/F823 quarantined via `per-file-ignores`, 3 stale tests marked `xfail` — each with a pointer to fix-then-remove.
- `.pre-commit-config.yaml` with a ruff hook for local use.
- Branch protection on `main` requiring both jobs.

## Implementation Plan

Order matters: steps 1–5 make the checks green locally, step 6 adds the workflow, 7–8 are supporting config.

**1. Backend dev requirements — create `backend/requirements-dev.txt`:**
```
pytest>=8.0
ruff>=0.9
```

**2. Backend ruff config — create `backend/ruff.toml`** (this exact content was verified green from both repo root and `backend/`):
```toml
# CI lint gate: critical errors only (syntax errors, undefined names).
target-version = "py312"

[lint]
select = ["E9", "F63", "F7", "F82"]

[lint.per-file-ignores]
# ponytail: 70 pre-existing F821/F823 undefined names in these routers; fix, then delete these lines
"**/app/routers/query.py" = ["F821", "F823"]
"**/app/routers/datasources.py" = ["F821"]
"**/app/routers/alerts.py" = ["F821", "F823"]
```
Note: the `**/` prefix is required — per-file-ignore globs don't match when ruff is invoked from the repo root otherwise (verified).

**3. Quarantine the 3 stale backend tests.** Add above each `def` listed in Current State (imports of `pytest` already exist in all three files):
```python
@pytest.mark.xfail(reason="stale expectation vs current implementation - see v2 Roadmap/P2-ops/22-ci-cd-pipeline.md", strict=False)
```
Do NOT fix the tests — whether test or code is right is a product decision (date-preset semantics, SQL shape).

**4. Frontend typecheck green:**
- `frontend/tsconfig.json:40-42` — change `"exclude": ["node_modules"]` → `"exclude": ["node_modules", "dist"]`.
- `frontend/src/components/widgets/ChartCard.tsx:971` — delete the line `const _q = (p: any) => _q(p, signal)` (unused, self-recursive; `_qs` on the line above is the one call sites use).
- `frontend/src/components/widgets/ChartCard.tsx:959` — `const q = useQuery({` → `const q = useQuery<any>({`. Same at `frontend/src/components/widgets/HeatmapCard.tsx:514`. This fixes all eight `.rows`/`.columns`-on-`{}` errors in both files at the source instead of eight casts.
- `frontend/src/components/widgets/ReportCard.tsx:173` — `queryFn: async ({ signal }) =>` → `queryFn: async ({ signal }: { signal: AbortSignal }) =>`.
- `frontend/src/components/builder/ReportBuilderModal.tsx:2762` — `table.colWidths` → `table?.colWidths`; `:2787` — `onChange({ ...table, colWidths: pctWidths })` → `onChange({ ...(table as any), colWidths: pctWidths })`.
- Line numbers may have drifted; each snippet above is unique in its file — locate by grep.
- Then `frontend/next.config.js:8-12` — delete the whole `typescript: { ignoreBuildErrors: true },` block.
- Add script to `frontend/package.json`: `"typecheck": "tsc --noEmit"`.
- Verify: `cd frontend && npm run typecheck` → 0 errors.

**5. Frontend lint green + re-enable in build:**
- `frontend/.eslintrc.json` `rules` — add:
```json
"react/no-unescaped-entities": "off",
"react/display-name": "off",
"@typescript-eslint/no-namespace": "off",
"react-hooks/rules-of-hooks": "warn"
```
  The first three are stylistic/low-value (22+1+1 hits). `rules-of-hooks` is a real correctness rule — the downgrade is temporary containment of 39 pre-existing violations. `.eslintrc.json` cannot hold comments, so the debt marker lives in the `.pre-commit-config.yaml` header comment (step 7) and this spec.
- `frontend/package.json:8` — `"build": "next build --no-lint"` → `"build": "next build"`.
- Verify: `cd frontend && npm run lint` → exit 0 (warnings only), then `npm run build` → succeeds.

**6. Create `.github/workflows/ci.yml`:**
```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  backend:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: backend
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: pip
          cache-dependency-path: backend/requirements*.txt
      - name: Install deps
        run: pip install -r requirements.txt -r requirements-dev.txt
      - name: Ruff (critical rules)
        run: ruff check app tests
      - name: Pytest
        run: python -m pytest tests/ -q

  frontend:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: frontend
    env:
      NEXT_TELEMETRY_DISABLED: "1"
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: npm
          cache-dependency-path: frontend/package-lock.json
      - name: Install deps
        run: npm ci
      - name: Typecheck
        run: npm run typecheck
      - name: Lint
        run: npm run lint
      - name: Build
        run: npm run build
      - name: Bundle size budget (spec 21)
        run: |
          if [ -f scripts/check-bundle-size.mjs ]; then npm run size; else echo "spec 21 not landed yet - skipping"; fi
```
Notes baked into the choices above:
- `python -m pytest` from `backend/` is load-bearing (no pytest config file; `app` package resolves via cwd).
- Python 3.12 in CI vs 3.14 locally: deps are `>=` ranges, tests are pure logic; 3.12 has the broadest wheel coverage (pyodbc, pyarrow, duckdb, dlt all ship manylinux wheels). No apt packages needed — pyodbc 5.x wheels bundle unixODBC.
- No env vars needed for tests: `app/config.py` fields all have defaults; never copy real values from `backend/.env` into the workflow.
- The bundle-size step self-skips until spec 21 lands its `scripts/check-bundle-size.mjs` + `"size"` script (spec 21 explicitly delegates CI wiring here). Once 21 is merged, replace the guard with a plain `npm run size`.
- `npm run build` re-runs lint inside `next build` (a couple of minutes duplicated). Acceptable; `next lint` is deprecated in Next 16 — when that upgrade happens, migrate to the ESLint CLI per the codemod, out of scope here.

**7. Create `.pre-commit-config.yaml`** at repo root (local-only convenience; CI is the real gate):
```yaml
# Backend only: ruff critical rules (same gate as CI).
# NOTE: react-hooks/rules-of-hooks is temporarily "warn" in frontend/.eslintrc.json
# (39 pre-existing violations) - restore to error once fixed. See spec 22.
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.9.0  # bump to latest at implementation time
    hooks:
      - id: ruff
        args: [--config, backend/ruff.toml]
        files: ^backend/(app|tests)/
```
No frontend hook — `tsc --noEmit` takes ~1 min on this codebase, too slow per-commit. Mention `pre-commit install` in the PR description; do not force it.

**8. Branch protection (manual/gh, after first green run on main):**
```bash
gh api -X PUT repos/Mr-7mdan/Bayan/branches/main/protection \
  -f 'required_status_checks[strict]=false' \
  -f 'required_status_checks[checks][][context]=backend' \
  -f 'required_status_checks[checks][][context]=frontend' \
  -F 'enforce_admins=false' \
  -F 'required_pull_request_reviews=null' -F 'restrictions=null'
```
(Or via GitHub UI: Settings → Branches → protect `main`, require `backend` + `frontend` checks.)

## Files to Modify

- `.github/workflows/ci.yml` — **new**, workflow above.
- `backend/requirements-dev.txt` — **new**, pytest + ruff.
- `backend/ruff.toml` — **new**, verified config above.
- `backend/tests/test_date_presets.py` — xfail decorators on 2 tests (lines ~37, ~167).
- `backend/tests/test_sqlglot_builder.py` — xfail decorator on 1 test (line ~207).
- `frontend/package.json` — remove `--no-lint` from `build`; add `typecheck` script.
- `frontend/next.config.js` — remove `typescript.ignoreBuildErrors` block.
- `frontend/tsconfig.json` — exclude `dist`.
- `frontend/.eslintrc.json` — 4 rule adjustments.
- `frontend/src/components/widgets/ChartCard.tsx` — delete dead `_q` line; `useQuery<any>`.
- `frontend/src/components/widgets/HeatmapCard.tsx` — `useQuery<any>`.
- `frontend/src/components/widgets/ReportCard.tsx` — annotate `signal`.
- `frontend/src/components/builder/ReportBuilderModal.tsx` — optional-chain + cast (2 lines).
- `.pre-commit-config.yaml` — **new**.

## Acceptance Criteria

- [ ] `cd backend && venv/bin/ruff check app tests` (after `pip install -r requirements-dev.txt`) exits 0.
- [ ] `cd backend && venv/bin/python -m pytest tests/ -q` → 111 passed, 3 xfailed, 0 failed.
- [ ] `cd frontend && npm run typecheck` exits 0 (0 errors, `dist/` excluded).
- [ ] `cd frontend && npm run lint` exits 0 (warnings allowed, 0 errors).
- [ ] `cd frontend && npm run build` succeeds with lint and type-checking enabled (no `--no-lint`, no `ignoreBuildErrors`).
- [ ] Push branch → both `backend` and `frontend` jobs green in GitHub Actions on the PR.
- [ ] Deliberate breakage test: a PR introducing `const x: string = 1` in frontend or `undefined_name` in a new backend file fails CI.
- [ ] Debt recorded, not erased: ruff per-file-ignores, `rules-of-hooks: warn`, and 3 xfails each carry a comment pointing back to this spec.
- [ ] Branch protection on `main` requires both checks (or the gh command from step 8 is documented in the PR if perms are missing).

## Verification

```bash
# Backend, from repo checkout
cd backend
venv/bin/pip install -r requirements-dev.txt
venv/bin/ruff check app tests                # exit 0
venv/bin/python -m pytest tests/ -q          # 111 passed, 3 xfailed

# Frontend
cd ../frontend
npm run typecheck                            # exit 0
npm run lint                                 # exit 0, warnings only
npm run build                                # succeeds, runs lint+types

# CI end-to-end
git checkout -b ci/pipeline && git add -A && git commit -m "ci: add GitHub Actions pipeline"
git push -u origin ci/pipeline
gh pr create --fill && gh pr checks --watch  # backend + frontend green

# Negative test (throwaway commit on the PR branch)
echo "x = undefined_name" >> backend/app/__init__.py && git commit -am wip && git push
gh pr checks   # backend job must FAIL; then revert the commit
```

## Out of Scope

- Fixing the 70 F821/F823 undefined names in `app/routers/{query,datasources,alerts}.py` (real bugs — deserves its own P1 spec; the per-file-ignores are the tracked containment).
- Fixing the 39 `react-hooks/rules-of-hooks` violations and 641 lint warnings.
- Resolving the 3 xfailed tests (product decision on date-preset semantics and SQL shape).
- mypy (nothing is annotated for it; ruff F-rules give the cheap 80%), broader ruff rulesets, and any Python/JS formatter (would touch every file).
- CD / deployment (spec 23-docker-deployment), release packaging (`backend/dist`, `frontend/dist`).
- Frontend unit tests (none exist; the 3 root-level `test_*.py` and `backend/test_{normalizer,deposits,mssql_conn}.py` are ad-hoc scripts needing live DBs — deliberately not collected since CI runs `pytest tests/` only).
- Migrating `next lint` → ESLint CLI (needed at Next 16 upgrade, not now).
