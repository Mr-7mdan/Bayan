---
id: 01-secret-rotation-and-history-purge
title: Rotate leaked secrets and purge them from git history
priority: P0
effort: M
depends_on: []
area: ops
---

## Problem

`backend/.env` is committed to git (public-facing remote `https://github.com/Mr-7mdan/Bayan.git`) and contains live secrets:

- `SECRET_KEY` (line 4) — derives the Fernet key that encrypts ALL stored datasource DSNs and SMTP passwords, and signs reset/embed tokens.
- `ADMIN_PASSWORD` (line 46) — bootstrap admin credential.
- `GITHUB_TOKEN` (line 64) — a live fine-grained GitHub PAT (`github_pat_...` prefix), used for the update-manifest feature.

Also tracked in git: `backend/bayan.db` (SQLite metadata DB — historical versions may contain encrypted DSNs and password hashes), multiple `.log` files, `frontend/.env.local`, and `.playwright-mcp/` console logs. `.gitignore` does not exclude `.env`, `*.log`, or `*.db`. Anyone with repo access (or a clone) holds keys to every connected customer database.

## Current State

Verified 2026-07-07:

- `backend/.env` — tracked; keys at lines: `SECRET_KEY` (4), `ADMIN_PASSWORD` (46), `GITHUB_TOKEN` (64). Values NOT reproduced here.
- `backend/app/security.py:17-23` — Fernet key is derived directly from `SECRET_KEY`:
  ```python
  def _derive_key(secret: str) -> bytes:
      digest = hashlib.sha256(secret.encode("utf-8")).digest()
      return base64.urlsafe_b64encode(digest)

  def _fernet() -> Fernet:
      return Fernet(_derive_key(settings.secret_key))
  ```
- `SECRET_KEY` is also used for: legacy SHA256 password verification (`security.py:53`), reset tokens (`security.py:94,108`), embed tokens (`security.py:138,152`).
- `backend/app/main.py:44-56` — startup guard already refuses placeholder keys (`_PLACEHOLDER_KEYS`) in non-dev environments.
- `backend/app/main.py:131-146` — admin bootstrap only creates an admin if none exists; changing `ADMIN_PASSWORD` in `.env` does NOT update an existing admin's password.
- `backend/migrate_secret_key.py` (NOT `scripts/` — it lives in `backend/`) — existing one-time re-encryption script. Usage from its docstring:
  ```
  OLD_SECRET_KEY=<old-key> ./venv/bin/python migrate_secret_key.py
  ```
  Re-encrypts `Datasource.connection_encrypted` and `EmailConfig.password_encrypted` from old key to the key currently in `.env`. This means datasource credentials do NOT need manual re-entry if the old key is still known.
- `.gitignore` (repo root, 24 lines) — has `.DS_Store` (line 19) but lacks `.env`, `*.log`, `*.db`, `.playwright-mcp/`.
- `git ls-files` shows these sensitive/junk tracked files:
  - `backend/.env`
  - `frontend/.env.local` (no secrets — only `NEXT_PUBLIC_*` keys — but is machine-local config)
  - `backend/bayan.db` (0 bytes at HEAD, but history may contain populated versions)
  - `backend/backend.log`, `backend/temp.logs.log`, `backend/temp2.logs.log`
  - `logs/backend.log`, `logs/backend_clean.log`, `logs/gunicorn.log`, `logs/gunicorn_new.log`
  - `.playwright-mcp/console-*.log` (9 files)
- `backend/.env.example` exists but is stale — missing keys present in real `.env`: `ADMIN_EMAIL/PASSWORD/NAME`, `UPDATES_ENABLED`, `UPDATE_REPO_OWNER/NAME`, `GITHUB_TOKEN`, `ENABLE_SQLGLOT`, `SQLGLOT_USERS`, `ENABLE_LEGACY_FALLBACK`, `WEEK_START_DAY`, `SCHEDULER_TIMEZONE`, `PLAYWRIGHT_BROWSERS_PATH`, `APP_VERSION`, `WEB_CONCURRENCY`, `HOT_RELOAD`, `DUCKDB_THREADS`, `DUCKDB_MEMORY_LIMIT`.
- Remote branches: `main` plus 4 `feature/alpha-themes-*` branches, all pushed to origin. All must be rewritten together.
- `git-filter-repo` is NOT installed on this machine.

## Desired State

- All three leaked secrets rotated/revoked; app fully functional with the new `SECRET_KEY` (datasource creds re-encrypted via `backend/migrate_secret_key.py`).
- No `.env`, `.db`, or `.log` file tracked in git, on any branch, in any historical commit.
- `.gitignore` blocks re-introduction.
- `backend/.env.example` documents every key the app reads, with placeholder values only.

## Implementation Plan

### Phase A — Rotate secrets (do FIRST; history purge does not un-leak anything)

1. **Revoke the GitHub PAT** (manual, browser): GitHub → Settings → Developer settings → Fine-grained personal access tokens → find the token matching the value at `backend/.env:64` → Delete. If the update-manifest feature is still needed, generate a replacement PAT scoped read-only to the release repo (`UPDATE_REPO_OWNER`/`UPDATE_REPO_NAME` values in `.env`) and put it in the local `.env` only.
2. **Generate a new SECRET_KEY** and stash the old one temporarily:
   ```bash
   cd /Users/mohammed/Documents/Bayan/backend
   OLD_KEY=$(grep '^SECRET_KEY=' .env | cut -d= -f2-)
   NEW_KEY=$(python3 -c "import secrets; print(secrets.token_urlsafe(48))")
   # edit .env: replace SECRET_KEY value with $NEW_KEY (do not commit)
   ```
3. **Re-encrypt stored credentials** with the existing script (run against the real metadata DB — confirm `metadata_db_path` / `bayan.db` the backend actually uses in this deployment before running):
   ```bash
   cd /Users/mohammed/Documents/Bayan/backend
   OLD_SECRET_KEY="$OLD_KEY" ./venv/bin/python migrate_secret_key.py
   ```
   Expected output: `OK: <ds-id>` per datasource, `Done: N credentials re-encrypted`. If the old key were unknown, all DSNs would need manual re-entry in the UI — not the case here.
4. **Rotation side effects to handle:**
   - Legacy SHA256 password hashes (`security.py:46-54`) verify against `secret_key` — any user still on a legacy hash can no longer log in. Check: `SELECT id,email FROM users WHERE length(password_hash)=64 AND password_hash NOT LIKE '$%';` — reset those passwords via admin UI if any exist.
   - In-flight reset tokens and signed embed-token URLs are invalidated (acceptable; embed links using stored public IDs without signed tokens are unaffected).
5. **Change the admin password**: bootstrap (`main.py:131-146`) will NOT re-apply `ADMIN_PASSWORD` because an admin already exists. Change it through the app's profile/user-management UI (argon2id re-hash via `hash_password`). Also replace the `ADMIN_PASSWORD` value in local `.env` so the leaked value is nowhere live.
6. Restart backend; verify startup passes the placeholder guard and datasources connect (proves Fernet re-encryption worked).
7. Discard `$OLD_KEY` from shell history: `unset OLD_KEY NEW_KEY; history -p` (zsh: check `~/.zsh_history` if `INC_APPEND_HISTORY` is on).

### Phase B — Fix .gitignore and untrack files (commit BEFORE the history rewrite so the rewritten tip is clean)

8. Append to `/Users/mohammed/Documents/Bayan/.gitignore`:
   ```gitignore
   # Secrets and local config
   .env
   *.env.local
   !*.env.example
   # Databases and logs
   *.db
   *.sqlite
   *.duckdb
   *.log
   logs/
   .playwright-mcp/
   ```
   (`.DS_Store` already present at line 19; `.env.example` files must stay tracked — the `!` negation covers `reporting-api.env.example`; plain `backend/.env.example` is not matched by `.env` since gitignore `.env` matches basename exactly, `.env.example` is a different basename — no negation needed for it.)
9. Untrack without deleting local copies:
   ```bash
   cd /Users/mohammed/Documents/Bayan
   git rm --cached backend/.env frontend/.env.local backend/bayan.db \
     backend/backend.log backend/temp.logs.log backend/temp2.logs.log \
     logs/backend.log logs/backend_clean.log logs/gunicorn.log logs/gunicorn_new.log \
     .playwright-mcp/console-*.log
   git commit -m "chore(security): untrack env, db, and log files; harden .gitignore"
   ```

### Phase C — Refresh .env.example

10. Update `backend/.env.example` to include every key from Current State's stale-list with placeholder values (e.g. `SECRET_KEY=CHANGE-ME-generate-with-secrets.token_urlsafe`, `GITHUB_TOKEN=`, `ADMIN_PASSWORD=`). Copy key names from the real `.env` — never values. Include this in the Phase B commit or its own.

### Phase D — Purge history

11. Install tool: `brew install git-filter-repo`.
12. Coordinate collaborators FIRST: history rewrite orphans every existing clone. All 5 branches (`main`, 4 `feature/alpha-themes-*`) are rewritten. Collaborators must re-clone (not pull) afterward. Any open PRs will need rebasing onto rewritten history.
13. Work on a fresh mirror clone (filter-repo refuses non-fresh clones by design):
    ```bash
    cd /private/tmp/claude-501/-Users-mohammed-Documents-Bayan/*/scratchpad
    git clone --mirror https://github.com/Mr-7mdan/Bayan.git bayan-purge.git
    cd bayan-purge.git
    git filter-repo \
      --invert-paths \
      --path backend/.env \
      --path frontend/.env.local \
      --path backend/bayan.db \
      --path backend/backend.log \
      --path backend/temp.logs.log \
      --path backend/temp2.logs.log \
      --path logs/ \
      --path .playwright-mcp/
    ```
14. Verify the purge inside the mirror:
    ```bash
    git log --all --oneline -- backend/.env        # must print nothing
    git grep -I "github_pat_" $(git rev-list --all) -- || echo CLEAN
    ```
15. Force-push all refs:
    ```bash
    git push --force --mirror https://github.com/Mr-7mdan/Bayan.git
    ```
    Note: `--mirror` push also deletes any remote refs not in the mirror (fine here — mirror clone has them all). GitHub may retain orphaned commits reachable via cached PR refs/API for a while — contact GitHub Support to run garbage collection, or accept residual exposure since secrets are already rotated (that is why Phase A comes first).
16. Reset the working repo to the rewritten remote (do NOT pull-merge):
    ```bash
    cd /Users/mohammed/Documents/Bayan
    git stash -u   # if dirty (AlertDialog.tsx modification is currently unstaged)
    git fetch origin
    for b in main feature/alpha-themes-foundation feature/alpha-themes-core-interface feature/alpha-themes-nova-estate feature/alpha-themes-platform-architecture; do
      git checkout "$b" && git reset --hard "origin/$b"
    done
    git checkout feature/alpha-themes-foundation
    git stash pop
    git reflog expire --expire=now --all && git gc --prune=now --aggressive
    ```
    Confirm local `backend/.env` still exists on disk (it was never deleted, only untracked) and `git status` shows it ignored.

## Files to Modify

- `.gitignore` — add `.env`, `*.env.local`, `!*.env.example`, `*.db`, `*.sqlite`, `*.duckdb`, `*.log`, `logs/`, `.playwright-mcp/`.
- `backend/.env.example` — add all missing keys with placeholders (no real values).
- `backend/.env` — local only, never committed: new `SECRET_KEY`, new `ADMIN_PASSWORD`, new/empty `GITHUB_TOKEN`.
- Git index — `git rm --cached` on the 13 tracked sensitive/junk files listed in Phase B.
- Git history — rewritten via `git filter-repo` (all branches, force-pushed).

## Acceptance Criteria

- [ ] Old GitHub PAT is revoked (attempting to use it returns 401).
- [ ] `SECRET_KEY` in `backend/.env` differs from the leaked value; backend starts without the placeholder-key fatal.
- [ ] `backend/migrate_secret_key.py` ran successfully; every datasource connects/queries in the UI post-rotation.
- [ ] Admin login works with the NEW password; the leaked password no longer authenticates.
- [ ] No users left with legacy 64-hex password hashes (or their passwords were reset).
- [ ] `git ls-files | grep -E '\.env$|\.env\.local|\.db$|\.log$'` returns nothing on all branches.
- [ ] `git log --all -- backend/.env` returns nothing in a fresh clone from origin.
- [ ] Searching all history for the PAT prefix finds nothing: `git grep "github_pat_" $(git rev-list --all)` → no hits (in fresh clone).
- [ ] `.gitignore` prevents re-adding: `touch backend/test.env.local backend/x.log && git status --porcelain | grep -E 'x.log|test.env'` → empty (then delete test files).
- [ ] `backend/.env.example` contains every key the app reads (cross-check against `backend/app/config.py` fields) with placeholder values only.
- [ ] All collaborators notified and re-cloned.

## Verification

```bash
# 1. Backend healthy with new key
cd /Users/mohammed/Documents/Bayan/backend && ./venv/bin/uvicorn app.main:app --port 8000 &
curl -s http://localhost:8000/api/health || curl -s http://localhost:8000/health

# 2. Datasource decryption works (list datasources, then test-connect one via UI or API)
curl -s http://localhost:8000/api/datasources -H "Authorization: Bearer <session>" | head

# 3. Fresh clone is clean
cd $SCRATCHPAD && git clone https://github.com/Mr-7mdan/Bayan.git verify-clone && cd verify-clone
git log --all --oneline -- backend/.env backend/bayan.db   # expect: empty
git grep -I "github_pat_" $(git rev-list --all) -- ; echo "exit=$? (1 = clean)"
git ls-files | grep -E '\.log$|\.db$|\.env$'                # expect: empty

# 4. Ignore rules hold
cd /Users/mohammed/Documents/Bayan
git check-ignore backend/.env backend/bayan.db logs/backend.log .playwright-mcp/x.log && echo IGNORED
```

Manual: log in as admin with new password; open a dashboard backed by an external datasource and confirm data loads (proves Fernet re-encryption end-to-end).

## Out of Scope

- Moving secrets to a secret manager (Vault/SOPS/1Password) — separate hardening spec.
- Key-versioning or envelope encryption for DSNs (would make future rotations zero-downtime) — future improvement.
- Pre-commit secret-scanning hooks (gitleaks/trufflehog in CI) — recommended follow-up spec.
- Rotating credentials of the external customer databases whose DSNs were encrypted (the DSNs were encrypted at rest; only rotate those if you assess the Fernet ciphertexts + leaked SECRET_KEY were both exfiltrated — note: anyone with the repo had both, so this SHOULD be a follow-up decision, but it is operationally owned by DB admins, not this spec).
- Auth/session hardening (JWT, cookie flags) — separate P0 specs.
