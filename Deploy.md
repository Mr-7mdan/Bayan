
## Build Artifacts
- Frontend:
  ```bash
  bash scripts/build_frontend_artifact.sh
  ```
- Backend:
  ```bash
  bash scripts/build_backend_artifact.sh
  ```
- Outputs are written to `scripts/out/` and `*-latest.json` files record metadata.

## Generate Update Manifest
Create a manifest that references the release assets (urls and sha256).
```bash
bash scripts/make_manifest.sh "<owner>" "<repo>"
# Example
bash scripts/make_manifest.sh "Mr-7mdan" "Bayan"
```
- Result: `scripts/out/bayan-manifest.json`

## First-time GitHub Push & Release
Use the helper script; it handles empty repositories by initializing git, pushing the main branch and tag, then creating the release.
```bash
# Authenticate once
gh auth login

# Push and create the release (owner/repo optional; defaults to UPDATE_REPO_* env or Mr-7mdan/Bayan)
bash scripts/push_to_repo.sh "Mr-7mdan" "Bayan"
```
What it does:
- Initializes a git repo if missing, sets `origin`, pushes `main`.
- Creates tag `v<version>` from `scripts/out/*-latest.json`.
- Creates (or reuses) a GitHub release and uploads:
  - `frontend-<version>.tar.gz`
  - `backend-<version>.tar.gz`
  - `bayan-manifest.json`

Troubleshooting:
- `HTTP 422 Repository is empty` → The script now pushes `main` before creating the release.
- `gh: not found` → Install GitHub CLI and run `gh auth login`.

## Deployment (Overview)
- Backend: See `backend/deploy/` for systemd and production examples; configure env and run the packaged app.
- Frontend: Deploy the built Next.js artifact according to your environment. Serve the compiled output from `frontend-<version>` with your preferred Node/hosting setup.
