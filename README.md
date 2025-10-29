# Bayan

A modular analytics and reporting platform with a FastAPI backend and a Next.js app router frontend. Bayan lets you connect datasources, build dashboards and widgets, schedule sync tasks, and share or embed charts securely.

## Features
- **Datasources**: Connect SQL databases and APIs, inspect schemas, preview tables.
- **Dashboards & Widgets**: KPI, charts, tables, heatmaps, and more.
- **Global Filters**: URL-aware (builder) filters with persistence per dashboard.
- **Embedding**: Generate secure embeds with optional tokens and expiry.
- **Scheduling**: Snapshot/sequence sync tasks with progress and logs.
- **Admin**: Environment, metrics, users, and scheduler jobs.
- **Updates**: Versioned artifacts with a manifest for auto-update checks.

## Tech Stack
- **Frontend**: Next.js (App Router), React, Tremor UI, Radix, TanStack Query
- **Backend**: FastAPI, SQLAlchemy
- **Packaging**: Bash scripts producing versioned `.tar.gz` artifacts
- **Releases**: GitHub Releases via `gh` CLI

## Prerequisites
- Node.js 20+ and npm
- Python 3.11+
- Git and GitHub CLI (`gh auth login`)

Optional (datasources): ODBC drivers (see `docs/mssql-odbc.md` and `scripts/install_*` helpers).

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

## License
Proprietary. All rights reserved.
