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

## License
Proprietary. All rights reserved.
