---
id: NN-kebab-slug
title: Short imperative title
priority: P0 | P1 | P2
effort: S | M | L | XL          # S<1d, M 1-3d, L 3-7d, XL >1wk
depends_on: []                  # list of spec ids that must land first
area: backend | frontend | ops | fullstack
---

## Problem

What is wrong today and why it blocks safe corporate/public use. One or two paragraphs. State the business/security risk in plain terms.

## Current State

Verified `file:line` references and short code snippets showing the actual current behavior. Never paste secrets — describe them (e.g. "SECRET_KEY present in `backend/.env`") without quoting the value.

## Desired State

The target behavior/architecture after this spec is implemented. Concrete and testable.

## Implementation Plan

Ordered, concrete steps an LLM subagent can execute without further discovery. Include library choices, config keys, migration order, and any backward-compat handling. Prefer reusing existing utilities (name them with paths).

## Files to Modify

- `path/to/file` — what changes here
- `path/to/new_file` — new; what it contains

## Acceptance Criteria

- [ ] Checkable outcome 1
- [ ] Checkable outcome 2

## Verification

Exact commands / tests / manual steps to prove it works end-to-end (run the server, hit the endpoint, run the test file).

## Out of Scope

What this spec deliberately does not cover (and which spec id covers it instead, if any).
