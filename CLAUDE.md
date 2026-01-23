# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based billing automation project that runs scheduled tasks via GitHub Actions. The primary use case is checking business registration status (사업자 상태) using Korea's National Tax Service API and updating results to Google Sheets.

## Commands

```bash
# Install dependencies (uses uv package manager)
uv sync

# Run the main task locally (requires environment variables)
PYTHONPATH=. uv run python tasks/business_check/main.py
```

## Required Environment Variables

- `NTS_API_KEY`: Korea National Tax Service API key
- `WEBHOOK_URL_{TASK_KEY}`: Google Chat webhook URL (태스크별, 예: `WEBHOOK_URL_BUSINESS`)
- `GCP_SA_KEY`: Google Cloud service account JSON (entire JSON content)
- `GOOGLE_SHEET_ID`: Target Google Sheet ID

## Architecture

```
tasks/           # Individual scheduled tasks (each with its own main.py)
  └─ business_check/  # Checks business registration status via NTS API
shared/          # Reusable modules across tasks
  ├─ sheets.py   # Google Sheets connection (gspread + oauth2client)
  └─ notifier.py # Notifier 클래스: 태스크별 Google Chat 알림 (task_key, task_name 기반)
```

**Key pattern**: Tasks are self-contained in `tasks/<task_name>/` folders. Each task imports from `shared/` for common functionality. The `PYTHONPATH=.` setting enables these imports.

## GitHub Actions

Workflow at `.github/workflows/business_status.yml` runs daily at 09:00 KST (cron: `0 0 * * *`). Uses `uv sync --frozen` for reproducible dependency installation.
