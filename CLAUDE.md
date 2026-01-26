# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based billing automation project that runs scheduled tasks via GitHub Actions. Two primary use cases:
1. **HB 데이터 수집**: HyperBilling API에서 CSP별(Alibaba, Akamai, GCP) 인보이스·계약·회사 데이터를 수집하여 Google Sheets에 사업자등록번호·회사명 업데이트
2. **사업자 상태 점검**: 국세청 API로 사업자등록 상태를 조회하여 Google Sheets에 결과 기록

## Commands

```bash
# Install dependencies (uses uv package manager)
uv sync

# Run tasks locally (requires environment variables)
PYTHONPATH=. uv run python tasks/business_check/main.py
PYTHONPATH=. uv run python tasks/hb_collect/main.py
```

## Required Environment Variables

### 공통
- `GCP_SA_KEY`: Google Cloud service account JSON (entire JSON content)
- `GOOGLE_SHEET_ID`: Target Google Sheet ID
- `WEBHOOK_URL_{TASK_KEY}`: Google Chat webhook URL (태스크별, 예: `WEBHOOK_URL_BUSINESS`, `WEBHOOK_URL_HB_COLLECT`)

### business_check
- `NTS_API_KEY`: Korea National Tax Service API key

### hb_collect
- `ALIBABA_COOKIE`: Alibaba Cloud HyperBilling connect.sid 쿠키
- `AKAMAI_COOKIE`: Akamai HyperBilling connect.sid 쿠키
- `GCP_COOKIE`: GCP HyperBilling connect.sid 쿠키
- `INVOICE_MONTH`: (선택) 인보이스 월 YYYYMM 형식 (미지정 시 자동으로 전월)

## Architecture

```
tasks/               # Individual scheduled tasks (each with its own main.py)
  ├─ business_check/ # 국세청 API로 사업자등록 상태 조회
  └─ hb_collect/     # HyperBilling API에서 CSP별 데이터 수집 → 시트 업데이트
shared/              # Reusable modules across tasks
  ├─ sheets.py       # Google Sheets connection (gspread + oauth2client)
  ├─ notifier.py     # Notifier 클래스: 태스크별 Google Chat 알림 (task_key, task_name 기반)
  └─ hb_client.py    # HBApiClient: CSP별 HyperBilling API 호출 및 데이터 추출
```

**Key pattern**: Tasks are self-contained in `tasks/<task_name>/` folders. Each task imports from `shared/` for common functionality. The `PYTHONPATH=.` setting enables these imports.

**태스크 간 연동**: `hb_collect`가 시트의 A(사업자등록번호), B(회사명) 컬럼을 채우면 `business_check`가 A 컬럼을 읽어 상태를 조회하여 C(상태), D(날짜)에 기록합니다.

## GitHub Actions

- `.github/workflows/business_status.yml`: 매일 09:00 KST — 사업자 상태 점검
- `.github/workflows/hb_collect.yml`: 매일 11:00 KST — HB 데이터 수집 (수동 실행 시 invoice_month 지정 가능)
- `.github/workflows/hb_keepalive.yml`: 12시간마다 (09:00, 21:00 KST) — HB 쿠키 세션 유지 (가벼운 API ping)

모든 워크플로우는 `uv sync --frozen`으로 의존성을 설치합니다 (keepalive 제외 — 인라인 스크립트 사용).
