"""
HyperBilling 데이터 수집 및 사업자등록번호 시트 업데이트

매월 익월 정산 데이터를 수집하여:
1. Invoice, Contract, Company 데이터 조인
2. 활성 계약의 회사명과 사업자등록번호 추출
3. Google Sheets에 업데이트 (CSP별 워크시트)
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

from shared.hb_client import (
    HBApiClient, 
    get_previous_month, 
    extract_active_contracts
)
from shared.sheets import get_document, get_worksheet
from shared.notifier import Notifier

load_dotenv()

# 환경변수
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
ALIBABA_COOKIE = os.environ.get("ALIBABA_COOKIE")
AKAMAI_COOKIE = os.environ.get("AKAMAI_COOKIE")
GCP_COOKIE = os.environ.get("GCP_COOKIE")
INVOICE_MONTH = os.environ.get("INVOICE_MONTH", "")  # 빈칸이면 자동으로 전월

notifier = Notifier(task_key="HB_COLLECT", task_name="HB 데이터 수집")

# CSP별 워크시트 이름 매핑
WORKSHEET_NAMES = {
    'alibaba': 'Alibaba',
    'akamai': 'Akamai',
    'gcp': 'GCP',
}


def process_csp(csp_name: str, cookie: str, invoice_month: str) -> dict:
    """
    단일 CSP 데이터 수집 및 처리
    
    Returns:
        {
            'success': bool,
            'contracts': list,
            'total': int,
            'error': str|None
        }
    """
    print(f"\n[{csp_name.upper()}] 데이터 수집 시작...")
    
    try:
        # API 클라이언트 생성
        client = HBApiClient(csp_name, cookie)
        
        # 데이터 수집
        data = client.fetch_all_data(invoice_month)
        
        # 활성 계약 추출
        contracts = extract_active_contracts(
            data['invoice'],
            data['contract'],
            data['company']
        )
        
        print(f"[{csp_name.upper()}] ✅ {len(contracts)}건 추출 완료")
        
        return {
            'success': True,
            'contracts': contracts,
            'total': len(contracts),
            'error': None
        }
        
    except Exception as e:
        print(f"[{csp_name.upper()}] ❌ 실패: {e}")
        return {
            'success': False,
            'contracts': [],
            'total': 0,
            'error': str(e)
        }


def update_sheet(csp_name: str, contracts: list, worksheet) -> bool:
    """
    워크시트에 회사명과 사업자등록번호 업데이트

    전략: 사업자등록번호 기준으로 기존 C/D(상태/날짜) 데이터를 보존하며 A/B 갱신

    시트 구조:
    A열: 사업자등록번호 (이 태스크에서 관리)
    B열: 회사명 (이 태스크에서 관리)
    C열: 상태 (business_check 태스크에서 관리)
    D열: 날짜 (business_check 태스크에서 관리)

    Args:
        csp_name: CSP 이름
        contracts: 계약 리스트
        worksheet: gspread 워크시트

    Returns:
        성공 여부
    """
    print(f"  [{csp_name.upper()}] 시트 업데이트 중...")

    try:
        # 데이터 준비 (사업자등록번호 있는 것만, 중복 제거, 정렬)
        rows = []
        seen_licenses = set()
        for contract in contracts:
            license_no = contract.get('company_license', '')
            company_name = contract.get('company_name', '')

            if not license_no or str(license_no).strip() == '':
                continue

            license_key = str(license_no).strip()
            if license_key in seen_licenses:
                continue
            seen_licenses.add(license_key)

            rows.append([license_key, company_name])

        rows.sort(key=lambda r: r[0])

        if not rows:
            print(f"  [{csp_name.upper()}] 사업자등록번호 있는 데이터 없음")
            return True

        # 기존 시트 데이터 읽기
        try:
            existing_values = worksheet.get_all_values()
            is_empty = len(existing_values) <= 1
        except Exception:
            existing_values = []
            is_empty = True

        # 기존 C/D 데이터를 사업자등록번호 기준으로 맵 구성
        existing_cd = {}
        if not is_empty:
            for row in existing_values[1:]:  # 헤더 제외
                if len(row) >= 1 and row[0].strip():
                    license_key = row[0].strip()
                    status = row[2] if len(row) >= 3 else ''
                    date = row[3] if len(row) >= 4 else ''
                    existing_cd[license_key] = [status, date]

        # 새 데이터에 기존 C/D 매칭
        all_rows = [['사업자등록번호', '회사명', '상태', '날짜']]
        for license_no, company_name in rows:
            cd = existing_cd.get(license_no, ['', ''])
            all_rows.append([license_no, company_name, cd[0], cd[1]])

        # 전체 쓰기 (A~D)
        worksheet.update(f'A1:D{len(all_rows)}', all_rows)

        # 기존 행이 더 많았으면 나머지 정리
        if len(existing_values) > len(all_rows):
            excess_start = len(all_rows) + 1
            excess_end = len(existing_values)
            excess_count = excess_end - excess_start + 1
            empty_rows = [['', '', '', '']] * excess_count
            worksheet.update(f'A{excess_start}:D{excess_end}', empty_rows)

        if is_empty:
            worksheet.format('A1:D1', {
                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9},
                'textFormat': {'bold': True}
            })

        dup_count = len(contracts) - len(rows) - sum(
            1 for c in contracts
            if not c.get('company_license') or str(c.get('company_license', '')).strip() == ''
        )
        dup_msg = f" (중복 {dup_count}건 제거)" if dup_count > 0 else ""
        print(f"  [{csp_name.upper()}] {len(rows)}개 회사 업데이트 완료{dup_msg}")
        return True

    except Exception as e:
        print(f"  [{csp_name.upper()}] 시트 업데이트 실패: {e}")
        return False


def run():
    """메인 실행 함수"""
    
    # 1. 인보이스 월 결정
    if INVOICE_MONTH and INVOICE_MONTH.strip():
        invoice_month = INVOICE_MONTH.strip()
        print(f"📅 수동 지정된 인보이스 월: {invoice_month}")
    else:
        invoice_month = get_previous_month()
        current_month = datetime.now().strftime('%Y년 %m월')
        previous_month_display = datetime.strptime(invoice_month, '%Y%m').strftime('%Y년 %m월')
        print(f"📅 자동 계산된 인보이스 월: {invoice_month}")
        print(f"   현재: {current_month} → 수집: {previous_month_display} (익월 정산)")
    
    print(f"\n{'='*70}")
    print(f"HyperBilling 데이터 수집 시작")
    print(f"{'='*70}\n")
    
    # 2. CSP별 데이터 수집
    csps = {
        'alibaba': ALIBABA_COOKIE,
        'akamai': AKAMAI_COOKIE,
        'gcp': GCP_COOKIE
    }
    
    results = {}
    for csp_name, cookie in csps.items():
        if not cookie:
            print(f"[{csp_name.upper()}] 쿠키 없음 - 스킵")
            results[csp_name] = {
                'success': False,
                'contracts': [],
                'total': 0,
                'error': '쿠키 미설정'
            }
            continue
        
        results[csp_name] = process_csp(csp_name, cookie, invoice_month)
    
    # 3. Google Sheets 업데이트
    print(f"\n{'='*70}")
    print(f"Google Sheets 업데이트")
    print(f"{'='*70}\n")
    
    try:
        doc = get_document(SHEET_ID)
        
        for csp_name, result in results.items():
            if not result['success']:
                print(f"[{csp_name.upper()}] 데이터 없음 - 스킵")
                continue
            
            worksheet_name = WORKSHEET_NAMES[csp_name]
            
            try:
                worksheet = get_worksheet(doc, worksheet_name)
            except Exception:
                # 워크시트가 없으면 생성
                print(f"  [{csp_name.upper()}] 워크시트 '{worksheet_name}' 생성 중...")
                worksheet = doc.add_worksheet(
                    title=worksheet_name,
                    rows=1000,
                    cols=10
                )
            
            # 시트 업데이트
            update_success = update_sheet(csp_name, result['contracts'], worksheet)
            results[csp_name]['sheet_updated'] = update_success
    
    except Exception as e:
        error_msg = f"시트 연결 실패: {e}"
        print(f"❌ {error_msg}")
        notifier.send(status="실패", details=error_msg)
        return
    
    # 4. 결과 알림
    today_str = datetime.now().strftime("%Y-%m-%d")
    total_contracts = sum(r['total'] for r in results.values() if r['success'])
    success_count = sum(1 for r in results.values() if r['success'])
    
    details_lines = [
        f"📅 {today_str}",
        f"📊 인보이스 월: {invoice_month}",
        ""
    ]
    
    for csp_name, result in results.items():
        if result['success']:
            sheet_status = "✅" if result.get('sheet_updated', False) else "⚠️"
            details_lines.append(
                f"{sheet_status} {csp_name.upper()}: {result['total']}건"
            )
        else:
            details_lines.append(f"❌ {csp_name.upper()}: {result['error']}")
    
    details_lines.append("")
    details_lines.append(f"───────────")
    details_lines.append(f"총 {total_contracts}건 수집 ({success_count}/3 CSP 성공)")
    
    status = "완료" if success_count == 3 else "일부 성공" if success_count > 0 else "실패"
    notifier.send(status=status, details="\n".join(details_lines))
    
    print(f"\n{'='*70}")
    print(f"완료: {total_contracts}건")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    # 필수 환경변수 체크
    if not SHEET_ID:
        print("❌ GOOGLE_SHEET_ID 환경변수가 설정되지 않았습니다.")
        sys.exit(1)
    
    # 쿠키 하나라도 있는지 체크
    if not any([ALIBABA_COOKIE, AKAMAI_COOKIE, GCP_COOKIE]):
        print("❌ 최소 하나의 CSP 쿠키가 필요합니다.")
        print("   환경변수: ALIBABA_COOKIE, AKAMAI_COOKIE, GCP_COOKIE")
        sys.exit(1)
    
    run()