import os
import sys
import json
import requests
from datetime import datetime

from dotenv import load_dotenv

from shared.sheets import get_document, get_all_worksheets
from shared.notifier import Notifier

load_dotenv()

SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
API_KEY = os.environ.get("NTS_API_KEY")
NTS_API_URL = f"https://api.odcloud.kr/api/nts-businessman/v1/status?serviceKey={API_KEY}"

notifier = Notifier(task_key="BUSINESS", task_name="사업자 상태 점검")


def fetch_status_batch(b_no_list):
    """국세청 API로 100개씩 상태 조회"""
    results = {}

    for i in range(0, len(b_no_list), 100):
        chunk = b_no_list[i:i+100]
        clean_chunk = [str(no).replace("-", "").strip() for no in chunk if str(no).strip()]

        if not clean_chunk:
            continue

        try:
            resp = requests.post(
                NTS_API_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps({"b_no": clean_chunk})
            )

            if resp.status_code == 200:
                data = resp.json().get('data', [])
                for item in data:
                    state = item['b_stt']
                    date = item.get('end_dt', '')

                    if not date and item['b_stt_cd'] == '01':
                        date = "운영중"

                    results[item['b_no']] = {'state': state, 'date': date}
            else:
                print(f"API Error ({resp.status_code}): {resp.text}")

        except Exception as e:
            print(f"Batch processing error: {e}")

    return results


def process_worksheet(csp_name: str, worksheet) -> dict:
    """
    단일 워크시트(CSP) 처리.

    Returns:
        {'total': int, 'closed': int, 'error': str|None}
    """
    print(f"  [{csp_name}] 처리 중...")

    try:
        business_numbers = worksheet.col_values(1)[1:]  # A열, 헤더 제외
    except Exception as e:
        return {'total': 0, 'closed': 0, 'error': str(e)}

    if not business_numbers:
        return {'total': 0, 'closed': 0, 'error': None}

    # API 조회
    status_results = fetch_status_batch(business_numbers)

    # 업데이트 데이터 준비
    status_col = []
    date_col = []
    closed_count = 0

    for b_no in business_numbers:
        clean_no = str(b_no).replace("-", "").strip()
        info = status_results.get(clean_no, {'state': '확인불가', 'date': '-'})

        status_col.append([info['state']])
        date_col.append([info['date']])

        if '폐업' in info['state']:
            closed_count += 1

    # 시트 업데이트
    try:
        end_row = len(business_numbers) + 1
        worksheet.update(range_name=f'B2:B{end_row}', values=status_col)
        worksheet.update(range_name=f'C2:C{end_row}', values=date_col)
        print(f"  [{csp_name}] ✅ 완료 (조회: {len(business_numbers)}, 폐업: {closed_count})")
        return {'total': len(business_numbers), 'closed': closed_count, 'error': None}

    except Exception as e:
        print(f"  [{csp_name}] ❌ 시트 업데이트 실패: {e}")
        return {'total': len(business_numbers), 'closed': closed_count, 'error': str(e)}


def run():
    print("🔄 사업자 상태 조회 시작...")

    # 1. 문서 열기
    try:
        doc = get_document(SHEET_ID)
        worksheets = get_all_worksheets(doc)
    except Exception as e:
        notifier.send(status="실패", details=f"시트 연결 오류: {e}")
        return

    if not worksheets:
        print("처리할 워크시트가 없습니다.")
        return

    # 2. 모든 CSP(워크시트) 처리
    results = {}
    for csp_name, worksheet in worksheets:
        results[csp_name] = process_worksheet(csp_name, worksheet)

    # 3. 통합 알림 생성
    today_str = datetime.now().strftime("%Y-%m-%d")
    total_all = sum(r['total'] for r in results.values())
    closed_all = sum(r['closed'] for r in results.values())
    errors = [csp for csp, r in results.items() if r['error']]

    details_lines = [f"📅 {today_str}"]

    for csp_name, r in results.items():
        if r['error']:
            details_lines.append(f"• {csp_name}: ❌ 오류")
        else:
            details_lines.append(f"• {csp_name}: {r['total']}건 (폐업 {r['closed']})")

    details_lines.append(f"───────────")
    details_lines.append(f"합계: {total_all}건 조회, 폐업 {closed_all}건")

    status = "완료" if not errors else "일부 실패"
    notifier.send(status=status, details="\n".join(details_lines))


if __name__ == "__main__":
    if not API_KEY:
        print("API Key가 설정되지 않았습니다.")
        sys.exit(1)
    run()
