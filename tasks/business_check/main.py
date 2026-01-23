import os
import sys
import json
import requests
from datetime import datetime

from shared.sheets import get_connection
from shared.notifier import Notifier

SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
API_KEY = os.environ.get("NTS_API_KEY")
NTS_API_URL = f"https://api.odcloud.kr/api/nts-businessman/v1/status?serviceKey={API_KEY}"

notifier = Notifier(task_key="BUSINESS", task_name="사업자 상태 점검")

def fetch_status_batch(b_no_list):
    """국세청 API로 100개씩 상태 조회"""
    results = {}
    
    # 100개씩 청크 분할
    for i in range(0, len(b_no_list), 100):
        chunk = b_no_list[i:i+100]
        # 하이픈 제거 및 공백 제거
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
                    # 결과 매핑: b_no -> {state: ..., date: ...}
                    state = item['b_stt'] # 계속/휴업/폐업
                    date = item.get('end_dt', '') # 폐업일자
                    
                    # 계속사업자인데 날짜가 없으면 '운영중' 표시
                    if not date and item['b_stt_cd'] == '01':
                         date = "운영중"
                    
                    results[item['b_no']] = {'state': state, 'date': date}
            else:
                print(f"API Error ({resp.status_code}): {resp.text}")
                
        except Exception as e:
            print(f"Batch processing error: {e}")
            
    return results

def run():
    print("🔄 사업자 상태 조회 시작...")
    
    # URL 조합
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    
    print("🔄 사업자 상태 조회 시작...")

    # 1. 시트 데이터 가져오기
    try:
        sheet = get_connection(sheet_url)
        business_numbers = sheet.col_values(1)[1:]  # A열, 헤더 제외
    except Exception as e:
        notifier.send(status="실패", details=f"시트 연결 오류: {e}")
        return

    if not business_numbers:
        print("조회할 사업자 번호가 없습니다.")
        return

    # 2. API 조회
    status_results = fetch_status_batch(business_numbers)
    
    # 3. 업데이트 데이터 준비 (B열: 상태, C열: 일자)
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

    # 4. 시트 업데이트 (B2, C2부터 시작)
    # 데이터 행 개수만큼 범위 지정
    end_row = len(business_numbers) + 1
    
    try:
        sheet.update(range_name=f'B2:B{end_row}', values=status_col)
        sheet.update(range_name=f'C2:C{end_row}', values=date_col)
        print("✅ 시트 업데이트 완료")

        today_str = datetime.now().strftime("%Y-%m-%d")
        details = (f"📅 {today_str}\n"
                   f"🔍 총 조회: {len(business_numbers)}건\n"
                   f"❌ 폐업: {closed_count}건")
        notifier.send(status="완료", details=details)

    except Exception as e:
        print(f"시트 업데이트 실패: {e}")
        notifier.send(status="실패", details=f"시트 업데이트 오류: {e}")

if __name__ == "__main__":
    if not API_KEY:
        print("API Key가 설정되지 않았습니다.")
        sys.exit(1)
    run()