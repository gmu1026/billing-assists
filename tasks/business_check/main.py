import os
import sys
import json
import requests
import time
from datetime import datetime

# shared 모듈을 불러오기 위한 임포트 (GitHub Actions의 PYTHONPATH 설정에 의존)
from shared.sheets import get_connection
from shared.notifier import send_message

# --- 설정 ---
# 시트 ID는 환경변수로 관리하거나 여기에 직접 적어도 무방(공개 repo가 아니라면)
SHEET_URL = 'https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID_HERE' 
API_KEY = os.environ.get("NTS_API_KEY")
NTS_API_URL = f"https://api.odcloud.kr/api/nts-businessman/v1/status?serviceKey={API_KEY}"

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
    
    # 1. 시트 데이터 가져오기
    try:
        sheet = get_connection(SHEET_URL)
        # 1열(A열) 사업자 번호 가져오기 (헤더 제외 2행부터)
        business_numbers = sheet.col_values(1)[1:]
    except Exception as e:
        send_message(f"🚨 [오류] 시트 연결 실패: {e}")
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
        
        # 5. 결과 알림
        today_str = datetime.now().strftime("%Y-%m-%d")
        msg = (f"📅 [{today_str}] 사업자 상태 점검 완료\n"
               f"🔍 총 조회: {len(business_numbers)}건\n"
               f"❌ 폐업 확인: {closed_count}건\n"
               f"✅ 구글 시트가 최신 상태로 갱신되었습니다.")
        send_message(msg)
        
    except Exception as e:
        err_msg = f"🚨 [오류] 시트 업데이트 중 실패: {e}"
        print(err_msg)
        send_message(err_msg)

if __name__ == "__main__":
    if not API_KEY:
        print("API Key가 설정되지 않았습니다.")
        sys.exit(1)
    run()