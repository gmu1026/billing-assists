import os
import json
import requests

def send_message(message):
    """
    환경변수 WEBHOOK_URL로 메시지를 전송합니다.
    (Google Chat 호환성 수정 버전)
    """
    webhook_url = os.environ.get("WEBHOOK_URL")
    
    if not webhook_url:
        print("[Warning] WEBHOOK_URL이 없어 알림을 보내지 못했습니다.")
        return

    headers = {"Content-Type": "application/json; charset=UTF-8"}
    
    # [수정됨] Google Chat은 오직 'text' 필드만 허용합니다.
    # 다른 필드(body, content 등)가 섞여 있으면 400 에러가 발생합니다.
    payload = {
        "text": message
    }

    try:
        resp = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
        
        # Google Chat 등은 성공 시 200 OK를 반환
        if resp.status_code >= 400:
            print(f"[Error] 알림 전송 실패: {resp.status_code} {resp.text}")
        else:
            print("✅ 알림 전송 성공")
            
    except Exception as e:
        print(f"[Error] 알림 전송 중 예외 발생: {e}")