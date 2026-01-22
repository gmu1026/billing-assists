import os
import json
import requests

def send_message(message):
    """
    환경변수 WEBHOOK_URL로 메시지를 전송합니다.
    """
    webhook_url = os.environ.get("WEBHOOK_URL")
    
    if not webhook_url:
        print("[Warning] WEBHOOK_URL이 없어 알림을 보내지 못했습니다.")
        return

    headers = {"Content-Type": "application/json"}
    
    # 대부분의 메신저(Slack, Jandi, Discord 등) 호환을 위한 페이로드
    payload = {
        "body": message,      # Jandi
        "text": message,      # Slack
        "content": message    # Discord
    }

    try:
        resp = requests.post(webhook_url, headers=headers, data=json.dumps(payload))
        if resp.status_code >= 400:
            print(f"[Error] 알림 전송 실패: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[Error] 알림 전송 중 예외 발생: {e}")