import os
import requests


class Notifier:
    """
    Google Chat 웹훅으로 알림을 전송하는 클래스.

    사용법:
        notifier = Notifier(task_key="BUSINESS", task_name="사업자 상태 점검")
        notifier.send(status="성공", details="총 100건 조회, 폐업 2건")
        notifier.send(status="실패", details="API 연결 오류")

    환경변수:
        WEBHOOK_URL_{task_key} 형식으로 설정 (예: WEBHOOK_URL_BUSINESS)
    """

    def __init__(self, task_key: str, task_name: str):
        """
        Args:
            task_key: 환경변수 접미사 (예: "BUSINESS" -> WEBHOOK_URL_BUSINESS)
            task_name: 메시지에 표시될 작업명 (예: "사업자 상태 점검")
        """
        self.task_key = task_key.upper()
        self.task_name = task_name
        self.webhook_url = os.environ.get(f"WEBHOOK_URL_{self.task_key}")

    def send(self, status: str, details: str = "") -> bool:
        """
        알림을 전송합니다.

        Args:
            status: 작업 상태 (예: "성공", "실패", "완료")
            details: 상세 내용

        Returns:
            전송 성공 여부
        """
        if not self.webhook_url:
            print(f"[Warning] WEBHOOK_URL_{self.task_key} 환경변수가 설정되지 않았습니다.")
            return False

        # 메시지 포맷: [작업명] 상태\n상세내용
        message = f"[{self.task_name}] {status}"
        if details:
            message += f"\n{details}"

        payload = {"text": message}

        try:
            resp = requests.post(self.webhook_url, json=payload)

            if resp.status_code >= 400:
                print(f"[Error] 알림 전송 실패: {resp.status_code} {resp.text}")
                return False

            print(f"[{self.task_name}] 알림 전송 성공")
            return True

        except Exception as e:
            print(f"[Error] 알림 전송 중 예외 발생: {e}")
            return False
