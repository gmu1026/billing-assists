"""
Akamai Billing API 클라이언트 + RateLimiter

EdgeGrid 인증을 사용하여 Akamai Billing API를 호출합니다.
환경변수는 호출자(main.py)에서 주입합니다.
"""

import time
import threading
import requests
from akamai.edgegrid import EdgeGridAuth
from typing import Dict, List, Optional, Tuple


class RateLimiter:
    """분당 요청 수 제한 (thread-safe)"""

    def __init__(self, max_per_minute: int):
        self.max_per_minute = max_per_minute
        self.request_times: list[float] = []
        self._lock = threading.Lock()

    def acquire(self):
        """요청 허가 획득"""
        while True:
            with self._lock:
                now = time.time()
                self.request_times = [t for t in self.request_times if now - t < 60]

                if len(self.request_times) < self.max_per_minute:
                    self.request_times.append(now)
                    return

                sleep_time = 60 - (now - self.request_times[0])

            # 락 밖에서 대기 → 다른 스레드 차단하지 않음
            if sleep_time > 0:
                time.sleep(sleep_time)


class AkamaiClient:
    """Akamai Billing API 클라이언트"""

    def __init__(self, client_token: str, client_secret: str, access_token: str, base_url: str):
        if not base_url:
            raise ValueError("AKAMAI_BASE_URL이 비어있습니다. GitHub Secret을 확인하세요.")

        self.session = requests.Session()
        self.session.auth = EdgeGridAuth(
            client_token=client_token,
            client_secret=client_secret,
            access_token=access_token,
        )
        self.base_url = base_url.rstrip("/")

    def _make_request(self, path: str, params: Dict) -> Tuple[Optional[Dict], Optional[str]]:
        """
        API 요청 실행

        Returns:
            (data, error_message) 튜플
        """
        try:
            response = self.session.get(
                self.base_url + path,
                params=params,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code == 200:
                return response.json(), None
            else:
                return None, f"{response.status_code} - {response.text[:200]}"

        except Exception as e:
            return None, f"Exception: {str(e)}"

    def get_products(
        self, contract_id: str, account_id: str, start: str, end: str
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """계약의 Product 목록 조회"""
        path = f"/billing/v1/contracts/{contract_id}/products"
        params = {"accountSwitchKey": account_id, "start": start, "end": end}
        return self._make_request(path, params)

    def get_product_usage(
        self, contract_id: str, account_id: str, product_id: str, month: str
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """Product 일별 사용량 조회"""
        path = f"/billing/v1/contracts/{contract_id}/products/{product_id}/usage/daily"
        params = {"accountSwitchKey": account_id, "month": month}
        return self._make_request(path, params)

    def get_reporting_group_usage(
        self, account_id: str, reporting_group_id: str, product_id: str, month: str
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """Reporting Group 일별 사용량 조회"""
        path = f"/billing/v1/reporting-groups/{reporting_group_id}/products/{product_id}/usage/daily"
        params = {"accountSwitchKey": account_id, "month": month}
        return self._make_request(path, params)


def extract_products(products_data: Dict) -> List[Dict]:
    """API 응답에서 product 목록을 추출"""
    products = []

    if "products" in products_data:
        products = products_data["products"]
    elif "usagePeriods" in products_data:
        for period in products_data["usagePeriods"]:
            usage_products = period.get("usageProducts", [])
            products.extend(usage_products)

    return products
