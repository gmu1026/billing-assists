"""
Akamai Billing API 클라이언트 + RateLimiter

EdgeGrid 인증을 사용하여 Akamai Billing API를 호출합니다.
환경변수는 호출자(main.py)에서 주입합니다.
"""

import time
import threading
import requests
from requests.adapters import HTTPAdapter
from akamai.edgegrid import EdgeGridAuth
from typing import Any, Dict, List, Optional, Tuple


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
        # 동시 요청 수에 맞게 커넥션 풀 확장
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        self.session.mount("https://", adapter)
        self.base_url = base_url.rstrip("/")

    def _make_request(self, path: str, params: Dict) -> Tuple[Optional[Any], Optional[str]]:
        """
        API 요청 실행

        Returns:
            (data, error_message) 튜플. data는 dict 또는 list.
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

    def get_account_switch_keys(self, client_id: str) -> Tuple[Optional[List], Optional[str]]:
        """clientId에 속한 accountSwitchKey 목록 조회"""
        path = f"/identity-management/v3/api-clients/{client_id}/account-switch-keys"
        return self._make_request(path, {})

    def get_contracts(self, account_switch_key: str) -> Tuple[Optional[List], Optional[str]]:
        """accountSwitchKey에 속한 계약 ID 목록 조회 (문자열 리스트 반환)"""
        path = "/contract-api/v1/contracts/identifiers"
        return self._make_request(path, {"accountSwitchKey": account_switch_key})

    def get_products(
        self, contract_id: str, account_switch_key: str, start: str, end: str
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """계약의 Product 목록 조회"""
        path = f"/billing/v1/contracts/{contract_id}/products"
        params = {"accountSwitchKey": account_switch_key, "start": start, "end": end}
        return self._make_request(path, params)

    def get_product_usage_monthly(
        self, contract_id: str, account_switch_key: str, product_id: str, start: str, end: str
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """Product monthly-summary 사용량 조회 (말일자 데이터)"""
        path = f"/billing/v1/contracts/{contract_id}/products/{product_id}/usage/monthly-summary"
        params = {"accountSwitchKey": account_switch_key, "start": start, "end": end}
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
