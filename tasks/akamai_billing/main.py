"""
Akamai Billing Pipeline

상태확인 → 수집 → 변환 → BigQuery 적재 통합 파이프라인
flow: clientId → accountSwitchKeys → contracts → products → monthly-summary usage
"""

import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from shared.notifier import Notifier
from shared.akamai_client import AkamaiClient, RateLimiter, extract_products
from shared.bigquery import (
    get_bq_client,
    upload_records,
    PRODUCT_USAGE_SCHEMA,
)

RATE_LIMIT_PER_MINUTE = 100
MAX_WORKERS = 10

notifier = Notifier(task_key="AKAMAI_BILLING", task_name="Akamai 빌링")


# ─── 빌링 월 계산 ───────────────────────────────────────────

def get_billing_month() -> str:
    """BILLING_MONTH 환경변수 또는 자동 전월 (YYYY-MM 형식)"""
    env_month = os.environ.get("BILLING_MONTH", "").strip()
    if env_month:
        return env_month

    now = datetime.now()
    if now.month == 1:
        return f"{now.year - 1}-12"
    return f"{now.year}-{now.month - 1:02d}"


# ─── 계정 목록 조회 ──────────────────────────────────────────

def fetch_account_switch_keys(client: AkamaiClient, client_id: str) -> List[Dict]:
    """
    clientId에 속한 accountSwitchKey 목록 조회

    Returns:
        [{'accountName': ..., 'accountSwitchKey': ...}, ...]
    """
    keys, err = client.get_account_switch_keys(client_id)
    if err or not keys:
        raise RuntimeError(f"accountSwitchKey 조회 실패: {err}")

    print(f"총 {len(keys)}개 계정 조회 완료")
    return keys


# ─── 단일 계정 처리 ─────────────────────────────────────────

def process_account(
    account: Dict,
    client: AkamaiClient,
    rate_limiter: RateLimiter,
    start: str,
    end: str,
) -> Dict:
    """
    단일 계정의 contracts → products → monthly-summary usage 수집

    Returns:
        {account_name, account_switch_key, success, product_usage, error}
    """
    acc_name = account["accountName"]
    acc_key = account["accountSwitchKey"]

    result = {
        "account_name": acc_name,
        "account_switch_key": acc_key,
        "success": False,
        "product_usage": {},
        "error": None,
    }

    # contracts 조회
    rate_limiter.acquire()
    contracts, err = client.get_contracts(acc_key)
    if err or contracts is None:
        result["error"] = f"계약 목록 조회 실패: {err}"
        return result

    for contract_id in contracts:
        # products 조회
        rate_limiter.acquire()
        products_data, _ = client.get_products(contract_id, acc_key, start, end)
        if not products_data:
            continue

        products = extract_products(products_data)

        for prod in products:
            prod_id = prod.get("productId")
            prod_name = prod.get("productName")
            if not prod_id:
                continue

            # monthly-summary usage 조회 (말일자 데이터)
            rate_limiter.acquire()
            usage, _ = client.get_product_usage_monthly(contract_id, acc_key, prod_id, start, end)

            if usage:
                key = f"{contract_id}_{prod_id}"
                result["product_usage"][key] = {
                    "accountName": acc_name,
                    "accountSwitchKey": acc_key,
                    "contractId": contract_id,
                    "productId": prod_id,
                    "productName": prod_name,
                    "data": usage,
                }

    result["success"] = True
    print(f"  → [{acc_name}] 완료!")
    return result


# ─── 전체 수집 ───────────────────────────────────────────────

def collect_all(
    client: AkamaiClient,
    accounts: List[Dict],
    start: str,
    end: str,
) -> Dict:
    """전체 계정 병렬 수집"""
    rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE)

    all_product_usage = {}
    failed = []
    success_count = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_account, acc, client, rate_limiter, start, end): acc
            for acc in accounts
        }

        for idx, future in enumerate(as_completed(futures), 1):
            account = futures[future]
            try:
                result = future.result()
                acc_name = result["account_name"]

                if result["success"]:
                    success_count += 1
                    all_product_usage.update(result["product_usage"])
                    print(f"[{idx}/{len(accounts)}] OK {acc_name}")
                else:
                    failed.append({
                        "account_name": acc_name,
                        "account_switch_key": result["account_switch_key"],
                        "reason": result.get("error"),
                    })
                    print(f"[{idx}/{len(accounts)}] FAIL {acc_name}: {result.get('error')}")

            except Exception as e:
                acc_name = account["accountName"]
                failed.append({
                    "account_name": acc_name,
                    "account_switch_key": account["accountSwitchKey"],
                    "reason": str(e),
                })
                print(f"[{idx}/{len(accounts)}] ERROR {acc_name}: {e}")

    return {
        "product_usage": all_product_usage,
        "success_count": success_count,
        "failed": failed,
    }


# ─── JSONL 변환 (인메모리) ───────────────────────────────────

def to_billing_date(billing_month: str) -> str:
    """YYYY-MM → YYYY-MM-01 (DATE 타입용)"""
    return f"{billing_month}-01"


def flatten_product_usage(raw_data: Dict, billing_month: str) -> List[Dict]:
    """
    monthly-summary usage 응답을 BigQuery용 JSONL 레코드로 변환.
    values 리스트에는 해당 월 말일자 데이터 하나만 포함.
    """
    records = []
    for record in raw_data.values():
        account_name = record.get("accountName")
        account_switch_key = record.get("accountSwitchKey")
        contract_id = record.get("contractId")
        product_id = record.get("productId")
        product_name = record.get("productName")

        data = record.get("data", {})
        request_date = data.get("requestDate")

        for period in data.get("usagePeriods", []):
            region = period.get("region")
            data_status = period.get("dataStatus")
            date = period.get("end")
            for stat in period.get("stats", []):
                stat_type = stat.get("statType")
                unit = stat.get("unit")
                is_billable = stat.get("isBillable")
                value = stat.get("value")
                records.append({
                    "billing_month": to_billing_date(billing_month),
                    "account_name": account_name,
                    "account_switch_key": account_switch_key,
                    "contract_id": contract_id,
                    "product_id": product_id,
                    "product_name": product_name,
                    "region": region,
                    "stat_type": stat_type,
                    "unit": unit,
                    "is_billable": is_billable,
                    "date": date,
                    "value": value,
                    "data_status": data_status,
                    "request_date": request_date,
                })
    return records


# ─── BigQuery 적재 ───────────────────────────────────────────

def upload_to_bigquery(
    product_usage: List[Dict],
    dataset_id: str,
    billing_month: str,
) -> Dict[str, int]:
    """product_usage 테이블 적재 (billing_month 월별 파티셔닝, 해당 월만 덮어쓰기)"""
    client = get_bq_client()
    result = {}

    partition_value = billing_month.replace("-", "")

    if product_usage:
        result["product_usage"] = upload_records(
            client, dataset_id, "product_usage", product_usage, PRODUCT_USAGE_SCHEMA,
            partition_field="billing_month", partition_value=partition_value
        )
        print(f"  product_usage: {result['product_usage']}행 적재")

    return result


# ─── 메인 파이프라인 ─────────────────────────────────────────

def main():
    start_time = time.time()

    try:
        # 1. 환경변수 로드
        billing_month = get_billing_month()
        year, mon = billing_month.split("-")
        next_month = f"{year}-{int(mon)+1:02d}" if int(mon) < 12 else f"{int(year)+1}-01"

        client_id = os.environ["AKAMAI_CLIENT_ID"]
        dataset_id = os.environ.get("BQ_DATASET", "akamai_billing")

        print("=" * 60)
        print("Akamai Billing Pipeline")
        print(f"Billing Month: {billing_month}  (start={billing_month}, end={next_month})")
        print("=" * 60)

        # 2. Akamai 클라이언트 초기화
        akamai_client = AkamaiClient(
            client_token=os.environ["AKAMAI_CLIENT_TOKEN"],
            client_secret=os.environ["AKAMAI_CLIENT_SECRET"],
            access_token=os.environ["AKAMAI_ACCESS_TOKEN"],
            base_url=os.environ["AKAMAI_BASE_URL"],
        )

        # 3. accountSwitchKey 목록 조회
        print("\n[1/4] 계정 목록 조회")
        accounts = fetch_account_switch_keys(akamai_client, client_id)

        if not accounts:
            notifier.send("실패", "계정 목록이 비어있습니다.")
            return

        # 4. 전체 데이터 수집 (계정별 병렬)
        # monthly-summary 엔드포인트는 COLLECTING_DATA 상태가 없으므로 별도 상태 확인 불필요
        print(f"\n[2/4] 데이터 수집 ({len(accounts)}개 계정)")
        collected = collect_all(akamai_client, accounts, billing_month, next_month)

        success = collected["success_count"]
        failed = collected["failed"]
        print(f"\n수집 완료: 성공 {success}, 실패 {len(failed)}")

        # 5. JSONL 변환 (인메모리)
        print("\n[3/4] 데이터 변환")
        usage_flat = flatten_product_usage(collected["product_usage"], billing_month)
        print(f"  product_usage: {len(usage_flat)}건")

        # 6. BigQuery 적재
        print("\n[4/4] BigQuery 적재")
        bq_result = upload_to_bigquery(usage_flat, dataset_id, billing_month)

        # 8. 완료 알림
        duration = time.time() - start_time
        summary = (
            f"{billing_month}\n"
            f"계정: {success}/{len(accounts)} (실패 {len(failed)})\n"
            f"product_usage: {bq_result.get('product_usage', 0)}행\n"
            f"소요: {duration:.0f}초"
        )
        notifier.send("완료", summary)

        print(f"\n{'=' * 60}")
        print(f"파이프라인 완료 ({duration:.0f}초)")
        print(f"{'=' * 60}")

    except Exception as e:
        duration = time.time() - start_time
        error_msg = f"{type(e).__name__}: {e}"
        print(f"\n파이프라인 실패: {error_msg}")
        notifier.send("실패", f"{error_msg}\n소요: {duration:.0f}초")
        raise


if __name__ == "__main__":
    main()
