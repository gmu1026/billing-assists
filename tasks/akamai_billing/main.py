"""
Akamai Billing Pipeline

상태확인 → 수집 → 변환 → BigQuery 적재 통합 파이프라인
"""

import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

from shared.notifier import Notifier
from shared.hb_client import HBApiClient
from shared.akamai_client import AkamaiClient, RateLimiter, extract_products
from shared.bigquery import (
    get_bq_client,
    upload_records,
    PRODUCT_USAGE_SCHEMA,
    REPORTING_GROUP_USAGE_SCHEMA,
    PRODUCTS_SCHEMA,
)

RATE_LIMIT_PER_MINUTE = 100
MAX_CONCURRENT_REQUESTS = 20

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


# ─── 계약 목록 조회 ─────────────────────────────────────────

def fetch_akamai_contracts(cookie: str) -> List[Dict]:
    """
    HyperBilling에서 Akamai 계약 목록 조회

    Returns:
        [{'contract_id', 'account_id', 'company_name', 'seq'}]
    """
    client = HBApiClient("akamai", cookie)
    response = client.fetch("contract", {"reseller_seq": 0})

    contracts = []
    for contract in response.get("data", []):
        if not contract.get("enabled"):
            continue

        company_name = contract.get("name", "")

        for account in contract.get("accounts", []):
            contract_id = account.get("contract_id")
            account_id = account.get("account_id")

            if contract_id and account_id:
                contracts.append({
                    "contract_id": contract_id,
                    "account_id": account_id,
                    "company_name": company_name,
                    "seq": contract.get("seq"),
                })

    print(f"총 {len(contracts)}개 계약 조회 완료")
    return contracts


# ─── 상태 확인 ───────────────────────────────────────────────

def check_data_status(
    client: AkamaiClient,
    rate_limiter: RateLimiter,
    contracts: List[Dict],
    billing_month: str,
    next_month: str,
) -> Tuple[bool, str]:
    """
    샘플 계약으로 데이터 수집 상태 확인

    Returns:
        (is_ready, report_text)
    """
    # 샘플 계약 선정 (균등 분포, 최대 5개)
    n = len(contracts)
    sample_count = min(5, n)
    step = max(1, n // sample_count)
    samples = [contracts[i * step] for i in range(sample_count)]

    statuses: Dict[str, int] = {}
    details: List[str] = []

    for contract in samples:
        cid = contract["contract_id"]
        aid = contract["account_id"]
        name = contract["company_name"]

        rate_limiter.acquire()
        products_data, err = client.get_products(cid, aid, billing_month, next_month)

        if err or not products_data:
            details.append(f"  {cid} ({name}): 조회 실패 - {err}")
            continue

        products = extract_products(products_data)
        if not products:
            details.append(f"  {cid} ({name}): Product 없음")
            continue

        # 첫 번째 product의 usage dataStatus 확인
        first_product = products[0]
        pid = first_product.get("productId")

        rate_limiter.acquire()
        usage_data, usage_err = client.get_product_usage(cid, aid, pid, billing_month)

        if usage_err or not usage_data:
            details.append(f"  {cid} ({name}): Usage 조회 실패 - {usage_err}")
            continue

        status = usage_data.get("dataStatus", "UNKNOWN")
        statuses[status] = statuses.get(status, 0) + 1
        details.append(f"  {cid} ({name}): {status}")

    collecting = statuses.get("COLLECTING_DATA", 0)
    is_ready = collecting == 0 and len(statuses) > 0

    status_lines = "\n".join(f"  {k}: {v}건" for k, v in sorted(statuses.items()))
    detail_lines = "\n".join(details)
    report = f"샘플 {sample_count}개 상태:\n{status_lines}\n\n{detail_lines}"

    return is_ready, report


# ─── 단일 계약 처리 ─────────────────────────────────────────

def process_contract(
    contract: Dict,
    client: AkamaiClient,
    rate_limiter: RateLimiter,
    month: str,
    next_month: str,
) -> Dict:
    """단일 계약의 products, product_usage, reporting_group_usage 수집"""
    contract_id = contract["contract_id"]
    account_id = contract["account_id"]
    company_name = contract["company_name"]

    result = {
        "contract_id": contract_id,
        "account_id": account_id,
        "company_name": company_name,
        "success": False,
        "products": {},
        "product_usage": {},
        "reporting_group_usage": {},
    }

    # Product 목록 조회
    rate_limiter.acquire()
    products_data, error = client.get_products(contract_id, account_id, month, next_month)

    if error:
        result["error"] = f"Product 목록 조회 실패: {error}"
        return result

    if not products_data:
        result["error"] = "Product 데이터 없음"
        return result

    result["products"] = products_data
    products = extract_products(products_data)

    if not products:
        result["success"] = True
        result["error"] = "사용 중인 Product 없음"
        return result

    # 각 Product별 사용량 + Reporting Group 사용량 조회
    for product in products:
        product_id = product.get("productId")
        product_name = product.get("productName", "Unknown")
        if not product_id:
            continue

        rate_limiter.acquire()
        usage_data, _ = client.get_product_usage(contract_id, account_id, product_id, month)

        if usage_data:
            key = f"{contract_id}_{product_id}"
            result["product_usage"][key] = {
                "contractId": contract_id,
                "accountId": account_id,
                "companyName": company_name,
                "productId": product_id,
                "productName": product_name,
                "data": usage_data,
            }

        for rg in product.get("reportingGroups", []):
            rg_id = rg.get("reportingGroupId")
            rg_name = rg.get("reportingGroupName", "Unknown")
            if not rg_id:
                continue

            rate_limiter.acquire()
            rg_data, _ = client.get_reporting_group_usage(account_id, rg_id, product_id, month)

            if rg_data:
                key = f"{contract_id}_{product_id}_{rg_id}"
                result["reporting_group_usage"][key] = {
                    "contractId": contract_id,
                    "accountId": account_id,
                    "companyName": company_name,
                    "productId": product_id,
                    "productName": product_name,
                    "reportingGroupId": rg_id,
                    "reportingGroupName": rg_name,
                    "data": rg_data,
                }

    result["success"] = True
    return result


# ─── 전체 수집 ───────────────────────────────────────────────

def collect_all(
    client: AkamaiClient,
    contracts: List[Dict],
    billing_month: str,
    next_month: str,
) -> Dict:
    """전체 계약 병렬 수집"""
    rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE)

    all_products = {}
    all_product_usage = {}
    all_rg_usage = {}
    failed = []
    success_count = 0

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
        futures = {
            executor.submit(
                process_contract, c, client, rate_limiter, billing_month, next_month
            ): c
            for c in contracts
        }

        for idx, future in enumerate(as_completed(futures), 1):
            contract = futures[future]
            try:
                result = future.result()
                cid = result["contract_id"]
                name = result["company_name"]

                if result["success"]:
                    success_count += 1

                    if result["products"]:
                        all_products[cid] = {
                            "accountId": result["account_id"],
                            "companyName": name,
                            "data": result["products"],
                        }

                    all_product_usage.update(result["product_usage"])
                    all_rg_usage.update(result["reporting_group_usage"])

                    print(f"[{idx}/{len(contracts)}] OK {cid} ({name})")
                else:
                    failed.append({"contract_id": cid, "company_name": name, "reason": result.get("error")})
                    print(f"[{idx}/{len(contracts)}] FAIL {cid} ({name}): {result.get('error')}")

            except Exception as e:
                cid = contract["contract_id"]
                failed.append({"contract_id": cid, "company_name": contract["company_name"], "reason": str(e)})
                print(f"[{idx}/{len(contracts)}] ERROR {cid}: {e}")

    return {
        "products": all_products,
        "product_usage": all_product_usage,
        "reporting_group_usage": all_rg_usage,
        "success_count": success_count,
        "failed": failed,
    }


# ─── JSONL 변환 (인메모리) ───────────────────────────────────

def flatten_product_usage(raw_data: Dict, billing_month: str) -> List[Dict]:
    records = []
    for record in raw_data.values():
        contract_id = record.get("contractId")
        account_id = record.get("accountId")
        company_name = record.get("companyName")
        product_id = record.get("productId")
        product_name = record.get("productName")

        data = record.get("data", {})
        data_status = data.get("dataStatus")
        request_date = data.get("requestDate")

        for period in data.get("usagePeriods", []):
            region = period.get("region")
            for stat in period.get("stats", []):
                stat_type = stat.get("statType")
                unit = stat.get("unit")
                is_billable = stat.get("isBillable")
                for v in stat.get("values", []):
                    records.append({
                        "billing_month": billing_month,
                        "contract_id": contract_id,
                        "account_id": account_id,
                        "company_name": company_name,
                        "product_id": product_id,
                        "product_name": product_name,
                        "region": region,
                        "stat_type": stat_type,
                        "unit": unit,
                        "is_billable": is_billable,
                        "date": v.get("date"),
                        "value": v.get("value"),
                        "data_status": data_status,
                        "request_date": request_date,
                    })
    return records


def flatten_reporting_group_usage(raw_data: Dict, billing_month: str) -> List[Dict]:
    records = []
    for record in raw_data.values():
        contract_id = record.get("contractId")
        account_id = record.get("accountId")
        company_name = record.get("companyName")
        product_id = record.get("productId")
        product_name = record.get("productName")
        rg_id = record.get("reportingGroupId")
        rg_name = record.get("reportingGroupName")

        data = record.get("data", {})
        data_status = data.get("dataStatus")
        request_date = data.get("requestDate")

        for period in data.get("usagePeriods", []):
            region = period.get("region")
            for stat in period.get("stats", []):
                stat_type = stat.get("statType")
                unit = stat.get("unit")
                is_billable = stat.get("isBillable")
                for v in stat.get("values", []):
                    records.append({
                        "billing_month": billing_month,
                        "contract_id": contract_id,
                        "account_id": account_id,
                        "company_name": company_name,
                        "product_id": product_id,
                        "product_name": product_name,
                        "reporting_group_id": rg_id,
                        "reporting_group_name": rg_name,
                        "region": region,
                        "stat_type": stat_type,
                        "unit": unit,
                        "is_billable": is_billable,
                        "date": v.get("date"),
                        "value": v.get("value"),
                        "data_status": data_status,
                        "request_date": request_date,
                    })
    return records


def flatten_products(raw_data: Dict, billing_month: str) -> List[Dict]:
    records = []
    for contract_id, record in raw_data.items():
        account_id = record.get("accountId")
        company_name = record.get("companyName")

        data = record.get("data", {})
        request_date = data.get("requestDate")
        start = data.get("start")
        end = data.get("end")

        for period in data.get("usagePeriods", []):
            month = period.get("month")
            for product in period.get("usageProducts", []):
                product_id = product.get("productId")
                product_name = product.get("productName")
                reporting_groups = product.get("reportingGroups", [])

                if reporting_groups:
                    for rg in reporting_groups:
                        records.append({
                            "billing_month": billing_month,
                            "contract_id": contract_id,
                            "account_id": account_id,
                            "company_name": company_name,
                            "product_id": product_id,
                            "product_name": product_name,
                            "reporting_group_id": rg.get("reportingGroupId"),
                            "reporting_group_name": rg.get("reportingGroupName"),
                            "month": month,
                            "start": start,
                            "end": end,
                            "request_date": request_date,
                        })
                else:
                    records.append({
                        "billing_month": billing_month,
                        "contract_id": contract_id,
                        "account_id": account_id,
                        "company_name": company_name,
                        "product_id": product_id,
                        "product_name": product_name,
                        "reporting_group_id": None,
                        "reporting_group_name": None,
                        "month": month,
                        "start": start,
                        "end": end,
                        "request_date": request_date,
                    })
    return records


# ─── BigQuery 적재 ───────────────────────────────────────────

def upload_to_bigquery(
    products: List[Dict],
    product_usage: List[Dict],
    rg_usage: List[Dict],
    dataset_id: str,
) -> Dict[str, int]:
    """3개 테이블 순차 적재"""
    client = get_bq_client()
    result = {}

    if products:
        result["products"] = upload_records(client, dataset_id, "products", products, PRODUCTS_SCHEMA)
        print(f"  products: {result['products']}행 적재")

    if product_usage:
        result["product_usage"] = upload_records(
            client, dataset_id, "product_usage", product_usage, PRODUCT_USAGE_SCHEMA
        )
        print(f"  product_usage: {result['product_usage']}행 적재")

    if rg_usage:
        result["reporting_group_usage"] = upload_records(
            client, dataset_id, "reporting_group_usage", rg_usage, REPORTING_GROUP_USAGE_SCHEMA
        )
        print(f"  reporting_group_usage: {result['reporting_group_usage']}행 적재")

    return result


# ─── 메인 파이프라인 ─────────────────────────────────────────

def main():
    start_time = time.time()

    try:
        # 1. 환경변수 로드
        billing_month = get_billing_month()
        year, mon = billing_month.split("-")
        next_month = f"{year}-{int(mon)+1:02d}" if int(mon) < 12 else f"{int(year)+1}-01"

        akamai_cookie = os.environ.get("AKAMAI_COOKIE", "")
        dataset_id = os.environ.get("BQ_DATASET", "akamai_billing")

        print("=" * 60)
        print(f"Akamai Billing Pipeline")
        print(f"Billing Month: {billing_month}")
        print("=" * 60)

        # 2. HyperBilling에서 계약 목록 조회
        print("\n[1/5] 계약 목록 조회")
        contracts = fetch_akamai_contracts(akamai_cookie)

        if not contracts:
            notifier.send("실패", "계약 목록이 비어있습니다.")
            return

        # 3. Akamai 클라이언트 초기화
        akamai_client = AkamaiClient(
            client_token=os.environ["AKAMAI_CLIENT_TOKEN"],
            client_secret=os.environ["AKAMAI_CLIENT_SECRET"],
            access_token=os.environ["AKAMAI_ACCESS_TOKEN"],
            base_url=os.environ["AKAMAI_BASE_URL"],
        )

        # 4. 상태 확인
        print("\n[2/5] 데이터 상태 확인")
        status_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE)
        is_ready, status_report = check_data_status(
            akamai_client, status_limiter, contracts, billing_month, next_month
        )

        if not is_ready:
            notifier.send("수집 중", f"{billing_month}\n{status_report}")
            print(f"\n데이터 수집 중 — 파이프라인 종료")
            return

        notifier.send("수집 완료", f"{billing_month} — 데이터 수집 시작\n{status_report}")

        # 5. 전체 데이터 수집
        print(f"\n[3/5] 데이터 수집 ({len(contracts)}개 계약)")
        collected = collect_all(akamai_client, contracts, billing_month, next_month)

        success = collected["success_count"]
        failed = collected["failed"]
        print(f"\n수집 완료: 성공 {success}, 실패 {len(failed)}")

        # 6. JSONL 변환 (인메모리)
        print("\n[4/5] 데이터 변환")
        products_flat = flatten_products(collected["products"], billing_month)
        usage_flat = flatten_product_usage(collected["product_usage"], billing_month)
        rg_flat = flatten_reporting_group_usage(collected["reporting_group_usage"], billing_month)
        print(f"  products: {len(products_flat)}건, product_usage: {len(usage_flat)}건, rg_usage: {len(rg_flat)}건")

        # 7. BigQuery 적재
        print("\n[5/5] BigQuery 적재")
        bq_result = upload_to_bigquery(products_flat, usage_flat, rg_flat, dataset_id)

        # 8. 완료 알림
        duration = time.time() - start_time
        summary = (
            f"{billing_month}\n"
            f"계약: {success}/{len(contracts)} (실패 {len(failed)})\n"
            f"products: {bq_result.get('products', 0)}행\n"
            f"product_usage: {bq_result.get('product_usage', 0)}행\n"
            f"rg_usage: {bq_result.get('reporting_group_usage', 0)}행\n"
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
