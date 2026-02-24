"""
BigQuery 업로드 유틸리티

GCP_SA_KEY 환경변수로 인증하여 BigQuery에 데이터를 적재합니다.
"""

import os
import json
from typing import List, Dict

from google.cloud import bigquery
from google.oauth2 import service_account


def get_bq_client() -> bigquery.Client:
    """BigQuery 클라이언트 반환 (BQ_SA_KEY 우선, 없으면 GCP_SA_KEY 사용)"""
    json_key_string = os.environ.get("BQ_SA_KEY") or os.environ.get("GCP_SA_KEY")
    if not json_key_string:
        raise ValueError("BQ_SA_KEY 또는 GCP_SA_KEY 환경변수가 설정되지 않았습니다.")

    credentials_dict = json.loads(json_key_string)
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)

    # BQ_PROJECT 환경변수로 프로젝트 오버라이드 가능
    project_id = os.environ.get("BQ_PROJECT") or credentials_dict.get("project_id")

    return bigquery.Client(credentials=credentials, project=project_id)


def upload_records(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    records: List[Dict],
    schema: List[bigquery.SchemaField],
    write_disposition: str = "WRITE_TRUNCATE",
    partition_field: str = None,
    partition_value: str = None,
) -> int:
    """
    레코드를 BigQuery 테이블에 적재

    Args:
        client: BigQuery 클라이언트
        dataset_id: 데이터셋 ID
        table_id: 테이블 ID
        records: 적재할 레코드 리스트
        schema: BigQuery 스키마
        write_disposition: 쓰기 모드 (WRITE_TRUNCATE, WRITE_APPEND 등)
        partition_field: 파티션 필드명 (월별 파티셔닝, DATE 타입 필드)
        partition_value: 파티션 값 (YYYYMM 형식, 해당 파티션만 덮어쓰기)

    Returns:
        적재된 행 수
    """
    if not records:
        return 0

    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    # 특정 월 파티션만 덮어쓰기: 해당 월 DML 삭제 후 APPEND
    # (파티션 데코레이터 대신 DELETE+APPEND 방식 → 테이블 파티션 설정과 무관하게 동작)
    if partition_value and partition_field:
        billing_date = f"{partition_value[:4]}-{partition_value[4:6]}-01"
        delete_sql = (
            f"DELETE FROM `{full_table_id}` "
            f"WHERE DATE_TRUNC(`{partition_field}`, MONTH) = DATE '{billing_date}'"
        )
        try:
            client.query(delete_sql).result()
        except Exception:
            pass  # 테이블 미존재 시 삭제 스킵
        write_disposition = "WRITE_APPEND"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(records, full_table_id, job_config=job_config)
    load_job.result()  # 완료 대기

    return load_job.output_rows


# ─── 스키마 상수 ─────────────────────────────────────────────

PRODUCT_USAGE_SCHEMA = [
    bigquery.SchemaField("billing_month", "DATE"),
    bigquery.SchemaField("account_name", "STRING"),
    bigquery.SchemaField("account_switch_key", "STRING"),
    bigquery.SchemaField("contract_id", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("stat_type", "STRING"),
    bigquery.SchemaField("unit", "STRING"),
    bigquery.SchemaField("is_billable", "BOOLEAN"),
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("value", "FLOAT64"),
    bigquery.SchemaField("data_status", "STRING"),
    bigquery.SchemaField("request_date", "TIMESTAMP"),
]
