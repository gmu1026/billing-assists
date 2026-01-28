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
    """GCP_SA_KEY JSON으로 인증된 BigQuery 클라이언트 반환"""
    json_key_string = os.environ.get("GCP_SA_KEY")
    if not json_key_string:
        raise ValueError("GCP_SA_KEY 환경변수가 설정되지 않았습니다.")

    credentials_dict = json.loads(json_key_string)
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    project_id = credentials_dict.get("project_id")

    return bigquery.Client(credentials=credentials, project=project_id)


def upload_records(
    client: bigquery.Client,
    dataset_id: str,
    table_id: str,
    records: List[Dict],
    schema: List[bigquery.SchemaField],
    write_disposition: str = "WRITE_TRUNCATE",
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

    Returns:
        적재된 행 수
    """
    if not records:
        return 0

    table_ref = f"{client.project}.{dataset_id}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(records, table_ref, job_config=job_config)
    load_job.result()  # 완료 대기

    return load_job.output_rows


# ─── 스키마 상수 ─────────────────────────────────────────────

PRODUCT_USAGE_SCHEMA = [
    bigquery.SchemaField("billing_month", "STRING"),
    bigquery.SchemaField("contract_id", "STRING"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("company_name", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("stat_type", "STRING"),
    bigquery.SchemaField("unit", "STRING"),
    bigquery.SchemaField("is_billable", "BOOLEAN"),
    bigquery.SchemaField("date", "STRING"),
    bigquery.SchemaField("value", "FLOAT64"),
    bigquery.SchemaField("data_status", "STRING"),
    bigquery.SchemaField("request_date", "STRING"),
]

REPORTING_GROUP_USAGE_SCHEMA = [
    bigquery.SchemaField("billing_month", "STRING"),
    bigquery.SchemaField("contract_id", "STRING"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("company_name", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("reporting_group_id", "STRING"),
    bigquery.SchemaField("reporting_group_name", "STRING"),
    bigquery.SchemaField("region", "STRING"),
    bigquery.SchemaField("stat_type", "STRING"),
    bigquery.SchemaField("unit", "STRING"),
    bigquery.SchemaField("is_billable", "BOOLEAN"),
    bigquery.SchemaField("date", "STRING"),
    bigquery.SchemaField("value", "FLOAT64"),
    bigquery.SchemaField("data_status", "STRING"),
    bigquery.SchemaField("request_date", "STRING"),
]

PRODUCTS_SCHEMA = [
    bigquery.SchemaField("billing_month", "STRING"),
    bigquery.SchemaField("contract_id", "STRING"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("company_name", "STRING"),
    bigquery.SchemaField("product_id", "STRING"),
    bigquery.SchemaField("product_name", "STRING"),
    bigquery.SchemaField("reporting_group_id", "STRING"),
    bigquery.SchemaField("reporting_group_name", "STRING"),
    bigquery.SchemaField("month", "STRING"),
    bigquery.SchemaField("start", "STRING"),
    bigquery.SchemaField("end", "STRING"),
    bigquery.SchemaField("request_date", "STRING"),
]
