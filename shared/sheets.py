import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials


def _get_client():
    """GCP 서비스 계정으로 인증된 gspread 클라이언트 반환"""
    json_key_string = os.environ.get("GCP_SA_KEY")

    if not json_key_string:
        raise ValueError("GCP_SA_KEY 환경변수가 설정되지 않았습니다.")

    try:
        credentials_dict = json.loads(json_key_string)
    except json.JSONDecodeError:
        raise ValueError("GCP_SA_KEY가 올바른 JSON 형식이 아닙니다.")

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    return gspread.authorize(creds)


def get_document(sheet_id: str):
    """
    시트 ID로 Google Sheets 문서 객체 반환.

    Args:
        sheet_id: Google Sheets 문서 ID

    Returns:
        gspread.Spreadsheet 객체
    """
    client = _get_client()
    return client.open_by_key(sheet_id)


def get_all_worksheets(doc) -> list:
    """
    문서 내 모든 워크시트 반환.

    Args:
        doc: gspread.Spreadsheet 객체

    Returns:
        워크시트 리스트 [(이름, worksheet), ...]
    """
    return [(ws.title, ws) for ws in doc.worksheets()]


def get_worksheet(doc, name: str):
    """
    이름으로 특정 워크시트 반환.

    Args:
        doc: gspread.Spreadsheet 객체
        name: 워크시트 이름

    Returns:
        gspread.Worksheet 객체
    """
    return doc.worksheet(name)
