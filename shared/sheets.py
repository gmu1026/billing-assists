import os
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def get_connection(sheet_url):
    """
    환경변수 GCP_SA_KEY를 이용해 구글 시트에 연결하고 
    첫 번째 워크시트를 반환합니다.
    """
    # GitHub Secrets에 저장된 JSON 문자열을 바로 딕셔너리로 변환
    json_key_string = os.environ.get("GCP_SA_KEY")
    
    if not json_key_string:
        raise ValueError("GCP_SA_KEY 환경변수가 설정되지 않았습니다.")
    
    try:
        credentials_dict = json.loads(json_key_string)
    except json.JSONDecodeError:
         raise ValueError("GCP_SA_KEY가 올바른 JSON 형식이 아닙니다.")

    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    
    # 파일 생성 없이 딕셔너리로 바로 인증 (보안상 더 안전)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(creds)
    
    doc = client.open_by_url(sheet_url)
    return doc.sheet1