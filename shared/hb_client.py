"""
HyperBilling API 클라이언트 공통 모듈

CSP별 HB API 호출 및 쿠키 관리를 담당합니다.
"""

import os
import requests
from typing import Dict, List, Optional
from datetime import datetime
from dateutil.relativedelta import relativedelta


class HBApiClient:
    """
    HyperBilling API 클라이언트
    
    사용법:
        client = HBApiClient("alibaba", cookie)
        data = client.fetch_all_data("202512")
    """
    
    CSP_CONFIGS = {
        "alibaba": {
            "name": "Alibaba Cloud",
            "base_url": "https://alibabacloud.hyperbilling.kr",
            "endpoints": {
                "invoice": "/admin/api/v1/billing/invoice",
                "contract": "/admin/api/v1/billing/contract",
                "account": "/admin/api/v1/billing/account",
                "company": "/admin/api/v1/iam/company"
            }
        },
        "akamai": {
            "name": "Akamai",
            "base_url": "https://akamai.hyperbilling.kr",
            "endpoints": {
                "invoice": "/admin/api/v1/billing/invoice",
                "contract": "/admin/api/v1/billing/contract",
                "account": "/admin/api/v1/billing/account",
                "company": "/admin/api/v1/iam/company"
            }
        },
        "gcp": {
            "name": "GCP",
            "base_url": "https://gcp.hyperbilling.kr",
            "endpoints": {
                "invoice": "/admin/api/v1/billing/invoice",
                "contract": "/admin/api/v1/billing/contract",
                "account": "/admin/api/v1/billing/account",
                "company": "/admin/api/v1/iam/company"
            }
        }
    }
    
    def __init__(self, csp: str, cookie: str):
        """
        Args:
            csp: CSP 이름 (alibaba, akamai, gcp)
            cookie: connect.sid 쿠키 값
        """
        if csp not in self.CSP_CONFIGS:
            raise ValueError(f"지원하지 않는 CSP: {csp}")
        
        self.csp = csp
        self.cookie = cookie
        self.config = self.CSP_CONFIGS[csp]
        self.session = self._create_session()
    
    def _create_session(self) -> requests.Session:
        """HTTP 세션 생성"""
        session = requests.Session()
        session.headers.update({
            'Cookie': f'connect.sid={self.cookie}',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            'Accept': 'application/json'
        })
        return session
    
    def fetch(self, endpoint_name: str, params: Optional[Dict] = None) -> Dict:
        """
        API 엔드포인트 호출
        
        Args:
            endpoint_name: 엔드포인트 이름 (invoice, contract, account, company)
            params: 쿼리 파라미터
        
        Returns:
            API 응답 JSON
        
        Raises:
            Exception: API 호출 실패 시
        """
        endpoint = self.config['endpoints'][endpoint_name]
        url = f"{self.config['base_url']}{endpoint}"
        
        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise Exception(f"{self.csp} 쿠키가 만료되었거나 유효하지 않습니다")
            else:
                raise Exception(f"{self.csp} API 호출 실패: {e}")
        except Exception as e:
            raise Exception(f"{self.csp} API 에러: {e}")
    
    def fetch_all_data(self, invoice_month: str) -> Dict:
        """
        모든 데이터 수집

        Args:
            invoice_month: 인보이스 월 (YYYYMM)

        Returns:
            {'invoice': {...}, 'contract': {...}, 'company': {...}}
        """
        data = {}

        # Invoice
        data['invoice'] = self.fetch('invoice', {
            'invoice_month': invoice_month,
            'reseller_seq': 0
        })

        # Contract
        data['contract'] = self.fetch('contract', {
            'reseller_seq': 0
        })

        # Company
        data['company'] = self.fetch('company', {
            'status': 1
        })

        return data


def get_previous_month(date_str: Optional[str] = None) -> str:
    """
    전월을 YYYYMM 형식으로 반환 (익월 정산용)
    
    Args:
        date_str: YYYY-MM-DD 형식 (None이면 오늘 날짜 기준)
    
    Returns:
        전월 YYYYMM (예: 202512)
    
    Examples:
        2026-01-26 → 202512
        2026-02-15 → 202601
    """
    if date_str:
        current = datetime.strptime(date_str, '%Y-%m-%d')
    else:
        current = datetime.now()
    
    previous = current - relativedelta(months=1)
    return previous.strftime('%Y%m')


def extract_active_contracts(invoice_data: Dict, contract_data: Dict, 
                            company_data: Dict) -> List[Dict]:
    """
    Invoice, Contract, Company 데이터를 조인하여 활성 계약 정보 추출
    
    Args:
        invoice_data: Invoice API 응답
        contract_data: Contract API 응답
        company_data: Company API 응답
    
    Returns:
        활성 계약 리스트 [{'invoice_seq': ..., 'company_name': ..., ...}]
    """
    # Contract 맵 생성 (seq -> company_seq)
    contract_map = {}
    for contract in contract_data.get('data', []):
        seq = contract.get('seq')
        company_seq = contract.get('company_seq')
        if seq and company_seq:
            contract_map[seq] = company_seq
    
    # Company 맵 생성 (seq -> {name, license})
    company_map = {}
    for company in company_data.get('data', []):
        seq = company.get('seq')
        if seq:
            company_map[seq] = {
                'name': company.get('name'),
                'license': company.get('license')
            }
    
    # Invoice에서 데이터 추출
    results = []
    for invoice in invoice_data.get('data', []):
        contract_seq = invoice.get('contract_seq')
        if not contract_seq:
            continue
        
        company_seq = contract_map.get(contract_seq)
        if not company_seq:
            continue
        
        company_info = company_map.get(company_seq)
        if not company_info:
            continue
        
        results.append({
            'invoice_seq': invoice.get('seq'),
            'invoice_name': invoice.get('name'),
            'invoice_month': invoice.get('invoice_month'),
            'contract_seq': contract_seq,
            'company_seq': company_seq,
            'company_name': company_info['name'],
            'company_license': company_info['license'],
            'cost': invoice.get('cost'),
            'final_cost': invoice.get('final_cost'),
            'currency': invoice.get('currency'),
            'status': invoice.get('status')
        })
    
    return results