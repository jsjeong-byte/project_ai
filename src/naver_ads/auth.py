"""
네이버 검색광고 API 인증 헬퍼

인증 방식:
  X-Timestamp  : 현재 Unix 밀리초
  X-API-KEY    : 발급받은 API 키
  X-Customer   : 광고 계정 ID (숫자)
  X-Signature  : HMAC-SHA256( "{timestamp}.{method}.{uri}", secretKey )

자격증명 로드 우선순위:
  1. 환경변수  NAVER_AD_API_KEY / NAVER_AD_SECRET_KEY / NAVER_AD_CUSTOMER_ID
  2. 파일      .credentials/naver_ads_credentials.txt
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from dataclasses import dataclass
from pathlib import Path

CRED_FILE = Path(__file__).resolve().parents[2] / ".credentials" / "naver_ads_credentials.txt"


@dataclass
class NaverAdsCredentials:
    api_key: str
    secret_key: str
    customer_id: str


def load_credentials() -> NaverAdsCredentials:
    """환경변수 → 파일 순서로 자격증명을 로드합니다."""
    api_key = os.environ.get("NAVER_AD_API_KEY", "").strip()
    secret_key = os.environ.get("NAVER_AD_SECRET_KEY", "").strip()
    customer_id = os.environ.get("NAVER_AD_CUSTOMER_ID", "").strip()

    if api_key and secret_key and customer_id:
        return NaverAdsCredentials(api_key, secret_key, customer_id)

    if CRED_FILE.is_file():
        cfg: dict[str, str] = {}
        for line in CRED_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            cfg[k.strip()] = v.strip()
        api_key = cfg.get("API_KEY", "")
        secret_key = cfg.get("SECRET_KEY", "")
        customer_id = cfg.get("CUSTOMER_ID", "")

    if not (api_key and secret_key and customer_id):
        raise RuntimeError(
            "네이버 광고 API 자격증명이 없습니다.\n"
            f"  파일 생성: {CRED_FILE}\n"
            "  내용 예시:\n"
            "    API_KEY=발급받은API키\n"
            "    SECRET_KEY=발급받은시크릿키\n"
            "    CUSTOMER_ID=광고계정숫자ID\n"
            "  또는 환경변수 NAVER_AD_API_KEY / NAVER_AD_SECRET_KEY / NAVER_AD_CUSTOMER_ID 설정"
        )

    return NaverAdsCredentials(api_key, secret_key, customer_id)


def make_signature(timestamp: int, method: str, uri: str, secret_key: str) -> str:
    """HMAC-SHA256 서명 생성 (Base64 인코딩)."""
    message = f"{timestamp}.{method.upper()}.{uri}"
    raw = hmac.new(
        secret_key.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(raw).decode("utf-8")


def auth_headers(method: str, uri: str, creds: NaverAdsCredentials) -> dict[str, str]:
    """API 요청에 필요한 인증 헤더를 반환합니다."""
    ts = int(time.time() * 1000)
    return {
        "X-Timestamp": str(ts),
        "X-API-KEY": creds.api_key,
        "X-Customer": creds.customer_id,
        "X-Signature": make_signature(ts, method, uri, creds.secret_key),
        "Content-Type": "application/json; charset=UTF-8",
    }
