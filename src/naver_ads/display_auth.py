"""
네이버 성과형 디스플레이 광고 API (Beta) — OAuth 2.0 인증 헬퍼

사전 준비:
  1. https://developers.naver.com 에서 앱 등록 (로그인 오픈 API)
  2. 네이버 광고주센터(ads.naver.com) → API 사용 신청 (공식 파트너사 한정)
  3. OAuth 인가 코드 발급 → 액세스 토큰 / 리프레시 토큰 획득

토큰 파일 (.credentials/naver_display_token.json):
  {
    "access_token":  "...",
    "refresh_token": "...",
    "expires_at":    1234567890   ← Unix timestamp (초)
  }

자격증명 파일 (.credentials/naver_display_credentials.txt):
  CLIENT_ID=네이버개발자센터_앱_클라이언트ID
  CLIENT_SECRET=네이버개발자센터_앱_클라이언트시크릿
  MANAGER_ACCOUNT_NO=관리계정ID(숫자)   ← 관리 계정으로 접근 시 필요
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

CRED_FILE = Path(__file__).resolve().parents[2] / ".credentials" / "naver_display_credentials.txt"
TOKEN_FILE = Path(__file__).resolve().parents[2] / ".credentials" / "naver_display_token.json"

NAVER_TOKEN_URL = "https://nid.naver.com/oauth2.0/token"
# 토큰 만료 여유 시간 (초) — 만료 5분 전에 갱신
TOKEN_REFRESH_MARGIN = 300


@dataclass
class DisplayCredentials:
    client_id: str
    client_secret: str
    access_token: str
    refresh_token: str
    expires_at: float          # Unix timestamp
    manager_account_no: str = ""


def load_display_credentials() -> DisplayCredentials:
    """자격증명 + 토큰 파일을 읽어 DisplayCredentials 반환."""
    # 1) 클라이언트 ID / Secret
    client_id = os.environ.get("NAVER_DISPLAY_CLIENT_ID", "").strip()
    client_secret = os.environ.get("NAVER_DISPLAY_CLIENT_SECRET", "").strip()
    manager_account_no = os.environ.get("NAVER_DISPLAY_MANAGER_ACCOUNT_NO", "").strip()

    if not (client_id and client_secret) and CRED_FILE.is_file():
        cfg: dict[str, str] = {}
        for line in CRED_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            cfg[k.strip()] = v.strip()
        client_id = client_id or cfg.get("CLIENT_ID", "")
        client_secret = client_secret or cfg.get("CLIENT_SECRET", "")
        manager_account_no = manager_account_no or cfg.get("MANAGER_ACCOUNT_NO", "")

    if not (client_id and client_secret):
        raise RuntimeError(
            "성과형 디스플레이 광고 API 자격증명이 없습니다.\n"
            f"  파일 생성: {CRED_FILE}\n"
            "  내용:\n"
            "    CLIENT_ID=네이버개발자센터앱클라이언트ID\n"
            "    CLIENT_SECRET=클라이언트시크릿\n"
            "    MANAGER_ACCOUNT_NO=관리계정숫자ID"
        )

    # 2) 토큰 파일
    if not TOKEN_FILE.is_file():
        raise RuntimeError(
            "성과형 디스플레이 광고 API 토큰 파일이 없습니다.\n"
            f"  파일 생성: {TOKEN_FILE}\n"
            "  내용 예시:\n"
            '    {"access_token":"...","refresh_token":"...","expires_at":9999999999}\n\n'
            "  최초 토큰 발급 방법:\n"
            "    python scripts/naver_display_oauth_login.py"
        )

    token_data = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
    return DisplayCredentials(
        client_id=client_id,
        client_secret=client_secret,
        access_token=token_data.get("access_token", ""),
        refresh_token=token_data.get("refresh_token", ""),
        expires_at=float(token_data.get("expires_at", 0)),
        manager_account_no=manager_account_no,
    )


def ensure_valid_token(creds: DisplayCredentials) -> DisplayCredentials:
    """토큰 만료 임박 시 리프레시 토큰으로 갱신하고 파일에 저장."""
    now = time.time()
    if creds.expires_at > now + TOKEN_REFRESH_MARGIN:
        return creds  # 아직 유효

    if not creds.refresh_token:
        raise RuntimeError(
            "액세스 토큰이 만료되었고 리프레시 토큰이 없습니다.\n"
            "  python scripts/naver_display_oauth_login.py 를 다시 실행해 주세요."
        )

    params = {
        "grant_type": "refresh_token",
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "refresh_token": creds.refresh_token,
    }
    body = urllib.parse.urlencode(params).encode("utf-8")
    req = urllib.request.Request(
        NAVER_TOKEN_URL,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"토큰 갱신 실패 HTTP {e.code}: {e.read().decode('utf-8', errors='replace')}"
        ) from e

    if "error" in data:
        raise RuntimeError(
            f"토큰 갱신 실패: {data.get('error')} — {data.get('error_description', '')}\n"
            "  python scripts/naver_display_oauth_login.py 를 다시 실행해 주세요."
        )

    new_access = data["access_token"]
    new_refresh = data.get("refresh_token", creds.refresh_token)
    expires_in = int(data.get("expires_in", 3600))
    new_expires_at = now + expires_in

    # 파일 업데이트
    TOKEN_FILE.write_text(
        json.dumps(
            {
                "access_token": new_access,
                "refresh_token": new_refresh,
                "expires_at": new_expires_at,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    creds.access_token = new_access
    creds.refresh_token = new_refresh
    creds.expires_at = new_expires_at
    return creds


def display_auth_headers(creds: DisplayCredentials) -> dict[str, str]:
    """성과형 광고 API 인증 헤더 반환 (토큰 자동 갱신 포함)."""
    creds = ensure_valid_token(creds)
    headers = {
        "Authorization": f"Bearer {creds.access_token}",
        "Content-Type": "application/json; charset=UTF-8",
    }
    if creds.manager_account_no:
        headers["AccessManagerAccountNo"] = creds.manager_account_no
    return headers
