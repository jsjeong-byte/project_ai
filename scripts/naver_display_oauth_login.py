#!/usr/bin/env python3
"""
네이버 성과형 디스플레이 광고 API — 최초 OAuth 토큰 발급 헬퍼

실행:
  python scripts/naver_display_oauth_login.py

사전 준비:
  1. https://developers.naver.com 에서 앱 등록
     - 사용 API: 네이버 로그인
     - 로그인 오픈 API 서비스 환경: PC 웹
     - 서비스 URL: http://localhost:8080
     - Callback URL: http://localhost:8080/callback
  2. .credentials/naver_display_credentials.txt 생성
     CLIENT_ID=...
     CLIENT_SECRET=...
     MANAGER_ACCOUNT_NO=32344  (광고주센터 관리 계정 ID)
"""

from __future__ import annotations

import json
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

CRED_FILE = ROOT / ".credentials" / "naver_display_credentials.txt"
TOKEN_FILE = ROOT / ".credentials" / "naver_display_token.json"

NAVER_AUTH_URL = "https://nid.naver.com/oauth2.0/authorize"
NAVER_TOKEN_URL = "https://nid.naver.com/oauth2.0/token"
REDIRECT_URI = "http://localhost:8080/callback"
PORT = 8080


def _load_client_info() -> tuple[str, str]:
    cfg: dict[str, str] = {}
    if CRED_FILE.is_file():
        for line in CRED_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            cfg[k.strip()] = v.strip()
    client_id = cfg.get("CLIENT_ID", "")
    client_secret = cfg.get("CLIENT_SECRET", "")
    if not (client_id and client_secret):
        print(
            f"[오류] {CRED_FILE} 파일에 CLIENT_ID / CLIENT_SECRET 이 없습니다.",
            file=sys.stderr,
        )
        sys.exit(1)
    return client_id, client_secret


_auth_code: str = ""


class _CallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global _auth_code
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if "code" in params:
            _auth_code = params["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                "<h2>✅ 인증 완료! 이 창을 닫고 터미널로 돌아가세요.</h2>".encode("utf-8")
            )
        else:
            error = params.get("error", ["알 수 없는 오류"])[0]
            self.send_response(400)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(f"<h2>❌ 오류: {error}</h2>".encode("utf-8"))

    def log_message(self, *args):
        pass  # 서버 로그 억제


def main():
    client_id, client_secret = _load_client_info()

    # 1) 인가 URL 생성 & 브라우저 열기
    auth_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "state": "naver_display_login",
    }
    auth_url = NAVER_AUTH_URL + "?" + urllib.parse.urlencode(auth_params)
    print(f"\n브라우저에서 네이버 로그인 페이지가 열립니다...")
    print(f"자동으로 열리지 않으면 아래 URL을 직접 브라우저에 붙여넣으세요:\n  {auth_url}\n")
    webbrowser.open(auth_url)

    # 2) 로컬 서버로 콜백 대기
    print(f"로컬 서버 포트 {PORT} 에서 콜백 대기 중... (최대 120초)")
    server = HTTPServer(("localhost", PORT), _CallbackHandler)
    server.timeout = 120
    server.handle_request()

    if not _auth_code:
        print("[오류] 인가 코드를 받지 못했습니다.", file=sys.stderr)
        sys.exit(1)

    # 3) 인가 코드 → 액세스 토큰 교환
    token_params = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "code": _auth_code,
        "state": "naver_display_login",
        "redirect_uri": REDIRECT_URI,
    }
    body = urllib.parse.urlencode(token_params).encode("utf-8")
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
        print(f"[오류] 토큰 발급 실패 HTTP {e.code}: {e.read().decode()}", file=sys.stderr)
        sys.exit(1)

    if "error" in data:
        print(f"[오류] {data['error']}: {data.get('error_description', '')}", file=sys.stderr)
        sys.exit(1)

    expires_at = time.time() + int(data.get("expires_in", 3600))
    token_data = {
        "access_token": data["access_token"],
        "refresh_token": data.get("refresh_token", ""),
        "expires_at": expires_at,
    }
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps(token_data, indent=2), encoding="utf-8")

    print(f"\n✅ 토큰 발급 완료 → {TOKEN_FILE}")
    print(f"   액세스 토큰 만료: {int(data.get('expires_in', 3600) / 60)}분 후")
    print(f"   이후에는 자동으로 리프레시 토큰으로 갱신됩니다.")


if __name__ == "__main__":
    main()
