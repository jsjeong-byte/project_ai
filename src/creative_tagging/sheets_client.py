"""
Google Sheets API v4 — 서비스 계정 전용 최소 래퍼.
keywordsound_volume_sync 등에서 batchUpdate / 읽기에 사용합니다.
"""

from __future__ import annotations

import random
import time
from typing import Any, List, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ("https://www.googleapis.com/auth/spreadsheets",)


def a1_range(worksheet: str, cell_range: str) -> str:
    """예: ('시트1', 'A1:B2') -> \"'시트1'!A1:B2\""""
    w = (worksheet or "Sheet1").replace("'", "''")
    return f"'{w}'!{cell_range}"


class SheetsClient:
    def __init__(self, service: Any) -> None:
        self._service = service

    @staticmethod
    def _should_retry(exc: BaseException) -> bool:
        """
        GitHub Actions 등에서 종종 발생하는 일시적 네트워크/SSL EOF 를 재시도합니다.
        - ssl.SSLError: EOF occurred in violation of protocol
        - 429 / 5xx HttpError
        """
        try:
            import ssl  # stdlib
        except Exception:
            ssl = None  # type: ignore

        if ssl is not None and isinstance(exc, ssl.SSLError):
            return True

        # googleapiclient HttpError (429/5xx)
        try:
            from googleapiclient.errors import HttpError
        except Exception:
            HttpError = None  # type: ignore
        if HttpError is not None and isinstance(exc, HttpError) and getattr(exc, "resp", None) is not None:
            try:
                status = int(exc.resp.status)
                return status == 429 or 500 <= status <= 599
            except Exception:
                return False

        # requests/httplib2 계열 (메시지 기반)
        msg = str(exc).lower()
        return any(
            s in msg
            for s in (
                "eof occurred in violation of protocol",
                "connection reset by peer",
                "remote end closed connection",
                "timed out",
                "temporary failure",
                "tls",
            )
        )

    def _execute_with_retry(self, req: Any, *, what: str) -> Any:
        """
        googleapiclient Request.execute() 를 재시도 래핑.
        지수 백오프 + 지터로 5회까지 재시도.
        """
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            try:
                return req.execute()
            except Exception as e:
                if attempt >= max_attempts or not self._should_retry(e):
                    raise
                # 1.0, 2.0, 4.0, 8.0... + 지터(0~0.5)
                sleep_sec = (2 ** (attempt - 1)) + random.random() * 0.5
                print(f"[SheetsClient] retry {attempt}/{max_attempts} after error in {what}: {e!r} (sleep {sleep_sec:.1f}s)")
                time.sleep(sleep_sec)

    @classmethod
    def from_service_account(cls, json_path: str) -> SheetsClient:
        creds = service_account.Credentials.from_service_account_file(
            json_path,
            scopes=SCOPES,
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return cls(service)

    def get_sheet_title_by_gid(self, spreadsheet_id: str, gid: int) -> str:
        req = self._service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets(properties(sheetId,title))",
        )
        spreadsheet = self._execute_with_retry(req, what="spreadsheets.get(meta)")
        for sh in spreadsheet.get("sheets", []):
            props = sh.get("properties") or {}
            if int(props.get("sheetId", -1)) == int(gid):
                title = str(props.get("title") or "")
                if title:
                    return title
        raise RuntimeError(f"gid={gid} 인 워크시트를 스프레드시트에서 찾지 못했습니다.")

    def get_values(self, spreadsheet_id: str, range_a1: str) -> list[list[str]]:
        req = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_a1)
        )
        result = self._execute_with_retry(req, what="values.get")
        values = result.get("values") or []
        out: list[list[str]] = []
        for row in values:
            if isinstance(row, list):
                out.append(["" if v is None else str(v) for v in row])
            else:
                out.append([str(row)])
        return out

    def batch_write(
        self,
        spreadsheet_id: str,
        updates: List[Tuple[str, List[List[Any]]]],
    ) -> None:
        if not updates:
            return
        data = [{"range": rng, "values": vals} for rng, vals in updates]
        body = {"valueInputOption": "USER_ENTERED", "data": data}
        req = (
            self._service.spreadsheets()
            .values()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        )
        self._execute_with_retry(req, what="values.batchUpdate")
