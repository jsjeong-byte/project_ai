"""
Google Sheets API v4 — 서비스 계정 전용 최소 래퍼.
keywordsound_volume_sync 등에서 batchUpdate / 읽기에 사용합니다.
"""

from __future__ import annotations

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

    @classmethod
    def from_service_account(cls, json_path: str) -> SheetsClient:
        creds = service_account.Credentials.from_service_account_file(
            json_path,
            scopes=SCOPES,
        )
        service = build("sheets", "v4", credentials=creds, cache_discovery=False)
        return cls(service)

    def get_sheet_title_by_gid(self, spreadsheet_id: str, gid: int) -> str:
        spreadsheet = (
            self._service.spreadsheets()
            .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
            .execute()
        )
        for sh in spreadsheet.get("sheets", []):
            props = sh.get("properties") or {}
            if int(props.get("sheetId", -1)) == int(gid):
                title = str(props.get("title") or "")
                if title:
                    return title
        raise RuntimeError(f"gid={gid} 인 워크시트를 스프레드시트에서 찾지 못했습니다.")

    def get_values(self, spreadsheet_id: str, range_a1: str) -> list[list[str]]:
        result = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_a1)
            .execute()
        )
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
        (
            self._service.spreadsheets()
            .values()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute()
        )
