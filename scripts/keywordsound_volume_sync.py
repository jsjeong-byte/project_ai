#!/usr/bin/env python3
"""
keywordsound.com 검색량(일별 총검색량) → Google Sheets

모드
  - incremental (기본): KST 기준으로 채울 날짜만 수집하고, 시트는 필요한 셀만 batchUpdate
      · 월요일: 금·토·일 (today-3, today-2, today-1)
      · 그 외 요일: 전일 (today-1)
      · 날짜 열(--date-column, 기본 A)에서 첫 빈 행(2~max_row)부터 새 날짜 행을 쌓음. 이미 같은 날짜 행이 있으면 그 행만 갱신
  - full: 기존처럼 target_range 전체를 읽어 덮어쓰기(프록시 upload overwrite)

시트 버튼(Apps Script)
  - 스프레드시트에서 메뉴 실행 → GitHub repository_dispatch 로 이 레포 워크플로 트리거
  - 실제 Selenium 수집은 GitHub Actions(ubuntu)에서 수행 (비동기 1~2분)

자격
  - incremental + Google 쓰기: GOOGLE_SERVICE_ACCOUNT_JSON 또는 SERVICE_ACCOUNT_JSON 경로
      (또는 .credentials/cost_report_config.txt 의 SERVICE_ACCOUNT_JSON)
  - 키워드/시트 읽기(옵션): PROXY_API_KEY + /api/sheets/read
  - full + 프록시만: PROXY_API_KEY + upload

주의: 서비스 계정 이메일에 스프레드시트 편집 권한이 있어야 합니다.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

KEY_FILE = ROOT / ".credentials" / "proxy_api_key.txt"
COST_REPORT_CONFIG = ROOT / ".credentials" / "cost_report_config.txt"

PROXY_BASE = "https://api-auth.madup-dct.site"
KST = ZoneInfo("Asia/Seoul")

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _is_google_sheets_http_403(exc: BaseException) -> bool:
    try:
        from googleapiclient.errors import HttpError
    except ImportError:
        return False
    if isinstance(exc, HttpError) and exc.resp is not None:
        try:
            return int(exc.resp.status) == 403
        except (TypeError, ValueError):
            return False
    return False


def _google_sa_json_configured() -> bool:
    return bool((os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip())


def _service_account_email_for_hint() -> str:
    raw = (os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if not raw:
        return "JSON Secret 의 client_email 값"
    try:
        em = json.loads(raw).get("client_email")
        if em:
            return str(em)
    except Exception:
        pass
    return "JSON 의 client_email 값"


def _sheets_403_message(*, spreadsheet_id: str, what: str) -> str:
    email = _service_account_email_for_hint()
    return (
        f"Google Sheets API 403 ({what}): 서비스 계정에 이 스프레드시트 접근 권한이 없습니다.\n"
        f"  · 스프레드시트 ID: {spreadsheet_id}\n"
        f"  · 아래 이메일을 구글 시트「공유」에「편집자」로 추가하세요: {email}\n"
        "  · (다른 Google 계정으로 만든 JSON 이면, 그 client_email 이 시트에 있어야 합니다.)"
    )


def _h(api_key: str) -> dict:
    return {"X-API-Key": api_key, "Content-Type": "application/json"}


def load_api_key() -> str:
    key = os.environ.get("PROXY_API_KEY", "").strip()
    if key:
        return key
    if KEY_FILE.exists():
        key = KEY_FILE.read_text(encoding="utf-8").strip()
        if key:
            return key
    raise FileNotFoundError(
        f"PROXY_API_KEY 없음.\n  - 환경변수 PROXY_API_KEY 또는\n  - {KEY_FILE}"
    )


def sheets_read(api_key: str, spreadsheet_id: str, worksheet: str, range_: str) -> list[list[str]]:
    payload = {"spreadsheet_id": spreadsheet_id, "worksheet": worksheet, "range": range_}
    r = requests.post(f"{PROXY_BASE}/api/sheets/read", headers=_h(api_key), json=payload, timeout=120)
    r.raise_for_status()
    body = r.json()
    if not body.get("success"):
        raise RuntimeError(f"Sheets read 실패: {body.get('error')}")
    data = body.get("data") or {}
    values = data.get("values") or data.get("data") or []
    if not isinstance(values, list):
        raise RuntimeError(f"Sheets read 응답 파싱 실패: {body}")
    out: list[list[str]] = []
    for row in values:
        if not isinstance(row, list):
            out.append([str(row)])
            continue
        out.append(["" if v is None else str(v) for v in row])
    return out


def sheets_upload_overwrite(
    api_key: str, spreadsheet_id: str, worksheet: str, data: list[list[str]]
) -> None:
    payload = {"spreadsheet_id": spreadsheet_id, "worksheet": worksheet, "data": data, "mode": "overwrite"}
    r = requests.post(f"{PROXY_BASE}/api/sheets/upload", headers=_h(api_key), json=payload, timeout=300)
    r.raise_for_status()
    body = r.json()
    if not body.get("success"):
        raise RuntimeError(f"Sheets upload 실패: {body.get('error')}")


def sheets_info(api_key: str, spreadsheet_id: str) -> dict:
    r = requests.get(
        f"{PROXY_BASE}/api/sheets/{spreadsheet_id}/info",
        headers={"X-API-Key": api_key},
        timeout=30,
    )
    r.raise_for_status()
    body = r.json()
    if not body.get("success"):
        raise RuntimeError(f"Sheets info 실패: {body.get('error')}")
    return body.get("data") or {}


def worksheet_title_from_gid(api_key: str, spreadsheet_id: str, gid: int) -> str:
    info = sheets_info(api_key, spreadsheet_id)
    worksheets = info.get("worksheets") or []
    for ws in worksheets:
        if int(ws.get("id")) == int(gid):
            return str(ws.get("title"))
    raise RuntimeError(f"gid={gid} 워크시트를 찾지 못했습니다.")


def parse_int_maybe(num: str) -> Optional[int]:
    s = (num or "").strip()
    if not s:
        return None
    s = s.replace(",", "")
    s = re.sub(r"[^\d\-]", "", s)
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def normalize_keyword(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def col_letters_to_index(col: str) -> int:
    """'A' -> 0, 'Z' -> 25, 'AA' -> 26"""
    n = 0
    for c in col.upper().strip():
        n = n * 26 + (ord(c) - 64)
    return n - 1


def index_to_col_letters(idx: int) -> str:
    """0 -> 'A'"""
    if idx < 0:
        raise ValueError("col index < 0")
    s = ""
    n = idx + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def normalize_date_column_letter(col: str) -> str:
    """날짜가 쌓이는 열 (A, B, …). 헤더 매칭 시 이 열은 키워드에서 제외."""
    s = (col or "A").strip().upper()
    if not s or not re.match(r"^[A-Z]+$", s):
        raise ValueError(f"date_column 은 열 문자만 가능합니다 (예: A, B, AA): {col!r}")
    return s


def parse_a1_range(rng: str) -> tuple[int, int, int, int]:
    """
    'A1:U204' -> (start_row 1-based, start_col 0-based, end_row 1-based, end_col 0-based)
    """
    rng = rng.strip()
    if ":" not in rng:
        raise ValueError(f"A1 범위 형식이 아닙니다: {rng}")
    a, b = rng.split(":", 1)
    m1 = re.match(r"^([A-Za-z]+)(\d+)$", a.strip())
    m2 = re.match(r"^([A-Za-z]+)(\d+)$", b.strip())
    if not m1 or not m2:
        raise ValueError(f"A1 범위 파싱 실패: {rng}")
    c1, r1 = m1.group(1), int(m1.group(2))
    c2, r2 = m2.group(1), int(m2.group(2))
    col1, col2 = col_letters_to_index(c1), col_letters_to_index(c2)
    row1, row2 = r1, r2
    return min(row1, row2), min(col1, col2), max(row1, row2), max(col1, col2)


def compute_target_dates_kst(as_of: Optional[date] = None) -> list[str]:
    """
    월요일(KST): 금·토·일
    그 외: 전일 1일
    """
    d = as_of or datetime.now(tz=KST).date()
    wd = d.weekday()  # Mon=0
    if wd == 0:
        return [
            (d - timedelta(days=3)).isoformat(),
            (d - timedelta(days=2)).isoformat(),
            (d - timedelta(days=1)).isoformat(),
        ]
    return [(d - timedelta(days=1)).isoformat()]


def build_sheets_client():
    """서비스 계정 또는 OAuth (run_daily_cost_report 와 동일 우선순위)."""
    from src.creative_tagging.sheets_client import SheetsClient

    sa_json_content = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if sa_json_content:
        p = Path(tempfile.gettempdir()) / "keywordsound_sa.json"
        p.write_text(sa_json_content, encoding="utf-8")
        return SheetsClient.from_service_account(str(p))

    path = os.environ.get("SERVICE_ACCOUNT_JSON", "").strip()
    if not path and COST_REPORT_CONFIG.exists():
        cfg: dict[str, str] = {}
        for line in COST_REPORT_CONFIG.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            cfg[k.strip()] = v.strip()
        path = cfg.get("SERVICE_ACCOUNT_JSON", "").strip()

    if path and Path(path).is_file():
        return SheetsClient.from_service_account(path)

    try:
        from scripts.run_daily_cost_report import _build_sheets_client, _load_config

        cfg = _load_config()
        return _build_sheets_client(cfg)
    except ImportError:
        raise RuntimeError(
            "Google Sheets 자격이 없습니다. GitHub Actions Secrets에 "
            "GOOGLE_SERVICE_ACCOUNT_JSON(서비스 계정 JSON 전체)를 넣거나, "
            "PROXY_API_KEY(프록시 read/upload)를 설정하세요."
        ) from None


def resolve_keywordsound_datatables_timeout_sec(explicit: Optional[int] = None) -> int:
    """
    keywordsound.com DataTables 가 느릴 때 대기(초).
    --keywordsound-timeout 이 있으면 우선, 없으면 KEYWORDSOUND_DATATABLES_TIMEOUT_SEC, 기본 240.
    """
    if explicit is not None:
        return max(60, min(int(explicit), 900))
    raw = (os.environ.get("KEYWORDSOUND_DATATABLES_TIMEOUT_SEC") or "").strip()
    if raw:
        try:
            return max(60, min(int(raw), 900))
        except ValueError:
            pass
    return 240


class KeywordSoundScraper:
    def __init__(self, headless: bool = True, timeout_sec: Optional[int] = None):
        self.headless = headless
        self.timeout_sec = resolve_keywordsound_datatables_timeout_sec(timeout_sec)
        self.driver = self._new_driver()
        self.wait = WebDriverWait(self.driver, self.timeout_sec)

    def close(self) -> None:
        try:
            self.driver.quit()
        except Exception:
            pass

    def _new_driver(self):
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1400,900")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def _hide_ad_overlays(self) -> None:
        try:
            self.driver.execute_script(
                """
                if (document.querySelector('style[data-ks-automation="1"]')) return;
                const style = document.createElement('style');
                style.setAttribute('data-ks-automation', '1');
                style.textContent = `
                  iframe[src*="doubleclick.net"],
                  iframe[src*="googlesyndication.com"],
                  iframe[id^="aswift"],
                  iframe[name^="aswift"] {
                    display: none !important;
                    visibility: hidden !important;
                    pointer-events: none !important;
                  }
                `;
                document.documentElement.appendChild(style);
                """
            )
        except Exception:
            pass

    def _wait_datatables_ready(self) -> None:
        t0 = time.time()
        while True:
            info = self.driver.execute_script(
                """
                try {
                  if (window.jQuery && jQuery.fn && jQuery.fn.dataTable) {
                    return jQuery('#tableSearchVolume').DataTable().page.info();
                  }
                  return null;
                } catch (e) {
                  return { error: String(e) };
                }
                """
            )
            if isinstance(info, dict) and int(info.get("recordsDisplay") or 0) > 0:
                return
            if (time.time() - t0) > max(self.timeout_sec, 90):
                raise RuntimeError(
                    "검색량 테이블(DataTables) 로딩 타임아웃 "
                    f"({self.timeout_sec}초). 사이트가 느리면 GitHub Actions 환경변수 "
                    "KEYWORDSOUND_DATATABLES_TIMEOUT_SEC=300 처럼 늘리거나, "
                    "--keywordsound-timeout 300 으로 실행하세요."
                )
            time.sleep(0.25)

    def fetch_totals_for_dates(self, keyword: str, want_dates: set[str]) -> Dict[str, int]:
        """want_dates 에 해당하는 날짜만 추출 (DataTables 전체 직렬화 없이 JS에서 필터)."""
        keyword = (keyword or "").strip()
        if not keyword or not want_dates:
            return {}

        url = "https://keywordsound.com/service/keyword-analysis?keywords=" + quote(keyword)
        self.driver.get(url)
        self._hide_ad_overlays()
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table#tableSearchVolume tbody")))
        self._hide_ad_overlays()
        self._wait_datatables_ready()

        payload = json.dumps(sorted(want_dates), ensure_ascii=False)
        raw = self.driver.execute_script(
            """
            const want = new Set(JSON.parse(arguments[0]));
            const t = jQuery('#tableSearchVolume').DataTable();
            const out = {};
            t.rows({search:'applied'}).every(function () {
              const row = this.data();
              const d = row.date;
              if (want.has(d)) {
                out[d] = row.searchVolumeTotal;
                if (Object.keys(out).length === want.size) {
                  return false;
                }
              }
            });
            return out;
            """,
            payload,
        )
        if not isinstance(raw, dict):
            return {}

        out: Dict[str, int] = {}
        for d, v in raw.items():
            ds = str(d).strip()
            if not _DATE_RE.match(ds):
                continue
            n = parse_int_maybe(str(v))
            if n is None:
                continue
            out[ds] = n
        return out

    def fetch_daily_totals_all(self, keyword: str) -> Dict[str, int]:
        """full 모드용: 전체 일자 맵."""
        keyword = (keyword or "").strip()
        if not keyword:
            return {}
        url = "https://keywordsound.com/service/keyword-analysis?keywords=" + quote(keyword)
        self.driver.get(url)
        self._hide_ad_overlays()
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table#tableSearchVolume tbody")))
        self._hide_ad_overlays()
        self._wait_datatables_ready()
        rows = self.driver.execute_script(
            """
            const t = jQuery('#tableSearchVolume').DataTable();
            return t.rows({search:'applied'}).data().toArray();
            """
        )
        if not isinstance(rows, list):
            return {}
        out: Dict[str, int] = {}
        for r in rows:
            if not isinstance(r, dict):
                continue
            d = str(r.get("date") or "").strip()
            if not _DATE_RE.match(d):
                continue
            n = parse_int_maybe(str(r.get("searchVolumeTotal") or ""))
            if n is None:
                continue
            out[d] = n
        return out


def pad_rows(values: list[list[str]], target_len: int) -> list[list[str]]:
    out = [list(r) for r in values]
    while len(out) < target_len:
        out.append([])
    return out


def first_empty_row_in_column(
    col_values: list[list[str]],
    *,
    sheet_start_row: int,
    expected_rows: int,
    column_letter: str,
) -> int:
    """
    col_values: {column_letter}{sheet_start_row}:… 한 열 구간의 2차원 배열(행마다 길이 1)
    반환: 시트 상 1-based 행 번호 중 첫 빈 칸
    """
    col_values = pad_rows(col_values, expected_rows)
    for i in range(expected_rows):
        cell = col_values[i][0] if col_values[i] else ""
        if not str(cell).strip():
            return sheet_start_row + i
    raise RuntimeError(
        f"{column_letter}열 {sheet_start_row}~{sheet_start_row + expected_rows - 1} 에 빈 행이 없습니다. "
        "행을 늘리거나 target 범위·--date-column 을 조정하세요."
    )


def run_incremental_google(
    *,
    spreadsheet_id: str,
    worksheet: str,
    keywords: list[str],
    target_dates: list[str],
    keywords_range: str,
    target_range: str,
    date_column: str,
    scraper: KeywordSoundScraper,
    dry_run: bool,
) -> None:
    from src.creative_tagging.sheets_client import SheetsClient, a1_range

    sheets: SheetsClient = build_sheets_client()

    r1, c1, r2, c2 = parse_a1_range(target_range)
    if r1 != 1 or c1 != 0:
        raise RuntimeError("incremental 모드는 target_range 가 A1 시작(A1:U204 형태)일 것을 권장합니다.")

    dc_letter = normalize_date_column_letter(date_column)
    dc_idx = col_letters_to_index(dc_letter)
    if dc_idx < c1 or dc_idx > c2:
        raise RuntimeError(
            f"date_column={dc_letter} 이 target_range 열 범위 안에 있어야 합니다. target_range={target_range}"
        )

    max_row = r2
    max_col_letter = index_to_col_letters(c2)
    n_body_rows = max_row - 1  # body rows 2..max_row

    header_rng = a1_range(worksheet, f"A1:{max_col_letter}1")
    col_date_rng = a1_range(worksheet, f"{dc_letter}2:{dc_letter}{max_row}")

    header_rows = sheets.get_values(spreadsheet_id, header_rng)
    header = header_rows[0] if header_rows else []
    header_norm = [normalize_keyword(x) for x in header]
    kw_to_col: Dict[str, int] = {}
    for i, h in enumerate(header_norm):
        if i == dc_idx:
            continue
        if h:
            kw_to_col[h] = i

    col_dates = sheets.get_values(spreadsheet_id, col_date_rng)
    col_dates = pad_rows(col_dates, n_body_rows)

    date_to_row: Dict[str, int] = {}
    for i in range(n_body_rows):
        v = col_dates[i][0] if col_dates[i] else ""
        ds = str(v).strip()
        if _DATE_RE.match(ds):
            date_to_row[ds] = i + 2  # sheet row

    first_empty = first_empty_row_in_column(
        col_dates, sheet_start_row=2, expected_rows=n_body_rows, column_letter=dc_letter
    )
    append_cursor = first_empty

    want = set(target_dates)
    pending_new_dates = {d for d in want if d not in date_to_row}
    updates: List[Tuple[str, List[List[Any]]]] = []

    for kw in keywords:
        print(f"\n[수집] {kw} ({len(want)}일)")
        got = scraper.fetch_totals_for_dates(kw, want)
        print(f"  - OK: {len(got)}일 {sorted(got.keys())}")
        col_idx = kw_to_col.get(kw)
        if col_idx is None:
            print(f"  - 스킵: 1행에 '{kw}' 헤더 없음")
            continue
        col_letter = index_to_col_letters(col_idx)
        for d in sorted(want):
            total = got.get(d)
            if total is None:
                print(f"  - 경고: keywordsound 에 {d} 데이터 없음 ({kw})")
                continue
            row = date_to_row.get(d)
            if row is None:
                if d not in pending_new_dates:
                    continue
                row = append_cursor
                append_cursor += 1
                if row > max_row:
                    raise RuntimeError(f"행 부족: {d} 를 쓸 행이 {max_row} 을 초과합니다.")
                date_to_row[d] = row
                pending_new_dates.discard(d)
                updates.append((a1_range(worksheet, f"{dc_letter}{row}"), [[d]]))
            updates.append((a1_range(worksheet, f"{col_letter}{row}"), [[total]]))

    if pending_new_dates:
        print(
            f"\n[경고] keywordsound 에서 찾지 못해 새 행({dc_letter}열)을 만들지 못한 날짜: {sorted(pending_new_dates)}"
        )

    if dry_run:
        print(f"\n[dry-run] 쓰기 스킵. updates={len(updates)} 블록")
        return

    if not updates:
        print("\n[결과] 반영할 업데이트 없음")
        return

    print(f"\n[Sheets] batchUpdate {len(updates)} 블록")
    sheets.batch_write(spreadsheet_id, updates)
    print("[완료]")


def run_incremental_proxy(
    *,
    api_key: str,
    spreadsheet_id: str,
    worksheet: str,
    keywords: list[str],
    target_dates: list[str],
    keywords_range: str,
    target_range: str,
    date_column: str,
    scraper: KeywordSoundScraper,
    dry_run: bool,
) -> None:
    """Google 자격이 없을 때: target_range 전체를 읽고 일부만 수정 후 overwrite (느리지만 동작)."""
    r1, c1, r2, c2 = parse_a1_range(target_range)
    if r1 != 1 or c1 != 0:
        raise RuntimeError("proxy incremental 은 A1 시작 범위를 권장합니다.")
    dc_letter = normalize_date_column_letter(date_column)
    dc_idx = col_letters_to_index(dc_letter)
    if dc_idx < c1 or dc_idx > c2:
        raise RuntimeError(
            f"date_column={dc_letter} 이 target_range 열 범위 안에 있어야 합니다. target_range={target_range}"
        )

    max_row = r2
    max_col_letter = index_to_col_letters(c2)
    n_body_rows = max_row - 1

    grid = sheets_read(api_key, spreadsheet_id, worksheet, target_range)
    while len(grid) < max_row:
        grid.append([])
    for i in range(len(grid)):
        while len(grid[i]) <= c2:
            grid[i].append("")

    header = grid[0] if grid else []
    header_norm = [normalize_keyword(x) for x in header]
    kw_to_col: Dict[str, int] = {}
    for i, h in enumerate(header_norm):
        if i == dc_idx:
            continue
        if h:
            kw_to_col[h] = i

    col_dates = []
    for r in range(1, max_row):
        row = grid[r] if r < len(grid) else []
        cell = row[dc_idx] if len(row) > dc_idx else ""
        col_dates.append([cell])
    col_dates = pad_rows(col_dates, n_body_rows)

    date_to_row: Dict[str, int] = {}
    for i in range(n_body_rows):
        cell = col_dates[i][0] if col_dates[i] else ""
        ds = str(cell).strip()
        if _DATE_RE.match(ds):
            date_to_row[ds] = i + 2

    first_empty = first_empty_row_in_column(
        col_dates, sheet_start_row=2, expected_rows=n_body_rows, column_letter=dc_letter
    )
    append_cursor = first_empty
    want = set(target_dates)
    pending_new_dates = {d for d in want if d not in date_to_row}
    touched = 0

    for kw in keywords:
        print(f"\n[수집] {kw}")
        got = scraper.fetch_totals_for_dates(kw, want)
        print(f"  - OK: {len(got)}일")
        col_idx = kw_to_col.get(kw)
        if col_idx is None:
            print(f"  - 스킵: 1행에 '{kw}' 없음")
            continue
        for d in sorted(want):
            total = got.get(d)
            if total is None:
                continue
            row = date_to_row.get(d)
            if row is None:
                if d not in pending_new_dates:
                    continue
                row = append_cursor
                append_cursor += 1
                if row > max_row:
                    raise RuntimeError(f"행 부족: {max_row} 초과")
                date_to_row[d] = row
                pending_new_dates.discard(d)
                while len(grid) < row:
                    grid.append([])
                while len(grid[row - 1]) <= c2:
                    grid[row - 1].append("")
                while len(grid[row - 1]) <= dc_idx:
                    grid[row - 1].append("")
                grid[row - 1][dc_idx] = d
            r = row - 1
            while len(grid[r]) <= col_idx:
                grid[r].append("")
            grid[r][col_idx] = str(total)
            touched += 1

    if pending_new_dates:
        print(
            f"\n[경고] 데이터가 없어 새 행({dc_letter}열)을 만들지 못한 날짜: {sorted(pending_new_dates)}"
        )

    if dry_run:
        print(f"\n[dry-run] proxy overwrite 스킵. touched_cells~={touched}")
        return
    if not touched:
        print("\n[결과] 반영 없음")
        return
    print(f"\n[프록시] overwrite {target_range} ({touched}셀 변경)")
    sheets_upload_overwrite(api_key, spreadsheet_id, worksheet, grid)
    print("[완료]")


def run_full_proxy(
    *,
    api_key: str,
    spreadsheet_id: str,
    worksheet: str,
    keywords: list[str],
    keywords_range: str,
    target_range: str,
    scraper: KeywordSoundScraper,
    dry_run: bool,
) -> None:
    _, _, _, c2 = parse_a1_range(target_range)
    grid = sheets_read(api_key, spreadsheet_id, worksheet, target_range)
    if not grid:
        raise RuntimeError("대상 표가 비었습니다.")

    header = grid[0] if len(grid) >= 1 else []
    header_norm = [normalize_keyword(x) for x in header]
    kw_to_col: Dict[str, int] = {}
    for i, h in enumerate(header_norm):
        if i == 0:
            continue
        if h:
            kw_to_col[h] = i

    date_to_row: Dict[str, int] = {}
    for r in range(1, len(grid)):
        if not grid[r]:
            continue
        d = (grid[r][0] or "").strip()
        if _DATE_RE.match(d):
            date_to_row[d] = r

    all_updates: List[Tuple[str, str, int]] = []
    for kw in keywords:
        print(f"\n[수집] {kw}")
        totals = scraper.fetch_daily_totals_all(kw)
        print(f"  - 수집: {len(totals)}일")
        col = kw_to_col.get(kw)
        if col is None:
            print(f"  - 스킵: 1행에 '{kw}' 없음")
            continue
        n_match = 0
        for d, total in totals.items():
            row = date_to_row.get(d)
            if row is None:
                continue
            while len(grid[row]) <= col:
                grid[row].append("")
            grid[row][col] = str(total)
            all_updates.append((d, kw, total))
            n_match += 1
        print(f"  - 매칭 반영: {n_match} 셀")

    if dry_run:
        print(f"\n[dry-run] 업로드 스킵. updates={len(all_updates)}")
        return
    if not all_updates:
        print("\n[결과] 반영할 값 없음")
        return
    print(f"\n[프록시] overwrite {target_range}")
    sheets_upload_overwrite(api_key, spreadsheet_id, worksheet, grid)
    print(f"[완료] {len(all_updates)} 셀")


def load_keywords(
    *,
    spreadsheet_id: str,
    worksheet: str,
    keywords_range: str,
    api_key: Optional[str],
) -> list[str]:
    from src.creative_tagging.sheets_client import a1_range

    rows: list[list[str]]
    # PROXY 와 Google 둘 다 있으면 예전엔 프록시를 먼저 써서 502 등에 막힘 → Sheets API 우선
    try:
        sheets = build_sheets_client()
        rows = sheets.get_values(spreadsheet_id, a1_range(worksheet, keywords_range))
    except Exception as e:
        if _is_google_sheets_http_403(e):
            raise RuntimeError(_sheets_403_message(spreadsheet_id=spreadsheet_id, what="키워드 범위 읽기")) from e
        if _google_sa_json_configured():
            raise RuntimeError(
                "Google Sheets 로 키워드 범위를 읽지 못했습니다. "
                "GOOGLE_SERVICE_ACCOUNT_JSON 이 설정되어 있어 프록시(api-auth.madup-dct.site)로는 넘기지 않습니다(502 등).\n"
                "· JSON 의 client_email 을 이 스프레드시트에「편집자」로 공유했는지 확인\n"
                "· 시트 URL 의 ID 와 실행 중인 spreadsheet_id 가 같은지 확인\n"
                f"  spreadsheet_id={spreadsheet_id}\n"
                f"  client_email 힌트: {_service_account_email_for_hint()}\n"
                f"원본 오류: {e!r}"
            ) from e
        if not api_key:
            raise RuntimeError(
                "시트에서 키워드를 읽을 수 없습니다. "
                "GitHub Secret GOOGLE_SERVICE_ACCOUNT_JSON(권장) 또는 "
                "동작하는 PROXY_API_KEY 가 필요합니다."
            ) from e
        rows = sheets_read(api_key, spreadsheet_id, worksheet, keywords_range)
    keywords = [normalize_keyword(r[0]) for r in rows if r and normalize_keyword(r[0])]
    seen: set[str] = set()
    return [k for k in keywords if not (k in seen or seen.add(k))]


def resolve_worksheet_title(api_key: Optional[str], spreadsheet_id: str, gid: Optional[int], worksheet: Optional[str]) -> str:
    if worksheet:
        return worksheet
    if gid is None:
        raise RuntimeError("--gid 또는 --worksheet 필요")
    try:
        sheets = build_sheets_client()
        return sheets.get_sheet_title_by_gid(spreadsheet_id, int(gid))
    except Exception as e:
        if _is_google_sheets_http_403(e):
            raise RuntimeError(_sheets_403_message(spreadsheet_id=spreadsheet_id, what="시트 이름(gid) 조회")) from e
        if _google_sa_json_configured():
            raise RuntimeError(
                "Google Sheets 로 gid→시트 이름을 조회하지 못했습니다. "
                "GOOGLE_SERVICE_ACCOUNT_JSON 이 있으면 프록시로 대체하지 않습니다.\n"
                f"  spreadsheet_id={spreadsheet_id}, gid={gid}\n"
                f"  client_email 힌트: {_service_account_email_for_hint()}\n"
                f"원본 오류: {e!r}"
            ) from e
        if not api_key:
            raise RuntimeError(
                "gid 로 시트 이름을 알 수 없습니다. GOOGLE_SERVICE_ACCOUNT_JSON 또는 PROXY_API_KEY 를 확인하세요."
            ) from e
        return worksheet_title_from_gid(api_key, spreadsheet_id, int(gid))


def main() -> None:
    ap = argparse.ArgumentParser(description="keywordsound 검색량 → Google Sheets")
    # repository_dispatch 는 --github-dispatch-json 만 넘기므로, 병합 전에는 비워 둘 수 있음
    ap.add_argument("--spreadsheet-id", default="", help="스프레드시트 ID (dispatch JSON에 있으면 생략 가능)")
    ap.add_argument("--gid", type=int, default=None)
    ap.add_argument("--worksheet", default=None)
    ap.add_argument("--keywords-range", default="AH26:AH31")
    ap.add_argument("--target-range", default="A1:U204")
    ap.add_argument(
        "--date-column",
        default="A",
        help="날짜가 있는 열 (빈 행 탐색·신규 날짜 기록). 예: A 또는 B",
    )
    ap.add_argument("--mode", choices=("incremental", "full"), default="incremental")
    ap.add_argument(
        "--dates",
        default=None,
        help="쉼표 구분 YYYY-MM-DD (미지정 시 KST 기준 자동: 월=금토일, 그 외=전일)",
    )
    ap.add_argument("--auto-dates-kst", action="store_true", help="--dates 대신 KST 규칙만 사용")
    ap.add_argument(
        "--write-backend",
        choices=("auto", "google", "proxy"),
        default="auto",
        help="incremental 시: google=batch 부분 쓰기, proxy=범위 읽고 overwrite",
    )
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--keywordsound-timeout",
        type=int,
        default=None,
        help="keywordsound DataTables 대기 초 (60~900, 기본: 환경변수 또는 240)",
    )
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--no-headless", action="store_true")
    ap.add_argument(
        "--github-dispatch-json",
        default=None,
        help="repository_dispatch client_payload JSON (spreadsheet_id, gid, worksheet, dates[], ...)",
    )
    args = ap.parse_args()

    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass

    # GitHub dispatch 페이로드 병합
    if args.github_dispatch_json:
        payload = json.loads(args.github_dispatch_json)
        args.spreadsheet_id = payload.get("spreadsheet_id", args.spreadsheet_id)
        if payload.get("gid") is not None:
            args.gid = int(payload["gid"])
        if payload.get("worksheet"):
            args.worksheet = payload["worksheet"]
        if payload.get("keywords_range"):
            args.keywords_range = payload["keywords_range"]
        if payload.get("target_range"):
            args.target_range = payload["target_range"]
        if payload.get("date_column"):
            args.date_column = str(payload["date_column"]).strip()
        if payload.get("mode"):
            args.mode = str(payload["mode"])
        if payload.get("dates"):
            dl = payload["dates"]
            args.dates = ",".join(str(x) for x in dl) if isinstance(dl, list) else str(dl)
        if payload.get("keywordsound_timeout") is not None:
            args.keywordsound_timeout = int(payload["keywordsound_timeout"])

    sid = (args.spreadsheet_id or "").strip()
    if not sid:
        raise RuntimeError(
            "spreadsheet_id 가 없습니다. --spreadsheet-id 를 주거나, "
            "--github-dispatch-json(client_payload)에 spreadsheet_id 를 넣으세요."
        )
    args.spreadsheet_id = sid

    api_key: Optional[str] = None
    try:
        api_key = load_api_key()
    except FileNotFoundError:
        api_key = None

    worksheet = resolve_worksheet_title(api_key, args.spreadsheet_id, args.gid, args.worksheet)

    if args.dates:
        target_dates = [s.strip() for s in args.dates.split(",") if s.strip()]
    else:
        target_dates = compute_target_dates_kst()

    for d in target_dates:
        if not _DATE_RE.match(d):
            raise RuntimeError(f"날짜 형식 오류: {d}")

    headless = not args.no_headless
    if args.headless:
        headless = True

    keywords = load_keywords(
        spreadsheet_id=args.spreadsheet_id,
        worksheet=worksheet,
        keywords_range=args.keywords_range,
        api_key=api_key,
    )
    if not keywords:
        raise RuntimeError("키워드가 비었습니다.")
    print(f"[KST 기준일] {datetime.now(tz=KST).date().isoformat()} | target_dates={target_dates}")
    print(f"[키워드] {keywords}")

    write_backend = args.write_backend
    if write_backend == "auto":
        try:
            build_sheets_client()
            write_backend = "google"
        except Exception:
            write_backend = "proxy"

    scraper = KeywordSoundScraper(headless=headless, timeout_sec=args.keywordsound_timeout)
    try:
        if args.mode == "full":
            if not api_key:
                raise RuntimeError("full 모드는 PROXY_API_KEY 가 필요합니다 (프록시 read/upload).")
            run_full_proxy(
                api_key=api_key,
                spreadsheet_id=args.spreadsheet_id,
                worksheet=worksheet,
                keywords=keywords,
                keywords_range=args.keywords_range,
                target_range=args.target_range,
                scraper=scraper,
                dry_run=args.dry_run,
            )
            return

        # incremental
        if write_backend == "google":
            run_incremental_google(
                spreadsheet_id=args.spreadsheet_id,
                worksheet=worksheet,
                keywords=keywords,
                target_dates=target_dates,
                keywords_range=args.keywords_range,
                target_range=args.target_range,
                date_column=args.date_column,
                scraper=scraper,
                dry_run=args.dry_run,
            )
        else:
            if not api_key:
                raise RuntimeError(
                    "incremental + proxy 백엔드는 PROXY_API_KEY 가 필요합니다. "
                    "또는 Google 서비스 계정을 설정해 --write-backend google 을 사용하세요."
                )
            run_incremental_proxy(
                api_key=api_key,
                spreadsheet_id=args.spreadsheet_id,
                worksheet=worksheet,
                keywords=keywords,
                target_dates=target_dates,
                keywords_range=args.keywords_range,
                target_range=args.target_range,
                date_column=args.date_column,
                scraper=scraper,
                dry_run=args.dry_run,
            )
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
