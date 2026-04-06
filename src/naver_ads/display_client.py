"""
네이버 성과형 디스플레이 광고 API (Beta) — 소재 검수 상태 클라이언트

BASE URL: https://openapi.naver.com/v1/ad-api/{version}

소재(creative) status 값:
  PENDING              검수 대기  → 알림: 검수 보류
  PENDING_IN_OPERATION 운영 중 보류 → 알림: 검수 보류
  ACCEPT               승인(검토 완료) → 알림: 검토 완료
  REJECT               반려
  REJECT_IN_OPERATION  운영 중 반려
  DELETED              삭제
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any

from .display_auth import DisplayCredentials, display_auth_headers

BASE_URL = "https://openapi.naver.com/v1/ad-api"
API_VERSION = "1.0"

# 슬랙 알림 대상 상태 (상태코드: 한글 레이블)
DISPLAY_ALERT_STATUSES: dict[str, str] = {
    "PENDING": "검수 보류",
    "PENDING_IN_OPERATION": "검수 보류 (운영 중)",
    "ACCEPT": "검토 완료",
}

DISPLAY_STATUS_LABELS: dict[str, str] = {
    "PENDING": "검수 대기",
    "PENDING_IN_OPERATION": "운영 중 보류",
    "ACCEPT": "승인 / 검토 완료",
    "REJECT": "반려",
    "REJECT_IN_OPERATION": "운영 중 반려",
    "DELETED": "삭제",
}


class NaverDisplayClient:
    def __init__(self, creds: DisplayCredentials, ad_account_no: int | str):
        self._creds = creds
        self._ad_account_no = str(ad_account_no)

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{BASE_URL}/{API_VERSION}{path}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query}"

        headers = display_auth_headers(self._creds)
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Naver Display API HTTP {e.code}: {body}"
            ) from e

    # ------------------------------------------------------------------
    # 광고 계정 목록 조회 (관리 계정 하위)
    # ------------------------------------------------------------------
    def list_ad_accounts(self) -> list[dict]:
        data = self._get("/managerAccounts/adAccounts")
        if isinstance(data, list):
            return data
        return data.get("adAccounts") or []

    # ------------------------------------------------------------------
    # 캠페인 목록 조회
    # ------------------------------------------------------------------
    def list_campaigns(self) -> list[dict]:
        data = self._get(f"/adAccounts/{self._ad_account_no}/campaigns")
        if isinstance(data, list):
            return data
        return data.get("campaigns") or []

    # ------------------------------------------------------------------
    # 광고 그룹 목록 조회
    # ------------------------------------------------------------------
    def list_adsets(self) -> list[dict]:
        data = self._get(f"/adAccounts/{self._ad_account_no}/adSets")
        if isinstance(data, list):
            return data
        return data.get("adSets") or []

    # ------------------------------------------------------------------
    # 소재 목록 조회 (검수 상태 포함)
    # ------------------------------------------------------------------
    def list_creatives(self, adset_no: int | str | None = None, page: int = 0, size: int = 100) -> list[dict]:
        params: dict[str, Any] = {"page": page, "size": size}
        if adset_no:
            params["adSetNo"] = adset_no
        data = self._get(
            f"/adAccounts/{self._ad_account_no}/creatives",
            params,
        )
        if isinstance(data, list):
            return data
        # 페이지네이션 응답 형태
        return data.get("contents") or data.get("creatives") or []

    # ------------------------------------------------------------------
    # 전체 소재 검수 상태 스냅샷 수집
    # ------------------------------------------------------------------
    def snapshot_review_statuses(self) -> dict[str, dict]:
        """
        Returns:
            {creative_no: {"adId": ..., "status": ..., "campaign": ..., "adgroup": ..., "headline": ...}}
        """
        result: dict[str, dict] = {}

        # 캠페인 & 광고그룹 이름 맵 구성
        campaign_map: dict[str, str] = {}
        for c in self.list_campaigns():
            no = str(c.get("no") or c.get("campaignNo") or "")
            name = c.get("name") or c.get("campaignName") or no
            if no:
                campaign_map[no] = name

        adset_map: dict[str, tuple[str, str]] = {}  # adset_no → (adset_name, campaign_no)
        for a in self.list_adsets():
            no = str(a.get("no") or a.get("adSetNo") or "")
            name = a.get("name") or a.get("adSetName") or no
            c_no = str(a.get("campaignNo") or "")
            if no:
                adset_map[no] = (name, c_no)

        # 전체 소재 페이지 순회
        page = 0
        while True:
            batch = self.list_creatives(page=page, size=100)
            if not batch:
                break
            for creative in batch:
                no = str(creative.get("no") or creative.get("creativeNo") or "")
                if not no:
                    continue
                status = creative.get("status") or ""
                name = creative.get("name") or creative.get("title") or no
                adset_no = str(creative.get("adSetNo") or "")
                adset_name, c_no = adset_map.get(adset_no, (adset_no, ""))
                campaign_name = campaign_map.get(c_no, c_no)

                result[f"display_{no}"] = {
                    "adId": f"display_{no}",
                    "status": status,
                    "campaign": campaign_name,
                    "adgroup": adset_name,
                    "headline": name,
                    "platform": "성과형 디스플레이",
                }
            if len(batch) < 100:
                break
            page += 1

        return result
