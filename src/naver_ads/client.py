"""
네이버 검색광고 API 클라이언트

소재(ad) 검수 상태 조회에 필요한 엔드포인트만 구현합니다.

실제 검수 상태 필드: inspectStatus
  UNDER_REVIEW          검토 중
  UNDER_REVIEW_VERIFY   검수 진행 중
  REVIEW_VERIFIED       검토 완료
  WAIT                  검수 보류
  APPROVED              승인
  REJECTED              반려

성능 최적화:
  - 캠페인/광고그룹 한 번에 전체 조회 (캠페인 순회 제거)
  - 광고그룹별 소재 조회를 ThreadPoolExecutor 로 병렬 실행
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from .auth import NaverAdsCredentials, auth_headers

BASE_URL = "https://api.naver.com"
MAX_WORKERS = 5   # 병렬 요청 수 (너무 높으면 API 속도 제한으로 누락 발생)
MAX_RETRIES = 3   # 실패 시 재시도 횟수

ALERT_STATUSES: dict[str, str] = {
    "WAIT": "검수 보류",
    "REVIEW_VERIFIED": "검토 완료",
}

STATUS_LABELS: dict[str, str] = {
    "UNDER_REVIEW": "검토 중",
    "UNDER_REVIEW_VERIFY": "검수 진행 중",
    "REVIEW_VERIFIED": "검토 완료",
    "WAIT": "검수 보류",
    "APPROVED": "승인",
    "REJECTED": "반려",
}


class NaverAdsClient:
    def __init__(self, creds: NaverAdsCredentials):
        self._creds = creds

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        uri = path
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            full_url = f"{BASE_URL}{path}?{query}"
        else:
            full_url = f"{BASE_URL}{path}"

        headers = auth_headers("GET", uri, self._creds)
        req = urllib.request.Request(full_url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Naver API HTTP {e.code}: {body}") from e

    def list_campaigns(self) -> list[dict]:
        data = self._get("/ncc/campaigns")
        return data if isinstance(data, list) else (data.get("campaigns") or [])

    def list_adgroups(self) -> list[dict]:
        """캠페인 필터 없이 전체 광고그룹 한 번에 조회."""
        data = self._get("/ncc/adgroups")
        return data if isinstance(data, list) else (data.get("adgroups") or [])

    def list_ads(self, adgroup_id: str) -> list[dict]:
        data = self._get("/ncc/ads", {"nccAdgroupId": adgroup_id})
        return data if isinstance(data, list) else (data.get("ads") or [])

    # ------------------------------------------------------------------
    # 전체 소재 검수 상태 스냅샷 — 병렬 수집
    # ------------------------------------------------------------------
    def snapshot_review_statuses(self, verbose: bool = False) -> dict[str, dict]:
        """
        캠페인·광고그룹 각 1회 조회 후,
        광고그룹별 소재 조회를 ThreadPoolExecutor 로 병렬 실행.

        Returns:
            {ad_id: {"adId", "status", "campaign", "adgroup", "headline"}}
        """
        # 1) 캠페인 이름 맵
        campaigns = self.list_campaigns()
        campaign_map: dict[str, str] = {
            c.get("nccCampaignId", ""): c.get("campaignName") or c.get("name") or ""
            for c in campaigns
        }

        # 2) 광고그룹 전체 조회 — BRAND_SEARCH 타입만, 삭제된 것 제외
        # (쇼핑검색·파워링크 등 제외: 검수 알림 범위를 브랜드검색으로 한정)
        all_adgroups = self.list_adgroups()
        adgroups = [
            ag for ag in all_adgroups
            if not ag.get("delFlag") and ag.get("adgroupType") == "BRAND_SEARCH"
        ]
        adgroup_map: dict[str, tuple[str, str]] = {
            ag["nccAdgroupId"]: (
                ag.get("name") or ag.get("adgroupName") or ag["nccAdgroupId"],
                ag.get("nccCampaignId", ""),
            )
            for ag in adgroups
            if ag.get("nccAdgroupId")
        }

        if verbose:
            print(f"  광고그룹 전체 {len(all_adgroups)}개 중 미삭제 {len(adgroup_map)}개 소재 병렬 조회 중...")

        # 3) 소재 병렬 조회
        result: dict[str, dict] = {}

        import time as _time

        def _fetch(ag_id: str) -> list[dict]:
            for attempt in range(MAX_RETRIES):
                try:
                    return self.list_ads(ag_id)
                except Exception:
                    if attempt < MAX_RETRIES - 1:
                        _time.sleep(0.3 * (attempt + 1))
            return []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(_fetch, ag_id): ag_id for ag_id in adgroup_map}
            for future in as_completed(futures):
                ag_id = futures[future]
                ag_name, c_id = adgroup_map[ag_id]
                campaign_name = campaign_map.get(c_id, c_id)

                for ad in future.result():
                    ad_id = ad.get("nccAdId") or ""
                    if not ad_id:
                        continue
                    # 삭제된 소재만 제외
                    if ad.get("delFlag"):
                        continue
                    inspect_status = ad.get("inspectStatus") or ""
                    status_reason  = ad.get("statusReason") or ""
                    # 알림용 복합 상태키: inspectStatus|statusReason
                    composite = f"{inspect_status}|{status_reason}"
                    reg_tm  = ad.get("regTm") or ""   # 소재 등록 일시 (ISO8601)
                    edit_tm = ad.get("editTm") or ""  # 소재 최종 수정 일시
                    creative = ad.get("ad") or ad
                    headline = (
                        creative.get("headline")
                        or creative.get("title")
                        or creative.get("name")
                        or ad.get("name")
                        or ad_id
                    )
                    result[ad_id] = {
                        "adId": ad_id,
                        "status": composite,
                        "inspectStatus": inspect_status,
                        "statusReason": status_reason,
                        "regTm": reg_tm,
                        "editTm": edit_tm,
                        "campaign": campaign_name,
                        "adgroup": ag_name,
                        "headline": headline,
                        "platform": "검색광고",
                    }

        return result
