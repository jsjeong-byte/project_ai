#!/usr/bin/env python3
"""
네이버 광고 소재 검수 상태 변경 → Slack 알림

지원 플랫폼:
  [검색광고]   searchad.naver.com  — HMAC-SHA256 인증
  [성과형]     ads.naver.com       — OAuth 2.0 Bearer Token (공식 파트너사 전용 Beta)

감지 대상 상태:
  검색광고:  WAIT            → ⚠️ 검수 보류
             REVIEW_VERIFIED → ✅ 검토 완료
  성과형:    PENDING / PENDING_IN_OPERATION → ⚠️ 검수 보류
             ACCEPT          → ✅ 검토 완료

실행 방법:
  python scripts/run_naver_inspection_alert.py                    # 1회 (두 플랫폼)
  python scripts/run_naver_inspection_alert.py --only search      # 검색광고만
  python scripts/run_naver_inspection_alert.py --only display     # 성과형만
  python scripts/run_naver_inspection_alert.py --loop --interval 300
  python scripts/run_naver_inspection_alert.py --dry-run --verbose

Slack 웹훅:
  환경변수: SLACK_INSPECTION_WEBHOOK_URL
  파일:     .credentials/slack_inspection_webhook_url.txt

네이버 검색광고 자격증명:
  파일: .credentials/naver_ads_credentials.txt
    API_KEY=...  SECRET_KEY=...  CUSTOMER_ID=...

네이버 성과형 자격증명:
  파일: .credentials/naver_display_credentials.txt
    CLIENT_ID=...  CLIENT_SECRET=...  MANAGER_ACCOUNT_NO=32344
  토큰: .credentials/naver_display_token.json  (최초 발급: python scripts/naver_display_oauth_login.py)
  광고계정 목록: .credentials/naver_display_ad_accounts.txt (줄당 숫자 ID)
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Windows cp949 환경에서 UTF-8 출력 강제
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except AttributeError:
    pass


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

WEBHOOK_FILE = ROOT / ".credentials" / "slack_inspection_webhook_url.txt"
SNAPSHOT_FILE = ROOT / "data" / "naver_ads_inspection_snapshot.json"
DISPLAY_AD_ACCOUNTS_FILE = ROOT / ".credentials" / "naver_display_ad_accounts.txt"

# ─── 알림 대상 상태 ───────────────────────────────────────────────────────────
# 알림을 보낼 "도착 상태" (new_status 가 이 값일 때)
# 알림 조건 판별 함수 (inspectStatus + statusReason 복합 판단)
# 검수 진행 중 상태 (이 상태 다음에 검토 완료가 오면 알림)
_SEARCH_IN_REVIEW = {"UNDER_REVIEW", "UNDER_REVIEW_VERIFY", "UNDER_REVIEW_STOPPED"}


DISPLAY_ALERT_STATUSES: dict[str, str] = {
    "PENDING": "검수 보류",
    "PENDING_IN_OPERATION": "검수 보류 (운영 중)",
    "ACCEPT": "검토 완료",
}
DISPLAY_REVIEW_IN_PROGRESS = {"UNDER_REVIEW", "UNDER_REVIEW_VERIFY", "REVIEWING"}

STATUS_EMOJI: dict[str, str] = {
    "WAIT": ":warning:",
    "REVIEW_VERIFIED": ":white_check_mark:",
    "PENDING": ":warning:",
    "PENDING_IN_OPERATION": ":warning:",
    "ACCEPT": ":white_check_mark:",
    "REJECTED": ":x:",
    "REJECT": ":x:",
}


# ─── Slack ────────────────────────────────────────────────────────────────────

def _resolve_webhook() -> str:
    w = os.environ.get("SLACK_INSPECTION_WEBHOOK_URL", "").strip()
    if w:
        return w
    if WEBHOOK_FILE.is_file():
        for line in WEBHOOK_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and line.startswith("https://hooks.slack.com/"):
                return line
    return ""


def _post_slack(webhook: str, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        webhook, data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status < 200 or resp.status >= 300:
                raise RuntimeError(f"Slack HTTP {resp.status}")
    except urllib.error.HTTPError as e:
        raise RuntimeError(
            f"Slack HTTP {e.code}: {e.read().decode('utf-8', errors='replace')}"
        ) from e


# ─── 스냅샷 ───────────────────────────────────────────────────────────────────

def _load_snapshot() -> dict[str, str]:
    if SNAPSHOT_FILE.is_file():
        try:
            return json.loads(SNAPSHOT_FILE.read_text(encoding="utf-8")).get("statuses", {})
        except Exception:
            pass
    return {}


def _save_snapshot(statuses: dict[str, str]) -> None:
    SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_FILE.write_text(
        json.dumps({"updated_at": _kst_now(), "statuses": statuses}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _kst_now() -> str:
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Seoul")
    except Exception:
        tz = timezone(timedelta(hours=9))
    return datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")


# ─── 상태 한글 변환 ───────────────────────────────────────────────────────────

_ALL_LABELS: dict[str, str] = {
    "UNDER_REVIEW": "검토 중", "UNDER_REVIEW_VERIFY": "검수 진행 중",
    "REVIEW_VERIFIED": "검토 완료", "WAIT": "검수 보류",
    "APPROVED": "승인", "REJECTED": "반려",
    "PENDING": "검수 대기", "PENDING_IN_OPERATION": "운영 중 보류",
    "ACCEPT": "승인/검토 완료", "REJECT": "반려",
    "REJECT_IN_OPERATION": "운영 중 반려", "DELETED": "삭제",
    "": "알 수 없음",
}


def _label(status: str) -> str:
    return _ALL_LABELS.get(status, status)


# ─── 변경 감지 ────────────────────────────────────────────────────────────────

def _is_recent(dt_str: str, days: int = 3) -> bool:
    """
    ISO8601 날짜 문자열(UTC)이 오늘 기준 N일 이내(KST)인지 확인.
    기본 3일: 오늘 + 어제 + 그저께 등록/수정된 소재 포함.
    """
    if not dt_str:
        return False
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Seoul")
    except Exception:
        tz = timezone(timedelta(hours=9))
    try:
        dt_utc = datetime.strptime(dt_str[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        now_kst = datetime.now(tz)
        delta = now_kst - dt_utc.astimezone(tz)
        return delta.days < days
    except Exception:
        return False


def _final_alert_type(new_composite: str, prev_composite: str) -> str | None:
    """
    최종 알림 유형 결정.
      검수 보류  : statusReason=AD_DISAPPROVED 또는 inspectStatus=WAIT/PENDING
      검토 완료  : inspectStatus=REVIEW_VERIFIED
      검토 완료 (보류 해제): 이전이 보류였고 지금이 승인/통과
      검토 완료 (신규 통과): 처음 보는 소재인데 바로 승인/통과 상태
    """
    inspect = new_composite.split("|", 1)[0] if new_composite else ""
    reason  = new_composite.split("|", 1)[1] if "|" in new_composite else ""

    # 검수 보류
    if reason == "AD_DISAPPROVED" or inspect in ("WAIT", "PENDING"):
        return "검수 보류"

    # 검토 완료 (REVIEW_VERIFIED)
    if inspect == "REVIEW_VERIFIED":
        return "검토 완료"

    # 이전이 보류였는데 지금 승인 → 보류 해제
    prev_inspect = prev_composite.split("|", 1)[0] if prev_composite else ""
    prev_reason  = prev_composite.split("|", 1)[1] if "|" in prev_composite else ""
    prev_was_hold = (prev_reason == "AD_DISAPPROVED" or prev_inspect in ("WAIT", "PENDING"))
    if prev_was_hold and inspect in ("APPROVED", "ELIGIBLE"):
        return "검토 완료 (보류 해제)"

    # 처음 보는 소재(prev 없음)인데 이미 승인 상태 → 신규 통과
    if not prev_composite and inspect in ("APPROVED", "ELIGIBLE"):
        return "검토 완료 (신규 통과)"

    return None


def _detect_search_changes(
    prev: dict[str, str],
    curr_snapshot: dict[str, dict],
) -> list[dict]:
    """
    브랜드검색 소재 변경 감지.

    알림 조건 요약:
      [처음 보는 소재] regTm/editTm 최근 3일 이내인 경우만:
        · 검수 보류 상태  → ⚠️ 검수 보류
        · 승인/통과 상태  → ✅ 검토 완료 (신규 통과)
        · 검수 완료 상태  → ✅ 검토 완료

      [기존 추적 소재] 상태가 바뀐 경우:
        · 다른 상태 → 보류  → ⚠️ 검수 보류
        · 보류 → 승인       → ✅ 검토 완료 (보류 해제)
        · 검수중 → 검토완료  → ✅ 검토 완료
    """
    changes = []
    for ad_id, ad_info in curr_snapshot.items():
        new_composite = ad_info.get("status", "")
        prev_composite = prev.get(ad_id, "")

        if new_composite == prev_composite:
            continue

        alert_type = _final_alert_type(new_composite, prev_composite)
        if not alert_type:
            continue

        if not prev_composite:
            # ── 처음 보는 소재: 최근 3일 이내만 알림 ──
            edit_tm = ad_info.get("editTm", "")
            reg_tm  = ad_info.get("regTm", "")
            if not (_is_recent(edit_tm) or _is_recent(reg_tm)):
                continue

            # "신규 통과"는 이전 상태가 없으니 중복 방지 불필요
        else:
            # ── 기존 추적 소재 ──
            prev_alert = _final_alert_type(prev_composite, "")
            # 동일 알림 유형으로 재알림 방지
            if alert_type == prev_alert:
                continue
            # 검토 완료는 이전 상태가 '보류' 또는 '검수 진행 중'일 때만
            if "검토 완료" in alert_type:
                prev_inspect = prev_composite.split("|", 1)[0]
                prev_was_hold = prev_alert == "검수 보류"
                prev_in_review = prev_inspect in _SEARCH_IN_REVIEW
                if not (prev_was_hold or prev_in_review):
                    continue

        changes.append({
            "ad": ad_info,
            "prev_status": prev_composite,
            "new_status": new_composite,
            "alert_type": alert_type,
        })
    return changes


def _detect_display_changes(
    prev: dict[str, str],
    curr_snapshot: dict[str, dict],
    alert_statuses: dict[str, str],
    review_in_progress: set[str],
) -> list[dict]:
    """
    성과형 디스플레이 변경 감지.
    검수 보류: 이전 상태와 달라지면 알림
    검토 완료: 이전 상태가 검수 진행 중일 때만 알림
    """
    changes = []
    for ad_id, ad_info in curr_snapshot.items():
        new_status = ad_info.get("status", "")
        prev_status = prev.get(ad_id, "")
        if new_status not in alert_statuses:
            continue
        if new_status == prev_status:
            continue
        label = alert_statuses[new_status]
        if label == "검토 완료" and prev_status not in review_in_progress:
            continue
        changes.append({"ad": ad_info, "prev_status": prev_status, "new_status": new_status,
                        "alert_type": label})
    return changes


# ─── Slack 메시지 빌드 ────────────────────────────────────────────────────────

_BATCH_SIZE = 10  # 한 메시지당 최대 소재 수


def _escape(text: str) -> str:
    """Slack mrkdwn 특수문자 이스케이프."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_payloads(all_changes: list[dict], now_str: str) -> list[dict]:
    """
    Slack attachment 방식으로 메시지 구성.
    Block Kit을 사용하지 않아 invalid_blocks 오류 없음.
    10건 초과 시 여러 메시지로 분할.
    """
    hold_items = [c for c in all_changes if c.get("alert_type") == "검수 보류"]
    done_items = [c for c in all_changes if "검토 완료" in (c.get("alert_type") or "")]

    parts = []
    if hold_items:
        parts.append(f":warning: 검수 보류 {len(hold_items)}건")
    if done_items:
        parts.append(f":white_check_mark: 검토 완료 {len(done_items)}건")
    header = f"*[네이버 소재 검수 알림]* {', '.join(parts)}\n기준 시각: {now_str}"

    def _attachment(change: dict) -> dict:
        ad = change["ad"]
        alert_type = change.get("alert_type", "")
        color = "danger" if "보류" in alert_type else "good"
        emoji = ":warning:" if "보류" in alert_type else ":white_check_mark:"
        platform = _escape(ad.get("platform", "검색광고"))
        headline = _escape(ad.get("headline") or ad.get("adId", ""))
        campaign = _escape(ad.get("campaign") or "-")
        adgroup  = _escape(ad.get("adgroup") or "-")
        return {
            "color": color,
            "mrkdwn_in": ["text"],
            "text": (
                f"{emoji} *{alert_type}* [{platform}]\n"
                f"소재: {headline}\n"
                f"캠페인: {campaign}  /  광고그룹: {adgroup}"
            ),
        }

    all_attachments = [_attachment(c) for c in hold_items + done_items]

    payloads = []
    for i in range(0, max(len(all_attachments), 1), _BATCH_SIZE):
        batch = all_attachments[i:i + _BATCH_SIZE]
        idx = i // _BATCH_SIZE
        total = (len(all_attachments) + _BATCH_SIZE - 1) // _BATCH_SIZE
        suffix = f" ({idx + 1}/{total})" if total > 1 else ""
        payloads.append({
            "text": header + suffix,
            "attachments": batch,
        })
    return payloads


# ─── 플랫폼별 스냅샷 수집 ─────────────────────────────────────────────────────

def _collect_search_snapshot(verbose: bool) -> dict[str, dict]:
    """검색광고 소재 검수 상태 수집."""
    from src.naver_ads.auth import load_credentials
    from src.naver_ads.client import NaverAdsClient

    creds = load_credentials()
    client = NaverAdsClient(creds)
    if verbose:
        print("  [검색광고] 캠페인/광고그룹 조회 후 소재 병렬 수집 중...", flush=True)
    snapshot = client.snapshot_review_statuses(verbose=verbose)
    # platform 태그 추가
    for info in snapshot.values():
        info.setdefault("platform", "검색광고")
    if verbose:
        print(f"  [검색광고] 소재 {len(snapshot)}개 조회 완료", flush=True)
    return snapshot


def _collect_display_snapshot(verbose: bool) -> dict[str, dict]:
    """성과형 디스플레이 소재 검수 상태 수집 (광고 계정 목록 파일 기반)."""
    from src.naver_ads.display_auth import load_display_credentials
    from src.naver_ads.display_client import NaverDisplayClient

    creds = load_display_credentials()

    # 광고 계정 ID 목록 읽기
    ad_account_nos: list[str] = []
    if DISPLAY_AD_ACCOUNTS_FILE.is_file():
        for line in DISPLAY_AD_ACCOUNTS_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and line.isdigit():
                ad_account_nos.append(line)

    if not ad_account_nos:
        # 파일이 없으면 관리 계정 하위 광고 계정을 자동 조회
        dummy_client = NaverDisplayClient(creds, ad_account_no=0)
        accounts = dummy_client.list_ad_accounts()
        ad_account_nos = [str(a.get("no") or a.get("adAccountNo") or "") for a in accounts if a.get("no") or a.get("adAccountNo")]
        if verbose and ad_account_nos:
            print(f"  [성과형] 관리 계정 하위 광고 계정 {len(ad_account_nos)}개 자동 감지", flush=True)

    if not ad_account_nos:
        if verbose:
            print("  [성과형] 광고 계정 없음 — 건너뜀", flush=True)
        return {}

    combined: dict[str, dict] = {}
    for acc_no in ad_account_nos:
        client = NaverDisplayClient(creds, ad_account_no=acc_no)
        if verbose:
            print(f"  [성과형] 광고 계정 {acc_no} 소재 조회 중...", flush=True)
        try:
            snap = client.snapshot_review_statuses()
            combined.update(snap)
            if verbose:
                print(f"  [성과형] 광고 계정 {acc_no} — 소재 {len(snap)}개", flush=True)
        except Exception as e:
            print(f"  [성과형] 광고 계정 {acc_no} 조회 실패: {e}", file=sys.stderr, flush=True)

    return combined


# ─── 1회 실행 ─────────────────────────────────────────────────────────────────

def run_once(webhook: str, dry_run: bool, verbose: bool, only: str) -> int:
    now_str = _kst_now()
    prev_statuses = _load_snapshot()
    all_changes: list[dict] = []
    # 이전 스냅샷을 기본값으로 유지 → API 누락 시 재감지/중복 알림 방지
    curr_statuses: dict[str, str] = dict(prev_statuses)
    errors = 0

    # 검색광고
    if only in ("all", "search"):
        try:
            search_snap = _collect_search_snapshot(verbose)
            curr_statuses.update({k: v["status"] for k, v in search_snap.items()})
            all_changes.extend(
                _detect_search_changes(prev_statuses, search_snap)
            )
        except Exception as e:
            import traceback
            print(f"[검색광고 오류] {type(e).__name__}: {e}", flush=True)
            traceback.print_exc()
            errors += 1

    # 성과형 디스플레이
    if only in ("all", "display"):
        try:
            display_snap = _collect_display_snapshot(verbose)
            curr_statuses.update({k: v["status"] for k, v in display_snap.items()})
            all_changes.extend(
                _detect_display_changes(prev_statuses, display_snap, DISPLAY_ALERT_STATUSES, DISPLAY_REVIEW_IN_PROGRESS)
            )
        except Exception as e:
            import traceback
            print(f"[성과형 오류] {type(e).__name__}: {e}", flush=True)
            traceback.print_exc()
            errors += 1

    # 변경 없음
    if not all_changes:
        if verbose:
            print(f"[{now_str}] 상태 변경 없음 (검수 보류/검토 완료)", flush=True)
        _save_snapshot(curr_statuses)
        return 0 if errors == 0 else 1

    hold_cnt = sum(1 for c in all_changes if c.get("alert_type") == "검수 보류")
    done_cnt = sum(1 for c in all_changes if "검토 완료" in (c.get("alert_type") or ""))

    if dry_run:
        print(f"[dry-run] 검수 보류: {hold_cnt}건, 검토 완료: {done_cnt}건 ({now_str})", flush=True)
        for c in all_changes:
            ad = c["ad"]
            prev_label = _label(c["prev_status"].split("|")[0]) if c["prev_status"] else "알 수 없음"
            print(
                f"  [{ad.get('platform', '-')}] {ad.get('headline', ad.get('adId', ''))} "
                f"| {prev_label} → {c.get('alert_type', c['new_status'])}"
                f" | 캠페인: {ad.get('campaign', '-')}"
            )
    else:
        payloads = _build_payloads(all_changes, now_str)
        sent = 0
        for payload in payloads:
            try:
                _post_slack(webhook, payload)
                sent += 1
            except Exception as e:
                print(f"Slack 전송 실패 (배치 {sent+1}): {e}", file=sys.stderr, flush=True)
                errors += 1
        if sent > 0:
            print(f"[{now_str}] Slack 전송 완료 — 보류: {hold_cnt}건, 완료: {done_cnt}건 ({sent}/{len(payloads)} 배치)", flush=True)

    _save_snapshot(curr_statuses)
    return 0 if errors == 0 else 1


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description="네이버 광고 소재 검수 상태 변경 → Slack 알림")
    ap.add_argument("--only", choices=["all", "search", "display"], default="all",
                    help="실행 대상 플랫폼 (기본: all)")
    ap.add_argument("--loop", action="store_true", help="폴링 모드 반복 실행")
    ap.add_argument("--interval", type=int, default=300, metavar="SEC",
                    help="폴링 간격(초), 기본값 300 (5분)")
    ap.add_argument("--dry-run", action="store_true", help="Slack 전송 없이 변경 내역만 출력")
    ap.add_argument("--verbose", "-v", action="store_true", help="상세 로그 출력")
    ap.add_argument("--log-file", metavar="PATH", help="로그를 파일에 기록 (Task Scheduler 용)")
    args = ap.parse_args()

    # 로그 파일 지정 시 stdout/stderr 리디렉션
    if args.log_file:
        log_path = Path(args.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if log_path.exists() and log_path.stat().st_size > 1 * 1024 * 1024:
            log_path.unlink()
        _log = open(log_path, "a", encoding="utf-8", buffering=1)
        sys.stdout = _log
        sys.stderr = _log

    webhook = _resolve_webhook()
    if not args.dry_run and not webhook:
        print(
            "Slack 웹훅이 없습니다.\n"
            f"  파일 생성: {WEBHOOK_FILE}\n"
            "  (첫 줄에 https://hooks.slack.com/services/... 저장)\n"
            "  확인: python scripts/run_naver_inspection_alert.py --dry-run",
            file=sys.stderr,
        )
        return 1

    if not args.loop:
        return run_once(webhook, args.dry_run, args.verbose, args.only)

    print(f"폴링 시작 — 플랫폼: {args.only}, 간격: {args.interval}초 (Ctrl+C 종료)", flush=True)
    while True:
        run_once(webhook, args.dry_run, args.verbose, args.only)
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n폴링 종료.", flush=True)
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
