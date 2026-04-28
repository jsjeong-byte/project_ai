# 키워드사운드(keywordsound.com) 검색량 → Google Sheets

## 동작 요약

- **incremental (기본)**: KST 기준으로 “오늘 넣어야 할 날짜”만 keywordsound에서 가져와, **해당 날짜 행·키워드 열 셀만** `batchUpdate` 합니다.  
  - **월요일(KST)**: 금·토·일 (`today-3`, `today-2`, `today-1`)  
  - **화~일(KST)**: **전일** 하루 (`today-1`)
- **행 채우기 규칙**
  - A열(날짜)에서 **위에서부터** 첫 빈 칸이 있는 행 = “이어 쓸 시작 행”입니다. (예: 2~200행에 값이 있고 201행 A가 비어 있으면 201행부터)
  - 채울 날짜가 시트에 **이미 A열에 있으면** 그 행의 키워드 열만 갱신합니다.
  - 시트에 없는 새 날짜는, **실제로 keywordsound에서 값이 나온 키워드가 있을 때** 그때 A열 행을 새로 잡습니다(빈 A행만 만들고 수치가 비는 일을 줄임).

## 시트 안 메뉴 버튼 (Apps Script → GitHub Actions)

keywordsound 데이터는 **브라우저 JS(DataTables)** 이후에만 생기므로, 시트 버튼만으로 같은 프로세스를 끝내기 어렵습니다. 대신:

1. 스프레드시트에 **Apps Script**를 붙여 메뉴를 만듭니다. (레포: `google-apps-script/KeywordsoundSyncMenu.gs` 내용 복사)
2. 메뉴 실행 시 **GitHub `repository_dispatch`** 로 이 레포의 워크플로를 깨웁니다.
3. **GitHub Actions(ubuntu)** 가 Selenium으로 수집 후 시트에 **증분 반영**합니다. (보통 1~2분 지연)

### Apps Script 스크립트 속성

| 속성 | 설명 |
|------|------|
| `GITHUB_TOKEN` | `repo` 권한 PAT 등 (Actions 트리거 가능한 토큰) |
| `GITHUB_REPO` | `owner/repo` (예: `madupmarketing/project_ai`) |

### GitHub 쪽

- 워크플로: `.github/workflows/keywordsound_volume_sync.yml`
- 이벤트 타입: `keywordsound-sync` (`repository_dispatch`)
- Secrets (**둘 중 하나는 필수**)
  - **`GOOGLE_SERVICE_ACCOUNT_JSON`** (권장): 증분 시 `batchUpdate` 로 빠르게 반영
  - **`PROXY_API_KEY`**: 키워드 읽기 + 프록시 upload(느리지만 동작). 이 경우 incremental 도 **A1:U204 전체를 읽고 일부 수정 후 overwrite** 합니다.

서비스 계정 이메일에 스프레드시트 **편집 권한**을 공유해야 합니다.

## 로컬 / Actions CLI

```bash
pip install -r requirements-keywordsound.txt

# KST 규칙으로 날짜 자동 + 증분(기본)
python scripts/keywordsound_volume_sync.py \
  --spreadsheet-id 1M0JyzNtz5jQNguET_GksQ9FDtbWqIsfp_GqEXqG1tC0 \
  --gid 1001307486

# 날짜를 직접 지정
python scripts/keywordsound_volume_sync.py \
  --spreadsheet-id 1M0JyzNtz5jQNguET_GksQ9FDtbWqIsfp_GqEXqG1tC0 \
  --gid 1001307486 \
  --dates 2026-04-25,2026-04-26,2026-04-27

# 예전 방식(전체 일자 수집 + 프록시 전체 덮어쓰기)
python scripts/keywordsound_volume_sync.py \
  --spreadsheet-id 1M0JyzNtz5jQNguET_GksQ9FDtbWqIsfp_GqEXqG1tC0 \
  --gid 1001307486 \
  --mode full
```

### 쓰기 백엔드

- `--write-backend auto` (기본): Google 서비스 계정을 만들 수 있으면 `google`, 아니면 `proxy`
- `--write-backend google`: `SheetsClient.batchUpdate` (부분 쓰기)
- `--write-backend proxy`: `A1:U204` 읽고 → 수정 → `upload` overwrite

Google 자격은 다음 중 하나입니다.

- 환경변수 `GOOGLE_SERVICE_ACCOUNT_JSON` (JSON 문자열)
- 환경변수 `SERVICE_ACCOUNT_JSON` (파일 경로)
- `.credentials/cost_report_config.txt` 의 `SERVICE_ACCOUNT_JSON=...`

## 제한 사항

- 표는 **A열=날짜, 1행=키워드** 구조를 가정합니다. `target-range` 기본 `A1:U204`.
- A열 “첫 빈 행” 판단은 Sheets API가 **끝쪽 빈 행을 생략**할 수 있어, **중간에 빈 행이 있는 표**에서는 의도와 다르게 동작할 수 있습니다. (연속으로 쌓인 표에 맞춤)

## 대안

- **Make / n8n / 사내 웹훅**: 시트 버튼 → 웹훅 → 동일 Python 실행 (인프라만 다름).
- **Apps Script 단독**: keywordsound HTML만으로는 테이블 데이터가 없어 `UrlFetchApp` 만으로는 불가에 가깝습니다.
