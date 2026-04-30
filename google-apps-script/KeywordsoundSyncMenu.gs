/**
 * 키워드사운드 검색량 → 시트 증분 반영 트리거 (GitHub Actions)
 *
 * [필수 설정] 프로젝트 설정 → 스크립트 속성
 *   GITHUB_TOKEN : GitHub PAT
 *     - Classic: repo 범위 (공식 문서 기준)
 *     - Fine-grained: 해당 레포 선택 + Repository permissions → Contents → Read and write
 *       (Actions만 켜두면 403 "Resource not accessible by personal access token" 납니다)
 *   GITHUB_REPO  : owner/repo  (예: jsjeong-byte/project_ai)
 *
 * [중요] 커스텀 메뉴는 "시트 셀 안 버튼"이 아니라, 시트 상단 메뉴바(파일/수정/…/도움말 근처)에
 *       「키워드사운드」 라는 항목으로 붙습니다. 모바일 앱에서는 안 보일 수 있습니다.
 *
 * [어떤 시트에 붙는지] Apps Script는 "그 스프레드시트에서 확장 프로그램 → Apps Script 를 연 시트"에만 붙습니다.
 *   다른 URL의 시트에 메뉴를 만들려면, 그 시트를 연 뒤 거기서 다시 Apps Script 프로젝트를 열고
 *   이 코드·스크립트 속성·트리거를 그쪽에 복사해야 합니다. (한 프로젝트를 시트 간 이동 불가)
 *
 * [시트에 메뉴가 안 보일 때]
 *
 *  (A) 편집기 상단에 함수 선택 드롭다운이 있으면
 *      → INSTALL_ONCE_addOnOpenTrigger 선택 후 실행 → 시트 새로고침
 *
 *  (B) 함수 선택이 안 보일 때 — 트리거 화면으로 설치 (권장)
 *      왼쪽 시계 아이콘 "트리거" → "트리거 추가"
 *        이벤트 소스: 스프레드시트에서
 *        이벤트 유형: 열 때
 *        실행할 함수: whenSpreadsheetOpens
 *      저장 → 시트 새로고침
 *
 *  (C) 상단 메뉴바 "실행" 클릭 → 함수 목록에서 INSTALL_ONCE_addOnOpenTrigger 실행
 *
 *  공통: 스크립트는 해당 스프레드시트에 바인딩(확장 프로그램 → Apps Script)되어 있어야 함
 *
 * 동작: repository_dispatch (keywordsound-sync) → GitHub Actions → 시트 반영
 */

/** 키워드 목록(시트에 실제로 있는 열만). 콤마로 여러 개 한 셀 가능. 열이 좁으면 AH 대신 A 등 */
var DEFAULT_KEYWORDS_RANGE = 'A26:A31';
var DEFAULT_TARGET_RANGE = 'A1:U204';
/** 날짜를 쓰고·빈 행을 찾는 열 (A열이 꽉 차 있고 B열만 비면 'B') */
var DEFAULT_DATE_COLUMN = 'B';

/** 스크립트 속성 값 정리 (공백·URL 붙여넣기 실수 방지) */
function normalizeGithubRepo_(r) {
  r = (r || '').toString().trim();
  if (!r) return r;
  r = r.replace(/^https?:\/\/github\.com\//i, '');
  r = r.replace(/^github\.com\//i, '');
  r = r.replace(/\.git$/i, '');
  return r.trim();
}

function normalizeGithubToken_(t) {
  t = (t || '').toString().trim();
  // 붙여넣기 시 줄바꿈만 제거 (토큰 본문에 공백 없음 가정)
  return t.replace(/\r|\n|\t/g, '');
}

/** 메뉴 생성 — 성공 시 true */
function addKeywordSoundMenu_() {
  try {
    SpreadsheetApp.getUi()
      .createMenu('키워드사운드')
      .addItem('전일 반영 요청 (월요일=금~일)', 'requestKeywordSoundIncrementalSync')
      .addToUi();
    return true;
  } catch (e) {
    var msg = e && e.message ? e.message : String(e);
    SpreadsheetApp.getActiveSpreadsheet().toast(
      '메뉴 추가 실패: ' + msg + ' / 설치형 트리거·편집 권한·PC 웹 브라우저를 확인하세요.',
      '키워드사운드',
      25
    );
    return false;
  }
}

/**
 * 단순 onOpen — 일부 환경에서만 동작. 설치형 트리거가 있으면 중복 메뉴를 막기 위해 스킵.
 */
function onOpen() {
  if (hasInstallableOnOpenTrigger_()) {
    return;
  }
  addKeywordSoundMenu_(); // 단순 트리거: 일부 환경에서 getUi 제한
}

/** 설치형 스프레드시트 열 때 — 대부분 환경에서 메뉴 표시에 안정적 */
function whenSpreadsheetOpens() {
  if (!addKeywordSoundMenu_()) {
    return;
  }
  var up = PropertiesService.getUserProperties();
  if (!up.getProperty('KS_MENU_HINT_SHOWN')) {
    up.setProperty('KS_MENU_HINT_SHOWN', '1');
    SpreadsheetApp.getActiveSpreadsheet().toast(
      '상단 메뉴바에서 「키워드사운드」를 찾으세요. (표 안 버튼 아님, PC 웹 권장)',
      '안내',
      15
    );
  }
}

/**
 * 편집기에서 실행: 이 스프레드시트에 스크립트가 붙었는지 확인(토스트).
 * (getUi는 편집기에서 안 되지만 toast 는 종종 동작합니다.)
 */
function DEBUG_bindingCheck() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) {
    throw new Error('바인딩된 스프레드시트가 없습니다.');
  }
  ss.toast('연결 OK: ' + ss.getName() + ' / ' + ss.getId(), 'DEBUG', 10);
}

function hasInstallableOnOpenTrigger_() {
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    var t = triggers[i];
    if (t.getEventType() === ScriptApp.EventType.ON_OPEN && t.getHandlerFunction() === 'whenSpreadsheetOpens') {
      return true;
    }
  }
  return false;
}

/**
 * Apps Script 편집기에서 1회만 실행하세요. (메뉴가 안 뜰 때)
 * 스프레드시트에 바인딩된 프로젝트에서 실행해야 합니다.
 */
function INSTALL_ONCE_addOnOpenTrigger() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) {
    throw new Error('스프레드시트를 연 상태에서 이 프로젝트를 열어 주세요. (바인딩된 스크립트만 가능)');
  }
  var triggers = ScriptApp.getProjectTriggers();
  for (var i = 0; i < triggers.length; i++) {
    var t = triggers[i];
    if (t.getHandlerFunction() === 'whenSpreadsheetOpens') {
      ScriptApp.deleteTrigger(t);
    }
  }
  ScriptApp.newTrigger('whenSpreadsheetOpens').forSpreadsheet(ss).onOpen().create();
  return '설치 완료: 스프레드시트를 새로고침하면 메뉴가 보여야 합니다.';
}

function computeTargetDatesKst_() {
  var tz = 'Asia/Seoul';
  var now = new Date();
  var ymd = Utilities.formatDate(now, tz, 'yyyy-MM-dd');
  var parts = ymd.split('-');
  var y = parseInt(parts[0], 10);
  var m = parseInt(parts[1], 10) - 1;
  var d = parseInt(parts[2], 10);
  var today = new Date(y, m, d);
  var wd = today.getDay(); // Sun=0 ... Sat=6

  if (wd === 1) {
    var fri = new Date(today);
    fri.setDate(today.getDate() - 3);
    var sat = new Date(today);
    sat.setDate(today.getDate() - 2);
    var sun = new Date(today);
    sun.setDate(today.getDate() - 1);
    return [
      Utilities.formatDate(fri, tz, 'yyyy-MM-dd'),
      Utilities.formatDate(sat, tz, 'yyyy-MM-dd'),
      Utilities.formatDate(sun, tz, 'yyyy-MM-dd'),
    ];
  }
  var prev = new Date(today);
  prev.setDate(today.getDate() - 1);
  return [Utilities.formatDate(prev, tz, 'yyyy-MM-dd')];
}

function requestKeywordSoundIncrementalSync() {
  var props = PropertiesService.getScriptProperties();
  var token = normalizeGithubToken_(props.getProperty('GITHUB_TOKEN'));
  var repo = normalizeGithubRepo_(props.getProperty('GITHUB_REPO'));
  if (!token || !repo) {
    SpreadsheetApp.getUi().alert(
      '스크립트 속성에 GITHUB_TOKEN, GITHUB_REPO 를 먼저 설정하세요.\n' +
        '(GitHub 저장소에 repository_dispatch 권한이 있는 토큰)'
    );
    return;
  }

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var gid = ss.getActiveSheet().getSheetId();
  var payload = {
    spreadsheet_id: ss.getId(),
    gid: gid,
    keywords_range: DEFAULT_KEYWORDS_RANGE,
    target_range: DEFAULT_TARGET_RANGE,
    date_column: DEFAULT_DATE_COLUMN,
    mode: 'incremental',
    dates: computeTargetDatesKst_(),
  };

  var url = 'https://api.github.com/repos/' + repo + '/dispatches';
  var options = {
    method: 'post',
    contentType: 'application/json',
    muteHttpExceptions: true,
    headers: {
      Authorization: 'Bearer ' + token,
      Accept: 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
    },
    payload: JSON.stringify({
      event_type: 'keywordsound-sync',
      client_payload: payload,
    }),
  };

  var resp = UrlFetchApp.fetch(url, options);
  var code = resp.getResponseCode();
  if (code !== 204) {
    var body = resp.getContentText().slice(0, 800);
    var hdrs = resp.getHeaders() || {};
    var accepted =
      hdrs['X-Accepted-GitHub-Permissions'] ||
      hdrs['x-accepted-github-permissions'] ||
      '';
    var hint403 =
      '\n\n[403 "Resource not accessible…" 해결]\n' +
      '1) Fine-grained PAT: 반드시 Contents → Read and write (읽기 전용 X, Actions만 X).\n' +
      '   "Only select repositories"에 이 레포 포함: ' +
      repo +
      '\n' +
      '2) 토큰을 만든 GitHub 계정이 이 레포에 쓰기(Write) 권한이 있어야 합니다.\n' +
      '3) 조직 레포면 토큰 설정에서 SSO Authorize.\n' +
      '4) 가장 단순: Classic PAT 새로 발급 → repo 체크 → 스크립트 속성에 붙여넣기.\n' +
      (accepted ? '\nGitHub 안내 헤더: ' + accepted + '\n' : '');
    SpreadsheetApp.getUi().alert(
      'GitHub 요청 실패 HTTP ' + code + '\n' + body + (code === 403 ? hint403 : '')
    );
    return;
  }

  SpreadsheetApp.getUi().alert(
    '동기화를 요청했습니다.\n' +
      '대상 날짜: ' +
      payload.dates.join(', ') +
      '\n\n1~2분 후 시트를 새로고침해 확인하세요.'
  );
}

/* 파일은 반드시 위의 "}" 로 끝나야 합니다. 구문 오류 시 복사가 중간에 끊겼는지 확인하세요. */
