/**
 * 키워드사운드 검색량 → 시트 증분 반영 트리거 (GitHub Actions)
 *
 * 설정: 스프레드시트에 연결된 Apps Script 프로젝트 → 프로젝트 설정 → 스크립트 속성
 *   GITHUB_TOKEN   : classic PAT (repo 범위) 또는 fine-grained (contents+metadata, actions 쓰기 권한)
 *   GITHUB_REPO    : owner/repo  (예: madupmarketing/project_ai)
 *
 * 동작: repository_dispatch 이벤트 keywordsound-sync 를 쏴서
 *       이 레포의 GitHub Actions 워크플로가 Selenium으로 수집 후 시트에 부분 기록합니다.
 *       (시트 반영은 액션이 끝난 뒤 1~2분 정도 걸릴 수 있습니다.)
 */

/** 키워드/표 범위는 시트 구조에 맞게 수정 */
var DEFAULT_KEYWORDS_RANGE = 'AH26:AH31';
var DEFAULT_TARGET_RANGE = 'A1:U204';

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('키워드사운드')
    .addItem('전일(·월요일은 주말) 반영 요청', 'requestKeywordSoundIncrementalSync')
    .addToUi();
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
  var wd = today.getDay(); // Sun=0 ... Sat=6  (자바스크립트)

  // Python 쪽과 동일: 월요일(1)=금·토·일, 그 외=전일
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
  var token = props.getProperty('GITHUB_TOKEN');
  var repo = props.getProperty('GITHUB_REPO');
  if (!token || !repo) {
    SpreadsheetApp.getUi().alert(
      '스크립트 속성에 GITHUB_TOKEN, GITHUB_REPO 를 먼저 설정하세요.\n' +
        '(GitHub 저장소에 repository_dispatch 권한이 있는 토큰)'
    );
    return;
  }

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  /** URL의 #gid= 와 동일 — 메뉴 실행 시점의 활성 탭 기준 */
  var gid = ss.getActiveSheet().getSheetId();
  var payload = {
    spreadsheet_id: ss.getId(),
    gid: gid,
    keywords_range: DEFAULT_KEYWORDS_RANGE,
    target_range: DEFAULT_TARGET_RANGE,
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
    SpreadsheetApp.getUi().alert('GitHub 요청 실패 HTTP ' + code + '\n' + resp.getContentText().slice(0, 500));
    return;
  }

  SpreadsheetApp.getUi().alert(
    '동기화를 요청했습니다.\n' +
      '대상 날짜: ' +
      payload.dates.join(', ') +
      '\n\n1~2분 후 시트를 새로고침해 확인하세요.'
  );
}
