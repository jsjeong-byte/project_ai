' 네이버 소재 검수 알림 — 콘솔 창 없이 실행 (배치 파일 래퍼)
' WScript.Shell.Run 두 번째 인자 0 = SW_HIDE

Dim bat, shell
bat = "C:\Users\MADUP\project_ai\scripts\run_inspection_task.bat"
Set shell = CreateObject("WScript.Shell")
shell.Run """" & bat & """", 0, False
