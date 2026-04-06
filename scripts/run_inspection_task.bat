@echo off
set PYTHON=C:\Users\MADUP\AppData\Local\Programs\Python\Python313\python.exe
set SCRIPT=C:\Users\MADUP\project_ai\scripts\run_naver_inspection_alert.py
set LOGFILE=C:\Users\MADUP\project_ai\logs\inspection_alert.log

"%PYTHON%" -u "%SCRIPT%" --only search --verbose --log-file "%LOGFILE%"
