@echo off
echo Converting log to readable format...
powershell -Command "Get-Content sniper_verify.log | Set-Content -Encoding utf8 sniper_result.txt"
echo Done! Please tell the agent "Done".
pause
