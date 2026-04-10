@echo off
echo ==================================================
echo [Sniper v7.3 Final Integrated Verification]
echo ==================================================
echo.
echo Step 1: Running March Calibration...
python tmp\mar_calibration.py > sniper_verify.log 2>&1

echo Step 2: Running April Blind Test...
python tmp\apr_oos_backtest_with_mdd.py >> sniper_verify.log 2>&1

echo.
echo Step 3: Converting results for AI analysis...
powershell -Command "Get-Content sniper_verify.log | Set-Content -Encoding utf8 sniper_result.txt"

echo.
echo ==================================================
echo [ALL PROCESS COMPLETE] Results saved to sniper_result.txt
echo Please tell the agent "Done".
echo ==================================================
pause
