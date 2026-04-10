@echo off
echo ==================================================
echo [Sniper v7.3 Diagnosis Mode]
echo ==================================================
echo.
echo Trying to run March Calibration...
python tmp\mar_calibration.py
if %errorlevel% neq 0 (
    echo.
    echo [ERROR] python command failed or script crashed with error code %errorlevel%
)
echo.
echo ==================================================
echo Check the message above! Press any key to close.
echo ==================================================
pause
