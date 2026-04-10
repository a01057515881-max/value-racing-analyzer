@echo off
chcp 65001 >nul
echo ========================================================
echo        KRA Horse Racing Analyzer (AI Powered)
echo ========================================================
echo.
echo Checking and auto-analyzing today's races...
echo.
dist\KRA_Analyzer.exe --meet 1
echo.
echo Analysis Complete.
pause
