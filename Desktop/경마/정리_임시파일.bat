@echo off
echo =========================================
echo Cleaning up temporary backtest scripts...
echo =========================================

del /Q /F tmp_analyze_jan.py 2>nul
del /Q /F fix_speed_typo.py 2>nul
del /Q /F backtest_radar_mar*.py 2>nul

echo Emptying /tmp folder...
rmdir /S /Q tmp 2>nul
mkdir tmp

echo Emptying /archive_trash folder...
rmdir /S /Q archive_trash 2>nul
mkdir archive_trash

echo.
echo Cleanup complete!
pause
