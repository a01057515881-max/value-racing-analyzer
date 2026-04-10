@echo off
chcp 65001 >nul
echo ============================================
echo   Cleanup unused files/folders script
echo ============================================
echo.
echo This will delete cache, trash, and unused scripts.
echo Press any key to continue...
pause >nul

echo Starting cleanup...

if exist "archive_trash" rmdir /s /q "archive_trash"
if exist "archive_scripts" rmdir /s /q "archive_scripts"
if exist "__pycache__" rmdir /s /q "__pycache__"
if exist "data\html_cache" rmdir /s /q "data\html_cache"
if exist "data\backtest_logs" rmdir /s /q "data\backtest_logs"
if exist "data\ai_cache" rmdir /s /q "data\ai_cache"
if exist "data\temp_videos" rmdir /s /q "data\temp_videos"
if exist ".vscode" rmdir /s /q ".vscode"
if exist ".devcontainer" rmdir /s /q ".devcontainer"

if exist "tmp_analyze_jan.py" del /q "tmp_analyze_jan.py"
if exist "fix_speed_typo.py" del /q "fix_speed_typo.py"
if exist "backtest_radar_mar.py" del /q "backtest_radar_mar.py"
if exist "backtest_radar_mar_full.py" del /q "backtest_radar_mar_full.py"
if exist "backtest_radar_mar_final.py" del /q "backtest_radar_mar_final.py"
if exist "jeju_tables.json" del /q "jeju_tables.json"
if exist "bet_history.md" del /q "bet_history.md"
if exist "backtest_2h_report.md" del /q "backtest_2h_report.md"

echo.
echo ============================================
echo   Cleanup completed!
echo ============================================
pause
