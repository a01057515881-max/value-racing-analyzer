@echo off
chcp 65001 >nul
echo ===================================================
echo     Local PC Backup Started...
echo ===================================================
echo.
echo Running local backup script...
python make_backup.py

echo.
echo ===================================================
echo Local Backup Completed Successfully!
echo ===================================================
echo.
pause
