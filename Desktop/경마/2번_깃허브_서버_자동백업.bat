@echo off
chcp 65001 >nul
echo ===================================================
echo     GitHub Backup Started...
echo ===================================================
echo.
echo [1/3] Staging modified files...
git add .
echo.
echo [2/3] Committing changes...
git commit -m "Auto Backup: %date% %time%"
echo.
echo [3/3] Pushing to remote server...
git push
echo.
echo ===================================================
echo Backup Completed Successfully!
echo ===================================================
echo.
pause
