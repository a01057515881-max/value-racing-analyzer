@echo off
chcp 65001 >nul
echo ========================================================
echo     Running Auto Analysis Pipeline
echo ========================================================
echo.
echo Please wait until the process is fully complete...
echo.

if exist venv\Scripts\activate call venv\Scripts\activate

python main.py

echo.
echo ========================================================
echo Analysis finished successfully!
echo You can now close this window safely.
echo ========================================================
pause > nul
