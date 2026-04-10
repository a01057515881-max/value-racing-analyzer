@echo off
chcp 65001 >nul
echo ========================================================
echo     AI Racing Radar: Live Telegram Monitor
echo ========================================================
echo.
echo Scraping live weights, odds, and jockey changes 15 mins
echo before the race starts and sending to Telegram.
echo.
echo Please leave this window OPEN during race days!
echo.
set /p meet="Choose Meet (0: Auto, 1: Seoul, 2: Jeju, 3: Busan) [Default: 0]: "
if "%meet%"=="" set meet=0

if exist venv\Scripts\activate call venv\Scripts\activate

echo.
echo Starting the live monitor daemon...
python live_monitor.py --meet %meet%

pause
