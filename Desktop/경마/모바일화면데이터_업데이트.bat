@echo off
echo ===========================================
echo Fixing connections and Uploading Everything
echo Please wait about 10 seconds.
echo ===========================================
cd /d "%~dp0"
git add .
git commit -m "Auto sync from PC"
git pull origin main --rebase || git pull origin master --rebase
git push origin main || git push origin master
echo.
echo ===========================================
echo 100%% DONE! 
echo Now return to the cloud app, wait 1 minute, 
echo and refresh the browser.
echo ===========================================
pause
