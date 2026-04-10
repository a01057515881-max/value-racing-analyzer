@echo off
echo.
echo [INFO] Starting KRA AI Horse Racing Analyzer...
echo [INFO] The web browser will open automatically. Please wait.
echo.
if exist venv\Scripts\activate call venv\Scripts\activate
streamlit run app.py
pause
