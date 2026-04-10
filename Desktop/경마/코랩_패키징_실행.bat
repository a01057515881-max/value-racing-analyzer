@echo off
chcp 65001 >nul
echo ============================================
echo   Preparing Colab Package...
echo ============================================
echo.
if exist venv\Scripts\activate call venv\Scripts\activate
python prepare_colab.py
echo.
echo ============================================
echo   Done!
echo ============================================
pause
