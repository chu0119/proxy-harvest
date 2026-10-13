@echo off
title Proxy Scraper Tool
echo.
echo ========================================
echo   Proxy Scraper Tool
echo ========================================
echo.
python --version >/dev/null 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found. Please install Python 3.7+
    echo Download: https://www.python.org/downloads/
    echo.
    pause
    exit /b 1
)
echo.
echo Starting... This may take 5-15 minutes.
echo.
python proxy_scraper.py
echo.
echo ========================================
echo   Done!
echo ========================================
echo.
echo Output files:
echo   proxies_top20.txt   - Top 20 recommended
echo   proxies.txt         - All available
echo   proxies.csv         - Full details
echo.
pause
