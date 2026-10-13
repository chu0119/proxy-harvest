@echo off
title Setup Daily Auto Update
echo.
echo ========================================
echo   Setup Daily Auto Update (9:00 AM)
echo ========================================
echo.
set SCRIPT_DIR=%~dp0
set BAT_PATH=%SCRIPT_DIR%start_proxy.bat
schtasks /create /tn "DailyProxyUpdate" /tr "cmd /c "%BAT_PATH%"" /sc daily /st 09:00 /f
if errorlevel 1 (
    echo [ERROR] Failed. Please run as Administrator.
    echo.
    pause
    exit /b 1
)
echo.
echo OK! Daily task created (9:00 AM every day).
echo.
echo Manage:
echo   View:   schtasks /query /tn "DailyProxyUpdate"
echo   Delete: schtasks /delete /tn "DailyProxyUpdate"
echo   Run:    schtasks /run /tn "DailyProxyUpdate"
echo.
pause
