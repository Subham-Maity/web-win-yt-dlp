@echo off
title yt-dlp Web Downloader
color 0B
cls

echo.
echo  ================================================
echo   yt-dlp Web Downloader — Starting...
echo  ================================================
echo.

:: Check Node.js
where node >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Node.js not found. Please install it from https://nodejs.org
    pause
    exit /b 1
)

:: Install dependencies if node_modules missing
if not exist "%~dp0node_modules\express" (
    echo  [INFO] First run — installing dependencies...
    echo.
    cd /d "%~dp0"
    call npm install
    echo.
)

:: Start server
cd /d "%~dp0"
echo  [OK] Opening browser at http://localhost:3456
timeout /t 1 /nobreak >nul
start "" "http://localhost:3456"

echo.
echo  Press Ctrl+C to stop the server.
echo  ================================================
echo.

node server.js
pause
