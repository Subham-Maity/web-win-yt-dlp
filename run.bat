@echo off
title yt-dlp + BullMQ Pro Downloader
color 0B
cls

echo.
echo  ================================================
echo   yt-dlp + BullMQ Downloader — Starting...
echo  ================================================
echo.

:: 1. Check Node.js
where node >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Node.js not found. Please install it from https://nodejs.org
    pause
    exit /b 1
)

:: 2. Check and Start Docker Redis for BullMQ
echo  [1/4] Checking Docker Redis service for BullMQ...
where docker >nul 2>&1
if errorlevel 1 (
    echo  [WARN] Docker CLI not found. BullMQ will fallback to direct mode.
    goto :check_node_deps
)

docker info >nul 2>&1
if errorlevel 1 (
    echo  [WARN] Docker daemon is not active. BullMQ will fallback to direct mode.
    goto :check_node_deps
)

echo  [OK] Docker daemon is running. Starting Redis container...
cd /d "%~dp0"
call docker compose up -d

:check_node_deps
:: 3. Install Node.js dependencies if missing
echo.
echo  [2/4] Checking Node.js dependencies...
if not exist "%~dp0node_modules\bullmq" (
    echo  [INFO] First run or new dependencies detected - running npm install...
    echo.
    cd /d "%~dp0"
    call npm install
    echo.
) else (
    echo  [OK] Node dependencies are ready.
)

:: 4. Check Python and Instaloader for Instagram Profile Downloads
echo.
echo  [3/4] Checking Instagram Profile Downloader dependencies...
where python >nul 2>&1
if errorlevel 1 (
    echo  [INFO] Python not found - gallery-dl.exe fallback will be used.
    goto :start_server
)

python -c "import instaloader, browser_cookie3" >nul 2>&1
if errorlevel 1 (
    echo  [INFO] Installing instaloader and browser-cookie3 via pip...
    call python -m pip install instaloader browser-cookie3
) else (
    echo  [OK] Instaloader and browser-cookie3 are ready.
)

:start_server
:: 5. Start Server and Open Browser
echo.
echo  [4/4] Launching Web UI and starting server...
cd /d "%~dp0"
echo  [OK] Opening browser at http://localhost:3456
ping -n 2 127.0.0.1 >nul 2>&1
start "" "http://localhost:3456"

echo.
echo  Press Ctrl+C to stop the server.
echo  ================================================
echo.

node server.js
pause
