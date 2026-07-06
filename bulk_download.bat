@echo off
setlocal enabledelayedexpansion

:: =============================================
::  yt-dlp Bulk Downloader — 3 Simultaneous
::  Place this .bat in the same folder as:
::    - bulk.txt  (your URLs, one per line)
::    - cookies.txt
:: =============================================

set NODE_PATH=C:\Program Files\nodejs\node.exe
set COOKIES=cookies.txt
set BULK_FILE=bulk.txt
set MAX_PARALLEL= 20

if not exist "%BULK_FILE%" (
    echo [ERROR] bulk.txt not found in this folder.
    pause
    exit /b 1
)

if not exist "%COOKIES%" (
    echo [ERROR] cookies.txt not found in this folder.
    pause
    exit /b 1
)

echo.
echo  Starting bulk download — %MAX_PARALLEL% at a time
echo  Reading URLs from: %BULK_FILE%
echo -----------------------------------------------

set /a running=0
set /a total=0
set /a done=0

:: Count total non-empty URLs
for /f "usebackq tokens=* delims=" %%U in ("%BULK_FILE%") do (
    set "line=%%U"
    if not "!line!"=="" (
        set /a total+=1
    )
)
echo  Total URLs found: %total%
echo.

:: Download loop
for /f "usebackq tokens=* delims=" %%U in ("%BULK_FILE%") do (
    set "url=%%U"

    :: Skip blank lines
    if not "!url!"=="" (
        set /a done+=1
        echo [!done!/%total%] Starting: !url!

        :: Launch yt-dlp in a new minimized window (non-blocking)
        start "yt-dlp !done!" /min yt-dlp ^
            --js-runtimes "node:%NODE_PATH%" ^
            --cookies "%COOKIES%" ^
            --no-playlist ^
            -o "%%(title)s.%%(ext)s" ^
            "!url!"

        set /a running+=1

        :: When we hit MAX_PARALLEL, wait for all current batch to finish
        if !running! geq %MAX_PARALLEL% (
            echo  [Batch full — waiting for %MAX_PARALLEL% downloads to complete...]
            :waitloop
            :: Count active yt-dlp processes
            set /a active=0
            for /f %%P in ('tasklist /fi "imagename eq yt-dlp.exe" /fo csv /nh 2^>nul ^| find /c "yt-dlp"') do set /a active=%%P
            if !active! geq %MAX_PARALLEL% (
                timeout /t 5 /nobreak >nul
                goto waitloop
            )
            set /a running=0
            echo  [Slot open — continuing...]
        )
    )
)

:: Wait for any remaining downloads to finish
echo.
echo  All URLs dispatched. Waiting for remaining downloads...
:finalwait
set /a active=0
for /f %%P in ('tasklist /fi "imagename eq yt-dlp.exe" /fo csv /nh 2^>nul ^| find /c "yt-dlp"') do set /a active=%%P
if !active! gtr 0 (
    timeout /t 5 /nobreak >nul
    goto finalwait
)

echo.
echo ===============================================
echo  All downloads complete!
echo ===============================================
pause
