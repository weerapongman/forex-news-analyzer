@echo off
title Forex News Analyzer - Real-Time Economic Calendar
color 0A

echo.
echo ========================================
echo   FOREX NEWS ANALYZER
echo   Starting Real-Time Dashboard...
echo ========================================
echo.

REM Navigate to the script directory
cd /d "%~dp0"

REM Check if Node.js is installed
where node >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Node.js is not installed!
    echo.
    echo Please install Node.js from https://nodejs.org/
    echo.
    pause
    exit /b 1
)

REM Check if node_modules exists
if not exist "node_modules\" (
    echo [INFO] Installing dependencies...
    echo This will take 30-60 seconds...
    echo.
    call npm install
    if %ERRORLEVEL% NEQ 0 (
        echo.
        echo [ERROR] Failed to install dependencies
        echo.
        pause
        exit /b 1
    )
    echo.
    echo [SUCCESS] Dependencies installed!
    echo.
)

REM Open browser after 3 seconds
echo [INFO] Opening browser in 3 seconds...
timeout /t 3 /nobreak >nul
start http://localhost:8080

REM Start the server
echo.
echo [INFO] Starting server...
echo.
echo ========================================
echo   Server will start below
echo   Press Ctrl+C to stop
echo ========================================
echo.

node server.js

REM If server stops, wait before closing
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Server failed to start
    echo Check the error messages above
    echo.
)

pause