@echo off
chcp 65001 >nul
cd /d "%~dp0"

if not exist ".env" (
  echo [Error] .env not found. Please copy .env.example and fill in your config:
  echo   copy .env.example .env
  pause
  exit /b 1
)

if not exist "node_modules\" (
  echo Installing dependencies...
  npm install
  if errorlevel 1 (
    echo [Error] npm install failed.
    pause
    exit /b 1
  )
)

set "APP_MODE=full"
for /f "tokens=1,* delims==" %%A in (.env) do (
  if /I "%%A"=="APP_MODE" set "APP_MODE=%%B"
)

echo Starting BTC 5m monitor...
echo Mode: %APP_MODE%
echo State API: http://localhost:3456/api/state
if /I not "%APP_MODE%"=="headless" echo Open browser: http://localhost:3456
echo Press Ctrl+C to stop.
echo.

if /I not "%APP_MODE%"=="headless" (
  start "" "http://localhost:3456"
  timeout /t 2 >nul
)

npx --yes tsx server.ts
