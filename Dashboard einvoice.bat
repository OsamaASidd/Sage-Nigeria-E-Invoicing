@echo off
title Nigeria E-Invoicing Dashboard
color 0A

echo.
echo  ========================================
echo   Nigeria E-Invoicing Dashboard
echo   Proton Security Services Ltd
echo  ========================================
echo.

:: Change to the folder where this script lives
cd /d "%~dp0"

:: Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found in PATH.
    echo  Please install Python and try again.
    pause
    exit /b 1
)

:: Install dependencies if needed
echo  Checking dependencies...
pip install flask pyodbc requests reportlab qrcode Pillow --quiet

echo.
echo  Starting server at http://localhost:5000
echo  Press Ctrl+C to stop.
echo.

:: Open browser after a short delay (runs in background)
start "" cmd /c "timeout /t 2 >nul && start http://localhost:5000"

:: Start Flask
python app.py

pause