@echo off
REM Quick start script for Flask Azure Storage API on Windows

echo ================================================
echo Flask Azure Storage API - Quick Start
echo ================================================

REM Check Python version
echo.
echo [1/5] Checking Python version...
python --version >nul 2>&1
if errorlevel 1 (
    echo Python not found. Please install Python 3.8+
    exit /b 1
)

REM Create virtual environment
echo.
echo [2/5] Creating virtual environment...
python -m venv venv

REM Activate virtual environment
echo.
echo [3/5] Activating virtual environment...
call venv\Scripts\activate.bat

REM Install dependencies
echo.
echo [4/5] Installing dependencies...
pip install -r requirements.txt

REM Instructions for next steps
echo.
echo [5/5] Setup complete!
echo.
echo ================================================
echo NEXT STEPS:
echo ================================================
echo.
echo 1. Create .env file with your Azure Storage connection string:
echo    copy .env.example .env
echo    REM Edit .env with your connection string
echo.
echo 2. Create required Azure Storage resources:
echo    az storage container create --name files --connection-string "YOUR_CONNECTION_STRING"
echo    az storage table create --name filemetadata --connection-string "YOUR_CONNECTION_STRING"
echo.
echo 3. Start the Flask application:
echo    python app.py
echo.
echo 4. Test the API (in a new terminal):
echo    python test_api.py
echo.
echo 5. View the README for detailed API documentation:
echo    type README.md
echo.
echo ================================================
