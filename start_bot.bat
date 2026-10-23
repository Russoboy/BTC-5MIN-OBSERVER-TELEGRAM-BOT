@echo off
setlocal
cd /d "%~dp0"
if not exist .venv (
  py -m venv .venv
)
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
if not exist .env (
  copy .env.example .env
  echo.
  echo Created .env. Edit it and add your TELEGRAM_BOT_TOKEN, then run start_bot.bat again.
  pause
  exit /b 1
)
python bot.py
pause
