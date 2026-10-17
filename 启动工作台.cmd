@echo off
setlocal
chcp 65001 >nul
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  echo 请先运行 安装依赖.cmd
  pause
  exit /b 1
)
".venv\Scripts\python.exe" -u run.py
echo.
pause
