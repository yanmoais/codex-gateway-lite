@echo off
setlocal
chcp 65001 >nul
title Codex Gateway Lite Bootstrap
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0Codex Gateway Lite.ps1"
if errorlevel 1 (
  echo.
  echo Codex Gateway Lite bootstrap failed.
  pause
)
