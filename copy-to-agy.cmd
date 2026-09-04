@echo off
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\copy-to-agy.ps1" %*
