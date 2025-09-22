@echo off
REM Change these paths to match your system

REM Path to Python
set PYTHON="C:\Users\setiawa\AppData\Local\anaconda3\python.exe"

REM Path to your script
set SCRIPT="C:\Users\setiawa\Documents\ntl-academies-tracker\monitor\run_all.py"

REM Path to log file
set LOG="C:\Users\setiawa\Documents\ntl-academies-tracker\run_all.log"

echo === [%date% %time%] Starting run_all.py === >> %LOG%
%PYTHON% %SCRIPT% >> %LOG% 2>&1
echo === [%date% %time%] Finished run_all.py === >> %LOG%
