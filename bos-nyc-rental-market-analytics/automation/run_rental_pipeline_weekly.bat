@echo off

REM Change to the folder that contains rental_pipeline.py
cd /d "C:\path\to\Analytics-Portfolio\rental-market-analytics\python"

REM Run the weekly rental pipeline
python rental_pipeline.py

REM Optional: log output instead of writing to console
REM python rental_pipeline.py >> "..\logs\weekly_pipeline.log" 2>&1
