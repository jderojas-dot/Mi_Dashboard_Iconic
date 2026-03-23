@echo off
:: ══════════════════════════════════════════════════════════════
::   ICONIC TERROIRS — Script de inicio automático
:: ══════════════════════════════════════════════════════════════

:: 1) Sync Excel Sheets (en segundo plano, silencioso)
cd C:\sync_excel_sheets
start "" pythonw sync.py

:: 2) Materialización de vistas BigQuery (en segundo plano)
::    Se ejecuta en una ventana minimizada para no interrumpir.
::    Actualiza los KPIs YTD y clientes del año con la fecha de hoy.
cd C:\Dashboard_Iconic\Antigravity\BD
start "Materializando BigQuery" /MIN ^
    C:\Dashboard_Iconic\Antigravity\venv\Scripts\python.exe materializar_vistas.py