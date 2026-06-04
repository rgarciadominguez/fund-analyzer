@echo off
REM Keep-alive + health check diario de Supabase (anti auto-pause free tier).
REM Lo invoca la tarea programada "FundAnalyzer-SupabaseKeepalive".
chcp 65001 >nul
cd /d "c:\Users\RafaelGarcía\OneDrive - Nazca\Escritorio\fund-analyzer"
if not exist logs mkdir logs
python -m tools.supabase_maintenance >> "logs\supabase_maintenance.log" 2>&1
