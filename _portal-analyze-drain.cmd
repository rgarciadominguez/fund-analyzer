@echo off
chcp 65001 >nul
REM ====================================================================
REM _portal-analyze-drain.cmd — lo lanza el manejador de protocolo
REM   horizonte-fa://  cuando Rafa pulsa "Enviar a analizar" en el portal.
REM Procesa TODA la cola de analisis y se apaga (modo --drain, lock unico).
REM Si ya hay un drenado en marcha, el worker sale solo (lock) y el que
REM corre coge el fondo recien encolado.
REM ====================================================================
cd /d "%~dp0"
echo [%date% %time%] disparo horizonte-fa:// -> drenar cola >> "_drain-log.txt"
python -m tools.portal_analyze_worker --drain --wake >> "_drain-log.txt" 2>&1
echo [%date% %time%] drenado terminado >> "_drain-log.txt"
