@echo off
chcp 65001 > nul
title Fund Analyzer - Subiendo a GitHub...

set ISIN=%1
set TOKEN_FILE=.github_token

REM ── Leer token de GitHub ─────────────────────────────────────
if not exist "%TOKEN_FILE%" (
    echo.
    echo Para subir a GitHub necesitas tu Personal Access Token.
    echo Crealo en: github.com/settings/tokens
    echo Permisos necesarios: repo
    echo.
    set /p GH_TOKEN="Pega tu GitHub Token (ghp_...): "
    echo %GH_TOKEN%> %TOKEN_FILE%
    echo [OK] Token guardado para proximas veces.
) else (
    set /p GH_TOKEN=<%TOKEN_FILE%
)

REM Limpiar espacios del token
set GH_TOKEN=%GH_TOKEN: =%

echo.
echo [GitHub] Subiendo datos de %ISIN% a GitHub...

REM ── Usar Python para subir via GitHub API ────────────────────
python -c "
import json, base64, sys
import urllib.request, urllib.error

ISIN = '%ISIN%'
TOKEN = '%GH_TOKEN%'
REPO = 'rgarciadominguez/fund-analyzer'
FILE_PATH = f'data/{ISIN}.json'

# Leer el JSON del fondo
try:
    with open(FILE_PATH, 'rb') as f:
        content = f.read()
    content_b64 = base64.b64encode(content).decode()
except FileNotFoundError:
    print(f'[ERROR] No se encuentra {FILE_PATH}')
    sys.exit(1)

headers = {
    'Authorization': f'token {TOKEN}',
    'Content-Type': 'application/json',
    'User-Agent': 'FundAnalyzer/1.0'
}

# Verificar si el archivo ya existe (para obtener SHA)
api_url = f'https://api.github.com/repos/{REPO}/contents/{FILE_PATH}'

try:
    req = urllib.request.Request(api_url, headers=headers)
    with urllib.request.urlopen(req) as resp:
        existing = json.loads(resp.read())
    sha = existing.get('sha')
    action = 'Actualizando'
except urllib.error.HTTPError as e:
    if e.code == 404:
        sha = None
        action = 'Subiendo nuevo'
    else:
        print(f'[ERROR] GitHub API: {e.code}')
        sys.exit(1)

# Subir archivo
fondo_nombre = json.loads(content).get('meta', {}).get('nombre', ISIN)
payload = {
    'message': f'data: {action} {ISIN} ({fondo_nombre})',
    'content': content_b64,
}
if sha:
    payload['sha'] = sha

data = json.dumps(payload).encode()
req = urllib.request.Request(api_url, data=data, headers=headers, method='PUT')
try:
    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read())
    print(f'[OK] {action} correctamente.')
    print(f'     URL: https://github.com/{REPO}/blob/main/{FILE_PATH}')
except urllib.error.HTTPError as e:
    print(f'[ERROR] No se pudo subir: {e.code} - {e.read().decode()}')
    sys.exit(1)
"

if errorlevel 1 (
    echo.
    echo [ERROR] No se pudo subir a GitHub. Revisa el token.
    del %TOKEN_FILE% > nul 2>&1
) else (
    echo.
    echo [OK] Disponible en: https://github.com/rgarciadominguez/fund-analyzer
)

echo.
pause
