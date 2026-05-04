#!/bin/bash
# Lanza Trojan + DNCA en paralelo: extractor → analyst → dashboard cada uno.
# Ambos pipelines son independientes, así que paralelizan bien.
# Notifica con chime al terminar ambos.

cd "c:/Users/RafaelGarcía/OneDrive - Nazca/Escritorio/fund-analyzer"

run_pipeline() {
  local ISIN=$1
  local NOMBRE=$2
  local GESTORA=$3
  local TAG=$4

  echo "[$TAG] === Pipeline arrancando ==="
  PYTHONIOENCODING=utf-8 python -m agents.intl_extractor_v2 \
    --isin "$ISIN" --nombre "$NOMBRE" --gestora "$GESTORA" \
    > "data/funds/$ISIN/extractor_run.log" 2>&1
  echo "[$TAG] Extractor exit=$?"

  if [ ! -s "data/funds/$ISIN/intl_data.json" ]; then
    echo "[$TAG] ERROR: intl_data.json no se generó"
    echo "FAILED" > "data/funds/$ISIN/.pipeline_status"
    return 1
  fi

  PYTHONIOENCODING=utf-8 python -m agents.analyst_agent "$ISIN" \
    > "data/funds/$ISIN/analyst_run.log" 2>&1
  echo "[$TAG] Analyst exit=$?"

  python dashboard/generate_dashboard.py "$ISIN" > "data/funds/$ISIN/dashgen.log" 2>&1
  echo "[$TAG] Dashboard exit=$?"

  echo "DONE" > "data/funds/$ISIN/.pipeline_status"
  echo "[$TAG] === Pipeline COMPLETO ==="
}

# Lanzar ambos en paralelo
run_pipeline "IE00B6T42S66" "Trojan Fund (Ireland) O EUR ACC" "Troy Asset Management" "TROJAN" &
PID_TROJAN=$!
run_pipeline "LU1694789451" "DNCA Invest Alpha Bonds" "DNCA Investments" "DNCA" &
PID_DNCA=$!

wait $PID_TROJAN $PID_DNCA

# Quality check ambos
PYTHONIOENCODING=utf-8 python -m agents.dashboard_quality_agent IE00B6T42S66 > "data/funds/IE00B6T42S66/quality_run.log" 2>&1
PYTHONIOENCODING=utf-8 python -m agents.dashboard_quality_agent LU1694789451 > "data/funds/LU1694789451/quality_run.log" 2>&1

echo ""
echo "=== AMBOS PIPELINES COMPLETADOS ==="
for ISIN in IE00B6T42S66 LU1694789451; do
  STATUS=$(cat "data/funds/$ISIN/.pipeline_status" 2>/dev/null || echo "?")
  POS=$(python -c "
import json
try:
    d = json.loads(open('data/funds/$ISIN/output.json', encoding='utf-8').read())
    print(len(d.get('posiciones',{}).get('actuales',[])))
except: print('?')")
  FALLOS=$(grep -c "^  •" "data/funds/$ISIN/quality_run.log" 2>/dev/null || echo "?")
  echo "  $ISIN: status=$STATUS pos=$POS fallos_quality=$FALLOS"
done

# Chime sonoro: 3 notas melódicas
powershell -Command "[console]::beep(523,180); [console]::beep(659,180); [console]::beep(784,300)"

touch "data/.pipelines_done"
