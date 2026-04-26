#!/bin/bash
# Batch quality-loop sobre los 11 ES funds
# Sequencial, con deadline 5h, log por fondo
set -u
cd "$(dirname "$0")"

START=$(date +%s)
DEADLINE=$((START + 5*3600))
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

ISINS=(
  ES0112231008 ES0116567035 ES0128520006
  ES0140794001 ES0156572002 ES0173311103
  ES0175316001 ES0175414012 ES0175437039
  ES0175902008 ES0182527038
)

echo "[QBATCH] Start $(date -Iseconds)" | tee -a "$LOG_DIR/qbatch.log"
echo "[QBATCH] Deadline $(date -Iseconds -d @$DEADLINE)" | tee -a "$LOG_DIR/qbatch.log"

for ISIN in "${ISINS[@]}"; do
  NOW=$(date +%s)
  REMAIN=$((DEADLINE - NOW))
  if [ $REMAIN -le 0 ]; then
    echo "[QBATCH] $(date -Iseconds) DEADLINE reached, stopping before $ISIN" | tee -a "$LOG_DIR/qbatch.log"
    exit 0
  fi
  echo "[QBATCH] $(date -Iseconds) START $ISIN (remaining: ${REMAIN}s)" | tee -a "$LOG_DIR/qbatch.log"
  PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python run_quality_only.py "$ISIN" > "$LOG_DIR/qonly_$ISIN.log" 2>&1
  RC=$?
  echo "[QBATCH] $(date -Iseconds) END   $ISIN (exit $RC)" | tee -a "$LOG_DIR/qbatch.log"
done

echo "[QBATCH] $(date -Iseconds) ALL DONE" | tee -a "$LOG_DIR/qbatch.log"
