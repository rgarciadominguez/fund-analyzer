#!/bin/bash
# Batch ES funds: sequential, 7h hard deadline
# Start time recorded; before each fund, check elapsed > 7h => exit

set -u
cd "$(dirname "$0")"

START=$(date +%s)
DEADLINE=$((START + 7*3600))
LOG_DIR="logs"
mkdir -p "$LOG_DIR"

ISINS=(
  ES0140794001
  ES0175414012
  ES0182527038
  ES0128520006
  ES0173311103
  ES0175902008
)

echo "[BATCH] Start $(date -Iseconds)" | tee -a "$LOG_DIR/batch.log"
echo "[BATCH] Deadline $(date -Iseconds -d @$DEADLINE)" | tee -a "$LOG_DIR/batch.log"

for ISIN in "${ISINS[@]}"; do
  NOW=$(date +%s)
  REMAIN=$((DEADLINE - NOW))
  if [ $REMAIN -le 0 ]; then
    echo "[BATCH] $(date -Iseconds) DEADLINE reached, stopping before $ISIN" | tee -a "$LOG_DIR/batch.log"
    exit 0
  fi
  echo "[BATCH] $(date -Iseconds) START $ISIN (remaining: ${REMAIN}s)" | tee -a "$LOG_DIR/batch.log"
  python -m agents.orchestrator --isin "$ISIN" --auto > "$LOG_DIR/$ISIN.log" 2>&1
  RC=$?
  echo "[BATCH] $(date -Iseconds) END   $ISIN (exit $RC)" | tee -a "$LOG_DIR/batch.log"
done

echo "[BATCH] $(date -Iseconds) ALL DONE" | tee -a "$LOG_DIR/batch.log"
