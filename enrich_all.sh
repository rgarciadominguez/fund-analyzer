#!/bin/bash
# Aplicar cnmv_enrichment a los 11 fondos ES
set -u
cd "$(dirname "$0")"

ISINS=(
  ES0112231008 ES0116567035 ES0128520006
  ES0140794001 ES0156572002 ES0173311103
  ES0175316001 ES0175414012 ES0175437039
  ES0175902008 ES0182527038
)

for ISIN in "${ISINS[@]}"; do
  echo "=== $ISIN ==="
  PYTHONIOENCODING=utf-8 PYTHONUTF8=1 python -m agents.cnmv_enrichment "$ISIN" 2>&1 | tail -8
  echo
done
echo "DONE"
