"""Apply nombre patch (read latest CNMV PDF, fix output.json + cnmv_data.json)
to all ES funds. Cero coste — sin LLM."""
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from run_quality_only import patch_nombre_from_pdf, log

ISINS = [
    "ES0112231008", "ES0116567035", "ES0128520006",
    "ES0140794001", "ES0156572002", "ES0173311103",
    "ES0175316001", "ES0175414012", "ES0175437039",
    "ES0175902008", "ES0182527038",
]

if __name__ == "__main__":
    targets = sys.argv[1:] if len(sys.argv) > 1 else ISINS
    for isin in targets:
        try:
            patch_nombre_from_pdf(isin.strip().upper())
        except Exception as exc:
            log(isin, "ERROR", f"{exc}")
            import traceback
            traceback.print_exc()
        print()
    print("DONE patches")
