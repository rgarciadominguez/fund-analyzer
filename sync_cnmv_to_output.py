"""Sincroniza posiciones, serie_rentabilidad y mix_activos_historico
desde cnmv_data.json a output.json. Idempotente."""
import json
import shutil
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent
ISINS = [
    "ES0112231008", "ES0116567035", "ES0128520006",
    "ES0140794001", "ES0156572002", "ES0173311103",
    "ES0175316001", "ES0175414012", "ES0175437039",
    "ES0175902008", "ES0182527038",
]


def sync(isin):
    fund = ROOT / "data" / "funds" / isin
    cnmv = fund / "cnmv_data.json"
    out_p = fund / "output.json"
    if not cnmv.exists() or not out_p.exists():
        print(f"  {isin}: SKIP (sin archivos)")
        return

    cd = json.loads(cnmv.read_text(encoding="utf-8"))
    od = json.loads(out_p.read_text(encoding="utf-8"))

    bak = fund / "output.pre_sync.json"
    if not bak.exists():
        shutil.copy(out_p, bak)

    changes = []

    # Posiciones
    cnmv_pos = (cd.get("posiciones") or {}).get("actuales") or []
    if cnmv_pos:
        out_pos = od.setdefault("posiciones", {})
        old_n = len(out_pos.get("actuales") or [])
        old_sec = sum(1 for p in (out_pos.get("actuales") or []) if (p.get("sector") or "").strip())
        out_pos["actuales"] = cnmv_pos
        new_sec = sum(1 for p in cnmv_pos if (p.get("sector") or "").strip())
        changes.append(f"posiciones {old_n}/{old_sec}sec → {len(cnmv_pos)}/{new_sec}sec")

    # serie_rentabilidad
    cd_cuant = cd.get("cuantitativo") or {}
    od_cuant = od.setdefault("cuantitativo", {})
    if cd_cuant.get("serie_rentabilidad"):
        n_old = len(od_cuant.get("serie_rentabilidad") or [])
        n_new = len(cd_cuant["serie_rentabilidad"])
        if n_old != n_new:
            od_cuant["serie_rentabilidad"] = cd_cuant["serie_rentabilidad"]
            changes.append(f"serie_rentabilidad {n_old} → {n_new}")

    # mix_activos_historico
    if cd_cuant.get("mix_activos_historico"):
        od_cuant["mix_activos_historico"] = cd_cuant["mix_activos_historico"]
        changes.append("mix_activos sincronizado")

    if changes:
        with open(out_p, "w", encoding="utf-8") as f:
            json.dump(od, f, indent=2, ensure_ascii=False)
        print(f"  {isin}: {' | '.join(changes)}")
    else:
        print(f"  {isin}: sin cambios")


if __name__ == "__main__":
    targets = sys.argv[1:] if len(sys.argv) > 1 else ISINS
    for isin in targets:
        sync(isin.strip().upper())
    print("DONE sync")
