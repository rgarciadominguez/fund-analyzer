"""Copia gestores entre fondos hermanos de la misma gestora.
Cartesio X -> Y, Dunas Flexible -> Equilibrado/Prudente."""
import json
import shutil
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent

# Mapping: source_isin -> [target_isins]
SIBLINGS = {
    "ES0116567035": ["ES0182527038"],  # Cartesio X -> Y
    "ES0175316001": ["ES0175414012", "ES0175437039"],  # Dunas Flexible -> Equilibrado, Prudente
}


def copy_gestores(src_isin, target_isin):
    src_p = ROOT / "data" / "funds" / src_isin / "output.json"
    tgt_p = ROOT / "data" / "funds" / target_isin / "output.json"
    if not src_p.exists() or not tgt_p.exists():
        print(f"  {target_isin}: skip (sin archivos)")
        return

    src_d = json.loads(src_p.read_text(encoding="utf-8"))
    tgt_d = json.loads(tgt_p.read_text(encoding="utf-8"))

    src_perfiles = ((src_d.get("analyst_synthesis") or {}).get("gestores") or {}).get("perfiles") or []
    if not src_perfiles:
        print(f"  {target_isin}: source {src_isin} sin perfiles")
        return

    tgt_asy = tgt_d.setdefault("analyst_synthesis", {})
    tgt_g = tgt_asy.setdefault("gestores", {})
    n_old = len(tgt_g.get("perfiles") or [])

    # Solo sobreescribir si el target no tiene perfiles
    if n_old > 0:
        print(f"  {target_isin}: ya tiene {n_old} perfiles, skip")
        return

    bak = ROOT / "data" / "funds" / target_isin / "output.pre_copy_siblings.json"
    if not bak.exists():
        shutil.copy(tgt_p, bak)

    tgt_g["perfiles"] = src_perfiles
    # Preservar el texto narrativo si existe
    if not tgt_g.get("texto") and (src_d.get("analyst_synthesis") or {}).get("gestores", {}).get("texto"):
        tgt_g["texto"] = src_d["analyst_synthesis"]["gestores"]["texto"]

    with open(tgt_p, "w", encoding="utf-8") as f:
        json.dump(tgt_d, f, indent=2, ensure_ascii=False)

    nombres = [p.get("nombre","?") for p in src_perfiles]
    print(f"  {target_isin}: {n_old} -> {len(src_perfiles)} perfiles desde {src_isin}")
    print(f"    Nombres: {', '.join(nombres)}")


if __name__ == "__main__":
    for src, targets in SIBLINGS.items():
        print(f"\n=== {src} -> {targets} ===")
        for tgt in targets:
            copy_gestores(src, tgt)
    print("\nDONE")
