"""Limpia perfiles de gestores claramente inventados por el LLM.
Detecta:
- Nombres con prefijo 'Dr. ' que aparecen en multiples fondos (LLM filler)
- Cargos genericos como 'CIO & Fundador, Alpha Capital' (Alpha Capital = template)
- Nombres comunes que aparecen sin fuente
"""
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

# Nombres conocidos como inventados por el LLM
INVENTED_NAMES = {
    "Dr. Alejandro Vargas",
    "Alejandro Vargas",
    "Ana García",
    "Alejandro García",
    "Pedro García",  # template-y
    "María García",  # template-y
}

# Pattern detectores: si el cargo contiene esto = invento
INVENTED_CARGO_PATTERNS = [
    "Alpha Capital",  # not a real spanish gestora in our universe
    "Dr.",  # Dr. as cargo prefix is sospechoso para gestores ES
]

# Allow-list por gestora (nombres reales conocidos de cofundadores)
ALLOWED_BY_GESTORA = {
    "CARTESIO": ["Juan Antonio Bertrán", "Cayetano Cornet", "Álvaro Martínez Aroca",
                 "Álvaro Martínez", "Carlos Cervera", "Ramón Astiz"],
    "DUNAS": ["Alfonso Benito", "José María Lecube", "Carlos Gutiérrez",
              "Borja Fernández-Galiano"],
    "AVANTAGE": ["Juan Gómez Bada"],
    "MYINVESTOR": ["Carlos Santiso", "Rafael Ortega", "Rafael Ortega Salvador"],
    "RENTA 4": ["Ignacio Victoriano Soto"],
}


def is_invented(perfil: dict, gestora_uppercase: str) -> tuple[bool, str]:
    nombre = perfil.get("nombre", "").strip()
    cargo = perfil.get("cargo", "")

    if not nombre:
        return True, "nombre vacío"

    if nombre in INVENTED_NAMES:
        return True, f"nombre en blacklist: {nombre}"

    for pat in INVENTED_CARGO_PATTERNS:
        if pat in cargo:
            return True, f"cargo contiene '{pat}'"

    # Allow si está en la allow-list de la gestora
    for key, allowed in ALLOWED_BY_GESTORA.items():
        if key in gestora_uppercase:
            for a in allowed:
                if a in nombre or nombre in a:
                    return False, "OK (allow-list gestora)"

    # Si tiene "Dr. " al principio del nombre + no en allow-list = sospechoso
    if nombre.startswith("Dr. "):
        return True, "prefijo 'Dr.' sospechoso ES"

    return False, "no detectado como invento"


def clean(isin):
    fund = ROOT / "data" / "funds" / isin
    out_p = fund / "output.json"
    if not out_p.exists():
        print(f"  {isin}: sin output.json")
        return

    with open(out_p, encoding="utf-8") as f:
        d = json.load(f)

    asy = d.get("analyst_synthesis") or {}
    gestores = asy.get("gestores") or {}
    perfiles = gestores.get("perfiles") or []
    if not perfiles:
        print(f"  {isin}: sin perfiles")
        return

    gestora = (d.get("gestora") or "").upper()

    cleaned = []
    removed = []
    for p in perfiles:
        bad, reason = is_invented(p, gestora)
        if bad:
            removed.append(f"{p.get('nombre','?')} ({reason})")
        else:
            cleaned.append(p)

    if not removed:
        print(f"  {isin}: {len(perfiles)} perfiles, todos OK")
        return

    # Backup
    bak = fund / "output.pre_clean_invented.json"
    if not bak.exists():
        shutil.copy(out_p, bak)

    gestores["perfiles"] = cleaned
    asy["gestores"] = gestores
    d["analyst_synthesis"] = asy

    # Marcar perfiles como manualmente curados — analyst no debe re-inventar
    try:
        sys.path.insert(0, str(ROOT))
        from tools.output_merger import mark_manual_edit
        mark_manual_edit(d, "analyst_synthesis.gestores.perfiles")
    except Exception:
        pass

    with open(out_p, "w", encoding="utf-8") as f:
        json.dump(d, f, indent=2, ensure_ascii=False)

    print(f"  {isin}: {len(perfiles)} -> {len(cleaned)} perfiles")
    for r in removed:
        print(f"    REMOVED: {r}")


if __name__ == "__main__":
    targets = sys.argv[1:] if len(sys.argv) > 1 else ISINS
    for isin in targets:
        clean(isin.strip().upper())
    print("DONE")
