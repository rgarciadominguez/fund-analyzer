"""Calcula SHA256 canonicalizado de los output.json para detectar
modificaciones byte-by-byte tras refactor de lectura (Fase C).

Uso:
    python -m tools.fingerprint_outputs           # genera fingerprints
    python -m tools.fingerprint_outputs --verify  # compara contra fingerprints guardados
"""
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
FINGERPRINT_PATH = ROOT / "tests" / "fingerprints_pre_consolidacion.json"

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def canonical_sha256(json_obj: dict) -> str:
    """SHA256 de JSON canonicalizado (claves ordenadas, sin whitespace)."""
    canonical = json.dumps(json_obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_all() -> dict:
    """Calcula fingerprints de todos los output.json en data/funds/*/."""
    fingerprints = {}
    funds_dir = ROOT / "data" / "funds"
    for fd in sorted(funds_dir.iterdir()):
        if not fd.is_dir():
            continue
        out = fd / "output.json"
        if not out.exists():
            continue
        try:
            d = json.loads(out.read_text(encoding="utf-8"))
            fingerprints[fd.name] = {
                "sha256": canonical_sha256(d),
                "size_bytes": out.stat().st_size,
            }
        except Exception as exc:
            fingerprints[fd.name] = {"error": str(exc)}
    return fingerprints


def save():
    fps = compute_all()
    FINGERPRINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    FINGERPRINT_PATH.write_text(
        json.dumps(fps, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"[OK] Guardado: {FINGERPRINT_PATH}")
    print(f"[OK] {len(fps)} fondos fingerprinted")


def verify():
    if not FINGERPRINT_PATH.exists():
        print(f"[ERROR] No existe {FINGERPRINT_PATH}. Ejecuta sin --verify primero.")
        sys.exit(1)
    expected = json.loads(FINGERPRINT_PATH.read_text(encoding="utf-8"))
    actual = compute_all()

    drifts = []
    for isin, exp in expected.items():
        act = actual.get(isin)
        if act is None:
            drifts.append((isin, "MISSING en actual"))
            continue
        if "error" in exp or "error" in act:
            continue
        if exp["sha256"] != act["sha256"]:
            drifts.append((isin, f"SHA256 cambió: {exp['sha256'][:12]} → {act['sha256'][:12]}"))
    new_funds = set(actual.keys()) - set(expected.keys())
    for isin in new_funds:
        drifts.append((isin, "NUEVO fondo (no estaba en baseline)"))

    if drifts:
        print(f"[FAIL] {len(drifts)} drifts detectados:")
        for isin, msg in drifts:
            print(f"  - {isin}: {msg}")
        sys.exit(1)
    print(f"[OK] {len(actual)} fondos: SHA256 byte-idéntico al baseline ✓")


if __name__ == "__main__":
    if "--verify" in sys.argv:
        verify()
    else:
        save()
