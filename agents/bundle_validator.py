"""bundle_validator — Validate a bundle against BUNDLE_CONTRACT.md v1.0.0.

Reads `data/funds/{ISIN}/bundle/`, runs hard checks (errors → exit 1) and soft
checks (warnings → exit 0).

Hard checks:
    1. All 6 files present.
    2. Each .json parses.
    3. bundle_manifest.schema_version is supported.
    4. Each manifest hash matches the actual sha256.
    5. fund_data has isin/nombre/gestora/tipo, tipo in {ES, INT}.
    6. ISIN coherent across all 6 files.
    7. kpis.aum_actual_meur key present (None allowed).
    8. posiciones.actuales is a list.
    9. equipo_gestor is a list.

Soft checks (warnings only):
    1. cartas vacía
    2. readings vacía
    3. AUM is None
    4. equipo_gestor + equipo both empty
    5. kpis_completeness_pct < 50
    6. INT economia_fondo.viabilidad_nota missing
    7. bundle <100 KB
    8. bundle >2 MB
    9. extra files in bundle/

CLI:
    python -m agents.bundle_validator <ISIN>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
SUPPORTED_VERSIONS = {"1.0.0"}
VALIDATOR_VERSION = "1.0.0"

REQUIRED_FILES = [
    "fund_data.json",
    "manager_profile.json",
    "letters_data.json",
    "readings.json",
    "sources.json",
    "bundle_manifest.json",
]

VALID_TIPOS = {"ES", "INT"}


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def _sha256_hex(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_json_safe(path: Path) -> tuple[Any, str | None]:
    """Return (data, error). On failure data is None."""
    try:
        return json.loads(path.read_text(encoding="utf-8")), None
    except FileNotFoundError:
        return None, "missing"
    except json.JSONDecodeError as e:
        return None, f"invalid json: {e}"
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _normalize_hash(h: str | None) -> str | None:
    if not h:
        return None
    return h.replace("sha256:", "").lower()


# -----------------------------------------------------------------------------
# Validation core
# -----------------------------------------------------------------------------

def validate(isin: str) -> dict:
    """Validate the bundle of `isin`. Returns dict with valid/errors/warnings/stats."""
    bundle_dir = ROOT / "data" / "funds" / isin / "bundle"
    errors: list[str] = []
    warnings: list[str] = []

    if not bundle_dir.exists():
        return {
            "valid": False,
            "schema_version": None,
            "errors": [f"bundle directory does not exist: {bundle_dir}"],
            "warnings": [],
            "stats": {},
        }

    # --- Hard 1: all 6 files present ---
    missing = [f for f in REQUIRED_FILES if not (bundle_dir / f).exists()]
    for f in missing:
        errors.append(f"missing file: {f}")
    if missing:
        return {
            "valid": False,
            "schema_version": None,
            "errors": errors,
            "warnings": warnings,
            "stats": {},
        }

    # --- Hard 2: each .json parses ---
    parsed: dict[str, Any] = {}
    for f in REQUIRED_FILES:
        data, err = _read_json_safe(bundle_dir / f)
        if err:
            errors.append(f"{f}: {err}")
        else:
            parsed[f] = data
    if errors:
        return {
            "valid": False,
            "schema_version": None,
            "errors": errors,
            "warnings": warnings,
            "stats": {},
        }

    manifest = parsed["bundle_manifest.json"]
    fund_data = parsed["fund_data.json"]
    manager = parsed["manager_profile.json"]
    letters = parsed["letters_data.json"]
    readings = parsed["readings.json"]
    sources = parsed["sources.json"]

    # --- Hard 3: schema_version supported ---
    sv = manifest.get("schema_version")
    if sv not in SUPPORTED_VERSIONS:
        errors.append(
            f"unsupported schema_version: {sv!r}. Supported: {sorted(SUPPORTED_VERSIONS)}"
        )

    # --- Hard 4: hash verification (only for files listed in manifest.files) ---
    files_meta = manifest.get("files") or {}
    for fname, meta in files_meta.items():
        fpath = bundle_dir / fname
        if not fpath.exists():
            errors.append(f"manifest references missing file: {fname}")
            continue
        expected = _normalize_hash(meta.get("sha256"))
        actual = _sha256_hex(fpath)
        if expected and expected != actual:
            errors.append(
                f"hash mismatch for {fname}: expected {expected[:12]}..., got {actual[:12]}..."
            )

    # --- Hard 5: fund_data fields ---
    if not isinstance(fund_data, dict):
        errors.append("fund_data.json is not a JSON object")
    else:
        for k in ("isin", "nombre", "gestora", "tipo"):
            if not fund_data.get(k):
                errors.append(f"fund_data.{k} missing or empty")
        tipo = fund_data.get("tipo")
        if tipo and tipo not in VALID_TIPOS:
            errors.append(f"fund_data.tipo invalid: {tipo!r} (expected ES or INT)")

    # --- Hard 6: ISIN coherence ---
    expected_isin = fund_data.get("isin") if isinstance(fund_data, dict) else None
    for fname, data in (("manager_profile.json", manager),
                         ("letters_data.json", letters),
                         ("readings.json", readings),
                         ("sources.json", sources),
                         ("bundle_manifest.json", manifest)):
        if isinstance(data, dict):
            actual = data.get("isin")
            if expected_isin and actual and actual != expected_isin:
                errors.append(
                    f"isin mismatch in {fname}: expected {expected_isin}, got {actual}"
                )
        # arg-isin too
        if isin and expected_isin and isin != expected_isin:
            pass  # already covered by manifest mismatch
    if isin and expected_isin and isin != expected_isin:
        errors.append(f"argument isin {isin!r} does not match fund_data.isin {expected_isin!r}")

    # --- Hard 7-9: presence of structural keys ---
    if isinstance(fund_data, dict):
        kpis = fund_data.get("kpis")
        if not isinstance(kpis, dict) or "aum_actual_meur" not in kpis:
            errors.append("fund_data.kpis.aum_actual_meur key not present (None is allowed)")
        posiciones = fund_data.get("posiciones") or {}
        if not isinstance(posiciones.get("actuales"), list):
            errors.append("fund_data.posiciones.actuales is not a list")

    if isinstance(manager, dict):
        if not isinstance(manager.get("equipo_gestor"), list):
            errors.append("manager_profile.equipo_gestor is not a list")

    # If hard checks failed, return early
    if errors:
        return {
            "valid": False,
            "schema_version": sv,
            "errors": errors,
            "warnings": warnings,
            "stats": manifest.get("stats", {}),
        }

    # ============================== Soft checks ==============================

    # 1. cartas vacía
    cartas = (letters or {}).get("cartas") or []
    if not cartas:
        warnings.append("cartas vacía — analyst.evolucion y cartera.citas serán mínimos")

    # 2. readings vacía
    rd_list = (readings or {}).get("readings") or []
    if not rd_list:
        warnings.append("readings vacía — analyst.fuentes_externas será mínimo")

    # 3. AUM None
    kpis = fund_data.get("kpis") or {}
    if kpis.get("aum_actual_meur") is None:
        warnings.append("kpis.aum_actual_meur es None — analyst lo omitirá en encabezado")

    # 4. equipo vacío
    eq_gestor = manager.get("equipo_gestor") or []
    eq_full = manager.get("equipo") or []
    if not eq_gestor and not eq_full:
        warnings.append(
            "no se identificó equipo gestor con confidence suficiente — "
            "analyst.gestores será placeholder"
        )

    # 5. kpis completeness < 50%
    completeness = (manifest.get("stats") or {}).get("kpis_completeness_pct", 0)
    try:
        if float(completeness) < 50:
            warnings.append(
                f"kpis_completeness_pct={completeness} (<50) — output narrativo limitado"
            )
    except (TypeError, ValueError):
        pass

    # 6. INT economia_fondo.viabilidad_nota
    if fund_data.get("tipo") == "INT":
        eco = fund_data.get("economia_fondo") or {}
        if not eco.get("viabilidad_nota"):
            warnings.append(
                "economia_fondo.viabilidad_nota ausente (INT) — sección estrategia limitada"
            )

    # 7-8. bundle size
    total_bytes = sum((bundle_dir / f).stat().st_size for f in REQUIRED_FILES)
    if total_bytes < 100 * 1024:
        warnings.append(
            f"bundle inusualmente pequeño ({total_bytes/1024:.1f} KB <100 KB) — posible fallo upstream"
        )
    elif total_bytes > 2 * 1024 * 1024:
        warnings.append(
            f"bundle inusualmente grande ({total_bytes/1024/1024:.2f} MB >2 MB) — "
            f"revisar articulos_completos o letters_data"
        )

    # 9. extra files in bundle/ not in contract
    extras = sorted(
        p.name for p in bundle_dir.iterdir()
        if p.is_file() and p.name not in REQUIRED_FILES
    )
    for e in extras:
        warnings.append(f"fichero extra detectado en bundle/: {e}")

    return {
        "valid": not errors,
        "schema_version": sv,
        "errors": errors,
        "warnings": warnings,
        "stats": manifest.get("stats", {}),
    }


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def _print_table(isin: str, result: dict) -> None:
    sv = result.get("schema_version")
    valid = result.get("valid")
    print()
    print(f"╔══════════════════════════════════════════════════════════════════╗")
    print(f"║ Bundle validator v{VALIDATOR_VERSION}    isin={isin}    schema={sv}".ljust(67) + "║")
    print(f"║ Verdict: {'✓ VALID' if valid else '✗ INVALID'}".ljust(67) + "║")
    print(f"╚══════════════════════════════════════════════════════════════════╝")

    stats = result.get("stats") or {}
    if stats:
        print("\nStats:")
        for k, v in stats.items():
            print(f"  {k:<28} {v}")

    errors = result.get("errors") or []
    if errors:
        print(f"\n[ERRORS] ({len(errors)}):")
        for e in errors:
            print(f"  ✗ {e}")

    warnings = result.get("warnings") or []
    if warnings:
        print(f"\n[WARNINGS] ({len(warnings)}):")
        for w in warnings:
            print(f"  ! {w}")

    if not errors and not warnings:
        print("\nNo issues.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a fund bundle (BUNDLE_CONTRACT v1.0.0)")
    parser.add_argument("isin", help="ISIN of the fund whose bundle to validate")
    parser.add_argument("--json", action="store_true",
                        help="Output JSON instead of human-readable table")
    args = parser.parse_args()

    result = validate(args.isin)

    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        _print_table(args.isin, result)

    return 0 if result["valid"] else 1


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sys.exit(main())
