"""Test de no-regresión INT pipeline (extractor v3 + analyst).

Pick INT funds que ya tienen output.json y los snapshot. Detecta
regresiones futuras al tocar analyst_agent.py / orchestrator.py /
cnmv_agent.py compartidos con ES.

Uso:
    python tests/test_int_no_regression.py          # validación
    python tests/test_int_no_regression.py --update # refrescar baseline
"""
import contextlib
import io
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from agents.dashboard_quality_agent import DashboardQualityAgent

# INT funds con output.json (al menos uno por jurisdicción)
# Fase E (2026-04-27): añadido GAM (segundo IE). DE/FR pendientes hasta procesar.
ISINS = ["IE00B6T42S66", "IE00BF5GGB04", "LU1694789378"]
BASELINE_PATH = ROOT / "tests" / "baseline_int_v1.json"

# Tolerancia structural reducida (Fase E): 15% → 10% para detectar regresiones reales.
STRUCT_TOLERANCE_PCT = 10

# AUM bounds — descarta valores inverosímiles (Fase E: bug DNCA daba €41B = SICAV completo)
AUM_MIN_MEUR = 0.1     # < €100K probablemente bug extracción
AUM_MAX_MEUR = 50000   # > €50B suele ser umbrella SICAV sumado, no sub-fondo


def _snapshot_fund(isin: str) -> dict:
    out_path = ROOT / "data" / "funds" / isin / "output.json"
    if not out_path.exists():
        return {"isin": isin, "error": "output.json missing"}
    d = json.loads(out_path.read_text(encoding="utf-8"))
    # Lectura via accessor canónico (Fase C)
    from tools.output_accessor import (
        get_nombre, get_tipo, get_serie_aum, get_posiciones_actuales,
        get_perfiles, get_resumen_texto, get_estrategia_texto, get_kpi_aum,
    )
    aum = get_kpi_aum(d)
    return {
        "nombre": get_nombre(d),
        "tipo": get_tipo(d),
        "kpi_aum_meur": aum,
        "struct": {
            "n_aum_puntos": len(get_serie_aum(d)),
            "n_posiciones_actuales": len(get_posiciones_actuales(d)),
            "n_perfiles": len(get_perfiles(d)),
            "resumen_chars": len(get_resumen_texto(d)),
            "estrategia_chars": len(get_estrategia_texto(d)),
            "has_aum": bool(aum),
            # Fase E: validación rangos AUM razonables
            "aum_in_range": (aum is not None and AUM_MIN_MEUR <= aum <= AUM_MAX_MEUR),
        },
    }


def _compare(baseline: dict, current: dict, isin: str) -> list:
    issues = []
    if "error" in current:
        issues.append(f"output missing: {current['error']}")
        return issues
    if not baseline:
        return issues  # no baseline = no regression check
    bl_struct = baseline.get("struct", {})
    cur_struct = current.get("struct", {})
    for key, bl_v in bl_struct.items():
        cur_v = cur_struct.get(key, 0)
        if isinstance(bl_v, bool):
            if cur_v != bl_v:
                issues.append(f"{key}: {bl_v} -> {cur_v}")
        elif isinstance(bl_v, (int, float)) and bl_v > 0:
            drop_pct = (bl_v - cur_v) / bl_v * 100
            if drop_pct > STRUCT_TOLERANCE_PCT:
                issues.append(f"{key}: {bl_v} -> {cur_v} ({drop_pct:.0f}% pérdida)")
    return issues


def main():
    update = "--update" in sys.argv

    print("=" * 60)
    print("Test de no-regresión INT pipeline")
    print("=" * 60)

    baseline = {}
    if BASELINE_PATH.exists():
        baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))

    total_issues = []
    new_snapshots = {}

    for isin in ISINS:
        cur = _snapshot_fund(isin)
        new_snapshots[isin] = cur
        bl = baseline.get(isin, {})

        print(f"\n{cur.get('nombre', isin) or isin}")
        if "error" in cur:
            print(f"  [SKIP] {cur['error']}")
            continue
        print(f"  perfiles={cur['struct']['n_perfiles']}, "
              f"posiciones={cur['struct']['n_posiciones_actuales']}, "
              f"resumen={cur['struct']['resumen_chars']} chars")
        if not bl:
            print("  [NEW] sin baseline previa")
            continue

        issues = _compare(bl, cur, isin)
        if issues:
            print(f"  FAIL ({len(issues)} regresiones):")
            for i in issues:
                print(f"    ✗ {i}")
            total_issues.extend((isin, i) for i in issues)
        else:
            print("  OK")

    print("\n" + "=" * 60)
    if update:
        BASELINE_PATH.write_text(
            json.dumps(new_snapshots, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"[!] Baseline INT actualizado: {BASELINE_PATH}")
        return

    if total_issues:
        print(f"FAIL: {len(total_issues)} regresiones")
        sys.exit(1)
    print("PASS: pipeline INT sin regresiones")


if __name__ == "__main__":
    main()
