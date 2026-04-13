"""
Test de no-regresión del pipeline ES (v6, congelado 2026-04-13).

Compara el estado actual de los 4 fondos ES validados contra baseline_es_v6.json.
Cualquier cambio en agentes (analyst, cnmv, quality, dashboard, discovery) que
empeore los scores o pierda estructura CRÍTICA → FAIL.

Uso:
    python tests/test_es_no_regression.py          # validación
    python tests/test_es_no_regression.py --update # refrescar baseline (con precaución)

IMPORTANTE: No usar --update sin revisar qué ha cambiado y por qué.
"""
import contextlib
import io
import json
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from agents.dashboard_quality_agent import DashboardQualityAgent

ISINS = ["ES0112231008", "ES0156572002", "ES0175316001", "ES0116567035"]
BASELINE_PATH = ROOT / "tests" / "baseline_es_v6.json"

# Tolerancias: los scores pueden variar ±1 regla por aleatoriedad LLM
# Los campos estructurales críticos NO deben disminuir más de un 10%
STRUCT_TOLERANCE_PCT = 10


def _snapshot_fund(isin: str) -> dict:
    """Construye snapshot de un fondo: score + campos estructurales."""
    with contextlib.redirect_stdout(io.StringIO()):
        q = DashboardQualityAgent(isin)
        r = q.run()
    out_path = ROOT / "data" / "funds" / isin / "output.json"
    d = json.loads(out_path.read_text(encoding="utf-8"))
    cuant = d.get("cuantitativo", {}) or {}
    pos = d.get("posiciones", {}) or {}
    synth = d.get("analyst_synthesis", {}) or {}
    return {
        "nombre": d.get("nombre"),
        "score_display": r.get("score_display"),
        "fallos_estructura": r.get("fallos_estructura"),
        "fallos_scarcity": r.get("fallos_scarcity"),
        "aceptable": r.get("aceptable"),
        "total_fallos": len(r.get("fallos", [])),
        "struct": {
            "n_vl_puntos": len(cuant.get("serie_vl_base100", [])),
            "n_aum_puntos": len(cuant.get("serie_aum", [])),
            "n_participes_puntos": len(cuant.get("serie_participes", [])),
            "n_ter_puntos": len(cuant.get("serie_ter", [])),
            "n_posiciones_actuales": len(pos.get("actuales", [])),
            "n_posiciones_historicas": len(pos.get("historicas", [])),
            "n_perfiles": len((synth.get("gestores", {}) or {}).get("perfiles", [])),
            "n_hitos_historia": len((synth.get("historia", {}) or {}).get("hitos", [])),
            "resumen_chars": len((synth.get("resumen", {}) or {}).get("texto", "") or ""),
            "cartera_chars": len((synth.get("cartera", {}) or {}).get("texto", "") or ""),
            "estrategia_chars": len((synth.get("estrategia", {}) or {}).get("texto", "") or ""),
            "has_ter_efectivo": any("ter_efectivo_pct" in t for t in cuant.get("serie_ter", [])),
            "has_drawdown": bool(
                (synth.get("evolucion", {}) or {}).get("datos_graficos", {}).get("drawdown")
            ),
        },
    }


def _compare(baseline: dict, current: dict, isin: str) -> list[str]:
    """Compara snapshots. Devuelve lista de regresiones detectadas."""
    issues = []
    # 1. fallos_estructura NO debe aumentar (solo bajar o mantenerse)
    bl_est = baseline.get("fallos_estructura", 0)
    cu_est = current.get("fallos_estructura", 0)
    if cu_est > bl_est:
        issues.append(
            f"REGRESIÓN fallos_estructura: {bl_est} → {cu_est} (nuevos fallos detectados)"
        )

    # 2. Campos estructurales no deben perder más del X%
    for key, bl_val in baseline.get("struct", {}).items():
        cu_val = current.get("struct", {}).get(key, 0)
        if isinstance(bl_val, bool):
            if bl_val and not cu_val:
                issues.append(f"REGRESIÓN {key}: True → False (feature perdida)")
        elif isinstance(bl_val, (int, float)) and bl_val > 0:
            if cu_val < bl_val * (1 - STRUCT_TOLERANCE_PCT / 100):
                pct_loss = ((bl_val - cu_val) / bl_val) * 100
                issues.append(
                    f"REGRESIÓN {key}: {bl_val} → {cu_val} ({pct_loss:.0f}% pérdida)"
                )
    return issues


def main():
    update = "--update" in sys.argv

    if not BASELINE_PATH.exists():
        print(f"ERROR: no existe {BASELINE_PATH}")
        sys.exit(1)

    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))

    print("=" * 75)
    print("Test de no-regresión pipeline ES v6")
    print("=" * 75)

    total_issues = []
    new_snapshots = {}

    for isin in ISINS:
        current = _snapshot_fund(isin)
        new_snapshots[isin] = current
        bl = baseline.get(isin, {})
        if not bl:
            print(f"\n[!] {isin} sin baseline — SKIP")
            continue

        print(f"\n{bl.get('nombre', isin)}")
        print(f"  Score baseline: {bl.get('score_display')}")
        print(f"  Score actual:   {current.get('score_display')}")

        issues = _compare(bl, current, isin)
        if issues:
            print(f"  FAIL ({len(issues)} regresiones):")
            for i in issues:
                print(f"    ✗ {i}")
            total_issues.extend((isin, i) for i in issues)
        else:
            print("  OK — sin regresiones")

    print("\n" + "=" * 75)
    if total_issues:
        print(f"FAIL: {len(total_issues)} regresiones totales")
        sys.exit(1)

    print("PASS: pipeline ES v6 intacto")

    if update:
        BASELINE_PATH.write_text(
            json.dumps(new_snapshots, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\n[!] Baseline actualizado: {BASELINE_PATH}")


if __name__ == "__main__":
    main()
