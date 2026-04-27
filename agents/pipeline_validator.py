"""Pipeline validator — confirma que un pipeline (extractor + analyst + dashboard)
produjo un output completo. Si no, decide qué reagentar.

Usado por el orchestrator y por scripts de re-corrida automática.

Salida:
  {
    "valid": bool,
    "reasons": list[str],   # qué falló
    "should_retry_extractor": bool,
    "should_retry_analyst": bool,
  }
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).parent.parent


# Umbrales mínimos por tipo de fondo (heurística)
_MIN_HOLDINGS_DEFAULT = 15      # cualquier fondo con cartera real
_MIN_INTL_DATA_BYTES = 60_000    # intl_data.json sano
_MIN_OUTPUT_BYTES = 100_000      # output.json con secciones cualitativas
_MIN_HISTORICAS_AGE_PCT = 0.5    # ≥50% de los años recientes deben tener posiciones


def _load(path: Path) -> dict | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def validate_pipeline(isin: str) -> dict:
    """Audita el resultado del pipeline para `isin`. Devuelve diagnóstico
    estructurado con flags de retry."""
    fund_dir = ROOT / "data" / "funds" / isin
    intl_path = fund_dir / "intl_data.json"
    out_path = fund_dir / "output.json"

    reasons: list[str] = []
    should_retry_extractor = False
    should_retry_analyst = False

    # 1) intl_data.json existe y tiene tamaño razonable
    if not intl_path.exists():
        reasons.append("intl_data.json no existe")
        should_retry_extractor = True
    elif intl_path.stat().st_size < _MIN_INTL_DATA_BYTES:
        reasons.append(
            f"intl_data.json muy pequeño "
            f"({intl_path.stat().st_size:,} bytes < {_MIN_INTL_DATA_BYTES:,}) — extractor incompleto"
        )
        should_retry_extractor = True

    intl = _load(intl_path) or {}
    pos = intl.get("posiciones", {})
    actuales = pos.get("actuales", []) or []
    historicas = pos.get("historicas", []) or []

    # 2) Posiciones actuales: mínimo razonable
    if len(actuales) < _MIN_HOLDINGS_DEFAULT:
        reasons.append(
            f"Solo {len(actuales)} posiciones actuales (mínimo {_MIN_HOLDINGS_DEFAULT}) — "
            f"extractor saltó docs o cascada falló"
        )
        should_retry_extractor = True

    # 3) Históricas: si hay historicas declaradas, ≥50% deben tener `todas`
    if historicas:
        with_data = sum(1 for h in historicas if (h.get("todas") or h.get("top10")))
        if with_data < len(historicas) * _MIN_HISTORICAS_AGE_PCT:
            reasons.append(
                f"Históricas incompletas: {with_data}/{len(historicas)} "
                f"años con posiciones declaradas (≥50% requerido)"
            )
            should_retry_extractor = True

    # 4) output.json existe y completo
    if not out_path.exists():
        reasons.append("output.json no existe")
        should_retry_analyst = True
    elif out_path.stat().st_size < _MIN_OUTPUT_BYTES:
        reasons.append(
            f"output.json muy pequeño "
            f"({out_path.stat().st_size:,} bytes < {_MIN_OUTPUT_BYTES:,}) — analyst incompleto"
        )
        should_retry_analyst = True

    out = _load(out_path) or {}
    ana = out.get("analyst_synthesis", {}) or {}

    # 5) Secciones core presentes con `texto` no vacío
    required_sections = ("resumen", "historia", "gestores", "estrategia", "cartera")
    for sec in required_sections:
        s = ana.get(sec, {}) or {}
        if isinstance(s, dict) and len((s.get("texto") or "").strip()) < 200:
            reasons.append(f"Sección '{sec}' con texto insuficiente ({len((s.get('texto') or '').strip())} chars)")
            should_retry_analyst = True

    # 6) Gestora no vacía
    gestora = (out.get("gestora") or "").strip()
    if not gestora:
        reasons.append("output.gestora vacía")
        # No requiere retry — viene del extractor o enrichment header

    # 7) AUM razonable (si está, debe ser >0)
    aum = (out.get("kpis") or {}).get("aum_actual_meur")
    if aum is None or (isinstance(aum, (int, float)) and aum <= 0):
        reasons.append(f"AUM no válido: {aum!r}")

    valid = not should_retry_extractor and not should_retry_analyst
    return {
        "valid": valid,
        "reasons": reasons,
        "should_retry_extractor": should_retry_extractor,
        "should_retry_analyst": should_retry_analyst,
        "metrics": {
            "intl_data_bytes": intl_path.stat().st_size if intl_path.exists() else 0,
            "output_bytes": out_path.stat().st_size if out_path.exists() else 0,
            "actuales": len(actuales),
            "historicas_count": len(historicas),
            "gestora": gestora,
            "aum_meur": aum,
        },
    }


if __name__ == "__main__":
    import sys
    isin = sys.argv[1] if len(sys.argv) > 1 else "IE00BF5GGB04"
    result = validate_pipeline(isin)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    sys.exit(0 if result["valid"] else 1)
