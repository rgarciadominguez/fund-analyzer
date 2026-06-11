"""validate_int_standard.py — Valida que un fondo INT cumple el ESTÁNDAR
(documentos + formato de output) tras re-correr el pipeline (2026-06-11).

Estándar de DOCUMENTOS:
  - ≥1 Annual/Semi-annual report COMPLETO (parseable, %%EOF) en raw/discovery.
Estándar de FORMATO (output.json + dashboard):
  - analyst_synthesis con las 8 secciones y quality OK.
  - historia.resumen_general y estrategia.resumen_general extensos (>1500 chars)
    → alimentan las sub-pestañas "Resumen general".
  - posiciones con sector canónico (cobertura alta) y geografía normalizada
    (sin 'United States of America' crudo).
  - cartera con lenguaje de construcción/convicción.

CLI: python -m tools.validate_int_standard <ISIN>   (exit 0 = PASS)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"


def validate(isin: str) -> dict:
    from tools.verify_fund_docs import is_complete_pdf
    from tools.analysis_quality import assess_analysis_quality
    from tools.region_normalizer import canonical_country
    issues, ok_pts = [], []
    fd = FUNDS / isin
    op = fd / "output.json"
    if not op.exists():
        return {"isin": isin, "pass": False, "issues": ["sin output.json"]}
    j = json.loads(op.read_text(encoding="utf-8"))
    a = j.get("analyst_synthesis", {}) or {}

    # ── DOCS ──
    rd = fd / "raw" / "discovery"
    import re as _re
    ars = [p for p in rd.glob("*.pdf")] if rd.is_dir() else []
    ar_ok = [p for p in ars if _re.search(r"annual|semi|jahres|rapport|informe|comptes|financial", p.name, _re.I)
             and is_complete_pdf(p)]
    if ar_ok:
        ok_pts.append(f"{len(ar_ok)} AR/SAR completos")
    else:
        issues.append("DOCS: sin AR/SAR completo en discovery")

    # ── FORMATO ──
    q = assess_analysis_quality(j)
    if q.get("ok"):
        ok_pts.append("quality OK")
    else:
        issues.append(f"FORMATO: quality NOK {q.get('blockers')}")

    for sec in ("resumen", "historia", "gestores", "evolucion", "estrategia", "cartera"):
        if not (a.get(sec, {}) or {}).get("texto"):
            issues.append(f"FORMATO: sección '{sec}' sin texto")

    for sec in ("historia", "estrategia"):
        rg = (a.get(sec, {}) or {}).get("resumen_general", "") or ""
        if len(rg) >= 1500:
            ok_pts.append(f"{sec}.resumen_general {len(rg)}c")
        else:
            issues.append(f"FORMATO: {sec}.resumen_general corto/ausente ({len(rg)}c)")

    pos = (j.get("posiciones", {}) or {}).get("actuales", []) or []
    if pos:
        # Los sectores de RV no aplican a fondos de RENTA FIJA / bonos (cat bonds,
        # treasuries...). Detectar dominancia RF y marcar sector N/A (no fallo).
        def _is_bond(p):
            t = str(p.get("tipo", "")).upper()
            if any(k in t for k in ("RF", "FIJA", "BOND", "FUTURO", "DERIV")):
                return True
            return bool(_re.search(r"treasury|bill|\bbond|\bnote|coupon|\d{4}-\d{2}-\d{2}|\d(?:[.,]\d+)?%",
                                   str(p.get("nombre", "")), _re.I))
        from tools.sector_classifier import relevant_positions
        rel = relevant_positions(pos)  # top-50 por peso + peso alto; la cola se deja en blanco
        rf_share = sum(1 for p in pos if _is_bond(p)) / len(pos)
        with_sec = sum(1 for p in rel if p.get("sector"))
        cov = round(100 * with_sec / len(rel)) if rel else 0
        if rf_share > 0.5:
            ok_pts.append(f"sector N/A (renta fija, {round(rf_share*100)}% bonos)")
        elif cov >= 70:
            extra = f" (top-{len(rel)} relevantes de {len(pos)})" if len(pos) > len(rel) else ""
            ok_pts.append(f"sector cobertura {cov}%{extra}")
        else:
            issues.append(f"FORMATO: sector cobertura baja {cov}% ({with_sec}/{len(rel)} relevantes)")

    geo = j.get("geographic_allocation", []) or []
    raw_us = [g for g in geo if g.get("region") in ("United States", "United States of America", "USA")]
    if raw_us:
        issues.append("FORMATO: geografía sin normalizar (United States crudo)")
    elif geo:
        ok_pts.append("geografía normalizada")

    cart = (a.get("cartera", {}) or {}).get("texto", "") or ""
    if any(k in cart.lower() for k in ("construye", "construcción", "convicc", "años en cartera", "núcleo")):
        ok_pts.append("cartera con construcción/convicción")
    else:
        issues.append("FORMATO: cartera sin lenguaje construcción/convicción")

    return {"isin": isin, "pass": not issues, "issues": issues, "ok": ok_pts}


def main(argv=None) -> int:
    isin = (argv or sys.argv[1:])[0].strip().upper()
    r = validate(isin)
    mark = "✅ PASS" if r["pass"] else "❌ ISSUES"
    print(f"{mark} — {isin}")
    for o in r.get("ok", []):
        print(f"   ✓ {o}")
    for i in r.get("issues", []):
        print(f"   ✗ {i}")
    return 0 if r["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
