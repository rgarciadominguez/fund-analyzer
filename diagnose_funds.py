"""
Diagnose each fund's output.json:
- Cross-check nombre vs latest CNMV semestral PDF
- Detect gaps in KPIs, gestores, posiciones, series
- Read meta_report quality scores
"""
import json
import os
import sys
import re
from pathlib import Path

import pdfplumber

ROOT = Path(__file__).parent
DATA = ROOT / "data" / "funds"


def latest_pdf_name(isin):
    pdf_dir = DATA / isin / "raw" / "reports"
    if not pdf_dir.exists():
        return None, None
    pdfs = sorted(pdf_dir.glob(f"CNMV_{isin}_*_H*.pdf"))
    if not pdfs:
        return None, None
    last = pdfs[-1]
    try:
        with pdfplumber.open(last) as pdf:
            text = pdf.pages[0].extract_text() or ""
        # Fund name typically appears at top
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        # Skip very short lines, find first uppercase line that looks like a fund name
        for l in lines[:5]:
            if any(c.isalpha() for c in l) and l.isupper() and "INFORME" not in l and "CNMV" not in l:
                return l, last.name
        return lines[0] if lines else None, last.name
    except Exception as e:
        return f"ERROR: {e}", last.name


def diagnose(isin):
    fund_dir = DATA / isin
    out_path = fund_dir / "output.json"
    if not out_path.exists():
        return {"isin": isin, "error": "output.json missing"}

    with open(out_path, encoding="utf-8") as f:
        d = json.load(f)

    issues = []

    # Header
    nombre = d.get("nombre") or ""
    gestora = d.get("gestora") or ""
    pdf_name, pdf_file = latest_pdf_name(isin)

    name_match = "?"
    if pdf_name:
        # Check if any significant token of pdf_name appears in nombre
        pdf_tokens = set(re.findall(r"[A-ZÁÉÍÓÚÑ]{4,}", pdf_name))
        nom_tokens = set(re.findall(r"[A-ZÁÉÍÓÚÑ]{4,}", nombre.upper()))
        common = pdf_tokens & nom_tokens
        if not common and pdf_tokens:
            issues.append(f"NOMBRE_MISMATCH: output='{nombre}' vs PDF='{pdf_name}'")
            name_match = "MISMATCH"
        else:
            name_match = "OK"

    # KPIs
    kpis = d.get("kpis") or {}
    null_kpis = [k for k, v in kpis.items() if v in (None, "", 0) and k not in ("rotacion_cartera_anterior_pct",)]
    if not kpis.get("aum_actual_meur"):
        issues.append("KPI: AUM actual missing")
    if not kpis.get("ter_pct"):
        issues.append("KPI: TER missing")
    if not kpis.get("num_participes"):
        issues.append("KPI: partícipes missing")

    # Gestores
    cual = d.get("cualitativo") or {}
    gestores = d.get("gestores") or cual.get("gestores") or []
    if not gestores:
        issues.append("GESTORES: vacío")

    # Posiciones
    pos = d.get("posiciones") or {}
    n_pos = len(pos.get("actuales") or [])
    if n_pos == 0:
        issues.append("POSICIONES: actuales vacías")

    # Series
    cu = d.get("cuantitativo") or {}
    if not cu.get("serie_aum"):
        issues.append("SERIE: AUM vacía")
    if not cu.get("serie_rentabilidad"):
        issues.append("SERIE: rentabilidad vacía")
    if not cu.get("serie_ter"):
        issues.append("SERIE: TER vacía")

    # Análisis consistencia
    ac = d.get("analisis_consistencia") or {}
    if not ac.get("resumen_global"):
        issues.append("ANÁLISIS: resumen_global vacío")

    # Letters / readings
    letters_path = fund_dir / "letters_data.json"
    n_letters = 0
    if letters_path.exists():
        with open(letters_path, encoding="utf-8") as f:
            ld = json.load(f)
        n_letters = len(ld.get("cartas") or ld.get("letters") or [])

    readings_path = fund_dir / "readings_data.json"
    n_readings = 0
    if readings_path.exists():
        with open(readings_path, encoding="utf-8") as f:
            rd = json.load(f)
        n_readings = len(rd.get("lecturas") or rd.get("readings") or [])

    # Quality / meta
    meta_path = fund_dir / "meta_report.json"
    qscore = None
    if meta_path.exists():
        with open(meta_path, encoding="utf-8") as f:
            mr = json.load(f)
        qscore = mr.get("quality_score") or mr.get("score")

    qrep_path = fund_dir / "quality_report.json"
    qissues = 0
    if qrep_path.exists():
        with open(qrep_path, encoding="utf-8") as f:
            qr = json.load(f)
        qissues = len(qr.get("issues") or qr.get("problems") or [])

    return {
        "isin": isin,
        "nombre_output": nombre,
        "nombre_pdf": pdf_name,
        "pdf_file": pdf_file,
        "name_match": name_match,
        "gestora": gestora,
        "aum_meur": kpis.get("aum_actual_meur"),
        "participes": kpis.get("num_participes"),
        "ter_pct": kpis.get("ter_pct"),
        "n_gestores": len(gestores),
        "n_posiciones": n_pos,
        "n_letters": n_letters,
        "n_readings": n_readings,
        "quality_score": qscore,
        "quality_issues": qissues,
        "issues": issues,
    }


if __name__ == "__main__":
    isins = [
        "ES0112231008", "ES0116567035", "ES0128520006",
        "ES0140794001", "ES0156572002", "ES0173311103",
        "ES0175316001", "ES0175414012", "ES0175437039",
        "ES0175902008", "ES0182527038",
    ]
    if len(sys.argv) > 1:
        isins = sys.argv[1:]

    results = []
    for isin in isins:
        r = diagnose(isin)
        results.append(r)

    # Print compact report
    print(f"{'ISIN':<14} {'NM':<8} {'AUM':>8} {'PART':>6} {'TER':>6} {'GES':>4} {'POS':>5} {'LET':>4} {'RDG':>4} {'QS':>4} ISSUES")
    print("-" * 120)
    for r in results:
        nm = r.get("name_match", "?")[:8]
        aum = f"{r.get('aum_meur') or 0:>8.0f}" if r.get('aum_meur') else "    null"
        part = f"{r.get('participes') or 0:>6}" if r.get('participes') else "  null"
        ter = f"{r.get('ter_pct') or 0:>6.2f}" if r.get('ter_pct') else "  null"
        ng = r.get("n_gestores") or 0
        np = r.get("n_posiciones") or 0
        nl = r.get("n_letters") or 0
        nr = r.get("n_readings") or 0
        qs = r.get("quality_score") or "-"
        n_issues = len(r.get("issues") or [])
        print(f"{r['isin']:<14} {nm:<8} {aum} {part} {ter} {ng:>4} {np:>5} {nl:>4} {nr:>4} {str(qs):>4} {n_issues} issues")

    print()
    print("=" * 80)
    print("DETALLE DE ISSUES:")
    for r in results:
        if r.get("issues"):
            print(f"\n--- {r['isin']} ({r.get('nombre_output','?')[:40]}) ---")
            print(f"  PDF último: {r.get('pdf_file')} -> '{r.get('nombre_pdf')}'")
            for i in r["issues"]:
                print(f"  - {i}")

    # Save to JSON
    with open(ROOT / "diagnosis.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nResults saved: diagnosis.json")
