"""Audit general de los fondos: compara aum/posiciones de output.json
contra el último PDF semestral CNMV. Detecta discrepancias para fix."""
import json
import re
import sys
import glob
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

import pdfplumber

ROOT = Path(__file__).parent
ISINS = sorted([p.name for p in (ROOT / "data" / "funds").glob("ES*")])


def num_es(s):
    """Parse spanish number: 1.450,26 -> 1450.26"""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def extract_pdf_metrics(pdf_path):
    """Extract AUM (sum of all classes) + n_isins in sec10 from latest PDF."""
    with pdfplumber.open(pdf_path) as pdf:
        text = "\n".join((p.extract_text() or "") for p in pdf.pages)

    # AUM por clase: tabla "Patrimonio (en miles de EUR)"
    # Buscar líneas tipo "CLASE X EUR  300.638  ..."
    aum_pattern = re.compile(
        r'CLASE\s+\w+\s+(?:EUR|USD|GBP|CHF|JPY)\s+([\d.,]+)',
        re.IGNORECASE,
    )
    # Solo coger después de un header tipo "Patrimonio (en miles)" o variantes
    pat_section = re.search(
        r'(?:Patrimonio\s*\(en\s+miles[^)]*\)|patrimonio\s*por\s*clase|\(miles\s+de\s+EUR\))(.{0,3000})',
        text, re.IGNORECASE | re.DOTALL,
    )
    aums_miles = []
    if pat_section:
        block = pat_section.group(1)
        # Tomar primera columna ("Al final del periodo")
        for m in aum_pattern.finditer(block):
            v = num_es(m.group(1))
            if v and v > 100:  # > 100 miles = real AUM
                aums_miles.append(v)
    aum_total_meur = sum(aums_miles) / 1000 if aums_miles else None

    # Fallback: línea "PATRIMONIO FIN PERIODO ACTUAL (miles de EUR) 464.061 ..."
    # Suma columnas de todas las clases si hay múltiples filas, o usa primera columna
    if aum_total_meur is None:
        actual_lines = re.findall(
            r'PATRIMONIO\s+FIN\s+PERIODO\s+ACTUAL[^\n]*?(\d{1,3}(?:\.\d{3})*(?:,\d+)?)',
            text, re.IGNORECASE,
        )
        if actual_lines:
            # Primera línea suele ser fondo/clase principal o suma
            v = num_es(actual_lines[0])
            if v and v > 100:
                aum_total_meur = round(v / 1000, 2)

    # ISINs en sec 10
    sec10_match = re.search(
        r'10\.?\s*Detalle\s+de\s+inv(?:er)?siones\s+financieras(.+?)(?=\n\s*1[1-9]\.\s|$)',
        text, re.I | re.S,
    )
    n_isins_pdf = 0
    if sec10_match:
        sec10 = sec10_match.group(1)
        isins = set(re.findall(r'\b[A-Z]{2}[A-Z0-9]{10}\b', sec10))
        n_isins_pdf = len(isins)

    # Partícipes total (suma por clase)
    parts_pattern = re.compile(
        r'CLASE\s+\w+\s+([\d.,]+)\s+([\d.,]+)\s+EUR',
        re.IGNORECASE,
    )
    # En tabla "Nº de partícipes": CLASE X  408  408  EUR
    parts_section = re.search(
        r'N[º°o]\s*de\s*part[ií]cipes(.{0,1500})',
        text, re.IGNORECASE | re.DOTALL,
    )
    parts_total = None
    if parts_section:
        block = parts_section.group(1)
        parts = []
        for m in parts_pattern.finditer(block):
            v = num_es(m.group(1))
            if v:
                parts.append(int(v))
        if parts:
            parts_total = sum(parts)

    return {
        "aum_total_meur_pdf": round(aum_total_meur, 2) if aum_total_meur else None,
        "aums_por_clase_miles": aums_miles,
        "n_isins_sec10_pdf": n_isins_pdf,
        "participes_pdf": parts_total,
    }


def audit_fund(isin):
    fund_dir = ROOT / "data" / "funds" / isin
    out_path = fund_dir / "output.json"
    if not out_path.exists():
        return {"isin": isin, "error": "output.json missing"}
    with open(out_path, encoding="utf-8") as f:
        d = json.load(f)

    pdfs = sorted(glob.glob(str(fund_dir / "raw" / "reports" / f"CNMV_{isin}_*_H*.pdf")))
    if not pdfs:
        return {"isin": isin, "error": "no PDFs"}
    latest_pdf = pdfs[-1]

    pdf_metrics = extract_pdf_metrics(latest_pdf)
    aum_output = (d.get("kpis") or {}).get("aum_actual_meur")
    parts_output = (d.get("kpis") or {}).get("num_participes")
    n_pos_output = len((d.get("posiciones", {}).get("actuales") or []))

    aum_diff = None
    if pdf_metrics["aum_total_meur_pdf"] and aum_output:
        aum_diff = round(aum_output - pdf_metrics["aum_total_meur_pdf"], 2)

    parts_diff = None
    if pdf_metrics["participes_pdf"] and parts_output:
        parts_diff = parts_output - pdf_metrics["participes_pdf"]

    pos_diff = None
    if pdf_metrics["n_isins_sec10_pdf"]:
        pos_diff = n_pos_output - pdf_metrics["n_isins_sec10_pdf"]

    return {
        "isin": isin,
        "nombre": d.get("nombre", "")[:35],
        "pdf": Path(latest_pdf).name,
        "aum_pdf": pdf_metrics["aum_total_meur_pdf"],
        "aum_out": aum_output,
        "aum_diff": aum_diff,
        "n_clases": len(pdf_metrics["aums_por_clase_miles"]),
        "parts_pdf": pdf_metrics["participes_pdf"],
        "parts_out": parts_output,
        "parts_diff": parts_diff,
        "isins_pdf": pdf_metrics["n_isins_sec10_pdf"],
        "pos_out": n_pos_output,
        "pos_diff": pos_diff,
    }


if __name__ == "__main__":
    targets = sys.argv[1:] if len(sys.argv) > 1 else ISINS
    results = []
    print(f"{'ISIN':<14} {'Nombre':<32} {'AUM_pdf':>10} {'AUM_out':>10} {'Δ':>8} {'cl':>3} {'parts_pdf':>9} {'parts_out':>9} {'Δ':>6} {'pos_pdf':>7} {'pos_out':>7} {'Δ':>5}")
    print("-" * 140)
    for isin in targets:
        r = audit_fund(isin)
        results.append(r)
        if "error" in r:
            print(f"{isin:<14} ERROR: {r['error']}")
            continue
        aum_str = f"{r['aum_pdf']:>10.1f}" if r['aum_pdf'] else "      n/a"
        aum_out_str = f"{r['aum_out']:>10.1f}" if r['aum_out'] else "      n/a"
        aum_diff_str = f"{r['aum_diff']:+8.1f}" if r['aum_diff'] is not None else "    n/a"
        # Flag big diffs (>5%)
        flag = ""
        if r['aum_pdf'] and r['aum_out'] and r['aum_pdf'] > 0:
            pct = abs(r['aum_diff']) / r['aum_pdf'] * 100
            if pct > 5:
                flag = " ⚠"
        parts_pdf_str = f"{r['parts_pdf']:>9}" if r['parts_pdf'] else "     n/a "
        parts_out_str = f"{r['parts_out']:>9}" if r['parts_out'] else "     n/a "
        parts_diff_str = f"{r['parts_diff']:+6}" if r['parts_diff'] is not None else "   n/a"
        print(f"{r['isin']:<14} {r['nombre']:<32} {aum_str} {aum_out_str} {aum_diff_str} {r['n_clases']:>3} {parts_pdf_str} {parts_out_str} {parts_diff_str} {r['isins_pdf']:>7} {r['pos_out']:>7} {r['pos_diff'] or 0:>+5}{flag}")

    # Summary
    print()
    n_aum_diff = sum(1 for r in results if isinstance(r.get('aum_diff'), (int, float)) and abs(r['aum_diff']) > 5 and r.get('aum_pdf'))
    n_pos_diff = sum(1 for r in results if isinstance(r.get('pos_diff'), int) and r['pos_diff'] != 0)
    print(f"Fondos con AUM diff > 5: {n_aum_diff}")
    print(f"Fondos con posiciones diff != 0: {n_pos_diff}")

    # Save detailed report
    with open(ROOT / "audit_report.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\nDetalle: audit_report.json")
