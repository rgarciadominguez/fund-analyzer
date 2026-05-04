"""Rebuild serie_ter_por_clase con nombres comerciales reales.

Fase G Bug 1 (2026-04-28): el cnmv_agent v6 asignaba A/B/C/D por posición.
v7 lee el nombre real de "Individual CLASE X" antes de cada tabla TER.

Este script re-aplica SOLO el parser de TER por clase a los PDFs ya descargados
y actualiza serie_ter_por_clase en output.json. NO toca otros campos. NO llama LLMs.

Uso:
    python -m tools.rebuild_class_names --isin ES0159259011
    python -m tools.rebuild_class_names --all
"""
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from tools.pdf_extractor import extract_page_range, get_pdf_metadata


def _parse_ter_por_clase_from_pdf(pdf_path: Path) -> dict:
    """Re-aplica solo el extractor de TER por clase del cnmv_agent v7."""
    meta = get_pdf_metadata(str(pdf_path))
    text = extract_page_range(str(pdf_path), 0, meta["num_pages"])
    text = re.sub(r"\(cid:\d+\)", " ", text)

    ter_matches = list(re.finditer(r"Ratio\s+total\s+de\s+gastos", text, re.IGNORECASE))
    ter_por_clase = {}

    def _find_class(match_pos: int, table_idx: int) -> str:
        preceding = text[max(0, match_pos - 3000): match_pos]
        cls = list(re.finditer(
            r"(?:Individual|[A-Z]\))\s*CLASE\s+([A-Z0-9]{1,4})\b",
            preceding, re.IGNORECASE,
        ))
        if cls:
            return cls[-1].group(1).upper()
        return "A" if table_idx == 0 else chr(ord("A") + table_idx)

    for idx, ter_m in enumerate(ter_matches):
        block = text[ter_m.start(): ter_m.start() + 400]
        nums = []
        for n in re.findall(r"[\d,]+", block):
            try:
                v = float(n.replace(",", "."))
                if 0.01 <= v <= 5.0:
                    nums.append(v)
            except ValueError:
                pass
        if not nums:
            continue
        clase = _find_class(ter_m.start(), idx)
        ter_por_clase[clase] = nums[0]

    return ter_por_clase


def _year_from_pdf_name(fname: str) -> int | None:
    m = re.search(r"_(\d{4})_H[12]\.pdf$", fname, re.IGNORECASE)
    return int(m.group(1)) if m else None


def rebuild_for_isin(isin: str) -> bool:
    fund_dir = ROOT / "data" / "funds" / isin
    output_path = fund_dir / "output.json"
    reports_dir = fund_dir / "raw" / "reports"
    if not output_path.exists() or not reports_dir.exists():
        return False

    data = json.loads(output_path.read_text(encoding="utf-8"))
    cuant = data.get("cuantitativo", {})
    if not cuant:
        return False

    # Re-parsear cada PDF y reconstruir serie_ter_por_clase
    nueva_serie = []
    pdfs = sorted(reports_dir.glob("*_H2.pdf"))
    for pdf in pdfs:
        year = _year_from_pdf_name(pdf.name)
        if year is None:
            continue
        ter_clases = _parse_ter_por_clase_from_pdf(pdf)
        if ter_clases:
            nueva_serie.append({"periodo": str(year), "clases": ter_clases})

    if not nueva_serie:
        return False

    nueva_serie.sort(key=lambda x: x["periodo"])
    cuant["serie_ter_por_clase"] = nueva_serie
    output_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def _main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = sys.argv[1:]
    if not args:
        print(__doc__)
        return
    if args[0] == "--all":
        funds_dir = ROOT / "data" / "funds"
        updated = 0
        for fd in sorted(funds_dir.iterdir()):
            if fd.is_dir() and rebuild_for_isin(fd.name):
                updated += 1
                print(f"  [OK] {fd.name}")
        print(f"\nActualizados: {updated} fondos.")
    elif args[0] == "--isin" and len(args) >= 2:
        isin = args[1]
        ok = rebuild_for_isin(isin)
        print(f"{isin}: {'OK' if ok else 'FAIL'}")
        if ok:
            data = json.loads((ROOT / "data" / "funds" / isin / "output.json").read_text(encoding="utf-8"))
            serie = data.get("cuantitativo", {}).get("serie_ter_por_clase", [])
            for s in serie[-3:]:
                print(f"  {s}")


if __name__ == "__main__":
    _main()
