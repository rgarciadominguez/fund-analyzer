"""verify_fund_docs.py — Fase A.2: verifica que un documento descargado es
REALMENTE del fondo target y está sano, antes de extraerlo.

Rechaza dos clases de basura que ensucian el análisis INT:
  - CORRUPTO/TRUNCADO: PDFs cortados (p.ej. snapshots de Wayback a 1MB) que no
    parsean ("Unexpected EOF") → inservibles.
  - AJENO: docs de OTRO fondo/gestora (no contienen el ISIN ni el nombre del
    fondo target) → contaminan estrategia/cartera/historia.

Para umbrella SICAVs el ISIN de la clase puede estar en pág 40+; se escanea
amplio (hasta `max_pages`) y se acepta por ISIN o por tokens distintivos del
nombre del sub-fondo.

Uso:
  verify_doc_for_fund(path, isin, fund_name) -> (ok: bool, reason: str)
CLI:
  python -m tools.verify_fund_docs <ISIN>          # informe de sus docs
  python -m tools.verify_fund_docs --int           # todos los INT
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"

_GENERIC = frozenset({
    "fund", "funds", "fondo", "fondos", "class", "clase", "sicav", "ucits",
    "global", "value", "equity", "equities", "bond", "bonds", "capital",
    "invest", "investment", "investments", "sub", "growth", "income", "europe",
    "european", "world", "international", "select", "plus", "trust", "asset",
    "management", "opportunities", "premium", "qi", "the", "index",
})


def is_complete_pdf(path) -> bool:
    """True si el PDF parece COMPLETO (no truncado). Un PDF válido termina con
    el marcador '%%EOF'. Los snapshots truncados de Wayback (cortados a 1MB
    exacto) NO lo tienen → ilegibles. Cheque rápido sin parsear todo el doc."""
    try:
        p = Path(path)
        size = p.stat().st_size
        if size < 1024:
            return False
        with p.open("rb") as fh:
            head = fh.read(5)
            if not head.startswith(b"%PDF"):
                return False
            fh.seek(max(0, size - 4096))
            tail = fh.read()
        return b"%%EOF" in tail
    except Exception:
        return False


def _name_tokens(fund_name: str, gestora: str = "") -> list[str]:
    # Excluir tokens de la GESTORA: 'robeco' aparece en todos los docs
    # corporativos de Robeco → demasiado débil. Exigimos tokens del SUB-fondo.
    gtok = {t for t in re.split(r"[^a-z0-9]+", (gestora or "").lower()) if len(t) >= 4}
    return [t for t in re.split(r"[^a-z0-9]+", (fund_name or "").lower())
            if len(t) >= 4 and t not in _GENERIC and t not in gtok]


def verify_doc_for_fund(path, isin: str, fund_name: str = "", gestora: str = "", max_pages: int = 80) -> tuple[bool, str]:
    """(ok, reason). ok=True si el doc parsea y menciona el ISIN o el nombre."""
    p = Path(path)
    if not p.exists():
        return False, "no_existe"
    if not is_complete_pdf(p):
        return False, "truncado/incompleto (sin %%EOF)"
    try:
        from tools.pdf_extractor import extract_page_range
        text = extract_page_range(str(p), 0, max_pages)
    except Exception as e:
        return False, f"corrupto ({str(e)[:40]})"
    if not text or len(text.strip()) < 200:
        return False, "sin_texto (imagen/corrupto)"
    tl = text.lower()
    if isin.lower() in tl:
        return True, "ok (ISIN presente)"
    toks = _name_tokens(fund_name, gestora)
    if toks and sum(1 for t in toks if t in tl) >= max(1, len(toks) // 2):
        return True, "ok (nombre del fondo)"
    return False, "ajeno (ni ISIN ni nombre del sub-fondo)"


def _report(isin: str):
    j = {}
    p = FUNDS_DIR / isin / "output.json"
    if p.exists():
        try:
            j = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    fund_name = j.get("nombre", "")
    gestora = j.get("gestora", "")
    rd = FUNDS_DIR / isin / "raw" / "discovery"
    pdfs = sorted(rd.glob("*.pdf")) if rd.exists() else []
    print(f"=== {isin}  ({fund_name[:40]}) — {len(pdfs)} PDFs ===")
    good = bad = 0
    for pdf in pdfs:
        ok, reason = verify_doc_for_fund(pdf, isin, fund_name, gestora)
        good += ok
        bad += (not ok)
        mark = "OK " if ok else "XX "
        print(f"  {mark} {pdf.name[:50]:50} {reason}")
    print(f"  -> validos: {good} | rechazados: {bad}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Verifica docs de fondo (Fase A.2)")
    ap.add_argument("isin", nargs="?")
    ap.add_argument("--int", action="store_true", help="todos los INT")
    args = ap.parse_args(argv)
    if args.int:
        for d in sorted(FUNDS_DIR.iterdir()):
            if not d.is_dir() or "." in d.name:
                continue
            op = d / "output.json"
            if op.exists():
                try:
                    if json.loads(op.read_text(encoding="utf-8")).get("tipo") == "INT":
                        _report(d.name)
                except Exception:
                    pass
        return 0
    if not args.isin:
        ap.error("indica ISIN o --int")
    _report(args.isin.strip().upper())
    return 0


if __name__ == "__main__":
    sys.exit(main())
