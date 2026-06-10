"""finect_sourcing.py — Canal de sourcing de AR/SAR vía Finect (pista de Rafa,
2026-06-10). Genérico por ISIN, sirve para CUALQUIER fondo.

Receta (crackeada por agente Opus + validada):
  1. GET finect.com/fondos-inversion/{ISIN}-fund (slug da igual → 301 a canónico).
  2. La pagina lleva un blob JSON URL-encoded con investmentid (= Morningstar
     SecId) y URLs `doclegal?...documenttype=N`. documenttype: 4=AnnualReport,
     5=SemiannualReport, 1=Prospectus, 52=Factsheet, 74=KIID, 299=KID, 14=Quarterly.
  3. El doclegal redirige (302) a un .msdoc servido como application/pdf
     (backend Morningstar). Sirve SIEMPRE el AR/SAR MÁS RECIENTE (no históricos).

Limitación: solo el último AR/SAR (Finect no lista años previos; su Wayback no
archivó los PDFs). Para históricos: swissfunddata / hashes doc.morningstar /
Wayback id_ (ver [[int-annual-report-sourcing]] / known_annual_reports.json).

Uso:
  finect_report_urls(isin) -> {"annual_report": url|None, "semi_annual_report": url|None}
CLI:
  python -m tools.finect_sourcing IE00BF5GGB04          # muestra URLs
  python -m tools.finect_sourcing IE00BF5GGB04 --download  # baja a raw/discovery
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from urllib.parse import unquote

import httpx

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
_DOCTYPE = {"4": "annual_report", "5": "semi_annual_report"}


def finect_report_urls(isin: str) -> dict:
    """{annual_report: url|None, semi_annual_report: url|None} desde Finect."""
    out = {"annual_report": None, "semi_annual_report": None}
    try:
        with httpx.Client(follow_redirects=True, timeout=30,
                          headers={"User-Agent": _UA}) as c:
            r = c.get(f"https://www.finect.com/fondos-inversion/{isin}-fund")
            if r.status_code != 200:
                return out
            txt = unquote(r.text)
    except Exception:
        return out
    # URLs doclegal completas en la pagina (con su market correcto)
    for m in re.finditer(r'https://www\.finect\.com/doclegal\?[^"\\\s]*documenttype=(\d+)[^"\\\s]*', txt):
        dt = _DOCTYPE.get(m.group(1))
        if dt and not out[dt]:
            out[dt] = m.group(0).replace("&amp;", "&")
    # fallback: reconstruir desde investmentid si no había URLs en la pagina
    if not out["annual_report"]:
        sec = re.search(r'investmentid["=:\s]+([A-Z0-9]{8,12})', txt)
        if sec:
            sid = sec.group(1)
            base = f"https://www.finect.com/doclegal?language=451&investmentid={sid}&investmenttype=1&frame=0"
            out["annual_report"] = base + "&documenttype=4"
            out["semi_annual_report"] = base + "&documenttype=5"
    return out


def _download_verify(url: str, dest: Path) -> bool:
    try:
        with httpx.Client(follow_redirects=True, timeout=120,
                          headers={"User-Agent": _UA}) as c:
            r = c.get(url)
        content = r.content
        if r.status_code != 200 or not content.startswith(b"%PDF"):
            return False
        if b"%%EOF" not in content[-8192:]:
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        return True
    except Exception:
        return False


def fetch_finect_reports(isin: str, download: bool = False) -> dict:
    urls = finect_report_urls(isin)
    result = {}
    for dt, url in urls.items():
        if not url:
            result[dt] = None
            continue
        if not download:
            result[dt] = url
            continue
        fname = "annual_report_finect.pdf" if dt == "annual_report" else "semi_annual_finect.pdf"
        dest = FUNDS_DIR / isin / "raw" / "discovery" / fname
        ok = _download_verify(url, dest)
        result[dt] = f"OK {dest.stat().st_size // 1024}KB" if ok else "FALLO/incompleto"
    return result


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Sourcing de AR/SAR vía Finect")
    ap.add_argument("isin")
    ap.add_argument("--download", action="store_true")
    args = ap.parse_args(argv)
    res = fetch_finect_reports(args.isin.strip().upper(), download=args.download)
    for dt, v in res.items():
        print(f"  {dt}: {v}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
