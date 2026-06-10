"""fetch_annual_report.py — Sourcing A.2: descarga los AR OFICIALES y COMPLETOS
de data/known_annual_reports.json a raw/discovery de cada fondo.

Resuelve el problema raíz INT: el discovery automático no encontraba el AR del
sub-fondo (o cogía snapshots truncados de Wayback). Aquí usamos URLs directas
oficiales (verificadas por agentes de investigación), descargamos COMPLETO
(valida %%EOF), confirmamos que es del fondo (verify_doc_for_fund) y lo dejamos
en raw/discovery/ con nombre annual_report_{year}, listo para el extractor.

Un AR de PARAGUAS cubre varios sub-fondos → se descarga 1 vez (cache) y se copia
a cada ISIN.

CLI:
    python -m tools.fetch_annual_report                 # todos los de la KB
    python -m tools.fetch_annual_report --umbrella "Vontobel Fund"
    python -m tools.fetch_annual_report --isin LU0153585137
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import httpx

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"
KB_PATH = ROOT / "data" / "known_annual_reports.json"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


def _download(url: str, dest: Path) -> tuple[bool, str]:
    """Descarga url->dest. Devuelve (ok, motivo). Rechaza truncados (sin %%EOF)."""
    try:
        with httpx.Client(follow_redirects=True, timeout=120.0,
                          headers={"User-Agent": _UA}) as c:
            r = c.get(url)
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}"
        content = r.content
        if not content.startswith(b"%PDF"):
            return False, "no es PDF"
        if b"%%EOF" not in content[-8192:]:
            return False, f"truncado/incompleto ({len(content)} bytes, sin %%EOF)"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
        return True, f"OK ({len(content) // 1024} KB)"
    except Exception as e:
        return False, f"error: {str(e)[:60]}"


def run(umbrella_filter: str = "", isin_filter: str = "") -> int:
    kb = json.loads(KB_PATH.read_text(encoding="utf-8"))
    cache: dict[str, Path] = {}   # url -> ruta descargada (para copiar entre ISINs)
    n_ok = n_fail = 0
    for uname, u in kb.get("umbrellas", {}).items():
        if umbrella_filter and umbrella_filter.lower() not in uname.lower():
            continue
        isins = u.get("isins", [])
        if isin_filter:
            isins = [i for i in isins if i == isin_filter]
            if not isins:
                continue
        for rep in u.get("reports", []):
            year, url = rep.get("year"), rep.get("url")
            # descargar 1 vez por URL (paraguas compartido). Reusar caché en disco.
            from tools.verify_fund_docs import is_complete_pdf
            tmp = cache.get(url)
            if tmp is None:
                tmp = ROOT / "data" / ".ar_cache" / f"{uname.replace(' ', '_')}_{year}.pdf"
                if tmp.exists() and is_complete_pdf(tmp):
                    print(f"[{uname} {year}] caché en disco OK ({tmp.stat().st_size // 1024} KB)")
                    cache[url] = tmp
                else:
                    # Antes de ir a la fuente, intentar recuperar del ARCHIVO
                    # durable (Supabase). Así no re-descargamos años tras año y
                    # sobrevive a que la fuente muera (Wayback/CDN/Finect efímeros).
                    from tools.doc_archive import retrieve as _arch_retrieve
                    _fn = f"annual_report_{year}.pdf"
                    if isins and _arch_retrieve(isins[0], _fn, tmp) and is_complete_pdf(tmp):
                        print(f"[{uname} {year}] recuperado del archivo durable")
                        cache[url] = tmp
                    else:
                        ok, why = _download(url, tmp)
                        print(f"[{uname} {year}] descarga: {why}")
                        if not ok:
                            n_fail += 1
                            continue
                        cache[url] = tmp
            # copiar a cada sub-fondo (los AR de la KB ya están verificados por
            # los agentes; aquí solo confirmamos completitud, rápido).
            for isin in isins:
                dest = FUNDS_DIR / isin / "raw" / "discovery" / f"annual_report_{year}.pdf"
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(tmp, dest)
                status = "OK completo" if is_complete_pdf(dest) else "⚠ incompleto"
                # Archivar (durable, content-addressed → el AR umbrella se sube 1 vez)
                try:
                    from tools.doc_archive import archive_file
                    if archive_file(dest, isin, f"annual_report_{year}.pdf"):
                        status += " + archivado"
                except Exception:
                    pass
                print(f"    {isin} {year}: {status}")
                n_ok += 1
    print(f"\nResumen: {n_ok} AR colocados, {n_fail} descargas fallidas.")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Descarga AR oficiales (sourcing A.2)")
    ap.add_argument("--umbrella", default="")
    ap.add_argument("--isin", default="")
    args = ap.parse_args(argv)
    return run(args.umbrella, args.isin)


if __name__ == "__main__":
    sys.exit(main())
