"""ensure_kb_letters.py — Garantiza que las CARTAS del gestor (letters from the fund managers /
comentarios trimestrales) de la KB (known_manager_letters.json) se descarguen y se REGISTREN
como documentos de discovery (doc_type=quarterly_letter).

Motivo: discovery_v2 NO encuentra las cartas HTML de la gestora (p.ej. carmignac.com/articles/
...letter-from-the-fund-managers-qX-YYYY) → el análisis se quedaba con AR/factsheet y CERO cartas.
Registradas como quarterly_letter, las coge:
  - letters_collector  → letters_data.json → el analyst las usa (K15: tesis/decisiones/outlook).
  - archive_docs       → documentos.cartas (almacenadas y consultables por el copiloto).

Soporta artículos HTML (extrae el texto) además de PDF. Se llama desde la prep del orchestrator,
DESPUÉS de discovery_v2 (para anexar a intl_discovery_data.json) y ANTES de letters_collector.
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _kb_entry_for(isin: str) -> dict:
    """Entrada de la KB de cartas que cubre este ISIN, o {}."""
    kb_path = ROOT / "data" / "known_manager_letters.json"
    if not kb_path.exists():
        return {}
    try:
        kb = json.loads(kb_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    for name, e in (kb.get("funds") or {}).items():
        if isin.upper() in [str(i).upper() for i in (e.get("isins") or [])]:
            return {"nombre": name, **e}
    return {}


def _periodo_from_url(url: str) -> str:
    """q2-2026 / 2026-q2 → '2026-Q2'; si no, el año suelto."""
    m = re.search(r"[-/_]q([1-4])[-_](20\d{2})", url, re.I)
    if m:
        return f"{m.group(2)}-Q{m.group(1)}"
    m = re.search(r"(20\d{2})[-_]q([1-4])", url, re.I)
    if m:
        return f"{m.group(1)}-Q{m.group(2)}"
    m = re.search(r"(20\d{2})[-_/](0[1-9]|1[0-2])(?![0-9])", url)     # mensual -> YYYY-MM
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.search(r"(20\d{2})", url)
    return m.group(1) if m else ""


def _slug(url: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "-", url.split("//")[-1]).strip("-")[:80]


def _fetch(url: str):
    """Descarga la carta. Devuelve (texto, content_type, pdf_bytes). HTML → texto del artículo."""
    import urllib.request
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"})
    raw = urllib.request.urlopen(req, timeout=60).read()
    if raw[:5] == b"%PDF-":
        return None, "application/pdf", raw
    html = raw.decode("utf-8", errors="replace")
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "nav", "header", "footer", "form", "aside"]):
            tag.decompose()
        main = soup.find("article") or soup.find("main") or soup.body or soup
        text = main.get_text("\n", strip=True)
    except Exception:
        text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text, "text/html", None


def ensure(isin: str, log=print) -> dict:
    """Descarga+registra las cartas de la KB para el ISIN. No hace nada si no está en la KB."""
    isin = isin.upper()
    entry = _kb_entry_for(isin)
    if not entry:
        return {"in_kb": False, "registered": 0}
    letters = entry.get("letters") or []
    if not letters:
        return {"in_kb": True, "registered": 0}

    fund_dir = ROOT / "data" / "funds" / isin
    ldir = fund_dir / "raw" / "letters"
    ldir.mkdir(parents=True, exist_ok=True)
    disc_path = fund_dir / "intl_discovery_data.json"
    disc = {}
    if disc_path.exists():
        try:
            disc = json.loads(disc_path.read_text(encoding="utf-8"))
        except Exception:
            disc = {}
    docs = disc.get("documents") or []
    existing = {d.get("url") for d in docs if d.get("url")}

    registered = 0
    for lt in letters:
        url = lt.get("url")
        if not url or url in existing:
            continue
        periodo = lt.get("periodo") or _periodo_from_url(url)
        try:
            text, ctype, pdf_bytes = _fetch(url)
        except Exception as e:  # noqa: BLE001
            log(f"[KB_LETTERS] no descargada ({str(e)[:40]}): {url[:60]}")
            continue
        stem = f"kb_letter_{periodo or _slug(url)}"
        if ctype == "application/pdf":
            dest = ldir / f"{stem}.pdf"
            dest.write_bytes(pdf_bytes)
        else:
            if not text or len(text) < 400:
                log(f"[KB_LETTERS] artículo vacío/corto, ignorado: {url[:60]}")
                continue
            dest = ldir / f"{stem}.txt"
            dest.write_text(text, encoding="utf-8")
        docs.append({
            "doc_type": "quarterly_letter",
            "periodo": periodo,
            "url": url,
            "local_path": str(dest),
            "source": "kb_manager_letters",
            "source_detail": entry.get("nombre"),
            "content_type": ctype,
            "fecha_publicacion": periodo,
            "validated": True,
            "contains_manager_commentary": True,
            "lang": "en",
        })
        existing.add(url)
        registered += 1
        log(f"[KB_LETTERS] carta registrada: {periodo} ({dest.name})")

    if registered:
        disc["documents"] = docs
        disc.setdefault("isin", isin)
        disc["ultima_actualizacion"] = datetime.now(timezone.utc).isoformat()
        disc_path.parent.mkdir(parents=True, exist_ok=True)
        disc_path.write_text(json.dumps(disc, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"[KB_LETTERS] {isin}: {registered} carta(s) de la KB registradas en discovery")
    return {"in_kb": True, "registered": registered}


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("isin")
    a = ap.parse_args()
    print(json.dumps(ensure(a.isin), ensure_ascii=False))


if __name__ == "__main__":
    main()
