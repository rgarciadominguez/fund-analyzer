"""aum_pdf_recover.py — Recuperación DETERMINISTA del AUM desde los PDFs locales.

Motivación (2026-06-04): SIFTER y ROBECO estaban publicados con AUM=None pese a
tener el dato en un PDF descargado, en formatos totalmente estándar:
  - Annual report:  "Total net assets EUR 280,753,454.11"      (Sifter)
  - Factsheet:      "Total size of fund EUR 7,580,260,321"      (Robeco)
La extracción vía LLM lo perdía. Este módulo lo recupera con regex, sin coste y
sin depender del skill de extracción.

Conservador por diseño: para annual reports (riesgo de umbrella SICAV) solo
acepta el dato si el PDF es del fondo target (match por nombre, como el
clasificador). Para factsheets/KID el "Total size of fund" es del sub-fondo.

CLI:
    python -m tools.aum_pdf_recover LU0168736675           # muestra candidatos
    python -m tools.aum_pdf_recover LU0168736675 --backfill # rellena kpis.aum si vacío
    python -m tools.aum_pdf_recover --backfill-all          # todos los INT sin AUM
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"

# FX → EUR (fallback; mismos valores que intl_extractor_v2._FX_TO_EUR_FALLBACK)
_FX_TO_EUR = {
    "EUR": 1.0, "GBP": 1.18, "USD": 0.92, "CHF": 1.04, "JPY": 0.0061,
    "DKK": 0.134, "SEK": 0.087, "NOK": 0.085, "SGD": 0.68, "AUD": 0.61,
    "CAD": 0.68, "HKD": 0.12,
}
_CCY = "EUR|USD|GBP|CHF|JPY|DKK|SEK|NOK|SGD|AUD|CAD|HKD"

# Etiquetas que preceden al AUM total del (sub)fondo. Orden = prioridad.
_LABELS = [
    ("total size of fund", 95),
    ("total net assets", 90),
    ("net asset value of the fund", 88),
    ("fund size", 80),
    ("assets under management", 78),
    ("total net asset value", 88),
    ("patrimonio del fondo", 85),
    ("patrimonio total", 80),
]

_GENERIC_NAME_TOKENS = frozenset({
    "fund", "funds", "fondo", "fondos", "class", "clase", "sicav", "ucits",
    "global", "value", "equity", "equities", "bond", "bonds", "capital",
    "invest", "investment", "investments", "sub", "growth", "income", "europe",
    "european", "world", "international", "select", "plus", "trust", "asset",
    "management", "opportunities", "premium",
})


def _num(s: str) -> float | None:
    """Parsea un número con separadores de millar/decimales y espacios de OCR.

    Regla robusta para evitar la ambigüedad ',' vs '.': los dígitos tras el
    ÚLTIMO separador deciden. Si son exactamente 3 → separador de millar (entero
    agrupado, p.ej. '7,580,260,321' o '1.234.567'). Si son 1-2 → decimal
    ('280,753,454.11', '1.234.567,89'). Tolera espacios OCR ('2 80,753').
    """
    s = re.sub(r"\s+", "", s.strip())
    if not re.search(r"\d", s):
        return None
    seps = [i for i, ch in enumerate(s) if ch in ".,"]
    if not seps:
        try:
            return float(s)
        except ValueError:
            return None
    last = seps[-1]
    after = s[last + 1:]
    if len(after) == 3:  # separador de millar → entero agrupado
        digits = re.sub(r"[.,]", "", s)
    elif s[last] == ",":  # decimal europeo
        digits = s.replace(".", "").replace(",", ".")
    else:  # decimal anglosajón
        digits = s.replace(",", "")
    try:
        return float(digits)
    except ValueError:
        return None


def _fund_name_matches(head_lc: str, fund_name: str) -> bool:
    tokens = [t for t in re.split(r"[^a-z0-9]+", (fund_name or "").lower())
              if len(t) >= 4 and t not in _GENERIC_NAME_TOKENS]
    return bool(tokens) and any(t in head_lc for t in tokens)


def extract_aum_candidates(isin: str, fund_name: str = "") -> list[dict]:
    """Escanea los PDFs locales del fondo y devuelve candidatos de AUM en M€."""
    from tools.pdf_extractor import extract_page_range
    fund_dir = FUNDS_DIR / isin
    pdfs: list[Path] = []
    for sub in ("raw/discovery", "raw/reports"):
        d = fund_dir / sub
        if d.exists():
            pdfs.extend(sorted(d.glob("*.pdf")))

    candidates: list[dict] = []
    label_re = "|".join(re.escape(l) for l, _ in _LABELS)
    # amt: UN solo número con separadores de millar y decimales opcionales,
    # tolerando un único espacio de OCR tras los primeros dígitos ("2 80,753..").
    # Acotado para NO engullir la fila multi-año del annual report ("X Y Z").
    full_re = re.compile(
        rf"(?P<label>{label_re})\s*[:\-]?\s*(?P<ccy>{_CCY})\s*"
        rf"(?P<amt>\d{{1,3}}(?:\s?\d{{2,3}})?(?:[.,]\d{{3}})*(?:[.,]\d{{2}})?)",
        re.IGNORECASE,
    )
    name_lc = ""
    for pdf in pdfs:
        n = pdf.name.lower()
        is_ar = any(k in n for k in ("annual", "semi", "half-year", "informe", "rapport", "ar-", "bericht"))
        try:
            text = extract_page_range(str(pdf), 0, 25)
        except Exception:
            continue
        head_lc = text[:16000].lower()
        # Annual reports: exigir match por ISIN o nombre (anti umbrella)
        if is_ar and isin.lower() not in head_lc and not _fund_name_matches(head_lc, fund_name):
            continue
        for m in full_re.finditer(text):
            amt = _num(m.group("amt"))
            if amt is None or amt < 1_000_000:  # AUM de fondo siempre > 1M unidades
                continue
            ccy = m.group("ccy").upper()
            meur = round(amt * _FX_TO_EUR.get(ccy, 1.0) / 1e6, 2)
            if not (0.1 <= meur <= 100_000):  # sanidad: 0.1M–100bn
                continue
            label = m.group("label").lower()
            prio = next((p for l, p in _LABELS if l == label), 50)
            candidates.append({
                "value_meur": meur, "currency": ccy, "raw_amount": amt,
                "label": label, "source_pdf": pdf.name, "priority": prio,
                "is_annual_report": is_ar,
            })
    return candidates


def pick_best(candidates: list[dict]) -> dict | None:
    """Elige el candidato más fiable: mayor prioridad de etiqueta, luego mayor valor."""
    if not candidates:
        return None
    # Preferir: mayor prioridad de etiqueta → moneda EUR (sin error de FX) →
    # mayor valor. Cuando el mismo fondo se reporta en varias divisas, gana la
    # cifra en EUR (la base, exacta), no la convertida con FX aproximado.
    return sorted(
        candidates,
        key=lambda c: (c["priority"], c["currency"] == "EUR", c["value_meur"]),
        reverse=True,
    )[0]


def _load(isin: str) -> dict | None:
    p = FUNDS_DIR / isin / "output.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def recover(isin: str, backfill: bool = False) -> dict:
    data = _load(isin)
    fund_name = (data or {}).get("nombre", "") if data else ""
    cands = extract_aum_candidates(isin, fund_name)
    best = pick_best(cands)
    res = {"isin": isin, "candidates": cands, "best": best, "backfilled": False}
    if backfill and best and data is not None:
        cur = (data.get("kpis") or {}).get("aum_actual_meur")
        if cur is None:
            from tools.output_merger import save_output, mark_manual_edit
            data.setdefault("kpis", {})["aum_actual_meur"] = best["value_meur"]
            mark_manual_edit(data, "kpis.aum_actual_meur")
            save_output(isin, data)
            res["backfilled"] = True
    return res


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Recupera AUM desde PDFs locales (determinista)")
    ap.add_argument("isin", nargs="?", help="ISIN (omitir con --backfill-all)")
    ap.add_argument("--backfill", action="store_true", help="Rellena kpis.aum si está vacío")
    ap.add_argument("--backfill-all", action="store_true", help="Todos los INT sin AUM")
    args = ap.parse_args(argv)

    if args.backfill_all:
        targets = []
        for d in sorted(FUNDS_DIR.iterdir()):
            if not d.is_dir() or "." in d.name or d.name.startswith(("ES", "X", "TEST")):
                continue
            data = _load(d.name)
            if data and (data.get("kpis") or {}).get("aum_actual_meur") is None:
                targets.append(d.name)
        print(f"INT sin AUM: {targets}")
        for isin in targets:
            r = recover(isin, backfill=True)
            b = r["best"]
            print(f"  {isin}: {'AUM='+str(b['value_meur'])+'M€ ('+b['source_pdf'][:30]+')' if b else 'sin candidato'}"
                  + ("  [BACKFILLED]" if r["backfilled"] else ""))
        return 0

    if not args.isin:
        ap.error("indica un ISIN o usa --backfill-all")
    r = recover(args.isin.strip().upper(), backfill=args.backfill)
    print(f"=== {args.isin} ===")
    for c in r["candidates"]:
        print(f"  {c['value_meur']:>12,.2f} M€  [{c['label']}] {c['currency']} {c['raw_amount']:,.0f}  ({c['source_pdf'][:40]})")
    b = r["best"]
    print(f"BEST: {b['value_meur']} M€ desde {b['source_pdf']}" if b else "BEST: (ningún candidato)")
    if r["backfilled"]:
        print("→ kpis.aum_actual_meur rellenado.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
