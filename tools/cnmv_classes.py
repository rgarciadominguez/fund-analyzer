"""Listado oficial de clases de un fondo ES desde el registro CNMV (por NIF).

Fuente autoritativa y gratuita (sin login). Cruza dos vistas del portal CNMV:
  - vista=3  "Clases de participaciones": Nº, Denominación, Fecha alta, ISIN
  - vista=34 "Comparativa comisiones":    Denominación → Gestión, Depósito, Mínimo

Devuelve una lista compatible con `reconcile_fund_groups.populate_fund_classes`:
  [{isin, nombre_clase, divisa, comision_gestion_pct, fecha_inicio, min_suscripcion}]

Uso:
    python -m tools.cnmv_classes V87171005          # imprime clases de un NIF
    python -m tools.cnmv_classes --all              # DRY-RUN: todos los ES analizados
    python -m tools.cnmv_classes --all --apply      # inserta clases en Supabase
"""
from __future__ import annotations

import re
import sys

import httpx
from bs4 import BeautifulSoup

_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
_BASE = "https://www.cnmv.es/Portal/consultas/iic/fondo?nif={nif}&vista={v}&com=0"
_ISIN = re.compile(r"\bES[0-9]{10}\b")


def _get(nif: str, vista: int) -> str:
    r = httpx.get(_BASE.format(nif=nif, v=vista), headers=_UA, timeout=30, follow_redirects=True)
    if r.status_code == 400:
        return ""   # esa vista no existe para este NIF (fondo monoclase)
    r.raise_for_status()
    return r.text


def _pct(s: str):
    m = re.search(r"(\d+[.,]?\d*)\s*%", s or "")
    return float(m.group(1).replace(",", ".")) if m else None


def _year(s: str):
    m = re.search(r"(19|20)\d{2}", s or "")
    return f"{m.group(0)}-01-01" if m else None


def fetch_classes(nif: str) -> list[dict]:
    """Lista de clases del fondo (por NIF) con ISIN, denominación, año alta y
    comisión de gestión. [] si el fondo no expone clases en vista=3."""
    nif = (nif or "").strip().upper()
    if not nif:
        return []
    # vista=3 → ISIN + denominación + fecha alta
    soup = BeautifulSoup(_get(nif, 3), "html.parser")
    base = {}   # denominación -> {isin, fecha}
    for tbl in soup.find_all("table"):
        if not _ISIN.search(tbl.get_text(" ", strip=True)):
            continue
        for tr in tbl.find_all("tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            isins = [c for c in tds if _ISIN.fullmatch(c)]
            if not isins:
                continue
            denom = next((c for c in tds if c and not _ISIN.fullmatch(c)
                          and not c.isdigit() and "/" not in c), "")
            fecha = next((c for c in tds if re.search(r"\d{2}/\d{2}/\d{4}", c)), "")
            base[denom.upper()] = {"isin": isins[0], "fecha": fecha, "denom": denom}
        break
    if not base:
        return []
    # vista=34 → comisión de gestión + mínimo por denominación
    com = {}
    try:
        soup2 = BeautifulSoup(_get(nif, 34), "html.parser")
        for tbl in soup2.find_all("table"):
            if "CLASE" not in tbl.get_text(" ", strip=True).upper():
                continue
            for tr in tbl.find_all("tr"):
                tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if not tds or not tds[0]:
                    continue
                denom = tds[0].upper()
                gestion = _pct(tds[1]) if len(tds) > 1 else None
                minimo = tds[6] if len(tds) > 6 else None
                com[denom] = {"gestion": gestion, "minimo": minimo}
            break
    except Exception:
        pass
    out = []
    for denom, info in base.items():
        c = com.get(denom, {})
        out.append({
            "isin": info["isin"],
            "nombre_clase": info["denom"] or denom.title(),
            "divisa": "EUR",
            "comision_gestion_pct": c.get("gestion"),
            "fecha_inicio": _year(info["fecha"]),
            "min_suscripcion": c.get("minimo"),
        })
    return out


def _es_funds_with_nif():
    """{isin: nif} de los fondos ES analizados localmente (de cnmv_data.json)."""
    import glob
    import json
    import os
    out = {}
    for f in glob.glob(os.path.join("data", "funds", "ES*", "cnmv_data.json")):
        d = os.path.basename(os.path.dirname(f))
        if "." in d:
            continue
        try:
            nif = (json.load(open(f, encoding="utf-8")).get("nif") or "").strip()
        except Exception:
            nif = ""
        if nif:
            out[d] = nif
    return out


def main():
    args = sys.argv[1:]
    if args and re.fullmatch(r"[A-Z0-9]{8,10}", args[0].upper()):
        for c in fetch_classes(args[0]):
            print(c)
        return
    apply = "--apply" in args
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    from tools.supabase_client import get_client
    from tools.reconcile_fund_groups import populate_fund_classes
    client = get_client()
    funds = _es_funds_with_nif()
    print(f"{'APLICAR' if apply else 'DRY-RUN'} — {len(funds)} fondos ES con NIF\n")
    total = 0
    for isin, nif in sorted(funds.items()):
        try:
            clases = fetch_classes(nif)
        except Exception as e:
            print(f"{isin} (NIF {nif}): ERROR {str(e)[:80]}")
            continue
        if len(clases) <= 1:
            print(f"{isin} (NIF {nif}): {len(clases)} clase(s) — sin hermanas, salto")
            continue
        print(f"{isin} (NIF {nif}): {len(clases)} clases")
        for c in clases:
            print(f"    {c['isin']}  {c['nombre_clase']}  com={c['comision_gestion_pct']}  "
                  f"alta={c['fecha_inicio']}")
        total += populate_fund_classes(client, isin, clases, apply=apply)
        print()
    print(f"{'APLICADO' if apply else 'DRY-RUN'}: {total} clases {'insertadas' if apply else 'a insertar'}.")


if __name__ == "__main__":
    main()
