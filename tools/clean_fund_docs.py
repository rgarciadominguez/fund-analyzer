"""clean_fund_docs.py — Limpia el manifiesto de documentos de un fondo (contaminación + duplicados).

Rafa (2026-09-05): "hasta que TODOS los docs usados para el análisis no estén bien representados y
accesibles no pares". Los docs se archivan a Storage + se listan en dos sitios que hay que dejar
IGUALES (CLAUDE.md §0.9 alineación):
  1. output.json → analyst_synthesis.documentos.informes_pdf  (lo pinta el dashboard)
  2. fund_groups.portfolio_metrics_jsonb.documentos           (lo empuja el portal vía sync-meta)

Limpieza aplicada (verificada abriendo los PDFs):
  - Quita basenames de una BLOCKLIST por ISIN (contaminación confirmada leyendo el PDF).
  - Dedup por URL.
  - Dedup de contenido idéntico por (tipo + basename distinto pero MISMO fichero): se pasa la lista
    DROP_BASENAMES con el duplicado a eliminar (verificado idéntico por tamaño/bytes).

Uso: python -m tools.clean_fund_docs IE000Z9YV312
     (la config de qué quitar está en BLOCKLIST abajo, por ISIN de grupo)
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Basenames de fichero a ELIMINAR por ISIN (o por cualquier clase del grupo). Verificado leyendo
# el PDF: qué fondo es realmente. Añadir aquí conforme se detecte contaminación/duplicado.
DROP_BASENAMES: dict[str, set[str]] = {
    # MontLake Alpha Fixed Income (IE000Z9YV312 / IE000RDB0I49):
    #   2004_annual_report.pdf  = "NATIXIS BANQUES POPULAIRES 2004" (contaminación, otro emisor)
    #   semi_annual_finect.pdf  = byte-idéntico a semi_annual_report_2025.pdf (duplicado)
    "IE000Z9YV312": {"2004_annual_report.pdf", "semi_annual_finect.pdf"},
    "IE000RDB0I49": {"2004_annual_report.pdf", "semi_annual_finect.pdf"},
}


def _basename(url_or_name: str) -> str:
    s = (url_or_name or "").split("?")[0].rstrip("/")
    return s.split("/")[-1]


def _clean_list(items: list, drop: set[str]) -> list:
    seen_url = set()
    out = []
    for it in items:
        if not isinstance(it, dict):
            out.append(it)
            continue
        url = it.get("url") or ""
        bn = _basename(url) or _basename(it.get("nombre") or "")
        if bn in drop:
            continue
        key = url or it.get("nombre")
        if key and key in seen_url:
            continue
        if key:
            seen_url.add(key)
        out.append(it)
    return out


def clean_isin(isin: str) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    drop = DROP_BASENAMES.get(isin, set())
    report = {"isin": isin, "drop": sorted(drop), "output_json": 0, "manifest": 0}

    # 1) output.json local
    p = ROOT / "data" / "funds" / isin / "output.json"
    if p.exists():
        d = json.loads(p.read_text(encoding="utf-8"))
        docs = (d.get("analyst_synthesis") or {}).get("documentos") or {}
        pdfs = docs.get("informes_pdf") or []
        before = len(pdfs)
        docs["informes_pdf"] = _clean_list(pdfs, drop)
        report["output_json"] = before - len(docs["informes_pdf"])
        p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

    # 2) fund_groups manifest (portal)
    from tools.supabase_client import get_client
    c = get_client()
    f = c.table("funds").select("fund_group_id").eq("isin", isin.upper()).execute().data
    if f:
        gid = f[0]["fund_group_id"]
        g = c.table("fund_groups").select("portfolio_metrics_jsonb").eq("fund_group_id", gid).execute().data
        if g:
            pm = g[0].get("portfolio_metrics_jsonb") or {}
            man = pm.get("documentos") or []
            before = len(man)
            pm["documentos"] = _clean_list(man, drop)
            report["manifest"] = before - len(pm["documentos"])
            if report["manifest"]:
                c.table("fund_groups").update({"portfolio_metrics_jsonb": pm}).eq("fund_group_id", gid).execute()
    return report


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    for isin in (argv or sys.argv[1:]) or []:
        r = clean_isin(isin.upper())
        print(json.dumps(r, ensure_ascii=False))


if __name__ == "__main__":
    main()
