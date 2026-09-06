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

# Substrings de URL a ELIMINAR de discovery_kb/known_urls y demás JSON (la fuente de RE-contaminación:
# el discovery guarda la URL ORIGINAL —p.ej. web.archive.org/.../natixis.com/...— y en cada re-run la
# re-descarga). Sin esto, limpiar por basename no evita que discovery la vuelva a traer.
DROP_URL_SUBSTRINGS: dict[str, set[str]] = {
    "IE000Z9YV312": {"natixis.com", "2004_annual_report"},
    "IE000RDB0I49": {"natixis.com", "2004_annual_report"},
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


def _purge_everywhere(isin: str, drop: set[str], report: dict) -> None:
    """Elimina los ficheros de la blocklist de TODAS partes para que un re-run no los re-archive:
    (a) PDFs físicos en raw/*, (b) referencias en los JSON de discovery/sources, (c) Storage Supabase.
    Es lo que faltaba: limpiar solo output.json no evita que discovery los vuelva a meter."""
    fdir = ROOT / "data" / "funds" / isin
    # (a) ficheros físicos
    for sub in ("discovery", "reports", "letters", "manual", "xml"):
        d = fdir / "raw" / sub
        if d.exists():
            for f in d.iterdir():
                if f.name in drop:
                    try:
                        f.unlink(); report.setdefault("files_deleted", []).append(f.name)
                    except Exception:
                        pass
    # (b) referencias en JSON de discovery/sources. Incluye known_urls (dict url) de discovery_kb:
    # ahí es donde vive la URL ORIGINAL de Wayback/natixis que re-contamina en cada run.
    import json as _j
    urlsubs = DROP_URL_SUBSTRINGS.get(isin, set())
    needles = set(drop) | urlsubs
    for jn in ("discovery_kb.json", "intl_discovery_data.json", "sources.json", "intl_data.json",
               "bundle/sources.json"):
        jp = fdir / jn
        if not jp.exists():
            continue
        try:
            raw = jp.read_text(encoding="utf-8")
            if not any(b in raw for b in needles):
                continue
            data = _j.loads(raw)
            n = _strip_refs(data, drop, urlsubs)
            if n:
                jp.write_text(_j.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                report.setdefault("json_refs_removed", {})[jn] = n
        except Exception:
            pass
    # (c) Storage Supabase (borra el objeto para que la URL deje de servir el doc equivocado)
    try:
        from tools.supabase_client import get_client
        c = get_client()
        for bn in drop:
            for tipo in ("annual_report", "semi_annual_report", "kid", "prospectus", "factsheet"):
                path = f"docs/{isin}/{tipo}/{bn}"
                try:
                    c.storage.from_("funds-data").remove([path])
                    report.setdefault("storage_deleted", []).append(path)
                except Exception:
                    pass
    except Exception:
        pass


def _hit(s: str, drop: set[str], urlsubs: set[str]) -> bool:
    s = str(s or "")
    if any(b and (s.endswith(b) or b in _basename(s)) for b in drop):
        return True
    if any(u and u in s for u in urlsubs):
        return True
    return False


def _strip_refs(obj, drop: set[str], urlsubs: set[str] | None = None):
    """Elimina de listas los items (str/dict) cuyo archivo/nombre/url matchee la blocklist, y de dicts
    los pares clave→valor cuyo valor (string url) matchee (caso known_urls de discovery_kb)."""
    urlsubs = urlsubs or set()
    removed = 0
    if isinstance(obj, dict):
        for k, v in list(obj.items()):
            if isinstance(v, str) and _hit(v, drop, urlsubs):
                del obj[k]; removed += 1; continue
            if isinstance(v, list):
                new = []
                for it in v:
                    s = it if isinstance(it, str) else (
                        (it.get("archivo") or it.get("nombre") or it.get("url") or "") if isinstance(it, dict) else "")
                    if _hit(s, drop, urlsubs):
                        removed += 1; continue
                    new.append(it)
                obj[k] = new
                for it in new:
                    removed += _strip_refs(it, drop, urlsubs)
            else:
                removed += _strip_refs(v, drop, urlsubs)
    elif isinstance(obj, list):
        for it in obj:
            removed += _strip_refs(it, drop, urlsubs)
    return removed


def clean_isin(isin: str) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    drop = DROP_BASENAMES.get(isin, set())
    report = {"isin": isin, "drop": sorted(drop), "output_json": 0, "manifest": 0}
    if drop:
        _purge_everywhere(isin, drop, report)

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
