"""
archive_docs.py — Archiva en Supabase Storage SOLO los documentos CLAVE de un fondo
(no los 3,75GB de todo) y publica un manifiesto para que el portal los liste.

Decisión (calidad-coste): Supabase Storage (el portal ya lee de Supabase; URLs públicas =
enlaces directos, cero endpoint nuevo). Guardamos el HISTÓRICO DOCUMENTAL COMPLETO de los
docs consultables (todos los años de AR/SAR + cartas) para poder hacer preguntas sobre el
fondo con el máximo histórico; de los no-consultables (KID/folleto/factsheet) solo el último.

Qué archiva (por fondo): TODOS los años de annual_report y semi_annual_report, último KID,
prospectus, último factsheet, y las 12 cartas más recientes del gestor. Content-addressed
(dedup: un AR de paraguas se sube 1 vez). NADA de fragmentos web ni JSONs de extracción.

Manifiesto → fund_groups.portfolio_metrics_jsonb.documentos = [
  {tipo, fecha, nombre, url, url_original} , ... ]  (el portal lo lee y lista los docs).

CLI:
    python -m tools.archive_docs --isin IE00BF5GGB04
    python -m tools.archive_docs --all
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BUCKET = "funds-data"

# AR/SAR: guardamos TODOS los años (histórico documental completo para consultar el fondo).
_KEEP_ALL = ("annual_report", "semi_annual_report")
# KID/folleto/factsheet: solo el más reciente (no aportan histórico consultable).
_KEEP_LATEST = ("kid", "prospectus", "factsheet")
_N_LETTERS = 12    # cartas del gestor recientes a guardar (histórico de comentarios)


def _slug(name: str) -> str:
    """Nombre de fichero seguro para Storage (evita % º espacios → HTTP 400)."""
    name = (name or "doc.pdf").encode("ascii", "ignore").decode()
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return re.sub(r"_+", "_", name).strip("_") or "doc.pdf"


def _periodo_key(p) -> str:
    return str(p or "")[:10]


def _collect(isin: str) -> list[dict]:
    """Candidatos {doc_type, periodo, fecha, local_path, url} desde discovery (INT) y
    raw/reports (ES/CNMV)."""
    fd = ROOT / "data" / "funds" / isin
    cands = []
    dd = fd / "intl_discovery_data.json"
    if dd.exists():
        try:
            for d in json.loads(dd.read_text(encoding="utf-8")).get("documents", []):
                lp = d.get("local_path")
                if lp and Path(lp).exists() and d.get("doc_type"):
                    cands.append({"doc_type": d["doc_type"], "periodo": d.get("periodo"),
                                  "fecha": d.get("fecha_publicacion") or _periodo_key(d.get("periodo")),
                                  "local_path": lp, "url": d.get("url") or ""})
        except Exception:
            pass
    # INT: AR/SAR multi-año que fetch_annual_report/Finect dejan en raw/discovery con el
    # año en el nombre (annual_report_2023.pdf, semi_annual_report_2024.pdf). El discovery_data
    # no siempre los lista → escanear el directorio directamente para NO perder años.
    disc = fd / "raw" / "discovery"
    if disc.exists():
        _seen = set()   # solo dedup dentro de este scan (no contra discovery_data, que
                        # puede tener años mal-clasificados que bloquearían el AR limpio)
        for f in sorted(disc.glob("*.pdf")):
            low = f.name.lower()
            if "semi_annual" in low or "semiannual" in low:
                dt = "semi_annual_report"
            elif "annual_report" in low or "annualreport" in low:
                dt = "annual_report"
            else:
                continue
            m = re.search(r"(19|20)\d{2}", f.name)
            per = m.group(0) if m else "latest"
            if (dt, _periodo_key(per)) in _seen:
                continue
            cands.append({"doc_type": dt, "periodo": per, "fecha": per,
                          "local_path": str(f), "url": ""})
            _seen.add((dt, _periodo_key(per)))
    # ES/CNMV: raw/reports (semestrales). H2(dic)=anual, H1(jun)=semianual.
    rep = fd / "raw" / "reports"
    if rep.exists():
        from tools.publication_calendar import _date_from_filename
        for f in sorted(rep.glob("*.pdf")):
            dt = _date_from_filename(f.name)
            if not dt:
                continue
            cands.append({"doc_type": "annual_report" if dt.month == 12 else "semi_annual_report",
                          "periodo": dt.isoformat(), "fecha": dt.isoformat(),
                          "local_path": str(f), "url": ""})
    return cands


# Tokens en el nombre que delatan que un doc etiquetado como AR/SAR NO lo es (el discovery
# a veces mal-clasifica factsheets/anexos/KIDs como annual_report).
_AR_NOISE = ("fact", "annex", "pcdp", "kid", "kiid", "prospect", "folleto", "prof",
             "monthly", "commentary", "outlook", "priip", "-dic-", "_dic_")


def _is_real_report(c: dict) -> bool:
    """El candidato AR/SAR tiene pinta de informe real (por nombre de fichero o ruta ES)."""
    fn = str(c.get("local_path") or "").replace("\\", "/").split("/")[-1].lower()
    if any(tok in fn for tok in _AR_NOISE):
        return False
    if "/raw/reports/" in str(c.get("local_path") or "").replace("\\", "/"):
        return True   # ES CNMV semestrales
    good = ("annual_report", "annualreport", "semi_annual", "semiannual",
            "informe_anual", "informe-anual", "informe_semestral", "anr-", "anr_",
            "comptes-annuels", "-ar-", "_ar_", "-sar-", "_sar_")
    return any(g in fn for g in good)


def _select(cands: list[dict]) -> list[dict]:
    """Selecciona los docs a archivar: TODOS los años de AR/SAR (histórico documental) +
    último KID/folleto/factsheet + N cartas recientes. Dedup por (tipo, periodo)."""
    by = {}
    for c in cands:
        by.setdefault(c["doc_type"], []).append(c)
    sel = []
    # AR/SAR: todos los años (dedup por periodo, uno por año/periodo). Filtra mal-clasificados.
    for dt in _KEEP_ALL:
        seen = {}
        for c in sorted([c for c in by.get(dt, []) if _is_real_report(c)],
                        key=lambda c: _periodo_key(c["periodo"])):
            seen[_periodo_key(c["periodo"])] = c   # el último con ese periodo gana
        sel.extend(seen.values())
    # KID/folleto/factsheet: solo el más reciente
    for dt in _KEEP_LATEST:
        lst = by.get(dt)
        if lst:
            sel.append(max(lst, key=lambda c: _periodo_key(c["periodo"])))
    letters = sorted(by.get("quarterly_letter", []), key=lambda c: _periodo_key(c["periodo"]), reverse=True)
    sel.extend(letters[:_N_LETTERS])
    return sel


def archive(isin: str, client=None, log=print) -> list[dict]:
    """Sube los docs clave y devuelve el manifiesto. Escribe el manifiesto en
    fund_groups.portfolio_metrics_jsonb.documentos si hay client."""
    from tools.sync_to_supabase import _upload_file_to_storage
    isin = isin.upper()
    sel = _select(_collect(isin))
    # GUARD anti re-contaminación: nunca archivar ficheros en la blocklist del fondo (contaminación
    # verificada, p.ej. MontLake 2004_annual_report.pdf = Natixis). Sin esto, un re-run los re-sube.
    try:
        from tools.clean_fund_docs import DROP_BASENAMES
        _blocked = DROP_BASENAMES.get(isin, set())
        if _blocked:
            sel = [c for c in sel if Path(c["local_path"]).name not in _blocked]
    except Exception:
        pass
    base = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
    manifest = []
    for c in sel:
        f = Path(c["local_path"])
        if not f.exists() or f.stat().st_size == 0:
            continue
        dest = f"docs/{isin}/{c['doc_type']}/{_slug(f.name)}"
        if _upload_file_to_storage(client, BUCKET, dest, f, "application/pdf"):
            manifest.append({
                "tipo": c["doc_type"],
                "periodo": _periodo_key(c["periodo"]),        # periodo que cubre el doc (fiable)
                "fecha_publicacion": c.get("fecha") or None,  # cuándo se publicó (si se conoce)
                "nombre": f.name,
                "url": f"{base}/storage/v1/object/public/{BUCKET}/{dest}",
                "url_original": c["url"],
            })
    if client is not None and manifest:
        try:
            g = client.table("funds").select("fund_group_id").eq("isin", isin).execute().data
            fgid = g[0]["fund_group_id"] if g else None
            if fgid:
                cur = client.table("fund_groups").select("portfolio_metrics_jsonb").eq(
                    "fund_group_id", fgid).execute().data
                pm = (cur[0].get("portfolio_metrics_jsonb") if cur else None) or {}
                if not isinstance(pm, dict):
                    pm = {}
                pm["documentos"] = manifest
                client.table("fund_groups").update({"portfolio_metrics_jsonb": pm}).eq(
                    "fund_group_id", fgid).execute()
        except Exception as e:
            log(f"[DOCS] manifiesto no escrito: {str(e)[:70]}")
    # Volcar el manifiesto (con URLs de Storage) a output.json → analyst_synthesis.documentos
    # para que el DASHBOARD y el PORTAL listen los AR/SAR/cartas archivados (antes informes_pdf
    # quedaba vacío y no se veían los docs descargados).
    try:
        _merge_into_output_documentos(isin, manifest, log=log)
    except Exception as e:
        log(f"[DOCS] no volcado a output.json: {str(e)[:70]}")
    log(f"[DOCS] {isin}: {len(manifest)} docs clave archivados")
    return manifest


_TIPO_LABEL_ES = {"annual_report": "Informe anual", "semi_annual_report": "Informe semestral",
                  "kid": "KID", "prospectus": "Folleto", "factsheet": "Factsheet"}


def _merge_into_output_documentos(isin: str, manifest: list[dict], log=print) -> bool:
    """Escribe los docs archivados (AR/SAR/KID/folleto → informes_pdf; cartas → cartas_urls) en
    output.json → analyst_synthesis.documentos, con las URLs de Storage. Preserva lo que ya haya
    (fuentes externas del analyst). Cada informe: {tipo, periodo, nombre, url, archivo}."""
    p = ROOT / "data" / "funds" / isin.upper() / "output.json"
    if not p.exists() or not manifest:
        return False
    d = json.loads(p.read_text(encoding="utf-8"))
    syn = d.setdefault("analyst_synthesis", {})
    docs = syn.setdefault("documentos", {})
    if not isinstance(docs, dict):
        docs = {}; syn["documentos"] = docs
    informes, cartas = [], list(docs.get("cartas_urls") or [])
    # AR/SAR más nuevo primero
    for m in sorted(manifest, key=lambda x: str(x.get("periodo") or ""), reverse=True):
        tipo = m.get("tipo")
        if tipo in ("annual_report", "semi_annual_report", "kid", "prospectus", "factsheet"):
            etq = _TIPO_LABEL_ES.get(tipo, "Documento")
            per = str(m.get("periodo") or "").strip()
            per = per[:4] if per and per[:4].isdigit() else ""
            informes.append({"tipo": tipo, "periodo": per,
                             "nombre": f"{etq}{(' ' + per) if per else ''}",
                             "url": m.get("url"), "archivo": m.get("nombre")})
        elif tipo in ("carta_gestor", "quarterly_letter") and m.get("url") and m["url"] not in cartas:
            cartas.append(m["url"])
    if informes:
        docs["informes_pdf"] = informes
    docs["cartas_urls"] = cartas
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    log(f"[DOCS] output.json.documentos: {len(informes)} informes + {len(cartas)} cartas")
    return True


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin")
    ap.add_argument("--all", action="store_true")
    a = ap.parse_args()
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    client = get_client()
    if a.isin:
        isins = [a.isin]
    else:
        isins = [Path(fp).parent.name for fp in glob.glob(str(ROOT / "data" / "funds" / "*" / "output.json"))
                 if "." not in Path(fp).parent.name]
    total = 0
    for isin in isins:
        try:
            m = archive(isin, client=client)
            total += len(m)
        except Exception as e:
            print(f"  {isin}: ERR {type(e).__name__} {str(e)[:60]}")
    print(f"DONE: {total} docs clave archivados en {len(isins)} fondos")


if __name__ == "__main__":
    main()
