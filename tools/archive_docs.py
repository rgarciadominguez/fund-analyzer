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


def _year_from_pdf(path) -> str:
    """Extrae el AÑO fiscal que cubre un AR/SAR de su primera página (para ficheros sin año en el
    nombre: annual_report_latest.pdf, semi_annual_finect.pdf). Busca 'year/period ended <fecha> YYYY',
    '31 december YYYY', '30 june YYYY', 'au 31 décembre YYYY'. '' si no lo encuentra."""
    try:
        import pdfplumber
        with pdfplumber.open(str(path)) as p:
            txt = ""
            for pg in p.pages[:3]:
                txt += " " + (pg.extract_text() or "")
        low = " ".join(txt.split()).lower()
        pats = [
            r"(?:year|period|financial year|exercice|période).{0,40}?(?:ended|closed|clos).{0,20}?((?:19|20)\d{2})",
            r"31\s+december\s+((?:19|20)\d{2})",
            r"30\s+june\s+((?:19|20)\d{2})",
            r"31\s+d[ée]cembre\s+((?:19|20)\d{2})",
            r"30\s+juin\s+((?:19|20)\d{2})",
        ]
        for pat in pats:
            m = re.search(pat, low)
            if m:
                return m.group(1)
    except Exception:
        pass
    return ""


def _resolver(lp, fd: Path) -> Path | None:
    """La ruta absoluta guardada es del equipo que descargó (servidor C:\\Users\\Usuario\\…); en otro equipo se
    busca el mismo fichero por nombre dentro de las carpetas del fondo (OneDrive sincroniza la carpeta)."""
    if not lp:
        return None
    p = Path(lp)
    if p.exists():
        return p
    for sub in ("discovery", "letters", "reports", "aportados", "manual"):
        q = fd / "raw" / sub / Path(str(lp).replace("\\", "/")).name
        if q.exists():
            return q
    return None


def _collect(isin: str) -> list[dict]:
    """Candidatos {doc_type, periodo, fecha, local_path, url} desde discovery (INT), raw/letters (cartas),
    KID/folleto de raw/discovery y raw/reports (ES/CNMV)."""
    fd = ROOT / "data" / "funds" / isin
    cands = []
    dd = fd / "intl_discovery_data.json"
    if dd.exists():
        try:
            for d in json.loads(dd.read_text(encoding="utf-8")).get("documents", []):
                lp = _resolver(d.get("local_path"), fd)
                # solo PDFs: un artículo web o la transcripción de un vídeo (.txt) se enlaza en su web original
                if lp and d.get("doc_type") and lp.suffix.lower() == ".pdf":
                    cands.append({"doc_type": d["doc_type"], "periodo": d.get("periodo"),
                                  "fecha": d.get("fecha_publicacion") or _periodo_key(d.get("periodo")),
                                  "local_path": str(lp), "url": d.get("url") or ""})
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
            if any(k in low for k in ("semi_annual", "semiannual", "sar-", "sar_", "interim", "semestr")):
                dt = "semi_annual_report"
            elif any(k in low for k in ("annual_report", "annualreport", "anr-", "anr_",
                                        "comptes-annuels", "-ar-", "_ar_", "rechenschaft")):
                dt = "annual_report"
            else:
                continue
            m = re.search(r"(19|20)\d{2}", f.name)
            # sin año en el nombre (annual_report_latest.pdf, semi_annual_finect.pdf) → sácalo del PDF
            per = m.group(0) if m else (_year_from_pdf(f) or "latest")
            if (dt, _periodo_key(per)) in _seen:
                continue
            cands.append({"doc_type": dt, "periodo": per, "fecha": per,
                          "local_path": str(f), "url": ""})
            _seen.add((dt, _periodo_key(per)))
    # KID / folleto descargados (1-oct-2026: no se archivaban si el discovery_data traía otra ruta)
    if disc.exists():
        for f in sorted(disc.glob("*.pdf")):
            low = f.name.lower()
            dt = "kid" if any(k in low for k in ("kid", "kiid", "dici", "priip")) else \
                 "prospectus" if any(k in low for k in ("prospect", "folleto")) else None
            if dt and not any(Path(c["local_path"]).name == f.name for c in cands):
                m = re.search(r"(20\d{2})(\d{2})?(\d{2})?", f.name)
                cands.append({"doc_type": dt, "periodo": m.group(1) if m else "latest", "fecha": None,
                              "local_path": str(f), "url": ""})
    # Cartas del gestor descargadas (raw/letters/*.pdf), con su URL original si consta en letters_data
    let = fd / "raw" / "letters"
    if let.exists():
        _urls = {}
        try:
            for c in json.loads((fd / "letters_data.json").read_text(encoding="utf-8")).get("cartas") or []:
                if isinstance(c, dict) and c.get("archivo"):
                    _urls[Path(str(c["archivo"]).replace("\\", "/")).name] = c.get("url") or c.get("url_fuente") or ""
        except Exception:
            pass
        for f in sorted(let.glob("*.pdf")):
            if any(Path(c["local_path"]).name == f.name for c in cands):
                continue
            m = re.search(r"(20\d{2}(?:-(?:Q[1-4]|H[12]|\d{2}))?)", f.name)
            cands.append({"doc_type": "quarterly_letter", "periodo": m.group(1) if m else "latest",
                          "fecha": m.group(1) if m else None, "local_path": str(f), "url": _urls.get(f.name, "")})
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
    # APORTADOS (material curado que sube Rafa): se archivan SIEMPRE y se listan en Documentos.
    # Antes no se miraba esta carpeta → el doc con el que se mejoró el análisis no era consultable.
    apo = fd / "raw" / "aportados"
    if apo.exists():
        for f in sorted(apo.glob("*.pdf")):
            m = re.search(r"(19|20)\d{2}", f.name)
            per = m.group(0) if m else (_year_from_pdf(f) or "latest")
            cands.append({"doc_type": "aportado", "periodo": per, "fecha": per,
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
            "comptes-annuels", "-ar-", "_ar_", "-sar-", "_sar_", "sar-", "sar_",
            "rechenschaft", "interim", "semestr")
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
    # Aportados: todos (son pocos, curados y son la fuente de la mejora del análisis).
    sel.extend(by.get("aportado", []))
    return sel


def _archivar_en_r2(fichero: Path, isin: str) -> str | None:
    """Archiva un documento y devuelve su DIRECCIÓN PÚBLICA, o None si no se pudo.

    POR QUÉ ASÍ (25-sep-2026). Antes esto subía cada documento a Supabase con una ruta por fondo
    (`docs/<ISIN>/<tipo>/<nombre>`). Como el mismo informe lo comparten hasta 6 fondos de una
    misma gestora, el archivo acabó con **el 54% de duplicados**: 2,34 GB de los que 1,27 GB eran
    copias del mismo fichero. Supabase (1 GB en su plan gratuito) avisó de corte.

    Ahora se archiva **por contenido**: la clave es el sha256, así que el informe paraguas se
    guarda UNA vez por muchos fondos que lo compartan. Es exactamente lo que `doc_archive.py`
    dejó diseñado en junio y nunca llegó a aplicarse en esta vía.

    Dos destinos, a propósito:
      · el bucket PRIVADO es el archivo de verdad (y donde vive todo lo sensible);
      · el bucket PÚBLICO es solo un espejo de los documentos de fondos, porque el dashboard
        los enlaza y esos enlaces tienen que abrirse sin credenciales. Se copia de uno a otro
        DENTRO de Cloudflare: no se vuelve a subir el fichero.
    """
    from tools.doc_archive import archive_file
    from tools.r2_client import R2

    sha = archive_file(fichero, isin, fichero.name)
    if not sha:
        return None
    r2 = R2()
    if not r2.bucket_publico or not r2.url_publica:
        return None
    pub = R2(bucket=r2.bucket_publico)
    destino = f"docs/{sha}.pdf"
    if pub.existe(destino) is None:
        pub.copiar_desde(r2.bucket, f"fondos-docs/{sha}.pdf", destino)
    return f"{r2.url_publica}/{destino}"


def archive(isin: str, client=None, log=print) -> list[dict]:
    """Sube los docs clave y devuelve el manifiesto. Escribe el manifiesto en
    fund_groups.portfolio_metrics_jsonb.documentos si hay client."""
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
        url = _archivar_en_r2(f, isin)
        if url:
            manifest.append({
                "tipo": c["doc_type"],
                "periodo": _periodo_key(c["periodo"]),        # periodo que cubre el doc (fiable)
                "fecha_publicacion": c.get("fecha") or None,  # cuándo se publicó (si se conoce)
                "nombre": f.name,
                "url": url,
                "url_original": c["url"],
            })
        else:
            log(f"[DOCS] no se pudo archivar {f.name[:50]}")
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
    try:   # manifiesto local (1-oct-2026): enlaza cada original con nuestra copia (lo usa tools.documentos_fondo)
        (ROOT / "data" / "funds" / isin / "documentos_archivados.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        pass
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
        if tipo in ("annual_report", "semi_annual_report", "kid", "prospectus", "factsheet", "aportado"):
            etq = _TIPO_LABEL_ES.get(tipo, "Documento aportado" if tipo == "aportado" else "Documento")
            per = str(m.get("periodo") or "").strip()
            per = per[:4] if per and per[:4].isdigit() else ""
            informes.append({"tipo": tipo, "periodo": per,
                             "nombre": f"{etq}{(' ' + per) if per else ''}",
                             "url": m.get("url"), "archivo": m.get("nombre")})
        elif tipo in ("carta_gestor", "quarterly_letter") and m.get("url") and m["url"] not in cartas:
            cartas.append(m["url"])
    # Cartas EXTRAÍDAS por el letters agent (letters_data.json): tienen url_fuente pero no siempre
    # PDF local/archivado → listar la URL para que sean consultables. Bug MontLake: el analyst
    # citaba una carta (jun-2026) que no aparecía en Documentos.
    lp = p.with_name("letters_data.json")
    if lp.exists():
        try:
            for c in (json.loads(lp.read_text(encoding="utf-8")).get("cartas") or []):
                u = (c.get("url_fuente") or "").strip() if isinstance(c, dict) else ""
                if u and u.startswith("http") and u not in cartas:
                    cartas.append(u)
        except Exception:
            pass
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
