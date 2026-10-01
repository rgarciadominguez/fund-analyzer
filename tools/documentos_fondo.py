"""documentos_fondo.py — Lista de documentos del fondo para la pestaña "Documentos" (Rafa 1-oct-2026).

BNY Short-Dated HY salió sin ningún documento aunque el análisis había usado 7 annual reports, 6 semestrales y
4 cartas: la pestaña dependía de una lista que rellenaba el analista (vacía en fondos internacionales). Aquí se
construye de forma determinista desde lo que el sistema ha usado de verdad, siempre con el ENLACE ORIGINAL
(funciona desde el portal; las rutas locales no):
  · data/known_annual_reports.json  → informes anuales y semestrales por año
  · intl_discovery_data.json        → documentos del discovery (AR/SAR/folleto/KID/ficha/cartas)
  · letters_data.json + data/known_manager_letters.json → cartas del gestor
  · readings_data.json              → análisis externos
No incluye el material aportado por Rafa (el dashboard es público).
Salida con la forma que lee generate_dashboard.build_tab_documentos:
  {informes_pdf: [{nombre, url, tipo}], cartas_urls: [url], fuentes_externas_urls: [url], xmls_cnmv: [], total_fuentes}
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_NOMBRE = {"annual_report": "Informe anual", "semi_annual_report": "Informe semestral", "prospectus": "Folleto",
           "kid": "KID", "factsheet": "Ficha comercial"}
_TIPO = {"annual_report": "annual_report", "semi_annual_report": "semi_annual_report",
         "semiannual_report": "semi_annual_report", "prospectus": "prospectus", "folleto": "prospectus",
         "kid": "kid", "kiid": "kid", "factsheet": "factsheet"}
_CARTA = {"quarterly_letter", "letter", "carta", "carta_gestor", "monthly_commentary", "comentario"}


def _archivados(isin: str, fd: Path) -> dict:
    """{nombre_de_fichero: url pública de NUESTRA copia en R2} para los ficheros del fondo ya archivados."""
    try:
        from tools.doc_archive import _load_index
        from tools.r2_client import R2
        base = R2().url_publica
        if not base:
            return {}
        idx = _load_index() or {}
        pref = f"{isin}/"
        # solo PDFs que el fondo tiene HOY (el índice guarda también los de análisis anteriores)
        vivos = {f.name for sub in ("discovery", "letters", "reports", "manual") for f in (fd / "raw" / sub).glob("*.pdf")}             if (fd / "raw").exists() else set()
        return {k[len(pref):]: f"{base}/docs/{sha}.pdf" for k, sha in idx.items()
                if k.startswith(pref) and sha and k[len(pref):] in vivos}
    except Exception:
        return {}


def _load(p: Path, dflt=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dflt


def build(isin: str) -> dict:
    isin = isin.upper()
    fd = ROOT / "data" / "funds" / isin
    informes, cartas, externas, vistos = [], [], [], set()
    arch = _archivados(isin, fd)
    # original → nuestra copia (manifiesto de tools.archive_docs)
    copia = {m.get("url_original"): m.get("url") for m in (_load(fd / "documentos_archivados.json", []) or [])
             if isinstance(m, dict) and m.get("url_original") and m.get("url")}
    claves_inf = set()

    def _add_inf(tipo: str, periodo, url: str, fichero: str | None = None):
        """Enlaza a nuestra copia archivada si existe (siempre accesible); la web original como respaldo."""
        k = (tipo, str(periodo or "")[:4])
        if k in claves_inf and k[1]:
            return
        if fichero is None and tipo in ("annual_report", "semi_annual_report") and periodo:
            fichero = f"{tipo}_{str(periodo)[:4]}.pdf"
        propia = arch.get(fichero or "") or copia.get(url)
        u = propia or url
        if not u or u in vistos:
            return
        vistos.add(u)
        claves_inf.add(k)
        informes.append({"nombre": f"{_NOMBRE.get(tipo, 'Documento')} {periodo or ''}".strip(), "url": u,
                         "url_original": url or None, "archivado": bool(propia), "tipo": tipo,
                         "periodo": str(periodo or "")})

    def _add_carta(url: str):
        url = copia.get(url, url)
        if url and url.startswith("http") and url not in vistos:
            vistos.add(url)
            cartas.append(url)

    kb = _load(ROOT / "data" / "known_annual_reports.json", {}) or {}
    for _, u in (kb.get("umbrellas") or {}).items():
        if isin in (u.get("isins") or []):
            for r in u.get("reports") or []:
                _add_inf("annual_report", r.get("year"), r.get("url"))
            for r in u.get("semi_annual_reports") or []:
                _add_inf("semi_annual_report", r.get("year") or r.get("periodo"), r.get("url"))
    disc = _load(fd / "intl_discovery_data.json", {}) or {}
    for d in disc.get("documents") or []:
        if not isinstance(d, dict) or not str(d.get("url") or "").startswith("http"):
            continue
        t = str(d.get("doc_type") or "").lower()
        _f = Path(str(d.get("local_path") or "").replace("\\", "/")).name
        if t in _CARTA:
            _add_carta(arch.get(_f) or d["url"])
        elif t in _TIPO:
            _add_inf(_TIPO[t], d.get("year") or d.get("periodo"), d["url"], _f or None)
    for c in (_load(fd / "letters_data.json", {}) or {}).get("cartas") or []:
        if isinstance(c, dict):
            _f = Path(str(c.get("archivo") or "").replace("\\", "/")).name
            _add_carta(arch.get(_f) or str(c.get("url") or c.get("url_fuente") or ""))
    for _f, _u in arch.items():          # cartas archivadas aunque letters_data no traiga su fichero
        if _f.lower().startswith("kb_letter") or "carta" in _f.lower() or "letter" in _f.lower():
            _add_carta(_u)
    import re as _re
    for _f in sorted(arch):              # informes anuales/semestrales archivados que no estén ya (por año)
        _m = _re.match(r"(semi_annual_report|annual_report)_((?:19|20)\d{2})\.pdf$", _f)
        if _m:
            _add_inf(_m.group(1), _m.group(2), "", _f)
    for _f, _u in arch.items():          # KID / folleto archivados
        _l = _f.lower()
        if any(x in _l for x in ("kid", "kiid", "dici", "priip")):
            _add_inf("kid", "", "", _f)
        elif any(x in _l for x in ("prospect", "folleto")):
            _add_inf("prospectus", "", "", _f)
    for _, g in ((_load(ROOT / "data" / "known_manager_letters.json", {}) or {}).get("funds") or {}).items():
        if isin in [str(x).upper() for x in g.get("isins") or []]:
            for l in g.get("letters") or []:
                if isinstance(l, dict):
                    _add_carta(str(l.get("url") or ""))
    rd = _load(fd / "readings_data.json", {}) or {}
    for x in (rd.get("analisis_completos") or []) + (rd.get("otros_readings") or []):
        u = x.get("url") if isinstance(x, dict) else x
        if isinstance(u, str) and u.startswith("http") and u not in vistos:
            vistos.add(u)
            externas.append(u)
    informes.sort(key=lambda x: (x["tipo"], x["periodo"]), reverse=True)
    return {"informes_pdf": informes, "cartas_urls": cartas, "fuentes_externas_urls": externas, "xmls_cnmv": [],
            "total_fuentes": len(informes) + len(cartas) + len(externas)}


def fusionar(actual: dict | None, isin: str) -> dict:
    """Une la lista del analista (si la hay) con la construida aquí, sin duplicar enlaces."""
    base = dict(actual or {})
    nuevo = build(isin)
    for k in ("informes_pdf", "cartas_urls", "fuentes_externas_urls", "xmls_cnmv"):
        prev = list(base.get(k) or [])
        claves = {(x.get("url") if isinstance(x, dict) else x) for x in prev}
        prev += [x for x in nuevo.get(k) or [] if (x.get("url") if isinstance(x, dict) else x) not in claves]
        base[k] = prev
    base["total_fuentes"] = sum(len(base.get(k) or []) for k in ("informes_pdf", "cartas_urls", "fuentes_externas_urls", "xmls_cnmv"))
    return base


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    r = build(sys.argv[1])
    print(json.dumps({k: (len(v) if isinstance(v, list) else v) for k, v in r.items()}, ensure_ascii=False))
    for x in r["informes_pdf"][:20]:
        print(" ", x["nombre"], "→", x["url"][:80])
