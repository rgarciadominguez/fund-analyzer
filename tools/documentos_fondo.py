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


def _load(p: Path, dflt=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dflt


def build(isin: str) -> dict:
    isin = isin.upper()
    fd = ROOT / "data" / "funds" / isin
    informes, cartas, externas, vistos = [], [], [], set()

    def _add_inf(tipo: str, periodo, url: str):
        if not url or url in vistos:
            return
        vistos.add(url)
        informes.append({"nombre": f"{_NOMBRE.get(tipo, 'Documento')} {periodo or ''}".strip(), "url": url,
                         "tipo": tipo, "periodo": str(periodo or "")})

    def _add_carta(url: str):
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
        if t in _CARTA:
            _add_carta(d["url"])
        elif t in _TIPO:
            _add_inf(_TIPO[t], d.get("year") or d.get("periodo"), d["url"])
    for c in (_load(fd / "letters_data.json", {}) or {}).get("cartas") or []:
        if isinstance(c, dict):
            _add_carta(str(c.get("url") or c.get("url_fuente") or ""))
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
