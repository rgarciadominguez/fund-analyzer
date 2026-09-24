"""Comparativa PRE vs POST de un análisis (pruebas reales de relanzamiento, Rafa 2026-09-24).

Uso:
  python -m tools.compare_analysis ISIN --snapshot     # guarda el estado actual en data/funds/ISIN/_pre_test/
  python -m tools.compare_analysis ISIN --report       # compara _pre_test/ con el estado actual → informe markdown
                                                       #   en data/funds/ISIN/comparativa_pre_post.md (y lo imprime)
Qué compara (solo datos, sin juicio): KPIs de cabecera con su fuente, series cuantitativas (nº puntos),
posiciones y cobertura de sector/país, documentos extraídos por tipo/año, cartas (nº y de qué fondo),
gestores, secciones de la síntesis (longitud, cifras), ejes de diferenciación, control de calidad
(score y fallos), Novedades. El juicio cualitativo (mejor/peor) lo hace Claude leyendo este informe.
"""
from __future__ import annotations

import glob
import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"
KEEP = ("output.json", "quality_report.json", "meta_report.json", "letters_data.json", "manager_profile.json",
        "intl_discovery_data.json", "cnmv_data.json", "intl_data.json", "readings_data.json", "config.json")


def _load(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def snapshot(isin: str) -> Path:
    fd = FUNDS / isin
    dst = fd / "_pre_test"
    dst.mkdir(exist_ok=True)
    for name in KEEP:
        if (fd / name).exists():
            shutil.copy2(fd / name, dst / name)
    if (fd / "extracted").exists():
        shutil.copytree(fd / "extracted", dst / "extracted", dirs_exist_ok=True)
    dash = ROOT / "dashboard" / f"fund-{isin}.html"
    if dash.exists():
        shutil.copy2(dash, dst / dash.name)
    (dst / "_snapshot.json").write_text(json.dumps({"isin": isin, "fecha": datetime.now().isoformat()}), encoding="utf-8")
    return dst


def _cifras(t: str) -> int:
    return len(re.findall(r"\d+[.,]?\d*\s*%|\b\d{4}\b|\d+[.,]\d+\s*(M€|M\$|mill)", t or ""))


def _profile(fd: Path) -> dict:
    o = _load(fd / "output.json") or {}
    k = o.get("kpis") or {}
    q = o.get("cuantitativo") or {}
    pos = (o.get("posiciones") or {}).get("actuales") or []
    s = o.get("analyst_synthesis") or {}
    est = s.get("estrategia") or {}
    dif = est.get("diferenciacion") or {}
    qr = _load(fd / "quality_report.json") or {}
    ld = _load(fd / "letters_data.json") or {}
    mp = _load(fd / "manager_profile.json") or {}
    ext = {}
    for f in glob.glob(str(fd / "extracted" / "*.json")):
        n = os.path.basename(f)
        kind = re.sub(r"_(19|20)\d{2}.*", "", n).split("__")[0][:40]
        ext[kind] = ext.get(kind, 0) + 1
    nov = o.get("novedades_resumen") or {}
    return {
        "nombre": o.get("nombre"), "gestora": o.get("gestora"), "actualizado": o.get("ultima_actualizacion"),
        "kpis": {x: k.get(x) for x in ("anio_creacion", "benchmark", "rating_morningstar", "aum_actual_meur", "num_participes",
                                       "num_activos_cartera", "concentracion_top10_pct", "ter_pct", "coste_gestion_pct", "divisa")},
        "kpis_origen": o.get("kpis_origen") or {},
        "series": {x: len(q.get(x) or []) for x in ("serie_aum", "serie_participes", "serie_ter", "serie_rentabilidad",
                                                     "mix_activos_historico", "mix_geografico_historico", "serie_vl_base100")},
        "posiciones": {"n": len(pos), "con_peso": sum(1 for p in pos if p.get("peso_pct")),
                       "con_sector": sum(1 for p in pos if p.get("sector")), "con_pais": sum(1 for p in pos if p.get("pais"))},
        "extractos": ext,
        "cartas": {"n": len(ld.get("cartas") or []), "periodos": [c.get("periodo") or c.get("fecha") for c in (ld.get("cartas") or [])]},
        "gestores": mp.get("equipo_gestor") or mp.get("equipo") or [],
        "secciones": {sec: {"chars": len(json.dumps(v, ensure_ascii=False)), "cifras": _cifras(json.dumps(v, ensure_ascii=False))}
                      for sec, v in s.items() if isinstance(v, (dict, list))},
        "diferenciacion": {e: len((dif.get(e) or {}).get("texto") or "") for e in ("activos", "gestion", "geografia", "filosofia_equipo")},
        "calidad": {"score": qr.get("score"), "fallos": [f.get("regla_id") for f in (qr.get("fallos") or [])]},
        "novedades": {"modo": nov.get("modo"), "veredicto": (nov.get("veredicto") or {}).get("estado"),
                      "huecos": len(nov.get("huecos_de_fondo") or []), "hallazgos": len(nov.get("hallazgos") or [])},
        "graficos_documento": len(o.get("graficos_documento") or []),
        "clases_documento": len(o.get("clases_documento") or []),
    }


def report(isin: str) -> str:
    fd = FUNDS / isin
    pre_dir = fd / "_pre_test"
    if not pre_dir.exists():
        return f"No hay snapshot previo para {isin} (ejecuta --snapshot antes de la prueba)."
    pre, post = _profile(pre_dir), _profile(fd)
    L = [f"# Comparativa PRE vs POST — {isin} ({post.get('nombre')})", "",
         f"Snapshot PRE: {(_load(pre_dir / '_snapshot.json') or {}).get('fecha')} · POST: {post.get('actualizado')}", ""]
    L += ["## KPIs de cabecera", "", "| KPI | PRE | POST | fuente POST |", "|---|---|---|---|"]
    for x in pre["kpis"]:
        a, b = pre["kpis"].get(x), post["kpis"].get(x)
        mark = "" if a == b else " ←"
        L.append(f"| {x} | {a} | {b}{mark} | {post['kpis_origen'].get(x, '')} |")
    L += ["", "## Series cuantitativas (nº de puntos)", "", "| serie | PRE | POST |", "|---|---|---|"]
    for x in pre["series"]:
        L.append(f"| {x} | {pre['series'][x]} | {post['series'][x]} |")
    L += ["", "## Cartera", "", f"PRE: {pre['posiciones']}  →  POST: {post['posiciones']}", ""]
    L += ["## Documentos extraídos (por tipo)", "", f"PRE: {pre['extractos']}", f"POST: {post['extractos']}", ""]
    L += ["## Cartas", "", f"PRE: {pre['cartas']}", f"POST: {post['cartas']}", ""]
    L += ["## Gestores", "", f"PRE: {pre['gestores']}", f"POST: {post['gestores']}", ""]
    L += ["## Síntesis (longitud / nº de cifras por sección)", "", "| sección | PRE | POST |", "|---|---|---|"]
    for sec in sorted(set(pre["secciones"]) | set(post["secciones"])):
        a, b = pre["secciones"].get(sec, {}), post["secciones"].get(sec, {})
        L.append(f"| {sec} | {a.get('chars', 0)} chars / {a.get('cifras', 0)} cifras | {b.get('chars', 0)} chars / {b.get('cifras', 0)} cifras |")
    L += ["", f"Ejes de diferenciación (chars): PRE {pre['diferenciacion']} → POST {post['diferenciacion']}", ""]
    L += ["## Control de calidad", "", f"PRE: score {pre['calidad']['score']}, {len(pre['calidad']['fallos'])} fallos: {pre['calidad']['fallos']}",
          f"POST: score {post['calidad']['score']}, {len(post['calidad']['fallos'])} fallos: {post['calidad']['fallos']}", ""]
    L += ["## Novedades / gráficos / clases", "", f"PRE: {pre['novedades']} · gráficos {pre['graficos_documento']} · clases {pre['clases_documento']}",
          f"POST: {post['novedades']} · gráficos {post['graficos_documento']} · clases {post['clases_documento']}", ""]
    txt = "\n".join(L)
    (fd / "comparativa_pre_post.md").write_text(txt, encoding="utf-8")
    return txt


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    a = [x for x in sys.argv[1:] if not x.startswith("--")]
    if not a:
        print("uso: python -m tools.compare_analysis ISIN --snapshot | --report")
        sys.exit(1)
    if "--snapshot" in sys.argv:
        print("snapshot en", snapshot(a[0].upper()))
    else:
        print(report(a[0].upper()))
