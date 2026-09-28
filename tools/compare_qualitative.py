"""Comparativa CUALITATIVA entre dos versiones de un análisis (Rafa, 29-sep-2026: "la comparativa es cero
visual y demasiado cuantitativa; lo importante es entender las diferencias cualitativas").

Uso:
  python -m tools.compare_qualitative ISIN [--pre DIR] [--post DIR] [--titulo "..."] [--model claude-opus-5]
    --pre  por defecto data/funds/ISIN/_pre_test ; --post por defecto data/funds/ISIN
  Escribe data/funds/ISIN/comparativa_cualitativa.md y .html (y los abre no; el que llama decide).

Qué hace: recorta las dos síntesis (resumen, estrategia con ejes y perfil de riesgo, gestores, cartera,
evolución, historia, fuentes) y pide a Claude (Opus por defecto) una lectura de analista: qué cambia en la
conclusión, por eje qué decía antes y qué dice ahora, riesgos y exposición, equipo, cartera, evidencia
nueva y afirmaciones que desaparecen, errores en cualquiera de las dos, veredicto. Cabecera visual con
chips (cartas, documentos, cifras, ejes, calidad) calculada aquí, sin modelo.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"
SECCIONES = ("resumen", "estrategia", "gestores", "cartera", "evolucion", "historia", "fuentes_externas")


def _load(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _trim(s, n):
    s = s if isinstance(s, str) else json.dumps(s, ensure_ascii=False)
    return s if len(s) <= n else s[:n] + " […]"


def _sintesis(d: Path) -> dict:
    o = _load(d / "output.json") or {}
    s = o.get("analyst_synthesis") or {}
    out = {"nombre": o.get("nombre"), "actualizado": o.get("ultima_actualizacion"),
           "kpis": {k: (o.get("kpis") or {}).get(k) for k in ("aum_actual_meur", "num_participes", "ter_pct",
                                                              "coste_gestion_pct", "rating_morningstar")}}
    for sec in SECCIONES:
        v = s.get(sec) or {}
        if not isinstance(v, dict):
            continue
        item = {"texto": _trim(v.get("texto") or "", 7000)}
        for k in ("senal", "conclusion", "resumen_general", "fortalezas", "riesgos", "hitos_estrategia", "quotes",
                  "perfiles", "top_posiciones", "opiniones_clave"):
            if v.get(k):
                item[k] = _trim(v[k], 3500)
        if sec == "estrategia":
            item["diferenciacion"] = _trim(v.get("diferenciacion") or {}, 6000)
            item["perfil_riesgo"] = _trim(v.get("perfil_riesgo") or {}, 6000)
        out[sec] = item
    if s.get("glosario"):
        out["glosario"] = _trim(s["glosario"], 1500)
    if o.get("novedades_resumen"):
        out["novedades_resumen"] = _trim(o["novedades_resumen"], 4000)
    return out


def _chips(d: Path) -> dict:
    o = _load(d / "output.json") or {}
    s = o.get("analyst_synthesis") or {}
    ld = _load(d / "letters_data.json") or {}
    qr = _load(d / "quality_report.json") or {}
    txt = json.dumps({k: s.get(k) for k in SECCIONES}, ensure_ascii=False)
    cifras = len(re.findall(r"\d+[.,]?\d*\s*%|\b(?:19|20)\d{2}\b|\d+[.,]\d+\s*(?:M€|M\$|mill)", txt))
    dif = ((s.get("estrategia") or {}).get("diferenciacion") or {})
    ejes = sum(1 for e in ("activos", "gestion", "geografia", "filosofia_equipo") if (dif.get(e) or {}).get("texto"))
    pr = ((s.get("estrategia") or {}).get("perfil_riesgo") or {})
    n_exp = len(pr.get("desglose_exposicion") or [])
    docs = len(glob.glob(str(d / "extracted" / "*.json")))
    aport = len(glob.glob(str(d / "extracted" / "aportado_*.json")))
    return {
        "Cartas del gestor": len(ld.get("cartas") or []),
        "Documentos extraídos": docs,
        "De ellos aportados": aport,
        "Cifras en la síntesis": cifras,
        "Ejes de diferenciación": f"{ejes}/4",
        "Dimensiones de exposición": n_exp,
        "Fallos de calidad": len(qr.get("fallos") or []),
        "Gráficos del documento": len(o.get("graficos_documento") or []),
        "Clases del documento": len(o.get("clases_documento") or []),
    }


PROMPT = """Eres el analista senior que revisa el trabajo de otro analista para Rafa, asesor financiero
independiente. Rafa usa estos análisis para decidir si un fondo encaja en la cartera de un cliente y para
explicárselo. Le importa entender EN QUÉ SE DIFERENCIA el fondo (activos y mix, tipo de gestión y control,
geografía, filosofía y equipo), sus riesgos reales, y si lo que la gestora dice coincide con lo que hace.

Tienes dos versiones del mismo análisis del fondo {nombre} ({isin}):
- ANTES ("{titulo_pre}")
- AHORA ("{titulo_post}")

Escribe en español, para Rafa, una comparativa CUALITATIVA, en Markdown, con exactamente estas secciones:

## En una frase
Qué cambia en la conclusión (señal, tesis central, matiz principal). Una o dos frases.

## Por eje de diferenciación
Una tabla Markdown con columnas: Eje | Antes decía | Ahora dice | Qué aporta o pierde. Filas: Activos y mix ·
Gestión y control · Geografía · Filosofía y equipo. Celdas de 1-3 frases, concretas (cifras, años, nombres),
sin generalidades.

## Riesgos y exposición
Qué riesgos o dimensiones de exposición aparecen ahora que antes no, cuáles cambian de lectura, cuáles
desaparecen. Lista corta con bullets; cada bullet con la cifra o el hecho que lo sustenta.

## Equipo, cartera y evolución
Tres párrafos breves (uno por tema): qué se entiende ahora mejor o peor y por qué.

## Evidencia
Qué fuentes nuevas sustentan los cambios (cartas, documentos aportados, informes) y qué afirmaciones de
ANTES han desaparecido o se contradicen AHORA. Cita 2-4 frases literales cortas de AHORA que muestren la
mejora, y 1-2 de ANTES que ya no estén, si las hay.

## Errores o dudas
Afirmaciones que parecen incorrectas, incoherentes entre secciones o no respaldadas, en CUALQUIERA de las
dos versiones. Si no ves ninguna, dilo.

## Veredicto
¿Es mejor AHORA? En qué sí y en qué no. Termina con 2-4 mejoras concretas que pedirías para la siguiente
versión, ordenadas por importancia.

Reglas: sé crítico y honesto; no rellenes; cuando algo no se pueda juzgar con lo que tienes, dilo. Nada de
cifras sueltas sin decir qué significan. No repitas el análisis: compara.

=== ANTES ===
{pre}

=== AHORA ===
{post}
"""


def run(isin: str, pre: Path, post: Path, titulo_pre: str, titulo_post: str, model: str, log=print) -> Path:
    fd = FUNDS / isin
    s_pre, s_post = _sintesis(pre), _sintesis(post)
    nombre = s_post.get("nombre") or s_pre.get("nombre") or isin
    prompt = PROMPT.format(nombre=nombre, isin=isin, titulo_pre=titulo_pre, titulo_post=titulo_post,
                           pre=json.dumps(s_pre, ensure_ascii=False, indent=1),
                           post=json.dumps(s_post, ensure_ascii=False, indent=1))
    tmp = fd / "_comparativa_prompt.txt"
    tmp.write_text(prompt, encoding="utf-8")
    log(f"[compare_qualitative] prompt {len(prompt):,} chars → {model}")
    env = dict(os.environ)
    env.pop("CLAUDE_CODE_OAUTH_TOKEN", None)
    with open(tmp, encoding="utf-8") as fh:
        r = subprocess.run(["cmd", "/c", "claude", "-p", "--model", model, "--effort", "high",
                            "--allowedTools", ""], stdin=fh, capture_output=True, text=True,
                           encoding="utf-8", errors="replace", env=env, timeout=1800)
    md_body = (r.stdout or "").strip()
    if r.returncode != 0 or len(md_body) < 500:
        raise RuntimeError(f"claude rc={r.returncode}: {(r.stderr or md_body)[-300:]}")
    tmp.unlink(missing_ok=True)

    c_pre, c_post = _chips(pre), _chips(post)
    head = [f"# {nombre} — comparativa cualitativa", "",
            f"**Antes:** {titulo_pre} · **Ahora:** {titulo_post} · generado {datetime.now():%d-%m-%Y %H:%M}", "",
            "| | Antes | Ahora |", "|---|---:|---:|"]
    for k in c_pre:
        a, b = c_pre[k], c_post[k]
        head.append(f"| {k} | {a} | {b}{' ←' if a != b else ''} |")
    md = "\n".join(head) + "\n\n" + md_body + "\n"
    out_md = fd / "comparativa_cualitativa.md"
    out_md.write_text(md, encoding="utf-8")
    try:
        import markdown
        body = markdown.markdown(md, extensions=["tables"])
        html = ('<!doctype html><html lang="es"><head><meta charset="utf-8"><title>Comparativa cualitativa</title>'
                '<style>body{font-family:Segoe UI,system-ui,sans-serif;max-width:1000px;margin:32px auto;padding:0 20px;'
                'color:#1c2431;line-height:1.55}table{border-collapse:collapse;margin:12px 0;font-size:13.5px;width:100%}'
                'th,td{border:1px solid #d5dbe3;padding:7px 10px;text-align:left;vertical-align:top}th{background:#eef2f7}'
                'h1{font-size:24px}h2{font-size:18px;margin-top:30px;border-bottom:2px solid #0c2340;padding-bottom:4px;color:#0c2340}'
                'blockquote{border-left:3px solid #c8a23c;margin:8px 0;padding:4px 12px;color:#444}'
                'code{background:#f3f5f8;padding:1px 4px;border-radius:3px}</style></head><body>' + body + '</body></html>')
        (fd / "comparativa_cualitativa.html").write_text(html, encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        log(f"[compare_qualitative] html no generado: {e}")
    log(f"[compare_qualitative] escrito {out_md}")
    return out_md


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("isin")
    ap.add_argument("--pre")
    ap.add_argument("--post")
    ap.add_argument("--titulo-pre", default="versión anterior")
    ap.add_argument("--titulo-post", default="versión actual")
    ap.add_argument("--model", default="claude-opus-5")
    a = ap.parse_args()
    isin = a.isin.upper()
    pre = Path(a.pre) if a.pre else FUNDS / isin / "_pre_test"
    post = Path(a.post) if a.post else FUNDS / isin
    run(isin, pre, post, a.titulo_pre, a.titulo_post, a.model)
