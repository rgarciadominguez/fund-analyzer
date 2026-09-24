"""Publica en output.json lo que los DOCS APORTADOS traen y que no es una serie numérica:

  1. `graficos_documento` — gráficos de EVOLUCIÓN del documento RE-DIBUJADOS con el formato del
     dashboard (decisión de Rafa 2026-09-21: nada de recortes del PDF). Las cifras salen de la
     geometría vectorial del PDF (tools/pdf_chart_digitizer, medido contra los ejes); el extractor
     solo elige qué gráficos interesan, nombra las series, da las fechas del eje si no son legibles
     y escribe la lectura. Aquí se combinan ambos → {labels, series} listos para Chart.js.
  2. `clases_documento` — la tabla completa de clases del documento (código, ISIN, divisa,
     cubierta, reparto, comisión de gestión y de ÉXITO, mínimo, activa).

Idempotente y determinista (sin LLM). Lo llama el consume final antes de generar el dashboard.
CLI: python -m tools.aportado_publish --isin IE000Z9YV312
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CHARTS_DIR = ROOT / "dashboard" / "doc-charts"
MAX_CHARTS_PER_DOC = 8
SECCIONES = {"cartera", "rentabilidad", "riesgo", "patrimonio", "estrategia"}


def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")[:40] or "doc"


def _month_add(ym: str, k: int) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    t = y * 12 + (m - 1) + k
    return f"{t // 12}-{t % 12 + 1:02d}"


def _months_between(a: str, b: str) -> int:
    return (int(b[:4]) * 12 + int(b[5:7])) - (int(a[:4]) * 12 + int(a[5:7]))


def _norm_hex(h) -> str:
    return (h or "").strip().lower()


def build_chart(item: dict, digit: dict, digit_all: dict | None = None, log=print) -> dict | None:
    """Item del extractor (elige/nombra/interpreta) + gráfico digitalizado (cifras) → gráfico final
    {labels, series:[{nombre, data}]} listo para pintar. None si no se puede construir con garantías."""
    series_out, labels = [], None
    complete = False
    if item.get("aproximado") and not digit:
        # gráfico raster leído a ojo por el extractor: series con sus propios puntos
        labs = []
        for s_ in item.get("series") or []:
            for lab, _v in s_.get("puntos") or []:
                if str(lab) not in labs:
                    labs.append(str(lab))
        labs.sort()
        for s_ in item.get("series") or []:
            m = {str(l): v for l, v in (s_.get("puntos") or [])}
            series_out.append({"nombre": s_.get("nombre") or "", "data": [m.get(l) for l in labs]})
        labels = labs
    elif digit:
        want = {_norm_hex(s_.get("color_hex")): s_.get("nombre") for s_ in (item.get("series") or [])
                if isinstance(s_, dict) and s_.get("color_hex")}
        chosen = [(want[_norm_hex(d["color_hex"])], d) for d in digit["series"] if _norm_hex(d["color_hex"]) in want]
        if not chosen:
            return None
        complete = len(chosen) == len(digit["series"])
        rel = digit.get("eje_x") == "relativo"
        conv = {}
        if rel:
            xs = sorted({float(l[1:]) for _, d in chosen for l, _v in d["puntos"]})
            cats = item.get("categorias") or []
            x0, x1 = item.get("x_inicio"), item.get("x_fin")
            if cats:                                # categórico: agrupar posiciones en len(cats) cubos
                groups, tol = [], (xs[-1] - xs[0]) / max(len(cats) * 3, 1)
                for x in xs:
                    if groups and x - groups[-1][-1] <= tol:
                        groups[-1].append(x)
                    else:
                        groups.append([x])
                if len(groups) != len(cats):
                    log(f"[APORTADO-PUB] {item.get('id')}: {len(groups)} grupos vs {len(cats)} categorías → descartado")
                    return None
                for g, c in zip(groups, cats):
                    for x in g:
                        conv[f"@{x:.4f}"] = str(c)
            elif x0 and x1 and re.match(r"^\d{4}-\d{2}$", x0) and re.match(r"^\d{4}-\d{2}$", x1) and xs[-1] > xs[0]:
                n = _months_between(x0, x1)
                for x in xs:
                    conv[f"@{x:.4f}"] = _month_add(x0, int(round(n * (x - xs[0]) / (xs[-1] - xs[0]))))
            else:
                log(f"[APORTADO-PUB] {item.get('id')}: eje relativo sin x_inicio/x_fin ni categorias → descartado")
                return None
        agg_sum = digit.get("apilado") or item.get("formato") == "barras_apiladas"
        per_series = []
        for nombre, d in chosen:
            acc = {}
            for l, v in d["puntos"]:
                acc.setdefault(conv.get(l, l) if rel else l, []).append(v)
            per_series.append((nombre, {k: sum(v) / len(v) for k, v in acc.items()}))
        labels = sorted({k for _, m in per_series for k in m}) if not (rel and item.get("categorias"))             else [str(c) for c in item["categorias"]]
        for nombre, m in per_series:
            series_out.append({"nombre": nombre, "data": [round(m[l], 2) if l in m else None for l in labels]})
    if not series_out or not labels or len(labels) < 2:
        return None
    # DESGLOSE de una serie con otro gráfico (p.ej. "Financiero" × reparto bancos/aseguradoras del
    # gráfico p48#1): item.desglose = {serie: <nombre en este gráfico>, grafico: <id>, x_inicio, x_fin,
    # partes: [{color_hex, nombre}]}. Las partes sustituyen a la serie, con su peso × cuota.
    dg = item.get("desglose") if isinstance(item.get("desglose"), dict) else None
    if dg and digit_all and dg.get("grafico") in digit_all and dg.get("serie"):
        sub_item = {"id": dg["grafico"], "formato": "area_apilada", "unidad": "%",
                    "x_inicio": dg.get("x_inicio"), "x_fin": dg.get("x_fin"), "categorias": dg.get("categorias"),
                    "series": dg.get("partes") or []}
        sub = build_chart(sub_item, digit_all[dg["grafico"]], digit_all=None, log=log)
        base_idx = next((i for i, s_ in enumerate(series_out) if s_["nombre"] == dg["serie"]), None)
        if sub and base_idx is not None and sub["series"]:
            def _share_at(lab, k):
                # cuota de la parte k en la etiqueta `lab` (o la más cercana por orden)
                cand = [l for l in sub["labels"] if l <= lab] or sub["labels"][:1]
                j = sub["labels"].index(cand[-1])
                tot = sum((s_["data"][j] or 0) for s_ in sub["series"]) or 1
                return (sub["series"][k]["data"][j] or 0) / tot
            base = series_out.pop(base_idx)
            nuevas = []
            for k, part in enumerate(sub["series"]):
                nuevas.append({"nombre": f"{dg['serie']} · {part['nombre']}",
                               "data": [None if v is None else round(v * _share_at(lab, k), 2)
                                        for lab, v in zip(labels, base["data"])]})
            series_out[base_idx:base_idx] = nuevas
    # 100% apilado: normalizar pequeñas desviaciones de medida
    if item.get("formato") == "area_apilada" and (item.get("unidad") or "").strip() == "%":
        for s_ in series_out:              # en apilado, hueco = 0 (la banda no existe ese mes)
            s_["data"] = [0 if v is None else v for v in s_["data"]]
        # series no capturadas (ruido/pequeñas) → "Otros" hasta 100, para que el apilado no quede corto
        resto = [round(max(0.0, 100 - sum((s_["data"][i] or 0) for s_ in series_out)), 2) for i in range(len(labels))]
        if not complete and any(r >= 1 for r in resto) and all(r <= 25 for r in resto):
            series_out.append({"nombre": "Otros", "data": resto})
        for i in range(len(labels)):
            tot = sum((s_["data"][i] or 0) for s_ in series_out)
            if 97 <= tot <= 103:       # solo error de medida; si faltan series (omitidas) no se reescala
                for s_ in series_out:
                    if s_["data"][i] is not None:
                        s_["data"][i] = round(s_["data"][i] * 100 / tot, 2)
    return {"labels": labels, "series": series_out}



def consolidate_clases(clases: list[dict]) -> list[dict]:
    """Una fila por clase (Rafa 24-sep: Gamma salía con 10 filas para 2 clases, una por documento y
    con comisiones de años distintos). Clave = ISIN si lo hay, si no el código. Manda la fila del
    documento MÁS RECIENTE (`periodo` del extracto; a igualdad, la última leída) y las demás solo
    rellenan huecos. Se conserva `fuente` de la fila que manda y `fuentes` con todos los documentos."""
    def _key(c):
        return (c.get("isin") or "").upper() or ("cod:" + str(c.get("codigo") or "").upper())
    def _per(c):
        return str(c.get("periodo") or "")
    grupos: dict[str, list[dict]] = {}
    for c in clases:
        if not isinstance(c, dict):
            continue
        grupos.setdefault(_key(c), []).append(c)
    # un ISIN conocido absorbe las filas del mismo código sin ISIN
    by_cod: dict[str, str] = {}
    for k, rows in grupos.items():
        if not k.startswith("cod:"):
            for r in rows:
                cod = str(r.get("codigo") or "").upper()
                if cod:
                    by_cod.setdefault(cod, k)
    for k in list(grupos):
        if k.startswith("cod:") and k[4:] in by_cod:
            grupos[by_cod[k[4:]]].extend(grupos.pop(k))
    out = []
    for k, rows in grupos.items():
        rows_sorted = sorted(rows, key=_per, reverse=True)   # estable: a igual periodo, la última leída primero
        base = dict(rows_sorted[0])
        for r in rows_sorted[1:]:
            for kk, v in r.items():
                if base.get(kk) in (None, "", []) and v not in (None, "", []):
                    base[kk] = v
        base["fuentes"] = sorted({str(r.get("fuente") or "") for r in rows if r.get("fuente")})
        out.append(base)
    out.sort(key=lambda c: (str(c.get("divisa") or ""), str(c.get("codigo") or ""), str(c.get("isin") or "")))
    return out

def apply(isin: str, log=print) -> dict:
    isin = isin.upper()
    fd = ROOT / "data" / "funds" / isin
    op = fd / "output.json"
    if not op.exists():
        return {"changed": False}
    from tools.aportados import current_extracts
    graficos: list[dict] = []
    clases: list[dict] = []
    for ep in current_extracts(isin):
        try:
            ex = json.loads(ep.read_text(encoding="utf-8"))
        except Exception:
            continue
        data = ex.get("data") or {}
        if not isinstance(data, dict):
            continue
        doc_name = Path(str(ex.get("pdf_path") or ep.name).replace("\\", "/")).name
        # ── clases ──
        for c in data.get("clases_documento") or []:
            if isinstance(c, dict) and (c.get("isin") or c.get("codigo")):
                row = dict(c)
                row["isin"] = (row.get("isin") or "").upper().strip() or None
                row["fuente"] = doc_name
                row["periodo"] = data.get("periodo") or ""
                clases.append(row)
        # ── gráficos: elección/nombres/lectura del extractor + cifras digitalizadas ──
        items = [g for g in (data.get("graficos_documento") or []) if isinstance(g, dict)]
        if not items:
            continue
        from tools.aportados import charts_paths
        full_p, _ = charts_paths(isin, doc_name)
        digit_by_id = {}
        if full_p.exists():
            try:
                for _pg, chs in json.loads(full_p.read_text(encoding="utf-8")).items():
                    for c in chs:
                        digit_by_id[c["id"]] = c
            except Exception:
                pass
        for g in items[:MAX_CHARTS_PER_DOC]:
            built = build_chart(g, digit_by_id.get(str(g.get("id") or "")), digit_all=digit_by_id, log=log)
            if not built:
                continue
            sec = (g.get("seccion") or "").lower()
            m_pg = re.match(r"p(\d+)#", str(g.get("id") or ""))
            graficos.append({
                "id": g.get("id"), "titulo": g.get("titulo") or "",
                "seccion": sec if sec in SECCIONES else "cartera",
                "formato": g.get("formato") or "linea", "unidad": g.get("unidad") or "",
                "labels": built["labels"], "series": built["series"],
                "lectura": g.get("lectura") or "", "aproximado": bool(g.get("aproximado")),
                "dimension": (g.get("dimension") or "otro").lower(), "clave": bool(g.get("clave")),
                "documento": doc_name, "pagina": int(m_pg.group(1)) if m_pg else g.get("pagina"),
                "periodo": data.get("periodo") or "",
            })
    out = json.loads(op.read_text(encoding="utf-8"))
    # formato antiguo (recortes del PDF como imagen): se retira siempre, con sus ficheros
    if any(isinstance(g, dict) and g.get("img") for g in (out.get("graficos_documento") or [])):
        out["graficos_documento"] = [g for g in out["graficos_documento"] if not (isinstance(g, dict) and g.get("img"))]
        old_fmt = True
    else:
        old_fmt = False
    import shutil
    if (CHARTS_DIR / isin).exists():
        shutil.rmtree(CHARTS_DIR / isin, ignore_errors=True)
    before = (out.get("graficos_documento"), out.get("clases_documento"))
    # Vacío NO borra: si esta pasada no trae nada (extract aún sin re-hacer), se conserva lo previo.
    if graficos:
        # CUERPO de las pestañas: solo las dimensiones estándar (una por dimensión) + máx. 2 "clave"
        # del tipo de fondo. El resto va a la pestaña "Anexo gráficos" (decisión Rafa 2026-09-22).
        STD = ("rating", "sector", "geografia", "tipo_activo", "rotacion")
        vistos, n_clave = set(), 0
        for g in graficos:
            g["en_cuerpo"] = False
            if g["dimension"] in STD and g["dimension"] not in vistos:
                vistos.add(g["dimension"]); g["en_cuerpo"] = True
            elif g["clave"] and n_clave < 2:
                n_clave += 1; g["en_cuerpo"] = True
        out["graficos_documento"] = sorted(graficos, key=lambda g: (g["seccion"], g.get("pagina") or 0))
    if clases:
        out["clases_documento"] = consolidate_clases(clases)
    changed = old_fmt or before != (out.get("graficos_documento"), out.get("clases_documento"))
    if changed:
        op.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"[APORTADO-PUB] {len(graficos)} gráficos del documento + {len(clases)} clases publicados")
    return {"changed": changed, "graficos": len(graficos), "clases": len(clases)}


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin", required=True)
    print(apply(ap.parse_args().isin))
