"""Digitaliza gráficos VECTORIALES de un PDF (presentaciones de gestoras) → series numéricas.

Por qué: los docs aportados traen gráficos de evolución muy valiosos (yield/duración históricos,
IG vs no-IG, estructura de deuda, AUM…) SIN tabla de datos. Casi siempre son vectoriales: barras
(rects), líneas/áreas (curves) y los rótulos de los ejes como texto. Midiendo la geometría contra
el eje Y se recuperan los valores con precisión de ~1% del rango — mucho mejor que leerlos a ojo.
Con eso el dashboard pinta gráficos PROPIOS (mismo formato que el resto del análisis).

Qué hace por página:
  1. Ejes Y: columnas de rótulos numéricos equiespaciados → mapa lineal píxel→valor y región del gráfico.
  2. Eje X: rótulos bajo la región (texto girado incluido) → fechas 'YYYY-MM' / 'YYYY' o categorías.
  3. Series por COLOR: barras, líneas y áreas (apiladas: grosor de la banda). Nombre = leyenda del color.
No interpreta: qué gráfico interesa, su título y su lectura los pone el extractor (LLM), que usa
estas cifras como fuente. Si una página es una imagen (raster) devuelve charts=[].

CLI: python -m tools.pdf_chart_digitizer --pdf X.pdf --pages 33,48 [--out fichero.json]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

_NUM = re.compile(r"^\(?-?[\d]+(?:[.,]\d+)*\)?%?$")
_MON = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"])}
_MON.update({"ene": 1, "abr": 4, "ago": 8, "dic": 12})
MAX_POINTS = 90


def _val(txt: str) -> float | None:
    t = txt.strip().rstrip("%")
    neg = t.startswith("(") and t.endswith(")")
    t = t.strip("()")
    if t.count(",") and t.count("."):
        t = t.replace(",", "")
    elif t.count(",") == 1 and len(t.split(",")[1]) == 3:
        t = t.replace(",", "")
    else:
        t = t.replace(",", ".")
    try:
        v = float(t)
        return -v if neg else v
    except ValueError:
        return None


def _ckey(c) -> str:
    if c is None:
        return ""
    if isinstance(c, (int, float)):
        c = (c,)
    return ",".join(f"{float(x):.2f}" for x in c)


def _hex(ck: str) -> str:
    try:
        c = [float(x) for x in ck.split(",")]
        if len(c) == 4:
            rgb = [(1 - c[i]) * (1 - c[3]) for i in range(3)]
        elif len(c) == 3:
            rgb = c
        else:
            rgb = [c[0]] * 3
        return "#" + "".join(f"{int(round(255 * v)):02x}" for v in rgb)
    except Exception:
        return ""


def _is_blank(c) -> bool:
    """Sin color / blanco / transparente (CMYK 0000, RGB 111, gris 1)."""
    if c is None:
        return True
    if isinstance(c, (int, float)):
        c = (c,)
    c = [float(x) for x in c]
    if len(c) == 4:
        return sum(c) < 0.02
    return all(x > 0.97 for x in c)


def _parse_date(txt: str, upright: bool = True):
    """→ (date, granularidad 'M'|'Y') o None. Los rótulos girados llegan INVERTIDOS ('91-ceD'):
    si la palabra no es upright se prueba primero al revés ('12/60' es 06/21, no dic-2060)."""
    res = None
    for t in ((txt, txt[::-1]) if upright else (txt[::-1], txt)):
        res = _parse_date_one(t)
        if res and 1985 <= res[0].year <= date.today().year + 1:
            return res
    return None


def _parse_date_one(txt: str):
    for t in (txt,):
        t = t.strip().lower().replace(".", "")
        m = re.match(r"^([a-z]{3})[a-z]*[-/ ']?(\d{2}|\d{4})$", t)
        if m and m.group(1) in _MON:
            y = int(m.group(2)); y += 2000 if y < 100 else 0
            return date(y, _MON[m.group(1)], 1), "M"
        m = re.match(r"^(\d{1,2})[-/](\d{2}|\d{4})$", t)                      # 06/21
        if m and 1 <= int(m.group(1)) <= 12:
            y = int(m.group(2)); y += 2000 if y < 100 else 0
            return date(y, int(m.group(1)), 1), "M"
        m = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{2}|\d{4})$", t)         # 22/02/2019
        if m:
            d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
            y += 2000 if y < 100 else 0
            if mo > 12 and d <= 12:
                d, mo = mo, d
            if 1 <= mo <= 12:
                return date(y, mo, 1), "M"
        m = re.match(r"^(19|20)\d{2}$", t)
        if m:
            return date(int(t), 12, 1), "Y"
    return None


def _y_axes(words, page_w):
    """Columnas de rótulos numéricos equiespaciados → ejes Y."""
    nums = [w for w in words if _NUM.match(w["text"]) and _val(w["text"]) is not None]
    used, axes = set(), []
    for i, w in enumerate(sorted(nums, key=lambda w: (round(w["x1"]), w["top"]))):
        if id(w) in used:
            continue
        col = [v for v in nums if abs(v["x1"] - w["x1"]) <= 5 and id(v) not in used]
        col.sort(key=lambda v: v["top"])
        # tramo contiguo con paso vertical constante
        best = []
        for s in range(len(col)):
            run = [col[s]]
            for v in col[s + 1:]:
                if len(run) == 1 or abs((v["top"] - run[-1]["top"]) - (run[1]["top"] - run[0]["top"])) <= 2.5:
                    if v["top"] - run[-1]["top"] > 4:
                        run.append(v)
                else:
                    break
            if len(run) > len(best):
                best = run
        if len(best) < 4:
            continue
        vals = [_val(v["text"]) for v in best]
        steps = [vals[k] - vals[k + 1] for k in range(len(vals) - 1)]
        if not all(st > 0 for st in steps) or max(steps) - min(steps) > 1e-6 + 0.02 * abs(steps[0]):
            continue
        for v in best:
            used.add(id(v))
        yc = [(v["top"] + v["bottom"]) / 2 for v in best]
        a = (vals[-1] - vals[0]) / (yc[-1] - yc[0])
        axes.append({"x0": min(v["x0"] for v in best), "x1": max(v["x1"] for v in best),
                     "top": yc[0], "bottom": yc[-1], "a": a, "b": vals[0] - a * yc[0],
                     "vmax": vals[0], "vmin": vals[-1],
                     "pct": any(v["text"].endswith("%") for v in best)})
    return axes


def _regions(axes, page_w):
    regs = []
    axes = sorted(axes, key=lambda a: (a["top"], a["x0"]))
    skip = set()
    for i, a in enumerate(axes):
        if i in skip:
            continue
        right = page_w - 15
        for j, b in enumerate(axes):
            if j == i or b["x0"] <= a["x1"]:
                continue
            overlap = min(a["bottom"], b["bottom"]) - max(a["top"], b["top"])
            if overlap > 0.5 * (a["bottom"] - a["top"]):
                if b["x0"] < right:
                    right = b["x0"] - 2
                    twin = abs(b["vmax"] - a["vmax"]) < 1e-9 and abs(b["vmin"] - a["vmin"]) < 1e-9
                    nxt = (j, twin)
        # eje gemelo a la derecha (mismos valores) → no es otro gráfico
        for j, b in enumerate(axes):
            if j != i and abs(b["x0"] - 2 - right) < 1 and abs(b["vmax"] - a["vmax"]) < 1e-9 \
                    and abs(b["top"] - a["top"]) < 3:
                skip.add(j)
        regs.append({"axis": a, "x0": a["x1"] + 1, "x1": right, "top": a["top"] - 4, "bottom": a["bottom"] + 4})
    return regs


def _poly_span(pts, x):
    """(ymin, ymax) del contorno del polígono en la vertical x; None si no lo cruza."""
    ys = []
    n = len(pts)
    for k in range(n):
        (xa, ya), (xb, yb) = pts[k], pts[(k + 1) % n]
        if xa == xb:
            if abs(xa - x) < 0.05:
                ys += [ya, yb]
            continue
        if min(xa, xb) - 1e-6 <= x <= max(xa, xb) + 1e-6:
            ys.append(ya + (yb - ya) * (x - xa) / (xb - xa))
    return (min(ys), max(ys)) if ys else None


def _line_y(pts, x):
    best = None
    for k in range(len(pts) - 1):
        (xa, ya), (xb, yb) = pts[k], pts[k + 1]
        if xa != xb and min(xa, xb) - 1e-6 <= x <= max(xa, xb) + 1e-6:
            best = ya + (yb - ya) * (x - xa) / (xb - xa)
    return best


def _legend(page, words):
    """color → nombre, desde marcadores pequeños (cuadros o trazos) con texto a su derecha."""
    marks = []
    for r in list(page.rects) + list(page.curves):
        w, h = r["x1"] - r["x0"], r["bottom"] - r["top"]
        col = r.get("non_stroking_color") if r.get("fill", True) else r.get("stroking_color")
        if 2.5 <= w <= 9 and 2.5 <= h <= 9 and not _is_blank(col):
            marks.append((r["x0"], r["x1"], (r["top"] + r["bottom"]) / 2, _ckey(col)))
    for l in list(page.lines) + [c for c in page.curves if not c.get("fill")]:
        w, h = l["x1"] - l["x0"], l["bottom"] - l["top"]
        if 8 <= w <= 30 and h <= 2.5 and not _is_blank(l.get("stroking_color")):
            marks.append((l["x0"], l["x1"], (l["top"] + l["bottom"]) / 2, _ckey(l["stroking_color"])))
    out = []
    marks = sorted(set(marks), key=lambda m: (round(m[2]), m[0]))
    for mi, (mx0, mx1, my, ck) in enumerate(marks):
        nxt = min([m[0] for m in marks if abs(m[2] - my) < 4 and m[0] > mx1 + 3] or [1e9])
        line = sorted([w for w in words if abs((w["top"] + w["bottom"]) / 2 - my) < 5
                       and mx1 - 1 <= w["x0"] < nxt - 1], key=lambda w: w["x0"])
        txt, last = [], mx1
        for w in line:
            if w["x0"] - last > 14:
                break
            txt.append(w["text"]); last = w["x1"]
        if txt:
            out.append({"color": ck, "nombre": " ".join(txt), "x": mx0, "y": my})
    return out


def _name_for(color, region, legends, used):
    cands = [l for l in legends if l["color"] == color]
    if not cands:
        return None
    cx, cy = (region["x0"] + region["x1"]) / 2, region["bottom"]
    cands.sort(key=lambda l: (abs(l["y"] - cy) + (0 if region["x0"] - 40 <= l["x"] <= region["x1"] + 40 else 400)))
    return cands[0]["nombre"]


def digitize_page(page) -> list[dict]:
    words = [w for w in page.extract_words(keep_blank_chars=False, use_text_flow=False)
             if w["x0"] >= 0 and w["top"] >= 0 and w["x1"] <= page.width and w["bottom"] <= page.height]
    axes = _y_axes(words, page.width)
    if not axes:
        return []
    legends = _legend(page, words)
    charts = []
    for reg in _regions(axes, page.width):
        ax = reg["axis"]
        to_v = lambda y: ax["a"] * y + ax["b"]
        zero_y = (0 - ax["b"]) / ax["a"] if ax["vmin"] <= 0 <= ax["vmax"] else ax["bottom"]
        W = reg["x1"] - reg["x0"]
        inside = lambda o: (reg["x0"] - 2 <= (o["x0"] + o["x1"]) / 2 <= reg["x1"] + 2
                            and reg["top"] - 6 <= (o["top"] + o["bottom"]) / 2 <= reg["bottom"] + 8)
        # ── eje X ──
        xl = []
        for w in words:
            if reg["x0"] - 12 <= (w["x0"] + w["x1"]) / 2 <= reg["x1"] + 12 and \
                    reg["bottom"] - 2 <= w["top"] <= reg["bottom"] + 55:
                pd_ = _parse_date(w["text"], bool(w.get("upright", True)))
                if pd_:
                    xl.append(((w["x0"] + w["x1"]) / 2, pd_[0], pd_[1], w["top"]))
        if xl:   # quedarse con la fila de rótulos más poblada
            rows = {}
            for t in xl:
                rows.setdefault(round(t[3] / 6), []).append(t)
            xl = sorted(max(rows.values(), key=len))
        gran = "Y" if xl and all(t[2] == "Y" for t in xl) else "M"
        tmap = None
        if len(xl) >= 2 and xl[-1][1] > xl[0][1]:
            o0, o1 = xl[0][1].toordinal(), xl[-1][1].toordinal()
            k = (o1 - o0) / (xl[-1][0] - xl[0][0])
            tmap = lambda x: date.fromordinal(int(round(o0 + k * (x - xl[0][0]))))

        def label_at(x):
            if gran == "Y" and xl:
                return str(min(xl, key=lambda t: abs(t[0] - x))[1].year)
            if tmap:
                d = date.fromordinal(tmap(x).toordinal() + 14)   # el rótulo (día 1) va centrado en la barra
                return f"{d.year}-{d.month:02d}"
            return f"@{(x - reg['x0']) / W:.4f}"          # sin rótulos legibles: posición relativa

        series = {}
        # ── barras (rects, o curvas de pocos puntos: hay PDFs que dibujan así cada barra) ──
        small = [c for c in page.curves if c.get("fill") and len(c.get("pts") or []) <= 8
                 and (c["x1"] - c["x0"]) > 0.8 and (c["bottom"] - c["top"]) > 1.5 * (c["x1"] - c["x0"]) * 0 + 0.3
                 and not (2.5 <= (c["x1"] - c["x0"]) <= 9 and 2.5 <= (c["bottom"] - c["top"]) <= 9)]
        for r in list(page.rects) + small:
            w, h = r["x1"] - r["x0"], r["bottom"] - r["top"]
            col = r.get("non_stroking_color")
            if _is_blank(col) or not inside(r) or w > 0.2 * W or h < 0.05:
                continue
            up = abs(r["top"] - zero_y) >= abs(r["bottom"] - zero_y)
            v = to_v(r["top"]) - to_v(r["bottom"]) if False else (to_v(r["top"]) if up else to_v(r["bottom"]))
            stacked_v = to_v(r["top"]) - to_v(r["bottom"])
            series.setdefault(("bar", _ckey(col)), []).append(((r["x0"] + r["x1"]) / 2, v, stacked_v, r["top"], r["bottom"]))
        # ── líneas y áreas ──
        seen = set()
        for c in page.curves:
            pts = c.get("pts") or []
            if len(pts) < 12 or (c["x1"] - c["x0"]) < 0.35 * W or not inside(c):
                continue
            filled = bool(c.get("fill"))
            col = c.get("non_stroking_color") if filled else c.get("stroking_color")
            if _is_blank(col):
                continue
            key = (_ckey(col), round(c["x0"]), round(c["x1"]), round(c["top"]), round(c["bottom"]))
            if key in seen:
                continue
            seen.add(key)
            kind = "area" if filled else "line"
            if kind == "line" and ("area", _ckey(col)) in series:
                continue          # contorno de un área ya capturada
            series.setdefault((kind, _ckey(col)), []).append(pts)

        out_series = []
        # muestreo X para curvas: rótulos (mensual) o rejilla regular
        if tmap and gran == "M":
            months, d = [], date(xl[0][1].year, xl[0][1].month, 1)
            while d <= xl[-1][1]:
                months.append(d); d = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
            step = max(1, len(months) // MAX_POINTS + (1 if len(months) % MAX_POINTS and len(months) > MAX_POINTS else 0))
            months = months[::step]
            kx = (xl[-1][0] - xl[0][0]) / (xl[-1][1].toordinal() - xl[0][1].toordinal())
            grid = [(xl[0][0] + kx * (m.toordinal() - xl[0][1].toordinal()), f"{m.year}-{m.month:02d}") for m in months]
        elif xl:
            grid = [(t[0], str(t[1].year) if gran == "Y" else f"{t[1].year}-{t[1].month:02d}") for t in xl]
        else:
            grid = [(reg["x0"] + W * k / 59, f"@{k / 59:.4f}") for k in range(60)]

        n_area = sum(1 for (k, _) in series if k == "area")
        for (kind, ck), items in series.items():
            pts_out = []
            if kind == "bar":
                # barras apiladas = varias barras del MISMO x en la región (otros colores)
                # apiladas = en la misma x hay barras de OTRO color → valor = grosor con signo
                other_x = [o[0] for (k2, c2), it2 in series.items() if k2 == "bar" and c2 != ck for o in it2]
                n_co = sum(1 for it in items if any(abs(ox - it[0]) < 0.6 for ox in other_x))
                is_stacked = n_co > 0.5 * len(items)
                other_x = other_x if is_stacked else []
                agg = {}
                for (x, v, sv, top, bot) in sorted(items):
                    lab = label_at(x)
                    stacked = is_stacked
                    if stacked:
                        v = sv if (top + bot) / 2 <= zero_y else -sv
                    agg.setdefault(lab, []).append(v)
                # varias piezas del mismo color en la misma etiqueta: apiladas se suman; si no, media
                pts_out = [[lab, round(sum(vs) if other_x else sum(vs) / len(vs), 3)] for lab, vs in agg.items()]
            else:
                for pts in items:
                    for gx, lab in grid:
                        if kind == "area":
                            sp = _poly_span(pts, gx)
                            if not sp:
                                continue
                            vt, vb = to_v(sp[0]), to_v(sp[1])
                            v = (vt - max(vb, 0.0)) if (n_area > 1) else vt
                        else:
                            y = _line_y(pts, gx)
                            if y is None:
                                continue
                            v = to_v(y)
                        pts_out.append([lab, round(v, 3)])
            vs_ = [p_[1] for p_ in pts_out]
            if len(pts_out) >= 2 and (max(vs_) - min(vs_)) > 0.004 * abs(ax["vmax"] - ax["vmin"]):
                out_series.append({"nombre_detectado": _name_for(ck, reg, legends, None), "color_pdf": ck,
                                   "color_hex": _hex(ck), "tipo": kind, "puntos": pts_out})
        if out_series:      # restos (glifos, marcas): series con muy pocos puntos frente a la principal
            nmax = max(len(s_["puntos"]) for s_ in out_series)
            out_series = [s_ for s_ in out_series if len(s_["puntos"]) >= 0.4 * nmax]
        if not out_series:
            continue
        title = " ".join(w["text"] for w in sorted(
            [w for w in words if reg["top"] - 34 <= w["bottom"] <= reg["top"] - 2
             and reg["x0"] - 30 <= w["x0"] <= reg["x1"] and not _NUM.match(w["text"])],
            key=lambda w: (round(w["top"] / 4), w["x0"])))[:120]
        charts.append({"id": None, "bbox": [round(reg["x0"]), round(reg["top"]), round(reg["x1"]), round(reg["bottom"])],
                       "titulo_detectado": title,
                       "eje_y": {"min": ax["vmin"], "max": ax["vmax"], "unidad": "%" if ax["pct"] else ""},
                       "eje_x": "anual" if (gran == "Y" and xl) else ("mensual" if tmap else "relativo"),
                       "apilado": n_area > 1,
                       "series": out_series})
    charts.sort(key=lambda c: (round(c["bbox"][1] / 40), c["bbox"][0]))
    for n, c in enumerate(charts, 1):
        c["id"] = f"p{page.page_number}#{n}"
    return charts


def digitize(pdf_path: str, pages: list[int]) -> dict:
    import pdfplumber
    out = {}
    with pdfplumber.open(pdf_path) as doc:
        for pn in pages:
            if 1 <= pn <= len(doc.pages):
                try:
                    out[str(pn)] = digitize_page(doc.pages[pn - 1])
                except Exception as e:          # nunca rompe la extracción
                    out[str(pn)] = {"error": f"{type(e).__name__}: {e}"}
    return out


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", required=True)
    ap.add_argument("--pages", required=True, help="p.ej. 23,26,33 o 20-49")
    ap.add_argument("--out")
    a = ap.parse_args()
    pgs = []
    for tok in a.pages.split(","):
        if "-" in tok:
            lo, hi = tok.split("-"); pgs += list(range(int(lo), int(hi) + 1))
        else:
            pgs.append(int(tok))
    res = digitize(a.pdf, pgs)
    txt = json.dumps(res, ensure_ascii=False, indent=1)
    if a.out:
        Path(a.out).write_text(txt, encoding="utf-8")
        for k, v in res.items():
            n = len(v) if isinstance(v, list) else 0
            print(f"p{k}: {n} gráfico(s)" + ("".join(f" | {c['titulo_detectado'][:40]} [{len(c['series'])} series]" for c in v) if n else ""))
    else:
        print(txt)
