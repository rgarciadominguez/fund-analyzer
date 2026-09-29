"""seguimiento_fondos.py — Avisos de seguimiento de los fondos TOP y BUENO de Rafa (29-sep-2026).

Rafa: "cada vez que se haga un análisis o re-análisis marcar fecha para sacar el annual report o la nueva
carta para leérmela (de los fondos buenos y top únicamente)".
  · ANNUAL REPORT nuevo (vence fecha_proximo_analisis = publicación del último AR + 13 meses): tarea
    "Re-analizar fondo: … — update anual: annual report y semestral nuevos, cartas y análisis externos del
    año; después leer los docs nuevos y la performance del año". Se cierra sola cuando el re-análisis
    rueda la fecha al futuro.
  · CARTA nueva (trimestral/semestral) sin annual report nuevo cerca: tarea "Leer carta {periodo} de {fondo}:
    {enlace}". Si hay patrón de URL de la gestora (data/known_manager_letters.json) se comprueba si ya está
    publicada; si no hay patrón o no aparece a las 7 semanas del cierre del periodo → tarea "Revisar si ha
    salido la carta {periodo}: {página de cartas}".

Solo fondos analizados (data/funds/{ISIN}/output.json) cuya clasificación en el portal (o la de alguna de
sus clases) sea Top o Bueno. Tareas en el portal vía POST /admin/tarea (lo mismo que usa el Copiloto).
Estado y deduplicación: data/_seguimiento_fondos.json.

Se ejecuta 1 vez al día desde tools.consume_inputs_rafa (que el guardián del servidor lanza cada hora).
CLI:  python -m tools.seguimiento_fondos [--dry-run] [--isin X] [--forzar]
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"
STATE = ROOT / "data" / "_seguimiento_fondos.json"
CLASIF_CACHE = ROOT / "data" / "_clasificacion_rafa.json"
KB_LETTERS = ROOT / "data" / "known_manager_letters.json"
SEGUIBLES = {"top", "bueno"}
GRACIA_CARTA_DIAS = 49          # 7 semanas tras el cierre del periodo: si no hay carta, pedir revisión
MARGEN_AR_DIAS = 45             # si el update anual vence en <45 días, la carta va dentro de él
_MESES = {3: "marzo", 6: "junio", 9: "septiembre", 12: "diciembre"}


def _log(m: str) -> None:
    print(f"[SEGUIMIENTO] {m}", flush=True)


def _load(p: Path, dflt):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dflt


def _save(p: Path, d) -> None:
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(p)


# ── portal ──────────────────────────────────────────────────────────────────────────────────────
def _portal():
    c = json.load(open(os.path.expanduser("~/.horizonte-portal.json"), encoding="utf-8"))
    base = c["base_url"].rstrip("/") + "/wp-json/horizonte/v1/"
    auth = "Basic " + base64.b64encode(f"{c['usuario']}:{c['app_password']}".encode()).decode()
    return base, auth


def _tarea(body: dict, dry: bool) -> int | None:
    if dry:
        _log(f"  (dry) tarea: {json.dumps(body, ensure_ascii=False)[:260]}")
        return -1
    base, auth = _portal()
    req = urllib.request.Request(base + "admin/tarea", data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
                                 method="POST", headers={"Authorization": auth,
                                                         "Content-Type": "application/json; charset=utf-8"})
    r = json.loads(urllib.request.urlopen(req, timeout=30).read() or b"{}")
    if not r.get("ok"):
        raise RuntimeError(str(r)[:200])
    return r.get("id")


def clasificaciones(rows: list | None = None) -> dict:
    """{isin: 'Top'|'Bueno'|...}. Usa las filas ya leídas por consume_inputs_rafa o las pide al portal;
    cachea en disco para funcionar si el portal no responde."""
    if rows is None:
        try:
            from tools.consume_inputs_rafa import fetch
            rows = (fetch(None) or {}).get("rows")
        except Exception as e:  # noqa: BLE001
            _log(f"[WARN] no pude leer clasificaciones del portal: {str(e)[:80]}")
    if rows:
        m = {str(r.get("isin")).upper(): r.get("clasificacion") for r in rows if r.get("isin") and r.get("clasificacion")}
        if m:
            if len(m) > 20:            # filas completas (no un 'since' parcial) → refresca la caché
                _save(CLASIF_CACHE, m)
            else:
                m = {**_load(CLASIF_CACHE, {}), **m}
            return m
    return _load(CLASIF_CACHE, {})


def _isins_del_fondo(isin: str, out: dict) -> set:
    s = {isin}
    for c in out.get("clases_documento") or []:
        if isinstance(c, dict) and c.get("isin"):
            s.add(str(c["isin"]).upper())
    for c in out.get("class_isins_known") or []:
        s.add(str(c).upper())
    return s


def es_seguible(isin: str, out: dict, clasif: dict) -> str | None:
    for i in _isins_del_fondo(isin, out):
        v = str(clasif.get(i) or "").strip().lower()
        if v in SEGUIBLES:
            return clasif.get(i)
    return None


# ── cartas ──────────────────────────────────────────────────────────────────────────────────────
def _parse_d(s) -> date | None:
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:
        return None


def _siguiente_periodo(ultimo: date, freq: str) -> tuple[str, date, dict]:
    """(etiqueta, fecha de cierre, variables de patrón) del periodo siguiente al último conocido."""
    if freq == "semiannual":
        h = 1 if ultimo.month <= 6 else 2
        y, h = (ultimo.year, 2) if h == 1 else (ultimo.year + 1, 1)
        fin = date(y, 6, 30) if h == 1 else date(y, 12, 31)
        return f"{y}-H{h}", fin, {"YYYY": y, "H": h, "Q": 2 * h, "MES": _MESES[6 * h]}
    q = (ultimo.month - 1) // 3 + 1
    y, q = (ultimo.year, q + 1) if q < 4 else (ultimo.year + 1, 1)
    mes = 3 * q
    fin = (date(y, mes + 1, 1) - timedelta(days=1)) if mes < 12 else date(y, 12, 31)
    return f"{y}-Q{q}", fin, {"YYYY": y, "Q": q, "H": 1 if q <= 2 else 2, "MES": _MESES[mes]}


def _kb_de(isin: str) -> dict:
    for _, g in (_load(KB_LETTERS, {}).get("funds") or {}).items():
        if isin in [str(x).upper() for x in g.get("isins") or []]:
            return g
    return {}


def _urls_candidatas(kb: dict, v: dict) -> list[str]:
    urls = []
    for u in re.findall(r"https?://\S+", str(kb.get("pattern") or "")):
        u = u.rstrip(").,;'\"")
        if "*" in u or "{N}" in u:
            continue
        if not re.search(r"\{(YYYY|Q|H|MES)\}", u):
            continue
        for a, b in (("{YYYY}", str(v["YYYY"])), ("{Q}", str(v["Q"])), ("{H}", str(v["H"])), ("{MES}", v["MES"])):
            u = u.replace(a, b)
        if "{" not in u:
            urls.append(u)
    return urls


def _publicada(url: str) -> bool:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        with urllib.request.urlopen(req, timeout=25) as r:
            if r.status != 200:
                return False
            ct = (r.headers.get("Content-Type") or "").lower()
            head = r.read(2048)
            return head.startswith(b"%PDF") or "pdf" in ct or ("html" in ct and len(head) > 500)
    except Exception:
        return False


# ── ciclo ───────────────────────────────────────────────────────────────────────────────────────
def revisar(dry: bool = False, solo: str | None = None, rows: list | None = None) -> dict:
    hoy = date.today()
    clasif = clasificaciones(rows)
    st = _load(STATE, {"ar": {}, "cartas": {}})
    st.setdefault("ar", {}); st.setdefault("cartas", {})
    res = {"seguibles": 0, "tareas_ar": 0, "tareas_carta": 0, "cerradas": 0}
    for d in sorted(FUNDS.iterdir()):
        isin = d.name.upper()
        if solo and isin != solo.upper():
            continue
        out = _load(d / "output.json", None)
        if not isinstance(out, dict) or not re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}\d", isin):
            continue
        cl = es_seguible(isin, out, clasif)
        if not cl:
            continue
        res["seguibles"] += 1
        nombre = (out.get("nombre") or isin).strip()

        # 1) ANNUAL REPORT / update anual
        fpa = _parse_d(out.get("fecha_proximo_analisis"))
        if not fpa:
            try:
                from tools.next_analysis_date import compute
                fpa = _parse_d(compute(isin).get("fecha_proximo_analisis"))
            except Exception:
                fpa = None
        prev = st["ar"].get(isin)
        if prev and fpa and _parse_d(prev.get("fecha")) and fpa > _parse_d(prev["fecha"]):
            # re-analizado: la fecha rodó → cerrar la tarea anterior
            try:
                if prev.get("id") and prev["id"] > 0:
                    _tarea({"id": prev["id"], "hecha": True}, dry)
                res["cerradas"] += 1
            except Exception as e:  # noqa: BLE001
                _log(f"[WARN] no pude cerrar la tarea {prev.get('id')} de {isin}: {str(e)[:80]}")
            st["ar"].pop(isin, None); prev = None
        if fpa and not prev and hoy > fpa + timedelta(days=1):
            # ya vencido antes de este vigilante: el portal ya tiene su tarea "Re-analizar fondo" → no duplicar
            st["ar"][isin] = {"id": 0, "fecha": fpa.isoformat(), "creada": hoy.isoformat(), "nota": "tarea del portal"}
            prev = st["ar"][isin]
        if fpa and not prev and hoy >= fpa - timedelta(days=1):
            titulo = (f"Re-analizar fondo: {nombre} ({isin}) — update anual: annual report y semestral nuevos, "
                      f"cartas y análisis externos del año; después leer los docs nuevos y la performance del año")
            try:
                tid = _tarea({"titulo": titulo[:240], "fecha": fpa.isoformat()}, dry)
                st["ar"][isin] = {"id": tid, "fecha": fpa.isoformat(), "creada": hoy.isoformat()}
                res["tareas_ar"] += 1
                _log(f"{isin} ({cl}): tarea de update anual (vence {fpa})")
            except Exception as e:  # noqa: BLE001
                _log(f"[WARN] {isin}: no pude crear la tarea de update anual: {str(e)[:100]}")

        # 2) CARTA nueva sin annual report nuevo cerca
        if fpa and (fpa - hoy).days < MARGEN_AR_DIAS:
            continue                                   # la carta entra en el update anual
        kb = _kb_de(isin)
        if not kb.get("letters_page") and not kb.get("pattern"):
            # Solo fondos con la página de cartas de su gestora verificada (known_manager_letters.json, que
            # rellena letters-sourcing-cowork en cada full/annual_update). Sin eso el aviso sería una búsqueda
            # en Google sobre cartas que a veces ni son del fondo (contaminadas): ruido, no seguimiento.
            continue
        try:
            from tools.publication_calendar import build_publication_calendar
            pc = build_publication_calendar(isin) or {}
        except Exception:
            pc = out.get("publication_calendar") or {}
        lc = pc.get("quarterly_letters") or pc.get("letters") or {}
        ultimo = _parse_d(lc.get("last_known_date"))
        if not ultimo:
            continue
        freq = "semiannual" if str(lc.get("frequency")) == "semiannual" else "quarterly"
        periodo, fin, v = _siguiente_periodo(ultimo, freq)
        if hoy <= fin:
            continue                                   # el periodo aún no ha cerrado
        # Si la última carta conocida es antigua (el fondo no publica al día o no la tenemos), no se piden
        # revisiones de periodos pasados: solo se busca la del ÚLTIMO periodo cerrado, y solo por patrón.
        atrasado = False
        while True:
            p2, f2, v2 = _siguiente_periodo(fin, freq)
            if f2 >= hoy:
                break
            periodo, fin, v, atrasado = p2, f2, v2, True
        cst = st["cartas"].setdefault(isin, {})
        ya = cst.get(periodo) or {}
        if ya.get("estado") == "leer":
            continue
        encontrada = next((u for u in _urls_candidatas(kb, v) if _publicada(u)), None)
        try:
            if encontrada:
                tid = _tarea({"titulo": f"Leer carta {periodo} de {nombre} ({isin}): {encontrada}"[:240],
                              "fecha": hoy.isoformat()}, dry)
                if ya.get("estado") == "revisar" and ya.get("id", 0) > 0:
                    _tarea({"id": ya["id"], "hecha": True}, dry)
                cst[periodo] = {"estado": "leer", "id": tid, "url": encontrada, "fecha": hoy.isoformat()}
                res["tareas_carta"] += 1
                _log(f"{isin} ({cl}): carta {periodo} publicada → {encontrada}")
            elif not ya and not atrasado and (hoy - fin).days >= GRACIA_CARTA_DIAS:
                enlace = kb.get("letters_page") or kb.get("letters_page_alt") or "(página de cartas no registrada)"
                tid = _tarea({"titulo": f"Revisar si ha salido la carta {periodo} de {nombre} ({isin}) y leerla: {enlace}"[:240],
                              "fecha": hoy.isoformat()}, dry)
                cst[periodo] = {"estado": "revisar", "id": tid, "fecha": hoy.isoformat()}
                res["tareas_carta"] += 1
                _log(f"{isin} ({cl}): carta {periodo} sin localizar → revisar en {enlace}")
        except Exception as e:  # noqa: BLE001
            _log(f"[WARN] {isin}: no pude crear la tarea de la carta {periodo}: {str(e)[:100]}")
    if not dry:
        st["ultima_revision"] = datetime.now().isoformat(timespec="seconds")
        _save(STATE, st)
    _log(f"FIN: {res}")
    return res


def revisar_si_toca(rows: list | None = None) -> None:
    """1 vez al día (llamado desde consume_inputs_rafa, que corre cada hora)."""
    st = _load(STATE, {})
    if str(st.get("ultima_revision", ""))[:10] == date.today().isoformat():
        return
    try:
        revisar(rows=rows)
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] seguimiento falló (no crítico): {str(e)[:120]}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--isin")
    a = ap.parse_args()
    revisar(dry=a.dry_run, solo=a.isin)
