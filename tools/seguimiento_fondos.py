"""seguimiento_fondos.py — Agenda de seguimiento de los fondos TOP y BUENO de Rafa (29-sep-2026).

Rafa: "no quiero revisión diaria sino un sistema que tenga fechas aproximadas donde seguro que ya hay annual
report o carta, de cara a mandarme aviso para leer o lanzar la actualización anual".

Cómo funciona (sin procesos diarios): en CADA análisis o re-análisis de un fondo se PLANIFICAN sus avisos y
se AGENDAN (data/_seguimiento_fondos.json) y cada tarea se crea en el portal el día que toca
(disparar(), en la pasada horaria de consume_inputs_rafa) llega su fecha y se BUSCAN las novedades
(tools.novedades), que aparecen en la pantalla "Seguimiento de fondos" del portal:
  · UPDATE ANUAL — fecha_proximo_analisis (tools.next_analysis_date: cierre fiscal del último AR + 1 año +
    plazo de publicación + 30 d de margen → el AR nuevo ya está publicado seguro). Tarea "Re-analizar fondo:
    … — lanzar update anual: …". El título empieza por "Re-analizar fondo:" + ISIN para que el portal no
    cree la suya genérica encima.
  · CARTAS — cada carta trimestral/semestral que saldrá ANTES de ese update anual (las posteriores entran en
    él): fecha = cierre del periodo + margen (trimestral 50 d, semestral 75 d, o `margen_carta_dias` de la
    KB de la gestora). Tarea "Leer carta {periodo} de {fondo}: {enlace}". Solo fondos con la página de cartas
    de su gestora verificada en data/known_manager_letters.json (la rellena letters-sourcing-cowork).
Re-planificar sustituye: las tareas futuras que ya no tocan se cierran y las que cambian se editan; las ya
vencidas y sin hacer se respetan (son de Rafa). Si un fondo deja de ser Top/Bueno, sus tareas futuras se cierran.

Solo fondos cuya clasificación en el portal (o la de alguna de sus clases) sea Top o Bueno.
Estado: data/_seguimiento_fondos.json {isin: {clave: {id, fecha, titulo}}}.

Disparadores:
  · tools.sync_to_supabase (fin de cada análisis, incluso con Supabase caído) → planificar(isin)
  · tools.consume_inputs_rafa → si cambia la clasificación de un fondo analizado → planificar(isin)
CLI:  python -m tools.seguimiento_fondos --isin X [--dry-run]   |   --todos [--dry-run]   |   --ver
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"
STATE = ROOT / "data" / "_seguimiento_fondos.json"
CLASIF_CACHE = ROOT / "data" / "_clasificacion_rafa.json"
KB_LETTERS = ROOT / "data" / "known_manager_letters.json"
SEGUIBLES = {"top", "bueno"}
MARGEN_CARTA = {"quarterly": 50, "semiannual": 75}   # días tras el cierre del periodo: la carta ya ha salido
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


def _parse_d(s) -> date | None:
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:
        return None


# ── portal ──────────────────────────────────────────────────────────────────────────────────────
def _tarea(body: dict, dry: bool) -> int | None:
    if dry:
        _log(f"  (dry) {json.dumps(body, ensure_ascii=False)[:230]}")
        return -1
    c = json.load(open(os.path.expanduser("~/.horizonte-portal.json"), encoding="utf-8"))
    auth = "Basic " + base64.b64encode(f"{c['usuario']}:{c['app_password']}".encode()).decode()
    req = urllib.request.Request(c["base_url"].rstrip("/") + "/wp-json/horizonte/v1/admin/tarea",
                                 data=json.dumps(body, ensure_ascii=False).encode("utf-8"), method="POST",
                                 headers={"Authorization": auth, "Content-Type": "application/json; charset=utf-8"})
    r = json.loads(urllib.request.urlopen(req, timeout=30).read() or b"{}")
    if not r.get("ok"):
        raise RuntimeError(str(r)[:200])
    return r.get("id")


# ── clasificación ───────────────────────────────────────────────────────────────────────────────
def clasificaciones(rows: list | None = None) -> dict:
    """{ISIN: 'Top'|'Bueno'|...}. Filas ya leídas del portal (consume_inputs_rafa) o lectura completa;
    caché en disco para funcionar si el portal no responde."""
    cache = _load(CLASIF_CACHE, {})
    if rows is None and not cache:
        try:
            from tools.consume_inputs_rafa import fetch
            rows = (fetch(None) or {}).get("rows")
        except Exception as e:  # noqa: BLE001
            _log(f"[WARN] no pude leer clasificaciones del portal: {str(e)[:80]}")
    if rows:
        cache = {**cache, **{str(r.get("isin")).upper(): r.get("clasificacion")
                             for r in rows if r.get("isin") and r.get("clasificacion")}}
        _save(CLASIF_CACHE, cache)
    return cache


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
        v = str(clasif.get(i) or "").strip()
        if v.lower() in SEGUIBLES:
            return v
    return None


# ── calendario de avisos ────────────────────────────────────────────────────────────────────────
def _siguiente_periodo(ultimo: date, freq: str) -> tuple[str, date, dict]:
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


def _url_predicha(kb: dict, v: dict) -> str | None:
    for u in re.findall(r"https?://\S+", str(kb.get("pattern") or "")):
        u = u.rstrip(").,;'\"")
        if "*" in u or "{N}" in u or not re.search(r"\{(YYYY|Q|H|MES)\}", u):
            continue
        for a, b in (("{YYYY}", str(v["YYYY"])), ("{Q}", str(v["Q"])), ("{H}", str(v["H"])), ("{MES}", v["MES"])):
            u = u.replace(a, b)
        if "{" not in u:
            return u
    return None


def _fin_de_periodo(p: str) -> date | None:
    m = re.match(r"(\d{4})-(Q([1-4])|H([12])|(\d{2}))$", p.strip())
    if not m:
        return None
    y = int(m.group(1))
    mes = 3 * int(m.group(3)) if m.group(3) else (6 * int(m.group(4)) if m.group(4) else int(m.group(5)))
    return (date(y, mes + 1, 1) - timedelta(days=1)) if mes < 12 else date(y, 12, 31)


def plan(isin: str, out: dict, hoy: date | None = None) -> dict:
    """{clave: {fecha, titulo}} de los avisos futuros del fondo (sin IO de portal)."""
    hoy = hoy or date.today()
    nombre = (out.get("nombre") or isin).strip()
    avisos: dict = {}
    fpa = _parse_d(out.get("fecha_proximo_analisis"))
    if not fpa:
        try:
            from tools.next_analysis_date import compute
            fpa = _parse_d(compute(isin).get("fecha_proximo_analisis"))
        except Exception:
            fpa = None
    if fpa and fpa > hoy:          # ya vencido: el portal ya tiene su tarea "Re-analizar fondo" (no duplicar)
        avisos["update_anual"] = {
            "fecha": fpa.isoformat(),
            "titulo": (f"Re-analizar fondo: {nombre} ({isin}) — lanzar update anual: annual report y semestral "
                       f"nuevos, cartas y análisis externos del año; después leer los docs nuevos y la "
                       f"performance del año")[:240]}
    kb = _kb_de(isin)
    if not (kb.get("letters_page") or kb.get("pattern")):
        return avisos
    try:
        from tools.publication_calendar import build_publication_calendar
        pc = build_publication_calendar(isin) or {}
    except Exception:
        pc = out.get("publication_calendar") or {}
    lc = pc.get("quarterly_letters") or pc.get("letters") or {}
    ultimo = _parse_d(lc.get("last_known_date"))
    if ultimo and ultimo > hoy:
        ultimo = None                               # fecha futura = dato mal leído del calendario (Carmignac)
    kb_per = [str(x.get("periodo") or "") for x in kb.get("letters") or [] if isinstance(x, dict)]
    ult_kb = _fin_de_periodo(max(kb_per)) if kb_per else None
    if ult_kb and ult_kb <= hoy and (not ultimo or ult_kb > ultimo):
        ultimo = ult_kb
    if not ultimo:
        return avisos
    txt_kb = (str(kb.get("pattern") or "") + " " + str(kb.get("publica") or "")).lower()
    if "{h}" in txt_kb or "semestral" in txt_kb or "semiannual" in txt_kb or re.search(r"\d{4}-H[12]", " ".join(kb_per)):
        freq = "semiannual"
    elif "{q}" in txt_kb or "trimestral" in txt_kb or "quarterly" in txt_kb:
        freq = "quarterly"
    else:
        freq = "semiannual" if str(lc.get("frequency")) == "semiannual" else "quarterly"
    margen = int(kb.get("margen_carta_dias") or MARGEN_CARTA[freq])
    tope = (fpa - timedelta(days=30)) if fpa else hoy + timedelta(days=400)
    fin = ultimo
    for _ in range(12):
        periodo, fin, v = _siguiente_periodo(fin, freq)
        cuando = fin + timedelta(days=margen)
        if cuando > tope:
            break                                   # esa carta entra en el update anual
        if cuando < hoy - timedelta(days=7):
            continue                                # periodo pasado: ya lo cubrió la búsqueda de cartas del análisis
        enlace = kb.get("letters_page") or _url_predicha(kb, v) or kb.get("letters_page_alt") or ""
        avisos[f"carta_{periodo}"] = {"fecha": max(cuando, hoy).isoformat(),
                                      "titulo": f"Leer carta {periodo} de {nombre} ({isin}): {enlace}"[:240]}
    return avisos


# ── aplicar ─────────────────────────────────────────────────────────────────────────────────────
def planificar(isin: str, dry: bool = False, clasif: dict | None = None, hoy: date | None = None) -> dict:
    hoy = hoy or date.today()
    isin = isin.upper()
    out = _load(FUNDS / isin / "output.json", None)
    st = _load(STATE, {})
    prev = st.get(isin) or {}
    if not isinstance(out, dict):
        return {"isin": isin, "motivo": "sin análisis"}
    cl = es_seguible(isin, out, clasif if clasif is not None else clasificaciones())
    hermano = _hermano_ya_seguido(isin, out, st)
    if hermano:
        _log(f"{isin}: mismo fondo que {hermano}, que ya tiene sus avisos → no se duplican")
        cl = None
    nuevo = plan(isin, out, hoy) if cl else {}
    res = {"isin": isin, "clasificacion": cl, "creadas": 0, "editadas": 0, "cerradas": 0}
    guardado = {}
    # AGENDA: los avisos futuros se guardan aquí y la tarea se crea en el portal EL DÍA QUE TOCA
    # (disparar()); así la lista de tareas de Rafa no se llena de avisos de dentro de meses.
    for k, a in nuevo.items():
        p = prev.get(k)
        if p and (p.get("hecho") or p.get("buscando")):
            guardado[k] = p                     # ya buscado (o buscándose): se conserva
        else:
            if not p or p.get("fecha") != a["fecha"] or p.get("titulo") != a["titulo"]:
                res["creadas" if not p else "editadas"] += 1
            guardado[k] = {**a, "id": None}
    for k, p in prev.items():
        if k not in nuevo and not p.get("hecho"):
            res["cerradas"] += 1                # aviso agendado que ya no toca: se quita de la agenda
    if not dry:
        st = _load(STATE, {})
        if guardado:
            st[isin] = guardado
        else:
            st.pop(isin, None)
        _save(STATE, st)
    if res["creadas"] or res["editadas"] or res["cerradas"] or dry:
        _log(f"{isin} ({cl or 'no seguible'}): creadas {res['creadas']} · editadas {res['editadas']} · "
             f"cerradas {res['cerradas']} → " + ", ".join(f"{k} {a['fecha']}" for k, a in nuevo.items()))
    return res


def disparar(dry: bool = False, hoy: date | None = None) -> int:
    """Cuando llega la fecha de un aviso, BUSCA las novedades del fondo (tools.novedades: annual/semiannual,
    cartas, análisis externos, entrevistas, noticias desde el último análisis), las guarda y las manda a la
    pantalla "Seguimiento de fondos" del portal. Una búsqueda por pasada, en segundo plano y solo con la cola
    de análisis parada. Si aún no ha salido nada, reintenta cada semana hasta 6 semanas después de la fecha."""
    hoy = hoy or date.today()
    st = _load(STATE, {})
    due = []
    for isin, avs in st.items():
        for k, a in avs.items():
            f = _parse_d(a.get("fecha"))
            prox = _parse_d(a.get("proximo_intento"))
            if a.get("hecho") or a.get("buscando") or not f or f > hoy or (prox and prox > hoy):
                continue
            due.append((f, isin, k))
    if not due:
        return 0
    q = _load(ROOT / "data" / "queue_state.json", {}) or {}
    if any(i.get("status") in ("running", "queued", "paused_waiting_tokens") for i in q.get("items") or []):
        return 0
    if any(a.get("buscando") and (date.today() - (_parse_d(a.get("buscando")) or date.today())).days < 1
           for avs in st.values() for a in avs.values()):
        return 0                                    # ya hay una búsqueda en marcha
    _, isin, k = sorted(due)[0]
    if dry:
        _log(f"(dry) buscaría novedades de {isin} por {k}")
        return 1
    st[isin][k]["buscando"] = hoy.isoformat()
    _save(STATE, st)
    flags = (0x00000008 | 0x00000200 | 0x08000000) if os.name == "nt" else 0
    import subprocess
    exe = Path(sys.executable)
    pyw = exe.with_name("pythonw.exe")
    subprocess.Popen([str(pyw if pyw.exists() else exe), "-m", "tools.seguimiento_fondos", "--buscar", isin, "--clave", k],
                     cwd=str(ROOT), creationflags=flags, close_fds=True,
                     stdout=open(ROOT / "logs" / "seguimiento_fondos.log", "a", encoding="utf-8"), stderr=subprocess.STDOUT)
    _log(f"{isin}: buscando novedades ({k})")
    return 1


def buscar_y_registrar(isin: str, clave: str) -> None:
    from tools.novedades import buscar
    r = buscar(isin, clave)
    st = _load(STATE, {})
    a = (st.get(isin) or {}).get(clave)
    if a is None:
        return
    a.pop("buscando", None)
    hoy = date.today()
    f = _parse_d(a.get("fecha")) or hoy
    n = int(r.get("n") or 0)
    if r.get("ok") and n > 0:
        a["hecho"] = hoy.isoformat(); a["n_docs"] = n
    elif (hoy - f).days >= 42:
        a["hecho"] = hoy.isoformat(); a["n_docs"] = 0     # el portal ya muestra qué no se encontró
    else:
        a["proximo_intento"] = (hoy + timedelta(days=7)).isoformat()
    _save(STATE, st)


def _norm(n: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (n or "").lower())


def _hermano_ya_seguido(isin: str, out: dict, st: dict) -> str | None:
    """Otra clase del MISMO fondo (ISIN compartido o mismo nombre) que ya tiene avisos planificados."""
    mios = _isins_del_fondo(isin, out)
    nom = _norm(out.get("nombre"))
    for otro in st:
        if otro == isin or not st.get(otro):
            continue
        o = _load(FUNDS / otro / "output.json", None)
        if not isinstance(o, dict):
            continue
        if (_isins_del_fondo(otro, o) & mios) or (nom and _norm(o.get("nombre")) == nom):
            return otro
    return None


def clasificacion_cambiada(rows: list) -> None:
    """Desde consume_inputs_rafa: re-planifica los fondos analizados cuya clasificación ha cambiado."""
    antes = _load(CLASIF_CACHE, {})
    cambios = {str(r.get("isin")).upper() for r in rows or []
               if r.get("isin") and r.get("clasificacion") and antes.get(str(r["isin"]).upper()) != r["clasificacion"]}
    clasif = clasificaciones(rows)
    if not cambios or not antes:
        return                                       # sin caché previa no hay "cambio": se usa --todos una vez
    for d in FUNDS.iterdir():
        out = _load(d / "output.json", None)
        if isinstance(out, dict) and _isins_del_fondo(d.name.upper(), out) & cambios:
            try:
                planificar(d.name, clasif=clasif)
            except Exception as e:  # noqa: BLE001
                _log(f"[WARN] {d.name}: {str(e)[:100]}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin")
    ap.add_argument("--todos", action="store_true")
    ap.add_argument("--ver", action="store_true")
    ap.add_argument("--disparar", action="store_true")
    ap.add_argument("--buscar")
    ap.add_argument("--clave", default="update_anual")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    if a.ver:
        for i, avs in sorted(_load(STATE, {}).items()):
            for k, x in sorted(avs.items(), key=lambda kv: kv[1].get("fecha", "")):
                print(x.get("fecha"), i, k, "·", x.get("titulo", "")[:90])
        sys.exit(0)
    if a.buscar:
        buscar_y_registrar(a.buscar.upper(), a.clave); sys.exit(0)
    if a.disparar:
        print(disparar(dry=a.dry_run)); sys.exit(0)
    if a.todos:
        from tools.consume_inputs_rafa import fetch
        cl = clasificaciones((fetch(None) or {}).get("rows"))
        tot = {"creadas": 0, "editadas": 0, "cerradas": 0, "seguibles": 0}
        for d in sorted(FUNDS.iterdir()):
            if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{9}\d", d.name) and (d / "output.json").exists():
                r = planificar(d.name, dry=a.dry_run, clasif=cl)
                tot["seguibles"] += 1 if r.get("clasificacion") else 0
                for k in ("creadas", "editadas", "cerradas"):
                    tot[k] += r.get(k, 0)
        _log(f"FIN: {tot}")
    elif a.isin:
        print(planificar(a.isin, dry=a.dry_run))
