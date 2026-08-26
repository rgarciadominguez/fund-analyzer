"""
portal_analyze_worker.py — Cierra el flujo "analizar fondo" desde el portal Horizonte.

El portal encola un ISIN (botón «Análisis de fondo nuevo» → tabla assets, needs_review=1).
La cola se lee en:
    GET {base}/wp-json/horizonte/v1/admin/assets/pendientes-analisis   (Basic-Auth admin)

Este worker la SONDEA, y por cada ISIN pendiente hace el ciclo completo, sin que Rafa
tenga que activar nada a mano:

  1. (opcional) despierta Supabase        → tools.supabase_maintenance --keepalive
  2. MÉTRICAS (Morningstar, no necesita análisis):
        export_quant_feed --lista <tmp con el/los ISIN> --out <tmp_dir>   → metricas.json ACOTADO
        portal_push --file <tmp>/metricas.json --endpoint sync-metricas   (upsert por ISIN)
  3. ANÁLISIS COMPLETO (cualitativo + clases + Supabase):
        analizar_fondo.bat <ISIN>          (pipeline cowork; ~minutos)
        export_horfin_catalog              → refresca catalogo_supabase.json
        build_asset_classes                → asset_classes_por_isin.json
        portal_push --clases               (upsert por ISIN)

El fondo, al recibir métricas, DESAPARECE de la cola (el endpoint excluye ISIN con
asset_metrics.rentab_5a) y aparece en «Categorizar activos» para que Rafa lo clasifique
(el sync NUNCA toca clasificacion_horfin — eso es de Rafa).

Credenciales en .env (NO en el chat): PORTAL_BASE_URL / PORTAL_APP_USER / PORTAL_APP_PASSWORD.

CLI:
    python -m tools.portal_analyze_worker --once              # una pasada de la cola (limit por defecto)
    python -m tools.portal_analyze_worker --isin LU1815330277 # fuerza un ISIN concreto
    python -m tools.portal_analyze_worker --loop 300          # sondea cada 300 s
    python -m tools.portal_analyze_worker --once --limit 3    # hasta 3 fondos por pasada
    python -m tools.portal_analyze_worker --once --metrics-only   # solo métricas (sin el bat, rápido)
    python -m tools.portal_analyze_worker --once --dry-run    # enseña qué haría, no ejecuta
Flags útiles: --wake (despierta Supabase antes), --no-push (analiza pero no empuja).

SEGURIDAD DE COSTE: --limit por defecto = 1 (una pasada analiza como mucho 1 fondo).
Súbelo a conciencia; cada análisis completo tarda minutos y usa Claude Max.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
HORFIN = Path(r"C:\Users\RafaelGarcía\horizonte-datos")
LOG = ROOT / "logs" / "portal_analyze_worker.log"
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")
WEB_BASE = "http://127.0.0.1:5000"   # el server local del catálogo (web_server.py)


# ─────────────────────────── util ───────────────────────────
def _stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    line = f"[{_stamp()}] {msg}"
    print(line, flush=True)
    try:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        with LOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _cfg():
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    base = os.environ.get("PORTAL_BASE_URL", "").strip().rstrip("/")
    user = os.environ.get("PORTAL_APP_USER", "").strip()
    pwd = os.environ.get("PORTAL_APP_PASSWORD", "").strip()
    missing = [n for n, v in (("PORTAL_BASE_URL", base), ("PORTAL_APP_USER", user),
                              ("PORTAL_APP_PASSWORD", pwd)) if not v]
    if missing:
        raise RuntimeError(f"Faltan en .env: {', '.join(missing)}")
    return base, user, pwd


def _auth_header(user: str, pwd: str) -> dict:
    token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def fetch_queue() -> list[dict]:
    """Devuelve [{isin, name, needs_review}] que Rafa marcó para análisis con el botón
    (cola EXPLÍCITA `cola-analisis`, no el backlog de fondos sin métricas)."""
    base, user, pwd = _cfg()
    url = f"{base}/wp-json/horizonte/v1/admin/assets/cola-analisis"
    r = httpx.get(url, headers=_auth_header(user, pwd), timeout=60)
    r.raise_for_status()
    data = r.json()
    out = []
    for p in data.get("pendientes", []):
        iv = (p.get("isin") or "").upper().strip()
        if _ISIN.match(iv):
            # scope: "full" (default) o "annual_update" (solo el delta del último año)
            scope = (p.get("scope") or "full").strip().lower()
            out.append({"isin": iv, "name": p.get("name") or "",
                        "needs_review": p.get("needs_review"), "scope": scope,
                        # material aportado por Rafa al actualizar (docs profesionales /
                        # análisis externos) → fuente prioritaria para el análisis
                        "docs_aportados": p.get("docs_aportados") or [],
                        "analisis_externos": p.get("analisis_externos") or []})
    return out


def mark_done(isin: str, ok: bool = True, dry: bool = False) -> None:
    """Limpia el marcador en el portal para que no se re-analice en bucle. `ok`=análisis con
    informe (el portal pondrá needs_review=1 solo si ok, para no meter fallidos en Categorizar)."""
    if dry:
        log(f"  [dry] mark_done {isin} (ok={ok})")
        return
    try:
        base, user, pwd = _cfg()
        url = f"{base}/wp-json/horizonte/v1/admin/assets/analisis-hecho"
        h = _auth_header(user, pwd); h["Content-Type"] = "application/json"
        httpx.post(url, headers=h, content=json.dumps({"isin": isin, "ok": bool(ok)}), timeout=30)
    except Exception as e:  # noqa: BLE001
        log(f"  [WARN] no pude marcar hecho {isin}: {e}")


def mark_started(isin: str, fase: str = "analizando", dry: bool = False) -> None:
    """Avisa al portal que EMPIEZA el análisis (y la fase) → badge "Analizando…" en el admin."""
    _HB_STATE["isin"] = isin; _HB_STATE["fase"] = fase
    if dry:
        return
    try:
        base, user, pwd = _cfg()
        url = f"{base}/wp-json/horizonte/v1/admin/assets/analisis-empezado"
        h = _auth_header(user, pwd); h["Content-Type"] = "application/json"
        httpx.post(url, headers=h, content=json.dumps({"isin": isin, "fase": fase}), timeout=20)
    except Exception as e:  # noqa: BLE001
        log(f"  [WARN] no pude marcar empezado {isin}: {e}")


def _doc_dates(isin: str) -> dict:
    """Fechas de los últimos documentos (annual/semianual/carta del gestor) desde
    data/funds/{ISIN}/output.json → publication_calendar (ya las calcula publication_calendar.py).
    Gating de fiabilidad: AR si confidence != 'low'; carta solo confidence 'high' y NO futura;
    semianual si existe. Devuelve solo las presentes (el portal las muestra como mes-año)."""
    out: dict = {}
    try:
        p = ROOT / "data" / "funds" / isin / "output.json"
        if not p.exists():
            return out
        cal = (json.loads(p.read_text(encoding="utf-8")).get("publication_calendar") or {})
        ar = cal.get("annual_report") or {}
        if ar.get("last_known_date") and ar.get("confidence") != "low":
            out["fecha_annual_report"] = ar["last_known_date"]
        semi = cal.get("semiannual_report") or {}
        if semi.get("last_known_date"):
            out["fecha_semianual"] = semi["last_known_date"]
        ql = cal.get("quarterly_letters") or {}
        d = ql.get("last_known_date")
        if d and ql.get("confidence") == "high":
            try:
                if datetime.fromisoformat(str(d)[:10]).date() <= datetime.now().date():
                    out["fecha_carta_gestor"] = d
            except Exception:  # noqa: BLE001
                pass
    except Exception:  # noqa: BLE001
        pass
    return out


# Vocabulario de `tipo` que espera la pestaña Documentos del portal. El manifiesto de
# archive_docs usa nombres internos ligeramente distintos → se mapean aquí (el resto pasa tal cual).
_DOC_TIPO_PORTAL = {
    "semi_annual_report": "semiannual_report",
    "semiannual_report": "semiannual_report",
    "quarterly_letter": "carta_gestor",
    "carta_gestor": "carta_gestor",
    "annual_report": "annual_report",
    "kid": "kid",
    "kiid": "kid",
    "folleto": "folleto",
    "prospectus": "folleto",
    "prospecto": "folleto",
}
_DOC_TIPO_LABEL = {
    "annual_report": "Annual Report", "semiannual_report": "Informe semestral",
    "carta_gestor": "Carta del gestor", "kid": "KID", "folleto": "Folleto",
}


def _doc_list(g: dict, nombre_fondo: str = "") -> list:
    """Lista de documentos para la pestaña Documentos del portal, desde el manifiesto ya
    archivado en Supabase (`fund_groups.portfolio_metrics_jsonb.documentos`), que ya trae
    las URLs de Storage. Solo normaliza el vocabulario de `tipo` y compone un `nombre`
    legible si el guardado es un nombre de fichero. NO inventa nada: si no hay manifiesto,
    devuelve []."""
    pm = g.get("portfolio_metrics_jsonb") or {}
    if not isinstance(pm, dict):
        return []
    docs = pm.get("documentos") or []
    out = []
    for d in docs:
        if not isinstance(d, dict) or not (d.get("url") or d.get("url_original")):
            continue
        tipo_raw = str(d.get("tipo") or "").strip()
        tipo = _DOC_TIPO_PORTAL.get(tipo_raw, tipo_raw or "documento")
        periodo = d.get("periodo")
        nombre = d.get("nombre") or ""
        # nombre legible si el guardado es solo el fichero (acaba en .pdf) y tenemos contexto
        if (not nombre or nombre.lower().endswith(".pdf")) and nombre_fondo:
            etq = _DOC_TIPO_LABEL.get(tipo, tipo.replace("_", " ").title())
            nombre = f"{nombre_fondo} — {etq}{(' ' + str(periodo)) if periodo else ''}"
        out.append({
            "tipo": tipo,
            "periodo": periodo,
            "fecha_publicacion": d.get("fecha_publicacion"),
            "nombre": nombre or None,
            "url": d.get("url") or None,
            "url_original": d.get("url_original") or None,
        })
    return out


def push_meta(isin: str, dry: bool = False, do_push: bool = True) -> bool:
    """Empuja taxonomía + fechas REALES desde Supabase fund_groups al portal (sync-meta): el fondo
    sale COMPLETO en la tabla al instante, sin esperar al sync diario. Ancla en el primario del grupo."""
    if dry or not do_push:
        log(f"  [dry/no-push] meta {isin}")
        return True
    try:
        from tools.supabase_client import get_client
        from tools.reconcile_fund_groups import _primary
        c = get_client()
        f = c.table("funds").select("fund_group_id").eq("isin", isin).execute().data
        if not f:
            return False
        gid = f[0]["fund_group_id"]
        members = c.table("funds").select("*").eq("fund_group_id", gid).execute().data or []
        gg = c.table("fund_groups").select("*").eq("fund_group_id", gid).execute().data
        g = gg[0] if gg else {}
        prim = _primary(members) if members else {"isin": isin}

        def _dt(v):
            return str(v).replace("T", " ")[:19] if v else None
        meta = {
            "isin": prim["isin"].upper(), "nombre": g.get("nombre_base") or "",
            "gestora": g.get("gestora"), "tipo_activo": g.get("tipo_activo"),
            "categoria_activo": g.get("categoria"), "region": g.get("geografia"),
            "estilo": g.get("estilo"), "tema_sector": g.get("tema_sector"),
            "caracteristicas": g.get("caracteristicas_especiales"), "categoria_rf": g.get("categoria_rf"),
            "plazo": g.get("plazo"), "riesgo_ucits": g.get("srri"),
            "aum_meur": g.get("aum_meur"), "anios_antiguedad": g.get("años_antiguedad"),
            "benchmark": prim.get("benchmark"), "estrellas": prim.get("estrellas"),
            "moneda": prim.get("divisa"), "ter": prim.get("ter_pct"),
            "comision_gestion_pct": prim.get("comision_gestion_pct"),
            "minimo": prim.get("importe_minimo_eur"), "distribucion": prim.get("distribucion"),
            "broker": prim.get("broker_disponible"),
            "fecha_ultimo_analisis": _dt(g.get("fecha_ultimo_analisis")), "fecha_alta": _dt(g.get("fecha_alta")),
            "has_qualitative_analysis": 1,
        }
        meta.update(_doc_dates(isin))   # fechas de documentos (annual/semianual/carta) para la pantalla de update anual
        docs = _doc_list(g, g.get("nombre_base") or "")   # lista completa de documentos (pestaña Documentos del portal)
        if docs:
            meta["documentos"] = docs
        meta = {k: v for k, v in meta.items() if v is not None}
        base, user, pwd = _cfg()
        url = f"{base}/wp-json/horizonte/v1/admin/assets/sync-meta"
        h = _auth_header(user, pwd); h["Content-Type"] = "application/json"
        r = httpx.post(url, headers=h, content=json.dumps({"metas": [meta]}), timeout=40)
        return r.status_code == 200
    except Exception as e:  # noqa: BLE001
        log(f"  [WARN] push_meta {isin}: {e}")
        return False


# Heartbeat en disco (sobrevive a reinicios del web_server) → el dashboard sabe si el worker está
# VIVO y qué analiza, sin depender del hilo interno del web_server. Un thread lo refresca cada 20s.
_HB_STATE = {"isin": "", "fase": "idle"}


def write_heartbeat() -> None:
    try:
        import time as _t
        hb = Path("data") / "worker_heartbeat.json"
        hb.parent.mkdir(exist_ok=True)
        hb.write_text(json.dumps({"pid": os.getpid(), "ts": int(_t.time()),
                                  "isin": _HB_STATE["isin"], "fase": _HB_STATE["fase"]}), encoding="utf-8")
    except Exception:
        pass


_FASES = {1: "Prep", 2: "Extract PDFs", 3: "Manager Deep", 4: "Letters K15", 5: "Analyst", 6: "Dashboard"}


def push_progress(isin: str) -> None:
    """Lee el log del run en curso, saca la fase (1-6 del bat) + el tail del log, y lo empuja al portal
    (analisis-progreso) → el admin muestra el MISMO modal de progreso que fund-analyzer."""
    if not isin:
        return
    try:
        import glob as _glob
        from datetime import datetime as _dt
        logs = sorted(_glob.glob(os.path.join("logs", f"run_{isin}_*.log")))
        if not logs:
            return
        logp = logs[-1]
        run_id = os.path.basename(logp)[4:-4]   # quita "run_" y ".log"
        started = ""; dur = 0
        m = re.search(r"_(\d{8})_(\d{6})$", run_id)
        if m:
            try:
                st = _dt.strptime(m.group(1) + m.group(2), "%Y%m%d%H%M%S")
                started = st.strftime("%Y-%m-%d %H:%M:%S")
                dur = max(0, int((_dt.now() - st).total_seconds()))
            except Exception:
                pass
        with open(logp, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()
        # Fase = la que aparece MÁS TARDE en el log (por marcador de skill o "Paso X/6").
        _PAT = {
            1: r"\[PREP\]|Prep determinista|Paso 1(?:\.\d)?/6",
            2: r"extract-pdfs|\[EXTRACT|Paso 2(?:\.\d)?/6",
            3: r"\[MANAGER\]|manager-deep|Paso 3(?:\.\d)?/6",
            4: r"\[LETTERS\]|letters-extract|Paso 4(?:\.\d)?/6",
            5: r"\[ANALYST\]|analyst-cowork|Paso 5(?:\.\d)?/6",
            6: r"dashboard|consume-all|Paso 6(?:\.\d)?/6|Sync a Supabase|Paso 7",
        }
        fnum, best = 1, -1
        for _n, _pat in _PAT.items():
            _ms = list(re.finditer(_pat, text, re.I))
            if _ms and _ms[-1].start() > best:
                best = _ms[-1].start(); fnum = _n
        tail = "\n".join(text.splitlines()[-18:])
        base, user, pwd = _cfg()
        url = f"{base}/wp-json/horizonte/v1/admin/assets/analisis-progreso"
        h = _auth_header(user, pwd); h["Content-Type"] = "application/json"
        httpx.post(url, headers=h, content=json.dumps({
            "isin": isin, "run_id": run_id, "fase": _FASES.get(fnum, ""), "fase_num": fnum,
            "estado": "running", "started": started, "dur_seg": dur, "log": tail}), timeout=15)
    except Exception:
        pass


def _heartbeat_thread() -> None:
    import time as _t
    while True:
        write_heartbeat()
        if _HB_STATE.get("isin"):
            push_progress(_HB_STATE["isin"])   # relay del progreso detallado al portal
        _t.sleep(12)


# ─────────────────────────── pasos ───────────────────────────
def _child_env() -> dict:
    """Entorno para los subprocesos: fuerza UTF-8 en su stdout (si no, en Windows la
    consola hija hereda cp1252 y revienta al imprimir '→'/acentos)."""
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def _run(cmd: list[str], *, dry: bool, timeout: int | None = None) -> int:
    """Ejecuta un comando en la raíz del proyecto. Devuelve el returncode (0=OK)."""
    log("  $ " + " ".join(cmd))
    if dry:
        return 0
    try:
        p = subprocess.run(cmd, cwd=str(ROOT), timeout=timeout, env=_child_env())
        return p.returncode
    except subprocess.TimeoutExpired:
        log(f"  [TIMEOUT] tras {timeout}s")
        return 124
    except Exception as e:  # noqa: BLE001
        log(f"  [ERROR] {e}")
        return 1


def wake_supabase(dry: bool) -> None:
    log("Despertando Supabase (keepalive)…")
    _run([sys.executable, "-m", "tools.supabase_maintenance", "--keepalive"], dry=dry, timeout=120)


def push_metricas(isins: list[str], dry: bool, do_push: bool) -> bool:
    """Genera métricas ACOTADAS a `isins` (Morningstar) y las empuja sin tocar el metricas.json canónico."""
    tmp = Path(tempfile.mkdtemp(prefix="hf_metricas_"))
    lista = tmp / "lista.json"
    lista.write_text(json.dumps({"activos": [{"isin": i, "nombre": ""} for i in isins]},
                                ensure_ascii=False), encoding="utf-8")
    rc = _run([sys.executable, "-m", "tools.export_quant_feed",
               "--lista", str(lista), "--out", str(tmp), "--track-record"], dry=dry, timeout=1200)
    if rc != 0:
        log(f"  [WARN] export_quant_feed rc={rc} — métricas no generadas")
        return False
    metricas = tmp / "metricas.json"
    if not do_push:
        log("  [--no-push] métricas generadas, no se empujan")
        return True
    rc = _run([sys.executable, "-m", "tools.portal_push",
               "--file", str(metricas), "--endpoint", "sync-metricas"], dry=dry, timeout=180)
    return rc == 0


def web_server_up(timeout: float = 2.0) -> bool:
    try:
        return httpx.get(f"{WEB_BASE}/api/health", timeout=timeout).status_code == 200
    except Exception:
        return False


def ensure_web_server(dry: bool) -> bool:
    """Garantiza que el server local del catálogo esté ENCENDIDO (para que, al abrir
    fund-analyzer, se vea en vivo lo que se está analizando). Lo arranca si está caído."""
    if web_server_up():
        return True
    if dry:
        log("  [dry] arrancaría iniciar.ps1 (server + túnel Cloudflare + registro)")
        return True
    flags = 0
    if os.name == "nt":
        flags = subprocess.CREATE_NEW_PROCESS_GROUP | 0x00000008  # DETACHED_PROCESS
    iniciar = ROOT / "iniciar.ps1"
    try:
        if iniciar.exists():
            # iniciar.ps1 = server + túnel Cloudflare + registro de la URL en Supabase →
            # el catálogo PÚBLICO auto-detecta el túnel y muestra server ON + lo que se analiza.
            log("Server/túnel apagado — arrancando iniciar.ps1 (server + túnel + registro)…")
            subprocess.Popen(["powershell", "-ExecutionPolicy", "Bypass", "-File", str(iniciar)],
                             cwd=str(ROOT), creationflags=flags,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            log("Server local apagado — arrancando web_server…")
            subprocess.Popen([sys.executable, "-m", "tools.web_server"], cwd=str(ROOT),
                             env=_child_env(), creationflags=flags,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:  # noqa: BLE001
        log(f"  [WARN] no pude arrancar el server: {e}")
        return False
    for _ in range(40):
        time.sleep(1)
        if web_server_up():
            log("  server local ON (+ túnel → visible también en el catálogo público)")
            return True
    log("  [WARN] el server no respondió a tiempo")
    return False


def _run_bat_directo(isin: str, dry: bool) -> int:
    bat = ROOT / "analizar_fondo.bat"
    return _run(["cmd", "/c", str(bat), isin], dry=dry, timeout=3600)


def analizar(isin: str, dry: bool) -> int:
    """Lanza el análisis completo A TRAVÉS del server local (/api/analyze) para que el
    catálogo lo muestre en vivo. Si el server no está disponible, cae al bat directo.
    Devuelve 0 OK, 10 fallo, 5 sin terminar a tiempo."""
    if not ensure_web_server(dry):
        log("  server no disponible — ejecuto el bat directo (sin visibilidad en catálogo)")
        return _run_bat_directo(isin, dry)
    if dry:
        log(f"  [dry] POST {WEB_BASE}/api/analyze-batch {{isins:[{isin}], cold_start:true}}")
        return 0

    def _run_by_isin() -> dict:
        try:
            return httpx.get(f"{WEB_BASE}/api/runs/by-isin/{isin}", timeout=10).json() or {}
        except Exception:
            return {}

    def _run_id_of(d: dict):
        return d.get("run_id") or (d.get("run") or {}).get("run_id")

    def _status_of(d: dict) -> str:
        return str(d.get("status") or (d.get("run") or {}).get("status") or "").lower()

    # run_id del análisis ANTERIOR de este ISIN (si lo hubo) → para no confundirlo con el nuevo
    prev_id = _run_id_of(_run_by_isin())
    # Lanzar por la COLA (analyze-batch) → aparece en el monitor de arriba del catálogo.
    try:
        d = httpx.post(f"{WEB_BASE}/api/analyze-batch",
                       json={"isins": [isin], "cold_start": True}, timeout=30).json()
    except Exception as e:  # noqa: BLE001
        log(f"  [WARN] POST /api/analyze-batch falló ({e}) — fallback bat directo")
        return _run_bat_directo(isin, dry)
    log(f"  análisis encolado en el server (queued={d.get('queued')}) → VISIBLE en el catálogo")
    ok = ("done", "completed", "completed_with_warnings", "ok", "success")
    bad = ("failed", "error", "cancelled", "canceled", "timeout", "interrupted", "skipped")
    # esperar a que arranque un run NUEVO (id distinto) y llegue a estado terminal (~90 min máx)
    for _ in range(360):
        time.sleep(15)
        cur = _run_by_isin()
        cur_id = _run_id_of(cur)
        if not cur_id or cur_id == prev_id:
            continue  # aún no ha arrancado el nuevo run
        st = _status_of(cur)
        if st in ok:
            log(f"  run {cur_id}: {st}")
            return 5 if st == "completed_with_warnings" else 0
        if st in bad:
            log(f"  run {cur_id}: {st}")
            return 10
    log(f"  [WARN] análisis de {isin} no terminó en el tiempo máximo")
    return 5


def push_clases(dry: bool, do_push: bool) -> bool:
    """Refresca el catálogo desde Supabase, regenera clases y las empuja."""
    rc = _run([sys.executable, "-m", "tools.export_horfin_catalog"], dry=dry, timeout=300)
    if rc != 0:
        log(f"  [WARN] export_horfin_catalog rc={rc}")
    rc = _run([sys.executable, "-m", "tools.build_asset_classes"], dry=dry, timeout=300)
    if rc != 0:
        log(f"  [WARN] build_asset_classes rc={rc} — clases no regeneradas")
        return False
    if not do_push:
        log("  [--no-push] clases generadas, no se empujan")
        return True
    rc = _run([sys.executable, "-m", "tools.portal_push", "--clases"], dry=dry, timeout=180)
    return rc == 0


# ─────────────────────────── ciclo ───────────────────────────
def procesar(isin: str, *, dry: bool, do_push: bool, metrics_only: bool, name: str = "",
             scope: str = "full", docs_aportados: list | None = None,
             analisis_externos: list | None = None) -> bool:
    """Devuelve True si el análisis produjo informe (para marcar needs_review en el portal).

    scope="annual_update": actualiza SOLO el delta del último año de un fondo ya analizado
    (no rehace desde cero). Escribe la señal en config.json que discovery/analyst leen, y
    al cerrar rueda fecha_proximo_analisis + limpia la tarea de vencido."""
    es_annual = scope == "annual_update"
    log(f"── Procesando {isin} {('· ' + name) if name else ''} "
        f"{'[UPDATE ANUAL]' if es_annual else ''}──")
    if es_annual and not dry:
        from tools.annual_update import prepare as _au_prepare
        _cfg = _au_prepare(isin)
        if _cfg.get("modo") == "annual_update":
            log(f"  modo update anual · solo delta desde {_cfg.get('since_date')}")
        else:
            es_annual = False
            log(f"  {_cfg.get('motivo', 'sin análisis previo')} → análisis completo")
    # Material APORTADO por Rafa (docs profesionales / análisis externos) → fuente
    # prioritaria. Se ingiere ANTES del análisis para que la prep/extracción lo incluya.
    if (docs_aportados or analisis_externos) and not dry:
        try:
            from tools.aportados import ingest as _apo_ingest
            _r = _apo_ingest(isin, docs_urls=docs_aportados, analisis_externos=analisis_externos,
                             log=lambda m: log(f"  {m}"))
            log(f"  aportados: {_r.get('n_docs',0)} docs + {_r.get('n_externos',0)} análisis externos")
        except Exception as e:  # noqa: BLE001
            log(f"  [WARN] ingesta de aportados falló: {e}")

    mark_started(isin, "analizando", dry=dry)   # badge "Analizando…" en el portal
    # 1. Métricas (rápido, no necesita análisis)
    _HB_STATE["fase"] = "métricas"
    ok_m = push_metricas([isin], dry=dry, do_push=do_push)
    log(f"  métricas: {'OK' if ok_m else 'FALLO/incompletas'}")
    if metrics_only:
        push_meta(isin, dry=dry, do_push=do_push)
        return True
    # 2. Análisis completo + clases
    _HB_STATE["fase"] = "análisis"
    rc = analizar(isin, dry=dry)
    tag = {0: "OK", 5: "OK con avisos", 10: "FALLO crítico"}.get(rc, f"rc={rc}")
    log(f"  análisis: {tag}")
    if rc == 10:
        log("  [SKIP] análisis crítico — no se empujan clases (datos parciales)")
        return False   # análisis fallido → el portal NO lo marca para categorizar (sin informe)
    _HB_STATE["fase"] = "clases"
    ok_c = push_clases(dry=dry, do_push=do_push)
    log(f"  clases: {'OK' if ok_c else 'FALLO/incompletas'}")
    # 3. Meta (taxonomía + fechas reales) → la tabla del portal sale COMPLETA al instante.
    _HB_STATE["fase"] = "meta"
    ok_meta = push_meta(isin, dry=dry, do_push=do_push)
    log(f"  meta portal: {'OK' if ok_meta else 'FALLO'}")
    # 4. Cierre del update anual: rueda fecha_proximo_analisis (+1 año, AR incorporado) y
    #    limpia la tarea de vencido. El fondo vuelve a "Categorizar" (needs_review) para
    #    que Rafa revise la ficha actualizada.
    if es_annual and not dry:
        try:
            from tools.annual_update import close as _au_close
            _au_close(isin, log=log)
        except Exception as e:  # noqa: BLE001
            log(f"  [ANNUAL] cierre falló (no crítico): {e}")
    return True        # el análisis produjo informe → el portal lo marcará needs_review=1


def una_pasada(args) -> int:
    if args.isin:
        cola = [{"isin": args.isin.upper().strip(), "name": "", "needs_review": None,
                 "scope": getattr(args, "scope", "full")}]
    else:
        cola = fetch_queue()
        log(f"Cola del portal: {len(cola)} ISIN pendiente(s) de análisis")
    if not cola:
        log("Nada que hacer.")
        return 0
    lote = cola[: max(1, args.limit)]
    if len(cola) > len(lote):
        log(f"Limitado a {len(lote)} de {len(cola)} (usa --limit para subirlo).")
    for item in lote:
        ok = False
        try:
            ok = bool(procesar(item["isin"], dry=args.dry_run, do_push=not args.no_push,
                     metrics_only=args.metrics_only, name=item.get("name", ""),
                     scope=item.get("scope", "full"),
                     docs_aportados=item.get("docs_aportados"),
                     analisis_externos=item.get("analisis_externos")))
        except Exception as e:  # noqa: BLE001
            log(f"  [ERROR procesando {item['isin']}] {e}")
            ok = False
        finally:
            # Limpia el marcador SIEMPRE (éxito o fallo) para no re-analizar en bucle; si falló,
            # Rafa lo vuelve a encolar. `ok` decide si el portal lo marca para categorizar
            # (solo con informe): un análisis fallido NO debe salir en "Categorizar".
            if not args.isin:
                mark_done(item["isin"], ok=ok, dry=args.dry_run)
    _HB_STATE["isin"] = ""; _HB_STATE["fase"] = "idle"
    return len(lote)


def main() -> None:
    ap = argparse.ArgumentParser(description="Consumidor de la cola de análisis del portal Horizonte.")
    ap.add_argument("--once", action="store_true", help="una sola pasada de la cola")
    ap.add_argument("--loop", type=int, metavar="SEG", help="sondea cada SEG segundos (bucle)")
    ap.add_argument("--isin", help="fuerza un ISIN concreto (ignora la cola)")
    ap.add_argument("--scope", default="full", choices=["full", "annual_update"],
                    help="con --isin: modo de análisis (full o annual_update). Def. full.")
    ap.add_argument("--limit", type=int, default=1, help="máx. fondos por pasada (def. 1, por coste)")
    ap.add_argument("--wake", action="store_true", help="despierta Supabase (keepalive) antes")
    ap.add_argument("--metrics-only", action="store_true", help="solo métricas Morningstar, sin el bat")
    ap.add_argument("--no-push", action="store_true", help="genera pero no empuja al portal")
    ap.add_argument("--dry-run", action="store_true", help="enseña qué haría, sin ejecutar")
    ap.add_argument("--drain", action="store_true", help="procesa la cola hasta vaciarla y termina (bridge de arranque)")
    a = ap.parse_args()

    if not (a.once or a.loop or a.isin or a.drain):
        ap.print_help()
        return

    if a.wake:
        wake_supabase(a.dry_run)

    # Heartbeat en disco en modo continuo (el dashboard sabe si el worker VIVE y qué analiza,
    # sin depender del hilo interno del web_server). import threading (NO 'os') para no shadowear.
    if a.loop or a.drain:
        import threading
        threading.Thread(target=_heartbeat_thread, daemon=True).start()

    # Mantener el server local del catálogo encendido (salvo en modo solo-métricas, que no
    # lanza análisis) → al abrir fund-analyzer se ve en vivo lo que se está analizando.
    keep_server = not a.metrics_only
    if keep_server:
        ensure_web_server(a.dry_run)

    if a.drain:
        # Bridge de arranque: procesa TODO lo encolado y se apaga. Lock de instancia única para que
        # dos pulsaciones seguidas no lancen dos drenados (el que ya corre coge la cola nueva).
        import tempfile   # NO importar 'os' aquí: haría 'os' local a main() y rompería os.getpid() en la rama del bucle
        lock = Path(tempfile.gettempdir()) / "hf_portal_drain.lock"
        if lock.exists() and (time.time() - lock.stat().st_mtime) < 7200:
            log("Ya hay un drenado en marcha (lock reciente). Salgo; el que corre cogerá la cola.")
            return
        try:
            lock.write_text(str(os.getpid()))
            log("DRENAR: proceso la cola hasta vaciarla y me apago.")
            vueltas = 0
            while True:
                try:
                    n = una_pasada(a)
                except Exception as e:  # noqa: BLE001
                    log(f"[ERROR pasada] {e}")
                    n = 0
                if not n:
                    log("Cola vacía. Fin del drenado.")
                    break
                vueltas += 1
                if vueltas > 500:
                    log("Backstop 500 vueltas — salgo.")
                    break
                lock.touch()  # refresca el lock para que no lo roben mientras trabaja
        finally:
            try:
                lock.unlink()
            except Exception:
                pass
        return

    if a.loop:
        # Lock de instancia única: si ya hay OTRO worker vivo (mismo lock, PID activo) salgo, para
        # no procesar la cola por duplicado. PID-liveness (no mtime) porque una pasada puede bloquear
        # ~40min analizando; si el PID viejo murió, este lo roba (compatible con el auto-relanzado del .cmd).
        import tempfile, ctypes
        _lock = Path(tempfile.gettempdir()) / "hf_portal_worker.lock"
        _otro = False
        if _lock.exists():
            try:
                _pid = int(_lock.read_text().strip())
            except Exception:
                _pid = 0
            if _pid and _pid != os.getpid():
                try:
                    _h = ctypes.windll.kernel32.OpenProcess(0x1000, False, _pid)
                    if _h:
                        ctypes.windll.kernel32.CloseHandle(_h)
                        _otro = True
                except Exception:
                    pass
        if _otro:
            log("Ya hay OTRO worker del poller vivo (lock) — salgo para no duplicar el análisis.")
            return
        try:
            _lock.write_text(str(os.getpid()))
            log(f"lock adquirido: {_lock} (existe={_lock.exists()})")
        except Exception as e:  # noqa: BLE001
            log(f"[WARN] no pude escribir el lock ({_lock}): {e}")
        log(f"BUCLE cada {a.loop}s (Ctrl-C para parar). limit={a.limit}")
        try:
            while True:
                try:
                    if keep_server:
                        ensure_web_server(a.dry_run)
                    una_pasada(a)
                except Exception as e:  # noqa: BLE001
                    log(f"[ERROR pasada] {e}")
                time.sleep(max(30, a.loop))
        finally:
            try:
                _lock.unlink()
            except Exception:
                pass
    else:
        una_pasada(a)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    main()
