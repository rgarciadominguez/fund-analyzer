"""
web_server.py — Flask server para lanzar y monitorear análisis cualitativos
desde el catálogo web.

Sirve `dashboard/catalog.html` + `dashboard/fund-*.html` y expone una pequeña
API para:
  - Lanzar `analizar_fondo.bat ISIN` en background (no bloquea el server)
  - Consultar progreso (tail de log + skill logs detectados)
  - Listar runs activos / históricos
  - Regenerar el catálogo bajo demanda

Uso:
    python -m tools.web_server                    # localhost:5000
    python -m tools.web_server --port 8000
    python -m tools.web_server --no-cold-start    # NO mueve carpeta a .bak

Requisitos: pip install flask flask-cors
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from flask import Flask, jsonify, redirect, request, send_from_directory
    from flask_cors import CORS
except ImportError:
    print("[ERROR] Flask no instalado. Ejecuta:")
    print("    pip install flask flask-cors")
    sys.exit(1)


ROOT = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = ROOT / "dashboard"
DATA_DIR = ROOT / "data"
LOGS_DIR = ROOT / "logs"
RUNS_FILE = DATA_DIR / "runs.jsonl"

ISIN_REGEX = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}\d$")


# ─────────────────────────────────────────────────────────────────────────
# Estado global de runs (in-memory + persistido en data/runs.jsonl)
# ─────────────────────────────────────────────────────────────────────────
RUNS: dict[str, dict] = {}
RUNS_LOCK = threading.Lock()

# Cola multi-ISIN: ejecución secuencial en background thread.
# QUEUE contiene items {isin, queued_at, cold_start, status: 'queued'|'running'|'done'|'failed'|'skipped',
#                       run_id (cuando arranca), error (si falla validación)}
QUEUE: list[dict] = []
QUEUE_LOCK = threading.Lock()
QUEUE_WORKER: "threading.Thread | None" = None
QUEUE_STARTED_AT: "str | None" = None

# P4: pausa por tokens Claude Max agotados.
# Si el worker detecta agotamiento, setea blocked_until + lanza thread monitor.
# Monitor prueba cada 30min si los tokens están disponibles; si sí, marca el
# item paused como queued de nuevo y reanuda el worker.
QUEUE_TOKENS_BLOCKED_UNTIL: "str | None" = None       # ISO timestamp
QUEUE_TOKENS_MONITOR: "threading.Thread | None" = None
QUEUE_TOKENS_LAST_CHECK: "str | None" = None
QUEUE_TOKENS_CHECK_INTERVAL_S = 30 * 60               # 30 min entre checks automáticos
QUEUE_TOKENS_DEFAULT_WAIT_HOURS = 4                   # cuánto esperar tras agotamiento

# W1 (2026-05-21): watchdog detecta inconsistencias (bat murió pero status sigue running)
WATCHDOG_THREAD: "threading.Thread | None" = None

# Patrones a buscar en log/stderr del bat que indican agotamiento de tokens
# en Claude Max (claude -p login subscription). Match case-insensitive substring.
TOKEN_EXHAUSTION_PATTERNS = (
    "rate limit",
    "rate_limit_exceeded",
    "credit balance is too low",
    "quota exceeded",
    "quota_exceeded",
    "max usage limit",
    "usage limit reached",
    "model overloaded",
    "overloaded_error",
    "you've reached your",
    "monthly token limit",
    "claude_max_quota",
    "subscription limit",
)


def load_persisted_runs():
    """Carga runs del archivo persistido (para mostrar histórico tras reinicios)."""
    if not RUNS_FILE.exists():
        return
    try:
        with RUNS_FILE.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    r = json.loads(line.strip())
                    if r.get("status") == "running":
                        # Si reinicias el server, los "running" persistidos pueden
                        # estar muertos. Marcamos como "unknown" para no confundir.
                        r["status"] = "unknown_after_restart"
                    rid = r.get("run_id")
                    if rid and rid not in RUNS:
                        RUNS[rid] = r
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"[WARN] Error cargando runs persistidos: {e}")


def persist_run(run_id: str, r: dict):
    """Append-only de runs a data/runs.jsonl. Solo serializa campos JSON-safe."""
    safe = {k: v for k, v in r.items() if not k.startswith("_")}
    with RUNS_LOCK:
        DATA_DIR.mkdir(exist_ok=True)
        with RUNS_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(safe, ensure_ascii=False) + "\n")


# ─────────────────────────────────────────────────────────────────────────
# Watcher: hilo que espera a que el subprocess termine y actualiza el run
# ─────────────────────────────────────────────────────────────────────────
def watch_run(run_id: str):
    """Vigila un subprocess y actualiza RUNS cuando termina.

    B1 (2026-05-19): usa polling (proc.poll() + sleep) en vez de proc.wait()
    porque en Windows 11 + Python 3.14 con CREATE_NEW_PROCESS_GROUP, wait()
    puede retornar antes de tiempo dejando el subprocess vivo. Polling con
    poll() es robusto y consistente cross-platform.
    """
    import time
    r = RUNS.get(run_id)
    if not r:
        return
    proc = r.get("_proc")
    if not proc:
        return
    rc: int | None = None
    try:
        # Polling cada 5s. Sin timeout aquí — el worker tiene su propio cap.
        while True:
            rc = proc.poll()
            if rc is not None:
                break
            time.sleep(5)
    except Exception as e:
        print(f"[WATCH {run_id}] poll error: {e}")
        rc = proc.poll()  # último intento de leer
    finally:
        log_file = r.get("_log_file")
        if log_file:
            try:
                log_file.close()
            except Exception:
                pass
        # Si rc sigue siendo None (excepción rara), conservar status "running"
        if rc is None:
            rc = -1
            r["status"] = "unknown"
        else:
            r["status"] = "done" if rc == 0 else "failed"
        r["exit_code"] = rc
        r["end_time"] = datetime.now(timezone.utc).isoformat()
        try:
            start = datetime.fromisoformat(r["start_time"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(r["end_time"])
            r["duration_seconds"] = int((end - start).total_seconds())
        except Exception:
            r["duration_seconds"] = None
        persist_run(run_id, r)
        print(
            f"[WATCH {run_id}] terminado status={r['status']} "
            f"exit={rc} duración={r.get('duration_seconds')}s"
        )


# ─────────────────────────────────────────────────────────────────────────
# Flask app
# ─────────────────────────────────────────────────────────────────────────
def make_app(cold_start: bool = True) -> Flask:
    app = Flask(
        __name__,
        static_folder=str(DASHBOARD_DIR),
        static_url_path="/dashboard",
    )
    # Registrado ANTES de CORS para que corra EL ÚLTIMO (after_request se ejecuta
    # en orden inverso de registro) y tenga la última palabra sobre esta cabecera.
    @app.after_request
    def _allow_private_network(resp):
        # Permite que una página https (github.io) llame a este server en localhost
        # (Chrome Private Network Access exige esta cabecera en el preflight).
        if request.headers.get("Access-Control-Request-Private-Network"):
            resp.headers["Access-Control-Allow-Private-Network"] = "true"
        return resp

    CORS(app)
    app.config["FUND_COLD_START"] = cold_start

    @app.route("/")
    def index():
        return redirect("/dashboard/catalog.html", code=302)

    @app.route("/api/health")
    def api_health():
        return jsonify(
            {
                "ok": True,
                "version": "1.0",
                "n_runs_active": sum(1 for r in RUNS.values() if r.get("status") == "running"),
                "n_runs_total": len(RUNS),
            }
        )

    # ── Chat por fondo (2026-06-16) ───────────────────────────────────────
    # Unifica el chat AQUÍ (mismo backend que el feedback, mismo API_BASE) para
    # que NO haga falta arrancar `chat_server.py` aparte. Usa Anthropic (Haiku)
    # con prompt caching; sin dependencia de Gemini (que tiene kill-switch).
    @app.route("/api/chat/<isin>/info", methods=["GET"])
    def api_chat_info(isin):
        isin = (isin or "").strip().upper()
        if not ISIN_REGEX.match(isin):
            return jsonify({"error": "ISIN inválido", "documents_loaded": []}), 400
        try:
            from tools.chat_context import load_fund_context
            ctx = load_fund_context(isin, ROOT)
        except Exception as e:
            return jsonify({"error": str(e)[:200], "documents_loaded": [], "llm_ready": False}), 200
        if ctx.get("error"):
            return jsonify({"error": "fondo sin documentos", "documents_loaded": [], "llm_ready": False}), 200
        docs = [k for k, v in ctx.items() if v and k != "isin"]
        nombre = (ctx.get("output", {}) or {}).get("nombre", isin)
        return jsonify({
            "isin": isin, "nombre": nombre,
            "documents_loaded": docs,
            "llm_ready": bool(os.environ.get("ANTHROPIC_API_KEY")),
        })

    @app.route("/api/chat/<isin>", methods=["POST"])
    def api_chat(isin):
        isin = (isin or "").strip().upper()
        if not ISIN_REGEX.match(isin):
            return jsonify({"error": "ISIN inválido"}), 400
        body = request.get_json(silent=True) or {}
        question = (body.get("question") or "").strip()
        history = body.get("history") or []
        if not question:
            return jsonify({"error": "pregunta vacía"}), 400
        if not os.environ.get("ANTHROPIC_API_KEY"):
            return jsonify({"answer": "El chat no está configurado en el servidor "
                                      "(falta ANTHROPIC_API_KEY en .env)."}), 200
        try:
            from tools.chat_context import load_fund_context, build_system_prompt
            ctx = load_fund_context(isin, ROOT)
            if ctx.get("error"):
                return jsonify({"answer": f"No encuentro los documentos del fondo {isin}."}), 200
            system = build_system_prompt(ctx)
            if len(system) > 350000:  # cap defensivo de contexto (~90K tokens)
                system = system[:350000] + "\n[...contexto truncado...]"
            msgs = []
            for m in history[-10:]:
                role = "assistant" if (m.get("role") in ("ai", "model", "assistant")) else "user"
                txt = (m.get("text") or m.get("content") or "").strip()
                if txt:
                    msgs.append({"role": role, "content": txt})
            msgs.append({"role": "user", "content": question})
            from anthropic import Anthropic
            client = Anthropic()
            resp = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=1500,
                system=[{"type": "text", "text": system,
                         "cache_control": {"type": "ephemeral"}}],
                messages=msgs,
            )
            answer = "".join(
                b.text for b in resp.content if getattr(b, "type", None) == "text"
            ).strip()
            return jsonify({"answer": answer or "No tengo ese dato en los documentos del fondo."})
        except Exception as e:
            return jsonify({"answer": f"Error del chat: {str(e)[:200]}"}), 200

    # ── Vínculos de clase (2026-06-16): alias_isin → primary_isin ─────────
    @app.route("/api/class-links", methods=["GET"])
    def api_class_links_get():
        from tools.class_links import all_links
        return jsonify(all_links())

    @app.route("/api/class-links", methods=["POST"])
    def api_class_links_add():
        from tools.class_links import add_link, sync_link_to_supabase
        body = request.get_json(silent=True) or {}
        res = add_link(body.get("alias", ""), body.get("primary", ""), body.get("label", ""))
        if res.get("ok"):
            # Propagar a Supabase para que el catálogo público también agrupe.
            res["supabase"] = sync_link_to_supabase(res["alias"], res["primary"])
        return jsonify(res), (200 if res.get("ok") else 400)

    @app.route("/api/class-links/<alias>", methods=["DELETE"])
    def api_class_links_del(alias):
        from tools.class_links import remove_link
        res = remove_link(alias)
        return jsonify(res), (200 if res.get("ok") else 404)

    @app.route("/api/regenerate-catalog", methods=["POST"])
    def api_regenerate_catalog():
        """Re-ejecuta build_catalog para refrescar el JSON."""
        try:
            result = subprocess.run(
                [sys.executable, "-m", "tools.build_catalog", "--quiet"],
                cwd=str(ROOT),
                capture_output=True,
                text=True,
                timeout=120,
            )
            return jsonify(
                {
                    "ok": result.returncode == 0,
                    "stdout": result.stdout[-500:],
                    "stderr": result.stderr[-500:],
                }
            )
        except subprocess.TimeoutExpired:
            return jsonify({"ok": False, "error": "timeout"}), 500
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    def _start_analysis_for_isin(
        isin: str, force_cold: bool, apply_feedback: bool = False
    ) -> tuple[dict, int]:
        """Helper compartido por /api/analyze (single) y el worker de la cola.

        Devuelve (payload, http_status). Si status != 200/201, payload contiene "error".
        Si OK, payload contiene run_id, isin, status='running', pid, log_relative, start_time.

        T3.5 (2026-05-28): apply_feedback=True añade `--apply-feedback` a argv del
        bat. Implica resume mode (cold_start=False) — aplicar feedback sobre
        outputs vacíos no tiene sentido. Si force_cold=True y apply_feedback=True,
        gana force_cold y se ignora apply_feedback (con un warning).
        """
        if not ISIN_REGEX.match(isin):
            return {"error": f"ISIN inválido: '{isin}'"}, 400

        # Evitar relanzar si ya hay run activo para este ISIN
        for rid, r in RUNS.items():
            if r.get("isin") == isin and r.get("status") == "running":
                return {
                    "error": f"ya hay un run activo: {rid}",
                    "run_id": rid,
                    "status": "already_running",
                }, 409

        # Cold-start: mover carpeta del fondo a .bak si existe
        fund_dir = DATA_DIR / "funds" / isin
        if fund_dir.exists() and force_cold:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup = DATA_DIR / "funds" / f"{isin}.bak_pre_web_{ts}"
            try:
                fund_dir.rename(backup)
                print(f"[ANALYZE {isin}] backup: {backup.name}")
            except Exception as e:
                return {"error": f"no se pudo mover backup: {e}"}, 500

        # Bat
        bat_path = ROOT / "analizar_fondo.bat"
        if not bat_path.exists():
            return {"error": "analizar_fondo.bat no existe"}, 500

        # Run id + log
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"{isin}_{ts}"
        LOGS_DIR.mkdir(exist_ok=True)
        log_path = LOGS_DIR / f"run_{run_id}.log"

        # M7 (2026-05-19): construir argv del bat según modo cold_start.
        # - cold_start=True: argv [bat, ISIN]  → bat mueve data/funds/<ISIN> a .bak
        # - cold_start=False (resume mode): argv [bat, ISIN, --resume] → N5 bat
        #   skip los pasos cuyos outputs ya existen. Requiere fund_dir presente
        #   con al menos cnmv_data.json o intl_data.json (sino bat aborta exit 3).
        bat_argv = [str(bat_path), isin]
        if not force_cold:
            bat_argv.append("--resume")
            # T3.5: añadir --apply-feedback si el caller lo pidió
            if apply_feedback:
                bat_argv.append("--apply-feedback")
            # Verificar pre-condiciones del resume: si no hay fund_dir o no hay
            # ningún data file, el bat fallará exit 3. Avisamos antes.
            cnmv_p = fund_dir / "cnmv_data.json"
            intl_p = fund_dir / "intl_data.json"
            if not fund_dir.exists() or (not cnmv_p.exists() and not intl_p.exists()):
                # No hay datos parciales → degradar a cold-start automático
                print(f"[ANALYZE {isin}] resume requested but no partial data — fallback to cold-start")
                bat_argv = [str(bat_path), isin]
                if apply_feedback:
                    print(f"[ANALYZE {isin}] apply_feedback ignorado (fallback cold-start sin outputs previos)")
                # Cold-start manual: mover fund_dir a .bak si existe (sin reusar)
                if fund_dir.exists():
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup = DATA_DIR / "funds" / f"{isin}.bak_pre_web_{ts}"
                    try:
                        fund_dir.rename(backup)
                    except Exception:
                        pass
        elif apply_feedback:
            # cold_start + apply_feedback no tiene sentido: apply_feedback
            # opera sobre outputs existentes que cold_start acaba de borrar.
            print(f"[ANALYZE {isin}] [WARN] cold_start=True ignora apply_feedback=True")

        # Test-guard (2026-06-09): bajo pytest NUNCA lanzar el bat de producción.
        # Un test que crea la app o dispara el worker leía el queue_state.json
        # REAL y lanzaba runs de verdad (bat + orchestrator orphan, contra ISINs
        # reales de la cola del usuario). Devolvemos un run stub sin Popen.
        if os.environ.get("PYTEST_CURRENT_TEST") and not os.environ.get("FUND_ANALYZER_ALLOW_REAL_LAUNCH"):
            print(f"[ANALYZE {isin}] [TEST-GUARD] launch real omitido bajo pytest")
            RUNS[run_id] = {"run_id": run_id, "isin": isin, "status": "test_skipped", "pid": None}
            return {"run_id": run_id, "isin": isin, "status": "test_skipped", "test_guard": True}, 200

        try:
            log_file = log_path.open("w", encoding="utf-8")
            creationflags = 0
            if sys.platform == "win32":
                creationflags = (
                    subprocess.CREATE_NEW_PROCESS_GROUP
                    | subprocess.CREATE_NO_WINDOW
                )
            print(f"[ANALYZE {isin}] argv={bat_argv}")
            proc = subprocess.Popen(
                bat_argv,
                cwd=str(ROOT),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                creationflags=creationflags,
                shell=False,
            )
        except Exception as e:
            return {"error": f"no se pudo lanzar bat: {e}"}, 500

        run = {
            "run_id": run_id,
            "isin": isin,
            "status": "running",
            "pid": proc.pid,
            "log_path": str(log_path),
            "log_relative": str(log_path.relative_to(ROOT)),
            "start_time": datetime.now(timezone.utc).isoformat(),
            "cold_start": bool(force_cold),
            "_proc": proc,
            "_log_file": log_file,
        }
        RUNS[run_id] = run
        persist_run(run_id, run)
        threading.Thread(target=watch_run, args=(run_id,), daemon=True).start()

        print(f"[ANALYZE {isin}] run_id={run_id} pid={proc.pid}")
        return {
            "run_id": run_id,
            "isin": isin,
            "status": "running",
            "pid": proc.pid,
            "log_relative": run["log_relative"],
            "start_time": run["start_time"],
        }, 200

    @app.route("/api/analyze", methods=["POST"])
    def api_analyze():
        """Lanza analizar_fondo.bat para un ISIN único. Devuelve run_id.

        Body opcional: {cold_start: bool, apply_feedback: bool}.
        """
        data = request.get_json(silent=True) or {}
        isin = (data.get("isin") or "").strip().upper()
        force_cold = data.get("cold_start", app.config["FUND_COLD_START"])
        apply_feedback = bool(data.get("apply_feedback", False))
        payload, status = _start_analysis_for_isin(
            isin, bool(force_cold), apply_feedback=apply_feedback,
        )
        return jsonify(payload), status

    # ─────────────────────────────────────────────────────────────────────
    # Cola multi-ISIN (ejecución secuencial)
    # ─────────────────────────────────────────────────────────────────────

    def _queue_snapshot() -> list[dict]:
        """Copia thread-safe (sin lock) de la cola para serializar.
        W5 (2026-05-21): para items running, añade `log_last_activity_min`
        (minutos desde la última escritura al log del run) para que la UI
        muestre actividad real y el user vea si está atascado."""
        with QUEUE_LOCK:
            items = [{k: v for k, v in item.items() if not k.startswith("_")}
                     for item in QUEUE]
        # Enriquecer running items con log freshness
        for it in items:
            if it.get("status") != "running":
                continue
            run_id = it.get("run_id")
            if not run_id:
                continue
            log_path = RUNS.get(run_id, {}).get("log_path", "")
            stale = _log_last_activity_min(log_path) if log_path else None
            if stale is not None:
                it["log_last_activity_min"] = round(stale, 1)
        return items

    # B3: Persistencia de la cola en disco (sobrevive reinicios).
    QUEUE_STATE_FILE = DATA_DIR / "queue_state.json"

    def _save_queue_state():
        """Vuelca QUEUE + metadata global a data/queue_state.json (atómico)."""
        try:
            items = _queue_snapshot()
            tmp = QUEUE_STATE_FILE.with_suffix(".json.tmp")
            tmp.parent.mkdir(parents=True, exist_ok=True)
            with tmp.open("w", encoding="utf-8") as f:
                json.dump({
                    "saved_at": datetime.now(timezone.utc).isoformat(),
                    "items": items,
                    # M1: persistir bloqueo de tokens para que el monitor
                    # respete la espera tras restart.
                    "tokens_blocked_until": QUEUE_TOKENS_BLOCKED_UNTIL,
                    "tokens_last_check": QUEUE_TOKENS_LAST_CHECK,
                }, f, ensure_ascii=False, indent=2, default=str)
            tmp.replace(QUEUE_STATE_FILE)
        except Exception as e:
            print(f"[QUEUE] error guardando queue_state: {e}")

    def _load_queue_state():
        """Carga QUEUE desde disco al arrancar. Items en estado 'running' que
        ya no tienen proceso real → 'interrupted' (B4 auto-detección zombies).
        M1: también restaura el timestamp de bloqueo de tokens si estaba."""
        global QUEUE_TOKENS_BLOCKED_UNTIL, QUEUE_TOKENS_LAST_CHECK
        if not QUEUE_STATE_FILE.exists():
            print("[QUEUE] no hay queue_state.json previo (arranque limpio)")
            return
        try:
            with QUEUE_STATE_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
            items = data.get("items", []) or []
            # M1: restaurar bloqueo de tokens si el timestamp aún no venció.
            blocked_until = data.get("tokens_blocked_until")
            if blocked_until:
                try:
                    until_dt = datetime.fromisoformat(blocked_until.replace("Z", "+00:00"))
                    if until_dt > datetime.now(timezone.utc):
                        QUEUE_TOKENS_BLOCKED_UNTIL = blocked_until
                        print(f"[QUEUE] tokens_blocked_until restaurado hasta {blocked_until}")
                    else:
                        print(f"[QUEUE] tokens_blocked_until={blocked_until} ya venció")
                except Exception:
                    pass
            QUEUE_TOKENS_LAST_CHECK = data.get("tokens_last_check")
        except Exception as e:
            print(f"[QUEUE] error cargando queue_state.json: {e}")
            return

        # B4: marcar zombies. Cualquier item 'running' tras un restart → interrupted
        # (no podemos reconectar al subprocess huérfano). Items 'queued' SÍ se
        # restauran y el worker los recogerá.
        n_zombies = 0
        n_restored = 0
        for it in items:
            if it.get("status") == "running":
                it["status"] = "interrupted"
                it["finished_at"] = it.get("finished_at") or datetime.now(timezone.utc).isoformat()
                it["error"] = "subprocess perdido tras reinicio del web_server"
                n_zombies += 1
            elif it.get("status") == "queued":
                n_restored += 1
        with QUEUE_LOCK:
            QUEUE.clear()
            QUEUE.extend(items)
        if n_zombies or n_restored:
            print(f"[QUEUE] cargado queue_state.json — {n_zombies} zombies marcados interrupted, {n_restored} pendientes restaurados")
        else:
            print(f"[QUEUE] cargado queue_state.json — {len(items)} items (terminados de sesiones previas)")

    # B1+B2 (2026-05-19): polling con poll() en vez de wait() — Win11/Py3.14
    # con CREATE_NEW_PROCESS_GROUP hacían retornar wait() antes de tiempo.
    # Además clasificación "completed_with_warnings" cuando exit_code != 0
    # pero output.json existe (caso bat con skills opcionales fallando).
    QUEUE_MAX_RUN_SECONDS = 60 * 240  # 4h máximo por run (failsafe).
    # Subido de 120 → 240 min (2026-05-20) porque fondos INT con HTML fallback
    # + manager_deep + letters_deep + analyst superan los 2h. Override con env
    # QUEUE_MAX_RUN_MINUTES si se quiere ajustar.
    QUEUE_POLL_INTERVAL_S = 5

    def _wait_for_proc_with_polling(proc, run_id: str, isin: str) -> tuple[int | None, bool]:
        """Espera a que proc termine usando poll() periódico.

        P4 (2026-05-19): también monitoriza el log del run buscando patrones
        de agotamiento de tokens Claude Max. Si detecta, mata el proc y
        devuelve (None, True).

        Retorna tupla (exit_code, token_exhaustion_detected).
        Es más robusto que proc.wait() en Windows con CREATE_NEW_PROCESS_GROUP.
        """
        import time as _t
        elapsed = 0
        log_path = RUNS.get(run_id, {}).get("log_path", "")
        last_log_size = 0
        while elapsed < QUEUE_MAX_RUN_SECONDS:
            rc = proc.poll()
            if rc is not None:
                _t.sleep(2)  # dejar a watch_run actualizar RUNS
                return rc, False

            # P4: cada poll, leer las últimas 5KB del log nuevo y buscar
            # patrones de agotamiento de tokens. Solo si el log creció.
            if log_path:
                try:
                    p = Path(log_path)
                    if p.exists():
                        sz = p.stat().st_size
                        if sz > last_log_size:
                            with p.open("r", encoding="utf-8", errors="replace") as f:
                                f.seek(max(0, sz - 5000))
                                new_text = f.read().lower()
                            if _detect_token_exhaustion(new_text):
                                print(f"[QUEUE] {isin} TOKEN EXHAUSTION detected, killing bat")
                                try:
                                    proc.terminate()
                                    _t.sleep(3)
                                    if proc.poll() is None:
                                        proc.kill()
                                except Exception:
                                    pass
                                return None, True
                            last_log_size = sz
                except Exception:
                    pass

            _t.sleep(QUEUE_POLL_INTERVAL_S)
            elapsed += QUEUE_POLL_INTERVAL_S
        # Timeout: matar subprocess y todos sus hijos
        print(f"[QUEUE] {isin} (run {run_id}) excedió {QUEUE_MAX_RUN_SECONDS}s — terminando")
        try:
            proc.terminate()
            _t.sleep(5)
            if proc.poll() is None:
                proc.kill()
                _t.sleep(2)
        except Exception as e:
            print(f"[QUEUE] terminate error {run_id}: {e}")
        return proc.poll(), False

    def _detect_token_exhaustion(log_text: str) -> bool:
        """True si el texto contiene algún patrón de agotamiento de tokens."""
        if not log_text:
            return False
        lower = log_text.lower()
        return any(pat in lower for pat in TOKEN_EXHAUSTION_PATTERNS)

    def _test_claude_max_available() -> bool:
        """Hace un ping mínimo a 'claude' CLI para ver si tokens están disponibles.

        Llama a `claude -p "ok"` con timeout 30s. Si devuelve algo non-error
        rápido, asume tokens OK. Si timeout o error de quota, no disponible.
        """
        try:
            result = subprocess.run(
                ["claude", "-p", "ok"],
                capture_output=True, text=True, timeout=30,
                shell=False,
            )
            out = (result.stdout or "") + (result.stderr or "")
            if result.returncode != 0:
                # Exit no zero — probablemente quota o login issue
                if _detect_token_exhaustion(out):
                    return False
                # Otro tipo de error — asumir disponible (no podemos verificar)
                return True
            if _detect_token_exhaustion(out):
                return False
            return True
        except subprocess.TimeoutExpired:
            return False
        except FileNotFoundError:
            # claude CLI no instalado — no podemos verificar, asumir OK
            print("[QUEUE] [WARN] 'claude' CLI no encontrado en PATH")
            return True
        except Exception as e:
            print(f"[QUEUE] [WARN] check claude max error: {e}")
            return True  # benefit of doubt

    def _start_tokens_monitor():
        """Lanza thread que verifica disponibilidad de tokens periódicamente
        y reanuda items pausados cuando los tokens vuelven."""
        global QUEUE_TOKENS_MONITOR
        with QUEUE_LOCK:
            if QUEUE_TOKENS_MONITOR is not None and QUEUE_TOKENS_MONITOR.is_alive():
                return  # ya activo
            QUEUE_TOKENS_MONITOR = threading.Thread(target=_tokens_monitor_loop, daemon=True)
            QUEUE_TOKENS_MONITOR.start()
        print("[QUEUE] tokens monitor arrancado")

    def _tokens_monitor_loop():
        """Loop: cada 30 min verifica tokens. Si OK, marca paused → queued y
        arranca worker. Si KO, espera otros 30 min. Termina cuando no quedan
        items paused."""
        global QUEUE_TOKENS_BLOCKED_UNTIL, QUEUE_TOKENS_LAST_CHECK, QUEUE_WORKER
        import time as _t
        while True:
            # ¿Hay items paused?
            with QUEUE_LOCK:
                n_paused = sum(1 for it in QUEUE if it.get("status") == "paused_waiting_tokens")
            if n_paused == 0:
                print("[QUEUE] tokens monitor: ya no hay items paused, terminando")
                QUEUE_TOKENS_BLOCKED_UNTIL = None
                break

            print(f"[QUEUE] tokens monitor: {n_paused} items paused, probando claude max…")
            QUEUE_TOKENS_LAST_CHECK = datetime.now(timezone.utc).isoformat()
            available = _test_claude_max_available()
            print(f"[QUEUE] tokens monitor: available={available}")
            if available:
                # Reanudar: marcar paused → queued
                with QUEUE_LOCK:
                    for it in QUEUE:
                        if it.get("status") == "paused_waiting_tokens":
                            it["status"] = "queued"
                            it["_resumed_at"] = datetime.now(timezone.utc).isoformat()
                QUEUE_TOKENS_BLOCKED_UNTIL = None
                _save_queue_state()
                # Arrancar worker si no está corriendo
                with QUEUE_LOCK:
                    if QUEUE_WORKER is None or not QUEUE_WORKER.is_alive():
                        QUEUE_WORKER = threading.Thread(target=_queue_worker, daemon=True)
                        QUEUE_WORKER.start()
                print("[QUEUE] tokens monitor: items reanudados, worker arrancado")
                break

            # No disponibles aún. Esperar y reintentar.
            _t.sleep(QUEUE_TOKENS_CHECK_INTERVAL_S)
        print("[QUEUE] tokens monitor: finalizado")

    def _classify_run_result(isin: str, run_id: str) -> tuple[str, int | None]:
        """Decide status final del item de cola tras terminar el run.

        Reglas:
        - RUNS[run_id]["status"] == "done"      → "done"
        - RUNS[run_id]["status"] == "failed"
          → si output.json existe en disco        → "completed_with_warnings"
          → si NO                                 → "failed"
        - cualquier otro                          → ese mismo
        """
        final = RUNS.get(run_id, {})
        status = final.get("status", "unknown")
        exit_code = final.get("exit_code")
        if status == "failed":
            output_json = DATA_DIR / "funds" / isin / "output.json"
            meta_json = DATA_DIR / "funds" / isin / "meta_report.json"
            if output_json.exists() and meta_json.exists():
                return "completed_with_warnings", exit_code
        return status, exit_code

    # W1-W4 (2026-05-21): Watchdog que detecta inconsistencias del sistema.
    # Caso real que ocurrió: bat murió por cierre sesión Windows, watch_run y
    # queue_worker no detectaron el exit, los chips quedaron "running" 12h+.
    # El watchdog corre cada 60s y verifica para cada item "running":
    # - W3: PID sigue vivo (vía psutil)
    # - W2: log creció en últimos N min
    # Si NO → marca interrupted (W4).
    WATCHDOG_INTERVAL_S = 60
    WATCHDOG_LOG_STALE_MIN = 40  # Si NINGÚN output del run (log+skill logs+data
    # dir) crece en 40 min, asumir muerto. Subido 15→40: extract-pdfs escribe
    # incrementalmente (cubierto por el check de data dir a cualquier umbral),
    # pero analyst/manager/letters-cowork escriben su salida UNA sola vez al
    # final → durante su generación (10-25 min) no hay actividad de ficheros.
    # 40 min da margen sin matarlas. Failsafe último: QUEUE_MAX_RUN_SECONDS (4h).

    def _start_watchdog():
        global WATCHDOG_THREAD
        if WATCHDOG_THREAD is not None and WATCHDOG_THREAD.is_alive():
            return
        WATCHDOG_THREAD = threading.Thread(target=_watchdog_loop, daemon=True)
        WATCHDOG_THREAD.start()
        print("[WATCHDOG] arrancado")

    def _is_pid_alive(pid: int) -> "bool | None":
        """True si PID existe, False si no existe, None si no podemos verificar."""
        if not pid:
            return None
        try:
            import psutil
            return psutil.pid_exists(pid)
        except ImportError:
            # Fallback Windows con tasklist (lento, pero funciona sin deps)
            try:
                if sys.platform == "win32":
                    result = subprocess.run(
                        ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                        capture_output=True, text=True, timeout=5,
                    )
                    return str(pid) in result.stdout
            except Exception:
                pass
            return None
        except Exception:
            return None

    def _log_last_activity_min(log_path: str) -> "float | None":
        """Minutos desde la última escritura al log. None si no podemos leer."""
        if not log_path:
            return None
        try:
            p = Path(log_path)
            if not p.exists():
                return None
            mtime = p.stat().st_mtime
            return (time.time() - mtime) / 60.0
        except Exception:
            return None

    def _run_last_activity_min(log_path: str, isin: str) -> "float | None":
        """Minutos desde la ÚLTIMA actividad de todo el run, no solo del log
        principal. CRÍTICO: durante una skill cowork (`claude -p`) la salida va a
        logs/skill_*_{ISIN}.log y en print-mode NO se vuelca hasta terminar, así
        que el log principal queda 'mudo' 5-15 min. Sin esto el watchdog mataba la
        skill por falso 'cuelgue'. Tomamos el mtime MÁS RECIENTE entre:
          - el log principal del run
          - los skill logs del ISIN
          - los ficheros que las skills escriben incrementalmente en
            data/funds/{ISIN}/ (extracted/, bundle/, *.json)
        """
        newest = 0.0
        try:
            if log_path and Path(log_path).exists():
                newest = max(newest, Path(log_path).stat().st_mtime)
        except Exception:
            pass
        try:
            for sl in LOGS_DIR.glob(f"skill_*_{isin}*.log"):
                newest = max(newest, sl.stat().st_mtime)
        except Exception:
            pass
        try:
            fund_dir = ROOT / "data" / "funds" / isin
            if fund_dir.exists():
                for f in fund_dir.rglob("*"):
                    if f.is_file():
                        try:
                            newest = max(newest, f.stat().st_mtime)
                        except Exception:
                            pass
        except Exception:
            pass
        if newest <= 0:
            return None
        return (time.time() - newest) / 60.0

    def _watchdog_loop():
        """Loop cada 60s: revisa items running y detecta zombies por PID
        muerto o log stale. Marca interrupted automáticamente."""
        while True:
            try:
                _watchdog_tick()
            except Exception as e:
                import traceback
                print(f"[WATCHDOG] error en tick: {e}")
                traceback.print_exc()
            time.sleep(WATCHDOG_INTERVAL_S)

    def _watchdog_tick():
        """Una iteración del watchdog. Detecta:
        1. Items running con PID que ya no existe → mark interrupted
        2. Items running cuyo log no ha crecido en WATCHDOG_LOG_STALE_MIN min → mark interrupted
        3. Items running cuyo proc.returncode != None pero status no fue actualizado → reclasificar
        """
        with QUEUE_LOCK:
            running_items = [it for it in QUEUE if it.get("status") == "running"]
        if not running_items:
            return

        changes_made = False
        for it in running_items:
            run_id = it.get("run_id")
            if not run_id:
                continue
            run = RUNS.get(run_id, {})
            isin = it.get("isin", "")

            # W3: PID verification
            pid = run.get("pid")
            pid_alive = _is_pid_alive(pid)
            if pid_alive is False:
                print(f"[WATCHDOG] {isin} (run {run_id}) PID={pid} NO EXISTE — marcando interrupted")
                with QUEUE_LOCK:
                    it["status"] = "interrupted"
                    it["finished_at"] = datetime.now(timezone.utc).isoformat()
                    it["error"] = f"watchdog: PID {pid} murió, log no detectó exit"
                # Actualizar también RUNS para coherencia
                if run_id in RUNS:
                    RUNS[run_id]["status"] = "interrupted"
                    RUNS[run_id]["end_time"] = it["finished_at"]
                changes_made = True
                continue

            # W2: staleness sobre TODO el footprint del run (log principal +
            # skill logs + data/funds/{ISIN}/). Antes solo miraba el log
            # principal → mataba skills cowork largas por falso cuelgue.
            log_path = run.get("log_path", "")
            stale_min = _run_last_activity_min(log_path, isin)
            if stale_min is not None and stale_min > WATCHDOG_LOG_STALE_MIN:
                # Verificar adicionalmente que pid_alive es None (no podemos saber) o True
                # Si pid_alive es True pero log está stale 15+ min, asumir cuelgue
                print(f"[WATCHDOG] {isin} (run {run_id}) log stale {stale_min:.1f}min, PID_alive={pid_alive} — marcando interrupted")
                with QUEUE_LOCK:
                    it["status"] = "interrupted"
                    it["finished_at"] = datetime.now(timezone.utc).isoformat()
                    it["error"] = f"watchdog: log sin actividad {stale_min:.1f}min"
                if run_id in RUNS:
                    RUNS[run_id]["status"] = "interrupted"
                    RUNS[run_id]["end_time"] = it["finished_at"]
                changes_made = True
                # Intentar matar el proceso colgado si sigue vivo
                if pid_alive and pid:
                    try:
                        if sys.platform == "win32":
                            subprocess.run(["taskkill", "/F", "/PID", str(pid), "/T"],
                                           capture_output=True, timeout=5)
                        else:
                            import os
                            os.kill(pid, 9)
                        print(f"[WATCHDOG] proceso PID={pid} matado por staleness")
                    except Exception as e:
                        print(f"[WATCHDOG] no se pudo matar PID={pid}: {e}")
                continue

            # W3b: proc.returncode no None pero status del item sigue running
            proc = run.get("_proc")
            if proc is not None:
                try:
                    rc = proc.poll()
                    if rc is not None and run.get("status") not in ("done", "failed"):
                        # watch_run no actualizó RUNS, hazlo aquí
                        print(f"[WATCHDOG] {isin} subprocess terminó (rc={rc}) pero watch_run no actualizó")
                        RUNS[run_id]["status"] = "done" if rc == 0 else "failed"
                        RUNS[run_id]["exit_code"] = rc
                        RUNS[run_id]["end_time"] = datetime.now(timezone.utc).isoformat()
                        changes_made = True
                except Exception:
                    pass

        if changes_made:
            _save_queue_state()

    def _detect_orphan_bats():
        """M2 (2026-05-19): al arrancar, detecta procesos cmd/python que
        parezcan invocaciones huérfanas de analizar_fondo.bat (de un web_server
        previo que cayó). Solo REPORTA, no mata.

        Si psutil no está instalado, skip silencioso.
        """
        try:
            import psutil  # type: ignore
        except ImportError:
            return  # no podemos detectar sin psutil

        my_pid = os.getpid()
        suspects = []
        try:
            for p in psutil.process_iter(["pid", "name", "cmdline", "create_time", "ppid"]):
                try:
                    info = p.info
                    name = (info.get("name") or "").lower()
                    if name not in ("cmd.exe", "python.exe"):
                        continue
                    cmdline = info.get("cmdline") or []
                    cmd_str = " ".join(str(c) for c in cmdline).lower()
                    if "analizar_fondo.bat" not in cmd_str and "analizar_fondo" not in cmd_str:
                        continue
                    # Skip si es hijo de mi proceso actual (es bat lanzado por mí en esta sesión)
                    if info.get("ppid") == my_pid:
                        continue
                    age_min = (time.time() - (info.get("create_time") or 0)) / 60
                    suspects.append({
                        "pid": info.get("pid"),
                        "name": info.get("name"),
                        "age_min": round(age_min, 1),
                        "cmd": cmd_str[:120],
                    })
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
        except Exception as e:
            print(f"[STARTUP] error detectando huérfanos: {e}")
            return

        if not suspects:
            return

        print("=" * 70)
        print(f"[STARTUP] [WARN] Detectados {len(suspects)} proceso(s) bat huérfano(s):")
        for s in suspects:
            print(f"  PID={s['pid']} {s['name']} ({s['age_min']}min) — {s['cmd']}")
        print("Si son legítimos (otro web_server activo) ignora. Si no:")
        for s in suspects:
            print(f"  Stop-Process -Id {s['pid']} -Force")
        print("=" * 70)

    def _queue_worker():
        """Procesa la cola secuencialmente. Se ejecuta en un thread daemon."""
        global QUEUE_WORKER, QUEUE_STARTED_AT
        QUEUE_STARTED_AT = datetime.now(timezone.utc).isoformat()
        print(f"[QUEUE] worker arrancado a las {QUEUE_STARTED_AT}")
        try:
            while True:
                # Sacar próximo ítem queued
                next_item = None
                with QUEUE_LOCK:
                    for it in QUEUE:
                        if it.get("status") == "queued":
                            next_item = it
                            it["status"] = "running"
                            it["started_at"] = datetime.now(timezone.utc).isoformat()
                            break
                if next_item is None:
                    break  # Nada más en cola
                _save_queue_state()  # B3: persistir tras cada cambio

                isin = next_item["isin"]
                force_cold = next_item.get("cold_start", True)
                apply_feedback = bool(next_item.get("apply_feedback", False))
                payload, status = _start_analysis_for_isin(
                    isin, force_cold, apply_feedback=apply_feedback,
                )
                if status != 200:
                    next_item["status"] = "failed"
                    next_item["error"] = payload.get("error", "unknown")
                    next_item["finished_at"] = datetime.now(timezone.utc).isoformat()
                    print(f"[QUEUE] {isin} lanzamiento FAILED: {next_item['error']}")
                    _save_queue_state()
                    continue

                run_id = payload["run_id"]
                next_item["run_id"] = run_id
                _save_queue_state()

                # B1: polling robusto del subprocess
                # P4: ahora también detecta agotamiento de tokens Claude Max
                proc = RUNS.get(run_id, {}).get("_proc")
                token_exhausted = False
                if proc:
                    print(f"[QUEUE] {isin} pid={proc.pid} esperando con poll()…")
                    exit_code_observed, token_exhausted = _wait_for_proc_with_polling(proc, run_id, isin)
                    if token_exhausted:
                        print(f"[QUEUE] {isin} TOKEN EXHAUSTION — pausando cola")
                    else:
                        print(f"[QUEUE] {isin} subprocess terminó con exit_code={exit_code_observed}")
                else:
                    print(f"[QUEUE] {isin} sin proc — no se puede esperar; siguiendo")

                if token_exhausted:
                    # P4: marcar item como paused + todos los queued siguientes
                    # también se "congelan" (no procesan hasta reanudación)
                    blocked_until = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                    QUEUE_TOKENS_BLOCKED_UNTIL_HRS = QUEUE_TOKENS_DEFAULT_WAIT_HOURS
                    next_item["status"] = "paused_waiting_tokens"
                    next_item["paused_at"] = blocked_until
                    next_item["pause_reason"] = "token_exhaustion_detected_in_log"
                    global QUEUE_TOKENS_BLOCKED_UNTIL
                    from datetime import timedelta
                    QUEUE_TOKENS_BLOCKED_UNTIL = (datetime.now(timezone.utc) +
                                                  timedelta(hours=QUEUE_TOKENS_BLOCKED_UNTIL_HRS)).isoformat()
                    _save_queue_state()
                    # Arrancar monitor que decidirá cuándo reanudar
                    _start_tokens_monitor()
                    # Salir del loop del worker — el monitor lo reanudará
                    print(f"[QUEUE] worker pausando cola hasta {QUEUE_TOKENS_BLOCKED_UNTIL}")
                    break

                # B2: clasificación final (lee RUNS que watch_run actualizó)
                final_status, exit_code = _classify_run_result(isin, run_id)
                # P4 extra check: aún si el run reportó "done" o "failed", revisar
                # el log final por si hubo agotamiento que no detectamos durante el poll
                if final_status in ("failed", "completed_with_warnings"):
                    log_path = RUNS.get(run_id, {}).get("log_path", "")
                    if log_path and Path(log_path).exists():
                        try:
                            with Path(log_path).open("r", encoding="utf-8", errors="replace") as f:
                                tail = f.read()[-3000:].lower()
                            if _detect_token_exhaustion(tail):
                                # Reclasificar como paused y devolver a cola
                                print(f"[QUEUE] {isin} token exhaustion detectado en log post-mortem — pausando")
                                final_status = "paused_waiting_tokens"
                                next_item["pause_reason"] = "token_exhaustion_detected_post_mortem"
                                from datetime import timedelta
                                QUEUE_TOKENS_BLOCKED_UNTIL = (datetime.now(timezone.utc) +
                                                              timedelta(hours=QUEUE_TOKENS_DEFAULT_WAIT_HOURS)).isoformat()
                                _start_tokens_monitor()
                        except Exception:
                            pass
                next_item["status"] = final_status
                next_item["exit_code"] = exit_code
                final = RUNS.get(run_id, {})
                next_item["duration_seconds"] = final.get("duration_seconds")
                next_item["finished_at"] = final.get("end_time") or datetime.now(timezone.utc).isoformat()
                _save_queue_state()
                print(f"[QUEUE] {isin} status final = {final_status} (exit={exit_code})")
                # Si pausamos en post-mortem, salir del worker loop
                if final_status == "paused_waiting_tokens":
                    break
        except Exception as e:
            # Capturar cualquier excepción no esperada para que NO mate el thread silenciosamente
            import traceback
            print(f"[QUEUE] EXCEPCIÓN no manejada en worker: {e}")
            traceback.print_exc()
        finally:
            QUEUE_WORKER = None
            QUEUE_STARTED_AT = None
            _save_queue_state()
            print("[QUEUE] worker finalizado")

    @app.route("/api/analyze-batch", methods=["POST"])
    def api_analyze_batch():
        """Encola N ISINs para análisis secuencial.

        Body: {"isins": ["X","Y",...], "cold_start": bool (default True)}
        Devuelve: {"queued": N, "invalid": [...], "items": [...], "worker_running": bool}
        """
        global QUEUE_WORKER
        data = request.get_json(silent=True) or {}
        raw = data.get("isins")
        if not isinstance(raw, list):
            return jsonify({"error": "body debe contener 'isins' como lista"}), 400
        force_cold = bool(data.get("cold_start", app.config["FUND_COLD_START"]))
        # T3.5: apply_feedback en el batch (carry-through al worker)
        apply_feedback = bool(data.get("apply_feedback", False))

        # Normalizar + deduplicar + validar
        seen = set()
        valid: list[str] = []
        invalid: list[dict] = []
        for entry in raw:
            isin = str(entry or "").strip().upper()
            if not isin:
                continue
            if isin in seen:
                continue
            seen.add(isin)
            if not ISIN_REGEX.match(isin):
                invalid.append({"isin": isin, "reason": "formato inválido"})
                continue
            valid.append(isin)

        if not valid:
            return jsonify({
                "error": "no hay ISINs válidos para encolar",
                "invalid": invalid,
            }), 400

        # Filtrar los que ya están en cola (queued/running)
        added: list[dict] = []
        skipped: list[dict] = []
        now_iso = datetime.now(timezone.utc).isoformat()
        with QUEUE_LOCK:
            queued_or_running = {
                it["isin"] for it in QUEUE
                if it.get("status") in ("queued", "running")
            }
            for isin in valid:
                if isin in queued_or_running:
                    skipped.append({"isin": isin, "reason": "ya en cola"})
                    continue
                # También skip si hay un RUN activo para ese ISIN
                already = any(
                    r.get("isin") == isin and r.get("status") == "running"
                    for r in RUNS.values()
                )
                if already:
                    skipped.append({"isin": isin, "reason": "ya hay run activo"})
                    continue
                item = {
                    "isin": isin,
                    "queued_at": now_iso,
                    "status": "queued",
                    "cold_start": force_cold,
                    "apply_feedback": apply_feedback,
                }
                QUEUE.append(item)
                added.append(item)

        # B3: persistir cola tras encolar
        _save_queue_state()

        # Arrancar worker si no está corriendo (con lock para evitar race)
        with QUEUE_LOCK:
            if QUEUE_WORKER is None or not QUEUE_WORKER.is_alive():
                QUEUE_WORKER = threading.Thread(target=_queue_worker, daemon=True)
                QUEUE_WORKER.start()

        return jsonify({
            "queued": len(added),
            "added": added,
            "skipped": skipped,
            "invalid": invalid,
            "worker_running": True,
            "queue_size": len(QUEUE),
        })

    @app.route("/api/status", methods=["GET"])
    def api_status():
        """W7 (2026-05-21): health check completo del sistema para monitor permanente.

        Devuelve: estado threads (worker/watchdog/tokens_monitor), counts cola,
        última actividad detectada, alertas pendientes, etc.

        El frontend lo polea cada 10s y muestra un strip verde/amarillo/rojo.
        """
        worker_alive = QUEUE_WORKER is not None and QUEUE_WORKER.is_alive()
        watchdog_alive = WATCHDOG_THREAD is not None and WATCHDOG_THREAD.is_alive()
        tokens_alive = QUEUE_TOKENS_MONITOR is not None and QUEUE_TOKENS_MONITOR.is_alive()

        items = _queue_snapshot()
        n_running = sum(1 for it in items if it.get("status") == "running")
        n_queued = sum(1 for it in items if it.get("status") == "queued")
        n_paused = sum(1 for it in items if it.get("status") == "paused_waiting_tokens")
        n_interrupted = sum(1 for it in items if it.get("status") == "interrupted")
        n_failed = sum(1 for it in items if it.get("status") == "failed")
        n_warnings = sum(1 for it in items if it.get("status") == "completed_with_warnings")
        n_done = sum(1 for it in items if it.get("status") == "done")

        # Última actividad detectable: mtime del log más reciente de los running,
        # o último archivo escrito en data/funds/ recientemente.
        last_activity = None
        running_isins = [it.get("isin") for it in items if it.get("status") == "running"]
        for it in items:
            if it.get("status") == "running" and isinstance(it.get("log_last_activity_min"), (int, float)):
                if last_activity is None or it["log_last_activity_min"] < last_activity:
                    last_activity = it["log_last_activity_min"]

        # Detectar items running con staleness >10 min (warning visible)
        stale_running = [
            it.get("isin") for it in items
            if it.get("status") == "running"
            and isinstance(it.get("log_last_activity_min"), (int, float))
            and it["log_last_activity_min"] > 10
        ]

        # Determinar status global del sistema
        if not worker_alive and n_queued > 0:
            global_status = "error"
            global_msg = f"Worker thread muerto pero hay {n_queued} items pendientes"
        elif not watchdog_alive:
            global_status = "error"
            global_msg = "Watchdog thread muerto — sin protección contra cuelgues"
        elif stale_running:
            global_status = "warn"
            global_msg = f"{len(stale_running)} run(s) sin actividad >10min: {', '.join(stale_running)}"
        elif n_interrupted > 0 or n_failed > 0:
            global_status = "warn"
            global_msg = f"{n_interrupted + n_failed} item(s) necesitan atención"
        elif n_paused > 0:
            global_status = "warn"
            global_msg = f"{n_paused} item(s) pausado(s) por tokens"
        elif n_running > 0:
            global_status = "running"
            global_msg = f"Analizando {running_isins[0] if running_isins else ''}" + (
                f" (+{n_running-1} más)" if n_running > 1 else ""
            )
        elif n_queued > 0:
            global_status = "running"
            global_msg = f"{n_queued} pendientes en cola"
        else:
            global_status = "idle"
            global_msg = "Sistema en reposo"

        return jsonify({
            "global_status": global_status,
            "global_msg": global_msg,
            "threads": {
                "worker": worker_alive,
                "watchdog": watchdog_alive,
                "tokens_monitor": tokens_alive,
            },
            "queue_counts": {
                "running": n_running,
                "queued": n_queued,
                "paused_tokens": n_paused,
                "interrupted": n_interrupted,
                "failed": n_failed,
                "warnings": n_warnings,
                "done": n_done,
            },
            "running_isins": running_isins,
            "stale_running": stale_running,
            "min_activity_min": last_activity,
            "tokens_blocked_until": QUEUE_TOKENS_BLOCKED_UNTIL,
            "server_time": datetime.now(timezone.utc).isoformat(),
        })

    @app.route("/api/queue", methods=["GET"])
    def api_queue():
        """Estado actual de la cola."""
        items = _queue_snapshot()
        worker_alive = QUEUE_WORKER is not None and QUEUE_WORKER.is_alive()
        tokens_monitor_alive = QUEUE_TOKENS_MONITOR is not None and QUEUE_TOKENS_MONITOR.is_alive()
        return jsonify({
            "items": items,
            "worker_running": worker_alive,
            "started_at": QUEUE_STARTED_AT,
            "n_queued": sum(1 for it in items if it.get("status") == "queued"),
            "n_running": sum(1 for it in items if it.get("status") == "running"),
            "n_done": sum(1 for it in items if it.get("status") == "done"),
            "n_failed": sum(1 for it in items if it.get("status") == "failed"),
            "n_paused_tokens": sum(1 for it in items if it.get("status") == "paused_waiting_tokens"),
            "n_completed_warnings": sum(1 for it in items if it.get("status") == "completed_with_warnings"),
            "n_interrupted": sum(1 for it in items if it.get("status") == "interrupted"),
            # P4: estado de tokens
            "tokens_blocked_until": QUEUE_TOKENS_BLOCKED_UNTIL,
            "tokens_last_check": QUEUE_TOKENS_LAST_CHECK,
            "tokens_monitor_running": tokens_monitor_alive,
        })

    @app.route("/api/supabase/status", methods=["GET"])
    def api_supabase_status():
        """Estado del proyecto Supabase (ACTIVE_HEALTHY / INACTIVE / ...).
        Permite al catálogo mostrar si está pausado y ofrecer reactivar."""
        try:
            from tools.supabase_admin import get_status
            return jsonify(get_status())
        except Exception as e:
            return jsonify({"status": "UNKNOWN", "healthy": False, "error": str(e)[:200]}), 200

    @app.route("/api/supabase/restore", methods=["POST"])
    def api_supabase_restore():
        """Reactiva (un-pause) el proyecto Supabase vía Management API.
        Requiere SUPABASE_ACCESS_TOKEN en .env."""
        try:
            from tools.supabase_admin import ensure_active
            res = ensure_active()
            return jsonify(res), (200 if res.get("ok", True) else 502)
        except Exception as e:
            return jsonify({"ok": False, "message": str(e)[:200]}), 500

    @app.route("/api/queue/check-tokens", methods=["POST"])
    def api_queue_check_tokens():
        """P4: chequeo manual de tokens. Si están OK, reanuda items paused.
        Útil cuando el user sabe que ya tiene tokens disponibles y no quiere
        esperar al chequeo automático cada 30 min.
        """
        with QUEUE_LOCK:
            n_paused = sum(1 for it in QUEUE if it.get("status") == "paused_waiting_tokens")
        if n_paused == 0:
            return jsonify({
                "ok": True,
                "message": "no hay items paused, no se hace check",
                "n_paused": 0,
            })
        available = _test_claude_max_available()
        global QUEUE_TOKENS_LAST_CHECK, QUEUE_TOKENS_BLOCKED_UNTIL, QUEUE_WORKER
        QUEUE_TOKENS_LAST_CHECK = datetime.now(timezone.utc).isoformat()
        if not available:
            return jsonify({
                "ok": True,
                "available": False,
                "message": f"tokens aún no disponibles, {n_paused} items siguen paused",
                "n_paused": n_paused,
            })
        # Disponibles: reanudar
        n_resumed = 0
        with QUEUE_LOCK:
            for it in QUEUE:
                if it.get("status") == "paused_waiting_tokens":
                    it["status"] = "queued"
                    it["_resumed_at"] = datetime.now(timezone.utc).isoformat()
                    n_resumed += 1
            QUEUE_TOKENS_BLOCKED_UNTIL = None
            if QUEUE_WORKER is None or not QUEUE_WORKER.is_alive():
                QUEUE_WORKER = threading.Thread(target=_queue_worker, daemon=True)
                QUEUE_WORKER.start()
        _save_queue_state()
        return jsonify({
            "ok": True,
            "available": True,
            "message": f"tokens disponibles, {n_resumed} items reanudados",
            "n_resumed": n_resumed,
        })

    @app.route("/api/queue/<isin>", methods=["DELETE"])
    def api_queue_remove(isin: str):
        """Quita un ISIN de la cola (solo si está pendiente)."""
        isin = (isin or "").strip().upper()
        removed = False
        with QUEUE_LOCK:
            for i, it in enumerate(QUEUE):
                if it["isin"] == isin and it.get("status") == "queued":
                    QUEUE.pop(i)
                    removed = True
                    break
        if removed:
            _save_queue_state()
            return jsonify({"ok": True, "isin": isin, "removed": True})
        return jsonify({"error": f"ISIN {isin} no está en cola pendiente"}), 404

    @app.route("/api/queue/clear-finished", methods=["POST"])
    def api_queue_clear_finished():
        """Limpia entradas terminadas/zombies de la cola (mantiene queued y running activos).
        Estados que se limpian: done, failed, completed_with_warnings, interrupted, skipped.
        """
        with QUEUE_LOCK:
            before = len(QUEUE)
            QUEUE[:] = [it for it in QUEUE if it.get("status") in ("queued", "running")]
            removed = before - len(QUEUE)
        _save_queue_state()
        return jsonify({"ok": True, "removed": removed, "remaining": len(QUEUE)})

    @app.route("/api/queue/force-clear", methods=["POST"])
    def api_queue_force_clear():
        """Limpia TODOS los items de la cola, incluso los "running" (zombies).
        Útil tras un crash/reinicio donde quedaron items huérfanos."""
        with QUEUE_LOCK:
            before = len(QUEUE)
            QUEUE.clear()
        _save_queue_state()
        return jsonify({"ok": True, "removed": before, "remaining": 0})

    # ─────────────────────────────────────────────────────────────────────
    # P4d (2026-05-19): Reanudar items interrumpidos / pausados
    # ─────────────────────────────────────────────────────────────────────
    RESUMABLE_STATES = ("interrupted", "paused_waiting_tokens", "failed", "completed_with_warnings")

    def _resume_item(isin: str, cold_start: bool) -> tuple[bool, str]:
        """Marca un item como queued (de nuevo) para que el worker lo coja.
        Devuelve (success, message). cold_start=True implica que al lanzar
        el bat se moverá data/funds/<ISIN> a backup; False reusa lo existente.
        """
        global QUEUE_WORKER
        with QUEUE_LOCK:
            for it in QUEUE:
                if it["isin"] != isin:
                    continue
                if it.get("status") not in RESUMABLE_STATES:
                    return False, f"status={it.get('status')} no es reanudable"
                # Reset campos
                it["status"] = "queued"
                it["cold_start"] = bool(cold_start)
                it["queued_at"] = datetime.now(timezone.utc).isoformat()
                # Limpiar marcadores de la sesión anterior
                for k in ("run_id", "started_at", "finished_at", "exit_code",
                          "duration_seconds", "error", "paused_at", "pause_reason"):
                    it.pop(k, None)
                it["_resumed"] = True
                it["_resumed_at"] = datetime.now(timezone.utc).isoformat()
                break
            else:
                return False, "ISIN no encontrado en cola"

        _save_queue_state()
        with QUEUE_LOCK:
            if QUEUE_WORKER is None or not QUEUE_WORKER.is_alive():
                QUEUE_WORKER = threading.Thread(target=_queue_worker, daemon=True)
                QUEUE_WORKER.start()
        return True, "reanudado"

    @app.route("/api/queue/<isin>/resume", methods=["POST"])
    def api_queue_resume_one(isin: str):
        """Reanuda un item interrumpido/pausado.

        Body JSON: {"cold_start": false (default)}
        - cold_start=False: bat reusa datos parciales en data/funds/<ISIN>/
        - cold_start=True: bat mueve la carpeta a .bak y empieza limpio
        """
        isin = (isin or "").strip().upper()
        body = request.get_json(silent=True) or {}
        cold_start = bool(body.get("cold_start", False))
        ok, msg = _resume_item(isin, cold_start)
        if not ok:
            return jsonify({"error": msg}), 404
        return jsonify({"ok": True, "isin": isin, "cold_start": cold_start, "message": msg})

    @app.route("/api/queue/resume-all", methods=["POST"])
    def api_queue_resume_all():
        """Reanuda TODOS los items reanudables (interrupted, paused, failed,
        completed_with_warnings). Body JSON: {"cold_start": false (default),
        "include_states": [...] opcional para filtrar}."""
        body = request.get_json(silent=True) or {}
        cold_start = bool(body.get("cold_start", False))
        states_filter = body.get("include_states")
        if states_filter and isinstance(states_filter, list):
            target_states = tuple(s for s in states_filter if s in RESUMABLE_STATES)
        else:
            # Por defecto solo interrupted + paused (los más comunes de reanudar)
            target_states = ("interrupted", "paused_waiting_tokens")

        resumed_isins = []
        skipped_isins = []
        with QUEUE_LOCK:
            target_items = [it for it in QUEUE if it.get("status") in target_states]
        for it in target_items:
            ok, msg = _resume_item(it["isin"], cold_start)
            if ok:
                resumed_isins.append(it["isin"])
            else:
                skipped_isins.append({"isin": it["isin"], "reason": msg})
        return jsonify({
            "ok": True,
            "n_resumed": len(resumed_isins),
            "resumed": resumed_isins,
            "skipped": skipped_isins,
            "cold_start": cold_start,
            "target_states": list(target_states),
        })

    # ─────────────────────────────────────────────────────────────────────
    # T3.5 (2026-05-28): endpoints de human feedback
    # ─────────────────────────────────────────────────────────────────────

    @app.route("/api/feedback/<isin>", methods=["GET"])
    def api_feedback_list(isin: str):
        """Lista todos los feedbacks (cualquier estado) de un fondo."""
        from tools import feedback_store as fs
        isin = (isin or "").strip().upper()
        if not ISIN_REGEX.match(isin):
            return jsonify({"error": "ISIN inválido"}), 400
        return jsonify({"isin": isin, "feedbacks": fs.list_feedback(isin)})

    @app.route("/api/feedback/<isin>/parse", methods=["POST"])
    def api_feedback_parse(isin: str):
        """Llama a Haiku para estructurar el texto libre del usuario.
        NO guarda — solo devuelve preview editable para que el user confirme.

        Body: {raw_text: str, raw_urls?: [str], fund_name?: str, gestora?: str}
        Devuelve: {structured_items: [...], parse_meta: {method, n_items, error?}}
        """
        from tools.feedback_parser import parse_feedback
        isin = (isin or "").strip().upper()
        if not ISIN_REGEX.match(isin):
            return jsonify({"error": "ISIN inválido"}), 400
        data = request.get_json(silent=True) or {}
        raw_text = (data.get("raw_text") or "").strip()
        raw_urls = data.get("raw_urls") or []
        fund_name = (data.get("fund_name") or "").strip()
        gestora = (data.get("gestora") or "").strip()
        if not raw_text and not raw_urls:
            return jsonify({"error": "raw_text o raw_urls requerido"}), 400
        result = parse_feedback(
            raw_text=raw_text, raw_urls=raw_urls,
            isin=isin, fund_name=fund_name, gestora=gestora,
        )
        return jsonify(result)

    @app.route("/api/feedback/<isin>", methods=["POST"])
    def api_feedback_save(isin: str):
        """Guarda un feedback estructurado (potencialmente editado por el user
        tras el preview de /parse).

        Body: {raw_text, raw_urls, structured_items, fund_name?}
        Devuelve el feedback creado.
        """
        from tools import feedback_store as fs
        isin = (isin or "").strip().upper()
        if not ISIN_REGEX.match(isin):
            return jsonify({"error": "ISIN inválido"}), 400
        data = request.get_json(silent=True) or {}
        raw_text = (data.get("raw_text") or "").strip()
        raw_urls = data.get("raw_urls") or []
        items = data.get("structured_items") or []
        fund_name = (data.get("fund_name") or "").strip()
        if not items:
            return jsonify({"error": "structured_items vacío — nada que guardar"}), 400
        try:
            fb = fs.append_feedback(
                isin=isin, raw_text=raw_text, raw_urls=raw_urls,
                structured_items=items, fund_name=fund_name,
            )
            return jsonify({"ok": True, "feedback": fb})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/feedback/<isin>/<feedback_id>", methods=["DELETE"])
    def api_feedback_delete(isin: str, feedback_id: str):
        """Borra un feedback pending (los applied/resolved son histórico inmutable)."""
        from tools import feedback_store as fs
        isin = (isin or "").strip().upper()
        if not ISIN_REGEX.match(isin):
            return jsonify({"error": "ISIN inválido"}), 400
        ok = fs.delete_feedback(isin, feedback_id)
        if not ok:
            return jsonify({
                "ok": False,
                "error": "feedback no existe o no es pending (no se puede borrar applied/resolved)",
            }), 404
        return jsonify({"ok": True})

    @app.route("/api/index/resolve", methods=["GET"])
    def api_index_resolve():
        """Busca un índice en Investing por nombre. ?q=MSCI India
        Devuelve candidatos {pair_id, symbol, exchange} para que el user elija."""
        q = (request.args.get("q") or "").strip()
        if len(q) < 2:
            return jsonify({"error": "query muy corto"}), 400
        try:
            from tools.investing_downloader import resolve as inv_resolve
            cands = inv_resolve(q)
            # prioriza los de exchange tipo índice
            cands.sort(key=lambda c: 0 if "index" in str(c.get("exchange", "")).lower() else 1)
            return jsonify({"ok": True, "query": q, "candidates": cands})
        except Exception as e:  # noqa: BLE001
            return jsonify({"ok": False, "error": str(e)[:200]}), 502

    @app.route("/api/index/add", methods=["POST"])
    def api_index_add():
        """Añade un índice nuevo: descarga histórico de Investing, lo registra y lo
        sube a Supabase (aparece en el dashboard). Body:
        {name, pair_id, fx?(1=EUR/USD), symbol?, subdir?}"""
        from tools.investing_downloader import add_index
        data = request.get_json(silent=True) or {}
        name = (data.get("name") or "").strip()
        pair_id = data.get("pair_id")
        if not name or not pair_id:
            return jsonify({"ok": False, "error": "name y pair_id requeridos"}), 400
        try:
            fx = int(data["fx"]) if data.get("fx") else None
            r = add_index(name, int(pair_id), fx=fx, symbol=data.get("symbol"),
                          csv_name=data.get("csv_name") or name,
                          subdir=(data.get("subdir") or "Renta Variable"),
                          upload=True)
            return jsonify(r), (200 if r.get("ok") else 400)
        except Exception as e:  # noqa: BLE001
            return jsonify({"ok": False, "error": str(e)[:200]}), 500

    @app.route("/api/runs")
    def api_runs():
        """Lista runs (activos + recientes)."""
        runs = []
        for r in RUNS.values():
            runs.append({k: v for k, v in r.items() if not k.startswith("_")})
        runs.sort(key=lambda x: x.get("start_time", ""), reverse=True)
        # Limitar a últimos 50
        return jsonify(runs[:50])

    @app.route("/api/runs/by-isin/<isin>")
    def api_run_by_isin(isin: str):
        """T2a (2026-05-27): devuelve el run más reciente para un ISIN.

        Usado por el botón 'Ver progreso' del catalog para encontrar el run_id
        activo de un fondo sin que el frontend tenga que recordarlo.
        Devuelve 404 si no hay runs para ese ISIN.
        """
        isin = (isin or "").strip().upper()
        if not isin:
            return jsonify({"error": "isin required"}), 400
        matching = [r for r in RUNS.values() if r.get("isin", "").upper() == isin]
        if not matching:
            return jsonify({"error": "no runs for isin"}), 404
        # El más reciente por start_time
        latest = max(matching, key=lambda r: r.get("start_time", ""))
        safe = {k: v for k, v in latest.items() if not k.startswith("_")}
        return jsonify(safe)

    @app.route("/api/runs/<run_id>")
    def api_run_detail(run_id: str):
        """Detalle del run + tail del log + checkpoints de progreso.

        Query params (T2a 2026-05-27):
          - tail=N: nº de líneas del log a devolver (default 60, max 500)
        """
        r = RUNS.get(run_id)
        if not r:
            return jsonify({"error": "run not found"}), 404

        try:
            tail_n = int(request.args.get("tail", 60))
        except (TypeError, ValueError):
            tail_n = 60
        tail_n = max(1, min(tail_n, 500))

        safe = {k: v for k, v in r.items() if not k.startswith("_")}

        # Tail del log
        log_path = Path(r.get("log_path", ""))
        tail_lines: list[str] = []
        log_size = 0
        if log_path.exists():
            try:
                log_size = log_path.stat().st_size
                with log_path.open("r", encoding="utf-8", errors="replace") as f:
                    lines = f.readlines()
                    tail_lines = lines[-tail_n:]
            except Exception as e:
                tail_lines = [f"[error reading log: {e}]"]
        safe["log_tail"] = "".join(tail_lines)
        safe["log_size"] = log_size

        # Checkpoints (qué paso del bat ya completó)
        isin = r.get("isin", "")
        start_iso = r.get("start_time", "")
        try:
            start_ts = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        except Exception:
            start_ts = datetime.now(timezone.utc)

        skill_log = lambda name: LOGS_DIR / f"skill_{name}_{isin}.log"
        fund_dir = DATA_DIR / "funds" / isin
        dashboard_html = DASHBOARD_DIR / f"fund-{isin}.html"

        def file_after_start(p: Path) -> bool:
            if not p.exists():
                return False
            try:
                m = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
                return m >= start_ts.replace(microsecond=0)
            except Exception:
                return p.exists()

        progress = {
            "step1_prep": (
                fund_dir.exists() and (fund_dir / "config.json").exists()
            ),
            "step2_extract_pdfs": file_after_start(skill_log("extract_pdfs")),
            "step3_manager_deep": file_after_start(skill_log("manager_deep")),
            "step4_letters_extract": file_after_start(skill_log("letters_extract")),
            "step5_analyst": file_after_start(skill_log("analyst")),
            "step6_dashboard": file_after_start(dashboard_html),
        }
        safe["progress"] = progress
        safe["progress_pct"] = round(
            sum(1 for v in progress.values() if v) / len(progress) * 100
        )

        # Si terminó, incluir referencia al dashboard generado.
        # Bug fix 2026-05-28: usar URL relativa `fund-<ISIN>.html?v=<mtime>`
        # (igual que getDashboardUrl en catalog.html). El path absoluto
        # `/dashboard/...` daba 404 en el catalog público de Cloudflare
        # Workers porque ahí los HTML se sirven sin prefijo `/dashboard/`.
        if r.get("status") in ("done", "failed") and dashboard_html.exists():
            try:
                mtime = int(dashboard_html.stat().st_mtime)
            except Exception:
                mtime = ""
            safe["dashboard_url"] = (
                f"fund-{isin}.html?v={mtime}" if mtime else f"fund-{isin}.html"
            )

        return jsonify(safe)

    @app.route("/api/runs/<run_id>/cancel", methods=["POST"])
    def api_run_cancel(run_id: str):
        """Cancela un run activo (envía SIGTERM al subprocess)."""
        r = RUNS.get(run_id)
        if not r:
            return jsonify({"error": "run not found"}), 404
        if r.get("status") != "running":
            return jsonify({"error": f"run no está activo (status={r.get('status')})"}), 400
        proc = r.get("_proc")
        if not proc:
            return jsonify({"error": "subprocess no disponible"}), 500
        try:
            proc.terminate()
            return jsonify({"ok": True, "run_id": run_id, "status": "terminating"})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/fund/<isin>", methods=["DELETE"])
    def api_delete_fund(isin: str):
        """W12 (2026-05-26): elimina un fondo de Supabase (DB + Storage).

        Wrapper sobre tools.cleanup_supabase_isins.cleanup_isin para que se
        pueda hacer desde el catalog vía botón 🗑 sin abrir PowerShell.

        NO toca data/funds/<ISIN>/ local. Solo Supabase.
        """
        isin = (isin or "").strip().upper()
        if not ISIN_REGEX.match(isin):
            return jsonify({"error": f"ISIN inválido: {isin}"}), 400
        try:
            from tools.cleanup_supabase_isins import cleanup_isin
            from tools.supabase_client import get_client
            client = get_client()
        except Exception as e:
            return jsonify({"error": f"Supabase no disponible: {e}"}), 503
        try:
            stats = cleanup_isin(client, isin)
        except Exception as e:
            return jsonify({"error": f"cleanup falló: {e}"}), 500
        return jsonify({
            "ok": True,
            "isin": isin,
            "fund_row_action": stats.get("fund_row_action"),
            "fund_group_action": stats.get("fund_group_action"),
            "storage_deleted": stats.get("storage_deleted", []),
            "storage_errors": stats.get("storage_errors", []),
        })

    @app.route("/api/update-fund/<isin>", methods=["POST"])
    def api_update_fund(isin: str):
        """Actualiza campos editables de un fondo en Supabase (tabla `funds`).

        Body JSON con cualquier subset de:
          - clasificacion_user   (str: Top | Bueno | Medio | Clase_similar | Clase_sucia | null)
          - opinion_user         (str)
          - encaje_texto         (str)
          - notas_internas       (str)
          - broker_disponible    (list[str]: MyInvestor, Renta4, Mapfre, ...)  [W13]

        Solo se envían a Supabase los campos presentes en el body.
        """
        isin = (isin or "").strip().upper()
        if not ISIN_REGEX.match(isin):
            return jsonify({"error": f"ISIN inválido: {isin}"}), 400

        body = request.get_json(silent=True) or {}
        if not isinstance(body, dict):
            return jsonify({"error": "body debe ser un objeto JSON"}), 400

        ALLOWED_FIELDS = {
            "clasificacion_user",
            "opinion_user",
            "encaje_texto",
            "notas_internas",
            "broker_disponible",  # W13: lista de brokers donde está el ISIN
        }
        # Campos de TAXONOMÍA (categorización) — viven en fund_taxonomy.json y
        # alimentan tanto el catálogo de fund-analyzer como el fund-dashboard.
        TAX_FIELDS = {
            "categoria", "tipo_activo", "geografia", "divisa",
            "issuer", "estilo", "clase_comercial", "gestora",
        }
        ALLOWED_CLASIF = {"Top", "Bueno", "Medio", "Clase_similar", "Clase_sucia", None}

        update_dict: dict = {}
        tax_dict: dict = {}
        for k, v in body.items():
            if k in TAX_FIELDS:
                tax_dict[k] = (v.strip() if isinstance(v, str) else v) or ""
                continue
            if k not in ALLOWED_FIELDS:
                continue
            if k == "broker_disponible":
                if v is None:
                    v = []
                if not isinstance(v, list):
                    return jsonify({"error": "broker_disponible debe ser lista"}), 400
                v = [str(b).strip() for b in v if str(b).strip()]
            elif isinstance(v, str):
                v = v.strip() or None  # "" → None
            if k == "clasificacion_user" and v not in ALLOWED_CLASIF:
                return jsonify({
                    "error": f"clasificacion_user inválida: {v!r}",
                    "allowed": sorted([c for c in ALLOWED_CLASIF if c]),
                }), 400
            update_dict[k] = v

        if not update_dict and not tax_dict:
            return jsonify({
                "error": "no hay campos válidos para actualizar",
                "allowed_fields": sorted(ALLOWED_FIELDS | TAX_FIELDS),
            }), 400

        # 1) Campos de usuario → Supabase funds (como hasta ahora)
        sb_row = None
        if update_dict:
            try:
                from tools.supabase_client import get_client
                client = get_client()
                res = client.table("funds").update(update_dict).eq("isin", isin).execute()
                data = getattr(res, "data", None) or []
                if not data:
                    return jsonify({"error": f"ISIN {isin} no encontrado en funds"}), 404
                sb_row = data[0] if isinstance(data, list) else data
            except Exception as e:
                return jsonify({"error": f"error actualizando funds: {e}"}), 500

        # 2) Taxonomía → fund_taxonomy.json + push al fund-dashboard
        fd_pushed = False
        if tax_dict:
            try:
                import json as _json
                from pathlib import Path as _Path
                taxf = _Path(__file__).resolve().parent.parent / "data" / "fund_taxonomy.json"
                tx = _json.loads(taxf.read_text(encoding="utf-8")) if taxf.exists() else {"funds": {}}
                tx.setdefault("funds", {})
                ent = tx["funds"].setdefault(isin, {"isin": isin})
                ent.update({k: v for k, v in tax_dict.items()})
                taxf.write_text(_json.dumps(tx, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                return jsonify({"error": f"error guardando taxonomía: {e}"}), 500
            # push al fund-dashboard (mapeo de campos)
            try:
                from tools.funddash_sync import _patch_meta, _post, _repo_isins
                fd_meta = {
                    "isin": isin, "name": ent.get("nombre") or isin,
                    "category": tax_dict.get("categoria", ""),
                    "assetType": tax_dict.get("tipo_activo", ""),
                    "geography": tax_dict.get("geografia", ""),
                    "currency": {"EUR": "Euro", "USD": "USD", "GBP": "GBP", "JPY": "JPY"}.get(
                        tax_dict.get("divisa", ""), tax_dict.get("divisa", "")),
                    "issuer": tax_dict.get("issuer", ""),
                    "className": tax_dict.get("clase_comercial", ""),
                    "gestora": tax_dict.get("gestora", ""),
                }
                fd_meta = {k: v for k, v in fd_meta.items() if v or k in ("isin", "name")}
                if isin in _repo_isins():
                    _patch_meta(isin, fd_meta)
                else:
                    _post({"isin": isin, "meta": fd_meta, "rows": [], "updated_at": "2026-06-15T00:00:00Z"})
                fd_pushed = True
            except Exception as e:
                print(f"[WARN] funddash push falló para {isin}: {e}")

        # Espejo Excel: regenerar SIEMPRE que se categoriza (best-effort, en
        # background para no bloquear la respuesta). Supabase = fuente de verdad;
        # el Excel queda al día solo tras cada cambio de categorización.
        try:
            import threading
            from tools.export_bdd_excel import regenerate as _regen_excel
            threading.Thread(target=lambda: _regen_excel(quiet=True),
                             daemon=True).start()
        except Exception as e:
            print(f"[WARN] no se pudo lanzar regen del Excel espejo: {e}")

        return jsonify({
            "ok": True,
            "isin": isin,
            "updated_fields": sorted(list(update_dict.keys()) + list(tax_dict.keys())),
            "funddash_pushed": fd_pushed,
            "row": sb_row,
        })

    # M2 (2026-05-19): detector de procesos huérfanos del bat.
    # Si el web_server cayó con bats corriendo (cierre sesión Windows, crash),
    # esos bats pueden seguir vivos. Aquí solo los REPORTAMOS — no matamos
    # automáticamente porque puede haber casos legítimos (otro web_server).
    _detect_orphan_bats()

    # B3+B4: cargar cola persistida + marcar zombies (items running tras restart).
    # Si quedaron items "queued" pendientes, arrancar worker para reanudar.
    # P4c (2026-05-19): si hay items paused_waiting_tokens, arrancar también
    # el thread monitor para que reanude solo cuando los tokens vuelvan.
    _load_queue_state()
    has_queued = any(it.get("status") == "queued" for it in QUEUE)
    has_paused = any(it.get("status") == "paused_waiting_tokens" for it in QUEUE)
    if has_queued:
        global QUEUE_WORKER
        with QUEUE_LOCK:
            if QUEUE_WORKER is None or not QUEUE_WORKER.is_alive():
                QUEUE_WORKER = threading.Thread(target=_queue_worker, daemon=True)
                QUEUE_WORKER.start()
                print("[QUEUE] reanudando worker para items pendientes tras restart")
    if has_paused:
        n_paused = sum(1 for it in QUEUE if it.get("status") == "paused_waiting_tokens")
        print(f"[QUEUE] {n_paused} items en paused_waiting_tokens tras restart — arrancando monitor tokens")
        _start_tokens_monitor()

    # W1+W4 (2026-05-21): arrancar watchdog SIEMPRE (incluso si no hay items
    # ahora, detectará inconsistencias futuras). Daemon thread, no bloquea exit.
    _start_watchdog()

    return app


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Web server para fund-analyzer")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument(
        "--no-cold-start",
        action="store_true",
        help="NO mueve carpeta data/funds/{ISIN} a .bak antes de analizar",
    )
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    load_persisted_runs()

    app = make_app(cold_start=not args.no_cold_start)
    print("=" * 70)
    print(f"  fund-analyzer web server")
    print(f"  Root: {ROOT}")
    print(f"  Catalog:  http://{args.host}:{args.port}/dashboard/catalog.html")
    print(f"  API base: http://{args.host}:{args.port}/api/")
    print(f"  Cold start (move to .bak): {not args.no_cold_start}")
    print("=" * 70)
    app.run(host=args.host, port=args.port, threaded=True, debug=args.debug)


if __name__ == "__main__":
    main()
