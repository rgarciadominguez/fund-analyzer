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
    r = RUNS.get(run_id)
    if not r:
        return
    proc = r.get("_proc")
    if not proc:
        return
    try:
        proc.wait()
    except Exception as e:
        print(f"[WATCH {run_id}] error: {e}")
    finally:
        log_file = r.get("_log_file")
        if log_file:
            try:
                log_file.close()
            except Exception:
                pass
        r["status"] = "done" if proc.returncode == 0 else "failed"
        r["exit_code"] = proc.returncode
        r["end_time"] = datetime.now(timezone.utc).isoformat()
        # Duración en segundos
        try:
            start = datetime.fromisoformat(r["start_time"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(r["end_time"])
            r["duration_seconds"] = int((end - start).total_seconds())
        except Exception:
            r["duration_seconds"] = None
        persist_run(run_id, r)
        print(
            f"[WATCH {run_id}] terminado status={r['status']} "
            f"exit={proc.returncode} duración={r.get('duration_seconds')}s"
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

    @app.route("/api/analyze", methods=["POST"])
    def api_analyze():
        """Lanza analizar_fondo.bat para un ISIN. Devuelve run_id."""
        data = request.get_json(silent=True) or {}
        isin = (data.get("isin") or "").strip().upper()
        if not ISIN_REGEX.match(isin):
            return jsonify({"error": f"ISIN inválido: '{isin}'"}), 400

        # Evitar relanzar si ya hay run activo para este ISIN
        for rid, r in RUNS.items():
            if r.get("isin") == isin and r.get("status") == "running":
                return (
                    jsonify(
                        {
                            "error": f"ya hay un run activo: {rid}",
                            "run_id": rid,
                            "status": "already_running",
                        }
                    ),
                    409,
                )

        # Cold-start: mover carpeta del fondo a .bak si existe
        force_cold = data.get("cold_start", app.config["FUND_COLD_START"])
        fund_dir = DATA_DIR / "funds" / isin
        if fund_dir.exists() and force_cold:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup = DATA_DIR / "funds" / f"{isin}.bak_pre_web_{ts}"
            try:
                fund_dir.rename(backup)
                print(f"[ANALYZE {isin}] backup: {backup.name}")
            except Exception as e:
                return jsonify({"error": f"no se pudo mover backup: {e}"}), 500

        # Bat
        bat_path = ROOT / "analizar_fondo.bat"
        if not bat_path.exists():
            return jsonify({"error": "analizar_fondo.bat no existe"}), 500

        # Run id + log
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        run_id = f"{isin}_{ts}"
        LOGS_DIR.mkdir(exist_ok=True)
        log_path = LOGS_DIR / f"run_{run_id}.log"

        # Lanzar bat en background
        try:
            log_file = log_path.open("w", encoding="utf-8")
            # CREATE_NEW_PROCESS_GROUP en Windows permite que el subprocess
            # NO reciba señales del parent. CREATE_NO_WINDOW oculta consola.
            creationflags = 0
            if sys.platform == "win32":
                creationflags = (
                    subprocess.CREATE_NEW_PROCESS_GROUP
                    | subprocess.CREATE_NO_WINDOW
                )
            proc = subprocess.Popen(
                [str(bat_path), isin],
                cwd=str(ROOT),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                creationflags=creationflags,
                shell=False,
            )
        except Exception as e:
            return jsonify({"error": f"no se pudo lanzar bat: {e}"}), 500

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
        return jsonify(
            {
                "run_id": run_id,
                "isin": isin,
                "status": "running",
                "pid": proc.pid,
                "log_relative": run["log_relative"],
                "start_time": run["start_time"],
            }
        )

    @app.route("/api/runs")
    def api_runs():
        """Lista runs (activos + recientes)."""
        runs = []
        for r in RUNS.values():
            runs.append({k: v for k, v in r.items() if not k.startswith("_")})
        runs.sort(key=lambda x: x.get("start_time", ""), reverse=True)
        # Limitar a últimos 50
        return jsonify(runs[:50])

    @app.route("/api/runs/<run_id>")
    def api_run_detail(run_id: str):
        """Detalle del run + tail del log + checkpoints de progreso."""
        r = RUNS.get(run_id)
        if not r:
            return jsonify({"error": "run not found"}), 404

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
                    tail_lines = lines[-60:]
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

        # Si terminó, incluir referencia al dashboard generado
        if r.get("status") in ("done", "failed") and dashboard_html.exists():
            safe["dashboard_url"] = f"/dashboard/fund-{isin}.html"

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
