"""feedback_sistema.py — El feedback de Rafa sobre un análisis MEJORA EL SISTEMA (29-sep-2026).

Sustituye a "Mejorar este análisis" (que solo parcheaba ese fondo y solo funcionaba con el servidor local).
Rafa: "una vez publicado y revisado se pueda dar feedback sobre el fondo que ayude a mejorar el sistema;
la idea es únicamente ir mejorando el sistema mediante feedback".

Flujo:
  1. ENTRADA — Rafa lo dice en el Copiloto del portal (widget en todas las páginas, también en la del
     análisis; skill `feedback-fondo` del Copiloto) o en cualquier sesión de Claude Code:
         python -m tools.feedback_sistema add ES0140794001 "la cartera sale toda como RV y es un fondo de deuda"
     → data/feedback_sistema/pendientes/{id}.json
  2. PROCESO — en el servidor, solo con la cola de análisis parada: skill `feedback-sistema-cowork` con
     Fable 5.1 (effort high). Entiende el feedback, lo comprueba contra los datos del fondo, busca la CAUSA
     en el sistema (skills, reglas, código), la arregla de forma general (nunca un parche para ese fondo),
     pasa tests, sube el cambio a git y, si hace falta, relanza el análisis del fondo para aplicarlo.
     Deja su informe en data/feedback_sistema/hechos/{id}.json.
  3. AVISO — tarea en el portal con el resumen en llano de qué se cambió y por qué. Registro histórico en
     data/feedback_sistema/registro.jsonl (aprendizaje acumulado: qué feedback llevó a qué cambio).

Disparo: consume_inputs_rafa (pasada horaria del guardián) llama procesar_si_toca(): si hay pendientes, la
cola de análisis está parada y no hay otro proceso en marcha, lanza `--procesar` en segundo plano.
CLI:  add ISIN "texto" [--origen X] | lista | --procesar
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASE = ROOT / "data" / "feedback_sistema"
PEND = BASE / "pendientes"
HECHOS = BASE / "hechos"
REGISTRO = BASE / "registro.jsonl"
LOCK = BASE / ".procesando.lock"
MODEL = os.environ.get("MODEL_FEEDBACK", "claude-fable-5-1")
EFFORT = os.environ.get("EFFORT_FEEDBACK", "high")


def _log(m: str) -> None:
    print(f"[FEEDBACK] {m}", flush=True)


def _load(p: Path, dflt=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dflt


def add(isin: str, texto: str, origen: str = "manual") -> str:
    PEND.mkdir(parents=True, exist_ok=True)
    isin = isin.strip().upper()
    fid = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + isin
    out = _load(ROOT / "data" / "funds" / isin / "output.json", {}) or {}
    item = {"id": fid, "isin": isin, "nombre": out.get("nombre"), "texto": texto.strip(), "origen": origen,
            "creado": datetime.now().isoformat(timespec="seconds"), "estado": "pendiente"}
    (PEND / f"{fid}.json").write_text(json.dumps(item, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"guardado {fid}: se procesará solo en el servidor cuando la cola de análisis esté parada")
    return fid


def _cola_parada() -> bool:
    q = _load(ROOT / "data" / "queue_state.json", {}) or {}
    return not any(i.get("status") in ("running", "queued", "paused_waiting_tokens") for i in q.get("items") or [])


def _lock_vivo() -> bool:
    if not LOCK.exists():
        return False
    try:
        return time.time() - LOCK.stat().st_mtime < 4 * 3600     # un lock de >4 h es de un proceso muerto
    except Exception:
        return False


def _tarea_portal(titulo: str) -> None:
    try:
        from tools.seguimiento_fondos import _tarea
        _tarea({"titulo": titulo[:240], "fecha": date.today().isoformat()}, False)
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] aviso al portal no enviado: {str(e)[:100]}")


def procesar() -> int:
    BASE.mkdir(parents=True, exist_ok=True)
    HECHOS.mkdir(parents=True, exist_ok=True)
    if _lock_vivo():
        _log("ya hay un proceso de feedback en marcha")
        return 0
    LOCK.write_text(str(os.getpid()), encoding="utf-8")
    n = 0
    try:
        for p in sorted(PEND.glob("*.json")):
            if not _cola_parada():
                _log("la cola de análisis se ha puesto en marcha: sigo en la próxima pasada")
                break
            it = _load(p)
            if not isinstance(it, dict):
                continue
            fid = it["id"]
            LOCK.touch()
            logf = ROOT / "logs" / f"skill_feedback_{fid}.log"
            _log(f"procesando {fid} con {MODEL} ({EFFORT})")
            rc = subprocess.call([sys.executable, "-m", "tools.claude_cowork", str(logf),
                                  f"feedback sistema cowork {fid}", "--model", MODEL, "--effort", EFFORT,
                                  "--allowedTools", "Read,Write,Edit,Bash,Glob,Grep,WebFetch,WebSearch"],
                                 cwd=str(ROOT))
            rep = _load(HECHOS / f"{fid}.json")
            if not isinstance(rep, dict):
                if rc != 0 and "limit" in (logf.read_text(encoding="utf-8", errors="ignore")[-3000:].lower() if logf.exists() else ""):
                    _log("límite de uso de Claude: se reintenta en la próxima pasada")
                    break
                rep = {"id": fid, "resumen_rafa": f"No he podido procesar el feedback (rc={rc}); ver {logf.name}",
                       "cambios_sistema": [], "estado": "error"}
            rep.setdefault("id", fid)
            it.update({"estado": rep.get("estado") or "hecho", "procesado": datetime.now().isoformat(timespec="seconds")})
            with REGISTRO.open("a", encoding="utf-8") as f:
                f.write(json.dumps({**it, "informe": rep}, ensure_ascii=False) + "\n")
            p.unlink(missing_ok=True)
            nombre = it.get("nombre") or it["isin"]
            _tarea_portal(f"Feedback sobre {nombre} ({it['isin']}) procesado: {rep.get('resumen_rafa', '')}")
            n += 1
    finally:
        LOCK.unlink(missing_ok=True)
    _log(f"procesados: {n}")
    return 0


def procesar_si_toca() -> None:
    """Pasada horaria: si hay feedback pendiente y la cola de análisis está parada, lo procesa en segundo plano."""
    if not any(PEND.glob("*.json")) if PEND.exists() else True:
        return
    if _lock_vivo() or not _cola_parada():
        return
    flags = 0
    if os.name == "nt":
        flags = 0x00000008 | 0x00000200 | 0x08000000   # DETACHED | NEW_PROCESS_GROUP | NO_WINDOW
    exe = Path(sys.executable)
    pyw = exe.with_name("pythonw.exe")
    subprocess.Popen([str(pyw if pyw.exists() else exe), "-m", "tools.feedback_sistema", "--procesar"],
                     cwd=str(ROOT), creationflags=flags, close_fds=True,
                     stdout=open(ROOT / "logs" / "feedback_sistema.log", "a", encoding="utf-8"),
                     stderr=subprocess.STDOUT)
    _log("lanzado el proceso de feedback en segundo plano")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("accion", nargs="?", choices=["add", "lista"])
    ap.add_argument("isin", nargs="?")
    ap.add_argument("texto", nargs="?")
    ap.add_argument("--origen", default="manual")
    ap.add_argument("--procesar", action="store_true")
    a = ap.parse_args()
    if a.procesar:
        sys.exit(procesar())
    if a.accion == "add":
        if not a.isin or not a.texto:
            print('uso: python -m tools.feedback_sistema add ISIN "texto"'); sys.exit(2)
        print(add(a.isin, a.texto, a.origen)); sys.exit(0)
    if a.accion == "lista":
        for p in sorted(PEND.glob("*.json")) if PEND.exists() else []:
            it = _load(p, {}); print("PENDIENTE", it.get("id"), "·", (it.get("texto") or "")[:100])
        for line in (REGISTRO.read_text(encoding="utf-8").splitlines()[-10:] if REGISTRO.exists() else []):
            r = json.loads(line); print("HECHO", r.get("id"), "·", (r.get("informe") or {}).get("resumen_rafa", "")[:120])
        sys.exit(0)
    ap.print_help()
