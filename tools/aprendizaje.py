"""aprendizaje.py — Agente de aprendizaje del sistema de análisis de fondos (Rafa 30-sep-2026).

Rafa: "un sistema de agentes especializado que vaya aprendiendo a medida que vamos haciendo análisis";
"no quiero que arregle hacia atrás sino el fondo en cuestión + aprenda casuísticas que puedan aplicar a
futuros análisis".

Qué es:
  · Una BASE DE LECCIONES (data/aprendizaje/lecciones.json): casuísticas con CUÁNDO aplican (domicilio,
    tipo de activo, gestora, estructura…) y en qué ETAPA del análisis (fuentes, cartas, gestores, cartera,
    cuantitativo, síntesis). Ejemplo: "en paraguas irlandeses, las 'cartas' que traen distribuidores o
    plataformas suelen ser de otro fondo: verifica el nombre del fondo dentro del documento".
  · Un AGENTE CURADOR (skill aprendizaje-cowork, Fable 5.1) que procesa cada entrada de feedback: decide si
    hay que corregir ESE fondo (solo ese, solo las secciones afectadas) y destila lecciones nuevas o refina
    las existentes. No cambia código ni rehace análisis antiguos; si ve un fallo de programación, lo deja
    como propuesta para Rafa.
  · Cada análisis nuevo CONSULTA las lecciones que le aplican: las skills del pipeline ejecutan
    `python -m tools.aprendizaje lecciones ISIN [--etapa X]` al empezar.

Entradas (data/aprendizaje/entradas/):
  · 'validacion' — lo que Rafa marca al validar las dudas de un borrador (tools.revision.publicar).
  · 'comentario' — un feedback libre sobre un análisis (Copiloto, skill feedback-fondo, o CLI `add`).
Se procesan en el servidor desde la pasada horaria de consume_inputs_rafa, solo con la cola parada.

CLI:  add ISIN "texto"  ·  lecciones ISIN [--etapa X]  ·  lista  ·  --procesar
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
BASE = ROOT / "data" / "aprendizaje"
ENTRADAS = BASE / "entradas"
HECHOS = BASE / "hechos"
LECCIONES = BASE / "lecciones.json"
REGISTRO = BASE / "registro.jsonl"
LOCK = BASE / ".procesando.lock"
MODEL = os.environ.get("MODEL_APRENDIZAJE", "claude-fable-5-1")
EFFORT = os.environ.get("EFFORT_APRENDIZAJE", "high")
ETAPAS = ("fuentes", "cartas", "gestores", "cartera", "cuantitativo", "sintesis")


def _log(m: str) -> None:
    print(f"[APRENDIZAJE] {m}", flush=True)


def _load(p: Path, dflt=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dflt


# ── entradas ────────────────────────────────────────────────────────────────────────────────────
def add(isin: str, tipo: str, datos) -> str:
    """tipo: 'validacion' ({nombre, items:[{duda, detalle, veredicto, comentario}]}) o 'comentario' (texto)."""
    ENTRADAS.mkdir(parents=True, exist_ok=True)
    isin = isin.strip().upper()
    eid = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + isin
    out = _load(ROOT / "data" / "funds" / isin / "output.json", {}) or {}
    e = {"id": eid, "isin": isin, "nombre": out.get("nombre"), "tipo": tipo,
         "datos": datos if tipo != "comentario" else {"texto": str(datos).strip()},
         "creado": datetime.now().isoformat(timespec="seconds")}
    (ENTRADAS / f"{eid}.json").write_text(json.dumps(e, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"entrada {eid} ({tipo}) guardada: se procesa sola en el servidor cuando no haya análisis en marcha")
    return eid


# ── lecciones ───────────────────────────────────────────────────────────────────────────────────
def _atributos(isin: str) -> dict:
    out = _load(ROOT / "data" / "funds" / isin / "output.json", {}) or {}
    k = out.get("kpis") or {}
    mix = ((out.get("cuantitativo") or {}).get("mix_activos_historico") or [{}])[-1] or {}
    tipo_activo = []
    try:
        if float(mix.get("renta_fija_pct") or 0) >= 55:
            tipo_activo.append("RF")
        if float(mix.get("renta_variable_pct") or 0) >= 55:
            tipo_activo.append("RV")
        if not tipo_activo:
            tipo_activo.append("Mixto")
    except Exception:
        pass
    return {"domicilio": isin[:2], "tipo": out.get("tipo") or ("ES" if isin.startswith("ES") else "INT"),
            "gestora": out.get("gestora") or "", "nombre": out.get("nombre") or "",
            "clasificacion": k.get("clasificacion") or "", "tipo_activo": tipo_activo}


def _aplica(lec: dict, at: dict) -> bool:
    c = lec.get("cuando") or {}
    for campo, vals in c.items():
        if not vals:
            continue
        vals = [str(v).lower() for v in (vals if isinstance(vals, list) else [vals])]
        mio = at.get(campo)
        mio = [str(x).lower() for x in (mio if isinstance(mio, list) else [mio or ""])]
        if not any(v in m or m == v for v in vals for m in mio):
            return False
    return True


def lecciones(isin: str, etapa: str | None = None) -> list[dict]:
    at = _atributos(isin.upper())
    base = (_load(LECCIONES, {}) or {}).get("lecciones") or []
    return [l for l in base if l.get("activa", True) and _aplica(l, at)
            and (not etapa or not l.get("etapas") or etapa in l.get("etapas"))]


# ── proceso ─────────────────────────────────────────────────────────────────────────────────────
def _cola_parada() -> bool:
    q = _load(ROOT / "data" / "queue_state.json", {}) or {}
    return not any(i.get("status") in ("running", "queued", "paused_waiting_tokens") for i in q.get("items") or [])


def _lock_vivo() -> bool:
    try:
        return LOCK.exists() and time.time() - LOCK.stat().st_mtime < 4 * 3600
    except Exception:
        return False


def procesar() -> int:
    HECHOS.mkdir(parents=True, exist_ok=True)
    if _lock_vivo():
        return 0
    LOCK.write_text(str(os.getpid()), encoding="utf-8")
    n = 0
    try:
        for p in sorted(ENTRADAS.glob("*.json")) if ENTRADAS.exists() else []:
            if not _cola_parada():
                _log("hay análisis en marcha: sigo en la próxima pasada")
                break
            e = _load(p)
            if not isinstance(e, dict):
                continue
            LOCK.touch()
            logf = ROOT / "logs" / f"skill_aprendizaje_{e['id']}.log"
            rc = subprocess.call([sys.executable, "-m", "tools.claude_cowork", str(logf),
                                  f"aprendizaje cowork {e['id']}", "--model", MODEL, "--effort", EFFORT,
                                  "--allowedTools", "Read,Write,Edit,Bash,Glob,Grep,WebFetch,WebSearch"], cwd=str(ROOT))
            rep = _load(HECHOS / f"{e['id']}.json")
            if not isinstance(rep, dict):
                tail = logf.read_text(encoding="utf-8", errors="ignore")[-3000:].lower() if logf.exists() else ""
                if "limit" in tail:
                    _log("límite de uso de Claude: se reintenta en la próxima pasada")
                    break
                rep = {"resumen_rafa": f"No se pudo procesar (rc={rc}); ver {logf.name}", "estado": "error"}
            with REGISTRO.open("a", encoding="utf-8") as f:
                f.write(json.dumps({**e, "informe": rep, "procesado": datetime.now().isoformat(timespec="seconds")},
                                   ensure_ascii=False) + "\n")
            p.unlink(missing_ok=True)
            n += 1
    finally:
        LOCK.unlink(missing_ok=True)
    _log(f"procesadas: {n}")
    return 0


def procesar_si_toca() -> None:
    if not ENTRADAS.exists() or not any(ENTRADAS.glob("*.json")) or _lock_vivo() or not _cola_parada():
        return
    flags = (0x00000008 | 0x00000200 | 0x08000000) if os.name == "nt" else 0
    exe = Path(sys.executable)
    pyw = exe.with_name("pythonw.exe")
    (ROOT / "logs").mkdir(exist_ok=True)
    subprocess.Popen([str(pyw if pyw.exists() else exe), "-m", "tools.aprendizaje", "--procesar"],
                     cwd=str(ROOT), creationflags=flags, close_fds=True,
                     stdout=open(ROOT / "logs" / "aprendizaje.log", "a", encoding="utf-8"), stderr=subprocess.STDOUT)
    _log("lanzado el agente de aprendizaje en segundo plano")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("accion", nargs="?", choices=["add", "lecciones", "lista"])
    ap.add_argument("isin", nargs="?")
    ap.add_argument("texto", nargs="?")
    ap.add_argument("--etapa", choices=ETAPAS)
    ap.add_argument("--origen", default="manual")
    ap.add_argument("--procesar", action="store_true")
    a = ap.parse_args()
    if a.procesar:
        sys.exit(procesar())
    if a.accion == "add" and a.isin and a.texto:
        print(add(a.isin, "comentario", a.texto)); sys.exit(0)
    if a.accion == "lecciones" and a.isin:
        ls = lecciones(a.isin, a.etapa)
        if not ls:
            print("Sin lecciones aplicables."); sys.exit(0)
        print(f"LECCIONES APRENDIDAS QUE APLICAN A {a.isin.upper()}" + (f" (etapa {a.etapa})" if a.etapa else "") + ":")
        for l in ls:
            print(f"- [{l.get('id')}] {l.get('texto')}" + (f"  (por qué: {l.get('por_que')})" if l.get("por_que") else ""))
        sys.exit(0)
    if a.accion == "lista":
        for p in sorted(ENTRADAS.glob("*.json")) if ENTRADAS.exists() else []:
            e = _load(p, {}); print("PENDIENTE", e.get("id"), e.get("tipo"))
        for line in (REGISTRO.read_text(encoding="utf-8").splitlines()[-10:] if REGISTRO.exists() else []):
            r = json.loads(line); print("HECHA", r.get("id"), "·", (r.get("informe") or {}).get("resumen_rafa", "")[:120])
        print(f"{len((_load(LECCIONES, {}) or {}).get('lecciones') or [])} lecciones en la base")
        sys.exit(0)
    ap.print_help()
