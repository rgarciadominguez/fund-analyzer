"""seguir_run.py — avisa en el CHAT que sigue un análisis cuando su run cambia de estado.

Rafa 29-sep-2026: "en cada chat concreto se envíe un mensaje de que se ha vuelto al curro" (el panel técnico
del portal no aporta). El chat que lanza o sigue un fondo deja esto corriendo en segundo plano; el script
SALE con una línea en cuanto hay un evento, y esa salida despierta al chat, que se lo cuenta a Rafa y lo
vuelve a lanzar si el run sigue vivo.

Eventos (lee data/queue_state.json, que OneDrive trae del servidor):
  PAUSA      running/queued -> paused_waiting_tokens   (con la hora prevista de reanudación)
  REANUDADO  paused_waiting_tokens -> queued/running    ("vuelta al trabajo")
  FIN        -> completed / completed_with_warnings / failed / skipped / cancelled (con exit code)
  PERDIDO    el item desaparece de la cola

Uso:  python -m tools.seguir_run ES0140794001 [--poll 60] [--max-horas 24]
Sale 0 con una línea "[EVENTO] ..." ; sale 2 si pasa --max-horas sin eventos (volver a lanzar).
"""
from __future__ import annotations

import argparse
import os
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QS = Path(os.environ.get("SEGUIR_RUN_QS") or ROOT / "data" / "queue_state.json")
VIVOS = {"running", "queued"}
PAUSA = "paused_waiting_tokens"


def _item(isin: str) -> dict | None:
    try:
        d = json.loads(QS.read_text(encoding="utf-8"))
    except Exception:
        return {}  # lectura a medias (OneDrive escribiendo): se reintenta
    its = [i for i in d.get("items", []) if i.get("isin") == isin]
    if not its:
        return None
    its.sort(key=lambda i: str(i.get("queued_at") or ""))
    return its[-1]


def _hora(iso: str | None) -> str:
    if not iso:
        return "?"
    try:
        dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone().strftime("%d-%m %H:%M")
    except Exception:
        return str(iso)[:16]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("isin")
    ap.add_argument("--poll", type=int, default=60)
    ap.add_argument("--max-horas", type=float, default=24)
    a = ap.parse_args()
    isin = a.isin.strip().upper()
    fin = time.time() + a.max_horas * 3600

    it = _item(isin)
    while it == {}:
        time.sleep(5); it = _item(isin)
    if it is None:
        print(f"[PERDIDO] {isin}: no está en la cola del servidor.", flush=True)
        return 0
    prev = it.get("status")
    run = it.get("run_id")
    if prev not in VIVOS and prev != PAUSA:
        print(f"[FIN] {isin}: ya terminado ({prev}, exit {it.get('exit_code')}) a las {_hora(it.get('finished_at'))}.", flush=True)
        return 0

    while time.time() < fin:
        time.sleep(a.poll)
        it = _item(isin)
        if it == {}:
            continue
        if it is None:
            print(f"[PERDIDO] {isin}: el run {run} ha desaparecido de la cola (último estado: {prev}).", flush=True)
            return 0
        st = it.get("status")
        if st == prev:
            continue
        if st == PAUSA:
            try:
                hasta = json.loads(QS.read_text(encoding="utf-8")).get("tokens_blocked_until")
            except Exception:
                hasta = None
            cuando = f"hacia las {_hora(hasta)} (+5 min)" if hasta else "cuando vuelvan los tokens"
            print(f"[PAUSA] {isin}: en pausa por límite de tokens de Claude; se reanuda solo {cuando} "
                  f"(run {it.get('run_id')}).", flush=True)
            return 0
        if prev == PAUSA and st in VIVOS:
            print(f"[REANUDADO] {isin}: vuelta al trabajo a las {_hora(it.get('_resumed_at') or it.get('started_at'))}; "
                  f"retoma donde se quedó (run {it.get('run_id')}).", flush=True)
            return 0
        if st not in VIVOS and st != PAUSA:
            print(f"[FIN] {isin}: {st}, exit {it.get('exit_code')}, a las {_hora(it.get('finished_at'))} "
                  f"(run {it.get('run_id')}).", flush=True)
            return 0
        prev = st  # queued -> running: no es un evento para Rafa
    print(f"[SIN-EVENTOS] {isin}: {a.max_horas} h sin cambios (estado {prev}); volver a lanzar el seguimiento.", flush=True)
    return 2


if __name__ == "__main__":
    sys.exit(main())
