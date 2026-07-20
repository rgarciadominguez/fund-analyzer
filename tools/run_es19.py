"""
run_es19.py — Analiza en lote los 19 ES nuevos de Horizonte (luz verde 2026-07-19).

Por cada ISIN: orchestrator --auto (CNMV → analyst) → sync_to_supabase (crea fila + cablea
el enrichment del contrato: benchmark, comisiones, estrellas, textos) → marca estado=pendiente
(no se auto-aprueban; Rafa los cierra tras revisar).

Tolerante a fallos: un fondo que peta no tumba el lote; se registra y se sigue.
Reanudable: salta los que ya tengan output.json + fila en Supabase.

CLI:
    python -m tools.run_es19 [--only ISIN] [--from N]
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
STATE = ROOT / "data" / ".es19_progress.json"


def _load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text(encoding="utf-8"))
    return {}


def _save_state(s: dict) -> None:
    STATE.write_text(json.dumps(s, ensure_ascii=False, indent=1), encoding="utf-8")


def _run(cmd: list[str], timeout: int) -> tuple[int, str]:
    # Análisis COMPLETO (no lean): perfiles de gestores, cartas, lecturas, síntesis rica.
    # Los ES tienen la mejor fuente (CNMV) y merecen el análisis entero.
    try:
        p = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True,
                           timeout=timeout, encoding="utf-8", errors="replace")
        return p.returncode, (p.stdout or "")[-1500:] + (p.stderr or "")[-800:]
    except subprocess.TimeoutExpired:
        return 124, "TIMEOUT"
    except Exception as e:
        return 1, f"{type(e).__name__}: {e}"


def _output_completo(outp) -> bool:
    """True si output.json tiene análisis REAL: ≥2 secciones con texto narrativo (>50c).
    Un output hueco (las llamadas LLM narrativas fallaron → solo secciones estructurales,
    0 texto) NO cuenta como completo aunque el analyst 'corriera' → hay que reanalizar.
    Alinea con la validación del sync (que exige ≥2 secciones con contenido real)."""
    try:
        d = json.loads(outp.read_text(encoding="utf-8"))
    except Exception:
        return False
    a = d.get("analyst_synthesis") or {}
    reales = sum(1 for v in a.values()
                 if isinstance(v, dict) and len(str(v.get("texto") or "").strip()) > 50)
    return reales >= 2


def analyze_one(isin: str) -> dict:
    out = {"isin": isin, "steps": {}}
    outp = ROOT / "data" / "funds" / isin / "output.json"
    log = ""
    # 1) pipeline CNMV + analyst COMPLETO. Salta SOLO si el output ya está completo
    #    (un output fino de un run cortado se reanaliza). Timeout amplio (60 min).
    if outp.exists() and _output_completo(outp):
        out["steps"]["orchestrator"] = "skip(completo)"
    else:
        if outp.exists():                       # output fino de run cortado → fuera
            try:
                outp.unlink()
            except Exception:
                pass
        rc, log = _run([sys.executable, "-m", "agents.orchestrator", "--isin", isin, "--auto"], 3600)
        out["steps"]["orchestrator"] = rc
    out["output_json"] = outp.exists() and _output_completo(outp)
    if not out["output_json"]:
        out["error"] = "sin output.json completo"
        out["log"] = log[-600:]
        return out
    # 2) sync a Supabase (crea fila + enrichment del contrato)
    rc, log = _run([sys.executable, "-m", "tools.sync_to_supabase", isin], 600)
    out["steps"]["sync"] = rc
    # 3) estado pendiente (no se auto-aprueba)
    try:
        from tools.fund_estado import set_estado
        set_estado(isin, "pendiente")
        out["estado"] = "pendiente"
    except Exception as e:
        out["estado_error"] = str(e)[:100]
    return out


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--only")
    ap.add_argument("--from", dest="frm", type=int, default=0)
    a = ap.parse_args()

    es = json.loads((ROOT / "data" / ".es19.json").read_text(encoding="utf-8"))
    if a.only:
        es = [a.only]
    else:
        es = es[a.frm:]

    state = _load_state()
    for i, isin in enumerate(es, 1):
        if state.get(isin, {}).get("ok"):
            print(f"[{i}/{len(es)}] {isin} ya hecho, salto")
            continue
        print(f"[{i}/{len(es)}] {isin} analizando...", flush=True)
        t0 = time.monotonic()
        r = analyze_one(isin)
        r["secs"] = round(time.monotonic() - t0)
        r["ok"] = r.get("output_json") and r["steps"].get("sync") == 0
        state[isin] = r
        _save_state(state)
        print(f"    -> ok={r['ok']} | orch={r['steps'].get('orchestrator')} "
              f"sync={r['steps'].get('sync')} | {r['secs']}s "
              f"{'| ' + r.get('error','') if r.get('error') else ''}", flush=True)

    ok = sum(1 for v in state.values() if v.get("ok"))
    print(f"\nRESUMEN: {ok}/{len(state)} OK")
    for isin, v in state.items():
        if not v.get("ok"):
            print(f"  FALLO {isin}: {v.get('error','sync/orch rc!=0')}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    main()
