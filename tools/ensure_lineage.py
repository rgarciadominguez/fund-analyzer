"""ensure_lineage.py — Reconoce fondos con posible PREDECESOR (estrategia anterior al vehículo legal
actual: AMC/RAIF/Cayman → UCITS, fondos renombrados) y arranca su resolución. Contrato §0.9:
track-record = serie NAV real más larga, NO la fecha de inicio legal.

Se cablea en la PREP del pipeline (INT). Barato: primero un gate por antigüedad/gap (sin red); solo
si aplica corre el detector determinista (Morningstar, 0 tokens LLM) y, si hace falta nombrar el
predecesor, ENCOLA para el resolver agéntico (skill lineage-resolver-cowork) — que corre 1 vez por
fondo y se cachea en data/fund_lineage.json (compartido por todas las clases del grupo).

Gate (decisión Rafa 2026-09-06): fondo INT y (antigüedad legal < 7 años  OR  gap de histórico claro).
7 años porque muchos relanzamientos UCITS recientes (5-7a) vienen de un vehículo previo.

ensure(isin, log) -> {status, ...}
  status: "resolved" (ya cacheado) | "flagged" (detectado+encolado) | "skip" (no aplica)
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUEUE = ROOT / "data" / "lineage_queue.json"
YOUNG_FUND_YEARS = 7


def _legal_age(isin: str) -> int:
    """Antigüedad legal en años (best-effort, sin red primero). 0 si desconocida."""
    import re
    p = ROOT / "data" / "funds" / isin / "output.json"
    yr = 0
    if p.exists():
        try:
            d = json.loads(p.read_text(encoding="utf-8"))
            k = d.get("kpis") or {}
            for v in (k.get("anio_creacion"), k.get("fecha_registro"), k.get("fecha_inicio"),
                      d.get("fecha_creacion")):
                m = re.search(r"(19|20)\d{2}", str(v or ""))
                if m:
                    yr = int(m.group(0)); break
        except Exception:
            pass
    if not yr:
        try:
            from dotenv import load_dotenv
            load_dotenv(ROOT / ".env")
            from tools.supabase_client import get_client
            c = get_client()
            f = c.table("funds").select("fund_group_id,fecha_creacion_clase").eq(
                "isin", isin.upper()).execute().data
            if f:
                m = re.search(r"(19|20)\d{2}", str(f[0].get("fecha_creacion_clase") or ""))
                if m:
                    yr = int(m.group(0))
                gid = f[0]["fund_group_id"]
                g = c.table("fund_groups").select("fecha_creacion_fondo").eq(
                    "fund_group_id", gid).execute().data
                if g:
                    m2 = re.search(r"(19|20)\d{2}", str(g[0].get("fecha_creacion_fondo") or ""))
                    if m2:
                        yr = min(yr or 9999, int(m2.group(0)))
        except Exception:
            pass
    return (date.today().year - yr) if yr else 0


def _enqueue(entry: dict, log) -> None:
    try:
        q = json.loads(QUEUE.read_text(encoding="utf-8")) if QUEUE.exists() else {"pending": []}
    except Exception:
        q = {"pending": []}
    q.setdefault("pending", [])
    if not any(e.get("isin") == entry["isin"] for e in q["pending"]):
        q["pending"].append(entry)
        QUEUE.write_text(json.dumps(q, ensure_ascii=False, indent=1), encoding="utf-8")
        log(f"[LINEAGE] encolado para el resolver agéntico: {entry['isin']}")


def ensure(isin: str, log=print) -> dict:
    isin = (isin or "").upper().strip()
    if isin.startswith("ES"):
        return {"status": "skip", "reason": "ES"}

    from tools import lineage_kb
    rec = lineage_kb.get_record(isin)
    # ya resuelto por el agente (tiene predecesores nombrados) → nada que investigar
    if rec and rec.get("predecessors") and str(rec.get("resolved_by", "")).find("subagent") >= 0 \
            or (rec and rec.get("predecessors") and rec.get("confidence")):
        return {"status": "resolved", "record": {"predecessors": len(rec.get("predecessors") or []),
                                                  "track_start": (rec.get("track_record") or {}).get("quant_series_start")}}

    age = _legal_age(isin)
    gap = {}
    try:
        from tools.historico_gap import detect as _gap
        gap = _gap(isin)
    except Exception:
        pass

    gated = (0 < age < YOUNG_FUND_YEARS) or bool(gap.get("sugerir_mejora"))
    if not gated:
        return {"status": "skip", "reason": f"fondo maduro ({age}a) y sin gap", "age": age}

    # detector determinista (Morningstar): serie real vs lanzamiento legal
    det = {}
    try:
        from tools.lineage_detect import detect as _det
        det = _det(isin)
    except Exception as e:
        log(f"[LINEAGE] detector falló: {str(e)[:80]}")

    if det.get("predecessor_signal") or (0 < age < YOUNG_FUND_YEARS):
        _enqueue({
            "isin": isin, "age": age,
            "predecessor_signal": det.get("predecessor_signal", False),
            "longest_series_isin": det.get("longest_series_isin"),
            "longest_series_start": det.get("longest_series_start"),
            "gap": gap.get("motivo", ""),
        }, log)
        motivo = det.get("motivo") or f"fondo joven ({age}a)"
        log(f"[LINEAGE] {isin}: posible predecesor → {motivo}")
        return {"status": "flagged", "detector": det, "age": age}
    return {"status": "skip", "reason": det.get("motivo", "sin señal"), "age": age}


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = argv or sys.argv[1:]
    if args and args[0] == "--queue":
        q = json.loads(QUEUE.read_text(encoding="utf-8")) if QUEUE.exists() else {"pending": []}
        print(json.dumps(q, ensure_ascii=False, indent=1))
        return
    if len(args) >= 2 and args[0] == "--is-queued":
        # imprime "1" si el ISIN está pendiente en la cola de lineage (para gate del bat), si no "0"
        isin = args[1].upper()
        try:
            q = json.loads(QUEUE.read_text(encoding="utf-8")) if QUEUE.exists() else {"pending": []}
            print("1" if any(e.get("isin") == isin for e in q.get("pending", [])) else "0")
        except Exception:
            print("0")
        return
    for isin in args:
        print(json.dumps(ensure(isin), ensure_ascii=False))


if __name__ == "__main__":
    main()
