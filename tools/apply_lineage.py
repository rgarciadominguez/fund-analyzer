"""apply_lineage.py — Vuelca el lineage/predecesor resuelto (data/fund_lineage.json) al análisis del
fondo para tener histórico COMPLETO (§0.9, decisión Rafa "todo con etiqueta 100%"):

  1. output.json["_lineage"]  → bloque para el analyst (narrativa historia/estrategia/consistencia),
     el chat y el dashboard (aviso de vehículo predecesor).
  2. output.json["analisis_cuantitativo"]["rendimiento_diario"] = compute_metrics(isin) — que ya es
     lineage-aware (usa la serie NAV real más larga, con `_lineage`). Extiende CAGR/vol/maxDD/anuales
     al histórico del predecesor (MontLake: desde 2021, no 2024).
  3. Encola el sourcing de AR del predecesor (si tiene vehículo con cuentas propias) para extender el
     histórico de CARTERA — vía tools.ar_sourcing_queue.

Idempotente. No inventa: solo aplica lo que el resolver dejó en el KB (con sus caveats).

CLI: python -m tools.apply_lineage IE000Z9YV312
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def apply(isin: str, log=print) -> dict:
    isin = (isin or "").upper().strip()
    from tools import lineage_kb
    rec = lineage_kb.get_record(isin)
    if not rec or not rec.get("predecessors"):
        return {"applied": False, "reason": "sin lineage resuelto"}

    p = ROOT / "data" / "funds" / isin / "output.json"
    if not p.exists():
        return {"applied": False, "reason": "sin output.json"}
    d = json.loads(p.read_text(encoding="utf-8"))

    # 1) bloque _lineage (compacto) para analyst/chat/dashboard
    d["_lineage"] = {
        "strategy_name": rec.get("strategy_name"),
        "manager": rec.get("manager"),
        "lead_pm": rec.get("lead_pm"),
        "strategy_inception": rec.get("strategy_inception"),
        "current_vehicle": rec.get("current_vehicle"),
        "predecessors": rec.get("predecessors"),
        "track_record": rec.get("track_record"),
        "sources": rec.get("sources"),
        "confidence": rec.get("confidence"),
        "caveat_global": rec.get("caveat_global"),
    }

    # 2) quant extendido (compute_metrics ya es lineage-aware) + SecId de la serie larga para que
    #    los gráficos de evolución del dashboard (client-side por SecId) usen el track completo.
    quant_start = None
    try:
        from tools.morningstar_daily import compute_metrics, resolve_secid
        md = compute_metrics(isin)
        if md and md.get("rentabilidades_anuales"):
            d.setdefault("analisis_cuantitativo", {})["rendimiento_diario"] = md
            quant_start = (md.get("_lineage") or {}).get("desde")
        qsi = (rec.get("track_record") or {}).get("quant_series_isin")
        if qsi:
            sid = resolve_secid(qsi)
            if sid:
                d["_lineage"].setdefault("track_record", {})["quant_series_secid"] = sid
    except Exception as e:
        log(f"[LINEAGE] quant no aplicado: {str(e)[:80]}")

    p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

    # 2b) Propagar _lineage al fichero FUENTE (intl_data.json / cnmv_data.json) porque el bundle que
    # lee el analyst-cowork es una COPIA de ese fichero (no de output.json) y se exporta ANTES del
    # consume. Así el analyst ve el lineage y narra la historia/estrategia/consistencia con él.
    for src_name in ("intl_data.json", "cnmv_data.json"):
        sp = ROOT / "data" / "funds" / isin / src_name
        if sp.exists():
            try:
                sd = json.loads(sp.read_text(encoding="utf-8"))
                sd["_lineage"] = d["_lineage"]
                if d.get("analisis_cuantitativo", {}).get("rendimiento_diario"):
                    sd.setdefault("analisis_cuantitativo", {})["rendimiento_diario"] = \
                        d["analisis_cuantitativo"]["rendimiento_diario"]
                sp.write_text(json.dumps(sd, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass

    # 3) encolar sourcing de AR del predecesor (histórico de cartera), si publica cuentas
    enq = 0
    try:
        from tools.ar_sourcing_queue import enqueue
        for pre in rec.get("predecessors") or []:
            pi = pre.get("isin")
            # AMC (XS…) no publica AR; RAIF/SICAV (LU…) sí puede → encolar para su AR
            if pi and pre.get("type", "").upper() in ("RAIF", "SICAV", "RAIF/SICAV", "RENAMED"):
                enqueue(pi, fund_name=pre.get("name", ""), gestora=rec.get("manager", ""), ar_count=0)
                enq += 1
    except Exception:
        pass

    log(f"[LINEAGE] aplicado a {isin}: quant desde {quant_start or '?'}, "
        f"{len(rec.get('predecessors') or [])} predecesor(es), {enq} AR-predecesor encolados")
    return {"applied": True, "quant_start": quant_start,
            "predecessors": len(rec.get("predecessors") or []), "ar_enqueued": enq}


def main(argv=None):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    for isin in (argv or sys.argv[1:]) or []:
        print(json.dumps(apply(isin.upper()), ensure_ascii=False))


if __name__ == "__main__":
    main()
