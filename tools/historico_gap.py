"""historico_gap.py — Detecta si un fondo tiene un GAP CLARO de histórico: pocos años de
cartera analizada frente a los años que hay disponibles (AR en la KB o antigüedad del fondo).

Uso: el portal, antes/al pulsar "Actualizar análisis", consulta esto. Si `sugerir_mejora` es
True, en vez de asumir el delta anual puede PREGUNTAR "¿mejorar el histórico?" (re-análisis full
multi-año). Si es False, el flujo normal de "Actualizar" = delta del último año.

Criterio de gap CLARO (conservador, para no molestar sin motivo):
  - Es INT (los ES traen cartera completa de CNMV; no aplica el mismo histórico multi-AR).
  - Tiene <=1 año de `posiciones.historicas`, Y
  - Hay >=3 años disponibles: en la KB (`known_annual_reports.json`) o por antigüedad del fondo.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _load(p: Path) -> dict:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _kb_years(isin: str) -> list[str]:
    """Años de AR disponibles en la KB para el paraguas del fondo (si está)."""
    try:
        from tools.ensure_kb_ar import _kb_entry_for
        e = _kb_entry_for(isin)
    except Exception:
        e = None
    if not isinstance(e, dict):
        return []
    ys = [str(r.get("year")) for r in (e.get("reports") or [])]
    return sorted({y for y in ys if re.match(r"^\d{4}$", y)})


def detect(isin: str) -> dict:
    """Devuelve {isin, tipo, anios_historico, anios_kb, anios_disponibles, sugerir_mejora, motivo}.
    Best-effort; nunca lanza."""
    isin = (isin or "").upper().strip()
    out = {"isin": isin, "anios_historico": 0, "anios_kb": 0,
           "anios_disponibles": 0, "sugerir_mejora": False, "motivo": ""}
    p = ROOT / "data" / "funds" / isin / "output.json"
    if not p.exists():
        out["motivo"] = "sin output.json"
        return out
    d = _load(p)
    es = isin.startswith("ES")
    out["tipo"] = "ES" if es else "INT"
    hist = (d.get("posiciones") or {}).get("historicas") or []
    anios_h = sorted({str(e.get("periodo"))[:4] for e in hist
                      if re.match(r"^\d{4}", str(e.get("periodo")))})
    out["anios_historico"] = len(anios_h)

    kb = _kb_years(isin)
    out["anios_kb"] = len(kb)

    # Antigüedad como proxy de "años potencialmente disponibles" (sourcing podría hallarlos).
    # Se toma el primer campo de fecha/año disponible: anio_creacion, años_antiguedad, o el
    # año de fecha_registro/fecha_inicio.
    from datetime import date
    kpis = d.get("kpis") or {}
    edad = 0
    try:
        anio = None
        if kpis.get("anio_creacion"):
            anio = int(str(kpis["anio_creacion"])[:4])
        elif kpis.get("anios_antiguedad"):
            edad = int(kpis["anios_antiguedad"])
        else:
            for k in ("fecha_registro", "fecha_inicio", "fecha_creacion"):
                v = str(kpis.get(k) or d.get(k) or "")
                m = re.search(r"(19|20)\d{2}", v)
                if m:
                    anio = int(m.group(0)); break
        if anio:
            edad = date.today().year - anio
    except Exception:
        edad = 0
    out["anios_disponibles"] = max(len(kb), min(max(edad, 0), 10))

    if es:
        out["motivo"] = "ES (cartera de CNMV, no aplica histórico multi-AR)"
        return out

    if out["anios_historico"] <= 1 and out["anios_disponibles"] >= 3:
        out["sugerir_mejora"] = True
        fuente = f"{len(kb)} años en KB" if len(kb) >= 3 else f"fondo de ~{edad} años"
        out["motivo"] = (f"solo {out['anios_historico']} año(s) de histórico y hay "
                         f"~{out['anios_disponibles']} disponibles ({fuente}) → conviene mejorar")
    else:
        out["motivo"] = (f"{out['anios_historico']} años de histórico "
                         f"(disponibles ~{out['anios_disponibles']}) — sin gap claro")
    return out


def main(argv=None) -> int:
    import argparse
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser(description="Detecta gap de histórico de un fondo")
    ap.add_argument("isin")
    a = ap.parse_args(argv)
    print(json.dumps(detect(a.isin), ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
