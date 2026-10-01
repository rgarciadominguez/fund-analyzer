"""versiones.py — Historial de versiones publicadas de cada análisis (Rafa 1-oct-2026).

Rafa: "lo ideal sería la fecha del último análisis, mejora o actualización → así queda claro que es versión
nueva (aunque se guarden el resto de fechas históricas, que siempre viene bien para saber cuándo se
actualizó antes)".

Cada PUBLICACIÓN de un análisis (normal o tras validar un borrador) añade una entrada a
output.json["historial_versiones"] = [{fecha, tipo, etiqueta}] y deja output.json["ultima_version"]. El portal
recibe por sync-meta `fecha_ultimo_analisis` (= fecha de la última versión), `version_tipo` (etiqueta) y
`versiones` (historial) y los muestra en el catálogo y en la ficha. El dashboard lo enseña en su cabecera.

Tipos: full → "Análisis completo" · annual_update → "Actualización anual" · aporte → "Mejora con material
aportado" · mejora_feedback → "Mejora por feedback".
Backfill: `python -m tools.versiones --backfill` reconstruye el historial de todos los fondos desde los runs
terminados de data/queue_state.json (sin pisar lo ya registrado).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"
ETIQUETAS = {"full": "Análisis completo", "annual_update": "Actualización anual",
             "aporte": "Mejora con material aportado", "mejora_feedback": "Mejora por feedback"}


def _load(p: Path, dflt=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dflt


def tipo_actual(isin: str) -> str:
    cfg = _load(FUNDS / isin / "config.json", {}) or {}
    m = str(cfg.get("modo") or "full")
    return m if m in ETIQUETAS else "full"


def registrar(isin: str, tipo: str | None = None, fecha: str | None = None) -> dict:
    isin = isin.upper()
    p = FUNDS / isin / "output.json"
    o = _load(p)
    if not isinstance(o, dict):
        return {}
    tipo = tipo if tipo in ETIQUETAS else tipo_actual(isin)
    fecha = (fecha or datetime.now().isoformat(timespec="seconds"))[:19]
    v = {"fecha": fecha, "tipo": tipo, "etiqueta": ETIQUETAS[tipo]}
    hist = [x for x in (o.get("historial_versiones") or []) if isinstance(x, dict)]
    if not any(x.get("fecha", "")[:10] == fecha[:10] and x.get("tipo") == tipo for x in hist):
        hist.append(v)
    hist.sort(key=lambda x: x.get("fecha", ""))
    o["historial_versiones"] = hist
    o["ultima_version"] = hist[-1]
    o["ultima_actualizacion"] = hist[-1]["fecha"]
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(o, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return hist[-1]


def para_portal(isin: str) -> dict:
    o = _load(FUNDS / isin.upper() / "output.json", {}) or {}
    hist = [x for x in (o.get("historial_versiones") or []) if isinstance(x, dict)]
    if not hist:
        return {}
    u = hist[-1]
    return {"fecha_ultimo_analisis": u["fecha"].replace("T", " ")[:19], "version_tipo": u.get("etiqueta"),
            "versiones": [{"fecha": x["fecha"][:10], "tipo": x.get("etiqueta")} for x in reversed(hist)]}


def backfill() -> int:
    q = _load(ROOT / "data" / "queue_state.json", {}) or {}
    n = 0
    for it in q.get("items") or []:
        if it.get("status") not in ("done", "completed_with_warnings") or not it.get("finished_at"):
            continue
        isin = str(it.get("isin") or "").upper()
        p = FUNDS / isin / "output.json"
        o = _load(p)
        if not isinstance(o, dict):
            continue
        tipo = "mejora_feedback" if it.get("apply_feedback") else (it.get("scope") or "full")
        if tipo not in ETIQUETAS:
            tipo = "full"
        try:
            fecha = datetime.fromisoformat(str(it["finished_at"]).replace("Z", "+00:00")).astimezone().replace(tzinfo=None)
        except Exception:
            continue
        rev = _load(FUNDS / isin / "revision.json", {}) or {}
        if rev.get("estado") in ("pendiente_validacion", "no_publicado", "corrigiendo", "publicando") and                 str(rev.get("fecha") or "")[:10] >= fecha.isoformat()[:10]:
            continue                        # ese run acabó en borrador o sin publicar: no es versión publicada
        hist = [x for x in (o.get("historial_versiones") or []) if isinstance(x, dict)]
        if any(x.get("fecha", "")[:10] == fecha.isoformat()[:10] and x.get("tipo") == tipo for x in hist):
            continue
        hist.append({"fecha": fecha.isoformat(timespec="seconds"), "tipo": tipo, "etiqueta": ETIQUETAS[tipo]})
        hist.sort(key=lambda x: x.get("fecha", ""))
        o["historial_versiones"] = hist
        o["ultima_version"] = hist[-1]
        p.write_text(json.dumps(o, ensure_ascii=False, indent=2), encoding="utf-8")
        n += 1
    return n


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if "--backfill" in sys.argv:
        print(f"versiones añadidas: {backfill()}")
    elif len(sys.argv) >= 2:
        print(registrar(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None))
    else:
        print(__doc__)
