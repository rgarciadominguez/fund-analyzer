"""quality_gate.py — Freno de calidad ANTES de publicar un análisis (Rafa 29-sep-2026).

Rafa: "errores graves impidan publicar hasta corregirlos, o si tiene dudas lo anota en una pestaña para que
quede claro al revisarlo".

  · GRAVES (bloquean la publicación: ni Supabase ni dashboard público): los de error seguro que ya usaba el
    guard de Supabase — identidad (nombre/gestora inválidos, deriva de identidad), texto de prueba, análisis
    vacío o con contenido fabricado, patrimonio imposible. El análisis anterior sigue publicado; se crea una
    tarea en el portal para revisarlo y relanzarlo.
  · DUDAS (no bloquean): se anotan en `revision_pendiente` → pestaña "Novedades" del dashboard, bloque "a
    reconciliar", con fuente "auditoría de calidad". Vienen de: reglas de contenido/datos de la auditoría
    del dashboard (cifras que no cuadran con los datos, etc.), avisos del análisis (secciones flojas) y las
    dudas que el propio analista declara (anti-invención, supuestos).
El formato (nº de subtítulos, etc.) NO es duda: Rafa quiere síntesis ejecutiva sin topes ni reglas.

CLI:  python -m tools.quality_gate ISIN        → exit 0 publicable · exit 3 bloqueado (graves)
Escribe data/funds/{ISIN}/quality_gate.json y regenera el dashboard si cambian las dudas.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUENTE = "auditoría de calidad"
TIPOS_DUDA = {"content", "datos_incorrectos", "completitud", "incompletitud"}
REGLAS_DUDA_EXTRA = {"aum_jump_alert"}          # salto de patrimonio: confirmar (puede ser real o de unidades)


def _log(m: str) -> None:
    print(f"[QUALITY-GATE] {m}", flush=True)


def _load(p: Path, dflt=None):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return dflt


def evaluar(isin: str) -> dict:
    fd = ROOT / "data" / "funds" / isin
    out = _load(fd / "output.json")
    if not isinstance(out, dict):
        return {"graves": ["no hay output.json"], "dudas": []}
    graves: list[str] = []
    try:
        from tools.sync_to_supabase import _validate_before_sync
        ok, reasons = _validate_before_sync(out, isin)
        graves += list(reasons or [])
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] validación de identidad no disponible: {str(e)[:80]}")

    dudas: list[dict] = []
    hoy = date.today().isoformat()
    try:
        from agents.dashboard_quality_agent import DashboardQualityAgent
        rep = DashboardQualityAgent(isin).run() or {}
        for f in rep.get("fallos") or []:
            if f.get("fail_type") in TIPOS_DUDA or f.get("regla_id") in REGLAS_DUDA_EXTRA:
                dudas.append({"titulo": f"Comprobar: {str(f.get('problema') or '')[:140]}",
                              "detalle": str(f.get("accion") or ""), "seccion": f.get("seccion"),
                              "regla": f.get("regla_id")})
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] auditoría del dashboard no disponible: {str(e)[:80]}")
    try:
        from tools.analysis_quality import assess_analysis_quality
        for w in assess_analysis_quality(out).get("warnings") or []:
            dudas.append({"titulo": f"Análisis incompleto: {w}", "detalle": "", "regla": "analysis_quality"})
    except Exception:
        pass
    meta = (_load(fd / "analyst_synthesis_cowork.json", {}) or {}).get("_meta") or {}
    for x in meta.get("anti_invencion_flagged") or []:
        dudas.append({"titulo": "El analista no pudo verificarlo del todo", "detalle": str(x), "regla": "anti_invencion"})
    for x in meta.get("supuestos") or []:
        dudas.append({"titulo": "Supuesto del analista", "detalle": str(x if isinstance(x, str) else json.dumps(x, ensure_ascii=False)),
                      "regla": "supuesto"})
    for d in dudas:
        d.update({"fuente": FUENTE, "fecha": hoy})
    return {"graves": graves, "dudas": dudas}


def _anotar_dudas(isin: str, dudas: list[dict]) -> bool:
    p = ROOT / "data" / "funds" / isin / "output.json"
    out = _load(p)
    if not isinstance(out, dict):
        return False
    prev = [x for x in (out.get("revision_pendiente") or []) if isinstance(x, dict)]
    otras = [x for x in prev if x.get("fuente") != FUENTE]
    nuevas = otras + dudas
    if [(x.get("titulo"), x.get("detalle")) for x in prev] == [(x.get("titulo"), x.get("detalle")) for x in nuevas]:
        return False
    out["revision_pendiente"] = nuevas
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return True


def _avisar_bloqueo(isin: str, nombre: str, graves: list[str]) -> None:
    try:
        from tools.seguimiento_fondos import _tarea
        _tarea({"titulo": (f"Análisis de {nombre} ({isin}) NO publicado por calidad: " + "; ".join(graves))[:240],
                "fecha": date.today().isoformat()}, False)
    except Exception as e:  # noqa: BLE001
        _log(f"[WARN] no pude crear la tarea de aviso en el portal: {str(e)[:100]}")


def main(isin: str) -> int:
    isin = isin.strip().upper()
    r = evaluar(isin)
    cambio = _anotar_dudas(isin, r["dudas"])
    if cambio:
        try:
            subprocess.run([sys.executable, str(ROOT / "dashboard" / "generate_dashboard.py"), isin],
                           cwd=str(ROOT), capture_output=True, text=True, timeout=300)
        except Exception as e:  # noqa: BLE001
            _log(f"[WARN] no pude regenerar el dashboard: {str(e)[:80]}")
    out = _load(ROOT / "data" / "funds" / isin / "output.json", {}) or {}
    res = {"isin": isin, "fecha": datetime.now().isoformat(timespec="seconds"),
           "publicable": not r["graves"], "graves": r["graves"], "n_dudas": len(r["dudas"]),
           "dudas": r["dudas"]}
    (ROOT / "data" / "funds" / isin / "quality_gate.json").write_text(
        json.dumps(res, ensure_ascii=False, indent=2), encoding="utf-8")
    _log(f"{isin}: {len(r['dudas'])} duda(s) anotadas en la pestaña Novedades")
    if r["graves"]:
        for g in r["graves"]:
            _log(f"  GRAVE: {g}")
        _log("BLOQUEADO: no se publica; sigue visible el análisis anterior")
        _avisar_bloqueo(isin, out.get("nombre") or isin, r["graves"])
        return 3
    _log("publicable")
    return 0


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    if len(sys.argv) < 2:
        print("uso: python -m tools.quality_gate ISIN")
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
