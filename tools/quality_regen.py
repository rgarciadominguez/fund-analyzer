"""Regeneración DIRIGIDA por calidad: una sola pasada, solo secciones con fallos DE FONDO.

Rafa (2026-09-24): "que los fallos de fondo disparen una regeneración automática, pero eficiente, sin
bucles y sin gastar tokens en regeneraciones que no aportan". Reglas:
  · Solo fallos `fail_type == "content"` de la lista blanca (genérica, sin cifras, ejes de diferenciación
    ausentes, equipo genérico). Los de estilo/estructura (headers, formato) NO disparan nada.
  · UNA pasada por run: marca `quality_regen_done` en meta_report.json y `quality_regen.json` con `intento`.
  · Regenera SOLO las secciones afectadas (el consumidor las mergea y preserva el resto verbatim).
  · Interruptor: config.json `quality_regen: false` o env QUALITY_REGEN=0 lo desactivan.
Flujo: quality_report.json → decidir → escribir quality_regen.json → skill analyst-cowork (claude) →
_consume_cowork_analyst → volver a evaluar calidad → registrar antes/después en meta_report.json.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS = ROOT / "data" / "funds"

# Reglas de FONDO que justifican una regeneración (id → sección). Nada de estilo.
REGLAS_FONDO = {
    "estrategia_has_cifras": "estrategia",
    "diferenciacion_activos": "estrategia",
    "diferenciacion_gestion": "estrategia",
    "diferenciacion_geografia": "estrategia",
    "diferenciacion_filosofia_equipo": "estrategia",
    "equipo_not_generic": "gestores",
    "gestores_equipo_no_generico": "gestores",
    "cartera_has_cifras": "cartera",
    "historia_has_cifras": "historia",
}
MODEL = os.environ.get("MODEL_ANALYST", "claude-opus-4-8")


def _load(p: Path):
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def enabled(isin: str) -> bool:
    if os.environ.get("QUALITY_REGEN", "1") == "0":
        return False
    cfg = _load(FUNDS / isin / "config.json") or {}
    return cfg.get("quality_regen", True) is not False


def decide(isin: str) -> dict:
    """{secciones: [...], motivos: {sec: [...]}} a partir de quality_report.json (sin escribir nada)."""
    rep = _load(FUNDS / isin / "quality_report.json") or {}
    secciones: dict[str, list[str]] = {}
    for f in rep.get("fallos") or []:
        rid = f.get("regla_id")
        if f.get("fail_type") != "content" or rid not in REGLAS_FONDO:
            continue
        sec = REGLAS_FONDO[rid]
        secciones.setdefault(sec, []).append(str(f.get("problema") or rid))
    return {"secciones": sorted(secciones), "motivos": secciones}


def run(isin: str, log=print, dry_run: bool = False) -> dict:
    isin = isin.upper().strip()
    fd = FUNDS / isin
    meta_p = fd / "meta_report.json"
    meta = _load(meta_p) or {}
    if not enabled(isin):
        log(f"[QREGEN] {isin}: desactivado (config/env)")
        return {"ran": False, "reason": "disabled"}
    if meta.get("quality_regen_done"):
        log(f"[QREGEN] {isin}: ya se hizo una pasada en este análisis ({meta['quality_regen_done'].get('fecha')}) → no se repite")
        return {"ran": False, "reason": "already_done"}
    d = decide(isin)
    if not d["secciones"]:
        log(f"[QREGEN] {isin}: sin fallos de fondo → no se regenera nada")
        return {"ran": False, "reason": "no_fondo_failures"}
    antes = _load(fd / "quality_report.json") or {}
    log(f"[QREGEN] {isin}: fallos de fondo en {d['secciones']} → regeneración dirigida (1 pasada)")
    for sec, ms in d["motivos"].items():
        for m in ms:
            log(f"[QREGEN]   {sec}: {m[:120]}")
    if dry_run:
        return {"ran": False, "reason": "dry_run", "decision": d}
    (fd / "quality_regen.json").write_text(json.dumps({**d, "intento": 1, "fecha": datetime.now().isoformat()}, ensure_ascii=False, indent=2), encoding="utf-8")
    # 1) skill analyst-cowork (misma invocación que el .bat, paso 5)
    logfile = ROOT / "logs" / f"skill_analyst_qregen_{isin}.log"
    cmd = [sys.executable, "-m", "tools.claude_cowork", str(logfile), f"analyst cowork {isin}",
           "--model", MODEL, "--allowedTools", "Read,Write,Bash,Edit,Agent,Glob,Grep"]
    r = subprocess.run(cmd, cwd=str(ROOT), timeout=3600)
    ok_skill = r.returncode == 0
    log(f"[QREGEN] skill analyst-cowork rc={r.returncode}")
    despues = antes
    if ok_skill:
        # 2) consumir la síntesis (merge selectivo por quality_regen.json → ver orchestrator._consume_cowork_analyst)
        try:
            from agents.orchestrator import _consume_cowork_analyst
            _consume_cowork_analyst(isin, fd, lambda a, b, m: log(f"[QREGEN][{a}] {m}"))
        except Exception as e:  # noqa: BLE001
            log(f"[QREGEN] consumo de la síntesis falló: {e}")
            ok_skill = False
        # 3) volver a evaluar
        try:
            from agents.dashboard_quality_agent import DashboardQualityAgent
            despues = DashboardQualityAgent(isin).run()
            (fd / "quality_report.json").write_text(json.dumps(despues, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:  # noqa: BLE001
            log(f"[QREGEN] re-evaluación de calidad falló: {e}")
    res = {"ran": ok_skill, "secciones": d["secciones"], "fecha": datetime.now().isoformat(),
           "fallos_antes": len(antes.get("fallos") or []), "fallos_despues": len((despues or {}).get("fallos") or []),
           "score_antes": antes.get("score"), "score_despues": (despues or {}).get("score")}
    meta = _load(meta_p) or {}
    meta["quality_regen_done"] = res
    try:
        meta_p.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    try:
        (fd / "quality_regen.json").unlink(missing_ok=True)
    except Exception:
        pass
    log(f"[QREGEN] {isin}: fallos {res['fallos_antes']} → {res['fallos_despues']} | score {res['score_antes']} → {res['score_despues']}")
    return res


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    a = [x for x in sys.argv[1:] if not x.startswith("--")]
    if not a:
        print("uso: python -m tools.quality_regen ISIN [--dry-run]")
        sys.exit(1)
    print(json.dumps(run(a[0], dry_run="--dry-run" in sys.argv), ensure_ascii=False, indent=1))
