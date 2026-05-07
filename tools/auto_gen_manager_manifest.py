"""auto_gen_manager_manifest — fallback helper para analizar_fondo.bat.

Uso:
    python -m tools.auto_gen_manager_manifest <ISIN>

Genera un pending_manager_deep.json mínimo cuando manager_profiler legacy
falló (típicamente por Gemini denied) y no dejó manifest. La skill
manager-deep-cowork lo recoge y al menos puede buscar online basándose
en fund_name + gestora.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from tools.pending_manifest import append_manager_deep_task


def main(isin: str) -> int:
    isin = isin.strip().upper()
    fund_dir = Path("data/funds") / isin
    if not fund_dir.exists():
        print(f"[ERROR] {fund_dir} no existe")
        return 1

    if (fund_dir / "pending_manager_deep.json").exists():
        print("[SKIP] pending_manager_deep.json ya existe")
        return 0

    # Lee fund_name + gestora del primer JSON disponible
    fund_name = ""
    gestora = ""
    for fname in ("cnmv_data.json", "intl_data.json", "cssf_data.json"):
        p = fund_dir / fname
        if not p.exists():
            continue
        try:
            src = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not fund_name:
            fund_name = src.get("nombre", "") or src.get("nombre_oficial", "") or ""
        if not gestora:
            gestora = src.get("gestora", "") or src.get("gestora_oficial", "") or ""
        if fund_name and gestora:
            break

    if not fund_name:
        print(f"[WARN] No fund_name en cnmv/intl/cssf. Manifest mínimo sin nombre.")

    append_manager_deep_task(
        fund_dir, isin,
        task_type="identify_lead_co",
        fund_name=fund_name,
        gestora=gestora,
        candidate_names=[],
        candidate_urls=[],
        context=(
            "Auto-generado por bat: manager_profiler legacy falló (probable "
            "Gemini denied). Identifica lead/co/asistentes consultando web "
            "gestora oficial, finect.com, citywire.com, morningstar, expansion. "
            f"Fondo: {fund_name}. Gestora: {gestora or 'desconocida'}."
        ),
    )
    append_manager_deep_task(
        fund_dir, isin,
        task_type="extract_articles",
        fund_name=fund_name,
        gestora=gestora,
        context=(
            "Tras identify_lead_co, busca 3-5 articulos por gestor identificado: "
            "entrevistas, comentarios de mercado, perfiles. Fuentes recomendadas: "
            "valuewalk, finect, citywire, morningstar, expansion, value investing fm."
        ),
    )
    print(f"[OK] pending_manager_deep.json auto-generado para {isin} "
          f"(fund_name='{fund_name[:40]}', gestora='{gestora[:30]}')")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python -m tools.auto_gen_manager_manifest <ISIN>")
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
