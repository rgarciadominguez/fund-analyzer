"""Ejecuta ManagerDeepAgent en fondos sin gestores reales.
Usa búsquedas Google + Citywire + web gestora."""
import asyncio
import json
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

from agents.manager_deep_agent import ManagerDeepAgent

# Fondos sin gestores reales (verificado en analyst_synthesis.gestores.perfiles)
TARGETS = [
    ("ES0116567035", "CARTESIO X, FI", "Cartesio Inversiones SGIIC"),
    ("ES0128520006", "RENTA 4 RENTA FIJA 6 MESES, FI", "Renta 4 Gestora SGIIC"),
    ("ES0140794001", "GAMMA GLOBAL, FI", "Singular Asset Management SGIIC"),
    ("ES0173311103", "RENTA 4 MULTIGESTION, FI", "Renta 4 Gestora SGIIC"),
    ("ES0175414012", "DUNAS VALOR EQUILIBRADO FI", "Dunas Capital Asset Management SGIIC"),
    ("ES0175437039", "DUNAS VALOR PRUDENTE FI", "Dunas Capital Asset Management SGIIC"),
    ("ES0175902008", "SIGMA INTERNACIONAL, FI", "Sigma Inversiones SGIIC"),
    ("ES0182527038", "CARTESIO Y, FI", "Cartesio Inversiones SGIIC"),
]


async def run_one(isin, fund_name, gestora):
    print(f"\n{'='*70}\n{isin} | {fund_name}\n{'='*70}")
    agent = ManagerDeepAgent(isin=isin, fund_name=fund_name, gestora=gestora)
    try:
        result = await agent.run()
        # Read what was saved
        prof_path = ROOT / "data" / "funds" / isin / "manager_profile.json"
        if prof_path.exists():
            with open(prof_path, encoding="utf-8") as f:
                prof = json.load(f)
            equipo = prof.get("equipo", []) or prof.get("equipo_detalle_web", [])
            print(f"\n[RESULT] {isin}: equipo={len(equipo)} | nombres={[g.get('nombre') if isinstance(g,dict) else g for g in equipo[:5]]}")
        else:
            print(f"[RESULT] {isin}: sin manager_profile.json")
    except Exception as exc:
        print(f"[ERROR] {isin}: {exc}")
        import traceback
        traceback.print_exc()


async def main():
    targets = TARGETS
    if len(sys.argv) > 1:
        # Filtrar por ISIN si pasados argumentos
        wanted = set(a.strip().upper() for a in sys.argv[1:])
        targets = [t for t in TARGETS if t[0] in wanted]
    for isin, name, gestora in targets:
        await run_one(isin, name, gestora)
    print("\nDONE")


if __name__ == "__main__":
    asyncio.run(main())
