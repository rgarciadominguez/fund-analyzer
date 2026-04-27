"""Marca paths editados manualmente en los output.json existentes.
Esto protege el trabajo realizado el 2026-04-26/27 (nombres patcheados,
gestores limpiados, posiciones extraídas vía sec10) de ser sobrescrito
por futuros runs del analyst."""
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from tools.output_merger import (
    load_output, save_output, mark_manual_edit, get_manual_edits,
)

# Para cada fondo, listamos los paths que se han editado manualmente
# y deben preservarse en futuras re-ejecuciones del analyst.
ISIN_MARKS = {
    # Nombres patcheados desde último PDF semestral CNMV
    "ES0128520006": ["nombre", "gestora"],
    "ES0173311103": ["nombre", "gestora"],
    "ES0175414012": ["nombre", "gestora"],
    "ES0175437039": ["nombre", "gestora"],
    # Gestores curados manualmente (limpios + verified)
    "ES0116567035": ["analyst_synthesis.gestores.perfiles"],  # Cartesio X (cleaned Dr.Vargas)
    "ES0140794001": ["analyst_synthesis.gestores.perfiles"],  # Gamma (manual google)
    "ES0175902008": ["analyst_synthesis.gestores.perfiles"],  # Sigma
    "ES0128520006_g": ["analyst_synthesis.gestores.perfiles"],  # R4 RF (manual)
    "ES0173311103_g": ["analyst_synthesis.gestores.perfiles"],  # R4 Multi (manual)
    "ES0175414012_g": ["analyst_synthesis.gestores.perfiles"],  # Dunas Equilib (sibling)
    "ES0175437039_g": ["analyst_synthesis.gestores.perfiles"],  # Dunas Prudente (sibling)
    "ES0182527038": ["analyst_synthesis.gestores.perfiles"],  # Cartesio Y (sibling)
    # Posiciones enriquecidas con sectores Gemini
    "ES0140794001_p": ["posiciones.actuales", "cuantitativo.serie_rentabilidad"],
    "ES0175902008_p": ["posiciones.actuales", "cuantitativo.serie_rentabilidad"],
}

# Aplanar el dict (las claves _g, _p son sufijos para listar varios sets de paths
# para el mismo ISIN). Compactar a dict {isin: [paths_unicos]}.
def _consolidate(specs):
    out = {}
    for k, paths in specs.items():
        isin = k.split("_")[0]
        out.setdefault(isin, set()).update(paths)
    return {isin: sorted(p) for isin, p in out.items()}


def main():
    consolidated = _consolidate(ISIN_MARKS)
    for isin, paths in consolidated.items():
        out = load_output(isin)
        if not out:
            print(f"  {isin}: sin output.json")
            continue
        before = len(get_manual_edits(out))
        for p in paths:
            mark_manual_edit(out, p)
        after = len(get_manual_edits(out))
        save_output(isin, out)
        print(f"  {isin}: marks {before} -> {after} (paths: {paths})")


if __name__ == "__main__":
    main()
    print("\nDONE — runs futuros del analyst preservarán estos campos")
