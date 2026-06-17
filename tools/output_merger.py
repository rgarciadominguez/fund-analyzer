"""
Output Merger — preserva ediciones manuales cuando el analyst regenera output.json.

Problema observado el 2026-04-26:
  - Patcheas output.nombre con valor correcto
  - Re-ejecutas pipeline → analyst sobrescribe nombre desde cnmv_data.json (que tiene valor antiguo)
  - Patch perdido

Solución: marker `_manual_edits` en output.json con lista de paths dotted que
fueron editados manualmente. Cuando analyst._save() escribe, merge_preserving
mantiene esos campos del existente.

Uso desde patches:
    from tools.output_merger import mark_manual_edit, save_output
    output["nombre"] = "DUNAS VALOR PRUDENTE FI"
    mark_manual_edit(output, "nombre")
    save_output(isin, output)

Uso desde analyst._save():
    from tools.output_merger import merge_preserving_manual_edits
    existing = load_output(isin) or {}
    final = merge_preserving_manual_edits(existing, new_output)
    save_output(isin, final)
"""
import json
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent
MANUAL_EDITS_KEY = "_manual_edits"  # lista de paths dotted: ["nombre", "gestores.perfiles", ...]


def _get_nested(d: dict, path: str):
    """Lee valor por path dotted (ej. 'analyst_synthesis.gestores.perfiles')."""
    cur = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _set_nested(d: dict, path: str, value):
    """Escribe valor por path dotted, creando dicts intermedios si faltan."""
    parts = path.split(".")
    cur = d
    for part in parts[:-1]:
        if part not in cur or not isinstance(cur.get(part), dict):
            cur[part] = {}
        cur = cur[part]
    cur[parts[-1]] = value


def mark_manual_edit(output: dict, path: str) -> None:
    """Marca un path como editado manualmente. Idempotente (no duplica)."""
    edits = output.setdefault(MANUAL_EDITS_KEY, [])
    if path not in edits:
        edits.append(path)


def unmark_manual_edit(output: dict, path: str) -> None:
    """Elimina marca manual (útil si quieres permitir que analyst lo regenere)."""
    edits = output.get(MANUAL_EDITS_KEY) or []
    if path in edits:
        edits.remove(path)


def get_manual_edits(output: dict) -> list:
    """Devuelve lista actual de paths manuales."""
    return output.get(MANUAL_EDITS_KEY, []) or []


def merge_preserving_manual_edits(existing: dict, new: dict) -> dict:
    """Merge 'new' sobre 'existing' preservando campos en _manual_edits del existing.

    Estrategia: arranca de 'new' (datos frescos), luego sobrescribe los
    paths marcados como manuales con valores del 'existing'.
    """
    if not existing:
        return dict(new) if new else {}
    if not new:
        return dict(existing)

    result = dict(new)
    manual_paths = get_manual_edits(existing)
    restored = []
    for path in manual_paths:
        val = _get_nested(existing, path)
        if val is not None:
            _set_nested(result, path, val)
            restored.append(path)

    # Conservar el marker
    if manual_paths:
        result[MANUAL_EDITS_KEY] = list(manual_paths)
    if restored:
        result.setdefault("_merge_log", []).append({
            "ts": datetime.now().isoformat(),
            "preserved_paths": restored,
        })
    return result


def load_output(isin: str) -> dict:
    """Carga output.json del fondo. Devuelve {} si no existe."""
    isin = isin.strip().upper()
    p = ROOT / "data" / "funds" / isin / "output.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_output(isin: str, output: dict, atomic: bool = True) -> None:
    """Guarda output.json con write atómico (.tmp → rename)."""
    isin = isin.strip().upper()
    # Garantiza kpis.anio_creacion desde la fecha de inicio del análisis (fallo
    # común: venía la fecha pero no se calculaba el KPI). Idempotente.
    try:
        from tools.anios_antiguedad import ensure_anio_creacion
        ensure_anio_creacion(output)
    except Exception:
        pass
    fd = ROOT / "data" / "funds" / isin
    fd.mkdir(parents=True, exist_ok=True)
    out_path = fd / "output.json"
    if atomic:
        tmp = out_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            tmp.replace(out_path)
        except Exception:
            out_path.write_text(tmp.read_text(encoding="utf-8"), encoding="utf-8")
            try: tmp.unlink()
            except Exception: pass
    else:
        out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    # Smoke test
    existing = {
        "nombre": "DUNAS VALOR PRUDENTE FI",
        "gestora": "DUNAS CAPITAL",
        "kpis": {"aum": 1554, "ter": 0.41},
        "_manual_edits": ["nombre", "kpis.aum"],
    }
    new = {
        "nombre": "SEGURFONDO GESTION DINAMICA",  # analyst quiere sobrescribir
        "gestora": "DUNAS CAPITAL",
        "kpis": {"aum": 1200, "ter": 0.45, "extra": 1},  # aum quiere bajar pero está protegido
    }
    merged = merge_preserving_manual_edits(existing, new)
    print(json.dumps(merged, ensure_ascii=False, indent=2))
    assert merged["nombre"] == "DUNAS VALOR PRUDENTE FI", "nombre debe preservarse"
    assert merged["kpis"]["aum"] == 1554, "aum debe preservarse"
    assert merged["kpis"]["ter"] == 0.45, "ter no era manual, debe actualizarse"
    assert merged["kpis"]["extra"] == 1, "campo nuevo debe entrar"
    print("\nOK")
