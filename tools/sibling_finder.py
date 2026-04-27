"""
Auto-sibling finder — encuentra fondos hermanos (misma gestora) y propaga
gestores cuando un fondo no los tiene pero un hermano sí.

Reemplaza el hardcoded copy_gestores_siblings.py con detección automática
basada en el campo `gestora` del output.json.

Uso CLI:
    python -m tools.sibling_finder                # propaga a todos los fondos sin gestores
    python -m tools.sibling_finder ES0XXXXX       # solo a un ISIN concreto
    python -m tools.sibling_finder --dry-run      # solo lista, no escribe

Programáticamente:
    from tools.sibling_finder import find_siblings, propagate_gestores
    siblings = find_siblings("ES0182527038")  # devuelve [(isin, gestora), ...]
    propagate_gestores("ES0182527038")        # aplica copy si hay hermano con gestores
"""
import json
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent.parent


def _norm_gestora(g: str) -> str:
    """Normaliza nombre de gestora para matching: minúsculas, sin sufijos legales."""
    if not g:
        return ""
    s = g.lower()
    # Eliminar sufijos legales comunes
    for suf in ["s.g.i.i.c.", "sgiic", "s.g.i.i.c", "s.a.u.", "s.a.", "sa", "sau",
                ", s.a.", " s.a", ", s.l.", " s.l"]:
        s = s.replace(suf, "")
    # Eliminar grupo (ej: "GRUPO RENTA 4")
    s = re.sub(r'\b(grupo|group)\b', '', s)
    s = re.sub(r'\s+', ' ', s).strip(' ,.')
    return s


def _load_funds() -> list:
    """Lee todos los fondos disponibles con sus metadatos clave."""
    funds = []
    for fd in (ROOT / "data" / "funds").glob("ES*"):
        out = fd / "output.json"
        if not out.exists():
            continue
        try:
            d = json.loads(out.read_text(encoding="utf-8"))
        except Exception:
            continue
        gestora = d.get("gestora", "") or ""
        perfiles = ((d.get("analyst_synthesis") or {}).get("gestores") or {}).get("perfiles") or []
        funds.append({
            "isin": fd.name,
            "nombre": d.get("nombre", ""),
            "gestora": gestora,
            "gestora_norm": _norm_gestora(gestora),
            "perfiles_count": len(perfiles),
            "perfiles": perfiles,
            "fund_dir": fd,
            "output_path": out,
        })
    return funds


def find_siblings(isin: str) -> list:
    """Devuelve fondos con la misma gestora normalizada (excluyendo el propio)."""
    isin = isin.strip().upper()
    funds = _load_funds()
    target = next((f for f in funds if f["isin"] == isin), None)
    if not target or not target["gestora_norm"]:
        return []
    return [
        f for f in funds
        if f["isin"] != isin and f["gestora_norm"] == target["gestora_norm"]
    ]


def _name_root(nombre: str) -> str:
    """Extrae primera palabra significativa del nombre (Dunas, Cartesio, Renta 4...).
    Para detectar familia de fondos del mismo gestor (no solo distribuidor)."""
    if not nombre:
        return ""
    tokens = re.split(r'[\s,.]+', nombre.upper())
    # Skip articles/prefijos cortos
    skip = {"FONDO", "FUND", "FI", "SICAV", "EL", "LA", "DE", "EUR", "USD"}
    for t in tokens:
        if t and t not in skip and len(t) > 2:
            return t
    return tokens[0] if tokens else ""


def propagate_gestores(isin: str, dry_run: bool = False) -> dict:
    """Si target no tiene perfiles y hay hermano con perfiles, copia.
    Heurística: solo copia si target y source comparten ROOT del nombre
    (Cartesio X/Y, Dunas Flexible/Prudente). Esto evita falsos positivos
    cuando la gestora es solo un distribuidor (Renta 4 administra fondos
    independientes con gestores propios)."""
    isin = isin.strip().upper()
    funds = _load_funds()
    target = next((f for f in funds if f["isin"] == isin), None)
    if not target:
        return {"isin": isin, "status": "not_found"}

    if target["perfiles_count"] > 0:
        return {"isin": isin, "status": "already_has_profiles", "n": target["perfiles_count"]}

    # Buscar hermanos con perfiles + filtrar por similaridad de nombre
    target_root = _name_root(target["nombre"])
    siblings = find_siblings(isin)
    sibs_with = [
        s for s in siblings
        if s["perfiles_count"] > 0 and _name_root(s["nombre"]) == target_root
    ]
    if not sibs_with:
        return {"isin": isin, "status": "no_matching_sibling",
                "n_siblings": len(siblings),
                "target_root": target_root}

    # Tomar el hermano con más perfiles
    src = max(sibs_with, key=lambda s: s["perfiles_count"])

    if dry_run:
        return {
            "isin": isin, "status": "would_copy",
            "from": src["isin"], "from_nombre": src["nombre"],
            "n_perfiles": src["perfiles_count"],
            "nombres": [p.get("nombre", "?") for p in src["perfiles"]],
        }

    # Copiar
    bak = target["fund_dir"] / "output.pre_sibling_auto.json"
    if not bak.exists():
        shutil.copy(target["output_path"], bak)

    out = json.loads(target["output_path"].read_text(encoding="utf-8"))
    asy = out.setdefault("analyst_synthesis", {})
    g = asy.setdefault("gestores", {})
    # Marcar fuente para trazabilidad
    perfiles_copy = []
    for p in src["perfiles"]:
        p2 = dict(p)
        p2.setdefault("fuente", "sibling_auto")
        p2["copied_from"] = src["isin"]
        perfiles_copy.append(p2)
    g["perfiles"] = perfiles_copy
    target["output_path"].write_text(
        json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    return {
        "isin": isin, "status": "copied",
        "from": src["isin"], "from_nombre": src["nombre"],
        "n_perfiles": len(perfiles_copy),
        "nombres": [p.get("nombre", "?") for p in perfiles_copy],
    }


def list_all_groupings():
    """Imprime el mapping gestora → [ISINs]."""
    funds = _load_funds()
    groups: dict = {}
    for f in funds:
        if not f["gestora_norm"]:
            continue
        groups.setdefault(f["gestora_norm"], []).append(f)
    print(f"Total gestoras únicas: {len(groups)}")
    for gnorm, fs in sorted(groups.items(), key=lambda x: -len(x[1])):
        if len(fs) <= 1:
            continue
        print(f"\n=== {gnorm} ({len(fs)} fondos) ===")
        for f in fs:
            mark = "✓" if f["perfiles_count"] > 0 else "✗"
            print(f"  {mark} {f['isin']} | {f['nombre'][:40]:<40} | perfiles={f['perfiles_count']}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    args = sys.argv[1:]
    dry = "--dry-run" in args
    args = [a for a in args if not a.startswith("-")]

    if not args:
        # Modo: listar agrupaciones + propagar a todos los huérfanos
        list_all_groupings()
        print("\n\n=== PROPAGACIÓN ===")
        for fd in sorted((ROOT / "data" / "funds").glob("ES*")):
            r = propagate_gestores(fd.name, dry_run=dry)
            if r["status"] == "copied":
                print(f"  {r['isin']}: copied {r['n_perfiles']} from {r['from']} -> {r['nombres']}")
            elif r["status"] == "would_copy":
                print(f"  [DRY] {r['isin']}: would copy {r['n_perfiles']} from {r['from']}")
    else:
        for isin in args:
            r = propagate_gestores(isin.strip().upper(), dry_run=dry)
            print(json.dumps(r, ensure_ascii=False, indent=2))
