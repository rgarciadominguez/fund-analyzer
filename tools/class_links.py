"""Vínculos manuales de clase: alias_isin → primary_isin (mismo fondo, otra clase).

Caso: un fondo tiene varias clases (ISINs distintos). El agrupado automático por
nombre+gestora falla cuando los nombres no coinciden exactamente. Este registro
permite vincular A MANO una clase a un fondo ya analizado:
  - la clase vinculada NO se re-analiza,
  - en el catálogo aparece como SUB-CLASE del primario (no como fila propia),
  - al abrirla, va al análisis del primario.

Registro local en `data/fund_class_links.json` (fuente de verdad, reversible).
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LINKS_PATH = ROOT / "data" / "fund_class_links.json"
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def _load() -> dict:
    if not LINKS_PATH.exists():
        return {}
    try:
        return json.loads(LINKS_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _save(d: dict) -> None:
    LINKS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = LINKS_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(LINKS_PATH)


def all_links() -> dict:
    """{alias_isin: {primary_isin, clase_label, linked_at}}."""
    return _load()


def resolve_primary(isin: str) -> str:
    """Si `isin` es un alias devuelve su primary (resolviendo cadenas); si no, `isin`."""
    isin = (isin or "").upper().strip()
    d = _load()
    seen: set[str] = set()
    while isin in d and isin not in seen:
        seen.add(isin)
        isin = (d[isin].get("primary_isin") or "").upper().strip()
    return isin


def is_alias(isin: str) -> bool:
    return (isin or "").upper().strip() in _load()


def get_classes(primary: str) -> list[dict]:
    """Lista de clases vinculadas a un primario: [{isin, clase_label}]."""
    primary = (primary or "").upper().strip()
    d = _load()
    out = [{"isin": a, "clase_label": v.get("clase_label", "")}
           for a, v in d.items() if (v.get("primary_isin") or "").upper().strip() == primary]
    return sorted(out, key=lambda x: x["isin"])


def add_link(alias: str, primary: str, label: str = "") -> dict:
    alias = (alias or "").upper().strip()
    primary = (primary or "").upper().strip()
    if not _ISIN_RE.match(alias):
        return {"ok": False, "error": f"ISIN de clase inválido: {alias}"}
    if not _ISIN_RE.match(primary):
        return {"ok": False, "error": f"ISIN de fondo primario inválido: {primary}"}
    if alias == primary:
        return {"ok": False, "error": "el alias y el primario no pueden ser iguales"}
    # El primario real es la raíz (por si el primario indicado fuese a su vez un alias).
    primary_root = resolve_primary(primary)
    if primary_root == alias:
        return {"ok": False, "error": "el vínculo crearía un ciclo"}
    d = _load()
    # Si el alias ya era primario de otras clases, re-apuntar esas clases a la raíz.
    for a, v in d.items():
        if (v.get("primary_isin") or "").upper().strip() == alias:
            v["primary_isin"] = primary_root
    d[alias] = {"primary_isin": primary_root, "clase_label": (label or "").strip(),
                "linked_at": datetime.now(timezone.utc).isoformat()}
    _save(d)
    return {"ok": True, "alias": alias, "primary": primary_root}


def remove_link(alias: str) -> dict:
    alias = (alias or "").upper().strip()
    d = _load()
    if alias in d:
        del d[alias]
        _save(d)
        return {"ok": True, "alias": alias}
    return {"ok": False, "error": "no existe ese vínculo"}


def sync_link_to_supabase(alias: str, primary: str) -> dict:
    """Propaga el vínculo a Supabase: la fila `funds` del alias hereda el
    `fund_group_id` del primario → el catálogo PÚBLICO los agrupa (colapsa por
    fund_group_id). Best-effort: si Supabase no está configurado, no rompe.
    """
    alias = (alias or "").upper().strip()
    primary = (primary or "").upper().strip()
    try:
        from tools.supabase_client import get_client
        client = get_client()
    except Exception as e:
        return {"ok": False, "error": f"supabase no disponible: {str(e)[:120]}"}
    try:
        r = client.table("funds").select("fund_group_id").eq("isin", primary).execute()
        if not r.data:
            return {"ok": False, "error": "el primario no está en Supabase todavía"}
        gid = r.data[0].get("fund_group_id")
        if not gid:
            return {"ok": False, "error": "el primario no tiene fund_group_id"}
        client.table("funds").update({"fund_group_id": gid}).eq("isin", alias).execute()
        return {"ok": True, "fund_group_id": gid}
    except Exception as e:
        return {"ok": False, "error": str(e)[:160]}


__all__ = ["all_links", "resolve_primary", "is_alias", "get_classes",
           "add_link", "remove_link", "sync_link_to_supabase", "LINKS_PATH"]
