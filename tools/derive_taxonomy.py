"""Deriva la taxonomía de un fondo (tipo/geo/categoria_rf/srri/tags) desde
MyInvestor (myinvestor_data.json) + categoría Morningstar, SIN depender del Excel.
Rellena SOLO los campos vacíos del grupo (no pisa lo curado a mano/Excel).

Pensado para que un fondo NUEVO se autopopule al analizarse. Uso desde
sync_to_supabase: derive_and_fill(client, isin).
"""
from __future__ import annotations
import json
from pathlib import Path

from tools.sync_taxonomy import m_geo, m_cat_rf, m_tags, m_srri


def _tipo_from(asset_class, cat_ms):
    a = (asset_class or "").lower(); c = (cat_ms or "").lower()
    if "money market" in c or "ultra short" in c or "monetar" in c:
        return "Monetario"
    if "renta variable" in a or "equity" in c:
        return "RV"
    if "renta fija" in a or "bond" in c:
        return "RF"
    if "mixto" in a or "allocation" in c:
        return "Mixtos"
    if "alternativ" in a:
        return "Alternativos"
    if "materias" in a or "commodit" in c:
        return "Materias_primas"
    return None


def derive_and_fill(client, isin: str, log=None) -> dict:
    isin = (isin or "").upper().strip()
    root = Path(__file__).resolve().parent.parent
    mi = {}
    jp = root / "data" / "funds" / isin / "myinvestor_data.json"
    if jp.exists():
        try:
            mi = json.loads(jp.read_text(encoding="utf-8"))
        except Exception:
            mi = {}
    rg = client.table("funds").select("fund_group_id,nombre_clase").eq("isin", isin).execute().data
    if not rg:
        return {}
    gid = rg[0]["fund_group_id"]
    nombre = rg[0].get("nombre_clase")
    g = client.table("fund_groups").select(
        "tipo_activo,geografia,categoria_rf,srri,caracteristicas_especiales,categoria_morningstar,nombre_base"
    ).eq("fund_group_id", gid).execute().data
    g = g[0] if g else {}
    cat_ms = g.get("categoria_morningstar")
    nombre = nombre or g.get("nombre_base")

    tipo = g.get("tipo_activo") or _tipo_from(mi.get("asset_class") or mi.get("categoria_morningstar_es"), cat_ms)
    geo_src = g.get("geografia") or mi.get("geographic_zone")
    geo = m_geo(geo_src, tipo, nombre)
    upd = {}
    if not g.get("tipo_activo") and tipo:
        upd["tipo_activo"] = tipo
    if not g.get("geografia") and geo:
        upd["geografia"] = geo
    if not g.get("categoria_rf"):
        cr = m_cat_rf(cat_ms, tipo)
        if cr:
            upd["categoria_rf"] = cr
    if not g.get("srri"):
        sr = m_srri(mi.get("srri"))
        if sr:
            upd["srri"] = sr
    if not g.get("caracteristicas_especiales"):
        tags = m_tags(None, None, cat_ms, nombre)
        if tags:
            upd["caracteristicas_especiales"] = tags
    if upd:
        client.table("fund_groups").update(upd).eq("fund_group_id", gid).execute()
        if log:
            log(f"[SYNC] taxonomía derivada (campos vacíos): {upd}")
    return upd
