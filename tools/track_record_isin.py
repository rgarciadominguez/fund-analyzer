"""Resuelve el ISIN de referencia para el TRACK RECORD de un fondo multi-clase.

Problema: el track record cuantitativo (serie NAV, rentabilidades anuales, CAGR,
volatilidad, drawdown, rentab. por plazo) se calculaba SIEMPRE sobre el ISIN que
entra al pipeline. Si analizas una clase NUEVA (poco histórico), el fondo aparecía
con un track record corto pese a existir una clase antigua con 5+ años.

Solución: para el track record del FONDO (no las métricas por-clase), usar la clase
MÁS ANTIGUA del grupo (min `funds.fecha_creacion_clase`) que tenga serie NAV válida
en Morningstar. Mismo criterio que ya usan `anios_antiguedad.fill_anios` (años =
clase más antigua) y `reconcile_fund_groups._primary` (fecha_creacion_clase ASC).

Las métricas POR CLASE siguen calculándose por ISIN en `tools/quant_sync.py` — esto
solo cambia la SERIE DE REFERENCIA del track record de grupo, que es la más larga.

Degrada con gracia: si no hay grupo, ni fechas, ni Supabase, devuelve el ISIN de
entrada (comportamiento anterior).
"""
from __future__ import annotations

_MIN_POINTS = 30  # mismo umbral que metrics_from_series para considerar la serie útil


def _get_client(client):
    if client is not None:
        return client
    from tools.supabase_client import get_client
    return get_client()


def _class_date(r: dict) -> str:
    """fecha_creacion_clase normalizada a 'YYYY-MM-DD' ordenable; sin fecha va al final."""
    d = (r or {}).get("fecha_creacion_clase")
    s = str(d)[:10] if d else ""
    return s if s else "9999-99-99"


def _ordered_by_age(client, isin: str) -> list:
    """ISINs del grupo del `isin` ordenados para track record: primero las clases de la
    MISMA divisa que el `isin` (antigüedad ASC), luego el resto (antigüedad ASC). Así el
    histórico se ancla en la clase más veterana de la divisa correcta (una clase EUR no
    coge la serie GBP), pero si no hay veterana en la misma divisa cae a la más antigua.
    El propio `isin` se garantiza al final como fallback. Lista vacía si no hay grupo."""
    rg = client.table("funds").select("fund_group_id,divisa").eq("isin", isin).execute().data
    if not rg:
        return []
    gid = rg[0].get("fund_group_id")
    if not gid:
        return []
    tgt_cur = (rg[0].get("divisa") or "").upper()
    rows = client.table("funds").select(
        "isin,fecha_creacion_clase,divisa").eq("fund_group_id", gid).execute().data or []

    def _key(r):
        cur = (r.get("divisa") or "").upper()
        same = 0 if (tgt_cur and cur == tgt_cur) else 1
        return (same, _class_date(r), r.get("isin", ""))

    cands = sorted([r for r in rows if r.get("isin")], key=_key)
    ordered = []
    for r in cands:
        iu = r["isin"].upper()
        if iu not in ordered:
            ordered.append(iu)
    if isin not in ordered:
        ordered.append(isin)
    return ordered


def oldest_class_isins(isin, client=None, log=None):
    """Lista de ISINs del MISMO fondo ordenada para sourcing de DOCUMENTOS: la clase más
    antigua (misma divisa primero) primero. A diferencia de resolve_track_record, NO sondea
    la serie NAV (no descarga nada) — solo devuelve los ISINs del grupo por antigüedad, para
    que el sourcing de AR/SAR pruebe también la clase veterana (más años de documentos) cuando
    se analiza una clase nueva. Best-effort: si no hay grupo/Supabase devuelve [isin]."""
    isin = (isin or "").upper().strip()
    if not isin:
        return []
    try:
        client = _get_client(client)
        ordered = _ordered_by_age(client, isin)
        return ordered or [isin]
    except Exception as e:
        if log:
            log(f"[oldest-class] fallo ({isin}): {type(e).__name__} {str(e)[:80]}")
        return [isin]


def resolve_track_record(client, isin, log=None, max_probe=4):
    """Devuelve (tr_isin, serie_nav) de la clase más antigua del grupo con serie NAV
    válida (>= _MIN_POINTS puntos). Reutiliza la serie ya bajada para no re-fetchear.
    Fallback: (isin, serie_de_isin). `client` puede ser None (se crea uno)."""
    from tools.morningstar_daily import fetch_series
    isin = (isin or "").upper().strip()
    if not isin:
        return isin, []
    try:
        client = _get_client(client)
        ordered = _ordered_by_age(client, isin) or [isin]
        for cand in ordered[:max_probe]:
            s = fetch_series(cand)
            if len(s) >= _MIN_POINTS:
                if log and cand != isin:
                    log(f"[track-record] {isin} -> {cand} (clase más antigua con serie NAV)")
                return cand, s
        # Ninguna de las más antiguas tenía serie: intenta el propio ISIN de entrada.
        s = fetch_series(isin) if isin not in ordered[:max_probe] else []
        return isin, s
    except Exception as e:
        if log:
            log(f"[track-record] fallo resolviendo ({isin}): {type(e).__name__} {str(e)[:80]}")
        return isin, fetch_series(isin)


def resolve_track_record_isin(isin, client=None, log=None, max_probe=4):
    """Solo el ISIN de la clase con más track record (más antigua con serie NAV).
    Fallback: el propio `isin`. Best-effort: nunca lanza."""
    try:
        tr_isin, _ = resolve_track_record(client, isin, log=log, max_probe=max_probe)
        return tr_isin
    except Exception:
        return (isin or "").upper().strip()


if __name__ == "__main__":
    import sys
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    arg = (sys.argv[1] if len(sys.argv) > 1 else "").upper().strip()
    if not arg:
        print("uso: python -m tools.track_record_isin <ISIN>")
        raise SystemExit(1)
    tr, serie = resolve_track_record(None, arg, log=print)
    print(f"{arg} -> track-record ISIN: {tr}  (puntos serie: {len(serie)})")
