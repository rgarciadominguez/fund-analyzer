"""Datos cuantitativos de un fondo desde el screener público de Morningstar.

Misma vía que `morningstar_classes` (key pública `klr5zyak8x`, sin login). Trae
lo que Yahoo no da o da peor: Medalist rating, riesgo a 3/5/10A (alpha, beta,
sharpe, sortino, max drawdown...), valoración (P/E, P/B, cap. media), ratios de
renta fija (calidad crediticia, maturity, duración), comisiones, y la serie NAV
para el gráfico de captura.

NO accesible por esta vía (API SAL con login, 401): holdings, % por rango de
capitalización, pesos por sector/país, fecha de compra por posición.

Uso:
    python -m tools.morningstar_quant LU0203975437
"""
from __future__ import annotations

import re
import sys

import httpx

_UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
_SCR = "https://lt.morningstar.com/api/rest.svc/klr5zyak8x/security/screener"
_TS = "https://tools.morningstar.es/api/rest.svc/timeseries_price/klr5zyak8x"
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")

# medalist: número → etiqueta Morningstar
_MEDALIST = {5: "Gold", 4: "Silver", 3: "Bronze", 2: "Neutral", 1: "Negative"}

_DP = [
    "SecId", "Name", "Isin", "FundId", "CategoryName", "GlobalCategoryName",
    "BrandingCompanyName", "Domicile", "InceptionDate", "FundTNAV", "OngoingCharge",
    "ManagementFee", "MaxFrontEndLoad", "InitialPurchase",
    "StarRatingM255", "Medalist_RatingNumber", "SustainabilityRating",
    "ReturnM0", "ReturnM12", "ReturnM36", "ReturnM60", "ReturnM120",
    "AlphaM36", "BetaM36", "SharpeM36", "SortinoM36", "StandardDeviationM36",
    "MaxDrawdownM36", "InformationRatioM36", "R2M36",
    "AlphaM60", "BetaM60", "SharpeM60", "SortinoM60", "StandardDeviationM60", "MaxDrawdownM60",
    "AlphaM120", "BetaM120", "SharpeM120", "SortinoM120", "StandardDeviationM120", "MaxDrawdownM120",
    "PERatio", "PBRatio", "AverageMarketCapital",
    "AverageCreditQualityCode", "ModifiedDuration", "EffectiveMaturity",
]


def _num(v):
    try:
        return round(float(v), 4)
    except Exception:
        return None


def _row_for_isin(rows: list, isin: str) -> dict | None:
    """Devuelve la fila cuyo `Isin` == isin pedido. El screener `term=` es búsqueda
    FUZZY: ante un ISIN no indexado devuelve el 'mejor match' (OTRO fondo). Sin esta
    validación escribiríamos rating/riesgo/categoría de un fondo ajeno. Si ninguna
    fila matchea exactamente, None (mejor sin dato que con dato equivocado)."""
    isin = (isin or "").upper().strip()
    for r in (rows or []):
        if (r.get("Isin") or "").upper().strip() == isin:
            return r
    return None


def _risk_block(row: dict, suf: str) -> dict:
    d = {
        "alpha": _num(row.get(f"Alpha{suf}")),
        "beta": _num(row.get(f"Beta{suf}")),
        "sharpe": _num(row.get(f"Sharpe{suf}")),
        "sortino": _num(row.get(f"Sortino{suf}")),
        "volatilidad": _num(row.get(f"StandardDeviation{suf}")),
        "max_drawdown": _num(row.get(f"MaxDrawdown{suf}")),
        "information_ratio": _num(row.get(f"InformationRatio{suf}")),
        "r2": _num(row.get(f"R2{suf}")),
    }
    return {k: v for k, v in d.items() if v is not None}


def fetch_quant(isin: str) -> dict:
    """Dict de cuantitativos Morningstar (vacío si no se encuentra)."""
    isin = (isin or "").upper().strip()
    if not _ISIN.match(isin):
        return {}
    dp = "%7C".join(_DP)
    url = (f"{_SCR}?page=1&pageSize=5&outputType=json&version=1"
           f"&universeIds=FOALL%24%24ALL&securityDataPoints={dp}&term={isin}")
    try:
        from tools.http_retry import get_json
        rows = get_json(url, headers=_UA, timeout=20).get("rows") or []
    except Exception:
        return {}
    row = _row_for_isin(rows, isin)
    if not row:
        return {}
    med = row.get("Medalist_RatingNumber")
    out = {
        "_fuente": "morningstar",
        "secid": row.get("SecId"),
        "categoria_morningstar": row.get("CategoryName"),
        "rating_estrellas": row.get("StarRatingM255"),
        "medalist_rating": _MEDALIST.get(med) if med is not None else None,
        "sustainability_rating": row.get("SustainabilityRating"),
        "rentabilidades": {k: _num(row.get(v)) for k, v in {
            "ytd": "ReturnM0", "1a": "ReturnM12", "3a": "ReturnM36",
            "5a": "ReturnM60", "10a": "ReturnM120"}.items() if _num(row.get(v)) is not None},
        "riesgo": {p: b for p, b in {
            "3a": _risk_block(row, "M36"), "5a": _risk_block(row, "M60"),
            "10a": _risk_block(row, "M120")}.items() if b},
        "valoracion": {k: v for k, v in {
            "per": _num(row.get("PERatio")), "pbv": _num(row.get("PBRatio")),
            "cap_media_meur": _num(row.get("AverageMarketCapital"))}.items() if v is not None},
        "renta_fija": {k: v for k, v in {
            "calidad_crediticia": row.get("AverageCreditQualityCode"),
            "duracion_modificada": _num(row.get("ModifiedDuration")),
            "maturity_efectiva": _num(row.get("EffectiveMaturity"))}.items() if v},
        "comisiones": {k: v for k, v in {
            "ter_pct": _num(row.get("OngoingCharge")),
            "comision_gestion_pct": _num(row.get("ManagementFee")),
            "comision_entrada_max_pct": _num(row.get("MaxFrontEndLoad")),
            "inversion_minima": _num(row.get("InitialPurchase"))}.items() if v is not None},
        "patrimonio_fondo_meur": _num(row.get("FundTNAV")),
        "gestora": row.get("BrandingCompanyName"),
        "domicilio": row.get("Domicile"),
    }
    return {k: v for k, v in out.items() if v not in (None, {}, "")}


def fetch_category(isin: str) -> dict:
    """Categoría Morningstar (CategoryName) + estilo inferido (Value/Growth/Blend),
    barato (1 call). {} si no se encuentra."""
    isin = (isin or "").upper().strip()
    if not _ISIN.match(isin):
        return {}
    dp = "%7C".join(["Name", "Isin", "CategoryName", "EquityStyleBox"])
    url = (f"{_SCR}?page=1&pageSize=5&outputType=json&version=1"
           f"&universeIds=FOALL%24%24ALL&securityDataPoints={dp}&term={isin}")
    try:
        from tools.http_retry import get_json
        rows = get_json(url, headers=_UA, timeout=15).get("rows") or []
    except Exception:
        return {}
    row = _row_for_isin(rows, isin)
    if not row:
        return {}
    cat = (row.get("CategoryName") or "").strip()
    out = {}
    if cat:
        out["categoria_morningstar"] = cat
        cl = cat.lower()
        # Estilo solo para RV con vocabulario claro (no inventar en RF).
        if "value" in cl:
            out["estilo"] = "Value"
        elif "growth" in cl:
            out["estilo"] = "Growth"
        elif "blend" in cl:
            out["estilo"] = "Blend"
    return out


def fetch_identity(isin: str) -> dict:
    """Identidad AUTORITATIVA del fondo desde Morningstar (por ISIN, validado): nombre
    limpio + gestora (BrandingCompanyName). Fuente fiable para rellenar nombre/gestora
    cuando la extracción del pipeline falla o queda vacía. {} si no se encuentra."""
    isin = (isin or "").upper().strip()
    if not _ISIN.match(isin):
        return {}
    dp = "%7C".join(["Name", "Isin", "BrandingCompanyName"])
    url = (f"{_SCR}?page=1&pageSize=5&outputType=json&version=1"
           f"&universeIds=FOALL%24%24ALL&securityDataPoints={dp}&term={isin}")
    try:
        r = httpx.get(url, headers=_UA, timeout=15, follow_redirects=True)
        r.raise_for_status()
        rows = r.json().get("rows") or []
    except Exception:
        return {}
    row = _row_for_isin(rows, isin)   # valida que la fila es del ISIN pedido
    if not row:
        return {}
    out = {}
    nm = (row.get("Name") or "").strip()
    g = (row.get("BrandingCompanyName") or "").strip()
    if nm:
        out["name"] = nm
    if g:
        out["gestora"] = g
    return out


def fetch_rating(isin: str) -> dict:
    """Rating Morningstar ligero (1 call): {estrellas 1-5, medalist Gold/Silver/Bronze}."""
    isin = (isin or "").upper().strip()
    if not _ISIN.match(isin):
        return {}
    dp = "%7C".join(["Name", "Isin", "StarRatingM255", "Medalist_RatingNumber"])
    url = (f"{_SCR}?page=1&pageSize=5&outputType=json&version=1"
           f"&universeIds=FOALL%24%24ALL&securityDataPoints={dp}&term={isin}")
    try:
        from tools.http_retry import get_json
        rows = get_json(url, headers=_UA, timeout=15).get("rows") or []
    except Exception:
        return {}
    row = _row_for_isin(rows, isin)
    if not row:
        return {}
    med = row.get("Medalist_RatingNumber")
    out = {}
    if row.get("StarRatingM255") is not None:
        out["estrellas"] = row["StarRatingM255"]
    if med is not None:
        out["medalist"] = _MEDALIST.get(med)
    return out


def fetch_nav(secid: str, start: str, end: str, freq: str = "monthly") -> list:
    """Serie NAV [[epoch_ms, valor], ...] del SecId (para gráfico/captura)."""
    if not secid:
        return []
    url = (f"{_TS}?currencyId=EUR&idtype=Morningstar&frequency={freq}"
           f"&id={secid}&startDate={start}&endDate={end}&outputType=COMPACTJSON")
    try:
        from tools.http_retry import get_json
        return get_json(url, headers=_UA, timeout=20) or []
    except Exception:
        return []


def main():
    if len(sys.argv) < 2:
        print("uso: python -m tools.morningstar_quant <ISIN>")
        return
    import json
    print(json.dumps(fetch_quant(sys.argv[1]), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
