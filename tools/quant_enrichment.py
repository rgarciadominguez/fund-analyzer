"""Enriquecimiento cuantitativo estilo-Morningstar desde fuentes automatizables.

Morningstar bloquea el acceso automatizado (CloudFront 403 a httpx y a Chrome
headless/headed). Así que NO scrapeamos Morningstar. En su lugar:

  - **Yahoo Finance** (ISIN→símbolo→crumb→quoteSummary): style box + sectores.
  - **Cálculo propio** de capture ratios (upside/downside) desde la serie NAV
    del fondo vs un benchmark — esto es 100% nuestro, no depende de nadie.

Lo que NO se puede obtener (propietario Morningstar, sin fuente abierta):
  - Perfil Factor (Momentum/Calidad/Liquidez scores), Active Share.
  - P/E, P/B, P/S de fondos europeos (Yahoo solo los trae fiables en US).

Uso:
    from tools.quant_enrichment import enrich_fund
    data = enrich_fund("ES0159259011", benchmark_symbol="^STOXX")
    # → {"yahoo_symbol", "style_box", "sectores", "capture_ratios", ...}

Defensivo: cualquier fallo de red/cobertura → el campo correspondiente queda None.
"""
from __future__ import annotations

import re
from typing import Optional

import httpx

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# Benchmark por defecto según geografía → (símbolo ETF, etiqueta). Usamos ETFs
# (no índices) porque el ETF tiene a la vez serie NAV (capture ratios) Y holdings
# con ratios de valoración (P/E, P/B…) para la columna "Índice". Proxies
# regionales razonables cuando no se pasa un benchmark explícito.
DEFAULT_BENCHMARKS = {
    "europe":   ("EXSA.DE", "STOXX Europe 600"),
    "eurozone": ("EXSA.DE", "STOXX Europe 600"),
    "usa":      ("IVV",     "S&P 500"),
    "global":   ("IWDA.L",  "MSCI World"),
    "spain":    ("EXSA.DE", "STOXX Europe 600"),
    "emerging": ("IEMG",    "MSCI Emerging Markets"),
}


# ════════════════════════════════════════════════════════════════════
# Cliente Yahoo (cookie + crumb)
# ════════════════════════════════════════════════════════════════════

def _yahoo_client() -> tuple[Optional[httpx.Client], Optional[str]]:
    """Devuelve (client con cookies, crumb) o (None, None) si falla."""
    try:
        c = httpx.Client(headers={"User-Agent": _UA}, timeout=20, follow_redirects=True)
        try:
            c.get("https://fc.yahoo.com")
        except Exception:
            pass
        c.get("https://finance.yahoo.com")
        crumb = c.get("https://query2.finance.yahoo.com/v1/test/getcrumb").text.strip()
        if not crumb or len(crumb) > 40 or "<" in crumb:
            return c, None
        return c, crumb
    except Exception:
        return None, None


def resolve_yahoo_symbol(isin: str) -> Optional[str]:
    """ISIN → símbolo Yahoo del fondo (p.ej. ES0159259011 → 0P0001572X.F)."""
    try:
        r = httpx.get(
            f"https://query2.finance.yahoo.com/v1/finance/search?q={isin}&quotesCount=5",
            headers={"User-Agent": _UA}, timeout=15,
        )
        for q in r.json().get("quotes", []):
            sym = q.get("symbol")
            if sym:
                return sym
    except Exception:
        pass
    return None


# ════════════════════════════════════════════════════════════════════
# Style box + sectores (Yahoo quoteSummary topHoldings/fundProfile)
# ════════════════════════════════════════════════════════════════════

# Style box equity 3×3: filas = tamaño (Large/Mid/Small), columnas = estilo
# (Value/Blend/Growth). Box 1..9 row-major desde Large-Value.
_STYLE_BOX = {
    1: ("Large", "Value"), 2: ("Large", "Blend"), 3: ("Large", "Growth"),
    4: ("Mid", "Value"),   5: ("Mid", "Blend"),   6: ("Mid", "Growth"),
    7: ("Small", "Value"), 8: ("Small", "Blend"), 9: ("Small", "Growth"),
}


def decode_style_box(style_box_url: Optional[str]) -> Optional[dict]:
    """Decodifica el gif de style box de Yahoo a {box, size, style}.

    Yahoo usa imágenes tipo `.../fi/{N}_0style{size}eq{M}.gif` donde el dígito
    M (tras 'eq') es la casilla activa 1..9 del style box de Morningstar.
    """
    if not style_box_url:
        return None
    m = re.search(r"eq(\d)\b", style_box_url)
    if not m:
        return None
    box = int(m.group(1))
    if box not in _STYLE_BOX:
        return None
    size, style = _STYLE_BOX[box]
    return {"box": box, "size": size, "style": style, "_source": "yahoo"}


def get_style_and_sectors(symbol: str, client: httpx.Client, crumb: str) -> dict:
    """{style_box, sectores} desde quoteSummary. Campos None si no hay cobertura."""
    out: dict = {"style_box": None, "sectores": None}
    try:
        u = (f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
             f"?modules=topHoldings,fundProfile&crumb={crumb}")
        r = client.get(u)
        if r.status_code != 200:
            return out
        res = r.json()["quoteSummary"]["result"][0]
        th = res.get("topHoldings", {}) or {}
        fp = res.get("fundProfile", {}) or {}
        out["style_box"] = decode_style_box(th.get("styleBoxUrl") or fp.get("styleBoxUrl"))
        secs = []
        for s in th.get("sectorWeightings", []) or []:
            for k, v in s.items():
                raw = v.get("raw") if isinstance(v, dict) else v
                if raw:
                    secs.append({"sector": k, "peso_pct": round(float(raw) * 100, 2)})
        out["sectores"] = sorted(secs, key=lambda x: -x["peso_pct"]) or None
    except Exception:
        pass
    return out


# ════════════════════════════════════════════════════════════════════
# Ratios de valoración (Yahoo equityHoldings) — fondo e índice
# ════════════════════════════════════════════════════════════════════

# Yahoo devuelve P/E, P/B, P/S, P/CF de la cartera como YIELD (recíproco):
# priceToEarnings 0.0555 ≡ earnings yield → P/E = 1/0.0555 ≈ 18. Validado contra
# benchmarks conocidos (STOXX600 P/E~18, P/B~2.3). Convertimos a ratio = 1/yield.
def _ratio_from_yield(v) -> Optional[float]:
    try:
        v = float(v)
        if v <= 0:
            return None
        return round(1.0 / v, 2)
    except Exception:
        return None


def get_valuation_ratios(symbol: str, client: httpx.Client, crumb: str) -> Optional[dict]:
    """{per, pvc, pventas, pcf} de la cartera de un fondo/ETF. None si no hay."""
    try:
        u = (f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
             f"?modules=topHoldings&crumb={crumb}")
        r = client.get(u)
        if r.status_code != 200:
            return None
        eqh = (r.json()["quoteSummary"]["result"][0].get("topHoldings", {}) or {}
               ).get("equityHoldings", {}) or {}

        def _g(k):
            v = eqh.get(k, {})
            return _ratio_from_yield(v.get("raw") if isinstance(v, dict) else v)

        out = {
            "per": _g("priceToEarnings"),
            "pvc": _g("priceToBook"),
            "pventas": _g("priceToSales"),
            "pcf": _g("priceToCashflow"),
        }
        return out if any(v is not None for v in out.values()) else None
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════
# Riesgo / rentabilidad por holding-period (Yahoo fundPerformance)
# ════════════════════════════════════════════════════════════════════

_PERIOD_ORDER = {"3y": 0, "5y": 1, "10y": 2}


def get_risk_return(symbol: str, client: httpx.Client, crumb: str) -> Optional[dict]:
    """{riesgo: [{periodo, volatilidad, sharpe, beta, alpha, r2, treynor}],
        retornos: {ytd, 1a, 3a, 5a, 10a}} desde fundPerformance. None si no hay.

    Las métricas de riesgo vienen separadas por holding-period (3/5/10 años),
    que es justo lo que se quiere mostrar. Retornos trailing en % anualizado.
    """
    try:
        u = (f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
             f"?modules=fundPerformance&crumb={crumb}")
        r = client.get(u)
        if r.status_code != 200:
            return None
        fp = r.json()["quoteSummary"]["result"][0].get("fundPerformance", {}) or {}

        def _raw(d, k):
            v = (d or {}).get(k, {})
            return v.get("raw") if isinstance(v, dict) else v

        riesgo = []
        for rs in (fp.get("riskOverviewStatistics", {}) or {}).get("riskStatistics", []) or []:
            riesgo.append({
                "periodo": rs.get("year"),
                "volatilidad": _raw(rs, "stdDev"), "sharpe": _raw(rs, "sharpeRatio"),
                "beta": _raw(rs, "beta"), "alpha": _raw(rs, "alpha"),
                "r2": _raw(rs, "rSquared"), "treynor": _raw(rs, "treynorRatio"),
            })
        riesgo.sort(key=lambda x: _PERIOD_ORDER.get(x.get("periodo"), 9))

        retornos = {}
        tr = fp.get("trailingReturns", {}) or {}
        for out_k, in_k in [("ytd", "ytd"), ("1a", "oneYear"), ("3a", "threeYear"),
                            ("5a", "fiveYear"), ("10a", "tenYear")]:
            raw = _raw(tr, in_k)
            if raw is not None:
                retornos[out_k] = round(float(raw) * 100, 2)

        out = {"riesgo": riesgo, "retornos": retornos}
        return out if (riesgo or retornos) else None
    except Exception:
        return None


# ════════════════════════════════════════════════════════════════════
# Capture ratios (cálculo propio desde series NAV)
# ════════════════════════════════════════════════════════════════════

def get_nav_history(symbol: str, rng: str = "5y", interval: str = "1mo") -> Optional[list[tuple[int, float]]]:
    """Serie [(timestamp, close)] desde el chart de Yahoo. None si falla."""
    try:
        u = (f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
             f"?range={rng}&interval={interval}")
        r = httpx.get(u, headers={"User-Agent": _UA}, timeout=20)
        res = r.json()["chart"]["result"][0]
        ts = res["timestamp"]
        quote = res["indicators"]["quote"][0]
        closes = quote.get("close") or []
        adj = (res["indicators"].get("adjclose") or [{}])[0].get("adjclose")
        series = adj if adj else closes
        out = [(t, c) for t, c in zip(ts, series) if c is not None]
        return out or None
    except Exception:
        return None


def _monthly_returns(nav: list[tuple[int, float]]) -> dict[str, float]:
    """{YYYY-MM: retorno_mensual} a partir de la serie NAV."""
    out: dict[str, float] = {}
    import datetime as _dt
    prev = None
    for ts, close in nav:
        key = _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime("%Y-%m")
        if prev is not None and prev[1] and close:
            out[key] = (close / prev[1]) - 1.0
        prev = (ts, close)
    return out


def compute_capture_ratios(fund_nav: list, bench_nav: list) -> Optional[dict]:
    """Upside/Downside/Total capture (def. Morningstar) desde series NAV.

    Up-market = meses con retorno del benchmark > 0. Capture = retorno compuesto
    del fondo / retorno compuesto del benchmark en esos meses × 100.
    Upside >100 y Downside <100 = perfil deseable.
    """
    if not fund_nav or not bench_nav:
        return None
    fr = _monthly_returns(fund_nav)
    br = _monthly_returns(bench_nav)
    common = sorted(set(fr) & set(br))
    if len(common) < 12:
        return None

    def _compound(months):
        p = 1.0
        for m in months:
            p *= (1.0 + fr[m])
        f = p - 1.0
        p = 1.0
        for m in months:
            p *= (1.0 + br[m])
        b = p - 1.0
        return f, b

    up = [m for m in common if br[m] > 0]
    down = [m for m in common if br[m] < 0]
    res: dict = {"n_meses": len(common), "n_up": len(up), "n_down": len(down),
                 "_source": "computed_nav"}

    # Rendimiento MEDIO mensual por régimen (Fondo vs Índice) — para comparar
    # visualmente el comportamiento relativo en subidas y en bajadas.
    def _avg(months, series):
        return round(sum(series[m] for m in months) / len(months) * 100, 2) if months else None
    res["rendimiento_regimen"] = {
        "subidas": {"fondo": _avg(up, fr), "indice": _avg(up, br)},
        "caidas": {"fondo": _avg(down, fr), "indice": _avg(down, br)},
    }

    if up:
        f, b = _compound(up)
        res["upside_pct"] = round((f / b) * 100, 1) if b else None
        # Exceso en subidas (pp): retorno compuesto fondo − índice en meses al alza.
        # Positivo = sube MÁS que el índice.
        res["exceso_subidas_pp"] = round((f - b) * 100, 1)
    if down:
        f, b = _compound(down)
        res["downside_pct"] = round((f / b) * 100, 1) if b else None
        # Exceso en caídas (pp): fondo − índice en meses a la baja (ambos negativos).
        # Positivo = cae MENOS que el índice (mejor).
        res["exceso_caidas_pp"] = round((f - b) * 100, 1)
    # Total capture = up/down ratio (>1 mejor)
    if res.get("upside_pct") and res.get("downside_pct"):
        res["capture_ratio"] = round(res["upside_pct"] / res["downside_pct"], 2)
    # Veredicto explícito: ¿bate al índice en subidas, en caídas, ambos, ninguno?
    msub = res.get("exceso_subidas_pp")
    mcai = res.get("exceso_caidas_pp")
    mejor_sub = msub is not None and msub > 0
    mejor_cai = mcai is not None and mcai > 0
    if mejor_sub and mejor_cai:
        res["perfil"] = "Bate al índice en subidas Y en caídas"
        res["perfil_tag"] = "ambos"
    elif mejor_cai and not mejor_sub:
        res["perfil"] = "Defensivo: cae menos que el índice; capta algo menos en subidas"
        res["perfil_tag"] = "defensivo"
    elif mejor_sub and not mejor_cai:
        res["perfil"] = "Ofensivo: sube más que el índice, pero cae igual o más"
        res["perfil_tag"] = "ofensivo"
    elif msub is not None and mcai is not None:
        res["perfil"] = "No bate al índice ni en subidas ni en caídas"
        res["perfil_tag"] = "ninguno"
    return res


# ════════════════════════════════════════════════════════════════════
# API pública
# ════════════════════════════════════════════════════════════════════

def enrich_fund(isin: str, benchmark_symbol: Optional[str] = None,
                geo: Optional[str] = None) -> dict:
    """Enriquecimiento cuanti de un fondo. Campos None donde no hay cobertura."""
    out: dict = {
        "isin": isin, "yahoo_symbol": None, "style_box": None,
        "sectores": None, "valoracion": None, "riesgo": None, "retornos": None,
        "capture_ratios": None, "benchmark_symbol": None, "benchmark_label": None,
    }
    symbol = resolve_yahoo_symbol(isin)
    out["yahoo_symbol"] = symbol
    if not symbol:
        return out

    # Benchmark (ETF) para valoración-índice y capture ratios
    geo_bench = DEFAULT_BENCHMARKS.get((geo or "").lower())
    if benchmark_symbol:
        bench, bench_label = benchmark_symbol, benchmark_symbol
    elif geo_bench:
        bench, bench_label = geo_bench
    else:
        bench, bench_label = None, None
    out["benchmark_symbol"] = bench
    out["benchmark_label"] = bench_label

    client, crumb = _yahoo_client()
    if client and crumb:
        out.update(get_style_and_sectors(symbol, client, crumb))
        # Sectores del benchmark (ETF) → comparativa de pesos fondo vs índice
        if bench:
            out["sectores_benchmark"] = get_style_and_sectors(bench, client, crumb).get("sectores")
        # Valoración: fondo vs índice (mismo recíproco→ratio)
        fund_val = get_valuation_ratios(symbol, client, crumb)
        idx_val = get_valuation_ratios(bench, client, crumb) if bench else None
        if fund_val or idx_val:
            out["valoracion"] = {"fondo": fund_val, "indice": idx_val,
                                 "indice_label": bench_label}
        # Riesgo/rentabilidad por holding-period (3/5/10 años)
        rr = get_risk_return(symbol, client, crumb)
        if rr:
            out["riesgo"] = rr.get("riesgo")
            out["retornos"] = rr.get("retornos")
    if client:
        client.close()

    # Capture ratios (serie NAV fondo vs benchmark)
    if bench:
        out["capture_ratios"] = compute_capture_ratios(
            get_nav_history(symbol), get_nav_history(bench))

    # Morningstar (autoritativo): medalist, riesgo 3/5/10A, valoración, RF, comisiones.
    # Reemplaza/complementa a Yahoo donde es mejor.
    try:
        from tools.morningstar_quant import fetch_quant as _ms_quant
        ms = _ms_quant(isin)
        if ms:
            out["morningstar"] = ms
    except Exception:
        pass
    return out


def resolve_geo(isin: str, texto: str = "") -> str:
    """Geografía para elegir benchmark, desde ISIN + texto (nombre/categoría/benchmark)."""
    t = (texto or "").lower()
    if any(k in t for k in ["españa", "espana", "spain", "ibex", "spanish"]):
        return "spain"
    if any(k in t for k in ["emerg", "emerging", "em ", "mercados emergentes"]):
        return "emerging"
    if any(k in t for k in ["eurozon", "euro zone", "zona euro", "euro stoxx", "emu"]):
        return "eurozone"
    if any(k in t for k in ["eeuu", "ee.uu", "estados unidos", "u.s", "usa", "us ",
                            "s&p", "sp500", "s&p 500", "norteam", "american"]):
        return "usa"
    if any(k in t for k in ["europ", "europe"]):
        return "europe"
    if any(k in t for k in ["global", "world", "mundial", "internacional", "worldwide"]):
        return "global"
    # Fallback por prefijo ISIN del fondo (domicilio, aproximación grosera)
    pref = (isin or "")[:2].upper()
    if pref == "ES":
        return "spain"
    return "global"


def enrich_and_save(isin: str, benchmark_symbol: Optional[str] = None,
                    log=None) -> dict:
    """Enriquece un fondo y escribe `output.json["analisis_cuantitativo"]`.

    Pura Python (Yahoo + cálculo), sin LLM — apta para el pipeline. Idempotente.
    Resuelve la geografía desde nombre/categoría/benchmark del propio output.
    """
    import json
    from pathlib import Path
    root = Path(__file__).parent.parent
    out_path = root / "data" / "funds" / isin.upper() / "output.json"
    if not out_path.exists():
        return {"ok": False, "reason": "no_output"}
    data = json.loads(out_path.read_text(encoding="utf-8"))

    # Texto para inferir geografía (nombre + clasificación + benchmark)
    kpis = data.get("kpis", {}) or {}
    texto = " ".join(str(x) for x in [
        data.get("nombre", ""), kpis.get("clasificacion", ""),
        kpis.get("benchmark", ""), data.get("gestora", ""),
    ])
    geo = resolve_geo(isin, texto)
    enr = enrich_fund(isin, benchmark_symbol=benchmark_symbol, geo=geo)
    enr["geo_inferida"] = geo
    from datetime import datetime, timezone
    enr["generado"] = datetime.now(timezone.utc).isoformat()

    data["analisis_cuantitativo"] = enr
    tmp = out_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(out_path)
    if log:
        cr = (enr.get("capture_ratios") or {})
        log("QUANT", "OK",
            f"{isin}: style={enr.get('style_box')} | capture up/down="
            f"{cr.get('upside_pct')}/{cr.get('downside_pct')} | sectores="
            f"{len(enr.get('sectores') or [])}")
    return {"ok": True, "enrichment": enr}


__all__ = ["enrich_fund", "enrich_and_save", "resolve_yahoo_symbol",
           "resolve_geo", "decode_style_box", "compute_capture_ratios",
           "get_nav_history", "DEFAULT_BENCHMARKS"]
