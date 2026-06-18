"""Localiza la web OFICIAL del fondo/gestor por ISIN o nombre (búsqueda dirigida).

Feedback Rafa (2026-06-18): no basta con que la web la traiga el KID; buscando por
ISIN o nombre del fondo el sistema debe ACCEDER fácil a la web oficial del gestor/
fondo. Esto hace una búsqueda Serper dirigida y filtra agregadores/brokers para
quedarse con el dominio oficial (la web de la gestora o la plataforma del fondo).

Devuelve {'domain', 'url', 'score'} o {} si no encuentra nada fiable.
"""
from __future__ import annotations

import re
import unicodedata
from urllib.parse import urlparse

# Agregadores / brokers / data-providers / social → NO son la web oficial del gestor.
_AGGREGATORS = {
    "morningstar", "ft.com", "bloomberg", "justetf", "trustnet", "citywire",
    "finect", "swissfunddata", "fundinfo", "reuters", "marketscreener", "boursorama",
    "quefondos", "wsj", "investing.com", "yahoo", "google", "wikipedia", "linkedin",
    "facebook", "twitter", "x.com", "youtube", "instagram", "etoro", "hl.co.uk",
    "fidelity", "interactivebrokers", " statista", "fool.", "seekingalpha",
    "rankia", "expansion.com", "eleconomista", "bolsamania", "morningstar.",
    "fundsquare", "openfunds", "tradingview", "barrons", "ig.com", "degiro",
    "myinvestor", "renta4", "selfbank", "indexacapital", "accipartners",
    "kbassocies", "fnz", "clearstream", "sixgroup", "lipperweb", "refinitiv",
    "annualreports", "docfinder", "fundslibrary", "kneip", "edgar", "sec.gov",
}


def _strip(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def _domain(url: str) -> str:
    try:
        net = urlparse(url if "://" in url else "https://" + url).netloc.lower()
        return net[4:] if net.startswith("www.") else net
    except Exception:
        return ""


def _is_aggregator(dom: str) -> bool:
    return any(a in dom for a in _AGGREGATORS)


def _tokens(s: str) -> set:
    return {t for t in re.split(r"[^a-z0-9]+", _strip(s)) if len(t) > 3}


def _score(dom: str, name: str, gestora: str, title: str, snippet: str) -> float:
    """Cuánto 'huele' a web oficial del gestor/fondo de este fondo."""
    dom_toks = _tokens(dom.split(".")[0])  # 'magallanesvalue' → {magallanes...}
    dom_str = _strip(dom)
    score = 0.0
    # Solapamiento del dominio con tokens de gestora (señal fuerte) y del nombre.
    g_toks = _tokens(gestora)
    n_toks = _tokens(name)
    if g_toks and any(t in dom_str for t in g_toks):
        score += 4
    if n_toks and any(t in dom_str for t in n_toks):
        score += 2
    # El title/snippet menciona la gestora o el fondo (confirma relevancia).
    blob = _strip(title + " " + snippet)
    if g_toks and any(t in blob for t in g_toks):
        score += 1
    if n_toks and sum(1 for t in n_toks if t in blob) >= 2:
        score += 1
    # Penaliza dominios genéricos muy cortos o numéricos.
    if len(dom_toks) == 0:
        score -= 1
    return score


async def find_official_site(isin: str, name: str = "", gestora: str = "",
                             log=None, max_queries: int = 2) -> dict:
    """Busca la web oficial del gestor/fondo. Respeta el guardrail de coste Serper."""
    isin = (isin or "").upper().strip()
    # Auto-suficiente por ISIN: si no llega nombre/gestora, los saca de Morningstar
    # (autoritativo). Así "buscar por ISIN" basta para acceder a la web oficial.
    if not (name and gestora):
        try:
            from tools.morningstar_quant import fetch_identity
            ident = fetch_identity(isin) or {}
            name = name or ident.get("name", "")
            gestora = gestora or ident.get("gestora", "")
        except Exception:
            pass
    try:
        from tools.google_search import SearchEngine
    except Exception:
        return {}
    eng = SearchEngine()
    queries: list[str] = []
    if gestora and name:
        queries.append(f"{gestora} {name}")
    elif name:
        queries.append(f"{name} fund")
    if gestora:
        queries.append(f"{gestora} official website")
    if not queries:
        queries.append(isin)
    queries = queries[:max_queries]

    candidates: dict[str, tuple] = {}   # dom -> (score, url)
    for q in queries:
        try:
            results = await eng.search(q, num=8, agent="fund_site_finder")
        except Exception as e:
            if log:
                log(f"[SITE_FINDER] search '{q}' falló: {str(e)[:60]}")
            continue
        for r in (results or []):
            url = r.get("link") or r.get("url") or ""
            dom = _domain(url)
            if not dom or _is_aggregator(dom):
                continue
            sc = _score(dom, name, gestora, r.get("title", ""), r.get("snippet", ""))
            if sc <= 0:
                continue
            if dom not in candidates or sc > candidates[dom][0]:
                candidates[dom] = (sc, url)
        if candidates:
            break  # ya hay candidatos → no gastar más cuota
    if not candidates:
        return {}
    best_dom = max(candidates, key=lambda d: candidates[d][0])
    sc, url = candidates[best_dom]
    res = {"domain": best_dom, "url": f"https://www.{best_dom}", "score": sc}
    # Cachear en gestoras_registry → los fondos HERMANOS de esta gestora reusan la
    # web con 0 Serper (Robeco ×4 clases comparten web, etc.).
    _cache_to_registry(gestora, best_dom, res["url"])
    if log:
        log(f"[SITE_FINDER] {isin}: web oficial = {best_dom} (score {sc})")
    return res


def _cache_to_registry(gestora: str, domain: str, url: str) -> None:
    """Guarda la web encontrada en gestoras_registry.json (no pisa una web ya puesta).
    Así un fondo hermano de la misma gestora la reusa por registro (0 Serper)."""
    import sys, json
    from pathlib import Path
    if "pytest" in sys.modules:   # no contaminar el registro real desde tests
        return
    gestora = (gestora or "").strip()
    if not gestora or len(gestora) < 4 or not domain:
        return
    path = Path(__file__).resolve().parent.parent / "data" / "gestoras_registry.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except Exception:
        return
    gestoras = data.setdefault("gestoras", {})
    gl = gestora.lower()
    key = next((k for k in gestoras if k.lower() == gl), None)
    if key:
        if gestoras[key].get("web"):
            return  # ya tiene web → no pisar
        gestoras[key]["web"] = url
        gestoras[key].setdefault("_web_source", "fund_site_finder")
    else:
        gestoras[gestora] = {"web": url, "_web_source": "fund_site_finder",
                             "letters_pages": [], "successful_pdf_urls": []}
    try:
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(path)
    except Exception:
        pass


def find_official_site_sync(isin: str, name: str = "", gestora: str = "", log=None) -> dict:
    import asyncio
    try:
        return asyncio.run(find_official_site(isin, name, gestora, log=log))
    except RuntimeError:
        # ya hay loop (raro en CLI) → nuevo loop
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(find_official_site(isin, name, gestora, log=log))
        finally:
            loop.close()


def main():
    import sys, json
    if len(sys.argv) < 2:
        print("uso: python -m tools.fund_site_finder <ISIN> [nombre] [gestora]")
        return
    isin = sys.argv[1].upper()
    name = sys.argv[2] if len(sys.argv) > 2 else ""
    gestora = sys.argv[3] if len(sys.argv) > 3 else ""
    print(json.dumps(find_official_site_sync(isin, name, gestora, log=print),
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
