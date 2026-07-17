"""
benchmark_classifier.py — Asigna `benchmark` (categorización Horizonte) a cada fondo.

QUÉ ES ESTE CAMPO (importante, se malinterpretó una vez):
    `benchmark` NO es "el índice de referencia del folleto". Es la CATEGORIZACIÓN de
    Horizonte: su vocabulario (Euribor, SP500, MSCI World, Renta Fija Medio Plazo,
    Cartera Permanente, 40% RV y 60% RF...). Se asigna a cada fondo a partir de SU
    ANÁLISIS. Si ningún valor del vocabulario encaja, se acuña uno nuevo con wording
    similar al de Horizonte (no una etiqueta ajena).

FUENTES DE LA FICHA (por ISIN, todo lo que sepamos del fondo):
    - nombre / gestora            (Supabase funds + fund_taxonomy)
    - tipo_activo / geografia / estilo   (fund_taxonomy)
    - categoría Morningstar       (screener público — señal fuerte y barata)
    - resumen + estrategia        (analyst_synthesis, donde haya análisis)

VALIDACIÓN (lo que hace fiable esto):
    Horizonte ya tiene 263 benchmarks puestos a mano. `--validate` clasifica a ciegas
    esos mismos ISIN y mide el acierto contra los suyos. El % que salga es el que se
    reporta: no se promete precisión, se mide.

CLI:
    python -m tools.benchmark_classifier --fichas          # construye/refresca caché de fichas
    python -m tools.benchmark_classifier --validate        # mide acierto vs Horizonte
    python -m tools.benchmark_classifier --run [--apply]   # clasifica todo (--apply escribe Supabase)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

FICHAS = ROOT / "data" / ".benchmark_fichas.json"
RESULT = ROOT / "data" / "benchmark_asignado.json"
CSV_HORFIN = Path(r"C:\Users\RafaelGarcía\horizonte-datos\Activos y Bancos (1).csv")

MODEL = "claude-sonnet-4-5"


# ---------------------------------------------------------------- fichas
def _tax() -> dict:
    p = ROOT / "data" / "fund_taxonomy.json"
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8")).get("funds", {})


def _analysis_snippet(isin: str, limit: int = 700) -> str:
    """Resumen + estrategia del análisis, si existe. Vacío si no hay."""
    p = ROOT / "data" / "funds" / isin / "output.json"
    if not p.exists():
        return ""
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return ""
    try:
        from tools.output_accessor import get_resumen_texto, get_section_estrategia
        txt = (get_resumen_texto(d) or "")
        est = get_section_estrategia(d) or {}
        txt += " " + (est.get("texto") or "" if isinstance(est, dict) else "")
    except Exception:
        txt = ""
    return re.sub(r"\s+", " ", txt).strip()[:limit]


def build_fichas(refresh: bool = False) -> dict:
    """{isin: ficha} para todos los fondos de Supabase. Cachea en disco."""
    cache = {}
    if FICHAS.exists() and not refresh:
        cache = json.loads(FICHAS.read_text(encoding="utf-8"))

    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    from tools.morningstar_quant import fetch_category

    rows = get_client().table("funds").select("isin,nombre_clase").execute().data
    tax = _tax()
    todo = [r for r in rows if r["isin"] not in cache]
    print(f"fondos: {len(rows)} | en caché: {len(rows) - len(todo)} | a construir: {len(todo)}")

    for i, r in enumerate(todo, 1):
        isin = r["isin"]
        t = tax.get(isin, {})
        try:
            cat = fetch_category(isin) or {}
        except Exception:
            cat = {}
        cache[isin] = {
            "isin": isin,
            "nombre": t.get("nombre") or r.get("nombre_clase") or "",
            "gestora": t.get("issuer") or "",
            "tipo_activo": t.get("tipo_activo") or "",
            "geografia": t.get("geografia") or "",
            "estilo": t.get("estilo") or "",
            "categoria_morningstar": cat.get("categoria_morningstar") or cat.get("categoria") or "",
            "analisis": _analysis_snippet(isin),
        }
        if i % 25 == 0:
            print(f"  {i}/{len(todo)}")
            FICHAS.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

    FICHAS.parent.mkdir(parents=True, exist_ok=True)
    FICHAS.write_text(json.dumps(cache, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"fichas: {len(cache)} -> {FICHAS}")
    return cache


# ---------------------------------------------------------------- clasificación
_SYSTEM = """Eres un analista de fondos. Clasificas fondos en la CATEGORIZACIÓN de Horizonte Financiero.

Este campo se llama "benchmark" pero NO es el índice del folleto: es la categoría con la que
Horizonte agrupa sus activos. Su vocabulario mezcla índices (SP500, MSCI World) con categorías
(Renta Fija Medio Plazo, Cartera Permanente, 40% RV y 60% RF). Ambas cosas son válidas aquí.

REGLAS:
1. Usa SIEMPRE un valor del VOCABULARIO si alguno describe bien el fondo.
2. Si ninguno encaja, acuña uno NUEVO con wording del MISMO ESTILO que el vocabulario
   (mismo idioma, misma forma: "MSCI China", "Renta Fija Emergente"). Marca nuevo=true.
3. Un fondo de RV se clasifica por su índice/geografía de referencia.
4. Si el fondo no encaja en NADA con la información dada, devuelve benchmark=null.
   Prefiero un hueco a un valor inventado. NO adivines por el nombre del fondo.
5. confianza: "alta" solo si la ficha lo dice claramente; "media" si lo deduces de la
   categoría Morningstar; "baja" si es un salto.

CONVENCIONES DE HORIZONTE (deducidas de sus 263 clasificaciones a mano — respétalas
aunque te parezcan poco literales; SON su criterio, no una aproximación):

- "Euribor" NO es solo para floating: es su cubo de MONETARIOS y RF ULTRA-CORTO/tesorería.
  Van aquí: money market, "Ultra Short-Term Bond" de Morningstar, floating rate notes,
  trésorerie, "6 meses", "corto plazo" conservador de banca.
  Ejemplos suyos: BBVA Rentabilidad Ahorro Corto Plazo, Groupama Ultra Short Term Bond,
  Groupama Trésorerie, Dunas Valor Prudente, Renta 4 Renta Fija 6 Meses, DWS Floating
  Rate Notes, AXA Trésor Court Terme, Caixabank Master Renta Fija Corto Plazo.

- "Renta Fija Corto Plazo" NO significa duración corta: es su cubo de RF de RETORNO
  ABSOLUTO / long-short credit / credit opportunities / enhanced yield.
  Ejemplos suyos: Candriam Long Short Credit, BlueBay Investment Grade Absolute Return,
  Aegon Absolute Return Bond, Muzinich Enhancedyield Short-Term, Groupama Alpha Fixed
  Income, BGF Fixed Income Global Opportunities.

- "Renta Fija Medio Plazo" es su cubo POR DEFECTO de RF direccional: crédito corporativo,
  total return, bond opportunities, deuda gubernamental, global bond, ligados a inflación.
  Ejemplos suyos: BBVA Bonos Corporativos Largo Plazo (sí, "Largo" va a Medio),
  Flossbach Bond Opportunities, Dodge & Cox Global Bond, AXA Euro Credit Total Return,
  DNCA Credit Conviction, Vanguard Eurozone Inflation-Linked, Vanguard Japan Govt Bond.

- "Cartera Permanente" es multiactivo DEFENSIVO / preservación de capital (mezcla RV, oro,
  bonos, liquidez). Ejemplos suyos: MyInvestor Cartera Permanente, Cartesio X, Ruffer
  Total Return, Trojan Fund, DWS Concept Kaldemorgen, M&G (Lux) Dynamic Allocation.

- MIXTOS: casi NUNCA uses "Mixto Flexible" (ellos solo lo usan 1 vez). Estima el reparto
  RV/RF real del fondo y elige el cubo más cercano:
    "75% RV y 25% RF"  mixtos flexibles con sesgo claro a RV
                       (Cartesio Y, Avantage Fund, Acatis Datini Valueflex,
                        Flossbach Multiple Opportunities, R-co Valor)
    "40% RV y 60% RF"  mixtos moderados/neutrales
                       (Quality Inversión Moderada, Olea Neutral, Dunas Valor Flexible,
                        Tikehau Cross Assets)
    "25% RV y 75% RF"  mixtos conservadores
  Si es defensivo con oro/inflación -> "Cartera Permanente", no un reparto.

- "Inflación" NO es para fondos: ellos lo usan para posiciones de liquidez de bróker.
  NUNCA lo asignes a un fondo.

Devuelve SOLO un array JSON, sin markdown:
[{"isin":"...","benchmark":"..."|null,"nuevo":false,"confianza":"alta|media|baja","motivo":"<12 palabras"}]"""


def _call(fichas: list[dict], vocab: list[str]) -> list[dict]:
    from anthropic import Anthropic
    cli = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    payload = {"VOCABULARIO": vocab, "FONDOS": fichas}
    msg = cli.messages.create(
        model=MODEL,
        max_tokens=4000,
        temperature=0,
        system=[{"type": "text", "text": _SYSTEM, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
    )
    txt = msg.content[0].text.strip()
    txt = re.sub(r"^```(?:json)?|```$", "", txt, flags=re.M).strip()
    m = re.search(r"\[.*\]", txt, re.S)
    res = json.loads(m.group(0)) if m else []
    # Normaliza también lo que acuña el modelo: la regla "un concepto = una grafía"
    # aplica a los valores nuevos igual que a los de Horizonte (si no, el clasificador
    # inventa "Cat Bonds" y "Bonos Catástrofe" para lo mismo — pasó de verdad).
    from tools.benchmark_vocab import canon
    for r in res:
        if r.get("benchmark"):
            r["benchmark"] = canon(r["benchmark"])
    return res


def build_ficha_one(isin: str, client=None) -> dict:
    """Ficha de UN fondo, sin caché (para el sync de un análisis nuevo)."""
    from tools.morningstar_quant import fetch_category
    tax = _tax().get(isin, {})
    nombre = tax.get("nombre") or ""
    if not nombre and client is not None:
        r = client.table("funds").select("nombre_clase").eq("isin", isin).execute().data
        nombre = (r[0].get("nombre_clase") if r else "") or ""
    try:
        cat = fetch_category(isin) or {}
    except Exception:
        cat = {}
    return {
        "isin": isin,
        "nombre": nombre,
        "gestora": tax.get("issuer") or "",
        "tipo_activo": tax.get("tipo_activo") or "",
        "geografia": tax.get("geografia") or "",
        "estilo": tax.get("estilo") or "",
        "categoria_morningstar": cat.get("categoria_morningstar") or cat.get("categoria") or "",
        "analisis": _analysis_snippet(isin),
    }


def classify_one(ficha: dict) -> dict:
    """Clasifica una ficha. {} si el modelo no devuelve nada."""
    from tools.benchmark_vocab import load as load_vocab
    res = _call([ficha], load_vocab())
    return res[0] if res else {}


def classify(fichas: dict, batch: int = 12) -> dict:
    from tools.benchmark_vocab import load as load_vocab
    vocab = load_vocab()
    items = list(fichas.values())
    out = {}
    for i in range(0, len(items), batch):
        chunk = items[i:i + batch]
        try:
            for r in _call(chunk, vocab):
                if r.get("isin"):
                    out[r["isin"]] = r
        except Exception as e:
            print(f"  [WARN] batch {i}: {type(e).__name__} {str(e)[:80]}")
        print(f"  clasificados {len(out)}/{len(items)}")
    return out


# ---------------------------------------------------------------- validación
def horfin_benchmarks() -> dict:
    """{isin: benchmark_canonico} de Horizonte (verdad de referencia para validar)."""
    import csv
    from tools.benchmark_vocab import canon
    out = {}
    with open(CSV_HORFIN, encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f):
            isin = (r.get("ISIN") or "").strip()
            b = canon(r.get("Benchmark") or "")
            if isin and b:
                out[isin] = b
    return out


def validate() -> None:
    fichas = build_fichas()
    truth = horfin_benchmarks()
    common = {i: f for i, f in fichas.items() if i in truth}
    print(f"\nValidación a ciegas sobre {len(common)} fondos que Horizonte ya clasificó\n")
    got = classify(common)
    ok = miss = null = 0
    diffs = []
    for isin, exp in truth.items():
        if isin not in got:
            continue
        mine = got[isin].get("benchmark")
        if mine is None:
            null += 1
        elif mine == exp:
            ok += 1
        else:
            miss += 1
            diffs.append((isin, common[isin]["nombre"][:40], exp, mine, got[isin].get("motivo", "")))
    tot = ok + miss + null
    print(f"\n=== ACIERTO: {ok}/{tot} ({100*ok/max(tot,1):.0f}%) | distinto {miss} | null {null} ===")
    for d in diffs[:30]:
        print(f"  {d[0]} {d[1]:40s} HORFIN={d[2]:28s} MIO={d[3]:28s} ({d[4]})")
    (ROOT / "data" / "benchmark_validacion.json").write_text(
        json.dumps({"ok": ok, "distinto": miss, "null": null, "total": tot,
                    "diffs": [{"isin": d[0], "nombre": d[1], "horfin": d[2], "mio": d[3],
                               "motivo": d[4]} for d in diffs]},
                   ensure_ascii=False, indent=1), encoding="utf-8")


def run(apply: bool = False) -> None:
    fichas = build_fichas()
    got = classify(fichas)
    RESULT.write_text(json.dumps(got, ensure_ascii=False, indent=1), encoding="utf-8")
    n_new = sum(1 for r in got.values() if r.get("nuevo"))
    n_null = sum(1 for r in got.values() if not r.get("benchmark"))
    print(f"\nasignados {len(got)} | nuevos acuñados {n_new} | null {n_null} -> {RESULT}")
    if apply:
        from tools.supabase_client import get_client
        c = get_client()
        n = 0
        for isin, r in got.items():
            if r.get("benchmark"):
                c.table("funds").update({"benchmark": r["benchmark"]}).eq("isin", isin).execute()
                n += 1
        print(f"escritos en Supabase: {n}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--fichas", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.fichas:
        build_fichas(refresh=a.refresh)
    elif a.validate:
        validate()
    elif a.run:
        run(apply=a.apply)
    else:
        ap.print_help()
