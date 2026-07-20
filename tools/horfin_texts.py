"""
horfin_texts.py — Genera `descripcion`, `opinion` y `encaje` al estilo Horizonte.

OBJETIVO (esto es lo que pidió Rafa, y no es "rellenar el catálogo"):
    que DE AHORA EN ADELANTE todo fondo que analicemos salga ya con `descripcion` y
    `opinion` escritas, para que él no tenga que redactarlas a mano.

TRES CAMPOS, TRES PREGUNTAS DISTINTAS:
    descripcion  QUÉ ES        factual, NO valora. "Costes altos" es opinion, no descripcion.
    opinion      QUÉ NOS PARECE juicio: costes, exposición, AUM, track record.
    encaje       A QUIÉN LE SIRVE

BLINDADO: si Horizonte ya escribió el campo, gana el suyo. Este generador es
fill-if-empty puro. Nunca pisa texto humano.

POR QUÉ POR CLASE (aprendido del aporte 2026-07-17):
    Nuestras opiniones eran a nivel FONDO y se copiaban entre clases -> perdían el dato
    accionable. Las de Horizonte son a nivel CLASE: "hay otra clase disponible con menores
    comisiones". Por eso aquí se cargan las CLASES HERMANAS (mismo fund_group_id) con su
    TER/comisión y se le pide al modelo que compare. Si esta clase es la cara, hay que decirlo.

Few-shot: se toman ejemplos REALES del aporte de Horizonte en cada llamada, así el estilo
se mantiene alineado solo (y se actualiza si ellos cambian de estilo).

CLI:
    python -m tools.horfin_texts --isin ES0112231008
    python -m tools.horfin_texts --all --fill-empty [--apply]
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

APORTE = Path(r"C:\Users\RafaelGarcía\horizonte-datos\aporte_horfin.json")
MODEL = "claude-sonnet-4-5"

_SYSTEM = """Escribes fichas de fondos para Horizonte Financiero, en español, con SU estilo.

TRES CAMPOS, TRES PREGUNTAS DISTINTAS. No los mezcles:

- "descripcion" (~85 caracteres): QUÉ ES el fondo. Factual y neutro. NO VALORA NUNCA.
  Tipo de activo, exposición, duración/geografía si aplica.
  MAL: "Fondo monetario con costes altos"  <- "costes altos" es un juicio, va en opinion.
  BIEN: "Fondo monetario con 2-3 años duración media con foco en deuda corporativa"

- "opinion" (~170 caracteres): QUÉ NOS PARECE. Juicio explícito sobre: costes, exposición,
  AUM y track record. Si esta CLASE es más cara que una clase hermana, DILO
  ("hay otra clase disponible con menores comisiones") — es el dato más accionable.
  Es una opinión de ESTA CLASE, no del fondo en abstracto.

- "encaje": A QUIÉN LE SIRVE. Perfil de inversor y horizonte.

REGLAS DURAS:
- Solo puedes usar datos de la FICHA. Prohibido inventar cifras, gestores o rentabilidades.
- Si te falta base para un campo, devuelve null en ese campo. Un hueco se gestiona;
  un dato inventado rompe una decisión de inversión.
- No copies el nombre del fondo como descripcion.
- Los TRES campos son obligatorios salvo que de verdad te falte base. En particular
  "encaje" NO se omite: si sabes el tipo de activo y el riesgo, sabes a quién le sirve.

Devuelve SOLO JSON, sin markdown:
{"descripcion":"..."|null,"opinion":"..."|null,"encaje":"..."|null,"omitido":"<motivo si algún campo es null>"}"""


def _fewshot(n: int = 6, client=None) -> list[dict]:
    """Ejemplos reales de Horizonte (estilo de referencia).

    El aporte NO trae `encaje` (solo descripcion/opinion). Si el few-shot solo enseña dos
    campos, el modelo se deja el tercero: pasó de verdad en la validación end-to-end
    (encaje volvía null). Por eso los ejemplos de encaje se sacan de los `encaje_texto`
    que Rafa ya escribió en Supabase.
    """
    out = []
    if APORTE.exists():
        acts = json.loads(APORTE.read_text(encoding="utf-8"))["activos"]
        ok = [a for a in acts
              if (a.get("descripcion") or "").strip() and (a.get("opinion") or "").strip()]
        out = [{"descripcion": a["descripcion"], "opinion": a["opinion"]} for a in ok[:n]]
    try:
        from tools.supabase_client import get_client
        c = client or get_client()
        rows = c.table("funds").select("encaje_texto").not_.is_("encaje_texto", "null") \
            .limit(n).execute().data
        ej = [(r.get("encaje_texto") or "").strip() for r in rows]
        for i, e in enumerate(x for x in ej if x):
            if i < len(out):
                out[i]["encaje"] = e
            else:
                out.append({"encaje": e})
    except Exception:
        pass
    return out


def build_ficha(isin: str, client=None) -> dict:
    """Todo lo que sabemos del fondo + sus clases hermanas."""
    from tools.supabase_client import get_client
    c = client or get_client()

    # Las columnas del contrato Horizonte pueden no existir todavía (DDL manual
    # pendiente). Si faltan, se degrada al set base en vez de reventar.
    _full = ("isin,nombre_clase,fund_group_id,ter_pct,comision_gestion_pct,divisa,"
             "categoria_activo,benchmark,estrellas,importe_minimo_eur,distribucion")
    _base = "isin,nombre_clase,fund_group_id,ter_pct,comision_gestion_pct,divisa,importe_minimo_eur,distribucion"
    try:
        rows = c.table("funds").select(_full).eq("isin", isin).execute().data
    except Exception:
        rows = c.table("funds").select(_base).eq("isin", isin).execute().data
    if not rows:
        return {}
    me = rows[0]

    hermanas = []
    if me.get("fund_group_id"):
        sib = c.table("funds").select(
            "isin,nombre_clase,ter_pct,comision_gestion_pct,divisa"
        ).eq("fund_group_id", me["fund_group_id"]).neq("isin", isin).execute().data
        hermanas = [{k: s.get(k) for k in
                     ("isin", "nombre_clase", "ter_pct", "comision_gestion_pct", "divisa")}
                    for s in sib]

    tax = {}
    p = ROOT / "data" / "fund_taxonomy.json"
    if p.exists():
        tax = json.loads(p.read_text(encoding="utf-8")).get("funds", {}).get(isin, {})

    analisis, kpis = "", {}
    op = ROOT / "data" / "funds" / isin / "output.json"
    if op.exists():
        try:
            d = json.loads(op.read_text(encoding="utf-8"))
            from tools.output_accessor import get_resumen_texto, get_section_estrategia
            est = get_section_estrategia(d) or {}
            analisis = re.sub(r"\s+", " ", (get_resumen_texto(d) or "") + " " +
                              (est.get("texto", "") if isinstance(est, dict) else ""))[:1200]
            kpis = d.get("kpis") or {}
            an = d.get("analisis_cuantitativo") or {}
            ms = an.get("morningstar") or {}
            if ms.get("rentabilidades"):
                kpis["rentabilidades"] = ms["rentabilidades"]
            if ms.get("riesgo"):
                kpis["riesgo_3a"] = (ms["riesgo"] or {}).get("3a")
        except Exception:
            pass

    return {
        "isin": isin,
        "nombre": tax.get("nombre") or me.get("nombre_clase") or "",
        "clase": me.get("nombre_clase") or "",
        "gestora": tax.get("issuer") or "",
        "tipo_activo": tax.get("tipo_activo") or "",
        "geografia": tax.get("geografia") or "",
        "estilo": tax.get("estilo") or "",
        "categoria_activo": me.get("categoria_activo"),
        "benchmark": me.get("benchmark"),
        "divisa": me.get("divisa"),
        "ter_pct": me.get("ter_pct"),
        "comision_gestion_pct": me.get("comision_gestion_pct"),
        "estrellas_morningstar": me.get("estrellas"),
        "importe_minimo_eur": me.get("importe_minimo_eur"),
        "kpis": {k: v for k, v in kpis.items() if v not in (None, {}, [])},
        "clases_hermanas": hermanas,
        "analisis": analisis,
    }


def generate(ficha: dict, _fs_client=None) -> dict:
    from anthropic import Anthropic
    cli = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    payload = {"EJEMPLOS_DE_ESTILO_HORIZONTE": _fewshot(client=_fs_client),
               "FICHA_DEL_FONDO": ficha}
    msg = cli.messages.create(
        model=MODEL, max_tokens=900, temperature=0,
        system=[{"type": "text", "text": _SYSTEM, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
    )
    try:
        from tools.cost_monitor import track_anthropic, CAT_ANALISIS
        track_anthropic("horfin_texts", MODEL, msg,
                        isin=ficha.get("isin", ""), categoria=CAT_ANALISIS)
    except Exception:
        pass
    txt = re.sub(r"^```(?:json)?|```$", "", msg.content[0].text.strip(), flags=re.M).strip()
    m = re.search(r"\{.*\}", txt, re.S)
    return json.loads(m.group(0)) if m else {}


def run_all(fill_empty: bool = True, apply: bool = False, limit: int | None = None) -> None:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    c = get_client()

    rows = c.table("funds").select("isin,descripcion,opinion_user,encaje_texto").execute().data
    if fill_empty:
        rows = [r for r in rows if not (r.get("descripcion") or "").strip()
                or not (r.get("opinion_user") or "").strip()
                or not (r.get("encaje_texto") or "").strip()]
    if limit:
        rows = rows[:limit]
    print(f"fondos a generar (fill-if-empty, nunca pisa a Horizonte): {len(rows)}")

    done = 0
    for i, r in enumerate(rows, 1):
        isin = r["isin"]
        try:
            f = build_ficha(isin, client=c)
            if not f:
                continue
            g = generate(f)
        except Exception as e:
            print(f"  [WARN] {isin}: {type(e).__name__} {str(e)[:70]}")
            continue

        upd = {}
        if g.get("descripcion") and not (r.get("descripcion") or "").strip():
            upd["descripcion"] = g["descripcion"]
        if g.get("opinion") and not (r.get("opinion_user") or "").strip():
            upd["opinion_user"] = g["opinion"]
        if g.get("encaje") and not (r.get("encaje_texto") or "").strip():
            upd["encaje_texto"] = g["encaje"]
        if upd and apply:
            c.table("funds").update(upd).eq("isin", isin).execute()
        if upd:
            done += 1
        if i % 20 == 0:
            print(f"  {i}/{len(rows)}")
    print(f"{'escritos' if apply else '(dry-run) generados'}: {done}")


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--isin")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--fill-empty", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int)
    a = ap.parse_args()
    if a.isin:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        f = build_ficha(a.isin)
        print(json.dumps(f, ensure_ascii=False, indent=1)[:1500])
        print("\n--- generado ---")
        print(json.dumps(generate(f), ensure_ascii=False, indent=1))
    elif a.all:
        run_all(fill_empty=a.fill_empty or True, apply=a.apply, limit=a.limit)
    else:
        ap.print_help()
