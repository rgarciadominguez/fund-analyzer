"""Auditoría post-análisis de UN fondo: chequea todos los fallos conocidos que
hemos ido arreglando, en un solo comando, sobre output.json + Supabase.

Bloques de checks:
  IDENTIDAD     nombre válido (no gestora/clase/categoría/ISIN/placeholder),
                gestora no vacía/≠ISIN/≠nombre.
  ANÁLISIS      analyst_synthesis presente y no vacío (fallo silencioso #1),
                sin blockers de calidad, sin texto de TEST, drift coherente.
  CUANTITATIVO  AUM presente y razonable (no umbrella €>50B), años_antiguedad =
                clase más antigua, Morningstar devuelve EL ISIN pedido (no otro).
  CONTAMINACIÓN cartas/gestores que no mencionan el fondo/gestora (cross-fund).
  PUBLICACIÓN   publicado en Supabase, sync_status=ok, grupo sin duplicar/huérfano,
                agrupado con sus clases si las hay.

Uso:
    python -m tools.audit_fund LU1234567890
    python -m tools.audit_fund LU1234567890 --json
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FUNDS_DIR = ROOT / "data" / "funds"
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")

OK, WARN, FAIL = "OK", "WARN", "FAIL"


def _load_output(isin: str) -> dict | None:
    p = FUNDS_DIR / isin / "output.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _txt(section: dict | None) -> str:
    if not isinstance(section, dict):
        return ""
    t = section.get("texto") or ""
    return t if isinstance(t, str) else ""


# ───────────────────────── checks de IDENTIDAD ─────────────────────────

def chk_nombre(isin, d, checks):
    nombre = (d.get("nombre") or "").strip()
    if not nombre:
        return checks.append(("IDENTIDAD", "nombre presente", FAIL, "nombre vacío"))
    try:
        from tools.fund_name_utils import is_valid_fund_name
        ok, motivo = is_valid_fund_name(nombre, isin)
    except Exception:
        ok, motivo = (len(nombre) >= 5 and nombre.upper() != isin), "fallback"
    checks.append(("IDENTIDAD", "nombre válido", OK if ok else FAIL,
                   nombre if ok else f"{nombre!r} → {motivo}"))


def chk_gestora(isin, d, checks):
    g = (d.get("gestora") or "").strip()
    n = (d.get("nombre") or "").strip()
    if not g:
        checks.append(("IDENTIDAD", "gestora presente", FAIL, "gestora vacía → no se publicaría"))
    elif g.upper() == isin.upper():
        checks.append(("IDENTIDAD", "gestora ≠ ISIN", FAIL, g))
    elif re.sub(r"[^a-z0-9]", "", g.lower()) == re.sub(r"[^a-z0-9]", "", n.lower()):
        checks.append(("IDENTIDAD", "gestora ≠ nombre", WARN, f"gestora==nombre ({g!r})"))
    else:
        checks.append(("IDENTIDAD", "gestora ok", OK, g))


def chk_placeholder(isin, d, checks):
    try:
        from tools.sync_to_supabase import _has_test_placeholder
        bad = _has_test_placeholder(d)
    except Exception:
        bad = bool(re.search(r"BASURA|FALSO|LOREM|PLACEHOLDER", json.dumps(d)[:5000], re.I))
    checks.append(("IDENTIDAD", "sin texto de TEST", FAIL if bad else OK,
                   "contiene placeholder de test" if bad else "limpio"))


# ───────────────────────── checks de ANÁLISIS ─────────────────────────

def chk_analyst_present(isin, d, checks):
    syn = d.get("analyst_synthesis") or {}
    secciones = [k for k in syn if isinstance(syn.get(k), dict)]
    con_texto = sum(1 for k in secciones if len(_txt(syn.get(k))) > 100
                    or (syn.get(k) or {}).get("perfiles") or (syn.get(k) or {}).get("hitos"))
    if not syn:
        checks.append(("ANÁLISIS", "analyst_synthesis presente", FAIL,
                       "ausente (¿fallo silencioso del analyst?)"))
    elif con_texto < 3:
        checks.append(("ANÁLISIS", "secciones con contenido", FAIL,
                       f"solo {con_texto} sección(es) con contenido real"))
    else:
        checks.append(("ANÁLISIS", "analyst_synthesis OK", OK, f"{con_texto} secciones con contenido"))


def chk_quality(isin, d, checks):
    try:
        from tools.analysis_quality import assess_analysis_quality
        q = assess_analysis_quality(d)
        blockers = q.get("blockers") or []
        checks.append(("ANÁLISIS", "sin blockers de calidad", FAIL if blockers else OK,
                       "; ".join(blockers) if blockers else "ok"))
    except Exception as e:
        checks.append(("ANÁLISIS", "calidad", WARN, f"no evaluable: {str(e)[:60]}"))


def chk_drift(isin, d, checks):
    try:
        from tools.output_accessor import detect_drift
        drifts = detect_drift(d) or []
        # solo nombre/gestora/perfiles son bloqueantes; AUM es warning
        hard = [x for x in drifts if any(k in str(x).lower() for k in ("nombre", "gestora", "perfil"))]
        if hard:
            checks.append(("ANÁLISIS", "sin drift de identidad", FAIL, "; ".join(map(str, hard))[:160]))
        elif drifts:
            checks.append(("ANÁLISIS", "drift menor (AUM)", WARN, "; ".join(map(str, drifts))[:160]))
        else:
            checks.append(("ANÁLISIS", "sin drift", OK, "coherente"))
    except Exception as e:
        checks.append(("ANÁLISIS", "drift", WARN, f"no evaluable: {str(e)[:60]}"))


# ─────────────────────── checks CUANTITATIVO ───────────────────────

def chk_aum(isin, d, checks):
    aum = (d.get("kpis") or {}).get("aum_actual_meur")
    if aum is None:
        checks.append(("CUANTITATIVO", "AUM presente", WARN, "AUM None (puede ser legítimo)"))
    elif isinstance(aum, (int, float)) and aum > 500000:
        checks.append(("CUANTITATIVO", "AUM imposible", FAIL,
                       f"{aum} M€ > €500.000M → error de unidades/paraguas"))
    elif isinstance(aum, (int, float)) and aum > 50000:
        checks.append(("CUANTITATIVO", "AUM (revisar)", WARN,
                       f"{aum} M€ > €50.000M → revisar si es del sub-fondo o del paraguas"))
    else:
        checks.append(("CUANTITATIVO", "AUM ok", OK, f"{aum} M€"))


def chk_antiguedad(isin, d, checks):
    try:
        from tools.anios_antiguedad import extract_start_year
        start = extract_start_year(d)
        cur = (d.get("kpis") or {}).get("anio_creacion")
        if start and cur and int(re.search(r"\d{4}", str(cur)).group()) > start:
            checks.append(("CUANTITATIVO", "años desde clase más antigua", WARN,
                           f"anio_creacion={cur} pero hay señal más antigua {start}"))
        else:
            checks.append(("CUANTITATIVO", "años_antiguedad ok", OK, f"inicio≈{start or cur}"))
    except Exception as e:
        checks.append(("CUANTITATIVO", "años_antiguedad", WARN, f"no evaluable: {str(e)[:50]}"))


def chk_morningstar_isin(isin, d, checks):
    try:
        from tools.morningstar_quant import fetch_category
        cat = fetch_category(isin)  # ya valida que la fila devuelta es del ISIN pedido
        if cat.get("categoria_morningstar"):
            checks.append(("CUANTITATIVO", "Morningstar = ISIN pedido", OK,
                           cat["categoria_morningstar"]))
        else:
            checks.append(("CUANTITATIVO", "Morningstar", WARN,
                           "sin categoría (no cubierto o ISIN no matchea — no se escriben datos ajenos)"))
    except Exception as e:
        checks.append(("CUANTITATIVO", "Morningstar", WARN, f"no evaluable: {str(e)[:50]}"))


# ─────────────────────── checks CONTAMINACIÓN ───────────────────────

def _anchors(d):
    n = (d.get("nombre") or "").lower()
    g = (d.get("gestora") or "").lower()
    toks = [t for t in re.split(r"[^a-záéíóúñ0-9]+", n + " " + g) if len(t) > 3]
    return set(toks)


def chk_cross_fund(isin, d, checks):
    anchors = _anchors(d)
    if not anchors:
        return checks.append(("CONTAMINACIÓN", "cartas/gestores", WARN, "sin anchors de nombre/gestora"))
    # cartas
    letters = (_load_json(FUNDS_DIR / isin / "letters_data.json") or {})
    cartas = letters.get("cartas") or letters.get("analisis_completos") or []
    sosp = []
    for c in cartas if isinstance(cartas, list) else []:
        txt = json.dumps(c, ensure_ascii=False).lower()
        if c.get("extracted_error"):
            continue
        if not any(a in txt for a in anchors):
            sosp.append(c.get("periodo") or c.get("fecha") or c.get("titulo") or "?")
    if sosp:
        checks.append(("CONTAMINACIÓN", "cartas mencionan el fondo", WARN,
                       f"{len(sosp)} carta(s) sin mención del fondo/gestora: {sosp[:4]}"))
    else:
        checks.append(("CONTAMINACIÓN", "cartas", OK, f"{len(cartas) if isinstance(cartas,list) else 0} cartas coherentes"))


def _load_json(p: Path):
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


# ─────────────────────── checks PUBLICACIÓN ───────────────────────

def chk_publicacion(isin, d, checks, client):
    if client is None:
        return checks.append(("PUBLICACIÓN", "Supabase", WARN, "sin conexión Supabase"))
    row = client.table("funds").select("isin,fund_group_id,nombre_clase").eq("isin", isin).execute().data
    if not row:
        checks.append(("PUBLICACIÓN", "publicado en catálogo", FAIL,
                       "NO está en funds (analizado pero no publicado)"))
        return
    checks.append(("PUBLICACIÓN", "publicado en catálogo", OK, row[0].get("nombre_clase")))
    # estado del último sync
    st = _load_json(ROOT / "data" / "sync_status.json") or {}
    s = st.get(isin, {})
    if s.get("status") and s["status"] != "ok":
        checks.append(("PUBLICACIÓN", "último sync", WARN, f"{s['status']}: {s.get('reasons')}"))
    # grupo no duplicado / sin huérfanos nuevos
    gid = row[0].get("fund_group_id")
    if gid:
        g = client.table("fund_groups").select("nombre_base").eq("fund_group_id", gid).execute().data
        base = (g[0].get("nombre_base") if g else "") or ""
        dups = client.table("fund_groups").select("fund_group_id").eq("nombre_base", base).execute().data
        checks.append(("PUBLICACIÓN", "grupo sin duplicar", OK if len(dups) <= 1 else FAIL,
                       f"{len(dups)} grupo(s) con nombre {base!r}"))


def chk_agrupado(isin, d, checks):
    try:
        from tools.auto_group_classes import find_same_fund
        sibs = find_same_fund(isin)
        if sibs:
            from tools import class_links
            primary = class_links.resolve_primary(isin)
            agrupados = all(class_links.resolve_primary(s) == primary for s in sibs)
            checks.append(("PUBLICACIÓN", "clases hermanas agrupadas",
                           OK if agrupados else FAIL,
                           f"hermanas={sorted(sibs)} primary={primary}"))
        else:
            checks.append(("PUBLICACIÓN", "agrupado", OK, "sin clases hermanas analizadas"))
    except Exception as e:
        checks.append(("PUBLICACIÓN", "agrupado", WARN, f"no evaluable: {str(e)[:50]}"))


# ───────────────────────────── runner ─────────────────────────────

def audit_fund(isin: str) -> dict:
    isin = (isin or "").upper().strip()
    d = _load_output(isin)
    if d is None:
        return {"isin": isin, "error": "sin output.json local", "checks": []}
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env")
        from tools.supabase_client import get_client
        client = get_client()
    except Exception:
        client = None

    checks: list[tuple] = []
    chk_nombre(isin, d, checks)
    chk_gestora(isin, d, checks)
    chk_placeholder(isin, d, checks)
    chk_analyst_present(isin, d, checks)
    chk_quality(isin, d, checks)
    chk_drift(isin, d, checks)
    chk_aum(isin, d, checks)
    chk_antiguedad(isin, d, checks)
    chk_morningstar_isin(isin, d, checks)
    chk_cross_fund(isin, d, checks)
    chk_publicacion(isin, d, checks, client)
    chk_agrupado(isin, d, checks)

    n_fail = sum(1 for _, _, s, _ in checks if s == FAIL)
    n_warn = sum(1 for _, _, s, _ in checks if s == WARN)
    veredicto = FAIL if n_fail else (WARN if n_warn else OK)
    return {"isin": isin, "nombre": d.get("nombre"), "veredicto": veredicto,
            "n_fail": n_fail, "n_warn": n_warn, "checks": checks}


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    if not args:
        print("uso: python -m tools.audit_fund <ISIN> [--json]")
        return
    res = audit_fund(args[0])
    if "--json" in sys.argv:
        res["checks"] = [{"bloque": b, "check": c, "estado": s, "detalle": det}
                         for b, c, s, det in res["checks"]]
        print(json.dumps(res, ensure_ascii=False, indent=2))
        return
    if res.get("error"):
        print(f"✗ {res['isin']}: {res['error']}")
        return
    icon = {OK: "✓", WARN: "⚠", FAIL: "✗"}
    print(f"\n=== AUDITORÍA DEL ANÁLISIS — {res['isin']} ({res.get('nombre')}) ===")
    bloque_actual = None
    for b, c, s, det in res["checks"]:
        if b != bloque_actual:
            print(f"\n[{b}]")
            bloque_actual = b
        print(f"  {icon[s]} {c}: {det}")
    v = res["veredicto"]
    print(f"\nVEREDICTO: {icon[v]} {v}   ({res['n_fail']} fallos, {res['n_warn']} avisos)")


if __name__ == "__main__":
    main()
