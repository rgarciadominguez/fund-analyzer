"""Chequeo diario de salud: fund-analyzer + Supabase (catálogo + fund-dashboard).

Valida lo que revisamos a mano: reconciliación local↔Supabase, grupos huérfanos,
nombres rotos (solo analizados), nombres de grupo duplicados, AUM absurdo, sincronía
del puente al fund-dashboard, grupos sin gestora. Auto-limpia el drift SEGURO (grupos
huérfanos). Escribe un informe y sale 0 (sano) / 1 (con avisos).

Uso:
    python -m tools.daily_healthcheck            # chequea + auto-limpia huérfanos
    python -m tools.daily_healthcheck --no-fix   # solo chequea, no toca nada
"""
from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
_ISIN = re.compile(r"^[A-Z]{2}[A-Z0-9]{10}$")


def run(fix: bool = True) -> dict:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    from tools.supabase_client import get_client
    from tools.fund_name_utils import is_valid_fund_name
    from tools.cleanup_orphan_groups import find_orphans
    c = get_client()

    checks: list[tuple] = []   # (nivel, check, detalle)  nivel: OK|WARN|FAIL|FIX

    # 1) Reconciliación local↔Supabase (analizados publicados, sin zombis)
    try:
        from tools.audit_sync import audit
        a = audit()
        np, zomb = a.get("not_published", []), a.get("zombies", [])
        checks.append(("FAIL" if np else "OK", "analizados publicados",
                       f"{len(np)} sin publicar" if np else f"{a.get('n_local')} analizados, todos publicados"))
        checks.append(("FAIL" if zomb else "OK", "sin filas zombi",
                       f"{len(zomb)} zombis: {zomb[:5]}" if zomb else "0 zombis"))
    except Exception as e:
        checks.append(("WARN", "reconciliación", f"no evaluable: {str(e)[:80]}"))

    # 2) Grupos huérfanos → AUTO-LIMPIAR (seguro: 0 fondos apuntan a ellos)
    try:
        orph = find_orphans(c)
        if orph and fix:
            for g in orph:
                c.table("fund_groups").delete().eq("fund_group_id", g["fund_group_id"]).execute()
            checks.append(("FIX", "grupos huérfanos", f"{len(orph)} borrados"))
        elif orph:
            checks.append(("WARN", "grupos huérfanos", f"{len(orph)} (usa sin --no-fix para limpiar)"))
        else:
            checks.append(("OK", "grupos huérfanos", "0"))
    except Exception as e:
        checks.append(("WARN", "grupos huérfanos", f"no evaluable: {str(e)[:80]}"))

    # Datos de Supabase para el resto
    funds = c.table("funds").select(
        "isin,nombre_clase,has_qualitative_analysis,fund_group_id").limit(10000).execute().data
    groups = {g["fund_group_id"]: g for g in c.table("fund_groups").select(
        "fund_group_id,nombre_base,gestora,aum_meur").limit(10000).execute().data}

    # 3) Nombres rotos — SOLO en fondos analizados (los importados con ISIN son esperados)
    malos = []
    for f in funds:
        if not f.get("has_qualitative_analysis"):
            continue
        nm = (f.get("nombre_clase") or "").strip()
        ok, mot = is_valid_fund_name(nm, f["isin"])
        if not ok:
            malos.append(f"{f['isin']}:{mot}")
    checks.append(("FAIL" if malos else "OK", "nombres de analizados válidos",
                   f"{len(malos)} rotos: {malos[:5]}" if malos else "todos válidos"))

    # 3b) Filas de CLASE con nombre-etiqueta ("CLASE I") — no dicen de qué fondo son.
    # Se publican al catálogo Y al portal, así que se miran en TODAS las filas, no
    # solo en las analizadas (2026-07-16: 23 filas así llevaban tiempo publicadas).
    from tools.reconcile_fund_groups import _BARE_CLASS_RE
    etiqueta = [f["isin"] for f in funds
                if _BARE_CLASS_RE.match((f.get("nombre_clase") or "").strip())]
    checks.append(("FAIL" if etiqueta else "OK", "clases con nombre de fondo",
                   f"{len(etiqueta)} solo-etiqueta: {etiqueta[:5]}" if etiqueta
                   else "ninguna con nombre-etiqueta"))

    # 4) Nombres de grupo duplicados
    from collections import Counter
    names = Counter((g.get("nombre_base") or "").strip().lower() for g in groups.values() if g.get("nombre_base"))
    dups = [n for n, ct in names.items() if ct > 1]
    checks.append(("FAIL" if dups else "OK", "grupos sin nombre duplicado",
                   f"{len(dups)} dups: {dups[:5]}" if dups else "0 duplicados"))

    # 5) AUM absurdo (>500.000 M€ = error de unidades/paraguas)
    absurd = [g["fund_group_id"][:8] for g in groups.values()
              if isinstance(g.get("aum_meur"), (int, float)) and g["aum_meur"] > 500000]
    checks.append(("FAIL" if absurd else "OK", "AUM sin errores de unidades",
                   f"{len(absurd)} absurdos" if absurd else "ok"))

    # 6) Sincronía con fund-dashboard (todos los analizados presentes en el puente)
    try:
        from tools.benchmarks_to_funddash import _repo_isins
        fd = _repo_isins()
        analizados = [f["isin"] for f in funds if f.get("has_qualitative_analysis")]
        faltan = [i for i in analizados if i not in fd]
        checks.append(("WARN" if faltan else "OK", "fund-dashboard sincronizado",
                       f"{len(faltan)} sin puentear: {faltan[:5]}" if faltan else f"{len(analizados)} presentes"))
    except Exception as e:
        checks.append(("WARN", "fund-dashboard", f"no evaluable: {str(e)[:80]}"))

    # Veredicto
    n_fail = sum(1 for lvl, _, _ in checks if lvl == "FAIL")
    n_warn = sum(1 for lvl, _, _ in checks if lvl == "WARN")
    n_fix = sum(1 for lvl, _, _ in checks if lvl == "FIX")
    verdict = "FAIL" if n_fail else ("WARN" if n_warn else "OK")
    report = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "verdict": verdict, "n_fail": n_fail, "n_warn": n_warn, "n_fix": n_fix,
        "checks": [{"nivel": l, "check": ch, "detalle": d} for l, ch, d in checks],
    }
    return report


_MARKER = ROOT / "data" / "HEALTHCHECK_FAIL.txt"   # marcador visible si hay fallo


def _write(report: dict):
    # último estado + log rotativo (últimas 60 líneas)
    (ROOT / "data" / "healthcheck_last.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    logp = ROOT / "data" / "healthcheck.log"
    line = (f"{report['ts']}  {report['verdict']}  "
            f"fail={report['n_fail']} warn={report['n_warn']} fix={report['n_fix']}\n")
    prev = logp.read_text(encoding="utf-8").splitlines()[-59:] if logp.exists() else []
    logp.write_text("\n".join(prev + [line.rstrip()]) + "\n", encoding="utf-8")


def _alert_if_fail(report: dict):
    """Aviso VISIBLE en Windows si hay FAIL (cuadro de diálogo, sin instalar nada) +
    marcador en disco. Si vuelve a estar sano, borra el marcador."""
    if report["verdict"] != "FAIL":
        try:
            _MARKER.unlink(missing_ok=True)
        except Exception:
            pass
        return
    fails = [c for c in report["checks"] if c["nivel"] == "FAIL"]
    detalle = "; ".join(f"{c['check']}: {c['detalle']}" for c in fails)[:400]
    _MARKER.write_text(f"{report['ts']}\nFAIL ({report['n_fail']})\n{detalle}\n", encoding="utf-8")
    msg = f"fund-analyzer: CHEQUEO FALLIDO ({report['n_fail']}). {detalle[:200]}"
    try:                       # cuadro de diálogo de Windows (built-in en Pro)
        import subprocess
        subprocess.run(["msg", "*", "/TIME:900", msg], timeout=15, capture_output=True)
    except Exception:
        pass


def status():
    """¿Está corriendo el chequeo y todo bien? Un vistazo."""
    from datetime import timedelta
    rep_p = ROOT / "data" / "healthcheck_last.json"
    if not rep_p.exists():
        print("✗ Nunca ha corrido (sin data/healthcheck_last.json)")
        return 1
    rep = json.loads(rep_p.read_text(encoding="utf-8"))
    ts = datetime.fromisoformat(rep["ts"])
    age = datetime.now(timezone.utc) - ts
    fresh = age < timedelta(hours=36)          # debe correr 1x/día
    icon = {"OK": "✓", "WARN": "⚠", "FAIL": "✗"}
    print("\n=== ESTADO DEL CHEQUEO DIARIO ===")
    print(f"  ¿Está corriendo?  {'✓ SÍ' if fresh else '✗ NO — lleva ' + str(age).split('.')[0] + ' sin correr'}"
          f"   (última: {ts.astimezone().strftime('%Y-%m-%d %H:%M')}, hace {str(age).split('.')[0]})")
    print(f"  Último veredicto: {icon.get(rep['verdict'],'?')} {rep['verdict']} "
          f"({rep['n_fail']} fallos, {rep['n_warn']} avisos, {rep['n_fix']} auto-arreglos)")
    if rep["verdict"] != "OK":
        for c in rep["checks"]:
            if c["nivel"] in ("FAIL", "WARN"):
                print(f"      → {c['check']}: {c['detalle']}")
    # estado de la tarea de Windows
    try:
        import subprocess
        out = subprocess.run(["schtasks", "/query", "/tn", "healthcheck-fund-analyzer", "/fo", "LIST"],
                             capture_output=True, text=True, timeout=15).stdout
        st = next((l.split(":", 1)[1].strip() for l in out.splitlines()
                   if l.lower().startswith(("estado", "status"))), "?")
        print(f"  Tarea Windows:    healthcheck-fund-analyzer — {st}")
    except Exception:
        print("  Tarea Windows:    (no consultable)")
    return 0 if (fresh and rep["verdict"] != "FAIL") else 1


def main():
    if "--status" in sys.argv:            # ¿está corriendo y todo bien?
        sys.exit(status())
    fix = "--no-fix" not in sys.argv
    report = run(fix=fix)
    _write(report)
    _alert_if_fail(report)                # aviso visible si hay FAIL
    icon = {"OK": "✓", "WARN": "⚠", "FAIL": "✗", "FIX": "🔧"}
    print(f"\n=== CHEQUEO DIARIO fund-analyzer — {report['verdict']} ===")
    for ch in report["checks"]:
        print(f"  {icon.get(ch['nivel'], '?')} {ch['check']}: {ch['detalle']}")
    print(f"\nVEREDICTO: {icon[report['verdict']]} {report['verdict']} "
          f"({report['n_fail']} fallos, {report['n_warn']} avisos, {report['n_fix']} auto-arreglos)")
    sys.exit(1 if report["verdict"] == "FAIL" else 0)


if __name__ == "__main__":
    main()
