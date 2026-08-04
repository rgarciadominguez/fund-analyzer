"""
Cost Monitor — telemetría persistente de coste LLM.

Reemplaza cost_report.json por fondo (que se sobrescribe en cada rerun) por un
log append-only global: data/cost_log.jsonl. Cada llamada LLM añade una línea.

Uso programático:
    from tools.cost_monitor import log_call, summary_today, summary_month

    log_call(
        agent="analyst",
        model="claude-sonnet-4-6",
        isin="ES0112602000",
        input_tokens=12000,
        output_tokens=1500,
        cost_usd=0.0625,
    )

CLI:
    python -m tools.cost_monitor                     # resumen últimos 7 días + mes
    python -m tools.cost_monitor --today             # solo hoy
    python -m tools.cost_monitor --month             # mes actual
    python -m tools.cost_monitor --by-agent          # breakdown por agente
    python -m tools.cost_monitor --by-model          # breakdown por modelo
    python -m tools.cost_monitor --top-funds 10      # top fondos más caros
    python -m tools.cost_monitor --since YYYY-MM-DD  # desde fecha

Fase Cost-Opt (2026-05-02): respuesta a usuario tras factura €100 inesperada Google.
"""
import json
import sys
import os
from collections import defaultdict
from datetime import datetime, date, timedelta
from pathlib import Path

ROOT = Path(__file__).parent.parent
LOG_PATH = ROOT / "data" / "cost_log.jsonl"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

EUR_USD_RATIO = 0.92  # aproximado, actualizar manualmente

# Umbrales de alerta
WARN_DAY_USD = 3.0
WARN_MONTH_USD = 50.0


# Categorías de gasto (para el admin: separar análisis de fondos vs imágenes).
CAT_ANALISIS = "analisis_fondos"      # síntesis/extracción/enriquecimiento de texto
CAT_IMAGENES = "procesar_imagenes"    # visión sobre PDFs/imágenes
CAT_OTROS = "otros"

# Precio USD por 1M tokens (input, output). Actualizar si cambian tarifas.
PRICING = {
    "claude-haiku-4-5-20251001": (1.0, 5.0),
    "claude-haiku-4-5": (1.0, 5.0),
    "claude-sonnet-4-5": (3.0, 15.0),
    "claude-sonnet-4-5-20241022": (3.0, 15.0),
    "claude-opus-4-8": (15.0, 75.0),
    "claude-opus-4-7": (15.0, 75.0),
}


def cost_from_usage(model: str, input_tokens: int, output_tokens: int) -> float:
    """Coste USD desde tokens según PRICING. 0 si el modelo no está tarifado."""
    pin, pout = PRICING.get(model, (0.0, 0.0))
    return (input_tokens * pin + output_tokens * pout) / 1_000_000


def log_call(agent: str, model: str, isin: str = "",
             input_tokens: int = 0, output_tokens: int = 0,
             cost_usd: float = 0.0, cached: bool = False,
             categoria: str = CAT_ANALISIS) -> None:
    """Append entry al log global. Idempotente y bajo overhead.

    Args:
        agent: nombre del agente que disparó la call (analyst, extractor, etc.)
        model: identificador del modelo usado
        isin: ISIN del fondo (opcional)
        input_tokens: tokens enviados
        output_tokens: tokens generados
        cost_usd: coste en USD (calcular antes de pasar)
        cached: True si fue un cache hit (cost=0 implícito)
        categoria: analisis_fondos | procesar_imagenes | otros (para el admin)
    """
    try:
        entry = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "date": date.today().isoformat(),
            "agent": agent,
            "model": model,
            "isin": isin,
            "categoria": categoria,
            "input_tok": input_tokens,
            "output_tok": output_tokens,
            "cost_usd": round(cost_usd, 6),
            "cached": cached,
        }
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass  # log failure no debe romper pipeline


def track_anthropic(agent: str, model: str, response, isin: str = "",
                    categoria: str = CAT_ANALISIS, has_images: bool = False) -> float:
    """Loguea el coste de una respuesta del SDK de Anthropic leyendo response.usage.

    Devuelve el coste USD. `has_images=True` fuerza la categoría procesar_imagenes.
    Best-effort: nunca rompe el flujo del que llama.
    """
    try:
        u = getattr(response, "usage", None)
        it = int(getattr(u, "input_tokens", 0) or 0)
        ot = int(getattr(u, "output_tokens", 0) or 0)
        cost = cost_from_usage(model, it, ot)
        log_call(agent, model, isin=isin, input_tokens=it, output_tokens=ot,
                 cost_usd=cost, categoria=(CAT_IMAGENES if has_images else categoria))
        return cost
    except Exception:
        return 0.0


def _read_entries(since: date | None = None) -> list[dict]:
    """Lee entries del log filtrando por fecha (since incluido)."""
    if not LOG_PATH.exists():
        return []
    out = []
    try:
        with LOG_PATH.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                    if since:
                        d = date.fromisoformat(e.get("date", "1970-01-01"))
                        if d < since:
                            continue
                    out.append(e)
                except Exception:
                    continue
    except Exception:
        return []
    return out


def summary_today() -> dict:
    """Coste de hoy: total + breakdown."""
    today = date.today()
    entries = _read_entries(since=today)
    total = sum(e["cost_usd"] for e in entries)
    by_agent = defaultdict(float)
    by_model = defaultdict(float)
    cached_count = 0
    for e in entries:
        by_agent[e["agent"]] += e["cost_usd"]
        by_model[e["model"]] += e["cost_usd"]
        if e.get("cached"):
            cached_count += 1
    return {
        "date": today.isoformat(),
        "total_usd": round(total, 4),
        "total_eur": round(total * EUR_USD_RATIO, 4),
        "n_calls": len(entries),
        "cache_hits": cached_count,
        "by_agent": dict(by_agent),
        "by_model": dict(by_model),
    }


def summary_month() -> dict:
    """Coste mes actual + proyección a mes completo."""
    today = date.today()
    first_day = today.replace(day=1)
    entries = _read_entries(since=first_day)
    total = sum(e["cost_usd"] for e in entries)
    days_elapsed = (today - first_day).days + 1
    days_in_month = 30
    proj_full = total / days_elapsed * days_in_month if days_elapsed else 0
    return {
        "month": today.strftime("%Y-%m"),
        "total_usd": round(total, 4),
        "total_eur": round(total * EUR_USD_RATIO, 4),
        "days_elapsed": days_elapsed,
        "n_calls": len(entries),
        "projection_usd": round(proj_full, 2),
        "projection_eur": round(proj_full * EUR_USD_RATIO, 2),
    }


def summary_by_agent(days: int = 30) -> list[dict]:
    """Breakdown por agente, últimos N días."""
    since = date.today() - timedelta(days=days)
    entries = _read_entries(since=since)
    agg = defaultdict(lambda: {"cost_usd": 0, "n_calls": 0, "tokens": 0})
    for e in entries:
        a = e["agent"]
        agg[a]["cost_usd"] += e["cost_usd"]
        agg[a]["n_calls"] += 1
        agg[a]["tokens"] += e.get("input_tok", 0) + e.get("output_tok", 0)
    return sorted(
        [{"agent": k, **v} for k, v in agg.items()],
        key=lambda x: -x["cost_usd"],
    )


def summary_by_model(days: int = 30) -> list[dict]:
    """Breakdown por modelo."""
    since = date.today() - timedelta(days=days)
    entries = _read_entries(since=since)
    agg = defaultdict(lambda: {"cost_usd": 0, "n_calls": 0, "tokens": 0})
    for e in entries:
        m = e["model"]
        agg[m]["cost_usd"] += e["cost_usd"]
        agg[m]["n_calls"] += 1
        agg[m]["tokens"] += e.get("input_tok", 0) + e.get("output_tok", 0)
    return sorted(
        [{"model": k, **v} for k, v in agg.items()],
        key=lambda x: -x["cost_usd"],
    )


def summary_by_category(days: int = 30) -> dict:
    """Breakdown por categoría de gasto (análisis de fondos vs imágenes) para el admin."""
    since = date.today() - timedelta(days=days)
    entries = _read_entries(since=since)
    agg = defaultdict(lambda: {"cost_usd": 0.0, "n_calls": 0, "tokens": 0})
    for e in entries:
        cat = e.get("categoria") or CAT_ANALISIS
        agg[cat]["cost_usd"] += e["cost_usd"]
        agg[cat]["n_calls"] += 1
        agg[cat]["tokens"] += e.get("input_tok", 0) + e.get("output_tok", 0)
    total = sum(v["cost_usd"] for v in agg.values())
    return {
        "days": days,
        "total_usd": round(total, 4),
        "total_eur": round(total * EUR_USD_RATIO, 4),
        "categorias": {
            k: {"cost_usd": round(v["cost_usd"], 4),
                "cost_eur": round(v["cost_usd"] * EUR_USD_RATIO, 4),
                "n_calls": v["n_calls"], "tokens": v["tokens"],
                "pct": round(100 * v["cost_usd"] / total, 1) if total else 0}
            for k, v in sorted(agg.items(), key=lambda x: -x[1]["cost_usd"])
        },
    }


def by_month(months: int = 12) -> list[dict]:
    """Coste por mes natural (todos los meses del log), con desglose por categoría.
    Nivel MENSUAL que pide el admin."""
    entries = _read_entries()
    # por categoría guardamos coste Y n_calls (para que cost_month no duplique el total del
    # mes en cada fila categoría → sumar por categorías daba doble/triple conteo).
    agg = defaultdict(lambda: {"cost_usd": 0.0, "n_calls": 0,
                               "cat_cost": defaultdict(float), "cat_calls": defaultdict(int)})
    for e in entries:
        mes = (e.get("date") or "")[:7]  # YYYY-MM
        if not mes:
            continue
        cat = e.get("categoria") or CAT_ANALISIS
        agg[mes]["cost_usd"] += e["cost_usd"]
        agg[mes]["n_calls"] += 1
        agg[mes]["cat_cost"][cat] += e["cost_usd"]
        agg[mes]["cat_calls"][cat] += 1
    out = [{"mes": m,
            "cost_usd": round(v["cost_usd"], 4),
            "cost_eur": round(v["cost_usd"] * EUR_USD_RATIO, 4),
            "n_calls": v["n_calls"],
            "por_categoria": {k: round(c, 4) for k, c in v["cat_cost"].items()},
            "n_calls_por_categoria": dict(v["cat_calls"])}
           for m, v in agg.items()]
    return sorted(out, key=lambda x: x["mes"], reverse=True)[:months]


def by_fund(limit: int | None = None) -> list[dict]:
    """Coste acumulado (de por vida) por fondo. Nivel ANÁLISIS DE FONDO que pide el admin."""
    entries = _read_entries()
    agg = defaultdict(lambda: {"cost_usd": 0.0, "n_calls": 0, "tokens": 0,
                               "cat": defaultdict(float)})
    for e in entries:
        isin = (e.get("isin") or "").strip()
        if not isin:
            continue
        agg[isin]["cost_usd"] += e["cost_usd"]
        agg[isin]["n_calls"] += 1
        agg[isin]["tokens"] += e.get("input_tok", 0) + e.get("output_tok", 0)
        agg[isin]["cat"][e.get("categoria") or CAT_ANALISIS] += e["cost_usd"]
    out = [{"isin": i,
            "cost_usd": round(v["cost_usd"], 4),
            "cost_eur": round(v["cost_usd"] * EUR_USD_RATIO, 4),
            "n_calls": v["n_calls"], "tokens": v["tokens"],
            "por_categoria": {k: round(c, 4) for k, c in v["cat"].items()}}
           for i, v in agg.items()]
    out.sort(key=lambda x: -x["cost_usd"])
    return out[:limit] if limit else out


def admin_overview(days: int = 30) -> dict:
    """Vista única para el panel admin: categorías + agentes + modelos + hoy/mes."""
    return {
        "por_categoria": summary_by_category(days),
        "por_agente": summary_by_agent(days)[:15],
        "por_modelo": summary_by_model(days),
        "por_mes": by_month(12),
        "por_fondo": by_fund(limit=25),
        "hoy": summary_today(),
        "mes": summary_month(),
        "aviso_tracking": "Incluye solo llamadas instrumentadas. El saldo real está en console.anthropic.com.",
    }


def top_funds(n: int = 10, days: int = 30) -> list[dict]:
    """Top N fondos por coste acumulado."""
    since = date.today() - timedelta(days=days)
    entries = _read_entries(since=since)
    agg = defaultdict(lambda: {"cost_usd": 0, "n_calls": 0})
    for e in entries:
        isin = e.get("isin", "?") or "?"
        agg[isin]["cost_usd"] += e["cost_usd"]
        agg[isin]["n_calls"] += 1
    sorted_funds = sorted(agg.items(), key=lambda x: -x[1]["cost_usd"])[:n]
    return [{"isin": k, **v} for k, v in sorted_funds]


def check_alerts() -> list[str]:
    """Chequea si día u mes superan umbrales. Devuelve lista de strings warn."""
    alerts = []
    today_s = summary_today()
    month_s = summary_month()
    if today_s["total_usd"] > WARN_DAY_USD:
        alerts.append(
            f"⚠ Coste hoy: ${today_s['total_usd']:.2f} (>{WARN_DAY_USD}$ umbral)"
        )
    if month_s["total_usd"] > WARN_MONTH_USD:
        alerts.append(
            f"⚠ Coste mes: ${month_s['total_usd']:.2f} (>{WARN_MONTH_USD}$ umbral)"
        )
    return alerts


def print_session_summary() -> str:
    """Resumen breve para imprimir al final de cada pipeline run."""
    today_s = summary_today()
    month_s = summary_month()
    lines = [
        "Coste de esta sesión / día / mes",
        f"  Hoy: ${today_s['total_usd']:.2f} (~€{today_s['total_eur']:.2f}) | {today_s['n_calls']} calls ({today_s['cache_hits']} cached)",
        f"  Mes {month_s['month']}: ${month_s['total_usd']:.2f} (~€{month_s['total_eur']:.2f}) | proyección €{month_s['projection_eur']:.0f}",
    ]
    for a in check_alerts():
        lines.append(f"  {a}")
    return "\n".join(lines)


# ── CLI ──────────────────────────────────────────────────────────────────────

def _print_table(rows: list[dict], cols: list[str], title: str = ""):
    if not rows:
        print(f"{title}: (sin datos)")
        return
    if title:
        print(f"\n=== {title} ===")
    widths = {c: max(len(c), max(len(str(r.get(c, ""))) for r in rows)) for c in cols}
    print("  " + " | ".join(c.ljust(widths[c]) for c in cols))
    print("  " + "-+-".join("-" * widths[c] for c in cols))
    for r in rows:
        print("  " + " | ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))


def _main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = sys.argv[1:]
    if not args:
        # Resumen general default
        print(print_session_summary())
        print()
        print(_format_block(summary_by_agent(days=7), "Top agentes (7d)"))
        print()
        print(_format_block(summary_by_model(days=7), "Top modelos (7d)"))
        return
    if args[0] == "--today":
        s = summary_today()
        print(json.dumps(s, ensure_ascii=False, indent=2))
    elif args[0] == "--month":
        s = summary_month()
        print(json.dumps(s, ensure_ascii=False, indent=2))
    elif args[0] == "--by-agent":
        days = int(args[1]) if len(args) > 1 else 30
        rows = summary_by_agent(days=days)
        _print_table(rows, ["agent", "cost_usd", "n_calls", "tokens"],
                     title=f"Breakdown por agente ({days}d)")
    elif args[0] == "--by-model":
        days = int(args[1]) if len(args) > 1 else 30
        rows = summary_by_model(days=days)
        _print_table(rows, ["model", "cost_usd", "n_calls", "tokens"],
                     title=f"Breakdown por modelo ({days}d)")
    elif args[0] == "--by-category":
        days = int(args[1]) if len(args) > 1 else 30
        s = summary_by_category(days=days)
        rows = [{"categoria": k, **{kk: vv for kk, vv in v.items() if kk in ("cost_usd", "cost_eur", "n_calls", "pct")}}
                for k, v in s["categorias"].items()]
        _print_table(rows, ["categoria", "cost_usd", "cost_eur", "n_calls", "pct"],
                     title=f"Coste por categoría ({days}d) — total ${s['total_usd']}")
    elif args[0] == "--admin":
        print(json.dumps(admin_overview(days=int(args[1]) if len(args) > 1 else 30),
                         ensure_ascii=False, indent=2))
    elif args[0] == "--top-funds":
        n = int(args[1]) if len(args) > 1 else 10
        rows = top_funds(n=n)
        _print_table(rows, ["isin", "cost_usd", "n_calls"],
                     title=f"Top {n} fondos por coste (30d)")
    elif args[0] == "--since":
        since = date.fromisoformat(args[1])
        entries = _read_entries(since=since)
        total = sum(e["cost_usd"] for e in entries)
        print(f"Desde {since}: ${total:.4f} ({len(entries)} calls)")
    else:
        print(__doc__)


def _format_block(rows: list[dict], title: str) -> str:
    if not rows:
        return f"{title}: (sin datos)"
    lines = [f"=== {title} ==="]
    for r in rows[:8]:
        key = r.get("agent") or r.get("model") or r.get("isin", "?")
        lines.append(f"  {key:35s} ${r['cost_usd']:.4f}  ({r['n_calls']} calls)")
    return "\n".join(lines)


if __name__ == "__main__":
    _main()
