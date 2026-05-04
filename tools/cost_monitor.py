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


def log_call(agent: str, model: str, isin: str = "",
             input_tokens: int = 0, output_tokens: int = 0,
             cost_usd: float = 0.0, cached: bool = False) -> None:
    """Append entry al log global. Idempotente y bajo overhead.

    Args:
        agent: nombre del agente que disparó la call (analyst, extractor, etc.)
        model: identificador del modelo usado
        isin: ISIN del fondo (opcional)
        input_tokens: tokens enviados
        output_tokens: tokens generados
        cost_usd: coste en USD (calcular antes de pasar)
        cached: True si fue un cache hit (cost=0 implícito)
    """
    try:
        entry = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "date": date.today().isoformat(),
            "agent": agent,
            "model": model,
            "isin": isin,
            "input_tok": input_tokens,
            "output_tok": output_tokens,
            "cost_usd": round(cost_usd, 6),
            "cached": cached,
        }
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass  # log failure no debe romper pipeline


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
