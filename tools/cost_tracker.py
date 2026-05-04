"""
Cost tracker — registra tokens y coste $ por llamada LLM.
Aditivo, no requiere modificar agentes existentes excepto envolver llamadas.

Uso:
    from tools.cost_tracker import track, get_summary, save_to_meta

    # Wrap Anthropic call
    resp = client.messages.create(...)
    track(isin, model="claude-sonnet-4-5", input_tokens=resp.usage.input_tokens,
          output_tokens=resp.usage.output_tokens, agent="analyst")

    # Wrap Gemini call
    resp = client.models.generate_content(...)
    track(isin, model="gemini-2.5-flash",
          input_tokens=resp.usage_metadata.prompt_token_count,
          output_tokens=resp.usage_metadata.candidates_token_count,
          agent="analyst")

    # Al final del pipeline
    save_to_meta(isin, fund_dir)

Precios actualizados a 2026-04 (USD por MTok):
- Claude Opus 4 / 4.7: $15 in / $75 out
- Claude Sonnet 4.5: $3 in / $15 out
- Claude Haiku 4.5: $1 in / $5 out
- Gemini 2.5 Pro: $1.25 in / $10 out
- Gemini 2.5 Flash: $0.30 in / $2.50 out (con thinking) o $0.075 in / $0.30 out (estandar)
- Gemini 2.5 Flash-Lite: $0.10 in / $0.40 out
"""
import json
from datetime import datetime
from pathlib import Path
from threading import Lock

# USD per Million tokens
PRICES = {
    # Anthropic
    "claude-opus-4": (15.00, 75.00),
    "claude-opus-4-7": (15.00, 75.00),
    "claude-opus-4-7[1m]": (15.00, 75.00),
    "claude-opus-4-20250514": (15.00, 75.00),
    "claude-sonnet-4-5": (3.00, 15.00),
    "claude-sonnet-4-5-20241022": (3.00, 15.00),
    "claude-sonnet-4-6": (3.00, 15.00),
    "claude-haiku-4-5": (1.00, 5.00),
    "claude-haiku-4-5-20251001": (1.00, 5.00),
    # Gemini
    "gemini-2.5-pro": (1.25, 10.00),
    "gemini-2.5-flash": (0.30, 2.50),  # con thinking; sin thinking sería $0.075/0.30
    "gemini-2.5-flash-lite": (0.10, 0.40),
    "gemini-pro": (1.25, 10.00),  # alias
    "gemini-flash": (0.30, 2.50),  # alias
}

# Estado global por ISIN
_TRACK: dict = {}
_LOCK = Lock()


def _normalize_model(model: str) -> str:
    """Normaliza el nombre de modelo a una key conocida."""
    m = (model or "").lower().strip()
    if m in PRICES:
        return m
    # Match por prefijo
    for k in PRICES:
        if m.startswith(k.lower()):
            return k
    # Match por substring
    if "opus" in m:
        return "claude-opus-4"
    if "sonnet" in m:
        return "claude-sonnet-4-5"
    if "haiku" in m:
        return "claude-haiku-4-5"
    if "flash-lite" in m:
        return "gemini-2.5-flash-lite"
    if "flash" in m:
        return "gemini-2.5-flash"
    if "gemini" in m and "pro" in m:
        return "gemini-2.5-pro"
    return m  # devuelve original; cost será 0


def calc_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Coste USD para una llamada."""
    key = _normalize_model(model)
    prices = PRICES.get(key)
    if not prices:
        return 0.0
    pi, po = prices
    return (input_tokens / 1_000_000) * pi + (output_tokens / 1_000_000) * po


def track(isin: str, model: str, input_tokens: int = 0,
          output_tokens: int = 0, agent: str = "unknown") -> dict:
    """Registra una llamada LLM. Devuelve el dict actualizado del ISIN.

    Cost-Opt Fase 2 (2026-05-02): además del cost_report.json por fondo,
    también escribe append-only al cost_log.jsonl global vía cost_monitor.
    Esto da visibilidad TOTAL del coste por fondo (no solo analyst).
    """
    if not isin:
        return {}
    isin = isin.strip().upper()
    cost = calc_cost(model, input_tokens, output_tokens)
    entry = {
        "ts": datetime.now().isoformat(),
        "model": model,
        "agent": agent,
        "input_tokens": int(input_tokens or 0),
        "output_tokens": int(output_tokens or 0),
        "cost_usd": round(cost, 6),
    }
    with _LOCK:
        d = _TRACK.setdefault(isin, {
            "calls": [],
            "by_agent": {},
            "by_model": {},
            "total_input": 0,
            "total_output": 0,
            "total_cost_usd": 0.0,
        })
        d["calls"].append(entry)
        d["total_input"] += entry["input_tokens"]
        d["total_output"] += entry["output_tokens"]
        d["total_cost_usd"] = round(d["total_cost_usd"] + cost, 6)
        a = d["by_agent"].setdefault(agent, {"calls": 0, "input": 0, "output": 0, "cost_usd": 0.0})
        a["calls"] += 1
        a["input"] += entry["input_tokens"]
        a["output"] += entry["output_tokens"]
        a["cost_usd"] = round(a["cost_usd"] + cost, 6)
        m = d["by_model"].setdefault(_normalize_model(model), {"calls": 0, "input": 0, "output": 0, "cost_usd": 0.0})
        m["calls"] += 1
        m["input"] += entry["input_tokens"]
        m["output"] += entry["output_tokens"]
        m["cost_usd"] = round(m["cost_usd"] + cost, 6)
    # Cost-Opt Fase 2: append al log global (visibilidad TOTAL por fondo)
    try:
        from tools.cost_monitor import log_call
        log_call(agent=agent, model=model, isin=isin,
                 input_tokens=int(input_tokens or 0),
                 output_tokens=int(output_tokens or 0),
                 cost_usd=cost)
    except Exception:
        pass  # nunca rompe el caller
    return d


def get_summary(isin: str) -> dict:
    """Devuelve el resumen actual de un ISIN."""
    isin = (isin or "").strip().upper()
    with _LOCK:
        return _TRACK.get(isin, {})


def save_to_meta(isin: str, fund_dir=None) -> bool:
    """Guarda el tracking en {fund_dir}/cost_report.json y lo añade a meta_report.json si existe."""
    isin = isin.strip().upper()
    summary = get_summary(isin)
    if not summary:
        return False
    fund_dir = Path(fund_dir) if fund_dir else Path(__file__).parent.parent / "data" / "funds" / isin
    fund_dir.mkdir(parents=True, exist_ok=True)

    cost_path = fund_dir / "cost_report.json"
    cost_path.write_text(
        json.dumps({"isin": isin, "generado": datetime.now().isoformat(), **summary},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Añadir summary compacto al meta_report.json
    meta_path = fund_dir / "meta_report.json"
    meta = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    meta["cost"] = {
        "total_usd": summary.get("total_cost_usd", 0),
        "total_calls": len(summary.get("calls", [])),
        "input_tokens": summary.get("total_input", 0),
        "output_tokens": summary.get("total_output", 0),
        "by_agent": {a: round(v["cost_usd"], 4) for a, v in summary.get("by_agent", {}).items()},
        "by_model": {m: round(v["cost_usd"], 4) for m, v in summary.get("by_model", {}).items()},
    }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def reset(isin: str = None):
    """Reset tracking para un ISIN o todos."""
    with _LOCK:
        if isin:
            _TRACK.pop(isin.strip().upper(), None)
        else:
            _TRACK.clear()


if __name__ == "__main__":
    # Smoke test
    track("TEST", "claude-sonnet-4-5", 10000, 2000, "analyst")
    track("TEST", "gemini-2.5-flash", 50000, 5000, "extractor")
    track("TEST", "claude-opus-4", 5000, 1000, "quality")
    print(json.dumps(get_summary("TEST"), indent=2))
