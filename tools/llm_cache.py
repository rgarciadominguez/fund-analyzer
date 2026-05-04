"""
LLM Cache — caché local persistente por hash(model + prompt + context).

Diseño:
- Cada llamada LLM genera key = sha256(model | prompt | context)
- Si key existe en cache + no expirada (TTL 24h por defecto) → devuelve cacheada (FREE)
- Si no → llama LLM, guarda result, devuelve

Storage: data/.llm_cache/<key>.json (cada entry self-contained con timestamp + result)
Global, NO por fondo (mismo prompt en distintos fondos comparte cache si contexto idéntico).

Uso:
    from tools.llm_cache import cached_llm_call

    result = cached_llm_call(
        model="claude-sonnet-4-6",
        prompt="...",
        context="...",
        ttl_hours=24,
        actual_call=lambda: anthropic_client.messages.create(...).content[0].text,
    )

Ahorro estimado: 60-80% en iteraciones de desarrollo donde mismo prompt se repite.

Fase Cost-Opt (2026-05-02): respuesta a usuario tras factura €100 inesperada Google.
"""
import hashlib
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Any

ROOT = Path(__file__).parent.parent
CACHE_DIR = ROOT / "data" / ".llm_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TTL_HOURS = 24
# Si LLM_CACHE_DISABLED=1 en env → no cachear (modo refresh)
DISABLED = os.environ.get("LLM_CACHE_DISABLED") == "1"


def _make_key(model: str, prompt: str, context: str = "") -> str:
    """Genera SHA256 de (model + prompt + context). Determinista."""
    payload = f"{model}|||{prompt}|||{context}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def _cache_path(key: str) -> Path:
    return CACHE_DIR / f"{key}.json"


def get_cached(model: str, prompt: str, context: str = "",
               ttl_hours: float = DEFAULT_TTL_HOURS) -> Any | None:
    """Devuelve resultado cacheado o None si no existe / expirado / disabled."""
    if DISABLED:
        return None
    key = _make_key(model, prompt, context)
    path = _cache_path(key)
    if not path.exists():
        return None
    try:
        entry = json.loads(path.read_text(encoding="utf-8"))
        age_hours = (time.time() - entry["ts"]) / 3600
        if age_hours > ttl_hours:
            return None
        return entry["result"]
    except Exception:
        return None


def set_cached(model: str, prompt: str, context: str, result: Any) -> None:
    """Guarda result en cache. result debe ser JSON-serializable."""
    if DISABLED:
        return
    key = _make_key(model, prompt, context)
    path = _cache_path(key)
    try:
        path.write_text(
            json.dumps({
                "ts": time.time(),
                "ts_iso": datetime.now().isoformat(),
                "model": model,
                "prompt_hash": hashlib.sha256(prompt.encode()).hexdigest()[:8],
                "context_hash": hashlib.sha256((context or "").encode()).hexdigest()[:8],
                "result": result,
            }, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception:
        pass  # cache failure no debe romper pipeline


def cached_llm_call(model: str, prompt: str, context: str,
                    actual_call: Callable[[], Any],
                    ttl_hours: float = DEFAULT_TTL_HOURS,
                    log_fn: Callable | None = None) -> Any:
    """Wrapper completo: si cache hit → devuelve cacheado. Si miss → ejecuta y cachea.

    Args:
        model: identificador del modelo (ej: "claude-sonnet-4-6", "gemini-2.5-flash")
        prompt: prompt completo enviado
        context: contexto/datos adicionales (incluido en hash)
        actual_call: función sin args que ejecuta la llamada real al LLM
        ttl_hours: cuántas horas vale la cache (default 24)
        log_fn: función opcional para loggear hit/miss

    Returns:
        Resultado del LLM (cacheado o fresco)
    """
    cached = get_cached(model, prompt, context, ttl_hours)
    if cached is not None:
        if log_fn:
            log_fn("CACHE_HIT", f"model={model}")
        return cached
    if log_fn:
        log_fn("CACHE_MISS", f"model={model} — calling LLM")
    result = actual_call()
    if result is not None:
        set_cached(model, prompt, context, result)
    return result


def clear_cache(older_than_days: float | None = None) -> int:
    """Limpia cache. Si older_than_days especificado, solo borra los más viejos."""
    deleted = 0
    cutoff = time.time() - (older_than_days * 86400) if older_than_days else None
    for f in CACHE_DIR.glob("*.json"):
        try:
            if cutoff:
                entry = json.loads(f.read_text(encoding="utf-8"))
                if entry.get("ts", 0) > cutoff:
                    continue
            f.unlink()
            deleted += 1
        except Exception:
            continue
    return deleted


def cache_stats() -> dict:
    """Stats del cache: count, size, edad media."""
    files = list(CACHE_DIR.glob("*.json"))
    if not files:
        return {"count": 0, "total_size_kb": 0, "oldest_age_hours": 0}
    total_size = sum(f.stat().st_size for f in files)
    now = time.time()
    ages = []
    for f in files:
        try:
            entry = json.loads(f.read_text(encoding="utf-8"))
            ages.append((now - entry.get("ts", now)) / 3600)
        except Exception:
            continue
    return {
        "count": len(files),
        "total_size_kb": round(total_size / 1024, 1),
        "oldest_age_hours": round(max(ages), 1) if ages else 0,
        "median_age_hours": round(sorted(ages)[len(ages) // 2], 1) if ages else 0,
    }


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    args = sys.argv[1:]
    if not args or args[0] in ("-h", "--help"):
        print(__doc__)
        print("\nCLI:")
        print("  python -m tools.llm_cache --stats          # estadísticas cache")
        print("  python -m tools.llm_cache --clear          # borrar todo")
        print("  python -m tools.llm_cache --clear-old N    # borrar más viejos que N días")
    elif args[0] == "--stats":
        s = cache_stats()
        print(f"Cache: {s['count']} entries, {s['total_size_kb']} KB, "
              f"oldest {s['oldest_age_hours']}h, median {s['median_age_hours']}h")
    elif args[0] == "--clear":
        n = clear_cache()
        print(f"Borradas {n} entries del cache")
    elif args[0] == "--clear-old":
        days = float(args[1]) if len(args) > 1 else 7
        n = clear_cache(older_than_days=days)
        print(f"Borradas {n} entries más viejas que {days} días")
