"""CLI de análisis del log global de feedback humano (T3.11, 2026-05-28).

Lee `data/feedback_global.jsonl` y agrupa los items por patrón para sugerir
mejoras al pipeline. NO auto-aplica nada — la revisión humana decide qué
commitear como mejora estructural.

Uso:
    python -m tools.feedback_review
    python -m tools.feedback_review --by-gestora
    python -m tools.feedback_review --since 2026-05-01
    python -m tools.feedback_review --unresolved-only
    python -m tools.feedback_review --suggestions    # solo sugerencias accionables

Categorías de agrupación implementadas:
  - por target_path: qué campos del schema fallan más
  - por isin_prefix: qué reguladores/países tienen más problemas
  - por gestora: qué gestoras concentran más feedback
  - por action: qué tipo de corrección es más frecuente
  - por URL host: qué sitios web aparecen como fuentes adicionales útiles

Output formato texto plano + opcional JSON con --json.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
GLOBAL_LOG = ROOT / "data" / "feedback_global.jsonl"


def _load_log_entries() -> list[dict]:
    """Lee todas las líneas del log global."""
    if not GLOBAL_LOG.exists():
        return []
    out = []
    with GLOBAL_LOG.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def _filter_entries(
    entries: list[dict],
    since: str | None = None,
    isin: str | None = None,
    gestora: str | None = None,
    unresolved_only: bool = False,
) -> list[dict]:
    """Aplica filtros al log."""
    res = []
    # Compute resolved_set: feedback_ids+item_idx que tienen evento item_resolved
    resolved_pairs: set[tuple] = set()
    for e in entries:
        if e.get("event") == "item_resolved":
            resolved_pairs.add((e.get("feedback_id"), e.get("item_idx")))

    # Para isin/gestora/since: aplicamos a los entries data (no eventos)
    for e in entries:
        if e.get("event") == "item_resolved" or e.get("event") == "deleted":
            continue  # son eventos, no items
        if since and e.get("ts", "") < since:
            continue
        if isin and e.get("isin") != isin:
            continue
        if gestora:
            if (e.get("gestora") or "").lower() != gestora.lower():
                continue
        if unresolved_only:
            # Necesitamos saber el item_idx — no está en el log entry
            # Simplificación: si el entry tiene resolved=true → skip
            if e.get("resolved"):
                continue
        res.append(e)
    return res


def _group_count(entries: list[dict], key_fn) -> Counter:
    c = Counter()
    for e in entries:
        k = key_fn(e)
        if k is None:
            continue
        c[k] += 1
    return c


def _suggestions(entries: list[dict]) -> list[str]:
    """Genera sugerencias accionables basadas en patrones del log."""
    out: list[str] = []
    n_total = len(entries)
    if n_total == 0:
        return ["(sin entries — nada que sugerir)"]

    # 1) target_path repetidos: si nombre==ISIN problema, name_recovery puede
    #    estar fallando para ciertos prefijos
    by_target = Counter(e.get("target_path") for e in entries if e.get("target_path"))
    top_targets = by_target.most_common(5)
    for tgt, n in top_targets:
        if n >= 3:
            # Per isin_prefix
            prefixes = Counter(
                e.get("isin_prefix")
                for e in entries
                if e.get("target_path") == tgt and e.get("isin_prefix")
            )
            top_prefix = prefixes.most_common(1)
            prefix_note = f" (concentrado en prefijo {top_prefix[0][0]}: {top_prefix[0][1]} casos)" if top_prefix else ""
            out.append(
                f"Campo `{tgt}` corregido {n} veces{prefix_note}. "
                f"Considera reforzar el agente que lo produce o añadir regla "
                f"a tools/name_recovery.py / dashboard_quality_agent."
            )

    # 2) gestoras con muchos feedbacks
    by_gestora = Counter(
        e.get("gestora") for e in entries
        if e.get("gestora") and e.get("gestora").strip()
    )
    for gest, n in by_gestora.most_common(3):
        if n >= 4:
            out.append(
                f"Gestora «{gest}»: {n} items de feedback. Probable problema "
                f"de discovery/extractor — revisar gestoras_registry.json para "
                f"esa gestora o añadir entry específica."
            )

    # 3) URLs fuente que aparecen >=2 veces para misma gestora → candidato
    #    a html_fallback_useful_domains
    url_per_gestora: dict[str, Counter] = defaultdict(Counter)
    for e in entries:
        gest = (e.get("gestora") or "").strip()
        if not gest:
            continue
        for u in (e.get("source_urls") or []):
            try:
                from urllib.parse import urlparse
                host = urlparse(u).netloc.lower()
                if host:
                    url_per_gestora[gest][host] += 1
            except Exception:
                pass
    for gest, hosts in url_per_gestora.items():
        for host, n in hosts.most_common(2):
            if n >= 2:
                out.append(
                    f"URL host `{host}` referenciada {n}× para gestora «{gest}». "
                    f"Considera añadirla a gestoras_registry[{gest!r}]"
                    f".html_fallback_useful_domains."
                )

    # 4) action=revisar sobre la misma sección
    sec_revisar = Counter(
        e.get("target_section") for e in entries
        if e.get("action") == "revisar" and e.get("target_section")
    )
    for sec, n in sec_revisar.most_common(3):
        if n >= 3:
            out.append(
                f"Sección `{sec}` marcada como 'revisar' {n} veces. El analyst "
                f"está produciendo output insuficiente en esa sección — revisar "
                f"prompt de analyst-cowork SKILL.md para reforzar instrucciones."
            )

    if not out:
        out.append(f"({n_total} entries pero ningún patrón concentrado todavía. Sigue acumulando datos.)")

    return out


def _print_section(title: str, lines: list[str], indent: str = "  "):
    print(f"\n{'═' * 60}")
    print(f" {title}")
    print(f"{'═' * 60}")
    for line in lines:
        print(f"{indent}{line}")


def main():
    parser = argparse.ArgumentParser(
        description="Análisis del log global de feedback humano (T3.11)."
    )
    parser.add_argument("--since", help="ISO date YYYY-MM-DD: solo entries desde esa fecha")
    parser.add_argument("--isin", help="Filtrar a un ISIN concreto")
    parser.add_argument("--gestora", help="Filtrar a una gestora concreta")
    parser.add_argument("--unresolved-only", action="store_true",
                        help="Solo items no marcados como resolved")
    parser.add_argument("--by-gestora", action="store_true",
                        help="Agrupar por gestora")
    parser.add_argument("--by-target", action="store_true",
                        help="Agrupar por target_path")
    parser.add_argument("--by-prefix", action="store_true",
                        help="Agrupar por prefijo ISIN")
    parser.add_argument("--by-section", action="store_true",
                        help="Agrupar por sección del analyst")
    parser.add_argument("--suggestions", action="store_true",
                        help="Solo mostrar sugerencias accionables")
    parser.add_argument("--json", action="store_true",
                        help="Output como JSON en lugar de texto")
    args = parser.parse_args()

    entries = _load_log_entries()
    if not entries:
        print(f"[feedback_review] log vacío o no existe: {GLOBAL_LOG}")
        return 0

    entries = _filter_entries(
        entries, since=args.since, isin=args.isin,
        gestora=args.gestora, unresolved_only=args.unresolved_only,
    )
    n_items = len(entries)

    # Modo --suggestions: solo eso
    if args.suggestions:
        sugs = _suggestions(entries)
        if args.json:
            print(json.dumps({"n_entries": n_items, "suggestions": sugs}, ensure_ascii=False, indent=2))
        else:
            _print_section(f"Sugerencias accionables ({n_items} entries)", sugs)
        return 0

    # Modo agregado normal
    out_data = {
        "n_entries": n_items,
        "filters": {
            "since": args.since, "isin": args.isin,
            "gestora": args.gestora, "unresolved_only": args.unresolved_only,
        },
    }
    groups = []
    if args.by_gestora or not any([args.by_target, args.by_prefix, args.by_section]):
        c = _group_count(entries, lambda e: e.get("gestora") or "(sin gestora)")
        groups.append(("Top por gestora", c.most_common(15)))
        out_data["by_gestora"] = dict(c.most_common(20))
    if args.by_target or not any([args.by_gestora, args.by_prefix, args.by_section]):
        c = _group_count(entries, lambda e: e.get("target_path") or "(sin path)")
        groups.append(("Top por target_path", c.most_common(15)))
        out_data["by_target_path"] = dict(c.most_common(20))
    if args.by_prefix or not any([args.by_gestora, args.by_target, args.by_section]):
        c = _group_count(entries, lambda e: e.get("isin_prefix"))
        groups.append(("Por prefijo ISIN", c.most_common()))
        out_data["by_isin_prefix"] = dict(c)
    if args.by_section or not any([args.by_gestora, args.by_target, args.by_prefix]):
        c = _group_count(entries, lambda e: e.get("target_section") or "(sin section)")
        groups.append(("Top por sección analyst", c.most_common(10)))
        out_data["by_section"] = dict(c.most_common(15))

    # action breakdown siempre
    c = _group_count(entries, lambda e: e.get("action") or "(unknown)")
    groups.append(("Por tipo de action", c.most_common()))
    out_data["by_action"] = dict(c)

    sugs = _suggestions(entries)
    out_data["suggestions"] = sugs

    if args.json:
        print(json.dumps(out_data, ensure_ascii=False, indent=2))
    else:
        print(f"\n[feedback_review] {n_items} entries analizados\n")
        for title, items in groups:
            lines = [f"{n:>4} · {k}" for k, n in items if n > 0]
            if lines:
                _print_section(title, lines)
        _print_section("Sugerencias accionables", sugs)

    return 0


if __name__ == "__main__":
    sys.exit(main())
