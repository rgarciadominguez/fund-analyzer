"""
URL template learning.

Cuando una gestora usa URL templates consistentes (ej. DNCA:
`{base}/funds/{fund-slug}/parts/{share-slug}/{TYPE}-{share-slug}/download_doc_share`),
y ya hemos descargado exitosamente 2+ tipos distintos, derivamos el template
y PROBAMOS los tipos que faltan (AR si no lo tenemos, etc.).

100% genérico: aplica a cualquier gestora que use patrón `/{TYPE}-{identifier}/`
donde TYPE es una abreviatura de documento.
"""
from __future__ import annotations

import re
from collections import Counter
from urllib.parse import urlparse

import httpx
from rich.console import Console

from agents.discovery import kb as kb_mod
from agents.discovery.downloader import download_and_register
from agents.discovery.state import SharedState

console = Console()


# Abreviaturas industriales conocidas y su doc_type
_TYPE_PREFIX_MAP = {
    "AR": "annual_report",
    "ANNREP": "annual_report",
    "SAR": "semi_annual_report",
    "SEMIREP": "semi_annual_report",
    "MR": "factsheet",
    "PRS": "prospectus",
    "PR": "prospectus",
    "PRSEN": "prospectus",
    "PROSPECTUS": "prospectus",
    "KID": "kid",
    "KIID": "kid",
    "LETTER": "quarterly_letter",
    "QR": "quarterly_letter",
}


def extract_template(url: str) -> tuple[str, str, str] | None:
    """
    Extrae template de URL con placeholder en el TYPE prefix.
    Devuelve (template_sin_query, prefix_detectado, query_sample).

    El template se normaliza SIN query string para que URLs del mismo
    endpoint con locales distintos se agrupen.

    Ej: `https://x.com/parts/i-lu1/MR-i-lu1/download?locale=uk` →
        template: `https://x.com/parts/i-lu1/{TYPE}-i-lu1/download`
        prefix:   `MR`
        query:    `?locale=uk`
    """
    m = re.search(r"/([A-Z]{2,10})-([a-z0-9\-]+)/([^?]*)", url)
    if not m:
        return None
    prefix = m.group(1)
    if prefix not in _TYPE_PREFIX_MAP:
        return None
    # Template SIN query (para agrupar URLs equivalentes con distintos locales)
    template = url[:m.start()] + f"/{{TYPE}}-{m.group(2)}/{m.group(3)}"
    query = url[url.index("?"):] if "?" in url else ""
    return (template, prefix, query)


def apply_template(template: str, prefix: str, query: str = "") -> str:
    return template.replace("{TYPE}", prefix) + query


async def learn_and_enumerate(
    state: SharedState,
    c: httpx.AsyncClient,
) -> None:
    """
    Examina los docs ya descargados, detecta templates con múltiples tipos
    exitosos, y prueba los tipos de doc que aún nos faltan.

    Ejemplo DNCA: tenemos MR, SAR, PRS, LETTER. Falta AR/KID. Derivar
    `/AR-i-lu.../...` y `/KID-i-lu.../...` y probar.
    """
    # Agrupar docs por template base (sin query)
    templates_count: Counter = Counter()
    templates_types: dict[str, set] = {}    # template → set of (prefix, doc_type)
    templates_query: dict[str, str] = {}    # template → sample query (para re-utilizarlo)

    for doc in state.downloaded_docs:
        if not doc.validated:
            continue
        result = extract_template(doc.url)
        if not result:
            continue
        tmpl, prefix, query = result
        templates_count[tmpl] += 1
        templates_types.setdefault(tmpl, set()).add((prefix, doc.doc_type))
        # Guardamos un query sample para re-utilizarlo al inferir
        if tmpl not in templates_query:
            templates_query[tmpl] = query

    # Aprender templates con ≥1 tipo exitoso. Con 1 solo podemos probar
    # prefijos industriales (AR/SAR/PRSEN/KID/LETTER/VKP) sobre el mismo
    # template — común en CDNs como im.natixis.com.
    for tmpl, count in templates_count.most_common():
        if count < 1:
            continue
        seen_prefixes = {p for p, _ in templates_types[tmpl]}
        seen_doctypes = {dt for _, dt in templates_types[tmpl]}
        query_sample = templates_query.get(tmpl, "")

        console.log(f"[bold magenta]Template aprendido:[/bold magenta] "
                    f"{len(seen_prefixes)} prefijos ({', '.join(sorted(seen_prefixes))})")

        missing_doctypes = {"annual_report", "semi_annual_report", "factsheet",
                            "prospectus", "kid", "quarterly_letter"} - seen_doctypes
        console.log(f"[dim]Template — missing doctypes: {sorted(missing_doctypes)}[/dim]")

        for dt_missing in missing_doctypes:
            try:
                # strict=True: solo cuenta si tenemos un doc DIRECTAMENTE del tipo
                # (no por mención en el texto de otro doc).
                if state.coverage(dt_missing, "", strict=True):
                    console.log(f"[dim]Template — {dt_missing} already covered, skip[/dim]")
                    continue
                candidate_prefixes = [p for p, d in _TYPE_PREFIX_MAP.items() if d == dt_missing]
                candidate_prefixes = [p for p in candidate_prefixes if p not in seen_prefixes]
                console.log(f"[dim]Template — {dt_missing}: trying prefixes {candidate_prefixes}[/dim]")

                for prefix in candidate_prefixes:
                    if state.budget.download_remaining <= 0:
                        console.log(f"[yellow]Template — budget exhausted[/yellow]")
                        return
                    candidate_url = apply_template(tmpl, prefix, query_sample)
                    if state.already_downloaded(candidate_url):
                        continue
                    console.log(f"[magenta]url-template try: {prefix} -> {dt_missing}[/magenta]")
                    doc = await download_and_register(
                        state, c, candidate_url, dt_missing, "",
                        source="url_template_learn",
                        source_detail=f"inferred from {len(seen_prefixes)} prefixes",
                    )
                    if doc:
                        kb_mod.remember(state.kb, doc.doc_type, doc.periodo, doc.url)
                        console.log(f"[bold green]template hit: {doc.doc_type}[/bold green] <- {prefix}")
                        break
            except Exception as exc:
                console.log(f"[red]Template — error processing {dt_missing}: {exc}[/red]")
                import traceback
                console.log(traceback.format_exc())
