"""
Gestora Resources Extractor v2 — M3 v2 Fase M (2026-04-30).

PIVOT (vs v1): en lugar de scrape HTML directo de página producto (que falla en
SPAs JS-heavy como Cobas/Bestinver/Renta 4 y depende de adivinar URL exacta),
usar **Google/Serper** para descubrir PDFs públicos hosteados en el dominio
de la gestora. Google indexa todos los PDFs públicos aunque la SPA no los
enlace en HTML inicial.

Por fondo: 5-7 queries Serper. Coste ~$0.01/fondo (con 50€ → ~7000 fondos).

Categorización igual que v1: por extensión + keywords del título.

Output: `data/funds/{ISIN}/gestora_resources.json` con
`[{tipo, titulo, url, fecha?, fuente_query?}]`.

Integración pendiente con orchestrator + analyst.
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse


# Categorización por keywords del texto del enlace (sin cambios vs v1)
DOC_CATEGORIES = {
    "annual_report":   r"annual\s*report|informe\s+anual|memoria\s+anual|memoria\s+\d{4}",
    "semestral":       r"semestral|half[\-\s]?year|informe\s+semestral",
    "carta_gestor":    r"\bcarta(?:s)?\b|trimestral|Q[1-4]\b|cuarto\s+trimestre|primer\s+trimestre|carta\s+a\s+(?:los\s+)?inversores|newsletter",
    "presentacion":    r"presentaci[oó]n|presentation|\bdeck\b|slides|webinar",
    "kiid":            r"\bKIID\b|\bDFI\b|\bDIC\b|datos\s+fundamentales|key\s+investor\s+information",
    "factsheet":       r"factsheet|ficha\s+(?:t[eé]cnica|comercial)",
    "folleto":         r"\bfolleto\b|prospectus",
    "mensual":         r"\bmensual\b|\bmonthly\b",
    "comentario":      r"comentario|commentary|insight|opinion",
    "sostenibilidad":  r"sostenibilidad|sustainability|esg|sfdr",
}

VIDEO_HOSTS = ("youtube.com", "youtu.be", "vimeo.com")


class GestoraResourcesExtractor:
    """Extrae recursos públicos del fondo en web gestora vía Serper.

    M3 v2: pivot desde scrape HTML a Serper-based PDF discovery.
    """

    def __init__(self, isin: str, gestora_domain: Optional[str] = None,
                 fund_name: str = "", gestora: str = "",
                 fund_url: Optional[str] = None):
        """
        Args:
            isin: ISIN del fondo
            gestora_domain: dominio principal de la gestora (ej: 'magallanesvalue.com').
                Si no se pasa, se intenta derivar de fund_url.
            fund_name: nombre del fondo (usado en queries Serper)
            gestora: nombre de la gestora (usado para query video YouTube)
            fund_url: opcional — URL página producto (para fallback / scrape directo)
        """
        self.isin = isin.upper().strip()
        self.fund_name = fund_name
        self.gestora = gestora
        self.fund_url = fund_url
        # Derivar dominio si no se pasa
        if gestora_domain:
            self.gestora_domain = gestora_domain.lower().replace("www.", "")
        elif fund_url:
            self.gestora_domain = self._domain_of(fund_url).replace("www.", "")
        else:
            self.gestora_domain = ""
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

    def _log(self, level: str, msg: str):
        safe = msg.encode("cp1252", errors="replace").decode("cp1252")
        print(f"[GESTORA_RES] [{level}] {safe}", flush=True)

    async def run(self) -> dict:
        """Ejecuta extracción vía Serper. Devuelve dict con recursos categorizados."""
        if not self.gestora_domain:
            self._log("WARN", "Sin gestora_domain — skip extracción")
            return self._empty_result(reason="no_gestora_domain")

        if not self.fund_name:
            self._log("WARN", "Sin fund_name — skip extracción (queries serían genéricas)")
            return self._empty_result(reason="no_fund_name")

        try:
            # 1. Búsquedas Serper específicas por categoría de doc
            queries = self._build_queries()
            self._log("INFO",
                f"Lanzando {len(queries)} queries Serper en domain={self.gestora_domain!r}")

            from tools.google_search import SearchEngine
            search = SearchEngine(self.isin)
            search_results = await search.search_multiple(
                queries, num_per_query=8, agent="gestora_resources_extractor"
            )
            self._log("INFO", f"Serper devolvió {len(search_results)} resultados raw")

            # 2. Filtrar y categorizar
            recursos = self._categorize_search_results(search_results)
            recursos_dedup = self._dedup(recursos)
            self._log("OK", f"Total recursos extraídos: {len(recursos_dedup)}")

            return self._save(recursos_dedup)
        except Exception as exc:
            self._log("ERROR", f"Run falló: {exc}")
            return self._empty_result(reason=f"exception: {exc}")

    def _build_queries(self) -> list[str]:
        """Genera queries Serper específicas para descubrir PDFs y videos.

        Estrategia: 5 queries cubren las categorías principales (anual, carta,
        KIID/folleto, semestral, factsheet) + 1 query para videos YouTube.
        """
        domain = self.gestora_domain
        # Nombre del fondo limpio (sin sufijo legal redundante en quotas)
        fund = self.fund_name.strip()
        # Quita sufijos demasiado genéricos para no rebajar recall
        fund_short = re.sub(
            r"\s*,?\s*(FI|FCR|FIL|SICAV|S\.?A\.?)\s*$", "", fund, flags=re.IGNORECASE
        ).strip()

        queries = [
            # 1. Cualquier PDF del fondo
            f'site:{domain} "{fund_short}" filetype:pdf',
            # 2. Cartas trimestrales / newsletters
            f'site:{domain} "{fund_short}" (carta OR trimestral OR newsletter) filetype:pdf',
            # 3. KIID / DFI / Folleto (documentos legales)
            f'site:{domain} "{fund_short}" (KIID OR DFI OR folleto OR prospectus) filetype:pdf',
            # 4. Semestral / Anual / Memoria (informes oficiales)
            f'site:{domain} "{fund_short}" (semestral OR anual OR memoria) filetype:pdf',
            # 5. Factsheet / Ficha
            f'site:{domain} "{fund_short}" (factsheet OR ficha OR mensual) filetype:pdf',
        ]
        # 6. Videos YouTube de la gestora sobre el fondo
        if self.gestora:
            gestora_token = self.gestora.split()[0]
            queries.append(
                f'site:youtube.com "{gestora_token}" "{fund_short}"'
            )
        return queries

    def _categorize_search_results(self, results: list[dict]) -> list[dict]:
        """Filtra (PDFs + videos del dominio gestora) + categoriza."""
        recursos = []
        for r in results:
            url = r.get("url", "") or r.get("link", "")
            if not url:
                continue
            title = r.get("title", "") or r.get("titulo", "")
            snippet = r.get("snippet", "") or r.get("description", "")
            url_lower = url.lower()
            domain = self._domain_of(url)

            # PDF en dominio gestora (o subdominio)
            is_pdf = url_lower.endswith(".pdf") or ".pdf?" in url_lower or ".pdf#" in url_lower
            in_gestora_domain = self.gestora_domain in domain
            is_video = any(vh in domain for vh in VIDEO_HOSTS)

            if is_pdf and in_gestora_domain:
                tipo = self._categorize(title + " " + snippet + " " + url_lower)
                recursos.append({
                    "tipo": tipo,
                    "titulo": title[:200] if title else self._title_from_url(url),
                    "url": url,
                    "fecha": self._extract_date(title + " " + snippet + " " + url_lower),
                    "fuente": self.gestora_domain,
                })
            elif is_video:
                # Filtro mínimo: el título o snippet debe mencionar el fondo
                fund_in_text = self.fund_name.lower().split()[0] in (title + snippet).lower()
                if fund_in_text or self.gestora.split()[0].lower() in (title + snippet).lower():
                    recursos.append({
                        "tipo": "video",
                        "titulo": title[:200] if title else self._title_from_url(url),
                        "url": url,
                        "fecha": self._extract_date(title + " " + snippet),
                        "fuente": domain,
                    })
        return recursos

    def _domain_of(self, url: str) -> str:
        try:
            return urlparse(url).netloc.lower()
        except Exception:
            return ""

    def _categorize(self, text: str) -> str:
        """Match keywords contra texto + URL para clasificar."""
        text_lower = text.lower()
        for tipo, pattern in DOC_CATEGORIES.items():
            if re.search(pattern, text_lower, re.IGNORECASE):
                return tipo
        return "documento_otro"

    def _extract_date(self, text: str) -> str:
        m = re.search(r"(20\d{2})(?:[-/_](\d{1,2}))?", text)
        if m:
            year = m.group(1)
            month = m.group(2)
            try:
                return f"{year}-{int(month):02d}" if month else year
            except ValueError:
                return year
        return ""

    def _title_from_url(self, url: str) -> str:
        try:
            path = urlparse(url).path
            name = Path(path).stem
            return name.replace("-", " ").replace("_", " ").title()[:200]
        except Exception:
            return url[:200]

    def _dedup(self, recursos: list[dict]) -> list[dict]:
        seen = set()
        out = []
        for r in recursos:
            url = r.get("url", "")
            if url and url not in seen:
                seen.add(url)
                out.append(r)
        return out

    def _save(self, recursos: list[dict]) -> dict:
        out_path = self.fund_dir / "gestora_resources.json"
        from collections import Counter
        tipos = Counter(r.get("tipo", "?") for r in recursos)
        result = {
            "isin": self.isin,
            "fund_name": self.fund_name,
            "gestora": self.gestora,
            "gestora_domain": self.gestora_domain,
            "generated": datetime.now().isoformat(),
            "total": len(recursos),
            "por_tipo": dict(tipos),
            "recursos": recursos,
        }
        out_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        self._log("OK", f"Guardado: {out_path.name} ({len(recursos)} recursos, tipos={dict(tipos)})")
        return result

    def _empty_result(self, reason: str) -> dict:
        return {
            "isin": self.isin,
            "fund_name": self.fund_name,
            "gestora": self.gestora,
            "gestora_domain": self.gestora_domain,
            "generated": datetime.now().isoformat(),
            "total": 0,
            "por_tipo": {},
            "recursos": [],
            "skip_reason": reason,
        }


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()
    isin = sys.argv[1] if len(sys.argv) > 1 else "ES0159259011"
    domain = sys.argv[2] if len(sys.argv) > 2 else "magallanesvalue.com"
    fund = sys.argv[3] if len(sys.argv) > 3 else "Magallanes European Equity"
    gestora = sys.argv[4] if len(sys.argv) > 4 else "Magallanes Value Investors"
    extractor = GestoraResourcesExtractor(
        isin=isin, gestora_domain=domain, fund_name=fund, gestora=gestora
    )
    result = asyncio.run(extractor.run())
    print(f"\nResultado: {result['total']} recursos")
    for r in result["recursos"][:15]:
        print(f"  [{r['tipo']:15s}] {r['titulo'][:60]:60s} ({r.get('fecha','')})")
