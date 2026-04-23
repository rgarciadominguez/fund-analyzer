"""
Readings Collector — Curador de analisis externos SOBRE el fondo.

Principio: busquedas DIRIGIDAS a sites especificos que sabemos que tienen
contenido de calidad. NO busquedas genericas de Google.

Pipeline:
  1. Busquedas dirigidas: site:astralisfundsacademy.com, site:trustnet.com,
     site:hl.co.uk, site:citywire.com, etc.
  2. Fetch cada URL encontrada y extraer texto completo
  3. Gemini Flash extrae contenido estructurado en espanol
  4. Guardar readings_data.json con contenido REAL (no solo URLs)

Diferencia vs letters_collector:
  - Letters = VOZ del gestor (primera persona, decisiones propias)
  - Readings = ANALISIS de terceros sobre el fondo (opinion externa)

Output: data/funds/{ISIN}/readings_data.json
"""
from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime
from pathlib import Path

import httpx
from rich.console import Console

console = Console()

# Sites con analisis de calidad — cobertura GLOBAL (ES + UK + US + EU)
# Criterio de inclusion: la plataforma publica analisis editoriales sobre fondos
# concretos (no solo fichas de producto). Ordenado por calidad editorial.
DIRECTED_SOURCES = [
    # ── GLOBALES: cubren fondos de cualquier jurisdiccion ──
    {
        "domain": "morningstar.com",
        "name": "Morningstar",
        "query_template": 'site:morningstar.com "{fund}" analyst OR analysis OR review',
        "type": "analisis",
        "quality": "high",
    },
    {
        "domain": "morningstar.co.uk",
        "name": "Morningstar UK",
        "query_template": 'site:morningstar.co.uk "{fund}" analysis OR review',
        "type": "analisis",
        "quality": "high",
    },
    {
        "domain": "citywire.com",
        "name": "Citywire",
        "query_template": 'site:citywire.com "{fund}" OR "{isin}"',
        "type": "analisis",
        "quality": "high",
    },
    {
        "domain": "ft.com",
        "name": "Financial Times",
        "query_template": 'site:ft.com "{fund}" OR "{gestora}"',
        "type": "analisis",
        "quality": "high",
    },
    {
        "domain": "fundspeople.com",
        "name": "FundsPeople",
        "query_template": 'site:fundspeople.com "{fund}" OR "{gestora}"',
        "type": "articulo",
        "quality": "high",
    },
    # ── UK: plataformas con analisis editoriales de fondos ──
    {
        "domain": "hl.co.uk",
        "name": "Hargreaves Lansdown",
        "query_template": 'site:hl.co.uk "{fund}" fund update OR commentary',
        "type": "fund_update",
        "quality": "high",
    },
    {
        "domain": "trustnet.com",
        "name": "Trustnet",
        "query_template": 'site:trustnet.com "{fund}" analysis OR review',
        "type": "analisis",
        "quality": "high",
    },
    {
        "domain": "ii.co.uk",
        "name": "Interactive Investor",
        "query_template": 'site:ii.co.uk "{fund}" review OR analysis',
        "type": "analisis",
        "quality": "medium",
    },
    {
        "domain": "moneyweek.com",
        "name": "MoneyWeek",
        "query_template": 'site:moneyweek.com "{fund}" OR "{gestora}"',
        "type": "analisis",
        "quality": "high",
    },
    {
        "domain": "yodelar.com",
        "name": "Yodelar",
        "query_template": 'site:yodelar.com "{fund}"',
        "type": "analisis",
        "quality": "medium",
    },
    {
        "domain": "bestinvest.co.uk",
        "name": "Bestinvest",
        "query_template": 'site:bestinvest.co.uk "{fund}" review',
        "type": "analisis",
        "quality": "medium",
    },
    {
        "domain": "monevator.com",
        "name": "Monevator",
        "query_template": 'site:monevator.com "{fund}" OR "{gestora}"',
        "type": "analisis",
        "quality": "medium",
    },
    # ── US: analisis de fondos UCITS/globales ──
    {
        "domain": "seekingalpha.com",
        "name": "Seeking Alpha",
        "query_template": 'site:seekingalpha.com "{fund}" OR "{gestora}"',
        "type": "analisis",
        "quality": "high",
    },
    {
        "domain": "barrons.com",
        "name": "Barron's",
        "query_template": 'site:barrons.com "{fund}" OR "{gestora}"',
        "type": "analisis",
        "quality": "high",
    },
    {
        "domain": "institutionalinvestor.com",
        "name": "Institutional Investor",
        "query_template": 'site:institutionalinvestor.com "{fund}" OR "{gestora}"',
        "type": "analisis",
        "quality": "high",
    },
    # ── EU: medios financieros continentales ──
    {
        "domain": "morningstar.fr",
        "name": "Morningstar FR",
        "query_template": 'site:morningstar.fr "{fund}" OR "{isin}"',
        "type": "analisis",
        "quality": "medium",
    },
    {
        "domain": "morningstar.de",
        "name": "Morningstar DE",
        "query_template": 'site:morningstar.de "{fund}" OR "{isin}"',
        "type": "analisis",
        "quality": "medium",
    },
    {
        "domain": "quantalys.com",
        "name": "Quantalys",
        "query_template": 'site:quantalys.com "{fund}" OR "{isin}"',
        "type": "analisis",
        "quality": "medium",
    },
    # ── ES: nicho espanol ──
    {
        "domain": "astralisfundsacademy.com",
        "name": "Astralis Funds Academy",
        "query_template": 'site:astralisfundsacademy.com "{fund}"',
        "type": "analisis_completo",
        "quality": "high",
    },
    {
        "domain": "saludfinanciera.substack.com",
        "name": "Salud Financiera",
        "query_template": 'site:saludfinanciera.substack.com "{fund}"',
        "type": "analisis_completo",
        "quality": "high",
    },
    {
        "domain": "masdividendos.com",
        "name": "Mas Dividendos",
        "query_template": 'site:masdividendos.com "{fund}"',
        "type": "comunidad",
        "quality": "medium",
    },
    {
        "domain": "rankia.com",
        "name": "Rankia",
        "query_template": 'site:rankia.com "{fund}" OR "{gestora}" analisis OR opinion',
        "type": "comunidad",
        "quality": "medium",
    },
    {
        "domain": "finect.com",
        "name": "Finect",
        "query_template": 'site:finect.com "{fund}" OR "{isin}"',
        "type": "articulo",
        "quality": "medium",
    },
    # ── Video/Podcast ──
    {
        "domain": "youtube.com",
        "name": "YouTube",
        "query_template": '"{fund}" OR "{gestora}" interview OR podcast OR webinar',
        "type": "video_entrevista",
        "quality": "medium",
    },
]

# Schema para extraccion de readings
READING_SCHEMA = {
    "titulo": "str - titulo del articulo/analisis",
    "autor": "str - nombre del autor si aparece",
    "fecha": "str - fecha de publicacion (YYYY-MM-DD o YYYY-MM)",
    "tipo": "str - analisis_completo | fund_update | opinion | entrevista | resena",
    "resumen": "str - resumen ejecutivo del contenido en 3-5 frases (ESPANOL)",
    "puntos_clave": ["str - punto clave sobre el fondo"],
    "opinion_sobre_fondo": "str - opinion general del autor sobre el fondo (positiva/negativa/neutral + por que)",
    "datos_mencionados": {
        "aum": "str - si menciona patrimonio",
        "rentabilidad": "str - si menciona performance",
        "rating": "str - si menciona rating Morningstar/Citywire",
        "comisiones": "str - si menciona fees",
        "gestores": ["str - gestores mencionados"],
    },
    "citas_relevantes": ["str - frases literales del texto relevantes"],
}


class ReadingsCollector:
    """Curador de analisis externos — busquedas dirigidas + extraccion profunda."""

    def __init__(self, isin: str, fund_name: str = "", gestora: str = "",
                 gestores: list[str] | None = None):
        self.isin = isin.upper().strip()
        self.fund_name = fund_name
        self.fund_short = fund_name.split(" - ")[-1] if " - " in fund_name else fund_name
        self.gestora = gestora
        self.gestores = gestores or []
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.fund_dir.mkdir(parents=True, exist_ok=True)

    def _log(self, level: str, msg: str):
        safe = msg.encode("cp1252", errors="replace").decode("cp1252")
        print(f"[READINGS] [{level}] {safe}", flush=True)

    # ══════════════════════════════════════════════════════════════════════
    # PASO 1: Busquedas dirigidas
    # ══════════════════════════════════════════════════════════════════════

    async def _directed_searches(self) -> list[dict]:
        """Ejecutar busquedas dirigidas — inteligente, no 1 query por source.

        Estrategia:
        - Tier 1 (high quality): 1 query individual por source (max ~10)
        - Tier 2 (medium): queries batch agrupadas por region
        - Queries genericas: capturan lo que no cae en ninguna source conocida
        """
        from tools.google_search import SearchEngine
        search = SearchEngine(self.isin)

        fund_q = self.fund_short or self.fund_name
        gestora_q = self.gestora or ""
        fund_variants = self._fund_name_variants(fund_q)
        primary = fund_variants[0]  # variante mas limpia

        queries = []

        # ── Tier 0: Claude identifica fuentes especializadas para este fondo ──
        smart_sources = self._identify_best_sources()
        if smart_sources:
            self._log("INFO", f"Claude sugiere: {smart_sources[:5]}")
            for domain in smart_sources[:5]:
                queries.append(f'site:{domain} "{primary}"')

        # ── Tier 1: sources high quality — 1 query cada una ──
        high_sources = [s for s in DIRECTED_SOURCES if s["quality"] == "high"]
        for src in high_sources:
            q = src["query_template"].format(
                fund=primary, isin=self.isin, gestora=gestora_q
            )
            queries.append(q)

        # ── Tier 2: sources medium — agrupar por region ──
        medium_sources = [s for s in DIRECTED_SOURCES if s["quality"] == "medium"]
        # Batch: combinar dominios con OR en site:
        medium_domains = [s["domain"] for s in medium_sources if s["domain"] != "youtube.com"]
        if medium_domains:
            # Google permite max ~3 site: con OR
            for i in range(0, len(medium_domains), 3):
                batch = medium_domains[i:i+3]
                site_clause = " OR ".join(f"site:{d}" for d in batch)
                queries.append(f'({site_clause}) "{primary}"')

        # ── Queries genericas multi-idioma (capturan sources no listadas) ──
        queries.extend([
            f'"{primary}" fund analysis review',
            f'"{primary}" fund commentary opinion 2024 OR 2025',
            f'"{primary}" analisis fondo opinion',           # ES
            f'"{primary}" analyse fonds commentaire',        # FR
        ])

        # Con ISIN para capturar paginas de producto con datos
        queries.append(f'"{self.isin}" fund analysis OR review')

        # ── Gestores: entrevistas/filosofia ──
        for gestor in self.gestores[:2]:
            queries.append(f'"{gestor}" "{primary}" interview OR entrevista')

        # YouTube/podcast aparte
        queries.append(f'"{primary}" OR "{gestora_q}" interview OR podcast OR webinar')

        results = await search.search_multiple(queries, num_per_query=3, agent="readings_collector")
        self._log("INFO", f"Busquedas dirigidas: {len(results)} resultados de {len(queries)} queries")

        # Enriquecer con metadata de source
        enriched = []
        seen_urls: set[str] = set()
        for r in results:
            url = r.get("url", "")
            if not url or url in seen_urls:
                continue
            if any(d in url for d in ["google.com", "bing.com", "linkedin.com", "facebook.com"]):
                continue
            seen_urls.add(url)

            # Identificar source
            source_info = None
            for src in DIRECTED_SOURCES:
                if src["domain"] in url.lower():
                    source_info = src
                    break

            enriched.append({
                **r,
                "source_name": source_info["name"] if source_info else self._domain_from_url(url),
                "source_type": source_info["type"] if source_info else "articulo",
                "source_quality": source_info["quality"] if source_info else "low",
            })

        # Ordenar: high quality primero
        quality_order = {"high": 0, "medium": 1, "low": 2}
        enriched.sort(key=lambda x: quality_order.get(x.get("source_quality", "low"), 3))

        return enriched

    def _fund_name_variants(self, fund_q: str) -> list[str]:
        """Generar variantes del nombre para mejor matching en Google.
        Regla: la variante principal debe ser lo suficientemente especifica
        para no devolver resultados genericos."""
        variants = []

        # Nombre completo sin parentesis
        no_parens = re.sub(r'\s*\([^)]*\)', '', fund_q).strip()
        if no_parens:
            variants.append(no_parens)

        # Nombre original si es diferente
        if fund_q != no_parens:
            variants.append(fund_q)

        # Con gestora prefijada: "DNCA Invest Alpha Bonds" o "Troy Trojan Fund"
        if self.gestora and self.fund_name:
            # Reconstruir nombre completo sin " - "
            full = self.fund_name.replace(" - ", " ")
            full = re.sub(r'\s*\([^)]*\)', '', full).strip()
            if full and full not in variants:
                variants.insert(0, full)

        # Sin "Fund" solo si el resultado tiene >6 chars (evitar "Trojan" solo)
        no_fund = re.sub(r'\s+Fund\b', '', no_parens, flags=re.IGNORECASE).strip()
        if no_fund and len(no_fund) > 6 and no_fund not in variants:
            variants.append(no_fund)

        # Fallback: con gestora (solo si no empieza ya con ella)
        if self.gestora and variants:
            gestora_short = self.gestora.split()[0]
            if not variants[0].lower().startswith(gestora_short.lower()):
                with_gestora = f"{gestora_short} {variants[0]}"
                if with_gestora not in variants:
                    variants.append(with_gestora)

        return variants or [fund_q]

    def _identify_best_sources(self) -> list[str]:
        """Identifica webs especializadas para este fondo usando LLM.

        Usa Claude Opus (mejor conocimiento financiero del mercado) con
        fallback a Gemini Pro. 1 sola call, ~$0.02, input minimo.
        """
        asset_class = self._get_asset_class()
        prompt = (
            f"Fondo: {self.fund_name} ({self.isin})\n"
            f"Gestora: {self.gestora}\n"
            f"Clase de activo: {asset_class}\n\n"
            f"¿En qué 5-8 webs encontraría un analista profesional los mejores "
            f"análisis, opiniones y datos sobre ESTE fondo? "
            f"Piensa en: plataformas de la jurisdicción, sites especializados "
            f"en {asset_class or 'esta clase de activo'}, blogs financieros del nicho, "
            f"web de la gestora. Devuelve SOLO dominios, 1 por línea."
        )

        # Intentar Claude Opus (conocimiento financiero superior)
        try:
            import anthropic
            import os
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            r = client.messages.create(
                model="claude-opus-4-20250514",
                max_tokens=200,  # solo necesitamos dominios
                messages=[{"role": "user", "content": prompt}],
            )
            text = r.content[0].text
            self._log("INFO", f"Opus fuentes ({r.usage.input_tokens}+{r.usage.output_tokens} tok)")
            sites = [line.strip().lower().replace("www.", "")
                     for line in text.split("\n")
                     if "." in line and len(line.strip()) > 4
                     and not any(x in line.lower() for x in
                                 ["google", "wikipedia", "linkedin", "facebook"])]
            if sites:
                return sites[:8]
        except Exception as e:
            self._log("INFO", f"Opus no disponible ({type(e).__name__}), usando Gemini Pro")

        # Fallback: Gemini Pro
        try:
            from tools.gemini_wrapper import extract_fast, MODEL_PRO
            result = extract_fast(
                text=prompt,
                schema={"sites": ["str - dominio web"]},
                context="Devuelve SOLO dominios de webs financieras relevantes.",
                model=MODEL_PRO,
            )
            sites = result.get("sites", []) if isinstance(result, dict) else []
            return [s.strip().lower().replace("www.", "")
                    for s in sites if "." in s and len(s) > 4][:8]
        except Exception as e:
            self._log("WARN", f"Source identification failed: {e}")
            return []

    def _get_asset_class(self) -> str:
        """Leer clase de activo del fondo desde intl_data.json."""
        intl = self.fund_dir / "intl_data.json"
        if intl.exists():
            try:
                d = json.loads(intl.read_text(encoding="utf-8"))
                return (d.get("cualitativo") or {}).get("tipo_activos", "") or ""
            except Exception:
                pass
        return ""

    def _domain_from_url(self, url: str) -> str:
        from urllib.parse import urlparse
        return urlparse(url).netloc.replace("www.", "")

    # ══════════════════════════════════════════════════════════════════════
    # PASO 2: Fetch y extraer contenido
    # ══════════════════════════════════════════════════════════════════════

    async def _fetch_and_extract(self, url: str) -> str:
        """Fetch URL y devolver texto limpio."""
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9,es;q=0.8",
            }
            async with httpx.AsyncClient(follow_redirects=True, timeout=20) as c:
                r = await c.get(url, headers=headers)
                if r.status_code != 200:
                    return ""
                ct = (r.headers.get("content-type") or "").lower()
                if "html" not in ct and "text" not in ct:
                    return ""
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(r.text, "html.parser")
                # Eliminar elementos no-contenido
                for tag in soup(["script", "style", "nav", "footer", "header",
                                 "aside", "form", "iframe"]):
                    tag.decompose()
                # Extraer texto del body/main/article
                main = soup.find("article") or soup.find("main") or soup.find("body")
                if not main:
                    return ""
                text = main.get_text("\n", strip=True)
                return text[:20000]
        except Exception as e:
            self._log("WARN", f"Fetch failed {url[:50]}: {e}")
            return ""

    def _validate_relevance(self, text: str) -> bool:
        """Verificar que el texto habla realmente del fondo."""
        if not text or len(text) < 200:
            return False
        text_lower = text.lower()
        # Debe mencionar al fondo por nombre o ISIN
        fund_terms = [w.lower() for w in self.fund_short.split() if len(w) > 3]
        fund_terms.append(self.isin.lower())
        if self.gestora:
            fund_terms.extend(w.lower() for w in self.gestora.split() if len(w) > 3)
        return any(t in text_lower for t in fund_terms)

    # ══════════════════════════════════════════════════════════════════════
    # PASO 3: Extraer contenido estructurado con Gemini
    # ══════════════════════════════════════════════════════════════════════

    def _extract_structured(self, text: str, url: str, source_name: str) -> dict | None:
        """Extraer contenido estructurado con Gemini Flash."""
        from tools.gemini_wrapper import extract_fast

        try:
            result = extract_fast(
                text=text[:15000],
                schema=READING_SCHEMA,
                context=(
                    f"Extrae informacion sobre el fondo {self.fund_name} ({self.isin}), "
                    f"gestora {self.gestora}. Fuente: {source_name}. "
                    f"El texto puede estar en CUALQUIER idioma (ES, EN, FR, DE, IT). "
                    f"Quiero: resumen, puntos clave, opinion del autor, datos concretos "
                    f"(rentabilidad, AUM, rating, comisiones, riesgo). "
                    f"ACEPTA como contenido valido: analisis editoriales, fichas con datos "
                    f"de performance/riesgo/comisiones, noticias sobre el fondo (cambio gestor, "
                    f"flujos entrada/salida, cambio benchmark), entrevistas del gestor, "
                    f"datos de Morningstar/Citywire/Trustnet, blogs de inversion. "
                    f"RECHAZA SOLO si: el fondo aparece en un listado generico de 50+ fondos "
                    f"sin datos individuales, o es pagina de login/error/cookie. "
                    f"Resumen SIEMPRE en ESPANOL. No inventar datos."
                ),
            )
            if isinstance(result, dict):
                # Filtrar no-relevantes
                resumen = result.get("resumen") or ""
                if resumen.lower().startswith("no_relev"):
                    return None
                if len(resumen) < 30:
                    return None
                return result
        except Exception as e:
            self._log("WARN", f"Extract failed: {e}")
        return None

    # ══════════════════════════════════════════════════════════════════════
    # RUN
    # ══════════════════════════════════════════════════════════════════════

    async def run(self) -> dict:
        self._log("START", f"ReadingsCollector {self.isin} - {self.fund_name}")

        # Paso 1: Busquedas dirigidas
        search_results = await self._directed_searches()
        self._log("INFO", f"Encontradas {len(search_results)} URLs potenciales")

        # Paso 2: Fetch y validar (max 15 URLs, priorizando high quality)
        readings: list[dict] = []
        fuentes_consultadas: list[str] = []

        for entry in search_results[:15]:
            url = entry.get("url", "")
            source_name = entry.get("source_name", "")
            source_type = entry.get("source_type", "articulo")

            text = await self._fetch_and_extract(url)
            if not self._validate_relevance(text):
                continue

            self._log("INFO", f"  [{source_name}] {len(text)} chars - relevante")
            fuentes_consultadas.append(url)

            # Paso 3: Extraer estructurado
            extracted = self._extract_structured(text, url, source_name)
            if not extracted:
                continue

            reading = {
                "url": url,
                "source": source_name,
                "source_type": source_type,
                "quality": entry.get("source_quality", "medium"),
                **extracted,
            }
            readings.append(reading)
            self._log("OK", f"  Extraido: {(extracted.get('titulo') or '')[:50]}")

        # ── Merge con readings existentes (NUNCA perder datos entre runs) ──
        readings = self._merge_with_existing(readings)

        # Clasificar por tipo
        analisis_completos = [r for r in readings if r.get("quality") == "high"
                              and len(r.get("resumen", "")) > 100]
        otros = [r for r in readings if r not in analisis_completos]

        output = {
            "isin": self.isin,
            "fund_name": self.fund_name,
            "gestora": self.gestora,
            "generated": datetime.now().isoformat(),
            "num_readings": len(readings),
            "analisis_completos": analisis_completos,
            "otros_readings": otros,
            "fuentes_consultadas": fuentes_consultadas,
        }

        self._log("OK", f"Total: {len(analisis_completos)} analisis completos + "
                  f"{len(otros)} otros readings")
        return self._save(output)

    def _merge_with_existing(self, new_readings: list[dict]) -> list[dict]:
        """Merge con readings existentes — NUNCA perder datos entre runs.

        Si un reading anterior tiene la misma URL, mantener el que tenga
        resumen más rico. Si es una URL nueva, añadir.
        """
        existing_path = self.fund_dir / "readings_data.json"
        if not existing_path.exists():
            return new_readings

        try:
            existing = json.loads(existing_path.read_text(encoding="utf-8"))
        except Exception:
            return new_readings

        # Indexar existentes por URL
        by_url: dict[str, dict] = {}
        for r in (existing.get("analisis_completos", []) +
                  existing.get("otros_readings", [])):
            url = r.get("url", "")
            if url:
                by_url[url] = r

        # Añadir nuevos (o reemplazar si tienen mejor resumen)
        for r in new_readings:
            url = r.get("url", "")
            if not url:
                continue
            existing_r = by_url.get(url)
            if existing_r:
                # Mantener el que tenga resumen más largo
                new_len = len(r.get("resumen") or "")
                old_len = len(existing_r.get("resumen") or "")
                if new_len > old_len:
                    by_url[url] = r
            else:
                by_url[url] = r

        merged = list(by_url.values())
        if len(merged) > len(new_readings):
            self._log("INFO", f"Merge: {len(new_readings)} nuevos + "
                      f"{len(merged) - len(new_readings)} preservados = "
                      f"{len(merged)} total")
        return merged

    def _save(self, data: dict) -> dict:
        path = self.fund_dir / "readings_data.json"
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        self._log("OK", f"Guardado: {path}")
        return data


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", required=True)
    parser.add_argument("--fund-name", default="")
    parser.add_argument("--gestora", default="")
    args = parser.parse_args()

    agent = ReadingsCollector(args.isin, fund_name=args.fund_name, gestora=args.gestora)
    result = asyncio.run(agent.run())
    print(f"\nReadings: {result['num_readings']}")
    for r in result.get("analisis_completos", []):
        print(f"  [HIGH] {r.get('source',''):20s} {r.get('titulo','')[:50]}")
    for r in result.get("otros_readings", []):
        print(f"  [----] {r.get('source',''):20s} {r.get('titulo','')[:50]}")
