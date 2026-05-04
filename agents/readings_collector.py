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
        # Bloque 1 Fase I (2026-04-28): añadido por feedback usuario.
        # Moclano publica analisis profundos de fondos value/quality (ES + INT).
        "domain": "moclano.substack.com",
        "name": "Moclano",
        "query_template": 'site:moclano.substack.com "{fund}"',
        "type": "analisis_completo",
        "quality": "high",
    },
    {
        "domain": "astralis.es",
        "name": "Astralis",
        "query_template": 'site:astralis.es "{fund}"',
        "type": "analisis_completo",
        "quality": "high",
    },
    # ── INT: blogs/webs analistas reconocidos ──
    {
        "domain": "moiglobal.com",
        "name": "MOI Global",
        "query_template": 'site:moiglobal.com "{fund}" OR "{gestora}"',
        "type": "analisis_completo",
        "quality": "high",
    },
    {
        "domain": "valuewalk.com",
        "name": "ValueWalk",
        "query_template": 'site:valuewalk.com "{fund}" OR "{gestora}"',
        "type": "analisis",
        "quality": "high",
    },
    {
        "domain": "gurufocus.com",
        "name": "GuruFocus",
        "query_template": 'site:gurufocus.com "{fund}" OR "{gestora}"',
        "type": "analisis",
        "quality": "medium",
    },
    {
        "domain": "fool.com",
        "name": "Motley Fool",
        "query_template": 'site:fool.com "{fund}" OR "{gestora}"',
        "type": "articulo",
        "quality": "medium",
    },
    {
        "domain": "advisorperspectives.com",
        "name": "Advisor Perspectives",
        "query_template": 'site:advisorperspectives.com "{fund}" OR "{gestora}"',
        "type": "analisis",
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


# ══════════════════════════════════════════════════════════════════════════════
# K22 Fase K (2026-04-29): Adaptación INT — press + niche blogs por región/idioma
# ══════════════════════════════════════════════════════════════════════════════
# Funcionamiento: para fondos NO-ES con cobertura mediática (Comgest, Carmignac,
# DNCA, R-Co, GAM, Fundsmith, Lindsell Train, etc.), añadir queries dirigidas a
# prensa financiera + blogs de nicho del país de la gestora con keywords en
# idioma local. Aditivo — no toca el camino ES.

INT_PRESS_BY_REGION = {
    "FR": ["lesechos.fr", "lemonde.fr", "lefigaro.fr", "boursorama.com",
           "investir.lesechos.fr", "agefi.fr", "capital.fr"],
    "DE": ["handelsblatt.com", "faz.net", "manager-magazin.de",
           "boerse-online.de", "wirtschaftswoche.de", "n-tv.de"],
    "GB": ["ft.com", "telegraph.co.uk", "thetimes.co.uk", "cityam.com",
           "investorschronicle.co.uk", "thisismoney.co.uk", "sharesmagazine.co.uk"],
    "IT": ["milanofinanza.it", "ilsole24ore.com", "soldionline.it",
           "borsaitaliana.it", "wallstreetitalia.com"],
    "CH": ["nzz.ch", "finews.ch", "fuw.ch", "cash.ch", "handelszeitung.ch"],
    "BE": ["lecho.be", "tijd.be", "trends.knack.be"],
    "NL": ["fd.nl", "iex.nl", "vastgoedmarkt.nl"],
    "EN_GLOBAL": ["ft.com", "wsj.com", "reuters.com", "bloomberg.com",
                  "barrons.com", "marketwatch.com"],
}

INT_NICHE_BLOGS_BY_REGION = {
    "FR": ["h24finance.com", "club-patrimoine.com", "morningstar.fr",
           "quantalys.com", "boursier.com", "cafedelabourse.com"],
    "DE": ["dasinvestment.com", "fondsprofessionell.de", "fondsweb.com",
           "morningstar.de", "extra-funds.de", "fonds-fuer-alle.de"],
    "GB": ["monevator.com", "diyinvestor.net", "trustnet.com", "citywire.com",
           "morningstar.co.uk", "hl.co.uk", "ii.co.uk", "bestinvest.co.uk"],
    "IT": ["morningstar.it", "fondidoc.it", "advisoronline.it"],
    "CH": ["finews.ch", "fundplat.com"],
    "BE": ["mafr.be"],
    "NL": ["iex.nl"],
    "EN_GLOBAL": ["seekingalpha.com", "valuewalk.com", "moiglobal.com",
                  "sumzero.com", "advisorperspectives.com", "gurufocus.com"],
}

# Keywords por idioma para queries de entrevistas/análisis
LANG_KEYWORDS = {
    "FR": {"interview": "entretien OR interview", "analysis": "analyse OR commentaire OR avis"},
    "DE": {"interview": "Interview OR Gespräch", "analysis": "Analyse OR Bewertung OR Kommentar"},
    "GB": {"interview": "interview", "analysis": "analysis OR review OR commentary"},
    "EN_GLOBAL": {"interview": "interview", "analysis": "analysis OR review OR commentary"},
    "IT": {"interview": "intervista", "analysis": "analisi OR recensione OR commento"},
    "CH": {"interview": "Interview OR entretien", "analysis": "Analyse OR analyse"},
    "BE": {"interview": "interview OR entretien", "analysis": "analyse OR analyse"},
    "NL": {"interview": "interview", "analysis": "analyse OR beoordeling"},
}

# Mapping ISIN prefix → región explícita (LU/IE son ambiguos, se resuelven por gestora)
ISIN_PREFIX_TO_REGION = {
    "FR": "FR", "DE": "DE", "AT": "DE", "GB": "GB",
    "IT": "IT", "CH": "CH", "LI": "CH",
    "BE": "BE", "NL": "NL",
}

# Detección región por gestora (caso LU/IE — UCITS multidomicilio)
GESTORA_REGION_HINTS = {
    "FR": ["dnca", "carmignac", "rothschild", "comgest", "ostrum",
           "groupama", "amundi", "bnp paribas", "natixis", "edmond de rothschild",
           "tikehau", "axiom", "lazard frères"],
    "DE": ["dws", "allianz global investors", "union investment", "dje kapital",
           "deka", "metzler", "lupus alpha", "flossbach von storch"],
    "GB": ["fundsmith", "lindsell train", "troy asset", "guinness asset",
           "evenlode", "polar capital", "baillie gifford", "schroders", "jupiter",
           "marlborough", "liontrust", "fidelity international", "m&g", "ruffer"],
    "IT": ["azimut", "anima", "eurizon", "mediolanum", "generali"],
    "CH": ["pictet", "lombard odier", "vontobel", "gam ", "ubs ",
           "credit suisse", "julius baer", "swisscanto", "banque syz"],
    "EN_GLOBAL": [],  # fallback
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
        is_es = self.isin.upper().startswith("ES")  # K19: para queries press españolas
        int_region = self._detect_region() if not is_es else "ES"  # K22: región INT para press/blogs locales

        queries = []

        # K17 Fase K (2026-04-29): elegir variante según tipo de source.
        # FULL para medios grandes (Morningstar/FT/Bloomberg indexan nombre legal).
        # SHORT para blogs de nicho (escriben coloquial: "Magallanes European").
        # variants[0] es FULL, variants[-1] suele ser SHORT.
        full_name = fund_variants[0]
        short_name = fund_variants[-1] if len(fund_variants) > 1 else full_name
        # MEDIUM (sin sufijos legales) — usado para blogs si SHORT es muy genérico
        medium_name = fund_variants[1] if len(fund_variants) > 2 else short_name

        self._log("INFO", f"Variantes nombre: FULL={full_name!r} MEDIUM={medium_name!r} SHORT={short_name!r}")
        if not is_es:
            self._log("INFO", f"K22: detectada región INT = {int_region} (gestora={self.gestora!r})")

        # ── Tier 0: Claude identifica fuentes especializadas para este fondo ──
        smart_sources = self._identify_best_sources()
        if smart_sources:
            self._log("INFO", f"Claude sugiere: {smart_sources[:5]}")
            for domain in smart_sources[:5]:
                # Smart sources: usar SHORT (Claude propone blogs específicos)
                name = short_name if self._is_niche_blog(domain) else full_name
                queries.append(f'site:{domain} "{name}"')

        # ── Tier 1: sources high quality — 1 query cada una ──
        # K17: blogs de nicho usan SHORT, medios grandes usan FULL del template.
        high_sources = [s for s in DIRECTED_SOURCES if s["quality"] == "high"]
        for src in high_sources:
            if self._is_niche_blog(src["domain"]):
                # Blogs de nicho: usar SHORT name (más probable que coincida)
                queries.append(f'site:{src["domain"]} "{short_name}"')
                # Si SHORT es muy corto (1 palabra), añadir MEDIUM como backup
                if len(short_name.split()) <= 1 and medium_name != short_name:
                    queries.append(f'site:{src["domain"]} "{medium_name}"')
            else:
                # Medios grandes: usar template original (FULL name)
                q = src["query_template"].format(
                    fund=full_name, isin=self.isin, gestora=gestora_q
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
        # K19 Fase K (2026-04-29): queries amplias por gestor con SHORT name
        # de fondo + token gestora. Los gestores conocidos (Guzmán, Bernad,
        # Iván Martín, Paramés, etc.) tienen MUCHA cobertura web pero con
        # nombre legal completo del fondo no aparecen — usar SHORT/gestora.
        gestora_token = self.gestora.split()[0] if self.gestora else ""
        # Medios nacionales españoles donde suelen aparecer entrevistas a gestores
        SPANISH_PRESS = [
            "elconfidencial.com", "expansion.com", "cincodias.elpais.com",
            "elmundo.es", "abc.es", "elpais.com", "eleconomista.es",
            "publico.es", "lainformacion.com", "vozpopuli.com",
        ]
        for gestor in self.gestores[:3]:  # ampliado de 2 a 3
            if not gestor or len(gestor) < 5:
                continue
            # Combinada gestor + SHORT del fondo
            queries.append(f'"{gestor}" "{short_name}"')
            # Combinada gestor + token gestora
            if gestora_token:
                queries.append(f'"{gestor}" "{gestora_token}"')
            # Entrevistas/análisis del gestor
            queries.append(f'"{gestor}" entrevista OR análisis OR opinión')
            queries.append(f'"{gestor}" interview OR analysis')
            # Queries por medios nacionales prensa (gestores ES suelen aparecer)
            if is_es:
                for press in SPANISH_PRESS[:5]:  # top 5 para no saturar
                    queries.append(f'site:{press} "{gestor}"')
                # Blogs nicho con gestor (no fund — más probable que coincida)
                for blog in ["moclano.substack.com", "saludfinanciera.substack.com",
                             "rankia.com", "finect.com", "masdividendos.com"]:
                    queries.append(f'site:{blog} "{gestor}"')
            else:
                # K22 Fase K (2026-04-29): rama INT — press + nicho regionales con
                # keywords en idioma local. Aplica a fondos NO-ES con gestores
                # nombrados (Comgest, Fundsmith, DNCA, GAM, etc.).
                int_press = INT_PRESS_BY_REGION.get(int_region,
                                                    INT_PRESS_BY_REGION["EN_GLOBAL"])
                int_blogs = INT_NICHE_BLOGS_BY_REGION.get(int_region,
                                                          INT_NICHE_BLOGS_BY_REGION["EN_GLOBAL"])
                lang_kw = LANG_KEYWORDS.get(int_region, LANG_KEYWORDS["EN_GLOBAL"])
                # Press regional (5 medios top)
                for press in int_press[:5]:
                    queries.append(f'site:{press} "{gestor}"')
                # Blogs nicho regionales (5 top)
                for blog in int_blogs[:5]:
                    queries.append(f'site:{blog} "{gestor}"')
                # Entrevista/análisis en idioma local
                queries.append(f'"{gestor}" {lang_kw["interview"]}')
                queries.append(f'"{gestor}" {lang_kw["analysis"]}')

        # K22 Fase K (2026-04-29): para INT sin gestores nombrados, queries
        # fund-level por press regional con SHORT name. Captura cobertura
        # mediática de fondos institucionales sin gestor público (Comgest
        # Europe Opportunities, Carmignac Patrimoine, etc.).
        if not is_es:
            int_press = INT_PRESS_BY_REGION.get(int_region,
                                                INT_PRESS_BY_REGION["EN_GLOBAL"])
            for press in int_press[:3]:
                queries.append(f'site:{press} "{short_name}"')
            self._log("INFO", f"INT region={int_region} | press={len(int_press)} blogs={len(INT_NICHE_BLOGS_BY_REGION.get(int_region, []))}")

        # YouTube/podcast aparte (con SHORT + gestora)
        queries.append(f'"{short_name}" OR "{gestora_q}" interview OR podcast OR webinar')
        # Si tenemos gestores, también YouTube por gestor
        for gestor in self.gestores[:2]:
            if gestor and len(gestor) >= 5:
                queries.append(f'"{gestor}" YouTube interview OR podcast')

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

        K17 Fase K (2026-04-29): genera 3 niveles de especificidad:
        - FULL: nombre legal completo ("MAGALLANES EUROPEAN EQUITY FI")
        - MEDIUM: sin sufijos legales/regulatorios ("Magallanes European Equity")
        - SHORT: gestora + 1-2 tokens distintivos ("Magallanes European" o "Magallanes Value")

        Para queries en BLOGS DE NICHO usar SHORT (más probable que coincida
        con cómo el blog escribe el nombre). Para MEDIOS GRANDES usar FULL.
        Lista ordenada: [FULL, MEDIUM, SHORT, ...alternativas].
        """
        variants = []

        # FULL: Nombre completo sin parentesis
        no_parens = re.sub(r'\s*\([^)]*\)', '', fund_q).strip()
        if no_parens:
            variants.append(no_parens)
        if fund_q != no_parens and fund_q not in variants:
            variants.append(fund_q)

        # FULL alt: con gestora prefijada (ej. INT)
        if self.gestora and self.fund_name:
            full = self.fund_name.replace(" - ", " ")
            full = re.sub(r'\s*\([^)]*\)', '', full).strip()
            if full and full not in variants:
                variants.insert(0, full)

        # MEDIUM: sin sufijos legales/regulatorios + puntuación
        SUFFIX_PATTERNS = [
            r',\s*FI\b', r',\s*FCR\b', r',\s*FIL\b', r',\s*SICAV\b',
            r'\s+FI\b', r'\s+FCR\b', r'\s+FIL\b', r'\s+SICAV\b',
            r'\s+S\.?A\.?\b', r'\s+S\.?L\.?\b',
            r'\s+Fund\b', r'\s+FONDO\b',
            r'\s+(Acc|ACC|Cap|CAP|Inc|INC)\b',
            r'\s+Class\s+\w+', r'\s+Clase\s+\w+',
            r'\s+EUR\b$', r'\s+USD\b$', r'\s+GBP\b$', r'\s+CHF\b$',
        ]
        medium = no_parens
        for pat in SUFFIX_PATTERNS:
            medium = re.sub(pat, '', medium, flags=re.IGNORECASE)
        # Limpiar puntuación final + collapse espacios
        medium = re.sub(r'[,;:.\s]+$', '', medium).strip()
        medium = re.sub(r'\s+', ' ', medium)
        if medium and medium not in variants and len(medium) > 5:
            variants.append(medium)

        # SHORT: si MEDIUM tiene ≥2 palabras significativas, usar las 2 primeras.
        # Si MEDIUM es 1 sola palabra (ej. "AVANTAGE"), usar gestora_token + esa.
        if medium:
            tokens = [t for t in medium.split() if len(t) > 2]  # tokens significativos
            if len(tokens) >= 2:
                short = " ".join(tokens[:2])
                if short and short not in variants:
                    variants.append(short)
            elif len(tokens) == 1 and self.gestora:
                # SHORT = gestora_token + fund_token si fondo es 1 palabra
                gestora_token = self.gestora.split()[0]
                if gestora_token.lower() != tokens[0].lower():
                    short = f"{gestora_token} {tokens[0]}"
                else:
                    short = tokens[0]  # gestora == fund (ej. "Avantage")
                if short and short not in variants:
                    variants.append(short)

        # Fallback con gestora (solo si no empieza ya con ella)
        if self.gestora and variants:
            gestora_short = self.gestora.split()[0]
            if not variants[0].lower().startswith(gestora_short.lower()):
                with_gestora = f"{gestora_short} {variants[0]}"
                if with_gestora not in variants:
                    variants.append(with_gestora)

        return variants or [fund_q]

    # K17 Fase K (2026-04-29): clasificación blog nicho vs medio grande
    NICHE_BLOG_DOMAINS = (
        "moclano.substack.com", "saludfinanciera.substack.com",
        "astralisfundsacademy.com", "astralis.es",
        "rankia.com", "finect.com", "masdividendos.com",
        "valueschool.es", "quenoteloinviertan.com", "inversor-tranquilo.com",
        "moiglobal.com", "valuewalk.com", "gurufocus.com",
        "fool.com", "advisorperspectives.com", "valueinvestorsclub.com",
        "seekingalpha.com",
    )

    def _is_niche_blog(self, domain: str) -> bool:
        """True si el dominio es un blog de nicho (mejor query con SHORT name).

        K22 Fase K (2026-04-29): incluye también los blogs INT regionales
        (h24finance, dasinvestment, monevator, etc.) para que las queries usen
        SHORT name en vez de FULL.
        """
        d = domain.lower()
        if any(nb in d for nb in self.NICHE_BLOG_DOMAINS):
            return True
        # K22: blogs nicho regionales INT
        for region_blogs in INT_NICHE_BLOGS_BY_REGION.values():
            if any(nb in d for nb in region_blogs):
                return True
        return False

    def _detect_region(self) -> str:
        """K22 Fase K (2026-04-29): detecta región para queries INT.

        Devuelve código de región (FR/DE/GB/IT/CH/BE/NL/EN_GLOBAL):
        1. Por prefijo ISIN si es explícito (FR/DE/GB/IT/...)
        2. Por gestora si LU/IE (multidomicilio UCITS)
        3. Fallback EN_GLOBAL
        """
        prefix = self.isin[:2].upper()
        explicit = ISIN_PREFIX_TO_REGION.get(prefix)
        if explicit:
            return explicit
        # LU/IE/otros: detectar por gestora
        g = (self.gestora or "").lower().strip()
        if g:
            for region, hints in GESTORA_REGION_HINTS.items():
                if region == "EN_GLOBAL":
                    continue
                if any(h in g for h in hints):
                    return region
        return "EN_GLOBAL"

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

        # Fase M (2026-05-04): Opus → Haiku. Tarea de clasificación pura
        # (lista de dominios). Haiku 4.5 basta. ~5x más barato.
        try:
            import anthropic
            import os
            from tools.llm_models import HAIKU_HINTS
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            r = client.messages.create(
                model=HAIKU_HINTS,
                max_tokens=200,  # solo necesitamos dominios
                temperature=0,  # K5 Fase K: determinismo
                messages=[{"role": "user", "content": prompt}],
            )
            try:
                from tools.llm_logger import log_llm_response
                log_llm_response(r, agent="readings_collector_haiku",
                                  isin=self.isin, model=HAIKU_HINTS,
                                  provider="anthropic")
            except Exception:
                pass
            text = r.content[0].text
            self._log("INFO", f"Haiku fuentes ({r.usage.input_tokens}+{r.usage.output_tokens} tok)")
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
    # Bloque 1 Fase I (2026-04-28): features portadas de readings_agent
    # ══════════════════════════════════════════════════════════════════════

    def _load_known_fund_names(self) -> dict[str, str]:
        """Carga nombres de TODOS los fondos del sistema → {nombre_lower: isin}.
        Usado por _check_cross_fund_contamination para detectar readings que
        hablan de OTRO fondo más que del actual.
        """
        funds_dir = Path(__file__).parent.parent / "data" / "funds"
        out = {}
        if not funds_dir.exists():
            return out
        for fd in funds_dir.iterdir():
            if not fd.is_dir():
                continue
            out_path = fd / "output.json"
            if not out_path.exists():
                continue
            try:
                d = json.loads(out_path.read_text(encoding="utf-8"))
                nombre = d.get("nombre", "").strip()
                if nombre and len(nombre) > 5:
                    nombre_short = nombre.split(",")[0].strip().lower()
                    out[nombre_short] = fd.name
            except Exception:
                continue
        return out

    def _check_cross_fund_contamination(
        self, text: str, known_funds: dict
    ) -> tuple[int, int, str]:
        """Cuenta menciones del fondo actual vs otros fondos conocidos.
        Returns: (this_count, other_max_count, other_name).

        Fix B Fase J: contamos por anchor multi-palabra (primary + segundo token)
        para evitar falsos positivos con palabras genéricas del dominio
        ("value", "equity", "fund"...). Ej. "True Value, FI" → anchor "true value",
        no solo "value". "Magallanes European Equity" → "magallanes european".

        Si el nombre tiene 1 sola palabra significativa, anchor = esa palabra
        (asumimos que es distintiva, ej. "Trojan", "Magallanes" — no genérica).
        """
        def _anchor(name: str) -> str:
            # Limpiar puntuación y tokenizar
            cleaned = re.sub(r"[,;:.()\"']", " ", (name or "")).lower()
            tokens = [w for w in cleaned.split() if len(w) > 3]
            if not tokens:
                return ""
            # Si nombre tiene ≥2 tokens significativos: bigrama de los 2 primeros
            # (más distintivo, evita match con palabras genéricas)
            if len(tokens) >= 2:
                return f"{tokens[0]} {tokens[1]}"
            return tokens[0]

        text_lower = text.lower()
        this_anchor = _anchor(self.fund_short)
        if not this_anchor:
            return (0, 0, "")
        this_count = text_lower.count(this_anchor)
        other_max = 0
        other_name = ""
        for other_short, other_isin in known_funds.items():
            if other_isin == self.isin:
                continue
            other_anchor = _anchor(other_short)
            if not other_anchor or other_anchor == this_anchor:
                continue
            cnt = text_lower.count(other_anchor)
            if cnt > other_max:
                other_max = cnt
                other_name = other_short
        return (this_count, other_max, other_name)

    def _classify_quality_post_fetch(
        self, url: str, title: str, text: str
    ) -> str:
        """Clasifica calidad de un reading tras descarga full-text:
        - analysis: ≥1500 chars + dominio pro O kw análisis|review|tesis|carta
        - news: ≥500 chars + kw noticia|publica|anuncia
        - marketing: dominio gestora directo + kw rentabilidad histórica|premio
        - data: factsheet/KIID/prospectus
        """
        url_l = url.lower()
        text_l = (title + " " + text[:500]).lower()
        is_pro = any(s["domain"] in url_l for s in DIRECTED_SOURCES if s["quality"] == "high")

        if any(kw in text_l for kw in ["factsheet", "kiid", "prospectus", "folleto"]):
            return "data"
        if is_pro and len(text) >= 1500:
            return "analysis"
        if any(kw in text_l for kw in ["análisis", "analysis", "review", "opinion", "opinión", "tesis", "carta a inversores"]) and len(text) >= 1500:
            return "analysis"
        if any(kw in text_l for kw in ["noticia", "news", "publica", "anuncia", "lanza"]) and len(text) >= 500:
            return "news"
        if any(kw in text_l for kw in ["rentabilidad histórica", "premio", "galardón"]):
            return "marketing"
        return "news" if len(text) >= 500 else "marketing"

    def _validate_full_text_match(
        self, text: str, is_pro: bool, url: str = ""
    ) -> tuple[bool, list[str]]:
        """Validation post-fetch (Bloque 1.4 Fase I + Fix C Fase J + K6 Fase K).

        Threshold dinámico:
        - Pro source con ISIN en URL: ≥1 mención del fund/gestora.
        - Pro source sin ISIN en URL: ≥2 menciones.
        - Generalista: ≥3 menciones.

        K6 (2026-04-29): si fund_short tokens son genéricos (managers/value/equity/
        etc.), añadir gestora primer token como anchor adicional. Cuenta el MAX
        entre fund_short tokens y gestora token.
        """
        GENERIC_TOKENS = {
            "managers", "manager", "value", "equity", "equities", "fund", "fondos",
            "fondo", "bond", "bonds", "growth", "income", "balanced", "global",
            "international", "europe", "european", "world", "select", "selection",
            "strategy", "core", "active", "passive", "index",
        }

        text_l = text.lower()
        url_l = (url or "").lower()
        log = []
        if is_pro:
            log.append("pro_source")
        isin_in_text = self.isin.lower() in text_l
        isin_in_url = self.isin.lower() in url_l
        if isin_in_text:
            log.append("isin_match")
        if isin_in_url:
            log.append("isin_in_url")

        # Token-based name counting:
        # Cuenta apariciones del token MÁS LARGO/significativo del fund_short.
        # K6: si todos los tokens son genéricos, fallback a gestora token.
        name_tokens = [w.lower() for w in (self.fund_short or "").split() if len(w) > 3]
        name_tokens.sort(key=len, reverse=True)

        # K6 Fase K: detectar si TODOS los tokens son genéricos del dominio
        all_generic = name_tokens and all(t in GENERIC_TOKENS for t in name_tokens)
        if all_generic and self.gestora:
            gest_tokens = [w.lower() for w in self.gestora.split() if len(w) > 3]
            if gest_tokens:
                gest_tokens.sort(key=len, reverse=True)
                name_tokens = gest_tokens + name_tokens  # gestora prioritaria
                log.append("anchor_gestora")

        name_count = 0
        if name_tokens:
            name_count = max(text_l.count(t) for t in name_tokens)
        if name_count >= 1:
            log.append(f"name_match_{name_count}x")

        # Threshold dinámico
        if is_pro and isin_in_url:
            min_name = 1
        elif is_pro:
            min_name = 2
        else:
            min_name = 3
        valido_basic = isin_in_text or name_count >= min_name

        # K17 Fase K (2026-04-29): refuerzo anti-mención-tangencial.
        # Si el reading menciona el fund_short solo 1-2 veces pero el texto es
        # MUY largo (>5000c), puede ser mención tangencial (lista de fondos,
        # comparativa breve). Exigir contexto cercano: keywords del dominio
        # (rentabilidad, gestor, AUM, value, cartera, posición, comisión, fondo)
        # cerca del nombre del fondo.
        if valido_basic and not isin_in_text and len(text) > 5000 and name_count <= 2:
            # Buscar contexto fund + keyword en ventana de 200 chars
            CONTEXT_KW = ("rentabilidad", "gestor", "patrimonio", "aum", "value",
                          "cartera", "posición", "posicion", "comisión", "comision",
                          "fondo", "alpha", "beta", "tracking", "benchmark",
                          "manager", "performance", "ratio", "drawdown")
            primary_token = name_tokens[0] if name_tokens else ""
            has_context = False
            if primary_token:
                for m in __import__("re").finditer(re.escape(primary_token), text_l):
                    s, e = max(0, m.start()-200), min(len(text_l), m.end()+200)
                    window = text_l[s:e]
                    if any(kw in window for kw in CONTEXT_KW):
                        has_context = True
                        break
            if not has_context:
                log.append("rejected_tangential_mention")
                return (False, log)
            log.append("context_validated")

        return (valido_basic, log)

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

        # Bloque 1 Fase I (2026-04-28): cargar fondos conocidos para cross-fund check
        known_funds = self._load_known_fund_names()

        # Pro sources attempted (auditoría)
        pro_sources_attempted = [s["domain"] for s in DIRECTED_SOURCES if s["quality"] == "high"]

        # Paso 2: Fetch y validar (Bloque 1.5: cap 15 → 30, priorizando high quality)
        readings: list[dict] = []
        fuentes_consultadas: list[str] = []
        discarded_cross_fund: list[dict] = []

        for entry in search_results[:30]:
            url = entry.get("url", "")
            source_name = entry.get("source_name", "")
            source_type = entry.get("source_type", "articulo")

            text = await self._fetch_and_extract(url)
            if not self._validate_relevance(text):
                continue

            # Bloque 1.4 Fase I + Fix C Fase J: filtro post-fetch ISIN/nombre fondo
            url_l = url.lower()
            is_pro = any(s["domain"] in url_l for s in DIRECTED_SOURCES if s["quality"] == "high")
            valido, validation_log = self._validate_full_text_match(text, is_pro, url=url)
            if not valido:
                self._log("SKIP", f"  insufficient match: {url[:60]}")
                continue

            # Bloque 1.2 Fase I + Fix B Fase J: cross-fund check, pero ACEPTAR
            # comparativas legítimas si el ISIN del fondo target aparece en URL o
            # título (señal de que el reading es PRINCIPALMENTE sobre este fondo).
            this_count, other_count, other_name = self._check_cross_fund_contamination(text, known_funds)
            isin_in_url_or_title = (
                self.isin.lower() in url.lower()
                or self.isin.lower() in (entry.get("title", "") or "").lower()
            )
            if (
                other_count > this_count + 2
                and other_count >= 3
                and not isin_in_url_or_title
            ):
                self._log("DROP_CROSS", f"  reading habla más de '{other_name}' "
                          f"({other_count}x) que de '{self.fund_short}' ({this_count}x): {url[:60]}")
                discarded_cross_fund.append({
                    "url": url,
                    "this_fund_mentions": this_count,
                    "other_fund": other_name,
                    "other_mentions": other_count,
                })
                continue

            self._log("INFO", f"  [{source_name}] {len(text)} chars - relevante")
            fuentes_consultadas.append(url)

            # Bloque 1.3 Fase I: quality classification post-fetch
            quality_class = self._classify_quality_post_fetch(url, "", text)

            # Paso 3: Extraer estructurado (mantenido — gemini_wrapper)
            extracted = self._extract_structured(text, url, source_name)
            if not extracted:
                continue

            reading = {
                "url": url,
                "source": source_name,
                "source_type": source_type,
                "quality": entry.get("source_quality", "medium"),
                "quality_classification": quality_class,
                "_validation_log": validation_log,
                "_is_pro_source": is_pro,
                "_fund_name_mentions": this_count,
                "_isin_in_text": self.isin.lower() in text.lower(),
                **extracted,
            }
            readings.append(reading)
            self._log("OK", f"  [{quality_class:9s}] Extraido: {(extracted.get('titulo') or '')[:50]}")

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
            "_pro_sources_attempted": pro_sources_attempted,
            "_discarded_cross_fund": discarded_cross_fund,
        }

        self._log("OK", f"Total: {len(analisis_completos)} analisis completos + "
                  f"{len(otros)} otros readings | "
                  f"discarded cross-fund: {len(discarded_cross_fund)}")
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
