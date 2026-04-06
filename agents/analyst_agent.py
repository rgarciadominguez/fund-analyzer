"""
Analyst Agent — Síntesis final con Claude API

Lee todos los JSONs parciales del fondo, unifica en el schema universal
y llama a Claude para generar análisis cualitativo y de consistencia.

Output must read like a briefing written by a senior analyst for an
investment committee: no filler, no generic statements, maximum signal
per sentence. Based on feedback/analyst_agent.txt prompt.

Si no hay ANTHROPIC_API_KEY → campos cualitativos = null, nunca bloquea.
"""
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.claude_extractor import extract_structured_data

console = Console()

# ── Analyst prompt from feedback/analyst_agent.txt ───────────────────────────
_ANALYST_PROMPT_PATH = Path(__file__).parent.parent / "feedback" / "analyst_agent.txt"
_ANALYST_SYSTEM_PROMPT = ""
if _ANALYST_PROMPT_PATH.exists():
    _ANALYST_SYSTEM_PROMPT = _ANALYST_PROMPT_PATH.read_text(encoding="utf-8")


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


class AnalystAgent:
    """
    Agente analista. Lee parciales, unifica schema, llama Claude API.
    Clase con run() -> dict (síncrono, Claude SDK es síncrono).
    """

    def __init__(self, isin: str, config: dict):
        self.isin = isin.strip().upper()
        self.config = config

        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.schema_path = root / "schemas" / "fund_output.json"
        self.log_path = root / "progress.log"

    def _log(self, level: str, msg: str):
        line = f"[{_ts()}] [ANALYST] [{level}] {msg}"
        console.log(line)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    # ── Entry point ──────────────────────────────────────────────────────────

    def run(self) -> dict:
        self._log("START", f"Iniciando análisis para {self.isin}")

        # 1. Leer schema universal como plantilla
        output = self._load_schema_template()

        # 2. Fusionar todos los JSONs parciales disponibles
        self._merge_partial_jsons(output)

        # 3. Llamar a Claude para cualitativos si hay API key
        self._enrich_with_claude(output)

        # 4. Calcular análisis de consistencia
        self._build_consistency_analysis(output)

        # 5. Guardar output.json validado
        output["ultima_actualizacion"] = datetime.now().isoformat()
        self._save(output)

        # Resumen de campos completados
        completed, nulls = self._count_fields(output)
        self._log("OK", f"output.json guardado — {completed} campos completados, {len(nulls)} null")
        return output

    # ── Schema template ───────────────────────────────────────────────────────

    def _load_schema_template(self) -> dict:
        """Carga el schema universal como base del output."""
        template = json.loads(self.schema_path.read_text(encoding="utf-8"))
        # Limpiar arrays de ejemplo → vacíos
        def _clean(obj):
            if isinstance(obj, dict):
                return {k: _clean(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return []
            return obj
        result = _clean(template)
        result["isin"] = self.isin
        return result

    # ── Merge parciales ───────────────────────────────────────────────────────

    def _merge_partial_jsons(self, output: dict) -> None:
        """
        Fusiona cnmv_data.json, intl_data.json y letters_data.json en output.
        La regla: los datos concretos (no-null, no-vacíos) sobreescriben la plantilla.
        """
        files_to_merge = [
            self.fund_dir / "cnmv_data.json",
            self.fund_dir / "intl_data.json",
            self.fund_dir / "cssf_data.json",
            self.fund_dir / "letters_data.json",
        ]

        for fpath in files_to_merge:
            if not fpath.exists():
                self._log("INFO", f"No existe: {fpath.name} — saltando")
                continue
            try:
                data = json.loads(fpath.read_text(encoding="utf-8"))
                self._deep_merge(output, data)
                self._log("OK", f"Fusionado: {fpath.name}")
            except Exception as exc:
                self._log("WARN", f"Error leyendo {fpath.name}: {exc}")

        # Asegurar que isin no se sobreescriba
        output["isin"] = self.isin

        # Mover campo "nif" fuera del schema si existe (campo interno)
        output.pop("nif", None)

    def _deep_merge(self, base: dict, override: dict) -> None:
        """
        Fusión profunda: override sobreescribe base para valores no-nulos.
        Listas: se concatenan (deduplicando por contenido JSON).
        """
        for key, val in override.items():
            if key not in base:
                base[key] = val
                continue

            if val is None or val == "" or val == [] or val == {}:
                continue  # no sobreescribir con vacío

            if isinstance(val, dict) and isinstance(base[key], dict):
                self._deep_merge(base[key], val)
            elif isinstance(val, list) and isinstance(base[key], list):
                # Concatenar listas, evitar duplicados exactos
                existing_strs = {json.dumps(e, sort_keys=True) for e in base[key]}
                for item in val:
                    if json.dumps(item, sort_keys=True) not in existing_strs:
                        base[key].append(item)
                        existing_strs.add(json.dumps(item, sort_keys=True))
            else:
                base[key] = val

        # Poblar campos del schema desde fuentes alternativas
        self._remap_fields(base, override)

    def _remap_fields(self, output: dict, source: dict) -> None:
        """Mapea campos con nombre distinto entre parciales y schema universal."""
        # nombre del fondo
        if not output.get("nombre") and source.get("nombre"):
            output["nombre"] = source["nombre"]

        # nombre oficial desde CSSF (si intl_data no tiene nombre)
        if not output.get("nombre") and source.get("nombre_oficial"):
            output["nombre"] = source["nombre_oficial"]

        # gestora
        if not output.get("gestora") and source.get("gestora"):
            output["gestora"] = source["gestora"]

        # gestora oficial desde CSSF (si intl_data no tiene gestora)
        if not output.get("gestora") and source.get("gestora_oficial"):
            output["gestora"] = source["gestora_oficial"]

        # tipo ES/INT
        if not output.get("tipo") and source.get("tipo"):
            output["tipo"] = source["tipo"]

        # KPIs desde parciales
        for kpi_key in ["anio_creacion", "aum_actual_meur", "num_participes",
                         "num_participes_anterior", "ter_pct", "coste_gestion_pct",
                         "coste_deposito_pct", "num_activos_cartera",
                         "concentracion_top10_pct", "clasificacion", "perfil_riesgo",
                         "divisa", "depositario", "fecha_registro", "volatilidad_pct"]:
            if (output.get("kpis") is not None
                    and not output["kpis"].get(kpi_key)
                    and source.get("kpis", {}).get(kpi_key)):
                output["kpis"][kpi_key] = source["kpis"][kpi_key]

        # analisis_consistencia desde PDF (sección 9 + 10)
        periodos_pdf = source.get("analisis_consistencia", {}).get("periodos", [])
        if periodos_pdf:
            existing_periodos = {p.get("periodo"): p
                                 for p in output.get("analisis_consistencia", {}).get("periodos", [])}
            for p in periodos_pdf:
                periodo_key = p.get("periodo", "")
                if periodo_key not in existing_periodos:
                    existing_periodos[periodo_key] = p
                else:
                    # Merge: fill empty fields from PDF data
                    for k, v in p.items():
                        if v and not existing_periodos[periodo_key].get(k):
                            existing_periodos[periodo_key][k] = v
            output.setdefault("analisis_consistencia", {})["periodos"] = sorted(
                existing_periodos.values(), key=lambda x: x.get("periodo", ""), reverse=True
            )

        # Fuentes
        if "fuentes" in source and "fuentes" in output:
            for fk in ["informes_descargados", "cartas_gestores",
                       "urls_consultadas", "xmls_cnmv"]:
                src_list = source["fuentes"].get(fk, [])
                if src_list:
                    existing = set(output["fuentes"].get(fk, []))
                    for item in src_list:
                        if item not in existing:
                            output["fuentes"].setdefault(fk, []).append(item)
                            existing.add(item)

        # Cartas → enriquecer posiciones y cualitativo
        if "cartas" in source:
            self._enrich_from_letters(output, source["cartas"])

    def _enrich_from_letters(self, output: dict, cartas: list) -> None:
        """Extrae info de cartas para enriquecer posiciones y cualitativo."""
        if not cartas:
            return

        # Tesis de inversión de la carta más reciente
        for carta in cartas:
            if carta.get("tesis_inversion") and not output.get("cualitativo", {}).get("filosofia_inversion"):
                output.setdefault("cualitativo", {})["filosofia_inversion"] = carta["tesis_inversion"]
            if carta.get("perspectivas") and not output.get("cualitativo", {}).get("objetivos_reales"):
                output.setdefault("cualitativo", {})["objetivos_reales"] = carta["perspectivas"]

        # Posiciones comentadas en cartas → enriquecer racional de posiciones actuales
        for carta in cartas:
            for pos_carta in carta.get("posiciones_comentadas", []) or []:
                nombre = pos_carta.get("nombre", "")
                if not nombre:
                    continue
                # Buscar en posiciones actuales
                actuales = output.get("posiciones", {}).get("actuales", [])
                for pos in actuales:
                    if nombre.lower() in (pos.get("nombre") or "").lower():
                        if not pos.get("racional"):
                            pos["racional"] = pos_carta.get("racional", "")

    # ── Claude enrichment ─────────────────────────────────────────────────────

    def _enrich_with_claude(self, output: dict) -> None:
        """
        Genera el análisis completo estilo briefing de analista senior.
        8 secciones basadas en feedback/analyst_agent.txt.
        Si no hay API key → deja campos null y continúa.
        """
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key:
            self._log("WARN", "ANTHROPIC_API_KEY no configurada — campos cualitativos quedarán null")
            return

        self._log("START", "Generando análisis estilo briefing de analista senior")

        # ── Build comprehensive context from ALL available data ───────────────
        context_text = self._build_full_context(output)

        # ── ANALYST BRIEFING: 8 sections ─────────────────────────────────────
        # Each section is generated separately for quality and token efficiency

        nombre = output.get("nombre", self.isin)
        gestora = output.get("gestora", "")

        analyst_base_ctx = (
            f"Fondo: {nombre} ({self.isin}), Gestora: {gestora}. "
            "Eres un analista senior de fondos de inversión escribiendo un briefing para "
            "el comité de inversión. Máxima densidad de información, cero filler. "
            "Prioriza ESPECIFICIDAD sobre generalidad. Separa HECHOS de OPINIONES. "
            "Ignora disclaimers, regulatorio genérico y marketing."
        )

        # Section 1: Fund Snapshot (mostly from structured data, fill gaps)
        kpis_faltantes = [k for k in ["clasificacion", "benchmark"]
                          if not output.get("kpis", {}).get(k)]
        if kpis_faltantes:
            try:
                result = extract_structured_data(
                    context_text[:3000],
                    {k: f"valor de {k}" for k in kpis_faltantes},
                    context=f"KPIs del fondo {nombre}. clasificacion=categoría CNMV/Morningstar. benchmark=índice de referencia.",
                )
                if isinstance(result, dict):
                    for k, v in result.items():
                        if v:
                            output.setdefault("kpis", {})[k] = v
            except Exception as exc:
                self._log("WARN", f"Error KPIs: {exc}")

        # Section 2: Investment Philosophy
        cual = output.get("cualitativo", {})
        if not cual.get("filosofia_inversion"):
            try:
                result = extract_structured_data(
                    context_text[:6000],
                    {"filosofia_inversion": (
                        "Análisis de la filosofía de inversión REAL observada, no la declaración de marketing. "
                        "En 200-300 palabras: qué busca el gestor (métricas concretas como ROCE, PER, net cash), "
                        "qué evita, actitud hacia macro vs bottom-up, horizonte temporal, "
                        "concentración habitual (número de posiciones), uso de cash/derivados. "
                        "Basarse en las decisiones históricas y evolución de cartera, no en el folleto."
                    )},
                    context=analyst_base_ctx,
                )
                if isinstance(result, dict) and result.get("filosofia_inversion"):
                    output.setdefault("cualitativo", {})["filosofia_inversion"] = result["filosofia_inversion"]
            except Exception as exc:
                self._log("WARN", f"Error filosofía: {exc}")

        # Section 2b: Estrategia (complementa filosofía)
        if not cual.get("estrategia"):
            try:
                result = extract_structured_data(
                    context_text[:5000],
                    {"estrategia": (
                        "Párrafo ejecutivo de 150-200 palabras resumiendo el fondo: clase de activo, "
                        "geografía, estilo (value/growth/blend), nivel de riesgo observado, "
                        "rango típico de posiciones, comportamiento en distintos ciclos de mercado. "
                        "Escribir como analista experimentado, no copiar folleto."
                    ),
                     "tipo_activos": "Tipos de activos específicos (renta variable europea, RF investment grade, etc.)",
                     "objetivos_reales": "Objetivo real de rentabilidad/riesgo inferido de los datos históricos",
                     "proceso_seleccion": (
                        "Proceso de selección observado: criterios de entrada (valoración, calidad, momentum), "
                        "triggers de salida, concentración, rotación. 100-150 palabras."
                     )},
                    context=analyst_base_ctx,
                )
                if isinstance(result, dict):
                    for k, v in result.items():
                        if v:
                            output.setdefault("cualitativo", {})[k] = v
            except Exception as exc:
                self._log("WARN", f"Error estrategia: {exc}")

        # Section 3: Portfolio Managers — web search if not found in PDFs
        gestores_actual = cual.get("gestores", [])
        if not gestores_actual or not any(g.get("nombre") for g in gestores_actual):
            gestores_found = self._search_gestores_web(output)
            if gestores_found:
                output.setdefault("cualitativo", {})["gestores"] = gestores_found

        # Section 8: Analyst Synthesis (green flags, red flags, signal)
        try:
            result = extract_structured_data(
                context_text[:8000],
                {"analyst_synthesis": {
                    "green_flags": ["máximo 5 puntos fuertes concretos del fondo/gestor (no genéricos)"],
                    "red_flags": ["máximo 5 puntos de preocupación: style drift, AUM impact, key-man risk, inconsistencias"],
                    "signal": "STRONG CONVICTION | MONITOR | PASS",
                    "signal_rationale": "párrafo de 2-3 frases justificando la señal con datos concretos",
                }},
                context=(
                    f"{analyst_base_ctx} "
                    "Esta sección es TU juicio analítico, no resumen de lo que dice el gestor. "
                    "Evalúa: consistencia filosofía-acción, impact de AUM en estrategia, key-man risk, "
                    "track record en distintos ciclos, honestidad en comunicación."
                ),
            )
            if isinstance(result, dict) and result.get("analyst_synthesis"):
                output.setdefault("cualitativo", {})["analyst_synthesis"] = result["analyst_synthesis"]
                self._log("OK", "Analyst synthesis generada")
        except Exception as exc:
            self._log("WARN", f"Error analyst synthesis: {exc}")

        self._log("OK", "Enriquecimiento Claude completado")

        # ── Historia del fondo ────────────────────────────────────────────────
        if not output.get("cualitativo", {}).get("historia_fondo"):
            self._generate_historia_fondo(output)

    def _search_gestores_web(self, output: dict) -> list[dict]:
        """
        Search for fund managers via web: Morningstar equipo, Citywire, Trustnet,
        gestora website, and external analyses already downloaded.
        """
        import asyncio
        import urllib.parse
        nombre = output.get("nombre", "")
        fund_short = nombre.split(",")[0].strip() if "," in nombre else nombre

        self._log("START", f"Buscando gestores en web para {fund_short}")

        # Step 1: Extract from existing analisis_externos.json
        gestores_text_hints = []
        for fname in ["analisis_externos.json", "letters_data.json"]:
            fpath = self.fund_dir / fname
            if fpath.exists():
                try:
                    data = json.loads(fpath.read_text(encoding="utf-8"))
                    # Collect all text that might contain gestor names
                    if isinstance(data, list):
                        for item in data:
                            for k in ["titulo", "resumen_generado", "resumen"]:
                                if item.get(k):
                                    gestores_text_hints.append(item[k][:500])
                    elif isinstance(data, dict):
                        for carta in data.get("cartas", []):
                            for k in ["url_fuente", "tesis_inversion", "resumen_mercado"]:
                                if carta.get(k):
                                    gestores_text_hints.append(str(carta[k])[:300])
                except Exception:
                    pass

        # Step 2: DDG search for gestores
        web_texts = []
        try:
            from tools.http_client import get_with_headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept-Language": "es-ES,es;q=0.9",
            }

            queries = [
                f'"{fund_short}" gestor equipo manager',
                f'"{fund_short}" morningstar equipo',
                f'"{fund_short}" citywire manager',
                f'"{fund_short}" trustnet manager',
            ]

            for query in queries[:3]:
                try:
                    enc_q = urllib.parse.quote_plus(query)
                    ddg_url = f"https://html.duckduckgo.com/html/?q={enc_q}"
                    html = asyncio.get_event_loop().run_until_complete(
                        get_with_headers(ddg_url, headers)
                    ) if not asyncio.get_event_loop().is_running() else ""

                    # If we can't do async from sync context, try sync
                    if not html:
                        import httpx
                        resp = httpx.get(ddg_url, headers=headers, timeout=15, follow_redirects=True)
                        html = resp.text

                    if html:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(html, "html.parser")
                        for result_div in soup.select(".result__snippet")[:3]:
                            web_texts.append(result_div.get_text(strip=True))
                        # Try to fetch first Morningstar/Citywire/Trustnet result
                        for a in soup.select("a.result__a")[:5]:
                            href = a.get("href", "")
                            if any(site in href for site in ["morningstar", "citywire", "trustnet"]):
                                # Extract actual URL from DDG redirect
                                if "uddg=" in href:
                                    href = urllib.parse.unquote(href.split("uddg=")[1].split("&")[0])
                                try:
                                    page = httpx.get(href, headers=headers, timeout=15, follow_redirects=True)
                                    page_soup = BeautifulSoup(page.text, "html.parser")
                                    # Get text from manager/team sections
                                    page_text = page_soup.get_text(separator=" ", strip=True)[:3000]
                                    web_texts.append(page_text)
                                    self._log("INFO", f"Fetched gestor page: {href[:60]}")
                                except Exception:
                                    pass
                                break
                except Exception as exc:
                    self._log("WARN", f"DDG search error: {exc}")

        except Exception as exc:
            self._log("WARN", f"Web search for gestores failed: {exc}")

        # Step 3: Send all collected text to Claude for extraction
        all_text = "\n---\n".join(gestores_text_hints + web_texts)
        if not all_text.strip():
            self._log("WARN", "No text found for gestor extraction")
            return []

        try:
            result = extract_structured_data(
                all_text[:6000],
                {"gestores": [{
                    "nombre": "nombre completo de la persona física que gestiona el fondo",
                    "cargo": "cargo: gestor principal, asesor, portfolio manager, CIO",
                    "background": "trayectoria profesional: firmas anteriores, experiencia, formación",
                    "anio_incorporacion": None,
                }]},
                context=(
                    f"Extraer el equipo gestor del fondo {fund_short}. "
                    "Buscar nombres de PERSONAS FÍSICAS que gestionan o asesoran este fondo. "
                    "Fuentes: artículos de análisis, Morningstar, Citywire, Trustnet, web gestora. "
                    "NO incluir sociedades gestoras como personas."
                ),
            )
            if isinstance(result, dict) and result.get("gestores"):
                gestores = [g for g in result["gestores"] if g and g.get("nombre")]
                if gestores:
                    self._log("OK", f"Gestores encontrados via web: {[g['nombre'] for g in gestores]}")
                    return gestores
        except Exception as exc:
            self._log("WARN", f"Claude gestor extraction failed: {exc}")

        return []

    def _build_full_context(self, output: dict) -> str:
        """Build comprehensive text context from all available data for Claude."""
        parts = []
        nombre = output.get("nombre", self.isin)
        gestora = output.get("gestora", "")
        tipo = output.get("tipo", "")
        parts.append(f"Fondo: {nombre} ({self.isin}), Gestora: {gestora}, Tipo: {tipo}")

        kpis = output.get("kpis", {})
        if kpis:
            parts.append(f"KPIs: AUM={kpis.get('aum_actual_meur')} M€, "
                         f"Partícipes={kpis.get('num_participes')}, "
                         f"Comisión gestión={kpis.get('coste_gestion_pct')}%, "
                         f"TER={kpis.get('ter_pct')}%, "
                         f"Año creación={kpis.get('anio_creacion')}, "
                         f"Rotación cartera={kpis.get('rotacion_cartera_pct')}")

        cuant = output.get("cuantitativo", {})
        aum_series = cuant.get("serie_aum", [])
        if aum_series:
            aum_txt = ", ".join(f"{e['periodo']}: {e.get('valor_meur','')}M€" for e in sorted(aum_series, key=lambda x: x.get("periodo",""))[-8:])
            parts.append(f"Evolución AUM: {aum_txt}")

        rent_series = cuant.get("serie_rentabilidad", [])
        if rent_series:
            parts.append(f"Rentabilidad: {rent_series[:6]}")

        mix_hist = cuant.get("mix_activos_historico", [])
        if mix_hist:
            mix_txt = "; ".join(
                f"{m.get('periodo')}: RV={m.get('rv_pct','?')}% RF={m.get('renta_fija_pct','?')}% Liq={m.get('liquidez_pct','?')}%"
                for m in sorted(mix_hist, key=lambda x: str(x.get("periodo","")))[-6:]
            )
            parts.append(f"Mix activos histórico: {mix_txt}")

        cual = output.get("cualitativo", {})
        for k in ["estrategia", "filosofia_inversion", "tipo_activos"]:
            if cual.get(k):
                parts.append(f"{k}: {str(cual[k])[:300]}")

        pos = output.get("posiciones", {}).get("actuales", [])
        if pos:
            pos_txt = "\n".join(f"  {p.get('nombre','')} ({p.get('ticker','')}) {p.get('peso_pct','')}% {p.get('sector','')}" for p in pos[:15])
            parts.append(f"Top posiciones ({len(pos)} total):\n{pos_txt}")

        periodos = output.get("analisis_consistencia", {}).get("periodos", [])
        if periodos:
            for p in sorted(periodos, key=lambda x: x.get("periodo",""))[-6:]:
                periodo_parts = [f"[{p.get('periodo','')}]"]
                if p.get("tesis_gestora"):
                    periodo_parts.append(f"Tesis: {p['tesis_gestora'][:200]}")
                if p.get("decisiones_tomadas"):
                    periodo_parts.append(f"Decisiones: {p['decisiones_tomadas'][:200]}")
                if p.get("contexto_mercado"):
                    periodo_parts.append(f"Mercado: {p['contexto_mercado'][:150]}")
                parts.append(" | ".join(periodo_parts))

        # Letters data
        letters_path = self.fund_dir / "letters_data.json"
        if letters_path.exists():
            try:
                letters_data = json.loads(letters_path.read_text(encoding="utf-8"))
                cartas = letters_data.get("cartas", [])
                for carta in cartas[:3]:
                    tesis = carta.get("tesis_inversion", "")
                    if tesis:
                        parts.append(f"Carta [{carta.get('periodo','')}]: {tesis[:300]}")
            except Exception:
                pass

        # Readings data
        for fname in ["lecturas.json", "analisis_externos.json"]:
            fpath = self.fund_dir / fname
            if fpath.exists():
                try:
                    data = json.loads(fpath.read_text(encoding="utf-8"))
                    for item in (data.get("lecturas", []) + data.get("analisis", []))[:3]:
                        resumen = item.get("resumen_generado", "") or item.get("resumen", "")
                        if resumen:
                            parts.append(f"Análisis externo [{item.get('fuente','')}]: {resumen[:300]}")
                except Exception:
                    pass

        return "\n".join(parts)

    def _generate_historia_fondo(self, output: dict) -> None:
        """
        Genera narrativa histórica del fondo con Claude (400-600 palabras).
        Combina: hechos_relevantes, kpis, gestores, periodos de consistencia.
        Solo ejecuta si hay API key.
        """
        import os
        if not os.getenv("ANTHROPIC_API_KEY", ""):
            return

        nombre = output.get("nombre", self.isin)
        kpis = output.get("kpis", {})
        cual = output.get("cualitativo", {})
        gestores = cual.get("gestores", [])
        hechos = cual.get("hechos_relevantes", [])
        periodos = output.get("analisis_consistencia", {}).get("periodos", [])
        aum_series = output.get("cuantitativo", {}).get("serie_aum", [])

        # Build rich context from all available data
        ctx_parts = [
            f"Fondo: {nombre} ({self.isin})",
            f"Gestora: {output.get('gestora', '')}",
            f"Año creación: {kpis.get('anio_creacion', '')}",
            f"Fecha registro: {kpis.get('fecha_registro', '')}",
            f"AUM actual: {kpis.get('aum_actual_meur', '')} M€",
            f"Partícipes: {kpis.get('num_participes', '')}",
            f"Estrategia: {(cual.get('estrategia') or cual.get('filosofia_inversion') or '')[:500]}",
        ]
        if gestores:
            g_names = [g.get("nombre") for g in gestores if g.get("nombre")]
            ctx_parts.append(f"Gestores: {', '.join(g_names)}")
        if hechos:
            hechos_txt = "\n".join(
                f"[{h.get('periodo', '')}] {h.get('epigrafe', '')} — {h.get('detalle', '')[:300]}"
                for h in sorted(hechos, key=lambda x: x.get("periodo", ""))
                if h.get("detalle") or h.get("epigrafe")
            )
            ctx_parts.append(f"Hechos relevantes registrados:\n{hechos_txt}")
        if aum_series:
            aum_summary = ", ".join(
                f"{e['periodo']}: {e.get('valor_meur', '')} M€"
                for e in sorted(aum_series, key=lambda x: x.get("periodo", ""))
            )
            ctx_parts.append(f"Evolución patrimonio: {aum_summary}")
        if periodos:
            per_summary = "\n".join(
                f"[{p.get('periodo', '')}] {(p.get('tesis_gestora') or p.get('contexto_mercado') or '')[:200]}"
                for p in sorted(periodos, key=lambda x: x.get("periodo", ""))[-6:]
                if p.get("tesis_gestora") or p.get("contexto_mercado")
            )
            ctx_parts.append(f"Visión gestores por periodo:\n{per_summary}")

        context_text = "\n".join(ctx_parts)

        try:
            result = extract_structured_data(
                context_text,
                {
                    "historia_fondo": (
                        "Narrativa detallada de 400-600 palabras sobre la historia del fondo. "
                        "Incluir: origen y motivación del fundador, estructura inicial (SICAV/FI), "
                        "estrategia de inversión y sus pilares, transformaciones clave (cambios regulatorios, "
                        "de estructura, de comisiones, lanzamientos de nuevos vehículos), "
                        "comportamiento en momentos de mercado relevantes, estado actual. "
                        "Tono profesional e informativo. En español. "
                        "Basarte SOLO en los datos proporcionados, no inventes hechos."
                    )
                },
                context=f"Historia del fondo de inversión {nombre} ({self.isin}). "
                        f"Analiza los datos disponibles y construye una narrativa cronológica y coherente.",
            )
            if isinstance(result, dict) and result.get("historia_fondo"):
                output.setdefault("cualitativo", {})["historia_fondo"] = result["historia_fondo"]
                self._log("OK", "Historia del fondo generada con Claude")
        except Exception as exc:
            self._log("WARN", f"Error generando historia_fondo: {exc}")

    # ── Análisis de consistencia ───────────────────────────────────────────────

    def _build_consistency_analysis(self, output: dict) -> None:
        """
        Construye analisis_consistencia con los datos disponibles.
        Si ya hay periodos desde PDF (sección 9) → usarlos como base.
        Si hay API key → Claude genera resumen_global y score.
        Si no → rellena con datos objetivos disponibles.
        """
        import os
        periodos_data: list[dict] = []

        # Use existing periodos from PDF extraction if available
        existing_periodos = output.get("analisis_consistencia", {}).get("periodos", [])
        if existing_periodos:
            periodos_data = existing_periodos
            # Try to generate resumen_global if API key available
            api_key = os.getenv("ANTHROPIC_API_KEY", "")
            if api_key and periodos_data:
                try:
                    # Build rich context: tesis + decisiones longitudinales
                    tesis_lines = "\n".join(
                        f"[{p.get('periodo','')}] Tesis: {(p.get('tesis_gestora') or '')[:200]} | "
                        f"Decisiones: {(p.get('decisiones_tomadas') or '')[:150]}"
                        for p in sorted(periodos_data, key=lambda x: x.get("periodo",""))
                        if p.get("tesis_gestora") or p.get("decisiones_tomadas")
                    )
                    ctx = (
                        f"Fondo {output.get('nombre', self.isin)} ({self.isin}). "
                        f"Gestora: {output.get('gestora','')}. "
                        f"Periodos analizados: {len(periodos_data)}. "
                        f"Evolución tesis y decisiones:\n{tesis_lines[:3000]}"
                    )
                    r = extract_structured_data(
                        ctx,
                        {"resumen_global": (
                            "Síntesis de 3-5 frases del track record del gestor: "
                            "consistencia de la filosofía a lo largo del tiempo, cómo gestiona "
                            "distintos entornos de mercado, fortalezas y debilidades observadas. "
                            "Basarse en la evolución real de tesis y decisiones descritas arriba."
                        )},
                        context="Análisis de consistencia longitudinal del fondo.",
                    )
                    resumen = r.get("resumen_global") if isinstance(r, dict) else None
                    if resumen:
                        output.setdefault("analisis_consistencia", {})["resumen_global"] = resumen
                except Exception:
                    pass
            self._log("OK", f"Análisis consistencia: {len(periodos_data)} periodos (desde PDF)")
            return

        rent_series = output.get("cuantitativo", {}).get("serie_rentabilidad", [])
        aum_series = output.get("cuantitativo", {}).get("serie_aum", [])

        # Agrupar por año
        rent_by_year: dict[str, dict] = {}
        for r in rent_series:
            yr = str(r.get("periodo", ""))[:4]
            if yr and yr.isdigit():
                rent_by_year[yr] = r

        aum_by_period: dict[str, float] = {}
        for a in aum_series:
            p = str(a.get("periodo", ""))
            aum_by_period[p] = a.get("valor_meur", 0)

        # Cartas como fuente de tesis
        cartas = []
        letters_path = self.fund_dir / "letters_data.json"
        if letters_path.exists():
            try:
                letters_data = json.loads(letters_path.read_text(encoding="utf-8"))
                cartas = letters_data.get("cartas", [])
            except Exception:
                pass

        cartas_by_year: dict[str, dict] = {}
        for c in cartas:
            yr = str(c.get("periodo", ""))[:4]
            if yr and yr.isdigit():
                cartas_by_year[yr] = c

        # Construir periodos (años con datos de rentabilidad)
        years_with_data = sorted(rent_by_year.keys(), reverse=True)

        # Si no hay datos de rentabilidad, usar años con AUM
        if not years_with_data:
            years_with_data = sorted(
                {str(p)[:4] for p in aum_by_period if str(p)[:4].isdigit()},
                reverse=True
            )[:3]

        api_key = os.getenv("ANTHROPIC_API_KEY", "")

        for yr in years_with_data[:6]:  # máx 6 periodos
            rent = rent_by_year.get(yr, {})
            carta = cartas_by_year.get(yr, {})

            periodo_data = {
                "periodo": yr,
                "contexto_mercado": None,
                "tesis_gestora": carta.get("tesis_inversion") or carta.get("resumen_mercado"),
                "decisiones_tomadas": carta.get("decisiones_cartera"),
                "resultado_real": (f"Rentabilidad: {rent.get('rentabilidad_pct')}%"
                                   if rent.get("rentabilidad_pct") is not None else None),
                "consistencia_score": None,
                "notas": "",
            }

            # Si hay Claude, calcular consistencia
            if api_key and periodo_data["tesis_gestora"] and periodo_data["resultado_real"]:
                try:
                    score_schema = {
                        "contexto_mercado": "breve descripción del contexto de mercado de ese año",
                        "consistencia_score": "puntuación de 1-10 de consistencia tesis vs resultado",
                        "notas": "explicación del score",
                    }
                    ctx = (f"Año {yr}, fondo {output.get('nombre', self.isin)}. "
                           f"Tesis gestora: {periodo_data['tesis_gestora'][:300]}. "
                           f"Resultado: {periodo_data['resultado_real']}.")
                    scored = extract_structured_data(ctx, score_schema,
                                                     context="Evalúa consistencia tesis vs resultado.")
                    if isinstance(scored, dict):
                        periodo_data.update({k: v for k, v in scored.items() if v})
                except Exception:
                    pass

            periodos_data.append(periodo_data)

        # Resumen global
        resumen = None
        if api_key and periodos_data:
            try:
                ctx = (f"Fondo {output.get('nombre', self.isin)}. "
                       f"Periodos analizados: {len(periodos_data)}. "
                       f"Datos disponibles: {[p['periodo'] for p in periodos_data]}.")
                r = extract_structured_data(
                    ctx,
                    {"resumen_global": "resumen del track record y consistencia del gestor"},
                    context="Síntesis del análisis de consistencia del fondo.",
                )
                resumen = r.get("resumen_global") if isinstance(r, dict) else None
            except Exception:
                pass

        if periodos_data:
            output["analisis_consistencia"] = {
                "periodos": periodos_data,
                "resumen_global": resumen,
            }
            self._log("OK", f"Análisis consistencia: {len(periodos_data)} periodos")
        else:
            self._log("INFO", "Sin datos suficientes para análisis de consistencia")

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _count_fields(self, obj, path="") -> tuple[int, list[str]]:
        """Cuenta campos completados vs null recursivamente."""
        completed = 0
        nulls = []

        if isinstance(obj, dict):
            for k, v in obj.items():
                p = f"{path}.{k}" if path else k
                c, n = self._count_fields(v, p)
                completed += c
                nulls.extend(n)
        elif isinstance(obj, list):
            if obj:
                completed += 1
            else:
                nulls.append(path)
        elif obj is None or obj == "":
            nulls.append(path)
        else:
            completed += 1

        return completed, nulls

    def _save(self, output: dict) -> None:
        out_path = self.fund_dir / "output.json"
        out_path.write_text(
            json.dumps(output, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self._log("OK", f"output.json guardado en {out_path}")


# ── Standalone ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    for isin in ["ES0112231008"]:
        agent = AnalystAgent(isin, {})
        result = agent.run()
        console.print(Panel(
            f"[green]output.json generado[/green]\n"
            f"Nombre: {result.get('nombre', '-')}\n"
            f"Tipo: {result.get('tipo', '-')}\n"
            f"AUM: {result.get('kpis', {}).get('aum_actual_meur', '-')} M€",
            title=isin, expand=False,
        ))
