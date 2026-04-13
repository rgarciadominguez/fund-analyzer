"""
Analyst Agent — Síntesis final del análisis del fondo

Arquitectura de 4 capas:
  CAPA 1: Filtradores especializados (4 en paralelo)
    1A. CNMV: cuantitativo completo + cualitativo único por año
    1B. Cartas: visión, decisiones, cambios por año sin duplicados
    1C. Gestores: perfil completo de cada persona
    1D. Lecturas: URL + resumen de cada fuente externa
  CAPA 2: Consolidador
    - Junta, deduplica, ordena cronológicamente
    - Verifica que todo es del fondo correcto
  CAPA 3: Analyst Senior — 8 secciones via Gemini (1 llamada/sección)
  CAPA 3b: Gemini Quality Checker — verifica cuantitativo, consistencia, redacción
  CAPA 4: Presentación ejecutiva (pospuesta)
"""
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

console = Console()


class AnalystAgent:

    def __init__(self, isin: str, config: dict = None, quality_feedback: list = None):
        self.isin = isin.strip().upper()
        self.config = config or {}
        # quality_feedback: lista de fallos del DashboardQualityAgent (re-ejecución)
        # Cada fallo: {seccion, problema, agente_responsable, accion, prioridad}
        self.quality_feedback = quality_feedback or []
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.schema_path = root / "schemas" / "fund_output_v2.json"
        self.log_path = root / "progress.log"

    def _quality_hint(self, section: str) -> str:
        """Build a 'CORRECCIONES OBLIGATORIAS' block to inject in prompts when re-running.
        Only includes fallos targeted at analyst_agent for the given section."""
        if not self.quality_feedback:
            return ""
        relevant = [f for f in self.quality_feedback
                    if f.get("seccion") == section
                    and f.get("agente_responsable") == "analyst_agent"]
        if not relevant:
            return ""
        lines = ["", "CORRECCIONES OBLIGATORIAS (iteración previa marcó fallos):"]
        for f in relevant:
            prob = f.get("problema", "")
            acc = f.get("accion", "")
            prio = f.get("prioridad", "")
            lines.append(f"- [{prio}] {prob} → {acc}")
        lines.append("DEBES corregir TODOS los fallos arriba listados en esta nueva versión.")
        lines.append("")
        return "\n".join(lines)

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [ANALYST] [{level}] {msg}"
        console.log(line)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    # ═══════════════════════════════════════════════════════════════════════════
    # ENTRY POINT
    # ═══════════════════════════════════════════════════════════════════════════

    def run(self) -> dict:
        self._log("START", f"Analyst Agent — {self.isin}")

        # Load all raw data from other agents
        cnmv = self._load_json("cnmv_data.json")
        letters = self._load_json("letters_data.json")
        manager = self._load_json("manager_profile.json")
        readings = self._load_json("readings_data.json")

        # ── CAPA 1: Filtradores especializados ───────────────────────────────
        self._log("START", "Capa 1: Filtradores especializados")
        filtered_cnmv = self._filter_cnmv(cnmv)
        filtered_letters = self._filter_letters(letters)
        filtered_gestores = self._filter_gestores(manager)
        filtered_lecturas = self._filter_lecturas(readings)
        self._log("OK", "Capa 1 completada")

        # ── CAPA 2: Consolidador ─────────────────────────────────────────────
        self._log("START", "Capa 2: Consolidador")
        consolidated = self._consolidate(
            filtered_cnmv, filtered_letters, filtered_gestores, filtered_lecturas,
            fund_name=cnmv.get("nombre", ""),
            gestora=cnmv.get("gestora", ""),
        )
        self._log("OK", "Capa 2 completada")

        # Save consolidated data for capa 3
        consolidated["isin"] = self.isin
        consolidated["nombre"] = cnmv.get("nombre", "")
        consolidated["gestora"] = cnmv.get("gestora", "")
        consolidated["tipo"] = cnmv.get("tipo", "ES")
        consolidated["ultima_actualizacion"] = datetime.now().isoformat()

        # Pass through raw cuantitativo — but PRESERVE existing output.json data
        # if it has MORE data points (e.g. from a previous full pipeline run).
        # This prevents re-running analyst from losing historical series.
        existing_output = self._load_json("output.json")
        existing_cuant = existing_output.get("cuantitativo", {}) if existing_output else {}
        new_cuant = cnmv.get("cuantitativo", {})

        # Merge: for each series, keep whichever has more data points
        merged_cuant = {}
        all_keys = set(list(existing_cuant.keys()) + list(new_cuant.keys()))
        for key in all_keys:
            old_val = existing_cuant.get(key)
            new_val = new_cuant.get(key)
            if isinstance(old_val, list) and isinstance(new_val, list):
                merged_cuant[key] = old_val if len(old_val) >= len(new_val) else new_val
            elif isinstance(old_val, list) and not new_val:
                merged_cuant[key] = old_val
            elif isinstance(new_val, list) and not old_val:
                merged_cuant[key] = new_val
            else:
                merged_cuant[key] = new_val if new_val is not None else old_val

        consolidated["cuantitativo"] = merged_cuant
        consolidated["kpis"] = cnmv.get("kpis", {}) or existing_output.get("kpis", {})

        # Same for posiciones — keep richer data
        new_pos = cnmv.get("posiciones", {})
        old_pos = existing_output.get("posiciones", {}) if existing_output else {}
        merged_pos = {}
        for key in set(list(new_pos.keys()) + list(old_pos.keys())):
            ov = old_pos.get(key, [])
            nv = new_pos.get(key, [])
            if isinstance(ov, list) and isinstance(nv, list):
                merged_pos[key] = ov if len(ov) >= len(nv) else nv
            else:
                merged_pos[key] = nv if nv else ov
        consolidated["posiciones"] = merged_pos
        consolidated["fuentes"] = cnmv.get("fuentes", {}) or existing_output.get("fuentes", {})

        # ── CAPA 3: Analyst Senior — 8 secciones ────────────────────────
        self._log("START", "Capa 3: Síntesis Analyst Senior (8 secciones)")
        synthesis = self._run_capa3(consolidated)
        consolidated["analyst_synthesis"] = synthesis
        self._log("OK", f"Capa 3: {synthesis.get('sections_completed', 0)}/8 secciones")

        # ── CAPA 3b: Quality Checker ─────────────────────────────────────
        self._log("START", "Capa 3b: Quality Checker")
        check_result = self._run_checker(synthesis, consolidated)
        if check_result:
            consolidated["analyst_synthesis"] = self._apply_corrections(synthesis, check_result)
            self._log("OK", f"Capa 3b: score {check_result.get('score', {}).get('global', '?')}/10")
        else:
            self._log("WARN", "Capa 3b: checker no devolvió resultado")

        self._save(consolidated)
        self._print_summary(consolidated)
        return consolidated

    # ═══════════════════════════════════════════════════════════════════════════
    # CAPA 1A: FILTRADOR CNMV
    # ═══════════════════════════════════════════════════════════════════════════

    def _filter_cnmv(self, cnmv: dict) -> dict:
        """Extract relevant qualitative info from CNMV semiannual reports.
        Cuantitativo passes through untouched.
        Qualitative: extract unique content per year, avoid repetition."""
        result = {
            "hechos_relevantes": [],
            "vision_por_anio": [],
        }

        # Hechos relevantes — structured timeline
        cual = cnmv.get("cualitativo", {})
        for hr in cual.get("hechos_relevantes", []):
            if not isinstance(hr, dict):
                continue
            result["hechos_relevantes"].append({
                "anio": hr.get("periodo", ""),
                "evento": hr.get("epigrafe", ""),
                "detalle": hr.get("detalle", ""),
            })

        # Cualitativo per year — extract unique content, flag what changes
        periodos = cnmv.get("analisis_consistencia", {}).get("periodos", [])
        prev_vision = ""
        for p in sorted(periodos, key=lambda x: x.get("periodo", "")):
            anio = p.get("periodo", "")
            sec9 = p.get("seccion_9_texto", "") or ""
            sec10 = p.get("seccion_10_texto", "") or ""
            sec1 = p.get("seccion_1_texto", "") or ""

            # Detect if vision changed vs previous year
            current_vision = sec9[:500]
            is_new = current_vision != prev_vision and len(current_vision) > 100
            prev_vision = current_vision

            entry = {
                "anio": anio,
                "seccion_9_vision_mercado_y_decisiones": sec9,
                "seccion_10_perspectivas": sec10,
                "seccion_1_politica": sec1 if is_new else "",  # Only include if changed
                "cambio_detectado": is_new,
            }
            result["vision_por_anio"].append(entry)

        # Most recent qualitative texts (for direct use)
        result["estrategia_actual"] = cual.get("estrategia", "")
        result["seccion_9_mas_reciente"] = cual.get("seccion_9_texto_completo", "")
        result["seccion_10_mas_reciente"] = cual.get("seccion_10_perspectivas_texto", "")

        n_hechos = len(result["hechos_relevantes"])
        n_years = len(result["vision_por_anio"])
        self._log("INFO", f"CNMV filtrado: {n_hechos} hechos, {n_years} años de visión")
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # CAPA 1B: FILTRADOR CARTAS
    # ═══════════════════════════════════════════════════════════════════════════

    def _filter_letters(self, letters: dict) -> dict:
        """Extract key content from manager letters, avoiding repetition.
        Per year: vision, decisions, reflection on what happened vs expectations,
        changes in team/rules/fund, key metrics mentioned."""
        result = {"cartas_por_anio": {}}

        cartas = letters.get("cartas", [])
        if not cartas:
            return result

        # Group by year
        by_year: dict[str, list[dict]] = {}
        for carta in cartas:
            if not isinstance(carta, dict):
                continue
            periodo = carta.get("periodo", "") or ""
            year = periodo[:4] if periodo else ""
            if not year:
                continue
            by_year.setdefault(year, []).append(carta)

        # For each year: consolidate unique info from all cartas of that year
        for year in sorted(by_year.keys()):
            year_cartas = by_year[year]
            year_entry = {
                "anio": year,
                "num_cartas": len(year_cartas),
                "vision_mercado": "",
                "decisiones_cartera": "",
                "reflexion_resultado_vs_expectativa": "",
                "cambios_fondo": "",  # equipo, reglas, comisiones
                "metricas_mencionadas": "",  # rentabilidad, AUM, partícipes
                "tesis_principales": "",
                "fuentes": [],
            }

            # Collect all text content from this year's letters
            all_texts = []
            for carta in year_cartas:
                texto = carta.get("texto_completo", "")
                if texto and len(texto) > 200:
                    all_texts.append(texto)
                year_entry["fuentes"].append({
                    "url": carta.get("url_fuente", ""),
                    "tipo": carta.get("tipo", ""),
                    "periodo": carta.get("periodo", ""),
                })

            # Combine texts (prefer longest carta as primary, others as supplement)
            if all_texts:
                all_texts.sort(key=len, reverse=True)
                # Primary: longest carta (usually semestral)
                year_entry["texto_primario"] = all_texts[0]
                # Supplementary: other cartas (trimestral/mensual) — only unique parts
                if len(all_texts) > 1:
                    year_entry["textos_complementarios"] = all_texts[1:3]  # Max 2 extras

            result["cartas_por_anio"][year] = year_entry

        n_years = len(result["cartas_por_anio"])
        n_cartas = sum(v["num_cartas"] for v in result["cartas_por_anio"].values())
        self._log("INFO", f"Cartas filtradas: {n_years} años, {n_cartas} cartas total")
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # CAPA 1C: FILTRADOR GESTORES
    # ═══════════════════════════════════════════════════════════════════════════

    def _filter_gestores(self, manager: dict) -> dict:
        """Extract complete manager profiles — everything that helps understand
        who is managing the money: trajectory, philosophy, decisions, commitments."""
        result = {
            "equipo": manager.get("equipo_gestor", []),
            "equipo_detalle_web": manager.get("equipo_detalle_web", []),
            "perfiles": [],
            "fuentes_web": [],
        }

        # Collect all web content about managers
        for page in manager.get("fuentes_web_raw", manager.get("fuentes_web", [])):
            if isinstance(page, dict) and page.get("text"):
                result["fuentes_web"].append({
                    "url": page.get("url", ""),
                    "titulo": page.get("title", ""),
                    "texto": page.get("text", ""),
                })

        # Gemini-extracted info per page
        for info in manager.get("info_extraida_por_fuente", []):
            if isinstance(info, dict) and len(info) > 2:  # More than just _fuente/_titulo
                result["perfiles"].append(info)

        # Info from CNMV and letters about managers
        result["info_cartas"] = manager.get("informacion_cartas", [])
        result["info_cnmv"] = manager.get("informacion_cnmv", {})

        n_equipo = len(result["equipo"])
        n_fuentes = len(result["fuentes_web"])
        n_perfiles = len(result["perfiles"])
        self._log("INFO", f"Gestores filtrados: {n_equipo} personas, {n_fuentes} fuentes, {n_perfiles} perfiles")
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # CAPA 1D: FILTRADOR LECTURAS
    # ═══════════════════════════════════════════════════════════════════════════

    def _filter_lecturas(self, readings: dict) -> dict:
        """For each external source: URL + summary of key topics discussed.
        Split into: análisis (written), multimedia (video/podcast/entrevista)."""
        result = {
            "analisis_escritos": [],
            "multimedia": [],
        }

        for item in readings.get("analisis", []) + readings.get("lecturas", []):
            if not isinstance(item, dict):
                continue

            entry = {
                "fuente": item.get("fuente", ""),
                "tipo": item.get("tipo", ""),
                "titulo": item.get("titulo", ""),
                "url": item.get("url", ""),
                "fecha": item.get("fecha", ""),
                "texto_completo": item.get("texto_completo", ""),
            }

            tipo = item.get("tipo", "")
            if tipo in ("video", "podcast", "entrevista"):
                result["multimedia"].append(entry)
            else:
                result["analisis_escritos"].append(entry)

        n_analisis = len(result["analisis_escritos"])
        n_multi = len(result["multimedia"])
        self._log("INFO", f"Lecturas filtradas: {n_analisis} análisis, {n_multi} multimedia")
        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # CAPA 2: CONSOLIDADOR
    # ═══════════════════════════════════════════════════════════════════════════

    def _consolidate(self, cnmv_f: dict, letters_f: dict,
                     gestores_f: dict, lecturas_f: dict,
                     fund_name: str = "", gestora: str = "") -> dict:
        """Consolidate all filtered data:
        - Merge chronologically
        - Deduplicate (same info from different sources)
        - Verify all content relates to this fund
        - Structure for easy consumption by Capa 3"""
        result = {}

        # ── Timeline cronológico unificado ────────────────────────────────────
        timeline: dict[str, dict] = {}

        # From CNMV (vision por año)
        for entry in cnmv_f.get("vision_por_anio", []):
            anio = entry.get("anio", "")
            if not anio:
                continue
            timeline.setdefault(anio, {
                "anio": anio,
                "cnmv_vision": "",
                "cnmv_perspectivas": "",
                "carta_texto": "",
                "hechos": [],
            })
            timeline[anio]["cnmv_vision"] = entry.get("seccion_9_vision_mercado_y_decisiones", "")
            timeline[anio]["cnmv_perspectivas"] = entry.get("seccion_10_perspectivas", "")

        # From Letters (cartas por año)
        for anio, carta_data in letters_f.get("cartas_por_anio", {}).items():
            timeline.setdefault(anio, {
                "anio": anio,
                "cnmv_vision": "",
                "cnmv_perspectivas": "",
                "carta_texto": "",
                "hechos": [],
            })
            timeline[anio]["carta_texto"] = carta_data.get("texto_primario", "")
            timeline[anio]["carta_fuentes"] = carta_data.get("fuentes", [])
            timeline[anio]["num_cartas"] = carta_data.get("num_cartas", 0)

        # From CNMV hechos relevantes
        for hr in cnmv_f.get("hechos_relevantes", []):
            anio = hr.get("anio", "")[:4]
            if anio in timeline:
                timeline[anio]["hechos"].append(hr)

        result["timeline"] = [timeline[k] for k in sorted(timeline.keys())]

        # ── Hechos relevantes (timeline separado) ─────────────────────────────
        result["hechos_relevantes"] = cnmv_f.get("hechos_relevantes", [])

        # ── Equipo gestor consolidado ─────────────────────────────────────────
        result["gestores"] = gestores_f

        # ── Lecturas externas ─────────────────────────────────────────────────
        result["lecturas_externas"] = lecturas_f

        # ── Info actual (más reciente) ────────────────────────────────────────
        result["estrategia_actual"] = cnmv_f.get("estrategia_actual", "")
        result["seccion_9_mas_reciente"] = cnmv_f.get("seccion_9_mas_reciente", "")
        result["seccion_10_mas_reciente"] = cnmv_f.get("seccion_10_mas_reciente", "")

        # ── Stats ─────────────────────────────────────────────────────────────
        n_years = len(result["timeline"])
        n_hechos = len(result["hechos_relevantes"])
        total_chars = sum(
            len(t.get("cnmv_vision", "")) + len(t.get("carta_texto", ""))
            for t in result["timeline"]
        )
        n_lecturas = (len(lecturas_f.get("analisis_escritos", [])) +
                      len(lecturas_f.get("multimedia", [])))

        self._log("INFO", f"Consolidado: {n_years} años timeline, {n_hechos} hechos, "
                         f"{total_chars//1000}K chars contenido, {n_lecturas} lecturas externas")

        return result

    # ═══════════════════════════════════════════════════════════════════════════
    # CAPA 3: LLM SYNTHESIS — 8 SECCIONES (multi-tier: Sonnet T1, Flash T2, Flash-lite T3)
    # ═══════════════════════════════════════════════════════════════════════════

    # Model tiers:
    #   T1 (Sonnet or Flash) — critical narratives: resumen.texto, estrategia.texto, gestores.trayectoria
    #   T2 (Flash)  — secondary narratives: historia, cartera, fuentes, filosofia
    #   T3 (Lite)   — mechanical extraction: JSON (criterios, hitos, quotes, cv_bullets)
    # Set USE_SONNET=true in .env to enable Claude Sonnet for T1 (requires Anthropic credits)
    GEMINI_FLASH = "gemini-2.5-flash"
    GEMINI_LITE = "gemini-2.5-flash-lite"
    SONNET_MODEL = "claude-sonnet-4-5-20241022"
    USE_SONNET = os.getenv("USE_SONNET", "").lower() in ("true", "1", "yes")

    def _get_anthropic_client(self):
        """Get or create Anthropic client for Sonnet calls."""
        if not hasattr(self, '_anthropic_client'):
            import anthropic
            self._anthropic_client = anthropic.Anthropic(
                api_key=os.getenv("ANTHROPIC_API_KEY", "")
            )
        return self._anthropic_client

    def _sonnet_text(self, system: str, prompt: str, max_tokens: int = 8000, retries: int = 2) -> str:
        """Call Claude Sonnet for high-quality text generation (Tier 1).
        Falls back to Gemini Flash if USE_SONNET is not set or Sonnet fails."""
        if not self.USE_SONNET:
            self._log("INFO", "T1 using Flash (USE_SONNET not set)")
            return self._gemini_text(f"{system}\n\n{prompt}", max_tokens, retries)
        try:
            client = self._get_anthropic_client()
        except Exception as exc:
            self._log("WARN", f"Sonnet init failed, falling back to Flash: {exc}")
            return self._gemini_text(f"{system}\n\n{prompt}", max_tokens, retries)

        for attempt in range(retries + 1):
            try:
                resp = client.messages.create(
                    model=self.SONNET_MODEL,
                    max_tokens=max_tokens,
                    system=system,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.2,
                )
                text = resp.content[0].text.strip() if resp.content else ""
                if not text:
                    raise ValueError("Empty Sonnet response")
                self._log("INFO", f"Sonnet T1: {len(text)} chars")
                return text
            except Exception as exc:
                exc_str = str(exc)
                if "overloaded" in exc_str.lower() or "529" in exc_str:
                    wait = 30 * (attempt + 1)
                    self._log("WARN", f"Sonnet overloaded — espera {wait}s")
                    time.sleep(wait)
                elif "rate_limit" in exc_str.lower() or "429" in exc_str:
                    wait = 45 * (attempt + 1)
                    self._log("WARN", f"Sonnet rate limit — espera {wait}s")
                    time.sleep(wait)
                elif attempt < retries:
                    self._log("WARN", f"Sonnet error (intento {attempt+1}): {exc}")
                    time.sleep(5)
                else:
                    self._log("WARN", f"Sonnet falló, falling back to Flash: {exc}")
                    return self._gemini_text(f"{system}\n\n{prompt}", max_tokens, retries=1)
        # Fallback to Flash
        return self._gemini_text(f"{system}\n\n{prompt}", max_tokens, retries=1)

    def _sonnet_call(self, system: str, prompt: str, max_tokens: int = 8000, retries: int = 2) -> dict | None:
        """Call Claude Sonnet for JSON extraction (Tier 1). Falls back to Gemini Flash."""
        if not self.USE_SONNET:
            return self._gemini_call(f"{system}\n\n{prompt}", max_tokens, retries)
        try:
            client = self._get_anthropic_client()
        except Exception as exc:
            self._log("WARN", f"Sonnet init failed, falling back to Flash: {exc}")
            return self._gemini_call(f"{system}\n\n{prompt}", max_tokens, retries)

        for attempt in range(retries + 1):
            try:
                resp = client.messages.create(
                    model=self.SONNET_MODEL,
                    max_tokens=max_tokens,
                    system=system + "\nResponde SOLO JSON válido, sin markdown ni texto adicional.",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                )
                raw = resp.content[0].text.strip() if resp.content else ""
                if not raw:
                    raise ValueError("Empty Sonnet response")
                # Strip markdown code fences if present
                if raw.startswith("```"):
                    raw = re.sub(r"^```(?:json)?\s*", "", raw)
                    raw = re.sub(r"\s*```$", "", raw)
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    repaired = self._repair_json(raw)
                    if repaired:
                        return repaired
                    raise
            except Exception as exc:
                exc_str = str(exc)
                if "429" in exc_str or "rate_limit" in exc_str.lower():
                    wait = 45 * (attempt + 1)
                    self._log("WARN", f"Sonnet rate limit — espera {wait}s")
                    time.sleep(wait)
                elif attempt < retries:
                    self._log("WARN", f"Sonnet JSON error (intento {attempt+1}): {exc}")
                    time.sleep(5)
                else:
                    self._log("WARN", f"Sonnet JSON falló, falling back to Flash: {exc}")
                    return self._gemini_call(f"{system}\n\n{prompt}", max_tokens, retries=1)
        return self._gemini_call(f"{system}\n\n{prompt}", max_tokens, retries=1)

    def _get_gemini_client(self):
        """Get or create Gemini client (new google-genai SDK)."""
        if not hasattr(self, '_gemini_client'):
            from google import genai
            self._gemini_client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY", ""))
        return self._gemini_client

    def _gemini_call(self, prompt: str, max_tokens: int = 8000, retries: int = 2, tier: str = "standard") -> dict | None:
        """Call Gemini with JSON response. tier='lite' uses Flash-lite (T3), 'standard' uses Flash (T2)."""
        model = self.GEMINI_LITE if tier == "lite" else self.GEMINI_FLASH
        try:
            from google.genai import types
            client = self._get_gemini_client()
        except Exception as exc:
            self._log("ERROR", f"Gemini init failed: {exc}")
            return None

        for attempt in range(retries + 1):
            try:
                resp = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1,
                        max_output_tokens=max_tokens,
                    ),
                )
                raw = resp.text.strip() if resp.text else ""
                if not raw:
                    raise ValueError("Empty Gemini response")
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    repaired = self._repair_json(raw)
                    if repaired:
                        self._log("INFO", f"JSON reparado ({len(raw)} chars)")
                        return repaired
                    raise
            except Exception as exc:
                exc_str = str(exc)
                if "429" in exc_str or "ResourceExhausted" in exc_str:
                    wait = 45 * (attempt + 1)
                    self._log("WARN", f"Gemini rate limit — espera {wait}s")
                    time.sleep(wait)
                elif attempt < retries:
                    self._log("WARN", f"Gemini error (intento {attempt+1}): {exc}")
                    time.sleep(5)
                else:
                    self._log("ERROR", f"Gemini falló tras {retries+1} intentos: {exc}")
                    return None
        return None

    def _gemini_text(self, prompt: str, max_tokens: int = 8000, retries: int = 2, tier: str = "standard") -> str:
        """Call Gemini for FREE TEXT. tier='lite' uses Flash-lite (T3), 'standard' uses Flash (T2)."""
        model = self.GEMINI_LITE if tier == "lite" else self.GEMINI_FLASH
        try:
            from google.genai import types
            client = self._get_gemini_client()
        except Exception as exc:
            self._log("ERROR", f"Gemini init failed: {exc}")
            return ""

        for attempt in range(retries + 1):
            try:
                resp = client.models.generate_content(
                    model=model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        temperature=0.2,
                        max_output_tokens=max_tokens,
                    ),
                )
                text = resp.text.strip() if resp.text else ""
                if not text:
                    raise ValueError("Empty Gemini response")
                return text
            except Exception as exc:
                exc_str = str(exc)
                if "429" in exc_str or "ResourceExhausted" in exc_str:
                    wait = 45 * (attempt + 1)
                    self._log("WARN", f"Gemini rate limit — espera {wait}s")
                    time.sleep(wait)
                elif attempt < retries:
                    self._log("WARN", f"Gemini text error (intento {attempt+1}): {exc}")
                    time.sleep(5)
                else:
                    self._log("ERROR", f"Gemini text falló tras {retries+1} intentos: {exc}")
                    return ""
        return ""

    def _repair_json(self, raw: str) -> dict | None:
        """Attempt to repair truncated JSON from Gemini."""
        # Common case: JSON string truncated mid-value
        # Strategy: close all open strings, arrays, objects
        try:
            # First try: maybe it's just missing closing braces
            test = raw
            # Close any open string
            quote_count = test.count('"') - test.count('\\"')
            if quote_count % 2 != 0:
                test += '"'
            # Close brackets/braces
            open_braces = test.count('{') - test.count('}')
            open_brackets = test.count('[') - test.count(']')
            test += ']' * max(0, open_brackets)
            test += '}' * max(0, open_braces)
            result = json.loads(test)
            if isinstance(result, dict) and len(result) > 0:
                result["_truncated"] = True
                return result
        except json.JSONDecodeError:
            pass

        # Second try: find the last valid JSON substring
        # Remove trailing incomplete key-value pairs
        try:
            # Find last complete value (ends with ", or ], or }, or number, or true/false/null)
            for i in range(len(raw) - 1, max(0, len(raw) - 500), -1):
                if raw[i] in ']}':
                    test = raw[:i+1]
                    open_braces = test.count('{') - test.count('}')
                    open_brackets = test.count('[') - test.count(']')
                    test += ']' * max(0, open_brackets)
                    test += '}' * max(0, open_braces)
                    try:
                        result = json.loads(test)
                        if isinstance(result, dict) and len(result) > 0:
                            result["_truncated"] = True
                            return result
                    except json.JSONDecodeError:
                        continue
        except Exception:
            pass

        return None

    def _truncate(self, text: str, max_chars: int) -> str:
        if not text or len(text) <= max_chars:
            return text
        truncated = text[:max_chars]
        last_period = truncated.rfind(".")
        if last_period > max_chars * 0.7:
            return truncated[:last_period + 1]
        return truncated + "..."

    def _system_role(self, data: dict) -> str:
        nombre = data.get("nombre", "")
        isin = data.get("isin", self.isin)
        gestora = data.get("gestora", "")
        return (
            "Eres un analista senior de fondos de inversión escribiendo un informe ejecutivo para comité de inversión.\n"
            "REGLAS CRÍTICAS DE REDACCIÓN:\n"
            "1. Escribe NARRATIVA FLUIDA con hilo conductor — NO listas de bullets ni esquemas.\n"
            "2. Cada párrafo debe fluir al siguiente con transiciones naturales.\n"
            "3. Usa subsecciones con **Título en negrita** para separar temas.\n"
            "4. Incluye CIFRAS CONCRETAS (AUM, partícipes, rentabilidad, %) integradas en el texto.\n"
            "5. Usa **negritas** en datos clave, nombres de posiciones y conclusiones importantes.\n"
            "6. Tono: analista profesional neutro. Sin adjetivos laudatorios.\n"
            "7. NO hagas copy-paste de datos en bruto. PROCESA, SINTETIZA y CONCLUYE.\n"
            "8. El texto debe tener PENSAMIENTO detrás — no ser una descripción mecánica.\n"
            "9. Si no hay datos suficientes, indica qué falta. NUNCA inventes.\n"
            "10. Mínimo 3-4 párrafos extensos por sección.\n"
            f"Fondo: {nombre} ({isin}) — Gestora: {gestora}"
        )

    def _run_capa3(self, data: dict) -> dict:
        synthesis = {
            "version": "3.1",
            "generated_at": datetime.now().isoformat(),
            "model": "multi-tier: sonnet-t1 + flash-t2 + flash-lite-t3",
            "sections_completed": 0,
        }

        sections = [
            ("resumen", self._section_resumen),
            ("historia", self._section_historia),
            ("gestores", self._section_gestores),
            ("evolucion", self._section_evolucion),
            ("estrategia", self._section_estrategia),
            ("cartera", self._section_cartera),
            ("fuentes_externas", self._section_fuentes_externas),
            ("documentos", self._section_documentos),
        ]

        for name, method in sections:
            self._log("START", f"Capa 3 — Sección: {name}")
            try:
                result = method(data)
                if result:
                    synthesis[name] = result
                    synthesis["sections_completed"] += 1
                    self._log("OK", f"Sección {name} completada")
                else:
                    synthesis[name] = {"error": "Gemini no generó resultado"}
                    self._log("WARN", f"Sección {name} sin resultado")
            except Exception as exc:
                synthesis[name] = {"error": str(exc)}
                self._log("ERROR", f"Sección {name} falló: {exc}")

            if name != "documentos":
                time.sleep(3)

        return synthesis

    # ── Sección 1: Resumen (2 llamadas: texto + datos) ──────────────────

    def _section_resumen(self, data: dict) -> dict | None:
        kpis = data.get("kpis", {})
        rentabilidades = self._compute_annual_returns(data)

        input_data = json.dumps({
            "kpis": kpis,
            "rentabilidades_anuales": rentabilidades,
            "estrategia": self._truncate(data.get("estrategia_actual", ""), 1500),
            "seccion_9": self._truncate(data.get("seccion_9_mas_reciente", ""), 2500),
            "seccion_10": self._truncate(data.get("seccion_10_mas_reciente", ""), 1500),
            "hechos": data.get("hechos_relevantes", []),
            "gestores_info": data.get("gestores", {}).get("info_cartas", [])[:2],
        }, ensure_ascii=False)

        # Call 1: TEXTO — TIER 1 (Sonnet) — narrativa corta SIN subsecciones
        texto = self._sonnet_text(
            self._system_role(data),
            f"{self._quality_hint('resumen')}"
            f"Escribe un RESUMEN EJECUTIVO CONCISO del fondo en MÁXIMO 4 PÁRRAFOS.\n"
            f"NO uses subsecciones, NO uses **títulos en negrita**, NO uses headers.\n"
            f"Es narrativa fluida pura — cada párrafo conecta con el siguiente.\n\n"
            f"Párrafo 1: Qué es el fondo, quién lo gestiona/asesora, fecha de creación, "
            f"clasificación Morningstar, AUM actual, nº de partícipes.\n"
            f"Párrafo 2: Rentabilidad acumulada desde inicio (VL base 100), CAGR, "
            f"volatilidad registrada, posición en categoría, rating Morningstar si lo tiene.\n"
            f"Párrafo 3: Composición actual de la cartera (% RV, % RF, % liquidez), "
            f"nº de posiciones, temáticas principales, exposición geográfica.\n"
            f"Párrafo 4: Crecimiento del patrimonio (de X a Y M€), evolución de partícipes, "
            f"confianza de los inversores.\n\n"
            f"IMPORTANTE: Usar cifras CONCRETAS con negritas (**151,6 M€**, **5.176 partícipes**).\n"
            f"MÁXIMO 1.500 caracteres. Denso, cada frase aporta info nueva.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: FILOSOFÍA DE INVERSIÓN (2-3 párrafos para columna izquierda)
        filosofia = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"Escribe la FILOSOFÍA DE INVERSIÓN del fondo en 2-3 párrafos.\n"
            f"NO uses **títulos** ni headers — es texto fluido puro.\n"
            f"Describe: enfoque de inversión (bottom-up/top-down/macro), universo de inversión, "
            f"criterios de selección, horizonte temporal, rotación de cartera, "
            f"cómo se ha adaptado el enfoque con el tiempo.\n"
            f"Usa cifras y datos concretos (% exposición, nº posiciones, rotación histórica).\n"
            f"Si hay citas del gestor, inclúyelas entrecomilladas.\n"
            f"MÁXIMO 800 caracteres.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 3: CRITERIOS — TIER 3 (Flash-lite) — JSON extraction
        criterios_data = self._gemini_call(
            f"Extrae los 3 CRITERIOS DE INVERSIÓN principales del fondo.\n"
            f"Cada criterio tiene un título corto (2-4 palabras) y una descripción (2-3 frases).\n"
            f"Los criterios deben ser ESPECÍFICOS del fondo, no genéricos.\n"
            f"Ejemplos de buenos criterios: 'Alineación de intereses', 'Modelo de negocio excelente', "
            f"'Precio razonable', 'Diversificación estructural', 'Ventaja competitiva duradera'.\n"
            f"Responde SOLO JSON:\n"
            f"{{\"criterios\":[{{\"titulo\":\"string corto\",\"descripcion\":\"string 2-3 frases\"}},"
            f"{{\"titulo\":\"\",\"descripcion\":\"\"}},{{\"titulo\":\"\",\"descripcion\":\"\"}}]}}\n\n"
            f"DATOS:\n{input_data}",
            tier="lite"
        )
        time.sleep(2)

        # Call 4: DATOS — TIER 3 (Flash-lite) — JSON extraction
        datos = self._gemini_call(
            f"Extrae datos estructurados de este fondo.\n"
            f"OBLIGATORIO: mínimo 4 fortalezas y 3 riesgos concretos y específicos del fondo.\n"
            f"'para_quien_es': 3-4 frases describiendo el perfil EXACTO del inversor adecuado "
            f"(tolerancia al riesgo, horizonte, necesidades, qué NO debería esperar).\n"
            f"'compromiso_gestor': info CONCRETA sobre coinversión del gestor (% capital propio, "
            f"si come su propia cocina, alineamiento de intereses). Si no hay info, indicarlo.\n"
            f"Responde SOLO JSON:\n"
            f"{{\"fortalezas\":[\"string\",\"string\",\"string\",\"string\"],"
            f"\"riesgos\":[\"string\",\"string\",\"string\"],"
            f"\"para_quien_es\":\"string 3-4 frases\","
            f"\"compromiso_gestor\":\"string concreto\","
            f"\"signal\":\"POSITIVO|NEUTRAL|NEGATIVO\",\"signal_rationale\":\"string\"}}\n\n"
            f"TEXTO DEL ANÁLISIS:\n{texto[:3000]}\n\n"
            f"DATOS BRUTOS:\n{input_data}",
            tier="lite"
        )
        if not datos:
            datos = {}

        # Reintentro si faltan fortalezas/riesgos
        n_fort = len(datos.get("fortalezas", []) or [])
        n_riesg = len(datos.get("riesgos", []) or [])
        if n_riesg < 3 or n_fort < 4:
            self._log("RETRY", f"Resumen: {n_fort} fort / {n_riesg} riesg — re-extrayendo")
            time.sleep(2)
            extra = self._gemini_call(
                f"Extrae EXACTAMENTE 4 fortalezas y 3 riesgos concretos del fondo.\n"
                f"Cada item: frase específica con cifras/nombres (no genérica).\n"
                f"JSON: {{\"fortalezas\":[\"\",\"\",\"\",\"\"],\"riesgos\":[\"\",\"\",\"\"]}}\n\n"
                f"TEXTO:\n{texto[:5000]}",
                tier="lite"
            )
            if extra:
                if len(extra.get("fortalezas", []) or []) >= 4:
                    datos["fortalezas"] = extra["fortalezas"]
                if len(extra.get("riesgos", []) or []) >= 3:
                    datos["riesgos"] = extra["riesgos"]

        datos["texto"] = texto
        datos["filosofia_inversion"] = filosofia or ""
        datos["criterios_inversion"] = (criterios_data or {}).get("criterios", [])
        return datos if texto else None

    # ── Sección 2: Historia (2 llamadas) ──────────────────────────────────

    def _section_historia(self, data: dict) -> dict | None:
        input_data = json.dumps({
            "nombre": data.get("nombre", ""),
            "gestora": data.get("gestora", ""),
            "anio_creacion": data.get("kpis", {}).get("anio_creacion", ""),
            "fecha_registro": data.get("kpis", {}).get("fecha_registro", ""),
            "hechos": data.get("hechos_relevantes", []),
            "serie_aum": data.get("cuantitativo", {}).get("serie_aum", []),
            "serie_participes": data.get("cuantitativo", {}).get("serie_participes", []),
            "timeline_resumen": [{"a": t.get("anio", ""), "v": self._truncate(t.get("cnmv_vision", ""), 150), "n": t.get("num_cartas", 0)} for t in data.get("timeline", [])],
        }, ensure_ascii=False)

        texto = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"{self._quality_hint('historia')}"
            f"Escribe la HISTORIA del fondo en 4-6 PÁRRAFOS FLUIDOS.\n"
            f"NO uses subsecciones ni **títulos en negrita como headers**.\n"
            f"SÍ usa **negritas** para resaltar datos clave dentro del texto "
            f"(cifras, nombres, fechas, hitos importantes).\n"
            f"Es narrativa fluida — un resumen extenso de la historia completa.\n"
            f"Agrupa los años en FASES temáticas con transiciones naturales.\n\n"
            f"CONTENIDO OBLIGATORIO:\n"
            f"- Fundación: quién, cuándo, con qué patrimonio y partícipes iniciales\n"
            f"- Primeros años: crecimiento orgánico, primeras decisiones, filosofía inicial\n"
            f"- Puntos de inflexión: crisis de mercado, cambios de gestora, fusiones, "
            f"entradas/salidas masivas de partícipes, cambios regulatorios\n"
            f"- Evolución de la estrategia: cómo ha cambiado el enfoque con el tiempo\n"
            f"- Estado actual: AUM, partícipes, posición en categoría, rating\n\n"
            f"Cifras concretas con negritas: **X M€**, **X partícipes**, **+X%**, **VL X**.\n"
            f"NO entres en detalles concretos por año (eso va en la cronología).\n"
            f"MÍNIMO 2.500 caracteres — extenso pero con hilo conductor.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # DATOS: cronología — TIER 3 (Flash-lite) — JSON extraction
        datos = self._gemini_call(
            f"Genera una CRONOLOGÍA DETALLADA del fondo con un hito por cada año relevante.\n"
            f"Cubre TODOS los años desde la creación del fondo (o los últimos 10 años como mínimo).\n"
            f"Cada hito tiene:\n"
            f"- 'anio': año (YYYY o MES YYYY para hitos puntuales)\n"
            f"- 'titulo': frase corta que resume el año (ej: 'Patrimonio se cuadruplica a 34,5 M€')\n"
            f"- 'evento': 2-4 frases con: contexto de mercado ese año, decisiones/tesis del gestor, "
            f"resultado concreto (AUM, partícipes, VL, rentabilidad), hecho diferencial.\n"
            f"- 'tipo': 'hito'|'crisis'|'estrategia'|'regulatorio'|'crecimiento'\n\n"
            f"OBLIGATORIO: mínimo 7 hitos. Para años donde pasó poco, resaltar posicionamiento del fondo vs mercado.\n"
            f"Responde SOLO JSON:\n"
            f"{{\"hitos\":[{{\"anio\":\"YYYY\",\"titulo\":\"frase corta\",\"evento\":\"2-4 frases con contexto\",\"tipo\":\"string\"}}]}}\n\n"
            f"DATOS:\n{input_data}",
            tier="lite"
        )

        result = datos if datos else {}

        # Reintentro si pocos hitos
        n_hitos = len(result.get("hitos", []) or [])
        if n_hitos < 7:
            self._log("RETRY", f"Historia: solo {n_hitos} hitos — re-extrayendo")
            time.sleep(2)
            extra = self._gemini_call(
                f"Genera MÍNIMO 7 hitos cronológicos del fondo, uno por cada año relevante.\n"
                f"Cada uno con: anio, titulo (frase corta), evento (2-4 frases con contexto mercado + decisiones + resultado), tipo.\n"
                f"JSON: {{\"hitos\":[{{\"anio\":\"\",\"titulo\":\"\",\"evento\":\"\",\"tipo\":\"\"}}]}}\n\n"
                f"DATOS:\n{input_data}"
            )
            if extra and len(extra.get("hitos", []) or []) >= 7:
                result["hitos"] = extra["hitos"]
                self._log("OK", f"Historia retry: {len(result['hitos'])} hitos")

        result["texto"] = texto
        return result if texto else None

    # ── Sección 3: Gestores (3 llamadas) ──────────────────────────────────

    def _section_gestores(self, data: dict) -> dict | None:
        gestores = data.get("gestores", {})
        fuentes_compact = []
        for f in gestores.get("fuentes_web", []):
            fuentes_compact.append({
                "url": f.get("url", ""),
                "titulo": f.get("titulo", ""),
                "texto": self._truncate(f.get("texto", ""), 600),
            })

        input_data = json.dumps({
            "equipo": gestores.get("equipo", []),
            "equipo_detalle": gestores.get("equipo_detalle_web", []),
            "fuentes_web": fuentes_compact[:10],
            "info_cartas": gestores.get("info_cartas", []),
        }, ensure_ascii=False)

        # Call 1: TEXTO — overview del equipo (para los párrafos de arriba)
        texto_equipo = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"{self._quality_hint('gestores')}"
            f"Escribe 2-3 PÁRRAFOS sobre el equipo gestor SIN headers ni subsecciones.\n"
            f"Narrativa fluida pura que cubra:\n"
            f"- Composición del equipo: cuántas personas, quién lidera, roles clave\n"
            f"- Estabilidad: si ha habido cambios en el equipo (SIEMPRE destacar, positivo o negativo)\n"
            f"- Filosofía compartida del equipo y cómo se complementan los perfiles\n"
            f"- Fortalezas del equipo y riesgos (ej: dependencia de persona clave)\n"
            f"NO menciones nombres con **negrita** ni con formato **Nombre — Cargo**.\n"
            f"Máximo 1.000 caracteres.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: TEXTO — TIER 1 (Sonnet) — perfil narrativo extenso de cada gestor
        texto_principal = self._sonnet_text(
            self._system_role(data),
            f"Para CADA gestor principal del fondo, escribe un perfil narrativo EXTENSO.\n"
            f"Usa **Nombre del gestor — Cargo** como separador entre gestores.\n"
            f"Para el gestor PRINCIPAL (lead), escribe 3-4 párrafos cubriendo:\n"
            f"- Trayectoria profesional y formación académica\n"
            f"- Filosofía de inversión con CITAS TEXTUALES del gestor si las hay (entrecomilladas)\n"
            f"- Decisiones clave documentadas: contexto de mercado + qué hizo + resultado concreto\n"
            f"- Rasgos diferenciales: transparencia, coinversión, comunicación\n"
            f"Para gestores SECUNDARIOS: 1-2 párrafos con su rol y contribución al equipo.\n"
            f"NARRATIVA FLUIDA, no bullets.\n"
            f"Mínimo 3.000 caracteres total.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 3: DATOS — TIER 2 (Flash) — perfiles estructurados con trayectoria extensa
        datos = self._gemini_call(
            f"Extrae perfiles del equipo gestor para FICHAS de un dashboard profesional.\n"
            f"Para CADA gestor principal incluye:\n"
            f"- nombre: nombre completo\n"
            f"- cargo: cargo actual\n"
            f"- cv_bullets: 4-6 strings CORTOS tipo CV bullet, ej:\n"
            f"  ['Fundador de X Capital', 'Rating AA Citywire', '+15 años en gestión', "
            f"  'Dirige: Fondo A, Fondo B', 'CFA / MBA IESE', 'Experto en value investing']\n"
            f"- trayectoria: TEXTO EXTENSO de 3-4 párrafos (mín 1000 chars) separados por \\n\\n.\n"
            f"  USA **negritas** para resaltar: nombres propios, fondos que gestiona, cifras, "
            f"conceptos clave de inversión, hitos profesionales.\n"
            f"  Párrafo 1: Quién es, **formación académica**, carrera profesional, **otros fondos** que gestiona.\n"
            f"  Párrafo 2: **Estilo de inversión** y en qué basa su filosofía/estrategia. "
            f"Citas textuales entrecomilladas si hay. **Referentes intelectuales** (Graham, Browne, Buffett...).\n"
            f"  Párrafo 3: **Decisiones documentadas** que ilustren su capacidad. "
            f"Contexto de mercado + qué hizo + **resultado concreto con cifras**.\n"
            f"  Párrafo 4 (opcional): **Rasgos diferenciales** — transparencia, coinversión, comunicación.\n"
            f"- filosofia: 3-5 frases con su filosofía CONCRETA de inversión (para bloque italic)\n"
            f"- decisiones_clave: lista de 2-4 strings (contexto + acción + resultado)\n"
            f"- rasgos_diferenciales: 1-2 frases\n"
            f"IMPORTANTE: la 'trayectoria' es lo que se muestra visible en la ficha — debe ser EXTENSO y rico.\n"
            f"Responde SOLO JSON:\n"
            f"{{\"perfiles\":[{{\"nombre\":\"\",\"cargo\":\"\",\"cv_bullets\":[\"\",\"\",\"\",\"\"],"
            f"\"trayectoria\":\"3-4 parrafos extensos separados por newlines\","
            f"\"filosofia\":\"3-5 frases\","
            f"\"decisiones_clave\":[\"\",\"\"],\"rasgos_diferenciales\":\"\"}}]}}\n"
            f"DATOS:\n{input_data}"
        )

        result = datos if datos else {}

        # Call 4: SIEMPRE extraer filosofía del gestor principal si hay fuentes web
        # (resuelve el caso 'gestor sin filosofía' del quality_agent)
        perfiles = result.get("perfiles", []) or []
        if perfiles and fuentes_compact:
            lead_name = perfiles[0].get("nombre", "")
            if lead_name and not perfiles[0].get("filosofia"):
                self._log("RETRY", f"Gestores: extrayendo filosofía explícita de {lead_name}")
                time.sleep(2)
                fuentes_str = "\n\n".join(
                    f"FUENTE: {f.get('titulo','')}\n{f.get('texto','')[:1000]}"
                    for f in fuentes_compact[:6]
                )
                filo_text = self._gemini_text(
                    f"Analiza estas fuentes web sobre el gestor {lead_name}.\n"
                    f"Extrae su FILOSOFÍA DE INVERSIÓN en 3-5 frases concretas.\n"
                    f"Si hay citas textuales del gestor, inclúyelas entrecomilladas.\n"
                    f"NO uses bullets — escribe párrafo fluido.\n"
                    f"NO digas 'la filosofía de X es...' — entra directo al contenido.\n"
                    f"Si las fuentes no contienen información explícita sobre filosofía, "
                    f"infiere la filosofía a partir de las decisiones, posiciones y comentarios documentados.\n\n"
                    f"FUENTES:\n{fuentes_str}",
                    max_tokens=2000,
                )
                if filo_text and len(filo_text) > 100:
                    perfiles[0]["filosofia"] = filo_text.strip()
                    result["perfiles"] = perfiles
                    self._log("OK", f"Filosofía extraída para {lead_name}: {len(filo_text)} chars")

        result["texto"] = (texto_equipo + "\n\n" + texto_principal) if texto_principal else texto_equipo
        return result if result.get("texto") else None

    # ── Sección 4: Evolución ──────────────────────────────────────────────

    def _compute_annual_returns(self, data: dict) -> list[dict]:
        vl = data.get("cuantitativo", {}).get("serie_vl_base100", [])
        returns = []
        for i in range(1, len(vl)):
            prev = vl[i-1].get("base100", 0)
            curr = vl[i].get("base100", 0)
            if prev > 0:
                returns.append({
                    "periodo": vl[i].get("periodo", ""),
                    "rentabilidad_pct": round((curr / prev - 1) * 100, 1)
                })
        return returns

    def _compute_geographic_mix(self, data: dict) -> list[dict]:
        """Compute geographic mix from posiciones with actual data."""
        result = []
        # Current positions
        actuales = data.get("posiciones", {}).get("actuales", [])
        if actuales:
            by_country: dict[str, float] = {}
            for p in actuales:
                pais = p.get("pais", "Otros") or "Otros"
                by_country[pais] = by_country.get(pais, 0) + (p.get("peso_pct", 0) or 0)
            if by_country:
                cuant = data.get("cuantitativo", {})
                periodo = "2025"
                for s in reversed(cuant.get("serie_aum", [])):
                    periodo = s.get("periodo", "2025")
                    break
                result.append({"periodo": periodo, "zonas": by_country, "fuente": "posiciones_actuales"})

        # Historical positions
        for year_data in data.get("posiciones", {}).get("historicas", []):
            todas = year_data.get("todas", [])
            if not todas:
                continue
            by_country = {}
            for p in todas:
                pais = p.get("pais", "Otros") or "Otros"
                by_country[pais] = by_country.get(pais, 0) + (p.get("peso_pct", 0) or 0)
            if by_country:
                result.append({
                    "periodo": year_data.get("periodo", ""),
                    "zonas": by_country,
                    "fuente": "posiciones_historicas"
                })
        return result

    def _compute_concentration(self, data: dict) -> list[dict]:
        """Compute top5/10/15 concentration from positions."""
        result = []
        # Current
        actuales = data.get("posiciones", {}).get("actuales", [])
        if actuales:
            sorted_pos = sorted(actuales, key=lambda x: x.get("peso_pct", 0) or 0, reverse=True)
            weights = [p.get("peso_pct", 0) or 0 for p in sorted_pos]
            result.append({
                "periodo": "actual",
                "top5_pct": round(sum(weights[:5]), 1),
                "top10_pct": round(sum(weights[:10]), 1),
                "top15_pct": round(sum(weights[:15]), 1),
                "fuente": "posiciones_actuales",
            })
        # Historical
        for year_data in data.get("posiciones", {}).get("historicas", []):
            todas = year_data.get("todas", [])
            if not todas:
                continue
            sorted_pos = sorted(todas, key=lambda x: x.get("peso_pct", 0) or 0, reverse=True)
            weights = [p.get("peso_pct", 0) or 0 for p in sorted_pos]
            result.append({
                "periodo": year_data.get("periodo", ""),
                "top5_pct": round(sum(weights[:5]), 1),
                "top10_pct": round(sum(weights[:10]), 1),
                "top15_pct": round(sum(weights[:15]), 1),
                "fuente": "posiciones_historicas",
            })
        return result

    def _compute_positions_count(self, data: dict) -> list[dict]:
        result = []
        actuales = data.get("posiciones", {}).get("actuales", [])
        if actuales:
            result.append({"periodo": "actual", "num_posiciones": len(actuales), "fuente": "posiciones_actuales"})
        for year_data in data.get("posiciones", {}).get("historicas", []):
            todas = year_data.get("todas", [])
            if todas:
                result.append({
                    "periodo": year_data.get("periodo", ""),
                    "num_posiciones": len(todas),
                    "fuente": "posiciones_historicas"
                })
        return result

    def _section_evolucion(self, data: dict) -> dict | None:
        cuant = data.get("cuantitativo", {})
        input_data = json.dumps({
            "aum": [{"p": s.get("periodo"), "v": s.get("valor_meur")} for s in cuant.get("serie_aum", [])],
            "participes": [{"p": s.get("periodo"), "v": s.get("valor")} for s in cuant.get("serie_participes", [])],
            "ter": [{"p": s.get("periodo"), "v": s.get("ter_pct")} for s in cuant.get("serie_ter", [])],
            "rotacion": [{"p": s.get("periodo"), "v": s.get("rotacion_pct")} for s in cuant.get("serie_rotacion", [])],
            "vl100": [{"p": s.get("periodo"), "v": s.get("base100")} for s in cuant.get("serie_vl_base100", [])],
            "mix": [{"p": s.get("periodo"), "rv": s.get("rv_pct"), "rf": s.get("renta_fija_pct"), "liq": s.get("liquidez_pct")} for s in cuant.get("mix_activos_historico", [])],
            "rentabilidades": self._compute_annual_returns(data),
        }, ensure_ascii=False)

        # Call 1: TEXTO narrativo
        texto = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"Escribe un ANÁLISIS CUANTITATIVO de la evolución del fondo.\n"
            f"Estructura con subsecciones usando **Título** en negrita:\n"
            f"- **Patrimonio (AUM)** — evolución con cifras por año, fases de crecimiento\n"
            f"- **Partícipes** — evolución, episodios de entrada/salida masiva\n"
            f"- **Rentabilidad** — rentabilidades anuales concretas, comparación con índices si hay datos\n"
            f"- **Comisiones (TER)** — evolución, comparación entre clases\n"
            f"- **Mix de activos** — cómo cambió de puro RV a mixto, por qué\n"
            f"- **Rotación** — evolución y qué indica\n"
            f"Cada subsección: 1-2 párrafos con cifras concretas.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: DATOS — pre-computados en Python, solo los pasamos
        datos_graficos = {
            "exposicion_geografica": self._compute_geographic_mix(data),
            "num_posiciones_por_anio": self._compute_positions_count(data),
            "concentracion_historica": self._compute_concentration(data),
            "rentabilidades_anuales": self._compute_annual_returns(data),
        }

        return {"texto": texto, "datos_graficos": datos_graficos} if texto else None

    # ── Sección 5: Estrategia (2 llamadas) ────────────────────────────────

    def _section_estrategia(self, data: dict) -> dict | None:
        timeline_compact = []
        for t in data.get("timeline", []):
            timeline_compact.append({
                "a": t.get("anio", ""),
                "v": self._truncate(t.get("cnmv_vision", ""), 300),
                "c": self._truncate(t.get("carta_texto", ""), 200),
                "h": [h.get("detalle", "")[:80] for h in t.get("hechos", [])],
            })

        cuant = data.get("cuantitativo", {})
        input_data = json.dumps({
            "timeline": timeline_compact,
            "mix": [{"p": s.get("periodo"), "rv": s.get("rv_pct"), "rf": s.get("renta_fija_pct")} for s in cuant.get("mix_activos_historico", [])],
            "rotacion": [{"p": s.get("periodo"), "v": s.get("rotacion_pct")} for s in cuant.get("serie_rotacion", [])],
        }, ensure_ascii=False)

        # Call 1: TEXTO — TIER 1 (Sonnet) — análisis evaluativo extenso
        texto = self._sonnet_text(
            self._system_role(data),
            f"{self._quality_hint('estrategia')}"
            f"Escribe un ANÁLISIS EVALUATIVO EXTENSO de la estrategia del fondo en 4-6 PÁRRAFOS.\n"
            f"NO uses subsecciones ni líneas que sean solo **título** como headers.\n"
            f"SÍ usa **negritas** dentro del texto para resaltar conceptos clave, "
            f"nombres de posiciones, cifras, decisiones importantes.\n\n"
            f"CONTENIDO OBLIGATORIO:\n"
            f"- Párrafo 1: **Filosofía de inversión** del fondo — criterios, enfoque, "
            f"qué le diferencia de la competencia. Evolución del enfoque con el tiempo.\n"
            f"- Párrafo 2: **Coherencia discurso vs acción** — ¿han sido fieles a lo que dicen? "
            f"Ejemplos concretos con posiciones y % de exposición.\n"
            f"- Párrafo 3: **Momentos clave de decisión** — las 2-3 decisiones más importantes "
            f"del equipo gestor con contexto de mercado, qué hicieron y resultado.\n"
            f"- Párrafo 4: **Evolución del mix de activos** — cómo ha cambiado la composición "
            f"de la cartera (% RV, RF, liquidez) y por qué.\n"
            f"- Párrafo 5-6: **Conclusión evaluativa** — patrones de acierto y error, "
            f"¿es un equipo que aprende? ¿cuáles son sus sesgos?\n\n"
            f"MÍNIMO 2.500 caracteres — extenso, con cifras concretas y análisis propio.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: QUOTES — TIER 3 (Flash-lite) — JSON extraction
        quotes_data = self._gemini_call(
            f"Extrae 2-4 CITAS TEXTUALES o frases representativas del gestor del fondo.\n"
            f"Cada quote debe ayudar a entender la filosofía de inversión.\n"
            f"Si no hay citas textuales, genera frases que resuman fielmente su pensamiento "
            f"basándote en sus decisiones documentadas y comunicaciones.\n"
            f"Responde SOLO JSON:\n"
            f"{{\"quotes\":[{{\"texto\":\"frase entrecomillada\",\"autor\":\"nombre del gestor\",\"contexto\":\"cuándo/dónde lo dijo\"}}]}}\n\n"
            f"DATOS:\n{input_data}",
            tier="lite"
        )
        time.sleep(2)

        # Call 3: MATRIZ — TIER 3 (Flash-lite) — JSON extraction
        datos = self._gemini_call(
            f"Genera una MATRIZ de consistencia estratégica del fondo.\n"
            f"Para cada periodo temporal relevante (mínimo 4 periodos):\n"
            f"- periodo: rango de años (ej: '2017-2019')\n"
            f"- contexto_mercado: qué pasaba en el mercado y qué esperaban los gestores\n"
            f"- decisiones: qué hicieron realmente (posiciones, exposición, cambios)\n"
            f"- resultado: qué resultado obtuvieron (rentabilidad, AUM, vs mercado)\n"
            f"Responde SOLO JSON:\n"
            f"{{\"hitos_estrategia\":[{{\"periodo\":\"\",\"contexto_mercado\":\"\",\"decisiones\":\"\",\"resultado\":\"\"}}],"
            f"\"estrategia_actual_resumen\":\"2-3 frases\"}}\n\n"
            f"DATOS:\n{input_data}",
            tier="lite"
        )

        result = datos if datos else {}
        result["texto"] = texto
        result["quotes"] = (quotes_data or {}).get("quotes", [])
        return result if texto else None

    # ── Sección 6: Cartera (2 llamadas) ───────────────────────────────────

    def _section_cartera(self, data: dict) -> dict | None:
        actuales = data.get("posiciones", {}).get("actuales", [])
        sorted_pos = sorted(actuales, key=lambda x: x.get("peso_pct", 0) or 0, reverse=True)
        top_positions = [{
            "nombre": p.get("nombre", ""),
            "peso_pct": p.get("peso_pct", 0),
            "tipo": p.get("tipo", ""),
            "pais": p.get("pais", ""),
            "divisa": p.get("divisa", ""),
        } for p in sorted_pos[:20]]

        concentration = self._compute_concentration(data)

        # Get last period date for prompt
        last_period = ""
        for s in reversed(data.get("cuantitativo", {}).get("serie_aum", [])):
            last_period = s.get("periodo", "")
            break

        input_data = json.dumps({
            "posiciones_top20": top_positions,
            "total_posiciones": len(actuales),
            "concentracion": concentration,
            "fecha_datos": last_period,
        }, ensure_ascii=False)

        # Call 1: TEXTO análisis — conciso, sin subsecciones
        texto = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"{self._quality_hint('cartera')}"
            f"Escribe un ANÁLISIS CONCISO de la cartera actual en 2-3 PÁRRAFOS.\n"
            f"NO uses subsecciones ni **headers** — narrativa fluida pura.\n"
            f"Datos a fecha {last_period}. NO inventes otra fecha.\n\n"
            f"Párrafo 1: Composición general — nº posiciones, distribución RV/RF/liquidez/otros, "
            f"exposición geográfica principal, cambios más significativos vs periodo anterior.\n"
            f"Párrafo 2: Posiciones más relevantes y por qué están en cartera — "
            f"tesis del gestor, apuestas temáticas (ej: Argentina, tecnología, renta fija emergente).\n"
            f"Párrafo 3 (opcional): Concentración y riesgos — top 5/10 vs media histórica, "
            f"exposición temática, riesgo de divisa.\n\n"
            f"Incluye nombres de posiciones en NEGRITA y cifras concretas (**X%**, **Y M€**).\n"
            f"Máximo 1.200 caracteres — denso, cada frase con dato concreto.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: DATOS — TIER 3 (Flash-lite) — JSON extraction
        datos = self._gemini_call(
            f"Extrae distribución de cartera. JSON:\n"
            f"{{\"concentracion\":{{\"top5_pct\":0,\"top10_pct\":0,\"top15_pct\":0}},"
            f"\"distribucion_tipo\":{{\"rv_españa_pct\":0,\"rv_internacional_pct\":0,\"rf_pct\":0,\"liquidez_pct\":0}}}}\n"
            f"DATOS:\n{input_data}",
            tier="lite"
        )

        result = datos if datos else {}
        result["texto"] = texto
        result["concentracion_historica"] = concentration
        return result if texto else None

    # ── Sección 7: Fuentes Externas (2 llamadas) ─────────────────────────

    def _section_fuentes_externas(self, data: dict) -> dict | None:
        lecturas = data.get("lecturas_externas", {})
        items_compact = []
        gestora_name = (data.get("gestora", "") or "").lower()
        gestora_domain = gestora_name.split()[0] if gestora_name else ""

        # Collect from readings
        for item in lecturas.get("analisis_escritos", []) + lecturas.get("multimedia", []):
            url = item.get("url", "")
            titulo = (item.get("titulo", "") or "").lower()
            if "carta semestral" in titulo and gestora_domain and gestora_domain in url:
                continue
            items_compact.append({
                "fuente": item.get("fuente", ""),
                "tipo": item.get("tipo", ""),
                "titulo": item.get("titulo", ""),
                "url": url,
                "fecha": item.get("fecha", ""),
                "texto": self._truncate(item.get("texto_completo", ""), 600),
            })

        # Also include manager web sources (entrevistas, artículos sobre gestores)
        gestores = data.get("gestores", {})
        seen_urls = {i["url"] for i in items_compact}
        for f in gestores.get("fuentes_web", []):
            url = f.get("url", "")
            if url and url not in seen_urls:
                items_compact.append({
                    "fuente": f.get("dominio", "web"),
                    "tipo": "articulo",
                    "titulo": f.get("titulo", ""),
                    "url": url,
                    "fecha": "",
                    "texto": self._truncate(f.get("texto", ""), 400),
                })
                seen_urls.add(url)

        input_data = json.dumps({"fuentes": items_compact[:25]}, ensure_ascii=False)

        # Call 1: TEXTO síntesis
        texto = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"Sintetiza las OPINIONES DE TERCEROS sobre el fondo.\n"
            f"Estructura con subsecciones usando **Nombre de la fuente** en negrita:\n"
            f"Para cada fuente relevante: qué dice, qué destaca, qué critica, citas textuales si las hay.\n"
            f"Excluye páginas genéricas (listados de podcasts, fichas sin contenido específico).\n"
            f"Cada fuente: 1-2 párrafos con lo más relevante, no resúmenes genéricos.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: DATOS opiniones — TIER 3 (Flash-lite) — JSON extraction
        datos = self._gemini_call(
            f"Extrae MÍNIMO 10 opiniones/fuentes relevantes sobre el fondo o sus gestores.\n"
            f"PRIORIDAD (de mayor a menor):\n"
            f"1. Análisis profesionales de fuentes fiables (Rankia, Finect, Substack, blogs financieros)\n"
            f"2. Entrevistas/artículos en prensa (El Confidencial, Expansión, Citywire)\n"
            f"3. Vídeos/podcasts sobre el fondo o el gestor\n"
            f"4. Noticias relevantes sobre el fondo\n"
            f"Excluir: páginas genéricas, fichas sin contenido, listados de fondos.\n"
            f"Cada titulo debe ser DESCRIPTIVO — indicar de qué trata específicamente.\n"
            f"Responde SOLO JSON:\n"
            f"{{\"opiniones_clave\":[{{\"fuente\":\"nombre de la fuente\","
            f"\"titulo\":\"titulo descriptivo del contenido\","
            f"\"url\":\"url completa\","
            f"\"tipo\":\"analisis|entrevista|video|podcast|noticia\","
            f"\"opinion\":\"resumen de 2-3 frases de lo que dice\","
            f"\"fecha\":\"YYYY o YYYY-MM si se conoce\"}}]}}\n"
            f"OBLIGATORIO: mínimo 10 items. Si hay menos de 10 fuentes disponibles, "
            f"incluir todas las que hay.\n"
            f"DATOS:\n{input_data}",
            tier="lite"
        )

        result = datos if datos else {}
        result["texto"] = texto
        return result if texto else None

    # ── Sección 8: Documentos (puro Python) ───────────────────────────────

    def _section_documentos(self, data: dict) -> dict:
        fuentes = data.get("fuentes", {})
        lecturas = data.get("lecturas_externas", {})
        gestores = data.get("gestores", {})

        # XMLs
        xmls = []
        for x in fuentes.get("xmls_cnmv", []):
            if isinstance(x, str):
                name = x.split("/")[-1] if "/" in x else x.split("\\")[-1]
                xmls.append({"archivo": name})

        # PDFs
        pdfs = []
        for p in fuentes.get("informes_descargados", []):
            if isinstance(p, str):
                name = p.split("/")[-1] if "/" in p else p.split("\\")[-1]
                pdfs.append({"archivo": name})

        # External URLs
        ext_urls = set()
        for item in lecturas.get("analisis_escritos", []) + lecturas.get("multimedia", []):
            url = item.get("url", "")
            if url:
                ext_urls.add(url)
        for f in gestores.get("fuentes_web", []):
            url = f.get("url", "")
            if url:
                ext_urls.add(url)

        # Cartas URLs
        cartas_urls = set()
        for t in data.get("timeline", []):
            for src in t.get("carta_fuentes", []):
                url = src.get("url", "")
                if url:
                    cartas_urls.add(url)

        return {
            "xmls_cnmv": xmls,
            "informes_pdf": pdfs,
            "urls_consultadas": fuentes.get("urls_consultadas", []),
            "cartas_urls": sorted(cartas_urls),
            "fuentes_externas_urls": sorted(ext_urls),
            "total_fuentes": len(xmls) + len(pdfs) + len(ext_urls) + len(cartas_urls),
        }

    # ═══════════════════════════════════════════════════════════════════════════
    # CAPA 3b: GEMINI QUALITY CHECKER
    # ═══════════════════════════════════════════════════════════════════════════

    def _run_checker(self, synthesis: dict, raw_data: dict) -> dict | None:
        """Gemini reviews the synthesis against raw data."""
        cuant = raw_data.get("cuantitativo", {})
        posiciones = raw_data.get("posiciones", {})

        # Check which historical periods have REAL position data
        periodos_con_posiciones = []
        for p in posiciones.get("historicas", []):
            if p.get("todas"):
                periodos_con_posiciones.append(p.get("periodo", ""))

        verification_data = {
            "cuantitativo_series": {
                "serie_aum": cuant.get("serie_aum", []),
                "serie_participes": cuant.get("serie_participes", []),
                "serie_vl_base100": cuant.get("serie_vl_base100", []),
                "serie_ter": cuant.get("serie_ter", []),
                "serie_rotacion": cuant.get("serie_rotacion", []),
                "mix_activos_historico": cuant.get("mix_activos_historico", []),
            },
            "kpis": raw_data.get("kpis", {}),
            "num_posiciones_actuales": len(posiciones.get("actuales", [])),
            "periodos_con_posiciones_historicas_reales": periodos_con_posiciones,
            "posiciones_top10": sorted(
                posiciones.get("actuales", []),
                key=lambda x: x.get("peso_pct", 0) or 0,
                reverse=True
            )[:10],
        }

        # Truncate synthesis for prompt (keep structure, truncate long texts)
        synth_compact = {}
        for key, val in synthesis.items():
            if isinstance(val, dict) and "texto" in val:
                compact = dict(val)
                compact["texto"] = self._truncate(compact["texto"], 800)
                synth_compact[key] = compact
            else:
                synth_compact[key] = val

        prompt = (
            "Eres un controller de calidad de informes de fondos de inversión.\n"
            "Tu PRIORIDAD MÁXIMA es verificar datos cuantitativos y gráficos evolutivos.\n\n"
            "Revisa el análisis y devuelve JSON con:\n"
            '1. errores_numericos: [{"seccion","dato","valor_analisis","valor_real","correccion"}]\n'
            '2. datos_estimados_como_reales: [{"seccion","dato","motivo"}] — CRÍTICO: si datos_graficos contiene puntos que NO existen en los datos raw, márcalos\n'
            '3. omisiones_cuantitativas: [{"seccion","dato_faltante","valor_disponible"}]\n'
            '4. clasificaciones_incorrectas: [{"posicion","clasificacion_actual","clasificacion_correcta"}]\n'
            '5. inconsistencias_entre_secciones: [{"dato","seccion_1","valor_1","seccion_2","valor_2"}]\n'
            '6. correcciones_redaccion: [{"seccion","frase_actual","frase_corregida","motivo"}]\n'
            '7. score: {"cuantitativo":0-10,"completitud":0-10,"redaccion":0-10,"global":0-10}\n\n'
            "VERIFICACIONES OBLIGATORIAS:\n"
            "- Cada punto de serie (AUM, partícipes, VL, TER, rotación) debe coincidir con datos raw\n"
            "- Los datos de exposición geográfica SOLO son válidos si tienen fuente 'posiciones_actuales' o 'posiciones_historicas'\n"
            "- Periodos con posiciones históricas reales: " + str(periodos_con_posiciones) + "\n"
            "- Si NO hay posiciones históricas, concentración y geo históricos NO pueden existir\n"
            "- Rentabilidades anuales deben calcularse desde VL base 100\n"
            "- Top5/10/15 debe calcularse desde posiciones reales ordenadas por peso\n"
            "- La misma cifra debe aparecer igual en TODAS las secciones donde se mencione\n\n"
            f"=== ANÁLISIS ===\n{json.dumps(synth_compact, ensure_ascii=False)[:10000]}\n\n"
            f"=== DATOS RAW ===\n{json.dumps(verification_data, ensure_ascii=False)[:8000]}\n\n"
            "Responde SOLO con el JSON:"
        )
        return self._gemini_call(prompt, max_tokens=8000)

    def _apply_corrections(self, synthesis: dict, check_result: dict) -> dict:
        """Apply automatic corrections from checker."""
        # Store quality check metadata
        synthesis["_quality_check"] = {
            "score": check_result.get("score", {}),
            "num_errores": len(check_result.get("errores_numericos", [])),
            "num_datos_estimados": len(check_result.get("datos_estimados_como_reales", [])),
            "num_omisiones": len(check_result.get("omisiones_cuantitativas", [])),
            "num_inconsistencias": len(check_result.get("inconsistencias_entre_secciones", [])),
            "num_clasificaciones": len(check_result.get("clasificaciones_incorrectas", [])),
            "checked_at": datetime.now().isoformat(),
            "full_report": check_result,
        }

        # Mark estimated data in datos_graficos
        for item in check_result.get("datos_estimados_como_reales", []):
            seccion = item.get("seccion", "")
            if seccion in synthesis and isinstance(synthesis[seccion], dict):
                synthesis[seccion].setdefault("_warnings", []).append(
                    f"Dato estimado: {item.get('dato', '')} — {item.get('motivo', '')}"
                )

        return synthesis

    # ═══════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════════════════════════

    def _load_json(self, filename: str) -> dict:
        path = self.fund_dir / filename
        if not path.exists():
            self._log("INFO", f"No existe: {filename}")
            return {}
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {"items": data}
        except Exception as exc:
            self._log("WARN", f"Error leyendo {filename}: {exc}")
            return {}

    def _save(self, output: dict):
        out_path = self.fund_dir / "output.json"
        out_path.write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        self._log("OK", f"Guardado: {out_path}")

    def _print_summary(self, output: dict):
        """Print a Rich summary of what was consolidated."""
        from rich.table import Table

        table = Table(title=f"Analyst Agent — Consolidación {self.isin}")
        table.add_column("Componente", width=25)
        table.add_column("Datos", width=50)

        timeline = output.get("timeline", [])
        table.add_row("Timeline", f"{len(timeline)} años")
        table.add_row("Hechos relevantes", f"{len(output.get('hechos_relevantes', []))}")

        gestores = output.get("gestores", {})
        table.add_row("Equipo gestor", str(gestores.get("equipo", [])))
        table.add_row("Fuentes gestor", f"{len(gestores.get('fuentes_web', []))} páginas")

        lecturas = output.get("lecturas_externas", {})
        n_a = len(lecturas.get("analisis_escritos", []))
        n_m = len(lecturas.get("multimedia", []))
        table.add_row("Lecturas externas", f"{n_a} análisis + {n_m} multimedia")

        kpis = output.get("kpis", {})
        table.add_row("AUM", f"{kpis.get('aum_actual_meur', '?')} M€")
        table.add_row("Partícipes", str(kpis.get("num_participes", "?")))

        cuant = output.get("cuantitativo", {})
        table.add_row("Serie AUM", f"{len(cuant.get('serie_aum', []))} puntos")
        table.add_row("Posiciones actuales", f"{len(output.get('posiciones', {}).get('actuales', []))}")

        console.print(table)


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")

    isin = sys.argv[1] if len(sys.argv) > 1 else "ES0112231008"
    agent = AnalystAgent(isin, {})
    result = agent.run()
