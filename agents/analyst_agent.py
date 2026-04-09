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

    def __init__(self, isin: str, config: dict = None):
        self.isin = isin.strip().upper()
        self.config = config or {}
        root = Path(__file__).parent.parent
        self.fund_dir = root / "data" / "funds" / self.isin
        self.schema_path = root / "schemas" / "fund_output_v2.json"
        self.log_path = root / "progress.log"

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

        # Also pass through raw cuantitativo (untouched by filters)
        consolidated["cuantitativo"] = cnmv.get("cuantitativo", {})
        consolidated["kpis"] = cnmv.get("kpis", {})
        consolidated["posiciones"] = cnmv.get("posiciones", {})
        consolidated["fuentes"] = cnmv.get("fuentes", {})

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
    # CAPA 3: GEMINI SYNTHESIS — 8 SECCIONES
    # ═══════════════════════════════════════════════════════════════════════════

    def _get_gemini_client(self):
        """Get or create Gemini client (new google-genai SDK)."""
        if not hasattr(self, '_gemini_client'):
            from google import genai
            self._gemini_client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY", ""))
        return self._gemini_client

    def _gemini_call(self, prompt: str, max_tokens: int = 8000, retries: int = 2) -> dict | None:
        """Call Gemini with JSON response. Retry on rate limit. Repair truncated JSON."""
        try:
            from google.genai import types
            client = self._get_gemini_client()
        except Exception as exc:
            self._log("ERROR", f"Gemini init failed: {exc}")
            return None

        for attempt in range(retries + 1):
            try:
                resp = client.models.generate_content(
                    model="gemini-2.5-flash",
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

    def _gemini_text(self, prompt: str, max_tokens: int = 8000, retries: int = 2) -> str:
        """Call Gemini for FREE TEXT (no JSON mode). Returns plain string."""
        try:
            from google.genai import types
            client = self._get_gemini_client()
        except Exception as exc:
            self._log("ERROR", f"Gemini init failed: {exc}")
            return ""

        for attempt in range(retries + 1):
            try:
                resp = client.models.generate_content(
                    model="gemini-2.5-flash",
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
            "version": "3.0",
            "generated_at": datetime.now().isoformat(),
            "model": "gemini-2.5-flash",
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

        # Call 1: TEXTO libre — prompt nivel Avantage
        texto = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"Escribe un RESUMEN EJECUTIVO que permita entender el fondo completo en un solo vistazo.\n"
            f"El texto debe ser NARRATIVA FLUIDA con hilo conductor — NO una lista de datos.\n"
            f"Cada párrafo debe fluir al siguiente con transiciones naturales.\n\n"
            f"ESTRUCTURA OBLIGATORIA (usa **Título** en negrita para cada subsección):\n"
            f"**Descripción general**: Qué es el fondo, quién lo gestiona y asesora, cuándo se creó, AUM actual, partícipes. "
            f"Contextualizar la posición del fondo en su categoría.\n"
            f"**Filosofía y proceso de inversión**: Cómo invierte, qué criterios aplica, qué le diferencia. "
            f"Incluir citas del gestor si las hay. Explicar la evolución del enfoque si ha habido cambios.\n"
            f"**Track record**: TODAS las rentabilidades anuales con cifras concretas, VL base 100, "
            f"comparación con categoría. Destacar los mejores y peores años con contexto.\n"
            f"**Evolución del patrimonio**: Crecimiento del AUM y partícipes año a año, fases de crecimiento, "
            f"episodios de salidas/entradas masivas y sus causas.\n"
            f"**Conclusión**: 2-3 frases que sinteticen la propuesta de valor del fondo, para quién es adecuado, "
            f"y los principales riesgos a considerar.\n\n"
            f"MÍNIMO 3.000 caracteres. Cada subsección 2-3 párrafos FLUIDOS.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: DATOS estructurados
        datos = self._gemini_call(
            f"Extrae datos estructurados de este fondo. Responde SOLO JSON.\n"
            f"Schema: {{\"fortalezas\":[\"string específica\"],\"riesgos\":[\"string específica\"],"
            f"\"para_quien_es\":\"string\",\"compromiso_gestor\":\"string\","
            f"\"signal\":\"POSITIVO|NEUTRAL|NEGATIVO\",\"signal_rationale\":\"string\"}}\n\n"
            f"DATOS:\n{input_data}"
        )
        if not datos:
            datos = {}

        datos["texto"] = texto
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
            f"Escribe la HISTORIA del fondo como una NARRATIVA CRONOLÓGICA FLUIDA — NO como lista de bullets.\n"
            f"El texto debe leer como un artículo de análisis, con párrafos conectados por transiciones naturales.\n"
            f"Agrupa los años en FASES temáticas (no año por año mecánicamente).\n"
            f"Estructura con subsecciones usando **Periodo: Título descriptivo** en negrita:\n"
            f"Ejemplo: **2017-2019: Los primeros años de construcción**, **2020: COVID como punto de inflexión**\n"
            f"Cada subsección: 1-2 párrafos con datos concretos (AUM, partícipes, VL, rentabilidad).\n"
            f"Incluye episodios de crecimiento, crisis, cambios regulatorios, hitos.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        datos = self._gemini_call(
            f"Extrae hitos del fondo. JSON: {{\"hitos\":[{{\"anio\":\"YYYY\",\"evento\":\"string\"}}]}}\n"
            f"DATOS:\n{input_data}"
        )

        result = datos if datos else {}
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

        # Call 1: TEXTO — perfil detallado del gestor principal
        texto_principal = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"Escribe un PERFIL DETALLADO de CADA gestor del fondo (no solo el principal).\n"
            f"Para el gestor principal, escribe extensamente (mín 2.000 chars). Para los demás, 1-2 párrafos.\n\n"
            f"Para CADA gestor incluye:\n"
            f"**[Nombre del gestor] — [Cargo]**\n"
            f"- Trayectoria profesional: formación, experiencia previa, años en el fondo\n"
            f"- Filosofía de inversión: cómo piensa, qué le diferencia, CITAS TEXTUALES si las hay\n"
            f"- Decisiones clave documentadas: con CONTEXTO de mercado + qué hizo + RESULTADO concreto\n"
            f"- Rasgos diferenciales: estilo de comunicación, transparencia, coinversión\n\n"
            f"IMPORTANTE: Escribe NARRATIVA FLUIDA, no bullets. Los párrafos deben fluir con transiciones.\n"
            f"Si hay citas del gestor en las fuentes, inclúyelas entrecomilladas.\n"
            f"MÍNIMO 4.000 caracteres total.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: TEXTO — overview del equipo y roles
        texto_equipo = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"Escribe 3 párrafos sobre el equipo gestor en su conjunto:\n"
            f"1. Composición, estructura, número de personas, estabilidad (¿ha habido cambios?)\n"
            f"2. Filosofía compartida, cómo se complementan los gestores\n"
            f"3. Conclusión: fortalezas y riesgos del equipo (ej: persona clave, experiencia, etc.)\n"
            f"para cada persona: **Nombre — Cargo** seguido de 1-2 frases con su rol.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 3: DATOS estructurados
        datos = self._gemini_call(
            f"Extrae perfiles del equipo. JSON:\n"
            f"{{\"perfiles\":[{{\"nombre\":\"\",\"cargo\":\"\",\"decisiones_clave\":[\"string\"]}}]}}\n"
            f"DATOS:\n{input_data}"
        )

        result = datos if datos else {}
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

        # Call 1: TEXTO narrativo — nivel Avantage
        texto = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"Escribe un ANÁLISIS DE LA ESTRATEGIA Y COHERENCIA del fondo.\n"
            f"Este NO es un resumen de hechos — es una EVALUACIÓN con pensamiento detrás.\n\n"
            f"ESTRUCTURA:\n"
            f"Párrafo 1-2: Visión general de la evolución estratégica. ¿Han sido coherentes entre lo que dicen y lo que hacen?\n"
            f"¿Qué ha cambiado y qué se ha mantenido? ¿Hay aprendizaje institucional?\n\n"
            f"Luego organiza por FASES con **Periodo — Título** en negrita:\n"
            f"Para CADA fase (mínimo 4 fases):\n"
            f"- Contexto de mercado en ese periodo\n"
            f"- Qué decisiones tomó el equipo (nombres de posiciones, % de exposición)\n"
            f"- Qué resultado obtuvieron (rentabilidad concreta, AUM, partícipes)\n"
            f"- ¿Fue coherente con lo que decían? ¿Aprendieron algo?\n\n"
            f"Párrafo final: Síntesis evaluativa — ¿es un equipo coherente? ¿cuáles son sus patrones de acierto y error?\n\n"
            f"MÍNIMO 4.000 caracteres. NARRATIVA FLUIDA, no lista de hechos.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: DATOS hitos — forzar al menos 4
        datos = self._gemini_call(
            f"Extrae AL MENOS 4 hitos estratégicos del fondo, uno por cada fase temporal relevante.\n"
            f"JSON: {{\"hitos_estrategia\":[{{\"periodo\":\"2017-2019\",\"cambio\":\"descripción concreta\"}}],"
            f"\"estrategia_actual_resumen\":\"resumen de la estrategia actual en 2-3 frases\"}}\n"
            f"IMPORTANTE: Genera MÍNIMO 4 hitos con periodos diferentes.\n"
            f"DATOS:\n{input_data}"
        )

        result = datos if datos else {}
        result["texto"] = texto
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

        # Call 1: TEXTO análisis
        texto = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"Escribe un ANÁLISIS de la cartera actual (datos a fecha {last_period}).\n"
            f"IMPORTANTE: La fecha de los datos es {last_period}. NO inventes otra fecha.\n"
            f"Estructura con subsecciones usando **Título** en negrita:\n"
            f"- **Composición general** — nº posiciones, distribución RV/RF/liquidez\n"
            f"- **Renta Variable España** — principales posiciones con peso y por qué\n"
            f"- **Renta Variable Internacional** — por bloques temáticos (tech, emergentes, Europa)\n"
            f"- **Renta Fija** — emisores, cupones, vencimientos, riesgo\n"
            f"- **Concentración y riesgos** — temática (ej: total Argentina), top 5/10\n"
            f"Cada subsección: 1-2 párrafos con nombres y cifras concretas.\n\n"
            f"DATOS:\n{input_data}"
        )
        time.sleep(2)

        # Call 2: DATOS estructurados
        datos = self._gemini_call(
            f"Extrae distribución de cartera. JSON:\n"
            f"{{\"concentracion\":{{\"top5_pct\":0,\"top10_pct\":0,\"top15_pct\":0}},"
            f"\"distribucion_tipo\":{{\"rv_españa_pct\":0,\"rv_internacional_pct\":0,\"rf_pct\":0,\"liquidez_pct\":0}}}}\n"
            f"DATOS:\n{input_data}"
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

        input_data = json.dumps({"fuentes": items_compact[:15]}, ensure_ascii=False)

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

        # Call 2: DATOS opiniones (con titulo y url)
        datos = self._gemini_call(
            f"Extrae opiniones clave de cada fuente. JSON:\n"
            f"{{\"opiniones_clave\":[{{\"fuente\":\"\",\"titulo\":\"titulo del análisis\",\"url\":\"url de la fuente\",\"opinion\":\"resumen sustancial de lo que dice\",\"sentimiento\":\"POSITIVO|NEUTRAL|NEGATIVO\"}}]}}\n"
            f"DATOS:\n{input_data}"
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
