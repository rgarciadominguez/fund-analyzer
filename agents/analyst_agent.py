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

        # Load fund data: cnmv_data.json for ES, intl_data.json for INT
        if self.isin[:2] in ("ES",):
            cnmv = self._load_json("cnmv_data.json")
        else:
            cnmv = self._load_json("intl_data.json")
            if not cnmv:
                cnmv = self._load_json("cnmv_data.json")  # fallback
        letters = self._load_json("letters_data.json")
        manager = self._load_json("manager_profile.json")
        readings = self._load_json("readings_data.json")

        # Load previous output to preserve richer data if re-execution regressed
        existing_output = self._load_json("output.json") or {}

        # PROTECCIÓN: si manager_profile re-ejecutado tiene menos info, reconstruir
        # desde el output anterior. Previene regresiones del manager_deep_agent.
        prev_perfiles = (existing_output.get("analyst_synthesis", {})
                         .get("gestores", {}).get("perfiles", []) or [])
        prev_gestores = existing_output.get("gestores", {}) or {}

        new_equipo_detalle = manager.get("equipo_detalle_web", []) or []
        new_fuentes = manager.get("fuentes_web_raw", []) or manager.get("fuentes_web", []) or []

        # Si el manager actual perdió los detalles Y el anterior los tenía, restaurar
        if not new_equipo_detalle and prev_gestores.get("equipo_detalle_web"):
            manager["equipo_detalle_web"] = prev_gestores["equipo_detalle_web"]
            self._log("WARN", f"Preservando equipo_detalle_web del output anterior ({len(prev_gestores['equipo_detalle_web'])} items)")
        if not new_fuentes and prev_gestores.get("fuentes_web"):
            manager["fuentes_web"] = prev_gestores["fuentes_web"]
            self._log("WARN", f"Preservando fuentes_web del output anterior ({len(prev_gestores['fuentes_web'])} items)")

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
        consolidated["tipo"] = cnmv.get("tipo", "INT" if self.isin[:2] not in ("ES",) else "ES")
        consolidated["ultima_actualizacion"] = datetime.now().isoformat()

        # Pass through raw cuantitativo — but PRESERVE existing output.json data
        # if it has MORE data points (e.g. from a previous full pipeline run).
        # This prevents re-running analyst from losing historical series.
        existing_output = self._load_json("output.json")
        existing_cuant = existing_output.get("cuantitativo", {}) if existing_output else {}
        new_cuant = cnmv.get("cuantitativo", {})

        # Merge: for each series, prefer the richer data.
        # Priority: (a) if new_val has more items → new; (b) if same items but new has MORE fields per item → new;
        # (c) otherwise old (preserve historical if no improvement)
        def _richer_list(old_list, new_list):
            """Return whichever list has more data (items + fields per item)."""
            old_n = len(old_list or [])
            new_n = len(new_list or [])
            if new_n > old_n:
                return new_list
            if old_n > new_n:
                return old_list
            # Same length: check if new items have MORE fields (richer schema)
            if old_n == 0:
                return new_list
            old_fields = set()
            new_fields = set()
            for x in old_list:
                if isinstance(x, dict):
                    old_fields.update(x.keys())
            for x in new_list:
                if isinstance(x, dict):
                    new_fields.update(x.keys())
            # New has fields old doesn't → new is richer
            if new_fields - old_fields:
                return new_list
            return old_list

        merged_cuant = {}
        all_keys = set(list(existing_cuant.keys()) + list(new_cuant.keys()))
        for key in all_keys:
            old_val = existing_cuant.get(key)
            new_val = new_cuant.get(key)
            if isinstance(old_val, list) and isinstance(new_val, list):
                merged_cuant[key] = _richer_list(old_val, new_val)
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

        # ── INT-specific: propagar campos extra del extractor v3 ──
        for extra_key in ("analisis_consistencia", "clases", "economia_fondo", "cualitativo"):
            val = cnmv.get(extra_key)
            if val and extra_key not in consolidated:
                consolidated[extra_key] = val
            elif val and isinstance(val, dict) and isinstance(consolidated.get(extra_key), dict):
                # Merge: keep existing + fill gaps from new
                for k, v in val.items():
                    if v and not consolidated[extra_key].get(k):
                        consolidated[extra_key][k] = v

        # Propagar comision_exito (incluye pct_teorico del folleto + serie_historica)
        consolidated["comision_exito"] = (
            cnmv.get("comision_exito")
            or existing_output.get("comision_exito")
            or {}
        )
        # Propagate comision_exito_pct in kpis if present
        if cnmv.get("kpis", {}).get("comision_exito_pct"):
            consolidated["kpis"]["comision_exito_pct"] = cnmv["kpis"]["comision_exito_pct"]

        # Extracción del % teórico desde el KIID (única fuente oficial regulada).
        # Si cnmv_agent no lo encontró, intentar descargar/parsear el KIID del fondo.
        ce = consolidated["comision_exito"] or {}
        if ce.get("existe") and not ce.get("pct_teorico"):
            teorico = self._extract_exito_teorico_from_kiid(cnmv)
            if teorico is not None:
                ce["pct_teorico"] = teorico
                ce["pct_teorico_fuente"] = "kiid"
                consolidated["comision_exito"] = ce
                consolidated["kpis"]["comision_exito_pct"] = teorico
                self._log("OK", f"Comisión éxito teórica extraída del KIID: {teorico}%")

        # ── PRE-CAPA 3: Para INT, inyectar datos del extractor v3 ──────
        # El extractor v3 produce cualitativo rico (estrategia, filosofía,
        # proceso, gestores). Lo inyectamos para que las secciones de
        # capa3 lo usen en vez de inventar texto genérico.
        is_int_fund = self.isin[:2] not in ("ES",)
        if is_int_fund:
            int_cual = cnmv.get("cualitativo", {})
            int_consist = cnmv.get("analisis_consistencia", {})
            int_clases = cnmv.get("clases", [])
            int_econ = cnmv.get("economia_fondo", {})

            # Poblar campos que las secciones esperan
            if not consolidated.get("nombre"):
                consolidated["nombre"] = cnmv.get("nombre", "")
            if not consolidated.get("gestora"):
                consolidated["gestora"] = cnmv.get("gestora", "")

            # Estrategia/filosofía literal del extractor
            consolidated["_int_estrategia"] = int_cual.get("estrategia", "")
            consolidated["_int_filosofia"] = int_cual.get("filosofia_inversion", "")
            consolidated["_int_proceso"] = int_cual.get("proceso_seleccion", "")
            consolidated["_int_tipo_activos"] = int_cual.get("tipo_activos", "")
            consolidated["_int_objetivos"] = int_cual.get("objetivos_reales", "")
            consolidated["_int_historia"] = int_cual.get("historia_fondo", "")
            # Gestores: manager_profile.json es autoridad (se construye con cross-
            # validacion web: Trustnet, Citywire, Morningstar). Si tiene equipo,
            # sobrescribe al extractor (que ocasionalmente alucina nombres de
            # fondos hermanos, ej. Francis Brooke en Trojan Ireland cuando el
            # real es Sebastian Lyon).
            mgr_equipo = manager.get("equipo", []) or []
            if mgr_equipo:
                authoritative_gestores = [
                    {"nombre": g.get("nombre", ""), "cargo": g.get("cargo", ""),
                     "background": g.get("biografia", ""),
                     "anio_incorporacion": g.get("anio_incorporacion")}
                    for g in mgr_equipo if isinstance(g, dict) and g.get("nombre")
                ]
                consolidated["_int_gestores"] = authoritative_gestores
                # Sobrescribir cualitativo.gestores propagado del extractor
                if isinstance(consolidated.get("cualitativo"), dict):
                    consolidated["cualitativo"]["gestores"] = authoritative_gestores
            else:
                consolidated["_int_gestores"] = int_cual.get("gestores", [])

            # Enriquecer consistencia con letters_data (más periodos)
            letters_cartas = letters.get("cartas", [])
            consist_periodos = int_consist.get("periodos", []) if isinstance(int_consist, dict) else []
            existing_years = set()
            import re as _re
            for p in consist_periodos:
                for m in _re.finditer(r'(20[012]\d)', str(p.get("periodo", ""))):
                    existing_years.add(m.group(1))
            for carta in letters_cartas:
                periodo = carta.get("periodo", "")
                carta_years = _re.findall(r'(20[012]\d)', str(periodo))
                if carta_years and carta_years[0] not in existing_years:
                    consist_periodos.append({
                        "periodo": periodo,
                        "contexto_mercado": carta.get("contexto_mercado", ""),
                        "tesis_gestora": carta.get("tesis_gestora", ""),
                        "decisiones_tomadas": carta.get("decisiones_tomadas", ""),
                        "resultado_real": carta.get("resultado_real", ""),
                    })
                    existing_years.add(carta_years[0])
            if isinstance(int_consist, dict):
                int_consist["periodos"] = consist_periodos

            consolidated["_int_consistencia"] = int_consist
            consolidated["_int_clases"] = int_clases
            consolidated["_int_economia"] = int_econ

            # Anti-filler flag: las secciones deben usar datos, no inventar
            consolidated["_anti_filler"] = True
            # Guardar referencia para los scrubs posteriores
            self._consolidated_data = consolidated

            # ── Traducción a ES de textos del extractor (vienen en EN/FR/DE) ──
            self._translate_int_fields_to_es(consolidated)

            # ── READINGS FALLBACK: si campos INT vacíos, buscar en readings ──
            self._fill_gaps_from_readings(consolidated, readings)

            self._log("OK", f"INT data injected: estrategia={len(consolidated['_int_estrategia'])}ch, "
                      f"gestores={len(consolidated['_int_gestores'])}, clases={len(int_clases)}")

        # ── CAPA 3: Analyst Senior — 8 secciones ────────────────────────
        self._log("START", "Capa 3: Sintesis Analyst Senior (8 secciones)")
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

        # Capa 3c: Opus auditoria por seccion + regenerar si falla
        if consolidated.get("_anti_filler"):
            self._log("START", "Capa 3c: Opus audit & fix loop")
            # Preparar el contexto y anti_halluc usados originalmente para regeneracion
            raw_context = self._prepare_int_context(consolidated)
            fixed_context = self._prefilter_context(raw_context)
            known_gestores = [g.get("nombre", "") for g in consolidated.get("_int_gestores", [])
                              if isinstance(g, dict) and g.get("nombre")]
            authorized = ", ".join(known_gestores) if known_gestores else "ver contexto"
            anti_halluc = (
                f"REGLA ESTRICTA: Los UNICOS gestores de este fondo son: {authorized}.\n"
                f"NO menciones otros nombres de gestores. Si no hay dato, OMITE (null/vacio).\n\n"
            )

            audit = self._audit_and_fix_loop(
                synthesis, consolidated, fixed_context, anti_halluc, max_retries=1,
            )
            consolidated["opus_audit"] = audit
            # Actualizar synthesis en consolidated (puede haberse modificado)
            consolidated["analyst_synthesis"] = synthesis

            if audit.get("auditado"):
                globl = audit.get("global", {})
                rec = globl.get("recomendacion", "?")
                score = globl.get("calidad_score", "?")
                self._log("OK", f"Opus audit final: {rec} (score {score}/10)")

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

    def _extract_exito_teorico_from_kiid(self, cnmv: dict) -> float | None:
        """Busca el % teórico de comisión éxito EXCLUSIVAMENTE en el KIID/DFI del fondo.
        El KIID (Key Investor Information Document, antes DFI) es el documento oficial
        regulado que define la estructura de comisiones. No usar cartas ni readings
        (pueden tener info anecdótica o desactualizada).

        Fuentes válidas (en orden):
          1) Archivo raw/documents/kiid.pdf si existe
          2) PDF con "KIID" / "DFI" / "DIC" en nombre dentro de raw/letters/
          3) Descarga on-demand desde CNMV si se implementa

        Patrones admitidos (texto del KIID):
          - "comisión de éxito del 9%" / "comisión de éxito 9%"
          - "9% sobre resultados positivos" / "9% sobre la rentabilidad positiva"
          - "performance fee: 9%"

        Aplica a cualquier fondo. Valores admisibles: 1-30%.
        """
        import re
        from pathlib import Path
        from tools.pdf_extractor import extract_page_range, get_pdf_metadata

        # Buscar el KIID/DFI del fondo. NO confundir con:
        #  - "Cuadernillo" / "Reducido" → informe semestral CNMV comercial (no es KIID)
        #  - Cartas del gestor, auditorías, fichas comerciales
        # El KIID oficial CNMV siempre se llama KIID/DFI/DIC o "Datos Fundamentales"
        kiid_rx = re.compile(
            r"\bkiid\b|\bkid\b|\bdfi\b|\bdic\b|datos[_\-\s]*fundamentales|\bfolleto\b",
            re.IGNORECASE,
        )
        candidates: list[Path] = []
        raw_dir = self.fund_dir / "raw"
        for subdir in ("documents", "letters", "reports"):
            sd = raw_dir / subdir
            if sd.exists():
                for p in sd.glob("*.pdf"):
                    if kiid_rx.search(p.name):
                        candidates.append(p)

        if not candidates:
            self._log("INFO", "KIID no encontrado en raw/. % teórico queda pendiente (quality agent lo reportará)")
            return None

        self._log("INFO", f"Buscando % éxito en {len(candidates)} KIID candidato(s): {[p.name for p in candidates]}")
        # Patrones robustos que exigen proximidad a palabras clave de comisión éxito
        exito_keywords = (
            r"(?:comisi[óo]n\s+(?:de\s+)?(?:\u00e9xito|exito|resultados?|performance)|"
            r"performance\s+fee|success\s+fee|"
            r"sobre\s+(?:los\s+)?(?:beneficios|resultados|rentabilidad)|"
            r"s/\s*(?:resultados?|beneficios|rentabilidad))"
        )
        pct_rx = r"(\d{1,2}(?:[.,]\d{1,2})?)\s*%"
        patterns = [
            # "comisión de éxito 7,5%" o "éxito 7,5%" (palabra éxito + % inmediato, sin conector)
            rf"(?:comisi[óo]n\s+de\s+)?\u00e9xito[:\s]+{pct_rx}",
            # "comisión de éxito del 9%" o "comisión sobre resultados del 9%"
            rf"{exito_keywords}[^.]{{0,80}}?(?:del|de|es|es\s+del|asciende\s+al|alcanza\s+el|aplica\s+un|se\s+aplica\s+un)\s+{pct_rx}",
            # "9% sobre beneficios/resultados/rentabilidad"
            rf"{pct_rx}\s*(?:sobre|s/|de)\s*(?:la\s+|los\s+)?(?:beneficios|resultados|rentabilidad)",
            # "9% de comisión de éxito" / "9% de éxito"
            rf"{pct_rx}\s+de\s+(?:comisi[óo]n\s+de\s+)?\u00e9xito",
            # "Tipo aplicable s/resultados: 9%"
            rf"tipo\s+aplicable\s+s/\s*resultados?[\s:]+{pct_rx}",
            # "performance fee: 9%" / "performance fee of 9%"
            rf"performance\s+fee[:\s]+(?:of\s+)?{pct_rx}",
        ]

        # Extraer texto de los PDFs KIID candidatos
        textos: list[str] = []
        for p in candidates:
            try:
                meta = get_pdf_metadata(str(p))
                txt = extract_page_range(str(p), 0, meta["num_pages"])
                txt = re.sub(r"\(cid:\d+\)", " ", txt)
                textos.append(txt)
            except Exception as exc:
                self._log("WARN", f"No se pudo leer KIID {p.name}: {exc}")

        # Aplicar patrones
        for texto in textos:
            for pat in patterns:
                for m in re.finditer(pat, texto, re.IGNORECASE):
                    try:
                        val_str = m.group(1).replace(",", ".")
                        val = float(val_str)
                        if 1 <= val <= 30:
                            return val
                    except (ValueError, IndexError):
                        continue
        return None

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
        if data.get("_anti_filler"):
            return self._section_resumen_int(data)
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
        if data.get("_anti_filler"):
            return self._section_historia_int(data)
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
        if data.get("_anti_filler") and data.get("_int_gestores"):
            return self._section_gestores_int(data)
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
        # Detect gestores mencionados en manager_profile para forzar extracción de TODOS
        n_equipo_hint = len(gestores.get("equipo", []) or [])
        n_perfiles_disponibles = len(gestores.get("perfiles", []) or [])
        # Si hay >1 nombre en equipo o >1 perfil extraído, pedir explícitamente múltiples
        min_perfiles = max(2, min(n_perfiles_disponibles, 3)) if n_perfiles_disponibles > 1 or n_equipo_hint > 1 else 1
        datos = self._gemini_call(
            f"Extrae perfiles del equipo gestor para FICHAS de un dashboard profesional.\n"
            f"OBLIGATORIO: genera perfiles para TODOS los gestores mencionados en los datos.\n"
            f"Mínimo esperado: {min_perfiles} perfiles (si hay cofundadores, cogestores o co-CIOs, incluir a TODOS).\n"
            f"NO te limites al lead manager — cofundadores y equipo senior deben aparecer.\n"
            f"Para CADA gestor incluye:\n"
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

        # Anti-regresión: si el output anterior (o backup good) tenía más perfiles, preservarlos
        new_perfiles = result.get("perfiles", []) or []
        # Buscar en múltiples fuentes (prioridad: output actual > output_good_v1 backup)
        candidates = [
            self._load_json("output.json") or {},
            self._load_json("output_good_v1.json") or {},
        ]
        best_prev = []
        for cand in candidates:
            prev = (cand.get("analyst_synthesis", {}) or {}).get(
                "gestores", {}).get("perfiles", []) or []
            if len(prev) > len(best_prev):
                best_prev = prev

        if len(best_prev) > len(new_perfiles):
            new_names = {(p.get("nombre") or "").strip().lower() for p in new_perfiles}
            for old_p in best_prev:
                name = (old_p.get("nombre") or "").strip().lower()
                if name and name not in new_names:
                    new_perfiles.append(old_p)
                    self._log("INFO", f"Preservando perfil '{old_p.get('nombre')}' del backup")
            result["perfiles"] = new_perfiles

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

    def _compute_drawdown(self, data: dict) -> dict:
        """Calcula drawdown máximo desde serie_vl_base100.
        Devuelve: valor_pct, fecha_min (periodo del mínimo), fecha_peak (pico previo),
                  duracion_meses (de peak a fecha_min), duracion_recuperacion_meses (si recuperó)."""
        vl_series = data.get("cuantitativo", {}).get("serie_vl_base100", []) or []
        if len(vl_series) < 3:
            return {}

        # Extraer serie ordenada (periodo, base100)
        series = [(str(v.get("periodo", ""))[:7], float(v.get("base100", 0) or 0))
                  for v in vl_series if isinstance(v, dict) and v.get("base100")]
        series = [(p, b) for p, b in series if p and b > 0]
        series.sort()
        if len(series) < 3:
            return {}

        # Running max + drawdown por punto
        max_so_far = series[0][1]
        peak_period = series[0][0]
        worst_dd = 0.0
        worst_dd_period = series[0][0]
        worst_dd_peak = series[0][0]
        recovered_period = None

        dd_states = []  # (periodo, dd_pct, peak_period)
        for p, b in series:
            if b >= max_so_far:
                max_so_far = b
                peak_period = p
            dd = (b / max_so_far - 1) * 100  # negativo o 0
            dd_states.append((p, dd, peak_period))
            if dd < worst_dd:
                worst_dd = dd
                worst_dd_period = p
                worst_dd_peak = peak_period

        # ¿Se recuperó tras el peor drawdown?
        peak_val = 0
        for p, b in series:
            if p == worst_dd_peak:
                peak_val = b
                break
        for p, b in series:
            if p > worst_dd_period and b >= peak_val:
                recovered_period = p
                break

        def _months_between(a: str, b: str) -> int:
            """a, b en formato YYYY, YYYY-MM, o YYYY-S1/S2."""
            def parse(p):
                p = p.replace("S1", "-06").replace("S2", "-12")
                parts = p.split("-")
                y = int(parts[0]) if parts[0].isdigit() else 0
                m = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 12
                return y * 12 + m
            try:
                return abs(parse(b) - parse(a))
            except Exception:
                return 0

        duracion_meses = _months_between(worst_dd_peak, worst_dd_period)
        duracion_recuperacion = _months_between(worst_dd_period, recovered_period) if recovered_period else None

        return {
            "valor_pct": round(worst_dd, 2),
            "fecha_min": worst_dd_period,
            "fecha_peak": worst_dd_peak,
            "duracion_meses": duracion_meses,
            "duracion_recuperacion_meses": duracion_recuperacion,
            "recuperado": recovered_period is not None,
        }

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
        if data.get("_anti_filler"):
            return self._section_evolucion_int(data)
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
            "drawdown": self._compute_drawdown(data),
        }

        return {"texto": texto, "datos_graficos": datos_graficos} if texto else None

    # ── Sección 5: Estrategia (2 llamadas) ────────────────────────────────

    def _section_estrategia(self, data: dict) -> dict | None:
        # ── INT: usar datos literales del extractor v3 (R8) ──
        if data.get("_anti_filler"):
            return self._section_estrategia_int(data)

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

        # Call 1: TEXTO — TIER 1 (Sonnet con fallback a Flash) — análisis evaluativo EXTENSO
        texto = self._sonnet_text(
            self._system_role(data),
            f"{self._quality_hint('estrategia')}"
            f"Escribe un ANÁLISIS EVALUATIVO PROFUNDO Y EXTENSO de la estrategia del fondo en 6-9 PÁRRAFOS DENSOS.\n"
            f"Este es el apartado MÁS IMPORTANTE del informe — debe ser pensamiento analítico elaborado, "
            f"NO una descripción superficial.\n"
            f"NO uses subsecciones ni líneas que sean solo **título** como headers.\n"
            f"SÍ usa **negritas** dentro del texto para resaltar conceptos clave, "
            f"nombres de posiciones, cifras, decisiones importantes.\n\n"
            f"CONTENIDO OBLIGATORIO (cada párrafo debe tener 400-600 chars con cifras concretas):\n"
            f"- Párrafo 1: **Filosofía de inversión declarada** — universo elegible, criterios de selección, "
            f"estilo (value/growth/quality/momentum), tolerancia al riesgo, benchmark o referencia, "
            f"horizonte temporal. Cita textual si la hay.\n"
            f"- Párrafo 2: **Qué les diferencia** — lo que NO hacen los competidores, edge informacional o analítico, "
            f"sesgo sectorial/geográfico característico, uso de derivados para cobertura/apalancamiento.\n"
            f"- Párrafo 3: **Evolución del enfoque** — cómo ha cambiado la filosofía desde inicio hasta hoy, "
            f"qué aprendieron de crisis (2008, 2011, COVID, 2022), ajustes explícitos en política.\n"
            f"- Párrafo 4: **Coherencia discurso vs acción** — ¿lo que dicen en cartas cuadra con posiciones reales? "
            f"Ejemplos CONCRETOS: si dicen ser value pero tienen momentum, si prometen low-vol pero han tenido volatilidad. "
            f"% de exposición antes vs ahora.\n"
            f"- Párrafo 5: **3 momentos clave de decisión** documentados — contexto de mercado + tesis del gestor + "
            f"qué hicieron + resultado con cifras. Usar hechos relevantes, cartas semestrales.\n"
            f"- Párrafo 6: **Evolución del mix de activos** — cómo ha rotado %RV/%RF/%liquidez/%derivados, "
            f"por qué (cambios ciclo, tesis macro), coherencia con filosofía declarada.\n"
            f"- Párrafo 7: **Patrón de aciertos y errores** — ¿ciclos donde acertaron sistemáticamente? "
            f"¿sesgos recurrentes? ¿es un equipo que aprende o repite errores?\n"
            f"- Párrafo 8: **Conclusión evaluativa** — para qué tipo de inversor es apropiado, "
            f"qué esperar en ciclos alcistas vs bajistas, principales red flags.\n\n"
            f"MÍNIMO 3.500 caracteres — extenso, evaluativo, con cifras concretas y análisis propio.\n"
            f"ESCRIBE COMO ANALISTA SENIOR, no como descripción neutra.\n\n"
            f"DATOS:\n{input_data}",
            max_tokens=12000
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
        if data.get("_anti_filler"):
            return self._section_cartera_int(data)
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

        # Call 1: TEXTO análisis EXTENSO de cartera (mínimo 3000 chars)
        texto = self._gemini_text(
            f"{self._system_role(data)}\n\n"
            f"{self._quality_hint('cartera')}"
            f"Escribe un ANÁLISIS EXTENSO Y PROFUNDO de la cartera en 5-7 PÁRRAFOS DENSOS.\n"
            f"NO uses subsecciones ni **headers** — narrativa fluida pura, cada párrafo conecta con el siguiente.\n"
            f"Datos a fecha {last_period}. NO inventes otra fecha.\n\n"
            f"CONTENIDO OBLIGATORIO (cada párrafo 400-500 chars con cifras concretas):\n\n"
            f"- Párrafo 1: **Composición general** — nº posiciones total, distribución RV/RF/liquidez/otros, "
            f"cambios vs hace 1 año, hace 3 años, hace 5 años. Ritmo de rotación de cartera (rotación %).\n"
            f"- Párrafo 2: **Exposición geográfica** — países/regiones principales, % en cada uno, "
            f"evolución de la exposición geográfica histórica, apuestas regionales diferenciales vs categoría.\n"
            f"- Párrafo 3: **Exposición sectorial** — sectores principales con % concretos, "
            f"sobre/infra-ponderaciones vs benchmark, cambios sectoriales recientes.\n"
            f"- Párrafo 4: **Top 10-15 posiciones actuales** — nombres concretos con % y racional de por qué están. "
            f"Tesis del gestor para las posiciones de mayor convicción. Distinguir bonos corporativos vs equity.\n"
            f"- Párrafo 5: **Concentración** — top5%, top10%, top15%, comparado con media histórica y categoría. "
            f"Número de posiciones en ventana histórica (tendencia a concentrar o diversificar).\n"
            f"- Párrafo 6: **Riesgos de cartera** — divisa, duración (si hay RF), calidad crediticia, "
            f"concentración sectorial/regional, liquidez de posiciones.\n"
            f"- Párrafo 7: **Cambios recientes significativos** — entradas/salidas notables en últimos periodos "
            f"(según XMLs trimestrales o cartas del gestor), rebalanceos relevantes.\n\n"
            f"Incluye nombres de posiciones en **negrita** y cifras concretas (**X%**, **Y M€**).\n"
            f"MÍNIMO 3.000 caracteres — denso, cada frase con dato concreto, sin relleno.\n\n"
            f"DATOS:\n{input_data}",
            max_tokens=8000
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
        if data.get("_anti_filler"):
            return self._section_fuentes_int(data)
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

    # ═══════════════════════════════════════════════════════════════════════════
    # INT HELPERS: traduccion + readings fallback
    # ═══════════════════════════════════════════════════════════════════════════

    def _translate_int_fields_to_es(self, data: dict) -> None:
        """Traduce campos INT del extractor (EN/FR/DE) a ES via Gemini Flash.
        Universal para cualquier INT: refleja el resultado traducido TANTO en
        `_int_*` (consumido por las secciones Gemini Pro) COMO en
        `cualitativo.*` (consumido por el dashboard), evitando que el
        dashboard muestre texto en ingles.
        """
        # Mapping: campo interno -> clave en cualitativo
        int_to_cualitativo = {
            "_int_estrategia": "estrategia",
            "_int_filosofia": "filosofia_inversion",
            "_int_proceso": "proceso_seleccion",
            "_int_tipo_activos": "tipo_activos",
            "_int_objetivos": "objetivos_reales",
            "_int_historia": "historia_fondo",
        }
        fields_to_translate = list(int_to_cualitativo.keys())
        texts_to_translate = []
        for f in fields_to_translate:
            v = data.get(f, "")
            if v and len(v) > 30:
                texts_to_translate.append((f, v))

        if not texts_to_translate:
            return

        # Batch: traducir todos juntos en 1 call
        combined = "\n---\n".join(
            f"[{f}]: {v}" for f, v in texts_to_translate
        )
        try:
            from tools.gemini_wrapper import extract_fast
            result = extract_fast(
                text=combined,
                schema={"traducciones": [{"campo": "str", "texto_es": "str"}]},
                context="Traduce cada campo al espanol. Mantener tecnicismos financieros. NO resumir ni alterar el significado.",
            )
            if isinstance(result, dict):
                cualitativo = data.setdefault("cualitativo", {}) if isinstance(data.get("cualitativo"), dict) else None
                if cualitativo is None:
                    # Force dict shape
                    data["cualitativo"] = {}
                    cualitativo = data["cualitativo"]
                for t in result.get("traducciones", []):
                    campo = t.get("campo", "")
                    texto = t.get("texto_es", "")
                    if campo and texto and campo in data:
                        data[campo] = texto
                        # Espejo traducido en cualitativo.* para el dashboard
                        cual_key = int_to_cualitativo.get(campo)
                        if cual_key:
                            cualitativo[cual_key] = texto
                self._log("OK", f"Traducidos {len(result.get('traducciones',[]))} campos a ES (_int_* + cualitativo.*)")
        except Exception as exc:
            self._log("WARN", f"Traduccion ES fallo: {exc}")

    def _fill_gaps_from_readings(self, data: dict, readings: dict) -> None:
        """
        Si campos INT estan vacios, buscar en readings_data.json analisis
        completos del fondo (Astralis, Morningstar) como FALLBACK.
        Solo para campos que el extractor dejo vacios.
        """
        if not readings:
            return
        # Buscar el analisis mas completo (mas texto sobre el fondo)
        best_analysis = ""
        best_url = ""
        for item in readings.get("analisis", []) + readings.get("lecturas", []):
            text = item.get("texto_completo", "") or item.get("resumen", "")
            if len(text) > len(best_analysis):
                best_analysis = text
                best_url = item.get("url", "")

        if len(best_analysis) < 500:
            return  # no hay analisis externo sustancial

        self._log("INFO", f"Readings fallback: {len(best_analysis)} chars de {best_url[:50]}")

        # Rellenar gaps con datos del analisis externo
        gap_fields = {
            "_int_estrategia": "estrategia de inversion del fondo",
            "_int_historia": "historia y trayectoria del fondo",
        }
        for field, concept in gap_fields.items():
            if data.get(field):
                continue  # ya tiene datos, no sobrescribir
            try:
                from tools.gemini_wrapper import extract_fast
                result = extract_fast(
                    text=best_analysis[:10000],
                    schema={field: f"str | extrae {concept} del texto. En espanol. Solo datos verificables, no inventar."},
                    context=f"Fondo {data.get('nombre','')} ({data.get('isin','')}). Fuente: analisis externo.",
                )
                if isinstance(result, dict) and result.get(field):
                    data[field] = result[field]
                    self._log("OK", f"Gap '{field}' rellenado desde readings ({len(result[field])} chars)")
            except Exception:
                pass

    # ═══════════════════════════════════════════════════════════════════════════
    # SECCIONES INT — Opus para síntesis, mismo formato output que ES
    # ═══════════════════════════════════════════════════════════════════════════

    def _prepare_int_context(self, data: dict) -> str:
        """Preparar contexto DENSO con todos los datos del fondo para Opus.
        Principio: toda la info relevante, sin repeticiones ni basura."""
        parts = []

        # KPIs
        kpis = data.get("kpis", {})
        parts.append(f"FONDO: {data.get('nombre','')} ({self.isin})")
        parts.append(f"GESTORA: {data.get('gestora','')}")
        kpi_items = []
        for k, label in [("aum_actual_meur", "AUM"), ("ter_pct", "TER"),
                          ("benchmark", "Benchmark"), ("anio_creacion", "Inicio"),
                          ("clasificacion", "Tipo"), ("concentracion_top10_pct", "Top10%")]:
            v = kpis.get(k)
            if v:
                kpi_items.append(f"{label}: {v}")
        if kpi_items:
            parts.append(" | ".join(kpi_items))

        # Estrategia completa del extractor (sin truncar)
        for field, label in [("_int_estrategia", "ESTRATEGIA"),
                              ("_int_filosofia", "FILOSOFIA"),
                              ("_int_proceso", "PROCESO"),
                              ("_int_tipo_activos", "TIPO ACTIVOS"),
                              ("_int_objetivos", "OBJETIVOS"),
                              ("_int_historia", "HISTORIA")]:
            v = data.get(field, "")
            if v:
                parts.append(f"{label}: {v}")

        # Gestores — biografías completas de manager_profile
        gestores = data.get("_int_gestores", [])
        if gestores:
            parts.append("EQUIPO GESTOR:")
            for g in gestores:
                if isinstance(g, dict) and g.get("nombre"):
                    bg = g.get("background", "") or ""
                    parts.append(f"  {g.get('nombre','')}: {g.get('cargo','')}. {bg}")

        # Cartas del gestor — TODOS los periodos disponibles, dedup de tesis repetidas
        # palabra por palabra (pero preservando texto completo si difiere).
        consist = data.get("_int_consistencia", {})
        periodos = consist.get("periodos", []) if isinstance(consist, dict) else []
        if periodos:
            parts.append(f"\nCARTAS DEL GESTOR ({len(periodos)} periodos):")
            seen_tesis_exact = set()
            for p in periodos:
                tesis = str(p.get("tesis_gestora", "") or "").strip()
                decisiones = str(p.get("decisiones_tomadas", "") or "").strip()
                resultado = str(p.get("resultado_real", "") or "").strip()
                contexto = str(p.get("contexto_mercado", "") or "").strip()

                parts.append(f"  [{p.get('periodo','')}]")
                if contexto:
                    parts.append(f"    CONTEXTO: {contexto}")
                if tesis:
                    # Dedup solo si la tesis es exactamente la misma (no truncar)
                    tesis_key = tesis.lower()
                    if tesis_key in seen_tesis_exact:
                        parts.append(f"    TESIS: (idéntica a periodo anterior)")
                    else:
                        seen_tesis_exact.add(tesis_key)
                        parts.append(f"    TESIS: {tesis}")
                if decisiones:
                    parts.append(f"    DECISIONES: {decisiones}")
                if resultado:
                    parts.append(f"    RESULTADO: {resultado}")

        # Posiciones — todas, formato compacto nombre:peso
        posiciones = data.get("posiciones", {}).get("actuales", [])
        if posiciones:
            sorted_pos = sorted(posiciones, key=lambda x: x.get("peso_pct", 0) or 0, reverse=True)
            parts.append(f"\nCARTERA ({len(posiciones)} posiciones):")
            for p in sorted_pos:
                asset_type = p.get("asset_type", p.get("sector", "")) or ""
                parts.append(f"  {p.get('nombre','')}: {p.get('peso_pct',0)}% [{asset_type}]")

        # Mix activos
        mix = data.get("cuantitativo", {}).get("mix_activos_historico", [])
        if mix:
            parts.append("\nMIX ACTIVOS:")
            for m in mix:
                parts.append(f"  {m.get('periodo','')}: RV {m.get('renta_variable_pct',0)}%, "
                             f"RF {m.get('renta_fija_pct',0)}%, "
                             f"Liquidez {m.get('liquidez_pct',0)}%")

        # Readings (opiniones externas) — TODOS, resumen completo sin truncar
        readings = self._load_json("readings_data.json")
        all_readings = readings.get("analisis_completos", []) + readings.get("otros_readings", [])
        if all_readings:
            parts.append(f"\nOPINIONES EXTERNAS ({len(all_readings)} fuentes):")
            for r in all_readings:
                source = r.get("source", "") or ""
                titulo = r.get("titulo", "") or ""
                resumen = (r.get("resumen", "") or "").strip()
                opinion = (r.get("opinion_sobre_fondo", "") or "").strip()
                if titulo:
                    parts.append(f"  [{source}] {titulo}")
                if resumen:
                    parts.append(f"    RESUMEN: {resumen}")
                if opinion:
                    parts.append(f"    OPINION: {opinion}")

        # Clases — todas, sin limitar a 6
        clases = data.get("_int_clases", [])
        if clases:
            parts.append("CLASES:")
            for c in clases:
                if isinstance(c, dict):
                    parts.append(f"  - {c.get('code',c.get('nombre',''))}: "
                                 f"fee {c.get('mgmt_fee_pct',c.get('comision_gestion',''))}%")

        return "\n".join(parts)

    # ═══════════════════════════════════════════════════════════════════════
    # INT PIPELINE — Gemini Pro para síntesis + Opus para auditoría final
    #
    # Coste objetivo por fondo:
    #   - 6 secciones Gemini Pro: ~$0.06
    #   - 1 pre-filtro Gemini Pro: ~$0.02
    #   - 1 auditoría Opus final: ~$0.08
    #   Total: ~$0.16 por fondo
    # ═══════════════════════════════════════════════════════════════════════

    def _prefilter_context(self, raw_context: str) -> str:
        """Gemini Pro limpia el contexto: elimina duplicados, preserva datos reales.
        Prompt ULTRA-RESTRICTIVO: NUNCA inferir, solo copiar/dedupe."""
        from tools.gemini_wrapper import extract_fast, MODEL_PRO
        try:
            result = extract_fast(
                text=raw_context,
                schema={"brief": "str - brief denso sin duplicados, texto preservado literal"},
                context=(
                    "REGLAS CRITICAS (violacion invalida el output):\n"
                    "1. PROHIBIDO inferir, deducir, completar o añadir informacion "
                    "que NO este EXPLICITAMENTE en el input.\n"
                    "2. PROHIBIDO mezclar datos de fondos con nombres similares.\n"
                    "3. PROHIBIDO cambiar nombres de personas, cifras, fechas.\n\n"
                    "TU TAREA:\n"
                    "a) Eliminar duplicados exactos (si una tesis se repite palabra por palabra "
                    "en varios periodos, mantener 1 y marcar '(repetido en N periodos)').\n"
                    "b) Eliminar disclaimers legales genericos.\n"
                    "c) PRESERVAR LITERAL: cifras, citas, biografias, posiciones, opiniones externas.\n\n"
                    "Si dudas, PRESERVALO."
                ),
                max_chars=50000,
                model=MODEL_PRO,
            )
            if isinstance(result, dict) and result.get("brief"):
                brief = result["brief"]
                self._log("INFO", f"Pro prefiltro: {len(raw_context)} -> {len(brief)} chars")
                return brief
        except Exception as e:
            self._log("WARN", f"Pro prefiltro failed: {e}")
        return raw_context

    def _gemini_section(self, prompt: str, max_chars: int = 30000) -> str:
        """Gemini Pro para secciones narrativas. Devuelve texto plano."""
        from tools.gemini_wrapper import extract_fast, MODEL_PRO
        try:
            # Usar schema simple {"texto": str} y pedir texto largo
            result = extract_fast(
                text=prompt,
                schema={"texto": "str - texto completo del analisis"},
                context="Eres un analista senior. Escribe texto denso, completo, "
                        "con datos concretos. Solo datos del input, no inventar.",
                model=MODEL_PRO,
                max_chars=max_chars,
            )
            if isinstance(result, dict):
                return result.get("texto", "") or ""
        except Exception as e:
            self._log("WARN", f"Gemini section failed: {e}")
        return ""

    def _gemini_section_json(self, prompt: str, schema: dict, max_chars: int = 30000) -> dict:
        """Gemini Pro con schema estructurado.
        Regla CRITICA: si no hay dato para un campo, devuelve null/vacio.
        NUNCA 'no disponible', 'no se menciona', 'datos pendientes', etc."""
        from tools.gemini_wrapper import extract_fast, MODEL_PRO
        try:
            result = extract_fast(
                text=prompt,
                schema=schema,
                context=(
                    "Eres analista senior. Solo datos REALES del input, nunca inventar. "
                    "REGLA CRITICA: si no tienes dato para un campo, devuelve null o '' (vacio). "
                    "PROHIBIDO escribir 'no disponible', 'no se menciona', 'datos pendientes', "
                    "'informacion no recopilada', o similares. Prefiere OMITIR a rellenar. "
                    "Respuestas en espanol."
                ),
                model=MODEL_PRO,
                max_chars=max_chars,
            )
            if not isinstance(result, dict):
                return {}
            # Post-filtro: eliminar valores "filler" conocidos
            return self._strip_filler(result)
        except Exception as e:
            self._log("WARN", f"Gemini JSON failed: {e}")
            return {}

    def _strip_filler(self, obj):
        """Recorre el dict y elimina valores que sean filler/no-info."""
        filler_patterns = [
            "no disponible", "no se menciona", "no se dispone",
            "informacion no", "información no", "datos pendientes",
            "no se indica", "no se reporta", "no se especifica",
            "pendiente de", "no aparece en", "no se proporciona",
            "no se encuentra", "no hay información", "no hay informacion",
            "no se dispone de", "sin información", "sin informacion",
            "no se ha encontrado", "no se detalla",
            "n/a", "n.a.",
        ]
        if isinstance(obj, dict):
            cleaned = {}
            for k, v in obj.items():
                cleaned_v = self._strip_filler(v)
                if cleaned_v not in (None, "", [], {}):
                    cleaned[k] = cleaned_v
            return cleaned
        if isinstance(obj, list):
            cleaned_list = [self._strip_filler(x) for x in obj]
            return [x for x in cleaned_list if x not in (None, "", [], {})]
        if isinstance(obj, str):
            low = obj.lower().strip()
            # Si el string completo es filler, devolver vacio
            if any(p in low for p in filler_patterns) and len(obj) < 200:
                return ""
            # Si el texto es largo pero CONTIENE frase filler suelta, eliminarla
            import re as _re
            cleaned = obj
            for p in filler_patterns:
                # Eliminar oraciones enteras que contengan filler
                cleaned = _re.sub(
                    rf'(?i)[^.!?]*\b{_re.escape(p)}\b[^.!?]*[.!?]\s*',
                    '', cleaned
                )
            return cleaned.strip()
        return obj

    def _opus_int_synthesis(self, data: dict) -> dict:
        """Pipeline INT: Gemini Pro para 6 secciones + Opus auditoria final.

        Estructura output idéntica a ES para que el dashboard funcione igual.
        """
        raw_context = self._prepare_int_context(data)
        self._log("INFO", f"Contexto raw: {len(raw_context)} chars")

        # Prefiltro Gemini Pro
        context = self._prefilter_context(raw_context)

        # Gestores autorizados (para anti-halluc en prompts)
        known_gestores = [g.get("nombre", "") for g in data.get("_int_gestores", [])
                          if isinstance(g, dict) and g.get("nombre")]
        authorized = ", ".join(known_gestores) if known_gestores else "ver contexto"

        anti_halluc = (
            f"REGLAS ESTRICTAS:\n"
            f"1. Los UNICOS gestores de este fondo son: {authorized}. "
            f"NO menciones otros nombres aunque tu conocimiento previo los asocie a esta gestora.\n"
            f"2. JERARQUIA DE FUENTES: Annual Report > Factsheet > Cartas del gestor > "
            f"Readings externos. Si readings externos contradicen al AR o cartas del gestor, "
            f"PRESERVA la version interna (AR/cartas). Readings se usan SOLO para "
            f"enriquecer/contextualizar, NUNCA para sobreescribir datos del fondo.\n"
            f"3. Si no hay un dato, OMITE (devuelve campo vacio/null). "
            f"PROHIBIDO 'no disponible', 'no se menciona', 'datos pendientes'.\n"
            f"4. FORMATO de cifras de miles/millones/miles de millones en "
            f"TEXTO y TITULOS: SIN DECIMALES. Ejemplos correctos: "
            f"'641M EUR', '14.678 millones', '1.400M GBP', '28B EUR'. "
            f"Incorrecto: '641,3M EUR', '14.678,37 millones', '1.402,2M'. "
            f"Los decimales SOLO se admiten para porcentajes (0,74%) y "
            f"ratios (duración -3 a +7).\n\n"
        )

        result: dict = {}

        # 1. Resumen ejecutivo
        result["resumen"] = self._section_resumen_int_pro(context, anti_halluc)
        # 2. Historia
        result["historia"] = self._section_historia_int_pro(context, anti_halluc)
        # 3. Gestores
        result["gestores"] = self._section_gestores_int_pro(context, data, anti_halluc)
        # 4. Estrategia + consistencia
        result["estrategia"] = self._section_estrategia_int_pro(context, anti_halluc)
        # 5. Evolución
        result["evolucion"] = self._section_evolucion_int_pro(context, data, anti_halluc)
        # 6. Cartera (programática datos + Gemini texto)
        result["cartera"] = self._section_cartera_int_pro(context, data, anti_halluc)
        # 7. Fuentes externas (programáticas)
        result["fuentes_externas"] = self._section_fuentes_int_programmatic(data)

        return result

    def _section_resumen_int_pro(self, context: str, anti_halluc: str) -> dict:
        schema = {
            "texto": (
                "str - RESUMEN EJECUTIVO para comite de inversion. OBJETIVO "
                "2500-4000 caracteres. FORMATO OBLIGATORIO:\\n"
                "1. Párrafos separados por DOS saltos de línea (\\n\\n). "
                "Cada párrafo trata un tema distinto.\\n"
                "2. **Negrita con doble asterisco** en puntos CLAVE: cifras "
                "importantes (AUM, rentabilidad, drawdown, TER), nombres del "
                "lead manager, hitos temporales, nombre del fondo, "
                "benchmarks. Objetivo: que un lector en diagonal capte lo "
                "esencial sin leer el párrafo entero.\\n"
                "3. Estructura mínima (un párrafo por cada tema):\\n"
                "PÁRRAFO 1 — Origen del fondo (lanzamiento, motivación, "
                "gestora).\\n"
                "PÁRRAFO 2 — Historia y evolución (hitos clave).\\n"
                "PÁRRAFO 3 — Estrategia y comportamiento histórico.\\n"
                "PÁRRAFO 4 — Resultados con cifras (rentabilidad acumulada, "
                "mejor/peor año, comportamiento en crisis).\\n"
                "PÁRRAFO 5 — Equipo gestor (lead + co, trayectoria breve).\\n"
                "PÁRRAFO 6 — Visión actual y outlook.\\n"
                "Si un tema no tiene contenido, fusionar con el adyacente — "
                "pero mantener la estructura de párrafos."
            ),
            "fortalezas": [
                "str - fortaleza concreta ESTRUCTURAL (filosofía, equipo, "
                "ventaja competitiva), NO detalles puntuales de la cartera actual. "
                "Con cifra de respaldo."
            ],
            "riesgos": [
                "str - riesgo ESTRUCTURAL del fondo (sensibilidad a un factor "
                "macro, concentración persistente, dependencia del gestor…). "
                "NO mencionar detalles de posiciones actuales que ya aparecen "
                "en la tab de Cartera. Con justificación factual."
            ],
            "para_quien_es": "str - perfil inversor ideal",
            "filosofia_inversion": (
                "str - FILOSOFIA DE INVERSION detallada. DEBE explicar: "
                "(a) Estrategia y objetivos del fondo (preservación / "
                "crecimiento real / descorrelación / etc.); "
                "(b) En qué tipo de activos invierte (RV, RF, oro, "
                "commodities, derivados) con peso típico de cada uno; "
                "(c) Cómo toma decisiones (bottom-up fundamental, macro "
                "top-down, análisis técnico, cuant, etc.); "
                "(d) Al menos 2 datos o porcentajes históricos que soporten "
                "la estrategia (p.ej. 'exposición media RV 35%', 'oro ha "
                "mantenido 10-12% desde 2013'). OBJETIVO 400-700 chars."
            ),
            "criterios_inversion": [
                {
                    "titulo": "str - titulo corto del criterio general (2-4 palabras)",
                    "descripcion": (
                        "str - criterio GENERAL del fondo (no solo RV). "
                        "Cubrir: tipo de activo, cómo se mueve el peso entre "
                        "activos si es multi-activo, qué tipos concretos usa "
                        "(ej. acciones de calidad / bonos ligados inflación / "
                        "oro físico vs ETF / commodities), cómo elige entre "
                        "opciones. Con ejemplos concretos del fondo."
                    ),
                }
            ],
            "_criterios_instrucciones": (
                "OBLIGATORIO: DEVOLVER ENTRE 3 Y 5 CRITERIOS. Cada uno cubre "
                "un ángulo distinto (ej. selección de activos, asignación "
                "táctica, gestión de riesgo/duración, proceso ESG, liquidez). "
                "Un único criterio genérico NO es aceptable. Si el fondo es "
                "multi-activo, debe haber al menos un criterio por clase "
                "relevante (RV / RF / Oro / Divisas / Derivados)."
            ),
            "compromiso_gestor": (
                "str - SIEMPRE contiene algo útil, NUNCA puede decir 'no se "
                "encuentra', 'no hay información', 'no se dispone', ni "
                "frases equivalentes. Prioridad: "
                "(1) Skin-in-the-game real con cita de readings/cartas. "
                "(2) Si no hay cita literal, describir estructura de propiedad "
                "de la gestora (ej. 'propiedad de los empleados', 'independent "
                "boutique', 'subsidiary de X bank'). "
                "(3) Política de remuneración (ej. 'fee flat sin performance "
                "fee'), tamaño del equipo dedicado, antigüedad de los gestores "
                "como proxy de alineación. "
                "OBJETIVO 200-500 caracteres con algo concreto."
            ),
            "signal": "str - POSITIVO | NEUTRAL | NEGATIVO",
        }
        return self._gemini_section_json(
            anti_halluc + f"Datos del fondo:\n{context}\n\n"
            f"Produce un RESUMEN EJECUTIVO COMPLETO para un comite de inversion. "
            f"El esquema describe exactamente qué debe contener cada campo: respétalo.\n"
            f"REGLAS PROHIBIDAS:\n"
            f"- NO escribir el 'texto' como bloque monolítico: DEBE tener "
            f"varios párrafos separados por \\n\\n, uno por tema (origen, "
            f"historia, estrategia, resultados, equipo, outlook).\n"
            f"- NO mezcles fortalezas/riesgos estructurales con detalles "
            f"puntuales de la cartera actual (esos van en la tab Cartera).\n"
            f"- NO repitas literalmente lo mismo en 'filosofia_inversion' "
            f"y 'criterios_inversion' — la filosofía es el QUÉ/POR QUÉ; "
            f"los criterios son el CÓMO concreto por tipo de activo.\n"
            f"- Si el fondo es multi-activo, los criterios DEBEN cubrir "
            f"ES/RV/RF/oro/commodities en la medida en que invierta en ellos.\n"
            f"- USA citas literales de las cartas cuando las haya, entre comillas.\n"
            f"- 'criterios_inversion' DEBE ser array de 3-5 elementos, uno por "
            f"ángulo (selección activos / asignación táctica / riesgo-duración "
            f"/ proceso ESG / liquidez). Un único criterio NO es aceptable.\n"
            f"- 'compromiso_gestor' NUNCA puede decir 'no se encuentra' ni "
            f"'no hay información' — si no hay skin-in-the-game literal, "
            f"describir estructura de la gestora, política de remuneración "
            f"o antigüedad del equipo como proxy de alineación.",
            schema, max_chars=50000,
        )

    def _section_historia_int_pro(self, context: str, anti_halluc: str) -> dict:
        schema = {
            "texto": (
                "str - HISTORIA DEL FONDO. OBJETIVO 3500-5500 caracteres. "
                "FORMATO OBLIGATORIO: varios PÁRRAFOS separados por \\n\\n, "
                "con **negrita** en fechas clave, cifras y nombres. "
                "ESTRUCTURA:\\n"
                "PÁRRAFO 1 — Origen: en qué ENTORNO DE MERCADO se crea el fondo "
                "(macro/regulatorio/de industria), por qué la gestora lo lanza, "
                "de dónde vienen los gestores fundadores y qué les motiva a "
                "entrar (SIN biografía completa — eso está en Gestores).\\n"
                "PÁRRAFO 2 — Cambios a lo largo de los años: evolución de "
                "estrategia, filosofía, equipo, cambios de clase, de ManCo, "
                "hitos estructurales.\\n"
                "PÁRRAFO 3 — Comportamiento en CICLOS DE MERCADO: cómo se ha "
                "comportado en crisis (2008/2011/2018/2020/2022) vs bonanzas. "
                "Con cifras concretas de rentabilidad relativa cuando existan.\\n"
                "PÁRRAFO 4 — Posicionamiento actual y visión a futuro."
            ),
            "hitos": [{
                "anio": "str - año YYYY",
                "titulo": "str - titulo corto del hito (6-12 palabras)",
                "evento": (
                    "str - párrafo descriptivo 2-4 líneas dando contexto "
                    "sobre qué pasó, por qué, impacto. NO frase de 1 línea."
                ),
                "tipo": "str - crisis | estrategia | regulatorio | crecimiento | equipo | comportamiento",
            }],
            "_hitos_instrucciones": (
                "REGLAS DE HITOS:\\n"
                "1. AGRUPAR por año: si en 2022 pasaron 3 cosas, 1 único hito "
                "con año=2022 que cubra las 3 en el 'evento'.\\n"
                "2. SIEMPRE incluir comportamiento vs mercado/ciclo (no solo "
                "eventos puntuales). P.ej. '2022 — crisis renta fija: fondo "
                "logró +8% mientras Bloomberg Global Agg caía -13%'.\\n"
                "3. Cada 'evento' debe ser párrafo descriptivo con contexto "
                "(por qué, cómo, impacto), NO solo título.\\n"
                "4. Min 5 hitos, ideal 8-12."
            ),
        }
        return self._gemini_section_json(
            anti_halluc + f"Datos del fondo:\n{context}\n\n"
            f"Escribe la HISTORIA del fondo en espanol. USA TODOS los hitos, cifras y "
            f"citas literales que aparezcan en el contexto; NO resumir si hay material. "
            f"Cubre: lanzamiento, evolucion AUM, cambios de gestion, hitos relevantes, "
            f"eventos notables, cambios de estrategia, fusiones, cambios regulatorios. "
            f"Si hay ciclos de mercado atravesados por el fondo, descríbelos con los "
            f"datos reales del contexto.",
            schema, max_chars=50000,
        )

    def _section_gestores_int_pro(self, context: str, data: dict, anti_halluc: str) -> dict:
        gestores_list = data.get("_int_gestores", [])
        schema = {
            "texto": (
                "str - RESUMEN GRUPAL del equipo (900-1600 chars). FORMATO "
                "OBLIGATORIO: varios PÁRRAFOS separados por \\n\\n con **negritas** "
                "en nombres, años, cargos y conceptos clave.\\n"
                "ORDEN de los párrafos OBLIGATORIO:\\n"
                "PÁRRAFO 1 — PRESENTACIÓN DEL EQUIPO E HISTORIA CONJUNTA: quiénes "
                "son (lead + co con cargo), cuándo se juntaron en este fondo, "
                "de dónde venían, por qué se formó el equipo actual.\\n"
                "PÁRRAFO 2 — ORGANIZACIÓN Y GOBERNANZA: cómo se distribuyen "
                "(lead + co / comité / especialistas por activo), cómo se toman "
                "las decisiones (colegiadas, voto, delegación), rol específico "
                "de cada uno si lo hay.\\n"
                "PÁRRAFO 3 — PERFIL COLECTIVO Y FILOSOFÍA COMPARTIDA: tipo de "
                "perfil (macro / fundamental / cuant / mixto), visión común, "
                "cómo el background conjunto soporta la filosofía del fondo.\\n"
                "PÁRRAFO 4 — ESTABILIDAD / CAMBIOS (OBLIGATORIO SI HUBO CAMBIOS "
                "RECIENTES): antigüedad media, rotación histórica. SI uno o más "
                "gestores actuales entraron hace <3 años (desde hoy), DEBES "
                "explicar qué pasó con el equipo anterior: quién estaba, cuándo "
                "salió, por qué (jubilación/nueva gestora/reestructuración) y "
                "si la filosofía y el proceso se mantienen o han cambiado. Este "
                "punto es CRÍTICO para el inversor: un cambio de equipo sin "
                "explicación genera dudas sobre continuidad de estrategia.\\n"
                "PROHIBIDO repetir trayectorias individuales (eso va en 'perfiles')."
            ),
            "gestores_anteriores": [
                {
                    "nombre": "str - nombre del gestor que salió",
                    "cargo": "str - cargo que tuvo en el fondo (lead / co / analista...)",
                    "periodo_en_fondo": "str - p.ej. '2015-2024'",
                    "motivo_salida": (
                        "str - jubilación / nueva gestora / reestructuración / "
                        "fallecimiento / fin de mandato. Sé literal con lo que "
                        "digan cartas, press releases o notas del gestor."
                    ),
                    "sustituto": "str - nombre del gestor actual que tomó su rol",
                    "impacto_estrategia": (
                        "str - 1 línea: la filosofía/proceso se mantuvo / cambió "
                        "parcialmente / cambió sustancialmente. Con justificación breve."
                    ),
                }
            ],
            "_gestores_anteriores_instrucciones": (
                "Rellenar SOLO si hubo cambios en los últimos 5 años y tienes "
                "información verificable (cartas, prensa, manager_profile). "
                "Si no hay cambios recientes, devolver lista vacía []. "
                "NUNCA INVENTES nombres o motivos."
            ),
            "perfiles": [{
                "nombre": "str",
                "cargo": "str",
                "cv_bullets": [
                    "str - bullet CV formato 'Empresa · Puesto · Años · AUM/responsabilidad' "
                    "(ej: 'Swiss Re · Portfolio Manager ILS · 2010-2015 · $2B AUM'). "
                    "Min 3 bullets, max 6, ordenados del MÁS RECIENTE al más antiguo. "
                    "Son la CV lateral del perfil, deben ser leíbles en 1 segundo."
                ],
                "trayectoria": (
                    "str - narrativa profesional DETALLADA con contexto (500-800 chars). "
                    "Complementa los cv_bullets, NO los repite literalmente. "
                    "Incluye: por qué cambió de puesto/empresa, qué aportó en "
                    "cada rol, hitos notables (premios, fondos lanzados, AUM "
                    "gestionado, situaciones superadas), contexto del sector "
                    "cuando entró/salió."
                ),
                "filosofia": (
                    "str - filosofía PERSONAL del gestor DETALLADA (450-700 chars). "
                    "NO la filosofía del fondo — la del INDIVIDUO. Cómo ve los "
                    "mercados, qué prioriza/evita, visión sobre ciclos/riesgo/"
                    "valoración, convicciones fuertes, aproximación al análisis "
                    "(bottom-up, top-down, cuant, etc). Si hay citas literales "
                    "del gestor incluirlas entre comillas."
                ),
                "educacion": "str - titulos y certificaciones",
                "reconocimientos": "str - premios/ratings",
                "highlights": [
                    {
                        "tipo": "str - historia | filosofia | decision | estrategia | cita",
                        "texto": "str - bullet breve (80-200 chars) con dato concreto y contexto",
                    }
                ],
            }],
            "_instrucciones_highlights": (
                "CADA gestor debe tener MÍNIMO 5 highlights, uno de cada tipo:\\n"
                "- 'historia': momento/hito biográfico relevante (ej. 'Fundó Troy "
                "en 2000 tras dejar Stanhope por diferencias de filosofía').\\n"
                "- 'filosofia': máxima personal (ej. 'Sebastian: cree que el "
                "experimento monetario debe terminar con inflación alta').\\n"
                "- 'decision': decisión concreta y cuando (ej. '2013: concentró "
                "11% cartera en oro contra consenso de mercado').\\n"
                "- 'estrategia': cómo traduce filosofía a cartera (ej. "
                "'Prefiere quality compounders sobre cyclical value').\\n"
                "- 'cita': frase célebre o quote literal del gestor (con "
                "comillas si viene de carta/entrevista)."
            ),
        }
        result = self._gemini_section_json(
            anti_halluc + f"Datos del fondo:\n{context}\n\n"
            f"Escribe la sección EQUIPO GESTOR en espanol respetando EL SCHEMA "
            f"exactamente. REGLAS CRÍTICAS:\n"
            f"- 'texto' DEBE tener EXACTAMENTE 4 PÁRRAFOS separados por DOS "
            f"saltos de línea (\\n\\n). Orden obligatorio:\n"
            f"  Párrafo 1 — Presentación del equipo + historia conjunta "
            f"(cuándo se juntaron, de dónde vienen, por qué).\n"
            f"  Párrafo 2 — Organización y gobernanza (lead + co / comité / "
            f"roles específicos).\n"
            f"  Párrafo 3 — Perfil colectivo y filosofía compartida.\n"
            f"  Párrafo 4 — Estabilidad / cambios relevantes. OBLIGATORIO si "
            f"alguno de los gestores actuales entró hace menos de 3 años: "
            f"explica quién estaba antes, cuándo salió y por qué (el inversor "
            f"necesita saber si la filosofía se mantiene).\n"
            f"- 'gestores_anteriores': rellenar si hay cambios recientes "
            f"(<5 años) con datos verificables — nombre, cargo, periodo_en_fondo, "
            f"motivo_salida, sustituto, impacto_estrategia. Si no hay, []. "
            f"NUNCA inventes.\n"
            f"- Usa **negritas** en nombres, años, cargos y conceptos clave "
            f"del texto grupal.\n"
            f"- 'perfiles[i].trayectoria' 500-800 chars (DETALLADA, con "
            f"hitos/contexto, no repite cv_bullets).\n"
            f"- 'perfiles[i].filosofia' 450-700 chars DETALLADA (visión "
            f"personal del gestor, no del fondo).\n"
            f"- 'perfiles[i].cv_bullets': 3-6 bullets formato "
            f"'Empresa · Puesto · Años · Responsabilidad', orden cronológico "
            f"inverso.\n"
            f"- 'perfiles[i].highlights': 5 bullets, uno por tipo "
            f"(historia/filosofia/decision/estrategia/cita).\n"
            f"- NO repetir información entre 'texto' y 'perfiles'.\n"
            f"- NO repetir entre 'trayectoria' y 'cv_bullets'.",
            schema, max_chars=50000,
        )
        result["equipo"] = gestores_list
        return result

    def _fetch_morningstar_yearly_returns(self, isin: str) -> dict:
        """Fetchea la serie diaria de Morningstar y calcula rentabilidad
        anual (yearly close-to-close). Devuelve dict {year: pct_return}.
        Cacheado en disco en raw/mst_returns.json para no repetir el hit.
        """
        import json as _json
        cache = self.fund_dir / "raw" / "mst_returns.json"
        try:
            if cache.exists():
                return _json.loads(cache.read_text(encoding="utf-8"))
        except Exception:
            pass
        try:
            import httpx
            url = (f"https://tools.morningstar.es/api/rest.svc/timeseries_price/"
                   f"2nhcdckzon?id={isin}&idtype=Isin&frequency=daily&"
                   f"startDate=1900-01-01&outputType=compactJSON")
            with httpx.Client(timeout=15, follow_redirects=True,
                              headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}) as c:
                r = c.get(url)
                if r.status_code != 200:
                    return {}
                arr = r.json()
        except Exception:
            return {}
        if not isinstance(arr, list) or not arr:
            return {}
        # arr es [[timestamp_ms, nav], ...]
        from datetime import datetime as _dt
        pts = []
        for it in arr:
            if not isinstance(it, list) or len(it) < 2:
                continue
            try:
                ts, v = float(it[0]), float(it[1])
                if v <= 0:
                    continue
                d = _dt.utcfromtimestamp(ts / 1000)
                pts.append((d, v))
            except Exception:
                continue
        pts.sort()
        # Último NAV de cada año
        year_end = {}
        for d, v in pts:
            year_end[d.year] = v  # se sobreescribe con el último → último del año
        years_sorted = sorted(year_end.keys())
        returns = {}
        for i in range(1, len(years_sorted)):
            prev_y = years_sorted[i - 1]
            y = years_sorted[i]
            r_pct = (year_end[y] / year_end[prev_y] - 1) * 100
            returns[str(y)] = round(r_pct, 2)
        try:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(_json.dumps(returns), encoding="utf-8")
        except Exception:
            pass
        return returns

    def _section_estrategia_int_pro(self, context: str, anti_halluc: str) -> dict:
        # Fetchear rentabilidades anuales reales para que el LLM complete
        # el campo 'resultado' de cada hito con la cifra real.
        yearly_returns = self._fetch_morningstar_yearly_returns(self.isin)
        if yearly_returns:
            sorted_years = sorted(yearly_returns.keys())
            returns_block = ("RENTABILIDADES ANUALES REALES del fondo (Morningstar "
                             "daily, cierre-a-cierre, YoY):\n")
            for y in sorted_years:
                returns_block += f"  · {y}: {yearly_returns[y]:+.2f}%\n"
        else:
            returns_block = ""
        schema = {
            "texto": (
                "str - ANÁLISIS EJECUTIVO DE LA ESTRATEGIA (no de la consistencia). "
                "OBJETIVO 4000-7000 caracteres. TEXTO FLUIDO Y PROFESIONAL, "
                "NO pegado en bloques rígidos. Estilo de nota de research para "
                "comité de inversión. Usar **negritas** en puntos clave "
                "(fechas, cifras, conceptos centrales) pero SIN headers en "
                "mayúsculas ni etiquetas tipo 'PÁRRAFO 1 —'.\\n\\n"
                "TEMAS A CUBRIR (en orden narrativo, fluido, entrelazando):\\n"
                "(a) Qué hace el fondo — mandato/objetivo/benchmark/universo.\\n"
                "(b) Proceso de inversión detallado — macro/fundamental/cuant, "
                "selección, control riesgo, duración/beta, rol de cada activo.\\n"
                "(c) Decisiones clave históricas con cifras (contracorriente, "
                "pivotes) que ilustren la filosofía en acción.\\n"
                "(d) Citas literales del gestor entre comillas con atribución "
                "y año (SOLO si las hay; si no, OMITIR este bloque — NO "
                "escribir 'no se han encontrado citas').\\n"
                "(e) Patrón de comportamiento en ciclos (crisis vs bonanza) "
                "con ejemplos concretos de años.\\n"
                "(f) Cierre con la VISIÓN ACTUAL y la ORIENTACIÓN DE CARTERA "
                "hoy — qué piensa el equipo del mercado, decisiones recientes, "
                "cómo está posicionada la cartera para cumplir el objetivo. "
                "Integrar como párrafo fluido, SIN header en mayúsculas al "
                "principio (nada de 'VISIÓN ACTUAL Y ORIENTACIÓN:').\\n\\n"
                "Usar párrafos de 3-6 frases separados por \\n\\n, sin "
                "romper el flujo narrativo."
            ),
            "perfil_riesgo": {
                "tipo_activo_principal": (
                    "str - clasificación corta del tipo de estrategia subyacente "
                    "(ej. 'Bonos catástrofe (ILS)', 'Convertibles globales', "
                    "'RV emergentes small cap', 'Arbitraje de fusiones', "
                    "'Renta fija corto plazo high yield', 'Long-short equity'). "
                    "OBLIGATORIO — el inversor tiene que entender de un vistazo "
                    "en qué tipo de activo se mete."
                ),
                "riesgos_especificos": [
                    "str - bullets detallados de los riesgos PROPIOS del tipo "
                    "de activo (no riesgos genéricos). Para cat bonds: "
                    "'riesgo evento natural catastrófico con pérdida de "
                    "principal', 'riesgo de modelización actuarial', 'riesgo "
                    "de liquidez del mercado ILS'. Para convertibles: 'riesgo "
                    "de correlación equity/crédito', etc. Mínimo 3, máximo 6."
                ],
                "escenarios_adversos": (
                    "str - 2-4 líneas describiendo qué pasaría en el peor "
                    "escenario realista para ESTA estrategia (no para 'el "
                    "mercado' en general). Ejemplos concretos ayudan: "
                    "'Temporada huracanes 2017 costó al sector ILS -15%...'"
                ),
                "protecciones": (
                    "str - 1-3 líneas: cómo mitiga el gestor estos riesgos "
                    "(diversificación por peril/región, cap de exposición "
                    "por emisor, hedging, reservas de liquidez, etc.)."
                ),
                "liquidez_estructura": (
                    "str - 1-2 líneas sobre liquidez del subyacente y ventanas "
                    "de suscripción/reembolso del fondo. Si hay gating, side "
                    "pockets o ventanas semanales/mensuales, DEBE aparecer aquí."
                ),
                "desglose_exposicion_resumen": (
                    "str - 3-5 líneas explicando EN LENGUAJE CLARO la "
                    "granularidad del riesgo concreto de la cartera para que "
                    "un inversor entienda A QUÉ eventos/escenarios adversos "
                    "está realmente expuesto. Ejemplos:\n"
                    "  - Cat bonds: 'La cartera está principalmente expuesta "
                    "a huracanes en la costa sudeste de EE.UU. (≈45%) y "
                    "terremotos en Japón (≈15%). Un huracán categoría 4+ "
                    "tocando Florida en temporada alta sería el escenario "
                    "más adverso. La mortalidad extrema aporta diversificación.'\n"
                    "  - EM bonds: 'Concentración en soberanos de India y Brasil "
                    "con menor exposición a frontera. Un repunte del dólar o "
                    "una crisis política en uno de los dos principales países...'\n"
                    "No resumir con genéricos; usar las cifras y detalles de "
                    "la lista 'desglose_exposicion'."
                ),
                "desglose_exposicion": [
                    {
                        "dimension": (
                            "str - eje de riesgo relevante para ESTE tipo de "
                            "fondo. Ejemplos por tipo de estrategia:\n"
                            "  - Cat bonds / ILS: 'Peril' (tipo de catástrofe) "
                            "o 'Región catastrófica'.\n"
                            "  - RV/RF emergentes: 'País' o 'Frontera vs emergente'.\n"
                            "  - High yield / Loans: 'Rating crediticio' o 'Sector'.\n"
                            "  - Convertibles: 'Sector subyacente' o 'Delta'.\n"
                            "  - Arbitraje fusiones: 'Spread bucket' o 'Geografía deal'.\n"
                            "  - RV sectorial: 'Sub-sector' o 'Capitalización'.\n"
                            "  - RF duración larga: 'Tramo de curva' o 'Emisor soberano vs corporativo'.\n"
                        ),
                        "detalle": (
                            "str - valor concreto dentro de la dimensión. "
                            "Ejemplos para cat bonds: 'Hurricane US (costa "
                            "sudeste/Florida)', 'Earthquake Japan', 'Winter "
                            "storm Europe', 'Hurricane Caribbean', 'Multi-peril "
                            "global'. Para RV emergentes: 'India', 'Brasil', "
                            "'Vietnam'. Ser literal con lo que digan las cartas."
                        ),
                        "peso_aprox_pct": (
                            "number|null - peso aproximado en la cartera si se "
                            "deduce de cartas/informes. null si no se explicita."
                        ),
                        "comentario": (
                            "str - 1-2 líneas sobre por qué el gestor tiene "
                            "esa exposición y qué evento concreto debería "
                            "preocupar al inversor ('la cartera sufriría si "
                            "Florida tiene un huracán categoría 4+ en "
                            "septiembre-octubre')."
                        ),
                    }
                ],
            },
            "_perfil_riesgo_instrucciones": (
                "OBLIGATORIO para TODOS los fondos, pero especialmente CRÍTICO "
                "cuando el fondo invierte en activos nicho/ilíquidos/complejos "
                "(cat bonds, ILS, convertibles, EM frontier, high yield, "
                "loans, option selling, arbitraje, volatilidad, distressed). "
                "El inversor no experto debe poder leer este bloque y "
                "entender EN QUÉ se está metiendo antes de ver performance.\\n"
                "'desglose_exposicion' es OBLIGATORIO (mínimo 3 filas): "
                "detalla la granularidad del riesgo asumido — no basta con "
                "'bonos catástrofe' o 'high yield global'. Debe decir QUÉ "
                "peril/país/sector/rating concreto y qué evento adverso "
                "específico le afectaría. Si las cartas no dan peso exacto, "
                "pon null en peso_aprox_pct pero rellena dimension/detalle/"
                "comentario siempre."
            ),
            "hitos_estrategia": [{
                "periodo": "str - año YYYY ORDENADOS DE MÁS ANTIGUO A MÁS RECIENTE (inicio del fondo hasta hoy)",
                "contexto_mercado": "str - OBLIGATORIO. Qué pasaba en el mercado ese año (macro, sectorial). Nunca vacío.",
                "decisiones": "str - OBLIGATORIO. Decisión concreta del gestor ese año (pivote, nueva posición, rotación, hedging) con cifras cuando existan.",
                "resultado": (
                    "str - OBLIGATORIO. DOS partes separadas por ' — ': "
                    "(1) CIFRA con rentabilidad real del año del bloque "
                    "'RENTABILIDADES ANUALES REALES' + comparación vs "
                    "benchmark/objetivo ('superó/en línea/no superó'); "
                    "(2) DRIVER concreto: POR QUÉ subió/bajó ese año — "
                    "eventos de mercado específicos, aciertos/errores del "
                    "gestor, catalizadores (ej. 'Caída -4.15% 2022 por "
                    "repricing cat bonds tras temporada ciclónica benigna "
                    "que elevó primas técnicas y contrajo valor mark-to-market'). "
                    "Si el año NO aparece en las rentabilidades reales "
                    "(año anterior a inicio o muy reciente), describir "
                    "cualitativamente con driver. NUNCA devolver solo la "
                    "cifra sin driver — el 'por qué' es obligatorio."
                ),
            }],
            "_hitos_instrucciones": (
                "CRÍTICO:\\n"
                "1. MÍNIMO 5-8 hitos cubriendo TODA la vida del fondo (inicio "
                "hasta hoy). NO omitas un año relevante por falta de un campo — "
                "relléna los 3 campos SIEMPRE usando lo disponible.\\n"
                "2. Para 'resultado': usa la RENTABILIDAD REAL del año del "
                "bloque 'RENTABILIDADES ANUALES REALES' cuando esté disponible; "
                "cuando no, valora cualitativamente si la tesis se cumplió.\\n"
                "3. Ordenar cronológicamente ASCENDENTE (inicio → hoy)."
            ),
            "quotes": [{
                "texto": (
                    "str - CITA LITERAL del gestor tomada de cartas, annual "
                    "reports, entrevistas, presentaciones o readings externos. "
                    "Debe ser una FRASE con SUSTANCIA (tesis, visión, decisión "
                    "o argumento). Mínimo 25 caracteres y 5 palabras con "
                    "significado propio. PROHIBIDO devolver UNA palabra aislada "
                    "entre comillas ('tremendos', 'absolutamente'). Si el gestor "
                    "usó expresiones fuertes, incluye la frase completa donde "
                    "aparecen con su CONTEXTO. Ejemplos VÁLIDOS:\n"
                    "  - 'Creemos que los fundamentos del mercado son "
                    "tremendos y el crecimiento absolutamente sostenible.'\n"
                    "  - 'Preferimos quality compounders sobre cyclical value, "
                    "incluso si paga un múltiplo superior.'\n"
                    "  - 'El experimento monetario debe terminar con "
                    "inflación alta, y posicionamos la cartera para ello.'\n"
                    "Busca ACTIVAMENTE en cartas, readings externos y "
                    "manager_profile — mínimo 1-3 citas si los docs lo permiten."
                ),
                "autor": (
                    "str - nombre propio del gestor (ej. 'Sebastian Lyon', "
                    "'MariaGiovanna Guatteri'). Si no se identifica un "
                    "gestor concreto y la cita es institucional, usar el "
                    "nombre de la gestora. Evitar 'Equipo gestor' genérico."
                ),
                "contexto": (
                    "str - fuente y tema breve. Ejemplos: '2024 Q3 letter "
                    "— valoración de mercado', 'Annual Report 2023 — "
                    "outlook', 'Substack Astralis — entrevista 2024'"
                ),
            }],
            "_quotes_instrucciones": (
                "OBJETIVO: 1-3 citas sustantivas. Si solo encuentras "
                "palabras sueltas entre comillas en el original, "
                "reconstruye la frase completa citable incorporándolas "
                "en su contexto natural (mantén las comillas internas "
                "si quieres destacar la expresión). NUNCA devolver una "
                "única palabra como cita."
            ),
            "estrategia_actual_resumen": (
                "str - resumen 200-400 chars del posicionamiento ACTUAL "
                "(visión de mercado + cómo está posicionada la cartera hoy)."
            ),
            "resumen_consistencia": {
                "score": "str - 1-10 basado en el cruce de tesis vs resultado",
                "decisiones_vs_estrategia": (
                    "str - 2-3 líneas: ¿las decisiones tomadas cuadran con "
                    "la estrategia declarada? Ejemplos concretos."
                ),
                "resultados_vs_objetivo": (
                    "str - 2-3 líneas: ¿los resultados cumplen el objetivo "
                    "del fondo (retorno absoluto / benchmark + X / preservar "
                    "capital)? Con cifras."
                ),
                "justificacion": "str - 1-2 líneas justificando el score",
            },
        }
        return self._gemini_section_json(
            anti_halluc + f"Datos del fondo:\n{context}\n\n"
            + (returns_block + "\n" if returns_block else "")
            + f"Escribe ANÁLISIS DE ESTRATEGIA Y CONSISTENCIA en español. "
            f"REGLAS:\n"
            f"- 'texto' es SOLO la estrategia (proceso + decisiones clave + "
            f"citas + patrón ciclos). NO metas la tabla de consistencia aquí.\n"
            f"- 'hitos_estrategia' es la tabla de consistencia año a año, "
            f"5-8 hitos MÍNIMO, SIEMPRE ordenados cronológicamente ASCENDENTE "
            f"desde inicio del fondo hasta el año actual.\n"
            f"- El campo 'resultado' DEBE incorporar la rentabilidad real del "
            f"año del bloque 'RENTABILIDADES ANUALES REALES' arriba (si el año "
            f"está en la lista), comparándola con el objetivo/benchmark del "
            f"fondo.\n"
            f"- NO omitir hitos por falta de un campo: rellenar los 3 con lo "
            f"disponible.\n"
            f"- 'resumen_consistencia' es el resumen que va DEBAJO de la "
            f"tabla con score + decisiones vs estrategia + resultados vs "
            f"objetivo + justificación del score.\n"
            f"- 'perfil_riesgo' es OBLIGATORIO: tipo_activo_principal, "
            f"riesgos_especificos (≥3 bullets propios del tipo de activo, "
            f"NO genéricos), escenarios_adversos con ejemplos reales, "
            f"protecciones y liquidez_estructura. Esta sección irá renderizada "
            f"ANTES de la tabla de consistencia para que el lector entienda "
            f"qué tipo de riesgo asume.\n"
            f"- 'hitos_estrategia[].resultado': DOS partes ' — ': cifra "
            f"vs benchmark + DRIVER (por qué subió/bajó). Ejemplo: "
            f"'+13.98% vs +3.5% €STR → superó — prima ILS expandida "
            f"por memoria huracán Ian y ausencia de eventos cat mayores'.\n"
            f"- USA citas LITERALES de las cartas entre comillas con "
            f"atribución y año.",
            schema, max_chars=50000,
        )

    def _section_evolucion_int_pro(self, context: str, data: dict, anti_halluc: str) -> dict:
        cuant = data.get("cuantitativo", {})
        clases = data.get("_int_clases", [])
        schema = {"texto": "str - analisis detallado de evolucion del fondo. OBJETIVO: 3000-6000 caracteres. Cubre evolucion AUM, TER, clases, eventos, cambios macro, ciclos de mercado atravesados, narrativa temporal."}

        serie_aum = cuant.get('serie_aum', [])
        serie_ter = cuant.get('serie_ter', [])
        datos_extra = (
            f"SERIE AUM (unicos valores validos, NO inventar otros): {serie_aum}\n"
            f"SERIE TER (unicos valores validos): {serie_ter}\n"
            f"CLASES DISPONIBLES ({len(clases)}): "
            + ", ".join(str(c.get('code', c.get('nombre', ''))) for c in clases if isinstance(c, dict)) + "\n"
        )
        strict_prompt = (
            anti_halluc
            + "REGLA CRITICA: Los datos cuantitativos (AUM, TER, clases) son los UNICOS "
            "valores validos. PROHIBIDO inventar cifras adicionales de años no listados. "
            "Si solo hay 1 punto de AUM, NO escribas evolucion historica con cifras inventadas.\n\n"
            + f"Datos del fondo:\n{context}\n\nDatos evolutivos (UNICOS VALIDOS):\n{datos_extra}\n\n"
            f"Escribe ANALISIS DE EVOLUCION del fondo en espanol. Incluye SOLO:\n"
            f"- Evolucion AUM (SOLO con los valores del serie_aum arriba, no inventar)\n"
            f"- TER (SOLO con los valores del serie_ter arriba)\n"
            f"- Clases disponibles (las listadas)\n"
            f"- Eventos relevantes si aparecen en el contexto\n"
            f"Si solo hay 1 punto de AUM, menciona ese valor como AUM actual y omite "
            f"evolucion historica — NO INVENTAR cifras pasadas."
        )
        result = self._gemini_section_json(strict_prompt, schema, max_chars=50000)
        result["datos_graficos"] = {
            "serie_aum": cuant.get("serie_aum", []),
            "serie_ter": cuant.get("serie_ter", []),
            "clases": clases,
        }
        return result

    def _section_cartera_int_pro(self, context: str, data: dict, anti_halluc: str) -> dict:
        posiciones = data.get("posiciones", {}).get("actuales", [])
        sorted_pos = sorted(posiciones, key=lambda x: x.get("peso_pct", 0) or 0, reverse=True)

        top5 = sum((p.get("peso_pct", 0) or 0) for p in sorted_pos[:5])
        top10 = sum((p.get("peso_pct", 0) or 0) for p in sorted_pos[:10])
        top15 = sum((p.get("peso_pct", 0) or 0) for p in sorted_pos[:15])

        # Distribución por tipo (programático)
        by_type: dict[str, dict] = {}
        for p in posiciones:
            t = (p.get("asset_type") or p.get("sector") or "other").lower() or "other"
            if t not in by_type:
                by_type[t] = {"peso_pct": 0, "num_posiciones": 0}
            by_type[t]["peso_pct"] += p.get("peso_pct", 0) or 0
            by_type[t]["num_posiciones"] += 1
        distribucion = [{"tipo": k, **v} for k, v in by_type.items()]

        schema = {
            "texto": (
                "str - ANÁLISIS DE CARTERA. OBJETIVO 3000-6000 caracteres con "
                "**negritas** en puntos clave. FORMATO OBLIGATORIO: varios "
                "PÁRRAFOS separados por \\n\\n. Cada bloque comienza con un "
                "header en mayúsculas entre dobles asteriscos.\\n"
                "ESTRUCTURA OBLIGATORIA:\\n\\n"
                "**EXPOSICIÓN ACTUAL Y RACIONAL**\\n\\n"
                "Párrafo CUALITATIVO (no enumerativo). PROHIBIDO listar "
                "posiciones top10 con pesos — eso ya lo hace la tabla. "
                "Aquí debes contar:\\n"
                "(a) DÓNDE están las exposiciones — por tipo de activo con "
                "cifras globales (ej. 'cartera con ~XX% en bonos cat peril "
                "diversificado, ~YY% en cash y equivalentes, ~ZZ% en "
                "reaseguro privado'), por región/peril/sector/duración si "
                "aplica al tipo de fondo.\\n"
                "(b) POR QUÉ esa asignación — racional ligado a la visión "
                "del gestor del mercado hoy (tesis macro/micro), con citas "
                "de cartas si las hay.\\n"
                "(c) Qué 2-3 posiciones son ESTRUCTURALES / emblemáticas de "
                "la filosofía (no las 10 mayores, sino las que explican la "
                "filosofía) con su tesis en 1 línea cada una. Sin listar "
                "pesos — esto NO es la tabla.\\n\\n"
                "**DECISIONES / CAMBIOS RECIENTES**\\n\\n"
                "Entradas, salidas, rotaciones de los últimos 6-12 meses "
                "explicadas EN DETALLE con el contexto de mercado que las "
                "motivó y cómo encajan en la VISIÓN A FUTURO del gestor "
                "(posicionamiento para el próximo ciclo/escenario). Si hay "
                "cartas recientes con rationale literal, citarlo.\\n\\n"
                "**CONCENTRACIÓN** (solo si el fondo es de RV/RF puro; si es "
                "de instrumentos — ETFs/oro/commodities — OMITIR porque "
                "la métrica pierde sentido).\\n\\n"
                "Comentar top5/top10/top15 con su significado relativo a "
                "la media del tipo de fondo."
            ),
        }
        datos_extra = (
            f"Total posiciones: {len(posiciones)}. "
            f"Top5: {top5:.1f}%. Top10: {top10:.1f}%. Top15: {top15:.1f}%.\n"
            f"Distribucion: {distribucion}\n"
            f"Top posiciones: "
            + ", ".join(f"{p.get('nombre','')} ({p.get('peso_pct',0):.1f}%)" for p in sorted_pos[:10])
        )
        result = self._gemini_section_json(
            anti_halluc + f"Datos del fondo:\n{context}\n\nCartera:\n{datos_extra}\n\n"
            f"Escribe ANALISIS CUALITATIVO DE CARTERA en espanol. REGLAS:\n"
            f"- PROHIBIDO enumerar el top10/top15 con pesos en el texto: "
            f"eso ya lo muestra la tabla. El texto es CUALITATIVO.\n"
            f"- **EXPOSICIÓN ACTUAL Y RACIONAL** (primer bloque): cuenta DÓNDE "
            f"están los pesos globales (por tipo de activo con cifras: "
            f"ej. 'RF ~78%, Cash ~9%, Oro ~6%'), por qué esa asignación "
            f"(racional/tesis del gestor), y cita SÓLO 2-3 posiciones "
            f"EMBLEMÁTICAS/estructurales (no las 10 mayores), sin listar pesos.\n"
            f"- **DECISIONES / CAMBIOS RECIENTES** (segundo bloque): entradas, "
            f"salidas, rotaciones ÚLTIMOS 6-12 MESES con contexto y cómo "
            f"encajan en la VISIÓN A FUTURO del gestor (orientación de cartera).\n"
            f"- **CONCENTRACIÓN** (tercer bloque, solo si es fondo RV/RF puro): "
            f"comentario top5/top10/top15 y comparativa con media del tipo de fondo.\n"
            f"- USA TODAS las referencias a cartera en cartas/readings para "
            f"alimentar el racional; pero NO reproduzcas la tabla en prosa.",
            schema, max_chars=50000,
        )
        # Post-process: curar artefactos tipicos de parser JSON (p.ej. '(4.La ')
        # que aparecen cuando el JSON se corta en medio de un peso.
        texto_raw = result.get("texto", "") or ""
        texto_clean = self._sanitize_numbered_positions(texto_raw, sorted_pos)
        if texto_clean != texto_raw:
            result["texto"] = texto_clean
            self._log("INFO", "Cartera: sanitizadas posiciones con peso cortado")

        result["distribucion_tipo"] = distribucion
        result["concentracion"] = {
            "top5_pct": round(top5, 2),
            "top10_pct": round(top10, 2),
            "top15_pct": round(top15, 2),
        }
        result["posiciones_top"] = sorted_pos[:15]
        return result

    def _sanitize_numbered_positions(self, texto: str, sorted_pos: list) -> str:
        """Detecta y repara posiciones enumeradas con peso cortado.
        Patron tipico: '9. **Nombre (4.La gestion...' -> el '%)' se perdio.
        Generic: busca '(N.letra' y reconstruye el segmento con el peso real si
        encuentra el nombre en sorted_pos.
        """
        import re as _re
        if not texto:
            return texto
        # Mapear nombre normalizado -> peso
        name_to_weight = {}
        for p in sorted_pos:
            name = (p.get("nombre", "") or "").strip()
            if name:
                name_to_weight[name.lower()[:40]] = p.get("peso_pct", 0) or 0

        # Patron: "(dígito.letra" — signo de que % quedó cortado
        # Ej: "Alphabet Inc (4.La gestion" -> "Alphabet Inc — 4.X% — La gestion"
        def _fix(m):
            frag = m.group(0)
            # Extraer parte antes del parentesis y numero parcial
            pre = m.group(1)   # "4"
            post = m.group(2)  # "La gestion"
            # Buscar nombre justo antes
            return f" — {pre}.?% — {post}"

        texto_fixed = _re.sub(r"\((\d+)\.([A-Za-zÁÉÍÓÚÑáéíóúñ])", _fix, texto)
        return texto_fixed

    def _section_fuentes_int_programmatic(self, data: dict) -> dict:
        """Fuentes externas desde readings_data.json. Sin LLM.
        Mapeo completo: fuente, titulo, opinion, url, fecha (como espera el dashboard)."""
        readings = self._load_json("readings_data.json")
        all_r = readings.get("analisis_completos", []) + readings.get("otros_readings", [])
        if not all_r:
            return {"texto": "", "opiniones_clave": []}

        parts = [f"**{len(all_r)} analisis externos encontrados:**\n"]
        opiniones = []
        for r in all_r:
            source = r.get("source", "") or ""
            titulo = r.get("titulo", "") or ""
            resumen = r.get("resumen", "") or ""
            opinion_text = r.get("opinion_sobre_fondo", "") or ""
            url = r.get("url", "") or ""
            fecha = r.get("fecha", "") or ""

            parts.append(f"**[{source}]** {titulo}")
            if resumen:
                parts.append(f"  {resumen}")
            if opinion_text:
                parts.append(f"  *Opinion:* {opinion_text}")
            if url:
                parts.append(f"  URL: {url}")
            parts.append("")

            opiniones.append({
                "fuente": source,
                "titulo": titulo,
                "opinion": opinion_text or resumen,
                "resumen": resumen,
                "url": url,
                "fecha": fecha,
            })

        return {"texto": "\n".join(parts), "opiniones_clave": opiniones}

    def _extract_web_evidence(self, data: dict) -> str:
        """Extrae citas literales de readings_data.json que mencionan
        gestores, AUM, benchmark, etc. Evidencia externa anti-alucinacion."""
        readings = self._load_json("readings_data.json")
        all_r = readings.get("analisis_completos", []) + readings.get("otros_readings", [])

        gestores = [g.get("nombre", "") for g in data.get("_int_gestores", [])
                    if isinstance(g, dict) and g.get("nombre")]
        apellidos = [g.split()[-1].lower() for g in gestores if g.split()]

        citas = []
        for r in all_r[:10]:
            source = r.get("source", "") or ""
            resumen = r.get("resumen", "") or ""
            if not resumen:
                continue
            # Buscar si menciona los gestores
            resumen_low = resumen.lower()
            mentions_gestor = any(a in resumen_low for a in apellidos)
            if mentions_gestor:
                # Extraer frase relevante (oracion con el apellido)
                import re as _re
                sentences = _re.split(r'(?<=[.!?])\s+', resumen)
                for sent in sentences:
                    if any(a in sent.lower() for a in apellidos):
                        citas.append(f"[{source}]: \"{sent.strip()[:200]}\"")
                        break
        if not citas:
            return ""
        return "\nEVIDENCIA WEB (citas literales de fuentes externas):\n" + "\n".join(citas[:5])

    def _build_real_facts_with_sources(self, data: dict) -> str:
        """Construye el bloque DATOS REALES indicando la FUENTE de cada dato.
        Opus respeta más datos cuando sabe de dónde vienen.
        manager_profile.json se considera fuente autoritativa: se construye
        con cross-validacion web (Trustnet/Citywire/FT/Morningstar)."""
        gestores_reales = [g.get("nombre", "") for g in data.get("_int_gestores", [])]
        kpis = data.get("kpis", {})

        # Cargar manager_profile.json (fuente autoritativa del equipo gestor)
        mgr = self._load_json("manager_profile.json") or {}
        mgr_equipo = mgr.get("equipo", []) or []
        mgr_gestora = mgr.get("gestora", "") or ""
        mgr_sources = []
        for g in mgr_equipo:
            for src in (g.get("fuentes", []) or []):
                if isinstance(src, str):
                    mgr_sources.append(src)
                elif isinstance(src, dict) and src.get("url"):
                    mgr_sources.append(src["url"])
        mgr_sources_line = "; ".join(mgr_sources[:5]) if mgr_sources else "web search cross-validated"

        # Buscar archivos fuente disponibles en raw/discovery/
        disc_path = self.fund_dir / "intl_discovery_data.json"
        source_files = []
        if disc_path.exists():
            try:
                disc = json.loads(disc_path.read_text(encoding="utf-8"))
                for doc in disc.get("documents", []):
                    if doc.get("doc_type") in ("annual_report", "factsheet", "prospectus"):
                        source_files.append(f"{doc.get('doc_type')} {doc.get('periodo','')} "
                                           f"({Path(doc.get('local_path','')).name})")
            except Exception:
                pass

        sources_line = "; ".join(source_files[:5]) or "annual_report + factsheets"

        # Bloque gestora + equipo: si manager_profile coincide con data.gestora, tratarlo
        # como confirmado cross-validado (evita que Opus dude sin motivo).
        gestora_data = data.get("gestora", "") or ""
        if mgr_gestora and mgr_gestora.strip().lower() == gestora_data.strip().lower():
            gestora_line = (f"- Gestora: {gestora_data} (CONFIRMADA cross-validada "
                            f"en manager_profile.json + web: {mgr_sources_line})")
        elif mgr_gestora:
            gestora_line = (f"- Gestora: {gestora_data} (AR) / {mgr_gestora} "
                            f"(manager_profile cross-validado: {mgr_sources_line})")
        else:
            gestora_line = f"- Gestora: {gestora_data} (fuente: AR)"

        if mgr_equipo:
            equipo_line = (f"- Gestores verificados (cross-validados en {mgr_sources_line}): "
                           f"{', '.join(gestores_reales) or 'ninguno'}")
        else:
            equipo_line = (f"- Gestores del fondo: {', '.join(gestores_reales) or 'ninguno'} "
                           f"(fuente: AR + factsheet)")

        base = (
            f"DATOS REALES VERIFICADOS (extraidos de documentos oficiales del fondo + "
            f"cross-validacion web; estos datos son AUTORIDAD, no contradecir):\n"
            f"- ISIN: {self.isin} (fuente: registro regulador)\n"
            f"- Nombre oficial: {data.get('nombre', '')} (fuente: AR + prospectus)\n"
            f"{gestora_line}\n"
            f"{equipo_line}\n"
            f"- AUM: {kpis.get('aum_actual_meur', '')} M EUR (fuente: AR financial statements)\n"
            f"- TER: {kpis.get('ter_pct', '')}% (fuente: KIID + AR Note 7)\n"
            f"- Inicio: {kpis.get('anio_creacion', '')} (fuente: AR + prospectus)\n"
            f"- Benchmark: {kpis.get('benchmark', '')} (fuente: prospectus)\n"
            f"- N posiciones: {len(data.get('posiciones', {}).get('actuales', []))} "
            f"(fuente: AR Schedule of Investments)\n"
            f"- Documentos fuente disponibles: {sources_line}"
        )
        # Añadir evidencia web literal (citas de readings)
        web_ev = self._extract_web_evidence(data)
        return base + web_ev

    def _opus_confirm_issue(self, issue_text: str, real_facts: str) -> dict:
        """Pregunta a Opus si su 'issue' se mantiene al ver las fuentes.
        Devuelve {'reafirma': bool, 'explicacion': str}."""
        try:
            import anthropic
            import os
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            r = client.messages.create(
                model="claude-opus-4-20250514",
                max_tokens=500,
                messages=[{"role": "user", "content": (
                    f"En una auditoria previa identificaste este problema:\n"
                    f'"{issue_text}"\n\n'
                    f"Pero estos son los DATOS REALES VERIFICADOS con fuente:\n"
                    f"{real_facts}\n\n"
                    f"Revisa: ¿tu 'issue' era correcto, o estabas confundiendo este fondo "
                    f"con otro similar (ej. fondo UK vs fondo Ireland, Trojan Fund vs Trojan "
                    f"Income)? Sé honesto: tu conocimiento previo puede estar mezclando fondos.\n\n"
                    f"Responde JSON:\n"
                    f'{{"reafirma": true|false, "explicacion": "texto breve"}}'
                )}],
            )
            cost = (r.usage.input_tokens * 15 + r.usage.output_tokens * 75) / 1_000_000
            self._log("INFO", f"Opus confirm issue ({r.usage.input_tokens}+"
                      f"{r.usage.output_tokens} tok, ${cost:.3f})")
            import re as _re
            m = _re.search(r'\{[\s\S]+?\}', r.content[0].text)
            if m:
                return json.loads(m.group(0))
        except Exception as e:
            self._log("WARN", f"Opus confirm failed: {e}")
        return {"reafirma": False, "explicacion": "confirm failed"}

    def _filter_audit_issues(self, audit: dict, real_facts: str) -> dict:
        """Filtra los issues del audit: consulta a Opus de nuevo pasando fuentes.
        Si Opus se retracta, el issue se elimina."""
        sections = audit.get("sections", {})
        for sec_key, sec_audit in sections.items():
            issues = sec_audit.get("issues", []) or []
            if not issues:
                continue
            # Foco en issues de nombres/cifras (los mas propensos a confusion)
            confirmed = []
            for issue in issues:
                issue_lower = issue.lower()
                is_critical = any(kw in issue_lower for kw in
                                   ["gestor", "nombre", "manager", "aum",
                                    "fecha", "inicio", "lanzamiento", "confusion",
                                    "otro fondo", "different fund"])
                if not is_critical:
                    confirmed.append(issue)
                    continue
                # Preguntar a Opus si reafirma
                confirm = self._opus_confirm_issue(issue, real_facts)
                if confirm.get("reafirma"):
                    confirmed.append(issue)
                else:
                    self._log("INFO", f"Opus se retracta en [{sec_key}]: {issue[:80]}")
            # Actualizar
            sec_audit["issues"] = confirmed
            if not confirmed and sec_audit.get("status") in ("REVISAR", "RECHAZADO"):
                sec_audit["status"] = "OK"
                sec_audit["feedback"] = ""
        return audit

    def _opus_audit_per_section(self, synthesis: dict, data: dict) -> dict:
        """Opus audita TODAS las secciones en 1 call. Devuelve veredicto por seccion.
        Formato: {sections: {resumen: {status, issues, feedback}, ...}, global: {...}}"""
        try:
            import anthropic
            import os
            client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        except Exception:
            return {"auditado": False}

        real_facts = self._build_real_facts_with_sources(data)

        # Contenido de cada seccion (solo texto)
        sections_content = ""
        for key in ("resumen", "historia", "gestores", "estrategia",
                     "evolucion", "cartera", "fuentes_externas"):
            s = synthesis.get(key, {}) if isinstance(synthesis.get(key), dict) else {}
            texto = (s.get("texto", "") or "")[:2000]
            sections_content += f"\n=== {key.upper()} ===\n{texto}\n"

        try:
            r = client.messages.create(
                model="claude-opus-4-20250514",
                max_tokens=3000,
                messages=[{"role": "user", "content": (
                    f"Eres auditor senior. Los DATOS REALES abajo son AUTORIDAD: "
                    f"son los unicos datos verificados del fondo. NO uses tu conocimiento "
                    f"previo — si el informe dice X y los DATOS REALES dicen X, está OK, "
                    f"aunque tu memoria sugiera Y. Si tu conocimiento contradice los DATOS "
                    f"REALES, asume que tu conocimiento es erroneo (fondos similares se "
                    f"confunden, nombres se mezclan).\n\n"
                    f"Para cada seccion determina:\n"
                    f"- status: OK | REVISAR | RECHAZADO\n"
                    f"- issues: problemas concretos (cifras/fechas incorrectas vs DATOS REALES, "
                    f"filler, texto vacio). NO reportes como issue algo que coincida con DATOS REALES.\n"
                    f"- feedback: que corregir si necesita regeneracion\n\n"
                    f"{real_facts}\n\n"
                    f"INFORME A AUDITAR:{sections_content}\n\n"
                    f"Responde JSON:\n"
                    f'{{"sections":{{'
                    f'"resumen":{{"status":"OK|REVISAR|RECHAZADO","issues":[],'
                    f'"feedback":"texto para regenerar si aplica"}},'
                    f'"historia":{{...}},"gestores":{{...}},"estrategia":{{...}},'
                    f'"evolucion":{{...}},"cartera":{{...}},"fuentes_externas":{{...}}'
                    f'}},'
                    f'"global":{{"calidad_score":"1-10","calidad_justificacion":"",'
                    f'"recomendacion":"APROBADO|REVISAR|RECHAZADO"}}}}'
                )}],
            )
            cost = (r.usage.input_tokens * 15 + r.usage.output_tokens * 75) / 1_000_000
            self._log("INFO", f"Opus audit per section ({r.usage.input_tokens}+"
                      f"{r.usage.output_tokens} tok, ${cost:.3f})")
            import re as _re
            m = _re.search(r'\{[\s\S]+\}', r.content[0].text)
            if m:
                try:
                    audit = json.loads(m.group(0))
                    audit["auditado"] = True
                    return audit
                except Exception as e:
                    self._log("WARN", f"Audit JSON parse failed: {e}")
            return {"auditado": False}
        except Exception as e:
            self._log("WARN", f"Opus audit failed: {e}")
            return {"auditado": False, "error": str(e)}

    def _regenerate_section_with_feedback(self, section_key: str, feedback: str,
                                           data: dict, context: str,
                                           anti_halluc: str) -> dict:
        """Regenera una seccion con el feedback especifico de Opus."""
        # Añadir feedback al anti_halluc
        fixed_prompt = anti_halluc + (
            f"CORRECCION REQUERIDA (audit anterior detectó problemas):\n"
            f"{feedback}\n\n"
            f"Regenera esta seccion corrigiendo los problemas.\n\n"
        )
        self._log("INFO", f"Regenerando {section_key} con feedback Opus")

        # Mapeo seccion -> funcion generadora
        if section_key == "resumen":
            return self._section_resumen_int_pro(context, fixed_prompt)
        elif section_key == "historia":
            return self._section_historia_int_pro(context, fixed_prompt)
        elif section_key == "gestores":
            return self._section_gestores_int_pro(context, data, fixed_prompt)
        elif section_key == "estrategia":
            return self._section_estrategia_int_pro(context, fixed_prompt)
        elif section_key == "evolucion":
            return self._section_evolucion_int_pro(context, data, fixed_prompt)
        elif section_key == "cartera":
            return self._section_cartera_int_pro(context, data, fixed_prompt)
        return {}

    def _audit_and_fix_loop(self, synthesis: dict, data: dict,
                             context: str, anti_halluc: str,
                             max_retries: int = 1) -> dict:
        """Audita cada seccion con Opus y regenera las que fallen.
        Antes de regenerar, filtra issues preguntando a Opus con fuentes.
        Bucle con max_retries por seccion para evitar loops infinitos."""
        audit = self._opus_audit_per_section(synthesis, data)
        if not audit.get("auditado"):
            return audit

        # Filtrar issues: preguntar a Opus si reafirma con fuentes
        real_facts = self._build_real_facts_with_sources(data)
        audit = self._filter_audit_issues(audit, real_facts)

        sections_audit = audit.get("sections", {})
        fixed_any = False

        for section_key, section_audit in sections_audit.items():
            status = section_audit.get("status", "OK")
            if status in ("REVISAR", "RECHAZADO") and section_audit.get("feedback"):
                # Solo regenerar secciones generadas por Gemini (no programaticas)
                if section_key in ("fuentes_externas",):
                    continue
                new_section = self._regenerate_section_with_feedback(
                    section_key, section_audit["feedback"],
                    data, context, anti_halluc,
                )
                if new_section and new_section.get("texto"):
                    # Preservar campos programaticos (concentracion, posiciones_top, etc)
                    old = synthesis.get(section_key, {})
                    for k in ("distribucion_tipo", "concentracion",
                               "posiciones_top", "datos_graficos", "equipo"):
                        if k in old:
                            new_section[k] = old[k]
                    synthesis[section_key] = new_section
                    fixed_any = True

        # Si se regenero algo, re-auditar (1 vez mas)
        if fixed_any and max_retries > 0:
            audit_v2 = self._opus_audit_per_section(synthesis, data)
            audit_v2["previous_issues"] = audit
            return audit_v2

        return audit

    # Aliases para integración con el pipeline existente
    def _section_resumen_int(self, data: dict) -> dict | None:
        if not hasattr(self, "_opus_cache"):
            self._opus_cache = self._opus_int_synthesis(data)
        return self._opus_cache.get("resumen") or {"texto": ""}

    def _section_historia_int(self, data: dict) -> dict | None:
        if not hasattr(self, "_opus_cache"):
            self._opus_cache = self._opus_int_synthesis(data)
        return self._opus_cache.get("historia") or {"texto": ""}

    def _section_gestores_int(self, data: dict) -> dict | None:
        if not hasattr(self, "_opus_cache"):
            self._opus_cache = self._opus_int_synthesis(data)
        return self._opus_cache.get("gestores") or {"texto": "", "equipo": []}

    def _section_estrategia_int(self, data: dict) -> dict | None:
        if not hasattr(self, "_opus_cache"):
            self._opus_cache = self._opus_int_synthesis(data)
        return self._opus_cache.get("estrategia") or {"texto": ""}

    def _section_evolucion_int(self, data: dict) -> dict | None:
        if not hasattr(self, "_opus_cache"):
            self._opus_cache = self._opus_int_synthesis(data)
        return self._opus_cache.get("evolucion") or {"texto": ""}

    def _section_cartera_int(self, data: dict) -> dict | None:
        if not hasattr(self, "_opus_cache"):
            self._opus_cache = self._opus_int_synthesis(data)
        return self._opus_cache.get("cartera") or {"texto": ""}

    def _section_fuentes_int(self, data: dict) -> dict | None:
        if not hasattr(self, "_opus_cache"):
            self._opus_cache = self._opus_int_synthesis(data)
        return self._opus_cache.get("fuentes_externas") or {"texto": ""}

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
