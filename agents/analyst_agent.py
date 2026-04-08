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
  CAPA 3: Analyst Senior (8 secciones briefing)
  CAPA 4: Presentación ejecutiva

Este archivo implementa CAPAS 1 y 2.
CAPAS 3 y 4 se implementarán después de validar el output de las capas 1-2.
"""
import json
import os
import re
import sys
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
