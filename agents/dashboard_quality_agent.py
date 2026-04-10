"""
Dashboard Quality Agent — evalúa output.json contra el patrón de calidad Avantage.
Genera informe de fallos con agente responsable y acción correctiva.

Usage: python -m agents.dashboard_quality_agent [ISIN]
"""
import json
import sys
from pathlib import Path
from datetime import datetime
from rich.console import Console
from rich.table import Table

console = Console()
ROOT = Path(__file__).parent.parent


# ═══════════════════════════════════════════════════════════════
# QUALITY PATTERN — derived from Avantage Fund dashboard
# ═══════════════════════════════════════════════════════════════

# Avantage reference metrics (the gold standard)
AVANTAGE_REF = {
    "resumen_chars": 3646, "historia_chars": 6844, "gestores_chars": 8548,
    "estrategia_chars": 5220, "cartera_chars": 3459, "fuentes_chars": 3813,
    "hitos": 7, "perfiles": 5, "hitos_estrategia": 4, "posiciones": 46,
    "serie_aum_pts": 12, "serie_vl_pts": 12, "serie_participes_pts": 12,
    "fortalezas": 9, "riesgos": 6, "opiniones": 13,
}

QUALITY_PATTERN = {
    "resumen": {
        "min_chars": 2500, "target_chars": 3500,
        "must_mention_fund_name": True,
        "must_have_subsections": True,
        "must_have_cifras": 5,  # minimum number of concrete figures
        "min_fortalezas": 4, "min_riesgos": 3,
        "must_have_para_quien": True,
        "must_have_signal": True,
        "weight": 20,
    },
    "historia": {
        "min_chars": 3000, "target_chars": 5000,
        "min_hitos": 5,
        "must_have_subsections": True,
        "must_have_cifras": 5,
        "weight": 15,
    },
    "gestores": {
        "min_chars": 4000, "target_chars": 7000,
        "min_perfiles": 2,
        "lead_must_have_filosofia": True,
        "lead_must_have_decisiones": True,
        "lead_min_chars": 1500,
        "weight": 15,
    },
    "estrategia": {
        "min_chars": 3000, "target_chars": 5000,
        "min_hitos_estrategia": 3,
        "must_have_subsections": True,
        "must_have_cifras": 3,
        "weight": 15,
    },
    "cartera": {
        "min_chars": 2000, "target_chars": 3000,
        "min_positions": 15,
        "must_have_hist_positions": True,
        "weight": 15,
    },
    "fuentes_externas": {
        "min_chars": 800, "target_chars": 2000,
        "min_opiniones": 2,
        "weight": 10,
    },
    "datos_cuantitativos": {
        "min_serie_aum": 4,
        "min_serie_vl": 3,
        "min_serie_participes": 3,
        "must_have_comisiones": True,
        "must_have_rotacion": True,
        "weight": 10,
    },
}


class DashboardQualityAgent:

    def __init__(self, isin: str):
        self.isin = isin.strip().upper()
        self.fund_dir = ROOT / "data" / "funds" / self.isin

    def run(self) -> dict:
        """Evaluate output.json quality. Returns quality report."""
        output_path = self.fund_dir / "output.json"
        if not output_path.exists():
            return {"fund": self.isin, "score": 0, "aceptable": False,
                    "fallos": [{"seccion": "global", "problema": "No existe output.json",
                               "agente_responsable": "orchestrator", "accion": "Ejecutar pipeline completo",
                               "prioridad": "CRITICA"}]}

        data = json.loads(output_path.read_text(encoding="utf-8"))
        synth = data.get("analyst_synthesis", {})
        cuant = data.get("cuantitativo", {})
        pos = data.get("posiciones", {})

        fallos = []
        scores = {}
        total_weight = sum(p["weight"] for p in QUALITY_PATTERN.values())

        # ── Evaluate each section ──
        for section, rules in QUALITY_PATTERN.items():
            if section == "datos_cuantitativos":
                score, section_fallos = self._eval_cuantitativo(cuant, pos, rules, data)
            else:
                sec_data = synth.get(section, {})
                score, section_fallos = self._eval_section(section, sec_data, rules, data)
            scores[section] = score
            fallos.extend(section_fallos)

        # Calculate weighted global score
        global_score = 0
        for section, rules in QUALITY_PATTERN.items():
            global_score += scores.get(section, 0) * rules["weight"] / total_weight

        report = {
            "fund": self.isin,
            "nombre": data.get("nombre", ""),
            "score": round(global_score),
            "aceptable": global_score >= 80,
            "scores_por_seccion": scores,
            "fallos": sorted(fallos, key=lambda x: {"CRITICA": 0, "ALTA": 1, "MEDIA": 2}.get(x.get("prioridad", ""), 3)),
            "secciones_ok": [s for s, sc in scores.items() if sc >= 80],
            "evaluado_at": datetime.now().isoformat(),
        }

        # Save report
        report_path = self.fund_dir / "quality_report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        # Print summary
        self._print_report(report)

        return report

    def _eval_section(self, name: str, sec_data: dict, rules: dict, full_data: dict) -> tuple:
        """Evaluate a single analyst_synthesis section. Returns (score 0-100, [fallos])."""
        fallos = []
        checks_passed = 0
        checks_total = 0

        texto = sec_data.get("texto", "")
        fund_name = full_data.get("nombre", "")

        # Check: section exists
        checks_total += 1
        if not sec_data:
            fallos.append({
                "seccion": name, "problema": f"Sección '{name}' vacía — no generada por analyst_agent",
                "agente_responsable": "analyst_agent",
                "accion": f"Re-ejecutar analyst_agent. Verificar que los agentes upstream proporcionan datos suficientes.",
                "prioridad": "CRITICA"
            })
            return 0, fallos
        checks_passed += 1

        # Check: minimum chars
        min_chars = rules.get("min_chars", 0)
        checks_total += 1
        if len(texto) < min_chars:
            fallos.append({
                "seccion": name,
                "problema": f"Texto demasiado corto: {len(texto)} chars (mínimo {min_chars})",
                "agente_responsable": "analyst_agent",
                "accion": f"Re-generar {name} con prompt más detallado. Mínimo {min_chars} chars con subsecciones.",
                "prioridad": "CRITICA" if len(texto) < min_chars * 0.5 else "ALTA"
            })
        else:
            checks_passed += 1

        # Check: mentions fund name
        if rules.get("must_mention_fund_name") and fund_name:
            checks_total += 1
            if fund_name.split(",")[0].strip().lower() not in texto.lower() and fund_name.lower() not in texto.lower():
                fallos.append({
                    "seccion": name,
                    "problema": f"Texto no menciona el nombre del fondo '{fund_name}' — posible texto genérico",
                    "agente_responsable": "analyst_agent",
                    "accion": "Re-generar: el texto debe ser específico de este fondo, no genérico.",
                    "prioridad": "ALTA"
                })
            else:
                checks_passed += 1

        # Check: has subsections (**bold headers**)
        if rules.get("must_have_subsections"):
            checks_total += 1
            bold_count = texto.count("**")
            if bold_count < 4:  # At least 2 subsection headers
                fallos.append({
                    "seccion": name,
                    "problema": "Sin subsecciones marcadas con **negrita**. Texto plano difícil de leer.",
                    "agente_responsable": "analyst_agent",
                    "accion": "Re-generar con instrucción: 'Estructura con subsecciones usando **Título** en negrita.'",
                    "prioridad": "ALTA"
                })
            else:
                checks_passed += 1

        # Check: has concrete numbers
        min_cifras = rules.get("must_have_cifras", 0)
        if min_cifras:
            checks_total += 1
            import re
            numbers = re.findall(r'\d+[.,]\d+%|\d+\s*M€|\d+\s*partícipes|\d+\s*posiciones|\d+\s*años', texto)
            if len(numbers) < min_cifras:
                fallos.append({
                    "seccion": name,
                    "problema": f"Texto con pocas cifras concretas ({len(numbers)} encontradas). Parece genérico.",
                    "agente_responsable": "analyst_agent",
                    "accion": "Re-generar: incluir cifras de AUM, partícipes, rentabilidad, volatilidad.",
                    "prioridad": "ALTA"
                })
            else:
                checks_passed += 1

        # Section-specific checks
        if name == "resumen":
            for field, min_count in [("fortalezas", rules.get("min_fortalezas", 0)),
                                      ("riesgos", rules.get("min_riesgos", 0))]:
                checks_total += 1
                items = sec_data.get(field, [])
                if len(items) < min_count:
                    fallos.append({
                        "seccion": name,
                        "problema": f"Solo {len(items)} {field} (mínimo {min_count})",
                        "agente_responsable": "analyst_agent",
                        "accion": f"Re-generar con al menos {min_count} {field} específicas del fondo.",
                        "prioridad": "ALTA"
                    })
                else:
                    checks_passed += 1

        if name == "historia":
            checks_total += 1
            hitos = sec_data.get("hitos", [])
            min_hitos = rules.get("min_hitos", 0)
            if len(hitos) < min_hitos:
                fallos.append({
                    "seccion": name,
                    "problema": f"Solo {len(hitos)} hitos (mínimo {min_hitos}). Cronología vacía o pobre.",
                    "agente_responsable": "analyst_agent",
                    "accion": "Re-generar historia con más hitos. Si no hay datos, pedir a cnmv_agent hechos_relevantes.",
                    "prioridad": "ALTA"
                })
            else:
                checks_passed += 1

        if name == "gestores":
            perfiles = sec_data.get("perfiles", [])
            checks_total += 1
            if len(perfiles) < rules.get("min_perfiles", 1):
                fallos.append({
                    "seccion": name,
                    "problema": "Sin perfiles de gestores detallados.",
                    "agente_responsable": "manager_deep_agent",
                    "accion": "Re-ejecutar manager_deep_agent con más búsquedas.",
                    "prioridad": "CRITICA"
                })
            else:
                checks_passed += 1
                # Check lead profile quality
                if perfiles:
                    lead = perfiles[0]
                    if rules.get("lead_must_have_filosofia") and not lead.get("filosofia"):
                        checks_total += 1
                        fallos.append({
                            "seccion": name,
                            "problema": f"Gestor principal '{lead.get('nombre','')}' sin filosofía de inversión.",
                            "agente_responsable": "manager_deep_agent",
                            "accion": "Buscar filosofía en web de gestora, cartas, entrevistas.",
                            "prioridad": "ALTA"
                        })

        if name == "estrategia":
            checks_total += 1
            hitos = sec_data.get("hitos_estrategia", [])
            if len(hitos) < rules.get("min_hitos_estrategia", 0):
                fallos.append({
                    "seccion": name,
                    "problema": f"Solo {len(hitos)} hitos estratégicos (mínimo {rules.get('min_hitos_estrategia',0)}).",
                    "agente_responsable": "analyst_agent",
                    "accion": "Re-generar estrategia con hitos por periodo. Usar datos de timeline + mix_activos.",
                    "prioridad": "ALTA"
                })
            else:
                checks_passed += 1

        if name == "cartera":
            checks_total += 1
            actuales = full_data.get("posiciones", {}).get("actuales", [])
            if len(actuales) < rules.get("min_positions", 0):
                fallos.append({
                    "seccion": name,
                    "problema": f"Solo {len(actuales)} posiciones (mínimo {rules.get('min_positions',0)}).",
                    "agente_responsable": "cnmv_agent",
                    "accion": "Verificar extracción de posiciones de sección 10 del PDF semestral.",
                    "prioridad": "ALTA"
                })
            else:
                checks_passed += 1

        if name == "fuentes_externas":
            checks_total += 1
            opiniones = sec_data.get("opiniones_clave", [])
            if len(opiniones) < rules.get("min_opiniones", 0):
                fallos.append({
                    "seccion": name,
                    "problema": "Sin fuentes externas. El fondo no tiene análisis de terceros.",
                    "agente_responsable": "readings_agent",
                    "accion": "Re-ejecutar readings_agent con más queries y fuentes.",
                    "prioridad": "MEDIA"
                })
            else:
                checks_passed += 1

        # Score: start at 100, deduct HARD by severity
        # CRITICA: -50pts, ALTA: -30pts, MEDIA: -15pts
        score = 100
        for fallo in fallos:
            prio = fallo.get("prioridad", "MEDIA")
            if prio == "CRITICA":
                score -= 50
            elif prio == "ALTA":
                score -= 30
            else:
                score -= 15
        return max(0, min(100, score)), fallos

    def _eval_cuantitativo(self, cuant: dict, pos: dict, rules: dict, full_data: dict) -> tuple:
        """Evaluate quantitative data completeness."""
        fallos = []
        checks_passed = 0
        checks_total = 0

        for serie, min_pts in [("serie_aum", rules.get("min_serie_aum", 0)),
                                ("serie_vl_base100", rules.get("min_serie_vl", 0)),
                                ("serie_participes", rules.get("min_serie_participes", 0))]:
            checks_total += 1
            pts = len(cuant.get(serie, []))
            if pts < min_pts:
                fallos.append({
                    "seccion": "datos_cuantitativos",
                    "problema": f"{serie}: {pts} puntos (mínimo {min_pts})",
                    "agente_responsable": "cnmv_agent",
                    "accion": f"Re-ejecutar cnmv_agent con horizonte_historico='1' para descargar todos los informes.",
                    "prioridad": "ALTA" if pts == 0 else "MEDIA"
                })
            else:
                checks_passed += 1

        if rules.get("must_have_rotacion"):
            checks_total += 1
            rot = cuant.get("serie_rotacion", [])
            # Cartera permanente: rotación nula es por diseño — no marcar como fallo
            nombre_lower = (full_data.get("nombre", "") or "").lower()
            es_cartera_permanente = "cartera permanente" in nombre_lower or "permanent portfolio" in nombre_lower
            if es_cartera_permanente:
                # Estrategia es no-rotar — aceptamos rotacion=0 como diseño
                checks_passed += 1
            elif len(rot) < 2:
                fallos.append({
                    "seccion": "datos_cuantitativos",
                    "problema": f"serie_rotacion: {len(rot)} puntos. Necesaria para análisis de estrategia.",
                    "agente_responsable": "cnmv_agent",
                    "accion": "Verificar extracción de rotación de los PDFs semestrales.",
                    "prioridad": "MEDIA"
                })
            else:
                checks_passed += 1

        if rules.get("must_have_comisiones"):
            checks_total += 1
            com = cuant.get("serie_comisiones_por_clase", [])
            ter = cuant.get("serie_ter", [])
            if not com and not ter:
                fallos.append({
                    "seccion": "datos_cuantitativos",
                    "problema": "Sin datos de comisiones ni TER.",
                    "agente_responsable": "cnmv_agent",
                    "accion": "Verificar extracción de comisiones del PDF semestral (tabla 'Comisiones aplicadas').",
                    "prioridad": "ALTA"
                })
            else:
                checks_passed += 1

        # Score: start at 100, deduct HARD by severity
        # CRITICA: -50pts, ALTA: -30pts, MEDIA: -15pts
        score = 100
        for fallo in fallos:
            prio = fallo.get("prioridad", "MEDIA")
            if prio == "CRITICA":
                score -= 50
            elif prio == "ALTA":
                score -= 30
            else:
                score -= 15
        return max(0, min(100, score)), fallos

    def _print_report(self, report: dict):
        """Print a Rich summary of the quality report."""
        score = report["score"]
        color = "green" if score >= 70 else "yellow" if score >= 50 else "red"

        table = Table(title=f"Quality Report — {report['nombre']} ({report['fund']})")
        table.add_column("Sección", width=20)
        table.add_column("Score", width=8, justify="right")
        table.add_column("Estado", width=10)

        for section, sc in report.get("scores_por_seccion", {}).items():
            state = "[green]OK" if sc >= 80 else "[yellow]MEJORAR" if sc >= 50 else "[red]FALLO"
            table.add_row(section, f"{sc}%", state)

        table.add_row("", "", "")
        table.add_row("[bold]GLOBAL", f"[bold {color}]{score}%", f"[bold {'green' if report['aceptable'] else 'red'}]{'ACEPTABLE' if report['aceptable'] else 'NO ACEPTABLE'}")

        console.print(table)

        if report["fallos"]:
            console.print(f"\n[bold red]Fallos ({len(report['fallos'])}):")
            for fallo in report["fallos"][:10]:
                prio = fallo.get("prioridad", "?")
                prio_color = {"CRITICA": "red", "ALTA": "yellow", "MEDIA": "blue"}.get(prio, "white")
                console.print(f"  [{prio_color}][{prio}][/{prio_color}] {fallo['seccion']}: {fallo['problema'][:80]}")
                console.print(f"         >> {fallo['agente_responsable']}: {fallo['accion'][:80]}")


# ── CLI ──
if __name__ == "__main__":
    isin = sys.argv[1] if len(sys.argv) > 1 else "ES0112231008"
    agent = DashboardQualityAgent(isin)
    report = agent.run()
