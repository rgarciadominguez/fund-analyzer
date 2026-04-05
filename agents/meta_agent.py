"""
Meta-Agent — QA del pipeline + feedback loop

Se ejecuta al final del pipeline automáticamente. Revisa la calidad del output.json
y genera sugerencias de mejora. También solicita feedback del usuario al terminar.

Output:
  data/funds/{ISIN}/meta_report.json
  data/funds/{ISIN}/feedback.json  (si el usuario da feedback)
"""
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

sys.path.insert(0, str(Path(__file__).parent.parent))

console = Console(highlight=False, force_terminal=False)


MIX_KEYS = ["renta_fija_pct", "rv_pct", "iic_pct", "liquidez_pct", "depositos_pct"]


# Patrones de issues conocidos aprendidos del feedback del usuario
KNOWN_ISSUES_PATTERNS = {
    "mix_activos_suma_incorrecta": {
        "check": lambda output: any(
            abs(sum((m.get(k) or 0) for k in MIX_KEYS) - 100) > 15
            for m in output.get("cuantitativo", {}).get("mix_activos_historico", [])
            if sum((m.get(k) or 0) for k in MIX_KEYS) > 5
        ),
        "mensaje": "mix_activos suma incorrecta — probable doble conteo en extraccion XML CNMV seccion 3.1 "
                   "(se deben tomar solo los valores TOTAL globales, no sumas de subtotales)",
        "accion": "verificar cnmv_agent: usar rf_pcts[-1] en vez de sum(rf_pcts)",
    },
    "gestores_todos_null": {
        "check": lambda output: bool(output.get("cualitativo", {}).get("gestores")) and all(
            g.get("nombre") is None
            for g in output.get("cualitativo", {}).get("gestores", [])
        ),
        "mensaje": "todos los gestores tienen nombre=null — extraccion PDF cualitativo fallida o sin PDFs cualitativos",
        "accion": "re-ejecutar analyst_agent con PDF semestral mas reciente disponible",
    },
    "serie_aum_muy_corta": {
        "check": lambda output: len(output.get("cuantitativo", {}).get("serie_aum", [])) < 4,
        "mensaje": "serie AUM muy corta (<4 puntos) — XMLs CNMV no procesados correctamente o fondo muy nuevo",
        "accion": "verificar que cnmv_agent itera todos los XMLs desde anio_creacion hasta hoy",
    },
    "analisis_externos_google_urls": {
        "check": lambda _: False,  # Checked via file, not output
        "mensaje": "analisis_externos contiene URLs de busqueda Google en vez de articulos reales",
        "accion": "re-ejecutar readings_agent — ahora fetchea articulos reales y genera resumen con Claude",
    },
}


class MetaAgent:
    """
    Agente meta-QA: revisa calidad del output de todos los agentes.
    async def run() -> dict según convenio del proyecto.
    """

    # Peso mínimo de AUM para alertar (M€)
    MIN_AUM_MEUR = 0.1

    def __init__(self, isin: str, fund_dir: Path | str | None = None, config: dict | None = None):
        self.isin   = isin.strip().upper()
        self.config = config or {}
        root = Path(__file__).parent.parent
        self.fund_dir = Path(fund_dir) if fund_dir else (root / "data" / "funds" / self.isin)
        self._log_path = root / "progress.log"

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [META] [{level}] {msg}"
        safe_line = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe_line, flush=True)
        with open(self._log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    def _load_output(self) -> dict:
        p = self.fund_dir / "output.json"
        if not p.exists():
            return {}
        return json.loads(p.read_text(encoding="utf-8"))

    def _load_file(self, filename: str) -> dict | list:
        p = self.fund_dir / filename
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}

    # ── Revisión de calidad ───────────────────────────────────────────────────

    def _find_issues(self, output: dict) -> list[str]:
        """Detecta campos vacíos, inconsistencias y datos sospechosos."""
        issues = []

        kpis     = output.get("kpis", {})
        cual     = output.get("cualitativo", {})
        cuant    = output.get("cuantitativo", {})
        pos_data = output.get("posiciones", {})
        consist  = output.get("analisis_consistencia", {})

        # 1. Campos críticos vacíos
        if not output.get("nombre"):
            issues.append("nombre del fondo vacío")
        if not output.get("gestora"):
            issues.append("gestora vacía")
        if not kpis.get("aum_actual_meur"):
            issues.append("AUM actual vacío — revisar XMLs CNMV o annual report")
        if not cual.get("gestores"):
            issues.append("gestores vacíos — buscar en Citywire / Finect")
        if not cual.get("estrategia") and not cual.get("filosofia_inversion"):
            issues.append("sin estrategia ni filosofía de inversión — necesita PDFs cualitativos")
        if not pos_data.get("actuales"):
            issues.append("sin posiciones actuales — revisar extracción de cartera")

        # 2. Series históricas cortas
        serie_aum = cuant.get("serie_aum", [])
        if len(serie_aum) < 3:
            issues.append(f"serie AUM muy corta ({len(serie_aum)} puntos) — añadir XMLs históricos")

        periodos = consist.get("periodos", [])
        if len(periodos) < 3:
            issues.append(f"solo {len(periodos)} periodos de consistencia — añadir más PDFs semestrales")

        # 3. Mix activos no suma 100%
        for mix in cuant.get("mix_activos_historico", []):
            keys = ["renta_fija_pct", "rv_pct", "iic_pct", "liquidez_pct", "depositos_pct"]
            total = sum(mix.get(k, 0) or 0 for k in keys)
            if total > 5 and abs(total - 100) > 10:
                issues.append(f"mix_activos {mix.get('periodo','?')} suma {total:.1f}% (esperado ~100%)")

        # 4. Posiciones con peso nulo
        actuales = pos_data.get("actuales", [])
        sin_peso = [p for p in actuales if not p.get("peso_pct")]
        if sin_peso and len(sin_peso) > len(actuales) * 0.5:
            issues.append(f"{len(sin_peso)}/{len(actuales)} posiciones sin peso % — revisar extracción")

        # 5. AUM inconsistente
        if kpis.get("aum_actual_meur") and (kpis["aum_actual_meur"] or 0) < self.MIN_AUM_MEUR:
            issues.append(f"AUM muy bajo ({kpis['aum_actual_meur']} M€) — posible error de unidades")

        # 6. Sin cartas de gestores
        letters_d = self._load_file("letters_data.json")
        if isinstance(letters_d, dict) and not letters_d.get("cartas"):
            issues.append("sin cartas de gestores — ejecutar letters_agent con DuckDuckGo")

        # 7. Sin análisis externos
        if not (self.fund_dir / "analisis_externos.json").exists():
            issues.append("sin análisis externos — ejecutar readings_agent")
        else:
            # Check if analisis_externos has only search URLs (not real articles)
            try:
                ext_data = json.loads((self.fund_dir / "analisis_externos.json").read_text(encoding="utf-8"))
                ext_list = ext_data if isinstance(ext_data, list) else ext_data.get("analisis_externos", [])
                search_domains = ("google.com", "duckduckgo.com", "bing.com")
                bad_urls = [it for it in ext_list if any(d in (it.get("url", "") or "") for d in search_domains)]
                if bad_urls and len(bad_urls) == len(ext_list):
                    issues.append("analisis_externos contiene solo URLs de busqueda Google — re-ejecutar readings_agent")
            except Exception:
                pass

        # 8. Check learned patterns from user feedback
        for pattern_id, pattern in KNOWN_ISSUES_PATTERNS.items():
            if pattern_id == "analisis_externos_google_urls":
                continue  # Already checked above
            try:
                if pattern["check"](output):
                    issues.append(f"[patron_conocido:{pattern_id}] {pattern['mensaje']}")
            except Exception:
                pass

        return issues

    def _suggest_improvements(self, issues: list[str], output: dict) -> list[str]:
        """Genera sugerencias concretas para mejorar el análisis."""
        suggestions = []
        kpis  = output.get("kpis", {})
        cuant = output.get("cuantitativo", {})

        anio_creacion = kpis.get("anio_creacion")
        serie_aum = cuant.get("serie_aum", [])
        current_year = datetime.now().year

        # Sugerencia: años que faltan en serie AUM
        if anio_creacion and serie_aum:
            years_present = set()
            for s in serie_aum:
                m = __import__("re").match(r"^(20\d{2})", str(s.get("periodo", "")))
                if m:
                    years_present.add(int(m.group(1)))
            years_missing = [y for y in range(anio_creacion, current_year + 1) if y not in years_present]
            if years_missing:
                suggestions.append(
                    f"Años sin datos AUM: {', '.join(map(str, years_missing))} "
                    f"-> descargar XMLs CNMV de esos años"
                )

        # Sugerencia: gestores sin background
        cual = output.get("cualitativo", {})
        gestores_sin_bg = [g.get("nombre", "") for g in cual.get("gestores", [])
                           if not g.get("background") and g.get("nombre")]
        if gestores_sin_bg:
            suggestions.append(
                f"Gestores sin background: {', '.join(gestores_sin_bg)} "
                f"-> buscar en Citywire / Trustnet"
            )

        # Sugerencia general por cada issue
        for issue in issues:
            if "XML" in issue or "XMLs" in issue:
                suggestions.append(f"-> Descargar XMLs faltantes: python -m agents.orchestrator --isin {self.isin} --auto")
            elif "PDFs" in issue or "PDFs" in issue:
                suggestions.append("-> Anadir más PDFs semestrales en raw/reports/")

        return list(dict.fromkeys(suggestions))  # dedup manteniendo orden

    def _calculate_completeness(self, output: dict) -> float:
        """Calcula % de completitud del output (0-100)."""
        fields = [
            output.get("nombre"),
            output.get("gestora"),
            output.get("kpis", {}).get("aum_actual_meur"),
            output.get("kpis", {}).get("num_participes"),
            output.get("kpis", {}).get("ter_pct"),
            output.get("kpis", {}).get("clasificacion"),
            output.get("cualitativo", {}).get("estrategia"),
            output.get("cualitativo", {}).get("gestores"),
            output.get("cualitativo", {}).get("filosofia_inversion"),
            output.get("cualitativo", {}).get("proceso_seleccion"),
            output.get("cuantitativo", {}).get("serie_aum"),
            output.get("cuantitativo", {}).get("serie_participes"),
            output.get("cuantitativo", {}).get("serie_ter"),
            output.get("cuantitativo", {}).get("mix_activos_historico"),
            output.get("posiciones", {}).get("actuales"),
            output.get("posiciones", {}).get("historicas"),
            output.get("analisis_consistencia", {}).get("periodos"),
            output.get("analisis_consistencia", {}).get("resumen_global"),
        ]
        filled = sum(1 for f in fields if f)
        return round(filled / len(fields) * 100, 1)

    def _load_prior_feedback(self) -> list[dict]:
        """Carga feedback previo del usuario para incluirlo en el reporte."""
        fb_path = self.fund_dir / "feedback.json"
        if not fb_path.exists():
            return []
        try:
            data = json.loads(fb_path.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
        except Exception:
            pass
        return []

    # ── Run ───────────────────────────────────────────────────────────────────

    async def run(self) -> dict:
        self._log("START", f"Meta-agent revisando output de {self.isin}")
        output = self._load_output()

        if not output:
            self._log("WARN", "output.json no encontrado — pipeline incompleto")
            report = {
                "isin": self.isin,
                "timestamp": datetime.now().isoformat(),
                "completeness_pct": 0,
                "issues": ["output.json no encontrado"],
                "suggestions": ["Ejecutar el pipeline completo primero"],
            }
            self._save_report(report)
            return report

        issues      = self._find_issues(output)
        suggestions = self._suggest_improvements(issues, output)
        completeness = self._calculate_completeness(output)

        # Load prior user feedback to include in report
        prior_feedback = self._load_prior_feedback()

        report = {
            "isin":             self.isin,
            "nombre":           output.get("nombre", ""),
            "timestamp":        datetime.now().isoformat(),
            "completeness_pct": completeness,
            "issues":           issues,
            "suggestions":      suggestions,
            "feedback_previo":  prior_feedback,
            "stats": {
                "periodos_consistencia": len(output.get("analisis_consistencia", {}).get("periodos", [])),
                "posiciones_actuales":   len(output.get("posiciones", {}).get("actuales", [])),
                "puntos_aum":            len(output.get("cuantitativo", {}).get("serie_aum", [])),
                "gestores":              len(output.get("cualitativo", {}).get("gestores", [])),
            },
        }

        self._save_report(report)
        self._print_report(report)
        self._log("OK", f"Completitud: {completeness}% | Issues: {len(issues)}")
        return report

    def _print_report(self, report: dict):
        """Muestra el reporte en consola con rich."""
        completeness = report["completeness_pct"]
        color = "green" if completeness >= 70 else "yellow" if completeness >= 40 else "red"

        # Use print() instead of console.print() to avoid cp1252 encoding errors on Windows
        safe_nombre = report.get('nombre', self.isin).encode("cp1252", errors="replace").decode("cp1252")
        print(f"\nMeta-QA: {safe_nombre}")
        print(f"  Completitud: {completeness}%")
        for k, v in report.get("stats", {}).items():
            print(f"  {k.replace('_', ' ').title()}: {v}")

        if report["issues"]:
            print("\nIssues detectados:")
            for i in report["issues"]:
                safe_i = i.encode("cp1252", errors="replace").decode("cp1252")
                prefix = "  [patron] " if "[patron_conocido:" in i else "  ! "
                print(f"{prefix}{safe_i}")
        if report["suggestions"]:
            print("\nSugerencias de mejora:")
            for s in report["suggestions"]:
                safe_s = s.encode("cp1252", errors="replace").decode("cp1252")
                print(f"  -> {safe_s}")
        prior = report.get("feedback_previo", [])
        if prior:
            print(f"\nFeedback previo del usuario: {len(prior)} entradas registradas")
            for fb in prior[-2:]:  # Show last 2
                ts = str(fb.get("timestamp", ""))[:10]
                resp = fb.get("respuestas", {})
                print(f"  [{ts}] {len(resp)} respuestas")

    def _save_report(self, report: dict):
        p = self.fund_dir / "meta_report.json"
        self.fund_dir.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        self._log("OK", f"Meta-report guardado: {p}")

    # ── Feedback del usuario ──────────────────────────────────────────────────

    async def request_user_feedback(self) -> dict:
        """Muestra preguntas de feedback en terminal y guarda respuestas."""
        console.print(Panel(
            "Tu feedback mejora el análisis. Puedes pulsar Enter para saltar cualquier pregunta.",
            title="📝 Feedback del análisis",
            border_style="green",
        ))

        questions = [
            ("utilidad",      "¿Qué información fue más útil en este análisis?"),
            ("faltante",      "¿Qué información echaste en falta o fue poco profunda?"),
            ("fuentes",       "¿Qué fuentes adicionales sugerirías para este fondo?"),
            ("mejoras",       "¿Qué mejorarías del formato o presentación del dashboard?"),
            ("puntaje",       "Puntúa el análisis del 1 al 10:"),
        ]

        feedback = {
            "isin":      self.isin,
            "timestamp": datetime.now().isoformat(),
            "respuestas": {},
        }

        for q_id, q_text in questions:
            try:
                resp = Prompt.ask(f"[cyan]{q_text}[/]", default="")
                if resp.strip():
                    feedback["respuestas"][q_id] = resp.strip()
            except (KeyboardInterrupt, EOFError):
                break

        if feedback["respuestas"]:
            fb_path = self.fund_dir / "feedback.json"
            # Acumular feedbacks
            existing = []
            if fb_path.exists():
                try:
                    existing = json.loads(fb_path.read_text(encoding="utf-8"))
                    if isinstance(existing, dict):
                        existing = [existing]
                except Exception:
                    existing = []
            existing.append(feedback)
            fb_path.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
            self._log("OK", f"Feedback guardado: {fb_path}")
            console.print("[green]OK Gracias por tu feedback![/]")
        else:
            console.print("[dim]Sin feedback registrado.[/]")

        return feedback


# ── CLI standalone ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--isin", required=True)
    parser.add_argument("--feedback", action="store_true", help="Pedir feedback al usuario")
    args = parser.parse_args()

    agent = MetaAgent(args.isin)
    result = asyncio.run(agent.run())

    print(f"\nCompletitud: {result['completeness_pct']}%")
    print(f"Issues: {len(result['issues'])}")

    if args.feedback:
        asyncio.run(agent.request_user_feedback())
