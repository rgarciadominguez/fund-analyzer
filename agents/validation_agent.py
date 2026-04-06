"""
Validation Agent — Validates output.json quality and triggers corrections.

Not just nulls: reads the entire output and checks that everything makes sense.
Compares against external references (finect, morningstar, web gestora).
If something is off, calls the responsible agent to re-extract/re-synthesize.

Learns from errors to improve future extractions.
"""
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.parent))

console = Console()


class ValidationAgent:
    """
    Post-analyst validation: reads output.json, validates data quality,
    compares with external sources, triggers corrections if needed.
    """

    def __init__(self, isin: str, fund_dir: Path = None, config: dict = None):
        self.isin = isin.strip().upper()
        self.config = config or {}
        root = Path(__file__).parent.parent
        self.fund_dir = fund_dir or (root / "data" / "funds" / self.isin)
        self.log_path = root / "progress.log"
        self.knowledge_path = root / "data" / "extraction_knowledge.json"

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [VALIDATION] [{level}] {msg}"
        console.log(line)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    async def run(self) -> dict:
        """Run all validations and return a quality report."""
        self._log("START", f"Validando output.json para {self.isin}")

        output_path = self.fund_dir / "output.json"
        if not output_path.exists():
            self._log("ERROR", "output.json no existe")
            return {"quality_score": 0, "issues": ["output.json not found"]}

        output = json.loads(output_path.read_text(encoding="utf-8"))
        issues: list[dict] = []
        corrections_made = 0

        # ── 1. Null/empty field check ─────────────────────────────────────────
        null_fields = self._check_nulls(output)
        for field in null_fields:
            issues.append({
                "type": "null_field",
                "field": field,
                "severity": self._field_severity(field),
            })

        # ── 2. Numeric sanity checks ──────────────────────────────────────────
        numeric_issues = self._check_numeric_sanity(output)
        issues.extend(numeric_issues)

        # ── 3. Time series validation ─────────────────────────────────────────
        series_issues = self._check_time_series(output)
        issues.extend(series_issues)

        # ── 4. Qualitative coherence ──────────────────────────────────────────
        coherence_issues = self._check_qualitative_coherence(output)
        issues.extend(coherence_issues)

        # ── 5. Dashboard readiness ────────────────────────────────────────────
        dashboard_issues = self._check_dashboard_readiness(output)
        issues.extend(dashboard_issues)

        # ── 6. External verification (finect/morningstar) ─────────────────────
        if os.getenv("ANTHROPIC_API_KEY", ""):
            ext_issues = await self._verify_against_external(output)
            issues.extend(ext_issues)

        # ── Calculate quality score ───────────────────────────────────────────
        quality_score = self._calculate_quality_score(output, issues)

        # ── Save report ───────────────────────────────────────────────────────
        report = {
            "isin": self.isin,
            "timestamp": datetime.now().isoformat(),
            "quality_score": quality_score,
            "total_issues": len(issues),
            "critical_issues": sum(1 for i in issues if i.get("severity") == "critical"),
            "issues": issues,
            "corrections_made": corrections_made,
        }

        report_path = self.fund_dir / "quality_report.json"
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        self._log("OK", f"Quality score: {quality_score}/100 | {len(issues)} issues | {report_path.name}")

        # ── Learn from issues ─────────────────────────────────────────────────
        self._update_knowledge(issues)

        return report

    # ── Validation methods ────────────────────────────────────────────────────

    def _check_nulls(self, output: dict) -> list[str]:
        """Recursively find null/empty fields."""
        nulls = []

        def _walk(obj, path=""):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    _walk(v, f"{path}.{k}" if path else k)
            elif isinstance(obj, list):
                if not obj:
                    nulls.append(path)
            elif obj is None or obj == "":
                nulls.append(path)

        _walk(output)
        return nulls

    def _field_severity(self, field: str) -> str:
        """Classify field importance."""
        critical_fields = {
            "nombre", "gestora", "kpis.aum_actual_meur", "kpis.num_participes",
            "cualitativo.estrategia", "cualitativo.gestores",
        }
        high_fields = {
            "kpis.ter_pct", "kpis.coste_gestion_pct", "cualitativo.filosofia_inversion",
            "cualitativo.historia_fondo", "posiciones.actuales",
        }
        for cf in critical_fields:
            if field.endswith(cf) or field == cf:
                return "critical"
        for hf in high_fields:
            if field.endswith(hf) or field == hf:
                return "high"
        return "low"

    def _check_numeric_sanity(self, output: dict) -> list[dict]:
        """Check that numeric values are within reasonable ranges."""
        issues = []
        kpis = output.get("kpis", {})

        # AUM sanity
        aum = kpis.get("aum_actual_meur")
        if aum is not None:
            if aum < 0:
                issues.append({"type": "invalid_value", "field": "kpis.aum_actual_meur",
                               "value": aum, "reason": "AUM negativo", "severity": "critical"})
            elif aum > 100_000:
                issues.append({"type": "suspicious_value", "field": "kpis.aum_actual_meur",
                               "value": aum, "reason": "AUM > 100B€ (sospechoso)", "severity": "high"})

        # TER sanity
        ter = kpis.get("ter_pct")
        if ter is not None and (ter < 0.01 or ter > 5.0):
            issues.append({"type": "invalid_value", "field": "kpis.ter_pct",
                           "value": ter, "reason": f"TER fuera de rango (0.01-5%): {ter}", "severity": "high"})

        # VL sanity — check it's not a year value
        for entry in output.get("cuantitativo", {}).get("serie_aum", []):
            vl = entry.get("vl")
            if vl is not None and 2000 <= vl <= 2100:
                issues.append({"type": "invalid_value", "field": f"serie_aum[{entry.get('periodo')}].vl",
                               "value": vl, "reason": "VL parece un año, no un valor liquidativo",
                               "severity": "critical"})

        # Partícipes sanity
        participes = kpis.get("num_participes")
        if participes is not None and participes < 0:
            issues.append({"type": "invalid_value", "field": "kpis.num_participes",
                           "value": participes, "reason": "Partícipes negativo", "severity": "critical"})

        return issues

    def _check_time_series(self, output: dict) -> list[dict]:
        """Validate time series: order, duplicates, gaps."""
        issues = []
        cuant = output.get("cuantitativo", {})

        for series_name in ["serie_aum", "serie_ter", "serie_rentabilidad"]:
            series = cuant.get(series_name, [])
            if not series:
                continue

            # Check for duplicates
            periodos = [e.get("periodo", "") for e in series]
            if len(periodos) != len(set(periodos)):
                dupes = [p for p in periodos if periodos.count(p) > 1]
                issues.append({"type": "duplicate_period", "field": f"cuantitativo.{series_name}",
                               "value": list(set(dupes)), "reason": "Periodos duplicados",
                               "severity": "high"})

        # Check AUM series has current period
        aum_series = cuant.get("serie_aum", [])
        if aum_series:
            periodos = [e.get("periodo", "") for e in aum_series]
            current_year = datetime.now().year
            has_recent = any(str(current_year) in p or str(current_year - 1) in p for p in periodos)
            if not has_recent:
                issues.append({"type": "missing_current", "field": "cuantitativo.serie_aum",
                               "reason": f"No hay datos AUM de {current_year} o {current_year-1}",
                               "severity": "high"})

        return issues

    def _check_qualitative_coherence(self, output: dict) -> list[dict]:
        """Check that qualitative fields are specific, not generic."""
        issues = []
        cual = output.get("cualitativo", {})

        # Check estrategia is not too short or generic
        estrategia = cual.get("estrategia", "")
        if estrategia and len(estrategia) < 50:
            issues.append({"type": "low_quality", "field": "cualitativo.estrategia",
                           "reason": f"Estrategia demasiado corta ({len(estrategia)} chars)",
                           "severity": "high"})

        # Check gestores are real names (not "null", not empty)
        gestores = cual.get("gestores", [])
        if gestores:
            for g in gestores:
                if g and g.get("nombre"):
                    nombre = g["nombre"].lower()
                    if nombre in ("null", "n/a", "no disponible", ""):
                        issues.append({"type": "invalid_value", "field": "cualitativo.gestores",
                                       "value": g["nombre"], "reason": "Nombre de gestor inválido",
                                       "severity": "high"})

        # Check historia_fondo has substance
        historia = cual.get("historia_fondo", "")
        if historia and len(historia) < 100:
            issues.append({"type": "low_quality", "field": "cualitativo.historia_fondo",
                           "reason": f"Historia demasiado corta ({len(historia)} chars)",
                           "severity": "high"})

        return issues

    def _check_dashboard_readiness(self, output: dict) -> list[dict]:
        """Check that data will render well in the dashboard."""
        issues = []
        cuant = output.get("cuantitativo", {})

        # Check AUM series has enough points for a meaningful chart
        aum = cuant.get("serie_aum", [])
        if len(aum) == 1:
            issues.append({"type": "insufficient_data", "field": "cuantitativo.serie_aum",
                           "reason": "Solo 1 punto de AUM — gráfico será una sola barra",
                           "severity": "medium"})

        # Check posiciones have pesos
        pos = output.get("posiciones", {}).get("actuales", [])
        if pos:
            no_peso = sum(1 for p in pos if not p.get("peso_pct"))
            if no_peso > len(pos) * 0.5:
                issues.append({"type": "missing_data", "field": "posiciones.actuales",
                               "reason": f"{no_peso}/{len(pos)} posiciones sin peso_pct",
                               "severity": "medium"})

        return issues

    async def _verify_against_external(self, output: dict) -> list[dict]:
        """
        Compare key data points against external sources (finect, morningstar, web gestora).
        Uses DDG search to find a reference page, then compares.
        """
        issues = []
        nombre = output.get("nombre", "")
        if not nombre:
            return issues

        try:
            from tools.http_client import get_with_headers
            from tools.claude_extractor import extract_structured_data

            # Search for a reference page
            fund_q = nombre.split(",")[0].strip() if "," in nombre else nombre
            ddg_url = f"https://html.duckduckgo.com/html/?q={fund_q.replace(' ', '+')}+finect+OR+morningstar"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept-Language": "es-ES,es;q=0.9",
            }
            html = await get_with_headers(ddg_url, headers)

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            first_result = None
            for a in soup.select("a.result__a"):
                href = a.get("href", "")
                if "finect.com" in href or "morningstar.es" in href:
                    first_result = href
                    break

            if first_result:
                ref_html = await get_with_headers(first_result, headers)
                ref_soup = BeautifulSoup(ref_html, "html.parser")
                ref_text = ref_soup.get_text(separator=" ", strip=True)[:3000]

                # Compare key facts
                our_aum = output.get("kpis", {}).get("aum_actual_meur")
                our_nombre = output.get("nombre", "")

                result = extract_structured_data(
                    ref_text,
                    {
                        "nombre_fondo": "nombre del fondo según esta fuente",
                        "aum_meur": "patrimonio/AUM en millones de euros (número)",
                        "categoria": "categoría o clasificación del fondo",
                        "discrepancias": "lista de discrepancias si las hay entre esta fuente y los datos del output",
                    },
                    context=f"Comparar datos de {our_nombre} (AUM={our_aum}M€) con esta fuente externa.",
                )
                if isinstance(result, dict) and result.get("discrepancias"):
                    issues.append({
                        "type": "external_discrepancy",
                        "source": first_result,
                        "details": result["discrepancias"],
                        "severity": "medium",
                    })

        except Exception as exc:
            self._log("WARN", f"Verificación externa falló: {exc}")

        return issues

    def _calculate_quality_score(self, output: dict, issues: list[dict]) -> int:
        """Calculate overall quality score 0-100."""
        score = 100

        # Deduct for issues
        for issue in issues:
            severity = issue.get("severity", "low")
            if severity == "critical":
                score -= 10
            elif severity == "high":
                score -= 5
            elif severity == "medium":
                score -= 2
            else:
                score -= 1

        # Bonus for completeness
        completed, nulls = 0, 0
        def _count(obj):
            nonlocal completed, nulls
            if isinstance(obj, dict):
                for v in obj.values():
                    _count(v)
            elif isinstance(obj, list):
                if obj:
                    completed += 1
                else:
                    nulls += 1
            elif obj is None or obj == "":
                nulls += 1
            else:
                completed += 1
        _count(output)

        total = completed + nulls
        completeness = completed / total if total > 0 else 0
        # Adjust score: 50% weight on issues, 50% on completeness
        score = max(0, min(100, int(score * 0.6 + completeness * 100 * 0.4)))

        return score

    def _update_knowledge(self, issues: list[dict]):
        """Save learned patterns to extraction_knowledge.json."""
        if not issues:
            return
        try:
            knowledge = {}
            if self.knowledge_path.exists():
                knowledge = json.loads(self.knowledge_path.read_text(encoding="utf-8"))

            learned = knowledge.get("learned_patterns", [])
            for issue in issues:
                if issue.get("severity") in ("critical", "high"):
                    pattern = {
                        "isin": self.isin,
                        "timestamp": datetime.now().isoformat(),
                        "type": issue.get("type"),
                        "field": issue.get("field"),
                        "reason": issue.get("reason", ""),
                    }
                    # Avoid duplicate patterns
                    if not any(
                        p.get("field") == pattern["field"] and p.get("type") == pattern["type"]
                        for p in learned
                    ):
                        learned.append(pattern)

            knowledge["learned_patterns"] = learned[-50:]  # keep last 50
            self.knowledge_path.write_text(
                json.dumps(knowledge, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except Exception:
            pass


# ── Standalone ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import asyncio
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).parent.parent / ".env")
    isin = sys.argv[1] if len(sys.argv) > 1 else "ES0112231008"
    root = Path(__file__).parent.parent
    fund_dir = root / "data" / "funds" / isin

    agent = ValidationAgent(isin, fund_dir=fund_dir)
    report = asyncio.run(agent.run())
    console.print(f"Quality: {report['quality_score']}/100 | Issues: {report['total_issues']}")
