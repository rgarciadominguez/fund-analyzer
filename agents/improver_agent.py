"""
Improver Agent — Auto-mejora del sistema a partir de errores acumulados

Lee los meta_report.json de todos los fondos analizados, detecta patrones
de error recurrentes y propone (o aplica) mejoras concretas a los agentes.

Ciclo de aprendizaje:
  1. Recopila todos los meta_report.json + feedback.json del sistema
  2. Agrupa issues por tipo y frecuencia
  3. Llama a Claude con el historial de errores + código actual del agente
     para generar un patch explicado
  4. Guarda el patch propuesto en data/improvements/
  5. (Opcional --apply) aplica el patch automáticamente si supera umbral
     de confianza y pasa py_compile

Output:
  data/improvements/{timestamp}_report.json   ← análisis del ciclo
  data/improvements/{timestamp}_{agent}.patch ← patches propuestos
  data/improvements/knowledge_base.json       ← historial acumulado de mejoras

CLI:
  python -m agents.improver_agent               # solo analiza y propone
  python -m agents.improver_agent --apply       # apliza patches de alta confianza
  python -m agents.improver_agent --summary     # resumen del knowledge base
"""
import asyncio
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).parent.parent))

from tools.claude_extractor import extract_structured_data

# ── Constants ─────────────────────────────────────────────────────────────────

ROOT = Path(__file__).parent.parent
DATA_DIR = ROOT / "data" / "funds"
IMPROVEMENTS_DIR = ROOT / "data" / "improvements"
KB_PATH = IMPROVEMENTS_DIR / "knowledge_base.json"
LOG_PATH = ROOT / "progress.log"
AGENTS_DIR = ROOT / "agents"

# Umbral de confianza para aplicar patches automáticamente (0-100)
AUTO_APPLY_THRESHOLD = 85

# Agentes que pueden ser modificados automáticamente
PATCHABLE_AGENTS = {
    "cnmv_agent.py",
    "letters_agent.py",
    "readings_agent.py",
    "meta_agent.py",
    "analyst_agent.py",
}

# Categorías de issues y el agente responsable de cada una
ISSUE_TO_AGENT = {
    "mix_activos":           "cnmv_agent.py",
    "serie_aum":             "cnmv_agent.py",
    "posiciones":            "cnmv_agent.py",
    "gestores.*null":        "analyst_agent.py",
    "gestores.*vacío":       "analyst_agent.py",
    "cartas.*gestores":      "letters_agent.py",
    "sitemap":               "letters_agent.py",
    "analisis_externos":     "readings_agent.py",
    "lecturas":              "readings_agent.py",
    "dashboard":             "meta_agent.py",
    "completitud":           "analyst_agent.py",
}


class ImproverAgent:
    """
    Agente de auto-mejora: aprende de los errores acumulados en todos los
    meta_report.json y propone/aplica mejoras a los agentes del sistema.
    """

    def __init__(self, mode: Literal["propose", "apply", "summary"] = "propose"):
        self.mode = mode
        self.ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        IMPROVEMENTS_DIR.mkdir(parents=True, exist_ok=True)

    def _log(self, level: str, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] [IMPROVER] [{level}] {msg}"
        safe = line.encode("cp1252", errors="replace").decode("cp1252")
        print(safe, flush=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    # ── Data collection ───────────────────────────────────────────────────────

    def _collect_all_issues(self) -> list[dict]:
        """Lee todos los meta_report.json y feedback.json del sistema."""
        all_issues = []

        for fund_dir in sorted(DATA_DIR.iterdir()):
            if not fund_dir.is_dir():
                continue
            isin = fund_dir.name

            # meta_report.json
            meta_path = fund_dir / "meta_report.json"
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    for issue in meta.get("issues", []):
                        all_issues.append({
                            "isin":      isin,
                            "nombre":    meta.get("nombre", isin),
                            "source":    "meta_report",
                            "timestamp": meta.get("timestamp", ""),
                            "issue":     issue,
                            "agent":     self._issue_to_agent(issue),
                        })
                except Exception:
                    pass

            # feedback.json (usuario)
            fb_path = fund_dir / "feedback.json"
            if fb_path.exists():
                try:
                    fb_data = json.loads(fb_path.read_text(encoding="utf-8"))
                    if isinstance(fb_data, dict):
                        fb_data = [fb_data]
                    for fb in fb_data:
                        respuestas = fb.get("respuestas", {})
                        for key, text in respuestas.items():
                            if text.strip():
                                all_issues.append({
                                    "isin":      isin,
                                    "nombre":    fb.get("fondo", isin),
                                    "source":    f"feedback_usuario:{key}",
                                    "timestamp": fb.get("timestamp", ""),
                                    "issue":     text,
                                    "agent":     None,
                                })
                        # issues directos en feedback
                        for iss in fb.get("issues", []):
                            all_issues.append({
                                "isin":      isin,
                                "nombre":    fb.get("fondo", isin),
                                "source":    "feedback_usuario:issue",
                                "timestamp": fb.get("timestamp", ""),
                                "issue":     iss,
                                "agent":     self._issue_to_agent(iss),
                            })
                except Exception:
                    pass

        return all_issues

    def _issue_to_agent(self, issue: str) -> str | None:
        """Infiere el agente responsable del issue."""
        issue_lower = issue.lower()
        for pattern, agent in ISSUE_TO_AGENT.items():
            if re.search(pattern, issue_lower):
                return agent
        return None

    def _aggregate_by_agent(self, issues: list[dict]) -> dict[str, list[dict]]:
        """Agrupa issues por agente responsable."""
        by_agent: dict[str, list[dict]] = {}
        for iss in issues:
            agent = iss.get("agent") or "unknown"
            by_agent.setdefault(agent, []).append(iss)
        return by_agent

    def _frequency_analysis(self, issues: list[dict]) -> list[dict]:
        """Cuenta frecuencia de cada tipo de issue."""
        counter: dict[str, dict] = {}
        for iss in issues:
            key = re.sub(r'\b(ES|LU|IE)\w+\b', 'ISIN', iss["issue"])
            key = re.sub(r'\d{4}', 'YYYY', key)[:80]
            if key not in counter:
                counter[key] = {
                    "issue_template": key,
                    "count":          0,
                    "agent":          iss.get("agent"),
                    "isins":          [],
                    "examples":       [],
                }
            counter[key]["count"] += 1
            if iss["isin"] not in counter[key]["isins"]:
                counter[key]["isins"].append(iss["isin"])
            if len(counter[key]["examples"]) < 3:
                counter[key]["examples"].append(iss["issue"])

        return sorted(counter.values(), key=lambda x: x["count"], reverse=True)

    # ── Knowledge base ────────────────────────────────────────────────────────

    def _load_kb(self) -> dict:
        if KB_PATH.exists():
            try:
                return json.loads(KB_PATH.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"cycles": [], "applied_patches": [], "learned_patterns": []}

    def _save_kb(self, kb: dict):
        KB_PATH.write_text(json.dumps(kb, ensure_ascii=False, indent=2), encoding="utf-8")

    def _already_fixed(self, issue_template: str, kb: dict) -> bool:
        """Comprueba si este tipo de issue ya fue corregido en una iteración anterior."""
        for patch in kb.get("applied_patches", []):
            if issue_template[:40] in patch.get("issue_template", ""):
                return True
        return False

    # ── Claude-powered improvement proposal ──────────────────────────────────

    def _propose_fix(self, agent_file: str, issues_for_agent: list[dict]) -> dict:
        """
        Llama a Claude con:
          - código actual del agente
          - lista de issues recurrentes
          - historial de patches anteriores
        Para generar una propuesta de mejora concreta.
        """
        agent_path = AGENTS_DIR / agent_file
        if not agent_path.exists():
            return {}

        agent_code = agent_path.read_text(encoding="utf-8", errors="replace")
        # Truncar si es muy largo (max 8000 chars — Claude context)
        if len(agent_code) > 8000:
            agent_code = agent_code[:8000] + "\n... [truncado]"

        issue_texts = [iss["issue"] for iss in issues_for_agent[:10]]
        issue_summary = "\n".join(f"- {t}" for t in issue_texts)

        schema = {
            "diagnostico": "análisis de la causa raíz de los errores en 2-3 párrafos",
            "propuesta": "descripción de la mejora propuesta en términos concretos",
            "old_code": "fragmento exacto del código actual que tiene el bug o limitación (copy-paste del código)",
            "new_code": "fragmento exacto del código mejorado (listo para hacer replace directo)",
            "confianza": "puntuación 0-100 de confianza en que el fix es correcto",
            "riesgos": "posibles efectos secundarios o casos en que el fix podría fallar",
            "test_suggestion": "cómo testear que el fix funciona correctamente",
        }

        context = (
            f"Eres un experto en Python mejorando el agente '{agent_file}' de un sistema "
            f"multi-agente de análisis de fondos de inversión españoles e internacionales.\n\n"
            f"ERRORES RECURRENTES DETECTADOS EN PRODUCCIÓN:\n{issue_summary}\n\n"
            f"CÓDIGO ACTUAL DEL AGENTE:\n```python\n{agent_code}\n```"
        )

        try:
            result = extract_structured_data(
                f"Analiza los errores y propón una mejora concreta al código del agente.",
                schema,
                context=context,
            )
            result["agent"] = agent_file
            result["issues_count"] = len(issues_for_agent)
            result["timestamp"] = datetime.now().isoformat()
            return result
        except Exception as exc:
            self._log("WARN", f"Claude error para {agent_file}: {exc}")
            return {}

    # ── Patch application ─────────────────────────────────────────────────────

    def _try_apply_patch(self, proposal: dict) -> bool:
        """
        Aplica un patch propuesto por Claude si:
          1. Confianza >= AUTO_APPLY_THRESHOLD
          2. old_code existe en el fichero actual
          3. El fichero resultado compila sin errores
        """
        agent_file = proposal.get("agent", "")
        agent_path = AGENTS_DIR / agent_file
        if not agent_path.exists() or agent_file not in PATCHABLE_AGENTS:
            return False

        confidence = int(proposal.get("confianza", 0))
        if confidence < AUTO_APPLY_THRESHOLD:
            self._log("INFO", f"Patch {agent_file} confianza {confidence}% < {AUTO_APPLY_THRESHOLD}% — solo propuesta")
            return False

        old_code = proposal.get("old_code", "").strip()
        new_code = proposal.get("new_code", "").strip()

        if not old_code or not new_code or old_code == new_code:
            self._log("WARN", f"Patch {agent_file}: old_code o new_code vacíos/iguales — skip")
            return False

        current = agent_path.read_text(encoding="utf-8")
        if old_code not in current:
            self._log("WARN", f"Patch {agent_file}: old_code no encontrado en fichero actual — skip")
            return False

        # Hacer backup
        backup_path = IMPROVEMENTS_DIR / f"{self.ts}_{agent_file}.bak"
        backup_path.write_text(current, encoding="utf-8")

        # Aplicar
        updated = current.replace(old_code, new_code, 1)
        agent_path.write_text(updated, encoding="utf-8")

        # Verificar que compila
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", str(agent_path)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            self._log("ERROR", f"Patch {agent_file} no compila — revertiendo: {result.stderr}")
            agent_path.write_text(current, encoding="utf-8")  # revert
            return False

        self._log("OK", f"Patch aplicado a {agent_file} (confianza {confidence}%)")
        return True

    # ── Main run ──────────────────────────────────────────────────────────────

    async def run(self) -> dict:
        self._log("START", f"ImproverAgent — modo={self.mode}")
        kb = self._load_kb()

        if self.mode == "summary":
            return self._print_summary(kb)

        # 1. Recopilar todos los issues del sistema
        all_issues = self._collect_all_issues()
        self._log("INFO", f"Issues recopilados: {len(all_issues)} de {len(list(DATA_DIR.iterdir()))} fondos")

        if not all_issues:
            self._log("INFO", "Sin issues — sistema limpio")
            return {"status": "clean", "issues_found": 0}

        # 2. Análisis de frecuencia
        freq = self._frequency_analysis(all_issues)
        by_agent = self._aggregate_by_agent(all_issues)

        # 3. Proponer fixes para cada agente con issues
        proposals = []
        agents_with_issues = [a for a in by_agent if a != "unknown" and a in PATCHABLE_AGENTS]

        for agent_file in agents_with_issues:
            issues_for_agent = by_agent[agent_file]
            # Skip si ya fue corregido
            sample_issue = issues_for_agent[0]["issue"][:40]
            if self._already_fixed(sample_issue, kb):
                self._log("INFO", f"{agent_file}: issues ya corregidos previamente — skip")
                continue

            self._log("INFO", f"Analizando {agent_file} ({len(issues_for_agent)} issues)...")
            proposal = self._propose_fix(agent_file, issues_for_agent)
            if proposal:
                proposals.append(proposal)
                # Guardar patch propuesto
                patch_path = IMPROVEMENTS_DIR / f"{self.ts}_{agent_file}.json"
                patch_path.write_text(json.dumps(proposal, ensure_ascii=False, indent=2), encoding="utf-8")
                self._log("INFO", f"Propuesta guardada: {patch_path.name}")

        # 4. Aplicar patches si modo=apply
        applied = []
        if self.mode == "apply":
            for proposal in proposals:
                if self._try_apply_patch(proposal):
                    applied.append(proposal["agent"])
                    kb["applied_patches"].append({
                        "timestamp":      proposal["timestamp"],
                        "agent":          proposal["agent"],
                        "issue_template": freq[0]["issue_template"] if freq else "",
                        "confianza":      proposal.get("confianza"),
                        "diagnostico":    proposal.get("diagnostico", "")[:200],
                    })

        # 5. Registrar ciclo en knowledge base
        cycle = {
            "timestamp":         datetime.now().isoformat(),
            "mode":              self.mode,
            "total_issues":      len(all_issues),
            "agents_analyzed":   agents_with_issues,
            "proposals":         len(proposals),
            "applied":           applied,
            "top_issues":        [f["issue_template"] for f in freq[:5]],
        }
        kb["cycles"].append(cycle)

        # Actualizar learned patterns
        for f in freq[:10]:
            pattern = f["issue_template"]
            existing = [p for p in kb["learned_patterns"] if p["pattern"] == pattern]
            if existing:
                existing[0]["count"] += f["count"]
                existing[0]["last_seen"] = datetime.now().isoformat()
            else:
                kb["learned_patterns"].append({
                    "pattern":    pattern,
                    "agent":      f["agent"],
                    "count":      f["count"],
                    "first_seen": datetime.now().isoformat(),
                    "last_seen":  datetime.now().isoformat(),
                })

        self._save_kb(kb)

        # 6. Guardar reporte del ciclo
        report = {
            "timestamp":        datetime.now().isoformat(),
            "cycle":            cycle,
            "frequency_top10":  freq[:10],
            "proposals":        proposals,
        }
        report_path = IMPROVEMENTS_DIR / f"{self.ts}_report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

        self._print_cycle_report(cycle, freq, proposals)
        return report

    def _print_cycle_report(self, cycle: dict, freq: list, proposals: list):
        print(f"\n{'='*60}")
        print(f"IMPROVER AGENT — Ciclo {self.ts}")
        print(f"{'='*60}")
        print(f"  Issues totales analizados: {cycle['total_issues']}")
        print(f"  Agentes con issues:        {', '.join(cycle['agents_analyzed']) or 'ninguno'}")
        print(f"  Propuestas generadas:      {cycle['proposals']}")
        print(f"  Patches aplicados:         {', '.join(cycle['applied']) or 'ninguno'}")
        print(f"\nTop issues más frecuentes:")
        for f in freq[:5]:
            print(f"  [{f['count']}x] {f['agent'] or '?'}: {f['issue_template'][:70]}")
        if proposals:
            print(f"\nPropuestas de mejora:")
            for p in proposals:
                conf = p.get('confianza', '?')
                print(f"  {p['agent']} (confianza {conf}%): {str(p.get('propuesta',''))[:80]}")
        print(f"\nReporte completo: data/improvements/{self.ts}_report.json")
        print(f"{'='*60}")

    def _print_summary(self, kb: dict) -> dict:
        cycles = kb.get("cycles", [])
        patterns = kb.get("learned_patterns", [])
        applied = kb.get("applied_patches", [])
        print(f"\n{'='*60}")
        print(f"IMPROVER AGENT — Knowledge Base Summary")
        print(f"{'='*60}")
        print(f"  Ciclos ejecutados:      {len(cycles)}")
        print(f"  Patches aplicados:      {len(applied)}")
        print(f"  Patrones aprendidos:    {len(patterns)}")
        if patterns:
            print(f"\nPatrones más frecuentes:")
            for p in sorted(patterns, key=lambda x: x['count'], reverse=True)[:8]:
                print(f"  [{p['count']}x] {p['agent'] or '?'}: {p['pattern'][:70]}")
        if applied:
            print(f"\nÚltimos patches aplicados:")
            for p in applied[-3:]:
                print(f"  {p['timestamp'][:10]} {p['agent']} (confianza {p['confianza']}%)")
        print(f"{'='*60}")
        return {"cycles": len(cycles), "patterns": len(patterns), "applied": len(applied)}


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")

    parser = argparse.ArgumentParser(description="Improver Agent — auto-mejora del sistema")
    parser.add_argument("--apply",   action="store_true", help="Aplicar patches de alta confianza automáticamente")
    parser.add_argument("--summary", action="store_true", help="Mostrar resumen del knowledge base")
    args = parser.parse_args()

    mode = "apply" if args.apply else ("summary" if args.summary else "propose")
    agent = ImproverAgent(mode=mode)
    asyncio.run(agent.run())
