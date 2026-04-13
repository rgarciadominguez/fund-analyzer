"""
Dashboard Quality Agent — motor de reglas declarativo.

Lee `data/quality_rules.json` y evalúa el output.json del fondo contra cada regla.
Genera `quality_report.json` con la lista de fallos detectados.

NO usa scores ni umbrales. Solo emite fallos. Cada fallo lleva:
  - seccion
  - problema (texto humano-legible)
  - agente_responsable (para que el orchestrator sepa a quién reagentar)
  - accion (qué hacer para corregirlo)

Las reglas son declarativas (JSON). Cada regla tiene un `check_type` que el motor
sabe interpretar. Para añadir reglas nuevas: editar `data/quality_rules.json`,
no este código.

Usage:
    python -m agents.dashboard_quality_agent ES0112231008
"""
import json
import re
import sys
from datetime import datetime
from pathlib import Path

from rich.console import Console
from rich.table import Table

console = Console()
ROOT = Path(__file__).parent.parent
RULES_PATH = ROOT / "data" / "quality_rules.json"


# ═══════════════════════════════════════════════════════════════
# Helpers de acceso a campos anidados con notación tipo "a.b.c[0].d"
# ═══════════════════════════════════════════════════════════════

_INDEX_RX = re.compile(r"^([^\[]+)\[(\d+)\]$")


def _get_nested(data: dict, path: str):
    """Obtiene un valor anidado de un dict usando notación 'a.b.c[0].d'.
    Devuelve None si cualquier paso del camino no existe."""
    if not path:
        return None
    cur = data
    for part in path.split("."):
        if cur is None:
            return None
        m = _INDEX_RX.match(part)
        if m:
            key = m.group(1)
            idx = int(m.group(2))
            if not isinstance(cur, dict) or key not in cur:
                return None
            arr = cur.get(key)
            if not isinstance(arr, list) or idx >= len(arr):
                return None
            cur = arr[idx]
        else:
            if not isinstance(cur, dict):
                return None
            cur = cur.get(part)
    return cur


# ═══════════════════════════════════════════════════════════════
# Validadores por check_type
# ═══════════════════════════════════════════════════════════════

# Regex para "cifras concretas" en texto (porcentajes, importes, conteos)
_CIFRAS_RX = re.compile(r"\d+[.,]\d+\s*%|\d+\s*M€|\d+\s*partícipes|\d+\s*posiciones|\d+\s*años")


def _check_min_chars(rule: dict, data: dict) -> tuple[bool, dict]:
    """Texto en `field_path` debe tener al menos `value` caracteres."""
    text = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(text, str):
        text = ""
    actual = len(text)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_min_count_array(rule: dict, data: dict) -> tuple[bool, dict]:
    """Lista en `field_path` debe tener al menos `value` elementos."""
    arr = _get_nested(data, rule["field_path"]) or []
    if not isinstance(arr, list):
        arr = []
    actual = len(arr)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_min_bold_headers(rule: dict, data: dict) -> tuple[bool, dict]:
    """Texto debe tener al menos `value` headers en negrita (líneas con **...**).
    Cuenta líneas que son completamente **bold**, no solo apariciones de **."""
    text = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(text, str):
        text = ""
    headers = 0
    for para in text.split("\n\n"):
        ps = para.strip()
        if ps.startswith("**") and ps.endswith("**") and ps.count("**") == 2 and len(ps) > 4:
            headers += 1
        # También contar headers inline al inicio de párrafo
        elif ps.startswith("**") and "**" in ps[2:]:
            # patrón "**Título**: contenido"
            close = ps.find("**", 2)
            if close > 2:
                headers += 1
    actual = headers
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_min_cifras(rule: dict, data: dict) -> tuple[bool, dict]:
    """Texto debe contener al menos `value` cifras concretas (regex)."""
    text = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(text, str):
        text = ""
    matches = _CIFRAS_RX.findall(text)
    actual = len(matches)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_must_contain_fund_name(rule: dict, data: dict) -> tuple[bool, dict]:
    """El texto debe mencionar el nombre del fondo (o su primera parte antes de coma)."""
    text = _get_nested(data, rule["field_path"]) or ""
    fund_name = data.get("nombre", "") or ""
    if not isinstance(text, str) or not fund_name:
        return True, {"fund_name": fund_name}  # no podemos comprobar
    short = fund_name.split(",")[0].strip().lower()
    text_l = text.lower()
    ok = (short and short in text_l) or fund_name.lower() in text_l
    return ok, {"fund_name": fund_name}


def _check_field_present(rule: dict, data: dict) -> tuple[bool, dict]:
    """El campo `field_path` existe y no está vacío."""
    val = _get_nested(data, rule["field_path"])
    ok = bool(val) and val not in ("", [], {}, None)
    return ok, {}


def _check_nested_field_present(rule: dict, data: dict) -> tuple[bool, dict]:
    """Igual que field_present, pero formatea con info del lead manager."""
    val = _get_nested(data, rule["field_path"])
    ok = bool(val) and val not in ("", [], {}, None)
    # Para problema_template con {lead_name}
    perfiles = _get_nested(data, "analyst_synthesis.gestores.perfiles") or []
    lead_name = perfiles[0].get("nombre", "") if perfiles else ""
    return ok, {"lead_name": lead_name}


def _check_nested_array_min(rule: dict, data: dict) -> tuple[bool, dict]:
    """Lista anidada (ej. perfiles[0].decisiones_clave) debe tener N items."""
    val = _get_nested(data, rule["field_path"]) or []
    if not isinstance(val, list):
        val = []
    actual = len(val)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_any_field_present(rule: dict, data: dict) -> tuple[bool, dict]:
    """Al menos uno de los `field_paths` debe estar presente y no vacío."""
    for path in rule.get("field_paths", []):
        val = _get_nested(data, path)
        if val and val not in ("", [], {}, None):
            return True, {}
    return False, {}


def _check_no_bold_headers(rule: dict, data: dict) -> tuple[bool, dict]:
    """Text must NOT have standalone **bold** headers (subsections). Pure prose only."""
    text = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(text, str):
        return True, {"actual": 0}
    headers = 0
    for para in text.split("\n\n"):
        ps = para.strip()
        if ps.startswith("**") and ps.endswith("**") and ps.count("**") == 2 and len(ps) > 4:
            headers += 1
    ok = headers <= rule.get("value", 0)  # value = max allowed headers (usually 0)
    return ok, {"actual": headers, "expected": rule.get("value", 0)}


def _check_min_chars_nested(rule: dict, data: dict) -> tuple[bool, dict]:
    """Nested field (e.g. perfiles[0].trayectoria) must have min chars."""
    val = _get_nested(data, rule["field_path"]) or ""
    if not isinstance(val, str):
        val = ""
    actual = len(val)
    expected = rule["value"]
    ok = actual >= expected
    # Get name for template
    perfiles = _get_nested(data, "analyst_synthesis.gestores.perfiles") or []
    lead_name = perfiles[0].get("nombre", "") if perfiles else ""
    return ok, {"actual": actual, "expected": expected, "lead_name": lead_name}


def _check_has_quotes(rule: dict, data: dict) -> tuple[bool, dict]:
    """Section must have quotes array with min items."""
    val = _get_nested(data, rule["field_path"]) or []
    if not isinstance(val, list):
        val = []
    actual = len(val)
    expected = rule["value"]
    ok = actual >= expected
    return ok, {"actual": actual, "expected": expected}


def _check_has_field_in_hitos(rule: dict, data: dict) -> tuple[bool, dict]:
    """Each hito in array must have specific fields (e.g. contexto_mercado, decisiones, resultado)."""
    hitos = _get_nested(data, rule["field_path"]) or []
    if not isinstance(hitos, list) or not hitos:
        return False, {"actual": 0, "expected": rule.get("value", 1)}
    required_field = rule.get("required_field", "")
    count_with = sum(1 for h in hitos if isinstance(h, dict) and h.get(required_field))
    ok = count_with >= rule.get("value", 1)
    return ok, {"actual": count_with, "expected": rule.get("value", 1)}


# Registro de validadores
CHECK_REGISTRY = {
    "min_chars": _check_min_chars,
    "min_count_array": _check_min_count_array,
    "no_bold_headers": _check_no_bold_headers,
    "min_chars_nested": _check_min_chars_nested,
    "has_quotes": _check_has_quotes,
    "has_field_in_hitos": _check_has_field_in_hitos,
    "min_bold_headers": _check_min_bold_headers,
    "min_cifras": _check_min_cifras,
    "must_contain_fund_name": _check_must_contain_fund_name,
    "field_present": _check_field_present,
    "nested_field_present": _check_nested_field_present,
    "nested_array_min": _check_nested_array_min,
    "any_field_present": _check_any_field_present,
}


# ═══════════════════════════════════════════════════════════════
# Excepciones (applies_when)
# ═══════════════════════════════════════════════════════════════

def _rule_applies(rule: dict, data: dict) -> bool:
    """Evalúa la condición `applies_when` de una regla. Si no hay condición, aplica."""
    cond = rule.get("applies_when")
    if not cond:
        return True

    fund_name = (data.get("nombre", "") or "").lower()

    # fund_name_contains: aplica solo si el nombre contiene alguno de estos
    if "fund_name_contains" in cond:
        if not any(s.lower() in fund_name for s in cond["fund_name_contains"]):
            return False

    # fund_name_not_contains: NO aplica si el nombre contiene alguno
    if "fund_name_not_contains" in cond:
        if any(s.lower() in fund_name for s in cond["fund_name_not_contains"]):
            return False

    return True


# ═══════════════════════════════════════════════════════════════
# Agente principal
# ═══════════════════════════════════════════════════════════════

class DashboardQualityAgent:

    def __init__(self, isin: str):
        self.isin = isin.strip().upper()
        self.fund_dir = ROOT / "data" / "funds" / self.isin
        self.rules = self._load_rules()

    def _load_rules(self) -> dict:
        if not RULES_PATH.exists():
            console.print(f"[red]ERROR: no existe {RULES_PATH}[/red]")
            return {"rules": [], "sections": []}
        return json.loads(RULES_PATH.read_text(encoding="utf-8"))

    def run(self) -> dict:
        """Evalúa output.json contra todas las reglas. Devuelve report."""
        output_path = self.fund_dir / "output.json"
        if not output_path.exists():
            return {
                "fund": self.isin,
                "nombre": "",
                "fallos": [{
                    "seccion": "global",
                    "problema": "No existe output.json",
                    "agente_responsable": "orchestrator",
                    "accion": "Ejecutar pipeline completo"
                }],
                "secciones_evaluadas": [],
                "evaluado_at": datetime.now().isoformat(),
            }

        data = json.loads(output_path.read_text(encoding="utf-8"))
        fallos = []

        for rule in self.rules.get("rules", []):
            if not _rule_applies(rule, data):
                continue
            check_type = rule.get("check_type")
            checker = CHECK_REGISTRY.get(check_type)
            if not checker:
                console.print(f"[yellow]WARN: check_type desconocido: {check_type} (regla {rule.get('id')})[/yellow]")
                continue

            try:
                ok, ctx = checker(rule, data)
            except Exception as exc:
                console.print(f"[red]ERROR ejecutando regla {rule.get('id')}: {exc}[/red]")
                continue

            if not ok:
                # Formatear problema y accion con el contexto del check
                fmt_ctx = dict(ctx)
                fmt_ctx.setdefault("section", rule.get("section", ""))
                problema = rule.get("problema_template", "Fallo en regla {id}").format(
                    id=rule.get("id", ""), **fmt_ctx)
                accion = rule.get("accion_template", "").format(
                    id=rule.get("id", ""), **fmt_ctx)
                fallos.append({
                    "regla_id": rule.get("id"),
                    "seccion": rule.get("section", "global"),
                    "fail_type": rule.get("fail_type", "estructura"),
                    "problema": problema,
                    "agente_responsable": rule.get("agente_responsable", "analyst_agent"),
                    "accion": accion,
                })

        # Compute scoring metrics
        total_reglas = len([r for r in self.rules.get("rules", []) if _rule_applies(r, data)])
        fallos_estructura = sum(1 for f in fallos if f.get("fail_type") in ("estructura", "content"))
        fallos_scarcity = sum(1 for f in fallos if f.get("fail_type") == "scarcity")
        reglas_ok = total_reglas - len(fallos)
        aceptable = fallos_estructura == 0
        score_display = f"{reglas_ok}/{total_reglas} reglas OK"
        if fallos_scarcity > 0:
            score_display += f" ({fallos_scarcity} pendientes de datos)"

        report = {
            "fund": self.isin,
            "nombre": data.get("nombre", ""),
            "fallos": fallos,
            "fallos_estructura": fallos_estructura,
            "fallos_scarcity": fallos_scarcity,
            "total_reglas": total_reglas,
            "reglas_ok": reglas_ok,
            "aceptable": aceptable,
            "score_display": score_display,
            "secciones_evaluadas": self.rules.get("sections", []),
            "evaluado_at": datetime.now().isoformat(),
        }

        # Persistir y mostrar
        report_path = self.fund_dir / "quality_report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        self._print_report(report)

        return report

    def _print_report(self, report: dict):
        """Tabla rich con count de fallos por sección."""
        # Agrupar fallos por sección
        per_section: dict[str, int] = {}
        for f in report["fallos"]:
            sec = f.get("seccion", "global")
            per_section[sec] = per_section.get(sec, 0) + 1

        table = Table(
            title=f"Quality Report — {report['nombre']} ({report['fund']})",
            show_header=True,
            border_style="cyan",
        )
        table.add_column("Sección", width=22)
        table.add_column("Fallos", width=8, justify="right")
        table.add_column("Estado", width=30)

        for sec in report.get("secciones_evaluadas", []):
            n = per_section.get(sec, 0)
            estado = "[green]OK" if n == 0 else "[yellow]REVISAR"
            table.add_row(sec, str(n), estado)

        # Sección 'global' (fallos sin sección concreta)
        if per_section.get("global", 0):
            table.add_row("global", str(per_section["global"]), "[red]ERROR")

        table.add_row("", "", "")
        total = len(report["fallos"])
        total_color = "green" if total == 0 else "yellow"
        table.add_row("[bold]TOTAL", f"[bold {total_color}]{total}",
                      f"[bold {total_color}]{'OK' if total == 0 else 'REVISAR'}")

        # Show scoring summary
        aceptable = report.get("aceptable", False)
        score_display = report.get("score_display", "")
        accept_color = "green" if aceptable else "yellow"
        table.add_row(
            f"[bold {accept_color}]Score",
            f"[bold {accept_color}]{score_display}",
            f"[bold {accept_color}]{'ACEPTABLE' if aceptable else 'NO ACEPTABLE'}",
        )

        console.print(table)

        if report["fallos"]:
            console.print(f"\n[bold]Fallos ({len(report['fallos'])}):[/bold]")
            for f in report["fallos"][:15]:
                console.print(
                    f"  [yellow]•[/yellow] [{f['seccion']}] {f['problema'][:90]}"
                )
                console.print(
                    f"    [dim]→ {f['agente_responsable']}: {f['accion'][:90]}[/dim]"
                )


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    isin = sys.argv[1] if len(sys.argv) > 1 else "ES0112231008"
    agent = DashboardQualityAgent(isin)
    agent.run()
