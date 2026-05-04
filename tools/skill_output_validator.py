"""
skill_output_validator.py — Validador hard+soft+anti-invención del output del analyst.

Compara el `output.json` producido tras la skill `analyst-cowork` (consume-cowork)
contra el baseline guardado en el git tag `v1-api-stable` (analyst legacy con API).

3 niveles de checks:
- HARD: KPIs idénticos, top posiciones, gestores, schema completo (FAIL si fallan).
- SOFT: longitud por sección ±20%, coverage, campos opcionales (WARN si fallan).
- ANTI-INVENCIÓN: cada entidad, cifra y cita en analyst_synthesis tiene respaldo
  en al menos uno de los 5 inputs del bundle (WARN si hay invenciones).

CLI:
    python -m tools.skill_output_validator ES0112231008
    python -m tools.skill_output_validator ES0112231008 --baseline-tag v1-api-stable
    python -m tools.skill_output_validator ES0112231008 --output-report report.json
    python -m tools.skill_output_validator ES0112231008 --skip-anti-invencion

Verdict final:
- pass:  todos los hard pasan, soft warns ≤2, anti-invención clean.
- warn:  hard pasan, pero soft >2 warns o anti-invención flags ≥1 ≤5.
- fail:  algún hard falla o anti-invención flags >5.

Exit codes: 0=pass, 1=warn, 2=fail.

Diseñado para ejecutarse durante el smoke test de Fase 1.6 con AVANTAGE
(ES0112231008) y TROJAN (IE00B6T42S66).
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

REQUIRED_ANALYST_SECTIONS = [
    "resumen", "historia", "gestores", "evolucion",
    "estrategia", "cartera", "fuentes_externas", "documentos",
]

KPIS_HARD = ["aum_actual_meur", "num_participes", "ter_pct", "anio_creacion", "divisa"]
KPIS_SOFT = ["volatilidad_pct", "coste_gestion_pct", "depositario", "perfil_riesgo"]

# Tolerancias
AUM_REL_TOLERANCE = 0.02       # 2% diferencia aceptable (re-extracción puede variar)
TER_ABS_TOLERANCE = 0.05       # ±0.05 puntos pct (redondeo)
PESO_PCT_TOLERANCE = 0.5       # ±0.5pp en pesos de posiciones
LENGTH_REL_TOLERANCE = 0.20    # ±20% en longitud por sección
TOTAL_LENGTH_REL_TOLERANCE = 0.15  # ±15% en longitud total

# Soft warns máximas antes de degradar a "warn" final
SOFT_WARN_BUDGET = 2

# Anti-invención
ANTI_INV_MAX_FLAGS_FOR_WARN = 5
# Proper noun regex (v3): mínimo 2 palabras capitalizadas, separadas por espacios HORIZONTALES
# (espacio o tab) — NO newlines. Esto evita que el regex cruce boundaries entre fields del JSON
# y cree entidades compuestas artificiales como "POSITIVO Combinación", "DFI Actualización",
# "Substack Salud Financiera Análisis", etc.
PROPER_NOUN_RE = re.compile(
    r"\b[A-ZÁÉÍÓÚÑ][a-záéíóúñA-ZÁÉÍÓÚÑ]+(?:[ \t]+[A-ZÁÉÍÓÚÑ][a-záéíóúñA-ZÁÉÍÓÚÑ]+){1,4}\b"
)
# Quote regex: cita entre comillas dobles o simples.
# v3.1: requiere que la cita empiece por mayúscula o letra (no por coma/punto/espacio)
# para evitar capturar fragmentos sueltos como ", una empresa" o ", cursos técnicos"
# que vienen de paréntesis o fragmentos de prosa con comillas mal balanceadas.
QUOTE_RE = re.compile(r'"([A-ZÁÉÍÓÚÑa-záéíóúñ][^"\n]{14,200})"|\'([A-ZÁÉÍÓÚÑa-záéíóúñ][^\'\n]{14,200})\'')
NUMERIC_RE = re.compile(r"\b\d+(?:[\.,]\d+)?\s*(?:%|€|millones|mil|MEUR|M€|M\$|MUSD|MGBP)?\b")

# Stopwords / nombres comunes que no son entidades sospechosas
ENTITY_STOPLIST = {
    # Términos geográficos genéricos
    "España", "Europa", "Estados Unidos", "EE.UU.", "EE UU", "USA", "Reino Unido", "UK",
    "China", "Asia", "Latinoamérica", "Iberoamérica", "Mediterráneo", "Norteamérica",
    "Europa Occidental", "Europa Central", "Latinoamerica", "Sudamérica",
    "República Argentina", "Republic Argentina",
    # Conceptos financieros generales
    "Renta Variable", "Renta Fija", "Patrimonio", "Cartera", "Fondo", "Sub-Fondo",
    "Anexo", "Informe", "Folleto", "KIID", "Annual Report", "Prospectus",
    "Inversión", "Valor", "Activos", "Capital", "Mercado", "Mercados",
    "Plan de Pensiones", "Plan Pensiones", "Pure Equity",
    "Class A", "Class B", "Clase A", "Clase B",
    "Bono USD", "Bono EUR", "Government Argentina", "Acción Consumer",
    # Reguladores y entidades neutras
    "CNMV", "CSSF", "AMF", "CBI", "Bundesanzeiger", "ESMA", "SEC",
    "Morningstar", "Bloomberg", "Reuters", "Citywire", "Lipper",
    "Capital Radio", "Tu Dinero Nunca Duerme", "Estrategias de Inversión",
    "Value Investing FM", "Cinco Días", "El Confidencial",
    # Indices y benchmarks comunes
    "Ibex", "Ibex 35", "Euro Stoxx", "Euro Stoxx 50", "Stoxx 600",
    "S&P 500", "Nasdaq", "Nasdaq Composite", "MSCI ACWI", "MSCI World",
    "MSCI All Country", "DAX", "FTSE", "CAC", "Hang Seng", "Bovespa", "Kospi",
    # Frecuentes en cualquier informe
    "Bolsa", "Acciones", "Bonos", "Dividendos", "Rentabilidad", "Volatilidad",
    "Trimestre", "Semestre", "Año", "Anual", "Semestral", "Trimestral",
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
    # Conceptos de gestión
    "Skin", "Track", "Track Record", "Skin Game", "Plan", "Lanzamiento",
    "Compromiso", "Mantenimiento", "Consolidación", "Inscripción", "Coordinadora",
    "Responsable", "Director", "Directora", "CEO", "Presidente",
    "Entrevista", "Entrevistado", "Entrevistas",
    "Consejo", "Conferencia", "Conferencias", "Tribuna", "Tribunas",
    # Fórmulas declarativas frecuentes que el regex confunde con entidades
    "Track Record", "Buen Comportamiento", "Comportamiento Relativo",
    "Análisis Cuantitativo", "Análisis Cualitativo", "Análisis Profesional",
    # Lead-ins de bullets/labels (v3)
    "Lanzamiento Avantage Fund FI", "Lanzamiento Avantage Fund Plan",
    "Concentración", "Concentración Argentina",
    "Rating", "Rating AA Citywire",
    "Composición", "Distribución", "Sectores", "Sector", "Geografía",
    "Riesgo", "Riesgos", "Fortaleza", "Fortalezas",
    "Adición", "Salida", "Gestión", "Disciplina",
    # Fuentes mediáticas como compounds (lead-ins de readings)
    "Salud Financiera Substack", "Substack Salud Financiera",
    "Astralis Funds Academy", "Astralis Podcast", "Más Dividendos",
    "Cinco Días", "Tu Dinero", "El Confidencial",
    "Funds Society", "Citywire España",
    # Anexos CNMV y referencias de fuentes
    "CNMV Anexo", "CNMV Anexo Sección", "Anexo Sección",
    "Carta Semestral", "Carta Anual", "Carta Trimestral",
    "Annual Report", "Informe Anual",
    # Referencias temporales con sustantivos
    "Pensiones Enero",
    # Asset class compounds (común en multi-activos como Trojan)
    "Japan Government Bonds", "UK RPI", "TIPS US", "US TIPS", "UK Linkers",
    "UK Gilts", "UK Inflation", "UK Inflation-Linked Gilts",
    "US Treasury Inflation Indexed Bonds", "Gold ETCs", "Physical Gold",
    "iShares Physical Gold", "Invesco Physical Gold",
    "Inflation-Linked", "Index-Linked", "Government Bonds",
    "Trojan UK", "Trojan Income", "Trojan Capital Fund",
    "CN Railway", "Canadian National Railway",
    # Cargos compuestos
    "Cofundador Troy", "Cofundador Troy Asset Management",
    "Vice Chairman", "Vice Chairman Troy", "Vice Chairman Troy Asset Management",
    "Lead Manager", "Co-Manager", "Portfolio Manager",
    "Director Inversiones", "Director de Inversiones",
    "Director Relación", "Directora Comunicación",
    "Domicilio Irlanda", "Lanzamiento Trojan", "Lanzamiento Trojan Fund",
}

# Palabras-stop intercaladas que indican que la "entidad" es una frase, no un nombre propio.
# Si el match contiene alguna de estas en posición intermedia, se descarta.
ENTITY_FILLER_WORDS = {
    "el", "la", "los", "las", "un", "una", "unos", "unas", "del", "de",
    "y", "o", "en", "con", "por", "para", "sin", "sobre", "tras",
    "no", "ni", "que", "qué", "cuyo", "cuya", "cuyos", "cuyas",
    "es", "son", "ha", "han", "hay", "fue", "fueron", "ser", "estar",
    "muy", "más", "menos", "mucho", "poco", "tanto",
}


class Validator:
    def __init__(self, isin: str, baseline_tag: str = "v1-api-stable",
                 fund_dir: Path | None = None,
                 skip_anti_invencion: bool = False):
        self.isin = isin
        self.baseline_tag = baseline_tag
        self.fund_dir = fund_dir or Path(f"data/funds/{isin}")
        self.skip_anti_invencion = skip_anti_invencion

        self.results: dict[str, list[dict]] = {
            "hard": [], "soft": [], "anti_invencion": []
        }

    # -----------------------------------------------------------------------
    # Loaders
    # -----------------------------------------------------------------------

    def load_current(self) -> dict | None:
        path = self.fund_dir / "output.json"
        if not path.exists():
            self._add("hard", "load_current", False,
                     f"No existe {path}. Ejecuta `python -m agents.orchestrator --isin {self.isin} --consume-cowork` antes.")
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            self._add("hard", "load_current", False, f"output.json inválido: {e}")
            return None

    def load_baseline(self) -> dict | None:
        try:
            result = subprocess.run(
                ["git", "show", f"{self.baseline_tag}:data/funds/{self.isin}/output.json"],
                cwd=self.fund_dir.parent.parent.parent if "data/funds" in str(self.fund_dir) else ".",
                capture_output=True, text=True, encoding="utf-8", check=True
            )
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            self._add("hard", "load_baseline", False,
                     f"No se pudo extraer baseline desde tag {self.baseline_tag}: {e.stderr}")
            return None
        except json.JSONDecodeError as e:
            self._add("hard", "load_baseline", False, f"baseline JSON inválido: {e}")
            return None

    def load_bundle_inputs(self) -> dict[str, Any]:
        """Carga los 5 inputs del bundle para anti-invención.
        Devuelve dict con texto extraído (solo valores string del JSON, no la sintaxis).

        v2: en vez de json.dumps (que pega strings adyacentes con sintaxis JSON),
        extraemos solo los VALORES string del árbol y los unimos con separadores.
        Esto evita matches espúrios donde el regex pilla, p.ej., "AA Citywire" + "El"
        de un JSON donde son keys/values distintos pero adyacentes textualmente.
        """
        bundle = self.fund_dir / "bundle"
        inputs_text: dict[str, str] = {}

        def extract_strings(obj, out: list) -> None:
            """Recolectar solo los valores string del árbol JSON."""
            if isinstance(obj, str):
                out.append(obj)
            elif isinstance(obj, dict):
                for v in obj.values():
                    extract_strings(v, out)
            elif isinstance(obj, list):
                for v in obj:
                    extract_strings(v, out)

        for fname in ["fund_data.json", "manager_profile.json", "letters_data.json",
                      "readings.json", "sources.json"]:
            path = bundle / fname
            if path.exists():
                try:
                    data = json.loads(path.read_text(encoding="utf-8"))
                    strings: list[str] = []
                    extract_strings(data, strings)
                    # Unir con separador inequívoco para evitar collision entre fields
                    inputs_text[fname] = "\n||\n".join(strings)
                except Exception as e:
                    self._add("soft", f"load_bundle_{fname}", False, f"no leíble: {e}")
                    inputs_text[fname] = ""
            else:
                # Fallback al fichero original si el bundle no se ha exportado aún
                fallback = {
                    "fund_data.json": ["cnmv_data.json", "intl_data.json"],
                    "manager_profile.json": ["manager_profile.json"],
                    "letters_data.json": ["letters_data.json"],
                    "readings.json": ["lecturas.json", "analisis_externos.json"],
                    "sources.json": [],
                }
                for fb in fallback.get(fname, []):
                    fb_path = self.fund_dir / fb
                    if fb_path.exists():
                        try:
                            data = json.loads(fb_path.read_text(encoding="utf-8"))
                            strings: list[str] = []
                            extract_strings(data, strings)
                            inputs_text[fname] = inputs_text.get(fname, "") + "\n||\n" + "\n||\n".join(strings)
                        except Exception:
                            pass
        return inputs_text

    # -----------------------------------------------------------------------
    # Hard checks
    # -----------------------------------------------------------------------

    def hard_kpis(self, current: dict, baseline: dict) -> None:
        cur_kpis = current.get("kpis", {}) or {}
        base_kpis = baseline.get("kpis", {}) or {}

        # AUM (tolerancia relativa)
        cur_aum = cur_kpis.get("aum_actual_meur")
        base_aum = base_kpis.get("aum_actual_meur")
        if cur_aum is not None and base_aum is not None:
            diff_rel = abs(cur_aum - base_aum) / max(abs(base_aum), 0.001)
            ok = diff_rel <= AUM_REL_TOLERANCE
            self._add("hard", "kpi_aum_actual_meur", ok,
                     f"baseline={base_aum} actual={cur_aum} diff_rel={diff_rel:.3%} (tol {AUM_REL_TOLERANCE:.0%})")
        elif (cur_aum is None) != (base_aum is None):
            self._add("hard", "kpi_aum_actual_meur", False,
                     f"baseline={base_aum} actual={cur_aum} — cambió de None a valor o viceversa")

        # Num partícipes (exacto)
        if base_kpis.get("num_participes") is not None:
            ok = cur_kpis.get("num_participes") == base_kpis.get("num_participes")
            self._add("hard", "kpi_num_participes", ok,
                     f"baseline={base_kpis.get('num_participes')} actual={cur_kpis.get('num_participes')}")

        # TER (tolerancia absoluta)
        cur_ter = cur_kpis.get("ter_pct")
        base_ter = base_kpis.get("ter_pct")
        if cur_ter is not None and base_ter is not None:
            ok = abs(cur_ter - base_ter) <= TER_ABS_TOLERANCE
            self._add("hard", "kpi_ter_pct", ok,
                     f"baseline={base_ter} actual={cur_ter} diff={abs(cur_ter-base_ter):.3f} (tol {TER_ABS_TOLERANCE})")

        # Año creación (exacto)
        if base_kpis.get("anio_creacion"):
            ok = str(cur_kpis.get("anio_creacion", "")).strip() == str(base_kpis.get("anio_creacion", "")).strip()
            self._add("hard", "kpi_anio_creacion", ok,
                     f"baseline={base_kpis.get('anio_creacion')} actual={cur_kpis.get('anio_creacion')}")

        # Divisa (exacto)
        if base_kpis.get("divisa"):
            ok = cur_kpis.get("divisa") == base_kpis.get("divisa")
            self._add("hard", "kpi_divisa", ok,
                     f"baseline={base_kpis.get('divisa')} actual={cur_kpis.get('divisa')}")

    def hard_top_posiciones(self, current: dict, baseline: dict) -> None:
        cur_pos = (current.get("posiciones", {}) or {}).get("actuales", []) or []
        base_pos = (baseline.get("posiciones", {}) or {}).get("actuales", []) or []

        if not base_pos:
            self._add("soft", "top_posiciones", True, "baseline sin posiciones, salto check")
            return

        cur_top5 = sorted(cur_pos, key=lambda p: p.get("peso_pct", 0), reverse=True)[:5]
        base_top5 = sorted(base_pos, key=lambda p: p.get("peso_pct", 0), reverse=True)[:5]

        cur_names = {self._normalize_name(p.get("nombre", "")) for p in cur_top5}
        base_names = {self._normalize_name(p.get("nombre", "")) for p in base_top5}

        matches = cur_names & base_names
        ok = len(matches) >= 4  # al menos 4/5 deben coincidir
        self._add("hard", "top5_posiciones_match", ok,
                 f"matches={len(matches)}/5 — base={sorted(base_names)} actual={sorted(cur_names)}")

        # Pesos de las que sí coinciden
        for cp in cur_top5:
            cn = self._normalize_name(cp.get("nombre", ""))
            for bp in base_top5:
                if self._normalize_name(bp.get("nombre", "")) == cn:
                    diff = abs((cp.get("peso_pct") or 0) - (bp.get("peso_pct") or 0))
                    ok = diff <= PESO_PCT_TOLERANCE
                    self._add("soft", f"peso_{cn[:30]}", ok,
                             f"baseline={bp.get('peso_pct')} actual={cp.get('peso_pct')} diff={diff:.2f}pp")
                    break

    def hard_gestores(self, current: dict, baseline: dict) -> None:
        # Lead/co manager comparison
        def get_lead_co(d: dict) -> tuple[set, set]:
            leads, cos = set(), set()
            for src in [d.get("gestores", {}).get("equipo", []),
                        d.get("analyst_synthesis", {}).get("gestores", {}).get("perfiles", [])]:
                for p in src or []:
                    name = self._normalize_name(p.get("nombre", "") if isinstance(p, dict) else "")
                    if not name:
                        continue
                    role = (p.get("role", "") if isinstance(p, dict) else "").lower()
                    if role == "lead" or p.get("is_lead"):
                        leads.add(name)
                    elif role == "co" or p.get("is_co"):
                        cos.add(name)
            return leads, cos

        cur_lead, cur_co = get_lead_co(current)
        base_lead, base_co = get_lead_co(baseline)

        if base_lead:
            ok = bool(cur_lead & base_lead)
            self._add("hard", "lead_manager_match", ok,
                     f"baseline_lead={base_lead} actual_lead={cur_lead}")

        if base_co:
            overlap = cur_co & base_co
            ok = len(overlap) >= 1
            self._add("soft", "co_manager_match", ok,
                     f"baseline_co={base_co} actual_co={cur_co} overlap={overlap}")

    def hard_schema_compliance(self, current: dict) -> None:
        synth = current.get("analyst_synthesis", {}) or {}
        for sec in REQUIRED_ANALYST_SECTIONS:
            present = sec in synth and bool(synth[sec])
            self._add("hard", f"section_{sec}_present", present,
                     f"{'OK' if present else 'FALTA'} analyst_synthesis.{sec}")

        # Subkeys mínimas por sección
        if synth.get("resumen"):
            for k in ["texto", "filosofia_inversion", "criterios_inversion"]:
                ok = k in synth["resumen"]
                self._add("soft", f"resumen_has_{k}", ok, f"resumen.{k} {'presente' if ok else 'falta'}")

        if synth.get("historia"):
            ok = isinstance(synth["historia"].get("hitos"), list) and len(synth["historia"].get("hitos", [])) >= 2
            self._add("soft", "historia_hitos_count", ok,
                     f"hitos count = {len(synth['historia'].get('hitos', []))}")

        if synth.get("estrategia"):
            for k in ["fortalezas", "riesgos"]:
                arr = synth["estrategia"].get(k, [])
                ok = isinstance(arr, list) and len(arr) >= 3
                self._add("soft", f"estrategia_{k}_count", ok,
                         f"{k} count = {len(arr) if isinstance(arr, list) else 'no-list'}")

        if synth.get("cartera"):
            arr = synth["cartera"].get("top_posiciones", [])
            ok = isinstance(arr, list) and len(arr) >= 5
            self._add("soft", "cartera_top_posiciones_count", ok,
                     f"top_posiciones count = {len(arr) if isinstance(arr, list) else 'no-list'}")

    # -----------------------------------------------------------------------
    # Soft checks
    # -----------------------------------------------------------------------

    def soft_section_lengths(self, current: dict, baseline: dict) -> None:
        cur_synth = current.get("analyst_synthesis", {}) or {}
        base_synth = baseline.get("analyst_synthesis", {}) or {}

        total_cur = total_base = 0
        for sec in REQUIRED_ANALYST_SECTIONS:
            cur_text = self._extract_section_text(cur_synth.get(sec, {}))
            base_text = self._extract_section_text(base_synth.get(sec, {}))
            cur_len = len(cur_text)
            base_len = len(base_text)
            total_cur += cur_len
            total_base += base_len

            if base_len > 100:  # Solo evalúa si baseline tiene contenido
                rel_diff = abs(cur_len - base_len) / base_len
                ok = rel_diff <= LENGTH_REL_TOLERANCE
                self._add("soft", f"length_{sec}", ok,
                         f"baseline={base_len}c actual={cur_len}c diff={rel_diff:.1%} (tol {LENGTH_REL_TOLERANCE:.0%})")
            elif cur_len < 50 and base_len < 50:
                self._add("soft", f"length_{sec}", True, f"ambos cortos (base={base_len} cur={cur_len})")

        if total_base > 0:
            total_diff = abs(total_cur - total_base) / total_base
            ok = total_diff <= TOTAL_LENGTH_REL_TOLERANCE
            self._add("soft", "length_total", ok,
                     f"baseline={total_base}c actual={total_cur}c diff={total_diff:.1%} (tol {TOTAL_LENGTH_REL_TOLERANCE:.0%})")

    def soft_no_filler_text(self, current: dict) -> None:
        """Detecta frases genéricas tipo 'se considera un fondo equilibrado' que indican relleno."""
        synth = current.get("analyst_synthesis", {}) or {}
        text_all = json.dumps(synth, ensure_ascii=False).lower()

        red_flags = [
            "se considera un fondo",
            "es un fondo equilibrado",
            "no disponible",
            "información limitada",
            "no se ha podido determinar",
            "fondo de inversión típico",
            "estrategia estándar",
        ]
        found = [rf for rf in red_flags if rf in text_all]
        ok = len(found) <= 1
        self._add("soft", "no_filler_text", ok,
                 f"red flags encontrados: {found}" if found else "limpio")

    # -----------------------------------------------------------------------
    # Anti-invención
    # -----------------------------------------------------------------------

    def anti_invencion(self, current: dict, inputs_text: dict[str, str]) -> None:
        if self.skip_anti_invencion:
            return

        synth = current.get("analyst_synthesis", {}) or {}
        synth_text = self._extract_all_text(synth)

        # Concatenamos todos los inputs para búsqueda
        haystack = " ".join(inputs_text.values()).lower()
        haystack_norm = self._normalize(haystack)

        # 1. Entidades nombradas (proper nouns)
        # Cualquier token de la entidad que esté en stoplist se considera "frase con palabra común"
        # y se descarta. Reduce compounds del tipo "Lanzamiento Avantage Fund FI" o "DFI Actualización".
        stoplist_lower = {s.lower() for s in ENTITY_STOPLIST}
        flagged_entities = []
        for match in PROPER_NOUN_RE.finditer(synth_text):
            entity = match.group(0).strip()
            if len(entity) < 4 or entity in ENTITY_STOPLIST:
                continue
            # Filtrar entidades que contienen filler words (artículos/preposiciones/verbos comunes)
            # — esas son frases capitalizadas, no nombres propios
            tokens = entity.split()
            if any(t.lower() in ENTITY_FILLER_WORDS for t in tokens):
                continue
            # Filtrar compounds donde CUALQUIER token está en stoplist
            # (ej. "Lanzamiento Avantage Fund FI" — "Lanzamiento" está en stoplist)
            if any(t.lower() in stoplist_lower for t in tokens):
                continue
            # Filtrar single-word verbos en mayúscula
            if len(tokens) == 1 and len(tokens[0]) > 8 and tokens[0].endswith(
                ("amos", "emos", "imos", "ando", "iendo", "ido", "ado", "ada", "ando", "ente", "able", "ible")
            ):
                continue
            # Filtrar single-word capitalizado al inicio de frase narrativa que NO sea nombre propio claro
            # (heurística: si single-word, debe aparecer al menos 2 veces en el output para considerar entidad)
            if len(tokens) == 1:
                count_in_synth = synth_text.count(entity)
                if count_in_synth < 2:
                    continue
            # Filtrar compounds que empiezan o terminan con palabra capitalizada típica de inicio de frase
            # ("El Confidencial" valida, pero "POSITIVO Combinación" no — "POSITIVO" es señal, no entidad)
            uppercase_signals = {"POSITIVO", "NEGATIVO", "NEUTRAL", "OK", "FAIL", "WARN"}
            if tokens[0] in uppercase_signals:
                continue
            entity_norm = self._normalize(entity)
            if entity_norm not in haystack_norm:
                # Fuzzy fallback: a veces hay variantes ortográficas
                if not self._fuzzy_in_haystack(entity_norm, haystack_norm, threshold=0.85):
                    flagged_entities.append(entity)

        # Dedup case-insensitive
        seen = set()
        flagged_entities_dedup = []
        for e in flagged_entities:
            k = e.lower()
            if k not in seen:
                seen.add(k)
                flagged_entities_dedup.append(e)

        for e in flagged_entities_dedup[:20]:  # Cap a 20 para no spamear
            self._add("anti_invencion", f"entity:{e[:40]}", False,
                     f"entidad '{e}' no aparece en ningún input del bundle")

        if not flagged_entities_dedup:
            self._add("anti_invencion", "entities", True, "todas las entidades verificables")

        # 2. Citas literales entre comillas
        # Construir haystack ampliado para citas: incluye letters_data + readings + manager_profile.fuentes_web
        # (a veces las "citas" del analyst son frases de entrevistas/tribunas, no de cartas formales)
        flagged_quotes = []
        for match in QUOTE_RE.finditer(synth_text):
            quote = match.group(1) or match.group(2)
            if not quote:
                continue
            quote_norm = self._normalize(quote)
            if len(quote_norm) < 20:
                continue
            # Limpiar markdown ** del comienzo/final de la cita antes de buscarla
            quote_clean = re.sub(r'^\*+|\*+$', '', quote_norm).strip()
            if quote_clean in haystack_norm:
                continue
            if quote_norm in haystack_norm:
                continue
            # Heurística adicional: trocear la cita en n-gramas de 6 palabras y verificar
            # que al menos uno aparece en el haystack (para citas largas con paráfrasis menores)
            words = quote_clean.split()
            if len(words) >= 6:
                ngrams = [" ".join(words[i:i+6]) for i in range(len(words) - 5)]
                if any(ng in haystack_norm for ng in ngrams):
                    continue
            if not self._fuzzy_in_haystack(quote_norm, haystack_norm, threshold=0.7):
                flagged_quotes.append(quote[:80])

        for q in flagged_quotes[:10]:
            self._add("anti_invencion", f"quote:{q[:30]}", False,
                     f"cita '{q}...' no aparece en cartas/readings")

        if not flagged_quotes:
            self._add("anti_invencion", "quotes", True, "todas las citas verificables")

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _add(self, level: str, name: str, passed: bool, detail: str) -> None:
        self.results[level].append({
            "name": name, "passed": passed, "detail": detail
        })

    @staticmethod
    def _normalize(s: str) -> str:
        s = s.lower()
        s = re.sub(r"[\s\.,;:\-_/'\"\(\)\[\]]+", " ", s)
        return s.strip()

    @staticmethod
    def _normalize_name(s: str) -> str:
        s = (s or "").strip().lower()
        # Eliminar sufijos corporativos comunes
        for suffix in [" sa", " s.a.", " ag", " plc", " inc", " corp", " ltd", " gmbh", " s.l.", " sl"]:
            if s.endswith(suffix):
                s = s[: -len(suffix)]
        return re.sub(r"\s+", " ", s).strip()

    @staticmethod
    def _fuzzy_in_haystack(needle: str, haystack: str, threshold: float) -> bool:
        """Busca needle en haystack con difflib. Costoso, solo para fallback."""
        if len(needle) < 4:
            return False
        # Ventana deslizante con SequenceMatcher
        words_needle = needle.split()
        if len(words_needle) > 5:
            words_needle = words_needle[:5]
        for i in range(0, len(haystack), 200):
            window = haystack[i:i + 400]
            ratio = difflib.SequenceMatcher(None, needle[:100], window).ratio()
            if ratio >= threshold:
                return True
        return False

    @staticmethod
    def _extract_section_text(section: Any) -> str:
        """Igual que _extract_all_text: usar \\n\\n como separador entre fields
        para evitar entidades compuestas artificiales del regex."""
        if isinstance(section, str):
            return section
        if isinstance(section, dict):
            parts = []
            for k, v in section.items():
                if isinstance(v, str):
                    parts.append(v)
                elif isinstance(v, list):
                    for item in v:
                        if isinstance(item, str):
                            parts.append(item)
                        elif isinstance(item, dict):
                            parts.append(json.dumps(item, ensure_ascii=False))
            return "\n\n".join(parts)
        return ""

    def _extract_all_text(self, obj: Any) -> str:
        """Concatena todos los strings de un dict/list pero usando separador FUERTE
        (\\n\\n) entre values adyacentes. Sin esto, el regex de proper nouns puede
        unir strings de fields contiguos del JSON dump y crear falsos positivos
        compuestos como 'POSITIVO Combinación' (signal+signal_rationale) o
        'Substack Salud Financiera Análisis' (fuente+titulo siguiente).

        Heurística: \\n\\n es un boundary que NO matchea ningún regex de entidad
        ni cita, así que separa los fields semánticamente sin afectar al texto
        propio dentro de cada field.
        """
        if isinstance(obj, str):
            return obj
        if isinstance(obj, dict):
            return "\n\n".join(self._extract_all_text(v) for v in obj.values() if v is not None)
        if isinstance(obj, list):
            return "\n\n".join(self._extract_all_text(v) for v in obj if v is not None)
        return ""

    # -----------------------------------------------------------------------
    # Run + report
    # -----------------------------------------------------------------------

    def run(self) -> dict:
        current = self.load_current()
        baseline = self.load_baseline()
        if not current or not baseline:
            return self._build_report()

        self.hard_kpis(current, baseline)
        self.hard_top_posiciones(current, baseline)
        self.hard_gestores(current, baseline)
        self.hard_schema_compliance(current)

        self.soft_section_lengths(current, baseline)
        self.soft_no_filler_text(current)

        if not self.skip_anti_invencion:
            inputs_text = self.load_bundle_inputs()
            self.anti_invencion(current, inputs_text)

        return self._build_report()

    def _build_report(self) -> dict:
        hard_failed = [r for r in self.results["hard"] if not r["passed"]]
        soft_failed = [r for r in self.results["soft"] if not r["passed"]]
        ai_flagged = [r for r in self.results["anti_invencion"] if not r["passed"]]

        if hard_failed or len(ai_flagged) > ANTI_INV_MAX_FLAGS_FOR_WARN:
            verdict = "fail"
        elif len(soft_failed) > SOFT_WARN_BUDGET or ai_flagged:
            verdict = "warn"
        else:
            verdict = "pass"

        return {
            "isin": self.isin,
            "baseline_tag": self.baseline_tag,
            "verdict": verdict,
            "summary": {
                "hard": {
                    "total": len(self.results["hard"]),
                    "passed": len(self.results["hard"]) - len(hard_failed),
                    "failed": len(hard_failed),
                },
                "soft": {
                    "total": len(self.results["soft"]),
                    "passed": len(self.results["soft"]) - len(soft_failed),
                    "warned": len(soft_failed),
                },
                "anti_invencion": {
                    "checks_run": len(self.results["anti_invencion"]),
                    "clean": len(ai_flagged) == 0,
                    "flags": len(ai_flagged),
                },
            },
            "details": self.results,
        }


# ---------------------------------------------------------------------------
# CLI / pretty print
# ---------------------------------------------------------------------------

def _try_rich():
    try:
        from rich.console import Console
        from rich.table import Table
        return Console(), Table
    except ImportError:
        return None, None


def print_report(report: dict) -> None:
    console, Table = _try_rich()
    isin = report["isin"]
    verdict = report["verdict"]
    summary = report["summary"]

    if console:
        verdict_color = {"pass": "green", "warn": "yellow", "fail": "red"}[verdict]
        console.print(f"\n[bold]Validación skill output[/bold] — ISIN [cyan]{isin}[/cyan] · "
                      f"baseline [dim]{report['baseline_tag']}[/dim]")
        console.print(f"Verdict: [bold {verdict_color}]{verdict.upper()}[/bold {verdict_color}]\n")

        t = Table(title="Resumen")
        t.add_column("Categoría")
        t.add_column("Pass", justify="right")
        t.add_column("Fail/Warn", justify="right")
        t.add_row("Hard checks",
                 str(summary["hard"]["passed"]),
                 f"[red]{summary['hard']['failed']}[/red]" if summary["hard"]["failed"] else "0")
        t.add_row("Soft checks",
                 str(summary["soft"]["passed"]),
                 f"[yellow]{summary['soft']['warned']}[/yellow]" if summary["soft"]["warned"] else "0")
        t.add_row("Anti-invención",
                 "clean" if summary["anti_invencion"]["clean"] else "—",
                 f"[yellow]{summary['anti_invencion']['flags']} flags[/yellow]"
                 if summary["anti_invencion"]["flags"] else "0")
        console.print(t)

        # Detalles de fallos
        for level, results in report["details"].items():
            failed = [r for r in results if not r["passed"]]
            if not failed:
                continue
            color = {"hard": "red", "soft": "yellow", "anti_invencion": "magenta"}[level]
            console.print(f"\n[bold {color}]{level.upper()} issues:[/bold {color}]")
            for r in failed:
                console.print(f"  • {r['name']}: {r['detail']}")
    else:
        # Fallback sin rich
        print(f"\nValidación skill output — ISIN {isin} · baseline {report['baseline_tag']}")
        print(f"Verdict: {verdict.upper()}\n")
        print(f"Hard:  {summary['hard']['passed']}/{summary['hard']['total']} pass, {summary['hard']['failed']} fail")
        print(f"Soft:  {summary['soft']['passed']}/{summary['soft']['total']} pass, {summary['soft']['warned']} warn")
        print(f"AI:    {summary['anti_invencion']['flags']} flags ({'clean' if summary['anti_invencion']['clean'] else 'issues'})")
        for level, results in report["details"].items():
            failed = [r for r in results if not r["passed"]]
            if not failed:
                continue
            print(f"\n{level.upper()} issues:")
            for r in failed:
                print(f"  - {r['name']}: {r['detail']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Valida el output del analyst (skill o legacy) contra baseline + anti-invencion.")
    parser.add_argument("isin", help="ISIN del fondo (ej. ES0112231008)")
    parser.add_argument("--baseline-tag", default="v1-api-stable",
                       help="Git tag del baseline (default: v1-api-stable)")
    parser.add_argument("--fund-dir", default=None,
                       help="Path explicito al directorio del fondo")
    parser.add_argument("--output-report", default=None,
                       help="Si se especifica, guarda el reporte JSON en ese path")
    parser.add_argument("--skip-anti-invencion", action="store_true",
                       help="Saltar la verificacion de invencion (mas rapido)")
    parser.add_argument("--quiet", action="store_true",
                       help="Solo imprime exit code, no detalles")
    args = parser.parse_args()

    fund_dir = Path(args.fund_dir) if args.fund_dir else None
    v = Validator(args.isin, baseline_tag=args.baseline_tag,
                  fund_dir=fund_dir,
                  skip_anti_invencion=args.skip_anti_invencion)
    report = v.run()

    if not args.quiet:
        print_report(report)

    if args.output_report:
        Path(args.output_report).write_text(json.dumps(report, ensure_ascii=False, indent=2),
                                            encoding="utf-8")
        if not args.quiet:
            print(f"\nReporte guardado en: {args.output_report}")

    return {"pass": 0, "warn": 1, "fail": 2}[report["verdict"]]


if __name__ == "__main__":
    sys.exit(main())

# v3.2 final
