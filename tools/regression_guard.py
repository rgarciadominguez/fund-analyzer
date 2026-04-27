"""
Regression Guard — protege el output del analyst de regresiones cuando se
re-genera una sección.

Problema observado el 2026-04-26:
  - Quality Loop re-ejecuta analyst con quality_feedback
  - El nuevo resumen sale más corto (-25%) o pierde cifras concretas
  - El nuevo gestores tiene menos perfiles
  - Sin guard, esto se sobrescribe y perdemos info acumulada

Solución: comparar nuevo vs anterior en métricas relevantes por sección.
Si nuevo es PEOR → mantener anterior y log warning. Nunca empeora silencioso.

Uso:
    from tools.regression_guard import accept_section

    # Generar nueva sección
    new_section = self._section_resumen(data)
    # Aceptar solo si no es regresión
    final_section = accept_section("resumen", new_section, prior_section, log_fn=self._log)
    synthesis["resumen"] = final_section

NUNCA acepta una sección con `error` campo set (siempre prefiere anterior).
"""
import re
from typing import Callable

# Threshold: nueva sección puede perder hasta este % en cualquier métrica
DEFAULT_DROP_THRESHOLD = 0.30  # 30% de pérdida es el umbral

_CIFRAS_RX = re.compile(
    r'\d+[.,]?\d*\s*(?:%|M€|MEUR|EUR\s*[mM]|partícipes|participes|posiciones|años|anos|bps|p\.b\.)'
)
_NEGRITAS_RX = re.compile(r'\*\*[^*]+\*\*')


def _safe_text(section: dict, key: str = "texto") -> str:
    if not isinstance(section, dict):
        return ""
    val = section.get(key, "")
    return val if isinstance(val, str) else ""


def _safe_list(section: dict, key: str) -> list:
    if not isinstance(section, dict):
        return []
    val = section.get(key, []) or []
    return val if isinstance(val, list) else []


# ── Métricas por tipo de sección ──────────────────────────────────────────
def _m_chars(section: dict) -> int:
    return len(_safe_text(section, "texto"))


def _m_cifras(section: dict) -> int:
    return len(_CIFRAS_RX.findall(_safe_text(section, "texto")))


def _m_negritas(section: dict) -> int:
    return len(_NEGRITAS_RX.findall(_safe_text(section, "texto")))


def _m_hitos(section: dict) -> int:
    return len(_safe_list(section, "hitos"))


def _m_perfiles(section: dict) -> int:
    return len(_safe_list(section, "perfiles"))


def _m_trayectoria_total(section: dict) -> int:
    perfiles = _safe_list(section, "perfiles")
    return sum(len(p.get("trayectoria", "") or "") for p in perfiles if isinstance(p, dict))


def _m_decisiones_total(section: dict) -> int:
    perfiles = _safe_list(section, "perfiles")
    return sum(len(p.get("decisiones_clave", []) or []) for p in perfiles if isinstance(p, dict))


# Mapping section_name -> métricas críticas
SECTION_METRICS: dict[str, dict[str, Callable[[dict], int]]] = {
    "resumen": {"chars": _m_chars, "cifras": _m_cifras, "negritas": _m_negritas},
    "historia": {"chars": _m_chars, "hitos": _m_hitos, "cifras": _m_cifras},
    "gestores": {
        "perfiles": _m_perfiles,
        "trayectoria_total": _m_trayectoria_total,
        "chars_text": _m_chars,
    },
    "evolucion": {"chars": _m_chars, "cifras": _m_cifras},
    "estrategia": {"chars": _m_chars, "cifras": _m_cifras, "negritas": _m_negritas},
    "cartera": {"chars": _m_chars, "cifras": _m_cifras},
    "fuentes_externas": {"chars": _m_chars},
    # documentos: no tiene texto típico, no aplicar guard
}


def _is_error_section(section) -> bool:
    if not isinstance(section, dict):
        return True
    if section.get("error"):
        return True
    return False


def _is_anti_invencion_empty(section) -> bool:
    """Si la sección está vacía por anti-invención guard (datos faltantes),
    no contar como regresión — es comportamiento esperado y honesto."""
    if not isinstance(section, dict):
        return False
    return bool(section.get("_anti_invencion_guard"))


def compute_metrics(section_name: str, section: dict) -> dict:
    """Devuelve dict de métricas para una sección."""
    metrics_def = SECTION_METRICS.get(section_name, {"chars": _m_chars})
    return {name: fn(section) for name, fn in metrics_def.items()}


def is_regression(
    new: dict, old: dict, section_name: str,
    threshold: float = DEFAULT_DROP_THRESHOLD,
) -> tuple[bool, list]:
    """Devuelve (es_regresion, [razones]).

    Es regresión si:
    - new tiene 'error' y old no
    - cualquier métrica clave cae más del threshold (default 30%)

    No es regresión si:
    - new es anti-invención guard (vacío honesto)
    - old está vacío o errored
    """
    reasons = []
    if _is_error_section(new) and not _is_error_section(old):
        reasons.append("nueva sección tiene error y la anterior no")
        return True, reasons
    if _is_error_section(new) and _is_error_section(old):
        return False, []  # ambas error, no hay nada que perder
    if _is_anti_invencion_empty(new):
        return False, []  # vacío honesto, no es regresión
    if _is_error_section(old):
        return False, []  # old estaba mal, new gana siempre

    new_m = compute_metrics(section_name, new)
    old_m = compute_metrics(section_name, old)
    for k, old_v in old_m.items():
        new_v = new_m.get(k, 0)
        if old_v <= 0:
            continue
        drop = (old_v - new_v) / old_v
        if drop > threshold:
            reasons.append(
                f"{k}: {old_v} -> {new_v} ({drop*100:.0f}% pérdida, umbral {threshold*100:.0f}%)"
            )
    return bool(reasons), reasons


def accept_section(
    section_name: str, new: dict, prior: dict,
    log_fn=None, threshold: float = DEFAULT_DROP_THRESHOLD,
) -> dict:
    """Decide qué sección guardar. Si nueva es regresión, mantiene prior.
    Devuelve siempre la mejor opción."""
    if not prior:
        return new  # no había anterior, aceptar new
    is_reg, reasons = is_regression(new, prior, section_name, threshold)
    if is_reg:
        if log_fn:
            log_fn("REGRESSION_GUARD",
                   f"Sección '{section_name}' rechazada por regresión: {'; '.join(reasons)}. Mantengo anterior.")
        # Marcar que rechazamos esta versión
        kept = dict(prior)
        kept.setdefault("_regression_guard_rejected", []).append({
            "rejected_at": __import__("datetime").datetime.now().isoformat(),
            "reasons": reasons,
        })
        return kept
    return new


if __name__ == "__main__":
    # Smoke test
    old = {"texto": "El fondo Avantage tiene **150 M€** de AUM y 5000 partícipes con TER 0.91%.\n\n**Estrategia:** Equity global value. Resultado **+12.3%** vs benchmark."}
    new_better = {"texto": old["texto"] + "\n\n**Cartera:** Top 10 holdings concentran 35% con peso medio 3.5%."}
    new_worse = {"texto": "El fondo Avantage tiene AUM y partícipes."}
    new_error = {"error": "Gemini timeout"}

    print("Old metrics:", compute_metrics("resumen", old))
    print("New better:", is_regression(new_better, old, "resumen"))
    print("New worse:", is_regression(new_worse, old, "resumen"))
    print("New error:", is_regression(new_error, old, "resumen"))
