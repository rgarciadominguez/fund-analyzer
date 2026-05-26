"""
fund_name_utils.py — Helpers para parsing de nombres de fondos.

Bug histórico (G1, 2026-05-18): readings_collector / letters_collector /
manager_profiler hacían `fund_name.split(" - ")[-1]` para extraer "fund_short".

- Para ES (formato "Gestora — Fondo, FI") el split por " - " devuelve el nombre completo
  cuando no hay " - " (correcto).
- Para INT multi-clase (formato "NOMBRE FONDO - Action X") `[-1]` devuelve "Action X"
  (genérico, mata las queries Google a 0 resultados en blogs de nicho).

Este módulo unifica la extracción de fund_short con detección de patrones de
denominación de clase, conservando el resto cuando es seguro.
"""
from __future__ import annotations

import re

# Patrones típicos de denominación de clase al final del nombre.
# Si el ÚLTIMO segmento (tras split " - ") matchea uno de estos, se descarta.
CLASS_TAIL_PATTERNS = [
    r'^Action\s+[A-Z0-9]+(?:\s+(?:EUR|USD|GBP|CHF|JPY))?$',
    r'^Class\s+[A-Z0-9]+(?:\s+(?:EUR|USD|GBP|CHF|JPY))?$',
    r'^Klasse\s+[A-Z0-9]+(?:\s+(?:EUR|USD|GBP|CHF|JPY))?$',
    r'^Clase\s+[A-Z0-9]+(?:\s+(?:EUR|USD|GBP|CHF|JPY))?$',
    r'^Part\s+[A-Z0-9]+$',
    r'^Share(?:\s+Class)?\s+[A-Z0-9]+$',
    r'^Anteil(?:sklasse)?\s+[A-Z0-9]+$',
    r'^Cat(?:egor[ií]a)?\s+[A-Z0-9]+$',
    # Forma "I Acc" / "B EUR" / "C Inc Hedged"
    r'^[A-Z0-9]{1,3}\s+(?:Acc|Inc|Cap|Dist|Hedged?)$',
    r'^[A-Z0-9]{1,3}\s+(?:EUR|USD|GBP|CHF|JPY)(?:\s+(?:Acc|Inc|H|Hedged?))?$',
    # Single-token clase: A / I / R / Z / P / F (mayúsculas, 1-2 chars)
    r'^[A-Z]{1,2}$',
    r'^(?:Acc|Inc|Cap|Dist|Retail|Institutional)$',
]


def is_class_tail(s: str) -> bool:
    """True si la cadena parece denominación de clase (no nombre del fondo)."""
    s = s.strip() if s else ""
    if not s:
        return False
    return any(re.match(pat, s, re.IGNORECASE) for pat in CLASS_TAIL_PATTERNS)


# Tokens que indican que el primer segmento es una SICAV-paraguas genérica
# (en ese caso el sub-fondo está en los segmentos siguientes)
SICAV_GENERIC_TOKENS = {
    "INVEST", "FUNDS", "FUND", "SICAV", "UMBRELLA", "SERIES",
    "TRUST", "ICAV", "PLC", "OEIC", "OPCVM",
}


def extract_fund_short(fund_name: str) -> str:
    """Extrae nombre del fondo sin la clase comercial, conservando contexto útil.

    Examples:
      "SEXTANT QUALITY FOCUS - Action F"      -> "SEXTANT QUALITY FOCUS"
      "Magallanes European Equity, FI"        -> "Magallanes European Equity, FI"
      "Avantage Fund - Class I"               -> "Avantage Fund"
      "DNCA INVEST - Alpha Bonds I EUR"       -> "DNCA INVEST - Alpha Bonds I EUR" (no class tail)
      "DNCA INVEST - Alpha Bonds - Class I"   -> "DNCA INVEST - Alpha Bonds"
      "Trojan Fund O Acc"                     -> "Trojan Fund O Acc"  (no " - ")
      "Cobas Selección, FI"                   -> "Cobas Selección, FI"
      ""                                       -> ""
    """
    if not fund_name or not fund_name.strip():
        return fund_name or ""

    parts = [p.strip() for p in fund_name.split(" - ") if p.strip()]
    if len(parts) <= 1:
        return fund_name.strip()

    # Si el último segmento es claramente una clase → descartar
    if is_class_tail(parts[-1]):
        return " - ".join(parts[:-1]).strip()

    # Si el primer segmento es SICAV-paraguas (DNCA INVEST, FRANKLIN TEMPLETON FUNDS…)
    # mantenemos todo: la query "DNCA INVEST Alpha Bonds" es razonable, mientras que
    # solo "Alpha Bonds" sería demasiado genérico (varios SICAV tienen sub-fondos así).
    # Por defecto devolvemos el nombre completo: las queries de blogs nicho ya
    # tienen lógica de fund_variants que generará MEDIUM/SHORT desde aquí.
    return fund_name.strip()
