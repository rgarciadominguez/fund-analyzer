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


# ════════════════════════════════════════════════════════════════════
# Validación de nombre de fondo (gate anti-basura, 2026-06-08)
# ════════════════════════════════════════════════════════════════════
# Evita almacenar un `nombre` erróneo: ISIN, prosa, etiquetas, fragmentos.
# Usado por los agentes fuente ANTES de escribir, y por name_recovery para
# decidir si hace falta recuperar.

# ISIN embebido: token de 12 chars (2 letras + 10 alfanum) como palabra
# independiente Y con al menos un dígito (un ISIN real lleva check digit). El
# \b y el dígito evitan falsos positivos con nombres largos ("DNCA Invest...").
_ISIN_TOKEN_RE = re.compile(r"\b[A-Z]{2}[A-Z0-9]{10}\b")

# Conectores/artículos que delatan que el "nombre" es en realidad una frase.
# OJO: NO incluir artículos sueltos legítimos en gestoras (La Française, Le ...).
_CONNECTOR_STARTS = {
    "para", "como", "esta", "este", "estos", "estas", "desde", "durante",
    "según", "segun", "tras", "antes", "después", "despues", "porque", "cuando",
    "donde", "this", "these", "that", "those", "with", "from", "about",
    "the fund", "el fondo", "la sicav", "el subfondo", "el sub-fondo",
}

# Etiquetas/placeholders que a veces se cuelan como nombre.
_STUB_NAMES = {
    "sin nombre", "no disponible", "n/a", "na", "n.d.", "nd", "unknown",
    "desconocido", "pendiente", "fondo", "fund", "sicav", "el fondo",
    "información del fondo", "informacion del fondo", "ficha del fondo",
    "datos del fondo", "documento", "prospectus", "folleto", "kid", "kiid",
    "nombre del fondo", "fondo de inversión", "fondo de inversion",
}


def is_valid_fund_name(name: str, isin: str = "") -> tuple[bool, str]:
    """¿`name` es un nombre de fondo plausible? Devuelve (ok, motivo).

    Conservador: acepta nombres legales largos de paraguas (p.ej. "ROBECO
    CAPITAL GROWTH FUNDS - ROBECO QI GLOBAL DEVELOPED ENHANCED INDEX EQUITIES")
    y rechaza ISIN, prosa, etiquetas y fragmentos.
    """
    n = (name or "").strip()
    if not n:
        return False, "vacio"
    nl = n.lower().strip(" .,:;")
    if isin and n.upper() == isin.upper():
        return False, "es_isin"
    for _tok in _ISIN_TOKEN_RE.findall(n.upper()):
        if any(ch.isdigit() for ch in _tok):
            return False, "contiene_isin"
    if len(n) < 5:
        return False, "muy_corto"
    if len(n) > 110:
        return False, "muy_largo"
    words = n.split()
    if len(words) > 16:
        return False, "prosa_demasiadas_palabras"
    # Empieza en minúscula → prosa, SALVO marcas camelCase legítimas (iShares,
    # eShares): minúscula seguida de mayúscula (2026-07-12).
    if n[0].islower() and not (len(n) > 1 and n[1].isupper()):
        return False, "empieza_minuscula"
    # ". " solo delata prosa (varias frases) si el nombre es LARGO. Nombres cortos
    # con abreviaturas (U.S., U.K., Int., S.A.) son legítimos (2026-07-12).
    if ". " in n and len(words) > 8:
        return False, "prosa_varias_frases"
    if sum(c.isalpha() for c in n) < 3:
        return False, "sin_letras"
    if nl in _STUB_NAMES:
        return False, "stub_o_etiqueta"
    first1 = words[0].lower().rstrip(",.;:")
    first2 = " ".join(words[:2]).lower().rstrip(",.;:")
    if first1 in _CONNECTOR_STARTS or first2 in _CONNECTOR_STARTS:
        return False, "empieza_conector_prosa"
    # Cabecera de sección / prosa: "Origen: una boutique...", "Resumen: el fondo..."
    # (un nombre PUEDE llevar ':' pero no seguido de minúscula). Audit 2026-06-18:
    # name_recovery extrajo la 1ª frase de la sección historia como nombre y se publicó.
    if re.match(r"^\S+:\s+[a-záéíóúñ]", n):
        return False, "prosa_cabecera_seccion"
    # Frases delatoras de prosa que NO aparecen en nombres de fondo legítimos.
    if re.search(r"\b(?:una|unos|unas|se trata|es un|es una|porque|mientras|aunque)\b", nl):
        return False, "prosa_frase"
    return True, "ok"


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
