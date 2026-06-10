"""region_normalizer.py — Canonicaliza nombres de PAÍS/región para que el mismo
sitio bajo distintos nombres (USA / United States / Estados Unidos) cuente como
uno solo en los desgloses geográficos. Canónico en español (el dashboard lo es).

Uso:
  canonical_country("United States of America") -> "Estados Unidos"
  aggregate_by_country({"USA":30, "United States":5, "France":10})
      -> {"Estados Unidos":35, "Francia":10}
"""
from __future__ import annotations

import re

# Sinónimos → canónico (español). Claves en minúscula sin puntuación.
_CANON = {
    # Estados Unidos
    "usa": "Estados Unidos", "us": "Estados Unidos", "u s a": "Estados Unidos",
    "u s": "Estados Unidos", "united states": "Estados Unidos",
    "united states of america": "Estados Unidos", "estados unidos": "Estados Unidos",
    "eeuu": "Estados Unidos", "ee uu": "Estados Unidos", "america": "Estados Unidos",
    "north america": "Estados Unidos",
    # Reino Unido
    "uk": "Reino Unido", "u k": "Reino Unido", "united kingdom": "Reino Unido",
    "great britain": "Reino Unido", "britain": "Reino Unido", "reino unido": "Reino Unido",
    "england": "Reino Unido",
    # Europa continental
    "france": "Francia", "francia": "Francia",
    "germany": "Alemania", "deutschland": "Alemania", "alemania": "Alemania",
    "spain": "España", "espana": "España", "españa": "España",
    "italy": "Italia", "italia": "Italia",
    "netherlands": "Países Bajos", "the netherlands": "Países Bajos",
    "holland": "Países Bajos", "paises bajos": "Países Bajos", "países bajos": "Países Bajos",
    "switzerland": "Suiza", "suiza": "Suiza",
    "ireland": "Irlanda", "irlanda": "Irlanda",
    "sweden": "Suecia", "suecia": "Suecia",
    "denmark": "Dinamarca", "dinamarca": "Dinamarca",
    "finland": "Finlandia", "finlandia": "Finlandia",
    "norway": "Noruega", "noruega": "Noruega",
    "belgium": "Bélgica", "belgica": "Bélgica", "bélgica": "Bélgica",
    "portugal": "Portugal", "austria": "Austria", "luxembourg": "Luxemburgo",
    "luxemburgo": "Luxemburgo",
    # Asia / resto
    "japan": "Japón", "japon": "Japón", "japón": "Japón",
    "china": "China", "hong kong": "Hong Kong", "hongkong": "Hong Kong",
    "south korea": "Corea del Sur", "korea": "Corea del Sur", "corea del sur": "Corea del Sur",
    "republic of korea": "Corea del Sur",
    "taiwan": "Taiwán", "taiwán": "Taiwán",
    "india": "India", "indonesia": "Indonesia", "thailand": "Tailandia",
    "tailandia": "Tailandia", "malaysia": "Malasia", "malasia": "Malasia",
    "singapore": "Singapur", "singapur": "Singapur", "vietnam": "Vietnam",
    "philippines": "Filipinas", "filipinas": "Filipinas",
    "australia": "Australia", "new zealand": "Nueva Zelanda",
    "canada": "Canadá", "canadá": "Canadá",
    "brazil": "Brasil", "brasil": "Brasil", "mexico": "México", "méxico": "México",
    "argentina": "Argentina", "chile": "Chile",
    "south africa": "Sudáfrica", "sudafrica": "Sudáfrica", "sudáfrica": "Sudáfrica",
    "turkey": "Turquía", "turquia": "Turquía", "turquía": "Turquía",
    "bermuda": "Bermudas", "cayman islands": "Islas Caimán", "jersey": "Jersey",
    "israel": "Israel", "saudi arabia": "Arabia Saudí",
    "greece": "Grecia", "grecia": "Grecia", "poland": "Polonia", "polonia": "Polonia",
    # zonas/agregados
    "global": "Global", "europe": "Europa", "europa": "Europa",
    "emerging markets": "Emergentes", "emerging": "Emergentes",
    "eurozone": "Eurozona", "euro zone": "Eurozona", "asia": "Asia",
    "other": "Otros", "others": "Otros", "otros": "Otros", "cash": "Liquidez",
}


def canonical_country(name: str) -> str:
    if not name:
        return ""
    key = re.sub(r"[.\-_/(),']", " ", str(name).lower())
    key = re.sub(r"\s+", " ", key).strip()
    if key in _CANON:
        return _CANON[key]
    # quitar sufijos tipo 'rep.'/'republic of'
    key2 = re.sub(r"^(the|republic of|kingdom of|state of)\s+", "", key).strip()
    if key2 in _CANON:
        return _CANON[key2]
    # por defecto, Title Case del original (mantener país no mapeado)
    return str(name).strip().title()


def aggregate_by_country(weights: dict) -> dict:
    """Suma pesos de sinónimos del mismo país. Devuelve {canónico: peso}."""
    out: dict = {}
    for k, v in (weights or {}).items():
        if v is None:
            continue
        c = canonical_country(k)
        if not c:
            continue
        out[c] = round(out.get(c, 0) + (v or 0), 2)
    return out
