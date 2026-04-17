"""
Taxonomía de conceptos financieros universales para extracción de fondos.

Este módulo es el **único sitio** donde vive el conocimiento del dominio.
Cada concepto está descrito en lenguaje financiero puro, sin hacer
referencia a cómo se etiqueta o estructura en ningún documento concreto
(ni "Note 18" ni "Statistics" ni "Schedule of Net Assets"). El LLM debe
razonar sobre el CONCEPTO y localizar la información sea cual sea el
formato o idioma del documento.

Contratos:
- `TAXONOMY` es la API estable que consumen mapper y extractor.
- Los `description` y `computation_hints` son el prompt; los prompts de
  mapper/extractor NO añaden contenido del dominio más allá de esto.
- Si un fondo requiere un concepto nuevo, se añade aquí (amplia
  taxonomía), nunca se parchean prompts del mapper/extractor.

Cada entry tiene:
  - description: qué es el concepto en lenguaje financiero
  - output_shape: estructura JSON esperada para los valores extraídos
  - computation_hints: instrucciones de post-proceso si el dato viene
    en múltiples formatos (ej. absoluto vs per-share × shares)
  - applies_to: qué tipos de documento lo pueden contener
  - priority: "core" (crítico para el schema), "useful" (enriquece
    análisis), "nice_to_have"
"""
from __future__ import annotations


# ── Tipos de documento que puede procesar el pipeline ─────────────────────
DOC_TYPES = {
    "annual_report",
    "semi_annual_report",
    "factsheet",
    "quarterly_letter",
    "manager_presentation",
    "kid",
    "prospectus",
}

ALL = frozenset(DOC_TYPES)
REPORTS = frozenset({"annual_report", "semi_annual_report"})
COMMERCIAL = frozenset({"quarterly_letter", "manager_presentation"})
REGULATORY = frozenset({"kid", "prospectus"})
FACTSHEET = frozenset({"factsheet"})


TAXONOMY: dict[str, dict] = {
    # ══════════════════════════════════════════════════════════════════════
    # IDENTIDAD
    # ══════════════════════════════════════════════════════════════════════
    "target_fund_identity": {
        "priority": "core",
        "applies_to": ALL,
        "description": (
            "Identificación inequívoca del sub-fondo que estamos analizando "
            "dentro del documento. Necesitamos confirmar que el documento "
            "habla efectivamente de este sub-fondo (no sólo de su paraguas "
            "o de fondos hermanos) y recoger cómo el documento lo nombra, "
            "su divisa base, y cuándo fue lanzado."
        ),
        "output_shape": {
            "display_name": "nombre del sub-fondo tal como aparece en el documento",
            "base_currency": "divisa base de reporting del sub-fondo (EUR, USD, GBP, …)",
            "inception_date": "YYYY-MM-DD del lanzamiento (o solo YYYY si el doc no da mes/día)",
            "legal_structure": "descripción corta del vehículo legal (UCITS SICAV, UCITS FCP, OEIC, AIF, …)",
            "domicile": "jurisdicción (Luxembourg, Ireland, France, UK, Germany, …)",
        },
    },

    "portfolio_management_team": {
        "priority": "useful",
        "applies_to": REPORTS | COMMERCIAL,
        "description": (
            "Personas responsables de gestionar el sub-fondo. Queremos los "
            "nombres de los gestores principales, su rol (lead manager, "
            "co-manager, CIO supervisor, …), su trayectoria profesional si "
            "el documento la describe, y el año en que comenzaron a "
            "gestionar este sub-fondo si aparece."
        ),
        "output_shape": {
            "members": [
                {
                    "name": "nombre completo",
                    "role": "rol en la gestión de este fondo",
                    "background": "trayectoria si aparece (experiencia previa, educación, años en la industria)",
                    "since_year": "año que comenzó en este fondo (int)",
                }
            ]
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # TAMAÑO Y CLASES
    # ══════════════════════════════════════════════════════════════════════
    "fund_size_history": {
        "priority": "core",
        "applies_to": REPORTS | FACTSHEET,
        "description": (
            "Capital total gestionado por el sub-fondo objetivo a distintas "
            "fechas de cierre. Queremos tantos puntos temporales como el "
            "documento ofrezca, expresados en su divisa original. "
            "IMPORTANTE: esto es el capital del sub-fondo, no el capital "
            "consolidado del paraguas umbrella. No confundir con el NAV "
            "por acción (precio unitario, típicamente valores entre 1 y "
            "un par de miles)."
        ),
        "output_shape": {
            "snapshots": [
                {
                    "date": "YYYY-MM-DD (o YYYY-MM si solo se da mes y año)",
                    "value": "valor numérico del capital total",
                    "currency": "divisa en que se reporta (EUR, USD, GBP, …)",
                    "source_description": "cómo aparece en el documento (ej. 'suma de NAV de todas las clases', 'total net assets del sub-fondo')",
                }
            ]
        },
        "computation_hints": (
            "Si el documento reporta el total del sub-fondo directamente, "
            "usar ese valor. Si solo reporta por clase, sumar todas las "
            "clases del sub-fondo (ignorando las de fondos hermanos). Si "
            "solo reporta NAV por acción y número de acciones outstanding "
            "por clase, multiplicar y sumar. Indicar en source_description "
            "qué método se ha usado."
        ),
    },

    "share_classes_catalog": {
        "priority": "core",
        "applies_to": REPORTS | REGULATORY,
        "description": (
            "Catálogo de todas las clases de acciones del sub-fondo "
            "objetivo. Para cada clase queremos tres piezas de datos "
            "distintas, para cada fecha/año disponible: "
            "(a) CAPITAL TOTAL de la clase (valor absoluto, cifra grande "
            "con comas, ej. 62,809,554 o 305,7 millions); "
            "(b) NÚMERO DE ACCIONES en circulación (cifra con decimales a "
            "veces, ej. 35,905,117.188); "
            "(c) PRECIO POR ACCIÓN / NAV per share (valor pequeño 1-2000, "
            "ej. 180.37 o 1.2238). "
            "Es frecuente que el documento reporte solo DOS de las tres: "
            "típicamente NAV per share + número de acciones (hay que "
            "multiplicar), o el capital total + NAV per share. Rellenar "
            "lo que el documento efectivamente muestre y dejar null lo "
            "que no aparezca — el pipeline calculará el tercer campo si "
            "tiene dos. "
            "IMPORTANTE: solo clases del sub-fondo objetivo, nunca "
            "mezclar con clases de otros sub-fondos del paraguas."
        ),
        "output_shape": {
            "classes": [
                {
                    "code": "identificador de la clase tal como aparece (ej. 'Class I EUR accumulation', 'Class O USD Inc', 'A-EUR')",
                    "currency": "divisa de la clase",
                    "nav_total_snapshots": [
                        {
                            "date": "YYYY-MM-DD",
                            "nav_total": "capital total de la clase en su divisa (null si no se reporta directamente)",
                            "shares_outstanding": "número de acciones en circulación (null si no se reporta)",
                            "nav_per_share": "precio por acción (null si no se reporta)",
                        }
                    ],
                }
            ]
        },
        "computation_hints": (
            "Llenar los tres sub-campos (nav_total, shares_outstanding, "
            "nav_per_share) con lo que reporte literalmente el documento. "
            "No calcular en la extracción — el pipeline calculará después "
            "shares × nav_per_share si nav_total viene null. "
            "Extraer tantas fechas (snapshots) como el documento ofrezca "
            "comparativamente."
        ),
    },

    # ══════════════════════════════════════════════════════════════════════
    # ECONOMÍA / COMISIONES
    # ══════════════════════════════════════════════════════════════════════
    "fee_structure": {
        "priority": "core",
        "applies_to": REPORTS | REGULATORY | FACTSHEET,
        "description": (
            "Esquema completo de comisiones aplicables al sub-fondo. "
            "Queremos distinguir las distintas capas de coste que paga el "
            "partícipe: comisión de gestión del gestor (fee principal), "
            "TER/ongoing charges agregado, performance fee si existe con "
            "su trigger, comisión del ManCo administrativo, fees de "
            "depositario y administración. Valores en porcentaje sobre "
            "NAV. Si la fee varía por clase, devolver la de la clase con "
            "más capital (o equivalente retail)."
        ),
        "output_shape": {
            "management_fee_pct": "comisión de gestión del Investment Manager en % NAV",
            "ter_pct": "Total Expense Ratio / Ongoing Charges Figure agregado en % NAV",
            "performance_fee_pct": "comisión de éxito en % (null si el fondo no cobra)",
            "performance_fee_trigger": "descripción del trigger (benchmark + spread, high water mark, hurdle rate, …) si aplica",
            "management_company_fee_pct": "fee del ManCo/administrador si se reporta separadamente",
            "admin_depositary_fees_pct": "fees combinados de administrador + depositario si se reportan",
            "other_fees": [
                {"name": "nombre del concepto", "pct": "% NAV", "amount": "valor absoluto con divisa si no da %"}
            ],
        },
    },

    "fund_economics_yearly": {
        "priority": "useful",
        "applies_to": REPORTS,
        "description": (
            "Resultado económico del sub-fondo en un ejercicio. Sirve para "
            "entender si la gestora obtiene ingresos razonables del fondo "
            "(un proxy de su viabilidad comercial) y si el fondo genera "
            "rentabilidad neta para el partícipe. Queremos: total de "
            "comisiones de gestión cobradas en el año (línea del cuadro "
            "de gastos), resultado neto atribuible a partícipes (última "
            "línea del estado de resultados), costes operativos totales."
        ),
        "output_shape": {
            "year": "YYYY del ejercicio fiscal",
            "currency": "divisa en que se reporta",
            "management_fees_collected": "total cobrado por la gestora en concepto de management fee (valor absoluto)",
            "net_result_attributable_to_holders": "resultado neto del ejercicio atribuible a los partícipes",
            "total_operating_costs": "suma de gastos operativos del fondo",
            "total_income": "ingresos totales (dividendos + intereses + otros)",
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # CONVERSIÓN DE DIVISAS
    # ══════════════════════════════════════════════════════════════════════
    "currency_conversion_rates": {
        "priority": "useful",
        "applies_to": REPORTS,
        "description": (
            "Tipos de cambio que el documento usa para convertir clases de "
            "distintas divisas a la divisa funcional del sub-fondo al "
            "reporting date. Nos permite sumar clases multi-divisa en una "
            "única moneda con precisión histórica (en vez de usar FX "
            "actual que introduce sesgo cuando las fechas son pasadas)."
        ),
        "output_shape": {
            "functional_currency": "divisa funcional del sub-fondo/paraguas",
            "rates": [
                {
                    "date": "YYYY-MM-DD del reporting date al que aplica esta tasa",
                    "from_currency": "divisa origen",
                    "to_functional_rate": "cuántas unidades de functional equivalen a 1 unidad de from_currency (o su recíproco — el documento lo aclara)",
                    "direction": "'per_1_from' o 'per_1_to' según cómo lo exprese el documento",
                }
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # CARTERA
    # ══════════════════════════════════════════════════════════════════════
    "asset_allocation_history": {
        "priority": "useful",
        "applies_to": REPORTS | FACTSHEET,
        "description": (
            "Distribución de la cartera del sub-fondo por tipo de activo a "
            "distintas fechas. Las grandes categorías son: renta variable, "
            "renta fija (bonos, deuda), instrumentos de liquidez (cash, "
            "equivalents, money market), y otros (derivados netos, "
            "alternativos, commodities, convertibles, inmobiliario). "
            "Queremos porcentajes que sumen aproximadamente 100%."
        ),
        "output_shape": {
            "snapshots": [
                {
                    "date": "YYYY-MM-DD o YYYY-MM",
                    "equity_pct": "% en renta variable",
                    "fixed_income_pct": "% en renta fija",
                    "cash_pct": "% en liquidez",
                    "other_pct": "% en otros activos",
                    "notes": "si el documento categoriza distinto, explicar brevemente",
                }
            ]
        },
    },

    "geographic_allocation_history": {
        "priority": "useful",
        "applies_to": REPORTS | FACTSHEET,
        "description": (
            "Distribución de la cartera del sub-fondo por zona geográfica "
            "o país del emisor a distintas fechas. Queremos un mapa "
            "país/zona → porcentaje. CADA snapshot DEBE llevar su fecha "
            "(date); si el documento no la explicita pero es un factsheet/"
            "AR con fecha de reporte conocida, usar esa fecha. Nunca "
            "dejar `date` vacía. Distinguir breakdown geográfico (países/"
            "zonas) de breakdown por divisa (currencies) — aquí queremos "
            "SOLO geográfico."
        ),
        "output_shape": {
            "snapshots": [
                {
                    "date": "YYYY-MM-DD o YYYY-MM obligatorio",
                    "allocations": [
                        {"zone": "país o zona", "pct": "porcentaje"}
                    ],
                }
            ]
        },
    },

    "portfolio_metrics": {
        "priority": "core",
        "applies_to": REPORTS | FACTSHEET | REGULATORY,
        "description": (
            "Métricas de cartera del sub-fondo que aportan contexto "
            "cuantitativo de riesgo/concentración/benchmark: "
            "benchmark oficial del fondo (nombre completo con spread "
            "si aplica, ej. '€STER + 1.40%'), concentración del top 10 "
            "(suma de pesos de las 10 primeras posiciones), número total "
            "de posiciones distintas en cartera, métricas de riesgo si "
            "el documento las reporta (volatilidad anualizada, Sharpe, "
            "tracking error, max drawdown), duración modificada para "
            "fondos de renta fija, yield to maturity para RF."
        ),
        "output_shape": {
            "benchmark": "nombre del benchmark con estructura completa",
            "concentracion_top10_pct": "% del NAV en top 10 posiciones",
            "num_holdings_total": "número total de posiciones distintas",
            "volatility_pct": "volatilidad anualizada del fondo si aparece",
            "sharpe_ratio": "Sharpe si aparece",
            "tracking_error_pct": "tracking error si aparece",
            "max_drawdown_pct": "peor caída histórica si aparece",
            "duration_years": "duración modificada (RF)",
            "yield_to_maturity_pct": "YTM (RF)",
            "classification": "categoría del fondo (Bond Fund / Global Equity / Multi-asset / etc.)",
        },
    },

    "top_holdings": {
        "priority": "core",
        "applies_to": REPORTS | FACTSHEET,
        "description": (
            "Principales posiciones de la cartera del sub-fondo ordenadas "
            "por peso. Para cada posición queremos el nombre (emisor o "
            "instrumento), peso como % del NAV, sector GICS o similar si "
            "aparece, país del emisor si aparece, y racional de la "
            "inversión si el documento lo explica."
        ),
        "output_shape": {
            "as_of_date": "YYYY-MM-DD de la foto",
            "holdings": [
                {
                    "name": "nombre del emisor o instrumento",
                    "ticker": "ticker si aparece",
                    "weight_pct": "% NAV",
                    "sector": "sector GICS o similar si aparece",
                    "country": "país del emisor si aparece",
                    "rationale": "razón de la inversión si el documento la da",
                }
            ],
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # PERFORMANCE
    # ══════════════════════════════════════════════════════════════════════
    "performance_history": {
        "priority": "nice_to_have",
        "applies_to": REPORTS,
        "description": (
            "Rentabilidad histórica del sub-fondo por periodo y por clase "
            "de acción. Para cada (año, clase) queremos la rentabilidad "
            "del fondo en % y la del benchmark de referencia en % si "
            "aparece. Los periodos típicos son años civiles o años "
            "fiscales; si el documento usa otra granularidad (YTD, "
            "trimestres), incluirla."
        ),
        "output_shape": {
            "series": [
                {
                    "period": "YYYY o YYYY-MM o 'YTD' o '1Y'/'3Y'/'5Y'/'since_inception'",
                    "class_code": "clase a la que corresponde",
                    "fund_return_pct": "rentabilidad del fondo",
                    "benchmark_return_pct": "rentabilidad del benchmark",
                    "benchmark_name": "nombre del benchmark si aparece",
                }
            ]
        },
    },

    # ══════════════════════════════════════════════════════════════════════
    # CUALITATIVO
    # ══════════════════════════════════════════════════════════════════════
    "manager_qualitative": {
        "priority": "core",
        "applies_to": REPORTS | COMMERCIAL | REGULATORY | FACTSHEET,
        "description": (
            "Visión cualitativa del equipo gestor sobre su enfoque. "
            "Queremos texto sustantivo (no slogans publicitarios) que "
            "describa: estrategia de inversión, filosofía del gestor, "
            "proceso de selección de posiciones, tipo de activos en los "
            "que invierten, objetivos reales del fondo, e historia del "
            "fondo si aparece. Prefiere extractos literales del documento "
            "frente a paráfrasis."
        ),
        "output_shape": {
            "strategy": "descripción de la estrategia",
            "philosophy": "filosofía de inversión",
            "selection_process": "proceso para seleccionar posiciones",
            "asset_types": "tipos de activos típicos del fondo",
            "real_objectives": "objetivos de largo plazo más allá del benchmark",
            "fund_history": "hitos, cambios relevantes, track record narrativo",
        },
    },

    "manager_thesis_and_decisions": {
        "priority": "core",
        "applies_to": REPORTS | COMMERCIAL | FACTSHEET,
        "description": (
            "Tesis de inversión del gestor en un periodo concreto + "
            "decisiones de cartera tomadas en ese periodo + resultado "
            "observado (si el documento lo cubre). Sirve para el análisis "
            "de consistencia: ¿las decisiones fueron coherentes con la "
            "tesis? ¿el resultado validó la tesis? Queremos tantas "
            "entradas (periodos) como el documento ofrezca."
        ),
        "output_shape": {
            "periods": [
                {
                    "period": "YYYY, YYYY-Q1/Q2/Q3/Q4, o rango",
                    "market_context": "contexto macro del periodo según el gestor",
                    "thesis": "tesis del gestor en ese momento",
                    "decisions_taken": "cambios en cartera (posiciones iniciadas, aumentadas, vendidas)",
                    "observed_outcome": "resultado posterior si el doc lo refleja",
                    "notes": "observaciones adicionales",
                }
            ]
        },
    },
}


# ── Helpers públicos ──────────────────────────────────────────────────────

def concepts_for_doc_type(doc_type: str) -> dict[str, dict]:
    """Subset de la taxonomía aplicable a un doc_type dado."""
    return {
        name: entry
        for name, entry in TAXONOMY.items()
        if doc_type in entry.get("applies_to", ALL)
    }


def core_concepts() -> list[str]:
    """Conceptos 'core' — los que DEBEN poder extraerse para un output válido."""
    return [
        name for name, entry in TAXONOMY.items()
        if entry.get("priority") == "core"
    ]


def concept_names() -> list[str]:
    return list(TAXONOMY.keys())
