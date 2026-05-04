"""One-shot: actualizar quality_rules.json conforme al feedback 2026-04-26.
1. Borra reglas min_chars / min_chars_nested (no son fallos relevantes).
2. Añade `scope` (ES/INT/all) a cada regla restante.
3. Añade reglas nuevas para features universales (citas/drivers/perfil_riesgo/desglose/gestores_anteriores/header).
"""
import json
from pathlib import Path

ROOT = Path(__file__).parent.parent
PATH = ROOT / "data" / "quality_rules.json"

# Reglas ES-only (validan datos que solo provee CNMV / KIID español / FONDCART XML)
ES_ONLY_IDS = {
    "cuant_min_serie_aum",         # CNMV monthly XML
    "cuant_min_serie_vl",          # serie VL del extractor cnmv
    "cuant_min_serie_participes",  # solo CNMV
    "cuant_has_rotacion",          # FONDREGISTRO
    "cuant_serie_vl_valid",        # validación VL CNMV
    "cuant_vl_desde_fuentes_oficiales",  # CNMV
    "cuant_has_comisiones",        # CNMV
    "cuant_serie_clases_info_presente",  # CNMV
    "comision_exito_teorica_presente",   # KIID español
    "comision_exito_cobros_no_superan_teorico",
    "cuant_ter_efectivo_coherente",      # serie cnmv
    "cartera_posiciones_historicas_cobertura",  # FONDCART XMLs
    "historia_kpis_calculables",   # serie cnmv
    "nombre_match_latest_pdf",     # PDFs CNMV
}

# Reglas a borrar (min_chars son arbitrarios - "lo importante es que sea explicativo y completo, no contar caracteres")
DELETE_IDS = {
    "resumen_min_chars",
    "historia_min_chars",
    "gestores_min_chars",
    "estrategia_min_chars",
    "cartera_min_chars",
    "fuentes_min_chars",
    "gestores_lead_trayectoria_min",
    # Cartera AHORA debe tener 3 headers (EXPOSICIÓN/DECISIONES/CONCENTRACIÓN) según
    # las reglas universales nuevas, así que la regla "no_headers" en cartera contradice.
    "cartera_no_headers",
}

# Reglas nuevas para features universales aplicadas hoy
NEW_RULES = [
    {
        "id": "quotes_no_word_only",
        "section": "estrategia",
        "scope": "all",
        "check_type": "quotes_substantive",
        "field_path": "analyst_synthesis.estrategia.quotes",
        "fail_type": "formato",
        "agente_responsable": "analyst_agent",
        "problema_template": "Citas con palabras sueltas detectadas ({offenders} de {total}). Una cita debe ser frase completa (≥25 chars, ≥5 palabras).",
        "accion_template": "Re-generar quotes en estrategia: si solo hay expresiones sueltas en cartas ('tremendos', 'absolutamente'), reconstruir la frase completa donde aparecen. Nunca devolver palabras aisladas.",
    },
    {
        "id": "hitos_estrategia_have_driver",
        "section": "estrategia",
        "scope": "all",
        "check_type": "hitos_with_driver",
        "field_path": "analyst_synthesis.estrategia.hitos_estrategia",
        "fail_type": "completitud",
        "agente_responsable": "analyst_agent",
        "problema_template": "Hitos sin driver explicativo ({offenders}/{total}). Cada resultado debe llevar 'cifra — driver' (por qué subió/bajó).",
        "accion_template": "Re-generar estrategia: cada hito debe tener resultado con formato '+X.XX% vs benchmark — DRIVER concreto'. Driver = evento/decisión que explica el resultado.",
    },
    {
        "id": "perfil_riesgo_present",
        "section": "estrategia",
        "scope": "all",
        "check_type": "perfil_riesgo_complete",
        "field_path": "analyst_synthesis.estrategia.perfil_riesgo",
        "fail_type": "completitud",
        "agente_responsable": "analyst_agent",
        "problema_template": "Perfil de riesgo de la estrategia incompleto: falta {missing}.",
        "accion_template": "Re-generar estrategia rellenando perfil_riesgo: tipo_activo_principal + ≥3 riesgos_especificos + escenarios_adversos + protecciones + liquidez_estructura.",
    },
    {
        "id": "desglose_exposicion_present",
        "section": "cartera",
        "scope": "all",
        "check_type": "desglose_exposicion_complete",
        "field_path": "analyst_synthesis.estrategia.perfil_riesgo.desglose_exposicion",
        "fail_type": "completitud",
        "agente_responsable": "analyst_agent",
        "problema_template": "Desglose de exposición incompleto: {actual} filas (mínimo 2). Cada fila necesita dimension + detalle + comentario.",
        "accion_template": "Re-generar estrategia/perfil_riesgo: desglose_exposicion debe tener ≥2 filas con la granularidad relevante al tipo de fondo (peril/región para cat bonds; país para EM; rating/sector para HY; etc.).",
    },
    {
        "id": "gestores_anteriores_when_recent_change",
        "section": "gestores",
        "scope": "all",
        "check_type": "gestores_anteriores_if_recent_change",
        "field_path": "analyst_synthesis.gestores",
        "fail_type": "completitud",
        "agente_responsable": "analyst_agent",
        "problema_template": "Hay gestor con incorporación reciente ({recent_year}) pero gestores_anteriores está vacío. Cuando hay cambio <3 años, explicar quién salió, motivo, sustituto e impacto.",
        "accion_template": "Re-generar gestores: rellenar gestores_anteriores con el equipo previo (nombre, cargo, periodo_en_fondo, fecha_salida, motivo_salida, sustituto, periodo_solapamiento, impacto_estrategia, tipo_cambio, ownership_nuevo).",
    },
    {
        "id": "header_gestora_with_suffix",
        "section": "header",
        "scope": "INT",
        "check_type": "string_has_pattern",
        "field_path": "gestora",
        "value": "(?i)\\b(?:Ltd\\.?|Limited|Plc|PLC|S\\.?A\\.?|S\\.?L\\.?|GmbH|AG|BV|N\\.?V\\.?|LLC|Inc\\.?|Management|Investments?|Capital|Partners|Asset\\s+Management|Fund\\s+Managers?|Holdings?|Advisors?)\\b",
        "fail_type": "incompletitud",
        "agente_responsable": "intl_extractor_v2",
        "problema_template": "Gestora '{actual}' aparece sin sufijo institucional (Ltd/Plc/SA/GmbH/Management). Debe usarse el nombre oficial completo.",
        "accion_template": "Enriquecer gestora desde PDFs (annual_report/KID/prospectus). Función _hdr_enrich_gestora_from_ar busca '{short_name} Fund Management Limited/Ltd/Plc'.",
    },
    {
        "id": "header_depositario_no_regulatory_text",
        "section": "header",
        "scope": "all",
        "check_type": "string_no_pattern",
        "field_path": "kpis.depositario",
        "value": "(?i)Register\\s*No|RegisterNo|FinancialServicesRegister|Authority[,.]|Prudential\\s*Regulation|Conduct\\s*Authority|FinancialConduct|regulated\\s+by|authori[sz]ed\\s+by|No\\.?\\s*\\d{4,}",
        "fail_type": "formato",
        "agente_responsable": "dashboard_header",
        "problema_template": "Depositario '{actual}' contiene texto regulatorio del KID/prospectus, no es un nombre institucional válido.",
        "accion_template": "Aplicar blacklist en _hdr_extract_depositario_from_ar: rechazar si contiene 'Register No', 'Authority,', 'FinancialServices', 'Prudential Regulation', etc. Si no hay candidato válido, dejar vacío.",
    },
    {
        "id": "header_fecha_inicio_consistent_with_classes",
        "section": "header",
        "scope": "INT",
        "check_type": "fecha_inicio_vs_clases",
        "field_path": "kpis",
        "fail_type": "datos_incorrectos",
        "agente_responsable": "intl_extractor_v2",
        "problema_template": "Fecha inicio fondo ({fecha_inicio}) posterior a snapshot de NAV de una clase ({snapshot_date}). La fecha de inicio debe ser de la clase MÁS ANTIGUA.",
        "accion_template": "intl_extractor_v2 debe extraer inception_date de TODAS las clases (no solo R EUR). El header usa min(inception_date) de todas las clases.",
    },
    {
        "id": "tipo_activo_completo_en_cartera",
        "section": "cartera",
        "scope": "all",
        "check_type": "posiciones_tipo_activo",
        "field_path": "posiciones.actuales",
        "fail_type": "incompletitud",
        "agente_responsable": "intl_extractor_v2",
        "problema_template": "Posiciones sin tipo de activo: {missing}/{total} ({pct}%) sin clasificación. Mínimo 80% deben tener tipo (RF/RV/Oro/Cash/ETF/Deriv).",
        "accion_template": "Post-process del extractor: inferir asset_type por nombre (treasury/bund/btp → RF; gold/oro → GOLD; future/swap → DER; sector financiero → RV; cash/deposit → CASH).",
    },
]


def main():
    data = json.loads(PATH.read_text(encoding="utf-8"))
    rules = data.get("rules", [])

    # 1) Borrar reglas obsoletas
    new_rules = [r for r in rules if r.get("id") not in DELETE_IDS]
    deleted = len(rules) - len(new_rules)

    # 2) Añadir scope a cada regla
    for r in new_rules:
        if "scope" not in r:
            r["scope"] = "ES" if r.get("id") in ES_ONLY_IDS else "all"

    # 3) Añadir reglas nuevas
    existing_ids = {r.get("id") for r in new_rules}
    added = 0
    for nr in NEW_RULES:
        if nr["id"] not in existing_ids:
            new_rules.append(nr)
            added += 1

    data["rules"] = new_rules
    data["schema_version"] = "2.1"
    data["comment"] = (
        data.get("comment", "")
        + " [v7 2026-04-26: removed min_chars rules (length not a relevant fail); "
        "added scope ES/INT/all per rule (auto-applied based on ISIN); "
        "added 9 rules for: quotes substantive, hitos drivers, perfil_riesgo, "
        "desglose_exposicion, gestores_anteriores when recent change, header "
        "gestora suffix, depositario no regulatory text, fecha_inicio coherent "
        "with classes, tipo_activo en posiciones]"
    )

    PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK: -{deleted} reglas borradas, +{added} reglas nuevas, total={len(new_rules)}")


if __name__ == "__main__":
    main()
