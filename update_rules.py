"""One-time script to add structural rules to quality_rules.json"""
import json

r = json.load(open('data/quality_rules.json', encoding='utf-8'))
rules = r['rules']

# Remove old rules that conflict with new structural rules
remove_ids = ['resumen_has_subsections', 'historia_has_subsections', 'estrategia_has_subsections', 'historia_min_hitos']
rules = [rule for rule in rules if rule['id'] not in remove_ids]

new_rules = [
    # RESUMEN
    {"id": "resumen_no_headers", "section": "resumen", "check_type": "no_bold_headers",
     "field_path": "analyst_synthesis.resumen.texto", "value": 0,
     "agente_responsable": "analyst_agent",
     "problema_template": "Resumen tiene {actual} subsecciones **bold** — debe ser narrativa fluida sin headers",
     "accion_template": "Re-generar resumen SIN subsecciones. Max 4 parrafos de narrativa pura."},
    {"id": "resumen_has_filosofia", "section": "resumen", "check_type": "field_present",
     "field_path": "analyst_synthesis.resumen.filosofia_inversion",
     "agente_responsable": "analyst_agent",
     "problema_template": "Resumen sin campo filosofia_inversion",
     "accion_template": "Re-generar con filosofia_inversion como campo separado (2-3 parrafos)."},
    {"id": "resumen_has_criterios", "section": "resumen", "check_type": "min_count_array",
     "field_path": "analyst_synthesis.resumen.criterios_inversion", "value": 3,
     "agente_responsable": "analyst_agent",
     "problema_template": "Solo {actual} criterios de inversion (minimo 3 numerados)",
     "accion_template": "Re-generar con 3 criterios de inversion numerados (titulo + descripcion)."},

    # HISTORIA
    {"id": "historia_no_headers", "section": "historia", "check_type": "no_bold_headers",
     "field_path": "analyst_synthesis.historia.texto", "value": 0,
     "agente_responsable": "analyst_agent",
     "problema_template": "Historia tiene {actual} subsecciones — debe ser 3-5 parrafos fluidos",
     "accion_template": "Re-generar historia como narrativa pura sin headers."},
    {"id": "historia_min_hitos_7", "section": "historia", "check_type": "min_count_array",
     "field_path": "analyst_synthesis.historia.hitos", "value": 7,
     "agente_responsable": "analyst_agent",
     "problema_template": "Solo {actual} hitos en cronologia (minimo 7)",
     "accion_template": "Re-generar con minimo 7 hitos con anio, titulo, evento, tipo."},
    {"id": "historia_hitos_have_titulo", "section": "historia", "check_type": "has_field_in_hitos",
     "field_path": "analyst_synthesis.historia.hitos", "required_field": "titulo", "value": 5,
     "agente_responsable": "analyst_agent",
     "problema_template": "Solo {actual} hitos con titulo (minimo {expected})",
     "accion_template": "Cada hito debe tener campo titulo descriptivo."},

    # GESTORES
    {"id": "gestores_lead_trayectoria_min", "section": "gestores", "check_type": "min_chars_nested",
     "field_path": "analyst_synthesis.gestores.perfiles[0].trayectoria", "value": 800,
     "agente_responsable": "analyst_agent",
     "problema_template": "Trayectoria del gestor principal solo {actual} chars (minimo {expected})",
     "accion_template": "Re-generar perfiles con trayectoria EXTENSA (3-4 parrafos)."},
    {"id": "gestores_lead_cv_bullets", "section": "gestores", "check_type": "nested_array_min",
     "field_path": "analyst_synthesis.gestores.perfiles[0].cv_bullets", "value": 4,
     "agente_responsable": "analyst_agent",
     "problema_template": "Gestor principal con {actual} CV bullets (minimo {expected})",
     "accion_template": "Re-generar perfiles con 4-6 CV bullets cortos por gestor."},

    # ESTRATEGIA
    {"id": "estrategia_no_headers", "section": "estrategia", "check_type": "no_bold_headers",
     "field_path": "analyst_synthesis.estrategia.texto", "value": 0,
     "agente_responsable": "analyst_agent",
     "problema_template": "Estrategia tiene {actual} subsecciones — debe ser parrafos evaluativos sin headers",
     "accion_template": "Re-generar estrategia como narrativa evaluativa pura."},
    {"id": "estrategia_has_quotes", "section": "estrategia", "check_type": "has_quotes",
     "field_path": "analyst_synthesis.estrategia.quotes", "value": 1,
     "agente_responsable": "analyst_agent",
     "problema_template": "Estrategia sin quotes del gestor ({actual} encontradas)",
     "accion_template": "Re-generar con 2-3 citas/frases representativas del gestor."},
    {"id": "estrategia_matrix_4col", "section": "estrategia", "check_type": "has_field_in_hitos",
     "field_path": "analyst_synthesis.estrategia.hitos_estrategia", "required_field": "contexto_mercado", "value": 3,
     "agente_responsable": "analyst_agent",
     "problema_template": "Solo {actual} hitos con columnas completas (minimo {expected})",
     "accion_template": "Re-generar hitos con campos: periodo, contexto_mercado, decisiones, resultado."},

    # CARTERA
    {"id": "cartera_no_headers", "section": "cartera", "check_type": "no_bold_headers",
     "field_path": "analyst_synthesis.cartera.texto", "value": 0,
     "agente_responsable": "analyst_agent",
     "problema_template": "Cartera tiene {actual} subsecciones — debe ser 2-3 parrafos concisos",
     "accion_template": "Re-generar cartera como narrativa concisa sin headers (max 1200 chars)."},
]

rules.extend(new_rules)
r['rules'] = rules
r['schema_version'] = '2.0'

with open('data/quality_rules.json', 'w', encoding='utf-8') as f:
    json.dump(r, f, ensure_ascii=False, indent=2)

print(f"Total reglas: {len(rules)}")
for rule in new_rules:
    print(f"  + {rule['id']}")
