---
name: letters-extract-cowork
description: Extrae datos K15 estructurados (tesis del gestor, decisiones de cartera, contexto de mercado, citas textuales, posiciones mencionadas, outlook) de las cartas trimestrales/semestrales de los fondos. Reemplaza `letters_deep_agent.py` que usa Gemini para esta tarea. Úsala SIEMPRE que Rafa diga "letters extract cowork", "extrae K15 de cartas X", "skill letters X", o cualquier variante. Espera que `letters_collector.py` haya descargado las cartas como PDFs/HTMLs en `data/funds/{ISIN}/raw/letters/` y haya generado un manifiesto `letters_data.json` con la lista de cartas pendientes de procesar.
---

# letters-extract-cowork v1.0

Sustituto de `letters_deep_agent.py`. Extrae K15 estructurado de cartas trimestrales/semestrales del gestor. Diseñada para correr bajo Claude Max sin coste API.

**Reemplaza**: la extracción K15 (tesis_gestora, decisiones_tomadas, contexto_mercado, citas_textuales, posiciones_mencionadas, outlook, resultado_real) de cartas en `letters_deep_agent`.

## Pre-requisito obligatorio

Debe existir `data/funds/{ISIN}/letters_data.json` con el campo `cartas`:

```json
{
  "isin": "...",
  "cartas": [
    {
      "periodo": "2025-S2",
      "fecha_inferida": "2026-01-15",
      "tipo": "semestral",
      "titulo": "Carta Semestral enero 2026",
      "url_fuente": "https://...",
      "archivo": "data/funds/{ISIN}/raw/letters/2026-01-carta-semestral.pdf",
      "num_paginas": 6,
      "texto_completo": "..."  // o vacío si solo hay archivo
    }
  ]
}
```

Si `cartas` está vacío → no hay nada que procesar. Mensaje informativo y terminar.

## Schema EXACTO del output

Para CADA carta que aún NO tenga K15 estructurado, añadir estos campos al objeto carta:

```json
{
  "periodo": "2025-S2",
  ...campos existentes...,
  "tesis_gestora": "string (300-1500 chars). Cómo el gestor describe su filosofía y tesis principal en este periodo.",
  "decisiones_tomadas": "string (200-800 chars). Qué decisiones de cartera concretas tomó este periodo (entradas, salidas, ajustes de exposición).",
  "contexto_mercado": "string (300-1500 chars). Cómo describe el gestor el contexto macro del periodo.",
  "outlook": "string (300-1000 chars). Visión a futuro articulada en la carta.",
  "citas_textuales": ["cita literal 1", "cita 2", ...],
  "posiciones_mencionadas": [
    {"nombre": "Inversión X", "accion": "compra|venta|aumentado|reducido|mantenido", "racional": "string corto"}
  ],
  "resultado_real": "string (100-400 chars) o null. Qué rentabilidad se reportó vs benchmark.",
  "doc_type": "manager_presentation|carta_semestral|carta_trimestral|informe_mensual|annual_letter",
  "fuente_tipo": "pdf_discovery|web_scraping|manual",
  "_k15_extracted_at": "ISO timestamp",
  "_k15_model": "claude-opus-4-8 (cowork)"
}
```

Y al terminar TODAS las cartas, actualizar `letters_data.deep_extraction.stats`:

```json
"deep_extraction": {
  "stats": {
    "total": 25,
    "extracted_ok": 23,
    "skipped_already_done": 0,
    "skipped_no_text": 1,
    "extracted_error": 1
  },
  "ultima_actualizacion": "ISO"
}
```

## Workflow paso a paso

### 1. Validación pre-requisitos (1 turn)

Read `data/funds/{ISIN}/letters_data.json`. Contar cuántas cartas hay y cuántas faltan por procesar K15.

### 2. Backup defensivo (1 turn)

Bash:
```
cp data/funds/{ISIN}/letters_data.json data/funds/{ISIN}/letters_data.backup_pre_k15.json
```

### 3. Procesar cartas (1-2 turns por carta)

Para CADA carta sin `tesis_gestora` (i.e., aún no procesada):

#### Si tiene `texto_completo` populado
Usar ese texto directamente como input.

#### Si tiene solo `archivo` (PDF local)
Read del PDF usando mi `Read` tool nativo.

#### Si tiene solo `url_fuente`
WebFetch del URL para extraer contenido.

#### Si nada de lo anterior
Marcar como `skipped_no_text` y seguir.

#### Aplicar extracción K15
Razonar sobre el texto y extraer los 7 campos K15:

1. **tesis_gestora**: ¿Cómo describe el gestor su enfoque/filosofía en este periodo? Buscar frases tipo "nuestro objetivo es...", "la estrategia se basa en...", "preservamos capital mediante...".
2. **decisiones_tomadas**: ¿Qué decisiones concretas? "Hemos comprado...", "Vendimos...", "Aumentamos exposición a...", "Salimos de...".
3. **contexto_mercado**: ¿Cómo describe el entorno macro? "Los mercados se vieron afectados por...", "El periodo estuvo marcado por...".
4. **outlook**: ¿Qué espera para el futuro? "De cara al año/trimestre próximo...", "Mantenemos la convicción...".
5. **citas_textuales**: 2-5 citas LITERALES (entre comillas dobles) que mejor capturen el pensamiento del gestor en este periodo.
6. **posiciones_mencionadas**: Lista de empresas/activos mencionados explícitamente con la acción tomada.
7. **resultado_real**: Si la carta reporta rentabilidad del periodo (vs benchmark), capturarlo.

### 4. Reglas críticas

- **Citas LITERALES** (`citas_textuales`): copiar exactamente como aparecen en la carta. No parafrasear.
- **Idioma original**: respetar el idioma de la carta. Si está en inglés, K15 en inglés.
- **NO inventar decisiones**: si la carta no menciona compras/ventas concretas, dejar `decisiones_tomadas` corto o vacío.
- **NO confundir periodos**: cada K15 es del periodo de SU carta, no de periodos previos mencionados como contexto.
- **doc_type sensato**: distinguir entre carta semestral oficial, informe mensual breve, presentación a inversores, etc.

### 5. Tasks fallidas

Si una carta no se puede procesar (PDF corrupto, URL 404, contenido irrelevante), marcar como `extracted_error` con el motivo, NO inventar K15.

### 6. Actualizar stats finales

Tras procesar todas las cartas, actualizar `deep_extraction.stats` y `ultima_actualizacion`.

### 7. Mensaje final a Rafa

Confirma:
- Total cartas procesadas: extracted_ok / skipped_no_text / extracted_error
- Path de letters_data.json actualizado
- Periodos K15 cubiertos cronológicamente

## Modelo recomendado

- **Opus 4.8** — el bat lo fuerza vía `claude -p --model claude-opus-4-8` (var `MODEL_LETTERS`). Calidad-primero y coherente con el resto de skills.
- Lo crítico de calidad aquí es **no mezclar cartas de otro fondo** (contaminación cross-fund): verifica el fondo del documento antes de extraer K15, sea cual sea el modelo.

## Coste y rate limit

- Por carta: ~1-2 turns
- Por fondo (5-30 cartas): 5-50 turns
- Bajo Max: 0€ marginal
- Volumen: para fondos con muchas cartas (>20), puede requerir 2 sesiones Max

## Errores comunes a evitar

1. **Mezclar periodos**: el K15 de una carta es DEL PERIODO de esa carta, no del histórico que comenta como contexto.
2. **Inventar resultado_real**: si la carta no reporta rentabilidad explícita, deja `null`.
3. **Citas no literales**: NUNCA reformular una cita. Si no es literal, no es cita.
4. **Posiciones inventadas**: solo lista posiciones que SE MENCIONAN explícitamente con acción tomada.
5. **Saltar el backup**: SIEMPRE crear `letters_data.backup_pre_k15.json` antes de modificar.
6. **Procesar cartas ya extraídas**: si una carta ya tiene `tesis_gestora` populado, saltarla (skipped_already_done).
