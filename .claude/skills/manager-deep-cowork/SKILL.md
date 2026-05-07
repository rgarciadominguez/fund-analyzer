---
name: manager-deep-cowork
description: Identifica al equipo gestor (lead/co + biografía inicial) Y enriquece `manager_profile.json` extrayendo el texto completo de los artículos web encontrados sobre los gestores del fondo. Reemplaza `manager_deep_agent.py` (39 calls Gemini) Y `manager_profiler._enrich_with_opus()` (lead/co identification + bio enrichment). Filtra contaminación cross-fund (homónimos, otras personas con mismo nombre). Úsala SIEMPRE que Rafa diga "manager deep cowork", "enriquece manager X", "skill manager extraer X", "identifica equipo X", o cualquier variante. Espera que `manager_profiler.py` haya identificado URLs candidatas + nombres tentativos.
---

# manager-deep-cowork v1.1

Sustituto de `manager_deep_agent.py` (Gemini Flash) Y `manager_profiler._enrich_with_opus()` (Anthropic). Diseñada para correr bajo Claude Max sin coste API.

**Reemplaza**:
- `manager_deep_agent.run()` — extrae texto completo de URLs sobre gestores + filtra contaminación + populates `articulos_completos[gestor]`
- `manager_profiler._enrich_with_opus()` — identificación de lead/co manager + bio inicial enriquecida (post-Fase M usaba Haiku/Sonnet escalation)

## Pre-requisito obligatorio

Debe existir `data/funds/{ISIN}/manager_profile.json` con:
- `equipo_gestor`: lista de nombres canónicos de los gestores (post manager_profiler)
- `fuentes_web`: URLs candidatas con título y snippet
- O un archivo `pending_manager_deep.json` con tasks específicas

Si no existe → manager_profiler no se ejecutó. Aborta.

## Schema EXACTO del output

Actualizar `data/funds/{ISIN}/manager_profile.json` añadiendo o actualizando el campo `articulos_completos`:

```json
{
  "isin": "...",
  "equipo_gestor": ["Sebastian Lyon", "Francis Brooke"],
  "fuentes_web": [...],
  "articulos_completos": {
    "Sebastian Lyon": [
      {
        "fuente_url": "https://moneyweek.com/...",
        "titulo": "Sebastian Lyon: 'Win by not losing'",
        "fecha": "2020-09",
        "texto_completo": "...3000-8000 chars del artículo...",
        "relevancia_score": "high|medium|low",
        "incluido_porque": "Entrevista directa + tema central de la filosofía"
      }
    ],
    "Francis Brooke": [...]
  },
  "_known_public_undersourced": [],
  "_manager_deep_meta": {
    "skill_version": "1.0.0",
    "extracted_at": "ISO",
    "n_urls_evaluated": 12,
    "n_articulos_includados": 5,
    "n_descartados_homonimo": 3,
    "n_descartados_irrelevantes": 4
  }
}
```

## Workflow paso a paso

### 1. Validación pre-requisitos (1 turn)

Read `data/funds/{ISIN}/manager_profile.json`. Verifica `fuentes_web` no vacío.

Si `equipo_gestor` está vacío → la prep no identificó nombres tentativos. Continúa con paso 2 (identify_lead_co) que intentará identificarlos desde `fuentes_web` + `recursos_encontrados`. Si tras paso 2 sigue vacío → no hay nada que enriquecer; devuelve `_known_public_undersourced: ["fund_team_unidentified"]` y termina.

### 2. Identify lead/co (1-2 turns) — ABSORBE manager_profiler._enrich_with_opus

Razonar sobre `fuentes_web` + `recursos_encontrados` + `informacion_cnmv` + `informacion_cartas` para:

1. **Identificar nombres canónicos**: detectar candidatos a lead/co manager. Aplica:
   - Dedup acentual: "Álvaro Guzmán" == "Alvaro Guzman"
   - Cross-validation: el nombre debe aparecer en al menos 2 fuentes distintas (cartas firmadas, web gestora, entrevistas)
   - Excluir homónimos cross-fund (ver Casos Históricos abajo)
   - Máximo 2 nombres canónicos en `equipo_gestor` (lead + co)

2. **Asignar lead vs co**: usar evidencia de:
   - Quién firma las cartas habitualmente
   - Quién aparece como "Lead Manager" / "Senior PM" en biografías
   - Quién es entrevistado como portavoz del fondo
   - Si no hay evidencia clara para asignar lead/co → marcar todos como peers con `_opus_lead_confidence: "low"` y NO inventar lead arbitrario

3. **Bio inicial por persona**: 1-3 frases descriptivas con datos REALES de las fuentes (formación, años en la firma, otras responsabilidades). NO inventar.

4. **Update manager_profile.json**:
   ```json
   {
     "equipo_gestor": ["Lead Name", "Co Name"],
     "equipo": [
       {"nombre": "Lead Name", "cargo": "Lead Manager", "biografia_inicial": "...", "_source": "opus_high|medium|low"},
       {"nombre": "Co Name", "cargo": "Co Manager", "biografia_inicial": "...", "_source": "opus_high|medium|low"}
     ],
     "equipo_roles": {
       "Lead Name": {"is_lead": true, "_source": "opus_high"},
       "Co Name": {"is_co": true, "_source": "opus_high"}
     },
     "_opus_lead_confidence": "high|medium|low",
     "_known_public_undersourced": []
   }
   ```

### 3. Backup defensivo (1 turn)

Bash:
```
cp data/funds/{ISIN}/manager_profile.json data/funds/{ISIN}/manager_profile.backup_pre_deep.json
```

### 4. Procesar URLs por gestor (1-2 turns por gestor)

Para CADA gestor en `equipo_gestor`:

#### Filtrar URLs candidatas
De `fuentes_web`, identificar las URLs que mencionan a este gestor concreto. Filtros:
- URL contiene el nombre del gestor o su gestora
- Título o snippet menciona el gestor

#### Para cada URL filtrada
1. Si la URL ya tiene `text` extraído en `fuentes_web` → usar ese texto.
2. Si no → `WebFetch` el URL para extraer contenido.
3. Aplicar **filtro de relevancia y contaminación**:
   - **Relevancia alta**: artículo dedicado al gestor o entrevista directa
   - **Relevancia media**: el gestor aparece como participante o citado
   - **Relevancia baja**: solo mención de pasada → DESCARTAR
   - **Contaminación cross-fund (homónimo)**: persona con mismo nombre pero NO el gestor del fondo. DESCARTAR explícitamente. Casos típicos:
     - Político/funcionario con mismo nombre (verificar contexto)
     - Familiar/ancestro con mismo nombre (verificar fechas y contexto)
     - Persona de diferente industria
4. Si pasa los filtros, extraer:
   - `titulo`: del artículo
   - `fecha`: de publicación si aparece
   - `texto_completo`: 3000-8000 chars del contenido relevante (no el HTML completo)
   - `relevancia_score`: high|medium|low
   - `incluido_porque`: 1-2 frases justificando

### 4. Detección de contaminación cross-fund (CRÍTICO)

Casos históricos a vigilar:
- "Francis Brooke" del Trojan vs "General Alan Brooke" (WWII) vs "Francis John Brooke Jr" (US Treasury) vs "Francis Brooke" (White House) — todos tienen artículos en Google pero NO son el gestor.
- "Iván Martín" de Magallanes vs otros con mismo nombre.
- Verifica contexto: gestora correcta, industria financiera, fechas coherentes con la trayectoria conocida.

Si descartas una URL por contaminación, registra en `_manager_deep_meta.n_descartados_homonimo` y opcionalmente menciona el caso en `_known_public_undersourced`.

### 5. Manejo de gestores sin información

Si tras procesar todas las URLs un gestor termina con `articulos_completos[gestor] = []`:
- Añadir el gestor a `_known_public_undersourced` (siguiendo patrón Fase K)
- NO inventar trayectoria
- El analyst después usará la info disponible (manager_profile.equipo + biografia base)

### 6. Escritura final (1 turn)

Update `data/funds/{ISIN}/manager_profile.json` con `articulos_completos` poblado.

### 7. Mensaje final a Rafa

Confirma:
- N gestores procesados, N URLs evaluadas, N incluidas, N descartadas (contaminación + irrelevantes)
- Path del manager_profile.json actualizado
- Si hubo `_known_public_undersourced`, listarlos

## Reglas críticas

- **NO inventar trayectoria** si las URLs no la respaldan
- **DESCARTAR explícitamente** homónimos (no incluirlos como "extra info")
- **Fecha del artículo** SIEMPRE que aparezca en el contenido
- **texto_completo** debe ser EL TEXTO REAL del artículo, no resúmenes propios
- Si URL devuelve 404 o vacío → registrar en errores, no inventar
- Citas literales del gestor entre comillas dobles `"..."` SÍ son válidas (después el analyst las puede usar)

## Modelo recomendado

- Sonnet es suficiente para esta tarea (filtrado + extracción + relevancia)
- Opus solo si hay >20 URLs y necesitas filtrado más sofisticado

## Coste y rate limit

- Por gestor: 2-5 turns (depende cuántas URLs)
- Total fondo (1-2 gestores): 5-15 turns
- Bajo Max: 0€ marginal
- Volumen: 15-25 fondos por ventana 5h

## Errores comunes a evitar

1. **Incluir homónimos como info válida**: si el "Francis Brooke" del artículo es de WWII, descártalo. No mezcles trayectorias.
2. **Inventar fechas**: si no aparece en el artículo, deja `null`.
3. **Resumir el texto**: el `texto_completo` debe ser literal, no tu propio resumen.
4. **Dejar gestores con `[]` sin marcar como undersourced**: hay que apuntarlos explícitamente para que el analyst lo sepa.
5. **WebFetch sin límite de longitud**: si el texto es enorme (>20K chars), recorta a las secciones relevantes al gestor (no incluyas footers, sidebars, navegación).
