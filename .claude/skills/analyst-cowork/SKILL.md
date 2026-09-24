---
name: analyst-cowork
description: Genera el bloque `analyst_synthesis.*` (8 secciones narrativas + estructuradas) de un fondo del proyecto fund-analyzer usando la cuota de Claude Max. Reemplaza al `agents/analyst_agent.py` legacy. Úsala SIEMPRE que Rafa diga "analyst cowork", "analiza fondo X con cowork", "regenera síntesis de X via skill", "skill analyst X", "consume preview de X", "monta el analyst de X aquí", o cualquier variante sobre ejecutar la síntesis del analyst del fund-analyzer dentro de Cowork. NO la uses para ejecutar el pipeline de descarga (CNMV, PDFs, scraping) — eso sigue en Python. NO la uses para fondos que no han pasado antes por la prep determinista (`python -m agents.orchestrator --isin X --prep-only`).
---

# analyst-cowork v2.6

Sustituto del `agents/analyst_agent.py` del proyecto fund-analyzer. Genera el bloque `analyst_synthesis.*` con 8 secciones siguiendo el **schema EXACTO** que espera el dashboard renderer (`dashboard/generate_dashboard.py`). Diseñada para correr bajo Claude Max y eliminar el coste API de Anthropic.

**v2 (2026-05-04)**: corrige 3 schema mismatches críticos descubiertos en smoke test Avantage. Ahora usa exactamente los nombres de campo que el dashboard renderiza. Audit pass obligatorio.

**v2.2 (2026-05-04)**: añade campos que el quality_loop v1 chequea — `estrategia.fortalezas/riesgos/perfil_riesgo` y `cartera.top_posiciones`.

**v2.3 (2026-05-04)**: reglas estrictas de formato en `texto` para evitar headers disruptivos y listas inline apelmazadas (feedback visual usuario).

## GLOSARIO FINANCIERO (fondos especialistas / renta fija compleja)

Para fondos con estrategias que un inversor no experto no entendería de un vistazo (crédito,
retorno absoluto, derivados, CLO/estructurados, market-neutral, ILS/cat bonds, quant…), añade
`analyst_synthesis.glosario` = `[{termino, definicion, ejemplo_fondo}]` (3-6 entradas). Cada una:
- `termino`: la estrategia/concepto (ej. "Carry", "CDS", "CLO y bonos estructurados", "Bono perpetuo/AT1", "Market-neutral", "Cat bond/ILS").
- `definicion`: explicación BREVE y clara para no-experto (2-4 frases), sin jerga innecesaria; usa `**negrita**` para el término clave.
- `ejemplo_fondo`: cómo lo aplica ESTE fondo en concreto, anclado en datos reales de la cartera/cartas (posiciones, % derivados, emisores concretos). NO inventes; si no hay evidencia del uso, no incluyas el término.

El dashboard pinta una pestaña "Glosario" solo si esta sección existe. Para fondos plain-vanilla
(RV diversificada, indexados simples) NO hace falta glosario — omítelo.

## REGLAS DE CONTENIDO (aplican a TODOS los modos: full, annual_update y aporte)

Estas tres cosas faltaban sistemáticamente (caso MontLake) aunque los datos SÍ estaban en los extractos. Son obligatorias:

**R1 · Criterios de inversión: incorporar, EXPLICAR y RELATIVIZAR.** Busca en los extractos `criterios_inversion` (y en `cualitativo.estrategia`): spread objetivo, rating mínimo, tamaño/liquidez mínima del emisor, límites de duración/concentración/geografía/divisa. No los enuncies sin más: **explica qué significa cada uno y qué implica para el fondo**, y **relativiza las cifras** para que un lector no experto las entienda. Ejemplos del patrón: "+300 pb sobre la tasa libre de riesgo → solo compran crédito que paga al menos 3 puntos más que el bono sin riesgo; eso sesga la cartera hacia emisores con más rendimiento (y algo más de riesgo) y da un colchón de carry"; "capitalización mínima de $5bn → equivale a quedarse solo con emisores grandes y líquidos, del tamaño de un banco o una utility cotizada relevante: deja fuera las empresas pequeñas y mejora la liquidez y la información disponible"; "rating mínimo BBB- → todo investment grade, nada de high yield".

**R2 · Estructura de gestión: quién gestiona de verdad y por qué.** Usa `datos_clave.gestora_management_company`, `datos_clave.investment_manager`, `datos_clave.estructura_legal` y `estructura_gestion` del extracto. Muchos fondos usan una **plataforma/ManCo legal** (p.ej. Waystone, que crea paraguas como MontLake para agrupar fondos de distintas gestoras) que solo lleva **regulatorio, legal y administración**, mientras el **gestor de inversión real** (p.ej. Fortune) toma las decisiones. Explica este reparto de roles y **por qué existe** (da acceso UCITS/distribución a gestoras boutique). **Atribuye al gestor REAL** (no a la plataforma) el equipo gestor, el AUM de la gestora y el nº de fondos que gestiona; nombra a la plataforma solo en su papel. Si son la misma entidad, dilo en una línea.

**R3 · Novedades = temas de fondo, no rentabilidad de un mes.** Cuando hay docs nuevos (aporte o update anual), lo que importa son los **principales temas del último periodo**: decisiones de inversión tomadas, **cambios en cartera y en estrategia**, contexto y **visión a futuro de los gestores** (`cualitativo.decisiones_periodo`, `cualitativo.outlook`, `vision_gestores`). La rentabilidad de un mes concreto NO es una novedad relevante por sí sola; solo cítala si explica una decisión.

**R4 · Gráficos de evolución.** Si los extractos traen `sector_allocation_history` / `geographic_allocation_history` / `asset_allocation_history` / `rating_allocation` (o snapshots de presentaciones), **úsalos en la narrativa de Evolución/Cartera** (qué ha cambiado en la exposición por sector/tipo de activo/geografía/calidad crediticia y por qué) — son de los datos más valiosos que puede traer un documento. En renta fija comenta SIEMPRE el reparto por rating (cuánto investment grade vs high yield, dónde se concentra) si está disponible.

**R4b · Gráficos del documento aportado (`graficos_documento`).** Si un extracto de doc aportado trae `graficos_documento` (catálogo de páginas con gráficos, cada una con `seccion`, `tipo` y `lectura`), esas páginas se **incrustan tal cual en el dashboard** (Cartera → "Evolución de la cartera según el gestor"; Evolución → "Evolución histórica según el gestor"). Tu trabajo es **usar su `lectura` en la narrativa**, no describir el gráfico: en `cartera` y `estrategia` explica cómo ha EVOLUCIONADO la cartera en el tiempo (p.ej. peso investment grade vs no-IG, yield y duración frente al índice, estructura de deuda senior/subordinada, bancos vs seguros, rotación) y **qué decisiones del gestor revelan esos cambios**; en `evolucion`, patrimonio y flujos, atribución por año, volatilidad y drawdown frente a comparables. Enlaza con los criterios de R1 (¿la evolución es coherente con lo que dicen que hacen?). Los niveles leídos de un eje van con "≈". Son datos de la gestora: dilo una vez ("según la presentación de la gestora"). No inventes cifras que la `lectura` no dé.

**R4c · Clases (`clases_documento`).** Si existe, la tabla de clases del dashboard sale de ahí. En `resumen`/`estrategia` explica en llano **qué clase es la analizada** (código, divisa, si cubre divisa y qué implica, acumulación/reparto), **cuánto cuesta** (gestión + comisión de éxito con su base: sobre qué referencia y si hay marca de agua) y **qué otras clases relevantes hay** para un inversor español (la institucional más barata y su mínimo, la equivalente en USD). Nunca digas "no cobra comisión de éxito" si `comision_exito_pct` > 0.

**R5 · Track record con vehículos predecesores.** Si algún extracto trae `track_record_lineage` (o `performance[].vehiculo`), el histórico de rentabilidad NO es todo del fondo actual: **dilo explícitamente** en Evolución/Historia — qué tramo corresponde a qué vehículo (certificado, RAIF, fondo previo, con su ISIN y fechas), que es la MISMA estrategia/equipo, y desde cuándo existe el vehículo actual. Es legítimo usar ese histórico para tener más track, pero el lector debe verlo claro de un vistazo; nunca lo presentes como si el fondo actual tuviera esa antigüedad.

**R6 · EJES DE DIFERENCIACIÓN (Rafa, 2026-09-24) — OBLIGATORIO en todos los modos.** El análisis tiene que dejar claro en qué se
diferencia este fondo de otros, en CUATRO ejes, y en cada uno decir si cambia con el tiempo y POR QUÉ (con la evidencia: qué informe,
carta o año lo muestra). Van como campo estructurado `estrategia.diferenciacion` (schema más abajo) Y deben estar desarrollados en la
narrativa (estrategia/cartera/gestores/evolución):
1. **Activos**: en qué tipo de activos invierte y cómo cambia el mix (RV/RF/liquidez/otros; subtipos: HY, IG, subordinada, small caps…).
   Si cambia, por qué (mandato, visión, ciclo, entradas/salidas de dinero). Si el mandato no le deja cambiar, dilo.
2. **Gestión**: tipo de gestión (autor vs equipo vs casa), cómo se toman las decisiones (comité, gestor único, modelo cuantitativo),
   quién controla al gestor (órgano de control: comité de riesgos, depositario, consejo, límites del folleto) y skin-in-the-game.
3. **Geografía**: mix geográfico y cómo cambia; por qué; y si el mandato permite cambiarlo.
4. **Filosofía y equipo**: filosofía y estrategia de inversión + expertise real del equipo (años, especialidad, track record previo).
Sin datos para un eje: dilo explícitamente ("no consta en los informes") en vez de rellenar. Nada genérico: cada eje con cifras,
años o nombres concretos cuando existan.

## MODO UPDATE ANUAL (v2.5 — solo el delta del último año)

**Antes de generar nada, lee `data/funds/{ISIN}/config.json`. Si `modo == "annual_update"`, NO rehagas el análisis desde cero: actualiza el existente solo con el delta del último año.**

En ese modo:

1. **Punto de partida**: carga el `analyst_synthesis` YA EXISTENTE de `output.json` y el `since_date` del config (= `fecha_ultimo_analisis`; si falta, últimos 12 meses). Todo lo anterior a esa fecha SE DA POR INCORPORADO — no lo re-mires ni lo reescribas.

2. **Mira SOLO lo nuevo** (desde `since_date`): informe anual/semianual más reciente, cartas/comentarios del gestor del último año, cambios de cartera (altas/bajas/rotación) y de exposición (sector, geografía, divisa, duración, crédito), cambios de equipo gestor, comisiones (TER), tamaño (AUM) y política de la clase. Las fuentes nuevas están en el bundle/prep (docs con fecha posterior a `since_date`).

3. **INTEGRA en la narrativa — NO apiles bloques** (regla de oro): el resultado es UN análisis coherente y actualizado, no el viejo con un anexo pegado.
   - Donde el dato del último año **cambia, refina o contradice** algo del texto previo (cartera, exposición, tesis, equipo, comisiones, tamaño), **reescribe esa frase/párrafo integrando el dato nuevo** — para que quede un solo relato sin duplicados ni contradicciones. Preserva las CONCLUSIONES y el peso del análisis previo: ajustas el texto, no lo tiras.
   - Lo que NO ha cambiado, déjalo VERBATIM (no reescribas por reescribir).
   - **RECONSTRUCCIÓN DIRIGIDA (2026-09-24):** si tu `novedades_resumen.veredicto.estado` va a ser `cambia`, o un
     `hueco_de_fondo` afecta a la estrategia/tesis/equipo/cartera, NO complementes esas secciones: REGENÉRALAS COMPLETAS con todo
     el material (bundle previo + nuevo), como en un análisis full, y sigue preservando verbatim las secciones no afectadas. Un
     análisis previo flojo no se arregla añadiendo párrafos.
   - **NO añadas bloques `**Novedades {año}**` al final de las secciones.** El "qué ha cambiado y por qué" va APARTE, en `novedades_resumen` (pestaña Novedades). Si el texto previo traía un bloque `**Novedades {año-1}**` de un run viejo, fúndelo en el cuerpo y elimínalo.
   - Actualiza los campos estructurados (KPIs, top_posiciones, perfil_riesgo, comisiones…) SOLO donde el dato nuevo difiera del anterior. Si un dato no ha cambiado, déjalo idéntico.

4. **Cuantitativo**: lo refresca el pipeline Python (NAV → métricas → sync-metricas). El delta cualitativo va integrado en el texto + resumido en `novedades_resumen`.

5. **Emite** el `analyst_synthesis` completo (integrado y coherente). El cierre (rodar `fecha_proximo_analisis` +1 año, limpiar la tarea de vencido, volver a "Categorizar") lo hace el worker Python — tú no lo tocas.
6. **`novedades_resumen` (OBLIGATORIO) — formato EJECUTIVO v2.** Rafa YA revisó el análisis anterior. Abre esta pestaña para saber, en 30 segundos: (a) si aquel análisis **se sostenía y estaba completo**, (b) qué le faltaba **DE FONDO**, y (c) qué has encontrado que **influya en la tesis, la estrategia, la cartera o el riesgo**. NO es un registro de tus ediciones ni un inventario de datos añadidos. Esquema:
```json
{"modo": "aporte|annual_update", "fecha": "YYYY-MM",
 "veredicto": {"estado": "se_mantiene|se_matiza|cambia", "texto": "1-2 frases: ¿el análisis previo era correcto y completo? ¿cambia la opinión sobre el fondo y en qué dirección?"},
 "huecos_de_fondo": [{"titulo": "≤10 palabras", "detalle": "1-2 frases: qué faltaba o estaba mal ENTENDIDO en el análisis previo y por qué importa"}],
 "hallazgos": [{"titulo": "≤10 palabras", "impacto": "tesis|estrategia|cartera|riesgo|costes|equipo", "detalle": "1-2 frases: el hecho + su implicación para quien invierte"}],
 "sin_cambios": "1 frase: lo esencial que se confirma"}
```
Reglas duras: **máx. 3 huecos y 4 hallazgos**, `titulo` ≤ 8 palabras y `detalle` ≤ 180 caracteres (UNA idea: el hecho y su consecuencia; sin cifras de relleno), ordenados por importancia. Se muestran como tarjetas a todo el ancho: si no cabe en una tarjeta, sobra. Si no hay huecos de fondo, `huecos_de_fondo: []` y dilo en el veredicto ("estaba completo"). **Prueba de relevancia** para cada punto: *¿cambiaría esto lo que Rafa le dice a un cliente sobre el fondo, o cómo lo encaja en una cartera?* Si no, FUERA. Quedan fuera por definición: "se ha añadido una tabla/gráfico/sección", precisiones de cifras (AUM, TER al decimal), rentabilidad de un mes, nombres de posiciones sueltas, que el equipo tiene más analistas, "uso de gráficos de la gestora", y cualquier descripción de TU trabajo. Ejemplos de lo que SÍ es un hallazgo: "El 78% del patrimonio es dinero del propio grupo gestor", "La cartera pasó de 40% a 68% investment grade al convertirse en UCITS: el track record previo se hizo con más riesgo del que hoy puede tomar", "Cobra 10% de éxito sobre el tipo sin riesgo — el análisis previo decía que no cobraba". Escribe llano, sin jerga sin explicar. (Compatibilidad: NO uses ya el campo `puntos`.) En annual_update el veredicto responde además: ¿qué ha cambiado este año que altere la tesis? Si `sin_novedades` es true, no lo emitas.

**Sin novedades**: mira `data/funds/{ISIN}/intl_discovery_data.json` → `annual_update.sin_novedades`. Si es `true` (discovery no encontró NINGÚN AR/SAR/carta posterior a `since_date`), NO reescribas nada: emite el `analyst_synthesis` previo TAL CUAL (sin bloque Novedades). El cierre solo rodará la fecha. No inventes cambios que no hay.

## MODO APORTE (v1 — mejora con material aportado: complementar, NO rehacer)

**Si `data/funds/{ISIN}/config.json` tiene `modo == "aporte"`, NO rehagas el análisis desde cero: COMPLEMENTA el existente con el material aportado por Rafa.** (No hay discovery en este modo — usa SOLO el aporte + lo que el fondo ya tiene.)

En ese modo:
1. El material aportado está **extraído y marcado como fuente prioritaria** (tasks con `aportado:true` en `pending_extraction.json` → holdings/datos fiables) y/o en `data/funds/{ISIN}/aportados/` y `analisis_externos`. Léelo y compáralo con el `analyst_synthesis` PREVIO.
2. **INTEGRA en la narrativa, preservando las conclusiones — NO apiles un "Complemento".** El resultado es UN análisis coherente y mejorado, no el viejo con un bloque pegado. En las secciones que el aporte enriquezca (resumen/estrategia/cartera/gestores/evolución/consistencia):
   - Donde el aporte **aporta, refina, corrige o contradice** algo del texto previo (cartera, tesis, estrategia, equipo, cifras cualitativas), **reescribe esa frase/párrafo integrando el dato nuevo** — sin duplicar ni contradecirte. NO quites peso a las conclusiones previas: las integras con lo nuevo, no las tiras.
   - Explica en el propio relato los puntos CLAVE que el aporte aclara (p.ej. qué significa y qué implica un objetivo de spread mínimo), no solo los enuncies.
   - Confirma (integrado en el texto) qué **SIGUE IGUAL** en estrategia / filosofía / equipo.
   - **NO añadas bloques `**Complemento (aporte)**`.** El "qué has cambiado/mejorado y por qué" va APARTE, en `novedades_resumen`.
3. Si una sección NO la toca el aporte, no la reescribas (el consumidor preserva verbatim las que no emitas).
   **RECONSTRUCCIÓN DIRIGIDA (2026-09-24):** si tu `novedades_resumen.veredicto.estado` va a ser `cambia`, o un `hueco_de_fondo`
   afecta a la estrategia/tesis/equipo/cartera, REGENERA COMPLETAS esas secciones con todo el material (análisis previo + aporte),
   no las complementes. Caso real: Gamma Global (aporte 24-sep) dio veredicto "cambia" y dejó una estrategia genérica heredada.
4. **Cuantitativo**: no cambia (el aporte no re-descubre NAV). No inventes cambios que el aporte no soporte.
5. **`revision_pendiente` (OBLIGATORIO en aporte)**: emite en el JSON, junto a `analyst_synthesis`, una lista `revision_pendiente` con los **datos frescos del aporte que NO has propagado a lo estructurado** y conviene reconciliar cuando lleguen los informes oficiales completos. Típicamente:
   - Un **KPI que no cuadra**: el doc trae un AUM/TER/YTM más reciente que NO coincide con `kpis.*` (que dejas intacto). → item con el valor del doc, el del KPI y la fecha.
   - Una **corrección o dato de equipo/cartera que solo quedó en prosa** (p.ej. "el fondo lo gestionan 3, no 2" o holdings nuevos) y NO está en `cualitativo.gestores` / `posiciones.actuales`.
   Cada item: `{"titulo": "...", "detalle": "qué dice el aporte vs qué hay en los datos, y qué reconciliar", "fuente": "nombre del doc aportado", "fecha": "YYYY-MM"}`. **Máximo 3 y solo MATERIALES** (un KPI de cabecera que no cuadra, el equipo gestor, la comisión): nada de datos de un mes, duraciones puntuales ni precisiones menores — si dudas, no lo pongas. Si el aporte NO deja ningún desajuste (todo cuadra), emite `revision_pendiente: []`. **No dupliques** ítems que ya estén (el consumidor deduplica por título). Estos ítems se muestran en la pestaña "Novedades" (bloque "A reconciliar") y se limpian solos en la actualización anual.
6. **`novedades_resumen` (OBLIGATORIO) — formato EJECUTIVO v2.** Rafa YA revisó el análisis anterior. Abre esta pestaña para saber, en 30 segundos: (a) si aquel análisis **se sostenía y estaba completo**, (b) qué le faltaba **DE FONDO**, y (c) qué has encontrado que **influya en la tesis, la estrategia, la cartera o el riesgo**. NO es un registro de tus ediciones ni un inventario de datos añadidos. Esquema:
```json
{"modo": "aporte|annual_update", "fecha": "YYYY-MM",
 "veredicto": {"estado": "se_mantiene|se_matiza|cambia", "texto": "1-2 frases: ¿el análisis previo era correcto y completo? ¿cambia la opinión sobre el fondo y en qué dirección?"},
 "huecos_de_fondo": [{"titulo": "≤10 palabras", "detalle": "1-2 frases: qué faltaba o estaba mal ENTENDIDO en el análisis previo y por qué importa"}],
 "hallazgos": [{"titulo": "≤10 palabras", "impacto": "tesis|estrategia|cartera|riesgo|costes|equipo", "detalle": "1-2 frases: el hecho + su implicación para quien invierte"}],
 "sin_cambios": "1 frase: lo esencial que se confirma"}
```
Reglas duras: **máx. 3 huecos y 4 hallazgos**, `titulo` ≤ 8 palabras y `detalle` ≤ 180 caracteres (UNA idea: el hecho y su consecuencia; sin cifras de relleno), ordenados por importancia. Se muestran como tarjetas a todo el ancho: si no cabe en una tarjeta, sobra. Si no hay huecos de fondo, `huecos_de_fondo: []` y dilo en el veredicto ("estaba completo"). **Prueba de relevancia** para cada punto: *¿cambiaría esto lo que Rafa le dice a un cliente sobre el fondo, o cómo lo encaja en una cartera?* Si no, FUERA. Quedan fuera por definición: "se ha añadido una tabla/gráfico/sección", precisiones de cifras (AUM, TER al decimal), rentabilidad de un mes, nombres de posiciones sueltas, que el equipo tiene más analistas, "uso de gráficos de la gestora", y cualquier descripción de TU trabajo. Ejemplos de lo que SÍ es un hallazgo: "El 78% del patrimonio es dinero del propio grupo gestor", "La cartera pasó de 40% a 68% investment grade al convertirse en UCITS: el track record previo se hizo con más riesgo del que hoy puede tomar", "Cobra 10% de éxito sobre el tipo sin riesgo — el análisis previo decía que no cobraba". Escribe llano, sin jerga sin explicar. (Compatibilidad: NO uses ya el campo `puntos`.)

Si `modo` NO es `annual_update` ni `aporte` (o no hay config), genera el análisis COMPLETO como siempre (resto de esta skill) y NO emitas `revision_pendiente` ni `novedades_resumen`.

## REGLAS DE FORMATO en `texto` fields (críticas — v2.4)

Las siguientes pautas aplican a TODOS los campos `texto` y `trayectoria`/`filosofia` largos. **Asume que el dashboard renderiza `**Header**\n\n` como sub-header sutil** (font-size 15px, bold, sin border horizontal, sin caps). Si tu instalación del dashboard aún tiene el render antiguo con border + caps, deja headers en línea con `—` como fallback (versión v2.3).

### Regla 1 (v2.4): SÍ usa `**Header**\n\n` para sub-secciones — el renderer los pinta sutil.

Para dividir secciones largas (>2K chars) en bloques temáticos, usa headers standalone. El dashboard renderiza tipo "subtitle" sin border ni text-transform.

✅ BIEN:
```
... párrafo previo.

**Origen y fundación de Troy (2000)**

Troy Asset Management fue fundada en Londres en el año 2000...

**Lanzamiento del sub-fondo Irlandés (febrero 2012)**

La fecha del lanzamiento es notable...
```

Pautas:
- Headers descriptivos pero no demasiado largos (3-12 palabras).
- Sin "(YYYY)" final salvo cuando aporta contexto temporal claro.
- 1ª letra en mayúscula, resto del header en minúsculas (no all-caps).
- Máximo 3-5 sub-headers por sección. Más es exceso de fragmentación.

### Regla 2: Listas con periodos temporales — cada periodo en su propio párrafo, NO bullets.

Los periodos son párrafos narrativos con cuerpo amplio, no items de lista corta. Mantenlos como párrafos con `**YYYY**:` como label inline.

✅ BIEN — cada periodo es su propio párrafo con label en bold:
```
A lo largo de su historia el fondo ha enfrentado:

**2013-2015**: periodo de discusión sobre tapering de la Fed...

**2022**: año de fuertes correcciones en RV+RF por subidas de tipos...

**2023**: año del rendimiento del 4,8% del bono del Tesoro a 10 años...
```

❌ MAL — todo apelmazado:
```
A lo largo de su historia el fondo ha enfrentado: - 2013-2015: ... - 2022: ...
```

❌ TAMPOCO — bullets para periodos largos (los items quedan como tarjetas extrañas):
```
- **2013-2015**: largo párrafo descriptivo...
- **2022**: largo párrafo descriptivo...
```

### Regla 3 (v2.4): Enumeraciones cortas con label — SÍ usa bullets markdown explícitos.

Cuando enumeras 3-6 criterios/principios/escenarios con cuerpo corto (≤500 chars cada uno), usa lista markdown explícita. El dashboard renderiza como `<ul>` con `<li>` correctos.

✅ BIEN:
```
El proceso combina criterios sistemáticos:

- **Adición contracíclica**: añadir a posiciones existentes y abrir nuevas cuando los mercados caen.
- **Salida disciplinada**: salir de posiciones por incertidumbre sobre el crecimiento futuro de beneficios.
- **Gestión activa de oro**: ajustar la ponderación de metales preciosos según ciclo y valoración relativa.
- **Disciplina de duración**: reducir la duración media ponderada de los bonos en escenarios de tipos en máximos.
```

Diferencia con la Regla 2: aquí cada item es **corto** (1-3 sentences). Si los items son párrafos largos, usa Regla 2 (párrafos con label inline) en vez de bullets.

### Regla 4: `**bold**` solo para énfasis dentro de un sentence o como label de párrafo, NUNCA para envolver párrafos enteros.

❌ MAL: `**Todo este párrafo en bold porque parece importante.**`

✅ BIEN: `Texto normal con **palabras clave** o **conceptos importantes** en bold.`

### Regla 5: Separación de párrafos siempre con `\n\n`.

`\n` simple no separa párrafos en el render del dashboard. Cualquier salto de párrafo debe ser doble newline.

### Regla 6 (v2.4): No abuses de sub-headers. Máximo 3-5 por sección.

Si una sección tiene más de 5 sub-headers, probablemente cabe reorganizar agrupando temas. Más de 5 fragmenta la lectura. Menos de 2 hace que la sección lea como un bloque único difícil de escanear.

## Pre-requisito obligatorio

El usuario debe haber ejecutado primero la prep determinista:
```
cd /ruta/a/fund-analyzer
python -m agents.orchestrator --isin {ISIN} --prep-only
```

Esto deja en `data/funds/{ISIN}/bundle/`:
- `fund_data.json` (cnmv_data.json o intl_data.json normalizado)
- `manager_profile.json`
- `letters_data.json`
- `readings.json`
- `sources.json`
- `bundle_manifest.json`
- `human_feedback.json` **(opcional, T3.7)** — feedback humano APPLIED del usuario sobre el análisis anterior

Si falta cualquiera de los 5 inputs **obligatorios**, ABORTA y pide a Rafa que ejecute la prep antes. `human_feedback.json` es opcional — solo aparece si el usuario ha guardado feedback con `📝 Mejorar este análisis` y lanzado un re-run con `♺` o `--apply-feedback`.

## Human feedback (T3.7, 2026-05-28) — INSTRUCCIÓN PRIORITARIA

Si existe `bundle/human_feedback.json`, **léelo antes de generar cada sección**. Estructura:

```json
{
  "isin": "...",
  "n_relevant_items": 3,
  "items": [
    {
      "feedback_id": "fb_xxx",
      "item_idx": 0,
      "raw_text_hint": "El resumen no menciona la estrategia value...",
      "target_path": null,
      "target_section": "resumen",
      "action": "revisar",
      "value": null,
      "confidence": "high",
      "source_urls": ["https://..."],
      "rationale": "usuario dice que falta tratamiento de value en el resumen"
    }
  ]
}
```

Reglas:
1. **Por cada item con `target_section`**, al generar ESA sección, considera el `rationale` y `raw_text_hint` como instrucción del usuario que PRIORIZA sobre la generación automática previa. Tienes que reflejar el feedback.
2. **Por cada item con `source_urls`** que apliquen a una sección, intenta incorporar la información de esas URLs en `analyst_synthesis.fuentes_externas.texto` o como referencia en la sección target.
3. **Items con `action=revisar` y sin `target_section`** (global): aplica el feedback al contexto general de TODAS las secciones (suele ser feedback de calidad/tono).
4. **Confianza humana > automática**: si el usuario contradice algo que el analyst anterior dijo, el usuario gana.

### Regeneración dirigida por CALIDAD (2026-09-24) — `data/funds/{ISIN}/quality_regen.json`

Si existe ese fichero, lo ha escrito el control de calidad tras el primer pase (`tools/quality_regen.py`). Estructura:
`{"secciones": ["estrategia", "gestores"], "motivos": {"estrategia": ["Estrategia con pocas cifras (2). Parece genérica.", ...]}, "intento": 1}`.
Regenera **SOLO esas secciones, COMPLETAS y corrigiendo de raíz los motivos listados** (cifras concretas, ejes de diferenciación,
equipo con nombres y expertise), y emítelas en `analyst_synthesis`; el consumidor preserva verbatim el resto. Es una única pasada:
no habrá una segunda. No toques `novedades_resumen` salvo que esté entre las secciones.

### Regeneración selectiva (2026-06-06) — IMPORTANTE

Cuando **TODOS** los items de `human_feedback.json` tienen `target_section` (es decir, el feedback apunta a secciones concretas, no es global):

- **Regenera SOLO esas secciones** y emítelas en `analyst_synthesis`. NO regeneres las demás: el consumidor (`orchestrator._consume_cowork_analyst`) preservará verbatim las secciones que NO incluyas, copiándolas del análisis anterior. Esto evita empeorar pestañas que el usuario no pidió tocar (caso real: feedback sobre `cartera`/`evolucion` no debe alterar `gestores`) y ahorra tokens.
- Si **algún** item es global (sin `target_section`), regenera TODAS las secciones como siempre.

**REGEN POR DEFECTO = COMPLETA (2026-06-11, crítico):** salvo el caso de feedback dirigido por sección de arriba, **SIEMPRE regenera las 8 secciones COMPLETAS con el formato actual del schema**. NO hagas regen parcial ni reutilices una síntesis previa aunque los inputs del bundle parezcan idénticos (mismos SHA256): el FORMATO/PROMPT evoluciona entre runs (p.ej. se añadieron `resumen_general`, sub-pestañas, sectores), así que una síntesis previa "con los mismos inputs" puede tener el formato VIEJO. Emite siempre las 8 secciones.

### Veredicto por item (2026-06-06) — OBLIGATORIO: `_meta.feedback_outcomes`

Por **cada** item de `human_feedback.json` que proceses, añade una entrada a `_meta.feedback_outcomes` declarando HONESTAMENTE si conseguiste lo que el usuario pidió:

```json
"feedback_outcomes": [
  {
    "feedback_id": "fb_xxx",
    "item_idx": 2,
    "resolved": false,
    "reason": "La cartera real del fondo solo reporta 8 posiciones en la última fuente disponible (informe Dic-2024), no 10. No puedo inventar 2 posiciones más. El número '10' que aparecía era un error de plantilla, ya corregido a 8 en la narrativa."
  },
  {
    "feedback_id": "fb_xxx",
    "item_idx": 0,
    "resolved": true,
    "reason": "Añadida la evolución completa de AUM por periodo en evolucion.datos_graficos y narrada en evolucion.texto."
  }
]
```

Reglas del veredicto:
- `resolved: true` SOLO si de verdad corregiste/mejoraste lo que el usuario señaló. `reason` = qué cambiaste.
- `resolved: false` si NO pudiste (faltan datos en el bundle, el dato pedido no existe, la fuente contradice lo pedido). `reason` = por qué no, con concreción (qué fuente, qué cifra real). **Es preferible un `false` honesto que un `true` falso**: el `false` se muestra al usuario en ámbar con tu razón, para que sepa que ese punto sigue pendiente y por qué.
- `feedback_id` y `item_idx` deben coincidir EXACTAMENTE con los del item en `human_feedback.json` (la verificación los emparea por esos dos campos).
- No inventes datos para poder marcar `resolved: true`. La honestidad del veredicto es el objetivo de este campo.

## Inputs cualitativos del bundle (CRÍTICOS para narrativa)

`fund_data.json` (renombrado de cnmv_data.json para fondos ES, intl_data.json para INT) contiene un campo `cualitativo` con texto pre-extraído de los PDFs semestrales CNMV o annual reports INT. **DEBES leer y USAR estos campos** para enriquecer las secciones narrativas. Si no los usas, las secciones saldrán pobres y genéricas.

Campos planos (texto del último periodo disponible):

- `fund_data.cualitativo.contexto_mercado` (string ~150-250 palabras): visión de la gestora sobre el entorno macro y mercado durante el último periodo. **Usar en**: `resumen.texto`, `evolucion.texto`, `estrategia.texto` para anclar el análisis al contexto real.
- `fund_data.cualitativo.decisiones_tomadas` (string ~100-300 palabras): decisiones de inversión del último periodo (compras, ventas, ajustes), con nombres de activos. **Usar en**: `cartera.texto` (narrativa de movimientos recientes), `estrategia.texto` (cómo se ejecuta la tesis).
- `fund_data.cualitativo.tesis_gestora` (string ~100-200 palabras, opcional): tesis o filosofía expresada en este periodo. **Usar en**: `resumen.filosofia_inversion`, `estrategia.texto`.
- `fund_data.cualitativo.perspectivas` (string ~100-200 palabras, opcional): outlook expresado para el próximo periodo. **Usar en**: `evolucion.texto` (cierre prospectivo), `estrategia.texto` (visión a futuro).

Campo histórico (todos los periodos):

- `fund_data.cualitativo._historico` (dict por periodo: `{"2024_H2": {contexto_mercado: ..., decisiones_tomadas: ..., ...}, "2023_H2": {...}, ...}`). **Usar en**:
  - `historia.texto`: narrar la evolución del fondo periodo a periodo, citando contexto y decisiones de cada año (esto es lo que da riqueza a la cronología).
  - `historia.hitos[]`: cada periodo con cambios significativos puede ser un hito (`{anio, titulo, evento, tipo: "contexto_mercado"}` o `tipo: "cambio_estrategia"`).
  - `evolucion.texto`: hilar la evolución de AUM/posiciones con el contexto de cada periodo.

### Reglas de uso

1. **Cita del periodo**: cuando uses datos del histórico, indica el periodo entre paréntesis: `"En 2024 H2 la gestora destacó (...)" `.
2. **No inventar**: si un campo es null, NO inventes. Si todo el histórico está vacío, di explícitamente que no hay datos cualitativos disponibles.
3. **Prioridad reciente**: para `resumen` y `estrategia` (visión actual), usa los campos planos (último periodo). Para `historia` y `evolucion` usa `_historico` (todos).
4. **Diferenciación ES vs INT**:
   - ES: estos campos vienen de PDFs semestrales CNMV (sección 9, sección 10 perspectivas).
   - INT: estos campos pueden venir de annual reports / factsheets / commentaries del sub-fondo. Mismo schema, distinta procedencia.

## Histórico ESTRUCTURADO multi-año (cartera/exposición año a año) — CRÍTICO para consistencia

Además del `_historico` cualitativo, el bundle trae el histórico CUANTITATIVO reconstruido de los
Annual Reports de VARIOS años (no solo el último). **DEBES analizarlo y COMENTARLO** — es lo que
permite ver si el fondo/equipo es consistente o ha virado. Campos en `fund_data`:

- `fund_data.posiciones.historicas[]` = una entrada por año: `{periodo, top10, holdings[], num_posiciones, aum_meur}`. `holdings` es la cartera de ESE año (nombre, peso_pct, sector, país).
- `fund_data.cuantitativo.mix_activos_historico[]` = `{periodo, renta_variable_pct, renta_fija_pct, liquidez_pct, otros_pct}` por año.
- `fund_data.cuantitativo.mix_geografico_historico[]` = `{periodo, zonas:{región:pct}}` por año.
- `fund_data.cuantitativo.serie_rentabilidad[]` = `{periodo, clase, rentabilidad_pct, benchmark_pct}` por año.

**Cómo usarlo (compara año-1 vs año, no describas solo el último):**
- `historia.texto`: narra la EVOLUCIÓN real de la cartera y la exposición a lo largo de los años, con cifras (ej. "la exposición a EE.UU. bajó del 40% en 2020 al 24% en 2024 y repuntó al 31% en 2025"). Convierte los cambios en `historia.hitos[]` (`tipo: "cambio_cartera"` / `"rotacion_geografica"` / `"cambio_estrategia"`).
- `estrategia.texto`: valora la **CONSISTENCIA** — ¿la cartera y el estilo confirman la tesis declarada a lo largo del tiempo, o ha habido deriva de estilo (style drift)? Cita nombres que entraron/salieron y rotaciones sectoriales concretas.
- `cartera.texto`: **cambios estructurales de cartera** — compara los `holdings`/`top10` actuales con los de años anteriores: qué posiciones son de convicción persistente (aparecen varios años), cuáles rotaron, cambios de concentración (`num_posiciones`), giros sectoriales/geográficos. Con periodos y % concretos.
- `evolucion.texto`: hila AUM + rentabilidad vs benchmark por año (`serie_rentabilidad`) con el mix histórico — comportamiento en mercados alcistas/bajistas (ej. protección en el año de caída), y si el resultado es coherente con la estrategia.
- `gestores.texto`: si el histórico revela que el equipo ha mantenido (o no) el proceso a lo largo de los años, coméntalo como evidencia de disciplina/consistencia del equipo.

**Reglas**: cita SIEMPRE el periodo y la cifra; NO inventes (si solo hay 1 año de histórico, dilo y no fuerces comparaciones); prioriza los cambios ESTRUCTURALES y significativos sobre el ruido año a año. Este análisis histórico es una de las partes de MÁS valor del informe — no lo omitas si hay `posiciones.historicas` con ≥2 años.

## LINEAGE / vehículo predecesor (`fund_data._lineage`) — usar si existe

Si el bundle trae `fund_data._lineage` (el fondo actual es el relanzamiento de una estrategia que ya
existía en otro vehículo: AMC/RAIF/Cayman → UCITS, o un fondo renombrado), **incorpóralo a la narrativa**:
- `historia.texto`: cuenta la evolución REAL de la estrategia desde su `strategy_inception` (no desde el
  lanzamiento legal): qué vehículos la albergaron y cuándo (`predecessors[]`: nombre, tipo, from→to),
  mismo gestor/equipo. Ej.: "la estrategia se gestiona desde 2018, primero como AMC, luego RAIF (2021) y
  desde 2024 como UCITS".
- `estrategia`/`evolucion`: al valorar CONSISTENCIA y comportamiento en mercados (alcista/bajista), usa
  el track-record COMPLETO — los cuantitativos ya vienen extendidos al histórico del predecesor
  (`analisis_cuantitativo.rendimiento_diario`, con `_lineage`). Comenta p.ej. el comportamiento en el año
  malo de renta fija 2022 aunque el UCITS sea de 2024.
- **Caveat OBLIGATORIO de honestidad**: indica que el track largo procede de un vehículo predecesor
  (`caveat_global`): dato del gestor / vehículo o mandato distinto. No lo presentes como si fuera el
  mismo fondo legal. Si `_lineage` dice que los documentos del predecesor no son públicos, dilo.

## Schema EXACTO del output (no inventes nombres de campos)

Producir un único fichero JSON: `data/funds/{ISIN}/analyst_synthesis_cowork.json`

```json
{
  "_meta": {
    "isin": "...",
    "generated": "ISO timestamp",
    "generator": "skill:analyst-cowork",
    "skill_version": "2.0.0",
    "main_model": "claude-opus-4-8",
    "audit_model": "claude-sonnet-4-6",
    "sections_generated": [...],
    "audit_pass_done": true,
    "audit_iterations": 1,
    "anti_invencion_flagged": [],
    "input_files_hash": {...},
    "feedback_outcomes": []
  },
  "analyst_synthesis": {
    "resumen": {...},
    "historia": {...},
    "gestores": {...},
    "evolucion": {...},
    "estrategia": {...},
    "cartera": {...},
    "fuentes_externas": {...},
    "documentos": {...}
  }
}
```

### resumen (campos obligatorios — usa EXACTAMENTE estos nombres)

```json
{
  "texto": "1500-2000 chars de narrativa principal. Cita fuentes entre paréntesis. Densa.",
  "filosofia_inversion": "1000-1400 chars. Filosofía detallada del gestor.",
  "criterios_inversion": [
    {"titulo": "...", "descripcion": "..."},
    ...3-5 criterios
  ],
  "fortalezas": ["frase 1", "frase 2", ...4-6 strings cada uno 100-300 chars],
  "riesgos": ["frase 1", "frase 2", ...3-5 strings cada uno 100-300 chars],
  "para_quien_es": "300-500 chars. Perfil del inversor adecuado.",
  "compromiso_gestor": "200-400 chars. Skin in the game, alineación, propiedad.",
  "signal": "POSITIVO | NEUTRAL | NEGATIVO",
  "signal_rationale": "200-300 chars. Por qué esa señal."
}
```

### historia

**ENFOQUE (2026-06-10): la CLAVE es vincular performance del fondo ↔ DECISIONES del gestor ↔ ENTORNO de mercado.** No una cronología genérica, y NO repetir arriba lo que ya está en el timeline (`hitos`) de abajo.

```json
{
  "resumen_general": "2000-3500 chars. PROSA FLUIDA EXTENSA (3-5 párrafos, SIN headers) que resume toda la historia del fondo de un tirón: origen, su trayectoria frente al mercado ligada a las decisiones del gestor, y los hitos/inflexiones clave. 1ª pestaña de Historia (visión de conjunto); el detalle por bloque va en `texto`.",
  "texto": "3500-6000 chars. Detalle en sub-secciones con **bold** headers (el dashboard las convierte en pestañas): (1) ORIGEN del fondo; (2) FONDO VS MERCADO año a año ligado a DECISIONES del gestor y CONTEXTO (patrón: 'en 2022 cayó X% vs Y% del mercado porque el gestor mantuvo/rotó Z'; formatea cada año como línea que empieza con **AÑO — título:**); (3) LECTURA DE CONJUNTO. NO enumerar los hitos de abajo.",
  "hitos": [
    {"anio": "2014", "titulo": "...", "evento": "...", "tipo": "..."},
    ...8-15 hitos
  ]
}
```

Los `hitos` (TIMELINE de abajo) se centran en **PUNTOS DE INFLEXIÓN y hechos relevantes estructurales**, NO en la narrativa de performance de arriba: cambios de equipo gestor (entradas/salidas), caídas fuertes de AUM, reembolsos/salidas de dinero, grandes drawdowns, cambios de propiedad/gestora, cambios de estrategia.
`tipo` opciones: `lanzamiento, cambio_gestor, cambio_owner, cambio_estrategia, caida_aum, reembolsos, drawdown, hecho_relevante, premio, otro`.

### gestores (CRÍTICO — schema exacto, no usar `biografia`)

```json
{
  "texto": "10000-18000 chars. Narrativa profunda del equipo gestor.",
  "perfiles": [
    {
      "nombre": "Juan Gómez Bada",
      "cargo": "CEO y Director de Inversiones",
      "trayectoria": "Texto largo (1500-3000 chars) en MARKDOWN. FOCO en el RECORRIDO PROFESIONAL, no en la estrategia del fondo: (1) **año de incorporación a la gestora actual** y rol con que entró; (2) **empresas y cargos PREVIOS con fechas/periodos** (dónde trabajó antes y haciendo qué); (3) **formación** (titulación, universidad, certificaciones tipo CFA); (4) **años de experiencia** y track record previo (fondos que gestionó antes, resultados/reconocimientos documentados). Ordénalo cronológicamente cuando puedas. La filosofía de inversión va en su campo aparte y la estrategia del fondo en otra sección — aquí cuenta la PERSONA y su carrera.",
      "filosofia": "Texto (500-1000 chars) sobre su filosofía de inversión personal. NO duplicar con resumen.filosofia_inversion (este es del GESTOR, aquel es del FONDO).",
      "cv_bullets": [
        "Formación: Licenciado en X por Universidad Y; CFA charterholder (2010)",
        "Antes: Analista de RV en Gestora Z (2008-2014)",
        "Incorporación a la gestora actual: 2014 como Director de Inversiones",
        "+15 años de experiencia en gestión de activos",
        "Reconocimientos: Rating AA Citywire; 5★ Morningstar",
        ...4-8 bullets — PRIORIZA hitos de carrera/CV (fechas, empresas previas, formación) sobre rasgos de estilo
      ],
      "decisiones_clave": [
        "Decisión 1 con contexto: qué hizo y por qué (200-400 chars)",
        ...3-5 decisiones
      ],
      "rasgos_diferenciales": "300-500 chars. Qué le distingue de otros gestores.",
      "fuente": "manager_profiler"
    },
    ...incluir TODOS los miembros del equipo (lead + co-managers + IR si aplica)
  ]
}
```

**REGLAS GESTORES**:
- `nombre` y `cargo` SON OBLIGATORIOS para todos los perfiles.
- **PRIORIDAD: track-record y CV sobre estilo/estrategia.** Lo que el lector quiere entender de cada gestor es su RECORRIDO: cuándo llegó a la gestora, qué hizo antes (empresas y cargos previos con fechas), formación, años de experiencia y resultados previos documentados. Tanto `trayectoria` como `texto` y `cv_bullets` deben liderar con eso. La filosofía/estilo es secundaria aquí (ya tiene su campo y su sección).
- `trayectoria` es OBLIGATORIO para los 1-2 perfiles lead. Para miembros secundarios puede ser más breve (300-600 chars) pero incluyendo igualmente incorporación + experiencia previa si consta.
- `cv_bullets` es OBLIGATORIO para lead. Opcional para secundarios.
- **ANTI-INVENCIÓN (crítico aquí):** NO inventes empresas previas, fechas, titulaciones ni años no presentes en `manager_profile.json` / `articulos_completos`. Si el recorrido previo no está documentado, di explícitamente "no hay datos públicos sobre su experiencia previa" en vez de rellenar. Un CV corto y verdadero > uno largo inventado. (Recuerda los casos de contaminación cross-fund/homónimos: si un dato de carrera viene de una fuente que no es claramente de ESTE gestor, NO lo uses.)
- `fuente` debe ser uno de: `manager_profiler, manager_deep_agent, google_snippet, sibling_auto, manual_verificado, analyst_llm`.

### evolucion

```json
{
  "texto": "5000-9000 chars. Narrativa de la evolución del fondo: AUM, partícipes, rentabilidad, posiciones, cambios estratégicos en el tiempo.",
  "datos_graficos": {
    "concentracion_historica": [
      {"periodo": "2024", "top5_pct": 23.5, "top10_pct": 38.2, "top15_pct": 49.8},
      ...
    ],
    "drawdown": [{"periodo": "...", "drawdown_pct": ...}],
    "exposicion_geografica": [{"periodo": "...", "espana_pct": ..., "internacional_pct": ...}],
    "num_posiciones_por_anio": [{"anio": "...", "num": ...}],
    "rentabilidades_anuales": [{"anio": "2024", "fondo_pct": ..., "ibex_pct": ..., "sp500_eur_pct": ...}]
  }
}
```

`datos_graficos` debe poblarse con datos REALES extraídos de `fund_data.cuantitativo` y `letters_data.cartas[*]`. Si no tienes el dato, omite la entry. NO inventes números.

### estrategia

**ENFOQUE (2026-06-10): CENTRAR el tiro — pocas sub-secciones, más profundidad en lo diferencial. NADA de historia (va en Historia) ni de posiciones concretas (van en Cartera).** El texto DEBE dejar claro y explícito, en este orden:
1. **Objetivo del fondo** (qué busca lograr y para qué inversor).
2. **Benchmark** (contra quién compara; si no tiene, decirlo).
3. **Universo de inversión**: tipo de activo, geografía/sectores permitidos, y **rangos de inversión permitidos** (% RV/RF, límites por posición, liquidez, etc. del folleto).
4. **Estilo de inversión** (value/growth/quality/quant/macro...).
5. **Fondo AUTOR vs GESTIONADO** (¿gestión personalista de un autor con skin-in-the-game, o producto gestionado por equipo/casa?).
6. **Cuantitativo vs cualitativo** + la **"fórmula"/forma concreta de analizar y seleccionar** (proceso, filtros, métricas, cómo deciden comprar/vender) — lo más diferencial.

```json
{
  "resumen_general": "2500-4000 chars. PROSA FLUIDA EXTENSA (4-6 párrafos, SIN headers ni subsecciones) que RESUME de forma sustancial TODO lo que se detalla en las demás pestañas/subsecciones (objetivo, benchmark, universo+rangos, estilo, autor vs gestionado, cuanti/cuali y la fórmula). Es la 1ª pestaña de Estrategia: una visión de conjunto sólida y completa que se pueda leer sola y entender la estrategia entera; el detalle por bloque va en `texto`. Debe ser CONSIDERABLEMENTE extensa, no un resumen corto.",
  "texto": "5000-9000 chars. Estrategia centrada en los 6 puntos de arriba (objetivo, benchmark, universo+rangos, estilo, autor/gestionado, cuanti/cuali+fórmula), profundizando en lo diferencial. Cada uno como sub-sección con **bold** header (el dashboard las convierte en pestañas). Sin historia ni enumerar posiciones.",
  "estrategia_actual_resumen": "200-400 chars. Resumen de la estrategia hoy.",
  "diferenciacion": {
    "activos":          {"texto": "400-900 chars: qué activos y cómo cambia el mix", "cambia": "si|no|no_puede", "por_que": "1-2 frases", "evidencia": ["Informe anual 2025", "Carta 2024-Q4"]},
    "gestion":          {"texto": "400-900 chars: autor/equipo/casa, toma de decisiones, órgano de control, skin-in-the-game", "cambia": "si|no|no_puede", "por_que": "...", "evidencia": ["..."]},
    "geografia":        {"texto": "400-900 chars: mix geográfico y cómo cambia", "cambia": "si|no|no_puede", "por_que": "...", "evidencia": ["..."]},
    "filosofia_equipo": {"texto": "400-900 chars: filosofía/estrategia + expertise real del equipo", "cambia": "si|no|no_puede", "por_que": "...", "evidencia": ["..."]}
  },
  "fortalezas": [
    "frase 1 (100-300 chars)",
    "frase 2",
    ...4-6 strings — DUPLICAN los de resumen.fortalezas (el dashboard renderiza ambos sitios)
  ],
  "riesgos": [
    "frase 1 (100-300 chars)",
    ...3-5 strings — DUPLICAN los de resumen.riesgos
  ],
  "perfil_riesgo": {
    "tipo_activo_principal": "Mixto Flexible Global / Renta Variable Global / Renta Fija / etc",
    "riesgos_especificos": ["riesgo 1", "riesgo 2", "riesgo 3"],
    "desglose_exposicion": [
      {"dimension": "geografía", "detalle": "60% internacional, 20% España, 16% Argentina, 4% otros"},
      {"dimension": "sectores", "detalle": "..."}
    ]
  },
  "hitos_estrategia": [
    {
      "periodo": "2025-S2",
      "contexto_mercado": "300-500 chars. Qué pasaba en el mercado.",
      "decisiones": "300-600 chars. Qué decidió el gestor.",
      "resultado": "+X.XX% — driver explicativo (200-400 chars)"
    },
    ...4-8 hitos cubriendo años con cartas K15
  ],
  "quotes": [
    {
      "texto": "cita literal entre 30-200 chars",
      "autor": "Juan Gómez Bada",
      "contexto": "Carta Semestral julio 2025"
    },
    ...3-6 quotes
  ]
}
```

**REGLAS estrategia** (v2.2):
- Los `quotes` deben ser CITAS LITERALES extraídas de `letters_data.cartas[*].texto_completo`. Si no encuentras citas reales, devuelve lista vacía (no inventes).
- `fortalezas` y `riesgos` se DUPLICAN aquí en estrategia (también en resumen). El v1 dashboard renderiza ambos sitios. NO es redundancia inocua — el quality_loop chequea los dos.
- `hitos_estrategia[].resultado` debe seguir el formato `"+X.XX% — driver"` (cifra + por qué). Sin driver explicativo, el quality_loop lo flaggea.
- `perfil_riesgo` es OBLIGATORIO con los 3 sub-campos. Sin esto, el quality_loop reporta "Perfil de riesgo de la estrategia incompleto".

### cartera

```json
{
  "texto": "5000-8000 chars. ENFOQUE (2026-06-10): la CARTERA, no la estrategia. Cuatro bloques: (1) CÓMO SE CONSTRUYE (proceso: nº de posiciones objetivo, criterios de entrada/peso, concentración buscada, gestión de liquidez/riesgo); (2) CÓMO HAN EVOLUCIONADO LOS PESOS y POR QUÉ — CUALITATIVO y concreto: rotaciones y cambios de sesgo (geográfico/sectorial/tipo-activo) a lo largo del tiempo ligados al MERCADO y a las CONVICCIONES del gestor (qué aumentó/redujo y por qué); (3) POSICIONES DE ALTA CONVICCIÓN — lo más importante: los valores/temas que el gestor lleva AÑOS en cartera o con peso destacado (núcleo de convicción), por qué los mantiene y qué representan; (4) COMPOSICIÓN ACTUAL y RACIONAL (tipo de activo, geografía, sector, concentración top-N como %). NO repetir filosofía/estilo (eso es Estrategia). NO enumerar todas las posiciones con su peso — la tabla ya las muestra; nombra valores para ilustrar convicción/racional, sin listar pesos uno a uno.",
  "top_posiciones": [
    {"nombre": "...", "peso_pct": 6.05, "categoria": "Real estate / Tech / Banca / etc"},
    ...10 entries — DUPLICAN top10 de posiciones.actuales[] del top-level del schema
  ],
  "concentracion": {
    "top5_pct": 23.5,
    "top10_pct": 38.2,
    "top15_pct": 49.8
  },
  "concentracion_historica": [
    {"periodo": "2024", "top5_pct": ..., "top10_pct": ..., "top15_pct": ..., "fuente": "fund_data"},
    ...8-15 entries
  ],
  "distribucion_tipo": {
    "rv_internacional_pct": 60.0,
    "rv_españa_pct": 20.3,
    "rf_pct": 18.7,
    "liquidez_pct": 1.0
  }
}
```

**REGLAS cartera** (v2.2):
- `top_posiciones` es OBLIGATORIO con 10 entries duplicando los top10 de `posiciones.actuales` (datos top-level). El quality_loop chequea esto. NO es redundancia con el dashboard — es input para el quality_check, no para visualización (el dashboard sigue leyendo de `posiciones.actuales`).

**REGLAS cartera.texto** (críticas — feedback v2.1):
- **NO ENUMERAR posiciones individuales con sus pesos en el texto**. La tabla de `posiciones.actuales` (top-level del schema, top-level del dashboard) ya las muestra. Repetirlo es redundancia visual molesta para el lector.
- El texto debe ser **un párrafo resumen sobre composición**: tipo de activo (RV vs RF vs liquidez), distribución geográfica (zonas y tesis por zona), distribución sectorial general, concentración relativa (top-N como % del patrimonio sin nombrar valores), racional global de la cartera y riesgos estructurales actuales.
- Cuando menciones una posición concreta porque ilustra un punto del racional (p.ej. "exposición a Argentina vía empresas como X e Y"), hazlo con moderación y SIN poner el peso. El peso lo da la tabla.
- Cifras de `concentracion` y `distribucion_tipo` SOLO de `fund_data.posiciones.actuales` y `fund_data.cuantitativo.mix_activos_historico`. NO inventes.

### fuentes_externas (CRÍTICO — usa `opiniones_clave`, no `lecturas_destacadas`)

```json
{
  "texto": "5000-9000 chars. Síntesis cualitativa de qué dicen las fuentes externas sobre el fondo.",
  "opiniones_clave": [
    {
      "fuente": "Substack Salud Financiera",
      "titulo": "Análisis de Avantage Fund - Salud Financiera",
      "url": "https://saludfinanciera.substack.com/p/analisis-avantage-fund",
      "tipo": "analisis | review | interview | rating | comunidad | podcast | video",
      "opinion": "200-400 chars. Síntesis de lo que dice esta fuente.",
      "fecha": "2025"
    },
    ...8-15 opiniones
  ]
}
```

**REGLAS fuentes_externas**:
- Lee `readings.readings[]` del bundle.
- Filtra ruido (videos de música irrelevantes, posts genéricos sin contenido).
- Para cada reading legítimo, escribe su `opinion` sintetizada (no copies, sintetiza).
- `tipo` debe ser uno de: `analisis, review, interview, rating, comunidad, podcast, video, articulo`.

### documentos (CRÍTICO — schema URL-based, no descriptivo)

```json
{
  "informes_pdf": [
    {"archivo": "CNMV_ES0112231008_2024_H2.pdf"},
    ...todos los pdfs en fund_data.fuentes.informes_descargados
  ],
  "xmls_cnmv": [
    {"archivo": "Abril_FONDMENS_202504.xml"},
    ...todos los xmls en fund_data.fuentes.xmls_cnmv
  ],
  "cartas_urls": [
    "https://www.avantagecapital.com/carta-semestral-a-los-inversores-enero-2025/",
    ...todas las URL únicas en letters_data.cartas[].url_fuente
  ],
  "fuentes_externas_urls": [
    "https://saludfinanciera.substack.com/p/analisis-avantage-fund",
    ...todas las URL únicas en readings.readings[].url
  ],
  "urls_consultadas": [
    "https://www.cnmv.es/portal/Consultas/IIC/Fondo.aspx?isin=ES0112231008",
    ...URLs de fund_data.fuentes.urls_consultadas (CNMV portal, regulator portals)
  ],
  "total_fuentes": 127
}
```

**REGLAS documentos**:
- `documentos` es 100% AGREGACIÓN MECÁNICA, no narrativa. NO incluyas `texto` ni descripciones.
- `informes_pdf` y `xmls_cnmv`: extrae directo de `fund_data.fuentes.{informes_descargados, xmls_cnmv}` (cada item es `{archivo: "..."}`).
- `cartas_urls`: deduplica `letters_data.cartas[].url_fuente`, ordena.
- `fuentes_externas_urls`: deduplica `readings.readings[].url`, ordena. Filtra URLs claramente ruidosas (videos no relacionados, etc.).
- `urls_consultadas`: típicamente 1-2 URLs del portal del regulador.
- `total_fuentes` = suma de longitudes de las 5 listas anteriores.

## Workflow paso a paso

### 1. Validación de pre-requisitos (1 turn)

Bash:
```
ISIN={ISIN}
cd /ruta/a/fund-analyzer
ls -la data/funds/$ISIN/bundle/
```

Si falta el bundle o cualquiera de los 5 inputs → aborta y pide ejecutar prep. Si OK → continúa.

### 2. Lectura del schema (1 turn)

Lee `docs/cowork_handoff/CLAUDE.md` secc