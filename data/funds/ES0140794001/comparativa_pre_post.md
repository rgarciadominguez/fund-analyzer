# Comparativa PRE vs POST — ES0140794001 (GAMMA GLOBAL, FI)

Snapshot PRE: 2026-09-24T16:33:15.702487 · POST: 2026-09-25T13:26:55.142995

## KPIs de cabecera

| KPI | PRE | POST | fuente POST |
|---|---|---|---|
| anio_creacion | 2021 | 2021 |  |
| benchmark | None | None |  |
| rating_morningstar | None | 5 ← | Supabase estrellas |
| aum_actual_meur | 293.717 | 488.416 ← |  |
| num_participes | 112 | 11942 ← |  |
| num_activos_cartera | None | 79 ← | cartera (posiciones actuales) |
| concentracion_top10_pct | None | 34.95 ← | cartera (suma top 10) |
| ter_pct | 0.86 | 0.86 |  |
| coste_gestion_pct | 0.38 | 0.75 ← |  |
| divisa | EUR | EUR |  |

## Series cuantitativas (nº de puntos)

| serie | PRE | POST |
|---|---|---|
| serie_aum | 5 | 6 |
| serie_participes | 2 | 6 |
| serie_ter | 9 | 9 |
| serie_rentabilidad | 1 | 0 |
| mix_activos_historico | 5 | 6 |
| mix_geografico_historico | 0 | 0 |
| serie_vl_base100 | 2 | 2 |

## Cartera

PRE: {'n': 79, 'con_peso': 79, 'con_sector': 79, 'con_pais': 79}  →  POST: {'n': 79, 'con_peso': 79, 'con_sector': 4, 'con_pais': 79}

## Documentos extraídos (por tipo)

PRE: {'aportado_v5': 5, 'aportado_v5_Analisis-de-Gamma-Global.pdf': 1, 'aportado_v5_Gamma-Global-octubre-2025.pd': 1, 'cnmv_cualitativo_seccion9.json': 1, 'reports_qualitativo_CNMV_ES0140794001': 6, 'web_qualitativo_Annual-Accounts-Quadriga': 1, 'web_qualitativo_CCAA-2023-Quadriga-Asset': 1, 'web_qualitativo_incometric-prospectus.js': 1, 'web_qualitativo_Quadriga-Asset-Managers-': 1, 'web_qualitativo_Quadriga-Investors-Prosp': 1}
POST: {'cnmv_cualitativo_seccion9.json': 1, 'reports_qualitativo_CNMV_ES0140794001': 6}

## Cartas

PRE: {'n': 4, 'periodos': ['2026', '2025-Q4', '2024-Q2', '2023-Q4']}
POST: {'n': 2, 'periodos': ['2026-Q2', '2025']}

## Gestores

PRE: ['Gabriel Castro Lafuente', 'José Ramón Boluda Vicedo']
POST: ['Gabriel Castro Lafuente', 'José Ramón Boluda Vicedo']

## Síntesis (longitud / nº de cifras por sección)

| sección | PRE | POST |
|---|---|---|
| _quality_check | 2237 chars / 18 cifras | 0 chars / 0 cifras |
| cartera | 6777 chars / 26 cifras | 8222 chars / 148 cifras |
| documentos | 7507 chars / 40 cifras | 4694 chars / 3 cifras |
| estrategia | 15045 chars / 59 cifras | 29235 chars / 326 cifras |
| evolucion | 6211 chars / 51 cifras | 8361 chars / 205 cifras |
| fuentes_externas | 9154 chars / 26 cifras | 12449 chars / 112 cifras |
| gestores | 16655 chars / 32 cifras | 18485 chars / 121 cifras |
| glosario | 3154 chars / 5 cifras | 5383 chars / 40 cifras |
| historia | 8305 chars / 52 cifras | 12901 chars / 150 cifras |
| resumen | 9327 chars / 38 cifras | 10635 chars / 105 cifras |

Ejes de diferenciación (chars): PRE {'activos': 0, 'gestion': 0, 'geografia': 0, 'filosofia_equipo': 0} → POST {'activos': 884, 'gestion': 898, 'geografia': 920, 'filosofia_equipo': 927}

## Control de calidad

PRE: score None, 16 fallos: ['estrategia_has_cifras', 'cuant_min_serie_vl', 'cuant_min_serie_participes', 'historia_no_headers', 'estrategia_no_headers', 'resumen_kpis_match_data', 'historia_kpis_match_data', 'aum_jump_alert', 'cuant_serie_clases_info_presente', 'historia_kpis_calculables', 'perfil_riesgo_present', 'desglose_exposicion_present', 'diferenciacion_activos', 'diferenciacion_gestion', 'diferenciacion_geografia', 'diferenciacion_filosofia_equipo']
POST: score None, 13 fallos: ['cuant_min_serie_vl', 'estrategia_no_headers', 'resumen_kpis_match_data', 'historia_kpis_match_data', 'aum_jump_alert', 'cartera_posiciones_sectores', 'comision_exito_teorica_presente', 'cuant_serie_clases_info_presente', 'hitos_historia_percentages_verifiable', 'historia_kpis_calculables', 'evolucion_drawdown_con_fecha', 'perfil_riesgo_present', 'desglose_exposicion_present']

## Novedades / gráficos / clases

PRE: {'modo': 'aporte', 'veredicto': 'cambia', 'huecos': 3, 'hallazgos': 4} · gráficos 2 · clases 10
POST: {'modo': None, 'veredicto': None, 'huecos': 0, 'hallazgos': 0} · gráficos 0 · clases 0


---

# Lectura crítica (Claude, 25-sep-2026 14:35)

Run completo desde cero en el servidor: 13:26 → 14:16 (50 min), todos los pasos OK, publicado y verificado
(local = Storage = Worker). Síntesis con Fable 5.1 a esfuerzo `high`: 16 min (Opus tardó 17 en el aporte del 24-sep).

## Qué ha mejorado (con evidencia)

1. **KPIs de cabecera correctos y con origen.** Partícipes 112 (dato de 2021) → 11.942 (CNMV 1S-2026); patrimonio
   293,7 → 488,4 M€; comisión de gestión 0,38 → 0,75 % (clase A, CNMV 2S-2025); rating 5 estrellas; 79 activos;
   top-10 = 35 %. Todo viene del parser CNMV v8.2 y de la conciliación, no del texto.
2. **Series**: partícipes 2 → 6 puntos; patrimonio y mix de activos 5 → 6.
3. **Síntesis mucho más anclada en cifras**: estrategia 59 → 326 cifras, historia 52 → 150, evolución 51 → 205,
   gestores 32 → 121. No es relleno: son cifras por informe y año.
4. **Ejes de diferenciación** (activos, gestión, geografía, filosofía y equipo): antes vacíos, ahora ~900
   caracteres cada uno con evidencia (mix bolsa 27 % → 6 %, dólar 38 % → 63 % cubierto, equipo sin cambios, misma
   filosofía que Boluda en la EAF anterior).
5. **Sin contaminación en el descubrimiento**: los 5 documentos de Quadriga e Incometric que había en el análisis
   anterior ya no entran; el único ajeno (NEPC) lo marcó el extractor de cartas y Fable lo descartó y lo anotó.
6. **Control de calidad**: 16 → 13 fallos, y los 4 de diferenciación desaparecen.

## Qué ha empeorado o sigue mal (y por qué)

A. **Se han perdido los documentos que aportaste (grave).** Los 7 PDF del aporte del 24-sep (informes anuales
   2021-2025, "Análisis de Gamma Global", presentación oct-2025) no están en el análisis nuevo: ni sus extractos, ni
   los gráficos incrustados (2 → 0) ni la tabla de clases (10 → 0). Causa: el arranque en frío del servidor web mueve
   la carpeta entera a `.bak` y solo conserva `raw/aportados` en una de sus dos ramas de código; en la rama normal no.
   Arreglo: conservar `raw/aportados` + `aportados.json` también en esa rama (requiere reiniciar el servidor web).
B. **Sectores de la cartera: 79 → 4 con sector.** Consecuencia directa de A: en un fondo de renta fija los sectores
   venían de la presentación aportada, no de Yahoo. Se recupera al arreglar A.
C. **MyInvestor no funciona en el servidor.** La skill dice que el conector de MyInvestor no está cargado en esa
   sesión (`claude -p`), así que la regla "comisión y TER de la clase accesible al particular según MyInvestor" no
   puede aplicarse allí. Hoy la comisión sale de CNMV (correcta), pero la referencia MyInvestor está muerta en el
   pipeline. Hay que dar acceso al conector en el servidor o buscar otra vía de datos.
D. **Cartas: 2, y ninguna útil.** Una (2026-Q2) es un extracto de 4 líneas sin fuente; la otra (2025) es el
   comentario de NEPC, ajeno (bien marcado). Singular AM publica comentarios mensuales y el colector no los
   encuentra. Es el mismo hueco del filtro de identidad y del sourcing por dominio de la gestora, aparcado por decisión
   de Rafa; en INT pesará más que aquí.
E. **Falso positivo del control de calidad**: `resumen_kpis_match_data` marca "112 partícipes" como incoherente,
   pero el texto dice "de 4,5 M€ y 112 partícipes a cierre de 2021 a 488,4 M€ y 11.942 a 30/06/2026", que es correcto.
   La regla debe ignorar cifras históricas cuando la actual también aparece.
F. **Serie de rentabilidad anual**: 1 punto (2022, calculado desde VL) → 0. Menor: lo cuantitativo sale de las series
   de Morningstar, pero el cálculo desde VL base 100 no se ha ejecutado esta vez. Revisar.
G. **Avisos engañosos del pipeline**: `meta_report.issues` dice "gestores vacíos" y "sin estrategia" cuando ambas
   secciones están llenas (lo calcula antes de la síntesis); el consumo de manager-deep avisa "skill no ejecutada"
   aunque el perfil de gestores se generó (52 KB); el aviso de "drift" dice que faltan ficheros del bundle que sí
   existen. Ninguno rompe nada, pero confunden a quien revisa.
H. **Config automática**: `clase_accion` por defecto "I EUR" no existe en este fondo (solo A y Z). Fable lo detectó y
   analizó la clase A del ISIN, pero el valor por defecto debería ser la clase del ISIN analizado.
I. `estrategia_no_headers`: 6 sub-encabezados frente a un máximo de 5. Legítimo y menor.

## Veredicto

El sistema nuevo produce un análisis claramente mejor en datos, cifras y diferenciación, y sin contaminación de
otras gestoras. Pero un análisis desde cero hoy **pierde el material aportado por Rafa** (A), que era la fuente de
gráficos, tabla de clases y sectores. Antes de repetir con más fondos: arreglar A (y reiniciar el servidor web),
decidir C (MyInvestor en el servidor) y corregir E. D queda como hueco conocido, mayor en INT.
