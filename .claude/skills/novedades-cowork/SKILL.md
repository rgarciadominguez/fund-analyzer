---
name: novedades-cowork
description: Busca TODO lo nuevo publicado sobre un fondo desde su último análisis (annual y semiannual report, cartas del gestor, análisis externos, entrevistas, noticias, vídeos o podcasts), verifica que es de ese fondo, lo guarda y deja un manifiesto para que Rafa lo revise y para alimentar el update anual. Úsala cuando el prompt sea "novedades cowork {ISIN}" (lo lanza tools.novedades en el servidor) o cuando Rafa pida "busca las novedades de X", "qué ha salido nuevo de X".
---

# novedades-cowork v1 (30-sep-2026)

## Para qué

Rafa hace seguimiento de sus fondos buenos y top. Cuando llega la fecha en la que seguro ya ha salido algo
nuevo (una carta trimestral o el annual report del año), quiere que el sistema **ya lo haya encontrado y
guardado**: él lo revisa a mano y decide si actualiza el análisis con esa información. El update anual parte
de este trabajo: no vuelve a buscar lo que tú ya has encontrado.

Busca todo lo que ayude a **entender la situación actual del fondo**: qué ha pasado, qué ha hecho el gestor y
por qué, cómo le ha ido, qué opinan terceros.

## Entrada

- `data/funds/{ISIN}/novedades/_encargo.json`: `{isin, nombre, gestora, desde, motivo}`. `desde` es la fecha
  del último análisis o del último documento conocido; `motivo` es `update_anual` (busca todo) o
  `carta_{periodo}` (céntrate en la carta, pero recoge también lo demás nuevo que veas).
- Lo que ya tiene el sistema, para no repetir: `data/funds/{ISIN}/letters_data.json`, `raw/`, `extracted/`,
  `data/known_manager_letters.json` (página de cartas y patrón de URL de la gestora),
  `data/known_annual_reports.json`, y `output.json` (`fuentes`).

## Lecciones aprendidas (antes de empezar)

Ejecuta `python -m tools.aprendizaje lecciones {ISIN} --etapa fuentes` y `… --etapa cartas` y aplícalas.

## Qué buscar (desde `desde` hasta hoy)

1. **Documentos oficiales**: annual report y semiannual report (en paraguas, el del paraguas: basta con que
   contenga el sub-fondo), informes CNMV semestrales en fondos españoles, factsheet más reciente, cambios de
   folleto o KID.
2. **Voz del gestor**: cartas trimestrales o semestrales, comentarios mensuales, presentaciones a partícipes,
   vídeos o podcasts del gestor sobre el fondo.
3. **Terceros**: análisis externos (Morningstar, Citywire, Finect, Rankia, blogs de analistas serios),
   entrevistas al gestor, noticias relevantes (cambios de equipo, de gestora, fusiones, cierres a nuevos
   partícipes, entradas o salidas de patrimonio importantes).

Empieza por la web de la gestora (página de cartas y documentos de la KB), sigue con búsquedas web por
nombre del fondo, nombre del gestor e ISIN, y usa Wayback solo si la web no deja descargar.

## Verificación (obligatoria)

Cada documento debe ser de **este fondo**: el nombre del fondo o el ISIN aparece en el texto (o, en un
paraguas, el sub-fondo aparece en el índice). Descarta sin dudar lo que sea de otro fondo, lo corporativo de
la gestora (informes firm-wide, resultados de la matriz, house views) y lo anterior a `desde`.

## Guardar

- Descarga cada PDF a `data/funds/{ISIN}/novedades/` con un nombre claro (`2026-Q3_carta.pdf`,
  `2026_annual_report.pdf`). Para artículos web o vídeos, no descargues: guarda la URL.
- Si encuentras cartas nuevas, añádelas también a `data/known_manager_letters.json` (mismo formato que ya
  tiene) para que el sistema las conozca.
- Escribe `data/funds/{ISIN}/novedades/novedades.json`:

```json
{"isin": "…", "desde": "YYYY-MM-DD", "buscado": "YYYY-MM-DDTHH:MM", "motivo": "…",
 "docs": [{"tipo": "annual_report | semiannual_report | carta | factsheet | folleto | analisis_externo | entrevista | noticia | video | podcast",
           "titulo": "…", "fecha": "YYYY-MM-DD", "url": "https://…", "archivo": "novedades/…pdf o null",
           "resumen": "1-2 frases: qué aporta para entender el fondo ahora", "verificado": true}],
 "no_encontrado": ["lo que se esperaba y no ha salido todavía, p.ej. 'carta 2026-Q3'"],
 "resumen_rafa": "2-3 frases en llano: qué hay de nuevo y si merece actualizar el análisis"}
```

Nunca termines con una pregunta.
