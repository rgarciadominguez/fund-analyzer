---
name: letters-sourcing-cowork
description: Encuentra en la web de la gestora (y en las plataformas donde la gestora publica) TODAS las cartas y comentarios del gestor de un fondo concreto, de todos los años desde su lanzamiento, las verifica, las registra en la base de conocimiento y las deja recolectadas en letters_data.json. Úsala cuando Rafa diga "letters sourcing cowork ISIN", "busca las cartas de X", "cartas históricas de X", "comentarios del gestor de X". NO extrae el contenido (eso lo hace letters-extract-cowork).
---

# letters-sourcing-cowork v1 (25-sep-2026)

## Para qué

Rafa (asesor financiero) necesita, para cada fondo, **la voz del gestor a lo largo del tiempo**: qué decía, qué
decidía y por qué, año a año. Es la base del análisis de consistencia (¿hace lo que dice?) y de los ejes de
diferenciación. Hasta ahora el pipeline se quedaba con 0-2 cartas por fondo aunque la gestora publicara
trimestrales desde el lanzamiento, porque el colector automático no navega la web de la gestora. Tú sí.
Regla de Rafa: **cuanta más información del gestor, mejor, siempre que sea de ESTE fondo** (lo que se comparte
entre clases del mismo fondo vale; lo de otro fondo de la misma gestora, no).

## Entrada

- ISIN. Identidad en `data/funds/{ISIN}/output.json` (o `cnmv_data.json` / `intl_data.json` / `cssf_data.json`):
  `nombre`, `gestora`, clases hermanas, paraguas, y el año de lanzamiento real (`python -m tools.fund_age {ISIN}`;
  si hay predecesor, `data/fund_lineage.json`).
- Lo que YA tenemos: `data/funds/{ISIN}/letters_data.json` (`cartas[].periodo`, `url`, `archivo`),
  `data/known_manager_letters.json` (entrada del fondo, si existe) y `python -m tools.doc_completeness {ISIN}`
  (`faltan_carta`). No re-busques periodos que ya están registrados con URL.

## Cómo encontrar las cartas (estrategia, no receta)

Piensa como alguien que quiere leer todo lo que el gestor ha escrito sobre este fondo:

1. **La web oficial de la gestora es la fuente principal.** Localízala (`tools/fund_site_finder`, o búsqueda
   "<gestora> <fondo>"; el dominio suele estar en `sources.json` o `data/regulators_knowledge.json`). Recorre
   la ficha del fondo (pestañas Documentos / Informes / Comentarios / Cartas / Literature / Insights), la
   sección de publicaciones, noticias, blog, newsletters y los archivos por año. Sigue la paginación y los
   filtros por fondo. Mira también `sitemap.xml` y `wp-content/uploads` cuando la web sea WordPress: los
   nombres de fichero suelen seguir un patrón (`carta-trimestral-<Fondo>-Q1-2026.pdf`), y **un patrón
   permite deducir los periodos anteriores**: pruébalos.
2. **Cartas que ya no están enlazadas**: si la web solo muestra la última, busca las anteriores por el patrón de
   URL y, si ya no existen, en Wayback (CDX: `http://web.archive.org/cdx/search/cdx?url=<dominio>/*carta*&output=json`;
   la URL descargable es `https://web.archive.org/web/<ts>id_/<original>`).
3. **Plataformas donde la gestora publica**: Finect (documentos del fondo), Rankia, Morningstar (doc library),
   fundinfo/fundsquare, la web del distribuidor o del paraguas (SICAV), LinkedIn/Substack/Medium del gestor,
   podcasts con transcripción. Solo valen si el texto es del gestor sobre este fondo.
4. **Tipos que cuentan como carta**: carta trimestral/mensual/anual a partícipes, comentario del gestor en la
   ficha o factsheet (si tiene 2+ párrafos), "letter from the fund managers", informe mensual con comentario,
   transcripción de webinar/podcast del gestor sobre el fondo, entrevista extensa. **No cuentan**: folleto, KID,
   informe CNMV/anual completo (ya los tenemos por otra vía), house-view de la casa que no menciona el fondo,
   comentarios de mercado genéricos (NEPC, BlackRock...), notas de prensa.
5. **Busca en el idioma de la gestora** (español, inglés, francés, alemán...) y con el nombre nativo del fondo
   y su abreviatura.

## Verificación de identidad (obligatoria)

Antes de registrar cada documento, ábrelo (WebFetch o descarga) y comprueba que **habla de este fondo**: nombre
del fondo o del compartimento en el título o el cuerpo, gestor coincidente, o ISIN. Una "carta estrategia" de la
casa vale SOLO si dedica una parte identificable a este fondo (regístrala con `tipo: "carta_casa"` y dilo en
`nota`). Si el documento es de un fondo hermano homónimo (mismo nombre en otra divisa o paraguas distinto),
descártalo. Anota los descartes con el motivo en tu respuesta final: es información útil.

## Salida

1. **`data/known_manager_letters.json`** → `funds["<nombre del fondo>"]` (créala si no existe):
   ```json
   {"isins": ["ES0140794001", "ES0140794019"], "gestora": "Singular Asset Management",
    "letters_page": "https://www.singularam.es/publicaciones-y-noticias/",
    "pattern": "https://www.singularam.es/wp-content/uploads/carta-trimestral-Gamma-Q{Q}-{YYYY}.pdf",
    "publica": "trimestral desde 2021-Q3; mensual 'Carta Estrategia' es de la casa (no del fondo)",
    "letters": [
      {"periodo": "2026-Q1", "tipo": "carta_trimestral", "url": "https://.../carta-trimestral-Gamma-Q1-2026.pdf",
       "titulo": "Carta trimestral Gamma Global Q1 2026", "verificado": "nombre del fondo en portada"}
    ]}
   ```
   `periodo`: `YYYY`, `YYYY-Q1..4`, `YYYY-S1/S2` o `YYYY-MM` (mensual). `tipo`: carta_trimestral | comentario_mensual |
   carta_anual | carta_casa | transcripcion | entrevista | factsheet_comentario. Incluye TODOS los ISIN de las clases
   del fondo en `isins` (las cartas se comparten entre clases). No borres entradas existentes: añade.
   Si la gestora NO publica cartas, deja la entrada con `letters: []` y `publica: "no publica cartas (comprobado
   <fecha>: solo factsheet/folleto)"` para que nadie vuelva a buscar en vano.
2. **Descarga + recolección** (para que el análisis de ESTE run las use):
   ```
   python -m tools.ensure_kb_letters {ISIN}     # descarga y registra como documentos (quarterly_letter)
   python -m tools.letters_recollect {ISIN}     # vuelve a construir letters_data.json con lo nuevo (no pierde nada)
   ```
   Comprueba al final cuántas cartas tiene `letters_data.json` y de qué periodos.
3. **Respuesta final**: cuántas cartas por año has conseguido y de dónde, qué periodos siguen sin carta y por qué
   (no publicadas / desaparecidas / no verificables), y qué documentos descartaste y por qué. Sin inventar URLs:
   cada una la has abierto.

## Límites

- Trabajas sin nadie mirando: no preguntes, decide y anota los supuestos.
- No modifiques `letters_data.json` a mano: pasa siempre por `ensure_kb_letters` + `letters_recollect`.
- Tiempo razonable: si tras explorar la web de la gestora, su patrón de URLs, Wayback y dos plataformas no
  aparece nada de un periodo, dalo por no publicado y sigue.
