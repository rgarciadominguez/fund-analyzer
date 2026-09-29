---
name: feedback-sistema-cowork
description: Convierte el feedback de Rafa sobre el análisis de un fondo en una MEJORA DEL SISTEMA de análisis (skills, reglas, código), arreglando la causa de forma general y, si hace falta, relanzando el análisis del fondo. Úsala cuando el prompt sea "feedback sistema cowork {id}" (lo lanza tools.feedback_sistema en el servidor) o cuando Rafa pida procesar un feedback pendiente.
---

# feedback-sistema-cowork v1 (29-sep-2026)

## Para qué

Rafa es asesor financiero. Revisa los análisis de fondos que publica este sistema y, cuando algo no le convence,
deja un feedback. **El objetivo no es parchear ese análisis: es que el sistema mejore** para que ese tipo de fallo
no vuelva a pasar en ningún fondo. Trabajas como lo haría un buen ingeniero del equipo al recibir una queja de
un usuario exigente: entiendes qué quiere decir, lo compruebas en los datos, encuentras por qué el sistema lo
hizo así y lo arreglas en su origen.

Regla de Rafa, literal: "no quiero que cuando te dé feedback fuerces cosas; quiero que revises el problema, lo
entiendas y lo arregles para que no vuelva a pasar".

## Entrada

- `data/feedback_sistema/pendientes/{id}.json`: `{id, isin, nombre, texto, origen, creado}`.
- El fondo: `data/funds/{ISIN}/` (output.json, analyst_synthesis_cowork.json, bundle/, extracted/,
  letters_data.json, quality_gate.json con las dudas de la auditoría) y su dashboard `dashboard/fund-{ISIN}.html`.
- El sistema: `analizar_fondo.bat` (orden de pasos), `.claude/skills/*/SKILL.md` (analyst-cowork,
  extract-pdfs-cowork, letters-*, manager-deep-cowork…), `agents/`, `tools/`, `dashboard/generate_dashboard.py`,
  `data/quality_rules.json`, y el histórico `data/feedback_sistema/registro.jsonl` (feedback anterior y qué se
  cambió: úsalo para no repetir ni contradecir decisiones previas).
- Contexto del proyecto: `CLAUDE.md` del repo.

## Cómo trabajar

1. **Entiende el feedback.** Sepáralo en puntos. Para cada uno, di con tus palabras qué espera Rafa ver.
2. **Compruébalo en los datos del fondo.** ¿Es cierto? ¿Dónde está el dato bueno (documentos, CNMV,
   Morningstar, cartas)? Si el feedback no se sostiene con los datos, no cambies nada de ese punto y explícalo.
3. **Encuentra la causa en el sistema.** Sigue el dato desde la fuente hasta el dashboard: ¿falta en la
   extracción, se pierde en un merge, la skill del analista no lo pide, el dashboard no lo pinta, una regla lo
   filtra? Nombra el fichero y la lógica responsable.
4. **Arréglalo de forma general**, en el sitio de la causa, para todos los fondos a los que aplique. Nunca un
   `if isin == ...`, nunca editar a mano el output del fondo para que "salga bien", nunca bajar el listón de
   una comprobación para que pase.
   - Si la causa está en lo que el analista escribe, el arreglo va en la skill (`analyst-cowork`), explicando
     el porqué, no como una regla de longitud o un tope (Rafa no quiere topes: quiere síntesis ejecutiva).
   - Si está en código, cambia el código y añade o ajusta un test en `tests/` cuando sea razonable.
5. **Verifica.** Compila lo tocado (`python -m py_compile`), pasa los tests relacionados
   (`python -m pytest -q tests/test_<área>*.py`) y, si afecta al dashboard, regenera el del fondo
   (`python dashboard/generate_dashboard.py {ISIN}`) y comprueba en el HTML que ahora sale bien.
6. **Aplica al fondo.** Si el arreglo necesita rehacer el análisis del fondo, encola la re-sección con el
   mecanismo existente: guarda el punto con `tools.feedback_store.append_feedback` y lanza
   `POST http://127.0.0.1:5000/api/analyze-batch` con `{"isins": ["{ISIN}"], "cold_start": false,
   "apply_feedback": true}` (entra en la cola; no lances `analizar_fondo.bat` a mano).
   Si basta con regenerar el dashboard, regénéralo y publícalo con `python -m tools.publish_dashboard --isin {ISIN}`.
7. **Sube el cambio del sistema a git** (rama actual, `v2-cowork`): `git add` SOLO los ficheros que has tocado,
   commit en español que diga qué feedback lo motivó y qué cambia, y `git push`. Nunca `--force` ni `reset --hard`.

## Límites (obligatorios)

- **Nunca edites un `.bat` si hay un análisis en marcha** (mira `data/queue_state.json`: ningún item en
  running/queued/paused_waiting_tokens). cmd lee el .bat por posición y lo rompería.
- Nunca rutas absolutas de usuario en el código. Nunca imprimas secretos (.env).
- `clasificacion_user`, `opinion_user`, `encaje_texto` son de Rafa: no los toques.
- No toques el portal (WordPress) ni horizontefinancieroasesores.com: solo REST como ya hace el sistema.
- Si un arreglo exige una decisión de Rafa (cambiar el alcance del análisis, una fuente de pago, algo
  irreversible), no lo hagas: déjalo propuesto en el informe.
- Código Python con barras invertidas: escríbelo con la herramienta Write, nunca con un heredoc de bash.

## Salida (obligatoria)

Escribe `data/feedback_sistema/hechos/{id}.json`:

```json
{
  "id": "...",
  "estado": "hecho | parcial | no_procede | error",
  "resumen_rafa": "2-3 frases en llano para Rafa: qué había detrás y qué se ha cambiado. Sin jerga ni nombres de ficheros.",
  "puntos": [{"feedback": "...", "diagnostico": "...", "causa": "fichero/lógica", "arreglo": "...", "verificado": "cómo"}],
  "cambios_sistema": [{"fichero": "...", "que": "...", "por_que": "..."}],
  "commit": "hash o null",
  "fondo": "relanzado | dashboard regenerado | no necesario",
  "propuestas_para_rafa": ["decisiones que no has tomado tú"]
}
```

Termina tu respuesta con el mismo `resumen_rafa`. Nunca termines con una pregunta: si algo requiere a Rafa, va
en `propuestas_para_rafa`.
