# Fund Analyzer — Known gaps de documentación operativa

Lista de áreas donde la documentación es incompleta y un tercero replicando el sistema necesitaría ayuda adicional. Identificados durante el inventario de Mayo 2026.

---

## 1. Deployment Windows vs Linux

**Gap**: el sistema está desarrollado en Windows 11 con OneDrive en el path. No está documentado:
- Cómo ejecutar en Linux/macOS (paths con espacios, encoding UTF-8 vs CP1252)
- Permisos de directorios (`data/funds/{ISIN}/raw/` se crea automáticamente — pero en Linux puede fallar si parent no escribible)
- Equivalente Linux de `analizar_fondo.bat` (script de conveniencia Windows)
- Encoding obligatorio: `sys.stdout.reconfigure(encoding="utf-8")` en orchestrator. En PowerShell legacy puede requerir `$OutputEncoding = [Text.Encoding]::UTF8`.

**Workaround actual**: WSL2 funciona transparente. macOS no probado.

**Recomendación**: añadir sección "Cross-platform" al README.md con verificaciones específicas por OS.

---

## 2. Flujo `_manual_edits` no claro

**Gap**: la convención `_manual_edits` (lista de paths protegidos en `output.json`) es crítica pero solo está documentada en CLAUDE.md sección 6. Falta:
- Workflow recomendado para hacer un patch manual (script ejemplo end-to-end)
- Cómo detectar conflictos cuando el analyst quiere actualizar un campo manualmente editado
- Cómo "liberar" un campo (quitar de `_manual_edits` para permitir regeneración)
- UI o CLI para inspeccionar qué campos están protegidos

**Workaround actual**: leer scripts existentes (`patch_all_nombres.py`, `clean_invented_gestores.py`, `mark_existing_manual_edits.py`) como referencia.

**Recomendación**: crear `tools/manual_edits_cli.py` con `--list`, `--add`, `--remove`, `--check-conflicts`.

---

## 3. Integraciones externas no documentadas

**Gap**: las 3 APIs externas tienen quirks no obvios:

### CNMV bulkdata
- URL catálogo: `https://www.cnmv.es/portal/publicaciones/descarga-informacion-individual`
- NO tiene API REST. Requiere navegar HTML y descargar XMLs uno por uno.
- Cookies necesarias para sesión (httpx `follow_redirects=True` + manual cookie jar).
- Rate limiting no documentado oficialmente — en práctica >50 req/min puede dar 429.

### Serper.dev
- Free tier: 2.500 queries/mes (suficiente para ~30 fondos nuevos)
- Pricing producción: $50 = 50K queries
- Auth: API key en header `X-API-KEY`
- Alternativas drop-in: SerpAPI (más caro), Brave Search API (gratis 2K/mes), ScaleSerp

### Wayback Machine CDX
- Gratis, sin auth
- Rate limit: ~10 req/sec sostenido, retries con backoff
- Endpoint: `http://web.archive.org/cdx/search/cdx?url={url}&output=json`

**Recomendación**: documentar en README.md sección "External Integrations" con quirks + alternativas.

---

## 4. Discovery offline fallback

**Gap**: si Serper.dev cae o se acaba el quota, ¿qué pasa?
- Actualmente: discovery_v2 falla en cascada Google y procede solo con Wayback + web directo
- NO hay cache de queries Serper (solo cache de respuestas LLM)
- NO hay fallback a Brave/SerpAPI configurable

**Workaround actual**: re-lanzar cuando Serper esté disponible. Cache LLM evita re-ejecutar el resto.

**Recomendación**: añadir `tools/serper_cache.py` (TTL 7 días) + abstracción `tools/search_provider.py` con fallback chain.

---

## 5. Quality rules no versionadas

**Gap**: `data/quality_rules.json` (106 reglas, 921 líneas) es editado a mano. Sin:
- Versionado semántico (v1.0.0, v1.1.0)
- Changelog que correlacione cambios con baselines de tests
- Validación de schema al cargar (cualquier error tipográfico solo se detecta en runtime)
- Tooling para añadir/modificar reglas (CLI o UI)

**Workaround actual**: Git history + comments en CLAUDE.md sección 9 (Histórico de cambios por Fase).

**Recomendación**: añadir `quality_rules_schema.json` + validador automático + `tools/quality_rules_cli.py` para diff/add/remove con bump automático de versión.

---

## 6. Roadmap post-Fase L opaco

**Gap**: el changelog en CLAUDE.md llega hasta Fase L (2026-04-29). Lo posterior está en:
- Memoria local del autor (no incluida en este paquete)
- Plan files temporales (`~/.claude/plans/*.md`)

Un tercero replicando no sabe qué está en curso, qué está bloqueado, qué se ha intentado y abandonado.

**Workaround actual**: leer `MEMORY_SUMMARY.md` (Fase Cost-Opt + Fase M).

**Recomendación**: mantener `ROADMAP.md` en raíz del repo con tabla "En curso / Pendiente / Abandonado" actualizada cada Fase.

---

## Resumen de prioridades

| Gap | Prioridad para uso producción | Tiempo estimado fix |
|---|---|---|
| 1. Deployment cross-platform | ALTA si target Linux/macOS | 2h |
| 2. `_manual_edits` workflow | ALTA (riesgo de borrar patches) | 3h |
| 3. APIs externas quirks | MEDIA (descubrible por errores runtime) | 1h |
| 4. Discovery offline fallback | BAJA (Serper raramente cae) | 4h |
| 5. Quality rules versioning | MEDIA (edición frecuente) | 4h |
| 6. Roadmap actualizado | BAJA (informativo) | 30 min |

Total: ~14h trabajo para cerrar todos los gaps de documentación operativa.
