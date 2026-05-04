# BUNDLE_CONTRACT.md — Contrato del bundle de inputs para el analyst

**Versión del contrato**: `1.0.0`
**Generado**: 2026-05-04
**Aplica a**: pipeline fund-analyzer post-Fase M, branch `v2-cowork`

---

## Propósito

Define el formato exacto del paquete de datos que la prep determinista (CNMV/INT pipeline) entrega a CUALQUIER consumidor del analyst — sea el `analyst_agent.py` legacy (vía API Anthropic), la skill `analyst-cowork` (vía Claude Max), o futuros consumidores (otro modelo, otro proveedor).

Garantías:
- Si el bundle valida → cualquier consumidor compatible con esta versión del contrato puede generar `analyst_synthesis.*` consistente.
- Si dos consumidores reciben el mismo bundle → producen outputs estructuralmente equivalentes (variabilidad solo en redacción narrativa).
- Si la prep cambia internamente (nuevo agente upstream, nueva fuente) pero respeta este contrato → los consumidores no se enteran ni rompen.

El contrato es la **frontera estable**. La prep puede evolucionar libremente por dentro siempre que la salida del bundle siga cumpliendo este documento.

---

## Layout del bundle

```
data/funds/{ISIN}/bundle/
├── fund_data.json         (obligatorio — cnmv_data.json para ES, intl_data.json para INT, copiado tal cual)
├── manager_profile.json   (obligatorio)
├── letters_data.json      (obligatorio, puede tener cartas=[])
├── readings.json          (obligatorio, puede tener readings=[])
├── sources.json           (obligatorio)
└── bundle_manifest.json   (obligatorio — meta del bundle)
```

**Naming**: el bundle usa `fund_data.json` como nombre canónico independientemente del tipo. La prep copia `cnmv_data.json` o `intl_data.json` con ese nombre nuevo. El campo `tipo` dentro del JSON ("ES" | "INT") es la señal que el consumidor usa para saber qué schema esperar.

**No otros ficheros**: el directorio `bundle/` contiene exactamente esos 6 ficheros y nada más. Cualquier fichero adicional → warning en el validador.

**Tamaños esperados**:
- `fund_data.json`: 50-200 KB
- `manager_profile.json`: 20-150 KB (depende de cuántos `articulos_completos` extrajo `manager_deep_agent`)
- `letters_data.json`: 100-600 KB (dominante por `texto_completo` de cartas)
- `readings.json`: 5-50 KB
- `sources.json`: 5-30 KB
- `bundle_manifest.json`: <5 KB

Bundle total típico: 200-800 KB. Si <100 KB → bundle pobre, sospechoso. Si >2 MB → algún input desbordado, investigar.

---

## Ficheros del bundle (especificación)

### 1. `fund_data.json`

Datos extraídos por la prep determinista del fondo (CNMV para ES, regulators+discovery+extractor para INT). Este es el corazón factual del fondo.

**Campos top-level obligatorios** (ambos tipos):

| Campo | Tipo | Notas |
|---|---|---|
| `isin` | str | Match `^[A-Z]{2}[A-Z0-9]{9}[0-9]$` |
| `nombre` | str | Nombre oficial del fondo |
| `gestora` | str | Nombre de la gestora |
| `tipo` | str | `"ES"` o `"INT"` |
| `ultima_actualizacion` | str (ISO 8601) | Timestamp de la última extracción |
| `kpis` | dict | Ver subesquema |
| `cuantitativo` | dict | Ver subesquema |
| `posiciones` | dict | Ver subesquema |
| `fuentes` | dict | Catálogo de docs descargados |

**Campo `kpis` — obligatorio que `aum_actual_meur` esté presente** (puede ser `None` si genuinamente desconocido). Otros campos típicos: `num_participes`, `ter_pct`, `coste_gestion_pct`, `volatilidad_pct`, `clasificacion`, `perfil_riesgo`, `divisa`, `depositario`, `anio_creacion`, `fecha_registro`. Para INT también: `rating_morningstar`, `benchmark`, `num_activos_cartera`, `concentracion_top10_pct`.

**Campo `cuantitativo` — al menos `serie_aum` debe ser una lista (puede estar vacía)**. Estructura típica de cada serie: lista de `{periodo: "YYYY-MM" o "YYYY", ...}` con valor.

**Campo `posiciones`** — debe tener `actuales` (lista, posible vacía) e `historicas` (lista, posible vacía). Cada posición actual: `nombre`, `peso_pct`, opcional `sector`, `pais`, `divisa`, `tipo`, `valor_mercado_miles`.

**Específico ES** (extra):
- `cualitativo` (dict con `seccion_9_texto_completo` y `seccion_10_perspectivas_texto`)
- `analisis_consistencia` (dict con `periodos: list`)
- `comision_exito` (dict)
- `nif` (str)

**Específico INT** (extra):
- `clases` (lista de share classes)
- `economia_fondo` (dict con management_fees_total, net_result, expense_ratio_breakdown, viabilidad_nota)
- `cualitativo` (dict con `estrategia`, `historia_fondo`, `gestores: list`, `tipo_activos`, `filosofia_inversion`, `objetivos_reales`, `proceso_seleccion`)
- `analisis_consistencia` (dict con `periodos: list` siguiendo schema K15)

**Anti-invención**: si un campo no está disponible, debe ser `None` o lista vacía. NUNCA un string genérico tipo "no disponible" o "ver informe completo".

### 2. `manager_profile.json`

Identificación + perfil del equipo gestor. Producto de `manager_profiler.py` + `manager_deep_agent.py`.

**Campos obligatorios**:

| Campo | Tipo | Notas |
|---|---|---|
| `isin` | str | Match con bundle |
| `equipo_gestor` | list[str] | Lista plana de nombres canónicos. Max 2 nombres (lead + co). PUEDE ESTAR VACÍA si el extractor no identificó a nadie con confidence suficiente. |
| `equipo` | list[dict] | Cada item: `{nombre, cargo, biografia, ...}`. Estructura más rica. |
| `fuentes_web` | list[dict] | URLs consultadas para identificar el equipo. |

**Campos recomendados pero opcionales**:

| Campo | Tipo | Notas |
|---|---|---|
| `equipo_roles` | dict | `{nombre: {is_lead: bool, is_co: bool, _source: "opus_high"}}` (post-Fase J). |
| `articulos_completos` | dict | `{nombre_gestor: [{fuente_url, titulo, texto_completo, fecha}, ...]}` — extraído por manager_deep_agent. |
| `_opus_lead_confidence` | str | `"high" | "medium" | "low"`. |
| `informacion_cartas` | list[dict] | Síntesis K15 de cartas: `{periodo, resumen_ejecutivo, decisiones_cartera, perspectivas, ...}`. |
| `informacion_cnmv` | dict | (solo ES) seccion_9_texto_completo, seccion_10_perspectivas_texto, hechos_relevantes. |

**Compatibilidad nombres**: los pipelines actuales también escriben `nombre_completo` (ES) o `fund_name` (INT). El bundle_exporter normaliza ambos a `fund_name` en el bundle.

**Anti-invención v2 (Fase I)**: si `equipo_gestor` está vacía, NO se puede inferir nada. El consumidor debe respetar y NO inventar nombres.

### 3. `letters_data.json`

Cartas trimestrales/semestrales del gestor. Producto de `letters_collector.py` + `letters_deep_agent.py`.

**Campos obligatorios**:

| Campo | Tipo | Notas |
|---|---|---|
| `isin` | str | Match con bundle |
| `cartas` | list[dict] | Lista de cartas. PUEDE estar vacía. |
| `fuentes_consultadas` | list | URLs/dominios consultados |

**Estructura de cada item de `cartas`**:

| Campo | Tipo | Notas |
|---|---|---|
| `periodo` | str | `"2025-Q4"`, `"2025-S1"`, etc. |
| `fecha_inferida` | str (YYYY-MM-DD) | Fecha publicación |
| `tipo` | str | `"trimestral"`, `"semestral"`, `"anual"`, `"factsheet"` |
| `titulo` | str | Título del documento |
| `url_fuente` | str | URL de descarga |
| `archivo` | str | Path local relativo |
| `num_paginas` | int | Páginas del PDF |
| `texto_completo` | str | Texto extraído (puede ser "" si fallback K15 lo pobló) |

**Campos K15 opcionales** (post-Fase L1, dentro de cada carta o en `analisis_consistencia`):
- `tesis_gestora`, `decisiones_tomadas`, `contexto_mercado`, `citas_textuales`

**Validación**: si `cartas` está vacía Y `analisis_consistencia.periodos` también vacío → la sección `evolucion` y `cartera.citas` del analyst quedarán pobres. Es legítimo (gestoras pequeñas no publican), pero el validador debe emitir warning.

### 4. `readings.json`

Análisis externos del fondo en prensa especializada, blogs pro, agregadores. Producto de `readings_collector.py` (puede combinar `lecturas.json` + `analisis_externos.json` históricos).

**Estructura**:

```json
{
  "isin": "...",
  "generado": "ISO timestamp",
  "readings": [
    {
      "titulo": "...",
      "fuente": "Morningstar | Citywire | Funds Society | ...",
      "fecha": "YYYY-MM-DD",
      "url": "...",
      "tipo": "review | interview | rating | article",
      "resumen": "..." (str con resumen del extractor),
      "puntos_clave": [...] (list[str], opcional),
      "citas_textuales": [...] (list[str], opcional),
      "palabras_estimadas": int (opcional)
    },
    ...
  ]
}
```

**Schema mínimo de cada reading**: `titulo`, `fuente`, `url`. Los demás opcionales pero el consumidor puede no poder usar el reading si falta `fecha` o `resumen`/`puntos_clave`.

**Compatibilidad**: si el upstream entregó `lecturas.json` + `analisis_externos.json` separados, el bundle_exporter los unifica deduplicando por URL.

**Validación**: lista puede estar vacía. Si vacía → la sección `fuentes_externas` del analyst será mínima. Legítimo para fondos sin cobertura mediática.

### 5. `sources.json`

Catálogo consolidado de TODAS las fuentes que la prep ha usado o detectado. Sirve al analyst para la sección `documentos` del output y para anti-invención.

**Estructura**:

```json
{
  "isin": "...",
  "generado": "ISO timestamp",
  "documentos": [
    {
      "tipo": "KIID | Prospectus | Annual Report | Carta trimestral | Factsheet | Letter | Article",
      "titulo": "...",
      "url": "...",
      "fecha": "YYYY-MM-DD" (opcional),
      "fuente_origen": "CNMV | CSSF | Gestora | Wayback | Morningstar | ..." 
    },
    ...
  ],
  "fuentes_consultadas": [
    {"url": "...", "tipo": "web | pdf | xml", "exito": bool}
  ]
}
```

**Validación**: lista `documentos` puede estar vacía solo si todos los upstream también lo están. Para un fondo ES con CNMV, debería haber al menos 1 entry de tipo "Anexo CNMV".

### 6. `bundle_manifest.json`

Meta del bundle. **Lo escribe el `bundle_exporter`, lo verifica el `bundle_validator`**.

**Estructura**:

```json
{
  "schema_version": "1.0.0",
  "isin": "ES0112231008",
  "fund_name": "AVANTAGE FUND, FI",
  "tipo": "ES",
  "generated_at": "2026-05-04T18:30:00Z",
  "exporter_version": "1.0.0",
  "files": {
    "fund_data.json": {
      "size_bytes": 110515,
      "sha256": "abc123...",
      "source_path": "data/funds/ES0112231008/cnmv_data.json"
    },
    "manager_profile.json": {
      "size_bytes": 54340,
      "sha256": "def456...",
      "source_path": "data/funds/ES0112231008/manager_profile.json"
    },
    "letters_data.json": {
      "size_bytes": 505424,
      "sha256": "ghi789...",
      "source_path": "data/funds/ES0112231008/letters_data.json"
    },
    "readings.json": {
      "size_bytes": 8200,
      "sha256": "jkl012...",
      "source_paths": [
        "data/funds/ES0112231008/lecturas.json",
        "data/funds/ES0112231008/analisis_externos.json"
      ]
    },
    "sources.json": {
      "size_bytes": 12300,
      "sha256": "mno345...",
      "source_path": "synthesized"
    }
  },
  "validation": {
    "validator_version": "1.0.0",
    "validated_at": "2026-05-04T18:30:05Z",
    "valid": true,
    "errors": [],
    "warnings": []
  },
  "stats": {
    "num_letters": 46,
    "num_readings": 10,
    "num_managers_identified": 5,
    "num_positions": 46,
    "kpis_completeness_pct": 92.3
  }
}
```

`stats` es informativo, ayuda a detectar bundles pobres sin abrir los JSONs.

---

## Reglas de validación (bundle_validator)

### Hard checks (FAIL si fallan)

1. Existen los 6 ficheros del bundle.
2. Cada `.json` parsea correctamente.
3. `bundle_manifest.json.schema_version` está en la lista de versiones soportadas (`["1.0.0"]` por ahora).
4. Cada hash en `bundle_manifest.files[*].sha256` coincide con el sha256 real del fichero.
5. `fund_data.json` tiene `isin`, `nombre`, `gestora`, `tipo` y match `tipo ∈ {"ES", "INT"}`.
6. `fund_data.json.isin` == `manager_profile.json.isin` == `letters_data.json.isin` == `readings.json.isin` == `sources.json.isin` == `bundle_manifest.json.isin`.
7. `kpis.aum_actual_meur` está presente (puede ser `None` o número).
8. `posiciones.actuales` es lista (puede estar vacía).
9. `equipo_gestor` en manager_profile es lista (puede estar vacía).

### Soft checks (WARNING si fallan)

1. `cartas` está vacía → "fondo sin cartas trimestrales detectadas — analyst.evolucion y cartera.citas serán mínimos".
2. `readings` está vacía → "fondo sin cobertura externa detectada — analyst.fuentes_externas será mínimo".
3. `kpis.aum_actual_meur` es None → "AUM no disponible — analyst lo omitirá en encabezado".
4. `equipo_gestor` está vacía y `equipo` también → "no se identificó equipo gestor con confidence suficiente — analyst.gestores será placeholder".
5. `bundle_manifest.stats.kpis_completeness_pct < 50` → "más de la mitad de KPIs faltan — output narrativo limitado".
6. Para INT: `economia_fondo.viabilidad_nota` ausente → "no hay análisis de viabilidad económica — sección estrategia limitada".
7. Tamaño bundle <100KB total → "bundle inusualmente pequeño, posible fallo upstream".
8. Tamaño bundle >2MB total → "bundle inusualmente grande, revisar `articulos_completos` o `letters_data`".
9. Ficheros no listados en el contrato presentes en `bundle/` → "fichero extra detectado: X".

### Output del validador

```json
{
  "valid": true,
  "schema_version": "1.0.0",
  "errors": [],
  "warnings": [
    "cartas vacía — analyst.evolucion limitado",
    "readings vacía — analyst.fuentes_externas mínimo"
  ],
  "stats": {...}
}
```

CLI: `python -m agents.bundle_validator <ISIN>` imprime tabla legible + exit code 0 (valid) o 1 (errors). Warnings no causan exit code != 0.

---

## Compatibilidad y versionado

### Versionado semántico

`schema_version` sigue SemVer:
- **MAJOR** (`2.0.0`): cambios que rompen compatibilidad. Renombrar campos obligatorios, cambiar tipos, eliminar ficheros del bundle. Los consumidores existentes NO funcionarán sin actualización.
- **MINOR** (`1.1.0`): añade campos opcionales o ficheros opcionales. Consumidores existentes siguen funcionando, ignoran lo nuevo.
- **PATCH** (`1.0.1`): clarificaciones sin cambios estructurales (typo fixes, mejor descripción).

### Política de bumps

- Cualquier PR que toque la prep o este contrato debe declarar bump esperado en el commit message.
- Si nuevo campo es OBLIGATORIO → MAJOR bump (rompe consumidores).
- Si nuevo campo es OPCIONAL pero recomendado → MINOR bump.
- Si solo cambia el manifest (más stats) → MINOR bump.

### Lista de versiones soportadas

| Versión | Estado | Cambios | Fecha |
|---|---|---|---|
| 1.0.0 | Activa | Inicial. ES + INT bundle, 5 ficheros + manifest. | 2026-05-04 |

Cuando llegue v1.1.0, el validador acepta `schema_version ∈ {"1.0.0", "1.1.0"}`. Cuando llegue v2.0.0, deja de aceptar 1.x salvo que se mantenga rama de compatibilidad.

### Migración entre versiones

Cada bump MAJOR debe traer un script `tools/bundle_migrate_vN_to_vM.py` que convierte un bundle viejo al nuevo schema. Ese script lo ejecuta el validador automáticamente si detecta `schema_version` antiguo, escribe el migrado a `bundle/.migrated_from_v{N}/`, y avisa.

---

## Garantías que el bundle DA y NO da al consumidor

### Da

- Los 5 ficheros JSON existen y son válidos.
- Los `isin` coinciden entre todos.
- El tipo (ES/INT) es claro y los campos type-specific están presentes.
- La completitud de cada sección está medida y reportada en stats.
- Los hashes permiten detectar drift si el bundle se modifica entre exportación y consumo.

### NO da

- **Calidad del contenido**. Un bundle válido puede tener `letters` con texto OCR malo. El consumidor es responsable de manejar contenido pobre.
- **Recencia**. El bundle es snapshot de cuándo se ejecutó la prep. Si pasaron meses, el AUM/posiciones pueden estar desactualizados. El consumidor debe respetar `ultima_actualizacion`.
- **Completitud semántica**. Que `kpis.aum_actual_meur` no sea `None` no significa que sea correcto. Se asume que la prep hizo su trabajo.
- **Anti-invención del consumidor**. El bundle no puede impedir que el LLM consumidor invente. La responsabilidad de no fabricar está en el prompt del consumidor + validación post-output.

---

## Ejemplo mínimo válido (bundle ES)

```
data/funds/ES0112231008/bundle/
├── fund_data.json          (AVANTAGE FUND extraído por cnmv_agent, ~110KB)
├── manager_profile.json    (Juan Gómez Bada + 4 más, fuentes_web 13 entries, ~54KB)
├── letters_data.json       (46 cartas trimestrales + semestrales, ~505KB)
├── readings.json           (10 readings de Morningstar/Citywire/blogs, ~8KB)
├── sources.json            (~12KB con doc list completa)
└── bundle_manifest.json    (~3KB con hashes + stats)
```

Total: ~692 KB. Válido sin warnings (todos los campos obligatorios cubiertos, cartas y readings ricos).

## Ejemplo mínimo válido (bundle INT)

```
data/funds/IE00B6T42S66/bundle/
├── fund_data.json          (Trojan IE extraído por intl_extractor_v2, ~100KB)
├── manager_profile.json    (Sebastian Lyon + Francis Brooke, opus_high confidence, ~25KB)
├── letters_data.json       (97 cartas, deep_extraction K15, ~150KB)
├── readings.json           (variable, depende cobertura)
├── sources.json            (CSSF + gestora + Wayback ~30 docs, ~15KB)
└── bundle_manifest.json    (~3KB)
```

Total: ~290-400 KB. Válido. Warning esperable: si `cartas K15` no estructurado correctamente para algunos periodos, soft check de letters K15-completeness.

---

## Implementación recomendada del bundle_exporter

```python
# agents/bundle_exporter.py — pseudocódigo

def run(isin: str) -> dict:
    """Genera el bundle en data/funds/{ISIN}/bundle/. Devuelve manifest dict."""
    fund_dir = Path(f"data/funds/{isin}")
    bundle_dir = fund_dir / "bundle"
    bundle_dir.mkdir(exist_ok=True)
    
    # 1. fund_data.json — copia normalizada
    if (fund_dir / "cnmv_data.json").exists():
        src = fund_dir / "cnmv_data.json"
    elif (fund_dir / "intl_data.json").exists():
        src = fund_dir / "intl_data.json"
    else:
        raise BundleExportError(f"Ni cnmv_data.json ni intl_data.json en {fund_dir}")
    shutil.copy2(src, bundle_dir / "fund_data.json")
    
    # 2. manager_profile.json — copia
    mp_src = fund_dir / "manager_profile.json"
    if not mp_src.exists():
        mp_src = fund_dir / "manager_profile.backup_pre_deep.json"
    shutil.copy2(mp_src, bundle_dir / "manager_profile.json")
    
    # 3. letters_data.json — copia
    shutil.copy2(fund_dir / "letters_data.json", bundle_dir / "letters_data.json")
    
    # 4. readings.json — sintetiza desde lecturas.json + analisis_externos.json
    readings = synthesize_readings(fund_dir)
    (bundle_dir / "readings.json").write_text(json.dumps(readings, ensure_ascii=False, indent=2))
    
    # 5. sources.json — sintetiza desde fund_data.fuentes + manager_profile.fuentes_web + ...
    sources = synthesize_sources(fund_dir)
    (bundle_dir / "sources.json").write_text(json.dumps(sources, ensure_ascii=False, indent=2))
    
    # 6. manifest
    manifest = build_manifest(bundle_dir, isin, schema_version="1.0.0")
    (bundle_dir / "bundle_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    
    return manifest
```

---

## Implementación recomendada del bundle_validator

```python
# agents/bundle_validator.py — pseudocódigo

SUPPORTED_VERSIONS = {"1.0.0"}

def validate(isin: str) -> dict:
    bundle_dir = Path(f"data/funds/{isin}/bundle")
    errors, warnings = [], []
    
    # Hard checks
    for fname in ["fund_data.json", "manager_profile.json", "letters_data.json",
                  "readings.json", "sources.json", "bundle_manifest.json"]:
        if not (bundle_dir / fname).exists():
            errors.append(f"missing: {fname}")
    if errors:
        return {"valid": False, "errors": errors, "warnings": warnings}
    
    manifest = json.loads((bundle_dir / "bundle_manifest.json").read_text())
    if manifest["schema_version"] not in SUPPORTED_VERSIONS:
        errors.append(f"unsupported schema_version: {manifest['schema_version']}")
    
    # Hash verification
    for fname, meta in manifest["files"].items():
        actual = hashlib.sha256((bundle_dir / fname).read_bytes()).hexdigest()
        if actual != meta["sha256"].replace("sha256:", ""):
            errors.append(f"hash mismatch: {fname}")
    
    # ISIN coherence
    fund_data = json.loads((bundle_dir / "fund_data.json").read_text())
    # ... otros checks ...
    
    # Soft checks
    letters = json.loads((bundle_dir / "letters_data.json").read_text())
    if not letters.get("cartas"):
        warnings.append("cartas vacía — analyst.evolucion limitado")
    
    # ... más soft checks ...
    
    return {
        "valid": not errors,
        "schema_version": manifest["schema_version"],
        "errors": errors,
        "warnings": warnings,
        "stats": manifest.get("stats", {})
    }
```

---

## Cambios futuros previstos (NO incluidos en v1.0.0)

Estos son hooks identificados que probablemente requerirán bump:

- **v1.1.0** (MINOR esperado): añadir `bundle/discovery_data.json` con metadata de cómo discovery encontró cada doc (Wayback vs gestora vs Google). Actualmente esa info está dispersa en `intl_discovery_data.json` (solo INT).
- **v1.2.0** (MINOR esperado): añadir campo `regulator_data` en bundle_manifest con la identity card del regulador (CSSF/CBI/AMF/Bundesanzeiger). Hoy se queda en `cssf_data.json` o equivalente fuera del bundle.
- **v2.0.0** (MAJOR especulativo): refactor de `cualitativo` para INT — actualmente vive dentro de `fund_data.json`, podría ser fichero separado `qualitative_data.json` con estructura más rica de cara a multi-modelo.

Cualquier propuesta de cambio se discute en issue de GitHub etiquetado `bundle-contract` antes de mergear.
