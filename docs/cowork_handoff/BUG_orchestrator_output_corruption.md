# BUG — Orchestrator corrompe output.json en rollback de quality_loop

**Severidad**: ALTA en producción si las APIs fallan en mitad de un run.
**Detectado**: 2026-05-04 durante smoke test cowork de Avantage (ES0112231008).
**Branch**: `v2-cowork`
**Fichero implicado**: `agents/orchestrator.py` — método `_run_quality_loop` y `_consume_cowork_analyst`.

---

## Síntoma observado

Tras ejecutar `python -m agents.orchestrator --isin ES0112231008 --consume-cowork`:
1. El consume integró correctamente las 8 secciones del JSON cowork en `output.json` (mensaje "Analyst de Cowork integrado").
2. validation_agent OK, meta_agent OK.
3. quality_loop detectó 12 fallos (esperable — schema mismatch entre cowork v2 y reglas v1).
4. quality_loop intentó re-ejecutar el `analyst_agent` para corregir.
5. El analyst_agent falló porque las APIs Anthropic+Gemini estaban sin saldo: `"Your credit balance is too low to access the Anthropic API"`.
6. quality_loop hizo rollback al `output.iter_0.json`.
7. **Tras el rollback, el `output.json` quedó truncado a la mitad** (en concreto, mid-string en `analyst_synthesis.gestores.perfiles[0].trayectoria`).

Tamaño esperado: ~770 KB. Tamaño obtenido: 720 KB con el fichero terminando en mitad de palabra ("**5 ").

## Reproducción

1. Tener un fondo con bundle válido en `data/funds/{ISIN}/bundle/`.
2. Tener un `analyst_synthesis_cowork.json` válido producido por la skill.
3. Vaciar el credit balance de Anthropic (o forzar fallo de API otra forma).
4. Ejecutar `python -m agents.orchestrator --isin {ISIN} --consume-cowork`.
5. Esperar a que quality_loop intente retry y haga rollback.
6. Inspeccionar `output.json` → estará truncado.

## Hipótesis de la causa

El método `_consume_cowork_analyst` o el `_rollback` del quality_loop:
- Probablemente usa `open(path, 'w')` y escribe en chunks, o usa `json.dump` con un buffer que se interrumpe.
- En algún punto del flujo de rollback, la escritura no se completa pero el fichero queda en su tamaño truncado en disco.

Posibles culpables:
- **Atomic write missing**: la escritura del rollback no usa el patrón "escribir a tmp + rename atómico". Si se interrumpe, queda parcial.
- **Excepción no manejada en mitad del rollback**: si una excepción salta entre `truncate()` y el final del `write()`, el fichero queda corrupto.
- **Write durante restauración** desde `output.iter_0.json`: probablemente lee iter_0 y escribe a output.json sin lock, pero si en mitad falla...

## Propuesta de fix

En `agents/orchestrator.py` (y en `tools/output_merger.save_output` si se usa):

### Opción A — Atomic write con rename

```python
import os
import tempfile
from pathlib import Path

def atomic_write_json(path: Path, data: dict) -> None:
    """Escribe JSON atómicamente. Si falla, output.json queda intacto."""
    tmp_dir = path.parent
    fd, tmp_path = tempfile.mkstemp(suffix='.tmp', dir=tmp_dir, text=True)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        # Rename atómico — solo se sobrescribe output.json si tmp se escribió completo
        os.replace(tmp_path, path)
    except Exception:
        # Limpiar tmp si quedó
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        raise
```

Aplicar en:
- `_consume_cowork_analyst` cuando guarda `output_data`
- `_rollback_to_iter` (o equivalente del quality_loop)
- `save_output` en `tools/output_merger.py`

### Opción B — Backup defensivo antes de cualquier escritura

Mantener `output.json.before_quality` siempre antes de quality_loop:

```python
def _run_quality_loop(self):
    # Backup defensivo
    output_path = self.fund_dir / "output.json"
    backup_path = self.fund_dir / "output.json.before_quality"
    if output_path.exists():
        shutil.copy2(output_path, backup_path)
    try:
        # ... quality_loop logic ...
    except Exception as e:
        # Si algo falla, restaurar backup
        if backup_path.exists():
            shutil.copy2(backup_path, output_path)
        raise
    finally:
        if backup_path.exists():
            backup_path.unlink()
```

### Opción C — Exigir éxito del retry o NO modificar output.json

Si el quality_loop no consigue mejora por API failure, NO debería tocar output.json en absoluto. Salir con warning, dejar el output del consume tal cual.

```python
def _run_quality_loop(self):
    iter_n = 0
    while iter_n < self.max_iter:
        try:
            self._reagent_upstream(iter_n)
            self._reanalyst(iter_n)  # esto puede fallar por API
        except APIException as e:
            self._log("WARN", f"Quality iter {iter_n} falló por API: {e}")
            self._log("WARN", "Output sin modificar — quality loop abortado")
            return  # NO rollback, NO modificación, salir
        # Evaluar fallos...
```

## Recomendación

Combinar **A + C**: atomic writes en TODA escritura de output.json + abortar quality_loop sin modificar nada si el retry falla por API.

## Tests para verificar fix

1. **Unit test**: `tests/test_orchestrator_atomic_writes.py` que mockea un fallo de escritura a mitad del flujo y verifica que `output.json` queda intacto.
2. **Integration test**: simular API failure en quality_loop con un fondo de prueba, verificar que `output.json` post-error es válido (parseable + tamaño completo).
3. **Smoke test manual**: re-ejecutar el escenario que produjo el bug (consume-cowork con APIs sin saldo) y verificar que tras el fix el output queda OK.

## Workaround temporal (mientras no haya fix)

NO ejecutar `--consume-cowork` cuando las APIs están sin saldo. Si pasa accidentalmente, el output.json se puede reconstruir desde:
- `data/funds/{ISIN}/cnmv_data.json` o `intl_data.json` (top-level fields)
- `data/funds/{ISIN}/analyst_synthesis_cowork.json` (analyst_synthesis)
- `git show v1-api-stable:data/funds/{ISIN}/output.json` para campos como `lecturas_externas`, `timeline`, `hechos_relevantes`

(Ya hay un script ad-hoc usado para Avantage en este escenario — referencia para futura skill de recovery.)
