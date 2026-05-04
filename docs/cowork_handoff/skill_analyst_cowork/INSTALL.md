# Instalación de la skill `analyst-cowork`

## 1. Copiar la skill al directorio de skills

Tu setup tiene las skills personales en:
```
C:\Users\RafaelGarcía\AppData\Roaming\Claude\local-agent-mode-sessions\skills-plugin\1e3e9be3-b0b5-4203-b4e6-39c115948f19\14551871-19e2-48c8-bafd-9a59eb38c418\skills\
```

Crea la carpeta `analyst-cowork/` ahí y copia los 2 ficheros:
- `SKILL.md`
- `output_schema.example.json`

Después invoca `gestion-skills` para que el `Resumen skills.xlsx` quede actualizado (regla global de tu CLAUDE.md personal).

## 2. Modificaciones al pipeline Python (orchestrator.py)

Hay que añadir 2 flags nuevos:

### Flag `--prep-only`

Corta el pipeline antes de `analyst_agent`. Ejecuta todos los agentes de recolección y deja los JSONs intermedios listos para que la skill los consuma.

En `agents/orchestrator.py`, dentro de la función principal de orquestación (busca el bloque que invoca `analyst_agent.run()`), añade:

```python
# Argparse: añadir el flag
parser.add_argument(
    "--prep-only",
    action="store_true",
    help="Ejecuta toda la prep determinista (CNMV, INT, manager, letters, readings, sources) "
         "pero salta analyst_agent. Útil para correr el analyst en Cowork bajo Max."
)
parser.add_argument(
    "--consume-cowork",
    action="store_true",
    help="Salta toda la prep y consume data/funds/{ISIN}/analyst_synthesis_cowork.json "
         "para integrarlo en output.json + correr quality + dashboard."
)

# En el flujo principal, justo antes de llamar a analyst_agent:
if args.prep_only:
    self._log("INFO", "PREP-ONLY mode: deteniendo antes de analyst_agent")
    self._log("INFO", f"Inputs listos en data/funds/{isin}/")
    self._log("INFO", "Próximo paso: invoca skill 'analyst-cowork' en Cowork/Claude Code")
    self._log("INFO", f"Tras la skill, ejecuta: python -m agents.orchestrator --isin {isin} --consume-cowork")
    return  # exit early

# En el flujo principal, al inicio:
if args.consume_cowork:
    cowork_json = Path(f"data/funds/{isin}/analyst_synthesis_cowork.json")
    if not cowork_json.exists():
        self._log("ERROR", f"No existe {cowork_json}. Ejecuta la skill analyst-cowork primero.")
        sys.exit(1)
    self._consume_cowork_analyst(isin, cowork_json)
    # Saltamos directamente al paso 5-8 (validation + meta + quality + publication_calendar)
    self._run_validation_and_quality(isin)
    return
```

### Método `_consume_cowork_analyst`

Añade este método nuevo a la clase del orchestrator:

```python
def _consume_cowork_analyst(self, isin: str, cowork_json: Path) -> None:
    """Integra analyst_synthesis_cowork.json en output.json del fondo.

    Respeta _manual_edits (no sobrescribe campos protegidos).
    Marca analyst_synthesis.* como _manual_edits para que el cnmv/int agent no lo pise.
    Verifica input_files_hash contra los JSONs actuales — avisa si han cambiado.
    """
    import hashlib
    from tools.output_merger import save_output, mark_manual_edit
    from tools.output_accessor import get_isin

    cowork_data = json.loads(cowork_json.read_text(encoding="utf-8"))

    # Verificar hash de inputs
    expected_hashes = cowork_data.get("_meta", {}).get("input_files_hash", {})
    drift = []
    for fname, expected in expected_hashes.items():
        path = Path(f"data/funds/{isin}/{fname}")
        if not path.exists():
            drift.append(f"{fname} ya no existe")
            continue
        actual = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        if expected and not expected.endswith(actual.split(":")[1]):
            drift.append(f"{fname} cambió desde la skill")
    if drift:
        self._log("WARN", f"Drift en inputs detectado: {drift}. El analyst puede estar desactualizado.")

    # Cargar output.json actual y mergear
    output_path = Path(f"data/funds/{isin}/output.json")
    output_data = json.loads(output_path.read_text(encoding="utf-8")) if output_path.exists() else {}

    # Sobrescribir analyst_synthesis con el de la skill
    output_data["analyst_synthesis"] = cowork_data["analyst_synthesis"]

    # Marcar como manual_edit para que futuras runs del Python analyst no lo sobrescriban
    for sec in cowork_data["analyst_synthesis"]:
        mark_manual_edit(output_data, f"analyst_synthesis.{sec}")

    # Guardar metadata del run
    output_data.setdefault("_meta", {}).setdefault("cowork_runs", []).append({
        "ts": cowork_data["_meta"]["generated"],
        "main_model": cowork_data["_meta"]["main_model"],
        "audit_model": cowork_data["_meta"]["audit_model"],
        "anti_invencion_flagged_count": len(cowork_data["_meta"].get("anti_invencion_flagged", [])),
    })

    save_output(isin, output_data)
    self._log("INFO", f"Analyst de Cowork integrado en {output_path}")
    self._log("INFO", "Continuando con validation + quality + dashboard...")
```

### Método `_run_validation_and_quality`

Si no existe ya, extrae a un método separado los pasos 5-8 originales (validation_agent, meta_agent, quality_loop, publication_calendar, dashboard generation) para poder reusarlo desde el modo `--consume-cowork`.

## 3. Smoke test

Una vez instalado todo:

```bash
# Fondo de prueba ya analizado previamente (compara output)
ISIN=LU3038481936

# 1. Limpia el analyst previo (mantén KPIs)
python -c "
import json
from pathlib import Path
p = Path(f'data/funds/$ISIN/output.json')
d = json.loads(p.read_text(encoding='utf-8'))
d.pop('analyst_synthesis', None)
p.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding='utf-8')
"

# 2. Re-ejecuta la prep (debería ir rápido si los JSONs intermedios existen)
python -m agents.orchestrator --isin $ISIN --prep-only

# 3. Abre Cowork (o claude-code en esta carpeta) y di:
#    "analiza fondo LU3038481936 con la skill analyst cowork"
# La skill debe leer los inputs, generar las 8 secciones, hacer audit con
# subagente Sonnet, y escribir analyst_synthesis_cowork.json.

# 4. Integra
python -m agents.orchestrator --isin $ISIN --consume-cowork

# 5. Genera dashboard
python dashboard/generate_dashboard.py $ISIN

# 6. Compara visualmente con el dashboard previo en Git history
git diff dashboard/fund-$ISIN.html | head -200
```

## 4. Flujo para Rafa día a día

```bash
# Cuando quiera analizar un fondo nuevo:
ISIN=ESxxxxxxxxxxx
python -m agents.orchestrator --isin $ISIN --prep-only       # 5-15 min, ~$0 (sin LLM síntesis)

# En Cowork:
"analyst cowork ESxxxxxxxxxxx"                                # 10-15 min, 0€ (Max)

# Volver a terminal:
python -m agents.orchestrator --isin $ISIN --consume-cowork  # 1-2 min, ~$0
python dashboard/generate_dashboard.py $ISIN                  # 10s
start dashboard/fund-$ISIN.html
```

## 5. Qué NO se debe romper

Tras instalar todo, verifica que el flujo viejo sigue funcionando:
```bash
# Sin flags = comportamiento original con analyst Python
python -m agents.orchestrator --isin ES0112231008 --auto
```

Esto debe seguir generando el mismo output.json que antes (vía analyst_agent.py + Anthropic API). La skill de Cowork es una alternativa, no un reemplazo forzoso.

## 6. Cuándo usar cada modo

| Caso | Modo |
|---|---|
| Fondo nuevo que voy a auditar manualmente al final | Cowork (calidad alta, 0€) |
| Re-run rutinario tras nueva carta trimestral | Python con cache (€0.01-0.05) |
| Batch overnight de >20 fondos | Python (rate limit Max no aguanta) |
| Solo regenerar una sección que borré | Cowork con argumento "regenera sección X" |
| Fondo problemático que ha fallado quality 3 veces | Cowork (mejor reasoning Opus) |

## 7. Métricas a vigilar tras la migración

Crea un widget o un script que compare mes a mes:
- Coste API total (cost_log.jsonl) → debería bajar 70-90% si la mayoría de fondos van vía Cowork.
- Calidad: número de fallos en `dashboard_quality_agent` por fondo Cowork vs fondo Python.
- Anti-invención flagged: cuántos issues catch el subagente Sonnet vs los que pasaban silenciosamente antes.

Si la calidad Cowork se degrada visiblemente, vuelves al modo Python con el flag por defecto. La skill no quema puentes.
