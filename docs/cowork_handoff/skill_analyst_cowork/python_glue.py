"""
python_glue.py — Snippet listo para integrar en agents/orchestrator.py

Añade soporte para los flags --prep-only y --consume-cowork al pipeline
existente, sin romper el flujo Python actual.

Cómo aplicarlo:
1. Localiza la función argparse del orchestrator (busca `parser = argparse.ArgumentParser`).
2. Añade los dos add_argument de la sección 1 de este fichero.
3. Localiza el método run() o main() del orchestrator.
4. Inserta el bloque de la sección 2 ANTES de la llamada a self._step_4_analyst().
5. Inserta el bloque de la sección 3 al INICIO del método run() (después del lock).
6. Añade el método _consume_cowork_analyst de la sección 4 a la clase.
7. Si los pasos 5-8 (validation+meta+quality+publication+dashboard) no están ya
   en un método separado, refactorízalos a _run_validation_and_quality
   (sección 5).

Después: ejecuta los tests existentes (test_es_no_regression, test_int_no_regression)
para confirmar que el flujo original sigue funcionando sin flags.
"""

# =============================================================================
# Sección 1 — argparse: añadir 2 flags
# =============================================================================

# Añade dentro del parser principal:
"""
parser.add_argument(
    "--prep-only",
    action="store_true",
    help="Ejecuta toda la prep determinista (CNMV o INT, manager, letters, "
         "readings, sources) pero salta analyst_agent. Deja JSONs listos para "
         "que la skill 'analyst-cowork' los consuma desde Claude Max."
)
parser.add_argument(
    "--consume-cowork",
    action="store_true",
    help="Salta toda la prep. Lee data/funds/{ISIN}/analyst_synthesis_cowork.json "
         "(producido por la skill), lo integra en output.json marcando los paths "
         "como _manual_edits, y corre validation + quality + dashboard."
)
"""


# =============================================================================
# Sección 2 — corte para --prep-only (insertar antes de _step_4_analyst)
# =============================================================================
"""
if getattr(self.args, 'prep_only', False):
    self._log('INFO', '--prep-only: deteniendo antes de analyst_agent')
    self._log('INFO', f'Inputs listos en data/funds/{self.isin}/:')
    for f in ['cnmv_data.json', 'intl_data.json', 'manager_profile.json',
              'letters_data.json', 'readings.json', 'sources.json']:
        path = self.fund_dir / f
        if path.exists():
            size_kb = path.stat().st_size / 1024
            self._log('INFO', f'  {f} ({size_kb:.1f} KB)')
    self._log('INFO', '')
    self._log('INFO', 'Próximo paso:')
    self._log('INFO', '  1. Abre Cowork o Claude Code en esta carpeta')
    self._log('INFO', f'  2. Di: "analyst cowork {self.isin}"')
    self._log('INFO', f'  3. Tras la skill: python -m agents.orchestrator --isin {self.isin} --consume-cowork')
    return  # exit early — no analyst, no quality, no dashboard
"""


# =============================================================================
# Sección 3 — entrada para --consume-cowork (insertar al inicio de run())
# =============================================================================
"""
if getattr(self.args, 'consume_cowork', False):
    cowork_json = self.fund_dir / 'analyst_synthesis_cowork.json'
    if not cowork_json.exists():
        self._log('ERROR', f'No existe {cowork_json}.')
        self._log('ERROR', 'Ejecuta primero la skill analyst-cowork en Cowork/Claude Code.')
        sys.exit(1)
    self._log('INFO', f'--consume-cowork: integrando {cowork_json}')
    self._consume_cowork_analyst(cowork_json)
    self._run_validation_and_quality()
    self._step_8_publication_calendar()
    self._generate_dashboard()
    return
"""


# =============================================================================
# Sección 4 — método _consume_cowork_analyst (añadir a la clase)
# =============================================================================

def _consume_cowork_analyst(self, cowork_json):
    """Integra analyst_synthesis_cowork.json (producido por la skill) en output.json.

    - Verifica el sha256 de los inputs vs los actuales (drift detection).
    - Sobrescribe analyst_synthesis.* con el de la skill.
    - Marca los 8 paths como _manual_edits para preservarlos en futuras runs Python.
    - Registra metadata del run en _meta.cowork_runs[].
    """
    import json
    import hashlib
    from pathlib import Path
    from tools.output_merger import save_output, mark_manual_edit

    cowork_data = json.loads(cowork_json.read_text(encoding="utf-8"))

    # Drift detection sobre los inputs
    expected_hashes = cowork_data.get("_meta", {}).get("input_files_hash", {})
    drift = []
    for fname, expected in expected_hashes.items():
        path = self.fund_dir / fname
        if not path.exists():
            drift.append(f"{fname}: no existe ahora")
            continue
        actual_hex = hashlib.sha256(path.read_bytes()).hexdigest()
        expected_hex = expected.replace("sha256:", "") if expected.startswith("sha256:") else expected
        if expected_hex and expected_hex != actual_hex:
            drift.append(f"{fname}: cambió desde la skill")
    if drift:
        self._log("WARN", "Drift en inputs detectado:")
        for d in drift:
            self._log("WARN", f"  - {d}")
        self._log("WARN", "El analyst de Cowork puede estar desactualizado vs los datos crudos actuales.")

    # Carga output.json actual
    output_path = self.fund_dir / "output.json"
    if output_path.exists():
        output_data = json.loads(output_path.read_text(encoding="utf-8"))
    else:
        output_data = {"isin": self.isin}

    # Reemplaza analyst_synthesis con el de la skill
    output_data["analyst_synthesis"] = cowork_data["analyst_synthesis"]

    # Marca cada sección como _manual_edits para que futuras runs Python no la pisen
    for sec in cowork_data["analyst_synthesis"].keys():
        mark_manual_edit(output_data, f"analyst_synthesis.{sec}")

    # Registra metadata
    cowork_meta = cowork_data.get("_meta", {})
    output_data.setdefault("_meta", {}).setdefault("cowork_runs", []).append({
        "ts": cowork_meta.get("generated"),
        "main_model": cowork_meta.get("main_model"),
        "audit_model": cowork_meta.get("audit_model"),
        "anti_invencion_flagged_count": len(cowork_meta.get("anti_invencion_flagged", [])),
        "audit_iterations": cowork_meta.get("audit_iterations", 0),
        "input_drift": drift,
    })

    save_output(self.isin, output_data)
    self._log("INFO", f"Analyst de Cowork integrado. {len(cowork_data['analyst_synthesis'])} secciones.")
    if cowork_meta.get("anti_invencion_flagged"):
        self._log("WARN", f"Anti-invención: {len(cowork_meta['anti_invencion_flagged'])} flags residuales — revísalos en _meta.cowork_runs[-1]")


# =============================================================================
# Sección 5 — método _run_validation_and_quality (extraer si no existe ya)
# =============================================================================

def _run_validation_and_quality(self):
    """Pasos 5-7 del pipeline original, extraídos para que --consume-cowork
    pueda invocarlos sin re-ejecutar prep ni analyst.

    Si en el orchestrator actual estos pasos están inline en run(), basta con
    encapsularlos en este método y llamar a self._run_validation_and_quality()
    desde el flujo normal y desde --consume-cowork.
    """
    self._step_5_validation()
    self._step_6_meta()
    self._step_7_quality_loop()
