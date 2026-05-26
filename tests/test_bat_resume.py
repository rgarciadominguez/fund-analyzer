"""
Tests N5 (branch v2-cowork, 2026-05-20): modo `--resume` de analizar_fondo.bat.

Estrategia:
  - Tests estáticos (parse del .bat): comprueban que las features de resume
    están presentes en el bat sin ejecutar nada — robusto contra el entorno.
  - Tests de ejecución del fast-fail: invocan `cmd /c analizar_fondo.bat ZZ
    --resume` con un ISIN inexistente o sin prep files y validan que sale
    con exit code 3 sin entrar al pipeline.

Los tests de ejecución solo se activan en Windows (la pipeline está en .bat).
"""
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BAT_PATH = ROOT / "analizar_fondo.bat"
IS_WINDOWS = sys.platform == "win32"
skip_if_not_windows = pytest.mark.skipif(
    not IS_WINDOWS, reason="analizar_fondo.bat solo se ejecuta en Windows"
)


# ════════════════════════════════════════════════════════════════════
# Static analysis — el bat contiene las features de resume
# ════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module")
def bat_content() -> str:
    return BAT_PATH.read_text(encoding="utf-8")


def test_bat_documents_resume_flag(bat_content):
    """La cabecera del bat debe documentar la sintaxis con --resume."""
    assert "--resume" in bat_content
    # La línea de uso refleja la opción
    assert "[--resume]" in bat_content


def test_bat_parses_resume_flag_in_args(bat_content):
    """El bat detecta --resume en cualquiera de las posiciones 2-4."""
    # Soporta resume en posiciones 2, 3 o 4 (combinable con --allow-api-fallback)
    assert 'if "%2"=="--resume"' in bat_content
    assert 'if "%3"=="--resume"' in bat_content
    assert "set RESUME_MODE=1" in bat_content


def test_bat_has_critical_files_check(bat_content):
    """En modo resume, el bat debe abortar (exit 3) si faltan archivos críticos."""
    assert "data\\funds\\%ISIN%" in bat_content
    # mensaje de error específico y exit code 3
    assert "[ERROR] --resume pedido pero" in bat_content
    assert "exit /b 3" in bat_content
    # check de HAS_ANY_PREP (al menos cnmv_data.json o intl_data.json)
    assert "HAS_ANY_PREP" in bat_content
    assert "cnmv_data.json" in bat_content
    assert "intl_data.json" in bat_content


def test_bat_skip_prep_when_all_four_outputs_present(bat_content):
    """Paso 1 (prep) tiene skip-guard que requiere los 4 archivos."""
    assert "SKIP_PREP" in bat_content
    # los 4 archivos en sucesión de existencia
    assert "cnmv_data.json" in bat_content
    assert "intl_data.json" in bat_content
    assert "manager_profile.json" in bat_content
    assert "letters_data.json" in bat_content
    # mensaje de skip explícito
    assert "[RESUME-SKIP] prep" in bat_content


def test_bat_skip_each_skill_step(bat_content):
    """Pasos 2-5 (skills) tienen sus respectivos SKIP_* guards."""
    for var in ("SKIP_EXTRACT", "SKIP_MGRDEEP", "SKIP_LETTERS", "SKIP_ANALYST"):
        assert var in bat_content, f"falta variable {var}"
    # mensajes de skip explícitos
    assert "[RESUME-SKIP] extract-pdfs-cowork" in bat_content
    assert "[RESUME-SKIP] manager-deep-cowork" in bat_content
    assert "[RESUME-SKIP] letters-extract-cowork" in bat_content
    assert "[RESUME-SKIP] analyst-cowork" in bat_content


def test_bat_consume_and_sync_always_run(bat_content):
    """Paso 6 (consume-all-cowork) y 7 (sync supabase) NUNCA llevan
    skip-guard de resume — siempre se ejecutan."""
    # Conteo de SKIP_ — debe ser solo para los 4 skills + prep, no para
    # consume-all-cowork ni sync.
    skip_vars = {
        "SKIP_PREP", "SKIP_EXTRACT", "SKIP_MGRDEEP",
        "SKIP_LETTERS", "SKIP_ANALYST",
    }
    # Buscar otras variables SKIP_ adicionales: ninguna debe existir
    import re
    found = set(re.findall(r"SKIP_[A-Z_]+", bat_content))
    extra = found - skip_vars
    assert not extra, f"SKIP_* extras inesperados (consume/sync deben correr siempre): {extra}"


# ════════════════════════════════════════════════════════════════════
# Execution — fast-fail en --resume sin prep
# ════════════════════════════════════════════════════════════════════


def _run_bat(args: list[str], cwd: Path) -> subprocess.CompletedProcess:
    """Ejecuta analizar_fondo.bat con args y devuelve el resultado.
    Usa la copia REAL del bat (sin sandbox) — los tests fast-fail no llegan a
    invocar python/claude porque salen antes en el pre-flight check."""
    cmd = ["cmd.exe", "/c", str(BAT_PATH)] + args
    return subprocess.run(
        cmd, cwd=str(cwd),
        capture_output=True, text=True, timeout=20,
    )


@skip_if_not_windows
def test_resume_with_missing_fund_dir_exits_3(tmp_path):
    """`--resume` con fund_dir inexistente → exit 3 con mensaje claro."""
    # Crear estructura mínima vacía (data/funds/ pero no data/funds/<ISIN>)
    (tmp_path / "data" / "funds").mkdir(parents=True)
    result = _run_bat(["ZZ_TESTONLY_NOTEXIST", "--resume"], cwd=tmp_path)
    assert result.returncode == 3, (
        f"esperaba exit 3, obtuve {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    out = result.stdout + result.stderr
    assert "--resume pedido pero" in out
    assert "no existe" in out


@skip_if_not_windows
def test_resume_with_empty_fund_dir_exits_3(tmp_path):
    """`--resume` con fund_dir SIN cnmv ni intl → exit 3 con mensaje claro."""
    fund_dir = tmp_path / "data" / "funds" / "ZZ_TESTONLY_EMPTY"
    fund_dir.mkdir(parents=True)
    # Un archivo cualquiera que no es prep
    (fund_dir / "config.json").write_text("{}", encoding="utf-8")
    result = _run_bat(["ZZ_TESTONLY_EMPTY", "--resume"], cwd=tmp_path)
    assert result.returncode == 3, (
        f"esperaba exit 3, obtuve {result.returncode}\n"
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    out = result.stdout + result.stderr
    assert "faltan archivos criticos" in out


@skip_if_not_windows
def test_no_args_shows_usage_with_resume_option(tmp_path):
    """Bat sin args → exit 1 + mensaje de uso menciona --resume."""
    result = _run_bat([], cwd=tmp_path)
    assert result.returncode == 1
    out = result.stdout + result.stderr
    assert "Uso:" in out
    assert "[--resume]" in out


@skip_if_not_windows
def test_resume_with_prep_present_passes_preflight(tmp_path):
    """`--resume` con cnmv_data.json presente pasa el pre-flight (no exit 3).
    Va a fallar después porque PATH no tiene python/claude reales en tmp,
    pero el pre-flight check al menos no aborta. Comprobamos que el exit
    NO es 3 (pre-flight passed) — puede ser 1 (python fail) o 2 (claude
    no encontrado) según orden de comprobaciones."""
    fund_dir = tmp_path / "data" / "funds" / "ZZ_TESTONLY_HASPREP"
    fund_dir.mkdir(parents=True)
    (fund_dir / "cnmv_data.json").write_text("{}", encoding="utf-8")
    # Logs dir
    (tmp_path / "logs").mkdir()
    # Sin python/claude reales en este cwd, el bat fallará en el primer
    # python -m que sea necesario. Pero el pre-flight check de --resume
    # debe haber pasado.
    result = _run_bat(["ZZ_TESTONLY_HASPREP", "--resume"], cwd=tmp_path)
    assert result.returncode != 3, (
        f"pre-flight de --resume debió pasar (cnmv_data.json existe), "
        f"pero salió con 3.\nstdout={result.stdout}\nstderr={result.stderr}"
    )
    # Y el mensaje de aviso "saltare pasos" debe haber aparecido
    out = result.stdout + result.stderr
    assert "modo resume activo" in out


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
