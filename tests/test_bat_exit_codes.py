"""
Tests N1 (branch v2-cowork, 2026-05-20): exit codes 0/5/10 de analizar_fondo.bat.

  - 0  : todos los pasos OK
  - 5  : non-critical falló (output.json + sync ok pero algún skill auxiliar)
  - 10 : critical (output.json ausente, o consume-all-cowork falló, o sync falló)

Estrategia:
  - Tests estáticos verifican la lógica de clasificación en el .bat.
  - Tests E2E ejecutan el bat con --resume (skill steps saltados) + stubs
    `python.bat`/`claude.bat` en PATH para forzar cada combinación.
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
# Static analysis — clasificación 0/5/10
# ════════════════════════════════════════════════════════════════════


@pytest.fixture(scope="module")
def bat_content() -> str:
    return BAT_PATH.read_text(encoding="utf-8")


def test_bat_has_three_exit_code_branches(bat_content):
    assert "set EXIT_CODE=10" in bat_content
    assert "set EXIT_CODE=5" in bat_content
    assert "set EXIT_CODE=0" in bat_content


def test_critical_fails_includes_consume_and_sync(bat_content):
    """Los pasos críticos que escalan a exit 10 son: output.json missing,
    consume-all-cowork failed, sync-supabase failed."""
    assert "CRITICAL_FAILS" in bat_content
    assert "consume-all-cowork" in bat_content
    assert "sync-supabase" in bat_content
    # output-missing es el marker que se añade cuando output.json no existe
    assert "output-missing" in bat_content


def test_exit_code_propagation_uses_endlocal(bat_content):
    """`endlocal & exit /b %EXIT_CODE%` para propagar el código fuera del
    setlocal scope (parse-time expansion)."""
    assert "endlocal & exit /b" in bat_content


# ════════════════════════════════════════════════════════════════════
# E2E execution — verifica cada exit code con stubs en PATH
# ════════════════════════════════════════════════════════════════════


ISIN = "ZZ_E2E_EXIT"


def _setup_resume_ready_fixture(tmp_path: Path, *, with_output: bool = True) -> None:
    """Crea fund_dir + logs + extracted/ con los outputs esperados, de modo que
    `--resume` saltea todos los skills y solo se ejecutan consume-all-cowork
    + sync-supabase (stubeados)."""
    fund_dir = tmp_path / "data" / "funds" / ISIN
    fund_dir.mkdir(parents=True)
    prep_files = [
        "cnmv_data.json", "intl_data.json",
        "manager_profile.json", "letters_data.json",
        "pending_manager_deep.json",
        "analyst_synthesis_cowork.json",
    ]
    for f in prep_files:
        (fund_dir / f).write_text("{}", encoding="utf-8")
    if with_output:
        (fund_dir / "output.json").write_text("{}", encoding="utf-8")
    (fund_dir / "extracted").mkdir()

    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    for skill in (
        "skill_extract_pdfs", "skill_manager_deep",
        "skill_letters_extract", "skill_analyst",
    ):
        (logs_dir / f"{skill}_{ISIN}.log").write_text("ok", encoding="utf-8")


def _make_stubs(
    tmp_path: Path, *,
    python_consume_extracted_exit: int = 0,
    python_consume_all_exit: int = 0,
    python_sync_exit: int = 0,
    python_default_exit: int = 0,
) -> Path:
    """Genera stubs python.bat/claude.bat en tmp_path/bin que devuelven
    distintos exit codes según los argumentos. PATH-prependear el bin."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    # python.bat: clasifica por args
    py_stub = f"""@echo off
echo %* | findstr /C:"consume-all-cowork" >nul && exit /b {python_consume_all_exit}
echo %* | findstr /C:"consume-extracted" >nul && exit /b {python_consume_extracted_exit}
echo %* | findstr /C:"sync_to_supabase" >nul && exit /b {python_sync_exit}
echo %* | findstr /C:"bundle_exporter" >nul && exit /b {python_default_exit}
echo %* | findstr /C:"auto_gen_manager_manifest" >nul && exit /b 0
exit /b {python_default_exit}
"""
    (bin_dir / "python.bat").write_text(py_stub, encoding="utf-8")
    # claude.bat: siempre OK (en resume mode no se llama, pero `where claude`
    # comprueba que existe)
    (bin_dir / "claude.bat").write_text(
        "@echo off\nexit /b 0\n", encoding="utf-8",
    )
    return bin_dir


def _run_bat_with_stubs(
    tmp_path: Path, args: list[str], bin_dir: Path,
) -> subprocess.CompletedProcess:
    env = os.environ.copy()
    env["PATH"] = str(bin_dir) + os.pathsep + env.get("PATH", "")
    cmd = ["cmd.exe", "/c", str(BAT_PATH)] + args
    return subprocess.run(
        cmd, cwd=str(tmp_path), env=env,
        capture_output=True, text=True, timeout=30,
    )


@skip_if_not_windows
def test_exit_code_0_all_ok(tmp_path):
    """Resume mode + stubs all-zero + output.json existe → exit 0."""
    _setup_resume_ready_fixture(tmp_path, with_output=True)
    bin_dir = _make_stubs(tmp_path)
    result = _run_bat_with_stubs(tmp_path, [ISIN, "--resume"], bin_dir)
    assert result.returncode == 0, (
        f"esperaba 0, obtuve {result.returncode}\n"
        f"stdout={result.stdout[-2000:]}"
    )
    assert "Listo -- todos los pasos OK" in result.stdout


@skip_if_not_windows
def test_exit_code_10_no_output_json(tmp_path):
    """Sin output.json en disco al final → exit 10 critical."""
    _setup_resume_ready_fixture(tmp_path, with_output=False)
    bin_dir = _make_stubs(tmp_path)
    result = _run_bat_with_stubs(tmp_path, [ISIN, "--resume"], bin_dir)
    assert result.returncode == 10, (
        f"esperaba 10, obtuve {result.returncode}\n"
        f"stdout={result.stdout[-2000:]}"
    )
    assert "output-missing" in result.stdout
    assert "FAIL critico" in result.stdout


@skip_if_not_windows
def test_exit_code_10_consume_all_fails(tmp_path):
    """consume-all-cowork falla → exit 10 critical (incluso con output.json)."""
    _setup_resume_ready_fixture(tmp_path, with_output=True)
    bin_dir = _make_stubs(tmp_path, python_consume_all_exit=1)
    result = _run_bat_with_stubs(tmp_path, [ISIN, "--resume"], bin_dir)
    assert result.returncode == 10, (
        f"esperaba 10, obtuve {result.returncode}\n"
        f"stdout={result.stdout[-2000:]}"
    )
    assert "consume-all-cowork" in result.stdout


@skip_if_not_windows
def test_exit_code_10_sync_fails(tmp_path):
    """sync-supabase falla → exit 10 critical."""
    _setup_resume_ready_fixture(tmp_path, with_output=True)
    bin_dir = _make_stubs(tmp_path, python_sync_exit=1)
    result = _run_bat_with_stubs(tmp_path, [ISIN, "--resume"], bin_dir)
    assert result.returncode == 10, (
        f"esperaba 10, obtuve {result.returncode}\n"
        f"stdout={result.stdout[-2000:]}"
    )
    assert "sync-supabase" in result.stdout


@skip_if_not_windows
def test_exit_code_5_non_critical_failure(tmp_path):
    """Solo consume-extracted falla (non-critical), output.json existe,
    consume-all-cowork + sync OK → exit 5."""
    _setup_resume_ready_fixture(tmp_path, with_output=True)
    bin_dir = _make_stubs(tmp_path, python_consume_extracted_exit=1)
    result = _run_bat_with_stubs(tmp_path, [ISIN, "--resume"], bin_dir)
    assert result.returncode == 5, (
        f"esperaba 5, obtuve {result.returncode}\n"
        f"stdout={result.stdout[-2000:]}"
    )
    assert "Listo CON AVISOS" in result.stdout
    assert "consume-extracted" in result.stdout


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
