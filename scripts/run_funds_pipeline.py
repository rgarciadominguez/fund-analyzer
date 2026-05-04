"""Pipeline serial: extractor → analyst → dashboard → quality para 1 o más fondos.
Evita problemas de PATH/race conditions usando subprocess con sys.executable.
"""
import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))

# Asegurar utf-8 en subprocess
ENV = dict(os.environ)
ENV["PYTHONIOENCODING"] = "utf-8"

FUNDS = [
    ("IE00B6T42S66", "Trojan Fund (Ireland) O EUR ACC", "Troy Asset Management"),
    ("LU1694789451", "DNCA Invest Alpha Bonds", "DNCA Investments"),
]


def run_step(label, cmd, log_path):
    print(f"  {label}...")
    t0 = time.time()
    with open(log_path, "w", encoding="utf-8") as f:
        result = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, env=ENV)
    elapsed = time.time() - t0
    print(f"    exit={result.returncode}  time={elapsed:.0f}s  log={log_path}")
    return result.returncode


def main():
    py = sys.executable
    for isin, nombre, gestora in FUNDS:
        print(f"\n==================== {isin} ({nombre}) ====================")
        fund_dir = ROOT / "data" / "funds" / isin
        fund_dir.mkdir(parents=True, exist_ok=True)

        # 1) Extractor
        rc = run_step(
            "EXTRACTOR",
            [py, "-m", "agents.intl_extractor_v2",
             "--isin", isin, "--nombre", nombre, "--gestora", gestora],
            fund_dir / "extractor_run.log",
        )
        intl_path = fund_dir / "intl_data.json"
        if not intl_path.exists() or intl_path.stat().st_size == 0:
            print(f"    !!! intl_data.json no existe o vacío — saltando resto del fondo")
            continue
        print(f"    intl_data.json size={intl_path.stat().st_size:,}")

        # 2) Analyst
        rc = run_step(
            "ANALYST",
            [py, "-m", "agents.analyst_agent", isin],
            fund_dir / "analyst_run.log",
        )
        out_path = fund_dir / "output.json"
        if out_path.exists():
            d = json.loads(out_path.read_text(encoding="utf-8"))
            n_pos = len(d.get("posiciones", {}).get("actuales", []))
            print(f"    output.json size={out_path.stat().st_size:,}  pos={n_pos}")

        # 3) Dashboard
        rc = run_step(
            "DASHBOARD",
            [py, "dashboard/generate_dashboard.py", isin],
            fund_dir / "dashgen.log",
        )
        html_path = ROOT / "dashboard" / f"fund-{isin}.html"
        if html_path.exists():
            print(f"    HTML size={html_path.stat().st_size:,}")

        # 4) Quality
        rc = run_step(
            "QUALITY",
            [py, "-m", "agents.dashboard_quality_agent", isin],
            fund_dir / "quality_run.log",
        )
        qlog = (fund_dir / "quality_run.log").read_text(encoding="utf-8", errors="ignore")
        n_fallos = qlog.count("\n  • ")
        print(f"    fallos quality: {n_fallos}")

    print("\n=== PIPELINE TERMINADO ===")
    # Chime sonoro
    try:
        subprocess.run(
            ["powershell", "-Command",
             "[console]::beep(523,180); [console]::beep(659,180); [console]::beep(784,300)"],
            timeout=5,
        )
    except Exception:
        pass
    # Flag
    (ROOT / "data" / ".pipelines_done").touch()


if __name__ == "__main__":
    main()
