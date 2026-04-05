"""
Configuración pytest para el proyecto fund-analyzer.
Añade la raíz del proyecto al PYTHONPATH para que los imports de tools/ funcionen.
"""
import sys
from pathlib import Path

# Raíz del proyecto (un nivel arriba de tests/)
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
