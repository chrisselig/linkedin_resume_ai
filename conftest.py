"""Root conftest — adds project root to sys.path before any test collection."""

import sys
from pathlib import Path

_ROOT = str(Path(__file__).parent)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
