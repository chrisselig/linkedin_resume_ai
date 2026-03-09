"""Pytest configuration — ensures project root and app/ are importable."""
import sys
from pathlib import Path

# Project root (contains app/, src/, tests/)
_ROOT = Path(__file__).parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
