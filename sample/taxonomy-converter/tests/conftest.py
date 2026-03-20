"""Configure test paths for taxonomy-converter tests."""

from __future__ import annotations

import sys
from pathlib import Path

# Add sample module and tests directories to sys.path
_sample_root = Path(__file__).resolve().parent.parent
_tests_dir = _sample_root / "tests"

for p in (_sample_root, _tests_dir):
    s = str(p)
    if s not in sys.path:
        sys.path.insert(0, s)
