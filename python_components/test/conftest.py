"""
Pytest configuration for RiskBeacon.

Why this exists:
- Some tools (notably mutation testing tools that copy the repo into a sandbox)
  may execute tests with a different working directory / sys.path layout.
- Our tests import project modules via top-level packages like `services.*` and `backend.*`.

This file ensures the `python_components/` directory is on `sys.path` so imports are stable
across local runs, CI, and mutation-testing sandboxes.
"""

from __future__ import annotations

import os
import sys


def _ensure_project_root_on_syspath() -> None:
    # `test/` lives under `python_components/test/` → parent is `python_components/`
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


_ensure_project_root_on_syspath()


