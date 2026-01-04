"""
Convenience entrypoint to run data validation (GX) and export suites.

This is useful for coursework evidence generation:
- Run GX pytest tests
- Export human-readable suite JSON + markdown summary

Usage:
  python -m test.data_validation.run_data_validation
"""

from __future__ import annotations

import subprocess
import sys


def main() -> int:
    # Run only GX tests (fast subset)
    rc = subprocess.call([sys.executable, "-m", "pytest", "-q", "test/data_validation"])
    if rc != 0:
        return rc

    # Export suites (requires great_expectations installed)
    rc2 = subprocess.call([sys.executable, "scripts/export_gx_suites.py"])
    return rc2


if __name__ == "__main__":
    raise SystemExit(main())