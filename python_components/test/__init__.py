"""
Test package marker.

Why this exists:
- Our repository uses a top-level folder named `test/`.
- Python also ships with a standard library package named `test`.
- Without an `__init__.py`, `from test...` imports can resolve to the stdlib package instead of our repo folder,
  causing CI-only import failures.
"""


