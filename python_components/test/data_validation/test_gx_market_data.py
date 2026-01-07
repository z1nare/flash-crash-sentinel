import os

import pandas as pd
import pytest


pytest.importorskip("great_expectations")
import great_expectations as ge  

from test.data_validation.gx_suites import apply_market_suite  
from test.data_validation.gx_test_utils import build_validator_from_df  

def test_gx_market_data_batch_validation():
    """
    Validates that market CSVs satisfy basic schema/invariant expectations.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    hist_dir = os.path.join(base_dir, "historicalData")

    if not os.path.exists(hist_dir):
        pytest.skip("No historicalData/*.csv found to validate")

    # Validate all ticker CSVs present (bounded for speed per file)
    csv_files = [f for f in os.listdir(hist_dir) if f.lower().endswith(".csv")]
    if not csv_files:
        pytest.skip("No historicalData/*.csv found to validate")

    suite_name = "rb_market_data_suite"

    failures = []
    for f in csv_files:
        path = os.path.join(hist_dir, f)
        df = pd.read_csv(path, low_memory=False)
        # For robustness + speed: validate recent window and a small head window
        df_head = df.head(2000)
        df_tail = df.tail(2000)
        df_batch = pd.concat([df_head, df_tail], ignore_index=True)

        validator = build_validator_from_df(df_batch, suite_name=suite_name)
        apply_market_suite(validator)
        results = validator.validate()
        if not results.success:
            failures.append((f, results))

    assert not failures, f"GX market data validation failures: {[name for name, _ in failures]}"


