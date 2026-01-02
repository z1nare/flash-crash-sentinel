import os

import pandas as pd
import pytest


pytest.importorskip("great_expectations")
import great_expectations as ge  # noqa: E402

from test.data_validation.gx_suites import apply_market_suite  # noqa: E402

def test_gx_market_data_batch_validation():
    """
    Validates that market CSVs satisfy basic schema/invariant expectations.
    """
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    hist_dir = os.path.join(base_dir, "historicalData")

    # Pick a representative ticker file present in repo
    candidates = ["NVDA.csv", "AMD.csv", "TSLA.csv", "SPY.csv"]
    path = None
    for f in candidates:
        p = os.path.join(hist_dir, f)
        if os.path.exists(p):
            path = p
            break

    if path is None:
        pytest.skip("No historicalData/*.csv found to validate")

    # Read as a batch (bounded for speed)
    df = pd.read_csv(path, low_memory=False)

    context = ge.get_context(mode="ephemeral")
    suite_name = "rb_market_data_suite"
    try:
        context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    except Exception:
        pass

    # Use runtime batch (Data Context + batch)
    batch_request = ge.core.batch.RuntimeBatchRequest(
        datasource_name="rb_runtime",
        data_connector_name="runtime_connector",
        data_asset_name=os.path.basename(path),
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default"},
    )

    # Build datasource (runtime)
    try:
        context.sources.add_pandas(name="rb_runtime")
    except Exception:
        # Older GE versions may already have it
        pass

    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
    apply_market_suite(validator)
    results = validator.validate()

    assert results.success, f"GX market data validation failed: {results}"


