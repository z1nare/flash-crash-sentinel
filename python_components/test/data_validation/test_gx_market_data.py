import os

import pandas as pd
import pytest


pytest.importorskip("great_expectations")
import great_expectations as ge  

from test.data_validation.gx_suites import apply_market_suite  

def _get_validator_create_suite(context, batch_request, suite_name: str):
    """
    Great Expectations API varies by version.
    Make suite creation explicit so ephemeral contexts don't error with:
    'ExpectationSuite with name ... was not found.'
    """
    try:
        return context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name,
            create_expectation_suite=True,
        )
    except TypeError:
        # Some versions don't accept create_expectation_suite kwarg
        try:
            context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
        except Exception:
            try:
                context.add_expectation_suite(expectation_suite_name=suite_name)
            except Exception:
                pass
        return context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)


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

    context = ge.get_context(mode="ephemeral")
    suite_name = "rb_market_data_suite"
    try:
        context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    except Exception:
        pass

    # Use runtime batch (Data Context + batch)
    # Build datasource (runtime)
    try:
        context.sources.add_pandas(name="rb_runtime")
    except Exception:
        # Older GE versions may already have it
        pass

    failures = []
    for f in csv_files:
        path = os.path.join(hist_dir, f)
        df = pd.read_csv(path, low_memory=False)
        # For robustness + speed: validate recent window and a small head window
        df_head = df.head(2000)
        df_tail = df.tail(2000)
        df_batch = pd.concat([df_head, df_tail], ignore_index=True)

        batch_request = ge.core.batch.RuntimeBatchRequest(
            datasource_name="rb_runtime",
            data_connector_name="runtime_connector",
            data_asset_name=os.path.basename(path),
            runtime_parameters={"batch_data": df_batch},
            batch_identifiers={"default_identifier_name": "default"},
        )

        validator = _get_validator_create_suite(context, batch_request, suite_name)
        apply_market_suite(validator)
        results = validator.validate()
        if not results.success:
            failures.append((f, results))

    assert not failures, f"GX market data validation failures: {[name for name, _ in failures]}"


