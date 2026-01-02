import os

import pandas as pd
import pytest


pytest.importorskip("great_expectations")
import great_expectations as ge  # noqa: E402

from test.data_validation.gx_suites import apply_sentiment_suite  # noqa: E402

def test_gx_sentiment_data_batch_validation():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(base_dir, "dataInCsv", "articles_with_sentiment.csv")

    if not os.path.exists(path):
        pytest.skip("Sentiment file not found")

    # Load only a batch for speed (still a valid 'batch' for GX)
    df = pd.read_csv(path, low_memory=False, nrows=20000)

    context = ge.get_context(mode="ephemeral")
    suite_name = "rb_sentiment_data_suite"
    try:
        context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    except Exception:
        pass

    batch_request = ge.core.batch.RuntimeBatchRequest(
        datasource_name="rb_runtime",
        data_connector_name="runtime_connector",
        data_asset_name=os.path.basename(path),
        runtime_parameters={"batch_data": df},
        batch_identifiers={"default_identifier_name": "default"},
    )

    try:
        context.sources.add_pandas(name="rb_runtime")
    except Exception:
        pass

    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
    apply_sentiment_suite(validator)
    results = validator.validate()

    assert results.success, f"GX sentiment validation failed: {results}"


