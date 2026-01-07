import os

import pandas as pd
import pytest


pytest.importorskip("great_expectations")
import great_expectations as ge  

from test.data_validation.gx_suites import apply_sentiment_suite 


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

def test_gx_sentiment_data_batch_validation():
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    path = os.path.join(base_dir, "dataInCsv", "articles_with_sentiment.csv")

    if not os.path.exists(path):
        pytest.skip("Sentiment file not found")

    # Load two batches (head + tail) for robustness, bounded for speed.
    first_chunk = None
    last_chunk = None
    for chunk in pd.read_csv(path, low_memory=False, chunksize=10000):
        if first_chunk is None:
            first_chunk = chunk
        last_chunk = chunk
    if first_chunk is None or last_chunk is None:
        pytest.skip("Sentiment file is empty")
    df = pd.concat([first_chunk, last_chunk], ignore_index=True)

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

    validator = _get_validator_create_suite(context, batch_request, suite_name)
    apply_sentiment_suite(validator)
    results = validator.validate()

    assert results.success, f"GX sentiment validation failed: {results}"


