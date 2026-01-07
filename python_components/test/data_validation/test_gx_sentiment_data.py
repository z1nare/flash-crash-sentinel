import os

import pandas as pd
import pytest


pytest.importorskip("great_expectations")
import great_expectations as ge  

from test.data_validation.gx_suites import apply_sentiment_suite 
from test.data_validation.gx_test_utils import build_validator_from_df

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

    suite_name = "rb_sentiment_data_suite"

    validator = build_validator_from_df(df, suite_name=suite_name)
    apply_sentiment_suite(validator)
    results = validator.validate()

    assert results.success, f"GX sentiment validation failed: {results}"


