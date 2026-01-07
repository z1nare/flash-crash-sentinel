from __future__ import annotations

from typing import Any, Optional


def build_validator_from_df(df, suite_name: str):
    """
    Build a Great Expectations validator from an in-memory pandas DataFrame.

    Why:
    - Great Expectations has breaking API changes across versions.
    - DataContext suite stores can behave differently in ephemeral mode, causing:
      "ExpectationSuite with name ... was not found."

    Strategy:
    - Prefer a DataContext RuntimeBatchRequest (modern GE) if it works.
    - Fall back to constructing a Validator directly with a PandasExecutionEngine.
    - Fall back to `great_expectations.from_pandas(df)` if available.
    """
    import great_expectations as ge

    # 1) Prefer PandasDataset (stable: has expectation methods + validate()).
    # This avoids DataContext suite-store differences entirely.
    try:
        try:
            from great_expectations.dataset import PandasDataset  # type: ignore
        except Exception:
            from great_expectations.dataset.pandas_dataset import PandasDataset  # type: ignore

        ds = PandasDataset(df)
        # Best-effort: attach suite name for readability/debugging
        try:
            if hasattr(ds, "expectation_suite") and hasattr(ds.expectation_suite, "expectation_suite_name"):
                ds.expectation_suite.expectation_suite_name = suite_name
        except Exception:
            pass
        return ds
    except Exception:
        pass

    # 2) Try `ge.from_pandas(df)` if present (older GE convenience)
    from_pandas = getattr(ge, "from_pandas", None)
    if callable(from_pandas):
        v = from_pandas(df)
        try:
            if hasattr(v, "expectation_suite") and hasattr(v.expectation_suite, "expectation_suite_name"):
                v.expectation_suite.expectation_suite_name = suite_name
        except Exception:
            pass
        return v

    # 3) Try DataContext RuntimeBatchRequest path (modern GE). This is last because some
    # GE versions behave unexpectedly in ephemeral mode (suite store not writable).
    try:
        context = ge.get_context(mode="ephemeral")

        try:
            context.sources.add_pandas(name="rb_runtime")
        except Exception:
            pass

        batch_request = ge.core.batch.RuntimeBatchRequest(
            datasource_name="rb_runtime",
            data_connector_name="runtime_connector",
            data_asset_name="rb_df",
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "default"},
        )

        # Some versions accept create_expectation_suite=True, others ignore it.
        try:
            return context.get_validator(
                batch_request=batch_request,
                expectation_suite_name=suite_name,
                create_expectation_suite=True,
            )
        except TypeError:
            return context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
    except Exception:
        pass

    raise RuntimeError(
        "Could not construct a Great Expectations validator from DataFrame. "
        "Your installed great_expectations version may be incompatible with this repo."
    )


