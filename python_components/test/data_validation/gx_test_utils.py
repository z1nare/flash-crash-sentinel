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

    # 1) Try DataContext RuntimeBatchRequest path (works on many GE versions)
    try:
        context = ge.get_context(mode="ephemeral")

        # Best-effort: add runtime datasource
        try:
            context.sources.add_pandas(name="rb_runtime")
        except Exception:
            pass

        # Best-effort: create suite (APIs vary)
        try:
            if hasattr(context, "add_or_update_expectation_suite"):
                context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
            elif hasattr(context, "add_expectation_suite"):
                context.add_expectation_suite(expectation_suite_name=suite_name)
            elif hasattr(context, "create_expectation_suite"):
                context.create_expectation_suite(expectation_suite_name=suite_name, overwrite_existing=True)
        except Exception:
            pass

        # RuntimeBatchRequest signature varies by version, but this is the most common form.
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
            # Fall through to other strategies
            pass
    except Exception:
        pass

    # 2) Try direct Validator construction (avoids DataContext suite store entirely)
    try:
        from great_expectations.execution_engine import PandasExecutionEngine  # type: ignore
        from great_expectations.validator.validator import Validator  # type: ignore

        suite: Optional[Any] = None
        try:
            from great_expectations.core.expectation_suite import ExpectationSuite  # type: ignore

            suite = ExpectationSuite(expectation_suite_name=suite_name)
        except Exception:
            try:
                from great_expectations.core import ExpectationSuite  # type: ignore

                suite = ExpectationSuite(expectation_suite_name=suite_name)
            except Exception:
                suite = None

        engine = PandasExecutionEngine()

        # Batch APIs vary too; try the simplest supported forms.
        try:
            return Validator(execution_engine=engine, batches=[{"data": df}], expectation_suite=suite)
        except Exception:
            return Validator(execution_engine=engine, batch_data=df, expectation_suite=suite)
    except Exception:
        pass

    # 3) Fallback: legacy convenience helper (if present)
    from_pandas = getattr(ge, "from_pandas", None)
    if callable(from_pandas):
        v = from_pandas(df)
        try:
            if hasattr(v, "expectation_suite") and hasattr(v.expectation_suite, "expectation_suite_name"):
                v.expectation_suite.expectation_suite_name = suite_name
        except Exception:
            pass
        return v

    raise RuntimeError(
        "Could not construct a Great Expectations validator from DataFrame. "
        "Your installed great_expectations version may be incompatible with this repo."
    )


