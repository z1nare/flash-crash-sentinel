from __future__ import annotations

from typing import Any, Optional, List, Tuple


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
    try:
        from great_expectations.exceptions import DataContextError  # type: ignore
    except Exception:  # pragma: no cover
        DataContextError = Exception  # type: ignore[misc,assignment]

    errors: List[Tuple[str, Exception]] = []

    # 1) Prefer PandasDataset (legacy GE). Many newer GE versions removed this module.
    try:
        try:
            from great_expectations.dataset import PandasDataset  # type: ignore
        except Exception:
            from great_expectations.dataset.pandas_dataset import PandasDataset  # type: ignore

        ds = PandasDataset(df)
        try:
            if hasattr(ds, "expectation_suite") and hasattr(ds.expectation_suite, "expectation_suite_name"):
                ds.expectation_suite.expectation_suite_name = suite_name
        except Exception:
            pass
        return ds
    except Exception as e:
        errors.append(("PandasDataset", e))

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

    # 3) DataContext RuntimeBatchRequest path (modern GE).
    # Key gotcha: runtime data connector name differs by GE version.
    try:
        context = ge.get_context(mode="ephemeral")

        datasource_name = "rb_runtime"
        try:
            context.sources.add_pandas(name=datasource_name)
        except Exception:
            pass

        # Try common runtime connector names across GE versions.
        connector_candidates = [
            "runtime_connector",
            "default_runtime_data_connector_name",
            "default_runtime_data_connector",
            "default_runtime_connector",
        ]

        def _ensure_suite_exists() -> None:
            # Suite management APIs vary.
            try:
                if hasattr(context, "add_or_update_expectation_suite"):
                    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
                    return
            except Exception:
                pass
            try:
                if hasattr(context, "add_expectation_suite"):
                    context.add_expectation_suite(expectation_suite_name=suite_name)
                    return
            except Exception:
                pass
            # Newer GE: suite factory (context.suites.add)
            try:
                if hasattr(context, "suites") and hasattr(context.suites, "add"):
                    try:
                        from great_expectations.core.expectation_suite import ExpectationSuite  # type: ignore
                    except Exception:
                        from great_expectations.core import ExpectationSuite  # type: ignore
                    context.suites.add(ExpectationSuite(expectation_suite_name=suite_name))
            except Exception:
                pass

        last_exc: Optional[Exception] = None
        for connector_name in connector_candidates:
            try:
                batch_request = ge.core.batch.RuntimeBatchRequest(
                    datasource_name=datasource_name,
                    data_connector_name=connector_name,
                    data_asset_name="rb_df",
                    runtime_parameters={"batch_data": df},
                    batch_identifiers={"default_identifier_name": "default"},
                )

                # Preferred: ask GE to create suite implicitly.
                try:
                    return context.get_validator(
                        batch_request=batch_request,
                        expectation_suite_name=suite_name,
                        create_expectation_suite=True,
                    )
                except TypeError:
                    # Older/newer versions may not accept kwarg.
                    _ensure_suite_exists()
                    return context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
                except DataContextError:
                    _ensure_suite_exists()
                    return context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
            except Exception as e:
                last_exc = e
                errors.append((f"DataContext connector={connector_name}", e))
                continue

        if last_exc is not None:
            raise last_exc
    except Exception as e:
        errors.append(("DataContext", e))

    raise RuntimeError(
        "Could not construct a Great Expectations validator from DataFrame. "
        "Tried: "
        + ", ".join([name for name, _ in errors])
        + ". Last error: "
        + (repr(errors[-1][1]) if errors else "unknown")
    )


