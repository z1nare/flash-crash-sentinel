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

        # --- Create or locate a pandas datasource (Fluent API) ---
        datasource_name = "rb_runtime"
        datasource = None
        try:
            # GE 0.18+ typically exposes fluent sources
            if hasattr(context, "sources") and hasattr(context.sources, "add_pandas"):
                try:
                    datasource = context.sources.add_pandas(name=datasource_name)  # type: ignore[attr-defined]
                except TypeError:
                    # Some versions don't accept keyword
                    datasource = context.sources.add_pandas(datasource_name)  # type: ignore[attr-defined]
                # Use the actual name GE assigned
                if hasattr(datasource, "name"):
                    datasource_name = str(datasource.name)
        except Exception as e:
            errors.append(("DataContext add_pandas", e))
            datasource = None

        # If fluent datasource couldn't be created, try to discover an existing datasource name.
        if datasource is None:
            try:
                if hasattr(context, "list_datasources"):
                    ds_list = context.list_datasources()  # type: ignore[call-arg]
                    # list_datasources can be list[dict] or list[str] depending on GE version
                    if isinstance(ds_list, list) and ds_list:
                        if isinstance(ds_list[0], dict) and "name" in ds_list[0]:
                            datasource_name = str(ds_list[0]["name"])
                        elif isinstance(ds_list[0], str):
                            datasource_name = str(ds_list[0])
            except Exception as e:
                errors.append(("DataContext list_datasources", e))

        # Try common runtime connector names across GE versions.
        connector_candidates = [
            "runtime_connector",
            "default_runtime_data_connector_name",
            "default_runtime_data_connector",
            "default_runtime_connector",
        ]

        def _suite_exists() -> bool:
            try:
                if hasattr(context, "suites") and hasattr(context.suites, "get"):
                    context.suites.get(name=suite_name)  # type: ignore[arg-type]
                    return True
            except Exception:
                pass
            try:
                if hasattr(context, "get_expectation_suite"):
                    context.get_expectation_suite(expectation_suite_name=suite_name)  # type: ignore[arg-type]
                    return True
            except Exception:
                pass
            return False

        def _ensure_suite_exists() -> bool:
            """
            Best-effort suite creation across GE versions.
            Returns True iff suite can be resolved afterward.
            """
            if _suite_exists():
                return True

            # 1) Classic DataContext methods
            try:
                if hasattr(context, "add_or_update_expectation_suite"):
                    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
                    return _suite_exists()
            except Exception:
                pass
            try:
                if hasattr(context, "add_expectation_suite"):
                    context.add_expectation_suite(expectation_suite_name=suite_name)
                    return _suite_exists()
            except Exception:
                pass

            # 2) Suite factory APIs (newer GE)
            if hasattr(context, "suites"):
                suites = context.suites

                for attempt in (
                    lambda: suites.add(name=suite_name),  # type: ignore[misc]
                    lambda: suites.add(expectation_suite_name=suite_name),  # type: ignore[misc]
                    lambda: suites.add(suite_name),  # type: ignore[misc]
                ):
                    try:
                        attempt()
                        if _suite_exists():
                            return True
                    except Exception:
                        continue

                # Try providing an ExpectationSuite instance with different ctor signatures.
                ExpectationSuite = None
                try:
                    from great_expectations.core.expectation_suite import ExpectationSuite as _ES  # type: ignore
                    ExpectationSuite = _ES
                except Exception:
                    try:
                        from great_expectations.core import ExpectationSuite as _ES  # type: ignore
                        ExpectationSuite = _ES
                    except Exception:
                        ExpectationSuite = None

                if ExpectationSuite is not None:
                    for suite_ctor in (
                        lambda: ExpectationSuite(expectation_suite_name=suite_name),
                        lambda: ExpectationSuite(name=suite_name),
                        lambda: ExpectationSuite(suite_name),
                    ):
                        try:
                            suites.add(suite_ctor())  # type: ignore[misc]
                            if _suite_exists():
                                return True
                        except Exception:
                            continue

            return _suite_exists()

        last_exc: Optional[Exception] = None
        for connector_name in connector_candidates:
            try:
                # Preferred: if we have a fluent datasource, build the batch request from a DataFrame asset.
                batch_request = None
                if datasource is not None and hasattr(datasource, "add_dataframe_asset"):
                    try:
                        asset = datasource.add_dataframe_asset(name="rb_df_asset")  # type: ignore[attr-defined]
                        if hasattr(asset, "build_batch_request"):
                            try:
                                batch_request = asset.build_batch_request(dataframe=df)  # type: ignore[misc]
                            except TypeError:
                                batch_request = asset.build_batch_request(batch_parameters={"dataframe": df})  # type: ignore[misc]
                    except Exception as e:
                        errors.append(("DataContext add_dataframe_asset", e))
                        batch_request = None

                # Fallback: use RuntimeBatchRequest with guessed connector name.
                if batch_request is None:
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
                    ok = _ensure_suite_exists()
                    if ok:
                        return context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
                    # Last resort: create a validator without a named suite (if supported),
                    # then attach suite name for readability.
                    try:
                        v = context.get_validator(batch_request=batch_request, create_expectation_suite=True)  # type: ignore[call-arg]
                        try:
                            v.expectation_suite.expectation_suite_name = suite_name  # type: ignore[attr-defined]
                        except Exception:
                            pass
                        return v
                    except Exception as e:
                        raise e
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


