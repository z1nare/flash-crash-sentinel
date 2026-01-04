# Test Strategy: RiskBeacon

**Author:** [Your Name]
**Project:** RiskBeacon - Real-Time Market Risk Monitoring System
**Date:** January 2026

## 1. Introduction
This document outlines the testing strategy for RiskBeacon, an institutional-grade risk monitoring system. The strategy moves beyond standard functional verification to include advanced techniques suitable for financial software, including **Property-Based Testing** for quantitative metrics, **Mutation Testing** for test suite robustness, and **Metamorphic Testing** for the Machine Learning components.

## 2. Testing Pyramid & Scope

### 2.1 Unit Testing (Foundational)
* **Scope:** Individual mathematical functions (VPIN, Yang-Zhang Volatility) and data parsing logic.
* **Tools:** `pytest`
* **Distinction Approach:**
    * [cite_start]**Math Verification:** Verify Equation (2) (VPIN) [cite: 85] [cite_start]and Equation (3) (Yang-Zhang) [cite: 87] against manual calculations in Excel/Spreadsheets to ensure implementation correctness.
    * **Boundary Analysis:** Test behavior when trade volume is zero (division by zero protection in VPIN formula).

### 2.2 Integration Testing (Mocking Strategy)
* **Scope:** Interaction between `FastAPI`, `ServiceController`, and the new `InteractiveBrokers` adapter.
* **Tools:** `pytest-mock`, `unittest.mock`
* **Strategy:**
    * [cite_start]**Dependency Injection:** As defined in the architecture[cite: 51], we utilize `RegimeModelInterface` and data provider interfaces.
    * **Mocking External Feeds:** We mock the Interactive Brokers API to simulate:
        * **Scenario A:** Standard market data stream (Happy Path).
        * **Scenario B:** Network disconnection/timeout (Resilience).
        * **Scenario C:** Malformed packets/Invalid Ticker symbols.
    * **Goal:** Ensure the system degrades gracefully without crashing the Dashboard when the data feed is interrupted.

### 2.3 System Testing (End-to-End)
* [cite_start]**Scope:** The full data pipeline from `POST /api/tick` to the Streamlit Dashboard update[cite: 63].
* **Tools:** `TestClient` (FastAPI), potentially Selenium/Playwright for Streamlit.

### 2.4 Test inventory (repo evidence pointers)
This section enumerates the **tests and evidence that already exist** in this repository, as well as the **planned tests** that are referenced in LO1 traceability.

#### Existing tests (already implemented)
* **Great Expectations (data validation, batch-based via Data Context):**
    * Market suite definition: `test/data_validation/gx_suites.py` (`apply_market_suite`)
    * Sentiment suite definition: `test/data_validation/gx_suites.py` (`apply_sentiment_suite`)
    * Market validation test (head+tail batches per ticker CSV): `test/data_validation/test_gx_market_data.py`
    * Sentiment validation test (first+last batch): `test/data_validation/test_gx_sentiment_data.py`
    * Convenience runner (runs GX tests + exports suites): `test/data_validation/run_data_validation.py`
    * Suite export (human-readable JSON + summary): `scripts/export_gx_suites.py`
* **MLOps (MLflow smoke + registry + signature):**
    * MLflow + sklearn model logging, signature, registry + reload: `test/mlops/test_mlflow_tracking.py`
* **Test runner configuration:**
    * pytest discovery config for `test/`: `pytest.ini`
    * Dev dependencies: `requirements-dev.txt`

#### Planned tests (mapped from LO1 requirements)
The following are planned tests and will be added incrementally (see LO1 traceability mapping in `docs/requirements_to_tests.md`):
* **Quant unit + property tests (Hypothesis):**
    * `test/unit_testing/Quant_services/test_vpin_invariants.py`
    * `test/unit_testing/Quant_services/test_vol_yang_zhang_invariants.py`
* **System/integration tests (FastAPI TestClient):**
    * `test/integration_testing/test_api_tick_persists_csv.py`
    * `test/integration_testing/test_api_plot_generation.py`
* **Performance tests:**
    * `test/integration_testing/test_perf_api_tick_latency.py` (RB-REQ-16)
    * `test/integration_testing/test_perf_plot_generation_time.py` (RB-REQ-17)
* **Backtesting / time-series evaluation tests:**
    * Strategy/backtest planning: `test/backtesting/backtest.py`
    * Time-series split/no look-ahead evidence (planned): `test/backtesting/test_time_series_split.py`

---

## 3. Instrumentation and testability changes (evidence)
This section lists **concrete code changes** made to improve testability and robustness (instrumentation in the broad sense: logging, defensive parsing, explicit diagnostics, and controllable behavior).

* **Plot pipeline robustness fix (data schema instrumentation):**
    * Fixed plot generation failure caused by duplicated columns (`VPIN` + `vpin`, `vol` + `volatility`) leading to non-1D column access errors.
    * Evidence: `services/plotService.py` (deduplicate normalized columns in `load_data`).
* **CSV normalization and recalculation tooling (diagnostic + repair instrumentation):**
    * Added a repeatable normalization/recalculation script for historical CSVs (used to repair corrupted/duplicated metric columns and recompute derived metrics).
    * Evidence: `normalize_historical_data.py`.
* **Interactive Brokers integration instrumentation:**
    * Added structured logging and explicit connection status surfaces so failures are diagnosable without breaking the dashboard.
    * Evidence:
        * `services/ib_client_service.py` (logs, connection info)
        * `controllers/ServiceController.py` (IB status + connect overrides)
        * `api/routes.py` (IB endpoints)
        * `frontend/dashboard.py` (IB connection settings UI)
        * Local troubleshooting tool: `ib_troubleshoot.py`
* **VPIN/regime pipeline stability improvements (testability of quantitative behavior):**
    * Improved VPIN bar-based classification to avoid saturation and to keep outputs meaningful for downstream regime detection.
    * Added a rule-based regime mode (`REGIME_MODE=rule`) for explainability and as a fallback when model assumptions fail.
    * Evidence:
        * `services/vpin_service.py`
        * `controllers/ServiceController.py`

---

## 4. Advanced Testing Techniques (Distinction Evidence)

### 4.1 Property-Based Testing (Hypothesis)
* **Rationale:** Financial metrics must adhere to mathematical invariants regardless of input data. Standard example-based tests are insufficient to cover the infinite state space of market prices.
* **Tool:** `hypothesis`
* **Implementation Plan:**
    * **Invariant 1 (Volatility):** For any sequence of High/Low/Open/Close prices generated, the Yang-Zhang volatility $\sigma^2_{YZ}$ must always be $\geq 0$.
    * [cite_start]**Invariant 2 (VPIN):** The VPIN metric must always fall within the range $[0, 1]$, regardless of buy/sell volume distribution[cite: 85].
    * **Invariant 3 (Crash Preservation):** If the input stream contains `NaN` or `Infinite` values, the system must handle them (raise specific error or filter) rather than returning a 500 Server Error.

### 4.2 Mutation Testing
* **Rationale:** To quantitatively measure the quality of the test suite (Yield). We must ensure that our tests actually fail if the underlying logic is broken.
* **Tool:** `mutmut` or `cosmic-ray`
* **Implementation Plan:**
    * Run mutation analysis on the `volatility_service.py` and `vpin_service.py` modules.
    * **Target:** Achieve a mutation score > 80% (i.e., kill 80% of generated mutants).
    * [cite_start]**Example Mutant:** If the code changes `VPIN < 0.5` (Regime 0 boundary) [cite: 90] to `VPIN <= 0.5`, is there a test case specifically at `0.5` that fails?

### 4.3 Metamorphic Testing (Machine Learning)
* **Rationale:** It is the "Oracle Problem"—we do not always know the "correct" regime for unseen data.
* **Technique:** We test relationships between inputs and outputs.
* **Metamorphic Relations:**
    * **Relation 1 (Monotonicity):** If we take a historical data slice classified as "Normal" and artificially inject high volatility and toxic order flow (perturbation), the probability of "Crash" regime classification should *increase* or stay the same. It should never *decrease*.
    * **Relation 2 (Stability):** Adding white noise (random small variations < 0.01%) to the price data should not flip the classification from "Normal" to "Crash".

---

## 5. CI/CD Pipeline Automation (planned)

* **Platform:** GitHub Actions / GitLab CI
* **Workflow:**
    1.  **Static Analysis:** `ruff` (linting) and `bandit` (security scan for API keys).
    2.  **Unit & Integration:** Run `pytest` with coverage.
    3.  **Threshold Enforcement:** Pipeline fails if Coverage < 85%.
    4.  **Property Checks:** Run `hypothesis` suite (slower tests).

## 6. Traceability Matrix (LO1 linkage)
RiskBeacon uses an LO1 requirements set `RB-REQ-01..RB-REQ-17` defined in `docs/requirements.md` and mapped to tests in `docs/requirements_to_tests.md`.

Summary table (high-signal examples):

| Requirement ID | Description | Test component(s) | File location(s) |
| :--- | :--- | :--- | :--- |
| **RB-REQ-04** | Generate 5 plots per ticker | System | `test/integration_testing/test_api_plot_generation.py` (planned), `services/plotService.py` (fix evidence) |
| **RB-REQ-06** | VPIN in [0,1] | Unit/Property + GX | `test/unit_testing/Quant_services/test_vpin_invariants.py` (planned), `test/data_validation/test_gx_market_data.py` |
| **RB-REQ-12** | Reproducible ML artifacts | MLOps (MLflow) | `test/mlops/test_mlflow_tracking.py` |
| **RB-REQ-15** | IB failures degrade gracefully | Integration | `ib_troubleshoot.py`, `api/routes.py`, `services/ib_client_service.py` |

---

## 7. Plan evolution (iterations tied to evidence)
This plan evolved based on defects encountered during development and testing:

* **Iteration 1 — Plot failure → schema validation + repair tooling**
    * Observation: plot generation could fail due to duplicate column names after normalization (`VPIN` + `vpin` → duplicated `vpin`).
    * Response: fixed plot loader, added GX schema checks, and added `normalize_historical_data.py` to repair data.
    * Evidence: `services/plotService.py`, `test/data_validation/*`, `normalize_historical_data.py`, RB-REQ-04/RB-REQ-13/RB-REQ-14.

* **Iteration 2 — IB integration → diagnosable connectivity and graceful degradation**
    * Observation: IB connectivity is environment-dependent (ports, permissions) and should not break the dashboard.
    * Response: implemented IB endpoints + connection settings UI + `ib_troubleshoot.py` and ensured API still works when IB fails.
    * Evidence: `services/ib_client_service.py`, `api/routes.py`, `frontend/dashboard.py`, `ib_troubleshoot.py`, RB-REQ-15.

* **Iteration 3 — Regime/VPIN stability → explainability + fallback mode**
    * Observation: bar-based VPIN classification can saturate; ML regimes can become degenerate when input distributions are pathological.
    * Response: improved VPIN heuristic and added `REGIME_MODE=rule` fallback; strengthened data validation + reproducibility evidence (MLflow).
    * Evidence: `services/vpin_service.py`, `controllers/ServiceController.py`, `test/mlops/test_mlflow_tracking.py`, RB-REQ-10/RB-REQ-11/RB-REQ-12.

---

## 8. Evaluation of Limitations (LO4)
* [cite_start]**Data Drift:** Tests use historical Bloomberg data[cite: 79]. We acknowledge that this does not guarantee future performance (Covariate Shift).
* **Oracle Limit:** We assume the "Ground Truth" labels in the training set are correct, but market regimes are subjective.
* [cite_start]**Time-Series Constraint:** Standard K-Fold cross-validation was used[cite: 93], but Time-Series Split is more appropriate for backtesting to prevent look-ahead bias. This is a noted limitation in the current test plan.