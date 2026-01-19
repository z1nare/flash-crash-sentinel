# RiskBeacon Test Plan

**Author:** Arsenii Harbar  
**Project:** RiskBeacon (Real-Time Market Risk Monitoring System)  
**Date:** January 2026  
**Repo:** tests in `python_components/test/…`, requirements in `python_components/docs/requirements.md`, CI in `.github/workflows/ci.yml`.

## 1. Introduction

This test plan maps the LO1 requirements to actual test activities and evidence for RiskBeacon. The approach is requirements-driven (verification), but I also included validation to make sure the dashboard actually works and monitoring to track how things are going.

**Strategy vs plan:**

- **Strategy:** The long-term stuff - CI automation, conventions, GitHub Actions workflow (`.github/workflows/ci.yml`), test folder structure, pinned dev dependencies. This changes slowly.
- **Plan:** This document, the test inventory, and the plan evolution section that tracks real bugs and commits. This changes quickly as we find issues.

**Context:** The main user is someone monitoring market stability (could be a student, trader, or risk analyst). The risky parts are: getting the quant metrics right (VPIN/vol), handling bad CSV data, dealing with IB connection failures, and keeping performance good under load.

## 2. Testing Pyramid & Scope

### 2.1 Unit Testing

**Scope:** Testing individual math functions (VPIN, Yang-Zhang Volatility) and data parsing logic.

**Tools:** `pytest`

**Approach:**
- **Math verification:** Using Hypothesis for invariants plus regular unit tests on the same services. This gives redundancy.
- **Boundary testing:** Made sure to test edge cases like when trade volume is zero (need to avoid division by zero in VPIN).

### 2.2 Integration Testing

**Scope:** Testing how FastAPI, ServiceController, and the InteractiveBrokers adapter work together.

**Tools:** `pytest-mock`, `unittest.mock`

**Strategy:**
- **Dependency injection:** The code uses `RegimeModelInterface` and data provider interfaces which makes it easier to test.
- **Mocking IB:** I mock the Interactive Brokers API to test different scenarios:
  - **Scenario A:** Normal market data stream (happy path)
  - **Scenario B:** Network disconnection or timeout (need resilience)
  - **Scenario C:** Bad packets or invalid ticker symbols
- **Goal:** Make sure the system doesn't crash the dashboard when IB fails.

### 2.3 System Testing

**Scope:** End-to-end testing of the full pipeline: `POST /api/tick` → CSV persistence → metric computation → plots/dashboard.

**Tools:** FastAPI `TestClient`, maybe Selenium/Playwright for Streamlit later (didn't get to this).

### 2.4 Test Inventory

This section lists the tests and evidence in the repo and maps them to the LO1 requirements (`docs/requirements.md`).

**Test Inventory Table:**

| Test type | Purpose | Tooling | Evidence (file paths) | Requirements covered |
|---|---|---|---|---|
| Unit (example-based) | Correctness + regression for services | `pytest` | `test/unit_testing/service_modules/*` | RB-REQ-06/07/09 |
| Property-based | Quant invariants on generated inputs | `hypothesis` | `test/unit_testing/Quant_services/test_vpin_invariants.py`, `test/unit_testing/Quant_services/test_vol_yang_zhang_invariants.py` | RB-REQ-06/07 |
| Integration (API) | Verify endpoint behaviour + filesystem effects | `fastapi.testclient` | `test/integration_testing/test_api_tick_persists_csv.py`, `test/integration_testing/test_api_plot_generation.py` | RB-REQ-01/04/14 |
| Performance | Latency/time measurements | `pytest` + timers | `test/integration_testing/test_perf_api_tick_latency.py`, `test/integration_testing/test_perf_plot_generation_time.py` | RB-REQ-16/17 |
| Data validation | Schema + invariants on datasets | Great Expectations | `test/data_validation/gx_suites.py`, `test/data_validation/test_gx_market_data.py`, `test/data_validation/test_gx_sentiment_data.py` | RB-REQ-13/14 |
| MLOps | Reproducibility artifacts | MLflow | `test/mlops/test_mlflow_tracking.py` | RB-REQ-12 |
| Backtesting | Time-series validation + strategy reliability | `bt` (Backtrader) | `test/backtesting/research.ipynb` | RB-REQ-11 |
| CI automation | Repeatability + visibility | GitHub Actions | `.github/workflows/ci.yml` | LO5 evidence; supports all |

**CI Artifacts:**

The CI workflow uploads:
- Coverage reports: `coverage.xml`, `htmlcov/`
- GX suite exports: `docs/data_validation/gx_suites/`

**What's Implemented:**

- Backtesting with time-series splits (RB-REQ-11): `test/backtesting/research.ipynb` has strategy backtesting with 70/30 train/test splits, sentiment integration, stop-loss, and multi-asset support (AMD, NVDA, TSLA). Shows portfolio returns, Sharpe ratios, drawdown analysis, and prevents look-ahead bias.

**What's Not Done Yet:**

- Metamorphic regime tests (RB-REQ-10): Planned but didn't get to it. Maybe later.


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

## 4. Risk register (LO2 monitoring + contingency)

| Risk | Likelihood | Impact | Mitigation | Test hook / evidence |
|---|---:|---:|---|---|
| IB not running / wrong port | High | Medium | Feature-flag + graceful degradation; troubleshooting script | `ib_troubleshoot.py`, `/api/ib/*` endpoints |
| IB market data permissions | Medium | Medium | Document limitation; fall back to historical | `ib_troubleshoot.py` logs; dashboard messaging |
| Malformed/duplicated CSV columns | High | High | Normalize columns on load; normalization script | `services/plotService.py`, `normalize_historical_data.py` |
| Model degeneracy (regime always crash) | Medium | High | VPIN heuristic fix; rule fallback | `services/vpin_service.py`, `REGIME_MODE=rule` |
| Heavy deps break CI (FinBERT/torch) | High | High | Disable via env var in CI | `.github/workflows/ci.yml`, `services/sentimentService.py` |
| Performance regressions (tick latency) | Medium | High | Perf measurement tests; remove redundant CSV IO | `test/integration_testing/test_perf_*`, `services/vol_service.py` |
| Great Expectations version mismatch | Medium | Medium | Pin dev deps; export suites in CI | `requirements-dev.txt`, `scripts/export_gx_suites.py` |
| Large datasets not in repo | High | Medium | CI uses synthetic export; tests use temp data | `scripts/export_gx_suites.py`, TestClient tests |

---

## 5. Process monitoring (visibility)

Weekly metrics (evidence-backed):
- **CI pass rate** (GitHub Actions)
- **Coverage % + trend** (`coverage.xml`, `htmlcov/`)
- **Open defects list** (GitHub Issues or `docs/KNOWN_ISSUES.md` if used)
- **Performance**: `/api/tick` median/p95; plot generation time (printed by perf tests)

---

## 6. Advanced Testing Techniques

### 4.1 Property-Based Testing (Hypothesis)
* **Rationale:** Financial metrics must adhere to mathematical invariants regardless of input data. Standard example-based tests are insufficient to cover the infinite state space of market prices.
* **Tool:** `hypothesis`
* **Implemented evidence:**
    * **Invariant 1 (Volatility):** For any sequence of High/Low/Open/Close prices generated, the Yang-Zhang volatility $\sigma^2_{YZ}$ must always be $\geq 0$.
    * **Invariant 2 (VPIN):** VPIN must always be in the range $[0, 1]$.
    * Evidence: `test/unit_testing/Quant_services/*`

### 4.2 Mutation Testing
* **Rationale:** To quantitatively measure the quality of the test suite (Yield). We must ensure that our tests actually fail if the underlying logic is broken.
* **Tool:** `mutmut` or `cosmic-ray`
* **Implementation Plan:**
    * Run mutation analysis on the `volatility_service.py` and `vpin_service.py` modules.
    * **Target:** Achieve a mutation score > 80% (i.e., kill 80% of generated mutants).
    * **Example mutant:** If the code changes `VPIN < 0.5` (Regime 0 boundary) to `VPIN <= 0.5`, is there a test case specifically at `0.5` that fails?

### 4.3 Metamorphic Testing (Machine Learning)
* **Rationale:** It is the "Oracle Problem"—we do not always know the "correct" regime for unseen data.
* **Technique:** We test relationships between inputs and outputs.
* **Metamorphic Relations:**
    * **Relation 1 (Monotonicity):** If we take a historical data slice classified as "Normal" and artificially inject high volatility and toxic order flow (perturbation), the probability of "Crash" regime classification should *increase* or stay the same. It should never *decrease*.
    * **Relation 2 (Stability):** Adding white noise (random small variations < 0.01%) to the price data should not flip the classification from "Normal" to "Crash".

---

## 7. CI/CD Pipeline Automation (implemented LO5 evidence)

* **Platform:** GitHub Actions
* **Workflow file:** `.github/workflows/ci.yml` (repo root)
* **Automated steps:** install minimal deps, run pytest with coverage, export GX suite docs, upload artefacts.
* **Why it matters (lecture alignment):** cost-effective repeatability + visibility; prevents “big bang” integration and makes results auditable.

## 8. Traceability Matrix (LO1 linkage)
RiskBeacon uses an LO1 requirements set `RB-REQ-01..RB-REQ-17` defined in `docs/requirements.md` and mapped to tests in `docs/requirements_to_tests.md`.

Summary table (high-signal examples):

| Requirement ID | Description | Test component(s) | File location(s) |
| :--- | :--- | :--- | :--- |
| **RB-REQ-04** | Generate 5 plots per ticker | System | `test/integration_testing/test_api_plot_generation.py` (planned), `services/plotService.py` (fix evidence) |
| **RB-REQ-06** | VPIN in [0,1] | Unit/Property + GX | `test/unit_testing/Quant_services/test_vpin_invariants.py` (planned), `test/data_validation/test_gx_market_data.py` |
| **RB-REQ-12** | Reproducible ML artifacts | MLOps (MLflow) | `test/mlops/test_mlflow_tracking.py` |
| **RB-REQ-15** | IB failures degrade gracefully | Integration | `ib_troubleshoot.py`, `api/routes.py`, `services/ib_client_service.py` |

---

## 9. Plan evolution (iterations tied to evidence + commit IDs)
This plan evolved based on defects encountered during development and testing:

* **Iteration 1 — Plot failure → schema validation + repair tooling**
    * Observation: plot generation could fail due to duplicate column names after normalization (`VPIN` + `vpin` → duplicated `vpin`).
    * Response: fixed plot loader, added GX schema checks, and added `normalize_historical_data.py` to repair data.
    * Evidence: `services/plotService.py`, `test/data_validation/*`, `normalize_historical_data.py` (e.g. commits: `bda8aec`, `0d5997f`).

* **Iteration 2 — IB integration → diagnosable connectivity and graceful degradation**
    * Observation: IB connectivity is environment-dependent (ports, permissions) and should not break the dashboard.
    * Response: implemented IB endpoints + connection settings UI + `ib_troubleshoot.py` and ensured API still works when IB fails.
    * Evidence: `services/ib_client_service.py`, `api/routes.py`, `frontend/dashboard.py`, `ib_troubleshoot.py` (e.g. commit: `7fa6ada`).

* **Iteration 3 — Regime/VPIN stability → explainability + fallback mode**
    * Observation: bar-based VPIN classification can saturate; ML regimes can become degenerate when input distributions are pathological.
    * Response: improved VPIN heuristic and added `REGIME_MODE=rule` fallback; strengthened data validation + reproducibility evidence (MLflow).
    * Evidence: `services/vpin_service.py`, `controllers/ServiceController.py`, `test/mlops/test_mlflow_tracking.py` (e.g. commits: `2cceb5c`, `c1cae2d`).

---

## 10. Evaluation of Limitations (LO4)
* **Data drift:** Tests use historical data. We acknowledge that this does not guarantee future performance (covariate shift).
* **Oracle limit:** We assume the "ground truth" labels in the training set are correct, but market regimes are subjective.
* **Time-series constraint:** Time-series split is used for backtesting to prevent look-ahead bias, as documented in the backtesting implementation.
