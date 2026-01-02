# Requirements → Tests Mapping (LO1 Traceability)

This document maps each `RB-REQ-XX` requirement to:
- **Test type**: unit / integration / system / property / metamorphic / perf / review / data validation (GX)
- **Planned test path** in your repository (under `test/…`)
- **Pass criteria** (what the test asserts)
- **CI hook**: placeholder job name you can later link in your portfolio (e.g., GitLab pipeline job URL)

> Note: pytest defaults to discovering tests in `tests/` and files named `test_*.py`.  
> Because your folder is named `test/`, you’ll likely add `pytest.ini` later with `testpaths = test`.

---

## Traceability matrix (summary)

| ReqID | Requirement (short) | Test type(s) | Test file(s) | CI job |
|---|---|---|---|---|
| RB-REQ-01 | `/api/tick` persists OHLCV | system | `test/integration_testing/test_api_tick_persists_csv.py` | `ci:test` |
| RB-REQ-02 | bucket fill triggers VPIN+vol | integration | `test/integration_testing/test_pipeline_metrics_persist.py` | `ci:test` |
| RB-REQ-03 | `/api/metrics/history` schema | system | `test/integration_testing/test_api_metrics_history_schema.py` | `ci:test` |
| RB-REQ-04 | `/api/plots/generate` creates 5 plots | system | `test/integration_testing/test_api_plot_generation.py` | `ci:test` |
| RB-REQ-05 | dashboard ticker selection updates | system (manual/automated) | `test/integration_testing/test_dashboard_smoke.md` | `ci:manual` |
| RB-REQ-06 | VPIN in [0,1] | unit + property + GX | `test/unit_testing/Quant_services/test_vpin_invariants.py`, `test/data_validation/test_gx_market_data.py` | `ci:test`, `ci:gx` |
| RB-REQ-07 | vol ≥ 0 | unit + property + GX | `test/unit_testing/Quant_services/test_vol_yang_zhang_invariants.py`, `test/data_validation/test_gx_market_data.py` | `ci:test`, `ci:gx` |
| RB-REQ-08 | vol finite with malformed rows | unit | `test/unit_testing/Quant_services/test_vol_robustness_bad_rows.py` | `ci:test` |
| RB-REQ-09 | crash_prob in [0,1] | unit | `test/unit_testing/Quant_services/test_crash_prob_bounds.py` | `ci:test` |
| RB-REQ-10 | regime stability under noise | metamorphic | `test/unit_testing/ML_services/test_regime_stability_noise.py` | `ci:test-ml` |
| RB-REQ-11 | time-series split used | system (eval) | `test/backtesting/test_time_series_split.py` | `ci:eval` |
| RB-REQ-12 | reproducible artifacts (seed/data/code) | system (MLOps) + MLflow | `test/unit_testing/ML_services/test_reproducibility_metadata.py`, `test/mlops/test_mlflow_tracking.py` | `ci:mlops` |
| RB-REQ-13 | CSV bad lines don’t crash API | integration | `test/integration_testing/test_api_tolerates_bad_csv_lines.py` | `ci:test` |
| RB-REQ-14 | missing sentiment doesn’t break plots | system + GX | `test/integration_testing/test_plot_generation_without_sentiment.py`, `test/data_validation/test_gx_sentiment_data.py` | `ci:test`, `ci:gx` |
| RB-REQ-15 | IB failure degrades gracefully | integration | `test/unit_testing/SE_services/test_ib_failure_degrades_gracefully.py` | `ci:test` |
| RB-REQ-16 | `/api/tick` latency | perf | `test/integration_testing/test_perf_api_tick_latency.py` | `ci:perf` |
| RB-REQ-17 | plot generation time | perf | `test/integration_testing/test_perf_plot_generation_time.py` | `ci:perf` |

---

## Detailed mapping

### RB-REQ-01 — Tick ingestion persists OHLCV
- **Test type**: system
- **Planned test file**: `test/integration_testing/test_api_tick_persists_csv.py`
- **Pass criteria**:
  - `POST /api/tick` returns 200
  - CSV exists for ticker and last row matches payload fields
- **CI job**: `ci:test`

### RB-REQ-02 — Bucket fill triggers metric computation when bucket fills
- **Test type**: integration
- **Planned test file**: `test/integration_testing/test_pipeline_metrics_persist.py`
- **Pass criteria**:
  - After sending enough ticks, CSV contains non-null `VPIN` and non-null `vol` in at least one row
- **CI job**: `ci:test`

### RB-REQ-03 — Metrics history endpoint returns consistent schema
- **Test type**: system
- **Planned test file**: `test/integration_testing/test_api_metrics_history_schema.py`
- **Pass criteria**:
  - response validates required keys and timestamps parse
- **CI job**: `ci:test`

### RB-REQ-04 — Plot generation produces 5 plot files for chosen ticker
- **Test type**: system
- **Planned test file**: `test/integration_testing/test_api_plot_generation.py`
- **Pass criteria**:
  - response 200
  - output folder contains 5 expected ticker-prefixed plots
- **CI job**: `ci:test`

### RB-REQ-05 — Dashboard displays selected ticker and updates metrics
- **Test type**: system (manual or Playwright/Selenium optional)
- **Planned evidence**: `test/integration_testing/test_dashboard_smoke.md` (manual checklist) and/or `test/integration_testing/test_dashboard_e2e.py`
- **Pass criteria**:
  - selecting ticker changes displayed charts/metrics
  - no exceptions in UI
- **CI job**: `ci:manual` (or `ci:e2e` if automated)

### RB-REQ-06 — VPIN boundedness invariant
- **Test type**: unit + property (Hypothesis)
- **Planned test file**: `test/unit_testing/Quant_services/test_vpin_invariants.py`
- **Pass criteria**:
  - VPIN always within [0,1] for generated inputs
- **CI job**: `ci:test`

### RB-REQ-07 — Yang–Zhang volatility non-negativity
- **Test type**: unit + property (Hypothesis)
- **Planned test file**: `test/unit_testing/Quant_services/test_vol_yang_zhang_invariants.py`
- **Pass criteria**:
  - computed vol ≥ 0 and finite for generated OHLC
- **CI job**: `ci:test`

### RB-REQ-08 — Volatility robustness to malformed rows
- **Test type**: unit
- **Planned test file**: `test/unit_testing/Quant_services/test_vol_robustness_bad_rows.py`
- **Pass criteria**:
  - no NaN/Inf outputs when dataset has bad timestamps/missing OHLC/bad lines
- **CI job**: `ci:test`

### RB-REQ-09 — Crash probability boundedness
- **Test type**: unit
- **Planned test file**: `test/unit_testing/Quant_services/test_crash_prob_bounds.py`
- **Pass criteria**:
  - crash_prob in [0,1]
- **CI job**: `ci:test`

### RB-REQ-10 — Metamorphic stability under small perturbations
- **Test type**: metamorphic
- **Planned test file**: `test/unit_testing/ML_services/test_regime_stability_noise.py`
- **Pass criteria**:
  - flip rate under small noise below threshold
- **CI job**: `ci:test-ml`

### RB-REQ-11 — No look-ahead bias in evaluation split
- **Test type**: system (evaluation/backtesting)
- **Planned test file**: `test/backtesting/test_time_series_split.py`
- **Pass criteria**:
  - evaluation code uses chronological split; fails if random split is used
- **CI job**: `ci:eval`

### RB-REQ-12 — Reproducible model artifacts
- **Test type**: system (MLOps)
- **Planned test file**: `test/unit_testing/ML_services/test_reproducibility_metadata.py`
- **Pass criteria**:
  - metadata includes seed + code hash + data hash; metrics reproducible within tolerance
- **CI job**: `ci:mlops`

#### MLflow evidence hook (recommended)
- **Test type**: MLOps (experiment tracking)
- **Planned test file**: `test/mlops/test_mlflow_tracking.py`
- **Pass criteria**:
  - MLflow run logs parameters + metrics + dataset identifiers
  - run artifact store exists on disk (local `mlruns/` or temp dir)
- **CI job**: `ci:mlops`

### RB-REQ-13 — CSV ingestion tolerates bad lines
- **Test type**: integration
- **Planned test file**: `test/integration_testing/test_api_tolerates_bad_csv_lines.py`
- **Pass criteria**:
  - API endpoints return 200/4xx, not 500, when CSV has some malformed lines
- **CI job**: `ci:test`

### RB-REQ-14 — Missing sentiment does not break plot generation
- **Test type**: system
- **Planned test file**: `test/integration_testing/test_plot_generation_without_sentiment.py`
- **Pass criteria**:
  - plot generation succeeds with sentiment missing/empty for ticker
- **CI job**: `ci:test`

### RB-REQ-15 — IB connectivity failures degrade gracefully
- **Test type**: integration (mock external dependency)
- **Planned test file**: `test/unit_testing/SE_services/test_ib_failure_degrades_gracefully.py`
- **Pass criteria**:
  - `/api/status` and dashboard logic still work if IB connect fails
- **CI job**: `ci:test`

### RB-REQ-16 — Tick processing latency target
- **Test type**: perf
- **Planned test file**: `test/integration_testing/test_perf_api_tick_latency.py`
- **Pass criteria**:
  - Over 100 requests, median < 50ms and p95 < 150ms
- **CI job**: `ci:perf`

### RB-REQ-17 — Plot generation time target
- **Test type**: perf
- **Planned test file**: `test/integration_testing/test_perf_plot_generation_time.py`
- **Pass criteria**:
  - completes in < 300 seconds for dataset size ≥ 3000 rows
- **CI job**: `ci:perf`


