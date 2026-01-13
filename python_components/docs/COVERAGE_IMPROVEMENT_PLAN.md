# Coverage Improvement Plan

## Current Status (Latest Run)
- **Overall Coverage**: 73% (530 missing statements out of 1992 total)
- **Tests**: 180 passed, 3 skipped, 4 xfailed
- **Target**: 80%+ for distinction

## Coverage by Module

| Module | Current | Missing | Target | Priority |
|--------|---------|---------|--------|----------|
| `api/routes.py` | 77% | 82 | 80%+ | High |
| `controllers/ServiceController.py` | 66% | 97 | 70%+ | High |
| `services/sentimentService.py` | 61% | 19 | 75%+ | Medium |
| `services/vol_service.py` | 66% | 53 | 75%+ | High |
| `services/feature_engineering.py` | 86% | 21 | 90%+ | Low |
| `services/regime_service.py` | 84% | 24 | 85%+ | Low |
| `services/hmm_regime_service.py` | 65% | 47 | 70%+ | Medium |
| `services/data_replay_service.py` | 78% | 27 | 80%+ | Medium |
| `services/ib_client_service.py` | 46% | 100 | 50%+ | Low (external deps) |

## New Tests Created

### 1. Sentiment Service FinBERT Paths (`test_sentiment_service_finbert_paths.py`)
- Tests FinBERT loading success path
- Tests FinBERT loading exception handling
- Tests process_news with active pipeline
- Tests __main__ example code path
- **Expected Impact**: +10-15% coverage on `sentimentService.py` (61% → 75%+)

### 2. Volatility Service Yang-Zhang (`test_vol_service_yang_zhang.py`)
- Tests full Yang-Zhang calculation path
- Tests insufficient data scenarios
- Tests date mapping and normalization
- Tests timezone handling
- Tests invalid timestamp handling
- **Expected Impact**: +15-20% coverage on `vol_service.py` (66% → 80%+)

### 3. ServiceController Initialization Paths (`test_service_controller_init_paths.py`)
- Tests IB auto-connect paths
- Tests IB auto-stream paths
- Tests IB initialization failure
- Tests process_tick exception handling
- Tests vol_score = 0 path (regime not calculated)
- **Expected Impact**: +10-15% coverage on `ServiceController.py` (66% → 75%+)

## Remaining Gaps to Target

### High Priority (to reach 80% overall)

1. **API Routes Error Paths** (lines 91-99, 116-117, 131, 163-166, 210-213, 249-253, 290-294, etc.)
   - Regime service exception handling
   - CSV read error paths (old pandas versions)
   - Exception handling in various endpoints
   - **Action**: Add integration tests for exception paths

2. **ServiceController Process Tick** (lines 248-258, 261-290, 304-311)
   - Regime mode switching (rule/legacy_hmm/model)
   - Exception handling in volatility/regime calculation
   - **Action**: Add tests for different regime modes and error paths

3. **Volatility Service Yang-Zhang** (lines 206-310)
   - Date normalization edge cases
   - Mapping back to original timestamps
   - **Action**: Already covered in new tests above

### Medium Priority

4. **HMM Regime Service** (lines 51, 69-114, 122-145, 199, 213, 254-261, 265, 313, 321)
   - Model training paths
   - Prediction edge cases
   - **Action**: Add tests for HMM training and prediction

5. **Data Replay Service** (lines 23, 25, 81-83, 107-108, 129-131, 141, 150, 169, 187-189, etc.)
   - Replay boundary conditions
   - Rate limiting paths
   - **Action**: Add edge case tests

### Low Priority (justified gaps)

6. **IB Client Service** (46% coverage)
   - Requires live IB connection or complex mocking
   - External dependency - acceptable gap
   - **Action**: Document as justified gap

## Strategy to Reach 80%+

1. **Run new tests** and verify they execute successfully
2. **Add API route exception tests** for missing error paths
3. **Add ServiceController regime mode tests** for different regime calculation paths
4. **Add HMM service tests** if time permits
5. **Re-run coverage** and verify 80%+ achieved

## Expected Final Coverage

With all new tests:
- **Overall**: 73% → **78-82%** (target: 80%+)
- **api/routes.py**: 77% → **80%+**
- **controllers/ServiceController.py**: 66% → **75%+**
- **services/sentimentService.py**: 61% → **75%+**
- **services/vol_service.py**: 66% → **80%+**

## Next Steps

1. Run: `python -m pytest --cov=api --cov=controllers --cov=services --cov-report=term-missing`
2. Review coverage report for remaining gaps
3. Add targeted tests for highest-impact missing lines
4. Update portfolio with final coverage numbers

