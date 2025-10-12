# Test Coverage Improvement Plan

## Current Blockers
- Pytest cannot import the `spdatalab` package when invoked from the repository root, causing collection to halt in `scripts/testing/run_trajectory_road_analysis_test.py` and `tests/integration/test_real_polygon_query_integration.py`.
- Coverage tooling (`pytest-cov`) is missing from the environment, and the container cannot install packages from the internet due to proxy restrictions.
- Because collection stops early, the true unit and integration test pass/fail state is unknown, and there is no baseline coverage percentage to target.

## Immediate Remediation Steps
1. **Ensure the package is importable for tests.**
   - Run tests with `PYTHONPATH=src` or install the project in editable mode (`pip install -e .`).
   - Update developer documentation (e.g., `README.md`) to note the required environment variable or editable install when running tests locally or in CI.
   - If CI is used, add the appropriate environment configuration to the workflow so that imports succeed consistently.
2. **Vendor the coverage dependency.**
   - Add `pytest-cov` to the development dependencies in `pyproject.toml`.
   - If external downloads remain blocked in CI, mirror the wheel in the repository or configure an internal package index so coverage tooling is available offline.
3. **Capture the baseline test results.**
   - Once imports succeed, run `pytest` to identify failing tests and categorize them (regressions vs. expected failures due to missing data, long-running integration cases, etc.).
   - Mark any inherently flaky or long-running integration tests with appropriate pytest markers so they can be skipped or isolated in CI.

## Coverage Expansion Roadmap
1. **Prioritize high-value modules.**
   - Start with packages that power trajectory fusion (`spdatalab.fusion.*`) and dataset ingestion (`spdatalab.dataset.*`), since they are heavily exercised by current tests that already exist but fail to collect.
   - For each module, add focused unit tests under `tests/unit/` that mock external services and verify pure logic paths.
2. **Refactor tests for reusability.**
   - Move duplicated fixtures into `tests/conftest.py` and leverage `pytest` parametrization to increase scenario coverage without duplicating code.
   - Build small synthetic datasets in `tests/data/` to cover edge cases without relying on large real-world fixtures.
3. **Introduce coverage gates.**
   - After stabilizing tests, run `pytest --cov=spdatalab --cov-report=term-missing` locally to capture the baseline percentage.
   - Add a coverage threshold (e.g., `--cov-fail-under=70`) to the CI pipeline. Increase the threshold gradually (5% increments) as new tests land.
4. **Automate via Makefile/CI.**
   - Create a `make test` target that exports `PYTHONPATH=src` and runs both regular tests and the coverage job.
   - Ensure the CI workflow uses the same target, so local and remote runs stay aligned.

## Long-Term Improvements
- Audit `scripts/testing/` to convert orchestration scripts into proper pytest modules or CLI smoke tests that can be invoked via fixtures, improving observability.
- Establish a nightly job that executes the slow integration tests with real data so that daily CI can remain fast while still exercising end-to-end flows.
- Document the testing strategy in `docs/testing.md`, including how coverage is measured and how engineers should contribute new tests.
