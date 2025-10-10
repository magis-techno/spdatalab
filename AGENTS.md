# Agent Instructions

These instructions summarize the expectations captured in `docs/analysis_workflow_guidelines.md` and apply to the entire repository unless a more specific `AGENTS.md` overrides them.

## Analysis code organisation
- Keep reusable analysis logic in the `src/spdatalab` package. Prefer feature-focused subpackages such as `spdatalab.dataset.<feature>` or `spdatalab.fusion`.
- Structure feature packages with `core.py` (pure business logic), optional `io.py`/`pipeline.py` helpers, and a dedicated `cli.py` for orchestration. Reuse `spdatalab.common` utilities when possible instead of duplicating infrastructure code.
- When touching existing single-file modules that mix responsibilities, consider splitting them according to the above pattern. Preserve compatibility shims (e.g., thin wrappers in legacy entry points) until downstream consumers migrate.

## CLI expectations
- Command line entry points should only parse arguments, build configuration/clients, and call functions in `core` modules. Implement `main()` functions that return exit codes and guard them with `if __name__ == "__main__": raise SystemExit(main())`.
- Example scripts in `examples/` should delegate to the appropriate CLI module rather than duplicating orchestration logic.

## Notebook guidance
- Notebooks must import reusable helpers from Python modules under `src/spdatalab` (e.g., `analysis/notebook_support.py`) instead of embedding large logic blocks.
- Strip execution outputs (e.g., via `nbstripout`) before committing Notebook changes.

## Testing and validation
- Provide automated coverage for new or refactored analysis logic via `pytest`, targeting both core functions and CLI entry points (e.g., smoke tests with minimal datasets).
- When migrating workflows, keep or update baseline artefacts under `tests/data/baseline/` and add comparison scripts/tests when appropriate.

## Documentation and automation
- Update relevant documentation (such as `README.md` or analysis guides) whenever commands, entry points, or notebook flows change.
- Prefer codifying common tasks in the `Makefile` or scripts under `scripts/` for reproducibility.
