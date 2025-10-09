"""
Temporary CLI for bbox pipelines.

This entry point currently proxies to the legacy overlap analysis script while
we migrate the implementation into ``spdatalab.dataset.bbox.core``.
"""

from __future__ import annotations

import sys
import warnings
from types import ModuleType
from typing import Iterable


def _load_legacy_module() -> ModuleType:
    """Import the legacy overlap runner from the refactored package."""

    from importlib import import_module

    return import_module("spdatalab.dataset.bbox.legacy_overlap")


def _run_legacy_main(module: ModuleType, argv: Iterable[str]) -> int:
    """Execute the copied legacy ``main`` while preserving ``sys.argv``."""

    argv = list(argv)
    original_argv = sys.argv
    try:
        sys.argv = [original_argv[0]] + argv
        module.main()
    except SystemExit as exc:  # pragma: no cover - mirrors legacy behaviour
        return int(exc.code or 0)
    finally:
        sys.argv = original_argv
    return 0


def main(argv: list[str] | None = None) -> int:
    """Launch the bbox overlap analysis via the legacy implementation."""

    if argv is None:
        argv = sys.argv[1:]

    warnings.warn(
        "spdatalab.dataset.bbox.cli currently delegates to the legacy "
        "overlap runner. The refactor will expose a native implementation "
        "via spdatalab.dataset.bbox.core in upcoming changes.",
        DeprecationWarning,
        stacklevel=2,
    )

    module = _load_legacy_module()
    return _run_legacy_main(module, argv)


if __name__ == "__main__":
    raise SystemExit(main())
