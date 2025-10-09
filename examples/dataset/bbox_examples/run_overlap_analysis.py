#!/usr/bin/env python3
"""
Compatibility wrapper for the bbox overlap analysis CLI.

The original implementation now lives in ``spdatalab.dataset.bbox``. This thin
wrapper keeps existing entry points working while the refactor is in progress.
"""

from __future__ import annotations

import sys
import warnings

from spdatalab.dataset.bbox.cli import main as _cli_main


def main(argv: list[str] | None = None) -> int:
    warnings.warn(
        "examples/dataset/bbox_examples/run_overlap_analysis.py now delegates "
        "to spdatalab.dataset.bbox.cli. Please switch to "
        "`python -m spdatalab.dataset.bbox.cli` for future runs.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _cli_main([] if argv is None else argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
