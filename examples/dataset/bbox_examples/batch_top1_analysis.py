#!/usr/bin/env python3
"""Compatibility wrapper delegating to :mod:`spdatalab.dataset.bbox.cli`."""

from __future__ import annotations

import sys
import warnings

from spdatalab.dataset.bbox.cli import main as _bbox_main


def main(argv: list[str] | None = None) -> int:
    warnings.warn(
        (
            "examples/dataset/bbox_examples/batch_top1_analysis.py 现已委托给 "
            "`spdatalab.dataset.bbox.cli batch-top1`，后续请直接调用新的 CLI。"
        ),
        DeprecationWarning,
        stacklevel=2,
    )
    args = [] if argv is None else list(argv)
    return _bbox_main(["batch-top1", *args])


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
