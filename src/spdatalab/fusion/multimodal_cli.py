"""Compatibility wrapper for the multimodal CLI.

The real implementation now lives in :mod:`spdatalab.fusion.cli.multimodal`.
This module keeps legacy entry points working while emitting a deprecation
warning so that downstream scripts can migrate at their own pace.
"""

from __future__ import annotations

import sys
import warnings

from spdatalab.fusion.cli.multimodal import main as _main


def main(argv: list[str] | None = None) -> int:
    """Delegate to :mod:`spdatalab.fusion.cli.multimodal` with a warning."""

    warnings.warn(
        "spdatalab.fusion.multimodal_cli is deprecated; "
        "use spdatalab.fusion.cli.multimodal instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _main([] if argv is None else argv)


if __name__ == "__main__":  # pragma: no cover - compatibility entry point
    raise SystemExit(main(sys.argv[1:]))
