"""
Refactored bbox dataset package.

The legacy logic from the monolithic ``bbox.py`` module remains available
through the compatibility imports below. New code should prefer the modules
under this package instead of ``legacy``.
"""

from __future__ import annotations

import warnings

from . import core, io, pipeline  # re-export planned modules
from . import legacy as _legacy  # noqa: F401
from .legacy import *  # noqa: F401,F403 - temporary shim for compatibility

warnings.warn(
    "spdatalab.dataset.bbox now loads via the refactored package layout; "
    "legacy attributes remain available via spdatalab.dataset.bbox.legacy "
    "but will be removed after the refactor completes.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["core", "io", "pipeline"]
__all__ += [name for name in dir(_legacy) if not name.startswith("_")]
