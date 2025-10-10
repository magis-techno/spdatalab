"""
Refactored bbox dataset package.

The legacy logic from the monolithic ``bbox.py`` module remains available
through the compatibility imports below. New code should prefer the modules
under this package instead of ``legacy``.
"""

from __future__ import annotations

import warnings

from . import core, io, pipeline, summary  # re-export planned modules
from .io import (  # noqa: F401 - surface the new IO helpers at package root
    BBoxDataRepository,
    fetch_bbox_with_geometry,
    fetch_meta,
    load_scene_ids,
    load_scene_ids_from_json,
    load_scene_ids_from_parquet,
    load_scene_ids_from_text,
)
from . import legacy as _legacy  # noqa: F401
from .legacy import *  # noqa: F401,F403 - temporary shim for compatibility

warnings.warn(
    "spdatalab.dataset.bbox now loads via the refactored package layout; "
    "legacy attributes remain available via spdatalab.dataset.bbox.legacy "
    "but will be removed after the refactor completes.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "core",
    "io",
    "pipeline",
    "summary",
    "BBoxDataRepository",
    "fetch_bbox_with_geometry",
    "fetch_meta",
    "load_scene_ids",
    "load_scene_ids_from_json",
    "load_scene_ids_from_parquet",
    "load_scene_ids_from_text",
]
__all__ += [name for name in dir(_legacy) if not name.startswith("_")]
