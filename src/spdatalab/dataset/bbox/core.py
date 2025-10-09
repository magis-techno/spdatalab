"""
Core analytics logic for bbox workflows.

The functions defined here will gradually replace the legacy implementations
that previously lived in ``spdatalab.dataset.bbox``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol


class BBoxRepository(Protocol):
    """Abstracts data access for bbox scenes so the core logic stays testable."""

    def list_scene_tokens(self, limit: int | None = None) -> Iterable[str]:
        ...


@dataclass(slots=True)
class OverlapAnalysisConfig:
    """Carries the parameters for running an overlap analysis batch."""

    batch_size: int
    insert_batch_size: int
    work_dir: str = "./bbox_import_logs"


def run_overlap_analysis(repo: BBoxRepository, config: OverlapAnalysisConfig) -> None:
    """
    Entry point for the refactored overlap analysis.

    Parameters
    ----------
    repo:
        Provides access to scene metadata and bounding boxes.
    config:
        Tunable execution knobs (batch sizes, workspace paths, etc.).
    """

    raise NotImplementedError(
        "New implementation pending: migrate logic from legacy.run."
    )
