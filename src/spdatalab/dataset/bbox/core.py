"""
Core analytics logic for bbox workflows.

The functions defined here will gradually replace the legacy implementations
that previously lived in ``spdatalab.dataset.bbox``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Protocol, Sequence, TypeVar

__all__ = [
    "BBoxRepository",
    "OverlapAnalysisConfig",
    "chunk",
    "run_overlap_analysis",
]

T_co = TypeVar("T_co", covariant=False)


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


def chunk(items: Sequence[T_co], size: int) -> Iterator[Sequence[T_co]]:
    """
    Yield ``items`` in evenly sized blocks (mirrors the legacy ``chunk`` helper).
    """

    if size <= 0:
        raise ValueError("chunk size must be a positive integer")

    for index in range(0, len(items), size):
        yield items[index : index + size]


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
