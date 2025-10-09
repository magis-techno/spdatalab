"""
Persistence helpers for bbox pipelines.

The refactor will move the database and filesystem interactions that used to
live in ``legacy.py`` into focused functions here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(slots=True)
class SceneRecord:
    """Lightweight view of the scene metadata stored in the warehouse."""

    scene_token: str
    data_name: str
    city_id: int
    timestamp: str


def fetch_scenes(city: str | None = None) -> Iterable[SceneRecord]:
    """Placeholder for the upcoming SQLAlchemy-backed scene query."""

    raise NotImplementedError("Pending migration from legacy implementation.")
