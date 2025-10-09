"""
Persistence helpers for bbox pipelines.

This module centralises the routines that load analysis inputs from local files
or warehouse backends so they can be reused across the refactor.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

from .pipeline import PARQUET_AVAILABLE

__all__ = [
    "SceneRecord",
    "fetch_scenes",
    "load_scene_ids",
    "load_scene_ids_from_json",
    "load_scene_ids_from_parquet",
    "load_scene_ids_from_text",
]


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


# ---------------------------------------------------------------------------
# Dataset loading helpers


def load_scene_ids_from_json(file_path: str | Path) -> list[str]:
    """Read scene identifiers from a dataset manifest stored as JSON."""

    with open(Path(file_path), "r", encoding="utf-8") as handle:
        dataset_data = json.load(handle)

    scene_ids: list[str] = []
    for subdataset in dataset_data.get("subdatasets", []):
        scene_ids.extend(subdataset.get("scene_ids", []))

    return scene_ids


def load_scene_ids_from_parquet(file_path: str | Path) -> list[str]:
    """Read scene identifiers from a parquet manifest."""

    if not PARQUET_AVAILABLE:
        raise ImportError(
            "读取 parquet 需要安装 pandas 与 pyarrow: pip install pandas pyarrow"
        )

    df = pd.read_parquet(Path(file_path))
    return df["scene_id"].unique().tolist()


def load_scene_ids_from_text(file_path: str | Path) -> list[str]:
    """Read one scene identifier per line from a plain text file."""

    lines = Path(file_path).read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip()]


def load_scene_ids(file_path: str | Path) -> list[str]:
    """
    Auto-detect the manifest format and return the deduplicated scene id list.

    Parameters
    ----------
    file_path:
        Path to a JSON / Parquet / plain text manifest file.
    """

    suffix = Path(file_path).suffix.lower()
    if suffix == ".json":
        return load_scene_ids_from_json(file_path)
    if suffix == ".parquet":
        return load_scene_ids_from_parquet(file_path)
    return load_scene_ids_from_text(file_path)
