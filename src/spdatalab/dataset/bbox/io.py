"""
Persistence helpers for bbox pipelines.

This module centralises the routines that load analysis inputs from local files
or warehouse backends so they can be reused across the refactor.
"""

from __future__ import annotations

import json
import locale
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, ContextManager, Iterable, Sequence

import geopandas as gpd
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from spdatalab.common.io_hive import hive_cursor

from .pipeline import PARQUET_AVAILABLE

__all__ = [
    "SceneRecord",
    "fetch_scenes",
    "load_scene_ids",
    "load_scene_ids_from_json",
    "load_scene_ids_from_parquet",
    "load_scene_ids_from_text",
    "fetch_meta",
    "fetch_bbox_with_geometry",
    "BBoxDataRepository",
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


def _load_json_with_fallback(file_path: Path) -> Any:
    """Load JSON from ``file_path`` trying UTF-8 first then locale encoding."""

    encodings = ["utf-8"]
    preferred = locale.getpreferredencoding(False)
    if preferred and preferred.lower() not in {enc.lower() for enc in encodings}:
        encodings.append(preferred)

    last_error: UnicodeDecodeError | None = None
    for encoding in encodings:
        try:
            with open(file_path, "r", encoding=encoding) as handle:
                return json.load(handle)
        except UnicodeDecodeError as exc:
            last_error = exc

    # If we exhausted the encodings list re-raise the last error to keep
    # the standard error semantics of ``open``/``json.load``.
    if last_error is not None:
        raise last_error

    # Reaching this point means we never attempted to open the file, but
    # that can only happen when ``encodings`` is empty, which should not
    # occur. Raise ``RuntimeError`` for completeness.
    raise RuntimeError("No encodings available to decode JSON file")


def load_scene_ids_from_json(file_path: str | Path) -> list[str]:
    """Read scene identifiers from a dataset manifest stored as JSON."""

    dataset_data = _load_json_with_fallback(Path(file_path))

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


HiveCursorFactory = Callable[[], ContextManager[Any]]


def fetch_meta(
    tokens: Sequence[str],
    cursor_factory: HiveCursorFactory | None = None,
) -> pd.DataFrame:
    """
    Fetch metadata records for the provided scene tokens via Hive.
    """

    if not tokens:
        return pd.DataFrame(columns=["scene_token", "data_name", "event_id", "city_id", "timestamp"])

    cursor_factory = cursor_factory or hive_cursor
    sql = (
        "SELECT id AS scene_token, origin_name AS data_name, event_id, city_id, timestamp "
        "FROM transform.ods_t_data_fragment_datalake WHERE id IN %(tok)s"
    )
    with cursor_factory() as cur:
        cur.execute(sql, {"tok": tuple(tokens)})
        cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def fetch_bbox_with_geometry(
    names: Sequence[str],
    engine,
    *,
    point_table: str = "public.ddi_data_points",
) -> gpd.GeoDataFrame:
    """
    Fetch aggregated bounding boxes for datasets stored in PostGIS.
    """

    if not names:
        return gpd.GeoDataFrame(
            {"dataset_name": [], "all_good": []},
            geometry=gpd.GeoSeries([], crs=4326),
            crs=4326,
        )

    sql_query = text(
        f"""
        WITH bbox_data AS (
            SELECT
                dataset_name,
                ST_XMin(ST_Extent(point_lla)) AS xmin,
                ST_YMin(ST_Extent(point_lla)) AS ymin,
                ST_XMax(ST_Extent(point_lla)) AS xmax,
                ST_YMax(ST_Extent(point_lla)) AS ymax,
                bool_and(workstage = 2) AS all_good
            FROM {point_table}
            WHERE dataset_name = ANY(:names_param)
            GROUP BY dataset_name
        )
        SELECT
            dataset_name,
            all_good,
            CASE
                WHEN xmin = xmax OR ymin = ymax THEN
                    ST_Point((xmin + xmax) / 2, (ymin + ymax) / 2)
                ELSE
                    ST_MakeEnvelope(xmin, ymin, xmax, ymax, 4326)
            END AS geometry
        FROM bbox_data;
        """
    )

    return gpd.read_postgis(
        sql_query,
        engine,
        params={"names_param": list(names)},
        geom_col="geometry",
    )


@dataclass(slots=True)
class BBoxDataRepository:
    """Implements the refactored repository interface backed by Hive + PostGIS."""

    engine: Engine
    point_table: str = "public.ddi_data_points"
    hive_cursor_factory: HiveCursorFactory | None = None
    scene_loader: Callable[[str | Path], list[str]] = load_scene_ids
    metadata_fetcher: Callable[..., pd.DataFrame] = fetch_meta
    bbox_fetcher: Callable[..., gpd.GeoDataFrame] = fetch_bbox_with_geometry

    def load_scene_ids(self, manifest_path: str | Path) -> list[str]:
        return self.scene_loader(manifest_path)

    def fetch_metadata(self, tokens: Sequence[str]) -> pd.DataFrame:
        cursor_factory = self.hive_cursor_factory or hive_cursor
        return self.metadata_fetcher(tokens, cursor_factory=cursor_factory)

    def fetch_bbox_geometries(self, dataset_names: Sequence[str]) -> gpd.GeoDataFrame:
        return self.bbox_fetcher(
            dataset_names,
            self.engine,
            point_table=self.point_table,
        )
