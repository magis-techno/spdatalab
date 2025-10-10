"""
Core analytics logic for bbox workflows.

The functions defined here will gradually replace the legacy implementations
that previously lived in ``spdatalab.dataset.bbox``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Iterator, Protocol, Sequence, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    import geopandas as gpd
    import pandas as pd

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, box

from .pipeline import InterruptFlag, LightweightProgressTracker

__all__ = [
    "BBoxRepository",
    "OverlapAnalysisConfig",
    "chunk",
    "ensure_bbox_geodataframe",
    "merge_metadata_with_bboxes",
    "run_overlap_analysis",
]

T_co = TypeVar("T_co", covariant=False)


class BBoxRepository(Protocol):
    """Abstracts data access for bbox scenes so the core logic stays testable."""

    def load_scene_ids(self, manifest_path: str | Path) -> list[str]:
        ...

    def fetch_metadata(self, tokens: Sequence[str]) -> "pd.DataFrame":
        ...

    def fetch_bbox_geometries(self, dataset_names: Sequence[str]) -> "gpd.GeoDataFrame":
        ...


@dataclass(slots=True)
class OverlapAnalysisConfig:
    """Carries the parameters for running an overlap analysis batch."""

    batch_size: int
    insert_batch_size: int
    work_dir: str = "./bbox_import_logs"
    retry_failed: bool = False


def chunk(items: Sequence[T_co], size: int) -> Iterator[Sequence[T_co]]:
    """
    Yield ``items`` in evenly sized blocks (mirrors the legacy ``chunk`` helper).
    """

    if size <= 0:
        raise ValueError("chunk size must be a positive integer")

    for index in range(0, len(items), size):
        yield items[index : index + size]


def ensure_bbox_geodataframe(
    frame: "pd.DataFrame | gpd.GeoDataFrame",
) -> gpd.GeoDataFrame:
    """Coerce ``frame`` into a GeoDataFrame exposing a ``geometry`` column."""

    if isinstance(frame, gpd.GeoDataFrame):
        if frame.crs is None:
            return frame.set_crs(4326)
        return frame

    if "geometry" in frame.columns:
        geo_frame = gpd.GeoDataFrame(frame, geometry="geometry", crs=4326)
        if geo_frame.crs is None:
            return geo_frame.set_crs(4326)
        return geo_frame

    required = {"xmin", "ymin", "xmax", "ymax"}
    if not required.issubset(frame.columns):
        missing = ", ".join(sorted(required.difference(frame.columns)))
        raise KeyError(f"Missing expected bbox columns: {missing}")

    geometries = []
    for row in frame.itertuples():
        if row.xmin == row.xmax or row.ymin == row.ymax:
            geometries.append(
                Point((row.xmin + row.xmax) / 2, (row.ymin + row.ymax) / 2)
            )
        else:
            geometries.append(box(row.xmin, row.ymin, row.xmax, row.ymax))

    enriched = frame.copy()
    enriched["geometry"] = geometries
    geo_frame = gpd.GeoDataFrame(enriched, geometry="geometry", crs=4326)
    if geo_frame.crs is None:
        return geo_frame.set_crs(4326)
    return geo_frame


def merge_metadata_with_bboxes(
    metadata: pd.DataFrame, bbox_frame: "pd.DataFrame | gpd.GeoDataFrame"
) -> gpd.GeoDataFrame:
    """Return the metadata merged with bbox geometries as a GeoDataFrame."""

    if metadata.empty:
        return gpd.GeoDataFrame(
            {
                "scene_token": [],
                "data_name": [],
                "event_id": [],
                "city_id": [],
                "timestamp": [],
                "all_good": [],
            },
            geometry=gpd.GeoSeries([], crs=4326),
            crs=4326,
        )

    bbox_gdf = ensure_bbox_geodataframe(bbox_frame)
    if bbox_gdf.empty:
        return gpd.GeoDataFrame(
            {
                "scene_token": [],
                "data_name": [],
                "event_id": [],
                "city_id": [],
                "timestamp": [],
                "all_good": [],
            },
            geometry=gpd.GeoSeries([], crs=bbox_gdf.crs or 4326),
            crs=bbox_gdf.crs or 4326,
        )

    merged = metadata.merge(
        bbox_gdf,
        left_on="data_name",
        right_on="dataset_name",
        how="inner",
    )

    if merged.empty:
        return gpd.GeoDataFrame(
            {
                "scene_token": [],
                "data_name": [],
                "event_id": [],
                "city_id": [],
                "timestamp": [],
                "all_good": [],
            },
            geometry=gpd.GeoSeries([], crs=bbox_gdf.crs or 4326),
            crs=bbox_gdf.crs or 4326,
        )

    result = merged[
        [
            "scene_token",
            "data_name",
            "event_id",
            "city_id",
            "timestamp",
            "all_good",
            "geometry",
        ]
    ].copy()

    geo_result = gpd.GeoDataFrame(result, geometry="geometry", crs=bbox_gdf.crs or 4326)
    if geo_result.crs is None:
        return geo_result.set_crs(bbox_gdf.crs or 4326)
    return geo_result


def run_overlap_analysis(
    repo: BBoxRepository,
    manifest_path: str | Path,
    config: OverlapAnalysisConfig,
    *,
    tracker: LightweightProgressTracker | None = None,
    batch_writer: Callable[..., int] | None = None,
    interrupt_flag: InterruptFlag | None = None,
) -> dict[str, int]:
    """
    Entry point for the refactored overlap analysis.

    Parameters
    ----------
    repo:
        Provides access to scene metadata and bounding boxes.
    manifest_path:
        Path to the manifest file containing the scene identifiers to process.
    config:
        Tunable execution knobs (batch sizes, workspace paths, etc.).
    tracker:
        Optional progress tracker. Defaults to ``LightweightProgressTracker``
        configured with :attr:`OverlapAnalysisConfig.work_dir`.
    batch_writer:
        Optional callable invoked with the merged GeoDataFrame for each batch.
        When omitted the function behaves as an in-memory run and simply counts
        the processed rows.
    interrupt_flag:
        Flag that indicates when execution should stop early. A fresh
        ``InterruptFlag`` is created when one is not supplied.
    """

    tracker = tracker or LightweightProgressTracker(config.work_dir)
    interrupt_flag = interrupt_flag or InterruptFlag()

    total_processed = 0
    total_inserted = 0
    total_scenes = 0
    last_batch = 0

    try:
        scene_ids = repo.load_scene_ids(manifest_path)
        total_scenes = len(scene_ids)

        if config.retry_failed:
            tokens = tracker.load_failed_tokens()
        else:
            tokens = tracker.get_remaining_tokens(scene_ids)

        if not tokens:
            print("没有找到需要处理的scene_id")
            tracker.save_progress(total_scenes, 0, 0, 0)
            return {
                "total_scenes": total_scenes,
                "processed_records": 0,
                "inserted_records": 0,
                "completed_batches": 0,
                "interrupted": False,
            }

        print(f"开始处理 {len(tokens)} 个场景，批次大小: {config.batch_size}")

        for batch_num, token_batch in enumerate(chunk(tokens, config.batch_size), 1):
            last_batch = batch_num

            if interrupt_flag.is_set():
                print(f"\n程序被中断，已处理 {batch_num - 1} 个批次")
                break

            print(f"[批次 {batch_num}] 处理 {len(token_batch)} 个场景")

            existing = tracker.check_tokens_exist(token_batch)
            if existing:
                print(f"[批次 {batch_num}] 跳过 {len(existing)} 个已处理的记录")
            token_batch = [token for token in token_batch if token not in existing]

            if not token_batch:
                continue

            try:
                metadata = repo.fetch_metadata(token_batch)
            except Exception as exc:  # pragma: no cover - defensive
                print(f"[批次 {batch_num}] 获取元数据失败: {exc}")
                for token in token_batch:
                    tracker.save_failed_record(
                        token,
                        f"获取元数据异常: {exc}",
                        batch_num,
                        "fetch_meta",
                    )
                continue

            if metadata.empty:
                print(f"[批次 {batch_num}] 没有找到元数据，跳过")
                for token in token_batch:
                    tracker.save_failed_record(
                        token,
                        "无法获取元数据",
                        batch_num,
                        "fetch_meta",
                    )
                continue

            if interrupt_flag.is_set():
                print(f"\n程序被中断，正在处理批次 {batch_num}")
                break

            try:
                bbox_frame = repo.fetch_bbox_geometries(metadata.data_name.tolist())
            except Exception as exc:  # pragma: no cover - defensive
                print(f"[批次 {batch_num}] 获取边界框失败: {exc}")
                for token in metadata.scene_token.tolist():
                    tracker.save_failed_record(
                        token,
                        f"获取边界框异常: {exc}",
                        batch_num,
                        "fetch_bbox",
                    )
                continue

            if getattr(bbox_frame, "empty", True):
                print(f"[批次 {batch_num}] 没有找到边界框数据，跳过")
                for token in metadata.scene_token.tolist():
                    tracker.save_failed_record(
                        token,
                        "无法获取边界框数据",
                        batch_num,
                        "fetch_bbox",
                    )
                continue

            if interrupt_flag.is_set():
                print(f"\n程序被中断，正在处理批次 {batch_num}")
                break

            try:
                merged = merge_metadata_with_bboxes(metadata, bbox_frame)
            except Exception as exc:  # pragma: no cover - defensive
                print(f"[批次 {batch_num}] 数据合并失败: {exc}")
                for token in metadata.scene_token.tolist():
                    tracker.save_failed_record(
                        token,
                        f"数据合并异常: {exc}",
                        batch_num,
                        "data_merge",
                    )
                continue

            if merged.empty:
                print(f"[批次 {batch_num}] 合并后数据为空，跳过")
                for token in metadata.scene_token.tolist():
                    tracker.save_failed_record(
                        token,
                        "元数据与边界框数据无法匹配",
                        batch_num,
                        "data_merge",
                    )
                continue

            try:
                inserted = (
                    len(merged)
                    if batch_writer is None
                    else batch_writer(
                        merged,
                        batch_size=config.insert_batch_size,
                        tracker=tracker,
                        batch_num=batch_num,
                    )
                )
            except Exception as exc:  # pragma: no cover - defensive
                print(f"[批次 {batch_num}] 插入数据库失败: {exc}")
                for token in merged.scene_token.tolist():
                    tracker.save_failed_record(
                        token,
                        f"批量插入异常: {exc}",
                        batch_num,
                        "batch_insert",
                    )
                continue

            total_processed += len(merged)
            total_inserted += inserted

            tracker.save_successful_batch(merged.scene_token.tolist(), batch_num)
            tracker.save_progress(
                total_scenes,
                total_processed,
                total_inserted,
                batch_num,
            )

        interrupted = interrupt_flag.is_set()
        return {
            "total_scenes": total_scenes,
            "processed_records": total_processed,
            "inserted_records": total_inserted,
            "completed_batches": last_batch,
            "interrupted": interrupted,
        }
    finally:
        tracker.finalize()
        tracker.save_progress(
            total_scenes,
            total_processed,
            total_inserted,
            last_batch,
        )
