from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from spdatalab.dataset.bbox.core import (
    OverlapAnalysisConfig,
    ensure_bbox_geodataframe,
    merge_metadata_with_bboxes,
    run_overlap_analysis,
)


class DummyTracker:
    def __init__(self, *, failed_tokens: list[str] | None = None) -> None:
        self.failed_tokens = failed_tokens or []
        self.successes: list[list[str]] = []
        self.failures: list[tuple[str, str, int, str]] = []
        self.progress_calls: list[tuple[int, int, int, int]] = []
        self.checked_tokens: list[list[str]] = []
        self.finalized = False

    def load_failed_tokens(self) -> list[str]:
        return list(self.failed_tokens)

    def get_remaining_tokens(self, all_tokens):
        tokens = list(all_tokens)
        self.checked_tokens.append(tokens)
        return tokens

    def check_tokens_exist(self, tokens):
        return set()

    def save_failed_record(self, scene_token, error_msg, batch_num, fail_stage):
        self.failures.append((scene_token, str(error_msg), batch_num, fail_stage))

    def save_successful_batch(self, scene_tokens, batch_num):
        self.successes.append(list(scene_tokens))

    def save_progress(self, total, processed, inserted, batch_num):
        self.progress_calls.append((total, processed, inserted, batch_num))

    def finalize(self):
        self.finalized = True


class DummyRepo:
    def __init__(self):
        self.manifest_requests: list[str] = []
        self.metadata_requests: list[list[str]] = []
        self.bbox_requests: list[list[str]] = []
        self._metadata_source = {
            "scene_001": {
                "data_name": "dataset_a",
                "event_id": 1,
                "city_id": 100,
                "timestamp": "2024-01-01",
            },
            "scene_002": {
                "data_name": "dataset_b",
                "event_id": 2,
                "city_id": 200,
                "timestamp": "2024-01-02",
            },
            "scene_003": {
                "data_name": "dataset_c",
                "event_id": 3,
                "city_id": 300,
                "timestamp": "2024-01-03",
            },
        }
        self._bbox_source = {
            "dataset_a": {"xmin": 0.0, "ymin": 0.0, "xmax": 1.0, "ymax": 1.0, "all_good": True},
            "dataset_b": {"xmin": 1.0, "ymin": 1.0, "xmax": 2.0, "ymax": 2.0, "all_good": False},
            "dataset_c": {"xmin": 2.0, "ymin": 2.0, "xmax": 3.0, "ymax": 3.0, "all_good": True},
        }

    def load_scene_ids(self, manifest_path):
        self.manifest_requests.append(str(manifest_path))
        return list(self._metadata_source.keys())

    def fetch_metadata(self, tokens):
        requested = list(tokens)
        self.metadata_requests.append(requested)
        rows = []
        for token in requested:
            meta = self._metadata_source.get(token)
            if meta:
                rows.append({"scene_token": token, **meta})
        return pd.DataFrame(rows)

    def fetch_bbox_geometries(self, dataset_names):
        requested = list(dataset_names)
        self.bbox_requests.append(requested)
        rows = []
        for name in requested:
            bbox = self._bbox_source.get(name)
            if bbox:
                rows.append({"dataset_name": name, **bbox})
        return pd.DataFrame(rows)


def test_ensure_bbox_geodataframe_builds_polygons_and_points():
    frame = pd.DataFrame(
        {
            "dataset_name": ["dataset_a", "dataset_b"],
            "xmin": [0.0, 5.0],
            "ymin": [0.0, 5.0],
            "xmax": [1.0, 5.0],
            "ymax": [1.0, 5.0],
            "all_good": [True, False],
        }
    )

    result = ensure_bbox_geodataframe(frame)

    assert isinstance(result, gpd.GeoDataFrame)
    assert list(result["dataset_name"]) == ["dataset_a", "dataset_b"]
    assert result.crs == 4326
    assert result.geometry.iloc[0].geom_type == "Polygon"
    assert result.geometry.iloc[1].geom_type == "Point"


def test_ensure_bbox_geodataframe_preserves_existing_geometries():
    geo_frame = gpd.GeoDataFrame(
        {
            "dataset_name": ["dataset_c"],
            "all_good": [True],
        },
        geometry=[Point(0.0, 0.0)],
        crs=None,
    )

    coerced = ensure_bbox_geodataframe(geo_frame)

    assert isinstance(coerced, gpd.GeoDataFrame)
    assert coerced.crs == 4326
    assert coerced.geometry.iloc[0].geom_type == "Point"


def test_merge_metadata_with_bboxes_returns_joined_geodataframe():
    metadata = pd.DataFrame(
        {
            "scene_token": ["scene_1", "scene_2"],
            "data_name": ["dataset_a", "dataset_b"],
            "event_id": [1, 2],
            "city_id": [101, 202],
            "timestamp": ["2024-01-01", "2024-01-02"],
        }
    )
    bbox_frame = pd.DataFrame(
        {
            "dataset_name": ["dataset_a", "dataset_b"],
            "xmin": [0.0, 1.0],
            "ymin": [0.0, 1.0],
            "xmax": [1.0, 2.0],
            "ymax": [1.0, 2.0],
            "all_good": [True, False],
        }
    )

    merged = merge_metadata_with_bboxes(metadata, bbox_frame)

    assert isinstance(merged, gpd.GeoDataFrame)
    assert list(merged["scene_token"]) == ["scene_1", "scene_2"]
    assert list(merged["all_good"]) == [True, False]
    assert merged.crs == 4326
    assert merged.geometry.iloc[0].geom_type == "Polygon"


def test_merge_metadata_with_bboxes_handles_missing_matches():
    metadata = pd.DataFrame(
        {
            "scene_token": ["scene_3"],
            "data_name": ["dataset_x"],
            "event_id": [3],
            "city_id": [303],
            "timestamp": ["2024-02-01"],
        }
    )
    bbox_frame = pd.DataFrame(
        {
            "dataset_name": ["dataset_y"],
            "xmin": [0.0],
            "ymin": [0.0],
            "xmax": [1.0],
            "ymax": [1.0],
            "all_good": [True],
        }
    )

    merged = merge_metadata_with_bboxes(metadata, bbox_frame)

    assert merged.empty
    assert isinstance(merged, gpd.GeoDataFrame)
    assert merged.crs == 4326


def test_run_overlap_analysis_processes_batches_and_invokes_writer(tmp_path):
    repo = DummyRepo()
    tracker = DummyTracker()
    config = OverlapAnalysisConfig(
        batch_size=2,
        insert_batch_size=10,
        work_dir=str(tmp_path / "logs"),
    )

    writer_calls: list[tuple[int, int, int]] = []

    def writer(gdf, *, batch_size, tracker, batch_num):
        writer_calls.append((batch_num, batch_size, len(gdf)))
        assert batch_size == config.insert_batch_size
        assert tracker is tracker_instance
        return len(gdf)

    tracker_instance = tracker

    result = run_overlap_analysis(
        repo,
        "manifest.json",
        config,
        tracker=tracker,
        batch_writer=writer,
    )

    assert result == {
        "total_scenes": 3,
        "processed_records": 3,
        "inserted_records": 3,
        "completed_batches": 2,
        "interrupted": False,
    }
    assert writer_calls == [(1, 10, 2), (2, 10, 1)]
    assert tracker.successes == [["scene_001", "scene_002"], ["scene_003"]]
    assert tracker.failures == []
    assert tracker.finalized is True
    assert repo.manifest_requests == ["manifest.json"]
    assert repo.metadata_requests == [["scene_001", "scene_002"], ["scene_003"]]
    assert tracker.progress_calls[-1] == (3, 3, 3, 2)


def test_run_overlap_analysis_records_failures_when_metadata_missing(tmp_path):
    class EmptyMetadataRepo(DummyRepo):
        def fetch_metadata(self, tokens):
            self.metadata_requests.append(list(tokens))
            return pd.DataFrame(
                columns=["scene_token", "data_name", "event_id", "city_id", "timestamp"]
            )

    repo = EmptyMetadataRepo()
    tracker = DummyTracker()
    config = OverlapAnalysisConfig(batch_size=5, insert_batch_size=5, work_dir=str(tmp_path))

    result = run_overlap_analysis(repo, "manifest.json", config, tracker=tracker)

    assert result["processed_records"] == 0
    assert result["inserted_records"] == 0
    assert tracker.successes == []
    assert len(tracker.failures) == 3
    assert {failure[-1] for failure in tracker.failures} == {"fetch_meta"}


def test_run_overlap_analysis_respects_retry_failed_flag(tmp_path):
    repo = DummyRepo()
    tracker = DummyTracker(failed_tokens=["scene_002"])
    config = OverlapAnalysisConfig(
        batch_size=10,
        insert_batch_size=5,
        work_dir=str(tmp_path),
        retry_failed=True,
    )

    result = run_overlap_analysis(repo, "manifest.json", config, tracker=tracker)

    assert tracker.checked_tokens == []
    assert tracker.successes == [["scene_002"]]
    assert result["processed_records"] == 1
    assert result["inserted_records"] == 1
    assert result["completed_batches"] == 1
