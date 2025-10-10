from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from spdatalab.dataset.bbox.summary import BatchTop1Config, BBoxHotspotBatch


def _prepare_engine():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE bbox_overlap_analysis_results (
                    analysis_id TEXT,
                    analysis_params TEXT,
                    overlap_count INTEGER,
                    subdataset_count INTEGER,
                    scene_count INTEGER,
                    total_overlap_area REAL,
                    geometry TEXT,
                    hotspot_rank INTEGER,
                    analysis_time TEXT
                )
                """
            )
        )
    return engine


def _insert_sample_rows(engine):
    rows = [
        {
            "analysis_id": "run-1",
            "analysis_params": '{"city_filter": "A001", "grid_coords": "(1,2)"}',
            "overlap_count": 12,
            "subdataset_count": 3,
            "scene_count": 5,
            "total_overlap_area": 1.5,
            "geometry": "POLYGON(...)",
            "hotspot_rank": 1,
            "analysis_time": datetime(2024, 1, 1, 8, 0, 0).isoformat(),
        },
        {
            "analysis_id": "run-1",
            "analysis_params": '{"city_filter": "A001", "grid_coords": "(2,3)"}',
            "overlap_count": 8,
            "subdataset_count": 3,
            "scene_count": 5,
            "total_overlap_area": 1.1,
            "geometry": "POLYGON(...)",
            "hotspot_rank": 2,
            "analysis_time": datetime(2024, 1, 1, 8, 0, 0).isoformat(),
        },
        {
            "analysis_id": "run-2",
            "analysis_params": '{"city_filter": "B009", "grid_coords": "(4,5)"}',
            "overlap_count": 4,
            "subdataset_count": 1,
            "scene_count": 2,
            "total_overlap_area": 0.5,
            "geometry": "POLYGON(...)",
            "hotspot_rank": 1,
            "analysis_time": datetime(2024, 1, 1, 9, 0, 0).isoformat(),
        },
    ]
    pd.DataFrame(rows).to_sql(
        "bbox_overlap_analysis_results",
        engine,
        if_exists="append",
        index=False,
    )


def test_batch_top1_summary(tmp_path):
    engine = _prepare_engine()
    _insert_sample_rows(engine)

    batch = BBoxHotspotBatch(engine)
    config = BatchTop1Config(output_table="city_hotspots", top_percent=50.0)

    inspection = batch.inspect(config)
    assert inspection.available_cities == ["A001", "B009"]
    assert inspection.expected_rows == 2

    result = batch.run(config)
    assert result.successful_cities == ["A001", "B009"]
    assert result.failed_cities == []
    assert result.extracted_rows == 2

    with engine.begin() as conn:
        df = pd.read_sql_query(text("SELECT * FROM city_hotspots ORDER BY city_id"), conn)
    assert list(df["city_id"]) == ["A001", "B009"]
    assert list(df["bbox_count"]) == [12, 4]


def test_batch_top1_config_validation():
    with pytest.raises(ValueError):
        BatchTop1Config(output_table="", top_percent=10)

    with pytest.raises(ValueError):
        BatchTop1Config(output_table="tbl", top_n=0)

    with pytest.raises(ValueError):
        BatchTop1Config(output_table="tbl", top_percent=0)

    config = BatchTop1Config(output_table="tbl", top_n=3, top_percent=25)
    assert config.top_percent is None
    assert config.top_n == 3


def test_safe_parse_params_handles_edge_cases():
    assert BBoxHotspotBatch._safe_parse_params(None) == {}
    assert BBoxHotspotBatch._safe_parse_params({"foo": "bar"}) == {"foo": "bar"}
    assert BBoxHotspotBatch._safe_parse_params("{not valid}") == {}

    payload = BBoxHotspotBatch._safe_parse_params('{"city_filter": "A263"}')
    assert payload == {"city_filter": "A263"}
