"""Utilities for summarising bbox overlap analysis results."""

from __future__ import annotations

import json
import logging
import math
import time
from dataclasses import dataclass
from datetime import date
from typing import Iterable, Sequence

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

__all__ = [
    "BatchTop1Config",
    "BatchTop1Inspection",
    "BatchTop1Result",
    "BBoxHotspotBatch",
]


@dataclass(slots=True)
class BatchTop1Config:
    """Configuration for generating per-city hotspot summaries."""

    output_table: str
    cities: Sequence[str] | None = None
    max_cities: int | None = None
    top_n: int | None = None
    top_percent: float | None = 1.0
    results_table: str = "bbox_overlap_analysis_results"
    analysis_date: date | None = None

    def __post_init__(self) -> None:
        if self.top_n is not None:
            if self.top_n <= 0:
                raise ValueError("top_n must be a positive integer when provided")
            # ``argparse`` 默认仍会为互斥组的另一个参数设置默认值，这里清空它。
            self.top_percent = None
        if self.top_percent is not None:
            if self.top_percent <= 0 or self.top_percent > 100:
                raise ValueError("top_percent must fall within (0, 100]")
        if not self.output_table:
            raise ValueError("output_table must not be empty")


@dataclass(slots=True)
class BatchTop1Inspection:
    """Lightweight metadata about a prospective batch run."""

    available_cities: list[str]
    analysis_date: date
    expected_rows: int


@dataclass(slots=True)
class BatchTop1Result:
    """Aggregated information about the executed batch."""

    successful_cities: list[str]
    failed_cities: list[str]
    extracted_rows: int
    elapsed_seconds: float


class BBoxHotspotBatch:
    """Coordinate the extraction of hotspot summaries from analysis results."""

    def __init__(self, engine: Engine, *, logger: logging.Logger | None = None) -> None:
        self.engine = engine
        self.logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------ helpers
    def _load_results(self, table: str) -> pd.DataFrame:
        query = text(
            f"""
            SELECT
                analysis_id,
                analysis_params,
                overlap_count,
                subdataset_count,
                scene_count,
                total_overlap_area,
                geometry,
                hotspot_rank,
                analysis_time
            FROM {table}
            """
        )
        with self.engine.begin() as conn:
            frame = pd.read_sql_query(query, conn)
        if frame.empty:
            return frame

        frame["analysis_time"] = pd.to_datetime(frame["analysis_time"], errors="coerce")
        params = frame["analysis_params"].apply(self._safe_parse_params)
        frame["city_id"] = params.apply(lambda payload: payload.get("city_filter"))
        frame["grid_coords"] = params.apply(lambda payload: payload.get("grid_coords"))
        return frame.dropna(subset=["city_id"])

    @staticmethod
    def _safe_parse_params(raw: str | dict | None) -> dict[str, object]:
        if isinstance(raw, dict):
            return raw
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return {}

    def _resolve_cities(self, config: BatchTop1Config, frame: pd.DataFrame) -> list[str]:
        if config.cities:
            unique = list(dict.fromkeys(config.cities))
        else:
            grouped = frame.groupby("city_id")["overlap_count"].sum().sort_values(ascending=False)
            unique = grouped.index.tolist()
        if config.max_cities is not None:
            unique = unique[: config.max_cities]
        return unique

    def _ensure_summary_table(self, table: str) -> None:
        dialect = self.engine.dialect.name
        if dialect == "postgresql":
            ddl = text(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    city_id TEXT NOT NULL,
                    analysis_id TEXT,
                    bbox_count INTEGER,
                    subdataset_count INTEGER,
                    scene_count INTEGER,
                    total_overlap_area DOUBLE PRECISION,
                    geometry TEXT,
                    grid_coords TEXT,
                    analysis_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
        else:
            ddl = text(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city_id TEXT NOT NULL,
                    analysis_id TEXT,
                    bbox_count INTEGER,
                    subdataset_count INTEGER,
                    scene_count INTEGER,
                    total_overlap_area REAL,
                    geometry TEXT,
                    grid_coords TEXT,
                    analysis_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

        with self.engine.begin() as conn:
            conn.execute(ddl)

    @staticmethod
    def _latest_slice(frame: pd.DataFrame, city_id: str, day: date) -> pd.DataFrame:
        city_frame = frame[frame["city_id"] == city_id]
        if city_frame.empty:
            return city_frame
        city_frame = city_frame[city_frame["analysis_time"].dt.date == day]
        if city_frame.empty:
            return city_frame
        latest_time = city_frame["analysis_time"].max()
        return city_frame[city_frame["analysis_time"] == latest_time]

    def _determine_window(self, city_frame: pd.DataFrame, config: BatchTop1Config) -> pd.DataFrame:
        if city_frame.empty:
            return city_frame
        city_frame = city_frame.sort_values(
            by=["hotspot_rank", "overlap_count"],
            ascending=[True, False],
        )
        if config.top_n is not None:
            limit = config.top_n
        else:
            percent = (config.top_percent or 0) / 100
            limit = max(1, math.ceil(len(city_frame) * percent))
        return city_frame.head(limit)

    def _write_summary(self, table: str, rows: pd.DataFrame) -> int:
        if rows.empty:
            return 0
        payload = rows[
            [
                "city_id",
                "analysis_id",
                "overlap_count",
                "subdataset_count",
                "scene_count",
                "total_overlap_area",
                "geometry",
                "grid_coords",
                "analysis_time",
            ]
        ].rename(columns={"overlap_count": "bbox_count"})

        with self.engine.begin() as conn:
            payload.to_sql(table, conn, if_exists="append", index=False)
        return len(payload)

    def _clear_existing(self, table: str, cities: Iterable[str], day: date) -> None:
        if not cities:
            return
        placeholders = ",".join([":city_" + str(index) for index, _ in enumerate(cities)])
        params = {f"city_{index}": city for index, city in enumerate(cities)}
        params["target_day"] = day.isoformat()
        sql = text(
            f"""
            DELETE FROM {table}
            WHERE city_id IN ({placeholders})
              AND DATE(analysis_time) = DATE(:target_day)
            """
        )
        with self.engine.begin() as conn:
            conn.execute(sql, params)

    # ----------------------------------------------------------------- inspection
    def inspect(self, config: BatchTop1Config) -> BatchTop1Inspection:
        frame = self._load_results(config.results_table)
        analysis_day = config.analysis_date or (frame["analysis_time"].dt.date.max() if not frame.empty else date.today())
        if frame.empty:
            return BatchTop1Inspection([], analysis_day, 0)
        cities = self._resolve_cities(config, frame)
        total_rows = 0
        for city in cities:
            slice_frame = self._latest_slice(frame, city, analysis_day)
            total_rows += len(self._determine_window(slice_frame, config))
        return BatchTop1Inspection(cities, analysis_day, total_rows)

    # ----------------------------------------------------------------------- main
    def run(self, config: BatchTop1Config) -> BatchTop1Result:
        start = time.perf_counter()
        frame = self._load_results(config.results_table)
        if frame.empty:
            return BatchTop1Result([], [], 0, time.perf_counter() - start)

        analysis_day = config.analysis_date or frame["analysis_time"].dt.date.max()
        cities = self._resolve_cities(config, frame)
        if not cities:
            return BatchTop1Result([], [], 0, time.perf_counter() - start)

        self._ensure_summary_table(config.output_table)
        self._clear_existing(config.output_table, cities, analysis_day)

        successful: list[str] = []
        failed: list[str] = []
        collected: list[pd.DataFrame] = []

        for city in cities:
            slice_frame = self._latest_slice(frame, city, analysis_day)
            if slice_frame.empty:
                self.logger.warning("城市 %s 在 %s 没有可用的分析结果", city, analysis_day)
                failed.append(city)
                continue

            window = self._determine_window(slice_frame, config)
            if window.empty:
                self.logger.warning("城市 %s 缺少热点记录", city)
                failed.append(city)
                continue

            collected.append(window)
            successful.append(city)

        if not collected:
            return BatchTop1Result(successful, failed, 0, time.perf_counter() - start)

        combined = pd.concat(collected, ignore_index=True)
        inserted = self._write_summary(config.output_table, combined)

        elapsed = time.perf_counter() - start
        return BatchTop1Result(successful, failed, inserted, elapsed)

