from __future__ import annotations

import argparse
import logging
import runpy
import sys
from pathlib import Path

import pytest

from spdatalab.dataset.multimodal_data_retriever import APIConfig
from spdatalab.fusion.cli import multimodal as cli
from spdatalab.fusion.multimodal_trajectory_retrieval import MultimodalConfig


@pytest.fixture
def api_config() -> APIConfig:
    return APIConfig(
        project="proj",
        api_key="key",
        username="user",
        platform="platform",
        region="region",
        entrypoint_version="v2",
        api_base_url="https://api.example.com",
        api_path="/retrieve",
        timeout=30,
        max_retries=3,
    )


def test_validate_args_rejects_invalid_count() -> None:
    args = argparse.Namespace(
        count=10001,
        start=0,
        start_time=None,
        end_time=None,
        buffer_distance=1.0,
        overlap_threshold=0.5,
    )

    with pytest.raises(ValueError):
        cli.validate_args(args)


def test_main_returns_error_code_for_invalid_arguments() -> None:
    def failing_loader() -> APIConfig:
        raise AssertionError("loader should not be called for invalid args")

    exit_code = cli.main(
        ["--text", "query", "--collection", "ddi_collection", "--count", "0"],
        api_config_loader=failing_loader,
    )

    assert exit_code == 2


def test_main_success_writes_results(tmp_path: Path, api_config: APIConfig, capsys: pytest.CaptureFixture[str]) -> None:
    captured_config: dict[str, MultimodalConfig] = {}
    captured_query: dict[str, object] = {}

    def workflow_factory(config: MultimodalConfig):
        captured_config["value"] = config

        class _Workflow:
            def process_text_query(self, **kwargs):
                captured_query.update(kwargs)
                return {
                    "success": True,
                    "summary": {
                        "optimization_ratio": "0.80",
                        "total_points": 42,
                        "unique_datasets": 3,
                        "polygon_sources": 2,
                    },
                    "stats": {
                        "query_type": "text",
                        "query_content": kwargs["text"],
                        "collection": kwargs["collection"],
                        "search_results_count": kwargs["count"],
                        "aggregated_datasets": 1,
                        "total_duration": 1.23,
                        "config": {
                            "buffer_distance": config.buffer_distance,
                            "time_window_days": config.time_window_days,
                            "overlap_threshold": config.overlap_threshold,
                        },
                        "dataset_details": {"dataset_a": 5},
                    },
                }

        return _Workflow()

    output_path = tmp_path / "results.json"
    exit_code = cli.main(
        [
            "--text",
            "query",
            "--collection",
            "ddi_collection_camera_encoded_1",
            "--count",
            "3",
            "--start",
            "1",
            "--output-json",
            str(output_path),
        ],
        api_config_loader=lambda: api_config,
        workflow_factory=workflow_factory,
    )

    assert exit_code == 0
    assert captured_config["value"].buffer_distance == 10.0
    assert captured_query == {
        "text": "query",
        "collection": "ddi_collection_camera_encoded_1",
        "count": 3,
        "start": 1,
        "start_time": None,
        "end_time": None,
    }
    assert output_path.exists()

    saved = output_path.read_text(encoding="utf-8")
    assert "summary" in saved

    std = capsys.readouterr().out
    assert "多模态轨迹检索结果摘要" in std


def test_main_handles_api_config_error() -> None:
    def failing_loader() -> APIConfig:
        raise RuntimeError("boom")

    exit_code = cli.main(
        ["--text", "query", "--collection", "ddi_collection"],
        api_config_loader=failing_loader,
    )

    assert exit_code == 1


def test_get_api_config_from_env_logs_missing_env(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    def fake_from_env(cls):  # type: ignore[no-untyped-def]
        raise RuntimeError("missing MULTIMODAL_API_KEY")

    monkeypatch.setattr(cli.APIConfig, "from_env", classmethod(fake_from_env))

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError):
            cli.get_api_config_from_env()

    assert "MULTIMODAL_API_KEY" in caplog.text
    assert "MULTIMODAL_USERNAME" in caplog.text


def test_module_entry_point_invokes_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    called = {}

    def fake_main(argv=None, **_kwargs):
        called["called"] = True
        return 3

    monkeypatch.setattr("spdatalab.fusion.cli.multimodal.main", fake_main)
    monkeypatch.delitem(sys.modules, "spdatalab.fusion.multimodal_trajectory_retrieval", raising=False)

    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module(
            "spdatalab.fusion.multimodal_trajectory_retrieval",
            run_name="__main__",
        )

    assert called["called"] is True
    assert excinfo.value.code == 3
