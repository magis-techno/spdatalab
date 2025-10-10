from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from spdatalab.dataset.bbox import cli


class DummyTracker:
    def __init__(self, work_dir: str) -> None:
        self.work_dir = work_dir
        self.stats_requested = False

    def get_statistics(self) -> dict[str, object]:
        self.stats_requested = True
        return {
            "success_count": 2,
            "failed_count": 1,
            "failed_by_step": {"fetch_meta": 1},
        }

    def finalize(self) -> None:  # pragma: no cover - compatibility shim
        pass


def test_cli_show_stats(monkeypatch, caplog, tmp_path):
    tracker = DummyTracker(str(tmp_path))
    monkeypatch.setattr(cli, "LightweightProgressTracker", lambda work_dir: tracker)

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")

    with caplog.at_level("INFO", logger="spdatalab.dataset.bbox.cli"):
        exit_code = cli.main(["--input", str(manifest), "--show-stats"])

    assert exit_code == 0
    assert tracker.stats_requested is True
    assert "处理统计信息" in caplog.text
    assert "fetch_meta" in caplog.text


def test_cli_invokes_core_pipeline(monkeypatch, tmp_path, caplog):
    created = {}

    class DummyRepo:
        def __init__(self, *, engine, **kwargs):
            created["engine"] = engine
            created["repo_kwargs"] = kwargs

    def fake_tracker(work_dir):
        tracker = DummyTracker(work_dir)
        created["tracker"] = tracker
        return tracker

    def fake_create_engine(dsn, future):  # noqa: ARG001 - signature mirror
        created["dsn"] = dsn
        created["future"] = future
        return SimpleNamespace(dispose=lambda: created.setdefault("disposed", True))

    def fake_batch_insert(frame, engine, *, batch_size, tracker, batch_num):
        created["batch_insert_called_with"] = {
            "frame": frame,
            "engine": engine,
            "batch_size": batch_size,
            "tracker": tracker,
            "batch_num": batch_num,
        }
        return 42

    def fake_run(repo, manifest_path, config, **kwargs):
        created["repo"] = repo
        created["manifest_path"] = manifest_path
        created["config"] = config
        created["run_kwargs"] = kwargs
        writer = kwargs["batch_writer"]
        inserted = writer(
            SimpleNamespace(name="dummy"),
            batch_size=config.insert_batch_size,
            tracker=kwargs["tracker"],
            batch_num=3,
        )
        return {
            "total_scenes": 5,
            "processed_records": 10,
            "inserted_records": inserted,
            "completed_batches": 2,
            "interrupted": False,
        }

    def fake_setup_interrupt(flag, on_interrupt):
        created["interrupt_flag"] = flag
        created["on_interrupt"] = on_interrupt

    def fake_create_table(engine):
        created["create_table_engine"] = engine
        return True

    monkeypatch.setattr(cli, "BBoxDataRepository", DummyRepo)
    monkeypatch.setattr(cli, "LightweightProgressTracker", fake_tracker)
    monkeypatch.setattr(cli, "create_engine", fake_create_engine)
    monkeypatch.setattr(cli, "batch_insert_to_postgis", fake_batch_insert)
    monkeypatch.setattr(cli, "run_overlap_analysis", fake_run)
    monkeypatch.setattr(cli, "setup_interrupt_handlers", fake_setup_interrupt)
    monkeypatch.setattr(cli, "create_table_if_not_exists", fake_create_table)

    manifest = tmp_path / "manifest.json"
    manifest.write_text("{}", encoding="utf-8")

    with caplog.at_level("INFO", logger="spdatalab.dataset.bbox.cli"):
        exit_code = cli.main(
            [
                "--input",
                str(manifest),
                "--dsn",
                "postgresql://example",
                "--batch",
                "5",
                "--insert-batch",
                "17",
                "--retry-failed",
                "--create-table",
                "--work-dir",
                str(tmp_path),
            ]
        )

    assert exit_code == 0
    assert created["dsn"] == "postgresql://example"
    assert created["future"] is True
    assert "disposed" in created and created["disposed"] is True
    assert created["manifest_path"] == str(manifest)
    assert created["config"].batch_size == 5
    assert created["config"].insert_batch_size == 17
    assert created["config"].retry_failed is True
    assert created["tracker"].work_dir == str(tmp_path)
    assert created["batch_insert_called_with"]["batch_size"] == 17
    assert created["interrupt_flag"].is_set() is False
    assert "处理完成" in caplog.text
    assert "总场景数" in caplog.text


def test_batch_top1_dry_run(monkeypatch, caplog):
    calls = {}

    class DummyBatch:
        def __init__(self, engine):
            calls["engine"] = engine

        def inspect(self, config):
            calls["config"] = config

            class Inspection:
                available_cities = ["A001"]
                expected_rows = 3

                def __init__(self) -> None:
                    self.analysis_date = SimpleNamespace(isoformat=lambda: "2024-01-01")

            return Inspection()

    engine_stub = SimpleNamespace(dispose=lambda: None)
    monkeypatch.setattr(cli, "_open_engine", lambda dsn: engine_stub)
    monkeypatch.setattr(cli, "BBoxHotspotBatch", DummyBatch)

    with caplog.at_level("INFO", logger="spdatalab.dataset.bbox.cli"):
        exit_code = cli.main(
            [
                "batch-top1",
                "--dry-run",
            ]
        )

    assert exit_code == 0
    assert calls["engine"] is engine_stub
    assert calls["config"].output_table == "city_hotspots"
    assert "待处理城市" in caplog.text
