"""Command line interface for the bbox analysis workflows."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Callable, Sequence

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from .core import OverlapAnalysisConfig, run_overlap_analysis
from .io import BBoxDataRepository
from .pipeline import InterruptFlag, LightweightProgressTracker, setup_interrupt_handlers
from .legacy import batch_insert_to_postgis, create_table_if_not_exists
from .summary import BatchTop1Config, BBoxHotspotBatch

DEFAULT_LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
SUPPORTED_MANIFEST_SUFFIXES = {".json", ".parquet", ".txt"}


def _resolve_dsn(explicit: str | None) -> str:
    """Return the SQLAlchemy DSN using CLI, environment or default value."""

    if explicit:
        return explicit
    env_dsn = os.environ.get("LOCAL_DSN")
    if env_dsn:
        return env_dsn
    return DEFAULT_LOCAL_DSN


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:  # pragma: no cover - defensive path
        raise argparse.ArgumentTypeError("需要正整数") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("需要正整数")
    return parsed


def _percentage(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:  # pragma: no cover - defensive path
        raise argparse.ArgumentTypeError("需要数值百分比") from exc
    if not 0 < parsed <= 100:
        raise argparse.ArgumentTypeError("百分比需在 0-100 范围内")
    return parsed


def _manifest_path(value: str) -> str:
    path = Path(value)
    if not path.exists():
        raise argparse.ArgumentTypeError(f"输入文件不存在: {value}")
    if path.suffix.lower() not in SUPPORTED_MANIFEST_SUFFIXES:
        supported = ", ".join(sorted(SUPPORTED_MANIFEST_SUFFIXES))
        raise argparse.ArgumentTypeError(
            f"暂不支持的输入格式: {path.suffix or '<无扩展名>'}，支持: {supported}"
        )
    return str(path)


def _print_statistics(tracker: LightweightProgressTracker, logger: logging.Logger) -> None:
    """Print cached processing statistics and exit."""

    stats = tracker.get_statistics()
    logger.info("=== 处理统计信息 ===")
    logger.info("成功处理: %s 个场景", stats["success_count"])
    logger.info("失败场景: %s 个", stats["failed_count"])

    failed_by_step = stats.get("failed_by_step", {})
    if failed_by_step:
        logger.info("按步骤分类的失败统计:")
        for step, count in failed_by_step.items():
            logger.info("  %s: %s 个", step, count)


def _make_batch_writer(engine) -> Callable[..., int]:
    """Return a batch writer bound to the provided engine."""

    def _writer(frame, *, batch_size: int, tracker, batch_num: int) -> int:
        return batch_insert_to_postgis(
            frame,
            engine,
            batch_size=batch_size,
            tracker=tracker,
            batch_num=batch_num,
        )

    return _writer


def _default_interrupt_message(signum: int, _frame: object | None) -> None:
    try:
        sig_name = signal.Signals(signum).name
    except ValueError:  # pragma: no cover - fallback for unexpected signals
        sig_name = str(signum)
    logging.getLogger(__name__).warning(
        "收到中断信号 (%s)，正在尝试优雅退出...", sig_name
    )


def _build_overlap_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="从数据集文件生成边界框重叠分析结果",
    )
    parser.add_argument("--log-level", default="INFO", help="日志级别，例如 INFO 或 DEBUG")
    parser.add_argument(
        "--input",
        required=True,
        type=_manifest_path,
        help="输入文件路径（支持 JSON / Parquet / 文本格式）",
    )
    parser.add_argument(
        "--dsn",
        help=(
            "SQLAlchemy 数据库连接字符串；默认读取 LOCAL_DSN 环境变量，"
            "若未设置则使用本地 PostgreSQL 的默认值"
        ),
    )
    parser.add_argument(
        "--batch",
        type=_positive_int,
        default=1000,
        help="处理批次大小",
    )
    parser.add_argument(
        "--insert-batch",
        type=_positive_int,
        default=1000,
        help="插入数据库时的批次大小",
    )
    parser.add_argument(
        "--create-table",
        action="store_true",
        help="若目标表不存在则尝试创建（使用 legacy 逻辑）",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="只重试历史执行中失败的 scene_token",
    )
    parser.add_argument(
        "--work-dir",
        default="./bbox_import_logs",
        help="日志与进度文件存放目录",
    )
    parser.add_argument(
        "--show-stats",
        action="store_true",
        help="仅查看已有统计信息并退出",
    )
    return parser


def _build_batch_top1_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="汇总 bbox 分析结果并提取热点城市",
    )
    parser.add_argument("--log-level", default="INFO", help="日志级别，例如 INFO 或 DEBUG")
    parser.add_argument(
        "--dsn",
        help=(
            "SQLAlchemy 数据库连接字符串；默认读取 LOCAL_DSN 环境变量，"
            "若未设置则使用本地 PostgreSQL 的默认值"
        ),
    )
    parser.add_argument(
        "--output-table",
        default="city_hotspots",
        help="输出汇总表名 (默认: city_hotspots)",
    )
    parser.add_argument(
        "--cities",
        nargs="+",
        help="指定分析的城市列表，如: --cities A263 B001",
    )
    parser.add_argument(
        "--max-cities",
        type=_positive_int,
        help="最多分析城市数量 (默认: 无限制)",
    )
    result_group = parser.add_mutually_exclusive_group()
    result_group.add_argument(
        "--top-n",
        type=_positive_int,
        help="返回的热点数量（与 --top-percent 互斥）",
    )
    result_group.add_argument(
        "--top-percent",
        type=_percentage,
        default=1.0,
        help="返回最密集的前 X% 网格（默认 1%）",
    )
    parser.add_argument(
        "--results-table",
        default="bbox_overlap_analysis_results",
        help="历史结果所在的数据表 (默认: bbox_overlap_analysis_results)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅统计城市及数量，不写入数据库",
    )
    return parser


def _open_engine(dsn: str) -> Engine:
    engine = create_engine(dsn, future=True)
    return engine


def _run_overlap(args: argparse.Namespace, logger: logging.Logger) -> int:
    tracker = LightweightProgressTracker(args.work_dir)

    if args.show_stats:
        _print_statistics(tracker, logger)
        return 0

    manifest_path = args.input
    dsn = _resolve_dsn(args.dsn)

    logger.info("开始处理输入文件: %s", manifest_path)
    logger.info("工作目录: %s", tracker.work_dir)
    logger.info("数据库连接: %s", dsn)

    engine = _open_engine(dsn)

    if args.create_table:
        if not create_table_if_not_exists(engine):
            logger.error("创建表失败，退出")
            return 1

    repo = BBoxDataRepository(engine=engine)

    interrupt_flag = InterruptFlag()
    setup_interrupt_handlers(interrupt_flag, on_interrupt=_default_interrupt_message)

    config = OverlapAnalysisConfig(
        batch_size=args.batch,
        insert_batch_size=args.insert_batch,
        work_dir=args.work_dir,
        retry_failed=args.retry_failed,
    )

    try:
        result = run_overlap_analysis(
            repo,
            manifest_path,
            config,
            tracker=tracker,
            batch_writer=_make_batch_writer(engine),
            interrupt_flag=interrupt_flag,
        )
    finally:
        engine.dispose()

    logger.info("=== 处理完成 ===")
    logger.info("总场景数: %s", result["total_scenes"])
    logger.info("已处理记录: %s", result["processed_records"])
    logger.info("已插入记录: %s", result["inserted_records"])
    logger.info("完成批次数: %s", result["completed_batches"])
    if result["interrupted"]:
        logger.warning("状态: 已被中断")
    else:
        logger.info("状态: 正常完成")

    return 0


def _run_batch_top1(args: argparse.Namespace, logger: logging.Logger) -> int:
    dsn = _resolve_dsn(args.dsn)
    engine = _open_engine(dsn)

    config = BatchTop1Config(
        output_table=args.output_table,
        cities=args.cities,
        max_cities=args.max_cities,
        top_n=args.top_n,
        top_percent=args.top_percent,
        results_table=args.results_table,
    )

    batch_runner = BBoxHotspotBatch(engine)

    try:
        if args.dry_run:
            stats = batch_runner.inspect(config)
            logger.info("待处理城市: %s", ", ".join(stats.available_cities))
            logger.info("分析日期: %s", stats.analysis_date.isoformat())
            logger.info("预计写入 %s 条记录", stats.expected_rows)
            return 0

        result = batch_runner.run(config)
    finally:
        engine.dispose()

    if result.successful_cities:
        logger.info("成功分析的城市: %s", ", ".join(result.successful_cities))
    if result.failed_cities:
        logger.warning("分析失败的城市: %s", ", ".join(result.failed_cities))

    logger.info("写入热点记录: %s", result.extracted_rows)
    logger.info("总耗时: %.2f 秒", result.elapsed_seconds)

    return 0 if not result.failed_cities else 1


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch CLI commands for bbox analysis workflows."""

    argv_list = list(sys.argv[1:] if argv is None else argv)
    command = "overlap"
    if argv_list and argv_list[0] in {"overlap", "batch-top1"}:
        command = argv_list.pop(0)

    if command == "batch-top1":
        parser = _build_batch_top1_parser()
    else:
        parser = _build_overlap_parser()

    args = parser.parse_args(argv_list)
    _configure_logging(args.log_level)
    logger = logging.getLogger("spdatalab.dataset.bbox.cli")

    if command == "batch-top1":
        return _run_batch_top1(args, logger)
    return _run_overlap(args, logger)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
