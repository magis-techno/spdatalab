"""Command line interface for the bbox overlap analysis pipeline."""

from __future__ import annotations

import argparse
import os
import signal
import sys
from typing import Callable

from sqlalchemy import create_engine

from .core import OverlapAnalysisConfig, run_overlap_analysis
from .io import BBoxDataRepository
from .pipeline import InterruptFlag, LightweightProgressTracker, setup_interrupt_handlers
from .legacy import batch_insert_to_postgis, create_table_if_not_exists

DEFAULT_LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"


def _resolve_dsn(explicit: str | None) -> str:
    """Return the SQLAlchemy DSN using CLI, environment or default value."""

    if explicit:
        return explicit
    env_dsn = os.environ.get("LOCAL_DSN")
    if env_dsn:
        return env_dsn
    return DEFAULT_LOCAL_DSN


def _print_statistics(tracker: LightweightProgressTracker) -> None:
    """Print cached processing statistics and exit."""

    stats = tracker.get_statistics()
    print("\n=== 处理统计信息 ===")
    print(f"成功处理: {stats['success_count']} 个场景")
    print(f"失败场景: {stats['failed_count']} 个")

    failed_by_step = stats.get("failed_by_step", {})
    if failed_by_step:
        print("\n按步骤分类的失败统计:")
        for step, count in failed_by_step.items():
            print(f"  {step}: {count} 个")


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
    print(f"\n🛑 收到中断信号 ({sig_name})，正在尝试优雅退出...")


def build_parser() -> argparse.ArgumentParser:
    """Create the argument parser for the overlap analysis CLI."""

    parser = argparse.ArgumentParser(
        description="从数据集文件生成边界框重叠分析结果",
    )
    parser.add_argument(
        "--input",
        required=True,
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
        type=int,
        default=1000,
        help="处理批次大小",
    )
    parser.add_argument(
        "--insert-batch",
        type=int,
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


def main(argv: list[str] | None = None) -> int:
    """Launch the overlap analysis using the refactored core pipeline."""

    parser = build_parser()
    args = parser.parse_args(argv)

    tracker = LightweightProgressTracker(args.work_dir)

    if args.show_stats:
        _print_statistics(tracker)
        return 0

    manifest_path = args.input
    dsn = _resolve_dsn(args.dsn)

    print(f"开始处理输入文件: {manifest_path}")
    print(f"工作目录: {tracker.work_dir}")
    print(f"数据库连接: {dsn}")

    engine = create_engine(dsn, future=True)

    if args.create_table:
        if not create_table_if_not_exists(engine):
            print("创建表失败，退出")
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

    print("\n=== 处理完成 ===")
    print(f"总场景数: {result['total_scenes']}")
    print(f"已处理记录: {result['processed_records']}")
    print(f"已插入记录: {result['inserted_records']}")
    print(f"完成批次数: {result['completed_batches']}")
    if result["interrupted"]:
        print("状态: 已被中断")
    else:
        print("状态: 正常完成")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
