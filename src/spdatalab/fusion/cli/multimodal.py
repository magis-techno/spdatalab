"""Command line interface for the multimodal trajectory workflow."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Sequence

from spdatalab.dataset.multimodal_data_retriever import APIConfig
from spdatalab.dataset.polygon_trajectory_query import PolygonTrajectoryConfig
from spdatalab.fusion.multimodal_trajectory_retrieval import (
    MultimodalConfig,
    MultimodalTrajectoryWorkflow,
)

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logger = logging.getLogger(__name__)


def configure_logging(verbose: bool) -> None:
    """Configure logging for the CLI run."""

    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format=LOG_FORMAT, force=True)


def build_parser() -> argparse.ArgumentParser:
    """Create the argument parser for the multimodal CLI."""

    parser = argparse.ArgumentParser(
        description="多模态轨迹检索系统 - 研发分析专用工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
使用示例:
  # 基础文本查询
  python -m spdatalab.fusion.cli.multimodal \\
      --text "bicycle crossing intersection" \\
      --collection "ddi_collection_camera_encoded_1" \\
      --output-table "discovered_trajectories"

  # 完整参数示例（文本检索）
  python -m spdatalab.fusion.cli.multimodal \\
      --text "red car turning left" \\
      --collection "ddi_collection_camera_encoded_1" \\
      --count 10 \\
      --start 0 \\
      --start-time 1234567891011 \\
      --end-time 1234567891111 \\
      --time-window 30 \\
      --buffer-distance 10 \\
      --output-table "red_car_trajectories" \\
      --output-geojson "red_car_results.geojson" \\
      --verbose

环境变量配置:
  MULTIMODAL_API_KEY=<your_api_key> (必需)
  MULTIMODAL_USERNAME=<your_username> (必需)
  MULTIMODAL_API_BASE_URL=<api_base_url> (必需)
  MULTIMODAL_PROJECT=<your_project> (可选)
  MULTIMODAL_API_PATH=<api_path> (可选)

API限制:
  - 单次查询最多10,000条
  - 累计查询最多100,000条
        """,
    )

    parser.add_argument(
        "--text",
        type=str,
        required=True,
        help='查询文本，如 "bicycle crossing intersection"、"红色汽车转弯"',
    )
    parser.add_argument(
        "--collection",
        type=str,
        required=True,
        help='相机表选择，如 "ddi_collection_camera_encoded_1"（camera参数自动推导）',
    )
    parser.add_argument(
        "--count",
        type=int,
        default=5,
        help="返回数量，默认5，最大10000",
    )
    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="起始偏移量，默认0",
    )
    parser.add_argument(
        "--start-time",
        type=int,
        help="事件开始时间，13位时间戳（可选）",
    )
    parser.add_argument(
        "--end-time",
        type=int,
        help="事件结束时间，13位时间戳（可选）",
    )
    parser.add_argument(
        "--time-window",
        type=int,
        default=30,
        help="时间窗口（天），默认30天",
    )
    parser.add_argument(
        "--buffer-distance",
        type=float,
        default=10.0,
        help="空间缓冲区距离（米），默认10米",
    )
    parser.add_argument(
        "--overlap-threshold",
        type=float,
        default=0.7,
        help="Polygon重叠合并阈值，默认0.7",
    )
    parser.add_argument(
        "--output-table",
        type=str,
        help="输出数据库表名",
    )
    parser.add_argument(
        "--output-geojson",
        type=str,
        help="输出GeoJSON文件路径",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        help="输出完整结果JSON文件路径",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="详细输出模式",
    )
    parser.add_argument(
        "--batch-threshold",
        type=int,
        default=50,
        help="批量查询阈值，默认50",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=20,
        help="分块大小，默认20",
    )

    return parser


def validate_args(args: argparse.Namespace) -> None:
    """Validate CLI arguments and raise ``ValueError`` on invalid values."""

    if args.count > 10000:
        raise ValueError("count参数不能超过10000")
    if args.count <= 0:
        raise ValueError("count参数必须大于0")
    if args.start < 0:
        raise ValueError("start参数必须大于等于0")
    if (
        args.start_time is not None
        and args.end_time is not None
        and args.start_time >= args.end_time
    ):
        raise ValueError("start-time必须小于end-time")
    if args.buffer_distance <= 0:
        raise ValueError("buffer-distance必须大于0")
    if not 0.0 <= args.overlap_threshold <= 1.0:
        raise ValueError("overlap-threshold必须在0.0-1.0之间")


def get_api_config_from_env() -> APIConfig:
    """Load the API configuration from environment variables."""

    try:
        return APIConfig.from_env()
    except RuntimeError as exc:
        logger.error("❌ API配置不完整，请设置以下环境变量：")
        logger.error("   MULTIMODAL_API_KEY=<your_api_key> (必需)")
        logger.error("   MULTIMODAL_USERNAME=<your_username> (必需)")
        logger.error("   MULTIMODAL_API_BASE_URL=<api_base_url> (必需)")
        logger.error("   MULTIMODAL_PROJECT=<your_project> (默认: your_project)")
        logger.error("   MULTIMODAL_API_PATH=<api_path> (默认: /xmodalitys/retrieve)")
        logger.error("   MULTIMODAL_PLATFORM=<platform> (默认: xmodalitys-external)")
        logger.error("   MULTIMODAL_REGION=<region> (默认: RaD-prod)")
        logger.error(
            "   MULTIMODAL_ENTRYPOINT_VERSION=<version> (默认: v2)"
        )
        logger.error("   MULTIMODAL_TIMEOUT=<timeout> (默认: 30)")
        logger.error("   MULTIMODAL_MAX_RETRIES=<retries> (默认: 3)")
        logger.error("   具体错误: %s", exc)
        raise


def create_multimodal_config(
    args: argparse.Namespace, api_config: APIConfig
) -> MultimodalConfig:
    """Create the workflow configuration from parsed arguments."""

    polygon_config = PolygonTrajectoryConfig(
        batch_threshold=args.batch_threshold,
        chunk_size=args.chunk_size,
        limit_per_polygon=15000,
    )

    return MultimodalConfig(
        api_config=api_config,
        max_search_results=args.count,
        time_window_days=args.time_window,
        buffer_distance=args.buffer_distance,
        overlap_threshold=args.overlap_threshold,
        polygon_config=polygon_config,
        output_table=args.output_table,
        output_geojson=args.output_geojson,
    )


def save_results(results: dict, output_path: Path) -> None:
    """Persist workflow results to a JSON file."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    serializable_results: dict[str, object] = {}

    for key, value in results.items():
        if key in {"trajectory_points", "source_polygons", "summary"}:
            serializable_results[key] = value
        elif key == "stats" and isinstance(value, dict):
            stats: dict[str, object] = {}
            for k, v in value.items():
                if isinstance(v, (str, int, float, bool, list, dict)) or v is None:
                    stats[k] = v
                elif hasattr(v, "isoformat"):
                    stats[k] = v.isoformat()  # type: ignore[assignment]
                else:
                    stats[k] = str(v)
            serializable_results[key] = stats

    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(serializable_results, fh, indent=2, ensure_ascii=False)

    logger.info("💾 结果已保存到: %s", output_path)


def print_summary(results: dict, verbose: bool = False) -> None:
    """Display a human readable summary of the workflow results."""

    if not results.get("success", False):
        logger.error("❌ 查询失败: %s", results.get("message", results.get("error", "未知错误")))
        return

    summary = results.get("summary", {})
    stats = results.get("stats", {})

    print("\n" + "=" * 60)
    print("🎯 多模态轨迹检索结果摘要")
    print("=" * 60)
    print("📊 查询统计:")
    print(f"   查询类型: {stats.get('query_type', 'unknown')}")
    print(f"   查询内容: {stats.get('query_content', 'unknown')}")
    print(f"   Collection: {stats.get('collection', 'unknown')}")
    print(f"   检索结果: {stats.get('search_results_count', 0)} 条")
    print(f"   聚合数据集: {stats.get('aggregated_datasets', 0)} 个")

    if verbose and "dataset_details" in stats:
        print("\n📁 检索到的数据集详情:")
        dataset_details = stats["dataset_details"]
        for index, (dataset_name, count) in enumerate(dataset_details.items(), 1):
            display_name = dataset_name if len(dataset_name) <= 60 else dataset_name[:57] + "..."
            print(f"   {index}. {display_name}")
            print(f"      └─ {count} 条结果")
        if len(dataset_details) > 5:
            print(f"   ... 共 {len(dataset_details)} 个数据集")

    print("\n🔄 智能优化效果:")
    print(f"   Polygon优化: {summary.get('optimization_ratio', 'N/A')}")
    print(f"   发现轨迹点: {summary.get('total_points', 0)} 个")
    print(f"   涉及数据集: {summary.get('unique_datasets', 0)} 个")
    print(f"   Polygon来源: {summary.get('polygon_sources', 0)} 个")

    duration = stats.get("total_duration", 0)
    print("\n⏱️  性能统计:")
    print(f"   总耗时: {duration:.2f} 秒")

    if verbose:
        print("\n🔧 详细配置:")
        config = stats.get("config", {})
        print(f"   缓冲区距离: {config.get('buffer_distance', 'N/A')} 米")
        print(f"   时间窗口: {config.get('time_window_days', 'N/A')} 天")
        print(f"   重叠阈值: {config.get('overlap_threshold', 'N/A')}")

        print("\n📈 阶段统计:")
        print(f"   原始Polygon: {stats.get('raw_polygon_count', 0)} 个")
        print(f"   合并Polygon: {stats.get('merged_polygon_count', 0)} 个")
        print(f"   轨迹数据: {stats.get('trajectory_data_count', 0)} 条")

        if "saved_to_database" in stats:
            print("\n💾 数据库保存:")
            print(f"   保存记录: {stats.get('saved_to_database', 0)} 条")
        elif "database_save_error" in stats:
            print("\n❌ 数据库保存失败:")
            print(f"   错误信息: {stats.get('database_save_error', 'N/A')}")

    print("=" * 60)


def run(
    args: argparse.Namespace,
    *,
    api_config_loader: Callable[[], APIConfig] = get_api_config_from_env,
    workflow_factory: Callable[[MultimodalConfig], MultimodalTrajectoryWorkflow] = MultimodalTrajectoryWorkflow,
) -> dict:
    """Execute the workflow using the provided arguments."""

    logger.info("🔑 获取API配置...")
    api_config = api_config_loader()

    logger.info("⚙️ 创建多模态配置...")
    workflow_config = create_multimodal_config(args, api_config)

    logger.info("🚀 初始化多模态轨迹检索工作流...")
    workflow = workflow_factory(workflow_config)

    logger.info("🔍 开始执行查询: '%s'", args.text)
    start_time = datetime.now()
    results = workflow.process_text_query(
        text=args.text,
        collection=args.collection,
        count=args.count,
        start=args.start,
        start_time=args.start_time,
        end_time=args.end_time,
    )
    end_time = datetime.now()
    logger.info("✅ 查询完成，耗时: %.2f 秒", (end_time - start_time).total_seconds())

    return results


def main(
    argv: Sequence[str] | None = None,
    *,
    api_config_loader: Callable[[], APIConfig] = get_api_config_from_env,
    workflow_factory: Callable[[MultimodalConfig], MultimodalTrajectoryWorkflow] = MultimodalTrajectoryWorkflow,
) -> int:
    """Run the multimodal CLI and return an exit code."""

    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.verbose)

    try:
        validate_args(args)
    except ValueError as exc:
        logger.error("❌ 参数错误: %s", exc)
        return 2

    try:
        results = run(
            args,
            api_config_loader=api_config_loader,
            workflow_factory=workflow_factory,
        )
    except KeyboardInterrupt:
        logger.info("⏹️ 用户中断操作")
        return 1
    except RuntimeError as exc:
        logger.error("❌ API配置错误: %s", exc)
        return 1
    except Exception as exc:  # pragma: no cover - defensive logging branch
        logger.error("❌ 执行失败: %s", exc)
        if args.verbose:
            logger.exception("详细错误")
        return 1

    if args.output_json:
        save_results(results, Path(args.output_json))

    print_summary(results, args.verbose)

    if results.get("success", False):
        logger.info("🎉 多模态轨迹检索完成！")
        return 0

    logger.error("❌ 多模态轨迹检索失败！")
    return 1


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
