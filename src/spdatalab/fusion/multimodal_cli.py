"""多模态轨迹检索系统 - CLI接口

支持的命令格式：
python -m spdatalab.fusion.multimodal_trajectory_retrieval \
    --text "bicycle crossing intersection" \
    --collection "ddi_collection_camera_encoded_1" \
    --output-table "discovered_trajectories" \
    --buffer-distance 10
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from spdatalab.dataset.multimodal_data_retriever import APIConfig
from spdatalab.fusion.multimodal_trajectory_retrieval import (
    MultimodalConfig,
    MultimodalTrajectoryWorkflow
)
from spdatalab.dataset.polygon_trajectory_query import PolygonTrajectoryConfig

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_api_config_from_env() -> APIConfig:
    """从环境变量获取API配置"""
    try:
        return APIConfig.from_env()
    except RuntimeError as e:
        logger.error("❌ API配置不完整，请设置环境变量：")
        logger.error("   MULTIMODAL_API_KEY=<your_api_key> (必需)")
        logger.error("   MULTIMODAL_USERNAME=<your_username> (必需)")
        logger.error("   MULTIMODAL_API_BASE_URL=<api_base_url> (必需)")
        logger.error("   MULTIMODAL_PROJECT=<your_project> (默认: your_project)")
        logger.error("   MULTIMODAL_API_PATH=<api_path> (默认: /xmodalitys/retrieve)")
        logger.error("   MULTIMODAL_PLATFORM=<platform> (默认: xmodalitys-external)")
        logger.error("   MULTIMODAL_REGION=<region> (默认: RaD-prod)")
        logger.error("   MULTIMODAL_ENTRYPOINT_VERSION=<version> (默认: v2)")
        logger.error(f"\n具体错误: {e}")
        sys.exit(1)


def create_parser() -> argparse.ArgumentParser:
    """创建命令行参数解析器"""
    parser = argparse.ArgumentParser(
        description="多模态轨迹检索系统 - 研发分析专用工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 基础文本查询
  python -m spdatalab.fusion.multimodal_trajectory_retrieval \\
      --text "bicycle crossing intersection" \\
      --collection "ddi_collection_camera_encoded_1" \\
      --output-table "discovered_trajectories"

  # 完整参数示例（文本检索）
  python -m spdatalab.fusion.multimodal_trajectory_retrieval \\
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
        """
    )
    
    # 必选参数
    parser.add_argument(
        '--text',
        type=str,
        required=True,
        help='查询文本，如 "bicycle crossing intersection"、"红色汽车转弯"'
    )
    
    parser.add_argument(
        '--collection',
        type=str,
        required=True,
        help='相机表选择，如 "ddi_collection_camera_encoded_1"（camera参数自动推导）'
    )
    
    # 可选查询参数
    parser.add_argument(
        '--count',
        type=int,
        default=5,
        help='返回数量，默认5，最大10000'
    )
    
    parser.add_argument(
        '--start',
        type=int,
        default=0,
        help='起始偏移量，默认0'
    )
    
    parser.add_argument(
        '--start-time',
        type=int,
        help='事件开始时间，13位时间戳（可选）'
    )
    
    parser.add_argument(
        '--end-time',
        type=int,
        help='事件结束时间，13位时间戳（可选）'
    )
    
    # 分析参数
    parser.add_argument(
        '--time-window',
        type=int,
        default=30,
        help='时间窗口（天），默认30天'
    )
    
    parser.add_argument(
        '--buffer-distance',
        type=float,
        default=10.0,
        help='空间缓冲区距离（米），默认10米'
    )
    
    parser.add_argument(
        '--overlap-threshold',
        type=float,
        default=0.7,
        help='Polygon重叠合并阈值，默认0.7'
    )
    
    # 输出参数
    parser.add_argument(
        '--output-table',
        type=str,
        help='输出数据库表名'
    )
    
    parser.add_argument(
        '--output-geojson',
        type=str,
        help='输出GeoJSON文件路径'
    )
    
    parser.add_argument(
        '--output-json',
        type=str,
        help='输出完整结果JSON文件路径'
    )
    
    # 系统参数
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='详细输出模式'
    )
    
    parser.add_argument(
        '--batch-threshold',
        type=int,
        default=50,
        help='批量查询阈值，默认50'
    )
    
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=20,
        help='分块大小，默认20'
    )
    
    return parser


def validate_args(args) -> None:
    """验证命令行参数"""
    # 验证count参数
    if args.count > 10000:
        logger.error(f"❌ count参数不能超过10000，当前值: {args.count}")
        sys.exit(1)
    
    if args.count <= 0:
        logger.error(f"❌ count参数必须大于0，当前值: {args.count}")
        sys.exit(1)
    
    # 验证start参数
    if args.start < 0:
        logger.error(f"❌ start参数必须大于等于0，当前值: {args.start}")
        sys.exit(1)
    
    # 验证时间参数
    if hasattr(args, 'start_time') and hasattr(args, 'end_time'):
        if args.start_time and args.end_time:
            if args.start_time >= args.end_time:
                logger.error(f"❌ start-time必须小于end-time")
                sys.exit(1)
    
    # 验证缓冲区距离
    if args.buffer_distance <= 0:
        logger.error(f"❌ buffer-distance必须大于0，当前值: {args.buffer_distance}")
        sys.exit(1)
    
    # 验证重叠阈值
    if not 0.0 <= args.overlap_threshold <= 1.0:
        logger.error(f"❌ overlap-threshold必须在0.0-1.0之间，当前值: {args.overlap_threshold}")
        sys.exit(1)


def create_multimodal_config(args, api_config: APIConfig) -> MultimodalConfig:
    """根据命令行参数创建配置"""
    # 创建polygon查询配置
    polygon_config = PolygonTrajectoryConfig(
        batch_threshold=args.batch_threshold,
        chunk_size=args.chunk_size,
        limit_per_polygon=15000  # 固定值
    )
    
    return MultimodalConfig(
        api_config=api_config,
        max_search_results=args.count,
        time_window_days=args.time_window,
        buffer_distance=args.buffer_distance,
        overlap_threshold=args.overlap_threshold,
        polygon_config=polygon_config,
        output_table=args.output_table,
        output_geojson=args.output_geojson
    )


def save_results(results: dict, args) -> None:
    """保存结果到文件"""
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 处理不可序列化的对象
        serializable_results = {}
        for key, value in results.items():
            if key == 'trajectory_points' and isinstance(value, list):
                serializable_results[key] = value
            elif key == 'source_polygons':
                serializable_results[key] = value
            elif key == 'summary':
                serializable_results[key] = value
            elif key == 'stats':
                # 过滤stats中的不可序列化对象
                stats = {}
                for k, v in value.items():
                    if isinstance(v, (str, int, float, bool, list, dict)) or v is None:
                        stats[k] = v
                    elif hasattr(v, 'isoformat'):  # datetime对象
                        stats[k] = v.isoformat()
                    else:
                        stats[k] = str(v)
                serializable_results[key] = stats
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 结果已保存到: {output_path}")


def print_summary(results: dict, verbose: bool = False) -> None:
    """打印结果摘要"""
    if not results.get('success', False):
        logger.error(f"❌ 查询失败: {results.get('message', results.get('error', '未知错误'))}")
        return
    
    summary = results.get('summary', {})
    stats = results.get('stats', {})
    
    print("\n" + "="*60)
    print("🎯 多模态轨迹检索结果摘要")
    print("="*60)
    
    # 基础统计
    print(f"📊 查询统计:")
    print(f"   查询类型: {stats.get('query_type', 'unknown')}")
    print(f"   查询内容: {stats.get('query_content', 'unknown')}")
    print(f"   Collection: {stats.get('collection', 'unknown')}")
    print(f"   检索结果: {stats.get('search_results_count', 0)} 条")
    print(f"   聚合数据集: {stats.get('aggregated_datasets', 0)} 个")
    
    # 优化效果
    print(f"\n🔄 智能优化效果:")
    print(f"   Polygon优化: {summary.get('optimization_ratio', 'N/A')}")
    print(f"   发现轨迹点: {summary.get('total_points', 0)} 个")
    print(f"   涉及数据集: {summary.get('unique_datasets', 0)} 个")
    print(f"   Polygon来源: {summary.get('polygon_sources', 0)} 个")
    
    # 性能统计
    duration = stats.get('total_duration', 0)
    print(f"\n⏱️  性能统计:")
    print(f"   总耗时: {duration:.2f} 秒")
    
    if verbose:
        print(f"\n🔧 详细配置:")
        config = stats.get('config', {})
        print(f"   缓冲区距离: {config.get('buffer_distance', 'N/A')} 米")
        print(f"   时间窗口: {config.get('time_window_days', 'N/A')} 天")
        print(f"   重叠阈值: {config.get('overlap_threshold', 'N/A')}")
        
        print(f"\n📈 阶段统计:")
        print(f"   原始Polygon: {stats.get('raw_polygon_count', 0)} 个")
        print(f"   合并Polygon: {stats.get('merged_polygon_count', 0)} 个")
        print(f"   轨迹数据: {stats.get('trajectory_data_count', 0)} 条")
    
    print("="*60)


def main():
    """主入口函数"""
    parser = create_parser()
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.info("🔧 详细输出模式已启用")
    
    # 验证参数
    validate_args(args)
    
    try:
        # 获取API配置
        logger.info("🔑 获取API配置...")
        api_config = get_api_config_from_env()
        
        # 创建多模态配置
        logger.info("⚙️ 创建多模态配置...")
        multimodal_config = create_multimodal_config(args, api_config)
        
        # 初始化工作流
        logger.info("🚀 初始化多模态轨迹检索工作流...")
        workflow = MultimodalTrajectoryWorkflow(multimodal_config)
        
        # 执行查询
        logger.info(f"🔍 开始执行查询: '{args.text}'")
        start_time = datetime.now()
        
        results = workflow.process_text_query(
            text=args.text,
            collection=args.collection,
            count=args.count,
            start=args.start,
            start_time=args.start_time,
            end_time=args.end_time
        )
        
        end_time = datetime.now()
        logger.info(f"✅ 查询完成，耗时: {(end_time - start_time).total_seconds():.2f} 秒")
        
        # 保存结果
        if args.output_json:
            save_results(results, args)
        
        # 打印摘要
        print_summary(results, args.verbose)
        
        if results.get('success', False):
            logger.info("🎉 多模态轨迹检索完成！")
            sys.exit(0)
        else:
            logger.error("❌ 多模态轨迹检索失败！")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("⏹️ 用户中断操作")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 执行失败: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
