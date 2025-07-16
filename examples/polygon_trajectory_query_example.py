"""
高性能Polygon轨迹查询示例

演示如何使用优化的polygon_trajectory_query模块进行批量轨迹查询
展示不同配置选项和性能特性
"""

import json
import logging
from pathlib import Path
from spdatalab.dataset.polygon_trajectory_query import (
    process_polygon_trajectory_query,
    PolygonTrajectoryConfig,
    HighPerformancePolygonTrajectoryQuery
)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_sample_geojson():
    """创建示例GeoJSON文件"""
    # 创建一个简单的矩形polygon（北京附近区域）
    sample_polygon = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "id": "beijing_area_1",
                    "name": "北京测试区域1"
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [116.3, 39.9],   # 西南角
                        [116.4, 39.9],   # 东南角
                        [116.4, 40.0],   # 东北角
                        [116.3, 40.0],   # 西北角
                        [116.3, 39.9]    # 回到起始点
                    ]]
                }
            },
            {
                "type": "Feature", 
                "properties": {
                    "id": "beijing_area_2",
                    "name": "北京测试区域2"
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [116.35, 39.85],
                        [116.45, 39.85],
                        [116.45, 39.95],
                        [116.35, 39.95],
                        [116.35, 39.85]
                    ]]
                }
            }
        ]
    }
    
    # 保存示例文件
    sample_file = Path("sample_polygons.geojson")
    with open(sample_file, 'w', encoding='utf-8') as f:
        json.dump(sample_polygon, f, ensure_ascii=False, indent=2)
    
    logger.info(f"创建示例GeoJSON文件: {sample_file}")
    return str(sample_file)

def run_basic_example():
    """运行基础示例"""
    logger.info("=== 基础Polygon轨迹查询示例 ===")
    
    try:
        # 1. 创建示例GeoJSON文件
        geojson_file = create_sample_geojson()
        
        # 2. 使用默认配置处理
        logger.info("使用默认配置进行polygon轨迹查询...")
        
        stats = process_polygon_trajectory_query(
            geojson_file=geojson_file,
            output_table="polygon_trajectories_basic_example",
            output_geojson="trajectories_basic_result.geojson"
        )
        
        # 3. 输出结果统计
        display_results(stats, "基础示例")
        
        # 4. 清理示例文件
        Path(geojson_file).unlink(missing_ok=True)
        Path("trajectories_basic_result.geojson").unlink(missing_ok=True)
        
        return stats.get('success', False)
        
    except Exception as e:
        logger.error(f"基础示例运行失败: {str(e)}")
        return False

def run_performance_example():
    """运行高性能配置示例"""
    logger.info("=== 高性能配置示例 ===")
    
    try:
        # 1. 创建示例GeoJSON文件
        geojson_file = create_sample_geojson()
        
        # 2. 创建高性能配置
        config = PolygonTrajectoryConfig(
            batch_threshold=30,          # 降低批量阈值
            chunk_size=10,               # 较小分块
            limit_per_polygon=20000,     # 更多轨迹点
            batch_insert_size=500,       # 较小批次插入
            min_points_per_trajectory=3, # 至少3个点
            enable_speed_stats=True,     # 启用速度统计
            enable_avp_stats=True        # 启用AVP统计
        )
        
        logger.info("使用高性能配置:")
        logger.info(f"  • 批量阈值: {config.batch_threshold}")
        logger.info(f"  • 分块大小: {config.chunk_size}")
        logger.info(f"  • 轨迹点限制: {config.limit_per_polygon:,}")
        logger.info(f"  • 批量插入: {config.batch_insert_size}")
        
        # 3. 执行高性能查询
        stats = process_polygon_trajectory_query(
            geojson_file=geojson_file,
            output_table="polygon_trajectories_performance_example",
            config=config
        )
        
        # 4. 输出详细结果
        display_results(stats, "高性能示例")
        
        # 5. 清理示例文件
        Path(geojson_file).unlink(missing_ok=True)
        
        return stats.get('success', False)
        
    except Exception as e:
        logger.error(f"高性能示例运行失败: {str(e)}")
        return False

def run_direct_api_example():
    """运行直接API调用示例"""
    logger.info("=== 直接API调用示例 ===")
    
    try:
        # 1. 创建示例GeoJSON文件
        geojson_file = create_sample_geojson()
        
        # 2. 创建查询器实例
        config = PolygonTrajectoryConfig(
            batch_threshold=5,  # 小阈值便于演示分块
            chunk_size=1,
            limit_per_polygon=1000
        )
        
        query_processor = HighPerformancePolygonTrajectoryQuery(config)
        
        # 3. 分步执行
        from spdatalab.dataset.polygon_trajectory_query import load_polygons_from_geojson
        
        logger.info("步骤1: 加载polygon...")
        polygons = load_polygons_from_geojson(geojson_file)
        logger.info(f"加载了 {len(polygons)} 个polygon")
        
        logger.info("步骤2: 查询轨迹点...")
        points_df, query_stats = query_processor.query_intersecting_trajectory_points(polygons)
        logger.info(f"查询统计: {query_stats}")
        
        if not points_df.empty:
            logger.info("步骤3: 构建轨迹...")
            trajectories, build_stats = query_processor.build_trajectories_from_points(points_df)
            logger.info(f"构建统计: {build_stats}")
            
            logger.info("步骤4: 保存到数据库...")
            saved_count, save_stats = query_processor.save_trajectories_to_table(
                trajectories, "polygon_trajectories_api_example"
            )
            logger.info(f"保存统计: {save_stats}")
        else:
            logger.info("没有找到轨迹点数据")
        
        # 4. 清理示例文件
        Path(geojson_file).unlink(missing_ok=True)
        
        return True
        
    except Exception as e:
        logger.error(f"直接API示例运行失败: {str(e)}")
        return False

def display_results(stats: dict, example_name: str):
    """显示处理结果"""
    logger.info(f"=== {example_name}处理结果 ===")
    
    if not stats.get('success', False):
        logger.error("❌ 处理失败")
        if 'error' in stats:
            logger.error(f"错误信息: {stats['error']}")
        return
    
    logger.info(f"✅ 处理成功完成！")
    logger.info(f"📊 基本统计:")
    logger.info(f"   • Polygon数量: {stats.get('polygon_count', 0)}")
    
    query_stats = stats.get('query_stats', {})
    if query_stats:
        logger.info(f"🔍 查询统计:")
        logger.info(f"   • 查询策略: {query_stats.get('strategy', 'unknown')}")
        logger.info(f"   • 轨迹点总数: {query_stats.get('total_points', 0):,}")
        logger.info(f"   • 数据集数量: {query_stats.get('unique_datasets', 0)}")
        logger.info(f"   • 查询用时: {query_stats.get('query_time', 0):.2f}s")
    
    build_stats = stats.get('build_stats', {})
    if build_stats:
        logger.info(f"🔧 构建统计:")
        logger.info(f"   • 有效轨迹数: {build_stats.get('valid_trajectories', 0)}")
        logger.info(f"   • 跳过轨迹数: {build_stats.get('skipped_trajectories', 0)}")
        logger.info(f"   • 构建用时: {build_stats.get('build_time', 0):.2f}s")
    
    save_stats = stats.get('save_stats', {})
    if save_stats and not save_stats.get('skipped', False):
        logger.info(f"💾 保存统计:")
        logger.info(f"   • 保存记录数: {save_stats.get('saved_records', 0)}")
        logger.info(f"   • 批次数量: {save_stats.get('batch_count', 0)}")
        logger.info(f"   • 保存用时: {save_stats.get('save_time', 0):.2f}s")
    
    total_time = stats.get('total_duration', 0)
    if total_time > 0:
        logger.info(f"⏱️ 总用时: {total_time:.2f}s")
        
        total_points = query_stats.get('total_points', 0)
        if total_points > 0:
            logger.info(f"🚀 处理速度: {total_points/total_time:.1f} 点/秒")

def show_usage_examples():
    """显示命令行使用示例"""
    print("\n=== 命令行使用示例 ===")
    print("1. 查询轨迹并保存到数据库表:")
    print("   python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson --table my_trajectories")
    
    print("\n2. 查询轨迹并导出到GeoJSON文件:")
    print("   python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson --output trajectories.geojson")
    
    print("\n3. 同时保存到数据库和文件:")
    print("   python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson --table my_trajectories --output trajectories.geojson")
    
    print("\n4. 设置每个polygon的轨迹点限制:")
    print("   python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson --table my_trajectories --limit 20000")
    
    print("\n5. 启用详细日志:")
    print("   python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson --table my_trajectories --verbose")

if __name__ == "__main__":
    # 显示使用示例
    show_usage_examples()
    
    print("\n=== 高性能Polygon轨迹查询示例 ===")
    print("请选择要运行的示例:")
    print("1. 基础示例 - 使用默认配置")
    print("2. 高性能示例 - 展示优化配置")
    print("3. 直接API示例 - 分步调用API")
    print("4. 运行所有示例")
    print("0. 跳过示例运行")
    
    try:
        choice = input("\n请输入选择 (0-4): ").strip()
        
        if choice == '0':
            print("跳过示例运行")
        elif choice == '1':
            logger.info("运行基础示例...")
            success = run_basic_example()
            print("✅ 基础示例运行成功！" if success else "❌ 基础示例运行失败！")
        elif choice == '2':
            logger.info("运行高性能示例...")
            success = run_performance_example()
            print("✅ 高性能示例运行成功！" if success else "❌ 高性能示例运行失败！")
        elif choice == '3':
            logger.info("运行直接API示例...")
            success = run_direct_api_example()
            print("✅ 直接API示例运行成功！" if success else "❌ 直接API示例运行失败！")
        elif choice == '4':
            logger.info("运行所有示例...")
            results = []
            
            logger.info("\n" + "="*50)
            results.append(("基础示例", run_basic_example()))
            
            logger.info("\n" + "="*50)
            results.append(("高性能示例", run_performance_example()))
            
            logger.info("\n" + "="*50)
            results.append(("直接API示例", run_direct_api_example()))
            
            logger.info("\n" + "="*50)
            logger.info("🎯 所有示例运行完成！")
            logger.info("📊 结果汇总:")
            for name, success in results:
                status = "✅ 成功" if success else "❌ 失败"
                logger.info(f"   • {name}: {status}")
        else:
            print("无效选择，跳过示例运行")
            
    except KeyboardInterrupt:
        print("\n用户中断，退出示例运行")
    except Exception as e:
        logger.error(f"示例运行出错: {e}") 