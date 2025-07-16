#!/usr/bin/env python3
"""
快速测试polygon轨迹查询功能

测试polygon_trajectory_query模块的基础功能
"""

import json
import logging
import tempfile
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_polygon_trajectory_query():
    """测试polygon轨迹查询功能"""
    try:
        from src.spdatalab.dataset.polygon_trajectory_query import (
            load_polygons_from_geojson,
            HighPerformancePolygonTrajectoryQuery,
            PolygonTrajectoryConfig,
            process_polygon_trajectory_query
        )
        logger.info("✅ 成功导入polygon_trajectory_query模块")
    except ImportError as e:
        logger.error(f"❌ 导入模块失败: {e}")
        return False
    
    # 创建测试GeoJSON
    test_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "id": "test_area",
                    "name": "测试区域"
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [116.3, 39.9],
                        [116.4, 39.9],
                        [116.4, 40.0],
                        [116.3, 40.0],
                        [116.3, 39.9]
                    ]]
                }
            }
        ]
    }
    
    # 创建临时文件
    with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False, encoding='utf-8') as f:
        json.dump(test_geojson, f, ensure_ascii=False, indent=2)
        temp_geojson_file = f.name
    
    try:
        logger.info("🧪 开始测试GeoJSON加载...")
        
        # 测试1: 加载GeoJSON
        polygons = load_polygons_from_geojson(temp_geojson_file)
        if len(polygons) == 1:
            logger.info(f"✅ 成功加载 {len(polygons)} 个polygon")
        else:
            logger.error(f"❌ 加载polygon数量错误: {len(polygons)}")
            return False
        
        # 测试2: 创建高性能查询器
        logger.info("🧪 开始测试高性能查询器...")
        config = PolygonTrajectoryConfig(limit_per_polygon=100)
        query_processor = HighPerformancePolygonTrajectoryQuery(config)
        logger.info("✅ 查询器创建成功")
        
        # 测试3: 查询轨迹点（可能没有数据，但不应该出错）
        logger.info("🧪 开始测试批量轨迹点查询...")
        points_df, query_stats = query_processor.query_intersecting_trajectory_points(polygons)
        logger.info(f"✅ 轨迹点查询完成，找到 {len(points_df)} 个点")
        logger.info(f"   查询策略: {query_stats.get('strategy', 'unknown')}")
        
        # 测试4: 构建轨迹（如果有数据的话）
        if not points_df.empty:
            logger.info("🧪 开始测试智能轨迹构建...")
            trajectories, build_stats = query_processor.build_trajectories_from_points(points_df)
            logger.info(f"✅ 轨迹构建完成，生成 {build_stats['valid_trajectories']} 条有效轨迹")
        else:
            logger.info("ℹ️ 没有轨迹点数据，跳过轨迹构建测试")
        
        # 测试5: 完整流程测试
        logger.info("🧪 开始测试完整高性能流程...")
        
        # 创建临时输出文件
        with tempfile.NamedTemporaryFile(mode='w', suffix='.geojson', delete=False) as f:
            temp_output_file = f.name
        
        # 创建测试配置
        test_config = PolygonTrajectoryConfig(
            limit_per_polygon=100,
            batch_threshold=5,  # 小阈值便于测试
            chunk_size=2
        )
        
        stats = process_polygon_trajectory_query(
            geojson_file=temp_geojson_file,
            output_table=None,  # 不保存到数据库表，避免创建测试表
            output_geojson=temp_output_file,
            config=test_config
        )
        
        logger.info("✅ 完整流程测试完成")
        logger.info(f"   Polygon数量: {stats.get('polygon_count', 0)}")
        logger.info(f"   查询统计: {stats.get('query_stats', {})}")
        logger.info(f"   构建统计: {stats.get('build_stats', {})}")
        logger.info(f"   处理成功: {stats.get('success', False)}")
        
        # 清理临时文件
        Path(temp_output_file).unlink(missing_ok=True)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试过程出错: {e}")
        return False
    
    finally:
        # 清理临时文件
        Path(temp_geojson_file).unlink(missing_ok=True)

def test_module_imports():
    """测试所需依赖模块的导入"""
    logger.info("🧪 测试依赖模块导入...")
    
    required_modules = [
        'geopandas',
        'pandas', 
        'shapely.geometry',
        'sqlalchemy',
        'json',
        'logging'
    ]
    
    success = True
    for module_name in required_modules:
        try:
            __import__(module_name)
            logger.info(f"✅ {module_name}")
        except ImportError as e:
            logger.error(f"❌ {module_name}: {e}")
            success = False
    
    return success

def main():
    """主测试函数"""
    logger.info("=" * 50)
    logger.info("🚀 开始polygon轨迹查询功能快速测试")
    logger.info("=" * 50)
    
    # 测试1: 依赖模块
    logger.info("\n📦 步骤1: 测试依赖模块")
    if not test_module_imports():
        logger.error("❌ 依赖模块测试失败")
        return 1
    
    # 测试2: 功能测试  
    logger.info("\n🔧 步骤2: 测试功能模块")
    if not test_polygon_trajectory_query():
        logger.error("❌ 功能测试失败")
        return 1
    
    logger.info("\n" + "=" * 50)
    logger.info("🎉 所有测试通过！polygon轨迹查询功能正常")
    logger.info("=" * 50)
    
    # 显示使用提示
    logger.info("\n📖 使用方法:")
    logger.info("命令行: python -m spdatalab.dataset.polygon_trajectory_query --input polygons.geojson --table my_trajectories")
    logger.info("API: from spdatalab.dataset.polygon_trajectory_query import process_polygon_trajectory_query")
    
    return 0

if __name__ == "__main__":
    exit(main()) 