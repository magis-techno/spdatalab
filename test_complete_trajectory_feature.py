#!/usr/bin/env python3
"""
测试完整轨迹获取功能

验证新增的完整轨迹获取功能是否正常工作
"""

import logging
import json
from pathlib import Path
from src.spdatalab.dataset.polygon_trajectory_query import (
    HighPerformancePolygonTrajectoryQuery, 
    PolygonTrajectoryConfig
)

def create_test_polygon():
    """创建测试polygon"""
    test_polygon = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "id": "test_complete_trajectory",
                    "name": "完整轨迹测试区域"
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
    
    # 保存到临时文件
    test_file = "test_complete_trajectory_polygon.geojson"
    with open(test_file, 'w', encoding='utf-8') as f:
        json.dump(test_polygon, f, ensure_ascii=False, indent=2)
    
    return test_file

def test_complete_trajectory_feature():
    """测试完整轨迹获取功能"""
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=== 完整轨迹获取功能测试 ===")
    
    try:
        # 创建测试polygon
        polygon_file = create_test_polygon()
        logger.info(f"✅ 创建测试polygon文件: {polygon_file}")
        
        # 测试1: 启用完整轨迹获取
        logger.info("\n📋 测试1: 启用完整轨迹获取")
        config_with_complete = PolygonTrajectoryConfig(
            limit_per_polygon=100,
            fetch_complete_trajectories=True  # 启用完整轨迹获取
        )
        
        query_processor = HighPerformancePolygonTrajectoryQuery(config_with_complete)
        
        # 执行查询
        logger.info("🚀 执行完整轨迹查询...")
        trajectories, stats = query_processor.process_complete_workflow(
            polygon_geojson=polygon_file,
            output_table=None  # 不保存到数据库，仅测试功能
        )
        
        logger.info(f"📊 完整轨迹查询结果:")
        logger.info(f"   - 轨迹数量: {len(trajectories)}")
        logger.info(f"   - 查询策略: {stats.get('strategy', 'unknown')}")
        logger.info(f"   - 总点数: {stats.get('total_points', 0)}")
        logger.info(f"   - 数据集数: {stats.get('unique_datasets', 0)}")
        logger.info(f"   - 查询时间: {stats.get('query_time', 0):.2f}s")
        
        # 检查是否获取了完整轨迹
        if stats.get('complete_trajectories_fetched'):
            logger.info(f"✅ 成功获取完整轨迹!")
            logger.info(f"   - 原始点数: {stats.get('original_points', 0)}")
            logger.info(f"   - 完整轨迹点数: {stats.get('complete_points', 0)}")
            logger.info(f"   - 完整轨迹查询时间: {stats.get('complete_query_time', 0):.2f}s")
        else:
            logger.warning("⚠️ 未获取到完整轨迹数据")
        
        # 测试2: 禁用完整轨迹获取
        logger.info("\n📋 测试2: 禁用完整轨迹获取（对比测试）")
        config_without_complete = PolygonTrajectoryConfig(
            limit_per_polygon=100,
            fetch_complete_trajectories=False  # 禁用完整轨迹获取
        )
        
        query_processor_2 = HighPerformancePolygonTrajectoryQuery(config_without_complete)
        
        # 执行查询
        logger.info("🚀 执行普通相交查询...")
        trajectories_2, stats_2 = query_processor_2.process_complete_workflow(
            polygon_geojson=polygon_file,
            output_table=None
        )
        
        logger.info(f"📊 普通相交查询结果:")
        logger.info(f"   - 轨迹数量: {len(trajectories_2)}")
        logger.info(f"   - 总点数: {stats_2.get('total_points', 0)}")
        logger.info(f"   - 数据集数: {stats_2.get('unique_datasets', 0)}")
        logger.info(f"   - 查询时间: {stats_2.get('query_time', 0):.2f}s")
        
        # 对比结果
        logger.info("\n📊 结果对比:")
        complete_points = stats.get('total_points', 0)
        intersection_points = stats_2.get('total_points', 0)
        
        if complete_points > intersection_points:
            logger.info(f"✅ 完整轨迹获取有效: {complete_points} > {intersection_points} 个点")
            logger.info(f"   完整轨迹增加了 {complete_points - intersection_points} 个点")
        elif complete_points == intersection_points:
            logger.info(f"ℹ️ 点数相同: {complete_points} = {intersection_points}")
            logger.info("   可能polygon区域包含了完整轨迹或没有找到相交轨迹")
        else:
            logger.warning(f"⚠️ 异常: 完整轨迹点数少于相交点数 {complete_points} < {intersection_points}")
        
        # 清理临时文件
        Path(polygon_file).unlink(missing_ok=True)
        logger.info(f"🧹 清理临时文件: {polygon_file}")
        
        logger.info("\n✅ 完整轨迹获取功能测试完成!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {str(e)}")
        return False

if __name__ == '__main__':
    success = test_complete_trajectory_feature()
    exit(0 if success else 1) 