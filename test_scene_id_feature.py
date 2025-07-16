#!/usr/bin/env python3
"""
测试scene_id功能

验证新增的scene_id功能是否正常工作
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
                    "id": "scene_id_test",
                    "name": "scene_id测试区域"
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
    test_file = "test_scene_id_polygon.geojson"
    with open(test_file, 'w', encoding='utf-8') as f:
        json.dump(test_polygon, f, ensure_ascii=False, indent=2)
    
    return test_file

def test_scene_id_feature():
    """测试scene_id功能"""
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=== scene_id功能测试 ===")
    
    try:
        # 创建测试polygon
        polygon_file = create_test_polygon()
        logger.info(f"✅ 创建测试polygon文件: {polygon_file}")
        
        # 配置：启用完整轨迹获取以确保scene_id功能被触发
        config = PolygonTrajectoryConfig(
            limit_per_polygon=100,
            fetch_complete_trajectories=True  # 启用完整轨迹获取以测试scene_id功能
        )
        
        query_processor = HighPerformancePolygonTrajectoryQuery(config)
        
        # 执行查询（不保存到数据库，仅测试功能）
        logger.info("🚀 执行查询，测试scene_id功能...")
        trajectories, stats = query_processor.process_complete_workflow(
            polygon_geojson=polygon_file,
            output_table=None  # 不保存到数据库，仅测试功能
        )
        
        logger.info(f"📊 查询结果:")
        logger.info(f"   - 轨迹数量: {len(trajectories)}")
        logger.info(f"   - 查询策略: {stats.get('strategy', 'unknown')}")
        logger.info(f"   - 总点数: {stats.get('total_points', 0)}")
        logger.info(f"   - 数据集数: {stats.get('unique_datasets', 0)}")
        
        # 检查轨迹中是否包含scene_id信息
        if trajectories:
            logger.info("🔍 检查轨迹中的scene_id信息...")
            
            scene_id_found = False
            scene_ids = set()
            
            for i, traj in enumerate(trajectories[:3]):  # 检查前3个轨迹
                if 'scene_id' in traj:
                    scene_id_found = True
                    scene_ids.add(traj['scene_id'])
                    logger.info(f"   轨迹 {i+1}: dataset_name={traj.get('dataset_name', 'unknown')}, "
                               f"scene_id={traj.get('scene_id', 'unknown')}")
                else:
                    logger.warning(f"   轨迹 {i+1}: 缺少scene_id字段")
            
            if scene_id_found:
                logger.info(f"✅ scene_id功能正常: 发现 {len(scene_ids)} 个不同的scene_id")
                logger.info(f"   scene_id值: {list(scene_ids)}")
            else:
                logger.warning("⚠️ 所有轨迹都缺少scene_id字段")
        else:
            logger.warning("⚠️ 没有找到轨迹数据")
        
        # 测试数据库表结构（如果保存到数据库）
        if False:  # 设置为True来测试数据库表结构
            test_table = "test_scene_id_trajectories"
            logger.info(f"\n🗄️ 测试数据库表结构: {test_table}")
            
            saved_count, save_stats = query_processor.save_trajectories_to_table(
                trajectories, test_table
            )
            
            if saved_count > 0:
                logger.info(f"✅ 成功保存到数据库: {saved_count} 条记录")
                logger.info("   表结构应包含scene_id列")
            else:
                logger.warning("⚠️ 数据库保存失败")
        
        # 清理临时文件
        Path(polygon_file).unlink(missing_ok=True)
        logger.info(f"🧹 清理临时文件: {polygon_file}")
        
        logger.info("\n✅ scene_id功能测试完成!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {str(e)}")
        return False

if __name__ == '__main__':
    success = test_scene_id_feature()
    exit(0 if success else 1) 