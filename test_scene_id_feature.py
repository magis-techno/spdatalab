#!/usr/bin/env python3
"""
测试scene_id字段功能

验证轨迹数据中scene_id字段的添加功能是否正常工作
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
                    "id": "test_scene_id_feature",
                    "name": "scene_id字段测试区域"
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
    """测试scene_id字段功能"""
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=== scene_id字段功能测试 ===")
    
    try:
        # 创建测试polygon
        polygon_file = create_test_polygon()
        logger.info(f"✅ 创建测试polygon文件: {polygon_file}")
        
        # 配置查询处理器
        config = PolygonTrajectoryConfig(
            limit_per_polygon=50,  # 限制数量以便快速测试
            fetch_complete_trajectories=False  # 先测试基本功能
        )
        
        query_processor = HighPerformancePolygonTrajectoryQuery(config)
        
        # 执行查询
        logger.info("🚀 执行轨迹查询...")
        trajectories, stats = query_processor.process_complete_workflow(
            polygon_geojson=polygon_file,
            output_table=None  # 不保存到数据库，仅测试数据结构
        )
        
        logger.info(f"📊 查询结果:")
        logger.info(f"   - 轨迹数量: {len(trajectories)}")
        logger.info(f"   - 查询策略: {stats.get('strategy', 'unknown')}")
        logger.info(f"   - 总点数: {stats.get('total_points', 0)}")
        logger.info(f"   - 数据集数: {stats.get('unique_datasets', 0)}")
        logger.info(f"   - 查询时间: {stats.get('query_time', 0):.2f}s")
        
        # 检查轨迹数据结构
        if trajectories:
            logger.info("\n🔍 检查轨迹数据结构:")
            first_trajectory = trajectories[0]
            
            # 检查必要字段
            required_fields = ['dataset_name', 'scene_id', 'start_time', 'end_time', 'geometry']
            missing_fields = []
            present_fields = []
            
            for field in required_fields:
                if field in first_trajectory:
                    present_fields.append(field)
                else:
                    missing_fields.append(field)
            
            logger.info(f"   ✅ 存在字段: {present_fields}")
            if missing_fields:
                logger.warning(f"   ❌ 缺失字段: {missing_fields}")
            
            # 详细检查前几条轨迹的scene_id
            logger.info("\n📋 前几条轨迹的scene_id信息:")
            for i, traj in enumerate(trajectories[:5], 1):
                dataset_name = traj.get('dataset_name', 'unknown')
                scene_id = traj.get('scene_id', None)
                point_count = traj.get('point_count', 0)
                
                if scene_id:
                    logger.info(f"   {i}. {dataset_name} -> scene_id: {scene_id} ({point_count} 点)")
                else:
                    logger.warning(f"   {i}. {dataset_name} -> scene_id: None ({point_count} 点)")
            
            # 统计scene_id情况
            total_trajectories = len(trajectories)
            trajectories_with_scene_id = sum(1 for traj in trajectories if traj.get('scene_id'))
            trajectories_without_scene_id = total_trajectories - trajectories_with_scene_id
            
            logger.info(f"\n📊 scene_id统计:")
            logger.info(f"   - 总轨迹数: {total_trajectories}")
            logger.info(f"   - 有scene_id: {trajectories_with_scene_id} ({trajectories_with_scene_id/total_trajectories*100:.1f}%)")
            logger.info(f"   - 无scene_id: {trajectories_without_scene_id} ({trajectories_without_scene_id/total_trajectories*100:.1f}%)")
            
            if trajectories_with_scene_id > 0:
                logger.info("✅ scene_id字段功能正常工作！")
            else:
                logger.warning("⚠️ 所有轨迹都没有scene_id，可能是查询失败或数据问题")
        else:
            logger.warning("⚠️ 未找到任何轨迹数据")
        
        # 清理临时文件
        Path(polygon_file).unlink(missing_ok=True)
        logger.info(f"🧹 清理临时文件: {polygon_file}")
        
        logger.info("\n✅ scene_id字段功能测试完成!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {str(e)}")
        return False

if __name__ == '__main__':
    success = test_scene_id_feature()
    exit(0 if success else 1) 