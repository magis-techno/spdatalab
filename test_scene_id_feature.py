#!/usr/bin/env python3
"""
测试scene_id字段功能

验证在轨迹输出数据中是否正确添加了scene_id字段
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

def test_scene_id_field():
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
        
        # 配置查询器
        config = PolygonTrajectoryConfig(
            limit_per_polygon=10,  # 限制返回数量以便观察
            fetch_complete_trajectories=True
        )
        
        query_processor = HighPerformancePolygonTrajectoryQuery(config)
        
        # 执行查询
        logger.info("🚀 执行轨迹查询...")
        trajectories, stats = query_processor.process_complete_workflow(
            polygon_geojson=polygon_file,
            output_table=None  # 不保存到数据库，仅测试功能
        )
        
        logger.info(f"📊 查询结果:")
        logger.info(f"   - 轨迹数量: {len(trajectories)}")
        logger.info(f"   - 总点数: {stats.get('total_points', 0)}")
        logger.info(f"   - 数据集数: {stats.get('unique_datasets', 0)}")
        
        # 检查scene_id字段
        if trajectories:
            logger.info("\n🔍 检查轨迹数据中的scene_id字段:")
            
            scene_id_found = 0
            valid_scene_ids = 0
            
            for i, traj in enumerate(trajectories[:5]):  # 只检查前5条
                dataset_name = traj.get('dataset_name', 'unknown')
                scene_id = traj.get('scene_id', 'missing')
                
                logger.info(f"   轨迹 {i+1}: dataset_name='{dataset_name}', scene_id='{scene_id}'")
                
                if 'scene_id' in traj:
                    scene_id_found += 1
                    if scene_id != f'unknown_{dataset_name}' and not scene_id.startswith('unknown_'):
                        valid_scene_ids += 1
            
            logger.info(f"\n📈 scene_id字段统计:")
            logger.info(f"   - 包含scene_id字段的轨迹: {scene_id_found}/{len(trajectories)}")
            logger.info(f"   - 有效scene_id的轨迹: {valid_scene_ids}/{len(trajectories)}")
            
            if scene_id_found == len(trajectories):
                logger.info("✅ 所有轨迹都包含scene_id字段!")
                
                if valid_scene_ids > 0:
                    logger.info(f"✅ 成功从数据库获取了 {valid_scene_ids} 个有效的scene_id!")
                else:
                    logger.warning("⚠️ 所有scene_id都是默认值，可能数据库查询未返回结果")
            else:
                logger.error(f"❌ 有 {len(trajectories) - scene_id_found} 条轨迹缺少scene_id字段")
            
            # 显示详细的字段结构
            if trajectories:
                sample_traj = trajectories[0]
                logger.info(f"\n📋 轨迹数据结构示例:")
                for key in sorted(sample_traj.keys()):
                    if key != 'geometry':  # 跳过几何对象
                        value = sample_traj[key]
                        logger.info(f"   - {key}: {type(value).__name__} = {value}")
        else:
            logger.warning("⚠️ 未找到任何轨迹数据")
        
        # 清理临时文件
        Path(polygon_file).unlink(missing_ok=True)
        logger.info(f"\n🧹 清理临时文件: {polygon_file}")
        
        logger.info("\n✅ scene_id字段功能测试完成!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {str(e)}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return False

if __name__ == '__main__':
    success = test_scene_id_field()
    exit(0 if success else 1) 