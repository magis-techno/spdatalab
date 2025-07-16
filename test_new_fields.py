#!/usr/bin/env python3
"""
测试新增字段功能

验证scene_id、event_id、event_name三个新字段是否正确添加和工作
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
                    "id": "test_new_fields",
                    "name": "新字段测试区域"
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
    test_file = "test_new_fields_polygon.geojson"
    with open(test_file, 'w', encoding='utf-8') as f:
        json.dump(test_polygon, f, ensure_ascii=False, indent=2)
    
    return test_file

def test_new_fields():
    """测试新增字段功能"""
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=== 新字段功能测试 ===")
    
    try:
        # 创建测试polygon
        polygon_file = create_test_polygon()
        logger.info(f"✅ 创建测试polygon文件: {polygon_file}")
        
        # 创建配置（限制数据量）
        config = PolygonTrajectoryConfig(
            limit_per_polygon=50,  # 限制数据量用于测试
            fetch_complete_trajectories=False  # 不获取完整轨迹，专注测试新字段
        )
        
        query_processor = HighPerformancePolygonTrajectoryQuery(config)
        
        # 执行查询和轨迹构建
        logger.info("🚀 执行查询和轨迹构建...")
        trajectories, stats = query_processor.process_complete_workflow(
            polygon_geojson=polygon_file,
            output_table=None  # 不保存到数据库，仅测试构建过程
        )
        
        logger.info(f"📊 查询结果:")
        logger.info(f"   - 轨迹数量: {len(trajectories)}")
        logger.info(f"   - 总点数: {stats.get('total_points', 0)}")
        logger.info(f"   - 数据集数: {stats.get('unique_datasets', 0)}")
        
        # 检查新字段
        if trajectories:
            logger.info("\n🔍 检查新字段:")
            
            for i, traj in enumerate(trajectories[:3], 1):  # 只检查前3个轨迹
                logger.info(f"   轨迹 {i}:")
                logger.info(f"     - dataset_name: {traj.get('dataset_name', 'N/A')}")
                logger.info(f"     - scene_id: {traj.get('scene_id', 'N/A')}")
                logger.info(f"     - event_id: {traj.get('event_id', 'N/A')}")
                logger.info(f"     - event_name: {traj.get('event_name', 'N/A')}")
                logger.info(f"     - point_count: {traj.get('point_count', 'N/A')}")
            
            # 验证字段存在性
            required_fields = ['scene_id', 'event_id', 'event_name']
            missing_fields = []
            
            for field in required_fields:
                if field not in trajectories[0]:
                    missing_fields.append(field)
            
            if not missing_fields:
                logger.info("✅ 所有新字段都已正确添加!")
                
                # 验证字段值的合理性
                valid_event_ids = all(isinstance(traj.get('event_id'), int) for traj in trajectories)
                valid_event_names = all(isinstance(traj.get('event_name'), str) and traj.get('event_name') for traj in trajectories)
                
                logger.info(f"   - event_id类型正确: {valid_event_ids}")
                logger.info(f"   - event_name类型正确: {valid_event_names}")
                
                # 检查scene_id查询结果
                scene_ids_found = sum(1 for traj in trajectories if traj.get('scene_id'))
                scene_ids_empty = sum(1 for traj in trajectories if not traj.get('scene_id'))
                
                logger.info(f"   - 找到scene_id: {scene_ids_found} 个轨迹")
                logger.info(f"   - 未找到scene_id: {scene_ids_empty} 个轨迹")
                
            else:
                logger.error(f"❌ 缺失字段: {missing_fields}")
                return False
        else:
            logger.warning("⚠️ 没有找到轨迹数据，无法测试新字段")
        
        # 测试数据库表创建（如果需要）
        if len(trajectories) > 0:
            logger.info("\n🗃️ 测试数据库表创建...")
            test_table = "test_new_fields_trajectories"
            
            try:
                # 测试表创建
                if query_processor._create_trajectory_table(test_table):
                    logger.info(f"✅ 数据库表创建成功: {test_table}")
                    
                    # 测试数据保存
                    logger.info("🔄 测试数据保存...")
                    saved_count, save_stats = query_processor.save_trajectories_to_table(
                        trajectories[:2],  # 只保存前2条测试数据
                        test_table
                    )
                    
                    if saved_count > 0:
                        logger.info(f"✅ 数据保存成功: {saved_count} 条记录")
                        logger.info(f"   - 保存时间: {save_stats.get('save_time', 0):.2f}s")
                    else:
                        logger.warning("⚠️ 数据保存失败")
                else:
                    logger.error("❌ 数据库表创建失败")
            except Exception as e:
                logger.error(f"❌ 数据库操作失败: {str(e)}")
        
        # 清理临时文件
        Path(polygon_file).unlink(missing_ok=True)
        logger.info(f"🧹 清理临时文件: {polygon_file}")
        
        logger.info("\n✅ 新字段功能测试完成!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {str(e)}")
        return False

if __name__ == '__main__':
    success = test_new_fields()
    exit(0 if success else 1) 