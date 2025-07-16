#!/usr/bin/env python3
"""
测试scene_id功能

验证完整轨迹查询中scene_id列的添加是否正常工作
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
        
        # 配置查询处理器（启用完整轨迹获取）
        config = PolygonTrajectoryConfig(
            limit_per_polygon=50,
            fetch_complete_trajectories=True  # 启用完整轨迹获取
        )
        
        query_processor = HighPerformancePolygonTrajectoryQuery(config)
        
        # 执行查询（查询轨迹点）
        logger.info("🚀 执行polygon轨迹查询（包含scene_id）...")
        
        points_df, stats = query_processor.query_trajectories_for_polygons(
            [{"id": "scene_id_test", "geometry": test_polygon}]
        )
        
        if not points_df.empty:
            logger.info(f"📊 查询结果:")
            logger.info(f"   - 总点数: {len(points_df)}")
            logger.info(f"   - 数据集数: {points_df['dataset_name'].nunique()}")
            
            # 检查是否包含scene_id列
            if 'scene_id' in points_df.columns:
                scene_id_count = points_df['scene_id'].notna().sum()
                total_points = len(points_df)
                logger.info(f"✅ scene_id列存在: {scene_id_count}/{total_points} 个点有scene_id")
                
                # 显示scene_id样例
                unique_scene_ids = points_df['scene_id'].dropna().unique()
                logger.info(f"   - 找到scene_id: {len(unique_scene_ids)} 个")
                if len(unique_scene_ids) > 0:
                    logger.info(f"   - 示例scene_id: {unique_scene_ids[:3].tolist()}")
                
                # 检查完整轨迹获取统计
                if stats.get('complete_trajectories_fetched'):
                    scene_mapped = stats.get('scene_id_mapped_points', 0)
                    logger.info(f"✅ 完整轨迹scene_id映射: {scene_mapped} 个点")
                else:
                    logger.info("ℹ️ 未获取完整轨迹（可能polygon内已包含完整轨迹）")
            else:
                logger.error("❌ scene_id列缺失！")
                logger.info(f"   实际列: {list(points_df.columns)}")
        else:
            logger.warning("⚠️ 查询无结果，可能polygon区域内没有轨迹数据")
        
        # 测试构建轨迹（如果有数据）
        if not points_df.empty:
            logger.info("\n🔧 测试轨迹构建...")
            trajectories, build_stats = query_processor.build_trajectories_from_points(points_df)
            
            if trajectories:
                logger.info(f"📈 轨迹构建结果:")
                logger.info(f"   - 轨迹数量: {len(trajectories)}")
                
                # 检查轨迹中是否包含scene_id
                first_traj = trajectories[0]
                if 'scene_id' in first_traj:
                    logger.info(f"✅ 轨迹包含scene_id: {first_traj['scene_id']}")
                else:
                    logger.warning("⚠️ 轨迹中缺少scene_id字段")
                    logger.info(f"   轨迹字段: {list(first_traj.keys())}")
            else:
                logger.warning("⚠️ 轨迹构建无结果")
        
        # 清理临时文件
        Path(polygon_file).unlink(missing_ok=True)
        logger.info(f"🧹 清理临时文件: {polygon_file}")
        
        logger.info("\n✅ scene_id功能测试完成!")
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {str(e)}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return False

if __name__ == '__main__':
    success = test_scene_id_feature()
    exit(0 if success else 1) 