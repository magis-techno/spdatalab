#!/usr/bin/env python3
"""
新字段功能使用示例

展示如何使用新增的scene_id、event_id、event_name字段
"""

import logging
import json
from pathlib import Path
from src.spdatalab.dataset.polygon_trajectory_query import (
    HighPerformancePolygonTrajectoryQuery, 
    PolygonTrajectoryConfig
)

def create_example_polygon():
    """创建示例polygon"""
    example_polygon = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "id": "example_area",
                    "name": "示例测试区域",
                    "description": "用于展示新字段功能的测试区域"
                },
                "geometry": {
                    "type": "Polygon", 
                    "coordinates": [[
                        [116.30, 39.90],
                        [116.35, 39.90], 
                        [116.35, 39.95],
                        [116.30, 39.95],
                        [116.30, 39.90]
                    ]]
                }
            }
        ]
    }
    
    # 保存到文件
    example_file = "example_polygon_new_fields.geojson"
    with open(example_file, 'w', encoding='utf-8') as f:
        json.dump(example_polygon, f, ensure_ascii=False, indent=2)
    
    return example_file

def demonstrate_new_fields():
    """演示新字段功能"""
    
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("=== 新字段功能演示 ===")
    
    try:
        # 创建示例polygon
        polygon_file = create_example_polygon()
        logger.info(f"📄 创建示例polygon: {polygon_file}")
        
        # 配置查询处理器
        config = PolygonTrajectoryConfig(
            limit_per_polygon=100,  # 适中的数据量
            fetch_complete_trajectories=False,  # 专注演示新字段
            enable_speed_stats=True,
            enable_avp_stats=True
        )
        
        query_processor = HighPerformancePolygonTrajectoryQuery(config)
        
        # 执行查询
        logger.info("🚀 执行轨迹查询...")
        trajectories, stats = query_processor.process_complete_workflow(
            polygon_geojson=polygon_file,
            output_table="demo_new_fields_trajectories"  # 保存到数据库演示
        )
        
        # 显示查询结果概览
        logger.info("\n📊 查询结果概览:")
        logger.info(f"   - 总轨迹数: {len(trajectories)}")
        logger.info(f"   - 总轨迹点数: {stats.get('total_points', 0)}")
        logger.info(f"   - 涉及数据集数: {stats.get('unique_datasets', 0)}")
        logger.info(f"   - 查询时间: {stats.get('query_time', 0):.2f}s")
        
        # 演示新字段
        if trajectories:
            logger.info("\n🔍 新字段详细信息:")
            
            for i, traj in enumerate(trajectories[:5], 1):  # 显示前5个轨迹
                logger.info(f"\n   === 轨迹 {i} ===")
                logger.info(f"   📛 dataset_name: {traj.get('dataset_name')}")
                logger.info(f"   🏷️  scene_id: {traj.get('scene_id') or '未找到'}")
                logger.info(f"   🔢 event_id: {traj.get('event_id')}")
                logger.info(f"   📝 event_name: {traj.get('event_name')}")
                logger.info(f"   📍 点数: {traj.get('point_count')}")
                logger.info(f"   ⏱️  持续时间: {traj.get('duration')}秒")
                
                # 显示速度信息（如果有）
                if traj.get('avg_speed') is not None:
                    logger.info(f"   🏃 平均速度: {traj.get('avg_speed')} m/s")
                    logger.info(f"   ⚡ 最大速度: {traj.get('max_speed')} m/s")
                
                # 显示相交的polygon
                polygon_ids = traj.get('polygon_ids', [])
                logger.info(f"   🎯 相交polygon: {', '.join(polygon_ids)}")
            
            # 统计新字段的情况
            logger.info("\n📈 新字段统计:")
            
            # scene_id统计
            scene_ids_found = sum(1 for traj in trajectories if traj.get('scene_id'))
            scene_ids_empty = len(trajectories) - scene_ids_found
            logger.info(f"   - 成功查询到scene_id: {scene_ids_found}/{len(trajectories)} ({scene_ids_found/len(trajectories)*100:.1f}%)")
            logger.info(f"   - 未查询到scene_id: {scene_ids_empty}/{len(trajectories)} ({scene_ids_empty/len(trajectories)*100:.1f}%)")
            
            # event_id范围
            event_ids = [traj.get('event_id') for traj in trajectories if traj.get('event_id')]
            if event_ids:
                logger.info(f"   - event_id范围: {min(event_ids)} - {max(event_ids)}")
            
            # event_name格式
            event_names = [traj.get('event_name') for traj in trajectories[:3] if traj.get('event_name')]
            if event_names:
                logger.info(f"   - event_name示例: {', '.join(event_names)}")
            
            # 数据库保存结果
            if stats.get('save_stats'):
                save_stats = stats['save_stats']
                logger.info(f"\n💾 数据库保存结果:")
                logger.info(f"   - 保存记录数: {save_stats.get('saved_records', 0)}")
                logger.info(f"   - 保存时间: {save_stats.get('save_time', 0):.2f}s")
                logger.info(f"   - 批次数: {save_stats.get('batch_count', 0)}")
                logger.info(f"   - 表名: demo_new_fields_trajectories")
        else:
            logger.warning("⚠️ 未找到轨迹数据")
        
        # 清理临时文件
        Path(polygon_file).unlink(missing_ok=True)
        logger.info(f"\n🧹 清理临时文件: {polygon_file}")
        
        logger.info("\n✅ 新字段功能演示完成!")
        logger.info("\n💡 使用说明:")
        logger.info("   - scene_id: 通过data_name反查数据库获得，如果查不到则为空")
        logger.info("   - event_id: 自动递增的整数ID，从1开始")
        logger.info("   - event_name: 基于event_id和dataset_name生成的事件名称")
        logger.info("\n🗃️ 数据库字段类型:")
        logger.info("   - scene_id: text (可为NULL)")
        logger.info("   - event_id: integer")
        logger.info("   - event_name: varchar(765)")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 演示失败: {str(e)}")
        return False

if __name__ == '__main__':
    success = demonstrate_new_fields()
    print(f"\n{'=' * 50}")
    print("✅ 演示成功完成!" if success else "❌ 演示运行失败!")
    print("新字段功能已就绪，可以在实际项目中使用。")
    exit(0 if success else 1) 