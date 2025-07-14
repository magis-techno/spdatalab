#!/usr/bin/env python
"""
轨迹集成分析系统快速测试脚本

这个脚本演示了如何使用新的轨迹集成分析系统：
1. 创建测试GeoJSON数据
2. 验证数据格式
3. 执行集成分析
4. 检查结果

使用方法：
    python quick_test_integration.py
"""

import json
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_test_geojson():
    """创建测试GeoJSON数据"""
    logger.info("创建测试GeoJSON数据...")
    
    # 基础时间戳
    base_time = datetime.now()
    
    # 构造测试轨迹数据（使用北京市区坐标）
    test_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "scene_id": "quick_test_001",
                    "data_name": "quick_trajectory_001",
                    "start_time": int(base_time.timestamp()),
                    "end_time": int((base_time + timedelta(minutes=5)).timestamp()),
                    "avg_speed": 15.5,
                    "max_speed": 25.0,
                    "avp_flag": 1
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [116.3974, 39.9093],  # 天安门
                        [116.4074, 39.9093],  # 向东约1km
                        [116.4074, 39.9193],  # 向北约1km
                        [116.4174, 39.9193],  # 向东约1km
                        [116.4174, 39.9293]   # 向北约1km
                    ]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "scene_id": "quick_test_002",
                    "data_name": "quick_trajectory_002",
                    "start_time": int((base_time + timedelta(minutes=10)).timestamp()),
                    "end_time": int((base_time + timedelta(minutes=18)).timestamp()),
                    "avg_speed": 12.8,
                    "max_speed": 22.0,
                    "avp_flag": 0
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [116.4274, 39.9293],  # 接续上条轨迹
                        [116.4374, 39.9293],  # 向东约1km
                        [116.4374, 39.9393],  # 向北约1km
                        [116.4474, 39.9393],  # 向东约1km
                        [116.4474, 39.9493]   # 向北约1km
                    ]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "scene_id": "quick_test_003",
                    "data_name": "quick_trajectory_003",
                    "start_time": int((base_time + timedelta(minutes=20)).timestamp()),
                    "end_time": int((base_time + timedelta(minutes=25)).timestamp()),
                    "avg_speed": 18.2,
                    "max_speed": 28.5,
                    "avp_flag": 1
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [116.4574, 39.9493],  # 继续向东
                        [116.4674, 39.9493],
                        [116.4674, 39.9593],
                        [116.4774, 39.9593],
                        [116.4774, 39.9693]
                    ]
                }
            }
        ]
    }
    
    # 保存文件
    filename = "quick_test_trajectories.geojson"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(test_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"✓ 创建测试文件: {filename}")
    logger.info(f"  - 包含 {len(test_data['features'])} 条轨迹")
    
    return filename

def validate_geojson(filename):
    """验证GeoJSON格式"""
    logger.info(f"验证GeoJSON格式: {filename}")
    
    try:
        from src.spdatalab.fusion.trajectory_data_processor import TrajectoryDataProcessor
        
        processor = TrajectoryDataProcessor()
        validation_result = processor.validate_geojson(filename)
        
        if validation_result.is_valid:
            logger.info("✓ GeoJSON格式验证通过")
            logger.info(f"  - 有效轨迹数: {validation_result.valid_count}")
            logger.info(f"  - 错误轨迹数: {validation_result.error_count}")
            
            if validation_result.warnings:
                logger.warning("验证警告:")
                for warning in validation_result.warnings:
                    logger.warning(f"  - {warning}")
            
            return True
        else:
            logger.error("✗ GeoJSON格式验证失败")
            logger.error(f"  - 有效轨迹数: {validation_result.valid_count}")
            logger.error(f"  - 错误轨迹数: {validation_result.error_count}")
            
            if validation_result.errors:
                logger.error("验证错误:")
                for error in validation_result.errors:
                    logger.error(f"  - {error}")
            
            return False
            
    except Exception as e:
        logger.error(f"验证过程出错: {e}")
        return False

def run_integration_analysis(filename):
    """运行集成分析"""
    logger.info(f"执行轨迹集成分析: {filename}")
    
    try:
        from src.spdatalab.fusion.trajectory_integrated_analysis import (
            TrajectoryIntegratedAnalyzer,
            TrajectoryIntegratedAnalysisConfig
        )
        
        # 创建配置
        config = TrajectoryIntegratedAnalysisConfig(
            # 启用道路分析
            enable_road_analysis=True,
            # 暂时禁用lane分析（Phase 1 未完成）
            enable_lane_analysis=False,
            # 分析参数
            buffer_distance=3.0,
            batch_size=2,
            output_prefix="quick_test"
        )
        
        # 创建分析器
        analyzer = TrajectoryIntegratedAnalyzer(config)
        
        # 执行分析
        analysis_result = analyzer.analyze_trajectories_from_geojson(filename)
        
        if analysis_result:
            logger.info("✓ 集成分析完成")
            logger.info(f"  - 分析ID: {analysis_result.analysis_id}")
            logger.info(f"  - 轨迹总数: {analysis_result.trajectory_count}")
            logger.info(f"  - 成功处理: {analysis_result.success_count}")
            logger.info(f"  - 处理失败: {analysis_result.error_count}")
            
            # 显示分析统计
            if hasattr(analysis_result, 'total_lanes'):
                logger.info(f"  - 总lane数: {analysis_result.total_lanes}")
            if hasattr(analysis_result, 'total_intersections'):
                logger.info(f"  - 总intersection数: {analysis_result.total_intersections}")
            if hasattr(analysis_result, 'total_roads'):
                logger.info(f"  - 总road数: {analysis_result.total_roads}")
            
            return analysis_result
        else:
            logger.error("✗ 集成分析失败")
            return None
            
    except Exception as e:
        logger.error(f"分析过程出错: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return None

def check_database_results(analysis_id=None):
    """检查数据库结果"""
    logger.info("检查数据库结果...")
    
    try:
        from sqlalchemy import create_engine, text
        
        # 创建数据库连接
        dsn = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
        engine = create_engine(dsn)
        
        with engine.connect() as conn:
            # 检查分析结果表
            result = conn.execute(text("""
                SELECT 
                    analysis_id,
                    trajectory_count,
                    success_count,
                    error_count,
                    created_at
                FROM trajectory_integrated_analysis
                ORDER BY created_at DESC
                LIMIT 5
            """))
            
            analysis_results = result.fetchall()
            
            if analysis_results:
                logger.info("✓ 找到分析结果:")
                for row in analysis_results:
                    logger.info(f"  - {row[0]}: {row[1]}条轨迹, {row[2]}成功, {row[3]}失败, {row[4]}")
            else:
                logger.warning("未找到分析结果")
            
            # 检查轨迹汇总表
            result = conn.execute(text("""
                SELECT 
                    analysis_id,
                    trajectory_id,
                    scene_id,
                    processing_status,
                    road_analysis_id
                FROM trajectory_integrated_summary
                ORDER BY created_at DESC
                LIMIT 10
            """))
            
            trajectory_results = result.fetchall()
            
            if trajectory_results:
                logger.info("✓ 找到轨迹处理记录:")
                for row in trajectory_results:
                    logger.info(f"  - {row[1]} ({row[2]}): {row[3]}, road_analysis: {row[4]}")
            else:
                logger.warning("未找到轨迹处理记录")
                
    except Exception as e:
        logger.error(f"数据库检查失败: {e}")

def cleanup_test_files():
    """清理测试文件"""
    logger.info("清理测试文件...")
    
    files_to_remove = [
        "quick_test_trajectories.geojson",
        "trajectory_integration.log"
    ]
    
    for filename in files_to_remove:
        if os.path.exists(filename):
            os.remove(filename)
            logger.info(f"✓ 删除文件: {filename}")

def main():
    """主函数"""
    logger.info("=" * 50)
    logger.info("轨迹集成分析系统快速测试")
    logger.info("=" * 50)
    
    try:
        # 1. 创建测试数据
        test_file = create_test_geojson()
        
        # 2. 验证数据格式
        if not validate_geojson(test_file):
            logger.error("数据格式验证失败，停止测试")
            return
        
        # 3. 执行集成分析
        analysis_result = run_integration_analysis(test_file)
        
        if analysis_result:
            # 4. 检查数据库结果
            check_database_results(analysis_result.analysis_id)
            
            logger.info("=" * 50)
            logger.info("✓ 快速测试完成！")
            logger.info("=" * 50)
            
            # 提供后续操作建议
            logger.info("后续操作建议：")
            logger.info("1. 查看QGIS可视化结果")
            logger.info("2. 检查详细的分析日志")
            logger.info("3. 运行更大规模的测试")
            
        else:
            logger.error("集成分析失败，请检查日志")
            
    except Exception as e:
        logger.error(f"快速测试失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        
    finally:
        # 询问是否清理测试文件
        try:
            response = input("\n是否清理测试文件？(y/n): ")
            if response.lower() in ['y', 'yes']:
                cleanup_test_files()
        except KeyboardInterrupt:
            logger.info("\n用户取消操作")

if __name__ == "__main__":
    main() 