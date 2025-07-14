#!/usr/bin/env python3
"""测试轨迹邻近性分析修复"""

import logging
from src.spdatalab.fusion.trajectory_lane_analysis import TrajectoryLaneAnalyzer

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_trajectory_neighbors_analysis():
    """测试轨迹邻近性分析"""
    
    # 使用road analysis的结果
    road_analysis_id = "integrated_20250714_031801_road_f8f65ca59e094aa89f3121fa2510c506"
    
    # 测试轨迹几何（使用之前的轨迹）
    input_trajectory_id = "test_trajectory_001"
    input_trajectory_geom = "LINESTRING(116.397 39.916, 116.398 39.917, 116.399 39.918, 116.400 39.919)"
    
    logger.info("=== 测试轨迹邻近性分析 ===")
    logger.info(f"输入轨迹ID: {input_trajectory_id}")
    logger.info(f"道路分析ID: {road_analysis_id}")
    
    try:
        # 配置分析器
        # 根据集成分析的逻辑，lanes表名是 road_analysis_id + "_lanes"
        lanes_table_name = f"{road_analysis_id}_lanes"
        
        config = {
            'road_analysis_lanes_table': lanes_table_name,
            'buffer_radius': 15.0,
            'max_lane_distance': 50.0,
            'min_points_single_lane': 5
        }
        
        logger.info(f"使用候选车道表: {lanes_table_name}")
        
        # 创建分析器
        analyzer = TrajectoryLaneAnalyzer(config=config, road_analysis_id=road_analysis_id)
        
        # 执行邻近性分析
        logger.info("开始执行邻近性分析...")
        analysis_result = analyzer.analyze_trajectory_neighbors(input_trajectory_id, input_trajectory_geom)
        
        # 检查结果
        if 'error' in analysis_result:
            logger.error(f"❌ 分析失败: {analysis_result['error']}")
            return False
        
        # 输出结果统计
        stats = analysis_result['stats']
        logger.info("✓ 分析完成！")
        logger.info(f"候选lanes: {stats['candidate_lanes_found']} 个")
        logger.info(f"轨迹点数: {stats['trajectory_points_found']} 个")
        logger.info(f"data_name数: {stats['unique_data_names_found']} 个")
        logger.info(f"符合条件的轨迹: {stats['trajectories_passed_filter']} 个")
        logger.info(f"  - 多车道: {stats['trajectories_multi_lane']} 个")
        logger.info(f"  - 足够点数: {stats['trajectories_sufficient_points']} 个")
        logger.info(f"完整轨迹数: {len(analysis_result['complete_trajectories'])} 个")
        
        # 输出符合条件的轨迹详情
        if analysis_result['complete_trajectories']:
            logger.info("\n=== 符合条件的轨迹详情 ===")
            for data_name, trajectory in analysis_result['complete_trajectories'].items():
                logger.info(f"轨迹: {data_name}")
                logger.info(f"  - 过滤原因: {trajectory['filter_reason']}")
                logger.info(f"  - 涉及lanes: {len(trajectory['lanes_touched'])} 个 {trajectory['lanes_touched']}")
                logger.info(f"  - 命中点数: {trajectory['hit_points_count']} 个")
                logger.info(f"  - 总点数: {trajectory['total_points']} 个")
                logger.info(f"  - 轨迹长度: {trajectory['trajectory_length']:.6f} 度")
                logger.info(f"  - 平均速度: {trajectory['avg_speed']} km/h")
        else:
            logger.warning("没有找到符合条件的轨迹")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = test_trajectory_neighbors_analysis()
    if success:
        logger.info("✅ 测试通过")
    else:
        logger.error("❌ 测试失败")
        exit(1) 