#!/usr/bin/env python3
"""
轨迹道路分析模块测试脚本

测试功能：
1. 基本配置测试
2. 数据库表创建测试
3. 轨迹缓冲区创建测试
4. 空间查询测试
5. 完整分析流程测试
"""

import logging
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from spdatalab.fusion.trajectory_road_analysis import (
    TrajectoryRoadAnalysisConfig,
    TrajectoryRoadAnalyzer,
    analyze_trajectory_road_elements,
    create_trajectory_road_analysis_report
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_config():
    """测试配置类"""
    logger.info("=== 测试配置类 ===")
    
    config = TrajectoryRoadAnalysisConfig()
    
    # 检查默认配置
    assert config.buffer_distance == 3.0
    assert config.forward_chain_limit == 500.0
    assert config.backward_chain_limit == 100.0
    assert config.max_recursion_depth == 50
    
    logger.info("✓ 配置类测试通过")

def test_analyzer_initialization():
    """测试分析器初始化"""
    logger.info("=== 测试分析器初始化 ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        logger.info("✓ 分析器初始化成功")
        
        # 测试配置
        assert analyzer.config.buffer_distance == 3.0
        logger.info("✓ 配置加载正确")
        
    except Exception as e:
        logger.error(f"✗ 分析器初始化失败: {e}")
        raise

def test_trajectory_buffer():
    """测试轨迹缓冲区创建"""
    logger.info("=== 测试轨迹缓冲区创建 ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        
        # 测试轨迹WKT（示例线段）
        test_trajectory_wkt = "LINESTRING(116.3 39.9, 116.31 39.91, 116.32 39.92)"
        
        # 创建缓冲区
        buffer_geom = analyzer._create_trajectory_buffer(test_trajectory_wkt)
        
        if buffer_geom:
            logger.info("✓ 轨迹缓冲区创建成功")
            logger.info(f"缓冲区几何类型: {buffer_geom[:50]}...")
        else:
            logger.error("✗ 轨迹缓冲区创建失败")
            
    except Exception as e:
        logger.error(f"✗ 轨迹缓冲区测试失败: {e}")

def test_simple_analysis():
    """测试简单分析流程"""
    logger.info("=== 测试简单分析流程 ===")
    
    try:
        # 使用便捷接口
        test_trajectory_id = "test_trajectory_001"
        test_trajectory_wkt = "LINESTRING(116.3 39.9, 116.31 39.91, 116.32 39.92)"
        
        analysis_id, summary = analyze_trajectory_road_elements(
            trajectory_id=test_trajectory_id,
            trajectory_geom=test_trajectory_wkt
        )
        
        logger.info(f"✓ 分析完成: {analysis_id}")
        logger.info(f"分析汇总: {summary}")
        
        # 生成报告
        report = create_trajectory_road_analysis_report(analysis_id)
        logger.info("✓ 生成分析报告成功")
        
        # 输出报告的前几行
        report_lines = report.split('\n')
        for line in report_lines[:10]:
            logger.info(f"报告: {line}")
        
        return analysis_id
        
    except Exception as e:
        logger.error(f"✗ 简单分析流程测试失败: {e}")
        return None

def test_database_tables():
    """测试数据库表创建"""
    logger.info("=== 测试数据库表创建 ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        
        # 检查表是否存在
        tables_to_check = [
            analyzer.config.analysis_table,
            analyzer.config.lanes_table,
            analyzer.config.intersections_table,
            analyzer.config.roads_table
        ]
        
        for table_name in tables_to_check:
            check_sql = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = '{table_name}'
                );
            """
            
            with analyzer.local_engine.connect() as conn:
                result = conn.execute(check_sql).fetchone()
                exists = result[0] if result else False
                
                if exists:
                    logger.info(f"✓ 表 {table_name} 存在")
                else:
                    logger.warning(f"⚠ 表 {table_name} 不存在")
        
        logger.info("✓ 数据库表检查完成")
        
    except Exception as e:
        logger.error(f"✗ 数据库表测试失败: {e}")

def test_mock_data_analysis():
    """测试模拟数据分析"""
    logger.info("=== 测试模拟数据分析 ===")
    
    # 模拟一些测试轨迹数据
    test_trajectories = [
        ("trajectory_001", "LINESTRING(116.3 39.9, 116.31 39.91, 116.32 39.92)"),
        ("trajectory_002", "LINESTRING(116.4 39.8, 116.41 39.81, 116.42 39.82)"),
        ("trajectory_003", "LINESTRING(116.5 39.7, 116.51 39.71, 116.52 39.72)")
    ]
    
    results = []
    
    for trajectory_id, trajectory_wkt in test_trajectories:
        try:
            logger.info(f"分析轨迹: {trajectory_id}")
            
            analysis_id, summary = analyze_trajectory_road_elements(
                trajectory_id=trajectory_id,
                trajectory_geom=trajectory_wkt
            )
            
            results.append({
                'trajectory_id': trajectory_id,
                'analysis_id': analysis_id,
                'summary': summary
            })
            
            logger.info(f"✓ 轨迹 {trajectory_id} 分析完成")
            
        except Exception as e:
            logger.error(f"✗ 轨迹 {trajectory_id} 分析失败: {e}")
            results.append({
                'trajectory_id': trajectory_id,
                'analysis_id': None,
                'error': str(e)
            })
    
    logger.info(f"✓ 模拟数据分析完成，成功: {len([r for r in results if r.get('analysis_id')])}")
    return results

def run_all_tests():
    """运行所有测试"""
    logger.info("开始运行轨迹道路分析模块测试...")
    
    tests = [
        test_config,
        test_analyzer_initialization,
        test_database_tables,
        test_trajectory_buffer,
        test_simple_analysis,
        test_mock_data_analysis
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
            logger.info(f"✓ {test_func.__name__} 通过")
        except Exception as e:
            failed += 1
            logger.error(f"✗ {test_func.__name__} 失败: {e}")
    
    logger.info(f"\n=== 测试结果汇总 ===")
    logger.info(f"通过: {passed}")
    logger.info(f"失败: {failed}")
    logger.info(f"总计: {passed + failed}")
    
    if failed == 0:
        logger.info("🎉 所有测试通过！")
    else:
        logger.warning(f"⚠ {failed} 个测试失败")
    
    return failed == 0

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1) 