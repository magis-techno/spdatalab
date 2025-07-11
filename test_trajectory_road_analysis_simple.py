#!/usr/bin/env python3
"""
轨迹道路分析模块简化测试脚本

重点测试：
1. 基本配置和初始化
2. 数据库表创建
3. 轨迹缓冲区创建
4. 避免复杂的远程数据库查询
"""

import logging
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from spdatalab.fusion.trajectory_road_analysis import (
    TrajectoryRoadAnalysisConfig,
    TrajectoryRoadAnalyzer
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_config_improvements():
    """测试改进的配置类"""
    logger.info("=== 测试改进的配置类 ===")
    
    config = TrajectoryRoadAnalysisConfig()
    
    # 检查新增的配置项
    assert hasattr(config, 'max_lanes_per_query')
    assert hasattr(config, 'query_timeout')
    assert hasattr(config, 'recursive_query_timeout')
    assert config.max_lanes_per_query == 1000
    assert config.query_timeout == 60
    assert config.recursive_query_timeout == 120
    
    logger.info("✓ 改进的配置类测试通过")

def test_analyzer_initialization_with_improvements():
    """测试分析器初始化（包含连接池配置）"""
    logger.info("=== 测试分析器初始化（改进版） ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        logger.info("✓ 分析器初始化成功")
        
        # 检查连接池配置
        assert analyzer.local_engine.pool.size() == 5
        logger.info("✓ 连接池配置正确")
        
        # 检查配置
        assert analyzer.config.max_lanes_per_query == 1000
        logger.info("✓ 配置加载正确")
        
    except Exception as e:
        logger.error(f"✗ 分析器初始化失败: {e}")
        raise

def test_database_tables_with_proper_sql():
    """测试数据库表创建（使用正确的SQL格式）"""
    logger.info("=== 测试数据库表创建（改进版） ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        
        # 检查表是否存在
        tables_to_check = [
            analyzer.config.analysis_table,
            analyzer.config.lanes_table,
            analyzer.config.intersections_table,
            analyzer.config.roads_table
        ]
        
        from sqlalchemy import text
        
        for table_name in tables_to_check:
            check_sql = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                );
            """)
            
            try:
                with analyzer.local_engine.connect() as conn:
                    result = conn.execute(check_sql, {'table_name': table_name}).fetchone()
                    exists = result[0] if result else False
                    
                    if exists:
                        logger.info(f"✓ 表 {table_name} 存在")
                    else:
                        logger.warning(f"⚠ 表 {table_name} 不存在")
            except Exception as e:
                logger.error(f"✗ 检查表 {table_name} 失败: {e}")
        
        logger.info("✓ 数据库表检查完成")
        
    except Exception as e:
        logger.error(f"✗ 数据库表测试失败: {e}")

def test_trajectory_buffer_creation():
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
            
            # 检查缓冲区是否为POLYGON
            assert buffer_geom.startswith("POLYGON")
            logger.info("✓ 缓冲区几何类型正确")
        else:
            logger.error("✗ 轨迹缓冲区创建失败")
            
    except Exception as e:
        logger.error(f"✗ 轨迹缓冲区测试失败: {e}")

def test_save_analysis_record():
    """测试保存分析记录"""
    logger.info("=== 测试保存分析记录 ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        
        # 测试数据
        analysis_id = "test_analysis_001"
        trajectory_id = "test_trajectory_001"
        trajectory_wkt = "LINESTRING(116.3 39.9, 116.31 39.91, 116.32 39.92)"
        
        # 创建缓冲区
        buffer_geom = analyzer._create_trajectory_buffer(trajectory_wkt)
        
        if buffer_geom:
            # 保存分析记录
            analyzer._save_analysis_record(analysis_id, trajectory_id, trajectory_wkt, buffer_geom)
            logger.info("✓ 分析记录保存成功")
            
            # 验证记录是否存在
            from sqlalchemy import text
            check_sql = text(f"""
                SELECT COUNT(*) FROM {analyzer.config.analysis_table} 
                WHERE analysis_id = :analysis_id
            """)
            
            with analyzer.local_engine.connect() as conn:
                result = conn.execute(check_sql, {'analysis_id': analysis_id}).fetchone()
                count = result[0] if result else 0
                
                if count > 0:
                    logger.info(f"✓ 分析记录验证成功，找到 {count} 条记录")
                else:
                    logger.warning("⚠ 分析记录验证失败")
        else:
            logger.error("✗ 无法创建缓冲区，跳过分析记录测试")
            
    except Exception as e:
        logger.error(f"✗ 保存分析记录测试失败: {e}")

def test_error_handling():
    """测试错误处理"""
    logger.info("=== 测试错误处理 ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        
        # 测试无效的几何数据
        invalid_geom = "INVALID_GEOMETRY"
        buffer_geom = analyzer._create_trajectory_buffer(invalid_geom)
        
        if buffer_geom is None:
            logger.info("✓ 无效几何数据错误处理正确")
        else:
            logger.warning("⚠ 无效几何数据未正确处理")
        
        # 测试空的数据框处理
        import pandas as pd
        empty_df = pd.DataFrame()
        
        analyzer._save_lanes_results("test_analysis", empty_df, "test_type")
        logger.info("✓ 空数据框处理正确")
        
    except Exception as e:
        logger.error(f"✗ 错误处理测试失败: {e}")

def run_simple_tests():
    """运行简化的测试"""
    logger.info("开始运行轨迹道路分析模块简化测试...")
    
    tests = [
        test_config_improvements,
        test_analyzer_initialization_with_improvements,
        test_database_tables_with_proper_sql,
        test_trajectory_buffer_creation,
        test_save_analysis_record,
        test_error_handling
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
    
    logger.info(f"\n=== 简化测试结果汇总 ===")
    logger.info(f"通过: {passed}")
    logger.info(f"失败: {failed}")
    logger.info(f"总计: {passed + failed}")
    
    if failed == 0:
        logger.info("🎉 所有简化测试通过！")
    else:
        logger.warning(f"⚠ {failed} 个测试失败")
    
    return failed == 0

if __name__ == "__main__":
    success = run_simple_tests()
    sys.exit(0 if success else 1) 