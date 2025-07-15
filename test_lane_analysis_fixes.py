#!/usr/bin/env python3
"""
测试轨迹车道分析修复
验证以下修复：
1. geometry_columns表查询修复
2. 批量分析表名唯一性修复
"""

import logging
import sys
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_geometry_columns_query():
    """测试geometry_columns表查询修复"""
    from sqlalchemy import create_engine, text
    
    LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
    
    try:
        engine = create_engine(LOCAL_DSN, future=True)
        
        # 测试修复后的查询
        check_geometry_sql = text("""
            SELECT f_geometry_column, coord_dimension 
            FROM geometry_columns 
            WHERE f_table_schema = 'public' 
            AND f_table_name = 'test_table'
            AND f_geometry_column = 'geometry'
        """)
        
        with engine.connect() as conn:
            # 这个查询应该不会报错（即使返回空结果）
            result = conn.execute(check_geometry_sql).fetchall()
            logger.info(f"✓ geometry_columns查询测试通过，返回 {len(result)} 行")
            return True
            
    except Exception as e:
        logger.error(f"❌ geometry_columns查询测试失败: {e}")
        return False

def test_table_name_uniqueness():
    """测试表名唯一性修复"""
    from src.spdatalab.fusion.trajectory_lane_analysis import TrajectoryLaneAnalyzer
    
    try:
        # 创建两个分析器实例，模拟批量分析
        config = {'enable_direction_matching': True}
        road_analysis_id = "integrated_20250715_123456_road_test"
        
        analyzer1 = TrajectoryLaneAnalyzer(config=config, road_analysis_id=road_analysis_id)
        analyzer2 = TrajectoryLaneAnalyzer(config=config, road_analysis_id=road_analysis_id)
        
        # 生成两个不同轨迹的表名
        analysis_id1 = "batch_lane_20250715_123456_trajectory_abc123def456"
        analysis_id2 = "batch_lane_20250715_123456_trajectory_xyz789uvw012"
        
        table_names1 = analyzer1._generate_dynamic_table_names(analysis_id1)
        table_names2 = analyzer2._generate_dynamic_table_names(analysis_id2)
        
        # 验证表名不同
        all_unique = True
        for table_type in table_names1:
            if table_names1[table_type] == table_names2[table_type]:
                logger.error(f"❌ 表名冲突: {table_type} - {table_names1[table_type]}")
                all_unique = False
            else:
                logger.info(f"✓ 表名唯一: {table_type}")
                logger.info(f"  轨迹1: {table_names1[table_type]}")
                logger.info(f"  轨迹2: {table_names2[table_type]}")
        
        if all_unique:
            logger.info("✓ 表名唯一性测试通过")
            return True
        else:
            logger.error("❌ 表名唯一性测试失败")
            return False
            
    except Exception as e:
        logger.error(f"❌ 表名唯一性测试失败: {e}")
        return False

def test_analysis_id_parsing():
    """测试analysis_id解析逻辑"""
    from src.spdatalab.fusion.trajectory_lane_analysis import TrajectoryLaneAnalyzer
    
    try:
        config = {'enable_direction_matching': True}
        road_analysis_id = "integrated_20250715_123456_road_test"
        analyzer = TrajectoryLaneAnalyzer(config=config, road_analysis_id=road_analysis_id)
        
        # 测试不同格式的analysis_id
        test_cases = [
            "batch_lane_20250715_123456_trajectory_abc123def456",
            "lane_analysis_xyz789uvw012_20250715_123456",
            "some_random_analysis_id_with_long_trajectory_id_12345678901234567890",
            "short_id"
        ]
        
        for analysis_id in test_cases:
            try:
                table_names = analyzer._generate_dynamic_table_names(analysis_id)
                logger.info(f"✓ analysis_id解析成功: {analysis_id}")
                logger.info(f"  生成表名: {table_names['lane_analysis_main_table']}")
                
                # 验证表名长度
                for table_type, table_name in table_names.items():
                    if len(table_name) > 63:
                        logger.warning(f"⚠️ 表名过长: {table_name} ({len(table_name)} > 63)")
                    
            except Exception as e:
                logger.error(f"❌ analysis_id解析失败: {analysis_id}, 错误: {e}")
                return False
        
        logger.info("✓ analysis_id解析测试通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ analysis_id解析测试失败: {e}")
        return False

def main():
    """主测试函数"""
    logger.info("开始轨迹车道分析修复验证")
    
    tests = [
        ("geometry_columns查询修复", test_geometry_columns_query),
        ("表名唯一性修复", test_table_name_uniqueness),
        ("analysis_id解析", test_analysis_id_parsing)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n=== {test_name} ===")
        try:
            if test_func():
                passed += 1
                logger.info(f"✓ {test_name} 通过")
            else:
                logger.error(f"❌ {test_name} 失败")
        except Exception as e:
            logger.error(f"❌ {test_name} 异常: {e}")
    
    logger.info(f"\n=== 测试结果 ===")
    logger.info(f"通过: {passed}/{total}")
    logger.info(f"失败: {total - passed}/{total}")
    
    if passed == total:
        logger.info("🎉 所有测试通过！修复验证成功")
        return 0
    else:
        logger.error("❌ 部分测试失败，需要进一步检查")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 