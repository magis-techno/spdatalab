#!/usr/bin/env python3
"""
测试车道分析修复效果的脚本
"""

import sys
import os
import logging
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def test_lane_analysis_config():
    """测试车道分析配置传递"""
    logger = setup_logging()
    
    logger.info("测试车道分析配置传递...")
    
    try:
        from spdatalab.fusion.trajectory_lane_analysis import TrajectoryLaneAnalyzer
        
        # 测试1: 创建车道分析器，使用正确的配置
        test_config = {
            'road_analysis_lanes_table': 'integrated_20250714_030456_road_lanes',
            'sampling_strategy': 'distance',
            'distance_interval': 10.0
        }
        
        analyzer = TrajectoryLaneAnalyzer(
            config=test_config,
            road_analysis_id='integrated_20250714_030456_road_f8f65ca59e094aa89f3121fa2510c506'
        )
        
        # 验证配置是否正确设置
        assert analyzer.config['road_analysis_lanes_table'] == 'integrated_20250714_030456_road_lanes'
        assert analyzer.road_analysis_id == 'integrated_20250714_030456_road_f8f65ca59e094aa89f3121fa2510c506'
        
        logger.info("✓ 车道分析器配置传递测试通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ 车道分析器配置传递测试失败: {e}")
        return False

def test_database_connection():
    """测试数据库连接和表查询"""
    logger = setup_logging()
    
    logger.info("测试数据库连接...")
    
    try:
        from sqlalchemy import create_engine, text
        
        # 连接数据库
        engine = create_engine("postgresql+psycopg://postgres:postgres@local_pg:5432/postgres")
        
        # 查询集成分析相关的表
        with engine.connect() as conn:
            # 查找所有integrated开头的表
            tables_sql = text("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public' 
                AND tablename LIKE 'integrated_%'
                ORDER BY tablename;
            """)
            
            result = conn.execute(tables_sql)
            tables = [row[0] for row in result.fetchall()]
            
            logger.info(f"找到 {len(tables)} 个集成分析相关的表:")
            
            road_lanes_tables = []
            for table in tables:
                logger.info(f"  - {table}")
                if '_road_lanes' in table:
                    road_lanes_tables.append(table)
            
            if road_lanes_tables:
                logger.info(f"找到 {len(road_lanes_tables)} 个道路分析lanes表:")
                for table in road_lanes_tables:
                    # 检查表中的数据
                    count_sql = text(f"SELECT COUNT(*) FROM {table}")
                    count = conn.execute(count_sql).scalar()
                    logger.info(f"  - {table}: {count} 条记录")
                    
                    # 获取不同的analysis_id
                    if count > 0:
                        ids_sql = text(f"""
                            SELECT DISTINCT analysis_id, COUNT(*) as lane_count
                            FROM {table}
                            GROUP BY analysis_id
                            LIMIT 3
                        """)
                        ids_result = conn.execute(ids_sql).fetchall()
                        for row in ids_result:
                            logger.info(f"    分析ID: {row[0]} ({row[1]} lanes)")
                
                return True
            else:
                logger.warning("❌ 没有找到道路分析lanes表")
                return False
        
    except Exception as e:
        logger.error(f"❌ 数据库连接测试失败: {e}")
        return False

def test_integrated_analysis_run():
    """测试运行集成分析（只检查输出，不实际执行完整流程）"""
    logger = setup_logging()
    
    logger.info("准备测试集成分析运行...")
    
    # 检查sample_trajectories.geojson是否存在
    geojson_file = "sample_trajectories.geojson"
    if not os.path.exists(geojson_file):
        logger.warning(f"测试文件不存在: {geojson_file}")
        return False
    
    try:
        from spdatalab.fusion.integrated_trajectory_analysis import IntegratedTrajectoryAnalyzer
        from spdatalab.fusion.integrated_analysis_config import create_default_config
        
        # 创建分析器
        config = create_default_config()
        analyzer = IntegratedTrajectoryAnalyzer(config)
        
        # 验证输入文件
        analyzer._validate_input_file(geojson_file)
        logger.info("✓ 输入文件验证通过")
        
        # 加载轨迹数据
        trajectories = analyzer._load_trajectories(geojson_file)
        logger.info(f"✓ 加载轨迹数据: {len(trajectories)} 条")
        
        logger.info("✓ 集成分析预备测试通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ 集成分析预备测试失败: {e}")
        return False

def main():
    """主函数"""
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("开始车道分析修复效果测试")
    logger.info("=" * 60)
    
    tests = [
        ("车道分析配置传递", test_lane_analysis_config),
        ("数据库连接和表查询", test_database_connection),
        ("集成分析预备", test_integrated_analysis_run),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        logger.info(f"\n{'=' * 20} {test_name} {'=' * 20}")
        try:
            result = test_func()
            results.append((test_name, result))
            if result:
                logger.info(f"✓ {test_name} 通过")
            else:
                logger.error(f"❌ {test_name} 失败")
        except Exception as e:
            logger.error(f"❌ {test_name} 异常: {e}")
            results.append((test_name, False))
    
    # 总结
    logger.info("\n" + "=" * 60)
    logger.info("测试结果总结")
    logger.info("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✓ 通过" if result else "❌ 失败"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\n总计: {passed}/{total} 测试通过")
    
    if passed == total:
        logger.info("🎉 所有测试通过！修复成功！")
        return 0
    else:
        logger.error("❌ 部分测试失败，需要进一步检查")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 