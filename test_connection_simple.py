#!/usr/bin/env python3
"""
简单的数据库连接和查询测试脚本
用于诊断轨迹道路分析中的连接问题
"""

import logging
import sys
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from sqlalchemy import create_engine, text
from spdatalab.fusion.trajectory_road_analysis import TrajectoryRoadAnalysisConfig

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def test_database_connection():
    """测试数据库连接"""
    config = TrajectoryRoadAnalysisConfig()
    
    logger.info("=== 测试数据库连接 ===")
    
    # 测试远程数据库连接
    try:
        logger.info("创建远程数据库引擎...")
        remote_engine = create_engine(
            config.remote_dsn,
            future=True,
            connect_args={
                "client_encoding": "utf8", 
                "connect_timeout": 60,
                "command_timeout": 120,
            },
            pool_pre_ping=True,
            pool_size=1,
            max_overflow=0
        )
        
        logger.info("测试远程数据库连接...")
        with remote_engine.connect() as conn:
            # 简单连接测试
            result = conn.execute(text("SELECT 1 as test")).fetchone()
            logger.info(f"✓ 远程数据库连接成功: {result[0]}")
            
            # 测试超时设置
            conn.execute(text("SET statement_timeout = '120s'"))
            logger.info("✓ 超时设置成功")
            
            # 测试road表基本信息
            logger.info(f"测试{config.road_table}表...")
            road_count_sql = text(f"""
                SELECT COUNT(*) as road_count 
                FROM {config.road_table} 
                LIMIT 1
            """)
            
            road_result = conn.execute(road_count_sql).fetchone()
            logger.info(f"✓ {config.road_table}表连接成功，总记录数: {road_result[0]}")
            
            # 测试intersection表
            logger.info(f"测试{config.intersection_table}表...")
            intersection_count_sql = text(f"""
                SELECT COUNT(*) as intersection_count 
                FROM {config.intersection_table} 
                LIMIT 1
            """)
            
            intersection_result = conn.execute(intersection_count_sql).fetchone()
            logger.info(f"✓ {config.intersection_table}表连接成功，总记录数: {intersection_result[0]}")
            
        return True
        
    except Exception as e:
        logger.error(f"✗ 远程数据库连接失败: {e}")
        return False

def test_simple_spatial_query():
    """测试简单的空间查询"""
    config = TrajectoryRoadAnalysisConfig()
    
    logger.info("=== 测试简单空间查询 ===")
    
    try:
        remote_engine = create_engine(
            config.remote_dsn,
            future=True,
            connect_args={
                "client_encoding": "utf8", 
                "connect_timeout": 60,
                "command_timeout": 120,
            },
            pool_pre_ping=True,
            pool_size=1,
            max_overflow=0
        )
        
        # 使用一个简单的测试轨迹（北京市区）
        test_trajectory = "LINESTRING(116.4 39.9, 116.41 39.91, 116.42 39.92)"
        
        with remote_engine.connect() as conn:
            conn.execute(text("SET statement_timeout = '120s'"))
            
            # 测试轨迹缓冲区创建
            logger.info("测试轨迹缓冲区创建...")
            buffer_sql = text(f"""
                SELECT ST_AsText(
                    ST_Buffer(
                        ST_SetSRID(ST_GeomFromText('{test_trajectory}'), 4326)::geography,
                        3.0
                    )::geometry
                ) as buffer_geom
            """)
            
            buffer_result = conn.execute(buffer_sql).fetchone()
            buffer_geom = buffer_result[0]
            logger.info(f"✓ 缓冲区创建成功，长度: {len(buffer_geom)} 字符")
            
            # 测试简单的road查询（限制1条记录）
            logger.info("测试简单road查询...")
            simple_road_sql = text(f"""
                SELECT 
                    id as road_id,
                    ST_AsText(wkb_geometry) as geometry_wkt
                FROM {config.road_table}
                WHERE ST_Intersects(
                    ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326),
                    wkb_geometry
                )
                AND wkb_geometry IS NOT NULL
                LIMIT 1
            """)
            
            road_result = conn.execute(simple_road_sql, {'buffer_geom': buffer_geom}).fetchall()
            logger.info(f"✓ Road查询成功，找到 {len(road_result)} 条记录")
            
            # 测试简单的intersection查询
            logger.info("测试简单intersection查询...")
            simple_intersection_sql = text(f"""
                SELECT 
                    id as intersection_id,
                    intersectiontype
                FROM {config.intersection_table}
                WHERE ST_Intersects(
                    ST_SetSRID(ST_GeomFromText(:buffer_geom), 4326),
                    wkb_geometry
                )
                AND wkb_geometry IS NOT NULL
                LIMIT 1
            """)
            
            intersection_result = conn.execute(simple_intersection_sql, {'buffer_geom': buffer_geom}).fetchall()
            logger.info(f"✓ Intersection查询成功，找到 {len(intersection_result)} 条记录")
            
        return True
        
    except Exception as e:
        logger.error(f"✗ 空间查询测试失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return False

def test_pandas_compatibility():
    """测试pandas兼容性"""
    logger.info("=== 测试pandas兼容性 ===")
    
    try:
        import pandas as pd
        logger.info("✓ pandas导入成功")
        
        config = TrajectoryRoadAnalysisConfig()
        
        remote_engine = create_engine(
            config.remote_dsn,
            future=True,
            connect_args={
                "client_encoding": "utf8", 
                "connect_timeout": 60,
                "command_timeout": 120,
            },
            pool_pre_ping=True,
            pool_size=1,
            max_overflow=0
        )
        
        # 测试pandas.read_sql
        logger.info("测试pandas.read_sql...")
        with remote_engine.connect() as conn:
            conn.execute(text("SET statement_timeout = '120s'"))
            
            simple_sql = text(f"SELECT id, ST_AsText(wkb_geometry) as geom FROM {config.road_table} LIMIT 2")
            df = pd.read_sql(simple_sql, conn)
            
            logger.info(f"✓ pandas.read_sql成功，返回 {len(df)} 行")
            
        return True
        
    except Exception as e:
        logger.error(f"✗ pandas兼容性测试失败: {e}")
        return False

def main():
    """主测试函数"""
    logger.info("开始数据库连接诊断...")
    
    tests = [
        ("数据库连接测试", test_database_connection),
        ("简单空间查询测试", test_simple_spatial_query),
        ("pandas兼容性测试", test_pandas_compatibility)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        logger.info(f"\n--- {test_name} ---")
        try:
            if test_func():
                passed += 1
                logger.info(f"✓ {test_name} 通过")
            else:
                logger.error(f"✗ {test_name} 失败")
        except Exception as e:
            logger.error(f"✗ {test_name} 异常: {e}")
    
    logger.info(f"\n=== 测试结果汇总 ===")
    logger.info(f"通过: {passed}/{total}")
    
    if passed == total:
        logger.info("🎉 所有测试通过！数据库连接正常")
        logger.info("建议：现在可以运行完整的轨迹道路分析")
    else:
        logger.warning("⚠️ 部分测试失败，请检查数据库配置")
        logger.info("建议：")
        logger.info("1. 检查数据库连接参数")
        logger.info("2. 确认网络连接稳定")
        logger.info("3. 检查表名和权限")
    
    return passed == total

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1) 