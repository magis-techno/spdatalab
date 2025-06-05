#!/usr/bin/env python3
"""
简化的点相交测试脚本
测试单个点与 full_intersection 表的相交查询性能
"""

import time
import logging
from sqlalchemy import create_engine, text

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 远端数据库连接
REMOTE_DSN = "postgresql+psycopg://**:**@10.170.30.193:9001/rcdatalake_gy1"

def test_single_point_intersection():
    """测试单个点与 full_intersection 的相交查询"""
    
    # 测试点坐标 (经度, 纬度)
    test_lon = 121.62530652
    test_lat = 31.26092271
    
    logger.info(f"开始测试点相交查询")
    logger.info(f"测试点坐标: ({test_lon}, {test_lat})")
    
    try:
        # 连接远端数据库
        engine = create_engine(REMOTE_DSN, future=True)
        
        with engine.connect() as conn:
            # 测试1: 简单的点相交查询（无索引优化）
            logger.info("=== 测试1: 基础点相交查询 ===")
            start_time = time.time()
            
            sql1 = text(f"""
                SELECT COUNT(*) as intersection_count
                FROM full_intersection 
                WHERE ST_Intersects(
                    ST_SetSRID(ST_MakePoint({test_lon}, {test_lat}), 4326),
                    wkb_geometry
                )
            """)
            
            result1 = conn.execute(sql1).fetchone()
            time1 = time.time() - start_time
            
            logger.info(f"基础查询结果: {result1[0]} 个相交要素")
            logger.info(f"基础查询耗时: {time1:.2f} 秒")
            
            # 测试2: 使用边界框预过滤的查询（性能优化）
            logger.info("=== 测试2: 边界框预过滤查询 ===")
            start_time = time.time()
            
            # 创建一个小的边界框（大约100米范围）
            buffer_degrees = 0.001  # 大约100米
            
            sql2 = text(f"""
                SELECT COUNT(*) as intersection_count
                FROM full_intersection 
                WHERE ST_Intersects(
                    ST_SetSRID(ST_MakePoint({test_lon}, {test_lat}), 4326),
                    wkb_geometry
                )
                AND wkb_geometry && ST_MakeEnvelope(
                    {test_lon - buffer_degrees}, 
                    {test_lat - buffer_degrees},
                    {test_lon + buffer_degrees}, 
                    {test_lat + buffer_degrees}, 
                    4326
                )
            """)
            
            result2 = conn.execute(sql2).fetchone()
            time2 = time.time() - start_time
            
            logger.info(f"边界框预过滤结果: {result2[0]} 个相交要素")
            logger.info(f"边界框预过滤耗时: {time2:.2f} 秒")
            
            # 测试3: 距离查询（用于对比）
            logger.info("=== 测试3: 距离查询 (50米范围) ===")
            start_time = time.time()
            
            sql3 = text(f"""
                SELECT COUNT(*) as nearby_count
                FROM full_intersection 
                WHERE ST_DWithin(
                    ST_SetSRID(ST_MakePoint({test_lon}, {test_lat}), 4326)::geography,
                    wkb_geometry::geography,
                    50
                )
            """)
            
            result3 = conn.execute(sql3).fetchone()
            time3 = time.time() - start_time
            
            logger.info(f"50米距离查询结果: {result3[0]} 个附近要素")
            logger.info(f"50米距离查询耗时: {time3:.2f} 秒")
            
            # 测试4: 获取实际相交的要素详情（少量记录）
            logger.info("=== 测试4: 获取相交要素详情 ===")
            start_time = time.time()
            
            sql4 = text(f"""
                SELECT intersection_id, road_type, 
                       ST_Distance(
                           ST_SetSRID(ST_MakePoint({test_lon}, {test_lat}), 4326)::geography,
                           wkb_geometry::geography
                       ) as distance_meters
                FROM full_intersection 
                WHERE ST_Intersects(
                    ST_SetSRID(ST_MakePoint({test_lon}, {test_lat}), 4326),
                    wkb_geometry
                )
                AND wkb_geometry && ST_MakeEnvelope(
                    {test_lon - buffer_degrees}, 
                    {test_lat - buffer_degrees},
                    {test_lon + buffer_degrees}, 
                    {test_lat + buffer_degrees}, 
                    4326
                )
                ORDER BY distance_meters
                LIMIT 5
            """)
            
            result4 = conn.execute(sql4).fetchall()
            time4 = time.time() - start_time
            
            logger.info(f"获取详情耗时: {time4:.2f} 秒")
            logger.info("相交要素详情:")
            for row in result4:
                logger.info(f"  ID: {row[0]}, 类型: {row[1]}, 距离: {row[2]:.2f}米")
            
            # 测试5: 检查数据库表的基本信息
            logger.info("=== 测试5: 检查表基本信息 ===")
            start_time = time.time()
            
            # 检查表记录数
            total_count = conn.execute(text("SELECT COUNT(*) FROM full_intersection")).scalar()
            
            # 检查索引信息
            index_info = conn.execute(text("""
                SELECT indexname, indexdef 
                FROM pg_indexes 
                WHERE tablename = 'full_intersection' 
                AND indexdef LIKE '%gist%'
            """)).fetchall()
            
            time5 = time.time() - start_time
            
            logger.info(f"表信息查询耗时: {time5:.2f} 秒")
            logger.info(f"full_intersection 总记录数: {total_count:,}")
            logger.info("空间索引信息:")
            if index_info:
                for idx in index_info:
                    logger.info(f"  索引: {idx[0]}")
                    logger.info(f"  定义: {idx[1]}")
            else:
                logger.warning("  未找到GIST空间索引！这可能是性能问题的原因")
            
            # 汇总
            logger.info("=== 性能测试汇总 ===")
            logger.info(f"基础相交查询: {time1:.2f}秒")
            logger.info(f"边界框预过滤: {time2:.2f}秒")
            logger.info(f"距离查询: {time3:.2f}秒")
            logger.info(f"详情获取: {time4:.2f}秒")
            
            if time1 > 1.0:
                logger.warning("基础查询超过1秒，建议检查空间索引")
            if time2 > 0.5:
                logger.warning("边界框查询超过0.5秒，性能有待提升")
                
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        raise

if __name__ == "__main__":
    test_single_point_intersection() 