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
        engine = create_engine(
            REMOTE_DSN, 
            future=True,
            connect_args={"client_encoding": "utf8"}
        )
        
        with engine.connect() as conn:
            # 先测试基本连接和表信息
            logger.info("=== 测试0: 基本连接和表信息 ===")
            start_time = time.time()
            
            # 检查表存在和记录数
            total_count = conn.execute(text("SELECT COUNT(*) FROM full_intersection")).scalar()
            
            # 检查几何列的数据类型
            geom_info = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'full_intersection' 
                AND column_name LIKE '%geom%'
            """)).fetchall()
            
            time0 = time.time() - start_time
            logger.info(f"连接测试耗时: {time0:.2f} 秒")
            logger.info(f"full_intersection 总记录数: {total_count:,}")
            logger.info("几何列信息:")
            for col in geom_info:
                logger.info(f"  列名: {col[0]}, 类型: {col[1]}")
            
            # 测试1: 简单的边界框查询（避免复杂几何操作）
            logger.info("=== 测试1: 边界框范围查询 ===")
            start_time = time.time()
            
            # 使用边界框查询，避免ST_Intersects
            buffer_degrees = 0.001  # 大约100米
            
            sql1 = text(f"""
                SELECT COUNT(*) as count_in_bbox
                FROM full_intersection 
                WHERE wkb_geometry && ST_MakeEnvelope(
                    {test_lon - buffer_degrees}, 
                    {test_lat - buffer_degrees},
                    {test_lon + buffer_degrees}, 
                    {test_lat + buffer_degrees}, 
                    4326
                )
            """)
            
            result1 = conn.execute(sql1).fetchone()
            time1 = time.time() - start_time
            
            logger.info(f"边界框查询结果: {result1[0]} 个要素")
            logger.info(f"边界框查询耗时: {time1:.2f} 秒")
            
            # 测试2: 尝试简单的几何查询（如果边界框查询成功）
            if result1[0] > 0:
                logger.info("=== 测试2: 简单几何查询 ===")
                start_time = time.time()
                
                try:
                    # 只获取基本信息，避免复杂的几何计算
                    sql2 = text(f"""
                        SELECT intersection_id, road_type
                        FROM full_intersection 
                        WHERE wkb_geometry && ST_MakeEnvelope(
                            {test_lon - buffer_degrees}, 
                            {test_lat - buffer_degrees},
                            {test_lon + buffer_degrees}, 
                            {test_lat + buffer_degrees}, 
                            4326
                        )
                        LIMIT 5
                    """)
                    
                    result2 = conn.execute(sql2).fetchall()
                    time2 = time.time() - start_time
                    
                    logger.info(f"几何查询耗时: {time2:.2f} 秒")
                    logger.info("附近要素:")
                    for row in result2:
                        logger.info(f"  ID: {row[0]}, 类型: {row[1]}")
                        
                except Exception as e:
                    logger.error(f"几何查询失败: {str(e)}")
                    time2 = -1
            else:
                logger.info("=== 测试2: 跳过（无数据） ===")
                time2 = 0
            
            # 测试3: 检查空间索引
            logger.info("=== 测试3: 检查空间索引 ===")
            start_time = time.time()
            
            try:
                # 检查索引信息
                index_info = conn.execute(text("""
                    SELECT indexname, indexdef 
                    FROM pg_indexes 
                    WHERE tablename = 'full_intersection'
                """)).fetchall()
                
                time3 = time.time() - start_time
                
                logger.info(f"索引查询耗时: {time3:.2f} 秒")
                logger.info("表索引信息:")
                spatial_index_found = False
                for idx in index_info:
                    logger.info(f"  索引: {idx[0]}")
                    if 'gist' in idx[1].lower() or 'spatial' in idx[1].lower():
                        spatial_index_found = True
                        logger.info(f"    -> 空间索引: {idx[1]}")
                
                if not spatial_index_found:
                    logger.warning("  未找到空间索引！这可能是性能问题的原因")
                else:
                    logger.info("  找到空间索引")
                    
            except Exception as e:
                logger.error(f"索引查询失败: {str(e)}")
                time3 = -1
            
            # 汇总
            logger.info("=== 性能测试汇总 ===")
            logger.info(f"边界框查询: {time1:.2f}秒 (返回{result1[0]}条记录)")
            if time2 >= 0:
                logger.info(f"几何查询: {time2:.2f}秒")
            if time3 >= 0:
                logger.info(f"索引查询: {time3:.2f}秒")
            
            # 性能建议
            if time1 > 1.0:
                logger.warning("边界框查询超过1秒，数据库性能可能有问题")
            elif time1 > 0.1:
                logger.info("边界框查询超过0.1秒，建议检查空间索引")
            else:
                logger.info("查询性能良好")
                
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        raise

if __name__ == "__main__":
    test_single_point_intersection() 