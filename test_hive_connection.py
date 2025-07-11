#!/usr/bin/env python3
"""
测试Hive连接和数据表访问
"""

import logging
import pandas as pd
from spdatalab.common.io_hive import hive_cursor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_hive_connection():
    """测试Hive连接"""
    try:
        logger.info("开始测试Hive连接...")
        
        # 测试连接rcdatalake_gy1 catalog
        catalog = "rcdatalake_gy1"
        logger.info(f"连接catalog: {catalog}")
        
        with hive_cursor(catalog) as cur:
            logger.info("✓ 成功建立Hive连接")
            
            # 测试查询表列表
            logger.info("查询表列表...")
            cur.execute("SHOW TABLES")
            tables = cur.fetchall()
            logger.info(f"✓ 找到 {len(tables)} 个表")
            
            # 输出前10个表名
            logger.info("前10个表名:")
            for i, table in enumerate(tables[:10]):
                logger.info(f"  {i+1}. {table[0]}")
            
            # 检查我们需要的表是否存在
            required_tables = [
                'full_road', 'full_lane', 'full_intersection', 
                'roadnextroad', 'full_intersectiongoinroad', 'full_intersectiongooutroad'
            ]
            
            table_names = [table[0] for table in tables]
            
            logger.info("\n检查必需的表:")
            for table in required_tables:
                if table in table_names:
                    logger.info(f"✓ 表 {table} 存在")
                else:
                    logger.warning(f"✗ 表 {table} 不存在")
            
            # 测试查询full_road表的基本信息
            logger.info("\n测试查询full_road表...")
            try:
                cur.execute("SELECT COUNT(*) FROM full_road LIMIT 1")
                result = cur.fetchone()
                if result:
                    logger.info(f"✓ full_road表有 {result[0]} 行数据")
                else:
                    logger.warning("✗ full_road表查询无结果")
            except Exception as e:
                logger.error(f"✗ 查询full_road表失败: {e}")
            
            # 测试查询full_road表的列信息
            logger.info("\n测试查询full_road表的列信息...")
            try:
                cur.execute("DESCRIBE full_road")
                columns = cur.fetchall()
                logger.info(f"✓ full_road表有 {len(columns)} 列")
                
                # 输出列信息
                logger.info("列信息:")
                for col in columns:
                    logger.info(f"  {col[0]}: {col[1]}")
                
                # 检查是否有几何字段
                geom_columns = [col[0] for col in columns if 'geom' in col[0].lower()]
                if geom_columns:
                    logger.info(f"✓ 找到几何字段: {geom_columns}")
                else:
                    logger.warning("✗ 未找到几何字段")
                    
            except Exception as e:
                logger.error(f"✗ 查询full_road表结构失败: {e}")
                
    except Exception as e:
        logger.error(f"✗ Hive连接测试失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return False
    
    logger.info("✓ Hive连接测试完成")
    return True

def test_spatial_query():
    """测试空间查询"""
    try:
        logger.info("\n开始测试空间查询...")
        
        # 使用一个简单的点作为测试
        test_point = "POINT(116.3974 39.9093)"  # 北京天安门附近
        
        catalog = "rcdatalake_gy1"
        
        with hive_cursor(catalog) as cur:
            logger.info("测试ST_GeomFromText函数...")
            
            # 简单的空间函数测试
            simple_spatial_sql = f"""
                SELECT ST_AsText(ST_GeomFromText('{test_point}')) as test_geom
            """
            
            cur.execute(simple_spatial_sql)
            result = cur.fetchone()
            if result:
                logger.info(f"✓ 空间函数测试成功: {result[0]}")
            else:
                logger.warning("✗ 空间函数测试失败")
                
            # 测试缓冲区功能
            logger.info("测试缓冲区功能...")
            buffer_sql = f"""
                SELECT ST_AsText(
                    ST_Buffer(
                        ST_SetSRID(ST_GeomFromText('{test_point}'), 4326)::geography,
                        3
                    )::geometry
                ) as buffer_geom
            """
            
            cur.execute(buffer_sql)
            result = cur.fetchone()
            if result:
                logger.info(f"✓ 缓冲区功能测试成功，长度: {len(result[0])}")
            else:
                logger.warning("✗ 缓冲区功能测试失败")
                
    except Exception as e:
        logger.error(f"✗ 空间查询测试失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return False
    
    logger.info("✓ 空间查询测试完成")
    return True

if __name__ == "__main__":
    print("=" * 50)
    print("Hive连接和数据表访问测试")
    print("=" * 50)
    
    # 测试基本连接
    connection_ok = test_hive_connection()
    
    if connection_ok:
        # 测试空间查询
        spatial_ok = test_spatial_query()
        
        if spatial_ok:
            print("\n🎉 所有测试通过！")
            print("轨迹道路分析模块已准备就绪")
        else:
            print("\n⚠️  基本连接正常，但空间查询有问题")
    else:
        print("\n❌ 连接测试失败")
        print("请检查网络连接和数据库配置") 