#!/usr/bin/env python3
"""测试polygon分析表创建过程"""

import logging
import sys
from sqlalchemy import create_engine, text

# 配置详细日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def check_table_exists(engine, table_name):
    """检查表是否存在"""
    with engine.connect() as conn:
        check_sql = text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = '{table_name}'
            );
        """)
        exists = conn.execute(check_sql).scalar()
        return exists

def main():
    """测试主函数"""
    try:
        LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
        engine = create_engine(LOCAL_DSN, future=True)
        
        logger.info("🚀 开始测试表创建过程")
        
        # 首先检查数据库连接
        logger.info("📡 测试数据库连接...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()")).scalar()
            logger.info(f"✓ 数据库连接成功: {result}")
        
        # 导入分析器类
        logger.info("📦 导入分析器类...")
        from src.spdatalab.fusion.polygon_road_analysis import BatchPolygonRoadAnalyzer
        
        # 创建分析器（不会自动创建表）
        logger.info("🔧 创建分析器实例...")
        analyzer = BatchPolygonRoadAnalyzer()
        logger.info("✓ 分析器创建成功")
        
        # 检查表是否已存在
        tables_to_check = [
            analyzer.config.polygon_analysis_table,
            analyzer.config.polygon_roads_table,
            analyzer.config.polygon_intersections_table
        ]
        
        logger.info("🔍 检查表是否已存在...")
        for table_name in tables_to_check:
            exists = check_table_exists(engine, table_name)
            logger.info(f"  - {table_name}: {'存在' if exists else '不存在'}")
        
        # 手动调用表初始化
        logger.info("🏗️ 手动调用表初始化...")
        analyzer._init_analysis_tables()
        
        # 再次检查表是否创建成功
        logger.info("✅ 验证表创建结果...")
        all_created = True
        for table_name in tables_to_check:
            exists = check_table_exists(engine, table_name)
            status = "✓ 创建成功" if exists else "✗ 创建失败"
            logger.info(f"  - {table_name}: {status}")
            if not exists:
                all_created = False
        
        # 检查表结构
        if all_created:
            logger.info("🔧 检查表结构...")
            with engine.connect() as conn:
                for table_name in tables_to_check:
                    logger.info(f"  检查表: {table_name}")
                    
                    # 检查列信息
                    columns_sql = text(f"""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                    """)
                    
                    columns = conn.execute(columns_sql).fetchall()
                    logger.info(f"    列数: {len(columns)}")
                    for col in columns[:5]:  # 只显示前5列
                        logger.info(f"      - {col[0]} ({col[1]}) {'NULL' if col[2] == 'YES' else 'NOT NULL'}")
                    
                    # 检查几何列
                    geom_sql = text(f"""
                        SELECT f_geometry_column, coord_dimension, srid, type
                        FROM geometry_columns 
                        WHERE f_table_schema = 'public' 
                        AND f_table_name = '{table_name}'
                    """)
                    
                    geom_info = conn.execute(geom_sql).fetchall()
                    if geom_info:
                        for geom in geom_info:
                            logger.info(f"    几何列: {geom[0]} ({geom[3]}, {geom[1]}D, SRID:{geom[2]})")
                    else:
                        logger.info(f"    几何列: 无")
        
        if all_created:
            logger.info("🎉 所有表创建成功！")
            return 0
        else:
            logger.error("❌ 部分表创建失败")
            return 1
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 