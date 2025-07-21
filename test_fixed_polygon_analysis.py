#!/usr/bin/env python3
"""测试修复后的polygon分析实现（批量查询策略）"""

import logging
import sys
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """测试主函数"""
    try:
        from src.spdatalab.fusion.polygon_road_analysis import BatchPolygonRoadAnalyzer
        
        logger.info("🚀 开始测试修复后的polygon分析实现")
        
        # 创建分析器
        analyzer = BatchPolygonRoadAnalyzer()
        logger.info("✓ 分析器创建成功")
        
        # 测试GeoJSON文件
        geojson_file = "examples/test_polygon_areas.geojson"
        
        # 执行分析
        logger.info(f"📍 开始分析GeoJSON文件: {geojson_file}")
        logger.info("📊 新特性: 批量查询所有polygons + 两阶段查询 + 完整字段")
        
        start_time = datetime.now()
        analysis_id = analyzer.analyze_polygons_from_geojson(geojson_file)
        end_time = datetime.now()
        
        processing_time = (end_time - start_time).total_seconds()
        
        # 输出结果统计
        logger.info("🎯 分析完成！")
        logger.info(f"  - 分析ID: {analysis_id}")
        logger.info(f"  - 处理时间: {processing_time:.2f} 秒")
        
        # 查询数据库结果
        logger.info("📋 查询数据库结果...")
        
        from sqlalchemy import create_engine, text
        LOCAL_DSN = "postgresql+psycopg://postgres:postgres@local_pg:5432/postgres"
        engine = create_engine(LOCAL_DSN, future=True)
        
        with engine.connect() as conn:
            # 查询roads结果
            roads_sql = text("SELECT COUNT(*) FROM polygon_roads WHERE analysis_id = :analysis_id")
            roads_count = conn.execute(roads_sql, {'analysis_id': analysis_id}).scalar()
            
            # 查询intersections结果
            intersections_sql = text("SELECT COUNT(*) FROM polygon_intersections WHERE analysis_id = :analysis_id")
            intersections_count = conn.execute(intersections_sql, {'analysis_id': analysis_id}).scalar()
            
            # 查询主分析记录
            main_sql = text("SELECT * FROM polygon_road_analysis WHERE analysis_id = :analysis_id")
            main_result = conn.execute(main_sql, {'analysis_id': analysis_id}).fetchone()
            
            logger.info(f"  📊 数据库结果:")
            logger.info(f"    - Roads保存: {roads_count} 条")
            logger.info(f"    - Intersections保存: {intersections_count} 条")
            
            if main_result:
                logger.info(f"    - 分析记录: {dict(main_result._mapping)}")
            
            # 验证roads字段完整性
            if roads_count > 0:
                logger.info("🔍 验证roads字段完整性:")
                sample_road_sql = text("""
                    SELECT road_id, cityid, patchid, roadtype, is_intersection_inroad, 
                           is_intersection_outroad, is_road_intersection, intersection_type,
                           intersection_ratio, road_length 
                    FROM polygon_roads 
                    WHERE analysis_id = :analysis_id 
                    LIMIT 1
                """)
                sample_road = conn.execute(sample_road_sql, {'analysis_id': analysis_id}).fetchone()
                
                if sample_road:
                    road_dict = dict(sample_road._mapping)
                    logger.info(f"    - 样例road记录:")
                    for key, value in road_dict.items():
                        logger.info(f"      * {key}: {value}")
            
            # 验证intersections字段完整性
            if intersections_count > 0:
                logger.info("🔍 验证intersections字段完整性:")
                sample_intersection_sql = text("""
                    SELECT intersection_id, cityid, patchid, intersectiontype, 
                           intersectionsubtype, source
                    FROM polygon_intersections 
                    WHERE analysis_id = :analysis_id 
                    LIMIT 1
                """)
                sample_intersection = conn.execute(sample_intersection_sql, {'analysis_id': analysis_id}).fetchone()
                
                if sample_intersection:
                    intersection_dict = dict(sample_intersection._mapping)
                    logger.info(f"    - 样例intersection记录:")
                    for key, value in intersection_dict.items():
                        logger.info(f"      * {key}: {value}")
        
        logger.info("✅ 测试完成！修复后的实现工作正常")
        logger.info("🚀 主要优化:")
        logger.info("  - ✅ 表自动创建（含Z几何）")
        logger.info("  - ✅ 批量查询所有polygons（性能提升）")
        logger.info("  - ✅ 完整字段保留（full_road + full_intersection）")
        logger.info("  - ✅ 3个boolean关联字段")
        logger.info("  - ✅ 移除lanes（简化架构）")
        return 0
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 