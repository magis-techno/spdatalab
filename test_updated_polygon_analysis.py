#!/usr/bin/env python3
"""测试更新后的polygon分析实现（两阶段查询策略）"""

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
        
        logger.info("🚀 开始测试更新后的polygon分析实现")
        
        # 创建分析器
        analyzer = BatchPolygonRoadAnalyzer()
        logger.info("✓ 分析器创建成功")
        
        # 测试GeoJSON文件（使用之前的测试文件）
        geojson_file = "examples/test_polygon_areas.geojson"
        
        # 执行分析
        logger.info(f"📍 开始分析GeoJSON文件: {geojson_file}")
        
        start_time = datetime.now()
        results = analyzer.analyze_polygons_from_geojson(geojson_file)
        end_time = datetime.now()
        
        processing_time = (end_time - start_time).total_seconds()
        
        # 输出结果统计
        logger.info("🎯 分析完成！结果统计:")
        logger.info(f"  - 处理时间: {processing_time:.2f} 秒")
        logger.info(f"  - 分析ID: {results.get('analysis_id', 'N/A')}")
        
        query_results = results.get('query_results', {})
        logger.info(f"  - Roads找到: {len(query_results.get('roads', []))} 条")
        logger.info(f"  - Intersections找到: {len(query_results.get('intersections', []))} 条")
        
        # 输出部分roads详情（验证新字段）
        roads_df = query_results.get('roads')
        if not roads_df.empty:
            logger.info("🔍 Roads字段验证:")
            sample_road = roads_df.iloc[0]
            logger.info(f"  - 原始字段检查:")
            logger.info(f"    * cityid: {sample_road.get('cityid', 'N/A')}")
            logger.info(f"    * patchid: {sample_road.get('patchid', 'N/A')}")
            logger.info(f"    * releaseversion: {sample_road.get('releaseversion', 'N/A')}")
            logger.info(f"    * roadtype: {sample_road.get('roadtype', 'N/A')}")
            logger.info(f"  - Boolean字段检查:")
            logger.info(f"    * is_intersection_inroad: {sample_road.get('is_intersection_inroad', 'N/A')}")
            logger.info(f"    * is_intersection_outroad: {sample_road.get('is_intersection_outroad', 'N/A')}")
            logger.info(f"    * is_road_intersection: {sample_road.get('is_road_intersection', 'N/A')}")
        
        # 输出部分intersections详情
        intersections_df = query_results.get('intersections')
        if not intersections_df.empty:
            logger.info("🔍 Intersections字段验证:")
            sample_intersection = intersections_df.iloc[0]
            logger.info(f"  - 原始字段检查:")
            logger.info(f"    * cityid: {sample_intersection.get('cityid', 'N/A')}")
            logger.info(f"    * patchid: {sample_intersection.get('patchid', 'N/A')}")
            logger.info(f"    * intersectiontype: {sample_intersection.get('intersectiontype', 'N/A')}")
            logger.info(f"    * intersectionsubtype: {sample_intersection.get('intersectionsubtype', 'N/A')}")
        
        logger.info("✅ 测试完成！新的两阶段查询实现工作正常")
        return 0
        
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 