#!/usr/bin/env python3
"""
测试polygon道路分析功能

运行命令:
python test_polygon_road_analysis.py
"""

import sys
import logging
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / "src"))

from spdatalab.fusion.polygon_road_analysis import (
    BatchPolygonRoadAnalyzer,
    PolygonRoadAnalysisConfig,
    analyze_polygons_from_geojson
)

def test_polygon_analysis():
    """测试polygon分析功能"""
    
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    
    # 测试文件路径
    geojson_file = "examples/test_polygon_areas.geojson"
    
    if not Path(geojson_file).exists():
        logger.error(f"测试文件不存在: {geojson_file}")
        return False
    
    try:
        logger.info("=== 开始polygon道路分析测试 ===")
        
        # 创建配置
        config = PolygonRoadAnalysisConfig(
            polygon_batch_size=10,
            enable_parallel_queries=True
        )
        
        logger.info(f"配置信息:")
        logger.info(f"  - 批处理大小: {config.polygon_batch_size}")
        logger.info(f"  - 并行查询: {config.enable_parallel_queries}")
        logger.info(f"  - 远程catalog: {config.remote_catalog}")
        
        # 执行分析
        analysis_id, summary = analyze_polygons_from_geojson(
            geojson_file=geojson_file,
            config=config,
            batch_analysis_id="test_batch_20241201"
        )
        
        logger.info("=== 分析完成 ===")
        logger.info(f"分析ID: {analysis_id}")
        logger.info(f"批量分析ID: {summary['batch_analysis_id']}")
        logger.info(f"处理的polygon数: {summary['total_polygons']}")
        logger.info(f"找到的road数: {summary['total_roads']}")
        logger.info(f"找到的intersection数: {summary['total_intersections']}")
        logger.info(f"找到的lane数: {summary['total_lanes']}")
        logger.info(f"处理时间: {summary['processing_time_seconds']:.2f} 秒")
        
        # 详细统计
        logger.info("=== 各polygon详细统计 ===")
        for polygon_id, road_count in summary['polygon_road_stats'].items():
            logger.info(f"Polygon {polygon_id}: {road_count} roads")
        
        # 验证结果
        if summary['total_polygons'] == 3:
            logger.info("✓ polygon数量验证通过")
        else:
            logger.warning(f"✗ polygon数量异常: 期望3，实际{summary['total_polygons']}")
        
        if summary['total_roads'] > 0:
            logger.info("✓ 找到了roads数据")
        else:
            logger.warning("✗ 未找到roads数据")
        
        logger.info("=== 测试完成 ===")
        return True
        
    except Exception as e:
        logger.error(f"测试失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return False

def test_geojson_loading():
    """测试GeoJSON加载功能"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("=== 测试GeoJSON加载 ===")
        
        config = PolygonRoadAnalysisConfig()
        analyzer = BatchPolygonRoadAnalyzer(config)
        
        geojson_file = "examples/test_polygon_areas.geojson"
        polygons = analyzer._load_and_validate_geojson(geojson_file)
        
        logger.info(f"加载的polygon数: {len(polygons)}")
        
        for polygon in polygons:
            logger.info(f"Polygon: {polygon['polygon_id']}")
            logger.info(f"  - 面积: {polygon['area_m2']:.0f} 平方米")
            logger.info(f"  - 属性: {polygon['properties']}")
        
        return True
        
    except Exception as e:
        logger.error(f"GeoJSON加载测试失败: {e}")
        return False

def main():
    """主测试函数"""
    
    print("开始polygon道路分析测试...")
    
    # 测试1: GeoJSON加载
    if not test_geojson_loading():
        print("❌ GeoJSON加载测试失败")
        return 1
    
    print("✅ GeoJSON加载测试通过")
    
    # 测试2: 完整分析流程
    if not test_polygon_analysis():
        print("❌ polygon分析测试失败")
        return 1
    
    print("✅ polygon分析测试通过")
    print("🎉 所有测试完成!")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 