#!/usr/bin/env python3
"""
轨迹道路分析模块测试脚本

专注于基于GeoJSON的完整链路测试
"""

import logging
import sys
import json
import argparse
from pathlib import Path
from typing import List, Tuple, Dict, Any

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from spdatalab.fusion.trajectory_road_analysis import (
    TrajectoryRoadAnalysisConfig,
    TrajectoryRoadAnalyzer,
    analyze_trajectory_road_elements,
    create_trajectory_road_analysis_report
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def load_trajectories_from_geojson(geojson_file: str) -> List[Tuple[str, str]]:
    """从GeoJSON文件加载轨迹数据
    
    Args:
        geojson_file: GeoJSON文件路径
        
    Returns:
        轨迹数据列表 [(trajectory_id, trajectory_wkt), ...]
    """
    try:
        with open(geojson_file, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        trajectories = []
        
        # 处理FeatureCollection
        if geojson_data.get('type') == 'FeatureCollection':
            features = geojson_data.get('features', [])
        elif geojson_data.get('type') == 'Feature':
            features = [geojson_data]
        else:
            # 直接是几何对象
            features = [{'geometry': geojson_data, 'properties': {}}]
        
        for i, feature in enumerate(features):
            geometry = feature.get('geometry', {})
            properties = feature.get('properties', {})
            
            # 获取轨迹ID
            trajectory_id = properties.get('id') or properties.get('name') or f"trajectory_{i+1:03d}"
            
            # 转换几何为WKT
            trajectory_wkt = geojson_geometry_to_wkt(geometry)
            
            if trajectory_wkt:
                trajectories.append((trajectory_id, trajectory_wkt))
                logger.info(f"加载轨迹: {trajectory_id}")
            else:
                logger.warning(f"跳过无效几何: {trajectory_id}")
        
        logger.info(f"从GeoJSON文件加载了 {len(trajectories)} 个轨迹")
        return trajectories
        
    except Exception as e:
        logger.error(f"加载GeoJSON文件失败: {e}")
        return []

def geojson_geometry_to_wkt(geometry: Dict[str, Any]) -> str:
    """将GeoJSON几何转换为WKT格式
    
    Args:
        geometry: GeoJSON几何对象
        
    Returns:
        WKT字符串
    """
    try:
        geom_type = geometry.get('type', '')
        coordinates = geometry.get('coordinates', [])
        
        if geom_type == 'LineString':
            # LineString: [[lon, lat], [lon, lat], ...]
            points = [f"{coord[0]} {coord[1]}" for coord in coordinates]
            return f"LINESTRING({', '.join(points)})"
            
        elif geom_type == 'MultiLineString':
            # MultiLineString: [[[lon, lat], ...], [[lon, lat], ...]]
            # 取第一条线
            if coordinates and len(coordinates) > 0:
                points = [f"{coord[0]} {coord[1]}" for coord in coordinates[0]]
                return f"LINESTRING({', '.join(points)})"
                
        elif geom_type == 'Point':
            # Point: [lon, lat]
            return f"POINT({coordinates[0]} {coordinates[1]})"
            
        elif geom_type == 'Polygon':
            # Polygon: [[[lon, lat], ...]]
            # 取外环作为线
            if coordinates and len(coordinates) > 0:
                points = [f"{coord[0]} {coord[1]}" for coord in coordinates[0]]
                return f"LINESTRING({', '.join(points)})"
        
        logger.warning(f"不支持的几何类型: {geom_type}")
        return ""
        
    except Exception as e:
        logger.error(f"几何转换失败: {e}")
        return ""

def create_sample_geojson(output_file: str = "sample_trajectories.geojson"):
    """创建示例GeoJSON文件（北京、上海真实道路坐标）
    
    Args:
        output_file: 输出文件路径
    """
    sample_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "id": "trajectory_beijing_001",
                    "name": "北京三环主路",
                    "description": "北京三环东路到朝阳门"
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [116.4526, 39.9042],  # 三环东路
                        [116.4556, 39.9052],
                        [116.4586, 39.9062],
                        [116.4616, 39.9072],
                        [116.4646, 39.9082],
                        [116.4676, 39.9092],
                        [116.4706, 39.9102]   # 朝阳门附近
                    ]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "id": "trajectory_beijing_002",
                    "name": "北京四环主路",
                    "description": "北京四环东路段"
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [116.4826, 39.9242],
                        [116.4856, 39.9252],
                        [116.4886, 39.9262],
                        [116.4916, 39.9272],
                        [116.4946, 39.9282],
                        [116.4976, 39.9292]
                    ]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "id": "trajectory_shanghai_001",
                    "name": "上海内环主路",
                    "description": "上海内环高架东段"
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [121.5026, 31.2242],
                        [121.5056, 31.2252],
                        [121.5086, 31.2262],
                        [121.5116, 31.2272],
                        [121.5146, 31.2282]
                    ]
                }
            }
        ]
    }
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, ensure_ascii=False, indent=2)
        logger.info(f"创建示例GeoJSON文件: {output_file}")
        return True
    except Exception as e:
        logger.error(f"创建示例GeoJSON文件失败: {e}")
        return False

def test_complete_analysis_pipeline(geojson_file: str, verbose: bool = False):
    """完整的轨迹道路分析链路测试
    
    Args:
        geojson_file: GeoJSON文件路径
        verbose: 是否详细输出
    """
    logger.info("=== 开始完整链路测试 ===")
    
    try:
        # 1. 加载轨迹数据
        logger.info("1. 加载轨迹数据...")
        trajectories = load_trajectories_from_geojson(geojson_file)
        
        if not trajectories:
            logger.error("没有加载到轨迹数据")
            return False
        
        logger.info(f"加载了 {len(trajectories)} 个轨迹")
        
        # 2. 初始化分析器
        logger.info("2. 初始化分析器...")
        config = TrajectoryRoadAnalysisConfig()
        analyzer = TrajectoryRoadAnalyzer(config)
        
        # 3. 执行分析
        logger.info("3. 执行轨迹道路分析...")
        
        analysis_ids = []
        for trajectory_id, trajectory_wkt in trajectories:
            logger.info(f"分析轨迹: {trajectory_id}")
            
            try:
                # 执行轨迹分析
                analysis_id = analyzer.analyze_trajectory_roads(
                    trajectory_id=trajectory_id,
                    trajectory_geom=trajectory_wkt
                )
                
                if analysis_id:
                    analysis_ids.append(analysis_id)
                    logger.info(f"✓ 轨迹 {trajectory_id} 分析成功，分析ID: {analysis_id}")
                    
                    if verbose:
                        # 获取分析汇总
                        summary = analyzer.get_analysis_summary(analysis_id)
                        logger.info(f"  - 总lanes: {summary.get('total_lanes', 0)}")
                        logger.info(f"  - 总intersections: {summary.get('total_intersections', 0)}")
                        logger.info(f"  - 总roads: {summary.get('total_roads', 0)}")
                else:
                    logger.warning(f"✗ 轨迹 {trajectory_id} 分析失败")
            except Exception as e:
                logger.error(f"✗ 轨迹 {trajectory_id} 分析出错: {e}")
        
        # 4. 导出QGIS可视化
        logger.info("4. 导出QGIS可视化...")
        
        for analysis_id in analysis_ids:
            try:
                export_info = analyzer.export_results_for_qgis(analysis_id)
                logger.info(f"✓ QGIS导出完成: {analysis_id}")
                if verbose:
                    logger.info(f"  导出信息: {export_info}")
            except Exception as e:
                logger.error(f"✗ QGIS导出失败: {analysis_id}, {e}")
        
        # 5. 生成分析报告
        logger.info("5. 生成分析报告...")
        for analysis_id in analysis_ids:
            try:
                report = create_trajectory_road_analysis_report(analysis_id, config)
                
                if report:
                    logger.info(f"✓ 分析报告生成成功: {analysis_id}")
                    
                    if verbose:
                        logger.info("=== 分析报告摘要 ===")
                        # 输出报告的前几行
                        report_lines = report.split('\n')
                        for line in report_lines[:10]:
                            if line.strip():
                                logger.info(f"  {line}")
                else:
                    logger.warning(f"✗ 分析报告生成失败: {analysis_id}")
            except Exception as e:
                                 logger.error(f"✗ 生成分析报告出错: {analysis_id}, {e}")
        
        logger.info("=== 完整链路测试完成 ===")
        logger.info(f"总共分析了 {len(analysis_ids)} 个轨迹")
        return len(analysis_ids) > 0
        
    except Exception as e:
        logger.error(f"完整链路测试失败: {e}")
        return False

def main():
    """主函数，CLI入口点"""
    parser = argparse.ArgumentParser(
        description='轨迹道路分析模块测试',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--geojson', type=str, help='GeoJSON文件路径')
    parser.add_argument('--create-sample', action='store_true', 
                       help='创建示例GeoJSON文件')
    parser.add_argument('--verbose', '-v', action='store_true', 
                       help='详细输出')
    parser.add_argument('--output', type=str, default='sample_trajectories.geojson',
                       help='示例文件输出路径')
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # 创建示例文件
        if args.create_sample:
            logger.info("创建示例GeoJSON文件...")
            if create_sample_geojson(args.output):
                logger.info(f"示例文件已创建: {args.output}")
                logger.info("使用方法: python test_trajectory_road_analysis.py --geojson sample_trajectories.geojson")
            else:
                logger.error("创建示例文件失败")
                return 1
        
        # 执行GeoJSON文件分析
        if args.geojson:
            geojson_file = args.geojson
            
            if not Path(geojson_file).exists():
                logger.error(f"GeoJSON文件不存在: {geojson_file}")
                return 1
            
            logger.info(f"开始测试GeoJSON文件: {geojson_file}")
            success = test_complete_analysis_pipeline(geojson_file, args.verbose)
            
            if success:
                logger.info("✓ 测试成功完成")
                return 0
            else:
                logger.error("✗ 测试失败")
                return 1
        
        # 如果没有指定参数，显示帮助
        if not args.create_sample and not args.geojson:
            parser.print_help()
            logger.info("\n建议使用:")
            logger.info("  1. 创建示例: python test_trajectory_road_analysis.py --create-sample")
            logger.info("  2. 测试分析: python test_trajectory_road_analysis.py --geojson sample_trajectories.geojson")
            return 0
        
        return 0
        
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())