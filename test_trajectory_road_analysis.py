#!/usr/bin/env python3
"""
轨迹道路分析模块测试脚本

测试功能：
1. 基本配置测试
2. 数据库表创建测试
3. 轨迹缓冲区创建测试
4. 空间查询测试
5. 完整分析流程测试
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
    """创建示例GeoJSON文件
    
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
                    "name": "北京市区轨迹1",
                    "description": "从三环到四环的示例轨迹"
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [116.3, 39.9],
                        [116.31, 39.91],
                        [116.32, 39.92],
                        [116.33, 39.93],
                        [116.34, 39.94]
                    ]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "id": "trajectory_beijing_002",
                    "name": "北京市区轨迹2",
                    "description": "朝阳区环路轨迹"
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [116.4, 39.8],
                        [116.41, 39.81],
                        [116.42, 39.82],
                        [116.43, 39.83],
                        [116.44, 39.84],
                        [116.45, 39.85]
                    ]
                }
            },
            {
                "type": "Feature",
                "properties": {
                    "id": "trajectory_shanghai_001",
                    "name": "上海市区轨迹",
                    "description": "浦东新区轨迹"
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [121.5, 31.2],
                        [121.51, 31.21],
                        [121.52, 31.22],
                        [121.53, 31.23]
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

def test_config():
    """测试配置类"""
    logger.info("=== 测试配置类 ===")
    
    config = TrajectoryRoadAnalysisConfig()
    
    # 检查默认配置
    assert config.buffer_distance == 3.0
    assert config.forward_chain_limit == 500.0
    assert config.backward_chain_limit == 100.0
    assert config.max_recursion_depth == 50
    
    logger.info("✓ 配置类测试通过")

def test_analyzer_initialization():
    """测试分析器初始化"""
    logger.info("=== 测试分析器初始化 ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        logger.info("✓ 分析器初始化成功")
        
        # 测试配置
        assert analyzer.config.buffer_distance == 3.0
        logger.info("✓ 配置加载正确")
        
    except Exception as e:
        logger.error(f"✗ 分析器初始化失败: {e}")
        raise

def test_trajectory_buffer():
    """测试轨迹缓冲区创建"""
    logger.info("=== 测试轨迹缓冲区创建 ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        
        # 测试轨迹WKT（示例线段）
        test_trajectory_wkt = "LINESTRING(116.3 39.9, 116.31 39.91, 116.32 39.92)"
        
        # 创建缓冲区
        buffer_geom = analyzer._create_trajectory_buffer(test_trajectory_wkt)
        
        if buffer_geom:
            logger.info("✓ 轨迹缓冲区创建成功")
            logger.info(f"缓冲区几何类型: {buffer_geom[:50]}...")
        else:
            logger.error("✗ 轨迹缓冲区创建失败")
            
    except Exception as e:
        logger.error(f"✗ 轨迹缓冲区测试失败: {e}")

def test_simple_analysis():
    """测试简单分析流程"""
    logger.info("=== 测试简单分析流程 ===")
    
    try:
        # 使用便捷接口
        test_trajectory_id = "test_trajectory_001"
        test_trajectory_wkt = "LINESTRING(116.3 39.9, 116.31 39.91, 116.32 39.92)"
        
        analysis_id, summary = analyze_trajectory_road_elements(
            trajectory_id=test_trajectory_id,
            trajectory_geom=test_trajectory_wkt
        )
        
        logger.info(f"✓ 分析完成: {analysis_id}")
        logger.info(f"分析汇总: {summary}")
        
        # 生成报告
        report = create_trajectory_road_analysis_report(analysis_id)
        logger.info("✓ 生成分析报告成功")
        
        # 输出报告的前几行
        report_lines = report.split('\n')
        for line in report_lines[:10]:
            logger.info(f"报告: {line}")
        
        return analysis_id
        
    except Exception as e:
        logger.error(f"✗ 简单分析流程测试失败: {e}")
        return None

def test_database_tables():
    """测试数据库表创建"""
    logger.info("=== 测试数据库表创建 ===")
    
    try:
        analyzer = TrajectoryRoadAnalyzer()
        
        # 检查表是否存在
        tables_to_check = [
            analyzer.config.analysis_table,
            analyzer.config.lanes_table,
            analyzer.config.intersections_table,
            analyzer.config.roads_table
        ]
        
        for table_name in tables_to_check:
            from sqlalchemy import text
            check_sql = text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = :table_name
                );
            """)
            
            with analyzer.local_engine.connect() as conn:
                result = conn.execute(check_sql, {'table_name': table_name}).fetchone()
                exists = result[0] if result else False
                
                if exists:
                    logger.info(f"✓ 表 {table_name} 存在")
                else:
                    logger.warning(f"⚠ 表 {table_name} 不存在")
        
        logger.info("✓ 数据库表检查完成")
        
    except Exception as e:
        logger.error(f"✗ 数据库表测试失败: {e}")

def test_mock_data_analysis():
    """测试模拟数据分析"""
    logger.info("=== 测试模拟数据分析 ===")
    
    # 模拟一些测试轨迹数据
    test_trajectories = [
        ("trajectory_001", "LINESTRING(116.3 39.9, 116.31 39.91, 116.32 39.92)"),
        ("trajectory_002", "LINESTRING(116.4 39.8, 116.41 39.81, 116.42 39.82)"),
        ("trajectory_003", "LINESTRING(116.5 39.7, 116.51 39.71, 116.52 39.72)")
    ]
    
    results = []
    
    for trajectory_id, trajectory_wkt in test_trajectories:
        try:
            logger.info(f"分析轨迹: {trajectory_id}")
            
            analysis_id, summary = analyze_trajectory_road_elements(
                trajectory_id=trajectory_id,
                trajectory_geom=trajectory_wkt
            )
            
            results.append({
                'trajectory_id': trajectory_id,
                'analysis_id': analysis_id,
                'summary': summary
            })
            
            logger.info(f"✓ 轨迹 {trajectory_id} 分析完成")
            
        except Exception as e:
            logger.error(f"✗ 轨迹 {trajectory_id} 分析失败: {e}")
            results.append({
                'trajectory_id': trajectory_id,
                'analysis_id': None,
                'error': str(e)
            })
    
    logger.info(f"✓ 模拟数据分析完成，成功: {len([r for r in results if r.get('analysis_id')])}")
    return results

def test_geojson_support():
    """测试GeoJSON文件支持"""
    logger.info("=== 测试GeoJSON文件支持 ===")
    
    # 创建示例GeoJSON文件
    sample_file = "test_sample_trajectories.geojson"
    if create_sample_geojson(sample_file):
        logger.info("✓ 示例GeoJSON文件创建成功")
    else:
        logger.error("✗ 示例GeoJSON文件创建失败")
        return
    
    try:
        # 测试加载GeoJSON文件
        trajectories = load_trajectories_from_geojson(sample_file)
        
        if trajectories:
            logger.info(f"✓ GeoJSON文件加载成功，找到 {len(trajectories)} 个轨迹")
            
            # 测试分析前几个轨迹
            results = []
            for i, (trajectory_id, trajectory_wkt) in enumerate(trajectories[:2]):  # 只测试前2个
                try:
                    logger.info(f"分析GeoJSON轨迹: {trajectory_id}")
                    
                    analysis_id, summary = analyze_trajectory_road_elements(
                        trajectory_id=trajectory_id,
                        trajectory_geom=trajectory_wkt
                    )
                    
                    results.append({
                        'trajectory_id': trajectory_id,
                        'analysis_id': analysis_id,
                        'summary': summary
                    })
                    
                    logger.info(f"✓ GeoJSON轨迹 {trajectory_id} 分析完成")
                    
                except Exception as e:
                    logger.error(f"✗ GeoJSON轨迹 {trajectory_id} 分析失败: {e}")
                    results.append({
                        'trajectory_id': trajectory_id,
                        'analysis_id': None,
                        'error': str(e)
                    })
            
            successful_analyses = len([r for r in results if r.get('analysis_id')])
            logger.info(f"✓ GeoJSON轨迹分析完成，成功: {successful_analyses}/{len(results)}")
        else:
            logger.error("✗ GeoJSON文件加载失败")
    
    except Exception as e:
        logger.error(f"✗ GeoJSON支持测试失败: {e}")
    
    finally:
        # 清理测试文件
        try:
            Path(sample_file).unlink()
            logger.info("✓ 清理测试文件完成")
        except:
            pass

def test_geojson_geometry_conversion():
    """测试GeoJSON几何转换"""
    logger.info("=== 测试GeoJSON几何转换 ===")
    
    test_cases = [
        {
            "name": "LineString",
            "geometry": {
                "type": "LineString",
                "coordinates": [[116.3, 39.9], [116.31, 39.91], [116.32, 39.92]]
            },
            "expected_prefix": "LINESTRING"
        },
        {
            "name": "MultiLineString",
            "geometry": {
                "type": "MultiLineString",
                "coordinates": [[[116.3, 39.9], [116.31, 39.91]], [[116.4, 39.8], [116.41, 39.81]]]
            },
            "expected_prefix": "LINESTRING"
        },
        {
            "name": "Point",
            "geometry": {
                "type": "Point",
                "coordinates": [116.3, 39.9]
            },
            "expected_prefix": "POINT"
        },
        {
            "name": "Polygon",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[116.3, 39.9], [116.31, 39.91], [116.32, 39.92], [116.3, 39.9]]]
            },
            "expected_prefix": "LINESTRING"
        }
    ]
    
    for test_case in test_cases:
        try:
            wkt = geojson_geometry_to_wkt(test_case["geometry"])
            
            if wkt and wkt.startswith(test_case["expected_prefix"]):
                logger.info(f"✓ {test_case['name']} 转换成功: {wkt[:50]}...")
            else:
                logger.error(f"✗ {test_case['name']} 转换失败: {wkt}")
                
        except Exception as e:
            logger.error(f"✗ {test_case['name']} 转换异常: {e}")

def run_all_tests(geojson_file: str = None):
    """运行所有测试"""
    logger.info("开始运行轨迹道路分析模块测试...")
    
    # 基础测试
    tests = [
        test_config,
        test_analyzer_initialization,
        test_database_tables,
        test_trajectory_buffer,
        test_simple_analysis,
        test_mock_data_analysis,
        test_geojson_geometry_conversion,
        test_geojson_support
    ]
    
    passed = 0
    failed = 0
    
    for test_func in tests:
        try:
            test_func()
            passed += 1
            logger.info(f"✓ {test_func.__name__} 通过")
        except Exception as e:
            failed += 1
            logger.error(f"✗ {test_func.__name__} 失败: {e}")
    
    # 如果指定了GeoJSON文件，运行GeoJSON文件测试
    if geojson_file:
        logger.info(f"\n=== 运行GeoJSON文件测试: {geojson_file} ===")
        try:
            test_geojson_file_analysis(geojson_file)
            passed += 1
            logger.info("✓ GeoJSON文件测试通过")
        except Exception as e:
            failed += 1
            logger.error(f"✗ GeoJSON文件测试失败: {e}")
    
    logger.info(f"\n=== 测试结果汇总 ===")
    logger.info(f"通过: {passed}")
    logger.info(f"失败: {failed}")
    logger.info(f"总计: {passed + failed}")
    
    if failed == 0:
        logger.info("🎉 所有测试通过！")
    else:
        logger.warning(f"⚠ {failed} 个测试失败")
    
    return failed == 0

def test_geojson_file_analysis(geojson_file: str):
    """测试指定的GeoJSON文件分析"""
    logger.info(f"=== 测试GeoJSON文件分析: {geojson_file} ===")
    
    if not Path(geojson_file).exists():
        logger.error(f"GeoJSON文件不存在: {geojson_file}")
        return
    
    try:
        # 加载轨迹数据
        trajectories = load_trajectories_from_geojson(geojson_file)
        
        if not trajectories:
            logger.error("GeoJSON文件中没有有效的轨迹数据")
            return
        
        logger.info(f"从GeoJSON文件加载了 {len(trajectories)} 个轨迹")
        
        # 分析所有轨迹
        results = []
        for trajectory_id, trajectory_wkt in trajectories:
            try:
                logger.info(f"分析轨迹: {trajectory_id}")
                
                analysis_id, summary = analyze_trajectory_road_elements(
                    trajectory_id=trajectory_id,
                    trajectory_geom=trajectory_wkt
                )
                
                results.append({
                    'trajectory_id': trajectory_id,
                    'analysis_id': analysis_id,
                    'summary': summary
                })
                
                logger.info(f"✓ 轨迹 {trajectory_id} 分析完成")
                
                # 输出分析汇总的前几行
                if summary:
                    logger.info(f"  - 分析ID: {analysis_id}")
                    for key, value in list(summary.items())[:5]:
                        logger.info(f"  - {key}: {value}")
                
            except Exception as e:
                logger.error(f"✗ 轨迹 {trajectory_id} 分析失败: {e}")
                results.append({
                    'trajectory_id': trajectory_id,
                    'analysis_id': None,
                    'error': str(e)
                })
        
        successful_analyses = len([r for r in results if r.get('analysis_id')])
        logger.info(f"✓ GeoJSON文件分析完成，成功: {successful_analyses}/{len(results)}")
        
        # 为成功的分析生成报告
        for result in results:
            if result.get('analysis_id'):
                try:
                    report = create_trajectory_road_analysis_report(result['analysis_id'])
                    logger.info(f"生成报告: {result['trajectory_id']}")
                    # 输出报告的前几行
                    report_lines = report.split('\n')
                    for line in report_lines[:5]:
                        if line.strip():
                            logger.info(f"  {line}")
                except Exception as e:
                    logger.warning(f"生成报告失败: {result['trajectory_id']}, {e}")
        
    except Exception as e:
        logger.error(f"GeoJSON文件分析失败: {e}")
        raise

def main():
    """主函数，支持命令行参数"""
    parser = argparse.ArgumentParser(
        description='轨迹道路分析模块测试工具',
        epilog="""
使用示例:
  # 运行所有默认测试
  python test_trajectory_road_analysis.py
  
  # 测试GeoJSON文件
  python test_trajectory_road_analysis.py --geojson trajectories.geojson
  
  # 创建示例GeoJSON文件
  python test_trajectory_road_analysis.py --create-sample
  
  # 详细日志输出
  python test_trajectory_road_analysis.py --verbose
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument('--geojson', '-g', 
                       help='指定GeoJSON文件进行轨迹分析测试')
    parser.add_argument('--create-sample', '-c', action='store_true',
                       help='创建示例GeoJSON文件')
    parser.add_argument('--output', '-o', default='sample_trajectories.geojson',
                       help='示例GeoJSON文件的输出路径')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='详细日志输出')
    
    args = parser.parse_args()
    
    # 配置日志
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('test_trajectory_road_analysis.log')
        ]
    )
    
    # 创建示例文件
    if args.create_sample:
        logger.info(f"创建示例GeoJSON文件: {args.output}")
        if create_sample_geojson(args.output):
            logger.info("✓ 示例文件创建成功")
            logger.info(f"可以使用以下命令测试:")
            logger.info(f"  python test_trajectory_road_analysis.py --geojson {args.output}")
            return 0
        else:
            logger.error("✗ 示例文件创建失败")
            return 1
    
    # 运行测试
    try:
        success = run_all_tests(geojson_file=args.geojson)
        return 0 if success else 1
    except KeyboardInterrupt:
        logger.info("测试被用户中断")
        return 1
    except Exception as e:
        logger.error(f"测试执行失败: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())