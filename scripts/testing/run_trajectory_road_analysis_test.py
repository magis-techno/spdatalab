#!/usr/bin/env python3
"""
轨迹道路分析完整测试脚本

步骤：
1. 测试Hive连接
2. 创建示例GeoJSON文件
3. 运行轨迹道路分析
"""

import logging
import sys
import json
from pathlib import Path

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

def test_hive_connection_simple():
    """简单的Hive连接测试"""
    try:
        from spdatalab.common.io_hive import hive_cursor
        
        logger.info("测试Hive连接...")
        with hive_cursor("rcdatalake_gy1") as cur:
            cur.execute("SELECT COUNT(*) FROM full_road LIMIT 1")
            result = cur.fetchone()
            if result:
                logger.info(f"✓ 连接成功，full_road表有 {result[0]} 行")
                return True
            else:
                logger.error("✗ 连接失败，查询无结果")
                return False
                
    except Exception as e:
        logger.error(f"✗ 连接测试失败: {e}")
        return False

def create_test_geojson():
    """创建测试用的GeoJSON文件"""
    sample_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "id": "test_trajectory_001",
                    "name": "北京测试轨迹",
                    "description": "用于测试的北京道路轨迹"
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [116.4526, 39.9042],  # 起点
                        [116.4556, 39.9052],
                        [116.4586, 39.9062],
                        [116.4616, 39.9072],
                        [116.4646, 39.9082],  # 终点
                    ]
                }
            }
        ]
    }
    
    output_file = "test_trajectory.geojson"
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, ensure_ascii=False, indent=2)
        logger.info(f"✓ 创建测试GeoJSON文件: {output_file}")
        return output_file
    except Exception as e:
        logger.error(f"✗ 创建GeoJSON文件失败: {e}")
        return None

def geojson_geometry_to_wkt(geometry):
    """将GeoJSON几何转换为WKT"""
    try:
        geom_type = geometry.get('type', '')
        coordinates = geometry.get('coordinates', [])
        
        if geom_type == 'LineString':
            points = [f"{coord[0]} {coord[1]}" for coord in coordinates]
            return f"LINESTRING({', '.join(points)})"
        else:
            logger.warning(f"不支持的几何类型: {geom_type}")
            return ""
    except Exception as e:
        logger.error(f"几何转换失败: {e}")
        return ""

def run_trajectory_analysis():
    """运行轨迹道路分析"""
    try:
        # 创建测试GeoJSON
        geojson_file = create_test_geojson()
        if not geojson_file:
            return False
        
        # 加载轨迹数据
        logger.info("加载轨迹数据...")
        with open(geojson_file, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)
        
        # 提取第一个轨迹
        feature = geojson_data['features'][0]
        trajectory_id = feature['properties']['id']
        geometry = feature['geometry']
        trajectory_wkt = geojson_geometry_to_wkt(geometry)
        
        if not trajectory_wkt:
            logger.error("✗ 轨迹几何转换失败")
            return False
        
        logger.info(f"✓ 轨迹ID: {trajectory_id}")
        logger.info(f"✓ 轨迹WKT: {trajectory_wkt}")
        
        # 执行分析
        logger.info("开始轨迹道路分析...")
        
        # 创建配置
        config = TrajectoryRoadAnalysisConfig()
        
        # 执行分析
        analysis_id, summary = analyze_trajectory_road_elements(
            trajectory_id=trajectory_id,
            trajectory_geom=trajectory_wkt,
            config=config
        )
        
        if analysis_id:
            logger.info(f"✓ 分析完成，分析ID: {analysis_id}")
            logger.info(f"✓ 汇总信息: {summary}")
            
            # 生成报告
            report = create_trajectory_road_analysis_report(analysis_id, config)
            logger.info("✓ 分析报告:")
            print("\n" + "="*50)
            print(report)
            print("="*50)
            
            return True
        else:
            logger.error("✗ 分析失败")
            return False
            
    except Exception as e:
        logger.error(f"✗ 轨迹分析失败: {e}")
        import traceback
        logger.error(f"详细错误: {traceback.format_exc()}")
        return False

def main():
    """主函数"""
    print("=" * 60)
    print("轨迹道路分析完整测试")
    print("=" * 60)
    
    # 步骤1：测试Hive连接
    print("\n步骤1：测试Hive连接")
    print("-" * 30)
    if not test_hive_connection_simple():
        print("❌ Hive连接测试失败，请检查数据库连接")
        return False
    
    # 步骤2：运行轨迹分析
    print("\n步骤2：运行轨迹道路分析")
    print("-" * 30)
    if not run_trajectory_analysis():
        print("❌ 轨迹分析失败")
        return False
    
    print("\n🎉 所有测试完成!")
    print("轨迹道路分析模块工作正常")
    return True

if __name__ == "__main__":
    main() 