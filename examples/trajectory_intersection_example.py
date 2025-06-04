#!/usr/bin/env python3
"""
轨迹交集分析使用示例

演示如何使用新开发的intersection overlay分析功能
"""

import sys
from pathlib import Path
import logging
import json
from datetime import datetime

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from spdatalab.fusion import TrajectoryIntersectionAnalyzer, OverlayAnalyzer, IntersectionProcessor
from spdatalab.common.config import LOCAL_DSN

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def basic_trajectory_intersection_example():
    """基础轨迹交集分析示例"""
    logger.info("=== 基础轨迹交集分析示例 ===")
    
    # 初始化分析器
    analyzer = TrajectoryIntersectionAnalyzer()
    
    # 1. 轨迹与路口交集分析
    logger.info("1. 分析轨迹与路口的交集...")
    junction_results = analyzer.analyze_trajectory_intersection_with_junctions(
        trajectory_table="clips_bbox",
        junction_table="intersections",
        buffer_meters=25.0,
        output_table="trajectory_junction_results"
    )
    
    if len(junction_results) > 0:
        logger.info(f"找到 {len(junction_results)} 个轨迹-路口交集")
        logger.info(f"涉及 {junction_results['scene_token'].nunique()} 个唯一场景")
        logger.info(f"涉及 {junction_results['city_id'].nunique()} 个城市")
        
        # 显示统计信息
        avg_distance = junction_results['distance_meters'].mean()
        avg_area = junction_results['intersection_area_m2'].mean()
        logger.info(f"平均距离: {avg_distance:.2f} 米")
        logger.info(f"平均交集面积: {avg_area:.2f} 平方米")
    else:
        logger.warning("没有找到轨迹-路口交集")
    
    # 2. 轨迹间交集分析
    logger.info("2. 分析轨迹间的交集...")
    traj_results = analyzer.analyze_trajectory_to_trajectory_intersection(
        trajectory_table1="clips_bbox",
        buffer_meters=10.0,
        time_tolerance_seconds=1800,  # 30分钟
        output_table="trajectory_to_trajectory_results"
    )
    
    if len(traj_results) > 0:
        logger.info(f"找到 {len(traj_results)} 个轨迹间交集")
        
        # 按时间分类统计
        time_categories = traj_results['intersection_type'].value_counts()
        logger.info("交集类型分布:")
        for category, count in time_categories.items():
            logger.info(f"  {category}: {count}")
    else:
        logger.warning("没有找到轨迹间交集")
    
    return junction_results, traj_results

def overlay_analysis_example():
    """通用叠置分析示例"""
    logger.info("=== 通用叠置分析示例 ===")
    
    # 初始化叠置分析器
    overlay_analyzer = OverlayAnalyzer()
    
    # 1. 缓冲区分析
    logger.info("1. 生成轨迹缓冲区...")
    buffer_results = overlay_analyzer.buffer_analysis(
        input_table="clips_bbox",
        buffer_distance_meters=50.0,
        dissolve=False,
        output_table="trajectory_buffers"
    )
    
    if len(buffer_results) > 0:
        logger.info(f"生成了 {len(buffer_results)} 个缓冲区")
        total_area = buffer_results['buffer_area_m2'].sum()
        logger.info(f"总缓冲区面积: {total_area:.2f} 平方米")
    
    # 2. 邻近分析
    logger.info("2. 执行邻近分析...")
    proximity_results = overlay_analyzer.proximity_analysis(
        target_table="clips_bbox",
        reference_table="intersections",
        max_distance_meters=200.0,
        k_nearest=3,  # 每个轨迹找最近的3个路口
        output_table="trajectory_proximity_results"
    )
    
    if len(proximity_results) > 0:
        logger.info(f"找到 {len(proximity_results)} 个邻近关系")
        
        # 按距离分类统计
        proximity_categories = proximity_results['proximity_category'].value_counts()
        logger.info("邻近距离分布:")
        for category, count in proximity_categories.items():
            logger.info(f"  {category}: {count}")
    
    return buffer_results, proximity_results

def comprehensive_analysis_example():
    """综合分析示例"""
    logger.info("=== 综合交集分析示例 ===")
    
    # 初始化高级处理器
    processor = IntersectionProcessor(max_workers=2)
    
    # 配置综合分析
    analysis_config = {
        # 轨迹与路口交集分析
        'trajectory_junction_analysis': {
            'enabled': True,
            'trajectory_table': 'clips_bbox',
            'junction_table': 'intersections',
            'buffer_meters': 20.0,
            'output_table': 'comprehensive_junction_results'
        },
        
        # 轨迹与道路交集分析
        'trajectory_road_analysis': {
            'enabled': False,  # 假设没有roads表
            'trajectory_table': 'clips_bbox',
            'road_table': 'roads',
            'buffer_meters': 15.0
        },
        
        # 轨迹间交集分析
        'trajectory_to_trajectory_analysis': {
            'enabled': True,
            'trajectory_table1': 'clips_bbox',
            'buffer_meters': 8.0,
            'time_tolerance_seconds': 600,  # 10分钟
            'output_table': 'comprehensive_traj_results'
        },
        
        # 生成可视化报告
        'generate_visualizations': True
    }
    
    # 运行综合分析
    output_dir = "data/intersection_analysis_results"
    results = processor.run_comprehensive_intersection_analysis(
        analysis_config=analysis_config,
        output_dir=output_dir,
        export_formats=['csv', 'geojson', 'gpkg']
    )
    
    logger.info("综合分析完成！")
    logger.info(f"结果保存到: {output_dir}")
    logger.info(f"导出文件数量: {len(results['exported_files'])}")
    
    # 显示结果统计
    for analysis_type, gdf in results['results'].items():
        if len(gdf) > 0:
            logger.info(f"{analysis_type}: {len(gdf)} 个交集")
    
    return results

def parallel_analysis_example():
    """并行分析示例"""
    logger.info("=== 并行分析示例 ===")
    
    processor = IntersectionProcessor(max_workers=3)
    
    # 假设有多个城市
    city_ids = ['city_001', 'city_002', 'city_003']
    
    # 配置分析参数
    analysis_params = {
        'trajectory_table': 'clips_bbox',
        'junction_table': 'intersections',
        'buffer_meters': 25.0
    }
    
    # 运行并行分析
    parallel_results = processor.run_parallel_intersection_analysis(
        city_ids=city_ids,
        analysis_type='trajectory_junction',
        analysis_params=analysis_params,
        output_dir="data/parallel_analysis_results"
    )
    
    logger.info("并行分析完成！")
    for city_id, result_gdf in parallel_results.items():
        logger.info(f"城市 {city_id}: {len(result_gdf)} 个交集")
    
    return parallel_results

def quality_evaluation_example():
    """结果质量评估示例"""
    logger.info("=== 结果质量评估示例 ===")
    
    # 先运行基础分析获取结果
    analyzer = TrajectoryIntersectionAnalyzer()
    junction_results = analyzer.analyze_trajectory_intersection_with_junctions(
        buffer_meters=20.0
    )
    
    if len(junction_results) > 0:
        # 初始化处理器
        processor = IntersectionProcessor()
        
        # 评估结果质量
        quality_report = processor.evaluate_intersection_quality(
            intersection_results=junction_results,
            quality_thresholds={
                'min_intersection_area_m2': 2.0,
                'max_distance_meters': 500.0,
                'min_intersection_count': 10
            }
        )
        
        logger.info("质量评估结果:")
        logger.info(f"总交集数: {quality_report['total_intersections']}")
        logger.info(f"质量得分: {quality_report['quality_score']}")
        logger.info(f"质量等级: {quality_report['quality_level']}")
        
        if quality_report['quality_flags']:
            logger.warning("发现的质量问题:")
            for flag, value in quality_report['quality_flags'].items():
                logger.warning(f"  {flag}: {value}")
        
        if quality_report['recommendations']:
            logger.info("改进建议:")
            for rec in quality_report['recommendations']:
                logger.info(f"  - {rec}")
    
    return quality_report if len(junction_results) > 0 else None

def export_example_results(results_dict, output_file="data/example_results_summary.json"):
    """导出示例结果汇总"""
    logger.info("=== 导出结果汇总 ===")
    
    # 准备可序列化的汇总数据
    summary = {
        'export_time': datetime.now().isoformat(),
        'analysis_summary': {}
    }
    
    for analysis_name, gdf in results_dict.items():
        if hasattr(gdf, '__len__'):
            summary['analysis_summary'][analysis_name] = {
                'record_count': len(gdf),
                'columns': list(gdf.columns) if hasattr(gdf, 'columns') else [],
                'unique_scenes': gdf['scene_token'].nunique() if 'scene_token' in getattr(gdf, 'columns', []) else 0,
                'unique_cities': gdf['city_id'].nunique() if 'city_id' in getattr(gdf, 'columns', []) else 0
            }
        else:
            summary['analysis_summary'][analysis_name] = str(gdf)
    
    # 保存到文件
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    logger.info(f"结果汇总已保存到: {output_file}")

def main():
    """主函数，运行所有示例"""
    logger.info("开始运行轨迹交集分析示例...")
    
    all_results = {}
    
    try:
        # 1. 基础轨迹交集分析
        junction_results, traj_results = basic_trajectory_intersection_example()
        all_results['basic_junction'] = junction_results
        all_results['basic_trajectory'] = traj_results
        
        # 2. 通用叠置分析
        buffer_results, proximity_results = overlay_analysis_example()
        all_results['buffer_analysis'] = buffer_results
        all_results['proximity_analysis'] = proximity_results
        
        # 3. 综合分析
        comprehensive_results = comprehensive_analysis_example()
        all_results['comprehensive_analysis'] = comprehensive_results
        
        # 4. 并行分析
        parallel_results = parallel_analysis_example()
        all_results['parallel_analysis'] = parallel_results
        
        # 5. 质量评估
        quality_report = quality_evaluation_example()
        all_results['quality_evaluation'] = quality_report
        
        # 6. 导出结果汇总
        export_example_results(all_results)
        
        logger.info("所有示例运行完成！")
        
    except Exception as e:
        logger.error(f"示例运行过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 