#!/usr/bin/env python3
"""
收费站轨迹分析示例

这个示例展示如何：
1. 查找intersectiontype=2的收费站数据
2. 分析收费站范围内的轨迹数据
3. 按dataset_name对轨迹进行聚合
4. 导出结果供QGIS可视化

使用方法：
    python examples/toll_station_analysis_example.py

环境要求：
    - local_pg数据库连接正常
    - 远程数据库连接正常
    - 已有bbox数据和intersection数据
    - 已有ddi_data_points轨迹数据
"""

import sys
import logging
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.spdatalab.fusion.toll_station_analysis import (
    TollStationAnalyzer,
    TollStationAnalysisConfig,
    analyze_toll_station_trajectories,
    get_toll_station_analysis_summary,
    export_toll_station_results_for_qgis
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def basic_analysis_example():
    """基础分析示例"""
    print("=" * 60)
    print("收费站轨迹分析 - 基础示例")
    print("=" * 60)
    
    # 使用默认配置进行分析
    try:
        toll_stations, trajectory_results, analysis_id = analyze_toll_station_trajectories(
            num_bbox=500,           # 分析500个bbox
            city_filter=None,       # 不限制城市
            use_buffer=True,        # 使用缓冲区
            buffer_distance_meters=100.0  # 缓冲区100米
        )
        
        print(f"\n✅ 分析完成，分析ID: {analysis_id}")
        print(f"📍 找到收费站数量: {len(toll_stations)}")
        
        if not toll_stations.empty:
            print(f"🚗 轨迹分析结果: {len(trajectory_results)} 个数据集-收费站组合")
            
            # 显示收费站信息
            print("\n📋 收费站详情:")
            if 'city_id' in toll_stations.columns:
                city_stats = toll_stations['city_id'].value_counts()
                for city, count in city_stats.head(5).items():
                    print(f"   {city}: {count} 个收费站")
            
            # 显示轨迹统计
            if not trajectory_results.empty:
                print("\n📊 轨迹数据统计:")
                total_trajectories = trajectory_results['trajectory_count'].sum()
                total_datasets = trajectory_results['dataset_name'].nunique()
                print(f"   总轨迹数: {total_trajectories:,}")
                print(f"   数据集数: {total_datasets}")
                
                # 显示前5个数据集
                print("\n🔝 Top 5 数据集:")
                top_datasets = trajectory_results.groupby('dataset_name')['trajectory_count'].sum().sort_values(ascending=False).head(5)
                for dataset, count in top_datasets.items():
                    print(f"   {dataset}: {count:,} 条轨迹")
        
        return analysis_id
        
    except Exception as e:
        print(f"❌ 分析失败: {e}")
        logger.error(f"基础分析失败: {e}", exc_info=True)
        return None

def advanced_analysis_example():
    """高级分析示例 - 自定义配置"""
    print("\n" + "=" * 60)
    print("收费站轨迹分析 - 高级示例")
    print("=" * 60)
    
    # 自定义配置
    config = TollStationAnalysisConfig(
        buffer_distance_meters=200.0,    # 200米缓冲区
        max_trajectory_records=20000     # 增加轨迹记录限制
    )
    
    try:
        analyzer = TollStationAnalyzer(config)
        
        # 1. 查找收费站（指定城市）
        print("🔍 步骤1: 查找收费站...")
        toll_stations, analysis_id = analyzer.find_toll_stations(
            num_bbox=1000,
            city_filter="shanghai",  # 仅分析上海地区
            analysis_id="shanghai_toll_analysis"
        )
        
        if toll_stations.empty:
            print("⚠️ 未找到收费站数据，可能没有上海的数据或数据库连接问题")
            return None
        
        print(f"✅ 找到 {len(toll_stations)} 个收费站")
        
        # 2. 分析轨迹数据
        print("🔍 步骤2: 分析轨迹数据...")
        trajectory_results = analyzer.analyze_trajectories_in_toll_stations(
            analysis_id=analysis_id,
            use_buffer=True
        )
        
        # 3. 获取分析汇总
        print("📊 步骤3: 生成分析汇总...")
        summary = analyzer.get_analysis_summary(analysis_id)
        
        print(f"\n📈 分析汇总:")
        for key, value in summary.items():
            if key != 'error':
                print(f"   {key}: {value}")
        
        # 4. 导出QGIS数据
        print("🗺️ 步骤4: 导出QGIS可视化数据...")
        export_info = analyzer.export_results_for_qgis(analysis_id)
        
        print(f"\n🎯 QGIS可视化:")
        for view_type, view_name in export_info.items():
            print(f"   {view_type}: {view_name}")
        
        return analysis_id
        
    except Exception as e:
        print(f"❌ 高级分析失败: {e}")
        logger.error(f"高级分析失败: {e}", exc_info=True)
        return None

def city_comparison_example():
    """城市对比分析示例"""
    print("\n" + "=" * 60)
    print("收费站轨迹分析 - 城市对比")
    print("=" * 60)
    
    cities = ["shanghai", "beijing", "shenzhen"]
    analysis_results = {}
    
    for city in cities:
        try:
            print(f"\n🏙️ 分析城市: {city}")
            
            toll_stations, trajectory_results, analysis_id = analyze_toll_station_trajectories(
                num_bbox=300,
                city_filter=city,
                use_buffer=True,
                buffer_distance_meters=150.0
            )
            
            if not toll_stations.empty:
                summary = get_toll_station_analysis_summary(analysis_id)
                analysis_results[city] = {
                    'analysis_id': analysis_id,
                    'toll_stations': len(toll_stations),
                    'datasets': summary.get('unique_datasets', 0),
                    'trajectories': summary.get('total_trajectories', 0),
                    'points': summary.get('total_points', 0)
                }
                print(f"   ✅ {city}: {len(toll_stations)} 个收费站")
            else:
                print(f"   ⚠️ {city}: 未找到数据")
                
        except Exception as e:
            print(f"   ❌ {city}: 分析失败 - {e}")
    
    # 对比结果
    if analysis_results:
        print(f"\n📊 城市对比结果:")
        print(f"{'城市':<12} {'收费站':<8} {'数据集':<8} {'轨迹数':<12} {'数据点':<12}")
        print("-" * 60)
        
        for city, results in analysis_results.items():
            print(f"{city:<12} {results['toll_stations']:<8} {results['datasets']:<8} "
                  f"{results['trajectories']:<12,} {results['points']:<12,}")
    
    return analysis_results

def qgis_integration_example(analysis_id: str):
    """QGIS集成示例"""
    if not analysis_id:
        print("⚠️ 没有可用的分析ID，跳过QGIS集成示例")
        return
    
    print("\n" + "=" * 60)
    print("QGIS集成示例")
    print("=" * 60)
    
    try:
        # 导出QGIS视图
        export_info = export_toll_station_results_for_qgis(analysis_id)
        
        print("🗺️ QGIS可视化说明:")
        print("1. 在QGIS中连接到local_pg数据库")
        print("2. 添加以下视图作为图层:")
        
        for view_type, view_name in export_info.items():
            print(f"   - {view_name} ({view_type.replace('_', ' ').title()})")
        
        print("\n💡 使用建议:")
        print("- 收费站视图：显示收费站位置和缓冲区")
        print("- 轨迹统计视图：按数据集显示轨迹密度")
        print("- 使用不同颜色表示不同的dataset_name")
        print("- 使用符号大小表示轨迹数量")
        
        # 数据库连接信息
        print(f"\n🔗 数据库连接信息:")
        print(f"   Host: local_pg")
        print(f"   Port: 5432")
        print(f"   Database: postgres")
        print(f"   Username: postgres")
        
    except Exception as e:
        print(f"❌ QGIS集成示例失败: {e}")
        logger.error(f"QGIS集成失败: {e}", exc_info=True)

def main():
    """主函数"""
    print("🚀 收费站轨迹分析工具演示")
    print("功能：分析intersectiontype=2的收费站及范围内轨迹数据")
    
    # 基础分析
    analysis_id = basic_analysis_example()
    
    # 高级分析
    advanced_analysis_id = advanced_analysis_example()
    
    # 城市对比
    city_results = city_comparison_example()
    
    # QGIS集成
    qgis_integration_example(analysis_id or advanced_analysis_id)
    
    print("\n" + "=" * 60)
    print("✅ 演示完成!")
    print("=" * 60)
    
    if analysis_id or advanced_analysis_id:
        print(f"💾 分析结果已保存到本地数据库")
        print(f"🗺️ 可在QGIS中查看可视化结果")
    else:
        print("⚠️ 未生成分析结果，请检查数据库连接和数据")

if __name__ == "__main__":
    main() 