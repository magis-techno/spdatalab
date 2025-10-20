#!/usr/bin/env python3
"""
Grid轨迹聚类分析示例脚本
===============================

基于city_hotspots表的热点grid，对每个200m×200m区域内的高质量轨迹进行聚类分析。

核心功能：
1. 从city_hotspots表加载热点grid
2. 查询grid内的高质量轨迹点（workstage=2）
3. 按距离优先+时长上限策略切分轨迹段（50米/15秒）
4. 提取10维特征向量（速度、加速度、航向、形态）
5. DBSCAN聚类分析
6. 保存结果到数据库

使用方法：
    # 1. 对某个城市的hotspot grids进行聚类
    python examples/dataset/bbox_examples/grid_clustering_analysis.py \\
        --city A72 \\
        --min-distance 50 \\
        --max-duration 15
    
    # 2. 只处理前N个热点grids
    python examples/dataset/bbox_examples/grid_clustering_analysis.py \\
        --top-n 5 \\
        --eps 0.4 \\
        --min-samples 3
    
    # 3. 指定特定grid IDs
    python examples/dataset/bbox_examples/grid_clustering_analysis.py \\
        --grid-ids 123 456 789
    
    # 4. 导出聚类结果到GeoJSON
    python examples/dataset/bbox_examples/grid_clustering_analysis.py \\
        --city A72 \\
        --top-n 1 \\
        --export-geojson results.geojson

输出结果：
    1. 数据库表：grid_trajectory_segments, grid_clustering_summary
    2. 终端统计报告
    3. GeoJSON文件（可选）
"""

import sys
from pathlib import Path
import argparse
import json
from datetime import datetime

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.grid_trajectory_clustering import GridTrajectoryClusterer, ClusterConfig
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.grid_trajectory_clustering import GridTrajectoryClusterer, ClusterConfig

import pandas as pd
import geopandas as gpd
from shapely import wkt


def export_to_geojson(clusterer, city_id: str, analysis_id: str, output_file: str):
    """导出聚类结果到GeoJSON文件"""
    print(f"\n📤 导出聚类结果到GeoJSON...")
    
    # 查询结果
    sql = f"""
        SELECT 
            id,
            grid_id,
            city_id,
            dataset_name,
            segment_index,
            cluster_label,
            quality_flag,
            avg_speed,
            trajectory_length_m,
            duration,
            ST_AsText(geometry) as geometry_wkt
        FROM grid_trajectory_segments
        WHERE city_id = '{city_id}'
        AND quality_flag = 'valid'
        ORDER BY grid_id, cluster_label;
    """
    
    with clusterer.engine.connect() as conn:
        df = pd.read_sql(sql, conn)
    
    if df.empty:
        print("⚠️ 没有数据可导出")
        return
    
    # 转换为GeoDataFrame
    df['geometry'] = df['geometry_wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    
    # 删除WKT列
    gdf = gdf.drop(columns=['geometry_wkt'])
    
    # 导出
    gdf.to_file(output_file, driver='GeoJSON')
    
    print(f"✅ 成功导出 {len(gdf)} 个轨迹段到: {output_file}")
    print(f"   聚类数量: {gdf['cluster_label'].nunique()}")
    print(f"   Grid数量: {gdf['grid_id'].nunique()}")


def print_summary_stats_from_memory(stats_df: pd.DataFrame):
    """从内存统计结果打印汇总（不依赖数据库表）"""
    print(f"\n" + "="*70)
    print(f"📊 聚类结果汇总统计 (基于运行结果)")
    print(f"="*70)
    
    if stats_df.empty:
        print("\n⚠️ 没有统计数据")
        return
    
    # 筛选成功的grid
    successful = stats_df[stats_df['success'] == True]
    
    if successful.empty:
        print("\n⚠️ 没有成功处理的grid")
        return
    
    # 轨迹段统计（从stats_df汇总）
    total_points = successful['total_points'].sum()
    total_trajectories = successful['trajectory_count'].sum()
    total_segments = successful['total_segments'].sum()
    valid_segments = successful['valid_segments'].sum()
    grid_count = len(successful)
    
    print(f"\n📊 处理统计:")
    print(f"  处理Grid数: {grid_count}")
    print(f"  总轨迹点数: {total_points:,}")
    print(f"  总轨迹数: {total_trajectories:,}")
    print(f"  总轨迹段数: {total_segments:,}")
    print(f"  有效轨迹段: {valid_segments:,} ({valid_segments/max(total_segments,1)*100:.1f}%)")
    
    # 聚类统计（从cluster_info汇总）
    all_clusters = []
    for _, row in successful.iterrows():
        if 'cluster_info' in row and isinstance(row['cluster_info'], dict):
            for label, info in row['cluster_info'].items():
                all_clusters.append({
                    'grid_id': row['grid_id'],
                    'cluster_label': label,
                    'behavior_label': info.get('behavior_label', 'unknown'),
                    'segment_count': info.get('segment_count', 0),
                    'speed_range': info.get('speed_range', ''),
                    'avg_speed': info.get('centroid_avg_speed', 0)
                })
    
    if all_clusters:
        clusters_df = pd.DataFrame(all_clusters)
        
        # 按行为类型统计
        behavior_stats = clusters_df.groupby('behavior_label').agg({
            'cluster_label': 'count',
            'segment_count': 'sum',
            'avg_speed': 'mean'
        }).reset_index()
        behavior_stats.columns = ['behavior_label', 'cluster_count', 'total_segments', 'avg_speed']
        behavior_stats = behavior_stats.sort_values('total_segments', ascending=False)
        
        print(f"\n🎯 行为类型分布:")
        for _, row in behavior_stats.iterrows():
            print(f"  {row['behavior_label']:15s}: {row['cluster_count']:3.0f}个聚类, "
                  f"{row['total_segments']:5.0f}段 "
                  f"(平均速度 {row['avg_speed']:.1f}m/s)")
        
        # 噪声统计
        noise_clusters = clusters_df[clusters_df['cluster_label'] == -1]
        if not noise_clusters.empty:
            noise_segments = noise_clusters['segment_count'].sum()
            print(f"\n⚠️ 噪声点: {len(noise_clusters)}个噪声簇, {noise_segments}段 "
                  f"({noise_segments/valid_segments*100:.1f}%)")
    
    # 质量过滤统计（从quality_stats汇总）
    all_quality = {}
    for _, row in successful.iterrows():
        if 'quality_stats' in row and isinstance(row['quality_stats'], dict):
            for flag, count in row['quality_stats'].items():
                all_quality[flag] = all_quality.get(flag, 0) + count
    
    if all_quality:
        print(f"\n🔍 质量过滤统计:")
        quality_total = sum(all_quality.values())
        for flag, count in sorted(all_quality.items(), key=lambda x: x[1], reverse=True):
            print(f"  {flag:20s}: {count:5.0f} ({count/quality_total*100:.1f}%)")


def print_summary_stats_from_db(clusterer, city_id: str = None):
    """从数据库查询统计（需要表已存在）"""
    print(f"\n" + "="*70)
    print(f"📊 聚类结果汇总统计 (从数据库)")
    print(f"="*70)
    
    try:
        # 查询轨迹段统计
        where_clause = f"WHERE city_id = '{city_id}'" if city_id else ""
        
        segments_sql = f"""
            SELECT 
                COUNT(*) as total_segments,
                COUNT(*) FILTER (WHERE quality_flag = 'valid') as valid_segments,
                COUNT(DISTINCT grid_id) as grid_count,
                COUNT(DISTINCT cluster_label) as cluster_count,
                COUNT(*) FILTER (WHERE cluster_label = -1) as noise_count
            FROM grid_trajectory_segments
            {where_clause};
        """
        
        with clusterer.engine.connect() as conn:
            stats = pd.read_sql(segments_sql, conn).iloc[0]
        
        print(f"\n轨迹段统计:")
        print(f"  总轨迹段数: {stats['total_segments']}")
        print(f"  有效轨迹段: {stats['valid_segments']} ({stats['valid_segments']/max(stats['total_segments'],1)*100:.1f}%)")
        print(f"  Grid数量: {stats['grid_count']}")
        print(f"  聚类数量: {stats['cluster_count']}")
        print(f"  噪声点: {stats['noise_count']} ({stats['noise_count']/max(stats['valid_segments'],1)*100:.1f}%)")
        
        # 查询聚类行为统计
        behavior_sql = f"""
            SELECT 
                behavior_label,
                COUNT(*) as count,
                AVG(segment_count) as avg_segments_per_cluster,
                AVG(centroid_avg_speed) as avg_speed
            FROM grid_clustering_summary
            {where_clause.replace('city_id', 's.city_id') if where_clause else ''}
            GROUP BY behavior_label
            ORDER BY count DESC;
        """
        
        with clusterer.engine.connect() as conn:
            behavior_stats = pd.read_sql(behavior_sql, conn)
        
        if not behavior_stats.empty:
            print(f"\n行为类型分布:")
            for _, row in behavior_stats.iterrows():
                print(f"  {row['behavior_label']:15s}: {row['count']:3.0f}个聚类 "
                      f"(平均 {row['avg_segments_per_cluster']:.1f}段/聚类, "
                      f"平均速度 {row['avg_speed']:.1f}m/s)")
        
        # 查询质量过滤统计
        quality_sql = f"""
            SELECT 
                quality_flag,
                COUNT(*) as count
            FROM grid_trajectory_segments
            {where_clause}
            GROUP BY quality_flag
            ORDER BY count DESC;
        """
        
        with clusterer.engine.connect() as conn:
            quality_stats = pd.read_sql(quality_sql, conn)
        
        if not quality_stats.empty:
            print(f"\n质量过滤统计:")
            for _, row in quality_stats.iterrows():
                print(f"  {row['quality_flag']:20s}: {row['count']:5.0f} ({row['count']/quality_stats['count'].sum()*100:.1f}%)")
    
    except Exception as e:
        print(f"\n⚠️ 无法从数据库查询统计: {e}")
        print(f"   提示：可能需要先创建数据库表，或使用内存统计")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Grid轨迹聚类分析',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例：
  # 对某个城市的前5个热点grid进行聚类
  python grid_clustering_analysis.py --city A72 --top-n 5
  
  # 指定特定的grid IDs
  python grid_clustering_analysis.py --grid-ids 123 456 789
  
  # 自定义切分和聚类参数
  python grid_clustering_analysis.py --city A72 --min-distance 30 --max-duration 20 --eps 0.3
  
  # 导出结果到GeoJSON
  python grid_clustering_analysis.py --city A72 --top-n 1 --export-geojson results.geojson
        """
    )
    
    # Grid选择参数
    grid_group = parser.add_argument_group('Grid选择')
    grid_group.add_argument('--city', help='城市ID过滤（如：A72）')
    grid_group.add_argument('--top-n', type=int, help='只处理前N个热点grid')
    grid_group.add_argument('--grid-ids', type=int, nargs='+', help='指定特定grid的ID列表')
    
    # 查询参数
    query_group = parser.add_argument_group('轨迹查询参数')
    query_group.add_argument('--query-limit', type=int, default=50000,
                            help='每个grid的轨迹点查询限制，默认50000')
    
    # 切分参数
    segment_group = parser.add_argument_group('轨迹切分参数')
    segment_group.add_argument('--min-distance', type=float, default=50.0, 
                              help='主切分条件：累计距离（米），默认50')
    segment_group.add_argument('--max-duration', type=float, default=15.0,
                              help='强制切分：时长上限（秒），默认15')
    segment_group.add_argument('--min-points', type=int, default=5,
                              help='最少点数要求，默认5')
    segment_group.add_argument('--time-gap', type=float, default=3.0,
                              help='时间间隔断开阈值（秒），默认3')
    
    # 质量过滤参数
    quality_group = parser.add_argument_group('质量过滤参数')
    quality_group.add_argument('--min-movement', type=float, default=10.0,
                              help='最小移动距离（米），默认10')
    quality_group.add_argument('--max-jump', type=float, default=100.0,
                              help='最大点间距（米），默认100')
    quality_group.add_argument('--max-speed', type=float, default=30.0,
                              help='最大合理速度（m/s），默认30')
    
    # 聚类参数
    cluster_group = parser.add_argument_group('聚类参数')
    cluster_group.add_argument('--eps', type=float, default=0.8,
                              help='DBSCAN距离阈值，默认0.8（减少噪声）')
    cluster_group.add_argument('--min-samples', type=int, default=5,
                              help='DBSCAN最小样本数，默认5（更稳定的簇）')
    
    # 输出参数
    output_group = parser.add_argument_group('输出参数')
    output_group.add_argument('--save-to-database', action='store_true',
                             help='保存结果到数据库表（需要先创建表）')
    output_group.add_argument('--export-geojson', metavar='FILE',
                             help='导出结果到GeoJSON文件')
    output_group.add_argument('--show-summary', action='store_true', default=True,
                             help='显示汇总统计（默认开启）')
    
    args = parser.parse_args()
    
    # 打印配置
    print("🚀 Grid轨迹聚类分析")
    print("=" * 70)
    print(f"\n📋 配置参数:")
    print(f"   Grid选择: ", end="")
    if args.city:
        print(f"城市={args.city}", end="")
    if args.top_n:
        print(f", 前{args.top_n}个", end="")
    if args.grid_ids:
        print(f", IDs={args.grid_ids}", end="")
    if not (args.city or args.top_n or args.grid_ids):
        print("所有grid", end="")
    print()
    
    print(f"   查询限制: {args.query_limit}点/grid")
    print(f"   切分策略: {args.min_distance}米 / {args.max_duration}秒")
    print(f"   质量过滤: 移动>{args.min_movement}m, 跳点<{args.max_jump}m, 速度<{args.max_speed}m/s")
    print(f"   聚类参数: eps={args.eps}, min_samples={args.min_samples}")
    print(f"   数据库保存: {'✅ 开启' if args.save_to_database else '❌ 关闭（仅内存统计）'}")
    
    # 创建配置
    config = ClusterConfig(
        query_limit=args.query_limit,
        min_distance=args.min_distance,
        max_duration=args.max_duration,
        min_points=args.min_points,
        time_gap_threshold=args.time_gap,
        min_movement=args.min_movement,
        max_jump=args.max_jump,
        max_speed=args.max_speed,
        eps=args.eps,
        min_samples=args.min_samples,
        save_to_database=args.save_to_database
    )
    
    # 创建聚类器
    clusterer = GridTrajectoryClusterer(config)
    
    # 执行聚类
    try:
        stats_df = clusterer.process_all_grids(
            city_id=args.city,
            max_grids=args.top_n,
            grid_ids=args.grid_ids
        )
        
        if stats_df.empty:
            print("\n❌ 没有处理任何grid")
            return 1
        
        # 保存统计到CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        stats_file = f"grid_clustering_stats_{timestamp}.csv"
        stats_df.to_csv(stats_file, index=False)
        print(f"\n💾 统计信息已保存到: {stats_file}")
        
        # 显示汇总统计（从内存，不依赖数据库表）
        if args.show_summary:
            print_summary_stats_from_memory(stats_df)
        
        # 导出GeoJSON（需要数据库表）
        if args.export_geojson:
            if args.save_to_database:
                export_to_geojson(clusterer, args.city, analysis_id, args.export_geojson)
            else:
                print("\n⚠️ GeoJSON导出需要先保存到数据库")
                print("   请重新运行并添加 --save-to-database 参数")
                print("   或使用内存统计结果")
        
        print(f"\n✅ 分析完成！")
        
        # 根据是否保存到数据库，给出不同的下一步建议
        if args.save_to_database:
            print(f"\n💡 下一步操作:")
            print(f"   1. 在QGIS中加载 grid_trajectory_segments 表")
            print(f"   2. 按 cluster_label 字段分类着色")
            print(f"   3. 查看 grid_clustering_summary 表了解聚类统计")
            if args.export_geojson:
                print(f"   4. 在QGIS/Kepler.gl中加载 {args.export_geojson}")
        else:
            print(f"\n💡 下一步操作:")
            print(f"   • 查看上方统计结果，调整参数（--eps, --min-samples）")
            print(f"   • 若要保存到数据库查看详细结果:")
            print(f"     1. 创建数据库表: psql -f sql/grid_clustering_tables.sql")
            print(f"     2. 重新运行并添加 --save-to-database 参数")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ 分析失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())







