#!/usr/bin/env python3
"""
PG聚类性能基准测试脚本
====================

自动运行PostgreSQL聚类性能测试并分析结果

使用方法：
    python examples/dataset/bbox_examples/run_clustering_benchmark.py --city A263
"""

import sys
import time
from pathlib import Path
import argparse
from datetime import datetime

# 添加项目路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from spdatalab.dataset.bbox import LOCAL_DSN
except ImportError:
    sys.path.insert(0, str(project_root / "src"))
    from spdatalab.dataset.bbox import LOCAL_DSN

from sqlalchemy import create_engine, text
import pandas as pd

def create_clustering_results_table(conn):
    """创建聚类结果表"""
    create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS clustering_benchmark_results (
            id SERIAL PRIMARY KEY,
            city_id VARCHAR(50),
            method_name VARCHAR(50),
            cluster_id INTEGER,
            bbox_count INTEGER,
            cluster_rank INTEGER,
            test_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            performance_metrics JSONB
        );
        
        -- 添加几何列
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'clustering_benchmark_results' 
                AND column_name = 'geometry'
            ) THEN
                PERFORM AddGeometryColumn('public', 'clustering_benchmark_results', 'geometry', 4326, 'GEOMETRY', 2);
            END IF;
        END $$;
        
        -- 创建索引
        CREATE INDEX IF NOT EXISTS idx_clustering_results_city_method 
        ON clustering_benchmark_results (city_id, method_name);
        CREATE INDEX IF NOT EXISTS idx_clustering_results_geom 
        ON clustering_benchmark_results USING GIST (geometry);
    """)
    
    conn.execute(create_table_sql)
    conn.commit()

def save_clustering_results_to_db(conn, city_id, performance_results):
    """保存聚类结果到数据库供QGIS可视化"""
    
    print(f"\n💾 保存聚类结果到数据库...")
    
    # 创建结果表
    create_clustering_results_table(conn)
    
    # 清理该城市的旧结果
    cleanup_sql = text(f"""
        DELETE FROM clustering_benchmark_results 
        WHERE city_id = '{city_id}' 
        AND test_timestamp < NOW() - INTERVAL '1 hour';
    """)
    conn.execute(cleanup_sql)
    
    # 1. 保存DBSCAN聚类结果
    print("   📊 保存DBSCAN聚类结果...")
    dbscan_sql = text(f"""
        WITH dbscan_clusters AS (
            SELECT 
                id,
                geometry,
                ST_ClusterDBSCAN(geometry, 0.002, 5) OVER() as cluster_id
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
        ),
        cluster_stats AS (
            SELECT 
                cluster_id,
                COUNT(*) as bbox_count,
                ST_Centroid(ST_Collect(geometry)) as cluster_center,
                ST_ConvexHull(ST_Collect(geometry)) as cluster_boundary,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as cluster_rank
            FROM dbscan_clusters 
            WHERE cluster_id IS NOT NULL
            GROUP BY cluster_id
        )
        INSERT INTO clustering_benchmark_results 
        (city_id, method_name, cluster_id, bbox_count, cluster_rank, geometry, performance_metrics)
        SELECT 
            '{city_id}',
            'DBSCAN',
            cluster_id,
            bbox_count,
            cluster_rank,
            cluster_boundary,
            '{{"eps": 0.002, "min_samples": 5, "query_time": {performance_results["dbscan"]["query_time"]}}}'::jsonb
        FROM cluster_stats
        WHERE bbox_count >= 5;  -- 只保存有意义的聚类
    """)
    
    result = conn.execute(dbscan_sql)
    dbscan_count = result.rowcount
    
    # 2. 保存网格方法结果
    print("   🔲 保存网格方法结果...")
    grid_sql = text(f"""
        WITH grid_density AS (
            SELECT 
                floor(ST_X(ST_Centroid(geometry)) / 0.002)::int as grid_x,
                floor(ST_Y(ST_Centroid(geometry)) / 0.002)::int as grid_y,
                COUNT(*) as bbox_count
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
            GROUP BY grid_x, grid_y
            HAVING COUNT(*) >= 5
        ),
        grid_with_geom AS (
            SELECT 
                (grid_x || '_' || grid_y)::integer as grid_id,
                bbox_count,
                ROW_NUMBER() OVER (ORDER BY bbox_count DESC) as grid_rank,
                ST_MakeEnvelope(
                    grid_x * 0.002, 
                    grid_y * 0.002,
                    (grid_x + 1) * 0.002, 
                    (grid_y + 1) * 0.002, 
                    4326
                ) as grid_geom
            FROM grid_density
        )
        INSERT INTO clustering_benchmark_results 
        (city_id, method_name, cluster_id, bbox_count, cluster_rank, geometry, performance_metrics)
        SELECT 
            '{city_id}',
            'Grid',
            grid_id,
            bbox_count,
            grid_rank,
            grid_geom,
            '{{"grid_size": 0.002, "min_density": 5, "query_time": {performance_results["grid"]["query_time"]}}}'::jsonb
        FROM grid_with_geom;
    """)
    
    result = conn.execute(grid_sql)
    grid_count = result.rowcount
    
    # 3. 保存分层聚类结果
    print("   🏗️ 保存分层聚类结果...")
    hierarchical_sql = text(f"""
        WITH coarse_clusters AS (
            SELECT 
                geometry,
                ST_ClusterDBSCAN(geometry, 0.01, 20) OVER() as coarse_id
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true
        ),
        fine_clusters AS (
            SELECT 
                coarse_id,
                geometry,
                ST_ClusterDBSCAN(geometry, 0.002, 5) OVER(PARTITION BY coarse_id) as fine_id
            FROM coarse_clusters 
            WHERE coarse_id IS NOT NULL
        ),
        hierarchical_stats AS (
            SELECT 
                (coarse_id * 1000 + fine_id) as hierarchical_id,
                COUNT(*) as bbox_count,
                ST_Centroid(ST_Collect(geometry)) as cluster_center,
                ST_ConvexHull(ST_Collect(geometry)) as cluster_boundary,
                ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as cluster_rank
            FROM fine_clusters
            WHERE fine_id IS NOT NULL
            GROUP BY coarse_id, fine_id
        )
        INSERT INTO clustering_benchmark_results 
        (city_id, method_name, cluster_id, bbox_count, cluster_rank, geometry, performance_metrics)
        SELECT 
            '{city_id}',
            'Hierarchical',
            hierarchical_id,
            bbox_count,
            cluster_rank,
            cluster_boundary,
            '{{"coarse_eps": 0.01, "fine_eps": 0.002, "query_time": {performance_results["hierarchical"]["query_time"]}}}'::jsonb
        FROM hierarchical_stats
        WHERE bbox_count >= 3;
    """)
    
    result = conn.execute(hierarchical_sql)
    hierarchical_count = result.rowcount
    
    conn.commit()
    
    print(f"   ✅ DBSCAN结果: {dbscan_count} 个聚类")
    print(f"   ✅ 网格结果: {grid_count} 个网格")  
    print(f"   ✅ 分层结果: {hierarchical_count} 个聚类")
    
    # 创建QGIS友好的视图
    create_qgis_views(conn, city_id)

def create_qgis_views(conn, city_id):
    """创建QGIS友好的视图"""
    
    print("   🎨 创建QGIS可视化视图...")
    
    view_sql = text(f"""
        -- 聚类对比视图
        CREATE OR REPLACE VIEW qgis_clustering_comparison AS
        SELECT 
            id,
            city_id,
            method_name,
            cluster_id,
            bbox_count,
            cluster_rank,
            CASE 
                WHEN method_name = 'DBSCAN' THEN '#FF6B6B'
                WHEN method_name = 'Grid' THEN '#4ECDC4'  
                WHEN method_name = 'Hierarchical' THEN '#45B7D1'
            END as method_color,
            CASE 
                WHEN bbox_count >= 50 THEN 'Very High'
                WHEN bbox_count >= 20 THEN 'High'
                WHEN bbox_count >= 10 THEN 'Medium'
                ELSE 'Low'
            END as density_level,
            geometry,
            performance_metrics,
            test_timestamp
        FROM clustering_benchmark_results
        WHERE city_id = '{city_id}'
        ORDER BY method_name, cluster_rank;
        
        -- 方法性能对比视图
        CREATE OR REPLACE VIEW qgis_method_performance AS
        SELECT 
            method_name,
            COUNT(*) as cluster_count,
            MAX(bbox_count) as max_density,
            ROUND(AVG(bbox_count)::numeric, 2) as avg_density,
            (performance_metrics->>'query_time')::float as query_time_seconds,
            ST_ConvexHull(ST_Collect(geometry)) as method_coverage
        FROM clustering_benchmark_results
        WHERE city_id = '{city_id}'
        GROUP BY method_name, performance_metrics->>'query_time'
        ORDER BY query_time_seconds;
    """)
    
    conn.execute(view_sql)
    conn.commit()
    
    print(f"   ✅ 创建视图: qgis_clustering_comparison")
    print(f"   ✅ 创建视图: qgis_method_performance")

def run_clustering_benchmark(city_id='A263'):
    """运行聚类性能基准测试"""
    
    print("🚀 PostgreSQL聚类性能基准测试")
    print("=" * 50)
    print(f"测试城市: {city_id}")
    print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    engine = create_engine(LOCAL_DSN, future=True)
    
    results = {}
    
    with engine.connect() as conn:
        
        # 测试1: 基础数据统计
        print("\n📊 测试1: 基础数据统计")
        start_time = time.time()
        
        basic_stats_sql = text(f"""
            SELECT 
                COUNT(*) as total_bbox,
                COUNT(*) FILTER (WHERE all_good = true) as good_bbox,
                ROUND(AVG(ST_Area(geometry))::numeric, 8) as avg_area,
                ST_XMax(ST_Extent(geometry)) - ST_XMin(ST_Extent(geometry)) as width_deg,
                ST_YMax(ST_Extent(geometry)) - ST_YMin(ST_Extent(geometry)) as height_deg
            FROM clips_bbox_unified 
            WHERE city_id = '{city_id}' AND all_good = true;
        """)
        
        basic_stats = conn.execute(basic_stats_sql).fetchone()
        basic_time = time.time() - start_time
        
        print(f"   总bbox数: {basic_stats.total_bbox:,}")
        print(f"   优质bbox数: {basic_stats.good_bbox:,}")
        print(f"   城市范围: {basic_stats.width_deg:.4f}° × {basic_stats.height_deg:.4f}°")
        print(f"   查询耗时: {basic_time:.2f}秒")
        
        results['basic_stats'] = {
            'total_bbox': basic_stats.total_bbox,
            'good_bbox': basic_stats.good_bbox,
            'query_time': basic_time
        }
        
        # 测试2: DBSCAN聚类性能
        print("\n⚡ 测试2: DBSCAN聚类性能")
        start_time = time.time()
        
        dbscan_sql = text(f"""
            WITH dbscan_clusters AS (
                SELECT 
                    ST_ClusterDBSCAN(geometry, 0.002, 5) OVER() as cluster_id
                FROM clips_bbox_unified 
                WHERE city_id = '{city_id}' AND all_good = true
            )
            SELECT 
                COUNT(DISTINCT cluster_id) FILTER (WHERE cluster_id IS NOT NULL) as cluster_count,
                COUNT(*) FILTER (WHERE cluster_id IS NULL) as noise_points,
                COUNT(*) as total_points
            FROM dbscan_clusters;
        """)
        
        dbscan_stats = conn.execute(dbscan_sql).fetchone()
        dbscan_time = time.time() - start_time
        
        print(f"   聚类数量: {dbscan_stats.cluster_count}")
        print(f"   噪声点数: {dbscan_stats.noise_points}")
        print(f"   聚类率: {(1 - dbscan_stats.noise_points/dbscan_stats.total_points)*100:.1f}%")
        print(f"   查询耗时: {dbscan_time:.2f}秒")
        
        results['dbscan'] = {
            'cluster_count': dbscan_stats.cluster_count,
            'noise_points': dbscan_stats.noise_points,
            'clustering_rate': (1 - dbscan_stats.noise_points/dbscan_stats.total_points)*100,
            'query_time': dbscan_time
        }
        
        # 测试3: 网格方法对比
        print("\n🔲 测试3: 网格方法基准")
        start_time = time.time()
        
        grid_sql = text(f"""
            WITH grid_density AS (
                SELECT 
                    floor(ST_X(ST_Centroid(geometry)) / 0.002)::int as grid_x,
                    floor(ST_Y(ST_Centroid(geometry)) / 0.002)::int as grid_y,
                    COUNT(*) as bbox_count
                FROM clips_bbox_unified 
                WHERE city_id = '{city_id}' AND all_good = true
                GROUP BY grid_x, grid_y
                HAVING COUNT(*) >= 5
            )
            SELECT 
                COUNT(*) as total_grids,
                MAX(bbox_count) as max_density,
                ROUND(AVG(bbox_count)::numeric, 2) as avg_density
            FROM grid_density;
        """)
        
        grid_stats = conn.execute(grid_sql).fetchone()
        grid_time = time.time() - start_time
        
        print(f"   网格数量: {grid_stats.total_grids}")
        print(f"   最高密度: {grid_stats.max_density}")
        print(f"   平均密度: {grid_stats.avg_density}")
        print(f"   查询耗时: {grid_time:.2f}秒")
        
        results['grid'] = {
            'total_grids': grid_stats.total_grids,
            'max_density': grid_stats.max_density,
            'avg_density': float(grid_stats.avg_density),
            'query_time': grid_time
        }
        
        # 测试4: 分层聚类模拟
        print("\n🏗️ 测试4: 分层聚类性能")
        start_time = time.time()
        
        hierarchical_sql = text(f"""
            WITH coarse_clusters AS (
                SELECT 
                    geometry,
                    ST_ClusterDBSCAN(geometry, 0.01, 20) OVER() as coarse_id
                FROM clips_bbox_unified 
                WHERE city_id = '{city_id}' AND all_good = true
            ),
            fine_clusters AS (
                SELECT 
                    coarse_id,
                    ST_ClusterDBSCAN(geometry, 0.002, 5) OVER(PARTITION BY coarse_id) as fine_id
                FROM coarse_clusters 
                WHERE coarse_id IS NOT NULL
            )
            SELECT 
                COUNT(DISTINCT coarse_id) as coarse_clusters,
                COUNT(DISTINCT (coarse_id, fine_id)) FILTER (WHERE fine_id IS NOT NULL) as fine_clusters
            FROM fine_clusters;
        """)
        
        hierarchical_stats = conn.execute(hierarchical_sql).fetchone()
        hierarchical_time = time.time() - start_time
        
        print(f"   粗聚类数: {hierarchical_stats.coarse_clusters}")
        print(f"   细聚类数: {hierarchical_stats.fine_clusters}")
        print(f"   查询耗时: {hierarchical_time:.2f}秒")
        
        results['hierarchical'] = {
            'coarse_clusters': hierarchical_stats.coarse_clusters,
            'fine_clusters': hierarchical_stats.fine_clusters,
            'query_time': hierarchical_time
        }
        
        # 保存详细聚类结果到数据库表供QGIS可视化
        save_clustering_results_to_db(conn, city_id, results)
    
    # 性能分析和建议
    print("\n" + "=" * 50)
    print("📈 性能分析报告")
    print("=" * 50)
    
    total_bbox = results['basic_stats']['good_bbox']
    
    print(f"\n🎯 数据规模: {total_bbox:,} bbox")
    
    # 效率对比
    print(f"\n⏱️ 方法耗时对比:")
    print(f"   网格方法:     {results['grid']['query_time']:.2f}秒")
    print(f"   DBSCAN聚类:   {results['dbscan']['query_time']:.2f}秒")
    print(f"   分层聚类:     {results['hierarchical']['query_time']:.2f}秒")
    
    # 结果质量对比
    print(f"\n📊 结果质量对比:")
    print(f"   网格热点数:   {results['grid']['total_grids']}")
    print(f"   DBSCAN聚类数: {results['dbscan']['cluster_count']}")
    print(f"   分层细聚类数: {results['hierarchical']['fine_clusters']}")
    
    # 性能建议
    print(f"\n💡 性能建议:")
    if total_bbox < 10000:
        print("   ✅ 数据量较小，建议使用网格方法（最快）")
    elif total_bbox < 50000:
        print("   ⚖️ 数据量适中，DBSCAN和网格方法性能接近")
        print("   💡 建议：网格方法用于密度分析，DBSCAN用于形状识别")
    else:
        print("   🚀 数据量较大，建议使用分层聚类方法")
        print("   💡 策略：DBSCAN粗筛 + 网格精化")
    
    # 效率比较
    grid_speed = total_bbox / results['grid']['query_time']
    dbscan_speed = total_bbox / results['dbscan']['query_time']
    
    print(f"\n📊 处理速度:")
    print(f"   网格方法: {grid_speed:,.0f} bbox/秒")
    print(f"   DBSCAN:   {dbscan_speed:,.0f} bbox/秒")
    
    if dbscan_speed > grid_speed * 0.8:
        print("   🎯 DBSCAN性能可接受，可考虑结合使用")
    else:
        print("   ⚡ 网格方法明显更快，建议优先使用")
    
    return results

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='PostgreSQL聚类性能基准测试')
    parser.add_argument('--city', default='A263', help='测试城市ID')
    
    args = parser.parse_args()
    
    try:
        results = run_clustering_benchmark(args.city)
        
        print(f"\n✅ 测试完成！")
        print(f"💾 结果已保存到数据库表: clustering_benchmark_results")
        print(f"🎨 QGIS可视化视图已创建:")
        print(f"   • qgis_clustering_comparison - 聚类对比视图")
        print(f"   • qgis_method_performance - 性能对比视图")
        print(f"\n🎯 QGIS使用指南:")
        print(f"   1. 连接数据库: host=local_pg, database=postgres")
        print(f"   2. 加载图层: qgis_clustering_comparison")
        print(f"   3. 按 method_name 字段分类显示")
        print(f"   4. 使用 method_color 字段设置颜色")
        print(f"   5. 按 density_level 字段设置符号大小")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
